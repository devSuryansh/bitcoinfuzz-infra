"""Metrics agent: attaches to a bitcoinfuzz container and streams metrics.

Runs as an asyncio task per campaign. Polls the Docker container's logs
in bounded windows, parses libFuzzer output, and updates the shared
Prometheus collector. Also watches the corpus directory. Crash counts
come from the orchestrator CrashWatcher so they match SQLite records.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from .collector import MetricsCollector
from .log_lines import iter_log_lines
from .parser import FuzzStats, FuzzEvent, parse_line
from .watcher import CorpusWatcher

logger = logging.getLogger(__name__)

# Seconds between log-window polls, and the worst-case delay before a
# cancelled agent hands its worker thread back to the executor.
LOG_POLL_INTERVAL = 1.0


class MetricsAgent:
    """Per-campaign metrics collection agent.

    Polls a running container's logs in bounded windows (via docker-py),
    parses libFuzzer output lines, updates Prometheus metrics, and
    watches the corpus directory for growth.
    """

    def __init__(
        self,
        campaign_id: str,
        target_name: str,
        container_id: str,
        data_dir: Path,
        collector: MetricsCollector,
        docker_client=None,
    ) -> None:
        self._campaign_id = campaign_id
        self._target = target_name
        self._container_id = container_id
        self._data_dir = data_dir
        self._collector = collector
        self._docker = docker_client

        # Corpus watcher only. Crash attribution is owned by the
        # orchestrator CrashWatcher so SQLite and Prometheus agree.
        target_dir = data_dir / target_name
        self._corpus_watcher = CorpusWatcher(target_dir / "corpus")

        self._task: Optional[asyncio.Task] = None
        self._watcher_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the log parsing and directory watching tasks."""
        self._collector.set_container_status(
            self._target, self._campaign_id, True
        )
        self._task = asyncio.create_task(self._parse_log_stream())
        self._watcher_task = asyncio.create_task(self._watch_directories())
        logger.info(
            "Metrics agent started for campaign %s (target=%s)",
            self._campaign_id,
            self._target,
        )

    async def stop(self, *, remove_metrics: bool = True) -> None:
        """Stop all agent tasks.

        On a clean stop the campaign's metric series are removed so
        ``bitcoinfuzz_container_status == 0`` is not left behind for
        Prometheus to treat as a crash. Pass ``remove_metrics=False``
        when replacing this agent with another for the same campaign.
        """
        for task in [self._task, self._watcher_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if remove_metrics:
            self._collector.remove_campaign(self._target, self._campaign_id)
        logger.info(
            "Metrics agent stopped for campaign %s", self._campaign_id
        )

    def _handle_line(self, line: str) -> None:
        """Parse one log line into collector updates or an event log."""
        result = parse_line(line)
        if isinstance(result, FuzzStats):
            self._collector.update_from_fuzz_stats(
                target=self._target,
                campaign_id=self._campaign_id,
                iteration=result.iteration,
                coverage=result.coverage,
                features=result.features,
                corpus_count=result.corpus_count,
                corpus_bytes=result.corpus_bytes,
                exec_per_sec=result.exec_per_sec,
                rss_mb=result.rss_mb,
            )
        elif isinstance(result, FuzzEvent):
            if result.event_type in ("OOM", "TIMEOUT", "CRASH"):
                logger.warning(
                    "Fuzzer event %s in campaign %s: %s",
                    result.event_type,
                    self._campaign_id,
                    result.raw_line[:200],
                )

    async def _parse_log_stream(self) -> None:
        """Poll container logs in disjoint windows and parse each line.

        Windows rather than a follow stream, because ``next()`` on a
        follow iterator blocks its worker thread until the container
        exits. :meth:`stop` cancels this task, but cancellation cannot
        interrupt a thread already parked inside that call, and the
        worker comes from the default executor shared with every
        orchestrator ``to_thread`` call. A campaign stopped while its
        container keeps running would strand one worker per restart.

        Each ``container.logs`` call here is bounded, so a cancelled
        agent gives its worker back within one poll interval. Chunks are
        still reassembled into complete lines before parsing.
        """
        if self._docker is None:
            logger.warning("No Docker client, agent running in watch-only mode")
            return

        try:
            container = await asyncio.to_thread(
                self._docker.containers.get, self._container_id
            )
            # Start from now: tail=0 on the old follow stream meant the
            # same thing, no backlog replay.
            cursor = time.time()

            while True:
                await asyncio.sleep(LOG_POLL_INTERVAL)

                # Sampled before the window so an exiting container still
                # gets its final lines drained below.
                await asyncio.to_thread(container.reload)
                running = container.attrs.get("State", {}).get("Running", False)

                until = time.time()
                if until > cursor:
                    raw = await asyncio.to_thread(
                        container.logs,
                        follow=False,
                        stream=False,
                        since=cursor,
                        until=until,
                        timestamps=False,
                    )
                    cursor = until
                    for line in iter_log_lines([raw] if raw else []):
                        self._handle_line(line)

                if not running:
                    break

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Log stream error for campaign %s: %s",
                self._campaign_id,
                exc,
            )
            # Unexpected disconnect while the campaign should still be up.
            # Leave the gauge at 0 so CampaignCrashed can fire; a clean
            # stop removes the series instead.
            self._collector.set_container_status(
                self._target, self._campaign_id, False
            )

    async def _watch_directories(self) -> None:
        """Periodically scan corpus and crash directories."""
        while True:
            try:
                # Corpus
                corpus_stats = await asyncio.to_thread(
                    self._corpus_watcher.scan
                )
                self._collector.update_corpus_stats(
                    self._target,
                    self._campaign_id,
                    corpus_stats.file_count,
                    corpus_stats.total_bytes,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Directory watcher error for campaign %s",
                    self._campaign_id,
                )

            await asyncio.sleep(15)

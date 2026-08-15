"""Prometheus metrics collector.

Aggregates parsed libFuzzer stats and directory watcher data into
Prometheus gauges and counters, then exposes them on an HTTP ``/metrics``
endpoint.
"""

import logging
from typing import Optional

from prometheus_client import (
    Counter,
    Gauge,
    CollectorRegistry,
    start_http_server,
)

logger = logging.getLogger(__name__)


class MetricsCollector:
    """Manages Prometheus metrics for one or more bitcoinfuzz campaigns.

    Each metric is labelled with ``target`` and ``campaign_id`` so that
    Prometheus can distinguish between concurrent campaigns.

    Exposed metrics match proposal §6.2::

        bitcoinfuzz_exec_per_second{target, campaign_id}
        bitcoinfuzz_coverage_edges{target, campaign_id}
        bitcoinfuzz_features{target, campaign_id}
        bitcoinfuzz_corpus_files_total{target, campaign_id}
        bitcoinfuzz_corpus_bytes_total{target, campaign_id}
        bitcoinfuzz_crash_total{target, campaign_id}
        bitcoinfuzz_container_status{target, campaign_id}
        bitcoinfuzz_rss_mb{target, campaign_id}
        bitcoinfuzz_memory_limit_mb{target, campaign_id}
        bitcoinfuzz_iteration{target, campaign_id}
    """

    def __init__(self, registry: Optional[CollectorRegistry] = None) -> None:
        self._registry = registry or CollectorRegistry()
        self._labels = ["target", "campaign_id"]

        # libFuzzer stats
        self.exec_per_second = Gauge(
            "bitcoinfuzz_exec_per_second",
            "Fuzzer executions per second",
            self._labels,
            registry=self._registry,
        )
        self.coverage_edges = Gauge(
            "bitcoinfuzz_coverage_edges",
            "Total libFuzzer coverage edges discovered",
            self._labels,
            registry=self._registry,
        )
        self.features = Gauge(
            "bitcoinfuzz_features",
            "Total libFuzzer feature count",
            self._labels,
            registry=self._registry,
        )
        self.iteration = Gauge(
            "bitcoinfuzz_iteration",
            "Current libFuzzer iteration number",
            self._labels,
            registry=self._registry,
        )
        self.rss_mb = Gauge(
            "bitcoinfuzz_rss_mb",
            "Resident set size of the fuzzer process in MB",
            self._labels,
            registry=self._registry,
        )
        self.memory_limit_mb = Gauge(
            "bitcoinfuzz_memory_limit_mb",
            "Configured container memory limit in MB (0 = unlimited)",
            self._labels,
            registry=self._registry,
        )

        # Corpus stats
        self.corpus_files_total = Gauge(
            "bitcoinfuzz_corpus_files_total",
            "Number of files in the fuzzing corpus",
            self._labels,
            registry=self._registry,
        )
        self.corpus_bytes_total = Gauge(
            "bitcoinfuzz_corpus_bytes_total",
            "Total size of the fuzzing corpus in bytes",
            self._labels,
            registry=self._registry,
        )

        # Crash counter (monotonic)
        self.crash_total = Counter(
            "bitcoinfuzz_crash_total",
            "Total number of crash artefacts found",
            self._labels,
            registry=self._registry,
        )
        self._last_crash_counts: dict[tuple[str, str], int] = {}

        # Container status
        self.container_status = Gauge(
            "bitcoinfuzz_container_status",
            "Container health (1=running, 0=unexpected stop; series removed on clean stop)",
            self._labels,
            registry=self._registry,
        )

    def update_from_fuzz_stats(
        self,
        target: str,
        campaign_id: str,
        iteration: int,
        coverage: int,
        features: int,
        corpus_count: int,
        corpus_bytes: int,
        exec_per_sec: int,
        rss_mb: int,
    ) -> None:
        """Update all libFuzzer-derived metrics at once."""
        labels = {"target": target, "campaign_id": campaign_id}
        self.exec_per_second.labels(**labels).set(exec_per_sec)
        self.coverage_edges.labels(**labels).set(coverage)
        self.features.labels(**labels).set(features)
        self.iteration.labels(**labels).set(iteration)
        self.corpus_files_total.labels(**labels).set(corpus_count)
        self.corpus_bytes_total.labels(**labels).set(corpus_bytes)
        self.rss_mb.labels(**labels).set(rss_mb)

    def set_memory_limit(
        self, target: str, campaign_id: str, memory_mb: int
    ) -> None:
        """Publish a campaign's configured memory ceiling.

        HighMemoryUsage divides RSS by this series. Without it the rule
        has to hardcode its own copy of the default, which is wrong for
        any campaign launched with a different limit.
        """
        self.memory_limit_mb.labels(
            target=target, campaign_id=campaign_id
        ).set(memory_mb)

    def update_corpus_stats(
        self,
        target: str,
        campaign_id: str,
        file_count: int,
        total_bytes: int,
    ) -> None:
        """Update corpus directory watcher metrics."""
        labels = {"target": target, "campaign_id": campaign_id}
        self.corpus_files_total.labels(**labels).set(file_count)
        self.corpus_bytes_total.labels(**labels).set(total_bytes)

    def inc_crash(self, target: str, campaign_id: str) -> None:
        """Count one crash attributed by the orchestrator CrashWatcher."""
        self.crash_total.labels(target=target, campaign_id=campaign_id).inc()
        key = (target, campaign_id)
        self._last_crash_counts[key] = self._last_crash_counts.get(key, 0) + 1

    def set_container_status(
        self, target: str, campaign_id: str, running: bool
    ) -> None:
        """Set container health status."""
        self.container_status.labels(
            target=target, campaign_id=campaign_id
        ).set(1 if running else 0)

    def remove_campaign(self, target: str, campaign_id: str) -> None:
        """Remove all metric label sets for a completed campaign."""
        labels = {"target": target, "campaign_id": campaign_id}
        for metric in [
            self.exec_per_second,
            self.coverage_edges,
            self.features,
            self.iteration,
            self.corpus_files_total,
            self.corpus_bytes_total,
            self.crash_total,
            self.container_status,
            self.rss_mb,
            self.memory_limit_mb,
        ]:
            try:
                metric.remove(*[labels["target"], labels["campaign_id"]])
            except KeyError:
                pass  # Label set didn't exist

        self._last_crash_counts.pop((target, campaign_id), None)

    def start_server(self, port: int = 9091) -> None:
        """Start a background HTTP server on ``/metrics``.

        Uses prometheus_client's built-in threaded server.
        """
        start_http_server(port, registry=self._registry)
        logger.info("Metrics server started on port %d", port)

"""Background crash directory watcher.

Polls the ``crash/`` subdirectory of each active campaign's data volume
and fires alerts when new crash artefacts appear.

Crash directories are shared per fuzz *target* (``<data_dir>/<target>/crash``).
When multiple campaigns watch the same target, each new crash file is
attributed to exactly one campaign to avoid duplicate alerts.
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Coroutine, Optional

logger = logging.getLogger(__name__)

# Type alias for the alert callback
AlertCallback = Callable[[str, str, Path], Coroutine]
# Signature: async callback(campaign_id, target_name, crash_file_path)


class CrashWatcher:
    """Watches crash directories for active campaigns.

    Runs as an asyncio background task.  For each registered campaign it
    periodically scans the crash directory and calls the alert callback
    when new files appear.

    Attribution model
    -----------------
    Crash artefacts live at ``<data_dir>/<target>/crash/`` and are therefore
    shared across campaigns for the same target.  Known filenames are tracked
    *per target*.  When a new file appears it is claimed by exactly one
    watching campaign (the earliest-registered still-active watcher for that
    target), so a single crash cannot be attributed to multiple campaigns.
    """

    def __init__(
        self,
        data_dir: Path,
        poll_interval: int = 10,
        alert_callback: Optional[AlertCallback] = None,
    ) -> None:
        """
        Args:
            data_dir: Root data directory (e.g. ``./docker``).  Campaign
                      crash files live at ``<data_dir>/<target>/crash/``.
            poll_interval: Seconds between scans.
            alert_callback: Async callable invoked for each new crash.
        """
        self._data_dir = Path(data_dir)
        self._poll_interval = poll_interval
        self._alert_callback = alert_callback

        # campaign_id -> target_name (registration order preserved via dict)
        self._watched: dict[str, str] = {}
        # target_name -> set of known crash file names (shared across campaigns)
        self._known_by_target: dict[str, set[str]] = {}
        # (target_name, filename) -> campaign_id that claimed the crash
        self._claimed: dict[tuple[str, str], str] = {}
        # Per-campaign crash counts (only claimed crashes)
        self._counts: dict[str, int] = {}
        self._task: Optional[asyncio.Task] = None

    # -- registration --------------------------------------------------------

    def register(self, campaign_id: str, target_name: str) -> None:
        """Start watching the crash directory for a campaign."""
        crash_dir = self._crash_dir_for(target_name)
        if target_name not in self._known_by_target:
            # Seed shared known-set so pre-existing files do not alert.
            self._known_by_target[target_name] = self._scan_existing(crash_dir)
        self._watched[campaign_id] = target_name
        self._counts.setdefault(campaign_id, 0)
        logger.info(
            "Watching crash dir for campaign %s (target=%s, existing=%d files)",
            campaign_id,
            target_name,
            len(self._known_by_target[target_name]),
        )

    def unregister(self, campaign_id: str) -> None:
        """Stop watching a campaign's crash directory."""
        target_name = self._watched.pop(campaign_id, None)
        self._counts.pop(campaign_id, None)
        if target_name is None:
            return
        # Drop claims owned by this campaign; keep shared known-set so a
        # remaining watcher for the same target does not re-alert.
        self._claimed = {
            key: owner
            for key, owner in self._claimed.items()
            if owner != campaign_id
        }
        # If nobody else watches this target, free the known-set.
        if target_name not in self._watched.values():
            self._known_by_target.pop(target_name, None)
        logger.debug("Stopped watching crashes for campaign %s", campaign_id)

    # -- background task -----------------------------------------------------

    def start(self) -> None:
        """Start the background polling task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._poll_loop())
            logger.info("Crash watcher started (interval=%ds)", self._poll_interval)

    def stop(self) -> None:
        """Cancel the background polling task."""
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("Crash watcher stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop — runs until cancelled."""
        while True:
            try:
                await self._scan_all()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in crash watcher poll loop")
            await asyncio.sleep(self._poll_interval)

    async def _scan_all(self) -> None:
        """Scan each watched *target* once and attribute new crashes uniquely."""
        # Group campaign IDs by target, preserving registration order.
        by_target: dict[str, list[str]] = {}
        for campaign_id, target_name in self._watched.items():
            by_target.setdefault(target_name, []).append(campaign_id)

        for target_name, campaign_ids in by_target.items():
            crash_dir = self._crash_dir_for(target_name)
            if not crash_dir.exists():
                continue

            known = self._known_by_target.setdefault(target_name, set())
            current = set(self._list_crash_files(crash_dir))
            new_files = current - known

            for fname in sorted(new_files):
                # Attribute to the earliest-registered watcher for this target.
                owner = campaign_ids[0]
                claim_key = (target_name, fname)
                if claim_key in self._claimed:
                    known.add(fname)
                    continue

                crash_path = crash_dir / fname
                logger.warning(
                    "New crash detected: campaign=%s target=%s file=%s",
                    owner,
                    target_name,
                    crash_path,
                )
                if self._alert_callback:
                    try:
                        await self._alert_callback(owner, target_name, crash_path)
                    except Exception:
                        logger.exception(
                            "Alert callback failed for crash %s", crash_path
                        )
                        # Leave unknown so the next scan retries the record.
                        continue

                self._claimed[claim_key] = owner
                self._counts[owner] = self._counts.get(owner, 0) + 1
                known.add(fname)

    # -- helpers -------------------------------------------------------------

    def _crash_dir_for(self, target_name: str) -> Path:
        """Return the path to a target's crash directory."""
        return self._data_dir / target_name / "crash"

    @staticmethod
    def _scan_existing(crash_dir: Path) -> set[str]:
        """Return the set of filenames currently in a crash directory."""
        if not crash_dir.exists():
            return set()
        return set(CrashWatcher._list_crash_files(crash_dir))

    @staticmethod
    def _list_crash_files(crash_dir: Path) -> list[str]:
        """List filenames in a crash directory, ignoring subdirs."""
        try:
            return [
                entry.name
                for entry in crash_dir.iterdir()
                if entry.is_file()
            ]
        except OSError:
            return []

    # -- inspection ----------------------------------------------------------

    def get_crash_count(self, campaign_id: str) -> int:
        """Return crashes claimed by a campaign since it was registered."""
        return self._counts.get(campaign_id, 0)

    @property
    def watched_campaigns(self) -> list[str]:
        """Return IDs of campaigns currently being watched."""
        return list(self._watched.keys())

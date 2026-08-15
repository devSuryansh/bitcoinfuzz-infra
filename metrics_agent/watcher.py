"""Corpus directory watcher.

Polls a directory on the shared Docker volume to track corpus growth
without reading file contents (only metadata). Crash detection belongs
to the orchestrator's CrashWatcher, which owns crash attribution.
"""

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DirectoryStats:
    """Snapshot of a directory's metadata."""

    file_count: int = 0
    total_bytes: int = 0
    newest_mtime: float = 0.0
    last_scan_time: float = 0.0


class CorpusWatcher:
    """Watches a corpus directory for growth.

    Scans the directory at a configurable interval and tracks:
    - Total file count
    - Total size in bytes
    - Modification time of the newest file
    """

    def __init__(self, corpus_path: Path, poll_interval: int = 15) -> None:
        self._path = corpus_path
        self._poll_interval = poll_interval
        self._stats = DirectoryStats()
        self._last_scan: float = 0.0

    def scan(self) -> DirectoryStats:
        """Scan the corpus directory and update stats.

        This is intentionally non-destructive — it reads only file
        metadata (name, size, mtime), never file contents.
        """
        now = time.time()
        if now - self._last_scan < self._poll_interval:
            return self._stats

        self._last_scan = now

        if not self._path.exists():
            self._stats = DirectoryStats(last_scan_time=now)
            return self._stats

        file_count = 0
        total_bytes = 0
        newest_mtime = 0.0

        try:
            with os.scandir(self._path) as entries:
                for entry in entries:
                    if entry.is_file(follow_symlinks=False):
                        try:
                            stat = entry.stat(follow_symlinks=False)
                            file_count += 1
                            total_bytes += stat.st_size
                            if stat.st_mtime > newest_mtime:
                                newest_mtime = stat.st_mtime
                        except OSError:
                            continue
        except OSError as exc:
            logger.debug("Error scanning corpus dir %s: %s", self._path, exc)

        self._stats = DirectoryStats(
            file_count=file_count,
            total_bytes=total_bytes,
            newest_mtime=newest_mtime,
            last_scan_time=now,
        )
        return self._stats

    @property
    def stats(self) -> DirectoryStats:
        return self._stats

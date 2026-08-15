"""SQLite persistence layer for campaign state and historical data.

One ``sqlite3`` connection opened with ``check_same_thread=False`` and WAL
journal mode, shared by every caller. Callers reach it from the FastAPI
event loop through ``asyncio.to_thread``, so several worker threads can be
inside this class at once.

Reads run unserialised: WAL lets them proceed against a consistent snapshot
while a write is in flight. Writes take ``_write_lock`` for the whole
statement-plus-commit sequence, because a method like ``record_crash``
issues two statements that must land together, and ``commit()`` on a shared
connection commits whatever else happens to be pending on it.
"""

import json
import logging
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import (
    ACTIVE_STATES,
    DEFAULT_CPU_QUOTA,
    DEFAULT_MEMORY_MB,
    Campaign,
    CampaignState,
    ResourceLimits,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = f"""
-- Active campaigns (mutable, written on every state change)
CREATE TABLE IF NOT EXISTS campaigns (
    id              TEXT PRIMARY KEY,
    target_name     TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'queued',
    container_id    TEXT,
    image_tag       TEXT,
    started_at      TEXT,
    ended_at        TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    max_duration_seconds INTEGER,
    cpu_quota       INTEGER NOT NULL DEFAULT {DEFAULT_CPU_QUOTA},
    memory_mb       INTEGER NOT NULL DEFAULT {DEFAULT_MEMORY_MB},
    priority        INTEGER NOT NULL DEFAULT 1,
    modules         TEXT,
    -- '{{}}' is an escaped literal: this is an f-string so the resource
    -- defaults below stay in sync with orchestrator.models.
    env_overrides   TEXT NOT NULL DEFAULT '{{}}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    crash_count     INTEGER NOT NULL DEFAULT 0,
    exit_reason     TEXT,
    final_coverage      INTEGER,
    final_corpus_count  INTEGER,
    final_corpus_bytes  INTEGER,
    total_executions    INTEGER,
    avg_exec_per_sec    REAL,
    peak_rss_mb         INTEGER
);

-- Archived summaries (immutable once written, for historical queries)
CREATE TABLE IF NOT EXISTS campaign_summaries (
    id                  TEXT PRIMARY KEY,
    target_name         TEXT NOT NULL,
    started_at          TEXT,
    ended_at            TEXT,
    duration_seconds    INTEGER,
    final_coverage      INTEGER,
    final_corpus_count  INTEGER,
    final_corpus_bytes  INTEGER,
    total_executions    INTEGER,
    avg_exec_per_sec    REAL,
    peak_rss_mb         INTEGER,
    crash_count         INTEGER NOT NULL DEFAULT 0,
    exit_reason         TEXT,
    modules             TEXT,
    cxxflags            TEXT
);

-- Crash artefacts discovered during campaigns
CREATE TABLE IF NOT EXISTS crash_records (
    id              TEXT PRIMARY KEY,
    campaign_id     TEXT NOT NULL REFERENCES campaigns(id),
    target_name     TEXT NOT NULL,
    discovered_at   TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    file_size       INTEGER,
    notified        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_campaigns_state ON campaigns(state);
CREATE INDEX IF NOT EXISTS idx_campaigns_target ON campaigns(target_name);
CREATE INDEX IF NOT EXISTS idx_summaries_target ON campaign_summaries(target_name);
CREATE INDEX IF NOT EXISTS idx_crashes_campaign ON crash_records(campaign_id);
CREATE INDEX IF NOT EXISTS idx_crashes_target ON crash_records(target_name);
"""


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    """Thin synchronous wrapper around SQLite for campaign persistence.

    Every method is blocking; call them from ``asyncio.to_thread``. Writes
    are serialised by ``_write_lock``, reads are not. The lock is plain
    (non-reentrant) and no write method calls another, so it cannot
    deadlock against itself.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        # Held across statement-plus-commit in every write method. Without
        # it a second thread's commit can land between two statements of
        # the first, persisting half of what was meant to be one change.
        self._write_lock = threading.Lock()

    # -- lifecycle -----------------------------------------------------------

    def connect(self) -> None:
        """Open the database and apply the schema."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._db_path),
            check_same_thread=False,
            timeout=30.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._migrate()
        self._conn.commit()
        logger.info("Database initialised at %s", self._db_path)

    def _migrate(self) -> None:
        """Apply additive column migrations for existing databases."""
        cols = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(campaigns)").fetchall()
        }
        if "max_duration_seconds" not in cols:
            self._conn.execute(
                "ALTER TABLE campaigns ADD COLUMN max_duration_seconds INTEGER"
            )
            logger.info("Migrated campaigns: added max_duration_seconds")

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn

    # -- campaign CRUD -------------------------------------------------------

    def create_campaign(self, campaign: Campaign) -> None:
        """Insert a new campaign row.

        Both timestamps come from the campaign itself rather than the
        clock, so a row read back reproduces the object that wrote it.
        """
        with self._write_lock:
            self.conn.execute(
                """
                INSERT INTO campaigns
                    (id, target_name, state, container_id, image_tag,
                     started_at, ended_at, retry_count, max_retries,
                     max_duration_seconds,
                     cpu_quota, memory_mb, priority, modules, env_overrides,
                     created_at, updated_at, crash_count, exit_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign.id,
                    campaign.target_name,
                    campaign.state.value,
                    campaign.container_id,
                    campaign.image_tag,
                    campaign.started_at.isoformat() if campaign.started_at else None,
                    campaign.ended_at.isoformat() if campaign.ended_at else None,
                    campaign.retry_count,
                    campaign.max_retries,
                    campaign.max_duration_seconds,
                    campaign.resource_limits.cpu_quota,
                    campaign.resource_limits.memory_mb,
                    campaign.priority,
                    campaign.modules,
                    json.dumps(campaign.env_overrides),
                    campaign.created_at.isoformat(),
                    campaign.updated_at.isoformat(),
                    campaign.crash_count,
                    campaign.exit_reason,
                ),
            )
            self.conn.commit()

    def update_campaign(self, campaign: Campaign) -> None:
        """Persist current campaign state to the database."""
        now = datetime.now(timezone.utc).isoformat()
        with self._write_lock:
            self.conn.execute(
                """
                UPDATE campaigns SET
                    state = ?, container_id = ?, started_at = ?, ended_at = ?,
                    retry_count = ?, max_duration_seconds = ?,
                    crash_count = ?, exit_reason = ?,
                    final_coverage = ?, final_corpus_count = ?,
                    final_corpus_bytes = ?,
                    total_executions = ?, avg_exec_per_sec = ?, peak_rss_mb = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    campaign.state.value,
                    campaign.container_id,
                    campaign.started_at.isoformat() if campaign.started_at else None,
                    campaign.ended_at.isoformat() if campaign.ended_at else None,
                    campaign.retry_count,
                    campaign.max_duration_seconds,
                    campaign.crash_count,
                    campaign.exit_reason,
                    campaign.final_coverage,
                    campaign.final_corpus_count,
                    campaign.final_corpus_bytes,
                    campaign.total_executions,
                    campaign.avg_exec_per_sec,
                    campaign.peak_rss_mb,
                    now,
                    campaign.id,
                ),
            )
            self.conn.commit()

    def get_campaign(self, campaign_id: str) -> Optional[Campaign]:
        """Load a single campaign by ID, or None if not found."""
        row = self.conn.execute(
            "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_campaign(row)

    def list_campaigns(
        self,
        state: Optional[CampaignState] = None,
        target_name: Optional[str] = None,
        limit: Optional[int] = 100,
    ) -> list[Campaign]:
        """List campaigns with optional filters.

        ``limit=None`` returns every matching row. Startup reload must
        use that; the default 100 is only for API listings.
        """
        query = "SELECT * FROM campaigns WHERE 1=1"
        params: list = []
        if state is not None:
            query += " AND state = ?"
            params.append(state.value)
        if target_name is not None:
            query += " AND target_name = ?"
            params.append(target_name)
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_campaign(r) for r in rows]

    # -- archival ------------------------------------------------------------

    def archive_campaign(self, campaign: Campaign, cxxflags: str = "") -> None:
        """Write a completed/failed campaign to the summaries table."""
        duration = None
        if campaign.started_at and campaign.ended_at:
            duration = int(
                (campaign.ended_at - campaign.started_at).total_seconds()
            )
        with self._write_lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO campaign_summaries
                    (id, target_name, started_at, ended_at, duration_seconds,
                     final_coverage, final_corpus_count, final_corpus_bytes,
                     total_executions, avg_exec_per_sec, peak_rss_mb,
                     crash_count, exit_reason, modules, cxxflags)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    campaign.id,
                    campaign.target_name,
                    campaign.started_at.isoformat() if campaign.started_at else None,
                    campaign.ended_at.isoformat() if campaign.ended_at else None,
                    duration,
                    campaign.final_coverage,
                    campaign.final_corpus_count,
                    campaign.final_corpus_bytes,
                    campaign.total_executions,
                    campaign.avg_exec_per_sec,
                    campaign.peak_rss_mb,
                    campaign.crash_count,
                    campaign.exit_reason,
                    campaign.modules,
                    cxxflags,
                ),
            )
            self.conn.commit()

    def get_target_history(
        self, target_name: str, limit: int = 10
    ) -> list[dict]:
        """Return archived summaries for a given target, newest first."""
        rows = self.conn.execute(
            """
            SELECT * FROM campaign_summaries
            WHERE target_name = ?
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (target_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def compare_campaigns(self, campaign_ids: list[str]) -> list[dict]:
        """Return summaries for the given campaign IDs."""
        placeholders = ",".join("?" for _ in campaign_ids)
        rows = self.conn.execute(
            f"SELECT * FROM campaign_summaries WHERE id IN ({placeholders})",
            campaign_ids,
        ).fetchall()
        return [dict(r) for r in rows]

    # -- crash records -------------------------------------------------------

    def record_crash(
        self,
        campaign_id: str,
        target_name: str,
        file_path: str,
        file_size: Optional[int] = None,
    ) -> str:
        """Insert a crash record and bump the campaign counter, atomically.

        The two statements are one logical change: a crash row whose
        campaign counter was never incremented would understate the count
        forever. Holding the lock across both keeps another thread's
        ``commit()`` from landing between them.
        """
        crash_id = uuid.uuid4().hex[:12]
        now = datetime.now(timezone.utc).isoformat()
        with self._write_lock:
            self.conn.execute(
                """
                INSERT INTO crash_records
                    (id, campaign_id, target_name, discovered_at,
                     file_path, file_size)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (crash_id, campaign_id, target_name, now, file_path, file_size),
            )
            self.conn.execute(
                "UPDATE campaigns SET crash_count = crash_count + 1 WHERE id = ?",
                (campaign_id,),
            )
            self.conn.commit()
        return crash_id

    def get_crashes(
        self,
        campaign_id: Optional[str] = None,
        target_name: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """List crash records with optional filters."""
        query = "SELECT * FROM crash_records WHERE 1=1"
        params: list = []
        if campaign_id:
            query += " AND campaign_id = ?"
            params.append(campaign_id)
        if target_name:
            query += " AND target_name = ?"
            params.append(target_name)
        query += " ORDER BY discovered_at DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_dashboard_stats(self) -> dict:
        """Aggregate stats for the dashboard summary endpoint."""
        states = self.conn.execute(
            "SELECT state, COUNT(*) as cnt FROM campaigns GROUP BY state"
        ).fetchall()
        state_counts: dict[str, int] = {r["state"]: r["cnt"] for r in states}
        total_crashes = (
            self.conn.execute("SELECT COUNT(*) FROM crash_records").fetchone()[0]
        )
        return {
            "active_campaigns": sum(
                state_counts.get(state.value, 0) for state in ACTIVE_STATES
            ),
            "queued_campaigns": state_counts.get("queued", 0),
            "completed_campaigns": state_counts.get("completed", 0),
            "failed_campaigns": state_counts.get("failed", 0),
            "total_crashes": total_crashes,
        }

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _row_to_campaign(row: sqlite3.Row) -> Campaign:
        """Convert a database row to a Campaign instance."""
        env_overrides = json.loads(row["env_overrides"]) if row["env_overrides"] else {}
        # Keys may be missing on very old rows before migration; default safely.
        keys = row.keys()
        max_duration = (
            row["max_duration_seconds"] if "max_duration_seconds" in keys else None
        )
        # Both columns are NOT NULL in the schema, so the fallbacks only
        # cover a hand-edited database. Omitting them let ``default_factory``
        # fire at read time, which made every reloaded campaign look as
        # though it had just been created.
        created_at = (
            datetime.fromisoformat(row["created_at"])
            if "created_at" in keys and row["created_at"]
            else datetime.now(timezone.utc)
        )
        updated_at = (
            datetime.fromisoformat(row["updated_at"])
            if "updated_at" in keys and row["updated_at"]
            else created_at
        )
        return Campaign(
            id=row["id"],
            target_name=row["target_name"],
            state=CampaignState(row["state"]),
            container_id=row["container_id"],
            image_tag=row["image_tag"],
            started_at=(
                datetime.fromisoformat(row["started_at"])
                if row["started_at"]
                else None
            ),
            ended_at=(
                datetime.fromisoformat(row["ended_at"])
                if row["ended_at"]
                else None
            ),
            retry_count=row["retry_count"],
            max_retries=row["max_retries"],
            max_duration_seconds=max_duration,
            resource_limits=ResourceLimits(
                cpu_quota=row["cpu_quota"],
                memory_mb=row["memory_mb"],
            ),
            priority=row["priority"],
            modules=row["modules"],
            env_overrides=env_overrides,
            crash_count=row["crash_count"],
            exit_reason=row["exit_reason"],
            final_coverage=row["final_coverage"],
            final_corpus_count=row["final_corpus_count"],
            final_corpus_bytes=row["final_corpus_bytes"],
            total_executions=row["total_executions"],
            avg_exec_per_sec=row["avg_exec_per_sec"],
            peak_rss_mb=row["peak_rss_mb"],
            created_at=created_at,
            updated_at=updated_at,
        )

"""Tests for the SQLite database persistence layer."""

import pytest
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from orchestrator.database import Database
from orchestrator.models import (
    DEFAULT_CPU_QUOTA,
    DEFAULT_MEMORY_MB,
    Campaign,
    CampaignCreate,
    CampaignState,
    ResourceLimits,
)


@pytest.fixture
def db():
    """Create an in-memory database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        database = Database(db_path)
        database.connect()
        yield database
        database.close()


class TestDatabaseCampaignCRUD:
    """Test basic CRUD operations on campaigns."""

    def test_create_and_get(self, db: Database):
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)
        loaded = db.get_campaign(campaign.id)
        assert loaded is not None
        assert loaded.target_name == "script"
        assert loaded.state == CampaignState.QUEUED

    def test_max_duration_persists(self, db: Database):
        """Shared hosts rely on duration limits surviving orchestrator restarts."""
        campaign = Campaign(
            target_name="script",
            max_duration_seconds=3600,
        )
        db.create_campaign(campaign)
        loaded = db.get_campaign(campaign.id)
        assert loaded is not None
        assert loaded.max_duration_seconds == 3600

        campaign.max_duration_seconds = 120
        db.update_campaign(campaign)
        loaded = db.get_campaign(campaign.id)
        assert loaded.max_duration_seconds == 120

    def test_get_nonexistent(self, db: Database):
        assert db.get_campaign("nonexistent") is None

    def test_update_campaign(self, db: Database):
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)
        campaign.transition_to(CampaignState.STARTING)
        campaign.transition_to(CampaignState.RUNNING)
        campaign.container_id = "abc123"
        db.update_campaign(campaign)
        loaded = db.get_campaign(campaign.id)
        assert loaded.state == CampaignState.RUNNING
        assert loaded.container_id == "abc123"

    def test_list_campaigns(self, db: Database):
        for name in ["script", "psbt_parse", "ecdh"]:
            db.create_campaign(Campaign(target_name=name))
        all_campaigns = db.list_campaigns()
        assert len(all_campaigns) == 3

    def test_list_by_state(self, db: Database):
        c1 = Campaign(target_name="script")
        c2 = Campaign(target_name="psbt_parse")
        db.create_campaign(c1)
        db.create_campaign(c2)
        c1.transition_to(CampaignState.STARTING)
        c1.transition_to(CampaignState.RUNNING)
        db.update_campaign(c1)
        running = db.list_campaigns(state=CampaignState.RUNNING)
        assert len(running) == 1
        assert running[0].target_name == "script"

    def test_list_campaigns_unlimited(self, db: Database):
        for _ in range(12):
            db.create_campaign(Campaign(target_name="script"))
        assert len(db.list_campaigns(limit=5)) == 5
        assert len(db.list_campaigns(limit=None)) == 12

    def test_list_by_target(self, db: Database):
        db.create_campaign(Campaign(target_name="script"))
        db.create_campaign(Campaign(target_name="script"))
        db.create_campaign(Campaign(target_name="ecdh"))
        scripts = db.list_campaigns(target_name="script")
        assert len(scripts) == 2

    def test_created_at_survives_a_round_trip(self, db: Database):
        """A reloaded campaign kept its creation time, not the read time."""
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)

        loaded = db.get_campaign(campaign.id)

        assert loaded.created_at == campaign.created_at
        assert loaded.updated_at == campaign.updated_at

    def test_updated_at_tracks_state_changes_across_reload(self, db: Database):
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)
        created = campaign.created_at

        campaign.transition_to(CampaignState.STARTING)
        db.update_campaign(campaign)

        loaded = db.get_campaign(campaign.id)
        assert loaded.created_at == created
        assert loaded.updated_at > created

    def test_timestamps_survive_list_and_archive_paths(self, db: Database):
        """``list_campaigns`` goes through the same row converter."""
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)

        (loaded,) = db.list_campaigns(target_name="script")

        assert loaded.created_at == campaign.created_at

    def test_env_overrides_roundtrip(self, db: Database):
        campaign = Campaign(
            target_name="script",
            env_overrides={"LIBFUZZ_DETECT_LEAKS": "0", "ASAN_OPTIONS": "detect_leaks=0"},
        )
        db.create_campaign(campaign)
        loaded = db.get_campaign(campaign.id)
        assert loaded.env_overrides == {"LIBFUZZ_DETECT_LEAKS": "0", "ASAN_OPTIONS": "detect_leaks=0"}

    def test_resource_limits_roundtrip(self, db: Database):
        limits = ResourceLimits(cpu_quota=400_000, memory_mb=4096)
        campaign = Campaign(target_name="script", resource_limits=limits)
        db.create_campaign(campaign)
        loaded = db.get_campaign(campaign.id)
        assert loaded.resource_limits.cpu_quota == 400_000
        assert loaded.resource_limits.memory_mb == 4096


class TestResourceDefaults:
    """The default lives in one constant; every consumer must agree."""

    def test_schema_defaults_match_the_constants(self, db: Database):
        db.conn.execute(
            "INSERT INTO campaigns (id, target_name, created_at, updated_at) "
            "VALUES ('defaults', 'script', '2020-01-01', '2020-01-01')"
        )
        row = db.conn.execute(
            "SELECT cpu_quota, memory_mb FROM campaigns WHERE id = 'defaults'"
        ).fetchone()

        assert row["cpu_quota"] == DEFAULT_CPU_QUOTA
        assert row["memory_mb"] == DEFAULT_MEMORY_MB

    def test_resource_limits_defaults_match_the_constants(self):
        limits = ResourceLimits()
        assert limits.cpu_quota == DEFAULT_CPU_QUOTA
        assert limits.memory_mb == DEFAULT_MEMORY_MB

    def test_request_model_defaults_match_the_constants(self):
        request = CampaignCreate(target_name="script")
        assert request.cpu_quota == DEFAULT_CPU_QUOTA
        assert request.memory_mb == DEFAULT_MEMORY_MB


class TestDatabaseArchival:
    """Test campaign archival to summaries table."""

    def test_archive_campaign(self, db: Database):
        campaign = Campaign(target_name="script")
        campaign.transition_to(CampaignState.STARTING)
        campaign.transition_to(CampaignState.RUNNING)
        campaign.transition_to(CampaignState.COMPLETED)
        campaign.exit_reason = "completed"
        campaign.final_coverage = 847
        campaign.final_corpus_count = 412
        campaign.crash_count = 2
        db.create_campaign(campaign)
        db.archive_campaign(campaign, cxxflags="-DBITCOIN_CORE -DRUST_BITCOIN")

        history = db.get_target_history("script")
        assert len(history) == 1
        assert history[0]["final_coverage"] == 847
        assert history[0]["crash_count"] == 2
        assert history[0]["cxxflags"] == "-DBITCOIN_CORE -DRUST_BITCOIN"

    def test_archive_with_duration(self, db: Database):
        campaign = Campaign(target_name="script")
        campaign.transition_to(CampaignState.STARTING)
        campaign.transition_to(CampaignState.RUNNING)
        campaign.transition_to(CampaignState.COMPLETED)
        db.create_campaign(campaign)
        db.archive_campaign(campaign)
        history = db.get_target_history("script")
        assert history[0]["duration_seconds"] is not None

    def test_compare_campaigns(self, db: Database):
        ids = []
        for i in range(3):
            c = Campaign(target_name="script")
            c.transition_to(CampaignState.STARTING)
            c.transition_to(CampaignState.RUNNING)
            c.transition_to(CampaignState.COMPLETED)
            c.final_coverage = 100 * (i + 1)
            db.create_campaign(c)
            db.archive_campaign(c)
            ids.append(c.id)

        compared = db.compare_campaigns(ids[:2])
        assert len(compared) == 2
        assert all(row["target_name"] == "script" for row in compared)

    def test_compare_campaigns_mixed_targets(self, db: Database):
        script = Campaign(target_name="script")
        script.transition_to(CampaignState.STARTING)
        script.transition_to(CampaignState.RUNNING)
        script.transition_to(CampaignState.COMPLETED)
        ecdh = Campaign(target_name="ecdh")
        ecdh.transition_to(CampaignState.STARTING)
        ecdh.transition_to(CampaignState.RUNNING)
        ecdh.transition_to(CampaignState.COMPLETED)
        db.create_campaign(script)
        db.create_campaign(ecdh)
        db.archive_campaign(script)
        db.archive_campaign(ecdh)

        compared = db.compare_campaigns([script.id, ecdh.id])
        names = {row["target_name"] for row in compared}
        assert names == {"script", "ecdh"}


class TestDatabaseCrashRecords:
    """Test crash record operations."""

    def test_record_crash(self, db: Database):
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)
        crash_id = db.record_crash(
            campaign.id, "script", "/data/script/crash/crash-abc", 1024
        )
        assert crash_id is not None

        crashes = db.get_crashes(campaign_id=campaign.id)
        assert len(crashes) == 1
        assert crashes[0]["file_path"] == "/data/script/crash/crash-abc"
        assert crashes[0]["file_size"] == 1024

    def test_concurrent_crashes_all_counted(self, db: Database):
        """The insert and the counter bump must land as one change.

        Without the write lock a second thread's commit can slip between
        them, persisting a crash row whose campaign counter never moved.
        """
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)
        crash_count = 40

        def record(i: int) -> None:
            db.record_crash(campaign.id, "script", f"/crash{i}", 100)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(record, range(crash_count)))

        assert len(db.get_crashes(campaign_id=campaign.id)) == crash_count
        assert db.get_campaign(campaign.id).crash_count == crash_count

    def test_crash_increments_campaign_count(self, db: Database):
        campaign = Campaign(target_name="script")
        db.create_campaign(campaign)
        db.record_crash(campaign.id, "script", "/crash1", 100)
        db.record_crash(campaign.id, "script", "/crash2", 200)
        loaded = db.get_campaign(campaign.id)
        assert loaded.crash_count == 2

    def test_get_crashes_by_target(self, db: Database):
        c1 = Campaign(target_name="script")
        c2 = Campaign(target_name="ecdh")
        db.create_campaign(c1)
        db.create_campaign(c2)
        db.record_crash(c1.id, "script", "/crash1")
        db.record_crash(c2.id, "ecdh", "/crash2")
        script_crashes = db.get_crashes(target_name="script")
        assert len(script_crashes) == 1


class TestDashboardStats:
    """Test dashboard aggregate stats."""

    def test_dashboard_stats(self, db: Database):
        c1 = Campaign(target_name="script")
        c2 = Campaign(target_name="ecdh")
        c3 = Campaign(target_name="psbt_parse")
        db.create_campaign(c1)
        db.create_campaign(c2)
        db.create_campaign(c3)

        c1.transition_to(CampaignState.STARTING)
        c1.transition_to(CampaignState.RUNNING)
        db.update_campaign(c1)

        c3.transition_to(CampaignState.STARTING)
        c3.transition_to(CampaignState.RUNNING)
        c3.transition_to(CampaignState.COMPLETED)
        db.update_campaign(c3)

        stats = db.get_dashboard_stats()
        assert stats["active_campaigns"] == 1  # c1 running
        assert stats["queued_campaigns"] == 1  # c2 queued
        assert stats["completed_campaigns"] == 1  # c3 completed

    def test_dashboard_active_includes_paused_and_crashed(self, db: Database):
        """Same ACTIVE_STATES set as /health: paused and crashed count."""
        paused = Campaign(target_name="script")
        paused.transition_to(CampaignState.STARTING)
        paused.transition_to(CampaignState.RUNNING)
        paused.transition_to(CampaignState.PAUSED)
        crashed = Campaign(target_name="ecdh")
        crashed.transition_to(CampaignState.STARTING)
        crashed.transition_to(CampaignState.RUNNING)
        crashed.transition_to(CampaignState.CRASHED)
        queued = Campaign(target_name="psbt_parse")
        db.create_campaign(paused)
        db.create_campaign(crashed)
        db.create_campaign(queued)

        stats = db.get_dashboard_stats()
        assert stats["active_campaigns"] == 2
        assert stats["queued_campaigns"] == 1

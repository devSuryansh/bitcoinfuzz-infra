"""Tests for campaign lifecycle manager.

Docker is mocked. Persistence uses a temp SQLite file.
"""

import asyncio
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from metrics_agent.collector import MetricsCollector
from orchestrator.campaign_manager import CampaignManager
from orchestrator.crash_watcher import CrashWatcher
from orchestrator.database import Database
from orchestrator.models import (
    ACTIVE_STATES,
    Campaign,
    CampaignCreate,
    CampaignState,
    ResourceLimits,
)
from orchestrator.scheduler import ResourceScheduler
from orchestrator.targets import FuzzTarget
from prometheus_client import CollectorRegistry


@pytest.fixture
def manager(tmp_path: Path):
    db = Database(tmp_path / "test.db")
    db.connect()
    docker_mgr = MagicMock()
    scheduler = ResourceScheduler(total_cpu_quota=800_000, total_memory_mb=16384)
    crash_watcher = CrashWatcher(data_dir=tmp_path / "data", poll_interval=60)
    target_registry = {
        "script": FuzzTarget(name="script", cxxflags="", fuzz="script"),
    }
    docker_client = MagicMock()
    container = MagicMock()
    container.logs.return_value = iter([])
    docker_client.containers.get.return_value = container
    mgr = CampaignManager(
        db=db,
        docker_mgr=docker_mgr,
        scheduler=scheduler,
        crash_watcher=crash_watcher,
        target_registry=target_registry,
        data_dir=tmp_path / "data",
        collector=MetricsCollector(registry=CollectorRegistry()),
        docker_client=docker_client,
        health_poll_interval=60,
    )
    yield mgr
    db.close()


def _persist(manager: CampaignManager, campaign: Campaign) -> Campaign:
    manager._db.create_campaign(campaign)
    return campaign


@pytest.mark.asyncio
async def test_stop_queued_campaign_marks_completed(manager: CampaignManager):
    """Queued campaigns never launched; stop must not require RUNNING first."""
    campaign = Campaign(target_name="script")
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign
    manager._scheduler.enqueue(campaign.id, campaign.priority)

    result = await manager.stop_campaign(campaign.id)

    assert result.state == CampaignState.COMPLETED
    assert result.exit_reason == "stopped"
    assert manager._scheduler.queue_depth == 0
    docker_mgr = manager._docker
    docker_mgr.stop_container.assert_not_called()


@pytest.mark.asyncio
async def test_stop_starting_campaign_marks_completed(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.container_id = "ctr-starting"
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign

    result = await manager.stop_campaign(campaign.id)

    assert result.state == CampaignState.COMPLETED
    assert result.exit_reason == "stopped"
    manager._docker.stop_container.assert_called_once_with("ctr-starting")


@pytest.mark.asyncio
async def test_stop_crashed_campaign_marks_completed(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.transition_to(CampaignState.CRASHED)
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign

    result = await manager.stop_campaign(campaign.id)

    assert result.state == CampaignState.COMPLETED
    assert result.exit_reason == "stopped"


@pytest.mark.asyncio
async def test_stop_restarting_campaign_marks_completed(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.transition_to(CampaignState.CRASHED)
    campaign.transition_to(CampaignState.RESTARTING)
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign

    result = await manager.stop_campaign(campaign.id)

    assert result.state == CampaignState.COMPLETED
    assert result.exit_reason == "stopped"


@pytest.mark.asyncio
async def test_start_recovers_starting_with_live_container(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.container_id = "ctr-live"
    _persist(manager, campaign)
    manager._docker.get_container_status.return_value = {
        "running": True,
        "paused": False,
    }

    await manager.start()
    try:
        loaded = manager._campaigns[campaign.id]
        assert loaded.state == CampaignState.RUNNING
        assert campaign.id in manager._agents
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_start_relaunches_starting_without_container(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    _persist(manager, campaign)
    manager._docker.launch_container.return_value = "ctr-new"

    await manager.start()
    try:
        loaded = manager._campaigns[campaign.id]
        assert loaded.state == CampaignState.RUNNING
        assert loaded.container_id == "ctr-new"
        manager._docker.launch_container.assert_called_once()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_start_recovers_crashed_by_restarting(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.transition_to(CampaignState.CRASHED)
    campaign.container_id = "ctr-dead"
    _persist(manager, campaign)
    manager._docker.launch_container.return_value = "ctr-restarted"

    await manager.start()
    try:
        loaded = manager._campaigns[campaign.id]
        assert loaded.state == CampaignState.RUNNING
        assert loaded.container_id == "ctr-restarted"
        assert loaded.retry_count == 1
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_start_continues_restarting(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.transition_to(CampaignState.CRASHED)
    campaign.transition_to(CampaignState.RESTARTING)
    campaign.retry_count = 1
    _persist(manager, campaign)
    manager._docker.launch_container.return_value = "ctr-continued"

    await manager.start()
    try:
        loaded = manager._campaigns[campaign.id]
        assert loaded.state == CampaignState.RUNNING
        assert loaded.container_id == "ctr-continued"
        assert loaded.retry_count == 1
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_start_reattaches_metrics_for_running(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.container_id = "ctr-running"
    _persist(manager, campaign)

    await manager.start()
    try:
        assert campaign.id in manager._agents
        agent = manager._agents[campaign.id]
        assert agent._container_id == "ctr-running"
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_start_metrics_agent_stops_previous(manager: CampaignManager):
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.container_id = "ctr-old"
    old_agent = MagicMock()
    old_agent.stop = AsyncMock()
    manager._agents[campaign.id] = old_agent

    campaign.container_id = "ctr-new"
    await manager._start_metrics_agent(campaign)

    old_agent.stop.assert_awaited_once_with(remove_metrics=False)
    assert manager._agents[campaign.id] is not old_agent
    await manager._agents[campaign.id].stop()


@pytest.mark.asyncio
async def test_start_launches_queued_when_resources_free(manager: CampaignManager):
    """Persisted QUEUED campaigns must launch on startup, not wait for a finalise."""
    campaign = Campaign(target_name="script")
    _persist(manager, campaign)
    manager._docker.launch_container.return_value = "ctr-from-queue"

    await manager.start()
    try:
        loaded = manager._campaigns[campaign.id]
        assert loaded.state == CampaignState.RUNNING
        assert loaded.container_id == "ctr-from-queue"
        manager._docker.launch_container.assert_called_once()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_start_keeps_queued_when_resources_full(manager: CampaignManager):
    running = Campaign(
        target_name="script",
        resource_limits=ResourceLimits(cpu_quota=800_000, memory_mb=16384),
    )
    running.transition_to(CampaignState.STARTING)
    running.transition_to(CampaignState.RUNNING)
    running.container_id = "ctr-full"
    queued = Campaign(target_name="script")
    _persist(manager, running)
    _persist(manager, queued)

    await manager.start()
    try:
        assert manager._campaigns[running.id].state == CampaignState.RUNNING
        assert manager._campaigns[queued.id].state == CampaignState.QUEUED
        manager._docker.launch_container.assert_not_called()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_stop_during_launch_removes_container(manager: CampaignManager):
    """A DELETE while Docker is creating the container must not leak it."""
    release = threading.Event()

    def blocking_launch(campaign, cxxflags, target):
        release.wait(timeout=5)
        return "orphan-ctr-123456"

    manager._docker.launch_container.side_effect = blocking_launch
    campaign = Campaign(target_name="script")
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign

    start_task = asyncio.create_task(manager._start_campaign(campaign))
    for _ in range(100):
        await asyncio.sleep(0.01)
        if manager._docker.launch_container.called:
            break
    assert manager._docker.launch_container.called
    assert campaign.state == CampaignState.STARTING

    stop_task = asyncio.create_task(manager.stop_campaign(campaign.id))
    await asyncio.sleep(0.05)
    release.set()
    await start_task
    await stop_task

    manager._docker.remove_container.assert_called()
    assert campaign.state == CampaignState.COMPLETED


@pytest.mark.asyncio
async def test_handle_crash_alert_does_not_clobber_db_count(
    manager: CampaignManager, tmp_path: Path
):
    campaign = Campaign(target_name="script")
    campaign.crash_count = 0
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign
    manager._db.conn.execute(
        "UPDATE campaigns SET crash_count = 5 WHERE id = ?", (campaign.id,)
    )
    manager._db.conn.commit()

    crash = tmp_path / "crash-x"
    crash.write_text("x")
    await manager.handle_crash_alert(campaign.id, "script", crash)

    loaded = manager._db.get_campaign(campaign.id)
    assert loaded is not None
    assert loaded.crash_count == 6
    assert campaign.crash_count == 6
    assert (
        manager._collector.crash_total.labels(
            target="script", campaign_id=campaign.id
        )._value.get()
        == 1.0
    )


@pytest.mark.asyncio
async def test_first_launch_failure_is_finalised(manager: CampaignManager):
    """A campaign that never launches must still be archived.

    The failure path used to write the row and stop there, leaving the
    campaign out of campaign_summaries and its resources allocated.
    """
    manager._docker.launch_container.side_effect = RuntimeError("no such image")
    campaign = Campaign(target_name="script")
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign
    manager._scheduler.allocate(campaign.id, campaign.resource_limits)

    await manager._start_campaign(campaign)

    assert campaign.state == CampaignState.FAILED
    assert campaign.exit_reason.startswith("start_failed:")
    # transition_to, not a bare assignment: both stamps must move.
    assert campaign.ended_at is not None
    assert campaign.updated_at == campaign.ended_at
    history = manager._db.get_target_history("script")
    assert [row["id"] for row in history] == [campaign.id]
    assert manager._scheduler.get_resource_summary()["cpu_allocated"] == 0


@pytest.mark.asyncio
async def test_first_launch_failure_drains_the_queue(manager: CampaignManager):
    """One campaign failing to start must not strand the ones behind it."""
    doomed = Campaign(target_name="script")
    queued = Campaign(target_name="script")
    for c in (doomed, queued):
        manager._db.create_campaign(c)
        manager._campaigns[c.id] = c
    manager._scheduler.allocate(doomed.id, doomed.resource_limits)
    manager._scheduler.enqueue(queued.id, queued.priority)

    def launch(campaign, cxxflags, target):
        if campaign.id == doomed.id:
            raise RuntimeError("no such image")
        return "ctr-next"

    manager._docker.launch_container.side_effect = launch

    await manager._start_campaign(doomed)

    assert doomed.state == CampaignState.FAILED
    assert queued.state == CampaignState.RUNNING
    assert queued.container_id == "ctr-next"
    assert manager._scheduler.queue_depth == 0


@pytest.mark.asyncio
async def test_one_bad_campaign_does_not_skip_the_health_sweep(
    manager: CampaignManager,
):
    """A Docker error on one campaign must not abort the whole cycle."""
    broken = Campaign(target_name="script")
    healthy = Campaign(target_name="script")
    for c, ctr in ((broken, "ctr-broken"), (healthy, "ctr-healthy")):
        c.transition_to(CampaignState.STARTING)
        c.transition_to(CampaignState.RUNNING)
        c.container_id = ctr
        manager._db.create_campaign(c)
        manager._campaigns[c.id] = c

    polled: list[str] = []

    def status(container_id):
        polled.append(container_id)
        if container_id == "ctr-broken":
            raise RuntimeError("docker daemon unreachable")
        return {"running": False, "paused": False, "exit_code": 0}

    manager._docker.get_container_status.side_effect = status

    await manager._check_all_health()

    assert polled == ["ctr-broken", "ctr-healthy"]
    assert broken.state == CampaignState.RUNNING
    assert healthy.state == CampaignState.COMPLETED


@pytest.mark.asyncio
async def test_terminal_campaigns_are_evicted_but_still_readable(
    manager: CampaignManager,
):
    """Eviction bounds memory; the DB fallback keeps reads working."""
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.container_id = "ctr-done"
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign
    manager._lifecycle_locks[campaign.id] = asyncio.Lock()

    await manager.stop_campaign(campaign.id)

    assert campaign.id not in manager._campaigns
    assert campaign.id not in manager._lifecycle_locks

    loaded = await manager.get_campaign(campaign.id)
    assert loaded is not None
    assert loaded.state == CampaignState.COMPLETED

    # Stopping again reports the real conflict rather than a bogus 404.
    with pytest.raises(ValueError):
        await manager.stop_campaign(campaign.id)


@pytest.mark.asyncio
async def test_held_lifecycle_lock_is_not_evicted(manager: CampaignManager):
    """Dropping a held lock would let two coroutines share one section.

    ``_lock_for`` mints a new lock on a miss, so evicting one that
    another task still owns silently destroys the mutual exclusion the
    lock exists to provide.
    """
    campaign = Campaign(target_name="script")
    campaign.transition_to(CampaignState.STARTING)
    campaign.transition_to(CampaignState.RUNNING)
    campaign.transition_to(CampaignState.COMPLETED)
    manager._db.create_campaign(campaign)
    manager._campaigns[campaign.id] = campaign

    lock = manager._lock_for(campaign.id)
    async with lock:
        manager._evict_terminal(campaign)

    assert campaign.id not in manager._campaigns
    assert manager._lifecycle_locks[campaign.id] is lock


@pytest.mark.asyncio
async def test_max_retries_falls_back_to_config(tmp_path: Path):
    """ORCHESTRATOR max_retries was a dead store; it is now the default."""
    db = Database(tmp_path / "retries.db")
    db.connect()
    try:
        mgr = CampaignManager(
            db=db,
            docker_mgr=MagicMock(),
            scheduler=ResourceScheduler(
                total_cpu_quota=800_000, total_memory_mb=16384
            ),
            crash_watcher=CrashWatcher(
                data_dir=tmp_path / "data", poll_interval=60
            ),
            target_registry={
                "script": FuzzTarget(name="script", cxxflags="", fuzz="script")
            },
            data_dir=tmp_path / "data",
            max_retries=7,
            health_poll_interval=60,
        )
        mgr._docker.launch_container.return_value = "ctr-retry"

        defaulted = await mgr.launch_campaign(CampaignCreate(target_name="script"))
        explicit = await mgr.launch_campaign(
            CampaignCreate(target_name="script", max_retries=2)
        )

        assert defaulted.max_retries == 7
        assert explicit.max_retries == 2
        assert db.get_campaign(defaulted.id).max_retries == 7
    finally:
        db.close()


@pytest.mark.asyncio
async def test_active_count_matches_dashboard_states(manager: CampaignManager):
    running = Campaign(target_name="script")
    running.transition_to(CampaignState.STARTING)
    running.transition_to(CampaignState.RUNNING)
    queued = Campaign(target_name="script")
    manager._campaigns[running.id] = running
    manager._campaigns[queued.id] = queued

    assert manager.active_campaign_count == 1
    assert running.state in ACTIVE_STATES
    assert queued.state not in ACTIVE_STATES

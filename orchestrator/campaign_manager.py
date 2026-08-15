"""Campaign lifecycle manager — ties all orchestrator components together.

Owns the state machine, Docker integration, resource scheduler, crash
watcher, and database persistence.  Provides the high-level API that
the FastAPI routes call.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .crash_watcher import CrashWatcher
from .database import Database
from .docker_manager import DockerManager
from .models import (
    ACTIVE_STATES,
    Campaign,
    CampaignCreate,
    CampaignState,
    ResourceLimits,
    TERMINAL_STATES,
)
from .scheduler import ResourceScheduler
from .targets import FuzzTarget

logger = logging.getLogger(__name__)


class CampaignManager:
    """Central campaign lifecycle controller.

    Coordinates between all sub-systems:
    - ``DockerManager`` for container operations
    - ``ResourceScheduler`` for resource accounting and queueing
    - ``CrashWatcher`` for new-crash detection
    - ``Database`` for persistence
    - ``MetricsCollector`` / ``MetricsAgent`` (optional)

    Background tasks:
    - Health monitor: polls container status every N seconds
    - Crash watcher: polls crash directories
    """

    def __init__(
        self,
        db: Database,
        docker_mgr: DockerManager,
        scheduler: ResourceScheduler,
        crash_watcher: CrashWatcher,
        target_registry: dict[str, FuzzTarget],
        data_dir: Path,
        health_poll_interval: int = 5,
        max_retries: int = 3,
        collector=None,       # Optional[MetricsCollector]
        docker_client=None,   # raw docker-py client for MetricsAgent
    ) -> None:
        self._db = db
        self._docker = docker_mgr
        self._scheduler = scheduler
        self._crash_watcher = crash_watcher
        self._targets = target_registry
        self._data_dir = data_dir
        self._health_interval = health_poll_interval
        # Default retry cap for campaigns created without an explicit one.
        # The per-campaign value on the Campaign is what the retry loop
        # actually reads.
        self._max_retries = max_retries
        self._collector = collector
        self._docker_client = docker_client

        # In-memory campaign cache for fast access
        self._campaigns: dict[str, Campaign] = {}

        # Per-campaign MetricsAgent instances
        self._agents: dict[str, object] = {}  # campaign_id -> MetricsAgent

        # Serialise stop vs post-launch adopt so a DELETE during
        # ``launch_container`` cannot leave an untracked container.
        self._lifecycle_locks: dict[str, asyncio.Lock] = {}

        # Background tasks
        self._health_task: Optional[asyncio.Task] = None

    # -- startup / shutdown --------------------------------------------------

    async def start(self) -> None:
        """Initialise the manager: reload state from DB, start background tasks."""
        # Reload active campaigns from database
        interrupted: list[Campaign] = []
        for state in [
            CampaignState.RUNNING,
            CampaignState.STARTING,
            CampaignState.PAUSED,
            CampaignState.QUEUED,
            CampaignState.RESTARTING,
            CampaignState.CRASHED,
        ]:
            campaigns = await asyncio.to_thread(
                self._db.list_campaigns, state=state, limit=None
            )
            for c in campaigns:
                self._campaigns[c.id] = c
                if c.state in {CampaignState.RUNNING, CampaignState.PAUSED}:
                    self._scheduler.allocate(c.id, c.resource_limits)
                    self._crash_watcher.register(c.id, c.target_name)
                elif c.state == CampaignState.QUEUED:
                    # Re-enqueue so _process_queue can pick them up after resources free.
                    # Bug fix: previously QUEUED campaigns were loaded into _campaigns but
                    # never placed back into the scheduler queue, leaving them stuck forever.
                    self._scheduler.enqueue(c.id, c.priority)
                elif c.state in {
                    CampaignState.STARTING,
                    CampaignState.CRASHED,
                    CampaignState.RESTARTING,
                }:
                    interrupted.append(c)

        logger.info(
            "Campaign manager started: %d active campaigns loaded",
            len(self._campaigns),
        )

        # Resume campaigns left mid-transition across an orchestrator restart.
        for campaign in interrupted:
            try:
                await self._recover_campaign(campaign)
            except Exception:
                logger.exception(
                    "Failed to recover campaign %s from state %s",
                    campaign.id,
                    campaign.state.value,
                )

        # Reattach metrics for campaigns that still have a live container.
        # Recovery may already have started an agent; skip those.
        for campaign in self._campaigns.values():
            if (
                campaign.state in {CampaignState.RUNNING, CampaignState.PAUSED}
                and campaign.container_id
                and campaign.id not in self._agents
            ):
                await self._start_metrics_agent(campaign)

        # QUEUED campaigns were re-enqueued above. Drain the queue now;
        # otherwise they wait until some other campaign finalises.
        await self._process_queue()

        # Start background tasks
        self._crash_watcher.start()
        self._health_task = asyncio.create_task(self._health_monitor_loop())

    async def stop(self) -> None:
        """Shut down background tasks gracefully."""
        self._crash_watcher.stop()
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        # Stop all running metrics agents
        for agent in list(self._agents.values()):
            try:
                await agent.stop()
            except Exception:
                pass
        self._agents.clear()

        logger.info("Campaign manager stopped")

    # -- campaign operations -------------------------------------------------

    async def launch_campaign(self, request: CampaignCreate) -> Campaign:
        """Create and launch (or queue) a new campaign.

        Returns the Campaign object.
        Raises ValueError if the target name is not in the registry.
        """
        target = self._targets.get(request.target_name)
        if target is None:
            raise ValueError(f"Unknown target: {request.target_name}")

        limits = ResourceLimits(
            cpu_quota=request.cpu_quota,
            memory_mb=request.memory_mb,
        )

        campaign = Campaign(
            target_name=request.target_name,
            resource_limits=limits,
            priority=request.priority,
            # Falls back to the orchestrator-wide setting when the request
            # omits it, which is what ORCHESTRATOR max_retries configures.
            max_retries=(
                self._max_retries
                if request.max_retries is None
                else request.max_retries
            ),
            max_duration_seconds=request.max_duration_seconds,
            modules=request.modules or target.modules,
            env_overrides={**target.env_overrides, **request.env_overrides},
            image_tag=f"bitcoinfuzz:{request.target_name}",
        )

        # Persist to DB
        await asyncio.to_thread(self._db.create_campaign, campaign)
        self._campaigns[campaign.id] = campaign

        # Try to launch immediately or queue
        if self._scheduler.can_launch(limits):
            await self._start_campaign(campaign)
        else:
            self._scheduler.enqueue(campaign.id, campaign.priority)
            logger.info(
                "Campaign %s queued (insufficient resources)", campaign.id
            )

        return campaign

    async def stop_campaign(self, campaign_id: str) -> Campaign:
        """Stop a running campaign and archive it.

        Raises KeyError if not found, ValueError if already terminal.
        """
        campaign = await self._get_campaign_or_raise(campaign_id)

        async with self._lock_for(campaign_id):
            if campaign.state in TERMINAL_STATES:
                raise ValueError(
                    f"Campaign {campaign_id} is already in terminal state: {campaign.state.value}"
                )

            # If queued, just remove from queue
            if campaign.state == CampaignState.QUEUED:
                self._scheduler.remove_from_queue(campaign_id)
                campaign.transition_to(CampaignState.COMPLETED)
                campaign.exit_reason = "stopped"
            else:
                # Stop the container
                if campaign.container_id:
                    await asyncio.to_thread(
                        self._docker.stop_container, campaign.container_id
                    )
                campaign.transition_to(CampaignState.COMPLETED)
                campaign.exit_reason = "stopped"

        await self._finalise_campaign(campaign)
        return campaign

    async def pause_campaign(self, campaign_id: str) -> Campaign:
        """Pause a running campaign (SIGSTOP the container)."""
        campaign = await self._get_campaign_or_raise(campaign_id)

        if campaign.state != CampaignState.RUNNING:
            raise ValueError(
                f"Can only pause RUNNING campaigns, got: {campaign.state.value}"
            )

        if campaign.container_id:
            await asyncio.to_thread(
                self._docker.pause_container, campaign.container_id
            )

        campaign.transition_to(CampaignState.PAUSED)
        await asyncio.to_thread(self._db.update_campaign, campaign)
        return campaign

    async def resume_campaign(self, campaign_id: str) -> Campaign:
        """Resume a paused campaign (SIGCONT the container)."""
        campaign = await self._get_campaign_or_raise(campaign_id)

        if campaign.state != CampaignState.PAUSED:
            raise ValueError(
                f"Can only resume PAUSED campaigns, got: {campaign.state.value}"
            )

        if campaign.container_id:
            await asyncio.to_thread(
                self._docker.resume_container, campaign.container_id
            )

        campaign.transition_to(CampaignState.RUNNING)
        await asyncio.to_thread(self._db.update_campaign, campaign)
        return campaign

    async def get_campaign(self, campaign_id: str) -> Optional[Campaign]:
        """Retrieve a campaign by ID."""
        return self._campaigns.get(campaign_id) or await asyncio.to_thread(
            self._db.get_campaign, campaign_id
        )

    async def list_campaigns(
        self,
        state: Optional[CampaignState] = None,
        target_name: Optional[str] = None,
    ) -> list[Campaign]:
        """List campaigns with optional filters."""
        return await asyncio.to_thread(
            self._db.list_campaigns, state=state, target_name=target_name
        )

    async def get_campaign_logs(
        self, campaign_id: str, tail: int = 100
    ) -> list[str]:
        """Return recent log lines from a campaign's container."""
        campaign = await self._get_campaign_or_raise(campaign_id)
        if not campaign.container_id:
            return []
        lines = list(
            await asyncio.to_thread(
                lambda: list(
                    self._docker.get_container_logs(
                        campaign.container_id, tail=tail
                    )
                )
            )
        )
        return lines

    async def get_campaign_crashes(self, campaign_id: str) -> list[dict]:
        """Return crash records for a campaign."""
        return await asyncio.to_thread(
            self._db.get_crashes, campaign_id=campaign_id
        )

    # -- internal lifecycle --------------------------------------------------

    async def _start_campaign(self, campaign: Campaign) -> None:
        """Move a campaign from QUEUED to RUNNING via Docker launch."""
        try:
            async with self._lock_for(campaign.id):
                if campaign.state in TERMINAL_STATES:
                    return
                if campaign.state != CampaignState.STARTING:
                    campaign.transition_to(CampaignState.STARTING)
                    await asyncio.to_thread(self._db.update_campaign, campaign)

            target = self._targets.get(campaign.target_name)
            cxxflags = target.cxxflags if target else ""

            container_id = await asyncio.to_thread(
                self._docker.launch_container,
                campaign,
                cxxflags,
                campaign.target_name,
            )
            if not await self._claim_launched_container(
                campaign, container_id, allocate=True
            ):
                return

            await self._start_metrics_agent(campaign)

        except Exception as exc:
            if campaign.state in TERMINAL_STATES:
                return
            logger.error(
                "Failed to start campaign %s: %s", campaign.id, exc
            )
            campaign.transition_to(CampaignState.FAILED)
            campaign.exit_reason = f"start_failed: {exc}"
            # Same finalisation as the restart path. Persisting the row
            # alone left the resources allocated, the metrics agent
            # running, and the queue undrained, so everything waiting
            # behind this campaign stalled until some other campaign
            # happened to finish.
            await self._finalise_campaign(campaign)

    async def _restart_campaign(self, campaign: Campaign) -> None:
        """Attempt to restart a crashed campaign."""
        if campaign.retry_count >= campaign.max_retries:
            logger.warning(
                "Campaign %s exceeded max retries (%d), marking FAILED",
                campaign.id,
                campaign.max_retries,
            )
            campaign.transition_to(CampaignState.FAILED)
            campaign.exit_reason = "max_retries_exceeded"
            await self._finalise_campaign(campaign)
            return

        campaign.retry_count += 1
        campaign.transition_to(CampaignState.RESTARTING)
        await asyncio.to_thread(self._db.update_campaign, campaign)

        # Clean up old container
        if campaign.container_id:
            await asyncio.to_thread(
                self._docker.remove_container, campaign.container_id
            )
            campaign.container_id = None

        logger.info(
            "Restarting campaign %s (attempt %d/%d)",
            campaign.id,
            campaign.retry_count,
            campaign.max_retries,
        )

        campaign.transition_to(CampaignState.STARTING)
        await self._start_campaign_container(campaign)

    async def _start_campaign_container(self, campaign: Campaign) -> None:
        """Launch the container (used by both start and restart paths)."""
        try:
            if campaign.state in TERMINAL_STATES:
                return
            target = self._targets.get(campaign.target_name)
            cxxflags = target.cxxflags if target else ""

            container_id = await asyncio.to_thread(
                self._docker.launch_container,
                campaign,
                cxxflags,
                campaign.target_name,
            )
            if not await self._claim_launched_container(
                campaign, container_id, allocate=False
            ):
                return

            # Restart metrics agent
            await self._start_metrics_agent(campaign)

        except Exception as exc:
            if campaign.state in TERMINAL_STATES:
                return
            logger.error("Restart failed for campaign %s: %s", campaign.id, exc)
            campaign.transition_to(CampaignState.FAILED)
            campaign.exit_reason = f"restart_failed: {exc}"
            await self._finalise_campaign(campaign)

    async def _finalise_campaign(self, campaign: Campaign) -> None:
        """Archive a terminal campaign and release its resources."""
        self._scheduler.deallocate(campaign.id)
        self._crash_watcher.unregister(campaign.id)

        target = self._targets.get(campaign.target_name)
        cxxflags = target.cxxflags if target else ""

        await asyncio.to_thread(
            self._db.archive_campaign, campaign, cxxflags
        )
        await asyncio.to_thread(self._db.update_campaign, campaign)

        await self._stop_metrics_agent(campaign)

        self._evict_terminal(campaign)

        # Try to launch queued campaigns now that resources are free
        await self._process_queue()

        logger.info(
            "Campaign %s finalised (state=%s, reason=%s)",
            campaign.id,
            campaign.state.value,
            campaign.exit_reason,
        )

    def _evict_terminal(self, campaign: Campaign) -> None:
        """Drop a finished campaign's in-memory state.

        Both dicts otherwise grow one entry per campaign ever run, for the
        life of the process. Reads still resolve: ``get_campaign`` and
        ``_get_campaign_or_raise`` both fall back to the database.

        The lock is the delicate half. ``_lock_for`` mints a fresh
        ``asyncio.Lock`` on a miss, so dropping one another task still
        holds would let the next caller build a second lock and put two
        coroutines inside the same critical section. Only drop it when
        provably idle: a lock left behind is a small leak, a lock removed
        too early is a correctness bug. Every caller reaches here outside
        the campaign's own critical section, so a held lock means some
        other task owns it and this entry stays until a later sweep.
        """
        if campaign.state not in TERMINAL_STATES:
            return
        self._campaigns.pop(campaign.id, None)
        lock = self._lifecycle_locks.get(campaign.id)
        if lock is not None and not lock.locked():
            self._lifecycle_locks.pop(campaign.id, None)

    async def _process_queue(self) -> None:
        """Try to launch queued campaigns when resources become available."""
        eligible = self._scheduler.dequeue_eligible()
        for cid in eligible:
            campaign = self._campaigns.get(cid)
            if campaign is None:
                continue
            if self._scheduler.can_launch(campaign.resource_limits):
                await self._start_campaign(campaign)
            else:
                # Re-enqueue if still not enough resources
                self._scheduler.enqueue(cid, campaign.priority)

    # -- metrics agent helpers -----------------------------------------------

    async def _recover_campaign(self, campaign: Campaign) -> None:
        """Resume a campaign left in STARTING, CRASHED, or RESTARTING."""
        logger.info(
            "Recovering campaign %s from state %s",
            campaign.id,
            campaign.state.value,
        )
        if campaign.state == CampaignState.STARTING:
            await self._recover_starting(campaign)
        elif campaign.state == CampaignState.CRASHED:
            self._scheduler.allocate(campaign.id, campaign.resource_limits)
            self._crash_watcher.register(campaign.id, campaign.target_name)
            await self._restart_campaign(campaign)
        elif campaign.state == CampaignState.RESTARTING:
            self._scheduler.allocate(campaign.id, campaign.resource_limits)
            self._crash_watcher.register(campaign.id, campaign.target_name)
            campaign.transition_to(CampaignState.STARTING)
            await asyncio.to_thread(self._db.update_campaign, campaign)
            await self._start_campaign_container(campaign)

    async def _recover_starting(self, campaign: Campaign) -> None:
        """Attach to a container launched before restart, or relaunch."""
        if campaign.container_id:
            status = await asyncio.to_thread(
                self._docker.get_container_status, campaign.container_id
            )
            if status and (status.get("running") or status.get("paused")):
                campaign.transition_to(CampaignState.RUNNING)
                self._scheduler.allocate(campaign.id, campaign.resource_limits)
                self._crash_watcher.register(campaign.id, campaign.target_name)
                await asyncio.to_thread(self._db.update_campaign, campaign)
                await self._start_metrics_agent(campaign)
                return
            await asyncio.to_thread(
                self._docker.remove_container, campaign.container_id
            )
            campaign.container_id = None

        await self._start_campaign(campaign)

    async def _start_metrics_agent(self, campaign: Campaign) -> None:
        """Create and start a MetricsAgent for a newly-running campaign."""
        if self._collector is None or campaign.container_id is None:
            return

        # Publish the ceiling HighMemoryUsage divides RSS by.
        self._collector.set_memory_limit(
            campaign.target_name,
            campaign.id,
            campaign.resource_limits.memory_mb,
        )

        existing = self._agents.pop(campaign.id, None)
        if existing is not None:
            try:
                await existing.stop(remove_metrics=False)
            except Exception:
                logger.exception(
                    "Error stopping previous metrics agent for campaign %s",
                    campaign.id,
                )

        try:
            from metrics_agent.agent import MetricsAgent  # lazy import
            agent = MetricsAgent(
                campaign_id=campaign.id,
                target_name=campaign.target_name,
                container_id=campaign.container_id,
                data_dir=self._data_dir,
                collector=self._collector,
                docker_client=self._docker_client,
            )
            await agent.start()
            self._agents[campaign.id] = agent
        except Exception:
            logger.exception(
                "Failed to start metrics agent for campaign %s", campaign.id
            )

    async def _stop_metrics_agent(self, campaign: Campaign) -> None:
        """Stop the MetricsAgent for a campaign."""
        agent = self._agents.pop(campaign.id, None)
        if agent is not None:
            try:
                await agent.stop()
            except Exception:
                logger.exception(
                    "Error stopping metrics agent for campaign %s", campaign.id
                )

    # -- health monitoring ---------------------------------------------------

    async def _health_monitor_loop(self) -> None:
        """Background loop that polls container health for all active campaigns."""
        while True:
            try:
                await self._check_all_health()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error in health monitor loop")
            await asyncio.sleep(self._health_interval)

    async def _check_all_health(self) -> None:
        """Check container status for every running campaign.

        Each campaign is isolated. A Docker API error or a failure while
        handling one campaign's crash used to abort the whole sweep, so
        every campaign after it in iteration order went unchecked: their
        duration limits went unenforced and their exits unnoticed. If the
        fault is sticky, the same campaign poisons every subsequent tick
        and the campaigns behind it are never checked again.
        """
        now = datetime.now(timezone.utc)

        for campaign_id, campaign in list(self._campaigns.items()):
            if campaign.state not in {CampaignState.RUNNING, CampaignState.PAUSED}:
                continue
            if not campaign.container_id:
                continue
            try:
                await self._check_campaign_health(campaign, now)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Health check failed for campaign %s; continuing sweep",
                    campaign_id,
                )

    async def _check_campaign_health(
        self, campaign: Campaign, now: datetime
    ) -> None:
        """Enforce the duration limit and react to container state for one campaign.

        Also enforces max_duration_seconds: if a campaign has been running
        longer than its configured limit it is stopped gracefully.
        """
        campaign_id = campaign.id

        # --- max_duration_seconds enforcement ---
        if (
            campaign.max_duration_seconds is not None
            and campaign.max_duration_seconds > 0
            and campaign.started_at is not None
            and campaign.state == CampaignState.RUNNING
        ):
            elapsed = (now - campaign.started_at).total_seconds()
            if elapsed >= campaign.max_duration_seconds:
                logger.info(
                    "Campaign %s reached max duration (%ds), stopping",
                    campaign_id,
                    campaign.max_duration_seconds,
                )
                await self.stop_campaign(campaign_id)
                return

        # --- Docker health check ---
        status = await asyncio.to_thread(
            self._docker.get_container_status, campaign.container_id
        )
        if status is None:
            # Container disappeared
            logger.warning(
                "Container for campaign %s disappeared", campaign_id
            )
            await self._handle_crash(campaign, "container_disappeared")
            return

        if not status["running"] and not status.get("paused", False):
            exit_code = status.get("exit_code", -1)
            oom = status.get("oom_killed", False)
            reason = "oom_killed" if oom else f"exited_code_{exit_code}"

            if exit_code == 0:
                # Normal completion
                campaign.transition_to(CampaignState.COMPLETED)
                campaign.exit_reason = "completed"
                await self._finalise_campaign(campaign)
            else:
                await self._handle_crash(campaign, reason)

    async def _handle_crash(self, campaign: Campaign, reason: str) -> None:
        """Handle a crashed campaign — attempt restart or mark failed."""
        logger.warning(
            "Campaign %s crashed: %s (retry %d/%d)",
            campaign.id,
            reason,
            campaign.retry_count,
            campaign.max_retries,
        )
        campaign.transition_to(CampaignState.CRASHED)
        campaign.exit_reason = reason
        if self._collector is not None:
            self._collector.set_container_status(
                campaign.target_name, campaign.id, False
            )
        await asyncio.to_thread(self._db.update_campaign, campaign)
        await self._restart_campaign(campaign)

    # -- crash alert callback ------------------------------------------------

    async def handle_crash_alert(
        self, campaign_id: str, target_name: str, crash_path: Path
    ) -> None:
        """Called by the CrashWatcher when a new crash file is found."""
        file_size = crash_path.stat().st_size if crash_path.exists() else None
        await asyncio.to_thread(
            self._db.record_crash,
            campaign_id,
            target_name,
            str(crash_path),
            file_size,
        )

        # record_crash already incremented the DB counter. Sync memory
        # from the row; do not write crash_count back via update_campaign.
        # Bookkeeping after the insert must not raise: the watcher retries
        # failed callbacks and would insert a duplicate row.
        try:
            campaign = self._campaigns.get(campaign_id)
            if campaign:
                loaded = await asyncio.to_thread(
                    self._db.get_campaign, campaign_id
                )
                if loaded is not None:
                    campaign.crash_count = loaded.crash_count
            if self._collector is not None:
                self._collector.inc_crash(target_name, campaign_id)
        except Exception:
            logger.exception(
                "Crash recorded but bookkeeping failed: campaign=%s file=%s",
                campaign_id,
                crash_path.name,
            )

        logger.warning(
            "Crash recorded: campaign=%s target=%s file=%s size=%s",
            campaign_id,
            target_name,
            crash_path.name,
            file_size,
        )

    # -- helpers -------------------------------------------------------------

    def _lock_for(self, campaign_id: str) -> asyncio.Lock:
        lock = self._lifecycle_locks.get(campaign_id)
        if lock is None:
            lock = asyncio.Lock()
            self._lifecycle_locks[campaign_id] = lock
        return lock

    async def _claim_launched_container(
        self,
        campaign: Campaign,
        container_id: str,
        *,
        allocate: bool,
    ) -> bool:
        """Adopt a just-launched container, or discard it if the campaign stopped."""
        async with self._lock_for(campaign.id):
            if campaign.state in TERMINAL_STATES:
                logger.warning(
                    "Campaign %s stopped during launch; removing container %s",
                    campaign.id,
                    container_id[:12],
                )
                await asyncio.to_thread(
                    self._docker.remove_container, container_id
                )
                return False
            campaign.container_id = container_id
            campaign.transition_to(CampaignState.RUNNING)
            if allocate:
                self._scheduler.allocate(campaign.id, campaign.resource_limits)
            self._crash_watcher.register(campaign.id, campaign.target_name)
            await asyncio.to_thread(self._db.update_campaign, campaign)
            logger.info(
                "Campaign %s RUNNING (container=%s)",
                campaign.id,
                container_id[:12],
            )
            return True

    async def _get_campaign_or_raise(self, campaign_id: str) -> Campaign:
        """Return a campaign from cache or the database, else raise KeyError.

        The database fallback is what makes evicting terminal campaigns
        safe. Reading only the cache would turn a stop or pause of a
        finished campaign into a spurious 404 instead of the 409 that
        says what actually happened.
        """
        campaign = self._campaigns.get(campaign_id)
        if campaign is None:
            campaign = await asyncio.to_thread(
                self._db.get_campaign, campaign_id
            )
        if campaign is None:
            raise KeyError(f"Campaign not found: {campaign_id}")
        return campaign

    @property
    def active_campaign_count(self) -> int:
        return sum(1 for c in self._campaigns.values() if c.state in ACTIVE_STATES)

    @property
    def resource_summary(self) -> dict:
        return self._scheduler.get_resource_summary()

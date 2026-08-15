"""FastAPI application for the bitcoinfuzz orchestrator.

Provides the complete REST API surface described in proposal §6.1,
including campaign lifecycle management, log streaming via SSE,
real-time metrics via WebSocket, and historical query endpoints.
"""

import asyncio
import logging
import secrets
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

from .campaign_manager import CampaignManager
from .config import OrchestratorConfig, load_default_config
from .crash_watcher import CrashWatcher
from .database import Database
from .docker_manager import DockerManager
from .models import (
    CampaignCreate,
    CampaignState,
    DashboardSummary,
    HealthResponse,
)
from .scheduler import ResourceScheduler
from .scheduled_campaigns import CampaignScheduler, load_schedules
from .targets import build_target_registry

try:
    from metrics_agent.collector import MetricsCollector
    _METRICS_AVAILABLE = True
except ImportError:  # pragma: no cover
    _METRICS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Bind addresses that keep the API off the network. Any other value needs a
# token, because every mutating route reaches the host Docker socket.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

# ---------------------------------------------------------------------------
# App state (populated during lifespan)
# ---------------------------------------------------------------------------

_start_time: float = 0.0
_manager: Optional[CampaignManager] = None
_db: Optional[Database] = None
_campaign_scheduler: Optional[CampaignScheduler] = None
_target_registry: dict = {}


def _make_auth_dependency(config: OrchestratorConfig):
    """Build the bearer-token guard applied to every mutating route.

    With no token configured the guard is a no-op, so a loopback dev run
    needs no ceremony. The startup warning in the lifespan covers the case
    where that combination is paired with a non-loopback bind.
    """

    async def require_token(
        authorization: Optional[str] = Header(default=None),
    ) -> None:
        expected = config.api_token
        if not expected:
            return
        scheme, _, presented = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(
            presented, expected
        ):
            raise HTTPException(
                401,
                "Missing or invalid bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return require_token


# Seconds between log-window polls when following a campaign's output.
# Also the worst-case delay before a disconnected client's worker thread
# is handed back to the executor.
_LOG_POLL_INTERVAL = 1.0


async def _stream_campaign_logs(
    campaign_id: str, tail: int
) -> AsyncGenerator[dict, None]:
    """Yield SSE log events for a campaign, following the container.

    Polls disjoint ``(since, until]`` windows instead of holding a
    blocking follow stream. Every ``to_thread`` call here returns at the
    end of its window, so when the client disconnects and this generator
    is cancelled, the worker thread is released at the next boundary.

    A follow stream cannot do that: ``next()`` on it parks the worker
    until the container exits, the thread comes from the default executor
    shared with database writes, health polls and campaign launches, and
    closing the generator from another thread raises
    ``ValueError: generator already executing``. Enough disconnects and
    the whole service stalls, not just log streaming.
    """
    campaign = await _manager.get_campaign(campaign_id)
    if not campaign or not campaign.container_id:
        return
    container_id = campaign.container_id
    docker = _manager._docker

    # Seed with the requested tail, then follow from this instant so the
    # first window cannot repeat a line the backlog already carried.
    cursor = time.time()
    backlog = await asyncio.to_thread(
        lambda: list(docker.get_container_logs(container_id, tail=tail))
    )
    for line in backlog:
        yield {"event": "log", "data": line}

    while True:
        await asyncio.sleep(_LOG_POLL_INTERVAL)

        # Sampled before the window so a container that exits mid-poll
        # still gets its final lines drained below.
        status = await asyncio.to_thread(
            docker.get_container_status, container_id
        )
        running = bool(status and status.get("running"))

        until = time.time()
        if until > cursor:
            lines = await asyncio.to_thread(
                docker.get_log_window, container_id, cursor, until
            )
            cursor = until
            for line in lines:
                yield {"event": "log", "data": line}

        if not running:
            break


def _make_lifespan(config: OrchestratorConfig):
    """Build a lifespan closure bound to a specific config instance."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator:
        """Initialise subsystems on startup; tear them down on shutdown."""
        global _start_time, _manager, _db, _campaign_scheduler, _target_registry

        _start_time = time.time()

        if not config.api_token and config.host not in _LOOPBACK_HOSTS:
            logger.warning(
                "ORCHESTRATOR_API_TOKEN is unset while bound to %s — the "
                "campaign routes can launch and stop containers on this "
                "Docker host and are reachable by anyone who can route to "
                "it. Set a token or bind 127.0.0.1.",
                config.host,
            )

        # --- Database ---
        db = Database(config.db_path)
        await asyncio.to_thread(db.connect)
        _db = db

        # --- Target registry ---
        if not config.compose_file.exists():
            logger.error(
                "Compose file not found: %s — set ORCHESTRATOR_COMPOSE_FILE "
                "to the bitcoinfuzz docker-compose.yml that defines FUZZ targets",
                config.compose_file,
            )
            _target_registry = {}
        else:
            _target_registry = build_target_registry(config.compose_file)
            logger.info(
                "Loaded %d targets from %s",
                len(_target_registry),
                config.compose_file,
            )

        # --- Metrics collector ---
        collector = None
        if _METRICS_AVAILABLE:
            collector = MetricsCollector()
            if config.metrics_port > 0:
                try:
                    collector.start_server(config.metrics_port)
                    logger.info(
                        "Metrics server started on port %d", config.metrics_port
                    )
                except Exception as exc:
                    logger.warning("Could not start metrics server: %s", exc)
            else:
                logger.info("Metrics HTTP server disabled (metrics_port<=0)")

        # --- Docker manager ---
        try:
            docker_mgr = DockerManager(data_dir=str(config.bind_data_dir))
            logger.info(
                "Campaign bind-mount source (Docker daemon): %s",
                config.bind_data_dir,
            )
            import docker as _docker_sdk

            docker_client = _docker_sdk.from_env()
            docker_client.ping()
        except Exception as exc:
            logger.warning(
                "Docker not available — running in API-only mode: %s", exc
            )
            docker_mgr = None  # type: ignore[assignment]
            docker_client = None

        # --- Resource scheduler ---
        scheduler = ResourceScheduler()

        # --- Crash watcher ---
        crash_watcher = CrashWatcher(
            data_dir=config.data_dir,
            poll_interval=config.crash_poll_interval,
        )

        # --- Campaign manager ---
        if docker_mgr is not None:
            manager = CampaignManager(
                db=db,
                docker_mgr=docker_mgr,
                scheduler=scheduler,
                crash_watcher=crash_watcher,
                target_registry=_target_registry,
                data_dir=config.data_dir,
                health_poll_interval=config.health_poll_interval,
                max_retries=config.max_retries,
                collector=collector,
                docker_client=docker_client,
            )
            crash_watcher._alert_callback = manager.handle_crash_alert
            await manager.start()
            _manager = manager
        else:
            _manager = None

        # --- Scheduled campaigns ---
        if config.schedule_file and config.schedule_file.exists() and _manager:
            entries = load_schedules(config.schedule_file)
            cs = CampaignScheduler()
            cs.configure(entries, _manager.launch_campaign)
            cs.start()
            _campaign_scheduler = cs

        logger.info("Orchestrator ready on %s:%d", config.host, config.port)
        yield

        # --- Shutdown ---
        if _campaign_scheduler:
            _campaign_scheduler.stop()
        if _manager:
            await _manager.stop()
        if _db:
            await asyncio.to_thread(_db.close)
        logger.info("Orchestrator shut down")

    return lifespan


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def build_app(config: Optional[OrchestratorConfig] = None) -> FastAPI:
    """Create the FastAPI application with all routes.

    Pass *config* to override env/defaults (tests and shared deployments).
    When omitted, settings are loaded from the environment at call time —
    not at import time — so ``ORCHESTRATOR_*`` env vars always apply.
    """
    cfg = config or load_default_config()
    app = FastAPI(
        title="bitcoinfuzz orchestrator",
        description="Campaign orchestration and monitoring for bitcoinfuzz",
        version="0.1.0",
        lifespan=_make_lifespan(cfg),
    )
    app.state.orchestrator_config = cfg

    # CORS. Credentials stay off so Starlette can answer with a literal "*"
    # only for the origins named here; pairing "*" with credentials makes
    # Starlette echo the caller's Origin instead, which lets any page a
    # logged-in operator visits drive the campaign routes.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.cors_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
    )

    # Applied to routes that launch, stop, or otherwise mutate campaigns.
    auth = [Depends(_make_auth_dependency(cfg))]

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------

    @app.get("/health", response_model=HealthResponse, tags=["system"])
    def health() -> HealthResponse:
        active = _manager.active_campaign_count if _manager else 0
        return HealthResponse(
            status="ok",
            version="0.1.0",
            active_campaigns=active,
            uptime_seconds=round(time.time() - _start_time, 1),
        )

    # -----------------------------------------------------------------------
    # Targets
    # -----------------------------------------------------------------------

    @app.get("/targets", tags=["targets"])
    def list_targets() -> list[dict]:
        """List all registered fuzz targets from docker-compose.yml."""
        return [t.to_dict() for t in _target_registry.values()]

    @app.get("/targets/{name}", tags=["targets"])
    def get_target(name: str) -> dict:
        """Get a single target by name."""
        target = _target_registry.get(name)
        if target is None:
            raise HTTPException(404, f"Target not found: {name}")
        return target.to_dict()

    @app.get("/targets/{name}/history", tags=["targets"])
    async def target_history(
        name: str,
        limit: int = Query(10, ge=1, le=100),
    ) -> list[dict]:
        """Historical campaign summaries for a target."""
        if name not in _target_registry:
            raise HTTPException(404, f"Target not found: {name}")
        return await asyncio.to_thread(_db.get_target_history, name, limit)

    # -----------------------------------------------------------------------
    # Campaigns
    # -----------------------------------------------------------------------

    @app.post(
        "/campaigns", status_code=201, tags=["campaigns"], dependencies=auth
    )
    async def create_campaign(request: CampaignCreate) -> dict:
        """Launch a new fuzzing campaign (or queue it if resources are full)."""
        if _manager is None:
            raise HTTPException(503, "Docker not available")
        try:
            campaign = await _manager.launch_campaign(request)
            return campaign.to_dict()
        except ValueError as exc:
            raise HTTPException(400, str(exc))

    @app.get("/campaigns", tags=["campaigns"])
    async def list_campaigns(
        state: Optional[str] = None,
        target_name: Optional[str] = None,
    ) -> list[dict]:
        """List all campaigns with optional state/target filter."""
        try:
            state_enum = CampaignState(state) if state else None
        except ValueError:
            raise HTTPException(400, f"Invalid state: {state}")
        if _manager:
            campaigns = await _manager.list_campaigns(
                state=state_enum, target_name=target_name
            )
        elif _db:
            campaigns = await asyncio.to_thread(
                _db.list_campaigns, state=state_enum, target_name=target_name
            )
        else:
            campaigns = []
        return [c.to_dict() for c in campaigns]

    @app.get("/campaigns/{campaign_id}", tags=["campaigns"])
    async def get_campaign(campaign_id: str) -> dict:
        """Get details for a single campaign."""
        campaign = None
        if _manager:
            campaign = await _manager.get_campaign(campaign_id)
        elif _db:
            campaign = await asyncio.to_thread(_db.get_campaign, campaign_id)
        if campaign is None:
            raise HTTPException(404, f"Campaign not found: {campaign_id}")
        return campaign.to_dict()

    @app.delete("/campaigns/{campaign_id}", tags=["campaigns"], dependencies=auth)
    async def delete_campaign(campaign_id: str) -> dict:
        """Stop and remove a campaign."""
        if _manager is None:
            raise HTTPException(503, "Docker not available")
        try:
            campaign = await _manager.stop_campaign(campaign_id)
            return campaign.to_dict()
        except KeyError:
            raise HTTPException(404, f"Campaign not found: {campaign_id}")
        except ValueError as exc:
            raise HTTPException(409, str(exc))

    @app.post(
        "/campaigns/{campaign_id}/pause", tags=["campaigns"], dependencies=auth
    )
    async def pause_campaign(campaign_id: str) -> dict:
        """Pause a running campaign (SIGSTOP)."""
        if _manager is None:
            raise HTTPException(503, "Docker not available")
        try:
            campaign = await _manager.pause_campaign(campaign_id)
            return campaign.to_dict()
        except KeyError:
            raise HTTPException(404, f"Campaign not found: {campaign_id}")
        except ValueError as exc:
            raise HTTPException(409, str(exc))

    @app.post(
        "/campaigns/{campaign_id}/resume", tags=["campaigns"], dependencies=auth
    )
    async def resume_campaign(campaign_id: str) -> dict:
        """Resume a paused campaign (SIGCONT)."""
        if _manager is None:
            raise HTTPException(503, "Docker not available")
        try:
            campaign = await _manager.resume_campaign(campaign_id)
            return campaign.to_dict()
        except KeyError:
            raise HTTPException(404, f"Campaign not found: {campaign_id}")
        except ValueError as exc:
            raise HTTPException(409, str(exc))

    @app.get("/campaigns/{campaign_id}/logs", tags=["campaigns"])
    async def campaign_logs(
        campaign_id: str,
        follow: bool = Query(False),
        tail: int = Query(100, ge=1, le=10000),
    ):
        """Stream container logs.  Use follow=true for SSE streaming."""
        if _manager is None:
            raise HTTPException(503, "Docker not available")

        try:
            if follow:
                return EventSourceResponse(
                    _stream_campaign_logs(campaign_id, tail)
                )
            else:
                lines = await _manager.get_campaign_logs(campaign_id, tail=tail)
                return {"logs": lines}
        except KeyError:
            raise HTTPException(404, f"Campaign not found: {campaign_id}")

    @app.get("/campaigns/{campaign_id}/crashes", tags=["campaigns"])
    async def campaign_crashes(campaign_id: str) -> list[dict]:
        """List crash artefacts for a campaign."""
        if _manager:
            return await _manager.get_campaign_crashes(campaign_id)
        elif _db:
            return await asyncio.to_thread(
                _db.get_crashes, campaign_id=campaign_id
            )
        return []

    # -----------------------------------------------------------------------
    # Dashboard
    # -----------------------------------------------------------------------

    @app.get("/dashboard/summary", response_model=DashboardSummary, tags=["dashboard"])
    async def dashboard_summary() -> DashboardSummary:
        """Aggregate stats for the dashboard overview."""
        stats = await asyncio.to_thread(_db.get_dashboard_stats) if _db else {}
        resources = _manager.resource_summary if _manager else {}
        return DashboardSummary(
            active_campaigns=stats.get("active_campaigns", 0),
            queued_campaigns=stats.get("queued_campaigns", 0),
            completed_campaigns=stats.get("completed_campaigns", 0),
            failed_campaigns=stats.get("failed_campaigns", 0),
            total_crashes=stats.get("total_crashes", 0),
            total_targets=len(_target_registry),
            cpu_allocated=resources.get("cpu_allocated", 0),
            cpu_total=resources.get("cpu_total", 0),
            memory_allocated_mb=resources.get("memory_allocated_mb", 0),
            memory_total_mb=resources.get("memory_total_mb", 0),
        )

    @app.get("/dashboard/resources", tags=["dashboard"])
    def resource_utilization() -> dict:
        """Current resource allocation vs. host capacity."""
        if _manager:
            return _manager.resource_summary
        return {}

    # -----------------------------------------------------------------------
    # Historical comparison
    # -----------------------------------------------------------------------

    @app.get("/targets/{name}/compare", tags=["targets"])
    async def compare_campaigns(
        name: str,
        campaign_ids: str = Query(..., description="Comma-separated campaign IDs"),
    ) -> list[dict]:
        """Compare historical stats across multiple campaigns for a target."""
        if name not in _target_registry:
            raise HTTPException(404, f"Target not found: {name}")
        ids = [cid.strip() for cid in campaign_ids.split(",") if cid.strip()]
        if not ids:
            raise HTTPException(400, "No campaign IDs provided")
        rows = await asyncio.to_thread(_db.compare_campaigns, ids)
        mismatched = [
            row["id"] for row in rows if row.get("target_name") != name
        ]
        if mismatched:
            raise HTTPException(
                400,
                f"Campaign IDs not in target {name}: {', '.join(mismatched)}",
            )
        return rows

    # -----------------------------------------------------------------------
    # Alerting webhook (receives from Alertmanager)
    # -----------------------------------------------------------------------

    @app.post("/api/alerts/webhook", tags=["alerts"], dependencies=auth)
    async def alerts_webhook(payload: dict) -> dict:
        """Receive alerts from Alertmanager and log them."""
        alerts = payload.get("alerts", [])
        for alert in alerts:
            logger.warning(
                "Alert received: status=%s name=%s summary=%s",
                alert.get("status"),
                alert.get("labels", {}).get("alertname"),
                alert.get("annotations", {}).get("summary"),
            )
        return {"status": "received", "count": len(alerts)}

    # -----------------------------------------------------------------------
    # CI/CD webhook (receives from GitHub Actions)
    # -----------------------------------------------------------------------

    @app.post("/api/ci-hook", tags=["ci"], dependencies=auth)
    async def ci_hook(payload: dict) -> dict:
        """Trigger campaigns from CI (e.g. on main branch push)."""
        commit = payload.get("commit", "unknown")
        trigger = payload.get("trigger", "unknown")
        logger.info("CI hook received: commit=%s trigger=%s", commit, trigger)
        # TODO: optionally launch a full campaign round
        return {"status": "acknowledged", "commit": commit, "trigger": trigger}

    # -----------------------------------------------------------------------
    # WebSocket for real-time metrics
    # -----------------------------------------------------------------------

    @app.websocket("/ws/campaigns/{campaign_id}/metrics")
    async def ws_campaign_metrics(
        websocket: WebSocket, campaign_id: str
    ) -> None:
        """WebSocket endpoint for real-time metric streaming."""
        await websocket.accept()
        try:
            while True:
                campaign = None
                if _manager:
                    campaign = await _manager.get_campaign(campaign_id)
                if campaign is None:
                    await websocket.send_json({"error": "Campaign not found"})
                    break

                data = campaign.to_dict()
                # Add resource info
                if _manager:
                    data["resources"] = _manager.resource_summary
                await websocket.send_json(data)
                await asyncio.sleep(5)  # Push every 5 seconds
        except WebSocketDisconnect:
            pass
        except Exception as exc:
            logger.debug("WebSocket error for %s: %s", campaign_id, exc)

    # -----------------------------------------------------------------------
    # Scheduled campaigns info
    # -----------------------------------------------------------------------

    @app.get("/schedules", tags=["schedules"])
    def list_schedules() -> list[dict]:
        """List all configured campaign schedules."""
        if _campaign_scheduler:
            return _campaign_scheduler.get_scheduled_jobs()
        return []

    return app


# Module-level app instance for uvicorn
app = build_app()

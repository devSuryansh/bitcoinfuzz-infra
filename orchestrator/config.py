"""Orchestrator configuration.

Loads settings from environment variables. Works on a single workstation
or a shared team Docker host: all path and bind settings are overridable
via ``ORCHESTRATOR_*`` / ``BITCOINFUZZ_REPO``.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class OrchestratorConfig:
    """Central configuration for the orchestrator service."""

    # Paths
    repo_root: Path = field(default_factory=lambda: Path(__file__).resolve().parent.parent)
    compose_file: Path = field(default=None)  # type: ignore[assignment]
    data_dir: Path = field(default=None)  # type: ignore[assignment]
    # Path the *host* Docker daemon uses for campaign bind-mounts.
    # Distinct from data_dir when the orchestrator runs in a container:
    # data_dir is /app/data inside the container, but the daemon only
    # sees host paths. None means "same as data_dir" (local / API-only).
    host_data_dir: Optional[Path] = None
    db_path: Path = field(default=None)  # type: ignore[assignment]

    # Campaign lifecycle
    max_retries: int = 3
    health_poll_interval: int = 5  # seconds between container health checks
    crash_poll_interval: int = 10  # seconds between crash directory scans

    # Scheduling
    schedule_file: Optional[Path] = None  # YAML file with cron schedules

    # Server. Loopback by default: the mutating routes launch and stop
    # containers on the Docker host, so binding anywhere else without
    # ``api_token`` set hands that control to the whole network.
    host: str = "127.0.0.1"
    port: int = 8000

    # Security
    # Bearer token required on every mutating route. None disables the
    # check entirely, which is only defensible on a loopback bind.
    api_token: Optional[str] = None
    # Browser origins permitted to call the API. Never widen this to "*"
    # while any form of ambient credential is in play.
    cors_origins: list[str] = field(
        default_factory=lambda: ["http://127.0.0.1:8000", "http://localhost:8000"]
    )

    # Prometheus (0 disables the HTTP bind; useful in unit tests)
    metrics_port: int = 9091

    def __post_init__(self) -> None:
        if self.compose_file is None:
            # Prefer a sibling bitcoinfuzz checkout's compose when present so
            # local runs register real fuzz targets instead of infra services.
            # Shared hosts should set ORCHESTRATOR_COMPOSE_FILE explicitly.
            sibling = self.repo_root.parent / "bitcoinfuzz" / "docker-compose.yml"
            if sibling.exists():
                self.compose_file = sibling
            else:
                self.compose_file = self.repo_root / "docker-compose.yml"
        if self.data_dir is None:
            sibling_data = self.repo_root.parent / "bitcoinfuzz" / "docker"
            if sibling_data.exists():
                self.data_dir = sibling_data
            else:
                self.data_dir = self.repo_root / "docker"
        if self.db_path is None:
            self.db_path = self.repo_root / "orchestrator.db"
        if self.schedule_file is None:
            candidate = self.repo_root / "orchestrator-schedules.yml"
            if candidate.exists():
                self.schedule_file = candidate

    @property
    def bind_data_dir(self) -> Path:
        """Host path bind-mounted into campaign containers by the Docker daemon.

        Relative values (the sibling-checkout default) are resolved against
        ``repo_root``. The Docker Engine API rejects paths like
        ``../bitcoinfuzz/docker``.
        """
        path = self.host_data_dir if self.host_data_dir is not None else self.data_dir
        if not path.is_absolute():
            return (self.repo_root / path).resolve()
        return path


def load_default_config() -> OrchestratorConfig:
    """Load configuration, preferring environment variables over repo-root defaults.

    The following environment variables are recognised (matching what the
    Dockerfile and docker-compose.yml document):

        ORCHESTRATOR_DATA_DIR      — path to the shared fuzzing data directory
                                     (as seen by this process)
        ORCHESTRATOR_HOST_DATA_DIR — host path the Docker daemon bind-mounts
                                     into campaign containers (Compose must
                                     set this; /app/data is not a host path)
        ORCHESTRATOR_DB_PATH       — path to the SQLite database file
        ORCHESTRATOR_COMPOSE_FILE  — path to the *fuzz-target* docker-compose.yml
                                     (bitcoinfuzz repo), NOT the infra compose
        ORCHESTRATOR_HOST          — API bind host
        ORCHESTRATOR_PORT          — API bind port
        ORCHESTRATOR_METRICS_PORT  — Prometheus metrics exporter port
        ORCHESTRATOR_API_TOKEN     — bearer token required on mutating routes
        ORCHESTRATOR_CORS_ORIGINS  — comma-separated browser origin allow-list
    """
    import os

    config = OrchestratorConfig()
    if val := os.environ.get("ORCHESTRATOR_DATA_DIR"):
        config.data_dir = Path(val)
    if val := os.environ.get("ORCHESTRATOR_HOST_DATA_DIR"):
        config.host_data_dir = Path(val)
    if val := os.environ.get("ORCHESTRATOR_DB_PATH"):
        config.db_path = Path(val)
    if val := os.environ.get("ORCHESTRATOR_COMPOSE_FILE"):
        config.compose_file = Path(val)
    if val := os.environ.get("ORCHESTRATOR_HOST"):
        config.host = val
    if val := os.environ.get("ORCHESTRATOR_PORT"):
        config.port = int(val)
    if val := os.environ.get("ORCHESTRATOR_METRICS_PORT"):
        config.metrics_port = int(val)
    if val := os.environ.get("ORCHESTRATOR_API_TOKEN"):
        config.api_token = val
    if val := os.environ.get("ORCHESTRATOR_CORS_ORIGINS"):
        config.cors_origins = [o.strip() for o in val.split(",") if o.strip()]
    return config

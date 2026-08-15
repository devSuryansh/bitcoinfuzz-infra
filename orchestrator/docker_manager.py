"""Docker SDK integration for managing bitcoinfuzz containers.

Wraps the ``docker-py`` library to provide campaign-specific container
lifecycle operations without modifying the existing Dockerfile or
docker-compose.yml.
"""

import logging
from pathlib import Path
from typing import Any, Generator, Optional

import docker
from docker.errors import APIError, DockerException, NotFound

from metrics_agent.log_lines import iter_log_lines

from .models import Campaign

logger = logging.getLogger(__name__)


class DockerManager:
    """Manages Docker container operations for fuzzing campaigns.

    Uses the Docker Engine API via ``docker-py``.  The manager never
    modifies docker-compose.yml — it creates standalone containers that
    mirror the compose service definition.
    """

    def __init__(self, data_dir: str = "./docker") -> None:
        """
        Args:
            data_dir: Host path to the shared data directory that gets
                      bind-mounted to ``/app/data`` inside containers.
        """
        try:
            self._client = docker.from_env()
            self._client.ping()
            logger.info("Connected to Docker daemon")
        except DockerException as exc:
            logger.error("Failed to connect to Docker: %s", exc)
            raise
        # Docker bind-mount sources must be absolute. A relative path is
        # treated as a named volume and rejected (slashes are illegal).
        bind = Path(data_dir).expanduser()
        self._data_dir = str(bind.resolve() if not bind.is_absolute() else bind)

    # -- container lifecycle -------------------------------------------------

    def launch_container(
        self,
        campaign: Campaign,
        cxxflags: str = "",
        fuzz_target: str = "",
    ) -> str:
        """Create and start a container for the given campaign.

        Returns the Docker container ID.
        """
        target = fuzz_target or campaign.target_name
        image_tag = campaign.image_tag or f"bitcoinfuzz:{target}"

        environment = {
            "FUZZ": target,
            "MODULES": campaign.modules or "",
        }
        if cxxflags:
            environment["CXXFLAGS"] = cxxflags
        # Merge target-specific env overrides (e.g. LIBFUZZ_DETECT_LEAKS=0)
        environment.update(campaign.env_overrides)

        container_name = f"bitcoinfuzz-{campaign.id}-{target}"

        # Resource constraints
        limits = campaign.resource_limits
        kwargs: dict[str, Any] = {
            "image": image_tag,
            "name": container_name,
            "environment": environment,
            "volumes": {
                self._data_dir: {"bind": "/app/data", "mode": "rw"},
            },
            "detach": True,
            "remove": False,  # Keep for log inspection after exit
            "network_disabled": True,  # Security: no network access
        }

        if limits.cpu_quota > 0:
            kwargs["cpu_quota"] = limits.cpu_quota
            kwargs["cpu_period"] = 100_000  # Default 100ms period
        if limits.memory_mb > 0:
            kwargs["mem_limit"] = f"{limits.memory_mb}m"

        try:
            container = self._client.containers.run(**kwargs)
            logger.info(
                "Launched container %s for campaign %s (target=%s)",
                container.short_id,
                campaign.id,
                target,
            )
            return container.id
        except APIError as exc:
            logger.error(
                "Failed to launch container for campaign %s: %s",
                campaign.id,
                exc,
            )
            raise

    def stop_container(
        self, container_id: str, timeout: int = 30
    ) -> None:
        """Gracefully stop a container."""
        try:
            container = self._client.containers.get(container_id)
            container.stop(timeout=timeout)
            logger.info("Stopped container %s", container_id[:12])
        except NotFound:
            logger.warning("Container %s not found (already removed?)", container_id[:12])
        except APIError as exc:
            logger.error("Error stopping container %s: %s", container_id[:12], exc)
            raise

    def pause_container(self, container_id: str) -> None:
        """Pause a running container (SIGSTOP)."""
        try:
            container = self._client.containers.get(container_id)
            container.pause()
            logger.info("Paused container %s", container_id[:12])
        except (NotFound, APIError) as exc:
            logger.error("Error pausing container %s: %s", container_id[:12], exc)
            raise

    def resume_container(self, container_id: str) -> None:
        """Unpause a paused container (SIGCONT)."""
        try:
            container = self._client.containers.get(container_id)
            container.unpause()
            logger.info("Resumed container %s", container_id[:12])
        except (NotFound, APIError) as exc:
            logger.error("Error resuming container %s: %s", container_id[:12], exc)
            raise

    def remove_container(self, container_id: str, force: bool = True) -> None:
        """Remove a container."""
        try:
            container = self._client.containers.get(container_id)
            container.remove(force=force)
            logger.info("Removed container %s", container_id[:12])
        except NotFound:
            pass  # Already gone
        except APIError as exc:
            logger.error("Error removing container %s: %s", container_id[:12], exc)

    # -- status queries ------------------------------------------------------

    def get_container_status(self, container_id: str) -> Optional[dict]:
        """Return container status info or None if not found.

        Returns a dict with keys:
        - ``status``: 'running', 'exited', 'paused', 'created', etc.
        - ``exit_code``: int (only meaningful when status is 'exited')
        - ``oom_killed``: bool
        - ``running``: bool convenience flag
        """
        try:
            container = self._client.containers.get(container_id)
            container.reload()
            state = container.attrs.get("State", {})
            return {
                "status": state.get("Status", "unknown"),
                "exit_code": state.get("ExitCode", -1),
                "oom_killed": state.get("OOMKilled", False),
                "running": state.get("Running", False),
                "paused": state.get("Paused", False),
            }
        except NotFound:
            return None
        except APIError as exc:
            logger.error("Error inspecting container %s: %s", container_id[:12], exc)
            return None

    def get_container_logs(
        self,
        container_id: str,
        tail: int = 100,
    ) -> Generator[str, None, None]:
        """Yield the last *tail* log lines from a container.

        Returns promptly: this never follows the stream. Callers that want
        to keep up with a running container should poll
        ``get_log_window`` instead, which stays cancellable.
        """
        try:
            container = self._client.containers.get(container_id)
            stream = container.logs(
                follow=False,
                stream=True,
                tail=tail,
                timestamps=False,
            )
            yield from iter_log_lines(stream)
        except NotFound:
            logger.warning("Container %s not found for log streaming", container_id[:12])
        except APIError as exc:
            logger.error("Error reading logs from %s: %s", container_id[:12], exc)

    def get_log_window(
        self,
        container_id: str,
        since: float,
        until: float,
    ) -> list[str]:
        """Return log lines stamped within the window ``(since, until]``.

        Both bounds are fractional-second epochs, so consecutive windows
        tile with no gap and no overlap. Every call returns promptly,
        which is the point: a follow stream parks its worker thread
        inside ``next()`` until the container exits, and that thread
        comes from the same default executor every other ``to_thread``
        call shares. Polling bounded windows means a cancelled reader
        frees its worker at the next boundary instead of never.
        """
        try:
            container = self._client.containers.get(container_id)
            raw = container.logs(
                follow=False,
                stream=False,
                since=since,
                until=until,
                timestamps=False,
            )
        except NotFound:
            logger.warning(
                "Container %s not found for log window", container_id[:12]
            )
            return []
        except APIError as exc:
            logger.error(
                "Error reading log window from %s: %s", container_id[:12], exc
            )
            return []
        if not raw:
            return []
        return list(iter_log_lines([raw]))

"""Fuzz target registry — parses docker-compose.yml into typed records.

Extracts target definitions, build args, environment overrides, and
derives sensible resource-limit defaults so the orchestrator knows
what it can launch.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .models import ResourceLimits


@dataclass(frozen=True)
class FuzzTarget:
    """Description of a single fuzz target derived from docker-compose.yml."""

    name: str
    cxxflags: str  # e.g. "-DBITCOIN_CORE -DRUST_BITCOIN"
    fuzz: str  # The FUZZ= env value (usually same as name)

    # Runtime overrides
    modules: Optional[str] = None  # Default MODULES= value
    env_overrides: dict[str, str] = field(default_factory=dict)

    # Resource defaults (can be overridden per-campaign)
    resource_limits: ResourceLimits = field(default_factory=ResourceLimits)
    priority: int = 1

    def to_dict(self) -> dict:
        """Serialise to a JSON-friendly dict."""
        return {
            "name": self.name,
            "cxxflags": self.cxxflags,
            "fuzz": self.fuzz,
            "modules": self.modules,
            "env_overrides": self.env_overrides,
            "resource_limits": {
                "cpu_quota": self.resource_limits.cpu_quota,
                "memory_mb": self.resource_limits.memory_mb,
            },
            "priority": self.priority,
        }


# Services that belong to the infra stack (never launchable fuzz targets).
_INFRA_SERVICE_NAMES = frozenset(
    {"orchestrator", "prometheus", "alertmanager", "grafana"}
)


def load_targets(compose_file: Path) -> list[FuzzTarget]:
    """Parse a bitcoinfuzz ``docker-compose.yml`` into FuzzTarget objects.

    Only services that declare ``build.args.FUZZ`` are treated as fuzz
    targets. Infra services (orchestrator, prometheus, alertmanager, …)
    are skipped even if they appear in the same compose file.

    Extracts:
    - Target name from the service key
    - CXXFLAGS and FUZZ from build args
    - Environment overrides (LIBFUZZ_DETECT_LEAKS, ASAN_OPTIONS, etc.)
    - MODULES default value
    """
    with compose_file.open("r", encoding="utf-8") as handle:
        compose = yaml.safe_load(handle) or {}

    services = compose.get("services", {}) or {}
    targets: list[FuzzTarget] = []

    for name, service in services.items():
        if name in _INFRA_SERVICE_NAMES:
            continue
        if not isinstance(service, dict):
            continue

        build = service.get("build", {}) or {}
        if not isinstance(build, dict):
            continue
        args = build.get("args", {}) or {}
        if not isinstance(args, dict):
            continue

        # Require an explicit FUZZ build arg so infra / helper services
        # without fuzz metadata are never registered as launchable targets.
        fuzz_arg = args.get("FUZZ")
        if fuzz_arg is None or str(fuzz_arg).strip() == "":
            continue

        # Collect environment overrides (skip MODULES and FUZZ)
        env_overrides: dict[str, str] = {}
        modules_default: Optional[str] = None

        # Parse the 'environment' block
        env_block = service.get("environment", {})
        if isinstance(env_block, dict):
            for key, value in env_block.items():
                value_str = str(value) if value is not None else ""
                if key == "MODULES":
                    # Extract default MODULES value (strip shell default syntax)
                    clean = value_str.replace("${MODULES:-}", "").strip()
                    modules_default = clean if clean else None
                elif key not in ("FUZZ",):
                    env_overrides[key] = value_str
        elif isinstance(env_block, list):
            for entry in env_block:
                if isinstance(entry, str) and "=" in entry:
                    key, value = entry.split("=", 1)
                    if key == "MODULES":
                        modules_default = value if value else None
                    elif key != "FUZZ":
                        env_overrides[key] = value

        targets.append(
            FuzzTarget(
                name=name,
                cxxflags=str(args.get("CXXFLAGS", "")),
                fuzz=str(fuzz_arg),
                modules=modules_default,
                env_overrides=env_overrides,
            )
        )

    return targets


def build_target_registry(compose_file: Path) -> dict[str, FuzzTarget]:
    """Load targets and return them as a name-keyed dict for fast lookup."""
    targets = load_targets(compose_file)
    return {t.name: t for t in targets}

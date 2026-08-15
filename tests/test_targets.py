"""Tests for the target registry (parsing docker-compose.yml)."""

from pathlib import Path

from orchestrator.targets import (
    FuzzTarget,
    build_target_registry,
    load_targets,
)


def _compose_path() -> Path:
    """Use the fixtures compose file (not the infra stack compose)."""
    return (
        Path(__file__).resolve().parent
        / "fixtures"
        / "docker-compose.targets.yml"
    )


class TestLoadTargets:
    """Test loading targets from a bitcoinfuzz-shaped compose fixture."""

    def test_load_targets_returns_nonempty(self):
        targets = load_targets(_compose_path())
        assert len(targets) > 0

    def test_all_targets_have_names(self):
        targets = load_targets(_compose_path())
        for t in targets:
            assert t.name, f"Target missing name: {t}"

    def test_all_targets_have_cxxflags(self):
        targets = load_targets(_compose_path())
        for t in targets:
            assert t.cxxflags, f"Target {t.name} missing CXXFLAGS"

    def test_script_target_present(self):
        targets = load_targets(_compose_path())
        names = [t.name for t in targets]
        assert "script" in names

    def test_known_targets_present(self):
        """Verify a sample of expected targets from the fixture."""
        targets = load_targets(_compose_path())
        names = [t.name for t in targets]
        expected = [
            "script",
            "deserialize_block",
            "psbt_parse",
            "sign_schnorr",
            "ecdh",
            "descriptor_parse",
        ]
        for name in expected:
            assert name in names, f"Expected target {name} not found"

    def test_infra_services_are_filtered(self):
        """orchestrator/prometheus without FUZZ must not become targets."""
        targets = load_targets(_compose_path())
        names = [t.name for t in targets]
        assert "orchestrator" not in names
        assert "prometheus" not in names

    def test_target_count(self):
        """Fixture has 6 real fuzz targets (+ 2 infra services filtered)."""
        targets = load_targets(_compose_path())
        assert len(targets) == 6

    def test_script_cxxflags(self):
        targets = load_targets(_compose_path())
        script = [t for t in targets if t.name == "script"][0]
        assert "-DBITCOIN_CORE" in script.cxxflags
        assert "-DRUST_BITCOIN" in script.cxxflags

    def test_env_overrides_parsed(self):
        """descriptor_parse should have LIBFUZZ_DETECT_LEAKS=0."""
        targets = load_targets(_compose_path())
        desc = [t for t in targets if t.name == "descriptor_parse"][0]
        assert (
            "LIBFUZZ_DETECT_LEAKS" in desc.env_overrides
            or "ASAN_OPTIONS" in desc.env_overrides
        )


class TestBuildTargetRegistry:
    def test_registry_is_dict(self):
        registry = build_target_registry(_compose_path())
        assert isinstance(registry, dict)
        assert "script" in registry

    def test_registry_values_are_targets(self):
        registry = build_target_registry(_compose_path())
        for name, target in registry.items():
            assert isinstance(target, FuzzTarget)
            assert target.name == name

    def test_to_dict_roundtrip(self):
        registry = build_target_registry(_compose_path())
        script = registry["script"]
        d = script.to_dict()
        assert d["name"] == "script"
        assert "cxxflags" in d
        assert "resource_limits" in d

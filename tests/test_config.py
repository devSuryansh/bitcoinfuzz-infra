"""Tests for orchestrator configuration loading."""

from pathlib import Path

from orchestrator.config import OrchestratorConfig, load_default_config


class TestBindDataDir:
    def test_defaults_to_data_dir(self, tmp_path: Path):
        config = OrchestratorConfig(data_dir=tmp_path / "docker")
        assert config.bind_data_dir == tmp_path / "docker"

    def test_relative_bind_path_resolved_against_repo_root(self, tmp_path: Path):
        repo = tmp_path / "infra"
        repo.mkdir()
        config = OrchestratorConfig(
            repo_root=repo,
            data_dir=Path("../bitcoinfuzz/docker"),
        )
        assert config.bind_data_dir == (tmp_path / "bitcoinfuzz" / "docker")

    def test_host_data_dir_overrides(self, tmp_path: Path):
        config = OrchestratorConfig(
            data_dir=Path("/app/data"),
            host_data_dir=tmp_path / "bitcoinfuzz" / "docker",
        )
        assert config.data_dir == Path("/app/data")
        assert config.bind_data_dir == tmp_path / "bitcoinfuzz" / "docker"


class TestLoadDefaultConfig:
    def test_host_data_dir_from_env(self, monkeypatch, tmp_path: Path):
        monkeypatch.setenv("ORCHESTRATOR_DATA_DIR", "/app/data")
        monkeypatch.setenv(
            "ORCHESTRATOR_HOST_DATA_DIR", str(tmp_path / "docker")
        )
        config = load_default_config()
        assert config.data_dir == Path("/app/data")
        assert config.bind_data_dir == tmp_path / "docker"

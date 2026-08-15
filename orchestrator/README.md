# Orchestrator

FastAPI control plane for bitcoinfuzz campaign lifecycle management.

## Scope

- Load configuration from environment variables / optional YAML
- Parse fuzz targets from the **bitcoinfuzz** `docker-compose.yml` (`ORCHESTRATOR_COMPOSE_FILE`)
- Manage campaign containers via the Docker socket with resource-aware scheduling
- Persist campaign state and crash records to SQLite
- Expose Prometheus metrics on `:9091` and integrate with Alertmanager

## Quickstart

```bash
pip install -r ../requirements.txt

# Point at the bitcoinfuzz repo that defines real fuzz targets
export ORCHESTRATOR_COMPOSE_FILE=../../bitcoinfuzz/docker-compose.yml
export ORCHESTRATOR_DATA_DIR=../../bitcoinfuzz/docker
# Compose only: host path the Docker daemon bind-mounts into campaigns.
# export ORCHESTRATOR_HOST_DATA_DIR=/absolute/path/to/bitcoinfuzz/docker

../run_orchestrator.sh
```

Interactive docs: http://127.0.0.1:8000/docs

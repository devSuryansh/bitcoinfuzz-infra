# bitcoinfuzz-infra

Campaign orchestration, metrics collection, and monitoring for [bitcoinfuzz](https://github.com/bitcoinfuzz/bitcoinfuzz).

Built for a shared Docker host: operators launch and monitor fuzz campaigns through one API, with Prometheus metrics and Alertmanager routing. A single workstation checkout works the same way.

## Components

| Component | Role |
|-----------|------|
| `orchestrator/` | FastAPI control plane — campaign lifecycle, Docker management, scheduling |
| `metrics_agent/` | libFuzzer log parser + Prometheus exporter (`:9091`) |
| `prometheus/` | Scrape config + alert rules |
| `alertmanager/` | Routes alerts to the orchestrator webhook |

## Quick start

1. Clone this repo next to (or mount) a [bitcoinfuzz](https://github.com/bitcoinfuzz/bitcoinfuzz) tree that provides `docker-compose.yml` fuzz targets and images.
2. Copy `.env.example` → `.env` and set paths. On a team host, use absolute paths and `ORCHESTRATOR_HOST=0.0.0.0`.

```bash
export BITCOINFUZZ_REPO=/path/to/bitcoinfuzz   # or ../bitcoinfuzz for siblings
mkdir -p "${BITCOINFUZZ_REPO}/docker"

python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# API only
set -a && source .env && set +a
./run_orchestrator.sh

# Full stack (API + Prometheus + Alertmanager) — preferred for shared hosts
docker compose up --build -d
```

Step-by-step: [SETUP.md](./SETUP.md). Package notes: `orchestrator/README.md`.

## Tests

```bash
pip install -r requirements.txt
PYTHONPATH=$(pwd) pytest tests/ -v
```

Tests use `tests/fixtures/` for target compose data and do not need a live bitcoinfuzz checkout or Docker daemon.

## Design notes (PR #1)

- Target registry reads **bitcoinfuzz** compose (`ORCHESTRATOR_COMPOSE_FILE`), filtering infra services
- Metrics scraped **once** from **`:9091`** (static `orchestrator` job); no per-campaign scrape targets
- Mutating routes require `ORCHESTRATOR_API_TOKEN` when it is set; Compose requires it
- Crash attribution is **one campaign per crash file** when targets are shared
- CI under `.github/workflows/ci.yml`

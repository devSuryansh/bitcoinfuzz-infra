# Setup

Run [bitcoinfuzz-infra](https://github.com/bitcoinfuzz/bitcoinfuzz-infra) as a shared orchestration host or on a single workstation.

The orchestrator is the control plane for a group of operators: it loads fuzz targets from a bitcoinfuzz `docker-compose.yml`, launches campaign containers on a Docker host, persists state, and exposes metrics for Prometheus/Alertmanager.

## Prerequisites

| Requirement | Minimum | Check |
|-------------|---------|-------|
| Python | 3.11+ | `python3 --version` |
| Docker Engine | 24+ | `docker --version` |
| Docker Compose | v2 | `docker compose version` |
| bitcoinfuzz compose + images | — | path configured below |

On a shared host, operators need network access to the API (`8000`), metrics (`9091`), and optionally Prometheus (`9090`) / Alertmanager (`9093`). The Docker socket stays on the host that runs campaign containers.

## Layout

Two common layouts:

**Sibling checkouts (dev machine)**

```text
projects/
  bitcoinfuzz/          # fuzz targets, images, ./docker data
  bitcoinfuzz-infra/    # this repository
```

**Shared / team host**

```text
/opt/bitcoinfuzz/              # or any absolute path
  bitcoinfuzz/                # compose + pre-built images + data volume
  bitcoinfuzz-infra/          # this repository
```

Use **absolute paths** in `.env` on shared hosts so Compose and the orchestrator do not depend on whoever's cwd.

## 1. Clone and install

```bash
git clone https://github.com/bitcoinfuzz/bitcoinfuzz-infra.git
cd bitcoinfuzz-infra

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Point `BITCOINFUZZ_REPO` at a bitcoinfuzz tree that already has (or will have) fuzz images:

```bash
# example — adjust to your host
export BITCOINFUZZ_REPO=/opt/bitcoinfuzz/bitcoinfuzz
ls "${BITCOINFUZZ_REPO}/docker-compose.yml"
mkdir -p "${BITCOINFUZZ_REPO}/docker"
```

## 2. Configure environment

```bash
cp .env.example .env
# edit paths: prefer absolute paths on shared hosts
```

| Variable | Purpose |
|----------|---------|
| `BITCOINFUZZ_REPO` | Path to bitcoinfuzz (Compose volume mounts). Use an absolute path with Compose. |
| `DOCKER_GID` | GID of `/var/run/docker.sock` (`stat -c '%g' /var/run/docker.sock`). Compose `group_add` so UID 1001 can talk to Docker. |
| `ORCHESTRATOR_COMPOSE_FILE` | bitcoinfuzz `docker-compose.yml` with `FUZZ` targets |
| `ORCHESTRATOR_DATA_DIR` | Shared corpus/crash root as this process sees it (`docker/` under bitcoinfuzz, or `/app/data` in Compose) |
| `ORCHESTRATOR_HOST_DATA_DIR` | Host path the Docker daemon bind-mounts into campaign containers. Required in Compose; omit for local API-only runs. |
| `ORCHESTRATOR_DB_PATH` | SQLite database for campaign state |
| `ORCHESTRATOR_HOST` | API bind address (`0.0.0.0` for team access, `127.0.0.1` for local-only). Only use `0.0.0.0` with `ORCHESTRATOR_API_TOKEN` set. |
| `ORCHESTRATOR_PORT` | REST API port (default `8000`) |
| `ORCHESTRATOR_METRICS_PORT` | Prometheus `/metrics` port (default `9091`) |
| `ORCHESTRATOR_API_TOKEN` | Bearer token required on the mutating routes. Unset means no auth. |
| `ORCHESTRATOR_CORS_ORIGINS` | Comma-separated browser origin allow-list (default loopback only) |

`ORCHESTRATOR_COMPOSE_FILE` must point at the **bitcoinfuzz** compose file, not this repo’s `docker-compose.yml`. Infra services are not fuzz targets.

### API authentication

`POST`/`DELETE /campaigns`, the pause and resume routes, `/api/alerts/webhook` and `/api/ci-hook` all reach the Docker socket. They require `Authorization: Bearer <token>` whenever `ORCHESTRATOR_API_TOKEN` is set, and are unauthenticated when it is not. Read-only routes are always open.

Leaving the token unset is fine on a loopback bind. Pair it with `ORCHESTRATOR_HOST=0.0.0.0` and anyone who can route to the host can launch or stop containers, so the orchestrator logs a warning at startup in that combination.

```bash
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'
```

Put the result in `.env` as `ORCHESTRATOR_API_TOKEN`, then give the same value to every client that posts to a mutating route:

- Alertmanager: uncomment `bearer_token` in `alertmanager/alertmanager.yml`.
- CI jobs hitting `/api/ci-hook`: send the `Authorization: Bearer <token>` header.
- `curl`: `-H "Authorization: Bearer ${ORCHESTRATOR_API_TOKEN}"`.

Browser clients additionally need their origin listed in `ORCHESTRATOR_CORS_ORIGINS`. Credentialed CORS is off, so a dashboard must send the token explicitly rather than relying on cookies.

Load env vars when running outside Compose:

```bash
set -a && source .env && set +a
```

### Shared-host example `.env`

```bash
BITCOINFUZZ_REPO=/opt/bitcoinfuzz/bitcoinfuzz
DOCKER_GID=999
ORCHESTRATOR_COMPOSE_FILE=/opt/bitcoinfuzz/bitcoinfuzz/docker-compose.yml
ORCHESTRATOR_DATA_DIR=/opt/bitcoinfuzz/bitcoinfuzz/docker
ORCHESTRATOR_HOST_DATA_DIR=/opt/bitcoinfuzz/bitcoinfuzz/docker
ORCHESTRATOR_DB_PATH=/opt/bitcoinfuzz/bitcoinfuzz-infra/orchestrator.db
ORCHESTRATOR_HOST=0.0.0.0
ORCHESTRATOR_PORT=8000
ORCHESTRATOR_METRICS_PORT=9091
ORCHESTRATOR_API_TOKEN=replace-with-a-generated-token
```

All operators share the same data dir and DB. Crash files under `<target>/crash/` are attributed to one campaign at a time so alerts are not duplicated.

## 3. Run the orchestrator (API only)

```bash
source .venv/bin/activate
set -a && source .env && set +a
./run_orchestrator.sh
```

Expected startup lines include:

```text
INFO:     Loaded N targets from .../bitcoinfuzz/docker-compose.yml
INFO:     Metrics server started on port 9091
INFO:     Uvicorn running on http://0.0.0.0:8000
```

(`run_orchestrator.sh` binds `127.0.0.1` by default; for team access use Docker Compose below, or run `uvicorn` with `--host 0.0.0.0` / set `ORCHESTRATOR_HOST` and use `python -m orchestrator` after adjusting the entrypoint.)

Verify:

```bash
curl -s http://127.0.0.1:8000/health | jq
curl -s http://127.0.0.1:8000/targets | jq 'length'   # expect dozens of FUZZ targets
curl -s http://127.0.0.1:9091/metrics | head
```

Interactive docs: http://127.0.0.1:8000/docs

## 4. Run the full monitoring stack (recommended for teams)

Compose publishes the API and metrics on all interfaces and keeps Prometheus/Alertmanager on the same network:

```bash
set -a && source .env && set +a
# Compose needs an absolute BITCOINFUZZ_REPO so campaign bind-mounts
# hit the host directory, not /app/data inside the orchestrator.
BITCOINFUZZ_REPO="$(cd "${BITCOINFUZZ_REPO}" && pwd)"
ORCHESTRATOR_HOST_DATA_DIR="${BITCOINFUZZ_REPO}/docker"
DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
mkdir -p "${BITCOINFUZZ_REPO}/docker"
docker compose up --build -d
```

Compose binds the API to `0.0.0.0`, so `ORCHESTRATOR_API_TOKEN` is required and Compose refuses to start without it.

| URL | Service |
|-----|---------|
| http://\<host\>:8000/health | Orchestrator API |
| http://\<host\>:8000/docs | Swagger UI |
| http://\<host\>:9091/metrics | Metrics exporter |
| http://\<host\>:9090 | Prometheus |
| http://\<host\>:9093 | Alertmanager |

Prometheus scrapes the orchestrator exporter **once** on **`:9091`** (static `orchestrator` job), not `:8000`. Campaign series already carry `target` and `campaign_id`, so campaigns need no scrape targets of their own. Do not add a second job for the same URL. Prometheus mounts only `prometheus.yml` and `alerts.yml`, never a directory over `/etc/prometheus`, which would hide the config.

Pre-build fuzz images on the same Docker host (or a registry the host can pull). Example:

```bash
cd "${BITCOINFUZZ_REPO}"
just docker-build script
```

## 5. Launch a campaign

```bash
curl -s -X POST http://127.0.0.1:8000/campaigns \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${ORCHESTRATOR_API_TOKEN}" \
  -d '{
    "target_name": "script",
    "cpu_quota": 100000,
    "memory_mb": 1024,
    "max_duration_seconds": 300
  }' | jq
```

Resource limits are enforced by the scheduler so multiple operators cannot oversubscribe the host without queueing.

## 6. Run tests

No Docker daemon or bitcoinfuzz checkout required (Docker is mocked; targets come from `tests/fixtures/`):

```bash
source .venv/bin/activate
PYTHONPATH=$(pwd) pytest tests/ -v
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `/targets` empty | Set `ORCHESTRATOR_COMPOSE_FILE` to bitcoinfuzz compose with `build.args.FUZZ` |
| `503 Docker not available` / `PermissionError` on the Docker socket | Mount `/var/run/docker.sock` and set `DOCKER_GID` to `stat -c '%g' /var/run/docker.sock`. Compose adds that GID via `group_add`. |
| Campaign corpus/crashes missing or written to host `/app/data` | Set `ORCHESTRATOR_HOST_DATA_DIR` to the absolute host path of bitcoinfuzz `docker/`. The daemon does not see the orchestrator's `/app/data`. |
| Campaign image missing | Build/pull `bitcoinfuzz:<target>` on this host |
| `401 Missing or invalid bearer token` | Send `Authorization: Bearer $ORCHESTRATOR_API_TOKEN`; for Alertmanager set `bearer_token` in `alertmanager/alertmanager.yml` |
| Browser dashboard blocked by CORS | Add its origin to `ORCHESTRATOR_CORS_ORIGINS` |
| Prometheus config missing | Mount `prometheus.yml` and `alerts.yml` as files; never mount a directory over `/etc/prometheus` |

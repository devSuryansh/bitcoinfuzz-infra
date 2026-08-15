#!/usr/bin/env bash
# Launch the orchestrator API for local or shared-host use.
# Bind address/port come from ORCHESTRATOR_HOST / ORCHESTRATOR_PORT
# (see .env.example). Default: 127.0.0.1:8000.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
export PYTHONPATH="${PYTHONPATH:-}:${ROOT}"
HOST="${ORCHESTRATOR_HOST:-127.0.0.1}"
PORT="${ORCHESTRATOR_PORT:-8000}"
exec uvicorn orchestrator.app:app --host "${HOST}" --port "${PORT}" --reload

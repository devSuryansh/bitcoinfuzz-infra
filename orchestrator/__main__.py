"""CLI entrypoint for the orchestrator package.

Run with: `python -m orchestrator` to start a development server.
Honours ORCHESTRATOR_HOST / ORCHESTRATOR_PORT for shared-host binds.
"""
import os

import uvicorn

from .app import app


def main() -> None:
    host = os.environ.get("ORCHESTRATOR_HOST", "127.0.0.1")
    port = int(os.environ.get("ORCHESTRATOR_PORT", "8000"))
    uvicorn.run(app, host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()

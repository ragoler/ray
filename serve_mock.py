"""Standalone offline harness for the playroom (MODE=MOCK).

Not used by the Hub — it mounts this feature's `hub_router` and serves the
`frontend/` so you can click through the whole demo with no cluster:

    MODE=MOCK uvicorn serve_mock:app --reload --port 8080
    open http://localhost:8080/ray/

The Hub provides equivalents of all of this in production.
"""

from __future__ import annotations

import os
import pathlib

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

os.environ.setdefault("MODE", "MOCK")

from hub_router import router  # noqa: E402  (after MODE is set)

ROOT = pathlib.Path(__file__).parent
app = FastAPI(title="Ray Render Farm (offline)")
app.include_router(router, prefix="/api/features/ray")

# Mirror the Hub's static layout: assets at /static/features/ray, page at /ray/.
app.mount("/static/features/ray", StaticFiles(directory=ROOT / "frontend"), name="assets")
app.mount("/ray", StaticFiles(directory=ROOT / "frontend", html=True), name="playroom")

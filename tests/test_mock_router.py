"""MODE=MOCK hub_router tests — the playroom must work fully offline."""

import json
import os

import pytest

os.environ["MODE"] = "MOCK"

fastapi = pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import hub_router  # noqa: E402


@pytest.fixture()
def client():
    app = FastAPI()
    app.include_router(hub_router.router, prefix="/api/features/ray")
    return TestClient(app)


def _events(text: str):
    return [json.loads(l[5:].strip()) for l in text.splitlines() if l.startswith("data:")]


def test_config_mock(client):
    r = client.get("/api/features/ray/config")
    assert r.status_code == 200
    body = r.json()
    assert body["mode"] == "MOCK"
    assert body["gateway_ip"] is None  # nothing to link to offline


def test_presets(client):
    r = client.get("/api/features/ray/presets")
    assert r.status_code == 200
    assert "seahorse" in r.json()


def test_solid_png_is_valid():
    import base64

    png = base64.b64decode(hub_router._solid_png(10, 20, 30, 16))
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_render_then_stream_paints_and_autoscales(client):
    # 256px @ 128px tiles -> 2x2 = 4 tiles.
    r = client.post("/api/features/ray/render", json={"resolution": 256})
    assert r.status_code == 200
    job_id = r.json()["job_id"]

    s = client.get(f"/api/features/ray/render/{job_id}/stream")
    assert s.status_code == 200
    events = _events(s.text)

    meta = events[0]
    assert meta["type"] == "meta" and meta["tiles"] == 4
    tiles = [e for e in events if e["type"] == "tile"]
    assert len(tiles) == 4
    # Every tile attributed to a worker pod and carries pixels.
    assert all(t["pod_name"].startswith("ray-render-farm-worker") for t in tiles)
    assert all(t["png_base64"] for t in tiles)
    assert events[-1]["type"] == "done"


def test_workers_includes_head(client):
    # Run a render first so the autoscaler mock has populated workers.
    job = client.post("/api/features/ray/render", json={"resolution": 512}).json()["job_id"]
    client.get(f"/api/features/ray/render/{job}/stream")
    pods = client.get("/api/features/ray/workers").json()["pods"]
    types = {p["node_type"] for p in pods}
    assert "head" in types
    assert "worker" in types


def test_stream_unknown_job_404(client):
    r = client.get("/api/features/ray/render/nope/stream")
    assert r.status_code == 404

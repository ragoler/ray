"""Hub data-plane router for the Ray Render Farm feature.

Mounted by the Hub at ``/api/features/ray`` behind the admin JWT. Kept thin:

* **LIVE** — the browser talks to the controller directly via the Gateway IP
  (CORS) for the heavy data plane (render + SSE + workers). This router only
  resolves ``/config`` (gateway IP + Ray Dashboard URL) using the shared SDK.
* **MOCK** — no cluster exists, so this router serves the *entire* surface
  (``/config``, ``/presets``, ``/render``, the SSE stream, ``/workers``) with
  deterministic data, including a synthetic autoscaling curve. Tiles are real
  PNGs encoded with a tiny pure-stdlib encoder, so the playroom animates fully
  offline without Pillow/numpy in the Hub image.
"""

from __future__ import annotations

import asyncio
import json
import math
import struct
import uuid
import zlib

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

# --------------------------------------------------------------------------- #
# Shared SDK — imported tolerantly so the router also loads standalone/in tests.
# --------------------------------------------------------------------------- #
try:  # pragma: no cover - exercised inside the Hub container
    from showcase_admin.app import config, k8s_client

    _MODE = getattr(config, "MODE", "LIVE")
    _get_gateway_ip = getattr(k8s_client, "get_gateway_ip", None)
    _get_feature_namespace = getattr(k8s_client, "get_feature_namespace", None)
except Exception:  # standalone / unit tests
    import os

    _MODE = os.environ.get("MODE", "MOCK")
    _get_gateway_ip = None
    _get_feature_namespace = None

FEATURE = "ray"
GATEWAY_NAME = "ray-gateway"

router = APIRouter()

PRESETS = {
    "overview": {"center_x": -0.5, "center_y": 0.0, "zoom": 1.6},
    "seahorse": {"center_x": -0.745, "center_y": 0.113, "zoom": 0.02},
    "spiral": {"center_x": -0.7435, "center_y": 0.1314, "zoom": 0.0035},
    "elephant": {"center_x": 0.275, "center_y": 0.007, "zoom": 0.02},
    "minibrot": {"center_x": -1.7687, "center_y": 0.0017, "zoom": 0.004},
}

TILE_PX = 128
MAX_MOCK_WORKERS = 6


# --------------------------------------------------------------------------- #
# Tiny pure-stdlib PNG encoder (solid-color RGB square). MOCK only.
# --------------------------------------------------------------------------- #
def _solid_png(r: int, g: int, b: int, size: int = 64) -> str:
    import base64

    def chunk(tag: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + tag
            + data
            + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
        )

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)  # 8-bit RGB
    row = b"\x00" + bytes([r, g, b]) * size  # filter byte + pixels
    raw = row * size
    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(raw, 6))
        + chunk(b"IEND", b"")
    )
    return base64.b64encode(png).decode("ascii")


def _hue_rgb(i: int) -> tuple[int, int, int]:
    """Distinct color per pod index (golden-angle hue stepping)."""
    h = (i * 0.61803398875) % 1.0
    k = h * 6.0
    c = 200
    x = int(c * (1 - abs((k % 2) - 1)))
    base = 40
    table = [(c, x, 0), (x, c, 0), (0, c, x), (0, x, c), (x, 0, c), (c, 0, x)]
    r, g, b = table[int(k) % 6]
    return base + r, base + g, base + b


# --------------------------------------------------------------------------- #
# MOCK render state
# --------------------------------------------------------------------------- #
_MOCK_JOBS: dict[str, dict] = {}
_MOCK_PODS: list[dict] = [{"pod_name": "ray-render-farm-head", "node_type": "head",
                           "status": "Running", "node": "node-pool-default"}]
_MOCK_CLOCK = {"t": 0.0}  # virtual time (advanced by stream), avoids Date.now()


def _mock_reset_pods() -> None:
    _MOCK_PODS[:] = [
        {"pod_name": "ray-render-farm-head", "node_type": "head",
         "status": "Running", "node": "node-pool-default"},
        {"pod_name": "ray-render-farm-worker-0", "node_type": "worker",
         "status": "Running", "node": "spot-pool-0"},
    ]


def _mock_add_worker() -> dict:
    idx = sum(1 for p in _MOCK_PODS if p["node_type"] == "worker")
    pod = {"pod_name": f"ray-render-farm-worker-{idx}", "node_type": "worker",
           "status": "Running", "node": f"spot-pool-{idx}"}
    _MOCK_PODS.append(pod)
    return pod


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@router.get("/config")
def config_endpoint() -> dict:
    if _MODE == "MOCK":
        return {"mode": "MOCK", "gateway_ip": None, "dashboard_url": None}

    gateway_ip = None
    if _get_gateway_ip:
        try:
            ns = _get_feature_namespace(FEATURE) if _get_feature_namespace else None
            gateway_ip = _get_gateway_ip(GATEWAY_NAME, ns) if ns else _get_gateway_ip(GATEWAY_NAME)
        except Exception:
            gateway_ip = None
    dashboard_url = f"http://{gateway_ip}/ray-dashboard" if gateway_ip else None
    return {"mode": "LIVE", "gateway_ip": gateway_ip, "dashboard_url": dashboard_url}


@router.get("/presets")
def presets() -> dict:
    return PRESETS


@router.post("/render")
async def render(req: dict) -> dict:
    """MOCK render planner. (LIVE renders go straight to the Gateway IP.)"""
    if _MODE != "MOCK":
        raise HTTPException(
            status_code=409,
            detail="LIVE render runs on the Gateway IP, not the Hub router.",
        )
    resolution = int(req.get("resolution", 1024))
    per_row = max(1, math.ceil(resolution / TILE_PX))
    total = per_row * per_row
    job_id = uuid.uuid4().hex[:12]
    _MOCK_JOBS[job_id] = {"total": total, "per_row": per_row, "resolution": resolution}
    _mock_reset_pods()
    return {"job_id": job_id}


@router.get("/render/{job_id}/stream")
async def stream(job_id: str) -> StreamingResponse:
    if _MODE != "MOCK":
        raise HTTPException(status_code=409, detail="LIVE stream is on the Gateway IP.")
    job = _MOCK_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="unknown job")

    per_row = job["per_row"]
    total = job["total"]
    res = job["resolution"]
    # Keep the whole animation to a few seconds regardless of tile count.
    delay = min(0.06, 6.0 / max(total, 1))
    # Grow workers across the run to mimic the autoscaler reacting to demand.
    add_every = max(1, total // MAX_MOCK_WORKERS)

    async def gen():
        yield f'data: {json.dumps({"type": "meta", "tiles": total, "tile_px": TILE_PX, "width": res, "height": res})}\n\n'
        start = _MOCK_CLOCK["t"]
        for i in range(total):
            if i and i % add_every == 0 and sum(
                1 for p in _MOCK_PODS if p["node_type"] == "worker"
            ) < MAX_MOCK_WORKERS:
                _mock_add_worker()
            workers = [p for p in _MOCK_PODS if p["node_type"] == "worker"]
            pod = workers[i % len(workers)]
            pidx = int(pod["pod_name"].rsplit("-", 1)[-1])
            r, g, b = _hue_rgb(pidx)
            row, col = divmod(i, per_row)
            tile = {
                "type": "tile", "index": i,
                "x": col * TILE_PX, "y": row * TILE_PX,
                "w": TILE_PX, "h": TILE_PX,
                "png_base64": _solid_png(r, g, b, 32),
                "pod_name": pod["pod_name"], "ms": 40,
            }
            _MOCK_CLOCK["t"] += delay
            yield f"data: {json.dumps(tile)}\n\n"
            await asyncio.sleep(delay)
        elapsed = int((_MOCK_CLOCK["t"] - start) * 1000)
        yield f'data: {json.dumps({"type": "done", "elapsed_ms": elapsed})}\n\n'
        _MOCK_JOBS.pop(job_id, None)

    return StreamingResponse(gen(), media_type="text/event-stream")


@router.get("/workers")
def workers() -> dict:
    if _MODE != "MOCK":
        raise HTTPException(status_code=409, detail="LIVE workers are on the Gateway IP.")
    return {"namespace": "gke-showcase-ray", "cluster": "ray-render-farm",
            "pods": list(_MOCK_PODS)}

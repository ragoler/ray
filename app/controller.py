"""FastAPI controller for the Ray Render Farm demo.

This process is the **Ray driver**. It connects to the in-namespace RayCluster
head with the Ray Client (``ray://ray-head:10001``), launches one task per image
tile, and streams completed tiles to the browser over Server-Sent Events as the
``ray.wait`` loop collects them. A separate ``/workers`` endpoint lists the Ray
worker pods (via the Kubernetes API) so the frontend can draw the autoscaling
cluster map.

Data-plane only: the browser calls this directly via the Gateway IP, so CORS is
mandatory. The Hub's JWT-protected control plane lives in ``hub_router.py``.
"""

from __future__ import annotations

import json
import os
import queue
import threading
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from tasks import build_tile_specs, render_tile

# --------------------------------------------------------------------------- #
# Configuration (all namespace-portable; nothing hardcodes "default").
# --------------------------------------------------------------------------- #
RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "ray://ray-head:10001")
POD_NAMESPACE = os.environ.get("POD_NAMESPACE", "default")
RAY_CLUSTER_NAME = os.environ.get("RAY_CLUSTER_NAME", "ray-render-farm")
TILE_PX = int(os.environ.get("TILE_PX", "128"))

# Curated regions of the Mandelbrot set. (center_x, center_y, zoom-half-width).
PRESETS: dict[str, tuple[float, float, float]] = {
    "overview": (-0.5, 0.0, 1.6),
    "seahorse": (-0.745, 0.113, 0.02),
    "spiral": (-0.7435, 0.1314, 0.0035),
    "elephant": (0.275, 0.007, 0.02),
    "minibrot": (-1.7687, 0.0017, 0.004),
}

app = FastAPI(title="Ray Render Farm Controller")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# In-flight jobs: job_id -> {"queue": Queue, "tiles": int, "started": float}.
_JOBS: dict[str, dict] = {}
_RAY_READY = False
_RAY_LOCK = threading.Lock()


def _ensure_ray() -> None:
    """Connect to the RayCluster lazily and once."""
    global _RAY_READY
    if _RAY_READY:
        return
    with _RAY_LOCK:
        if _RAY_READY:
            return
        import ray

        if not ray.is_initialized():
            ray.init(address=RAY_ADDRESS, ignore_reinit_error=True)
        _RAY_READY = True


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
class RenderRequest(BaseModel):
    preset: str = Field(default="overview")
    # Optional explicit window; overrides preset when all three are provided.
    center_x: float | None = None
    center_y: float | None = None
    zoom: float | None = None
    resolution: int = Field(default=1024, ge=128, le=4096)
    max_iter: int = Field(default=256, ge=32, le=2000)
    # Informational: the live cap is the RayCluster autoscaler maxReplicas.
    max_workers: int | None = None


# --------------------------------------------------------------------------- #
# Render driver
# --------------------------------------------------------------------------- #
def _run_render(job_id: str, req: RenderRequest) -> None:
    """Background driver: launch tile tasks, collect with ray.wait, enqueue."""
    import ray

    q: queue.Queue = _JOBS[job_id]["queue"]
    try:
        if req.center_x is not None and req.center_y is not None and req.zoom is not None:
            cx, cy, zoom = req.center_x, req.center_y, req.zoom
        else:
            cx, cy, zoom = PRESETS.get(req.preset, PRESETS["overview"])

        specs = build_tile_specs(
            width=req.resolution,
            height=req.resolution,
            tile=TILE_PX,
            center_x=cx,
            center_y=cy,
            zoom=zoom,
            max_iter=req.max_iter,
        )
        _JOBS[job_id]["tiles"] = len(specs)
        q.put({"type": "meta", "tiles": len(specs), "tile_px": TILE_PX,
               "width": req.resolution, "height": req.resolution})

        refs = [render_tile.remote(s) for s in specs]
        pending = list(refs)
        while pending:
            ready, pending = ray.wait(pending, num_returns=1, timeout=30.0)
            for ref in ready:
                tile = ray.get(ref)
                tile["type"] = "tile"
                q.put(tile)
        q.put({"type": "done", "elapsed_ms": int((time.time() - _JOBS[job_id]["started"]) * 1000)})
    except Exception as exc:  # surface failures, don't swallow them
        q.put({"type": "error", "message": str(exc)})
    finally:
        q.put(None)  # sentinel


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #
@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/presets")
def presets() -> dict:
    return {
        name: {"center_x": c[0], "center_y": c[1], "zoom": c[2]}
        for name, c in PRESETS.items()
    }


@app.post("/render")
def render(req: RenderRequest) -> dict:
    """Kick off a distributed render; returns a job id to stream."""
    try:
        _ensure_ray()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Ray not reachable: {exc}")

    job_id = uuid.uuid4().hex[:12]
    _JOBS[job_id] = {"queue": queue.Queue(), "tiles": 0, "started": time.time()}
    threading.Thread(target=_run_render, args=(job_id, req), daemon=True).start()
    return {"job_id": job_id}


@app.get("/render/{job_id}/stream")
def stream(job_id: str) -> StreamingResponse:
    """SSE stream of completed tiles for a render job."""
    job = _JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="unknown job")

    def gen():
        q: queue.Queue = job["queue"]
        while True:
            try:
                item = q.get(timeout=0.5)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue
            if item is None:
                break
            yield f"data: {json.dumps(item)}\n\n"
        _JOBS.pop(job_id, None)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/dashboard")
def dashboard() -> dict:
    """External URL of the Ray Dashboard LoadBalancer (null until it gets an IP)."""
    try:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        v1 = client.CoreV1Api()
        svc = v1.read_namespaced_service("ray-dashboard", POD_NAMESPACE)
        ingress = (svc.status.load_balancer.ingress or []) if svc.status.load_balancer else []
        ip = ingress[0].ip if ingress else None
        return {"url": f"http://{ip}/" if ip else None}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"cannot read dashboard service: {exc}")


@app.get("/workers")
def workers() -> dict:
    """List Ray pods for the cluster map (head + autoscaled workers)."""
    try:
        from kubernetes import client, config

        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        v1 = client.CoreV1Api()
        selector = f"ray.io/cluster={RAY_CLUSTER_NAME}"
        pods = v1.list_namespaced_pod(POD_NAMESPACE, label_selector=selector)
        out = []
        for p in pods.items:
            out.append({
                "pod_name": p.metadata.name,
                "node": p.spec.node_name,
                "node_type": (p.metadata.labels or {}).get("ray.io/node-type", "unknown"),
                "status": p.status.phase,
            })
        return {"namespace": POD_NAMESPACE, "cluster": RAY_CLUSTER_NAME, "pods": out}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"cannot list pods: {exc}")

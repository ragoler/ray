# Ray Render Farm 🌀

Distributed **Mandelbrot rendering** on **KubeRay / GKE**. Pick a region, hit
**Launch**, and watch the image paint in tile-by-tile as Ray fans the work out
across the cluster — while a live **cluster map** shows KubeRay autoscaling
worker pods onto **Spot nodes** and draining them when idle. Each tile is
labelled with the **pod that computed it**, and an **Open Ray Dashboard** button
links to the real Ray UI.

This is a [gke_all](https://github.com/ragoler/gke_all) showcase feature
(`feature.yaml`). It runs **standalone** and as a **Hub feature**. See
[PROPOSAL.md](PROPOSAL.md) for the full design.

## How it works

```
Browser ──/render──▶ Controller (Ray driver) ──ray://head:10001──▶ RayCluster
   ▲                       │  one @ray.remote task per tile             head +
   │  SSE: tiles +         │  ray.wait() collects results          autoscaling
   └─ pod attribution ◀────┘  each task returns its POD_NAME       Spot workers
```

- **Fan-out:** one Ray task per image tile; Ray schedules them across workers.
- **Autoscaling:** too many pending tasks → the Ray autoscaler asks KubeRay for
  more worker pods → KubeRay schedules them on Spot nodes (a `ComputeClass`).
- **Attribution:** each worker pod gets `POD_NAME` via the downward API; tasks
  return it, so every tile knows which pod made it.
- **Streaming:** the controller (the driver) streams completed tiles to the
  browser over SSE; the canvas paints each tile as it lands.

## Layout

| Path | Purpose |
|---|---|
| `feature.yaml` | Hub descriptor |
| `app/` | controller (FastAPI driver) + `render_tile` task + Dockerfile |
| `frontend/` | playroom: canvas, cluster map, dashboard button |
| `hub_router.py` | thin Hub data-plane router + full MOCK mode |
| `infra/` | per-namespace: RayCluster, controller, RBAC, Gateway, HTTPRoute |
| `cluster/` | cluster-scoped: KubeRay operator (pinned) + Spot ComputeClass |
| `tests/` | unit + mock-mode tests |

## Run the playroom offline (MOCK)

No cluster needed — the `hub_router` serves a deterministic render with a
synthetic autoscaling curve (real PNG tiles via a stdlib encoder).

```bash
python -m venv .venv && . .venv/bin/activate
pip install fastapi uvicorn
MODE=MOCK uvicorn serve_mock:app --reload --port 8080
open http://localhost:8080/ray/
```

## Standalone on GKE

```bash
export PROJECT_NAME=... REGION=us-central1 ARTIFACT_REGISTRY_REPO=...
export NAMESPACE=default
./setup_infra.sh           # operator + ComputeClass, build/push image, deploy infra
```

Then open the Gateway IP. As a Hub feature, none of this is needed — the Hub
discovers `feature.yaml`, builds the image, applies `cluster/` once and `infra/`
per deploy, and serves the playroom at `/ray/`.

## Tests

```bash
pip install -r app/requirements.txt pytest httpx
pytest tests/
```

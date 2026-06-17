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
| `.env.example` | standalone config template (`cp .env.example .env`) |
| `setup_infra.sh` | standalone: create GKE cluster + cluster-scoped prereqs |
| `deploy_app.sh` | standalone: build/push image + deploy `infra/` |
| `verify_setup.sh` | standalone: readiness + data-plane smoke test |
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

Three steps, mirroring the `inference_gateway` convention: configure, provision
the cluster, deploy the app.

```bash
# 1. Configure (edit PROJECT_ID, cluster name, region, worker cap, …)
cp .env.example .env

# 2. Provision: create the GKE cluster (Gateway API + Node Auto-Provisioning)
#    and the cluster-scoped prereqs (KubeRay operator + Spot ComputeClass).
./setup_infra.sh

# 3. Build & push the image, then deploy the RayCluster + controller + Gateway.
./deploy_app.sh

# 4. Validate: readiness + a real render through the Gateway IP.
./verify_setup.sh
```

Then open the **Gateway IP in a browser** — the controller serves the full
playroom (canvas + cluster map) standalone, calling its own API same-origin. The
Ray Dashboard is at the `ray-dashboard` Service's external IP (printed by the
scripts). As a Hub feature, the Hub serves the same playroom at `/ray/` instead.

Teardown (the cluster is only removed with `--delete-cluster`):

```bash
./setup_infra.sh --delete          # remove cluster-scoped prereqs, keep cluster
./setup_infra.sh --delete-cluster  # the above, plus delete the GKE cluster
```

Standalone uses **`PROJECT_ID`** (in `.env`); the Hub injects the equivalent as
**`PROJECT_NAME`** and supplies `NAMESPACE`/`REGION`/`ARTIFACT_REGISTRY_REPO`
itself. As a Hub feature none of these scripts run — the Hub discovers
`feature.yaml`, builds the image from its `build:` entry, applies `cluster/` once
at bootstrap and `infra/` per deploy (into `gke-showcase-ray`), and serves the
playroom at `/ray/`.

## Tests

```bash
pip install -r app/requirements.txt pytest httpx
pytest tests/
```

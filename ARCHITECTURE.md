# Architecture — how the Ray Render Farm works (a learning guide)

This walks through the whole implementation so you can learn from it. The demo is
deliberately simple compute (a Mandelbrot fractal) wrapped in the *real*
KubeRay-on-GKE machinery, so the interesting part is the **distributed systems
plumbing**, not the math.

---

## 1. The one-sentence idea

You request a big computation; the system splits it into many small pieces,
**Ray** runs the pieces across a pool of pods, **KubeRay** autoscales that pool
onto **Spot VMs** when there's demand and back to zero when idle, and results
stream back to a browser tile-by-tile.

Mandelbrot is just an *embarrassingly parallel* stand-in for real work (batch
inference, data processing, training sweeps).

---

## 2. The moving parts

```
Browser (playroom)                     ── served by the controller (standalone) or the Hub
  │  POST /render, GET /render/{id}/stream (SSE), /workers, /metrics-link
  ▼
Gateway (gke-l7-global-external-managed)   ── dedicated L7 load balancer, one public IP
  │  HTTPRoute "/" → ray-controller:80
  ▼
Controller  (Deployment, FastAPI)          ── THIS is the Ray *driver*
  │  ray.init("ray://ray-...-head-svc:10001")   (Ray Client)
  │  render_tile.remote(spec)  × N tiles
  ▼
RayCluster  (KubeRay CR)
  ├── head pod      (scheduling, GCS, dashboard :8265, metrics :8080)
  └── worker group  (autoscaled 0→N on Spot nodes; each runs render_tile)

Ray Dashboard  → its own LoadBalancer Service (ray-dashboard :8265)
Metrics        → Ray /metrics (:8080) → GMP PodMonitoring → Cloud Monitoring dashboard
```

Repo map:

| Path | Role |
|---|---|
| `app/controller.py` | FastAPI app = Ray **driver** + data-plane API + serves the UI |
| `app/tasks.py` | `render_tile` `@ray.remote` task + tile-grid math |
| `frontend/` | the playroom (canvas, cluster map, buttons) |
| `infra/` | per-namespace K8s: RayCluster, controller, Gateway/HTTPRoute, policies, RBAC, PodMonitoring |
| `cluster/` | cluster-scoped: KubeRay operator + Spot ComputeClass |
| `hub_router.py` | thin Hub data-plane router + offline MOCK |
| `monitoring/` | Cloud Monitoring dashboard definition |
| `*.sh`, `.env` | standalone lifecycle (create cluster, build/deploy, verify) |

---

## 3. The render data flow (the heart of it)

1. **Browser → `POST /render`** with `{preset, resolution, max_iter}`.
2. **Controller builds a tile grid** (`tasks.build_tile_specs`): the image is cut
   into 128×128 tiles, each a `TileSpec` describing its pixel box + its window in
   the complex plane.
3. **Controller submits one Ray task per tile**:
   ```python
   refs = [render_tile.remote(spec) for spec in specs]   # N ObjectRefs, runs remotely
   ```
   `render_tile` is decorated `@ray.remote`, so calling `.remote()` ships the work
   to the cluster instead of running locally. Each returns an **ObjectRef** (a
   future).
4. **Ray schedules tasks across workers.** The head has `num-cpus: 0`, so tasks
   *can't* run on it — they must go to workers. If there aren't enough workers,
   tasks sit **PENDING**.
5. **That pending demand drives autoscaling** (see §5).
6. **Controller collects results as they finish** (streaming, not all-at-once):
   ```python
   while pending:
       ready, pending = ray.wait(pending, num_returns=1, timeout=30)
       for ref in ready:
           tile = ray.get(ref)          # {index, x, y, png_base64, pod_name, ms}
           queue.put(tile)              # hand to the SSE generator
   ```
7. **Each tile streams to the browser over SSE** (`GET /render/{id}/stream`), which
   the canvas paints at `(x, y)` and attributes to `pod_name`.

**Pod attribution trick:** each worker pod gets `POD_NAME` via the Kubernetes
downward API; the task returns `os.environ["POD_NAME"]`, so every tile knows which
pod produced it — no log scraping.

**Why a background thread + queue?** `ray.wait` is blocking; the render runs in a
worker thread that pushes tiles into a `queue.Queue`, and the SSE endpoint drains
that queue. That decouples "compute" from "stream to client".

---

## 4. Ray concepts you can take away

- **Task** = a stateless function you mark `@ray.remote`; `.remote(args)` runs it
  somewhere in the cluster and returns an `ObjectRef` (future).
- **`ray.get(ref)`** blocks for one result; **`ray.wait(refs, num_returns=1)`**
  returns whichever finishes first → enables *streaming* completion.
- **Ray Client (`ray://host:10001`)** lets an *external* process (our controller)
  act as the driver. The catch: client and cluster must run the **same Ray
  version** — that's why the controller and the RayCluster use the **same image**.
- **Connections drop.** A long-lived Ray Client connection dies on head restarts /
  idle. The controller probes liveness (`ray.cluster_resources()`) before each
  render and reconnects — otherwise you get the "render failed: 503" we hit.

---

## 5. KubeRay + GKE autoscaling (the GKE story)

- **KubeRay operator** (installed once, cluster-scoped) watches `RayCluster`
  custom resources and turns them into head/worker pods.
- **`RayCluster`** (`infra/raycluster.yaml`) declares a head + a worker group with
  `enableInTreeAutoscaling: true` and `minReplicas: 0 / maxReplicas: N`.
- **Two-level autoscaling:**
  1. Pending Ray tasks → the **Ray autoscaler** asks KubeRay for more worker pods.
  2. Those pods can't schedule (no node) → **GKE Node Auto-Provisioning** creates
     a node. The worker pods select `cloud.google.com/compute-class: ray-spot`
     (`cluster/spot-computeclass.yaml`), so NAP brings up **Spot** VMs.
- **Scale to zero:** `idleTimeoutSeconds: 60` drains idle workers; NAP later
  removes the empty Spot nodes. That's the "back to zero" you see.

This is the whole value prop: *demand in → cheap compute appears → demand gone →
it disappears.*

---

## 6. Networking (and the gotchas we hit)

- **Dedicated Gateway.** We use `gke-l7-global-external-managed` (a **dedicated**
  L7 LB per Gateway). The classic `gke-l7-gxlb` shares one LB per cluster, which
  collided with another feature on this shared cluster — a real lesson.
- **`HTTPRoute`** sends `/` to the controller Service.
- **`GCPBackendPolicy` (`timeoutSec: 3600`)** — the default backend timeout is
  **30s**, which cut the SSE stream during cold starts ("Network Error"). Long
  streams need a long backend timeout.
- **`HealthCheckPolicy` (`/healthz`)** — the LB health-checks `/` by default; our
  app only serves `/healthz`, so without this the backend looked unhealthy (502s).
- **Ray Dashboard** gets its **own LoadBalancer Service** (served at root `/`), so
  the SPA works without URL rewriting (which the gateway class couldn't do anyway).

---

## 7. Observability — Google Managed Prometheus (GMP)

- Ray exports Prometheus metrics on each pod (`metrics` port `:8080/metrics`).
- `infra/podmonitoring.yaml` (a `PodMonitoring`) tells GMP's **managed
  collection** to scrape every Ray pod → metrics land in **Cloud Monitoring**, no
  self-run Prometheus.
- `deploy_app.sh` creates a curated **Cloud Monitoring dashboard**
  (`monitoring/ray-dashboard.json`) and stashes its URL in the `ray-links`
  ConfigMap; the controller serves it at `/metrics-link`; the playroom shows it as
  **Metrics ↗**.
- (The Ray Dashboard's *own* metrics tab is separate — it needs Grafana wired to
  the head; GMP collection above is independent and works via Cloud Monitoring.)

---

## 8. Standalone *and* Hub (the feature contract)

The repo follows the [gke_all feature contract](https://github.com/ragoler/gke_all/blob/main/feature.md):

- `feature.yaml` is the descriptor the Hub reads (paths, gateway, build, router).
- **Standalone:** `setup_infra.sh` (create cluster + operator + ComputeClass) →
  `deploy_app.sh` (build image + apply `infra/`) → `verify_setup.sh`. The
  controller serves the playroom itself, so the Gateway IP shows the full UI.
- **Hub:** the Hub builds the image, applies `cluster/` once + `infra/` per deploy,
  serves the playroom at `/ray/`, and mounts `hub_router.py` at
  `/api/features/ray`. The *same* frontend works both ways: it probes
  `/api/features/ray/config` (Hub) and falls back to its own origin (standalone).
- **`MODE=MOCK`** makes `hub_router.py` serve a deterministic render (real PNG
  tiles via a stdlib encoder) so the UI runs fully offline.

---

## 9. What "max iterations" means

The Mandelbrot set: for each pixel's complex number `c`, iterate `z = z² + c`
starting at `z = 0`. If `|z|` stays bounded forever, `c` is *in* the set (drawn
dark); if it escapes (`|z| > 2`), it's outside (colored by how fast it escaped).
You can't iterate forever, so **max iterations** is the cutoff: if a point hasn't
escaped after that many steps, we call it "inside". Higher → sharper detail near
the fractal boundary and **more CPU per tile** (heavier distributed work); lower →
faster but blockier. It's the knob for "how hard is each task".

---

## 10. Suggested reading order in the code

1. `app/tasks.py` — the unit of work (`render_tile`, `build_tile_specs`).
2. `app/controller.py` — driver + streaming (`_run_render`, `ray.wait` loop,
   `_ensure_ray` reconnect).
3. `infra/raycluster.yaml` — the autoscaling cluster declaration.
4. `frontend/app.js` — how the browser drives it (SSE reader, cluster map).
5. `infra/*-policy.yaml`, `infra/gateway.yaml` — the networking that made it work.
6. `infra/podmonitoring.yaml` + `monitoring/` — observability.

# Ray Render Farm — Demo Proposal

> Distributed compute with **KubeRay on GKE**, built to the gke_all
> [feature contract](https://github.com/ragoler/gke_all/blob/main/feature.md).
> Runs **standalone** and as a **Hub feature**. Status: proposal, pre-code.

## 1. The demo in one paragraph

The user picks a region of the **Mandelbrot set**, a resolution, and a worker
cap, then hits **Launch**. The image paints in **tile-by-tile** as Ray fans the
work out across the cluster. Beside the canvas, a live **cluster map** shows
KubeRay worker pods spinning up on **Spot nodes** under load and draining back to
zero when idle. A **"Open Ray Dashboard ↗"** button opens the real Ray UI. Every
finished tile is labelled with the **pod that computed it**, so you literally see
the workload propagate and the results come back.

Three things are visible at once:
1. **Fan-out** — tiles painting in out of order, from different pods.
2. **Autoscaling** — pods appearing/draining in the cluster map (the KubeRay autoscaler).
3. **Ground truth** — the Ray Dashboard, the real cluster/tasks/timeline view.

---

## 2. How the workload propagates to pods (the part you asked about)

This is plain Ray scheduling + the KubeRay autoscaler. Step by step:

```
 Browser ──POST /render──▶ Controller (Ray driver)
                              │  ray.init("ray://ray-head:10001")
                              │  for each tile: render_tile.remote(spec)   ← 256 tasks
                              ▼
                          Ray head schedules tasks
                              │
        not enough workers? ──┤── pending task demand
                              ▼
                    Ray autoscaler asks KubeRay for more workers
                              │
                              ▼
              KubeRay creates worker pods on Spot nodes (ComputeClass)
                              │  pods register with the head
                              ▼
              pending tasks schedule onto the new workers
                              │  each task computes one tile
                              ▼
        task returns {index, png, POD_NAME}  ──ObjectRef──▶ driver collects
```

Key mechanics:

- **Tasks → pods.** The driver launches one `@ray.remote` task per tile. Ray's
  scheduler places them across all worker processes. If there aren't enough, the
  tasks sit **PENDING**, which is exactly the signal the **Ray autoscaler** uses
  to request more worker pods from KubeRay (up to `maxReplicas`). KubeRay
  schedules those pods onto **Spot nodes** selected by a `ComputeClass`.
- **Pod attribution, the easy way.** Each RayCluster worker pod gets `POD_NAME`
  injected via the **downward API**. A task just returns
  `os.environ["POD_NAME"]` alongside its pixels — so every tile *knows which pod
  made it*. No log-scraping or node-id correlation needed.
- **Results collection.** The driver collects with a streaming
  `ready, pending = ray.wait(refs, num_returns=1)` loop. As each `ObjectRef`
  resolves, `ray.get(ref)` yields `{index, x, y, w, h, png, pod_name, ms}`, which
  the controller forwards to the browser immediately (see §3). The canvas draws
  the tile at its position; the cluster map flashes the contributing pod.
- **Scale-down.** After the render, the autoscaler's `idleTimeout` elapses and
  worker pods drain back to `minReplicas` (0). The cluster map shows them vanish.

So "how does it propagate / how are results collected" = **Ray tasks +
autoscaler-driven pods + `ray.wait` streaming + downward-API pod tagging.**

---

## 3. Architecture & data flow

```
┌───────────────── Hub playroom (browser) ─────────────────┐
│  controls · tile canvas · cluster map · dashboard button  │
└───────┬───────────────────────────────────┬──────────────┘
        │ control plane (admin JWT)          │ data plane (CORS, direct)
        ▼                                     ▼
   hub_router  ──get_gateway_ip / proxy──▶  Gateway IP ──▶ HTTPRoute
   (/api/features/ray, MOCK here)               │            ├─▶ controller Service
                                                │            └─▶ ray-head :8265 (dashboard)
                                                ▼
                                         Controller pod  ── ray://ray-head:10001 ──▶ RayCluster
                                         (= deployment_name)                          head + autoscaling
                                                                                      Spot worker group
```

Two channels, on purpose:

- **Control plane** (through `hub_router`, behind the Hub JWT): resolve the
  Gateway IP, return the Ray Dashboard URL, status, and **all MOCK responses**.
- **Data plane** (browser → Gateway IP directly, with CORS): submit the render
  and **stream tiles via SSE**, plus poll `/workers`. The contract explicitly
  allows data-plane calls to hit the Gateway IP with CORS — and SSE streams far
  more cleanly direct than tunnelled through the JWT layer.

### The controller is the Ray driver
The controller (a small FastAPI app) connects to the cluster with the **Ray
Client** (`ray://ray-head:10001`), launches the tile tasks, and streams results
out over SSE. Ray Client requires the client and cluster Ray versions to match —
fine here, since the controller and the RayCluster use the **same image**.

> Alternative considered: the **Ray Job Submission API** (driver runs *inside*
> the cluster, populates the Dashboard "Jobs" tab). Cleaner dashboard story, but
> results then need a side channel (GCS or an HTTP callback) to reach the
> browser. Recommendation: **Ray Client** for simplicity + live streaming; tasks
> still appear in the Dashboard's cluster/tasks/timeline views. (Open question Q3.)

---

## 4. Opening the Ray Dashboard

The Ray head serves the Dashboard **and** the Job API on port **8265**. Plan:

- Add a second **HTTPRoute** on the feature's Gateway:
  `path /ray-dashboard` → `URLRewrite ReplacePrefixMatch /` → `ray-head:8265`.
- The playroom shows **"Open Ray Dashboard ↗"** → `http://<gateway-ip>/ray-dashboard`
  in a new tab; `hub_router` returns the resolved URL.

**Caveat:** the Ray Dashboard is a single-page app; serving it under a path
prefix with a rewrite can break some asset/websocket URLs. Mitigations, in order
of preference:
1. Prefix + `ReplacePrefixMatch` rewrite (try first — usually works on Ray 2.x).
2. A tiny reverse-proxy route in the controller that proxies `/dashboard/*` to
   `:8265` and fixes links.
3. A dedicated hostname on the Gateway (needs DNS) — cleanest but heaviest.

**Two notes to confirm:**
- This keeps **one primary UI model** (the hub-hosted playroom); the dashboard is
  just a deep-link our router returns, not a second `entrypoint_service`. (Q1)
- Exposing the dashboard via the Gateway IP is **unauthenticated**. Acceptable for
  an ephemeral demo namespace; flagged so we decide intentionally. (Q2)

---

## 4a. Namespacing

The feature has two scopes, on purpose:

- **Per-namespace** (everything in `infra/`): the `RayCluster` (head + workers),
  the controller, Service, Gateway, HTTPRoute, and RBAC all deploy into the
  feature's own namespace — **`gke-showcase-ray`** in the Hub (created + namespace-
  rewritten by the Hub), or **`default`** standalone (`NAMESPACE=default`).
  Teardown deletes the whole namespace, taking every Ray pod with it.
- **Cluster-scoped** (everything in `cluster/`), shared once per cluster, **not**
  per feature: the **KubeRay operator** (installed once, watches `RayCluster` CRs
  across all namespaces, lives in its own `kuberay-operator` namespace; CRDs are
  cluster-wide) and the Spot **`ComputeClass`** (the Hub auto-routes a
  `ComputeClass` to the cluster-scoped API even if it sits under `infra/`).

Namespace-portability rules to honor when building (no hardcoded `default`):
- The controller reaches the head as `ray://ray-head:10001` — a **bare service
  name** that resolves within the feature namespace; read `POD_NAMESPACE` via the
  downward API where an explicit namespace is unavoidable.
- All `HTTPRoute` `backendRef`s (→ controller, → `ray-head:8265`) and the
  `parentRef` (→ Gateway) stay **namespace-relative** so the Hub's rewrite keeps
  them consistent.
- RBAC `Role`/`RoleBinding` (controller listing Ray worker pods) are namespaced;
  the admin SA may `bind`/`escalate`, so shipping them is fine.

## 5. Repository layout (mapped to the contract)

```
ray/
├── feature.yaml                 # Hub descriptor
├── README.md                    # standalone usage
├── cluster/                     # cluster-scoped, applied once at bootstrap
│   ├── kuberay-operator/        # pinned KubeRay operator (kustomize → apply -k)
│   │   └── kustomization.yaml
│   └── spot-computeclass.yaml   # Spot CPU ComputeClass (auto-routed to cluster scope)
├── infra/                       # per-namespace, applied each deploy
│   ├── raycluster.yaml          # RayCluster: head + autoscaling Spot worker group (min 0)
│   ├── deployment.yaml          # controller  (metadata.name = deployment_name)
│   ├── service.yaml             # controller Service
│   ├── rbac.yaml                # SA + Role to list Ray worker pods (cluster map)
│   ├── gateway.yaml             # Gateway (name == gateway.name)
│   └── http-route.yaml          # routes: / → controller, /ray-dashboard → ray-head:8265
├── app/
│   ├── Dockerfile               # Ray base image + controller + tile task module
│   ├── controller.py            # FastAPI: /render (SSE), /workers, /healthz, CORS
│   ├── tasks.py                 # @ray.remote render_tile(spec) → {png, POD_NAME}
│   └── requirements.txt
├── frontend/                    # hub-hosted playroom
│   ├── index.html
│   ├── app.js                   # controls, canvas painter, cluster map, dashboard btn
│   └── style.css
├── hub_router.py                # thin proxy + MOCK (module:attr → router)
└── setup_infra.sh               # standalone-only provisioning (Hub ignores)
```

### `feature.yaml` (draft)
```yaml
name: ray
title: Distributed Compute with Ray (KubeRay)
description: >-
  Render the Mandelbrot set as a distributed Ray job and watch KubeRay autoscale
  worker pods across Spot nodes in real time — tiles painting in, pods spinning
  up, results streaming back, with a live link to the Ray Dashboard.
gke_features:
  - KubeRay Operator
  - RayCluster Autoscaling
  - Spot VMs / ComputeClass
  - Distributed Task Fan-out

paths:
  infra_dir: infra
  cluster_dir: cluster
  frontend_dir: frontend
  playroom_slug: ray

deployment_name: ray-controller-deployment
gateway:
  name: ray-gateway
  class: gke-l7-gxlb

build:
  - image: ray-render-farm
    context: app
    dockerfile: app/Dockerfile

template_vars: [NAMESPACE, PROJECT_NAME, REGION, ARTIFACT_REGISTRY_REPO]
hub_router: "hub_router:router"
```

---

## 6. API surface

Controller (data plane, Gateway IP, CORS `*`):
- `GET  /healthz` → `{"status":"ok"}`
- `POST /render` `{region|preset, zoom, resolution, max_workers}` → `{job_id, tiles}`
- `GET  /render/{job_id}/stream` → **SSE**: one event per completed tile
  `{index,x,y,w,h,png_base64,pod_name,ms}`, then `done`.
- `GET  /workers` → `[{pod_name,node,status,cpu,tasks_running}]` (k8s/Ray State API)

Hub router (`/api/features/ray`, admin JWT, thin):
- `GET /config` → `{gateway_ip, dashboard_url, mode}`
- MOCK mirrors of the above when `MODE=MOCK`.

---

## 7. Mock mode (offline)

`MODE=MOCK` must fully animate with no cluster. `hub_router` serves a deterministic
script: a fixed Mandelbrot tile sequence with synthetic `pod_name`s and a canned
worker scale-up→drain curve. The frontend detects mock via `/config` and drives
the same canvas + cluster-map code paths. No imports require a live cluster.

---

## 8. What each GKE capability looks like in the demo

| Capability | Where the audience sees it |
|---|---|
| KubeRay Operator | the `RayCluster` CR becomes head+worker pods |
| RayCluster Autoscaling | cluster map: pods appear under load, drain when idle |
| Spot VMs / ComputeClass | workers land on Spot nodes; cheap, preemptible |
| Distributed Task Fan-out | tiles paint in from multiple pods, out of order |
| Ray Dashboard | "Open Dashboard ↗" → real cluster/tasks/timeline view |

---

## 9. Open questions to confirm before coding

- **Q1 — Dashboard as a deep-link** inside the hub playroom (not a second
  `entrypoint_service`): OK? *(Recommend: yes.)*
- **Q2 — Unauthenticated dashboard** via the Gateway IP for the demo namespace:
  acceptable, or should we gate it? *(Recommend: acceptable for ephemeral demo.)*
- **Q3 — Ray Client vs Job Submission API** for the driver. *(Recommend: Ray
  Client for live streaming; revisit if the Dashboard "Jobs" tab matters.)*
- **Q4 — Worker cap semantics.** Drive scale via resolution with a fixed
  RayCluster `maxReplicas`, or let the UI knob set the cap? *(Recommend: fixed
  max at deploy; resolution is the live driver.)*
- **Q5 — GPU?** Default CPU-only Spot (cheap, simple). Add an optional GPU worker
  group later? *(Recommend: CPU-only for v1.)*

---

## 10. Suggested build order (once approved)

1. `feature.yaml` + repo skeleton.
2. `app/` — `tasks.py` + `controller.py` + Dockerfile; run Ray locally to validate
   tile fan-out, `ray.wait` streaming, and `POD_NAME` tagging.
3. `frontend/` — canvas painter + cluster map against the mock first.
4. `hub_router.py` + MOCK mode; standalone playroom works offline end-to-end.
5. `infra/` — RayCluster (autoscaling), controller, Gateway/HTTPRoute (+dashboard route), RBAC.
6. `cluster/` — KubeRay operator (pinned) + Spot ComputeClass.
7. Standalone deploy on GKE; tune autoscaler `idleTimeout`/min/max for demo feel.
8. Hub integration test (MOCK): list → deploy → hit router → teardown.
```

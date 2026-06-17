/* Ray Render Farm — playroom frontend.
 *
 * Two call surfaces:
 *   - control plane: `/api/features/ray/...` (Hub, JWT). Used for /config,
 *     /presets, and ALL calls when MODE=MOCK.
 *   - data plane: the Gateway IP directly (CORS, no auth). Used for /render,
 *     the SSE tile stream, and /workers when running LIVE.
 *
 * SSE is consumed via fetch+ReadableStream (not EventSource) so we can attach
 * the admin JWT in MOCK mode — EventSource cannot set headers.
 */

const HUB_BASE = "/api/features/ray";

const els = {
  mode: document.getElementById("mode-badge"),
  dash: document.getElementById("dashboard-btn"),
  metrics: document.getElementById("metrics-btn"),
  preset: document.getElementById("preset"),
  resolution: document.getElementById("resolution"),
  maxIter: document.getElementById("max_iter"),
  maxIterOut: document.getElementById("max_iter_out"),
  launch: document.getElementById("launch"),
  progress: document.getElementById("progress"),
  canvas: document.getElementById("canvas"),
  pods: document.getElementById("pods"),
  workerCount: document.getElementById("worker-count"),
  statTiles: document.getElementById("stat-tiles"),
  statPods: document.getElementById("stat-pods"),
  statTput: document.getElementById("stat-tput"),
  statElapsed: document.getElementById("stat-elapsed"),
};

const ctx = els.canvas.getContext("2d");
let cfg = { mode: "MOCK", dataBase: HUB_BASE, dashboard_url: null };
let pods = new Map(); // pod_name -> {type, status, count, el}
let workersTimer = null;

/* ---- auth + bases ----------------------------------------------------- */
function jwt() {
  return localStorage.getItem("admin_jwt") || "";
}
function hubHeaders() {
  const h = { "Content-Type": "application/json" };
  const t = jwt();
  if (t) h["Authorization"] = `Bearer ${t}`;
  return h;
}
// In MOCK everything flows through the Hub (JWT). In LIVE the data plane hits
// the Gateway IP with CORS and no auth.
function dataHeaders() {
  return cfg.mode === "MOCK" ? hubHeaders() : { "Content-Type": "application/json" };
}

/* ---- config / bootstrap ---------------------------------------------- */
async function loadConfig() {
  // Allow a standalone override: ?api=http://IP points the data plane directly.
  const override = new URLSearchParams(location.search).get("api");
  try {
    const r = await fetch(`${HUB_BASE}/config`, { headers: hubHeaders() });
    if (r.ok) {
      const c = await r.json();
      cfg.mode = c.mode || "LIVE";
      cfg.dashboard_url = c.dashboard_url || null;
      cfg.dataBase =
        cfg.mode === "MOCK"
          ? HUB_BASE
          : override || (c.gateway_ip ? `http://${c.gateway_ip}` : HUB_BASE);
      return;
    }
  } catch (_) {
    /* fall through to standalone */
  }
  // Standalone: no Hub. Talk to the controller directly.
  cfg.mode = "LIVE";
  cfg.dataBase = override || location.origin;
}

async function loadPresets() {
  const url = cfg.mode === "MOCK" ? `${HUB_BASE}/presets` : `${cfg.dataBase}/presets`;
  try {
    const r = await fetch(url, { headers: dataHeaders() });
    const data = await r.json();
    els.preset.innerHTML = "";
    Object.keys(data).forEach((name) => {
      const o = document.createElement("option");
      o.value = name;
      o.textContent = name;
      els.preset.appendChild(o);
    });
  } catch (_) {
    ["overview", "seahorse", "spiral", "elephant", "minibrot"].forEach((n) => {
      const o = document.createElement("option");
      o.value = n; o.textContent = n; els.preset.appendChild(o);
    });
  }
}

function applyConfigUI() {
  els.mode.textContent = cfg.mode;
  els.mode.className = "badge " + (cfg.mode === "MOCK" ? "badge-mock" : "badge-live");
}

// The Ray Dashboard has its own LoadBalancer; its IP is reported by the
// controller's /dashboard endpoint (it may be null until the LB gets an IP).
async function refreshDashboard() {
  if (cfg.mode === "MOCK" || !els.dash.hidden) return;
  try {
    const r = await fetch(`${cfg.dataBase}/dashboard`, { headers: dataHeaders() });
    if (!r.ok) return;
    const { url } = await r.json();
    if (url) {
      els.dash.href = url;
      els.dash.hidden = false;
    }
  } catch (_) {
    /* not ready yet */
  }
}

// Cloud Monitoring dashboard link (set by deploy into the ray-links ConfigMap).
async function refreshMetrics() {
  if (cfg.mode === "MOCK" || !els.metrics.hidden) return;
  try {
    const r = await fetch(`${cfg.dataBase}/metrics-link`, { headers: dataHeaders() });
    if (!r.ok) return;
    const { url } = await r.json();
    if (url) {
      els.metrics.href = url;
      els.metrics.hidden = false;
    }
  } catch (_) {
    /* not ready yet */
  }
}

/* ---- cluster map ------------------------------------------------------ */
function ensurePod(name, type, status) {
  let p = pods.get(name);
  if (!p) {
    const el = document.createElement("div");
    el.className = `pod ${type === "head" ? "head" : "worker"}`;
    el.innerHTML =
      `<span class="dot"></span>` +
      `<span class="pname">${name}</span>` +
      `<span class="pcount">0</span>`;
    els.pods.appendChild(el);
    p = { type, status, count: 0, el };
    pods.set(name, p);
  }
  if (status) {
    p.status = status;
    p.el.classList.toggle("pending", status !== "Running");
  }
  return p;
}

function bumpPod(name) {
  const p = ensurePod(name, "worker");
  p.count += 1;
  p.el.querySelector(".pcount").textContent = p.count;
  p.el.classList.add("flash");
  setTimeout(() => p.el.classList.remove("flash"), 220);
}

async function pollWorkers() {
  const url = cfg.mode === "MOCK" ? `${HUB_BASE}/workers` : `${cfg.dataBase}/workers`;
  try {
    const r = await fetch(url, { headers: dataHeaders() });
    if (!r.ok) return;
    const data = await r.json();
    const seen = new Set();
    (data.pods || []).forEach((pod) => {
      seen.add(pod.pod_name);
      const type = pod.node_type === "head" ? "head" : "worker";
      ensurePod(pod.pod_name, type, pod.status);
    });
    // Drop pods that the autoscaler has removed (and that aren't mid-render).
    for (const [name, p] of pods) {
      if (!seen.has(name) && p.count === 0) {
        p.el.remove();
        pods.delete(name);
      }
    }
    const running = [...pods.values()].filter((p) => p.status === "Running").length;
    els.workerCount.textContent = `· ${pods.size} pod(s)`;
    els.statPods.textContent = running || pods.size;
  } catch (_) {
    /* transient */
  }
}

/* ---- render ----------------------------------------------------------- */
function resetCanvas(size) {
  els.canvas.width = size;
  els.canvas.height = size;
  ctx.fillStyle = "#000";
  ctx.fillRect(0, 0, size, size);
}

function drawTile(t) {
  const img = new Image();
  img.onload = () => ctx.drawImage(img, t.x, t.y, t.w, t.h);
  img.src = "data:image/png;base64," + t.png_base64;
}

async function streamSSE(url, headers, onMsg) {
  const r = await fetch(url, { headers });
  if (!r.ok || !r.body) throw new Error(`stream failed: ${r.status}`);
  const reader = r.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let i;
    while ((i = buf.indexOf("\n\n")) >= 0) {
      const chunk = buf.slice(0, i);
      buf = buf.slice(i + 2);
      const line = chunk.split("\n").find((l) => l.startsWith("data:"));
      if (line) onMsg(JSON.parse(line.slice(5).trim()));
    }
  }
}

async function runRender() {
  els.launch.disabled = true;
  // Reset render state (keep discovered pods; zero their counts).
  for (const p of pods.values()) {
    p.count = 0;
    p.el.querySelector(".pcount").textContent = "0";
  }

  const size = parseInt(els.resolution.value, 10);
  resetCanvas(size);

  const body = JSON.stringify({
    preset: els.preset.value,
    resolution: size,
    max_iter: parseInt(els.maxIter.value, 10),
  });

  let total = 0;
  let done = 0;
  const t0 = performance.now();
  const tick = setInterval(() => {
    els.statElapsed.textContent = ((performance.now() - t0) / 1000).toFixed(1) + " s";
    const secs = (performance.now() - t0) / 1000;
    els.statTput.textContent = (secs > 0 ? (done / secs).toFixed(1) : "0") + " /s";
  }, 100);

  try {
    const renderUrl = cfg.mode === "MOCK" ? `${HUB_BASE}/render` : `${cfg.dataBase}/render`;
    const r = await fetch(renderUrl, { method: "POST", headers: dataHeaders(), body });
    if (!r.ok) throw new Error(`render failed: ${r.status}`);
    const { job_id } = await r.json();

    const streamUrl =
      cfg.mode === "MOCK"
        ? `${HUB_BASE}/render/${job_id}/stream`
        : `${cfg.dataBase}/render/${job_id}/stream`;

    await streamSSE(streamUrl, dataHeaders(), (m) => {
      if (m.type === "meta") {
        total = m.tiles;
        els.statTiles.textContent = `0 / ${total}`;
        els.progress.textContent = `· rendering ${total} tiles`;
      } else if (m.type === "tile") {
        done += 1;
        drawTile(m);
        bumpPod(m.pod_name);
        els.statTiles.textContent = `${done} / ${total}`;
      } else if (m.type === "done") {
        els.progress.textContent = `· done in ${(m.elapsed_ms / 1000).toFixed(1)}s`;
      } else if (m.type === "error") {
        els.progress.textContent = `· error: ${m.message}`;
      }
    });
  } catch (e) {
    els.progress.textContent = `· ${e.message}`;
  } finally {
    clearInterval(tick);
    els.launch.disabled = false;
  }
}

/* ---- init ------------------------------------------------------------- */
els.maxIter.addEventListener("input", () => (els.maxIterOut.textContent = els.maxIter.value));
els.launch.addEventListener("click", runRender);

(async function init() {
  await loadConfig();
  applyConfigUI();
  await loadPresets();
  resetCanvas(parseInt(els.resolution.value, 10));
  pollWorkers();
  refreshDashboard();
  refreshMetrics();
  workersTimer = setInterval(() => { pollWorkers(); refreshDashboard(); refreshMetrics(); }, 1500);
})();

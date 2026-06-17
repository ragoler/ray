"""Ray remote tasks for the distributed Mandelbrot render.

Each tile of the output image is computed by one ``@ray.remote`` task. Ray's
scheduler spreads those tasks across worker pods; when there aren't enough
workers the tasks sit PENDING, which is what drives the KubeRay autoscaler to
add worker pods on Spot nodes.

Every task returns the name of the pod that computed it (``POD_NAME``, injected
via the Kubernetes downward API on the RayCluster worker spec). That is how the
frontend attributes each tile to a specific pod — no log scraping required.
"""

from __future__ import annotations

import base64
import io
import os
import socket
import time
from dataclasses import dataclass

import ray


@dataclass
class TileSpec:
    """A single tile of the output image."""

    index: int
    # Pixel position / size within the full image.
    px: int
    py: int
    pw: int
    ph: int
    # Complex-plane window for the *whole* image.
    cx_min: float
    cx_max: float
    cy_min: float
    cy_max: float
    # Full image dimensions (so the task can map pixels -> complex plane).
    width: int
    height: int
    max_iter: int


def _pod_identity() -> str:
    """Best-effort identity of the worker running this task.

    Prefers POD_NAME (downward API). Falls back to hostname so the demo still
    attributes tiles to a worker when run outside Kubernetes (local Ray).
    """
    return os.environ.get("POD_NAME") or socket.gethostname()


def _render_tile_pixels(spec: TileSpec) -> bytes:
    """Compute the Mandelbrot escape values for one tile -> PNG bytes.

    Uses numpy + Pillow. Kept dependency-light and CPU-only on purpose: the
    point of the demo is parallel fan-out, not raw per-tile speed.
    """
    import numpy as np
    from PIL import Image

    # Map this tile's pixel block to the complex plane.
    xs = np.linspace(
        spec.cx_min + (spec.cx_max - spec.cx_min) * (spec.px / spec.width),
        spec.cx_min + (spec.cx_max - spec.cx_min) * ((spec.px + spec.pw) / spec.width),
        spec.pw,
        endpoint=False,
    )
    ys = np.linspace(
        spec.cy_min + (spec.cy_max - spec.cy_min) * (spec.py / spec.height),
        spec.cy_min + (spec.cy_max - spec.cy_min) * ((spec.py + spec.ph) / spec.height),
        spec.ph,
        endpoint=False,
    )
    c = xs[np.newaxis, :] + 1j * ys[:, np.newaxis]
    z = np.zeros_like(c)
    div_iter = np.zeros(c.shape, dtype=np.int32)
    alive = np.ones(c.shape, dtype=bool)

    for i in range(spec.max_iter):
        z[alive] = z[alive] * z[alive] + c[alive]
        escaped = alive & (np.abs(z) > 2.0)
        div_iter[escaped] = i
        alive &= ~escaped
        if not alive.any():
            break
    div_iter[alive] = spec.max_iter

    # Colorize: smooth-ish palette from the escape iteration count.
    t = div_iter.astype(np.float32) / float(spec.max_iter)
    r = np.clip(9 * (1 - t) * t * t * t * 255, 0, 255)
    g = np.clip(15 * (1 - t) * (1 - t) * t * t * 255, 0, 255)
    b = np.clip(8.5 * (1 - t) * (1 - t) * (1 - t) * t * 255, 0, 255)
    rgb = np.dstack([r, g, b]).astype(np.uint8)
    # Interior (never escaped) -> black.
    rgb[div_iter >= spec.max_iter] = 0

    img = Image.fromarray(rgb, mode="RGB")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


@ray.remote
def render_tile(spec: TileSpec) -> dict:
    """Remote entrypoint: compute one tile and report who computed it."""
    start = time.time()
    png = _render_tile_pixels(spec)
    return {
        "index": spec.index,
        "x": spec.px,
        "y": spec.py,
        "w": spec.pw,
        "h": spec.ph,
        "png_base64": base64.b64encode(png).decode("ascii"),
        "pod_name": _pod_identity(),
        "ms": int((time.time() - start) * 1000),
    }


def build_tile_specs(
    *,
    width: int,
    height: int,
    tile: int,
    center_x: float,
    center_y: float,
    zoom: float,
    max_iter: int,
) -> list[TileSpec]:
    """Split an image into ``tile``x``tile`` blocks over a complex-plane window.

    ``zoom`` is the half-width of the complex window on the real axis; larger
    zoom => more zoomed out. The window is aspect-corrected to the image.
    """
    aspect = height / width
    cx_min, cx_max = center_x - zoom, center_x + zoom
    cy_min, cy_max = center_y - zoom * aspect, center_y + zoom * aspect

    specs: list[TileSpec] = []
    index = 0
    for py in range(0, height, tile):
        for px in range(0, width, tile):
            pw = min(tile, width - px)
            ph = min(tile, height - py)
            specs.append(
                TileSpec(
                    index=index,
                    px=px,
                    py=py,
                    pw=pw,
                    ph=ph,
                    cx_min=cx_min,
                    cx_max=cx_max,
                    cy_min=cy_min,
                    cy_max=cy_max,
                    width=width,
                    height=height,
                    max_iter=max_iter,
                )
            )
            index += 1
    return specs

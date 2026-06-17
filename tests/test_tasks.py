"""Unit tests for the Mandelbrot tile logic (no Ray cluster needed)."""

import base64

import pytest

pytest.importorskip("numpy")
pytest.importorskip("PIL")
pytest.importorskip("ray")

import tasks  # noqa: E402


def test_build_tile_specs_tiles_cover_image():
    specs = tasks.build_tile_specs(
        width=256, height=256, tile=128,
        center_x=-0.5, center_y=0.0, zoom=1.6, max_iter=64,
    )
    assert len(specs) == 4  # 2x2
    assert {s.index for s in specs} == {0, 1, 2, 3}
    # Tiles fully cover the image with no gaps/overlap.
    covered = sum(s.pw * s.ph for s in specs)
    assert covered == 256 * 256


def test_build_tile_specs_handles_ragged_edges():
    specs = tasks.build_tile_specs(
        width=300, height=300, tile=128,
        center_x=-0.5, center_y=0.0, zoom=1.6, max_iter=32,
    )
    assert len(specs) == 9  # 3x3 (128,128,44)
    edge = [s for s in specs if s.pw == 44 or s.ph == 44]
    assert edge, "expected ragged edge tiles"


def test_render_tile_pixels_returns_valid_png():
    spec = tasks.build_tile_specs(
        width=128, height=128, tile=128,
        center_x=-0.5, center_y=0.0, zoom=1.6, max_iter=64,
    )[0]
    png = tasks._render_tile_pixels(spec)
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_render_tile_payload_shape(monkeypatch):
    """Call the remote function's wrapped body directly (no Ray runtime)."""
    monkeypatch.setenv("POD_NAME", "ray-render-farm-worker-3")
    spec = tasks.build_tile_specs(
        width=128, height=128, tile=128,
        center_x=-0.5, center_y=0.0, zoom=1.6, max_iter=48,
    )[0]
    # render_tile is a Ray remote; invoke the undecorated function body.
    fn = tasks.render_tile._function  # underlying python callable
    out = fn(spec)
    assert out["pod_name"] == "ray-render-farm-worker-3"
    assert out["index"] == 0
    assert {"x", "y", "w", "h", "png_base64", "ms"} <= out.keys()
    base64.b64decode(out["png_base64"])  # decodes cleanly

"""Shared test fixtures / path setup."""

import os
import pathlib
import sys

# MOCK so hub_router never tries to reach a cluster.
os.environ.setdefault("MODE", "MOCK")

ROOT = pathlib.Path(__file__).resolve().parents[1]
# Make `hub_router`, and `app/` modules (tasks, controller) importable.
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "app"))

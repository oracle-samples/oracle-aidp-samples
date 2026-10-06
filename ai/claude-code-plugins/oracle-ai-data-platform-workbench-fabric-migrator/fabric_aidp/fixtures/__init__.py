"""Bundled demo estate.

`demo-workspace/` is a real Fabric Git export, committed so `--fixture demo`
works from a wheel with no network and no Azure credentials. It is read
through the same `discover_items` and scanners a customer export goes
through -- a fixture that bypassed them would prove nothing.
"""
from __future__ import annotations

from pathlib import Path

DEMO_WORKSPACE = "demo-workspace"


def demo_workspace_path() -> Path:
    """Filesystem path to the bundled demo Fabric Git export."""
    return Path(__file__).resolve().parent / DEMO_WORKSPACE


__all__ = ["demo_workspace_path", "DEMO_WORKSPACE"]

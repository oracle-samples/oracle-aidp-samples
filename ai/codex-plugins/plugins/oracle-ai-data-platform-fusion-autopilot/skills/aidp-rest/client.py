"""Skill shim — re-exports AidpRestClient from the plugin source.

The canonical AidpRestClient lives at
``scripts/oracle_ai_data_platform_fusion_autopilot/dispatch/rest_client.py``.
This shim adds the plugin checkout's ``scripts/`` directory to ``sys.path``
BEFORE importing, because skill scripts run directly from a checkout — the
plugin is not pip-installed when they import this module.

Path resolution: ``__file__`` lives at
``<checkout>/skills/aidp-rest/client.py``. ``parents[2]`` is the
checkout root; ``<checkout>/scripts/`` then makes
``oracle_ai_data_platform_fusion_autopilot.*`` importable.
"""
from __future__ import annotations

import sys
from pathlib import Path

_PLUGIN_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if _PLUGIN_SCRIPTS.is_dir() and str(_PLUGIN_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_SCRIPTS))

from oracle_ai_data_platform_fusion_autopilot.dispatch.rest_client import (  # noqa: E402, F401
    API_VERSION,
    DEFAULT_TIMEOUT_S,
    AidpRestClient,
    AidpRestError,
    ClusterSummary,
    RunResult,
    WorkspaceSummary,
)

__all__ = [
    "API_VERSION",
    "DEFAULT_TIMEOUT_S",
    "AidpRestClient",
    "AidpRestError",
    "ClusterSummary",
    "RunResult",
    "WorkspaceSummary",
]

"""Wire-through markers: prove customer YAML values reach downstream consumers.

Sets unforgeable marker strings in cfg, invokes each converted consumer, and
grep-asserts the markers appear in the produced artifact. Fails when a
consumer silently defaults to something else or drops the field.

Runs against the REFACTORED code — verifies Session 1 wiring end-to-end.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[2]
for p in (str(_REPO_ROOT), str(_REPO_ROOT / "scripts"), str(_REPO_ROOT / "dbx_export")):
    if p not in sys.path:
        sys.path.insert(0, p)


# ─── recommended_env_vars(cfg) ─────────────────────────────────────────────

def test_recommended_env_vars_reads_mlflow_from_cfg():
    """cfg.secrets.mlflow_tracking_uri → MLFLOW_TRACKING_URI cluster env."""
    # Isolate env: nothing preset
    old = os.environ.pop("MLFLOW_TRACKING_URI", None)
    try:
        from dbx_export.cluster_converter import recommended_env_vars
        cfg = SimpleNamespace(
            secrets=SimpleNamespace(mlflow_tracking_uri="MARKER_MLFLOW_URL_9f3a"),
        )
        out = recommended_env_vars(cfg)
        assert out.get("MLFLOW_TRACKING_URI") == "MARKER_MLFLOW_URL_9f3a"
    finally:
        if old is not None:
            os.environ["MLFLOW_TRACKING_URI"] = old


def test_recommended_env_vars_absent_when_cfg_lacks_mlflow():
    """No secrets.mlflow_tracking_uri → no env var injected."""
    old = os.environ.pop("MLFLOW_TRACKING_URI", None)
    try:
        from dbx_export.cluster_converter import recommended_env_vars
        cfg = SimpleNamespace(secrets=SimpleNamespace())
        out = recommended_env_vars(cfg)
        assert "MLFLOW_TRACKING_URI" not in out
    finally:
        if old is not None:
            os.environ["MLFLOW_TRACKING_URI"] = old


def test_recommended_env_vars_operator_env_wins():
    """Operator $MLFLOW_TRACKING_URI wins over cfg."""
    os.environ["MLFLOW_TRACKING_URI"] = "MARKER_FROM_ENV_92cf"
    try:
        from dbx_export.cluster_converter import recommended_env_vars
        cfg = SimpleNamespace(
            secrets=SimpleNamespace(mlflow_tracking_uri="MARKER_FROM_CFG_92cf"),
        )
        out = recommended_env_vars(cfg)
        assert out["MLFLOW_TRACKING_URI"] == "MARKER_FROM_ENV_92cf"
    finally:
        os.environ.pop("MLFLOW_TRACKING_URI", None)


def test_recommended_env_vars_no_cfg_no_env_returns_empty():
    """No cfg + no env → empty dict (no legacy fallback to constants.py)."""
    old = os.environ.pop("MLFLOW_TRACKING_URI", None)
    try:
        from dbx_export.cluster_converter import recommended_env_vars
        out = recommended_env_vars(None)
        assert out == {}
    finally:
        if old is not None:
            os.environ["MLFLOW_TRACKING_URI"] = old


def test_recommended_env_vars_injects_aidp_workspace_id_from_cfg():
    """cfg.aidp.workspace_id must be injected as AIDP_WORKSPACE_ID env var so
    aidp_compat/notebook.py's mock context returns the customer's real workspace
    UUID (not a hardcoded literal)."""
    from dbx_export.cluster_converter import recommended_env_vars
    cfg = SimpleNamespace(
        aidp=SimpleNamespace(workspace_id="MARKER_WORKSPACE_92cf"),
        secrets=SimpleNamespace(),
    )
    out = recommended_env_vars(cfg)
    assert out.get("AIDP_WORKSPACE_ID") == "MARKER_WORKSPACE_92cf"


def test_recommended_env_vars_no_workspace_id_when_cfg_lacks_aidp():
    """If cfg has no aidp section, AIDP_WORKSPACE_ID is not injected."""
    from dbx_export.cluster_converter import recommended_env_vars
    cfg = SimpleNamespace(secrets=SimpleNamespace())
    out = recommended_env_vars(cfg)
    assert "AIDP_WORKSPACE_ID" not in out


# ─── convert_dbx_cluster: spark_config + autoscale_config baseline ─────────

def test_convert_dbx_cluster_bakes_spark_config_from_cfg():
    """cfg.spark_config.<key>=v → final spark_conf has that value (tier-1 fill)."""
    from dbx_export.cluster_converter import convert_dbx_cluster
    cfg = SimpleNamespace(
        secrets=SimpleNamespace(),
        spark_config={"spark.sql.shuffle.partitions": "MARKER_1234"},
        autoscale_config={},
    )
    spec = {
        "num_workers": 1,
        "node_type_id": "Standard_DS3_v2",
        "spark_conf": {},
    }
    result = convert_dbx_cluster(spec, "t", prefix="ai_tool", cfg=cfg)
    conf = result.aidp_payload.get("clusterRuntimeConfig", {}).get("sparkAdvancedConfigurations", {})
    assert conf.get("spark.sql.shuffle.partitions") == "MARKER_1234"


def test_convert_dbx_cluster_dbx_source_wins_over_cfg_baseline():
    """A source cluster's valid spark_conf value beats the cfg baseline (tier-2 win).

    This is the auto-heal contract: baseline only fills gaps; the customer's
    original tuning stays unless the value is malformed for its expected type.
    Use a VALID "size" value ("400m") so it survives type-inference and reaches
    the payload verbatim — a malformed value would be auto-healed or replaced.
    """
    from dbx_export.cluster_converter import convert_dbx_cluster
    baseline_key = "spark.sql.autoBroadcastJoinThreshold"
    cfg = SimpleNamespace(
        secrets=SimpleNamespace(),
        spark_config={baseline_key: "200m"},   # baseline
        autoscale_config={},
    )
    spec = {
        "num_workers": 1,
        "node_type_id": "Standard_DS3_v2",
        "spark_conf": {baseline_key: "400m"},   # DBX source value — valid size
    }
    result = convert_dbx_cluster(spec, "t", prefix="ai_tool", cfg=cfg)
    conf = result.aidp_payload.get("clusterRuntimeConfig", {}).get("sparkAdvancedConfigurations", {})
    assert conf.get(baseline_key) == "400m"


# ─── context_tools: PRIORITY_NAMESPACES removed (Option A simplification) ──

def test_context_tools_no_priority_namespaces_symbol():
    """PRIORITY_NAMESPACES was removed in the Option-A simplification.
    Guards against accidental reintroduction."""
    import context_tools
    assert not hasattr(context_tools, "PRIORITY_NAMESPACES")

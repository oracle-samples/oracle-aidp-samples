"""Tests for the aggressive-autoscale defaults now sourced from the customer YAML.

These 6 spark.aidp.compute.autoscale.* keys are injected into every migrated
cluster's create payload (they never exist on a DBX source, so nothing
clobbers them). This pins:
  1. cfg.autoscale_config contents get baked into the cluster payload,
  2. an unrelated source spark_conf key is preserved,
  3. an explicit source value for an autoscale key wins over the baseline
     (two-tier merge — auto-heal only replaces MALFORMED values, not
     deliberate overrides),
  4. an empty autoscale_config dict opts out (no autoscale keys injected).
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # for dbx_export

from dbx_export.cluster_converter import convert_dbx_cluster  # noqa: E402

# The exact aggressive-autoscale defaults, matching default_aidp_migrator.yaml.
EXPECTED_AUTOSCALE = {
    "spark.aidp.compute.autoscale.schedulerBacklogTimeoutSeconds":          "1",
    "spark.aidp.compute.autoscale.sustainedSchedulerBacklogTimeoutSeconds": "1",
    "spark.aidp.compute.autoscale.allocation.batchDelayMillis":             "1000",
    "spark.aidp.compute.autoscale.allocation.batchSize":                    "10",
    "spark.aidp.compute.autoscale.allocation.maxPendingPods":               "60",
    "spark.aidp.compute.autoscale.executorIdleTimeoutSeconds":              "600",
}


def _cfg(spark_config: dict | None = None, autoscale_config: dict | None = None):
    """Build a minimal cfg namespace with just the blocks we care about."""
    return SimpleNamespace(
        spark_config=spark_config if spark_config is not None else {},
        autoscale_config=(
            autoscale_config if autoscale_config is not None else dict(EXPECTED_AUTOSCALE)
        ),
    )


def _spark_conf(spec, *, cfg=None):
    """Run the real production conversion and return the payload's spark configs."""
    if cfg is None:
        cfg = _cfg()
    payload = convert_dbx_cluster(spec, "test-cluster", prefix="ai_tool", cfg=cfg).aidp_payload
    return payload["clusterRuntimeConfig"]["sparkAdvancedConfigurations"]


# ── 1. cfg.autoscale_config gets baked in (baseline / tier-1 fill) ──────────

def test_autoscale_defaults_injected_from_cfg():
    conf = _spark_conf({"node_type_id": "m5.xlarge"})  # no spark_conf on source
    for key, value in EXPECTED_AUTOSCALE.items():
        assert conf.get(key) == value, key


def test_autoscale_defaults_survive_a_source_with_unrelated_confs():
    conf = _spark_conf({
        "node_type_id": "m5.xlarge",
        "spark_conf": {"spark.sql.shuffle.partitions": "800"},
    })
    for key, value in EXPECTED_AUTOSCALE.items():
        assert conf.get(key) == value, key
    assert conf["spark.sql.shuffle.partitions"] == "800"  # source value preserved


# ── 2. two-tier merge: an explicit source value wins (tier-2) ───────────────

def test_source_value_overrides_autoscale_default():
    # A source cluster that explicitly sets a valid int for one autoscale key
    # keeps the customer value (the auto-heal merge only replaces MALFORMED
    # values, not deliberate overrides).
    key = "spark.aidp.compute.autoscale.executorIdleTimeoutSeconds"
    conf = _spark_conf({
        "node_type_id": "m5.xlarge",
        "spark_conf": {key: "120"},
    })
    assert conf[key] == "120"                       # customer override kept
    for k, v in EXPECTED_AUTOSCALE.items():
        if k != key:
            assert conf.get(k) == v, k


# ── 3. opt-out via empty autoscale_config block ─────────────────────────────

def test_empty_autoscale_config_removes_autoscale_defaults():
    """Customer YAML with `autoscale_config: {}` should inject zero autoscale keys."""
    conf = _spark_conf(
        {"node_type_id": "m5.xlarge"},
        cfg=_cfg(autoscale_config={}),
    )
    for key in EXPECTED_AUTOSCALE:
        assert key not in conf, f"{key} should NOT be injected when autoscale_config is empty"


def test_cfg_none_injects_nothing():
    """Legacy callers that pass cfg=None get no baseline (existing degrade behavior)."""
    payload = convert_dbx_cluster(
        {"node_type_id": "m5.xlarge"}, "test-cluster", prefix="ai_tool", cfg=None
    ).aidp_payload
    conf = payload["clusterRuntimeConfig"]["sparkAdvancedConfigurations"]
    for key in EXPECTED_AUTOSCALE:
        assert key not in conf

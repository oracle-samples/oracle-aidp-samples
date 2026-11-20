"""Kryo serializer is enabled by default in the baseline spark_config, and is
create-time-only (excluded from the existing-cluster pre-flight advice).

`spark.serializer` is a STATIC Spark conf (read once at SparkContext creation), so
it only takes effect on newly-created clusters and must NOT be advised for existing
ones. A Hudi cross-tenancy KryoException is remediated by recreating the cluster
(buffer.max / Hudi registrar / Java) — not at runtime; see the yaml comments.

Run: python -m pytest tests/test_kryo_serializer_default.py -v
"""
import os
from types import SimpleNamespace

import yaml

from dbx_export.cluster_converter import recommended_spark_config

_ROOT = os.path.join(os.path.dirname(__file__), "..")
KRYO = "org.apache.spark.serializer.KryoSerializer"


def _spark_config(yaml_name):
    with open(os.path.join(_ROOT, yaml_name)) as f:
        return (yaml.safe_load(f) or {}).get("spark_config", {})


def test_default_yaml_enables_kryo():
    assert _spark_config("default_aidp_migrator.yaml").get("spark.serializer") == KRYO



def test_recommended_spark_config_includes_kryo():
    cfg = SimpleNamespace(spark_config={"spark.serializer": KRYO}, autoscale_config={})
    assert recommended_spark_config(cfg)["spark.serializer"] == KRYO


def test_untested_kryo_followups_not_shipped():
    # buffer.max / registrar are deliberately NOT in the baseline (deferred, unproven
    # against the pilot deployment's Hudi build) — add only after testing if a recurrence occurs.
    for y in ("default_aidp_migrator.yaml",):
        sc = _spark_config(y)
        assert "spark.kryoserializer.buffer.max" not in sc
        assert "spark.kryo.registrator" not in sc


def test_preflight_excludes_serializer_for_existing_clusters():
    # spark.serializer is create-time-only; the migration pre-flight must not advise
    # adding it to existing clusters (static conf; flipping a live cluster is risky).
    src = open(os.path.join(_ROOT, "scripts", "job_migrate_from_workflow.py")).read()
    assert '_CREATE_ONLY_KEYS = {"spark.serializer"}' in src
    assert "if k not in _CREATE_ONLY_KEYS" in src

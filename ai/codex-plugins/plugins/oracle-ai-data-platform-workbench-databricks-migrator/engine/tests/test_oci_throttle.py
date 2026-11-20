"""Tests for aidp_compat.oci_throttle."""
from __future__ import annotations

import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Import the submodule directly to avoid triggering aidp_compat/__init__.py
# (which imports notebook -> nbformat, an optional runtime dep not in test env).
sys.path.insert(0, os.path.join(PROJECT_DIR, "aidp_compat"))

from oci_throttle import (  # type: ignore[import-not-found]
    apply_object_storage_hardening,
    diff_against_defaults,
    get_profile,
    tune_for_parallel_migration,
)


class _FakeSparkConf:
    def __init__(self, fail_keys=None):
        self.values = {}
        self._fail = set(fail_keys or [])

    def set(self, key, value):
        if key in self._fail:
            raise RuntimeError("simulated SET failure")
        self.values[key] = value

    def get(self, key):
        return self.values[key]


class _FakeSparkSession:
    def __init__(self, fail_keys=None):
        self.conf = _FakeSparkConf(fail_keys=fail_keys)


# ----------------------------- get_profile ----------------------------------

def test_get_profile_returns_known_profiles():
    for name in ("conservative", "balanced", "aggressive"):
        cfg = get_profile(name)
        assert "spark.hadoop.fs.oci.client.circuitBreaker.enabled" in cfg
        assert cfg["spark.hadoop.fs.oci.client.circuitBreaker.enabled"] == "true"
        # Write-tuning knobs should be merged in
        assert "spark.sql.adaptive.enabled" in cfg


def test_get_profile_unknown_raises():
    with pytest.raises(ValueError):
        get_profile("turbo")


def test_aggressive_has_higher_failure_threshold_than_conservative():
    cons = int(get_profile("conservative")["spark.hadoop.fs.oci.client.circuitBreaker.failureRateThreshold"])
    aggr = int(get_profile("aggressive")["spark.hadoop.fs.oci.client.circuitBreaker.failureRateThreshold"])
    assert aggr > cons


def test_aggressive_has_more_retries_than_conservative():
    cons = int(get_profile("conservative")["spark.hadoop.fs.oci.client.retry.maxAttempts"])
    aggr = int(get_profile("aggressive")["spark.hadoop.fs.oci.client.retry.maxAttempts"])
    assert aggr > cons


def test_get_profile_returns_independent_copies():
    a = get_profile("balanced")
    b = get_profile("balanced")
    a["spark.sql.adaptive.enabled"] = "false"
    assert b["spark.sql.adaptive.enabled"] == "true"


# -------------------- apply_object_storage_hardening ------------------------

def test_apply_writes_all_keys_to_spark_conf():
    spark = _FakeSparkSession()
    applied = apply_object_storage_hardening(spark, profile="balanced", verbose=False)
    expected = get_profile("balanced")
    assert applied == expected
    for key, value in expected.items():
        assert spark.conf.values[key] == value


def test_apply_skips_failing_keys_silently():
    fail = "spark.hadoop.fs.oci.client.multipart.numthreads"
    spark = _FakeSparkSession(fail_keys=[fail])
    applied = apply_object_storage_hardening(spark, profile="balanced", verbose=False)
    # Failing key should not appear in returned dict
    assert fail not in applied
    # All other keys should have been applied
    for k in get_profile("balanced"):
        if k != fail:
            assert k in applied


def test_apply_overrides_take_precedence():
    spark = _FakeSparkSession()
    applied = apply_object_storage_hardening(
        spark,
        profile="balanced",
        overrides={"spark.hadoop.fs.oci.client.retry.maxAttempts": "99"},
        verbose=False,
    )
    assert applied["spark.hadoop.fs.oci.client.retry.maxAttempts"] == "99"
    assert spark.conf.values["spark.hadoop.fs.oci.client.retry.maxAttempts"] == "99"


def test_apply_unknown_profile_raises():
    spark = _FakeSparkSession()
    with pytest.raises(ValueError):
        apply_object_storage_hardening(spark, profile="zomg", verbose=False)


# --------------------- tune_for_parallel_migration --------------------------

@pytest.mark.parametrize(
    "concurrent, expected",
    [
        (1, "conservative"),
        (8, "conservative"),
        (9, "balanced"),
        (200, "balanced"),
        (201, "aggressive"),
        (2500, "aggressive"),
    ],
)
def test_tune_picks_correct_profile(concurrent, expected):
    spark = _FakeSparkSession()
    applied = tune_for_parallel_migration(spark, concurrent_jobs=concurrent, verbose=False)
    assert applied == get_profile(expected)


def test_tune_rejects_zero_or_negative():
    spark = _FakeSparkSession()
    with pytest.raises(ValueError):
        tune_for_parallel_migration(spark, concurrent_jobs=0, verbose=False)
    with pytest.raises(ValueError):
        tune_for_parallel_migration(spark, concurrent_jobs=-5, verbose=False)


# -------------------------- diff_against_defaults ---------------------------

def test_diff_against_defaults_clean_after_apply():
    spark = _FakeSparkSession()
    apply_object_storage_hardening(spark, profile="balanced", verbose=False)
    assert diff_against_defaults(spark, profile="balanced") == {}


def test_diff_against_defaults_flags_missing():
    spark = _FakeSparkSession()
    diff = diff_against_defaults(spark, profile="balanced")
    # Nothing applied yet -- every key should diff
    expected = get_profile("balanced")
    assert set(diff.keys()) == set(expected.keys())

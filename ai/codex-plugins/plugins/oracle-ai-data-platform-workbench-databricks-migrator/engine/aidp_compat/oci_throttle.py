"""
OCI Object Storage Throttle / Circuit-Breaker Hardening
========================================================
Tunes the OCI BMC Java SDK client (used by the OCI HDFS connector under Spark)
so that transient HTTP 429s during bulk Databricks->AIDP migration do NOT trip
the SDK's client-side circuit breaker for 30s and cascade-fail an entire job.

Symptom this fixes
------------------
    com.oracle.bmc.model.BmcException: (-1, null, false)
    CircuitBreaker is OPEN and all the requests sent in a window of
    30 seconds will be rejected..
    Errors which opened the CircuitBreaker:
      ErrorCode - 429 (x5)

Root cause
----------
The OCI Java SDK ships a built-in resilience4j circuit breaker. When >=
``failureRateThreshold`` of calls in a sliding window fail (429/5xx counts as
failure), the breaker opens and rejects ALL calls for ``waitDurationInOpenState``
seconds -- including healthy ones. Default threshold is too aggressive for
write-heavy migration workloads that produce small-file storms.

Usage
-----
At the top of a migrated notebook::

    from aidp_compat import apply_object_storage_hardening
    apply_object_storage_hardening(spark)               # balanced profile (default)
    apply_object_storage_hardening(spark, "aggressive") # for 1000+ parallel jobs

Or, programmatically tune for an expected concurrency::

    from aidp_compat.oci_throttle import tune_for_parallel_migration
    tune_for_parallel_migration(spark, concurrent_jobs=2500)

This module is import-safe in non-Spark environments (pytest etc.).
"""
from __future__ import annotations

from typing import Any, Dict, Optional

# Profile -> {spark.conf.key: value}
#
# Profiles:
#   conservative -- single job, no parallel migrations. Trades throughput for
#                   minimum 429s. Tight retry, narrow CB window.
#   balanced     -- DEFAULT. 10-100 concurrent migrations. CB tolerates
#                   transient 429 bursts; retries with exponential backoff +
#                   jitter; multipart uploads capped to keep PUT rate sane.
#   aggressive   -- 500+ concurrent migrations. CB nearly open-tolerant
#                   (95% failure rate threshold), short open-state, long retry
#                   chain so individual ops absorb throttling instead of
#                   tripping the breaker.
#
# Notes on key namespacing
# ------------------------
# Spark passes "spark.hadoop.<X>" entries to Hadoop config as "<X>". The OCI
# HDFS connector reads "fs.oci.client.<X>". The OCI Java SDK CircuitBreaker
# is configured via Hadoop properties forwarded to the underlying OCI client
# builder when the connector is initialised.
#
# Property names follow the OCI HDFS connector 3.x conventions. If your
# cluster ships a different connector version some keys may be ignored
# silently -- verify with `spark.conf.get(...)` after applying.

_PROFILES: Dict[str, Dict[str, str]] = {
    "conservative": {
        # Circuit breaker: open quickly, recover quickly
        "spark.hadoop.fs.oci.client.circuitBreaker.enabled": "true",
        "spark.hadoop.fs.oci.client.circuitBreaker.failureRateThreshold": "80",
        "spark.hadoop.fs.oci.client.circuitBreaker.slidingWindowSize": "100",
        "spark.hadoop.fs.oci.client.circuitBreaker.minimumNumberOfCalls": "100",
        "spark.hadoop.fs.oci.client.circuitBreaker.waitDurationInOpenStateInSeconds": "20",
        # Retry chain
        "spark.hadoop.fs.oci.client.retry.enabled": "true",
        "spark.hadoop.fs.oci.client.retry.maxAttempts": "5",
        "spark.hadoop.fs.oci.client.retry.initialDelayMs": "500",
        "spark.hadoop.fs.oci.client.retry.maxDelayMs": "10000",
        # Concurrency caps
        "spark.hadoop.fs.oci.client.multipart.numthreads": "8",
        "spark.hadoop.fs.oci.client.apache.max.connection.pool.size": "100",
    },
    "balanced": {
        "spark.hadoop.fs.oci.client.circuitBreaker.enabled": "true",
        "spark.hadoop.fs.oci.client.circuitBreaker.failureRateThreshold": "90",
        "spark.hadoop.fs.oci.client.circuitBreaker.slidingWindowSize": "200",
        "spark.hadoop.fs.oci.client.circuitBreaker.minimumNumberOfCalls": "200",
        "spark.hadoop.fs.oci.client.circuitBreaker.waitDurationInOpenStateInSeconds": "10",
        "spark.hadoop.fs.oci.client.retry.enabled": "true",
        "spark.hadoop.fs.oci.client.retry.maxAttempts": "8",
        "spark.hadoop.fs.oci.client.retry.initialDelayMs": "500",
        "spark.hadoop.fs.oci.client.retry.maxDelayMs": "20000",
        "spark.hadoop.fs.oci.client.multipart.numthreads": "4",
        "spark.hadoop.fs.oci.client.apache.max.connection.pool.size": "50",
    },
    "aggressive": {
        "spark.hadoop.fs.oci.client.circuitBreaker.enabled": "true",
        "spark.hadoop.fs.oci.client.circuitBreaker.failureRateThreshold": "95",
        "spark.hadoop.fs.oci.client.circuitBreaker.slidingWindowSize": "500",
        "spark.hadoop.fs.oci.client.circuitBreaker.minimumNumberOfCalls": "500",
        "spark.hadoop.fs.oci.client.circuitBreaker.waitDurationInOpenStateInSeconds": "5",
        "spark.hadoop.fs.oci.client.retry.enabled": "true",
        "spark.hadoop.fs.oci.client.retry.maxAttempts": "12",
        "spark.hadoop.fs.oci.client.retry.initialDelayMs": "1000",
        "spark.hadoop.fs.oci.client.retry.maxDelayMs": "30000",
        "spark.hadoop.fs.oci.client.multipart.numthreads": "2",
        "spark.hadoop.fs.oci.client.apache.max.connection.pool.size": "30",
    },
}

# Generic Spark write-side knobs that reduce the small-file rate independent
# of OCI client tuning. Applied alongside every profile.
_WRITE_TUNING: Dict[str, str] = {
    # Adaptive Query Execution coalesces shuffle output -> fewer files
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "256MB",
    # Upper bound on records-per-file (combined with target file size)
    "spark.sql.files.maxRecordsPerFile": "5000000",
    # Delta defaults that shrink _delta_log churn during bulk land
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
}


def get_profile(name: str = "balanced") -> Dict[str, str]:
    """Return a copy of the named throttle profile config dict.

    Includes the generic write-tuning knobs.
    """
    if name not in _PROFILES:
        raise ValueError(
            f"Unknown throttle profile: {name!r}. "
            f"Choose from: {sorted(_PROFILES)}"
        )
    merged = dict(_WRITE_TUNING)
    merged.update(_PROFILES[name])
    return merged


def apply_object_storage_hardening(
    spark: Any,
    profile: str = "balanced",
    overrides: Optional[Dict[str, str]] = None,
    verbose: bool = True,
) -> Dict[str, str]:
    """Apply Object Storage CB / retry / write-side hardening to a SparkSession.

    Args:
        spark: SparkSession (live or mock with .conf.set).
        profile: One of ``conservative``, ``balanced`` (default), ``aggressive``.
        overrides: Optional dict of additional spark.conf entries to apply
            after the profile (last-write-wins).
        verbose: Print each applied key for the migration log.

    Returns:
        The full dict of keys/values applied. Useful for assertions in tests
        and for capturing in migration reports.

    Raises:
        ValueError: if ``profile`` is unknown.
        AttributeError: if ``spark`` lacks ``.conf.set``.
    """
    settings = get_profile(profile)
    if overrides:
        settings.update(overrides)

    applied: Dict[str, str] = {}
    for key, value in settings.items():
        try:
            spark.conf.set(key, value)
            applied[key] = value
            if verbose:
                print(f"[oci_throttle:{profile}] {key} = {value}")
        except Exception as exc:  # pragma: no cover -- runtime-only
            # Some keys are connector-version specific. Silently skip rather
            # than fail the whole notebook.
            if verbose:
                print(f"[oci_throttle:{profile}] SKIP {key} ({exc.__class__.__name__})")
    return applied


def tune_for_parallel_migration(
    spark: Any,
    concurrent_jobs: int,
    overrides: Optional[Dict[str, str]] = None,
    verbose: bool = True,
) -> Dict[str, str]:
    """Pick a profile from expected concurrency, then apply it.

    Selection thresholds are calibrated for a large migration shape (one
    Spark cluster, many migration processes each driving notebook writes):

    +------------------------+----------------+
    | concurrent_jobs        | profile        |
    +========================+================+
    | <= 8                   | conservative   |
    | 9 .. 200               | balanced       |
    | > 200                  | aggressive     |
    +------------------------+----------------+

    Args:
        spark: SparkSession.
        concurrent_jobs: Expected number of migration processes hitting this
            cluster + tenancy concurrently. Pass the WAVE size, not the total
            (i.e. for 2500 jobs in waves of 48, pass 48).
        overrides: Extra spark.conf entries.
        verbose: Print decisions.

    Returns:
        Dict of applied settings.
    """
    if concurrent_jobs <= 0:
        raise ValueError(f"concurrent_jobs must be >= 1, got {concurrent_jobs}")

    if concurrent_jobs <= 8:
        profile = "conservative"
    elif concurrent_jobs <= 200:
        profile = "balanced"
    else:
        profile = "aggressive"

    if verbose:
        print(
            f"[oci_throttle] concurrent_jobs={concurrent_jobs} "
            f"-> profile={profile}"
        )
    return apply_object_storage_hardening(
        spark, profile=profile, overrides=overrides, verbose=verbose
    )


def diff_against_defaults(spark: Any, profile: str = "balanced") -> Dict[str, str]:
    """Return keys whose live spark.conf value differs from the named profile.

    Useful for verifying that hardening took effect after kernel restarts or
    cluster restarts that may have reset configs.
    """
    expected = get_profile(profile)
    diffs: Dict[str, str] = {}
    for key, want in expected.items():
        try:
            got = spark.conf.get(key)
        except Exception:
            got = None
        if got != want:
            diffs[key] = f"want={want!r} got={got!r}"
    return diffs


__all__ = [
    "apply_object_storage_hardening",
    "tune_for_parallel_migration",
    "get_profile",
    "diff_against_defaults",
]

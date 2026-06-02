---
shipped: 2026-05-10
---

# Shipped: details_json Round-Trip Parity Across Storage Backends

## Summary

Adds `tests/test_storage/test_details_json_roundtrip.py` — a single
file pinning a kitchen-sink `details_json` payload through
`write_results → read_health_data` (the canonical cross-backend
read path) on all four storage backends. SQLite always runs;
ExternalCatalog and Delta run when PySpark / delta-spark are
importable; JDBC has a Spark-side serialization parity test (always
on with PySpark) plus an env-gated real-driver round-trip
(`QUALIFIRE_JDBC_TEST_URL`). The kitchen-sink fixture is grep-audited
against every nested key qualifire validators currently emit
(`pattern_check`, `isolation_forest`, `forecast`, `historical`,
`slo`, `validation/base`, `core/deactivate`, `core/engine`,
`wap/pattern`).

The previously-SQLite-only
`test_value_drift_explainer_roundtrips_through_details_json` in
`tests/test_storage/test_sqlite.py` is removed in the same PR
(superseded; the parity suite covers a strict superset via the
canonical read path).

## Key Changes

- **New test file** — `tests/test_storage/test_details_json_roundtrip.py`
  (~360 LOC). Layout:
  - `_KITCHEN_SINK` covers ~35 keys spanning every emitted block.
  - `_assert_kitchen_sink_roundtrip(decoded, *, where: str)` —
    full-dict equality + per-block sanity spot-checks.
  - `_row(details)` builds a row with `datetime.now(UTC)` —
    no time-bomb against `read_health_data`'s `now() - days`
    window. Every column is non-None to avoid the orthogonal
    `[CANNOT_DETERMINE_TYPE]` failure mode in JDBCStorage's
    schema-inference write path.
  - `test_kitchen_sink_size_under_cap` — defensive 32 KB growth
    alarm.
  - `TestSQLiteRoundTrip` (always runs).
  - `TestExternalCatalogRoundTrip` (importorskip pyspark
    fixture-scoped).
  - `TestDeltaRoundTrip` (importorskip pyspark + delta;
    fixture stops the active Spark session before configuring
    delta extensions because PySpark sessions are JVM-process-
    global and `getOrCreate()` silently drops new configs).
  - `TestJDBCSparkSideSerialization` — class-level
    `monkeypatch.setattr(DataFrameWriter, "jdbc", lambda *a, **kw: None)`
    + `spark.createDataFrame` capture. Verifies
    `cleaned[col] = json.dumps(v)` and the Row → DataFrame STRING
    enforcement layer.
  - `TestJDBCRealDriverRoundTrip` — env-gated; skips cleanly when
    `QUALIFIRE_JDBC_TEST_URL` is unset.

- **`tests/test_storage/test_sqlite.py`** — removed the now-
  superseded SQLite-only test plus 2 unused imports.

- **`docs/CHANGELOG.md`** — Enhancement entry under Unreleased.

- **`docs/features/details-json-backend-roundtrip-parity/plan.md`** —
  the v4-frozen plan with grep-audited validator key table and
  iteration log.

## Files Changed

3 files; +391 / −62 lines.

## Plan PR

[#20](https://github.com/amitranjan-oracle/qualifire/pull/20).

## Review Cycles

Plan: 2 adversarial + 2 codex rounds (codex round 2 PASS after
audit-table backfill, fixture-scoped importorskip, per-test JDBC
table). Implementation: 2 adversarial + 2 codex rounds (codex round
2 surfaced 2 MEDIUM — `_KITCHEN_SINK` missing `auc`/`auc_std`/
`n_features`/`anomaly_count`/`total_current`/`anomaly_ratio`/
`raw_details`, and `value_drift_explainer_mapping_errors` should be
INT not list — both fixed; codex round 3 PASS).

## Local Test Results

- 1469 passed, 2 skipped (Delta — no `delta` package locally;
  JDBC real-driver — no env set).
- All 4 in-scope tests in the new file pass; 2 skip cleanly.

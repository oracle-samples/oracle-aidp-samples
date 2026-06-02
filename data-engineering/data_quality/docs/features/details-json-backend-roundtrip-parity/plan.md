# Plan: details_json Round-Trip Parity Across All Storage Backends

## Goal

Pin a single kitchen-sink `details_json` payload through `write_results
→ read_health_data` (the canonical cross-backend read path that
projects `details_json`) on **all four** storage backends —
`SQLiteStorage`, `DeltaStorage`, `ExternalCatalogStorage`,
`JDBCStorage` — so a backend regression that mangles, truncates, or
drops the column surfaces in CI / local pre-merge runs rather than in
production.

## Locked Decisions (from planning sweep)

1. **Option C** — both mock-shape guards (cheap, always-on) AND
   real-driver round-trip (gated by Spark/JDBC infra availability).
2. **New file** — `tests/test_storage/test_details_json_roundtrip.py`
   (do not graft onto existing per-backend test files; the value is
   reading the four backends side-by-side).
3. **Kitchen-sink fixture** — single payload covering every nested
   detail block currently emitted by qualifire validators (audited
   from grep at plan time; see Audit table below).
4. **No real ADW** — local SparkSession + Hive default catalog
   covers the bug class for ExternalCatalogStorage. JDBC real
   round-trip is env-gated on `QUALIFIRE_JDBC_TEST_URL` (documented
   contract: SQLite-via-JDBC for local dev, not Oracle).

## Audit of Currently-Emitted `details` Keys

Captured at plan time via `grep -rn 'details\[\|details=' qualifire/`.
The kitchen-sink fixture includes one row per cell unless marked.

| Source | Keys |
|--------|------|
| `validation/pattern_check.py` | `cold_start`, `top_contributing_features`, `value_drift_explainer`, `value_drift_explainer_truncated`, `value_drift_explainer_mapping_errors` (INT count, not list), `value_drift_explainer_error`, `explanation_error`, `new_columns`, `dropped_columns`, `inconsistent_past_columns`, `n_current`, `n_past`, `n_features`, `cv_folds`, `auc`, `auc_std`, `auc_folds` |
| `validation/isolation_forest.py` | `cold_start`, `top_contributing_features`, `value_drift_explainer`, `value_drift_explainer_truncated`, `value_drift_explainer_mapping_errors`, `value_drift_explainer_error`, `anomaly_count`, `total_current`, `anomaly_ratio` |
| `validation/forecast.py` | `cold_start`, `history_count`, `changepoint_prior_scale`, `seasonality_mode` |
| `validation/historical.py` | `cold_start`, `past_values`, `mean_past`, `stddev`, `deviation_pct`, `deviation_abs`, `z_score`, `rate_of_change_pct`, `rate_of_change_abs` |
| `validation/slo.py` | `recency_timestamp`, `delta_seconds`, `delta_iso` |
| `validation/base.py` | `missing_partition_anchor`, `reason` |
| `core/deactivate.py` | `deactivated_at`, `deactivated_by` |
| `core/engine.py` | `qualifire_internal_failure`, `error`, `message`, `collected_via` (= `"backfill"`), `raw_details`, `query` |
| `wap/pattern.py` | `staging_table` |

(Edge cases — unicode, control chars, HTML breakout, long string —
are tripwires the plan author adds beyond validator emission.)

## Why a Single Test File

The whole point of this feature is *parity* — a reviewer should be
able to open one file, see the same `_KITCHEN_SINK` payload assigned
to a row, and confirm that all four backends report identical
structural equality after round-trip. Splitting across four files
would lose the side-by-side comparability.

## Disposition of Existing SQLite Coverage

`tests/test_storage/test_sqlite.py::test_value_drift_explainer_roundtrips_through_details_json`
(line 391) is **superseded** by the new parity file's
`TestSQLiteRoundTrip`. The new test covers a strict superset of the
old test's payload and uses `read_health_data` (the canonical read
path) instead of raw SQL. The old test will be deleted in the same
PR. This avoids drift between two tests covering the same shape.

## Scope

### IN scope

- New test file `tests/test_storage/test_details_json_roundtrip.py`
  (~250 LOC).
- One module-level `_KITCHEN_SINK` plain `dict` (no `MappingProxyType`
  — see review #2 below; convention is "tests don't mutate it").
- `_assert_kitchen_sink_roundtrip(decoded, *, where: str)` helper.
- Per-backend round-trip test:
  - **SQLite** (always runs).
  - **ExternalCatalogStorage** (`importorskip("pyspark")`).
  - **DeltaStorage** (`importorskip("pyspark")` + `importorskip(
    "delta")`; skip the class if delta-spark configs fail to apply).
  - **JDBCStorage** real-driver round-trip — env-gated; documented
    contract = SQLite-via-JDBC (`sqlite-jdbc` jar on classpath, env
    `QUALIFIRE_JDBC_TEST_URL=jdbc:sqlite:/tmp/qf_test.db` or similar).
  - **JDBCStorage** Spark-side serialization parity (no driver needed;
    captures the DataFrame produced by `createDataFrame` *before* it
    hits the JDBC writer; verifies the `details_json` column survives
    Row → DataFrame → `df.collect()` round-trip).
- `test_kitchen_sink_size_under_cap` defensive sanity check at 32 KB.
- Removal of the now-superseded
  `test_value_drift_explainer_roundtrips_through_details_json` test
  in `tests/test_storage/test_sqlite.py`.

### OUT of scope

- Real ADW connectivity (explicit user decision: skip).
- Driver-specific dialect quirks beyond JSON column round-trip
  (covered by existing per-backend test files).
- A registry of validator-emitted detail keys (a brittle abstraction;
  the plan-time audit table above pins the fixture for now).

## Concrete Implementation Sketches

### Mutability convention (review #2)

The fixture is a plain `dict`. The test docstring says: "Tests must
not mutate `_KITCHEN_SINK`. The `_row()` helper materializes a fresh
copy via `dict(_KITCHEN_SINK)` before write." No `MappingProxyType`
— the freeze gave only outer protection and was a fake guarantee.

### JDBC Spark-side serialization patch target (review #4 + R2#3)

Concrete approach — class-level patch on the writer, mirrors
existing `test_jdbc_bucketing_real.py` style:

```python
from pyspark.sql.readwriter import DataFrameWriter

# Class-level swallow — every DataFrameWriter.jdbc call is a no-op
# for the duration of the test.
monkeypatch.setattr(DataFrameWriter, "jdbc", lambda *a, **kw: None)

# Capture the DataFrame produced inside write_results by wrapping
# spark.createDataFrame.
captured: dict = {}
original_create = storage._spark.createDataFrame
def capturing_create(*args, **kwargs):
    df = original_create(*args, **kwargs)
    captured["rows"] = df.collect()
    return df
monkeypatch.setattr(storage._spark, "createDataFrame", capturing_create)

storage.write_results([_row(_KITCHEN_SINK)])
assert "rows" in captured, "createDataFrame was never called"
serialized = captured["rows"][0].asDict()["details_json"]
assert isinstance(serialized, str)
_assert_kitchen_sink_roundtrip(json.loads(serialized), where="JDBC-spark-side")
```

Why this is the right level: catches a regression in
`cleaned[col] = json.dumps(v)` (write path serialization) and in
the Row → DataFrame schema enforcement layer that would, for
example, drop the column or mis-type it. Driver-side serialization
is not exercised — that's the env-gated real-driver test below.

### JDBC env-gated real-driver test (review #7 + codex R1#3)

Codex correctly flagged that a fixed-DB sqlite-jdbc URL would
accumulate stale rows across runs and pollute `read_health_data`,
which selects from the whole table. Fix: per-test unique table name
with hard-fail setup DROP + best-effort teardown DROP, mirroring
the existing `test_external_catalog_real.py` pattern.

```python
@pytest.fixture
def jdbc_storage(spark):
    JDBC_TEST_URL = os.environ.get("QUALIFIRE_JDBC_TEST_URL")
    if not JDBC_TEST_URL:
        pytest.skip(
            "JDBC real-driver round-trip skipped — set QUALIFIRE_JDBC_TEST_URL "
            "(documented contract: SQLite-via-JDBC for local dev, e.g. "
            "jdbc:sqlite:/tmp/qf_test.db with sqlite-jdbc jar on classpath. "
            "Not intended for Oracle / ADW — those run in separate live tests."
        )
    # Per-test unique table name so concurrent runs / retries don't
    # pollute each other. The Spark JDBC writer auto-creates this
    # table on first write; the JDBC catalog (not Spark's local
    # catalog) owns its lifetime, so we cannot DROP via spark.sql.
    # Stale tables accumulate in dev sqlite-jdbc files over time;
    # `rm /tmp/qf_test.db` between local runs is the documented
    # hygiene step (added to the test docstring).
    table = f"qf_details_rt_{time.monotonic_ns()}"
    storage = JDBCStorage(spark=spark, table=table, jdbc_url=JDBC_TEST_URL)
    storage.initialize()
    yield storage
```

### Float round-trip safety (review #8)

A side note in the test docstring: "JSON int/float values stored
in a STRING/TEXT column round-trip identically because no
intermediate cast happens — `json.dumps(0.506)` → `'0.506'` →
`json.loads('0.506')` → `0.506`. If a backend ever changes
`details_json` to a JSON-typed column (Postgres `JSONB`, Oracle 23
`JSON`), this test would still pass because `json.loads` is the
canonical decoder."

## Test File Structure (final shape)

```python
"""Cross-backend round-trip parity for ``details_json``.

A single ``_KITCHEN_SINK`` dict covers every nested detail block
qualifire validators emit today (audit captured in plan.md). Each
backend's ``write_results → read_health_data`` round-trip must
surface the payload byte-for-byte identical (structural equality on
the decoded dict).

Convention: tests must not mutate ``_KITCHEN_SINK``. The ``_row()``
helper copies it before write.

When a new validator adds a nested ``details_json`` block, extend
``_KITCHEN_SINK`` and ``_assert_kitchen_sink_roundtrip`` together —
the parity check is only meaningful if the fixture stays in lock-
step with the emitted shapes.
"""

# Imports — NO module-level pytest.importorskip("pyspark"). That
# would skip the SQLite test along with the Spark ones. Each
# Spark-touching class scopes its own importorskip inside a
# session-scoped fixture (matches existing test_external_catalog_real.py
# pattern as referenced by codex; that file does keep module-level
# importorskip but does NOT contain a non-Spark test — our file does,
# so we MUST scope per-class).
#
# _KITCHEN_SINK = {...}  # see audit table — every grep'd key
# _assert_kitchen_sink_roundtrip(decoded, *, where: str)
# _row(details: dict) -> dict
#   - run_timestamp = datetime.utcnow().isoformat() at call time
#     (NOT a hardcoded date — read_health_data filters on now() –
#     {days}, hardcoded dates become time-bombs once the year is up).
# test_kitchen_sink_size_under_cap()
# class TestSQLiteRoundTrip                    (no skip-guard)
# class TestExternalCatalogRoundTrip           (fixture: importorskip pyspark)
# class TestDeltaRoundTrip                     (fixture: importorskip pyspark + delta)
# class TestJDBCSparkSideSerialization         (fixture: importorskip pyspark)
# class TestJDBCRealDriverRoundTrip            (fixture: importorskip pyspark + env-gated)
```

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Local Spark fixture flake on CI workers | Mirror `test_external_catalog_real.py` (proven on this repo's CI). |
| Delta extra not on dev machine | importorskip pyspark + delta-spark; CI without delta-spark skips, doesn't fail. |
| Future validator adds a new nested block; fixture goes stale | Audit table in plan.md + test-file docstring; reviewers enforce in PR review. No registry. |
| Fixture grows past 32 KB | `test_kitchen_sink_size_under_cap` fails loudly. |
| JDBC real-driver test silently does nothing | Skip message documents the env contract; without env, the test logs "skipped" — visible in CI output. |
| Multiple SQLite tests (old + new) drift | Old test deleted in same PR (Disposition section). |

## Acceptance Criteria

- AC1: `_KITCHEN_SINK` covers every key in the plan-time audit table.
- AC2: SQLite round-trip passes (always runs).
- AC3: ExternalCatalogStorage round-trip passes when PySpark is
  importable; skips cleanly otherwise.
- AC4: DeltaStorage round-trip passes when PySpark + delta-spark are
  importable; skips cleanly otherwise.
- AC5: JDBC real-driver round-trip passes when
  `QUALIFIRE_JDBC_TEST_URL` is set; skips cleanly otherwise with
  documented env contract.
- AC6: JDBC Spark-side serialization parity test passes whenever
  PySpark is importable.
- AC7: `test_kitchen_sink_size_under_cap` enforces a defensive
  32 KB growth alarm (not a backend constraint — Spark STRING and
  SQLite TEXT are unbounded; Spark's JDBC auto-create on Oracle
  defaults to CLOB. The cap exists to flag fixture bloat early so a
  reviewer catches "fixture grew 50 KB" before a maintenance burden
  accumulates).
- AC8: `_assert_kitchen_sink_roundtrip` makes structural equality
  the primary assertion; spot-checks supplement, not replace, the
  full-dict equality.
- AC9: Old `test_value_drift_explainer_roundtrips_through_details_json`
  in `test_sqlite.py` is removed in the same PR.

## Out-of-Band Reviews

- 2 adversarial plan reviews (self-critique). #1 done.
- 2 codex plan reviews (`codex:codex-rescue` agent) → iterate to PASS.
- Then implement; 2 adversarial impl reviews + 2 codex impl reviews.

## Effort

Small (~250 LOC test file + 1 line deletion in test_sqlite.py).

## Plan Iteration Log

- v1: initial draft.
- v2: addressed adversarial round 1 — fixed kitchen-sink audit
  (real validator keys, not invented), removed `MappingProxyType`
  (fake guarantee), documented existing-test removal, sharpened
  JDBC patch target + env contract, dropped shaky "smallest backend
  cap" framing.
- v3: addressed adversarial round 2 — `run_timestamp` must
  use `datetime.utcnow().isoformat()` at test time (hardcoded date
  was a time-bomb against `read_health_data`'s `now() - days`
  window); added `wap/pattern.py:staging_table` to the audit table;
  rewrote the JDBC Spark-side patch sketch with class-level
  `DataFrameWriter.jdbc` swallow (mirrors existing
  `test_jdbc_bucketing_real.py` style); added rationale for the
  32 KB growth alarm.
- v4: addressed codex plan-review round 1; codex round 2 PASS
  ("three round-1 fixes are materially addressed"). —
  (a) MAJOR: backfilled missing audit-table keys
  (`new_columns`, `dropped_columns`, `inconsistent_past_columns`,
  `n_current`, `n_past`, `cv_folds`, `auc_folds`, `deactivated_at`);
  (b) MEDIUM: clarified that `pyspark` importorskip MUST be
  fixture-scoped, NOT module-scoped (the existing
  `test_external_catalog_real.py` precedent uses module-scope, but
  that file has no SQLite test — ours does);
  (c) MEDIUM: per-test JDBC table name + documented dev-hygiene
  step (`rm /tmp/qf_test.db` between local runs) since the JDBC
  catalog isn't reachable via `spark.sql DROP`.

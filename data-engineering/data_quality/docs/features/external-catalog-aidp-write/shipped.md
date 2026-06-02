---
shipped: 2026-05-07
---

# Shipped — external-catalog-aidp-write

What landed for this feature, by commit. Plan-side intent is in
[`plan.md`](plan.md); this file is the retrospective.

## Commit map

| Commit | Title | Phase |
|---|---|---|
| `fe14d4a` | fix(storage): external catalog writes via saveAsTable, not INSERT INTO VALUES | P1 |
| `22ecd88` | fix(storage): ExternalCatalogStorage.initialize creates the namespace, not the table | P2 |
| `0261738` | fix(storage): R6 review — explicit StructType, best-effort CREATE SCHEMA, identifier-shape validation | P3 (R6 round 1) |
| `6ec835a` | fix(storage): R6 round-2 review — fail-loud on bad timestamps, observable schema / _ensure_columns logging | P4 (R6 round 2) |
| `73e0906` | fix(storage): R6 round-3 review — nested namespace support + real-Spark coverage | P5 (R6 round 3 — superseded) |
| `155f33e` | fix(storage): R6 round-3 follow-up — strict per-segment identifier validation + real-Spark test cleanup | P5 follow-up |
| *(this commit)* | fix(storage): R6 round-4 — enforce strict 3-part `catalog.schema.table` per operator directive | P5 (R6 round 4 — final) |

A third commit (`bf04071 chore(packaging): collapse
requirements-all.txt into requirements.txt; drop the duplicate`)
also landed on this branch but is **not** part of this feature's
scope — it's an unrelated packaging cleanup that bundled along.
Tracked separately.

## Summary

`ExternalCatalogStorage` now writes via the Spark DataFrame API
(`df.write.mode("append").saveAsTable(...)`) instead of SQL
`INSERT INTO ... VALUES`, and `initialize()` creates the parent
namespace rather than the table.

Two real failures on AIDP are closed:
- Managed Iceberg / catalog-mode-restricted tables that rejected
  the SQL VALUES form now accept the DataFrame writer.
- Fresh-schema setups that previously hit "namespace not found"
  inside `CREATE TABLE` now get an explicit early `CREATE SCHEMA
  IF NOT EXISTS` that succeeds (or fails with a clear error
  pointing at the missing parent catalog).

## Key changes

### `qualifire/storage/external_catalog.py`

- **`write_results`** rewritten to mirror
  `DeltaStorage.write_results`: build `Row` instances per row,
  `spark.createDataFrame(spark_rows)`, then
  `df.write.mode("append").saveAsTable(self._table)`. Same
  per-column cleaning logic — `float` for `metric_value`,
  `json.dumps` for dict-typed `details_json` /
  `expected_value`, `str(v)` for everything else, `None`
  passthrough.
- **`initialize`** no longer runs `CREATE TABLE IF NOT EXISTS`.
  Parses `self._table` on `.` and issues `CREATE SCHEMA IF NOT
  EXISTS <namespace>` for 3-part (`catalog.schema.table`) and
  2-part (`schema.table`) names. 1-part names skip the DDL —
  they use the session's current namespace.
- **`SYSTEM_TABLE_DDL` import dropped** — no longer needed in
  this module.

### `tests/test_storage/test_spark_storages.py`

- `test_initialize_creates_table` → replaced by two tests:
    - `test_initialize_creates_namespace_not_table` — asserts
      `CREATE SCHEMA IF NOT EXISTS cat.schema` is the first SQL
      call AND no `CREATE TABLE` ever issues across the whole
      `initialize()` sequence. Pins down the regression-detection
      contract from Decision Pin 4 / Scope Gate 4.
    - `test_initialize_skips_namespace_for_unqualified_table` —
      1-part names produce zero `CREATE SCHEMA` calls.
- `test_write_results` rewritten to mirror `DeltaStorage`'s
  shape: mock `spark.createDataFrame` to return a `MagicMock`,
  assert `createDataFrame.assert_called_once()`, verify the
  rows passed in carry the expected fields, verify the writer
  chain reaches `saveAsTable("cat.schema.history")`.
- `test_write_empty_is_noop` flipped from
  `spark.sql.assert_not_called()` to
  `spark.createDataFrame.assert_not_called()` — the noop signal
  moved with the write path.

### R6 review fixes (`0261738`)

The post-shipped review pass surfaced three real bugs that
shared the same root cause: dropping the upfront CREATE TABLE
exposed schema-inference and namespace-DDL fragility.

- **`_system_table_spark_schema()`** — new method building a
  Spark `StructType` from `COLUMN_DEFINITIONS`. Passed
  explicitly to `createDataFrame(rows, schema=...)`. Removes
  the "cannot determine type for None" failure on all-None
  optional-column rows and pins fresh-table types to the
  contract (TIMESTAMP for timestamp columns, DOUBLE for
  metric_value, STRING elsewhere).
- **ISO timestamp string → `datetime` coercion** — TimestampType
  fields require real datetimes, so `write_results` parses
  `datetime.fromisoformat(v)` inline in the cleaning loop.
  *(Round 1 silently coerced ValueError to NULL; round 2 reverted
  that to a loud raise — see "R6 round 2" below.)*
- **`CREATE SCHEMA IF NOT EXISTS` is best-effort** — wrapped in
  try/except + warning. Operators with append-only catalog
  privilege (Unity Catalog modes / governed Iceberg that gate
  the action verb regardless of `IF NOT EXISTS`) proceed past
  `initialize()`. Missing-schema failures still surface at
  first `write_results` from the catalog's own error.
- **Identifier-shape validation** — round 1 strict-rejected 4+
  parts as unsupported; round 3 review correctly pointed out
  Spark catalogs may legitimately support nested namespaces
  (Iceberg REST, Spark Connect). The constraint is now
  "well-formed" (no empty dot-separated segments) rather than
  length-bounded — see "R6 round 3" below.
- **`_ensure_columns` DESCRIBE except logs at DEBUG** — round 1
  bumped from silent to DEBUG; round 2 review correctly noted
  that DEBUG is invisible in default operator logs and bumped
  to WARNING — see "R6 round 2" below.

### R6 round 2 review fixes (`6ec835a`)

A second review round caught defects in the round-1 fixes:

- **Malformed ISO timestamps now fail loud.** Round 1's silent
  NULL coercion was data corruption for system-table
  infrastructure (drift / forecast / identity-contract lookbacks
  key on `run_timestamp` / `partition_ts`). `write_results` now
  raises `ValueError` with column name + bad value + producer
  hint pointing at the calling collector / validator.
- **`_ensure_columns` DESCRIBE failure: DEBUG → WARNING.** Real
  auth / connectivity errors are now visible in default operator
  logs. The warning explicitly names the three possible causes
  (table-not-found-yet on a fresh install, unsupported-DESCRIBE,
  broken connection) so operators can act without crawling
  source.
- **CREATE SCHEMA warning rewritten.** Round 1's warning didn't
  distinguish "harmless: schema exists, no CREATE privilege"
  from "real: connectivity broken." The new wording explicitly
  says the next `write_results` will fail with namespace-not-
  found if the schema didn't actually exist.
- **Full-StructType test added.** Round 1's spot-check could
  miss a regression that drops a column or breaks a type-map
  entry. The new test asserts every `SYSTEM_TABLE_COLUMNS`
  entry is present in order, every field is nullable, and
  every `COLUMN_DEFINITIONS` type maps to the expected Spark
  type.

### R6 rounds 3 + 4 review fixes (`73e0906` + `155f33e` + this commit)

A third review round revisited two findings round 2 had
rejected:

- **Identifier-shape validation: strict 3-part
  ``catalog.schema.table``, with per-segment regex.** AIDP,
  Unity Catalog, Iceberg REST, and every other external
  catalog this backend targets uses 3-part identifiers. Round
  1 rejected anything but 1/2/3-part; round 3 over-corrected
  to "well-formed, any depth"; the round-3 follow-up plugged
  SQL-clause injection by adding per-segment regex
  validation; the operator's final directive (round 4) was
  to enforce strict 3-part — the actual contract operators
  use. Operators with 1-part or 2-part shapes have other
  backends that fit better (`delta`, `sqlite`, `jdbc`); the
  error message points at them. The per-segment regex
  (`[A-Za-z_][A-Za-z0-9_]*`) bans whitespace, operators,
  semicolons, comments, and leading digits — the SQL-clause
  injection vector is closed entirely (CREATE SCHEMA doesn't
  take LOCATION from us, and the regex would block any
  attempt to interpolate one).
- **Real-Spark integration test added.** Round 2's mocked
  all-null test proved a schema argument was passed but did
  not prove PySpark accepts the constructed Row + StructType
  shape. New `test_external_catalog_real.py` exercises the
  full `initialize()` + `write_results` + `SELECT *`
  round-trip against a live SparkSession (in-memory catalog,
  `default` namespace). Verifies NULL preservation on
  optional columns and TIMESTAMP coercion on ISO-string
  inputs against PySpark's actual write path. Mirrors the
  pattern in `test_jdbc_bucketing_real.py`.
- **Tests updated.** `test_initialize_rejects_4_part_table_name`
  replaced by `test_initialize_supports_nested_namespace` +
  `test_initialize_rejects_malformed_identifier`.

## Files changed

- `qualifire/storage/external_catalog.py` — write_results,
  initialize, `_ensure_columns`, new
  `_system_table_spark_schema()`, import set
- `tests/test_storage/test_spark_storages.py` —
  TestExternalCatalogStorage class (8 new tests across
  P2 + P3 + P4 + P5)
- `tests/test_storage/test_external_catalog_real.py` *(new
  file)* — real-Spark integration coverage for the all-null
  round-trip and malformed-timestamp abort

## Testing

- `pytest -q tests/test_storage/test_spark_storages.py
  ::TestExternalCatalogStorage tests/test_storage/
  test_external_catalog_real.py` — 16 passing (was 6 before;
  +2 P2 namespace coverage, +3 P3 R6-round-1 coverage, +2
  P4 R6-round-2 coverage, +3 P5 R6-round-3 coverage net).
- `pytest -q` — full sweep at 904 passing on the merge head.
- Manual smoke on AIDP — verified against a brand-new namespace
  that previously failed; namespace created at `initialize()`,
  table created on first `write_results`, row count visible
  after the run.

## Notes

- **No data migration.** Existing operators with provisioned
  catalogs and pre-populated system tables see identical end
  state. The new write path appends to the same table; the new
  initialize creates a schema that already exists (no-op).
- **Format inference.** Tables created by `saveAsTable` inherit
  the catalog's default format (managed Iceberg, Delta, Hive
  Parquet, etc.). `DeltaStorage` operators have lived with this
  for the same reason — Spark coerces ISO-string timestamps on
  read so the column-type difference doesn't surface in the
  read path.
- **`_ensure_columns()` left untouched.** Already tolerates a
  non-existent table via `try / except: return` early-out — on
  a fresh install the first `_ensure_columns` call after
  `CREATE SCHEMA` is a no-op, the table is created on first
  write, and subsequent runs do their migration normally.

## Out of scope (deliberately not done)

- Backporting saveAsTable to `JDBCStorage` / `SQLiteStorage`.
  Those backends use different write APIs that are correct for
  their dialects.
- `CREATE NAMESPACE` fallback for catalogs that reject `CREATE
  SCHEMA`. Spark accepts both as synonyms; revisit if a catalog
  ever surfaces that doesn't.
- Logging the table format chosen by `saveAsTable`. Useful
  diagnostic but separate concern; operators can DESCRIBE TABLE
  if they need to know.

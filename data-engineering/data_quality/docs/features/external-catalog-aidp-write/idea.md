---
id: external-catalog-aidp-write
name: External Catalog System-Table Write Path for AIDP
type: Bug Fix
priority: P0
effort: Small
impact: High
created: 2026-05-07
---

# External Catalog System-Table Write Path for AIDP

## Problem Statement

`ExternalCatalogStorage` is the system-table backend Qualifire
recommends for AIDP — it's wired in as the default when the
operator declares `system_table_backend: "external_catalog"`. Two
real-world failures hit operators on first run:

1. **`INSERT INTO catalog.schema.table (cols) VALUES (...)` fails.**
   Several catalog families behind AIDP — managed Iceberg tables
   served by the Unity Catalog REST shape, the AIDP-managed
   external Hive metastore, certain catalog modes that route
   writes through Spark Connect — reject the SQL `VALUES` form
   for managed tables. The error is opaque ("operation not
   supported on this table") and surfaces deep inside the
   notification / engine path on the first metric write of a run,
   not at config-load. Every operator on these catalog flavors
   has been blocked.

2. **The schema/namespace must pre-exist.** `initialize()` runs
   `CREATE TABLE IF NOT EXISTS catalog.schema.table` upfront. If
   the schema (`catalog.schema`) doesn't exist, the catalog
   raises a "namespace not found" error from the inner CREATE
   TABLE — again opaque, far from the YAML line that named the
   table. Operators provisioning a fresh catalog for Qualifire
   discover this only when the first run blows up.

The two failures compound: the second hides the first (you can't
hit the INSERT error if the table never gets created because the
namespace isn't there).

## Why It Matters

- Blocks first-day onboarding on AIDP for any operator whose
  catalog rejects SQL VALUES inserts. They either drop down to
  `delta_storage` (which works but means an explicit choice they
  didn't intend) or write a one-off `INSERT INTO ... SELECT *
  FROM` workaround using a temp view.
- AIDP is the primary delivery target for Qualifire. The
  `external_catalog` backend that ships there has to work on
  every catalog flavor AIDP exposes, not the subset that supports
  the SQL VALUES form.
- The fix is small and aligns the external_catalog write path
  with how `delta_storage` already writes (DataFrame writer +
  `saveAsTable`) — so the ergonomics converge without a backend
  reorganization.

## Who Benefits

- All operators bringing up Qualifire on AIDP for the first time.
- Operators on Unity Catalog–managed Iceberg tables.
- Anyone provisioning a fresh schema for the system table — they
  get a clear "schema not found" error from CREATE SCHEMA's own
  failure mode rather than buried inside CREATE TABLE.

## Affected Areas

- `qualifire/storage/external_catalog.py` (write_results,
  initialize)
- `tests/test_storage/test_spark_storages.py` (the
  `TestExternalCatalogStorage` class — assertions need to follow
  the new write path)

## Open Questions for Planning

(All resolved in `plan.md` — listing here as the things the
implementation has to commit on, not as open design questions.)

- DataFrame-writer port vs. row-by-row INSERT loop.
- Whether to keep the upfront `CREATE TABLE IF NOT EXISTS` as a
  sanity check, or drop it now that `saveAsTable` handles
  creation on first write.
- How `_ensure_columns()` (the ALTER TABLE migration path)
  interacts with a table that doesn't exist yet.
- (Surfaced during R6 review, after dropping CREATE TABLE)
  whether to rely on Spark schema inference for the first
  ``createDataFrame`` call, or pass an explicit ``StructType``
  derived from ``COLUMN_DEFINITIONS``.
- (Surfaced during R6 review) whether ``CREATE SCHEMA IF NOT
  EXISTS`` is hard-required at ``initialize()`` or best-effort —
  some governed catalog modes gate the action verb regardless of
  ``IF NOT EXISTS``.
- (Iterated across R6 rounds 1, 3, and 4 — final) the
  supported identifier shapes. R1 settled on strict 1/2/3-part;
  R3 over-relaxed to "any well-formed depth" after the
  adversarial reviewer pushed back; R3-follow-up plugged the
  SQL-injection gap with per-segment regex; R4 (operator
  directive) tightened to strict 3-part ``catalog.schema.table``
  — the actual shape every external catalog this backend
  targets uses. Operators with non-3-part shapes have other
  backends (``delta``, ``sqlite``, ``jdbc``) that fit better.
- (Surfaced during R6 round-3 review) whether the round-2 mocked
  test for the all-null optional-column path was sufficient, or
  whether a real-Spark integration test was needed to actually
  prove PySpark accepts the constructed Row + StructType shape
  and that ``saveAsTable`` round-trips through ``SELECT *``.

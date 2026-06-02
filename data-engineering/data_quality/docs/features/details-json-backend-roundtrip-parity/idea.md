---
id: details-json-backend-roundtrip-parity
name: details_json Round-Trip Parity Across All Storage Backends
type: Tech Debt
priority: P2
effort: Small
impact: Medium
created: 2026-05-10
---

# details_json Round-Trip Parity Across All Storage Backends

## Problem Statement

`feature-value-drift-explainer` (PR #15, merged 2026-05-09) added a
new nested `details["value_drift_explainer"]` block to the
validation `details_json` payload — a parallel-list of per-feature
value-shift summaries up to ~16 KB. The end-to-end round-trip
through SQLite is asserted by `test_value_drift_explainer_roundtrips_through_details_json`,
but the equivalent parity checks for the other three backends —
`DeltaStorage`, `JDBCStorage`, `ExternalCatalogStorage` — are
**not** in the test suite.

Today's existing storage tests cover the JSON column at the
serializer level (`json.dumps(details)` round-trips through a
mock connection), but they never persist a populated nested
validator-details payload and read it back. A backend that
truncates strings, rejects nested `list[dict]`, or mangles unicode
on the way in/out would silently corrupt the explainer block —
and operators wouldn't notice until they tried to read a
historical alert from the dashboard.

## Why It Matters

- The explainer's value is precisely in surfacing *historical*
  drift, which means the dashboard / programmatic API will be
  reading the persisted JSON back, not just rendering the freshly-
  computed in-memory dict.
- All four backends advertise a `details_json` column, but the
  underlying column types differ (SQLite TEXT, Delta STRING,
  JDBC VARCHAR2 with size limit, external catalog ADW CLOB) and
  each driver path stringifies differently.
- A regression here would land silently — the validator still
  emits the dict, the storage write still appears to succeed,
  and the corrupted payload only surfaces on read.

## Who Benefits

- Operators in JDBC / Delta / external-catalog deployments who
  query the dashboard or programmatic API for historical alerts.
- Future validators that emit nested `details_json` blobs (the
  pattern is now load-bearing).
- The qualifire team — backend regressions surface in CI rather
  than in production.

## Affected Areas

- `tests/test_storage/test_delta.py` — write a populated
  `value_drift_explainer` payload, read it back via the existing
  storage helper, assert structural equality.
- `tests/test_storage/test_jdbc.py` — same.
- `tests/test_storage/test_external_catalog.py` — same. May
  require a real ADW connection (existing tests have a mocked
  fast path); keep the round-trip test under the same skip
  guard.
- (Optional) Pull the round-trip helper into a fixture so each
  backend test can call it without duplicating the payload.

## Notes / Open Questions

- The 16 KB payload cap (enforced by the explainer) keeps the
  string under most backends' default column limits — JDBC
  VARCHAR2 is 32 KB on ADW. Worth pinning the cap relationship
  in the test (assert serialized length stays under
  `_PAYLOAD_MAX_BYTES`).
- For external_catalog (the AIDP-only path), the existing tests
  in `test_external_catalog_real.py` are gated on a live ADW
  connection. The new round-trip test should follow the same
  skip pattern so CI doesn't fail offline.

## Why Deferred

The explainer feature shipped with SQLite-only round-trip
coverage by explicit plan decision (codex round 1 narrowed the
scope from "all four backends" to "SQLite" because the existing
`tests/test_storage/` suite already covers JSON serializer
fidelity at a lower level). This feature closes the gap with
backend-level integration tests that exercise the full payload.

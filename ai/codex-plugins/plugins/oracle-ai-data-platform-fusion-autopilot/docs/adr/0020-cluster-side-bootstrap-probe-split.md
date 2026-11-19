# ADR-0020 — Cluster-side bootstrap probe split (Phase 4.1 / D3)

## Status

Accepted — 2026-06-07. Ships with Phase 4.1 / D3.

## Context

`aidp-fusion-autopilot bootstrap`'s variation phase (Phase 3a) had a
load-bearing architectural assumption: the operator can probe the
tenant's bronze schema from their laptop. The code path:

```
commands/variation_phase.py:_resolve_spark()
  → SparkSession.builder.master("local[1]").getOrCreate()
commands/bronze_probe.py:describe_bronze()
  → spark.sql("DESCRIBE TABLE fusion_catalog.bronze.<dataset>")
```

That assumption is false for any real tenant. Local Spark's
default `spark_catalog` only supports single-part namespaces
(`database.table`); the 3-part form (`catalog.schema.table`)
requires the AIDP cluster's multi-catalog setup. Even if the laptop
configured `spark.sql.catalog.fusion_catalog = …` locally, the
bronze tables physically live in the AIDP cluster's storage —
laptop has no network access to them. Phase 4 live evidence
(`docs/v2-phase-4-live-defects.md` §D3, historical working note not retained in
this repository) caught this on the first fresh-tenant bootstrap.

The variation phase has two responsibilities mixed in one process:

1. **Spark-touching** — probe bronze via `DESCRIBE TABLE`, run the
   pure-Python walker against observed columns.
2. **Filesystem-touching + interactive** — write
   `profiles/<tenant>.yaml`, `evidence/<tenant>/<ISO-ts>.yaml`,
   `profiles/<tenant>.schema-snapshot.yaml`; prompt operator for
   multi-match resolution.

Phase 4 PLAN §9.5.7 #6 already specifies that **only laptop-side
code writes to `profiles/` and `evidence/`** — that invariant is
load-bearing for the SOX-floor audit trail and the
operator-identity gate.

## Decision

Split the two responsibilities along a clean seam:

- **Mechanical work** (probe + walker + bronze-fingerprint) runs
  **on the cluster** via a dispatched notebook. The cluster has
  native 3-part-namespace support, real access to bronze storage,
  and (for AIDP-managed compute) a pre-configured Spark session.
- **Decisions + filesystem writes** stay **on the laptop**. The
  cluster emits a base64-wrapped JSON marker carrying observed
  schema + walker outcomes; the laptop unwraps, runs any multi-match
  prompts (or consumes `--resolutions`), and writes the three
  output files atomically.

The default `--dispatch-mode` flips to `cluster`. The legacy
laptop-Spark path is preserved opt-in via `--dispatch-mode=local`
for unit tests and laptop-POC bundles whose bronze is
laptop-accessible.

A new neutral helper
`dispatch.notebook_dispatch.dispatch_notebook_and_fetch_marker(...)`
wraps the existing `AidpRestClient` primitives into one
orchestrator-free end-to-end call. The bootstrap-specific
orchestration lives in `commands/cluster_bootstrap_probe.py` —
the **only** module that imports both `dispatch/` and
`orchestrator/`. The `dispatch/` package import boundary stays
locked by `tests/unit/dispatch/test_imports.py`.

## Consequences

**Positive:**
- Bootstrap actually works against real tenants — the bug `D3`
  describes is closed.
- §9.5.7 #6 (laptop-only writes) preserved unchanged.
- Multi-match resolution UX preserved unchanged — the laptop's
  interactive prompt machinery runs the same way in both modes
  (cluster mode just sources the walker outcomes from the marker
  instead of running the walkers in-process).
- The marker schema (`schema/cluster_probe_marker.py`) uses
  `Literal[1]` on `markerVersion` so a future cluster emitting
  `markerVersion: 2` fails closed on the laptop — same pattern as
  the diagnostic-artifact contract.
- The new helper is reusable: a follow-up "Phase 4.2 — consolidate
  dispatch primitives" entry will rewire `dispatch/__init__.py`'s
  run dispatcher to use it too, retiring
  `tests/live/dispatch_v2_seed.py::_parse_b64_marker`.

**Negative / costs:**
- Default-mode change is a behaviour change. CI / scripts that ran
  `bootstrap` without `--dispatch-mode` and relied on laptop-Spark
  must opt in to `--dispatch-mode=local` explicitly. CHANGELOG
  documents the migration.
- Cluster dispatch adds ~30s wheel-build + notebook-submission cost
  on every bootstrap. Cached via `dispatch.wheel_builder.build_wheel`
  (content-hash cache); cache hits keep retries cheap.
- The bootstrap entry-point now requires `aiDataPlatformId` /
  `clusterKey` / `clusterName` to be set under
  `aidp.config.yaml::environments.<env>` (or via CLI flags / env
  vars). Missing config emits `AIDPF-2047 (reason=missing_config)`.
- New diagnostic surfaces: `AIDPF-2047` (CLI-level, no artifact),
  `AIDPF-2048` (artifact at `.aidp/diagnostics/<run_id>/AIDPF-2048.json`),
  `AIDPF-2049` (artifact + companion `cluster_stdout.log`). The
  last two are **operator-actionable, NOT consumed by the
  `medallion-author` skill** — the skill's reader recognises them
  by errorCode and refuses-with-handoff instead of misclassifying
  them as malformed.

## Alternatives considered

**A. Keep laptop-Spark, document "operator must configure local
multi-catalog Spark first".** Rejected: the bronze tables
physically live in the cluster's storage, so even a correctly
configured local Spark can't reach them. The "configuration"
required is essentially "have direct JDBC + metastore access to
the cluster's storage" — not a typical operator environment.

**B. Use the run dispatcher's existing path
(`dispatch/__init__.py`).** Rejected: that dispatcher is wired to
`orchestrator.run(...)`, which builds + materialises the full
medallion stack. Bootstrap's probe is a read-only `DESCRIBE TABLE`
walk; reusing the run dispatcher would balloon the bootstrap's
latency and surface every run-dispatch failure mode (state-table
locks, schema-drift gates, etc.) at a phase where they're
nonsensical.

**C. Put dispatch primitives inside the new
`commands/cluster_bootstrap_probe.py`.** Rejected: violates the
`dispatch/` import-boundary contract documented at
`dispatch/__init__.py:8-16` and locked by
`tests/unit/dispatch/test_imports.py`. The boundary exists so a
future MCP / Airflow / queue dispatcher can plug in as a sibling
under `dispatch/` without code surgery; putting bootstrap-specific
primitives there would couple the boundary to a single
caller.

**D. Resolve multi-match cluster-side (the cluster prompts the
operator via Jupyter).** Rejected: dispatched notebooks are async
— the operator can't interact with them. Asymmetric UX between
local and cluster modes (cluster forces `--resolutions`, local
still prompts) would also force operators to maintain two distinct
muscle-memory paths. Marker carries enough payload for the laptop
to make every decision.

## References

- `docs/v2-phase-4-live-defects.md` §D3 — historical root-cause analysis +
  interim workaround; not retained in this repository.
- `docs/features/v2-phase-4.1-cluster-bootstrap-dispatcher/plan.md` —
  historical implementation plan + locked decisions table; not retained in
  this repository.
- PLAN §15 Phase 4 deferred follow-ups — entry for the Phase 4.2
  consolidation that this ADR enables.
- PLAN §25 — `AIDPF-2047` / `AIDPF-2048` / `AIDPF-2049` rows.
- PLAN §9.5.7 #6 — the laptop-only-writes invariant this split
  preserves.
- `scripts/oracle_ai_data_platform_fusion_autopilot/dispatch/notebook_dispatch.py`
  — the neutral helper introduced by this ADR.
- `scripts/oracle_ai_data_platform_fusion_autopilot/commands/cluster_bootstrap_probe.py`
  — the bootstrap-specific orchestration that consumes it.
- `scripts/oracle_ai_data_platform_fusion_autopilot/schema/cluster_probe_marker.py`
  — `ClusterProbeEnvelope` + `ClusterProbeMarker` Pydantic models
  carrying the cluster→laptop payload.

## Addendum — 2026-06-17: source-schema fallback for fresh tenants

This ADR assumed the bronze tables already exist (it probes them via
`DESCRIBE TABLE`, whether laptop-local or cluster-side). On a **truly fresh
tenant** — bronze never extracted — that assumption recreated a different
deadlock: bootstrap needs bronze to probe, but bronze is only produced by a
seed, and seed requires the profile that bootstrap writes. `DESCRIBE TABLE`
on a not-yet-landed table raised `TABLE_OR_VIEW_NOT_FOUND`, surfacing as an
error envelope → `ClusterMarkerError` → `AIDPF-2049`.

Decision: the variation probe no longer requires a *landed* bronze table. It
needs the bronze **schema** (column names + types) — to resolve variation
points (pure column-presence decisions) and compute `bronzeSchemaFingerprint`
— and that schema is obtainable from the **BICC PVO `inferSchema`** metadata
probe (`bronze_extract_adapter.probe_bronze_schemas`) with no table landed.

A single shared helper, `commands/bronze_probe.resolve_observed`, selects per
node between the landed `DESCRIBE` producer and the source `inferSchema`
producer (`describe_bronze_from_source`), keyed by a strict, fail-closed
absence detector (`bronze_table_absent` — only `TABLE_OR_VIEW_NOT_FOUND`
counts as absent; auth/catalog errors re-raise). Both the laptop path
(`variation_phase`) and the cluster cell (`cluster_bootstrap_probe`) call it,
so the probe still runs where this ADR put it (in AIDP for the default
cluster dispatch). The cluster notebook gains a creds cell mirroring the run
dispatcher's `_build_creds_cell` (secret fetched cluster-side from
`biccSecretName`/`biccSecretKey`, never serialized) plus a pre-dispatch
`_check_bicc_credential` preflight.

Crucially, this required **no change to the plan-hash formula** — bronze runs
the `AIDPF-4040` gate on incremental with a profile-inclusive hash, so
altering the formula would have blocked every existing tenant's next
incremental. Fingerprint parity holds because audit columns (`_extract_ts`
etc.) are stripped before fingerprinting, so a source-derived fingerprint
equals the later landed-derived one (asserted by
`tests/unit/test_fingerprint_source_landed_parity.py`). The fresh-tenant
sequence is now simply: `bootstrap` (source probe) → `run --mode seed`
(lands bronze + silver + gold) → `run --mode incremental`.

See `docs/features/bootstrap-source-schema-probe/plan.md` for the full design.

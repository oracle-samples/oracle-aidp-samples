# Content-pack execution

Phase 9 (ADR-0022) deleted the v1 `dim_*.py` / `gold_*.py` / bronze-
wrapper modules and the `legacy-python` execution backend. **The
content-pack runner is the single execution path** — `aidp-fusion-autopilot
run` always dispatches through `sql_runner.execute_node` against a
`ResolvedPack`. There is no `--execution-backend` flag; passing one
fails at Click parse with "No such option".

Bronze, silver, and gold all ship as per-file YAMLs under
`content_packs/<pack-id>/{bronze,silver,gold}/<id>.yaml`. Bronze
nodes carry `implementation.type: bronze_extract` and dispatch
through `orchestrator/builtins/bronze_extract_adapter.py`; silver
and gold ship `implementation.type: sql` with paired SQL templates;
`dim_calendar` is the lone `implementation.type: builtin` (ADR-0011).

## Prerequisites

Every `bundle.yaml` MUST declare a content pack and an active
profile. Bundles without `contentPack:` exit with `AIDPF-1031` at
the CLI; profiles missing or unloadable raise `AIDPF-1030` /
`AIDPF-1033`.

1. **`bundle.yaml` declares a `contentPack:` block.**

   ```yaml
   contentPack:
     name: fusion-finance-starter        # required
     path: ./packs/my-overlay            # optional override; omit for installed pack
     profile: acme-prod                  # required — profile name (NOT a path)
   ```

   Resolution order:

   * `path` absent → installed-pack lookup at
     `<plugin>/content_packs/<name>/`. Missing → `AIDPF-1037`.
   * `path` absolute → used as-is.
   * `path` relative → resolved against `<bundle.yaml>.parent`
     (NOT cwd; survives `cd`).

   In every case the resolved directory MUST contain a `pack.yaml`
   or `AIDPF-1038` raises. `aidp-fusion-autopilot validate` surfaces
   the same codes before `run` is attempted (Phase 9 round-9
   review fix — `validate` no longer silently falls back to the
   legacy catalog when `contentPack` resolution fails).

2. **A tenant profile YAML beside `bundle.yaml`.**

   The CLI resolves the profile at
   `<bundle.yaml.parent>/profiles/<contentPack.profile>.yaml`. Per
   PLAN §9.5.7, the profile lives BESIDE `bundle.yaml`, never
   inside the pack directory. Authored at bootstrap time and
   re-pinned via `bootstrap --refresh` when bronze drifts.

   ```yaml
   schemaVersion: 1
   tenant: acme-prod
   pinnedAt: 2026-06-01T00:00:00+00:00
   bronzeSchemaFingerprint: "sha256:..."
   resolved:
     column: {}      # variation-point picks (empty for packs with none)
     semantic: {}
   profile:
     calendar:
       fiscalStartMonth: 4
   ```

## CLI usage

```bash
# Seed mode (inline — runs in the laptop's Spark session).
aidp-fusion-autopilot run --inline --mode seed \
  --bundle path/to/bundle.yaml

# Incremental run with CLI scope filter.
aidp-fusion-autopilot run --inline --mode incremental \
  --datasets supplier_spend

# Resume a failed run by run_id (Phase 5 P1.5ε-fix5 + Step 9b —
# AIDPF-1032 resolved).
aidp-fusion-autopilot run --inline --mode incremental \
  --resume 2026-06-09T14:22:00Z-7a3f
```

The same commands work without `--inline` — they dispatch the
generated 4-cell orchestrator notebook to AIDP via the REST job
API. Dispatch coords (`aiDataPlatformId` / `clusterKey` /
`clusterName`) come from `aidp.config.yaml`.

### Scope filters

* `--datasets <csv>` — restrict to a subset of node ids. Filter
  validated against the resolved pack: unknown ids → `AIDPF-1034`;
  ids outside `bundle.datasets[] ∪ dimensions.build ∪ gold.marts`
  scope → `AIDPF-1043`.
* `--layers <csv>` — filter declared roots by layer (`bronze` /
  `silver` / `gold`). Transitive D-1 deps remain in the plan
  regardless of layer. Filter that empties the root set →
  `AIDPF-1045`.
* `--strict-scope` — opt out of D-1 implicit-transitive-include.
  Every transitive dep MUST also appear in the effective root set
  (CLI `--datasets` if given, else `bundle.datasets[]` ∪ dimensions
  ∪ marts). Missing → `AIDPF-1042`. Use for debug-style runs
  where exact control over the plan is required.

### D-1 implicit-transitive-include

Declaring a high-level node (e.g. `supplier_spend` gold) AUTO-PULLS
its `dependsOn` chain into the plan: bronze upstream(s), silver
dim(s), and any further transitive deps. The `--dry-run` preview
agrees with the real runtime byte-for-byte (rounds-6/7/8 review
fixes — dispatch dry-run mirrors `resolve_content_pack_plan`'s
effective-roots + closure semantics, presence-aware bundle scope,
and AIDPF-1045 guard).

`--strict-scope` opts out: with `--datasets supplier_spend
--strict-scope` on a bundle declaring the full chain, the run
still raises `AIDPF-1042` because `ap_invoices` / `dim_supplier`
aren't in the CLI dataset set.

## What the runner does, per node

1. **Static schema validation** — Phase 1 loader; trusted from the
   `ResolvedPack`.
2. **Preflight** — verifies declared `requiredColumns` are present
   in the live bronze schema and the merge-strategy watermark
   column exists. Metadata + bronze `DESCRIBE TABLE` only; never
   renders SQL.
3. **Render SQL** — exactly once, via the parameter-marker-bearing
   renderer in `orchestrator/sql_renderer.py`. Profile values flow
   through Spark's `args=` parameter binding, never inline. Bronze
   nodes skip rendering — `bronze_extract_adapter` runs the P1.17
   BICC extract directly.
4. **Compute expected plan-hash** — mixes pack/profile identity +
   `rendered_sql_hash` (or `sha256(callable_id:VERSION)` for
   builtins) + `output_schema_hash` + `profile_hash`.
5. **Plan-hash drift gate** (incremental only) — blocks resume on
   `AIDPF-4040` if the expected hash differs from the last
   successful state row's hash. Re-run `--mode seed` to clear.
6. **Dispatch by strategy** — `replace` (CREATE OR REPLACE TABLE),
   `merge` (NULL-safe MERGE INTO with empty-delta probe), or
   `bronze_extract` (P1.17 BICC extract + bronze MERGE on natural
   key). Reuses the same `RenderedSql` object across stages.
7. **Quality tests** — 4 fully implemented (`not_null`, `unique`,
   `accepted_values`, `row_count_min`); 5 deferred (`row_count_delta`,
   `freshness`, `reconcile_to`, `referential_integrity`, `custom`)
   are reported as `status='deferred'` and do NOT block cursor
   advancement.
8. **Materialized-schema assertion** — fail closed with
   `AIDPF-4070` if the Spark target's actual schema doesn't match
   `node.outputSchema`.
9. **Atomic state commit** — primary + every lookup row written
   as one Delta append. Failure preserves the prior watermark
   (§11.9 invariant).

## REST dispatch (no `--inline`)

`commands/run.py` resolves the pack + reads the profile YAML at
the laptop; `dispatch/notebook_builder.py` embeds the staged pack
files + profile YAML + schema-snapshot YAML (Phase 3d) as
**base64-encoded JSON** in the generated notebook source — no raw
payload leaks into the cell text. The cluster-side notebook
reconstructs `ResolvedPack` + `TenantProfile` via
`materialize_staged_pack` + `load_full_chain` and calls
`orchestrator.run(...)`.

The CLI threads `--strict-scope`, `--resume`, and
`--force-fingerprint-skip` through to the generated
`orchestrator.run(...)` call (Phase 9 round-5 review fix wired
these end-to-end + added regression tests for the CLI → dispatch
→ notebook → cluster path).

## Error codes

The complete operator-facing code table lives in
[aidpf-error-codes.md](aidpf-error-codes.md). This execution guide keeps only
the behavior details for how those codes are reached during content-pack runs.

Reserved exit code: `14` (`EXIT_CODE_SCHEMA_DRIFT`) — `AIDPF-2012`
raised on the active run.

> Removed in Phase 9: `AIDPF-1032` (was "resume not supported under
> content-pack"). Resume is supported on both inline and REST
> dispatch paths since Phase 5.

### Design-time column-contract gate (AIDPF-2045)

`content-pack validate` (and run-start) check that every silver/gold node only
demands columns its upstream nodes actually guarantee in their declared
`outputSchema`. This is offline and source-independent — it needs no live
cluster — and complements the live `AIDPF-4070`/`4071` gates that compare the
contract against live Fusion. A demand absent from (or, for a pass-through
column, type-incompatible with) the upstream contract fails closed with
`AIDPF-2045`; the fix is to extend the upstream `outputSchema` or correct the
consumer's `requiredColumns`.

`$column.*` / `$coa.*` consumer demands resolve against the active tenant
profile, so to check them at validate time run profile-aware:

```bash
aidp-fusion-autopilot content-pack validate <pack> --profile <profile-name-or-path>
```

Without `--profile`, validation is profile-less: literal + watermark demands are
still gated; alias demands are deferred to the live gates. This is the leg that
catches an overlay which narrows/retypes a bronze contract below what a
downstream consumer needs — run it after `use-pack`.

### Declared-inputs gate (AIDPF-2084 / AIDPF-2085)

The companion to AIDPF-2045, run at `content-pack validate` (offline): it parses
each silver/gold node's SQL and asserts every upstream column it reads is
declared in that node's `requiredColumns`. Together they form an honest chain —
SQL reads ⊆ `requiredColumns` ⊆ upstream `outputSchema` — so the live
preflight/drift gates (AIDPF-2042/2071/2072/4071) cover every column the SQL
actually consumes. Authoring rules (see CLAUDE.md "SQL authoring convention"):
alias every upstream source, never `SELECT *` from an upstream (hard
`AIDPF-2084`), qualify every upstream column and declare it. A bare unqualified
upstream column is a warn-only `AIDPF-2085`. Matching is at the author symbol
level (literal / `$column.<key>` / `$coa.<role>`), so the gate works on the
profile-less run-start path too.

### Bronze `requiredColumns` overlay (add / acknowledged relax)

An overlay may adjust a bronze node's `requiredColumns` (the source columns the
extract asserts exist in the live PVO; these feed the per-node preflight and the
AIDPF-4071 source-schema gate — they do **not** project the extract, which always
writes the full raw PVO):

- **Add** (additive, tightens the assertion) — `overrides: { bronze/<id>: {
  requiredColumns: { <src>: [COL] }}}`, or a same-id `bronze/<id>.yaml` file
  (add-only). Adds are validated live by the existing gates: requiring a column
  the PVO lacks fails closed (AIDPF-4071/2042).
- **Relax** (weakens the assertion, so acknowledged) — block-only: `overrides: {
  bronze/<id>: { relaxRequiredColumns: { <src>: [{ column: COL, reason: "…" }] }}}`.
  `reason` is mandatory. Dropping a required column in a same-id file fails closed
  (AIDPF-2062); relaxing a column the base never required fails closed
  (AIDPF-2063). Use relax for a tenant whose PVO legitimately lacks a normally-
  required column — note the column is still written to bronze when present; relax
  only turns off the existence assertion + drift watch for it.

## Renderer tokens worth knowing

* `{{ snapshot_date }}` — emits literal `CURRENT_DATE()` when
  `profile.profile.snapshotDate` is absent / empty; binds as
  `:snapshot_date` parameter when present + ISO-8601. Used by
  `ap_aging.sql` to anchor aging buckets deterministically.
* `{{ column.<name> }}` — substituted with the resolved alias from
  the profile's `resolved.column.<name>`. Unresolved → `AIDPF-5003`.
* `{{ semantic.<name> }}` — same, for semantic variants.
* `{{ run_id_literal }}` — substituted with the run's id (escaped).
  Used to populate `<layer>_run_id` audit columns.

## Bootstrap + drift recovery

`aidp-fusion-autopilot bootstrap` runs the variation-point resolution
phase when `bundle.content_pack` is non-None: probes the tenant's
bronze schema, walks each `columnAliases` / `semanticVariants`
declared in `pack.yaml`, pins resolved values to
`<bundle.yaml.parent>/profiles/<contentPack.profile>.yaml`, and
writes an evidence snapshot to
`<bundle.yaml.parent>/evidence/<tenant>/<ISO-ts>.yaml`.

Algorithm per PLAN §9.5.4:

1. **Identity gate** — resolve operator from `--operator` →
   `AIDP_OPERATOR` → `USER`. Empty / whitespace / unset →
   `AIDPF-1020`.
2. **Pack load + probe** — load the resolved pack (overlay chain
   included); run `DESCRIBE TABLE` once per bronze dataset.
3. **Walk** — for each variation point in priority order:
   * **Exactly one match** → auto-resolve.
   * **Multiple matches** → terminal prompt (or scripted via
     `--resolutions`, or auto-pick first under `--non-interactive`).
   * **Zero matches with `required: true`** →
     `AIDPF-2010__<vp-name>.json` / `AIDPF-2011__<vp-name>.json`
     diagnostic. Bootstrap COLLECTS all failures before exiting.
4. **Persist** — profile YAML + `bronzeSchemaFingerprint` + evidence
   snapshot. Pinned schema snapshot also written to
   `profiles/<contentPack.profile>.schema-snapshot.yaml` (Phase 3d).

Flags:

* `--refresh` — re-walk against the live bronze; resolves drift.
  No-op when fingerprint matches byte-for-byte. Back-fills the
  schema-snapshot file when missing / unparseable / desynced.
* `--operator <string>` — explicit identity override.
* `--non-interactive` — sandbox/CI mode: multi-match auto-picks
  first; `--refresh` refuses changes to pinned values.
* `--resolutions <json-file>` — scripted multi-match resolution.
* `--skip-preonboarding-probes` — skip phase-1 probes; useful for
  `--refresh` after initial onboarding succeeded.

When mechanical resolution fails (`AIDPF-2010` / `AIDPF-2011`),
the operator's Tier-2 recovery path is the
[`medallion-author`](v2-medallion-author-skill.md) Claude Code
plugin skill. The skill drafts an overlay under
`<bundle.yaml.parent>/overlays/<overlay-name>/` extending the
starter pack's candidate list; bootstrap remains the only writer
to `profiles/` and `evidence/` per §9.5.7 #6. Skill-authored
commits record `mechanism: skill_proposed` per §9.5.9.

### Runtime drift gate (Phase 3c)

`aidp-fusion-autopilot run --mode incremental` runs a **bronze-schema
fingerprint preflight gate** inside `_run_content_pack_backend`
AFTER Spark acquisition + `run_id` mint but BEFORE any state-table
write or node execution.

Outcomes (`PreflightOutcome.kind`):

* `match` — fingerprints byte-identical → proceed.
* `drift` — write `AIDPF-2012` diagnostic at
  `<bundle.yaml.parent>/.aidp/diagnostics/<run_id>/AIDPF-2012.json`
  with pinned + observed fingerprints, per-VP delta, and per-
  dataset column-level diff (`addedColumns` / `removedColumns` /
  `typeChangedColumns` from the Phase 3d schema-snapshot). Raise
  `SchemaDriftDetectedError`; CLI maps to exit **14**.
* `skip_seed` — `--mode seed` always skips (seed re-baselines bronze).
* `skip_legacy_profile` — profile has no `bronzeSchemaFingerprint`
  pinned (pre-3a profile). WARN + proceed. Remediation:
  `bootstrap --refresh`.
* `skip_force_flag` — `--force-fingerprint-skip` break-glass
  (hidden; audit row written to `fusion_autopilot_state`).

Closed-loop recovery:

1. Operator sees exit 14 + a stderr hand-off pointing at the
   diagnostic file.
2. Operator runs `aidp-fusion-autopilot bootstrap --refresh`.
3. If `bootstrap --refresh` itself fails with `AIDPF-2010` /
   `AIDPF-2011`, invoke the `medallion-author` skill to draft an
   overlay.
4. Re-run `aidp-fusion-autopilot run`. Match → proceeds.

REST-dispatch path: the cluster-side notebook catches the
exception, emits a discriminated marker (`_kind == "schema_drift"`)
carrying the artifact JSON, then re-raises. The laptop-side
dispatcher parses the marker BEFORE the SUCCESS/FAILED status
check, reconstructs the diagnostic locally, and raises
`SchemaDriftDetectedError` — drift surfaces as exit 14 whether
the operator ran with `--inline` or via REST dispatch.

### Pinned bronze-schema snapshot (Phase 3d)

`bootstrap` writes `<bundle.yaml.parent>/profiles/<contentPack.profile>.schema-snapshot.yaml`
at the same instant it computes `bronzeSchemaFingerprint` — same
`(name, type)` projection per dataset, no second probe.

Runtime preflight reads the snapshot on drift, recomputes the
fingerprint over its `datasets`, cross-checks against both the
snapshot's metadata fingerprint AND the profile's pinned
fingerprint, then diffs against the live observation. On match it
populates `SchemaDriftFailure.datasetDeltas`:

```yaml
schemaDrift:
  datasetDeltas:
    - datasetId: ap_invoices
      addedColumns:
        - {name: ApInvoicesNewColumn, type: string, nullable: true}
      removedColumns:
        - {name: ApInvoicesOldColumn, type: string, nullable: true}
      typeChangedColumns:
        - {name: ApInvoicesAmount, priorType: bigint, currentType: string}
```

Graceful-degrade paths (empty `datasetDeltas` + one-time WARN;
never crashes):

| Condition | Remediation |
|---|---|
| Snapshot file absent (pre-3d profile) | `bootstrap --refresh` repins both atomically. |
| Snapshot unparseable (hand-edit corruption) | Same. |
| Snapshot metadata fingerprint ≠ live OR content recompute disagrees | Same. |
| Snapshot fingerprint ≠ profile fingerprint (desync) | Same. |

**Snapshot path key is `bundle.contentPack.profile`** — NOT the
loaded profile's in-YAML `tenant:` field. Bootstrap writes /
preflight reads / the cluster-side bootstrap cell resolves the
SAME key by re-loading `bundle.yaml`. The loaded
`TenantProfile.tenant` field is treated as opaque tenant metadata,
not a filesystem key.

REST dispatch stages the snapshot alongside the profile YAML via
the same base64-encoded notebook channel. The cluster-side
bootstrap cell materialises the snapshot at the resolved
`profiles/<contentPack.profile>.schema-snapshot.yaml` path before
`orchestrator.run` fires.

## Reference fixtures

* **Starter pack** —
  `scripts/oracle_ai_data_platform_fusion_autopilot/content_packs/fusion-finance-starter/`
  with 11 bronze YAMLs (`erp_suppliers`, `ap_invoices`, …),
  5 SQL silver/gold marts, and `dim_calendar` as the lone builtin.
* **Example bundle + profile** —
  `examples/fusion-finance-starter.yaml` +
  `examples/profiles/finance-default.yaml`. Smoke tests live in
  `tests/unit/test_phase3_starter_bundle_example.py`.
* **Phase 2 minimal pack** —
  `tests/fixtures/content_packs/phase2_test_pack/` +
  `tests/fixtures/projects/phase2_project/`. Mocked-Spark unit
  tests in `tests/unit/test_orchestrator_run_content_pack.py`
  prove the CLI flag reaches `sql_runner.execute_node`.

## Architectural references

* [ADR-0021](adr/0021-pack-as-registry.md) — pack-as-registry.
* ADR-0022 — full v1 deletion + bronze as content-pack.
* PLAN §10 — strategy taxonomy + per-node correctness invariants.
* PLAN §11.9 — atomic-commit invariant.
* PLAN §11.10 — primary/lookup cursor policy.
* PLAN §13.3.2 — evidence-discipline rules for new variation points.

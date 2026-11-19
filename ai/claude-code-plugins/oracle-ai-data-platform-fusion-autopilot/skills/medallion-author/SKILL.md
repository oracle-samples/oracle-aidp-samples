---
name: medallion-author
description: "Draft a content-pack overlay extending the starter pack's variation-point candidate lists when `aidp-fusion-autopilot bootstrap` fails AIDPF-2010 / AIDPF-2011. Reads diagnostic artifacts under `.aidp/diagnostics/<run_id>/`, proposes new candidates from the observed bronze schema, presents a draft overlay for operator approval, and drafts a backend-aware remediation runbook (Option A/B/D/E; Option C deferred to v0.4). Use when the CLI exits 1 with AIDPF-2010 or AIDPF-2011 on a fresh tenant or after a Fusion-release upgrade. NOT for runtime drift detection or for authoring net-new silver/gold nodes; skill-authored SQL templates are forbidden."
---

# medallion-author — Tier-2 overlay-author skill

When `aidp-fusion-autopilot bootstrap` cannot mechanically resolve a
variation point (no candidate the pack declares is present on the
tenant's bronze), it writes a diagnostic artifact and exits non-zero.
This skill is the recovery path: read the artifact, propose new
candidates from the observed bronze schema, present a draft overlay
for operator approval, and draft a remediation runbook. The operator
runs `bootstrap --refresh` with the overlay applied; bootstrap commits the
resolution.

**Architectural authority**: `docs/workflow.md`,
`docs/project_setup.md`, and `docs/aidpf-error-codes.md` are current
guidance. The skill is operator-initiated, drafts overlay candidates only,
and never writes profile resolutions directly.

## When to use

- The CLI exited 1 with `AIDPF-2010` or `AIDPF-2011`.
- A `run --mode seed`/`incremental` left an `AIDPF-4071__<node>.json`
  diagnostic under `.aidp/diagnostics/<run_id>/` — a bronze node
  declares a column the live PVO doesn't expose (see the dedicated
  section below).
- The operator wants to author a pack overlay extending the starter
  pack with new variation-point candidates observed on their tenant.
- Mid-Fusion-upgrade recovery: a column got renamed and the existing
  candidate list no longer matches.

## When NOT to use

- During `run --mode seed` / `run --mode incremental` — the engine has zero
  LLM dependency at runtime. This skill is operator-initiated and runs between
  bootstraps.
- When the diagnostic artifact is missing — the skill refuses to
  invent context (no synthesised proposals from thin air). **Exception: the
  COA-depth mode** (below) is explicitly **operator-input driven** and runs with
  NO diagnostic artifact — its trigger (`AIDPF-2015`, a `content-pack validate`
  failure) writes none.
- When `AIDPF-1020` is present — operator-identity gate is unrelated
  to overlay drafting; fix the identity environment first.
- To author NEW silver/gold nodes (a SQL template the pack doesn't declare).
  The skill only EXTENDS existing variation-point candidate lists.

## AIDPF-2012 read-only context

When a runtime drift artifact (`AIDPF-2012.json`) is present alongside
2010/2011 in the same `<run_id>/` directory, the reader exposes it via
`DiagnosticReadResult.schema_drift_failure`. The skill SURFACES this
to the operator as context but DOES NOT act on it — drift recovery is
`bootstrap --refresh`, not an overlay draft.

The artifact's `schema_drift_failure.schema_drift.dataset_deltas` field is
populated whenever bootstrap pinned a
`profiles/<tenant>.schema-snapshot.yaml` file. Profiles without a snapshot emit
empty `dataset_deltas` and a one-time WARN on the operator's terminal;
remediation is the same `--refresh`. When `dataset_deltas` is
non-empty, each entry carries `addedColumns` / `removedColumns` /
`typeChangedColumns` lists with the operator-facing original casing
preserved. Surface those directly in any human hand-off message
instead of asking the operator to re-probe + diff manually against
the most recent evidence snapshot. The reader contract is unchanged
— the field just becomes useful.

## AIDPF-4071 — bronze source column missing (runtime seed gate)

The pre-ingest source-schema gate (sql_runner Step 3) fails a bronze
node in *seconds* — before the multi-minute extract — when a column the
pack declares is absent from the live PVO, and writes
`.aidp/diagnostics/<run_id>/AIDPF-4071__<node>.json`. The reader exposes
these via `DiagnosticReadResult.source_column_failures`. Each artifact
carries:

- `node` — the bronze node id (e.g. `ap_payments`).
- `datastore` — the full BICC PVO path it extracts from.
- `missingColumns` — declared columns absent from the live PVO.
- `pvoColumns` — **every** column the live PVO exposes, with `name` +
  `type` (this is the candidate set — you do NOT need to re-probe).

### Resolution algorithm (do this for each missing column)

1. **Get the Fusion PVO schema first.** It's already in the artifact's
   `pvoColumns`. (Only re-probe — `aidp-fusion-autopilot catalog probe
   --datastore <ds>` — if the artifact is stale or absent.)
2. **Classify the mismatch, then act:**
   - **Renamed column (the common case)** — the logical column exists
     in `pvoColumns` under a *different physical name* (e.g. declared
     `ApPayHistDistInvoicePaymentId` vs live
     `ApPaymentHistDistsInvoicePaymentId`). Match by suffix / token
     similarity / semantic meaning. Resolve by authoring a
     **`columnAlias` overlay** (the standard 8-step workflow below) that
     maps the logical column to the real physical name. This is the
     `AIDPF-4071` path.
   - **Type-only mismatch** — the column name IS present in the PVO but
     the bronze YAML declares a different `type` than Fusion returns.
     Note: this does **not** trigger `AIDPF-4071` (the pre-gate is
     presence-only); it surfaces post-write as `AIDPF-4070`. The fix is
     different and simpler: **go to the PVO schema, read the actual
     type, and update that column's `type:` in the bronze node YAML to
     the corresponding Fusion/BICC type** (e.g. `long` →
     `"decimal(38,30)"`, `timestamp` → `date`). BICC maps all numerics
     to `decimal(p,s)` and lowercases names — match the live type
     verbatim. This is a direct YAML edit, **not** a columnAlias, and
     **not** a profile write.
   - **Genuinely absent** — the logical column has no counterpart in the
     PVO at all (dropped in this Fusion release). Surface to the
     operator; the pack node may need its `requiredColumns` /
     `outputSchema` trimmed, or the feature isn't available on this
     tenant. Do not invent a mapping.
3. **Never edit `profiles/` or hand-write `resolved.*`** — same rule as
   the bootstrap variation-point path. The columnAlias goes
   in the overlay; bootstrap pins the resolved value on `--refresh`.

## AIDPF-4070 — bronze type mismatch (runtime type gate)

A bronze node's declared `outputSchema` **type** differs from the live PVO
type (e.g. the `decimal(38,30)` → `decimal(18,0)` supplier-ID overflow). The
run persists `AIDPF-4070__<node>.json` (read it via the diagnostic reader's
`type_mismatch_failures`). The sanctioned fix is a **bronze type-overlay** —
non-destructive, the shipped pack stays immutable. Two equivalent shapes:

- **`overrides:` block** (preferred for a small retype) — declare only the
  changed columns:
  ```yaml
  overrides:
    bronze/erp_suppliers:
      outputSchema:
        columns:
          - { name: VENDORID, type: "decimal(18,0)" }   # retype to live type
  ```
- **Same-id bronze file** `overlays/<name>/bronze/erp_suppliers.yaml` — a full
  `NodeYaml` (filename stem **must** equal `id`); may differ from base only in
  `outputSchema` (retain every base column; retype/append) and `quality.tests`
  (extend, never drop).

`draft_type_overlay()` builds the block shape from the diagnostic for operator
approval (it does **not** auto-apply/seed). **Off-limits to both shapes:** grain,
`naturalKey`, `target`, `datastore`/PVO, `refresh` — an identity change is a
**new node id**, not an override. (`requiredColumns` is now adjustable via its own
overlay — see the next section.) Type-overlays are **bronze-only**; a silver/gold
type fix is `overrides: { sql }` or a new mart id.

## Bronze `requiredColumns` overlay — add / acknowledged relax

`requiredColumns` lists the source columns the extract **asserts exist** in the
live PVO (feeding the per-node preflight + the `AIDPF-4071` source-schema gate).
It does **not** project the extract — bronze always writes the full raw PVO; an
add tightens the assertion, a removal turns the assertion + drift-watch off for
that column (the column still lands when present). A tenant may need to **add** a
required column (assert/pull an extra source field) or **relax** one its PVO
legitimately lacks. Both are non-destructive overlays; the shipped pack stays
immutable.

- **Add** (additive — allowed in **both** the `overrides:` block and a same-id
  `bronze/<id>.yaml` file, which is add-only):
  ```yaml
  overrides:
    bronze/erp_suppliers:
      requiredColumns:
        erp_suppliers: [BUSINESSRELATIONSHIP]   # unioned into the base list
  ```
- **Relax / remove** (weakens a gate → **block override only**, with a mandatory
  `reason` as the acknowledgement):
  ```yaml
  overrides:
    bronze/erp_suppliers:
      relaxRequiredColumns:
        erp_suppliers:
          - { column: PARTYID, reason: "tenant pod does not expose PARTYID" }
  ```

Guards (fail closed): a same-id file that **drops** a base required column →
`AIDPF-2062` (use `relaxRequiredColumns` instead); a relax naming a column the
base never required → `AIDPF-2063`; a blank/missing `reason` → schema validation
error. **Bronze-only** (a silver/gold `requiredColumns` override is rejected). The
block and same-id-file mechanisms are mutually exclusive per node. This is the
path `/fusion-drift-doctor` routes a `missing_literal` to when the column is
legitimately absent for the tenant.

## COA-depth mode — operator-input (no diagnostic)

For a tenant whose chart of accounts uses COA role segments beyond the starter
pack's `Segment1–6` (up to `CodeCombinationSegment30`), `content-pack validate`
rejects a binding to e.g. `Segment10` with **AIDPF-2015** (out of contract) — a
validate failure that writes **no** `.aidp/diagnostics` artifact. So COA-depth is
driven by **explicit operator input**, not a diagnostic:

```
/medallion-author coa-depth --tenant <tenant> --segments 7-10 \
    [--role natural_account=CodeCombinationSegment10] [--chart <id>=...]
```

`draft_coa_depth_overlay()` drafts ONE **coordinated** overlay that extends:

1. the three `coa_*` semantic-role **candidate lists** (`inherit` + the deep
   `CodeCombinationSegment<N>`), re-declaring `resolution: semanticRole` + `role:`
   (overlay merge keeps the overlay entry's fields; only `candidates` inherit), and
2. the **`gl_coa` bronze `outputSchema`** (`extendColumns` the same deep segments)
   — so the binding is contract-backed (no AIDPF-2015 / Tier-A AIDPF-2042).

It does **not** touch `gl_coa.requiredColumns` (out of scope — see
`bronze-required-columns-overlay`); it surfaces that dependency instead. The skill
drafts a **profile runbook fragment** for `profile.chartOfAccounts` (operator
authors the meaning; the skill never writes `resolved.column.coa_*` — bootstrap
does). Segments are capped 1..30 (Fusion GL flexfield max).

**Provenance (operator-input):** the overlay carries `trigger: operator_input` +
`operatorInputId: operator-input-<id>` (NOT a faked `diagnosticRunId`) +
`evidence: { trigger, tenant, segments, roles }`. `validate_overlay` requires
exactly one of `diagnosticRunId` XOR `operatorInputId`.

## Required overlay rules

1. **Probe bronze before declaring**. Skill reads the diagnostic
   artifact's `observedBronzeSchema`; never invents columns.
2. **Identify variation points via neighbour-tenant + release-delta
   knowledge**. Skill consults `known-deltas.yaml` for documented
   Fusion-release patterns.
3. **Add declarations to the pack overlay, not the profile**. Skill
   writes `overlays/<name>/pack.yaml`; never `profiles/<tenant>.yaml`
   (that's bootstrap's domain).
4. **Use `{{ column.<name> }}` / `{{ semantic.<name> }}` tokens, never
   hardcoded column names**. The skill ADDS candidates to existing
   `columnAliases` / `semanticVariants`; the SQL templates that
   reference the tokens are unchanged.
5. **Run `bootstrap --refresh` for final commit**. Skill never invokes
   bootstrap itself — operator gates every commit. Bootstrap is the only writer
   to `profiles/` and `evidence/`.
6. **Never hand-write `profile.resolved.*`**. Skill emits an
   overlay; bootstrap pins the resolved values.
7. **Stamp every artifact with `provenance` metadata**. Overlay
   carries `skillId`, `skillVersion`, `modelId`, `generatedAt`,
   `diagnosticRunId`, `proposals`, `incrementalImpact`.

## Forbidden actions

1. **Never hardcode the winning candidate into a SQL template**.
   The skill's drafter rejects any overlay that introduces a node
   definition or override block (`OverlayValidationError`).
2. **Never add candidates without observed-tenant evidence**. The
   reasoner only scores columns present in the diagnostic artifact's
   `observedBronzeSchema`.
3. **Never skip the human-approval step**. The propose phase shows
   each candidate to the operator for explicit Y/N approval.

## The 8-step workflow

When invoked via `/medallion-author [<run_id>]`:

### 1. Discovery (read-only)

- Scan `<workdir>/.aidp/diagnostics/<run_id>/` via
  `medallion_author.reader.read_run`.
- If `run_id` is omitted, auto-discover the latest run.
- Surface the parsed `VariationPointDiagnosticV1` entries to the
  operator: which VPs failed, which candidates were tried, what's
  observed on bronze.

### 2. Refuse-to-proceed gates

- `AIDPF-1020.json` present → refuse: "identity gate must be fixed
  before overlay drafting".
- Unknown `schemaVersion` → refuse for forward compatibility.
- Diagnostics directory missing / empty → refuse.

### 3. Pack load + affected-nodes computation

- Load the resolved pack via
  `orchestrator.content_pack.load_full_chain` (overlay chain
  included if any).
- For each failing VP, compute affected silver/gold node IDs via
  `medallion_author.affected_nodes.compute_affected_nodes`.

### 4. Propose phase (LLM-driven)

- For each failing VP, call
  `medallion_author.reasoner.score_candidates(...)` to get a ranked
  top-3 of observed columns (with confidence labels + KB hints).
- For refresh promotions (when `priorPinned` is set), call
  `classify_incremental_risk(...)` for the risk label.
- Write a one-paragraph reasoning per proposal — surface the KB hint
  rationale when present.

### 5. Operator review

Present each proposal in this shape:

```
Proposed candidate for invoice_currency_code:
  + ApInvoicesXCurrCode  (confidence: high, KB-hint: currency-code-casing-variants)
Rationale: Fusion 25C renamed CurrencyCode → XCurrCode for multi-currency
tenants. The known-deltas KB confirms this pattern. Observed on the
tenant's bronze.ap_invoices schema.
Incremental impact: likely-different-semantics; Option D recommended.
Affected nodes: silver.supplier_spend, silver.ap_aging, gold.supplier_spend, gold.ap_aging.

Approve this proposal? [y/N/edit]:
```

Operator approves / edits / rejects each proposal independently.

### 6. Draft phase (writes)

After approval, call:

- `medallion_author.drafter.draft_overlay(...)` — assembles a
  validated `PackYaml` overlay.
- `medallion_author.drafter.write_overlay(...)` — writes to
  `<workdir>/overlays/<overlay_name>/pack.yaml`.
- `medallion_author.drafter.write_resolutions(...)` — **conditional**.
  Returns `None` for initial-onboarding AutoResolved picks (no
  resolutions file needed; bootstrap walks the extended candidate
  list and AutoResolves trivially). Emits the file for MultiMatch
  or RefreshChange picks.
- `medallion_author.runbook.draft_remediation(...)` — drafts
  `remediation.md` (always) + `remediation.sql` (Option B only).
- `medallion_author.drafter.write_skill_evidence(...)` — drafts
  `skill-evidence.json` (skill's audit trail).

### 7. Hand-off

Print one of TWO templates per the conditional matrix.

#### 7a. Initial-onboarding

```
Overlay drafted: overlays/<overlay-name>/pack.yaml
Remediation:    overlays/<overlay-name>/remediation.md (Option D)

Next steps:
  1. Review the overlay + remediation.md.
  2. Wire the overlay into the pack chain:
       aidp-fusion-autopilot use-pack overlays/<overlay-name> --profile <tenant>
  3. Re-run bootstrap (NO --resolutions flag needed — the
     extended candidate list AutoResolves):
       aidp-fusion-autopilot bootstrap --operator "$USER"
  4. Apply Option D remediation per remediation.md (targets
     affected pack silver/gold node IDs):
       aidp-fusion-autopilot run --mode seed \
         --datasets <silver/gold-node-ids>
  5. Resume scheduled `aidp-fusion-autopilot run --mode incremental`.
```

#### 7b. MultiMatch / refresh-promotion

```
Overlay drafted: overlays/<overlay-name>/pack.yaml
Resolutions:    overlays/<overlay-name>/resolutions.json
Remediation:    overlays/<overlay-name>/remediation.md (Option D)

Next steps:
  1. Review the overlay + resolutions.json + remediation.md.
  2. Wire the overlay into the pack chain:
       aidp-fusion-autopilot use-pack overlays/<overlay-name> --profile <tenant>
  3. Commit the resolution:
       aidp-fusion-autopilot bootstrap --refresh \
         --operator "$USER" \
         --resolutions overlays/<overlay-name>/resolutions.json
  4. Apply Option D remediation per remediation.md:
       aidp-fusion-autopilot run --mode seed \
         --datasets <silver/gold-node-ids>
  5. Resume scheduled `aidp-fusion-autopilot run --mode incremental`.
```

### 8. Provenance trail

Every artifact the skill emits carries:

- `skillId: aidp-fusion-medallion-author`
- `skillVersion: 0.1.0` (bumped on every `known-deltas.yaml` change)
- `modelId: <claude-model-id>` (from runtime)
- `generatedAt: <iso-8601-utc>`
- `diagnosticRunId: <run_id>`
- `proposals[<vp-name>]: {candidateAdded, confidence, reasoning}`
- `incrementalImpact[<vp-name>]`: change kind, prior pinned, new
  candidate, risk label, affected nodes, remediation choice.

Feature #2's bootstrap reads `provenance.skillId` on commit and:

- Records `mechanism: skill_proposed` on resolutions driven by the
  overlay (including AutoResolved on a skill-added candidate).
- Populates `SnapshotProvenance.skill_version` from the overlay.
- Mirrors `provenance.incrementalImpact[vp]` into the snapshot's
  per-resolution `incremental_impact` field.

## Decision: which option to recommend?

| Risk label | Recommendation | Notes |
|---|---|---|
| `likely-rename` | Option A | No silver/gold re-seed needed. |
| `likely-different-semantics` | **Option D** (v0.3 default) | Targeted re-seed of affected nodes via the engine's tested seed path. |
| `unknown` | Option D + operator confirmation | Surface the uncertainty; operator's call. |

Option B is an advanced opt-in when the affected tables are too
large for Option D AND the column substitution is genuinely
surgical (no derived columns / joins reference the VP).

Option C (watermark rewind) is **deferred to v0.4** — requires an
`aidp-fusion-autopilot rewind` verb that knows both legacy and
content-pack state contracts. Round-2 review evidence:
`orchestrator/preflight.py:400` raises `IncrementalCursorMissingError`
on NULL silver/gold cursors; content-pack
`orchestrator/__init__.py:1591-1596` reads `output_watermark`
filtered by `source_role='primary'`. Until the rewind verb ships,
`draft_remediation(option="C", ...)` raises `OptionDeferredError`
with a redirect message.

Option E (full re-seed) is the audit-baseline reset — rare.

## Dependencies

- Reads `<workdir>/.aidp/diagnostics/<run_id>/*.json` (feature #2's
  artifacts). Schema in
  `scripts/.../schema/diagnostic_artifact.py`.
- Writes only to `<workdir>/overlays/<overlay-name>/`. All other
  paths are off-limits — `Write` tool calls outside this subtree
  are rejected by the path-segment validator.
- Imports `medallion_author.*` from
  `scripts/.../medallion_author/` (real Python package, pip-installable).
- Reads `known-deltas.yaml` from the package dir.

## Skill version policy

`SKILL_VERSION = "0.1.0"`. Bumped on every change to:

- `known-deltas.yaml` (new entry → patch; new VP family → minor).
- Schema of the overlay's `provenance` block (breaking → major).
- The 8-step workflow contract (breaking → major).

Skill version is loose-coupled with runtime execution: recorded in evidence
snapshots as audit metadata, NOT a plan-hash input.

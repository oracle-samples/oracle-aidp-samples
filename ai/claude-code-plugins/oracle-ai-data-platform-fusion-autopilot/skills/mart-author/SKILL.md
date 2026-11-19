---
name: mart-author
description: "Author a new medallion node (gold mart, silver dim, or additive column) when the live gold layer AND the content pack both cannot serve a business need. Takes the user's business logic, inspects the Fusion PVO SOURCE schema (not bronze) for available raw fields, and authors the lowest-cost, additive, non-destructive change as content-pack YAML + SQL in an overlay pack — never touching already-materialized (possibly terabyte-scale) bronze/silver. Then validates and hands off to seed. Use when oac-dataset-advisor reports a true GAP, or the user says 'add a metric/dimension my gold layer doesn't have', 'create a new mart for <business logic>', 'I need a column that doesn't exist'. Does NOT seed, query live data, alter existing nodes' grain/keys, or write Python dim modules."
allowed-tools: Read, Write, Edit, Bash, Glob, Grep, mcp__oac-mcp-server__oracle_analytics-search_catalog, mcp__oac-mcp-server__oracle_analytics-describe_data
---

# mart-author — author a new medallion node, additively and cheaply

When the gold layer genuinely can't serve a dashboard, this skill extends the
medallion **as v2 content-pack artifacts** (YAML + SQL), authored like a
careful data engineer: the smallest, additive, lowest-cost change that serves
the need **without disturbing already-materialized delta**.

It is the GAP-handler of the skill family. It does **not** seed, query live
data, create OAC datasets, or write `dim_*.py` modules (v2 forbids the last —
content-pack YAML+SQL only; `tests/architectural/test_no_new_legacy_modules.py`
enforces it).

> Not to be confused with **`medallion-author`**, which resolves *tenant
> variation* (column aliases / semantic variants) into an overlay. This skill
> authors *new analytical content*. Both write to overlay packs.

## When to use
- `oac-dataset-advisor` reports a **true GAP** (neither live gold nor the pack's
  buildable menu can serve the request).
- "Add a metric/dimension my gold layer doesn't have", "create a mart for
  <business logic>", "I need a column that isn't there".

## When NOT to use
- The data exists but isn't materialized → `aidp-fusion-autopilot run --mode seed`
  (or `/aidp-fusion-seed`); use `oac-dataset-advisor` to confirm.
- Resolving column-alias / semantic-variant tenant variation → `medallion-author`.
- A **bronze column-TYPE** bug (a declared `outputSchema` type is wrong vs the
  live PVO, e.g. `decimal(38,30)` → `decimal(18,0)`) → that's a **bronze
  type-overlay** via `medallion-author` (AIDPF-4070), **not** a new mart. The PVO
  source schema you inspect here surfaces the right type; retype in place via the
  overlay rather than authoring a new node.
- Changing a **bronze** node's `requiredColumns` (assert/pull an extra source
  column, or relax a normally-required one a tenant's PVO lacks) → that's a
  **bronze requiredColumns overlay** via `medallion-author` (add via
  `requiredColumns`, acknowledged removal via `relaxRequiredColumns`;
  AIDPF-2062/2063), **not** a new mart.
- Building the OAC dataset/workbook → `oac-dataset-advisor` + `workbook-authoring`.

## Non-negotiable safety rules
1. **Never disturb living delta.** Reading existing bronze/silver is fine;
   rewriting/altering/reprocessing them is forbidden (they may be terabytes).
2. **Additive only.** New nodes, or a new column on an existing node — never a
   change to an existing node's **grain or natural key**.
3. **New bronze extracts are additive** — a new `bronze_extract` node, never an
   edit to an existing one.
4. **Write to a persistent overlay pack beside the bundle**
   (`<bundle.yaml.parent>/overlays/<name>/`) — never `/tmp`, never the shipped
   installed `content_packs/` tree.
5. **Inspect the Fusion PVO source schema, not bronze**, to discover raw fields
   (metadata-only; cheap; authoritative for "what could be extracted").
6. **Classify every new bronze PVO before authoring.** A rung-4
   `bronze_extract` must be classified as transaction/change-feed,
   snapshot/config, or period-windowable snapshot before its YAML is accepted.
7. **Declare your inputs (AIDPF-2084).** In any silver/gold SQL you author:
   alias every upstream source, **never `SELECT *` from an upstream** (project
   columns explicitly), qualify every upstream column with its alias, and list
   every one in the node's `requiredColumns[<source>]` (literal, or
   `$column.<key>` / `$coa.<role>` for a token). If a consumed column isn't in
   the upstream's `outputSchema`, add it there too (keeps AIDPF-2045 green). The
   declared-inputs gate fails closed on an undeclared read or a wildcard.

## Helpers

| File | Role | Invoked via |
|---|---|---|
| `change_planner.py` | Given where each needed field is sourced (existing layer / PVO-only / missing), picks the lowest-cost rung on the change ladder and emits a node spec with audit columns + refresh strategy + currency-in-grain checks pre-stamped. | `Bash`, JSON in/out |
| `../oac-dataset-advisor/catalog_inventory.py` | Live materialized tables (what exists). | reuse |
| `../oac-dataset-advisor/pack_capability.py` | Pack's buildable menu (what nodes already exist). | reuse |

## The change-strategy ladder (cheapest first)

| Rung | Build | When |
|---|---|---|
| **3 — change a mart in place** | guarded same-id full replacement (`replaceNode`) of an existing silver/gold mart | add / remove / rewrite a column or logic from columns already in that node's sources, **identity unchanged** |
| **1 — new gold** | new aggregate/business mart over EXISTING bronze/silver | a new metric/grain from already-materialized data |
| **2 — new silver** | new conformed/typed node over EXISTING bronze | a conformed shape not yet in silver |
| **4 — new bronze + node** | additive `bronze_extract` + downstream node | a raw field isn't extracted yet (only at the PVO source) |

`change_planner.py` chooses the rung. Rung 3 keeps the mart's id (no downstream
`dependsOn` repointing) but ships a complete replacement under an acknowledged
`replaceNode` block — there is no separate additive mechanism, and add / remove /
rewrite all use this one shape.

**Rung-3 mechanism (guarded same-id full replacement).** Ship a complete new
`overlays/<name>/<layer>/<id>.yaml` + `<id>.sql`, plus:

```yaml
overrides:
  <layer>/<id>:
    replaceNode:
      reason: "Why this fork exists; e.g. TICKET-123."
      forkedFrom:
        sqlSha256: "<base SQL + referenced semantic fragments fingerprint>"
        contractSha256: "<base outputSchema/pii + requiredColumns + quality fingerprint>"
        packVersion: "<base pack version>"
```

- Compute/refresh `forkedFrom` with `aidp-fusion-autopilot content-pack refresh-fork
  overlays/<name> --node <layer>/<id>` (don't hand-author the hashes).
- **Identity must equal base** — `layer`, `target`, the `dependsOn` edge set, and
  the `refresh` contract. Changing any → `AIDPF-2065`; author a new mart id instead.
- A bare same-id file with no `replaceNode` → `AIDPF-2001`. SQL marts only (a
  builtin like `dim_calendar` → `AIDPF-2001`).
- If the base mart later changes, validate fails closed (`AIDPF-2064`) until you
  re-reconcile and re-run `refresh-fork`. This is the accepted cost of forking the
  SQL (the base fix does not flow through automatically).

---

## Workflow

### 1 — Frame the gap
Restate the missing metric/dimension/grain (ideally from `oac-dataset-advisor`'s
GAP output) and the business logic. Confirm the intended output grain with the user.

### 2 — Inventory what already exists (read-only)
Run the advisor's helpers to find the closest existing data to build on:
- live materialized tables — `catalog_inventory.py` (what exists);
- existing pack nodes/columns — `pack_capability.py` (what's already declared).
Do not scan bronze data — you only need the column inventory.

### 3 — Probe the Fusion PVO SOURCE schema (metadata-only)
For any field the business logic needs that is NOT already in an existing
table, confirm it exists at source and get its real name/type **from the PVO,
not bronze**:
```bash
aidp-fusion-autopilot catalog probe --pod <url>            # list/reconcile PVOs
aidp-fusion-autopilot catalog probe-pvo <dataset_id> \      # one PVO's schema, metadata-only
  --datastore <DatastorePVO> --bicc-schema <Financial|HCM|SCM> \
  --emit-pack-yaml overlays/<name>/bronze/<id>.yaml   # persistent, beside bundle.yaml
```
`probe-pvo` does a schema-only roundtrip (no row pull) and emits a **draft
bronze YAML** — the additive extract for rung 4.

For any rung-4 `bronze_extract`, classify the source PVO before accepting the
draft YAML. **The skill owns this classification. `change_planner.py` only
validates and carries the evidence; it is not the source of truth.** Use BICC
metadata, PVO semantics, and data-owner/business confirmation rather than
column-name guesses.

> **Verify incremental capability live — do not guess it from PVO class.** The
> table below is a *hypothesis*, not the answer. The "snapshot/config ⇒
> `incrementalCapable: false`" heuristic is wrong often enough to matter:
> `gl_coa` shipped `false` because it "looked like config," yet a live BICC probe
> proved BICC fully honors the lineage delta for it (recent-watermark extract = 0
> rows vs full 69,578) — it is perfectly incrementable. Before you stamp
> `incrementalCapable` (true OR false) on a new bronze node, hand the source PVO
> to **`/incremental-mechanism`**, which live-probes whether BICC honors
> `fusion.initial.extract-date` and returns the verified rung. Stamp the YAML from
> that verdict, not from the class label.

| PVO class | Required authoring behavior |
|---|---|
| **Transaction / change-feed PVO** | Use `incrementalCapable: true` only when live BICC metadata exposes a reliable `isLastUpdateDate` column and the business semantics confirm changes advance through that column. The column may have a PVO-specific name; record the exact metadata-observed watermark column and natural key in YAML. |
| **Snapshot / config PVO** | Use `incrementalCapable: false`. Warn the user that incremental runs will full-pull this bronze source, then MERGE/payload-diff downstream. If the source may be large, stop for explicit user approval. |
| **Period-windowable snapshot** | Treat high-volume period snapshots like `gl_period_balances` specially. Do not silently create a daily full-pull source; require explicit user approval and either attach/document an `extract_window` policy or create a backlog item until runtime supports it. |

If classification is uncertain, stop and ask for evidence from BICC metadata or
the Fusion data owner — or run `/incremental-mechanism` to settle it empirically.
Never mark a snapshot PVO `incrementalCapable: true` just to make the extract
faster; and never mark a node `false` on a hunch — the live probe is cheap and
authoritative.

### 4 — Plan the change (pick the rung)
Build a field-resolution map — for each required field, where is it sourced?
`existing_gold` / `existing_silver` / `existing_bronze` / `pvo_only` / `missing`
— then:
```bash
python3 change_planner.py --input change_request.json
```
For every `pvo_only` field, include the PVO classification before planning:

```json
{
  "name": "invoice_status",
  "source": "pvo_only",
  "pvo": "InvoiceHeaderExtractPVO",
  "sourceColumn": "StatusCode",
  "pvoClassification": "transaction_change_feed",
  "metadataLastUpdateColumns": ["ApInvoicesLastUpdateDate"],
  "watermarkColumn": "ApInvoicesLastUpdateDate",
  "businessSemanticsConfirmed": true
}
```

Supported `pvoClassification` values are `transaction_change_feed`,
`snapshot_config`, and `period_windowable_snapshot`. For a transaction/change
feed PVO, provide the exact `metadataLastUpdateColumns` seen from BICC, the
chosen `watermarkColumn`, and `businessSemanticsConfirmed: true`. The watermark
column does not need to be named `LastUpdateDate`; it must be the metadata-backed
column that really advances on meaningful source changes. For a
period-windowable snapshot, provide an `extractWindowPolicy` or capture explicit
user approval before writing bronze.

Returns `{decision, reason, blastRadius, requiresNewBronze, missingFields,
warnings, touchesLivingDelta, pvoClassifications, nodeSpecs}`. Act on it:
- **`hard_gap`** (a field exists nowhere, not even at the PVO) → stop and tell
  the user it can't be served as specified; name the missing field(s).
- **`requiresNewBronze: true`** → present the PVO classification, expected
  incremental behavior, and full-pull/window risk to the user before writing.
- otherwise → present the chosen rung + blast radius to the user before writing,
  and resolve any `warnings` (e.g. add `currency_code` to an aggregate's grain).

### 5 — Author the artifacts (correct-by-construction)
Write to a **persistent overlay pack beside the bundle**:
**`<bundle.yaml.parent>/overlays/<name>/`** (e.g. `overlays/fusion-finance-ar-ext/`),
with `pack.yaml` declaring `extends: fusion-finance-starter@<version>`. This is
the canonical home — mirrors `medallion-author`'s write boundary, survives
reboots, and is what the customer commits/points the bundle at.
**Never** write to a temp dir (`/tmp` is lost on reboot) and **never** to the
shipped installed `content_packs/` tree. For each `nodeSpec`:
- **`<id>.yaml`** — `implementation.type: sql` (or `bronze_extract`),
  `dependsOn`, the planner's `refresh` strategy **with its documented reason**,
  and `outputSchema.columns` with a **mandatory `pii` classification per column**
  (missing → AIDPF-2030). High-PII columns must not be exposed to dashboards.
  For `bronze_extract`, `incrementalCapable` must match the classification from
  step 3: true only for proven transaction/change-feed PVOs; false for
  snapshot/config PVOs; explicit approval or `extract_window` policy for
  high-volume period-windowable snapshots.
- **`<id>.sql`** — Jinja template enforcing the medallion invariants:
  `COALESCE(...,0)` around every amount arithmetic; **currency in the grain** of
  any amount aggregate; deterministic **`xxhash64(natural_key)`** surrogate keys
  (never `monotonically_increasing_id`); audit columns (`{{ run_id_literal }}` →
  `*_run_id`, `*_built_at`); single financially-correct shape (LEFT JOIN, fact
  preserved) over runtime path-selection; variation-point refs
  `{{ column.<name> }}` / `{{ semantic.<name> }}` where the tenant may differ.
- (rung 4) the `bronze_extract` YAML from step 3's `probe-pvo` (additive).

### 6 — Validate
```bash
aidp-fusion-autopilot content-pack validate <overlay>
```
Fix until clean — schema + content validators cover PII-missing (AIDPF-2030),
dependency/SQL integrity, and the no-new-legacy-module rule. Document new error
codes in `docs/aidpf-error-codes.md`.

### 7 — Wire the bundle for the client (one command), then hand off to seed
An overlay isn't seeded until the bundle points at it. **Do this FOR the
client** with the single wiring verb — don't hand-edit YAML:

```bash
aidp-fusion-autopilot use-pack overlays/<name> --profile <tenant>
```

Use the default aligning behavior when the client wants the bundle scope to
match every silver/gold node in the resolved pack. For narrow customer bundles
or a one-mart SQL override, preserve the existing scope instead:

```bash
aidp-fusion-autopilot use-pack overlays/<name> --profile <tenant> --no-align
```

When using `--no-align` for a new mart, add only the authored node to
`gold.marts` before seeding; otherwise `--datasets <new-id>` is outside the
bundle scope and the resolver fails with `AIDPF-1043`.

`use-pack` does the content-pack wiring in one step: sets
`contentPack: {name: <overlay-id>, path: overlays/<name>, profile}`, aligns
`dimensions.build` / `gold.marts` to the resolved pack's real nodes unless
`--no-align` is supplied (so stale v1 entries like `dim_org` / `po_backlog`
can't break the plan resolver in full-scope wiring), and
**normalizes a placeholder-vault `fusion.password` to `${FUSION_BICC_PASSWORD}`**
(the cluster loads it from the AIDP credential store; a placeholder vault ref
fails with `CredentialResolutionError`). It's comment-preserving and validates
the result. Then:

1. **Profile present** — `use-pack` warns if `profiles/<tenant>.yaml` is absent;
   run `bootstrap` (or reuse one) so it exists.
2. **Config coords** — if `aidp.config.yaml` coords are missing/placeholder,
   route to `/aidp-fusion-config` (don't make the client hand-copy OCIDs). Any
   remaining `${ENV}` ref in `bundle.yaml` must resolve **both** client-side
   (preflight) and cluster-side (literalize tenant values or set the env var).
3. **Hand to the seed step** — `/aidp-fusion-seed` (or
   `aidp-fusion-autopilot run --mode seed --datasets <new-id> --layers gold`).
   `--layers gold` lets the bronze-readiness gate verify the existing bronze
   dep instead of re-extracting it from BICC (the plan still lists the bronze
   dep; it is read, not rebuilt).
5. **Re-run `oac-dataset-advisor`** — it now sees the new **live** table and
   recommends the OAC dataset; then `workbook-authoring` builds the viz.

> **Overlay-on-installed-base requires plugin ≥ the `chain_roots` staging fix**
> (`content_pack_staging.py`): before it, seeding any overlay raised
> `AIDPF-1040` because inherited base-pack nodes weren't staged. See LIMITS.md.

---

## Skill family
`oac-dataset-advisor` (GAP) → **`mart-author`** (this skill: author node) →
`aidp-fusion-seed` (materialize) → `oac-dataset-advisor` (now COVERED) →
`workbook-authoring` (visualize). New content always lands as content-pack
YAML+SQL in an overlay, per ADR-0021 / CLAUDE.md "where new work goes".

## Safety invariants (do not regress)
- Author content-pack YAML+SQL only — **never** a new `dim_*.py` / gold `.py`.
- **Additive, non-destructive** — new node or new column; never alter an
  existing node's grain/keys, never rewrite materialized tables.
- **PVO, not bronze**, for source-field discovery (metadata-only).
- **Classify new bronze PVOs** before authoring; do not create a high-volume
  full-pull source or fake CDC by setting `incrementalCapable: true`.
- **PII mandatory** on every authored column; keep high-PII out of dashboards.
- **Overlay pack only** — never edit the shipped starter pack.
- Don't seed, don't query live data, don't create OAC datasets — hand off.

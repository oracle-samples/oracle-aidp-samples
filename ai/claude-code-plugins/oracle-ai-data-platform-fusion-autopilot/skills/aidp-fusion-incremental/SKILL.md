---
name: aidp-fusion-incremental
description: "Turn a natural-language refresh request into a guarded `aidp-fusion-autopilot run --mode incremental` (the daily delta-merge). Parses intent into scope flags (reusing the seed parser with --mode incremental), auto-satisfies preconditions, and runs the incremental-specific prechecks: a Fusion-PVO/bronze DRIFT precheck (route to /fusion-drift-doctor), a watermark-cursor check (a node with no prior cursor must be seeded first), and plan-hash awareness (if the YAML/SQL/profile changed since the last seed, AIDPF-4040 → re-seed). Use when the user says 'run incremental', 'refresh the marts', 'daily delta', 'incremental supplier_spend', 'pull the latest'. NOT for first build (use /aidp-fusion-seed) or a single CLI call you already know."
allowed-tools: Read, Bash, Glob, Grep
---

# aidp-fusion-incremental — guarded daily delta runner

Wraps `run --mode incremental` (delta-merge using prior watermarks from
`fusion_autopilot_state`) with the prechecks an incremental run actually needs.
Like `/aidp-fusion-seed`, it shells out to the CLI and never touches state
directly. Incremental is **less destructive than seed** (row-grain nodes MERGE
on the natural key; replace-marts still CREATE OR REPLACE), so the guard is
lighter — but incremental has its own failure modes that seed doesn't, and this
skill front-runs them.

## When to use
- "run incremental", "refresh the marts", "daily delta", "pull the latest",
  "incremental supplier_spend / just bronze / the marts".
- Routine refresh after an initial `seed`.

## When NOT to use
- **First build / nothing materialized** → `/aidp-fusion-seed` (incremental
  needs prior watermarks).
- A single known CLI invocation you already have.

## Workflow

### 1 — Parse intent (reuse the seed parser, mode=incremental)
Assemble known node ids (bundle datasets + `content-pack info <pack> --json`
silver+gold), then:
```bash
python3 ../aidp-fusion-seed/intent.py "<phrase>" --mode incremental --known-nodes "<ids>"
```
Honour `ambiguous` / `unknown_tokens` (list pack nodes, ask) and `needs_run_id`
(for resume) exactly as the seed skill does. `argv` comes back as
`run --mode incremental <scope>`.

### 2 — Preconditions (reuse the shared checker)
```bash
python3 ../aidp-fusion-seed/preconditions.py --bundle bundle.yaml --config aidp.config.yaml --env <env>
```
Act on `missing` via the same ladder as seed: `bundle` → init/fix; `config` →
`/aidp-fusion-config`; `profile` → `/aidp-fusion-bootstrap` (or `use-pack` if
the node is in an unwired overlay); `cluster` → start it. Re-run until
`ok: true`.

### 3 — Incremental-specific prechecks (this is what seed doesn't do)
These are the failure modes incremental adds — front-run them so the operator
gets a clear next step instead of a deep cluster error:

- **Watermark cursor present?** Incremental delta-merges from each node's prior
  `last_watermark` in `fusion_autopilot_state`. A node that was never seeded raises
  `IncrementalCursorMissingError` (it lists the offending datasets). → tell the
  operator to **`/aidp-fusion-seed`** those nodes first. (Use
  `/aidp-fusion-status` to see which nodes have a prior successful run.)
- **Plan unchanged since last seed?** If the node's YAML / SQL / tenant profile
  changed since it was seeded, the run fails with **AIDPF-4040 plan-hash drift**.
  → **re-seed** that node (`run --mode seed`) to re-pin the plan-hash (or revert
  the change). Incremental requires plan continuity with the seed that pinned it.
  Row-grain **MERGE** nodes (silver dims, `gl_balance`) increment cleanly after
  a seed — the plan-hash is mode-normalized (`LIMITS.md` P-incr-L1, resolved
  2026-06-15), so the watermark predicate rendering `1=1` (seed) vs
  `col > :watermark` (incremental) no longer false-trips 4040. A 4040 now means
  a **genuine** plan-shape change. If you edited the SQL/profile **on purpose**
  and don't want a full re-seed, pass the `--repin-plan-hash` flag (only once the user has confirmed the change was deliberate) to
  repin the new hash (writes a `mode='plan_hash_repin'` audit row); otherwise
  re-seed or revert.
- **New chart of accounts added?** A profile change that ONLY adds a new
  `chartOfAccounts.byChart` arm (existing charts byte-identical) is a *proven
  additive* change: incremental **absorbs it automatically** — no re-seed and no
  `--repin-plan-hash` needed. The per-node gate accepts the plan-hash advance
  (recording `coa_additive_accept_reason`) once the post-land COA checkpoint
  passes. A **mutating** COA change (an existing chart's segment moved/removed)
  is NOT additive and still routes to `--mode seed` (AIDPF-1048/4040) — an
  incremental would leave that chart's already-classified rows stale. So: adding
  a chart → incremental; correcting an existing chart → seed.
- **Fusion-PVO / bronze drift?** Incremental fires the drift gates — `AIDPF-2072`
  (live PVO column renamed/removed vs the pinned snapshot), `AIDPF-4070/4071`
  (source-schema), `AIDPF-2012` (bronze-table fingerprint). On any of these →
  hand to **`/fusion-drift-doctor`**, which classifies the drift and routes to
  `bootstrap --refresh` (a declared candidate still matches) or `/medallion-author`
  (a new column the pack never anticipated). Do NOT paper over it with
  `--force-fingerprint-skip` outside dev.
  An **absent** bronze table (a dataset whose extract never succeeded — e.g. the
  `LIMITS.md` L2-blocked PVO) no longer aborts the fingerprint gate: it is
  tolerated with a WARN and baseline-filled from the pinned schema snapshot, so
  every *present* table stays drift-checked and `--force-fingerprint-skip` is
  NOT needed for that case. If the snapshot is missing/desynced the gate still
  fails closed — run `bootstrap --refresh` to back-fill it (no repin needed).

### 4 — Dispatch
Fire `run --mode incremental <scope flags> --poll-timeout <N>` (default 3600).
- Scope `--layers` like seed: a mart-only refresh (`--layers silver,gold`) merges
  over existing bronze without re-extracting (the bronze-readiness gate verifies
  it). A full incremental re-extracts bronze deltas within the watermark safety
  window, then merges up.
- On non-terminal failure → capture the `run_id`, offer
  `run --mode incremental --resume <run_id>` (the original run_id is preserved
  end-to-end). When the failed run wrote a durable **run manifest**, `--resume`
  replays the manifest (topology + per-node fingerprints + mode + identity) rather
  than re-deriving scope, and a bare `--resume` never silently flips
  seed↔incremental; manifest drift guards (`AIDPF-1044/1046/1047/1048/1049`) route
  to a fresh run. A pre-manifest run uses the legacy path (scope reconstructed from
  state rows). A malformed manifest fails closed with `AIDPF-4022`.

### 5 — Present + recommend
Summarize the per-step table (dataset / layer / status / row_count / duration) +
`run_id`. On a gate failure, name the AIDPF code and route to the doctor / seed
per step 3 — point at the diagnostic at `.aidp/diagnostics/<run_id>/`.

### 6 — Hand off the next step (on success)
An incremental refreshes the **data** in the gold tables; it does not touch OAC.
Close the loop based on whether a dashboard already exists:

- **Dashboard already built** (the common day-2 case): the OAC dataset/workbook
  sits *live over* these tables, so the new rows are already visible — tell the
  user to **reopen / refresh the workbook** to see the delta. No re-author, no
  new dataset.
- **No dashboard yet**: point them forward exactly like seed step 6 —
  **`/oac-dataset-advisor`** (recommend the dataset for their question) → create
  it in the OAC UI → **`/workbook-authoring`**, or **`/aidp-fusion-autopilot`**
  to drive it. Don't auto-invoke; offer.

## Refresh-strategy reminder (don't be surprised)
- **Row-grain** nodes (silver dims, `gl_balance`) MERGE on the natural key — a
  delta, not a rebuild.
- **Aggregate / date-anchored** marts (`supplier_spend`, `ap_aging`,
  `dim_calendar`, and any replace-strategy overlay mart) CREATE OR REPLACE every
  cycle regardless of mode — incremental rebuilds them in full. (So such a mart
  must NOT use a watermark filter in its SQL — that trips AIDPF-5003; see
  `mart-author`.)

## Skill family
`/aidp-fusion-seed` (first build) → `/aidp-fusion-incremental` (deltas) ·
`/aidp-fusion-status` (what's stale / needs a delta) · **`/fusion-drift-doctor`**
(drift precheck + routing) · `bootstrap --refresh` / `/medallion-author`
(resolve PVO drift). Conducted by `/aidp-fusion-autopilot`.

## Safety invariants
- Never run incremental on a node with no prior seed cursor — seed it first.
- Never bypass a drift gate (`--force-fingerprint-skip`) outside dev; route to
  the doctor.
- Shell out to the CLI; never touch `fusion_autopilot_state` / watermarks directly.

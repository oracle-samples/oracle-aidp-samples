---
name: aidp-fusion-seed
description: "Turn a natural-language seed request into a correct, guarded `aidp-fusion-autopilot run --mode seed`. Parses intent into scope flags (--datasets / --layers / --strict-scope / --resume), auto-satisfies preconditions (validate + `/aidp-fusion-bootstrap` + cluster), guards the destructive replace-on-silver/gold behaviour with a fail-closed confirmation, then dispatches and offers resume on failure. Use when the user says 'seed', 'seed supplier_spend', 'seed the bronze layer', 'seed the marts', 'resume the seed', 'materialize bronze/silver/gold', or otherwise wants a first-run / re-run of the medallion pipeline. NOT for incremental runs (--mode incremental), bootstrap-only variation resolution (use `/aidp-fusion-bootstrap`), or dashboard install."
allowed-tools: Read, Bash, Glob, Grep
---

# aidp-fusion-seed — conversational seed runner

This skill turns a loose ask — "seed", "seed supplier_spend", "seed just
bronze", "resume the seed" — into a correct, guarded
`aidp-fusion-autopilot run --mode seed` invocation. It owns ONLY: intent
parsing, precondition orchestration, the destructive guard, dispatch, and
result presentation. **It shells out to the existing CLI for every stateful
action** — it never imports the orchestrator and never touches
`fusion_autopilot_state` directly (CLAUDE.md "the CLI is the contract" +
layering rule).

`seed` materializes bronze + silver + gold end-to-end and uses **replace
strategy on silver/gold** (`CREATE OR REPLACE TABLE`). Re-seeding a populated
tenant overwrites existing marts. That is why the destructive guard below is
**fail-closed**: it confirms whenever emptiness cannot be *proven*, not only
when data is seen.

## When to use

- "seed", "seed everything", "full seed" — fresh first-run of the pipeline.
- "seed supplier_spend" / "seed the supplier spend mart" — scoped to a mart
  (the D-1 implicit transitive include auto-pulls its bronze + silver deps).
- "seed dim_supplier and dim_account" — multiple named nodes.
- "seed just bronze" / "seed the bronze layer" / "seed the marts" — layer-scoped.
- "the seed died / resume the seed / finish run <id>" — resume an interrupted run.

## When NOT to use

- **Incremental runs** (`--mode incremental`) — that is the planned
  `aidp-fusion-incremental` sibling skill. This skill always runs `--mode seed`.
- **Bootstrap-only** variation resolution (no run) — use
  `/aidp-fusion-bootstrap`, or `medallion-author` for tier-2 overlay drafting.
- **Dashboard install / OAC** — use the `dashboard` command / OAC skills.
- **Unit-test runs** — use `pytest`. **Dataset-extract debugging** — use the
  catalog probe commands.

## Helper files (this skill folder)

| File | Role | Invoked via |
|---|---|---|
| `intent.py` | Deterministic phrase → `run` argv parser + resume resolver (tested). | `Bash`, returns JSON |
| `preconditions.py` | Reusable readiness checker (validate + profile + cluster + config coords). | `Bash`, returns JSON |
| `guard.py` | Fail-closed destructive-guard decision (confirm vs proceed) over per-target facts. | `Bash`, returns JSON |

Both are pure Bash-invoked helpers that emit JSON on stdout. Run them with the
plugin's Python (the repo's `.venv/bin/python` if present, else `python3` with
the plugin on `PYTHONPATH` — `preconditions.py` self-bootstraps `scripts/` onto
`sys.path`). Use the same interpreter the rest of the plugin's CLI uses.

---

## Workflow

### 1 — Parse intent → scope (`intent.py`)

First assemble the set of **known pack node ids** so the parser can tell a real
target from a typo. The union of:

- bronze dataset ids — from `bundle.yaml`'s `datasets[].id`;
- silver + gold node ids — from
  `aidp-fusion-autopilot content-pack info <pack> --json` (`.nodes.silver` +
  `.nodes.gold`). Resolve `<pack>` from `bundle.yaml`'s `contentPack.name`
  (fall back to `aidp-fusion-autopilot content-pack list --json` if only one is
  installed).

Then parse:

```bash
python3 intent.py "<the user's exact phrase>" \
  --known-nodes "<comma-joined known node ids>"
```

The JSON result tells you what to do next:

- **`ambiguous` non-empty** → do NOT guess. List each token's `candidates`
  (the matching node ids) and ask the user which they meant. Example: "seed
  supplier" is ambiguous across `dim_supplier`, `erp_suppliers`,
  `supplier_spend`.
- **`unknown_tokens` non-empty** → the target matches no known node. Show the
  full list of pack node ids and ask. Never seed a guessed scope.
- **`needs_run_id: true`** (resume intent, no id given) → you cannot reliably
  discover the run_id today (`status` does not print run_ids, and
  `.aidp/diagnostics/` is not a complete index). **Ask the user for the
  explicit run_id.** Do NOT scrape `status` text or guess from diagnostics dirs.
  (When `aidp-fusion-autopilot status --recent-runs --json` ships, feed its output
  to `intent.resolve_resume_run_id(recent_runs)` — it returns a single
  resumable run_id, or forces an ask on zero / multiple. Still ask on either.)
- Otherwise → `argv` is the resolved `run` invocation. Show the user the
  expanded scope (especially the D-1 dep expansion is made visible by the
  `--dry-run` plan in step 3 before anything destructive happens).

### 2 — Check + auto-satisfy preconditions (`preconditions.py`)

```bash
python3 preconditions.py --bundle bundle.yaml --config aidp.config.yaml --env <env>
```

Returns `{ok, missing[], tenant, profile_path, profile_present, dispatch_mode,
cluster_state, config_placeholders[], validate_ok, details{}}`. Act on
`missing` — this is the **auto-fix ladder**:

- **`missing` contains `"bundle"`** (validate failed / bundle unloadable) →
  **stop and tell the user to `init` / fix.** Connectivity (`fusion:` block,
  credentials) is customer policy the CLI cannot safely auto-generate. Surface
  `details.validate`.

- **`missing` contains `"config"`** (and `config_placeholders` is non-empty) →
  the dispatch **coordinates** in `aidp.config.yaml` are absent or still
  `*-PLACEHOLDER` (`workspaceKey` / `clusterKey` / `aiDataPlatformId`). These
  are resolvable from human-friendly names — do NOT dead-end. Tell the user to
  run **`/aidp-fusion-config`**, or shell out to:
  ```bash
  aidp-fusion-autopilot init-config --aidp-id <OCID> --workspace "<name>" --cluster "<name>"
  ```
  which resolves the keys so they never hand-copy OCIDs. (Still stop-and-ask on
  missing `fusion:` connectivity — `init-config` cannot supply that.)

- **`missing` contains `"profile"`** (no `contentPack.profile`, or
  `profiles/<tenant>.yaml` absent) → **route through `/aidp-fusion-bootstrap`**:
  ```bash
  aidp-fusion-autopilot bootstrap --check-iam
  ```
  ⚠️ Bootstrap **freezes tenant variation choices** (column-alias /
  semantic-variant picks) into the profile. **Surface what it resolved** to the
  user — do not let it silently pin choices. If bootstrap hits a multi-match
  that needs a human, let it prompt interactively; never pass `--non-interactive`
  for a real operator run.

- **`missing` contains `"cluster"`** (`cluster_state` ≠ `ACTIVE`) → surface
  `details.cluster`. If `STOPPED`, offer to start it (via the AIDP UI or the
  cluster start action); if `unprobed`, the coords/auth could not reach the
  plane — fix config first. Do not dispatch against a non-ACTIVE cluster.

- **Requested node lives in an overlay not wired into `contentPack`** (e.g. a
  mart `mart-author` just authored under `overlays/<name>/`). The orchestrator
  only knows nodes in the **active** `contentPack`, so a node not in it parses
  as unknown. Wire it for the client with the one-command verb:
  ```bash
  aidp-fusion-autopilot use-pack overlays/<name> --profile <tenant>
  ```
  (sets `contentPack`, aligns `dimensions`/`marts`, normalizes the credential
  ref — see `mart-author` step 7), then re-run. For a **narrow bundle** or a
  **one-mart override**, add `--no-align` so it keeps the bundle's existing
  `datasets`/`gold.marts` instead of broadening to every node in the resolved
  pack. If the overlay doesn't exist yet, route to `/mart-author`.

- **Cluster-side credential gotcha (pre-empt before dispatch).** If
  `bundle.yaml`'s `fusion.password` is a placeholder vault OCID, the cluster run
  fails with `CredentialResolutionError`. Set `fusion.password:
  ${FUSION_BICC_PASSWORD}` (loaded cluster-side from the AIDP credential store
  via `biccSecretName`), and make sure every `${ENV}` ref in `bundle.yaml`
  resolves **both** client-side (preflight `load_bundle`) and cluster-side
  (literalize tenant values, or set the env var in both places).

**Dispatch mode**: default **cluster REST** (no `--inline`). Switch to
`--inline` only when the environment is clearly an AIDP notebook session
(Spark + checkpointer + vault present) or the user explicitly asks. When in
doubt, ask.

Re-run `preconditions.py` after any auto-fix until `ok: true`.

### 3 — Destructive guard (FAIL CLOSED)

`seed` overwrites silver/gold. Before dispatching a **real** seed you MUST run
both probes and classify the outcome into **three** buckets — and the bar for
"proceed" is high:

```bash
aidp-fusion-autopilot status                                   # informational
aidp-fusion-autopilot run --mode seed <scope flags> --dry-run  # resolved plan
```

`--dry-run` prints the **"Would dispatch"** table (every in-scope
`dataset_id` + `layer` the seed would `CREATE OR REPLACE` / MERGE) plus the
extra-plan prerequisites with table paths. That table defines the **set of
in-scope target tables**.

Classify:

1. **Proven populated** — a resolved in-scope target table physically exists
   with rows → print an explicit **overwrite warning** listing the affected
   tables and **require confirmation** before dispatching.
2. **Proven empty** — *only* when **every** resolved in-scope target table is
   inspectable AND confirmed absent-or-zero-rows by inspecting the **actual
   table** → show the dry-run plan and proceed without prompting.
3. **Unknown / unprovable** — **fail closed: require confirmation.**

> **"Proven empty" is a PHYSICAL TARGET-TABLE check, never a state-row check.**
> Two fail-open traps you MUST close:
>
> 1. **State ≠ tables.** `aidp-fusion-autopilot status` reads `fusion_autopilot_state`
>    (run metadata), NOT the physical silver/gold marts. An empty or stale
>    state table can read "empty" while populated `dim_supplier` /
>    `supplier_spend` already exist. **State rows are a hint, never proof.**
> 2. **Unreadable ≠ empty.** `status` today is local-Spark/Rich only, has **no
>    JSON mode**, and **returns exit 0 with just a "pyspark not available" /
>    "cannot read state table" message** on the common laptop-to-cluster path.
>    "Could not inspect" is **unknown → confirm**, never "empty".

Encode the decision with `guard.py` (do not hand-roll it):

```bash
# Today: no per-target facts available -> ALWAYS confirm (fail closed).
python3 guard.py --targets-json '[]'
# Future, once `status --json` emits per-target physical facts:
python3 guard.py --status-json-supported \
  --targets-json '[{"target_table":"silver.dim_supplier","target_exists":true,"target_row_count":4213,"readable":true}]'
```

`guard.py` returns `{decision: "confirm"|"proceed", reason, populated_tables,
unprovable_tables}`. Honour `decision` exactly: on `"confirm"`, show the
overwrite warning (use `populated_tables` / `unprovable_tables` + the dry-run
target list) and require an explicit "yes"; only `"proceed"` skips the prompt.

**Today, with the current CLI, `guard.py` returns `"confirm"` every time** —
there is no `status --json` emitting per-target `{target_exists,
target_row_count, readable}` (so call it WITHOUT `--status-json-supported`).
You cannot machine-prove a physical target table is empty from the laptop.
**Therefore: confirm before every real seed.** Present the dry-run "Would
dispatch" target list as the tables about to be created-or-replaced, and get an
explicit "yes" — even on what looks like a fresh tenant.

**When the richer CLI ships** (`status --json` with per-resolved-target
`target_exists` / `target_row_count` / `readable`, working in the
cluster-dispatch path): "proven empty" holds **only if every** resolved
in-scope target is `readable: true` AND (`target_exists: false` OR
`target_row_count == 0`). If **any** resolved target is `readable: false` /
missing / errors, classify the whole outcome as **unknown → confirm**. Never
infer empty from a subset, and never decide off `fusion_autopilot_state` rows.

### 4 — Dispatch + resume

Fire the real run (drop `--dry-run`):

```bash
aidp-fusion-autopilot run --mode seed <scope flags> --poll-timeout <N>
```

- `--poll-timeout` default **3600** (1h). Recommend **14400** (4h) for a
  first-time full seed against a slow Fusion pod (cold-cache BICC extracts;
  `gl_period_balances` can be the long pole). Valid range 60–14400.
- For a **resume**: `aidp-fusion-autopilot run --mode seed --resume <run_id>`
  (omit `--datasets` / `--layers` unless narrowing). When the failed run wrote a
  durable **run manifest**, `--resume` *replays the manifest* (canonical topology,
  per-node fingerprints, mode, and identity) rather than re-deriving scope; a bare
  `--resume` never silently flips seed↔incremental. Manifest-present drift guards
  (topology `AIDPF-1044`, node/pack `AIDPF-1049`, identity/profile/COA-policy
  `AIDPF-1048`, scope `AIDPF-1047`, mode `AIDPF-1046`) route to a fresh seed. A
  pre-manifest run falls back to the legacy path (scope reconstructed from state
  rows). A malformed/unreadable manifest fails closed with `AIDPF-4022` — start a
  fresh seed.
- **On non-terminal failure / interruption**: capture the printed `run_id` and
  offer `aidp-fusion-autopilot run --mode seed --resume <run_id>`. The original
  run_id is preserved end-to-end (medallion `_run_id` audit invariant).

### 5 — Present the result

Summarize the CLI's per-step table: **dataset / layer / status / row_count /
duration**, plus the `run_id` and success/failed/skipped counters. On failure,
point the user at the diagnostic artifact the CLI wrote at
`.aidp/diagnostics/<run_id>/` (e.g. the `AIDPF-*.json` for gate failures) and
read it back to explain the AIDPF code + remediation.

### 6 — Hand off the next step (on success)

A successful seed materializes the tables but does **not** build a dashboard.
Close the loop by pointing the user at what comes next — name the **gold**
table(s) just seeded (those are the dashboard-facing ones):

> Seeded `gold.<table>` (N rows). To turn this into a dashboard:
> 1. **`/oac-dataset-advisor`** — tell it your dashboard question (e.g. *"supplier
>    spend by currency"*); it inspects the **live gold you just seeded** and
>    recommends the exact dataset (tables, columns, join key).
> 2. Create that dataset in the **OAC UI** (dataset creation is an OAC action —
>    the MCP server has no create-dataset tool).
> 3. **`/workbook-authoring`** to generate the workbook on it.
>
> Or run **`/aidp-fusion-autopilot`** with your goal to drive all of the above
> end-to-end (it detects what's already seeded and skips ahead).

Keep it to the tables actually seeded this run; don't advertise marts the scope
didn't build. This is advice, not an auto-invocation — let the user choose.

---

## Skill family

This is the first of a planned family — **seed / incremental / bootstrap /
status** — that shares a substrate:

- **`preconditions.py`** is the reusable contract. Its JSON shape (`ok`,
  `missing[]`, `tenant`, `profile_path`, `dispatch_mode`, `cluster_state`,
  `config_placeholders[]`) is what the sibling skills consume so each does not
  re-derive readiness. New readiness signals get added here, once, with a
  default that degrades safely.
- **`intent.py`** is `--mode seed`-specific today, but its phrase→argv shape
  (known-node classification, `ambiguous` / `unknown_tokens` / `needs_run_id`
  fail-safes) is the template the incremental sibling will extend with
  watermark-aware vocabulary.

Both helpers are import-safe and dependency-light precisely so the siblings can
reuse them without dragging in the orchestrator.

## Safety invariants (do not regress)

- **Never guess a dataset.** `ambiguous` / `unknown_tokens` → list pack node
  ids and ask.
- **Never assume empty.** State-empty and could-not-inspect both → confirm.
- **Never auto-generate connectivity.** Missing `fusion:` / credentials →
  stop and ask. Only the **coordinates** in `aidp.config.yaml` are
  auto-resolvable (via `/aidp-fusion-config`).
- **Never require the user to hand-copy OCIDs** for the config-coords case —
  route to `/aidp-fusion-config`.
- **Never re-implement OCI signing** in the helpers — `preconditions.py` calls
  the existing `AidpRestClient` / loaders.

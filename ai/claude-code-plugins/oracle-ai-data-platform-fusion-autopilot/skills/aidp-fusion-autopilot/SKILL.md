---
name: aidp-fusion-autopilot
description: "End-to-end conductor for the Fusion to AIDP to OAC dashboard journey. Takes one high-level goal ('I want a supplier-spend vs GL-balance dashboard from Fusion') and drives the whole chain: configure, connect OAC MCP, bootstrap, seed, advise dataset, author mart if needed, create OAC dataset, author workbook, and optionally enable end-user MCP chat by detecting current state and delegating each step to the right sibling skill/command. Auto-advances on clean steps; pauses for real decisions (destructive seed, variation freeze, OAC dataset creation, ambiguous intent, gaps). Use when the user states a dashboard/analytics goal and wants it driven start-to-finish, OR on first run after installing the plugin: 'I just installed this, what now', 'get me started', 'set me up', 'set up Fusion analytics from scratch / end to end', 'autopilot this', 'take me from nothing to a dashboard'. This is the front door for a fresh install (setup scaffolds the bundle). NOT for a single known step (call that skill directly)."
allowed-tools: Read, Bash, Glob, Grep, mcp__oac-mcp-server__oracle_analytics-search_catalog, mcp__oac-mcp-server__oracle_analytics-find_matching_datasources, mcp__oac-mcp-server__oracle_analytics-describe_data
---

# aidp-fusion-autopilot — one goal, driven A-to-Z

Turns *"I want a CFO dashboard of supplier spend vs GL balance, by currency"*
into a finished workbook by **conducting the existing skill family** — it does
not re-implement any step. Its only jobs are: **detect where the user already
is**, **drive the next incomplete step via the right skill**, and **stop at the
decisions a human must make**.

This is the single entry point so the user never has to know which of the
seven skills to invoke, or in what order.

## When to use
- The user states a goal and wants it taken end-to-end ("take me from nothing
  to a dashboard", "set up Fusion analytics for X", "autopilot this").
- Resuming a half-done journey ("finish setting up my supplier dashboard").

## When NOT to use
- A single known step → call that skill directly (`/aidp-fusion-seed`,
  `/oac-dataset-advisor`, `/mart-author`, `/workbook-authoring`, …). Autopilot
  is overhead when the user already knows the one thing they want.

## Operating principles
1. **Compose, never reimplement.** Each step delegates to a sibling skill or a
   CLI command. Autopilot owns only sequencing + state.
2. **State-first — don't redo finished work.** Detect each step's status before
   acting (a tenant with gold already seeded skips straight to advise).
3. **Inherit the sub-skills' guards.** The seed destructive guard, the advisor's
   live-evidence rule, bootstrap's variation-freeze surfacing — autopilot never
   weakens them. It auto-advances only on a clean result.
4. **Pause at human decisions** (see gates). Auto-advance otherwise. Always show
   the journey state + what it's about to do before a state-changing step.

---

## The journey (step state machine)

| # | Step | Done when (detect) | Drive with | PAUSE before if |
|---|---|---|---|---|
| 1 | **Config** | `bundle.yaml` + `aidp.config.yaml` exist; coords non-placeholder | `aidp-fusion-autopilot init` (scaffold if absent — fresh install) → `/aidp-fusion-config` for coords | missing `fusion:` connectivity (human-only) |
| **1b** | **OAC MCP connect** (front-loaded prerequisite — see §below) | `oac-mcp-server` tools answer a live `search_catalog` ping | project-scoped `dashboard mcp-setup`, then **restart/reconnect Claude Code** | **always when dead** — staging the connector needs a Claude Code restart before its tools work; write the resume checkpoint, then PAUSE for the restart |
| 2 | **Bootstrap** | `profiles/<tenant>.yaml` present + fingerprint pinned | `/aidp-fusion-bootstrap` | multi-match variation needs a human pick (never `--non-interactive`); surface frozen picks |
| 3 | **Seed** | live gold has the needed tables (probe) | `/aidp-fusion-seed` | always confirm the destructive guard's CONFIRM outcome |
| 4 | **Advise** | — (always run for the goal) | `/oac-dataset-advisor` | — |
| 5 | **Author mart** (only on advisor GAP) | the gap node exists live after seed | `/mart-author` → `use-pack` → back to Step 3 | confirm the authored change before seeding it |
| 6 | **OAC dataset** | a dataset over the recommended table(s) exists (OAC MCP) | `/oac-dataset-setup` | **always** — dataset creation is an OAC UI action today (MCP can't create datasets); hand the exact spec to the user |
| 7 | **Workbook** | a workbook on that dataset exists/renders | `/workbook-authoring` | confirm overwrite if replacing an existing workbook |
| 8 | **End-user MCP chat** (optional deliverable) | least-privilege OAC user handed the connect steps | `docs/oac_mcp_setup.md` hand-off | confirm the end-user OAC account is **least-privilege** (v1.4 exposes write/delete/ACL tools) |

> **Steps 6–7 read OAC through the `oac-mcp-server` tools** (autopilot's own
> detectors *and* `/workbook-authoring`'s required tools). That connection is
> **Step 1b**, a prerequisite — not Step 8. Step 8 is a *different* thing:
> enabling *end users* to chat with their own clients. Don't conflate them.

## Step 1b — OAC MCP connectivity (front-loaded prerequisite)

**Why front-loaded (not Step 8).** `/workbook-authoring` and autopilot's own
step-6/7 detectors *consume* the `oac-mcp-server` tools (`search_catalog`,
`describe_data`, save-validation). They are a **prerequisite** for the OAC half
of the journey. And establishing the connector (`dashboard mcp-setup`) requires
a **Claude Code restart/reconnect** before the tools come
alive (`docs/oac_mcp_setup.md` — MCP servers bind at session start; a mid-flow
setup can't transparently light them up). So the connection is set up **early,
once**, before the minutes-long Bootstrap/Seed — by the time those finish the
OAC tools are live and phases 6–7 flow with no further interruption.

**Run it as early as OAC coords exist.** It needs the OAC instance + creds, which
come from Config — so on a fresh install Step 1b falls **right after Step 1**,
before Bootstrap. On an already-configured tenant, probe it first thing.

**The gate:**
1. **Probe liveness** — a cheap `oracle_analytics-search_catalog` ping (or
   `claude mcp list` → expect `oac-mcp-server ✔ Connected`).
2. **Live → continue** to Bootstrap; record `[✓] mcp connect`.
3. **Dead → set up + restart.** From the customer project directory, run:
   ```bash
   env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
   aidp-fusion-autopilot dashboard mcp-setup \
     --connector-js <path-to-oac-mcp-connect.js>
   ```
   This stages the connector, writes the 0600 connector config, and wires
   the project `.mcp.json`. The `env -u ...` wrapper prevents a global shell
   OAC profile from overriding the project `.env`. Before stopping, write
   `.aidp/autopilot/resume.md` with the user's goal and next step. Then
   **PAUSE with an explicit handoff**: *"OAC MCP staged — restart/reconnect
   Claude Code (`/mcp` → reconnect `oac-mcp-server`, or relaunch), then paste:
   `Resume the Fusion dashboard workflow from .aidp/autopilot/resume.md`."*
   Autopilot is state-first, and the checkpoint preserves the goal if chat
   context is lost. **Do not** try to proceed into phases 6–7 on a dead
   connection.

## Persistent resume checkpoint

Before any pause that may cross a Claude Code restart/reconnect or a long
manual UI step, write a non-secret checkpoint at:

```text
.aidp/autopilot/resume.md
```

Use the helper from this skill directory:

```bash
python3 skills/aidp-fusion-autopilot/write_resume_checkpoint.py \
  --workdir . \
  --goal "<dashboard or workflow goal>" \
  --phase "Step 1b OAC MCP connect" \
  --next-step "Reconnect Claude Code, verify oac-mcp-server, then re-run /aidp-fusion-autopilot" \
  --completed "bundle.yaml and aidp.config.yaml exist" \
  --pending "OAC MCP liveness check" \
  --evidence "OAC URL configured" \
  --note "No secrets recorded"
```

Resolve the script path relative to the active skill directory when the user is
running from a separate customer bundle. Do not put passwords, private keys,
tokens, full OCIDs, or connection payloads in the checkpoint. On resume, read
the checkpoint first, then re-probe live state; the file is context, not proof.

## First run (fresh install)
If `bundle.yaml` / `aidp.config.yaml` don't exist yet (brand-new install), Step
1 starts from zero.

First, make sure the CLI exists. On a plugin-only install, the user may have
downloaded the Claude Code plugin but not manually run `pip install -e`. Do not
send them back to a setup guide for that. Check `command -v aidp-fusion-autopilot`;
if it is missing, install the CLI from the plugin root that contains this skill
directory, then continue. The plugin root is two directories above this file
(`skills/aidp-fusion-autopilot/../..`). Use the active Python, create or reuse
the plugin repo's `.venv` only when needed, and keep the customer bundle
directory separate.

Then run **`aidp-fusion-autopilot init`** from the clean customer bundle directory
to scaffold `bundle.yaml`, `aidp.config.yaml`, and `.env`, then
`/aidp-fusion-config` to resolve the AIDP coords from names. Capture the user's
goal first (even a rough one) so the rest of the journey has a target; if they
have no goal yet, scaffold + configure and stop there with "what dashboard do
you want?". Don't ask for OCIDs by hand — that's what `init` +
`/aidp-fusion-config` are for.

## Returning user (re-entry fast-path — don't redo built work)

The loop is **state-first**, so a returning user with a specific goal is just a
journey that starts mid-way. Concretely, for *"autopilot — I want a dashboard of
dim_supplier"*:

1. **Map the goal to its table(s)** via `/oac-dataset-advisor` (it resolves
   *"supplier"* → the gold/dim table(s) that answer it).
2. **Detect that entity's state**, in order, and **jump to the first gap**:
   - **No live gold table** for it → start at **Step 3 (seed)** (Bootstrap first
     if no profile).
   - **Seeded, but no OAC dataset** over it → **Step 6** (`/oac-dataset-setup`
     hands the dataset spec to the user for the OAC UI and verifies it after).
   - **Dataset exists, no workbook** → skip straight to **Step 7
     (`/workbook-authoring`)** — this is the common "already built upstream, just
     need the dashboard" case.
   - **Workbook already exists** → report it + its `viewUrl`; offer to
     **refresh/open** it or author a *new* one. Don't silently re-author.
3. Step 1b (OAC MCP connect) still gates anything that reads OAC — probe it
   before the dataset/workbook detection, since that detection *uses* the MCP
   tools (a dead connection ≠ "nothing exists").

So the answer to "if supplier already exists, go to workbook authoring; if not,
seed first" is the loop's normal behavior — this section just makes the entity-
scoped routing explicit.

## The loop
1. **Capture the goal** — restate the dashboard the user wants (metrics,
   dimensions, grain). Keep it; every phase serves it.
2. **Assess state** — run the cheap detectors (below) to find the **first
   incomplete step** *for the goal's entity* (see re-entry fast-path above).
   Report the full journey status (✓ done / ▶ next / ⏸ pause).
3. **Drive that step** via its skill/command.
4. **On clean success → advance** to the next step and repeat. **On a pause
   gate → stop, present the decision, wait** for the user.
5. **Stop when** Step 7 (workbook) is done — and offer Step 8 (end-user MCP
   chat) if the user wants downstream clients to query OAC directly.

**The one hard ordering rule:** never enter phases 6–7 with a dead
`oac-mcp-server`. If the Step 1b probe fails, set up, write the resume
checkpoint, hand off the restart, and stop there. Re-running autopilot after the
restart reads the checkpoint and resumes the journey.

## State detection (reuse the family's helpers — no new probes)
- **Config / profile / cluster:** `python3 skills/aidp-fusion-seed/preconditions.py
  --bundle bundle.yaml --config aidp.config.yaml --env <env>` → `{ok, missing[],
  config_placeholders[], cluster_state}`.
- **Live gold (what's seeded):** the advisor's live-catalog probe
  (`tests/live/aidp_catalog_probe_live.py` → `skills/oac-dataset-advisor/catalog_inventory.py`).
  **Live AIDP catalog is the evidence — never pack YAMLs.**
- **What the pack could build (seed-vs-gap routing):**
  `python3 skills/oac-dataset-advisor/pack_capability.py`.
- **OAC MCP liveness (Step 1b gate):** a cheap `oracle_analytics-search_catalog`
  ping, or `claude mcp list` (expect `oac-mcp-server ✔ Connected`). A failure /
  auth error means **route to Step 1b setup + restart** — it does **not** mean
  the catalog is empty.
- **Existing OAC datasets / workbooks:** OAC MCP `search_catalog` /
  `find_matching_datasources` / `describe_data`. **Only trust these once Step 1b
  is green** — a dead connection returning nothing must never be read as "no
  dataset/workbook exists" (false negative → autopilot would wrongly try to
  re-author).

## Pause gates (never auto-do these)
- **Destructive seed** — honour `/aidp-fusion-seed`'s fail-closed guard; if it
  says CONFIRM, surface the affected tables and wait.
- **Bootstrap variation freeze** — surface the resolved column-alias /
  semantic-variant picks; on a multi-match, let a human choose.
- **Mart authoring** — show `/mart-author`'s chosen change (rung, blast radius)
  before authoring/seeding it.
- **OAC dataset creation** — delegate to `/oac-dataset-setup`; it presents the
  exact spec (tables, columns, join key), pauses for the OAC UI action, then
  verifies the saved dataset with MCP.
- **OAC MCP not connected (Step 1b)** — stage it with `dashboard mcp-setup`,
  write `.aidp/autopilot/resume.md`, then **stop for the operator to
  restart/reconnect Claude Code**; the tools can't activate inside the current
  session. Resume from the checkpoint on re-run.
- **Missing `fusion:` connectivity / non-least-privilege MCP user** — stop and ask.
- **Anything overwriting populated data or external state.**

## Output
A compact journey ledger each turn — e.g.:
```
goal: supplier spend vs GL closing balance, by currency
[✓] config   [✓] mcp connect   [✓] bootstrap   [✓] seed (gl_balance, supplier_spend live)
[✓] advise → COVERED: dataset over gl_balance + supplier_spend on currency_code
[⏸] OAC dataset → create in OAC UI (spec below), then I continue
[ ] workbook   [ ] end-user mcp chat (optional)
```
On a pause, state exactly what you need from the user and which step resumes.

## Skill family (what autopilot conducts)
`/aidp-fusion-config` · `/aidp-fusion-bootstrap` / `medallion-author` · `/aidp-fusion-seed` ·
`/aidp-fusion-status` · `/oac-dataset-advisor` · `/oac-dataset-setup` ·
`/mart-author` (+ `use-pack`) · `/workbook-authoring` · `/aidpf-error-triage` ·
`dashboard mcp-setup`. Day-2: `/aidp-fusion-incremental` (deltas) with
`/fusion-drift-doctor` as its drift precheck. On known drift gates
(`AIDPF-2072`/`4070`/`4071`/`2012`/`4040` — schema/PVO drift, plan-hash), route
directly to `/fusion-drift-doctor`, which diagnoses and hands to
`bootstrap --refresh` / `/medallion-author` / re-seed. On any other `AIDPF-*`
code or ambiguous failure, route first to `/aidpf-error-triage`. Autopilot adds
no mechanism — it sequences these and holds the user's goal across them.

## Safety invariants (do not regress)
- Never weaken a sub-skill's guard to "keep moving."
- Never claim a phase is done without its detector confirming it (live evidence
  for seed/gold; OAC MCP for dataset/workbook).
- Never enter phases 6–7 with a dead `oac-mcp-server`, and never read a dead/
  unauthenticated MCP connection as an empty catalog — set up Step 1b + restart
  first.
- Never guess recovery from an unfamiliar `AIDPF-*` code; route to
  `/aidpf-error-triage`.
- Never create the OAC dataset or seed populated data without the gate.
- Surface every irreversible/external action before doing it.

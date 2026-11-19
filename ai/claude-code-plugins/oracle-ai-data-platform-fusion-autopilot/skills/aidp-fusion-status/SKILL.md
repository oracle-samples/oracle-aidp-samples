---
name: aidp-fusion-status
description: "Read-only health + freshness report for the Fusion->AIDP pipeline. Answers 'did the last seed/incremental succeed?', 'what's stale or failed?', 'which marts are materialized?', 'what needs re-running?'. Cross-references fusion_autopilot_state (recorded run history) with the LIVE AIDP catalog (what's actually materialized) so a 'success' row next to a missing table reads STALE, and a table with no run record reads UNTRACKED — the live table is the evidence, the state row the overlay. Use when the user asks about pipeline state/health/freshness/last-run, or before deciding what to (re)seed. Does NOT run or change anything (read-only); for running deltas use /aidp-fusion-incremental, for seeding /aidp-fusion-seed."
allowed-tools: Read, Bash, Glob, Grep
---

# aidp-fusion-status — is my pipeline healthy, and what's stale?

The read-only observability member of the family. It reports per-node health by
**reconciling two truth sources** and never changes anything.

## When to use
- "Did my last seed/incremental succeed? what failed?"
- "What's stale / hasn't refreshed / needs re-running?"
- "Which gold marts are actually materialized, and with how many rows?"
- As a pre-check before `/aidp-fusion-seed` or `/aidp-fusion-incremental`.

## When NOT to use
- To *run* anything — seeding is `/aidp-fusion-seed`, deltas are
  `/aidp-fusion-incremental`. This skill is strictly read-only.

## Evidence discipline (load-bearing)
**The live table is the evidence; the state row is the overlay.**
`fusion_autopilot_state` is run *metadata* — a `success` row can sit next to a
dropped/empty mart, and a table can exist with no state row. So health keys off
the **live AIDP catalog** (what's materialized), cross-checked against the state
row. Never report "healthy" from a state row alone. (Same rule as the seed
guard / the advisor — [[feedback_live_catalog_is_evidence]].)

## Helper

| File | Role | Invoked via |
|---|---|---|
| `status_report.py` | Classify per-node health from `{state rows} × {live tables}`. | `Bash`, JSON in/out |

Health classes: `HEALTHY` · `EMPTY_OK` · `STALE` (success recorded, table
absent) · `FAILED` · `SKIPPED` · `UNTRACKED` (table exists, no run record) ·
`NEVER_RUN` (declared node, nothing anywhere).

## Workflow
1. **Acquire both signals** (cluster-side — the laptop `aidp-fusion-autopilot
   status` is local-Spark-only and has no JSON):
   ```bash
   .venv/bin/python tests/live/aidp_state_probe_live.py \
     --aidp-id <OCID> --workspace-key <key> --cluster-key <key> \
     --cluster-name <name> --out status.json
   ```
   It runs, on an ACTIVE cluster, `SHOW TABLES` + `COUNT(*)` over the gold (and
   optionally silver/bronze) schema → `live`, and the latest-per-dataset query
   over `<catalog>.<bronzeSchema>.fusion_autopilot_state` → `state`, into one JSON.
   *(When `aidp-fusion-autopilot status --json` ships, use it instead.)*
2. **Classify**:
   ```bash
   python3 skills/aidp-fusion-status/status_report.py --input status.json
   ```
   Optionally add `known_nodes` (from `content-pack info <pack> --json` + bundle
   datasets) so declared-but-never-built nodes show as `NEVER_RUN`.
3. **Render** a per-node ledger + the `attention` list (STALE/FAILED/UNTRACKED/
   NEVER_RUN), with `lastRun` / `lastStatus` / `stateRowCount` vs
   `liveRowCount`.
4. **Recommend, don't act** — e.g. "re-run `gl_balance` (STALE) via
   `/aidp-fusion-seed`"; "investigate `mystery_mart` (UNTRACKED — built outside
   the orchestrator)"; "`po_backlog` NEVER_RUN — seed it or drop it from the
   bundle." Never run anything yourself.

## Output (example ledger)
```
fusion_catalog.gold — 4 nodes
  ✓ HEALTHY   ar_invoice_summary  last=2026-06-15 success  state=49  live=49
  ✓ HEALTHY   supplier_spend      last=2026-06-14 success  state=212 live=212
  ⚠ STALE     gl_balance          last=2026-06-09 success  live=absent
  ⚠ UNTRACKED _phase5_probe        — no run record (excluded: probe table)
attention: gl_balance (re-seed), 
```

## Skill family
- **Consumed by `aidp-fusion-autopilot`** as a state detector ("where am I /
  what's stale?").
- Pairs with `/aidp-fusion-seed` (first build) and the planned
  `/aidp-fusion-incremental` (deltas) — status tells you *what* to (re)run; they
  run it.

## Safety invariants
- **Read-only** — never seeds, runs, or mutates anything.
- **Live table = evidence**; never report HEALTHY off a state row alone.
- Exclude audit/probe tables (`fusion_autopilot_state`, `_*`) from UNTRACKED noise.

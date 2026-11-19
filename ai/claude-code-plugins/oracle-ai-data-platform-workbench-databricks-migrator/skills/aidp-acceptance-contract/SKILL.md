---
name: aidp-acceptance-contract
description: Manifest-declared acceptance contract for batch and streaming pipelines that need convergence verification, not just "no exceptions". Declares PASS only after K consecutive zero-result windows (consecutive-zero-window convergence). Use for streaming jobs, slowly-draining backfills, or any pipeline whose success is "the pending queue is empty".
---

# `aidp-acceptance-contract` — convergence verification for batch/streaming jobs

A "no exception" pass is not enough for streaming or backfill pipelines. They might still be processing rows that haven't drained yet. This skill wires up a manifest-declared contract: PASS only after K consecutive empty-pending windows.

## When to use

- Source pipeline is **structured streaming** (the migrator's Pass-2 returns when the trigger completes, but the stream itself hasn't necessarily drained).
- Source pipeline is a **slowly-draining batch backfill** (each task processes a partition, the workflow PASSes when the partition is processed but a separate retry queue holds the failed rows).
- User asks "wait for it to settle", "convergence", "acceptance test on convergence".

**Not needed** for plain ETL where success = "the saveAsTable finished".

## The pattern

```
1. Migrator runs the task's cells to completion (all PASS).
2. The task's acceptance contract then probes periodically (every `sleep_between_s` seconds).
3. Each probe runs `pending_count_sql` and checks `pending = 0`.
4. After `zero_window` consecutive zero-probes → the task keeps its PASS.
5. If we hit `max_attempts` without convergence → the task's status is demoted
   from PASS to ACCEPTANCE_CONTRACT_VIOLATED (non-PASS; runner exits non-zero).
```

## Contract format — a key IN the manifest (no flag, no sidecar file)

There is **no `--acceptance-contract` CLI flag**, and the migrator does **not** auto-load a YAML that happens to sit next to the manifest. The contract is read from the manifest itself: an `acceptance_contract` dict on the **task entry** (or on the job entry, as a fallback for every task) — see `engine/scripts/acceptance_contract.py` and its loading in `job_migrate.py`.

Edit `reports/<MyJob>_manifest.json` and add the key to the task that needs convergence:

```jsonc
{
  "job_name": "<MyJob>",
  "tasks": [
    {
      "task_key": "<task_key>",
      "notebook_path": "...",
      "acceptance_contract": {
        "pending_count_sql": "SELECT COUNT(*) AS pending FROM <sandbox_schema>.<retry_queue_table> WHERE status = 'PENDING'",
        "zero_window": 3,
        "sleep_between_s": 30,
        "max_attempts": 60
      }
    }
  ]
}
```

Recognized fields (anything else is ignored; a missing/empty `pending_count_sql` is a parse error):

| Field | Required | Default | Meaning |
|---|---|---|---|
| `pending_count_sql` | yes | — | Query returning the pending count |
| `zero_window` | no | 1 | K consecutive zero readings required |
| `sleep_between_s` | no | 30 | Seconds between probes |
| `max_attempts` | no | 10 | Give up after N probes → VIOLATED |

There is no `task_key`, `cluster_id`, or `on_violation_log_path` field — the contract is scoped by WHERE it sits in the manifest, probes run on the migrator's existing kernel session, and each attempt's count + the verdict land in `JOB_REPORT.md`.

## Wire into a migration

Nothing extra to pass — run the migration normally and the contract on each task entry is picked up:

```bash
python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate_from_workflow.py \
  --config <customer>.yaml \
  --manifest reports/<MyJob>_manifest.json
```

Tasks without an `acceptance_contract` key are unaffected (default behavior — PASS = cells all green). The contract runs only if the task's cells all passed; it demotes, never promotes.

## Per-task vs job-wide

Put the key on a task entry to scope it to that task. A job-level `acceptance_contract` (on the job object) acts as the fallback for tasks that don't declare their own. Multiple tasks needing different convergence rules each carry their own `acceptance_contract` dict; contracts run after their respective tasks complete.

## How to read the verdict

The verdict surfaces in two places:

- **The run log** — per-attempt `[acceptance] attempt K/N: pending_count = ...` lines, a reason + diagnostic on finish (e.g. "count stable at 42 across last 5 attempts — pipeline is stuck, not slow"), and an `| acceptance: PASSED (K attempts)` suffix on the task's `RESULT:` line.
- **`JOB_REPORT.md`** — the task's `Status` column in the Task Results table reads `ACCEPTANCE_CONTRACT_VIOLATED` when the contract did not converge (there is no separate acceptance table section).

A contract result is `PASSED` / `VIOLATED` / `ERROR` (probe SQL itself failed — treated like a violation, with the error quoted). A VIOLATED or ERROR contract demotes that task's status from `PASS` to `ACCEPTANCE_CONTRACT_VIOLATED` regardless of cell-level success, and the runner exits non-zero.

## Building a good `pending_count_sql`

Some rules of thumb for the query:

- Read from a sandbox table you control (most likely the redirect-schema version of the source's queue / retry table).
- The query must return an integer count as the FIRST column of the FIRST row (that one value is what the framework reads; a non-integer result fails the contract with an error).
- The query must be FAST — it runs every `sleep_between_s`. Add appropriate filters / partitions.
- The query must be IDEMPOTENT — running it doesn't mutate state.

Example shapes:

```sql
-- Streaming retry queue
SELECT COUNT(*) AS pending
FROM <sandbox_schema>.<retry_queue_table>
WHERE status IN ('PENDING','RETRYING')
  AND ingest_ts > current_date - INTERVAL 1 DAY

-- Backfill progress
SELECT (target_count - processed_count) AS pending
FROM <sandbox_schema>.backfill_progress
WHERE backfill_id = '<id>'

-- Streaming watermark gap
SELECT GREATEST(
  0,
  CAST((unix_timestamp(current_timestamp()) - unix_timestamp(max(event_ts))) / 60 AS INT)
) AS pending
FROM <sandbox_schema>.<streaming_output_table>
```

## What this skill does NOT do

- Doesn't actually run the streaming job — the underlying job_migrate.py already executed the cells. The contract is a POST-execution verification step.
- Doesn't define what "convergence" means semantically — that's the user's `pending_count_sql`.
- Doesn't retry failed cells — it only signals "the pipeline didn't drain in time". For cell-level retries use [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md).

## After this

- If PASS: proceed with downstream verification, sign off the migration.
- If VIOLATED: investigate the queue. Either (a) the underlying job needs more time (bump `max_attempts`), (b) the queue is genuinely stuck (data issue, scheduler issue), or (c) the contract is wrong (check the SQL).

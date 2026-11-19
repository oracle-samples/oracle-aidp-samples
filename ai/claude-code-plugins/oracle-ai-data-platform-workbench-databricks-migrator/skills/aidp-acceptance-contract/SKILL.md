---
name: aidp-acceptance-contract
description: YAML-driven acceptance contract for batch and streaming pipelines that need convergence verification, not just "no exceptions". Declares PASS only after K consecutive zero-result windows (consecutive-zero-window convergence). Use for streaming jobs, slowly-draining backfills, or any pipeline whose success is "the pending queue is empty".
---

# `aidp-acceptance-contract` — convergence verification for batch/streaming jobs

A "no exception" pass is not enough for streaming or backfill pipelines. They might still be processing rows that haven't drained yet. This skill wires up a YAML-driven contract: PASS only after K consecutive empty-pending windows.

## When to use

- Source pipeline is **structured streaming** (the migrator's Pass-2 returns when the trigger completes, but the stream itself hasn't necessarily drained).
- Source pipeline is a **slowly-draining batch backfill** (each task processes a partition, the workflow PASSes when the partition is processed but a separate retry queue holds the failed rows).
- User asks "wait for it to settle", "convergence", "acceptance test on convergence".

**Not needed** for plain ETL where success = "the saveAsTable finished".

## The pattern

```
1. Migrator runs Pass-2 to completion.
2. Acceptance contract starts probing periodically (every `sleep_between_s` seconds).
3. Each probe runs `pending_count_sql` and checks `pending = 0`.
4. After `zero_window` consecutive zero-probes → declare PASS.
5. If we hit `max_attempts` without convergence → declare ACCEPTANCE_CONTRACT_VIOLATED.
   Overall migration result is demoted to FAIL.
```

## YAML contract format

Save at `reports/<MyJob>_acceptance.yaml`:

```yaml
# Acceptance contract for <task_key>
task_key: "<task_key>"
description: "Wait until the retry queue drains"
pending_count_sql: "SELECT COUNT(*) AS pending FROM <sandbox_schema>.<retry_queue_table> WHERE status = 'PENDING'"
zero_window: 3                # K consecutive zero windows required for PASS
sleep_between_s: 30           # seconds between probes
max_attempts: 60              # ceiling; 60 × 30s = 30-min hard cap

# Optional: override the cluster (default = use job_migrate's cluster)
cluster_id: null

# Optional: notify-on-violation hook (called once if max_attempts hit before convergence)
on_violation_log_path: "/tmp/<MyJob>_acceptance.log"
```

## Wire into a migration

The contract is loaded by `job_migrate.py` automatically if the YAML exists alongside the manifest:

```bash
# Convention: <MyJob>_manifest.json + <MyJob>_acceptance.yaml side by side
ls reports/
# → reports/<MyJob>_manifest.json
# → reports/<MyJob>_acceptance.yaml

python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/job_migrate.py \
  --manifest reports/<MyJob>_manifest.json \
  --acceptance-contract reports/<MyJob>_acceptance.yaml \
  # + the rest of the args
```

If `--acceptance-contract` is omitted, the contract is skipped (default behavior — PASS = Pass-2 cells all green).

## Per-task vs job-wide

The YAML's `task_key` field scopes the contract to ONE task in the manifest. If you have multiple tasks needing convergence, write a list:

```yaml
contracts:
  - task_key: "<task_a>"
    pending_count_sql: "SELECT COUNT(*) FROM <schema>.<queue_a> WHERE pending"
    zero_window: 3
    sleep_between_s: 30
    max_attempts: 60
  - task_key: "<task_b>"
    pending_count_sql: "SELECT COUNT(*) FROM <schema>.<queue_b> WHERE pending"
    zero_window: 5
    sleep_between_s: 60
    max_attempts: 30
```

Contracts run sequentially after their respective tasks complete.

## How to read the verdict

After a run, `JOB_REPORT.md` will include an acceptance-contract section:

```
## Acceptance contracts

| task_key | status | windows_observed | converged_at |
|---|---|---|---|
| <task_a> | PASS | 3 consecutive zeros | 2026-XX-XX HH:MM:SS |
| <task_b> | ACCEPTANCE_CONTRACT_VIOLATED | 60 attempts, 0 consecutive zeros | -- |
```

If ANY contract is VIOLATED, the overall `RESULT:` line is demoted from `PASS` to `ACCEPTANCE_CONTRACT_VIOLATED` regardless of cell-level success.

## Building a good `pending_count_sql`

Some rules of thumb for the query:

- Read from a sandbox table you control (most likely the redirect-schema version of the source's queue / retry table).
- The query must return a single column of integer count (the framework specifically reads column "pending" or position 0).
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

# Night Shift agent for Oracle AI Data Platform

An agent that takes the data team's night shift. It runs as an ordinary scheduled Workflow job on Oracle AI Data Platform (AIDP), and before it asks anyone for anything it proves its work: every repair it proposes is replayed under realistic delivery faults on a shallow clone of the real table. A person approves in a Workbench notebook under their own identity; the next tick writes the approved repair as a runbook row. The agent never edits anyone's job.

Pure Python plus Spark SQL on Delta tables. No external services beyond the platform's own `ai_generate` model function. The unit tests run locally with pandas and duckdb and never call the platform.

## What a tick does

Every 30 minutes the job `nightshift_heartbeat` runs on a cluster and:

1. **Evidence.** Records flight-recorder rows from this cluster's own Spark UI (stage and SQL evidence) and counts the wake events waiting for the external actuator. Error-text fingerprinting into a small label vocabulary (`TIMEOUT`, `PERMISSION_DENIED`, `SCHEMA_MISMATCH`, `EXTERNAL_SYSTEM_ERROR`, ...) and incident upserts (twelve identical failures become one `incidents` row) live in `nightshift/fingerprint.py` and are driven by the optional laptop scripts `scripts/ingest_failures.py` and `scripts/record_incident.py`, which need a workspace client (see below); the in-cluster job cannot read job runs.
2. **Gate.** Every action passes a policy engine: rules are data (`Rule` rows in `nightshift/gate.py::default_rules`, mirrored by the `policy_rules` DDL but not yet loaded from it), lowest priority first, first deny wins, quarantine to an `approvals` row, rate limits counted from the ledger, fail closed when the decision row cannot be written. A compute-budget tier (`high`, `normal`, `low_compute`, `critical`, `paused`) decides how much the agent may do; the hysteresis and grace logic in `tiers.py` is fully exercised by the external actuator, while the scheduled job keeps the previous tick's tier unless the ledger has no tick yet.
3. **Replay.** For each watched production statement (one plain `MERGE INTO <table> USING <table>`; anything else is recorded as `rejected`) it builds a shallow-clone sandbox, runs the statement twice, then under five delivery hazards: late, duplicate, out-of-order and retried batches, and a trailing batch of revisions (same key, later timestamp, one changed column). The result is diffed against a batch recompute. Verdicts: `replay_safe`, `silent_wrong` (passes a single run, fails run-twice or a hazard), `snapshot_wrong`, `crash`. The revision hazard is the one that separates an upsert from an insert-if-absent MERGE.
4. **Draft.** When a statement grades `silent_wrong`, the job asks the platform's `ai_generate` for one replacement statement and treats the reply as untrusted text: the platform parser must accept it, the declared target and source must match, the target must be a Delta table. The draft is then replayed under the same hazards on the same clone. Only a `replay_safe` draft becomes a `statement_repair` action, and the gate always quarantines it to a person. Cap: three model replies per statement in any 24 h window (a call that fails before a reply does not count but backs the runner off for an hour); a statement with a live `replay_safe` draft is not drafted again; a draft text a person already decided on is never re-proposed.
5. **Ledger.** Everything is a Delta table in `nightshift_ops`: `wake_events`, `ticks`, `budget_ledger`, `llm_ledger`, `policy_rules`, `policy_decisions`, `approvals`, `incidents`, `runbooks`, `actions`, `flight_recorder`, `replay_verdicts`, `repair_drafts`, and more (`charter_history` and `agent_registry` are created by the DDL for the actuator). No hidden state; every screen is a query.
6. **Drain.** Approvals a reviewer recorded in the Workbench notebook are re-evaluated through the same gate (the recorded approval lifts the quarantine; the draft's verdict must still be fresh), the decision row is written first, then the draft lands as a `runbooks` row with who approved it and when, and the approval is marked `executed`. Who approved is checked: an empty name, a service identity, or a name not on the optional `NS_APPROVERS` list leaves the row approved with `DENIED_ON_EXECUTE`.

## Layout

```
night-shift-agent/
  nightshift/            the package (no secrets, no instance identifiers)
    config.py              environment (NS_*) and constants
    tiers.py               budget -> tier, hysteresis, grace
    gate.py                PolicyEngine: rules as Rule rows (hardcoded defaults), first deny wins, quarantine, fail-closed audit, rate limits
    fingerprint.py         error text -> label vocabulary (+ ai_generate SQL for the fallback)
    ledger.py              DDL for the ops tables + SQL builders (idempotent INSERT/MERGE strings)
    recorder.py            flight-recorder cell source (Spark UI REST inside the kernel)
    replay.py              hazard batches, run-twice, diff, grader (pandas/duckdb locally, Spark SQL live)
    merge_adapt.py         a real MERGE -> harness template (target/source placeholders, key columns, ordering guard)
    watch.py               trust boundary for watched statements and model drafts (parser-tree allowlist)
    draft.py               prompt, ai_generate SQL, reply extraction, statement_repair action params
    drain.py               the approval drain the heartbeat runs every tick
    actuator.py            the optional external tick for control-plane actions (dry-run by default)
    aidp_io.py             thin wrapper over a workspace client (lazy import; only the laptop scripts need it)
  jobs/nightshift_heartbeat.py   the scheduled in-cluster task
  jobs/watched_merges.json       the watch list: statements to grade (and whether this runner may draft for them)
  sql/ddl_ops.sql                the ledger DDL as plain Spark SQL
  notebooks/NightShift_Approvals.ipynb   the reviewer's notebook (pending, evidence, draft text, decide, after the tick)
  notebooks/*.py                 cells to paste: flight recorder, replay template, a synthetic failing job
  scripts/                       optional laptop tooling (see below)
  tests/                         pytest, local only
```

## Prerequisites

- An AIDP workspace with a cluster (Spark 3.5, Delta) and the Workflows feature.
- A catalog where the job may create the schema `nightshift_ops` and create/drop per-run sandbox schemas named `<NS_SBX>_<tick>_<minute>` (default prefix `default.nightshift_sbx`).
- For drafting: `ai_generate` with the model ids named in `NS_DRAFT_MODEL` and `NS_DRAFT_MODEL_LOW` (defaults `xai.grok-4.7` and `xai.grok-4.20-non-reasoning`); set them to ids that `SHOW MODELS` lists on your instance. `openai.*` ids are refused by the draft step.
- Python 3.12+ for the local tests: `pip install -r requirements.txt`.

## Setup (platform side, no laptop tooling needed)

1. **Create the ledger.** Open a notebook on the cluster and run the statements in `sql/ddl_ops.sql` one per cell (or, in a Python cell, `for s in ddl.split(';'): spark.sql(s)` over the file's text, skipping empty pieces). The ledger lives in `default.nightshift_ops`; to use another catalog or schema, set `NS_OPS` on the job and regenerate the DDL with `nightshift.ledger.render_ddl_file()` (the command is in the file's header). The laptop alternative is `python scripts/deploy_ops_schema.py --apply`.
2. **Upload the agent.** In the Workbench, create `Shared/nightshift/` and upload `jobs/nightshift_heartbeat.py` and `jobs/watched_merges.json` into it, and the `nightshift/` package folder into `Shared/nightshift/pkg/` (the heartbeat adds `/Workspace/Shared/nightshift/pkg` to its import path; `NS_PKG` overrides it).
3. **Describe what to watch.** Edit `Shared/nightshift/watched_merges.json`: one entry per production statement with the target table, its ordering column (`ts`), the source table and the statement text. `"draft": true` lets this runner draft for that statement; start with `false` and turn it on once the verdicts look right. An entry may also name the key columns (`key`); without it the MERGE's ON columns are used. `draft` defaults to true when the key is absent. The file ships with two synthetic entries over `default.nightshift_demo.orders_bronze` / `orders_silver`, built by `notebooks/synthetic_failing_job.py` (or `scripts/run_synthetic.py` from a laptop): an insert-if-absent MERGE that is drafted for, and a guarded upsert that is only graded. A third, placeholder entry is there to replace with your own statement.
4. **Schedule the job.** Workflows, create a job with one Python task pointing at `/Workspace/Shared/nightshift/nightshift_heartbeat.py` on your cluster, cron `0 15/30 * * * ?` (every 30 minutes), timeout 1800 s, max concurrent runs 1. Set the environment variables you need (table below). The first run creates and drops its per-run sandbox schema, grades the watched statements and writes the tick.
5. **Review and approve.** Upload `notebooks/NightShift_Approvals.ipynb` to `Shared/nightshift/`. When an approval is pending, open it on the cluster, put the approval id in the first cell, run the Pending, Evidence and Draft cells, then run the Decide cell: it updates the row with `approved_by = current_user()`, your own identity. The next tick drains it and the last two cells show the runbook row and the executed approval.

## Environment variables

Read by the scheduled heartbeat unless marked otherwise.

| Variable | Default | Meaning |
|---|---|---|
| `NS_OPS` | `default.nightshift_ops` | the ledger schema (`<catalog>.<schema>`); the DDL in `sql/ddl_ops.sql` must be generated for the same name |
| `NS_PKG` | `/Workspace/Shared/nightshift/pkg` | where the heartbeat imports the `nightshift` package from |
| `NS_SBX` | `default.nightshift_sbx` | sandbox base; each run uses `<base>_<tick>_<minute>` and drops it; stale ones are swept after 3 h |
| `NS_WATCHED` | `/Workspace/Shared/nightshift/watched_merges.json` | the watch list |
| `NS_DRAFT_ONLY` | unset | `1` skips the flight recorder and only grades, reuses verdicts and drafts (the external actuator's drafting pass) |
| `NS_JOB_NAME` | `nightshift_heartbeat` | runner name written as provenance on verdicts, drafts and decisions; also the drafting-lease owner and the scope of the one-hour backoff after a failed model call |
| `NS_DRAFT_MODEL` | `xai.grok-4.7` | `ai_generate` model id used for drafting in the `high` and `normal` tiers |
| `NS_DRAFT_MODEL_LOW` | `xai.grok-4.20-non-reasoning` | `ai_generate` model id used for drafting in the `low_compute` tier |
| `NS_DRAFT_ATTEMPTS` | `2` | model calls per statement per tick |
| `NS_TASK_BUDGET_S` | `1800` | no replay starts that cannot finish inside the job timeout |
| `NS_REUSE_VERDICT_H` | `1` | reuse a settled verdict that recent instead of replaying again |
| `NS_DRAFT_BEFORE_REPLAY` | `1` | make every model call before any replay in the run; statements graded live are drafted next tick |
| `NS_APPROVERS` | unset | comma-separated names allowed to lift a quarantine; unset = any non-service identity |
| `NS_FORCE_TICK` | unset | `1` ignores the idempotent half-hour window (measurement runs) |
| `NS_SHAPE_ALLOWLIST` | `amd.generic,VM.Standard.E4.Flex,VM.Standard.E5.Flex,VM.Standard3.Flex` | external actuator only: comma-separated worker shapes a resize action may choose from |

The heartbeat prints one JSON line per step (`replaying`, `statement_graded`, `verdict_reused`, `drafting`, `drafting_skipped`, `drafting_deferred`, `draft_received`, `draft_failed`, `draft_rejected`, `draft_validated`, `draft_graded`, `claim_release_failed`, `approval_opened`, `approval_skipped`, `approval_executed`, `approval_failed`, `approval_denied_on_execute`) and a final summary, so the job's run output shows the tick as it happens.

## Validating

- Local: `python -m pytest` (409 tests; pandas and duckdb stand in for Spark where syntax differs, and the tests say so).
- Platform: after the first tick, `SELECT * FROM default.nightshift_ops.ticks ORDER BY started_at DESC` shows the tick, `replay_verdicts` the verdicts, `repair_drafts` the drafts and `approvals` what waits for a person. A run started by hand with Run now goes through the same path.

## Things to know

- **Approvals are only as trustworthy as the UPDATE grant on `default.nightshift_ops.approvals`.** Give that grant to an approvers role alone and set `NS_APPROVERS` on the job. The notebook writes `current_user()` under the reviewer's session, so the table's Delta history records who ran the update.
- **Identity of a scheduled run.** A scheduled run can present a different principal to the Generative AI service than the same job started by hand. If the model call is refused on the schedule but succeeds with Run now, grant that principal the Generative AI permissions in the lake's compartment; until then the job grades and gates on the schedule and drafts whenever a run is started by hand.
- **Long replays belong in job tasks**, not in notebook kernels: a replay of a large production statement can take 5 to 10 minutes, and a long kernel-side command can leave the cluster's command execution unhealthy.
- Pending approvals expire after 24 hours and the statement becomes draftable again. A second run of the job inside an already recorded half-hour window ends at once.

## Optional laptop tooling (`scripts/`)

These scripts drive the platform from outside the cluster through a workspace client: deploy the ledger and the workspace files, run the external actuator tick for control-plane actions (cluster stop or resize, dry-run by default), approve or reject from the command line, ingest failed runs and record incidents, seed the budget ledger, run the replay baseline on a statement from a file. They expect a class importable as `aidp_agent.AIDPClient`, which you supply: a signed client for the workspace REST API with the methods listed at the top of `nightshift/aidp_io.py` (jobs, job runs, clusters, metrics, notebook sessions and a generic signed request). `nightshift/aidp_io.py` imports it lazily, so nothing else in the sample needs it; the tests inject a fake. Laptop-side identifiers (region, data lake, workspace, cluster, OCI profile) come from a `.env` file next to the package; see `.env.example`. `scripts/stage_approvals_notebook.py --out file.ipynb` regenerates the approvals notebook locally.

## License

Released under the Universal Permissive License v1.0, as shown at <https://oss.oracle.com/licenses/upl/>, like the rest of this repository.

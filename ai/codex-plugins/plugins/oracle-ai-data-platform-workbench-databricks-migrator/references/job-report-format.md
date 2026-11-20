# `JOB_REPORT.md` — Format Reference

After every `aidp-migrate-job` run, the migrator emits a `JOB_REPORT.md` summarizing per-cell results. This document specifies the format so `/migration-status` (and any automation reading the report) can parse it reliably.

## Location

```
<migrate-output>/<job-name>/JOB_REPORT.md
```

Also mirrored locally to `reports/<job-name>/JOB_REPORT.md` after a successful run.

---

## File structure

```markdown
# Job Report: <job-name>

- Date: <ISO timestamp>
- Tasks: <int>
- Parameters: <JSON dict>

## Task Results

| Task | Status | OK | Failed | DataUnavail | Fixed | Tokens |
|------|--------|-----|--------|-------------|-------|--------|
| <task_a> | PASS                     | 27 | 0  | 0 | 2  | 41,210 |
| <task_b> | PARTIAL                  | 18 | 2  | 0 | 7  | 120,004 |
| <task_c> | PARTIAL_DATA_UNAVAILABLE | 12 | 0  | 3 | 1  | 33,816 |
| <task_d> | FAIL                     | 0  | -  | - | -  | 0 |
| <task_e> | ALREADY_MIGRATED         | -  | -  | - | -  | 0 |

**Total tokens across all tasks:** <int> (billed: uncached input + cache reads + cache writes + output).

(followed by a whole-job model-usage table — labeled `Claude usage` by the engine regardless of provider — per-route breakdown including
dependency notebooks and fix loops — and token-rate notes)

## Errors & Warnings          (only when there are findings)

The following tasks/cells have errors, warnings, or unresolved dependencies:

- **<task_b>** cell 12: <error message + 1-line note>
- **<task_b>** cell 19: <error message + 1-line note>

## Unmapped Databricks Job Triggers   (only when code referenced job_ids missing from db_to_aidp_job_map)

## Dependency Backfill        (only when the backfill scan ran)

- Scanned migrated notebooks: <int>
- Unmigrated %run targets found: <int>
- Migrated by backfill: <int>
- Failed: <int>   (failures listed as a code block)

## Required Packages          (only when pip installs happened during migration)
```

The `DataUnavail` column counts cells whose failure was classified as a genuine
data gap (missing table/data) — these are reported separately from `Failed` and
drive the `PARTIAL_DATA_UNAVAILABLE` status. The `Tokens` column is that task's
billed model tokens (uncached input + cache reads + cache writes + output); the engine labels token lines `Claude tokens` regardless of the configured provider.

---

## Status field — possible values

| Status | Meaning | Action |
|---|---|---|
| `PASS` | Every code cell executed cleanly (possibly after retry). | Done. Sign off. |
| `PASS_CONDITIONAL_EXIT_COVERAGE` | A conditional `notebook.exit()` FIRED at migration time; cells below it were converted code-only (not executed). Counts as success at every gate — a conditional exit is normal control flow. | Validate the un-taken branch by re-running the migration when the condition evaluates the other way. |
| `PASS_DATA_SUBSTITUTED` | Cells passed, but only after a documented data substitution. | Review the substitutions in the report before sign-off. |
| `PARTIAL` | Some cells exhausted all 5 fix attempts but the task as a whole didn't bail out. The final `.ipynb` includes both the fixed cells AND the cells preserved-as-is at the point of failure. | Route failing cells to [`aidp-fixup-cell`](../skills/aidp-fixup-cell/SKILL.md). |
| `PARTIAL_DATA_UNAVAILABLE` | Cells failed because source data/tables are genuinely missing (`DataUnavail` column non-zero) — a data gap, not a code failure. Runner still exits non-zero. | Fix the data (backfill / catalog migration / bucket mapping), then re-run the task. |
| `PARTIAL_PARAM_MISSING` | The task couldn't run because a required widget/parameter was missing (report carries a `missing_params` list). Downstream dependents cascade to `BLOCKED`. | Supply the parameter (`--param KEY=VALUE` / manifest parameters) and re-run. |
| `PARTIAL_STATE_LOST` | Kernel/session state was lost mid-task (e.g. connection died); remaining cells could not be runtime-validated. | Restart the cluster, re-run the task (`--start-task` / `--only-tasks`). |
| `FAIL` | Structural failure — e.g. a dep notebook couldn't be migrated, or the task notebook itself couldn't be loaded. Most cells didn't run. | Manual investigation: open dep / source notebook, fix, then [`aidp-resume-migration`](../skills/aidp-resume-migration/SKILL.md). |
| `ALREADY_MIGRATED` | A prior run already migrated this task and this run was invoked with `--skip-migrated`. The `.ipynb` already exists at the output path. | No action. To force re-migration, see [`aidp-resume-migration`](../skills/aidp-resume-migration/SKILL.md). |
| `BLOCKED` | A dependency task failed, so this task never ran (`blocked_by` names it). | Fix the dependency first. |
| `ACCEPTANCE_CONTRACT_VIOLATED` | Cells passed but the post-run acceptance contract didn't converge in time (demoted from PASS). | See [`aidp-acceptance-contract`](../skills/aidp-acceptance-contract/SKILL.md) §"VIOLATED". |
| `UNKNOWN` | Report row malformed. Treat as `FAIL`. | Read the raw `JOB_REPORT.md`; if truly garbled, the run probably crashed mid-write. |

---

## Cell-count columns

- **OK** — cells that executed on the cluster without verification failure on the first attempt.
- **Failed** — cells that failed verification on every retry attempt (up to 5, the default `MAX_FIX_ATTEMPTS`). These end up preserved-as-is in the final `.ipynb`.
- **DataUnavail** — cells classified as blocked by genuinely missing data (missing table / data gap) — counted separately from `Failed`.
- **Fixed** — cells that initially failed but passed after one or more retry attempts. The `.ipynb` has the LAST (passing) version.
- **Tokens** — the task's billed model tokens (uncached input + cache reads + cache writes + output); the engine labels token lines `Claude tokens` regardless of the configured provider.

Markdown cells are not counted.

---

## Parsing in Python (sample)

```python
import re
from pathlib import Path

REPORT_RE = re.compile(
    r"^\|\s*(?P<task>[^|]+?)\s*\|\s*"
    r"(?P<status>[A-Z][A-Z_]+)\s*\|\s*"          # PASS, PARTIAL_DATA_UNAVAILABLE, ...
    r"(?P<ok>-|\d+)\s*\|\s*(?P<fail>-|\d+)\s*\|\s*(?P<data_unavail>-|\d+)\s*\|\s*"
    r"(?P<fix>-|\d+)\s*\|\s*(?P<tokens>-|[\d,]+)\s*\|"
)

def parse_report(path: str):
    text = Path(path).read_text(encoding="utf-8")
    rows = []
    in_table = False
    for line in text.splitlines():
        if line.strip().startswith("## Task Results"):
            in_table = True
            continue
        if in_table and line.startswith("## "):  # next section
            break
        m = REPORT_RE.match(line)
        if m:
            d = m.groupdict()
            rows.append({
                "task": d["task"].strip(),
                "status": d["status"],
                "ok": int(d["ok"]) if d["ok"].isdigit() else 0,
                "fail": int(d["fail"]) if d["fail"].isdigit() else 0,
                "data_unavail": int(d["data_unavail"]) if d["data_unavail"].isdigit() else 0,
                "fix": int(d["fix"]) if d["fix"].isdigit() else 0,
                "tokens": int(d["tokens"].replace(",", "")) if d["tokens"] != "-" else 0,
            })
    return rows

if __name__ == "__main__":
    import sys
    for row in parse_report(sys.argv[1]):
        print(row)
```

Run on a report:

```bash
python3 parse_report.py reports/<MyJob>/JOB_REPORT.md
# → {'task': '<task_a>', 'status': 'PASS', 'ok': 27, 'fail': 0, 'data_unavail': 0, 'fix': 2, 'tokens': 41210}
# → ...
```

---

## Roll-up rules

When `/migration-status` summarizes:

```
overall PASS         = ALL tasks are PASS-family (PASS, PASS_CONDITIONAL_EXIT_COVERAGE,
                       PASS_DATA_SUBSTITUTED — review substitutions) or ALREADY_MIGRATED.
overall PARTIAL      = at least one task PARTIAL / PARTIAL_DATA_UNAVAILABLE /
                       PARTIAL_PARAM_MISSING / PARTIAL_STATE_LOST, no FAIL.
overall FAIL         = at least one task FAIL (or BLOCKED by a failed dependency).
overall CONTRACT_FAIL = some task ACCEPTANCE_CONTRACT_VIOLATED, regardless of cell counts.
```

The `RESULT:` line in `/tmp/migration.log` (live log) and the wrap-up message echo this overall verdict.

---

## When the file is missing or empty

Possible causes (rank by likelihood):
1. The run crashed before the writer cell — check `/tmp/migration.log` for the last entries.
2. The migrate-output path wasn't writable — check workspace permission for the OCI principal.
3. The migrator was killed (SIGKILL) — the writer cell catches SIGTERM but not SIGKILL.

In all three, the local mirror at `reports/<job-name>/JOB_REPORT.md` MAY exist even when the workspace copy doesn't — check both paths.

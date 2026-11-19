# `JOB_REPORT.md` — Format Reference

After every `aidp-migrate-job` run, the migrator emits a `JOB_REPORT.md` summarizing per-cell results. This document specifies the format so `/migration-status` (and any automation reading the report) can parse it reliably.

## Location

```
<output-base>/<job-name>/JOB_REPORT.md
```

Also mirrored locally to `reports/<job-name>/JOB_REPORT.md` after a successful run.

---

## File structure

```markdown
# Job Report: <job-name>

- Date: 2026-XX-XX HH:MM:SS UTC
- Tasks: <int>
- Parameters: <dict literal>

## Task Results

| Task | Status | OK | Failed | Fixed |
|------|--------|-----|--------|-------|
| <task_a> | PASS              | 27 | 0  | 2  |
| <task_b> | PARTIAL           | 18 | 2  | 7  |
| <task_c> | FAIL              | 0  | -  | -  |
| <task_d> | ALREADY_MIGRATED  | -  | -  | -  |

## Dependency Backfill

- Scanned migrated notebooks: <int>
- Unmigrated %run targets found: <int>
- Migrated by backfill: <int>
- Failed: <int>

(if any failed, list them as a code block)

## Errors & Warnings

The following cells failed with actionable notes:

- **<task_b>** cell 12: <error message + 1-line note>
- **<task_b>** cell 19: <error message + 1-line note>

## Acceptance contracts  (optional section)

| task_key | status | windows_observed | converged_at |
|---|---|---|---|
| <task_a> | PASS | 3 consecutive zeros | 2026-XX-XX HH:MM:SS |

## Runtime metadata

- Cluster: <CLUSTER_ID>
- Output base: <output-base>
- Total wall clock: <int>s
- Claude tokens (approx): in <int>, out <int>
```

---

## Status field — possible values

| Status | Meaning | Action |
|---|---|---|
| `PASS` | Every code cell executed cleanly (possibly after retry). | Done. Sign off. |
| `PARTIAL` | Some cells exhausted all 10 fix attempts but the task as a whole didn't bail out. The final `.ipynb` includes both the fixed cells AND the cells preserved-as-is at the point of failure. | Route failing cells to [`aidp-fixup-cell`](../skills/aidp-fixup-cell/SKILL.md). |
| `FAIL` | Structural failure — e.g. a dep notebook couldn't be migrated, or the task notebook itself couldn't be loaded. Most cells didn't run. | Manual investigation: open dep / source notebook, fix, then [`aidp-resume-migration`](../skills/aidp-resume-migration/SKILL.md). |
| `ALREADY_MIGRATED` | A prior run already migrated this task. Migrator skipped it. The `.ipynb` already exists at the output path. | No action. If you want to force re-migration, see [`aidp-resume-migration`](../skills/aidp-resume-migration/SKILL.md) §"force re-migration". |
| `ACCEPTANCE_CONTRACT_VIOLATED` | Cells passed but the post-run acceptance contract didn't converge in time. | See [`aidp-acceptance-contract`](../skills/aidp-acceptance-contract/SKILL.md) §"VIOLATED". |
| `UNKNOWN` | Report row malformed. Treat as `FAIL`. | Read the raw `JOB_REPORT.md`; if truly garbled, the run probably crashed mid-write. |

---

## Cell-count columns

- **OK** — cells that executed on the cluster without verification failure on the first attempt.
- **Failed** — cells that failed verification on every retry attempt (up to 10). These end up preserved-as-is in the final `.ipynb`.
- **Fixed** — cells that initially failed but passed after one or more retry attempts. The `.ipynb` has the LAST (passing) version.

`OK + Failed + Fixed = total code cells in the task`. Markdown cells are not counted.

---

## Parsing in Python (sample)

```python
import re
from pathlib import Path

REPORT_RE = re.compile(
    r"^\|\s*(?P<task>[^|]+?)\s*\|\s*"
    r"(?P<status>PASS|PARTIAL|FAIL|ALREADY_MIGRATED|ACCEPTANCE_CONTRACT_VIOLATED|UNKNOWN)\s*\|\s*"
    r"(?P<ok>-|\d+)\s*\|\s*(?P<fail>-|\d+)\s*\|\s*(?P<fix>-|\d+)\s*\|"
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
                "fix": int(d["fix"]) if d["fix"].isdigit() else 0,
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
# → {'task': '<task_a>', 'status': 'PASS', 'ok': 27, 'fail': 0, 'fix': 2}
# → ...
```

---

## Roll-up rules

When `/migration-status` summarizes:

```
overall PASS         = ALL tasks are PASS or ALREADY_MIGRATED.
overall PARTIAL      = at least one task PARTIAL, no FAIL.
overall FAIL         = at least one task FAIL.
overall CONTRACT_FAIL = some task ACCEPTANCE_CONTRACT_VIOLATED, regardless of cell counts.
```

The `RESULT:` line in `/tmp/migration.log` (live log) and the wrap-up message echo this overall verdict.

---

## When the file is missing or empty

Possible causes (rank by likelihood):
1. The run crashed before the writer cell — check `/tmp/migration.log` for the last entries.
2. The output-base path wasn't writable — check workspace permission for the OCI principal.
3. The migrator was killed (SIGKILL) — the writer cell catches SIGTERM but not SIGKILL.

In all three, the local mirror at `reports/<job-name>/JOB_REPORT.md` MAY exist even when the workspace copy doesn't — check both paths.

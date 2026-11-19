---
description: Inventory, complexity, and compatibility report over an Informatica export. Free, no LLM call, no cluster. Run this before /infa-migrate.
---

# `/infa-analyze` — inventory before you migrate

Thin wrapper over [`infa-analyze`](../skills/infa-analyze/SKILL.md).

## Workflow

1. Resolve the export path from `$ARGUMENTS`, or ask for it.
2. Run:
   ```bash
   PYTHONPATH=engine python3 -m infa2aidp.cli analyze \
     -i <export-path> -o ./analysis_report --format all
   ```
3. Summarize: mapping/workflow/session/transformation counts, and
   compatibility error/issue counts.
4. If the compatibility report shows any reference to a workflow, worklet,
   taskflow, Mapping Task, or parameter set, say so explicitly — none of
   those have migration support in this tool (see
   [`infa-migrator-overview`](../skills/infa-migrator-overview/SKILL.md)),
   so the customer needs to know that gap exists in *this* export, not just
   in the abstract.

## Args

`$ARGUMENTS`

Treated as the input path if it looks like one; otherwise ask.

## Output template

```
== Analysis for <export> ==

Mappings: N   Workflows: N   Sessions: N   Transformations: N
Compatibility: E error(s), O other issue(s)

Reports written to ./analysis_report/

Notable gaps in this export:
  - <e.g. "2 taskflows referenced — no orchestration support in this tool">
```

## After this

`/infa-migrate` once the inventory and compatibility findings are
understood and the user is ready to convert.

---
description: Classify a migration's output as PASS / REVIEW / SKIP / FAIL.
---

```bash
fabric-aidp verify ./migrated
fabric-aidp verify ./migrated --filter dataflow    # one slice
```

Report the four counts, then state plainly what PASS means here: translated, with no
known issue detected — **not execution-verified**. Nothing parses or runs the
generated artifacts, so a construct no rule covers is reported clean. A low REVIEW
count is not by itself evidence of a clean migration.

`--filter` takes one of `notebook`, `warehouse`, `lakehouse`, `pipeline`,
`semanticmodel`, `dataflow`. It narrows the *report*, not the verdict. A FAIL is
never hidden by a filter: a failing row from another slice is still counted, still
printed, and carries the note `shown despite --filter <slice>`. Neither is a
refusal that belongs to no slice — an `unreadable.` or `unsupported.` row, for an
item no scanner could claim. It could otherwise be seen in no slice at all, so it
too is counted and printed under every filter. That is the tool
working — a failure a filter could hide is how a broken migration goes green. So
do not tell the user a filtered run was clean; tell them what the whole run said.

Exit codes: 0 when FAIL is 0, 1 when it is not — filtered or not — and 2 if the
report cannot be read.

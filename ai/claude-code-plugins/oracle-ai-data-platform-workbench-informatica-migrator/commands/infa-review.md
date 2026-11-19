---
description: Generate and walk the human-review file for LOW/MEDIUM/MANUAL-confidence conversions and REVIEW REQUIRED markers from a migration run.
---

# `/infa-review` — the human gate

Thin wrapper over [`infa-review`](../skills/infa-review/SKILL.md).

## Workflow

1. **Generate the review file:**
   ```bash
   PYTHONPATH=engine python3 -m infa2aidp.cli review generate \
     -i <export-path> -o ./review/review_items.json
   ```
2. **Report the count**, then walk the user through items one at a time —
   do not bulk-approve. For each item involving a construct with no
   migration support (workflow/worklet/taskflow/Mapping Task/parameter-set
   reference, or an unresolved source/target tier), say so plainly: editing
   the generated cell can't fix a missing orchestration layer.
3. **Record decisions** (`approved` / `edited` / `rejected`) back into the
   review file.
4. **Import the decisions:**
   ```bash
   PYTHONPATH=engine python3 -m infa2aidp.cli review import \
     -i ./review/review_items.json -o ./review
   ```
5. **Report the final tally** — approved/edited/rejected — and flag every
   `rejected` item as one that will not reconcile.

## Args

`$ARGUMENTS`

If it names an export path, use it for `generate`; if it names a review
file already in progress, skip straight to walking it / importing it.

## Output template

```
Review file: ./review/review_items.json (N item(s) to review)

[1/N] <mapping>.<transformation> — confidence: LOW
  Reason: <why it's flagged>
  Suggested decision: approve / edit / reject?

...

Approved: X  Edited: Y  Rejected: Z
```

## After this

Every `edited` decision needs its fix applied to the actual `.ipynb` (this
command records the decision, it doesn't regenerate the notebook). Then
`/infa-reconcile`.

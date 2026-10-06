---
name: infa-review
description: Human approval gate over LOW/MEDIUM/MANUAL-confidence conversions via `infa2aidp cli review generate|import`. Use after infa-migrate-mapping, before trusting any generated notebook, to surface every low-confidence conversion and REVIEW REQUIRED marker for a human decision (approve/edit/reject) rather than letting them ship silently.
---

# `infa-review` — the human gate

Runs `infa2aidp cli review`. Two actions: `generate` (produce a review file
from an export) and `import` (record human decisions back into a report).
This is the mechanism that keeps low-confidence conversions from shipping
unnoticed.

## When to use

- Immediately after [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md).
- The user asks "what needs a human look", "show me the low-confidence
  items", "what got flagged for review".

**Do NOT treat a clean `infa-migrate-mapping` exit code as "nothing to
review".** The migrate command's own exit code reflects whether it *ran*
without crashing, not whether every mapping converted with confidence.
`infa-review generate` re-parses the same input and re-runs the conversion
pipeline to find every item scored LOW, MEDIUM, or MANUAL — this is the
step that actually reveals that count.

## Step 1: generate the review file

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli review generate \
  -i <path-to-export> \
  -o ./review/review_items.json
```

Console: `Review file: <path> (<N> item(s) to review)`. `N` is every
transformation-level conversion scored LOW, MEDIUM, or MANUAL confidence —
this is a superset of the `# REVIEW REQUIRED` markers baked into generated
notebooks, since it also catches lower-confidence items that still produced
runnable (if uncertain) code.

## Step 2: a human works the review file

The review file is JSON. Each item needs a `decision`: `approved`,
`edited` (with the corrected content), or `rejected`. Walk the user through
it item by item rather than bulk-approving — that defeats the point of the
gate. Cross-reference anything involving an unsupported construct
(taskflow/Mapping Task/parameter-set/worklet reference, or a source/target
tier the generator can't resolve — see
[`infa-migrator-overview`](../infa-migrator-overview/SKILL.md)) against
the known-gap list before asking the user to decide; some of these can't be
fixed by editing the generated cell, only by handling the orchestration
piece manually outside this tool.

## Step 3: import the reviewed file

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli review import \
  -i ./review/review_items.json \
  -o ./review
```

Console: `Approved: X  Edited: Y  Rejected: Z`. Writes `review_report.md`
under the output directory.

## Flags

| Flag | Notes |
|---|---|
| `action` | positional, `generate` or `import` |
| `-i, --input` | for `generate`: the export path; for `import`: the reviewed review-file path |
| `-o, --output` | for `generate`: the review-file path to write; for `import`: a directory (`review_report.md` is written inside it), default `./review` |

## What "review" does not cover

- It does not re-run [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md)
  or regenerate the notebook from an `edited` decision automatically — an
  edit recorded here needs to be applied to the actual `.ipynb` by hand or
  by re-running migrate with adjustments (`--custom-rules`, `--params`).
- It has no concept of taskflow/Mapping Task/parameter-set/worklet review,
  because the parser produces nothing for those constructs to review in the
  first place — they simply don't appear.

## After this

- Every `rejected` item is a mapping the user has decided not to trust as
  generated — track it separately, it will not reconcile.
- Once approved, proceed to
  [`infa-reconcile`](../infa-reconcile/SKILL.md) to prove correctness
  against real data, then [`infa-deploy`](../infa-deploy/SKILL.md).

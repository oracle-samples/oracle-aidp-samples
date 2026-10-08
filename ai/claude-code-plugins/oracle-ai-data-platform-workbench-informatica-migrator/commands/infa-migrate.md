---
description: Guided mapping migration. Confirms an analyze pass exists, runs `infa2aidp cli migrate` with the right flags for the user's target, and points to review/reconcile next.
---

# `/infa-migrate` — guided mapping migration

Thin wrapper over [`infa-migrate-mapping`](../skills/infa-migrate-mapping/SKILL.md).

## Workflow

1. **Confirm an analyze pass exists.** If the user hasn't run
   [`infa-analyze`](../skills/infa-analyze/SKILL.md) against this export
   yet, run it first — it's free and surfaces incompatible constructs
   before spending LLM time on them.
2. **Confirm the target catalog type.** Default is `delta` (managed Delta).
   If the user's target is ADW/ALH/ATP, use `--target-catalog-type adw` and
   warn them this path is unverified against a live ADW (derived from the
   connector reference, never run — see
   [`infa-migrator-overview`](../skills/infa-migrator-overview/SKILL.md)).
3. **Confirm LLM usage.** Ask whether to use `--use-llm` (needs
   `ANTHROPIC_API_KEY`) or stay rule-based. Rule-based is free and
   deterministic; LLM-assisted handles more transformation types but costs
   tokens and time.
4. **Run it:**
   ```bash
   PYTHONPATH=engine python3 -m infa2aidp.cli migrate \
     -i <export-path> -o <output-dir> \
     [--use-llm] [--target-catalog-type adw] --comparison
   ```
5. **Report the summary** — notebook count, auto-convertible %, and (with
   `--comparison`) how many mappings had fidelity gaps.

## Args

`$ARGUMENTS`

If it names an input path, use it directly. If it also contains
`--target-catalog-type adw`, `--use-llm`, `--workers N`, or similar flags
recognized by `infa2aidp cli migrate --help`, pass them through as given.
Otherwise ask for the export path.

## When to STOP and ask first

- No prior `infa-analyze` run and the export is large/unfamiliar — confirm
  the user wants to skip straight to migration anyway.
- `--target-catalog-type adw` requested — confirm the user understands this
  path has never been run against a live ADW.
- `--use-llm` requested but `ANTHROPIC_API_KEY` is unset.

## After this

`/infa-review` for anything flagged LOW/MEDIUM/MANUAL confidence or marked
`REVIEW REQUIRED`, then `/infa-reconcile` once the target is populated.

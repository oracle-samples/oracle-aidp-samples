---
name: infa-lineage
description: Field-level data lineage report per mapping via `infa2aidp cli lineage`. Use when the user asks "where does this field come from" or wants a lineage artifact for documentation/audit purposes. Operates on the Informatica export directly (source mappings), not on the generated PySpark notebooks.
---

# `infa-lineage` — field-level lineage

Runs `infa2aidp cli lineage`. Produces a per-mapping field-level lineage
report, tracing each target field back through the transformation chain to
its source field(s).

## When to use

- The user asks "where does this column come from", "show me the lineage",
  or needs a lineage artifact for a compliance/audit deliverable.
- After [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) (unless
  `--skip-lineage` was passed, in which case this already ran).

## Canonical invocation

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli lineage \
  -i <path-to-export> \
  -o ./lineage
```

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-i, --input` | required | Export file or directory, same input contract as `analyze`/`migrate` |
| `-o, --output` | `./lineage` | One report file per mapping, named after the mapping |

## Output

Console: `Lineage generated for <N> mapping(s) -> <output dir>`.

## Scope

Traces lineage through the mapping's transformation graph as parsed from
the export — it has no visibility into anything a taskflow, worklet, or
Mapping Task might add (parameter substitution, session-level overrides),
because none of those constructs are parsed by this tool. Lineage for a
field whose value ultimately depends on a `$$`-parameter resolved outside
the mapping is bounded by what's visible in the mapping definition itself.

## After this

Useful as a standalone deliverable, or as a cross-check when
[`infa-review`](../infa-review/SKILL.md) surfaces an ambiguous
transformation — lineage shows exactly which upstream fields feed it.

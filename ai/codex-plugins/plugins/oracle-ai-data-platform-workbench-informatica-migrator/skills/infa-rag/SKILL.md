---
name: infa-rag
description: Manage the learned conversion-pattern store via `infa2aidp cli rag` (stats/list/export/import/clear). Use when the user asks what the tool has learned, wants to back up or move approved patterns between environments, or wants to reset the store. This is local pattern-reuse bookkeeping, not a knowledge base query tool.
---

# `infa-rag` — the learned-pattern store

Runs `infa2aidp cli rag`. Manages a local store of transformation patterns
the LLM-assisted conversion path (`migrate --use-llm`) has produced and, once
approved via [`infa-review`](../infa-review/SKILL.md), can reuse on future
similar transformations.

## When to use

- The user asks "what has the tool learned", "show me the pattern store",
  "how many approved patterns do we have".
- Moving approved patterns between environments (export on one, import on
  another).
- Resetting the store, e.g. before a clean benchmark run.

## Actions

```bash
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli rag stats
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli rag list --approved-only
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli rag export -o rag_export.json
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli rag import -i rag_export.json
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli rag clear
```

| Action | Flags used | Output |
|---|---|---|
| `stats` | — | `Entries: N  Approved: M  Retrievals: K` |
| `list` | `--approved-only` (optional), `-i`/`-o` unused | Up to 20 entries: `[id] <transformation_type> -- approved/pending -- used Nx` |
| `export` | `-o` (default `rag_export.json`) | `Exported to <path>` |
| `import` | `-i` (required for this action) | `Imported from <path>; total entries now N` |
| `clear` | — | `Cleared <N> entries` — **irreversible**, confirm with the user first |

## What "approved" means here

An entry becomes approved through the [`infa-review`](../infa-review/SKILL.md)
flow, not directly through `rag`. This command only reads/writes/manages
the store; it does not itself judge quality.

## When to use this vs. just re-running migrate

- Only relevant to the `--use-llm` path — the rule-based converters never
  touch this store.
- `clear` before a benchmark so results aren't skewed by prior-run pattern
  reuse; leave it alone in normal operation so accuracy improves run over
  run.

## After this

No downstream step — this is bookkeeping, consulted implicitly by future
`migrate --use-llm` runs.

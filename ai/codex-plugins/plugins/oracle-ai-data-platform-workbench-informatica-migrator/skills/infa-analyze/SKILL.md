---
name: infa-analyze
description: Inventory, complexity, and compatibility report over an Informatica export — no cluster, no LLM call, no cost. Use before migrating anything, to know what's in an export, how complex it is, and which pieces this tool cannot convert. Accepts a directory or file of PowerCenter XML and/or IDMC/IICS JSON, mixed.
---

# `infa-analyze` — inventory before you migrate

Read-only over the export. Runs `infa2aidp cli analyze`. Always run this
before [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) — it is
free (no LLM call) and tells you what you're about to spend time and tokens
on.

## When to use

- The user has an export (from [`infa-discover`](../infa-discover/SKILL.md)
  or handed to you directly) and wants to know what's in it before
  committing to a migration.
- The user asks "what would migrate", "how complex is this", "is this
  compatible with AIDP".

## Canonical invocation

```bash
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli analyze \
  -i <path-to-export-dir-or-file> \
  -o ./analysis_report \
  --format all
```

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-i, --input` | required | A single `.xml`/`.json` file, or a directory — scanned recursively for both `.xml` (PowerCenter) and `.json` (IDMC/IICS). Mixed directories are fine. |
| `-o, --output` | `./analysis_report` | |
| `--format` | `all` | `markdown`, `json`, `csv`, or `all` |

## What it produces

Console: `Mappings: N  Workflows: N  Sessions: N  Transformations: N` and
`Compatibility: <errors> error(s), <other> other issue(s)`.

Files under `<output>/`: the inventory and compatibility report in
whichever format(s) were requested.

## Reading the compatibility report

Every unsupported or partially-supported construct surfaces here as a
finding, categorized by severity (`ERROR` and others). This is where the
tool's real limitations become visible for *this specific export* —
including, if present, any workflow/worklet/taskflow, Mapping Task, or
parameter-set references, since none of those have any migration support
today (see [`infa-migrator-overview`](../infa-migrator-overview/SKILL.md)'s
gap list). `infa-analyze` reports these; it does not attempt to fix or work
around them.

## Behavior worth knowing

- A directory with a mix of PowerCenter XML and IDMC JSON is analyzed
  together — both formats detected and merged into one report.
- If one file in a directory fails to parse (e.g. an unsupported
  PowerCenter version), `infa-analyze` skips it, reports it by name, and
  still analyzes everything else. It only exits non-zero if **every** input
  in the batch failed to parse.
- No network access, no LLM provider key, no cluster — this command is
  local-only and safe to run repeatedly.

## After this

- Any `ERROR`-severity compatibility issue is a mapping that will not
  convert cleanly — decide with the user whether to exclude it, migrate it
  manually, or accept a `REVIEW REQUIRED` placeholder.
- Proceed to [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md) once
  the inventory and compatibility findings are understood.

---
name: infa-migrate-mapping
description: Convert Informatica mappings (PowerCenter XML or IDMC/IICS JSON) into PySpark notebooks for AIDP via `infa2aidp cli migrate`. Covers --use-llm/--agentic, --comparison fidelity reports, --workers batch mode, and --target-catalog-type delta|adw. This is the main run — use once infa-analyze has been read and the user is ready to actually convert. Does not orchestrate taskflows/Mapping Tasks/parameter sets (unsupported) and does not execute anything on a live cluster (that's a separate, unbuilt capability — see infa-migrator-overview).
---

# `infa-migrate-mapping` — convert mappings to notebooks

Runs `infa2aidp cli migrate`. This is the tool's core conversion step:
Informatica mapping → PySpark notebook, per mapping.

## When to use

- [`infa-analyze`](../infa-analyze/SKILL.md) has already run and the user is
  ready to convert.
- The user says "migrate this", "convert to PySpark", "generate the
  notebooks".

**Do NOT invoke without** having read the `infa-analyze` compatibility
report first — an export with unresolved `ERROR`-severity issues will
still run, but every affected mapping comes out as a `REVIEW REQUIRED`
placeholder rather than working code, and the user should know that going
in.

## Canonical invocation — rule-based (default, no LLM cost)

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli migrate \
  -i <path-to-export> \
  -o <output-dir> \
  --comparison
```

## With LLM-assisted conversion

```bash
export ANTHROPIC_API_KEY=sk-ant-...
PYTHONPATH=engine python3 -m infa2aidp.cli migrate \
  -i <path-to-export> \
  -o <output-dir> \
  --use-llm \
  --workers 5 \
  --comparison
```

`--agentic` selects the spec → generate → validate → fix pipeline as the
conversion path **when the LLM-first generator is not in use** (no
`--use-llm`, or `ANTHROPIC_API_KEY` unset). With a working `--use-llm` it is
a no-op (`migrator.py`: `if agentic and not llm_gen`). It is not a
"more thorough" add-on to `--use-llm`.

## Targeting an ADW (external catalog) instead of Delta

```bash
PYTHONPATH=engine python3 -m infa2aidp.cli migrate \
  -i <path-to-export> -o <output-dir> \
  --target-catalog-type adw
```

**Never inferred — always explicit.** Default is `delta` (managed Delta,
`DeltaTable.forName()` merges). `adw` switches every write cell to the
JDBC-based `AdwWriteStrategy`: overwrite-only writes, with upserts staged
into a temp table and merged via a real Oracle `MERGE INTO` run from the
driver through `python-oracledb`. **This path is unverified against a live
ADW** — derived from the AIDP connector reference (`aidp-alh`), never run.
Every code path it emits carries an inline comment saying so. Treat a
`--target-catalog-type adw` notebook as needing a first live run before
trusting its write semantics, and tell the user this explicitly.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-i, --input` | required | Export file or directory (mixed PowerCenter XML / IDMC JSON OK) |
| `-o, --output` | required | |
| `--use-llm` | off | Claude-assisted conversion for transformations the rule-based path can't handle |
| `--agentic` | off | Spec/generate/validate/fix pipeline, used only when the LLM-first path is not active (see above) |
| `--custom-rules` | none | Path to a custom rules file |
| `--params` | none | Path to a parameter file (Informatica `$$`-style mapping parameters *at the mapping level* — not the same thing as an IDMC parameter *set*, which has no support at all) |
| `--workers N` | `1` | Parallel LLM workers — batch mode when `N > 1` |
| `--comparison` | off | Emit side-by-side Informatica-vs-PySpark fidelity reports; recommended every time so gaps are visible immediately |
| `--skip-lineage` | off | Skip generating the lineage report for this run |
| `--skip-optimize` | off | Skip generating optimization suggestions for this run |
| `--target-catalog-type` | `delta` (or `TARGET_CATALOG_TYPE` env, default `delta`) | `delta` or `adw` — see above |

## Output layout

```
<output-dir>/
  <folder>/nb_<mapping>.ipynb       ← one notebook per mapping (or "Migrated/" if unfoldered)
  workflows/<workflow>.json         ← one AIDP job definition per PowerCenter workflow
  workflows/<workflow>.review.md    ← only when the workflow did not translate completely
  reports/fidelity_report.md        ← source-fidelity check (always)
  reports/confidence_report.md      ← confidence scores (always)
  reports/parameter_report.md       ← only with --params
  comparisons/<mapping>_comparison.{md,html}   ← only with --comparison
  lineage/<mapping>*                ← unless --skip-lineage
```

Console: `Migration complete: N notebook(s), M workflow(s) -> <output-dir>`,
plus `Auto-convertible: X%`, `Source fidelity: A/B mapping(s) checked ...
have gaps -> <report path>` (always), and an `Orchestration:` line when any
workflow has a `.review.md`. Every notebook's first (markdown) cell lists
**Parser notes** -- reusable transformations resolved from the folder,
mapplets expanded inline -- so the reader can see what the parser did to
the export.

## What every generated notebook guarantees, and what it does not

Guaranteed (checked by the test suite and `demo.sh`'s verify step):

- Catalog-addressed reads/writes only — `spark.table("catalog.schema.table")`
  / `saveAsTable(...)`. No JDBC URL, no credential literal, ever.
- Syntactically valid Python, with no name read before it's ever assigned
  (a guaranteed `NameError` at runtime that plain syntax checking misses).
- Real transformation logic, not just boilerplate — a notebook with no
  `withColumn`/`.filter`/`.join`/`.groupBy`/`.agg` marker is treated as
  hollow and is a bug, not an accepted output.
- Every unresolved Router-group condition, unsatisfiable upsert (no key
  columns), SQL override or User Defined Join the two translation gates
  reject (Oracle-only SQL, or a table that is not a source in the mapping),
  outer Joiner with no resolvable master side, dynamic lookup cache, or other
  construct the generator can't safely translate becomes a
  `# REVIEW REQUIRED: ...` comment — never a silent guess.

Not guaranteed:

- IDMC *orchestration* (taskflow, Mapping Task, parameter set) — none of
  that is parsed. A PowerCenter workflow does become `workflows/<name>.json`
  with the gaps listed in its `.review.md` (non-session tasks dropped,
  non-SUCCEEDED link conditions not applied; worklets are expanded).
- That the notebook has ever run. `infa-migrate-mapping` never touches a
  live cluster. Use [`infa-reconcile`](../infa-reconcile/SKILL.md) after a
  manual or `infa-deploy`-triggered run to prove correctness.
- Full fidelity for a source/target read/write whose Informatica export
  lacks real `<SOURCE>`/`<TARGET>`/`<INSTANCE>` metadata — the generator
  falls back to a placeholder read (`spark.sql("SELECT 1 AS placeholder")`)
  and skips the write entirely rather than guess. `infa-analyze`'s
  compatibility report should have already flagged this; `--comparison`
  surfaces it per-mapping too.

## Cost guidance

- Rule-based (`demo.sh`'s path): free, instant, no network.
- `--use-llm` with `--workers 1`: sequential, one Claude call per mapping
  needing LLM assistance.
- `--use-llm --workers N>1`: batch mode, `N` concurrent LLM calls — faster,
  proportionally more expensive per unit time, same total token cost.

## When it goes wrong

| Symptom | Fix |
|---|---|
| Exit 1, "Migration failed" with an LLM error | Check `ANTHROPIC_API_KEY`. If it errored rather than falling back, that's the tool's default (`RULE_BASED_FALLBACK=false`) — see `env.template`. Set `RULE_BASED_FALLBACK=true` only if a TODO-riddled fallback notebook is acceptable to the user. |
| Notebook full of `# REVIEW REQUIRED` | Expected for unsupported constructs — route to [`infa-review`](../infa-review/SKILL.md). Not a bug. |
| `--target-catalog-type adw` notebook fails on first live run | Expected risk — this path was never run against live ADW before. Capture the failure and treat it as a defect report, not a one-off fluke. |

## After this

- [`infa-review`](../infa-review/SKILL.md) — resolve every LOW/MEDIUM/MANUAL
  item and every `REVIEW REQUIRED` marker.
- [`infa-optimize`](../infa-optimize/SKILL.md) and
  [`infa-lineage`](../infa-lineage/SKILL.md) — unless `--skip-optimize` /
  `--skip-lineage` were passed, these already ran; otherwise run them now.
- [`infa-deploy`](../infa-deploy/SKILL.md) — once the notebooks are trusted.

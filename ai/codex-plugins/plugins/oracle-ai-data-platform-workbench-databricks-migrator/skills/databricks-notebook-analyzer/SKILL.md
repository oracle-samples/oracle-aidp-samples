---
name: databricks-notebook-analyzer
description: Read a single Databricks notebook (.ipynb / .py / .scala source format) and produce a migration-readiness report — what it does, what it depends on, what risks the migrator will hit, which gotchas (from references/gotchas.md) will apply. Run before manually authoring an entry in a migration manifest, OR as a one-off "should we migrate this?" assessment. Read-only — does not modify the notebook and does not invoke the migrator.
---

# `databricks-notebook-analyzer` — single-notebook readiness report

Reads a single Databricks notebook and produces a migration-readiness report. DO NOT modify the notebook. DO NOT call the migrator. DO NOT execute the notebook.

## When to use

- User points at a single Databricks notebook and asks "what does this do" or "can we migrate this".
- BEFORE manually authoring an entry in a migration manifest.
- BEFORE running [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) on a single notebook — sanity-check the risks first.

## Inputs the calling skill / user provides

- A path to the notebook (`.ipynb`, `.py`, or `.scala` source format).
- (Optional) the cluster ID it currently runs on, for context about Spark version / installed libs.

## What you produce

A markdown report with these exact sections:

```markdown
# Migration analysis: <notebook-name>

## What it does
<1–3 sentence summary of the pipeline's purpose, inferred from cell content + comments>

## Inputs (reads)
| Path / Table | Format | Detected via |
|---|---|---|
| <s3://... or schema.table> | parquet / delta / table | cell N: `spark.read.table(...)` |

## Outputs (writes)
| Target | Mode | Detected via |
|---|---|---|
| <schema.table> | overwrite / append | cell N: `.saveAsTable(...)` |

## Dependencies
- `%run`: <list>
- `dbutils.notebook.run`: <list>
- 3rd-party libs (imports): <list>

## Migration risks (Databricks-isms in this notebook)

Cross-reference each finding to a gotcha number in `references/gotchas.md`.

| Cell | Construct | Risk | Gotcha # |
|---|---|---|---|
| 5 | `from pyspark.sql.functions import *` | shadows `builtins.sum` | #3 |
| 12 | `entity_decrypt(...)` | AWS Secrets Manager — no OCI equiv | #1 |
| 18 | `dbutils.notebook.run("./helpers", ...)` | path with trailing `./` | #7 |

## Manual-conversion recommendations

For each risk, name the cell + the specific fix:

- Cell 5: change `from pyspark.sql.functions import *` to `import pyspark.sql.functions as F`.
- Cell 12: replace `entity_decrypt(<arg>)` with a passthrough or a sandbox stub — migrator does this if `--catalog-manifest` includes the UDF.
- ...

## Pass-2 expected behavior

Rough prediction of what `aidp-migrate-job` will do:
- N cells expected to pass first-try
- M cells will likely need 1 retry (specific cells: ...)
- K cells likely to be marked PARTIAL even after 10 attempts — these need [`aidp-fixup-cell`](../aidp-fixup-cell/SKILL.md) or manual

## Recommendation

PROCEED / PROCEED WITH CAUTION (list the cautions) / REWRITE FIRST (list the prerequisites)
```

## Method

1. **Read** the notebook source. For `.ipynb`, parse the JSON and walk `cells[].source` joined.
2. **Scan** every code cell for:
   - `spark.read.*` calls
   - `.saveAsTable(...)`, `.write.*` calls
   - `%run` lines
   - `dbutils.*` references
   - Imports
   - Magic line cells (`%scala`, `%sql`, `%sh`)
3. **Cross-reference** each finding to `references/gotchas.md`:
   ```
   grep -n "Gotcha #" references/gotchas.md
   ```
   Match the construct to its gotcha entry.
4. **Estimate** Pass-2 behavior by counting cells with no risk markers (likely first-try OK) vs cells with risk markers (likely needs retry).

## Boundaries

- Do NOT execute the notebook.
- Do NOT modify the notebook.
- Do NOT propose changes to the migrator itself.
- Do NOT speculate beyond what the source code shows. If a cell is unclear, mark it `UNKNOWN` rather than guessing.

## When to escalate back to the user

- The notebook references a `dbutils.notebook.run` with a dynamic path (built at runtime). Mark as a blocker — the migrator's DAG builder can't resolve it.
- The notebook uses a connector library that you can't find on the cluster (e.g. a Cassandra driver). The migrator will fail at the import; the user needs to install it OR rewrite to a different sink.
- The notebook references a UDF defined elsewhere whose source you can't locate. Surface the UDF name + ask the user where it's defined.

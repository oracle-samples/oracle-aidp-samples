"""dbx_export — Databricks Job → AIDP Workflow EXPORTER.

This package COPIES Databricks artifacts (notebooks, jars, init scripts,
workflow + cluster specs) to an AIDP workspace. It does NOT perform the
actual migration (cell-by-cell API rewrites, S3→OCI translation, etc.) —
that's a separate step run on the AIDP side later.

Think of this as `dbx → aidp` *replication*. Exported notebooks land at
`<export_output>/notebooks/[GIT/]<dbx-relative-path>.ipynb`; exported workflow
+ cluster definitions get the `<prefix>_` namespace on AIDP.
"""

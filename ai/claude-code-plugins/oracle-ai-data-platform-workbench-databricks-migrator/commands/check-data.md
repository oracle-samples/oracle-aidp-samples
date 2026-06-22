---
description: Pre-migration data-availability scan. Reads a manifest, probes every spark.read.* / saveAsTable target on the AIDP cluster, reports OK / MISSING / EMPTY.
---

# `/check-data` — pre-migration scan

Light wrapper over [`aidp-check-data`](../skills/aidp-check-data/SKILL.md). Use before any [`aidp-migrate-job`](../skills/aidp-migrate-job/SKILL.md) run.

## Workflow

1. Find an existing manifest at `reports/<job>_manifest.json`. If none, ask the user to build one via [`/migrate-job`](./migrate-job.md) Phase 1, OR run [`aidp-build-dag`](../skills/aidp-build-dag/SKILL.md).
2. Invoke `scripts/check_data_availability.py` (or `_for_workflow.py` if the manifest came from a Databricks Job ID).
3. Output a 3-section summary: TABLES (OK / MISSING / EMPTY), PATHS (same), and a remediation tip per category.

## Args

$ARGUMENTS

If `$ARGUMENTS` names a manifest file or job name, use it; else infer from the most recent `reports/<job>_manifest.json`.

## Output template

```
== Data availability for <MyJob> ==

TABLES — 23 total
  OK     21
  MISSING  1   → '<catalog>.<schema>.<table_b>' — run /migrate-catalog or create manually
  EMPTY    1   → '<catalog>.<schema>.<table_c>' (0 rows; data backfill needed)

PATHS — 8 total
  OK      7
  MISSING 1   → 'oci://<bucket>@<ns>/path' — confirm bucket-mapping config

VERDICT: 2 issues. Safe to proceed? (y / N / fix-first)
```

## When to STOP and remediate first

If MISSING tables > 0, do NOT proceed to [`/migrate-job`](./migrate-job.md) without resolving. Options surfaced to the user:
- "These schemas missing — run /migrate-catalog first?"
- "These S3 buckets unmapped — open aidp-bucket-mapping skill?"
- "These specific tables out of scope — exclude in manifest?"
- "Proceed anyway, accept Pass-2 failures at these reads?"

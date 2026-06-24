---
name: check-data-flow
description: Pre-migration data-availability scan. Reads a migration manifest, probes every spark.read.* / saveAsTable target on the AIDP cluster, reports OK / MISSING / EMPTY with remediation tips per category. Use before any aidp-migrate-job run, or whenever the user asks "is the data ready?", "pre-migration check", "what's missing on AIDP".
---

# `check-data-flow` — pre-migration scan

Light wrapper over [`aidp-check-data`](../aidp-check-data/SKILL.md). Use before any [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) run.

## When to use

- BEFORE invoking [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) (mandatory gate — Pass-2 cost is high; fail fast on missing inputs).
- After [`migrate-catalog-flow`](../migrate-catalog-flow/SKILL.md) — verify schemas + tables actually resolve on AIDP.
- After [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) changes — verify `s3://` → `oci://` resolutions actually return data.

## Workflow

1. Find an existing manifest at `reports/<job>_manifest.json`. If none, ask the user to build one via [`migrate-job-flow`](../migrate-job-flow/SKILL.md) Phase 1, OR run [`aidp-build-dag`](../aidp-build-dag/SKILL.md).
2. Invoke `scripts/check_data_availability.py` (or `_for_workflow.py` if the manifest came from a Databricks Job ID).
3. Output a 3-section summary: TABLES (OK / MISSING / EMPTY), PATHS (same), and a remediation tip per category.

## Args

If the user named a manifest file or job name, use it. Otherwise infer from the most recent `reports/<job>_manifest.json`.

## Output template

```
== Data availability for <MyJob> ==

TABLES — 23 total
  OK     21
  MISSING  1   → '<catalog>.<schema>.<table_b>' — run migrate-catalog-flow or create manually
  EMPTY    1   → '<catalog>.<schema>.<table_c>' (0 rows; data backfill needed)

PATHS — 8 total
  OK      7
  MISSING 1   → 'oci://<bucket>@<ns>/path' — confirm bucket-mapping config

VERDICT: 2 issues. Safe to proceed? (y / N / fix-first)
```

## When to STOP and remediate first

If MISSING tables > 0, do NOT proceed to [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) without resolving. Options surfaced to the user:

- "These schemas missing — run migrate-catalog-flow first?"
- "These S3 buckets unmapped — open aidp-bucket-mapping skill?"
- "These specific tables out of scope — exclude in manifest?"
- "Proceed anyway, accept Pass-2 failures at these reads?"

## After this

- All-clear (no MISSING, no EMPTY): proceed to [`migrate-job-flow`](../migrate-job-flow/SKILL.md) or [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) directly.
- Catalog-shaped issues: route to [`migrate-catalog-flow`](../migrate-catalog-flow/SKILL.md).
- Bucket-shaped issues: route to [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md).

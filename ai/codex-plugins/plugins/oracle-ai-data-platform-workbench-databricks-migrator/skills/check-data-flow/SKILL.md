---
name: check-data-flow
description: Pre-migration data-availability scan flow. Reads a migration manifest, probes every spark.read.* / saveAsTable target on the AIDP cluster, reports EXISTS / ERROR / MISSING (tables) and EXISTS / ERROR / NOT FOUND (paths) with remediation tips per category. Use before any aidp-migrate-job run, or whenever the user asks "is the data ready?", "pre-migration check", "what's missing on AIDP".
---

# `check-data-flow` — pre-migration scan

Light wrapper over [`aidp-check-data`](../aidp-check-data/SKILL.md). Use before any [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) run.

## When to use

- BEFORE invoking [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) (mandatory gate — Pass-2 cost is high; fail fast on missing inputs).
- After [`migrate-catalog-flow`](../migrate-catalog-flow/SKILL.md) — verify schemas + tables actually resolve on AIDP.
- After [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md) changes — verify `s3://` → `oci://` resolutions actually return data.

## Workflow

1. Find an existing manifest at `reports/<job>_manifest.json`. If none, ask the user to build one via [`migrate-job-flow`](../migrate-job-flow/SKILL.md) Phase 1, OR run [`aidp-build-dag`](../aidp-build-dag/SKILL.md).
2. Invoke `~/.aidp-migrator/engine/scripts/check_data_availability_for_workflow.py --job-key <workflow-key> --cluster <CLUSTER_ID> --config <customer>.yaml` (pass `--manifest` when one exists; use plain `check_data_availability.py --root ... --cluster ...` only for a path-rooted scan).
3. Output a 2-section summary: Spark Tables (EXISTS / ERROR / MISSING), Storage Paths (EXISTS / ERROR / NOT FOUND), and a remediation tip per category. The scan ends with `RESULT: ALL DATA AVAILABLE - ready to migrate` or `RESULT: <n> data sources missing`.

## Args

If the user named a manifest file or job name, use it. Otherwise infer from the most recent `reports/<job>_manifest.json`.

## Output template

```
== Data availability for <MyJob> ==

TABLES — 23 total
  EXISTS  21
  MISSING  1   → '<schema>.<table_b>' — run migrate-catalog-flow or create manually
  ERROR    1   → '<schema>.<table_c>' (probe failed; investigate before migrating)

PATHS — 8 total
  EXISTS     7
  NOT FOUND  1   → 'oci://<bucket>@<ns>/path' — confirm bucket-mapping config

RESULT: 2 data sources missing. Safe to proceed? (y / N / fix-first)
```

## When to STOP and remediate first

If MISSING tables > 0, do NOT proceed to [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) without resolving. Options surfaced to the user:

- "These schemas missing — run migrate-catalog-flow first?"
- "These S3 buckets unmapped — open aidp-bucket-mapping skill?"
- "These specific tables out of scope — exclude in manifest?"
- "Proceed anyway, accept Pass-2 failures at these reads?"

## After this

- All-clear (`RESULT: ALL DATA AVAILABLE`): proceed to [`migrate-job-flow`](../migrate-job-flow/SKILL.md) or [`aidp-migrate-job`](../aidp-migrate-job/SKILL.md) directly.
- Catalog-shaped issues: route to [`migrate-catalog-flow`](../migrate-catalog-flow/SKILL.md).
- Bucket-shaped issues: route to [`aidp-bucket-mapping`](../aidp-bucket-mapping/SKILL.md).

---
description: Compare source-DB rows against a migrated AIDP target using a reconcile config YAML. The customer-facing proof that a migration is correct, not merely that it ran.
---

# `/infa-reconcile` — prove it, don't just run it

Thin wrapper over [`infa-reconcile`](../skills/infa-reconcile/SKILL.md).

## Workflow

1. **Confirm a reconcile config exists.** If not, walk the user through
   building one from `config/reconcile_config.example.yaml` — one entry per
   table, with `source` (live DB connection), `target` (AIDP catalog/schema/
   table), `key_columns`, and `ignore_columns` for audit/surrogate columns
   that never match across independent runs.
2. **Confirm the AIDP target is actually populated.** This command only
   compares two already-populated datasets — it does not run the migrated
   notebook for the user. If the target is empty, stop and get it run
   first (manually, or via a job [`infa-deploy`](../skills/infa-deploy/SKILL.md)
   created).
3. **Run it:**
   ```bash
   PYTHONPATH=engine python3 -m infa2aidp.cli reconcile \
     -c <config.yaml> -o ./reconcile_report --format all
   ```
4. **Report pass/fail per config entry**, and for any failure, name which
   check type failed (`row_count`/`schema`/`aggregate`/`data`) — a
   `row_count` pass with a `data` fail is a meaningfully different finding
   than a `row_count` fail.

## Args

`$ARGUMENTS`

If it names a config path, use it; else look for `reconcile_config.yaml` in
the working directory, else ask.

## Output template

```
== Reconciliation ==

<config-name>: PASS/FAIL
  row_count:  match
  schema:     match
  aggregate:  match
  data:       <N> row(s) differ (see reconcile_report.md)

VERDICT: <P>/<T> passed. <Remediation note if any failed.>
```

## When to STOP and remediate first

- Target table is empty or missing — not a reconcile problem, get the
  notebook run first.
- Every column mismatches uniformly — check `ignore_columns` before
  assuming a real defect.

## After this

A clean reconcile is the actual acceptance bar for the migration. If
already deployed, nothing further is needed; if not,
[`infa-deploy`](../skills/infa-deploy/SKILL.md).

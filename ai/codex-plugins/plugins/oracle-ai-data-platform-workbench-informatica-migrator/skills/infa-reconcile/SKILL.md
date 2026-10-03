---
name: infa-reconcile
description: Compare source-DB rows against a migrated AIDP target via `infa2aidp cli reconcile`, driven by a YAML config (row_count / schema / aggregate / data checks, with ignore_columns for audit/surrogate columns that never match). Use after a migrated notebook has actually run and populated its AIDP target — this is the customer-facing proof the migration is correct, not merely that it ran.
---

# `infa-reconcile` — prove it, don't just run it

Runs `infa2aidp cli reconcile`. This is the check that answers "did the
migration actually produce the same data", as opposed to "did the notebook
execute without an exception" — those are different questions, and only
this command answers the first one.

## When to use

- A migrated notebook (from
  [`infa-migrate-mapping`](../infa-migrate-mapping/SKILL.md)) has been run
  — manually, or via a job created by
  [`infa-deploy`](../infa-deploy/SKILL.md) — and its AIDP target table now
  has rows in it.
- The user asks "does this match the source", "prove the migration worked",
  "reconcile these tables".

**Do not invoke this expecting it to execute the migrated notebook for
you.** `infa-reconcile` only compares two already-populated datasets — the
source database and the AIDP target. Getting the AIDP target populated is a
separate step this tool does not automate end-to-end (see
[`infa-migrator-overview`](../infa-migrator-overview/SKILL.md)'s
no-live-cluster-execution gap).

## Canonical invocation

```bash
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli reconcile \
  -c reconcile_config.yaml \
  -o ./reconcile_report \
  --format all
```

## Config shape

See `config/reconcile_config.example.yaml` for a full example. Minimal
shape per entry under `reconciliations:`:

```yaml
reconciliations:
  - name: "Orders fact validation"
    type: all                       # row_count | schema | aggregate | data | all
    source:
      type: oracle
      host: <host>
      port: 1521
      database: <service-or-sid>
      schema: <schema>
      table: <table>
      username: "${SOURCE_DB_USER}"  # read from env, never a literal in the file
      password: "${SOURCE_DB_PASS}"
    target:
      catalog: <aidp-catalog>
      schema: <schema>
      table: <table>
    key_columns: [<primary-key-column>]
    ignore_columns: [LOAD_ID, LOAD_TS]   # audit/surrogate columns that never match across independent runs
    tolerance: 0.01
```

`type: row_count` is a fast, cheap first check — use it before `all` to
catch gross breakage without paying for a full row-level comparison.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `-c, --config` | required | YAML per above |
| `-o, --output` | `./reconcile_report` | |
| `--format` | `all` | `markdown`, `json`, `csv`, or `all` |

## What it checks, and why `ignore_columns` matters

Four check types, run per config entry: `row_count`, `schema`, `aggregate`,
`data` (row-for-row). `ignore_columns` exists specifically for columns like
`LOAD_ID`/`LOAD_TS` that are populated independently on each run and will
never match between the source-era run and the migrated run — without
excluding them, every reconciliation would report a false failure on those
columns alone.

**Row-count and aggregate checks are not enough on their own.** They will
not catch a running-total computed in the wrong row order, or an
aggregation missing a `GROUP BY` that Informatica silently handled as
"return the last row." Use `type: all` (or `type: data`) for anything where
row-ordered or per-row correctness matters, not just `row_count`.

## Output

Console: `<N> config(s): <P> passed, <F> failed, <E> error(s) -> <output
dir>`. Files: `reconcile_report.{md,json,csv}` per requested format(s).
Exit code 1 if any config failed.

## When it goes wrong

| Symptom | Fix |
|---|---|
| Every column mismatches, even ones that should match | Check `ignore_columns` — audit/surrogate columns not excluded will always fail. |
| Connection error to `source` | This needs `jaydebeapi`/`oracledb` — `pip install -e ".[reconcile]"` — and real DB reachability, not just AIDP credentials. |
| `row_count` passes but the user suspects something's wrong | Re-run with `type: all` or `data` — aggregate/row-count checks hide row-ordered and NULL-handling divergences. |

## After this

A clean reconcile across every config is the actual acceptance bar for this
migration — not a clean `infa-migrate-mapping` exit code, and not a
notebook that merely ran. Once clean, [`infa-deploy`](../infa-deploy/SKILL.md)
(if not already done) is the last step.

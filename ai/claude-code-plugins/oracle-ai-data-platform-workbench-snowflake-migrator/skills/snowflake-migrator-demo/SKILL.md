---
name: snowflake-migrator-demo
description: Dev mode. Run the entire Snowflake-to-AIDP migration pipeline against a built-in emulated estate and an emulated AIDP — no credentials, no network, nothing real is touched. Use when the user wants to understand what the migrator does before pointing it at a real account, wants a training walkthrough, or asks "show me how this works" / "demo mode" / "dev mode". Produces every real artifact plus DEMO.md, a narrated walkthrough of the lessons.
---

# Dev mode — the pipeline, emulated

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" demo --out-dir ./snowmig_demo
```

One command, no arguments to collect, nothing to ask the user for. It runs the
**production** extractors, planner, DDL generator, smoke test, catalog
registration and deploy — only the two transports are replaced by the fakes in
`engine/emulation/`, which is the same seam the unit tests use.

For a larger enterprise estate, run the ENTERPRISE emulation (`SNOWENT`:
external and Iceberg tables, hybrid and event tables, an outbound share to two
consumers, masking/row-access policies and tags, search optimization, a
container service, a native app, replication and failover groups):

```bash
"${CLAUDE_PLUGIN_ROOT}/bin/snowmig" demo --estate enterprise --out-dir ./snowmig_demo_enterprise
```

It runs assess through `plan`, `ddl`, `external-registration`, `share-plan`
and `summary`, and stops there: the AIDP steps for those paths are generated
for review, not emulated.

## What to say when presenting it

1. **Lead with the banner: everything is emulated.** The out-dir carries an
   `emulation.json` marker and `DEMO.md` opens with it. Never let a demo
   artifact read as a customer run.
2. **Walk `DEMO.md`, not the JSON.** It narrates each stage in order and then
   lists the lessons the estate was built to teach — a blocked `VARIANT`
   table, a `QUALIFY` view refused rather than guessed, a secure view, a
   masked column arriving unmasked, a task whose target table goes stale
   after cutover, the EXTERNAL and Standard catalog paths, a deploy refusal against the
   read-only EXTERNAL catalog, an asynchronous create that never became
   visible and the engine's diagnosis of it, and a view whose column types
   the engine re-derived and narrowed.
3. **Point at the artifacts.** Every file has exactly the shape a prod run
   produces, so `SUMMARY.md`, `STAGES.md`, `DDL_PLAN.md` and
   `SOFT_CLONE_SUMMARY.md` here are the best preview of what a real
   engagement delivers.
4. **Say what prod changes**: real credentials (read-only, enforced at the
   transport), AIDP coordinates asked per conversation, and `--execute` gates
   in front of every write. The pipeline is otherwise the same code.

## What this mode must never do

- Never mix the demo out-dir with a real run's `--out-dir`.
- Never present demo numbers as evidence about a real estate.
- Never skip the emulated banner when summarising results.

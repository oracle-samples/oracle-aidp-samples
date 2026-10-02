# Changelog

## 0.1.0 — unreleased

First release. Nothing has been published to an index yet, so there is no
upgrade note for an installed copy: everything below is new. One change does
break a script written against a development checkout, and is listed first.

### Breaking

- **`publish --prefix` is required with `--apply`.** `publish --apply` with no
  `--prefix` used to publish notebooks straight into the workspace root and
  create jobs under their bare pipeline names, colliding with anyone else
  publishing there. It now refuses before anything is read or sent and exits
  2 ("--prefix is required with --apply"). A dry run without `--prefix` still
  runs and warns. To migrate: add `--prefix <name>` to every `publish --apply`
  invocation — a letter followed by letters, digits or underscores, since it
  becomes the front of every job name.

### Verbs

- **Inventory** a Fabric Git export across all six sources — notebooks,
  warehouses, lakehouses, pipelines, semantic models and Dataflow Gen2. No Azure
  credentials required.
- **Plan** with dependency edges and a stable topological order. Cycles are
  reported and ordered last rather than refused. `--namespace` and `--catalog`
  are separate settings and neither is guessed. `--lakehouses` supplies the
  lakehouse GUID → name map a Git export cannot carry, which is otherwise
  recoverable only from a notebook bound to the lakehouse.
- **Migrate** every slice to artifacts on disk, with `report.json`, `report.md`
  and `report.html`. `--filter` runs one slice, plus any refusal that belongs to
  no slice. Offline; writes nothing but the output directory. There is no mode
  flag: `--demo` was required in development, then accepted and ignored, and is
  removed before the first release — passing it now exits 2. Nothing has been
  published to an index, so nothing installed can be passing it.
- **Verify** with PASS / REVIEW / SKIP / FAIL and wording enforced by tests. A
  FAIL is never hidden by `--filter`, nor is a refusal that belongs to no slice,
  and a non-zero FAIL exits 1.
- **Publish** a finished migration into an AIDP workspace. The only verb that
  writes anywhere: a dry run until `--apply`, never overwrites, refuses a job
  whose notebooks this run did not upload, and namespaces everything by
  `--prefix`.

### Translators

- **Notebook**: OneLake paths, table references, `display()`, cell magics.
  Every `notebookutils` surface is flagged. `%%sql` cells are left byte-for-byte
  alone and T-SQL constructs found there are flagged rather than rewritten.
- **T-SQL**: identifiers, four argument-order traps (`DATEDIFF`, `CONVERT`,
  `DATEADD`, and `CHARINDEX` via `locate`), scalar renames, `+` concatenation,
  `TOP`/`SELECT INTO`, data types, constraints. Procedures are flagged whole.
- **Power Query M → PySpark** for Dataflow Gen2, via a Node parser installed
  with `npm install` in `fabric_aidp/mparse/`. Lakehouse and CSV/Web sources are
  translated; Excel, Snowflake, Databricks and SQL sources are named and
  refused. A query whose steps cannot all be translated is blocked, and no
  partial file is written. Without Node, Dataflows are counted and reported as
  not translated, never silently dropped.
- **Data Pipelines → AIDP workflow jobs**, tasks and `dependsOn` preserved. A
  pipeline holding a non-notebook activity is refused whole.
- **Shortcut targets** mapped to `oci://` locations.

### Naming

- **One name for one table.** `<catalog>.<item>[_<schema>].<table>`, built in one
  module (`fabric_aidp/naming.py`) that the planner and every translator share,
  with Fabric's default `dbo` schema dropped. The notebook translator, the T-SQL
  rules and the Dataflow translator previously produced three different names
  for one table and all three graded PASS.
- **Four-tier catalog resolution.** Fabric exports no lakehouse table list, so
  table existence is resolved from warehouse DDL, shortcuts, an optional
  `--tables-csv`, and last from notebook writes. An inferred table is reported
  as inferred.

### Packaging

- Bundled demo estate (`--fixture demo`) and a staged 106-asset demo input built
  from it plus 78 permissively-licensed third-party artifacts.
- MCP server (`fabric-aidp-mcp`, `pip install -e '.[mcp]'`, Python 3.10+).
- Claude Code plugin: five commands, one skill, `.mcp.json`.
- Agent Plugins 1.0.0 manifest for Codex under `.codex-plugin/`, exposing the
  skill only — Codex has no commands concept.
- No required runtime dependencies. Python 3.9–3.14.

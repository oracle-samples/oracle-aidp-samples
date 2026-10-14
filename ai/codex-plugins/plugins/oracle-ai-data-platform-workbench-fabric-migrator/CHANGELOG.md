# Changelog

## 0.1.1 — unreleased

Security and launcher fixes on top of the 0.1.0 plugins published to
oracle-samples. No verb gains, loses or renames an argument.

### Security

- **The MCP server contains the CLI it runs** (security review
  SEC-AIDP-SAMPLES-004): every path argument resolves under
  `FABRIC_AIDP_WORK_ROOT` (default: the directory the server started in,
  refused when that is a filesystem anchor or the home directory — clients
  other than Claude Code start servers there) and a `..` escape or an
  absolute path outside it is refused before the CLI runs, naming the
  resolved root; a bare artifact name goes to a per-session
  `fabric-aidp-runs/<utc-time>-<pid>/` directory so sessions do not collide
  (`./inv.json` names the root itself), and each result starts with the root
  and the resolved paths. A Windows `\\?\` prefix on a path inside the root
  is accepted. The CLI runs in the work root with an allowlisted environment
  — `PATH`, `HOME`, locale, the Python and Windows variables it needs, the
  `OCI_*` selectors, `AIDP_WORKSPACE_KEY` and `AIDP_CLUSTER_KEY` by name — so
  a `GITHUB_TOKEN`, `AWS_SECRET_ACCESS_KEY` or `AIDP_SECRET_*` in the
  client's shell never reaches it. `publish` over MCP stays a dry run with no
  `apply`; `--apply` is CLI-only.

### Fixed

- **A `.env` could redirect where requests and credentials go.** The loader
  took any `AIDP_`/`OCI_`/`FABRIC_` key from the `.env` in the working
  directory, and those values also reach the `aidp` CLI `publish` runs, so a
  planted `AIDP_ENDPOINT` or `OCI_CONFIG_FILE` -- in an export someone sent
  you -- would have been honoured (flagged, unfixed, in the oracle-samples
  security review #146). A `.env` may now set only `AIDP_WORKSPACE_KEY`,
  `AIDP_CLUSTER_KEY`, `AIDP_INSTANCE_ID`, `OCI_NAMESPACE`, `OCI_CLI_PROFILE`
  and `OCI_CONFIG_PROFILE`; other keys of ours are ignored with a warning
  naming the key, never its value. `OCI_CLI_AUTH` and `OCI_CLI_REGION` are
  among them: a planted `OCI_CLI_AUTH=instance_principal` would make
  `publish --apply` act as an OCI host's own identity. On Linux and macOS a
  `.env` that is a symbolic link, is owned by another user, or can be
  written by anyone else is not read; group-write is accepted when the group
  is the user's alone, so the `-rw-rw-r--` files Ubuntu and Fedora create
  are read. The check judges the file it reads, opened once. A shell value
  is still never overridden.

- **The Codex plugin's MCP server never started.** Its `.mcp.json` set
  `PYTHONPATH` to `${CLAUDE_PLUGIN_ROOT}` (later `${PLUGIN_ROOT}`), and Codex
  expands neither: the server got the literal text and could not find the
  package. Codex now has its own config, `.codex-mcp.json`, named by
  `.codex-plugin/plugin.json`, which starts the server inside the plugin
  folder and forwards `FABRIC_AIDP_WORK_ROOT`. Claude Code keeps `.mcp.json`.
  Launched by Codex with no work root, the server refuses rather than use its
  own install folder: set `FABRIC_AIDP_WORK_ROOT` before starting Codex.

- **A Windows name over 255 characters was not explained.** Windows reports
  it as `EINVAL` (error 123), not `ENAMETOOLONG`, so the "name too long"
  message never fired and the bare OS error was shown. It is now explained
  when the name is over 255 characters; a short invalid name is not blamed
  on length.

### Changed

- Trademark notices and wording follow the vendors' published guidelines
  (oracle-samples #137, #138): NOTICE ends with a Trademarks block, the
  vendored Microsoft fixtures carry Microsoft's trademark sentence, and the
  marketplace entry is named "Oracle AIDP Migrator for Microsoft Fabric".

## 0.1.0 — 2026-10-02

Published as the Claude Code and Codex plugins in oracle-samples
(oracle-aidp-samples #124, #125).

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

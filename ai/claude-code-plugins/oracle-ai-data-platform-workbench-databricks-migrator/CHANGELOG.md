# Changelog

All notable changes to this plugin are documented here. Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Security
- Cluster-side dependency resolution no longer installs an import name just because a PyPI
  package of that name exists; only curated mappings are installed and the rest are
  reported for the operator to add explicitly. The generated availability check follows
  the same rule.
- `aidp_executor` session calls carry a timeout; `run_migration.sh` logs to a private
  temp file and asks for `kill <PID>` instead of `pkill -f`.
- **Mandatory notebook sandbox gate** (`engine/aidp_compat/notebook_policy.py`,
  SEC-AIDP-SAMPLES-005). `dbutils.notebook.run` refuses to execute anything until a sandbox
  (`AIDP_SANDBOX_CATALOG` / `AIDP_SANDBOX_SCHEMA` / `AIDP_SANDBOX_PREFIX`, or an explicit
  `SandboxPolicy`) is declared, and AST-checks every non-magic cell before `exec`: process /
  network imports, `os.environ` / `os.getenv` / `os.system` / `os.popen` / `os.exec*`,
  `shutil.rmtree`, `eval` / `exec` / `compile` / `__import__`, `open()` on absolute paths
  outside the prefix, and `dbutils.fs.rm/mv/cp/put`, `saveAsTable` / `insertInto`,
  `df.write.*`, `spark.sql` DDL/DML whose literal target leaves the sandbox are refused with a
  `PermissionError` naming notebook path, cell index, rule id and remediation. Non-literal
  write targets are refused unless the rule id is listed in `AIDP_NOTEBOOK_POLICY_ALLOW`.
  `dbutils.fs.rm/mv/cp/put` and the `safe_io` write helpers assert their target at runtime.
  Refusals and allowed exceptions are recorded in a policy log that `job_migrate.py` renders
  into each task's test report (**Notebook Policy Log**); the cluster bootstrap declares the
  write-redirect schema/bucket as the sandbox automatically.
- **Sandbox gate hardening** (review of SEC-AIDP-SAMPLES-005 / 006). The bootstrap now also
  declares the staging areas the migrator itself generates (`/Volumes/default/default/dbfs/`,
  `/tmp/`) and the output directory as sandbox prefixes, so migrated FUSE / local writes are
  not refused. The policy is a one-shot snapshot per kernel: later `AIDP_SANDBOX_*` changes are
  ignored and redeclaring / clearing it is refused (`NBP-POLICY-TAMPER`). `path_in_sandbox`
  canonicalises paths, refuses `..` segments and (POSIX) symlinks leading outside. The AST gate
  follows import / shim aliases and single-assignment string constants across cells, refuses
  `importlib` / `builtins` / `sys.modules` / `getattr(os, ..)` (`NBP-INDIRECT`), imports and
  attribute rebinding of the compat shims and policy module (`NBP-POLICY-TAMPER`),
  `os.remove/rename/mkdir...`, `shutil.move/copy*` and `pathlib` file operations
  (`NBP-FS-PATH` / `NBP-FS-DYNAMIC`), `writeTo`, `option("path")` / `options(path=)`,
  double-quoted `LOCATION` and `OPTIONS (path ...)`, `dbutils.fs.mkdirs`, `os.fork`,
  `asyncio` subprocess helpers and more network modules; `open()` with an unresolvable path is
  refused in any mode and `spark.sql()` with an unresolvable statement is `NBP-SQL-DYNAMIC`.
  `dbutils.fs.mkdirs` asserts its target at runtime; `dbutils.fs.mount` no longer writes
  `extra_configs` to the process environment.
- **Secrets shim is Vault-only by default** (`engine/aidp_compat/secrets.py`,
  SEC-AIDP-SAMPLES-006). `AIDP_SECRET_*` environment scanning and the JSON file fallback
  (now only via an explicit `AIDP_SECRETS_FILE`) happen only with
  `AIDP_ALLOW_PLAINTEXT_SECRETS=1`; the file must be owner-only (`0600`) on POSIX (skipped
  with a note on Windows); plaintext mode logs one `insecure plaintext secrets mode active`
  line without values; `AIDP_SECRET_SCOPES` restricts `get` / `list` / `listScopes` to
  allowlisted scopes and list operations never reveal values. The allowlist is created during
  planning: `job_migrate.py` collects the `dbutils.secrets.get(scope, key)` literals of each
  task and declares `AIDP_SECRET_SCOPES` / `AIDP_SECRET_KEYS` (`scope/key`, `scope/*`) in the
  cluster bootstrap (`none` before the first task; an operator-provided value wins); the shim
  reads both per call and logs refusals as `NBP-RUNTIME-SECRET`.
- Added `engine/tests/` (pytest) covering both controls; `pytest.ini` scopes collection to
  them.

## [0.2.0] — 2026-06-24

**Self-contained engine bundled.** The plugin no longer requires a separate clone of the migrator toolkit. The full Python engine ships under `engine/` and is invoked from skills via `${CLAUDE_PLUGIN_ROOT}/engine/scripts/...`.

### Added

- **`engine/` directory bundled with the plugin:**
  - `engine/scripts/` — Python engine modules (`job_migrate.py`, `agent_migrate.py`, `cluster_session.py`, `aidp_executor.py`, `build_dag.py`, `check_data_availability.py`, `migrate_catalog.py`, `extract_catalog_databricks.py`, `acceptance_contract.py`, `fixup_cell` helpers, etc.)
  - `engine/aidp_compat/` — 21 Python files (drop-in `dbutils` compatibility shim)
  - `engine/schemas/` — JSON schemas (acceptance contract)
  - `engine/setup.py`, `engine/requirements.txt` — Python package metadata + deps
  - `engine/run_migration.sh` — generic convenience script
- LICENSE at plugin root (MIT).

### Changed

- All 10 SKILL.md script-path examples updated from `python3 scripts/...` to `python3 ${CLAUDE_PLUGIN_ROOT}/engine/scripts/...`.
- `references/cli-map.md` updated to canonical bundled-engine paths.
- `README.md` Prerequisites section: "Clone the migrator repo" → "the engine ships bundled — just `pip install -r ${CLAUDE_PLUGIN_ROOT}/engine/requirements.txt`".
- `PRIVACY.md`: "knowledge-only" framing → "self-contained, bundled engine, no telemetry".
- All hardcoded customer identifiers (OCIDs, UUIDs, region, namespaces, customer name, workspace paths, internal hostnames, personal usernames) in the bundled engine replaced with `<PLACEHOLDER>` style values.

### Security / governance

- Generalized examples and identifiers from the source migration toolkit so the public plugin is engagement-neutral.
- Source customer output artifacts (`reports/`, `dbx_export/`) removed from the source repo.

## [0.1.0] — 2026-06-20 (initial release)

First public release of the Claude Code plugin for the Oracle AIDP Databricks Migration Toolkit.

### Skills (10)

- `aidp-migrator-overview` — router / lay of the toolkit
- `aidp-migrator-bootstrap` — environment readiness check (Python deps, OCI auth, cluster state, env-coords)
- `aidp-build-dag` — build migration manifest from a Databricks workspace path
- `aidp-check-data` — pre-migration data-availability scan
- `aidp-migrate-job` — Pass-1 deps + Pass-2 cell-by-cell execute/verify/fix on a live AIDP cluster
- `aidp-fixup-cell` — targeted rewind: re-execute cells from a history index
- `aidp-resume-migration` — resume an interrupted run
- `aidp-migrate-catalog` — Unity Catalog / HMS DDL → 18-rule rewriter → batched replay
- `aidp-bucket-mapping` — `s3://` → `oci://` bucket/namespace mapping config
- `aidp-acceptance-contract` — consecutive-zero-window convergence for batch / streaming

### Slash commands (4)

- `/migrate-job` — guided full-job migration flow
- `/migrate-catalog` — guided catalog migration flow
- `/check-data` — data-availability scan
- `/migration-status` — parse + summarize a `JOB_REPORT.md`

### Agents (2)

- `databricks-notebook-analyzer` — pre-migration notebook analysis
- `migration-reviewer` — post-Pass-2 migrated-notebook correctness review

### References (5)

- DDL rewrite rules (18 rules with examples)
- 15 Databricks → AIDP gotchas + fix recipes
- Env-coords scaffold
- `JOB_REPORT.md` parsing format
- CLI map (every migrator entrypoint → purpose + canonical invocation)

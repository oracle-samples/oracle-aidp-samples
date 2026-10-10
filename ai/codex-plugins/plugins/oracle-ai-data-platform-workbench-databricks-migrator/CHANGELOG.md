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
- `check_aws_creds.py` reports whether an S3 secret is set and its last four characters,
  never the value.
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
- **`get_tool_output` is confined to the tool output directory**
  (`engine/scripts/context_compactor.py`, `agent_migrate.py`, SEC-AIDP-SAMPLES-DBX-NEW-01).
  The model-callable tool passed its `filename` argument straight to `os.path.join` + `open`,
  so an indirect prompt injection in customer notebook content could read any file the
  consultant's account can read (`../../../oci_api_key.pem`, absolute paths) into model
  context. `ContextCompactor.get_saved_output` now accepts only the bare
  `tool_<NNN>_<tool>.txt` names it generates itself, additionally resolves the path with
  `os.path.realpath` and requires it to be a direct child of the compactor directory (so a
  planted symlink cannot escape either), logs refusals and returns a
  `[context_compactor] Refused` string; `_handle_get_tool_output` validates before consulting
  any compactor and stops walking the compactor history on a refusal. "File not found" and
  read errors no longer echo the host path or non-output files. The name allow-list is
  anchored with `\A`/`\Z`, applied with `fullmatch` and restricted to ASCII digits, so a
  newline-terminated name or one spelt with non-ASCII decimal digits is refused before any
  filesystem access instead of reaching `open()`.
- Added `engine/tests/` (pytest) covering these controls; `pytest.ini` scopes collection to
  them.

## [0.2.0] - 2026-06-24

- Bundled the OpenAI-based Python migration engine under `engine/`.
- Added a Codex SessionStart hook that stages the engine to `~/.aidp-migrator/engine`.
- Updated skills and references to invoke `~/.aidp-migrator/engine/scripts/...` instead of requiring a separate migrator clone.
- Replaced legacy provider-specific setup language with `OPENAI_API_KEY` / `OPENAI_MODEL` guidance.
- Kept customer-specific coordinates and secrets out of the plugin; users provide them through local env vars and gitignored env-coords files.

## [0.1.0] — 2026-06-23 (initial release)

First public release of the **Codex CLI** plugin for the Oracle AIDP Databricks Migration Toolkit. Codex has no separate "commands" or "agents" abstraction at the plugin layer - both are folded into the `skills/` directory.

### Skills (16)

**Core toolkit (10) — identical to the source toolkit guidance:**

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

**Guided flows (4) — translated from the guided command flows:**

- `migrate-job-flow` — guided full-job migration with phase checkpoints
- `migrate-catalog-flow` — guided catalog migration with dry-run preview
- `check-data-flow` — pre-migration data-availability scan wrapper
- `migration-status` — parse + summarize a `JOB_REPORT.md`

**Specialist reviewers (2) — translated from the specialist reviewers:**

- `databricks-notebook-analyzer` — pre-migration single-notebook readiness report
- `migration-reviewer` — post-Pass-2 migrated-notebook correctness review (catches cell-execute drift that PASS verdicts miss)

### References (5)

- DDL rewrite rules (18 rules with examples)
- 15 Databricks → AIDP gotchas + fix recipes
- Env-coords scaffold
- `JOB_REPORT.md` parsing format
- CLI map (every migrator entrypoint → purpose + canonical invocation)

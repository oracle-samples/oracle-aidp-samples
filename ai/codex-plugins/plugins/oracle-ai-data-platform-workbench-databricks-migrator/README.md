# Oracle AI Data Platform — Migrator for Databricks (Codex CLI plugin)

> **Migrate Databricks notebooks, jobs, and catalogs onto Oracle AIDP — in natural language.**
> Drives the *AIDP Databricks Migration Toolkit* (an agentic migrator that runs cells live on the AIDP cluster, verifies, and self-fixes) end-to-end from the OpenAI Codex CLI.

> **Canonical home:** [`oracle-samples/oracle-aidp-samples/ai/codex-plugins/plugins/oracle-ai-data-platform-workbench-databricks-migrator`](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/codex-plugins/plugins/oracle-ai-data-platform-workbench-databricks-migrator).

This plugin is **self-contained for Codex**: it ships the OpenAI-based Python migration engine under `engine/`. On SessionStart, the hook stages that engine to `~/.aidp-migrator/engine` so Codex can run migrations without a separate toolkit clone.

> **Status:** **v0.2.0** - self-contained Codex release with the bundled OpenAI-based migration engine.

---

## What it does

| Area | Skills |
|---|---|
| **Bootstrap & setup** | `aidp-migrator-overview`, `aidp-migrator-bootstrap` |
| **Plan & inventory** | `aidp-build-dag`, `aidp-check-data`, `check-data-flow` |
| **Notebook migration** | `aidp-migrate-job`, `aidp-fixup-cell`, `aidp-resume-migration`, `migrate-job-flow` |
| **Catalog migration** | `aidp-migrate-catalog`, `aidp-bucket-mapping`, `migrate-catalog-flow` |
| **Verification & quality** | `aidp-acceptance-contract`, `migration-status`, `migration-reviewer`, `databricks-notebook-analyzer` |

Each skill is a single-file Markdown SKILL.md with a clear "When to use" trigger so Codex routes correctly without external direction.

### What gets automated end-to-end

```
Databricks workspace                                    AIDP DataLake
─────────────────────                                   ──────────────
Unity Catalog / HMS schemas + tables (DDL)   ──┐
   │                                           ├──→  Catalog migration (batched DDL replay)
External s3:// table locations             ──┘            18 rewrite rules:
                                                          • 3-part → 2-part name flatten
                                                          • s3:// → oci:// via bucket-map
                                                          • source format preserved (Delta stays Delta)
                                                          • delta.* / spark.sql.* catch-all scrub
                                                          • MV / streaming rejection
                                                          • CREATE SCHEMA COMMENT-colon strip

Notebooks (.dbc + .ipynb + .py)            ──→   Notebook migration (per workflow / per task)
Jobs + schedules + task DAGs                     Pass 1: %run dep tree, code-only rewrites
                                                 Pass 2: cell-by-cell execute on AIDP cluster,
                                                         4-way verify (status / stderr / Spark
                                                         logs / model eval), up to 10 fix attempts,
                                                         fixup_cell rewind for replays.
                                                 Output: per-job .ipynb + JOB_REPORT.md
```

### Signature differentiators

- **Cell-by-cell execute / verify / fix loop** — the migrator runs each Databricks-rewritten cell on a live AIDP cluster, parses the output, and self-corrects via a model with tool use (14 tools: `explore_path`, `suggest_oci_path`, `search_catalog`, `run_on_cluster`, `describe_table`, `list_schemas_and_tables`, `read_notebook_source`, `inspect_package_source`, `summarize_notebook`, `submit_code`, `make_note`, `get_cell_history`, `get_history_entry`, `fixup_cell`).
- **Write-redirect sandbox schema** — source data is never touched during migration. Every `.saveAsTable(...)` / `INSERT INTO` is silently redirected to a sandbox schema, then verified post-run.
- **Acceptance contract** — for batch / streaming pipelines, declare PASS only after K consecutive empty-pending windows (consecutive-zero convergence).

---

## When to use this plugin

You're moving a Databricks workload onto AIDP and want Codex to drive the whole port. Typical asks:

- *"Migrate this Databricks job to AIDP"* / *"port my workflow"*
- *"Build a migration manifest from this Databricks workspace path"*
- *"My migrated notebook fails at cell 23 — fix it"*
- *"Migrate the Unity Catalog DDL into the AIDP default catalog"*
- *"Check whether the source tables are available before I migrate"*
- *"Resume the migration from task X"*
- *"Set up the acceptance contract for the streaming task"*

---

## Install

The plugin lives in the `oracle-aidp-codex` marketplace, which is hosted as a sparse-checkout subdir of `oracle-samples/oracle-aidp-samples`.

```bash
# Register the marketplace (one-time)
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
    --ref main \
    --sparse ai/codex-plugins

# Install the plugin
codex plugin add oracle-ai-data-platform-workbench-databricks-migrator@oracle-aidp-codex
```

Update later:

```bash
codex plugin marketplace upgrade oracle-aidp-codex
```

Verify it's enabled:

```bash
codex plugin list | grep databricks-migrator
```

---

## Prerequisites

This plugin bundles the migrator engine. To actually run migrations, you need:

1. **Python dependencies for the bundled engine** - after installing the plugin and starting a new Codex thread, the SessionStart hook stages the engine to `~/.aidp-migrator/engine`. Install dependencies once:

   ```bash
   python -m pip install -r ~/.aidp-migrator/engine/requirements.txt
   ```

2. **OCI authentication** - `~/.oci/config` with either an `api_key` profile (recommended for unattended runs) or an `oci session authenticate` session-token profile (for interactive notebooks). The migrator reads whichever profile the operator selects via `--oci-profile`.

3. **An ACTIVE AIDP cluster** - the migrator's Pass-2 execute/verify/fix loop talks to a live cluster via WebSocket. The cluster must be in `Active` state before invoking `job_migrate.py`.

4. **An OpenAI API key** - set `OPENAI_API_KEY` in the shell before running Pass-2. Optionally set `OPENAI_MODEL` to your team's approved OpenAI model.

Once those are in place, the plugin's skills know how to invoke each entrypoint — Codex will run the right CLI commands in the right order based on your natural-language ask.

---

## Skill index

### Foundation
| Skill | Purpose |
|---|---|
| `aidp-migrator-overview` | Router. Read this first to get the lay of the toolkit + which skill handles which phase. |
| `aidp-migrator-bootstrap` | One-shot environment readiness check (Python deps, OCI auth, cluster state, env-coords file). |

### Plan
| Skill | Purpose |
|---|---|
| `aidp-build-dag` | Build a migration manifest (`reports/<job>_manifest.json`) from a Databricks workspace path. Walks `%run` chains, emits the execution DAG. |
| `aidp-check-data` | Pre-migration scan: verify source tables / paths exist on the AIDP cluster before kicking off. |
| `check-data-flow` | Guided variant of `aidp-check-data` with explicit OK / MISSING / EMPTY summary + remediation routing. |

### Execute
| Skill | Purpose |
|---|---|
| `aidp-migrate-job` | Run the migrator end-to-end against a manifest. Pass-1 deps + Pass-2 cell-by-cell on the live cluster. |
| `aidp-fixup-cell` | Targeted rewind: re-execute cells from history index N with a `why` reason, through execute+verify+fix. |
| `aidp-resume-migration` | Resume an interrupted run. Skips already-migrated notebooks via `_migration_cache` + on-cluster `os.path.exists()`. |
| `migrate-job-flow` | Guided full-job migration with phase checkpoints (DAG → check-data → migrate → status). |

### Catalog
| Skill | Purpose |
|---|---|
| `aidp-migrate-catalog` | Extract Unity Catalog / HMS metadata → 18-rule DDL rewriter → batched replay on AIDP. |
| `aidp-bucket-mapping` | Configure `s3://` → `oci://` bucket/namespace mappings the rewriter consumes. |
| `migrate-catalog-flow` | Guided catalog migration (extract → rewrite preview → batched replay) with stop points. |

### Verify
| Skill | Purpose |
|---|---|
| `aidp-acceptance-contract` | YAML-driven consecutive-zero-window acceptance for batch / streaming convergence. |
| `migration-status` | Parse + summarize a `JOB_REPORT.md` from a previous run. |
| `databricks-notebook-analyzer` | Read a Databricks notebook + report what it does, dependencies, risks (drives Pass-1 planning). |
| `migration-reviewer` | Review a migrated `.ipynb` post-Pass-2 for correctness (Apache Spark API drift, `%run` trailing-slash, builtins.sum shadow, etc.). |

### References (loaded on demand)
| Reference | Use |
|---|---|
| [`references/ddl-rewrite-rules.md`](./references/ddl-rewrite-rules.md) | The 18 DDL rewrite rules the catalog migrator applies, with input/output examples. |
| [`references/gotchas.md`](./references/gotchas.md) | 15 Databricks → AIDP gotchas + fix recipes. |
| [`references/env-coords.md`](./references/env-coords.md) | Scaffold for the customer's environment coordinates (DataLake OCID, workspace UUID, etc.). Fill once, refer to in every skill. |
| [`references/job-report-format.md`](./references/job-report-format.md) | How to parse `JOB_REPORT.md` to extract per-cell pass/fail/fix counts. |
| [`references/cli-map.md`](./references/cli-map.md) | Each migrator CLI entrypoint mapped to its purpose + canonical invocation. |

---

## Engine

Everything flows through the bundled migrator CLI staged under `~/.aidp-migrator/engine`:

```bash
# Inventory & plan
python3 $HOME/.aidp-migrator/engine/scripts/build_dag.py --root <workspace-path> --job-name <name> --output reports/<name>_manifest.json
python3 $HOME/.aidp-migrator/engine/scripts/check_data_availability.py --root <workspace-path> --cluster <cluster-id>

# Execute
python3 $HOME/.aidp-migrator/engine/scripts/job_migrate.py \
  --manifest reports/<name>_manifest.json \
  --cluster <cluster-id> \
  --aidp-base https://aidp.<region>.oci.oraclecloud.com/20240831 \
  --datalake-ocid <your-datalake-ocid> \
  --workspace-id <your-workspace-uuid> \
  --output-base <output-workspace-path> \
  --oci-profile <profile-name>

# Catalog (separate flow)
python3 $HOME/.aidp-migrator/engine/scripts/extract_catalog_databricks.py --catalogs <catalog> --schemas-only "<catalog>:<schema>" --out reports/catalog_pack.json
python3 $HOME/.aidp-migrator/engine/scripts/migrate_catalog.py --pack reports/catalog_pack.json --cluster <cluster-id> --aidp-base ... --datalake-ocid ...
```

The skills tell Codex when to call each + how to thread args from the env-coords reference into them.

The one script that talks to Databricks, `extract_catalog_databricks.py`, reads the PAT from
`DATABRICKS_TOKEN` in the environment or from `--token-file <path>` (a file only its owner can
read -- `chmod 600`; on Windows the permission check is skipped); the other scripts
(`build_dag_from_workflow.py`, `migrate_catalog.py`, ...) talk to AIDP with your OCI profile, not
to Databricks. No script takes a credential on the command line: `--token <value>` is refused
with exit code 2, because argv is visible in `ps`, shell history, CI transcripts and pasted
support commands, and a mistyped form (`--tok=<value>`, `-t <value>`, a bare value) is refused
without the value being repeated in the error. Output names where the token came from, never
the token.

---

## Relationship to the migrator toolkit

This plugin is the **Codex CLI** package for the AIDP Databricks Migration Toolkit. The plugin bundles the OpenAI-based engine in `engine/`, exposes the migration workflows as Codex skills, and registers through the shared `oracle-aidp-codex` marketplace under `ai/codex-plugins`.

---

## Runtime safety: notebook sandbox policy and secrets

The bundled `aidp_compat` layer that migrated notebooks import on the cluster enforces two
fail-closed controls at runtime (in addition to the static cell analysis and write redirects
the migrator applies while it drives the cluster):

**Notebook sandbox policy** (`aidp_compat.notebook_policy`). `dbutils.notebook.run(...)` and
the compat write helpers (`dbutils.fs.rm/mv/cp/put/mkdirs`, `safe_io.safe_*` writers) refuse to
run until a sandbox is declared for the run:

| Variable | Meaning |
|---|---|
| `AIDP_SANDBOX_CATALOG` / `AIDP_SANDBOX_SCHEMA` | The only catalog.schema that table writes may target |
| `AIDP_SANDBOX_PREFIX` | Comma list of prefixes that path writes, `dbutils.fs` deletes/moves, `os` / `shutil` / `pathlib` file operations and absolute `open()` calls must stay under; the first entry is the object-storage prefix (`oci://bucket@ns/path/`), the rest are staging areas. Paths are canonicalised; `..` segments and (on POSIX) symlinks leading outside are always outside |
| `AIDP_SANDBOX_ALLOW_NETWORK` | `1` permits network imports (`requests`, `socket`, `urllib`, `urllib3`, `httpx`, `aiohttp`, `http.client`, `paramiko`, `ftplib`, `smtplib`, ...); default off |
| `AIDP_NOTEBOOK_POLICY_ALLOW` | Comma list of rule ids that are a reviewed exception (for example `NBP-TABLE-DYNAMIC` for a `saveAsTable` target that cannot be resolved statically) |
| `AIDP_NOTEBOOK_POLICY_LOG` | Optional JSONL file that receives every refusal / allowed exception / sandbox declaration |

`job_migrate.py` declares the sandbox automatically on every cluster connect and per task: the
write-redirect schema and bucket, plus the staging areas its own migration rules steer
notebooks into (`/Volumes/default/default/dbfs/` for translated `/dbfs/` paths, `/tmp/` for
torch / h5py / sqlite staging) and the tool's output directory. An operator-provided value wins
and replaces the whole list. The first declaration is frozen for the kernel: later environment
changes are ignored by the runtime assertions and a redeclaration from notebook code is refused
(`NBP-POLICY-TAMPER`). Before each non-magic cell runs, the cell is parsed and refused with a
`PermissionError` naming the notebook path, cell index, rule id and remediation if it imports
process / network modules (`subprocess`, `multiprocessing`, `ctypes`, `pty`, `socket`,
`requests`, `urllib`, ...), touches `os.environ` / `os.getenv` / `os.system` / `os.popen` /
`os.exec*` / `os.fork` (also through an alias such as `import os as o`), calls `shutil.rmtree`,
`eval` / `exec` / `compile` / `__import__`, reaches for `importlib` / `builtins` /
`sys.modules` / `getattr(os, ...)`, imports or rebinds the compat shims or the policy module
itself, or opens / removes / renames / writes (`open`, `pathlib`, `os.remove`, `shutil.move`,
`saveAsTable`, `insertInto`, `writeTo`, `df.write.*` including `option("path")`, `CREATE TABLE`
/ `INSERT` / `DROP` / `MERGE` / `DELETE` / `LOCATION` / `OPTIONS (path ...)` via `spark.sql`,
`dbutils.fs.rm/mv/cp/put/mkdirs`) a target outside the sandbox. The gate is deny-by-default for
every route to the same capability, not only the direct spelling: the builtins namespace through
`__builtins__` / `builtins` / `globals()` / `locals()` / `vars()` / `getattr`, `getattr` /
`setattr` with a non-literal name, dunder and frame introspection chains (`__subclasses__`,
`__globals__`, `__dict__`, `f_globals`, ...; `NBP-DUNDER-ESCAPE`), the IPython shell API and
`%magic` / `!command` lines inside a cell (`get_ipython().system`, `InteractiveShell`;
`NBP-IPYTHON-ESCAPE`), the Spark JVM / py4j bridge (`spark._jvm`, `sc._jsc`, `df._jdf`, `py4j`;
`NBP-POLICY-TAMPER`), deserialisers that execute code (`pickle.loads`, `yaml.load` without a
safe loader, `torch.load` without `weights_only=True`; `NBP-DESERIALIZE`), process / debugger /
profiler modules, the stdlib open-equivalents (`io.open`, `os.open`, `gzip.open`, ...), and
names that were bound to any of these and later rebound to something the gate cannot resolve.
The same rule table serves every spelling: `posix` / `nt` are judged as `os`, `getattr(mod,
"literal")` like `mod.literal`, and `str.format` / `format_map` templates (literal or bound once)
are walked field by field (`{0.__class__.__mro__}` refused, `{0.__class__.__name__}` fine).
Library readers (`pd.read_csv`, `spark.read.csv`, `np.load`, `sc.textFile`, ...) with a literal
local path (`/etc/passwd`, `/dbfs/...`, `~`, `C:\`, `file://`) outside the prefix are refused.
`spark.sql` is an allowlist, not a blocklist: only read statements (`SELECT` / `VALUES` / `TABLE`
/ `WITH .. SELECT` / `SHOW` / `DESCRIBE` / `EXPLAIN` / `CACHE` / `UNCACHE` / `REFRESH TABLE` /
`CREATE TEMP VIEW`) and writes to tables inside the sandbox schema are accepted; the top-level
verb is resolved behind a `WITH` CTE list (so `WITH x AS (..) INSERT INTO prod.t` is the write it
is), `reflect(` / `java_method(` / `try_reflect(` (plain or as a backtick-quoted function name) and
`TRANSFORM .. USING` are refused anywhere in a statement, path tables (``<provider>.`<path>` `` for
any provider, quoted or a provider class) and `COPY INTO .. FROM` sources on the local filesystem
outside the prefix are refused, raw literals (`r'..'`) are lexed as Spark lexes them, the
destination of `ALTER .. RENAME TO` is a write target, `spark.sql` keyword arguments are rendered
into the statement as pyspark renders them, `IDENTIFIER(..)` is refused, network data sources
(`USING jdbc` / `kafka` / `socket`, `spark.read.format("jdbc")`, `spark.read.jdbc`) are refused
while the sandbox allows no network access, and `ADD JAR`, `CREATE FUNCTION`, `GRANT`, `SET` outside
a short conf allowlist, SQL scripting, multi-statement texts and every other verb are
`NBP-SQL-REFUSED`. The Python twins (`spark.table`, `spark.catalog.setCurrentCatalog` /
`setCurrentDatabase`, `spark.conf.set`, `spark.catalog.createTable`, `sc.addFile`,
`pyspark.sql.functions.reflect` / `java_method` / `call_function`) are judged by the same rules.
While `dbutils.notebook.run` executes, a runtime SQL guard re-classifies the final text of every
`spark.sql` (after keyword rendering) / `spark.table` / `selectExpr` / `filter` / `where` /
`functions.expr` / `functions.reflect` / `java_method` / `call_function` call before Spark sees it,
and `spark.newSession()` / `getOrCreate()` / `getActiveSession()` are refused because a fresh
session would not carry it.
A target that is a name bound once to a string literal earlier in the run is checked as that
literal; any other non-literal target is refused unless its rule id is allowlisted, with two
scope decisions that are allowed and recorded in the policy log instead: a reader path the gate
cannot resolve (`spark.read.parquet(src)`, `unverified_read_path`; reading source data through
parameters is the normal migration case and object-storage / catalog reads are never
path-checked) and a `spark.sql` statement with run-time fragments whose literal head verb is a
read verb (`unverified_sql_read`; admitted only inside `dbutils.notebook.run`, where the runtime
SQL guard re-classifies the final text before Spark executes it; a bare static scan refuses it as
`NBP-SQL-DYNAMIC`), plus a `DataFrame.pipe(fn)` whose callable the gate cannot type, on a receiver
the notebook bound (`unverified_pipe_callable`; `RDD.pipe(cmd)`, any string-typed `.pipe()`
argument and a receiver the notebook never bound are refused). `open(path_var)` is not such a
decision: a non-literal `open()` path is refused in read mode as well, and so is
`thing.open(path_var)` on a receiver the notebook never bound (`fs.open(p)` on a bound
object-store filesystem is recorded). The review-derived gap classes are closed as classes, not spellings: a gated
module or Spark object handed around as a value (`[os][0]`, `run(os)`, `return os`,
`lambda: os`, ...), a process sink name on a receiver the gate cannot resolve
(`helper().system`, `obj.popen`), string-keyed accessors (`operator.attrgetter` /
`methodcaller`, `functools.reduce(getattr, ..)`, `inspect.getattr_static` / `getmembers`),
re-rooting at a module named mid-chain (`shutil.os.system`, `pd.io.common.os.environ`),
deny-by-default deserialisers (the whole pickle family except the dump side, `torch.load` at
any depth, `numpy` `allow_pickle`, every unsafe yaml loader / constructor / hook / subclass),
`globals()[k]` with a tainted key or a dereferenced result, and `open(*a, **kw)`. The coverage
round on top of that: a list literal is folded into a path only while its name is used for
nothing but `sep.join(name)` (`p[0] = '/etc'`, `p.insert(..)`, `q = p`, `mutate(p)` make the
join dynamic, fail closed); keyword-gated loaders fail closed when the switch is positional or
hidden (`np.load(p, None, True)`, `np.load(p, **kw)`, `torch.load(.., pickle_module=..)` even
with `weights_only=True`, `torch.serialization.add_safe_globals`); the state of a gated module
is read-only (`yaml.SafeLoader.yaml_constructors[..] = h`, `.update(..)`, `setattr(yaml.SafeLoader,
..)`; `sys.path` and `torch` flags stay settable); a process sink name (`mymod.system`) or a
`_private` attribute (`mymod._run`, `np._core`) on an imported module the gate has no rules for
is refused (`platform.system()` is the documented homonym); `site` / `pip` load code from a path
and are refused like `addPyFile`. Library writers are the mirror of the readers and fail
closed like every other write: `np.save`, `torch.save`, `joblib.dump`, `scipy.io.savemat`,
`cv2.imwrite`, `plt.savefig`, `pq.write_table`, `pd.ExcelWriter`, `mlflow.*.save_model`, and
`df.to_csv` / `to_parquet` / `to_pickle` / `write_parquet` / `savefig` / `tofile` on any receiver
with a literal local path outside the prefix are `NBP-PATH-WRITE`, with a path the gate cannot
resolve `NBP-PATH-WRITE-DYNAMIC` (a `StringIO` / `BytesIO` / `ExcelWriter` handle the notebook
bound is not a path); generic `wb.save(..)` / `model.save(..)` / `img.save(..)` judge a literal
path only. `tempfile` creators with a literal `dir=` or an absolute `prefix=` outside the prefix
(and `tempfile.tempdir =`) are `NBP-FS-PATH`; `faulthandler`, `traceback.walk_stack` /
`walk_tb`, `capture_locals=True` and `Task.get_stack()` are frame dumps (`NBP-INDIRECT`). A
`DataFrameWriter` / `DataFrameReader` held in a variable (`wr = df.write.mode('overwrite')`,
`w2 = wr`, `rd = spark.read.format('csv')`) is followed like the chain it was built from, also
across cells; structured-streaming writers (`df.writeStream...start(p)` / `.toTable(t)` /
`.option('checkpointLocation', p)`) are judged like `df.write`; a re-export back to the chain's own
root (`os.path.os.system`, also via `import os.path as op`) is re-rooted like `shutil.os`; and
`http.server` / `wsgiref.simple_server` / the Python web servers are network modules at every door
(`import http; http.server.HTTPServer(..)` included). The gate
matches these forms statically and is not a Python sandbox: a static AST gate over a full
Python runtime is a mitigation layer, the platform-level control is the IAM scope of the
cluster the notebook runs on, and the runtime assertions in `dbutils.fs.*`, `safe_io` and
`dbutils.secrets` are the enforcement layer for writes that go through the compat helpers (see
`engine/aidp_compat/SUPPORTED_OPERATIONS.md` section 8 for the rule table, what is not covered
and the known false refusals). Refusals, allowed exceptions and skipped magic-led cells are
recorded and rendered into each task's test report under **Notebook Policy Log**. Scheduled
jobs that keep using `aidp_compat` after
migration must declare the same variables in their environment.

**Secrets** (`dbutils.secrets`). Lookups go to OCI Vault only (`AIDP_VAULT_OCID`, secret named
`<scope>/<key>`). The plaintext demo fallbacks (`AIDP_SECRET_<SCOPE>_<KEY>` environment
variables and the JSON file named by `AIDP_SECRETS_FILE`) are consulted only when
`AIDP_ALLOW_PLAINTEXT_SECRETS=1`; the file must be owner-only (`0600`) on POSIX, and the shim
logs one line (`insecure plaintext secrets mode active`, never a value) when the mode is on.
`AIDP_SECRET_SCOPES=<scope,scope>` and `AIDP_SECRET_KEYS=<scope/key,scope/*,...>` restrict
what `get` / `list` / `listScopes` may touch (`none` = nothing). `job_migrate.py` derives both
from the `dbutils.secrets.get(scope, key)` literals it finds while planning each task and
declares them in the cluster bootstrap (an operator-provided value wins; a scope used with a
non-literal key is planned as `scope/*`, a non-literal scope must be allowlisted by the
operator). Refusals name the scope/key only and appear in the Notebook Policy Log as
`NBP-RUNTIME-SECRET`; list operations return names only.

---

## Privacy

This plugin **does not collect, store, transmit, or share any user data**. Everything runs locally against **your own** AIDP tenancy. Full statement: [`PRIVACY.md`](./PRIVACY.md).

---

## License

MIT — see [`LICENSE`](./LICENSE).

## Core developers

- **Sid Rao** (Oracle Agentic AI Development)
- **Ahmed Awan** (Oracle Forward Deployed Engineering)
- **Nishant Patel** (Oracle Forward Deployed Engineering)

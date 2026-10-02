# fabric-aidp-migrator

Migration assistant for **Microsoft Fabric → Oracle AI Data Platform (AIDP)**. It reads
a Fabric workspace exported through Fabric's native Git integration, inventories the
estate, and translates notebooks, Warehouse T-SQL, Dataflow Gen2 (Power Query M) and
Data Pipelines to Spark and AIDP jobs — emitting reviewable artifacts and flagging
anything it cannot convert safely.

**Nothing is written to AIDP unless you ask for it.** `inventory`, `plan`,
`migrate` and `verify` are entirely offline and need no credentials. A fifth verb,
`publish`,
uploads a finished migration into a workspace — it is a dry run by default,
needs `--apply`, and never overwrites anything it did not create.

> **Why:** the Databricks and AWS plugins cover the other two common estates. Many AIDP
> migrations originate on Fabric — OneLake, Lakehouses, Warehouses, notebooks — and this
> covers that path.

## What it does

| Fabric source | → | AIDP target |
|---|---|---|
| **Notebooks** (PySpark, `%%sql`) | → | Spark notebooks, OneLake paths remapped |
| **Warehouse** tables, views, queries | → | Spark SQL |
| **Lakehouse** shortcuts (S3, ADLS Gen2, …) | → | `oci://` locations |
| **Data Pipelines** | → | AIDP workflow jobs (tasks + `dependsOn`) |
| **Semantic models** | → | inventoried only (DAX has no AIDP target) |
| **Dataflows** (Power Query / M) | → | PySpark scripts (needs Node; see below) |

Five verbs:

```
fabric-aidp inventory <git-export-dir>    # read-only scan → manifest
fabric-aidp plan      inv.json            # manifest → dependency-ordered plan
fabric-aidp migrate   plan.json          # translate; writes artifacts locally
fabric-aidp verify    ./migrated          # PASS / REVIEW / SKIP / FAIL
fabric-aidp publish   ./migrated --prefix <you>          # dry run: what would be uploaded
fabric-aidp publish   ./migrated --prefix <you> --apply  # upload notebooks, create AIDP jobs
```

## Quick start — no Fabric tenant needed

```bash
make setup      # virtualenv, the CLI, and the Power Query parser
make demo       # migrate the bundled estate; prints PASS/REVIEW/SKIP/FAIL
```

Evaluating it? `make check` does setup, demo and the full test suite in one
command and tells you which two numbers to read. `make` on its own lists the
targets.

**The `make` targets need a POSIX shell** — they hardcode `.venv/bin/python`
and `demo` runs `demo.sh`, which is bash. On Windows use WSL or Git Bash, or
run the same steps directly; the tool itself is pure Python and portable:

```
py -m venv .venv                                   # then .venv\Scripts\python
.venv\Scripts\python -m pip install -e .
cd fabric_aidp\mparse && npm install && cd ..\..   # Dataflows only
.venv\Scripts\python scripts\stage_demo_input.py
.venv\Scripts\fabric-aidp inventory demo-input -o out\inventory.json
.venv\Scripts\fabric-aidp plan out\inventory.json -o out\plan.json --namespace <ns>
.venv\Scripts\fabric-aidp migrate out\plan.json -o out\migrated
.venv\Scripts\fabric-aidp verify out\migrated
```

**Installed from a wheel instead of a clone?** `make setup` and the
`cd fabric_aidp/mparse` lines above assume a checkout. With
`pip install fabric_aidp_migrator-<version>-py3-none-any.whl` the parser
directory is inside site-packages, so ask the package where it is:

```bash
cd "$(python -c 'import fabric_aidp, pathlib; print(pathlib.Path(fabric_aidp.__file__).parent / "mparse")')" && npm ci
python -m pip install 'mcp>=1.2,<2'      # only for the MCP server; Python 3.10+
```

`npm ci` installs from the shipped lockfile, which resolves to the public
npm registry. `pip uninstall` does not remove the `node_modules` this
creates; delete that directory by hand. When the parser is missing,
`migrate` names the exact directory in its INV05 finding.

**Driving it from an MCP client?** (Codex, Cursor, Claude Desktop.) The repo
ships a `.mcp.json` that runs `python3 -m fabric_aidp.mcp_server` with the
plugin root on `PYTHONPATH` (so an installed plugin needs no console script on
PATH; `fabric-aidp-mcp` is the same server for a pip install), and the server
needs an extra `pip install -e .` does **not** install:

```bash
pip install -e '.[mcp]'
```

`mcp` requires Python 3.10 and this tool supports 3.9, so it cannot be a plain
dependency. Without it the server starts and exits, and the client shows
`fabric-aidp-mcp cannot start: the MCP SDK (1.x) is not installed`. Nothing
else needs the extra — the CLI and the Claude Code slash commands work without
it.

**Running it against a real workspace?** [docs/RUNBOOK.md](docs/RUNBOOK.md) is
the end-to-end path — Fabric Git export, through the four offline verbs, to
jobs published on an AIDP cluster.

`make demo` stages one input directory and migrates all of it — **110 assets**:

| Source | What |
|---|---|
| **Acme estate**, 12 items | Synthetic, written alongside the rules. The only source of Lakehouse shortcuts, a semantic model, and a Dataflow with a declared write target. |
| **78 real artifacts** | 25 notebooks, 30 pipelines, 14 dataflows, 9 Warehouse objects — from 17 public GitHub repositories, every one permissively licensed. See [tests/fixtures/real/corpora/NOTICE](tests/fixtures/real/corpora/NOTICE). |

Both, because they fail differently. The synthetic estate reaches every *source
kind* — it is the only input here with a shortcut, a semantic model and a Dataflow
that declares where it writes — but it can never surprise rules written against it.
The real files surprise them constantly — a planner bug that refused a whole
workspace, a Synapse notebook header, activities buried in a `ForEach` — but no
single public repository exercises the whole tool. Neither reaches most of the
rules; see **Limits worth knowing** for the measured figure.

Ends `PASS: 17  REVIEW: 92  SKIP: 1  FAIL: 0`.

**Dataflows are only translated if Node is installed** (`make setup` does it). Without
it the demo still runs and reports `parser=unavailable` with a translated count of
`None` — they are counted, not silently dropped. No Azure credentials, no AIDP access,
no network.

## Input: a Git export, not an API

The tool reads the folder a Fabric workspace is Git-synced to — the one containing
`<name>.Notebook/`, `<name>.Warehouse/`, `<name>.Lakehouse/` directories. That means no
service principal, no Entra ID app, and no tenant permissions to arrange before trying
it.

## What it translates

### One name for one table

A Fabric table becomes `<catalog>.<item>[_<schema>].<table>`, where the item is
the Lakehouse or Warehouse it lives in and `dbo` — Fabric's default Warehouse
schema — is dropped:

| Fabric | AIDP |
|---|---|
| Lakehouse `SalesLake`, `claim` | `default.SalesLake.claim` |
| Warehouse `AcmeDW`, `dbo.claim` | `default.AcmeDW.claim` |
| Warehouse `AcmeDW`, `postgres_air.t` | `default.AcmeDW_postgres_air.t` |

`--catalog` changes the first part and defaults to `default`. It is **not**
`--namespace`, which is the OCI object-storage namespace used in `oci://`
paths and never appears in a table name. A catalog has to be a single
identifier: `--catalog a.b` would add a name part rather than rename one, so
it is refused.

The item is whichever one the export says owns the table, not whichever one
happens to be nearby. A notebook bound to `SalesLake` that reads `dbo.claim`
gets `default.AcmeDW.claim` when the Warehouse DDL declares that table —
the same name the plan and the generated `CREATE TABLE` use. A Warehouse
object reading a table a *different* Warehouse declares gets that other
Warehouse's name too, rather than the one whose folder the `.sql` came out
of: the export is what knows, and the folder is only the default for a
table the export cannot place. One table, one
name, whichever end of the tool you read it from. A name part that is not a
plain identifier is backticked (`` default.`Sales Lake`.claim ``), which
Spark reads the same way in `spark.table("…")` and in SQL.

**Backticking makes such a name parse; it does not make AIDP accept it.**
AIDP's metastore allows only `[a-z0-9_]` in a schema name, after folding
case — measured on an AIDP cluster (Spark 3.5.0) on 2026-09-30:

```
CREATE SCHEMA IF NOT EXISTS default.`fabric-data-engineering-ws_on-prem-warehouse-test-wh`
  -> MetaException(message:name: fabric-data-engineering-ws_on-prem-warehouse-test-wh.
     Only lower-case characters, numbers and underscores are allowed.)
default.`R2 Sales Lake`, default.`r2Ünï`  -> the same error
default.R2MixedCase                       -> created; SHOW SCHEMAS lists r2mixedcase
```

So `SalesLake` is fine (it becomes `saleslake`), while an item or schema with
a hyphen, a space or a non-ASCII letter yields a schema that cannot be
created. The tool does not rename it — that choice is yours — but every
object that emits one is flagged and graded **REVIEW**, never PASS:
`SQ24_SCHEMA_NOT_ADDRESSABLE` for Warehouse T-SQL,
`NB39_SCHEMA_NOT_ADDRESSABLE` for notebooks and `M34_NAME_NOT_ADDRESSABLE`
for Dataflows, one finding per object naming the schema.

The same folding makes two different Fabric containers one AIDP schema:
Lakehouse `SalesLake` and Warehouse `saleslake`, or item `AcmeDW_sales` and
item `AcmeDW` with schema `sales` (which join to the same string before any
folding). `plan` lists every such pair under `warnings.schema_collisions`,
prints them in its summary, and flags each affected asset
`NM01_AIDP_SCHEMA_COLLISION`, so those objects grade **REVIEW** too.

**Notebooks**

| Fabric | → | AIDP |
|---|---|---|
| `abfss://ws@onelake.dfs.fabric.microsoft.com/LH.Lakehouse/Files/x` | → | `oci://ws_LH_Lakehouse@ns/Files/x` |
| `/lakehouse/default/Files/x` | → | `oci://<lakehouse>@ns/Files/x` |
| `Tables/t`, `Tables/<schema>/t` | → | `default.<lakehouse>.t` |
| `Files/<shortcut>/x` | → | the shortcut's own target, not the lakehouse |
| a bare `"Files/x"` no call takes as a path | → | left as written, flagged |
| `spark.table("claims")` | → | `spark.table("default.<item>.claims")` |
| `display(df)` | → | `df.show()` |

**Warehouse T-SQL** — argument-order and type traps are the reason this exists:

| T-SQL | → | Spark SQL | |
|---|---|---|---|
| `DATEDIFF(day, a, b)` | → | `datediff(b, a)` | **operands reverse** |
| `CONVERT(type, expr)` | → | `CAST(expr AS type)` | **operands reverse** |
| `DATEADD(day, n, d)` | → | `(d + make_interval(0, 0, 0, n))` | DATE stays DATE, TIMESTAMP stays TIMESTAMP (`date_add` truncated a datetime); a string literal becomes `timestampadd`, but a varchar *column* stays a string -- CAST it |
| `CHARINDEX(n, h)` | → | `locate(n, h)` | order preserved; `instr` would reverse it |
| `[ident]`, `ISNULL`, `GETDATE()`, `TOP n`, `SELECT INTO`, `IIF`, `'a' + b`, data types | → | direct | |

Translated naively, each reversal produces **wrong numbers rather than an error**.

### Dataflows (Power Query / M)

Dataflow Gen2 items are **counted with no extra tooling**. Translating them to PySpark
additionally needs Node 18+, because Power Query has no Python parser — Microsoft's own
is TypeScript:

```
cd fabric_aidp/mparse && npm install
```

Without it, `inventory` reports `parser=unavailable` and a translated count of `None`
rather than `0`: the Dataflows are counted, not silently dropped.

Coverage over 39 real Dataflow exports is in [docs/m-coverage.md](docs/m-coverage.md).
Lakehouse and CSV/Web sources are translated; Excel, Snowflake, Databricks and SQL
sources are **named and refused**, never silently skipped. The PySpark a Dataflow
generates is `ast.parse`d before it is written and is **not execution-verified**.
That gate is this translator's own and covers nothing else — see **Limits worth
knowing**.

A query whose steps cannot all be translated is **blocked** — no file is written.
A partly-translated PySpark script would run, and be wrong.

## What it flags rather than guessing

- `notebookutils` / `mssparkutils` — no AIDP equivalent, especially `credentials.getSecret`
- A table reference that is a **shortcut** — its data lives outside the lakehouse, so a
  three-part name would point at nothing
- A table no catalog tier knows
- A table reference that matches **two** catalog entries and does not say which

  These three are checked in Warehouse T-SQL as well as in notebooks. The T-SQL
  path once consulted no catalog at all, so a view over a shortcut or over a
  table nothing declares was rewritten to a confident three-part name and graded
  PASS. Both paths now leave the reference exactly as written: a name that is
  visibly unfinished is safer than one that reads as migrated work. The target of
  a `CREATE TABLE` / `CREATE VIEW` is exempt — a statement that brings a table
  into existence cannot be asked whether it already exists.
- Stored procedures, and the T-SQL control flow this list names: `DECLARE @`,
  `SET @`, a line-initial `IF`, `BEGIN…END`, `BEGIN TRY`/`CATCH`, `WHILE`, `GOTO`,
  `RETURN`, `THROW`, `RAISERROR`, `WAITFOR`, `BEGIN TRANSACTION`/`COMMIT`/`ROLLBACK`,
  cursors, `EXEC`. That is the list, not a summary of one: control flow
  outside it is not detected and the object is reported clean
- `#temp` and `##global` tables — named one by one rather than refused whole, so
  the rest of the object is still translated
- `MONEY` (scale maps, rounding does not), `UNIQUEIDENTIFIER`, `DATETIMEOFFSET`,
  `STRING_AGG`, `MERGE`, `LEN`, `WITH (NOLOCK)`, `OPTION (…)`, join hints
  (`INNER HASH JOIN`), `CROSS`/`OUTER APPLY`

## Status

| Verb | What works | Notes |
|---|---|---|
| `inventory` | ✅ all 6 sources | Git export + bundled inputs |
| `plan` | ✅ | Dependency-ordered; cycles reported, not fatal |
| `migrate` | ✅ notebooks · ✅ Warehouse T-SQL · ✅ shortcuts · ✅ Dataflow M · ✅ pipelines | Writes locally only |
| `verify` | ✅ | Per-asset PASS / REVIEW / SKIP / FAIL |
| `publish` | ✅ setup + notebooks + jobs | Opt-in, dry run by default, never overwrites |

**The target schemas are created for you.** AIDP cannot write a table into a
schema that does not exist, and nothing used to create one: every published
job failed on its first write with `There is no database hive.saleslake`.
`migrate` now writes `setup/catalogs.sql` and an `00_setup_catalogs` notebook
creating every schema the migration writes into, every migrated job runs that
notebook as its first task, and `publish` uploads it first. A schema name AIDP
would refuse is listed rather than emitted. The evidence that this is the whole
missing step is a hand-run on AIDP (issue #50): with the two schemas created by
hand, the published demo job ran green with the right data. The generated setup
was then run on an AIDP cluster (Spark 3.5.0), 2026-10-01, against a catalog
that did not exist: the job's `setup_catalogs` task created the catalog and its
three schemas, a write of 3 rows into one of them read back 3, a write into a
schema nothing created failed with exactly `There is no database`, and a second
run's setup task succeeded again over the existing objects.

**The demo runs end to end on AIDP** once its sample data exists:
`scripts/seed_demo_data.py` puts it there, and RUNBOOK step 7 walks through
it. Measured 2026-10-01: the demo job's four tasks all succeeded, and the
report showed the expected 4 policies of 5 claims.

**Not implemented:** live Fabric REST inventory (Git export only); semantic-model
translation; DAX and KQL; any data movement. A pipeline holding a non-notebook
activity (`Copy`, `Lookup`, `ExecutePipeline`) is refused whole rather than turned
into a job that would do less than the original.

## Limits worth knowing

**What PASS means.** PASS = *translated, and no known issue was detected*. It is **not
execution-verified**: `verify` does not parse or run the generated artifacts, so a
construct none of the rules cover is reported clean. Treat PASS as "nothing the tool
knows about is wrong here". A low REVIEW count is not by itself evidence of a clean
migration.

**Only Dataflow output is parse-checked, and that is not the same as verified.**
The Dataflow translator runs `ast.parse` over the whole script it generates and
refuses to write one that does not compile. No other translator has an equivalent
whole-file gate, and the notebook one should not: a translated notebook is *Fabric
notebook source*, which carries `%%sql` cell magics, and those cells are left
byte-for-byte alone on purpose — in a Fabric notebook `%%sql` is Spark SQL, and
applying the T-SQL rules to it would corrupt valid code. Measured on this tree: of
the six `.py` files the synthetic Acme estate produces, `notebooks/06_Sql_Summary.py`
— status `ok` — is rejected by `ast.parse` at the bare `%%sql` on line 15, and that
is the artifact behaving correctly. Over the whole staged demo estate, which adds
the vendored real-world notebooks, **17 of 31 emitted `.py` files do not parse as
Python** — which is how large a hole a whole-file parse gate here would punch. So read the sentence above as being about
Dataflows and nothing else.

**A Git export cannot name a Dataflow's lakehouse.** The `lakehouseId` a Power Query
navigation uses is a different identifier from the `logicalId` in a Lakehouse item's
`.platform`, and the export contains no mapping between them. So every Lakehouse table
read in a translated Dataflow is **flagged** for a human to confirm the database, even
though the table name itself resolves cleanly.

**The demo estate is a regression net, not a coverage gate.** It was authored
alongside the rules, so it can prove the rules that fire still fire — and it does.
It cannot prove the rest are right, because it never reaches them. Measured on this
tree: the 110 assets `make demo` migrates fire **57 of the 251 distinct rule ids**
the translators define, and the synthetic Acme estate on its own fires **37**. So
a green demo says nothing about the other 194, and it cannot discover a construct
that neither the rules nor the fixture anticipated. Running a real Fabric export
through `inventory` is worth more than any amount of fixture work.
`tests/test_docs_claims.py` re-takes both counts and fails if this paragraph and
the code disagree.

**Lakehouse tables are not in a Git export.** Fabric tracks lakehouse metadata and
shortcuts, but not tables or Spark views. Table existence is therefore resolved from
warehouse DDL, shortcuts, an optional `--tables-csv` list, and — last — inference from
notebook writes. An inferred table produces an `info` finding naming the notebook that
creates it, so a guess never reads as a fact.

**Shortcut tracking is opt-in.** If a lakehouse has it disabled, the export contains no
shortcuts and the tool reports `unknown`, never `0`.

**Seven of the eight shortcut shapes have never been seen in a real export.** The
scanner recognises `oneLake`, `amazonS3`, `adlsGen2`, `googleCloudStorage`,
`azureBlobStorage`, `s3Compatible`, `dataverse` and `oneDriveSharePoint`. Only
`oneLake` appears in a Fabric Git export vendored here; `amazonS3` and `adlsGen2`
appear in the demo estate, which this project authored from the same Microsoft
documentation the other field names came from. So the code path is exercised for
three and the *payload shape* is confirmed for one. Each row in
`fabric_aidp/inventory/lakehouse.py` says which it is, and
`tests/test_shortcut_to_oci.py::ShapeProvenanceTests` keeps those notes true in
both directions. Closing the remaining five needs a live-tenant export carrying an
ADLS, GCS or S3-compatible shortcut; there is no way to write one here, and a
fixture written from the documentation would only restate the guess.

## Testing

```bash
make test
```

or, without the Makefile:

```bash
PYTHONPATH=. python3 scripts/run_stress_tests.py --quiet
```

**Skips are expected, and `OK (skipped=N)` is a pass.** There are exactly four
reasons a test here skips, and none of them is a problem with your machine:

- **`pyspark` and a JVM are not installed** — they are deliberately not
  dependencies of this tool, so the handful of tests that execute generated
  Spark skip. This is the largest group.
- **Node is not installed** — the Power Query tests need the parser
  `make setup` installs.
- **A fuller corpus is not present** — `FABRIC_SIBLING_FIXTURES` and
  `FABRIC_M_CORPUS` point at larger private sets. The 78 real third-party
  artifacts that *do* ship run for everyone.
- **A distribution cannot be built here** — the packaging tests build a real
  wheel and a real sdist and read what is inside. The sdist needs `setuptools`
  **at the version `pyproject.toml`'s `build-system` requires**, because an
  older one ignores `[project]` and builds this project as `UNKNOWN-0.0.0`; the
  venv `make setup` creates has no setuptools at all. `pip wheel` fetches its
  own copy, so the wheel needs network instead, and a stalled fetch counts.
  All of those degrade to a skip, and `MANIFEST.in` is checked statically
  either way. An sdist that builds but carries the wrong root directory is the
  repository's problem, not the machine's, and fails loudly.

`tests/test_docs_claims.py` checks that list against the skip reasons actually
written in the suite, **and checks the count above against the length of the
list**, so a fifth reason cannot appear here unannounced. The second half was
missing, and a fourth reason duly appeared under a sentence still saying three.

## Compatibility

- CLI: Python 3.9–3.14, **no required runtime dependencies**.
- Optional MCP server: Python 3.10+, `pip install -e '.[mcp]'`, pinned `mcp>=1.2,<2`.
- Generated workloads target the AIDP runtime contract: Spark 3.5, Python 3.11, Java 17.

### What has actually been run on AIDP

`PASS` means translated with no known issue detected, **not**
execution-verified, and that is still the right reading for the estate. But
on 2026-09-29 a slice of the demo's own output was published to a live
workspace and executed, so some of it is no longer only a claim about the
translator. What was run, and what it settles:

| | result |
|---|---|
| `publish --apply` of the whole demo output | **31/31 notebooks uploaded, 1/1 job created, 0 refused** |
| `--prefix` rewriting | job task paths came back `/Workspace/<prefix>/<name>.ipynb`, correct prefix and extension — the on-disk `.job.json` holds the un-prefixed `.py` form, and publish rewrites it |
| the translated pipeline's `dependsOn` + `runIf: ALL_SUCCESS` | `RunIngest` FAILED, and `RunAggregates` / `RunReport` came back `UPSTREAM_FAILED`, "Skipped due to upstream failures" — downstream tasks correctly did not run |
| the demo's `%%sql` notebook (`06_Sql_Summary`) | **SUCCESS**, with `default.AcmeDW.claim` resolving — so the three-part naming convention addresses a real AIDP table |
| a `notebook-content.sql` T-SQL notebook, end to end | `SELECT TOP 5 …` translated to `… LIMIT 5`, published with `%sql` prepended, and **SUCCESS** |
| did the `%sql` cell return *data*, or merely not error? | it returned **2 groups**, the correct answer for the 3 seeded rows — written out as a marker table so the count, not the exit status, is the evidence |

**What this does not cover, and it is most of it.** The other 27 notebook
bodies, all 22 warehouse `.spark.sql` artifacts and all 4 Dataflow scripts
were published but not executed. No OneLake path was exercised, because the
demo's `oci://` buckets do not exist in that tenant — which is why
`RunIngest` failed, and that failure is the expected one rather than a
finding. Every object created for this was deleted afterwards.

So: the publish path, the job structure, the SQL-cell magic in both
directions, and the table naming are execution-verified. The translated
bodies, in the main, are not.

### The runtime the Spark claims were measured against

Measured on the AIDP cluster `fabricTest`, 2026-09-29, by running a probe
notebook as a `NOTEBOOK_TASK` and reading back marker tables:

| setting | value |
|---|---|
| `spark.version` | **3.5.0** |
| `spark.sql.ansi.enabled` | **false** |
| `spark.sql.ansi.enforceReservedKeywords` | false |
| `spark.sql.caseSensitive` | false |
| `spark.sql.sources.default` | **delta** |
| `spark.sql.legacy.createHiveTableByDefault` | false |

Most Spark measurements in this repo's comments were taken locally on
**pyspark 4.2.0**, where `ansi.enabled` defaults to **true**. Every
behavioural claim they rest on was re-run on a local 3.5.0 against the
settings above. The bulk is identical — every T-SQL control-flow rejection,
`SET QUOTED_IDENTIFIER`, `SELECT … INTO`, `INSERT` without `INTO`,
`ALTER TABLE … ADD` needing `COLUMNS`, `SELECT 1 / RETURN` aliasing a column
`RETURN`, a type-named column accepted bare, a hyphen safe in a column
reference and fatal in an expression string, `withColumn("X")` replacing
`x`, and `VARCHAR(n)` enforced on write but not on cast.

**Four differ, and each is recorded where the decision is made:**

1. `PRIMARY KEY` / `UNIQUE` / `CHECK` / `REFERENCES` in a `CREATE TABLE`
   parse on 4.2.0 and fail at *analysis*; on 3.5.0 they **do not parse**.
   Rejected either way, so the rule that removes them is unaffected.
2. `COLLATE UTF8_LCASE` is accepted on 4.2.0 and a parse error on 3.5.0 —
   collations arrived in Spark 4.0. Removing a T-SQL collation
   unconditionally is *more* clearly right on the target, not less.
3. `withColumnsRenamed` returns the same wrong swap on 4.2.0 and raises on
   3.5.0. Either way it is not the fix, which is why `_m_rename` uses `toDF`.
4. A **hyphenated table name** is rejected on both, with different error
   classes. The cluster confirmed the rejection directly.

None of the four changed a decision. All four had the wrong measurement
written beside them, which is the thing worth not leaving in place.

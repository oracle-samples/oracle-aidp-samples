# Runbook — Microsoft Fabric to Oracle AIDP, end to end

The whole path, from a Fabric workspace you own to jobs running on an AIDP
cluster. Seven steps. The tool accounts for seconds of it; getting the export
out of Fabric is the slow part, and it is done once.

Every command here has been run, and every block of output is real rather
than illustrative. Steps 2 to 6 show the bundled demo estate — substitute
`--fixture demo` for `~/fabric-export` in step 2 and you get those numbers
exactly; `tests/test_docs_claims.py` re-runs the pipeline and fails if they
drift. Your own export prints different numbers in the same shape. Only the
`--apply` transcript in step 6 cannot be reproduced offline: it came from a
live workspace, and the job key it returned has been redacted.

**Two things to know before you start.**

- **Only `publish` writes anywhere.** Steps 2–5 are entirely offline and need
  no credentials at all. You can run them against a customer's export on a
  laptop with no cloud access.
- **PASS does not mean it runs.** It means translated, with no known issue
  detected. Nothing here parses or executes the generated artifacts, except
  where you choose to in step 7.

---

## Step 0 — one-time setup

```bash
git clone <this-repository> fabric-aidp-migrator
cd fabric-aidp-migrator
make setup
```

`make setup` creates `.venv`, installs the CLI, and runs `npm install` in
`fabric_aidp/mparse/`. It stops with the install command if `python3` or `npm` is missing.

`make` needs a POSIX shell: it hardcodes `.venv/bin/python` and `make demo`
runs `demo.sh`. On Windows use WSL or Git Bash, or run the steps by hand —
see **Quick start** in [README.md](../README.md).

Needed: Python 3.9+, Node 18+. To publish (step 6) you also need
`pip install aidp-cli` and an OCI profile that can reach your AIDP instance.

Check it worked:

```bash
make demo
```

Expect `FAIL: 0` at the end. That runs the whole pipeline against a bundled
estate — no Fabric tenant, no credentials, no network.

---

## Step 1 — get the Fabric workspace out, as a Git export

**This is the only step that happens in Fabric, and the only manual one.**

In the Fabric portal:

1. Create a **private** GitHub repository for the export.
2. Open the workspace → **Workspace settings** → **Git integration**.
3. Connect it to that repository and a branch.
4. **Commit all** — this writes the workspace into the repo as directories.

Then clone it locally:

```bash
git clone https://github.com/<you>/<workspace-export>.git ~/fabric-export
```

You should see one directory per item:

```
01_Ingest_Claims.Notebook/    AcmeDW.Warehouse/     SalesLake.Lakehouse/
Daily_Claims.DataPipeline/    Agents_Dim.Dataflow/  Claims_Model.SemanticModel/
```

**Why a Git export and not the REST API.** No service principal, no Entra ID
app registration, no tenant permissions to arrange. A workspace admin can do
it in the portal in two minutes, and what lands is exactly what the tool reads.

**What a Git export does not contain**, so you are not surprised later:

- **Lakehouse tables.** Fabric tracks lakehouse metadata and shortcuts, not
  tables. The tool resolves table existence from Warehouse DDL, shortcuts, an
  optional CSV, and last of all inference from notebook writes.
- **A lakehouse GUID → name mapping.** The `lakehouseId` a Dataflow navigates
  by is a different identifier from the `logicalId` in a Lakehouse item's
  `.platform`. So Dataflow table reads are flagged for a human to confirm the
  database, even when the table name resolves cleanly.
- **Notebook outputs.** Never committed by Fabric, and not needed.

---

## Step 2 — inventory (read-only)

```bash
source .venv/bin/activate
fabric-aidp inventory ~/fabric-export -o out/inventory.json
```

Reads the export and writes a manifest. Touches nothing else.

```
  notebook       notebook_count=6, parse_error_count=0
  warehouse      warehouse_count=1, unreadable_object_count=0,
                 unterminated_object_count=0
  lakehouse      lakehouse_count=2, shortcut_count=3, external_shortcut_count=2,
                 coverage_unknown_count=1
  pipeline       pipeline_count=1, activity_count=3
  semanticmodel  semantic_model_count=1
  dataflow       query_count=4, pipeline_count=2, helper_count=1,
                 parameter_count=1, unread_member_count=0, dataflow_count=1,
                 parser=available, metadata_unreadable=0, translatable_count=2
    ArchiveLake.Lakehouse — shortcuts: not_tracked · data access roles: not_tracked
    SalesLake.Lakehouse — shortcuts: tracked (3 found) · data access roles: not_tracked

  resolved catalog:
      10  warehouse_ddl
       2  shortcut
       0  supplied
       3  notebook_inferred
    note: 3 table(s) inferred from notebook writes — name only, not confirmed to exist
```

**Read the two lakehouse lines.** `shortcuts: not_tracked` means Fabric was not
recording shortcuts for that lakehouse, so the export contains none and the
estate may hold shortcuts this scan cannot see. `coverage_unknown_count` counts
those lakehouses. It is never reported as zero shortcuts.

**Two different "unreadable" counts, and they mean different things.** A file
inside an item that *did* read — a `.sql` in a Warehouse whose encoding the
scan cannot decode, a Dataflow's metadata, a notebook that will not parse — is
counted on that source's own line: `unreadable_object_count`,
`metadata_unreadable`, `parse_error_count`. Each such object still reaches the
plan as an asset of its own carrying the decode error, and `migrate` refuses it
rather than translating an empty string. An **item directory** that would not
read at all is a separate block further down, printed as *"N item director(ies)
could not be read and were not scanned"* — nothing inside one of those was
looked at, so the plan holds a single `unreadable.<path>` asset for the whole
directory. In the manifest JSON these are the per-source summary counts and the
top-level `unreadable_items` list respectively. `unreadable_items` is item
directories only: an empty list does not mean every file read.

**Read the catalog block.** `notebook_inferred` means the tool believes a table
exists because a notebook writes to it — a name, not a confirmed object. If
that count is high, supply the real list:

```bash
fabric-aidp inventory ~/fabric-export --tables-csv tables.csv -o out/inventory.json
```

`tables.csv` needs a `table` column; `column` and `type` are optional.
Header case and spacing do not matter — `Table,Lakehouse` is read the same
as `table,lakehouse`. A file that names no tables is refused rather than
loaded as an empty catalog, so `--tables-csv` never silently does nothing.

**If `parser=unavailable`:** Node is not installed, so Dataflows are counted
but not translated, and `translatable_count` is `None` rather than `0`. Run
`cd fabric_aidp/mparse && npm install`.

Useful flags: `--sources notebook,warehouse` to scan a subset, `-v` for
per-item detail.

---

## Step 3 — plan

```bash
fabric-aidp plan out/inventory.json -o out/plan.json \
    --namespace <your-oci-namespace> --catalog <your-aidp-catalog>
```

Turns the manifest into a dependency-ordered list of assets. `--namespace` and
`--catalog` are different things, and both matter:

- `--namespace` is your OCI object-storage namespace; it appears only in
  generated `oci://` paths, never in a table name. Leave it out and you get
  `<your-oci-namespace>` as a visible placeholder, which is fine for review
  and wrong to run. `plan` says so in its summary, and every artifact that
  ends up carrying the placeholder is flagged `NS01_NAMESPACE_PLACEHOLDER`
  and graded **REVIEW**, never PASS — so a placeholder target cannot leave
  the migration looking clean.
  **Find it with `oci os ns get`.** It is a generated string, *not* your
  tenancy name, and the two are easy to confuse because the console shows
  the tenancy name everywhere. Pass the tenancy name and every `oci://` path
  points at a namespace no bucket can live in: each read fails at run time
  with `BucketNotFound … does not exist in the namespace '<tenancy name>'`,
  which looks like a missing bucket and is a wrong namespace.
- `--catalog` is the AIDP catalog table names are built under — the first part
  of `<catalog>.<item>.<table>`. It defaults to `default`. See **One name for
  one table** in [README.md](../README.md#one-name-for-one-table) for the
  full naming rule. AIDP's metastore creates a schema only if its name is
  `[a-z0-9_]` after folding case (measured on the cluster 2026-09-30), so an
  item or schema with a hyphen, a space or a non-ASCII letter is not renamed
  but flagged — `SQ24_SCHEMA_NOT_ADDRESSABLE`, `NB39_SCHEMA_NOT_ADDRESSABLE`
  or `M34_NAME_NOT_ADDRESSABLE` — and the object grades **REVIEW**. Rename the
  Fabric item, or pick the AIDP schema by hand, before running it.
  Two containers that fold onto one AIDP schema (`SalesLake` and
  `saleslake`, or `AcmeDW_sales` and `AcmeDW` + `sales`) are listed in the
  plan summary and under `warnings.schema_collisions`, and each affected
  asset is flagged `NM01_AIDP_SCHEMA_COLLISION`.

```
plan 20260925T154008Z-254a9ab21ca8
  total assets: 28
  by target type:
       8  aidp_table          6  aidp_notebook       3  aidp_dcat_external_table
       3  aidp_sql_object     2  aidp_pyspark_job    2  aidp_schema
       2  aidp_view           1  aidp_job            1  aidp_semantic_model
```

Ordering is topological: notebooks before the pipelines that run them.
A dependency cycle is **reported, not fatal** — the cycle is named and the
assets outside it still get ordered.

---

## Step 4 — migrate (still offline)

```bash
fabric-aidp migrate out/plan.json -o out/migrated
```

Writes every artifact locally. Nothing is sent anywhere.

```
# plan=20260929T161135Z-c23130dff218  mode=offline  filter=all

  REVIEW dataflow.Agents_Dim.dim_agent            changes=5 flags=3
  OK     notebook.01_Ingest_Claims                changes=4 flags=0
  REVIEW warehouse.AcmeDW.procedure.dbo.sp_load_claims changes=0 flags=1
  OK     warehouse.AcmeDW.view.dbo.v_open_claims  changes=8 flags=0
  ...
# done. ok=16  needs_review=10  blocked=1  planned=1  error=0
```

Four of the 24 rows, to show the shape. A Warehouse row carries the item it
came out of — `warehouse.AcmeDW.view.dbo.v_open_claims`, not
`warehouse.view.dbo.v_open_claims` — because two warehouses in one workspace
can both define `dbo.CurrentDate`, and without the item they collide.

**`blocked=1` is not an error.** It is one object the tool refused to translate
rather than translate partly. Read those first — they are the work that does
not go away.

What lands in `out/migrated/`:

| Directory | What |
|---|---|
| `notebooks/` | `.py`, Fabric cell structure preserved |
| `warehouse/` | `.spark.sql` per table, view and procedure |
| `dataflows/` | `.py` PySpark, one per translated Power Query query |
| `jobs/` | `.job.json`, an AIDP workflow job per translated pipeline |
| `shortcuts/` | `.md` describing each external location |
| `setup/` | `catalogs.sql` and the `00_setup_catalogs` notebook: create every AIDP schema the migration writes into |
| `report.html` | **open this first** — every asset, its findings, and a link to the file it became |
| `report.md`, `report.json` | the same, as text and as data |

**Setup runs first, and you do not have to remember it.** AIDP cannot write a
table into a schema that does not exist, and before this step existed every
first run died with `There is no database hive.saleslake`. `migrate` now writes
`setup/catalogs.sql` — one `CREATE SCHEMA IF NOT EXISTS` per schema the
migration writes into — and the same statements as the `00_setup_catalogs`
notebook. **Every migrated job runs that notebook as its first task**, and
`publish` uploads it first. Every statement is `IF NOT EXISTS`, so a second run
changes nothing. Run `setup/catalogs.sql` yourself only for what no job runs:
a warehouse script, a Dataflow script, a notebook run by hand.

A schema AIDP would refuse — a Fabric item name with a hyphen, a space or a
non-ASCII letter — is **left out of the setup and listed** in the migrate log
and in `report.md`, rather than emitted to fail. It would stop nothing else, but
no table under it can exist until the item is renamed.

`--filter <slice>` migrates one slice, and takes any of `notebook`,
`warehouse`, `lakehouse`, `pipeline`, `semanticmodel`, `dataflow` — the same
six names `inventory --sources` and `verify --filter` use. There is no mode
flag: `--demo` used to be required, then was accepted and ignored, and has been
removed.

If a Dataflow read or write is flagged "Lakehouse `<guid>` is not named anywhere
in this export", re-run `plan` with `--lakehouses <csv>` — two columns, `id` and
`name`. A Fabric Git export pairs a lakehouse GUID with its name only inside a
notebook bound to that lakehouse, so with no such notebook nothing but you can
supply it.

---

## Step 5 — verify and review

```bash
fabric-aidp verify out/migrated
```

```
  PASS:   16
  REVIEW: 11
  SKIP:   1
  FAIL:   0
```

**What each verdict means, and what to do:**

| | Meaning | Your move |
|---|---|---|
| **PASS** | Translated, nothing the tool knows about is wrong | Spot-check. It has not been run. |
| **REVIEW** | Translated, but something needs a human | Read the finding; most are one line. |
| **SKIP** | No translator for this asset type | Semantic models. Expected. |
| **FAIL** | The tool itself broke | Should be 0. If not, it is a bug — report it. |

`verify`'s REVIEW count is `migrate`'s `needs_review` **plus its `blocked`**:
migrate reports a refused object under its own `blocked` status and verify
folds that into REVIEW. Both are counting the same objects. Here that is
10 + 1 = 11; over the full bundled demo estate it is 46 + 46 = 92.

`FAIL: 0` is the number that matters. A **blocked** object counts as REVIEW,
not FAIL: the tool refused an object it could not translate faithfully, which
is it working, not failing.

`verify --filter <slice>` narrows the listing to one slice, but never the
verdict: a FAIL from another slice is still counted, still printed with the
note `shown despite --filter <slice>`, and still exits 1. A filter cannot make
a broken migration look clean.

Then read the report:

```bash
open out/migrated/report.html      # every asset, linked to what it produced
less out/migrated/report.md        # or the same as text
```

**The refusals worth understanding**, because they look like gaps and are not:

- A **Dataflow** using Excel, Snowflake, Databricks or SQL Server is named and
  refused. Out of scope by design, never silently skipped.
- A **pipeline** holding a `Copy`, `Lookup` or `ExecutePipeline` activity is
  refused *whole*. A job containing only its notebook tasks would run, skip the
  Copy that fed them, and be wrong.
- A **Dataflow query** whose steps cannot all be translated is refused whole,
  for the same reason. No partial file is written.
- **`%%sql` cells are left byte-for-byte alone.** In a Fabric notebook that is
  Spark SQL, not T-SQL, and applying the T-SQL rules to it corrupts valid code.
  T-SQL constructs found there are flagged (`NB24`) instead. `publish` does
  change one thing: the magic. AIDP runs `%sql` and silently skips `%%sql`
  (no error, and the job still reports SUCCESS), so the uploaded `.ipynb`
  spells it `%sql`. `%%tsql` goes the same way — by then the translator has
  converted that body to Spark SQL, so the T-SQL label is stale and AIDP has
  no `%tsql` — and either is matched whatever its case.
  **One caveat, stated because it is not settled.** The probes behind this
  all put the SQL on the magic line (`%sql SELECT 1`). A real migrated cell
  has the magic alone on line 1 with the SQL below, and nobody has run *that*
  shape on AIDP. It works if AIDP's `%sql` takes the whole cell; if `%sql` is
  an ordinary IPython line magic, lines 2+ run as Python and the cell fails
  with a `SyntaxError` instead. Failing loudly still beats today's silent
  SUCCESS, but if you have a live workspace, publish and run
  `06_Sql_Summary` and the question is answered.

---

## Step 6 — publish to AIDP

The only step that writes anywhere. **Always dry-run it first.**

```bash
pip install aidp-cli
export AIDP_INSTANCE_ID=ocid1.aidataplatform.oc1.<region>.<...>

fabric-aidp publish out/migrated --prefix <yourname> --cluster-key <cluster-uuid>
```

```
dry run -- nothing was sent. 7 notebook(s), 1 job(s) would be created, 0 refused.
re-run with --apply to publish.

would upload 7 notebook(s) and create 1 job(s); 0 refused.
    /Workspace/yourname/00_setup_catalogs.ipynb
    /Workspace/yourname/01_Ingest_Claims.ipynb
    /Workspace/yourname/02_Build_Aggregates.ipynb
    ...
    job yourname_Daily_Claims  (4 tasks)

nothing was sent. add --apply to publish.
```

(The dry run says "nothing was sent" twice, from the publisher and from the
CLI wrapping it. Cosmetic, and it is what the command prints.)

Read that list. When it is right:

```bash
fabric-aidp publish out/migrated --prefix <yourname> --apply \
    --workspace-key <workspace-uuid> \
    --cluster-key   <cluster-uuid> \
    --profile <oci-profile> --auth api_key
```

```
  uploaded /Workspace/yourname/01_Ingest_Claims.ipynb
  ...
  created  job yourname_Daily_Claims (<job-key-assigned-by-aidp>)

uploaded 6/6 notebook(s); created 1/1 job(s); 0 skipped, 0 failed or refused.
```

**What publish guarantees:**

- **Never overwrites.** A notebook path or job name that already exists is
  skipped (`skipped … already exists`). Re-running is safe: a second run
  changes nothing and exits 0. A job whose notebook was skipped because
  something else is already at that path is **refused** and the run exits 1 —
  it would otherwise run someone else's notebook. Use a new `--prefix`, or
  delete what is there, to publish over it.
- **`--prefix` namespaces everything** — paths and job names — so two people
  publishing into one workspace do not collide. `--apply` refuses to run
  without it (exit 2, nothing sent); a dry run without it still prints the
  plan, with a warning. It must be a letter followed by letters, digits or
  underscores, the only job names AIDP accepts.
- **Publishes only what `report.json` lists** as `ok` or `needs_manual_review`,
  and refuses a migration directory left half-written by an interrupted run.
- **Refuses** a job whose notebooks this migration did not produce, a job with
  no `--cluster-key`, and two pipelines that map to one job name, rather than
  creating one that cannot run. Any refusal makes `--apply` exit 1.
- **Converts every notebook before sending any.** One that cannot become an
  `.ipynb` — a REVIEW artifact with no `# Fabric notebook source` header, or
  unreadable `# META` — is listed as `REFUSED  notebook …` in the dry run,
  never uploaded, and takes any job that runs it down with it.
- Notebooks upload as `.ipynb`, because a `.py` lands as a *file* and a
  `NOTEBOOK_TASK` cannot run a file.
- **The conversion is not quite byte-for-byte, and this is the whole list of
  what it changes.** `outputs` and `execution_count` are cleared, because
  they record what a *Fabric* session computed before this tool rewrote the
  code; and a SQL cell's leading `%%sql` / `%%tsql` becomes `%sql`, because
  AIDP skips the Fabric spelling without erroring (see step 5). Nothing else
  differs. So the first line of a published SQL cell is **not** the first
  line of the artifact `verify` graded and you reviewed — if you diff the two
  and find that, it is this, and it is deliberate. One consequence worth
  naming: a migrated `DROP` or `CREATE` that used to be a silent no-op on
  AIDP will now actually run on the next `--apply`.

Credentials come from flags, from `AIDP_WORKSPACE_KEY` / `AIDP_CLUSTER_KEY` /
`AIDP_INSTANCE_ID` in the environment, or from a `.env` in the directory you
run in. **None are stored in this repository**, and a `.env` is gitignored —
keep it that way.

**A `.env` is read by every verb, not just `publish`.** Only keys beginning
`AIDP_`, `OCI_` or `FABRIC_` are taken from it, and only when the variable is
not already set, so a stray `PATH=` or `NODE_OPTIONS=` in that file cannot
redirect the `aidp` CLI or the Node parser. `plan` reads `OCI_NAMESPACE` the
same way — that is why a namespace set once in `.env` reaches steps 3 and 4
without a flag.

---

## Step 7 — run one thing, on purpose

Nothing so far has executed any generated code. Before declaring a migration
done, run one job by hand in the AIDP console and read the output.

Pick the simplest translated notebook, not the largest. What you are testing
is whether the *environment* matches what the translation assumed — paths,
table names, cluster libraries — and a small notebook tells you that faster.

Expect to find path and catalog problems here rather than translation
problems. That is the normal shape of a migration: the SQL and PySpark are
usually right, and the object they point at is usually not there yet.

### The order things have to exist in

A migrated job reads what earlier steps produced, and nothing in the job
produces them. On AIDP, before a job can run green:

1. **Schemas** — `setup/catalogs.sql`. Every migrated job already runs it as
   its first task, so this one takes care of itself for jobs.
2. **Warehouse tables** — the `warehouse/*.spark.sql` scripts. No job runs
   them: a notebook that reads a warehouse table fails with table-not-found
   until its script has been run.
3. **Data** — the files under each `oci://` path, and the rows in the
   warehouse tables. This tool moves no data; whatever moves your data has to
   have run.
4. **Then the jobs.**

### Running the bundled demo end to end

The demo is a synthetic estate, so steps 2 and 3 have nothing real to load.
`scripts/seed_demo_data.py` supplies them, reading every location from the
migration's own output: it runs the migrated `dbo.claim` warehouse script,
writes 20 sample claim rows to the bucket path the ingest notebook reads, and
inserts rows into the table. Spark on the cluster writes the data, so it
needs only the `aidp` CLI (and the `oci` CLI to create the bucket).

```bash
NS=$(oci os ns get --query data --raw-output)   # the namespace, not the tenancy name
fabric-aidp plan out/inventory.json -o out/plan.json --namespace "$NS"
fabric-aidp migrate out/plan.json -o out/migrated
fabric-aidp publish out/migrated --prefix <you> --cluster-key <uuid> --apply
python scripts/seed_demo_data.py out/migrated --prefix <you> \
    --workspace-key <key> --cluster-key <uuid> \
    --create-bucket --compartment <compartment-ocid>
```

Then run the `<you>_Daily_Claims` job. Measured on AIDP, 2026-10-01: all four
tasks **SUCCESS** — setup, ingest, aggregate, report — the ingest read the 20
seeded rows and the report showed 4 policies of 5 claims each; the
`06_Sql_Summary` notebook also succeeded against the seeded warehouse table.

The seed **keeps existing data**: if the bucket path already holds rows, or
the table already exists, it says so and leaves them alone. Pass
`--overwrite` to replace them. `--dry-run` writes the seed notebook locally
and contacts nothing. If a run stops part-way, run it again: it reuses the
`<you>_seed_demo_data` job it created rather than failing on the taken name.

**In a shared tenancy the demo bucket is shared too.** Its name,
`AcmeWS_SalesLake_Lakehouse`, comes from the Fabric path in the demo's
notebook, not from you, so everyone running the demo in one tenancy reads the
same bucket. That is safe as long as the bucket is treated as read-only
input: the demo job only *reads* it, and the seed only *writes* it when it is
empty. Each person's results are their own as long as each uses their own
`--catalog` and `--prefix` — the job writes its tables into the catalog, not
the bucket. What breaks other people's runs is `--overwrite` (the seed warns
before doing it) or deleting the bucket; do neither in a shared tenancy
unless you created it. A `--bucket` option would not help: the migrated
notebook reads the bucket its path names, so a differently named bucket is
one the job never reads.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `dataflow … parser=unavailable` | Node missing | `cd fabric_aidp/mparse && npm install` |
| `translatable_count: None` | Same | Same. `None` means unknown, not zero. |
| `error: no such migration directory` from publish | Wrong path | Point at the dir `migrate` wrote, the one with `report.json` |
| `publish` refuses a job: "no cluster key" | Tasks cannot run without one | Pass `--cluster-key` |
| `aidp ... 406 NotAcceptable` | Using the **console** URL as the endpoint | Drop `AIDP_ENDPOINT`; the CLI default data plane is correct. The `https://…datalake.oci.oraclecloud.com` link from the portal is the UI, and serves HTML. |
| `repository not found` cloning the export | Private repo, no access | Add the person as a collaborator |
| Tests skip | No Node, or no dataflow corpus | Both expected. `OK (skipped=N)` is a pass. |

---

## If you only have ten minutes

```bash
make setup && make check
```

Runs setup, the full offline pipeline against the bundled estate, and the test
suite. Ends with `FAIL: 0` and `OK`. No Fabric tenant, no AIDP account.

# AIDP runtime constraints for generated notebooks

Generated notebooks run on an AIDP Spark cluster, which differs from a
generic Spark environment in ways that break otherwise-valid code. A
notebook can be flawless PySpark and still fail here. Every constraint
below is observed AIDP cluster behaviour, not something drawn from
documentation, and each one exists because it broke something first.

Enforced (checkable in `tests/test_aidp_output_validity.py`) constraints
are marked **[enforced]**. Constraints marked **[recorded]** are real but
not fully checkable from the generator's current inputs — see the note
under each.

## Library availability

- **[enforced] `matplotlib` and `seaborn` are NOT installed** on the
  cluster image. A generated notebook that imports either fails at
  import time. The generators must never emit plotting code that
  imports them.
- **[enforced] The library installer shadows image packages.** If a
  notebook needs pandas, any `pip install` of it must pin
  `pandas==2.2.3` — an unpinned install silently replaces the image's
  own pandas version and breaks Spark interop. (No current generator
  emits a pandas install at all; this check guards against a future
  one being added unpinned.)
- **[enforced] `matplotlib.use("Agg")` poisons the kernel** for the rest
  of the session. If plotting is ever added, it must use
  `%matplotlib inline`, never `use("Agg")`.
- **[recorded] Cluster nodes may have no PyPI egress** in some
  deployments, so an in-cell `pip install` cannot be assumed to work.
  Prefer cluster-level library installation over notebook-level
  `pip install`. Checked negatively: no generator currently emits any
  `pip install` cell, so today's fixtures trivially satisfy this: the
  check would fire the moment one is added without a comment
  acknowledging it may not have egress.

## Table addressing

- **[enforced] Tables are addressed by 3-part catalog name**
  (`catalog.schema.table`) via `spark.table()` / `saveAsTable()`. No
  JDBC URLs and no credential literals (`password=`, `pwd=`,
  `secret=`) belong in a generated cell — see the design notes.
- **[recorded] `DeltaTable.forName()` is Delta-only.** Against an
  external (ADW-backed) catalog it does not apply — merges generated
  for UPSERT/SCD1/SCD2 targets will fail if the target catalog is not
  a managed Delta table. Generated upsert/SCD2 code must eventually
  branch on target catalog type before choosing a write path; that
  branching logic is out of scope for this task (the generator has no
  signal today about which catalog type a given target table lives
  in). Until that branching exists, every `DeltaTable.forName(...)`
  call site in `notebook_generator.py` carries an inline
  `# NOTE: ... is Delta-only ...` comment pointing back at this file,
  so the limitation is visible in the generated notebook itself, not
  just in this doc. The validity suite checks that the comment is
  present anywhere the call is emitted — it does not (yet) check that
  a real branch exists, since none does.

## Cluster libraries the notebooks depend on

- **[recorded] `infa_compat` must be installed on the cluster.** Every
  generated notebook imports it and asserts `infa_compat.__version__`
  in its second cell (`notebook_generator._infa_compat_header_cell`).
  Build it with `pip wheel engine/ -w dist/` and install the wheel as a
  cluster library; there is no in-notebook `pip install` (see "no PyPI
  egress" above). A cluster without it fails every notebook before the
  first read, which is the intended loud failure.
- **[recorded] `python-oracledb` and the Oracle JDBC driver** are needed
  only for `--target-catalog-type adw` notebooks (driver-side MERGE,
  Spark JDBC staging). Not checked by the validity suite.
- **[verified 2026-09-24, us-ashburn-1] The workspace is mounted on the
  driver at `/Workspace`.** `os.path.isdir("/Workspace/Migrated")` is true
  inside a notebook, and `sys.path.insert(0,
  "/Workspace/<folder>/infa_compat-0.1.0-py3-none-any.whl")` imports the
  package straight from the uploaded wheel (Python's zip importer). That
  is an alternative to a cluster-library install when the cluster is
  shared and its library list should not be touched; the notebook's
  version assertion still applies.
- **[verified 2026-09-24] Cluster runtime:** Spark 3.5.0, Python 3.11.13,
  `spark.sql.ansi.enabled` unset (i.e. off), `spark.sql.storeAssignmentPolicy`
  = ANSI by default, `delta` Python module importable, current catalog
  `spark_catalog`. pyspark 3.5's `array_position(col, value)` takes a
  Python literal for `value`; a Column raises "Column is not iterable"
  (the reason `INDEXOF` is emitted as a CASE chain).
- **[verified 2026-09-25] Job and task parameters reach a notebook task
  through `oidlUtils.parameters.getParameter(name, default)`**, never through
  `spark.conf`. Precedence job run > task > job; both task-level and
  job-level parameters are visible. The two-argument form is required: with
  one argument an unset name raises a Py4JError. `oidlUtils` exposes
  `parameters`, `notebook`, `gateway`, `set`; there is no `widgets`.
- **[verified 2026-09-25] Jobs API shapes.** `parameters` (job or task level)
  is a list of `{"name", "value"}` -- a flat object is rejected with 400
  "Unable to process JSON input". `schedule` is `{"quartzCronExpression",
  "timezoneId", "pauseStatus"}` (6-field Quartz with `?` accepted;
  `pauseStatus` `PAUSED` stops the timer but manual runs still work).
  `dependsOn: [{"taskKey"}]` orders tasks as expected. The job list pages
  (25 by default, `limit` <= 100, `opc-next-page`). `POST .../actions/mkdir`
  on an existing folder returns 409. The control plane returned a
  transient 503 on `uploadFileMeta` once in about 60 calls.
- **[verified 2026-09-25] pyspark 3.5 signatures that differ from 4.x:**
  `F.substring(col, pos, len)` and `F.array_position(col, value)` take
  Python literals only; use `Column.substr(Column, Column)` and a CASE chain.
- **[verified 2026-09-24] A target table that does not exist yet** makes
  `DESCRIBE DETAIL` raise and `DeltaTable.forName()` fail with
  `DELTA_MISSING_DELTA_TABLE`. Generated notebooks guard both with
  `spark.catalog.tableExists` (three-part names accepted on 3.5).

## Installing `infa_compat` on a cluster

**[verified live 2026-09-25]** The cluster-library API is
`PATCH /workspaces/{ws}/clusters/{cl}/libraries` with

```json
{"items": [{"type": "WORKSPACE_FILE",
            "name": "/Workspace/<dir>/infa_compat-0.1.0-py3-none-any.whl",
            "path": "/Workspace/<dir>/infa_compat-0.1.0-py3-none-any.whl"}]}
```

`type` is `WORKSPACE_FILE`, not a PyPI name -- the cluster has no PyPI
egress and `infa_compat` is not published. Upload the wheel to the
workspace first, then reference it by path. A successful install reports
`"status": "INSTALLED"` with `"stateMessage": "INSTALL_LIBRARY operation
successful!"`. The console path is Cluster -> Library -> + -> Workspace
File.

## Generated notebooks need their target table to already exist

**[verified live 2026-09-25; DDL added since]** A generated
notebook asserts its target is Delta (`DESCRIBE DETAIL`) before writing and
then uses `DeltaTable.forName(...).merge(...)`, so a target that does not
exist fails the assertion rather than being created. Informatica loads
frequently create the target, so a migration needs target DDL from
somewhere -- currently a manual step, and one the migration-steps document
should keep calling out.

### Target DDL is now generated, from the export's declared types

`migrate` writes one `ddl/<table>.sql` per target, built from the export's
own `DATATYPE`/`PRECISION`/`SCALE`. It is not applied: creating tables in a
catalog should not be a side effect of converting code, so a human runs it.

This exists because the runtime create-if-missing guard takes column types
from the DataFrame, which is a silent fidelity loss -- `NUMBER(10,0)`
becomes `BIGINT`, a `SUM` over `NUMBER(12,2)` becomes `DECIMAL(22,2)`, and
nothing fails. The declared type is derivable, so it should not be guessed.

Both paths are kept on purpose. The DDL is the intended route; the runtime
guard stays as the net that lets a first run complete when nobody applied
it.

**The two can disagree, and the generated file says where.** A column fed by
a COUNT, SUM or AVG will not hold its declared type at runtime -- COUNT
produces `BIGINT`, SUM and AVG widen a decimal -- so applying the DDL and
then MERGEing can fail on a type mismatch. That is a genuine conflict
between the export's declaration and the arithmetic's result, and the tool
does not resolve it silently: the affected columns are named in a `REVIEW`
comment at the top of the file, and `migrate` prints how many targets carry
one. MIN and MAX are not flagged, because they return their argument's type.

## Diagnosing a failed notebook run

**[verified live 2026-09-25]** The job-run API exposes no `errorTrace` for a
notebook exception -- `state.stateMessage` says only "Exception during
execution of notebook <path>". What does work:

- `GET /workspaces/{ws}/taskRuns?jobRunKey=<run>&sortBy=timeCreated&sortOrder=DESC`
  -- **`sortBy` is required**; omitting it returns 400 `Invalid SortBy: null`,
  and only `timeCreated` is accepted (`startTime` and `name` are rejected).
  It still carries no trace.
- `POST /workspaces/{ws}/clusters/{cl}/actions/searchLogs` with
  `timeBegin`, `timeEnd` and `logContentTypeContains` (all three required;
  `DRIVER` works, `STDERR` and `ALL` return nothing). Messages are at
  `data.logContent.data.message`. The response is capped around 500 rows
  and does not reliably include the newest, so it is unreliable for
  chasing a specific run.
- What actually worked: a probe notebook that writes its findings to a
  Delta table, read back over JDBC. Cheap, and the result channel is
  durable.

## Job clusters: a DEFAULT cluster cannot run a job task

**[verified live 2026-09-25]** Every AIDP workspace has a `Default Master
Catalog Compute` cluster with `type: DEFAULT`. It accepts a job *definition*
that references it and then fails every *run*:

```
WORKFLOW_EXECUTION_0071 - Default Cluster "Default Master Catalog Compute"
used for non-system task s_m_EmployeeSummary in workspace <ws> within
instance <instance>. Change the cluster before workflow execution.
```

So a job created against it looks deployed and is dead. Job tasks need a
cluster with `type: USER`.

`AIDPDeployer._reject_default_cluster` refuses a `DEFAULT` key before
uploading anything, because a job that can never run is a failed deploy
rather than a partial one. It degrades to a warning if the cluster list
cannot be read — a read failure is not evidence the cluster is wrong.

Two things this cost us, worth remembering:

- The whole deploy path had ~990 passing tests and had never contacted
  AIDP. `tests/test_deployer.py` pinned the request *shapes* against a
  recording fake, and the shape here was valid. Only the service knows a
  cluster's `type`. This is the same failure mode as the earlier
  string-asserted expression tests: **the instrument measured presence,
  not outcome.**
- `type` is the field that matters, not `displayName`. A renamed default
  cluster is still a default cluster.

## Filesystem

- `/Workspace` is readable from the driver, so workspace files can be
  read directly without a separate upload/download step.

## Name resolution

- **[enforced, via reuse]** Independently of AIDP specifically, a name
  read before it is ever assigned is a guaranteed `NameError` at
  runtime. `engine/infa2aidp/generators/code_validation.py` already
  implements `unresolved_names(code)` for this (used by `demo.sh`'s
  verify step); the validity suite runs it over all 12 fixtures rather
  than re-implementing the same check.

## Spark version: 3.5.0 today, 4.x imminent

**Verified against live AIDP compute clusters on 2026-09-23** — two clusters
in separate workspaces both report
`clusterRuntimeConfig.sparkVersion = "3.5.0"`. Previously this was an
assumption; generated notebooks pin `spark.sql.ansi.enabled` partly because
the runtime version was unknown.

Two consequences for anything this tool emits.

**Nothing may use a function newer than 3.5.0.** Every PySpark function the
converter emits has been checked against its own `versionadded`, and two of
them land exactly on the boundary: `make_interval` and `percentile` are both
3.5.0, a single release of margin. `tests/test_spark_api_compatibility.py`
enforces this, and it works without Spark 3.5 installed because it reads the
annotation rather than calling the function.

**The executable expression tests run a newer Spark than the target.** They
need whatever pyspark is installed locally, and Spark 3.5 **cannot start on
Java 25** — it calls `Subject.getSubject`, which newer JDKs removed (3.5
supports Java 8/11/17). So the local harness runs 4.x while AIDP runs 3.5.
The API-surface gap is closed by the check above; behavioural differences
between 3.5 and 4.x are not, and remain a known limitation. Closing that
would mean installing a JDK 17 alongside so the harness can run the target
version.

### The Spark 4 transition

AIDP adds Spark 4 support imminently. Two things follow, and they pull in
opposite directions.

**Emit for the oldest cluster, not the newest.** During the overlap a
notebook may land on either runtime, so the API floor stays 3.5.0 until no
3.5 cluster remains. `tests/test_spark_api_compatibility.py` enforces that,
and its floor should be raised deliberately rather than as a side effect of
upgrading a laptop.

**The ANSI pins stop being precautionary and start being load-bearing.**
Verified on Spark 4.2: `spark.sql.ansi.enabled` defaults to **true** and
`spark.sql.storeAssignmentPolicy` defaults to **ANSI**. With those defaults,
`10/0` raises and casting `'x'` to int raises. Informatica yields NULL for
both and logs a row error, so a migrated mapping that relied on permissive
evaluation would **abort** on a Spark 4 cluster where it produced rows on
3.5.

The generated setup cell already pins both, which is what makes the upgrade
a non-event for generated code. That was written when Spark 4 was a
hypothetical; it is now the reason nothing breaks next week. Three tests
assert the pins exist and that each is necessary.

**One incidental benefit:** the local test harness runs pyspark 4.x, so once
AIDP is on Spark 4 the harness matches the target rather than running ahead
of it. The behavioural gap documented above largely closes on its own.

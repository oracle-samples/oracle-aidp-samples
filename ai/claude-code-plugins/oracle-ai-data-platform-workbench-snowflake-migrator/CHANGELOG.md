# Changelog

Release notes for the Oracle AI Data Platform (AIDP) Workbench Snowflake
migrator plugin, newest first. The format loosely follows
[Keep a Changelog](https://keepachangelog.com/).

## [0.28.0] — 2026-09-30

### Added
- The copy reads each table with one qualified pushdown built from the plan's
  per-column read/convert spec. NUMBER(38,s), TIME and TIMESTAMP fractions and
  offsets arrive exact, and VECTOR, MAP and structured OBJECT/ARRAY arrive as
  typed Spark columns.
- `--parallel N` for the copy (default 8), with source counts batched per chunk
  of 50, and `--verify counts+sums`, which sums the source in Snowflake exactly.
  A decimal total past 38 digits is reported `sum_not_comparable`.
- Liquid CLUSTER BY (plain-column keys), retention and change tracking are
  carried into the reviewed CREATE TABLE. DEFAULT, IDENTITY and PRIMARY KEY are
  reported with the reason they are not emitted.
- Dynamic tables and materialized views migrate as table snapshots, created,
  copied and reconciled as tables. `snowmig jobs` generates refresh notebooks
  for them and MANUAL job specs for Snowflake task graphs; `--register` creates
  them unscheduled.
- External and Iceberg tables register in place over OCI Object Storage
  (`external-registration`), outbound shares get a Delta Sharing plan
  (`share-plan`), and `plan --secure-views as-view` is an opt-in with a security
  warning.
- `demo --estate enterprise` runs the Snowflake-side stages over an emulated
  enterprise estate (shares, external and Iceberg tables, replication groups,
  masking at scale).

### Changed
- Structure creates run 8 at a time by default (about 2 s a table); `--mode
  ctas` creates one at a time unless `--parallel` is given.
- A plan file over 16 MiB is pushed gzipped (`<name>.gz`), with a pointer under
  the plain name that the structure, copy and reconcile stages follow and check
  by sha256. JSON plans compress 49-75x.
- Artifacts default to `./migration-artifacts/` in the working directory, not
  the plugin folder, so a plugin update keeps the record `teardown` works
  from. Any out-dir the migrator creates, or finds empty, gets its own
  `.gitignore`; the demo writes to `./snowmig_demo`.
- The OCI auth mode falls back to `OCI_CLI_AUTH`, and the profile it is
  inferred from to `OCI_CLI_PROFILE`, when the config sets neither.
- The launchers show why a temporary venv could not be created, try
  `SNOWMIG_PYTHON` first, and write no bytecode or pytest cache.

### Fixed
- `ddl` over a large plan no longer runs one regex per planned object for every
  view: 94 s for a 50,500-object plan, which was on course for hours.
- `SHOW PRIMARY KEYS / UNIQUE KEYS / IMPORTED KEYS IN DATABASE` stop at 10,000
  rows. A read at the cap is completed per schema and then per table, so no
  declared key is dropped. Foreign keys are grouped in one pass.
- The view phase writes its report on an interval rather than after each view,
  and a report just written is never re-read.
- `run` shows a failed task's `errorTrace` (the fetched log is only its head),
  and identifies a transient service error (for example an HTTP 503) so the run
  can be resumed.
- Reconcile marks a table in the target that no structure report records as
  `structure: unrecorded`, with the re-run that checks it. A planned view no run
  has recorded is `VIEW_NOT_CREATED_YET`.
- A plan file that is not valid JSON fails naming the file and the remedy.
- `run` fetches this run's discovery manifest, from the reports-dir it was
  provisioned with, and a Windows drive path is refused as a stage parameter.
- CLUSTER BY keys are written bare, and a view's column list is carried as
  `SELECT * FROM (...) AS named_columns(...)`, the forms AIDP reads.
- A `cluster by (...)` before the column list in `GET_DDL` is skipped when its
  columns are read.
- Task bodies and dynamic-table queries are kept only with
  `--capture-definitions`; without it, the generated job or refresh names the
  flag that keeps the body. A materialized view's query is always captured,
  with the view text.
- `--mode append` refuses to start after an unfinished copy run of another
  mode, and `--tables` is deduplicated before the parallel copy. A later run
  over a narrower `--tables` scope carries the unfinished run's other tables
  forward, so append stays refused for them.
- One rule resolves the source database name for every statement, GET_DDL
  included, on the laptop (`assess`, `preflight`) as in the notebooks; a
  quoted, mixed-case database name is supported.
- `teardown --scope all` also removes the jobs and notebooks `jobs --register`
  created.
- The copy compares every column's live source type with the type its plan
  spec was decided for, not only DECIMAL columns. A column whose type changed
  after the plan was approved is refused by default (`type_drift`), and
  `mapping.source_type_drift: convert` copies it under its new type as
  `verified_with_conversion`, never plain `verified`.
- An object name placed in a Snowflake string literal (Iceberg lookups,
  security attachments, SHOW paging) escapes the backslash as well as the
  quote, so a crafted name cannot close the literal. An external table's
  LOCATION is a properly escaped Spark literal, and maintenance reads quote
  the database, schema and table they scope.
- Both read-only guards refuse Snowflake's `->>` statement chaining. The
  in-AIDP guard ends a block comment at the first `*/`, as Snowflake does,
  and refuses an unclosed literal, comment or `$$` string.
- A stage config that does not parse reports the line number only, never the
  line, which can hold the credential.
- A dynamic table whose query was not captured names `--capture-definitions`,
  not the role's grants. `TRANSLATION_MAP.md` counts the table snapshots the
  plan leaves out.
- `provision --delete-stale-copy-jobs` deletes only copy jobs this migration's
  records show it created (`created_jobs`, carried from push to push). Other
  `snowmig_02_copy_*` jobs on a reused workspace are reported, not deleted.
- `catalog --execute` reuses an existing catalog only when this migration
  created it or `--reuse-existing` is passed, and refuses a catalog of the
  other type in every case.
- A catalog recorded `created` stays a teardown target after a re-run of
  `catalog --execute` records it `reused`.
- `teardown`, `publish` and the per-stage publish pass the configured OCI
  profile and auth mode, like every other stage.

## [0.27.0] — 2026-09-29

### Added
- `teardown --scope credential` deletes the Snowflake credential object that
  `provision --source-config` placed on the workspace, and confirms it is gone.
- `teardown --scope all` removes everything a migration created (credential,
  jobs, clusters, the catalogs it created, then the workspace), each only where
  the provisioning record and catalog ledger show this migration created it. A
  reused workspace, cluster or catalog is never touched.
- The INTERNAL catalog and its tables are deleted only with `--include-data`.
- Every delete is read back. A delete that was accepted but is still listed is
  reported as `delete_requested`, never `deleted`. Nothing is deleted without
  `--execute`.
- The default scope is unchanged: the migration's clusters are stopped (or
  deleted with `--action delete`), and its workspace, catalogs and jobs are kept.

## [0.26.1] — 2026-09-29

### Fixed
- The stage board follows the runbook order (S1, S3/S4, S6, S7, S10, S12). It
  never proposes the copy on its own, shows a running job as RUNNING with what
  it is waiting on, and treats `assess` as satisfied once `ingest` has run.
- The S3 and S4 catalog registrations each keep their own record
  (`catalog_result_<name>.json`, `CATALOG_<name>.md`). `catalog_result.json`
  lists every catalog registered so far, with its connection test.
- An EXTERNAL catalog report is titled "Source catalog", and the `schema:` note
  appears only for an EXTERNAL registration.
- A `--reuse-existing` re-push keeps the recorded cluster name. PROVISION.md
  shows the cluster's real state, prints the workspace and cluster keys in a
  hand-off block, reports an existing folder as `exists` and names the next
  runbook step.
- `run` prints and records the result of its task-parameter check.
- Structure and copy logs show the elapsed time for each table and leave out
  tables that are not in the plan.
- Reconcile reports tables the approved plan leaves out as `NOT_IN_PLAN`. The
  plan summary reports objects excluded by restrictions as excluded.
- The demo and the test fixtures use English names.

## [0.26.0] — 2026-09-29

### Added
- One copy workflow per schema: with an approved plan, `provision` creates a
  `snowmig_02_copy_<schema>` job for each schema, passing `schema` as a task
  parameter. The stage board tracks every job.
- Workflow task parameters reach the stage notebooks and override their PARAMS
  cell. `provision --stage-param NAME=VALUE` sets stage values, and `run` checks
  task parameters before it starts a job.
- `run --refresh` / `--run-key` re-read a run from AIDP without submitting
  anything. `run` downloads the discovery manifest after a successful
  discovery, and `snowmig fetch` downloads any workspace file.
- `provision --delete-stale-copy-jobs` removes copy jobs the current plan no
  longer names. Discovery and every plan push write dated backups
  (`--plan-label FULL|REDUCED`).
- The census covers alerts, secrets, network rules, Streamlit apps, notebooks,
  container services, shares, roles, network policies, applications and
  compute pools. For each pipe or task, it names the migrating table it loads.
- The security report reads policy and tag attachments object by object,
  covers all four policy kinds and grants on 22 object classes, and reports
  secondary roles. `--only-primary-role` limits a session to `--role`.
- The plan carries more source facts: primary, unique and foreign keys, column
  defaults and identity, comments, collations and sub-microsecond timestamp
  precision. Each one comes with a warning where the target behaves differently.

### Changed
- Structure creation is faster: one `SHOW TABLES` per schema, and tables in a
  schema are created in parallel (`--parallel`, default 4), each read back.
- The structure job creates tables and views under the names in the approved
  plan, with views in dependency order. Nullability and comments are applied
  and verified. Copy and reconcile use the same names.
- The copy selects columns by name. It refuses a table whose layout or DECIMAL
  precision no longer matches the plan, and `--verify counts+sums` sums at the
  source's scale.
- Reconcile reports views by outcome and schemas not yet created as
  `NOT_MIGRATED`. It flags `STRUCTURE_FAILED`, `STRUCTURE_TYPE_DRIFT` and
  `COUNT_DRIFT` as problems.
- `run` waits up to 120 s for a run to start and retries up to five times. Its
  record is written as soon as a run is submitted and is kept if a status poll
  fails.
- View translation handles `//` comments, quoted identifiers, `''` escapes, a
  view's column list and references within the view's own schema. Constructs
  with no exact Spark equivalent (`::TIME`, `$$...$$`, `DATEDIFF`,
  `LISTAGG ... OVER`) are refused by name.
- The plan gives a reason for every object it leaves out: dynamic, external,
  Iceberg, event and hybrid tables, views over objects that do not migrate,
  and names or keys the target cannot accept. Restrictions are validated, and
  an entry that matches nothing is flagged.
- Secrets stay off the command line: `key_passphrase:` in the config replaces
  `--key-passphrase`, and `provision --source-config` uploads only the
  `snowflake:` block. `smoke --write-probe` needs `--execute`.
  `ddl --timestamp-ntz timestamp` re-maps `TIMESTAMP_NTZ` offline.

### Fixed
- `teardown` and the billing report act only on resources the migration
  created. They flag anything of unknown origin instead of touching it, and a
  cluster is reported deleted only after teardown confirms it.
- `provision --reuse-existing` keeps the migration's catalogs, credential,
  source mode and notebooks (use `--refresh-notebooks` to overwrite them). Each
  warehouse gets its own cluster, and PROVISION.md reports the copy jobs that
  are really on the workspace.
- The stage board and phase report show RUNNING, PARTIAL and UNKNOWN where they
  apply. A dry-run or failed stage no longer counts for its alternative, and the
  token report marks the runs it could not measure.
- Calls to AIDP follow pagination, retry when the response body reports an
  error, are time-bounded and recognise every final run state. The catalog
  connection test is polled until it has a result.
- `deploy` waits for each schema to be ACTIVE, reports the target's own answer
  when it refuses a create, and exits 1 when no statement matched the catalog.
- Discovery records column defaults, identity, comments, collations and table
  kind. `--schemas` filters the query itself and merges into the manifest, and
  a scoped re-discovery that finds nothing keeps the earlier result.
- Snowflake connection: an invalid account identifier fails fast with a hint,
  a programmatic access token works through `--auth pat` or an inline `token:`,
  and an explicit `--auth` takes precedence over the config. Both read-only
  guards parse CTEs, `//` comments and quoted identifiers the way Snowflake does.
- The CLI runs on Windows, and `run` reads the config's `aidp:` block. The
  reports and the documentation agree with the plan and with each other.

## [0.25.0] — 2026-09-19

### Added
- `ddl` checks the SQL it generates against column types the target metastore
  refuses (such as `TIMESTAMP_NTZ`). It halts with exit 3 and names the columns
  and the fix, before any cluster time is spent.
- `preflight` detects a session schema with no relations, which the connector
  needs to open a session, and suggests schemas from the account that would work.
- `run` will not start a job run while another run of that job is in progress,
  and it names the run that is in progress.

### Changed
- The target INTERNAL catalog is created by the plugin, as a container.
- The S10 structure job defaults to `ddl-plan` mode and reads its types from
  the approved plan.
- `run --param` is refused, with a pointer to where stage parameters are set.

### Fixed
- The stage notebooks read the nested `snowflake:` block, so one config file
  serves both ends.
- The `provision` dry run previews the `.ipynb` notebooks it will upload, and
  provisioning creates the backup folder the runbook writes to.

## [0.24.0] — 2026-09-19

### Added
- `snowmig databases` lists the source databases a role can see. `snowmig
  catalogs` lists every catalog on the AI Data Platform with its type.
- The `bin/snowmig` and `bin/snowmig-test` launchers: no interpreter path to
  remember, and nothing is left behind outside a temporary directory.

### Changed
- All output goes to one gitignored `migration-artifacts/` directory that
  includes a README describing its contents. `snowmig clean` removes it.
- Runbook order: S1 workspace, S2 cluster, S3 EXTERNAL source catalog, S4
  INTERNAL target catalog.
- The data plane ships as self-contained `.ipynb` notebooks under
  `data-migration-scripts/`, generated by `snowmig build-notebooks`.
- Discovery runs as the in-AIDP workflow: two `INFORMATION_SCHEMA` queries per
  database.

### Fixed
- The catalog types are `INTERNAL` and `EXTERNAL`. `STANDARD` is accepted as an
  alias and creates the INTERNAL container.
- `pytest` runs from the plugin root.

## [0.23.0] — 2026-09-18

### Changed
- Samples and documentation use generic names only.

## [0.22.0] — 2026-09-18

### Fixed
- Artifacts are written inside the plugin's own folder, never into the user's
  project, and output folders are gitignored. The skills never create anything
  outside the plugin folder.

## [0.21.0] — 2026-09-18

### Added
- `snowmig ingest` turns the in-AIDP `discovery_manifest.json` into the
  `inventory.json` that planning reads, using the same type mapper as `assess`.
- `snowmig run` runs an AIDP job as a workflow, polls it until it finishes and
  writes `RUN_*.md` with the task output.

### Changed
- The migration follows a fixed twelve-step runbook in the overview skill.
  `snowflake-assess-estate` is a laptop-side preview only, not the discovery
  step of a migration.
- `provision` stops with `name_taken` instead of adopting an existing
  workspace, cluster or job. `--reuse-existing` opts in.
- `catalog --catalog-type standard` creates the catalog container, and its
  tables are created on AIDP compute.

## [0.19.0] — 2026-09-18

### Added
- One config file, `snowmig-config.yaml`, for both ends (`snowflake:` and
  `aidp:` blocks), and flags override it. Inline secrets are supported. They
  are redacted wherever the config is shown, and the file is created `0600`.
- `init-config` writes the template and never overwrites an existing config.
  The CLI announces which config and destination it uses, and `preflight`
  reads every field back with secrets masked.
- `.claude-plugin/marketplace.json`, so the plugin can be installed with
  `claude plugin marketplace add`.

### Changed
- The `oci raw-request` backend is preferred, and the `aidp` CLI backend uses
  the current command flags and output format.
- The overview skill states that the engine is the method: if it cannot be
  found, stop and say so. Bootstrap builds every path from
  `${CLAUDE_PLUGIN_ROOT}`.

### Fixed
- A failed Snowflake connection error names the field most likely to be wrong
  and never includes the credential.
- `catalog` finds the config file without `--config` and accepts a `schema:`
  in it. An EXTERNAL catalog registers the whole database.

## [0.18.0] — 2026-09-16

### Added
- `snowmig preflight` reads the connection config back and tests both ends.
- By default, the in-AIDP scripts read Snowflake through the AIDP Snowflake
  connector (`--source-mode connector`). Discovery covers a whole database in
  two `INFORMATION_SCHEMA` queries.
- `01_create_structure --mode ddl-plan`, the new default, applies the types
  from the approved plan and reports tables outside the plan as `not_in_plan`.
- `diagnose_environment.ipynb`, and job tracking: start a run, poll it and
  fetch the task output.
- One AIDP compute cluster per Snowflake warehouse
  (`provision --warehouse-clusters`). The README walks through a full migration
  from start to finish.

### Changed
- EXTERNAL/SNOWFLAKE catalog registration follows the AIDP catalog contract,
  and `catalog --test-connection` is available.
- Jobs run notebook tasks. Workspace files are uploaded with `workspace-object`
  and read back.

### Fixed
- Structure and copy runs resume per target, and a table with no target is
  recorded and skipped. Refusals are printed to stdout, and reconcile exits
  non-zero only on real problems.

## [0.17.0] — 2026-09-16

### Added
- Dev mode (`snowmig demo`, `/snowflake-demo`) runs the whole pipeline against
  an emulated Snowflake estate and an emulated AIDP. It writes every real
  artifact plus a narrated `DEMO.md`.
- `snowmig provision` (`/snowflake-provision`) sets up the AIDP environment:
  the workspace (with a safe name), the migration cluster, cluster libraries,
  the `backup-snowflake-migration/` folder with the scripts and plan, and the
  migration jobs. It is a dry run by default.
- `data-migration-scripts/` holds the stages that run inside AIDP, schema by
  schema: discover, create structure, copy (verified by row counts and decimal
  sums) and reconcile.
- `MIGRATION-ARCHITECTURE.md`, the migration design record.

### Changed
- The CLI and the demo share the same `ddl` logic.

## [0.16.1] — 2026-09-16

### Added
- `/snowflake-catalog` command.

### Fixed
- `deploy --execute` refuses an EXTERNAL catalog, the smoke write probe skips
  one, and the stage board includes `catalog`.
- The catalog credential is passed through a `0600` temporary file instead of
  the command line, and printed commands redact it. `pyyaml` is now a runtime
  dependency.

## [0.16.0] — 2026-09-11

### Added
- A connection-config file for the EXTERNAL catalog, and `create_catalog` /
  `list_catalogs` on both backends.

### Changed
- By default the target catalog is EXTERNAL with source type SNOWFLAKE. A new
  `catalog` stage registers it as a read-only pointer at the live source that
  copies no data.
- Internal (Standard) catalogs are created only when explicitly requested, and
  their tables are created by a script run on AIDP compute.

## [0.15.2] — 2026-09-10

### Changed
- The skills verify a stage's result before reporting success. After each
  stage they report the status and the next stage without being asked.

## [0.15.1] — 2026-09-10

### Fixed
- The diagnosis probe is always named in the result, whether or not its cleanup
  completed.

## [0.15.0] — 2026-09-10

### Added
- Failed-create diagnosis: when an object does not appear, one probe per schema
  shows whether the name or the request is the problem. The report gives the
  fix, and `--no-diagnose` turns the probe off.

### Fixed
- The smoke test checks the destination through the catalog API with a read
  and a write probe. The probe creates a uniquely named schema, confirms it
  exists and removes it.

## [0.14.0] — 2026-09-10

### Changed
- The tests expect the lower-case target names that AIDP uses.

## [0.13.1] — 2026-09-10

### Added
- `derived_type_drift` covers a view that has every planned column but whose
  column types the target derived from the SQL. It is reported separately from
  a mismatch, and a narrower derived type is flagged as a risk of overflow.

## [0.13.0] — 2026-09-10

### Fixed
- Table structure is checked by reading the table itself. An existing schema
  is reused rather than created again, after waiting until it is ACTIVE.

## [0.12.0] — 2026-09-10

### Added
- `snowmig stages` and the `snowflake-stage-board` skill show the pipeline as a
  table: each stage, what it needs, whether it has run and what it found. A
  stage that could not look is flagged and never shown as clean.

### Changed
- The translator decides how `TIMESTAMP_NTZ` is mapped
  (`assess --timestamp-ntz {preserve,timestamp}`), so the plan shows the type
  that will actually be created.

## [0.11.0] — 2026-09-10

### Added
- The `catalog_api` transport, now the default, creates schemas, tables and
  views through the AIDP catalog API and needs no Spark cluster.
- `PREFLIGHT.md` is shown before the first write. New options:
  `--bronze-schema-style db`, `--timestamp-ntz` and `--probe-table-parameters`.

### Changed
- Target names are planned in lower case, the way AIDP stores them, and a case
  collision halts the run.

### Fixed
- Object keys are looked up on the server, ignoring case. Each create is read
  back with bounded polling until the object appears.

## [0.10.1] — 2026-09-10

### Fixed
- An HTTP error in an `oci raw-request` response body is raised as an error,
  and nested `data.items` collections are unwrapped.

## [0.10.0] — 2026-09-10

### Added
- `assess` runs an estate census of procedures, UDFs, tasks, streams, stages,
  pipes and more (`CENSUS.md`). `snowmig security` reports policy attachments,
  secure views and grants (`SECURITY.md`).

### Fixed
- Skills can now run `summary` and `maintenance`.

## [0.9.0] — 2026-09-10

### Added
- `snowmig maintenance` reports clustering, Search Optimization, change
  tracking, Time Travel retention and reclustering history (`MAINTENANCE.md`).
  Each data-movement option states who owns `OPTIMIZE`/`VACUUM` afterwards.

## [0.8.0] — 2026-09-10

### Added
- `references/maintenance-and-layout.md` and a maintenance section in the DDL
  plan. Clustering, retention and change tracking map to AIDP equivalents:
  liquid clustering or `ZORDER`, Delta retention properties, and Change Data
  Feed.

## [0.7.1] — 2026-09-09

### Fixed
- Spark string literals are escaped with a backslash. A test runs the generated
  DDL through a Spark-dialect parser.

## [0.7.0] — 2026-09-09

### Added
- A SQL lexer that leaves literals, quoted identifiers and comments untouched.
  Deploy verifies each object's structure. New options: `--row-counts`,
  `--semi-structured` and `--geospatial`. SHOW results are paginated past
  10,000 objects.

### Changed
- Row counts default to the count Snowflake keeps in its metadata. An
  untranslated view is HIGH risk, and the destination write probe cleans up
  after itself.

## [0.6.0] — 2026-09-09

### Added
- The `A6_CUSTOMER_DEFINED` data-movement option, for an architecture the
  customer designs themselves.

## [0.5.0] — 2026-09-09

### Added
- Every plan and summary presents the data-movement options. The transport
  enforces read-only access to the source, and a first set of dialect
  translation rules is added.

### Changed
- The unused WebSocket/Spark-session transport is removed.

## [0.4.0] — 2026-09-09

### Added
- A summary table per object, a smoke test of both ends, and the shallow clone
  delivered as an executable AIDP notebook.

## [0.3.0] — 2026-09-09

### Added
- The Bronze layer mirrors the source one to one. Also added: disabled Silver
  and Gold job stubs, the `aidp` CLI and `oci raw-request` backends, and a
  warehouse-to-cluster compute proposal.

## [0.2.0] — 2026-09-09

### Added
- The `snowmig` pipeline (assess, deps, plan, ddl, deploy): an inventory that
  halts on case collisions, Snowflake-to-Spark type mapping, dependency waves,
  Delta DDL with a record of every rule applied, and deployment that is a dry
  run by default.

## [0.1.0] — 2026-09-08

### Added
- The plugin skeleton, Snowflake authentication (key-pair, programmatic access
  token, password, SSO) and resolution of the AIDP target.

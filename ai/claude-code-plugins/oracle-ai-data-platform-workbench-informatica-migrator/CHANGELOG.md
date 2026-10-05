# Changelog

All notable changes to this project are documented here. This project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — unreleased

Initial release. An Informatica-to-AIDP migrator: it reads Informatica
exports and generates Spark notebooks that run on Oracle AI Data Platform,
packaged as a Claude Code plugin.

### Added

- **A Joiner whose export carries no MASTER flag now joins.** An inner join
  is symmetric, so the master/detail direction it could not resolve does not
  change the result; only an outer join depends on it, and that is still
  refused. Previously the join was dropped outright and the notebook returned
  rows with the second source absent. Two further defects were behind it: the
  Joiner read the raw predecessor while the prepared column-rename copy sat
  unused, and the join condition was oriented by the Designer's "master
  written first" convention rather than by the CONNECTORs that say which
  DataFrame actually carries each port.
- **SQL overrides and User Defined Joins are translated, not reported.** An
  override that JOINs, UNIONs or sub-selects runs as Spark SQL against
  catalog-qualified tables; a UDJ is synthesised into the same shape and goes
  through the same gates. Both refuse, naming the reason, when the SQL uses an
  Oracle-only construct or references a table that is not a source in the
  mapping. Two silent-wrong-data defects were found doing this: a
  comma-separated FROM list only qualified its first table, and a MINUS
  override was read as a single-table query with MINUS as a table alias --
  losing half the query with no review item at all.
- **`--schedule-timezone`.** PowerCenter records STARTTIME in the Integration
  Service's local time with no zone, so the export cannot supply it. Declaring
  it makes a converted schedule correct rather than plausible and drops the
  assumption from the review; a non-IANA zone is refused at construction,
  since AIDP rejects an unknown `timezoneId`. Jobs are still created PAUSED.
- **Dropped workflow tasks say how to rebuild them.** Command, Email,
  Decision, Timer, Event-Wait/Raise, Control and Assignment have no AIDP
  job-task equivalent and are still reported rather than invented -- but each
  now carries what it did, a specific rebuild path, and its own configured
  values. The parser reads `<TASK>`/`<ATTRIBUTE>`, which it did not before, so
  the review can quote the command line, the recipient, the decision
  expression and the timer time instead of only naming the task.
- **The Spark 3.5-vs-4 probe is a suite test.** All 14 documented differences
  reproduce on pyspark 4.2.0, 15 expressions believed ANSI-neutral are pinned
  as unaffected, and the count in the reference document is asserted against
  the probe.
- **The agentic pipeline is covered end to end** against a scripted model:
  that a validation failure reaches the fixer, that a fixed attempt is
  re-validated, that the output handed over is the best attempt rather than
  the last, that the retry is bounded, and that a model which raises does not
  take the migration down.

### Fixed

- **The repository crawler verified no TLS certificate.** `requests.Session.verify`
  was hard-coded to `False`, so repository credentials travelled over an
  unverified channel to every Web Services Hub. Verification is on by default;
  `INFA_CA_BUNDLE` names a corporate CA bundle and `INFA_TLS_VERIFY=0` is an
  explicit opt-out for a self-signed lab host. The `pmrep connect` password
  moved from `-x <password>` on the command line to `-X INFA_PMREP_PASSWORD`
  in the child's environment. Both defaults, the CA-bundle pass-through and
  the env-var handoff are pinned by `tests/test_crawler_security.py` (13
  tests, contributed in review).
- **Eighteen defects from a review of the SQL-override, workflow and
  write-range changes**, each reproduced first and pinned by a test that
  fails without the fix (`tests/test_sq_sql_review_fixes.py`,
  `tests/test_workflow_review_fixes.py`,
  `tests/test_write_range_review_fixes.py`).
  - **Source Qualifier SQL.** A User Defined Join replaced the SQL override,
    dropping its WHERE clause and its refusal; a SQL Query now overrides the
    UDJ, Source Filter, Select Distinct and Number Of Sorted Ports, as in
    PowerCenter, and says which it ignored. A UDJ was `SELECT *` over the
    joined tables, so the join key came back twice (`AMBIGUOUS_REFERENCE`);
    it now projects each source column once, and refuses a shared column
    the join does not equate. Table qualification rewrote string literals
    (`= 'CUSTOMERS'`) and skipped schema-qualified tables (`APP.ORDERS`),
    which then ran against the session's catalog. `'$$X'` became `''EU''`,
    and braces in an override were evaluated as an f-string once any
    parameter was present. Informatica's `{ A LEFT OUTER JOIN B ON ... }`
    UDJ syntax was emitted as invalid SQL; it is refused with a reason.
  - **Parameter values in SQL.** `_sql_lit` doubled quotes Oracle-style,
    which Spark reads as two adjacent literals (`'it''s'` is `its`), and
    left backslashes unescaped; it now backslash-escapes both.
  - **Refusals that did not compile.** A multi-line unconvertible Filter or
    Router condition put its second line in code position, so the notebook
    failed with SyntaxError before the intended `NotImplementedError`.
  - **Workflow tasks.** Non-reusable task definitions were looked up by name
    across every workflow in the file; each workflow now uses its own first.
    Command-task lines held in `VALUEPAIR` elements are read.
  - **Link conditions.** A `$s.Status = SUCCEEDED` condition on a link into
    a Command, Decision or worklet Start was lost, so the downstream session
    ran after a failure (`ALL_DONE`); conditions are now carried along the
    path. A task with mixed incoming links got `ALL_SUCCESS` while the
    review said `ALL_DONE`; the review now describes the runIf actually
    emitted.
  - **`--schedule-timezone`** rejected every zone on Windows, where Python
    has no tz database; `tzdata` is now a dependency, the zone is checked
    before anything is written, and case is significant as it is for
    AIDP's Java `ZoneId`.
  - **A notebook name clash** left the second workflow's job pointing at
    the first mapping's notebook.
  - **The Spark version gate** refused a cluster reporting `3.5` against a
    required `3.5.0`.
  - **Writes.** A Sequence Generator key was re-written on a re-run when the
    target spelled its column in mixed case. The declared-range check
    skipped integer targets, which then wrapped on the cast
    (3000000000 into INT gave -1294967296); compared in double, so valid
    18- and 38-digit values were refused; and passed values that only
    overflow after rounding, which were then written as NULL. It now uses
    the cast itself as the test.
- **Four regressions in the fixes above**, found reviewing them and
  pinned by six tests. An apostrophe in a `/* comment */` opened a fake
  string literal that hid the next JOIN's table from qualification;
  comments are now masked with literals, and a `--` line comment or an
  unterminated literal is refused. A simple override lost its `DISTINCT`
  and `ORDER BY` once the Select Distinct and Sorted Ports settings stopped
  applying under a SQL Query; such an override is run as written. A UDJ
  column named `ORDER#` was a ParseException; columns are backquoted in the
  select list and the join condition. The
  range check cast to the declared `DECIMAL(p,s)` rather than the clamped
  type the DDL creates, so `NUMBER(40,2)` refused every write.
- **Two systematic false positives in the source-fidelity report.** A Source
  Qualifier's name appeared nowhere in the generated notebook, because the
  read cell dropped the line carrying it -- so every Source Qualifier was
  reported missing, and a reviewer could not tie the read back to the export.
  And a port whose `EXPRESSION` is just its own name is a pass-through that
  computes nothing; PowerCenter writes one on every Source Qualifier port, so
  counting them demanded that every unconnected source column appear in the
  notebook. Together these accounted for most of the reported gaps on the
  bundled exports -- 17 of 19 mappings flagged, down to 12 -- with no change
  to any notebook.
- **The run summary counted every workflow assumption as a schedule
  assumption**, printing "CHECK AND CORRECT THE JOB SCHEDULE IN AIDP" for a
  run whose only assumption was about link semantics and whose schedule was
  correct. Wrong in both directions: a false alarm here, and a buried warning
  on runs where the schedule really is assumed.
- **A `$X.Status = FAILED` link** was reported as making the downstream task
  "run whenever the upstream one succeeds". The dependency is `ALL_DONE`,
  which runs on completion either way, so the line described neither the
  condition nor the generated behaviour.

- **A Sequence Generator surrogate key is no longer re-written by a
  re-run.** The MERGE condition already preferred a natural key over a
  surrogate one, because a Sequence Generator hands out a fresh value on
  every run — but the write then called `whenMatchedUpdateAll()`, which put
  that fresh value over the stored one. Every re-run therefore re-keyed the
  rows that already existed, so anything holding a foreign key into the
  dimension pointed at the wrong row, and the load still reported success.
  Informatica consumes NEXTVAL on the insert path and leaves the key alone
  on an update. Writes now emit `_FROZEN_ON_UPDATE` naming the sequence-fed
  columns the match condition does not use, and update everything else;
  `whenNotMatchedInsertAll()` still sets the key on new rows, since dropping
  it from the insert path too would leave it NULL. The frozen columns are
  traced forward through CONNECTORs from the NEXTVAL port rather than matched
  by name, so a renamed port does not escape. Covered by
  `tests/test_sequence_key_stability.py` (which fails without the fix) and by
  the `c06_union_filter_sequence` golden case, which executes the frozen
  merge on real Spark.
- **`references/conversion-coverage.md`: what the tool converts.** The repo
  documented how it had been tested but never stated plainly what it
  converts — a user had to read the dispatch table. Every transformation type
  with one of three outcomes (converted, reported with a named reason,
  refused), the expression functions, and how a workflow becomes an AIDP job.
  Counts are read off the code, and `tests/test_coverage_doc_accuracy.py`
  fails if code and document disagree, including if any modelled type reaches
  the `_unsupported` fallback without a decided outcome.

- **An unconvertible condition refuses instead of silently dropping every
  row.** When `_convert_or_flag` could not translate a Filter or Router
  group condition it returned the placeholder `F.lit(None)`, and both call
  sites emitted the review as a *comment* and then used the placeholder:
  `.filter(F.lit(None))` is not a NULL condition, it is FALSE for every
  row. The branch silently received nothing, the notebook ran, wrote an
  empty result and reported success — worse than failing. `_conversion_failed`
  had been written for exactly this case and was never called. Both sites now
  emit `REVIEW REQUIRED` and raise. Same treatment for an untranslated SQL
  `CASE`, which was emitting a bare `F.lit(None)` column — a VOID column
  Delta refuses outright (`DELTA_MERGE_ADD_VOID_COLUMN`) — behind a `TODO`
  comment.


- **Six defects in notebook generation, each of which produced code that
  could not run.** All are fixed, or now refuse explicitly rather than
  emitting something broken.

  - `TO_CHAR` over an expression ending in a numeric literal emitted
    `* 10.cast('string')`, which Python reads as the float `10.` — the
    notebook did not compile. It now goes through `_castable`, which exists
    for exactly this.
  - A multi-line Update Strategy expression was emitted as a comment with
    only its FIRST line prefixed, so the rest landed in code position and a
    `→` in the text became `invalid character '→' (U+2192)`. Export free
    text now gets one prefix per line, at all three sites that interpolate
    it.
  - A Router fed by a Sequence Generator never re-pointed the sequence's
    placeholder variable, so consumers read a bare `df` that nothing
    assigns.
  - `df` is bound to the main chain when the assembled notebook reads it
    unwritten — a safety net over the same bookkeeping, a no-op when `df` is
    already assigned or unread.
  - A mapping whose export carries no source definitions emitted a silent
    placeholder read and then a half-wired Joiner. It now binds one
    placeholder per Source Qualifier and refuses with `REVIEW REQUIRED`,
    because a notebook built on a placeholder cannot run whatever else is
    done to it.
  - **Two mappings sharing a folder and a name overwrote each other in
    silence**, while the run reported one notebook per mapping. Both are now
    kept, the later suffixed, and the clash reported.

  The self-check added in the same release is what found all of them, and it
  distinguishes a notebook that *refuses* (a declared outcome) from one that
  looks finished and dies.

- **`migrate` reads back what it wrote, and names any notebook that cannot
  run.** A generated notebook can parse, pass every string assertion and
  still raise `NameError` on its first cell, because the dataflow
  bookkeeping named a variable it never assigned. Each run now re-opens its
  own notebooks and reports those that reference an unassigned name, that
  prepared a per-consumer input copy and never read it, or that do not
  parse — in the summary and in `reports/broken_notebooks.md`, stated as a
  defect in the migrator rather than in the export.

- **SCD2 Router branches are matched by what their conditions test, not by
  name.** `new_df = "df_new_records" if "new_records" in group_names else
  "df_new"` assumed one naming convention and fell back to `df_new` /
  `df_changed`, which nothing assigns. An export naming its groups
  `Insert` / `Update` — the commoner convention — produced a notebook that
  died before reading a row. The NEW branch is now the group whose
  condition tests the lookup key for NULL and the CHANGED branch the one
  testing a lookup value for inequality; when neither can be identified the
  cell is a `REVIEW REQUIRED` naming the groups found, instead of code that
  cannot run.

- **A transformation no longer takes its own unassigned output as its
  input.** The in-place shortcut was guarded by `startswith("df_")`, which
  matches every `df_<name>` variable rather than only a Router group. It
  now requires the variable to be one of that transformation's resolved
  predecessors.

  All three were found during breadth testing, by running the tool over a
  wide set of PowerCenter exports and reading back what it generated.

- **A write refuses a value that does not fit the target's declared
  precision.** `storeAssignmentPolicy` is pinned `LEGACY` so migrated
  expressions keep Informatica's permissive evaluation; on the write path
  that same setting makes Spark *wrap* rather than raise -- 3000000000 into
  an `INT` stores `-1294967296`, a negative number where the source had a
  positive one, with no error anywhere. The pin stays, because the read path
  depends on it, and the write is now loud instead of lossy: the notebook
  counts rows outside each declared `NUMBER(p,s)` before writing and raises
  with the column and the count. Limits come from the export's own
  PRECISION/SCALE. Prepended in `emit_write` rather than per branch, so a
  write path added later cannot skip it. Verified by executing the emitted
  check on a real Spark session -- it must raise on an overflow and must not
  on boundary values like 999 in `NUMBER(3,0)`, because a check that fires
  on good data gets switched off. This closes what the Spark-4 document had
  recorded as an open decision.

- **The deploy refuses a cluster older than the notebooks need, and there is
  no per-version code generation.** Each generated notebook declares
  `_GENERATED_FOR_SPARK_MIN` in its setup cell and the deployer reads it
  from the artifact, so a notebook built by an older generator is judged by
  what *it* needs. A floor, not a match: the converter emits only
  constructs every supported Spark accepts and pins the behavioural flags,
  so one notebook runs on 3.5 and on 4.x and can move between a dev and a
  prod cluster during a staggered upgrade. The floor lives in one place
  (`infa2aidp/spark_target.py`) because three things must agree -- the
  API-surface guard, the notebook's declaration and this check; the test
  that used to redeclare it now imports it.

  Emitting different syntax per cluster version was considered and
  rejected: it would couple each notebook to the cluster it was generated
  against and make the generator non-deterministic, in exchange for nothing
  -- every construct the converter emits already has a single form both
  versions accept. If Spark removes something we emit, rather than widening
  a signature, that needs a runtime branch inside the notebook and the
  API-surface guard is what would surface it.

  Degrades to a warning when the cluster's version is missing or
  unparseable: an unreadable string is not evidence a cluster is too old.
  Both cluster guards now share one memoised lookup, so "checked once, not
  per workflow" stays true.

- **`runIf` is decided per task, and an unconditional link now gets
  `ALL_DONE`.** PowerCenter runs the downstream task of an unconditional
  link once the upstream COMPLETES, succeeded or failed; every task was
  emitted `ALL_SUCCESS`, which stops instead. That was a silent tightening:
  a safer pipeline but different behaviour, and the divergence is invisible
  in the notebooks when a reconciliation later disagrees. A task whose links
  include `$X.Status = SUCCEEDED` keeps `ALL_SUCCESS`, since that condition
  asks for success explicitly and `runIf` is per task rather than per link.
  A task with no incoming link is unchanged. The choice is reported as an
  assumption either way, naming that a downstream task will run on a failed
  upstream's output.

- **The DDL generator flags Sequence Generator keys, not just aggregates.**
  `infa_compat.sequence(...).assign()` ends in `value.cast("long")`, so a
  NEXTVAL arrives as BIGINT whatever the target declares, and Delta refuses
  the MERGE with `DELTA_FAILED_TO_MERGE_FIELDS` — found live on an SCD2
  surrogate key. The column is located by following CONNECTORs forward from
  the NEXTVAL port rather than by matching port names, so a rename on the
  way to the target cannot escape it. Verified against the golden cases: it
  flags `CUST_SK` in `c10_scd2_history`, the shape that failed on the
  cluster.

- **Golden case `c30_substr_indexof_signatures`.** Covers the two pyspark
  signatures that differ between 3.5 and 4.x — `SUBSTR` with a computed
  bound and `INDEXOF` — which the other 28 cases do not, because they use
  integer-literal `SUBSTR` only. Both of those were live-only findings, and
  the corpus could not have caught a regression to the 4.x-only form. A
  leading-`@` row is deliberately absent: whether Informatica returns an
  empty string or NULL for `SUBSTR(s, 1, 0)` is unverified, so no row relies
  on it.

- **Target DDL generated from the export's declared types.** `migrate` now
  writes `ddl/<table>.sql` per target, built from the export's own
  `DATATYPE`/`PRECISION`/`SCALE`. It is not applied -- creating tables in a
  catalog should not be a side effect of converting code.

  This replaces a silent fidelity loss. The runtime create-if-missing guard
  takes its column types from the DataFrame, so `NUMBER(10,0)` became
  `BIGINT` and a `SUM` over `NUMBER(12,2)` became `DECIMAL(22,2)`, with
  nothing failing. The declared type is derivable from the export, so it
  should not be guessed. Both paths stay: the DDL is the intended route and
  the runtime guard is the net for a first run where nobody applied it.

  Where the two disagree, the file says so rather than the tool choosing. A
  column fed by COUNT, SUM or AVG will not hold its declared type at
  runtime, so applying the DDL and then MERGEing can fail on a type
  mismatch; those columns are named in a `REVIEW` comment and `migrate`
  prints how many targets carry one. MIN and MAX are not flagged -- they
  return their argument's type.

  24 tests, the last of which executes every corpus target's DDL on a real
  Spark session, because a CREATE TABLE can read correctly and fail to
  parse. One is a drift guard requiring every base type known to
  `reconciler.comparators._TYPE_MAP` to be known to the DDL type table; it
  found 17 missing Postgres/MySQL spellings on its first run, against code
  written minutes earlier, each of which would have become `STRING`.

- **`abandoned_dataframes` catches a dropped join.** When a Joiner's
  master/detail side cannot be resolved the converter refuses to guess,
  emits `REVIEW REQUIRED` and passes the detail side through -- but the
  other side has already been prepared into a `df_in_*` variable that is
  then never read, and downstream code still references columns that only
  existed there. `unresolved_names` cannot see it: the variable IS
  assigned, and the column lives inside a string. Both checks passed on a
  notebook that could not run. `demo.sh` now fails on it. Scope is
  `df_in_*` only -- an unconsumed Router group is legitimate, and a first
  version that reported every unused `df*` flagged three correct Router
  groups in `data_quality_route.xml`.

- **A Joiner fixture marks its master ports.** The fixture
  carried neither a `MASTER`/`ISMASTER` port flag nor a `Master Source`
  attribute, so master/detail resolution correctly failed and the join was
  skipped -- in a fixture that had never been inspected, because it only
  started converting when the version gate widened to 10.x. A real
  PowerCenter export marks the master side; this one now does too, and the
  notebook emits the join.

- **Source fidelity no longer counts a construct named only in a comment.**
  Expression output fields and connector target fields are matched against
  comment-free code (`tokenize`-based, so a `#` inside a string literal is
  not treated as a comment); string literals are kept, because `F.col("X")`
  is how a column is really referenced. Transformation names still match
  against text including comments, since a transformation has no runtime
  artifact in PySpark and a comment is its only possible representation --
  and every fidelity report now says so, because it means the check cannot
  tell a generator that handles a Source Qualifier from one that merely
  mentions it. Found by running the LLM path over the same corpus as the
  rule-based path: it reported 0/12 fidelity gaps against 10/12, which was
  partly comment verbosity rather than better fidelity. With the fix the
  LLM path reports 1/12; the rule-based path is unchanged at 10/12.

- **Re-deploying a workflow updates its job instead of failing.** AIDP
  rejects a job create whose name already exists (`JOB_VALIDATE_0031`), so
  every deploy after the first failed on job creation while still reporting
  twelve successful notebook uploads. The deploy now looks the job up by
  name and, with `--overwrite`, replaces it via `PUT` (the service returns
  404 for `PATCH`, so the whole definition is sent); without `--overwrite`
  it is reported `skipped` rather than `failed`. Found on a live instance
  by deploying twice.

- **The deploy path refuses a DEFAULT master-catalog cluster.** Every AIDP
  workspace has a `Default Master Catalog Compute` cluster (`type: DEFAULT`)
  that accepts a job definition referencing it and then fails every run with
  `WORKFLOW_EXECUTION_0071`. The deploy now checks the cluster's type and
  refuses before uploading anything, since a job that can never run is a
  failed deploy rather than a partial one; it degrades to a warning when the
  cluster list cannot be read, because a read failure is not evidence the
  cluster is wrong. Found by deploying to a live AIDP instance and running
  the job — the request shape was valid, so the recording fake in
  `tests/test_deployer.py` had asserted it and passed.

- **Complete transformation coverage, both platforms.** Every transformation
  type Informatica supports — 39 in IDMC Cloud Data Integration, 33 in
  PowerCenter — has a defined, tested outcome. 29 distinct types emit Spark;
  5 preserve a foreign-language body verbatim (Java, Python, Velocity,
  Custom, External Procedure — the code *is* the logic, so translating it
  would be a guess); 16 are refused with a stated reason and a named
  alternative. Nothing reaches a generic "unsupported" message.
  `tests/test_transformation_coverage.py` asserts this over both platform
  lists, so a type added to the enum and forgotten fails the suite.
- **`Transformation.raw_type`** carries the export's own type string
  verbatim, so an unrecognised transformation is named rather than reported
  as "Unknown". It is also the evidence that will correct the IDMC type
  vocabulary, which is marked UNVERIFIED in `transformation_types.py`
  because no real IDMC export has been parsed.
- **`engine/infa2aidp/transformation_types.py`** — one resolver shared by
  both parsers, so PowerCenter's `Lookup Procedure` and IDMC's `lookup` land
  on the same enum member.
- **`references/migration-inventory.md`** — everything that has to move from
  a real estate for a pipeline to run, including the parts no export
  contains: sequence current values, credentials, scheduler integration,
  server-side files, unresolved shortcuts.

- **SQL semantics pinned in every generated notebook.**
  `spark.sql.ansi.enabled=false` and `spark.sql.storeAssignmentPolicy=LEGACY`,
  set explicitly with the reason inline. Informatica evaluates permissively —
  a failed cast, a divide by zero or an overflow yields NULL and a row error,
  not an aborted run — and migrated logic was written against that. AIDP moves
  from Spark 3.5 to Spark 4, where ANSI defaults from off to **on**, so a
  notebook inheriting the cluster default would silently change behaviour the
  day the runtime is upgraded: rows that were NULL become a failed job. Pinned
  rather than inherited so the upgrade is a non-event. See
  `references/spark-4-upgrade.md`, which also lists the work that becomes
  possible once AIDP is on Spark 4 — chiefly XML Parser and XML Generator,
  deliberately deferred rather than built twice.

- **Version gate widened to PowerCenter 10.x, and 186/187 re-read as
  10.1/10.2.** Scope is deliberately wider than the releases Informatica
  still supports — 10.4 left standard support in March 2024 and 10.5.x in
  March 2026, so every surviving PowerCenter estate is on an unsupported
  release by definition, and 10.1/10.2 is where a large share of them sit.
  Refusing those would exclude much of the population this tool exists to
  serve. `InfaVersion.V10_5` is a distinct member so a report can still name
  10.5 precisely.

  The 186/187 mapping is **inferred, not verified**: two reviewers with
  Informatica experience place them at 10.1/10.2 with 9.6.1 at 184, against
  a prior reading of 9.x supported only by a fixture we wrote ourselves. It
  now decides an outcome rather than a message — if the reviewers are wrong,
  a genuine 9.x export is converted rather than refused — so the version
  detail string carries the dispute into the generated report. One real
  export from a known release settles it.

  Consequence worth noting at the time: a fixture carrying 186.95 was
  refused and now converted, so the corpus grew by one mapping and the
  published conversion rate fell from 64% to 50%. The engine did not change;
  a mapping that was excluded from the denominator entered it.

### Fixed

- **Golden-output execution harness, and what it found.** `tests/golden/`
  migrates a simulated PowerCenter export, runs every generated cell on a
  local Spark 3.5 session (Delta MERGE, `saveAsTable` and sequences stood
  in by in-memory equivalents) and compares the target rows with rows
  derived by hand from Informatica's documented semantics. The string
  assertions elsewhere in the suite check what code is emitted; these
  check what it computes. First cases and the defects they exposed:
  - A connector rename onto an existing column name (an Expression's
    input-only `CUST_NAME` fed from `CUST_NAME_CLEAN`) left two columns
    of that name: `AMBIGUOUS_REFERENCE`. Renames are now one simultaneous
    projection (`_rename_cols`).
  - A divergence note on INITCAP/MD5/SOUNDEX/TO_DECIMAL replaced the
    whole port with NULL, and on an Update Strategy rejected every row.
    The conversion now stands and the note travels with it; INITCAP is
    exact (capitalises after any non-alphanumeric, `Mcdonald'S` quirk
    included).
  - A Router's DEFAULT group dropped rows whose group condition was NULL.
  - A Source Filter or Lookup Source Filter using `TO_DATE(..., 'MM/DD/YYYY
    HH24:MI:SS')` was dropped and the whole table read: the bind-variable
    guard matched `:mm` in the converted mask.
  - A date/time `$$PARAMETER` arrived as text; `_param()` now returns the
    declared MAPPINGVARIABLE type, and SQL filters get the raw text
    (`_param_text`) as Informatica substitutes it.
  - `REPLACECHR`/`REPLACESTR` CaseFlag was inverted (0 or NULL is
    case-INsensitive); a NULL NewChar raised a TypeError instead of
    deleting; REPLACESTR takes several OldStrings.
  - `CONCAT`/`||` of all-NULL operands returned `''`, not NULL.
  - `TO_INTEGER(3.7)` was `3.7.cast(...)`; a compound argument bound the
    cast to its last operand; a constant port (`4`) was not a Column.
  - An output port reading a variable port declared below it was treated
    as a previous-row reference and NULLed; Informatica evaluates all
    variable ports before any output port.
  - `IS_NUMBER`/`IS_DATE`/`IS_SPACES`/`REG_MATCH` into an integer port
    are 1/0, not BOOLEAN.
  - `SETMAXVARIABLE`/`SETMINVARIABLE`/`SETVARIABLE` were unmapped (NULL);
    they now return the running value, with a note that the variable is
    not persisted for the next run.
  - A Source Qualifier's "Number Of Sorted Ports" was ignored.

- **Dataflow wiring: parallel branches overwrote each other.** The
  generator shared one `df` variable down every chain and across
  branches, so two transformations fanning out of one producer ran one
  after the other, four Joiners into four targets all wrote `df` (every
  target got the last Joiner's rows), and an Aggregator right after a
  Router aggregated all rows instead of its group's. A transformation now
  reuses its input variable only when it is that input's sole reader;
  otherwise it gets its own `df_<name>`. A transformation fed by several
  row-aligned instances (an SQ and a Lookup on it) reads the one whose
  lineage carries the others and applies every connector's rename at
  once. Golden cases c02 (four join types), c04 (conditional
  aggregates, NULL groups), c05 (Rank + Sorter), c06 (Union + Sequence),
  c13 (Normalizer), c14 (mapplet used twice), c16 (IDMC Joiner, Lookup,
  Router, Aggregator) found it and the following:
  - Several instances of one target definition (SCD2's insert/expire
    paths) collapsed into copies of the first; each instance is now its
    own target on the definition's table.
  - A multi-target mapping's validation cell selected the FIRST target's
    columns from `df_final`, failing for any renamed column; a multi-
    target INSERT was upgraded to a keyed MERGE (a re-run updated rows
    meant to be appended).
  - Rank read attributes a real export does not carry ("Top/Bottom",
    "Number Of Ranks", the rank port flag and `EXPRESSIONTYPE="GROUPBY"`
    are read now) and numbered ties 1, 2 instead of 1, 1.
  - Aggregator group-by ports marked `EXPRESSIONTYPE="GROUPBY"` were not
    group-by ports; a passthrough port carrying its own name as
    EXPRESSION failed `MISSING_AGGREGATION`.
  - Sorter Distinct compared upstream columns the Sorter does not have.
  - Union (a real export's `TYPE="Custom Transformation"
    TEMPLATENAME="Union Transformation"`) resolved to CUSTOM and passed
    one input through; it now maps each input group's connected columns
    by position onto the output ports.
  - A mapplet instance (`TYPE="MAPPLET"`) was dropped silently.
  - Sequence Generator started at Current Value + 1; PowerCenter's first
    NEXTVAL is the Current Value.
  - A relational Normalizer (`SALES_in1..N`, `GK_`, `GCID_`) stacked the
    values into the key column and dropped NULL occurrences; each source
    row now yields N rows with GCID = occurrence index.
  - Writer flags (Insert, Update as Update, Delete, Truncate) are read
    from the target's `SESSIONEXTENSION TYPE="WRITER"` as a real export
    writes them.

- **Flat files, Transaction Control, dynamic lookups, stateful variable
  ports, and a defect only a live AIDP run could show.** Golden cases c18-c24
  (Transaction Control, dynamic lookup cache, overlapping Router groups, an
  aggregate re-joined to its source, variable-port state, delimited and
  fixed-width flat files) and a live run of every golden case on an AIDP
  cluster (Spark 3.5.0, real Delta tables) found:
  - **`DeltaTable.forName` with a one-part name fails on AIDP** once the
    session has USEd an AIDP catalog (`IndexOutOfBoundsException: 1`),
    while `tableExists` accepts the name -- so every Sequence Generator
    (its counter table `_infa_compat_sequences` is one-part) and every
    target named without an owner failed at the MERGE. New
    `infa_compat.delta_name()` qualifies such names with the current
    catalog and schema; the sequence backend, update strategy, SCD2 and
    the generated MERGE go through it.
  - **A MERGE or append with the DataFrame's types, not the table's,
    fails on Delta.** A Sequence Generator's BIGINT into a NUMBER(10,0) SCD2
    key was refused (`DELTA_FAILED_TO_MERGE_FIELDS`), and so was the append
    of three target instances' rejects into their shared reject table.
    New `infa_compat.align_to_table()` casts to the target's column types,
    as Informatica converts to the target port's type; update strategy,
    SCD2, the reject sink and the generated MERGE use it, and an
    unconnected target column is a NULL of the declared type. The golden
    harness's MERGE and append now refuse a type mismatch the same way.
  - **Flat files were read as catalog tables.** A `DATABASETYPE="Flat
    File"` source became `spark.table(<definition name>)`. Delimited and
    fixed-width sources are now read from the session File Reader's path
    (a `SRCFILE_<SOURCE>` job parameter overrides it with an AIDP path),
    with the export's delimiter, quote character and skipped header rows,
    each field cast to its declared type (empty -> NULL). Flat-file
    targets are written as delimited text (`TGTFILE_<TARGET>`, header and
    append options from the session writer, dates as MM/DD/YYYY
    HH24:MI:SS) and get no DDL or Delta catalog assertion.
  - **Rolled-back transactions landed.** Transaction Control passed every
    row through; transactions are now numbered over the row order and
    those ended by TC_ROLLBACK_BEFORE/AFTER are removed (Commit On End Of
    File honoured). The TC_* constants convert to their codes, and the
    Designer's "Transaction Control Condition" is read.
  - **A dynamic lookup cache was a static one**, and `NewLookupRow` did not
    exist (`UNRESOLVED_COLUMN`). With Insert Else Update, each key is now
    flagged 1/2/0 against the table on its first arrival and against its
    previous arrival after that, so a key arriving twice is inserted once.
    Update-strategy writes apply several rows for one key in row order:
    the last update wins, the first insert lands.
  - **Stateful variable ports were NULL.** `v_PREV = v_CURR` (previous
    row), `V + x` (running total), `IIF(c, V + x, y)` (running total that
    restarts) and `IIF(c, V, y)` (carry forward) are now windows over the
    row order, starting from the type's initial value (0, '', 01/01/1753).
    Other self-references keep the REVIEW placeholder.

- **Target load order, grouped Unions and date/string functions.** Golden
  cases c25-c29 (a parent/child Target Load Plan, a sorted-input
  Aggregator into a sorted Joiner, a Router split re-merged by a Union,
  nested DECODE/IIF with date and string functions, and a Data-driven
  Update Strategy forwarding rejects into a Filter) found:
  - **Target load order was ignored.** `<TARGETLOADORDER>` was not read,
    so every pipeline's cells were interleaved and a child pipeline's
    lookup on the parent table saw the table before this run loaded it
    (c25: new departments looked up as -1). The notebook is now emitted
    one load order group at a time -- read, transform, validate and write
    -- and a lookup on a table loaded by an earlier group is not
    snapshotted.
  - **A FOREIGN KEY column was a key.** Any `KEYTYPE` other than `NOT A
    KEY` set `is_key`, so a child target's MERGE matched on its foreign
    key. Only `PRIMARY KEY` and `PRIMARY/FOREIGN KEY` are keys now.
  - **A grouped Union lost its branches' columns.** The producer-side
    rename of each branch to the Union's port names was also applied when
    the Union's input groups map ports by connector, dropping them
    (c27, `UNRESOLVED_COLUMN`).
  - **An omitted IIF value2 was always NULL.** It is 0 for a numeric
    value1, an empty string for a string value1 and NULL otherwise.
  - **`ROUND(date, fmt)` was refused**, and **`INSTR` with an occurrence
    or a negative start** was refused. `ROUND` now follows the documented
    cutovers (MM from day 16, DD from 12:00, HH and MI from the half, YY
    from July); `INSTR` finds the nth occurrence, searches backward from
    a negative start and treats a start of 0 as 1. `comparison_type` is
    still refused.
  - **Insert-only and truncate appends kept the DataFrame's types.** They
    now go through `infa_compat.align_to_table()` like the MERGE paths, so
    a `DECIMAL(11,0)` into a `DECIMAL(10,0)` column is cast, not refused.

- **Lookups and update-strategy writes.** Golden cases c07 (SQL override,
  two-column condition, First/Last/Report Error on duplicate keys), c08
  (unconnected `:LKP` in an expression), c09 (effective-dated, non-equi
  condition), c10 (SCD Type 2 wizard mapping) and c11 (data-driven
  insert/update/delete/reject) found:
  - Lookup ports are named after the lookup source's columns, so a lookup
    on the target returning `CUST_NAME` beside the source's `CUST_NAME`,
    or two lookups returning `SEGMENT`, were `AMBIGUOUS_REFERENCE`. A
    connected Lookup's ports are now carried as `<lookup>__<port>` until
    a connector renames them, and a condition column can also be returned
    (`ISNULL(LKP_PRODUCT_ID)`, the usual insert test, used to lose it).
  - Only equi-joins were possible: `EFF_FROM <= IN_DATE` was split on
    `=`. `infa_compat.lookup_join` takes the whole condition, resolves
    First/Last per input row ordered by the lookup ports (Informatica's
    cache ORDER BY, NULL highest) -- it ordered by physical row order --
    and Report Error per input row.
  - `:LKP.NAME(args)` became `_lookup_NAME(...)`, which nothing defined
    (NameError). It is now a join inside the Expression on the call's
    arguments, returning the RETURN port; the same call written three
    times is joined once. `LOOKUP` and `RETURN` in PORTTYPE are parsed.
  - A lookup on a table the mapping also writes is snapshotted before any
    write (`localCheckpoint`): Informatica's static cache is built before
    the session writes, Spark reads lazily.
  - Update Strategy writes keyed on a "natural" key that dropped a
    declared `_SK` primary key, so an SCD2 new version matched the old
    row and was never inserted; they use the declared key now. DD_UPDATE
    set every column (`UPDATE SET *`), nulling the unconnected ones; it
    sets the connected columns only. Unconnected target columns are NULL
    in a multi-target write instead of failing the select.
  - A Sequence Generator feeding two branches numbered only the first;
    each consumer now draws from the one sequence.
  - Several port-exact Lookups on one Source Qualifier chain in place
    (they only add columns), so a target fed by all of them sees all of
    their columns.
- **Workflows built from worklets, reused sessions and parameter files.**
  Five losses in the workflow-to-job path, none of which raised:
  - Worklets were never expanded. A reusable (folder-level) or nested
    non-reusable `<WORKLET>` is now flattened into the job: its sessions
    become tasks keyed `<worklet>__<instance>`, and links into and out of
    it attach to its entry and exit sessions, through any depth of
    nesting. A worklet whose definition is not in the export is still
    reported as before.
  - Sessions were keyed by `TASKNAME` while links name instances, so a
    renamed instance of a reusable session lost its dependencies and one
    session run twice became one task.
  - A mapping run by two sessions gave only the last one a notebook path;
    the other task pointed at a placeholder. Every session of the mapping
    now points at the one notebook (generated from the first session in
    export order, with a warning if the sessions load differently).
  - Session `$$` overrides and `.par` values never reached the job:
    `workflow.sessions` held names, so the parameter branch was dead
    code. Each task now carries the `$$` values from every parameter-file
    section that applies to it, narrowest winning as in PowerCenter
    (`[Global]` < `[F.WF:wf]` < `[F.WF:wf.WT:wl]` < `[F.session]` <
    `[F.WF:wf.WT:wl.ST:instance]`). `$DBConnection`/`$InputFile` values are
    reported, not applied.
  - The parallel batch path (`--workers > 1`) wrote no job definitions.

  A `$X.Status = SUCCEEDED` link condition on X's own link is now treated
  as applied, since it is exactly `dependsOn` + `runIf: ALL_SUCCESS`; it
  used to be reported on nearly every workflow. An unconditional link out
  of a session is stated as an assumption: PowerCenter runs the next task
  even after a failure, the job does not.
- **Reconcile, CLI and peripheral modules: tests and the defects they
  found.** Eight test files (`tests/test_cov_*.py`, about 300 tests) take the
  reconciler, analyzer reports, compatibility checker, custom rules,
  optimizer, parameter-file parser, AIDP client and CLI from 10-58% line
  coverage to 97-100%, with every external call faked. They found:
  - `reconcile` exited 0 on a failed reconciliation: it counted `PASS`/`FAIL`
    while the reconciler reports `PASSED`/`FAILED` -- a false green in CI.
  - `optimize --auto-apply` wrote the joined code cells over the `.ipynb`,
    replacing the notebook with plain text. It now edits code cells in place.
  - The generated reconcile notebook never compared values on matched rows
    (the check compared each column with itself and was unused).
  - Data reconciliation passed a double-loaded target (rows keyed into a
    dict collapsed), matched `'00123'` with `'123'` (text compared as
    floats), ignored `date_tolerance_seconds`, reported equal infinities as
    different and scored an empty source against a loaded target as 100%.
  - MySQL `TINYINT(1)` was not treated as boolean and Spark `ByteType` not
    as numeric; a SQL Server sample read used `LIMIT`; a flat-file source
    silently ignored the `where` clause (it now refuses it).
  - Parameter-file references resolved across sections -- one session's
    `$$FILE=$$DIR/a.csv` took another session's `$$DIR` -- and widget
    defaults were not escaped.
  - The compatibility checker read session features from `parameters`
    (they are `properties`) and named no column in datatype warnings; the
    report dropped the data detail when nothing was compared on the source
    side; the AIDP client could not read a bare-list objects response.

- **The type resolver's substring fallback.** With the enum expanded to
  cover both platforms it mis-resolved rather than helped — `Parse` is a
  substring of `XML Parser`, `Input` of `Input Transformation` — and which
  wrong answer you got depended on enum declaration order. Replaced with
  exact matching against an explicit alias table. No fuzzy matching: an
  unrecognised type stays unrecognised rather than being guessed at.
- **Mapplet ports were a gap inside a covered feature.** `Mapplet`
  converted while its own `Input`/`Output` transformations emitted
  manual-conversion markers, and a renamed port silently lost its column
  downstream.
- **The demo corpus was measuring stubs.** Eight fixtures were flattened
  copies of complete mappings that already existed in the tree — same
  filenames, no `INSTANCE` elements — so every one produced a placeholder
  read and no target write, and the conversion metric counted the result.
  With the complete copies in place, `runnable` went 3/11 to 11/11 and
  `converted` 2/11 to 7/11. **The engine did not change**; the instrument
  did. The no-metadata path is still pinned, deliberately, by
  `flattened_no_instances.xml`.
- **A completeness test that excluded what it should have failed on.** The
  assertion that no transformation reaches the generic handler skipped eight
  types first, and five were skipped because nothing handled them. Those
  five — Application/MQ/XML Source Qualifier, XML Parser, XML Generator —
  are now handled, and the exclusion list carries only Source and Target,
  which the read/write path legitimately owns.

### Added (initial release)

- **Source formats.** IDMC/IICS mapping JSON and PowerCenter 10.5
  repository XML. Both formats reach the same intermediate representation,
  so everything downstream of the parsers is format-agnostic.
- **The engine** (`engine/infa2aidp/`). Parsers, an expression converter, a
  transformation converter, and two notebook generators — a deterministic
  rule-based compiler and an LLM-driven generator — behind a single CLI with
  ten commands: `discover`, `analyze`, `migrate`, `deploy`, `reconcile`,
  `optimize`, `review`, `rag`, `lineage`, `version`.
- **The plugin surface.** 11 skills, 5 slash commands, and 2 read-only
  reviewer agents wrapping the engine for natural-language use inside
  Claude Code (`skills/`, `commands/`, `agents/`) — see the README for the
  full list. All of it drives the same `infa2aidp.cli` underneath; none of
  it is a second implementation.
- **`infa_compat`** (`engine/infa_compat/`), the cluster-side runtime
  library for Informatica semantics that cannot be expressed as a pure
  Spark column expression: sequence generators, lookups, mapping
  parameters, SCD Type-2, update strategy, date masks, and `DECODE`.
  Generated notebooks call this library rather than inlining stateful
  logic, and both generators are wired to it. Its supported surface is
  documented in `engine/infa_compat/SUPPORTED_OPERATIONS.md` and is a
  closed API.
- **Pluggable write strategies** (`generators/write_strategies.py`).
  `DeltaWriteStrategy` handles managed-Delta `MERGE`/append/overwrite.
  `AdwWriteStrategy` handles external/ADW catalogs — JDBC overwrite, with
  upsert via a staged Oracle `MERGE INTO` issued from the driver, because
  external catalogs support neither Spark `MERGE` nor DDL. Neither
  strategy ever silently downgrades an unsatisfiable upsert to an
  overwrite.
- **Source-fidelity checking** (`generators/source_fidelity.py`). After a
  migration, this compares the generated notebook's transformations,
  expressions, and connectors against the *raw export* rather than against
  the parsed representation, so a parser gap cannot hide behind its own
  output. Gaps are reported in `fidelity_report.md` as a human-review
  signal, not a build gate.
- **Static name resolution** (`generators/code_validation.py`), which
  catches `NameError`-class defects in generated notebooks that an
  `ast.parse` syntax check passes over.
- **A release gate** (`tests/test_release_gate.py`): nine go/no-go criteria
  covering the test floor, `demo.sh` output, plugin manifest validity,
  version agreement across all four locations, repo-layout constraints, the
  default model, dual-source-format proof, and fidelity reporting. Runs
  under pytest or standalone.
- **An offline demo** (`demo.sh`) that migrates the bundled corpus in
  `tests/fixtures/` with no network and no Informatica install, producing 11
  notebooks.

### Fixed

- **Migrated jobs no longer inherit a fabricated schedule.** The workflow
  generator returned a hardcoded `0 0 2 * * ?` (daily 02:00 UTC) whenever
  it could not read a schedule off the workflow — which was always,
  because the parser never read `<SCHEDULER>`. Every migrated job
  deployed cleanly, reported no error, and ran at the wrong time.
  `<SCHEDULER>`/`<SCHEDULEINFO>` is now parsed, exact intervals convert to
  a real Quartz cron at the source `STARTTIME`, and anything that cannot
  be converted exactly yields **no schedule** plus a review item quoting
  the source values. An unscheduled job is visibly unscheduled; a job with
  a plausible wrong schedule is not.
- **A workflow's non-session task instances are no longer discarded at
  parse time.** Worklet, Command, Email, Decision, Timer, Event-Wait,
  Control and Assignment instances were read into a local dictionary that
  was never attached to the `Workflow`, so an incomplete job DAG was
  indistinguishable from a complete one. All instances are retained, and
  anything that cannot become a job task is reported.
- **The schedule timezone is reported as an assumption rather than passed
  off as derived.** Removing the fabricated cron left a fabricated
  *timezone*: PowerCenter records `STARTTIME` in the Integration Service's
  local time and stores no zone, so a converted `03:30` was emitted as
  03:30 **UTC**. For a US-hosted service that job runs five or six hours
  off — the same class of silent wrong-time defect, one level down. The
  hour is still derived from the source; the timezone is now flagged for
  the operator to confirm and correct in AIDP.

  This is tracked separately from translation failures. A generated
  workflow now carries two lists: constructs with no representation in the
  job (empty means it came across whole) and values the generator had to
  supply that may be wrong. Merging them would mean every scheduled
  workflow looked incomplete and "nothing was lost" would stop being a
  usable signal.
- **A session with no migrated notebook is now reported.** Its task
  previously got a silently invented `/Migrated/<name>` path that would
  fail only at run time.

#### From the 2026-09 review (real-export shapes, semantics, deploy path)

Every item below was reproduced by running the tool over a full-shape
PowerCenter export with one construct added, then pinned in
`tests/test_review_regressions.py`, `tests/test_deployer.py` and the
existing suites.

*Notebooks that could not run*

- **Any session with a top-level `ATTRIBUTE` produced a SyntaxError.**
  Session attributes ("Treat source rows as", "Commit Interval", ...)
  were filed as parameters and emitted as Python assignments named after
  the raw key. They now live in `Session.properties`; the parameters cell
  defines `_param(name)` over `$$` names only, with the mapping's
  `DEFAULTVALUE`s, and the expression converter emits
  `F.lit(_param('NAME'))` instead of a bare `spark.conf.get('NAME')` on a
  key nothing set.
- **A target column with no source expression raised `NameError`.** The
  LOAD_DATE-style defaults were added to a hard-coded `df` while the
  pipeline was on `df_source`. They are added to the DataFrame the
  pipeline is actually on.
- **Lookups joined on the lookup's own input port** (`IN_CUST_ID`), a
  column the pipeline does not carry. The generator now resolves each
  lookup input port to the upstream column through the connectors.
- **An incremental load with zero new rows failed the notebook**
  (`assert row_count > 0`). It is a warning; the Informatica session
  succeeded with 0 rows and so does the notebook.

*Real-export constructs the parser dropped*

- **Folder-level reusable transformations** (`REUSABLE="YES"`, referenced
  from the mapping by `INSTANCE` only) were not in the mapping at all --
  the notebook generated with that logic silently absent. They are
  resolved from the folder definition and named after the instance.
- **Mapplets** were not parsed; every connector through a mapplet
  instance dangled. A `<MAPPLET>` is now expanded inline (internal
  instances prefixed `<instance>__`, boundary Input/Output ports rewired).
- **Source Qualifier `Source Filter`, `Select Distinct` and `User Defined
  Join`** were not read, so an incremental extract read the whole table.
  Filters and DISTINCT are applied; a user-defined join is a review item.
- **A SQL override with a JOIN/UNION/sub-select was read as the base
  table plus its WHERE** (with an alias Spark does not know). Only a
  single-table override is translated; anything else is quoted under
  `REVIEW REQUIRED` and the base table is read.
- **`Lookup policy on multiple match` and `Dynamic Lookup Cache`** were
  ignored: "Report Error" ran as "Use First Value", a dynamic cache ran
  as a static join. The policy is passed to `infa_compat.cached_lookup`;
  a dynamic cache is a REVIEW REQUIRED item.
- **`TARGET/@CONSTRAINT` (DDL text) was read as a load strategy.** The
  load strategy now comes from the session ("Treat source rows as" +
  the target instance's Insert/Update/Delete/Truncate flags,
  `Session.load_strategy_for`), falling back to INSERT.

*Expression semantics*

- **`=`/`<>` emitted `eqNullSafe`.** Informatica comparisons are
  three-valued like Spark's; `NULL <> 'X'` was TRUE, so a Filter on
  `STATUS <> 'CLOSED'` passed NULL rows the session dropped and an SCD
  change-detect flagged NULL->NULL as changed. Plain `==`/`!=` now.
- **`||` emitted `F.concat`**, which is NULL if any operand is NULL;
  Informatica's `||` skips NULLs. `concat_ws('')` now, matching `CONCAT`.
- **`TO_DECIMAL(x, 2)` became `decimal(2,0)`** -- the second argument is
  the scale. `decimal(38, scale)` now.
- **Update Strategy `IIF(ISNULL(LKP), DD_INSERT, DD_UPDATE)` was inverted**
  and referenced columns named `DD_INSERT`/`DD_UPDATE`. The constants are
  integer literals (0/1/2/3) and the whole expression converts as one;
  the single-target write routes through `apply_update_strategy` /
  `write_update_strategy` (it used to run the generic keyed MERGE,
  upserting deletes and inserting rejects), and `DD_STRATEGY` survives the
  target-column select.
- `LPAD`/`RPAD` without a pad string, two-argument `LTRIM`/`RTRIM`,
  `REG_MATCH` (whole-value match, `str` pattern), `ADD_TO_DATE` (kept
  time of day; hour/minute/second form was an f-string SyntaxError with a
  column amount), `TO_INTEGER` (rounds by default), `SUBSTR(x, 0, n)`,
  `IN(...)` as a function, `MOD`, `TRUNC(numeric, n)`, `SESSSTARTTIME` /
  `$$$SessStartTime` (bound once per run), `'it''s'` and backslash
  literals, `--`/`//` comments, aggregate filter conditions, and a set of
  common functions (`CEIL`, `FLOOR`, `INITCAP`, `LAST_DAY`,
  `GET_DATE_PART`, `DATE_COMPARE`, `STDDEV`, `VARIANCE`, `PERCENTILE`,
  ...). A character no token matches now refuses the expression instead
  of vanishing (`A % 2` used to convert to `A`).
- Sorter NULL placement follows "Null Treated Low"; a non-aggregate
  Aggregator port returns the group's LAST row (Informatica's rule);
  Stored Procedure output ports are NULL placeholders under REVIEW
  REQUIRED, not the literal `'N'`.

*Wiring (second pass: which DataFrame feeds what)*

- **Router INPUT group was an output group.** Every PowerCenter Router
  carries `<GROUP TYPE="INPUT">`; it was parsed as a group with no
  condition, i.e. as the DEFAULT group, giving every Router a phantom
  output. Skipped now.
- **Router DataFrame names disagreed between converter and generator**
  (`df_GRP_SMALL` emitted, `df_grp_small` expected) -- a `NameError` at
  the first consumer for any upper-case group name. One normalisation.
- **Router groups reached their consumers and targets by name heuristic or
  position** ("valid"/"reject" substrings, else alphabetical order; target
  *i* got group *i*). The port `GROUP`/`REF_FIELD` attributes are read and
  the connectors leaving a Router resolve to the group DataFrame its ports
  belong to; the old heuristics remain only for fixtures without `GROUP`.
- **Connector renames moved to the consumer.** A connector whose FROMFIELD
  differs from its TOFIELD renamed the *producer's* DataFrame in place --
  wrong for a producer shared by two consumers, and never emitted at all
  for a Source Qualifier. Each consumer now gets its own renamed copy
  (`df_in_<consumer>[_<predecessor>]`); targets get
  `df_tgt_<target>`/a final rename before the target-column select.
- **Joiner conditions used guessed base names.** `_M`/`_D`/`_1`/`_2`
  suffixes were stripped to a "real" column that did not exist when the
  convention did not hold. Port names are used verbatim on each side's
  renamed copy; the MASTER flag decides which side is which.
- **The previous-row variable-port idiom converted as a plain column.**
  `v_PREV = v_CURR` declared above `v_CURR = KEY` reads the previous row
  in Informatica; it read the current row in Spark. A port referencing a
  variable declared below it is a REVIEW REQUIRED item (Window/lag needed).
- **Workflow links through a dropped task lost the dependency.**
  `s_a -> cmd_archive -> s_b` made `s_b` independent; it now depends on
  its nearest session ancestors through any non-session nodes.
- Key columns shared by two targets are no longer listed twice.

*`infa_compat` (cluster runtime)*

- `Sequence.assign` materialised every value in a Python list on the
  driver; it reserves one block and assigns `first + (row_number-1) *
  increment` as a column expression.
- `apply_update_strategy` drops the routing column from each partition
  (a `whenMatchedUpdateAll` would otherwise try to write `DD_STRATEGY`).
- `scd2_merge` stamps `high_date` cast to the effective-to column's type
  instead of a string.

*Deploy path*

- `deploy` could not construct its client (`AIDPClient(host, token)`
  against a `(region, instance_id, signer)` constructor), called four
  client methods that did not exist, globbed `*.py` while `migrate` writes
  `*.ipynb`, and POSTed Databricks-Jobs-shaped JSON (`task_key`,
  `notebook_task`, `quartz_cron_expression`). It now signs with the OCI SDK
  (`--region/--instance-id/--workspace-key/--profile/--cluster-key`), uses
  the `uploadFileMeta` PAR upload, and the workflow generator emits the
  AIDP job body (`taskKey`, `NOTEBOOK_TASK`, `notebookPath`, `dependsOn`,
  `runIf`, `schedule`, `maxConcurrentRuns`) with
  the cluster attached at deploy time. `--host`/`--token`/`--cluster-id`
  and `AIDP_HOST`/`AIDP_ACCESS_TOKEN` are gone (they were never used for
  authentication). Still not run against a live workspace.

*Other*

- Every text-mode file operation passes `encoding="utf-8"`; on a default
  Windows console 7 tests errored and the release gate failed before.
- Reconciler type normalisation never matched Spark type names
  (`"StringType()"`), so the schema check failed every column and the
  aggregate check compared nothing and passed; fixed, with `NUMBER(*,0)`
  no longer raising.
- `source_fidelity` honours the export's declared encoding (Windows-1252
  exports made the check silently "not run").
- Confidence scorer no longer scores every lookup LOW as "unconnected"
  (the property it tested for is on every lookup) and no longer rates a
  Sequence Generator HIGH.
- Hallucination detector no longer suppresses `F.nvl`/`F.nullif`/
  `F.ifnull`/`F.decode`/`F.substr`, which exist in Spark 3.5 and which the
  validator lists as valid; the agentic pipeline runs the detector on the
  attempt it actually chose.
- LLM prompt: rule 28k (NULL comparison) no longer asserts that
  Informatica's `!=` treats NULL as different; rule 36 (Sorter Distinct)
  no longer tells the model to add a unique-ID column to the dedup set.
- Crawler no longer crashes on the first `<Name>` element
  (`Element.getparent` does not exist in stdlib ElementTree).
- Notebook metadata carries no Databricks cell metadata; the parameters
  cell no longer mentions `dbutils.widgets`; `.prm` parameter values are
  emitted through `json.dumps`.

*Peripheral (third pass: the tools around the converter)*

Each item was reproduced by calling the module directly and is pinned in
`tests/test_peripheral_regressions.py`.

- **The generated-code validator rejected working notebooks.** A lambda's
  parameters and a walrus target were reported as read-before-assigned
  (`sorted(cols, key=lambda c: ...)` flagged `c`), so `validate` failed code
  that ran. Both are bound now; a genuinely missing name inside a lambda
  body is still reported.
- **A UTF-8 BOM made an export undetectable.** Files with an unknown
  extension were peeked at as UTF-8, so a BOM (routine on exports saved from
  Windows tools) arrived as U+FEFF and every such file was "Cannot detect
  format". Peeked with `utf-8-sig`.
- **Row-count-only reconciliations reported `Schema: FAILED`.** The
  markdown report printed a schema verdict whenever `schema_diffs` was not
  `None`, which it never is (default `[]`), with `schema_match` defaulting
  to `False`. The line appears only when a schema comparison ran.
- **The reconcile dashboard read a JSON shape the reconciler never writes**
  (`tables`/`PASS`/`table_name`/`schema_mismatches` against a report with
  `results`/`PASSED`/`config_name`/`schema_diffs`), so it found zero
  tables and rendered `ALL CHECKS PASSED` for every run, including failed
  ones. It reads the report's keys (the legacy spelling still works) and an
  empty result set is `NO RECONCILIATION RESULTS`, not a pass. The report
  path is embedded with `repr()`, so a Windows path no longer turns the
  setup cell into a `SyntaxError`.
- **Parameter files: `$$` references never resolved and `ST:` was part of
  the session name.** The reference regex matched `$DESC` inside `$$DESC`,
  which is not a key, so `$$REF=$$DESC/x` stayed literal; scope headers of
  the form `[Folder.WF:wf.ST:s_m]` produced session `ST:s_m`. Both fixed.
- **The RAG store returned another transformation's code as a
  0.95-confidence hit.** Fingerprints carried function names but not
  expression structure, so `ROUND(AMT*RATE,2)` and `ROUND(QTY/DOZ,0)` were
  the same entry. The fingerprint now includes an identifier-blanked
  skeleton (`ROUND(_*_,2)`), and two Expressions whose skeletons share
  nothing are never "similar", while a renamed port still matches exactly.
- **IICS package extraction had no size or entry bound** before the
  per-file 10 MB check could run. Capped at 512 MB uncompressed / 20,000
  entries.
- **The DOCTYPE guard checked the wrong thing.** Any `<!DOCTYPE>` passed as
  long as the string `POWRMART` appeared somewhere in the file; a foreign
  `SYSTEM "http://.../evil.dtd"` with a `powrmart.dtd` mention in a comment
  was accepted. Each DOCTYPE must now be exactly PowerCenter's
  `<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">`.

*Live run (fourth pass: the first deploy to a real AIDP workspace)*

On 2026-09-24 `migrate` + `deploy` were run against an AIDP workspace in
us-ashburn-1 (cluster on Spark 3.5.0, Python 3.11) and the generated notebook
was executed cell by cell on the cluster. Upload, job creation and the job
run all worked; what follows is what did not, each pinned in
`tests/test_live_run_regressions.py`.

- **Every first run failed twice before writing a row.** The catalog-type
  assertion treated a target that does not exist yet as "not Delta" and
  raised; and the INSERT strategy's Delta `MERGE` needs an existing table
  (`DELTA_MISSING_DELTA_TABLE`) while the migrator emits no DDL. Both cells
  now check `spark.catalog.tableExists` first: the assertion skips a missing
  target with a warning, and the write cell creates it empty from the
  batch's schema (flagged REVIEW -- types then come from the DataFrame, not
  the Informatica target definition). `infa_compat.write_update_strategy`
  got the same guard.
- **`INDEXOF` ran only on Spark 4.** It compiled to `F.array_position(array,
  column)`, and pyspark 3.5's `array_position` takes a literal value, so
  the generated code raised "Column is not iterable" on the AIDP runtime.
  Emitted as a CASE chain now (1-based position, 0 when absent, NULL for a
  NULL search value) -- same result on every Spark version.
- **The deployer accepted any workspace path.** Git Bash rewrote
  `/Workspace/Migrated` into `C:/Program Files/Git/Workspace/Migrated`,
  and the tool created a `C:` folder in the workspace and a job pointing at
  it. `DeployConfig` now normalises the path and refuses anything outside
  `/Workspace`, naming the Git Bash rewrite when it sees one.
- **A second `deploy` failed with `JOB_VALIDATE_0031`** ("job already
  exists"): `--overwrite` covered notebooks but never the job. The client
  now lists jobs with paging (25 per page, `limit` <= 100, `opc-next-page`
  continuation -- a single GET missed jobs on any real workspace), finds
  the existing job by name and `PUT`s it under `--overwrite`, or reports it
  as skipped without.
- **A dry run reported its notebooks as "Skipped"**, the opposite of what
  it meant; they are counted as "would deploy" now. The deploy report lands
  under `<input>/reports/` beside migrate's own reports instead of a
  `./deploy_report` folder in the current directory.
- **Joiner master ports spelled into `PORTTYPE`** (`INPUT/OUTPUT/MASTER`,
  the form real PowerCenter exports use) were not recognised; only a
  `MASTER`/`ISMASTER` attribute was. Without a master the join was skipped
  and the aggregate downstream failed on an unresolved column. The corpus
  Joiner fixture now carries the real spelling and its notebook joins.
- Verified and recorded in `references/aidp-runtime-constraints.md`: the
  workspace is mounted on the driver at `/Workspace`, so a wheel uploaded
  there can be put on `sys.path` without a cluster-library install.

*Live corpus run (fifth pass: every corpus notebook, as AIDP jobs)*

On 2026-09-25 all 12 corpus notebooks were run as an AIDP job on Spark
3.5.0 against scratch Delta tables built from each mapping's own source
definitions, the orchestration fixture's workflows were deployed and run,
and a probe notebook established how a job hands parameters to a notebook
task. Six of twelve notebooks failed on the cluster although the offline
suite called them clean; after this pass eleven run end to end. The
twelfth is the Normalizer fixture, whose new REVIEW marker says the unpivot
has to be written by hand. (Two fixtures' Lookups name no table -- their
notebooks carry that TODO -- and the test harness supplied one.)
Everything below is pinned in `tests/test_live_corpus_regressions.py`.

- **Lookup conditions with qualified names.** `LKP_CURRENT.EMP_ID =
  SQ_EMPLOYEES.EMP_ID` came out as `F.col('CURRENT.EMP_ID')` (the LKP_
  prefix rule fired on the qualifier) and a join on the dotted name
  `SQ_EMPLOYEES.EMP_ID`; Spark reads both as struct fields. Qualifiers are
  stripped first, and the one that names the Lookup marks the lookup side.
  An existing test pinned the dotted rename and was corrected.
- **A Lookup returned the whole lookup table.** Only LKP_-prefixed output
  ports were projected, so a Lookup with plainly named ports (`CUSTOMER_SK`,
  `REGION`) joined every column; two such lookups collided on
  `AMBIGUOUS_REFERENCE`. Every output port is projected now, under its port
  name.
- **IDMC Lookups carrying `joinCondition`** had no condition at all and
  compiled to a cross join. The parser reads `joinCondition` for Lookups.
- **`SUBSTR` with a computed start ran only on Spark 4.** pyspark 3.5's
  `F.substring` takes int positions; a Column raised "Column is not
  iterable". Computed positions use `Column.substr` with Column arguments.
- **The Normalizer emitted Python inside SQL** (`stack(2, 'X', F.col('X'))`)
  and treated every input port, keys included, as an occurrence. It stacks
  with backtick references when the ports share a type, and otherwise says
  it cannot identify the repeating group -- which moves the normalizer
  fixture from "zero-touch" (it could not run) to marked. The published
  rate is unchanged at 7/12 because the IDMC star-schema notebook lost its
  marker in the same pass.
- **Unconvertible expressions produced VOID columns.** The `F.lit(None)`
  placeholder is cast to the port's declared type; Delta refuses to create
  or evolve a VOID column, so a notebook with one REVIEW marker could not
  write at all.
- **Task parameters were a shape the jobs API rejects** (a flat dict: 400
  "Unable to process JSON input"). They are a `[{name, value}]` list now.
- **Job parameters never reached the notebook.** AIDP does not copy job or
  task parameters into `spark.conf`, the only place `_param()` looked, so
  every per-run override was ignored in favour of the default. `_param()`
  reads `oidlUtils.parameters.getParameter(name, default)` first (the
  two-argument form -- one argument raises for an unset name), then
  `spark.conf`, then the mapping default.
- **Scheduled workflows could not be deployed.** The schedule keys were
  `cronExpression`/`timezone`; the API requires `quartzCronExpression`/
  `timezoneId`. Schedules are also created `PAUSED`: the timezone is an
  assumption and the review file asks for a check before enabling.
  Definitions generated with the old keys are upgraded on the way through.
- **Deploy robustness.** The client retries 429/5xx with backoff (a 503
  interrupted a live upload), and `mkdir` on an existing folder no longer
  logs an ERROR per folder.
- Verified live and recorded in `references/aidp-runtime-constraints.md`:
  `dependsOn` ordering (the second task started one second after the first
  finished), a manual run of a paused scheduled job, the parameter
  precedence, and the jobs API shapes.
- A self-review of this pass tightened four of its own fixes before merge:
  the Normalizer stacks only a single numbered repeating group
  (`SALES_1..n`) and carries every other port -- stacking all same-typed
  inputs unpivoted the key as a value and wrote corrupt rows; a Lookup
  condition qualified with the lookup *table* name marks the lookup side
  too; POSTs retry only on 429/503 (a replayed `POST /jobs` after a 504
  fails on "already exists"), and an HTTP-date `Retry-After` no longer
  raises; `_param()` falls back on any error from the AIDP parameter call,
  not only a missing `oidlUtils`. A port with a scale but no precision maps
  to `decimal(38, scale)`, and a key-only Lookup is projected to its key.

### Known limitations

These are disclosed rather than worked around; the release notes cover each in
detail.

- **Not yet validated against a real Informatica export.** Every fixture in
  the test suite and the `demo.sh` corpus is hand-authored or synthetic.
  a real IDMC export is still needed to cover what
  needs to contain to close this gap.
- **`AdwWriteStrategy` has never been run against a live ADW.** It is
  derived from the AIDP connector reference documentation, and every code
  path it emits carries an explicit marker saying so.
- **Live-cluster, cell-by-cell verification of generated notebooks is not
  included.** The generators and the deploy path are implemented; an
  executor that runs each cell on a live AIDP cluster and iterates on
  failures is not vendored in this release.
- **Orchestration coverage is uneven.** PowerCenter workflows *are*
  compiled into an AIDP job DAG and deployed: task instances and workflow
  links become tasks with `depends_on`, topologically ordered, and the
  workflow's schedule is converted to a Quartz cron where it can be
  derived exactly, and worklets are expanded. Two gaps sit around that,
  each one reported in a companion `workflows/<name>.review.md` rather
  than left to be inferred:
  - Non-session task types (Command, Email, Decision, Timer, Event-Wait,
    Control, Assignment) have no notebook equivalent and are not
    translated. Each is named in the review report.
  - Workflow link **conditions** other than `$X.Status = SUCCEEDED` are
    not applied; the link becomes an unconditional dependency, and the
    condition is quoted in the review report.
- **IDMC/IICS orchestration is not implemented at all.** Taskflows,
  Mapping Tasks, and parameter sets are not parsed. For an IDMC source,
  mappings convert but no job DAG is produced.
- **Parameter file values reach the job, not the code.** `.par` `$$`
  values become per-task job parameters (narrowest section wins); they
  are not substituted into generated code, and non-`$$` session
  parameters are reported only.

# infa2aidp — Informatica to Oracle AI Data Platform Migrator

A Codex plugin that migrates Informatica ETL metadata into PySpark
notebooks and AIDP job definitions for Oracle AI Data Platform (AIDP).
It writes target table DDL from the export's declared types but never
applies it, and it does not register catalog connections; both are manual
steps (see `references/migration-steps.md`).

Two sources are supported, both current Informatica:

- **IDMC / IICS** — Informatica Intelligent Cloud Services mapping exports (JSON).
- **PowerCenter 10.x** — repository XML exports (`pmrep`/Repository Manager). 8.x and
  9.x are explicitly refused by a version gate, not silently mis-migrated. The scope is
  deliberately wider than the releases Informatica still supports: 10.4 left standard
  support in March 2024 and 10.5.x in March 2026, so every surviving PowerCenter estate
  is on an unsupported release by definition, and 10.1/10.2 is where a large share of
  them sit. See the caveat on repository versions 186/187 under
  [What this does NOT do](#what-this-does-not-do).

This tool has one job: current Informatica in, AIDP out.

## Install

1. Install the plugin the way you install any Codex plugin (place it where Codex looks
   for plugins, or add this directory to your plugin path).
2. On the next session the **SessionStart hook** stages the bundled engine to
   `~/.aidp-infa-migrator/engine`. If it did not run, stage it by hand:

   ```bash
   python hooks/session_start.py          # from the plugin root
   ```

3. Point `INFA_ENGINE` at the staged engine and install its dependencies:

   ```bash
   export INFA_ENGINE=~/.aidp-infa-migrator/engine
   pip install -r $INFA_ENGINE/requirements.txt
   ```

The full engine ships bundled under `engine/` — nothing else to fetch.

**No API key is needed for the migration itself.** The deterministic compiler converts
36 transformation types and 72 expression functions with no model involved, and that is
a supported configuration rather than a degraded one. `--use-llm` additionally needs
`OPENAI_API_KEY` (this is the Codex build; set `LLM_PROVIDER=anthropic` with
`ANTHROPIC_API_KEY` to use Claude instead). Deploying to AIDP needs OCI credentials.
See `env.template`.

> The staging directory is deliberately **not** `~/.aidp-migrator`: that belongs to the
> sibling Databricks migrator, and two plugins sharing one tree would overwrite each
> other's engine.

The repo ships the engine, its CLI, **and** the natural-language layer on top of
it: 11 skills (see [Skills](#skills) below). Drive the engine directly with:

```bash
# from the repo root, with the engine on PYTHONPATH -- there is no engine/scripts/
# directory; the module is invoked with python's -m flag
PYTHONPATH=$INFA_ENGINE python3 -m infa2aidp.cli <command> ...
```

**Do not rely on a bare `infa2aidp` console script without checking which one runs
first.** If any *other* Informatica-to-AIDP tool has ever been `pip install`-ed on this
machine, its console script can shadow this repo's on `PATH` — `which infa2aidp` may
resolve to a completely unrelated project (a different version, different license,
different behavior) rather than this code. `PYTHONPATH=$INFA_ENGINE python3 -m infa2aidp.cli`
has no such ambiguity: it always runs the package under `engine/` in this checkout. If
you do want the console-script form, run `pip install -e .` from this repo root first
and confirm with `pip show infa2aidp` that `Location`/`Editable project location` point
here — not somewhere else — before trusting `infa2aidp <command>`.

Verify the install: `python3 -m pytest tests/ -q` should print `1757 passed`, and
`./demo.sh` — an offline smoke run needing no cloud account, no AIDP cluster, and no
no LLM provider key — should end with `notebooks=11 error=0`.  `./demo_local.sh` is the same
run paced into narrated sections for showing to an audience.

## Skills

Skills give Codex the natural-language routing into the engine. All of them call the
same engine underneath — none of this is a second implementation.

> The Claude Code build of this plugin also ships slash commands and two read-only
> reviewer agents. Codex plugins express everything through skills and hooks, matching
> the sibling Codex plugins in this collection, so those are not part of this build.

**Skills** (`skills/`, 11):

| Skill | Purpose |
|---|---|
| `infa-migrator-overview` | Router — read first; lays out the toolkit and the tool's honest limitations |
| `infa-migrator-bootstrap` | One-shot environment readiness check (deps, which `infa2aidp` resolves, API key) |
| `infa-analyze` | Inventory, complexity, compatibility report — no cluster, no LLM call |
| `infa-discover` | Extract mappings/workflows from a live PowerCenter repository (SOAP/pmrep) |
| `infa-migrate-mapping` | Convert mappings to PySpark notebooks — the main run |
| `infa-review` | Human approval gate over LOW/MEDIUM/MANUAL-confidence conversions |
| `infa-reconcile` | Compare source-DB rows against a migrated AIDP target |
| `infa-optimize` | Spark performance suggestions over already-generated notebooks |
| `infa-deploy` | Upload notebooks and create AIDP jobs (no cluster execution) |
| `infa-lineage` | Field-level data lineage report per mapping |
| `infa-rag` | Manage the learned conversion-pattern store |

## The Pass-0 / 1 / 2 / 3 model

The migration is organized into four passes, summarized below. **Pass-0 and Pass-1
are implemented and tested today; Pass-2's live-cluster
half and the deeper parts of Pass-3 are not.**

| Pass | What it covers | Status |
|------|-----------------|--------|
| **Pass-0** — discover | `discover` (crawl a live PowerCenter repository) → `analyze` (inventory, complexity, compatibility report). Offline, no cluster, free. | Built |
| **Pass-1** — migrate | Parse → internal model → deterministic PySpark compile for 36 transformation types (5 of those preserved verbatim as foreign-language bodies rather than translated) and 14 reported with a named reason and a rebuild path, LLM-assist where the rules do not reach, hallucination check, confidence score, human `review` gate. See [`references/conversion-coverage.md`](references/conversion-coverage.md) -- its counts are asserted against the code by a test. | Built |
| **Pass-2** — deploy & execute | `deploy` uploads notebooks (three-step `uploadFileMeta` PAR upload) and creates AIDP jobs from `workflows/*.json`, signing requests with the OCI SDK. Run against a live AIDP workspace on 2026-09-24/25 (us-ashburn-1, Spark 3.5.0): upload, job create and update, paused schedules, task dependencies and job parameters all worked, and all 12 corpus notebooks ran as an AIDP job -- 11 end to end (see CHANGELOG, "Live run" and "Live corpus run"). An automated execute-and-fix loop does **not** exist yet. | Built, verified on one workspace |
| **Pass-3** — validate | `reconcile` (source-DB vs AIDP-target row/data comparison) and `optimize` (Spark performance suggestions) exist and are tested against fixtures. | Built, unproven on a real migration |

## How much converts

Vendors in this space quote conversion rates without stating a denominator,
which makes the numbers unfalsifiable. Ours is defined and measured.

> **Zero-touch mapping rate** — the percentage of source mappings whose
> generated notebook contains no manual-intervention marker (`REVIEW
> REQUIRED`, `# TODO`, `# MANUAL`). The denominator is every mapping in the
> input export. A mapping either needs a human before it runs, or it does not.

On the bundled corpus of 11 mappings (9 PowerCenter XML, 2 IDMC JSON):

| Metric | Result |
|---|---|
| Zero-touch — no manual-intervention marker | 55% (6/11) |
| Runnable — reads a real source table **and** writes a target | 100% (11/11) |
| **Converted — both** | **55% (6/11)** |
| Markers across the corpus | 7 total; mean 0.6, median 0, max 2 |
| Expressions represented | 100% |
| Executes end to end on AIDP (Spark 3.5.0, synthetic source tables) | measured live 2026-09-25 on the corpus as it then stood |

**Quote the converted rate, not zero-touch alone.** Zero-touch counts a
notebook that reads `SELECT 1 AS placeholder` and writes nothing as a
perfect conversion — it has no markers because there is nothing to put a
marker on. A conversion is only real if it is both marker-free and wired
end to end.

**This figure replaced a published 18%, and the engine did not change.**
Eight corpus fixtures were flattened copies of complete mappings that
already existed elsewhere in the tree — same filenames, no `INSTANCE`
elements — so every one produced a placeholder read and no target write,
and the metric counted that. 18% was not a low score; it was not a
measurement of the read/write path at all. Fixing the corpus fixed the
instrument, not the tool.

Run `pytest tests/test_conversion_metrics.py` to reproduce the table; the
floors under each rate fail on a regression.

**The rate has moved for reasons that were not engine changes**, which is
worth knowing before reading anything into it. It moved when the version gate
widened to 10.x and a previously-refused mapping entered the denominator; it
moved again when a Joiner's master side turned out to be spelled the way real
exports spell it (`PORTTYPE="INPUT/OUTPUT/MASTER"`) and the parser had not read
that spelling — found by running the notebook on an AIDP cluster (see
CHANGELOG, "Live run"); and it moves whenever a fixture enters or leaves the
corpus. A rate over a denominator this small is a regression signal, not a
capability claim.

**The denominator is the weakness.** These are 11 mappings we wrote
ourselves; the tool has never parsed a real Informatica export. This
measures the tool against our own idea of Informatica, and a real export
will move the number in both directions — properly wired exports push it
up, real-world mapping complexity pushes it down. Only a real export fixes the
denominator.

## Transformation coverage

Every transformation type on both platforms has a defined, tested outcome.
Nothing reaches a generic "unsupported" message, and
`tests/test_transformation_coverage.py` asserts that over both platform
lists — a type added to the enum and then forgotten fails the suite.

| | IDMC / IICS (39 types) | PowerCenter (33 types) |
|---|---|---|
| **Emits Spark** | 23 | 22 |
| Body preserved — foreign-language source carried verbatim | 3 | 3 |
| Refused, with a stated reason and alternative | 11 | 6 |
| Handled by the read/write path (Source, Target) | 2 | 2 |
| **Not recognised** | **0** | **0** |

The buckets are counted separately on purpose. A converter existing for a
type does not mean the type converts: Java, Python, Velocity, Custom and
External Procedure embed source in another language, so the body is
preserved verbatim rather than translated — the code *is* the logic, and
paraphrasing it would be a guess nobody can check. B2B, Data Services,
Verifier and eleven others have no Spark equivalent at all; those emit the
reason and what to do instead, because "unsupported" alone leaves a
migrator with no next step.

A tested, specific refusal is coverage. A silent drop is not.

## What this does NOT do

Read this before you plan a migration around it.

- **Orchestration is covered for PowerCenter workflows, and only for those.** A
  PowerCenter workflow does become a deployed AIDP job DAG: `<TASKINSTANCE>` and
  `<WORKFLOWLINK>` are parsed into a dependency graph, topologically ordered, emitted as
  `workflows/<name>.json`, and turned into an AIDP job by `deploy`. The gaps around that:
  - **Schedules are converted where they can be, and omitted where they cannot.**
    `<SCHEDULER>`/`<SCHEDULEINFO>` is parsed; a daily interval, an every-N-hours
    interval that divides 24, and an every-N-minutes interval that divides 60 become a
    real Quartz cron at the source `STARTTIME`. Run-on-demand becomes an unscheduled
    job, which is the faithful translation. Anything else — an interval no cron
    expresses exactly, such as every 7 hours — produces **no schedule at all** plus a
    review item quoting the source values. The tool never emits a schedule it could not
    derive. The **timezone** is the one part no export carries — PowerCenter records
    `STARTTIME` in the Integration Service's local time with no zone — so pass
    `--schedule-timezone America/Chicago` (any IANA zone) to make a converted schedule
    correct rather than plausible. Omit it and the job is created as UTC with the
    assumption reported for review; a zone that is not a real IANA name is refused up
    front, since AIDP rejects an unknown `timezoneId`. Jobs are created **PAUSED**
    either way.
  - **Worklets are expanded**, reusable and nested: their sessions become tasks keyed
    `<worklet>__<instance>`, wired to whatever precedes and follows the worklet. A
    worklet whose definition is not in the export is named in the review report.
    Sessions are keyed by instance, so a reusable session run twice is two tasks on one
    notebook, and every session of a mapping points at that mapping's notebook.
  - **Non-session tasks are not translated** — Command, Email, Decision, Timer,
    Event-Wait, Control, Assignment have no notebook equivalent. Each one is named in
    the review report; dependencies pass through them.
  - **Link conditions:** `$X.Status = SUCCEEDED` on X's own link is exactly what the
    job's `runIf: ALL_SUCCESS` does and is applied. Any other `CONDITION` becomes an
    unconditional dependency and is quoted in the review report. An unconditional link
    out of a session is stated as an assumption — PowerCenter runs the next task even
    after a failure.
  - Every workflow that does not translate completely gets a companion
    `workflows/<name>.review.md`, and `migrate` prints a one-line summary. The absence
    of that file is the signal that the workflow came across whole.
- **IDMC/IICS orchestration is not implemented at all.** Taskflows, Mapping Tasks, and
  parameter sets are not parsed; only a mapping's own JSON is converted. For an IDMC
  source, mappings migrate but **no job DAG is produced at all**.
- **Parameter file values become job task parameters, not code.** With `--params`, each
  session task carries the `$$` values of every `.par` section that applies to it, the
  narrowest winning as in PowerCenter; the notebook reads them at run time.
  `$DBConnection`/`$InputFile` session parameters are reported, not applied.
- **No live-cluster execution.** `deploy` uploads notebooks and creates AIDP jobs, but
  nothing here runs a generated notebook cell-by-cell against a live AIDP cluster,
  compares actual output, or auto-fixes a failing cell — that executor is not vendored in
  this repo. That is Pass-2's other half, and it is not implemented yet.
- **`deploy` has been verified against one live workspace, not many.** On
  2026-09-24/25 (us-ashburn-1, Spark 3.5.0) uploads, job create and update,
  paused schedules, task dependencies and job parameters were accepted, and all 12
  corpus notebooks were run as an AIDP job: 11 ran end to end, the twelfth stops where
  its REVIEW marker says a human is needed (see CHANGELOG, "Live corpus run"). Run
  `deploy --dry-run` first on a new workspace.
- **Generated notebooks need `infa_compat` on the cluster.** Every notebook imports
  the cluster-side runtime library under `engine/infa_compat/` and asserts its version
  in its second cell. Build it (`pip wheel engine/ -w dist/`) and install the wheel as a
  cluster library on the AIDP cluster the jobs run on before running any notebook —
  see "Installing infa_compat on the cluster" below.
- **`--comparison` and the source-fidelity check are review aids, not proofs.** The
  fidelity check reports the Source Qualifier as "missing" for every mapping because a
  qualifier folds into the `spark.table()` read; read its gaps with that in mind.
- **`AdwWriteStrategy` (the external/ADW write path) has never run against a live ADW.**
  Its overwrite-then-driver-side-`MERGE` upsert algorithm (`engine/infa2aidp/generators/write_strategies.py`)
  was derived from the AIDP connector reference documentation, not from testing against a
  real Autonomous Database. Every code path it emits carries an explicit marker in the
  generated notebook so this can't be mistaken for verified behavior.
- **Never validated against a real Informatica export.** Every fixture in the test suite
  and the bundled `demo.sh` corpus is hand-authored or synthetic — no PowerCenter 10.5 or
  IDMC/IICS export from an actual customer repository has been run through this tool.
  Every fixture is either authored for this project or ported from an earlier
  Informatica parser's test set (see `tests/fixtures/powercenter/README.md`) -- none is
  a real export. Treat generated notebooks as a strong first draft, not a certified
  conversion: a real validation pass still has to reconcile output against a live
  Informatica run, and nothing in this repo substitutes for that.
- **Informatica is a source only.** There is no path that migrates *into* Informatica,
  and no MDM/data-quality product support.
- **Repository versions 186 and 187 are inferred, not verified.** They are mapped to
  10.1 and 10.2 on the recollection of two reviewers with Informatica experience, who
  also place 9.6.1 at 184. This table previously read them as 9.x, supported only by a
  fixture we wrote ourselves. If the reviewers are wrong, a genuine 9.x export is
  converted rather than refused — the generated report names the dispute so it is not
  inherited silently. One real export from a known release settles it.
- **Coverage is not fidelity.** Every transformation type is recognised and has a
  defined outcome, but "emits Spark" is not "emits Spark that matches Informatica".
  Nothing here has been verified against real Informatica output. See
  [`conversion-coverage.md`](references/conversion-coverage.md) for exactly what
  "covered" means per transformation type.
- **Not every transformation converts cleanly.** `demo.sh`'s own corpus run surfaces
  known gaps (missing target-write metadata in some fixtures, partial fidelity on a
  handful of transformation types) — see the release notes for the current, disclosed list.

## Commands

| Command | Purpose |
|---------|---------|
| `discover` | Extract assets from a live PowerCenter repository |
| `analyze` | Inventory, complexity, and compatibility report (no cluster needed) |
| `migrate` | Convert mappings to PySpark notebooks (rule-based by default, `--use-llm` for LLM-assisted) |
| `deploy` | Upload notebooks and create AIDP jobs / catalog connections |
| `reconcile` | Compare source-DB rows against AIDP targets |
| `optimize` | Spark performance suggestions for generated notebooks |
| `review` | Human approval gate for low-confidence conversions |
| `rag` | Manage the learned-pattern store |
| `lineage` | Field-level data lineage report |
| `version` | Print tool and Python version |

Run `PYTHONPATH=$INFA_ENGINE python3 -m infa2aidp.cli <command> --help` for flags, or see
`demo.sh`, which exercises the full discover → migrate → reconcile sequence.

### Deploying to AIDP

`deploy` authenticates with the OCI SDK (Resource Principal, Instance Principal,
Session Token, or API Key from `~/.oci/config`) and needs the DataLake and workspace it
is deploying into:

```bash
export AIDP_REGION=us-ashburn-1
export AIDP_INSTANCE_ID=ocid1.aidataplatform.oc1.iad....   # the DataLake OCID
export AIDP_WORKSPACE_KEY=<workspace key>
export AIDP_CLUSTER_KEY=<cluster key the jobs run on>
export OCI_PROFILE=DEFAULT                                  # ~/.oci/config profile
PYTHONPATH=$INFA_ENGINE python3 -m infa2aidp.cli deploy -i <migrate-output> -o ./deploy_report --dry-run
PYTHONPATH=$INFA_ENGINE python3 -m infa2aidp.cli deploy -i <migrate-output> -o ./deploy_report
```

Notebooks land under `/Workspace/Migrated/<folder>/` (override with `--workspace-path`);
one AIDP job is created per `workflows/<name>.json`, every task on `AIDP_CLUSTER_KEY`.

### Installing infa_compat on the cluster

Generated notebooks call `infa_compat` (sequences, lookups, SCD2, update strategy,
parameters). It is a separate, stdlib-only package under `engine/`:

```bash
pip wheel engine/ -w dist/            # builds dist/infa_compat-0.1.0-py3-none-any.whl
```

Install that wheel as a cluster library on the AIDP cluster the jobs run on (AIDP
console → cluster → Libraries, or the `aidp-cluster-ops` skill). The notebook's second
cell asserts the installed version matches the one it was generated against, so a
missing or stale install fails loudly before any read or write.

On a shared cluster whose library list you would rather not touch, there is a second
route (verified 2026-09-24): the workspace is mounted on the driver at `/Workspace`, so
upload the wheel with the rest of the deployment and prepend it to `sys.path` in a
first cell -- `sys.path.insert(0, "/Workspace/Migrated/lib/infa_compat-0.1.0-py3-none-any.whl")`.
Python imports straight from the wheel.

### First run against a fresh workspace

The migrator **writes** `ddl/<table>.sql` for every target, from the export's declared
`DATATYPE`/`PRECISION`/`SCALE`, but does not apply it -- running it is your call, and
the file carries REVIEW comments where a generated column will not match its declared
type (an aggregate widens; a Sequence Generator is always `BIGINT`).

If you have not applied that DDL, the first run of a notebook whose target does not
exist yet still completes: the catalog-type check skips the missing target with a
warning, and the write cell creates the table empty from the batch's own schema before
merging. The column types then come from the DataFrame rather than from the Informatica
target definition, which is why **applying `ddl/` first is the better path wherever
precision and scale matter**. The generated cell is flagged REVIEW for exactly this.

Re-running `deploy` is safe: notebooks are re-uploaded and the job is updated in place
under `--overwrite`, or reported as skipped without it. From Git Bash on Windows, pass
`MSYS_NO_PATHCONV=1` (or `//Workspace/...`) so the shell does not rewrite the workspace
path into a Windows path; the tool refuses any path outside `/Workspace` rather than
creating a stray folder.

## Project layout

```text
engine/
├── requirements.txt
├── setup.py                 # packages infa_compat, the cluster-side runtime (pip wheel engine/)
└── infa2aidp/
    ├── cli.py                # thin argparse dispatcher — 10 commands
    ├── migrator.py           # run_migration(): path selection, orchestration
    ├── batch.py              # parallel batch migration
    ├── models.py             # internal representation (53 transformation types)
    ├── config.py             # .env loader
    ├── properties.py         # spelling-tolerant TABLEATTRIBUTE lookup helpers
    ├── parsers/               # PowerCenter XML + IICS/IDMC JSON + .par parsing
    ├── converters/            # rule-based transformation + expression converters
    ├── generators/            # notebook, workflow, lineage, comparison, fidelity generators
    ├── agents/                # LLM pipeline: spec, codegen, validate, fix, RAG store
    ├── handlers/              # OpenAI + Anthropic clients, provider factory, confidence scoring
    ├── crawlers/              # live PowerCenter repository crawler
    ├── analyzer/               # inventory / complexity / compatibility scanning
    ├── optimizer/             # Spark performance rules
    ├── reconciler/            # post-migration data validation
    └── deployer/              # AIDP REST API client

config/            custom transformation rules, reconcile config example
references/        conversion coverage, conversion hazards, AIDP runtime constraints,
                    migration inventory and steps -- see Further reading below
tests/             pytest suite
```

## Further reading

`references/` holds the detail this README intentionally doesn't repeat:

- `references/known-limitations.md` — **what this tool does not do, and why.**
  Every entry has a reason that is not "we ran out of time": the information is
  not in an export, there is no AIDP construct that means the same thing, or
  verifying it needs access nobody on this project has. Read this before
  scoping a migration.
- `references/conversion-coverage.md` — **what this tool converts**: every Informatica
  transformation type and whether it converts, is reported with a reason, or is refused;
  the expression functions covered; and how a workflow becomes an AIDP job. Read off the
  code, so it stays true as the converter changes.
- `references/conversion-hazards.md` — the semantic differences between Informatica and
  Spark that change results, and what the converter does about each one.
- `references/aidp-runtime-constraints.md` — what a generated notebook can and cannot do
  on an AIDP cluster, and the platform behaviour behind each constraint.
- `references/migration-inventory.md` — everything that has to move from a PowerCenter or
  IDMC estate for a pipeline to actually run on AIDP, including the parts no export contains.
- `references/migration-steps.md` — the end-to-end migration, step by step, with an
  explicit owner for each step: tool, tool-plus-human, or manual. **Read the schedule
  callout at the top before enabling any migrated job.**
- `references/spark-4-upgrade.md` / `references/spark-4-behaviour-differences.md` — why
  generated notebooks pin the ANSI flags, and the measured 3.5-vs-4 differences.

`CHANGELOG.md` lists what this release includes and what it does not.

## License

MIT. See `LICENSE`, `NOTICE`, and `PRIVACY.md`.

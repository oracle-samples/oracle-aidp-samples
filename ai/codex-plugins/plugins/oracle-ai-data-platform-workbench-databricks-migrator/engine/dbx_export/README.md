# Databricks → AIDP Exporter (`dbx_export`)

Exports a Databricks Job — notebooks, JARs, init scripts, workflow + cluster
specs — into an AIDP workspace for later migration. Does **not** rewrite code
(`dbutils.*`, S3 paths, DBFS calls, etc. stay exactly as they were on DBX);
code-level migration is a separate next-stage tool. Think of this as
`dbx → aidp` *replication*.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Quick Start — Step by Step](#quick-start--step-by-step)
- [CLI Reference](#cli-reference)
- [Output Structure](#output-structure)
- [Notebook Detection &amp; Extension Preservation](#notebook-detection--extension-preservation)
- [Cross-Validation](#cross-validation)
- [Workspace Destination Planning](#workspace-destination-planning)
- [Placeholders for Failed Exports](#placeholders-for-failed-exports)
- [Soft-Fail Behavior](#soft-fail-behavior)
- [Isolation Guarantees](#isolation-guarantees)
- [Module Map](#module-map)
- [Troubleshooting](#troubleshooting)

---

## How It Works

Three-phase commit:

| Phase | What happens | AIDP writes? |
|---|---|---|
| **1. Discover + download** | Fetch DBX job, recurse `%run` / `dbutils.notebook.run` / `dbutils.jobs.runNow(...)`, download every notebook + library + init script. Cross-validate SOURCE vs JUPYTER for every notebook. | no |
| **PRE-FLIGHT gate** | Print a summary: notebook counts (real vs placeholder), files, child jobs, cluster/workflow names. Failed support-files become warnings; failed task notebooks become placeholders so the workflow stays runnable. | no |
| **2. Upload** | Upload notebooks (`PUT /notebook/api/contents`) + jars/init scripts/wheels. Each one is read back and SHA / fingerprint-validated. | yes (only under `<target-base>`) |
| **3. Cluster + workflow** | Create/reuse `<prefix>_<name>` cluster, install file-backed libs via cluster libraries API, create `<prefix>_<name>` workflow pointing at the uploaded notebooks. | yes (only objects with the prefix) |

If Phase 2 fails partway, partial uploads stay under `<target-base>` (script
never deletes anything). Re-runs are idempotent — already-uploaded notebooks
are skipped, placeholders are auto-replaced when their DBX export succeeds.

---

## Prerequisites

### 1. Network access to DBX

- **From inside AIDP** (recommended): the Databricks proxy VNIC is the
  only DBX endpoint reachable from AIDP clusters. Pass via
  `--dbx-proxy host:port`. If the proxy front enforces HTTP Basic Auth,
  declare `secrets.dbx_proxy_basic_user/_pass` in the customer YAML as
  credential-store refs (resolved cluster-side via aidputils) — or pass
  `--proxy-basic-user/--proxy-basic-pass` / env
  `DBX_PROXY_BASIC_USER/DBX_PROXY_BASIC_PASS`, which take precedence.
  Supply both or neither; an unauthenticated request to an auth-requiring
  proxy fails the export with a 401 at root-job fetch (exit 3).
- **From laptop**: direct Databricks workspace URL + a PAT. Pass
  `--dbx-host https://...` + `--dbx-token <pat>` (or set
  `DATABRICKS_HOST` / `DATABRICKS_TOKEN`).

### 2. OCI access to AIDP

Standard OCI API-key config. Set `OCI_CONFIG_FILE` env var or pass
`--oci-config-path` to point at a config file with the matching profile.

### 3. Deployment location

Upload this package to a directory on AIDP that you can `%run` from. The
demo uses `/Workspace/scratch/<you>/ai_notebook_migration_code/dbx_export/`.

### 4. Target paths

| Setting | Default | Notes |
|---|---|---|
| `--export-output` | `/Workspace/ai_dbx_exported` | All exported artifacts land under this directory. Must be a `/Workspace/` subdirectory, not under reserved roots (`Users`, `Shared`, `Repos`). |
| `--prefix` | `ai_tool` | Applied to workflow + cluster `displayName`. Both isolation guarantees use this. |

---

## Quick Start — Step by Step

### Step 1: Connectivity probe

Read-only check that the DBX proxy and AIDP REST are reachable:

```python
%run $CODE_BASE/dbx_export/run.py --probe \
  --dbx-proxy $DBX_PROXY \
  --export-output $EXPORT_OUTPUT \
  --oci-config-path $OCI_CONFIG --oci-profile $OCI_PROFILE
```

Expected: `[OK] DBX reachable (HTTP 200)`.

### Step 2: Dry run

Phase 1 + pre-flight summary only. No AIDP writes. Use this to inspect
what *would* happen — notebook counts, child jobs discovered, cluster
names, any pre-existing failures:

```python
%run $CODE_BASE/dbx_export/run.py \
  --job-id $JOB_ID \
  --dbx-proxy $DBX_PROXY \
  --export-output $EXPORT_OUTPUT --prefix $PREFIX \
  --cluster-key $CLUSTER_KEY \
  --oci-config-path $OCI_CONFIG --oci-profile $OCI_PROFILE \
  --dry-run
```

### Step 3: Full export

Same call without `--dry-run`. Uploads to `<target-base>`, creates
cluster + workflow.

```python
%run $CODE_BASE/dbx_export/run.py \
  --job-id $JOB_ID \
  --dbx-proxy $DBX_PROXY \
  --export-output $EXPORT_OUTPUT --prefix $PREFIX \
  --cluster-key $CLUSTER_KEY \
  --oci-config-path $OCI_CONFIG --oci-profile $OCI_PROFILE
```

### Step 4: Review the report

Each run writes `reports/dbx_export/export_dbx_job_<id>_<run-id>.json`
with per-notebook status (`VALIDATED` / `PLACEHOLDER` / `SKIPPED_EXISTS`),
fingerprints, file SHAs, cluster/workflow IDs created, and any
warnings.

Notebook pip discovery also records `pip_findings`: each entry contains
`notebook_path`, a **zero-based** `cell_index`, `status` (`UNRESOLVED` or
`NOTE`), and `reason`. The report and terminal summary include `pip_unresolved`
and `pip_notes` counts. Repeated scans of the same finding in the same cell are
deduplicated; identical findings in different notebooks or cells stay separate.

`UNRESOLVED` means dependency discovery was incomplete, not that an installation
failed or that the package is definitely absent. Review the indicated source
and confirm how its dependencies will be supplied. These findings are warnings
(`NOTE` entries are informational); they do not change export exit codes,
dependency extraction, or installation behavior.

Subprocess pip discovery preserves argument positions when expanding supported
literal-list loops. For example, `['pip', 'install', '-r', filename]` reports an
unresolved requirements-file reference; the filename is not exported as a
package. Iterations are never joined into one command. Dynamic expressions,
ambiguous/rebound loop values, and unsupported nested-loop substitutions are
reported as `UNRESOLVED`, without executing notebook code. If an expanded call
has unresolved arguments in any iteration, its dependencies are not harvested;
independent valid calls are still collected. This analysis never rewrites the
notebook or resolves the contents of referenced requirements files.

Install commands containing `--hash` or `--require-hashes` are reported as
`UNRESOLVED` and contribute no installable requirements from that command:
the command conversion cannot preserve hash verification. Independent valid
commands are still collected. This is a fail-closed limitation, not support
for installing hash-protected requirements or silently dropping their hashes.

### Step 5: Re-run idempotently

If anything failed (e.g. transient 429 on a JAR), just re-run. Already
uploaded notebooks are skipped, placeholders are replaced when the
underlying DBX export succeeds, libraries / init scripts re-attempt.

Pass `--overwrite` if you want to refresh already-exported notebook bytes
or update the existing AIDP workflow definition in place.

---

## CLI Reference

```
--job-id ID              Databricks job_id to export (required unless --probe)
--probe                  Connectivity check only — hits jobs/list?limit=1 and exits
--export-output PATH       AIDP path under which everything lands.
                         Default: /Workspace/ai_dbx_exported
--prefix PREFIX          Prefix for workflow + cluster names. Default: ai_tool

--dbx-host URL           DBX host (env: DATABRICKS_HOST). Laptop / direct use.
--dbx-token PAT          DBX PAT (env: DATABRICKS_TOKEN). Pair with --dbx-host.
--dbx-proxy HOST:PORT    DBX proxy (env: DBX_PROXY). From inside AIDP.

--lake-ocid OCID         AIDP data lake OCID (default: CUSTOMER tenant)
--workspace-id UUID      AIDP workspace ID (default: CUSTOMER workspace)
--oci-profile NAME       Profile inside OCI config (default: CUSTOMER)
--oci-config-path PATH   Path to OCI config file (default: ~/.oci/config
                         or $OCI_CONFIG_FILE)

--cluster-key UUID       Reuse an existing AIDP cluster for ALL tasks
                         instead of creating new ones.
--overwrite              Re-upload existing artifacts under <target-base>
                         and update the existing workflow in place.
--no-placeholder         Don't upload placeholders for failed notebook exports.
--strict                 Fail the export if any task notebook fails.
--strict-deps            Fail the export if a confirmed notebook/resource dep
                         or child job fails, or resource directory discovery is
                         incomplete. Missing loose-only candidates warn.
--strict-jars            Fail when a JAR cannot be fetched, uploaded, or
                         checksum-verified against a shared fallback.
--strict-unresolved-jars Fail Phase 1 when notebook JVM dependencies cannot
                         be matched uniquely. Default behavior is warning-only.
--allow-unresolved-jars  Explicitly select the default warning-only behavior;
                         retained for existing runbooks.
--max-depth N            BFS depth cap for child-job recursion (default 10).
--param KEY=VALUE        Bind a widget/job parameter during dependency discovery;
                         repeat for multiple values. Resolves bounded dynamic
                         notebook.run paths before download.
--workflow-param KEY=VALUE
                         Declare or override an AIDP workflow-level parameter;
                         repeat for multiple values. Only an explicitly
                         overridden task-value operand is redirected to that
                         job parameter. With no option, existing conditions and
                         task-level parameter placeholders remain unchanged.
                         Overrides apply to every workflow in the discovered
                         root/child graph. Repeating the same KEY emits a WARN
                         and uses the last CLI value. A conflicting task-value
                         binding, a task base_parameter whose value differs,
                         multiple condition producers for the same overridden
                         key, or duplicate same-name DBX job parameters is
                         refused per workflow and recorded in the JSON report;
                         identical values are kept, while a same-name
                         {{job.parameters.KEY}} forwarder is redundant and
                         removed.
                         Refusals are evaluated during preflight and again
                         before cluster creation. --dry-run reports
                         [WILL REFUSE] and exits non-zero without AIDP writes.
                         Embedded task-value references that cannot be safely
                         redirected are surfaced as WARN decisions.
                         Log/report redaction is deliberately over-inclusive
                         for credential-like parameter names and affects only
                         displayed audit values, not the workflow payload.
                         Location suffixes path/dir/directory/name/file/
                         filename/filepath/location remain auditable unless a
                         strong secret term is present. uri/url are deliberately
                         not location exemptions because they can carry
                         userinfo, signatures, tokens, or secret references.
                         Redaction is defense-in-depth: arbitrary plaintext
                         secrets with generic names may not match these
                         heuristics. Pass Credential Store references instead
                         of plaintext secret values.
                         This changes the exported workflow definition; Phase 2
                         does not execute IF/ELSE expressions.
--dry-run                Phase 1 + pre-flight only — no AIDP writes.
--report-dir PATH        Where to write the JSON report.
                         Default: reports/dbx_export
```

Dependency discovery treats path-like literals as notebook candidates only
when the notebook contains an unresolved dynamic `dbutils.notebook.run`,
`oidlUtils.notebook.run`, or `aidputils.notebook.run` call. Known data, image,
model, archive, document, and
configuration extensions are excluded. For workspace candidates, Databricks
`object_type` metadata is checked before export; confirmed files, directories,
repos, MLflow experiments, and dashboards are reported as external assets,
not failed notebooks. Empty source files such as `__init__.py` are preserved as
valid zero-byte resources. A candidate found only by the loose literal scan
or an unresolved selector
is reported under `skipped_loose_candidates` when lookup confirms it is absent;
it is not a failed notebook dependency. Explicit `%run` and resolved
`notebook.run` targets still fail under `--strict-deps` if missing. Lookup
errors other than a confirmed 404 remain failures.


Explicit `load_config(directory)` and `load_config(config_dir=directory)` calls
also discover configuration-directory resources. Known task/job/exporter
parameter values, simple string/path construction, and known root-normalization
branches are resolved without executing customer code. Relative directories are
anchored at the calling notebook; absolute workspace directories retain their
exact location. The existing recursive resource exporter downloads supported
files, including `.yaml` and `.yml`, and records them as `role: "resource"` in
the export report. This does not promote notebooks found in a config directory
to execution dependencies or rewrite customer notebook code.

Unknown arguments (including omitted/default-only loader arguments), unsupported
locations, empty directories, and listing failures are reported through
`resource_discovery_failures`; `--strict-deps` rejects incomplete discovery.
Reader calls inside compound statements/functions that the bounded evaluator
cannot interpret are reported as unresolved, rather than certifying complete
discovery. A conditional root with an unknown alternative retains its known
export candidates but reports incomplete discovery, including through aliases,
string/path construction, and widget defaults. This completeness tracking is
opt-in within the shared evaluator; candidate inference is unchanged. It follows
short-circuiting and retains completeness per container field: an unknown
selector with an unknown entry is incomplete, while a known-key lookup does not
consume unrelated unknown metadata. Unsupported subscript stores open
completeness for every possible retained container alias, including nested
aliases; scalar values captured before the store remain known. Unsupported
calls also open completeness for containers reachable through their receiver or
arguments, including nested aliases. In-place assignments invalidate retained
aliases, and unsupported name writes (including assignment expressions) clear
stale bindings. This uses the evaluator's optional effect callback rather than a
list of mutator method names. Call arguments still execute in Python when their
values are unused, so fallback/keyword operands are inspected for effects even
when candidate inference does not consume them. Proven short-circuited operands
remain skipped. Opaque calls can conservatively make discovery unresolved even
when their implementation is read-only. Calls with ignored keywords or unsupported
`**kwargs` are incomplete; explicitly consumed
`.format(...)` keywords retain their existing behavior. Branch predicates consume
truth completeness,
so a nonempty mapping remains truthy despite unrelated unknown values. F-string
`!r`/`!a` conversions and nonempty or unknown format specifications are not modeled
by candidate inference and report incomplete configuration discovery. Plain
interpolation, `!s`, and proven-empty format specifications remain supported.
No agent/model call is made by the exporter.
See proposed ADR 0004 for the redesign and verification contract.
Uncertain control-flow mutations invalidate inferred root/widget
values; supplied parameters retain precedence over widget defaults. Exact
configuration paths and import-directory names are scanned independently, even
when both are named `config`. The supported `load_config` name contract also
means an unrelated same-named reader with a non-path argument is unresolved;
`--strict-deps` rejects it rather than guessing.
An absolute workspace directory in a Git-backed task is reported as unsupported
rather than guessed to be a repository-relative directory. Parameter relocation
and runtime package loading are separate existing mechanisms.

---

## Output Structure

```
<target-base>/                              # default: /Workspace/ai_dbx_exported
├── notebooks/
│   ├── Users/<email>/<rel>/<name>.ipynb    # workspace-source notebooks
│   ├── Users/<email>/<rel>/<name>.py       # plain Python utilities
│   ├── Users/<email>/<rel>/<name>.scala    # plain Scala utilities
│   └── GIT/<repo-rel-path>.ipynb           # git-source notebooks (job.git_source)
├── jars/<file>.jar                         # cluster + task libraries
├── python_dependencies/<file>.whl          # wheels / eggs / requirements.txt
└── init_scripts/<file>.sh                  # cluster init scripts (rewritten if needed)
```

On the AIDP side: a cluster gets created (or reused if `--cluster-key` is
passed) for each `job_clusters[]` entry, plus the workflow itself with
all of its tasks pointing at the uploaded notebook paths.

---

## Notebook Detection & Extension Preservation

For each fetched file the exporter inspects the first non-blank line:

| File | Header `# Databricks notebook source`? | Treatment |
|---|---|---|
| `.ipynb` | n/a | Notebook → uploaded as `.ipynb` |
| `.py` | yes | Notebook stored as SOURCE → convert to `.ipynb` + validate |
| `.py` | no | Utility module → uploaded as `.py` (no conversion) |
| `.scala` | yes / no | Same: notebook if header, otherwise preserve `.scala` |

This matters for git-backed jobs especially — `from utils.helpers import …`
references plain `.py` files that aren't notebooks and shouldn't be wrapped
in `.ipynb` shells.

---

## Cross-Validation

For every notebook (workspace AND git):

1. **Pre-upload**: parse the SOURCE-format cells independently from the
   converter, then compare against the converter's `.ipynb` output. Catches
   cell-drop / type-misclassification / content-truncation bugs.
   Mismatch → `placeholder_reason: conversion_validation_failed` /
   `jupyter_source_mismatch`.
2. **Post-upload**: GET the just-uploaded `.ipynb` from AIDP and compare
   cell fingerprints against the local pre-upload version. Catches upload
   corruption. Mismatch → `placeholder_reason: fingerprint_mismatch`.

**Cosmetic differences are tolerated**: trailing empty cells, leading blank
lines inside a cell, per-line trailing whitespace.

**Semantic differences are NOT tolerated**: cell-type flips (code ↔
markdown), indentation changes, real text changes.

---

## Workspace Destination Planning

[verified: 2026-10] The file/notebook APIs allow ASCII letters, digits, `_`,
`.`, `+`, `-`, `@` and spaces; filenames need not start with a letter. The
exporter retains its established punctuation mappings and replaces other
characters with underscores. Workflow/cluster/task-key contracts are separate.

Dry-run and real export validate the same graph-wide destination plan before
uploads. Empty names, manufactured traversal (`[.]`, `[..]`), and duplicate
destinations are reported per source, persist a failure report, and return
exit **17**, regardless of strict or overwrite flags. Copied files and
generated for-each driver destinations participate in the collision check.
Record-only, failed-fetch and unfetched files have no upload destination;
their existing skip/failure/fallback reporting remains in force. Zero-byte
files are valid upload candidates. Failed notebooks still reserve their
workflow/dependency paths so a different source cannot execute in their place.

Workspace notebook spellings accepted by the existing DBX export-API
normalizer (for example `/Workspace/Shared/Helper.ipynb` and `/Shared/Helper`)
identify one source only when their format and content fingerprints agree.
Git identity includes the owner, repository, branch and exact repository file;
extensions are significant. Matching contents alone never merge distinct
sources. Alias decisions are reported; existing overwrite behavior is preserved.
Git `name`/`name.py` and `name`/`name.ipynb` references share one upload when
successful fetches prove the same repository file, representation, and bytes.
So does a reference whose parent folder the 404-only parent-whitespace
fallback trimmed (task path `Folder /name`, fetched as `Folder/name.py`).
Separate `.py` and `.ipynb` files remain distinct sources. Both discovery rows
remain in the report, with `alias_of` and `referenced_by` provenance. A failed
shared upload fails the export even without `--strict-deps` (exit **9**).
For exit 17, inspect `path_plan_errors` and reconcile the named source or
existing-target ambiguity before rerunning; `--overwrite` does not bypass it.

For changed mappings the exporter checks exact current and historical paths.
It reuses one existing path only after content verification; both paths
existing, mismatched content or failed lookups require manual reconciliation.
Verified reuse does not rewrite the artifact, even with `--overwrite`.
Selected physical paths flow into workflow task references and dependency
lookup. Migration job folders retain their historical mapping for resume.
See the workspace-path identity design note
for the owner decision and remaining cross-run identity limits.

## Placeholders for Failed Exports

When a notebook can't be exported (DBX 404, unsupported format,
SOURCE≠JUPYTER mismatch, etc.) the exporter uploads a stand-in `.ipynb` at
the target path. The placeholder has:

- A markdown cell explaining why with the original DBX path
- A code cell that does `raise RuntimeError(...)` so the workflow fails
  loudly if anyone runs it
- A sentinel metadata key (`metadata.ai_dbx_migration.placeholder = true`)
  so subsequent runs auto-replace it when the export finally succeeds —
  no `--overwrite` needed

Use `--strict` to fail the export instead of uploading task-notebook
placeholders, or `--no-placeholder` to disable placeholders entirely.

---

## Notebook JVM / JAR Discovery

Phase 1 scans every task notebook and its transitive notebook dependencies for
Scala imports, fully qualified JVM classes, `_jvm` access, `Class.forName`, and
Java UDF registration. Non-platform references are matched against the class
entries contained in JARs under `paths.lib_repository_dir` (default:
`/Workspace/migration-dependencies/libs`; the deprecated aliases
`paths.lib_fallback_dir` and `paths.jar_fallback_dir` are still honored when
`paths.lib_repository_dir` is absent).

- A unique match is attached to that notebook's target cluster.
- If multiple versions match, a sole reviewed `*fixed*` artifact is preferred;
  otherwise the dependency is reported as ambiguous.
- Missing or ambiguous references are recorded as warnings by default.
- Use `--strict-unresolved-jars` to make those references stop Phase 1 with
  exit code `16`.
- The machine-readable result is written to
  `reports/dbx_job_<job_id>_jar_discovery.json` and embedded in the normal
  export report.

If a repository JAR download fails while the exporter is running in an AIDP
kernel, discovery tries that same file directly under the configured mounted
`/Workspace/` library repository. This permits indexing assemblies larger than
the 256 MiB HTTP download limit without loading the entire JAR into memory.
The 512-JAR count limit and 2 GiB cumulative index budget still apply. Selected
JARs are checksummed in chunks and attached through the existing library flow.
Each fallback is logged as `INDEX_FALLBACK` and recorded in the report's
`index_fallbacks`; an unavailable or invalid mounted file remains an
`INDEX_ERROR`. Off-cluster exports cannot use a local file as a substitute for
the workspace object.

Matched JARs use the existing cluster-library lifecycle. If AIDP returns
`INSTALL_ON_RESTART`, Phase 1 cluster readiness restarts the target cluster and
verifies the library is loaded before Phase 2 migration begins.

---

## AIDP-First Dependency Resolution

Before fetching a jar/wheel from Databricks, Phase 1 checks the shared AIDP
dependency repository (`paths.lib_repository_dir`, default
`/Workspace/migration-dependencies/libs`). If a same-named artifact is already
staged there, the Databricks/proxy fetch is skipped and the existing object is
attached as-is. Matching is by artifact **name**: an exact version match wins;
otherwise the **highest** available version of that name is used. JAR filenames
have no version standard — when a jar name cannot be confidently parsed into
`(name, version)`, only an **exact-basename** match is honored (never a fuzzy
highest-version guess). This repository holds every migration artifact — JARs and
wheels, including the `aidp_compat` wheel.

## Soft-Fail Behavior

Only JAR fetch/upload failures are non-fatal by default. They're logged as
warnings and recorded in the report. Use `--strict-jars` to make them fatal.
Shared fallbacks are checksum-verified when DBX bytes are available. When DBX
bytes are unavailable, the candidate is rejected and its digest/provenance are
recorded. Wheel, init-script,
requirements-file, and other non-JAR upload failures are fatal. A run that
soft-fails a JAR will continue
without that library attached — a runtime concern for the user to handle.

Notebook-discovered JVM dependencies are warning-only by default because static
discovery is evidence, not proof that a package must be provided by a customer
JAR. `--strict-unresolved-jars` opts into blocking enforcement.

Scoped data validation proves that the resolved target predicate returns at
least one row; without a corresponding Databricks-side comparison it does not
claim partition completeness. Static SQL discovery currently supports legacy
two-part `schema.table` names; three-part Unity Catalog names require manual
review. `/Volumes/...` references are validated as target AIDP paths and are
not automatically translated from a distinct Databricks volume namespace.

Compatibility-policy paths may be set through
`paths.compatibility_policy_file` in YAML or
`AIDP_COMPATIBILITY_POLICY_FILE`. Target-runtime requirement baselines may be
set through `paths.target_runtime_requirements_file` or
`AIDP_TARGET_RUNTIME_REQUIREMENTS_FILE`; YAML values are honored by both fresh
exports and upgrade-path materialization. If the repository-default policy is
missing or invalid, Phase 1 emits a prominent warning and consolidates source
requirements without compatibility rewrites. A missing or invalid explicitly
configured YAML/env policy is fatal so a typo cannot silently disable
protected-package guards. Extras and environment markers are preserved when a
valid policy changes a package version. Source extras are retained and unioned
across matching source requirements; policy-side extras do not replace
workload-requested extras.

### Requirements conflict handling

When the same package resolves to different versions across a cluster's
sources (notebook `%pip`, init-script pip, Databricks PyPI libraries,
requirements files), the per-cluster consolidation resolves it rather than
aborting the export (superseding decision 2026-09-16, PR #109; reverses the
earlier 2026-09-09 fail-loud gate that blocked multi-task `per_task` jobs):

- **Multiple distinct exact pins → highest-wins.** The highest version is kept
  (`shap==0.39.0` + `0.46.0` → `0.46.0`). The winner is **per cluster** — it is
  the highest of the notebooks that actually run on that cluster, so two
  clusters of one workflow can legitimately pin different versions.
- **A single exact pin vs an incompatible range/exclusion → pin-override.** The
  pin is kept and the range/exclusion is dropped (`foo==1.0` + `foo>=2.0` →
  `foo==1.0`), labelled `pin-override` (not "highest").
- **Every collapse is reported**: a `# …` audit line in the generated
  requirements file, a stdout `[requirements][conflict][WARN] <cluster>: …`
  line naming the dropped constraint and the source notebook, and a
  `WARN requirements-conflict: <cluster>: …` entry in the export report.
- **Still fatal (nothing to pick, exit 15):** genuinely unsatisfiable **pure**
  range combinations, conflicting exact pins across **overlapping markers**, and
  conflicting arbitrary-equality (`===`) pins. A marker-less pin overlaps every
  marker group, so a bare pin conflicting with a marked pin is also fatal.
- **Out of scope:** non-PEP-508 lines (`--hash=…`, `-r file`, `-e`, VCS URLs)
  are preserved verbatim and do **not** participate in the collapse — a version
  clash on such a line is left for pip to resolve at install time.

Cluster libraries are submitted one item per PATCH. Phase 1 waits for each
item to leave `PENDING`/`RESOLVING`/`INSTALLING` before submitting the next,
avoiding conflicts in AIDP's per-cluster asynchronous operation queue.

Task-notebook + `%run` dep failures still create placeholders by default.
Use `--strict` / `--strict-deps` to override.

---

## Isolation Guarantees

Every AIDP write goes through one of two assertions in
[`sanitizer.py`](sanitizer.py):

- `assert_under_export_output(path, export_output)` — for notebooks + files
- `assert_has_prefix(name, prefix)` — for workflows + clusters

If either assertion fails the script raises before any HTTP call goes out.
This guarantees the exporter cannot touch:

- Notebooks outside `<target-base>` (default `/Workspace/ai_dbx_exported`)
- Workflows without the `<prefix>_` namespace (default `ai_tool_`)
- Clusters without the `<prefix>_` namespace

The exporter also never deletes anything on AIDP. Use `--overwrite` to
re-upload existing artifacts in-place.

`&` becomes `_` in notebook paths, file basenames, and workflow/cluster names
(e.g. `HELPER_ADF&FRESH_Monitoring.ipynb` → `HELPER_ADF_FRESH_Monitoring.ipynb`).
Source paths and contents stay unchanged. Avoid `A&B`/`A_B` collisions and
ampersand-only segments; neither is rejected by the sanitizer.

---

## Module Map

| File | Role |
|---|---|
| [`__main__.py`](__main__.py) | CLI + 3-phase orchestrator |
| [`auth.py`](auth.py) | OCI signer (API key + token) |
| [`sanitizer.py`](sanitizer.py) | Path sanitization + isolation guardrails |
| [`dbx_client.py`](dbx_client.py) | DBX REST (GET only): workspace/export, dbfs/read, GitHub proxy |
| [`aidp_client.py`](aidp_client.py) | AIDP REST (writes go through guardrails) |
| [`dependency_resolver.py`](dependency_resolver.py) | BFS over notebooks + libs + child jobs |
| [`cluster_converter.py`](cluster_converter.py) | DBX cluster → AIDP cluster; init-script rewriter; spark conf filter |
| [`job_converter.py`](job_converter.py) | DBX job → AIDP workflow payload |
| [`validator.py`](validator.py) | Cell fingerprint + file SHA + read-back diff |
| [`placeholder.py`](placeholder.py) | Failed-export placeholder + sentinel marker |
| [`report.py`](report.py) | Run report + pre-flight summary |
| [`run.py`](run.py) | `%run`-friendly entry point for AIDP notebooks |

---

## Troubleshooting

### `ERROR: --export-output ... starts with reserved root`

`/Workspace/Users/...`, `/Workspace/Shared/...`, `/Workspace/Repos/...`
are user-home/shared-tree roots and would risk colliding with real work.
Pick a dedicated path under `/Workspace/` (default
`/Workspace/ai_dbx_exported`).

### `bad_request` for every task notebook

DBX returned 400 for the export path. Modern DBX returns notebook paths
with `/Workspace/...ipynb`; the `workspace/export` endpoint requires the
canonical REST form. The exporter strips the prefix internally — this
error usually means you're pointing at a path that doesn't exist on DBX.

### `jupyter_source_mismatch`

DBX's SOURCE and JUPYTER exports disagree for a notebook. Often a real
DBX bug (a cell silently became markdown during JUPYTER export). The
report's `placeholder_detail` shows the first cell that differs — open
the notebook in DBX UI and inspect that cell.

### `workspace_export_failed: HTTP 429`

The proxy is rate-limiting. The export soft-fails on JARs by default; use
`--strict-jars` to stop immediately. Init scripts and other non-JAR artifacts
remain mandatory. Re-run later for retry; the cluster will start
without the library and you may need to install it manually.

### `source_platform_size_limit`

The notebook exceeded Databricks' normal export limit and every read-only
fallback was rejected. The exporter automatically tries encoded JUPYTER,
direct-download JUPYTER, direct-download SOURCE (without stored outputs), and
the raw workspace-files route. The raw route tries the canonical workspace path
and then the explicit `.ipynb` representation required by some workspaces.
Fallback bytes are accepted only as a structurally valid IPYNB document or as
Databricks SOURCE after cell-fidelity validation. The exporter never clears
outputs or edits the source notebook. If all routes fail, provide a
cleared-output copy, a Git/source copy, or split the source notebook.

### Re-running after deleting AIDP artifacts

Safe. Delete the workflow / cluster / notebook tree manually on AIDP,
then re-run without `--overwrite`. The exporter creates everything fresh.

### Detecting a notebook vs a utility .py file

Check the first non-blank line of the file. If it starts with
`# Databricks notebook source` it's a notebook (will be converted to
`.ipynb`). Otherwise it's a utility module (preserved as `.py`).

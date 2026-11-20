#!/usr/bin/env python3
"""
Workflow-based Job Migration
==============================
Single-command migration from an AIDP workflow/job definition.
Fetches the job from the AIDP Job API, validates all notebooks exist,
discovers %run dependencies, and runs the full migration pipeline.

Combines build_dag_from_workflow.py (manifest generation) with
job_migrate.py (migration execution) into one step.

Usage:
    # Full migration from workflow
    python3 job_migrate_from_workflow.py \\
        --job-key 00000000-0000-4000-8000-000000000123 \\
        --job-name "example_workflow" \\
        --migrate-output /Workspace/user/output

    # With parameter overrides
    python3 job_migrate_from_workflow.py \\
        --job-key <uuid> \\
        --param "start_date=2026-01-01" \\
        --param "end_date=2026-03-31"

    # Validate only (no migration)
    python3 job_migrate_from_workflow.py \\
        --job-key <uuid> \\
        --validate-only

    # Resume interrupted run
    python3 job_migrate_from_workflow.py \\
        --job-key <uuid> \\
        --skip-migrated \\
        --start-task Feature_CSI

    # From pre-built manifest (skip API fetch + validation)
    python3 job_migrate_from_workflow.py \\
        --manifest reports/example_workflow_manifest_1.json \\
        --migrate-output /Workspace/user/output
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _scripts_dir)
# Repo root on sys.path — needed by the recommended-spark-config pre-flight
# (imports from dbx_export.cluster_converter + aidp_config).
sys.path.insert(0, os.path.dirname(_scripts_dir))

# Only evict build_dag from the kernel cache — it was the one missing 'unwrap'.
# Do NOT evict context_tools / cell_analyzer / cluster_session / agent_migrate
# etc.: the kernel loaded those at start-up with correct content; forcing a
# disk re-read hits the FUSE page-cache which may still show older file versions.
import glob as _glob
sys.modules.pop('build_dag', None)
for _pyc in _glob.glob(os.path.join(_scripts_dir, "__pycache__", "build_dag.cpython-*.pyc")):
    try:
        os.remove(_pyc)
    except OSError:
        pass

from aidp_executor import AIDPSession, get_oci_signer
from notebook_path_parameters import update_parameters
from dbx_export.sanitizer import migration_job_output_path

# build_dag_from_workflow and build_dag are imported lazily inside main()
# (only when --job-key is used, not when --manifest is given).

# ─── Defaults ───────────────────────────────────────────────────────

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Generic — no hardcoded customer/AIDP-instance config. lake/workspace/output
# are required CLI args (no default); AIDP_BASE is derived from the lake-OCID
# region; OCI profile defaults to "DEFAULT".
AIDP_BASE = None
DEFAULT_LAKE_OCID = None
DEFAULT_WORKSPACE_ID = None
DEFAULT_OCI_PROFILE = "DEFAULT"
DEFAULT_OUTPUT_BASE = None


class ArgumentParseError(Exception):
    """Raised when argparse identifies the exact bad or missing argument."""


class ReportingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        raise ArgumentParseError(message)


# Export-base detector. Matches the export convention <base>/notebooks/<subtree>/
# where <subtree> is the first segment of the ORIGINAL customer tree. The subtree
# alternation must list every root the exporter can emit — a root that is missing
# here makes derive_export_base() return "", which leaves EXPORT_BASE unset, so
# normalize_nb_path() never collapses the export prefix and every migrated
# notebook lands at {OUTPUT_BASE}/{job}/notebooks/<the entire export path>, e.g.
#   .../notebooks/testing/user/latest_..._v10/dbx_export/notebooks/GIT/utils/x.ipynb
# instead of the intended
#   .../notebooks/GIT/utils/x.ipynb
# GIT was added 2026-08-11: git-sourced exports live under
# <base>/notebooks/GIT/. Deliberately UPPERCASE-only: the exporter emits GIT,
# and a case-insensitive alternation false-positives on ordinary user folders
# (e.g. /Workspace/Users/bob@co.com/notebooks/git/helper would wrongly derive
# an export base and engage EXPORT_BASE path-collapsing for a non-export job).
_EXPORT_BASE_RE = __import__("re").compile(
    r'^(.*?/notebooks)/(?:Users|Repos|Shared|GIT)/')


def derive_export_base(manifest):
    """Return the workspace-relative export base (e.g. 'ai_dbx_exported/notebooks')
    from the manifest's task notebook paths, or '' if the notebooks aren't under
    a dbx_export base. Recognizes the export convention <base>/notebooks/<user-tree>
    for <user-tree> in Users|Repos|Shared|GIT (see _EXPORT_BASE_RE).
    Empty result => non-export job, job_migrate behaves exactly as before.
    """
    for job in manifest.get("jobs", []):
        for t in job.get("tasks", []):
            # Coalesce on falsiness: an explicit "notebook_path": null in the
            # manifest defeats .get()'s default and would AttributeError on
            # .startswith below — aborting export-base derivation entirely, which
            # silently turns EXPORT_BASE off and de-scopes dep sourcing and the
            # .py utility-module pass. Skip the null task, keep scanning.
            p = t.get("notebook_path") or ""
            if not p:
                continue
            if p.startswith("/Workspace/"):
                p = p[len("/Workspace/"):]
            m = _EXPORT_BASE_RE.match(p)
            if m:
                return m.group(1)
    return ""


def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


def build_dep_resolution(job: dict) -> dict:
    """Build a parent->[resolved_child_paths] map from a manifest job's
    validated run_deps tree. Used by job_migrate._resolve_relative_dep()
    to override naive parent_dir+relative resolution when a source
    notebook has a stale `%run ./X` whose actual file is elsewhere.

    Tasks with a missing or null notebook_path cannot anchor an edge:
    they are skipped with a warning (a raw subscript here used to crash
    the whole --manifest run with a KeyError, and an explicit null
    inserted a None parent key into the map).
    """
    dep_resolution: dict = {}

    def _walk(parent: str, deps: list) -> None:
        dep_resolution.setdefault(parent, [])
        for d in deps or []:
            child = d.get("path")
            if child:
                dep_resolution[parent].append(child)
                _walk(child, d.get("nested_deps", []))

    for _t in job.get("tasks", []):
        _parent = _t.get("notebook_path") or ""
        if not _parent:
            tprint(f"WARNING: manifest task {_t.get('task_key', '<unknown>')!r} "
                   f"has no notebook_path — skipping its dep-resolution edges")
            continue
        _walk(_parent, _t.get("run_deps", []))
    return dep_resolution


async def prepare_migration_cluster_once(
    lifecycle,
    cluster_id: str,
    prepared_clusters: set[str],
    reusable_reports: dict[str, dict] | None = None,
) -> bool:
    """Run the complete serialized readiness gate once per target cluster.

    ``ensure_cluster_ready_for_execution`` only covers baseline JAR lifecycle.
    Calling it directly here left runtime requirements and unrelated in-flight
    library operations outside the same ordering contract. This is readiness
    hardening: downstream execution failures retain their own root cause and
    are not attributed to a library HTTP 409 without direct evidence.

    Reuse Phase 1's complete gate so standalone workflow migration and the
    generated Phase 1 runner have identical ordering. Cache only the library
    preparation result; every later connection still rechecks cluster liveness.
    """
    if cluster_id in prepared_clusters:
        tprint(
            f"Cluster {cluster_id[:12]}... already passed the complete "
            "library-readiness gate; rechecking that it is running."
        )
        await lifecycle.ensure_cluster_running(cluster_id)
        return False

    from phase1_cluster_readiness import (
        prepare_cluster,
        reusable_cluster_report_is_live,
    )

    reusable = (reusable_reports or {}).get(cluster_id)
    if reusable is not None:
        reusable_ok, reason = reusable_cluster_report_is_live(
            lifecycle, cluster_id, reusable
        )
        if reusable_ok:
            prepared_clusters.add(cluster_id)
            tprint(
                f"Cluster {cluster_id[:12]}... reuses fresh Phase-1 readiness; "
                f"{reason}."
            )
            return False
        tprint(
            f"Cluster {cluster_id[:12]}... cannot reuse Phase-1 readiness "
            f"({reason}); running the complete live gate."
        )

    await prepare_cluster(lifecycle, cluster_id)
    prepared_clusters.add(cluster_id)
    return True


def load_phase1_readiness_report(
    report_path: str,
    expected_key: str,
    max_age_seconds: int,
) -> dict[str, dict]:
    """Load an explicitly configured report without masking I/O/JSON errors.

    A well-formed report that is stale, not READY, or for a different contract
    safely falls back to the live gate. A missing or malformed configured file
    is an operator/configuration error and propagates fail-loud.
    """
    from phase1_cluster_readiness import load_reusable_readiness_report

    try:
        return load_reusable_readiness_report(
            report_path,
            expected_key,
            max_age_seconds=max_age_seconds,
        )
    except RuntimeError as readiness_error:
        tprint(
            f"[warn] Phase-1 readiness report not reusable: "
            f"{readiness_error}. The complete live gate will run."
        )
        return {}


def resolve_readiness_settings(cli_report, cli_max_age, runtime_config):
    """Resolve per-run CLI overrides over customer YAML runtime settings."""
    if cli_report is not None and not str(cli_report).strip():
        raise ValueError("--readiness-report must name a file; an empty override cannot disable the configured report")
    report = cli_report
    if report is None:
        report = getattr(runtime_config, "readiness_report_path", "")
    max_age = cli_max_age
    if max_age is None:
        max_age = getattr(runtime_config, "readiness_report_max_age_s", 1800)
    max_age = int(max_age)
    if not 1 <= max_age <= 86400:
        raise ValueError(
            "readiness_report_max_age_s must be between 1 and 86400"
        )
    return str(report or "").strip(), max_age


async def _run_workflow_job(job, session, migrator, legacy_task_dependency_scan):
    """Bind the workflow CLI mode to the shared migration engine."""
    if job.get("parameters", {}).get("jobRunId") == "{{job.run_id}}":
        import copy
        import uuid

        # Interactive cell execution has no workflow context. Use ONE explicit
        # validation namespace across all tasks, never mutate the source manifest
        # or the workflow template that clone_workflow will copy for real runs.
        job = copy.deepcopy(job)
        job["parameters"]["jobRunId"] = "migration_validation_" + uuid.uuid4().hex
        tprint(f"[run-context] shared validation jobRunId: {job['parameters']['jobRunId']}; "
               "cloned workflow retains {{job.run_id}}")
    return await migrator.process_job(
        job, session,
        skip_task_dependency_scan=not legacy_task_dependency_scan,
    )


# Task terminal statuses that let this launcher exit zero. The cell counters
# miss failure terminals that carry no failed-cell count (a second kernel loss,
# PARTIAL_STATE_LOST, ACCEPTANCE_CONTRACT_VIOLATED, ...), and a zero exit marks
# the Phase 2 workflow SUCCESS. Fail-closed: any other status, including one
# added later, fails the run (incident 2026-10-12,
# spend_category_tickets_automation).
SUCCESSFUL_TASK_STATUSES = frozenset({
    "PASS",
    "PASS_CONDITIONAL_EXIT_COVERAGE",
    "PASS_DATA_SUBSTITUTED",
    "ALREADY_MIGRATED",       # --skip-migrated resume over a recorded success
    "SKIPPED",                # outside --start-task / --only-tasks
    "SKIP_MISSING_DEPS",      # graceful skip; a strict halt on it fails via job_halted
    # Not NON_NOTEBOOK_SKIPPED: non-notebook workflow tasks never reach the
    # manifest, so for a task it means its notebook was never migrated.
})
# --code-only converts without executing, so its success is unvalidated code.
CODE_ONLY_SUCCESSFUL_TASK_STATUSES = SUCCESSFUL_TASK_STATUSES | {
    "CONVERTED_NOT_VALIDATED",
}


def unsuccessful_tasks(task_results, code_only):
    """Return {task_key: status} for every task that did not end successfully."""
    allowed = (CODE_ONLY_SUCCESSFUL_TASK_STATUSES if code_only
               else SUCCESSFUL_TASK_STATUSES)
    unsuccessful = {}
    for task_key, task_result in (task_results or {}).items():
        status = task_result.get("status") if isinstance(task_result, dict) else None
        if status not in allowed:
            unsuccessful[task_key] = status or "UNKNOWN"
    return unsuccessful


def require_task_cluster_assignments(job):
    """Reject missing assignments as CLI input errors, never select a fallback."""
    if job.get("default_cluster"):
        return
    missing = [task.get("task_key", "<unknown>") for task in job.get("tasks", [])
               if not task.get("cluster_id")]
    if missing:
        # Both input modes share this CLI boundary; do not catch unrelated
        # ValueErrors from configuration, readiness or migration in __main__.
        print(
            "ERROR: Missing workflow/manifest cluster assignment for task(s): "
            + ", ".join(missing)
            + ". Assign task clusters or a workflow default; deprecated --cluster "
              "cannot supply a fallback. No cluster was selected for these tasks."
        )
        raise SystemExit(2)


async def main():
    parser = ReportingArgumentParser(
        description="Migrate AIDP workflow job — fetch, validate, and run migration in one step")

    # Workflow identification (--job-key OR --manifest required)
    parser.add_argument("--job-key",
                        help="AIDP job key (UUID) from the workflow")
    parser.add_argument("--manifest",
                        help="Pre-built manifest JSON from build_dag_from_workflow.py (skips API fetch + validation)")
    parser.add_argument("--job-name",
                        help="Job name for output (default: from workflow/manifest)")
    parser.add_argument("--cluster", dest="legacy_cluster", default=None,
                        help="DEPRECATED: ignored with a warning; task clusters "
                             "come from the workflow/manifest.")

    # Parameter overrides
    parser.add_argument("--param", action="append", default=[],
                        help="Override parameter key=value (repeatable)")

    # Output — default None so YAML is the primary source; CLI = per-run override.
    parser.add_argument("--migrate-output", "--output-base", dest="migrate_output",
                        default=None,
                        help="Override cfg.paths.migrate_output for this run only. "
                             "(--output-base is the pre-YAML-config flag name, "
                             "kept as a back-compat alias.)")
    parser.add_argument("--save-manifest",
                        help="Save generated manifest to file (optional)")
    parser.add_argument("--catalog-manifest", default="",
                        help="Path to the catalog-migration manifest (from migrate_catalog). "
                             "Used to deterministically remap source-catalog string literals "
                             "(e.g. \"main.schema\" -> \"default.schema\") in the saved notebooks.")

    # Execution control
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate notebooks exist and print task graph")
    parser.add_argument(
        "--legacy-task-dependency-scan", action="store_true",
        help="Restore task/child dependency scans and job-end backfill. "
             "By default, validated manifest run_deps are authoritative.",
    )
    # ── skip-migrated: DEFAULT FLIPPED TO DISABLED (2026-08-05) ───────────────
    # REVIEWER NOTE: default is now --no-skip-migrated (skip OFF). Reusing
    # already-migrated notebooks is unsafe for jobs with nested %run util deps:
    # a child-inlined dep (e.g. SaveTableUtils) can be served from its
    # execute-first UN-converted copy instead of its Opus-dep-migrated version,
    # leaving Databricks-only APIs (boto3/Glue, dbutils) unconverted and failing
    # every downstream task that %runs it. Fresh migration guarantees every dep
    # (incl. nested %run children) is Opus-converted. --skip-migrated stays
    # available for confident resumes; prefer --start-task to scope instead.
    # default=False on BOTH actions so the shared dest is unambiguous.
    _skip_grp = parser.add_mutually_exclusive_group()
    _skip_grp.add_argument("--skip-migrated", action="store_true", dest="skip_migrated", default=False,
                           help="Reuse already-migrated notebooks (default: DISABLED — resume optimization only).")
    _skip_grp.add_argument("--no-skip-migrated", action="store_false", dest="skip_migrated", default=False,
                           help="Force re-migration of all notebooks (DEFAULT).")
    parser.add_argument("--refresh-mirror", action="store_true",
                        help="Explicitly replace existing migrated local-module .py files from the export tree.")
    parser.add_argument("--start-task", default="",
                        help="Resume from this task_key (substring match)")
    _mode_grp = parser.add_mutually_exclusive_group()
    _mode_grp.add_argument("--direct-execute", action="store_true",
                           help="Skip AI migration, execute cells as-is. Still requires "
                                "the `llm` config block — recovery/diagnostic paths can "
                                "invoke the LLM.")
    _mode_grp.add_argument("--code-only", action="store_true",
                           help="Convert code WITHOUT executing cells (migrate-only). Every cell "
                                "is rewritten by Opus and saved, but nothing runs — no verify/fix "
                                "loop, no execution evidence. Default: migrate+execute. "
                                "Mutually exclusive with --direct-execute.")
    parser.add_argument(
        "--strict-cell-gating",
        action="store_true",
        help=("Fail the entire conversion immediately when any executable root or "
              "dependency cell is unexecuted, skipped, unverified, data-unavailable, "
              "or still failing after retries."),
    )
    # ─── BEGIN: skip-cells-on-connection-died (opt-out flag) ───────────────
    # Introduced 2026-07-20; default flipped True 2026-07-20 (see
    # scripts/job_migrate.py "SKIP_ON_CONNECTION_DIED" rationale). When True
    # (default), verify-fail with ConnectionDied ename doesn't burn the
    # 5-attempt retry budget — the cell is marked MIGRATED_NOT_VALIDATED,
    # code is saved, and the task switches to code-only for remaining cells.
    # Pass --no-skip-cells-on-connection-died to opt out.
    # To revert: delete this add_argument + the corresponding jm.SKIP_ON_...
    # assignment further below (search for the matching END marker).
    parser.add_argument("--no-skip-cells-on-connection-died",
                        dest="skip_cells_on_connection_died",
                        action="store_false", default=True,
                        help="Opt out of ConnectionDied auto-skip; retry cell "
                             "up to 5 times instead (default behavior pre-2026-07-20).")
    # Back-compat alias: prior versions used `--skip-cells-on-connection-died`
    # as an opt-in flag. Now that skip is default-on, accept the old flag as
    # a deprecated no-op so existing scripts / CI configs don't break with
    # argparse "unrecognized arguments" (H2 review finding).
    parser.add_argument("--skip-cells-on-connection-died",
                        dest="skip_cells_on_connection_died",
                        action="store_true",
                        help="DEPRECATED: skip-on-ConnectionDied is now the "
                             "default. This flag is a no-op alias kept for "
                             "back-compat; will be removed in a future release.")
    # ─── END: skip-cells-on-connection-died ────────────────────────────────
    parser.add_argument("--only-tasks", default="",
                        help="Comma-separated task_key substrings — run ONLY matching tasks. "
                             "Example: --only-tasks '11_2_hyperopt_GPU,11_3_gs'")

    # AIDP environment
    parser.add_argument("--aidp-base", default=None,
                        help="AIDP REST base URL. Default: derived from --lake-ocid region "
                             "(so end users don't need to pass it).")
    # AIDP infra args: default None so YAML is the primary source. CLI flag =
    # per-run override. If BOTH are absent, we fail loudly after cfg load.
    parser.add_argument("--lake-ocid", default=None,
                        help="Override cfg.aidp.lake_ocid for this run only.")
    parser.add_argument("--workspace-id", default=None,
                        help="Override cfg.aidp.workspace_id for this run only.")
    parser.add_argument("--oci-profile", default=None,
                        help="Override cfg.aidp.oci_profile for this run only.")
    parser.add_argument("--oci-config-path",
                        default=None,
                        help="Absolute /Workspace path to an OCI API-key config file for the "
                             "grandfathered %scala `OciFs.ls` helper (Scala on AIDP has no "
                             "aidputils / credential-store access — see phase-3 TODO). Python "
                             "migrations use aidputils.secrets and do NOT need this flag.")
    parser.add_argument("--bucket-mapping", default=None,
                        help="Path to the S3->OCI bucket mapping CSV. If omitted, "
                             "sourced from cfg.bucket_mapping.path in the customer YAML.")
    parser.add_argument("--config", default=None,
                        help="Path to aidp_migrator.yaml. Falls back to $AIDP_CONFIG_PATH, "
                             "./aidp_migrator.yaml, ~/aidp_migrator.yaml (discovery order).")
    parser.add_argument(
        "--readiness-report",
        default=None,
        help=(
            "Override runtime.readiness_report_path for this run. The file must "
            "be a READY JSON emitted by phase1_cluster_readiness.py."
        ),
    )
    parser.add_argument(
        "--readiness-max-age-seconds",
        type=int,
        default=None,
        help=(
            "Override runtime.readiness_report_max_age_s for this run."
        ),
    )

    try:
        args = parser.parse_args()
    except ArgumentParseError as exc:
        print("\nERROR: Failed to parse command-line arguments for job_migrate_from_workflow.py")
        print(f"       Argument issue: {exc}")
        print("       Please correct the parameter above and retry.")
        raise SystemExit(2) from exc

    if args.legacy_cluster:
        tprint(f"WARNING: DEPRECATED --cluster {args.legacy_cluster!r} is ignored. "
               "Using workflow/manifest cluster assignments only. "
               "Remove --cluster from the calling runbook.")

    # ── Load customer YAML + backfill any arg the user did not pass ──
    # Runs immediately after argparse so downstream uses of args.lake_ocid /
    # workspace_id / etc. see the backfilled values (e.g. AIDP base URL
    # derivation below). CLI flag = per-run override; YAML = primary source.
    from aidp_config import load_config as _load_cfg
    _cfg = _load_cfg(cli_config_path=args.config)
    # Honor an explicit --config for the on-cluster Credential-Store signer.
    # This runner calls build_dag_from_workflow.fetch_job_definition() DIRECTLY
    # (see Step 1 below), so that module's own main()-level export never runs —
    # and get_oci_signer -> _apply_cred_env_from_config re-discovers config
    # seeing only $AIDP_CONFIG_PATH / default locations, NOT --config. Without
    # this export the canonical runner would sign with the default credential
    # names even when --config points elsewhere (review #129: fixed the three
    # standalone entrypoints but missed this call path). Must precede the first
    # get_oci_signer/fetch_job_definition call.
    if args.config:
        os.environ["AIDP_CONFIG_PATH"] = args.config
    (
        args.readiness_report,
        args.readiness_max_age_seconds,
    ) = resolve_readiness_settings(
        args.readiness_report,
        args.readiness_max_age_seconds,
        _cfg.runtime,
    )
    if args.readiness_report:
        from phase1_cluster_readiness import read_readiness_report
        # Includes --validate-only and all CLI/env/YAML configuration routes.
        # A typo or malformed report must fail before any control-plane work.
        read_readiness_report(args.readiness_report)
    # Same bar for the AIDP_EFFORT_* knobs: agent_migrate reads them only when
    # Step 5 imports it, after cluster preparation (never under --validate-only).
    from agent_migrate import validate_effort_env
    validate_effort_env()
    if not args.lake_ocid:
        args.lake_ocid = _cfg.aidp.lake_ocid
    if not args.workspace_id:
        args.workspace_id = _cfg.aidp.workspace_id
    if not args.oci_profile:
        args.oci_profile = _cfg.aidp.oci_profile
    if not args.migrate_output:
        args.migrate_output = _cfg.paths.migrate_output
    if not args.bucket_mapping:
        args.bucket_mapping = _cfg.bucket_mapping.path
    # Fail loudly if any required value is STILL missing (schema requires
    # aidp.* + paths.* + bucket_mapping.*, so this only fires if a caller
    # somehow bypasses schema validation).
    _missing = [name for name, val in [
        ("--lake-ocid / cfg.aidp.lake_ocid",           args.lake_ocid),
        ("--workspace-id / cfg.aidp.workspace_id",     args.workspace_id),
        ("--oci-profile / cfg.aidp.oci_profile",       args.oci_profile),
        ("--migrate-output / cfg.paths.migrate_output", args.migrate_output),
        ("--bucket-mapping / cfg.bucket_mapping.path", args.bucket_mapping),
    ] if not val]
    if _missing:
        parser.error(
            "The following required values are missing (pass on CLI OR set in "
            "customer YAML):\n  - " + "\n  - ".join(_missing)
        )

    # Strip trailing slash(es) from --migrate-output so downstream f-strings like
    # f"{migrate_output}/{job_name}/..." don't produce a doubled slash (//). This
    # cleans both jm.MIGRATE_OUTPUT and the direct args.migrate_output uses below.
    args.migrate_output = args.migrate_output.rstrip("/")

    if args.strict_cell_gating and args.code_only:
        parser.error("--strict-cell-gating cannot be combined with --code-only")
    if args.strict_cell_gating and args.direct_execute:
        parser.error("--strict-cell-gating cannot be combined with --direct-execute")
    if args.strict_cell_gating and args.skip_migrated:
        parser.error("--strict-cell-gating requires --no-skip-migrated so every selected cell is executed")

    # Derive the AIDP base URL from the lake OCID region when not given, so the
    # end-user command doesn't need --aidp-base (the region is encoded in the
    # lake OCID: ocid1.aidataplatform.oc1.<region>.…). Mirrors build_dag_from_workflow.
    if not args.aidp_base:
        from aidp_executor import REGION_MAP
        _parts = args.lake_ocid.split(".")
        _region = REGION_MAP.get(_parts[3], _parts[3]) if len(_parts) > 3 else "ap-mumbai-1"
        args.aidp_base = f"https://aidp.{_region}.oci.oraclecloud.com/20240831"
        tprint(f"Derived AIDP base from lake region: {args.aidp_base}")

    if not args.manifest and not args.job_key:
        parser.error("Either --job-key or --manifest is required")

    # A --job-key run validates a cluster and later reconnects to the same
    # cluster for migration. Cache completed library preparation so the second
    # phase avoids redundant dependency work; connection paths still recheck
    # that a cached cluster is running.
    _migration_ready_clusters: set[str] = set()
    _reusable_readiness_clusters: dict[str, dict] = {}

    # Configure once, before the first validation/readiness gate. Do not delay
    # report handoff until after --job-key has already prepared its cluster.
    import cluster_lifecycle as _cl
    _cl.AIDP_BASE = args.aidp_base
    _cl.DATALAKE_OCID = args.lake_ocid
    _cl.WORKSPACE_ID = args.workspace_id
    _cl.OCI_PROFILE = args.oci_profile
    _cl._SIGNER = None
    _cl.configure(args.config)

    def readiness_for_manifest(readiness_manifest):
        if not args.readiness_report:
            return {}
        from phase1_cluster_readiness import build_readiness_key, collect_manifest_cluster_ids
        cluster_ids = collect_manifest_cluster_ids(readiness_manifest)
        reports = load_phase1_readiness_report(
            args.readiness_report, build_readiness_key(_cl, cluster_ids),
            args.readiness_max_age_seconds,
        )
        if reports:
            tprint(f"Reusable Phase-1 readiness loaded for {len(reports)} cluster(s): {args.readiness_report}")
        return reports

    if args.manifest:
        # ── Load pre-built manifest (skip steps 1-4) ──
        tprint(f"Loading manifest from {args.manifest}")
        with open(args.manifest) as f:
            manifest = json.load(f)
        _reusable_readiness_clusters = readiness_for_manifest(manifest)
        job = manifest["jobs"][0]
        require_task_cluster_assignments(job)
        job_name = args.job_name or job["job_name"]
        if args.job_name:
            job["job_name"] = job_name

        # Apply --param overrides
        param_overrides = {}
        for p in args.param:
            key, _, value = p.partition("=")
            if not key:
                print(f"WARNING: Ignoring malformed --param: {p}")
                continue
            param_overrides[key.strip()] = value.strip()
        if param_overrides:
            job_params = job.setdefault("parameters", {})
            for key, value in param_overrides.items():
                if key == "notebook_path" and value == "":
                    tprint("  job notebook_path parameter: empty CLI override ignored")
                    continue
                update_parameters(job_params, {key: value})
                if "parameters_meta" in job:
                    job["parameters_meta"][key] = {
                        "source": "cli",
                        "widget": job["parameters_meta"].get(key, {}).get("widget", False),
                        "value_present": value != "",
                    }
            # A pre-built manifest already has validated run_deps; use those
            # edges to remap an overridden notebook_path before migration.
            if "notebook_path" in param_overrides:
                from build_dag_from_workflow import _rewrite_manifest_notebook_path_parameters
                _export_base = derive_export_base(manifest)
                _base_prefix = f"/Workspace/{_export_base}" if _export_base else ""
                _rewrite_manifest_notebook_path_parameters(
                    job["tasks"], job["parameters"], _base_prefix, None,
                )
            applied = {
                key: job["parameters"][key] for key in param_overrides
                if key != "notebook_path" or param_overrides[key] != ""
            }
            if applied:
                tprint(f"Parameter overrides applied: {applied}")

        layers = job.get("execution_layers", [])
        tprint(f"Job: {job_name} ({len(job['tasks'])} tasks, {len(layers)} layers)")
        for i, layer in enumerate(layers):
            print(f"  Layer {i}: {layer}")
        tprint(f"Parameters: {json.dumps(job.get('parameters', {}))}")

    else:
        # Lazy imports — only needed when --job-key is used
        from build_dag_from_workflow import (
            fetch_job_definition, extract_tasks_from_job,
            validate_notebooks_exist, discover_run_deps_for_tasks,
            build_execution_layers, build_manifest,
        )
        from build_dag import collect_missing_external

        # ── Step 1: Fetch workflow definition ──
        tprint(f"Fetching workflow: {args.job_key}")
        job_data = fetch_job_definition(
            args.workspace_id, args.job_key,
            lake_ocid=args.lake_ocid, oci_profile=args.oci_profile)

        job_name = args.job_name or job_data.get("name", "").replace(".job", "") or args.job_key
        tprint(f"Workflow: {job_data.get('name', '?')} → job_name: {job_name}")

        # ── Step 2: Extract notebook tasks ──
        tasks = extract_tasks_from_job(job_data)
        if not tasks:
            print("ERROR: No notebook tasks found in workflow")
            sys.exit(1)

        tprint(f"Found {len(tasks)} notebook task(s):")
        for t in tasks:
            deps_str = f" (depends: {', '.join(t['depends_on'])})" if t["depends_on"] else ""
            # A task with no notebook path (null or missing) must not crash the
            # summary print with a TypeError from os.path.basename(None) — name it
            # so the operator sees WHICH task the workflow gave us nothing for.
            _nbp = t.get("notebook_path") or ""
            print(f"  {t['task_key']}: "
                  f"{os.path.basename(_nbp) if _nbp else '<NO NOTEBOOK PATH>'}{deps_str}")

        # ── Step 3: Pre-flight validation ──
        _job_clusters = job_data.get("jobClusters") or []
        require_task_cluster_assignments({
            "tasks": tasks,
            "default_cluster": _job_clusters[0].get("clusterKey") if _job_clusters else None,
        })
        # Validation must use a workflow-owned cluster, not the control cluster.
        _val_cluster = next((t["cluster_id"] for t in tasks if t.get("cluster_id")), None)
        if not _val_cluster:
            _jc = job_data.get("jobClusters", [])
            if _jc:
                _val_cluster = _jc[0].get("clusterKey")
        if not _val_cluster:
            print("ERROR: No cluster_id found in workflow tasks or job defaults.")
            print("       Assign clusters in the workflow; --cluster cannot supply a fallback.")
            sys.exit(1)

        # Task/default cluster identities are available before notebook probes;
        # use the same set that build_manifest records for the report key.
        _reusable_readiness_clusters = readiness_for_manifest({"jobs": [{
            "tasks": tasks,
            "default_cluster": _job_clusters[0].get("clusterKey") if _job_clusters else None,
        }]})

        # Preflight prerequisite: the aidp_compat wheel must be uploaded to the
        # deps folder on this workspace before migrating. Fail fast here (also
        # runs under --validate-only) with a clear, actionable message so the
        # user can upload it BEFORE the long migration starts — rather than
        # aborting mid-run with a LibraryTaskException.
        _whl = _cl.resolve_aidp_compat_whl()
        if not _whl:
            print(f"ERROR: PREREQUISITE NOT MET — expected {_cl.TARGET_WHEEL_PATH} "
                  f"not found in {_cl.AIDP_LIB_DIR}.")
            print(f"       Build the wheel (cd aidp_compat && python setup.py "
                  f"bdist_wheel) and upload aidp_compat-{_cl.AIDP_COMPAT_VERSION}-"
                  f"py3-none-any.whl to {_cl.AIDP_LIB_DIR}, then retry.")
            sys.exit(1)
        tprint(f"Prerequisite OK: aidp_compat wheel present ({_whl})")

        # Serialize all pending library work, install runtime pins/baseline
        # JARs/aidp_compat, restart when required, and verify everything before
        # opening the validation session. This is deliberately the SAME gate
        # used by Phase 1 rather than a partial duplicate in this launcher.
        await prepare_migration_cluster_once(
            _cl, _val_cluster, _migration_ready_clusters,
            reusable_reports=_reusable_readiness_clusters,
        )

        tprint(f"\nConnecting to cluster {_val_cluster[:12]}... for validation...")
        val_session = AIDPSession(
            lake_ocid=args.lake_ocid, workspace_id=args.workspace_id,
            cluster_id=_val_cluster, oci_profile=args.oci_profile,
            session_name=f"val_{job_name}")
        await val_session.connect()

        # Validate task notebooks exist
        tprint("Validating task notebooks...")
        valid, missing = await validate_notebooks_exist(val_session, tasks)
        if missing:
            print(f"\nERROR: {len(missing)} task notebook(s) NOT FOUND:")
            for m in missing:
                print(f"  ! {m['task_key']}: {m['notebook_path']}")
            print("\nAll task notebooks must exist before migration. Upload and retry.")
            await val_session.close()
            sys.exit(1)
        tprint(f"All {len(valid)} task notebook(s) validated")

        # Discover %run dependencies
        tprint("Scanning %run dependencies...")
        discovery_param_overrides = {
            p.get("key", p.get("name", "")): p.get("value", "")
            for p in (job_data.get("parameters") or []) if isinstance(p, dict)
        }
        param_overrides = {}
        for p in args.param:
            key, separator, value = p.partition("=")
            if not separator or not key.strip():
                print(f"WARNING: Ignoring malformed --param: {p}")
                continue
            param_overrides[key.strip()] = value.strip()
            update_parameters(discovery_param_overrides, {key.strip(): value.strip()})
        run_deps = await discover_run_deps_for_tasks(
            val_session, tasks,
            parameter_overrides=discovery_param_overrides,
        )
        await val_session.close()

        # Check for missing external deps
        missing_ext = collect_missing_external(run_deps)
        if missing_ext:
            print(f"\nERROR: {len(missing_ext)} external dependency notebook(s) NOT FOUND:")
            for m in missing_ext:
                print(f"  ! {m['path']}")
                for ref in m["referenced_by"]:
                    print(f"    referenced by: {ref}")
            print("\nAll %run dependencies must exist before migration. Upload and retry.")
            sys.exit(1)

        # ── Step 4: Build manifest ──
        layers = build_execution_layers(tasks)
        manifest = build_manifest(
            job_name, tasks, layers, run_deps, job_data,
            parameter_overrides=param_overrides,
        )

        if param_overrides:
            applied = {
                key: manifest["jobs"][0]["parameters"][key]
                for key in param_overrides
                if key != "notebook_path" or param_overrides[key] != ""
            }
            if applied:
                tprint(f"Parameter overrides applied: {applied}")

        # Save manifest if requested
        if args.save_manifest:
            os.makedirs(os.path.dirname(args.save_manifest) or ".", exist_ok=True)
            with open(args.save_manifest, "w") as f:
                json.dump(manifest, f, indent=2)
            tprint(f"Manifest saved to {args.save_manifest}")

        # Print summary
        job = manifest["jobs"][0]
        tprint(f"\nExecution Layers ({len(layers)} layers):")
        for i, layer in enumerate(layers):
            print(f"  Layer {i}: {layer}")
        tprint(f"Tasks: {len(job['tasks'])}")
        tprint(f"Parameters: {json.dumps(job.get('parameters', {}))}")

    if args.legacy_task_dependency_scan:
        tprint("Dependency preparation: legacy task scans and job-end backfill "
               "(--legacy-task-dependency-scan)")
    else:
        # Validate a pre-built manifest before cluster bootstrap or migration.
        # There is no automatic fallback to source scans for an incomplete DAG.
        from job_migrate import _manifest_dependency_map
        _manifest_dependency_map(job)
        tprint("Dependency preparation: validated manifest run_deps (default)")

    if args.validate_only:
        tprint("\n--validate-only: pre-flight checks passed. Exiting.")
        return

    # ── Step 5: Configure job_migrate globals ──
    import job_migrate as jm
    import agent_migrate as am

    # Both --manifest and --job-key produce the same validated job structure.
    # Install its dependency identity map only after either branch has built
    # ``job`` so child-as-task ordering and --skip-migrated cannot normalize a
    # shared child before its parent is considered.
    _dep_resolution = build_dep_resolution(job)
    jm.set_dep_resolution(_dep_resolution)
    if _dep_resolution:
        _entries = sum(len(v) for v in _dep_resolution.values())
        tprint(
            f"Dep resolution map: {len(_dep_resolution)} parents, "
            f"{_entries} edges"
        )

    # cfg was loaded + args backfilled right after argparse (see top of main()).
    # Wire cfg → module globals here so job_migrate's consumers see them.
    # ALWAYS configure the LLM — even for --direct-execute (PR#146 review #3):
    # the circuit breaker (monitor_analyze_failures) and child/Scala fix paths can
    # invoke the LLM before the direct-mode guard, so skipping configuration left
    # MODEL=None and crashed. A fully AI-free direct mode needs every AI path gated
    # and is a separate change; until then, direct mode requires the `llm` block.
    jm.apply_cfg_to_modules(_cfg, bucket_mapping_path=args.bucket_mapping,
                            configure_llm=True)

    jm.AIDP_BASE = args.aidp_base
    jm.DATALAKE_OCID = args.lake_ocid
    jm.WORKSPACE_ID = args.workspace_id
    jm.OCI_PROFILE = args.oci_profile
    # OCI_CONFIG_PATH is populated by apply_cfg_to_modules (env OCI_CONFIG_FILE
    # → default). Only overwrite when the caller passed --oci-config-path,
    # else the None default would clobber the resolved value.
    if args.oci_config_path is not None:
        jm.OCI_CONFIG_PATH = args.oci_config_path
    jm.MIGRATE_OUTPUT = args.migrate_output

    # Load source-catalog → default name remap from the catalog manifest so
    # string-literal catalog refs (e.g. SCHEMA = "main.example_team") are
    # deterministically rewritten in the saved notebooks (not left to the LLM).
    if args.catalog_manifest:
        jm.register_catalog_remap(args.catalog_manifest)

    # The cell-execution session (cluster_session → AIDPSession) is created with
    # only cluster_id and otherwise falls back to aidp_executor's module defaults.
    # Override those to the run's target so execution hits the right lake/
    # workspace/profile (the session endpoint auto-derives from the lake region).
    import aidp_executor as _ae
    _ae.DEFAULT_LAKE_OCID = args.lake_ocid
    _ae.DEFAULT_WORKSPACE_ID = args.workspace_id
    _ae.DEFAULT_OCI_PROFILE = args.oci_profile
    # Export base: source all notebooks (tasks + transitive %run/notebook.run deps)
    # from under the exported tree and strip it from migrated output paths. Empty
    # for non-export jobs (behaviour unchanged).
    jm.EXPORT_BASE = derive_export_base(manifest)
    if jm.EXPORT_BASE:
        tprint(f"Export base: /Workspace/{jm.EXPORT_BASE} (deps sourced from here; stripped from output paths)")
    jm.DOWNLOAD_META_URL = (
        f"{jm.AIDP_BASE}/dataLakes/{jm.DATALAKE_OCID}"
        f"/workspaces/{jm.WORKSPACE_ID}/actions/downloadFileMeta")
    jm._SIGNER = None
    jm.START_TASK = args.start_task.strip()
    jm.ONLY_TASKS = [t.strip() for t in args.only_tasks.split(",") if t.strip()] if args.only_tasks else []
    jm.SKIP_MIGRATED = args.skip_migrated
    jm.REFRESH_MIRROR = args.refresh_mirror
    jm.DIRECT_EXECUTE = args.direct_execute
    jm.CODE_ONLY = args.code_only
    jm.STRICT_CELL_GATING = args.strict_cell_gating
    tprint(f"MODE: {'code-only (migrate, no execution)' if jm.CODE_ONLY else 'migrate+execute'}")
    # ─── BEGIN: skip-cells-on-connection-died (feature-flag wiring) ────────
    # Introduced 2026-07-20. To revert: delete this single line + the
    # matching --skip-cells-on-connection-died argparse block above.
    jm.SKIP_ON_CONNECTION_DIED = (
        False if args.strict_cell_gating
        else getattr(args, "skip_cells_on_connection_died", True)
    )
    # ─── END: skip-cells-on-connection-died ────────────────────────────────

    am.AIDP_BASE = jm.AIDP_BASE
    am.DATALAKE_OCID = jm.DATALAKE_OCID
    am.WORKSPACE_ID = jm.WORKSPACE_ID
    am.OCI_PROFILE = jm.OCI_PROFILE
    am.DOWNLOAD_META_URL = jm.DOWNLOAD_META_URL
    am.UPLOAD_FOLDER_URL = (
        f"{jm.AIDP_BASE}/dataLakes/{jm.DATALAKE_OCID}"
        f"/workspaces/{jm.WORKSPACE_ID}/objects")
    am.SIGNER = None

    # context_tools (catalog search, Spark-log fetch, bucket mapping) — same target.
    import context_tools as _ct
    _ct.AIDP_BASE = jm.AIDP_BASE
    _ct.DATALAKE_OCID = jm.DATALAKE_OCID
    _ct.WORKSPACE_ID = jm.WORKSPACE_ID
    _ct.OCI_PROFILE = jm.OCI_PROFILE
    _ct._LOG_SIGNER = None

    # ── Step 6: Load bucket mapping ──
    try:
        from context_tools import load_bucket_mapping
    except ImportError:
        import csv as _csv_bm
        def load_bucket_mapping(csv_path=None):
            if csv_path is None or not os.path.exists(csv_path):
                return {}
            mapping = {}
            with open(csv_path) as _f:
                for row in _csv_bm.DictReader(_f):
                    s3 = row.get("s3_bucket", "").strip()
                    oci_bucket = row.get("oci_bucket", "").strip()
                    ns = row.get("oci_namespace", "").strip()
                    if s3 and oci_bucket and ns:
                        mapping.setdefault(s3, []).append(
                            {"oci_bucket": oci_bucket, "oci_namespace": ns})
            return mapping
    mapping = load_bucket_mapping(args.bucket_mapping)
    tprint(f"Bucket mapping: {len(mapping)} entries loaded")

    # ── Step 7: Connect cluster & bootstrap ──
    # Lifecycle config and reusable reports were resolved before preflight.

    # Clear stale .pyc so Python re-reads the .py source from FUSE.
    for _pyc in _glob.glob(os.path.join(_scripts_dir, "__pycache__", "cluster_session*.pyc")):
        try:
            os.remove(_pyc)
        except OSError:
            pass
    sys.modules.pop("cluster_session", None)

    try:
        from cluster_session import cluster
    except ImportError:
        try:
            from cluster_session import ClusterSession
            cluster = ClusterSession()
        except ImportError:
            # FUSE page-cache still serving old cluster_session.py that pre-dates
            # ClusterSession. Fetch the current content from OCI Object Storage
            # via the AIDP downloadFileMeta API (bypasses FUSE entirely) and exec().
            # Note: get_oci_signer is imported at module level (line ~76); the previous
            # inline `from aidp_executor import get_oci_signer` here was redundant and
            # caused a closure-scope NameError in the nested _check_recommended_spark_config
            # (Python bound `get_oci_signer` to main()'s local scope but never assigned it
            # in the non-recovery path).
            import types
            import requests as _req_cs
            _cfg_cs, _sig_cs = get_oci_signer(args.oci_profile)
            _dl_url = (
                f"{args.aidp_base}/dataLakes/{args.lake_ocid}"
                f"/workspaces/{args.workspace_id}/actions/downloadFileMeta"
            )
            _cs_remote = os.path.join(_scripts_dir, "cluster_session.py").replace("\\", "/")
            _r_cs = _req_cs.post(
                _dl_url, auth=_sig_cs,
                headers={"Content-Type": "application/json",
                         "path": _cs_remote, "type": "FILE"},
                data='{"action": "DOWNLOAD"}', timeout=30)
            if _r_cs.status_code not in (200, 201):
                raise RuntimeError(
                    f"FUSE cache stale and download API failed ({_r_cs.status_code}). "
                    f"Restart the notebook kernel and retry."
                )
            _par_cs = _r_cs.json().get("parUrl")
            _src_cs = _req_cs.get(_par_cs, timeout=60).text
            # Compile and exec into a fresh module, bypassing FUSE entirely
            _mod_cs = types.ModuleType("cluster_session")
            _mod_cs.__file__ = _cs_remote
            sys.modules["cluster_session"] = _mod_cs
            exec(compile(_src_cs, _cs_remote, "exec"), _mod_cs.__dict__)
            cluster = getattr(_mod_cs, "cluster", None)
            if cluster is None:
                cluster = _mod_cs.ClusterSession()
            tprint("cluster_session loaded via download API (FUSE cache bypass)")

    # The pop+reimport above re-executed cluster_session from source, which
    # reset RECYCLE_INTERVAL to its hardcoded default — discarding the value
    # apply_cfg_to_modules() set from cfg.runtime.ws_recycle_interval_s.
    # Re-apply it to the FRESH module object (the configured one was dropped).
    sys.modules["cluster_session"].RECYCLE_INTERVAL = _cfg.runtime.ws_recycle_interval_s

    # Workflow migration has no eager orchestration-cluster bootstrap.
    # Validate every reusable report now and seed the session's per-run cache so
    # job_migrate.process_job can skip the duplicate full readiness cycle.
    if _reusable_readiness_clusters and hasattr(cluster, "mark_cluster_ready"):
        from phase1_cluster_readiness import reusable_cluster_report_is_live

        for _ready_cluster_id, _ready_cluster_report in (
            _reusable_readiness_clusters.items()
        ):
            _is_live, _live_reason = reusable_cluster_report_is_live(
                _cl, _ready_cluster_id, _ready_cluster_report
            )
            if _is_live:
                cluster.mark_cluster_ready(_ready_cluster_id)
                _migration_ready_clusters.add(_ready_cluster_id)
                tprint(
                    f"Cluster {_ready_cluster_id[:12]}... pre-validated from "
                    f"Phase-1 report; {_live_reason}."
                )
            else:
                tprint(
                    f"Cluster {_ready_cluster_id[:12]}... Phase-1 report is no "
                    f"longer live ({_live_reason}); full readiness will run on use."
                )

    tv_file = f"{migration_job_output_path(args.migrate_output, job_name)}/task_values.json"

    # The warn-only Spark advisory now runs for task/default clusters even
    # without the legacy flag; previously only eager --cluster setup called it.
    # Readiness and actual session bootstrap remain owned by process_job.
    _preflight_checked: set = set()

    async def _check_recommended_spark_config(cluster_id: str):
        """WARN-only check: is the target cluster missing recommended Spark configs?"""
        if cluster_id in _preflight_checked:
            return
        _preflight_checked.add(cluster_id)
        try:
            from dbx_export.cluster_converter import (
                recommended_spark_config as _recommended,
                _autoscale_keys as _autoscale_keys_of,
            )
            # Reuse the cfg already loaded by main() — single load, single
            # source of truth. Closure captures _cfg from the enclosing scope.
            _REC_CFG = _recommended(_cfg)
            _AUTOSCALE_KEYS = _autoscale_keys_of(_cfg)
            import requests as _req
            _spec_url = (f"{args.aidp_base}/dataLakes/{args.lake_ocid}"
                         f"/workspaces/{args.workspace_id}/clusters/{cluster_id}")
            _, _s = get_oci_signer(args.oci_profile)
            _r = _req.get(_spec_url, auth=_s, timeout=30)
            if _r.status_code == 200:
                _current_conf = ((_r.json().get("clusterRuntimeConfig") or {})
                                 .get("sparkAdvancedConfigurations") or {})
                # spark.serializer is a STATIC conf (read once at SparkContext
                # creation): it can only take effect on a NEWLY created cluster,
                # and flipping it on an existing, working cluster (esp. Hudi jobs)
                # is risky. The tool bakes it into new-cluster creates, but the
                # pre-flight must NOT advise adding it to existing clusters.
                _CREATE_ONLY_KEYS = {"spark.serializer"}
                _check_cfg = {k: v for k, v in _REC_CFG.items()
                              if k not in _CREATE_ONLY_KEYS}
                _missing = [k for k, v in _check_cfg.items()
                            if _current_conf.get(k) != v]
                # Categorize so the perf/IO advisory isn't misattributed to
                # autoscale keys, and vice-versa (raised in PR review).
                _missing_perf = [k for k in _missing if k not in _AUTOSCALE_KEYS]
                _missing_autoscale = [k for k in _missing if k in _AUTOSCALE_KEYS]
                if _missing:
                    tprint("")
                    tprint(f"⚠️  RECOMMENDED SPARK CONFIG PRE-FLIGHT — cluster {cluster_id[:12]}… "
                           f"is missing {len(_missing)} of {len(_check_cfg)} recommended entries:")
                    if _missing_perf:
                        tprint(f"  • {len(_missing_perf)} performance/IO key(s) — "
                               f"performance may suffer for OCI reads or .toPandas():")
                        for _k in _missing_perf:
                            tprint(f"      {_k} = {_REC_CFG[_k]!r}  (currently: {_current_conf.get(_k, '<unset>')!r})")
                    if _missing_autoscale:
                        tprint(f"  • {len(_missing_autoscale)} aggressive-autoscale key(s) — "
                               f"executors will scale on AIDP platform defaults, not the aggressive profile:")
                        for _k in _missing_autoscale:
                            tprint(f"      {_k} = {_REC_CFG[_k]!r}  (currently: {_current_conf.get(_k, '<unset>')!r})")
                    tprint(f"    Migration will proceed. To apply: add these to the cluster's "
                           f"sparkAdvancedConfigurations and restart the cluster.")
                    tprint("")
                else:
                    tprint(f"Recommended Spark config: all {len(_check_cfg)} entries present on cluster {cluster_id[:12]}...")
            else:
                tprint(f"  [warn] recommended-spark-config pre-flight: could not fetch cluster spec "
                       f"for {cluster_id[:12]}... (HTTP {_r.status_code}); skipping check")
        except Exception as _e:
            # Non-fatal — pre-flight check errors shouldn't block migration.
            tprint(f"  [warn] recommended-spark-config pre-flight failed for {cluster_id[:12]}...: {_e!r}; skipping check")

    from phase1_cluster_readiness import collect_manifest_cluster_ids
    for task_cluster in collect_manifest_cluster_ids({"jobs": [job]}):
        await _check_recommended_spark_config(task_cluster)
    tprint("Will connect per-task from workflow/manifest cluster assignments.")

    tprint(f"Task values file: {tv_file}")

    session = cluster

    # ── Step 8: Run migration ──
    tprint(f"\n{'='*60}")
    tprint(f"STARTING MIGRATION: {job_name}")
    tprint(f"{'='*60}")
    tprint("Clusters: per-task from workflow/manifest")
    tprint(f"Output: {args.migrate_output}")
    tprint(f"Started: {datetime.now().isoformat()}")
    jm.ensure_token_usage_run_id()

    try:
        result = await _run_workflow_job(
            job, session, jm, args.legacy_task_dependency_scan,
        )
    except Exception as e:
        print(f"  JOB ERROR: {job_name}: {e}")
        result = {"job_name": job_name, "error": str(e)}

    # ── Step 9: Summary ──
    tprint(f"\n{'='*60}")
    tprint(f"MIGRATION COMPLETE")
    print(f"{'='*60}")

    name = result.get("job_name", job_name)
    migration_failed = "error" in result
    failure_reason = ""
    if "error" in result:
        print(f"  {name}: ERROR - {result['error']}")
        failure_reason = str(result["error"])
    else:
        ok = result.get("total_ok", 0)
        fail = result.get("total_failed", 0)
        fix = result.get("total_fixed", 0)
        errored = result.get("total_errored", 0)
        skipped = result.get("total_skipped", 0)
        data_unavail = result.get("total_data_unavailable", 0)
        line = f"  {name}: Cells OK={ok} Failed={fail} Fixed={fix}"
        if data_unavail:
            line += f" | Data-unavailable={data_unavail}"
        if errored:
            line += f" | Tasks errored={errored}"
        if skipped:
            line += f" | Tasks skipped={skipped}"
        # A cluster left with an unconfirmed idle-timeout restore is a COST
        # residual (never fatal — operator decision), but the summary must not
        # read as an unqualified PASS over it. See process_job / _revert_all.
        idle_unrestored = result.get("idle_timeout_unrestored") or []
        if idle_unrestored:
            line += (f" | idle-timeout-UNRESTORED={len(idle_unrestored)} "
                     f"(cost risk; manual reset may be needed)")
        # A JOB_REPORT.md left from an earlier run must not read as this one's.
        if result.get("job_report_error"):
            line += f" | JOB_REPORT NOT WRITTEN ({result['job_report_error']})"
        print(line)
        # A workflow notebook is marked successful when this launcher exits
        # zero.  Do not let a task-level migration error be hidden by a
        # successfully written report.
        #
        # data_unavail (a genuinely-missing table) exits non-zero exactly like a
        # cell failure: the code migrated, but the DATA isn't there, so the run is
        # NOT a green success — the operator must provision the table and resume.
        # This preserves the pre-reclassification outcome (missing-table used to be
        # counted as a cell failure → non-zero exit); only the label changed. The
        # reason string keeps the two distinct so the operator knows which it is.
        migration_failed = bool(fail or errored or data_unavail)
        # The counters alone are not the verdict: see SUCCESSFUL_TASK_STATUSES.
        not_successful = unsuccessful_tasks(result.get("task_results"), args.code_only)
        if not_successful:
            migration_failed = True
        # A halted run stopped before every task ran; its statuses can all be
        # graceful (a strict halt on SKIP_MISSING_DEPS as the last task).
        halted = result.get("job_halted")
        if halted:
            migration_failed = True
        if migration_failed:
            failure_reason = (
                f"cells failed={fail}, data-unavailable={data_unavail}, "
                f"tasks errored={errored}"
            )
            if not_successful:
                failure_reason += "; unsuccessful tasks: " + ", ".join(
                    f"{task_key}={status}"
                    for task_key, status in not_successful.items())
            if halted:
                failure_reason += f"; run halted: {halted}"

    await session.close()
    if migration_failed:
        # SystemExit raised inside an IPython notebook patched by nest_asyncio
        # can corrupt traceback rendering and hide the original cell failure.
        # A normal exception still gives CLI callers a non-zero exit code while
        # preserving a readable migration summary in notebook runners.
        raise RuntimeError(
            f"Migration failed for {name}: {failure_reason}"
        ) from None


if __name__ == "__main__":
    asyncio.run(main())

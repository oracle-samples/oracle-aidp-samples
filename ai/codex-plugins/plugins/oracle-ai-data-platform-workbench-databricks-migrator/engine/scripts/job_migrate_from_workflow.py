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
        --job-key <CLUSTER_ID_ALT> \\
        --job-name "sample_workflow" \\
        --cluster <CLUSTER_ID> \\
        --output-base /Workspace/user/output

    # With parameter overrides
    python3 job_migrate_from_workflow.py \\
        --job-key <uuid> \\
        --cluster <uuid> \\
        --param "start_date=2026-01-01" \\
        --param "end_date=2026-03-31"

    # Validate only (no migration)
    python3 job_migrate_from_workflow.py \\
        --job-key <uuid> \\
        --cluster <uuid> \\
        --validate-only

    # Resume interrupted run
    python3 job_migrate_from_workflow.py \\
        --job-key <uuid> \\
        --cluster <uuid> \\
        --skip-migrated \\
        --start-task Feature_CSI

    # From pre-built manifest (skip API fetch + validation)
    python3 job_migrate_from_workflow.py \\
        --manifest reports/sample_workflow_manifest.json \\
        --cluster <uuid> \\
        --output-base /Workspace/user/output
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _scripts_dir)

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

from aidp_executor import AIDPSession

# build_dag_from_workflow and build_dag are imported lazily inside main()
# (only when --job-key is used, not when --manifest is given).

# ─── Defaults ───────────────────────────────────────────────────────

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Generic — no hardcoded tenant/AIDP-instance config. lake/workspace/output
# are required CLI args (no default); AIDP_BASE is derived from the lake-OCID
# region; OCI profile defaults to "DEFAULT".
AIDP_BASE = None
DEFAULT_LAKE_OCID = None
DEFAULT_WORKSPACE_ID = None
DEFAULT_OCI_PROFILE = "DEFAULT"
DEFAULT_OUTPUT_BASE = None


def derive_export_base(manifest):
    """Return the workspace-relative export base (e.g. 'ai_dbx_exported/notebooks')
    from the manifest's task notebook paths, or '' if the notebooks aren't under
    a dbx_export base. Recognizes the export convention <base>/notebooks/<user-tree>.
    Empty result => non-export job, job_migrate behaves exactly as before.
    """
    import re as _re
    for job in manifest.get("jobs", []):
        for t in job.get("tasks", []):
            p = t.get("notebook_path", "")
            if p.startswith("/Workspace/"):
                p = p[len("/Workspace/"):]
            m = _re.match(r'^(.*?/notebooks)/(?:Users|Repos|Shared)/', p)
            if m:
                return m.group(1)
    return ""


def tprint(*args, **kwargs):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}]", *args, **kwargs)


async def main():
    parser = argparse.ArgumentParser(
        description="Migrate AIDP workflow job — fetch, validate, and run migration in one step")

    # Workflow identification (--job-key OR --manifest required)
    parser.add_argument("--job-key",
                        help="AIDP job key (UUID) from the workflow")
    parser.add_argument("--manifest",
                        help="Pre-built manifest JSON from build_dag_from_workflow.py (skips API fetch + validation)")
    parser.add_argument("--job-name",
                        help="Job name for output (default: from workflow/manifest)")
    parser.add_argument("--cluster", default=None,
                        help="AIDP cluster ID. If not specified, uses per-task cluster_id from workflow/manifest.")

    # Parameter overrides
    parser.add_argument("--param", action="append", default=[],
                        help="Override parameter key=value (repeatable)")

    # Output
    parser.add_argument("--output-base", default=DEFAULT_OUTPUT_BASE, required=DEFAULT_OUTPUT_BASE is None,
                        help="Workspace path for migrated notebooks (required)")
    parser.add_argument("--save-manifest",
                        help="Save generated manifest to file (optional)")
    parser.add_argument("--catalog-manifest", default="",
                        help="Path to the catalog-migration manifest (from migrate_catalog). "
                             "Used to deterministically remap source-catalog string literals "
                             "(e.g. \"main.schema\" -> \"default.schema\") in the saved notebooks.")

    # Execution control
    parser.add_argument("--validate-only", action="store_true",
                        help="Only validate notebooks exist and print task graph")
    _skip_grp = parser.add_mutually_exclusive_group()
    _skip_grp.add_argument("--skip-migrated", action="store_true", dest="skip_migrated", default=True,
                           help="Skip already-migrated notebooks (default: enabled).")
    _skip_grp.add_argument("--no-skip-migrated", action="store_false", dest="skip_migrated",
                           help="Force re-migration of all notebooks.")
    parser.add_argument("--start-task", default="",
                        help="Resume from this task_key (substring match)")
    parser.add_argument("--direct-execute", action="store_true",
                        help="Skip AI migration, execute cells as-is")
    parser.add_argument("--only-tasks", default="",
                        help="Comma-separated task_key substrings — run ONLY matching tasks. "
                             "Example: --only-tasks '11_2_hyperopt_GPU,11_3_gs'")

    # AIDP environment
    parser.add_argument("--aidp-base", default=None,
                        help="AIDP REST base URL. Default: derived from --lake-ocid region "
                             "(so end users don't need to pass it).")
    parser.add_argument("--lake-ocid", default=DEFAULT_LAKE_OCID, required=DEFAULT_LAKE_OCID is None)
    parser.add_argument("--workspace-id", default=DEFAULT_WORKSPACE_ID, required=DEFAULT_WORKSPACE_ID is None)
    parser.add_argument("--oci-profile", default=DEFAULT_OCI_PROFILE)
    parser.add_argument("--bucket-mapping", default=None,
                        help="(Deprecated/ignored) OCI namespace mapping now comes from "
                             "config/oci_bucket_tenancy_mapping.json; the old "
                             "s3_to_oci_bucket_mapping.csv no longer exists.")

    args = parser.parse_args()

    # Strip trailing slash(es) from --output-base so downstream f-strings like
    # f"{output_base}/{job_name}/..." don't produce a doubled slash (//). This
    # cleans both jm.OUTPUT_BASE and the direct args.output_base uses below.
    if args.output_base:
        args.output_base = args.output_base.rstrip("/")

    # Derive the AIDP base URL from the lake OCID region when not given, so the
    # end-user command doesn't need --aidp-base (the region is encoded in the
    # lake OCID: ocid1.aidataplatform.oc1.<region>.…). Mirrors build_dag_from_workflow.
    if not args.aidp_base:
        from aidp_executor import REGION_MAP
        _parts = args.lake_ocid.split(".")
        _region = REGION_MAP.get(_parts[3], _parts[3]) if len(_parts) > 3 else "<OCI_REGION>"
        args.aidp_base = f"https://aidp.{_region}.oci.oraclecloud.com/20240831"
        tprint(f"Derived AIDP base from lake region: {args.aidp_base}")

    if not args.manifest and not args.job_key:
        parser.error("Either --job-key or --manifest is required")

    if args.manifest:
        # ── Load pre-built manifest (skip steps 1-4) ──
        tprint(f"Loading manifest from {args.manifest}")
        with open(args.manifest) as f:
            manifest = json.load(f)
        job = manifest["jobs"][0]
        job_name = args.job_name or job["job_name"]
        if args.job_name:
            job["job_name"] = job_name

        # Build a parent->[resolved_child_paths] map from the manifest's
        # validated run_deps tree. Used by job_migrate._resolve_relative_dep()
        # to override naive parent_dir+relative resolution when a source
        # notebook has a stale `%run ./X` whose actual file is elsewhere.
        import job_migrate as _job_migrate
        _dep_resolution: dict = {}
        def _walk(parent: str, deps: list) -> None:
            _dep_resolution.setdefault(parent, [])
            for d in deps or []:
                child = d.get("path")
                if child:
                    _dep_resolution[parent].append(child)
                    _walk(child, d.get("nested_deps", []))
        for _t in job.get("tasks", []):
            _walk(_t["notebook_path"], _t.get("run_deps", []))
        if _dep_resolution:
            _job_migrate.set_dep_resolution(_dep_resolution)
            _entries = sum(len(v) for v in _dep_resolution.values())
            tprint(f"Dep resolution map: {len(_dep_resolution)} parents, {_entries} edges")

        # Apply --param overrides
        param_overrides = {}
        for p in args.param:
            key, _, value = p.partition("=")
            if not key:
                print(f"WARNING: Ignoring malformed --param: {p}")
                continue
            param_overrides[key.strip()] = value.strip()
        if param_overrides:
            job.setdefault("parameters", {}).update(param_overrides)
            tprint(f"Parameter overrides applied: {param_overrides}")

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
            print(f"  {t['task_key']}: {os.path.basename(t['notebook_path'])}{deps_str}")

        # ── Step 3: Pre-flight validation ──
        # Resolve validation cluster: CLI > first task > job default
        _val_cluster = args.cluster
        if not _val_cluster:
            for t in tasks:
                if t.get("cluster_id"):
                    _val_cluster = t["cluster_id"]
                    break
        if not _val_cluster:
            _jc = job_data.get("jobClusters", [])
            if _jc:
                _val_cluster = _jc[0].get("clusterKey")
        if not _val_cluster:
            print("ERROR: No --cluster specified and no cluster_id found in workflow tasks.")
            print("       Specify --cluster or ensure workflow tasks have cluster assignments.")
            sys.exit(1)

        # Ensure validation cluster is running
        from cluster_lifecycle import ensure_cluster_running, ensure_aidp_compat_installed
        import cluster_lifecycle as _cl
        _cl.AIDP_BASE = args.aidp_base
        _cl.DATALAKE_OCID = args.lake_ocid
        _cl.WORKSPACE_ID = args.workspace_id
        _cl.OCI_PROFILE = args.oci_profile
        _cl._SIGNER = None  # reset cached signer after profile change

        # Preflight prerequisite: the aidp_compat wheel must be uploaded to the
        # deps folder on this workspace before migrating. Fail fast here (also
        # runs under --validate-only) with a clear, actionable message so the
        # user can upload it BEFORE the long migration starts — rather than
        # aborting mid-run with a LibraryTaskException.
        _whl = _cl.resolve_aidp_compat_whl()
        if not _whl:
            print(f"ERROR: PREREQUISITE NOT MET — no aidp_compat-*.whl found in "
                  f"{_cl.AIDP_COMPAT_DEPS_DIR} on this workspace.")
            print(f"       Upload the wheel (from aidp_compat/dist/) to "
                  f"{_cl.AIDP_COMPAT_DEPS_DIR}, then retry.")
            sys.exit(1)
        tprint(f"Prerequisite OK: aidp_compat wheel present ({_whl})")

        await ensure_cluster_running(_val_cluster)

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
        run_deps = await discover_run_deps_for_tasks(val_session, tasks)
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
        manifest = build_manifest(job_name, tasks, layers, run_deps, job_data)

        # Apply --param overrides
        param_overrides = {}
        for p in args.param:
            key, _, value = p.partition("=")
            if not key:
                print(f"WARNING: Ignoring malformed --param: {p}")
                continue
            param_overrides[key.strip()] = value.strip()

        if param_overrides:
            manifest["jobs"][0]["parameters"].update(param_overrides)
            tprint(f"Parameter overrides applied: {param_overrides}")

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

    if args.validate_only:
        tprint("\n--validate-only: pre-flight checks passed. Exiting.")
        return

    # ── Step 5: Configure job_migrate globals ──
    import job_migrate as jm
    import agent_migrate as am

    jm.AIDP_BASE = args.aidp_base
    jm.DATALAKE_OCID = args.lake_ocid
    jm.WORKSPACE_ID = args.workspace_id
    jm.OCI_PROFILE = args.oci_profile
    jm.OUTPUT_BASE = args.output_base

    # Load source-catalog → default name remap from the catalog manifest so
    # string-literal catalog refs (e.g. SCHEMA = "main.sample_schema") are
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
    jm.DIRECT_EXECUTE = args.direct_execute

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
    # Propagate AIDP config into cluster_lifecycle module
    import cluster_lifecycle as _cl
    _cl.AIDP_BASE = jm.AIDP_BASE
    _cl.DATALAKE_OCID = jm.DATALAKE_OCID
    _cl.WORKSPACE_ID = jm.WORKSPACE_ID
    _cl.OCI_PROFILE = jm.OCI_PROFILE
    _cl._SIGNER = None  # reset cached signer after profile change

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
            import types
            import requests as _req_cs
            from aidp_executor import get_oci_signer
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

    # Bootstrap snippets — replayed on every fresh connect / cluster switch.
    # The third snippet (built by jm.build_oidlutils_bridge_snippet) installs
    # the oidlUtils wrapper that intercepts `getParameter` and reads from
    # task_values.json + manifest_params.json. Same helper is used by
    # job_migrate.py's per-task connect path so both stay in sync.
    tv_file = f"{args.output_base}/{job_name}/task_values.json"
    _bootstrap_snippets = [
        "from aidp_compat import dbutils, displayHTML, sql, translate_path",
        f"import os; os.makedirs('{args.output_base}', exist_ok=True)",
        jm.build_oidlutils_bridge_snippet(args.output_base, job_name),
    ]

    async def _connect_and_bootstrap(cluster_id: str):
        """Connect to a cluster and run all bootstrap snippets."""
        from cluster_lifecycle import ensure_cluster_running, ensure_aidp_compat_installed
        tprint(f"Connecting to cluster {cluster_id[:12]}...")
        await ensure_cluster_running(cluster_id)
        await ensure_aidp_compat_installed(cluster_id)
        await cluster.connect(cluster_id=cluster_id, session_name=f"aidp_mig_{job_name}")
        for snippet in _bootstrap_snippets:
            await cluster.execute(snippet, timeout=30)
            cluster.register_bootstrap(snippet)
        await asyncio.sleep(2)
        tprint(f"Cluster {cluster_id[:12]}... ready.")

    if args.cluster:
        # Explicit --cluster: connect immediately
        await _connect_and_bootstrap(args.cluster)
    else:
        tprint("No --cluster specified — will connect per-task from manifest.")

    tprint(f"Task values file: {tv_file}")

    session = cluster

    # ── Step 8: Run migration ──
    tprint(f"\n{'='*60}")
    tprint(f"STARTING MIGRATION: {job_name}")
    tprint(f"{'='*60}")
    tprint(f"Cluster: {args.cluster or '(per-task from manifest)'}")
    tprint(f"Output: {args.output_base}")
    tprint(f"Started: {datetime.now().isoformat()}")

    try:
        result = await jm.process_job(job, session)
    except Exception as e:
        print(f"  JOB ERROR: {job_name}: {e}")
        result = {"job_name": job_name, "error": str(e)}

    # ── Step 9: Summary ──
    tprint(f"\n{'='*60}")
    tprint(f"MIGRATION COMPLETE")
    print(f"{'='*60}")

    name = result.get("job_name", job_name)
    if "error" in result:
        print(f"  {name}: ERROR - {result['error']}")
    else:
        ok = result.get("total_ok", 0)
        fail = result.get("total_failed", 0)
        fix = result.get("total_fixed", 0)
        errored = result.get("total_errored", 0)
        skipped = result.get("total_skipped", 0)
        line = f"  {name}: Cells OK={ok} Failed={fail} Fixed={fix}"
        if errored:
            line += f" | Tasks errored={errored}"
        if skipped:
            line += f" | Tasks skipped={skipped}"
        print(line)

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())

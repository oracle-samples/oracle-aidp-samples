#!/usr/bin/env python3
"""SessionStart hook for the AIDP Databricks Migrator Codex plugin.

Stages the bundled OpenAI-based migration engine to ~/.aidp-migrator/engine so
skills can invoke a stable path without requiring a separate repository clone
or knowledge of Codex's plugin cache path.
"""
import json
import os
import shutil
import sys


PLUGIN_ROOT = os.environ.get("PLUGIN_ROOT") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(PLUGIN_ROOT, "engine")
DST_ROOT = os.path.join(os.path.expanduser("~"), ".aidp-migrator")
DST = os.path.join(DST_ROOT, "engine")
SKIP_DIRS = {"__pycache__", ".pytest_cache", ".git", "reports", "env"}


def _manifest_path() -> str:
    return os.path.join(DST_ROOT, ".staged_manifest.json")


# Every file the 0.2.x bundle shipped: the 0.2.x hook wrote no manifest, so a
# manifest-less staging dir is treated as a 0.2.x staging and pruned against
# this list (otherwise an upgrade would leave deleted 0.2.x modules — e.g. the
# old engine/setup.py — importable forever).
_LEGACY_020_BUNDLE = [
    "aidp_compat/SUPPORTED_OPERATIONS.md",
    "aidp_compat/__init__.py",
    "aidp_compat/bucket_shard.py",
    "aidp_compat/credentials.py",
    "aidp_compat/data.py",
    "aidp_compat/dbutils_shim.py",
    "aidp_compat/display.py",
    "aidp_compat/fs.py",
    "aidp_compat/glue_compat.py",
    "aidp_compat/jobs.py",
    "aidp_compat/library.py",
    "aidp_compat/notebook.py",
    "aidp_compat/notebook_policy.py",
    "aidp_compat/oci_throttle.py",
    "aidp_compat/optuna_compat.py",
    "aidp_compat/path_translator.py",
    "aidp_compat/s3_compat.py",
    "aidp_compat/safe_io.py",
    "aidp_compat/secrets.py",
    "aidp_compat/setup.py",
    "aidp_compat/spark_config.py",
    "aidp_compat/sql_magic.py",
    "aidp_compat/widgets.py",
    "requirements.txt",
    "run_migration.sh",
    "schemas/acceptance_contract.schema.json",
    "scripts/acceptance_contract.py",
    "scripts/agent_migrate.py",
    "scripts/aidp_executor.py",
    "scripts/analyze_jars.py",
    "scripts/analyze_notebooks.py",
    "scripts/build_dag.py",
    "scripts/build_dag_from_workflow.py",
    "scripts/catalog_ddl_rewriter.py",
    "scripts/cell_analyzer.py",
    "scripts/cell_context.py",
    "scripts/check_aws_creds.py",
    "scripts/check_data_availability.py",
    "scripts/check_data_availability_for_workflow.py",
    "scripts/clone_workflow.py",
    "scripts/cluster_lifecycle.py",
    "scripts/cluster_session.py",
    "scripts/consolidate_reports.py",
    "scripts/context_compactor.py",
    "scripts/context_tools.py",
    "scripts/deep_analyze_notebook.py",
    "scripts/deploy_aidp_compat.py",
    "scripts/enumerate_and_download.py",
    "scripts/extract_catalog_databricks.py",
    "scripts/fetch_spark_logs.py",
    "scripts/fuse_package_analyzer.py",
    "scripts/fuse_risk_db.json",
    "scripts/fuse_scanner.py",
    "scripts/install_cluster_libraries.py",
    "scripts/job_migrate.py",
    "scripts/job_migrate_from_workflow.py",
    "scripts/job_preflight.py",
    "scripts/migrate_catalog.py",
    "scripts/migrate_validate.py",
    "scripts/migration_429_detector.py",
    "scripts/openai_model.py",
    "scripts/provision_shard_buckets.sh",
    "scripts/scan_workspace_utils.py",
    "scripts/setup_migration_cluster.py",
    "scripts/test_jar_functionality.py",
    "scripts/throttle_coordinator.py",
    "scripts/upload_migration_dependencies.py",
    "setup.py",
    "tests/conftest.py",
    "tests/test_check_aws_creds_masking.py",
    "tests/test_context_compactor.py",
    "tests/test_extract_token_ingress.py",
    "tests/test_notebook_policy.py",
    "tests/test_sandbox_bootstrap.py",
    "tests/test_secrets.py",
]


def _valid_entry(rel) -> bool:
    """Manifest entries must be plain relative paths that RESOLVE inside the
    staging dir (realpath containment — string checks miss rooted drive-less
    paths and symlinked directories)."""
    if not isinstance(rel, str) or not rel or rel.startswith(("/", "\\", "~")):
        return False
    target = os.path.realpath(os.path.join(DST, rel))
    root = os.path.realpath(DST)
    return target == root or target.startswith(root + os.sep)


def _load_manifest() -> set:
    try:
        with open(_manifest_path()) as fh:
            loaded = json.load(fh)
    except FileNotFoundError:
        # No manifest: either a fresh install (DST empty — nothing to prune) or
        # a 0.2.x staging (its hook wrote no manifest) — prune the 0.2.x list.
        return set(_LEGACY_020_BUNDLE)
    except (OSError, ValueError):
        print("[aidp-migrator] staged manifest unreadable — ignoring it (stage continues)", file=sys.stderr)
        return set()
    if not isinstance(loaded, list):
        print("[aidp-migrator] staged manifest malformed — ignoring it (stage continues)", file=sys.stderr)
        return set()
    return {rel for rel in loaded if _valid_entry(rel)}


def _prune(src: str, dst: str, previously_staged: set) -> int:
    """Remove ONLY files this hook itself staged earlier that the bundled
    engine no longer ships (e.g. 0.2.x scripts removed in 0.3.0). Everything
    else in the staging dir is deployment-owned — customer YAML, an activated
    config/writer_wrappers.json, generated reports — and is never touched.
    A manifest-less staging is pruned against the embedded 0.2.x bundle list."""
    removed = 0
    for rel in sorted(previously_staged, reverse=True):
        if not _valid_entry(rel):
            continue
        if not os.path.exists(os.path.join(src, rel)) and not rel.endswith(".pyc"):
            target = os.path.join(dst, rel)
            if os.path.isfile(target):
                try:
                    os.remove(target)
                    removed += 1
                except OSError:
                    pass
    # drop directories the prune emptied (bundle-owned leftovers only)
    for root, dirs, files in os.walk(dst, topdown=False):
        if root != dst and not dirs and not files:
            rel = os.path.relpath(root, dst)
            if any(p.startswith(rel + os.sep) or p == rel for p in previously_staged):
                try:
                    os.rmdir(root)
                except OSError:
                    pass
    return removed


def stage():
    if not os.path.isdir(SRC):
        print(f"[aidp-migrator] bundled engine not found at {SRC}", file=sys.stderr)
        return False
    os.makedirs(DST, exist_ok=True)
    pruned = _prune(SRC, DST, _load_manifest())
    if pruned:
        print(f"[aidp-migrator] pruned {pruned} stale bundle file(s) from {DST}")
    staged_now = []
    for root, dirs, files in os.walk(SRC):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        rel = os.path.relpath(root, SRC)
        out = DST if rel == "." else os.path.join(DST, rel)
        os.makedirs(out, exist_ok=True)
        for name in files:
            staged_now.append(name if rel == "." else os.path.join(rel, name))
            if name.endswith((".pyc", ".pyo")):
                continue
            shutil.copy2(os.path.join(root, name), os.path.join(out, name))
    try:
        import json
        with open(_manifest_path(), "w") as fh:
            json.dump(sorted(staged_now), fh)
    except OSError:
        pass

    return True


def main():
    try:
        if stage():
            print(
                "[aidp-migrator] engine staged to ~/.aidp-migrator/engine. "
                "Run: python -m pip install -r ~/.aidp-migrator/engine/requirements.txt; "
                "set OPENAI_API_KEY plus AIDP/OCI/Databricks coordinates before migration."
            )
    except Exception as exc:
        print(
            f"[aidp-migrator] stage warning: {exc}. "
            "Run hooks/session_start.py manually from the plugin root.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()

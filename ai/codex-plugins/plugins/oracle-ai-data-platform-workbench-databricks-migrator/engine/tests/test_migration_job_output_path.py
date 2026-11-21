"""Job-folder orchestration regression for native mixed-language %run."""
import ast
import json
from pathlib import Path
import re

import pytest

from dbx_export.sanitizer import migration_job_output_path

ROOT = Path(__file__).resolve().parents[1]


def extracted(name):
    tree = ast.parse((ROOT / "scripts/job_migrate.py").read_text(encoding="utf-8"))
    node = next(n for n in tree.body if getattr(n, "name", None) == name)
    ns = {"migration_job_output_path": migration_job_output_path, "re": re, "json": json,
          "MIGRATE_OUTPUT": "/Workspace/output"}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<production-function>", "exec"), ns)
    return ns[name]


@pytest.mark.parametrize("name", ["EXRISK_Scoring_Job", "job-v1.2", "job__name"])
def test_existing_space_free_folders_unchanged(name):
    assert migration_job_output_path("/Workspace/output/", name) == "/Workspace/output/" + name


def test_space_bearing_folder_uses_standard_sanitizer():
    assert migration_job_output_path("/Workspace/output", "AutoRefit Feature Generation UNIT") == (
        "/Workspace/output/AutoRefit_Feature_Generation_UNIT"
    )


@pytest.mark.parametrize("name", ["Job\tName", "Job\nName", "Job\u00a0Name"])
def test_other_whitespace_is_removed(name):
    assert migration_job_output_path("/Workspace/output", name).endswith("/Job_Name")


@pytest.mark.parametrize("base,name", [("/Workspace/has space", "Job"), ("", "Job"),
    ("/Workspace/output", "../Job"), ("/Workspace/output", ".."),
    ("/Workspace/output", " . "), ("/Workspace/output", " .. "),
    ("/Workspace/output", "\t..\n"), ("/Workspace/output", "\u00a0.\u00a0"),
    ("/Workspace/output", " _.._ "),
    ("/Workspace/output", " "), ("/Workspace/output", "")])
def test_invalid_paths_fail_before_writes(base, name):
    with pytest.raises(ValueError):
        migration_job_output_path(base, name)


def test_three_dots_are_a_legal_job_folder():
    assert migration_job_output_path("/Workspace/output", " ... ") == "/Workspace/output/..."


def test_registry_bridge_and_percent_run_share_space_free_job_folder():
    job = "AutoRefit Feature Generation UNIT"
    base = migration_job_output_path("/Workspace/output", job)
    registry = extracted("_registry_path_for_job")(job)
    bridge = extracted("build_oidlutils_bridge_snippet")("/Workspace/output", job)
    assert registry == base + "/migration_registry.json"
    assert base + "/manifest_params.json" in bridge
    assert base + "/task_values.json" in bridge
    target = base + "/notebooks/GIT/HelperFunctions.ipynb"
    assert extracted("_quote_percent_run_target")(target) == target
    # An old space-root artifact cannot satisfy the in-job resume prefix.
    old = "/Workspace/output/" + job + "/notebooks/GIT/HelperFunctions.ipynb"
    assert not old.startswith(base + "/notebooks/")


def test_all_active_output_consumers_use_shared_path_rule():
    for relative in ["scripts/job_migrate.py", "scripts/job_migrate_from_workflow.py"]:
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.JoinedStr):
                rendered = ast.unparse(node)
                assert "{MIGRATE_OUTPUT}/{job_name}" not in rendered
                assert "{migrate_output}/{job_name}" not in rendered
                assert "{args.migrate_output}/{job_name}" not in rendered

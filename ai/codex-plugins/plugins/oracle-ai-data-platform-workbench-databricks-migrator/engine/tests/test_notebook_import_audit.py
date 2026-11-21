import sys

import pytest

from dbx_export.__main__ import (
    consolidate_cluster_requirements,
    _pip_source_for_init_scripts,
    phase1_discover_existing_cluster_libraries,
)
from dbx_export.import_audit import audit_notebook_imports
from dbx_export.dependency_resolver import FileNode, Graph, JobNode, NotebookNode
from dbx_export.report import Report
from dbx_export.requirements import RequirementConsolidationError, RequirementSource


def _notebook(path, source, referenced_by):
    return NotebookNode(
        dbx_path=path,
        role="task" if referenced_by[0].startswith("job:") else "run_dep",
        referenced_by=referenced_by,
        nb_dict={"cells": [{"cell_type": "code", "source": source}]},
    )


def _python_module(path, source, referenced_by):
    return NotebookNode(
        dbx_path=path,
        role="resource",
        referenced_by=referenced_by,
        content=source.encode(),
        is_notebook=False,
        final_extension=".py",
    )


def _graph():
    graph = Graph()
    graph.jobs["42"] = JobNode(
        dbx_job_id="42",
        depth=0,
        job_def={
            "settings": {
                "tasks": [{
                    "task_key": "root",
                    "existing_cluster_id": "cluster-1",
                    "notebook_task": {"notebook_path": "/root"},
                }]
            }
        },
    )
    return graph


def _report():
    return Report(
        run_id="test",
        dbx_root_job_id="42",
        export_output="/tmp/export",
        prefix="TEST",
    )


def _requirements(graph, generated):
    key = generated["existing_cluster:cluster-1"]
    return graph.files[key].content.decode()


def test_recursive_local_python_import_adds_approved_dependency_with_provenance():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "from helperFunctions.helper import *\n",
        ["job:42/task:root"],
    )
    graph.notebooks["/project/helperFunctions/helper.py"] = _python_module(
        "/project/helperFunctions/helper.py",
        "from imports.imports import *\n",
        ["resource-of:/root"],
    )
    graph.notebooks["/project/imports/imports.py"] = _python_module(
        "/project/imports/imports.py",
        "import os\nimport matplotlib.pyplot as plt\nimport mystery_package\n",
        ["resource-of:/root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert content.count("matplotlib") == 1
    assert "helperFunctions" not in content
    assert "mystery_package" not in content
    assert any(
        entry.startswith("INFO inferred-python-dependency: matplotlib")
        and "source=/project/imports/imports.py" in entry
        and "inferred from `import matplotlib.pyplot as plt`" in entry
        for entry in report.errors
    )
    assert any(
        entry.startswith("WARN unknown-python-import: mystery_package")
        and "source=/project/imports/imports.py" in entry
        for entry in report.errors
    )


def test_explicit_pin_wins_over_recursive_approved_inference():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "%pip install matplotlib==3.9.1\nfrom helperFunctions.helper import *\n",
        ["job:42/task:root"],
    )
    graph.notebooks["/project/helperFunctions/helper.py"] = _python_module(
        "/project/helperFunctions/helper.py",
        "import matplotlib.pyplot as plt\n",
        ["resource-of:/root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "matplotlib==3.9.1" in content
    assert not any("inferred-python-dependency: matplotlib" in e for e in report.errors)


def test_recursive_local_cycle_is_bounded_and_dependency_is_deduplicated():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import package.a\n", ["job:42/task:root"]
    )
    graph.notebooks["/project/package/a.py"] = _python_module(
        "/project/package/a.py",
        "import package.b\nimport seaborn as sns\n",
        ["resource-of:/root"],
    )
    graph.notebooks["/project/package/b.py"] = _python_module(
        "/project/package/b.py",
        "import package.a\nimport seaborn\n",
        ["resource-of:/root"],
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert content.count("seaborn") == 1


def test_local_module_named_like_approved_package_is_not_auto_installed():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import matplotlib\n", ["job:42/task:root"]
    )
    graph.notebooks["/project/matplotlib.py"] = _python_module(
        "/project/matplotlib.py", "VALUE = 1\n", ["resource-of:/root"]
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert "matplotlib" not in content


def test_existing_approved_notebook_imports_and_unknown_warning_contract():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "import os\nimport pyspark\nimport oci\nimport seaborn as sns\n"
        "from PIL import Image\nimport sklearn.metrics\nimport unknown_lib\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "seaborn" in content
    assert "pillow" in content
    assert "scikit-learn" in content
    assert "unknown_lib" not in content
    warnings = [e for e in report.errors if e.startswith("WARN unknown-python-import")]
    assert len(warnings) == 1
    assert "unknown_lib" in warnings[0]


def test_top5_runtime_imports_map_to_their_distribution_names():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "from dateutil.relativedelta import relativedelta\n"
        "from joblib import dump\n"
        "import pytz\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "python-dateutil" in content
    assert "joblib" in content
    assert "pytz" in content
    assert not any(
        entry.startswith("WARN unknown-python-import:")
        and any(name in entry for name in ("dateutil", "joblib", "pytz"))
        for entry in report.errors
    )


def test_explicit_wheel_source_suppresses_inferred_duplicate():
    graph = _graph()
    wheel = "/dbfs/wheels/seaborn-0.12.2-py3-none-any.whl"
    graph.notebooks["/root"] = _notebook(
        "/root",
        f"%pip install {wheel}\nimport seaborn\nimport pytz\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert wheel in content
    assert "\nseaborn\n" not in f"\n{content}\n"
    assert "\npytz\n" in f"\n{content}\n"
    assert any(
        entry.startswith("INFO import-managed-by-passthrough: seaborn")
        for entry in report.errors
    )


def test_explicit_vcs_sources_suppress_inferred_duplicate():
    for explicit in (
        "git+https://github.com/mwaskom/seaborn.git",
        "-e git+https://github.com/mwaskom/seaborn.git#egg=seaborn",
    ):
        graph = _graph()
        graph.notebooks["/root"] = _notebook(
            "/root",
            f"%pip install {explicit}\nimport seaborn as sns\n",
            ["job:42/task:root"],
        )
        report = _report()

        content = _requirements(
            graph,
            consolidate_cluster_requirements(graph, "42", report=report),
        )

        assert explicit in content
        assert "\nseaborn\n" not in f"\n{content}\n"
        assert any(
            entry.startswith("INFO import-managed-by-passthrough: seaborn")
            for entry in report.errors
        )


def test_explicit_pillow_simd_alias_suppresses_pillow_inference():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "%pip install pillow-simd\nfrom PIL import Image\n",
        ["job:42/task:root"],
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert "pillow-simd" in content
    assert "\npillow\n" not in f"\n{content}\n"


@pytest.mark.parametrize("install", (
    "%pip install pytz==2024.1 --hash=sha256:abc",
    'import subprocess\nsubprocess.check_call(["pip", "install", '
    '"pytz==2024.1", "--hash=sha256:abc"])',
    'import os\nos.system("pip install pytz==2024.1 --hash=sha256:abc")',
    'import pip\npip.main(["install", "pytz==2024.1", '
    '"--hash=sha256:abc"])',
))
def test_hash_blocked_requirement_does_not_suppress_an_uninstalled_provider(install):
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        f"{install}\nimport pytz\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "pytz" in content
    assert any("inferred-python-dependency: pytz" in e for e in report.errors)


def test_hash_blocked_opaque_artifact_is_not_reinferred_unhashed():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "%pip install https://example.invalid/private-artifact.bin "
        "--hash=sha256:abc\nimport pytz\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "pytz" not in content
    assert any(
        "hash-protected dependency has no parseable distribution identity"
        in entry
        for entry in report.errors
    )
    assert not any("inferred-python-dependency: pytz" in e for e in report.errors)


def test_hash_blocked_init_script_does_not_suppress_an_uninstalled_provider():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import pytz\n", ["job:42/task:root"]
    )
    graph.files["dbfs:/init.sh"] = FileNode(
        dbx_source="dbfs:/init.sh",
        kind="init_script",
        referenced_by=["job:42/task:root"],
        content=b"pip install pytz==2024.1 --hash=sha256:abc\n",
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "pytz" in content
    assert any("inferred-python-dependency: pytz" in entry for entry in report.errors)


class _ExistingClusterLibraries:
    def __init__(self, statuses):
        self._statuses = statuses

    def get_cluster_libraries(self, cluster_id):
        assert cluster_id == "cluster-1"
        return {"library_statuses": self._statuses}


def test_installed_source_cluster_pypi_provider_is_delivered_before_suppression():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import cv2\n", ["job:42/task:root"]
    )
    phase1_discover_existing_cluster_libraries(
        _ExistingClusterLibraries([{
            "status": "INSTALLED",
            "library": {"pypi": {"package": "opencv-python-headless==4.10.0.84"}},
        }]),
        graph,
    )

    content = _requirements(
        graph, consolidate_cluster_requirements(graph, "42", report=_report())
    )

    assert "opencv-python-headless==4.10.0.84" in content
    assert "\nopencv-python\n" not in f"\n{content}\n"


def test_existing_cluster_custom_pypi_repo_is_reported(capsys):
    graph = _graph()
    phase1_discover_existing_cluster_libraries(
        _ExistingClusterLibraries([{
            "status": "INSTALLED",
            "library": {"pypi": {
                "package": "private-package==1.0",
                "repo": "https://private.example/simple",
            }},
        }]),
        graph,
    )

    assert "custom repo" in capsys.readouterr().out


@pytest.mark.parametrize("status", ("FAILED", "UNINSTALLING", None))
def test_noninstalled_source_cluster_pypi_does_not_suppress_inference(status):
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import cv2\n", ["job:42/task:root"]
    )
    phase1_discover_existing_cluster_libraries(
        _ExistingClusterLibraries([{
            "status": status,
            "library": {"pypi": {"package": "opencv-python-headless"}},
        }]),
        graph,
    )

    content = _requirements(
        graph, consolidate_cluster_requirements(graph, "42", report=_report())
    )

    assert "\nopencv-python\n" in f"\n{content}\n"
    assert "opencv-python-headless" not in content


def test_dynamic_subprocess_provider_adds_a_structured_inference_barrier():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "import subprocess\npackage = get_package()\n"
        "subprocess.check_call(['pip', 'install', package])\nimport pytz\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph, consolidate_cluster_requirements(graph, "42", report=report)
    )

    assert "\npytz\n" not in f"\n{content}\n"
    assert any(item["status"] == "UNRESOLVED" for item in report.pip_findings)
    assert not any("inferred-python-dependency: pytz" in e for e in report.errors)


def test_declared_wheel_identity_suppresses_alias_inference():
    graph = _graph()
    task = graph.jobs["42"].job_def["settings"]["tasks"][0]
    task["libraries"] = [{
        "whl": "/dbfs/wheels/pillow_simd-9.5.0.post2-py3-none-any.whl",
    }]
    graph.notebooks["/root"] = _notebook(
        "/root", "from PIL import Image\n", ["job:42/task:root"]
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert "\npillow\n" not in f"\n{content}\n"


def test_unknown_explicit_provider_family_suppresses_inference_with_warning():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "%pip install opencv-python-rolling\nimport cv2\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph, consolidate_cluster_requirements(graph, "42", report=report)
    )

    assert "opencv-python-rolling" in content
    assert "\nopencv-python\n" not in f"\n{content}\n"
    assert any("alternate-provider" in entry for entry in report.errors)


def test_import_audit_failure_is_warning_and_does_not_abort(monkeypatch):
    import dbx_export.__main__ as exporter

    graph = _graph()
    report = _report()

    def fail_audit(*args, **kwargs):
        raise RuntimeError("synthetic audit failure")

    monkeypatch.setattr(exporter, "audit_notebook_imports", fail_audit)
    result = consolidate_cluster_requirements(graph, "42", report=report)

    assert result
    assert any("import-audit-unavailable" in entry for entry in report.errors)


def test_declared_egg_identity_suppresses_alias_inference():
    graph = _graph()
    task = graph.jobs["42"].job_def["settings"]["tasks"][0]
    task["libraries"] = [{
        "egg": "/dbfs/eggs/opencv_python_headless-4.10.0.84-py3.egg",
    }]
    graph.notebooks["/root"] = _notebook(
        "/root", "import cv2\n", ["job:42/task:root"]
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert "\nopencv-python\n" not in f"\n{content}\n"


def test_aidp_first_wheel_identity_suppresses_inference():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import seaborn\n", ["job:42/task:root"]
    )
    graph.files["dbfs:/wheels/seaborn.whl"] = FileNode(
        dbx_source="dbfs:/wheels/seaborn.whl",
        kind="whl",
        referenced_by=["job:42/task:root"],
        aidp_repo_path=(
            "/Workspace/migration-dependencies/libs/"
            "seaborn-0.13.2-py3-none-any.whl"
        ),
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert "\nseaborn\n" not in f"\n{content}\n"


@pytest.mark.parametrize("provider", (
    "opencv-python-headless",
    "opencv-contrib-python",
    "opencv-contrib-python-headless",
))
def test_explicit_opencv_provider_suppresses_second_cv2_provider(provider):
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", f"%pip install {provider}\nimport cv2\n", ["job:42/task:root"]
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert provider in content
    assert "\nopencv-python\n" not in f"\n{content}\n"


def test_google_namespace_is_unknown_instead_of_guessing_storage():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "from google.cloud import bigquery\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "google-cloud-storage" not in content
    assert any(
        entry.startswith("WARN unknown-python-import: google")
        for entry in report.errors
    )


def test_unrelated_cluster_local_module_does_not_hide_requirement():
    graph = _graph()
    graph.jobs["42"].job_def["settings"]["tasks"].append({
        "task_key": "other",
        "existing_cluster_id": "cluster-2",
        "notebook_task": {"notebook_path": "/other"},
    })
    graph.notebooks["/root"] = _notebook(
        "/root", "import pytz\n", ["job:42/task:root"]
    )
    graph.notebooks["/other"] = _notebook(
        "/other", "import helper\n", ["job:42/task:other"]
    )
    graph.notebooks["/other_project/pytz.py"] = _python_module(
        "/other_project/pytz.py", "VALUE = 1\n", ["resource-of:/other"]
    )

    generated = consolidate_cluster_requirements(graph, "42", report=_report())
    cluster_one = graph.files[generated["existing_cluster:cluster-1"]].content.decode()

    assert "pytz" in cluster_one


def test_non_provider_pip_option_does_not_suppress_inference():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import pytz\n", ["job:42/task:root"]
    )
    graph.files["dbfs:/requirements.txt"] = FileNode(
        dbx_source="dbfs:/requirements.txt",
        kind="requirements",
        referenced_by=["job:42/task:root"],
        content=b"--extra-index-url https://packages.example.invalid/simple\n",
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert "pytz" in content


def test_structured_inference_barrier_suppresses_inferred_imports():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import pytz\n", ["job:42/task:root"]
    )
    result = audit_notebook_imports(
        graph,
        {"/root"},
        [RequirementSource(
            "unresolved provider",
            "",
            inference_barrier=True,
        )],
    )
    assert not any(spec.startswith("pytz") for spec, _ in result.inferred)


def test_init_script_wheel_is_managed_before_import_inference():
    graph = _graph()
    graph.files["/Workspace/init/bootstrap.sh"] = FileNode(
        dbx_source="/Workspace/init/bootstrap.sh",
        kind="init_script",
        referenced_by=["job:42/task:root"],
        content=b"#!/bin/sh\n",
    )
    graph.files["/Workspace/libs/opencv_python_headless-4.10.0-py3-none-any.whl"] = FileNode(
        dbx_source="/Workspace/libs/opencv_python_headless-4.10.0-py3-none-any.whl",
        kind="whl",
        referenced_by=["init_script:/Workspace/init/bootstrap.sh"],
    )
    source = _pip_source_for_init_scripts(graph, {"job:42/task:root"})
    assert "opencv-python-headless" in source.managed_names


def test_unclassified_passthrough_conservatively_suppresses_inference():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "%pip install /mnt/private/custom-artifact.zip\nimport pytz\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "/mnt/private/custom-artifact.zip" in content
    assert "\npytz\n" not in f"\n{content}\n"
    assert any(
        entry.startswith("INFO import-managed-by-passthrough: pytz")
        for entry in report.errors
    )


def test_magic_parser_preserves_python_continuations_and_restart_imports():
    cases = (
        "import pytz\nmsg = ('%d items'\n       % 3)\n",
        "import pytz\nflag = (1\n        != 2)\n",
        "%restart_python\nimport pytz\n",
        "%reload_ext autoreload\n%autoreload 2\nimport pytz\n",
        "!echo preparing\nimport pytz\n",
    )
    for source in cases:
        graph = _graph()
        graph.notebooks["/root"] = _notebook(
            "/root", source, ["job:42/task:root"]
        )
        report = _report()

        content = _requirements(
            graph,
            consolidate_cluster_requirements(graph, "42", report=report),
        )

        assert "pytz" in content
        assert not any(
            entry.startswith("WARN import-audit-unparsed:")
            for entry in report.errors
        )


def test_residual_parse_failure_is_reported_with_cell_location():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "def broken(:\n    import pytz\n", ["job:42/task:root"]
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "\npytz\n" not in f"\n{content}\n"
    assert any(
        entry.startswith("WARN import-audit-unparsed: /root:cell0")
        for entry in report.errors
    )


def test_shared_policy_covers_runtime_jvm_and_additional_approved_imports():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "import mlflow\nimport dbutils\nimport org.apache.spark\n"
        "import yaml\nimport cv2\n",
        ["job:42/task:root"],
    )
    report = _report()

    content = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert "pyyaml" in content
    assert "opencv-python" in content
    for name in ("mlflow", "dbutils", "org"):
        assert not any(
            entry.startswith(f"WARN unknown-python-import: {name}")
            for entry in report.errors
        )


def test_python39_stdlib_fallback_uses_shared_policy(monkeypatch):
    monkeypatch.delattr(sys, "stdlib_module_names", raising=False)
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root",
        "import os\nimport json\nimport re\nimport calendar\nimport shlex\n"
        "import zoneinfo\n",
        ["job:42/task:root"],
    )
    report = _report()

    _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert not any(
        entry.startswith("WARN unknown-python-import:")
        for entry in report.errors
    )


def test_relative_import_is_reported_as_unparsed_residual():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "from .helper import VALUE\n", ["job:42/task:root"]
    )
    report = _report()

    _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=report),
    )

    assert any(
        entry.startswith("WARN import-audit-unparsed: /root:cell0")
        and "relative import not traversed" in entry
        for entry in report.errors
    )


def test_import_audit_is_idempotent_across_requirement_rebuilds():
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import pytz\n", ["job:42/task:root"]
    )

    first = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )
    second = _requirements(
        graph,
        consolidate_cluster_requirements(graph, "42", report=_report()),
    )

    assert first == second
    assert first.count("pytz") == 1


def test_custom_policy_controls_import_mapping(tmp_path):
    policy = tmp_path / "policy.yaml"
    policy.write_text(
        """schema_version: 1
protected_packages: {}
runtime_provided_packages: []
import_to_distribution:
  custom_mod: custom-dist==1.2.3
provided_imports: []
jvm_roots: []
stdlib_imports: [builtins]
rules: []
""",
        encoding="utf-8",
    )
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "import custom_mod\n", ["job:42/task:root"]
    )

    content = _requirements(
        graph,
        consolidate_cluster_requirements(
            graph,
            "42",
            compatibility_policy_file=str(policy),
            report=_report(),
        ),
    )

    assert "custom-dist==1.2.3" in content


def test_invalid_import_policy_aliases_fail_closed(tmp_path):
    policy = tmp_path / "policy.yaml"
    policy.write_text(
        """schema_version: 1
protected_packages: {}
runtime_provided_packages: []
import_to_distribution:
  PIL:
    requirement: Pillow
    aliases: pillow-simd
rules: []
""",
        encoding="utf-8",
    )
    graph = _graph()
    graph.notebooks["/root"] = _notebook(
        "/root", "from PIL import Image\n", ["job:42/task:root"]
    )

    with pytest.raises(
        RequirementConsolidationError,
        match="aliases must be a list",
    ):
        consolidate_cluster_requirements(
            graph,
            "42",
            compatibility_policy_file=str(policy),
            report=_report(),
        )

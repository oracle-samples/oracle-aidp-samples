import json

from dbx_export.dbx_client import DbxClient, DbxConfig, ExportedFile
from dbx_export.dependency_resolver import (
    DependencyResolver,
    extract_run_targets,
    extract_workspace_resource_paths,
)
from dbx_export.__main__ import _notebooks_reachable_from
from scripts.job_migrate import scan_for_run_deps


def _notebook(*sources):
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "cell_type": "code",
                "source": source.splitlines(keepends=True),
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            }
            for source in sources
        ]
    }


def _notebook_bytes(*sources):
    return json.dumps(_notebook(*sources)).encode("utf-8")


def test_loose_scan_does_not_classify_ordinary_runtime_assets():
    notebook = _notebook(
        'model_dir = "/Users/u/models/best_model"\n'
        'plot_path = "/Workspace/reports/monitoring.png"\n'
        'experiment = "/Users/u/MLflow/EX_PAYLOAD_RUN_JOBS"\n'
    )

    assert extract_run_targets(notebook, "/Workspace/Jobs/main") == []


def test_unresolved_dynamic_dispatch_enables_cross_cell_candidates_only():
    notebook = _notebook(
        'dispatch = [("score", "./score_child")]\n'
        'plot_path = "/Workspace/reports/monitoring.png"\n',
        'model_path = "/Users/u/model/{epoch}"\n'
        'dbutils.notebook.run(runtime_selected_path, 3600)\n',
    )

    targets = extract_run_targets(notebook, "/Workspace/Jobs/main")

    assert "/Workspace/Jobs/score_child" in targets
    assert all(not target.endswith("monitoring.png") for target in targets)
    assert all("{epoch}" not in target for target in targets)


def test_commented_dynamic_dispatch_does_not_enable_loose_scan():
    notebook = _notebook(
        'candidate = "./not_a_dependency"\n'
        '# dbutils.notebook.run(runtime_selected_path, 3600)\n'
    )

    assert extract_run_targets(notebook, "/Workspace/Jobs/main") == []


def test_resolved_parameter_dispatch_does_not_pull_unrelated_path_literals():
    notebook = _notebook(
        'root = dbutils.widgets.get("root")\n'
        'target = f"{root}/child"\n'
        'artifact = "/Users/u/models/best_model"\n'
        'dbutils.notebook.run(target, 3600)\n'
    )

    targets = extract_run_targets(
        notebook,
        "/Workspace/Jobs/main",
        parameter_bindings={"root": "/Workspace/Jobs"},
    )

    assert targets == ["/Jobs/child"]


def test_parameter_derived_workspace_file_read_is_discovered():
    notebook = _notebook(
        'root = dbutils.widgets.get("model_scaling_files_path").strip()\n'
        'with open(f"{root}/scaling_files/Feature_List.csv") as handle:\n'
        '    feature_list = handle.read()\n'
    )

    assert extract_workspace_resource_paths(
        notebook,
        parameter_bindings={
            "model_scaling_files_path": (
                "/Workspace/Users/user4@example.com/"
                "EXRISK/DL V2/artifacts"
            ),
        },
    ) == {
        "/Workspace/Users/user4@example.com/EXRISK/DL V2/"
        "artifacts/scaling_files/Feature_List.csv"
    }


def test_parameter_derived_workspace_write_is_not_an_export_dependency():
    notebook = _notebook(
        'root = dbutils.widgets.get("output_root")\n'
        'with open(f"{root}/result.csv", "w") as handle:\n'
        '    handle.write("x")\n'
    )

    assert extract_workspace_resource_paths(
        notebook,
        parameter_bindings={"output_root": "/Workspace/Users/u/output"},
    ) == set()


def test_resolver_exports_parameter_derived_workspace_file():
    master = _notebook_bytes(
        'root = dbutils.widgets.get("model_scaling_files_path").strip()\n'
        'with open(f"{root}/scaling_files/Feature_List.csv") as handle:\n'
        '    feature_list = handle.read()\n'
    )
    resource = (
        "/Users/user4@example.com/EXRISK/DL V2/artifacts/"
        "scaling_files/Feature_List.csv"
    )

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{
                "task_key": "score",
                "notebook_task": {
                    "notebook_path": "/Jobs/score",
                    "base_parameters": {
                        "model_scaling_files_path": (
                            "/Workspace/Users/user4@example.com/"
                            "EXRISK/DL V2/artifacts"
                        ),
                    },
                },
            }]}}

        def export_notebook(self, path):
            assert path == "/Jobs/score"
            return ExportedFile(master, True, ".ipynb")

        def export_workspace_resource(self, path):
            assert path == resource
            return ExportedFile(b'{"Feature_List_path": "oci://features"}', False, ".csv")

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _message: None).resolve("1")

    assert graph.notebooks[resource].role == "resource"
    assert graph.notebooks[resource].content.startswith(b'{"Feature_List_path"')


def test_migration_scanner_uses_notebook_wide_dynamic_gate_and_asset_filter():
    deps = scan_for_run_deps(_notebook_bytes(
        'dispatch = [("score", "./score_child")]\n'
        'image = "/Workspace/reports/monitoring.png"\n',
        'dbutils.notebook.run(runtime_selected_path, 3600)\n',
    ))

    paths = [path for path, _ in deps]
    assert "./score_child.ipynb" in paths
    assert all(not path.endswith("monitoring.png.ipynb") for path in paths)


def test_migration_scanner_ignores_paths_without_dynamic_dispatch():
    deps = scan_for_run_deps(_notebook_bytes(
        'model_dir = "/Users/u/models/best_model"\n'
        'experiment = "/Users/u/MLflow/EX_PAYLOAD_RUN_JOBS"\n'
    ))

    assert deps == []


def test_resolver_follows_notebook_run_path_from_percent_run_config():
    """A %run config shares its map with the caller's dynamic notebook.run."""
    notebooks = {
        "/Jobs/main": _notebook_bytes(
            '%run ./Config/0_Config\n'
            'cohort = dbutils.widgets.get("cohort")\n'
            'query_path = cohort_config[cohort]["fresh_base_query_path"]\n'
            'dbutils.notebook.run(query_path, 0, {"REFERENCE_DATE": "2026-09-10"})\n'
        ),
        "/Jobs/Config/0_Config": _notebook_bytes(
            'segment_a_query_path = "./Queries/segment_a"\n'
            'segment_a_config = {\n'
            '    "fresh_base_query_path": segment_a_query_path,\n'
            '    "model_file": translate_path("/Volumes/model.json"),\n'
            '    "range": np.arange(1, 3),\n'
            '}\n'
            'segment_b_query_path = "./Queries/segment_b"\n'
            'segment_b_config = {\n'
            '    "fresh_base_query_path": segment_b_query_path,\n'
            '    "model_file": translate_path("/Volumes/model.json"),\n'
            '}\n'
            'cohort_config = {"segment_a": segment_a_config, "segment_b": segment_b_config}\n'
        ),
        "/Jobs/Queries/segment_a": _notebook_bytes('print("segment_a")\n'),
        "/Jobs/Queries/segment_b": _notebook_bytes('print("segment_b")\n'),
    }

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{
                "task_key": "main",
                "notebook_task": {"notebook_path": "/Jobs/main"},
            }]}}

        def export_notebook(self, path):
            return ExportedFile(notebooks[path], True, ".ipynb")

        def get_workspace_status(self, _path):
            return {"object_type": "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _message: None).resolve("1")

    assert "/Jobs/Queries/segment_a" in graph.notebooks
    assert "/Jobs/Queries/segment_b" in graph.notebooks
    assert "/Jobs/main" in graph.notebooks["/Jobs/Queries/segment_a"].referenced_by
    assert "/Jobs/Queries/segment_a" in _notebooks_reachable_from(
        graph, {"/Jobs/main"}
    )


def test_percent_run_namespace_does_not_requeue_skipped_asset():
    """A namespace pass must not loop after the BFS classifies an asset."""
    notebooks = {
        "/Jobs/main": _notebook_bytes(
            "%run ./Config/cfg\n"
            'candidate = "/Users/u/MLflow/exp"\n'
            "dbutils.notebook.run(runtime_selected_path, 0)\n"
        ),
        "/Jobs/Config/cfg": _notebook_bytes("setting = 'x'\n"),
    }
    status_calls = []

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{
                "task_key": "main",
                "notebook_task": {"notebook_path": "/Jobs/main"},
            }]}}

        def export_notebook(self, path):
            return ExportedFile(notebooks[path], True, ".ipynb")

        def get_workspace_status(self, path):
            status_calls.append(path)
            assert status_calls.count(path) <= 5, f"asset re-enqueued repeatedly: {path}"
            return {"object_type": "MLFLOW_EXPERIMENT" if path.endswith("/exp")
                    else "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _message: None).resolve("1")

    assert status_calls.count("/Users/u/MLflow/exp") == 1
    assert "/Users/u/MLflow/exp" in graph.skipped_assets


def test_unknown_selector_only_yields_path_shaped_candidates():
    notebook = _notebook(
        "config = {\n"
        "    'a': './nb_a',\n"
        "    'timeout': 3600,\n"
        "    'name': 'job',\n"
        "    'owner': 'team@example.com',\n"
        "    'b': translate_path('./nb_b'),\n"
        "}\n"
        "selector = dbutils.widgets.get('selector')\n"
        "dbutils.notebook.run(config[selector], 0)\n"
    )

    targets = extract_run_targets(notebook, "/Jobs/main")

    assert "/Jobs/nb_a" in targets
    assert "/Jobs/nb_b" in targets
    assert all(not target.endswith(suffix) for target in targets
               for suffix in ("/3600", "/job", "/team@example.com"))


def test_modulo_continuation_is_not_removed_as_notebook_magic():
    notebook = _notebook(
        "prefix = dbutils.widgets.get('prefix')\n"
        "path = (\n"
        "    '%s/child_fmt'\n"
        "    % prefix\n"
        ")\n"
        "dbutils.notebook.run(path, 0)\n"
    )

    assert extract_run_targets(notebook, "/Jobs/main") == []


def test_percent_run_arguments_are_not_part_of_the_target_path():
    notebook = _notebook('%run ./child $as_of="2026-09-15"\n')

    assert extract_run_targets(notebook, "/Jobs/main") == ["/Jobs/child"]


def test_resolver_skips_confirmed_mlflow_object_instead_of_exporting_it():
    master = _notebook_bytes(
        'candidate = "/Users/u/MLflow/EX_PAYLOAD_RUN_JOBS"\n'
        'dbutils.notebook.run(runtime_selected_path, 3600)\n'
    )

    class FakeDbx:
        @staticmethod
        def parse_github_owner_repo(_url):
            return None, None

        def get_job(self, _job_id):
            return {"settings": {"tasks": [{
                "task_key": "main",
                "notebook_task": {"notebook_path": "/Workspace/Jobs/main"},
            }]}}

        def export_notebook(self, path):
            if path == "/Workspace/Jobs/main":
                return ExportedFile(master, True, ".ipynb")
            raise AssertionError(f"non-notebook asset was exported: {path}")

        def get_workspace_status(self, path):
            if path == "/Users/u/MLflow/EX_PAYLOAD_RUN_JOBS":
                return {"object_type": "MLFLOW_EXPERIMENT"}
            return {"object_type": "NOTEBOOK"}

        def list_workspace_folder(self, _path):
            return []

    graph = DependencyResolver(FakeDbx(), log=lambda _message: None).resolve("1")

    assert "/Users/u/MLflow/EX_PAYLOAD_RUN_JOBS" not in graph.notebooks
    assert (
        "workspace_asset:MLFLOW_EXPERIMENT:/Users/u/MLflow/EX_PAYLOAD_RUN_JOBS"
        in graph.external_refs
    )


def test_workspace_status_retries_http_429(monkeypatch):
    class Response:
        def __init__(self, status_code, body):
            self.status_code = status_code
            self.text = json.dumps(body)
            self._body = body

        def json(self):
            return self._body

    responses = iter((
        Response(429, {"message": "rate limited"}),
        Response(200, {"object_type": "NOTEBOOK"}),
    ))
    monkeypatch.setattr(
        "dbx_export.dbx_client.requests.get", lambda *args, **kwargs: next(responses)
    )
    monkeypatch.setattr("dbx_export.dbx_client.time.sleep", lambda _seconds: None)

    status = DbxClient(DbxConfig(host="https://example", token="x")).get_workspace_status(
        "/Workspace/Jobs/main"
    )

    assert status == {"object_type": "NOTEBOOK"}

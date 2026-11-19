from __future__ import annotations

import pickle
import sys
import types

import pytest

from dbx_export.dbx_client import DbxClient, DbxConfig
from scripts.pandas_compat_rewriter import rewrite_removed_dataframe_append
from scripts.ray_aidp_rewriter import (
    MARKER,
    UnsupportedRayPattern,
    rewrite_ray_on_databricks_notebook,
)


def _cell(source: str, cell_type: str = "code") -> dict:
    return {
        "cell_type": cell_type,
        "source": source.splitlines(keepends=True),
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }


def _ray_cells() -> list[dict]:
    return [
        _cell("import ray\nray.init(address='auto')\n"),
        _cell("def execute_func(*args, **kwargs):\n    return ray.get([])\n"),
        _cell("result = execute_func(path, name, param_list=params)\n"),
    ]


def test_resource_rewrite_requires_an_actually_staged_resource():
    from scripts import job_migrate as jm

    source = (
        'spark.read.csv("dbfs:/FileStore/input-data.csv")\n'
        'pd.read_csv("input-data.csv")\n'
        "from _pickle import loads\n"
        "import my_pkg._internal\n"
    )
    assert jm.rewrite_aidp_resource_references(source, set()) == source

    rewritten = jm.rewrite_aidp_resource_references(
        source,
        {
            "GIT/repo/input_data.csv",
            "GIT/repo/my_pkg/internal.py",
        },
    )
    assert 'dbfs:/FileStore/input-data.csv' in rewritten
    assert 'pd.read_csv("input_data.csv")' in rewritten
    assert "from _pickle import loads" in rewritten
    assert "import my_pkg.internal" in rewritten


def test_resource_stage_emits_machine_readable_manifest_marker():
    from scripts import job_migrate as jm

    code = jm.build_export_resource_stage_code("export/root", "/Workspace/migrated")
    assert jm._RESOURCE_STAGE_MARKER in code
    assert "_json_res.dumps(_copied_res)" in code
    assert "[resource-stage] WARN failures=" in code
    assert "raise RuntimeError('[resource-stage] failures='" not in code


@pytest.mark.parametrize(
    "segment",
    [
        "foo:bar+baz.yaml",
        "__leading.yaml",
        "trailing__",
        "a -():+ b.properties",
        "Workspace",
    ],
)
def test_migration_resource_sanitizer_matches_exporter(segment):
    from dbx_export.sanitizer import sanitize_segment
    from scripts import job_migrate as jm

    assert jm._sanitize_aidp_resource_segment(segment) == sanitize_segment(segment)


def test_colon_plus_resource_reference_resolves_to_exported_name():
    from scripts import job_migrate as jm

    source = 'config_path = "foo:bar+baz.yaml"\n'
    rewritten = jm.rewrite_aidp_resource_references(
        source,
        {"GIT/repo/foo_bar_baz.yaml"},
    )
    assert rewritten == 'config_path = "foo_bar_baz.yaml"\n'


def test_preexisting_aidp_display_alias_call_gets_required_import():
    from scripts import job_migrate as jm

    source = "value = get_unknown_object()\n_aidp_ipython_display(value)\n"
    rewritten = jm._rewrite_pandas_display_calls(source)
    assert rewritten.count(jm._PANDAS_DISPLAY_IMPORT.strip()) == 1
    assert "_aidp_ipython_display(value)" in rewritten
    assert jm._rewrite_pandas_display_calls(rewritten) == rewritten


def test_display_alias_text_in_comments_and_strings_does_not_add_import():
    from scripts import job_migrate as jm

    source = (
        "message = '_aidp_ipython_display(value)'\n"
        "# _aidp_ipython_display(value)\n"
    )
    assert jm._rewrite_pandas_display_calls(source) == source


def test_workspace_bootstrap_changes_cwd_only_for_repository_notebooks(monkeypatch):
    from scripts import job_migrate as jm

    monkeypatch.setattr(jm, "MIGRATED_BASE", "/Workspace/migrated/notebooks")
    monkeypatch.setattr(jm, "EXPORT_BASE", "export/root")
    repo_cell = jm.build_artifact_workspace_bootstrap_cell("GIT/repo/task.ipynb")
    external_cell = jm.build_artifact_workspace_bootstrap_cell(
        "Users/person@example.com/shared.ipynb"
    )
    repo_source = "".join(repo_cell["source"])
    external_source = "".join(external_cell["source"])
    assert "_os_workspace.chdir(_aidp_notebook_dir)" in repo_source
    assert "migrated-workspace context - generated" in repo_source
    assert "migrated-workspace context ? generated" not in repo_source
    assert "External dependency: preserve the caller repository's cwd" in external_source
    assert "_os_workspace.chdir(_aidp_notebook_dir)" not in external_source


def test_nonempty_migrated_cell_cannot_be_deleted():
    from scripts import job_migrate as jm

    assert jm.preserve_nonempty_migrated_cell("answer = 42\n", "") == "answer = 42\n"
    assert jm.preserve_nonempty_migrated_cell("answer = 42\n", "value = 7\n") == "value = 7\n"


def test_code_only_workflow_promotion_status(monkeypatch):
    from scripts import job_migrate as jm

    monkeypatch.setattr(jm, "CODE_ONLY", True)
    assert jm.workflow_promotion_status()["status"] == "DEFERRED_CODE_ONLY"
    monkeypatch.setattr(jm, "CODE_ONLY", False)
    assert jm.workflow_promotion_status()["status"] == "ELIGIBLE_AFTER_VALIDATION"


def test_dataframe_append_rewriter_preserves_list_accumulator():
    source = "frames = [df.drop('a', axis=1)]\nframes.append(df2)\n"
    rewritten, report = rewrite_removed_dataframe_append(source)
    assert rewritten == source
    assert report.replacements == 0


def test_dataframe_append_rewriter_is_idempotent_after_model_repair():
    source = "left = pd.DataFrame(values)\nout = left.append(right)\n"
    once, report = rewrite_removed_dataframe_append(source)
    twice, second_report = rewrite_removed_dataframe_append(once)
    assert report.replacements == 1
    assert second_report.replacements == 0
    assert twice == once


def test_display_routing_uses_last_assignment_before_call():
    from scripts import job_migrate as jm

    source = (
        "import pandas as pd\n"
        "df = pd.read_csv('input.csv')\n"
        "df = spark.createDataFrame(df)\n"
        "display(df)\n"
    )
    assert jm._rewrite_pandas_display_calls(source) == source


def test_ray_actor_decorator_with_arguments_is_rejected():
    cells = _ray_cells()
    cells.insert(2, _cell("@ray.remote(num_gpus=1)\nclass Trainer:\n    pass\n"))
    with pytest.raises(UnsupportedRayPattern, match="actor class Trainer"):
        rewrite_ray_on_databricks_notebook(cells)


def test_ray_remote_class_wrapper_is_rejected():
    cells = _ray_cells()
    cells.insert(2, _cell("class Trainer:\n    pass\nTrainer = ray.remote(Trainer)\n"))
    with pytest.raises(UnsupportedRayPattern, match=r"ray\.remote\(Trainer\)"):
        rewrite_ray_on_databricks_notebook(cells)


def test_markdown_ray_text_does_not_reject_conversion():
    cells = _ray_cells()
    cells.insert(0, _cell("We deliberately avoid ray.tune here", "markdown"))
    report = rewrite_ray_on_databricks_notebook(cells)
    assert report.converted is True


def test_runner_is_injected_once_for_multiple_execute_cells():
    cells = _ray_cells()
    cells.append(_cell("other = execute_func(path, name, param_list=other_params)\n"))
    report = rewrite_ray_on_databricks_notebook(cells)
    combined = "\n".join("".join(cell["source"]) for cell in cells)
    assert report.execute_calls_rewritten == 2
    assert combined.count(MARKER) == 1
    assert 'if _candidate_count > 0:' in combined


def test_ray_runner_executes_full_search_and_restores_order(
    monkeypatch, tmp_path
):
    """Execute the generated runner with shuffled Spark partition results.

    This protects the behavior rather than merely pinning implementation text:
    every input must run exactly once, results must return in input order, and
    an unset fixed executor count (``0``) must fall back to dynamic allocation.
    """
    from scripts import ray_aidp_rewriter as ray_rewriter

    params = [{"index": index} for index in range(617)]
    payload_path = tmp_path / "ray-input.pickle"
    with payload_path.open("wb") as handle:
        pickle.dump(
            {
                "common_objects": {},
                "input_variables": {"value": "kept"},
                "param_list": params,
            },
            handle,
        )

    cloudpickle_stub = types.ModuleType("cloudpickle")
    cloudpickle_stub.dumps = lambda value: value
    cloudpickle_stub.loads = lambda value: value
    monkeypatch.setitem(sys.modules, "cloudpickle", cloudpickle_stub)

    class RemoteFunction:
        def __init__(self, implementation):
            self.implementation = implementation

        def options(self, **_kwargs):
            return self

        def remote(self, **kwargs):
            return self.implementation(**kwargs)

    ray_stub = types.ModuleType("ray")
    ray_stub.shutdown = lambda: None
    ray_stub.init = lambda **_kwargs: None
    ray_stub.put = lambda value: value
    ray_stub.get = lambda values: list(values)
    ray_stub.remote = lambda **_kwargs: (
        lambda implementation: RemoteFunction(implementation)
    )
    monkeypatch.setitem(sys.modules, "ray", ray_stub)

    class Resource:
        addresses = ["0"]

    class TaskContext:
        @staticmethod
        def get():
            return TaskContext()

        def resources(self):
            return {"gpu": Resource()}

    pyspark_stub = types.ModuleType("pyspark")
    pyspark_stub.TaskContext = TaskContext
    monkeypatch.setitem(sys.modules, "pyspark", pyspark_stub)

    class Broadcast:
        def __init__(self, value):
            self.value = value

    class Collected:
        def __init__(self, values):
            self.values = values

        def collect(self):
            return self.values

    class Rdd:
        def __init__(self, values, partitions):
            self.values = values
            self.partitions = partitions

        def mapPartitions(self, function):
            buckets = [self.values[index::self.partitions]
                       for index in range(self.partitions)]
            # Spark does not promise collect order across independently
            # completed partitions; deliberately reverse it here.
            values = []
            for bucket in reversed(buckets):
                values.extend(function(iter(bucket)))
            return Collected(values)

    class Conf:
        values = {
            "spark.executor.instances": "0",
            "spark.dynamicAllocation.maxExecutors": "4",
        }

        def get(self, key, default=None):
            return self.values.get(key, default)

    class SparkContext:
        def __init__(self):
            self.partition_counts = []

        def getConf(self):
            return Conf()

        def broadcast(self, value):
            return Broadcast(value)

        def parallelize(self, values, partitions):
            self.partition_counts.append(partitions)
            return Rdd(values, partitions)

    spark_context = SparkContext()
    spark = types.SimpleNamespace(sparkContext=spark_context)

    def implementation(
        input_variables_dict,
        common_object_id_dict,
        params,
        store_model_path=None,
    ):
        del common_object_id_dict, store_model_path
        assert input_variables_dict == {"value": "kept"}
        return params["index"]

    function_wrapper = types.SimpleNamespace(_function=implementation)
    namespace = {"spark": spark, "test_function": function_wrapper}
    exec(ray_rewriter._RUNNER, namespace)
    result = namespace["_aidp_execute_func_spark_gpu"](
        str(payload_path),
        "test_function",
        num_gpus=0.2,
        num_cpus=1,
    )

    assert result == list(range(617))
    assert len(result) == 617
    assert spark_context.partition_counts == [8, 8, 8]


def test_same_stem_github_entries_are_all_retained(monkeypatch):
    client = DbxClient(DbxConfig(proxy="example.invalid"))
    monkeypatch.setattr(
        client,
        "list_github_dir_entries",
        lambda *args: [
            {"name": "database.py", "type": "file"},
            {"name": "database.yaml", "type": "file"},
        ],
    )
    lookup = client.list_github_dir("owner", "repo", "configs", "main")
    assert {"database.py", "database.yaml"}.issubset(set(lookup.values()))
    assert lookup["database"] == "database.py"


def test_git_resource_scan_recurses_into_dotted_directory():
    from dbx_export.dependency_resolver import DependencyResolver, NotebookNode

    class FakeDbx:
        def list_github_dir_entries(self, owner, repo, path, branch):
            if path == "configs":
                return [{"name": "v1.0", "type": "dir"}]
            if path == "configs/v1.0":
                return [{"name": "database.yaml", "type": "file"}]
            return []

    resolver = DependencyResolver(FakeDbx(), log=lambda *_: None)
    node = NotebookNode(
        dbx_path="repo/notebook",
        role="task",
        git_owner="owner",
        git_repo="repo",
        git_branch="main",
    )
    assert resolver._list_resource_tree(node, "configs") == [
        "configs/v1.0/database.yaml"
    ]


def test_pip_parser_removes_balanced_quotes_and_tolerates_unbalanced_quotes():
    from dbx_export.__main__ import _parse_pip_install_line

    assert _parse_pip_install_line("'urllib3<2' requests") == ["urllib3<2", "requests"]
    # The fallback must not raise just because a notebook contains malformed
    # shell text; it preserves the old best-effort behavior.
    assert isinstance(_parse_pip_install_line("'urllib3<2 requests"), list)


def test_pip_parser_tolerates_apostrophe_in_trailing_comment():
    from dbx_export.__main__ import _parse_pip_install_line

    assert _parse_pip_install_line("requests  # don't upgrade") == ["requests"]


def test_aidp_name_sanitizers_replace_colon_and_plus():
    from dbx_export.sanitizer import sanitize_segment, sanitize_task_key

    assert sanitize_segment("feature:daily+retry") == "feature_daily_retry"
    assert sanitize_task_key("feature:daily+retry") == "feature_daily_retry"

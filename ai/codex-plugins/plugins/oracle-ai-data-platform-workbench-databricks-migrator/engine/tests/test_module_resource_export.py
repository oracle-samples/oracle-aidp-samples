"""Resource folders named only inside imported .py modules must be exported.

Incident (DataBricks_Access_Control, 2026-10): the notebook reads
``pd.read_csv(ACCESS_CONTROL_CSV_RELATVE_PATH)`` where the constant
``"database/access_control.csv"`` lives in ``imports/constants.py``. The
exporter copied ``imports/`` (an import hint) but never scanned the module,
so ``database/`` was left behind and the migrated notebook could not find
its input CSV on AIDP.
"""

import json

import pytest

from dbx_export.__main__ import phase2_upload_notebooks
from dbx_export.dbx_client import ExportedFile
from dbx_export.dependency_resolver import DependencyResolver
from dbx_export.report import Report


def _notebook(source):
    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [{
            "cell_type": "code",
            "source": source.splitlines(keepends=True),
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        }],
    }


ACCESS_CONTROL_SOURCE = (
    "from imports.constants import *\n"
    "relative_file_path = ACCESS_CONTROL_CSV_RELATVE_PATH\n"
    "access_control_csv = pd.read_csv(relative_file_path)\n"
)

CONSTANTS_SOURCE = (
    b'ACCESS_CONTROL_CSV_RELATVE_PATH = "database/access_control.csv"\n'
    b'JOB_MANAGE_LEVEL_PERMISSION_CONFIG_PATH = "database/job_manage_permissions.yaml"\n'
)


class FakeRepo:
    """One fake for both Git-backed and workspace-backed jobs.

    ``files`` maps repository-relative paths (no leading slash) to bytes and
    ``notebooks`` maps notebook paths (no extension) to source. Workspace
    jobs live under ``/Jobs``; Git jobs use repository-relative paths.
    """

    def __init__(self, *, git, notebooks, files):
        self.git = git
        self.root = "" if git else "/Jobs"
        self.notebook_sources = notebooks
        self.files = files
        self.exported = []
        self.listed = []

    def _full(self, relative):
        return relative if self.git else f"{self.root}/{relative}"

    def _relative(self, path):
        path = path.lstrip("/") if self.git else path
        if not self.git:
            path = path[len(self.root):].lstrip("/") if path.startswith(self.root) else path
        return path.strip("/")

    def _children(self, directory):
        prefix = f"{directory}/" if directory else ""
        names = {}
        for path in list(self.files) + [f"{nb}.ipynb" for nb in self.notebook_sources]:
            if not path.startswith(prefix):
                continue
            head, _, rest = path[len(prefix):].partition("/")
            names[head] = "dir" if rest else "file"
        return names

    @staticmethod
    def parse_github_owner_repo(_url):
        return "owner", "repo"

    def get_job(self, _job_id):
        tasks = []
        for index, notebook in enumerate(self.notebook_sources):
            task = {"task_key": f"t{index}", "notebook_task": {
                "notebook_path": self._full(notebook)}}
            if self.git:
                task["notebook_task"]["source"] = "GIT"
            tasks.append(task)
        settings = {"tasks": tasks}
        if self.git:
            settings["git_source"] = {
                "git_url": "https://github.com/owner/repo.git",
                "git_branch": "main",
            }
        return {"settings": settings}

    def export_notebook_from_git(self, path, *_context):
        source = self.notebook_sources[path]
        return ExportedFile(json.dumps(_notebook(source)).encode(), True, ".ipynb")

    def export_notebook(self, path):
        source = self.notebook_sources[self._relative(path)]
        return ExportedFile(json.dumps(_notebook(source)).encode(), True, ".ipynb")

    def export_resource_from_git(self, path, *_context):
        self.exported.append(path)
        return ExportedFile(self.files[path], False, "." + path.rsplit(".", 1)[1])

    def export_workspace_resource(self, path):
        relative = self._relative(path)
        self.exported.append(relative)
        return ExportedFile(self.files[relative], False, "." + path.rsplit(".", 1)[1])

    # Git listing API
    def list_github_dir(self, _owner, _repo, directory, _branch):
        self.listed.append(directory)
        return {name: name for name in self._children(directory.strip("/"))}

    def list_github_dir_entries(self, _owner, _repo, directory, _branch):
        return [{"name": name, "type": kind}
                for name, kind in sorted(self._children(directory.strip("/")).items())]

    # Workspace listing API
    def list_workspace_folder(self, path):
        self.listed.append(path)
        directory = self._relative(path)
        entries = []
        for name, kind in sorted(self._children(directory).items()):
            child = self._full(f"{directory}/{name}" if directory else name)
            if kind == "dir":
                entries.append({"path": child, "object_type": "DIRECTORY"})
            elif name.endswith(".ipynb"):
                entries.append({"path": child[:-len(".ipynb")], "object_type": "NOTEBOOK"})
            else:
                entries.append({"path": child, "object_type": "FILE"})
        return entries

    def get_workspace_status(self, _path):
        return {"object_type": "NOTEBOOK"}


def _resolve(repo):
    logs = []
    graph = DependencyResolver(repo, log=logs.append).resolve("1")
    return graph, logs


def _resources(graph, repo):
    return {
        repo._relative(path): node
        for path, node in graph.notebooks.items() if node.role == "resource"
    }


@pytest.mark.parametrize("git", [True, False])
def test_resource_folder_named_only_in_imported_constants_is_exported(git):
    repo = FakeRepo(git=git, notebooks={"DataBricks_Access_Control": ACCESS_CONTROL_SOURCE}, files={
        "imports/constants.py": CONSTANTS_SOURCE,
        "imports/imports.py": b"import pandas as pd\n",
        "database/access_control.csv": b"job_id\n1\n",
        "database/job_manage_permissions.yaml": b"1: []\n",
    })

    graph, logs = _resolve(repo)

    resources = _resources(graph, repo)
    assert set(resources) == {
        "imports/constants.py", "imports/imports.py",
        "database/access_control.csv", "database/job_manage_permissions.yaml",
    }
    notebook_path = repo._full("DataBricks_Access_Control")
    # The importing notebook, not the module, owns the resource edge: the
    # relative path resolves against the notebook's working directory.
    assert resources["database/access_control.csv"].referenced_by == [
        f"resource-of:{notebook_path}"]
    assert resources["database/access_control.csv"].content == b"job_id\n1\n"
    assert any("[resource-module]" in line and "database" in line for line in logs)
    assert not any(node.resource_discovery_failures for node in graph.notebooks.values())


def test_module_discovered_resources_are_uploaded_next_to_the_notebook():
    repo = FakeRepo(git=True, notebooks={"DataBricks_Access_Control": ACCESS_CONTROL_SOURCE}, files={
        "imports/constants.py": CONSTANTS_SOURCE,
        "database/access_control.csv": b"job_id\n1\n",
        "database/job_manage_permissions.yaml": b"1: []\n",
    })
    graph, _ = _resolve(repo)

    class FakeAidp:
        def __init__(self):
            self.notebooks = {}
            self.files = {}

        def get_notebook(self, path):
            return self.notebooks.get(path)

        def put_notebook(self, path, value):
            self.notebooks[path] = value

        def object_exists(self, path):
            return path in self.files

        def upload_file(self, path, content, *, overwrite):
            assert not overwrite
            self.files[path] = content

        def download_file(self, path):
            return self.files[path]

    aidp = FakeAidp()
    report = Report(run_id="test", dbx_root_job_id="1", export_output="/Workspace/export", prefix="")
    _, failed_tasks = phase2_upload_notebooks(
        aidp, graph, "/Workspace/export", False, "test", "1", report,
    )

    assert failed_tasks == []
    notebook_path = next(iter(aidp.notebooks))
    assert notebook_path.endswith("/GIT/DataBricks_Access_Control.ipynb")
    root = notebook_path.removesuffix("DataBricks_Access_Control.ipynb")
    assert {path.removeprefix(root) for path in aidp.files} == {
        "imports/constants.py",
        "database/access_control.csv",
        "database/job_manage_permissions.yaml",
    }
    assert not report.errors


@pytest.mark.parametrize("git", [True, False])
def test_module_chain_is_followed_back_to_the_importing_notebook(git):
    repo = FakeRepo(git=git, notebooks={"jobs/main": "from helpers.util import load\n"}, files={
        "helpers/util.py": b"from settings.paths import *\n",
        "settings/paths.py": b'APP_CONFIG = "config/app.yaml"\n',
        "config/app.yaml": b"name: app\n",
    })

    graph, _ = _resolve(repo)

    resources = _resources(graph, repo)
    assert set(resources) == {"helpers/util.py", "settings/paths.py", "config/app.yaml"}
    assert resources["config/app.yaml"].referenced_by == [
        f"resource-of:{repo._full('jobs/main')}"]


@pytest.mark.parametrize("git", [True, False])
def test_each_importing_notebook_resolves_its_own_nearest_folder(git):
    # Two notebooks share one module; each runs with its own folder as the
    # working directory, so each gets the nearest ``data/`` to itself.
    repo = FakeRepo(git=git, notebooks={
        "a/first": "from common.constants import *\n",
        "b/second": "from common.constants import *\n",
    }, files={
        "common/constants.py": b'INPUT = "data/input.csv"\n',
        "a/data/input.csv": b"a\n",
        "b/data/input.csv": b"b\n",
        "data/input.csv": b"root\n",
    })

    graph, _ = _resolve(repo)

    resources = _resources(graph, repo)
    assert set(resources) == {"common/constants.py", "a/data/input.csv", "b/data/input.csv"}
    assert resources["a/data/input.csv"].referenced_by == [f"resource-of:{repo._full('a/first')}"]
    assert resources["b/data/input.csv"].referenced_by == [f"resource-of:{repo._full('b/second')}"]


@pytest.mark.parametrize("content, reason", [
    (b"\xff\xfe\x00 not utf-8", "MODULE_DECODE_FAILED"),
    (b"def broken(:\n", "MODULE_PARSE_FAILED"),
])
def test_unreadable_module_is_reported_not_silently_skipped(content, reason):
    repo = FakeRepo(git=True, notebooks={"main": "from imports.constants import *\n"}, files={
        "imports/constants.py": content,
        "database/access_control.csv": b"job_id\n",
    })

    graph, _ = _resolve(repo)

    assert "database/access_control.csv" not in _resources(graph, repo)
    assert graph.notebooks["main"].resource_discovery_failures == [{
        "notebook": "main", "directory": "imports/constants.py", "reason": reason}]


def test_module_hints_do_not_change_exports_when_nothing_matches():
    repo = FakeRepo(git=True, notebooks={"main": "from imports.constants import *\n"}, files={
        "imports/constants.py": b'import os\nOUTPUT = "results/out.csv"\nURL = "https://x/y.json"\n',
        "unrelated/data.csv": b"x\n",
    })

    graph, _ = _resolve(repo)

    assert set(_resources(graph, repo)) == {"imports/constants.py"}


def test_module_hints_never_promote_notebooks_to_dependencies():
    repo = FakeRepo(git=True, notebooks={"main": "from imports.constants import *\n"}, files={
        "imports/constants.py": b'REPORT = "reports/template.yaml"\n',
        "reports/template.yaml": b"a: 1\n",
        "reports/draft.ipynb": b"{}",
    })

    graph, _ = _resolve(repo)

    assert set(_resources(graph, repo)) == {"imports/constants.py", "reports/template.yaml"}
    assert "reports/draft" not in graph.notebooks


@pytest.mark.parametrize("git", [True, False])
def test_module_repeating_notebook_hints_does_not_relist_directories(git):
    def run(util_source):
        repo = FakeRepo(git=git, notebooks={"main": (
            "from imports.constants import *\n"
            "from helpers.util import *\n"
        )}, files={
            "imports/constants.py": b"LIMIT = 10\n",
            "helpers/util.py": util_source,
        })
        graph, logs = _resolve(repo)
        return repo, graph, logs

    baseline, _, _ = run(b"VALUE = 1\n")
    repo, graph, logs = run(b"from imports.constants import *\nfrom helpers import util\n")

    assert set(_resources(graph, repo)) == {"imports/constants.py", "helpers/util.py"}
    # Both hints were already searched from the notebook's folders, so the
    # module adds no directory listing (GitHub proxy calls are rate limited).
    assert repo.listed == baseline.listed
    assert not any("[resource-module]" in line for line in logs)


# REF-239: bootstrap cell 2 in run ccccf37f (2026-10-19) passed the
# repository config directory to load_config; no YAML resource was exported.
CONFIG_BOOTSTRAP = (
    'DEFAULT_REPO_ROOT = "/Workspace/Users/fixture/repo"\n'
    '_raw_repo = (dbutils.widgets.get("repo_root") or "").strip() or DEFAULT_REPO_ROOT\n'
    'if _raw_repo.startswith("/Users/") and not _raw_repo.startswith("/Workspace/"):\n'
    '    _raw_repo = f"/Workspace{_raw_repo}"\n'
    'REPO_ROOT = _raw_repo\n'
    'cfg = load_config(f"{REPO_ROOT}/config")\n'
)


class ConfigRepo(FakeRepo):
    def __init__(self, *, source=CONFIG_BOOTSTRAP, binding="/Workspace/Users/fixture/repo",
                 binding_source="task", git=False, files=None):
        super().__init__(git=git, notebooks={"notebooks/bootstrap": source}, files={
            "config/variables.yaml": b"variables: {}\n",
            "config/nested/services.yml": b"services: {}\n",
            "config/notes.txt": b"config companion\n",
            "config/unrelated.ipynb": b"not a dependency\n",
            "other/config/variables.yaml": b"wrong directory\n",
        } if files is None else files)
        if not git:
            self.root = "/Users/fixture/repo"
        self.binding = binding
        self.binding_source = binding_source

    def get_job(self, job_id):
        job = super().get_job(job_id)
        if self.binding is not None:
            if self.binding_source == "job":
                job["settings"]["parameters"] = [{"name": "repo_root", "default": self.binding}]
            else:
                job["settings"]["tasks"][0]["notebook_task"]["base_parameters"] = {
                    "repo_root": self.binding}
        return job


@pytest.mark.parametrize("binding_source", ["task", "job", "exporter"])
@pytest.mark.parametrize("root", ["/Workspace/Users/fixture/repo", "/Users/fixture/repo", ""])
def test_load_config_directory_exports_exact_parameter_root(binding_source, root):
    repo = ConfigRepo(binding=root, binding_source=binding_source)
    overrides = {"repo_root": root} if binding_source == "exporter" else {}
    # A different task value proves explicit exporter overrides win.
    if binding_source == "exporter":
        repo.binding = "/Workspace/other"
    logs = []
    graph = DependencyResolver(repo, log=logs.append, parameter_bindings=overrides).resolve("1")
    assert set(_resources(graph, repo)) == {
        "config/variables.yaml", "config/nested/services.yml", "config/notes.txt"}
    assert not graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures
    assert any("[resource-config]" in message for message in logs)
    assert "/Users/fixture/repo/config" in repo.listed
    assert "/" not in repo.listed


@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("call", [
    'load_config("../config")',
    'load_config(config_dir="../config")',
    'cfg_module.load_config("../config")',
    'read_config("../config")',
])
def test_load_config_relative_directory_spelling_at_orchestration_level(git, call):
    source = 'from src.config import load_config as read_config\n' + call
    repo = ConfigRepo(source=source, binding=None, git=git)
    graph, _ = _resolve(repo)
    assert {path for path in _resources(graph, repo) if path.startswith("config/")} == {
        "config/variables.yaml", "config/nested/services.yml", "config/notes.txt"}
    assert not any(path.endswith("unrelated.ipynb") for path in graph.notebooks)


@pytest.mark.parametrize("source,reason", [
    ('load_config(f"{unknown_root}/config")', "CONFIG_DIRECTORY_UNRESOLVED"),
    ('load_config()', "CONFIG_DIRECTORY_UNRESOLVED"),
    ('load_config(config_dir=None)', "CONFIG_DIRECTORY_UNRESOLVED"),
    ('load_config("oci://bucket/config")', "CONFIG_DIRECTORY_UNSUPPORTED"),
    ('load_config("/tmp/config")', "CONFIG_DIRECTORY_UNSUPPORTED"),
    ('load_config("/Workspace/Users")', "CONFIG_DIRECTORY_UNSUPPORTED"),
    ('load_config("../absent")', "CONFIG_DIRECTORY_EMPTY"),
])
def test_load_config_unresolved_or_missing_directories_are_reported(source, reason):
    repo = ConfigRepo(source=source, binding=None)
    graph, logs = _resolve(repo)
    node = graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"]
    assert any(failure["reason"] == reason for failure in node.resource_discovery_failures)
    assert not _resources(graph, repo)
    assert any("[resource-config]" in message for message in logs)


def test_load_config_directory_listing_failure_is_reported():
    class BrokenRepo(ConfigRepo):
        def list_workspace_folder(self, path):
            if path == "/Users/fixture/repo/config":
                raise RuntimeError("directory access denied")
            return super().list_workspace_folder(path)

    graph, logs = _resolve(BrokenRepo())
    failures = graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures
    assert {failure["reason"] for failure in failures} == {"WORKSPACE_DIRECTORY_LIST_FAILED"}
    assert not _resources(graph, BrokenRepo())
    assert any("directory access denied" in message for message in logs)


def test_load_config_unknown_widget_does_not_select_default_root():
    repo = ConfigRepo(binding=None)
    graph, _ = _resolve(repo)
    assert not _resources(graph, repo)
    assert graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures[0]["reason"] == (
        "CONFIG_DIRECTORY_UNRESOLVED")


def test_commented_load_config_does_not_export_directory():
    repo = ConfigRepo(source='# load_config("../config")', binding=None)
    graph, _ = _resolve(repo)
    assert not _resources(graph, repo)


def test_load_config_directory_upload_uses_existing_resource_report():
    repo = ConfigRepo()
    graph, _ = _resolve(repo)

    class Aidp:
        def __init__(self):
            self.files = {}

        def get_notebook(self, _path):
            return None

        def put_notebook(self, _path, _notebook):
            pass

        def object_exists(self, path):
            return path in self.files

        def upload_file(self, path, content, *, overwrite):
            assert not overwrite
            self.files[path] = content

        def download_file(self, path):
            return self.files[path]

    # This regression concerns the resource upload/report contract, not notebook saves.
    graph.notebooks = {path: node for path, node in graph.notebooks.items() if node.role == "resource"}
    aidp = Aidp()
    report = Report(run_id="test", dbx_root_job_id="1", export_output="/Workspace/export", prefix="")
    targets, failures = phase2_upload_notebooks(
        aidp, graph, "/Workspace/export", False, "test", "1", report)
    assert not failures and not report.errors
    assert set(targets) == set(graph.notebooks)
    assert len(report.notebooks) == 3
    assert all(result.role == "resource" and result.status == "VALIDATED" for result in report.notebooks)
    assert all(result.dbx_fingerprint == result.aidp_fingerprint for result in report.notebooks)
    assert aidp.files["/Workspace/export/notebooks/Users/fixture/repo/config/variables.yaml"] == b"variables: {}\n"


@pytest.mark.parametrize("binding", ["/Users/fixture/repo", "/Workspace/Users/fixture/repo"])
def test_load_config_customer_workspace_prefix_normalization(binding):
    repo = ConfigRepo(binding=binding)
    graph, _ = _resolve(repo)
    assert set(_resources(graph, repo)) == {
        "config/variables.yaml", "config/nested/services.yml", "config/notes.txt"}
    assert "/Users/fixture/repo/config" in repo.listed
    assert all("/Workspace/Workspace/" not in path for path in repo.listed)


def test_load_config_calls_use_the_root_at_each_call():
    repo = ConfigRepo(source=(
        'ROOT = "../config"\nload_config(ROOT)\n'
        'ROOT = "../other/config"\nload_config(ROOT)\n'), binding=None)
    graph, _ = _resolve(repo)
    assert set(_resources(graph, repo)) == {
        "config/variables.yaml", "config/nested/services.yml", "config/notes.txt",
        "other/config/variables.yaml"}


def test_load_config_unknown_normalization_branch_is_unresolved():
    repo = ConfigRepo(source=(
        'ROOT = "../config"\n'
        'if runtime_condition:\n    ROOT = "../other/config"\n'
        'load_config(ROOT)\n'), binding=None)
    graph, _ = _resolve(repo)
    assert not _resources(graph, repo)
    assert graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures


def test_load_config_does_not_escape_workspace_roots_through_dotdot():
    repo = ConfigRepo(source='load_config("/Users/../../tmp/config")', binding=None)
    graph, _ = _resolve(repo)
    assert not _resources(graph, repo)
    assert "/tmp/config" not in repo.listed
    assert graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures[0]["reason"] == (
        "CONFIG_DIRECTORY_UNSUPPORTED")


@pytest.mark.parametrize("binding", [None, "/Workspace/Users/fixture/repo"])
def test_load_config_uses_declared_widget_default_without_overriding_parameter(binding):
    default = "/Workspace/Users/fixture/repo" if binding is None else "/Workspace/Users/unused/repo"
    source = (
        f'DEFAULT = {default!r}\n'
        'dbutils.widgets.removeAll()\n'
        'dbutils.widgets.text("repo_root", DEFAULT)\n'
        'ROOT = (dbutils.widgets.get("repo_root") or "").strip() or DEFAULT\n'
        'load_config(f"{ROOT}/config")\n')
    repo = ConfigRepo(source=source, binding=binding)
    graph, _ = _resolve(repo)
    assert set(_resources(graph, repo)) == {
        "config/variables.yaml", "config/nested/services.yml", "config/notes.txt"}
    assert not graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures


def test_load_config_widget_reset_discards_previous_declared_default():
    source = (
        'dbutils.widgets.text("repo_root", "/Workspace/Users/unused/repo")\n'
        'dbutils.widgets.removeAll()\n'
        'dbutils.widgets.text("repo_root", "/Workspace/Users/fixture/repo")\n'
        'ROOT = dbutils.widgets.get("repo_root")\n'
        'load_config(f"{ROOT}/config")\n')
    graph, _ = _resolve(ConfigRepo(source=source, binding=None))
    assert not graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures
    assert "/Users/fixture/repo/config/variables.yaml" in graph.notebooks


@pytest.mark.parametrize("expression", [
    'dbutils.widgets.get("root") or "/Workspace/Users/fallback/repo"',
    'not ROOT.startswith("/Workspace/")',
    'ROOT.startswith("/Users/")',
])
def test_config_root_operators_do_not_change_notebook_run_evaluation(expression):
    import ast
    from dbx_export.dependency_resolver import _static_values

    env = {"ROOT": ["/Users/fixture/repo"]}
    bindings = {"root": "/Workspace/Users/fixture/repo"}
    node = ast.parse(expression, mode="eval").body
    assert _static_values(node, env, bindings) == []
    assert _static_values(node, env, bindings, config_paths=True)



def test_load_config_annotation_without_value_preserves_existing_root():
    source = ('ROOT = "../config"\nROOT: str\n'
              'unassigned: str\nload_config(ROOT)\n')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert set(_resources(graph, repo)) == {
        "config/variables.yaml", "config/nested/services.yml", "config/notes.txt"}
    assert not graph.notebooks["/Users/fixture/repo/notebooks/bootstrap"].resource_discovery_failures


# Review 5654949876: unsupported control flow must not certify discovery,
# and exact config paths must not consume a same-named package import hint.
@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("compound", [
    'try:\n    cfg = load_config("../config")\nexcept Exception:\n    raise',
    'with context():\n    cfg = load_config("../config")',
    'for item in items:\n    cfg = load_config("../config")',
    'while condition:\n    cfg = load_config("../config")',
    'def config():\n    return load_config("../config")',
])
def test_compound_load_config_reports_incomplete_discovery(git, compound):
    repo = ConfigRepo(source=compound, binding=None, git=git)
    graph, _ = _resolve(repo)
    node = graph.notebooks[repo._full("notebooks/bootstrap")]
    assert any(f["reason"] == "CONFIG_DIRECTORY_UNRESOLVED"
               for f in node.resource_discovery_failures)


def test_known_unreachable_config_call_is_not_reported_as_unresolved():
    source = 'if False:\n    load_config(unknown)\nelse:\n    load_config("../config")'
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert "config/variables.yaml" in _resources(graph, repo)


@pytest.mark.parametrize("git", [False, True])
def test_exact_config_path_does_not_consume_package_import_hint(git):
    repo = FakeRepo(git=git, notebooks={
        "jobs/bootstrap": 'from config.loader import load_config\nload_config("../config")',
    }, files={
        "jobs/config/__init__.py": b"",
        "jobs/config/loader.py": b"def load_config(path):\n    return path\n",
        "config/settings.yaml": b"settings: {}\n",
    })
    if not git:
        repo.root = "/Users/fixture/repo"
    resolver = DependencyResolver(repo, log=lambda _: None)
    graph = resolver.resolve("1")
    assert set(_resources(graph, repo)) == {
        "jobs/config/__init__.py", "jobs/config/loader.py", "config/settings.yaml"}
    node = graph.notebooks[repo._full("jobs/bootstrap")]
    assert not node.resource_discovery_failures
    listed = list(repo.listed)
    assert not resolver._discover_local_resources(graph, node, node.dbx_path)
    assert repo.listed == listed  # repeated scans deduplicate each intent independently


@pytest.mark.parametrize("widget_api", ["dbutils.widgets", "aidputils.widgets", "oidlUtils.widgets"])
@pytest.mark.parametrize("mutation", [
    'removeAll()',
    'text("repo_root", "/Users/other/repo")',
    'remove("repo_root")',
    'remove(dynamic_name)',
    'remove("other" if flag else runtime_name())',
    'text(dynamic_name, "/Users/other/repo")',
    'dropdown("repo_root", "/Users/other/repo", choices)',
])
@pytest.mark.parametrize("binding", [None, "/Workspace/Users/fixture/repo"])
def test_unknown_branch_widget_mutation_invalidates_only_inferred_defaults(widget_api, mutation, binding):
    source = (
        f'{widget_api}.text("repo_root", "/Users/fixture/repo")\n'
        f'if runtime_condition:\n    {widget_api}.{mutation}\n'
        f'ROOT = {widget_api}.get("repo_root")\n'
        'load_config(f"{ROOT}/config")\n')
    repo = ConfigRepo(source=source, binding=binding)
    graph, _ = _resolve(repo)
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    if binding is None:
        assert not _resources(graph, repo)
        assert any(f["reason"] == "CONFIG_DIRECTORY_UNRESOLVED" for f in failures)
    else:
        # Supplied task/job/exporter bindings retain their existing precedence.
        assert not failures
        assert "config/variables.yaml" in _resources(graph, repo)


@pytest.mark.parametrize("statement", [
    'ROOT += "/other"',
    'del ROOT',
    'ROOT, other = runtime_values',
    'try:\n    ROOT = "../other/config"\nexcept Exception:\n    raise',
])
def test_uninterpreted_root_mutation_does_not_reuse_stale_binding(statement):
    repo = ConfigRepo(source='ROOT = "../config"\n' + statement + '\nload_config(ROOT)', binding=None)
    graph, _ = _resolve(repo)
    assert not _resources(graph, repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


def test_unknown_branch_unrelated_widget_preserves_root_default():
    source = (
        'dbutils.widgets.text("repo_root", "/Users/fixture/repo")\n'
        'if runtime_condition:\n    dbutils.widgets.text("other", "value")\n'
        'ROOT = dbutils.widgets.get("repo_root")\nload_config(f"{ROOT}/config")')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert "config/variables.yaml" in _resources(graph, repo)


def test_known_widget_reset_restores_resolution_after_unknown_mutation():
    source = (
        'dbutils.widgets.text("repo_root", "/Users/other/repo")\n'
        'if runtime_condition:\n    dbutils.widgets.removeAll()\n'
        'dbutils.widgets.removeAll()\n'
        'dbutils.widgets.text("repo_root", "/Users/fixture/repo")\n'
        'ROOT = dbutils.widgets.get("repo_root")\nload_config(f"{ROOT}/config")')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert "config/variables.yaml" in _resources(graph, repo)


# Review 5663575141: known candidates from a ternary are not proof that
# every configuration directory is known. Keep the shared evaluator additive.
@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("conditional", [
    '"../config" if flag else runtime_root()',
    'runtime_root() if flag else "../config"',
    '"../config" if flag else ("../other/config" if other_flag else runtime_root())',
])
@pytest.mark.parametrize("use", ["inline", "alias", "format", "join", "widget_dbutils", "widget_aidputils", "widget_oidlUtils"])
def test_partial_conditional_root_exports_known_files_but_reports_incomplete(git, conditional, use):
    if use == "inline":
        source = f'load_config(config_dir={conditional})'
    elif use.startswith("widget_"):
        api = use.removeprefix("widget_") + ".widgets"
        source = (f'{api}.text("config_dir", {conditional})\n'
                  f'ROOT = {api}.get("config_dir")\nload_config(ROOT)')
    else:
        source = f'ROOT = {conditional}\n'
        source += {
            "alias": 'COPY = ROOT\nload_config(COPY)',
            "format": 'COPY = f"{ROOT}"\nload_config(COPY)',
            "join": 'COPY = os.path.join(ROOT, ".")\nload_config(COPY)',
        }[use]
    repo = ConfigRepo(source=source, binding=None, git=git)
    graph, _ = _resolve(repo)
    assert "config/variables.yaml" in _resources(graph, repo)
    node = graph.notebooks[repo._full("notebooks/bootstrap")]
    assert any(f["reason"] == "CONFIG_DIRECTORY_UNRESOLVED"
               for f in node.resource_discovery_failures)


@pytest.mark.parametrize("source", [
    'ROOT = ["../config", runtime_root()]\nload_config(ROOT)',
    'ROOT = flag and "../config" or runtime_root()\nload_config(ROOT)',
])
def test_partial_unknown_sibling_expressions_remain_unresolved(source):
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


@pytest.mark.parametrize("source", [
    'UNUSED = "../config" if flag else runtime_root()\nload_config("../config")',
    'ROOT = "../other/config" if flag else runtime_root()\nROOT = "../config"\nload_config(ROOT)',
    'dbutils.widgets.text("repo_root", "../other/config" if flag else runtime_root())\n'
    'dbutils.widgets.removeAll()\ndbutils.widgets.text("repo_root", "../config")\n'
    'load_config(dbutils.widgets.get("repo_root"))',
])
def test_unused_or_overwritten_partial_roots_do_not_taint_later_known_loads(source):
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert "config/variables.yaml" in _resources(graph, repo)


def test_supplied_parameter_wins_over_partial_widget_default():
    source = (
        'dbutils.widgets.text("repo_root", "../config" if flag else runtime_root())\n'
        'ROOT = dbutils.widgets.get("repo_root")\nload_config(f"{ROOT}/config")')
    repo = ConfigRepo(source=source, binding="/Users/fixture/repo")
    graph, _ = _resolve(repo)
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert "config/variables.yaml" in _resources(graph, repo)


def test_fully_known_ternary_roots_export_both_without_failure():
    repo = ConfigRepo(source='ROOT = "../config" if flag else "../other/config"\nload_config(ROOT)', binding=None)
    graph, _ = _resolve(repo)
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert {"config/variables.yaml", "other/config/variables.yaml"} <= set(_resources(graph, repo))


def test_partial_ternary_keeps_shared_candidate_evaluator_behavior():
    import ast
    from dbx_export.dependency_resolver import _static_values
    expression = ast.parse('"../config" if flag else runtime_root()', mode="eval").body
    assert _static_values(expression, {}, {}) == ["../config"]
    assert _static_values(expression, {}, {}, config_paths=True) == ["../config"]


def test_widget_registration_after_unknown_reset_remains_ambiguous():
    from aidp_compat.widgets import AIDPWidgetUtils
    roots = set()
    for reset in (False, True):
        # Use the real widget methods without environment-dependent initialization.
        widget = AIDPWidgetUtils.__new__(AIDPWidgetUtils)
        widget._widgets, widget._values = {}, {}
        widget.text("repo_root", "/Users/old/repo")
        if reset:
            widget.removeAll()
        widget.text("repo_root", "/Users/fixture/repo")
        roots.add(widget.get("repo_root"))
    assert roots == {"/Users/old/repo", "/Users/fixture/repo"}
    source = (
        'dbutils.widgets.text("repo_root", "/Users/old/repo")\n'
        'if unknown:\n    dbutils.widgets.removeAll()\n'
        'dbutils.widgets.text("repo_root", "/Users/fixture/repo")\n'
        'load_config(dbutils.widgets.get("repo_root"))')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


@pytest.mark.parametrize("binding", [None, "/Users/fixture/repo"])
def test_partial_widget_getargument_fallback_respects_supplied_binding(binding):
    source = ('ROOT = dbutils.widgets.getArgument("repo_root", '
              '"/Users/fixture/repo" if flag else runtime_root())\n'
              'load_config(f"{ROOT}/config")')
    repo = ConfigRepo(source=source, binding=binding)
    graph, _ = _resolve(repo)
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert bool(failures) == (binding is None)
    assert "config/variables.yaml" in _resources(graph, repo)


def test_partial_alias_survives_overwriting_its_source():
    source = ('ROOT = "../config" if flag else runtime_root()\n'
              'COPY = ROOT\nROOT = "../config"\nload_config(COPY)')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert "config/variables.yaml" in _resources(graph, repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


def test_partial_conditional_predicate_does_not_select_a_confident_if_branch():
    source = ('ROOT = "../config" if flag else runtime_root()\n'
              'if ROOT:\n    DIRECTORY = "../config"\n'
              'else:\n    DIRECTORY = "../other/config"\nload_config(DIRECTORY)')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


def test_partial_fallback_is_checked_before_registering_widget_default():
    source = (
        'dbutils.widgets.text("config_dir", dbutils.widgets.getArgument('
        '"config_dir", "../config" if flag else runtime_root()))\n'
        'load_config(dbutils.widgets.get("config_dir"))')
    repo = ConfigRepo(source=source, binding=None)
    graph, _ = _resolve(repo)
    assert "config/variables.yaml" in _resources(graph, repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


# Review 5665287001: completeness follows evaluation, including skipped operands
# and field selection, without changing shared additive candidate inference.
@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("binding_source", ["task", "job", "exporter"])
@pytest.mark.parametrize("expression", [
    'dbutils.widgets.get("repo_root") or DEFAULT',
    'dbutils.widgets.get("repo_root") or (runtime_root() if flag else DEFAULT)',
    'dbutils.widgets.get("repo_root") and dbutils.widgets.get("repo_root") or DEFAULT',
])
def test_supplied_root_skips_unused_partial_fallback(git, binding_source, expression):
    source = ('DEFAULT = "../config" if flag else runtime_root()\n'
              f'ROOT = {expression}\nload_config(f"{{ROOT}}/config")')
    binding = ".." if git else "/Users/fixture/repo"
    repo = ConfigRepo(source=source, git=git, binding=binding, binding_source=binding_source)
    overrides = {"repo_root": binding} if binding_source == "exporter" else {}
    if overrides:
        repo.binding = "/wrong/root"
    graph = DependencyResolver(repo, log=lambda _: None, parameter_bindings=overrides).resolve("1")
    assert not graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert "config/variables.yaml" in _resources(graph, repo)


@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("selector,unknown,failed", [
    ('runtime_choice()', 'runtime_root()', True),
    ('"primary"', 'runtime_root()', False),
    ('"other"', 'runtime_root()', True),
    ('runtime_choice()', '"../other/config"', False),
    ('"primary" if flag else runtime_choice()', '"../other/config"', True),
])
@pytest.mark.parametrize("use", ["direct", "alias", "nested", "sequence", "concat_list", "concat_tuple", "bool_container", "widget"])
def test_mapping_root_completeness_is_selected_per_entry(git, selector, unknown, failed, use):
    source = f'ROOTS = {{"primary": "../config", "other": {unknown}}}\n'
    expression = f'ROOTS[{selector}]'
    if use == "alias":
        source += 'COPY = ROOTS\nROOTS = {}\n'
        expression = f'COPY[{selector}]'
    elif use == "nested":
        source += 'NESTED = {"roots": ROOTS, "metadata": runtime_metadata()}\n'
        expression = f'NESTED["roots"][{selector}]'
    elif use == "sequence":
        source += 'NESTED = (ROOTS,)\n'
        expression = f'NESTED[0][{selector}]'
    elif use in {"concat_list", "concat_tuple"}:
        source += ('NESTED = [ROOTS] + ["metadata"]\n' if use == "concat_list"
                   else 'NESTED = (ROOTS,) + ("metadata",)\n')
        expression = f'NESTED[0][{selector}]'
    elif use == "bool_container":
        source += 'NESTED = ROOTS or runtime_mapping()\n'
        expression = f'NESTED[{selector}]'
    elif use == "widget":
        source += f'dbutils.widgets.text("directory", {expression})\n'
        expression = 'dbutils.widgets.get("directory")'
    source += f'load_config({expression})'
    repo = ConfigRepo(source=source, git=git, binding=None)
    graph, _ = _resolve(repo)
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert bool(failures) == failed
    if selector != '"other"':
        assert "config/variables.yaml" in _resources(graph, repo)
    if selector == 'runtime_choice()' and not failed:
        assert "other/config/variables.yaml" in _resources(graph, repo)


@pytest.mark.parametrize("mapping,failed", [
    ('{"primary": "../config", **runtime_mapping()}', True),
    ('{**runtime_mapping(), "primary": "../config"}', False),
    ('{"primary": "../config", runtime_key(): "../other/config"}', True),
    ('{runtime_key(): "../other/config", "primary": "../config"}', False),
    ('{"primary": "../config", "primary": runtime_root()}', True),
    ('{"primary": runtime_root(), "primary": "../config"}', False),
    ('{("primary" if flag else runtime_key()): "../config"}', True),
])
def test_mapping_overwrites_preserve_only_proven_selected_fields(mapping, failed):
    repo = ConfigRepo(source=f'ROOTS = {mapping}\nload_config(ROOTS["primary"])', binding=None)
    graph, _ = _resolve(repo)
    assert bool(graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures) == failed


@pytest.mark.parametrize("read", ["name", "widget"])
def test_parameter_candidate_truncation_cannot_certify_config_completeness(read):
    source = ('load_config(repo_root)' if read == "name"
              else 'load_config(dbutils.widgets.get("repo_root"))')
    repo = ConfigRepo(source=source, binding=None)
    graph = DependencyResolver(repo, log=lambda _: None,
                               parameter_bindings={"repo_root": ["../config"] * 64 + ["../other/config"]}).resolve("1")
    assert "config/variables.yaml" in _resources(graph, repo)
    assert graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures


# Team-lead review of 612c9a5 (2026-10-28): predicate consumers must
# request truth completeness; formatting options must not certify raw values.
@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("predicate,branch,failed", [
    ('META', 'config', False),
    ('not META', 'other/config', False),
    ('META or runtime_mapping()', 'config', False),
    ('META and True', 'config', False),
    ('[META]', 'config', False),
    ('{}', 'other/config', False),
    ('META if flag else runtime_mapping()', None, True),
    ('{} if flag else META', None, True),
    ('META and runtime_mapping()', None, True),
])
def test_config_if_consumes_truth_completeness(git, predicate, branch, failed):
    source = ('META = {"enabled": True, "runtime": runtime_metadata()}\n'
              f'if {predicate}:\n    ROOT = "../config"\n'
              'else:\n    ROOT = "../other/config"\nload_config(ROOT)')
    repo = ConfigRepo(source=source, binding=None, git=git)
    graph, _ = _resolve(repo)
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert bool(failures) == failed
    if failed:
        assert not _resources(graph, repo)
    else:
        assert set(_resources(graph, repo)) == {path for path in repo.files
                                              if path.startswith(branch + "/") and not path.endswith(".ipynb")}


@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("use", ["inline", "alias", "widget"])
@pytest.mark.parametrize("formatted,failed", [
    ('f"{ROOT}"', False),
    ('f"{ROOT:}"', False),
    ('f"{ROOT!s:{EMPTY}}"', False),
    ('f"{ROOT:{SPEC}}"', True),
    ('f"{ROOT:.8}"', True),
    ('f"{ROOT!r}"', True),
    ('f"{ROOT!a}"', True),
])
def test_config_formatted_options_are_not_certified_as_raw_paths(git, use, formatted, failed):
    source = ('ROOT = "../config"\nEMPTY = ""\n'
              'SPEC = "" if flag else runtime_format()\n')
    if use == "alias":
        source += f'COPY = {formatted}\n'
        argument = "COPY"
    elif use == "widget":
        source += f'dbutils.widgets.text("directory", {formatted})\n'
        argument = 'dbutils.widgets.get("directory")'
    else:
        argument = formatted
    source += f'load_config({argument})'
    repo = ConfigRepo(source=source, binding=None, git=git)
    graph, _ = _resolve(repo)
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert bool(failures) == failed
    assert set(_resources(graph, repo)) == {"config/variables.yaml", "config/nested/services.yml", "config/notes.txt"}


# Review 5680482727: unsupported stores must invalidate mutable provenance,
# including aliases retained by a bounded candidate union.
@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("setup,read,failed,expected", [
    ('ROOTS = {"primary": "../config"}\nROOTS["primary"] = "../other/config"', 'ROOTS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nROOTS["primary"]: str = "../other/config"', 'ROOTS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = ["../config"]\nROOTS[0] += "/nested"', 'ROOTS[0]', True, "config/settings.yaml"),
    ('ROOTS = ["../config", "../other/config"]\ndel ROOTS[0]', 'ROOTS[0]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nALIAS["primary"] = "../other/config"', 'ROOTS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nROOTS["primary"] = "../other/config"', 'ALIAS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nWRAP = {"roots": ROOTS}\nWRAP["roots"]["primary"] = "../other/config"', 'ROOTS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nWRAP = [ROOTS]\nROOTS["primary"] = "../other/config"', 'WRAP[0]["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nWRAP = {"roots": ROOTS}\nROOTS["primary"] = "../other/config"', 'WRAP["roots"]["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nif runtime_condition:\n    ROOTS["primary"] = "../other/config"', 'ROOTS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nOTHER = {"primary": "../config"}\nALIAS = ROOTS if flag else OTHER\nALIAS["primary"] = "../other/config"', 'OTHER["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nOTHER = {"primary": "../config"}\nALIAS = ROOTS if flag else OTHER\nALIAS["primary"] = "../other/config"', 'ROOTS["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nOTHER = {"primary": "../config"}\nWRAP = [ROOTS, OTHER]\nWRAP[runtime_index()]["primary"] = "../other/config"', 'OTHER["primary"]', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nSCALAR = ROOTS["primary"]\nROOTS["primary"] = "../other/config"', 'SCALAR', False, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nOTHER = {"primary": "../config"}\nROOTS["primary"] = "../other/config"', 'OTHER["primary"]', False, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nALIAS = {"primary": "../config"}\nALIAS["primary"] = "../other/config"', 'ROOTS["primary"]', False, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nROOTS["primary"] = "../other/config"\nROOTS = {"primary": "../other/config"}', 'ROOTS["primary"]', False, "other/config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nWRAP = {"roots": ROOTS}\nWRAP["roots"] = {"primary": "../other/config"}', 'ROOTS["primary"]', False, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nROOTS["primary"]: str', 'ROOTS["primary"]', False, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nOTHER = [1]\nOTHER[0] = 2', 'ROOTS["primary"]', False, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\nROOTS["primary"] = "../other/config"\ndbutils.widgets.text("directory", ROOTS["primary"])', 'dbutils.widgets.get("directory")', True, "config/settings.yaml"),
    ('ROOTS = {"primary": "../config"}\ndbutils.widgets.text("directory", ROOTS["primary"])\nROOTS["primary"] = "../other/config"', 'dbutils.widgets.get("directory")', False, "config/settings.yaml"),
    ('ROOT = "../config"\nUNKNOWN = runtime_mapping()\nUNKNOWN["key"] = "value"', 'ROOT', False, "config/settings.yaml"),
])
def test_config_container_mutation_invalidates_only_possible_aliases(git, setup, read, failed, expected):
    source = setup + "\nload_config(" + read + ")"
    repo = ConfigRepo(git=git, binding=None, source=source, files={
        "config/settings.yaml": b"old: true\n",
        "other/config/settings.yaml": b"new: true\n",
    })
    graph, _ = _resolve(repo)
    assert set(_resources(graph, repo)) == {expected}
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert bool(failures) == failed
    assert all(failure["reason"] == "CONFIG_DIRECTORY_UNRESOLVED" for failure in failures)


@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("expression,failed,expected", [
    ('str(b"../config", encoding="utf-8")', True, "notebooks/b'../config'/settings.yaml"),
    ('str(b"../config", **{"encoding": "utf-8"})', True, "notebooks/b'../config'/settings.yaml"),
    ('os.path.join("../config", ignored="value")', True, "config/settings.yaml"),
    ('"{root}".format(root="../config", **runtime_keywords())', True, "config/settings.yaml"),
    ('str("../config")', False, "config/settings.yaml"),
    ('os.path.join("..", "config")', False, "config/settings.yaml"),
    ('"{root}".format(root="../config")', False, "config/settings.yaml"),
    ('"{root}".format(root="../config", unused="known")', False, "config/settings.yaml"),
])
def test_config_calls_never_certify_ignored_keywords(git, expression, failed, expected):
    repo = ConfigRepo(git=git, binding=None, source="load_config(" + expression + ")", files={
        "config/settings.yaml": b"settings: {}\n",
        "notebooks/b'../config'/settings.yaml": b"wrong: true\n",
    })
    graph, _ = _resolve(repo)
    assert set(_resources(graph, repo)) == {expected}
    failures = graph.notebooks[repo._full("notebooks/bootstrap")].resource_discovery_failures
    assert bool(failures) == failed
    assert all(failure["reason"] == "CONFIG_DIRECTORY_UNRESOLVED" for failure in failures)


# Team-lead reproduction (2026-10-28): unsupported effects must invalidate
# completeness at the evaluator boundary, including ignored call operands.
@pytest.mark.parametrize("git", [False, True])
@pytest.mark.parametrize("setup,read,failed", [
    ('ROOTS = {"primary": "../config"}\nROOTS.update(primary="../other/config")', 'ROOTS["primary"]', True),
    ('ROOTS = ["../config"]\nROOTS.insert(0, "../other/config")', 'ROOTS[0]', True),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nALIAS.update(primary="../other/config")', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nWRAP = {"roots": ROOTS}\nWRAP["roots"].update(primary="../other/config")', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nWRAP = [ROOTS]\nmutate(WRAP[0])', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nmutate(roots=ROOTS)', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nALIAS = [ROOTS, "unused" if ROOTS.update(primary="../other/config") else "unused"]', 'ALIAS[0]["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nALIAS = {"roots": ROOTS, "unused": "unused" if ROOTS.update(primary="../other/config") else "unused"}', 'ALIAS["roots"]["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nALIAS = [ROOTS["primary"], "unused" if ROOTS.update(primary="../other/config") else "unused"]', 'ALIAS[0]', False),
    ('ROOTS = {"primary": "../config"}\ndbutils.widgets.text("unused", "value", str(ROOTS.update(primary="../other/config")))', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\ndbutils.widgets.text("supplied", "known")\n'
     'dbutils.widgets.getArgument("supplied", ROOTS.update(primary="../other/config"))', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nmutate({"roots": ROOTS})', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nmutate([ROOTS])', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nmutate(**{"roots": ROOTS})', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nROOTS |= {"primary": "../other/config"}', 'ALIAS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nif unknown:\n    ROOTS |= {"primary": "../other/config"}', 'ALIAS["primary"]', True),
    ('ROOT = "../config"\n(ROOT := "../other/config")', 'ROOT', True),
    ('ROOT = "../config"\nprint(ROOT := "../other/config")', 'ROOT', True),
    ('ROOT = "../config"\nUNUSED = str((ROOT := "../other/config"))', 'ROOT', True),
    ('ROOT = "../config"\nUNUSED = 1 if (ROOT := "../other/config") else 2', 'ROOT', True),
    ('ROOT = "../config"\nif (ROOT := "../other/config"):\n    pass', 'ROOT', True),
    ('ROOTS = {"primary": "../config"}\nUNUSED = {**mutate(ROOTS)}', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\ndbutils.widgets.text("supplied", "known")\n'
     'UNUSED = dbutils.widgets.getArgument("supplied", ROOTS.update(primary="../other/config"))', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\ndbutils.widgets.text("supplied", "known")\n'
     'UNUSED = dbutils.widgets.getArgument("supplied", defaultValue=ROOTS.update(primary="../other/config"))', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nUNUSED = "{root}".format(root="unused", effect=ROOTS.update(primary="../other/config"))', 'ROOTS["primary"]', True),
    ('ROOTS = {"primary": "../config"}\nSAVED = ROOTS["primary"]\nROOTS.update(primary="../other/config")', 'SAVED', False),
    ('ROOTS = {"primary": "../config"}\nOTHER = {"primary": "../config"}\nROOTS.update(primary="../other/config")', 'OTHER["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nROOTS.update(primary="../other/config")\nROOTS = {"primary": "../config"}', 'ROOTS["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nUNUSED = True or ROOTS.update(primary="../other/config")', 'ROOTS["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nUNUSED = False and ROOTS.update(primary="../other/config")', 'ROOTS["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nif False:\n    ROOTS.update(primary="../other/config")', 'ROOTS["primary"]', False),
    ('ROOT = "../config"\nUNUSED = True or (ROOT := "../other/config")', 'ROOT', False),
    ('ROOT = "../config"\nCOPY = ROOT\n(ROOT := "../other/config")', 'COPY', False),
    ('ROOTS = {"primary": "../config"}\nmutate(ROOTS["primary"])', 'ROOTS["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nmutate(str(ROOTS))', 'ROOTS["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nALIAS = ROOTS\nALIAS = {"primary": "../other/config"}\nALIAS.update(primary="../other/config")', 'ROOTS["primary"]', False),
    ('ROOTS = {"primary": "../config"}\nWRAP = {"roots": ROOTS}\nWRAP.update(roots={"primary": "../other/config"})', 'ROOTS["primary"]', True),
])
def test_unsupported_effects_cannot_recertify_stale_config_roots(git, setup, read, failed):
    repo = ConfigRepo(git=git, binding=None, source=setup + "\nload_config(" + read + ")", files={
        "config/settings.yaml": b"settings: {}\n",
        "other/config/settings.yaml": b"settings: {}\n",
    })
    graph, _ = _resolve(repo)
    node = graph.notebooks[repo._full("notebooks/bootstrap")]
    assert bool(node.resource_discovery_failures) == failed
    assert all(f["reason"] == "CONFIG_DIRECTORY_UNRESOLVED" for f in node.resource_discovery_failures)
    if not failed:
        assert set(_resources(graph, repo)) == {"config/settings.yaml"}

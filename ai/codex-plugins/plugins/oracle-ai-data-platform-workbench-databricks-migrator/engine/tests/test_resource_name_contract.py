"""REF-211: naming and legacy reuse through real exporter orchestration."""
from types import SimpleNamespace

import pytest

from dbx_export import __main__ as exporter
from dbx_export.aidp_client import AidpApiError
from dbx_export.dependency_resolver import FileNode
from dbx_export.report import Report
from dbx_export.sanitizer import (
    legacy_cluster_name, legacy_workflow_name,
    sanitize_cluster_name, sanitize_workflow_name,
)


class RecordingAidp:
    def __init__(self, workflows=None, clusters=None, fail_lookup=None):
        self.workflows = workflows or {}
        self.clusters = clusters or {}
        self.fail_lookup = fail_lookup
        self.lookups = []
        self.created = []
        self.updated = []
        self.created_clusters = []
        self.installed = []

    def find_workflow_by_name(self, name):
        self.lookups.append(("workflow", name))
        if name == self.fail_lookup:
            raise AidpApiError("find_workflow", 503, "unavailable")
        return self.workflows.get(name)

    def find_cluster_by_name(self, name):
        self.lookups.append(("cluster", name))
        return self.clusters.get(name)

    def create_cluster(self, payload):
        self.created_clusters.append(payload)
        return {"key": "created-cluster"}

    def install_cluster_libraries(self, key, items):
        self.installed.append((key, items))

    def create_workflow_shell(self, name, description):
        self.created.append(name)
        return {"key": "created-key"}

    def update_workflow(self, key, payload):
        self.updated.append((key, payload))


def run_phase3(aidp, name="source.job", overwrite=False, settings=None, dbx=None,
               cluster_override_key="operator-cluster"):
    settings = settings or {"name": name, "tasks": []}
    graph = SimpleNamespace(
        jobs={"1": SimpleNamespace(
            job_def={"job_id": "1", "settings": settings}, fetch_error=None,
        )}, notebooks={}, files={},
    )
    contexts = [f"job:1/job_cluster:{c['job_cluster_key']}" for c in settings.get("job_clusters", [])]
    contexts += [f"job:1/task_cluster:{t['task_key']}" for t in settings.get("tasks", []) if t.get("new_cluster")]
    contexts += [f"existing_cluster:{t['existing_cluster_id']}" for t in settings.get("tasks", []) if t.get("existing_cluster_id")]
    paths = {}
    for i, context in enumerate(contexts):
        uri = f"{exporter.CANONICAL_REQUIREMENTS_PREFIX}test_{i}.txt"
        graph.files[uri] = FileNode(uri, "requirements", referenced_by=[context], content=b"# empty requirements\n")
        paths[uri] = f"/Workspace/export/python_dependencies/test_{i}.txt"
    report = Report(run_id="test", dbx_root_job_id="1",
                    export_output="/Workspace/export", prefix="tool")
    exporter.phase3_create_clusters_and_workflows(
        aidp, dbx or SimpleNamespace(), graph, {}, paths,
        "/Workspace/export", "tool", overwrite, cluster_override_key, report,
    )
    return report


@pytest.mark.parametrize("state", ["new_only", "legacy_only", "both", "neither"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_phase3_workflow_name_lookup_and_overwrite_matrix(state, overwrite, capsys):
    name = "source.job"
    new, old = sanitize_workflow_name(name, "tool"), legacy_workflow_name(name, "tool")
    workflows = {}
    if state in ("new_only", "both"):
        workflows[new] = {"key": "new-key", "name": new}
    if state in ("legacy_only", "both"):
        workflows[old] = {"key": "legacy-key", "name": old}
    aidp = RecordingAidp(workflows)
    report = run_phase3(aidp, name, overwrite)
    expected_lookups = [("workflow", new)]
    if state not in ("new_only", "both"):
        expected_lookups.append(("workflow", old))
    assert aidp.lookups == expected_lookups
    result = report.jobs[0]
    if state == "neither":
        assert result.status == "CREATED"
        assert aidp.created == [new]
        assert aidp.updated[0][0] == "created-key"
    else:
        key = "legacy-key" if state == "legacy_only" else "new-key"
        assert aidp.created == []
        assert result.aidp_workflow_key == key
        if overwrite:
            assert result.status == "UPDATED"
            assert aidp.updated[0][0] == key
            assert aidp.updated[0][1]["name"] == new
            assert result.aidp_workflow_name == new
        else:
            assert result.status == "SKIPPED_EXISTS"
            assert aidp.updated == []
            assert result.aidp_workflow_name == (old if state == "legacy_only" else new)
    output = capsys.readouterr().out
    assert ("[LEGACY] workflow matched" in output) == (state == "legacy_only")
    assert not report.summary()["jobs_failed"]


def test_phase3_unchanged_name_has_one_lookup():
    aidp = RecordingAidp({"tool_plain": {"key": "existing"}})
    run_phase3(aidp, name="plain")
    assert aidp.lookups == [("workflow", "tool_plain")]
    assert not aidp.created and not aidp.updated


@pytest.mark.parametrize("failed_name", ["current", "legacy"])
def test_phase3_lookup_failure_never_creates_workflow(failed_name):
    name = "source.job"
    fail = (sanitize_workflow_name if failed_name == "current" else legacy_workflow_name)(name, "tool")
    aidp = RecordingAidp(fail_lookup=fail)
    with pytest.raises(AidpApiError, match="503"):
        run_phase3(aidp, name=name)
    assert not aidp.created and not aidp.updated


@pytest.mark.parametrize("source,expected", [
    ("top5_v5.inference Risk", "tool_top5_v5.inference_Risk"),
    ("Daily&Risk", "tool_Daily_Risk"),
    ("Pre_Aug'23 FL Addition", "tool_Pre_Aug_23_FL_Addition_ffea616c"),
    ("Workspace", "tool_Workspace_"),
    ("***", "tool_***"),
])
def test_legacy_lookup_names_preserve_pre268_mapping(source, expected):
    assert legacy_workflow_name(source, "tool") == expected
    assert legacy_cluster_name(source, "tool") == expected


@pytest.mark.parametrize("config_channel", ["default", "env", "cli"])
@pytest.mark.parametrize("prefix", ["", "123", "_tool", "ai-tool", "ai.tool", "ai*tool", "étool"])
def test_invalid_prefix_exits_before_config_auth_or_discovery(monkeypatch, capsys, config_channel, prefix):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid prefix reached config, auth or discovery")
    monkeypatch.setattr("aidp_config.load_config", forbidden)
    monkeypatch.setattr(exporter, "get_oci_signer", forbidden)
    monkeypatch.setattr(exporter, "phase1_discover", forbidden)
    args = ["--prefix", prefix]
    if config_channel == "env":
        monkeypatch.setenv("AIDP_CONFIG_PATH", "/missing/customer.yaml")
    elif config_channel == "cli":
        args += ["--config", "/missing/customer.yaml"]
    assert exporter.main(args) == exporter.ExitCode.BAD_ARGS
    captured = capsys.readouterr()
    assert "ERROR: invalid --prefix" in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize("args", [[], ["--prefix", "tool_123/path"]])
def test_valid_prefix_proceeds_to_config_loading(monkeypatch, capsys, args):
    def config_marker(**kwargs):
        raise ValueError("config loader reached")
    monkeypatch.setattr("aidp_config.load_config", config_marker)
    assert exporter.main(args) == exporter.ExitCode.BAD_ARGS
    assert "config loader reached" in capsys.readouterr().err


@pytest.mark.parametrize("context", ["job", "task", "existing"])
@pytest.mark.parametrize("state", ["new_only", "legacy_only", "both", "neither"])
def test_phase3_cluster_lookup_matrix(context, state, capsys):
    source = "legacy.cluster"
    new, old = sanitize_cluster_name(source, "tool"), legacy_cluster_name(source, "tool")
    clusters = {}
    if state in ("new_only", "both"):
        clusters[new] = {"key": "new-cluster", "displayName": new}
    if state in ("legacy_only", "both"):
        clusters[old] = {"key": "legacy-cluster", "displayName": old}
    aidp = RecordingAidp(workflows={"tool_plain": {"key": "workflow"}}, clusters=clusters)
    spec = {"cluster_name": source, "node_type_id": "m5.xlarge", "num_workers": 0}
    task = {"task_key": "task", "notebook_task": {"notebook_path": "/probe"}}
    settings = {"name": "plain", "tasks": [task]}
    if context == "job":
        settings["job_clusters"] = [{"job_cluster_key": "shared", "new_cluster": spec}]
        task["job_cluster_key"] = "shared"
    elif context == "task":
        task["new_cluster"] = spec
    else:
        task["existing_cluster_id"] = "dbx-old"
    dbx = SimpleNamespace(get_cluster=lambda key: spec)
    report = run_phase3(aidp, overwrite=True, settings=settings, dbx=dbx, cluster_override_key=None)
    expected_lookups = [("cluster", new)]
    if state not in ("new_only", "both"):
        expected_lookups.append(("cluster", old))
    assert [v for v in aidp.lookups if v[0] == "cluster"] == expected_lookups
    expected_key = ("created-cluster" if state == "neither" else
                    "legacy-cluster" if state == "legacy_only" else "new-cluster")
    assert report.jobs[0].status == "UPDATED"
    assert aidp.updated[0][1]["tasks"][0]["cluster"]["clusterKey"] == expected_key
    if state == "neither":
        assert aidp.created_clusters[0]["displayName"] == new
    else:
        assert aidp.created_clusters == []
        assert (old if state == "legacy_only" else new) in report.jobs[0].cluster_status
    assert ("[LEGACY] cluster matched" in capsys.readouterr().out) == (state == "legacy_only")

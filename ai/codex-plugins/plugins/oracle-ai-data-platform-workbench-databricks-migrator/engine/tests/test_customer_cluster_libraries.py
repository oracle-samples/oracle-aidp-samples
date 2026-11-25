from copy import deepcopy
from types import SimpleNamespace

import pytest
import yaml

from dbx_export.customer_cluster_libraries import (
    load_cluster_library_rules, prepare_cluster_library_rules,
    verify_cluster_library_contract,
)
from dbx_export.dependency_resolver import FileNode, Graph, JobNode
from dbx_export.report import Report
from dbx_export import __main__ as exporter


OLD = "dbfs:/jars/engine_assembly.jar"
REPO = "/Workspace/libraries"
FIXED = REPO + "/engine-assembly-fixed.jar"


def pack(tmp_path):
    tmp_path.joinpath("overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 1, "overrides": [], "cluster_libraries": [{
            "dbx_job_id": "123", "job_cluster_key": "scoring",
            "jar": "engine-assembly-fixed.jar", "replace_basenames": ["engine*assembly*.jar"],
            "reason": "customer approved replacement",
        }],
    }))
    return SimpleNamespace(customizations=SimpleNamespace(path=str(tmp_path)))


def graph():
    definition = {"settings": {"name": "job", "job_clusters": [{
        "job_cluster_key": key, "new_cluster": {"libraries": [{"jar": OLD}]},
    } for key in ("scoring", "other")], "tasks": [{
        "task_key": key, "job_cluster_key": key, "notebook_task": {"notebook_path": "/Users/" + key},
        "libraries": [{"jar": OLD}],
    } for key in ("scoring", "other")]}}
    return Graph(jobs={"123": JobNode("123", 0, definition)}, files={
        OLD: FileNode(OLD, "jar", referenced_by=[
            "job:123/job_cluster:scoring", "job:123/task:scoring", "job:123/job_cluster:other",
        ]),
    })


def report():
    return Report("test", "123", "/Workspace/export", "TEST")


class Aidp:
    def __init__(self):
        self.rows = []
        self.cluster_rows = {}
        self.calls = []
        self.available = True
        self.fail = False

    def list_workspace_objects(self, path):
        assert path == REPO
        return [{"path": FIXED, "type": "FILE"}] if self.available else []

    def list_cluster_libraries(self, cluster):
        return deepcopy(self.cluster_rows.get(cluster, self.rows))

    def install_cluster_libraries(self, cluster, items):
        self.calls.append((cluster, deepcopy(items)))
        if self.fail:
            raise exporter.AidpApiError("install", 500, "failed")
        rows = self.cluster_rows.setdefault(cluster, deepcopy(self.rows))
        for item in items:
            if not any(row["path"] == item["path"] for row in rows):
                rows.append({"path": item["path"], "status": "INSTALLED"})


def test_scoped_plan_preserves_source_and_other_cluster(tmp_path):
    original = graph()
    before = deepcopy(original)
    mapping = {OLD: REPO + "/engine_assembly.jar"}
    planned, contracts = prepare_cluster_library_rules(Aidp(), original, pack(tmp_path), REPO, mapping, print, report())
    assert original == before
    assert planned.files[OLD].referenced_by == ["job:123/job_cluster:other"]
    tasks = planned.jobs["123"].job_def["settings"]["tasks"]
    assert tasks[0]["libraries"] == []
    assert tasks[1]["libraries"] == [{"jar": OLD}]
    conv = SimpleNamespace(library_refs=[{"jar": OLD}, {"jar": "unrelated.jar"}])
    exporter._attach_context_jars(conv, planned, "job:123/job_cluster:scoring")
    assert {item["jar"] for item in conv.library_refs} == {"unrelated.jar", next(uri for uri in planned.files if uri != OLD)}
    assert exporter._context_jar_install_items(planned, "job:123/job_cluster:scoring", mapping) == [
        {"operation": "INSTALL", "type": "WORKSPACE_FILE", "path": FIXED},
    ]
    assert list(contracts) == ["job:123/job_cluster:scoring"]


def test_inert_without_pack_or_matching_job(tmp_path):
    original = graph()
    assert prepare_cluster_library_rules(Aidp(), original, None, REPO, {}, print, report()) == (original, {})
    original.jobs["456"] = original.jobs.pop("123")
    assert prepare_cluster_library_rules(Aidp(), original, pack(tmp_path), REPO, {}, print, report()) == (original, {})


def test_missing_exact_jar_refuses_no_fallback(tmp_path):
    aidp = Aidp()
    aidp.available = False
    with pytest.raises(ValueError, match="Required customer-pack JAR not found"):
        prepare_cluster_library_rules(aidp, graph(), pack(tmp_path), REPO, {}, print, report())
    assert not aidp.calls


@pytest.mark.parametrize("status", ["INSTALLED", "INSTALL_ON_RESTART", "FAILED", "UNINSTALL_ON_RESTART"])
def test_existing_competing_registration_fails_before_install(tmp_path, status):
    aidp = Aidp()
    aidp.rows = [{"path": REPO + "/engine_assembly.jar", "status": status}]
    rule = dict(load_cluster_library_rules(pack(tmp_path))[0], path=FIXED)
    with pytest.raises(ValueError, match="Detach competing assemblies"):
        verify_cluster_library_contract(aidp, "score", rule, print, report())
    assert not aidp.calls


@pytest.mark.parametrize("status", ["INSTALLED", "RESOLVED", "INSTALL_ON_RESTART"])
def test_exact_jar_loaded_or_staged_honestly(tmp_path, status):
    aidp = Aidp()
    aidp.rows = [{"path": FIXED, "status": status}, {"path": REPO + "/engine_assembly.jar", "status": "DELETED"}]
    rule = dict(load_cluster_library_rules(pack(tmp_path))[0], path=FIXED)
    messages = []
    verify_cluster_library_contract(aidp, "score", rule, messages.append, report())
    assert ("STAGED" in messages[-1]) == (status == "INSTALL_ON_RESTART")


@pytest.mark.parametrize("new", [False, True])
def test_real_phase3_installs_override_only_on_actual_scoring_cluster(tmp_path, monkeypatch, new):
    aidp = Aidp()
    aidp.find_cluster_by_name = lambda name: None if new else {"key": name}
    aidp.create_cluster = lambda payload: {"key": payload["displayName"]}
    aidp.find_workflow_by_name = lambda name: {"key": "existing-workflow"}
    aidp.created = []
    def conversion(**kw):
        return SimpleNamespace(
            library_refs=deepcopy(kw["new_cluster"]["libraries"]), init_script_refs=[],
            shape_mapping_log_lines=[], driver_shape_res=None, worker_shape_res=None,
            aidp_payload={"displayName": "TEST_" + kw["cluster_display_name"]},
        )
    monkeypatch.setattr(exporter, "convert_dbx_cluster", conversion)
    monkeypatch.setattr(exporter, "_report_dropped_conf", lambda *args: None)
    monkeypatch.setattr(exporter, "_create_cluster_with_intel_fallback", lambda a, p, *args: aidp.create_cluster(p))
    monkeypatch.setattr(exporter, "_attach_only_consolidated_requirements", lambda *args: "requirements")
    results = report()
    exporter.phase3_create_clusters_and_workflows(
        aidp, SimpleNamespace(), graph(), {}, {OLD: REPO + "/engine_assembly.jar"},
        "/Workspace/export", "TEST", False, None, results,
        cfg=pack(tmp_path), lib_repository_dir=REPO,
    )
    score_items = [item for cluster, items in aidp.calls if cluster == "TEST_scoring" for item in items]
    other_items = [item for cluster, items in aidp.calls if cluster == "TEST_other" for item in items]
    assert FIXED in [item["path"] for item in score_items]
    assert REPO + "/engine_assembly.jar" not in [item["path"] for item in score_items]
    assert FIXED not in [item["path"] for item in other_items]
    assert results.jobs[0].status == "SKIPPED_EXISTS"
    assert any("customer-pack-cluster-library" in line for line in results.errors)


def test_override_cluster_refused_without_mutating_migration_cluster(tmp_path):
    aidp = Aidp()
    results = report()
    exporter.phase3_create_clusters_and_workflows(
        aidp, SimpleNamespace(), graph(), {}, {}, "/Workspace/export", "TEST", False,
        "migration-cluster", results, cfg=pack(tmp_path), lib_repository_dir=REPO,
    )
    assert results.jobs[0].status == "FAILED"
    assert not aidp.calls


def test_install_failure_is_not_best_effort_success(tmp_path):
    aidp = Aidp()
    aidp.fail = True
    rule = dict(load_cluster_library_rules(pack(tmp_path))[0], path=FIXED)
    with pytest.raises(exporter.AidpApiError):
        verify_cluster_library_contract(aidp, "score", rule, print, report())


def test_phase3_conflict_blocks_before_any_cluster_write(tmp_path):
    aidp = Aidp()
    aidp.rows = [{"path": REPO + "/engine_assembly.jar", "status": "INSTALLED"}]
    aidp.find_cluster_by_name = lambda name: {"key": name}
    results = report()
    exporter.phase3_create_clusters_and_workflows(
        aidp, SimpleNamespace(), graph(), {}, {}, "/Workspace/export", "TEST", False,
        None, results, cfg=pack(tmp_path), lib_repository_dir=REPO,
    )
    assert results.jobs[0].status == "FAILED"
    assert not aidp.calls


def test_same_named_fixed_jar_elsewhere_is_not_exact_contract(tmp_path):
    aidp = Aidp()
    aidp.rows = [{"path": "/Workspace/elsewhere/engine-assembly-fixed.jar", "status": "INSTALLED"}]
    rule = dict(load_cluster_library_rules(pack(tmp_path))[0], path=FIXED)
    with pytest.raises(ValueError, match="JAR conflict"):
        verify_cluster_library_contract(aidp, "score", rule, print, report())
    assert not aidp.calls


@pytest.mark.parametrize("change", [
    {"jar": "../fixed.jar"}, {"replace_basenames": []},
    {"job_cluster_key": "../scoring"}, {"dbx_job_id": 123},
])
def test_malformed_rule_fails_loud(tmp_path, change):
    cfg = pack(tmp_path)
    file = tmp_path / "overrides.yaml"
    data = yaml.safe_load(file.read_text())
    data["cluster_libraries"][0].update(change)
    file.write_text(yaml.safe_dump(data))
    with pytest.raises(ValueError):
        load_cluster_library_rules(cfg)


def test_duplicate_rules_and_malformed_yaml_refused(tmp_path):
    cfg = pack(tmp_path)
    file = tmp_path / "overrides.yaml"
    data = yaml.safe_load(file.read_text())
    data["cluster_libraries"].append(deepcopy(data["cluster_libraries"][0]))
    file.write_text(yaml.safe_dump(data))
    with pytest.raises(ValueError, match="Duplicate"):
        load_cluster_library_rules(cfg)
    file.write_text("schema_version: [")
    with pytest.raises(ValueError, match="Invalid customer-pack YAML"):
        load_cluster_library_rules(cfg)


def test_plan_is_idempotent(tmp_path):
    cfg = pack(tmp_path)
    mapping = {OLD: REPO + "/engine_assembly.jar"}
    planned, contracts = prepare_cluster_library_rules(Aidp(), graph(), cfg, REPO, mapping, print, report())
    twice, again = prepare_cluster_library_rules(Aidp(), planned, cfg, REPO, mapping, print, report())
    assert twice == planned
    assert again == contracts


def test_source_alias_replaced_by_its_actual_staged_path(tmp_path):
    original = graph()
    alias = "dbfs:/opaque/uuid-assembly.jar"
    original.files[alias] = original.files.pop(OLD)
    settings = original.jobs["123"].job_def["settings"]
    for spec in settings["job_clusters"]:
        spec["new_cluster"]["libraries"] = [{"jar": alias}]
    for task in settings["tasks"]:
        task["libraries"] = [{"jar": alias}]
    planned, _ = prepare_cluster_library_rules(
        Aidp(), original, pack(tmp_path), REPO,
        {alias: REPO + "/engine_assembly.jar"}, print, report(),
    )
    settings = planned.jobs["123"].job_def["settings"]
    assert settings["job_clusters"][0]["new_cluster"]["libraries"] == []
    assert settings["job_clusters"][1]["new_cluster"]["libraries"] == [{"jar": alias}]
    conv = SimpleNamespace(library_refs=[{"jar": alias}])
    exporter._attach_context_jars(conv, planned, "job:123/job_cluster:scoring")
    assert alias not in [lib["jar"] for lib in conv.library_refs]

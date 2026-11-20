from copy import deepcopy
from types import SimpleNamespace

import pytest
import yaml

from dbx_export import __main__ as exporter
from dbx_export.customer_cluster_init_scripts import (
    attach_cluster_init_script,
    load_cluster_init_script_rules,
    prepare_cluster_init_script_rules,
    verify_existing_cluster_init_script,
)
from test_customer_cluster_libraries import graph, report


SCRIPT = "/Workspace/migration-dependencies/initscript/init_example_lib_resources.sh"


def pack(tmp_path, *, destination=SCRIPT):
    tmp_path.joinpath("overrides.yaml").write_text(yaml.safe_dump({
        "schema_version": 1,
        "cluster_init_scripts": [{
            "dbx_job_id": "123", "job_cluster_key": "scoring",
            "destination": destination, "reason": "staged RocksDB bootstrap",
        }],
    }), encoding="utf-8")
    return SimpleNamespace(customizations=SimpleNamespace(path=str(tmp_path)))


class Aidp:
    def __init__(self):
        self.created = []
        self.script_available = True
        self.existing = None
        self.existing_names = None

    def list_workspace_objects(self, path):
        assert path == "/Workspace/migration-dependencies/initscript"
        return [{"path": SCRIPT, "type": "FILE"}] if self.script_available else []

    def find_cluster_by_name(self, name):
        if self.existing_names is not None:
            return {"key": name, "displayName": name} if name in self.existing_names else None
        return {"key": name} if self.existing else None

    def get_cluster(self, key):
        return {"clusterRuntimeConfig": {"initScripts": deepcopy(self.existing)}}

    def create_cluster(self, payload):
        self.created.append(deepcopy(payload))
        return {"key": payload["displayName"]}

    def install_cluster_libraries(self, key, items):
        pass

    def find_workflow_by_name(self, name):
        return {"key": "existing-workflow"}


def run_phase3(tmp_path, monkeypatch, aidp, *, cluster_override_key=None):
    def conversion(**kwargs):
        return SimpleNamespace(
            library_refs=[], init_script_refs=[], shape_mapping_log_lines=[],
            driver_shape_res=None, worker_shape_res=None,
            aidp_payload={
                "displayName": "TEST_" + kwargs["cluster_display_name"],
                "clusterRuntimeConfig": {"initScripts": []},
            },
        )

    monkeypatch.setattr(exporter, "prepare_cluster_library_rules",
                        lambda aidp, graph, *args: (graph, {}))
    monkeypatch.setattr(exporter, "convert_dbx_cluster", conversion)
    monkeypatch.setattr(exporter, "_report_dropped_conf", lambda *args: None)
    monkeypatch.setattr(exporter, "_create_cluster_with_intel_fallback",
                        lambda client, payload, *args: client.create_cluster(payload))
    monkeypatch.setattr(exporter, "_attach_only_consolidated_requirements",
                        lambda *args: "requirements")
    result = report()
    exporter.phase3_create_clusters_and_workflows(
        aidp, SimpleNamespace(), graph(), {}, {}, "/Workspace/export", "TEST",
        False, cluster_override_key, result, cfg=pack(tmp_path),
    )
    return result


def test_phase3_attaches_only_exact_cluster_before_create(tmp_path, monkeypatch):
    aidp = Aidp()
    result = run_phase3(tmp_path, monkeypatch, aidp)
    by_name = {item["displayName"]: item for item in aidp.created}
    assert by_name["TEST_scoring"]["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}]
    assert by_name["TEST_other"]["clusterRuntimeConfig"]["initScripts"] == []
    assert result.jobs[0].status == "SKIPPED_EXISTS"
    assert any("customer-pack-cluster-init-script" in line for line in result.errors)


def test_missing_script_fails_before_cluster_write(tmp_path, monkeypatch):
    aidp = Aidp()
    aidp.script_available = False
    result = run_phase3(tmp_path, monkeypatch, aidp)
    assert aidp.created == []
    assert result.jobs[0].status == "FAILED"
    assert "Required customer-pack init script not found" in result.jobs[0].failure_reason
    assert any("ERROR customer-pack-cluster-init-script:" in line
               for line in result.errors)
    assert not any("ERROR customer-pack-cluster-library:" in line
                   for line in result.errors)


def test_reused_cluster_requires_exact_attachment(tmp_path, monkeypatch):
    rule = load_cluster_init_script_rules(pack(tmp_path))[0]
    aidp = Aidp()
    aidp.existing = [{"destination": SCRIPT}]
    verify_existing_cluster_init_script(aidp, "cluster", rule)
    aidp.existing = [{"destination": "/Workspace/old/bootstrap.sh"}]
    aidp.existing_names = {"TEST_scoring"}
    with pytest.raises(ValueError, match="does not have the required"):
        verify_existing_cluster_init_script(aidp, "cluster", rule)
    result = run_phase3(tmp_path, monkeypatch, aidp)
    assert aidp.created == []
    assert result.jobs[0].status == "FAILED"
    assert "no cluster override/fallback" in result.jobs[0].failure_reason


def test_inert_for_other_job_and_idempotent_attachment(tmp_path):
    source = graph()
    source.jobs["456"] = source.jobs.pop("123")
    aidp = Aidp()
    assert prepare_cluster_init_script_rules(aidp, source, pack(tmp_path), print, report()) == {}
    assert prepare_cluster_init_script_rules(aidp, source, None, print, report()) == {}
    rule = load_cluster_init_script_rules(pack(tmp_path))[0]
    payload = {"clusterRuntimeConfig": {"initScripts": []}}
    attach_cluster_init_script(payload, rule)
    attach_cluster_init_script(payload, rule)
    assert payload["clusterRuntimeConfig"]["initScripts"] == [{"destination": SCRIPT}]


def test_cluster_override_fails_before_write(tmp_path, monkeypatch):
    aidp = Aidp()
    result = run_phase3(tmp_path, monkeypatch, aidp, cluster_override_key="other")
    assert aidp.created == []
    assert result.jobs[0].status == "FAILED"
    assert "cluster override is forbidden" in result.jobs[0].failure_reason


@pytest.mark.parametrize("field,value", [
    ("dbx_job_id", 123), ("job_cluster_key", "../scoring"),
    ("destination", "s3://private/bootstrap.sh"),
    ("destination", "/Workspace/../elsewhere/bootstrap.sh"),
])
def test_bad_rule_fails_loud(tmp_path, field, value):
    cfg = pack(tmp_path)
    registry = tmp_path / "overrides.yaml"
    document = yaml.safe_load(registry.read_text(encoding="utf-8"))
    document["cluster_init_scripts"][0][field] = value
    registry.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError):
        load_cluster_init_script_rules(cfg)

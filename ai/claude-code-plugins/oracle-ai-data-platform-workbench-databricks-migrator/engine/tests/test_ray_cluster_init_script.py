from copy import deepcopy
from types import SimpleNamespace

import pytest
import yaml

from dbx_export import __main__ as exporter
from dbx_export.aidp_client import AidpClient, AidpConfig
from dbx_export.dependency_resolver import FileNode, NotebookNode
from dbx_export.ray_cluster_init_script import (
    attach_ray_init_script,
    prepare_ray_cluster_init_script_rules,
    verify_existing_ray_init_script,
)
from test_customer_cluster_libraries import Aidp, REPO, graph, report


SCRIPT = "/Workspace/migration-dependencies/initscript/ray_executor.sh"


def test_aidp_client_get_cluster_reads_complete_cluster_definition(monkeypatch):
    class Response:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {
                "key": "cluster",
                "clusterRuntimeConfig": {
                    "initScripts": [{"destination": SCRIPT}],
                },
            }

    calls = []

    def fake_get(url, auth):
        calls.append((url, auth))
        return Response()

    monkeypatch.setattr("dbx_export.aidp_client.requests.get", fake_get)
    config = AidpConfig(
        lake_ocid="ocid1.aidataplatform.oc1.ap-mumbai-1.test",
        workspace_id="workspace",
        export_output="/Workspace/export",
    )
    signer = object()
    client = AidpClient(signer, config)

    cluster = client.get_cluster("cluster")

    assert cluster["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}
    ]
    assert calls == [(f"{config.ws}/clusters/cluster", signer)]


def pack(tmp_path):
    document = {
        "schema_version": 1,
        "ray_init_script": {"destination": SCRIPT, "reason": "staged Ray head"},
    }
    tmp_path.joinpath("overrides.yaml").write_text(yaml.safe_dump(document), encoding="utf-8")
    return SimpleNamespace(customizations=SimpleNamespace(path=str(tmp_path)))


class InitAidp(Aidp):
    def __init__(self):
        super().__init__()
        self.created = []
        self.script_available = True
        self.existing = None

    def list_workspace_objects(self, path):
        assert path == "/Workspace/migration-dependencies/initscript"
        return [{"path": SCRIPT, "type": "FILE"}] if self.script_available else []

    def find_cluster_by_name(self, name):
        return {"key": name} if self.existing is not None else None

    def get_cluster(self, key):
        return {"clusterRuntimeConfig": {"initScripts": deepcopy(self.existing)}}

    def create_cluster(self, payload):
        self.created.append(deepcopy(payload))
        return {"key": payload["displayName"]}

    def find_workflow_by_name(self, name):
        return {"key": "existing-workflow"}


def run_phase3(
    tmp_path, monkeypatch, aidp, *, source, nested=False,
    source_graph=None, dbx=None, uri_to_aidp=None,
):
    def conversion(**kwargs):
        return SimpleNamespace(
            library_refs=[], init_script_refs=[], shape_mapping_log_lines=[],
            driver_shape_res=None, worker_shape_res=None,
            aidp_payload={
                "displayName": "TEST_" + kwargs["cluster_display_name"],
                "clusterRuntimeConfig": {"initScripts": []},
            },
        )

    monkeypatch.setattr(exporter, "convert_dbx_cluster", conversion)
    monkeypatch.setattr(exporter, "_report_dropped_conf", lambda *args: None)
    monkeypatch.setattr(exporter, "_create_cluster_with_intel_fallback",
                        lambda client, payload, *args: client.create_cluster(payload))
    monkeypatch.setattr(exporter, "_attach_only_consolidated_requirements",
                        lambda *args: "requirements")
    result = report()
    source_graph = source_graph or graph()
    path = "/Users/child" if nested else "/Users/scoring"
    source_graph.notebooks[path] = NotebookNode(
        path, "run_dep" if nested else "task",
        referenced_by=["/Users/scoring" if nested else "job:123/task:scoring"],
        nb_dict={"cells": [{"cell_type": "code", "source": source}]},
    )
    if nested:
        source_graph.notebooks["/Users/scoring"] = NotebookNode(
            "/Users/scoring", "task", referenced_by=["job:123/task:scoring"],
            nb_dict={"cells": []},
        )
    exporter.phase3_create_clusters_and_workflows(
        aidp, dbx or SimpleNamespace(), source_graph, {}, uri_to_aidp or {},
        "/Workspace/export", "TEST",
        False, None, result, cfg=pack(tmp_path), lib_repository_dir=REPO,
    )
    return result


@pytest.mark.parametrize("nested", [False, True])
def test_phase3_ray_source_attaches_script_only_to_owner(tmp_path, monkeypatch, nested):
    aidp = InitAidp()
    result = run_phase3(tmp_path, monkeypatch, aidp,
                        source="import ray\nray.init()\n", nested=nested)
    by_name = {item["displayName"]: item for item in aidp.created}
    assert by_name["TEST_scoring"]["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}]
    assert by_name["TEST_other"]["clusterRuntimeConfig"]["initScripts"] == []
    assert result.jobs[0].status == "SKIPPED_EXISTS"
    assert any("ray-cluster-init-script" in line for line in result.errors)


def test_ray_script_missing_fails_before_cluster_creation(tmp_path, monkeypatch):
    aidp = InitAidp()
    aidp.script_available = False
    result = run_phase3(tmp_path, monkeypatch, aidp, source="ray.init()\n")
    assert not aidp.created
    assert result.jobs[0].status == "FAILED"
    assert "Required Ray init script not found" in result.jobs[0].failure_reason


def test_ray_text_in_comment_does_not_attach(tmp_path, monkeypatch):
    aidp = InitAidp()
    result = run_phase3(tmp_path, monkeypatch, aidp, source="# ray.init()\n")
    assert result.jobs[0].status == "SKIPPED_EXISTS"
    assert all(not item["clusterRuntimeConfig"]["initScripts"] for item in aidp.created)


def test_phase3_import_only_ray_init_attaches_to_owner(tmp_path, monkeypatch):
    aidp = InitAidp()
    result = run_phase3(
        tmp_path, monkeypatch, aidp,
        source=("import ray\n"
                "ray.init(address='auto', ignore_reinit_error=True, "
                "_redis_password='legacy')\n"),
    )
    by_name = {item["displayName"]: item for item in aidp.created}
    assert by_name["TEST_scoring"]["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}]
    assert by_name["TEST_other"]["clusterRuntimeConfig"]["initScripts"] == []
    assert result.jobs[0].status == "SKIPPED_EXISTS"


def test_for_each_ray_notebook_uses_nested_job_cluster(tmp_path, monkeypatch):
    aidp = InitAidp()
    source_graph = graph()
    settings = source_graph.jobs["123"].job_def["settings"]
    settings["tasks"][0] = {
        "task_key": "scoring",
        "for_each_task": {
            "inputs": "[]",
            "task": {
                "job_cluster_key": "scoring",
                "notebook_task": {"notebook_path": "/Users/foreach-ray"},
            },
        },
    }
    source_graph.notebooks["/Users/foreach-ray"] = NotebookNode(
        "/Users/foreach-ray", "task",
        referenced_by=["job:123/task:scoring/for_each"],
        nb_dict={"cells": [{"cell_type": "code", "source": "ray.init()\n"}]},
    )

    result = run_phase3(
        tmp_path, monkeypatch, aidp, source="print('unused')\n",
        source_graph=source_graph,
    )

    by_name = {item["displayName"]: item for item in aidp.created}
    assert by_name["TEST_scoring"]["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}
    ]
    assert result.jobs[0].status == "SKIPPED_EXISTS"


def test_task_level_new_cluster_gets_ray_script(tmp_path, monkeypatch):
    aidp = InitAidp()
    source_graph = graph()
    settings = source_graph.jobs["123"].job_def["settings"]
    settings["tasks"][0].pop("job_cluster_key")
    settings["tasks"][0]["new_cluster"] = {"num_workers": 1}
    requirement_uri = "/dbx_export_generated/resolved_requirements_task.txt"
    source_graph.files[requirement_uri] = FileNode(
        requirement_uri, "requirements",
        referenced_by=["job:123/task_cluster:scoring"], content=b"ray==2.55.0\n",
    )

    result = run_phase3(
        tmp_path, monkeypatch, aidp, source="ray.init()\n",
        source_graph=source_graph,
        uri_to_aidp={requirement_uri: "/Workspace/requirements.txt"},
    )

    by_name = {item["displayName"]: item for item in aidp.created}
    task_cluster = by_name["TEST_job_scoring_cluster"]
    assert task_cluster["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}
    ]
    assert result.jobs[0].status == "SKIPPED_EXISTS"


@pytest.mark.parametrize("scripts,valid", [
    ([{"destination": SCRIPT}], True),
    ([], False),
    ([{"destination": "/Workspace/old/bootstrap.sh"}], False),
    ([{"destination": SCRIPT}, {"destination": "/Workspace/extra.sh"}], False),
])
def test_reused_task_level_cluster_requires_ray_script(tmp_path, monkeypatch, scripts, valid):
    aidp = InitAidp()
    aidp.existing = scripts
    source_graph = graph()
    settings = source_graph.jobs["123"].job_def["settings"]
    settings["tasks"][0].pop("job_cluster_key")
    settings["tasks"][0]["new_cluster"] = {"num_workers": 1}
    requirement_uri = "/dbx_export_generated/resolved_requirements_task.txt"
    source_graph.files[requirement_uri] = FileNode(
        requirement_uri, "requirements",
        referenced_by=["job:123/task_cluster:scoring"], content=b"ray==2.55.0\n",
    )

    result = run_phase3(
        tmp_path, monkeypatch, aidp, source="ray.init()\n",
        source_graph=source_graph,
        uri_to_aidp={requirement_uri: "/Workspace/requirements.txt"},
    )

    assert result.jobs[0].status == ("SKIPPED_EXISTS" if valid else "FAILED")
    assert any("ERROR ray-cluster-init-script" in line
               for line in result.errors) is not valid


def test_existing_cluster_task_gets_ray_script(tmp_path, monkeypatch):
    aidp = InitAidp()
    source_graph = graph()
    settings = source_graph.jobs["123"].job_def["settings"]
    settings["tasks"][0].pop("job_cluster_key")
    settings["tasks"][0]["existing_cluster_id"] = "dbx-existing"
    dbx = SimpleNamespace(get_cluster=lambda key: {
        "cluster_name": "source-existing",
        "num_workers": 1,
        "libraries": [],
    })

    result = run_phase3(
        tmp_path, monkeypatch, aidp, source="ray.init()\n",
        source_graph=source_graph, dbx=dbx,
    )

    by_name = {item["displayName"]: item for item in aidp.created}
    existing_cluster = by_name["TEST_source-existing"]
    assert existing_cluster["clusterRuntimeConfig"]["initScripts"] == [
        {"destination": SCRIPT}
    ]
    assert result.jobs[0].status == "SKIPPED_EXISTS"


@pytest.mark.parametrize("scripts,valid", [
    ([{"destination": SCRIPT}], True),
    ([], False),
    ([{"destination": "/Workspace/old/bootstrap.sh"}], False),
    ([{"destination": SCRIPT}, {"destination": "/Workspace/extra.sh"}], False),
])
def test_reused_existing_cluster_requires_ray_script(tmp_path, monkeypatch, scripts, valid):
    aidp = InitAidp()
    aidp.existing = scripts
    source_graph = graph()
    settings = source_graph.jobs["123"].job_def["settings"]
    settings["tasks"][0].pop("job_cluster_key")
    settings["tasks"][0]["existing_cluster_id"] = "dbx-existing"
    dbx = SimpleNamespace(get_cluster=lambda key: {
        "cluster_name": "source-existing",
        "num_workers": 1,
        "libraries": [],
    })

    result = run_phase3(
        tmp_path, monkeypatch, aidp, source="ray.init()\n",
        source_graph=source_graph, dbx=dbx,
    )

    assert result.jobs[0].status == ("SKIPPED_EXISTS" if valid else "FAILED")
    assert any("ERROR ray-cluster-init-script" in line
               for line in result.errors) is not valid


def test_map_style_ray_conversion_does_not_require_head_script(tmp_path, monkeypatch):
    aidp = InitAidp()
    source = ("import ray\nray.init(address='auto')\n"
              "def execute_func(items):\n    return items\n"
              "execute_func([1])\n")
    result = run_phase3(tmp_path, monkeypatch, aidp, source=source)
    assert result.jobs[0].status == "SKIPPED_EXISTS"
    assert all(not item["clusterRuntimeConfig"]["initScripts"] for item in aidp.created)


def test_reused_cluster_requires_exact_script_and_attachment_is_idempotent(tmp_path):
    aidp = InitAidp()
    contracts = prepare_ray_cluster_init_script_rules(
        aidp, graph(), pack(tmp_path), print, report(),
    )
    assert contracts == {}
    rule = {"destination": SCRIPT}
    aidp.existing = [{"destination": SCRIPT}]
    verify_existing_ray_init_script(aidp, "cluster", rule)
    aidp.existing = [{"destination": "/Workspace/old/bootstrap.sh"}]
    with pytest.raises(ValueError, match="does not have the required"):
        verify_existing_ray_init_script(aidp, "cluster", rule)
    aidp.existing = [
        {"destination": SCRIPT},
        {"destination": "/Workspace/extra/bootstrap.sh"},
    ]
    with pytest.raises(ValueError, match="does not have the required"):
        verify_existing_ray_init_script(aidp, "cluster", rule)
    payload = {"clusterRuntimeConfig": {"initScripts": []}}
    attach_ray_init_script(payload, rule)
    attach_ray_init_script(payload, rule)
    assert payload["clusterRuntimeConfig"]["initScripts"] == [{"destination": SCRIPT}]

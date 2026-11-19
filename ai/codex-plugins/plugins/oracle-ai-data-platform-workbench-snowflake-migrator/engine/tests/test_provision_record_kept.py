"""An executed push never drops what an earlier push allocated.

Found in review. `provision` wrote provision_result.json wholesale after
every executed run, and teardown reads only that file. Two documented
paths lost the clusters this migration had created:

  (a) an executed re-run without --reuse-existing halts on workspace
      name_taken before any key is recorded; the record it wrote named no
      workspace, and teardown then printed "provision never ran for real,
      so this migration allocated nothing to terminate", 0/0 verified,
      exit 0 -- while the created cluster stayed ACTIVE;
  (b) the README step-4 plan push carries no --warehouse-clusters, so it
      recorded `warehouse_clusters: []`, and teardown reported "1/1 stop
      verified" while every warehouse cluster kept running and billing.

A halted push that recorded no workspace now goes to a side file and the
earlier record is kept; a push into the same (or another) workspace
carries the earlier record's created clusters forward as
`earlier_allocations`. teardown reached with an executed record that names
no workspace says it cannot tell what was allocated, and exits 1.
"""
import json

import pytest

import snowmig
from target import provisioning
from target.provisioning import carry_forward
from target.teardown import render_teardown, teardown
from test_teardown_provenance import World

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)


def _cli(tmp_path, monkeypatch, world):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)

    def run(*argv):
        return snowmig.main([argv[0], "--datalake-ocid", OCID,
                             "--out-dir", str(tmp_path), *argv[1:]])
    return run


def _provision(run, *extra):
    return run("provision", "--workspace-name", "acme", "--skip-libraries",
               "--execute", *extra)


def _record(tmp_path):
    return json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))


def test_a_halted_re_run_does_not_replace_the_record_teardown_reads(
        tmp_path, monkeypatch, capsys):
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert _provision(run) == 0
    assert _provision(run) == 1, "no --reuse-existing: halts on name_taken"
    rec = _record(tmp_path)
    assert rec["cluster"]["key"] == "cl-migration_assets", \
        "the record teardown reads still names the cluster it created"
    halted = json.loads((tmp_path / "provision_result.halted.json")
                        .read_text(encoding="utf-8"))
    assert ("workspace", "name_taken") in {(s["step"], s["action"])
                                           for s in halted["steps"]}
    assert "kept" in capsys.readouterr().err.lower()

    assert run("teardown", "--execute") == 0
    assert ("stop_cluster", "cl-migration_assets") in world.destructive


def test_the_plan_push_keeps_the_warehouse_clusters_for_teardown(
        tmp_path, monkeypatch):
    (tmp_path / "warehouses.json").write_text(json.dumps({"warehouses": [
        {"name": "ETL_WH", "size": "X-Small"},
        {"name": "BI_WH", "size": "Small"}]}), encoding="utf-8")
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert _provision(run, "--warehouse-clusters") == 0
    assert _provision(run, "--reuse-existing", "--plan-label", "FULL") == 0
    rec = _record(tmp_path)
    assert rec["warehouse_clusters"] == []
    earlier = {a["key"] for a in rec["earlier_allocations"]}
    assert earlier == {"cl-etl", "cl-bi"}
    assert "earlier push" in (tmp_path / "PROVISION.md").read_text(
        encoding="utf-8").lower()

    assert run("teardown") == 0            # dry run
    targets = {s["cluster"] for s in json.loads(
        (tmp_path / "teardown_result.json").read_text())["steps"]}
    assert targets == {"cl-migration_assets", "cl-etl", "cl-bi"}
    assert run("teardown", "--execute") == 0
    assert {c for _, c in world.destructive} == targets


def test_allocations_in_another_workspace_are_carried_with_their_own_key():
    first = {"dry_run": False, "run": "20260101T000000Z",
             "workspace": {"key": "ws-a", "created": True},
             "cluster": {"name": "migration_assets", "key": "cl-a",
                         "created": True}, "warehouse_clusters": []}
    second = {"dry_run": False, "run": "20260102T000000Z",
              "workspace": {"key": "ws-b", "created": True},
              "cluster": {"name": "migration_assets", "key": "cl-b",
                          "created": True}, "warehouse_clusters": []}
    carry_forward(second, first)
    assert second["earlier_allocations"] == [{
        "kind": "cluster", "key": "cl-a", "name": "migration_assets",
        "role": "migration cluster", "workspace": "ws-a", "created": True,
        "created_run": "20260101T000000Z"}]
    res = teardown(None, second, action="stop", execute=False)
    assert {(s["cluster"], s["workspace"]) for s in res["steps"]} == {
        ("cl-b", "ws-b"), ("cl-a", "ws-a")}
    # And a third push keeps them all: nothing is ever dropped.
    third = {"dry_run": False, "run": "20260103T000000Z",
             "workspace": {"key": "ws-b"},
             "cluster": {"name": "migration_assets", "key": "cl-b",
                         "created": False}, "warehouse_clusters": []}
    carry_forward(third, second)
    assert third["cluster"]["created"] is True
    assert [a["key"] for a in third["earlier_allocations"]] == ["cl-a"]


def test_an_executed_record_naming_no_workspace_is_not_called_empty(
        tmp_path, monkeypatch):
    rec = {"dry_run": False, "workspace": {"requested": "acme"},
           "cluster": {"name": "migration_assets"},
           "steps": [{"step": "workspace", "action": "name_taken",
                      "verified": False, "detail": "acme"}]}
    res = teardown(None, rec, action="stop", execute=True)
    assert "allocated nothing" not in res["note"]
    assert "cannot tell" in res["note"] and res["unknown"] is True
    assert "cannot tell" in render_teardown(res)
    (tmp_path / "provision_result.json").write_text(json.dumps(rec))
    run = _cli(tmp_path, monkeypatch, World())
    assert run("teardown", "--execute") == 1

"""A cluster whose create was accepted but never listed is not "nothing".

Found in review. teardown selected only records with a `key`. A warehouse
cluster whose create was accepted (the 202 came back; creates are async)
but never appeared in the listing was recorded with key None and step
`create_requested` -- and teardown dropped it with no step and no note. The
CLI computed its denominator from the steps, so it printed "1/1 cluster(s)
stop verified" (or 0/0) and exited 0 while that cluster, which most likely
exists, kept running and billing.

It is now a failed step that names the cluster and says to look it up in
the console before terminating it; it counts, so the CLI exits 1. It is
never picked by name: that is how a teardown takes somebody else's compute.
"""
import json

import snowmig
from target.provisioning import carry_forward
from target.teardown import render_teardown, teardown
from test_teardown import Fake

PROV = {"dry_run": False, "workspace": {"key": "ws"},
        "cluster": {"name": "migration_assets", "key": "mc", "created": True},
        "warehouse_clusters": [{"warehouse": "COMPUTE_WH", "name": "compute",
                                "key": None, "created": False,
                                "create_requested": True}]}


def test_a_requested_cluster_without_a_key_is_a_failed_step():
    call = Fake()
    res = teardown(call, PROV, action="stop", execute=True, delays=(0,))
    unkeyed = [s for s in res["steps"] if s.get("cluster") is None]
    assert len(unkeyed) == 1
    step = unkeyed[0]
    assert step["verified"] is False and step["name"] == "compute"
    detail = step["detail"].lower()
    assert "look it up" in detail and "console" in detail
    assert ("stop_cluster", None) not in call.calls
    assert "compute" in render_teardown(res)
    dry = teardown(None, PROV, action="stop", execute=False)
    assert any(s.get("cluster") is None and s["verified"] is False
               for s in dry["steps"])


def test_the_cli_counts_it_and_exits_1(tmp_path, monkeypatch, capsys):
    (tmp_path / "provision_result.json").write_text(json.dumps(PROV))
    call = Fake()
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: call)
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)
    rc = snowmig.main(["teardown", "--out-dir", str(tmp_path), "--execute",
                       "--datalake-ocid", "ocid1.aidataplatform.oc1.iad.aaaafake"])
    assert rc == 1
    assert "1/2" in capsys.readouterr().out


def test_a_legacy_record_is_read_from_its_create_requested_step():
    legacy = {"dry_run": False, "workspace": {"key": "ws"},
              "cluster": {"name": "migration_assets", "key": "mc"},
              "warehouse_clusters": [{"warehouse": "COMPUTE_WH",
                                      "name": "compute", "key": None}],
              "steps": [{"step": "cluster", "action": "created",
                         "verified": True, "detail": "migration_assets"},
                        {"step": "warehouse-cluster",
                         "action": "create_requested", "verified": False,
                         "detail": "COMPUTE_WH -> compute — accepted, but "
                                   "it never became visible"}]}
    res = teardown(None, legacy, action="stop", execute=False)
    assert [s["name"] for s in res["steps"] if s["cluster"] is None] == [
        "compute"]


def test_a_later_push_does_not_drop_the_unkeyed_request():
    later = {"dry_run": False, "workspace": {"key": "ws"},
             "cluster": {"name": "migration_assets", "key": "mc",
                         "created": False}, "warehouse_clusters": []}
    carry_forward(later, PROV)
    assert [(a["name"], a["key"], a.get("create_requested"))
            for a in later["earlier_allocations"]] == [
        ("compute", None, True)]
    res = teardown(None, later, action="stop", execute=False)
    assert any(s["cluster"] is None and s["name"] == "compute"
               for s in res["steps"])

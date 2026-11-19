"""A record written before provenance does not prove "not this migration's".

Found in review. provision_result.json files written before `created` was
recorded carry no provenance field, so teardown reads their steps. The
documented plan push is `--reuse-existing` into the migration's own
workspace, and it records the migration's own cluster as `reused`. Such a
record has no `created` step, so teardown listed the cluster as "not this
migration's, left alone", returned no steps, and the CLI printed "0/0
cluster(s) stop verified" and exited 0 while the cluster stayed ACTIVE.

A keyed cluster in a record with no `created` field and no `created` step
now has UNKNOWN provenance. It is never stopped or deleted (a key is still
not proof). It is a failed step that says the record predates provenance
and asks the operator to confirm in the console, so the CLI exits 1. The
billing report lists it apart and bills nothing. A later push keeps it
unknown. The one legacy case that stays "left alone" is a
`uses_existing` step, the cluster `compute.warehouse_clusters: existing`
maps the warehouses to: nothing was created there.
"""
import json

import snowmig
from report.resources import build_resources, render_resources_section
from target.provisioning import carry_forward
from target.teardown import render_teardown, teardown
from test_teardown import Fake

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"

# What the documented `--reuse-existing` plan push wrote before provenance:
# the migration's own cluster, found by name, recorded as reused.
LEGACY_RE_PUSH = {
    "dry_run": False, "workspace": {"key": "ws-fake", "name": "acme"},
    "cluster": {"name": "migration_assets", "key": "cl-m"},
    "warehouse_clusters": [],
    "steps": [{"step": "workspace", "action": "reused", "verified": True,
               "detail": "ws-fake"},
              {"step": "cluster", "action": "reused", "verified": True,
               "detail": "cl-m"}]}


def _unknown(res):
    return [s for s in res["steps"] if s["action"] == "provenance_unknown"]


def test_a_legacy_re_push_record_does_not_leave_its_cluster_alone():
    call = Fake(states={"cl-m": "ACTIVE"})
    res = teardown(call, LEGACY_RE_PUSH, action="delete", execute=True,
                   delays=(0,))
    assert not [c for c in call.calls
                if c[0] in ("stop_cluster", "delete_cluster")], \
        "a key is not proof: the cluster is never touched"
    assert res["left_alone"] == [], \
        "the record does not show it is somebody else's either"
    [step] = _unknown(res)
    assert step["cluster"] == "cl-m" and step["verified"] is False
    detail = step["detail"].lower()
    assert "predates" in detail and "console" in detail
    assert res["verified"] == 0 and len(res["steps"]) == 1
    md = render_teardown(res)
    assert "cl-m" in md and "provenance_unknown" in md
    dry = teardown(None, LEGACY_RE_PUSH, action="stop", execute=False)
    assert [s["cluster"] for s in _unknown(dry)] == ["cl-m"]
    assert not [s for s in dry["steps"] if s["action"] == "would stop"]


def test_the_cli_exits_1_instead_of_printing_0_of_0(tmp_path, monkeypatch,
                                                     capsys):
    (tmp_path / "provision_result.json").write_text(
        json.dumps(LEGACY_RE_PUSH))
    call = Fake(states={"cl-m": "ACTIVE"})
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: call)
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)
    rc = snowmig.main(["teardown", "--out-dir", str(tmp_path), "--execute",
                       "--datalake-ocid", OCID])
    captured = capsys.readouterr()
    assert rc == 1
    assert "0/1" in captured.out and "0/0" not in captured.out
    assert "cl-m" in captured.err and "predates" in captured.err
    assert snowmig.main(["teardown", "--out-dir", str(tmp_path),
                         "--datalake-ocid", OCID]) == 0
    dry = capsys.readouterr()
    assert "would stop 0 cluster(s)" in dry.out, \
        "the dry run does not count it as a cluster it would stop"
    assert "cl-m" in dry.err


def test_a_legacy_created_step_is_still_torn_down_and_existing_left_alone():
    legacy = {"dry_run": False, "workspace": {"key": "ws-fake"},
              "cluster": {"name": "migration_assets", "key": "cl-m"},
              "warehouse_clusters": [
                  {"warehouse": "A_WH", "name": "a", "key": "cl-shared"}],
              "steps": [{"step": "cluster", "action": "created",
                         "verified": True, "detail": "migration_assets"},
                        {"step": "warehouse-cluster",
                         "action": "uses_existing", "verified": True,
                         "detail": "A_WH -> existing cluster cl-shared "
                                   "(not created, not resized)"}]}
    res = teardown(None, legacy, action="stop", execute=False)
    assert [(s["cluster"], s["action"]) for s in res["steps"]] == [
        ("cl-m", "would stop")]
    assert [c["cluster"] for c in res["left_alone"]] == ["cl-shared"]


def test_the_billing_report_lists_it_apart_and_bills_nothing(tmp_path):
    (tmp_path / "provision_result.json").write_text(
        json.dumps(LEGACY_RE_PUSH))
    res = build_resources(tmp_path)
    assert not [r for r in res["resources"] if r["kind"] == "cluster"]
    assert not [r for r in res["not_allocated"] if r["kind"] == "cluster"], \
        "not proven to be somebody else's either"
    assert [r["key"] for r in res["provenance_unknown"]] == ["cl-m"]
    md = "\n".join(render_resources_section(res))
    assert "cl-m" in md and "provenance unknown" in md.lower()


def test_a_later_push_keeps_it_unknown():
    later = {"dry_run": False, "run": "r2", "workspace": {"key": "ws-fake"},
             "cluster": {"name": "migration_assets", "key": "cl-m",
                         "created": False},
             "warehouse_clusters": [], "steps": []}
    carry_forward(later, LEGACY_RE_PUSH)
    res = teardown(None, later, action="stop", execute=False)
    assert [s["cluster"] for s in _unknown(res)] == ["cl-m"]
    assert res["left_alone"] == []

    elsewhere = {"dry_run": False, "run": "r2",
                 "workspace": {"key": "ws-fake"},
                 "cluster": {"name": "other", "key": "cl-o", "created": True},
                 "warehouse_clusters": [], "steps": []}
    carry_forward(elsewhere, LEGACY_RE_PUSH)
    res = teardown(None, elsewhere, action="stop", execute=False)
    assert [s["cluster"] for s in _unknown(res)] == ["cl-m"], \
        "a push that does not re-record it keeps it under earlier_allocations"
    assert [s["cluster"] for s in res["steps"]
            if s["action"] == "would stop"] == ["cl-o"]

"""The billing report reads a teardown the way teardown recorded it.

Found in review. build_resources stored `step.get('state') or
step.get('action')`; a verified delete leaves state None, so the stored
value was the action word 'deleted', which its own stopped set
({STOPPED, INACTIVE, TERMINATED}) did not contain. After a verified
`teardown --action delete`, SUMMARY.md and PHASES.md said both deleted
clusters were "Still accruing now ... compute while ACTIVE", contradicting
TEARDOWN.md. And the release time came only from teardown log rows with
exit code 0, while teardown exits 1 when any target is unverified: after a
partial teardown the cluster verified STOPPED was described as running "to
now (still running)", its OCPU-hours growing on every rebuild.

A verified step now releases its cluster whatever the run's exit code,
with the time teardown stamped on the step; a deleted cluster is shown as
deleted; and nothing in the stopped set is ever "still running".
"""
import json

from report.resources import build_resources, render_resources_section
from report.tokens import record_stage_run
from target.teardown import teardown
from test_resources import PROV


class Clusters:
    def __init__(self, refuse=()):
        self.states = {"cl-1": "ACTIVE", "wc-1": "ACTIVE"}
        self.refuse = set(refuse)

    def __call__(self, op, **kw):
        if op == "list_clusters":
            return {"items": [{"key": k, "state": v}
                              for k, v in self.states.items()]}
        if kw.get("cluster") in self.refuse:
            raise RuntimeError("403 NotAuthorized")
        if op == "stop_cluster":
            self.states[kw["cluster"]] = "STOPPED"
        if op == "delete_cluster":
            self.states.pop(kw["cluster"], None)
        return {}


def _out(tmp_path, td, exit_code):
    (tmp_path / "provision_result.json").write_text(json.dumps(PROV))
    record_stage_run(tmp_path, "provision", "2026-09-24T19:22:00+00:00",
                     "2026-09-24T19:23:00+00:00", 0, None)
    (tmp_path / "teardown_result.json").write_text(json.dumps(td))
    record_stage_run(tmp_path, "teardown", "2026-09-24T21:30:00+00:00",
                     "2026-09-24T21:35:00+00:00", exit_code, None)
    return tmp_path


def test_deleted_clusters_are_not_still_accruing(tmp_path, monkeypatch):
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)
    td = teardown(Clusters(), PROV, action="delete", execute=True,
                  delays=(0,))
    assert all(s["action"] == "deleted" and s["verified"]
               for s in td["steps"])
    res = build_resources(_out(tmp_path, td, 0))
    assert not [r for r in res["accruing_now"] if r["kind"] == "cluster"]
    clusters = {r["key"]: r for r in res["resources"]
                if r["kind"] == "cluster"}
    assert clusters["cl-1"]["state"] == "DELETED"
    md = "\n".join(render_resources_section(res))
    assert "still running" not in md
    accruing = md[md.index("**Still accruing now:**"):]
    assert "migration_assets" not in accruing.split("\n")[0]


def test_a_partial_teardown_still_releases_what_it_stopped(tmp_path,
                                                          monkeypatch):
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)
    td = teardown(Clusters(refuse={"wc-1"}), PROV, action="stop",
                  execute=True, delays=(0,))
    by = {s["cluster"]: s for s in td["steps"]}
    assert by["cl-1"]["verified"] is True and by["cl-1"]["at"]
    assert by["wc-1"]["verified"] is False
    res = build_resources(_out(tmp_path, td, 1))   # teardown exited 1
    clusters = {r["key"]: r for r in res["resources"]
                if r["kind"] == "cluster"}
    assert clusters["cl-1"]["state"] == "STOPPED"
    assert clusters["cl-1"]["released_at"] == by["cl-1"]["at"]
    assert clusters["wc-1"]["released_at"] is None
    md = "\n".join(render_resources_section(res))
    line = next(x for x in md.splitlines()
                if "Compute exposure" in x and "`migration_assets`" in x)
    assert "still running" not in line


def test_a_stopped_cluster_with_no_release_time_is_not_running(tmp_path):
    """A teardown_result written before steps were stamped, from a run whose
    log row is missing: the state still decides, the clock does not."""
    td = {"dry_run": False, "action": "stop",
          "steps": [{"cluster": "cl-1", "action": "stopped",
                     "verified": True, "state": "STOPPED"}]}
    (tmp_path / "provision_result.json").write_text(json.dumps(PROV))
    (tmp_path / "teardown_result.json").write_text(json.dumps(td))
    record_stage_run(tmp_path, "provision", "2026-09-24T19:22:00+00:00",
                     "2026-09-24T19:23:00+00:00", 0, None)
    res = build_resources(tmp_path)
    md = "\n".join(render_resources_section(res))
    assert "cl-1" not in {r["key"] for r in res["accruing_now"]}
    line = next(x for x in md.splitlines()
                if "Compute exposure" in x and "`migration_assets`" in x)
    assert "still running" not in line

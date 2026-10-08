"""Terminating the compute a migration allocated, and nothing else.

`teardown` reads provision_result.json, so it can only reach clusters this
migration created: the migration cluster and any warehouse clusters. The
workspace (scripts, plans, report/output), the catalogs (the migration's
output) and the jobs (the registered S11 copy scripts) are kept. `stop` is
the default and reversible; `delete` is asked for. Dry run unless --execute,
and every cluster is read back until it is stopped or gone.
"""
import json

import pytest

from migration_config import ConfigError, teardown_block
from target.teardown import teardown

# `created: true` is the provenance teardown acts on (see
# test_teardown_provenance.py); a key alone proves nothing.
PROV = {"dry_run": False,
        "workspace": {"key": "ws"},
        "cluster": {"name": "migration_assets", "key": "mc", "created": True},
        "warehouse_clusters": [{"warehouse": "COMPUTE_WH", "name": "compute",
                                "key": "wc", "created": True},
                               {"warehouse": "X_WH", "name": "x", "key": None,
                                "created": False}]}


class Fake:
    """Clusters change state when told to; `lag` polls before they settle."""

    def __init__(self, states=None, lag=0, refuse=None):
        self.states = dict(states or {"mc": "ACTIVE", "wc": "ACTIVE",
                                      "other": "ACTIVE"})
        self.calls, self.lag, self.pending, self.refuse = [], lag, {}, refuse

    def __call__(self, op, **kw):
        self.calls.append((op, kw.get("cluster")))
        if op == "list_clusters":
            for key, (target, left) in list(self.pending.items()):
                if left <= 0:
                    if target is None:
                        self.states.pop(key, None)
                    else:
                        self.states[key] = target
                    del self.pending[key]
                else:
                    self.pending[key] = (target, left - 1)
            return {"items": [{"key": k, "state": v}
                              for k, v in self.states.items()]}
        if self.refuse and op == self.refuse[0]:
            raise RuntimeError(self.refuse[1])
        if op == "stop_cluster":
            self.pending[kw["cluster"]] = ("STOPPED", self.lag)
        if op == "delete_cluster":
            self.pending[kw["cluster"]] = (None, self.lag)
        return {}


def test_only_the_migrations_own_clusters_are_targeted():
    call = Fake()
    res = teardown(call, PROV, action="stop", execute=True, delays=(0, 0))
    touched = {c for op, c in call.calls if op == "stop_cluster"}
    assert touched == {"mc", "wc"}, "never a cluster provision did not create"
    assert call.states["other"] == "ACTIVE"


def test_a_dry_run_changes_nothing():
    call = Fake()
    res = teardown(call, PROV, action="stop", execute=False)
    assert res["dry_run"] is True and call.calls == []
    assert {s["action"] for s in res["steps"]} == {"would stop"}


def test_stop_is_read_back_until_the_cluster_is_stopped():
    call = Fake(lag=1)
    res = teardown(call, PROV, action="stop", execute=True, delays=(0, 0, 0))
    mc = next(s for s in res["steps"] if s["cluster"] == "mc")
    assert mc["action"] == "stopped" and mc["verified"] is True
    assert res["verified"] == 2


def test_delete_is_read_back_until_the_cluster_is_gone():
    call = Fake()
    res = teardown(call, PROV, action="delete", execute=True, delays=(0, 0))
    assert "mc" not in call.states and "wc" not in call.states
    assert all(s["verified"] for s in res["steps"] if s["cluster"])


def test_a_cluster_that_never_settles_is_not_called_terminated():
    call = Fake(lag=99)
    res = teardown(call, PROV, action="stop", execute=True, delays=(0,))
    assert all(s["action"] == "stop_requested" and s["verified"] is False
               for s in res["steps"] if s["cluster"])


def test_an_already_stopped_cluster_is_done_not_an_error():
    call = Fake(states={"mc": "STOPPED", "wc": "ACTIVE"},
                refuse=None)
    res = teardown(call, PROV, action="stop", execute=True, delays=(0, 0))
    mc = next(s for s in res["steps"] if s["cluster"] == "mc")
    assert mc["action"] == "already_stopped" and mc["verified"] is True
    assert ("stop_cluster", "mc") not in call.calls


def test_a_refused_terminate_is_recorded_and_the_rest_continue():
    call = Fake(refuse=("stop_cluster", "400 InvalidParameter"))
    res = teardown(call, PROV, action="stop", execute=True, delays=(0,))
    assert all(s["action"] == "failed" for s in res["steps"] if s["cluster"])
    assert len([s for s in res["steps"] if s["cluster"]]) == 2


def test_a_dry_run_provision_leaves_nothing_to_terminate():
    res = teardown(Fake(), {"dry_run": True}, action="stop", execute=True)
    assert res["steps"] == [] and "nothing" in res["note"].lower()


def test_what_is_kept_is_said():
    res = teardown(Fake(), PROV, action="stop", execute=False)
    kept = " ".join(res["kept"]).lower()
    for thing in ("workspace", "catalog", "job"):
        assert thing in kept


def test_the_action_is_configurable_and_validated():
    assert teardown_block({}) == {"action": "stop"}
    assert teardown_block({"teardown": {"action": "delete"}})["action"] == "delete"
    with pytest.raises(ConfigError):
        teardown_block({"teardown": {"action": "nuke"}})


def test_teardown_is_a_writing_phase_after_the_structure():
    from report.stages import STAGES, RUNS_ON
    t = next(s for s in STAGES if s["stage"] == "teardown")
    assert t["writes"] is True and t["runs_on"] == RUNS_ON["control_plane"]
    assert ["structure-workflow", "deploy"] in t["requires"]
    assert STAGES[-1]["stage"] == "teardown"


def test_the_cli_is_a_dry_run_by_default(tmp_path, monkeypatch):
    import snowmig
    (tmp_path / "provision_result.json").write_text(json.dumps(PROV))
    call = Fake()
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: call)
    assert snowmig.main(["teardown", "--out-dir", str(tmp_path),
                         "--datalake-ocid", "ocid1.aidataplatform.oc1.iad.a"]) == 0
    assert call.calls == []
    assert json.loads((tmp_path / "teardown_result.json").read_text())["dry_run"]

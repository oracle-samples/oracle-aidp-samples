"""The default (compute) teardown says out loud when the Snowflake credential
`provision --source-config` placed is still on the workspace.

The compute scope never removes the credential -- the copy jobs need it,
and stopping the clusters is reversible -- so an operator who runs the
default teardown and stops there would otherwise leave a credential every
workspace member can read. Executed, the objects are looked for on the
workspace (the record is not updated when `--scope credential` removes
them, so the record alone would warn about a credential already gone);
in a dry run they are reported as on record, unchecked. The warning never
changes the exit code: the clusters are what this scope is judged on.
"""
import json

import pytest

import snowmig
from target.teardown import render_teardown, teardown

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"
CRED = "backup-snowflake-migration/plan/snowmig-config.json"
KEY = "backup-snowflake-migration/plan/snowmig-config.key_path"


def _prov(**over):
    prov = {"dry_run": False, "datalake_ocid": OCID,
            "workspace": {"name": "lab_ws", "key": "ws-1", "created": True},
            "cluster": {"name": "lab_cl", "key": "cl-1", "created": True},
            "credential_objects": [KEY, CRED]}
    prov.update(over)
    return prov


class Lake:
    """One cluster, and the workspace objects in `objects`."""

    def __init__(self, objects=(CRED, KEY), list_fails=None,
                 shape="relative"):
        self.objects = set(objects)
        self.state = "ACTIVE"
        self.list_fails = list_fails
        self.shape = shape
        self.ops: list[str] = []

    def _item(self, path):
        leaf = path.rsplit("/", 1)[-1]
        return {"relative": {"path": path},
                "absolute": {"path": "/Workspace/" + path},
                "leaf": {"displayName": leaf}}[self.shape]

    def __call__(self, op, **kw):
        self.ops.append(op)
        if op == "list_clusters":
            return {"items": [{"key": "cl-1", "state": self.state}]}
        if op == "stop_cluster":
            self.state = "STOPPED"
            return {}
        if op == "list_ws_objects":
            if self.list_fails:
                raise RuntimeError(self.list_fails)
            return {"items": [self._item(p) for p in sorted(self.objects)
                              if p.startswith(kw["path"])]}
        raise AssertionError(op)


def _stop(lake, prov=None, execute=True):
    return teardown(lake, prov or _prov(), action="stop", execute=execute,
                    delays=(), datalake_ocid=OCID)


def test_a_credential_still_listed_is_reported_after_the_clusters_stop():
    res = _stop(Lake())
    assert res["verified"] == len(res["steps"]) == 1
    left = {c["name"]: c["status"] for c in res["credential_left"]}
    assert left == {KEY: "on_workspace", CRED: "on_workspace"}
    md = render_teardown(res)
    assert "Snowflake credential is still on the workspace" in md
    assert "teardown --scope credential --execute" in md
    assert KEY in md and CRED in md
    # Prominent: before the cluster table, not after "Kept".
    assert md.index("still on the workspace") < md.index("| Cluster |")


def test_a_credential_already_removed_is_not_reported():
    lake = Lake(objects=())
    res = _stop(lake)
    assert res["credential_left"] == []
    assert "still on the workspace" not in render_teardown(res)
    assert "list_ws_objects" in lake.ops


@pytest.mark.parametrize("shape", ["relative", "absolute", "leaf"])
def test_it_is_found_whatever_shape_the_listing_names_it_in(shape):
    # Matched the way provision reads its own uploads back: the path, its
    # leaf, or the display name. A miss here would read as "removed".
    res = _stop(Lake(shape=shape))
    assert {c["status"] for c in res["credential_left"]} == {"on_workspace"}
    assert len(res["credential_left"]) == 2


def test_an_object_listed_as_deleted_is_gone():
    class Deleted(Lake):
        def _item(self, path):
            return {"path": path, "lifecycleState": "DELETED"}
    assert _stop(Deleted())["credential_left"] == []


def test_only_what_is_still_listed_is_reported():
    res = _stop(Lake(objects=(KEY,)))
    assert [c["name"] for c in res["credential_left"]] == [KEY]


def test_a_dry_run_reports_it_as_on_record_and_looks_nothing_up():
    lake = Lake()
    res = _stop(lake, execute=False)
    assert lake.ops == []
    assert {c["status"] for c in res["credential_left"]} == {"on_record"}
    md = render_teardown(res)
    assert "teardown --scope credential --execute" in md
    assert "not looked for" in md


def test_a_listing_that_fails_is_unknown_never_gone():
    res = _stop(Lake(list_fails="503 Service Unavailable"))
    assert {c["status"] for c in res["credential_left"]} == {"unknown"}
    assert "503" in res["credential_left"][0]["detail"]
    assert "could not be looked for" in render_teardown(res)


def test_unconfirmed_uploads_are_looked_for_too():
    prov = _prov(credential_objects=[], credential_unconfirmed=[CRED])
    res = _stop(Lake(objects=(CRED,)), prov)
    assert [(c["name"], c["status"]) for c in res["credential_left"]] == [
        (CRED, "on_workspace")]


def test_a_credential_on_another_workspace_is_named_not_looked_for():
    prov = _prov(credential_objects=[], earlier_credential_objects=[
        {"path": CRED, "workspace": "old-ws"}])
    lake = Lake()
    res = _stop(lake, prov)
    assert "list_ws_objects" not in lake.ops
    (only,) = res["credential_left"]
    assert only["status"] == "elsewhere" and only["workspace"] == "old-ws"
    md = render_teardown(res)
    assert "old-ws" in md
    # `--scope credential` leaves these alone ("remove it there"), so it
    # must not be offered as what removes them.
    assert "--scope credential" not in md
    assert "on that workspace" in md


def test_a_record_for_another_platform_is_not_looked_up_here():
    lake = Lake()
    res = teardown(lake, _prov(datalake_ocid="ocid1.other"), action="stop",
                   execute=True, delays=(), datalake_ocid=OCID)
    assert "list_ws_objects" not in lake.ops
    assert {c["status"] for c in res["credential_left"]} == {"unknown"}


def test_no_credential_on_record_means_no_lookup_and_no_warning():
    lake = Lake()
    res = _stop(lake, _prov(credential_objects=[]))
    assert res["credential_left"] == [] and "list_ws_objects" not in lake.ops
    assert "credential" not in render_teardown(res).lower()


def test_a_dry_run_provision_placed_nothing():
    res = _stop(Lake(), _prov(dry_run=True))
    assert not res.get("credential_left")


def test_the_cli_warns_on_stderr_and_keeps_exit_0(tmp_path, monkeypatch,
                                                   capsys):
    (tmp_path / "provision_result.json").write_text(json.dumps(_prov()))
    lake = Lake()
    monkeypatch.setattr("target.provisioning.make_provision_call",
                        lambda ocid, **k: lake)
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)
    rc = snowmig.main(["teardown", "--out-dir", str(tmp_path),
                       "--datalake-ocid", OCID, "--execute"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "SNOWFLAKE CREDENTIAL STILL ON THE WORKSPACE" in err
    assert CRED in err and "teardown --scope credential --execute" in err
    saved = json.loads((tmp_path / "teardown_result.json").read_text())
    assert len(saved["credential_left"]) == 2


def test_the_cli_dry_run_warns_too(tmp_path, monkeypatch, capsys):
    (tmp_path / "provision_result.json").write_text(json.dumps(_prov()))
    rc = snowmig.main(["teardown", "--out-dir", str(tmp_path),
                       "--datalake-ocid", OCID])
    assert rc == 0
    assert "SNOWFLAKE CREDENTIAL" in capsys.readouterr().err

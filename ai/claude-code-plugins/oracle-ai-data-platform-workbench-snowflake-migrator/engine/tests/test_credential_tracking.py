"""Every credential this migration places on a workspace stays tracked, and
an inherited credential path is checked before it is baked or announced.

Found in review. `credential_objects` was rebuilt on every push from the
REQUEST -- this push's object, or the inherited one -- filled before the
upload and never corrected after a failed one, and never merged with the
earlier record. So:

  * a re-push with --source-config pointing at a file with another stem
    left plan/<old>.json on the mount, still holding a readable credential,
    and no artifact recorded it any more;
  * when the first push's credential upload failed, the documented re-push
    exited 0, printed "holds the Snowflake connection block", and baked
    /Workspace/.../<stem>.json into every notebook although the file did
    not exist;
  * pushing the same out dir to a different aiDataPlatform under the same
    workspace name inherited the catalogs and the credential path.

Now a credential object is recorded only once its upload is read back;
earlier objects of the same workspace are kept (a superseded one flagged
"still holds the previous credential; remove it"); an inherited path is
listed on the workspace before it is used; and inheritance needs the same
aiDataPlatform and the same workspace key.
"""
import json

import pytest
import yaml

import snowmig
from target import provisioning
from target.provisioning import PLAN_FOLDER, SCRIPTS_FOLDER
from test_teardown_provenance import World

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"
OTHER_OCID = "ocid1.aidataplatform.oc1.iad.bbbbfake"
_FAKE_PASSWORD = "FAKE-PASSWORD-not-real-456"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def _cfg(tmp_path, stem):
    path = tmp_path / f"{stem}.yaml"
    path.write_text(yaml.safe_dump({"snowflake": {
        "account": "ACME-TEST", "user": "READER", "warehouse": "WH",
        "database": "DB", "auth": "password", "password": _FAKE_PASSWORD}}),
        encoding="utf-8")
    return path


def _cli(tmp_path, monkeypatch, world):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)

    def run(*extra, ocid=OCID):
        return snowmig.main(["provision", "--datalake-ocid", ocid,
                             "--workspace-name", "acme", "--skip-libraries",
                             "--out-dir", str(tmp_path / "out"), "--execute",
                             *extra])
    return run


def _record(tmp_path):
    return json.loads((tmp_path / "out" / "provision_result.json")
                      .read_text(encoding="utf-8"))


def _params(world, notebook="00_discover_snowflake.ipynb"):
    nb = json.loads(world.contents[f"{SCRIPTS_FOLDER}/{notebook}"]["body"])
    return "".join(nb["cells"][1]["source"])


def test_a_rotated_credential_keeps_the_old_object_on_the_record(
        tmp_path, monkeypatch):
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert run("--source-config", str(_cfg(tmp_path, "a"))) == 0
    assert run("--reuse-existing", "--refresh-notebooks",
               "--source-config", str(_cfg(tmp_path, "b"))) == 0
    rec = _record(tmp_path)
    old, new = f"{PLAN_FOLDER}/a.json", f"{PLAN_FOLDER}/b.json"
    assert old in world.contents, "the old copy is still on the mount"
    assert set(rec["credential_objects"]) == {old, new}
    assert rec["credential_superseded"] == [old]
    md = (tmp_path / "out" / "PROVISION.md").read_text(encoding="utf-8")
    assert old in md and new in md
    assert "still holds the previous credential" in md


class CredentialUploadFails(World):
    def __call__(self, operation, **kw):
        if (operation == "upload_ws_file"
                and kw.get("path", "").endswith("/a.json")):
            self.ops.append((operation, kw))
            raise RuntimeError("503 ServiceUnavailable")
        return super().__call__(operation, **kw)


def test_a_failed_credential_upload_is_not_inherited_as_if_it_landed(
        tmp_path, monkeypatch, capsys):
    world = CredentialUploadFails()
    run = _cli(tmp_path, monkeypatch, world)
    assert run("--source-config", str(_cfg(tmp_path, "a"))) == 1
    first = _record(tmp_path)
    assert first["credential_objects"] == [], \
        "a failed upload does not hold the credential as far as anyone knows"
    assert first["credential_unconfirmed"] == [f"{PLAN_FOLDER}/a.json"]
    capsys.readouterr()

    rc = run("--reuse-existing", "--refresh-notebooks")
    err = capsys.readouterr().err
    assert rc == 1, "the re-push names the missing credential and fails"
    assert "holds the Snowflake" not in err
    assert "source-config': '/Workspace" not in _params(world)
    rec = _record(tmp_path)
    step = next(s for s in rec["steps"] if s["step"] == "credential")
    assert step["verified"] is False and "a.json" in step["detail"]
    assert "--source-config" in step["detail"]
    assert rec["credential_missing"] == [f"{PLAN_FOLDER}/a.json"], \
        "the re-push records that the object it was told of is not there"
    assert rec["credential_objects"] == [] and not rec.get(
        "credential_unconfirmed"), "the listing settled it: not there"


def test_an_inherited_credential_that_is_there_is_used_and_recorded(
        tmp_path, monkeypatch, capsys):
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert run("--source-config", str(_cfg(tmp_path, "a")),
               "--target-catalog", "lake_dev") == 0
    capsys.readouterr()
    assert run("--reuse-existing", "--refresh-notebooks") == 0
    err = capsys.readouterr().err
    assert "holds" in err
    rec = _record(tmp_path)
    assert rec["credential_objects"] == [f"{PLAN_FOLDER}/a.json"]
    assert rec["target_catalog"] == "lake_dev"
    assert f"'source-config': '/Workspace/{PLAN_FOLDER}/a.json'" in \
        _params(world)


def test_another_ai_data_platform_inherits_nothing(tmp_path, monkeypatch):
    world = World()
    run = _cli(tmp_path, monkeypatch, world)
    assert run("--source-config", str(_cfg(tmp_path, "a")),
               "--target-catalog", "lake_dev") == 0
    elsewhere = World()
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: elsewhere)
    assert run("--reuse-existing", ocid=OTHER_OCID) == 0
    rec = _record(tmp_path)
    assert rec["datalake_ocid"] == OTHER_OCID
    assert rec["target_catalog"] is None
    assert rec["credential_objects"] == []
    assert "source-config': '/Workspace" not in _params(elsewhere)


def test_teardown_does_not_look_for_a_cluster_on_the_wrong_platform():
    """The DataLake OCID now on the record also guards teardown: a workspace
    key from one aiDataPlatform means nothing on another, and a cluster
    "not listed" there would read as already gone."""
    from target.teardown import teardown
    rec = {"dry_run": False, "datalake_ocid": OTHER_OCID,
           "workspace": {"key": "ws-b"},
           "cluster": {"name": "migration_assets", "key": "cl-b",
                       "created": True},
           "earlier_allocations": [{
               "kind": "cluster", "key": "cl-a", "name": "migration_assets",
               "role": "migration cluster", "workspace": "ws-a",
               "created": True, "datalake_ocid": OCID}]}
    world = World(clusters=("migration_assets",))
    world.clusters[0]["key"] = "cl-a"
    res = teardown(world, rec, action="stop", execute=True, delays=(0,),
                   datalake_ocid=OCID)
    by = {s["cluster"]: s for s in res["steps"]}
    assert by["cl-b"]["action"] == "not_reached"
    assert by["cl-b"]["verified"] is False
    assert OTHER_OCID in by["cl-b"]["detail"]
    assert by["cl-a"]["action"] == "stopped"

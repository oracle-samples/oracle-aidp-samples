"""`provision --source-config --reuse-existing` into a workspace this
migration did not create places the Snowflake credential where every
member of somebody else's workspace can read it (#175). That takes an
explicit `--allow-credential-on-adopted-workspace`.

Not refused: the runbook's own re-push (`--reuse-existing` into the
workspace an earlier push of this migration created, as the executed
provision_result.json proves), a workspace this run creates, and an
adoption that places no credential. The halt comes before the workspace
key is recorded, so nothing is created or uploaded and the earlier record
is kept.
"""
import json

import pytest

import snowmig
from target import provisioning
from target.provisioning import PLAN_FOLDER, provision, render_provision
from test_provisioning import Fake, _config, scripts  # noqa: F401 (fixture)

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"
FLAG = "--allow-credential-on-adopted-workspace"
CRED = f"{PLAN_FOLDER}/snowmig-config.json"


def _ours(**ws):
    """An executed record of this migration having created `acme`."""
    return {"dry_run": False, "datalake_ocid": OCID, "run": "r1",
            "workspace": {"name": "acme", "requested": "acme",
                          "key": "ws-acme", "created": True, **ws}}


def _push(fake, cfg, scripts, **kw):
    kw.setdefault("execute", True)
    return provision(call=fake, workspace_name="acme", scripts=scripts,
                     source_config=cfg, reuse_existing=True, delays=(),
                     datalake_ocid=OCID, **kw)


def _uploads(fake):
    return [kw["path"] for op, kw in fake.ops if op == "upload_ws_file"]


def test_an_adopted_workspace_is_refused_before_anything_is_written(
        scripts, tmp_path):
    fake = Fake(workspaces=("acme",))
    res = _push(fake, _config(tmp_path), scripts)
    halt = next(s for s in res["steps"] if s["step"] == "halt")
    assert halt["verified"] is False
    assert FLAG in halt["detail"] and "external-catalog" in halt["detail"]
    assert not res["workspace"].get("key"), "halted before the key"
    assert _uploads(fake) == [] and res["credential_objects"] == []
    assert not any(op.startswith("create_") for op, _ in fake.ops)


def test_the_runbook_re_push_into_this_migrations_workspace_goes_through(
        scripts, tmp_path):
    fake = Fake(workspaces=("acme",))
    res = _push(fake, _config(tmp_path), scripts, prior=_ours())
    assert not any(s["step"] == "halt" for s in res["steps"])
    assert CRED in res["credential_objects"]


def test_a_workspace_this_run_creates_is_ours(scripts, tmp_path):
    fake = Fake()
    res = _push(fake, _config(tmp_path), scripts)
    assert not any(s["step"] == "halt" for s in res["steps"])
    assert CRED in res["credential_objects"]


@pytest.mark.parametrize("prior", [
    _ours(created=False),                       # adopted by the earlier push
    _ours(key="ws-other"),                      # a different workspace
    {**_ours(), "datalake_ocid": "ocid1.aidataplatform.oc1.iad.other"},
    {**_ours(), "dry_run": True},               # never ran for real
])
def test_a_record_that_does_not_prove_ownership_is_refused(
        scripts, tmp_path, prior):
    fake = Fake(workspaces=("acme",))
    res = _push(fake, _config(tmp_path), scripts, prior=prior)
    assert any(s["step"] == "halt" for s in res["steps"])
    assert _uploads(fake) == []


def test_the_flag_places_it_and_says_so(scripts, tmp_path):
    fake = Fake(workspaces=("acme",))
    res = _push(fake, _config(tmp_path), scripts,
                allow_credential_on_adopted_workspace=True)
    assert not any(s["step"] == "halt" for s in res["steps"])
    assert CRED in res["credential_objects"]
    assert res["credential_on_adopted_workspace"] is True
    step = next(s for s in res["steps"] if s["step"] == "credential")
    assert step["action"] == "allowed_on_adopted_workspace"
    assert "adopted" in render_provision(res).lower()


def test_adopting_without_a_credential_is_unchanged(scripts, tmp_path):
    fake = Fake(workspaces=("acme",))
    res = _push(fake, None, scripts)
    assert not any(s["step"] == "halt" for s in res["steps"])
    assert res["workspace"]["key"] == "ws-acme"


def test_the_dry_run_says_the_execute_would_refuse(scripts, tmp_path):
    res = _push(None, _config(tmp_path), scripts, execute=False)
    step = next(s for s in res["steps"] if s["step"] == "credential")
    assert step["action"] == "would refuse on an adopted workspace"
    assert FLAG in step["detail"]


def test_the_dry_run_is_quiet_when_the_record_proves_ownership(scripts,
                                                               tmp_path):
    res = _push(None, _config(tmp_path), scripts, execute=False,
                prior=_ours())
    assert not any(s["step"] == "credential" for s in res["steps"])


def test_the_dry_run_with_the_flag_says_it_would_place_it(scripts, tmp_path):
    res = _push(None, _config(tmp_path), scripts, execute=False,
                allow_credential_on_adopted_workspace=True)
    step = next(s for s in res["steps"] if s["step"] == "credential")
    assert step["action"] == "would allow on an adopted workspace"


# --- through the CLI -----------------------------------------------------------

def _cli(tmp_path, monkeypatch, fake, *extra):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)
    return snowmig.main(["provision", "--datalake-ocid", OCID,
                         "--workspace-name", "acme", "--skip-libraries",
                         "--out-dir", str(tmp_path), "--execute",
                         "--reuse-existing", *extra])


def test_the_cli_exits_1_and_keeps_the_earlier_record(tmp_path, monkeypatch):
    earlier = {**_ours(key="ws-first", name="first", requested="first")}
    (tmp_path / "provision_result.json").write_text(json.dumps(earlier))
    fake = Fake(workspaces=("acme",))
    rc = _cli(tmp_path, monkeypatch, fake, "--source-config",
              str(_config(tmp_path)))
    assert rc == 1
    kept = json.loads((tmp_path / "provision_result.json").read_text())
    assert kept["workspace"]["key"] == "ws-first"
    assert (tmp_path / "provision_result.halted.json").is_file()
    assert _uploads(fake) == []


def test_the_cli_passes_the_flag_through(tmp_path, monkeypatch):
    fake = Fake(workspaces=("acme",))
    rc = _cli(tmp_path, monkeypatch, fake, "--source-config",
              str(_config(tmp_path)), FLAG)
    assert rc == 0
    res = json.loads((tmp_path / "provision_result.json").read_text())
    assert res["credential_on_adopted_workspace"] is True
    assert CRED in _uploads(fake)


def test_the_cli_says_it_on_stderr_dry_run_and_halt(tmp_path, monkeypatch,
                                                    capsys):
    cfg = str(_config(tmp_path))
    rc = snowmig.main(["provision", "--datalake-ocid", OCID,
                       "--workspace-name", "acme", "--skip-libraries",
                       "--out-dir", str(tmp_path / "dry"), "--reuse-existing",
                       "--source-config", cfg])
    assert rc == 0
    assert "ADOPTED WORKSPACE" in capsys.readouterr().err
    rc = _cli(tmp_path, monkeypatch, Fake(workspaces=("acme",)),
              "--source-config", cfg)
    assert rc == 1
    err = capsys.readouterr().err
    assert "ADOPTED WORKSPACE" in err and FLAG in err

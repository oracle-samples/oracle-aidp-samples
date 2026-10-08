"""`snowmig.py provision` end to end against a fake transport: what reaches
the operator -- exit code, provision_result.json, PROVISION.md, stderr --
when the environment does not cooperate.
"""
import json

import pytest

import snowmig
from target import provisioning
from target.provisioning import ProvisionTransportError
from test_provisioning import Fake


OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def _install(monkeypatch, fake):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)


def _provision(tmp_path, *extra):
    return snowmig.main(["provision", "--datalake-ocid", OCID,
                         "--workspace-name", "acme", "--skip-libraries",
                         "--out-dir", str(tmp_path), *extra])


class Always409(Fake):
    """The workspace is still CREATING; every cluster POST is a 409."""

    def __call__(self, operation, **kw):
        if operation == "create_cluster":
            self.ops.append((operation, kw))
            raise ProvisionTransportError(
                "create_cluster failed (exit 0): 409 Conflict ongoing "
                "operation")
        return super().__call__(operation, **kw)


def test_a_cluster_409_still_writes_the_provision_record(tmp_path,
                                                         monkeypatch):
    """The first live `provision --execute` on a fresh DataLake is expected
    to hit this. It used to exit 1 with a bare `error:` line and NO
    artifact, so the workspace it had just created was recorded nowhere."""
    fake = Always409()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--execute")
    assert rc == 1
    res = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert any(s["step"] == "workspace" and s["verified"] is True
               for s in res["steps"])
    assert res["workspace"]["key"] == "ws-acme"
    md = (tmp_path / "PROVISION.md").read_text(encoding="utf-8")
    assert "--reuse-existing" in md and "ws-acme" in md
    assert fake.workspaces, "the workspace really was created server-side"


class JobsListFails(Fake):
    """The workspace and cluster are created, then the session token expires
    on the job listing."""

    def __call__(self, operation, **kw):
        if operation == "list_jobs":
            self.ops.append((operation, kw))
            raise ProvisionTransportError(
                "list_jobs failed (exit 1): 401 NotAuthenticated")
        return super().__call__(operation, **kw)


def test_a_failed_job_listing_still_writes_the_provision_record(tmp_path,
                                                                monkeypatch):
    """Same loss as the 409 above, one step later: the job listing was the
    last bare call past the cluster, so a 401 there exited 1 with one
    `error:` line and no artifact."""
    fake = JobsListFails()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--execute")
    assert rc == 1
    res = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert any(s["step"] == "workspace" and s["verified"] is True
               for s in res["steps"])
    assert res["workspace"]["key"] == "ws-acme"
    md = (tmp_path / "PROVISION.md").read_text(encoding="utf-8")
    assert "--reuse-existing" in md
    assert fake.workspaces and fake.clusters, "both were created server-side"


# --- --source-config through the CLI -----------------------------------------

_FAKE_PASSWORD = "FAKE-PASSWORD-not-real-123"


def _source_config(tmp_path, **snowflake):
    import yaml
    block = {"account": "ACME-TEST", "user": "READER", "warehouse": "WH",
             "database": "DB", "auth": "password", "password": _FAKE_PASSWORD}
    block.update(snowflake)
    block = {k: v for k, v in block.items() if v is not None}
    cfg = tmp_path / "snowmig-config.yaml"
    cfg.write_text(yaml.safe_dump({"snowflake": block,
                                   "aidp": {"datalake_ocid": OCID}}),
                   encoding="utf-8")
    return cfg


def test_the_dry_run_says_out_loud_that_a_credential_would_be_placed(
        tmp_path, monkeypatch, capsys):
    cfg = _source_config(tmp_path)
    rc = _provision(tmp_path, "--source-config", str(cfg))
    assert rc == 0
    err = capsys.readouterr().err
    assert "CREDENTIAL ON THE WORKSPACE MOUNT" in err
    assert "backup-snowflake-migration/plan/snowmig-config.json" in err
    md = (tmp_path / "PROVISION.md").read_text(encoding="utf-8")
    assert "Credential placed on the workspace" in md
    assert _FAKE_PASSWORD not in md and _FAKE_PASSWORD not in err


def test_the_execute_path_uploads_only_the_block_and_says_so(
        tmp_path, monkeypatch, capsys):
    cfg = _source_config(tmp_path)
    fake = Fake()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--source-config", str(cfg), "--execute")
    assert rc == 0
    assert "backup-snowflake-migration/plan/snowmig-config.json" in fake.contents
    assert "backup-snowflake-migration/plan/snowmig-config.yaml" not in fake.contents
    body = json.loads(fake.contents[
        "backup-snowflake-migration/plan/snowmig-config.json"]["body"])
    assert set(body) == {"snowflake"} and "datalake_ocid" not in json.dumps(body)
    err = capsys.readouterr().err
    assert "CREDENTIAL ON THE WORKSPACE MOUNT" in err and "holds" in err


def test_a_path_form_secret_is_refused_by_the_cli_before_anything_is_written(
        tmp_path, monkeypatch, capsys):
    pem = tmp_path / "rsa_key.p8"
    pem.write_text("-----BEGIN PRIVATE KEY-----\nFAKE\n", encoding="utf-8")
    cfg = _source_config(tmp_path, auth="keypair", key_path=str(pem),
                         password=None)
    fake = Fake()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--source-config", str(cfg), "--execute")
    assert rc == 1
    err = capsys.readouterr().err
    assert "key_path" in err and "inline" in err.lower()
    assert fake.ops == []
    assert not (tmp_path / "provision_result.json").exists()


# --- --reuse-existing / --refresh-notebooks through the CLI ------------------

def _seeded_workspace():
    from target.provisioning import JOB_SPECS, SCRIPTS_FOLDER
    fake = Fake(workspaces=("acme",), clusters=("migration_assets",),
                jobs=tuple(s["name"] for s in JOB_SPECS))
    for spec in JOB_SPECS:
        fake.contents[f'{SCRIPTS_FOLDER}/{spec["notebook"]}'] = {
            "type": "NOTEBOOK", "body": "console-edited"}
    return fake


def test_reuse_existing_keeps_the_notebooks_and_provision_md_lists_them(
        tmp_path, monkeypatch):
    fake = _seeded_workspace()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--execute", "--reuse-existing")
    assert rc == 0
    assert all(v["body"] == "console-edited" for k, v in fake.contents.items()
               if k.endswith(".ipynb")
               and not k.endswith("diagnose_environment.ipynb"))
    md = (tmp_path / "PROVISION.md").read_text(encoding="utf-8")
    assert "Stage notebooks kept as found" in md
    assert "02_copy_schema.ipynb" in md and "--refresh-notebooks" in md


def test_refresh_notebooks_is_the_explicit_way_to_overwrite(tmp_path,
                                                            monkeypatch):
    fake = _seeded_workspace()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--execute", "--reuse-existing",
                    "--refresh-notebooks")
    assert rc == 0
    assert not any(v["body"] == "console-edited"
                   for k, v in fake.contents.items() if k.endswith(".ipynb"))
    md = (tmp_path / "PROVISION.md").read_text(encoding="utf-8")
    assert "kept as found" not in md and "OVERWRITTEN" in md

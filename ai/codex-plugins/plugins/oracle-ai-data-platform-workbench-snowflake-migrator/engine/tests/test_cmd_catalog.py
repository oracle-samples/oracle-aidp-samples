"""The `catalog` stage driven through main(), transport faked, no network.

The registration NAME is the thing under test: it may come from `--catalog`
or from the config's `aidp:` block, and which config key applies depends on
the catalog type. `external_catalog` names the Snowflake source's EXTERNAL
catalog; `catalog` names the INTERNAL target. They are never swapped for
one another, because ensure_catalog reuses ANY catalog carrying the name.
"""
import json
import subprocess

import pytest

import snowmig

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"

AIDP = {"datalake_ocid": OCID, "workspace": "ws-fake",
        "cluster_id": "cl-fake", "external_catalog": "my_ext",
        "catalog": "my_target"}


def _cfg(tmp_path, aidp: dict) -> str:
    lines = ["snowflake:", "  account: ORG-ACC", "  user: SVC",
             "  warehouse: WH", "  database: SALES_DB", "  role: READER",
             "  auth: password", "  password: not-a-real-password", "aidp:"]
    lines += [f"  {k}: {v}" for k, v in aidp.items()]
    p = tmp_path / "cfg.yaml"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


class Recorder:
    """Catalog transport double: empty DataLake, creates become visible."""

    def __init__(self):
        self.ops: list[tuple] = []
        self.catalogs: list[dict] = []

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        if operation == "list_catalogs":
            return {"items": list(self.catalogs)}
        if operation == "create_catalog":
            body = kw["body"]
            self.catalogs.append({"displayName": body["displayName"],
                                  "key": "k", "catalogType": body["catalogType"]})
            return {}
        raise AssertionError(f"unexpected operation {operation}")

    def creates(self) -> list[dict]:
        return [kw["body"] for op, kw in self.ops if op == "create_catalog"]


@pytest.fixture()
def rec(monkeypatch):
    recorder = Recorder()
    monkeypatch.setattr(snowmig, "detect_backend", lambda: "oci_raw")
    monkeypatch.setattr(snowmig, "make_call",
                        lambda target, *, backend, **kw: recorder)

    def no_subprocess(*a, **k):
        raise AssertionError("no CLI may be invoked from this test")
    monkeypatch.setattr(subprocess, "run", no_subprocess)
    return recorder


def _result(tmp_path) -> dict:
    return json.loads((tmp_path / "catalog_result.json").read_text(encoding="utf-8"))


def test_execute_registers_the_external_name_from_the_config(tmp_path, rec):
    cfg = _cfg(tmp_path, AIDP)
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path),
                       "--execute"])
    assert rc == 0
    bodies = rec.creates()
    assert len(bodies) == 1
    assert bodies[0]["displayName"] == "my_ext"
    assert "connectionDetails" in bodies[0]
    assert _result(tmp_path)["catalog"] == "my_ext"
    assert _result(tmp_path)["dry_run"] is False


def test_standard_execute_uses_the_internal_target_name(tmp_path, rec):
    cfg = _cfg(tmp_path, AIDP)
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path),
                       "--catalog-type", "standard", "--execute"])
    assert rc == 0
    bodies = rec.creates()
    assert len(bodies) == 1
    assert bodies[0]["displayName"] == "my_target"
    assert "connectionDetails" not in bodies[0]


def test_the_flag_wins_over_the_config_name(tmp_path, rec):
    cfg = _cfg(tmp_path, AIDP)
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path),
                       "--catalog", "flag_name", "--execute"])
    assert rc == 0
    assert [b["displayName"] for b in rec.creates()] == ["flag_name"]
    assert _result(tmp_path)["catalog"] == "flag_name"


def test_external_without_a_name_is_a_clean_error(tmp_path, rec, capsys):
    # `catalog:` names the INTERNAL target and is NOT borrowed for the
    # EXTERNAL registration: a fallback would register the source under the
    # target's name, and the later standard step would then "reuse" it.
    aidp = {k: v for k, v in AIDP.items() if k != "external_catalog"}
    cfg = _cfg(tmp_path, aidp)
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path),
                       "--execute"])
    assert rc == 1
    err = capsys.readouterr().err
    assert err.startswith("error:")
    assert "--catalog" in err and "aidp.external_catalog" in err
    assert "Traceback" not in err
    assert rec.ops == [], "the refusal must come before any API call"


def test_dry_run_records_the_config_name_not_none(tmp_path, rec, capsys):
    cfg = _cfg(tmp_path, AIDP)
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path)])
    assert rc == 0
    assert _result(tmp_path)["catalog"] == "my_ext"
    assert "catalog my_ext: dry run" in capsys.readouterr().out
    assert rec.ops == []


def test_external_execute_needs_no_internal_catalog_in_the_config(tmp_path, rec):
    # The example config sets `external_catalog:` before `catalog:` exists;
    # the unrelated INTERNAL coordinate must not trip MissingTarget here.
    aidp = {k: v for k, v in AIDP.items() if k != "catalog"}
    cfg = _cfg(tmp_path, aidp)
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path),
                       "--execute"])
    assert rc == 0
    assert [b["displayName"] for b in rec.creates()] == ["my_ext"]


def test_dry_run_refuses_to_overwrite_an_executed_record(tmp_path, rec, capsys):
    cfg = _cfg(tmp_path, AIDP)
    (tmp_path / "catalog_result.json").write_text(json.dumps(
        {"dry_run": False, "catalog": "my_ext", "catalog_type": "EXTERNAL",
         "action": "created", "key": "k", "verified": True}), encoding="utf-8")
    rc = snowmig.main(["catalog", "--config", cfg, "--out-dir", str(tmp_path)])
    assert rc == 1
    err = capsys.readouterr().err
    assert "EXECUTED" in err and "catalog_result.json" in err
    assert _result(tmp_path)["action"] == "created"
    assert _result(tmp_path)["dry_run"] is False


def test_a_re_run_reuses_the_catalog_it_created_and_teardown_still_owns_it(
        tmp_path, rec):
    """Run twice: the second run finds the catalog the first created. It
    is reused (the ledger proves it is ours) and stays a teardown target."""
    from target.teardown import catalogs_created
    cfg = _cfg(tmp_path, AIDP)
    argv = ["catalog", "--config", cfg, "--out-dir", str(tmp_path), "--execute"]
    assert snowmig.main(argv) == 0
    assert snowmig.main(argv) == 0
    assert len(rec.creates()) == 1
    assert _result(tmp_path)["action"] == "reused"
    ledger = [json.loads(line) for line in
              (tmp_path / "resources.jsonl").read_text().splitlines()]
    assert [r["action"] for r in ledger] == ["created", "reused"]
    assert all(r["datalake_ocid"] == OCID for r in ledger)
    assert [c["name"] for c in catalogs_created(ledger)] == ["my_ext"]


def test_a_catalog_someone_else_created_is_refused(tmp_path, rec, capsys):
    rec.catalogs.append({"displayName": "MY_EXT", "key": "theirs",
                         "catalogType": "EXTERNAL"})
    cfg = _cfg(tmp_path, AIDP)
    argv = ["catalog", "--config", cfg, "--out-dir", str(tmp_path), "--execute"]
    assert snowmig.main(argv) == 1
    assert "--reuse-existing" in capsys.readouterr().err
    assert rec.creates() == []
    assert not (tmp_path / "resources.jsonl").exists()
    assert snowmig.main([*argv, "--reuse-existing"]) == 0
    assert _result(tmp_path)["action"] == "reused"


def test_a_ledger_row_from_another_datalake_proves_nothing(tmp_path, rec):
    rec.catalogs.append({"displayName": "my_ext", "key": "k",
                         "catalogType": "EXTERNAL"})
    (tmp_path / "resources.jsonl").write_text(json.dumps(
        {"kind": "catalog", "name": "my_ext", "key": "k", "type": "EXTERNAL",
         "action": "created", "datalake_ocid": "ocid1.other"}) + "\n")
    cfg = _cfg(tmp_path, AIDP)
    assert snowmig.main(["catalog", "--config", cfg, "--out-dir",
                         str(tmp_path), "--execute"]) == 1

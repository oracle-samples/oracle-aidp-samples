"""teardown --scope credential | all: remove the credential, or undo the
migration -- only what the record proves it created, each delete read back.
The transport is faked; the request shapes are pinned against what ran live
on 2026-09-29.
"""
import json

import pytest

import snowmig
from target import provisioning
from target.provision_api import build_provision_command
from target.teardown import (catalogs_created, render_teardown,
                             teardown_everything)

OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"
CRED = "backup-snowflake-migration/plan/snowmig-config.json"


def _prov(**over):
    prov = {
        "dry_run": False, "datalake_ocid": OCID,
        "workspace": {"name": "lab_ws", "key": "ws-1", "created": True},
        "cluster": {"name": "lab_cl", "key": "cl-1", "created": True},
        "credential_objects": [CRED],
        "steps": [
            {"step": "job", "action": "reused", "verified": True,
             "detail": "snowmig_00_discover (stage notebook kept)"},
            {"step": "job", "action": "reused", "verified": True,
             "detail": "snowmig_01_structure (stage notebook kept)"},
            {"step": "job", "action": "stale_deleted", "verified": True,
             "detail": "snowmig_02_copy_old: its schema is not in this plan; "
                       "deleted, and gone from the listing"},
        ],
        "copy_jobs": [{"schema": "SALES", "job": "snowmig_02_copy_sales",
                       "status": "reused"}],
        "deleted_copy_jobs": ["snowmig_02_copy_old"],
    }
    prov.update(over)
    return prov


LEDGER = [
    {"kind": "catalog", "name": "src", "key": "src", "type": "EXTERNAL",
     "action": "created"},
    {"kind": "catalog", "name": "tgt", "key": "tgt", "type": "INTERNAL",
     "action": "created"},
    {"kind": "catalog", "name": "theirs", "key": "theirs", "type": "INTERNAL",
     "action": "reused"},
]


class Lake:
    """A DataLake holding what `_prov()` and LEDGER describe."""

    def __init__(self, *, async_status="SUCCEEDED", sticky=()):
        self.objects = {CRED, "backup-snowflake-migration/plan/plan.json"}
        self.jobs = {"snowmig_00_discover": "j0", "snowmig_01_structure": "j1",
                     "snowmig_02_copy_sales": "j2", "someone_elses": "j9"}
        self.clusters = {"cl-1"}
        self.catalogs = {"src", "tgt", "theirs"}
        self.workspaces = {"ws-1"}
        self.async_status = async_status
        self.sticky = set(sticky)          # deletes accepted, never applied
        self.ops: list[tuple] = []

    def _gone(self, bucket, name):
        if name not in self.sticky:
            bucket.discard(name) if isinstance(bucket, set) else \
                bucket.pop(name, None)

    def __call__(self, op, **kw):
        self.ops.append((op, kw))
        if op == "list_ws_objects":
            return {"items": [{"path": p} for p in sorted(self.objects)
                              if p.startswith(kw["path"])]}
        if op == "delete_ws_object":
            self._gone(self.objects, kw["path"])
            return {}
        if op == "list_jobs":
            return {"items": [{"name": n, "key": k}
                              for n, k in self.jobs.items()]}
        if op == "delete_job":
            name = next(n for n, k in self.jobs.items() if k == kw["job_key"])
            self._gone(self.jobs, name)
            return {}
        if op == "list_clusters":
            return {"items": [{"key": k, "state": "ACTIVE"}
                              for k in sorted(self.clusters)]}
        if op == "delete_cluster":
            self._gone(self.clusters, kw["cluster"])
            return {}
        if op == "delete_catalog":
            self._gone(self.catalogs, kw["catalog"])
            return {"_headers": {"aidp-async-operation-key": "op-cat"}}
        if op == "list_catalogs":
            return {"items": [{"key": c} for c in sorted(self.catalogs)]}
        if op == "delete_workspace":
            self._gone(self.workspaces, "ws-1")
            return {"_headers": {"aidp-async-operation-key": "op-ws"}}
        if op == "list_workspaces":
            return {"items": [{"key": w} for w in sorted(self.workspaces)]}
        if op == "get_async_operation":
            return {"status": self.async_status}
        raise AssertionError(op)

    def deletes(self):
        return [op for op, _ in self.ops if op.startswith("delete_")]


def _run(lake, prov=None, **kw):
    kw.setdefault("scope", "all")
    kw.setdefault("execute", True)
    return teardown_everything(lake, prov or _prov(), ledger=LEDGER,
                               datalake_ocid=OCID, delays=(),
                               async_delays=(0,), sleep=lambda _s: None,
                               **kw)


def test_the_dry_run_lists_everything_in_order_and_touches_nothing():
    res = teardown_everything(None, _prov(), scope="all", execute=False,
                              ledger=LEDGER, include_data=True)
    kinds = [s["kind"] for s in res["steps"]]
    assert kinds == ["credential", "job", "job", "job", "cluster", "catalog",
                     "catalog", "workspace"]
    assert all(s["action"] == "would delete" for s in res["steps"])
    names = [s.get("name") for s in res["steps"]]
    assert "snowmig_02_copy_old" not in names      # already deleted
    assert "theirs" not in names                   # reused, not ours


def test_the_internal_catalog_is_kept_without_include_data():
    res = teardown_everything(None, _prov(), scope="all", execute=False,
                              ledger=LEDGER)
    assert "tgt" not in [s.get("name") for s in res["steps"]]
    assert any("tgt" in k and "--include-data" in k for k in res["kept"])


def test_everything_ours_is_deleted_and_read_back_workspace_last():
    lake = Lake()
    res = _run(lake, include_data=True)
    assert res["verified"] == len(res["steps"]) == 8
    assert lake.deletes()[0] == "delete_ws_object"
    assert lake.deletes()[-1] == "delete_workspace"
    assert "someone_elses" in lake.jobs and "theirs" in lake.catalogs
    forced = {kw["catalog"]: kw["forced"] for op, kw in lake.ops
              if op == "delete_catalog"}
    assert forced == {"src": False, "tgt": True}
    md = render_teardown(res)
    assert "# Teardown — everything this migration created" in md


def test_a_reused_workspace_is_kept_and_only_created_jobs_go():
    prov = _prov(workspace={"name": "lab_ws", "key": "ws-1",
                            "created": False})
    prov["steps"].append({"step": "job", "action": "created",
                          "verified": True, "detail": "snowmig_03_reconcile"})
    res = teardown_everything(None, prov, scope="all", execute=False,
                              ledger=LEDGER)
    jobs = [s["name"] for s in res["steps"] if s["kind"] == "job"]
    assert jobs == ["snowmig_03_reconcile"]
    assert "workspace" not in [s["kind"] for s in res["steps"]]
    assert any("did not create it" in k for k in res["kept"])


def test_a_job_the_carried_record_says_we_created_goes_on_a_reused_workspace():
    """A re-push records the job `reused`; `created_jobs` is still the
    proof a push of this migration created it."""
    prov = _prov(workspace={"name": "lab_ws", "key": "ws-1",
                            "created": False},
                 created_jobs=[{"name": "snowmig_02_copy_sales",
                                "key": "j-1"}])
    res = teardown_everything(None, prov, scope="all", execute=False,
                              ledger=LEDGER)
    jobs = [s["name"] for s in res["steps"] if s["kind"] == "job"]
    assert jobs == ["snowmig_02_copy_sales"]


def test_a_failed_async_delete_is_not_verified():
    res = _run(Lake(async_status="FAILED"), include_data=True)
    cat = next(s for s in res["steps"] if s.get("catalog") == "src")
    assert cat["verified"] is False and "FAILED" in cat["detail"]


def test_an_accepted_delete_still_listed_is_requested_not_done():
    res = _run(Lake(sticky={"snowmig_00_discover"}))
    job = next(s for s in res["steps"] if s.get("name") == "snowmig_00_discover")
    assert job["action"] == "delete_requested" and job["verified"] is False


def test_the_credential_scope_removes_only_the_credential():
    lake = Lake()
    res = _run(lake, scope="credential")
    assert lake.deletes() == ["delete_ws_object"]
    assert res["verified"] == 1 and CRED not in lake.objects
    assert "backup-snowflake-migration/plan/plan.json" in lake.objects


def test_a_record_for_another_platform_is_refused():
    res = teardown_everything(Lake(), _prov(datalake_ocid="ocid1.other"),
                              scope="all", execute=True, ledger=LEDGER,
                              datalake_ocid=OCID)
    assert res.get("unknown") and not res["steps"]


def test_only_catalogs_the_ledger_says_were_created_are_candidates():
    assert [c["catalog"] for c in catalogs_created(LEDGER)] == ["src", "tgt"]


def test_a_re_run_that_records_created_then_reused_still_owns_the_catalog():
    """`catalog --execute` run twice: the second run finds the catalog the
    first created and records `reused`. Teardown must still reach it."""
    ledger = [*LEDGER,
              {"kind": "catalog", "name": "tgt", "key": "tgt",
               "type": "INTERNAL", "action": "reused"},
              {"kind": "catalog", "name": "theirs", "key": "theirs",
               "type": "INTERNAL", "action": "reused"}]
    assert [c["catalog"] for c in catalogs_created(ledger)] == ["src", "tgt"]


# --- request shapes (live 2026-09-29) ------------------------------------------

def test_a_workspace_object_path_is_one_percent_encoded_segment():
    cmd = build_provision_command("oci_raw", "delete_ws_object", OCID,
                                  workspace="ws", path=CRED)
    assert cmd[:4] == ["oci", "raw-request", "--http-method", "DELETE"]
    assert cmd[5].endswith("/workspaces/ws/objects/backup-snowflake-migration"
                           "%2Fplan%2Fsnowmig-config.json")


def test_an_internal_catalog_delete_is_forced_by_the_aidp_cli():
    forced = build_provision_command("oci_raw", "delete_catalog", OCID,
                                     catalog="tgt", forced=True)
    assert forced[:4] == ["aidp", "catalog", "delete", "tgt"]
    assert "--is-forced" in forced
    plain = build_provision_command("oci_raw", "delete_catalog", OCID,
                                    catalog="src", forced=False)
    assert "--is-forced" not in plain


def test_the_workspace_delete_and_catalog_listing_uris():
    ws = build_provision_command("oci_raw", "delete_workspace", OCID,
                                 workspace="ws")
    assert ws[3] == "DELETE" and ws[5].endswith(f"/{OCID}/workspaces/ws")
    cats = build_provision_command("oci_raw", "list_catalogs", OCID)
    assert "/20240831/dataLakes/" in cats[5] and cats[5].endswith("/catalogs")


# --- CLI -----------------------------------------------------------------------

def _seed(tmp_path):
    (tmp_path / "provision_result.json").write_text(json.dumps(_prov()),
                                                    encoding="utf-8")
    (tmp_path / "resources.jsonl").write_text(
        "\n".join(json.dumps(r) for r in LEDGER) + "\n", encoding="utf-8")


def test_the_cli_scope_all_is_a_dry_run_by_default(tmp_path, capsys):
    _seed(tmp_path)
    rc = snowmig.main(["teardown", "--out-dir", str(tmp_path),
                       "--datalake-ocid", OCID, "--scope", "all"])
    assert rc == 0
    assert "would delete 7 object(s)" in capsys.readouterr().out
    res = json.loads((tmp_path / "teardown_result.json").read_text())
    assert res["dry_run"] is True and res["scope"] == "all"


def test_scope_all_with_action_stop_is_refused(tmp_path):
    _seed(tmp_path)
    with pytest.raises(snowmig.MissingTarget, match="contradicts"):
        snowmig.cmd_teardown(snowmig.build_parser().parse_args(
            ["teardown", "--out-dir", str(tmp_path), "--scope", "all",
             "--action", "stop"]))


def test_include_data_outside_scope_all_is_refused(tmp_path):
    _seed(tmp_path)
    with pytest.raises(snowmig.MissingTarget, match="--scope all"):
        snowmig.cmd_teardown(snowmig.build_parser().parse_args(
            ["teardown", "--out-dir", str(tmp_path), "--include-data"]))


def test_the_cli_executes_through_the_provision_transport(tmp_path,
                                                          monkeypatch):
    _seed(tmp_path)
    lake = Lake()
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: lake)
    import target.teardown as td
    monkeypatch.setattr(td.time, "sleep", lambda _s: None)
    rc = snowmig.main(["teardown", "--out-dir", str(tmp_path),
                       "--datalake-ocid", OCID, "--scope", "all",
                       "--include-data", "--execute"])
    assert rc == 0
    assert lake.workspaces == set() and lake.catalogs == {"theirs"}

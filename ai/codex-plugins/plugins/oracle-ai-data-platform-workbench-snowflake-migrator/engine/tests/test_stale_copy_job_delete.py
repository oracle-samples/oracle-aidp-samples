"""`provision --delete-stale-copy-jobs`: stale copy jobs go, read back.

A plan push reports a copy job registered for a schema the plan no longer
names (and the schemaless generic job) as stale, and deletes nothing: the
operator used to have to remove them in the console. `aidp workflow
delete-job` answers 204 live (2026-09-29), so on request provision deletes
them -- and records a job deleted only once it is gone from the listing.
Only a job this migration's records show it created is deleted: on a
reused workspace a `snowmig_02_copy_*` job may be someone else's.
"""
import pytest

from target import provisioning
from target.provision_api import build_provision_command
from target.provisioning import provision, render_provision
from test_provisioning import Fake

OCID = "ocid1.aidataplatform.oc1.iad.a"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


class Deletes(Fake):
    """`delete_job` removes the job, raises (`refuse`), or is accepted and
    changes nothing (`sticky`)."""

    def __init__(self, *, refuse=False, sticky=False, **kw):
        super().__init__(**kw)
        self.refuse, self.sticky = refuse, sticky
        self.deleted: list[str] = []

    def __call__(self, operation, **kw):
        if operation == "delete_job":
            self.ops.append((operation, kw))
            if self.refuse:
                raise RuntimeError("403 NotAuthorized")
            self.deleted.append(kw["job_key"])
            if not self.sticky:
                self.jobs = [j for j in self.jobs
                             if j["key"] != kw["job_key"]]
            return {}
        return super().__call__(operation, **kw)


def _push(fake, schemas, **kw):
    return provision(call=fake, workspace_name="acme", scripts=[],
                     execute=True, delays=(), copy_schemas=schemas,
                     reuse_existing=True, **kw)


def _reduced(fake, **kw):
    """First a push without a plan (generic job), then SALES and HR, then
    a plan reduced to SALES -- the push under test. Each push gets the
    record before it, as `provision` passes provision_result.json."""
    first = provision(call=fake, workspace_name="acme", scripts=[],
                      execute=True, delays=(), copy_schemas=[])
    second = _push(fake, ["SALES", "HR"], prior=first)
    return _push(fake, ["SALES"], prior=second, **kw)


def _jobs(fake):
    return {j["name"] for j in fake.jobs}


def test_the_delete_command_is_the_live_verified_one():
    cmd = build_provision_command("oci_raw", "delete_job", OCID,
                                  workspace="ws", job_key="k1")
    assert cmd[:5] == ["aidp", "workflow", "delete-job", "ws", "k1"]


def test_on_request_stale_and_superseded_copy_jobs_are_deleted(tmp_path):
    fake = Deletes()
    res = _reduced(fake, delete_stale_copy_jobs=True)
    assert "snowmig_02_copy_hr" not in _jobs(fake)
    assert "snowmig_02_copy_schema" not in _jobs(fake)
    assert "snowmig_02_copy_sales" in _jobs(fake)
    assert sorted(res["deleted_copy_jobs"]) == [
        "snowmig_02_copy_hr", "snowmig_02_copy_schema"]
    deleted = [s for s in res["steps"] if s["action"] == "stale_deleted"]
    assert len(deleted) == 2 and all(s["verified"] for s in deleted)
    assert not [s for s in res["steps"] if s["action"] == "stale"]
    md = render_provision(res)
    assert "deleted (--delete-stale-copy-jobs)" in md


def test_by_default_nothing_is_deleted():
    fake = Deletes()
    res = _reduced(fake)
    assert fake.deleted == []
    assert "snowmig_02_copy_hr" in _jobs(fake)
    assert res["deleted_copy_jobs"] == []


def test_a_refused_delete_leaves_a_failed_step(tmp_path):
    fake = Deletes(refuse=True)
    res = _reduced(fake, delete_stale_copy_jobs=True)
    assert "snowmig_02_copy_hr" in _jobs(fake)
    stale = [s for s in res["steps"] if s["action"] == "stale"
             and s["detail"].startswith("snowmig_02_copy_hr")]
    assert stale and stale[0]["verified"] is False
    assert "refused" in stale[0]["detail"]
    assert res["deleted_copy_jobs"] == []


def test_a_delete_still_listed_is_not_recorded_deleted():
    fake = Deletes(sticky=True)
    res = _reduced(fake, delete_stale_copy_jobs=True)
    assert res["deleted_copy_jobs"] == []
    pending = [s for s in res["steps"]
               if s["action"] == "stale_delete_requested"]
    assert pending and all(s["verified"] is None for s in pending)


def test_a_planned_job_listed_in_another_case_is_not_stale():
    fake = Deletes()
    provision(call=fake, workspace_name="acme", scripts=[], execute=True,
              delays=(), copy_schemas=["SALES"])
    for job in fake.jobs:
        if job["name"] == "snowmig_02_copy_sales":
            job["name"] = "SNOWMIG_02_COPY_SALES"
    res = _push(fake, ["SALES"], delete_stale_copy_jobs=True)
    assert res["stale_copy_jobs"] == []
    assert fake.deleted == []


def test_a_stale_job_this_migration_did_not_create_is_left_alone():
    """Someone else's copy job on the reused workspace matches the prefix
    and is in no plan of ours; it is reported, never deleted."""
    fake = Deletes()
    first = _push(fake, ["SALES"])
    fake.jobs.append({"key": "theirs-1", "name": "snowmig_02_copy_finance",
                      "displayName": "snowmig_02_copy_finance"})
    res = _push(fake, ["SALES"], prior=first, delete_stale_copy_jobs=True)
    assert fake.deleted == []
    assert "snowmig_02_copy_finance" in _jobs(fake)
    assert res["stale_copy_jobs"] == ["snowmig_02_copy_finance"]
    stale = [s for s in res["steps"] if s["action"] == "stale"]
    assert stale and "NOT deleted" in stale[0]["detail"]


def test_without_an_earlier_record_nothing_is_ours_to_delete():
    fake = Deletes()
    _push(fake, ["SALES", "HR"])
    res = _push(fake, ["SALES"], delete_stale_copy_jobs=True)
    assert fake.deleted == []
    assert res["deleted_copy_jobs"] == []


def test_ownership_survives_a_re_push_that_records_the_job_reused():
    """Push 2 finds HR and records it `reused`; push 3 must still know
    push 1 created it."""
    fake = Deletes()
    first = _push(fake, ["SALES", "HR"])
    second = _push(fake, ["SALES", "HR"], prior=first)
    assert not [s for s in second["steps"]
                if s["action"] == "created" and s["step"] == "job"]
    assert "snowmig_02_copy_hr" in {e["name"] for e in second["created_jobs"]}
    res = _push(fake, ["SALES"], prior=second, delete_stale_copy_jobs=True)
    assert res["deleted_copy_jobs"] == ["snowmig_02_copy_hr"]
    assert "snowmig_02_copy_hr" not in {e["name"] for e in res["created_jobs"]}


def test_a_recreated_job_under_our_name_but_another_key_is_not_ours():
    fake = Deletes()
    first = _push(fake, ["SALES", "HR"])
    for job in fake.jobs:
        if job["name"] == "snowmig_02_copy_hr":
            job["key"] = "someone-elses-key"
    res = _push(fake, ["SALES"], prior=first, delete_stale_copy_jobs=True)
    assert fake.deleted == []
    assert "snowmig_02_copy_hr" in _jobs(fake)


def test_a_record_of_another_workspace_proves_nothing():
    fake = Deletes()
    first = _push(fake, ["SALES", "HR"])
    other = {**first, "workspace": {**first["workspace"], "key": "ws-other"}}
    res = _push(fake, ["SALES"], prior=other, delete_stale_copy_jobs=True)
    assert fake.deleted == []
    assert res["created_jobs"] == []

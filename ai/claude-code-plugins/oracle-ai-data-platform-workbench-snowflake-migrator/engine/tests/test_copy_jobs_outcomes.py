"""The copy jobs PROVISION.md reports are the ones on the workspace.

Found in review, after the documented FULL -> S9 reduction -> REDUCED push
sequence:

  * snowmig_02_copy_hr and snowmig_02_copy_fin (task parameter schema=HR /
    FIN) and the parameterless generic job were still on the workspace,
    runnable, while provision_result.copy_jobs and PROVISION.md listed only
    SALES and the generic job was reported `not_created`;
  * `copy_jobs` was built from the job specs before any call, so a
    name_taken halt, a dry run or a failed 02 upload still printed
    "Registered, never run" over N jobs nobody registered;
  * a per-schema job reused on a plain --reuse-existing re-push said its
    stage notebook was "OVERWRITTEN ... console edits are gone" in the same
    run that listed 02_copy_schema.ipynb as kept.

A copy job on the workspace that the present plan does not name is now a
failed `stale` step and a PROVISION.md section (it stays runnable until
someone deletes it); the generic job that exists is `exists_superseded`;
each copy_jobs entry carries its outcome; and a reused job says what
really happened to its notebook.
"""
import pytest

from target import provisioning
from target.provisioning import provision, render_provision
from test_provisioning import Fake


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def _push(fake, schemas, **kw):
    return provision(call=fake, workspace_name="acme", scripts=[],
                     execute=True, delays=(), copy_schemas=schemas, **kw)


def _steps(res, action):
    return [s for s in res["steps"] if s["step"] == "job"
            and s["action"] == action]


def test_a_schema_reduced_out_of_the_plan_leaves_a_stale_job_said_out_loud():
    fake = Fake()
    _push(fake, [])
    _push(fake, ["SALES", "HR"], reuse_existing=True)
    res = _push(fake, ["SALES"], reuse_existing=True)
    names = {j["name"] for j in fake.jobs}
    assert "snowmig_02_copy_hr" in names, "nothing is deleted behind a push"
    stale = _steps(res, "stale")
    assert [s["detail"].split(" ")[0] for s in stale] == [
        "snowmig_02_copy_hr"]
    assert stale[0]["verified"] is False
    md = render_provision(res)
    assert "snowmig_02_copy_hr" in md and "not in this plan" in md.lower()
    generic = [s for s in res["steps"] if s["step"] == "job"
               and s["detail"].startswith("snowmig_02_copy_schema")]
    assert [s["action"] for s in generic] == ["exists_superseded"], \
        "the generic job exists, so it is not reported as not created"


def test_copy_jobs_are_not_called_registered_when_nothing_was():
    res = _push(Fake(workspaces=("acme",)), ["SALES"])   # name_taken halt
    assert "Registered" not in render_provision(res)
    assert [j["status"] for j in res["copy_jobs"]] == ["not registered"]
    dry = provision(call=None, workspace_name="acme", scripts=[],
                    copy_schemas=["SALES"])
    assert [j["status"] for j in dry["copy_jobs"]] == ["would register"]
    assert "Registered, never run" not in render_provision(dry)


def test_a_registered_copy_job_carries_its_outcome():
    res = _push(Fake(), ["SALES"])
    assert [(j["job"], j["status"]) for j in res["copy_jobs"]] == [
        ("snowmig_02_copy_sales", "created")]
    assert "Registered, never run" in render_provision(res)


def test_a_reused_copy_job_does_not_claim_its_kept_notebook_was_overwritten():
    fake = Fake()
    _push(fake, ["SALES"])
    res = _push(fake, ["SALES"], reuse_existing=True)
    assert "02_copy_schema.ipynb" in res["notebooks_kept"]
    reused = [s for s in _steps(res, "reused")
              if s["detail"].startswith("snowmig_02_copy_sales")]
    assert reused and "OVERWRITTEN" not in reused[0]["detail"]
    assert "kept" in reused[0]["detail"]

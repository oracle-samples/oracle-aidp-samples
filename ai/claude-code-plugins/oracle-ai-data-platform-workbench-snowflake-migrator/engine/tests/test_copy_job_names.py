"""A per-schema copy job never takes the name of a stage job.

Found in review. copy_job_specs named jobs `snowmig_02_copy_<schema>` and
never checked them against the stage job names, so a (quoted) Snowflake
schema named SCHEMA got `snowmig_02_copy_schema` -- the generic,
parameterless copy job. The dry run skipped that name (it is the generic
job, "not created" when there is a plan), so the approved preview did not
show SCHEMA's copy job; and on the plan re-push the parameterless generic
job was adopted as SCHEMA's workflow ("reused", verified), which can only
fail when run. A reused job's task parameters were never compared either.

Such a name is now disambiguated (`snowmig_02_copy_schema_schema`), and a
reused per-schema job whose listed task parameters name another schema is
refused, not adopted.
"""
import pytest

from target import provisioning
from target.provisioning import JOB_SPECS, copy_job_specs, provision
from test_provisioning import Fake


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


STAGE_JOBS = {s["name"] for s in JOB_SPECS}


def test_a_schema_named_schema_does_not_take_the_generic_job_name():
    [spec] = copy_job_specs(["SCHEMA"])
    assert spec["name"] not in STAGE_JOBS
    assert spec["name"] == "snowmig_02_copy_schema_schema"
    assert spec["task_parameters"] == {"schema": "SCHEMA"}


def test_a_disambiguated_name_that_still_collides_is_refused():
    with pytest.raises(ValueError, match="collide"):
        copy_job_specs(["SCHEMA", "SCHEMA_SCHEMA"])


def test_the_dry_run_previews_its_copy_job():
    res = provision(call=None, workspace_name="acme", scripts=[],
                    copy_schemas=["SCHEMA", "SALES"])
    previewed = {s["detail"] for s in res["steps"] if s["step"] == "job"}
    assert "snowmig_02_copy_schema_schema" in previewed
    assert "snowmig_02_copy_sales" in previewed


class ListsTasks(Fake):
    """A job listing that carries each job's task parameters."""

    def __init__(self, tasks, **kw):
        super().__init__(**kw)
        for job in self.jobs:
            if job["name"] in tasks:
                job["tasks"] = [{"parameters": [
                    {"name": "schema", "value": tasks[job["name"]]}]}]


def test_a_reused_copy_job_for_another_schema_is_refused():
    fake = ListsTasks({"snowmig_02_copy_sales": "HR"},
                      workspaces=("acme",), clusters=("migration_assets",),
                      jobs=("snowmig_02_copy_sales",))
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    execute=True, delays=(), reuse_existing=True,
                    copy_schemas=["SALES"])
    step = next(s for s in res["steps"] if s["step"] == "job"
                and s["detail"].startswith("snowmig_02_copy_sales"))
    assert step["action"] == "name_taken" and step["verified"] is False
    assert "HR" in step["detail"]
    assert [j["status"] for j in res["copy_jobs"]] != ["reused"]


def test_a_reused_copy_job_for_the_same_schema_is_adopted():
    fake = ListsTasks({"snowmig_02_copy_sales": "SALES"},
                      workspaces=("acme",), clusters=("migration_assets",),
                      jobs=("snowmig_02_copy_sales",))
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    execute=True, delays=(), reuse_existing=True,
                    copy_schemas=["SALES"])
    assert [j["status"] for j in res["copy_jobs"]] == ["reused"]

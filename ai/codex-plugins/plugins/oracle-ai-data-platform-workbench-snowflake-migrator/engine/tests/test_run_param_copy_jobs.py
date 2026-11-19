"""`run --param` advice and `provision --stage-param` for per-schema copy jobs.

Since the stage notebooks read a job TASK's parameters over their PARAMS
literals (live-verified through oidlUtils.parameters.getParameter), and
provision registers one `snowmig_02_copy_<schema>` job per schema, each
passing `schema` as a task parameter, three things were stale:

* The `run --param` refusal looked the stage up by exact job name only, so
  for every real copy job (`snowmig_02_copy_sales`) it fell back to an
  UNQUALIFIED `--stage-param mode=<value>` -- advice provision then
  refuses, because `mode` reaches 01 too -- and told the operator to bake a
  `schema` the job's own task parameter silently overrides.
* provision accepted `--stage-param schema=HR` next to per-schema copy
  jobs: the copy jobs still copied their own schemas (task parameter wins),
  01 was narrowed to HR, and PROVISION.md recorded `schema = HR` as written.
  A baked `tables` went into the ONE shared 02 notebook and narrowed every
  schema's copy job, leaving reconcile gaps.
* The refusal, `--param`/`--stage-param` help, README and the provision
  skill still said job parameters never reach a notebook, and "four
  parametrised jobs".
"""
import argparse
import pathlib

import pytest

import snowmig
from target.stage_notebooks import check_stage_params

ROOT = pathlib.Path(__file__).resolve().parents[2]
OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"


def _refusal(tmp_path, job, *params):
    args = argparse.Namespace(
        out_dir=str(tmp_path), datalake_ocid=OCID, workspace="ws-fake",
        cluster_id=None, catalog=None, backend=None, config=None, job=job,
        job_key="job-fake", param=list(params), poll_seconds=0, max_polls=1,
        cold_start_seconds=60, cold_start_restarts=1)
    with pytest.raises(snowmig.MissingTarget) as exc:
        snowmig.cmd_run(args)
    return str(exc.value)


# ------------------------------------------------------------ run --param

def test_a_per_schema_copy_job_is_advised_as_the_copy_stage(tmp_path):
    message = _refusal(tmp_path, "snowmig_02_copy_sales", "mode=overwrite")
    assert "--stage-param copy_schema.mode=<value>" in message
    assert "--stage-param mode=" not in message
    # A baked value in the one shared notebook reaches every copy job.
    assert "every per-schema copy job" in message


def test_schema_on_a_copy_job_is_named_as_its_task_parameter(tmp_path):
    message = _refusal(tmp_path, "snowmig_02_copy_sales", "schema=HR")
    assert "task parameter" in message
    assert "--stage-param copy_schema.schema" not in message
    assert "--stage-param schema" not in message
    assert "snowmig_02_copy_<schema>" in message


def test_tables_on_a_copy_job_is_named_as_its_task_parameter(tmp_path):
    # With two or more copy schemas provision refuses a baked
    # copy_schema.tables: the one shared 02 notebook would narrow EVERY
    # per-schema copy job. Advising it here sent the operator straight into
    # that second refusal. `tables` narrows one copy job only as a task
    # parameter on that job's task.
    message = _refusal(tmp_path, "snowmig_02_copy_sales", "tables=ORDERS")
    assert "--stage-param copy_schema.tables" not in message
    assert "--stage-param tables" not in message
    assert "`tables`" in message
    assert "task parameter" in message
    assert "snowmig_02_copy_sales" in message


def test_tables_next_to_mode_on_a_copy_job_advises_mode_only(tmp_path):
    message = _refusal(tmp_path, "snowmig_02_copy_sales",
                       "tables=ORDERS", "mode=overwrite")
    assert "--stage-param copy_schema.mode=<value>" in message
    assert "copy_schema.tables=" not in message


def test_the_refusal_no_longer_says_parameters_never_reach_a_notebook(
        tmp_path):
    message = _refusal(tmp_path, "snowmig_01_structure", "schema=HR")
    assert "neither argv nor environment, so this run" not in message
    assert "getParameter" in message
    assert "task parameter" in message


def test_a_job_that_is_no_stage_gets_qualified_advice_for_a_shared_name(
        tmp_path):
    message = _refusal(tmp_path, "some_other_job", "mode=overwrite")
    assert "--stage-param mode=" not in message
    assert "--stage-param <stage>.mode=<value>" in message


# ----------------------------------------------- provision --stage-param

@pytest.mark.parametrize("name", ["schema", "copy_schema.schema"])
def test_a_baked_copy_schema_is_refused_next_to_per_schema_jobs(name):
    with pytest.raises(ValueError, match="task parameter"):
        check_stage_params({name: "X"}, copy_schemas=["CORE"])


def test_structure_schema_still_narrows_the_structure_stage_only():
    check_stage_params({"structure.schema": "HR"}, copy_schemas=["CORE"])


@pytest.mark.parametrize("name", ["tables", "copy_schema.tables"])
def test_a_baked_tables_that_would_narrow_every_copy_job_is_refused(name):
    with pytest.raises(ValueError, match="every per-schema copy job"):
        check_stage_params({name: "ORDERS"}, copy_schemas=["HR", "SALES"])


def test_tables_with_one_copy_schema_or_none_is_still_accepted():
    check_stage_params({"tables": "ORDERS"}, copy_schemas=["SALES"])
    check_stage_params({"tables": "ORDERS", "schema": "SALES"})


def test_provision_refuses_before_anything_is_called():
    from target.provisioning import provision
    calls = []
    with pytest.raises(ValueError, match="task parameter"):
        provision(call=lambda *a, **k: calls.append(a) or {},
                  workspace_name="ws-fake", scripts=[], execute=True,
                  delays=(), target_catalog="lake", copy_schemas=["CORE"],
                  stage_params={"schema": "X"})
    assert calls == []


# ------------------------------------------------------------------- docs

def _help(*argv):
    parser = snowmig.build_parser()
    sub = next(a for a in parser._actions
               if isinstance(a, argparse._SubParsersAction))
    return " ".join(sub.choices[argv[0]].format_help().split())


def test_provision_help_is_current():
    text = _help("provision")
    assert "do not reach a notebook" not in text
    assert "four parametrised" not in text
    assert "getParameter" in text


def test_run_help_does_not_advertise_param_as_a_job_parameter():
    text = _help("run")
    assert "a job parameter, repeatable" not in text
    assert "refused" in text


@pytest.mark.parametrize("rel", [
    "README.md", "skills/snowflake-provision-environment/SKILL.md",
    "commands/snowflake-provision.md"])
def test_the_docs_describe_task_parameters_winning(rel):
    text = " ".join((ROOT / rel).read_text(encoding="utf-8").split())
    assert "job parameters never reach a notebook" not in text
    assert "job parameters reach a notebook neither as argv nor" not in text
    assert "four parametrised" not in text

"""The demo's provision step is the prod provision on the same inputs.

The no-credentials demo promises exactly the shape a prod run produces, but
its provision step still called provision() the pre-plan way: no plan
files, no copy schemas, no plan label -- although ddl_plan.json and
plan.json were already in the same out dir. So the demo showed
`snowmig_02_copy_schema` as "would create" -- the schemaless job the code
itself says could only fail -- never showed the per-schema copy jobs or
the dated plan backups, and its PROVISION.md differed from what `snowmig
provision` writes on the same directory.

Both now read their inputs through one helper, plan_push_inputs.
"""
import json

import pytest

from emulation.runbook import run_demo
from target.provisioning import plan_push_inputs


@pytest.fixture(scope="module")
def demo(tmp_path_factory):
    out = tmp_path_factory.mktemp("demo_provision")
    run_demo(out)
    return out


def _prov(out):
    return json.loads((out / "provision_result.json").read_text(
        encoding="utf-8"))


def test_the_demo_registers_one_copy_job_per_planned_schema(demo):
    prov = _prov(demo)
    _, schemas = plan_push_inputs(demo)
    assert schemas, "the demo plan moves tables, so it has copy schemas"
    assert [j["schema"] for j in prov["copy_jobs"]] == schemas
    jobs = [s["detail"] for s in prov["steps"] if s["step"] == "job"]
    assert "snowmig_02_copy_schema" not in jobs
    assert {j["job"] for j in prov["copy_jobs"]} <= set(jobs)


def test_the_demo_pushes_and_backs_up_the_plan(demo):
    prov = _prov(demo)
    uploads = [s["detail"] for s in prov["steps"] if s["step"] == "upload"]
    assert any("ddl_plan.json -> backup-snowflake-migration/plan/" in u
               for u in uploads), uploads
    backups = [s for s in prov["steps"] if s["step"] == "backup"]
    assert backups, "every plan push is backed up, dated"
    text = (demo / "PROVISION.md").read_text(encoding="utf-8")
    assert "backup-snowflake-migration/backup/" in text
    assert "Per-schema copy workflows" in text


def test_the_demo_line_counts_notebooks_as_notebooks(demo):
    prov = _prov(demo)
    notebooks = sum(1 for s in prov["steps"] if s["step"] == "upload"
                    and "(generated" in s["detail"])
    line = next(l for l in (demo / "DEMO.md").read_text(
        encoding="utf-8").splitlines() if "provision (dry run)" in l)
    assert f"{notebooks} notebook(s)" in line, line
    jobs = sum(1 for s in prov["steps"] if s["step"] == "job")
    assert f"{jobs} jobs" in line, line


def test_a_corrupt_ddl_plan_is_named_with_a_remedy(tmp_path):
    # cmd_provision used to read ddl_plan.json through snowmig._read, which
    # names the file and says how to recover. Reading it through
    # plan_push_inputs with a bare json.loads surfaced the decoder's own
    # "Expecting property name ..." with no path, so the operator could not
    # tell which artifact was broken.
    (tmp_path / "ddl_plan.json").write_text("{bad", encoding="utf-8")
    with pytest.raises(ValueError) as exc:
        plan_push_inputs(tmp_path)
    message = str(exc.value)
    assert str(tmp_path / "ddl_plan.json") in message
    assert "not valid JSON" in message
    assert "re-run" in message

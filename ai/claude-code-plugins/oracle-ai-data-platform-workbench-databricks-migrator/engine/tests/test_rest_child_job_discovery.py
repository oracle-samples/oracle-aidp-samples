"""REF-249: REST launchers' uppercase JOB_ID uses existing soft discovery."""
import pytest

from dbx_export.dependency_resolver import extract_child_jobs_with_confidence


def notebook(*sources):
    return {"cells": [{"cell_type": "code", "source": source} for source in sources]}


@pytest.mark.parametrize("name", ["job_id", "JOB_ID", "Job_Id"])
@pytest.mark.parametrize("value", ["123", "'123'", '\"123\"'])
@pytest.mark.parametrize("split_cells", [False, True])
def test_rest_assignment_casing_and_cell_boundaries(name, value, split_cells):
    definitions = f'{name} = {value}\nENDPOINT = "https://dbx.invalid/api/2.1/jobs/run-now"'
    helper = f'''def trigger_and_wait(label, notebook_params):
    data = json.dumps({{"job_id": {name}, "notebook_params": notebook_params}})
    response = requests.post(ENDPOINT, data=data, headers=HEADERS).json()
    return response["run_id"]
'''
    cells = (definitions, helper) if split_cells else (definitions + "\n" + helper,)
    assert extract_child_jobs_with_confidence(notebook(*cells)) == [("123", False)]


@pytest.mark.parametrize("name", ["job_id", "JOB_ID", "Job_Id"])
@pytest.mark.parametrize("call", [
    "dbutils.jobs.runNow(job_id=unknown)",
    "client.jobs.run_now(unknown)",
])
def test_existing_ambiguous_sdk_call_hardens_candidates_in_any_case(name, call):
    assert extract_child_jobs_with_confidence(notebook(f"{name} = 123", call)) == [("123", True)]


@pytest.mark.parametrize("source", [
    "# JOB_ID = 123",
    "// JOB_ID = 123",
    "-- JOB_ID = 123",
    "OTHER_JOB_ID = 123",
    "JOB_IDENTIFIER = 123",
    "JOB_ID = runtime_id()",
    'requests.post(url, json={"job_id": 123})',
])
def test_existing_exclusions_and_unsupported_forms_are_preserved(source):
    assert extract_child_jobs_with_confidence(notebook(source)) == []


def test_later_sdk_literal_upgrades_uppercase_soft_candidate_without_duplicates():
    assert extract_child_jobs_with_confidence(notebook(
        "JOB_ID = 123", "client.jobs.run_now(job_id=123)"
    )) == [("123", True)]

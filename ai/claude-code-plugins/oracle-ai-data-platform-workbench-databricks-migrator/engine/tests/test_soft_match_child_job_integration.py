"""End-to-end (real main()) check of hard/soft child-job handling.

Real: main(), DependencyResolver, phase1_discover, phase3, Report, exit codes.
Faked: only the Databricks/AIDP network clients and config loading.

Constructed reproduction (not a cited production incident): a regex-only
``job_id="11"`` inside an HTTP-payload call is fetched as a child job, the
fetch fails, ``jobs_failed`` becomes 1+ and the export exits JOB_CREATE_FAILED
although the real workflow was created.
"""
import json
import warnings
from types import SimpleNamespace

import pytest

from dbx_export.__main__ import ExitCode, main
from dbx_export.dbx_client import DbxClient, DbxConfig, DbxExportError, ExportedFile

ROOT = "1"
SOFT_NB = "/Users/x/trigger"
HARD_NB = "/Users/x/driver"


def _nb(src):
    return json.dumps({"cells": [{"cell_type": "code", "source": src, "metadata": {},
                                  "outputs": [], "execution_count": None}],
                       "metadata": {}, "nbformat": 4, "nbformat_minor": 5}).encode()


# Regex-only: job_id literal in an unrelated HTTP payload call.
SOFT_SRC = 'import requests\nrequests.post(url, json=dict(job_id="11", note="x"))\n'
# Hard: a real dbutils.jobs.runNow call.
HARD_SRC = "dbutils.jobs.runNow(job_id=99)\n"


class FakeDbx:
    """Root job 1 -> one notebook task; jobs 11 / 99 do not exist."""
    def __init__(self, notebook_path, source, child_error):
        self.nb_path, self.source = notebook_path, source
        self.child_error = child_error

    def get_job(self, job_id):
        if str(job_id) == ROOT:
            return {"job_id": 1, "settings": {"name": "root_job", "tasks": [{
                "task_key": "t1", "notebook_task": {"notebook_path": self.nb_path}}]}}
        if self.child_error is not None:
            if self.child_error == "live-absence-response":
                # Real client classification is part of this main() grid;
                # only the HTTP transport is replaced with the captured shape.
                return DbxClient(DbxConfig(host="https://dbx.invalid", token="fixture")).get_job(job_id)
            raise self.child_error
        return {"job_id": int(job_id), "settings": {"name": f"child_{job_id}", "tasks": []}}

    def export_notebook(self, path):
        assert path == self.nb_path
        return ExportedFile(content=_nb(self.source), is_notebook=True, extension=".ipynb")

    def list_workspace_folder(self, _path):
        return []

    def list_github_dir(self, *a, **k):
        return []

    def list_github_dir_entries(self, *a, **k):
        return []

    @staticmethod
    def parse_github_owner_repo(_url):
        return None, None

    def get_cluster_libraries(self, _cid):
        return {}


class FakeAidp:
    def __init__(self):
        self.created = []

    def find_workflow_by_name(self, _n):
        return None

    def create_workflow_shell(self, name, description=""):
        self.created.append(name)
        return {"key": "wf-key"}

    def update_workflow(self, key, payload):
        return {}

    def install_cluster_libraries(self, key, items):
        return {}


@pytest.fixture
def harness(tmp_path, monkeypatch):
    def run(notebook_path, source, *extra,
            child_error=DbxExportError("job_not_found", "Job does not exist")):
        config = SimpleNamespace(
            aidp=SimpleNamespace(lake_ocid="lake", workspace_id="ws", oci_profile="P"),
            paths=SimpleNamespace(export_output="/Workspace/export",
                                  target_runtime_requirements_file=None,
                                  compatibility_policy_file=None),
            dbx_source=SimpleNamespace(host=None, proxy=None),
            secrets=SimpleNamespace(dbx_proxy_basic_user=None, dbx_proxy_basic_pass=None),
        )
        aidp = FakeAidp()

        class JarDiscovery:
            index_fallbacks = []
            resolutions, index_errors, blockers, allow_unresolved = [], [], [], True

            @staticmethod
            def to_dict():
                return {"summary": {"missing": 0, "ambiguous": 0, "index_errors": 0}}

            @staticmethod
            def write(*a):
                return str(tmp_path / "jar.json")

        mp = monkeypatch.setattr
        mp("aidp_config.load_config", lambda **k: config)
        mp("dbx_export.__main__.configured_lib_repository_dir", lambda _p: "/libs")
        mp("dbx_export.__main__.get_oci_signer", lambda **k: ({}, object()))
        mp("dbx_export.__main__.AidpClient", lambda **k: aidp)
        mp("dbx_export.__main__.DbxClient", lambda *a, **k: FakeDbx(notebook_path, source, child_error))
        mp("dbx_export.__main__._limits_startup_gate", lambda _a: None)
        mp("dbx_export.__main__.ph_mod.new_run_id", lambda: "run")
        for name in ("phase1_fetch_files", "phase1_discover_init_script_artifacts",
                     "phase1_discover_existing_cluster_libraries",
                     "discover_notebook_pip_deps", "consolidate_cluster_requirements",
                     "resolve_failed_jar_fallbacks", "reconcile_failed_declared_jars"):
            mp(f"dbx_export.__main__.{name}", lambda *a, **k: None)
        mp("dbx_export.__main__.discover_notebook_jars", lambda *a, **k: JarDiscovery())
        mp("dbx_export.__main__.phase2_upload_notebooks",
           lambda *a, **k: ({notebook_path: "/Workspace/export/nb"}, []))
        mp("dbx_export.__main__.phase2_upload_files", lambda *a, **k: ({}, False))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            rc = main(["--job-id", ROOT, "--dbx-proxy", "https://proxy.invalid",
                       "--cluster-key", "cl-1", "--report-dir", str(tmp_path), *extra])
        report = json.loads((tmp_path / "export_dbx_job_1_run.json").read_text())
        return rc, report, aidp
    return run


def _statuses(report):
    return {j["dbx_job_id"]: j["status"] for j in report["jobs"]}


# Genuine call through a variable named job_id: the literal exists only in the
# assignment. (A differently named variable, e.g. `jid = 99`, is not discovered
# at all -- a pre-existing gap, out of scope here.)
VAR_SRC = "job_id = 99\ndbutils.jobs.runNow(job_id=job_id)\n"
VARIABLE_CALL_SOURCES = [
    VAR_SRC,
    "job_id = 99\nclient.jobs.run_now(job_id=job_id)\n",
    "job_id = 99\ndbutils.jobs.runNow(job_id)\n",
    "args = dict(job_id=99)\ndbutils.jobs.runNow(**args)\n",
    "job_id = 99\ndbutils.jobs.runNow(jobId=job_id)\n",
]


# ── grid: match kind × --strict-deps ────────────────────────────────────
def test_soft_unfetchable_child_does_not_fail_export(harness):
    rc, report, aidp = harness(SOFT_NB, SOFT_SRC)
    assert rc == ExitCode.OK
    assert _statuses(report) == {ROOT: "CREATED", "11": "SKIPPED_SOFT_MATCH"}
    assert report["summary"]["jobs_failed"] == 0
    assert report["summary"]["jobs_soft_match_unresolved"] == 1
    assert aidp.created == ["ai_tool_root_job"] or len(aidp.created) == 1


def test_soft_unfetchable_child_is_surfaced_not_silent(harness):
    _, report, _ = harness(SOFT_NB, SOFT_SRC)
    warns = [e for e in report["errors"] if "soft-match-job-unresolved" in e]
    assert len(warns) == 1 and "job_id=11" in warns[0] and SOFT_NB in warns[0]
    assert warns[0].startswith("WARN ")  # a warning, not counted as an error


def test_soft_unfetchable_child_does_not_trip_strict_deps(harness):
    rc, report, _ = harness(SOFT_NB, SOFT_SRC, "--strict-deps")
    assert rc == ExitCode.OK
    assert report["summary"]["jobs_failed"] == 0


def test_hard_unfetchable_child_still_fails_export(harness):
    rc, report, _ = harness(HARD_NB, HARD_SRC)
    assert rc == ExitCode.JOB_CREATE_FAILED
    assert _statuses(report)["99"] == "FAILED"
    assert report["summary"]["jobs_soft_match_unresolved"] == 0


def test_hard_unfetchable_child_still_trips_strict_deps(harness):
    rc, _, aidp = harness(HARD_NB, HARD_SRC, "--strict-deps")
    assert rc == ExitCode.STRICT_DEPS_FAILED
    assert aidp.created == []  # aborted before any workflow was created


# Fail closed: a real runNow(job_id=<variable>) child must NOT be demoted to a
# skippable soft match just because its literal id sits in an assignment.
@pytest.mark.parametrize("source", VARIABLE_CALL_SOURCES)
def test_variable_id_runnow_child_still_fails_export(harness, source):
    rc, report, _ = harness(HARD_NB, source)
    assert rc == ExitCode.JOB_CREATE_FAILED
    assert _statuses(report)["99"] == "FAILED"
    assert report["summary"]["jobs_soft_match_unresolved"] == 0


@pytest.mark.parametrize("source", VARIABLE_CALL_SOURCES)
def test_variable_id_runnow_child_trips_strict_deps(harness, source):
    rc, _, aidp = harness(HARD_NB, source, "--strict-deps")
    assert rc == ExitCode.STRICT_DEPS_FAILED
    assert aidp.created == []


# Round-2 contract: provenance × fetch outcome × strict mode. A missing job
# is the only soft failure that may be warned and skipped; all uncertainty
# retains the failed-child gate through real main(), phase 3 and the report.
FETCH_OUTCOMES = [
    pytest.param(DbxExportError("job_not_found", "Job does not exist"), True, id="confirmed-absence"),
    pytest.param(DbxExportError("get_job_failed", "HTTP 403: permission denied", 403), False, id="permission-403"),
    pytest.param(DbxExportError("get_job_failed", "HTTP 500: unavailable", 500), False, id="service-500"),
    pytest.param(DbxExportError("get_job_failed", "HTTP 503: unavailable", 503), False, id="service-503"),
    pytest.param(DbxExportError("get_job_failed", "ConnectionError while fetching child"), False, id="network-connection"),
    pytest.param(DbxExportError("get_job_failed", "Timeout while fetching child"), False, id="network-timeout"),
    pytest.param(DbxExportError("get_job_failed", "Unknown failure"), False, id="unknown"),
    # Text alone must not imply absence when the error class is unknown.
    pytest.param(DbxExportError("get_job_failed", "Job does not exist"), False, id="uncategorized-absence-text"),
]


@pytest.mark.parametrize("error, confirmed_absence", FETCH_OUTCOMES)
@pytest.mark.parametrize("strict", [False, True], ids=["normal", "strict"])
@pytest.mark.parametrize("source, child_id, soft", [
    pytest.param(SOFT_SRC, "11", True, id="soft-payload"),
    pytest.param(HARD_SRC, "99", False, id="hard-literal"),
    pytest.param(VAR_SRC, "99", False, id="hard-variable"),
])
def test_child_fetch_failure_accounting_grid(harness, source, child_id, soft,
                                             strict, error, confirmed_absence):
    extra = ("--strict-deps",) if strict else ()
    rc, report, aidp = harness(SOFT_NB, source, *extra, child_error=error)
    skip = soft and confirmed_absence
    expected_rc = (ExitCode.OK if skip else
                   ExitCode.STRICT_DEPS_FAILED if strict else ExitCode.JOB_CREATE_FAILED)
    assert rc == expected_rc
    if strict and not skip:
        assert report["jobs"] == []  # refusal precedes Phase 3 job-result creation
    else:
        assert _statuses(report)[child_id] == ("SKIPPED_SOFT_MATCH" if skip else "FAILED")
    assert report["summary"]["jobs_soft_match_unresolved"] == int(skip)
    if not strict or skip:
        assert report["summary"]["jobs_failed"] == int(not skip)
    if strict and not skip:
        assert aidp.created == []
    else:
        assert len(aidp.created) == 1
    soft_warnings = [entry for entry in report["errors"]
                     if "soft-match-job-unresolved" in entry]
    assert bool(soft_warnings) == skip
    if skip:
        assert soft_warnings[0].startswith("WARN ")
    elif not strict:
        child_result = next(item for item in report["jobs"] if item["dbx_job_id"] == child_id)
        assert error.reason in child_result["failure_reason"]


@pytest.mark.parametrize("strict", [False, True], ids=["normal", "strict"])
@pytest.mark.parametrize("source, child_id", [
    pytest.param(SOFT_SRC, "11", id="soft-payload"),
    pytest.param(HARD_SRC, "99", id="hard-literal"),
    pytest.param(VAR_SRC, "99", id="hard-variable"),
])
def test_successful_child_fetch_exports_regardless_of_provenance(harness, source, child_id, strict):
    extra = ("--strict-deps",) if strict else ()
    rc, report, aidp = harness(SOFT_NB, source, *extra, child_error=None)
    assert rc == ExitCode.OK
    assert _statuses(report) == {ROOT: "CREATED", child_id: "CREATED"}
    assert report["summary"]["jobs_failed"] == 0
    assert report["summary"]["jobs_soft_match_unresolved"] == 0
    assert len(aidp.created) == 2


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("source,child_id,soft", [
    (SOFT_SRC, "11", True), (HARD_SRC, "99", False), (VAR_SRC, "99", False),
])
@pytest.mark.parametrize("message_kind", ["exact", "mismatched", "malformed"])
def test_real_job_client_absence_shape_through_export_grid(
        harness, monkeypatch, strict, source, child_id, soft, message_kind):
    import requests
    message = (f"Job {child_id} does not exist." if message_kind == "exact"
               else "Job 999 does not exist." if message_kind == "mismatched"
               else "Invalid job_id")
    response = requests.Response()
    response.status_code = 400
    response._content = json.dumps({"error_code": "INVALID_PARAMETER_VALUE",
                                    "message": message}).encode()
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", lambda *a, **k: response)
    rc, report, aidp = harness(SOFT_NB, source,
                              *(("--strict-deps",) if strict else ()),
                              child_error="live-absence-response")
    skip = soft and message_kind == "exact"
    assert rc == (ExitCode.OK if skip else ExitCode.STRICT_DEPS_FAILED
                  if strict else ExitCode.JOB_CREATE_FAILED)
    assert report["summary"]["jobs_soft_match_unresolved"] == int(skip)
    if strict and not skip:
        assert aidp.created == []
    else:
        assert _statuses(report)[child_id] == ("SKIPPED_SOFT_MATCH" if skip else "FAILED")


# REF-249: source shape from the supplied notebook, using a synthetic job ID.
REST_SRC = """import json
ENDPOINT = "https://dbx.invalid/api/2.1/jobs/run-now"
JOB_ID = 99

def trigger(params):
    data = json.dumps({"job_id": JOB_ID, "notebook_params": params})
    return requests.post(ENDPOINT, data=data).json()
"""


@pytest.mark.parametrize("name", ["job_id", "JOB_ID", "Job_Id"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("outcome", ["exists", "absent", "forbidden"])
def test_rest_assignment_casing_through_export_main(harness, name, strict, outcome):
    flags = ["--strict-deps"] if strict else []
    errors = {
        "exists": None,
        "absent": DbxExportError("job_not_found", "Job does not exist"),
        "forbidden": DbxExportError("get_job_failed", "HTTP 403 forbidden"),
    }
    source = REST_SRC.replace("JOB_ID", name)
    rc, report, aidp = harness(HARD_NB, source, *flags, child_error=errors[outcome])
    if outcome == "exists":
        assert rc == ExitCode.OK
        assert _statuses(report) == {ROOT: "CREATED", "99": "CREATED"}
        assert len(aidp.created) == 2
    elif outcome == "absent":
        assert rc == ExitCode.OK
        assert _statuses(report)["99"] == "SKIPPED_SOFT_MATCH"
        assert report["summary"]["jobs_soft_match_unresolved"] == 1
        assert any("soft-match-job-unresolved" in error and "job_id=99" in error
                   for error in report["errors"])
    else:
        assert rc == (ExitCode.STRICT_DEPS_FAILED if strict else ExitCode.JOB_CREATE_FAILED)
        assert report["summary"]["jobs_soft_match_unresolved"] == 0
        if strict:
            assert aidp.created == []
        else:
            assert _statuses(report)["99"] == "FAILED"

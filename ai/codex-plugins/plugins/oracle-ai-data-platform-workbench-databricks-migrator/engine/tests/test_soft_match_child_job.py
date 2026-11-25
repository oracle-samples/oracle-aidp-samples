"""Hard/soft child-job discovery in dbx_export.dependency_resolver.

Constructed reproduction (not a cited production incident): a bare
``job_id="11"`` literal in unrelated HTTP-payload code matches the regex
fallback and is fetched as a child job; the fetch fails and the export exits
non-zero although the real workflow was created. Only an AST
``runNow``/``run_now`` call with a literal id is a hard match; regex-only ids
are soft -- unless the notebook has a runNow call whose id is not a literal,
in which case regex ids are hard (fail closed).
"""
import pytest
from types import SimpleNamespace

from dbx_export.dependency_resolver import (
    DependencyResolver,
    Graph,
    extract_child_jobs,
    extract_child_jobs_with_confidence,
    can_skip_absent_soft_match,
)
from dbx_export.dbx_client import DbxExportError


def nb(*sources):
    return {"cells": [{"cell_type": "code", "source": s} for s in sources]}


@pytest.mark.parametrize("fields", [
    {},
    {"job_def": None, "fetch_error": "unknown", "soft_match": True},
    {"job_def": {}, "fetch_error": None, "soft_match": False},
])
def test_lightweight_job_without_absence_metadata_cannot_be_skipped(fields):
    assert can_skip_absent_soft_match(SimpleNamespace(**fields)) is False


# ── extractor: source form grid ─────────────────────────────────────────
@pytest.mark.parametrize("src, expected", [
    ("dbutils.jobs.runNow(123)", [("123", True)]),
    ("dbutils.jobs.runNow(job_id=123)", [("123", True)]),
    ("client.run_now(job_id=123)", [("123", True)]),
    ('requests.post(url, json=dict(job_id="11"))', [("11", False)]),
    ("payload = build(job_id=00)", [("00", False)]),
    ("x = 1", []),
])
def test_confidence_by_source_form(src, expected):
    assert extract_child_jobs_with_confidence(nb(src)) == expected


# ── fail closed: a runNow call with a non-literal id makes regex ids hard ──
@pytest.mark.parametrize("cells, expected", [
    # genuine call through a variable: the id lives only in the assignment
    (["job_id = 99", "dbutils.jobs.runNow(job_id=job_id)"], [("99", True)]),
    (["jid = 1\ndbutils.jobs.run_now(job_id=jid)", "cfg = dict(job_id=99)"], [("99", True)]),
    # **kwargs / positional variable
    (['p = dict(job_id=99)\ndbutils.jobs.runNow(**p)'], [("99", True)]),
    (["job_id = 99\ndbutils.jobs.runNow(job_id)"], [("99", True)]),
    # every runNow call is literal -> regex ids stay soft (the HTTP-payload case)
    (['dbutils.jobs.runNow(5)', 'requests.post(u, json=dict(job_id="11"))'],
     [("5", True), ("11", False)]),
    # no runNow call at all -> still soft
    (['requests.post(u, json=dict(job_id="11"))'], [("11", False)]),
    # ambiguity is per notebook: a variable call in ANOTHER notebook is unaffected
])
def test_non_literal_runnow_makes_regex_ids_hard(cells, expected):
    assert extract_child_jobs_with_confidence(nb(*cells)) == expected


def test_non_literal_runnow_in_one_notebook_does_not_harden_another():
    ambiguous = nb("job_id = 99", "dbutils.jobs.runNow(job_id=job_id)")
    clean = nb('requests.post(u, json=dict(job_id="11"))')
    assert extract_child_jobs_with_confidence(ambiguous) == [("99", True)]
    assert extract_child_jobs_with_confidence(clean) == [("11", False)]


def test_hard_upgrades_soft_across_cells_in_either_order():
    soft_then_hard = nb("f(job_id=7)", "dbutils.jobs.runNow(7)")
    hard_then_soft = nb("dbutils.jobs.runNow(7)", "f(job_id=7)")
    assert extract_child_jobs_with_confidence(soft_then_hard) == [("7", True)]
    assert extract_child_jobs_with_confidence(hard_then_soft) == [("7", True)]


def test_flat_wrapper_returns_hard_and_soft_ids_unchanged():
    n = nb("dbutils.jobs.runNow(1)", "f(job_id=2)")
    assert extract_child_jobs(n) == ["1", "2"]


@pytest.mark.parametrize("ambiguous", [False, True])
def test_regex_candidate_order_survives_later_hard_reference(ambiguous):
    cells = ["payload(job_id=11)", "dbutils.jobs.runNow(55)"]
    if ambiguous:
        cells.append("client.jobs.run_now(job_id=variable)")
    notebook = nb(*cells)
    assert extract_child_jobs_with_confidence(notebook) == [
        ("11", ambiguous), ("55", True),
    ]
    assert extract_child_jobs(notebook) == ["11", "55"]


# ── resolver: soft flag propagation ─────────────────────────────────────
class _Dbx:
    def __init__(self, missing=()):
        self.missing = set(missing)

    def get_job(self, job_id):
        if job_id in self.missing:
            raise DbxExportError("job_not_found", f"Job {job_id} does not exist")
        return {"settings": {"name": f"job{job_id}", "tasks": []}}

    def parse_github_owner_repo(self, _url):
        return None, None


def test_soft_job_that_fails_to_fetch_is_flagged_soft():
    r, g = DependencyResolver(_Dbx(missing={"11"}), log=lambda *_: None), Graph()
    r._enqueue_child_job(g, "11", "/nb/trigger", soft=True)
    assert g.jobs["11"].fetch_error and g.jobs["11"].soft_match is True


def test_hard_job_that_fails_to_fetch_is_not_soft():
    r, g = DependencyResolver(_Dbx(missing={"99"}), log=lambda *_: None), Graph()
    r._enqueue_child_job(g, "99", "/nb/driver")
    assert g.jobs["99"].fetch_error and g.jobs["99"].soft_match is False


def test_later_hard_reference_clears_soft_flag():
    r, g = DependencyResolver(_Dbx(missing={"5"}), log=lambda *_: None), Graph()
    r._enqueue_child_job(g, "5", "/nb/a", soft=True)
    r._enqueue_child_job(g, "5", "/nb/b")  # hard reference
    assert g.jobs["5"].soft_match is False
    assert g.jobs["5"].referenced_by == ["/nb/a", "/nb/b"]


# ── orchestration: phase 3 + report summary ─────────────────────────────
from dbx_export.__main__ import phase3_create_clusters_and_workflows
from dbx_export.dependency_resolver import JobNode
from dbx_export.report import Report


def _phase3(graph):
    report = Report(run_id="r", dbx_root_job_id="1", export_output="/x", prefix="p")
    phase3_create_clusters_and_workflows(
        aidp=object(), dbx=object(), graph=graph, nb_target_paths={},
        uri_to_aidp={}, export_output="/x", prefix="p",
        overwrite_workflow=False, cluster_override_key=None, report=report,
    )
    return report


def _graph(soft):
    g = Graph()
    g.jobs["11"] = JobNode(dbx_job_id="11", depth=1, fetch_error="job_not_found: gone",
                           referenced_by=["/nb/trigger"], soft_match=soft,
                           fetch_error_is_not_found=True)
    return g


def test_phase3_soft_unfetchable_job_is_skipped_not_failed():
    s = _phase3(_graph(soft=True)).summary()
    assert s["jobs_failed"] == 0 and s["jobs_soft_match_unresolved"] == 1


def test_phase3_hard_unfetchable_job_still_fails():
    s = _phase3(_graph(soft=False)).summary()
    assert s["jobs_failed"] == 1 and s["jobs_soft_match_unresolved"] == 0


# Real transport classifier: absence must come from response status or the
# structured code plus an exact request-bound message, never arbitrary prose.
import json
import requests
from dbx_export.dbx_client import DbxClient, DbxConfig


@pytest.mark.parametrize("status, body, expected_reason", [
    (400, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": "gone"}, "job_not_found"),
    (404, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "job_not_found"),
    (404, None, "job_not_found"),
    (400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "does not exist"}, "get_job_failed"),
    (401, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "get_job_failed"),
    (403, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "get_job_failed"),
    (500, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "get_job_failed"),
    (503, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "get_job_failed"),
    (429, {"error_code": "RESOURCE_DOES_NOT_EXIST"}, "get_job_failed"),
    (400, None, "get_job_failed"),
])
def test_get_job_classifies_only_definitive_absence(monkeypatch, status, body, expected_reason):
    response = requests.Response()
    response.status_code = status
    response._content = (json.dumps(body) if body is not None else "not JSON").encode()
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", lambda *a, **k: response)
    client = DbxClient(DbxConfig(host="https://dbx.invalid", token="test-token"))
    with pytest.raises(DbxExportError) as caught:
        client.get_job("11")
    assert caught.value.reason == expected_reason
    assert caught.value.status == status
    assert caught.value.is_job_not_found == (expected_reason == "job_not_found")


# Essential fields verbatim from the reviewer's read-only jobs/get v2.0 probe
# on 2026-10-12, session aaaaaaaa-0000-4000-8000-000000000110. The printed
# response was truncated in request metadata; no omitted details are invented.
LIVE_ABSENT_JOB_ID = "999999999999099"
LIVE_ABSENT_JOB_RESPONSE = {
    "error_code": "INVALID_PARAMETER_VALUE",
    "message": "Job 999999999999099 does not exist.",
}


def test_live_captured_jobs_response_is_confirmed_absence(monkeypatch):
    response = requests.Response()
    response.status_code = 400
    response._content = json.dumps(LIVE_ABSENT_JOB_RESPONSE).encode()
    calls = []
    def get(url, **kwargs):
        calls.append((url, kwargs["params"]))
        return response
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", get)
    client = DbxClient(DbxConfig(host="https://dbx.invalid", token="test-token"))
    with pytest.raises(DbxExportError) as caught:
        client.get_job(LIVE_ABSENT_JOB_ID)
    assert caught.value.reason == "job_not_found"
    assert caught.value.is_job_not_found
    assert calls == [("https://dbx.invalid/api/2.2/jobs/get",
                      {"job_id": LIVE_ABSENT_JOB_ID})]


@pytest.mark.parametrize("job_id", ["abc", "-11", "", "١١"])
def test_malformed_requested_id_cannot_be_confirmed_absent(monkeypatch, job_id):
    response = requests.Response()
    response.status_code = 400
    response._content = json.dumps({"error_code": "INVALID_PARAMETER_VALUE",
                                    "message": f"Job {job_id} does not exist."}).encode()
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", lambda *a, **k: response)
    with pytest.raises(DbxExportError) as caught:
        DbxClient(DbxConfig(host="https://dbx.invalid", token="fixture")).get_job(job_id)
    assert not caught.value.is_job_not_found


@pytest.mark.parametrize("status,code,message", [
    (400, "INVALID_PARAMETER_VALUE", "Job 12 does not exist."),
    (400, "INVALID_PARAMETER_VALUE", "Job 11 does not exist"),
    (400, "INVALID_PARAMETER_VALUE", "Job 11 does not exist. Retry later."),
    (400, "INVALID_PARAMETER_VALUE", "Job 11 does not exist.\n"),
    (400, "INVALID_PARAMETER_VALUE", "Invalid job_id: 11"),
    (400, "INVALID_PARAMETER_VALUE", None),
    (400, "PERMISSION_DENIED", "Job 11 does not exist."),
    (401, "INVALID_PARAMETER_VALUE", "Job 11 does not exist."),
    (403, "INVALID_PARAMETER_VALUE", "Job 11 does not exist."),
    (429, "INVALID_PARAMETER_VALUE", "Job 11 does not exist."),
    (500, "INVALID_PARAMETER_VALUE", "Job 11 does not exist."),
    (503, "INVALID_PARAMETER_VALUE", "Job 11 does not exist."),
])
def test_missing_job_message_requires_exact_status_code_and_requested_id(
        monkeypatch, status, code, message):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({"error_code": code, "message": message}).encode()
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", lambda *a, **k: response)
    client = DbxClient(DbxConfig(host="https://dbx.invalid", token="test-token"))
    with pytest.raises(DbxExportError) as caught:
        client.get_job("11")
    assert caught.value.reason == "get_job_failed"
    assert not caught.value.is_job_not_found


@pytest.mark.parametrize("error", [requests.ConnectionError("failed"), requests.Timeout("timed out")])
def test_get_job_transport_errors_remain_classified_failures(monkeypatch, error):
    def get(*args, **kwargs):
        raise error
    monkeypatch.setattr("dbx_export.dbx_client.requests.get", get)
    client = DbxClient(DbxConfig(host="https://dbx.invalid", token="test-token"))
    with pytest.raises(DbxExportError) as caught:
        client.get_job("11")
    assert caught.value.reason == "get_job_failed"
    assert caught.value.status == 0


@pytest.mark.parametrize("status", [401, 403, 429, 500, 503])
def test_job_not_found_reason_never_overrides_permission_or_service_status(status):
    assert not DbxExportError("job_not_found", "misleading absence", status).is_job_not_found


@pytest.mark.parametrize("soft", [False, True])
def test_phase3_unclassified_fetch_error_is_failed_by_default(soft):
    graph = _graph(soft)
    graph.jobs["11"].fetch_error_is_not_found = False
    summary = _phase3(graph).summary()
    assert summary["jobs_failed"] == 1
    assert summary["jobs_soft_match_unresolved"] == 0

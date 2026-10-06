"""`catalog --test-connection` must actually read the verdict.

POST /actions/testConnection answers 202 with an EMPTY body and the async
operation key in a response header. `oci raw-request` prints that as
{"data": null-or-{}, "headers": {...}, "status": "202 Accepted"}; the row
parser keeps the body and dropped the headers, and the catalog stage looked
for the key at the top level of the parsed row. It was never there, so the
poll loop was dead code, the outcome was the constant PENDING, and CATALOG.md
said nothing about the test at all -- a wrong role or a rotated password
read exactly like a working connection.
"""
import json
import types

import pytest

import snowmig
from report.render import render_catalog
from target import provisioning
from target.provisioning import (async_operation_key, make_provision_call,
                                 connection_test_outcome)


OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


def _proc(envelope):
    def fake(cmd):
        return types.SimpleNamespace(returncode=0, stderr="",
                                     stdout=json.dumps(envelope))
    return fake


def _probe(envelope):
    call = make_provision_call(OCID, run_process=_proc(envelope))
    return call("test_connection", body={"key": "cat-key"})


# --- where the key rides ----------------------------------------------------

def test_a_null_body_202_keeps_its_headers_and_yields_the_key():
    probe = _probe({"data": None, "status": "202 Accepted",
                    "headers": {"aidp-async-operation-key": "op-1"}})
    assert probe["_headers"]["aidp-async-operation-key"] == "op-1"
    assert async_operation_key(probe) == "op-1"


def test_an_empty_object_body_202_no_longer_loses_the_headers():
    # The shape that used to come back as a bare {}.
    probe = _probe({"data": {}, "status": "202 Accepted",
                    "headers": {"Datalake-Async-Operation-Key": "op-2"}})
    assert async_operation_key(probe) == "op-2"


@pytest.mark.parametrize("header", ["oidl-async-operation-key",
                                    "opc-work-request-id"])
def test_the_other_documented_header_spellings_are_read(header):
    probe = _probe({"data": {}, "status": "202 Accepted",
                    "headers": {header: "op-3"}})
    assert async_operation_key(probe) == "op-3"


def test_a_key_in_the_body_is_read_too():
    assert async_operation_key({"key": "op-4"}) == "op-4"
    assert async_operation_key({"data": {"key": "op-5"}}) == "op-5"
    # The parsed-row shape of a null body: the envelope itself.
    assert async_operation_key(
        {"data": None, "headers": {"aidp-async-operation-key": "op-6"},
         "status": "202 Accepted"}) == "op-6"


def test_no_key_anywhere_is_none():
    probe = _probe({"data": {}, "status": "202 Accepted",
                    "headers": {"opc-request-id": "r"}})
    assert async_operation_key(probe) is None
    assert async_operation_key({}) is None


def test_list_results_are_untouched_by_the_header_passthrough():
    call = make_provision_call(OCID, run_process=_proc(
        {"data": {"items": [{"key": "ws-1"}]}, "headers": {"x": "y"}}))
    assert call("list_workspaces") == {"items": [{"key": "ws-1"}]}


# --- the poll ----------------------------------------------------------------

class Polls:
    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.asked = []

    def __call__(self, op, **kw):
        assert op == "get_async_operation"
        self.asked.append(kw["key"])
        return self.statuses.pop(0)


def test_the_verdict_is_polled_to_a_terminal_state():
    slept = []
    polls = Polls({"status": "IN_PROGRESS"},
                  {"status": "FAILED", "errorCode": "CONNECTOR_0067",
                   "errorMessage": "Login has timed out"})
    out = connection_test_outcome(
        polls, {"_headers": {"aidp-async-operation-key": "op-1"}},
        delays=(5, 10, 15), sleep=slept.append)
    assert out["status"] == "FAILED"
    assert out["error"] == "CONNECTOR_0067: Login has timed out"
    assert out["operation_key"] == "op-1"
    assert "note" not in out, "a verdict carries no 'not yet readable' note"
    assert polls.asked == ["op-1", "op-1"]
    assert slept == [5, 10]


def test_success_is_reported_without_an_error():
    out = connection_test_outcome(Polls({"status": "SUCCEEDED"}),
                                  {"key": "op-9"}, delays=(0,),
                                  sleep=lambda s: None)
    assert out["status"] == "SUCCEEDED" and "error" not in out


def test_pending_means_the_budget_was_spent():
    polls = Polls(*[{"status": "IN_PROGRESS"}] * 3)
    out = connection_test_outcome(
        polls, {"_headers": {"aidp-async-operation-key": "op-1"}},
        delays=(1, 2, 3), sleep=lambda s: None)
    assert out["status"] == "PENDING"
    assert "IN_PROGRESS" in out["note"] and "3 polls" in out["note"]
    assert "not a pass" in out["note"]
    assert len(polls.asked) == 3


def test_a_missing_key_is_said_to_be_missing_and_nothing_is_polled():
    polls = Polls()
    out = connection_test_outcome(polls, {"_headers": {"opc-request-id": "r"}},
                                  sleep=lambda s: None)
    assert out["status"] == "PENDING" and out["operation_key"] is None
    assert "no async operation key" in out["note"]
    assert polls.asked == []


def test_a_poll_that_fails_is_unreadable_not_pending():
    def call(op, **kw):
        raise provisioning.ProvisionTransportError(
            "get_async_operation failed (exit 0): 404 NotFound")

    out = connection_test_outcome(call, {"key": "op-1"}, delays=(0,),
                                  sleep=lambda s: None)
    assert out["status"] == "UNREADABLE"
    assert "404" in out["error"]


# --- CATALOG.md ---------------------------------------------------------------

def _res(**test):
    return {"catalog": "db", "catalog_type": "EXTERNAL", "action": "created",
            "key": "db", "verified": True, "source_type": "SNOWFLAKE",
            "dry_run": False, "test_connection": test}


def test_catalog_md_reports_the_connection_test():
    md = render_catalog(_res(requested=True, status="FAILED",
                             error="CONNECTOR_0067: Login has timed out"))
    assert "## Connection test" in md
    assert "**FAILED**" in md and "CONNECTOR_0067" in md
    md = render_catalog(_res(requested=True, status="SUCCEEDED"))
    assert "**SUCCEEDED**" in md
    md = render_catalog(_res(requested=True, status="PENDING",
                             note="budget spent"))
    assert "PENDING is not a pass" in md and "budget spent" in md


def test_catalog_md_says_nothing_about_a_test_that_was_not_requested():
    res = _res()
    del res["test_connection"]
    assert "Connection test" not in render_catalog(res)


# --- end to end ---------------------------------------------------------------

def test_the_catalog_command_polls_and_reports_the_verdict(tmp_path,
                                                          monkeypatch):
    cfg = tmp_path / "snowmig-config.json"
    cfg.write_text(json.dumps({
        "snowflake": {"account": "ACME-TEST", "user": "READER",
                      "warehouse": "WH", "database": "DB", "auth": "password",
                      "password": "FAKE-NOT-A-REAL-PASSWORD"},
        "aidp": {"datalake_ocid": OCID}}), encoding="utf-8")

    asked = []

    def fake_proc(cmd):
        uri = cmd[cmd.index("--target-uri") + 1]
        asked.append(uri)
        if uri.endswith("/actions/testConnection"):
            env = {"data": None, "status": "202 Accepted",
                   "headers": {"aidp-async-operation-key": "op-abc-123"}}
        elif uri.endswith("/asyncOperations/op-abc-123"):
            env = {"data": {"status": "FAILED", "errorCode": "CONNECTOR_0067",
                            "errorMessage": "Login has timed out"},
                   "status": "200 OK"}
        else:
            raise AssertionError(uri)
        return types.SimpleNamespace(returncode=0, stderr="",
                                     stdout=json.dumps(env))

    real = provisioning.make_provision_call
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: real(ocid, run_process=fake_proc))
    monkeypatch.setattr(provisioning.time, "sleep", lambda s: None)
    monkeypatch.setattr(snowmig, "resolve_target", lambda **kw: object())
    monkeypatch.setattr(snowmig, "detect_backend", lambda: "oci_raw")
    monkeypatch.setattr(snowmig, "make_call", lambda target, **kw: None)
    monkeypatch.setattr(snowmig, "ensure_catalog", lambda **kw: {
        "catalog": "fake_db", "action": "created", "catalog_type": "EXTERNAL",
        "key": "cat-key-777", "verified": True})

    rc = snowmig.main(["catalog", "--out-dir", str(tmp_path), "--catalog",
                       "fake_db", "--config", str(cfg), "--execute",
                       "--test-connection"])
    assert rc == 0, "the verdict is reported; the command itself worked"
    assert any(u.endswith("/asyncOperations/op-abc-123") for u in asked), \
        asked
    res = json.loads((tmp_path / "catalog_result.json").read_text(
        encoding="utf-8"))
    assert res["test_connection"]["status"] == "FAILED"
    assert "CONNECTOR_0067" in res["test_connection"]["error"]
    assert res["test_connection"]["operation_key"] == "op-abc-123"
    md = (tmp_path / "CATALOG.md").read_text(encoding="utf-8")
    assert "Connection test" in md and "FAILED" in md
    assert "CONNECTOR_0067" in md
    assert "FAKE-NOT-A-REAL-PASSWORD" not in md

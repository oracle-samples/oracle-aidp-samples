"""An HTTP error in the response BODY is retried like one in the exit code.

`oci raw-request` exits 0 on an HTTP error and prints the error in its
envelope -- `{"status": "503 Service Unavailable", "data": {...}}` -- which
target/executor.py's BackendError docstring already records. The retry
layer wrapped only the subprocess call and its non-zero exit check; the
envelope was parsed into an exception AFTER retry_call had returned. So on
oci_raw, the one backend provisioning uses, a single throttle or 503 on
list_clusters, create_job, run_job, list_catalogs or a SQL statement failed
the stage on its first attempt, the `retry:` knobs had no effect, and the
phase report counted 0 retries. Every earlier retry test faked the error
as `returncode=1`, which is not what the oci CLI does.

The envelope is now parsed inside the retried attempt, so a body error goes
through the same rule as any other: a read on anything transient, a write
only on a refusal that proves nothing was applied (429, 409 "not in an
active state"). A write answered 5xx in the body is still sent ONCE.
"""
import json
from types import SimpleNamespace

import pytest

import retry
from retry import RetryPolicy

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    retry.set_policy(RetryPolicy())
    retry.drain_events()
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    yield
    retry.set_policy(RetryPolicy())
    retry.drain_events()


def _body(status, data=None):
    """What `oci raw-request` prints -- and it exits 0 either way."""
    return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps(
        {"status": status, "headers": {},
         "data": data if data is not None else
         {"code": status.split()[1] if " " in status else "Err",
          "message": status}}))


def _ok(data):
    return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps(
        {"status": "200 OK", "headers": {}, "data": data}))


class _Procs:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = 0

    def __call__(self, cmd, **_):
        self.calls += 1
        return self.answers.pop(0)


def _target():
    from target.coords import resolve_target
    return resolve_target(datalake_ocid=OCID, workspace="ws-fake",
                          cluster_id="cluster-fake", catalog="MYDB")


# ------------------------------------------------------------- provisioning

def _provision_call(procs):
    from target.provisioning import make_provision_call
    return make_provision_call(OCID, run_process=procs)


def test_provision_retries_a_read_answered_503_in_the_body():
    procs = _Procs(_body("503 Service Unavailable"),
                   _ok({"items": [{"key": "c1"}]}))
    out = _provision_call(procs)("list_clusters", workspace="ws-fake")
    assert out == {"items": [{"key": "c1"}]}
    assert procs.calls == 2
    assert [e["label"] for e in retry.drain_events()] == ["list_clusters"]


def test_provision_retries_a_write_throttled_429_in_the_body():
    procs = _Procs(_body("429 Too Many Requests"), _ok({"key": "job-1"}))
    out = _provision_call(procs)("create_job", workspace="ws-fake",
                                 body={"name": "j"})
    assert out["key"] == "job-1"
    assert procs.calls == 2


def test_provision_retries_the_cluster_race_answered_409_in_the_body():
    procs = _Procs(
        _body("409 Conflict", {"code": "Conflict",
                               "message": "workspace is not in an active "
                                          "state"}),
        _ok({"key": "cluster-1"}))
    _provision_call(procs)("create_cluster", workspace="ws-fake",
                           body={"displayName": "c"})
    assert procs.calls == 2


def test_provision_never_repeats_a_write_answered_500_in_the_body():
    from target.executor import BackendError
    procs = _Procs(_body("500 Internal Server Error"), _ok({"key": "dup"}))
    with pytest.raises(BackendError, match="500"):
        _provision_call(procs)("create_job", workspace="ws-fake",
                               body={"name": "j"})
    assert procs.calls == 1, "a write that may have applied is sent once"


# ------------------------------------------------------ catalog transport

def test_the_catalog_transport_retries_a_read_answered_500_in_the_body():
    from target.runner import make_call
    procs = _Procs(_body("500 Internal Server Error"),
                   _ok({"items": [{"key": "k"}]}))
    out = make_call(_target(), backend="oci_raw",
                    run_process=procs)("list_catalogs")
    assert out == {"items": [{"key": "k"}]}
    assert procs.calls == 2


def test_the_catalog_transport_never_repeats_a_create_answered_503():
    from target.executor import BackendError
    from target.runner import make_call
    procs = _Procs(_body("503 Service Unavailable"), _ok({}))
    with pytest.raises(BackendError):
        make_call(_target(), backend="oci_raw", run_process=procs)(
            "create_schema", catalog="k", body={"name": "s"})
    assert procs.calls == 1


# ------------------------------------------------------------ SQL transport

def test_the_sql_transport_retries_a_statement_throttled_in_the_body():
    from target.runner import make_run_sql
    procs = _Procs(_body("429 Too Many Requests"), _ok([{"n": 1}]))
    run = make_run_sql(_target(), backend="oci_raw", run_process=procs)
    run("CREATE SCHEMA IF NOT EXISTS x")
    assert procs.calls == 2


def test_the_sql_transport_sends_a_statement_answered_500_once():
    from target.executor import BackendError
    from target.runner import make_run_sql
    procs = _Procs(_body("500 Internal Server Error"), _ok([]))
    run = make_run_sql(_target(), backend="oci_raw", run_process=procs)
    with pytest.raises(BackendError):
        run("CREATE TABLE t (a INT)")
    assert procs.calls == 1

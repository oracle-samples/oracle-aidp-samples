"""One retry helper, exponential and configurable, at every call site.

Retry lived in isolated places with fixed delays: catalog_deploy's
(2.0, 5.0, 10.0) and the copy notebook's flat 30s. Now one policy --
base_delay * multiplier ** (attempt - 1), capped, max_attempts total -- set
in snowmig-config.yaml under `retry:`, is used by the AIDP call transports,
the Snowflake connection, the catalog deploy and the copy notebook.

What is retried is decided by what is SAFE to repeat: a read on any
transient failure; a write only on a response that proves it was not
applied (429 throttled, 409 "not in an active state").
"""
import pytest

import retry
from migration_config import ConfigError, retry_block
from retry import RetryPolicy, is_retryable, retry_call


@pytest.fixture(autouse=True)
def _clean():
    retry.set_policy(RetryPolicy())
    retry.drain_events()
    yield
    retry.set_policy(RetryPolicy())
    retry.drain_events()


# ------------------------------------------------------------ the policy

def test_backoff_is_exponential_and_capped():
    p = RetryPolicy(base_delay=2.0, multiplier=3.0, max_attempts=5,
                    max_delay=30.0)
    assert [p.delay(n) for n in (1, 2, 3, 4)] == [2.0, 6.0, 18.0, 30.0]
    assert p.schedule() == (2.0, 6.0, 18.0, 30.0)


def test_the_policy_comes_from_the_config():
    blk = retry_block({"retry": {"base_delay": 1, "multiplier": 2,
                                 "max_attempts": 3}})
    assert blk == {"base_delay": 1.0, "multiplier": 2.0, "max_attempts": 3,
                   "max_delay": 60.0}
    assert retry_block({})["max_attempts"] == 4


@pytest.mark.parametrize("bad", [{"base_delay": -1}, {"multiplier": 0.5},
                                 {"max_attempts": 0}, {"jitter": 1}])
def test_a_nonsense_policy_is_refused(bad):
    with pytest.raises(ConfigError):
        retry_block({"retry": bad})


# ------------------------------------------------------------ retry_call

def test_a_transient_failure_is_retried_until_it_succeeds():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("503 Service Unavailable")
        return "ok"
    out = retry_call(flaky, label="get_catalog", retryable=is_retryable(read=True),
                     policy=RetryPolicy(base_delay=1, multiplier=2,
                                        max_attempts=4),
                     sleep=sleeps.append)
    assert out == "ok" and len(calls) == 3 and sleeps == [1.0, 2.0]


def test_attempts_are_bounded_and_the_last_error_is_raised():
    sleeps = []
    with pytest.raises(RuntimeError, match="500"):
        retry_call(lambda: (_ for _ in ()).throw(RuntimeError("500 boom")),
                   label="x", retryable=is_retryable(read=True),
                   policy=RetryPolicy(base_delay=1, max_attempts=3),
                   sleep=sleeps.append)
    assert len(sleeps) == 2


def test_a_permanent_error_is_not_retried():
    sleeps = []
    with pytest.raises(RuntimeError):
        retry_call(lambda: (_ for _ in ()).throw(
                       RuntimeError("400 InvalidParameter")),
                   label="x", retryable=is_retryable(read=True),
                   sleep=sleeps.append)
    assert sleeps == []


@pytest.mark.parametrize("message,read,expected", [
    ("503 Service Unavailable", True, True),
    ("429 TooManyRequests", False, True),
    ("409 Conflict: is not in an active state", False, True),
    ("500 InternalError", False, False),   # a write that may have applied
    ("404 NotFound", True, False),
    ("timed out", True, True),
])
def test_what_is_safe_to_repeat(message, read, expected):
    assert is_retryable(read=read)(RuntimeError(message)) is expected


# ------------------------------------------------------------ call sites

def _target():
    from target.coords import resolve_target
    return resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                          workspace="w", cluster_id="c", catalog="MYDB")


def test_the_catalog_transport_retries_a_transient_read(monkeypatch):
    from types import SimpleNamespace
    from target.runner import make_call
    procs = [SimpleNamespace(returncode=1, stdout="", stderr="503 unavailable"),
             SimpleNamespace(returncode=0, stdout='{"data": {"k": 1}}',
                             stderr="")]
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    call = make_call(_target(), backend="oci_raw",
                     run_process=lambda cmd: procs.pop(0))
    call("list_catalogs")
    assert procs == []


def test_the_catalog_transport_never_retries_a_failed_create(monkeypatch):
    from types import SimpleNamespace
    from target.runner import make_call
    seen = []

    def run(cmd):
        seen.append(cmd)
        return SimpleNamespace(returncode=1, stdout="", stderr="500 internal")
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    call = make_call(_target(), backend="oci_raw", run_process=run)
    with pytest.raises(RuntimeError):
        call("create_schema", catalog="k", body={"name": "s"})
    assert len(seen) == 1


def test_provision_retries_the_cluster_create_race(monkeypatch):
    """Live 2026-09-24: create_cluster right after the workspace POST
    answered 409 'not in an active state'. That write was NOT applied, so
    it is safe to repeat."""
    from types import SimpleNamespace
    from target.provisioning import make_provision_call
    procs = [SimpleNamespace(returncode=1, stdout="",
                             stderr="409 Conflict ... is not in an active state"),
             SimpleNamespace(returncode=0, stdout='{"data": {}}', stderr="")]
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    call = make_provision_call("ocid1.aidataplatform.oc1.iad.a",
                               run_process=lambda cmd: procs.pop(0))
    call("create_cluster", workspace="w", body={"displayName": "c"})
    assert procs == []


def test_catalog_deploy_uses_the_policy_not_a_fixed_tuple():
    import inspect
    from target import catalog_deploy
    src = inspect.getsource(catalog_deploy)
    assert "(2.0, 5.0, 10.0)" not in src


def test_the_copy_notebook_backs_off_exponentially():
    import pathlib
    src = (pathlib.Path(__file__).resolve().parents[1] / "dataplane"
           / "02_copy_schema.py").read_text()
    assert "retry_wait: float = 30.0" not in src
    assert "retry_multiplier" in src and "** (attempt" in src


def test_the_sql_transport_retries_a_throttled_statement(monkeypatch):
    from types import SimpleNamespace
    from target.runner import make_run_sql
    procs = [SimpleNamespace(returncode=1, stdout="", stderr="429 TooManyRequests"),
             SimpleNamespace(returncode=0, stdout='{"data": []}', stderr="")]
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    run = make_run_sql(_target(), backend="oci_raw",
                       run_process=lambda cmd: procs.pop(0))
    run("CREATE SCHEMA IF NOT EXISTS x")
    assert procs == []


def test_the_sql_transport_does_not_repeat_a_statement_that_may_have_run(
        monkeypatch):
    from types import SimpleNamespace
    from target.runner import BackendError, make_run_sql
    seen = []

    def proc(cmd):
        seen.append(cmd)
        return SimpleNamespace(returncode=1, stdout="", stderr="500 internal")
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    run = make_run_sql(_target(), backend="oci_raw", run_process=proc)
    with pytest.raises(BackendError):
        run("CREATE TABLE t (a INT)")
    assert len(seen) == 1


# ------------------------------------ C15: every retry logged, per phase

def test_a_retry_is_logged_as_a_retry_with_attempt_delay_and_error(capsys):
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("503 Service Unavailable")
        return 1
    retry_call(flaky, label="list_catalogs", retryable=is_retryable(read=True),
               policy=RetryPolicy(base_delay=1.5, max_attempts=3),
               sleep=lambda s: None)
    # stdout: a retry is progress, and stderr is reserved for the one-line
    # `error:` a caller reads once the retries are exhausted.
    out = capsys.readouterr().out
    assert "RETRY list_catalogs: attempt 2/3 in 1.5s after: 503" in out
    ev = retry.drain_events()
    assert ev[0]["attempt"] == 2 and ev[0]["delay_seconds"] == 1.5
    assert "503" in ev[0]["error"]


def test_main_records_the_retries_of_a_stage(tmp_path, monkeypatch):
    import json
    import snowmig

    def stage(args):
        retry.note_retry("list_catalogs", 1, 4, 2.0, "503 unavailable")
        retry.note_retry("list_catalogs", 2, 4, 4.0, "503 unavailable")
        return 0
    monkeypatch.setattr(snowmig, "cmd_stages", stage)
    monkeypatch.setattr(snowmig, "build_parser", _parser_with(stage))
    assert snowmig.main(["stages", "--out-dir", str(tmp_path)]) == 0
    log = json.loads((tmp_path / "run_log.jsonl").read_text().splitlines()[-1])
    assert log["retries"] == 2
    events = [json.loads(l) for l in
              (tmp_path / "retries.jsonl").read_text().splitlines()]
    assert [e["stage"] for e in events] == ["stages", "stages"]
    assert events[1]["delay_seconds"] == 4.0


def _parser_with(func):
    import snowmig
    real = snowmig.build_parser

    def build():
        p = real()
        p._subparsers._group_actions[0].choices["stages"].set_defaults(
            func=func)
        return p
    return build


def test_the_phase_report_counts_and_lists_retries(tmp_path):
    import json
    from report.stages import phase_report
    from report.render import render_phase_report
    from report.tokens import record_stage_run
    record_stage_run(tmp_path, "assess", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:00:30+00:00", 0, None, retries=3)
    (tmp_path / "retries.jsonl").write_text(json.dumps(
        {"stage": "assess", "label": "snowflake connect", "attempt": 2,
         "max_attempts": 4, "delay_seconds": 2.0, "error": "251011 timeout",
         "at": "2026-09-24T10:00:05Z"}) + "\n")
    rep = phase_report(tmp_path)
    assess = next(p for p in rep["phases"] if p["stage"] == "assess")
    assert assess["retries"] == 3
    md = render_phase_report(rep)
    assert "## Retries" in md and "snowflake connect" in md
    assert "attempt 2/4" in md and "2.0s" in md


def test_retries_inside_a_workflow_are_counted_from_its_output(tmp_path):
    import json
    from report.stages import phase_report
    from report.tokens import record_stage_run
    record_stage_run(tmp_path, "copy-workflow", "2026-09-24T10:00:00+00:00",
                     "2026-09-24T10:10:00+00:00", 0, None,
                     job="snowmig_02_copy_schema")
    (tmp_path / "run_snowmig_02_copy_schema.json").write_text(json.dumps({
        "ok": True, "status": "SUCCESS",
        "output": ("[copy] ok\n"
                   "[copy]   RETRY `c`.`s`.`t`: attempt 2/3 in 30.0s after: x\n"
                   "[copy] a table named RETRY_LOG copied\n")}))
    copy = next(p for p in phase_report(tmp_path)["phases"]
                if p["stage"] == "copy-workflow")
    assert copy["retries"] == 1

"""A failed run says why, from the task run's errorTrace.

Live 2026-09-29, the 1,000-table scale copy: after 76 minutes the job run
ended FAILED with "Exception during execution of notebook", and the output
`run` fetched was the first ~10,800 characters of the notebook's log -- it
stopped at the 16th table. The cause was on the task run only:

    state.errorTrace = "TransientServiceError: {'target_service': 'dataflowdp',
                        'status': 503, 'code': 'ServiceUnavailable', ...
                        'operation_name': 'get_command_status', ...}"

-- the job runner lost the cluster's command status, a platform fault, not
the stage's. The watch now reads each task run's errorTrace when a run ends
not-successful, and flags that platform transient so the operator re-runs
the stage (its reports resume it) instead of debugging the notebook.
"""
from target.jobs import watch_job

TRACE_503 = ("TransientServiceError: {'target_service': 'dataflowdp', 'status': 503, "
             "'code': 'ServiceUnavailable', 'message': 'Service Unavailable', "
             "'operation_name': 'get_command_status'}")


class Fake:
    def __init__(self, status, trace=None, trace_fails=False):
        self.status, self.trace, self.trace_fails = status, trace, trace_fails
        self.ops = []

    def __call__(self, operation, **kw):
        self.ops.append(operation)
        if operation == "run_job":
            return {"key": "run-1"}
        if operation == "list_job_runs":
            return {"items": []}
        if operation == "get_job_run":
            return {"state": {"status": self.status,
                              "stateMessage": "Exception during execution of notebook x.ipynb."}}
        if operation == "list_task_runs":
            return {"items": [{"key": "task-1", "startTime": 1}]}
        if operation == "fetch_task_output":
            return {"data": []}
        if operation == "get_task_run":
            if self.trace_fails:
                raise RuntimeError("403 NotAuthorized")
            return {"key": "task-1", "state": {"status": self.status, "errorTrace": self.trace}}
        raise AssertionError(operation)


def _watch(fake):
    return watch_job(fake, workspace="ws", job_key="j", poll_seconds=0, max_polls=3,
                     sleep=lambda s: None, cold_start_restarts=0)


def test_a_failed_run_carries_the_task_error_trace():
    res = _watch(Fake("FAILED", TRACE_503))
    assert res["status"] == "FAILED" and not res["ok"]
    assert "TransientServiceError" in res["error_trace"]
    assert res["platform_transient"] is True


def test_a_notebook_error_is_not_called_a_platform_transient():
    res = _watch(Fake("FAILED", "AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] lake.s.t"))
    assert "TABLE_OR_VIEW_NOT_FOUND" in res["error_trace"]
    assert res["platform_transient"] is False


def test_an_unreadable_trace_leaves_the_verdict_alone():
    res = _watch(Fake("FAILED", trace_fails=True))
    assert res["status"] == "FAILED" and not res["ok"]
    assert res["error_trace"] is None and res["platform_transient"] is False


def test_a_successful_run_does_not_read_task_runs_for_a_trace():
    fake = Fake("SUCCESS")
    res = _watch(fake)
    assert res["ok"] and res["error_trace"] is None
    assert "get_task_run" not in fake.ops


def test_the_run_record_shows_the_trace_and_names_a_platform_transient():
    import snowmig
    md = snowmig._render_run({"job": "snowmig_02_copy_sales", "status": "FAILED",
                              "terminal": True, "ok": False,
                              "message": "Exception during execution of notebook x.ipynb.",
                              "error_trace": TRACE_503, "platform_transient": True})
    assert "## Task error trace" in md and "TransientServiceError" in md
    assert "re-run the same job to resume" in md

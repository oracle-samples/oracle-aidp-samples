"""Unit tests for is_transient + run_with_retry (P1.5α-fix20).

Validates:
  - Classifier: known transient patterns → True; known permanent → False;
    Py4J-style nested java_exception traversal; permanent wins over transient.
  - Retry loop: transient → retry; permanent → fail-fast; exhausted → re-raise
    last exception; eventual success → return value with correct call count.
  - Backoff schedule: 10s → 30s → 90s (3x factor); injected sleep for test speed.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.orchestrator.retry import (
    DEFAULT_MAX_RETRIES,
    is_transient,
    run_with_retry,
    _backoff_seconds,
)


# ---------------------------------------------------------------------------
# Fake Py4JJavaError shape (we don't depend on py4j in unit tests)
# ---------------------------------------------------------------------------


class _FakeJavaException:
    def __init__(self, message: str, class_name: str = "java.lang.RuntimeException") -> None:
        self._message = message
        self._class_name = class_name

    def getMessage(self) -> str:
        return self._message

    def getClass(self):
        outer = self

        class _C:
            def getName(self) -> str:
                return outer._class_name

        return _C()


class _FakePy4JJavaError(Exception):
    """Mirror of py4j.protocol.Py4JJavaError without the dependency."""

    def __init__(self, message: str, java_exception: _FakeJavaException) -> None:
        super().__init__(message)
        self.java_exception = java_exception


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


class TestIsTransient:
    def test_python_connection_reset_is_transient(self) -> None:
        assert is_transient(ConnectionResetError("Connection reset by peer"))

    def test_python_read_timeout_is_transient(self) -> None:
        # Use a normal exception with the magic string in args
        assert is_transient(RuntimeError("Read timed out after 60s"))

    def test_oci_object_storage_503_is_transient(self) -> None:
        assert is_transient(RuntimeError("503 Service Unavailable"))

    def test_oci_throttle_slowdown_is_transient(self) -> None:
        assert is_transient(RuntimeError("SlowDown — too many requests"))

    def test_spark_executor_lost_is_transient(self) -> None:
        assert is_transient(RuntimeError("Executor lost while running task"))

    def test_py4j_with_transient_java_message(self) -> None:
        """gl_coa's saveAsTable failure (TC26 live, run_id=da298a83)."""
        exc = _FakePy4JJavaError(
            "An error occurred while calling o577.saveAsTable.",
            _FakeJavaException("Connection reset by peer"),
        )
        assert is_transient(exc)

    def test_schema_not_found_is_permanent(self) -> None:
        """po_receipts on demo-pod (TC26 live, run_id=3f9b0648)."""
        exc = _FakePy4JJavaError(
            "An error occurred while calling o577.load.",
            _FakeJavaException("DATA_ACCESS_LAYER_0031 - Schema: SCM not found"),
        )
        assert not is_transient(exc)

    def test_unauthorized_is_permanent(self) -> None:
        assert not is_transient(RuntimeError("401 Unauthorized — bad creds"))

    def test_delta_schema_merge_is_permanent(self) -> None:
        """gl_coa with stale audit cols (TC26 live, run_id=023482f5 — fix16)."""
        exc = _FakePy4JJavaError(
            "AnalysisException",
            _FakeJavaException("DELTA_FAILED_TO_MERGE_FIELDS: _watermark_used"),
        )
        assert not is_transient(exc)

    def test_oom_is_permanent(self) -> None:
        # OOM happens on real loads, retry won't fix the executor config
        assert not is_transient(RuntimeError("java.lang.OutOfMemoryError: Java heap space"))

    def test_unknown_exception_defaults_to_permanent(self) -> None:
        """Bias toward fail-fast — unrecognized failure modes surface to operator."""
        assert not is_transient(RuntimeError("something weird happened"))

    def test_permanent_wins_when_both_patterns_present(self) -> None:
        """A message containing both Connection reset AND AccessDenied should
        be classified as permanent — the permanent pattern takes precedence
        because retry won't fix authorization."""
        exc = RuntimeError("Connection reset, also AccessDenied")
        assert not is_transient(exc)


# ---------------------------------------------------------------------------
# Backoff schedule
# ---------------------------------------------------------------------------


class TestBackoffSeconds:
    def test_default_schedule_is_10_30_90(self) -> None:
        # Attempt 1 → sleep BEFORE retry attempt 2
        assert _backoff_seconds(1, base_s=10.0, factor=3.0) == 10.0
        assert _backoff_seconds(2, base_s=10.0, factor=3.0) == 30.0
        assert _backoff_seconds(3, base_s=10.0, factor=3.0) == 90.0


# ---------------------------------------------------------------------------
# Retry loop
# ---------------------------------------------------------------------------


class TestRunWithRetry:
    def test_success_first_try_no_sleep(self) -> None:
        sleep = MagicMock()
        fn = MagicMock(return_value="ok")
        result = run_with_retry(fn, dataset_id="erp_suppliers", sleep=sleep)
        assert result == "ok"
        assert fn.call_count == 1
        sleep.assert_not_called()

    def test_transient_then_success(self) -> None:
        """The gl_coa-style scenario from TC26 live."""
        sleep = MagicMock()
        calls = []

        def fn():
            calls.append(1)
            if len(calls) == 1:
                raise _FakePy4JJavaError(
                    "saveAsTable failed",
                    _FakeJavaException("Connection reset by peer"),
                )
            return "ok"

        result = run_with_retry(fn, dataset_id="gl_coa", sleep=sleep)
        assert result == "ok"
        assert len(calls) == 2
        sleep.assert_called_once_with(10.0)  # First retry → 10s base

    def test_permanent_no_retry(self) -> None:
        """Schema-not-found should fail FAST — not waste 3 retries."""
        sleep = MagicMock()
        fn = MagicMock(side_effect=_FakePy4JJavaError(
            "load failed",
            _FakeJavaException("DATA_ACCESS_LAYER_0031 - Schema: SCM not found"),
        ))
        with pytest.raises(_FakePy4JJavaError):
            run_with_retry(fn, dataset_id="po_receipts", sleep=sleep)
        assert fn.call_count == 1, "permanent exceptions must not retry"
        sleep.assert_not_called()

    def test_exhaust_retries_reraises_last(self) -> None:
        """If transient happens every time, exhaust retries and re-raise."""
        sleep = MagicMock()
        transient = _FakePy4JJavaError("flaky", _FakeJavaException("Connection reset"))
        fn = MagicMock(side_effect=transient)
        with pytest.raises(_FakePy4JJavaError):
            run_with_retry(fn, dataset_id="erp_suppliers", sleep=sleep)
        # 1 initial + 3 retries (DEFAULT_MAX_RETRIES) = 4 attempts
        assert fn.call_count == DEFAULT_MAX_RETRIES + 1
        # Sleeps: 10s, 30s, 90s = 3 sleeps before exhaustion
        assert sleep.call_count == DEFAULT_MAX_RETRIES
        assert [c.args[0] for c in sleep.call_args_list] == [10.0, 30.0, 90.0]

    def test_zero_max_retries_means_one_attempt(self) -> None:
        sleep = MagicMock()
        fn = MagicMock(side_effect=_FakePy4JJavaError("flaky", _FakeJavaException("Connection reset")))
        with pytest.raises(_FakePy4JJavaError):
            run_with_retry(fn, dataset_id="ap_invoices", max_retries=0, sleep=sleep)
        assert fn.call_count == 1
        sleep.assert_not_called()

    def test_mixed_transient_then_permanent(self) -> None:
        """If we retry a transient, then hit a permanent, fail fast on the permanent."""
        sleep = MagicMock()
        attempts = []

        def fn():
            attempts.append(1)
            if len(attempts) == 1:
                raise _FakePy4JJavaError("retry me", _FakeJavaException("Connection reset"))
            raise _FakePy4JJavaError("nope", _FakeJavaException("401 Unauthorized"))

        with pytest.raises(_FakePy4JJavaError):
            run_with_retry(fn, dataset_id="gl_coa", sleep=sleep)
        assert len(attempts) == 2, "transient retried once, permanent failed fast"
        assert sleep.call_count == 1  # One sleep between the two attempts

    def test_custom_backoff_for_tests(self) -> None:
        """Tests can inject a fast backoff schedule to keep retries instant."""
        sleep = MagicMock()
        fn = MagicMock(side_effect=[
            ConnectionResetError("blip"),
            ConnectionResetError("blip"),
            "ok",
        ])
        result = run_with_retry(
            fn, dataset_id="ap_invoices",
            backoff_base_s=0.001, backoff_factor=1.0,
            sleep=sleep,
        )
        assert result == "ok"
        assert sleep.call_args_list == [((0.001,),), ((0.001,),)]

    def test_keyboard_interrupt_propagates_not_retried(self) -> None:
        """Reviewer catch: operator Ctrl-C must propagate IMMEDIATELY, not be
        treated as a transient (retried) or permanent (counted toward failed-
        step bookkeeping). Originally caught by ``except BaseException`` which
        is wrong — narrowed to ``except Exception`` + explicit re-raise of
        ``KeyboardInterrupt`` / ``SystemExit``.
        """
        sleep = MagicMock()
        attempts = []

        def fn():
            attempts.append(1)
            raise KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            run_with_retry(fn, dataset_id="gl_journal_lines", sleep=sleep)
        assert len(attempts) == 1, "KeyboardInterrupt must stop on first occurrence — not retry"
        sleep.assert_not_called()

    def test_system_exit_propagates_not_retried(self) -> None:
        """Same as Ctrl-C: ``sys.exit()`` must stop the retry loop."""
        sleep = MagicMock()
        fn = MagicMock(side_effect=SystemExit(1))
        with pytest.raises(SystemExit):
            run_with_retry(fn, dataset_id="ap_invoices", sleep=sleep)
        assert fn.call_count == 1
        sleep.assert_not_called()

    def test_silver_gold_count_must_be_inside_retried_callable(self) -> None:
        """Reviewer catch (fix17-20-skills PR): silver/gold's ``df.count()``
        MUST be inside the ``run_with_retry`` callable, not after it.

        Why: if ``builder()`` writes the table successfully but the subsequent
        ``df.count()`` hits a transient (Spark executor lost, OCI Object
        Storage 503 on the count's read path), the orchestrator would mark
        the step ``failed`` and cascade/abort downstream — even though the
        silver/gold table is already materialized correctly on disk.

        Mirror the production closure: ``_do_silver_gold()`` runs ``builder``
        then ``count`` and returns the count. The retry wrapper sees the
        count failure and retries the WHOLE thing (build is CREATE OR REPLACE
        under seed mode, so re-building is safe).
        """
        sleep = MagicMock()

        # Track builder + count calls to verify the WHOLE closure retried.
        builder_calls = []
        count_calls = []

        class _FakeDf:
            def __init__(self, fail_count_once: bool) -> None:
                self._fail_count_once = fail_count_once

            def count(self) -> int:
                count_calls.append(1)
                if self._fail_count_once and len(count_calls) == 1:
                    raise ConnectionResetError("transient count failure")
                return 42

        def fake_builder(spark, *, paths, run_id):
            builder_calls.append(1)
            # First call: builder + count(fails). Second call: builder + count(ok).
            fail_first = len(builder_calls) == 1
            return _FakeDf(fail_count_once=fail_first)

        def _do_silver_gold() -> int:
            # Mirrors the production closure in orchestrator/__init__.py
            df = fake_builder(spark=None, paths=None, run_id="r1")
            return df.count()

        row_count = run_with_retry(
            _do_silver_gold, dataset_id="dim_supplier",
            backoff_base_s=0.001, backoff_factor=1.0,
            sleep=sleep,
        )
        assert row_count == 42
        # Both the builder AND the count must have been called twice — the
        # retry RE-RAN the whole closure, not just the count.
        assert len(builder_calls) == 2, "builder must be retried (closure retry, not just count)"
        assert len(count_calls) == 2, "count must be retried"
        assert sleep.call_count == 1, "exactly one backoff between the failed and successful attempt"

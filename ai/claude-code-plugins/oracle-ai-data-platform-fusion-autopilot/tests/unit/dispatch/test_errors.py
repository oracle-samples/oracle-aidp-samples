"""P1.5ε §Step 6 — dispatch/errors.py taxonomy tests.

Locks the DISPATCH_* code values as a SemVer contract. Downstream tooling
greps stderr for these codes; renaming one is a breaking change.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_autopilot.dispatch.errors import (
    DispatchAuthError,
    DispatchClusterNotActiveError,
    DispatchConfigError,
    DispatchError,
    DispatchFetchOutputError,
    DispatchJobSubmitError,
    DispatchMarkerDegradedError,
    DispatchMarkerMissingError,
    DispatchPollTimeoutError,
    DispatchPreflightError,
    DispatchRunFailedError,
    DispatchUploadError,
    DispatchWheelBuildError,
)


@pytest.mark.parametrize(
    "cls,expected_code",
    [
        (DispatchConfigError, "DISPATCH_CONFIG_INVALID"),
        (DispatchAuthError, "DISPATCH_AUTH_OCI"),
        (DispatchPreflightError, "DISPATCH_PREFLIGHT_FAILED"),
        (DispatchWheelBuildError, "DISPATCH_WHEEL_BUILD_FAILED"),
        (DispatchClusterNotActiveError, "DISPATCH_CLUSTER_NOT_ACTIVE"),
        (DispatchUploadError, "DISPATCH_UPLOAD_HTTP"),
        (DispatchJobSubmitError, "DISPATCH_JOB_SUBMIT"),
        (DispatchPollTimeoutError, "DISPATCH_TIMEOUT"),
        (DispatchRunFailedError, "DISPATCH_RUN_FAILED"),
        (DispatchFetchOutputError, "DISPATCH_FETCH_OUTPUT"),
        (DispatchMarkerMissingError, "DISPATCH_MARKER_MISSING"),
    ],
)
def test_error_code_stability(cls, expected_code) -> None:
    """SemVer contract — operator tooling greps stderr for these codes."""
    assert cls.code == expected_code
    assert cls("anything").code == expected_code


@pytest.mark.parametrize(
    "cls",
    [
        DispatchConfigError,
        DispatchAuthError,
        DispatchPreflightError,
        DispatchWheelBuildError,
        DispatchUploadError,
        DispatchJobSubmitError,
        DispatchPollTimeoutError,
        DispatchRunFailedError,
        DispatchFetchOutputError,
        DispatchMarkerMissingError,
    ],
)
def test_str_prefixes_code(cls) -> None:
    err = cls("the message body")
    s = str(err)
    assert s.startswith(f"[{cls.code}]")
    assert "the message body" in s


def test_all_subclasses_are_dispatch_error() -> None:
    for cls in [
        DispatchConfigError,
        DispatchAuthError,
        DispatchPreflightError,
        DispatchWheelBuildError,
        DispatchClusterNotActiveError,
        DispatchUploadError,
        DispatchJobSubmitError,
        DispatchPollTimeoutError,
        DispatchRunFailedError,
        DispatchFetchOutputError,
        DispatchMarkerMissingError,
        DispatchMarkerDegradedError,
    ]:
        assert issubclass(cls, DispatchError)


# ---------------------------------------------------------------------------
# P1.5ε-fix5 — DispatchMarkerDegradedError code + str shape
# ---------------------------------------------------------------------------
# Separate test because the ctor requires keyword-only ``recovered_run_id``
# (the parametrize lists above call ``cls("anything")``, which doesn't fit).


def test_marker_degraded_code_stability() -> None:
    """SemVer contract — DISPATCH_MARKER_DEGRADED is grepped by operator
    tooling and the typed error class is constructed by
    ``dispatch_via_rest`` on the TC27 marker-parse trap path."""
    assert DispatchMarkerDegradedError.code == "DISPATCH_MARKER_DEGRADED"
    err = DispatchMarkerDegradedError(
        "the message body", recovered_run_id="abc-123",
    )
    assert err.code == "DISPATCH_MARKER_DEGRADED"
    assert err.recovered_run_id == "abc-123"


def test_marker_degraded_str_prefixes_code() -> None:
    err = DispatchMarkerDegradedError(
        "the message body", recovered_run_id="abc-123",
    )
    s = str(err)
    assert s.startswith("[DISPATCH_MARKER_DEGRADED]")
    assert "the message body" in s


def test_dispatch_error_is_not_a_value_error() -> None:
    """Distinct from OrchestratorConfigError's ValueError multi-inheritance
    — dispatch errors are network/state errors, not user-facing config
    bugs. A legacy `except ValueError` block must NOT catch them."""
    assert not isinstance(DispatchPreflightError("x"), ValueError)
    assert not issubclass(DispatchError, ValueError)

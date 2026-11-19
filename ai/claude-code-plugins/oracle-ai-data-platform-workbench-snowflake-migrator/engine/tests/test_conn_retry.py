"""The Snowflake connect retries a network blip, never a config mistake.

The set of driver errnos read as "could not reach it this time" held
251001, 253003 and 290400, though the comment above it says auth and config
errors are left out on purpose. 251001 is ER_NO_ACCOUNT_NAME, raised
LOCALLY -- no socket -- for "Invalid account identifier": pasting the
Account/Server URL into `account:`, the most common first-run mistake, made
preflight and every Snowflake-reading stage wait 14 s by default, print
three `RETRY snowflake connect` lines, and record a typo in retries.jsonl
and the phase report as network flakiness. 253003 is a stage-upload
failure and 290400 an HTTP 400: permanent both. 250003 (failed to request)
is transient, but NonRetryableTlsError carries the same errno for a TLS
failure the driver itself says cannot succeed on a retry.
"""
import pytest

import retry
from retry import RetryPolicy


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    retry.set_policy(RetryPolicy())
    retry.drain_events()
    monkeypatch.setattr(retry.time, "sleep", lambda s: None)
    yield
    retry.drain_events()


def _connect_raising(monkeypatch, make_error, succeed_after=None):
    import snowflake.connector
    calls = []

    def fake(**kwargs):
        calls.append(kwargs)
        if succeed_after is not None and len(calls) > succeed_after:
            return "connection"
        raise make_error()
    monkeypatch.setattr(snowflake.connector, "connect", fake)
    return calls


def _permanent():
    from snowflake.connector import errors
    return [
        lambda: errors.ProgrammingError(
            msg="Invalid account identifier: no slashes", errno=251001),
        lambda: errors.OperationalError(msg="400 Bad Request", errno=290400),
        lambda: errors.OperationalError(msg="stage upload", errno=253003),
        lambda: errors.NonRetryableTlsError(
            msg="certificate verify failed", errno=250003),
    ]


@pytest.mark.parametrize("index", range(4))
def test_a_permanent_error_fails_on_the_first_attempt(monkeypatch, index):
    from snowflake_source.conn import AuthError, connect
    calls = _connect_raising(monkeypatch, _permanent()[index])
    with pytest.raises(AuthError):
        connect(account="org-acct", user="U")
    assert len(calls) == 1
    assert retry.drain_events() == [], "a config error is not a network blip"


def test_a_url_pasted_as_the_account_says_so(monkeypatch):
    from snowflake.connector import errors
    from snowflake_source.conn import AuthError, connect
    _connect_raising(monkeypatch, lambda: errors.ProgrammingError(
        msg="Invalid account identifier", errno=251001))
    with pytest.raises(AuthError) as caught:
        connect(account="https://org-acct.snowflakecomputing.com", user="U")
    assert "identifier, not the URL" in str(caught.value)


@pytest.mark.parametrize("errno", [251011, 251012, 250003])
def test_a_network_blip_is_still_retried(monkeypatch, errno):
    from snowflake.connector import errors
    from snowflake_source.conn import connect
    calls = _connect_raising(
        monkeypatch,
        lambda: errors.OperationalError(msg="connection timed out",
                                        errno=errno),
        succeed_after=1)
    assert connect(account="org-acct", user="U") == "connection"
    assert len(calls) == 2
    assert [e["label"] for e in retry.drain_events()] == ["snowflake connect"]

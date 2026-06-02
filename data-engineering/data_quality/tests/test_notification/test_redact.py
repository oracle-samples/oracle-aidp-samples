"""URL-redaction helper + notifier log/persist sanitization.

Backstop for Phase 5 / acceptance gate 6: resolved credential
values must not appear in INFO/DEBUG/ERROR log records or in
persisted ``NotificationResult.message`` rows.
"""

from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from qualifire.core.models import DatasetResult, Severity
from qualifire.notification._redact import redact_jdbc_url, redact_url
from qualifire.notification.email_notifier import EmailNotifier
from qualifire.notification.slack_notifier import SlackNotifier
from qualifire.notification.webhook_notifier import WebhookNotifier


SECRET = "T0K3N-leaked-via-url"


def _ds():
    return DatasetResult(dataset_name="ds", table="t", validation_results=[], run_id="r")


# --- redact_url unit ---


@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://hooks.slack.com/services/T/B/" + SECRET, "https://hooks.slack.com/<redacted>"),
        ("https://x.example.com/webhook?token=" + SECRET, "https://x.example.com/<redacted>"),
        ("http://h:8080/path", "http://h:8080/<redacted>"),
        ("", "<unparseable-url>"),
        (None, "<unparseable-url>"),
        (123, "<unparseable-url>"),
        ("not-a-url", "<unparseable-url>"),
    ],
)
def test_redact_url(url, expected):
    assert redact_url(url) == expected


@pytest.mark.parametrize(
    "url, expected",
    [
        # Userinfo gets scrubbed; host/port/db preserved.
        ("jdbc:postgresql://user:pwd@h:5432/db",
         "jdbc:postgresql://<credentials>@h:5432/db"),
        # Oracle without inline creds — passes through.
        ("jdbc:oracle:thin:@h:1521/svc", "jdbc:oracle:thin:@h:1521/svc"),
        # Query-string credential param — only the value redacted.
        ("jdbc:mysql://h:3306/db?password=" + SECRET,
         "jdbc:mysql://h:3306/db?password=<redacted>"),
        # SQL Server semicolon-prop style.
        ("jdbc:sqlserver://h;user=u;password=" + SECRET,
         "jdbc:sqlserver://h;user=<redacted>;password=<redacted>"),
        # Opaque forms — passes through unchanged.
        ("jdbc:sqlite::memory:", "jdbc:sqlite::memory:"),
        ("", "<unparseable-jdbc-url>"),
        (None, "<unparseable-jdbc-url>"),
        ("https://example.com", "<unparseable-jdbc-url>"),
        ("jdbc:", "<unparseable-jdbc-url>"),
    ],
)
def test_redact_jdbc_url(url, expected):
    assert redact_jdbc_url(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        f"jdbc:postgresql://user:{SECRET}@h:5432/db",
        f"jdbc:mysql://h:3306/db?password={SECRET}",
        f"jdbc:sqlserver://h;user=u;password={SECRET};database=db",
        f"jdbc:postgresql://h:5432/db?user=u&password={SECRET}&ssl=true",
        # Codex r3 finding 2 — bypass attempts:
        # password contains literal '@' (last-@ semantics).
        f"jdbc:postgresql://user:p@{SECRET}@h:5432/db",
        # password contains '/' inside userinfo.
        f"jdbc:postgresql://user:p/{SECRET}@h:5432/db",
        # password value contains '&' in quoted string.
        f'jdbc:mysql://h:3306/db?password="{SECRET}&suffix"',
        # SQL Server brace-quoted value.
        f"jdbc:sqlserver://h;password={{{SECRET};extra}}",
        # Mixed `;` and `&` delimiters.
        f"jdbc:sqlserver://h;user=u;password={SECRET}&trailing=x",
    ],
)
def test_redact_jdbc_url_never_leaks_sentinel(url):
    """Sentinel must not survive in any output substring."""
    assert SECRET not in redact_jdbc_url(url)


@pytest.mark.parametrize(
    "url, expected_substr",
    [
        # Verify routing info preserved across all variants.
        ("jdbc:postgresql://h:5432/db?password=x", "h:5432/db"),
        ("jdbc:mysql://h:3306/db", "h:3306/db"),
        ("jdbc:sqlserver://h:1433;database=DB;password=x", "h:1433"),
    ],
)
def test_redact_jdbc_url_preserves_routing_info(url, expected_substr):
    assert expected_substr in redact_jdbc_url(url)


def test_redact_url_strips_basic_auth_userinfo():
    """Codex r3 finding 4 — `urlparse().netloc` includes user:password
    on HTTP basic-auth URLs. Make sure we rebuild from hostname+port."""
    out = redact_url(f"https://user:{SECRET}@example.com/hook")
    assert SECRET not in out
    assert "user" not in out
    assert "example.com" in out


# --- Slack: failure path ---


def test_slack_failure_redacts_url(caplog):
    n = SlackNotifier(webhook_url=f"https://hooks.slack.com/services/T/B/{SECRET}")
    fake_resp = MagicMock(side_effect=RuntimeError(f"connection refused for url with {SECRET}"))
    with caplog.at_level(logging.DEBUG):
        with patch("qualifire.notification.slack_notifier.requests.post", fake_resp):
            result = n.send(_ds(), Severity.ERROR, owner="o", bu="b")
    assert result.status == "failed"
    # No leak in persisted message.
    assert SECRET not in result.message
    # No leak in any log record (DEBUG/INFO/ERROR).
    for rec in caplog.records:
        assert SECRET not in rec.getMessage()


def test_slack_success_does_not_log_full_url(caplog):
    n = SlackNotifier(webhook_url=f"https://hooks.slack.com/services/T/B/{SECRET}")
    fake_resp = MagicMock(return_value=MagicMock(status_code=200, raise_for_status=lambda: None))
    with caplog.at_level(logging.DEBUG):
        with patch("qualifire.notification.slack_notifier.requests.post", fake_resp):
            n.send(_ds(), Severity.ERROR, owner="o", bu="b")
    # Slack success log doesn't print the URL — but make sure no log
    # record happens to contain the secret substring.
    for rec in caplog.records:
        assert SECRET not in rec.getMessage()


# --- Webhook: success + failure paths ---


def test_webhook_success_log_redacts_url(caplog):
    n = WebhookNotifier(url=f"https://api.example.com/hook?key={SECRET}")
    fake_resp = MagicMock(return_value=MagicMock(status_code=200, raise_for_status=lambda: None))
    with caplog.at_level(logging.DEBUG):
        with patch("qualifire.notification.webhook_notifier.requests.request", fake_resp):
            result = n.send(_ds(), Severity.ERROR, owner="o", bu="b")
    assert result.status == "sent"
    # Persisted message is HTTP status — no URL.
    assert SECRET not in result.message
    for rec in caplog.records:
        assert SECRET not in rec.getMessage()


def test_webhook_failure_redacts_in_log_and_message(caplog):
    n = WebhookNotifier(url=f"https://api.example.com/hook?key={SECRET}")
    err = RuntimeError(f"boom for url https://api.example.com/hook?key={SECRET}")
    fake_resp = MagicMock(side_effect=err)
    with caplog.at_level(logging.DEBUG):
        with patch("qualifire.notification.webhook_notifier.requests.request", fake_resp):
            result = n.send(_ds(), Severity.ERROR, owner="o", bu="b")
    assert result.status == "failed"
    assert SECRET not in result.message
    for rec in caplog.records:
        assert SECRET not in rec.getMessage()


# --- Email: failure path ---


def test_email_failure_message_omits_smtplib_detail(caplog):
    n = EmailNotifier(
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_user="qf",
        smtp_password="should-never-leak",
        sender="a@b",
        recipients=["x@y"],
    )
    err = RuntimeError(f"535 auth failed for password={SECRET}")
    with caplog.at_level(logging.DEBUG):
        with patch(
            "qualifire.notification.email_notifier.smtplib.SMTP",
            side_effect=err,
        ):
            result = n.send(_ds(), Severity.ERROR, owner="o", bu="b")
    assert result.status == "failed"
    # Persisted message must not carry resolver-supplied detail.
    assert SECRET not in result.message
    for rec in caplog.records:
        assert SECRET not in rec.getMessage()

"""Tests for email, slack, and webhook notifiers."""

from unittest.mock import MagicMock, patch

import pytest

from qualifire.core.models import DatasetResult, NotificationResult, Severity, ValidationResult
from qualifire.notification.base import format_notification_message, format_grouped_notification_message


def _make_dataset_result(severity=Severity.ERROR, name="sales", table="db.sales"):
    return DatasetResult(
        dataset_name=name,
        table=table,
        run_id="run-1",
        validation_results=[
            ValidationResult(
                validation_name="check",
                validation_type="threshold",
                severity=severity,
                message=f"row_count = 5 (violates {severity.value.lower()} threshold)",
                validation_base_name="check",
            )
        ],
    )


def _make_grouped_datasets():
    """Create 3 datasets for grouped notification testing."""
    return [
        _make_dataset_result(Severity.ERROR, "sales", "db.sales"),
        _make_dataset_result(Severity.ERROR, "inventory", "db.inventory"),
        _make_dataset_result(Severity.WARNING, "orders", "db.orders"),
    ]


class TestFormatNotificationMessage:
    def test_format_contains_key_fields(self):
        msg = format_notification_message(
            _make_dataset_result(), Severity.ERROR, "team-x", "finance"
        )
        assert "sales" in msg
        assert "team-x" in msg
        assert "finance" in msg
        assert "ERROR" in msg
        assert "row_count" in msg

    def test_pattern_includes_top_contributing_features(self):
        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="pattern_check",
                    validation_type="pattern",
                    severity=Severity.ERROR,
                    message="Pattern AUC: 0.91 (+/-0.03)",
                    validation_base_name="pattern_check",
                    details={
                        "auc": 0.910,
                        "auc_std": 0.030,
                        "n_current": 500,
                        "n_past": 1500,
                        "top_contributing_features": [
                            {"feature": "amount", "importance": 0.42},
                            {"feature": "channel_mobile", "importance": 0.18},
                            {"feature": "merchant_id", "importance": 0.09},
                        ],
                    },
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.ERROR, "team", "finance")
        # All features must be in the body — operators decide what
        # to act on; truncation hides signal.
        assert "amount" in msg
        assert "channel_mobile" in msg
        assert "merchant_id" in msg
        assert "0.4200" in msg or "0.42" in msg
        assert "AUC: 0.910" in msg

    def test_drift_includes_deviation_and_z_score(self):
        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="drift_check.avg_amount",
                    validation_type="drift",
                    severity=Severity.WARNING,
                    message="avg_amount = 142.5 deviates 28.4% from past mean",
                    actual_value=142.5,
                    validation_base_name="drift_check",
                    details={
                        "current_value": 142.5,
                        "mean_past": 110.0,
                        "deviation_pct": 29.5,
                        "z_score": 2.1,
                    },
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.WARNING, "team", "finance")
        assert "current: 142.5" in msg
        assert "past mean: 110" in msg
        assert "deviation_pct: 29.5" in msg
        assert "z_score: 2.1" in msg

    def test_threshold_zero_expected_value_surfaces(self):
        """``vr.expected_value=0`` (or ``False``, ``{}``) must still
        render — the threshold renderer used to truthiness-check and
        drop these. R4 review fix."""
        ds = DatasetResult(
            dataset_name="ds", table="t", run_id="r",
            validation_results=[
                ValidationResult(
                    validation_name="threshold_zero",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="cnt = -1",
                    actual_value=-1,
                    expected_value=0,
                    validation_base_name="threshold_zero",
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.ERROR, "t", "b")
        assert "threshold: 0" in msg

    def test_format_resilient_to_nondict_details(self):
        """``vr.details`` arriving as a non-mapping (corrupt
        persistence, third-party validator) must NOT crash the
        notification body — fall back to no-detail rendering."""
        ds = DatasetResult(
            dataset_name="ds", table="t", run_id="r",
            validation_results=[
                ValidationResult(
                    validation_name="custom_check",
                    validation_type="pattern",
                    severity=Severity.ERROR,
                    message="something",
                    validation_base_name="custom_check",
                    details="not a dict",  # type: ignore[arg-type]
                ),
            ],
        )
        # Must not raise.
        msg = format_notification_message(ds, Severity.ERROR, "t", "b")
        assert "[ERROR] custom_check" in msg

    def test_format_resilient_to_malformed_shap_item(self):
        """A non-dict entry in ``top_contributing_features`` (legacy
        validator producing a plain string) must not crash the body
        — fall back to rendering the string verbatim."""
        ds = DatasetResult(
            dataset_name="ds", table="t", run_id="r",
            validation_results=[
                ValidationResult(
                    validation_name="pattern_check",
                    validation_type="pattern",
                    severity=Severity.ERROR,
                    message="auc",
                    validation_base_name="pattern_check",
                    details={
                        "top_contributing_features": [
                            "amount",  # legacy: bare string
                            {"feature": "channel", "importance": 0.3},
                        ],
                    },
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.ERROR, "t", "b")
        assert "amount" in msg
        assert "channel" in msg

    def test_dimension_value_surfaces_in_details(self):
        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="threshold_check.row_count",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="row_count = 5 (violates error threshold)",
                    actual_value=5,
                    expected_value={"min": 100},
                    dimension_value='{"region": "us"}',
                    validation_base_name="threshold_check",
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.ERROR, "team", "finance")
        assert '{"region": "us"}' in msg
        assert "actual: 5" in msg

    def test_format_filters_by_severity(self):
        ds = DatasetResult(
            dataset_name="ds",
            table="t",
            run_id="r",
            validation_results=[
                ValidationResult(
                    validation_name="pass_check", validation_type="t",
                    severity=Severity.PASS, message="ok",
                    validation_base_name="pass_check",
                ),
                ValidationResult(
                    validation_name="warn_check", validation_type="t",
                    severity=Severity.WARNING, message="warning msg",
                    validation_base_name="warn_check",
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.WARNING, "o", "b")
        assert "warning msg" in msg
        assert "ok" not in msg  # PASS filtered out

    def test_partition_ts_rendered_below_table_when_set(self):
        """When the dataset carries a resolved partition stamp, the
        notification body must render ``Partition: <iso>`` directly
        below the ``Table:`` line so an alert tells the operator
        *which* partition tripped the check, not just *which*
        dataset.
        """
        from datetime import datetime

        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="run-1",
            partition_ts=datetime(2026, 5, 8),
            validation_results=[
                ValidationResult(
                    validation_name="check",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="msg",
                    validation_base_name="check",
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.ERROR, "team", "bu")
        # Line ordering pin: Table → Partition → Owner.
        lines = msg.splitlines()
        table_idx = next(i for i, l in enumerate(lines) if l.startswith("Table:"))
        partition_idx = next(
            i for i, l in enumerate(lines) if l.startswith("Partition:")
        )
        owner_idx = next(i for i, l in enumerate(lines) if l.startswith("Owner:"))
        assert table_idx < partition_idx < owner_idx, (
            f"Expected Table → Partition → Owner ordering; got\n{msg}"
        )
        assert "Partition: 2026-05-08T00:00:00" in msg

    def test_partition_ts_omitted_when_none(self):
        """No ``Partition:`` line should appear when the dataset
        doesn't declare a partition expression — the notifier must
        not emit an empty line or a literal 'None'.
        """
        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="run-1",
            partition_ts=None,
            validation_results=[
                ValidationResult(
                    validation_name="check",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="msg",
                    validation_base_name="check",
                ),
            ],
        )
        msg = format_notification_message(ds, Severity.ERROR, "team", "bu")
        assert "Partition:" not in msg
        assert "None" not in msg


class TestFormatGroupedNotificationMessage:
    def test_per_dataset_partition_in_section_header_when_set(self):
        from datetime import datetime

        d1 = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="run-1",
            partition_ts=datetime(2026, 5, 8),
            validation_results=[ValidationResult(
                validation_name="c", validation_type="threshold",
                severity=Severity.ERROR, message="m",
                validation_base_name="c",
            )],
        )
        d2 = DatasetResult(
            dataset_name="orders",
            table="db.orders",
            run_id="run-1",
            partition_ts=None,  # mixed: one has partition, one doesn't
            validation_results=[ValidationResult(
                validation_name="c", validation_type="threshold",
                severity=Severity.ERROR, message="m",
                validation_base_name="c",
            )],
        )
        msg = format_grouped_notification_message(
            [d1, d2], Severity.ERROR, "team", "bu", run_id="run-1",
        )
        # Section header for the partitioned dataset carries the
        # stamp inline; the non-partitioned section omits it.
        assert "--- sales (db.sales) @ 2026-05-08T00:00:00 ---" in msg
        assert "--- orders (db.orders) ---" in msg
        assert "@ None" not in msg


class TestConsoleNotifier:
    def test_send_prints_to_stdout(self, capsys):
        from qualifire.notification.console_notifier import ConsoleNotifier

        notifier = ConsoleNotifier()
        result = notifier.send(_make_dataset_result(), Severity.ERROR, "owner-x", "bu-y")

        captured = capsys.readouterr()
        assert "Qualifire Alert — ERROR" in captured.out
        assert "owner-x" in captured.out
        assert "bu-y" in captured.out
        assert result.status == "sent"
        assert result.channel == "console"

    def test_send_handles_stdout_failure(self, monkeypatch):
        """A closed / broken stdout shouldn't crash the alert path."""
        from qualifire.notification.console_notifier import ConsoleNotifier

        def _broken_print(*a, **kw):
            raise OSError("Broken pipe")

        monkeypatch.setattr("builtins.print", _broken_print)
        notifier = ConsoleNotifier()
        result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")
        assert result.status == "failed"
        assert "OSError" in result.message

    def test_channel_name(self):
        from qualifire.notification.console_notifier import ConsoleNotifier

        assert ConsoleNotifier().channel_name == "console"

    def test_registry_resolves_console(self):
        """``get_notifier('console')`` must return the ConsoleNotifier
        class — confirms the side-effect ``register_notifier`` call
        in ``console_notifier.py`` fires on import."""
        from qualifire.notification.base import get_notifier
        from qualifire.notification.console_notifier import ConsoleNotifier

        # Force the import so the registry side-effect runs.
        import qualifire.notification.console_notifier  # noqa: F401

        cls = get_notifier("console")
        assert cls is ConsoleNotifier


class TestEmailNotifier:
    def test_send_success(self):
        from qualifire.notification.email_notifier import EmailNotifier

        notifier = EmailNotifier(
            smtp_host="smtp.test.com",
            smtp_port=587,
            recipients=["a@b.com"],
        )
        with patch("qualifire.notification.email_notifier.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

            result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert result.status == "sent"
        assert result.channel == "email"

    def test_send_failure(self):
        from qualifire.notification.email_notifier import EmailNotifier

        notifier = EmailNotifier(smtp_host="bad", recipients=["a@b.com"])
        with patch("qualifire.notification.email_notifier.smtplib.SMTP", side_effect=ConnectionError("refused")):
            result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert result.status == "failed"

    def test_channel_name(self):
        from qualifire.notification.email_notifier import EmailNotifier

        n = EmailNotifier(smtp_host="h", recipients=[])
        assert n.channel_name == "email"


class TestSlackNotifier:
    def test_send_success(self):
        from qualifire.notification.slack_notifier import SlackNotifier

        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/test")
        with patch("qualifire.notification.slack_notifier.requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.raise_for_status = MagicMock()

            result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert result.status == "sent"
        assert result.channel == "slack"
        mock_post.assert_called_once()
        payload = mock_post.call_args[1]["json"]
        assert "ERROR" in payload["text"]

    def test_send_failure(self):
        from qualifire.notification.slack_notifier import SlackNotifier

        notifier = SlackNotifier(webhook_url="https://bad")
        with patch("qualifire.notification.slack_notifier.requests.post", side_effect=ConnectionError):
            result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert result.status == "failed"

    def test_channel_name(self):
        from qualifire.notification.slack_notifier import SlackNotifier

        assert SlackNotifier("url").channel_name == "slack"


class TestWebhookNotifier:
    def test_send_post(self):
        from qualifire.notification.webhook_notifier import WebhookNotifier

        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        with patch("qualifire.notification.webhook_notifier.requests.request") as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()

            result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert result.status == "sent"
        mock_req.assert_called_once()
        assert mock_req.call_args[0][0] == "POST"

    def test_send_put_with_headers(self):
        from qualifire.notification.webhook_notifier import WebhookNotifier

        notifier = WebhookNotifier(
            url="https://test", method="PUT",
            headers={"Authorization": "Bearer tok"},
        )
        with patch("qualifire.notification.webhook_notifier.requests.request") as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()

            notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert mock_req.call_args[0][0] == "PUT"
        headers = mock_req.call_args[1]["headers"]
        assert headers["Authorization"] == "Bearer tok"

    def test_send_failure(self):
        from qualifire.notification.webhook_notifier import WebhookNotifier

        notifier = WebhookNotifier(url="https://bad")
        with patch("qualifire.notification.webhook_notifier.requests.request", side_effect=ConnectionError):
            result = notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        assert result.status == "failed"

    def test_channel_name(self):
        from qualifire.notification.webhook_notifier import WebhookNotifier

        assert WebhookNotifier("url").channel_name == "webhook"

    def test_payload_handles_cyclic_details_without_stack_overflow(self):
        """A cyclic ``vr.details`` must not crash the webhook
        delivery — the sanitizer replaces repeat container references
        with a sentinel."""
        import json
        from qualifire.notification.webhook_notifier import WebhookNotifier

        cycle = {"a": 1}
        cycle["self"] = cycle  # self-reference

        ds = DatasetResult(
            dataset_name="sales", table="db.sales", run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="threshold_check",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="x = 1",
                    validation_base_name="threshold_check",
                    details=cycle,
                ),
            ],
        )
        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        with patch(
            "qualifire.notification.webhook_notifier.requests.request"
        ) as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()
            notifier.send(ds, Severity.ERROR, "team", "finance")

        payload = json.loads(mock_req.call_args[1]["data"])
        assert payload["validations"][0]["details"]["self"] == "<cycle>"
        assert payload["validations"][0]["details"]["a"] == 1

    def test_payload_handles_bytes_values(self):
        """Byte payloads decode as UTF-8 when valid, base64 fallback
        otherwise — raw bytes shouldn't reach json.dumps."""
        import json
        from qualifire.notification.webhook_notifier import WebhookNotifier

        ds = DatasetResult(
            dataset_name="sales", table="db.sales", run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="threshold_check",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="x",
                    validation_base_name="threshold_check",
                    details={
                        "utf8_blob": b"hello world",
                        "binary_blob": b"\xff\xfe\xfd",
                    },
                ),
            ],
        )
        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        with patch(
            "qualifire.notification.webhook_notifier.requests.request"
        ) as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()
            notifier.send(ds, Severity.ERROR, "team", "finance")

        payload = json.loads(mock_req.call_args[1]["data"])
        details = payload["validations"][0]["details"]
        assert details["utf8_blob"] == "hello world"
        assert "__b64__" in details["binary_blob"]

    def test_payload_sanitizes_non_json_values(self):
        """``vr.details`` may carry numpy scalars / pandas Timestamps
        / Decimal / datetime. The webhook serializer must unwrap them
        into pure JSON, not stringify or crash."""
        import json
        from datetime import datetime
        from decimal import Decimal
        from qualifire.notification.webhook_notifier import WebhookNotifier

        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="threshold_check",
                    validation_type="threshold",
                    severity=Severity.ERROR,
                    message="amount = 142.5",
                    actual_value=Decimal("142.5"),
                    validation_base_name="threshold_check",
                    details={
                        "as_of": datetime(2026, 4, 28, 12, 0),
                        "z_score": Decimal("2.1"),
                        "buckets": [Decimal("1.0"), Decimal("2.0")],
                        "nested": {"latency_ms": Decimal("300")},
                    },
                ),
            ],
        )
        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        with patch(
            "qualifire.notification.webhook_notifier.requests.request"
        ) as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()
            notifier.send(ds, Severity.ERROR, "team", "finance")

        payload = json.loads(mock_req.call_args[1]["data"])
        v = payload["validations"][0]
        assert v["actual_value"] == 142.5
        assert v["details"]["as_of"].startswith("2026-04-28T")
        assert v["details"]["z_score"] == 2.1
        assert v["details"]["buckets"] == [1.0, 2.0]
        assert v["details"]["nested"]["latency_ms"] == 300

    def test_payload_includes_details_and_dimension(self):
        """Webhook payload must carry ``details`` (SHAP top features,
        drift z-scores, forecast bands) and ``dimension_value`` so
        downstream pipelines don't have to re-fetch from the system
        table."""
        import json
        from qualifire.notification.webhook_notifier import WebhookNotifier

        ds = DatasetResult(
            dataset_name="sales",
            table="db.sales",
            run_id="r1",
            validation_results=[
                ValidationResult(
                    validation_name="pattern_check",
                    validation_type="pattern",
                    severity=Severity.ERROR,
                    message="Pattern AUC: 0.90",
                    actual_value=0.90,
                    dimension_value='{"region": "us"}',
                    validation_base_name="pattern_check",
                    details={
                        "auc": 0.90,
                        "top_contributing_features": [
                            {"feature": "amount", "importance": 0.42},
                        ],
                    },
                ),
            ],
        )
        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        with patch(
            "qualifire.notification.webhook_notifier.requests.request"
        ) as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()

            notifier.send(ds, Severity.ERROR, "team", "finance")

        payload = json.loads(mock_req.call_args[1]["data"])
        v = payload["validations"][0]
        assert v["dimension_value"] == '{"region": "us"}'
        assert v["details"]["auc"] == 0.90
        assert v["details"]["top_contributing_features"][0]["feature"] == "amount"


# ===========================================================================
# Grouped notification tests
# ===========================================================================


class TestFormatGroupedNotificationMessage:
    def test_single_dataset_delegates_to_single_format(self):
        datasets = [_make_dataset_result()]
        msg = format_grouped_notification_message(datasets, Severity.ERROR, "o", "b")
        # Should use single-dataset format (includes "Dataset:" line)
        assert "Dataset: sales" in msg

    def test_multi_dataset_shows_count_and_all_datasets(self):
        datasets = _make_grouped_datasets()
        msg = format_grouped_notification_message(datasets, Severity.ERROR, "o", "b")
        assert "Datasets: 3" in msg
        assert "sales" in msg
        assert "inventory" in msg
        assert "orders" in msg

    def test_multi_dataset_filters_by_severity(self):
        datasets = _make_grouped_datasets()
        msg = format_grouped_notification_message(datasets, Severity.ERROR, "o", "b")
        # orders has WARNING which is < ERROR, so its results should show "All validations passed"
        assert "All validations passed" in msg


class TestGroupedSlackNotifier:
    def test_grouped_title_shows_dataset_count(self):
        from qualifire.notification.slack_notifier import SlackNotifier

        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/test")
        datasets = _make_grouped_datasets()
        with patch("qualifire.notification.slack_notifier.requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.raise_for_status = MagicMock()

            notifier.send(
                _make_dataset_result(), Severity.ERROR, "o", "b",
                datasets=datasets,
            )

        payload = mock_post.call_args[1]["json"]
        assert "3 datasets" in payload["text"]
        assert "ERROR" in payload["text"]

    def test_single_dataset_uses_standard_title(self):
        from qualifire.notification.slack_notifier import SlackNotifier

        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/test")
        with patch("qualifire.notification.slack_notifier.requests.post") as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.raise_for_status = MagicMock()

            notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        payload = mock_post.call_args[1]["json"]
        assert "datasets" not in payload["text"]
        assert "ERROR" in payload["text"]


class TestGroupedEmailNotifier:
    def test_grouped_subject_shows_dataset_count(self):
        from qualifire.notification.email_notifier import EmailNotifier

        notifier = EmailNotifier(smtp_host="smtp.test.com", recipients=["a@b.com"])
        datasets = _make_grouped_datasets()
        with patch("qualifire.notification.email_notifier.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

            notifier.send(
                _make_dataset_result(), Severity.ERROR, "o", "b",
                datasets=datasets,
            )

        # Check the subject of the sent email
        sent_msg = mock_server.send_message.call_args[0][0]
        assert "3 datasets" in sent_msg["Subject"]
        assert "ERROR" in sent_msg["Subject"]

    def test_single_dataset_subject_uses_dataset_name(self):
        from qualifire.notification.email_notifier import EmailNotifier

        notifier = EmailNotifier(smtp_host="smtp.test.com", recipients=["a@b.com"])
        with patch("qualifire.notification.email_notifier.smtplib.SMTP") as mock_smtp:
            mock_server = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

            notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        sent_msg = mock_server.send_message.call_args[0][0]
        assert "sales" in sent_msg["Subject"]
        assert "datasets" not in sent_msg["Subject"]


class TestGroupedWebhookNotifier:
    def test_grouped_payload_includes_datasets_array(self):
        import json
        from qualifire.notification.webhook_notifier import WebhookNotifier

        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        datasets = _make_grouped_datasets()
        with patch("qualifire.notification.webhook_notifier.requests.request") as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()

            notifier.send(
                _make_dataset_result(), Severity.ERROR, "o", "b",
                datasets=datasets,
            )

        sent_data = json.loads(mock_req.call_args[1]["data"])
        assert "datasets" in sent_data
        assert len(sent_data["datasets"]) == 3
        assert sent_data["datasets"][0]["dataset_name"] == "sales"
        assert sent_data["datasets"][1]["dataset_name"] == "inventory"

    def test_single_dataset_payload_has_no_datasets_array(self):
        import json
        from qualifire.notification.webhook_notifier import WebhookNotifier

        notifier = WebhookNotifier(url="https://webhook.test/endpoint")
        with patch("qualifire.notification.webhook_notifier.requests.request") as mock_req:
            mock_req.return_value.status_code = 200
            mock_req.return_value.raise_for_status = MagicMock()

            notifier.send(_make_dataset_result(), Severity.ERROR, "o", "b")

        sent_data = json.loads(mock_req.call_args[1]["data"])
        assert "datasets" not in sent_data

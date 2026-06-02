"""Slack webhook notifier."""

from __future__ import annotations

import logging

import requests

from qualifire.core.models import DatasetResult, NotificationResult, Severity
from qualifire.notification._redact import redact_url
from qualifire.notification.base import Notifier, register_notifier

logger = logging.getLogger(__name__)


class SlackNotifier(Notifier):
    """Send notifications via Slack incoming webhook.

    Args:
        webhook_url: Slack incoming webhook URL.
    """

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @property
    def channel_name(self) -> str:
        return "slack"

    def send(
        self,
        dataset_result: DatasetResult,
        severity: Severity,
        owner: str,
        bu: str,
        datasets: list[DatasetResult] | None = None,
    ) -> NotificationResult:
        text = self._format_message(dataset_result, severity, owner, bu, datasets)
        emoji = ":warning:" if severity == Severity.WARNING else ":x:"
        if datasets and len(datasets) > 1:
            title = f"{emoji} *Qualifire {severity.value} — {len(datasets)} datasets*"
        else:
            title = f"{emoji} *Qualifire {severity.value}*"
        payload = {
            "text": f"{title}\n```{text}```",
        }

        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=30)
            resp.raise_for_status()
            logger.info("Slack notification sent for %s", dataset_result.dataset_name)
            return NotificationResult(
                channel="slack", severity=severity, status="sent",
                message="Webhook delivered",
            )
        except Exception as e:
            # ``str(e)`` from ``requests`` includes the URL — which on
            # Slack incoming webhooks IS the secret. Log + persist the
            # exception type only.
            safe = f"{type(e).__name__} for {redact_url(self.webhook_url)}"
            logger.error("Slack notification failed: %s", safe)
            return NotificationResult(
                channel="slack", severity=severity, status="failed",
                message=safe,
            )


register_notifier("slack", SlackNotifier)

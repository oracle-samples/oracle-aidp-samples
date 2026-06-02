"""SMTP email notifier."""

from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from qualifire.core.models import DatasetResult, NotificationResult, Severity
from qualifire.notification.base import Notifier, register_notifier

logger = logging.getLogger(__name__)


class EmailNotifier(Notifier):
    """Send notifications via SMTP email.

    Args:
        smtp_host: SMTP server hostname.
        smtp_port: SMTP server port.
        smtp_user: SMTP username (optional).
        smtp_password: SMTP password (optional).
        use_tls: Use STARTTLS.
        sender: From address.
        recipients: List of recipient email addresses.
    """

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int = 587,
        smtp_user: str | None = None,
        smtp_password: str | None = None,
        use_tls: bool = True,
        sender: str | None = None,
        recipients: list[str] | None = None,
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.use_tls = use_tls
        self.sender = sender or smtp_user or "qualifire@localhost"
        self.recipients = recipients or []

    @property
    def channel_name(self) -> str:
        return "email"

    def send(
        self,
        dataset_result: DatasetResult,
        severity: Severity,
        owner: str,
        bu: str,
        datasets: list[DatasetResult] | None = None,
    ) -> NotificationResult:
        body = self._format_message(dataset_result, severity, owner, bu, datasets)
        if datasets and len(datasets) > 1:
            subject = f"[Qualifire {severity.value}] {len(datasets)} datasets — {owner}/{bu}"
        else:
            subject = f"[Qualifire {severity.value}] {dataset_result.dataset_name}"

        msg = MIMEMultipart()
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            logger.info("Email sent to %s for %s", self.recipients, dataset_result.dataset_name)
            return NotificationResult(
                channel="email", severity=severity, status="sent",
                message=f"Sent to {', '.join(self.recipients)}",
            )
        except Exception as e:
            # ``smtplib`` exception messages may co-locate host with
            # an auth-failure detail — co-locating the host with a
            # credential-shaped error is the classic auth-failure
            # leak vector. Log + persist exception type + host only.
            safe = f"{type(e).__name__} on {self.smtp_host}:{self.smtp_port}"
            logger.error("Email send failed: %s", safe)
            return NotificationResult(
                channel="email", severity=severity, status="failed",
                message=safe,
            )


register_notifier("email", EmailNotifier)

"""Generic HTTP webhook notifier."""

from __future__ import annotations

import json
import logging

import requests

from qualifire.core.models import DatasetResult, NotificationResult, Severity
from qualifire.notification._redact import redact_url
from qualifire.notification.base import Notifier, register_notifier

logger = logging.getLogger(__name__)


class WebhookNotifier(Notifier):
    """Send notifications via generic HTTP webhook.

    Sends a JSON payload with the full dataset result details.

    Args:
        url: Webhook endpoint URL.
        method: HTTP method (POST or PUT).
        headers: Optional extra HTTP headers.
    """

    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
    ):
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}

    @property
    def channel_name(self) -> str:
        return "webhook"

    def send(
        self,
        dataset_result: DatasetResult,
        severity: Severity,
        owner: str,
        bu: str,
        datasets: list[DatasetResult] | None = None,
    ) -> NotificationResult:
        payload = {
            "severity": severity.value,
            "dataset_name": dataset_result.dataset_name,
            "table": dataset_result.table,
            "owner": owner,
            "bu": bu,
            "run_id": dataset_result.run_id,
            "validations": [
                _validation_payload(vr) for vr in dataset_result.validation_results
                if vr.severity >= severity
            ],
        }
        if datasets and len(datasets) > 1:
            payload["datasets"] = [
                {
                    "dataset_name": ds.dataset_name,
                    "table": ds.table,
                    "validations": [
                        _validation_payload(vr) for vr in ds.validation_results
                        if vr.severity >= severity
                    ],
                }
                for ds in datasets
            ]

        headers = {"Content-Type": "application/json", **self.headers}

        try:
            resp = requests.request(
                self.method, self.url,
                data=json.dumps(payload, default=str),
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            # ``self.url`` may carry a bearer secret (Slack-style webhooks
            # encode the token in the path, generic webhooks may include
            # it in the query). Log only host+scheme.
            logger.info(
                "Webhook sent to %s for %s",
                redact_url(self.url),
                dataset_result.dataset_name,
            )
            return NotificationResult(
                channel="webhook", severity=severity, status="sent",
                message=f"HTTP {resp.status_code}",
            )
        except Exception as e:
            # ``str(e)`` from ``requests`` typically embeds the full URL —
            # which after secret resolution carries the credential. Log
            # and persist the exception type only.
            safe = f"{type(e).__name__} for {redact_url(self.url)}"
            logger.error("Webhook failed: %s", safe)
            return NotificationResult(
                channel="webhook", severity=severity, status="failed",
                message=safe,
            )


def _safe_serialize(value, _seen: set | None = None):
    """Convert a single value to a JSON-safe primitive.

    ``vr.details`` is built by validators that may stash numpy
    scalars, ``Decimal``, ``datetime``, ``pandas.Timestamp``, or
    nested containers. Letting ``json.dumps(default=str)`` handle
    them produces ugly stringified payloads (``"Decimal('1.0')"``)
    and risks subtle receiver bugs. This helper unwraps the common
    types into pure JSON.

    Cycle protection: containers are tracked by ``id()`` and a
    repeat reference is replaced with the sentinel string
    ``"<cycle>"`` so a self-referential ``details`` doesn't blow
    the stack and drop the entire webhook delivery.

    ``bytes`` / ``bytearray`` / ``memoryview`` decode as UTF-8 when
    valid, otherwise emit a base64-encoded string so receivers see
    something useful instead of an unstable ``repr``.
    """
    from base64 import b64encode
    from decimal import Decimal

    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, Decimal):
        # Decimals serialize as floats. Receivers that need exact
        # precision should fetch from the system table; this payload
        # is for human and incident-pipeline readability.
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return {"__b64__": b64encode(raw).decode("ascii")}
    # numpy scalars expose ``.item()``; the same trick also works
    # for pandas-wrapped values that fall back through.
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _safe_serialize(item(), _seen)
        except Exception:
            pass
    # datetime / date / pandas.Timestamp all carry ``.isoformat()``.
    iso = getattr(value, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except Exception:
            pass
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        seen = _seen if _seen is not None else set()
        marker = id(value)
        if marker in seen:
            return "<cycle>"
        seen.add(marker)
        try:
            if isinstance(value, dict):
                return {
                    str(k): _safe_serialize(v, seen)
                    for k, v in value.items()
                }
            return [_safe_serialize(v, seen) for v in value]
        finally:
            seen.discard(marker)
    return str(value)


def _validation_payload(vr) -> dict:
    """Per-validation webhook payload — includes ``details`` so
    receivers (incident pipelines, analytics) get the SHAP top
    contributors / drift z-scores / forecast bands without re-fetching
    from the system table. Every nested value is recursively
    sanitized so non-JSON types (numpy / Decimal / datetime / pandas
    Timestamp) become pure JSON primitives.
    """
    return {
        "name": vr.validation_name,
        "type": vr.validation_type,
        "severity": vr.severity.value,
        "message": vr.message,
        "metric_name": vr.metric_name,
        "actual_value": _safe_serialize(vr.actual_value),
        "expected_value": _safe_serialize(vr.expected_value),
        "dimension_value": vr.dimension_value,
        "details": _safe_serialize(vr.details or {}),
    }


register_notifier("webhook", WebhookNotifier)

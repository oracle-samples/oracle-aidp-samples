"""
429 / CircuitBreaker Detector
==============================
Lightweight regex matcher that classifies a Spark cell's stdout/stderr
as suffering from OCI Object Storage rate-limit pressure.

When the validator's verify step (`process_notebook` in `job_migrate.py`)
sees the patterns this module recognises, it:

1. Records a CircuitBreaker event with the throttle coordinator (which may
   shrink the global concurrent-job budget).
2. Returns a structured retry directive: ``RetryDirective(backoff_sec, ...)``
   so the cell-execution loop can sleep + retry without burning a fix attempt.
3. Suggests the offending bucket (extracted from the URL in the error) so
   operators can correlate with OCI Console metrics.

Pure-Python, no Spark/OCI imports -- safe to unit-test in CI.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional


# -- Patterns -----------------------------------------------------------------

# OCI Java SDK CircuitBreaker open
_RE_CB_OPEN = re.compile(
    r"CircuitBreaker is OPEN.*?rejected",
    re.IGNORECASE | re.DOTALL,
)

# OCI BMC SDK 429 in CircuitBreaker error list or as a direct exception.
# Covers both Java SDK error blobs ("ErrorCode - 429") and Python SDK
# ServiceError dicts ("'status': 429", "code': 'TooManyRequests'") plus
# the camelCase token TooManyRequests from BMC service responses.
_RE_429 = re.compile(
    r"(HttpStatusErrorException.*?429"
    r"|ErrorCode\s*-\s*429"
    r"|Too\s*Many\s*Requests"  # matches both 'Too Many Requests' and 'TooManyRequests'
    r"|TooManyRequests"
    r"|['\"]?status['\"]?\s*[:=]\s*['\"]?429"  # 'status': 429 / status=429 / "status":"429"
    r"|status[_\s]code[\"']?\s*[:=]\s*['\"]?429)",  # status_code=429 / status code: 429
    re.IGNORECASE,
)

# Object Storage URL inside the error blob (for diagnosing which bucket)
_RE_OS_URL = re.compile(
    r"https?://objectstorage\.[a-z0-9-]+\.oraclecloud\.com/n/(?P<ns>[^/]+)/b/(?P<bucket>[^/]+)/o/",
    re.IGNORECASE,
)


@dataclass
class RetryDirective:
    """Structured outcome from inspecting a failed cell's output."""

    detected: bool
    reason: str = ""
    backoff_sec: float = 0.0
    bucket: Optional[str] = None
    namespace: Optional[str] = None
    raw_match: str = ""

    def __bool__(self) -> bool:  # truthy when retry is suggested
        return self.detected


def detect(stdout_or_stderr: str, attempt: int = 1) -> RetryDirective:
    """Inspect a cell's combined stdout+stderr blob for 429 / CB indicators.

    Args:
        stdout_or_stderr: Raw output bytes-decoded to str.
        attempt: 1-based retry attempt counter (for exponential backoff).

    Returns:
        RetryDirective. ``detected`` is True iff one of the rate-limit
        patterns matched. ``backoff_sec`` is computed as
        ``min(60, 5 * 2 ** (attempt-1))`` plus full jitter, biased so that
        attempt 1 sleeps ~5-10s and attempt 5 sleeps ~60s.
    """
    if not stdout_or_stderr:
        return RetryDirective(detected=False)

    cb_match = _RE_CB_OPEN.search(stdout_or_stderr)
    rate_match = _RE_429.search(stdout_or_stderr)
    if not (cb_match or rate_match):
        return RetryDirective(detected=False)

    bucket = namespace = None
    url_match = _RE_OS_URL.search(stdout_or_stderr)
    if url_match:
        bucket = url_match.group("bucket")
        namespace = url_match.group("ns")

    if cb_match:
        reason = "circuit_breaker_open"
        raw = cb_match.group(0)[:200]
    else:
        reason = "object_storage_429"
        raw = (rate_match.group(0) if rate_match else "")[:200]

    # Exponential backoff with light jitter. The CircuitBreaker open-state
    # itself is ~10-30s; we add headroom on top so the retry lands AFTER it
    # closes, not during the same window.
    base = min(60.0, 5.0 * (2 ** max(0, attempt - 1)))
    # Jitter: 0..50% of base
    import random as _r

    backoff_sec = base + _r.uniform(0.0, base * 0.5)

    return RetryDirective(
        detected=True,
        reason=reason,
        backoff_sec=round(backoff_sec, 1),
        bucket=bucket,
        namespace=namespace,
        raw_match=raw,
    )


def is_rate_limit_error(stdout_or_stderr: str) -> bool:
    """Convenience boolean check used by callers that don't need the directive."""
    return detect(stdout_or_stderr).detected


def summarize_events(outputs: List[str]) -> dict:
    """Aggregate detection results across multiple cell outputs.

    Useful for end-of-job reports. Returns a dict with counts and the set of
    distinct buckets observed.
    """
    counts = {"circuit_breaker_open": 0, "object_storage_429": 0}
    buckets = set()
    for blob in outputs:
        d = detect(blob)
        if d.detected:
            counts[d.reason] = counts.get(d.reason, 0) + 1
            if d.bucket:
                buckets.add(d.bucket)
    return {
        "counts": counts,
        "buckets": sorted(buckets),
        "total": sum(counts.values()),
    }


__all__ = [
    "RetryDirective",
    "detect",
    "is_rate_limit_error",
    "summarize_events",
]

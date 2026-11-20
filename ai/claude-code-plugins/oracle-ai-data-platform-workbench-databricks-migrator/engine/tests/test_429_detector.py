"""Tests for scripts/migration_429_detector.py."""
from __future__ import annotations

import os
import sys

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "scripts"))

from migration_429_detector import detect, is_rate_limit_error, summarize_events


# Real-world failure blob from a pilot deployment cluster (sanitised).
SAMPLE_CB_OPEN = """
Caused by: com.oracle.bmc.model.BmcException: (-1, null, false) CircuitBreaker is OPEN and all the requests sent in a window of 30 seconds will be rejected..
URL which CircuitBreaker rejected is - https://objectstorage.ap-mumbai-1.oraclecloud.com/n/exmplnspace1/b/oci-example-feature-bucket/o/example_central_tables%2Fauto_ft%2Fexample_feature_store%2Fapp_v162%2Fpartitioned_data%2Fdecision_date%3D2025-01-01%2Fpart-00179-aaaaaaaa-0000-4000-8000-000000000210.c000.snappy.parquet
Errors which opened the CircuitBreaker:
Error - com.oracle.bmc.circuitbreaker.internal.HttpStatusErrorException; ErrorMessage - Too Many Requests; ErrorCode - 429
Error - com.oracle.bmc.circuitbreaker.internal.HttpStatusErrorException; ErrorMessage - Too Many Requests; ErrorCode - 429
Error - com.oracle.bmc.circuitbreaker.internal.HttpStatusErrorException; ErrorMessage - Too Many Requests; ErrorCode - 429
"""

SAMPLE_429_ONLY = """
oci.exceptions.ServiceError: {'opc-request-id': 'X', 'status': 429, 'code': 'TooManyRequests',
 'message': 'Bucket request rate exceeded'}
"""

SAMPLE_UNRELATED = """
TypeError: 'NoneType' object is not subscriptable
  at /path/to/script.py:42
"""


def test_detect_circuit_breaker_open():
    d = detect(SAMPLE_CB_OPEN, attempt=1)
    assert d.detected is True
    assert d.reason == "circuit_breaker_open"
    assert d.bucket == "oci-example-feature-bucket"
    assert d.namespace == "exmplnspace1"
    assert d.backoff_sec >= 5.0


def test_detect_429_only():
    d = detect(SAMPLE_429_ONLY, attempt=1)
    assert d.detected is True
    assert d.reason == "object_storage_429"
    # No URL in this blob, so bucket should be None
    assert d.bucket is None


def test_detect_unrelated_returns_no_match():
    d = detect(SAMPLE_UNRELATED, attempt=1)
    assert d.detected is False
    assert bool(d) is False


def test_detect_empty_input():
    assert detect("").detected is False
    assert detect(None).detected is False  # type: ignore[arg-type]


def test_detect_backoff_grows_with_attempt():
    """Attempt 5 should sleep at least as long as attempt 1."""
    d1 = detect(SAMPLE_CB_OPEN, attempt=1)
    d5 = detect(SAMPLE_CB_OPEN, attempt=5)
    assert d5.backoff_sec >= d1.backoff_sec
    # Cap is 60 + 50% jitter = 90s. Should never exceed.
    assert d5.backoff_sec <= 90.0


def test_is_rate_limit_error_convenience():
    assert is_rate_limit_error(SAMPLE_CB_OPEN) is True
    assert is_rate_limit_error(SAMPLE_429_ONLY) is True
    assert is_rate_limit_error(SAMPLE_UNRELATED) is False


def test_summarize_events_aggregates():
    summary = summarize_events(
        [SAMPLE_CB_OPEN, SAMPLE_CB_OPEN, SAMPLE_429_ONLY, SAMPLE_UNRELATED]
    )
    assert summary["total"] == 3
    assert summary["counts"]["circuit_breaker_open"] == 2
    assert summary["counts"]["object_storage_429"] == 1
    assert "oci-example-feature-bucket" in summary["buckets"]


def test_directive_truthy_only_when_detected():
    d_yes = detect(SAMPLE_CB_OPEN, attempt=1)
    d_no = detect(SAMPLE_UNRELATED, attempt=1)
    if d_yes:
        ok = True
    else:
        ok = False
    assert ok is True
    if d_no:
        bad = True
    else:
        bad = False
    assert bad is False

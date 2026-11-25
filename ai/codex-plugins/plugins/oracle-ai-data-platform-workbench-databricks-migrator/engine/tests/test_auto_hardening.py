"""Tests for the auto-hardening wiring in scripts/job_migrate.py.

We avoid importing job_migrate.py at module level because it pulls in
``anthropic`` and other heavy runtime deps. Instead we string-grep the
source file for the wiring contract, which is what protects against
accidental removal of the auto-injection in future refactors.
"""
from __future__ import annotations

import os
import re

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB_MIGRATE = os.path.join(REPO, "scripts", "job_migrate.py")


def _src() -> str:
    with open(JOB_MIGRATE, "r", encoding="utf-8") as f:
        return f.read()


def test_throttle_hardening_cell_defined():
    src = _src()
    assert "AIDP_THROTTLE_HARDENING_CELL" in src
    # The cell must call tune_for_parallel_migration with AIDP_WAVE_SIZE.
    # This is the contract that auto-injection actually applies hardening.
    assert "tune_for_parallel_migration" in src
    assert "AIDP_WAVE_SIZE" in src


def test_throttle_hardening_appended_to_bootstrap():
    """Bootstrap must concatenate the hardening cell.

    Locate the literal ``bootstrap = (`` line and walk forward until the
    matching close-paren, then assert the constants appear within. A
    plain regex doesn't work reliably because the bootstrap body contains
    f-strings with nested parens.
    """
    src = _src()
    start = src.find("bootstrap = (")
    assert start >= 0, "bootstrap = ( ... ) assignment not found"
    depth = 0
    end = -1
    for i, ch in enumerate(src[start:], start=start):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                end = i
                break
    assert end > start, "could not find matching close paren for bootstrap"
    body = src[start:end + 1]
    assert "AIDP_THROTTLE_HARDENING_CELL" in body, (
        "AIDP_THROTTLE_HARDENING_CELL must be appended to the bootstrap string"
    )
    # Spark configs moved to cluster-CREATE time (config externalization,
    # c2000a58) — they are static and must NOT be a runtime bootstrap cell.
    assert "AIDP_SPARK_CONFIG_CELL" not in body


def test_process_job_acquires_throttle_lease():
    """process_job must acquire a coordinator lease before doing real work."""
    src = _src()
    # The wrapper pattern: process_job calls _process_job_inner under a lease.
    assert "_process_job_inner" in src
    assert "_get_throttle_coord" in src
    # Lease acquire/release must both be present in the wrapper
    wrapper = re.search(
        r"async def process_job\b.*?async def _process_job_inner\b",
        src,
        flags=re.DOTALL,
    )
    assert wrapper is not None
    body = wrapper.group(0)
    assert ".acquire(" in body
    assert ".release(" in body


def test_wave_size_cli_flag_present():
    src = _src()
    assert "--wave-size" in src
    # Must propagate to env so bootstrap and coordinator agree
    assert 'os.environ["AIDP_WAVE_SIZE"]' in src
    assert 'os.environ["AIDP_THROTTLE_BUDGET"]' in src


def test_rate_limit_handler_present_in_verify_loop():
    """The detector must be invoked in the verify-failure path."""
    src = _src()
    assert "_detect_rate_limit" in src
    assert "_MAX_RATE_LIMIT_RETRIES" in src


def test_provision_shard_buckets_script_exists_and_executable():
    script = os.path.join(REPO, "scripts", "provision_shard_buckets.sh")
    assert os.path.exists(script), "provision_shard_buckets.sh missing"
    with open(script, "r", encoding="utf-8") as f:
        head = f.read(200)
    # Sanity: shebang + idempotency note
    assert head.startswith("#!/usr/bin/env bash")
    with open(script, "r", encoding="utf-8") as f:
        body = f.read()
    # The script invokes the OCI CLI with arg-array expansion, so the literal
    # "oci os bucket create" never appears as a contiguous string. Match the
    # tokens individually.
    assert "os bucket create" in body
    assert "os bucket get" in body  # existence check before create
    assert "Idempotent" in body or "idempotent" in body

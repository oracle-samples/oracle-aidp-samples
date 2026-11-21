"""Unit tests for scripts/acceptance_contract.py.

No live AIDP cluster needed — we mock the SQL execution function. Run:
    python3 tests/test_acceptance_contract.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from acceptance_contract import (
    AcceptanceContract,
    ContractParseError,
    ContractResult,
    DEFAULTS,
    format_result_for_report,
    load_contract_from_manifest_entry,
    run_contract,
)


# ─── PARSE TESTS ───────────────────────────────────────────────────────

def test_parse_minimal_valid():
    """Only pending_count_sql required; others get defaults."""
    c = AcceptanceContract.from_dict({"pending_count_sql": "SELECT 0"})
    assert c.pending_count_sql == "SELECT 0"
    assert c.zero_window == DEFAULTS["zero_window"]
    assert c.sleep_between_s == DEFAULTS["sleep_between_s"]
    assert c.max_attempts == DEFAULTS["max_attempts"]


def test_parse_full():
    c = AcceptanceContract.from_dict({
        "pending_count_sql": "SELECT COUNT(*) FROM t",
        "zero_window": 3,
        "sleep_between_s": 15,
        "max_attempts": 20,
    })
    assert c.zero_window == 3
    assert c.sleep_between_s == 15
    assert c.max_attempts == 20


def test_parse_missing_sql_raises():
    try:
        AcceptanceContract.from_dict({"zero_window": 2})
        assert False, "should have raised"
    except ContractParseError as e:
        assert "pending_count_sql" in str(e)


def test_parse_empty_sql_raises():
    try:
        AcceptanceContract.from_dict({"pending_count_sql": "   "})
        assert False, "should have raised"
    except ContractParseError as e:
        assert "pending_count_sql" in str(e)


def test_parse_negative_zero_window_raises():
    try:
        AcceptanceContract.from_dict({"pending_count_sql": "SELECT 0", "zero_window": 0})
        assert False, "should have raised"
    except ContractParseError as e:
        assert "zero_window" in str(e)


def test_parse_non_int_sleep_raises():
    try:
        AcceptanceContract.from_dict({"pending_count_sql": "SELECT 0", "sleep_between_s": "thirty"})
        assert False, "should have raised"
    except ContractParseError as e:
        assert "sleep_between_s" in str(e)


def test_parse_int_compatible_string():
    """YAML can return numeric strings; parser should accept them."""
    c = AcceptanceContract.from_dict({
        "pending_count_sql": "SELECT 0",
        "zero_window": "2",
        "max_attempts": "5",
    })
    assert c.zero_window == 2
    assert c.max_attempts == 5


def test_parse_not_dict_raises():
    try:
        AcceptanceContract.from_dict("not a dict")
        assert False, "should have raised"
    except ContractParseError as e:
        assert "dict" in str(e)


# ─── RUN TESTS (mocked execute_sql_fn) ─────────────────────────────────

def _make_runner(counts: list[int], raises_on=None):
    """Build an async execute_sql_fn that returns counts in order; optionally
    raises on the Nth attempt (1-indexed) for error-path testing."""
    state = {"i": 0}

    async def runner(sql: str):
        state["i"] += 1
        if raises_on is not None and state["i"] == raises_on:
            raise RuntimeError("simulated probe failure")
        if state["i"] - 1 >= len(counts):
            raise IndexError(f"runner asked for attempt {state['i']} but only {len(counts)} counts queued")
        return counts[state["i"] - 1]

    return runner


async def _noop_sleep(_s):
    return None


def test_run_passes_immediately_when_zero_first_attempt():
    """zero_window=1 + count=0 on first attempt → PASS in 1 attempt."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=1, sleep_between_s=0, max_attempts=5)
    runner = _make_runner([0])
    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert res.passed
    assert res.attempts == 1
    assert res.readings == [0]
    assert "1 attempts" in res.diagnostic


def test_run_zero_window_2_requires_two_consecutive_zeros():
    """zero_window=2 with sequence [5, 0, 3, 0, 0] should pass on attempt 5."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=2, sleep_between_s=0, max_attempts=10)
    runner = _make_runner([5, 0, 3, 0, 0])
    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert res.passed
    assert res.attempts == 5
    assert res.readings == [5, 0, 3, 0, 0]


def test_run_violated_when_max_attempts_exhausted():
    """All non-zero readings → VIOLATED with full last-5 trace."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=2, sleep_between_s=0, max_attempts=5)
    runner = _make_runner([8, 8, 8, 8, 8])
    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert not res.passed
    assert res.attempts == 5
    assert res.readings == [8, 8, 8, 8, 8]
    assert res.status == "VIOLATED"
    assert "stable at 8" in res.diagnostic


def test_run_violated_decreasing_diagnostic():
    """Decreasing-but-not-zero pattern emits the 'not yet zero' diagnostic."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=1, sleep_between_s=0, max_attempts=5)
    runner = _make_runner([20, 15, 10, 7, 4])
    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert not res.passed
    assert res.attempts == 5
    assert "decreasing but not yet zero" in res.diagnostic


def test_run_zero_breaks_streak_then_recovers():
    """zero_window=2 + sequence [0, 3, 0, 0]: the 3 resets the counter."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=2, sleep_between_s=0, max_attempts=10)
    runner = _make_runner([0, 3, 0, 0])
    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert res.passed
    assert res.attempts == 4
    assert res.readings == [0, 3, 0, 0]


def test_run_probe_sql_error_returns_error_result():
    """A raise from execute_sql_fn becomes a non-passing ContractResult."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=1, sleep_between_s=0, max_attempts=3)
    runner = _make_runner([5], raises_on=2)
    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert not res.passed
    assert res.error is not None
    assert "simulated probe failure" in res.error
    assert res.status == "ERROR"


def test_run_non_int_result_returns_error():
    """If the SQL returns a non-int value, it's an error."""
    c = AcceptanceContract(pending_count_sql="X", zero_window=1, sleep_between_s=0, max_attempts=3)

    async def runner(sql):
        return "not a number"

    res = asyncio.run(run_contract(c, runner, sleep_fn=_noop_sleep))
    assert not res.passed
    assert res.error is not None
    assert "non-int" in res.error


# ─── MANIFEST INTEGRATION ──────────────────────────────────────────────

def test_load_from_manifest_no_contract_returns_none():
    """Manifest entries without a contract block return None (no-op path)."""
    entry = {"task_key": "01_foo", "notebook_path": "/foo.ipynb"}
    assert load_contract_from_manifest_entry(entry) is None


def test_load_from_manifest_with_contract():
    entry = {
        "task_key": "01_foo",
        "acceptance_contract": {"pending_count_sql": "SELECT 0", "zero_window": 2},
    }
    c = load_contract_from_manifest_entry(entry)
    assert c is not None
    assert c.zero_window == 2


def test_load_from_manifest_malformed_raises():
    entry = {"acceptance_contract": {"zero_window": 1}}  # missing pending_count_sql
    try:
        load_contract_from_manifest_entry(entry)
        assert False
    except ContractParseError:
        pass


# ─── REPORT FORMATTING ─────────────────────────────────────────────────

def test_format_passed_report():
    res = ContractResult(
        passed=True, attempts=4, readings=[5, 3, 1, 0],
        elapsed_s=120.0, reason="converged in 4 attempts",
        contract=AcceptanceContract(pending_count_sql="SELECT 0"),
    )
    out = format_result_for_report(res)
    assert "PASSED" in out
    assert "drained" in out
    assert "[5, 3, 1, 0]" in out  # last-5 readings


def test_format_violated_report():
    res = ContractResult(
        passed=False, attempts=5, readings=[8, 8, 8, 8, 8],
        elapsed_s=60.0, reason="exhausted",
        contract=AcceptanceContract(pending_count_sql="SELECT COUNT(*) FROM x"),
    )
    out = format_result_for_report(res)
    assert "VIOLATED" in out
    assert "stable" in out


def test_format_empty_contract():
    """A result with no contract attached still formats cleanly."""
    res = ContractResult(passed=False, attempts=0, contract=None)
    out = format_result_for_report(res)
    assert "no acceptance contract" in out


# ─── ENTRY POINT ───────────────────────────────────────────────────────

if __name__ == "__main__":
    test_funcs = [v for k, v in list(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    passed = 0
    for fn in test_funcs:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {fn.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERR   {fn.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed + failed} tests passed")
    sys.exit(0 if failed == 0 else 1)

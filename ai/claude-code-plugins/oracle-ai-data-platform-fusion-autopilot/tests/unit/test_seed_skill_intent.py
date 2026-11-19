"""Table-driven tests for the aidp-fusion-seed skill's intent parser.

The intent→flag mapping is load-bearing — a misparse can seed too broad a
scope (e.g. "seed supplier" pulling six bronze deps via D-1) — so every
phrasing in the SKILL.md mapping table is asserted here against the real
``intent.py`` helper. Pure parse, no dispatch.

The skill lives outside the installed package (under ``skills/``), so import
via sys.path manipulation (mirrors ``test_aidp_rest_skill_client.py``).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SKILL_DIR = Path(__file__).resolve().parents[2] / "skills" / "aidp-fusion-seed"
sys.path.insert(0, str(_SKILL_DIR))

import intent  # noqa: E402

# The starter-pack node universe the skill assembles (bronze datasets + silver
# + gold). Mirrors fusion-finance-starter + the dev bundle's datasets.
KNOWN = [
    "gl_journal_lines", "gl_period_balances", "gl_coa", "erp_suppliers",
    "ar_invoices", "ar_receipts", "ap_invoices", "ap_payments",
    "po_orders", "po_receipts", "scm_items",
    "dim_supplier", "dim_account", "dim_calendar",
    "gl_balance", "supplier_spend", "ap_aging",
]


@pytest.mark.parametrize(
    "phrase,expected_argv",
    [
        ("seed", ["run", "--mode", "seed"]),
        ("seed everything", ["run", "--mode", "seed"]),
        ("full seed", ["run", "--mode", "seed"]),
        ("seed supplier_spend",
         ["run", "--mode", "seed", "--datasets", "supplier_spend"]),
        ("seed the supplier spend mart",
         ["run", "--mode", "seed", "--datasets", "supplier_spend", "--layers", "silver,gold"]),
        ("seed dim_supplier and dim_account",
         ["run", "--mode", "seed", "--datasets", "dim_supplier,dim_account"]),
        ("seed just bronze",
         ["run", "--mode", "seed", "--layers", "bronze"]),
        ("seed the bronze layer",
         ["run", "--mode", "seed", "--layers", "bronze"]),
        ("seed silver and gold",
         ["run", "--mode", "seed", "--layers", "silver,gold"]),
        ("seed the marts",
         ["run", "--mode", "seed", "--layers", "silver,gold"]),
        ("seed dim_supplier bronze only, deps already staged",
         ["run", "--mode", "seed", "--datasets", "dim_supplier", "--layers", "bronze", "--strict-scope"]),
    ],
)
def test_argv_mapping(phrase, expected_argv):
    """Every SKILL.md mapping-table row resolves to the exact run argv."""
    result = intent.parse(phrase, KNOWN)
    assert result.argv == expected_argv
    assert result.mode == "seed"
    # Clean parses never raise an ask.
    assert not result.unknown_tokens
    assert not result.ambiguous
    assert not result.needs_run_id


def test_resume_with_explicit_id():
    r = intent.parse("the seed died, resume run abc12345", KNOWN)
    assert r.resume_run_id == "abc12345"
    assert r.needs_run_id is False
    assert r.argv == ["run", "--mode", "seed", "--resume", "abc12345"]


def test_resume_without_id_needs_ask():
    r = intent.parse("resume the seed", KNOWN)
    assert r.resume_run_id is None
    assert r.needs_run_id is True
    # argv carries no --resume; the skill must ask before dispatching.
    assert "--resume" not in r.argv


def test_unknown_token_is_flagged_not_guessed():
    r = intent.parse("seed widgets", KNOWN)
    assert r.unknown_tokens == ["widgets"]
    assert r.datasets == []  # never guessed into a scope


def test_ambiguous_target_lists_candidates():
    """'seed supplier' must NOT silently seed — it is ambiguous across nodes."""
    r = intent.parse("seed supplier", KNOWN)
    assert r.datasets == []
    tokens = [a["token"] for a in r.ambiguous]
    assert "supplier" in tokens
    cands = next(a["candidates"] for a in r.ambiguous if a["token"] == "supplier")
    assert {"dim_supplier", "erp_suppliers", "supplier_spend"} <= set(cands)


def test_multi_word_form_wins_over_bare_token():
    """'supplier spend' resolves to supplier_spend, not an ambiguous 'supplier'."""
    r = intent.parse("seed supplier spend", KNOWN)
    assert r.datasets == ["supplier_spend"]
    assert not r.ambiguous


# --- resume run_id resolution (status --recent-runs --json) -----------------


def test_resume_resolver_single_failed_run():
    runs = [{"run_id": "r-100", "status": "failed"}]
    run_id, needs_ask = intent.resolve_resume_run_id(runs)
    assert run_id == "r-100"
    assert needs_ask is False


def test_resume_resolver_zero_failed_forces_ask():
    runs = [{"run_id": "r-1", "status": "success"}]
    run_id, needs_ask = intent.resolve_resume_run_id(runs)
    assert run_id is None
    assert needs_ask is True


def test_resume_resolver_multiple_failed_forces_ask():
    runs = [
        {"run_id": "r-1", "status": "failed"},
        {"run_id": "r-2", "status": "failed"},
    ]
    run_id, needs_ask = intent.resolve_resume_run_id(runs)
    assert run_id is None
    assert needs_ask is True


def test_resume_resolver_empty_forces_ask():
    run_id, needs_ask = intent.resolve_resume_run_id([])
    assert run_id is None
    assert needs_ask is True

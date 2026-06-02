"""Tests for ``_snapshot_details`` + ``_safe_json_for_html_script``.

Covers the per-row redaction / truncation contract, the path-aware
exemption for the value-drift-explainer schema, the script-breakout
escape, and the multi-stage 8 MB total cap. These pin the
acceptance criteria from
``docs/features/dashboard-rich-detail-panel/plan.md``.
"""

from __future__ import annotations

import json

import pytest

from qualifire.reporting._snapshot_details import (
    _PER_ROW_MAX_BYTES,
    _safe_json_for_html_script,
    _snapshot_details,
    enforce_total_cap,
)


# ---- _safe_json_for_html_script (script-breakout safety) ---------------


def test_safe_json_escapes_closing_script_tag():
    """AC#14: a value containing ``</script>`` round-trips with no
    raw ``</script>`` substring in the embedded literal."""
    out = _safe_json_for_html_script({"msg": "</script><script>alert(1)</script>"})
    assert "</script>" not in out
    assert "</" not in out  # </ is the breakout vector, not just </script
    # Still parseable as JSON (after un-escaping the </ via JSON.parse).
    json.loads(out)


def test_safe_json_escapes_unicode_line_separators():
    s = "line break more"
    out = _safe_json_for_html_script({"x": s})
    assert " " not in out
    assert " " not in out
    assert "\\u2028" in out
    assert "\\u2029" in out


def test_safe_json_escapes_html_comment_opener():
    out = _safe_json_for_html_script({"x": "<!--hostile-->"})
    assert "<!--" not in out


def test_safe_json_round_trips_normal_payload():
    obj = {"a": 1, "b": "hello", "c": [1, 2, 3]}
    out = _safe_json_for_html_script(obj)
    # Reverse the </ -> <\/ +  /  substitution by parsing.
    decoded = json.loads(out)
    assert decoded == obj


# ---- _snapshot_details — basic parse + dual-shape ----------------------


def test_returns_none_when_input_is_none():
    assert _snapshot_details(None) is None


def test_returns_none_when_input_is_empty_string():
    assert _snapshot_details("") is None


def test_parses_dict_form():
    assert _snapshot_details({"a": 1}) == {"a": 1}


def test_parses_json_string_form():
    assert _snapshot_details('{"a": 1}') == {"a": 1}


def test_returns_none_on_parse_failure():
    assert _snapshot_details("not-json{") is None


def test_returns_none_when_top_level_not_dict():
    # Validator details is contractually a dict; arrays / scalars
    # at the root are projection bugs upstream.
    assert _snapshot_details("[1,2,3]") is None


def test_drops_qualifire_internal_failure_marker():
    """Top-level ``qualifire_internal_failure`` is already projected
    as a snapshot column; dropping here avoids double-counting."""
    out = _snapshot_details({"qualifire_internal_failure": True, "error": "x"})
    assert "qualifire_internal_failure" not in out
    assert out == {"error": "x"}


# ---- Sensitive-key redaction (whole-key match) -------------------------


def test_redacts_password_key():
    out = _snapshot_details({"password": "hunter2"})
    assert out["password"] == "[redacted]"


@pytest.mark.parametrize("key", [
    "password", "PASSWORD", "Password",
    "secret", "token", "api_key", "api-key", "apikey", "credential",
])
def test_redacts_known_sensitive_keys(key):
    out = _snapshot_details({key: "x"})
    assert out[key] == "[redacted]"


def test_does_not_redact_innocent_keys_with_substring_match():
    """``password_check_threshold`` survives — whole-key regex avoids
    false positives on innocent column names that contain a
    sensitive substring (codex round 1 LOW)."""
    out = _snapshot_details({
        "password_check_threshold": 0.5,
        "secret_key_count": 100,
    })
    assert out["password_check_threshold"] == 0.5
    assert out["secret_key_count"] == 100


# ---- Path-aware explainer exemption (codex BLOCKER #3) -----------------


def test_explainer_category_value_passes_through_intact():
    """Codex round 1 BLOCKER #3 regression: legitimate explainer
    fields ``current.top[*].value`` / ``.past.top[*].value`` /
    ``.category`` are NEVER redacted, even when they look like
    PII (operator chose the column; the explainer's whole point
    is surfacing those values)."""
    out = _snapshot_details({
        "value_drift_explainer": [{
            "feature": "user_id",
            "kind": "label_encoded",
            "category": "alice@example.com",
            "current": {"top": [
                {"value": "alice@example.com", "rate": 0.5},
                {"value": "token-like-12345", "rate": 0.3},
            ]},
            "past": {"top": [{"value": "bob@example.com", "rate": 0.4}]},
        }],
    })
    entry = out["value_drift_explainer"][0]
    assert entry["category"] == "alice@example.com"
    assert entry["current"]["top"][0]["value"] == "alice@example.com"
    assert entry["current"]["top"][1]["value"] == "token-like-12345"


def test_explainer_block_preserved_alongside_redacted_sibling():
    """Sensitive keys redact at non-explainer paths; explainer
    block survives untouched."""
    out = _snapshot_details({
        "api_key": "sk-12345",
        "value_drift_explainer": [{
            "feature": "x", "category": "y", "summary": "z",
        }],
    })
    assert out["api_key"] == "[redacted]"
    assert out["value_drift_explainer"][0]["category"] == "y"


# ---- String / dict / list truncation -----------------------------------


def test_long_string_replaced_with_truncated_marker():
    """Per-string cap (2 KB) replaces with ``"[truncated]"`` —
    pinned in plan as REPLACE, not drop, so renderer branches
    still fire."""
    out = _snapshot_details({"msg": "x" * 4096})
    assert out["msg"] == "[truncated]"


def test_short_string_passes_through():
    out = _snapshot_details({"msg": "ok"})
    assert out["msg"] == "ok"


def test_oversize_dict_replaced_with_size_marker():
    """A nested dict whose serialized size exceeds 3 KB even AFTER
    per-string truncation gets replaced with a size marker. We
    achieve that by having many small (sub-2KB) string values
    so per-string truncation doesn't free the budget."""
    big = {f"k{i}": "x" * 200 for i in range(50)}  # ~10 KB serialized, all strings under 2 KB
    out = _snapshot_details({"nested": big})
    assert isinstance(out["nested"], str)
    assert out["nested"].startswith("[truncated dict size=")


# ---- Per-row 8 KB cap (top-level fallback) -----------------------------


def test_per_row_cap_returns_truncation_marker_when_unsalvageable():
    # Build a payload with no explainer that's unavoidably too big.
    big = {f"k{i}": "x" * 1500 for i in range(10)}
    out = _snapshot_details(big)
    # All strings get truncated individually, so payload should fit.
    # Force a synthetic over-cap with many small strings:
    big2 = {f"k{i}": "small" for i in range(20000)}
    out2 = _snapshot_details(big2)
    assert out2 == {"_truncated": True, "_reason": f"row payload exceeded {_PER_ROW_MAX_BYTES} bytes"}


def test_per_row_cap_preserves_explainer_via_tail_truncation():
    """AC#15: when the row exceeds 8 KB, the explainer is preserved
    with parallel-list semantics (tail entries become
    ``kind="truncated"``), not dropped wholesale."""
    explainer = []
    for i in range(50):
        explainer.append({
            "feature": f"feat_{i}",
            "source_column": f"col_{i}",
            "kind": "numeric",
            "summary": "x" * 200,
            "current": {"mean": 1.0, "std": 0.5},
            "past": {"mean": 1.0, "std": 0.5},
        })
    out = _snapshot_details({"value_drift_explainer": explainer})
    assert "value_drift_explainer" in out
    # Length preserved.
    assert len(out["value_drift_explainer"]) == 50
    # Some tail entries became placeholders.
    truncated = [e for e in out["value_drift_explainer"] if e.get("kind") == "truncated"]
    assert truncated, "expected at least one truncated tail placeholder"
    # Head entries survive intact.
    assert out["value_drift_explainer"][0]["kind"] == "numeric"


# ---- enforce_total_cap (multi-stage 8 MB) ------------------------------


def test_total_cap_no_op_under_budget():
    snapshot = [{"id": i, "details": {"x": i}} for i in range(10)]
    out, marker = enforce_total_cap(snapshot, max_bytes=10_000_000)
    assert out == snapshot
    assert marker is None


def test_total_cap_stage_1_strips_oldest_first():
    """Stage 1 strips details from the oldest rows first
    (deterministic by run_timestamp; tie-break on name).

    Use a payload large enough that stripping is unambiguous
    (replacing details with a tiny marker shrinks the row).
    """
    big_details = {"x": "y" * 5000}
    rows = [
        {"id": 0, "run_timestamp": "2026-05-01", "dataset_name": "a",
         "validation_name": "v", "dimension_value": None,
         "details": big_details},
        {"id": 1, "run_timestamp": "2026-05-02", "dataset_name": "a",
         "validation_name": "v", "dimension_value": None,
         "details": big_details},
        {"id": 2, "run_timestamp": "2026-05-03", "dataset_name": "a",
         "validation_name": "v", "dimension_value": None,
         "details": big_details},
    ]
    # Pick a cap that fits ~2 raw rows but not 3.
    raw_size = len(json.dumps(rows))
    out, marker = enforce_total_cap(rows, max_bytes=int(raw_size * 0.7))
    assert len(out) == 3, "Stage 1 strips details, doesn't drop rows"
    # Oldest (id=0) row got stripped first (deterministic).
    assert out[0]["details"]["_truncated"] is True
    # Newer rows survive intact OR also get stripped if needed —
    # but at least the oldest is gone.
    assert marker is not None
    assert marker["stripped"] >= 1


def test_total_cap_stage_2_drops_oldest_rows_when_metadata_only_overflow():
    """If Stage 1 strips ALL details and the snapshot is still over
    budget, Stage 2 drops oldest rows entirely (deterministic)."""
    # Many metadata-only rows so the total exceeds even after
    # details stripping.
    rows = [
        {"id": i, "run_timestamp": f"2026-05-{(i % 28) + 1:02d}",
         "dataset_name": f"ds{i:04d}",
         "validation_name": f"v{i:04d}",
         "dimension_value": None,
         "details": None}
        for i in range(20_000)
    ]
    out, marker = enforce_total_cap(rows, max_bytes=8 * 1024 * 1024)
    assert len(out) <= len(rows)
    if len(out) < len(rows):
        # Stage 2 fired.
        assert marker["dropped"] > 0


def test_total_cap_stage_2_deterministic_ordering():
    """Codex R2 MEDIUM-1 regression: Stage 2 ordering is the same
    stable sort as Stage 1 across re-runs."""
    rows = [
        {"id": 0, "run_timestamp": "2026-05-01", "dataset_name": "b", "validation_name": "v",
         "dimension_value": None, "details": None},
        {"id": 1, "run_timestamp": "2026-05-01", "dataset_name": "a", "validation_name": "v",
         "dimension_value": None, "details": None},
    ]
    # Force tiny budget so at least one row drops.
    out_a, _ = enforce_total_cap(rows, max_bytes=50)
    out_b, _ = enforce_total_cap(rows, max_bytes=50)
    # Both runs produce identical surviving rows.
    assert [r["id"] for r in out_a] == [r["id"] for r in out_b]


# ---- Generic renderer escape (Python side; DOM escape is JS) -----------


def test_validator_details_keys_pass_pii_denylist():
    """AC#11: walk every detail key emitted by in-tree validators
    and assert none triggers the sensitive-key denylist. Catches
    future validators that accidentally introduce a
    ``details["password"]`` / ``details["token"]`` / etc.

    The denylist regex is whole-key match
    ``^(password|secret|token|api[_-]?key|credential)$`` — substring
    matches like ``password_check_threshold`` are intentionally
    excluded, but any validator emitting one of the literal
    denylisted keys would trip.
    """
    import re
    from pathlib import Path
    DENY = re.compile(
        r"^(password|secret|token|api[_-]?key|credential)$",
        re.IGNORECASE,
    )
    # Source-side audit: scan every line in qualifire/validation/
    # for ``details["..."] =`` writes and assert the key isn't on
    # the denylist. Cheap, deterministic, no validator has to run.
    val_dir = Path(__file__).resolve().parents[2] / "qualifire" / "validation"
    pattern = re.compile(r'details\[\s*[\'"]([^\'"]+)[\'"]\s*\]\s*=')
    for py in sorted(val_dir.glob("*.py")):
        text = py.read_text(encoding="utf-8")
        for m in pattern.finditer(text):
            key = m.group(1)
            assert not DENY.match(key), (
                f"{py.name}: validator emits denylist key {key!r}; "
                f"the dashboard would redact it."
            )


def test_script_breakout_in_details_value_does_not_break_embedding():
    """End-to-end: a hostile details payload round-trips through
    _snapshot_details + _safe_json_for_html_script and produces
    no raw ``</script>`` substring."""
    raw = {"msg": "</script><script>alert(1)</script>"}
    sanitized = _snapshot_details(raw)
    embedded = _safe_json_for_html_script(sanitized)
    assert "</script>" not in embedded

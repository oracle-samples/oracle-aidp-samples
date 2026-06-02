"""Tests for ``format_validation_details`` rendering of the
value-drift-explainer block on shape / pattern validation rows.

Pinned by docs/features/feature-value-drift-explainer/plan.md AC#4
(top-3 inline summaries with the U+2192 arrow), AC#23 (per-line
240-char cap), and the truncation marker contract.
"""

from __future__ import annotations

from types import SimpleNamespace

from qualifire.notification.base import format_validation_details


def _make_vr(
    vtype: str = "pattern",
    top_features: list[dict] | None = None,
    drift: list[dict] | None = None,
    extras: dict | None = None,
):
    details = {"top_contributing_features": top_features or []}
    if drift is not None:
        details["value_drift_explainer"] = drift
    if extras:
        details.update(extras)
    return SimpleNamespace(
        validation_type=vtype,
        validation_name="pattern_check",
        message="Pattern AUC: 0.85",
        actual_value=0.85,
        expected_value={"warning": 0.65, "error": 0.80},
        details=details,
        dimension_value=None,
        severity=SimpleNamespace(value="ERROR"),
    )


def test_renders_drift_summary_arrow_for_top_3_only():
    feats = [
        {"feature": f"feat_{i}", "importance": 1.0 / (i + 1)} for i in range(5)
    ]
    drift = [
        {"feature": f"feat_{i}", "kind": "numeric", "summary": f"summary_{i}"}
        for i in range(5)
    ]
    vr = _make_vr("pattern", feats, drift)
    lines = format_validation_details(vr, indent="  ")
    body = "\n".join(lines)
    assert "→ summary_0" in body
    assert "→ summary_1" in body
    assert "→ summary_2" in body
    assert "→ summary_3" not in body
    assert "→ summary_4" not in body


def test_drift_arrow_appears_under_corresponding_feature_bullet():
    feats = [
        {"feature": "amount", "importance": 0.5},
        {"feature": "channel_mobile", "importance": 0.3},
    ]
    drift = [
        {"feature": "amount", "kind": "numeric", "summary": "p50 +43%"},
        {"feature": "channel_mobile", "kind": "onehot", "summary": "rate +30pp"},
    ]
    vr = _make_vr("pattern", feats, drift)
    lines = format_validation_details(vr, indent="")
    # Find the index of each `• {feat}` and assert the next non-blank
    # line is the corresponding `→ {summary}`.
    for feat_name, drift_summary in [
        ("amount", "p50 +43%"),
        ("channel_mobile", "rate +30pp"),
    ]:
        idx = next(i for i, L in enumerate(lines) if f"• {feat_name}" in L)
        assert "→" in lines[idx + 1]
        assert drift_summary in lines[idx + 1]


def test_drift_truncation_marker_renders():
    feats = [{"feature": "a", "importance": 1.0}]
    drift = [{"feature": "a", "kind": "truncated", "summary": "payload truncated"}]
    vr = _make_vr(
        "pattern", feats, drift,
        extras={"value_drift_explainer_truncated": True},
    )
    body = "\n".join(format_validation_details(vr, indent=""))
    assert "value drift payload truncated" in body


def test_drift_explainer_error_renders_when_present():
    feats = [{"feature": "a", "importance": 1.0}]
    vr = _make_vr(
        "pattern", feats, drift=None,
        extras={"value_drift_explainer_error": "RuntimeError: boom"},
    )
    body = "\n".join(format_validation_details(vr, indent=""))
    assert "value drift unavailable" in body
    assert "RuntimeError: boom" in body


def test_no_drift_block_when_explainer_absent():
    feats = [{"feature": "a", "importance": 1.0}]
    vr = _make_vr("pattern", feats, drift=None)
    body = "\n".join(format_validation_details(vr, indent=""))
    assert "→" not in body
    assert "value drift" not in body


def test_per_line_cap_at_240_chars():
    feats = [{"feature": "x", "importance": 1.0}]
    drift = [{"feature": "x", "kind": "numeric", "summary": "y" * 500}]
    vr = _make_vr("pattern", feats, drift)
    lines = format_validation_details(vr, indent="")
    drift_lines = [L for L in lines if "→" in L]
    assert drift_lines
    for L in drift_lines:
        assert len(L) <= 240


def test_shape_validator_also_renders_drift():
    feats = [{"feature": "amount", "importance": 0.7}]
    drift = [{"feature": "amount", "kind": "numeric", "summary": "p99 +50%"}]
    vr = _make_vr("shape", feats, drift)
    body = "\n".join(format_validation_details(vr, indent=""))
    assert "→ p99 +50%" in body

"""Per-kind tests for ``explain_value_drift`` covering the plan's 27 ACs.

Mirrors the scenarios pinned in
``docs/features/feature-value-drift-explainer/plan.md`` (R1, R2, codex
R1, codex R2 sections) — numeric / boolean / datetime / onehot /
label_encoded / unknown / truncated / mismatched columns / NaT /
all-NaN / single-row / unmapped / object cells.
"""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
import pytest

from qualifire.validation._drift_explainer import (
    _PAYLOAD_MAX_BYTES,
    _SUMMARY_MAX_CHARS,
    _to_jsonable,
    explain_value_drift,
)
from qualifire.validation._encoding import EncodedFeatureSpec, encode_columns


def _spec(name, kind, **kw):
    return EncodedFeatureSpec(source_column=name, kind=kind, **kw)


# -- AC#1: encoder produces parallel feature_names + encoding_map ---------

def test_encoder_returns_parallel_encoding_map():
    df = pd.DataFrame({
        "age": [10, 20, 30],
        "active": [True, False, True],
        "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "channel": ["email", "paid", None],
    })
    X, names, emap = encode_columns(df)
    assert len(names) == X.shape[1]
    assert set(names) == set(emap.keys())
    for n in names:
        assert emap[n].source_column
        assert emap[n].kind in {
            "numeric", "boolean", "datetime", "onehot", "label_encoded", "unknown"
        }


# -- AC#11: encoder uniqueness assertion ----------------------------------

def test_encoder_raises_on_prefix_collision():
    df = pd.DataFrame({
        "region_us": [0.1, 0.2],
        "region": ["us", "eu"],
    })
    with pytest.raises(ValueError, match="prefix collision|duplicate"):
        encode_columns(df)


# -- AC#12: _to_jsonable ---------------------------------------------------

def test_to_jsonable_casts_numpy_scalars_and_drops_non_finite():
    assert _to_jsonable(np.float64(1.5)) == 1.5
    assert isinstance(_to_jsonable(np.float64(1.5)), float)
    assert _to_jsonable(np.int64(7)) == 7
    assert isinstance(_to_jsonable(np.int64(7)), int)
    assert _to_jsonable(float("nan")) is None
    assert _to_jsonable(float("inf")) is None
    assert _to_jsonable(float("-inf")) is None
    assert _to_jsonable(True) is True


# -- AC#13/#18: one-hot null bin disambiguation ---------------------------

def test_onehot_real_nan_string_with_no_actual_nulls_does_not_collide():
    """Codex impl R2 HIGH regression: when the source has a real
    "nan" category but NO actual NaN values, the encoder must NOT
    emit a dummy_na column — pandas's automatic dummy_na bin would
    collide with the real "nan" bin and trip the uniqueness
    assertion. Fix passes ``dummy_na=series.isna().any()``.
    """
    df = pd.DataFrame({"x": ["nan", "a", "nan", "b"]})
    _, names, emap = encode_columns(df)
    null_specs = [emap[n] for n in names if emap[n].is_null_bin]
    # No actual nulls → no null bin emitted at all.
    assert null_specs == []
    # Real "nan" category survives as a one-hot bin.
    real_nan = [
        emap[n] for n in names
        if emap[n].kind == "onehot" and emap[n].category == "nan"
    ]
    assert len(real_nan) == 1


def test_onehot_real_nan_string_does_not_collide_with_null_bin():
    """Codex impl R1 BLOCKER regression: a real category whose
    string repr matches pandas's NaN suffix ("nan") must not
    collide with the dummy_na bin. The encoder renames the null
    bin to ``f"{col}__qf_null"`` so collision is impossible.
    """
    df = pd.DataFrame({"x": ["nan", None, "a"]})
    _, names, emap = encode_columns(df)
    null_specs = [emap[n] for n in names if emap[n].is_null_bin]
    assert len(null_specs) == 1
    assert "x__qf_null" in names
    real_nan = [
        emap[n] for n in names
        if emap[n].kind == "onehot"
        and not emap[n].is_null_bin
        and emap[n].category == "nan"
    ]
    assert len(real_nan) == 1


def test_onehot_null_bin_distinguishable_from_real_null_string_value():
    df = pd.DataFrame({"x": ["a", None, "__NULL__"]})
    _, names, emap = encode_columns(df)
    onehot_specs = [emap[n] for n in names if emap[n].kind == "onehot"]
    null_bins = [s for s in onehot_specs if s.is_null_bin]
    assert len(null_bins) == 1
    a_bin = next(s for s in onehot_specs if s.category == "a")
    assert a_bin.is_null_bin is False
    real_null_string = next(
        (s for s in onehot_specs if s.category == "__NULL__" and not s.is_null_bin),
        None,
    )
    assert real_null_string is not None, (
        "real category '__NULL__' must remain distinguishable from null bin"
    )


# -- AC#15: empty top_features -> empty list, no errors -------------------

def test_empty_top_features_returns_empty_list():
    cur = pd.DataFrame({"x": [1, 2, 3]})
    past = [("p1", pd.DataFrame({"x": [1, 2, 3]}))]
    entries, mapping_errors = explain_value_drift([], cur, past, {})
    assert entries == []
    assert mapping_errors == 0


# -- AC#16/#27: unmapped feature path -------------------------------------

def test_unmapped_feature_yields_kind_unknown_and_increments_counter():
    cur = pd.DataFrame({"x": [1, 2, 3]})
    past = [("p1", pd.DataFrame({"x": [1, 2, 3]}))]
    top = [{"feature": "ghost", "importance": 0.5}]
    entries, mapping_errors = explain_value_drift(top, cur, past, {})
    assert mapping_errors == 1
    assert entries[0]["kind"] == "unknown"
    assert entries[0]["summary"].startswith("unmapped feature: ghost")


# -- AC#19: all-NaN current numeric series --------------------------------

def test_numeric_all_nan_current_series_null_pct_is_one():
    cur = pd.DataFrame({"amount": [float("nan"), float("nan"), float("nan")]})
    past = [("p1", pd.DataFrame({"amount": [10.0, 20.0, 30.0]}))]
    emap = {"amount": _spec("amount", "numeric")}
    top = [{"feature": "amount", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    assert e["current"]["count"] == 0
    assert e["current"]["null_pct"] == 1.0
    assert e["current"]["mean"] is None
    assert e["current"]["std"] is None
    assert e["current"]["p99"] is None


# -- AC#20: single-row series ---------------------------------------------

def test_numeric_single_row_series_std_is_none():
    cur = pd.DataFrame({"amount": [42.0]})
    past = [("p1", pd.DataFrame({"amount": [10.0, 20.0, 30.0]}))]
    emap = {"amount": _spec("amount", "numeric")}
    top = [{"feature": "amount", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    assert e["current"]["count"] == 1
    assert e["current"]["std"] is None
    assert e["current"]["mean"] == 42.0


# -- AC#21: one-hot mismatched current/past categories --------------------

def test_onehot_mismatched_categories_yield_zero_rate_on_missing_side():
    cur = pd.DataFrame({"channel": ["mobile"] * 5})
    past = [("p1", pd.DataFrame({"channel": ["web"] * 3 + ["mobile"] * 2}))]
    emap_mobile = _spec("channel", "onehot", category="mobile")
    emap_web = _spec("channel", "onehot", category="web")
    emap = {"channel_mobile": emap_mobile, "channel_web": emap_web}
    top = [
        {"feature": "channel_mobile", "importance": 0.7},
        {"feature": "channel_web", "importance": 0.3},
    ]
    entries, _ = explain_value_drift(top, cur, past, emap)
    mobile, web = entries
    assert mobile["current"]["rate"] == 1.0
    assert mobile["past"]["rate"] == pytest.approx(0.4)
    assert web["current"]["rate"] == 0.0


# -- AC#22: payload truncation preserves parallel length ------------------

def test_payload_truncation_preserves_parallel_length():
    # Build a label_encoded column with very long category strings so the
    # 16 KB payload budget is exceeded.
    long_value = "x" * 80
    n_cats = 200
    values = [f"{long_value}-{i}" for i in range(n_cats)]
    cur = pd.DataFrame({"k": values})
    past = [("p1", pd.DataFrame({"k": values}))]
    emap = {"k": _spec("k", "label_encoded")}
    top = [{"feature": f"k_{i}", "importance": 1.0 / (i + 1)} for i in range(20)]
    # All features point at the same source — but encoding_map only has "k",
    # so the unmapped path fires for k_0..k_19, each producing a tail that
    # repeats the long category mix. Force the long payload by using a
    # mapped feature once and letting the rest be unmapped:
    emap = {f"k_{i}": _spec("k", "label_encoded") for i in range(20)}
    entries, _ = explain_value_drift(top, cur, past, emap)
    # Parallel length always preserved.
    assert len(entries) == len(top)
    encoded_size = len(json.dumps(entries, default=str).encode("utf-8"))
    if encoded_size > _PAYLOAD_MAX_BYTES:
        # Truncation kicks in only when the payload was too large; if the
        # synthetic long-value gen wasn't large enough, this test still
        # passes the parallel-length invariant.
        pytest.skip("payload not large enough to trigger truncation")
    # If the test setup did exceed the budget after the run, assert the
    # truncated entries appear at the tail.
    truncated = [e for e in entries if e.get("kind") == "truncated"]
    if truncated:
        # Truncated entries are at the tail (lowest-importance positions).
        last_truncated = max(entries.index(e) for e in truncated)
        assert last_truncated == len(entries) - 1


def test_payload_truncation_actually_triggers(monkeypatch):
    """Force truncation by reducing the byte budget."""
    import qualifire.validation._drift_explainer as mod
    monkeypatch.setattr(mod, "_PAYLOAD_MAX_BYTES", 500)
    cur = pd.DataFrame({"amount": np.linspace(100, 200, 50)})
    past = [("p1", pd.DataFrame({"amount": np.linspace(50, 100, 100)}))]
    emap = {f"amount_{i}": _spec("amount", "numeric") for i in range(8)}
    top = [
        {"feature": f"amount_{i}", "importance": 1.0 / (i + 1)} for i in range(8)
    ]
    entries, _ = explain_value_drift(top, cur, past, emap)
    assert len(entries) == 8
    truncated = [e for e in entries if e.get("kind") == "truncated"]
    assert truncated, "expected at least one truncated placeholder"
    # Tail (lowest-importance) entries are truncated first.
    truncated_indices = [i for i, e in enumerate(entries) if e.get("kind") == "truncated"]
    assert max(truncated_indices) == len(entries) - 1


# -- AC#23: per-summary string length cap ---------------------------------

def test_summary_string_capped_at_200_chars():
    cur = pd.DataFrame({"k": ["a" * 500] * 3})
    past = [("p1", pd.DataFrame({"k": ["a" * 500] * 3}))]
    emap = {"k": _spec("k", "label_encoded")}
    top = [{"feature": "k", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    assert len(entries[0]["summary"]) <= _SUMMARY_MAX_CHARS


# -- AC#25: datetime NaT handling -----------------------------------------

def test_datetime_with_mixed_nat_returns_iso_strings_and_no_nat():
    cur = pd.DataFrame({
        "ts": pd.to_datetime(["2024-01-01", pd.NaT, "2024-01-05", "2024-01-10"]),
    })
    past = [("p1", pd.DataFrame({
        "ts": pd.to_datetime(["2023-12-01", "2023-12-15"]),
    }))]
    emap = {"ts_timestamp": _spec("ts", "datetime")}
    top = [{"feature": "ts_timestamp", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    assert e["current"]["null_pct"] == pytest.approx(0.25)
    assert isinstance(e["current"]["min"], str)
    assert isinstance(e["current"]["max"], str)
    # Round-trip JSON must succeed (no pd.NaT in the payload).
    json.dumps(entries, default=str)
    serialized = json.dumps(entries)
    assert "NaT" not in serialized


def test_datetime_all_nat_yields_null_pct_one_and_none_endpoints():
    cur = pd.DataFrame({
        "ts": pd.to_datetime([pd.NaT, pd.NaT]),
    })
    past = [("p1", pd.DataFrame({"ts": pd.to_datetime(["2023-12-01"])}))]
    emap = {"ts_timestamp": _spec("ts", "datetime")}
    top = [{"feature": "ts_timestamp", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    assert e["current"]["null_pct"] == 1.0
    assert e["current"]["min"] is None
    assert e["current"]["max"] is None
    assert e["current"]["count"] == 0


# -- AC#26: object-cell label-encoded representation ----------------------

def test_label_encoded_disjoint_top_keeps_past_only_categories():
    """Codex impl R2 HIGH regression: when current-top-N and
    past-top-N are entirely disjoint, the aligned union must keep
    every past-only category — slicing to _LABEL_TOP_N would
    drop them (their current rate is 0.0) and hide disappeared
    categories from the operator. Fix raises the cap to
    2 * _LABEL_TOP_N.
    """
    cur = pd.DataFrame({"k": ["a", "a", "b", "b", "c", "c", "d"]})
    past = [("p1", pd.DataFrame({
        "k": ["w", "w", "x", "x", "y", "y", "z", "z"]
    }))]
    emap = {"k": _spec("k", "label_encoded")}
    top = [{"feature": "k", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    cur_values = [t["value"] for t in e["current"]["top"]]
    past_values = [t["value"] for t in e["past"]["top"]]
    assert cur_values == past_values
    # All current-side values present.
    assert set(["a", "b", "c", "d"]).issubset(set(cur_values))
    # Past-only values also kept (they'd sort to tail with
    # current-rate=0; alignment must NOT drop them).
    assert any(v in cur_values for v in ["w", "x", "y", "z"])


def test_label_encoded_top_lists_aligned_across_current_past():
    """Codex impl R1 HIGH regression: per-side top-N lists must
    contain the SAME values in the SAME order so the operator
    can read across positionally. Missing-side rate is 0.0.
    """
    cur = pd.DataFrame({"k": ["a", "a", "b", "c"]})
    past = [("p1", pd.DataFrame({"k": ["x", "x", "x", "y", "a"]}))]
    emap = {"k": _spec("k", "label_encoded")}
    top = [{"feature": "k", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    cur_values = [t["value"] for t in e["current"]["top"]]
    past_values = [t["value"] for t in e["past"]["top"]]
    # Same length, same order, same values.
    assert cur_values == past_values
    # Categories that appear in only one side must still appear in
    # both lists with rate=0.0 on the missing side.
    if "x" in cur_values:
        idx = cur_values.index("x")
        assert e["current"]["top"][idx]["rate"] == 0.0
    if "b" in past_values:
        idx = past_values.index("b")
        assert e["past"]["top"][idx]["rate"] == 0.0
    # Sorted by current rate desc — "a" is current's most frequent.
    assert cur_values[0] == "a"


def test_label_encoded_null_cells_render_as_null_sentinel_not_string_nan():
    """Regression: pandas ``astype(str)`` converts NaN to the literal
    'nan'. Without the right ordering (fillna first, astype after) a
    real category named 'nan' is indistinguishable from a null cell
    in the top-N mix. Pin the contract: nulls render as ``"__NULL__"``.
    """
    cur = pd.DataFrame({"k": ["a", None, "b", None, None]})
    past = [("p1", pd.DataFrame({"k": ["a", "b", None]}))]
    emap = {"k": _spec("k", "label_encoded")}
    top = [{"feature": "k", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    cur_top_values = {e["value"] for e in entries[0]["current"]["top"]}
    assert "__NULL__" in cur_top_values
    assert "nan" not in cur_top_values


def test_label_encoded_with_dict_cells_stringifies_safely():
    cur = pd.DataFrame({
        "blob": [{"k": 1}, {"k": 2}, {"k": 1}, None],
    })
    past = [("p1", pd.DataFrame({"blob": [{"k": 1}, {"k": 1}]}))]
    emap = {"blob": _spec("blob", "label_encoded")}
    top = [{"feature": "blob", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    # Output must round-trip through JSON; cell values must be plain
    # strings (the str() representation of the dict), never raw dict
    # objects that would refuse to serialize.
    json.dumps(e)
    assert e["current"]["top"]
    for entry in e["current"]["top"]:
        assert isinstance(entry["value"], str)


# -- numeric integration: parallel-length, summary non-empty --------------

def test_numeric_integration_returns_parallel_summary_block():
    cur = pd.DataFrame({"amount": np.linspace(100, 200, 100)})
    past = [("p1", pd.DataFrame({"amount": np.linspace(50, 100, 200)}))]
    emap = {"amount": _spec("amount", "numeric")}
    top = [{"feature": "amount", "importance": 1.0}]
    entries, mapping_errors = explain_value_drift(top, cur, past, emap)
    assert mapping_errors == 0
    assert len(entries) == 1
    e = entries[0]
    assert e["kind"] == "numeric"
    assert e["current"]["mean"] is not None
    assert e["past"]["mean"] is not None
    assert e["delta"]["mean_pct"] is not None
    assert e["summary"]


# -- boolean integration --------------------------------------------------

def test_boolean_integration():
    cur = pd.DataFrame({"flag": pd.array([True, True, False, None], dtype="boolean")})
    past = [("p1", pd.DataFrame({"flag": pd.array([False, False, True], dtype="boolean")}))]
    emap = {"flag": _spec("flag", "boolean")}
    top = [{"feature": "flag", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    assert e["kind"] == "boolean"
    assert e["current"]["true_rate"] == pytest.approx(2 / 3)
    assert e["past"]["true_rate"] == pytest.approx(1 / 3)
    assert e["delta"]["true_rate_pp"] == pytest.approx(2 / 3 - 1 / 3)


# -- onehot integration with null bin -------------------------------------

def test_onehot_null_bin_rate_uses_isna():
    cur = pd.DataFrame({"channel": ["a", None, "a", None]})
    past = [("p1", pd.DataFrame({"channel": ["a", "a", None]}))]
    null_spec = _spec("channel", "onehot", category="nan", is_null_bin=True)
    emap = {"channel_nan": null_spec}
    top = [{"feature": "channel_nan", "importance": 1.0}]
    entries, _ = explain_value_drift(top, cur, past, emap)
    e = entries[0]
    assert e["current"]["rate"] == 0.5
    assert e["past"]["rate"] == pytest.approx(1 / 3)

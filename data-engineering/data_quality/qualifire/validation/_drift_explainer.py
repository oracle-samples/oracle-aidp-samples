"""Per-feature value-shift explainer for shape / pattern SHAP outputs.

Given the SHAP top-features list and the pre-encoding pandas
DataFrames the validator fed into the model, computes a per-feature
summary of *how* each feature's distribution shifted between the
current sample and the union of past slices.

Output is a list of dicts parallel to ``top_contributing_features``
(same length, same order), with size budgets enforced via in-place
``kind="truncated"`` placeholders so consumers can always
``zip(top_contributing_features, value_drift_explainer)``.

See ``docs/features/feature-value-drift-explainer/plan.md`` for the
schema contract, edge-case handling, and acceptance criteria.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from qualifire.validation._encoding import EncodedFeatureSpec

logger = logging.getLogger(__name__)


# Hard caps from the plan's "Size budgets" section.
_SUMMARY_MAX_CHARS = 200
_CATEGORY_MAX_CHARS = 64
_LABEL_TOP_N = 5
_PAYLOAD_MAX_BYTES = 16 * 1024  # 16 KB serialized JSON
_TRUNCATED_SUFFIX = "... [truncated]"

# drift-explainer-per-slice-breakdown:
# Per-entry serialized cap for `per_slice` attachment. When the
# entry's JSON size including the per_slice block exceeds this, we
# drop per_slice and set ``per_slice_truncated=True`` (Locked
# Decision 12). Pinned constant so impl-review can verify against
# the plan.
_PER_ENTRY_PER_SLICE_MAX_BYTES = 3072  # 3 KB

# Top-N entries that get the per_slice attachment when
# ``breakdown_by_slice=True``. The union top_features list is
# unchanged (still up to 5); only the first N carry per_slice
# (Locked Decision 8).
_PER_SLICE_TOP_N = 3

# Kinds for which we emit a per-slice block. Other kinds
# (`datetime`, `label_encoded`, `unknown`) and the unmapped /
# failed defensive paths get no per_slice block (Locked
# Decisions 5 / 5a / 5b / 6).
_PER_SLICE_SUPPORTED_KINDS = frozenset({"numeric", "onehot", "boolean"})


def explain_value_drift(
    top_features: list[dict[str, Any]],
    current_pdf: Any,
    past_pdfs: list[tuple[str, Any]],
    encoding_map: dict[str, EncodedFeatureSpec],
    *,
    breakdown_by_slice: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """Compute per-feature value-shift summaries.

    Args:
        top_features: SHAP attributions, each a dict with at least
            a ``"feature"`` key. Order is preserved in the output.
        current_pdf: Pre-encoding pandas DataFrame for the current
            slice (post-keep_cols, pre-``_qf_label`` injection).
        past_pdfs: Sampler-shaped list of ``(label, DataFrame)`` for
            historical slices. The explainer concatenates all past
            DataFrames to mirror SHAP's training contract (label-0 =
            union of past).
        encoding_map: Encoder output, keyed by every entry in
            ``feature_names``.
        breakdown_by_slice: When True, attach a ``per_slice`` block
            to the first ``_PER_SLICE_TOP_N`` entries with kind in
            ``_PER_SLICE_SUPPORTED_KINDS`` (numeric / onehot /
            boolean). Other kinds, unmapped entries, and per-feature
            failures get no per_slice block. Default ``False``
            preserves today's output verbatim.

    Returns:
        ``(entries, mapping_errors)`` where ``entries`` is a list of
        the same length / order as ``top_features`` and
        ``mapping_errors`` is the count of features whose name was
        missing from ``encoding_map`` (defensive path).
    """
    import pandas as pd

    if not top_features:
        return [], 0

    past_concat = _concat_past(past_pdfs)
    mapping_errors = 0
    entries: list[dict[str, Any]] = []
    # Track which input indices produced a real (mapped, non-failed)
    # entry — only those qualify for the per_slice attachment.
    mapped_indices: set[int] = set()

    for idx, tf in enumerate(top_features):
        feat_name = tf.get("feature", "")
        spec = encoding_map.get(feat_name)
        if spec is None:
            mapping_errors += 1
            logger.warning(
                "value drift explainer: unmapped feature %r", feat_name,
            )
            entries.append(_unmapped_entry(feat_name))
            continue
        try:
            entry = _entry_for_kind(
                feat_name, spec, current_pdf, past_concat
            )
            mapped_indices.add(idx)
        except Exception as exc:
            logger.warning(
                "value drift explainer: per-feature failure on %r: %s",
                feat_name, exc,
            )
            entry = {
                "feature": feat_name,
                "source_column": spec.source_column,
                "kind": "unknown",
                "current": {"count": 0, "null_pct": None},
                "past": {"count": 0, "null_pct": None},
                "summary": _truncate_summary(
                    f"{feat_name} — explainer failed: {type(exc).__name__}"
                ),
            }
        entries.append(entry)

    if breakdown_by_slice:
        _attach_per_slice(
            entries, top_features, encoding_map,
            current_pdf, past_pdfs, mapped_indices,
        )

    entries = _enforce_payload_budget(entries, top_features)
    return entries, mapping_errors


def _concat_past(past_pdfs: list[tuple[str, Any]]) -> Any:
    import pandas as pd
    if not past_pdfs:
        return pd.DataFrame()
    frames = [pdf for _, pdf in past_pdfs if pdf is not None]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def _attach_per_slice(
    entries: list[dict[str, Any]],
    top_features: list[dict[str, Any]],
    encoding_map: dict[str, EncodedFeatureSpec],
    current_pdf: Any,
    past_pdfs: list[tuple[str, Any]],
    mapped_indices: set[int],
) -> None:
    """In-place attach ``per_slice`` to qualifying entries.

    Gate (codex R1 MEDIUM, plan v4): all THREE conditions:
      a. index < _PER_SLICE_TOP_N (top-N)
      b. entry kind in _PER_SLICE_SUPPORTED_KINDS
      c. index in mapped_indices (entry was produced by a real
         _entry_for_kind call, not _unmapped_entry / failure path)
    """
    for idx in range(min(_PER_SLICE_TOP_N, len(entries))):
        if idx not in mapped_indices:
            continue
        entry = entries[idx]
        kind = entry.get("kind")
        if kind not in _PER_SLICE_SUPPORTED_KINDS:
            continue
        feat_name = top_features[idx].get("feature", "")
        spec = encoding_map.get(feat_name)
        if spec is None:
            continue  # defensive — would have been caught above
        per_slice = _build_per_slice(
            kind, spec, current_pdf, past_pdfs,
        )
        # Per-entry truncation (Locked Decision 12). Serialize the
        # candidate entry-with-per_slice and bail out to truncated
        # form if over budget.
        candidate = {**entry, "per_slice": per_slice}
        if len(json.dumps(candidate).encode("utf-8")) > _PER_ENTRY_PER_SLICE_MAX_BYTES:
            entry["per_slice_truncated"] = True
        else:
            entry["per_slice"] = per_slice


def _build_per_slice(
    kind: str,
    spec: EncodedFeatureSpec,
    current_pdf: Any,
    past_pdfs: list[tuple[str, Any]],
) -> list[dict[str, Any]]:
    """Walk past_pdfs as separate slices and compute per-slice stats.

    Per-kind ``current_vs_slice`` shape:
      numeric → {mean_pct, p99_pct, null_pct_abs}
      onehot  → {rate_pp}
      boolean → {true_rate_pp}
    Other kinds never reach here (gate filters them upstream).
    """
    src = spec.source_column
    cur = current_pdf[src] if src in current_pdf.columns else None
    out: list[dict[str, Any]] = []
    for label, slice_pdf in past_pdfs:
        slice_series = (
            slice_pdf[src]
            if (slice_pdf is not None and src in slice_pdf.columns)
            else None
        )
        if kind == "numeric":
            stats = _numeric_delta(
                _numeric_stats(cur), _numeric_stats(slice_series),
            )
            current_vs_slice = {
                "mean_pct": stats.get("mean_pct"),
                "p99_pct": stats.get("p99_pct"),
                "null_pct_abs": stats.get("null_pct_abs"),
            }
        elif kind == "onehot":
            cur_stats = _onehot_stats(cur, spec)
            slice_stats = _onehot_stats(slice_series, spec)
            current_vs_slice = {
                "rate_pp": _delta_pp(
                    cur_stats.get("rate"), slice_stats.get("rate"),
                ),
            }
        elif kind == "boolean":
            cur_stats = _boolean_stats(cur)
            slice_stats = _boolean_stats(slice_series)
            current_vs_slice = {
                "true_rate_pp": _delta_pp(
                    cur_stats.get("true_rate"),
                    slice_stats.get("true_rate"),
                ),
            }
        else:  # unreachable — filtered by gate
            current_vs_slice = {}
        out.append({
            "label": label,
            "current_vs_slice": current_vs_slice,
        })
    return out


def _unmapped_entry(feat_name: str) -> dict[str, Any]:
    return {
        "feature": feat_name,
        "source_column": feat_name,
        "kind": "unknown",
        "current": {"count": 0, "null_pct": None},
        "past": {"count": 0, "null_pct": None},
        "summary": _truncate_summary(f"unmapped feature: {feat_name}"),
    }


def _entry_for_kind(
    feat_name: str,
    spec: EncodedFeatureSpec,
    current_pdf: Any,
    past_pdf: Any,
) -> dict[str, Any]:
    src = spec.source_column
    cur = current_pdf[src] if src in current_pdf.columns else None
    past = past_pdf[src] if (past_pdf is not None and src in past_pdf.columns) else None

    if spec.kind == "numeric":
        cur_stats = _numeric_stats(cur)
        past_stats = _numeric_stats(past)
        delta = _numeric_delta(cur_stats, past_stats)
        summary = _summary_numeric(src, cur_stats, past_stats, delta)
        return {
            "feature": feat_name,
            "source_column": src,
            "kind": "numeric",
            "current": cur_stats,
            "past": past_stats,
            "delta": delta,
            "summary": _truncate_summary(summary),
        }
    if spec.kind == "boolean":
        cur_stats = _boolean_stats(cur)
        past_stats = _boolean_stats(past)
        delta = {
            "true_rate_pp": _delta_pp(
                cur_stats.get("true_rate"), past_stats.get("true_rate"),
            ),
        }
        summary = (
            f"{src} true_rate "
            f"{_pct(cur_stats.get('true_rate'))} vs "
            f"{_pct(past_stats.get('true_rate'))}"
        )
        return {
            "feature": feat_name,
            "source_column": src,
            "kind": "boolean",
            "current": cur_stats,
            "past": past_stats,
            "delta": delta,
            "summary": _truncate_summary(summary),
        }
    if spec.kind == "datetime":
        cur_stats = _datetime_stats(cur)
        past_stats = _datetime_stats(past)
        summary = (
            f"{src} range cur=[{cur_stats.get('min')}..{cur_stats.get('max')}] "
            f"past=[{past_stats.get('min')}..{past_stats.get('max')}]"
        )
        return {
            "feature": feat_name,
            "source_column": src,
            "kind": "datetime",
            "current": cur_stats,
            "past": past_stats,
            "summary": _truncate_summary(summary),
        }
    if spec.kind == "onehot":
        cur_stats = _onehot_stats(cur, spec)
        past_stats = _onehot_stats(past, spec)
        delta = {
            "rate_pp": _delta_pp(
                cur_stats.get("rate"), past_stats.get("rate"),
            ),
        }
        cat_disp = "<null>" if spec.is_null_bin else f"'{spec.category}'"
        summary = (
            f"{src}={cat_disp} rate "
            f"{_pct(cur_stats.get('rate'))} vs "
            f"{_pct(past_stats.get('rate'))} ({_signed_pp(delta['rate_pp'])})"
        )
        return {
            "feature": feat_name,
            "source_column": src,
            "kind": "onehot",
            "category": spec.category,
            "is_null_bin": spec.is_null_bin,
            "current": cur_stats,
            "past": past_stats,
            "delta": delta,
            "summary": _truncate_summary(summary),
        }
    if spec.kind == "label_encoded":
        cur_stats, past_stats = _label_encoded_stats_aligned(cur, past)
        cur_top = (cur_stats.get("top") or [])
        top_summary = ", ".join(
            f"{e['value']}={_pct(e['rate'])}" for e in cur_top[:3]
        )
        summary = f"{src} top: {top_summary}" if top_summary else f"{src} (no values)"
        return {
            "feature": feat_name,
            "source_column": src,
            "kind": "label_encoded",
            "current": cur_stats,
            "past": past_stats,
            "summary": _truncate_summary(summary),
        }
    # kind == "unknown"
    cur_count = _safe_len(cur)
    past_count = _safe_len(past)
    return {
        "feature": feat_name,
        "source_column": src,
        "kind": "unknown",
        "current": {"count": cur_count, "null_pct": _null_pct(cur)},
        "past": {"count": past_count, "null_pct": _null_pct(past)},
        "summary": _truncate_summary(
            f"{feat_name} — count cur={cur_count}, past={past_count}"
        ),
    }


# ---------- per-kind stat helpers ----------------------------------------

def _numeric_stats(series: Any) -> dict[str, Any]:
    import pandas as pd
    if series is None:
        return _empty_numeric_stats(zero_row=True)
    n = len(series)
    if n == 0:
        return _empty_numeric_stats(zero_row=True)
    nulls = int(series.isna().sum())
    finite = series.dropna()
    finite_n = len(finite)
    null_pct = _to_jsonable(nulls / n)
    if finite_n == 0:
        return {
            "count": 0,
            "null_pct": null_pct,
            "mean": None, "std": None,
            "p25": None, "p50": None, "p75": None, "p95": None, "p99": None,
            "min": None, "max": None,
        }
    try:
        std_val = float(finite.std()) if finite_n > 1 else None
    except Exception:
        std_val = None
    return {
        "count": int(finite_n),
        "null_pct": null_pct,
        "mean": _to_jsonable(float(finite.mean())),
        "std": _to_jsonable(std_val),
        "p25": _to_jsonable(float(finite.quantile(0.25))),
        "p50": _to_jsonable(float(finite.quantile(0.50))),
        "p75": _to_jsonable(float(finite.quantile(0.75))),
        "p95": _to_jsonable(float(finite.quantile(0.95))),
        "p99": _to_jsonable(float(finite.quantile(0.99))),
        "min": _to_jsonable(float(finite.min())),
        "max": _to_jsonable(float(finite.max())),
    }


def _empty_numeric_stats(zero_row: bool) -> dict[str, Any]:
    return {
        "count": 0,
        "null_pct": None if zero_row else 0.0,
        "mean": None, "std": None,
        "p25": None, "p50": None, "p75": None, "p95": None, "p99": None,
        "min": None, "max": None,
    }


def _numeric_delta(cur: dict[str, Any], past: dict[str, Any]) -> dict[str, Any]:
    return {
        "mean_pct": _delta_pct(cur.get("mean"), past.get("mean")),
        "p99_pct": _delta_pct(cur.get("p99"), past.get("p99")),
        "null_pct_abs": _delta_abs(cur.get("null_pct"), past.get("null_pct")),
    }


def _boolean_stats(series: Any) -> dict[str, Any]:
    if series is None:
        return {"count": 0, "null_pct": None, "true_rate": None}
    n = len(series)
    if n == 0:
        return {"count": 0, "null_pct": None, "true_rate": None}
    nulls = int(series.isna().sum())
    finite_n = n - nulls
    if finite_n == 0:
        return {"count": 0, "null_pct": _to_jsonable(nulls / n), "true_rate": None}
    truthy = int(series.fillna(False).astype(bool).sum())
    # Subtract nulls counted as True via fillna(False) — fillna(False)
    # makes nulls count as False, so truthy = number of real Trues.
    return {
        "count": int(finite_n),
        "null_pct": _to_jsonable(nulls / n),
        "true_rate": _to_jsonable(truthy / finite_n),
    }


def _datetime_stats(series: Any) -> dict[str, Any]:
    import pandas as pd
    if series is None:
        return {"count": 0, "null_pct": None, "min": None, "max": None}
    n = len(series)
    if n == 0:
        return {"count": 0, "null_pct": None, "min": None, "max": None}
    nulls = int(series.isna().sum())
    finite = series.dropna()
    finite_n = len(finite)
    null_pct = _to_jsonable(nulls / n)
    if finite_n == 0:
        return {"count": 0, "null_pct": null_pct, "min": None, "max": None}
    try:
        mn = pd.Timestamp(finite.min())
        mx = pd.Timestamp(finite.max())
    except Exception:
        return {
            "count": int(finite_n), "null_pct": null_pct,
            "min": str(finite.min()), "max": str(finite.max()),
        }
    return {
        "count": int(finite_n),
        "null_pct": null_pct,
        "min": _ts_to_iso(mn),
        "max": _ts_to_iso(mx),
    }


def _onehot_stats(series: Any, spec: EncodedFeatureSpec) -> dict[str, Any]:
    if series is None:
        return {"count": 0, "rate": None}
    n = len(series)
    if n == 0:
        return {"count": 0, "rate": None}
    if spec.is_null_bin:
        match = int(series.isna().sum())
    else:
        category_value = spec.category
        # Compare as string OR as raw — but the encoder dropped one-hot
        # bins from string suffixes, so the source column's values may
        # be strings or original types. Compare as string
        # representations to be type-agnostic, ignoring nulls.
        s = series.dropna()
        match = int((s.astype(str) == str(category_value)).sum())
    return {"count": int(n), "rate": _to_jsonable(match / n)}


def _label_encoded_stats_aligned(
    cur: Any, past: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build aligned current / past label-encoded stats.

    Plan R1.9: top-N category mix is the **union** of current-top-N
    and past-top-N raw values, sorted by current rate descending.
    Missing-side rates default to ``0.0`` so the operator can read
    the two lists positionally without alignment math.
    """
    cur_n, cur_nulls, cur_counts = _stringified_value_counts(cur)
    past_n, past_nulls, past_counts = _stringified_value_counts(past)
    cur_pct = (
        _to_jsonable(cur_nulls / cur_n) if cur_n else None
    )
    past_pct = (
        _to_jsonable(past_nulls / past_n) if past_n else None
    )
    # Union of top-N values from each side.
    cur_top_values = [v for v, _ in cur_counts.head(_LABEL_TOP_N).items()]
    past_top_values = [v for v, _ in past_counts.head(_LABEL_TOP_N).items()]
    union_values: list[str] = []
    seen: set[str] = set()
    for v in cur_top_values + past_top_values:
        if v not in seen:
            seen.add(v)
            union_values.append(v)
    # Sort by current rate desc, tie-break by past rate desc.
    def _cur_rate(v: str) -> float:
        c = int(cur_counts.get(v, 0))
        return c / cur_n if cur_n else 0.0
    def _past_rate(v: str) -> float:
        c = int(past_counts.get(v, 0))
        return c / past_n if past_n else 0.0
    union_values.sort(key=lambda v: (-_cur_rate(v), -_past_rate(v)))
    # Plan R1.9: list is the UNION of current-top-N and past-top-N,
    # which is at most 2*_LABEL_TOP_N entries. Slicing to _LABEL_TOP_N
    # would drop past-only categories (their current rate is 0.0 so
    # they sort to the tail) and hide disappeared categories from
    # the operator (codex impl-review R2 HIGH).
    _UNION_CAP = 2 * _LABEL_TOP_N
    cur_top: list[dict[str, Any]] = []
    past_top: list[dict[str, Any]] = []
    for v in union_values[:_UNION_CAP]:
        cur_top.append({
            "value": _truncate_category(v),
            "rate": _to_jsonable(_cur_rate(v)),
        })
        past_top.append({
            "value": _truncate_category(v),
            "rate": _to_jsonable(_past_rate(v)),
        })
    return (
        {"count": int(cur_n - cur_nulls), "null_pct": cur_pct, "top": cur_top},
        {"count": int(past_n - past_nulls), "null_pct": past_pct, "top": past_top},
    )


def _stringified_value_counts(series: Any) -> tuple[int, int, Any]:
    """Return (n, nulls, value_counts_series) over the str representation.

    Pre-fills NaN to ``"__NULL__"`` BEFORE astype(str) so a real
    null doesn't collide with a real category whose value happens
    to be the literal string ``"nan"``. See _label_encoded_stats
    docstring for the ordering rationale.
    """
    if series is None:
        import pandas as pd
        return 0, 0, pd.Series(dtype=int)
    n = len(series)
    if n == 0:
        import pandas as pd
        return 0, 0, pd.Series(dtype=int)
    nulls = int(series.isna().sum())
    try:
        as_str = series.fillna("__NULL__").astype(str)
    except Exception:
        as_str = series.apply(lambda v: "__NULL__" if v is None else str(v))
    return n, nulls, as_str.value_counts(dropna=False)


def _label_encoded_stats(series: Any) -> dict[str, Any]:
    if series is None:
        return {"count": 0, "null_pct": None, "top": []}
    n = len(series)
    if n == 0:
        return {"count": 0, "null_pct": None, "top": []}
    nulls = int(series.isna().sum())
    null_pct = _to_jsonable(nulls / n)
    # Stringify with the encoder's fill-null sentinel so unhashable
    # cells (dicts/lists) round-trip to a stable representation. Plan
    # AC#26: object cells must not appear raw in the JSON. Note: must
    # ``fillna`` BEFORE ``astype(str)`` — pandas' ``astype(str)`` on
    # NaN returns the literal string "nan", which would then survive
    # a post-cast ``fillna`` and pollute the top-N category mix as a
    # real category named "nan".
    try:
        as_str = series.fillna("__NULL__").astype(str)
    except Exception:
        as_str = series.apply(lambda v: "__NULL__" if v is None else str(v))
    counts = as_str.value_counts(dropna=False)
    top: list[dict[str, Any]] = []
    for value, cnt in counts.head(_LABEL_TOP_N).items():
        top.append({
            "value": _truncate_category(str(value)),
            "rate": _to_jsonable(int(cnt) / n),
        })
    return {
        "count": int(n - nulls),
        "null_pct": null_pct,
        "top": top,
    }


# ---------- helpers ------------------------------------------------------

def _delta_pct(cur: Any, past: Any) -> Any:
    if cur is None or past is None:
        return None
    try:
        if past == 0:
            return None
        v = (cur - past) / past
    except Exception:
        return None
    return _to_jsonable(v)


def _delta_abs(cur: Any, past: Any) -> Any:
    if cur is None or past is None:
        return None
    try:
        v = cur - past
    except Exception:
        return None
    return _to_jsonable(v)


def _delta_pp(cur: Any, past: Any) -> Any:
    return _delta_abs(cur, past)


def _pct(v: Any) -> str:
    if v is None:
        return "n/a"
    try:
        return f"{float(v) * 100:.1f}%"
    except Exception:
        return "n/a"


def _signed_pp(v: Any) -> str:
    if v is None:
        return "Δn/a"
    try:
        return f"{float(v) * 100:+.1f}pp"
    except Exception:
        return "Δn/a"


def _signed_pct(v: Any) -> str:
    if v is None:
        return "Δn/a"
    try:
        return f"{float(v) * 100:+.0f}%"
    except Exception:
        return "Δn/a"


def _summary_numeric(
    src: str,
    cur: dict[str, Any],
    past: dict[str, Any],
    delta: dict[str, Any],
) -> str:
    cur_p50 = cur.get("p50")
    past_p50 = past.get("p50")
    parts = [src]
    if cur_p50 is not None and past_p50 is not None:
        parts.append(f"p50 {cur_p50:g} vs {past_p50:g}")
    elif cur_p50 is not None:
        parts.append(f"p50 cur={cur_p50:g} (no past)")
    if delta.get("mean_pct") is not None:
        parts.append(f"mean {_signed_pct(delta['mean_pct'])}")
    if delta.get("p99_pct") is not None:
        parts.append(f"p99 {_signed_pct(delta['p99_pct'])}")
    return "; ".join(parts)


def _to_jsonable(value: Any) -> Any:
    """Cast numpy / pandas scalars to plain Python; non-finite → None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int,)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    # numpy / pandas scalar fallback
    try:
        import numpy as np
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            f = float(value)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
    except Exception:
        pass
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return None


def _safe_len(series: Any) -> int:
    if series is None:
        return 0
    try:
        return int(len(series))
    except Exception:
        return 0


def _null_pct(series: Any) -> Any:
    if series is None:
        return None
    n = _safe_len(series)
    if n == 0:
        return None
    try:
        return _to_jsonable(int(series.isna().sum()) / n)
    except Exception:
        return None


def _ts_to_iso(ts: Any) -> str | None:
    try:
        # pandas Timestamp → isoformat; tz-aware preserved.
        return ts.isoformat()
    except Exception:
        try:
            return str(ts)
        except Exception:
            return None


def _truncate_summary(s: str) -> str:
    if not isinstance(s, str):
        s = str(s)
    if len(s) <= _SUMMARY_MAX_CHARS:
        return s
    keep = _SUMMARY_MAX_CHARS - len(_TRUNCATED_SUFFIX)
    return s[: max(0, keep)] + _TRUNCATED_SUFFIX


def _truncate_category(s: str) -> str:
    if len(s) <= _CATEGORY_MAX_CHARS:
        return s
    keep = _CATEGORY_MAX_CHARS - len(_TRUNCATED_SUFFIX)
    return s[: max(0, keep)] + _TRUNCATED_SUFFIX


def _enforce_payload_budget(
    entries: list[dict[str, Any]],
    top_features: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Replace tail entries with kind="truncated" placeholders until
    the serialized payload fits the byte budget.

    Preserves parallel length / order with ``top_features`` (the head
    of the list — the most-important entries — survives intact).
    """
    if not entries:
        return entries
    try:
        encoded = json.dumps(entries, default=str).encode("utf-8")
    except Exception:
        encoded = b""
    if len(encoded) <= _PAYLOAD_MAX_BYTES:
        return entries

    out = list(entries)
    # Truncate from the tail (lowest importance) until under budget.
    for i in range(len(out) - 1, -1, -1):
        feat_name = (top_features[i] if i < len(top_features) else {}).get(
            "feature", ""
        ) or out[i].get("feature", "")
        out[i] = {
            "feature": feat_name,
            "source_column": out[i].get("source_column", feat_name),
            "kind": "truncated",
            "summary": "payload truncated",
        }
        try:
            encoded = json.dumps(out, default=str).encode("utf-8")
        except Exception:
            break
        if len(encoded) <= _PAYLOAD_MAX_BYTES:
            break
    return out

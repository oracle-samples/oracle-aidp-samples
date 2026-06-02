"""Helpers for projecting validator ``details_json`` into the
interactive dashboard's embedded SNAPSHOT JSON.

Two concerns, kept here so the ~1300-line html_report.py stays
focused on rendering:

1. ``_snapshot_details`` — parse the persisted JSON, apply
   path-aware redaction + truncation, return a dashboard-safe
   dict (or ``None`` if the source is missing / unparseable).
2. ``_safe_json_for_html_script`` — escape the four sequences
   that can break out of a ``<script>`` block during
   interpolation so a hostile or buggy ``details`` payload
   cannot terminate the dashboard's embedded JSON literal.

Both are unit-tested via ``tests/test_reporting/test_snapshot_details.py``
and load-bearing for the dashboard-rich-detail-panel feature.
See ``docs/features/dashboard-rich-detail-panel/plan.md`` for the
full schema contract and acceptance criteria.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# Per-row + per-payload size budgets (plan: "Size budgets" section).
_PER_ROW_MAX_BYTES = 8 * 1024
_PER_KEY_MAX_BYTES = 3 * 1024
_PER_STRING_MAX_BYTES = 2 * 1024
_TOTAL_SNAPSHOT_MAX_BYTES = 8 * 1024 * 1024
_TRUNCATED_STRING = "[truncated]"
_REDACTED_STRING = "[redacted]"


# Whole-key regex for unintended secret-y key names. Matches only on
# dict keys (NOT leaf string values) and only at non-explainer paths.
# Whole-key (not substring) so legitimate keys like
# "password_check_threshold" survive.
_SENSITIVE_KEY_RE = re.compile(
    r"^(password|secret|token|api[_-]?key|credential)$",
    re.IGNORECASE,
)


# JSON paths that the redaction MUST NOT walk into. The
# value-drift-explainer's category mix is operator-meaningful even
# when categories happen to look like emails / tokens; redacting
# them silently masks the very drift the explainer is meant to
# surface.
_EXPLAINER_EXEMPT_KEYS = frozenset({"value", "category"})

# Top-level keys whose subtrees the per-key cap must NOT replace
# with a scalar size marker — those structures are
# parallel-list-load-bearing for the dashboard renderer.
# Truncation, when needed, happens via the paired tail-replacement
# path (`_truncate_paired_to_budget`) that keeps both lists in
# lock-step (codex impl-review R1 BLOCKER #2).
_PAIRED_LIST_KEYS = ("top_contributing_features", "value_drift_explainer")


def _safe_json_for_html_script(obj: Any) -> str:
    """Serialize ``obj`` to JSON safe for embedding in a ``<script>`` block.

    Escapes the four sequences that can break out of a script tag or
    inject HTML comment state:

    * ``</`` → ``<\\/`` (closes any HTML tag, especially ``</script>``)
    * U+2028 → ``\\u2028`` (line separator; valid in JSON strings,
      invalid in JS source — V8 historically accepted it but the
      JSON.parse tests assume escaping for compat)
    * U+2029 → ``\\u2029`` (paragraph separator; same)
    * ``<!--`` → ``<\\u0021--`` (HTML comment opener inside a script
      tag changes parser state for some browsers)

    The ``\\/`` form is a valid JSON solidus escape; ``JSON.parse``
    decodes it back to ``/``, but the HTML parser can't see it as
    closing a tag.
    """
    raw = json.dumps(obj, default=str)
    return (
        raw
        .replace("</", "<\\/")
        .replace(" ", "\\u2028")
        .replace(" ", "\\u2029")
        .replace("<!--", "<\\u0021--")
    )


def _snapshot_details(
    raw: Any,
    *,
    max_bytes: int = _PER_ROW_MAX_BYTES,
) -> dict[str, Any] | None:
    """Project a persisted ``details_json`` value into a dashboard-safe dict.

    Args:
        raw: Either a ``dict`` (in-memory engine path) or a JSON
            string (re-read from the persisted TEXT column). Mirrors
            the dual-shape tolerance of
            ``qualifire.reporting.health._row_is_internal_failure``.
        max_bytes: Per-row JSON-serialized size cap. Default is the
            plan's 8 KB.

    Returns:
        - ``None`` when ``raw`` is missing / unparseable.
        - A dict with sensitive keys redacted, oversize values
          truncated, and the ``value_drift_explainer`` block
          preserved with parallel-list / parallel-order semantics
          (per PR #15's contract).
        - ``{"_truncated": True, "_reason": "row payload exceeded
          {N} bytes"}`` if the row still exceeds the cap after all
          per-key / per-string truncations.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.debug(
                "_snapshot_details: details_json parse failed (%s)", exc,
            )
            return None
    else:
        parsed = raw
    if not isinstance(parsed, dict):
        return None

    # Drop the qualifire_internal_failure marker — it's already
    # surfaced as a top-level snapshot field (see html_report.py
    # line ~406). Keeping it here would double-count.
    parsed = {k: v for k, v in parsed.items() if k != "qualifire_internal_failure"}

    # Truncate per-string and per-non-explainer-dict-or-list. The
    # explainer block is preserved separately by
    # ``_truncate_explainer_to_budget``.
    redacted = _redact_and_truncate(parsed, path=())

    if _json_size(redacted) <= max_bytes:
        return redacted

    # Still too big. The paired explainer schema
    # (``top_contributing_features`` × ``value_drift_explainer``) is
    # parallel-list-load-bearing for the dashboard renderer — we
    # truncate the same tail in BOTH lists together rather than
    # replacing either with a scalar marker (codex impl-review R1
    # BLOCKER #2). Strategy:
    # 1. Free non-paired payload first (drop largest non-paired
    #    keys until under budget).
    # 2. Tail-truncate the paired lists in lock-step until under
    #    budget.
    # 3. Last resort: row-level truncation marker.
    paired_keys = [k for k in _PAIRED_LIST_KEYS if isinstance(redacted.get(k), list)]
    if paired_keys:
        non_paired = {k: v for k, v in redacted.items() if k not in _PAIRED_LIST_KEYS}
        paired_payload = {k: redacted[k] for k in paired_keys}
        budget_left = max_bytes - _json_size(paired_payload) - 32
        if budget_left > 0 and _json_size(non_paired) > budget_left:
            redacted = _shrink_non_explainer(redacted, budget_left)
        if _json_size(redacted) <= max_bytes:
            return redacted

        # Tail-truncate the paired lists together.
        non_paired_now = {k: v for k, v in redacted.items() if k not in _PAIRED_LIST_KEYS}
        paired_budget = max_bytes - _json_size(non_paired_now) - 64
        truncated_paired = _truncate_paired_to_budget(
            {k: redacted[k] for k in paired_keys}, paired_budget,
        )
        for k, v in truncated_paired.items():
            redacted[k] = v
        if _json_size(redacted) <= max_bytes:
            return redacted

    return {
        "_truncated": True,
        "_reason": f"row payload exceeded {max_bytes} bytes",
    }


def _redact_and_truncate(
    value: Any, *, path: tuple[str, ...],
) -> Any:
    """Recurse the dict tree applying redaction + per-string + per-key caps.

    ``path`` is the JSON pointer to ``value`` from the row-root, used
    to exempt explainer-schema fields from redaction.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            new_path = path + (str(k),)
            if _is_sensitive_key(k, new_path):
                out[k] = _REDACTED_STRING
                continue
            out[k] = _redact_and_truncate(v, path=new_path)
        # Per-key cap applies to dicts/lists at non-explainer paths.
        # The explainer block is checked separately via
        # ``_truncate_explainer_to_budget``.
        return _maybe_truncate_collection(out, path)
    if isinstance(value, list):
        out_list = [
            _redact_and_truncate(item, path=path + (str(i),))
            for i, item in enumerate(value)
        ]
        return _maybe_truncate_collection(out_list, path)
    if isinstance(value, str):
        if len(value.encode("utf-8")) > _PER_STRING_MAX_BYTES:
            return _TRUNCATED_STRING
        return value
    return value


def _maybe_truncate_collection(
    value: Any, path: tuple[str, ...],
) -> Any:
    """Replace oversize NESTED dict/list with a size marker.

    The root path (``path == ()``) is exempt — the per-row cap is
    evaluated at the top level by the caller's row-level fallback
    (``{"_truncated": True, ...}``); replacing the entire row with
    a string here would short-circuit that path. The
    ``value_drift_explainer`` subtree is also exempt because its
    truncation contract is parallel-list aware (see
    ``_truncate_explainer_to_budget``).
    """
    if not path:
        return value
    # Both halves of the paired explainer schema are preserved
    # here — the per-row 8 KB fallback in ``_snapshot_details``
    # truncates them in lock-step via ``_truncate_paired_to_budget``.
    # Replacing one with a scalar marker would break the JS
    # renderer's parallel-list zip (codex impl-review R1 BLOCKER #2).
    if path[0] in _PAIRED_LIST_KEYS:
        return value
    size = _json_size(value)
    if size <= _PER_KEY_MAX_BYTES:
        return value
    if isinstance(value, dict):
        return f"[truncated dict size={size} bytes]"
    return f"[truncated list len={len(value)}]"


def _is_sensitive_key(
    key: str, path: tuple[str, ...],
) -> bool:
    """Whole-key sensitive-name match, scoped away from explainer paths."""
    if not isinstance(key, str):
        return False
    if path and path[0] == "value_drift_explainer" and key in _EXPLAINER_EXEMPT_KEYS:
        return False
    return bool(_SENSITIVE_KEY_RE.match(key))


def _truncate_paired_to_budget(
    paired: dict[str, list[Any]], byte_budget: int,
) -> dict[str, list[Any]]:
    """Tail-truncate ``top_contributing_features`` and
    ``value_drift_explainer`` in lock-step until the serialized
    pair fits the byte budget.

    Both lists keep their full length; tail entries are replaced
    with ``kind="truncated"`` (explainer) and a matching placeholder
    in ``top_contributing_features`` so a JS-side ``zip()`` still
    aligns. Truncation order is reverse-importance (the list head
    is preserved).
    """
    out = {k: list(v) for k, v in paired.items()}
    if _json_size(out) <= byte_budget:
        return out
    # Both lists should have the same length when the explainer
    # block is well-formed. Use whichever is longer as the upper
    # bound for tail-truncation.
    max_len = max((len(v) for v in out.values()), default=0)
    for i in range(max_len - 1, -1, -1):
        if "top_contributing_features" in out and i < len(out["top_contributing_features"]):
            entry = out["top_contributing_features"][i]
            feat = (entry.get("feature") if isinstance(entry, dict) else None) or ""
            out["top_contributing_features"][i] = {
                "feature": feat,
                "importance": 0.0,
                "kind": "truncated",
            }
        if "value_drift_explainer" in out and i < len(out["value_drift_explainer"]):
            entry = out["value_drift_explainer"][i]
            feat = (entry.get("feature") if isinstance(entry, dict) else None) or ""
            src = (entry.get("source_column") if isinstance(entry, dict) else None) or feat
            out["value_drift_explainer"][i] = {
                "feature": feat,
                "source_column": src,
                "kind": "truncated",
                "summary": "payload truncated",
            }
        if _json_size(out) <= byte_budget:
            return out
    return out


def _truncate_explainer_to_budget(
    entries: list[Any], byte_budget: int,
) -> list[Any]:
    """Replace tail entries with kind="truncated" placeholders until
    the serialized list fits the byte budget.

    Mirrors PR #15's own internal truncation contract so the dashboard
    panel's parallel-list rendering keeps lining up with
    ``top_contributing_features``.
    """
    out = list(entries)
    if _json_size(out) <= byte_budget:
        return out
    for i in range(len(out) - 1, -1, -1):
        if not isinstance(out[i], dict):
            continue
        feat = out[i].get("feature", "")
        out[i] = {
            "feature": feat,
            "source_column": out[i].get("source_column", feat),
            "kind": "truncated",
            "summary": "payload truncated",
        }
        if _json_size(out) <= byte_budget:
            break
    return out


def _shrink_non_explainer(
    payload: dict[str, Any], non_paired_budget: int,
) -> dict[str, Any]:
    """Drop non-paired keys (largest first) until the budget fits.

    Preserves both halves of the paired explainer schema
    (``top_contributing_features`` and ``value_drift_explainer``)
    untouched — those have their own pair-aware truncation path.
    """
    paired = {k: payload[k] for k in _PAIRED_LIST_KEYS if k in payload}
    items = [(k, v) for k, v in payload.items() if k not in _PAIRED_LIST_KEYS]
    items.sort(key=lambda kv: -_json_size(kv[1]))
    keep: dict[str, Any] = {}
    for k, v in items:
        keep[k] = v
        if _json_size(keep) > non_paired_budget:
            keep.pop(k)
            break
    keep.update(paired)
    return keep


def _json_size(value: Any) -> int:
    """Cheap UTF-8 byte size of ``value`` JSON-serialized."""
    try:
        return len(json.dumps(value, default=str).encode("utf-8"))
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Total-snapshot 8 MB ceiling — multi-stage cap (plan §3).


def _row_escaped_size(row: dict[str, Any]) -> int:
    """Escaped UTF-8 byte size of a single snapshot row.

    Used to maintain a running total in ``enforce_total_cap`` so
    Stage 1 / Stage 2 stay O(N) per pass rather than O(N²) by
    re-serializing the whole snapshot each iteration.
    """
    return len(_safe_json_for_html_script(row).encode("utf-8"))


def _escaped_size(snapshot: list[dict[str, Any]]) -> int:
    """Byte size of ``snapshot`` after the ``_safe_json_for_html_script``
    expansion. The embedded ``<script>`` block contains the escaped
    form, so the cap MUST be measured against that — not the raw
    ``json.dumps`` output (codex impl-review R1 BLOCKER #1)."""
    return len(_safe_json_for_html_script(snapshot).encode("utf-8"))


def enforce_total_cap(
    snapshot: list[dict[str, Any]],
    *,
    max_bytes: int = _TOTAL_SNAPSHOT_MAX_BYTES,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Cap the embedded SNAPSHOT JSON to ``max_bytes`` total.

    Stage 1: replace each row's ``details`` (oldest first by
    ``run_timestamp``, deterministic tiebreak) with a per-row
    truncation marker until the total fits.

    Stage 2: if Stage 1 strips ALL details and the total is still
    over budget, drop the oldest rows entirely using the same
    stable sort key.

    The cap is measured against the ``_safe_json_for_html_script``-
    expanded byte length (codex impl-review R1 BLOCKER #1) — the
    escaped form is what actually lands inside the dashboard's
    ``<script>`` tag, so a cap measured on raw ``json.dumps`` could
    silently exceed the 8 MB limit after ``</`` / ``<!--`` /
    U+2028 / U+2029 expansion.

    Performance contract (codex impl-review R1 BLOCKER #1): both
    stages stream once through the rows in deterministic order
    while maintaining a running total of ``_row_escaped_size``,
    so the cost is O(N) per stage rather than O(N²) per stage.
    The running total slightly underestimates the true escaped
    size by the per-row separator overhead (commas, brackets) —
    a final ``_escaped_size(out)`` check after each stage detects
    that case and re-runs with a tighter budget.
    """
    if _escaped_size(snapshot) <= max_bytes:
        return snapshot, None

    # Stable sort key (R2.2 / codex R2 MEDIUM-1): deterministic
    # tiebreak on run_timestamp ties.
    def _sort_key(row: dict[str, Any]) -> tuple:
        return (
            str(row.get("run_timestamp") or ""),
            str(row.get("dataset_name") or ""),
            str(row.get("validation_name") or ""),
            str(row.get("dimension_value") or ""),
        )

    # Index rows in deterministic strip order (oldest first).
    indexed = sorted(
        range(len(snapshot)), key=lambda i: _sort_key(snapshot[i]),
    )

    out = [dict(r) for r in snapshot]
    truncation_marker = {
        "_truncated": True,
        "_reason": "snapshot payload exceeded 8 MB",
    }
    marker_size = _row_escaped_size(truncation_marker)

    # Per-row escaped sizes, maintained as a running total. Includes
    # 1 byte / row for the ``,`` separator the JSON list serializer
    # would emit (a tiny overestimate; we re-check at the end with
    # the real escaped size and fall back to a tighter budget if
    # we're still over).
    row_sizes = [_row_escaped_size(r) + 1 for r in out]
    total = sum(row_sizes) + 2  # "[" and "]"

    # Stage 1: strip details, oldest first.
    stripped = 0
    for idx in indexed:
        if total <= max_bytes:
            break
        row = out[idx]
        if row.get("details") not in (None, truncation_marker):
            row["details"] = dict(truncation_marker)
            new_size = _row_escaped_size(row) + 1
            total -= row_sizes[idx]
            total += new_size
            row_sizes[idx] = new_size
            stripped += 1

    # True size check — running total is an estimate.
    actual = _escaped_size(out)
    if actual <= max_bytes:
        return out, ({
            "dropped": 0,
            "stripped": stripped,
            "reason": "details stripped to fit 8 MB cap",
        } if stripped else None)

    # Stage 2: drop oldest rows entirely via running-total prefix
    # drop. Walk in oldest-first order, deduct each row's size
    # from the total, stop when total fits.
    keep_mask = [True] * len(out)
    dropped = 0
    for idx in indexed:
        if total <= max_bytes:
            break
        keep_mask[idx] = False
        total -= row_sizes[idx]
        dropped += 1
    pruned = [out[i] for i in range(len(out)) if keep_mask[i]]

    # Final true-size check; if our running-total estimate was too
    # optimistic (rare — would mean per-row separator estimate was
    # off), drop additional rows until the actual size fits. This
    # second pass is bounded by the small drift between estimate
    # and reality and is O(N) at worst with O(serialize) per call.
    while pruned and _escaped_size(pruned) > max_bytes:
        # find the oldest still-kept index in `indexed`.
        for idx in indexed:
            if keep_mask[idx]:
                keep_mask[idx] = False
                dropped += 1
                pruned = [out[i] for i in range(len(out)) if keep_mask[i]]
                break

    return pruned, {
        "dropped": dropped,
        "stripped": stripped,
        "reason": "metadata overflow; oldest rows omitted",
    }

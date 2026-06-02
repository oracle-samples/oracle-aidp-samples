"""Random sampling collector for Isolation Forest anomaly detection."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from qualifire.backends.base import Backend
from qualifire.collection._filters import (
    # ``_and_combine`` was originally defined in this module; the
    # implementation moved to ``qualifire.collection._filters`` so
    # the engine's three filtered collector call sites can share
    # one helper. The private alias preserves the existing two
    # call sites at lines ~122 and ~249 byte-equal. The shared
    # helper additionally normalises whitespace-only operands to
    # ``None`` (the pre-move local ``_and_combine`` truthiness-
    # skipped only empty strings); ``test_and_combine_treats_
    # whitespace_as_empty`` in tests pins the new behaviour.
    and_combine_filters as _and_combine,
)
from qualifire.collection.base import Collector
from qualifire.core.context import QualifireContext
from qualifire.core.duration import parse_duration
from qualifire.core.models import CollectionResult


class SamplerCollector(Collector):
    """Samples N records from current and past partitions for anomaly detection.

    Adds a ``_qf_period`` column: ``"current"`` for the current batch,
    ``"past_N"`` for historical batches (N = 1, 2, 3, ...).

    Slicing model
    -------------
    The sampler uses an explicit ``slice_column`` + ``slice_value``
    pair. ``slice_value`` is Jinja-rendered, parsed as an ISO date /
    datetime, and used as the anchor:

        current slice  → slice_column = '<slice_value>'
        past slice k   → slice_column = '<slice_value − k·step>'  (k = 1..past_dates)

    ``slice_value`` may or may not equal the dataset's
    ``partition_ts``; the sampler doesn't assume they match.
    Operators write ``slice_value="{{ ds }}"`` or supply a literal
    like ``"2026-04-28"`` — the sampler renders, parses, and shifts.

    Dataset-level filters arrive via ``filter_expr`` (sourced from
    ``DatasetConfig.filter`` by the engine). They AND-combine with
    the per-slice partition predicate before each
    ``backend.sample_records`` call so the same ``region = 'us'``
    qualifier applies to every slice unchanged.

    Escape hatch: ``history_filters`` lets a caller supply explicit
    per-slice SQL predicates verbatim. The sampler doesn't compute
    anchors and doesn't AND-combine ``filter_expr`` — the predicates
    are taken exactly as written. Use this when the partition is not
    date-shaped (e.g. version IDs).

    Args:
        n_records: Number of records to sample per period.
        slice_column: Name of the partition column. Required with
            ``slice_value``.
        slice_value: Jinja-renderable value for the current slice
            (e.g. ``"{{ ds }}"`` or ``"2026-04-28"``). After rendering
            it must parse as an ISO date / datetime so the sampler
            can compute past anchors.
        filter_expr: Dataset-level filter applied unchanged to every
            slice (current and past). Comes from ``DatasetConfig.filter``
            via the engine.
        past_dates: Number of historical periods to sample.
        step: ISO 8601 duration between historical anchors
            (e.g. ``"P7D"``). Required when ``history_filters`` is
            absent and ``past_dates`` > 0.
        history_filters: Explicit filter expressions for each past
            period. Bypasses anchor math and ``filter_expr`` —
            those filters are taken verbatim.
    """

    def __init__(
        self,
        n_records: int = 10000,
        slice_column: str | None = None,
        slice_value: str | None = None,
        filter_expr: str | None = None,
        past_dates: int = 3,
        step: str | None = None,
        history_filters: list[str] | None = None,
    ):
        if (slice_column is None) != (slice_value is None):
            raise ValueError(
                "SamplerCollector: slice_column and slice_value must be "
                "set together (or both omitted). Pass both for the "
                "common date-partition case, or use history_filters "
                "with explicit per-slice predicates."
            )
        self.n_records = n_records
        self.slice_column = slice_column
        self.slice_value = slice_value
        self.filter_expr = filter_expr
        self.past_dates = past_dates
        self.step = step
        self.history_filters = history_filters

    def collect(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        **kwargs: Any,
    ) -> list[CollectionResult]:
        now = datetime.now()

        # Render the dataset-level filter_expr once. Other collectors
        # render their filter_expr at the engine boundary; the sampler
        # has to do it here because the same value gets AND-combined
        # into both current and past slice predicates and a Jinja
        # template like ``region = '{{ params.region }}'`` would
        # otherwise reach Spark / pandas / JDBC unrendered.
        rendered_filter_expr = (
            context.render(self.filter_expr) if self.filter_expr else None
        )

        # Current slice. If slice_column is set, render slice_value
        # and emit ``slice_column = '<rendered>'``. Either way, AND
        # the dataset-level filter_expr.
        slice_pred: str | None = None
        anchor: datetime | None = None
        slice_value_was_date_only: bool = False
        if self.slice_column:
            rendered, anchor, slice_value_was_date_only = (
                self._render_slice_value(context)
            )
            slice_pred = _slice_predicate(self.slice_column, rendered)
        current_predicate = _and_combine(slice_pred, rendered_filter_expr)
        current_df = backend.sample_records(
            table, self.n_records, current_predicate
        )

        # Past slices.
        past_dfs: list[tuple[str, Any]] = []
        if self.history_filters:
            # Verbatim mode — filter_expr does NOT apply. Operator
            # owns the full WHERE clause.
            for i, filt in enumerate(self.history_filters, 1):
                rendered = context.render(filt)
                past_df = backend.sample_records(table, self.n_records, rendered)
                past_dfs.append((f"past_{i}", past_df))
        elif self.past_dates > 0:
            past_dfs = self._collect_anchor_history(
                backend, table, context, anchor,
                rendered_filter_expr,
                slice_value_was_date_only,
            )

        return [
            CollectionResult(
                metric_name="sample",
                metric_value=None,
                collected_at=now,
                metadata={
                    "table": table,
                    "collection_type": "sample",
                    "n_records": self.n_records,
                    "current_df": current_df,
                    "past_dfs": past_dfs,
                },
            )
        ]

    def _render_slice_value(
        self, context: QualifireContext
    ) -> tuple[str, datetime | None, bool]:
        """Render ``slice_value`` and parse it as a datetime.

        Returns ``(rendered_string, parsed_datetime_or_None,
        date_only)``. The parsed datetime is the anchor for past-slice
        math; the raw string is what we put in the current-slice SQL
        predicate. ``date_only`` records whether the operator's
        rendered value was a bare ISO date (no time component) — past
        slices then keep the same shape (``YYYY-MM-DD``) so DATE-typed
        columns compare cleanly without operator surprise.

        Quote stripping is symmetric — only matched same-character
        wrapping (``'x'`` or ``"x"``). Mismatched quotes
        (``'x"``) survive verbatim so the operator gets a clear
        downstream parse error instead of a silently-mangled value.
        """
        rendered = context.render(self.slice_value).strip()
        if (
            len(rendered) >= 2
            and rendered[0] == rendered[-1]
            and rendered[0] in "'\""
        ):
            rendered = rendered[1:-1]
        parsed = _parse_iso_datetime(rendered)
        # If the rendered text contains no time component, treat the
        # whole series as date-grain. Avoids mixing
        # ``slice_column = '2026-04-28'`` (current) with
        # ``slice_column = '2026-04-21'`` (past) when the column is a
        # TIMESTAMP that happens to anchor at midnight in the parsed
        # datetime — operator wrote a date, we keep date format.
        date_only = "T" not in rendered and " " not in rendered
        return rendered, parsed, date_only

    def _collect_anchor_history(
        self,
        backend: Backend,
        table: str,
        context: QualifireContext,
        anchor: datetime | None,
        rendered_filter_expr: str | None,
        date_only: bool,
    ) -> list[tuple[str, Any]]:
        """Compute past slices using ``slice_value − k·step``.

        Raises:
            MissingPartitionAnchorError: ``step`` is missing,
                ``slice_column`` / ``slice_value`` is missing, or
                ``slice_value`` doesn't render to an ISO datetime /
                date. Anchor mode is partition-deterministic
                end-to-end: without a shiftable predicate the past
                slices would all be undifferentiated draws from the
                whole table (the classifier would see AUC ≈ 0.5
                regardless of true drift). The engine catches this
                exception class and surfaces a structured ERROR with
                ``details.missing_partition_anchor=True``.
        """
        from qualifire.core.exceptions import MissingPartitionAnchorError

        if not self.step:
            raise MissingPartitionAnchorError(
                "SamplerCollector requires step (ISO 8601, e.g. 'P7D') "
                "to compute past partitions, or history_filters with "
                "explicit per-slice predicates. Set rule.model.step on "
                "shape/pattern checks."
            )
        if not self.slice_column:
            raise MissingPartitionAnchorError(
                "SamplerCollector anchor mode requires slice_column + "
                "slice_value (auto-generates <slice_column> = '<shifted>' "
                "per slice). Set both on the sample collection, or "
                "supply history_filters with explicit per-slice predicates."
            )
        if anchor is None:
            raise MissingPartitionAnchorError(
                "SamplerCollector cannot parse slice_value "
                f"{self.slice_value!r} as an ISO datetime / date. "
                "Shape/pattern history needs a single anchor; pass an "
                "ISO-shaped value (e.g. \"{{ ds }}\" or \"2026-04-28\") "
                "or use history_filters with explicit per-slice "
                "predicates."
            )
        step_td = parse_duration(self.step)

        past_dfs: list[tuple[str, Any]] = []
        for i in range(1, self.past_dates + 1):
            past_anchor = anchor - step_td * i
            past_filter: str | None = _slice_predicate(
                self.slice_column, _format_anchor(past_anchor, date_only)
            )
            past_filter = _and_combine(past_filter, rendered_filter_expr)
            past_df = backend.sample_records(
                table, self.n_records, past_filter
            )
            past_dfs.append((f"past_{i}", past_df))
        return past_dfs


def _slice_predicate(slice_column: str, value: str) -> str:
    """Render ``<slice_column> = '<value>'`` with single-quote escaping."""
    safe = value.replace("'", "''")
    return f"{slice_column} = '{safe}'"


def _format_anchor(anchor: datetime, date_only: bool) -> str:
    """Format a shifted anchor for the SQL predicate.

    The format must match the shape the operator's ``slice_value``
    rendered to, otherwise current and past predicates compare against
    DATE vs TIMESTAMP literals on the same column and the past slice
    silently returns zero rows. ``date_only=True`` (operator wrote a
    bare ISO date) emits ``YYYY-MM-DD``; ``False`` emits the full
    ISO timestamp.
    """
    if date_only:
        return anchor.date().isoformat()
    return anchor.isoformat()




def _parse_iso_datetime(value: str) -> datetime | None:
    """Parse ``value`` as an ISO datetime / date. Returns ``None``
    on failure — the sampler raises a structured error up the stack."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass
    try:
        d = date.fromisoformat(value)
        return datetime(d.year, d.month, d.day)
    except ValueError:
        return None

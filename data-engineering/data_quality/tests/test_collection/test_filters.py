"""Tests for shared filter composition: ``and_combine_filters``.

Two layers of coverage:

1. **Unit tests** for the helper itself — truth table, parenthesisation,
   empty / whitespace operand normalisation, double-parens preservation,
   and the sampler import-alias regression that pins
   ``qualifire.collection.sampler._and_combine`` to the shared
   ``and_combine_filters``.

2. **Pydantic coercion tests** for the three collector configs
   (``AggregationCollectionConfig``, ``ProfilingCollectionConfig``,
   ``MetricsCollectionConfig``) — empty / whitespace ``filter:`` values
   coerce to ``None`` at config load.

Phase 2 integration tests (engine call-site swap, rendered-empty
Jinja regression, caplog redaction) live in this same file.
The previous ``skip_recollection`` runtime-bypass tests have been
removed: the bypass was deleted in the skip-recollection feature
(filter no longer in natural key; data-presence semantics reign).
"""

from __future__ import annotations

import pytest

from qualifire.collection._filters import and_combine_filters
from qualifire.core.config import (
    AggregationCollectionConfig,
    MetricsCollectionConfig,
    ProfilingCollectionConfig,
)


# -----------------------------------------------------------------
# Truth table — None / set in every combination.
# -----------------------------------------------------------------


def test_both_none_returns_none():
    assert and_combine_filters(None, None) is None


def test_dataset_set_collector_none_returns_dataset_verbatim():
    assert and_combine_filters("region = 'US'", None) == "region = 'US'"


def test_dataset_none_collector_set_returns_collector_verbatim():
    assert and_combine_filters(None, "status = 'returned'") == "status = 'returned'"


def test_both_set_returns_parenthesised_and():
    out = and_combine_filters("region = 'US'", "status = 'returned'")
    assert out == "(region = 'US') AND (status = 'returned')"


# -----------------------------------------------------------------
# Operator precedence — top-level OR on either side must round-trip
# inside its own parentheses.
# -----------------------------------------------------------------


def test_or_on_each_side_parenthesisation():
    """``a OR b`` and ``c OR d`` must compose to
    ``(a OR b) AND (c OR d)``, not ``a OR b AND c OR d`` (which
    would change the truth value via SQL precedence)."""
    out = and_combine_filters("a OR b", "c OR d")
    assert out == "(a OR b) AND (c OR d)"


# -----------------------------------------------------------------
# Empty / whitespace-only operand normalisation. Both inputs are
# defensively normalised so a Jinja template that resolves to ""
# at runtime composes correctly (Pin 7).
# -----------------------------------------------------------------


@pytest.mark.parametrize("empty", ["", " ", "   ", "\t", "\n", "  \t  "])
def test_empty_or_whitespace_dataset_returns_collector_verbatim(empty):
    assert and_combine_filters(empty, "x = 1") == "x = 1"


@pytest.mark.parametrize("empty", ["", " ", "   ", "\t", "\n", "  \t  "])
def test_empty_or_whitespace_collector_returns_dataset_verbatim(empty):
    assert and_combine_filters("x = 1", empty) == "x = 1"


@pytest.mark.parametrize(
    "ds,co",
    [
        ("", ""),
        ("   ", "   "),
        ("\t", "\n"),
        (None, "  "),
        ("  ", None),
    ],
)
def test_both_empty_or_whitespace_returns_none(ds, co):
    assert and_combine_filters(ds, co) is None


# -----------------------------------------------------------------
# Double-parenthesisation preservation — Decision Pin 4 explicitly
# accepts the cost of double parens over SQL-fragment parsing.
# Pin literal output so a future "smart" stripping change must
# update this test deliberately.
# -----------------------------------------------------------------


def test_already_parenthesised_operand_gets_a_second_pair():
    out = and_combine_filters("(a)", "b")
    assert out == "((a)) AND (b)"


def test_both_already_parenthesised_get_their_own_pairs():
    out = and_combine_filters("(a OR b)", "(c)")
    assert out == "((a OR b)) AND ((c))"


# -----------------------------------------------------------------
# Whitespace inside non-empty operands is preserved verbatim — the
# normaliser only rejects ALL-whitespace strings, never strips
# operator-authored whitespace inside multi-clause filters.
# -----------------------------------------------------------------


def test_internal_whitespace_preserved_verbatim():
    out = and_combine_filters("  a   AND   b  ", "c")
    # Leading/trailing whitespace on the operand survives — the
    # helper normalises empty-OR-only-whitespace, not "trim".
    assert out == "(  a   AND   b  ) AND (c)"


# -----------------------------------------------------------------
# Sampler import-alias regression — pins Pin 6's "no fork"
# guarantee. The sampler's private ``_and_combine`` MUST be the
# very same callable as the shared ``and_combine_filters``;
# anything else means the helpers have diverged.
# -----------------------------------------------------------------


def test_sampler_alias_resolves_to_shared_helper():
    from qualifire.collection import _filters
    from qualifire.collection import sampler

    assert sampler._and_combine is _filters.and_combine_filters


# -----------------------------------------------------------------
# `_render_filter_once` skips Jinja render for `_RenderedSql` to
# avoid the double-render trap when a rendered output happens to
# contain literal ``{{ }}`` markers (Codex impl-review round 1
# HIGH finding).
# -----------------------------------------------------------------


def test_render_filter_once_skips_render_for_rendered_sql():
    from unittest.mock import MagicMock
    from qualifire.collection.aggregation import _render_filter_once
    from qualifire.collection._filters import and_combine_filters

    # Helper output is a `_RenderedSql` marker.
    pre = and_combine_filters("a = 1", None)
    mock_ctx = MagicMock()
    mock_ctx.render = MagicMock(return_value="SHOULD_NOT_BE_CALLED")

    out = _render_filter_once(pre, mock_ctx)

    assert out == "a = 1"
    mock_ctx.render.assert_not_called()


def test_render_filter_once_renders_plain_strings():
    """Symmetric test: a plain ``str`` (not ``_RenderedSql``)
    DOES go through ``context.render``."""
    from unittest.mock import MagicMock
    from qualifire.collection.aggregation import _render_filter_once

    mock_ctx = MagicMock()
    mock_ctx.render = MagicMock(return_value="rendered = 'x'")

    out = _render_filter_once("raw = '{{ x }}'", mock_ctx)

    assert out == "rendered = 'x'"
    mock_ctx.render.assert_called_once_with("raw = '{{ x }}'")


def test_render_filter_once_returns_none_for_none():
    from unittest.mock import MagicMock
    from qualifire.collection.aggregation import _render_filter_once

    mock_ctx = MagicMock()
    assert _render_filter_once(None, mock_ctx) is None
    mock_ctx.render.assert_not_called()


# -----------------------------------------------------------------
# Pydantic coercion — empty / whitespace ``filter:`` on the three
# collector configs lands as ``None`` on the model. Static-config
# counterpart to the runtime helper-side normalisation.
# -----------------------------------------------------------------


_CONFIG_CLASSES = [
    pytest.param(
        lambda **kw: AggregationCollectionConfig(
            expressions={"row_count": "COUNT(*)"}, **kw,
        ),
        id="aggregation",
    ),
    pytest.param(
        lambda **kw: ProfilingCollectionConfig(**kw),
        id="profiling",
    ),
    pytest.param(
        lambda **kw: MetricsCollectionConfig(
            metrics={"x": "COUNT(*)"}, **kw,
        ),
        id="metrics",
    ),
]


@pytest.mark.parametrize("make_cfg", _CONFIG_CLASSES)
@pytest.mark.parametrize("value", [None, "", "   ", "\t", "\n", "  \t  "])
def test_filter_empty_or_whitespace_coerces_to_none(make_cfg, value):
    cfg = make_cfg(filter=value)
    assert cfg.filter is None


@pytest.mark.parametrize("make_cfg", _CONFIG_CLASSES)
def test_filter_non_empty_passes_through_unchanged(make_cfg):
    cfg = make_cfg(filter="x = 1")
    assert cfg.filter == "x = 1"


@pytest.mark.parametrize("make_cfg", _CONFIG_CLASSES)
def test_filter_with_internal_whitespace_passes_through_verbatim(make_cfg):
    """The normaliser rejects all-whitespace strings only, never
    trims operator-authored whitespace. ``"  x = 1  "`` must
    survive verbatim because some operators format their filters
    for readability."""
    cfg = make_cfg(filter="  x = 1  ")
    assert cfg.filter == "  x = 1  "


# =================================================================
# Integration tests — engine drives the three filtered collectors
# end-to-end via PandasBackend. These exercise the actual
# behaviour change from override to AND-combine. The previous
# ``skip_recollection`` runtime-bypass tests were removed when
# the bypass itself was deleted (skip-recollection feature).
# =================================================================


import logging  # noqa: E402

import pandas as pd  # noqa: E402

from qualifire.core.config import (  # noqa: E402
    DatasetConfig,
    HistoricalCompareConfig,
    HistoricalRuleConfig,
    HistoricalThresholds,
    HistoricalValidationConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.context import QualifireContext  # noqa: E402
from qualifire.core.engine import QualifireEngine  # noqa: E402
from qualifire.core.exceptions import QualifireValidationError  # noqa: E402
from qualifire.core.models import Severity  # noqa: E402
from tests._e2e_support.backends import (  # noqa: E402
    E2EPandasBackend,
    _make_storage,
)


@pytest.fixture
def filter_combo_df():
    """Dataset for filter-composition tests.

    Rows split across two regions (``us`` / ``de``) and two
    statuses (``completed`` / ``returned``). Total 12 rows;
    filter combinations select known sub-counts so an AND-scope
    versus override-scope row count diff is unambiguous:

      * region='US' only            → 6 rows
      * status='completed' only     → 6 rows
      * region='US' AND completed   → 3 rows
      * (override of dataset by
         collector) status='completed'
         disregarding region        → 6 rows  (pre-fix value)
    """
    rows = []
    for i, region in enumerate(["us"] * 6 + ["de"] * 6):
        rows.append({
            "sale_id": f"r{i}",
            "region": region,
            "status": "completed" if i % 2 == 0 else "returned",
            "amount": 10.0 + i,
            "sale_date": "2024-01-01",
            "updated_at": "2024-01-01 00:00:00",
        })
    return pd.DataFrame(rows)


def _run_thresh(backend, storage, ds_filter, co_filter, *,
                metric_name="row_count", expression="COUNT(*)"):
    """Build a single-dataset QualifireConfig with one threshold
    aggregation, run it, return the collected metric value.
    """
    ds = DatasetConfig(
        name="filter_combo",
        table="sales",
        filter=ds_filter,
        validations=[ThresholdValidationConfig(
            collection=AggregationCollectionConfig(
                expressions={metric_name: expression},
                filter=co_filter,
            ),
            rules=[ThresholdRuleConfig(
                metric=metric_name,
                thresholds=ThresholdLevels(
                    warning={"min": 0}, error={"min": 0},
                ),
            )],
        )],
    )
    cfg = QualifireConfig(
        owner="t", bu="t", system_table="x", datasets=[ds],
    )
    engine = QualifireEngine(
        backend=backend, storage=storage,
        context=QualifireContext(run_id="r1"), config=cfg,
    )
    try:
        result = engine.run()
    except QualifireValidationError as e:
        result = e.result
    # Single dataset, single rule → one metric persisted.
    [vr] = [
        v for ds in result.datasets for v in ds.validation_results
        if v.metric_name == metric_name
    ]
    return vr.actual_value


# -----------------------------------------------------------------
# Helpers for the parametrized matrix below.
# -----------------------------------------------------------------


def _build_count_validation(collection_kind, ds_filter, co_filter):
    """Construct a (DatasetConfig, metric_name) pair for a
    threshold-on-row_count validation across the three filtered
    collector types.

    Each path computes the same logical metric — total row count
    in the resolved scope — but via the collector-specific
    machinery so the matrix really exercises three separate
    engine call sites (engine.py:1229, 1239, 1259).
    """
    if collection_kind == "aggregation":
        return DatasetConfig(
            name="filter_combo", table="sales",
            filter=ds_filter,
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"},
                    filter=co_filter,
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        ), "row_count"
    if collection_kind == "metrics":
        return DatasetConfig(
            name="filter_combo", table="sales",
            filter=ds_filter,
            validations=[ThresholdValidationConfig(
                collection=MetricsCollectionConfig(
                    metrics={"row_count": "COUNT(*)"},
                    filter=co_filter,
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        ), "row_count"
    if collection_kind == "profiling":
        # Profiling exposes a count-shaped metric via the
        # profiled column's ``count`` stat. Pick ``amount`` —
        # always non-null in the fixture so count == row count.
        return DatasetConfig(
            name="filter_combo", table="sales",
            filter=ds_filter,
            validations=[ThresholdValidationConfig(
                collection=ProfilingCollectionConfig(
                    columns=["amount"],
                    filter=co_filter,
                    exclude_stats=[
                        "top_k", "skewness", "kurtosis", "variance",
                    ],
                    top_k=0,
                ),
                rules=[ThresholdRuleConfig(
                    metric="amount.count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        ), "amount.count"
    raise ValueError(f"unknown collection kind {collection_kind!r}")


def _run_count(backend, ds, metric_name, *, extra_context=None):
    """Run a single-dataset config and return the persisted
    metric value. Common runner used across the parametrized
    matrix below.
    """
    cfg = QualifireConfig(
        owner="t", bu="t", system_table="x", datasets=[ds],
    )
    ctx = QualifireContext(
        run_id="r1", extra_context=extra_context or {},
    )
    engine = QualifireEngine(
        backend=backend, storage=_make_storage(),
        context=ctx, config=cfg,
    )
    try:
        result = engine.run()
    except QualifireValidationError as e:
        result = e.result
    [vr] = [
        v for d in result.datasets for v in d.validation_results
        if v.metric_name == metric_name
    ]
    return vr.actual_value


# -----------------------------------------------------------------
# Three collector paths × three filter combos. The key regression
# is the "both filters set" case — pre-2026-05-09 the collector
# filter overrode the dataset filter; post-fix they AND-combine.
# -----------------------------------------------------------------


@pytest.mark.parametrize("kind", ["aggregation", "metrics", "profiling"])
class TestEngineFilterMatrixParametrized:
    """Three collector paths × three filter combos parametrized
    over collector kind. Replaces the per-kind one-offs.
    """

    def test_dataset_only(self, kind, filter_combo_df):
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds, metric = _build_count_validation(
            kind, ds_filter="region = 'us'", co_filter=None,
        )
        assert _run_count(backend, ds, metric) == 6

    def test_collector_only(self, kind, filter_combo_df):
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds, metric = _build_count_validation(
            kind, ds_filter=None, co_filter="status = 'completed'",
        )
        assert _run_count(backend, ds, metric) == 6

    def test_both_filters_and_scope(self, kind, filter_combo_df):
        """KEY REGRESSION across all three collector paths."""
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds, metric = _build_count_validation(
            kind,
            ds_filter="region = 'us'",
            co_filter="status = 'completed'",
        )
        n = _run_count(backend, ds, metric)
        assert n == 3, (
            f"[{kind}] AND-scope expected 3 (us AND completed); got {n}. "
            "A value of 6 means the collector filter overrode the "
            "dataset filter — pre-fix behaviour."
        )

    def test_collector_jinja_renders_to_empty(self, kind, filter_combo_df):
        """Rendered-empty Jinja regression across all three
        collector paths. Pre-Pin-7 compose would produce
        ``(region = 'us') AND ()`` (invalid SQL); post-fix the
        empty side normalises to ``None`` and the combined
        scope equals the dataset filter alone."""
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds, metric = _build_count_validation(
            kind,
            ds_filter="region = 'us'",
            co_filter="{{ optional_predicate }}",
        )
        n = _run_count(
            backend, ds, metric,
            extra_context={"optional_predicate": ""},
        )
        assert n == 6  # us rows only; collector scope is empty

    def test_dataset_jinja_renders_to_whitespace(self, kind, filter_combo_df):
        """Symmetric: dataset Jinja resolving to whitespace-only."""
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds, metric = _build_count_validation(
            kind,
            ds_filter="{{ optional_guard }}",
            co_filter="status = 'completed'",
        )
        n = _run_count(
            backend, ds, metric,
            extra_context={"optional_guard": "   "},
        )
        assert n == 6  # completed rows only


class TestEngineAndCombineScope:
    def test_dataset_filter_only_aggregation(self, filter_combo_df):
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        n = _run_thresh(backend, _make_storage(),
                        ds_filter="region = 'us'", co_filter=None)
        assert n == 6  # 6 us rows

    def test_collector_filter_only_aggregation(self, filter_combo_df):
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        n = _run_thresh(backend, _make_storage(),
                        ds_filter=None, co_filter="status = 'completed'")
        assert n == 6  # 6 completed rows

    def test_both_filters_aggregation_and_scope(self, filter_combo_df):
        """KEY REGRESSION — proves the override→AND switch.

        Pre-fix: collector filter would override dataset filter,
        producing 6 (status='completed' globally).
        Post-fix: filters AND-combine, producing 3 (US AND completed).
        """
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        n = _run_thresh(backend, _make_storage(),
                        ds_filter="region = 'us'",
                        co_filter="status = 'completed'")
        assert n == 3, (
            "AND-scope expected 3 (us AND completed); a value of 6 means "
            "the collector filter overrode the dataset filter — pre-fix "
            "behaviour."
        )

    def test_both_filters_metrics_and_scope(self, filter_combo_df):
        """Same regression on the ``metrics`` collector path."""
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds = DatasetConfig(
            name="filter_combo", table="sales",
            filter="region = 'us'",
            validations=[ThresholdValidationConfig(
                collection=MetricsCollectionConfig(
                    metrics={"row_count": "COUNT(*)"},
                    filter="status = 'completed'",
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="t", bu="t", system_table="x", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=_make_storage(),
            context=QualifireContext(run_id="r1"), config=cfg,
        )
        try:
            result = engine.run()
        except QualifireValidationError as e:
            result = e.result
        [vr] = [
            v for d in result.datasets for v in d.validation_results
            if v.metric_name == "row_count"
        ]
        assert vr.actual_value == 3

    def test_both_filters_profiling_and_scope(self, filter_combo_df):
        """Same regression on the ``profiling`` collector path.

        Asserts ``amount.mean`` because that metric DIFFERS
        between the override and AND scopes:
          * override (collector wins): mean of all 6 completed
            rows = mean(10,12,14,16,18,20) = 15.0
          * AND-combine: mean of 3 us-completed rows
            = mean(10,12,14) = 12.0
        ``status.approx_distinct`` would be 1 in both scopes
        and is therefore tautological — ``amount.mean`` is the
        tightest discriminator.
        """
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds = DatasetConfig(
            name="filter_combo", table="sales",
            filter="region = 'us'",
            validations=[ThresholdValidationConfig(
                collection=ProfilingCollectionConfig(
                    columns=["amount"],
                    filter="status = 'completed'",
                    exclude_stats=["top_k", "skewness", "kurtosis", "variance"],
                    top_k=0,
                ),
                rules=[ThresholdRuleConfig(
                    metric="amount.mean",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="t", bu="t", system_table="x", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=_make_storage(),
            context=QualifireContext(run_id="r1"), config=cfg,
        )
        try:
            result = engine.run()
        except QualifireValidationError as e:
            result = e.result
        [vr] = [
            v for d in result.datasets for v in d.validation_results
            if v.metric_name == "amount.mean"
        ]
        # AND-scope (us AND completed) → mean(10, 12, 14) = 12.0.
        # Override scope (completed only) would give 15.0 —
        # asserting 12.0 specifically catches the difference.
        assert vr.actual_value == 12.0, (
            f"AND-scope expected mean of 12.0 (us AND completed); "
            f"got {vr.actual_value} (15.0 means the override path "
            "is still active)."
        )


# -----------------------------------------------------------------
# Jinja-rendered filters on each side. The render happens in the
# engine *before* combine (Pin 7); the collector's internal render
# pass is idempotent on the resolved string.
# -----------------------------------------------------------------


class TestJinjaRenderingThenCombine:
    def test_jinja_each_side(self, filter_combo_df):
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds = DatasetConfig(
            name="filter_combo", table="sales",
            filter="region = '{{ tenant }}'",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"},
                    filter="status = '{{ status }}'",
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="t", bu="t", system_table="x", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=_make_storage(),
            context=QualifireContext(
                run_id="r1",
                extra_context={"tenant": "us", "status": "completed"},
            ),
            config=cfg,
        )
        try:
            result = engine.run()
        except QualifireValidationError as e:
            result = e.result
        [vr] = [
            v for d in result.datasets for v in d.validation_results
            if v.metric_name == "row_count"
        ]
        # Jinja-rendered then AND-combined → us AND completed → 3.
        assert vr.actual_value == 3

    def test_collector_jinja_renders_to_empty_does_not_break_sql(self, filter_combo_df):
        """KEY REGRESSION (Pin 7) — collector filter is a Jinja
        template that resolves to ``""`` at runtime. Pre-Pin-7
        compose would produce ``(region = 'us') AND ()`` (invalid
        SQL); post-fix the empty side normalises to ``None`` so
        the combined filter equals the dataset filter alone."""
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds = DatasetConfig(
            name="filter_combo", table="sales",
            filter="region = 'us'",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"},
                    filter="{{ optional_predicate }}",
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="t", bu="t", system_table="x", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=_make_storage(),
            context=QualifireContext(
                run_id="r1",
                extra_context={"optional_predicate": ""},
            ),
            config=cfg,
        )
        try:
            result = engine.run()
        except QualifireValidationError as e:
            result = e.result
        [vr] = [
            v for d in result.datasets for v in d.validation_results
            if v.metric_name == "row_count"
        ]
        # Collector template rendered to "" — composed scope is
        # the dataset filter alone (6 us rows). No SQL parse error.
        assert vr.actual_value == 6

    def test_dataset_jinja_renders_to_empty_does_not_break_sql(self, filter_combo_df):
        """Symmetric to the previous case — empty side flipped."""
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        ds = DatasetConfig(
            name="filter_combo", table="sales",
            filter="{{ optional_guard }}",
            validations=[ThresholdValidationConfig(
                collection=AggregationCollectionConfig(
                    expressions={"row_count": "COUNT(*)"},
                    filter="status = 'completed'",
                ),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(
                        warning={"min": 0}, error={"min": 0},
                    ),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="t", bu="t", system_table="x", datasets=[ds],
        )
        engine = QualifireEngine(
            backend=backend, storage=_make_storage(),
            context=QualifireContext(
                run_id="r1",
                extra_context={"optional_guard": "   "},  # whitespace
            ),
            config=cfg,
        )
        try:
            result = engine.run()
        except QualifireValidationError as e:
            result = e.result
        [vr] = [
            v for d in result.datasets for v in d.validation_results
            if v.metric_name == "row_count"
        ]
        # Dataset template rendered to "   " — collector scope alone (6 completed).
        assert vr.actual_value == 6


# -----------------------------------------------------------------
# DEBUG log redaction (Pin 3). caplog asserts no filter literal
# leaks into the engine's compose-log line.
# -----------------------------------------------------------------


class TestLoggingRedaction:
    def test_debug_log_does_not_leak_filter_strings(self, filter_combo_df, caplog):
        backend = E2EPandasBackend(tables={"sales": filter_combo_df})
        with caplog.at_level(logging.DEBUG, logger="qualifire.core.engine"):
            _run_thresh(
                backend, _make_storage(),
                ds_filter="region = 'us'",
                co_filter="status = 'completed'",
            )
        # Find at least one compose-log record so we know the
        # logging path actually fired.
        compose_records = [
            r for r in caplog.records
            if "filter compose" in r.getMessage()
        ]
        assert compose_records, (
            "Expected at least one 'filter compose' DEBUG log record."
        )
        # Verify NONE of those records leak the filter literal SQL.
        for r in compose_records:
            msg = r.getMessage()
            assert "region = 'us'" not in msg, (
                f"Filter literal leaked into DEBUG log: {msg!r}"
            )
            assert "status = 'completed'" not in msg, (
                f"Filter literal leaked into DEBUG log: {msg!r}"
            )
            # Presence flags + dataset name SHOULD be there.
            assert "filter_combo" in msg
            assert "ds_filter_present=True" in msg
            assert "co_filter_present=True" in msg



"""Tests for ``qualifire.core.selectors``.

Covers the selector grammar parser and the
``ScopedDataset`` resolver: literal vs wildcard segments, multi-
selector dedup, edge cases (empty / whitespace / no-match / trailing
or empty middle colon), and metric-filter narrowing across
overlapping selectors.
"""

from __future__ import annotations

import pytest

from qualifire.core.config import (
    AggregationCollectionConfig,
    DatasetConfig,
    QualifireConfig,
    ThresholdLevels,
    ThresholdRuleConfig,
    ThresholdValidationConfig,
)
from qualifire.core.exceptions import QualifireConfigError
from qualifire.core.selectors import (
    SelectorPart,
    parse_selector,
    resolve_selector,
)


def _validation(name: str, metric: str = "cnt") -> ThresholdValidationConfig:
    return ThresholdValidationConfig(
        name=name,
        collection=AggregationCollectionConfig(expressions={metric: "COUNT(*)"}),
        rules=[ThresholdRuleConfig(metric=metric, thresholds=ThresholdLevels())],
    )


def _config_with(datasets: list[DatasetConfig]) -> QualifireConfig:
    return QualifireConfig(owner="o", bu="b", system_table="s", datasets=datasets)


# ------------------------- parse_selector ----------------------------


class TestParseSelector:
    def test_two_segment_with_wildcard_validation(self):
        [p] = parse_selector("sales:*")
        assert p == SelectorPart(dataset="sales", validation="*", metric=None)

    def test_three_segment(self):
        [p] = parse_selector("sales:row_count_check:row_count")
        assert p == SelectorPart(
            dataset="sales", validation="row_count_check", metric="row_count"
        )

    def test_wildcards_each_position(self):
        [p] = parse_selector("*:row_count_check")
        assert p == SelectorPart(dataset="*", validation="row_count_check", metric=None)
        [p] = parse_selector("*:*:revenue")
        assert p == SelectorPart(dataset="*", validation="*", metric="revenue")

    def test_multi_selector_comma_separated(self):
        ps = parse_selector("sales:*, finance:row_count_check")
        assert len(ps) == 2
        assert ps[0].dataset == "sales"
        assert ps[1].dataset == "finance"

    def test_whitespace_tolerance(self):
        ps = parse_selector("  sales:*  ,   finance:*  ")
        assert [p.dataset for p in ps] == ["sales", "finance"]

    def test_empty_selector_raises(self):
        with pytest.raises(QualifireConfigError, match="cannot be empty"):
            parse_selector("")

    def test_whitespace_only_raises(self):
        with pytest.raises(QualifireConfigError, match="cannot be empty"):
            parse_selector("   ")

    def test_one_segment_raises(self):
        with pytest.raises(QualifireConfigError, match="missing the validation segment"):
            parse_selector("sales")

    def test_too_many_segments_raises(self):
        with pytest.raises(QualifireConfigError, match="too many segments"):
            parse_selector("a:b:c:d")

    def test_trailing_colon_raises(self):
        with pytest.raises(QualifireConfigError, match="validation segment may not be empty"):
            parse_selector("sales:")

    def test_empty_middle_segment_raises(self):
        with pytest.raises(QualifireConfigError, match="validation segment may not be empty"):
            parse_selector("sales::row_count")

    def test_empty_metric_segment_raises(self):
        with pytest.raises(QualifireConfigError, match="metric segment may not be empty"):
            parse_selector("sales:row_count_check:")

    def test_empty_dataset_segment_raises(self):
        with pytest.raises(QualifireConfigError, match="dataset segment may not be empty"):
            parse_selector(":row_count_check")

    def test_empty_entry_in_comma_list_raises(self):
        with pytest.raises(QualifireConfigError, match="entry cannot be empty"):
            parse_selector("sales:*,,finance:*")


# ------------------------- resolve_selector --------------------------


class TestResolveSelector:
    def test_dataset_wildcard_validation_literal(self):
        ds_a = DatasetConfig(name="sales", table="t", validations=[
            _validation("row_count_check"),
        ])
        ds_b = DatasetConfig(name="finance", table="t2", validations=[
            _validation("row_count_check"),
            _validation("amount_check"),
        ])
        config = _config_with([ds_a, ds_b])
        scopes = resolve_selector(parse_selector("*:row_count_check"), config)

        # both datasets matched, each with one validation
        assert {s.dataset.name for s in scopes} == {"sales", "finance"}
        for scope in scopes:
            assert len(scope.validations) == 1
            assert scope.validations[0].name == "row_count_check"

    def test_dataset_literal_validation_wildcard(self):
        ds = DatasetConfig(name="sales", table="t", validations=[
            _validation("row_count_check"),
            _validation("amount_check"),
        ])
        config = _config_with([ds])
        [scope] = resolve_selector(parse_selector("sales:*"), config)
        assert scope.dataset.name == "sales"
        assert {v.name for v in scope.validations} == {
            "row_count_check", "amount_check",
        }
        # wildcard validation segment ⇒ no metric filter
        assert scope.metric_filter is None

    def test_metric_filter_narrowed_to_literal(self):
        ds = DatasetConfig(name="sales", table="t", validations=[
            _validation("threshold_check_a", metric="cnt"),
        ])
        config = _config_with([ds])
        [scope] = resolve_selector(
            parse_selector("sales:threshold_check_a:cnt"), config
        )
        assert scope.metric_filter == {"cnt"}

    def test_metric_wildcard_resolves_to_no_filter(self):
        ds = DatasetConfig(name="sales", table="t", validations=[
            _validation("threshold_check_a"),
        ])
        config = _config_with([ds])
        [scope] = resolve_selector(
            parse_selector("sales:threshold_check_a:*"), config
        )
        assert scope.metric_filter is None

    def test_overlapping_selectors_dedup_by_validation_id(self):
        ds = DatasetConfig(name="sales", table="t", validations=[
            _validation("a"),
            _validation("b"),
        ])
        config = _config_with([ds])
        [scope] = resolve_selector(
            parse_selector("sales:*, sales:a"), config
        )
        assert {v.name for v in scope.validations} == {"a", "b"}

    def test_metric_filter_union_when_no_wildcard(self):
        ds = DatasetConfig(name="sales", table="t", validations=[
            _validation("a", metric="x"),
            _validation("b", metric="y"),
        ])
        config = _config_with([ds])
        [scope] = resolve_selector(
            parse_selector("sales:a:x, sales:b:y"), config
        )
        assert scope.metric_filter == {"x", "y"}

    def test_metric_filter_collapses_to_none_with_any_wildcard(self):
        ds = DatasetConfig(name="sales", table="t", validations=[
            _validation("a", metric="x"),
        ])
        config = _config_with([ds])
        # mix of literal-metric and no-metric => no-metric wins
        [scope] = resolve_selector(
            parse_selector("sales:a:x, sales:a"), config
        )
        assert scope.metric_filter is None

    def test_no_match_dataset_raises(self):
        ds = DatasetConfig(name="sales", table="t", validations=[_validation("a")])
        config = _config_with([ds])
        with pytest.raises(QualifireConfigError, match="matched no scope"):
            resolve_selector(parse_selector("nonexistent:*"), config)

    def test_no_match_validation_raises(self):
        ds = DatasetConfig(name="sales", table="t", validations=[_validation("a")])
        config = _config_with([ds])
        with pytest.raises(QualifireConfigError, match="matched no scope"):
            resolve_selector(parse_selector("sales:nonexistent"), config)

    def test_empty_parts_raises(self):
        ds = DatasetConfig(name="sales", table="t", validations=[_validation("a")])
        config = _config_with([ds])
        with pytest.raises(QualifireConfigError, match="parts list is empty"):
            resolve_selector([], config)

    def test_three_part_full_wildcard(self):
        ds_a = DatasetConfig(name="sales", table="t", validations=[
            _validation("a", metric="x"),
        ])
        ds_b = DatasetConfig(name="finance", table="t2", validations=[
            _validation("b", metric="x"),
        ])
        config = _config_with([ds_a, ds_b])
        scopes = resolve_selector(parse_selector("*:*:x"), config)
        assert len(scopes) == 2
        assert all(s.metric_filter == {"x"} for s in scopes)

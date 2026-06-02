"""Tests for sub-feature A — dict-form ``expressions:``.

Pin the new contract:
- ``AggregationCollectionConfig.expressions`` is a ``dict[str, str]``.
- List-form raises a precise migration error.
- Empty dict is rejected.
- The collector wraps each value as ``(<expr>) AS <key>`` and emits
  one ``CollectionResult`` per dict entry.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from qualifire.collection.aggregation import AggregationCollector
from qualifire.core.config import AggregationCollectionConfig
from qualifire.core.context import QualifireContext


class TestExpressionsField:
    def test_dict_form_round_trip(self):
        c = AggregationCollectionConfig(expressions={"cnt": "COUNT(*)"})
        assert c.expressions == {"cnt": "COUNT(*)"}

    def test_list_form_rejected(self):
        with pytest.raises(ValidationError, match="dict\\[str, str\\]"):
            AggregationCollectionConfig(
                expressions=["COUNT(*) AS cnt"],  # type: ignore[arg-type]
            )

    def test_empty_dict_rejected(self):
        with pytest.raises(ValidationError, match="must not be empty"):
            AggregationCollectionConfig(expressions={})

    def test_yaml_dict_keys_become_metric_names(self):
        c = AggregationCollectionConfig(
            expressions={
                "row_count": "COUNT(*)",
                "avg_amount": "AVG(amount)",
            }
        )
        assert set(c.expressions.keys()) == {"row_count", "avg_amount"}


class TestAggregationCollectorDict:
    def test_collector_renders_dict_form(self):
        from unittest.mock import MagicMock

        backend = MagicMock()
        backend.get_aggregations.return_value = {
            "cnt": 42, "avg_x": 3.5,
        }
        collector = AggregationCollector(
            expressions={"cnt": "COUNT(*)", "avg_x": "AVG(x)"},
        )
        ctx = QualifireContext()
        results = collector.collect(backend, "t", ctx)
        # One result per dict entry.
        assert len(results) == 2
        names = {r.metric_name for r in results}
        assert names == {"cnt", "avg_x"}

    def test_select_clause_wraps_expr_with_alias(self):
        ctx = QualifireContext()
        collector = AggregationCollector(
            expressions={"cnt": "COUNT(*)"},
        )
        rendered = collector._build_select_exprs(ctx)
        assert rendered == ["(COUNT(*)) AS cnt"]

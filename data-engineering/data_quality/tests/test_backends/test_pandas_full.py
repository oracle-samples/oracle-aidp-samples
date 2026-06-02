"""Tests for PandasBackend SQL execution and aggregations (requires pandasql)."""

import pandas as pd
import pytest

from qualifire.backends.pandas_backend import PandasBackend


@pytest.fixture
def backend():
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0],
        "category": ["A", "B", "A", "B", "C"],
    })
    return PandasBackend(tables={"sales": df})


class TestPandasBackendSQL:
    def test_execute_sql(self, backend):
        result = backend.execute_sql("SELECT COUNT(*) AS cnt FROM sales")
        assert result.iloc[0]["cnt"] == 5

    def test_execute_sql_with_filter(self, backend):
        result = backend.execute_sql("SELECT COUNT(*) AS cnt FROM sales WHERE category = 'A'")
        assert result.iloc[0]["cnt"] == 2

    def test_get_aggregations(self, backend):
        result = backend.get_aggregations(
            "sales",
            ["AVG(amount) AS avg_amt", "COUNT(*) AS cnt"],
        )
        assert result["cnt"] == 5
        assert result["avg_amt"] == 30.0

    def test_get_aggregations_with_filter(self, backend):
        result = backend.get_aggregations(
            "sales",
            ["SUM(amount) AS total"],
            "category = 'B'",
        )
        assert result["total"] == 60.0

    def test_sample_records_with_filter(self, backend):
        sample = backend.sample_records("sales", 10, "category == 'A'")
        assert len(sample) == 2
        assert all(sample["category"] == "A")

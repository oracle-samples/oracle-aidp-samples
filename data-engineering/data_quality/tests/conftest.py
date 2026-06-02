"""Shared test fixtures with mocked SparkSession."""

from __future__ import annotations

import os
import socket
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest


# PySpark's driver tries to bind to a port on ``hostname`` during
# ``SparkSession.builder.getOrCreate()``. On machines where the system
# hostname doesn't resolve (common on macOS where ``/etc/hosts`` doesn't
# include the box's hostname, and in many CI images), that bind fails
# with ``java.nio.channels.UnresolvedAddressException`` and the JVM-side
# SparkContext is left half-initialized. Every downstream test that
# touches Spark then inherits that broken context and fails even though
# the code under test is fine.
#
# Setting ``SPARK_LOCAL_IP`` pins the driver to a resolvable address so
# startup doesn't depend on hostname resolution. We only set the default
# when the operator hasn't already chosen an IP — respecting any
# intentional CI/runtime override.
if "SPARK_LOCAL_IP" not in os.environ:
    try:
        socket.gethostbyname(socket.gethostname())
    except OSError:
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"


class MockRow:
    """Simulates a Spark Row object."""

    def __init__(self, data: dict):
        self._data = data
        for k, v in data.items():
            setattr(self, k, v)

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self._data.values())[key]
        return self._data[key]

    def asDict(self):
        return dict(self._data)


class MockDataFrame:
    """Simulates a Spark DataFrame."""

    def __init__(self, rows: list[dict] | None = None, schema_fields: list | None = None):
        self._rows = rows or []
        self._schema_fields = schema_fields or []

    def collect(self):
        return [MockRow(r) for r in self._rows]

    def count(self):
        return len(self._rows)

    @property
    def columns(self):
        if self._rows:
            return list(self._rows[0].keys())
        return [f.name for f in self._schema_fields] if self._schema_fields else []

    @property
    def schema(self):
        mock_schema = MagicMock()
        mock_schema.fields = self._schema_fields
        return mock_schema

    def createOrReplaceTempView(self, name: str) -> None:
        pass

    def orderBy(self, *args, **kwargs):
        return self

    def limit(self, n):
        return MockDataFrame(self._rows[:n], self._schema_fields)

    # ---- minimal Spark DataFrame ops ---------------------------------
    # The K-storage-parity rewrites use Spark-side window functions
    # for soft-delete dedup on JDBC. Tests that mock the JDBC reader
    # (test_spark_storages.py::TestJDBCStorage) drive their fake rows
    # through the same pipeline as production. We don't faithfully
    # simulate window math here — the new operations are no-ops that
    # let pre-deduped fixture rows pass through unchanged. Tests that
    # need real window/filter semantics use the real-Spark fixtures
    # in test_jdbc_bucketing_real.py and test_external_catalog_real.py.
    def withColumn(self, _col_name, _expr):
        return self

    def drop(self, *_cols):
        return self

    def filter(self, _expr):
        return self

    def select(self, *_cols):
        return self

    def distinct(self):
        return self

    def toPandas(self):
        import pandas as pd

        return pd.DataFrame(self._rows)

    @property
    def write(self):
        writer = MagicMock()
        writer.mode.return_value = writer
        writer.partitionBy.return_value = writer
        writer.option.return_value = writer
        return writer


@pytest.fixture
def mock_spark():
    """Mock SparkSession with basic SQL support."""
    spark = MagicMock()
    spark.sql.return_value = MockDataFrame()
    spark.table.return_value = MockDataFrame()
    spark.createDataFrame.return_value = MockDataFrame()
    return spark


@pytest.fixture
def sample_datetime():
    """A fixed datetime for deterministic tests."""
    return datetime(2024, 6, 15, 10, 30, 0)


@pytest.fixture
def recent_datetime():
    """A datetime 2 hours ago from a fixed reference."""
    return datetime(2024, 6, 15, 8, 30, 0)

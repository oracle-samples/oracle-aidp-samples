"""Unit tests for :func:`bronze_probe.bronze_table_absent` (strict, fail-closed).

The detector must return ``True`` ONLY for a recognized table/view-not-found
condition and **re-raise** every other Spark failure (permission, catalog
unreachable, connector) so a transient outage is never misclassified as a
fresh tenant. Spark is mocked with a ``MagicMock`` whose ``sql()`` either
returns rows or raises a fake exception.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    UnsafeIdentifierError,
    _is_table_or_view_not_found,
    bronze_table_absent,
)


class _StructuredAnalysisException(Exception):  # noqa: N818 — mirrors Spark's `AnalysisException` name
    """Duck-types Spark 3.4+ AnalysisException.getErrorClass()."""

    def __init__(self, error_class: str, message: str = "") -> None:
        super().__init__(message or error_class)
        self._error_class = error_class

    def getErrorClass(self) -> str:  # noqa: N802 — match Spark's API name
        return self._error_class


def _spark_raising(exc: Exception) -> MagicMock:
    spark = MagicMock(name="spark")

    def _sql(_query: str):
        raise exc

    spark.sql.side_effect = _sql
    return spark


def _spark_ok() -> MagicMock:
    spark = MagicMock(name="spark")
    df = MagicMock(name="df")
    df.take.return_value = [object()]
    spark.sql.return_value = df
    return spark


class TestClassifier:
    def test_structured_error_class(self) -> None:
        assert _is_table_or_view_not_found(
            _StructuredAnalysisException("TABLE_OR_VIEW_NOT_FOUND")
        )

    def test_bracketed_message_prefix(self) -> None:
        assert _is_table_or_view_not_found(
            Exception("[TABLE_OR_VIEW_NOT_FOUND] The table `x` cannot be found")
        )

    def test_legacy_phrase(self) -> None:
        assert _is_table_or_view_not_found(Exception("Table or view not found: x"))

    def test_permission_error_not_classified(self) -> None:
        assert not _is_table_or_view_not_found(
            _StructuredAnalysisException("INSUFFICIENT_PERMISSIONS", "permission denied")
        )

    def test_generic_error_not_classified(self) -> None:
        assert not _is_table_or_view_not_found(Exception("Connection refused"))


class TestBronzeTableAbsent:
    def test_present_table_returns_false(self) -> None:
        assert bronze_table_absent(
            _spark_ok(), catalog="cat", bronze_schema="bronze", table="ap_invoices"
        ) is False

    def test_missing_table_returns_true(self) -> None:
        spark = _spark_raising(
            _StructuredAnalysisException("TABLE_OR_VIEW_NOT_FOUND")
        )
        assert bronze_table_absent(
            spark, catalog="cat", bronze_schema="bronze", table="ap_invoices"
        ) is True

    def test_permission_error_reraises(self) -> None:
        boom = _StructuredAnalysisException("INSUFFICIENT_PERMISSIONS", "denied")
        spark = _spark_raising(boom)
        with pytest.raises(_StructuredAnalysisException):
            bronze_table_absent(
                spark, catalog="cat", bronze_schema="bronze", table="ap_invoices"
            )

    def test_catalog_unreachable_reraises(self) -> None:
        spark = _spark_raising(RuntimeError("metastore connection timed out"))
        with pytest.raises(RuntimeError):
            bronze_table_absent(
                spark, catalog="cat", bronze_schema="bronze", table="ap_invoices"
            )

    def test_unsafe_identifier_fails_closed_before_spark(self) -> None:
        spark = MagicMock(name="spark")
        with pytest.raises(UnsafeIdentifierError):
            bronze_table_absent(
                spark, catalog="cat", bronze_schema="bronze", table="bad;DROP"
            )
        spark.sql.assert_not_called()

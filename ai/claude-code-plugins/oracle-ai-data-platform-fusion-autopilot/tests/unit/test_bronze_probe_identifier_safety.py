"""Regression tests for the bronze-probe SQL-identifier hardening
(round-1 review, should-fix #2).

The probe interpolates ``catalog``, ``bronze_schema``, and each
``dataset_id`` directly into the DESCRIBE TABLE statement via an
f-string. Without validation, a malformed bundle could change the SQL
text or inject additional statements. The fix validates every
identifier against a strict allowlist BEFORE any Spark call.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    UnsafeIdentifierError,
    describe_bronze,
)


def _mock_spark() -> MagicMock:
    spark = MagicMock(name="spark")
    df = MagicMock(name="df")
    df.collect.return_value = []
    spark.sql.return_value = df
    return spark


class TestCatalogIdentifierValidation:
    @pytest.mark.parametrize(
        "catalog",
        [
            "cat; DROP TABLE x",
            "cat`other",
            "cat'or'1'='1",
            'cat"',
            "cat space",
            "cat.dotted",
            "cat-dash",
            "",
            "1startsdigit",
            "cat\nnewline",
            "cat\x00null",
        ],
    )
    def test_unsafe_catalog_rejected_before_spark_call(
        self, catalog: str
    ) -> None:
        spark = _mock_spark()
        with pytest.raises(UnsafeIdentifierError):
            describe_bronze(
                spark,
                catalog=catalog,
                bronze_schema="bronze",
                dataset_ids=["erp_suppliers"],
            )
        # MUST NOT have called spark.sql.
        spark.sql.assert_not_called()


class TestBronzeSchemaIdentifierValidation:
    @pytest.mark.parametrize(
        "schema",
        ["bronze; --", "bronze`", "bronze space", "bronze.dotted", ""],
    )
    def test_unsafe_schema_rejected(self, schema: str) -> None:
        spark = _mock_spark()
        with pytest.raises(UnsafeIdentifierError):
            describe_bronze(
                spark,
                catalog="cat",
                bronze_schema=schema,
                dataset_ids=["erp_suppliers"],
            )
        spark.sql.assert_not_called()


class TestDatasetIdValidation:
    @pytest.mark.parametrize(
        "dataset_id",
        [
            "erp_suppliers; DROP",
            "table`name",
            "name with space",
            "ds.dotted",
            "",
            "../traverse",
        ],
    )
    def test_unsafe_dataset_id_rejected(self, dataset_id: str) -> None:
        spark = _mock_spark()
        with pytest.raises(UnsafeIdentifierError):
            describe_bronze(
                spark,
                catalog="cat",
                bronze_schema="bronze",
                dataset_ids=[dataset_id],
            )
        spark.sql.assert_not_called()

    def test_partial_list_validation_blocks_all(self) -> None:
        """Even if only one id in the list is unsafe, the probe must
        fail closed on ALL ids — no DESCRIBE is issued for any."""
        spark = _mock_spark()
        with pytest.raises(UnsafeIdentifierError):
            describe_bronze(
                spark,
                catalog="cat",
                bronze_schema="bronze",
                dataset_ids=["good_one", "bad; one"],
            )
        spark.sql.assert_not_called()


class TestSafeIdentifiersStillWork:
    @pytest.mark.parametrize(
        "catalog,schema,dataset_id",
        [
            ("fusion_catalog", "bronze", "erp_suppliers"),
            ("cat", "bronze", "ap_invoices"),
            ("CAT", "BRONZE", "GL_PERIOD_BALANCES"),
            ("_underscored", "_starts_underscore", "_dataset"),
        ],
    )
    def test_safe_identifiers_pass_through(
        self, catalog: str, schema: str, dataset_id: str
    ) -> None:
        spark = _mock_spark()
        result = describe_bronze(
            spark,
            catalog=catalog,
            bronze_schema=schema,
            dataset_ids=[dataset_id],
        )
        assert dataset_id in result
        # Verify the issued SQL uses the safe identifiers.
        issued = spark.sql.call_args[0][0]
        assert f"{catalog}.{schema}.{dataset_id}" in issued

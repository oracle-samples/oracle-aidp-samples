"""Unit tests for ``describe_bronze_present`` (feature:
bronze-fingerprint-gate-scope).

Absence-tolerant probe: absent tables (TABLE_OR_VIEW_NOT_FOUND) are collected,
present tables are DESCRIBEd, any other Spark failure raises
``BronzeProbeFailure``, and the identifier guard is validated FIRST — before
any Spark call — so ``UnsafeIdentifierError`` can never be translated into a
(force-skippable) probe failure.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oracle_ai_data_platform_fusion_autopilot.commands.bronze_probe import (
    BronzeProbeFailure,
    UnsafeIdentifierError,
    describe_bronze_present,
)


def _row(name: str, type_: str = "string"):
    return {"col_name": name, "data_type": type_, "comment": None}


def _spark(
    present: dict[str, list[str]],
    absent: set[str] = frozenset(),
    broken: dict[str, str] | None = None,
) -> MagicMock:
    """Fake Spark: DESCRIBE raises NOT_FOUND for ``absent`` tables, raises the
    given message for ``broken`` tables, returns rows otherwise."""
    spark = MagicMock(name="spark")

    def _sql(query: str):
        target = query.split()[-1]
        table = target.split(".")[-1]
        if table in absent:
            raise Exception(
                f"[TABLE_OR_VIEW_NOT_FOUND] The table or view `{target}` "
                f"cannot be found."
            )
        if broken and table in broken:
            raise Exception(broken[table])
        df = MagicMock(name=f"df_{table}")
        df.collect.return_value = [_row(c) for c in present.get(table, [])]
        df.take.return_value = [_row(c) for c in present.get(table, [])][:1]
        return df

    spark.sql.side_effect = _sql
    return spark


class TestSplit:
    def test_absent_collected_present_described(self) -> None:
        spark = _spark({"a_tbl": ["ColA"]}, absent={"b_tbl"})
        observed, absent = describe_bronze_present(
            spark,
            catalog="cat",
            bronze_schema="bronze",
            dataset_ids=["a_tbl", "b_tbl"],
        )
        assert absent == ["b_tbl"]
        assert list(observed) == ["a_tbl"]
        assert observed["a_tbl"][0].name == "ColA"

    def test_all_present(self) -> None:
        spark = _spark({"a_tbl": ["ColA"], "b_tbl": ["ColB"]})
        observed, absent = describe_bronze_present(
            spark, catalog="cat", bronze_schema="bronze",
            dataset_ids=["a_tbl", "b_tbl"],
        )
        assert absent == []
        assert set(observed) == {"a_tbl", "b_tbl"}

    def test_all_absent_returns_empty_observed(self) -> None:
        spark = _spark({}, absent={"a_tbl", "b_tbl"})
        observed, absent = describe_bronze_present(
            spark, catalog="cat", bronze_schema="bronze",
            dataset_ids=["a_tbl", "b_tbl"],
        )
        assert observed == {}
        assert absent == ["a_tbl", "b_tbl"]

    def test_id_target_mapping_respected(self) -> None:
        # dataset id `gl_journal_lines` targets physical `gl_journal_headers`.
        spark = _spark({"gl_journal_headers": ["Col1"]}, absent={"other"})
        observed, absent = describe_bronze_present(
            spark, catalog="cat", bronze_schema="bronze",
            dataset_ids=["gl_journal_lines"],
            table_names={"gl_journal_lines": "gl_journal_headers"},
        )
        assert absent == []
        assert observed["gl_journal_lines"][0].name == "Col1"


class TestFailClosed:
    def test_non_not_found_raises_probe_failure(self) -> None:
        spark = _spark({"a_tbl": ["ColA"]}, broken={"b_tbl": "PERMISSION_DENIED: nope"})
        with pytest.raises(BronzeProbeFailure) as exc_info:
            describe_bronze_present(
                spark, catalog="cat", bronze_schema="bronze",
                dataset_ids=["a_tbl", "b_tbl"],
            )
        assert exc_info.value.dataset_id == "b_tbl"

    def test_unsafe_identifier_raised_before_any_spark_call(self) -> None:
        """Round-2 review: the injection guard is validated FIRST and is never
        translated into a (force-skippable) BronzeProbeFailure."""
        spark = MagicMock(name="spark")
        with pytest.raises(UnsafeIdentifierError):
            describe_bronze_present(
                spark, catalog="cat", bronze_schema="bronze",
                dataset_ids=["good", "bad; DROP TABLE x"],
            )
        assert spark.sql.call_count == 0

    def test_unsafe_target_raised_before_any_spark_call(self) -> None:
        spark = MagicMock(name="spark")
        with pytest.raises(UnsafeIdentifierError):
            describe_bronze_present(
                spark, catalog="cat", bronze_schema="bronze",
                dataset_ids=["good"],
                table_names={"good": "evil`table"},
            )
        assert spark.sql.call_count == 0

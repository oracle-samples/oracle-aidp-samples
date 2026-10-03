"""Review fixes on the generated write path, executed on local Spark.

Each defect here was reproduced by running the emitted code, so each test
runs it too -- a string assertion on the emitted text is how all four got
past the existing suite.

1. A Sequence Generator key was re-written by a matched update when the
   target spelled the column in mixed case: the frozen set was upper-cased
   and the emitted test compared it case-sensitively against the
   DataFrame's own spelling.
2. Integer-typed targets had no declared-range check, so 3000000000 into
   an INT wrapped to -1294967296 -- the docstring's own example.
3. A value that only overflows after rounding (99999.6 into NUMBER(5,0))
   passed the check and the write's cast stored NULL.
4. The limit was computed in double, so valid values at the top of a wide
   precision (18 nines into NUMBER(18,0)) were refused.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="executed write-path tests need pyspark (test-only)")

from infa2aidp.generators.ddl_generator import target_ddl  # noqa: E402
from infa2aidp.generators.write_strategies import (  # noqa: E402
    DeltaWriteStrategy,
    _declared_integer_ranges,
    _range_check_lines,
)
from infa2aidp.models import FieldMapping, LoadStrategy, TargetDefinition  # noqa: E402

from tests.conftest_spark import spark  # noqa: E402,F401  (session fixture)

FIXTURE = Path(__file__).parent / "fixtures" / "powercenter" / "seq_into_router.xml"


def _target(col, datatype, precision=0, scale=0):
    return TargetDefinition(name="TGT", table_name="TGT", fields=[
        FieldMapping(source_field="a", target_field=col, datatype=datatype,
                     precision=precision, scale=scale)])


def _check(spark, schema, values, target):  # noqa: F811
    """Run the emitted check over one column of `values`, as the notebook
    would. Returns None when it passes, the ValueError's text when not."""
    from pyspark.sql import functions as F
    df_final = (values if schema is None
                else spark.createDataFrame([(v,) for v in values], schema))
    code = "\n".join(l[4:] if l.startswith("    ") else l
                     for l in _range_check_lines("df_final", target))
    assert code, "no range check emitted"
    try:
        exec(compile(code, "<range_check>", "exec"), {"F": F, "df_final": df_final}, {})
    except ValueError as e:
        return str(e)
    return None


def _fits(spark, schema, value, target):  # noqa: F811
    return _check(spark, schema, [value], target) is None


# ---------------------------------------------------------------------------
# 1. Sequence key frozen on update, whatever the target's spelling
# ---------------------------------------------------------------------------

def _seq_case(tmp_path, keycol):
    xml = FIXTURE.read_text(encoding="utf-8")
    if keycol != "SURROGATE_KEY":
        # Only the TARGET's spelling changes: its field and the connector
        # into it. The Router port keeps its own name.
        xml = (xml.replace('TOINSTANCE="TGT_BIG_ORDERS" TOFIELD="SURROGATE_KEY"',
                           f'TOINSTANCE="TGT_BIG_ORDERS" TOFIELD="{keycol}"')
                  .replace('TARGETFIELD NAME="SURROGATE_KEY"',
                           f'TARGETFIELD NAME="{keycol}"'))
        assert xml.count(keycol) == 2, "fixture shape changed; rename did not apply"
    case = tmp_path / "case"
    case.mkdir()
    (case / "m.xml").write_text(xml, encoding="utf-8")
    (case / "case.json").write_text(json.dumps({"export": "m.xml"}))
    (case / "seed.json").write_text(json.dumps({
        "ORDERS": {"schema": "ORDER_ID INT, AMOUNT DECIMAL(10,2)",
                   "rows": [[10, "2000.00"], [11, "5000.00"]]},
        # ORDER_ID 10 already loaded with key 500 by an earlier run.
        "TGT_BIG_ORDERS": {"schema": f"{keycol} BIGINT, ORDER_ID INT, AMOUNT DECIMAL(10,2)",
                           "rows": [[500, 10, "1500.00"]]},
    }))
    (case / "expected.json").write_text(json.dumps({"TGT_BIG_ORDERS": {"rows": []}}))
    return case


@pytest.mark.parametrize("keycol", ["SURROGATE_KEY", "Surrogate_Key", "surrogate_key"])
def test_existing_row_keeps_its_sequence_key_on_rerun(spark, tmp_path, keycol):  # noqa: F811
    """Was 500 -> 1 for Surrogate_Key: the matched update re-keyed the row."""
    from tests.golden.harness import run_case

    work = tmp_path / "work"
    work.mkdir()
    out = run_case(spark, _seq_case(tmp_path, keycol), work)
    rows = {r["ORDER_ID"]: r for r in out["TGT_BIG_ORDERS"]}
    assert rows[10][keycol] == 500, rows
    # The update itself still happened -- only the key was held back.
    assert Decimal(str(rows[10]["AMOUNT"])) == Decimal("2000.00"), rows
    # And the new row still got a key from the insert path.
    assert rows[11][keycol] is not None, rows


def test_frozen_set_is_compared_case_insensitively():
    """Both sides upper-cased, so neither spelling can slip through."""
    import types
    mapping_target = TargetDefinition(name="TGT_BIG_ORDERS", table_name="TGT_BIG_ORDERS", fields=[
        FieldMapping(source_field="k", target_field="Surrogate_Key", datatype="bigint"),
        FieldMapping(source_field="o", target_field="ORDER_ID", datatype="integer"),
    ])
    mapping = types.SimpleNamespace(
        connectors=[types.SimpleNamespace(from_instance="SEQ", from_field="NEXTVAL",
                                          to_instance="TGT_BIG_ORDERS", to_field="Surrogate_Key")],
        transformations=[types.SimpleNamespace(name="SEQ", type="Sequence Generator", fields=[])],
    )
    text = "\n".join(DeltaWriteStrategy().emit_write(
        mapping, mapping_target, "db.TGT_BIG_ORDERS", LoadStrategy.UPSERT,
        ["ORDER_ID"], "df_final"))
    assert "_FROZEN_ON_UPDATE = {'SURROGATE_KEY'}" in text
    assert "if c.upper() not in _FROZEN_ON_UPDATE" in text


# ---------------------------------------------------------------------------
# 2. Integer-typed targets are range-checked against the Spark type
# ---------------------------------------------------------------------------

def test_integer_spellings_get_the_type_the_ddl_creates():
    """The bound checked is the bound of the column the DDL creates."""
    cases = {"int": "INT", "integer": "INT", "small integer": "SMALLINT",
             "smallint": "SMALLINT", "bigint": "BIGINT", "tinyint": "SMALLINT"}
    for dt, spark_type in cases.items():
        tgt = _target("C", dt, 10, 0)
        assert [r[2] for r in _declared_integer_ranges(tgt)] == [spark_type], dt
        assert f"C {spark_type}" in target_ddl(None, tgt).sql, dt
    # Not integers: no integer check.
    for dt in ("number", "varchar2", "double", "date"):
        assert _declared_integer_ranges(_target("C", dt, 10, 0)) == [], dt


def test_the_docstring_example_is_refused_not_wrapped(spark):  # noqa: F811
    """3000000000 into an INT: the write's cast stores -1294967296."""
    msg = _check(spark, "ORDER_ID bigint", [1, 3000000000, None],
                 _target("ORDER_ID", "int", 10, 0))
    assert msg is not None
    assert "ORDER_ID: 1 row(s) exceed the declared INT" in msg
    assert "wrap these silently" in msg


def test_a_sequence_fed_integer_key_is_refused_past_int(spark):  # noqa: F811
    """NEXTVAL is a BIGINT; an Informatica `integer` key is created INT."""
    msg = _check(spark, "SURROGATE_KEY bigint", [2147483647, 2147483648],
                 _target("SURROGATE_KEY", "integer", 10, 0))
    assert msg is not None and "1 row(s)" in msg
    assert "INTEGER (Spark INT)" in msg


@pytest.mark.parametrize("dt,lo,hi", [
    ("integer", -2 ** 31, 2 ** 31 - 1),
    ("smallint", -2 ** 15, 2 ** 15 - 1),
    ("small integer", -2 ** 15, 2 ** 15 - 1),
    # The DDL creates TINYINT as SMALLINT, so that is the range that wraps.
    ("tinyint", -2 ** 15, 2 ** 15 - 1),
])
def test_integer_boundaries_both_sides(spark, dt, lo, hi):  # noqa: F811
    tgt = _target("K", dt, 10, 0)
    assert _fits(spark, "K bigint", hi, tgt)
    assert _fits(spark, "K bigint", lo, tgt)
    assert not _fits(spark, "K bigint", hi + 1, tgt)
    assert not _fits(spark, "K bigint", lo - 1, tgt)
    assert _fits(spark, "K bigint", None, tgt)


def test_bigint_boundaries_both_sides(spark):  # noqa: F811
    tgt = _target("K", "bigint", 19, 0)
    assert _fits(spark, "K bigint", 2 ** 63 - 1, tgt)
    assert _fits(spark, "K bigint", -2 ** 63, tgt)
    assert not _fits(spark, "K decimal(20,0)", Decimal(2 ** 63), tgt)
    assert not _fits(spark, "K decimal(20,0)", Decimal(-2 ** 63 - 1), tgt)


def test_integer_check_follows_the_casts_truncation(spark):  # noqa: F811
    """The write's cast truncates a fraction toward zero, so a value that
    truncates into range fits; only the integer part can overflow."""
    tgt = _target("K", "integer", 10, 0)
    assert _fits(spark, "K double", 2147483647.9, tgt)
    assert _fits(spark, "K double", -2147483648.9, tgt)
    assert not _fits(spark, "K double", 2147483648.0, tgt)
    assert _fits(spark, "K decimal(12,1)", Decimal("2147483647.9"), tgt)
    # A fractional string is not refused (try_cast to INT alone would).
    assert _fits(spark, "K string", "12.5", tgt)
    assert not _fits(spark, "K string", "2147483648", tgt)
    # Not a number at all is not an overflow.
    assert _fits(spark, "K string", "abc", tgt)


# ---------------------------------------------------------------------------
# 3. Overflow after rounding is refused (it used to be written as NULL)
# ---------------------------------------------------------------------------

def test_rounding_boundary_for_number_5_0(spark):  # noqa: F811
    tgt = _target("AMT", "number", 5, 0)
    for schema in ("AMT decimal(6,1)", "AMT double"):
        conv = (lambda s: Decimal(s)) if "decimal" in schema else float
        assert _fits(spark, schema, conv("99999.4"), tgt), schema
        assert not _fits(spark, schema, conv("99999.5"), tgt), schema
        assert not _fits(spark, schema, conv("99999.6"), tgt), schema
        assert _fits(spark, schema, conv("-99999.4"), tgt), schema
        assert not _fits(spark, schema, conv("-99999.5"), tgt), schema
        assert _fits(spark, schema, None, tgt), schema


def test_rounding_overflow_at_a_nonzero_scale(spark):  # noqa: F811
    tgt = _target("AMT", "decimal", 10, 2)
    assert _fits(spark, "AMT decimal(11,3)", Decimal("99999999.994"), tgt)
    assert not _fits(spark, "AMT decimal(11,3)", Decimal("99999999.995"), tgt)


def test_the_check_agrees_with_the_writes_cast(spark):  # noqa: F811
    """Refused exactly where the cast the write applies would store NULL."""
    from pyspark.sql import functions as F
    tgt = _target("AMT", "number", 5, 2)
    values = [Decimal(v) for v in ("999.994", "999.995", "-999.995", "-999.994",
                                   "0.005", "1000", "999.99")]
    df = spark.createDataFrame([(v,) for v in values], "AMT decimal(8,3)")
    written = {r.AMT: r.W for r in df.select(
        "AMT", F.col("AMT").cast("decimal(5,2)").alias("W")).collect()}
    for v in values:
        assert _fits(spark, "AMT decimal(8,3)", v, tgt) == (written[v] is not None), v


# ---------------------------------------------------------------------------
# 4. Exact decimal comparison at wide precisions
# ---------------------------------------------------------------------------

def test_valid_values_at_wide_precision_are_not_refused(spark):  # noqa: F811
    """All three were refused by the double-precision 10**n limit."""
    assert _fits(spark, "ID decimal(18,0)", Decimal("9" * 18), _target("ID", "number", 18, 0))
    assert _fits(spark, "ID decimal(38,0)", Decimal("9" * 38), _target("ID", "number", 38, 0))
    # Built in SQL: PySpark's own Python->JVM conversion rejects a negative
    # 38-digit Decimal before any of this code runs.
    from pyspark.sql import functions as F
    neg = spark.range(1).select(
        F.expr(f"CAST('-{'9' * 38}' AS DECIMAL(38,0))").alias("ID"))
    assert _check(spark, None, neg, _target("ID", "number", 38, 0)) is None
    assert _fits(spark, "ID decimal(17,2)", Decimal("999999999999999.99"),
                 _target("ID", "number", 17, 2))


def test_one_past_the_wide_maximum_is_refused(spark):  # noqa: F811
    assert not _fits(spark, "ID decimal(19,0)", Decimal("1" + "0" * 18),
                     _target("ID", "number", 18, 0))
    assert not _fits(spark, "ID decimal(19,0)", -Decimal("1" + "0" * 18),
                     _target("ID", "number", 18, 0))
    assert not _fits(spark, "ID decimal(18,3)", Decimal("999999999999999.995"),
                     _target("ID", "number", 17, 2))
    # 10^38 cannot be held in any Spark decimal; it arrives as a double.
    assert not _fits(spark, "ID double", 1e38, _target("ID", "number", 38, 0))


# ---------------------------------------------------------------------------
# Contract kept
# ---------------------------------------------------------------------------

def test_message_names_column_count_and_declaration(spark):  # noqa: F811
    tgt = TargetDefinition(name="T", table_name="T", fields=[
        FieldMapping(source_field="a", target_field="QTY", datatype="NUMBER",
                     precision=3, scale=0),
        FieldMapping(source_field="b", target_field="N", datatype="smallint",
                     precision=5, scale=0)])
    from pyspark.sql import functions as F
    df_final = spark.createDataFrame([(1000, 40000), (5, 1), (None, None)], "QTY int, N int")
    code = "\n".join(l[4:] if l.startswith("    ") else l
                     for l in _range_check_lines("df_final", tgt))
    with pytest.raises(ValueError) as exc:
        exec(compile(code, "<range_check>", "exec"), {"F": F, "df_final": df_final}, {})
    msg = str(exc.value)
    assert msg.startswith("Refusing to write: ")
    assert "QTY: 1 row(s) exceed the declared NUMBER(3,0)" in msg
    assert "N: 1 row(s) exceed the declared SMALLINT" in msg


def test_column_found_whatever_its_case(spark):  # noqa: F811
    """The target says Qty, the DataFrame says QTY: still checked."""
    assert _check(spark, "QTY int", [5000], _target("Qty", "number", 3, 0)) is not None

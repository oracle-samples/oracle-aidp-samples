"""Does the SQL we emit actually parse as Spark SQL?

Nothing in the suite ever answered that. 650 hand-written assertions checked
the emitted text against expectations written by the same author as the
implementation, so a systematically wrong Spark idiom would pass all of them.
The AIDP write path is unverified, and this is the strongest offline check
available: parse the generated DDL with a real Spark-dialect parser.

Dev-only. Skips when sqlglot is absent, so the plugin gains no runtime
dependency. Parsing proves syntax, not semantics -- a statement can parse and
still be wrong -- but a statement that does NOT parse is wrong for certain.
"""
from __future__ import annotations

import pytest

sqlglot = pytest.importorskip("sqlglot", reason="dev-only SQL parse check")

from snowflake_source.dialect.views import extract_view_body
from target.ddl import build_create_schema, build_create_table, build_create_view


def _parse(sql: str):
    return sqlglot.parse_one(sql, dialect="spark")


def _table(columns):
    return {"source_identifier": "DB.SC.T", "object_type": "TABLE",
            "source_metadata": {}, "columns": columns}


def _col(name, target_type, pos=1, nullable="YES", comment=None):
    return {"COLUMN_NAME": name, "target_type": target_type,
            "ORDINAL_POSITION": pos, "IS_NULLABLE": nullable,
            "DATA_TYPE": "TEXT", "COMMENT": comment}


def test_create_schema_parses():
    assert _parse(build_create_schema("CAT", "SC"))


def test_create_table_parses_for_every_type_we_emit():
    types = ["STRING", "BOOLEAN", "DATE", "BINARY", "DOUBLE", "TIMESTAMP",
             "TIMESTAMP_NTZ", "DECIMAL(38,0)", "DECIMAL(10,2)"]
    cols = [_col(f"C{i}", t, i) for i, t in enumerate(types, 1)]
    res = build_create_table(_table(cols), "CAT.SC.T")
    parsed = _parse(res.sql)
    assert parsed is not None
    # Every column we claimed to plan is actually in the statement.
    rendered = res.sql.upper()
    for c in res.expected_columns:
        assert c["name"].upper() in rendered
        assert c["type"].split("(")[0].upper() in rendered


def test_not_null_and_comment_still_parse():
    cols = [_col("A", "STRING", 1, nullable="NO", comment="it's fine"),
            _col("B", "DECIMAL(38,0)", 2)]
    assert _parse(build_create_table(_table(cols), "CAT.SC.T").sql)


def test_awkward_identifiers_still_parse():
    # Backtick quoting must survive a name that needs it.
    cols = [_col("order id", "STRING", 1), _col("SELECT", "STRING", 2),
            _col("we`ird", "STRING", 3)]
    assert _parse(build_create_table(_table(cols), "CAT.SC.T").sql)


@pytest.mark.parametrize("body", [
    "select 1 as a",
    "select a, b from t where a > 1",
    "select IFF(a, 'y', 'n') as f from t",
    "select a::varchar as v from t",
    "select LISTAGG(name, ', ') from t",
    "select DATEADD(day, 1, d) as d2 from t",
    "select DATEADD(week, n, d) as d2 from t",
    "select DATEADD(year, n, d) as d3 from t",
    "select DATEADD(hour, -1, ts) as d4 from t",
    "select DATEADD(day, -30, CURRENT_DATE()) as cutoff from t",
    "select DATEADD('day', 1, d) as d2 from t",
    "select DATEADD(day, abs(n), d) as d2 from t",
    "select DATEADD(day, 1, d) as d1, DATEADD(day, -30, CURRENT_DATE()) as c from t",
    "select ARRAY_CONSTRUCT(1, 2) as arr",
    "select OBJECT_CONSTRUCT('k', v) as o from t",
    'select "Order ID", "Amount" from DB.SC."Orders"',
    "select 'O''Brien' as who, 'it''s' as note from t",
    "select $$it's$$ as note from t",
])
def test_translated_view_bodies_parse_as_spark(body):
    """Each implemented rule's output must be valid Spark SQL.

    This is the check that would catch a rule producing plausible-looking but
    unparseable output -- the failure mode a hand-written expectation cannot
    catch, because the expectation and the implementation share an author.
    """
    record = {"source_identifier": "DB.SC.V", "object_type": "VIEW",
              "view_ddl_get_ddl": f"create view V as {body}",
              "source_metadata": {}, "columns": []}
    res = build_create_view(record, "CAT.SC.V", {})
    if res.blocked:
        pytest.skip(f"blocked, not translated: {res.blocked_reason}")
    parsed = _parse(res.sql)
    assert parsed is not None
    # And the translated body must not still contain the Snowflake-only form.
    translated = extract_view_body(res.sql).upper()
    for banned in ("IFF(", "::", "LISTAGG(", "DATEADD(", "ARRAY_CONSTRUCT(",
                   "OBJECT_CONSTRUCT("):
        assert banned not in translated, f"{banned} survived translation"


def test_cast_targets_are_spark_types_not_snowflake_names():
    """The parse gate is lenient: sqlglot accepts CAST(x AS NUMBER(18,2)) and
    CAST(x AS TEXT) in its spark dialect, so parsing alone cannot catch a type
    name copied over verbatim. Check the CAST targets themselves."""
    record = {"source_identifier": "DB.SC.V", "object_type": "VIEW",
              "view_ddl_get_ddl": ("create view V as select a::NUMBER(18,2) as n, "
                                   "b::FLOAT as f, c::INT as i from t"),
              "source_metadata": {}, "columns": []}
    res = build_create_view(record, "CAT.SC.V", {})
    assert res.blocked is False, res.blocked_reason
    tree = _parse(res.sql)
    targets = sorted(c.to.sql(dialect="spark") for c in tree.find_all(sqlglot.exp.Cast))
    assert targets == sorted(["DECIMAL(18, 2)", "DOUBLE", "DECIMAL(38, 0)"]), targets


def _view(body):
    record = {"source_identifier": "DB.SC.V", "object_type": "VIEW",
              "view_ddl_get_ddl": f"create view V as {body}",
              "source_metadata": {}, "columns": []}
    return build_create_view(record, "CAT.SC.V", {})


def test_doubled_quote_literal_is_one_literal_on_spark_not_a_concat():
    """A parse-only check passes on the CONCAT form: Spark reads 'O''Brien' as
    two adjacent literals. The assertion that catches the bug is that the
    tree holds exactly one string literal, and it is O'Brien."""
    res = _view("select 'O''Brien' as who, 'it''s' as note from t")
    assert res.blocked is False, res.blocked_reason
    tree = _parse(res.sql)
    literals = sorted(lit.this for lit in tree.find_all(sqlglot.exp.Literal)
                      if lit.is_string)
    assert literals == ["O'Brien", "it's"], literals
    assert not list(tree.find_all(sqlglot.exp.Concat))


def test_quoted_identifier_is_a_column_on_spark_not_a_string():
    res = _view('select "Order ID", "Amount" from t')
    assert res.blocked is False, res.blocked_reason
    tree = _parse(res.sql)
    names = sorted(c.name for c in tree.find_all(sqlglot.exp.Column))
    assert names == ["Amount", "Order ID"], names
    assert not [lit for lit in tree.find_all(sqlglot.exp.Literal) if lit.is_string]

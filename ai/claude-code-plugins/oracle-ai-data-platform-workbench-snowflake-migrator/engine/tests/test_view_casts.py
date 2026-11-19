"""A `::` cast is only "exact" when the type mapper says it is.

Reproduced through build_create_view. T02 sends the cast
type through the same mapper as table DDL, and TIME maps to STRING -- with
the column-level warning "The text is preserved". For a TIME column that
is true. For a cast it is not: the operand's type is unknown to a token
rule, and `order_ts::time`, the usual time-of-day extraction, became
`CAST(order_ts AS STRING)`, which on Spark returns the whole
'yyyy-MM-dd HH:mm:ss' where Snowflake returns 'HH:MI:SS'. In the same
DDL_PLAN section R43 said "every one is an exact rewrite" next to that
warning, because R43 only counted a rule's static caveat and T02 has none.
Row counts reconcile, so nothing later catches it.

So `::TIME` is refused with the reason, and any cast the mapper warns about
(`::TIMESTAMP`'s timezone semantics) is a caveat on the application, which
makes R43 read "NOT all exact" as DATEADD's does.
"""
from snowflake_source.dialect.translate import translate_sql
from target.ddl import build_create_view


def _view(ddl):
    return {"source_identifier": "DB.S.V", "object_type": "VIEW",
            "source_database": "DB", "source_schema": "S",
            "view_ddl_get_ddl": ddl, "source_metadata": {}, "columns": [],
            "compatibility_status": "supported"}


def _r43(res):
    return next(r.detail for r in res.rules_applied
                if r.rule_id == "R43_VIEW_DIALECT_TRANSLATED")


def test_a_time_cast_is_refused_not_cast_to_string():
    res = build_create_view(
        _view("create view V as select order_ts::time as t from DB.S.O"),
        "lake.db_s.v")
    assert res.blocked is True
    assert res.sql is None
    assert "::time" in res.blocked_reason and "TIME" in res.blocked_reason


def test_a_time_cast_with_precision_is_refused_too():
    r = translate_sql("select ts::TIME(3) from t")
    assert [u["rule_id"] for u in r.unsupported] == ["T02_CAST_SHORTHAND"]
    assert r.sql == "select ts::TIME(3) from t"
    assert "CAST(" not in r.sql


def test_a_timestamp_cast_is_not_labelled_exact():
    res = build_create_view(
        _view("create view V as select d::timestamp as ts from DB.S.O"),
        "lake.db_s.v")
    assert res.blocked is False, res.blocked_reason
    detail = _r43(res)
    assert "NOT all exact" in detail, detail
    assert "T02_CAST_SHORTHAND" in detail and "timezone" in detail, detail


def test_a_cast_the_mapper_does_not_warn_about_stays_exact():
    res = build_create_view(
        _view("create view V as select n::number(10,2) as x from DB.S.O"),
        "lake.db_s.v")
    assert res.blocked is False, res.blocked_reason
    assert _r43(res).endswith("every one is an exact rewrite"), _r43(res)


def test_the_caveat_is_on_the_applied_entry():
    r = translate_sql("select a::TIMESTAMP_LTZ from t")
    (applied,) = r.applied
    assert applied["rule_id"] == "T02_CAST_SHORTHAND"
    assert "timezone semantics differ" in applied["caveat"], applied

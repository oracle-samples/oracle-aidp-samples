"""LISTAGG used as a window function is refused, not half-translated.

Reproduced with translate_sql and build_create_view.
T06 rewrites `LISTAGG(a, ',')` to `concat_ws(',', collect_list(a))`, and its
match ends at LISTAGG's closing paren. So

    LISTAGG(a, ',') OVER (PARTITION BY b)

became `concat_ws(',', collect_list(a)) OVER (PARTITION BY b)` -- a scalar
function with a window clause, which Spark rejects -- while the view was
planned migratable and R43 said "every one is an exact rewrite". The deploy
then failed with a bare catalog-API 500 instead of the view being refused up
front with the construct named. The guards refused WITHIN GROUP and a
residual LISTAGG( but never looked for a following OVER.
"""
from snowflake_source.dialect.translate import translate_sql
from target.ddl import build_create_view


def test_listagg_over_a_window_is_refused_and_left_untouched():
    sql = "SELECT LISTAGG(a, ',') OVER (PARTITION BY b) FROM t"
    r = translate_sql(sql)
    assert [u["rule_id"] for u in r.unsupported] == ["T06_LISTAGG"]
    assert "OVER" in r.unsupported[0]["detail"]
    assert r.sql == sql
    assert not r.applied


def test_listagg_over_with_a_comment_between_is_still_refused():
    r = translate_sql("select listagg(a, ',') /* w */ over (order by b) from t")
    assert [u["rule_id"] for u in r.unsupported] == ["T06_LISTAGG"]


def test_plain_listagg_is_still_translated():
    r = translate_sql("select listagg(a, ',') as l, max(b) over () from t")
    assert r.sql == ("select concat_ws(',', collect_list(a)) as l, "
                     "max(b) over () from t")
    assert [a["rule_id"] for a in r.applied] == ["T06_LISTAGG"]


def test_a_view_with_windowed_listagg_is_blocked_naming_it():
    res = build_create_view({
        "source_identifier": "DB.S.V", "object_type": "VIEW",
        "source_database": "DB", "source_schema": "S",
        "view_ddl_get_ddl": "create view V as select listagg(a, ',') "
                            "over (partition by b) as l from DB.S.T",
        "source_metadata": {}, "columns": []}, "lake.db_s.v")
    assert res.blocked is True
    assert "LISTAGG" in res.blocked_reason and "OVER" in res.blocked_reason

"""View references that are not three-part names.

Found on a live run, a database planned into a new catalog:
the view ANALYTICS.VW_ORDER_PAYMENT reads `FROM COMMERCE.ORDERS o LEFT JOIN
PAYMENT.PAYMENTS p`. Snowflake resolves a two-part name against the VIEW'S
OWN DATABASE, so the target is known -- but the rewriter matched three-part
names only, recorded R40 "references unchanged", and emitted SQL that points
at schemas the target does not have.

Two forms are resolvable, both only after FROM / JOIN where nothing but a
table can appear:
  * `SCHEMA.NAME` -- any schema of the view's own database
  * bare `NAME`   -- the view's own schema
Anything else left one- or two-part is outside the migration and is named.
"""
from target.ddl import build_create_view

DB = "TEST_DB"
NAME_MAP = {
    f"{DB}.COMMERCE.ORDERS": "cat.test_db_commerce.orders",
    f"{DB}.PAYMENT.PAYMENTS": "cat.test_db_payment.payments",
    f"{DB}.ANALYTICS.DIM": "cat.test_db_analytics.dim",
    f"{DB}.ANALYTICS.V": "cat.test_db_analytics.v",
}


def _view(body, schema="ANALYTICS"):
    return {"source_identifier": f"{DB}.{schema}.V", "object_type": "VIEW",
            "source_database": DB, "source_schema": schema,
            "view_ddl_get_ddl": f"create or replace view V as {body};",
            "columns": []}


def _build(body, schema="ANALYTICS"):
    return build_create_view(_view(body, schema), "cat.test_db_analytics.v",
                             NAME_MAP)


def test_a_two_part_reference_into_another_schema_is_qualified():
    res = _build("select o.ID from COMMERCE.ORDERS o "
                 "left join PAYMENT.PAYMENTS p on p.ORDER_ID = o.ID")
    assert "from cat.test_db_commerce.orders o" in res.sql
    assert "join cat.test_db_payment.payments p" in res.sql
    ids = [r.rule_id for r in res.rules_applied]
    assert "R41_VIEW_REFS_REWRITTEN" in ids
    assert "R40_VIEW_REFS_IDENTITY" not in ids


def test_a_bare_name_resolves_in_the_views_own_schema():
    res = _build("select * from DIM")
    assert "from cat.test_db_analytics.dim" in res.sql


def test_a_bare_name_is_not_guessed_into_another_schema():
    """A bare ORDERS in ANALYTICS cannot mean COMMERCE.ORDERS."""
    res = _build("select * from ORDERS")
    assert "from ORDERS" in res.sql
    assert any("ORDERS" in w and "unresolved" in w.lower() for w in res.warnings)
    # R45: the id R44 is the view column list's.
    assert "R45_VIEW_REFS_UNRESOLVED" in [r.rule_id for r in res.rules_applied]


def test_a_column_named_like_a_table_is_left_alone():
    res = _build("select DIM, ORDERS from DIM")
    assert res.sql.split(" AS\n", 1)[1].startswith("select DIM, ORDERS from")


def test_text_in_a_string_literal_is_left_alone():
    res = _build("select 'from COMMERCE.ORDERS' as x from DIM")
    assert "'from COMMERCE.ORDERS'" in res.sql


def test_a_three_part_reference_still_rewrites():
    res = _build(f"select * from {DB}.COMMERCE.ORDERS")
    assert "from cat.test_db_commerce.orders" in res.sql


def test_quoted_parts_are_resolved():
    res = _build('select * from "COMMERCE"."ORDERS"')
    assert "from cat.test_db_commerce.orders" in res.sql


def test_a_reference_outside_the_migration_is_named_not_invented():
    res = _build("select * from OTHER_SCHEMA.THING")
    assert "OTHER_SCHEMA.THING" in res.sql
    assert any("OTHER_SCHEMA.THING" in w for w in res.warnings)


def test_a_subquery_is_not_a_reference():
    res = _build("select * from (select 1 as a) t join DIM d on 1=1")
    assert "join cat.test_db_analytics.dim d" in res.sql
    assert not any("unresolved" in w.lower() for w in res.warnings)


def test_an_identity_mirror_still_reports_identity():
    same = {f"{DB}.ANALYTICS.DIM": f"{DB}.ANALYTICS.DIM"}
    res = build_create_view(_view(f"select * from {DB}.ANALYTICS.DIM"),
                            f"{DB}.ANALYTICS.V", same)
    assert "R40_VIEW_REFS_IDENTITY" in [r.rule_id for r in res.rules_applied]

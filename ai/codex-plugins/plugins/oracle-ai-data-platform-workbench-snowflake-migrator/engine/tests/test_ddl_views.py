"""View DDL generation. Bronze mirrors source, so reference rewriting is identity;
the real work is refusing to ship a wrong dialect translation."""
import pytest

from snowflake_source.dialect.views import (
    UNSUPPORTED_CONSTRUCTS, detect_unsupported_constructs, extract_view_body,
)
from target.ddl import build_create_view

REAL = """create or replace view ACME_ORDER_360_VW(
\tORDER_ID,
\tITEM_COUNT
) as
  SELECT
    o.ORDER_ID,
    COUNT(i.ORDER_ITEM_ID) AS ITEM_COUNT
  FROM TEST_DB.PUBLIC.ORDER_DIMENSIONS o
  LEFT JOIN TEST_DB.PUBLIC.ORDER_ITEMS_FACT i ON o.ORDER_ID = i.ORDER_ID
  GROUP BY o.ORDER_ID;"""


def view_record(ddl=REAL, **over):
    r = {"source_identifier": "TEST_DB.PUBLIC.ACME_ORDER_360_VW",
         "object_type": "VIEW", "source_database": "TEST_DB",
         "source_schema": "PUBLIC", "view_ddl_get_ddl": ddl,
         "source_metadata": {}, "columns": [], "compatibility_status": "supported"}
    r.update(over)
    return r


# --- body extraction ------------------------------------------------------

def test_body_extracted_after_the_column_list():
    body = extract_view_body(REAL)
    assert body.startswith("SELECT")
    assert "GROUP BY o.ORDER_ID" in body
    assert not body.endswith(";")


def test_body_extracted_without_a_column_list():
    body = extract_view_body("create view V as select 1")
    assert body == "select 1"


def test_secure_view_header_still_parses():
    assert extract_view_body("create or replace secure view V as select 1") == "select 1"


def test_unparseable_ddl_raises():
    with pytest.raises(ValueError, match="could not locate"):
        extract_view_body("this is not a view definition")


# --- dialect detection ----------------------------------------------------

def test_portable_view_has_no_unsupported_constructs():
    assert detect_unsupported_constructs(extract_view_body(REAL)) == []


@pytest.mark.parametrize("sql,construct", [
    ("select * from t qualify row_number() over (order by a) = 1", "QUALIFY"),
    ("select f.value from t, lateral flatten(input => t.j) f", "LATERAL FLATTEN"),
    ("select decode(a, 1, 'x') from t", "DECODE"),
    ("select nvl2(a, 1, 2) from t", "NVL2"),
    ("select seq4() from table(generator(rowcount => 5))", "GENERATOR / SEQ4"),
    ("select system$current_user() from t", "SYSTEM$"),
    ("select * from t at(timestamp => x)", "Time Travel"),
    ("select j:field from t", "VARIANT path"),
    ('select j:"Field Name" from t', "VARIANT path"),
    ("select datediff(day, a, b) from t", "DATEDIFF / TIMESTAMPDIFF"),
    ("select timestampadd(day, 1, ts) from t", "TIMESTAMPADD / TIMEADD"),
])
def test_snowflake_only_constructs_detected(sql, construct):
    found = [c["construct"] for c in detect_unsupported_constructs(sql)]
    assert construct in found, found


@pytest.mark.parametrize("sql", [
    "select iff(a > 1, 'y', 'n') from t",
    "select a::string from t",
    "select listagg(a, ',') from t",
    "select array_construct(1, 2) from t",
    "select object_construct('a', 1) from t",
    "select dateadd(day, 7, d) from t",
])
def test_translatable_constructs_no_longer_block(sql):
    # These have exact rewrites, so they are translated rather than refused.
    assert detect_unsupported_constructs(sql) == []


def test_every_detection_carries_a_brief_reason():
    for c in detect_unsupported_constructs("select decode(a,1,2), nvl2(a,1,2) from t"):
        assert c["reason"] and len(c["reason"]) > 10


def test_max_by_is_not_flagged_spark_supports_it():
    assert detect_unsupported_constructs("select max_by(a, b) from t") == []


def test_constructs_table_is_exported():
    assert "QUALIFY" in UNSUPPORTED_CONSTRUCTS


def test_a_view_using_only_translatable_sql_migrates():
    ddl = "create view V as select IFF(a > 1, 'y', 'n') AS f, b::int AS n from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False
    assert "IF(a > 1, 'y', 'n')" in res.sql and "IFF(" not in res.sql
    assert "CAST(b AS DECIMAL(38,0))" in res.sql, "INT is NUMBER(38,0) in Snowflake"
    assert any(r.rule_id == "R43_VIEW_DIALECT_TRANSLATED" for r in res.rules_applied)
    assert any(r.rule_id == "T01_IFF" for r in res.rules_applied)


def test_a_view_mixing_translatable_and_structural_sql_is_still_blocked():
    ddl = ("create view V as select IFF(a,1,2) from D.S.T "
           "qualify row_number() over (order by a) = 1")
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "QUALIFY" in res.blocked_reason


# --- view generation ------------------------------------------------------

def test_portable_view_generates_create_view():
    res = build_create_view(view_record(), "TEST_DB.PUBLIC.ACME_ORDER_360_VW")
    assert res.blocked is False
    # REAL's header column list is carried by aliasing the body, never as a
    # view column list, which AIDP cannot read back (see
    # test_view_column_list_live.py).
    assert res.sql.startswith(
        "CREATE VIEW IF NOT EXISTS `TEST_DB`.`PUBLIC`.`ACME_ORDER_360_VW` "
        "AS SELECT * FROM (")
    assert res.sql.endswith(") AS named_columns(`ORDER_ID`, `ITEM_COUNT`)")
    assert "SELECT" in res.sql
    assert "OR REPLACE" not in res.sql


def test_bronze_mirror_means_references_are_unchanged():
    res = build_create_view(view_record(), "TEST_DB.PUBLIC.ACME_ORDER_360_VW")
    assert "TEST_DB.PUBLIC.ORDER_DIMENSIONS" in res.sql
    assert any(r.rule_id == "R40_VIEW_REFS_IDENTITY" for r in res.rules_applied)


def test_reference_rewrite_applied_when_names_change():
    res = build_create_view(
        view_record(), "bronze.TEST_DB_PUBLIC.ACME_ORDER_360_VW",
        name_map={"TEST_DB.PUBLIC.ORDER_DIMENSIONS": "bronze.TEST_DB_PUBLIC.ORDER_DIMENSIONS"})
    assert "bronze.TEST_DB_PUBLIC.ORDER_DIMENSIONS" in res.sql
    assert any(r.rule_id == "R41_VIEW_REFS_REWRITTEN" for r in res.rules_applied)


def test_view_with_qualify_is_blocked_naming_the_construct():
    ddl = "create view V as select * from t qualify row_number() over (order by a) = 1"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert res.sql is None
    assert "QUALIFY" in res.blocked_reason


def test_secure_view_is_blocked():
    res = build_create_view(
        view_record(source_metadata={"is_secure": "true"}), "D.S.V")
    assert res.blocked is True
    assert "secure" in res.blocked_reason.lower()


def test_materialized_view_is_blocked():
    res = build_create_view(
        view_record(source_metadata={"is_materialized": "true"}), "D.S.V")
    assert res.blocked is True
    assert "materiali" in res.blocked_reason.lower()


def test_missing_ddl_is_blocked_not_silently_skipped():
    res = build_create_view(view_record(view_ddl_get_ddl=None), "D.S.V")
    assert res.blocked is True
    assert "no view SQL" in res.blocked_reason


def test_unparseable_ddl_blocks_with_a_reason():
    res = build_create_view(view_record(ddl="garbage"), "D.S.V")
    assert res.blocked is True
    assert "could not locate" in res.blocked_reason


# --- regression: header/body boundary ------------------------------------

def test_body_is_not_truncated_by_an_as_inside_the_select():
    # REGRESSION: the header pattern used to match the FIRST `) as` anywhere, so
    # `IFF(a, 'y', 'n') AS f` was read as the end of the column list and the body
    # was silently truncated. Truncated SQL that still runs is the worst outcome.
    ddl = "create view V as select IFF(a > 1, 'y', 'n') AS f, b from D.S.T"
    body = extract_view_body(ddl)
    assert body.startswith("select IFF(")
    assert body.endswith("from D.S.T")


def test_column_list_header_still_parses_with_an_as_in_the_body():
    ddl = ("create or replace view V(\n\tF,\n\tB\n) as\n"
           "  select IFF(a, 1, 2) AS F, b AS B from D.S.T;")
    body = extract_view_body(ddl)
    assert body.startswith("select IFF(")
    assert "AS B" in body


def test_function_call_in_the_body_does_not_look_like_a_column_list():
    ddl = "create view V as select coalesce(sum(x), 0) as total from D.S.T"
    assert extract_view_body(ddl).startswith("select coalesce(")


def test_quoted_column_list_still_parses():
    ddl = 'create view V("A", "B") as select a, b from t'
    assert extract_view_body(ddl) == "select a, b from t"


# --------------------------------------------------------------------------
# Header location by scanner, not regex.
# --------------------------------------------------------------------------

def test_body_found_when_the_view_name_is_quoted_and_contains_a_space():
    # The old header regex allowed [\w$".]+ for the name, so a legal quoted
    # name with a space failed to match and the whole view was unreadable.
    body = extract_view_body('create view "my view" as select 1 as a')
    assert body == "select 1 as a"


def test_body_found_when_the_name_contains_a_doubled_quote():
    assert extract_view_body('create view "we""ird" as select 1') == "select 1"


def test_as_inside_a_string_literal_in_the_header_is_not_the_header_end():
    # COMMENT = 'x as y' -- a scan for ` as ` finds this first.
    body = extract_view_body(
        "create view v comment = 'defined as a rollup' as select 1 as a")
    assert body == "select 1 as a"


def test_as_inside_a_comment_in_the_header_is_not_the_header_end():
    body = extract_view_body("create view v /* used as a stub */ as select 1")
    assert body == "select 1"


def test_column_alias_as_is_not_mistaken_for_the_header_end():
    body = extract_view_body(
        "create view v (f) as select IFF(a, 'y', 'n') AS f from t")
    assert body == "select IFF(a, 'y', 'n') AS f from t"


def test_quoted_column_list_with_spaces_is_handled():
    body = extract_view_body('create view v ("col one", "col two") as select 1, 2')
    assert body == "select 1, 2"


def test_secure_recursive_and_or_replace_prefixes_are_all_accepted():
    for head in ("create view v",
                 "create or replace view v",
                 "create secure view v",
                 "create or replace secure recursive view v"):
        assert extract_view_body(f"{head} as select 1") == "select 1"


def test_a_statement_that_is_not_a_create_view_is_refused():
    with pytest.raises(ValueError):
        extract_view_body("select 1")


def test_a_create_view_with_no_as_is_refused():
    with pytest.raises(ValueError):
        extract_view_body("create view v")


def test_trailing_semicolon_and_whitespace_are_stripped():
    assert extract_view_body("create view v as select 1 ;  ") == "select 1"


def test_unterminated_literal_in_the_ddl_is_reported():
    with pytest.raises(ValueError):
        extract_view_body("create view v as select 'oops")


# --------------------------------------------------------------------------
# A backslash-escaped quote inside a cast operand used to raise out of the
# translator and abort the whole ddl stage with no view named.
# --------------------------------------------------------------------------

def test_build_create_view_with_escaped_quote_in_cast():
    ddl = "create view V_BAD as select 'don\\'t'::string as w, a from DB.SC.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V_BAD")
    assert res.blocked is False, res.blocked_reason
    assert "CAST('don\\'t' AS" in res.sql
    assert "::" not in res.sql


def test_translator_value_error_blocks_the_view_not_the_payload(monkeypatch):
    import target.ddl as ddl_mod

    def raise_value_error(body):
        raise ValueError("translator could not read this body")

    monkeypatch.setattr(ddl_mod, "translate_view_body", raise_value_error)
    res = build_create_view(view_record(), "D.S.V")
    assert res.blocked is True
    assert "translator could not read this body" in res.blocked_reason


# --- `::` casts go through the type mapper ---------------------------------

def test_view_with_variant_cast_is_blocked_with_the_mapper_reason():
    ddl = "create view V as select p::VARIANT as v from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "VARIANT" in res.blocked_reason


def test_view_cast_warnings_reach_the_rewrite_result():
    ddl = "create view V as select d::TIMESTAMP as ts from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False
    assert any("timezone" in w for w in res.warnings), res.warnings


# --- DATEADD is exact only for DATE operands, and only in its simple form ---

def test_a_view_with_dateadd_is_not_labelled_exact():
    ddl = "create view V as select DATEADD(day, 1, created_ts) as due_ts from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False
    r43 = [r for r in res.rules_applied if r.rule_id == "R43_VIEW_DIALECT_TRANSLATED"]
    assert r43 and "every one is an exact rewrite" not in r43[0].detail, r43
    assert any("TIMESTAMP" in w for w in res.warnings), res.warnings


def test_a_view_with_dateadd_column_hours_is_blocked_with_the_construct_named():
    ddl = "create view V as select DATEADD(hour, n_hours, ts) as x from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "DATEADD" in res.blocked_reason


def test_a_view_with_a_nested_dateadd_is_blocked_not_stamped_portable():
    ddl = ("create view V as select * from D.S.EVENTS "
           "where ts >= DATEADD(day, -30, CURRENT_DATE())")
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "DATEADD" in res.blocked_reason
    assert not any(r.rule_id in ("R42_VIEW_PORTABLE_SQL", "R43_VIEW_DIALECT_TRANSLATED")
                   for r in res.rules_applied)


# --- the constructs table is derived from the rules, not hand-maintained ---

def test_unsupported_constructs_table_is_derived_from_rules():
    from snowflake_source.dialect.translate import RULES
    assert set(UNSUPPORTED_CONSTRUCTS) == {
        r.construct for r in RULES if r.status == "declared"}


def test_datediff_view_is_blocked_end_to_end():
    from plan.build import _view_verdict
    ddl = ("create view V as select DATEDIFF(dd, order_date, ship_date) as d "
           "from D.S.T")
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "DATEDIFF" in res.blocked_reason
    ok, category, reason = _view_verdict(view_record(ddl=ddl))
    assert ok is False and category == "snowflake_only_sql"
    assert "DATEDIFF" in reason


# --- "portable" means only that no known construct matched -----------------

def test_r42_wording_does_not_claim_a_clean_check():
    # GREATEST/LEAST parse on Spark with different NULL handling and SPLIT's
    # separator is a regex there; neither is in the rule table, so the body
    # ships verbatim. The rule text must say that, not "no construct present".
    ddl = "create view V as select GREATEST(a, b) as g, SPLIT(p, '.')[0] as r from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False
    r42 = [r for r in res.rules_applied if r.rule_id == "R42_VIEW_PORTABLE_SQL"]
    assert r42, res.rules_applied
    assert "no known Snowflake-only construct matched" in r42[0].detail
    assert "carried verbatim" in r42[0].detail
    assert "no Snowflake-only construct present" not in r42[0].detail
    assert any("not in the rule table" in w for w in res.warnings), res.warnings


# --- quoted identifiers, string escapes and $$ strings in view bodies -------

def test_view_with_quoted_identifiers_is_backticked_and_its_body_survives():
    from target import ddl as ddl_mod
    ddl = 'create view V as select "Order ID", "we""ird" from DB.SC."Orders"'
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False, res.blocked_reason
    assert "`Order ID`" in res.sql and "DB.SC.`Orders`" in res.sql
    assert '"Order ID"' not in res.sql
    ids = {r.rule_id for r in res.rules_applied}
    assert "T07_QUOTED_IDENTIFIER" in ids and "R43_VIEW_DIALECT_TRANSLATED" in ids
    assert "R42_VIEW_PORTABLE_SQL" not in ids
    # The catalog API takes the body separately; it is re-lexed from the
    # emitted statement and must survive the `we"ird` identifier.
    body = ddl_mod._view_text(res.sql)
    assert body == 'select `Order ID`, `we"ird` from DB.SC.`Orders`'


def test_view_with_a_doubled_quote_literal_is_escaped_for_spark():
    ddl = "create view V as select * from D.S.CUST where last_name = 'O''Brien'"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is False
    assert "'O\\'Brien'" in res.sql
    assert "''Brien" not in res.sql
    ids = {r.rule_id for r in res.rules_applied}
    assert "R43_VIEW_DIALECT_TRANSLATED" in ids and "R42_VIEW_PORTABLE_SQL" not in ids


def test_view_with_a_dollar_quoted_string_is_blocked_naming_it():
    ddl = "create view V as select $$it's$$ as note, id from D.S.T"
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True
    assert "dollar" in res.blocked_reason.lower()


@pytest.mark.parametrize("body", [
    'select "Order ID" from DB.SC."Orders"',
    'select IFF(a, 1, 2) as "F", "we""ird" from t',
    "select 'O''Brien' as who from t",
    'select "c"::int from t',
])
def test_no_double_quoted_identifier_survives_into_emitted_spark_sql(body):
    from snowflake_source.dialect import lexer
    res = build_create_view(view_record(ddl=f"create view V as {body}"), "D.S.V")
    assert res.blocked is False, res.blocked_reason
    emitted = extract_view_body(res.sql)
    assert not any(kind == "ident" and text.startswith('"')
                   for kind, text in lexer.segments(emitted)), emitted


# --------------------------------------------------------------------------
# R41 object-reference rewrite. It used to be a plain re.sub over the whole
# body: it mutated string literals (data, not SQL), missed a reference whose
# parts GET_DDL had quoted, hit a longer sibling name as a substring, and the
# plan then reported a rewrite that had not happened.
# --------------------------------------------------------------------------

PREFIX_MAP = {"MY-DATA-DB.SALES.ORDERS": "lake.my-data-db_sales.orders"}


def _rules(res):
    return {r.rule_id: r.detail for r in res.rules_applied}


def test_string_literal_containing_a_name_is_not_rewritten():
    ddl = ("create view V as select CREATED_AT as D, "
           "'from MY-DATA-DB.SALES.ORDERS' as LBL from \"MY-DATA-DB\".SALES.ORDERS")
    res = build_create_view(view_record(ddl=ddl, source_database="MY-DATA-DB"),
                            "lake.my-data-db_analytics.v", name_map=PREFIX_MAP)
    assert res.blocked is False, res.blocked_reason
    assert "'from MY-DATA-DB.SALES.ORDERS'" in res.sql, "the literal is data"
    assert res.sql.count("lake.`my-data-db_sales`.orders") == 1, res.sql
    assert "R41_VIEW_REFS_REWRITTEN" in _rules(res)


def test_double_quoted_reference_is_rewritten_and_backticked():
    ddl = 'create view V as select o.x from "DB"."S"."ORDERS" o'
    res = build_create_view(view_record(ddl=ddl), "lake.db_s.v",
                            name_map={"DB.S.ORDERS": "lake.db_s.orders"})
    assert res.blocked is False
    assert '"DB"' not in res.sql and "`DB`" not in res.sql
    assert "from lake.db_s.orders o" in res.sql, res.sql
    assert "R41_VIEW_REFS_REWRITTEN" in _rules(res)


def test_quoted_part_matches_exact_case_only():
    # "db" is a different object from DB in Snowflake, so it is not rewritten.
    ddl = 'create view V as select o.x from "db"."S"."ORDERS" o'
    res = build_create_view(view_record(ddl=ddl), "lake.db_s.v",
                            name_map={"DB.S.ORDERS": "lake.db_s.orders"})
    assert "`db`.`S`.`ORDERS`" in res.sql, res.sql
    assert "R41_VIEW_REFS_REWRITTEN" not in _rules(res)


def test_unquoted_reference_still_matches_case_insensitively():
    ddl = "create view V as select o.x from db.s.orders o"
    res = build_create_view(view_record(ddl=ddl), "lake.db_s.v",
                            name_map={"DB.S.ORDERS": "lake.db_s.orders"})
    assert "from lake.db_s.orders o" in res.sql, res.sql


def test_longer_sibling_is_not_a_substring_hit():
    ddl = "create view V as select a.x from DB.S.ORDERS_ARCHIVE a"
    res = build_create_view(
        view_record(ddl=ddl), "lake.db_s.v",
        name_map={"DB.S.ORDERS": "lake.db_s.orders",
                  "DB.S.ORDERS_ARCHIVE": "lake.db_s.orders_archive"})
    assert "lake.db_s.orders_archive" in res.sql, res.sql
    assert "orders_ARCHIVE" not in res.sql
    assert _rules(res)["R41_VIEW_REFS_REWRITTEN"] == (
        "rewrote object references: DB.S.ORDERS_ARCHIVE -> lake.db_s.orders_archive")


def test_sibling_not_in_the_map_is_left_whole_and_no_rewrite_is_reported():
    ddl = "create view V as select a.x from DB.S.ORDERS_ARCHIVE a"
    res = build_create_view(view_record(ddl=ddl), "lake.db_s.v",
                            name_map={"DB.S.ORDERS": "lake.db_s.orders"})
    assert "from DB.S.ORDERS_ARCHIVE a" in res.sql, res.sql
    assert "R41_VIEW_REFS_REWRITTEN" not in _rules(res)
    assert "R40_VIEW_REFS_IDENTITY" in _rules(res)


def test_comment_mentioning_a_name_is_untouched():
    ddl = "create view V as select a.x from DB.S.ORDERS a -- see DB.S.ORDERS\n"
    res = build_create_view(view_record(ddl=ddl), "lake.db_s.v",
                            name_map={"DB.S.ORDERS": "lake.db_s.orders"})
    assert "-- see DB.S.ORDERS" in res.sql, res.sql
    assert "from lake.db_s.orders a" in res.sql


def test_r41_lists_only_the_references_that_were_rewritten():
    ddl = "create view V as select a.x from DB.S.ORDERS a"
    res = build_create_view(
        view_record(ddl=ddl), "lake.db_s.v",
        name_map={"DB.S.ORDERS": "lake.db_s.orders",
                  "DB.S.CUSTOMERS": "lake.db_s.customers"})
    assert _rules(res)["R41_VIEW_REFS_REWRITTEN"] == (
        "rewrote object references: DB.S.ORDERS -> lake.db_s.orders")


def test_rewritten_target_with_a_hyphen_is_backticked_and_parses():
    sqlglot = pytest.importorskip("sqlglot")
    ddl = 'create view V as select o.x from "MY-DATA-DB".SALES.ORDERS o'
    res = build_create_view(view_record(ddl=ddl, source_database="MY-DATA-DB"),
                            "lake.my-data-db_sales.v", name_map=PREFIX_MAP)
    tree = sqlglot.parse_one(res.sql, dialect="spark")
    tables = {t.sql(dialect="spark").split(" AS ")[0]
              for t in tree.find_all(sqlglot.exp.Table)}
    assert "lake.`my-data-db_sales`.orders" in tables, tables


def test_view_with_a_quoted_field_variant_path_is_blocked_not_quoted():
    # T16 used to want an identifier character after the colon, so the quoted
    # field slipped past it; T07 then turned "Field Name" into a backtick
    # identifier and the view was stamped an exact rewrite with a colon path
    # still in the body. The construct is refused, named.
    ddl = 'create view V as select payload:"Field Name" as f from t'
    res = build_create_view(view_record(ddl=ddl), "D.S.V")
    assert res.blocked is True and res.sql is None
    assert "VARIANT path" in res.blocked_reason

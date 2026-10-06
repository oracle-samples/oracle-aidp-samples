"""The translation map: every translation the session made, in one place.

Before this, each view's rewrite lived in its own DDL statement and each
column's type mapping in its own inventory record. Nobody could answer "what
did the translator do to this estate?" without reading every object.
"""
from report.render import render_summary, render_translation_map
from report.translation_map import build_translation_map


def _col(name, dt, target, pos=1, **over):
    c = {"COLUMN_NAME": name, "DATA_TYPE": dt, "target_type": target,
         "ORDINAL_POSITION": pos}
    c.update(over)
    return c


def _estate():
    inv = {"inventory": [
        {"source_identifier": "DB.S.ORDERS", "object_type": "TABLE",
         "columns": [_col("ID", "NUMBER", "DECIMAL(38,0)",
                          NUMERIC_PRECISION=38, NUMERIC_SCALE=0),
                     _col("NOTE", "TEXT", "STRING", 2),
                     _col("AT", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ", 3)]},
        {"source_identifier": "DB.S.CUSTOMERS", "object_type": "TABLE",
         "columns": [_col("NAME", "TEXT", "STRING"),
                     _col("DOC", "VARIANT", None, 2)],
         "compatibility_status": "blocked",
         "blocked_reasons": ["DOC: VARIANT is blocked"]},
        {"source_identifier": "DB.S.V_OK", "object_type": "VIEW",
         "columns": [_col("ID", "NUMBER", "DECIMAL(38,0)")],
         "view_ddl_get_ddl":
             "create or replace view V_OK as select IFF(ID > 0, 1, 0) X, "
             "ID::varchar Y from DB.S.ORDERS;"},
        {"source_identifier": "DB.S.V_BAD", "object_type": "VIEW",
         "columns": [],
         "view_ddl_get_ddl":
             "create or replace view V_BAD as select * from DB.S.ORDERS "
             "qualify row_number() over (order by ID) = 1;"},
    ]}
    plan = {"target_names": {
                "DB.S.ORDERS": "db.s.orders", "DB.S.CUSTOMERS": "db.s.customers",
                "DB.S.V_OK": "db.s.v_ok", "DB.S.V_BAD": "db.s.v_bad"},
            "can_migrate": [{"source_identifier": "DB.S.ORDERS", "object_type": "TABLE"},
                            {"source_identifier": "DB.S.V_OK", "object_type": "VIEW"}],
            "cannot_migrate": [
                {"source_identifier": "DB.S.CUSTOMERS",
                 "category": "unmapped_type"},
                {"source_identifier": "DB.S.V_BAD",
                 "category": "snowflake_only_sql"}]}
    ddl = {"statements": [
        {"source_identifier": "DB.S.ORDERS",
         "rules_applied": [{"rule_id": "R01_TARGET_NAME", "detail": ""},
                           {"rule_id": "R03_TYPE_MAP", "detail": ""},
                           {"rule_id": "R03_TYPE_MAP", "detail": ""}]},
        {"source_identifier": "DB.S.V_OK",
         "rules_applied": [{"rule_id": "R43_VIEW_DIALECT_TRANSLATED",
                            "detail": ""}]}]}
    return inv, plan, ddl


def test_type_mappings_are_aggregated_across_every_object():
    tmap = build_translation_map(*_estate())
    by_src = {t["source_type"]: t for t in tmap["types"]}
    assert by_src["TEXT"]["target_type"] == "STRING"
    assert by_src["TEXT"]["columns"] == 2
    assert sorted(by_src["TEXT"]["objects"]) == ["DB.S.CUSTOMERS", "DB.S.ORDERS"]
    assert by_src["NUMBER(38,0)"]["target_type"] == "DECIMAL(38,0)"


def test_a_type_with_no_target_is_listed_as_unmapped_not_dropped():
    tmap = build_translation_map(*_estate())
    variant = next(t for t in tmap["types"] if t["source_type"] == "VARIANT")
    assert variant["target_type"] is None
    assert variant["status"] == "unmapped"
    assert tmap["totals"]["unmapped_columns"] == 1


def test_a_type_that_changes_name_is_distinguished_from_one_that_is_identical():
    tmap = build_translation_map(*_estate())
    by_src = {t["source_type"]: t for t in tmap["types"]}
    assert by_src["TIMESTAMP_NTZ"]["status"] == "identical"
    assert by_src["TEXT"]["status"] == "mapped"


def test_every_view_body_is_run_through_the_translator():
    tmap = build_translation_map(*_estate())
    rules = {r["rule_id"]: r for r in tmap["dialect_rules"]}
    assert rules["T01_IFF"]["outcome"] == "applied"
    assert rules["T01_IFF"]["objects"] == ["DB.S.V_OK"]
    assert rules["T02_CAST_SHORTHAND"]["outcome"] == "applied"
    assert rules["T10_QUALIFY"]["outcome"] == "refused"
    assert rules["T10_QUALIFY"]["objects"] == ["DB.S.V_BAD"]


def test_a_rule_the_estate_never_exercised_is_still_listed_as_unused():
    tmap = build_translation_map(*_estate())
    rules = {r["rule_id"]: r for r in tmap["dialect_rules"]}
    assert rules["T17_DECODE"]["outcome"] == "not_encountered"
    assert rules["T17_DECODE"]["objects"] == []


def test_names_record_the_case_fold():
    tmap = build_translation_map(*_estate())
    names = {n["source"]: n for n in tmap["names"]}
    assert names["DB.S.ORDERS"]["target"] == "db.s.orders"
    assert names["DB.S.ORDERS"]["case_folded"] is True
    assert names["DB.S.ORDERS"]["verdict"] == "can_migrate"
    assert names["DB.S.V_BAD"]["verdict"] == "cannot_migrate: snowflake_only_sql"


def test_ddl_rule_counts_are_totalled_for_the_session():
    tmap = build_translation_map(*_estate())
    assert tmap["ddl_rules"]["R03_TYPE_MAP"] == 2
    assert tmap["ddl_rules"]["R43_VIEW_DIALECT_TRANSLATED"] == 1


def test_totals():
    t = build_translation_map(*_estate())["totals"]
    assert t["objects"] == 4
    assert t["columns"] == 6
    assert t["views_translated"] == 1
    assert t["views_refused"] == 1
    assert t["views_verbatim"] == 0


def test_the_map_renders_every_section():
    md = render_translation_map(build_translation_map(*_estate()))
    for heading in ("# Translation map", "## Types", "## Dialect rules",
                    "## Names", "## DDL rules"):
        assert heading in md
    assert "`VARIANT`" in md and "**unmapped**" in md
    assert "T10_QUALIFY" in md


def test_the_map_works_before_ddl_has_run():
    inv, plan, _ = _estate()
    tmap = build_translation_map(inv, plan, None)
    assert tmap["ddl_rules"] == {}
    assert tmap["types"]


def test_the_summary_carries_the_translation_map_when_given():
    inv, plan, ddl = _estate()
    tmap = build_translation_map(inv, plan, ddl)
    md = render_summary(plan, inv, None, None, translation_map=tmap)
    assert "## Translation map" in md
    assert "T01_IFF" in md


def test_the_summary_is_unchanged_without_one():
    inv, plan, _ = _estate()
    md = render_summary(plan, inv, None, None)
    assert "## Translation map" not in md


# ------------------------------- A8: every rename is listed and printed

def _prefixed():
    inv = {"inventory": [
        {"source_identifier": "DB.COMMERCE.ORDERS", "object_type": "TABLE",
         "source_database": "DB", "source_schema": "COMMERCE",
         "columns": [_col("ID", "NUMBER", "DECIMAL(38,0)")]},
        {"source_identifier": "DB.COMMERCE." + "T" * 251,
         "object_type": "TABLE", "source_database": "DB",
         "source_schema": "COMMERCE", "columns": []}]}
    plan = {"target_names": {
                "DB.COMMERCE.ORDERS": "cat.db_commerce.orders",
                "DB.COMMERCE." + "T" * 251: "cat.db_commerce." + "t" * 251},
            "can_migrate": [{"source_identifier": "DB.COMMERCE.ORDERS",
                             "object_type": "TABLE"}],
            "cannot_migrate": [{"source_identifier": "DB.COMMERCE." + "T" * 251,
                                "category": "target_key_too_long"}]}
    return inv, plan


def test_schema_and_catalog_renames_are_in_the_map():
    tmap = build_translation_map(*_prefixed(), None)
    schemas = {s["source"]: s for s in tmap["schemas"]}
    assert schemas["DB.COMMERCE"]["target"] == "cat.db_commerce"
    assert schemas["DB.COMMERCE"]["renamed"] is True
    assert tmap["catalogs"] == [{"source": "DB", "target": "cat",
                                 "renamed": True}]


def test_a_key_refused_for_length_is_listed_with_its_verdict():
    tmap = build_translation_map(*_prefixed(), None)
    long = next(n for n in tmap["names"] if n["source"].endswith("T" * 251))
    assert long["verdict"] == "cannot_migrate: target_key_too_long"
    assert tmap["totals"]["refused_for_length"] == 1


def test_every_rename_is_printed_in_the_map_and_the_summary():
    inv, plan = _prefixed()
    tmap = build_translation_map(inv, plan, None)
    md = render_translation_map(tmap)
    assert "## Catalogs and schemas" in md and "`DB.COMMERCE`" in md
    assert "`cat.db_commerce`" in md
    section = "\n".join(__import__("report.render", fromlist=["x"])
                        .translation_map_section(tmap))
    assert "`DB.COMMERCE` → `cat.db_commerce`" in section
    assert "target_key_too_long" in section

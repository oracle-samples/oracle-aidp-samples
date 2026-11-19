"""Spark/Delta DDL generation with a per-rule audit trail. Pure."""
import pytest

from target import ddl

from target.ddl import (
    DEFERRED_EQUIVALENT_PROPERTIES, RewriteResult, SCRUBBED_PROPERTIES, build_create_schema, build_create_table,
    build_create_view, quote_spark_string,
)


def col(name, dt, target, *, nullable="YES", pos=1, comment=None,
        precision=None, scale=None):
    return {"COLUMN_NAME": name, "DATA_TYPE": dt, "target_type": target,
            "IS_NULLABLE": nullable, "ORDINAL_POSITION": pos, "COMMENT": comment,
            "NUMERIC_PRECISION": precision, "NUMERIC_SCALE": scale}


def record(columns, **over):
    r = {"source_identifier": "D.PUBLIC.ORDERS", "object_type": "TABLE",
         "source_database": "D", "source_schema": "PUBLIC",
         "compatibility_status": "supported", "blocked_reasons": [],
         "columns": columns, "source_metadata": {}}
    r.update(over)
    return r


def test_minimal_table_sql():
    res = build_create_table(
        record([col("ORDER_ID", "NUMBER", "DECIMAL(38,0)", nullable="NO"),
                col("PAID", "NUMBER", "DECIMAL(18,2)", pos=2)]),
        "bronze.PUBLIC.ORDERS")
    assert isinstance(res, RewriteResult)
    assert res.sql == (
        "CREATE TABLE IF NOT EXISTS `bronze`.`PUBLIC`.`ORDERS` (\n"
        "  `ORDER_ID` DECIMAL(38,0) NOT NULL,\n"
        "  `PAID` DECIMAL(18,2)\n"
        ")\nUSING DELTA")
    assert res.blocked is False


def test_never_emits_create_or_replace():
    res = build_create_table(record([col("A", "TEXT", "STRING")]), "bronze.S.T")
    assert "OR REPLACE" not in res.sql
    assert "IF NOT EXISTS" in res.sql


def test_columns_ordered_by_ordinal_position():
    res = build_create_table(
        record([col("SECOND", "TEXT", "STRING", pos=2),
                col("FIRST", "TEXT", "STRING", pos=1)]), "bronze.S.T")
    assert res.sql.index("`FIRST`") < res.sql.index("`SECOND`")


def test_column_comment_emitted_and_escaped():
    # Backslash, not doubling. This test previously asserted `'it''s fine'`,
    # which Spark reads as two adjacent literals and concatenates -- so the
    # comment silently became "its fine".
    res = build_create_table(
        record([col("A", "TEXT", "STRING", comment="it's fine")]), "bronze.S.T")
    assert r"COMMENT 'it\'s fine'" in res.sql


def test_rules_recorded_with_ids():
    res = build_create_table(
        record([col("A", "NUMBER", "DECIMAL(5,2)", precision=5, scale=2)]),
        "bronze.S.T")
    ids = {r.rule_id for r in res.rules_applied}
    assert {"R01_TARGET_NAME", "R02_QUOTE_BACKTICK", "R30_USING_DELTA"} <= ids
    assert any(r.rule_id == "R03_TYPE_MAP" and "DECIMAL(5,2)" in r.detail
               for r in res.rules_applied)


@pytest.mark.parametrize("prop", ["is_iceberg", "is_dynamic", "is_secure",
                                  "max_data_extension_time_in_days"])
def test_snowflake_properties_scrubbed_and_recorded(prop):
    """Only properties with genuinely NO AIDP equivalent are 'dropped'."""
    res = build_create_table(
        record([col("A", "TEXT", "STRING")], source_metadata={prop: "something"}),
        "bronze.S.T")
    assert any(prop in o for o in res.omitted_properties)
    assert prop not in res.sql
    assert prop in SCRUBBED_PROPERTIES


@pytest.mark.parametrize("prop,value", [("cluster_by", "LINEAR(A)"),
                                        ("retention_time", "90"),
                                        ("change_tracking", "ON")])
def test_maintenance_properties_are_carried_not_dropped(prop, value):
    """These three DO have AIDP equivalents.

    Reporting them as "no Delta equivalent" was wrong, and wrong in the
    direction that costs the customer: a dropped clustering key is a silent
    performance regression on the largest tables in the estate. Since the
    live Delta 3.1 probe (2026-09-29) each is CARRIED into the CREATE TABLE
    (test_delta_features.py); it was "deferred" before that.
    """
    res = build_create_table(
        record([col("A", "TEXT", "STRING")], source_metadata={prop: value}),
        "bronze.S.T")
    assert not any(prop in o for o in res.omitted_properties)
    assert any(c["property"] == prop for c in res.carried_properties)
    assert prop not in SCRUBBED_PROPERTIES
    assert prop in DEFERRED_EQUIVALENT_PROPERTIES


def test_a_value_ddl_cannot_carry_is_still_deferred_not_dropped():
    res = build_create_table(
        record([col("A", "TEXT", "STRING")],
               source_metadata={"cluster_by": "something"}),
        "bronze.S.T")
    assert any(d["property"] == "cluster_by" for d in res.deferred_properties)


def test_blocked_record_produces_no_sql():
    res = build_create_table(
        record([col("P", "VARIANT", None)], compatibility_status="blocked",
               blocked_reasons=["P: VARIANT semi-structured"]),
        "bronze.S.T")
    assert res.blocked is True
    assert res.sql is None
    assert "VARIANT" in res.blocked_reason


def test_column_with_no_target_type_blocks_the_table():
    res = build_create_table(record([col("A", "TEXT", "STRING"),
                                     col("P", "GEOGRAPHY", None, pos=2)]),
                             "bronze.S.T")
    assert res.blocked is True
    assert "GEOGRAPHY" in res.blocked_reason


def test_table_with_no_columns_is_blocked():
    res = build_create_table(record([]), "bronze.S.T")
    assert res.blocked is True
    assert "no columns" in res.blocked_reason.lower()


def test_constraints_are_reported_not_emitted():
    res = build_create_table(
        record([col("A", "NUMBER", "DECIMAL(38,0)", nullable="NO")],
               constraints=[{"type": "PRIMARY KEY", "columns": ["A"]}]),
        "bronze.S.T")
    assert "PRIMARY KEY" not in res.sql
    assert any(r.rule_id == "R20_CONSTRAINTS_NOT_EMITTED" for r in res.rules_applied)


def test_create_schema_never_emits_comment():
    # AIDP silently fails to persist CREATE SCHEMA ... COMMENT, and ISO-timestamp
    # colons in the comment are the specific trigger. Never emit it.
    sql = build_create_schema("bronze", "PUBLIC")
    assert sql == "CREATE SCHEMA IF NOT EXISTS `bronze`.`PUBLIC`"
    assert "COMMENT" not in sql


def test_target_fqn_must_be_three_part():
    with pytest.raises(ValueError, match="three-part"):
        build_create_table(record([col("A", "TEXT", "STRING")]), "bronze.ORDERS")


@pytest.mark.parametrize("prop,value", [
    ("rows", 42), ("bytes", 2048), ("created_on", "2026-09-09"),
    ("owner", "ACCOUNTADMIN"),
])
def test_informational_metadata_is_not_reported_as_a_dropped_property(prop, value):
    # These are SHOW observations, not source-side settings. Listing them as
    # "dropped properties with no Delta equivalent" is misleading noise.
    res = build_create_table(
        record([col("A", "TEXT", "STRING")], source_metadata={prop: value}),
        "bronze.S.T")
    assert res.omitted_properties == []


@pytest.mark.parametrize("value", [None, "", "N", "OFF", "false"])
def test_unset_properties_are_not_reported(value):
    res = build_create_table(
        record([col("A", "TEXT", "STRING")],
               source_metadata={"change_tracking": value}),
        "bronze.S.T")
    assert res.omitted_properties == []


def test_a_set_property_is_still_reported():
    res = build_create_table(
        record([col("A", "TEXT", "STRING")],
               source_metadata={"cluster_by": "(COUNTRY_CODE)", "rows": 5}),
        "bronze.S.T")
    # Reported, but as a deferral with its equivalent -- not as a dead loss.
    assert res.omitted_properties == []
    assert res.deferred_properties[0]["property"] == "cluster_by"
    assert res.deferred_properties[0]["value"] == "(COUNTRY_CODE)"


# ==========================================================================
# Spark string literals escape with a BACKSLASH, not by doubling.
#
# Found by parsing generated DDL with a real Spark parser. `'it''s fine'` is
# not an escaped quote in Spark -- it is two adjacent literals, which Spark
# concatenates. A column comment of "Customer's orders" therefore became
# "Customers orders", silently, or failed to parse depending on position.
# ==========================================================================

def test_spark_string_escapes_with_a_backslash():
    assert quote_spark_string("it's fine") == r"'it\'s fine'"


def test_spark_string_escapes_backslashes_first():
    assert quote_spark_string(r"a\b") == r"'a\\b'"


def test_spark_string_leaves_plain_text_alone():
    assert quote_spark_string("plain") == "'plain'"


def test_a_column_comment_with_an_apostrophe_is_backslash_escaped():
    rec = {"source_identifier": "DB.SC.T", "object_type": "TABLE",
           "source_metadata": {},
           "columns": [{"COLUMN_NAME": "A", "target_type": "STRING",
                        "ORDINAL_POSITION": 1, "IS_NULLABLE": "YES",
                        "DATA_TYPE": "TEXT", "COMMENT": "Customer's orders"}]}
    sql = build_create_table(rec, "CAT.SC.T").sql
    assert r"\'" in sql
    assert "''" not in sql, "doubling is two literals in Spark, not an escape"


# ==========================================================================
# Maintenance and layout properties (vacuum / optimize question).
#
# `cluster_by`, `retention_time` and `change_tracking` were all reported as
# "dropped Snowflake properties with no Delta equivalent". That is FALSE for
# all three -- Delta has liquid clustering / ZORDER, deletedFileRetention +
# logRetention, and Change Data Feed. Telling a customer their clustering key
# has no equivalent invites them to accept a silent performance regression.
# ==========================================================================

def _with_props(**props):
    return {"source_identifier": "DB.SC.T", "object_type": "TABLE",
            "source_metadata": props,
            "columns": [{"COLUMN_NAME": "A", "target_type": "STRING",
                         "ORDINAL_POSITION": 1, "IS_NULLABLE": "YES",
                         "DATA_TYPE": "TEXT", "COMMENT": None}]}


def test_a_clustering_key_is_deferred_not_declared_equivalent_free():
    res = build_create_table(_with_props(cluster_by="(ORDER_DATE, STORE_ID)"),
                             "CAT.SC.T")
    assert not any("cluster_by" in o for o in res.omitted_properties), \
        "a clustering key HAS an AIDP equivalent"
    deferred = {d["property"]: d for d in res.deferred_properties}
    assert "cluster_by" in deferred
    assert deferred["cluster_by"]["value"] == "(ORDER_DATE, STORE_ID)"
    eq = deferred["cluster_by"]["aidp_equivalent"].upper()
    assert "CLUSTER BY" in eq or "ZORDER" in eq


def test_time_travel_retention_is_carried_with_its_delta_equivalent():
    # 7 days: Delta's own defaults already reach that far, so it is carried
    # by emitting nothing (see test_delta_features.py for longer ones).
    res = build_create_table(_with_props(retention_time=7), "CAT.SC.T")
    carried = {c["property"]: c for c in res.carried_properties}
    assert "retention_time" in carried
    assert "retention" in carried["retention_time"]["carried_as"].lower()


def test_change_tracking_maps_to_change_data_feed():
    res = build_create_table(_with_props(change_tracking="ON"), "CAT.SC.T")
    carried = {c["property"]: c for c in res.carried_properties}
    assert "change_tracking" in carried
    assert "delta.enableChangeDataFeed" in res.sql


def test_a_property_with_genuinely_no_equivalent_is_still_omitted():
    res = build_create_table(_with_props(is_iceberg="Y"), "CAT.SC.T")
    assert any("is_iceberg" in o for o in res.omitted_properties)
    assert not any(d["property"] == "is_iceberg" for d in res.deferred_properties)


def test_unset_maintenance_properties_are_not_reported():
    # cluster_by is '' on an unclustered table -- reporting that as a deferred
    # decision would be noise on every table in the estate.
    res = build_create_table(_with_props(cluster_by="", change_tracking="OFF"),
                             "CAT.SC.T")
    assert res.deferred_properties == []


def test_the_deferral_is_recorded_as_a_named_rule():
    # An expression key cannot be a liquid clustering key: still deferred.
    res = build_create_table(_with_props(cluster_by="(TO_DATE(A))"), "CAT.SC.T")
    assert any(r.rule_id == "R11_MAINTENANCE_DEFERRED" for r in res.rules_applied)


def test_no_maintenance_job_is_emitted():
    # The maintenance CADENCE is the customer's, so no OPTIMIZE, VACUUM or
    # ZORDER is invented. The table's own layout setting is a different
    # thing: a plain clustering key is carried as CLUSTER BY, which Delta
    # 3.1 on AIDP accepted live (2026-09-29).
    res = build_create_table(_with_props(cluster_by="(A)"), "CAT.SC.T")
    up = res.sql.upper()
    for banned in ("OPTIMIZE", "VACUUM", "ZORDER"):
        assert banned not in up
    assert "CLUSTER BY (A)" in res.sql


# --- types the TARGET refuses, caught offline -------------------------------

def test_timestamp_ntz_is_flagged_because_the_metastore_refuses_it():
    """Delta and Spark 3.4+ support TIMESTAMP_NTZ; the Hive metastore behind
    the AIDP catalog does not, and says so only at CREATE TABLE -- five to six
    minutes of cluster startup into a job run, inside a Java traceback."""
    stmts = [{"target_fqn": "c.s.t", "source_identifier": "DB.S.T",
              "sql": "CREATE TABLE `c`.`s`.`t` (\n"
                     "  `ID` DECIMAL(38,0),\n"
                     "  `EVENT_AT` TIMESTAMP_NTZ,\n"
                     "  `CREATED_AT` TIMESTAMP_NTZ\n) USING DELTA"}]
    found = ddl.unsupported_target_types(stmts)
    assert len(found) == 1
    assert found[0]["columns"] == ["EVENT_AT", "CREATED_AT"]
    assert "--timestamp-ntz timestamp" in found[0]["remedy"]
    # The remedy must offer the OFFLINE re-map first: re-reading Snowflake
    # to flip a mapping choice is warehouse time nobody needs to spend.
    assert "ddl --timestamp-ntz timestamp" in found[0]["remedy"]
    assert "SEMANTIC DOWNGRADE" in found[0]["remedy"]


def test_the_downgraded_plan_is_not_flagged():
    stmts = [{"target_fqn": "c.s.t", "source_identifier": "DB.S.T",
              "sql": "CREATE TABLE `c`.`s`.`t` (`EVENT_AT` TIMESTAMP) "
                     "USING DELTA"}]
    assert ddl.unsupported_target_types(stmts) == []


def test_the_type_named_in_a_rule_note_is_not_a_false_positive():
    """The mapping note legitimately says `TIMESTAMP_NTZ -> TIMESTAMP`. Only a
    column DECLARATION counts."""
    stmts = [{"target_fqn": "c.s.t", "source_identifier": "DB.S.T",
              "sql": "-- R03_TYPE_MAP EVENT_AT: TIMESTAMP_NTZ -> TIMESTAMP\n"
                     "CREATE TABLE `c`.`s`.`t` (`EVENT_AT` TIMESTAMP) "
                     "USING DELTA"}]
    assert ddl.unsupported_target_types(stmts) == []


def test_the_payload_carries_the_verdict_so_no_caller_can_forget_to_ask():
    stmts = [{"target_fqn": "c.s.t", "source_identifier": "DB.S.T",
              "sql": "CREATE TABLE `c`.`s`.`t` (`A` TIMESTAMP_NTZ) USING DELTA"}]
    assert ddl.unsupported_target_types(stmts)[0]["type"] == "TIMESTAMP_NTZ"


# --- the NTZ gate must see every column the emitter writes -------------------

def _ntz_table(*names):
    cols = [col(n, "TIMESTAMP_NTZ", "TIMESTAMP_NTZ", pos=i)
            for i, n in enumerate(names, 1)]
    return build_create_table(record(cols), "c.s.t")


def _stmt(res, with_columns=True):
    st = {"target_fqn": res.target_fqn, "source_identifier": res.source_identifier,
          "object_type": "TABLE", "sql": res.sql}
    if with_columns:
        st["expected_columns"] = res.expected_columns
    return st


def test_ntz_gate_catches_quoted_identifiers_with_spaces_and_punctuation():
    # The gate anchored on `(\w+)`, which cannot match the names the emitter
    # itself backticks: a plan whose only NTZ columns were so named passed the
    # gate and failed per table on the cluster instead.
    res = _ntz_table("order date", "order-ts", "a.b", "ok_col")
    found = ddl.unsupported_target_types([_stmt(res)])
    assert len(found) == 1
    assert found[0]["columns"] == ["order date", "order-ts", "a.b", "ok_col"]


def test_ntz_gate_reports_the_full_name_of_a_column_with_an_embedded_backtick():
    res = _ntz_table("we`ird")
    assert ddl.unsupported_target_types([_stmt(res)])[0]["columns"] == ["we`ird"]
    # And through the sql-only fallback, for callers that pass raw SQL.
    assert ddl.unsupported_target_types(
        [_stmt(res, with_columns=False)])[0]["columns"] == ["we`ird"]


def test_ntz_gate_fallback_regex_matches_the_emitter_quoting():
    stmts = [{"target_fqn": "c.s.t", "source_identifier": "DB.S.T",
              "sql": "CREATE TABLE `c`.`s`.`t` (`order date` TIMESTAMP_NTZ) USING DELTA"}]
    found = ddl.unsupported_target_types(stmts)
    assert [f["columns"] for f in found] == [["order date"]]


def test_ntz_gate_ignores_views_even_when_their_source_columns_are_ntz():
    # A view declares no types; its expected_columns carry the SOURCE types.
    stmts = [{"target_fqn": "c.s.v", "source_identifier": "DB.S.V",
              "object_type": "VIEW",
              "sql": "CREATE VIEW IF NOT EXISTS `c`.`s`.`v` AS select ts from t",
              "expected_columns": [{"name": "ts", "type": "TIMESTAMP_NTZ"}]}]
    assert ddl.unsupported_target_types(stmts) == []


def test_ntz_gate_fires_through_the_payload_for_a_space_named_column():
    inv = {"inventory": [record(
        [col("order date", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ")],
        source_identifier="D.PUBLIC.T")]}
    plan = {"waves": [["D.PUBLIC.T"]], "clone_targets": ["D.PUBLIC.T"],
            "target_names": {"D.PUBLIC.T": "c.s.t"}}
    payload = ddl.build_ddl_payload(inv, plan)
    assert payload["target_rejected"], "the HALT gate must fire"
    assert payload["target_rejected"][0]["columns"] == ["order date"]


# --- members of a dependency cycle are not emitted ---------------------------

def _view(ident, view_ddl):
    db, schema, _name = ident.split(".")
    return {"source_identifier": ident, "object_type": "VIEW",
            "source_database": db, "source_schema": schema,
            "view_ddl_get_ddl": view_ddl, "source_metadata": {}, "columns": [],
            "compatibility_status": "supported"}


def _cyclic_estate():
    # PLANNED_OBJECTS.md says cycle members are "excluded from the ordering;
    # they need a human decision". The ddl stage used to re-add every clone
    # target that was not in a wave -- which is exactly the cycle members -- so
    # both artifacts described the same objects in opposite ways and deploy
    # attempted them.
    inv = {"inventory": [
        record([col("A", "NUMBER", "DECIMAL(38,0)")], source_identifier="D.S.T"),
        _view("D.S.OK_VW", "create view OK_VW as select a from D.S.T"),
        _view("D.S.A_VW", "create view A_VW as select a from D.S.B_VW"),
        _view("D.S.B_VW", "create view B_VW as select a from D.S.A_VW"),
    ]}
    ids = ["D.S.T", "D.S.OK_VW", "D.S.A_VW", "D.S.B_VW"]
    plan = {"waves": [["D.S.T"], ["D.S.OK_VW"]],
            "cycles": [["D.S.A_VW", "D.S.B_VW"]],
            "clone_targets": sorted(ids),
            "target_names": {i: i.lower() for i in ids}}
    return inv, plan


def test_cycle_members_are_not_emitted_as_ddl():
    inv, plan = _cyclic_estate()
    payload = ddl.build_ddl_payload(inv, plan)
    assert [s["source_identifier"] for s in payload["statements"]] == [
        "D.S.T", "D.S.OK_VW"]
    assert not any("_vw" in s["target_fqn"] and "ok_vw" not in s["target_fqn"]
                   for s in payload["statements"])


def test_cycle_members_are_listed_as_not_emitted_naming_the_other_member():
    inv, plan = _cyclic_estate()
    payload = ddl.build_ddl_payload(inv, plan)
    blocked = {b["source_identifier"]: b for b in payload["blocked"]}
    assert set(blocked) == {"D.S.A_VW", "D.S.B_VW"}
    assert blocked["D.S.A_VW"]["reason"].startswith("not emitted: dependency cycle")
    assert "D.S.B_VW" in blocked["D.S.A_VW"]["reason"]
    assert "PLANNED_OBJECTS.md" in blocked["D.S.A_VW"]["reason"]
    assert blocked["D.S.A_VW"]["object_type"] == "VIEW"


def test_ddl_plan_report_lists_cycle_members_under_blocked_not_as_statements():
    from report.render import render_ddl_plan
    inv, plan = _cyclic_estate()
    text = render_ddl_plan(ddl.build_ddl_payload(inv, plan))
    assert "2 statement(s)" in text
    assert "## `D.S.A_VW`" not in text and "## `D.S.B_VW`" not in text
    assert "not emitted: dependency cycle" in text


def test_a_clone_target_outside_the_waves_but_not_in_a_cycle_is_still_emitted():
    # The fallback for a plan without waves stays; only cycle members are held.
    inv, plan = _cyclic_estate()
    plan["waves"] = [["D.S.T"]]
    payload = ddl.build_ddl_payload(inv, plan)
    assert "D.S.OK_VW" in [s["source_identifier"] for s in payload["statements"]]
    assert {b["source_identifier"] for b in payload["blocked"]} == {
        "D.S.A_VW", "D.S.B_VW"}


# ------------------------------- an unqualified reference the target cannot
#                                 resolve
#
# Live 2026-09-22, root cause of four failed view creates. Snowflake's
# GET_DDL emits a view body that references its own schema unqualified --
# `select ... from ORDERS`. The rewriter only ever matched three-part names,
# so the reference travelled verbatim, and the AIDP catalog API answered
# `500 InternalError` with no detail. Proven directly against the live
# DataLake: a view whose body says `from orders` returns 500, and the same
# view with `from <catalog>.<schema>.orders` is created ACTIVE.
#
# In Snowflake a view's unqualified reference resolves in the VIEW'S OWN
# schema, so the target name is not a guess.

def _view_rec(sql, ident="DB.SALES.V1"):
    db, schema, _ = ident.split(".")
    return {"source_identifier": ident, "object_type": "VIEW",
            "source_database": db, "source_schema": schema,
            "source_metadata": {}, "view_ddl_get_ddl": sql,
            "columns": [{"name": "A", "type": "NUMBER"}]}


_MAP = {"DB.SALES.V1": "lake.db_sales.v1",
        "DB.SALES.ORDERS": "lake.db_sales.orders"}


def test_an_unqualified_from_is_qualified_to_the_target():
    res = build_create_view(
        _view_rec("create view V1 as select A from ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert not res.blocked, res.blocked_reason
    assert "lake.db_sales.orders" in res.sql
    assert "R41_VIEW_REFS_REWRITTEN" in [r.rule_id for r in res.rules_applied]


def test_an_unqualified_join_is_qualified_too():
    res = build_create_view(
        _view_rec("create view V1 as select A from ORDERS o "
                  "join ORDERS p on o.A = p.A"),
        "lake.db_sales.v1", _MAP)
    assert res.sql.lower().count("lake.db_sales.orders") == 2


def test_a_two_part_reference_is_qualified():
    res = build_create_view(
        _view_rec("create view V1 as select A from SALES.ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert "lake.db_sales.orders" in res.sql
    assert "sales.orders" not in res.sql.lower().replace(
        "lake.db_sales.orders", "")


def test_a_column_that_shares_a_table_name_is_not_rewritten():
    """The whole reason a bare name is only rewritten after FROM or JOIN."""
    res = build_create_view(
        _view_rec("create view V1 as select ORDERS from DB.SALES.ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert "select ORDERS" in res.sql or "select orders" in res.sql.lower()[:60]
    assert res.sql.lower().count("lake.db_sales.orders") == 1


def test_a_name_inside_a_string_literal_is_left_alone():
    res = build_create_view(
        _view_rec("create view V1 as select 'from ORDERS' as note "
                  "from ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert "'from ORDERS'" in res.sql
    assert res.sql.lower().count("lake.db_sales.orders") == 1


def test_a_three_part_reference_still_works():
    res = build_create_view(
        _view_rec("create view V1 as select A from DB.SALES.ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert "lake.db_sales.orders" in res.sql


def test_an_unqualified_reference_with_no_mapping_is_warned_about():
    """It cannot be qualified -- the object is not in the migration -- and
    on the catalog-API transport it is what a 500 looks like."""
    res = build_create_view(
        _view_rec("create view V1 as select A from SOMETHING_ELSE"),
        "lake.db_sales.v1", _MAP)
    assert any("SOMETHING_ELSE" in w for w in res.warnings), res.warnings
    assert any("unqualified" in w.lower() for w in res.warnings)


def test_a_fully_qualified_body_records_no_leftover_warning():
    res = build_create_view(
        _view_rec("create view V1 as select A from DB.SALES.ORDERS"),
        "lake.db_sales.v1", _MAP)
    assert not [w for w in res.warnings if "unqualified" in w.lower()]

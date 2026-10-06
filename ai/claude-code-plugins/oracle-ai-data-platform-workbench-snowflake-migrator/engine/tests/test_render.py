"""Artifact -> markdown. Pure."""
from report.render import render_ddl_plan, render_inventory

INV = {
    "probed_at": "2026-09-09T00:00:00+00:00",
    "session": {"A": "TESTACCT01", "R": "AWS_US_EAST_2", "ROLE": "ACCOUNTADMIN"},
    "databases_in_scope": ["MYDB"], "object_count": 2,
    "counts_by_type": {"TABLE": 1, "VIEW": 1},
    "identifier_case_collisions": {}, "extraction_notes": [],
    "inventory": [
        {"source_identifier": "MYDB.PUBLIC.ORDERS", "object_type": "TABLE",
         "row_count_exact": 100, "source_metadata": {"bytes": 9728},
         "identifier_case_form": "UPPER_UNQUOTED",
         "compatibility_status": "supported", "columns": [1, 2, 3]},
        {"source_identifier": "MYDB.PUBLIC.ORDERS_VW", "object_type": "VIEW",
         "row_count_exact": 100, "source_metadata": {},
         "identifier_case_form": "UPPER_UNQUOTED",
         "compatibility_status": "requires_manual_design", "columns": [1]},
    ],
}


def test_inventory_lists_objects_with_exact_row_counts():
    # Exact mode, stated as such: the fixture says how the numbers were got.
    exact = {**INV, "row_count_mode": "exact",
             "inventory": [{**r, "row_count_source": "count_query"}
                           for r in INV["inventory"]]}
    md = render_inventory(exact)
    assert "MYDB.PUBLIC.ORDERS" in md
    assert "100" in md
    assert "Rows (exact)" in md, "must label counts as exact, not estimated"
    assert "count(*)" in md.lower()


def _row(md: str, ident: str) -> str:
    return next(l for l in md.splitlines() if f"`{ident}`" in l)


def test_inventory_metadata_mode_never_claims_exact():
    # The default mode. The numbers are SHOW estimates and the views were
    # deliberately not counted; neither is an error and neither is exact.
    inv = {**INV, "row_count_mode": "metadata", "inventory": [
        {**INV["inventory"][0], "row_count_source": "show_metadata"},
        {**INV["inventory"][1], "row_count_exact": None,
         "row_count_source": "not_counted",
         "row_count_note": "not counted: a view has no stored row count, so "
                           "counting it means executing the view"}]}
    md = render_inventory(inv)
    header = md.split("| Object |", 1)[0].lower()
    assert "are **exact**" not in header and "in-session" not in header
    assert "not" in header and "verified" in header, \
        "must say plainly that a metadata count is not a verified one"
    assert "Rows (metadata)" in md
    assert "ERROR" not in md
    assert "not counted" in _row(md, "MYDB.PUBLIC.ORDERS_VW")
    assert md.count("counting it means executing the view") == 1


def test_inventory_none_mode_labels_not_requested():
    inv = {**INV, "row_count_mode": "none", "inventory": [
        {**r, "row_count_exact": None, "row_count_source": "not_counted",
         "row_count_note": "row counts were not requested"}
        for r in INV["inventory"]]}
    md = render_inventory(inv)
    assert "ERROR" not in md
    assert "not requested" in md
    header = md.split("| Object |", 1)[0].lower()
    assert "are **exact**" not in header and "in-session" not in header


def test_inventory_count_error_is_the_only_thing_called_error():
    inv = {**INV, "row_count_mode": "exact", "inventory": [
        {**INV["inventory"][0], "row_count_source": "count_query"},
        {**INV["inventory"][1], "row_count_exact": None,
         "row_count_source": "error",
         "row_count_note": "No active warehouse selected"}]}
    md = render_inventory(inv)
    assert "ERROR" in _row(md, "MYDB.PUBLIC.ORDERS_VW")
    assert "ERROR" not in _row(md, "MYDB.PUBLIC.ORDERS")
    assert "No active warehouse selected" in md


def test_inventory_record_without_provenance_renders_dash():
    # An inventory.json written before row_count_source existed: a blank is
    # a blank, not a named error.
    inv = {**INV, "inventory": [
        {**INV["inventory"][1], "row_count_exact": None}]}
    md = render_inventory(inv)
    row = _row(md, "MYDB.PUBLIC.ORDERS_VW")
    assert "ERROR" not in row
    assert "| - |" in row


def test_inventory_shows_the_type_breakdown():
    md = render_inventory(INV)
    assert "TABLE" in md and "VIEW" in md


def test_collisions_rendered_as_a_halt_not_a_footnote():
    md = render_inventory({**INV, "identifier_case_collisions":
                           {"A.B.C": ["A.B.C", "A.B.c"]}})
    assert "HALT" in md.upper()
    assert "A.B.c" in md


def test_extraction_notes_surfaced_when_present():
    md = render_inventory({**INV, "extraction_notes": ["MYDB.X: denied"]})
    assert "denied" in md


def test_ddl_plan_shows_sql_rules_and_omissions():
    md = render_ddl_plan({"statements": [
        {"source_identifier": "MYDB.PUBLIC.ORDERS",
         "target_fqn": "bronze.PUBLIC.ORDERS",
         "sql": "CREATE TABLE IF NOT EXISTS `bronze`.`PUBLIC`.`ORDERS` (`A` STRING)",
         "rules_applied": [{"rule_id": "R03_TYPE_MAP", "detail": "A: TEXT -> STRING"}],
         "warnings": [], "omitted_properties": ["cluster_by=X"]}],
        "blocked": [{"source_identifier": "MYDB.PUBLIC.J", "reason": "VARIANT"}]})
    assert "CREATE TABLE IF NOT EXISTS" in md
    assert "R03_TYPE_MAP" in md
    assert "cluster_by=X" in md
    assert "VARIANT" in md




def test_the_catalog_dry_run_lists_the_connection_field_names():
    """The command promises CATALOG.md shows which properties would be sent.

    The NAMES are what a human checks against their deployment and carry no
    secret; the values must never appear.
    """
    from report.render import render_catalog
    md = render_catalog({
        "dry_run": True, "catalog": "snowcat", "source_type": "SNOWFLAKE",
        "connection_fields": ["SNOWFLAKE_HOST", "SNOWFLAKE_USERNAME",
                              "SNOWFLAKE_PRIVATE_KEY_CONTENT"]})
    assert "SNOWFLAKE_HOST" in md
    assert "SNOWFLAKE_PRIVATE_KEY_CONTENT" in md
    assert "nothing was created" in md


def test_the_catalog_dry_run_says_so_when_no_config_was_given():
    from report.render import render_catalog
    md = render_catalog({"dry_run": True, "catalog": "c",
                         "source_type": "SNOWFLAKE"})
    assert "No connection config" in md

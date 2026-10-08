"""Inventory extraction with injected I/O. No Snowflake connection."""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.catalog import SYSTEM_DBS, build_inventory

SESSION = [{"U": "TESTUSER", "A": "TESTACCT01", "R": "AWS_US_EAST_2",
            "ROLE": "ACCOUNTADMIN", "WH": "COMPUTE_WH", "V": "10.32.102"}]

COLUMNS = [
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 1,
     "COLUMN_NAME": "ORDER_ID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
     "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 2,
     "COLUMN_NAME": "PAID", "DATA_TYPE": "NUMBER", "IS_NULLABLE": "YES",
     "NUMERIC_PRECISION": 18, "NUMERIC_SCALE": 2,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
    {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "ORDERS", "ORDINAL_POSITION": 3,
     "COLUMN_NAME": "PAYLOAD", "DATA_TYPE": "VARIANT", "IS_NULLABLE": "YES",
     "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
     "CHARACTER_MAXIMUM_LENGTH": None, "DATETIME_PRECISION": None, "COMMENT": None},
]


def base_responses(**over):
    r = {
        "current_user()": SESSION,
        "show databases": [{"name": "MYDB"}, {"name": "SNOWFLAKE"},
                           {"name": "SNOWFLAKE_SAMPLE_DATA"}],
        "show schemas": [{"name": "PUBLIC"}, {"name": "INFORMATION_SCHEMA"}],
        "information_schema.columns": COLUMNS,
        "show tables": [{"name": "ORDERS", "rows": 99, "bytes": 4096,
                         "created_on": "2026-09-08", "comment": None,
                         "cluster_by": None}],
        "show views": [{"name": "ORDERS_VW", "text": "select * from ORDERS",
                        "created_on": "2026-09-08", "comment": None,
                        "is_secure": "false"}],
        "count(*)": [{"N": 100}],
        "get_ddl": [{"D": "create or replace view ORDERS_VW as select * from ORDERS;"}],
    }
    r.update(over)
    return r


def test_system_databases_are_excluded():
    inv = build_inventory(FakeSql(base_responses()))
    assert inv["databases_in_scope"] == ["MYDB"]
    assert "SNOWFLAKE" in SYSTEM_DBS


def test_information_schema_is_not_walked():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["source_schema"] == "PUBLIC" for r in inv["inventory"])


def test_finds_one_table_and_one_view():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert inv["counts_by_type"] == {"TABLE": 1, "VIEW": 1}
    assert inv["object_count"] == 2


def test_exact_mode_prefers_count_over_the_show_metadata_number():
    # The fixture has SHOW reporting 99 and COUNT(*) reporting 100 on purpose:
    # the maintained count can lag recent DML, so `exact` must not read it.
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"],
                          row_counts="exact")
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] == 100, "must be count(*), not SHOW's 99"
    assert table["row_count_source"] == "count_query"
    assert table["source_metadata"]["bytes"] == 4096


def test_metadata_mode_is_never_labelled_exact():
    # It is free and it is usually right, but claiming it is verified would be
    # the same overstatement as calling a structure clone a data clone.
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] == 99, "the metadata number, not COUNT(*)"
    assert table["row_count_source"] == "show_metadata"
    assert "exact" not in table["row_count_note"].split("COUNT(*)")[0].lower()


def test_precision_and_scale_carried_from_information_schema():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    paid = next(c for c in table["columns"] if c["COLUMN_NAME"] == "PAID")
    assert (paid["NUMERIC_PRECISION"], paid["NUMERIC_SCALE"]) == (18, 2)


def test_type_mapping_recorded_and_variant_blocked():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    m = {c["COLUMN_NAME"]: c["target_type"] for c in table["columns"]}
    assert m["ORDER_ID"] == "DECIMAL(38,0)"
    assert m["PAID"] == "DECIMAL(18,2)"
    assert m["PAYLOAD"] is None
    assert table["compatibility_status"] == "blocked"
    assert any("VARIANT" in b for b in table["blocked_reasons"])


def test_table_with_all_types_mapped_is_supported():
    cols = [c for c in COLUMNS if c["COLUMN_NAME"] != "PAYLOAD"]
    inv = build_inventory(
        FakeSql(base_responses(**{"information_schema.columns": cols})),
        databases=["MYDB"])
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["compatibility_status"] == "supported"


def test_view_keeps_both_ddl_forms_verbatim():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    view = next(r for r in inv["inventory"] if r["object_type"] == "VIEW")
    assert view["view_text_show"] == "select * from ORDERS"
    assert "create or replace view" in view["view_ddl_get_ddl"]
    # Migratability is the planner's verdict, not the extractor's. This
    # module reports what IS.
    assert view["compatibility_status"] in ("supported", "blocked")


def test_case_form_captured_per_object():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["identifier_case_form"] == "UPPER_UNQUOTED" for r in inv["inventory"])


def test_case_collision_is_surfaced():
    r = base_responses(**{"show tables": [
        {"name": "ORDERS", "rows": 1, "bytes": 1},
        {"name": "orders", "rows": 1, "bytes": 1}]})
    inv = build_inventory(FakeSql(r), databases=["MYDB"])
    assert inv["identifier_case_collisions"], "must report, not silently merge"


def test_count_failure_is_recorded_not_fatal():
    class Flaky(FakeSql):
        def __call__(self, sql, params=None):
            if "count(*)" in sql.lower():
                raise RuntimeError("warehouse suspended")
            return super().__call__(sql, params)

    inv = build_inventory(Flaky(base_responses()), databases=["MYDB"],
                          row_counts="exact")
    table = next(r for r in inv["inventory"] if r["object_type"] == "TABLE")
    assert table["row_count_exact"] is None
    assert table["row_count_source"] == "error"
    assert "warehouse suspended" in table["row_count_note"]
    # ...and it reaches the user, rather than sitting in a field nobody reads.
    assert any("warehouse suspended" in n for n in inv["extraction_notes"])


def test_schema_listing_failure_is_noted_and_extraction_continues():
    class NoSchemas(FakeSql):
        def __call__(self, sql, params=None):
            if "show schemas" in sql.lower():
                raise RuntimeError("insufficient privileges")
            return super().__call__(sql, params)

    inv = build_inventory(NoSchemas(base_responses()), databases=["MYDB"])
    assert inv["inventory"] == []
    assert any("insufficient privileges" in n for n in inv["extraction_notes"])


def test_migration_status_starts_at_discovered():
    inv = build_inventory(FakeSql(base_responses()), databases=["MYDB"])
    assert all(r["migration_status"] == "discovered" for r in inv["inventory"])


# ==========================================================================
# Row-count strategy, SHOW pagination, identifier quoting and count-failure
# reporting.
# ==========================================================================

def _session_rows():
    return [{"U": "U", "A": "A", "R": "R", "ROLE": "R", "WH": "W", "V": "1"}]


def _base_responses(*, tables, views=(), columns=()):
    return {
        "current_user()": _session_rows(),
        "show databases": [{"name": "DB"}],
        "show schemas in database": [{"name": "PUBLIC"}],
        "show tables in schema": list(tables),
        "show views in schema": list(views),
        "information_schema.columns": list(columns),
    }


def _col(table, name="C", dtype="TEXT"):
    return {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": table, "ORDINAL_POSITION": 1,
            "COLUMN_NAME": name, "DATA_TYPE": dtype, "IS_NULLABLE": "YES",
            "NUMERIC_PRECISION": None, "NUMERIC_SCALE": None,
            "CHARACTER_MAXIMUM_LENGTH": 10, "DATETIME_PRECISION": None,
            "COMMENT": None}


def test_default_row_counts_come_from_show_metadata_not_a_count_query():
    # A COUNT(*) per object is the single most expensive thing an assessment
    # can do, and for a TABLE it buys nothing: SHOW TABLES already carries an
    # exact row count in its `rows` column.
    fake = FakeSql(_base_responses(tables=[{"name": "T", "rows": 42}],
                                  columns=[_col("T")]))
    inv = build_inventory(fake)
    rec = inv["inventory"][0]
    assert rec["row_count_exact"] == 42
    assert rec["row_count_source"] == "show_metadata"
    assert not any("count(*)" in c.lower() for c in fake.calls)


def test_views_are_not_counted_by_default_because_counting_executes_them():
    # COUNT(*) on a view runs the view. On a wide join that is minutes to
    # hours of warehouse time, per view, during an assessment.
    fake = FakeSql(_base_responses(tables=[], views=[{"name": "V", "text": "select 1"}],
                                  columns=[_col("V")]))
    fake.responses["get_ddl"] = [{"D": "create view V as select 1"}]
    inv = build_inventory(fake)
    rec = inv["inventory"][0]
    assert rec["row_count_exact"] is None
    assert rec["row_count_source"] == "not_counted"
    assert "executing" in rec["row_count_note"].lower()
    assert not any("count(*)" in c.lower() for c in fake.calls)


def test_exact_mode_issues_a_count_query_and_quotes_the_identifier():
    fake = FakeSql({**_base_responses(tables=[{"name": 'we"ird', "rows": 1}],
                                      columns=[]),
                    "count(*)": [{"N": 7}]})
    inv = build_inventory(fake, row_counts="exact")
    rec = inv["inventory"][0]
    assert rec["row_count_exact"] == 7
    assert rec["row_count_source"] == "count_query"
    # THE BUG: f'"{name}"' emits "we"ird" -- the tail of the name lands
    # outside the quotes, where it is parsed as code.
    assert '"we""ird"' in [c for c in fake.calls if "count(*)" in c.lower()][0]


def test_none_mode_issues_no_count_and_reports_no_number():
    fake = FakeSql(_base_responses(tables=[{"name": "T", "rows": 42}], columns=[]))
    inv = build_inventory(fake, row_counts="none")
    assert inv["inventory"][0]["row_count_exact"] is None
    assert inv["inventory"][0]["row_count_source"] == "not_counted"


def test_a_count_failure_is_reported_not_silently_dropped():
    # The count error used to be written to the record and read by nothing, so
    # a whole estate could report blank row counts with no reason given.
    class Failing(FakeSql):
        def __call__(self, sql, params=None):
            if "count(*)" in sql.lower():
                self.calls.append(sql)
                raise RuntimeError("No active warehouse selected")
            return super().__call__(sql, params)

    fake = Failing(_base_responses(tables=[{"name": "T", "rows": None}], columns=[]))
    inv = build_inventory(fake, row_counts="exact")
    rec = inv["inventory"][0]
    assert rec["row_count_source"] == "error"
    assert "warehouse" in rec["row_count_note"]
    assert any("DB.PUBLIC.T" in n and "row count" in n
               for n in inv["extraction_notes"])


def test_unknown_row_count_mode_is_refused():
    with pytest.raises(ValueError):
        build_inventory(FakeSql(_base_responses(tables=[])), row_counts="guess")


def test_show_results_are_paginated_so_a_large_estate_is_not_truncated():
    # SHOW caps at 10k rows. Without paging the inventory looks complete and
    # is not -- the worst possible failure for an assessment.
    page1 = [{"name": f"T{i:05d}", "rows": 0} for i in range(10000)]
    page2 = [{"name": "T99999", "rows": 0}]

    class Paged(FakeSql):
        def __call__(self, sql, params=None):
            flat = " ".join(sql.split()).lower()
            if "show tables in schema" in flat:
                self.calls.append(sql)
                return page2 if "from 'T09999'".lower() in flat else page1
            return super().__call__(sql, params)

    fake = Paged(_base_responses(tables=[], columns=[]))
    inv = build_inventory(fake, row_counts="none")
    names = [r["source_identifier"].rsplit(".", 1)[1] for r in inv["inventory"]]
    assert len(names) == 10001
    assert "T99999" in names


def test_pagination_cursor_is_a_plain_name_not_a_like_pattern(monkeypatch):
    # `SHOW ... LIMIT n FROM '<name>'` takes a NAME STRING, and SHOW resumes
    # strictly after it. `_` and `%` are literal there, not wildcards, so a
    # LIKE-escaped cursor (`ORDER\_ITEMS`) names an object that does not
    # exist and the walk resumes in the wrong place: pages repeat and the
    # tail of the schema never enters the inventory. Nearly every real name
    # contains an underscore, so the 10k-name test above cannot see this.
    import re
    from snowflake_source.extract import catalog as catalog_module

    all_names = sorted(["ACCOUNTS_2019", "O'BRIEN_T", "ORDER_ITEMS_FACT",
                        "ORDER_ITEMS_STG", "ORDER_LINES", "PCT%DONE",
                        "ZZ_LAST"])
    monkeypatch.setattr(catalog_module, "SHOW_PAGE_SIZE", 2)

    class Seeking(FakeSql):
        """Emulates the live-verified FROM semantics: resume after the
        literal name given, exclusive; an unknown name seeks to wherever it
        would sort."""
        def __call__(self, sql, params=None):
            flat = " ".join(sql.split())
            if flat.lower().startswith("show tables in schema"):
                self.calls.append(sql)
                m = re.search(r" from '((?:[^']|'')*)'$", flat)
                after = m.group(1).replace("''", "'") if m else None
                names = [n for n in all_names if after is None or n > after]
                return [{"name": n, "rows": 0} for n in names[:2]]
            return super().__call__(sql, params)

    fake = Seeking(_base_responses(tables=[], columns=[]))
    inv = build_inventory(fake, row_counts="none")
    names = [r["source_identifier"].rsplit(".", 1)[1] for r in inv["inventory"]]
    assert names == all_names, "every object exactly once: no repeat, no loss"

    resumes = [c for c in fake.calls if " from '" in c]
    assert len(resumes) == 3
    assert not any("\\" in c for c in resumes), \
        "the cursor is a plain name; LIKE escaping does not belong here"
    assert resumes[0].endswith("from 'O''BRIEN_T'"), \
        "only the quote is doubled, so a name with ' cannot break the literal"
    assert resumes[1].endswith("from 'ORDER_ITEMS_STG'")
    assert resumes[2].endswith("from 'PCT%DONE'")


def test_columns_are_read_per_schema_so_one_query_cannot_be_unbounded():
    fake = FakeSql(_base_responses(tables=[{"name": "T", "rows": 0}],
                                  columns=[_col("T")]))
    build_inventory(fake, row_counts="none")
    col_calls = [c for c in fake.calls if "information_schema.columns" in c.lower()]
    assert col_calls, "columns must still be read"
    assert all("table_schema =" in c.lower() for c in col_calls)


def test_the_stale_mvp1_view_verdict_is_gone():
    # Views are in scope now. The record used to hard-code
    # compatibility_status=requires_manual_design and risk_level=high, both
    # read by nothing, with text citing a Databricks rewriter that was deleted.
    fake = FakeSql(_base_responses(tables=[], views=[{"name": "V", "text": "select 1"}],
                                  columns=[]))
    fake.responses["get_ddl"] = [{"D": "create view V as select 1"}]
    rec = build_inventory(fake, row_counts="none")["inventory"][0]
    assert rec["compatibility_status"] in ("supported", "blocked")
    assert "risk_level" not in rec
    assert "MVP-1" not in str(rec)
    assert "Databricks" not in str(rec)


def test_maintenance_columns_are_captured_from_show_output():
    # These are free -- SHOW TABLES already returns them -- and they are the
    # whole input to the maintenance assessment (M2). Missing them meant the
    # maintenance question could not be asked at all.
    fake = FakeSql(_base_responses(
        tables=[{"name": "T", "rows": 1, "cluster_by": "(A)",
                 "automatic_clustering": "ON", "change_tracking": "ON",
                 "search_optimization": "ON", "search_optimization_bytes": 4096,
                 "retention_time": 7, "is_external": "N", "is_hybrid": "N"}],
        columns=[]))
    meta = build_inventory(fake, row_counts="none")["inventory"][0]["source_metadata"]
    for key in ("cluster_by", "automatic_clustering", "change_tracking",
                "search_optimization", "search_optimization_bytes",
                "retention_time"):
        assert key in meta, f"{key} not captured from SHOW"


def test_show_tables_kind_and_flags_reach_the_plan():
    # SHOW TABLES says which rows are dynamic, and whether a table is
    # TRANSIENT/TEMPORARY. Both must survive extraction so the plan can act:
    # a dynamic table migrates as a table snapshot (its refresh is decided
    # separately); a transient one migrates with a warning.
    from plan.build import build_plan
    fake = FakeSql(_base_responses(
        tables=[{"name": "DT", "rows": 1, "is_dynamic": "Y", "kind": "TABLE"},
                {"name": "TT", "rows": 1, "is_dynamic": "N", "kind": "TRANSIENT"}],
        columns=[_col("DT"), _col("TT")]))
    inv = build_inventory(fake, row_counts="none")
    meta = {r["source_identifier"]: r["source_metadata"] for r in inv["inventory"]}
    assert meta["DB.PUBLIC.TT"]["kind"] == "TRANSIENT"
    plan = build_plan(inv, {"edges": []})
    assert plan["cannot_migrate"] == []
    can = {c["source_identifier"]: c for c in plan["can_migrate"]}
    assert can["DB.PUBLIC.DT"]["snapshot_of"] == "dynamic table"
    assert [w["source_identifier"] for w in plan["table_kind_warnings"]] == ["DB.PUBLIC.TT"]

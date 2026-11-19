"""Three restrictions, each found live against a real tenancy, pinned here.

  1. TARGET KEY LENGTH -- the destination stores at most 255 characters of
     `catalog.schema.name`. A longer key answers 202 Accepted, creates
     nothing, and burns the name. Refused at plan time.
  2. SECONDARY ROLES -- Snowflake activates every role granted to the user
     by default, so a count attributed to CURRENT_ROLE() can have been
     served by ACCOUNTADMIN. Read, named in every attribution, and
     droppable with --only-primary-role.
  3. COLUMN FACTS ON THE IN-AIDP PATH -- discovery must read DEFAULT,
     IDENTITY and COMMENT and the manifest bridge must carry them; a
     manifest that lacks them says UNKNOWN, never "no default".
"""
import json
import pathlib

import pytest

from fake_sql import FakeSql


# =========================================================== 1. key length

def _table(name, db="D", schema="SALES"):
    return {"source_identifier": f"{db}.{schema}.{name}", "object_type": "TABLE",
            "source_database": db, "source_schema": schema,
            "compatibility_status": "supported", "blocked_reasons": [],
            "row_count_exact": 0, "source_metadata": {}}


def test_the_key_limit_is_255():
    from plan.medallion import TARGET_KEY_MAX
    assert TARGET_KEY_MAX == 255


def test_a_key_of_exactly_255_is_accepted():
    from plan.medallion import target_key_overage
    fqn = "c.s." + "x" * 251
    assert len(fqn) == 255 and target_key_overage(fqn) == 0


def test_one_character_over_is_one_over():
    from plan.medallion import target_key_overage
    assert target_key_overage("c.s." + "x" * 252) == 1


def test_the_whole_key_is_bounded_not_the_name():
    """The live shape: one name, accepted under a short schema and refused
    under a long one."""
    from plan.medallion import target_key_overage
    name = "x" * 227
    assert target_key_overage(f"snowmig_coverage_v2.default.{name}") == 0
    assert target_key_overage(
        f"snowmig_coverage_v2.snowmig_coverage_edge.{name}") == 14


def test_a_too_long_target_is_refused_at_plan_time():
    from plan.build import build_plan
    plan = build_plan({"inventory": [_table("T" + "X" * 250)]}, {"edges": []})
    assert plan["can_migrate"] == []
    entry = plan["cannot_migrate"][0]
    assert entry["category"] == "target_key_too_long"
    assert "255" in entry["reason"]
    assert plan["summary"]["cannot_by_category"] == {"target_key_too_long": 1}


def test_the_refusal_names_what_is_eating_the_budget():
    """Told only "name too long", an operator shortens the table name. The
    catalog and schema -- the parts this tool chose -- are where the room is."""
    from plan.build import build_plan
    plan = build_plan({"inventory": [_table("T" + "X" * 250)]}, {"edges": []},
                      bronze_catalog_prefix="a_long_prefix")
    reason = plan["cannot_migrate"][0]["reason"]
    assert "catalog" in reason.lower() and "schema" in reason.lower()
    assert "--bronze-catalog-prefix" in reason


def test_an_ordinary_name_still_plans():
    from plan.build import build_plan
    plan = build_plan({"inventory": [_table("ORDERS")]}, {"edges": []},
                      bronze_catalog_prefix="bronze")
    assert [c["source_identifier"] for c in plan["can_migrate"]] == [
        "D.SALES.ORDERS"]


def test_a_too_long_key_never_reaches_the_ddl():
    from plan.build import build_plan
    from target.ddl import build_ddl_payload
    rec = {**_table("T" + "X" * 250),
           "columns": [{"COLUMN_NAME": "A", "DATA_TYPE": "NUMBER",
                        "target_type": "BIGINT", "ORDINAL_POSITION": 1}]}
    inv = {"inventory": [rec]}
    payload = build_ddl_payload(inv, build_plan(inv, {"edges": []}))
    assert payload["statements"] == []


# ====================================================== 2. secondary roles

def test_secondary_roles_are_parsed_from_snowflakes_json():
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active(
        '{"roles":"ORGADMIN,ACCOUNTADMIN","value":"ALL"}') == [
            "ORGADMIN", "ACCOUNTADMIN"]


@pytest.mark.parametrize("value", ['{"roles":"","value":""}', None, "",
                                   "{not json", "{}"])
def test_none_or_an_unreadable_shape_yields_no_roles(value):
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active(value) == []


def test_a_bare_comma_list_is_accepted():
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active("A, B") == ["A", "B"]


def _census_responses():
    return {k: [] for k in (
        "information_schema.procedures", "information_schema.functions",
        "information_schema.sequences", "information_schema.stages",
        "information_schema.file_formats", "information_schema.pipes",
        "show tasks", "show streams", "show materialized views",
        "show dynamic tables")}


def test_the_census_names_the_secondary_roles_the_reads_also_had():
    from snowflake_source.extract.census import build_census
    census = build_census(FakeSql(_census_responses()), ["DB"],
                          role="LIMITED", secondary_roles=["ACCOUNTADMIN"])
    text = census["scope_statement"] + " " + " ".join(
        k["note"] for k in census["kinds"].values())
    assert "ACCOUNTADMIN" in text and "secondary" in text.lower()
    assert census["secondary_roles"] == ["ACCOUNTADMIN"]


def test_without_secondary_roles_the_census_says_nothing_about_them():
    from snowflake_source.extract.census import build_census
    census = build_census(FakeSql(_census_responses()), ["DB"],
                          role="LIMITED", secondary_roles=[])
    text = census["scope_statement"] + " ".join(
        k["note"] for k in census["kinds"].values())
    assert "secondary" not in text.lower()


def test_the_inventory_session_reads_current_secondary_roles():
    from snowflake_source.extract.catalog import build_inventory
    sql = FakeSql({"current_user()": [{"ROLE": "R"}], "show databases": [],
                   "show schemas": []})
    build_inventory(sql, ["DB"])
    assert "current_secondary_roles()" in sql.calls[0].lower()


def test_drop_secondary_roles_issues_use_secondary_roles_none():
    from snowflake_source.conn import drop_secondary_roles

    class Cur:
        def __init__(self, log):
            self.log = log

        def execute(self, sql, *a):
            self.log.append(sql)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Conn:
        def __init__(self):
            self.log = []

        def cursor(self):
            return Cur(self.log)

    conn = Conn()
    drop_secondary_roles(conn)
    assert conn.log == ["use secondary roles none"]


def test_only_primary_role_is_a_flag_and_off_by_default():
    from snowmig import build_parser
    args = build_parser().parse_args(["assess"])
    assert args.only_primary_role is False
    args = build_parser().parse_args(["assess", "--only-primary-role"])
    assert args.only_primary_role is True


def _inv(**over):
    base = {"probed_at": "2026-09-23T00:00:00Z",
            "session": {"A": "ACC", "R": "REG", "ROLE": "LIMITED"},
            "databases_in_scope": ["DB_A", "DB_B"], "object_count": 8,
            "counts_by_type": {"TABLE": 8}, "inventory": [],
            "extraction_notes": []}
    base.update(over)
    return base


def test_the_inventory_role_line_names_secondary_roles():
    from report.render import render_inventory
    md = render_inventory(_inv(session={
        "A": "ACC", "R": "REG", "ROLE": "LIMITED",
        "SECONDARY_ROLES": '{"roles":"ACCOUNTADMIN","value":"ALL"}'}))
    line = next(l for l in md.splitlines() if l.startswith("Probed"))
    assert "ACCOUNTADMIN" in line and "secondary" in line.lower()


def test_a_refused_database_is_marked_in_the_scope_line():
    from report.render import render_inventory
    md = render_inventory(_inv(extraction_notes=[
        "database DB_B: 002043 (02000): SQL compilation error"]))
    scope = next(l for l in md.splitlines() if l.startswith("Databases in scope"))
    assert "DB_B (**NOT READ**)" in scope and "DB_A (**NOT READ**)" not in scope
    assert "could not be read at all" in md


def test_an_object_level_note_is_not_a_refused_database():
    from report.render import render_inventory
    md = render_inventory(_inv(extraction_notes=[
        "DB_A.PUBLIC.V1: GET_DDL failed: 002043 (02000)"]))
    assert "NOT READ" not in md


def test_the_summary_role_row_names_secondary_roles():
    from report.render import render_summary
    inv = _inv(session={"A": "ACC", "R": "REG", "ROLE": "LIMITED",
                        "SECONDARY_ROLES": '{"roles":"ACCOUNTADMIN"}'})
    md = render_summary({"can_migrate": [], "cannot_migrate": []}, inv,
                        None, None)
    row = next(l for l in md.splitlines() if l.startswith("| Role"))
    assert "ACCOUNTADMIN" in row


# ================================================= 3. column facts, in-AIDP

def _manifest(cols):
    return {"schemas": [{"name": "S", "tables": [{"name": "T", "columns": cols}]}]}


def _col(**over):
    base = {"name": "C", "data_type": "NUMBER", "numeric_precision": 38,
            "numeric_scale": 0, "character_maximum_length": None,
            "nullable": True}
    base.update(over)
    return base


def test_manifest_facts_reach_the_inventory():
    from snowflake_source.extract.manifest import inventory_from_manifest
    inv = inventory_from_manifest(_manifest([_col(
        column_default="'SMB'", identity_start=1, identity_increment=1,
        comment="a column comment")]), database="D")
    col = inv["inventory"][0]["columns"][0]
    assert col["COLUMN_DEFAULT"] == "'SMB'"
    assert (col["IDENTITY_START"], col["IDENTITY_INCREMENT"]) == (1, 1)
    assert col["COMMENT"] == "a column comment"


def test_an_old_manifest_says_UNKNOWN_not_absent():
    from snowflake_source.extract.manifest import inventory_from_manifest
    inv = inventory_from_manifest(_manifest([_col()]), database="D")
    assert inv["inventory"][0]["column_facts_unknown"] is True
    assert any("unknown" in n.lower() and "default" in n.lower()
               for n in inv["extraction_notes"])


def test_a_new_manifest_with_no_default_is_a_real_answer():
    from snowflake_source.extract.manifest import inventory_from_manifest
    inv = inventory_from_manifest(_manifest([_col(
        column_default=None, facts_recorded=True)]), database="D")
    assert inv["inventory"][0].get("column_facts_unknown") is not True
    assert not [n for n in inv["extraction_notes"] if "unknown" in n.lower()]


def _discover():
    import importlib.util
    import sys
    scripts = pathlib.Path(__file__).resolve().parents[1] / "dataplane"
    sys.path.insert(0, str(scripts))
    spec = importlib.util.spec_from_file_location(
        "snowmig_script_discover_l2", scripts / "00_discover_snowflake.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _DF:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)
        return [Row(r) for r in self._rows]


class _Source:
    def __init__(self, tables, columns):
        self.queries, self._t, self._c = [], tables, columns

    def pushdown(self, sql, schema=None):
        self.queries.append(sql)
        return _DF(self._c if "COLUMNS" in sql else self._t)


def test_discovery_selects_the_facts():
    sql = _discover()._COLUMNS_SQL
    for field in ("COLUMN_DEFAULT", "IDENTITY_START", "IDENTITY_INCREMENT",
                  "COMMENT"):
        assert field in sql


def test_discovery_WRITES_the_facts_into_the_manifest():
    """Selecting them is half. level2 selected them and still wrote a
    manifest without them, so the bridge flagged every object UNKNOWN."""
    src = _Source(
        tables=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 1, "BYTES": 1}],
        columns=[{"TABLE_SCHEMA": "S", "TABLE_NAME": "T", "COLUMN_NAME": "ID",
                  "DATA_TYPE": "NUMBER", "IS_NULLABLE": "NO",
                  "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0,
                  "CHARACTER_MAXIMUM_LENGTH": None, "COLUMN_DEFAULT": None,
                  "IDENTITY_START": 1, "IDENTITY_INCREMENT": 1,
                  "COMMENT": "pk"}])
    schemas = _discover().discover_via_connector(src, wanted=None,
                                                 exclude=set())
    col = schemas[0]["tables"][0]["columns"][0]
    assert col["identity_start"] == 1 and col["comment"] == "pk"
    assert col["facts_recorded"] is True

    from snowflake_source.extract.manifest import inventory_from_manifest
    inv = inventory_from_manifest({"schemas": schemas}, database="D")
    assert inv["inventory"][0].get("column_facts_unknown") is not True


def test_the_live_assess_reads_the_same_facts():
    """Both planning paths must reach the same verdict on the same column."""
    from snowflake_source.extract.catalog import _columns
    sql = FakeSql({"information_schema.columns": []})
    _columns(sql, "DB", "S", [])
    low = sql.calls[0].lower()
    for field in ("column_default", "identity_start", "identity_increment",
                  "comment"):
        assert field in low


def test_ddl_warns_that_default_and_identity_are_not_carried():
    from target.ddl import build_create_table
    rec = {"source_identifier": "D.S.T", "compatibility_status": "supported",
           "columns": [
               {"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER",
                "target_type": "BIGINT", "ORDINAL_POSITION": 1,
                "IDENTITY_START": 1, "IDENTITY_INCREMENT": 1},
               {"COLUMN_NAME": "SEG", "DATA_TYPE": "TEXT",
                "target_type": "STRING", "ORDINAL_POSITION": 2,
                "COLUMN_DEFAULT": "'SMB'"}]}
    res = build_create_table(rec, "d.s.t")
    ids = [r.rule_id for r in res.rules_applied]
    assert "R22_COLUMN_DEFAULT_NOT_EMITTED" in ids
    assert "R23_IDENTITY_NOT_EMITTED" in ids
    assert "DEFAULT" not in res.sql and "IDENTITY" not in res.sql
    assert any(w.startswith("SEG:") for w in res.warnings)
    assert any(w.startswith("ID:") for w in res.warnings)

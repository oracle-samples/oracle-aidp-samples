"""Regressions for the review of the coverage branch: each test pins one
behaviour a finding showed was unsafe or misleading."""
import json
import sys

import pytest

from fake_pushdown import FakeLakeSpark, FakeSnowflake
from test_copy_parallel import _estate, _files
from test_copy_resume_mid_chunk import _Dies, _Killed, _main, _report
from test_data_migration_scripts import _inject_spark, _load

_NAMES = [f"T{i:03d}" for i in range(6)]


def _rows(spark, name):
    return len(spark.rows[f"`lake`.`bulk`.`{name}`"])


# --- the database name: one rule for every statement that names it ----------

def test_the_database_name_resolves_the_way_snowflake_resolves_it():
    _load("02_copy_schema")
    src = sys.modules["snowmig_source"]
    assert src._database_name("snowmig_db") == "SNOWMIG_DB"
    assert src._database_name('"MyDb"') == "MyDb"
    assert src._database_name("my-db") == "my-db"
    assert src._sql_database('"MyDb"') == '"MyDb"'
    assert src._sql_database('"My""Db"') == '"My""Db"'


def test_get_ddl_names_the_same_database_as_the_other_pushdowns():
    discover = _load("00_discover_snowflake")
    literal = discover._qualified_literal("snowmig_db", "CORE", "T")
    assert '"SNOWMIG_DB"."CORE"."T"' in literal
    assert '"snowmig_db"' not in literal


# --- the copy: no duplicate rows ----------------------------------------------

def test_append_refuses_to_start_after_an_unfinished_run_of_another_mode(
        monkeypatch, tmp_path):
    tables, lake, statements = _estate(6)
    spark = _Dies(FakeSnowflake(tables), lake, kill="T003")
    reports, config = _files(tmp_path, statements, _NAMES)
    _inject_spark(monkeypatch, spark)
    with pytest.raises(_Killed):
        _main(reports, config, "--mode", "skip-existing", "--parallel", "1")
    before = {n: _rows(spark, n) for n in _NAMES}
    marker = json.loads((reports / "copy_report_bulk.json").read_text(
        encoding="utf-8"))["run"]
    assert marker["mode"] == "skip-existing" and marker["finished"] is False

    spark.kill = None
    assert _main(reports, config, "--mode", "append", "--parallel", "1") == 1
    assert {n: _rows(spark, n) for n in _NAMES} == before, \
        "nothing may be appended over a run whose last records may be lost"

    # Resumed in its own mode, the run finishes; append is allowed again.
    _main(reports, config, "--mode", "skip-existing", "--parallel", "1")
    marker = json.loads((reports / "copy_report_bulk.json").read_text(
        encoding="utf-8"))["run"]
    assert marker["finished"] is True


def test_a_narrower_finished_run_does_not_clear_the_append_guard(
        monkeypatch, tmp_path, capsys):
    tables, lake, statements = _estate(6)
    spark = _Dies(FakeSnowflake(tables), lake, kill="T003")
    reports, config = _files(tmp_path, statements, _NAMES)
    _inject_spark(monkeypatch, spark)
    with pytest.raises(_Killed):
        _main(reports, config, "--mode", "overwrite", "--parallel", "1")
    spark.kill = None

    # A finished run over one other table leaves the stopped run's tables
    # carried forward, unverified: the marker stays unfinished.
    assert _main(reports, config, "--mode", "overwrite", "--tables", "T005",
                 "--parallel", "1") == 0
    marker = json.loads((reports / "copy_report_bulk.json").read_text(
        encoding="utf-8"))["run"]
    assert marker["finished"] is False
    assert "T003" in marker["carried"] and "T005" not in marker["carried"]

    before = {n: _rows(spark, n) for n in _NAMES}
    capsys.readouterr()
    assert _main(reports, config, "--mode", "append", "--parallel", "1") == 1
    cap = capsys.readouterr()
    assert "Tables at risk" in cap.out + cap.err and "T003" in cap.out + cap.err
    assert {n: _rows(spark, n) for n in _NAMES} == before

    # A run that covers them finishes the marker.
    assert _main(reports, config, "--mode", "overwrite", "--parallel", "1") == 0
    marker = json.loads((reports / "copy_report_bulk.json").read_text(
        encoding="utf-8"))["run"]
    assert marker["finished"] is True and "carried" not in marker


def test_a_table_named_twice_is_copied_once(monkeypatch, tmp_path):
    tables, lake, statements = _estate(2)
    spark = FakeLakeSpark(FakeSnowflake(tables), lake)
    reports, config = _files(tmp_path, statements, ["T000", "T001"])
    _inject_spark(monkeypatch, spark)
    assert _main(reports, config, "--tables", "T000", "T000",
                 "--parallel", "8") == 0
    assert _rows(spark, "T000") == 3, "every source row exactly once"
    assert _report(reports)["T000"]["status"] == "verified"


# --- the structure: CTAS stays one at a time unless asked ---------------------

def test_ctas_defaults_to_one_table_at_a_time():
    structure = _load("01_create_structure")
    assert structure.effective_parallel(None, "ctas") == 1
    assert structure.effective_parallel(None, "ddl-plan") == \
        structure.DEFAULT_PARALLEL
    assert structure.effective_parallel(4, "ctas") == 4


# --- reconcile: "unrecorded" only against a structure report ------------------

def test_no_structure_report_is_not_a_stopped_structure_run(tmp_path):
    from test_data_migration_scripts import _CatalogSpark, _manifest
    reconcile = _load("03_reconcile")
    spark = _CatalogSpark({"`lake`.`SALES`.`ORDERS`": [("A", "string")]})
    rec = reconcile.reconcile(spark, manifest=_manifest("ORDERS"),
                              target_catalog="lake", reports=tmp_path,
                              counts=False)
    row = rec["schemas"][0]["tables"][0]
    assert row["structure"] != "unrecorded", row
    assert row["verdict"] == "STRUCTURE_ONLY"


# --- teardown reaches what `jobs --register` created --------------------------

def test_teardown_all_includes_registered_generated_jobs():
    from target.teardown import teardown_everything
    prov = {"dry_run": False, "workspace": {"name": "ws", "key": "ws-1",
                                            "created": False},
            "steps": [], "credential_objects": []}
    ledger = [
        {"kind": "job", "name": "snowmig_refresh_mv", "workspace": "ws-1",
         "action": "created"},
        {"kind": "ws_object", "workspace": "ws-1", "action": "created",
         "name": "backup-snowflake-migration/generated_jobs/mv.ipynb"},
        {"kind": "job", "name": "elsewhere", "workspace": "ws-2",
         "action": "created"}]
    res = teardown_everything(None, prov, scope="all", execute=False,
                              ledger=ledger)
    planned = [(s["kind"], s["name"]) for s in res["steps"]]
    assert ("job", "snowmig_refresh_mv") in planned
    assert ("notebook",
            "backup-snowflake-migration/generated_jobs/mv.ipynb") in planned
    assert ("job", "elsewhere") not in planned


# --- generated statements and reports -----------------------------------------

def test_an_iceberg_name_with_a_quote_is_escaped_the_spark_way():
    from target.external_registration import _register_table_call
    call = _register_table_call("cat.db.o'brien", "oci://b@n/m's.json")
    assert "'db.o\\'brien'" in call
    assert "'oci://b@n/m\\'s.json'" in call
    assert "''" not in call


def test_the_snapshot_count_follows_the_plan():
    from report.translation_map import _snapshots
    mv = {"source_identifier": "DB.S.MV", "object_type": "VIEW",
          "source_metadata": {"is_materialized": "Y"}}
    assert _snapshots([mv], {"can_migrate": []}) == []
    planned = _snapshots([mv], {"can_migrate": [
        {"source_identifier": "DB.S.MV", "refresh": {"verdict": "ok"}}]})
    assert [s["source_identifier"] for s in planned] == ["DB.S.MV"]


def test_a_snapshot_the_plan_leaves_out_is_still_counted():
    from report.render import render_translation_map
    from report.translation_map import build_translation_map
    mv = {"source_identifier": "DB.S.MV", "object_type": "VIEW",
          "source_metadata": {"is_materialized": "Y"}, "columns": []}
    tmap = build_translation_map({"inventory": [mv]}, {"can_migrate": []},
                                 None)
    assert tmap["totals"]["table_snapshots"] == 0
    assert tmap["totals"]["snapshots_not_planned"] == 1
    assert "**1** more not in the plan" in render_translation_map(tmap)


@pytest.mark.parametrize("body", [
    "create or replace secure view V as select A from T where CURRENT_ROLE  () = 'X'",
    "create or replace secure view V as select A from T where IS_ROLE_IN_SESSION\n('X')"])
def test_a_role_keyed_filter_is_named_whatever_the_whitespace(body):
    from plan.build import _secure_as_view_verdict
    rec = {"source_identifier": "DB.S.V", "object_type": "VIEW",
           "source_metadata": {"is_secure": "true"},
           "view_ddl_get_ddl": body, "columns": [{"name": "A",
                                                  "data_type": "TEXT"}]}
    verdict = _secure_as_view_verdict(rec, "as-view")
    assert verdict is not None and verdict[3] is not None
    assert "row filter keyed to Snowflake identities" in verdict[3]

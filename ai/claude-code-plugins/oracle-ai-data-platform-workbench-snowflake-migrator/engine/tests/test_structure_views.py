"""S10 creates the plan's views; the copy and the reconcile must know it.

Live 2026-09-29, on a schema with views in the approved plan. 01 created them
from the plan's own CREATE VIEW SQL and recorded each one in the structure
report's `objects` -- the map 02 takes its default copy scope from. 02 then
logged "7 table(s) the structure step created (the manifest lists 4)" and
tried to copy into the views (skipped_nonempty x3, type_drift x1, exit 1). In
the same run 01 still logged that the plan's views "are NOT created by this
stage", and wrote `views: not_created_by_this_path` beside the `created`
entries; 03 read only that key, so a view whose CREATE had FAILED came out
VIEW_NOT_CREATED_BY_THIS_PATH under "No table is in a problem state", exit 0.

Now a view's outcome is recorded under `views` only, 02's scope is tables
the manifest lists and never a VIEW, and 03 reports a created view as
created and a failed one as a problem.
"""
import json
import re

from test_data_migration_scripts import (
    _CatalogSpark, _MainSource, _inject_spark, _load, _report)


_COLS = [{"name": "ID", "type": "DECIMAL(38,0)"},
         {"name": "NOTE", "type": "STRING"}]
_SRC_TYPES = [("ID", "decimal(38,0)"), ("NOTE", "string")]


class _ViewSpark(_CatalogSpark):
    """`_CatalogSpark` that understands views, the way Spark treats them.

    CREATE VIEW resolves every backticked three-part name in its body and
    raises TABLE_OR_VIEW_NOT_FOUND for one that is not there (nothing is
    created); INSERT into a view raises EXPECT_TABLE_NOT_VIEW.
    """

    def __init__(self, catalog=None):
        super().__init__(catalog)
        self.views: list[str] = []

    def sql(self, statement):
        flat = " ".join(statement.split())
        m = re.match(r"create view if not exists (\S+)(?: \([^)]*\))? as (.*)$",
                     flat, re.IGNORECASE)
        if m:
            self.statements.append(flat)
            fqn, body = m.group(1), m.group(2)
            refs = re.findall(r"`[^`]+`\.`[^`]+`\.`[^`]+`", body)
            for ref in refs:
                if ref not in self.catalog:
                    raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] The table "
                                       f"or view {ref} cannot be found")
            if fqn not in self.catalog:
                self.catalog[fqn] = list(self.catalog[refs[0]]) if refs \
                    else [("X", "int")]
                self.views.append(fqn)
            return super().sql("SELECT 1")
        low = flat.lower()
        if low.startswith("insert"):
            tgt = flat.split()[2]
            if tgt in self.views:
                self.statements.append(flat)
                raise RuntimeError(f"[EXPECT_TABLE_NOT_VIEW] {tgt} is a view")
        return super().sql(statement)


def _view(name, body_ref):
    return {"source_identifier": f"DB.SALES.{name}", "object_type": "VIEW",
            "target_fqn": f"lake.SALES.{name}",
            "sql": (f"CREATE VIEW IF NOT EXISTS `lake`.`SALES`.`{name}` AS "
                    f"SELECT ID, NOTE FROM {body_ref}"),
            "expected_columns": _COLS}


def _estate(tmp_path, views=(("V_ORDERS", "`lake`.`SALES`.`ORDERS`"),
                              ("V_BAD", "`lake`.`SALES`.`MISSING`"))):
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "SALES", "tables": [{"name": "ORDERS", "columns": []}],
         "views": [{"name": v} for v, _ in views], "errors": []}]}),
        encoding="utf-8")
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(json.dumps(
        {"statements": [
            {"source_identifier": "DB.SALES.ORDERS", "object_type": "TABLE",
             "target_fqn": "lake.SALES.ORDERS", "expected_columns": _COLS},
            *[_view(v, ref) for v, ref in views]]}), encoding="utf-8")
    return reports


def _spark():
    spark = _ViewSpark({"`ext`.`SALES`.`ORDERS`": list(_SRC_TYPES)})
    spark.counts = {"`ext`.`SALES`.`ORDERS`": 5}
    return spark


def _structure(monkeypatch, reports, spark, *extra):
    _inject_spark(monkeypatch, spark)
    return _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "SALES",
         "--reports-dir", str(reports), "--output-dir", "", *extra])


def _copy(monkeypatch, reports, spark, *extra):
    _inject_spark(monkeypatch, spark)
    module = _load("02_copy_schema")
    monkeypatch.setattr(module, "SnowflakeSource", _MainSource)
    return module.main(["--target-catalog", "lake", "--schema", "SALES",
                        "--reports-dir", str(reports), "--output-dir", "",
                        *extra])


def _reconcile(monkeypatch, reports, spark):
    _inject_spark(monkeypatch, spark)
    return _load("03_reconcile").main(
        ["--target-catalog", "lake", "--reports-dir", str(reports),
         "--output-dir", ""])


# --- 01: one record per view, and it says what happened ----------------------

def test_the_structure_report_records_what_happened_to_each_view(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path)
    rc = _structure(monkeypatch, reports, _spark())
    out = capsys.readouterr().out
    assert rc == 1, "a planned view whose CREATE failed is a failure"
    report = _report(reports, "structure_report_sales.json")
    assert list(report["objects"]) == ["ORDERS"], \
        "objects is the table map 02 scopes the copy from; no view in it"
    assert report["views"]["V_ORDERS"]["status"] == "created"
    assert report["views"]["V_ORDERS"]["target_fqn"] == "lake.SALES.V_ORDERS"
    assert report["views"]["V_BAD"]["status"] == "failed"
    assert "TABLE_OR_VIEW_NOT_FOUND" in report["views"]["V_BAD"]["reason"]
    # One report, one answer per view: nothing says "not created here"
    # beside the view this very run created.
    assert "not_created_by_this_path" not in json.dumps(report)
    assert "NOT created by this stage" not in out
    assert "deploy --execute" not in out


def test_a_manifest_view_the_plan_does_not_carry_is_not_in_plan(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path, views=(("V_ORDERS",
                                        "`lake`.`SALES`.`ORDERS`"),))
    manifest = json.loads((reports / "discovery_manifest.json").read_text())
    manifest["schemas"][0]["views"].append({"name": "V_UNPLANNED"})
    (reports / "discovery_manifest.json").write_text(json.dumps(manifest))
    assert _structure(monkeypatch, reports, _spark()) == 0
    views = _report(reports, "structure_report_sales.json")["views"]
    assert views["V_ORDERS"]["status"] == "created"
    assert views["V_UNPLANNED"]["status"] == "not_in_plan"
    assert views["V_UNPLANNED"]["in_plan"] is False


def test_a_created_view_survives_the_next_run_and_is_skipped(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path, views=(("V_ORDERS",
                                        "`lake`.`SALES`.`ORDERS`"),))
    spark = _spark()
    assert _structure(monkeypatch, reports, spark) == 0
    capsys.readouterr()
    before = len([s for s in spark.statements if "CREATE VIEW" in s])
    assert _structure(monkeypatch, reports, spark) == 0
    assert len([s for s in spark.statements if "CREATE VIEW" in s]) == before
    assert "skip view SALES.V_ORDERS: already created" in capsys.readouterr().out
    views = _report(reports, "structure_report_sales.json")["views"]
    assert views["V_ORDERS"]["status"] == "created"


def test_ctas_mode_still_says_its_views_are_not_created_here(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path)
    spark = _spark()
    _inject_spark(monkeypatch, spark)
    module = _load("01_create_structure")

    class _Src:
        def __init__(self, spark, **_):
            self.spark = spark

        def register_temp_view(self, schema, table, view):
            return f"`ext`.`{schema}`.`{table}`"

        def drop_temp_view(self, view):
            pass
    monkeypatch.setattr(module, "SnowflakeSource", _Src)
    rc = module.main(["--target-catalog", "lake", "--schema", "SALES",
                      "--reports-dir", str(reports), "--output-dir", "",
                      "--mode", "ctas"])
    assert rc == 0
    views = _report(reports, "structure_report_sales.json")["views"]
    assert views["V_ORDERS"]["status"] == "not_created_by_this_path"
    assert not spark.views


# --- 02: the copy scope is tables, never a view ------------------------------

def test_the_copy_never_inserts_into_a_view_01_created(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path)
    spark = _spark()
    _structure(monkeypatch, reports, spark)
    capsys.readouterr()
    for mode in ("skip-existing", "overwrite"):
        rc = _copy(monkeypatch, reports, spark, "--mode", mode)
        assert rc == 0, f"--mode {mode}: every TABLE verified"
        copy = _report(reports, "copy_report_sales.json")
        assert set(copy["tables"]) == {"ORDERS"}
        assert copy["tables"]["ORDERS"]["status"] in ("verified",
                                                      "skipped_nonempty")
    inserts = [s for s in spark.statements if s.upper().startswith("INSERT")]
    assert inserts and not any("`V_" in s.split(" SELECT ")[0]
                               for s in inserts), inserts
    out = capsys.readouterr().out
    assert "scope: 1 table(s) the structure step created" in out
    assert "the manifest lists 1" in out


def test_a_view_left_in_objects_by_an_older_run_is_not_copied(
        monkeypatch, tmp_path, capsys):
    """A structure report written before the fix carries views in
    `objects`; the guard in 02 keeps them out of the scope anyway."""
    reports = _estate(tmp_path)
    (reports / "structure_report_sales.json").write_text(json.dumps(
        {"schema": "SALES", "target": "lake.SALES", "mode": "ddl-plan",
         "objects": {"ORDERS": {"status": "created"},
                     "V_ORDERS": {"status": "created", "kind": "VIEW"},
                     "STRAY": {"status": "created"}}}), encoding="utf-8")
    spark = _spark()
    spark.catalog["`lake`.`SALES`.`ORDERS`"] = list(_SRC_TYPES)
    spark.catalog["`lake`.`SALES`.`V_ORDERS`"] = list(_SRC_TYPES)
    spark.views.append("`lake`.`SALES`.`V_ORDERS`")
    rc = _copy(monkeypatch, reports, spark)
    assert rc == 0
    assert set(_report(reports, "copy_report_sales.json")["tables"]) \
        == {"ORDERS"}
    assert "scope: 1 table(s)" in capsys.readouterr().out


# --- 03: a created view is created, a failed one is a problem ----------------

def test_reconcile_reports_created_and_failed_views_for_what_they_are(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path)
    spark = _spark()
    _structure(monkeypatch, reports, spark)
    _copy(monkeypatch, reports, spark)
    capsys.readouterr()
    rc = _reconcile(monkeypatch, reports, spark)
    assert rc == 1, "a view whose CREATE failed is a problem, not pending"
    rec = _report(reports, "reconciliation.json")
    views = {v["view"]: v for v in rec["schemas"][0]["views"]}
    assert views["V_ORDERS"]["structure"] == "created"
    assert views["V_ORDERS"]["verdict"] == "VIEW_CREATED"
    assert views["V_BAD"]["verdict"] == "VIEW_FAILED"
    assert "TABLE_OR_VIEW_NOT_FOUND" in views["V_BAD"]["reason"]
    reconcile = _load("03_reconcile")
    assert "VIEW_FAILED" in reconcile.PROBLEM_VERDICTS
    assert "VIEW_CREATED" not in reconcile.PROBLEM_VERDICTS
    md = (reports / "MIGRATION_REPORT.md").read_text(encoding="utf-8")
    assert "No table is in a problem state" not in md
    assert "not created by the job path" not in md
    assert "VIEW_NOT_CREATED_BY_THIS_PATH" not in md


def test_reconcile_exits_zero_when_every_planned_view_was_created(
        monkeypatch, tmp_path, capsys):
    reports = _estate(tmp_path, views=(("V_ORDERS",
                                        "`lake`.`SALES`.`ORDERS`"),))
    spark = _spark()
    assert _structure(monkeypatch, reports, spark) == 0
    assert _copy(monkeypatch, reports, spark) == 0
    capsys.readouterr()
    assert _reconcile(monkeypatch, reports, spark) == 0
    assert "not migrated yet" not in capsys.readouterr().out


# --- 01: views are created in the plan's order, not alphabetically -----------
#
# build_ddl_payload emits statements in wave order, so a view follows every
# relation it reads -- views included. 01 collected them into a dict keyed by
# (schema, view) and created them `sorted()`: A_SUMMARY (which reads
# B_DETAIL) went first and failed TABLE_OR_VIEW_NOT_FOUND, exit 1, and each
# extra level of a view-on-view chain needed one more manual re-run.

def test_a_view_on_a_view_is_created_after_the_view_it_reads(
        monkeypatch, tmp_path):
    reports = _estate(tmp_path, views=(
        ("B_DETAIL", "`lake`.`SALES`.`ORDERS`"),
        ("A_SUMMARY", "`lake`.`SALES`.`B_DETAIL`")))
    spark = _spark()
    rc = _structure(monkeypatch, reports, spark)
    assert rc == 0, "one run creates the whole chain"
    created = [s.split()[5] for s in spark.statements
               if s.startswith("CREATE VIEW")]
    assert created == ["`lake`.`SALES`.`B_DETAIL`",
                       "`lake`.`SALES`.`A_SUMMARY`"], created
    views = _report(reports, "structure_report_sales.json")["views"]
    assert {v: r["status"] for v, r in views.items()} == {
        "B_DETAIL": "created", "A_SUMMARY": "created"}


# --- 03: a view the report calls created is LOOKED FOR in the target --------
#
# 03 decided VIEW_CREATED from 01's record alone. A view dropped since, or
# created somewhere else, still read VIEW_CREATED, was not counted as pending,
# and the run exited 0. Tables have MISSING_DESPITE_REPORT for exactly this.

def _one_view_estate(tmp_path):
    return _estate(tmp_path, views=(("V_ORDERS", "`lake`.`SALES`.`ORDERS`"),))


def test_a_created_view_that_is_gone_is_missing_despite_report(
        monkeypatch, tmp_path, capsys):
    reports = _one_view_estate(tmp_path)
    spark = _spark()
    assert _structure(monkeypatch, reports, spark) == 0
    spark.catalog.pop("`lake`.`SALES`.`V_ORDERS`")      # dropped since
    capsys.readouterr()
    assert _reconcile(monkeypatch, reports, spark) == 1
    view = _report(reports, "reconciliation.json")["schemas"][0]["views"][0]
    assert view["verdict"] == "VIEW_MISSING_DESPITE_REPORT"
    assert view["exists_in_target"] is False
    assert "VIEW_MISSING_DESPITE_REPORT" in \
        _load("03_reconcile").PROBLEM_VERDICTS


def test_a_created_view_that_is_there_is_verified(monkeypatch, tmp_path):
    reports = _one_view_estate(tmp_path)
    spark = _spark()
    assert _structure(monkeypatch, reports, spark) == 0
    assert _reconcile(monkeypatch, reports, spark) == 0
    view = _report(reports, "reconciliation.json")["schemas"][0]["views"][0]
    assert view["verdict"] == "VIEW_CREATED"
    assert view["exists_in_target"] is True


class _NoViewListing(_ViewSpark):
    """A catalog whose SHOW TABLES omits views and has no SHOW VIEWS: only
    DESCRIBE can find one, and it answers with `describe_error`."""

    describe_error = None

    def sql(self, statement):
        low = " ".join(statement.split()).lower()
        if low.startswith("show views"):
            raise RuntimeError("SHOW VIEWS is not supported")
        if low.startswith("show tables in"):
            df = super().sql(statement)
            df._rows = [r for r in df._rows
                       if not r["tableName"].startswith("V_")]
            return df
        if low.startswith("describe table") and self.describe_error:
            raise RuntimeError(self.describe_error)
        if low.startswith("describe table"):
            return super().sql("DESCRIBE " + statement.split(None, 2)[2])
        return super().sql(statement)


def _no_listing_spark():
    spark = _NoViewListing({"`ext`.`SALES`.`ORDERS`": list(_SRC_TYPES)})
    spark.counts = {"`ext`.`SALES`.`ORDERS`": 5}
    return spark


def test_a_view_only_describe_can_find_is_still_verified(
        monkeypatch, tmp_path):
    reports = _one_view_estate(tmp_path)
    spark = _no_listing_spark()
    assert _structure(monkeypatch, reports, spark) == 0
    assert _reconcile(monkeypatch, reports, spark) == 0
    view = _report(reports, "reconciliation.json")["schemas"][0]["views"][0]
    assert view["verdict"] == "VIEW_CREATED"


def test_a_view_nobody_could_look_for_is_unreadable_not_created(
        monkeypatch, tmp_path):
    reports = _one_view_estate(tmp_path)
    spark = _no_listing_spark()
    assert _structure(monkeypatch, reports, spark) == 0
    spark.describe_error = "PERMISSION_DENIED: cannot describe"
    assert _reconcile(monkeypatch, reports, spark) == 1
    view = _report(reports, "reconciliation.json")["schemas"][0]["views"][0]
    assert view["verdict"] == "TARGET_UNREADABLE"
    assert view["exists_in_target"] is None

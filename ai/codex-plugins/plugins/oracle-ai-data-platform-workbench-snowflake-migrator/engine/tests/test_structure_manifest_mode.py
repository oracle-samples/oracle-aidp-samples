"""`01_create_structure --mode manifest` must refuse a connector-built
manifest outright, and a ddl-plan run must not trust what another mode made.

The docstring and the overview SKILL both said a connector-mode manifest is
"refused rather than mistranslated". The refusal was a per-table prefix
check for NUMBER/TEXT/VARIANT/OBJECT/GEOGRAPHY/GEOMETRY/TIMESTAMP_LTZ/_TZ,
and it never read manifest["source"]["mode"]. Snowflake reports every
floating-point column as FLOAT, and Spark accepts FLOAT verbatim -- as
32-bit. Observed with a manifest built by the real discover_via_connector:
'[structure] IOT.READINGS: created', 'CREATE TABLE ... (`READING` FLOAT,
...)', exit 0. The copy then narrows every double to ~7 digits
(0.1234567890123 -> 0.12345679104328156), and 02's DECIMAL-only pre-flight
does not catch it.

Following the refusal's own advice afterwards -- re-run with --mode
ddl-plan against the same target -- printed 'skip IOT.READINGS: already
created' and exited 0 with the table still FLOAT: the resume logic trusted
a `created` recorded by a different --mode, whose layout the plan never
checked.
"""
import json

from test_data_migration_scripts import (_CatalogSpark, _FakeSource,
                                         _inject_spark, _load, _report)


def _connector_manifest(reports):
    """What 00_discover_snowflake writes in connector mode, for a table whose
    types all pass the old prefix check."""
    discover = _load("00_discover_snowflake")
    source = _FakeSource(
        tables=[{"TABLE_SCHEMA": "IOT", "TABLE_NAME": "READINGS",
                 "TABLE_TYPE": "BASE TABLE", "ROW_COUNT": 3, "BYTES": 30}],
        columns=[{"TABLE_SCHEMA": "IOT", "TABLE_NAME": "READINGS",
                  "COLUMN_NAME": n, "ORDINAL_POSITION": i, "DATA_TYPE": t,
                  "IS_NULLABLE": "YES", "NUMERIC_PRECISION": None,
                  "NUMERIC_SCALE": None, "CHARACTER_MAXIMUM_LENGTH": None}
                 for i, (n, t) in enumerate(
                     [("READING", "FLOAT"), ("DAY", "DATE"),
                      ("OK", "BOOLEAN")], 1)])
    manifest = {"source": source.describe(), "source_identity": "DB",
                "schemas": discover.discover_via_connector(
                    source, wanted=None, exclude=set())}
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "discovery_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8")
    return manifest


def _run(monkeypatch, reports, spark, *argv):
    _inject_spark(monkeypatch, spark)
    return _load("01_create_structure").main(
        ["--target-catalog", "lake", "--schema", "IOT",
         "--reports-dir", str(reports), *argv])


def test_a_connector_manifest_is_refused_in_manifest_mode(monkeypatch,
                                                          tmp_path, capsys):
    reports = tmp_path / "reports"
    manifest = _connector_manifest(reports)
    # The old check passes this table: none of its types has a listed prefix.
    cols = manifest["schemas"][0]["tables"][0]["columns"]
    assert [c["type"] for c in cols] == ["FLOAT", "DATE", "BOOLEAN"]

    spark = _CatalogSpark()
    rc = _run(monkeypatch, reports, spark, "--mode", "manifest")
    out = capsys.readouterr().out
    assert rc == 1
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements), \
        spark.statements
    assert "connector" in out and "--mode ddl-plan" in out


def test_raw_type_fields_are_refused_even_without_a_source_block(
        monkeypatch, tmp_path):
    """`data_type` is the connector's raw INFORMATION_SCHEMA field; DESCRIBE
    in external-catalog mode never writes it. A manifest whose `source` was
    lost or hand-edited still says where its types came from."""
    reports = tmp_path / "reports"
    manifest = _connector_manifest(reports)
    del manifest["source"]
    (reports / "discovery_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8")
    spark = _CatalogSpark()
    assert _run(monkeypatch, reports, spark, "--mode", "manifest") == 1
    assert not any(s.startswith("CREATE TABLE") for s in spark.statements)


def test_an_external_catalog_manifest_still_creates(monkeypatch, tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir(parents=True)
    (reports / "discovery_manifest.json").write_text(json.dumps({
        "source": {"mode": "external-catalog", "external_catalog": "ext"},
        "schemas": [{"name": "IOT", "views": [], "errors": [], "tables": [
            {"name": "READINGS", "columns": [
                {"name": "READING", "type": "double"},
                {"name": "DAY", "type": "date"}]}]}]}), encoding="utf-8")
    spark = _CatalogSpark()
    rc = _run(monkeypatch, reports, spark, "--mode", "manifest")
    assert rc == 0
    assert _report(reports, "structure_report_iot.json")[
        "objects"]["READINGS"]["status"] == "created"


def _plan(reports, cols):
    plan_dir = reports.parent / "plan"
    plan_dir.mkdir(parents=True, exist_ok=True)
    (plan_dir / "ddl_plan.json").write_text(json.dumps({"statements": [
        {"source_identifier": "DB.IOT.READINGS", "object_type": "TABLE",
         "target_fqn": "lake.IOT.READINGS", "expected_columns": cols}]}),
        encoding="utf-8")


_DOUBLE_PLAN = [{"name": "READING", "type": "DOUBLE"},
                {"name": "DAY", "type": "DATE"},
                {"name": "OK", "type": "BOOLEAN"}]


def _prior_manifest_run(reports, *, per_object_mode):
    """The state the old code left: a FLOAT table, recorded `created`."""
    obj = {"status": "created"}
    if per_object_mode:
        obj["mode"] = "manifest"
    (reports / "structure_report_iot.json").write_text(json.dumps({
        "schema": "IOT", "target": "lake.IOT", "mode": "manifest",
        "objects": {"READINGS": obj}}), encoding="utf-8")
    return _CatalogSpark({"`lake`.`IOT`.`READINGS`": [
        ("READING", "float"), ("DAY", "date"), ("OK", "boolean")]})


def test_ddl_plan_rechecks_a_table_another_mode_recorded_created(
        monkeypatch, tmp_path, capsys):
    for per_object_mode in (True, False):
        reports = tmp_path / str(per_object_mode) / "reports"
        _connector_manifest(reports)
        _plan(reports, _DOUBLE_PLAN)
        spark = _prior_manifest_run(reports, per_object_mode=per_object_mode)
        rc = _run(monkeypatch, reports, spark)            # default ddl-plan
        out = capsys.readouterr().out
        assert "skip IOT.READINGS" not in out, out
        assert rc == 1, "a FLOAT the plan says is DOUBLE is a problem state"
        rec = _report(reports, "structure_report_iot.json")["objects"][
            "READINGS"]
        assert rec["status"] == "type_drift"
        assert "READING DOUBLE" in rec["reason"] and "float" in rec[
            "reason"].lower()


def test_a_same_mode_resume_still_skips(monkeypatch, tmp_path, capsys):
    reports = tmp_path / "reports"
    _connector_manifest(reports)
    _plan(reports, _DOUBLE_PLAN)
    spark = _CatalogSpark()
    assert _run(monkeypatch, reports, spark) == 0
    rec = _report(reports, "structure_report_iot.json")["objects"]["READINGS"]
    # Subset, not equality: the entry also carries `target_fqn`, the
    # target the plan approved. What this test pins is the status and
    # the mode that produced it.
    assert rec["status"] == "created" and rec["mode"] == "ddl-plan"
    assert "unverified_properties" not in rec
    capsys.readouterr()
    assert _run(monkeypatch, reports, spark) == 0
    assert "skip IOT.READINGS: already created" in capsys.readouterr().out

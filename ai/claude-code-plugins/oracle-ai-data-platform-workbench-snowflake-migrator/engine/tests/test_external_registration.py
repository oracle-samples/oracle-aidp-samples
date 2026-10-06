"""External and Iceberg tables: register in place, never copy, never pretend.

An external table's rows are files in the customer's bucket; an Iceberg
table is already an open table format there. Copying either into Delta
through Snowflake reads every byte through a warehouse to re-write what is
already sitting in object storage. The planner used to refuse both as
`unsupported_object`, which was honest and left the operator with nothing.

The path that exists: register the SAME files on AIDP as a table over OCI
Object Storage. The one thing it must never do is pretend the source
location works. Live fact (probe 1, 2026-09-29): the tables an AIDP catalog
holds live under `oci://<bucket>@<namespace>/...`, read with the workspace
identity. An `s3://`, Azure or GCS path would need cloud credentials this
plugin never configures, so no generated statement points there. The files
move first -- a prerequisite the report states, not a step it performs --
and every generated statement carries `oci://<bucket>@<namespace>/...`
placeholders the operator fills, with the S3 path shown only as where the
files come FROM.

Iceberg carries one more trap, stated rather than hidden: its metadata
records ABSOLUTE file paths, so a byte-copy to a new bucket is not yet a
readable table. And an Iceberg table is NOT adopted by `CREATE TABLE ...
USING ICEBERG LOCATION`: by Iceberg's semantics that statement creates a new,
empty table at the location (Hive/REST catalog) or is rejected (Hadoop/path
catalog) -- a filled-in statement that silently reads 0 rows. The only form
that adopts existing snapshots is `CALL <catalog>.system.register_table(table
=> ..., metadata_file => <root metadata file>)`, and the file it must name is
the REWRITTEN root metadata file, which exists only after the path rewrite.
So an Iceberg entry carries that CALL, with the rewrite as its explicit
prerequisite, and is never counted as "registrable as generated".

Nothing here has been run on AIDP. `USING PARQUET|CSV|ICEBERG LOCATION
'oci://...'` is Spark syntax; that an AIDP catalog accepts it is a live
check listed in the report.
"""
import json

import pytest

import snowmig
from emulation.snowflake_fake import ENTERPRISE_DB, enterprise_run_sql
from plan.build import build_plan
from report.render import render_inventory, render_planned_objects
from snowflake_source.conn import assert_read_only
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.dependencies import extract_dependencies
from target.external_registration import (
    build_external_registration, render_external_registration)

REGISTERED = ("SNOWENT.LAKE.EXT_CLICKS", "SNOWENT.LAKE.EXT_PARTNER_FEED",
              "SNOWENT.LAKE.ICE_EVENTS", "SNOWENT.LAKE.ICE_GLUE_ORDERS")


class _Recorder:
    def __init__(self, fail_on=None):
        self.calls, self.fail_on = [], fail_on

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        if self.fail_on and self.fail_on in " ".join(sql.split()).lower():
            raise RuntimeError("SQL access control error: insufficient privileges")
        return enterprise_run_sql(sql, params)


@pytest.fixture(scope="module")
def planned():
    inv = build_inventory(enterprise_run_sql, [ENTERPRISE_DB])
    plan = build_plan(inv, extract_dependencies(enterprise_run_sql, inv))
    return inv, plan


@pytest.fixture(scope="module")
def reg(planned):
    inv, plan = planned
    run = _Recorder()
    return build_external_registration(run, inv, plan), run


def _entry(reg, ident):
    return next(e for e in reg["tables"] if e["source_identifier"] == ident)


# ------------------------------------------------------------- the planner

@pytest.mark.parametrize("ident", REGISTERED)
def test_external_and_iceberg_tables_are_planned_register_in_place(planned, ident):
    _, plan = planned
    entry = next(c for c in plan["cannot_migrate"]
                 if c["source_identifier"] == ident)
    assert entry["category"] == "register_in_place"
    # The reason carries the prerequisite and where the statements are.
    assert "OCI Object Storage" in entry["reason"]
    assert "EXTERNAL_REGISTRATION.md" in entry["reason"]
    assert "moved" in entry["reason"]


def test_hybrid_and_event_tables_are_still_refused(planned):
    _, plan = planned
    cannot = {c["source_identifier"]: c["category"] for c in plan["cannot_migrate"]}
    assert cannot["SNOWENT.OPS.HYB_SESSIONS"] == "unsupported_object"
    assert cannot["SNOWENT.OPS.APP_EVENTS"] == "unsupported_object"


def test_the_inventory_and_planned_objects_say_register_in_place(planned):
    inv, plan = planned
    md = render_inventory(inv)
    line = next(l for l in md.splitlines() if "`SNOWENT.LAKE.EXT_CLICKS`" in l)
    assert line.rstrip().endswith("| register in place (external table) |")
    line = next(l for l in md.splitlines() if "`SNOWENT.LAKE.ICE_EVENTS`" in l)
    assert line.rstrip().endswith("| register in place (Iceberg table) |")
    planned_md = render_planned_objects(plan)
    assert "Registered in place over OCI Object Storage" in planned_md
    assert "(`register_in_place`) — 4" in planned_md


# ------------------------------------------------ the registration report

def test_every_statement_it_sent_to_snowflake_is_a_read(reg):
    _, run = reg
    assert run.calls
    for sql in run.calls:
        assert_read_only(sql)
    flat = [" ".join(c.split()).lower() for c in run.calls]
    # Bounded: one SHOW per kind in the one schema that holds them, one
    # DESCRIBE per external volume -- never a read per table.
    assert sum("show external tables" in c for c in flat) == 1
    assert sum("show iceberg tables" in c for c in flat) == 1
    assert sum("describe external volume" in c for c in flat) == 1


def test_no_generated_location_is_the_source_location(reg):
    report, _ = reg
    for e in report["tables"]:
        stmt = e["statement"] or ""
        assert "s3://" not in stmt and "azure://" not in stmt and "gcs://" not in stmt
        if e["kind"] == "external table":
            assert "LOCATION 'oci://<bucket>@<namespace>/" in stmt, stmt
        else:
            assert "metadata_file => 'oci://<bucket>@<namespace>/" in stmt, stmt


def test_a_parquet_external_table_registers_as_parquet(reg):
    report, _ = reg
    e = _entry(report, "SNOWENT.LAKE.EXT_CLICKS")
    assert e["kind"] == "external table"
    assert e["move_from"] == "s3://emu-ent-lake/raw/clicks/"
    assert e["file_format_type"] == "PARQUET"
    assert e["statement"] == (
        "CREATE TABLE IF NOT EXISTS `snowent`.`lake`.`ext_clicks` USING PARQUET "
        "LOCATION 'oci://<bucket>@<namespace>/raw/clicks/'")
    # The schema is the files', not Snowflake's virtual columns.
    assert any("virtual column" in n for n in e["notes"])
    assert "CLICK_ID" in " ".join(e["notes"])


def test_a_csv_external_table_carries_its_option_placeholders(reg):
    report, _ = reg
    e = _entry(report, "SNOWENT.LAKE.EXT_PARTNER_FEED")
    assert "USING CSV" in e["statement"]
    # Snowflake's delimiter/header options were never read, so they are a
    # placeholder the statement cannot run with, not a default it guesses.
    assert "OPTIONS (header '<true|false>', sep '<delimiter>')" in e["statement"]
    assert any("FILE_FORMAT options" in n for n in e["notes"])


def test_a_managed_iceberg_table_names_its_full_source_path_and_the_path_trap(reg):
    report, _ = reg
    e = _entry(report, "SNOWENT.LAKE.ICE_EVENTS")
    assert e["kind"] == "Iceberg table"
    assert e["move_from"] == "s3://emu-ent-iceberg/warehouse/ice_events/"
    assert any("absolute" in n.lower() for n in e["notes"])


def test_an_iceberg_table_is_adopted_by_register_table_never_by_create_table(reg):
    # CREATE TABLE ... USING ICEBERG LOCATION does not adopt existing
    # metadata: it makes a new, empty table there, or a path catalog refuses
    # it. Filled in, it would register a table that reads 0 rows. The form
    # that adopts the moved snapshots is register_table over the REWRITTEN
    # root metadata file -- a file that exists only after the path rewrite,
    # so the placeholder naming it cannot be satisfied before then.
    report, _ = reg
    e = _entry(report, "SNOWENT.LAKE.ICE_EVENTS")
    assert e["statement"] == (
        "CALL <iceberg_catalog>.system.register_table("
        "table => 'lake.ice_events', "
        "metadata_file => 'oci://<bucket>@<namespace>/warehouse/ice_events/"
        "metadata/<rewritten root metadata file>')")
    assert e["oci_path"] == "oci://<bucket>@<namespace>/warehouse/ice_events/"
    # Not registrable as generated: the rewrite comes first.
    assert e["registrable"] is False
    assert e["needs_metadata_rewrite"] is True
    assert e["error"] is None
    # The source root metadata file is named, so the operator knows which
    # file the rewrite must produce the counterpart of.
    assert any("00003-0f0e0d0c" in n for n in e["notes"])
    for t in report["tables"]:
        if t["kind"] == "Iceberg table":
            assert "CREATE TABLE" not in (t["statement"] or "")
            assert "USING ICEBERG" not in (t["statement"] or "")


def test_iceberg_entries_are_not_counted_as_having_a_generated_registration(reg):
    report, _ = reg
    assert report["registrable"] == 2       # the two external tables only
    assert report["after_rewrite"] == 2     # the two Iceberg tables
    md = render_external_registration(report)
    assert "## Tables to register — 2 of 4" in md
    # The report names CREATE ... USING ICEBERG only as the thing NOT to
    # run: no SQL block in it carries that statement.
    blocks = [b.split("```", 1)[0] for b in md.split("```sql")[1:]]
    assert blocks and not any("USING ICEBERG" in b for b in blocks)
    section = md.split("## Iceberg tables — not registrable as generated", 1)[1]
    section = section.split("\n## ", 1)[0]
    for ident in ("SNOWENT.LAKE.ICE_EVENTS", "SNOWENT.LAKE.ICE_GLUE_ORDERS"):
        assert f"`{ident}`" in section
    # Both ways forward are named: rewrite then register, or re-write.
    assert "rewrite_table_path" in section
    assert "register_table" in section
    assert "re-write the table" in section.lower()
    assert "reads 0 rows" in section or "empty table" in section


def test_an_externally_catalogued_iceberg_table_says_its_catalog_is_elsewhere(reg):
    report, _ = reg
    e = _entry(report, "SNOWENT.LAKE.ICE_GLUE_ORDERS")
    assert e["catalog"] == "GLUE_ENT_CATALOG"
    assert e["catalog_source"] == "GLUE"
    # Snowflake does not hold the table's location; Glue does.
    assert e["move_from"] is None
    assert ("metadata_file => 'oci://<bucket>@<namespace>/<path>/metadata/"
            "<rewritten root metadata file>'") in e["statement"]
    assert e["registrable"] is False and e["needs_metadata_rewrite"] is True
    assert any("glue" in n.lower() and "fork" in n for n in e["notes"])


def test_the_report_states_the_move_as_a_prerequisite_first(reg):
    report, _ = reg
    md = render_external_registration(report)
    head = md.split("## ", 2)[1]
    assert head.startswith("Prerequisites")
    assert "moves no bytes" in md
    assert "s3://" in md, "the source location is named as where files come FROM"
    assert "NOT live-verified" in md
    assert "<bucket>" in md and "<namespace>" in md


def test_an_unreadable_detail_is_named_not_dropped(planned):
    inv, plan = planned
    report = build_external_registration(
        _Recorder(fail_on="show iceberg tables"), inv, plan)
    ice = [e for e in report["tables"] if e["kind"] == "Iceberg table"]
    assert len(ice) == 2
    for e in ice:
        assert e["registrable"] is False
        assert e["statement"] is None
        assert "insufficient privileges" in e["error"]
    assert report["unreadable"]


def test_an_external_table_the_inventory_missed_is_listed(planned):
    # If SHOW TABLES ever omits an external table, SHOW EXTERNAL TABLES
    # still names it, and the report says the inventory did not.
    inv, plan = planned
    inv = json.loads(json.dumps(inv))
    inv["inventory"] = [r for r in inv["inventory"]
                        if r["source_identifier"] != "SNOWENT.LAKE.EXT_PARTNER_FEED"]
    plan = build_plan(inv, {"edges": []})
    report = build_external_registration(enterprise_run_sql, inv, plan)
    assert report["not_in_inventory"] == ["SNOWENT.LAKE.EXT_PARTNER_FEED"]


# ------------------------------------------------------------------ the CLI

def test_the_cli_writes_both_artifacts(tmp_path, monkeypatch, planned, capsys):
    inv, plan = planned
    (tmp_path / "inventory.json").write_text(json.dumps(inv, default=str), encoding="utf-8")
    (tmp_path / "plan.json").write_text(json.dumps(plan, default=str), encoding="utf-8")
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: enterprise_run_sql)
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: None)
    rc = snowmig.main(["external-registration", "--out-dir", str(tmp_path)])
    assert rc == 0
    data = json.loads((tmp_path / "external_registration.json").read_text(encoding="utf-8"))
    assert len(data["tables"]) == 4
    assert (tmp_path / "EXTERNAL_REGISTRATION.md").is_file()
    # The count says what was generated: the Iceberg CALLs wait on a rewrite.
    said = capsys.readouterr().out
    assert "2 of 4 external/Iceberg table(s) have a generated registration" in said
    assert "2 Iceberg table(s)" in said and "rewrite" in said


def test_the_cli_with_nothing_to_register_says_so(tmp_path, monkeypatch):
    (tmp_path / "inventory.json").write_text(json.dumps(
        {"databases_in_scope": ["D"], "inventory": []}), encoding="utf-8")
    (tmp_path / "plan.json").write_text(json.dumps(
        {"cannot_migrate": [], "target_names": {}}), encoding="utf-8")
    # With no candidate, the only read is the safety net: one SHOW EXTERNAL
    # TABLES per database, in case SHOW TABLES did not list one.
    from fake_sql import FakeSql
    fake = FakeSql({"show external tables in database": []})
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: fake)
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: None)
    assert snowmig.main(["external-registration", "--out-dir", str(tmp_path)]) == 0
    assert len(fake.calls) == 1
    md = (tmp_path / "EXTERNAL_REGISTRATION.md").read_text(encoding="utf-8")
    assert "No external or Iceberg table" in md


def test_a_backslash_in_an_iceberg_name_cannot_close_the_literal():
    """Snowflake reads `\\'` as an escaped quote, so the name's backslash must
    be doubled too, or a crafted name ends the literal and runs as SQL."""
    from snowflake_source.dialect import lexer
    name = "X\\') union select current_user() --"
    ident = f"DB.SC.{name}"
    inv = {"databases_in_scope": [],
           "inventory": [{"source_identifier": ident, "source_database": "DB",
                          "source_schema": "SC",
                          "source_metadata": {"is_iceberg": "Y"}}]}
    plan = {"cannot_migrate": [{"source_identifier": ident,
                                "category": "register_in_place"}]}
    sent = []

    def run_sql(sql, params=None):
        sent.append(sql)
        if sql.startswith("show iceberg tables"):
            return [{"database_name": "DB", "schema_name": "SC", "name": name,
                     "catalog_name": "SNOWFLAKE"}]
        return [{"INFO": "{}"}]

    build_external_registration(run_sql, inv, plan)
    probe = next(s for s in sent if "get_iceberg_table_information" in s)
    assert_read_only(probe)
    strings = [text for kind, text in lexer.segments(probe) if kind == "string"]
    assert len(strings) == 1 and "union select" in strings[0]
    assert "union" not in lexer.code_only(probe).lower()

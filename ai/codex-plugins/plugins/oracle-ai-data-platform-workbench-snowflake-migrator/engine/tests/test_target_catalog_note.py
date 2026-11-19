"""PLANNED_OBJECTS.md must name the schemas the structure job really creates.

The structure job (01_create_structure, S10) was fixed to take its namespace
from each approved `target_fqn` and to refuse a plan whose catalog is not
its `--target-catalog`. The plan's note and the report section still
described the job before that fix: "does not read this column", "Schemas the
structure job (S10) creates: `core`", and an "Older deploy/notebook path
only: `lake.snowdb_core`" line. So with a prefix the approval artifact said
S10 creates `core` while the job ran `CREATE SCHEMA lake.snowdb_core`. With
no prefix (README step 4 as written) the note told the operator to pass the
S4 INTERNAL catalog as --target-catalog, and the job refused exactly that
with exit 1 -- under a remedy, "Re-run `ddl` for this catalog", naming a
flag `ddl` does not have.

These tests build the plan, render the report, and run the real job's
main() in --dry-run against the generated ddl_plan.json, so the report and
the job are compared rather than each checked against a hand-written
expectation.
"""
import json

from plan.build import build_plan
from report.render import render_planned_objects
from target.ddl import build_ddl_payload
from test_data_migration_scripts import _CatalogSpark, _inject_spark, _load


def _inventory():
    col = {"COLUMN_NAME": "ID", "DATA_TYPE": "NUMBER",
           "target_type": "DECIMAL(38,0)", "IS_NULLABLE": "YES",
           "ORDINAL_POSITION": 1, "COMMENT": None,
           "NUMERIC_PRECISION": 38, "NUMERIC_SCALE": 0}
    return {"inventory": [{
        "source_identifier": "SNOWDB.CORE.ORDERS", "object_type": "TABLE",
        "source_database": "SNOWDB", "source_schema": "CORE",
        "compatibility_status": "supported", "blocked_reasons": [],
        "columns": [col], "row_count_exact": 1, "source_metadata": {}}]}


def _stage(tmp_path, plan):
    """ddl_plan.json and the discovery manifest where the job reads them."""
    reports = tmp_path / "reports"
    reports.mkdir()
    (reports / "discovery_manifest.json").write_text(json.dumps({"schemas": [
        {"name": "CORE", "tables": [{"name": "ORDERS", "columns": []}],
         "views": [], "errors": []}]}), encoding="utf-8")
    (tmp_path / "plan").mkdir()
    (tmp_path / "plan" / "ddl_plan.json").write_text(
        json.dumps(build_ddl_payload(_inventory(), plan)), encoding="utf-8")
    return reports


def _run_structure(monkeypatch, capsys, reports, *args):
    _inject_spark(monkeypatch, _CatalogSpark())
    rc = _load("01_create_structure").main(
        ["--reports-dir", str(reports), "--dry-run", *args])
    return rc, capsys.readouterr().out


def _structure_section(md):
    return md.split("## Target structure to exist first", 1)[1].split("\n## ", 1)[0]


def test_with_a_prefix_the_report_names_the_schema_the_job_creates(
        tmp_path, monkeypatch, capsys):
    plan = build_plan(_inventory(), {"edges": []}, bronze_catalog_prefix="lake")
    section = _structure_section(render_planned_objects(plan))
    line = next(l for l in section.splitlines()
                if l.startswith("Schemas the structure job"))
    assert "`lake.snowdb_core`" in line, line
    assert "Older" not in section and "does not read" not in section
    assert "'lake'" in plan["target_catalog_note"]

    rc, out = _run_structure(monkeypatch, capsys, _stage(tmp_path, plan),
                             "--target-catalog", "lake")
    assert rc == 0, out
    assert "CREATE SCHEMA IF NOT EXISTS `lake`.`snowdb_core`" in out


def test_without_a_prefix_the_report_does_not_send_the_run_into_a_refusal(
        tmp_path, monkeypatch, capsys):
    plan = build_plan(_inventory(), {"edges": []})
    note = plan["target_catalog_note"]
    section = _structure_section(render_planned_objects(plan))
    # The old note: "Read the Target column with that catalog in place of
    # snowdb" -- i.e. pass the S4 INTERNAL catalog. The job refuses it:
    rc, out = _run_structure(monkeypatch, capsys, _stage(tmp_path, plan),
                             "--target-catalog", "snowmig_internal")
    assert rc == 1
    assert "in place of" not in note
    assert "does not read" not in note and "Older" not in section
    # What does work, said in the approval artifact itself.
    assert "`plan --bronze-catalog-prefix" in note
    assert "`plan --bronze-catalog-prefix" in section
    # And the refusal's remedy names a command that exists.
    assert "Re-run `ddl` for this catalog" not in out
    assert "plan --bronze-catalog-prefix snowmig_internal" in out


def test_the_note_is_what_the_job_does_when_the_catalogs_agree(
        tmp_path, monkeypatch, capsys):
    # db style: the report must say `lake.snowdb`, which is what is created.
    plan = build_plan(_inventory(), {"edges": []}, bronze_catalog_prefix="lake",
                      bronze_schema_style="db")
    line = next(l for l in _structure_section(render_planned_objects(plan))
                .splitlines() if l.startswith("Schemas the structure job"))
    assert "`lake.snowdb`" in line, line
    rc, out = _run_structure(monkeypatch, capsys, _stage(tmp_path, plan),
                             "--target-catalog", "lake")
    assert rc == 0, out
    assert "CREATE SCHEMA IF NOT EXISTS `lake`.`snowdb`" in out


# The skills are what an agent runs from, so they must give the same plan
# command README step 4 gives. After the note above was fixed, the runbook
# skill still said `snowmig plan [--restrictions <file>]` before `ddl` --
# the no-prefix plan S10 refuses with exit 1 -- and the plan skill still
# called the prefix an optional variation "for deployments that want a
# single bronze catalog".
_ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]


def _skill(name):
    return (_ROOT / "skills" / name / "SKILL.md").read_text(encoding="utf-8")


def test_the_runbook_skill_plans_with_the_s4_internal_catalog_as_prefix():
    text = _skill("snowflake-migrator-overview")
    block = text.split('bin/snowmig" plan', 1)[1].split('bin/snowmig" ddl', 1)[0]
    assert "--bronze-catalog-prefix" in block, block
    flat = " ".join(block.split())
    assert "INTERNAL" in flat and "S4" in flat, block


def test_the_plan_skill_says_the_prefix_names_the_s4_internal_catalog():
    text = _skill("snowflake-migration-plan")
    flat = " ".join(text.split())
    assert "for deployments that want a single bronze catalog" not in flat
    assert "[--bronze-catalog-prefix bronze]" not in flat
    assert "S4 INTERNAL catalog" in flat
    assert "EXTERNAL" in flat and "S10 refuses" in flat

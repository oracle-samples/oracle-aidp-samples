"""Export-supplied names cannot steer output paths (SEC-AIDP-SAMPLES-INFA-H1).

``run_migration`` names every file it writes after text read straight out
of the customer export: ``<FOLDER NAME>`` becomes the notebook directory,
``<MAPPING NAME>`` the notebook / lineage / comparison file, ``<WORKFLOW
NAME>`` the job definition and its review file; the IICS parser feeds
``project`` / ``folder`` / ``name`` into the same places. None of it was
sanitised, so an absolute FOLDER NAME made ``os.path.join`` discard the
``-o`` directory entirely and a ``..`` segment climbed out of it -- a
tampered export could overwrite a ``CLAUDE.md``, a ``settings.json`` or
another engagement's reviewed notebook wherever it pointed, and a name the
filesystem refused aborted the whole run with an uncaught OSError.

Pinned here:
  - ``safe_path_component`` reduces any name to one path component and
    leaves legal Informatica names untouched.
  - ``ensure_within`` refuses a path that resolves outside the base dir.
  - A PowerCenter XML and an IICS JSON export with an absolute folder, a
    ``x/../..`` mapping name and a ``../..`` workflow name produce every
    file under the output directory, nothing outside it, the run does not
    raise, and the renames are reported (summary line + CSV).
  - The same holds for ``infa2aidp lineage`` and for the parallel
    ``BatchMigrator`` path.
  - A notebook that cannot be written is that mapping's failure, not the
    run's.
"""
from __future__ import annotations

import csv
import json
import logging
import os
from pathlib import Path

import pytest

from infa2aidp import config as cfg
from infa2aidp.batch import BatchMigrator
from infa2aidp.cli import main
from infa2aidp.migrator import format_run_summary, run_migration
from infa2aidp.parsers.security import SecurityError

try:
    from infa2aidp.parsers.security import ensure_within, safe_path_component
except ImportError:  # a tree without the fix: the behavioural tests below still run (and fail)
    ensure_within = safe_path_component = None

needs_helpers = pytest.mark.skipif(safe_path_component is None, reason="helpers missing")

HOSTILE_MAPPING = "x/../.."
HOSTILE_WORKFLOW = "../.."


def _powercenter_xml(folder: str, mapping: str, workflow: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
<POWERMART CREATION_DATE="01/01/2024 00:00:00" REPOSITORY_VERSION="188.97">
<REPOSITORY NAME="REPO" VERSION="188" CODEPAGE="UTF-8" DATABASETYPE="Oracle">
<FOLDER NAME="{folder}" GROUP="" OWNER="x" SHARED="NOTSHARED" DESCRIPTION="" PERMISSIONS="rwx---r--" UUID="1">
  <SOURCE NAME="SRC" DBDNAME="DB" DATABASETYPE="Oracle" OWNERNAME="S" DESCRIPTION="" BUSINESSNAME="">
    <SOURCEFIELD NAME="ID" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL" KEYTYPE="PRIMARY KEY" FIELDNUMBER="1" PHYSICALOFFSET="0" PHYSICALLENGTH="10" PICTURETEXT="" USAGE_FLAGS=""/>
  </SOURCE>
  <TARGET NAME="TGT" DATABASETYPE="Oracle" DESCRIPTION="" BUSINESSNAME="" CONSTRAINT="" TABLEOPTIONS="">
    <TARGETFIELD NAME="ID" DATATYPE="number" PRECISION="10" SCALE="0" NULLABLE="NOTNULL" KEYTYPE="PRIMARY KEY" FIELDNUMBER="1" PICTURETEXT="" BUSINESSNAME=""/>
  </TARGET>
  <MAPPING NAME="{mapping}" DESCRIPTION="" ISVALID="YES" OBJECTVERSION="1" VERSIONNUMBER="1">
    <TRANSFORMATION NAME="SQ_SRC" TYPE="Source Qualifier" DESCRIPTION="" OBJECTVERSION="1" REUSABLE="NO" VERSIONNUMBER="1">
      <TRANSFORMFIELD NAME="ID" DATATYPE="decimal" PRECISION="10" SCALE="0" PORTTYPE="INPUT/OUTPUT" DEFAULTVALUE="" DESCRIPTION="" PICTURETEXT=""/>
    </TRANSFORMATION>
    <INSTANCE NAME="SRC" TYPE="SOURCE" TRANSFORMATION_NAME="SRC" TRANSFORMATION_TYPE="Source Definition" DBDNAME="DB" DESCRIPTION="" REUSABLE="NO"/>
    <INSTANCE NAME="SQ_SRC" TYPE="TRANSFORMATION" TRANSFORMATION_NAME="SQ_SRC" TRANSFORMATION_TYPE="Source Qualifier" DESCRIPTION="" REUSABLE="NO"/>
    <INSTANCE NAME="TGT" TYPE="TARGET" TRANSFORMATION_NAME="TGT" TRANSFORMATION_TYPE="Target Definition" DESCRIPTION="" REUSABLE="NO"/>
    <CONNECTOR FROMFIELD="ID" FROMINSTANCE="SRC" FROMINSTANCETYPE="Source Definition" TOFIELD="ID" TOINSTANCE="SQ_SRC" TOINSTANCETYPE="Source Qualifier"/>
    <CONNECTOR FROMFIELD="ID" FROMINSTANCE="SQ_SRC" FROMINSTANCETYPE="Source Qualifier" TOFIELD="ID" TOINSTANCE="TGT" TOINSTANCETYPE="Target Definition"/>
  </MAPPING>
  <WORKFLOW NAME="{workflow}" DESCRIPTION="" ISENABLED="YES" ISVALID="YES" REUSABLE_SCHEDULER="NO" SCHEDULERNAME="S" SERVERNAME="IS" SERVER_DOMAINNAME="D" SUSPEND_ON_ERROR="NO" TASKS_MUST_RUN_ON_SERVER="NO" VERSIONNUMBER="1">
    <SCHEDULER DESCRIPTION="" NAME="S" REUSABLE="NO" VERSIONNUMBER="1"><SCHEDULERINFO SCHEDULETYPE="ONDEMAND"/></SCHEDULER>
    <TASKINSTANCE DESCRIPTION="" ISENABLED="YES" NAME="Start" REUSABLE="NO" TASKNAME="Start" TASKTYPE="Start"/>
  </WORKFLOW>
</FOLDER>
</REPOSITORY>
</POWERMART>
"""


def _iics_json(project: str, name: str) -> str:
    return json.dumps({
        "name": name,
        "project": project,
        "description": "IICS mapping with a hostile project and name",
        "transformations": [
            {"name": "SRC_CUST", "type": "SOURCE",
             "fields": [{"name": "CUST_ID", "datatype": "integer"}]},
            {"name": "TGT_CUST", "type": "TARGET",
             "fields": [{"name": "CUST_ID", "datatype": "integer"}]},
        ],
        "connections": [
            {"from": {"transformation": "SRC_CUST", "field": "CUST_ID"},
             "to": {"transformation": "TGT_CUST", "field": "CUST_ID"}},
        ],
    })


class _Arena:
    """tmp/in (export), tmp/out (-o), tmp/ESCAPED (where the export points)."""

    def __init__(self, tmp_path: Path):
        self.root = tmp_path
        self.inp = tmp_path / "in"
        self.out = tmp_path / "out"
        self.escaped = tmp_path / "ESCAPED"
        for d in (self.inp, self.out, self.escaped):
            d.mkdir()
        # An absolute path, as the hunter's repro used: the whole directory
        # becomes attacker-chosen. Forward slashes so the same string is an
        # absolute path on both platforms.
        self.hostile_folder = str(self.escaped).replace("\\", "/")

    def files_outside_out(self) -> list[str]:
        inside = {p.resolve() for p in self.out.rglob("*") if p.is_file()}
        inputs = {p.resolve() for p in self.inp.rglob("*") if p.is_file()}
        return sorted(
            str(p) for p in self.root.rglob("*")
            if p.is_file() and p.resolve() not in inside and p.resolve() not in inputs
        )

    def files_inside_out(self) -> list[str]:
        return sorted(str(p.relative_to(self.out)) for p in self.out.rglob("*") if p.is_file())


def _assert_contained(arena: _Arena, expect_suffixes: tuple[str, ...]) -> None:
    outside = arena.files_outside_out()
    assert outside == [], f"written outside -o: {outside}"
    assert list(arena.escaped.rglob("*")) == []
    inside = arena.files_inside_out()
    for suffix in expect_suffixes:
        assert any(f.endswith(suffix) for f in inside), f"no {suffix} under -o: {inside}"
    for f in inside:
        for part in Path(f).parts:
            assert ".." not in part and ":" not in part, f


# ── the primitives ────────────────────────────────────────────────────

@needs_helpers
@pytest.mark.parametrize("name", [
    "m_ORDERS_TRANSFORM", "wf_SDE_Daily", "SALES", "m_v1.2", "a-b_c", "Migrated",
    # Unicode-mode repositories and IDMC allow non-ASCII names
    "m_顧客", "wf_Zahlungen_tägl", "Ventes_Été", "m_заказы",
])
def test_legal_informatica_names_are_unchanged(name):
    assert safe_path_component(name) == name


@needs_helpers
def test_distinct_non_ascii_names_stay_distinct():
    """An ASCII-only class turned every non-ASCII letter into ``_``, so two
    different mappings became the same file (``m___``) and a NAME CLASH."""
    assert safe_path_component("m_顧客") != safe_path_component("m_注文")
    assert "_" * 3 not in safe_path_component("m_顧客")


@needs_helpers
@pytest.mark.parametrize("name, forbidden", [
    ("a／b", "/"),            # fullwidth solidus normalises to the separator it looks like
    ("a＼b", "\\"),
    ("C：\\x", ":"),
    ("a\u202eb", "\u202e"),  # right-to-left override
    ("a\u200bb", "\u200b"),  # zero-width space
    ("a\x01b", "\x01"),
])
def test_unicode_lookalikes_and_format_characters_are_still_replaced(name, forbidden):
    safe = safe_path_component(name)
    assert forbidden not in safe and "/" not in safe and ":" not in safe
    assert os.path.basename(safe) == safe


@needs_helpers
def test_a_fullwidth_device_name_is_still_prefixed():
    assert safe_path_component("ＣＯＮ").upper() != "CON"


def test_unicode_mode_names_are_written_as_is_and_not_reported(tmp_path, caplog):
    """Two mappings from a Unicode-mode repository keep their names: separate
    notebooks, no NAME CLASH suffix, no RENAMED line, no sanitised_names.csv.
    (Also pins that the DDL and report writers do not depend on the platform
    default encoding -- cp1252 on Windows cannot encode these names.)"""
    arena = _Arena(tmp_path)
    for i, (mapping, workflow) in enumerate([("m_顧客", "wf_顧客"), ("m_注文", "wf_注文")]):
        (arena.inp / f"{i}.xml").write_text(_powercenter_xml("売上", mapping, workflow), encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="infa2aidp.migrator"):
        result = run_migration(sorted(str(p) for p in arena.inp.glob("*.xml")), str(arena.out),
                               use_llm=False, score_confidence=False, skip_lineage=True, skip_optimize=True)

    assert (result.notebooks, result.workflows) == (2, 2)
    assert (arena.out / "売上" / "nb_m_顧客.ipynb").is_file()
    assert (arena.out / "売上" / "nb_m_注文.ipynb").is_file()
    assert result.renamed_paths == []
    assert "RENAMED" not in format_run_summary(result)
    assert "NAME CLASH" not in format_run_summary(result)
    assert "is not a safe file name" not in caplog.text
    assert not (arena.out / "reports" / "sanitised_names.csv").exists()
    assert arena.files_outside_out() == []


@needs_helpers
@pytest.mark.parametrize("name", [
    "/home/consultant", "../../../../CLAUDE", "x/../..", "../..", "..", "C:\\Users\\x",
    "C:/x", "\\\\server\\share", "a/b", "a\\b", ".claude", "a\x00b", "...", "nb/../../x",
])
def test_hostile_names_become_one_safe_component(name):
    safe = safe_path_component(name)
    assert safe
    assert "/" not in safe and "\\" not in safe and ":" not in safe and "\x00" not in safe
    assert ".." not in safe
    assert not safe.startswith(".") and not safe.startswith("/")
    assert os.path.basename(safe) == safe
    assert os.path.join("out", safe).startswith("out")


@needs_helpers
def test_empty_name_gets_the_fallback():
    assert safe_path_component("") == "unnamed"
    assert safe_path_component(None) == "unnamed"
    assert safe_path_component(".", fallback="x") == "x"


@needs_helpers
@pytest.mark.parametrize("name", ["CON", "nul", "COM1.ipynb", "Lpt9"])
def test_windows_device_names_are_prefixed(name):
    safe = safe_path_component(name)
    assert safe.split(".")[0].upper() not in {"CON", "NUL", "COM1", "LPT9"}
    assert safe.endswith(name)


@needs_helpers
def test_ensure_within_accepts_the_base_and_anything_below_it(tmp_path):
    base = tmp_path / "out"
    base.mkdir()
    assert ensure_within(str(base), str(base)) == str(base)
    inner = str(base / "SALES" / "nb_m.ipynb")
    assert ensure_within(str(base), inner) == inner


@needs_helpers
@pytest.mark.parametrize("rel", ["../x", "sub/../../x", "../out2/x"])
def test_ensure_within_refuses_a_path_that_resolves_outside(tmp_path, rel):
    base = tmp_path / "out"
    base.mkdir()
    with pytest.raises(SecurityError):
        ensure_within(str(base), os.path.join(str(base), *rel.split("/")))


@needs_helpers
def test_ensure_within_refuses_an_absolute_path_elsewhere(tmp_path):
    base = tmp_path / "out"
    base.mkdir()
    with pytest.raises(SecurityError):
        ensure_within(str(base), str(tmp_path / "ESCAPED" / "CLAUDE.md"))
    # A sibling whose name merely starts with the base name is outside too.
    with pytest.raises(SecurityError):
        ensure_within(str(base), str(tmp_path / "out2" / "x"))


# ── run_migration: PowerCenter XML ────────────────────────────────────

def test_powercenter_export_with_hostile_names_stays_inside_output_dir(tmp_path, caplog):
    arena = _Arena(tmp_path)
    xml = arena.inp / "evil.xml"
    xml.write_text(_powercenter_xml(arena.hostile_folder, HOSTILE_MAPPING, HOSTILE_WORKFLOW),
                   encoding="utf-8")

    # at_level, not the bare fixture: it also re-enables logging that an
    # earlier test module (the golden harness) left disabled for the process.
    with caplog.at_level(logging.WARNING, logger="infa2aidp.migrator"):
        result = run_migration(
            [str(xml)], str(arena.out), use_llm=False, emit_comparison=True,
            score_confidence=False, skip_lineage=False, skip_optimize=True,
        )

    assert (result.notebooks, result.workflows) == (1, 1), "the run must not abort"
    _assert_contained(arena, (".ipynb", ".json", "_comparison.md", "_comparison.html"))
    # lineage .md/.json live under out/lineage, not two levels up
    assert any(f.startswith("lineage") and f.endswith(".md") for f in arena.files_inside_out())
    # the job definition points at the folder the notebook was actually written to
    wf_json = next(p for p in (arena.out / "workflows").glob("*.json"))
    job = json.loads(wf_json.read_text(encoding="utf-8"))
    for task in job.get("tasks", []):
        nb_path = task.get("notebookPath", "")
        assert ".." not in nb_path and arena.hostile_folder not in nb_path

    # every rename is reported, never silent
    kinds = {k for k, _o, _s in result.renamed_paths}
    assert kinds == {"folder", "mapping", "workflow"}
    originals = {o for _k, o, _s in result.renamed_paths}
    assert {arena.hostile_folder, HOSTILE_MAPPING, HOSTILE_WORKFLOW} <= originals
    assert "RENAMED: 3" in format_run_summary(result)
    assert "is not a safe file name" in caplog.text
    with open(arena.out / "reports" / "sanitised_names.csv", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    assert {r["original"] for r in rows} >= {HOSTILE_MAPPING, HOSTILE_WORKFLOW}
    assert all(r["written_as"] == safe_path_component(r["original"]) for r in rows)


def test_a_clean_export_reports_no_renames(tmp_path):
    arena = _Arena(tmp_path)
    xml = arena.inp / "ok.xml"
    xml.write_text(_powercenter_xml("SALES", "m_ORDERS", "wf_ORDERS"), encoding="utf-8")
    result = run_migration([str(xml)], str(arena.out), use_llm=False,
                           score_confidence=False, skip_lineage=True, skip_optimize=True)
    assert result.renamed_paths == []
    assert "RENAMED" not in format_run_summary(result)
    assert not (arena.out / "reports" / "sanitised_names.csv").exists()
    assert (arena.out / "SALES" / "nb_m_ORDERS.ipynb").is_file()
    assert (arena.out / "workflows" / "wf_ORDERS.json").is_file()


def test_two_names_that_sanitise_alike_collide_visibly_instead_of_overwriting(tmp_path):
    arena = _Arena(tmp_path)
    a = arena.inp / "a.xml"
    b = arena.inp / "b.xml"
    a.write_text(_powercenter_xml("SALES", "m/x", "wf_a"), encoding="utf-8")
    b.write_text(_powercenter_xml("SALES", "m\\x", "wf_b"), encoding="utf-8")
    result = run_migration([str(a), str(b)], str(arena.out), use_llm=False,
                           score_confidence=False, skip_lineage=True, skip_optimize=True)
    assert result.notebooks == 2
    notebooks = sorted(p.name for p in (arena.out / "SALES").glob("*.ipynb"))
    assert notebooks == ["nb_m_x.ipynb", "nb_m_x__2.ipynb"]
    assert len(result.notebook_collisions) == 1


# ── run_migration: IICS / IDMC JSON ───────────────────────────────────

def test_iics_export_with_hostile_project_and_name_stays_inside_output_dir(tmp_path):
    arena = _Arena(tmp_path)
    js = arena.inp / "evil.json"
    js.write_text(_iics_json(arena.hostile_folder, "../../CLAUDE"), encoding="utf-8")

    result = run_migration(
        [str(js)], str(arena.out), use_llm=False, emit_comparison=True,
        score_confidence=False, skip_lineage=False, skip_optimize=True,
    )

    assert result.notebooks == 1
    _assert_contained(arena, (".ipynb", "_comparison.md"))
    assert not (arena.escaped / "CLAUDE.md").exists()
    assert {k for k, _o, _s in result.renamed_paths} == {"folder", "mapping"}


# ── the other write sites ─────────────────────────────────────────────

def test_lineage_command_stays_inside_its_output_dir(tmp_path, capsys):
    arena = _Arena(tmp_path)
    xml = arena.inp / "evil.xml"
    xml.write_text(_powercenter_xml(arena.hostile_folder, "../../CLAUDE", HOSTILE_WORKFLOW),
                   encoding="utf-8")
    assert main(["lineage", "-i", str(xml), "-o", str(arena.out)]) == 0
    assert "Lineage generated for 1 mapping(s)" in capsys.readouterr().out
    _assert_contained(arena, (".md", ".json"))
    assert not (arena.root / "CLAUDE.md").exists()
    assert not (arena.root.parent / "CLAUDE.md").exists()


def test_batch_migrator_stays_inside_its_output_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "RULE_BASED_FALLBACK", True, raising=False)
    arena = _Arena(tmp_path)
    xml = arena.inp / "evil.xml"
    xml.write_text(_powercenter_xml(arena.hostile_folder, HOSTILE_MAPPING, HOSTILE_WORKFLOW),
                   encoding="utf-8")

    summary = BatchMigrator(llm_handler=None, max_workers=1).migrate_folder(
        str(arena.inp), str(arena.out)
    )

    assert summary.fallback == 1 and summary.failed == 0
    _assert_contained(arena, (".ipynb", "migration_mapping.csv"))
    # the raw name stays visible beside the path actually written
    csv_text = (arena.out / "reports" / "migration_mapping.csv").read_text(encoding="utf-8")
    assert HOSTILE_MAPPING in csv_text
    assert safe_path_component(HOSTILE_MAPPING) in csv_text


def test_a_notebook_that_cannot_be_written_fails_that_mapping_not_the_run(tmp_path, monkeypatch, caplog):
    """One refused write used to be an uncaught exception that took the
    rest of the run -- lineage, DDL, workflows -- down with it."""
    import infa2aidp.migrator as migrator_mod

    real = migrator_mod.ensure_within

    def refuse_bad(base_dir, path):
        if "nb_BAD" in os.path.basename(path):
            raise SecurityError("refused for the test")
        return real(base_dir, path)

    monkeypatch.setattr(migrator_mod, "ensure_within", refuse_bad)

    arena = _Arena(tmp_path)
    bad = arena.inp / "bad.xml"
    good = arena.inp / "good.xml"
    bad.write_text(_powercenter_xml("SALES", "BAD", "wf_bad"), encoding="utf-8")
    good.write_text(_powercenter_xml("SALES", "m_GOOD", "wf_good"), encoding="utf-8")

    with caplog.at_level(logging.WARNING, logger="infa2aidp.migrator"):
        result = run_migration([str(bad), str(good)], str(arena.out), use_llm=False,
                               score_confidence=False, skip_lineage=True, skip_optimize=True)

    assert result.notebooks == 1
    assert (arena.out / "SALES" / "nb_m_GOOD.ipynb").is_file()
    assert not (arena.out / "SALES" / "nb_BAD.ipynb").exists()
    assert "Could not write notebook for mapping 'BAD'" in caplog.text
    assert result.workflows == 2, "the good workflow and the bad one's job are still emitted"

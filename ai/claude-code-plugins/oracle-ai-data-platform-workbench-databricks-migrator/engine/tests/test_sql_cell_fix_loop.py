"""Failing write-safe %sql cells with a partition-repair statement reach the
call_fix loop, and the repair may change only that statement.

Incident 2026-10-06 (Daily_Example_Base, DBX job 1100000000000009, Phase 2
run cccc52ed): `%sql MSCK REPAIR TABLE ...` failed on AIDP with
NOT_SUPPORTED_COMMAND_FOR_V2_TABLE. The %sql branch re-ran the identical text
once and recorded FAIL; the model never saw the error. On AIDP the statement is
both unsupported and unnecessary (no partition registry; partitions written
under a table's location are visible on the next read -- live-probed
2026-10-13), so the correct migration is a tagged comment + REFRESH TABLE.

Top-level %sql executes WITHOUT the write sandbox (its SQL-form coverage is
incomplete), so the model may repair a cell only when every statement -- in the
cell AND in the repair -- is read-only or metadata-only. A shape check cannot
tell a real SQL fix from one that drops the cell's work (`assert_true(...)` ->
`count(*)`, a read -> `REFRESH TABLE`), so the repair contract is equality: only
MSCK REPAIR TABLE / ALTER TABLE ... RECOVER PARTITIONS may change. Every other
failing %sql cell keeps the previous behaviour (honest FAIL).

These tests drive the real process_notebook cell loop with the fake kernel
from test_conditional_exit_below_execute.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import agent_migrate as am  # noqa: E402
import job_migrate as jm  # noqa: E402
from test_conditional_exit_below_execute import (  # noqa: E402
    _Kernel, _patch_common, _saved_sources)
from test_eval_verdict_gate import _notebook  # noqa: E402

MSCK_CELL = "%sql\nMSCK REPAIR TABLE data_science.ds_serviceable_base;\n"
V2_ERROR = ("Command ID 1-2 failed with java.lang.RuntimeException: "
            "org.apache.spark.sql.AnalysisException: [NOT_SUPPORTED_COMMAND_FOR_V2_TABLE] "
            "MSCK REPAIR TABLE is not supported for v2 tables.")
MISSING_TABLE = ("Command ID 3-4 failed with org.apache.spark.sql.AnalysisException: "
                 "[TABLE_OR_VIEW_NOT_FOUND] The table or view `default`.`db`.`gone` "
                 "cannot be found.")
REPAIRED = (
    "%sql\n"
    "-- Oracle tool modification: MSCK REPAIR TABLE is not supported on AIDP (no partition\n"
    "-- registry); partitions are discovered from storage at read time. Original kept below.\n"
    "-- MSCK REPAIR TABLE default.data_science.ds_serviceable_base;\n"
    "REFRESH TABLE default.data_science.ds_serviceable_base;\n")
INSERT_CELL = ("%sql\nINSERT INTO default.prod_db.daily_scores "
               "SELECT * EXCEPT (dbx_only) FROM default.src_db.raw;\n")


def _active_sql(code):
    return [line for line in code.splitlines()
            if line.strip() and not line.lstrip().startswith(("--", "%sql"))]


class _SqlKernel(_Kernel):
    """%sql executions: a scripted AIDP error when an ACTIVE line contains a
    marker, otherwise success. The first `raises` %sql executions raise
    (a transient WebSocket drop). Everything else runs on the base fake."""

    def __init__(self, errors=None, raises=0):
        super().__init__()
        self.errors = errors or {}
        self.raises = raises
        self.sql = []

    async def execute(self, code, **kwargs):
        if not code.lstrip().startswith("%sql"):
            return await super().execute(code, **kwargs)
        self.executed.append(code)
        self.sql.append(code)
        if self.raises:
            self.raises -= 1
            raise ConnectionError("synthetic WebSocket drop")
        for marker, text in self.errors.items():
            if any(marker in line for line in _active_sql(code)):
                return {"status": "ok", "outputs": [
                    {"type": "stream", "name": "stdout", "text": text}]}
        return {"status": "ok", "outputs": []}


def _recording_fix(*replies, note=None):
    """call_fix stand-in: records each call and returns the scripted replies in
    order (the last repeats); with no replies it returns the code unchanged.
    `note` simulates the model calling make_note during the fix."""
    calls = []

    async def fix(code, error, context, attempt, extra_context="", **kwargs):
        calls.append({"code": code, "error": error, "attempt": attempt,
                      "extra_context": extra_context})
        if note:
            jm._current_cell_notes.append(note)
        if not replies:
            return code
        return replies[min(len(calls), len(replies)) - 1]

    return fix, calls


def _run(monkeypatch, tmp_path, *sources, kernel, fix, run_all=True, attempts=5):
    _patch_common(monkeypatch, tmp_path, _notebook(*sources))
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", attempts)
    monkeypatch.setattr(jm, "call_fix", fix)
    jm._current_cell_notes.clear()
    return asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_sql", "01_task", {},
        run_all=run_all))


def _report(kernel):
    texts = [t for p, t in kernel.files.items() if p.endswith("migration_report.md")]
    assert texts, "no migration report saved"
    return texts[0]


# ── Which cells the model may repair (category-1 gate) ────────────────────

@pytest.mark.parametrize("cell", [
    MSCK_CELL,
    "%sql\nALTER TABLE db.t RECOVER PARTITIONS;",
    "%sql\nSELECT * FROM db.t WHERE dt = '2026-10-04';",
    "%sql\nWITH a AS (SELECT 1 AS x) SELECT * FROM a;",
    "%sql\nSHOW TABLES IN db;",
    "%sql\nDESCRIBE TABLE EXTENDED db.t;",
    "%sql\nREFRESH TABLE db.t;",
    "%sql\nCACHE TABLE db.t;",
    "%sql\nUNCACHE TABLE db.t;",
    "%sql\nCLEAR CACHE;",
    "%sql\nUSE db;",
    "%sql\nSET spark.sql.shuffle.partitions = 8;",
    "%sql\nCREATE OR REPLACE TEMP VIEW v AS SELECT * FROM db.t;",
    "%sql\nSELECT 'x; INSERT INTO db.t SELECT 1' AS s;\n-- DELETE FROM db.t;",
    REPAIRED,
])
def test_read_only_and_metadata_cells_may_be_repaired(cell):
    assert am._sql_repair_block_reason(cell) is None


@pytest.mark.parametrize("cell", [
    INSERT_CELL,
    "%sql\nINSERT INTO TABLE db.t SELECT 1;",
    "%sql\nINSERT OVERWRITE TABLE db.t SELECT 1;",
    "%sql\nCREATE TABLE db.t AS SELECT 1;",
    "%sql\nCREATE OR REPLACE VIEW db.v AS SELECT 1;",
    "%sql\nREPLACE TABLE db.t AS SELECT 1;",
    "%sql\nMERGE INTO db.t USING db.s ON t.id = s.id WHEN MATCHED THEN DELETE;",
    "%sql\nUPDATE db.t SET a = 1;",
    "%sql\nDELETE FROM db.t WHERE dt = '2026-10-04';",
    "%sql\nDROP TABLE db.t;",
    "%sql\nTRUNCATE TABLE db.t;",
    "%sql\nALTER TABLE db.t ADD COLUMNS (c STRING);",
    "%sql\nOPTIMIZE db.t ZORDER BY (a);",
    "%sql\nVACUUM db.t;",
    "%sql\nCOPY INTO db.t FROM 'oci://b@ns/x' FILEFORMAT = PARQUET;",
    "%sql\nANALYZE TABLE db.t COMPUTE STATISTICS;",
    "%sql\nWITH a AS (SELECT 1) INSERT INTO db.t SELECT * FROM a;",
    "%sql\nSELECT 1;\nDELETE FROM db.t;",
    "%sql\nMSCK REPAIR TABLE db.t;\nINSERT INTO db.t SELECT 1;",
    "%sql\n-- only a comment",
    "%sql\nFROBNICATE db.t;",
    # Lexer/Spark disagreement shapes: Spark reads \' as an escaped quote and
    # nests block comments, so a write after them must not look masked.
    "%sql\nSELECT * FROM db.t WHERE name = 'O\\'Brien';\nINSERT INTO db.t SELECT 1;",
    "%sql\nSELECT 'x\\'' ; DROP TABLE db.t ; SELECT ''",
    '%sql\nSELECT "x\\"" ; DROP TABLE db.t ; SELECT ""',
    "%sql\n/* outer /* inner */ ' */ INSERT INTO db.t SELECT 1;",
    "%sql\nSELECT 'abc; DROP TABLE db.t",
    "%sql\nSELECT 1 /* never closed; DROP TABLE db.t",
])
def test_cells_that_write_or_cannot_be_classified_are_not_repaired(cell):
    assert am._sql_repair_block_reason(cell)


def test_default_lexer_is_unchanged_for_run_on_cluster():
    """strict mode is opt-in; the run_on_cluster read-only guard keeps today's lexer
    (which ends a literal at \\' — the disagreement strict mode refuses to classify)."""
    assert am._sql_without_comments_or_literals("SELECT 'a\\'b' AS x") == "SELECT     b      "
    assert am._sql_without_comments_or_literals("SELECT 'abc") == "SELECT     "
    assert am._sql_without_comments_or_literals("SELECT 'a\\'b'", strict=True) is None


def test_keep_literals_masks_comments_only_and_preserves_length():
    sql = "SELECT 'x--y' AS a -- note\n/* c */ FROM t WHERE s = 'it''s'"
    kept = am._sql_without_comments_or_literals(sql, keep_literals=True)
    assert len(kept) == len(sql)
    assert "'x--y'" in kept and "'it''s'" in kept
    assert "note" not in kept and "/*" not in kept


# ── Which failing cells go to the model at all ────────────────────────────

@pytest.mark.parametrize("cell", [
    MSCK_CELL,
    "%sql\nALTER TABLE db.t RECOVER PARTITIONS;",
    "%sql\nMSCK REPAIR TABLE db.t SYNC PARTITIONS;",
    "%sql\nUSE db;\nMSCK REPAIR TABLE db.t;\nSELECT count(*) FROM db.t;",
])
def test_partition_repair_cells_are_eligible(cell):
    assert jm._sql_repair_eligibility_reason(cell) is None


@pytest.mark.parametrize("cell, expected", [
    ("%sql\nSELECT dbx_only_fn(x) FROM db.daily;", "no partition-repair statement"),
    ("%sql\nSELECT assert_true(count(*) > 0) FROM db.daily;", "no partition-repair statement"),
    ("%sql\nCREATE OR REPLACE TEMP VIEW v AS SELECT * FROM db.t;", "no partition-repair statement"),
    ("%sql\n-- MSCK REPAIR TABLE db.t;\nSELECT 1;", "no partition-repair statement"),
    ("%sql\nSELECT 'MSCK REPAIR TABLE db.t' AS s;", "no partition-repair statement"),
    # A backticked name is masked away, so the statement is unclassified MSCK.
    ("%sql\nMSCK REPAIR TABLE `db`.`t`;", "MSCK"),
    ("%sql\nMSCK REPAIR TABLE db.t;\nINSERT INTO db.t SELECT 1;", "INSERT"),
])
def test_other_sql_cells_are_not_eligible(cell, expected):
    assert expected in jm._sql_repair_eligibility_reason(cell)


# ── The repair contract: only the partition-repair statement may change ───

MIXED_CELL = ("%sql\nSELECT assert_true(count(*) > 0) FROM db.daily WHERE dt = '2026-10-04';\n"
              "MSCK REPAIR TABLE db.daily;\n")
MIXED_REPAIRED = ("%sql\nSELECT assert_true(count(*) > 0) FROM db.daily WHERE dt = '2026-10-04';\n"
                  "-- Oracle tool modification: MSCK REPAIR TABLE is not supported on AIDP.\n"
                  "-- MSCK REPAIR TABLE db.daily;\n"
                  "REFRESH TABLE db.daily;\n")
_READ = "SELECT assert_true(count(*) > 0) FROM db.daily WHERE dt = '2026-10-04'"


@pytest.mark.parametrize("repair", [
    MIXED_REPAIRED,
    MIXED_REPAIRED.replace("REFRESH TABLE db.daily;\n", ""),          # REFRESH is optional
    MIXED_REPAIRED.replace("-- MSCK REPAIR", "/* MSCK REPAIR").replace(
        "db.daily;\nREFRESH", "db.daily; */\nREFRESH"),                   # block comment
    MIXED_REPAIRED.replace(_READ, "SELECT  assert_true(count(*) > 0)\n  FROM db.daily "
                                  "WHERE dt = '2026-10-04'"),          # whitespace only
    MIXED_CELL,                                                         # unchanged: may re-run
])
def test_contract_accepts_a_repair_that_changes_only_the_partition_repair(repair):
    assert jm._sql_repair_contract_violation(MIXED_CELL, repair) is None


@pytest.mark.parametrize("repair", [
    # The review's false-PASS shapes, each next to a correct MSCK replacement.
    MIXED_REPAIRED.replace("assert_true(count(*) > 0)", "count(*)"),
    MIXED_REPAIRED.replace(_READ, "WITH ok AS (SELECT 1 AS x) SELECT * FROM ok"),
    MIXED_REPAIRED.replace(_READ, "SET spark.sql.shuffle.partitions = 8"),
    MIXED_REPAIRED.replace(_READ + ";\n", ""),                          # read -> only REFRESH
    MIXED_REPAIRED.replace(_READ, "SELECT 1"),
    MIXED_REPAIRED.replace(_READ, "VALUES (1)"),
    MIXED_REPAIRED.replace(_READ, "-- " + _READ),                       # read commented out
    MIXED_REPAIRED.replace(_READ, "/* " + _READ + " */ SELECT 1"),
    MIXED_REPAIRED.replace(_READ, "SELECT '" + _READ.replace("'", "''") + "' AS skipped"),
    MIXED_REPAIRED.replace("'2026-10-04'", "'1900-01-01'"),             # literal edited
    MIXED_REPAIRED.replace("REFRESH TABLE db.daily", "REFRESH TABLE db.other"),
    MIXED_REPAIRED + "SHOW TABLES;\n",                                  # extra statement
    MIXED_REPAIRED.replace("-- MSCK REPAIR TABLE db.daily;\n", ""),     # original not kept
    MIXED_REPAIRED.replace("-- Oracle tool modification: MSCK REPAIR TABLE is not "
                           "supported on AIDP.\n", ""),                 # untagged
    MIXED_REPAIRED.replace("REFRESH TABLE db.daily;", "SELECT 'a\\'b';"),  # unclassifiable
])
def test_contract_rejects_any_other_change(repair):
    assert jm._sql_repair_contract_violation(MIXED_CELL, repair)


def test_an_inherited_tag_line_does_not_count_as_tagging_the_change():
    """The table-ref normalizer's tag is already in the source; the repair must add its own."""
    note = ("-- Oracle tool modification: table refs tagged to default catalog/schema "
            "(AIDP requires 3-part: catalog.schema.table)\n")
    source = "%sql\n" + note + "MSCK REPAIR TABLE default.db.t;\n"
    untagged = "%sql\n" + note + "-- MSCK REPAIR TABLE default.db.t;\nREFRESH TABLE default.db.t;\n"
    tagged = untagged.replace("-- MSCK", "-- Oracle tool modification: unsupported on AIDP.\n-- MSCK")
    assert "did not tag" in jm._sql_repair_contract_violation(source, untagged)
    assert jm._sql_repair_contract_violation(source, tagged) is None
    # Dropping the inherited note is fine once the change carries its own tag.
    assert jm._sql_repair_contract_violation(source, tagged.replace(note, "")) is None


_TAG = "-- Oracle tool modification: MSCK REPAIR TABLE is not supported on AIDP.\n"


def _with_msck(read, repaired=False):
    """A cell with `read` and an MSCK statement, or its correct repair."""
    tail = (f"{_TAG}-- MSCK REPAIR TABLE db.t;\nREFRESH TABLE db.t;\n" if repaired
            else "MSCK REPAIR TABLE db.t;\n")
    return f"%sql\n{read};\n{tail}"


@pytest.mark.parametrize("read, edited", [
    # 'a  b' and 'a b' are different values: the first edit turns a failing check green.
    ("SELECT assert_true('a  b' = 'a b')", "SELECT assert_true('a b' = 'a b')"),
    ("SELECT assert_true('a b' = 'a b')", "SELECT assert_true('a  b' = 'a b')"),
    ("SELECT assert_true('a  b' = 'a b')", "SELECT assert_true('a\tb' = 'a b')"),
    ("SELECT assert_true('a  b' = 'a b')", "SELECT assert_true('a\nb' = 'a b')"),
    ('SELECT "a  b" AS s', 'SELECT "a b" AS s'),
    ("SELECT `a  b` FROM db.t", "SELECT `a b` FROM db.t"),
])
def test_contract_rejects_a_whitespace_edit_inside_a_literal(read, edited):
    """Whitespace is collapsed outside literals only: a literal must stay byte-for-byte."""
    assert jm._sql_repair_contract_violation(_with_msck(read), _with_msck(edited, repaired=True))


def test_contract_still_ignores_whitespace_outside_literals():
    read = "SELECT assert_true('a  b' = 'a b')"
    reflowed = "SELECT\n   assert_true('a  b'  =\t'a b')"
    assert jm._sql_repair_contract_violation(_with_msck(read), _with_msck(reflowed, repaired=True)) is None


REFRESH_FIRST_CELL = "%sql\nREFRESH TABLE db.t;\nMSCK REPAIR TABLE db.t;\n"
REFRESH_FIRST_REPAIRED = (f"%sql\nREFRESH TABLE db.t;\n{_TAG}-- MSCK REPAIR TABLE db.t;\n"
                          "REFRESH TABLE db.t;\n")


@pytest.mark.parametrize("repair", [
    REFRESH_FIRST_REPAIRED,
    REFRESH_FIRST_REPAIRED[:-len("REFRESH TABLE db.t;\n")],             # no REFRESH added
])
def test_a_refresh_already_in_the_cell_does_not_block_a_valid_repair(repair):
    assert jm._sql_repair_contract_violation(REFRESH_FIRST_CELL, repair) is None


@pytest.mark.parametrize("repair", [
    f"%sql\n{_TAG}-- MSCK REPAIR TABLE db.t;\n",                         # original REFRESH dropped
    REFRESH_FIRST_REPAIRED.replace("%sql\nREFRESH TABLE db.t;", "%sql\nSELECT 1;"),
    REFRESH_FIRST_REPAIRED.replace("%sql\nREFRESH TABLE db.t;", "%sql\nREFRESH TABLE db.other;"),
])
def test_a_refresh_already_in_the_cell_is_still_kept_like_any_statement(repair):
    assert jm._sql_repair_contract_violation(REFRESH_FIRST_CELL, repair)


# ── The loop ──────────────────────────────────────────────────────────────

def test_v2_unsupported_msck_is_sent_to_call_fix_and_the_repair_runs(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(REPAIRED)
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix)

    assert len(calls) == 1
    assert "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE" in calls[0]["error"]
    assert calls[0]["code"].lstrip().startswith("%sql")
    assert calls[0]["extra_context"] == jm._SQL_PARTITION_REPAIR_SCOPE   # told the contract
    assert len(kernel.sql) == 2                 # as-is, then the repair
    assert "REFRESH TABLE" in kernel.sql[1]
    assert (result["ok"], result["failed"], result["fixed"]) == (1, 0, 1)
    assert result["cell_results"][0]["note"] == "%sql cell executed (Opus-fixed)"
    saved, _ = _saved_sources(kernel)
    assert "-- MSCK REPAIR TABLE default.data_science.ds_serviceable_base;" in saved
    assert "REFRESH TABLE default.data_science.ds_serviceable_base;" in saved


def test_a_repair_without_the_magic_gets_percent_sql_back(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, _ = _recording_fix(REPAIRED.split("\n", 1)[1])   # model dropped "%sql"
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix)

    assert len(kernel.sql) == 2                 # the repair ran as a %sql cell
    assert result["ok"] == 1


def test_a_passing_sql_cell_runs_once_without_a_model_call(monkeypatch, tmp_path):
    kernel = _SqlKernel()
    fix, calls = _recording_fix()
    result = _run(monkeypatch, tmp_path, "%sql\nSELECT 1 AS ok;\n", kernel=kernel, fix=fix)

    assert calls == [] and len(kernel.sql) == 1
    assert (result["ok"], result["fixed"]) == (1, 0)
    assert result["cell_results"][0]["note"] == "%sql cell executed"


def test_missing_table_on_the_unmodified_cell_is_data_unavailable_without_a_model_call(
        monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"gone": MISSING_TABLE})
    fix, calls = _recording_fix()
    result = _run(monkeypatch, tmp_path, "%sql\nMSCK REPAIR TABLE db.gone;\n",
                  kernel=kernel, fix=fix)

    assert calls == [] and len(kernel.sql) == 1
    assert (result["data_unavailable"], result["failed"]) == (1, 0)
    assert result["cell_results"][0]["note"] == "DATA_UNAVAILABLE: missing table (%sql)"


def test_missing_table_after_a_repair_is_data_unavailable(monkeypatch, tmp_path):
    """The contract admits no new table name: a table missing after the repair is
    one the original cell reads, so it is missing data, not a bad repair."""
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR, "REFRESH": MISSING_TABLE})
    fix, calls = _recording_fix(REPAIRED)
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix, attempts=3)

    assert len(calls) == 1 and len(kernel.sql) == 2
    assert (result["data_unavailable"], result["failed"]) == (1, 0)
    assert result["cell_results"][0]["note"] == "DATA_UNAVAILABLE: missing table (%sql)"


def test_a_model_note_confirming_missing_data_stops_the_retries(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(note="DATA_UNAVAIL: source partition for 2026-10-04 is absent")
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix)

    assert len(calls) == 1 and len(kernel.sql) == 2
    assert (result["data_unavailable"], result["failed"]) == (1, 0)
    assert result["cell_results"][0]["note"] == (
        "DATA_UNAVAILABLE: model note confirms missing data (%sql)")


def test_session_exception_is_retried_as_is_without_a_model_call(monkeypatch, tmp_path):
    kernel = _SqlKernel(raises=1)
    fix, calls = _recording_fix()
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix)

    assert calls == []
    assert len(kernel.sql) == 2 and kernel.sql[0] == kernel.sql[1]
    assert (result["ok"], result["fixed"]) == (1, 0)


def test_transient_exhaustion_reports_the_last_exception(monkeypatch, tmp_path):
    kernel = _SqlKernel(raises=3)
    fix, calls = _recording_fix()
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel,
                  fix=fix, attempts=3)

    assert calls == [] and result["failed"] == 1
    assert result["cell_results"][0]["note"] == (
        "%sql unresolved (transient): synthetic WebSocket drop")


def test_unresolved_error_fails_after_max_fix_attempts_executions(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix()              # model returns the code unchanged
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix, attempts=3)

    assert len(kernel.sql) == 3 and len(calls) == 2
    assert result["failed"] == 1 and result["status"] != "PASS"
    assert result["cell_results"][0]["note"] == "%sql execution error after Opus fixes"


def test_a_whitespace_only_reply_is_not_counted_as_a_repair(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, _ = _recording_fix(MSCK_CELL.rstrip("\n") + "   ")
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix, attempts=2)

    assert (result["fixed"], result["failed"]) == (0, 1)


def test_model_notes_reach_the_report_and_do_not_leak_into_the_next_cell(
        monkeypatch, tmp_path):
    note = "MSCK REPAIR TABLE unsupported on AIDP; replaced with REFRESH TABLE"
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, _ = _recording_fix(REPAIRED, note=note)
    result = _run(monkeypatch, tmp_path, MSCK_CELL, "SIBLING_OK = 1\n",
                  kernel=kernel, fix=fix)

    assert result["ok"] == 2
    assert note in _report(kernel)
    saved, _ = _saved_sources(kernel)
    assert f"# NOTE: {note}" not in saved       # not pinned on the Python sibling


# ── Write cells and code-only runs keep the previous behaviour ────────────

def test_write_cells_are_not_sent_to_the_model_and_keep_the_previous_behaviour(
        monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"EXCEPT": V2_ERROR})
    fix, calls = _recording_fix("%sql\nSELECT 1;\n")
    result = _run(monkeypatch, tmp_path, INSERT_CELL, kernel=kernel, fix=fix)

    assert calls == []
    assert len(kernel.sql) == 2 and kernel.sql[0] == kernel.sql[1]   # as-is + retry
    assert "INSERT INTO default.prod_db.daily_scores" in kernel.sql[0]
    assert result["failed"] == 1
    note = result["cell_results"][0]["note"]
    assert note.startswith("%sql execution error (not sent to the model: ")
    assert "INSERT" in note


@pytest.mark.parametrize("cell, marker", [
    ("%sql\nSELECT assert_true(count(*) > 0) FROM db.daily;\n", "assert_true"),
    ("%sql\nSELECT dbx_only_fn(x) FROM db.daily;\n", "dbx_only_fn"),
])
def test_failing_reads_are_not_sent_to_the_model_and_fail_honestly(
        monkeypatch, tmp_path, cell, marker):
    """The review's false-PASS originals: before the contract the model could
    replace them with a read that does no work and PASS."""
    kernel = _SqlKernel(errors={marker: V2_ERROR})
    fix, calls = _recording_fix("%sql\nSELECT count(*) FROM db.daily;\n")
    result = _run(monkeypatch, tmp_path, cell, kernel=kernel, fix=fix)

    assert calls == []
    assert len(kernel.sql) == 2 and kernel.sql[0] == kernel.sql[1]   # as-is + retry
    assert (result["ok"], result["fixed"], result["failed"]) == (0, 0, 1)
    assert result["cell_results"][0]["note"].startswith(
        "%sql execution error (not sent to the model: no partition-repair statement")
    saved, _ = _saved_sources(kernel)
    assert marker in saved


def test_a_repair_that_adds_a_write_is_never_executed(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(
        "%sql\n-- MSCK REPAIR TABLE db.t;\nINSERT INTO default.prod_db.t SELECT 1;\n")
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix)

    assert len(calls) == 1
    assert len(kernel.sql) == 1 and "INSERT" not in kernel.sql[0]
    assert result["failed"] == 1
    assert result["cell_results"][0]["note"].startswith("%sql model repair not executed: ")
    saved, _ = _saved_sources(kernel)
    assert "INSERT INTO default.prod_db.t" not in saved


def test_code_only_mode_keeps_the_previous_behaviour_and_never_calls_the_model(
        monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(REPAIRED)
    result = _run(monkeypatch, tmp_path, MSCK_CELL, kernel=kernel, fix=fix, run_all=False)

    assert calls == []
    assert len(kernel.sql) == 2                 # previous as-is run + retry
    assert result["cell_results"][0]["note"] == "%sql execution error"


# ── Anti-masking: the repair contract in the real loop ────────────────────

TEMP_VIEW_CELL = ("%sql\nCREATE OR REPLACE TEMP VIEW scores AS "
                  "SELECT * EXCEPT (dbx_only) FROM default.db.s;\n")
# 3-part names, so the table-ref normalizer leaves the cell as written.
MIXED_3P = MIXED_CELL.replace("db.daily", "default.db.daily")
MIXED_REPAIRED_3P = MIXED_REPAIRED.replace("db.daily", "default.db.daily")
_READ_3P = _READ.replace("db.daily", "default.db.daily")


def test_a_mixed_cell_repair_that_keeps_the_read_passes(monkeypatch, tmp_path):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(MIXED_REPAIRED_3P)
    result = _run(monkeypatch, tmp_path, MIXED_3P, kernel=kernel, fix=fix)

    assert len(calls) == 1 and len(kernel.sql) == 2
    assert _READ_3P in kernel.sql[1]             # the data check still runs
    assert (result["ok"], result["fixed"], result["failed"]) == (1, 1, 0)


@pytest.mark.parametrize("masking", [
    MIXED_REPAIRED_3P.replace("assert_true(count(*) > 0)", "count(*)"),
    MIXED_REPAIRED_3P.replace(_READ_3P, "WITH ok AS (SELECT 1 AS x) SELECT * FROM ok"),
    MIXED_REPAIRED_3P.replace(_READ_3P, "SET spark.sql.shuffle.partitions = 8"),
    MIXED_REPAIRED_3P.replace(_READ_3P + ";\n", ""),       # read -> only REFRESH TABLE
    MIXED_REPAIRED_3P.replace(_READ_3P, "-- " + _READ_3P),
])
def test_a_masking_repair_is_never_executed_or_saved(monkeypatch, tmp_path, masking):
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(masking)
    result = _run(monkeypatch, tmp_path, MIXED_3P, kernel=kernel, fix=fix, attempts=3)

    assert len(calls) == 2
    assert len(kernel.sql) == 3 and len(set(kernel.sql)) == 1   # only the original ran
    assert (result["ok"], result["fixed"], result["failed"]) == (0, 0, 1)
    assert result["cell_results"][0]["note"] == "%sql execution error after Opus fixes"
    assert "YOUR PREVIOUS REPAIR WAS REJECTED AND NOT EXECUTED" in calls[1]["extra_context"]
    saved, _ = _saved_sources(kernel)
    assert "\n" + _READ_3P + ";" in saved
    assert "\nMSCK REPAIR TABLE default.db.daily;" in saved


def test_a_whitespace_edit_inside_a_literal_is_never_executed(monkeypatch, tmp_path):
    source = _with_msck("SELECT assert_true('a  b' = 'a b')").replace("db.t", "default.db.t")
    flipped = _with_msck("SELECT assert_true('a b' = 'a b')", repaired=True).replace(
        "db.t", "default.db.t")
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(flipped)
    result = _run(monkeypatch, tmp_path, source, kernel=kernel, fix=fix, attempts=3)

    assert len(calls) == 2
    assert len(kernel.sql) == 3 and len(set(kernel.sql)) == 1   # only the original ran
    assert (result["ok"], result["fixed"], result["failed"]) == (0, 0, 1)
    assert "YOUR PREVIOUS REPAIR WAS REJECTED AND NOT EXECUTED" in calls[1]["extra_context"]


def test_a_cell_that_already_refreshes_the_table_is_repaired_first_time(monkeypatch, tmp_path):
    source = REFRESH_FIRST_CELL.replace("db.t", "default.db.t")
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(REFRESH_FIRST_REPAIRED.replace("db.t", "default.db.t"))
    result = _run(monkeypatch, tmp_path, source, kernel=kernel, fix=fix)

    assert len(calls) == 1                       # accepted: no rejection, no retry loop
    assert len(kernel.sql) == 2
    assert kernel.sql[1].count("\nREFRESH TABLE default.db.t;") == 2
    assert (result["ok"], result["fixed"], result["failed"]) == (1, 1, 0)


def test_a_rejected_repair_is_fed_back_and_a_corrected_repair_passes(monkeypatch, tmp_path):
    masking = MIXED_REPAIRED_3P.replace("assert_true(count(*) > 0)", "count(*)")
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix(masking, MIXED_REPAIRED_3P)
    result = _run(monkeypatch, tmp_path, MIXED_3P, kernel=kernel, fix=fix)

    assert len(calls) == 2
    assert calls[0]["extra_context"] == jm._SQL_PARTITION_REPAIR_SCOPE
    feedback = calls[1]["extra_context"]
    assert feedback.startswith(jm._SQL_PARTITION_REPAIR_SCOPE)
    assert "YOUR PREVIOUS REPAIR WAS REJECTED AND NOT EXECUTED: it changed, removed or added" in feedback
    assert masking in feedback                   # the model sees what it sent
    assert calls[1]["code"] == calls[0]["code"]  # the rejected repair was reverted
    assert not any("SELECT count(*) FROM" in executed for executed in kernel.sql)
    assert len(kernel.sql) == 3                  # original, original again, corrected repair
    assert _READ_3P in kernel.sql[2]
    assert (result["ok"], result["fixed"], result["failed"]) == (1, 1, 0)


def test_feedback_is_cleared_once_a_repair_is_accepted(monkeypatch, tmp_path):
    masking = MIXED_REPAIRED_3P.replace(_READ_3P, "SELECT 1")
    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    # rejected → accepted (the unchanged cell, which still fails) → third call
    fix, calls = _recording_fix(masking, MIXED_3P, MIXED_REPAIRED_3P)
    result = _run(monkeypatch, tmp_path, MIXED_3P, kernel=kernel, fix=fix)

    assert len(calls) == 3
    assert "REJECTED" in calls[1]["extra_context"]
    assert calls[2]["extra_context"] == jm._SQL_PARTITION_REPAIR_SCOPE
    assert (result["ok"], result["fixed"]) == (1, 1)


def test_a_fix_step_error_saves_the_vetted_code_not_the_model_reply(monkeypatch, tmp_path):
    real_namespace = jm._apply_namespace_from_mapping

    def namespace_fatal(code):
        if "unvetted_reply" in code:            # only the model's reply trips it
            raise RuntimeError("FATAL: invented OCI namespace — refusing to proceed")
        return real_namespace(code)

    kernel = _SqlKernel(errors={"MSCK": V2_ERROR})
    fix, calls = _recording_fix("%sql\nSELECT * FROM db.unvetted_reply;\n")
    _patch_common(monkeypatch, tmp_path, _notebook(MSCK_CELL))
    monkeypatch.setattr(jm, "_apply_namespace_from_mapping", namespace_fatal)
    monkeypatch.setattr(jm, "MAX_FIX_ATTEMPTS", 5)
    monkeypatch.setattr(jm, "call_fix", fix)
    jm._current_cell_notes.clear()
    result = asyncio.run(jm.process_notebook(
        "/Workspace/test_cond/source.ipynb", kernel, "test_sql", "01_task", {},
        run_all=True))

    assert len(calls) == 1 and result["failed"] == 1
    assert result["cell_results"][0]["note"] == "%sql fix error"
    saved, _ = _saved_sources(kernel)
    assert "unvetted_reply" not in saved
    assert "MSCK REPAIR TABLE default.data_science.ds_serviceable_base;" in saved


def test_default_guard_is_unchanged_for_python_child_and_replay_callers():
    orig = 'df.write.saveAsTable("db.t")\n'
    assert jm._reject_neutered_fix(orig, orig, 'print("skipped")\n') == orig
    kept = 'df.write.saveAsTable("db.t2")\n'
    assert jm._reject_neutered_fix(orig, orig, kept) == kept
    real_work = "x = compute()\n"
    assert jm._reject_neutered_fix(orig, orig, real_work) == real_work
    # The guard reads `--` lines as active (the behaviour the %run-child and
    # replay callers have always had; the %sql loop uses the repair contract).
    masked = "%sql\n-- CREATE TEMP VIEW scores AS SELECT 1;\nSELECT 1;\n"
    assert jm._reject_neutered_fix(TEMP_VIEW_CELL, TEMP_VIEW_CELL, masked) == masked

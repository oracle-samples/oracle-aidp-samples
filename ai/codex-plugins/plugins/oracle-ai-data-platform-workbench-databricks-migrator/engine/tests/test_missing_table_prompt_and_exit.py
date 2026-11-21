"""Missing-table handling: the fix-loop early-exit predicate + prompt consistency.

Covers the two review findings that SURVIVED removing the deterministic
table-substitution guard (prompt-only fix, PR #119; reviewer feedback):

  Major 3 — a genuinely-missing table must STOP the fix loop on the DATA_UNAVAIL
            note the prompt prescribes, instead of burning every Opus retry.
            (get_daily_data, 2026-09-18: cell 7 churned all attempts because the
            "DATA_UNAVAIL:" note text was not recognized by the early-exit.)

  Major 4 — the prompt is the SOLE defense now, so it must not carry contradictory
            "find the data / find correct path" guidance on any surface.
"""
import os as _os

from scripts.job_migrate import _note_confirms_missing_data

_SCRIPTS = _os.path.join(_os.path.dirname(__file__), "..", "scripts")


def _read(name):
    with open(_os.path.join(_SCRIPTS, name), encoding="utf-8") as f:
        return f.read()


# ── Major 3: early-exit predicate recognizes the prescribed note ─────────────

def test_data_unavail_note_stops_retries():
    # the exact prefix the TABLE_OR_VIEW_NOT_FOUND prompt prescribes
    assert _note_confirms_missing_data(
        ["DATA_UNAVAIL: data_science.ds_serviceable_base not found in AIDP catalog"])


def test_data_unavail_case_insensitive():
    assert _note_confirms_missing_data(["data_unavail: foo not registered"])


def test_legacy_forms_still_recognized():
    assert _note_confirms_missing_data(["TABLE_BLOCKED: db.t is MISSING"])
    assert _note_confirms_missing_data(["the table db.t does not exist on AIDP"])
    assert _note_confirms_missing_data(["cell_plan: MISSING table db.t"])


def test_unrelated_note_does_not_stop_retries():
    # a normal migration note must NOT trigger the missing-data early-exit
    assert not _note_confirms_missing_data(
        ["Cell 2: boto3 S3 client replaced with OCI Object Storage SDK"])
    assert not _note_confirms_missing_data(["STUBBED: Databricks job trigger"])


def test_empty_and_none_are_false():
    assert not _note_confirms_missing_data([])
    assert not _note_confirms_missing_data(None)


def test_any_note_in_list_triggers():
    assert _note_confirms_missing_data(
        ["migrated fs.ls path", "DATA_UNAVAIL: db.t not found"])


# ── Major 4: prompt sweep — no contradictory "find the data elsewhere" ───────

def test_evaluator_no_longer_says_find_correct_path():
    src = _read("job_migrate.py")
    assert "(use tools to find correct path)" not in src, (
        "evaluator must not tell the model to find a correct path for a missing table")


def test_fix_prompt_no_bare_find_the_data():
    src = _read("agent_migrate.py")
    assert "find the data in the AIDP catalog" not in src, (
        "FIX_PROMPT boto3 guidance must be scoped to the SAME dataset, not 'find the data'")


def test_fix_prompt_has_no_swap_caveat():
    src = _read("agent_migrate.py")
    assert "NOT a license to swap tables" in src


def test_cell_migrate_prompt_forbids_backup_swap_and_uses_data_unavail():
    src = _read("job_migrate.py")
    # the note keyword must match the early-exit predicate above
    assert 'DATA_UNAVAIL: <table> not found in AIDP catalog' in src
    # the exact incident vector must be prohibited
    assert '"migrated-equivalent"' in src
    assert "silently ships WRONG data" in src


def test_fix_prompt_stop_block_still_present():
    # the proven fix (prompt-only) must remain intact
    src = _read("agent_migrate.py")
    assert "STOP/DATA_UNAVAIL" in src
    assert "point the read at a DIFFERENT table" in src


def test_evaluator_explore_scoped_to_storage_paths_not_tables():
    # Review P1 (line ~14024): the evaluator must NOT tell the model to explore /
    # suggest_oci_path for a missing TABLE (its FIX feeds call_fix). Storage-path
    # exploration is fine; a missing table is DATA_UNAVAIL.
    src = _read("job_migrate.py")
    assert "check if the data exists elsewhere (different namespace)" not in src, (
        "evaluator must not send a missing table looking in a different namespace")
    assert "A missing TABLE (TABLE_OR_VIEW_NOT_FOUND) is DATA_UNAVAIL" in src
    assert "missing STORAGE PATH" in src

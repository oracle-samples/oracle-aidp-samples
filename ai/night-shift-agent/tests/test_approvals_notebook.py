"""The reviewer's notebook (scripts/stage_approvals_notebook.py): the committed .ipynb is what the generator emits, and
the Decide cell writes Spark-escaped literals (a doubled quote is not an escape in Spark SQL: 'it''s' reads back as
"its", so a name or note with a quote would be stored wrong, or could close the literal early)."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from nightshift import ledger as L

ROOT = Path(__file__).resolve().parents[1]


def _gen():
    spec = importlib.util.spec_from_file_location("ns_stage_approvals", ROOT / "scripts" / "stage_approvals_notebook.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_committed_notebook_matches_the_generator():
    nb = json.loads((ROOT / "notebooks" / "NightShift_Approvals.ipynb").read_text(encoding="utf-8"))
    assert nb["cells"] == _gen().cells("default.nightshift_ops", "")


class _Spark:
    def __init__(self, who):
        self.who, self.sql_seen = who, []

    def sql(self, q):
        self.sql_seen.append(q)
        outer = self

        class _R:
            def collect(self):
                return [(outer.who,)] if q.startswith("SELECT current_user()") else [("pending",)]

            def show(self, *a, **k):
                return None

        return _R()


@pytest.mark.parametrize("who,note", [("o'brien@example.com", "it's fine"), ("x', status = 'approved' --", "a \\ b"), ("plain", "")])
def test_decide_cell_escapes_the_approver_and_the_note_the_way_the_ledger_does(who, note):
    cells = _gen().cells("default.nightshift_ops", "0123456789abcdef")
    decide = next(c["source"] for c in cells if c["cell_type"] == "code" and "DECISION = " in c["source"])
    decide = decide.replace('NOTE = ""', f"NOTE = {note!r}")
    spark = _Spark(who)
    exec(compile(decide, "decide_cell", "exec"), {"spark": spark})
    update = next(q for q in spark.sql_seen if q.startswith("UPDATE default.nightshift_ops.approvals"))
    assert f"approved_by = {L.lit(who)}," in update                      # exactly the ledger's own literal
    assert "''" not in update.replace("' | ', '", "")                      # no doubled-quote "escape" anywhere
    if note:
        assert L.lit(f"approved by {who}: workbench; {note}") in update
    assert update.rstrip().endswith("WHERE approval_id = '0123456789abcdef' AND status = 'pending'")

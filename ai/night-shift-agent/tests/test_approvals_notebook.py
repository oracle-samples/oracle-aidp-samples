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
    def __init__(self, who, kind="statement_repair"):
        self.who, self.kind, self.sql_seen = who, kind, []

    def sql(self, q):
        self.sql_seen.append(q)
        outer = self

        class _R:
            def collect(self):
                return [(outer.who,)] if q.startswith("SELECT current_user()") else [("pending", outer.kind, "c1")]

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


def _decide(cell_edits=(), kind="statement_repair"):
    cells = _gen().cells("default.nightshift_ops", "0123456789abcdef")
    decide = next(c["source"] for c in cells if c["cell_type"] == "code" and "DECISION = " in c["source"])
    for old, new in cell_edits:
        assert old in decide
        decide = decide.replace(old, new)
    spark = _Spark("priya", kind)
    return decide, spark


@pytest.mark.parametrize("kind", ["cluster_update", "cluster_stop", "job_disable", "job_update_schedule"])
def test_decide_cell_refuses_to_approve_a_control_plane_row_unless_told_to(kind, capsys):
    """The actuator opens its approvals in the same table. Approving one lets its next --apply run change a cluster or
    a job, so the cell says what the row is and writes nothing until the reviewer sets ALLOW_CONTROL_PLANE."""
    decide, spark = _decide(kind=kind)
    with pytest.raises(ValueError, match=f"is a {kind} on c1, not a statement_repair"):
        exec(compile(decide, "decide_cell", "exec"), {"spark": spark})
    assert not any(q.startswith("UPDATE") for q in spark.sql_seen)
    decide, spark = _decide([("ALLOW_CONTROL_PLANE = False", "ALLOW_CONTROL_PLANE = True")], kind=kind)
    exec(compile(decide, "decide_cell", "exec"), {"spark": spark})
    assert any(q.startswith("UPDATE default.nightshift_ops.approvals") and "status = 'approved'" in q for q in spark.sql_seen)
    assert f"executes the {kind} on c1" in capsys.readouterr().out
    # rejecting one needs no flag
    decide, spark = _decide([('DECISION = "approve"', 'DECISION = "reject"')], kind=kind)
    exec(compile(decide, "decide_cell", "exec"), {"spark": spark})
    assert any(q.startswith("UPDATE") and "status = 'rejected'" in q for q in spark.sql_seen)


def test_the_notebook_says_which_kinds_it_lists_and_what_approving_each_does():
    cells = _gen().cells("default.nightshift_ops", "")
    text = "\n".join(c["source"] for c in cells)
    assert "Every row here is a `statement_repair`" not in text
    intro = cells[0]["source"]
    for kind in ("statement_repair", "cluster_update", "cluster_stop", "job_disable", "job_update_schedule"):
        assert f"`{kind}`" in intro
    assert "ALLOW_CONTROL_PLANE" in intro and "NS_APPROVERS" in intro
    pending = next(c["source"] for c in cells if c["cell_type"] == "code" and "status = 'pending'" in c["source"])
    assert "'$.kind') AS kind" in pending                      # the Pending table shows the kind of every row
    decide, spark = _decide()                                  # a statement_repair is decided as before
    exec(compile(decide, "decide_cell", "exec"), {"spark": spark})
    assert any(q.startswith("UPDATE") and "status = 'approved'" in q for q in spark.sql_seen)

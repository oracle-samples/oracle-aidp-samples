"""Reconcile after a structure run that stopped part-way.

Live 2026-09-29, the 50k-table scale estate: the structure job was stopped
after an hour with 1,800 tables recorded. Reconcile then reported

  * 16 tables that the run created after its last report write as
    STRUCTURE_ONLY, `structure: not_attempted`, no reason -- a table whose
    layout nobody checked, read as a verified structure;
  * all 500 planned views as VIEW_NOT_CREATED_BY_THIS_PATH, whose advice is
    to use --mode ddl-plan or `deploy` -- for a ddl-plan run that simply had
    not reached its view phase yet.

A table in the target that no structure report records now says so and how
to check it; a planned view no run has recorded is VIEW_NOT_CREATED_YET.
"""
import json

import pytest

from test_data_migration_scripts import _load
from test_reconcile_batched_counts import _TargetSpark


@pytest.fixture(scope="module")
def reconcile():
    return _load("03_reconcile")


def _estate(tmp_path):
    manifest = {"schemas": [{"name": "BULK",
                             "tables": [{"name": f"T00{i}"} for i in range(5)],
                             "views": [{"name": "V1"}, {"name": "V2"}]}]}
    (tmp_path / "structure_report_bulk.json").write_text(json.dumps(
        {"schema": "BULK", "target": "lake.BULK", "mode": "ddl-plan",
         "objects": {"T000": {"status": "created"}, "T001": {"status": "created"}}}),
        encoding="utf-8")
    live = {f"`lake`.`BULK`.`T00{i}`": 0 for i in range(3)}
    return manifest, live


def test_a_table_the_stopped_run_created_but_never_recorded_says_so(reconcile, tmp_path):
    manifest, live = _estate(tmp_path)
    rec = reconcile.reconcile(_TargetSpark(live), manifest=manifest, target_catalog="lake",
                              reports=tmp_path, counts=False)
    rows = {r["table"]: r for r in rec["schemas"][0]["tables"]}
    assert rows["T000"]["structure"] == "created" and rows["T000"]["verdict"] == "STRUCTURE_ONLY"
    t2 = rows["T002"]
    assert t2["exists_in_target"] is True
    assert t2["structure"] == "unrecorded", "present in the target is not 'not attempted'"
    assert "no structure report records it" in (t2["reason"] or "")
    assert rows["T003"]["verdict"] == "NOT_MIGRATED" and rows["T003"]["structure"] == "not_attempted"


def test_a_planned_view_no_run_has_recorded_is_not_created_yet(reconcile, tmp_path):
    manifest, live = _estate(tmp_path)
    rec = reconcile.reconcile(_TargetSpark(live), manifest=manifest, target_catalog="lake",
                              reports=tmp_path, counts=False,
                              planned_views={"BULK": {"V1"}})
    views = {v["view"]: v for v in rec["schemas"][0]["views"]}
    assert views["V1"]["verdict"] == "VIEW_NOT_CREATED_YET"
    assert "approved plan" in (views["V1"]["reason"] or "")
    assert views["V2"]["verdict"] == "VIEW_NOT_CREATED_BY_THIS_PATH"


def test_the_planned_views_come_from_the_plans_view_statements(reconcile):
    plan = {"statements": [
        {"source_identifier": "DB.BULK.V1", "object_type": "VIEW", "target_fqn": "lake.bulk.v1"},
        {"source_identifier": "DB.BULK.T000", "object_type": "TABLE", "target_fqn": "lake.bulk.t000"},
        {"source_identifier": "DB.BULK.MV", "object_type": "TABLE", "snapshot_of": "MATERIALIZED VIEW",
         "target_fqn": "lake.bulk.mv"},
        {"source_identifier": "DB.OTHER.V9", "object_type": "VIEW", "target_fqn": "elsewhere.other.v9"}]}
    assert reconcile.plan_views(plan, "lake") == {"BULK": {"V1"}}

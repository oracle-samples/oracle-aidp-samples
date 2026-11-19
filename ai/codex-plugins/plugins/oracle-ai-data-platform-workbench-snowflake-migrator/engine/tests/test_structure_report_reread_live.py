"""The structure job never re-reads a report it has just written.

Live 2026-09-29, coverage re-run on AIDP: the table phase wrote each
schema's structure report, and the view phase then read it back for its
first view and failed

    JSONDecodeError  (in _load_report, called from create_planned_views)

while the same file, fetched a minute later, was valid JSON (2,516 bytes).
The /Workspace mount can serve a just-written file incompletely. The view
loop re-read and re-wrote the report once per view, so every view was
another chance to hit it; the first run of the day got through eight views
the same way and this one did not.

Now the view phase loads each schema's report once and keeps it in memory,
and a read that returns invalid JSON is retried briefly before it fails
loudly, naming the file.
"""
import importlib.util
import json
import pathlib
import sys

import pytest

SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "dataplane"


def _structure():
    sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location("snowmig_s01_reread", SCRIPTS / "01_create_structure.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Spark:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        return None


def _facts():
    return {("CORE", f"V{i}"): {"schema": "snowmig_core", "sql": f"CREATE VIEW v{i} AS SELECT 1",
                                "target_fqn": f"lake.snowmig_core.v{i}"} for i in range(1, 5)}


def test_the_view_phase_reads_each_schema_report_once(tmp_path, monkeypatch):
    s01 = _structure()
    (tmp_path / "structure_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": "lake.snowmig_core", "objects": {}}), encoding="utf-8")
    loads = []
    real = s01._load_report

    def counting(path, schema, target):
        loads.append(schema)
        return real(path, schema, target)

    monkeypatch.setattr(s01, "_load_report", counting)
    failures = s01.create_planned_views(_Spark(), _facts(), ["CORE"], tmp_path, "lake",
                                        dry_run=False, force=False)
    assert failures == 0
    assert loads == ["CORE"], "one read per schema, never one per view"
    report = json.loads((tmp_path / "structure_report_core.json").read_text(encoding="utf-8"))
    assert sorted(report["views"]) == ["V1", "V2", "V3", "V4"]


def _many_views(n, schemas=("CORE",)):
    return {(s, f"V{i}"): {"schema": f"snowmig_{s.lower()}", "sql": f"CREATE VIEW v{i} AS SELECT 1",
                           "target_fqn": f"lake.snowmig_{s.lower()}.v{i}"}
            for i in range(1, n + 1) for s in schemas}


def _count_report_writes(monkeypatch):
    writes = []
    real = pathlib.Path.write_text

    def counting(self, *a, **k):
        if self.name.startswith("structure_report"):
            writes.append(self.name)
        return real(self, *a, **k)

    monkeypatch.setattr(pathlib.Path, "write_text", counting)
    return writes


def test_the_view_phase_does_not_rewrite_the_report_per_view(tmp_path, monkeypatch):
    # The 50k-table scale estate: a 20,000-table schema's report is several
    # MB, and the view phase rewrote all of it to /Workspace after every one
    # of its 200 views -- the per-object write the table phase dropped.
    s01 = _structure()
    for s in ("CORE", "EDGE"):
        (tmp_path / f"structure_report_{s.lower()}.json").write_text(json.dumps(
            {"schema": s, "target": f"lake.snowmig_{s.lower()}", "objects": {}}), encoding="utf-8")
    writes = _count_report_writes(monkeypatch)
    failures = s01.create_planned_views(_Spark(), _many_views(120, ("CORE", "EDGE")),
                                        ["CORE", "EDGE"], tmp_path, "lake",
                                        dry_run=False, force=False)
    assert failures == 0
    assert len(writes) <= 4, writes
    for s in ("core", "edge"):
        report = json.loads((tmp_path / f"structure_report_{s}.json").read_text(encoding="utf-8"))
        assert len(report["views"]) == 120
        assert {v["status"] for v in report["views"].values()} == {"created"}


def test_views_created_before_a_crash_are_still_recorded(tmp_path, monkeypatch):
    s01 = _structure()
    (tmp_path / "structure_report_core.json").write_text(json.dumps(
        {"schema": "CORE", "target": "lake.snowmig_core", "objects": {}}), encoding="utf-8")

    class _Dies(_Spark):
        def sql(self, statement):
            if statement.startswith("CREATE VIEW v3 "):
                raise KeyboardInterrupt("cluster stopped")
            return super().sql(statement)

    with pytest.raises(KeyboardInterrupt):
        s01.create_planned_views(_Dies(), _many_views(5), ["CORE"], tmp_path, "lake",
                                 dry_run=False, force=False)
    report = json.loads((tmp_path / "structure_report_core.json").read_text(encoding="utf-8"))
    assert sorted(report["views"]) == ["V1", "V2"]


def test_a_transiently_incomplete_read_is_retried(tmp_path, monkeypatch):
    s01 = _structure()
    path = tmp_path / "structure_report_core.json"
    good = json.dumps({"schema": "CORE", "target": "lake.snowmig_core", "objects": {"T": {"status": "created"}}})
    path.write_text(good, encoding="utf-8")
    reads = iter([good[:40], good])
    monkeypatch.setattr(pathlib.Path, "read_text", lambda self, *a, **k: next(reads))
    monkeypatch.setattr(sys.modules["snowmig_source"], "REPORT_READ_WAIT", 0)
    report = s01._load_report(path, "CORE", "lake.snowmig_core")
    assert report["objects"] == {"T": {"status": "created"}}


def test_a_report_that_stays_invalid_fails_loudly_naming_it(tmp_path, monkeypatch):
    s01 = _structure()
    path = tmp_path / "structure_report_core.json"
    path.write_text("{ not json", encoding="utf-8")
    monkeypatch.setattr(sys.modules["snowmig_source"], "REPORT_READ_WAIT", 0)
    with pytest.raises(ValueError) as e:
        s01._load_report(path, "CORE", "lake.snowmig_core")
    assert "structure_report_core.json" in str(e.value)

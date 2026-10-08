"""Reports are written a chunk at a time, not a table at a time.

Live 2026-09-29, scale test on AIDP: SNOWMIG_SCALE.BULKDATA, a 1,000-table
schema with 50 of them in the plan. Discovery took 116 s; the structure job
then ran for more than 40 minutes to create 50 tables that take about 2 s
each at 8 threads. The loop rewrote the whole schema report to the
/Workspace mount after EVERY manifest table -- the 950 not in the plan
included -- a growing file, one slow write each: the per-table overhead
Lucas measured (~25 s a table) was this, not the CREATE.

The structure job now writes once per chunk (creates are idempotent, so a
crash loses nothing a re-run does not redo). The copy writes at most every
REPORT_WRITE_INTERVAL seconds and at every chunk end -- except under
--mode append, where a table copied but not yet recorded would be appended
twice by a resumed run, so each table is still recorded as it finishes.
"""
import math

import pytest

from fake_pushdown import FakeLakeSpark, FakeSnowflake
from test_copy_parallel import _estate as _copy_estate, _run as _copy_run
from test_structure_parallel import _Timed, _estate as _structure_estate, _run as _structure_run


def _count_writes(monkeypatch, prefix):
    import pathlib
    calls = []
    real = pathlib.Path.write_text

    def counting(self, *a, **k):
        if self.name.startswith(prefix):
            calls.append(self.name)
        return real(self, *a, **k)

    monkeypatch.setattr(pathlib.Path, "write_text", counting)
    return calls


def test_the_structure_report_is_written_per_chunk_not_per_table(monkeypatch, tmp_path):
    reports = _structure_estate(tmp_path, n=120)
    writes = _count_writes(monkeypatch, "structure_report")
    rc = _structure_run(monkeypatch, reports, _Timed(), "--parallel", "8")
    assert rc == 0
    assert len(writes) <= math.ceil(120 / 50) + 3, len(writes)


@pytest.mark.parametrize("mode", ["skip-existing", "overwrite"])
def test_the_copy_report_is_throttled_outside_append_mode(monkeypatch, tmp_path, mode):
    tables, lake, statements = _copy_estate(120)
    names = sorted(t for _d, _s, t in tables)
    writes = _count_writes(monkeypatch, "copy_report")
    rc, report = _copy_run(monkeypatch, tmp_path, FakeLakeSpark(FakeSnowflake(tables), lake),
                           statements, names, "--parallel", "8", "--mode", mode)
    assert rc == 0
    assert {t["status"] for t in report["tables"].values()} == {"verified"}
    assert len(writes) <= 2 * math.ceil(120 / 50) + 3, len(writes)


def test_append_mode_still_records_each_table_as_it_finishes(monkeypatch, tmp_path):
    tables, lake, statements = _copy_estate(60)
    names = sorted(t for _d, _s, t in tables)
    writes = _count_writes(monkeypatch, "copy_report")
    rc, _report = _copy_run(monkeypatch, tmp_path, FakeLakeSpark(FakeSnowflake(tables), lake),
                            statements, names, "--parallel", "8", "--mode", "append")
    assert rc == 0
    assert len(writes) >= 60, "a resumed append must never re-append an unrecorded table"

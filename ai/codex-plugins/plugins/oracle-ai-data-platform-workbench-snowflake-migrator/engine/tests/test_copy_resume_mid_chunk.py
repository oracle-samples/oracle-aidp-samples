"""A copy killed mid-chunk leaves a record for every table it wrote into.

`--parallel` (round 4) settles a chunk of 50 tables at once: ONE batched
source after-count once every INSERT in the chunk has finished (live
2026-09-29: 50 qualified counts in one UNION ALL, 25 s). The report was
then written only when the whole chunk had settled -- even at --parallel 1,
where the stage before it wrote the report after every table. A job that
died mid-chunk (a job timeout, a lost driver) left up to 50 tables holding
rows with NO record, so none of them carried "re-copy with --mode
overwrite, not append", and an `--mode append` re-run added every one of
their rows a second time.

So each table's record is written the moment its worker finishes. A table
whose rows have landed but whose source recount is still to come is
written PROVISIONALLY: status `failed`, `insert_completed: true`,
`awaiting_source_recount: true`, and the "not append" reason -- exactly
what a verification that raised records, because until the chunk settles
that is all anybody knows. When the chunk settles each provisional record
is replaced by its real verdict, in the manifest's order. A resume that
finds a provisional record treats it as the failure it says it is: a
skip-existing run does not soften it, and an `append` run refuses to add
rows on top of rows that already landed.

The kill is a BaseException raised from inside an INSERT: nothing in the
stage catches it, as nothing runs when a driver is lost. What is on disk at
that moment is what a resume would find.
"""
import json
import threading
import time

import pytest

from fake_pushdown import FakeLakeSpark, FakeSnowflake
from test_copy_parallel import _estate, _files
from test_data_migration_scripts import _inject_spark, _load


class _Killed(BaseException):
    """The job dying: not an Exception, so no `except Exception` sees it."""


class _Dies(FakeLakeSpark):
    """Dies on the INSERT into one table, before its rows land. Each INSERT
    takes a moment (`delay(table)`), so a thread pool really overlaps."""

    def __init__(self, *a, kill=None, delay=None, **kw):
        super().__init__(*a, **kw)
        self.kill = kill
        self.delay = delay or (lambda _t: 0.01)

    def sql(self, statement):
        if statement.startswith("INSERT"):
            table = statement.split(" SELECT ")[0].rsplit("`.`", 1)[1][:-1]
            if table == self.kill:
                raise _Killed(f"driver lost while copying {table}")
            time.sleep(self.delay(table))
        return super().sql(statement)


def _main(reports, config, *argv):
    return _load("02_copy_schema").main(
        ["--target-catalog", "lake", "--schema", "BULK", "--reports-dir",
         str(reports), "--source-config", str(config), "--output-dir", "",
         "--retries", "0", *argv])


def _report(reports):
    path = reports / "copy_report_bulk.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))["tables"]


_NAMES = [f"T{i:03d}" for i in range(6)]


def _killed_run(monkeypatch, tmp_path, parallel):
    tables, lake, statements = _estate(6)
    spark = _Dies(FakeSnowflake(tables), lake, kill="T003")
    reports, config = _files(tmp_path, statements, _NAMES)
    _inject_spark(monkeypatch, spark)
    with pytest.raises(_Killed):
        _main(reports, config, "--mode", "append", "--parallel", parallel)
    return tables, lake, statements, spark, reports, config


# ------------------------------------------- what is on disk at the kill

@pytest.mark.parametrize("parallel", ["1", "8"])
def test_every_table_with_rows_has_a_record_when_the_job_dies(
        monkeypatch, tmp_path, parallel):
    *_rest, spark, reports, _config = _killed_run(monkeypatch, tmp_path,
                                                  parallel)
    report = _report(reports)
    landed = [n for n in _NAMES if spark.rows[f"`lake`.`bulk`.`{n}`"]]
    assert landed, "some tables were copied before the job died"
    if parallel == "1":
        assert landed == ["T000", "T001", "T002"], \
            "serially, the tables before the one it died on"
    for name in landed:
        rec = report.get(name)
        assert rec is not None, (
            f"{name} holds {len(spark.rows[f'`lake`.`bulk`.`{name}`'])} "
            f"row(s) and has no record: an append re-run duplicates them")
        assert rec["status"] == "failed", rec
        assert rec["insert_completed"] is True
        assert rec["awaiting_source_recount"] is True
        assert "not append" in rec["reason"]
        assert "_pending" not in rec
    assert "T003" not in report, "its INSERT never landed"


# --------------------------------------------------------- the resume

@pytest.mark.parametrize("parallel", ["1", "8"])
def test_an_append_resume_does_not_duplicate_the_rows_that_landed(
        monkeypatch, tmp_path, parallel):
    tables, lake, statements, spark, reports, config = _killed_run(
        monkeypatch, tmp_path, parallel)
    landed = {n for n in _NAMES if spark.rows[f"`lake`.`bulk`.`{n}`"]}
    spark.kill = None
    rc = _main(reports, config, "--mode", "append", "--parallel", parallel)
    report = _report(reports)
    assert rc == 1, "the provisional tables are still not verified"
    for name in _NAMES:
        assert len(spark.rows[f"`lake`.`bulk`.`{name}`"]) == 3, \
            f"{name}: every source row exactly once"
        rec = report[name]
        if name in landed:
            assert rec["status"] == "failed", rec
            assert rec["insert_completed"] is True
            assert "--mode overwrite" in rec["reason"]
            assert "append" in rec["reason"]
        else:
            assert rec["status"] == "verified", rec


def test_an_overwrite_resume_verifies_the_provisional_tables(
        monkeypatch, tmp_path):
    tables, lake, statements, spark, reports, config = _killed_run(
        monkeypatch, tmp_path, "1")
    spark.kill = None
    rc = _main(reports, config, "--mode", "overwrite")
    report = _report(reports)
    assert rc == 0
    assert {r["status"] for r in report.values()} == {"verified"}
    assert not any(r.get("awaiting_source_recount") for r in report.values())
    for name in _NAMES:
        assert len(spark.rows[f"`lake`.`bulk`.`{name}`"]) == 3


def test_a_skip_existing_resume_does_not_soften_a_provisional_record(
        monkeypatch, tmp_path):
    """The rows are there at the source's count, so skip-existing would say
    `skipped_nonempty`; nothing re-verified them, so the failure stands."""
    tables, lake, statements, spark, reports, config = _killed_run(
        monkeypatch, tmp_path, "1")
    spark.kill = None
    rc = _main(reports, config)
    report = _report(reports)
    assert rc == 1
    for name in ("T000", "T001", "T002"):
        assert report[name]["status"] == "failed"
        assert report[name]["insert_completed"] is True
        assert "overwrite" in report[name]["reason"]


# ------------------------------------------------ once the chunk settles

def test_a_settled_chunk_is_recorded_in_the_manifests_order(
        monkeypatch, tmp_path):
    """At 8 threads the LAST table finishes first (its INSERT is quickest);
    the settled report still lists the tables in the manifest's order, and
    carries no provisional marker."""
    tables, lake, statements = _estate(6)
    order = []
    lock = threading.Lock()

    class _Ordered(_Dies):
        def sql(self, statement):
            out = super().sql(statement)
            if statement.startswith("INSERT"):
                with lock:
                    order.append(statement.split("`.`")[-1].split("`")[0])
            return out
    spark = _Ordered(FakeSnowflake(tables), lake,
                     delay=lambda t: 0.02 * (6 - int(t[1:])))
    reports, config = _files(tmp_path, statements, _NAMES)
    _inject_spark(monkeypatch, spark)
    rc = _main(reports, config, "--mode", "append", "--parallel", "8")
    assert rc == 0
    assert order != _NAMES, "the fake really finished them out of order"
    report = _report(reports)
    assert list(report) == _NAMES
    assert {r["status"] for r in report.values()} == {"verified"}
    assert not any("awaiting_source_recount" in r for r in report.values())

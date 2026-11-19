"""`03_reconcile --counts` reads the live counts in batches, not one by one.

It ran `SELECT COUNT(*)` once per table: a Spark job per table, so a
1,000-table schema was 1,000 jobs before the report was written. The copy
already learnt the shape that scales (live 2026-09-29: 50 fully qualified
counts in ONE UNION ALL, 25 s); reconcile now does the same on the target
side -- one query per chunk of 50, every branch a three-part
`catalog`.`schema`.`table` and tagged with its position, so no name has to
survive being a string literal.

What a batch cannot do is tell WHICH table failed. One unreadable table
fails the whole query, so that chunk falls back to a count per table, and
the table that cannot be read keeps its own `count_error` while the rest
are counted -- "could not look" still never renders as zero.
"""
import json
import re

import pytest

from test_data_migration_scripts import _load


@pytest.fixture(scope="module")
def reconcile():
    return _load("03_reconcile")


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        class Row(dict):
            def asDict(self):
                return dict(self)

            def __getitem__(self, key):
                return dict.__getitem__(self, key)
        return [Row(r) for r in self._rows]


class _TargetSpark:
    """SHOW TABLES and COUNT(*) over a target catalog of {fqn: rows}."""

    def __init__(self, counts, *, unreadable=()):
        self.counts = counts
        self.unreadable = set(unreadable)
        self.statements: list[str] = []

    def _count(self, fqn):
        if fqn in self.unreadable:
            raise RuntimeError(f"[INSUFFICIENT_PERMISSIONS] SELECT on {fqn}")
        return self.counts[fqn]

    def sql(self, statement):
        flat = " ".join(statement.split())
        self.statements.append(flat)
        if flat.upper().startswith("SHOW TABLES IN"):
            prefix = flat.split(" IN ", 1)[1] + "."
            return _Rows([{"tableName": k[len(prefix) + 1:-1]
                           .replace("``", "`")}
                          for k in self.counts if k.startswith(prefix)])
        if " UNION ALL " in flat or re.match(r"SELECT \d+ AS `i`", flat):
            out = []
            for branch in flat.split(" UNION ALL "):
                m = re.fullmatch(r"SELECT (\d+) AS `i`, COUNT\(\*\) AS `n` "
                                 r"FROM (.+)", branch)
                assert m, branch
                out.append({"i": int(m.group(1)),
                            "n": self._count(m.group(2))})
            return _Rows(out)
        m = re.fullmatch(r"SELECT COUNT\(\*\) AS n FROM (.+)", flat)
        if m:
            return _Rows([{"n": self._count(m.group(1))}])
        raise AssertionError(flat)


def _estate(tmp_path, n, *, verified_count=3):
    names = [f"T{i:03d}" for i in range(n)]
    manifest = {"schemas": [{"name": "BULK",
                             "tables": [{"name": t} for t in names],
                             "views": []}]}
    (tmp_path / "copy_report_bulk.json").write_text(json.dumps(
        {"schema": "BULK", "target": "lake.BULK",
         "tables": {t: {"status": "verified", "source_count": verified_count,
                        "target_count": verified_count} for t in names}}),
        encoding="utf-8")
    return manifest, {f"`lake`.`BULK`.`{t}`": verified_count for t in names}


def _counts_issued(spark):
    return [s for s in spark.statements if "COUNT(*)" in s]


def test_counts_are_one_query_per_chunk_of_fifty(reconcile, tmp_path):
    manifest, counts = _estate(tmp_path, 60)
    spark = _TargetSpark(counts)
    rec = reconcile.reconcile(spark, manifest=manifest, target_catalog="lake",
                              reports=tmp_path, counts=True)
    issued = _counts_issued(spark)
    assert [s.count("COUNT(*)") for s in issued] == [50, 10]
    rows = rec["schemas"][0]["tables"]
    assert {r["target_count"] for r in rows} == {3}
    assert rec["totals"] == {"MIGRATED_VERIFIED": 60}
    for s in issued:
        assert s.count("`lake`.`BULK`.") == s.count("COUNT(*)"), \
            "every branch is a three-part name"


def test_count_drift_is_still_caught_from_a_batch(reconcile, tmp_path):
    manifest, counts = _estate(tmp_path, 5)
    counts["`lake`.`BULK`.`T003`"] = 1
    spark = _TargetSpark(counts)
    rec = reconcile.reconcile(spark, manifest=manifest, target_catalog="lake",
                              reports=tmp_path, counts=True)
    by_name = {r["table"]: r for r in rec["schemas"][0]["tables"]}
    assert by_name["T003"]["verdict"] == "COUNT_DRIFT"
    assert by_name["T003"]["target_count"] == 1
    assert len(_counts_issued(spark)) == 1


def test_an_unreadable_table_falls_back_and_keeps_its_own_error(
        reconcile, tmp_path):
    manifest, counts = _estate(tmp_path, 4)
    spark = _TargetSpark(counts, unreadable={"`lake`.`BULK`.`T002`"})
    rec = reconcile.reconcile(spark, manifest=manifest, target_catalog="lake",
                              reports=tmp_path, counts=True)
    by_name = {r["table"]: r for r in rec["schemas"][0]["tables"]}
    assert by_name["T002"]["target_count"] is None
    assert "INSUFFICIENT_PERMISSIONS" in by_name["T002"]["count_error"]
    assert by_name["T002"]["verdict"] == "MIGRATED_VERIFIED", \
        "a count that could not be read is not drift"
    assert [by_name[t]["target_count"] for t in ("T000", "T001", "T003")] \
        == [3, 3, 3]
    issued = _counts_issued(spark)
    assert issued[0].count("COUNT(*)") == 4 and len(issued) == 1 + 4, \
        "the batch, then one count per table of that chunk"


def test_a_name_with_a_backtick_is_quoted_in_the_batch(reconcile, tmp_path):
    manifest = {"schemas": [{"name": "S", "tables": [{"name": "we`ird"}],
                             "views": []}]}
    spark = _TargetSpark({"`lake`.`S`.`we``ird`": 2})
    rec = reconcile.reconcile(spark, manifest=manifest, target_catalog="lake",
                              reports=tmp_path, counts=True)
    assert rec["schemas"][0]["tables"][0]["target_count"] == 2

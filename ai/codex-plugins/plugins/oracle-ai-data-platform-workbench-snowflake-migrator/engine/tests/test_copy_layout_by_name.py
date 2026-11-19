"""The copy moves each column by NAME, and refuses a source whose columns are
not the target's.

The write was `INSERT INTO tgt SELECT * FROM src`, which is positional, and
the pre-flight compared only the source's DECIMAL columns (by name). 01's
drift check compares the target with the PLAN, never with the live source.
So a Snowflake table rebuilt after the plan -- CREATE OR REPLACE with
FIRST_NAME and LAST_NAME swapped, or DROP COLUMN MIDDLE_NAME then ADD COLUMN
EMAIL, which lands at the end -- put every row's values in the wrong
columns, and counts (and decimal sums, keyed by name) still matched.
Reproduced on the real `_copy`: `status=verified` with
`first_name=Smith, last_name=Alice`, and an email address in `city`, under
both --verify modes; reconcile then showed MIGRATED_VERIFIED.

The fake here holds real rows and evaluates the INSERT the way Spark does:
a column list on the target, else the target's own order, filled
positionally from the SELECT list, where `*` is the source's own order.
"""
import re

import pytest

from test_data_migration_scripts import _load

_SRC, _TGT = "`ext`.`CORE`.`STAFF`", "`lake`.`core`.`staff`"


@pytest.fixture(scope="module")
def copy_schema():
    return _load("02_copy_schema")


class _RowSpark:
    def __init__(self, tables):
        # {fqn: (columns [(name, type)], rows [dict])}
        self.tables = {k: (list(c), [dict(r) for r in rows])
                       for k, (c, rows) in tables.items()}
        self.statements: list[str] = []

    @staticmethod
    def _get(row, name):
        return next(v for k, v in row.items() if k.casefold() == name.casefold())

    def _df(self, rows):
        class DF:
            def collect(self_inner):
                class Row(dict):
                    def asDict(self):
                        return dict(self)
                return [Row(r) for r in rows]
        return DF()

    def sql(self, statement):
        flat = " ".join(statement.split())
        self.statements.append(flat)
        low = flat.lower()
        if low.startswith("describe"):
            cols, _rows = self.tables[flat.split(None, 1)[1]]
            return self._df([{"col_name": n, "data_type": t} for n, t in cols])
        if "count(*)" in low:
            fqn = flat.rsplit(" FROM ", 1)[1]
            return self._df([{"n": len(self.tables[fqn][1])}])
        if low.startswith("select") and "sum(" in low:
            fqn = flat.rsplit(" FROM ", 1)[1]
            cols = re.findall(r"AS STRING\) AS `([^`]+)`", flat)
            rows = self.tables[fqn][1]
            return self._df([{c: str(sum(self._get(r, c) for r in rows))
                              for c in cols}])
        if low.startswith("insert"):
            m = re.match(r"INSERT (INTO|OVERWRITE) (\S+)(?: \(([^)]*)\))? "
                         r"SELECT (.*) FROM (\S+)$", flat)
            assert m, flat
            kind, tgt, tcols, select, src = m.groups()
            tgt_cols, tgt_rows = self.tables[tgt]
            src_cols, src_rows = self.tables[src]
            into = ([c.strip().strip("`") for c in tcols.split(",")]
                    if tcols else [n for n, _ in tgt_cols])
            picked = ([n for n, _ in src_cols] if select.strip() == "*"
                      else [c.strip().strip("`") for c in select.split(",")])
            assert len(into) == len(picked), flat
            landed = []
            for row in src_rows:
                values = [self._get(row, c) for c in picked]
                by_target = {c.casefold(): v for c, v in zip(into, values)}
                landed.append({n: by_target.get(n.casefold())
                               for n, _ in tgt_cols})
            if kind == "OVERWRITE":
                tgt_rows.clear()
            tgt_rows.extend(landed)
            return self._df([])
        raise AssertionError(f"unexpected statement: {flat}")


def _run(copy_schema, spark, verify="counts", mode="append"):
    return copy_schema._copy(spark, _SRC, _TGT, mode=mode, verify=verify,
                             retries=0, retry_base_delay=0, started="now")


_TARGET = [("ID", "decimal(38,0)"), ("FIRST_NAME", "string"),
           ("LAST_NAME", "string")]


@pytest.mark.parametrize("verify", ["counts", "counts+sums"])
@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_a_reordered_source_lands_each_value_in_its_own_column(
        copy_schema, verify, mode):
    spark = _RowSpark({
        _SRC: ([("ID", "decimal(38,0)"), ("LAST_NAME", "string"),
                ("FIRST_NAME", "string")],
               [{"ID": 1, "LAST_NAME": "Smith", "FIRST_NAME": "Alice"}]),
        _TGT: (_TARGET, [])})
    out = _run(copy_schema, spark, verify=verify, mode=mode)
    assert out["status"] == "verified", out
    assert spark.tables[_TGT][1] == [
        {"ID": 1, "FIRST_NAME": "Alice", "LAST_NAME": "Smith"}], \
        "the value named FIRST_NAME lands in FIRST_NAME"
    insert = next(s for s in spark.statements if s.startswith("INSERT"))
    assert "SELECT *" not in insert
    assert "SELECT `ID`, `FIRST_NAME`, `LAST_NAME` FROM" in insert, \
        "the source's columns, by name, in the target's order"


@pytest.mark.parametrize("verify", ["counts", "counts+sums"])
def test_a_dropped_and_added_column_is_drift_not_a_shifted_copy(
        copy_schema, verify):
    """DROP COLUMN MIDDLE_NAME, then ADD COLUMN EMAIL: same count, and
    positionally the email lands in CITY."""
    spark = _RowSpark({
        _SRC: ([("ID", "decimal(38,0)"), ("NAME", "string"),
                ("CITY", "string"), ("EMAIL", "string")],
               [{"ID": 1, "NAME": "Alice", "CITY": "Paris",
                 "EMAIL": "a@x.com"}]),
        _TGT: ([("ID", "decimal(38,0)"), ("NAME", "string"),
                ("MIDDLE_NAME", "string"), ("CITY", "string")], [])})
    out = _run(copy_schema, spark, verify=verify)
    assert out["status"] == "type_drift", out
    assert out["layout_drift"] == {"not_on_target": ["EMAIL"],
                                   "not_in_source": ["MIDDLE_NAME"]}
    assert "EMAIL" in out["reason"] and "MIDDLE_NAME" in out["reason"]
    assert "NOT copied" in out["reason"]
    assert not any(s.startswith("INSERT") for s in spark.statements), \
        "the layout is checked before anything is written"
    assert spark.tables[_TGT][1] == []


def test_a_renamed_column_is_drift(copy_schema):
    spark = _RowSpark({
        _SRC: ([("ID", "decimal(38,0)"), ("GIVEN_NAME", "string"),
                ("LAST_NAME", "string")], [{"ID": 1, "GIVEN_NAME": "A",
                                            "LAST_NAME": "S"}]),
        _TGT: (_TARGET, [])})
    out = _run(copy_schema, spark)
    assert out["status"] == "type_drift"
    assert out["layout_drift"] == {"not_on_target": ["GIVEN_NAME"],
                                   "not_in_source": ["FIRST_NAME"]}


def test_names_match_case_insensitively_as_spark_resolves_them(copy_schema):
    spark = _RowSpark({
        _SRC: ([("id", "decimal(38,0)"), ("first_name", "string"),
                ("last_name", "string")],
               [{"id": 1, "first_name": "Alice", "last_name": "Smith"}]),
        _TGT: (_TARGET, [])})
    out = _run(copy_schema, spark)
    assert out["status"] == "verified", out
    assert spark.tables[_TGT][1] == [
        {"ID": 1, "FIRST_NAME": "Alice", "LAST_NAME": "Smith"}]


def test_two_source_columns_that_differ_only_in_case_are_drift(copy_schema):
    """Snowflake allows "Name" and "NAME" side by side; Spark resolves
    them as one name, so which one lands where cannot be decided."""
    spark = _RowSpark({
        _SRC: ([("ID", "decimal(38,0)"), ("Name", "string"),
                ("NAME", "string")], [{"ID": 1, "Name": "a", "NAME": "b"}]),
        _TGT: ([("ID", "decimal(38,0)"), ("NAME", "string"),
                ("NAME_2", "string")], [])})
    out = _run(copy_schema, spark)
    assert out["status"] == "type_drift"
    assert "case" in out["reason"]
    assert not any(s.startswith("INSERT") for s in spark.statements)

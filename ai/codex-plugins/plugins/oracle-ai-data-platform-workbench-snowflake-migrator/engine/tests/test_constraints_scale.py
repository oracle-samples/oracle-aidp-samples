"""Constraint reads past the SHOW row cap, and FK grouping at estate scale.

Live 2026-09-29, 50k-table scale estate with 20,000 primary keys (one per
BIG20K table) and 12,000 foreign keys (W01-W06):

    SHOW PRIMARY KEYS IN DATABASE   10,000 rows   679 s
    SHOW IMPORTED KEYS IN DATABASE  12,000 rows  1155 s
    grouping the FK rows in Python                321 s

The primary-key read stopped at the 10,000-row cap and still succeeded, with
no note: 10,000 tables that declare a key read as "none declared". A capped
database read is now re-read schema by schema; a schema that is itself at
the cap is completed table by table, from the tables INFORMATION_SCHEMA.
TABLE_CONSTRAINTS lists for that constraint type. What still cannot be read
is a note naming it. The FK rows are indexed once instead of rescanned per
key.
"""
import pytest

from snowflake_source.extract import constraints as C
from snowflake_source.extract.constraints import build_constraints


def _pk(schema, table, col="ID", seq=1, name=None):
    return {"database_name": "DB", "schema_name": schema, "table_name": table,
            "column_name": col, "key_sequence": seq,
            "constraint_name": name or f"PK_{table}"}


class Estate:
    """SHOW ... KEYS IN DATABASE / SCHEMA / TABLE and TABLE_CONSTRAINTS over
    a fixed set of primary-key rows, capped like Snowflake."""

    def __init__(self, pk_rows, cap, *, table_constraints_fail=False):
        self.pk, self.cap, self.calls = pk_rows, cap, []
        self.tc_fail = table_constraints_fail

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        low = sql.lower()
        if "information_schema.table_constraints" in low:
            if self.tc_fail:
                raise RuntimeError("insufficient privileges on INFORMATION_SCHEMA")
            tables = sorted({r["table_name"] for r in self.pk
                             if r["schema_name"] == params["schema"]
                             and params["type"] == "PRIMARY KEY"})
            return [{"TABLE_NAME": t} for t in tables]
        if not low.startswith("show primary keys"):
            return []
        if " in database " in low:
            rows = self.pk
        elif " in schema " in low:
            schema = sql.rsplit(".", 1)[1].strip('"')
            rows = [r for r in self.pk if r["schema_name"] == schema]
        else:
            table = sql.rsplit(".", 1)[1].strip('"')
            rows = [r for r in self.pk if r["table_name"] == table]
        rows = sorted(rows, key=lambda r: (r["schema_name"], r["table_name"], r["key_sequence"]))
        return rows[:self.cap]


def _pk_tables(out):
    return {ident for ident, entries in out.items()
            for e in entries if e["constraint_type"] == "PRIMARY KEY"}


def test_under_the_cap_it_is_one_statement_per_kind_as_before():
    fake = Estate([_pk("A", f"T{i}") for i in range(3)], cap=10)
    out = build_constraints(fake, "DB", [], schemas=["A"])
    assert len(_pk_tables(out)) == 3
    assert len(fake.calls) == 3


def test_a_capped_database_read_is_re_read_schema_by_schema(monkeypatch):
    monkeypatch.setattr(C, "SHOW_ROW_CAP", 5)
    rows = [_pk("A", f"T{i}") for i in range(4)] + [_pk("B", f"U{i}") for i in range(4)]
    notes = []
    out = build_constraints(Estate(rows, cap=5), "DB", notes, schemas=["A", "B"])
    assert len(_pk_tables(out)) == 8, "every declared key, not the first 5 rows"
    assert notes == []


def test_a_schema_at_the_cap_is_completed_table_by_table(monkeypatch):
    monkeypatch.setattr(C, "SHOW_ROW_CAP", 5)
    # 12 tables, one with a two-column key straddling the page boundary.
    rows = [_pk("A", f"T{i:02d}") for i in range(12)]
    rows.insert(4, _pk("A", "T03", col="LINE_NO", seq=2, name="PK_T03"))
    fake = Estate(rows, cap=5)
    notes = []
    out = build_constraints(fake, "DB", notes, schemas=["A"])
    assert len(_pk_tables(out)) == 12
    assert out["DB.A.T03"][0]["columns"] == ["ID", "LINE_NO"], "the key at the boundary is re-read whole"
    assert notes == []
    per_table = [c for c in fake.calls if " in table " in c.lower()]
    assert len(per_table) < 12, "rows proven complete are kept, not re-read"


def test_a_capped_schema_that_cannot_be_completed_says_so(monkeypatch):
    monkeypatch.setattr(C, "SHOW_ROW_CAP", 5)
    rows = [_pk("A", f"T{i:02d}") for i in range(12)]
    notes = []
    build_constraints(Estate(rows, cap=5, table_constraints_fail=True), "DB", notes, schemas=["A"])
    assert any("PRIMARY KEY" in n and "DB.A" in n and "cap" in n for n in notes), notes


def test_a_capped_read_with_no_schema_list_says_so(monkeypatch):
    monkeypatch.setattr(C, "SHOW_ROW_CAP", 5)
    notes = []
    build_constraints(Estate([_pk("A", f"T{i}") for i in range(9)], cap=5), "DB", notes)
    assert any("cap" in n and "PRIMARY KEY" in n for n in notes), notes


def test_foreign_keys_are_grouped_in_one_pass(monkeypatch):
    n = 3000
    fk_rows = [{"pk_database_name": "DB", "pk_schema_name": "P", "pk_table_name": f"P{i}",
                "pk_column_name": "ID", "fk_database_name": "DB", "fk_schema_name": "C",
                "fk_table_name": f"C{i}", "fk_column_name": "PID", "key_sequence": 1,
                "fk_name": f"FK_{i}"} for i in range(n)]

    def run_sql(sql, params=None):
        return fk_rows if sql.lower().startswith("show imported keys") else []

    calls = [0]
    real = C._get

    def counting(row, key):
        calls[0] += 1
        return real(row, key)

    monkeypatch.setattr(C, "_get", counting)
    out = build_constraints(run_sql, "DB", [])
    assert len(out) == n
    assert out["DB.C.C7"][0]["references"] == "DB.P.P7"
    assert out["DB.C.C7"][0]["referenced_columns"] == ["ID"]
    assert calls[0] < 40 * n, f"{calls[0]} row reads for {n} keys: quadratic"


def test_a_result_past_the_cap_proves_that_show_is_not_capped(monkeypatch):
    # Live: SHOW IMPORTED KEYS IN DATABASE returned 12,000 rows. More than
    # the cap means no cap applied, so re-reading it per schema only cost
    # time (16+ minutes on the 20,000-table schema alone).
    monkeypatch.setattr(C, "SHOW_ROW_CAP", 5)

    class Uncapped(Estate):
        def __call__(self, sql, params=None):
            self.calls.append(sql)
            return self.pk if sql.lower().startswith("show primary keys in database") else []

    fake = Uncapped([_pk("A", f"T{i}") for i in range(7)], cap=None)
    out = build_constraints(fake, "DB", [], schemas=["A"])
    assert len(_pk_tables(out)) == 7
    assert not any(" in schema " in c.lower() for c in fake.calls)

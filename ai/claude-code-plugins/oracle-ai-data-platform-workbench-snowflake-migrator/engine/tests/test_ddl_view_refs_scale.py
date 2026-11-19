"""A view's references are rewritten without one regex per planned object.

Live 2026-09-29, 50k-table scale estate: `ddl` over the 50,500-object plan
ran for hours. Sampled on a 2,020-object slice (20 views): 88 s, 91% of it
compiling regexes in _rewrite_view_refs -- one pattern per name_map entry,
plus one per positional (bare and SCHEMA.NAME) form of every object in the
view's schema, for every view, ~0.7 ms each. The full plan is ~34 million
of them.

Now a name is only tried when its last part can occur in the view body.
The check is exact for an ASCII body and name; a non-ASCII body or a part
holding a quote character is still tried the old way.
"""
import re

from target import ddl
from target.ddl import build_create_view

N = 20_000


def _name_map():
    m = {f"DB.S.T_{i:05d}": f"lake.s.t_{i:05d}" for i in range(1, N + 1)}
    m["DB.S.V"] = "lake.s.v"
    return m


def _view(body):
    return {"source_identifier": "DB.S.V", "object_type": "VIEW",
            "source_database": "DB", "source_schema": "S",
            "view_ddl_get_ddl": f"create or replace view V as {body};",
            "source_metadata": {}, "columns": [], "compatibility_status": "supported"}


def _count_reference_scans(monkeypatch):
    calls = []
    real = re.finditer

    def counting(pattern, *a, **k):
        if isinstance(pattern, str) and pattern.startswith("(?<![\\w`\"$.])"):
            calls.append(pattern)
        return real(pattern, *a, **k)

    monkeypatch.setattr(ddl.re, "finditer", counting)
    return calls


def test_a_view_over_a_large_plan_scans_only_the_names_its_body_can_hold(monkeypatch):
    scans = _count_reference_scans(monkeypatch)
    res = build_create_view(
        _view("select a.ID from DB.S.T_00100 a join T_00200 b on a.ID = b.ID"),
        "lake.s.v", _name_map())
    assert not res.blocked, res.blocked_reason
    assert "lake.s.t_00100" in res.sql and "lake.s.t_00200" in res.sql
    assert "T_00100" not in res.sql and "T_00200" not in res.sql
    assert len(scans) <= 10, f"{len(scans)} reference regexes for a two-table view"


def test_an_unquoted_reference_in_another_case_is_still_rewritten():
    res = build_create_view(_view("select * from db.s.t_00042"), "lake.s.v", _name_map())
    assert "lake.s.t_00042" in res.sql


def test_a_non_ascii_body_is_still_rewritten():
    res = build_create_view(
        _view("select ID as \"CAFÉ\" from DB.S.T_00007"), "lake.s.v", _name_map())
    assert "lake.s.t_00007" in res.sql


def test_a_quoted_name_holding_a_quote_is_still_tried():
    name_map = {'DB.S.A"B': "lake.s.a_b", "DB.S.V": "lake.s.v"}
    res = build_create_view(_view('select * from DB.S."A""B"'), "lake.s.v", name_map)
    assert "lake.s.a_b" in res.sql

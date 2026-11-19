"""Everything in the estate that is NOT a table or a view.

`assess` ran SHOW TABLES and SHOW VIEWS and nothing else, so the plan reported
"7 of 7 objects can move" while procedures, UDFs, tasks, streams, stages,
pipes, sequences and file formats sat there unexamined. That number was true of
what was looked at and overstated coverage of the estate.

Nothing here is migratable by this plugin. The census exists so the scope is
stated rather than implied, and so the effort is estimable.
"""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.census import (
    KINDS, LANGUAGE_VERDICTS, build_census,
)


def _responses(**over):
    empty = {
        "information_schema.procedures": [],
        "information_schema.functions": [],
        "information_schema.sequences": [],
        "information_schema.stages": [],
        "information_schema.file_formats": [],
        "information_schema.pipes": [],
        "show tasks": [],
        "show streams": [],
        "show materialized views": [],
        "show dynamic tables": [],
        "show alerts": [],
        "show secrets": [],
        "show network rules": [],
        "show streamlits": [],
        "show notebooks": [],
        "show services": [],
        # Account-scoped: read once for the account, not once per database.
        "show shares": [],
        "show roles": [],
        "show network policies": [],
        "show applications": [],
        "show compute pools": [],
        "show replication groups": [],
    }
    empty.update(over)
    return empty


def _proc(name="SP_LOAD", lang="SQL", schema="PUBLIC"):
    return {"PROCEDURE_CATALOG": "DB", "PROCEDURE_SCHEMA": schema,
            "PROCEDURE_NAME": name, "ARGUMENT_SIGNATURE": "(A VARCHAR)",
            "PROCEDURE_LANGUAGE": lang, "PROCEDURE_OWNER": "ETL",
            "PROCEDURE_DEFINITION": "begin end;"}


def _func(name="UDF_X", lang="PYTHON"):
    return {"FUNCTION_CATALOG": "DB", "FUNCTION_SCHEMA": "PUBLIC",
            "FUNCTION_NAME": name, "ARGUMENT_SIGNATURE": "(A NUMBER)",
            "FUNCTION_LANGUAGE": lang, "FUNCTION_OWNER": "ETL",
            "FUNCTION_DEFINITION": "return 1"}


# ---------------------------------------------------------------- the census

def test_an_empty_estate_reports_zero_not_an_error():
    c = build_census(FakeSql(_responses()), ["DB"])
    assert c["total"] == 0
    assert c["objects"] == []
    assert all(k["readable"] for k in c["kinds"].values())


def test_procedures_are_found_and_named():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc("SP_LOAD_ORDERS")]})), ["DB"])
    assert c["total"] == 1
    o = c["objects"][0]
    assert o["kind"] == "PROCEDURE"
    assert o["source_identifier"] == "DB.PUBLIC.SP_LOAD_ORDERS"
    assert "(A VARCHAR)" in o["detail"]


def test_built_in_procedures_are_not_counted():
    # SHOW PROCEDURES returns Snowflake's own built-ins (33 on an empty
    # schema). INFORMATION_SCHEMA does not, which is why it is the source.
    fake = FakeSql(_responses())
    build_census(fake, ["DB"])
    assert not any("show procedures" in c.lower() for c in fake.calls), \
        "SHOW PROCEDURES mixes built-ins with user objects"


def test_nothing_in_the_census_is_ever_marked_migratable():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc()],
        "information_schema.functions": [_func()],
        "show tasks": [{"name": "T1", "schema_name": "PUBLIC",
                        "database_name": "DB", "state": "started"}]})), ["DB"])
    assert c["objects"]
    for o in c["objects"]:
        assert o["migratable"] is False
        assert o["reason"], f'{o["kind"]} has no reason'


def test_every_declared_kind_appears_in_the_summary_even_at_zero():
    # Including the sub-kinds a single read splits into: a UDTF and an
    # external function come out of the FUNCTIONS read, and a kind that is
    # only ever reported inside another one is a kind nobody can count.
    c = build_census(FakeSql(_responses()), ["DB"])
    declared = {k["kind"] for k in KINDS}
    declared |= {s for k in KINDS for s in (k.get("sub_kinds") or ())}
    assert set(c["kinds"]) == declared


# ------------------------------------------------------------ language triage

@pytest.mark.parametrize("lang", ["SQL", "JAVASCRIPT", "PYTHON", "JAVA", "SCALA"])
def test_every_language_has_a_verdict(lang):
    assert lang in LANGUAGE_VERDICTS
    v = LANGUAGE_VERDICTS[lang]
    assert v["effort"] in ("LOW", "MEDIUM", "HIGH")
    assert v["aidp_path"]


def test_javascript_is_the_hardest_case():
    assert LANGUAGE_VERDICTS["JAVASCRIPT"]["effort"] == "HIGH"
    assert "no" in LANGUAGE_VERDICTS["JAVASCRIPT"]["aidp_path"].lower()


def test_python_is_easier_than_javascript():
    assert LANGUAGE_VERDICTS["PYTHON"]["effort"] != "HIGH"


def test_a_procedure_carries_its_language_verdict():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc(lang="JAVASCRIPT")]})), ["DB"])
    o = c["objects"][0]
    assert o["language"] == "JAVASCRIPT"
    assert o["effort"] == "HIGH"


def test_an_unknown_language_is_not_guessed():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc(lang="BRAINFUCK")]})), ["DB"])
    o = c["objects"][0]
    assert o["effort"] == "UNKNOWN"
    assert "not recognised" in o["reason"].lower() or "unknown" in o["reason"].lower()


def test_effort_rollup_counts_by_language():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc("A", "SQL"), _proc("B", "SQL")],
        "information_schema.functions": [_func("C", "PYTHON")]})), ["DB"])
    assert c["by_language"]["SQL"] == 2
    assert c["by_language"]["PYTHON"] == 1


# ----------------------------------------------------------- graceful failure

def test_one_unreadable_kind_does_not_lose_the_others():
    class Partial(FakeSql):
        def __call__(self, sql, params=None):
            if "show tasks" in sql.lower():
                raise RuntimeError("Insufficient privileges to view tasks")
            return super().__call__(sql, params)

    c = build_census(Partial(_responses(**{
        "information_schema.procedures": [_proc()]})), ["DB"])
    assert c["total"] == 1, "the procedure still counted"
    assert c["kinds"]["TASK"]["readable"] is False
    assert "privileges" in c["kinds"]["TASK"]["note"]
    assert any("TASK" in u for u in c["unreadable"])


def test_an_unreadable_kind_reports_no_count_rather_than_zero():
    class Partial(FakeSql):
        def __call__(self, sql, params=None):
            if "show streams" in sql.lower():
                raise RuntimeError("denied")
            return super().__call__(sql, params)

    c = build_census(Partial(_responses()), ["DB"])
    assert c["kinds"]["STREAM"]["count"] is None, \
        "'we could not look' must not render as zero"


# --------------------------------------------------------- scope statement

def test_the_scope_statement_names_what_was_examined():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc()]})), ["DB"])
    s = c["scope_statement"]
    assert "1" in s
    assert "cannot" in s.lower() or "not migrat" in s.lower()


def test_an_all_empty_census_never_claims_the_whole_estate():
    # SHOW and INFORMATION_SCHEMA return only objects the role holds a
    # privilege on. A read-only role with USAGE + SELECT sees no task, stream,
    # procedure or pipe at all, and every statement still succeeds -- so a
    # zero here is "none visible", not "none exist", and must not be turned
    # into "the migratable count is the whole estate".
    c = build_census(FakeSql(_responses()), ["DB"], role="MIGRATION_READER_ROLE")
    s = c["scope_statement"]
    assert "whole estate" not in s.lower()
    assert "MIGRATION_READER_ROLE" in s
    assert "visible" in s.lower() and "lower bound" in s.lower()
    assert c["role"] == "MIGRATION_READER_ROLE"
    assert c["completeness"] == "visible-to-role"


def test_zero_count_is_distinguished_from_denied():
    from report.render import render_census
    c = build_census(FakeSql(_responses()), ["DB"], role="R")
    task = c["kinds"]["TASK"]
    assert task["count"] == 0 and task["readable"] is True, "unchanged: we looked"
    assert "visible" in task["note"] and "lower bound" in task["note"]
    row = next(line for line in render_census(c).splitlines()
               if line.startswith("| Task |"))
    assert "| 0 | yes |" not in row, row
    assert "lower bound" in row


def test_a_populated_census_still_says_the_count_is_a_lower_bound():
    c = build_census(FakeSql(_responses(**{
        "information_schema.procedures": [_proc()]})), ["DB"], role="R")
    assert "lower bound" in c["scope_statement"].lower()
    assert "role `R`" in c["scope_statement"]


def test_scope_statement_without_a_role_falls_back_to_current_role_wording():
    c = build_census(FakeSql(_responses()), ["DB"])
    assert "current role" in c["scope_statement"]
    assert "None" not in c["scope_statement"]


def test_the_census_header_lists_the_grants_a_complete_census_needs():
    from report.render import render_census
    c = build_census(FakeSql(_responses()), ["DB"], role="R")
    note = c["visibility_note"]
    for grant in ("OPERATE", "MONITOR", "USAGE", "SELECT", "IMPORTED PRIVILEGES"):
        assert grant in note, grant
    md = render_census(c)
    assert note in md, "the grants list is in CENSUS.md's header"
    assert md.index(note) < md.index("## Counts by kind")

"""What the census could not see at all, and what it saw as the wrong thing.

The census answered for ten kinds. An alert, a secret, a network rule, a
Streamlit app, a Snowflake notebook, a container service, a share, a role, a
network policy, an application and a compute pool were not absent from the
report -- they were absent from the *questions*, which is worse, because the
report then reads as complete.

Two kinds were present but wrong: a UDTF and an external function were both
counted as scalar UDFs, and an external stage (already in object storage) got
the same verdict as an internal one (must be unloaded first).

Everything here is a SHOW or a SELECT, so the read-only transport takes it
unchanged, and everything the role cannot read reports as unreadable rather
than as zero -- "we could not look" and "there are none" lead to opposite
decisions.
"""
import pytest

from fake_sql import FakeSql
from snowflake_source.extract.census import KINDS, build_census


# The whole question set, all empty. Anything the census asks that is missing
# from this dict raises inside FakeSql and shows up as an unreadable kind,
# which is exactly how a forgotten read is caught.
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
        "show shares": [],
        "show roles": [],
        "show network policies": [],
        "show applications": [],
        "show compute pools": [],
        "show replication groups": [],
    }
    empty.update(over)
    return empty


def _spec(kind):
    return next(k for k in KINDS if k["kind"] == kind)


def _issued(fake, needle):
    return [c for c in fake.calls if needle in " ".join(c.split()).lower()]


def test_every_statement_the_census_issues_is_a_read():
    # The transport is the guarantee, not the convention: a new kind whose
    # read is not a SELECT or a SHOW would be refused at the connection, and
    # it must be refused here first.
    from snowflake_source.conn import assert_read_only

    fake = FakeSql(_responses())
    build_census(fake, ["DB"])
    assert fake.calls
    for sql in fake.calls:
        assert_read_only(sql)


# --------------------------------------------------------- the missing kinds

@pytest.mark.parametrize("kind,relation", [
    ("ALERT", "alerts"),
    ("SECRET", "secrets"),
    ("NETWORK_RULE", "network rules"),
    ("STREAMLIT", "streamlits"),
    ("NOTEBOOK", "notebooks"),
    ("SERVICE", "services"),
])
def test_each_new_database_kind_is_declared_and_asked_for(kind, relation):
    spec = _spec(kind)
    assert spec["source"] == "show", "SHOW is already on the read-only allowlist"
    assert spec["relation"] == relation
    assert spec.get("scope", "database") == "database"
    assert len(spec["reason"]) > 60, "a kind with no consequence stated is a count"

    fake = FakeSql(_responses())
    census = build_census(fake, ["DB"])
    assert _issued(fake, f"show {relation} in database"), \
        f"{kind} is declared but never read"
    assert census["kinds"][kind]["readable"] is True
    assert census["kinds"][kind]["count"] == 0


def test_an_alert_is_counted_and_carries_its_cutover_consequence():
    census = build_census(FakeSql(_responses(**{
        "show alerts": [{"name": "AL_STALE_ORDERS", "schema_name": "SALES",
                         "database_name": "DB", "state": "started"}]})), ["DB"])
    alert = next(o for o in census["objects"] if o["kind"] == "ALERT")
    assert alert["source_identifier"] == "DB.SALES.AL_STALE_ORDERS"
    assert alert["migratable"] is False
    assert "stops firing" in alert["reason"]


def test_a_secret_is_named_but_its_value_is_never_read():
    fake = FakeSql(_responses(**{
        "show secrets": [{"name": "S_API", "schema_name": "SALES",
                          "database_name": "DB"}]}))
    census = build_census(fake, ["DB"])
    secret = next(o for o in census["objects"] if o["kind"] == "SECRET")
    assert secret["source_identifier"] == "DB.SALES.S_API"
    assert "credential store" in secret["reason"]
    assert not any("describe secret" in c.lower() for c in fake.calls), \
        "the census names secrets; it never reads a secret value"


# ------------------------------------------------------------ account scope

_ACCOUNT_KINDS = ("SHARE", "ROLE", "NETWORK_POLICY", "APPLICATION",
                  "COMPUTE_POOL")


@pytest.mark.parametrize("kind,relation", [
    ("SHARE", "shares"),
    ("ROLE", "roles"),
    ("NETWORK_POLICY", "network policies"),
    ("APPLICATION", "applications"),
    ("COMPUTE_POOL", "compute pools"),
])
def test_an_account_scoped_kind_is_read_once_not_once_per_database(kind, relation):
    assert _spec(kind)["scope"] == "account"
    fake = FakeSql(_responses())
    census = build_census(fake, ["DB_A", "DB_B", "DB_C"])
    issued = _issued(fake, f"show {relation}")
    assert len(issued) == 1, f"{kind} is account-scoped; {len(issued)} reads issued"
    assert "in database" not in issued[0].lower()
    assert census["kinds"][kind]["scope"] == "account"


def test_a_share_is_a_live_contract_and_is_named_as_one():
    census = build_census(FakeSql(_responses(**{
        "show shares": [{"name": "ORG.ACCT.OUTBOUND_SALES", "kind": "OUTBOUND",
                         "database_name": "DB", "to": "CONSUMER_ACCT"}]})),
        ["DB"])
    share = next(o for o in census["objects"] if o["kind"] == "SHARE")
    assert share["source_identifier"] == "ORG.ACCT.OUTBOUND_SALES", \
        "an account object has no database.schema prefix to invent"
    assert "None" not in share["source_identifier"]
    assert "?" not in share["source_identifier"]
    assert "consumer" in share["reason"].lower()
    assert census["by_kind"]["SHARE"] == 1


def test_an_outbound_share_names_the_account_reading_through_it():
    census = build_census(FakeSql(_responses(**{
        "show shares": [{"name": "ORG.ACCT.OUTBOUND_SALES", "kind": "OUTBOUND",
                         "database_name": "DB", "to": "CONSUMER_ACCT"}]})),
        ["DB"])
    share = census["objects"][0]
    assert "OUTBOUND" in share["detail"]
    assert "CONSUMER_ACCT" in share["detail"], \
        "the consumer is who has to be told; naming the share is not enough"


def test_an_inbound_share_is_a_source_lost_not_a_consumer_broken():
    census = build_census(FakeSql(_responses(**{
        "show shares": [{"name": "ORG.PROVIDER.MARKET_DATA", "kind": "INBOUND",
                         "database_name": "MARKET", "owner": "PROVIDER"}]})),
        ["DB"])
    share = census["objects"][0]
    assert "INBOUND" in share["detail"]
    assert "consumer's queries" not in share["reason"], \
        "an inbound share breaks this estate, not somebody else's"
    assert "provider" in share["reason"].lower()


def test_an_account_read_the_role_cannot_run_is_unreadable_never_zero():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show shares" in " ".join(sql.split()).lower():
                raise RuntimeError("Insufficient privileges to operate on shares")
            return super().__call__(sql, params)

    census = build_census(Denied(_responses()), ["DB"], role="READER")
    share = census["kinds"]["SHARE"]
    assert share["count"] is None, "'we could not look' must not render as zero"
    assert share["readable"] is False
    assert any("SHARE" in u for u in census["unreadable"])
    assert "account" in " ".join(census["unreadable"]).lower(), \
        "the note must say the read was account-scoped, not blame a database"
    assert "SHARE" in census["scope_statement"]


def test_an_account_read_that_fails_does_not_lose_the_database_kinds():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show roles" in " ".join(sql.split()).lower():
                raise RuntimeError("denied")
            return super().__call__(sql, params)

    census = build_census(Denied(_responses(**{
        "show alerts": [{"name": "A", "schema_name": "S",
                         "database_name": "DB"}]})), ["DB"])
    assert census["total"] == 1
    assert census["kinds"]["ROLE"]["readable"] is False
    assert census["kinds"]["ALERT"]["count"] == 1


# -------------------------------------------------------- internal vs external

def _stage(name="ST_RAW", stage_type="Internal Named", url="", region=None):
    return {"STAGE_NAME": name, "STAGE_SCHEMA": "SALES",
            "STAGE_TYPE": stage_type, "STAGE_URL": url, "STAGE_REGION": region}


def test_the_stage_read_selects_what_tells_the_two_apart():
    fake = FakeSql(_responses())
    build_census(fake, ["DB"])
    select = next(c for c in fake.calls
                  if "information_schema.stages" in c.lower())
    low = select.lower()
    for col in ("stage_type", "stage_url", "stage_region"):
        assert col in low, f"{col} is what decides the verdict"


def test_an_external_stage_and_an_internal_stage_get_different_verdicts():
    census = build_census(FakeSql(_responses(**{
        "information_schema.stages": [
            _stage("ST_INTERNAL", "Internal Named"),
            _stage("ST_EXTERNAL", "External Named",
                   url="s3://example-bucket/raw/", region="us-east-1")]})),
        ["DB"])
    internal = next(o for o in census["objects"]
                    if o["source_identifier"].endswith("ST_INTERNAL"))
    external = next(o for o in census["objects"]
                    if o["source_identifier"].endswith("ST_EXTERNAL"))
    assert internal["reason"] != external["reason"]
    assert "unloaded to object storage" in internal["reason"].lower(), \
        "an internal stage has to be unloaded before AIDP can see anything"
    assert "nothing has to be unloaded" in external["reason"].lower(), \
        "an external stage is already in object storage; AIDP can be pointed "\
        "at it"
    assert "s3://example-bucket/raw/" in external["detail"]
    assert "Internal" in internal["detail"]


def test_a_stage_select_that_cannot_name_those_columns_keeps_the_old_answer():
    # An account or edition where the detail column is not there must lose the
    # distinction, not the stage. Never a crash, never an unreadable kind.
    class OldAccount(FakeSql):
        def __call__(self, sql, params=None):
            if "stage_type" in sql.lower():
                raise RuntimeError("invalid identifier 'STAGE_TYPE'")
            return super().__call__(sql, params)

    census = build_census(OldAccount(_responses(**{
        "information_schema.stages": [{"STAGE_NAME": "ST", "STAGE_SCHEMA": "S"}]
    })), ["DB"])
    stage = next(o for o in census["objects"] if o["kind"] == "STAGE")
    assert census["kinds"]["STAGE"]["readable"] is True
    assert census["kinds"]["STAGE"]["count"] == 1
    assert stage["reason"] == _spec("STAGE")["reason"]
    assert "detail" in census["kinds"]["STAGE"]["note"].lower() \
        or "distinguish" in census["kinds"]["STAGE"]["note"].lower()


# --------------------------------------------- UDTF and the external function

def _function(name="UDF_X", lang="PYTHON", data_type="NUMBER(38,0)",
              external="NO", api=None):
    return {"FUNCTION_NAME": name, "FUNCTION_SCHEMA": "SALES",
            "FUNCTION_LANGUAGE": lang, "ARGUMENT_SIGNATURE": "(A NUMBER)",
            "FUNCTION_OWNER": "ETL", "DATA_TYPE": data_type,
            "IS_EXTERNAL": external, "API_INTEGRATION": api}


def test_the_function_read_selects_the_columns_that_split_the_three():
    fake = FakeSql(_responses())
    build_census(fake, ["DB"])
    select = next(c for c in fake.calls
                  if "information_schema.functions" in c.lower())
    low = select.lower()
    for col in ("data_type", "is_external", "api_integration"):
        assert col in low, col


def test_a_table_function_is_not_reported_as_a_scalar_udf():
    census = build_census(FakeSql(_responses(**{
        "information_schema.functions": [
            _function("UDTF_SPLIT", data_type="TABLE (PART VARCHAR)"),
            _function("UDF_PLAIN")]})), ["DB"])
    udtf = next(o for o in census["objects"]
                if o["source_identifier"].endswith("UDTF_SPLIT"))
    plain = next(o for o in census["objects"]
                 if o["source_identifier"].endswith("UDF_PLAIN"))
    assert udtf["kind"] == "UDTF"
    assert plain["kind"] == "FUNCTION"
    assert udtf["reason"] != plain["reason"]
    assert "table" in udtf["reason"].lower()
    assert census["by_kind"]["UDTF"] == 1
    assert census["kinds"]["UDTF"]["count"] == 1


def test_an_external_function_names_the_endpoint_it_calls_out_to():
    census = build_census(FakeSql(_responses(**{
        "information_schema.functions": [
            _function("EF_SCORE", lang=None, external="YES",
                      api="SCORING_API_INT")]})), ["DB"])
    ef = next(o for o in census["objects"] if o["kind"] == "EXTERNAL_FUNCTION")
    assert "SCORING_API_INT" in ef["detail"]
    assert "api integration" in ef["reason"].lower() \
        or "remote endpoint" in ef["reason"].lower()
    assert "not recognised" not in ef["reason"], \
        "an external function has no handler language to fail to recognise"
    assert ef["effort"] and ef["effort"] != "UNKNOWN"


def test_a_table_function_in_an_unknown_language_keeps_both_findings():
    census = build_census(FakeSql(_responses(**{
        "information_schema.functions": [
            _function("UDTF_ODD", lang="BRAINFUCK",
                      data_type="TABLE (A VARCHAR)")]})), ["DB"])
    udtf = census["objects"][0]
    assert udtf["kind"] == "UDTF"
    assert "table" in udtf["reason"].lower()
    assert "not recognised" in udtf["reason"], \
        "narrowing the kind must not swallow the unknown-handler finding"
    assert udtf["effort"] == "UNKNOWN"


def test_a_function_select_without_the_new_columns_still_lists_functions():
    class OldAccount(FakeSql):
        def __call__(self, sql, params=None):
            if "is_external" in sql.lower():
                raise RuntimeError("invalid identifier 'IS_EXTERNAL'")
            return super().__call__(sql, params)

    census = build_census(OldAccount(_responses(**{
        "information_schema.functions": [
            {"FUNCTION_NAME": "F", "FUNCTION_SCHEMA": "S",
             "FUNCTION_LANGUAGE": "SQL", "ARGUMENT_SIGNATURE": "()",
             "FUNCTION_OWNER": "ETL"}]})), ["DB"])
    assert census["kinds"]["FUNCTION"]["readable"] is True
    assert census["kinds"]["FUNCTION"]["count"] == 1
    assert census["objects"][0]["kind"] == "FUNCTION"
    assert census["kinds"]["UDTF"]["count"] is None, \
        "not distinguishable is not the same as none present"
    assert census["kinds"]["EXTERNAL_FUNCTION"]["count"] is None


# --------------------------------------------------------------- the report

def test_census_md_lists_every_new_kind():
    from report.render import render_census
    md = render_census(build_census(FakeSql(_responses()), ["DB"], role="R"))
    for kind in ("Alert", "Secret", "Network Rule", "Streamlit", "Notebook",
                 "Service", "Share", "Role", "Network Policy", "Application",
                 "Compute Pool", "UDTF", "External Function"):
        assert f"| {kind} |" in md, f"{kind} is absent from CENSUS.md"


def test_census_md_says_which_reads_were_account_scoped():
    from report.render import render_census
    md = render_census(build_census(FakeSql(_responses()), ["DB"], role="R"))
    share_row = next(line for line in md.splitlines()
                     if line.startswith("| Share |"))
    assert "account" in share_row
    alert_row = next(line for line in md.splitlines()
                     if line.startswith("| Alert |"))
    assert "database" in alert_row


def test_an_account_read_the_role_cannot_run_reads_as_not_visible():
    from report.render import render_census

    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "show shares" in " ".join(sql.split()).lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    md = render_census(build_census(Denied(_responses()), ["DB"], role="R"))
    row = next(line for line in md.splitlines() if line.startswith("| Share |"))
    assert "| 0 |" not in row, row
    assert "not visible to this role" in row


def test_the_visibility_note_names_the_grants_the_new_reads_need():
    census = build_census(FakeSql(_responses()), ["DB"], role="R")
    note = census["visibility_note"].lower()
    for what in ("share", "role", "network", "alert", "secret", "compute pool"):
        assert what in note, what


def test_the_empty_scope_statement_does_not_still_list_only_the_old_kinds():
    census = build_census(FakeSql(_responses()), ["DB"], role="R")
    s = census["scope_statement"].lower()
    assert "share" in s and "alert" in s, \
        "the sentence claims what was looked at; it must name the new reads"
    assert "whole estate" not in s
    assert "lower bound" in s


def test_the_function_read_does_not_select_the_documented_but_rejected_name():
    """Live 2026-09-22: INFORMATION_SCHEMA.FUNCTIONS has IS_EXTERNAL, and the
    documented IS_EXTERNAL_FUNCTION is an invalid identifier. Selecting the
    wrong name silently cost the UDTF/external-function distinction on a
    real account (reported as not distinguishable, correctly, but avoidably)."""
    fake = FakeSql(_responses())
    build_census(fake, ["DB"])
    select = next(c for c in fake.calls
                  if "information_schema.functions" in c.lower())
    assert "is_external_function" not in select.lower()
    assert "is_external" in select.lower()


def test_an_external_function_is_recognised_from_a_no_yes_or_y_flag():
    for flag in ("YES", "Y"):
        census = build_census(FakeSql(_responses(**{
            "information_schema.functions": [
                _function("EF", lang=None, external=flag, api="API_X")]})), ["DB"])
        assert census["objects"][0]["kind"] == "EXTERNAL_FUNCTION", flag
    census = build_census(FakeSql(_responses(**{
        "information_schema.functions": [_function("F", external="NO")]})), ["DB"])
    assert census["objects"][0]["kind"] == "FUNCTION"


# --------------------------- a kind readable in one database, denied in another
#
# The
# per-kind `readable` flag was one value for the whole run: a kind that answers
# in DB A and is denied in DB B reported as "not visible to this role" with a
# null count, while the rows counted in DB A sat in the same report's by_kind
# and object table. The report contradicted itself, and the count it hid was
# real.
#
# Three states, not two: every database answered; none did; or some did, which
# is a real number that is also a lower bound, and has to say so.

class _DeniedIn(FakeSql):
    """Denies one kind in one database, exactly like a mixed-privilege role."""

    def __init__(self, responses, *, database, needle):
        super().__init__(responses)
        self.database = database
        self.needle = needle

    def __call__(self, sql, params=None):
        flat = " ".join(sql.split()).lower()
        if self.needle in flat and self.database.lower() in flat:
            raise RuntimeError(f"Insufficient privileges in {self.database}")
        return super().__call__(sql, params)


def _two_db_tasks():
    return _responses(**{"show tasks": [
        {"name": "T", "schema_name": "S", "state": "started"}]})


def test_a_kind_denied_in_one_database_still_reports_what_it_counted():
    census = build_census(
        _DeniedIn(_two_db_tasks(), database="DB2", needle="show tasks"),
        ["DB1", "DB2"])
    info = census["kinds"]["TASK"]
    assert info["count"] == 1, "DB1 answered and the row is in the report"
    assert census["by_kind"]["TASK"] == 1
    assert info["count"] == census["by_kind"]["TASK"], \
        "the count and the object table may not contradict each other"


def test_a_partially_read_kind_says_which_database_was_denied():
    census = build_census(
        _DeniedIn(_two_db_tasks(), database="DB2", needle="show tasks"),
        ["DB1", "DB2"])
    info = census["kinds"]["TASK"]
    assert info["unread"] == "partial"
    assert info["denied_databases"] == ["DB2"]
    assert "DB2" in info["note"]
    assert "lower bound" in info["note"].lower()


def test_a_kind_denied_everywhere_is_still_not_a_zero():
    class DeniedAll(FakeSql):
        def __call__(self, sql, params=None):
            if "show tasks" in sql.lower():
                raise RuntimeError("Insufficient privileges")
            return super().__call__(sql, params)

    census = build_census(DeniedAll(_two_db_tasks()), ["DB1", "DB2"])
    info = census["kinds"]["TASK"]
    assert info["count"] is None
    assert info["readable"] is False
    assert info["unread"] == "denied"


def test_a_kind_readable_everywhere_is_unchanged():
    census = build_census(FakeSql(_two_db_tasks()), ["DB1", "DB2"])
    info = census["kinds"]["TASK"]
    assert info["readable"] is True
    assert info["unread"] is None
    assert info["count"] == 2, "one per database"


def test_the_report_marks_a_partial_count_rather_than_calling_it_denied():
    from report.render import render_census
    census = build_census(
        _DeniedIn(_two_db_tasks(), database="DB2", needle="show tasks"),
        ["DB1", "DB2"])
    md = render_census(census)
    row = next(line for line in md.splitlines()
               if line.startswith("| Task "))
    assert "not visible to this role" not in row, row
    assert "1" in row
    assert "partial" in row.lower(), row


# ------------------- the role a count is attributed to may not be the whole
#                     authority it was produced under
#
# Live 2026-09-23. A session connected as `role=SNOWMIG_LIMITED`, which holds
# USAGE on exactly one database, read the whole account:
#
#     current_role()            SNOWMIG_LIMITED
#     current_secondary_roles() {"roles":"ORGADMIN,ACCOUNTADMIN","value":"ALL"}
#
# `USE SECONDARY ROLES NONE` on the same session turned the same read into
# "Database 'SNOWMIG_COV_B' does not exist or not authorized". Every count in
# CENSUS.md is attributed to CURRENT_ROLE(), so where secondary roles are
# active the report names an authority the numbers were not produced under --
# and it errs in the unsafe direction, making a restricted role look
# sufficient when the run leaned on ACCOUNTADMIN.

def test_secondary_roles_are_parsed_from_snowflakes_json():
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active(
        '{"roles":"ORGADMIN,ACCOUNTADMIN","value":"ALL"}') == [
            "ORGADMIN", "ACCOUNTADMIN"]


def test_no_secondary_roles_reads_as_none():
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active('{"roles":"","value":""}') == []
    assert secondary_roles_active(None) == []
    assert secondary_roles_active("") == []


def test_an_unreadable_shape_yields_no_roles_rather_than_a_guess():
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active("{not json") == []
    assert secondary_roles_active("{}") == []


def test_a_bare_comma_list_is_accepted_too():
    from snowflake_source.extract.census import secondary_roles_active
    assert secondary_roles_active("A, B") == ["A", "B"]


def test_the_census_names_the_secondary_roles_the_reads_also_had():
    census = build_census(FakeSql(_responses()), ["DB"], role="LIMITED",
                          secondary_roles=["ACCOUNTADMIN"])
    note = census["visibility_note"] + census["scope_statement"]
    assert "ACCOUNTADMIN" in note, \
        "a count produced with ACCOUNTADMIN may not be attributed to LIMITED alone"
    assert "secondary" in note.lower()


def test_without_secondary_roles_the_sentence_is_unchanged():
    census = build_census(FakeSql(_responses()), ["DB"], role="LIMITED",
                          secondary_roles=[])
    note = census["visibility_note"] + census["scope_statement"]
    assert "secondary" not in note.lower()
    assert "LIMITED" in note


def test_the_per_kind_note_carries_the_same_attribution():
    census = build_census(FakeSql(_responses()), ["DB"], role="LIMITED",
                          secondary_roles=["ACCOUNTADMIN"])
    zero = census["kinds"]["PIPE"]            # nothing visible
    assert "ACCOUNTADMIN" in zero["note"], zero["note"]


# ------------- one explanation per kind hid the verdict that mattered
#
# Live 2026-09-25, the first estate to hold BOTH an internal and an external
# stage, and BOTH an inbound and an outbound share. The per-object table
# showed all four with the right detail. The "Why each kind cannot move"
# section printed ONE paragraph per kind -- whichever object came first --
# so the internal stage, whose files have to be unloaded before AIDP can see
# them, was covered by the external stage's text saying nothing has to be
# unloaded. And the outbound share, a live contract with a consumer who
# finds out at cutover, was covered by the inbound share's text.
#
# `refine` exists to give objects of one kind different verdicts. A renderer
# that keeps one per kind throws that work away, silently, in the section a
# reader goes to for the verdict.

def _two_stages():
    return _responses(**{"information_schema.stages": [
        {"STAGE_NAME": "STG_INTERNAL", "STAGE_SCHEMA": "S",
         "STAGE_TYPE": "Internal Named", "STAGE_URL": None},
        {"STAGE_NAME": "STG_EXTERNAL", "STAGE_SCHEMA": "S",
         "STAGE_TYPE": "External Named",
         "STAGE_URL": "s3://bucket/data/"}]})


def test_every_distinct_reason_within_a_kind_is_explained():
    from report.render import render_census
    md = render_census(build_census(FakeSql(_two_stages()), ["DB"]))
    why = md[md.index("## Why each kind cannot move"):]
    assert "unloaded" in why.lower() or "inside snowflake" in why.lower(), (
        "the internal stage's verdict is missing")
    assert "object storage" in why.lower() or "external" in why.lower(), (
        "the external stage's verdict is missing")
    assert why.count("**STAGE**") == 2, why[:600]


def test_an_outbound_share_gets_its_own_explanation_next_to_the_inbound():
    from report.render import render_census
    census = build_census(FakeSql(_responses(**{"show shares": [
        {"name": "SAMPLE", "kind": "INBOUND", "database_name": "SNOWFLAKE"},
        {"name": "TO_PARTNER", "kind": "OUTBOUND", "to": "ACME",
         "database_name": "DB"}]})), ["DB"])
    md = render_census(census)
    why = md[md.index("## Why each kind cannot move"):]
    assert why.count("**SHARE**") == 2, why[:800]
    assert "consumer" in why.lower() and "provider" in why.lower()


def test_one_reason_per_kind_is_still_one_paragraph():
    """Objects that share a reason must not each print it again."""
    from report.render import render_census
    census = build_census(FakeSql(_responses(**{"show tasks": [
        {"name": "T1", "schema_name": "S", "state": "started"},
        {"name": "T2", "schema_name": "S", "state": "suspended"}]})), ["DB"])
    md = render_census(census)
    why = md[md.index("## Why each kind cannot move"):]
    assert why.count("**TASK**") == 1


def test_a_refined_kind_says_which_case_each_paragraph_is():
    """Two STAGE paragraphs are only useful if a reader can tell which
    stage each one is about."""
    from report.render import render_census
    md = render_census(build_census(FakeSql(_two_stages()), ["DB"]))
    why = md[md.index("## Why each kind cannot move"):]
    assert "STG_INTERNAL" in why or "internal named" in why.lower()
    assert "STG_EXTERNAL" in why or "external named" in why.lower()


# ------------- "state=1 day" is not a state
#
# Same run. A dynamic table's detail read `state=1 day` (its target lag)
# and a network rule's read `state=EGRESS` (its mode). Three different
# facts share one label, and two of them are wrong under it.

def test_a_target_lag_is_labelled_as_a_target_lag():
    census = build_census(FakeSql(_responses(**{"show dynamic tables": [
        {"name": "DT", "schema_name": "S", "target_lag": "1 day"}]})), ["DB"])
    detail = census["objects"][0]["detail"]
    assert "target_lag=1 day" in detail, detail
    assert "state=" not in detail


def test_a_mode_is_labelled_as_a_mode():
    census = build_census(FakeSql(_responses(**{"show network rules": [
        {"name": "NR", "schema_name": "S", "mode": "EGRESS"}]})), ["DB"])
    detail = census["objects"][0]["detail"]
    assert "mode=EGRESS" in detail, detail


def test_a_state_is_still_a_state():
    census = build_census(FakeSql(_responses(**{"show tasks": [
        {"name": "T", "schema_name": "S", "state": "started"}]})), ["DB"])
    assert census["objects"][0]["detail"] == "state=started"

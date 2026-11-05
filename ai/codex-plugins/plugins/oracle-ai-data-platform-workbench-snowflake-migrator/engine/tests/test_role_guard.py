"""The role gate (SEC-AIDP-SAMPLES-001): before any discovery, the session's
role is proven read-only on the source, and the check fails CLOSED.

All of it runs against a fake cursor: `run_sql` is injected everywhere.
"""
import json

import pytest

from snowflake_source.conn import AuthError
from snowflake_source.role_guard import (
    RoleNotReadOnly, SHOW_GRANTS_CAP, assert_role_read_only,
    describe_role_grants, read_role_grants)


def _grant(privilege, granted_on, name):
    # The columns SHOW GRANTS TO ROLE returns, as the cursor hands them back.
    return {"created_on": "2026-01-01", "privilege": privilege,
            "granted_on": granted_on, "name": name, "granted_to": "ROLE",
            "grantee_name": "READER", "grant_option": "false",
            "granted_by": "SECURITYADMIN"}


READ_ONLY = [
    _grant("USAGE", "DATABASE", "SALES_DB"),
    _grant("USAGE", "SCHEMA", "SALES_DB.PUBLIC"),
    _grant("SELECT", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    _grant("SELECT", "VIEW", "SALES_DB.PUBLIC.V_ORDERS"),
    _grant("REFERENCES", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    # Resuming the warehouse is not a write on the source.
    _grant("USAGE", "WAREHOUSE", "MIGRATE_WH"),
    _grant("OPERATE", "WAREHOUSE", "MIGRATE_WH"),
    _grant("MODIFY", "WAREHOUSE", "MIGRATE_WH"),
]


class FakeCursor:
    """Answers the statements the guard sends; records every call.

    `by_role` maps a role name to its SHOW GRANTS rows; a database role is
    keyed by its dotted name (`SALES_DB.WRITER`). A role not in `by_role`
    answers `grants`, except PUBLIC, which holds nothing unless `by_role`
    says otherwise (as in most accounts). `future_by_role` is the same for
    SHOW FUTURE GRANTS (a role not in it has none); `fail_future` makes
    that statement fail. `user_roles` is what SHOW GRANTS TO USER lists for
    the session's user SVC -- by default the session role and its secondary
    roles and nothing else, the service user the README asks for (a real
    listing always names every role the user holds, the one it is running
    as included); `user_rows` hands back rows verbatim instead, for the
    shapes a listing should not have; `fail_user_grants` makes that
    statement fail, as a cursor that may not run it would.
    """

    def __init__(self, grants, *, role="READER", secondary="",
                 secondary_value="", by_role=None, fail_grants=None,
                 user_roles=None, user_rows=None, fail_user_grants=False,
                 future_by_role=None, fail_future=None):
        self.grants = grants
        self.role = role
        self.secondary = secondary
        self.secondary_value = secondary_value
        self.by_role = by_role or {}
        self.fail_grants = fail_grants
        if user_roles is None:
            user_roles = [role] + [s.strip() for s in secondary.split(",")
                                   if s.strip() and s.strip() != role]
        self.user_roles = list(user_roles)
        self.user_rows = user_rows
        self.fail_user_grants = fail_user_grants
        self.future_by_role = future_by_role or {}
        self.fail_future = fail_future
        self.calls = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        low = " ".join(sql.split()).lower()
        if "current_role()" in low:
            return [{"R": self.role,
                     "S": json.dumps({"roles": self.secondary,
                                      "value": self.secondary_value})}]
        if low.startswith("select current_user()"):
            return [{"U": "SVC"}]
        if low.startswith("show grants to user"):
            if self.fail_user_grants:
                raise RuntimeError("SQL access control error")
            if self.user_rows is not None:
                return list(self.user_rows)
            return [{"created_on": "x", "role": r, "granted_to": "USER",
                     "grantee_name": "SVC"} for r in self.user_roles]
        if low.startswith("show grants to role") or \
                low.startswith("show grants to database role"):
            if self.fail_grants:
                raise RuntimeError(self.fail_grants)
            name = ".".join(part.strip('"') for part in
                            sql.split("role", 1)[1].strip().split('"."'))
            if name == "PUBLIC" and name not in self.by_role:
                return []
            return self.by_role.get(name, self.grants)
        if low.startswith("show future grants to role") or \
                low.startswith("show future grants to database role"):
            if self.fail_future:
                raise RuntimeError(self.fail_future)
            name = ".".join(part.strip('"') for part in
                            sql.split("role", 1)[1].strip().split('"."'))
            return self.future_by_role.get(name, [])
        raise AssertionError(f"unexpected sql: {sql}")


def _future(privilege, grant_on, name):
    # The columns SHOW FUTURE GRANTS TO ROLE returns: `grant_on`, not
    # `granted_on`, and the object is a placeholder (`DB.S.<TABLE>`).
    return {"created_on": "2026-01-01", "privilege": privilege,
            "grant_on": grant_on, "name": name, "grant_to": "ROLE",
            "grantee_name": "READER", "grant_option": "false"}


def test_a_read_only_role_passes_with_its_grants_as_evidence():
    cur = FakeCursor(READ_ONLY)
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["role"] == "READER"
    assert evidence["grants_read"] == len(READ_ONLY)
    # Warehouse grants are not source objects and are not counted.
    assert evidence["by_privilege"] == {"REFERENCES": 1, "SELECT": 2,
                                        "USAGE": 2}
    assert any(c.lower().startswith("show grants to role") for c in cur.calls)
    line = describe_role_grants(evidence)
    assert "read-only" in line and "SALES_DB" in line and "SELECT x2" in line


@pytest.mark.parametrize("privilege,granted_on,name", [
    ("CREATE TABLE", "SCHEMA", "SALES_DB.PUBLIC"),
    ("CREATE SCHEMA", "DATABASE", "SALES_DB"),
    ("ALTER", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("DROP", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("UPDATE", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("DELETE", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("MERGE", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("TRUNCATE", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("OWNERSHIP", "TABLE", "SALES_DB.PUBLIC.ORDERS"),
    ("OWNERSHIP", "DATABASE", "SALES_DB"),
    ("INSERT", "TABLE", '"SALES_DB"."PUBLIC"."Mixed Case"'),
])
def test_a_write_privilege_on_the_source_refuses_the_session(
        privilege, granted_on, name):
    cur = FakeCursor(READ_ONLY + [_grant(privilege, granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="sales_db")
    message = str(caught.value)
    assert privilege in message and "read-only" in message
    assert "`role:`" in message, "it must say how to fix it"
    # It is an AuthError, so main() prints `error:` rather than a traceback.
    assert isinstance(caught.value, AuthError)


@pytest.mark.parametrize("scope", [
    ["SALES_DB"], ("SALES_DB",), {"SALES_DB"}, ["sales_db"],
    ["SCRATCH", "SALES_DB"], ["SALES_DB", "ARCHIVE"],
])
def test_a_list_of_databases_scopes_the_guard_to_each_of_them(scope):
    """`--database` is repeatable and hands the guard a LIST. Each name in
    it is inside the migration: a write on any of them refuses the
    session. (The list used to be read as the text "['SALES_DB']", which
    no grant names, so every write became a report.)"""
    cur = FakeCursor(READ_ONLY + [_grant("INSERT", "TABLE",
                                         "SALES_DB.PUBLIC.ORDERS")])
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database=scope)
    evidence = read_role_grants(cur, database=scope)
    assert evidence["read_only"] is False
    assert evidence["out_of_scope_writes"] == 0
    assert "SALES_DB" in evidence["databases"]


def test_a_list_scope_still_reports_a_write_on_a_database_it_does_not_name():
    cur = FakeCursor(READ_ONLY + [_grant("INSERT", "TABLE",
                                         "SCRATCH.PUBLIC.T")])
    evidence = assert_role_read_only(cur, database=["SALES_DB", "ARCHIVE"])
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 1
    assert evidence["databases"] == ["ARCHIVE", "SALES_DB"]
    assert "on ARCHIVE, SALES_DB" in describe_role_grants(evidence)


@pytest.mark.parametrize("scope", [[], (), [None], [""], [3], ["A", None],
                                   {"database": "SALES_DB"}, 7, b"SALES_DB"])
def test_a_scope_of_an_unknown_shape_refuses_the_session(scope):
    """Fail closed: a scope the guard cannot read as names would be judged
    against the wrong thing, and every write would pass as a report."""
    with pytest.raises(RoleNotReadOnly, match="scope"):
        assert_role_read_only(FakeCursor(READ_ONLY), database=scope)


def test_the_source_database_is_the_scope_and_other_databases_are_reported():
    """A write grant on ANOTHER database is not a write on the source. It
    is counted and said, never silently dropped, and never fails the gate."""
    cur = FakeCursor(READ_ONLY + [
        _grant("INSERT", "TABLE", "SCRATCH.PUBLIC.T"),
        _grant("OWNERSHIP", "DATABASE", "SCRATCH")])
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 2
    assert "other databases" in describe_role_grants(evidence)


def test_a_quoted_mixed_case_database_is_matched_as_written():
    """`database: '"MyDb"'` names MyDb exactly, and SHOW GRANTS quotes it
    the same way; a fold to upper case on either side would miss the
    write and pass the gate."""
    cur = FakeCursor([_grant("INSERT", "TABLE", '"MyDb"."S"."T"')])
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database='"MyDb"')
    # ...and a different-cased quoted database is a different database.
    other = FakeCursor([_grant("INSERT", "TABLE", '"MYDB"."S"."T"')])
    assert assert_role_read_only(other, database='"MyDb"')["read_only"]


def test_without_a_source_database_every_object_is_in_scope():
    cur = FakeCursor(READ_ONLY + [_grant("INSERT", "TABLE", "SCRATCH.PUBLIC.T")])
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur)


def test_allowlisted_account_and_warehouse_grants_are_not_source_writes():
    """CREATE DATABASE on the ACCOUNT creates something new of the role's
    own; MONITOR USAGE reads history; USAGE/OPERATE on a warehouse resume
    it; USAGE on an integration is the stage or SSO path. None writes the
    source, and none is counted among the source privileges."""
    cur = FakeCursor(READ_ONLY + [
        _grant("CREATE DATABASE", "ACCOUNT", "ACME"),
        _grant("CREATE WAREHOUSE", "ACCOUNT", "ACME"),
        _grant("MONITOR USAGE", "ACCOUNT", "ACME"),
        _grant("MONITOR EXECUTION", "ACCOUNT", "ACME"),
        _grant("IMPORTED PRIVILEGES", "ACCOUNT", "ACME"),
        _grant("MONITOR", "WAREHOUSE", "MIGRATE_WH"),
        _grant("USAGE", "INTEGRATION", "S3_INT"),
        _grant("USAGE", "SHARE", "PARTNER_SHARE"),
        _grant("MONITOR", "RESOURCE_MONITOR", "RM")])
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["by_privilege"] == {"REFERENCES": 1, "SELECT": 2,
                                        "USAGE": 2}


def test_unreadable_grants_fail_closed():
    cur = FakeCursor(READ_ONLY, fail_grants="SQL access control error: "
                                             "Insufficient privileges")
    with pytest.raises(RoleNotReadOnly, match="could not be read"):
        read_role_grants(cur, database="SALES_DB")


def test_a_session_with_no_current_role_fails_closed():
    cur = FakeCursor(READ_ONLY, role=None)
    with pytest.raises(RoleNotReadOnly, match="no current role"):
        read_role_grants(cur)


def test_a_grants_listing_at_the_cap_cannot_prove_anything():
    cur = FakeCursor([_grant("SELECT", "TABLE", f"SALES_DB.S.T{i}")
                      for i in range(SHOW_GRANTS_CAP)])
    with pytest.raises(RoleNotReadOnly, match="cap"):
        read_role_grants(cur, database="SALES_DB")


def test_secondary_roles_are_checked_too():
    """A write held through a secondary role is a write the session can
    make; the guard reads every role the session holds."""
    cur = FakeCursor(READ_ONLY, secondary="WRITER,READER",
                     by_role={"WRITER": [_grant("DELETE", "TABLE",
                                                "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    assert "DELETE" in str(caught.value) and "WRITER" in str(caught.value)
    asked = [c for c in cur.calls if c.lower().startswith("show grants to role")]
    assert len(asked) == 3, "the primary, the one distinct secondary, PUBLIC"
    assert any('"WRITER"' in c for c in asked), "role names are quoted"


def test_the_evidence_carries_no_credential_and_only_grant_facts():
    evidence = read_role_grants(FakeCursor(READ_ONLY), database="SALES_DB")
    assert set(evidence) == {"role", "secondary_roles", "user", "user_roles",
                             "inherited_roles", "database", "databases",
                             "grants_read",
                             "future_grants_read", "by_privilege",
                             "write_grants", "out_of_scope_writes",
                             "read_only"}
    assert evidence["database"] == "SALES_DB"
    assert evidence["databases"] == ["SALES_DB"]


# --- the role hierarchy: a role holds what the roles granted to it hold ----

@pytest.mark.parametrize("granted", ["DATA_OWNER", "ETL_WRITER"])
def test_a_write_inherited_through_a_granted_role_refuses_the_session(granted):
    """`GRANT ROLE DATA_OWNER TO ROLE READER` is one row in READER's
    listing, `USAGE on ROLE DATA_OWNER`; what DATA_OWNER holds is not
    listed there. The guard follows the row and judges DATA_OWNER's grants
    as READER's own. (A system role in that position is refused before it
    is listed: see the system-role tests.)"""
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", granted)],
                     by_role={granted: [
                         _grant("OWNERSHIP", "DATABASE", "SALES_DB"),
                         _grant("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "OWNERSHIP" in message and f"via {granted}" in message
    assert f"inherited role {granted}" in message
    assert any(c.lower().startswith("show grants to role") and
               f'"{granted}"' in c for c in cur.calls), \
        "the granted role's own listing is read"


def test_a_write_held_by_a_database_role_refuses_the_session():
    """A database role is granted the same way (`USAGE on DATABASE_ROLE
    DB.NAME`) and is listed with `SHOW GRANTS TO DATABASE ROLE`."""
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "DATABASE_ROLE",
                                          "SALES_DB.WRITER")],
                     by_role={"SALES_DB.WRITER": [
                         _grant("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "INSERT" in message and "via database role SALES_DB.WRITER" in message
    assert 'show grants to database role "SALES_DB"."WRITER"' in cur.calls, \
        "the dotted name is requoted part by part"


def test_a_granted_role_that_is_itself_read_only_passes_and_is_named():
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", "VIEWER")],
                     by_role={"VIEWER": [
                         _grant("SELECT", "TABLE", "SALES_DB.PUBLIC.ITEMS")]})
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["inherited_roles"] == ["role PUBLIC", "role VIEWER"]
    assert evidence["grants_read"] == len(READ_ONLY) + 2
    assert evidence["by_privilege"]["SELECT"] == 3, "VIEWER's SELECT counts"
    assert "inherited role VIEWER" in describe_role_grants(evidence)


def test_a_write_two_levels_down_names_the_chain_that_carried_it():
    cur = FakeCursor([_grant("USAGE", "ROLE", "MIDDLE")],
                     by_role={"MIDDLE": [_grant("USAGE", "ROLE", "OWNER")],
                              "OWNER": [_grant("DROP", "SCHEMA",
                                               "SALES_DB.PUBLIC")]})
    with pytest.raises(RoleNotReadOnly, match="DROP on SCHEMA SALES_DB.PUBLIC "
                                              "via MIDDLE -> OWNER"):
        assert_role_read_only(cur, database="SALES_DB")


def test_a_cycle_in_the_hierarchy_terminates_and_each_role_is_read_once():
    cur = FakeCursor([_grant("USAGE", "ROLE", "A")],
                     by_role={"A": [_grant("USAGE", "ROLE", "B")],
                              "B": [_grant("USAGE", "ROLE", "READER"),
                                    _grant("USAGE", "ROLE", "A")]})
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["inherited_roles"] == ["role PUBLIC", "role A", "role B"]
    listings = [c for c in cur.calls
                if c.lower().startswith("show grants to role")]
    assert len(listings) == 4, "READER, PUBLIC, A, B: once each"


def test_an_unreadable_inherited_listing_fails_closed():
    """The fail-closed rule applies at every level: a granted role whose
    grants cannot be read may hold anything."""
    inner = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", "ETL")])

    def deny_etl(sql, params=None):
        if '"ETL"' in sql:
            raise RuntimeError("Insufficient privileges")
        return inner(sql, params)
    with pytest.raises(RoleNotReadOnly, match="role ETL .*could not be read"):
        assert_role_read_only(deny_etl, database="SALES_DB")


def test_a_write_row_with_no_object_name_is_judged_inside_the_source():
    """A write that cannot be placed outside the source is not waved through
    as `out of scope`."""
    cur = FakeCursor(READ_ONLY + [{"privilege": "INSERT",
                                   "granted_on": "TABLE", "name": None}])
    with pytest.raises(RoleNotReadOnly, match="INSERT"):
        assert_role_read_only(cur, database="SALES_DB")


# --- the user's roles: every role the credential can USE ROLE into ---------
#
# The threat is a credential that reaches someone else, and that credential
# can `USE ROLE` any role granted to the user, whatever role the session
# started with. So the user's grants are read for EVERY session, not only
# under USE SECONDARY ROLES ALL, and every role they name is walked.

def test_the_user_roles_are_read_for_every_session():
    cur = FakeCursor(READ_ONLY, secondary="", secondary_value="NONE")
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert 'show grants to user "SVC"' in cur.calls
    assert evidence["user"] == "SVC" and evidence["user_roles"] == []


def test_a_write_held_by_another_role_of_the_user_refuses_the_session():
    """User SVC holds READER and WRITER; the session runs as READER with no
    secondary roles. WRITER is still one USE ROLE away for whoever holds
    the credential, so its DELETE is judged as the session's own."""
    cur = FakeCursor(READ_ONLY, secondary="", secondary_value="NONE",
                     user_roles=["READER", "WRITER"],
                     by_role={"WRITER": [_grant("DELETE", "TABLE",
                                                "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "DELETE on TABLE SALES_DB.PUBLIC.ORDERS held by role WRITER" in message
    assert "granted to user SVC: WRITER" in message
    assert 'show grants to role "WRITER"' in cur.calls
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["user_roles"] == ["WRITER"]
    [refused] = evidence["write_grants"]
    assert refused["role"] == "WRITER" and refused["via"] is None


def test_a_system_role_granted_to_the_user_is_refused_unlisted():
    """SVC holds READER and SYSADMIN. The session is READER, but the
    credential can USE ROLE SYSADMIN, and SYSADMIN's reach is Snowflake's
    to define: refused on sight, never listed."""
    cur = FakeCursor(READ_ONLY, secondary="", secondary_value="NONE",
                     user_roles=["READER", "SYSADMIN"])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "role SYSADMIN, granted to user SVC, is a system role" in message
    assert "service user" in message, "it must say what to fix"
    assert not any('"SYSADMIN"' in c for c in cur.calls), "never listed"


def test_unreadable_user_grants_fail_closed():
    """When SHOW GRANTS TO USER cannot be run, the roles the credential
    reaches are unknown, under USE SECONDARY ROLES ALL or not: refused."""
    for value in ("ALL", "NONE"):
        cur = FakeCursor(READ_ONLY, secondary="", secondary_value=value,
                         fail_user_grants=True)
        with pytest.raises(RoleNotReadOnly) as caught:
            assert_role_read_only(cur, database="SALES_DB")
        message = str(caught.value)
        assert "roles granted to the session's user could not be read" in message
        assert "USE ROLE" in message and "service user" in message
        assert any(c.lower().startswith("show grants to user")
                   for c in cur.calls)


def test_secondary_roles_all_with_no_names_is_resolved_from_the_user_grants():
    """`{"roles":"","value":"ALL"}` says every role granted to the user is
    in effect without naming one; the user's grants name them."""
    cur = FakeCursor(READ_ONLY, secondary="", secondary_value="ALL",
                     user_roles=["READER", "WRITER"],
                     by_role={"WRITER": [_grant("DELETE", "TABLE",
                                                "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    assert "DELETE" in str(caught.value) and "WRITER" in str(caught.value)
    assert 'show grants to user "SVC"' in cur.calls
    # ...and a user who holds only the primary role passes.
    alone = FakeCursor(READ_ONLY, secondary="", secondary_value="ALL",
                       user_roles=["READER"])
    evidence = assert_role_read_only(alone, database="SALES_DB")
    assert evidence["read_only"] is True and evidence["secondary_roles"] == []


def test_a_named_secondary_role_is_walked_once_with_the_user_roles():
    cur = FakeCursor(READ_ONLY, secondary="VIEWER", secondary_value="ALL",
                     user_roles=["READER", "VIEWER"], by_role={"VIEWER": []})
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["secondary_roles"] == ["VIEWER"]
    assert evidence["user_roles"] == [], "VIEWER is not listed twice"
    assert sum('"VIEWER"' in c and c.lower().startswith("show grants to role")
               for c in cur.calls) == 1


@pytest.mark.parametrize("rows", [
    [],                                                    # an empty listing
    [{"created_on": "x", "role": None, "granted_to": "USER"}],
    [{"created_on": "x", "role": "", "granted_to": "USER"}],
    [{"created_on": "x", "role": "   ", "granted_to": "USER"}],
    [None],                                                # a None row
    [{"created_on": "x", "granted_to": "USER", "grantee_name": "SVC"}],
    [{"name": "ACCOUNTADMIN", "granted_to": "USER"}],     # role elsewhere
    [{"role": "READER"}, {"role": None}],                 # one good, one not
    [{"role": "READER"}, None],
    [{"role": "READER"}, {"granted_to": "USER"}],
])
def test_a_user_listing_that_names_no_role_fails_closed(rows):
    """SHOW GRANTS TO USER used to be read as `the user holds nothing else`
    whenever its rows named no role: an empty listing, a null or blank
    `role`, a None row, a row without the column, the role under another
    column. None of those is a listing the check can read -- a real one
    names at least the role the session runs as -- so the roles the
    credential can USE ROLE into are unknown, and the session is refused
    before any role is listed."""
    cur = FakeCursor(READ_ONLY, user_rows=rows)
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "user SVC" in message and "not provably read-only" in message
    assert "USE ROLE" in message and "service user" in message, \
        "it must say what to fix"
    assert 'show grants to user "SVC"' in cur.calls
    assert not any(c.lower().startswith("show grants to role")
                   for c in cur.calls), "refused before any role is walked"
    assert isinstance(caught.value, AuthError)


def test_a_user_listing_that_omits_a_role_the_session_holds_fails_closed():
    """The user holds the role it is running as and every secondary role in
    effect; a listing without them is cut or filtered and proves nothing
    about the other roles it may hold."""
    cur = FakeCursor(READ_ONLY, user_rows=[{"role": "OTHER"}],
                     by_role={"OTHER": []})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "do not name READER" in message and "cut or filtered" in message
    assert not any(c.lower().startswith("show grants to role")
                   for c in cur.calls), "OTHER is not walked on its own"
    # ...a secondary role in effect must be named too...
    cur = FakeCursor(READ_ONLY, secondary="VIEWER", user_roles=["READER"],
                     by_role={"VIEWER": []})
    with pytest.raises(RoleNotReadOnly, match="do not name VIEWER"):
        assert_role_read_only(cur, database="SALES_DB")
    # ...and the listing that names them passes as before.
    cur = FakeCursor(READ_ONLY, secondary="VIEWER", by_role={"VIEWER": []})
    assert cur.user_roles == ["READER", "VIEWER"]
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True and evidence["user_roles"] == []


def test_a_session_running_as_public_is_not_looked_for_in_the_user_listing():
    """No user listing names PUBLIC, so a PUBLIC session with an empty
    listing is consistent; PUBLIC is walked for every session anyway."""
    cur = FakeCursor([], role="PUBLIC", user_rows=[])
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True and evidence["user_roles"] == []
    assert 'show grants to role "PUBLIC"' in cur.calls
    # ...and a listing that names a system role next to nothing else is
    # still read, and refused for the system role.
    cur = FakeCursor([], role="PUBLIC", user_rows=[{"role": "SYSADMIN"}])
    with pytest.raises(RoleNotReadOnly, match="SYSADMIN.*is a system role"):
        assert_role_read_only(cur, database="SALES_DB")


def test_an_unreadable_secondary_roles_payload_fails_closed():
    inner = FakeCursor(READ_ONLY)

    def raw(sql, params=None):
        if "current_role()" in sql.lower():
            return [{"R": "READER", "S": "not json {"}]
        return inner(sql, params)
    with pytest.raises(RoleNotReadOnly, match="cannot read"):
        assert_role_read_only(raw, database="SALES_DB")
    # ...and a payload that parses but is not an object is the same.
    def listed(sql, params=None):
        if "current_role()" in sql.lower():
            return [{"R": "READER", "S": '["WRITER"]'}]
        return inner(sql, params)
    with pytest.raises(RoleNotReadOnly, match="cannot read"):
        assert_role_read_only(listed, database="SALES_DB")


@pytest.mark.parametrize("row", [
    {"R": "READER"},                # no S column at all
    {"R": "READER", "S": ""},       # an empty one
    {"R": "READER", "S": None},     # a null one
])
def test_a_missing_or_empty_secondary_roles_value_fails_closed(row):
    """`if raw:` used to read these as `no secondary roles`. The roles in
    effect are unknown, which the docstring promises to refuse."""
    inner = FakeCursor(READ_ONLY)

    def run_sql(sql, params=None):
        if "current_role()" in sql.lower():
            return [row]
        return inner(sql, params)
    with pytest.raises(RoleNotReadOnly, match="returned nothing"):
        assert_role_read_only(run_sql, database="SALES_DB")
    assert not any(c.lower().startswith("show grants") for c in inner.calls)


# --- grants that are not ON source data but lead to a source write ---------
#
# The reviewer's case: MANAGE GRANTS on the ACCOUNT lets a role grant itself
# any privilege, INSERT on every table included, so a role that holds it is
# not read-only whatever its table grants say. A grant on a non-source class
# is judged against that class's allowlist; everything else is refused with
# the escalation named.

def test_manage_grants_on_the_account_refuses_the_session():
    cur = FakeCursor(READ_ONLY + [_grant("MANAGE GRANTS", "ACCOUNT", "ACME")])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "MANAGE GRANTS on ACCOUNT ACME" in message
    assert "grant itself any privilege" in message, "it must name the escalation"
    assert "`role:`" in message, "it must say how to fix it"
    assert isinstance(caught.value, AuthError)


def test_manage_grants_inherited_through_a_granted_role_names_the_carrier():
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", "GRANTS_MGR")],
                     by_role={"GRANTS_MGR": [
                         _grant("MANAGE GRANTS", "ACCOUNT", "ACME")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "MANAGE GRANTS on ACCOUNT ACME via GRANTS_MGR" in message
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["read_only"] is False
    [refused] = evidence["write_grants"]
    assert refused["privilege"] == "MANAGE GRANTS"
    assert refused["granted_on"] == "ACCOUNT" and refused["name"] == "ACME"
    assert refused["via"] == "GRANTS_MGR" and refused["role"] == "READER"
    assert refused["future"] is False
    assert "grant itself any privilege" in refused["reason"]


@pytest.mark.parametrize("privilege,granted_on,name", [
    ("OWNERSHIP", "ROLE", "ANALYST"),
    ("OWNERSHIP", "USER", "SVC_OTHER"),
    ("OWNERSHIP", "ACCOUNT", "ACME"),
    ("OWNERSHIP", "WAREHOUSE", "MIGRATE_WH"),
    ("OWNERSHIP", "INTEGRATION", "S3_INT"),
    ("OWNERSHIP", "DATABASE_ROLE", "SALES_DB.VIEWER"),
    ("CREATE ROLE", "ACCOUNT", "ACME"),
    ("CREATE USER", "ACCOUNT", "ACME"),
    ("CREATE INTEGRATION", "ACCOUNT", "ACME"),
    ("CREATE SHARE", "ACCOUNT", "ACME"),
    ("APPLY MASKING POLICY", "ACCOUNT", "ACME"),
    ("APPLY ROW ACCESS POLICY", "ACCOUNT", "ACME"),
    ("APPLY AGGREGATION POLICY", "ACCOUNT", "ACME"),
    ("APPLY TAG", "ACCOUNT", "ACME"),
    ("APPLY AUTHENTICATION POLICY", "ACCOUNT", "ACME"),
    ("EXECUTE TASK", "ACCOUNT", "ACME"),
    ("EXECUTE MANAGED TASK", "ACCOUNT", "ACME"),
    ("MANAGE WAREHOUSES", "ACCOUNT", "ACME"),
])
def test_an_escalating_privilege_on_a_non_source_class_refuses_the_session(
        privilege, granted_on, name):
    """None of these is ON a table, and each is a step from `cannot write`
    to `can`: owning a role or user, minting roles or users, running tasks,
    rewriting policies, opening a share or an integration."""
    cur = FakeCursor(READ_ONLY + [_grant(privilege, granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert f"{privilege} on {granted_on} {name}" in message
    assert "(" in message.split(f"{privilege} on {granted_on} {name}", 1)[1], (
        "the refusal carries a reason")
    assert "read-only" in message and "`role:`" in message


def test_an_unknown_privilege_name_fails_closed_on_any_class():
    """A privilege the check does not know might be harmless. It cannot
    say, so it does not pass it: the burden of proof is on the grant."""
    on_account = FakeCursor(READ_ONLY + [
        _grant("MANAGE ACCOUNT SUPPORT CASES", "ACCOUNT", "ACME")])
    with pytest.raises(RoleNotReadOnly, match="MANAGE ACCOUNT SUPPORT CASES "
                                              "on ACCOUNT ACME .*unproven"):
        assert_role_read_only(on_account, database="SALES_DB")
    on_table = FakeCursor(READ_ONLY + [
        _grant("FROBNICATE", "TABLE", "SALES_DB.PUBLIC.ORDERS")])
    with pytest.raises(RoleNotReadOnly, match="FROBNICATE on TABLE .*unproven"):
        assert_role_read_only(on_table, database="SALES_DB")
    on_warehouse = FakeCursor(READ_ONLY + [
        _grant("APPLYBUDGET", "WAREHOUSE", "MIGRATE_WH")])
    with pytest.raises(RoleNotReadOnly, match="APPLYBUDGET on WAREHOUSE"):
        assert_role_read_only(on_warehouse, database="SALES_DB")


def test_operate_is_a_warehouse_read_and_a_task_write():
    """The same privilege word means different things per class: OPERATE
    on a warehouse resumes it, OPERATE on a task runs whatever it writes."""
    assert assert_role_read_only(
        FakeCursor(READ_ONLY), database="SALES_DB")["read_only"]
    cur = FakeCursor(READ_ONLY + [_grant("OPERATE", "TASK",
                                          "SALES_DB.PUBLIC.LOAD_ORDERS")])
    with pytest.raises(RoleNotReadOnly, match="OPERATE on TASK"):
        assert_role_read_only(cur, database="SALES_DB")


def test_a_usage_grant_on_an_application_role_is_refused_unwalked():
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "APPLICATION_ROLE",
                                          "SOME_APP.APP_ADMIN")])
    with pytest.raises(RoleNotReadOnly, match="APPLICATION_ROLE .*not walked"):
        assert_role_read_only(cur, database="SALES_DB")


# --- system roles: refused before they are listed --------------------------

SYSTEM = ["ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN",
          "ORGADMIN", "GLOBALORGADMIN"]


@pytest.mark.parametrize("granted", SYSTEM)
def test_a_system_role_in_the_hierarchy_is_refused_even_unlisted(granted):
    """`GRANT ROLE SECURITYADMIN TO ROLE READER` makes READER able to do
    what SECURITYADMIN can, and that is defined by Snowflake, not by the
    rows SHOW GRANTS returns for it -- which may not be readable at all.
    The role is refused on sight, and its listing is never asked for."""
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", granted)],
                     fail_grants=None,
                     by_role={granted: []})  # an EMPTY listing, if asked

    def deny_system(sql, params=None):
        if f'"{granted}"' in sql:
            raise RuntimeError("Insufficient privileges")
        return cur(sql, params)
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(deny_system, database="SALES_DB")
    message = str(caught.value)
    assert (f"role {granted}, granted to role READER via {granted}, is a "
            f"system role") in message
    assert "read-only" in message and "`role:`" in message
    assert not any(f'"{granted}"' in c for c in cur.calls), "never listed"


def test_a_system_role_two_levels_down_names_the_chain():
    cur = FakeCursor([_grant("USAGE", "ROLE", "MIDDLE")],
                     by_role={"MIDDLE": [_grant("USAGE", "ROLE", "SYSADMIN")],
                              "SYSADMIN": []})
    with pytest.raises(RoleNotReadOnly, match="role SYSADMIN, granted to role "
                                              "READER via MIDDLE -> SYSADMIN, "
                                              "is a system role"):
        assert_role_read_only(cur, database="SALES_DB")


def test_a_session_whose_own_role_is_a_system_role_is_refused_unlisted():
    cur = FakeCursor([], role="ACCOUNTADMIN")
    with pytest.raises(RoleNotReadOnly, match="session's role ACCOUNTADMIN is "
                                              "a system role"):
        assert_role_read_only(cur, database="SALES_DB")
    assert not any(c.lower().startswith("show") for c in cur.calls)
    # ...and a system role among the secondaries is refused the same way.
    cur = FakeCursor(READ_ONLY, secondary="SECURITYADMIN")
    with pytest.raises(RoleNotReadOnly, match="secondary role SECURITYADMIN "
                                              "is a system role"):
        assert_role_read_only(cur, database="SALES_DB")


def test_a_system_role_name_is_matched_against_the_stored_name():
    """SHOW GRANTS prints a role's STORED name unquoted, and a stored name
    is its case: `GRANT ROLE SYSADMIN` lists `SYSADMIN`, while a role
    created as `"sysadmin"` lists `sysadmin` and is a different role. So
    the inherited lower-case role is not the system role: it is walked
    and judged by what it holds (it used to be refused unwalked)."""
    clean = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", "sysadmin")],
                       by_role={"sysadmin": []})
    evidence = assert_role_read_only(clean, database="SALES_DB")
    assert evidence["read_only"] is True
    assert "role sysadmin" in evidence["inherited_roles"]
    assert 'show grants to role "sysadmin"' in clean.calls, "walked"
    dirty = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", "sysadmin")],
                       by_role={"sysadmin": [
                           _grant("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly, match="INSERT on TABLE .*via sysadmin"):
        assert_role_read_only(dirty, database="SALES_DB")
    # ...while the stored upper-case name is the system role, quoted or not.
    for listed in ("SYSADMIN", '"SYSADMIN"', " SYSADMIN "):
        cur = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", listed)],
                         by_role={"SYSADMIN": []})
        with pytest.raises(RoleNotReadOnly, match="is a system role"):
            assert_role_read_only(cur, database="SALES_DB")
        assert not any('"SYSADMIN"' in c for c in cur.calls), "never listed"


# --- future grants: a write on every object created later -------------------

def test_a_future_grant_of_a_write_refuses_the_session():
    """`GRANT INSERT ON FUTURE TABLES IN SCHEMA ... TO ROLE READER` is not
    in SHOW GRANTS TO ROLE at all; it is in SHOW FUTURE GRANTS TO ROLE, as
    `INSERT on TABLE SALES_DB.PUBLIC.<TABLE>`. It is a write the session
    can make on every table created there from now on."""
    cur = FakeCursor(READ_ONLY, future_by_role={"READER": [
        _future("SELECT", "TABLE", "SALES_DB.PUBLIC.<TABLE>"),
        _future("INSERT", "TABLE", "SALES_DB.PUBLIC.<TABLE>")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "future INSERT on TABLE SALES_DB.PUBLIC.<TABLE>" in message
    assert 'show future grants to role "READER"' in cur.calls
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["future_grants_read"] == 2
    assert evidence["grants_read"] == len(READ_ONLY)
    assert evidence["by_privilege"]["SELECT (future)"] == 1
    assert evidence["by_privilege"]["INSERT (future)"] == 1
    [refused] = evidence["write_grants"]
    assert refused["future"] is True and refused["privilege"] == "INSERT"


def test_future_grants_are_read_for_every_walked_role():
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "ROLE", "VIEWER"),
                                  _grant("USAGE", "DATABASE_ROLE",
                                         "SALES_DB.READER_DBR")],
                     by_role={"VIEWER": [], "SALES_DB.READER_DBR": []},
                     future_by_role={"SALES_DB.READER_DBR": [
                         _future("DELETE", "TABLE", "SALES_DB.<TABLE>")]})
    with pytest.raises(RoleNotReadOnly, match="future DELETE on TABLE "
                                              "SALES_DB.<TABLE> via database "
                                              "role SALES_DB.READER_DBR"):
        assert_role_read_only(cur, database="SALES_DB")
    asked = [c for c in cur.calls if c.lower().startswith("show future grants")]
    assert 'show future grants to role "READER"' in asked
    assert 'show future grants to role "VIEWER"' in asked
    assert 'show future grants to database role "SALES_DB"."READER_DBR"' in asked
    # ...and a role whose future grants are all reads passes, with them
    # counted in the evidence.
    clean = FakeCursor(READ_ONLY, future_by_role={"READER": [
        _future("SELECT", "VIEW", "SALES_DB.PUBLIC.<VIEW>")]})
    evidence = assert_role_read_only(clean, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["future_grants_read"] == 1
    assert "1 future grant(s)" in describe_role_grants(evidence)


def test_a_future_write_on_another_database_is_out_of_scope_and_reported():
    cur = FakeCursor(READ_ONLY, future_by_role={"READER": [
        _future("INSERT", "TABLE", "SCRATCH.PUBLIC.<TABLE>")]})
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 1


def test_unreadable_future_grants_fail_closed():
    cur = FakeCursor(READ_ONLY, fail_future="SQL access control error")
    with pytest.raises(RoleNotReadOnly, match="future grants of role READER "
                                              "could not be read"):
        assert_role_read_only(cur, database="SALES_DB")


def test_a_future_grants_listing_at_the_cap_cannot_prove_anything():
    cur = FakeCursor(READ_ONLY, future_by_role={"READER": [
        _future("SELECT", "TABLE", f"SALES_DB.S{i}.<TABLE>")
        for i in range(SHOW_GRANTS_CAP)]})
    with pytest.raises(RoleNotReadOnly, match="future grants, at or over"):
        assert_role_read_only(cur, database="SALES_DB")


# --- PUBLIC: held by every role, named by no listing -----------------------

def test_a_write_granted_to_public_refuses_the_session():
    """`GRANT INSERT ON TABLE ... TO ROLE PUBLIC` reaches every role in the
    account, and SHOW GRANTS TO ROLE READER never says `USAGE on ROLE
    PUBLIC`, so the walk cannot find it: PUBLIC is listed on its own."""
    cur = FakeCursor(READ_ONLY, by_role={"PUBLIC": [
        _grant("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert "INSERT on TABLE SALES_DB.PUBLIC.ORDERS held by role PUBLIC" in message
    assert "nothing to PUBLIC" in message, "it must say what to fix"
    assert 'show grants to role "PUBLIC"' in cur.calls
    assert 'show future grants to role "PUBLIC"' in cur.calls
    evidence = read_role_grants(cur, database="SALES_DB")
    assert "role PUBLIC" in evidence["inherited_roles"]
    [refused] = evidence["write_grants"]
    assert refused["role"] == "PUBLIC" and refused["via"] is None
    # ...and a future write granted to PUBLIC is the same.
    future = FakeCursor(READ_ONLY, future_by_role={"PUBLIC": [
        _future("INSERT", "TABLE", "SALES_DB.PUBLIC.<TABLE>")]})
    with pytest.raises(RoleNotReadOnly, match="future INSERT .*held by role "
                                              "PUBLIC"):
        assert_role_read_only(future, database="SALES_DB")


def test_public_is_walked_for_every_session_and_reported():
    cur = FakeCursor(READ_ONLY)
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert 'show grants to role "PUBLIC"' in cur.calls
    assert evidence["inherited_roles"] == ["role PUBLIC"]
    assert "inherited role PUBLIC" in describe_role_grants(evidence)
    # A role PUBLIC holds through its own hierarchy is walked too.
    deeper = FakeCursor(READ_ONLY,
                        by_role={"PUBLIC": [_grant("USAGE", "ROLE", "EVERYONE")],
                                 "EVERYONE": [_grant("DROP", "SCHEMA",
                                                     "SALES_DB.PUBLIC")]})
    with pytest.raises(RoleNotReadOnly, match="DROP on SCHEMA SALES_DB.PUBLIC "
                                              "via EVERYONE held by role PUBLIC"):
        assert_role_read_only(deeper, database="SALES_DB")


def test_an_unreadable_public_listing_fails_closed():
    inner = FakeCursor(READ_ONLY)

    def deny_public(sql, params=None):
        if '"PUBLIC"' in sql:
            raise RuntimeError("Insufficient privileges")
        return inner(sql, params)
    with pytest.raises(RoleNotReadOnly, match="role PUBLIC .*could not be read"):
        assert_role_read_only(deny_public, database="SALES_DB")


# --- the database: scope excuses only what it can PLACE elsewhere ----------
#
# With `database:` set, a write on a table of another database is reported,
# not refused. That carve-out used to apply to every class the module did
# not list, so MANAGE GRANTS on a padded ` ACCOUNT`, on the ORGANIZATION or
# on a class Snowflake adds tomorrow read as "another database" and passed.
# Only a database-contained source class with a database part in its name
# can be placed; everything else is judged inside the scope.

@pytest.mark.parametrize("privilege,granted_on,name", [
    ("MANAGE GRANTS", " ACCOUNT", "AB12345"),        # padded class
    ("MANAGE GRANTS", "ACCOUNT ", "AB12345"),
    ("MANAGE GRANTS", "ORGANIZATION", "ORG1"),
    ("OWNERSHIP", "LISTING", "MY_LISTING"),
    ("OWNERSHIP", "AUTHENTICATION_POLICY", "AP"),
    ("FROBNICATE", "NEW_THING", "OTHER_DB.PUBLIC.X"),
    ("OWNERSHIP", "NEW_THING", "OTHER_DB.PUBLIC.X"),
    (" manage  grants ", "account", "AB12345"),     # padded privilege
])
def test_a_refused_privilege_off_the_source_classes_is_refused_in_any_scope(
        privilege, granted_on, name):
    cur = FakeCursor(READ_ONLY + [_grant(privilege, granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    shown = " ".join(privilege.split()).upper()
    assert f"{shown} on {granted_on.strip().upper()} {name}" in message
    assert "other databases" not in message
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["out_of_scope_writes"] == 0
    assert evidence["read_only"] is False


@pytest.mark.parametrize("granted_on", [None, "", "   "])
def test_a_row_with_no_object_class_is_refused_whatever_its_privilege(
        granted_on):
    """A null or blank `granted_on` cannot be placed or judged. It used to
    fall through the scope filter (MANAGE GRANTS passed) or be read as a
    source read (USAGE naming SECURITYADMIN passed, unwalked)."""
    for privilege, name in [("MANAGE GRANTS", "AB12345"),
                            ("USAGE", "SECURITYADMIN"),
                            ("SELECT", "SALES_DB.PUBLIC.ORDERS")]:
        cur = FakeCursor(READ_ONLY + [{"privilege": privilege,
                                       "granted_on": granted_on,
                                       "name": name}])
        with pytest.raises(RoleNotReadOnly) as caught:
            assert_role_read_only(cur, database="SALES_DB")
        message = str(caught.value)
        assert f"{privilege} on ? {name}" in message
        assert "no object class" in message
        with pytest.raises(RoleNotReadOnly, match="no object class"):
            assert_role_read_only(cur)


def test_a_write_on_a_source_object_with_no_database_part_is_judged_inside():
    """`INSERT on TABLE ORDERS` names no database, so it cannot be shown to
    be on another one; a schema needs two parts, a database one."""
    for granted_on, name in [("TABLE", "ORDERS"), ("SCHEMA", "PUBLIC"),
                             ("TABLE", '"ORDERS"'), ("TABLE", ".X.Y")]:
        cur = FakeCursor(READ_ONLY + [_grant("INSERT", granted_on, name)])
        with pytest.raises(RoleNotReadOnly, match="INSERT"):
            assert_role_read_only(cur, database="SALES_DB")
    # ...while a placed write on another database is still only reported.
    for granted_on, name in [("TABLE", "SCRATCH.PUBLIC.T"),
                             ("SCHEMA", "SCRATCH.PUBLIC"),
                             ("DATABASE", "SCRATCH"),
                             ("EVENT_TABLE", "SCRATCH.PUBLIC.EVENTS"),
                             ("FUNCTION", "SCRATCH.PUBLIC.F(VARCHAR)")]:
        cur = FakeCursor(READ_ONLY + [_grant("OWNERSHIP", granted_on, name)])
        evidence = assert_role_read_only(cur, database="SALES_DB")
        assert evidence["read_only"] is True
        assert evidence["out_of_scope_writes"] == 1


@pytest.mark.parametrize("privilege,granted_on,name,reason", [
    ("MANAGE GRANTS", "DATABASE", "OTHER_DB", "grant itself any privilege"),
    ("MANAGE GRANTS", "SCHEMA", "OTHER_DB.S", "grant itself any privilege"),
    ("MANAGE GRANTS", "TABLE", "OTHER_DB.S.T", "grant itself any privilege"),
    ("CREATE ROLE", "SCHEMA", "OTHER_DB.S", "create roles it owns"),
    ("CREATE USER", "DATABASE", "OTHER_DB", "sign in as it"),
    ("EXECUTE TASK", "DATABASE", "OTHER_DB", "run tasks"),
    ("EXECUTE MANAGED TASK", "SCHEMA", "OTHER_DB.S", "serverless tasks"),
    ("APPLY MASKING POLICY", "DATABASE", "OTHER_DB", "source columns show"),
    ("APPLY TAG", "SCHEMA", "OTHER_DB.S", "drive source policies"),
    ("CREATE SHARE", "DATABASE", "OTHER_DB", "share the source"),
    ("CREATE INTEGRATION", "DATABASE", "OTHER_DB", "data to leave"),
    ("MANAGE WAREHOUSES", "DATABASE", "OTHER_DB", "every warehouse"),
    # The spelling class: any MANAGE / APPLY / EXECUTE privilege, known to
    # the check or not, reaches past the object it is reported on.
    ("MANAGE LISTING AUTO FULFILLMENT", "DATABASE", "OTHER_DB", "unproven"),
    ("APPLY PROJECTION POLICY", "SCHEMA", "OTHER_DB.S", "unproven"),
    ("EXECUTE ALERT", "DATABASE", "OTHER_DB", "unproven"),
    (" manage  grants ", "database", "OTHER_DB", "grant itself any privilege"),
])
def test_an_escalation_reported_on_another_database_is_refused_in_any_scope(
        privilege, granted_on, name, reason):
    """Snowflake grants these ON the ACCOUNT, but a row that reports one ON a
    database, schema or table of another database used to be excused by
    the `database:` scope as `a write on another database`. MANAGE GRANTS
    reaches every grant in the account whatever row carries it: its reach
    is not bounded by the object, so the scope cannot place it elsewhere."""
    cur = FakeCursor(READ_ONLY + [_grant(privilege, granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    shown = " ".join(privilege.split()).upper()
    assert f"{shown} on {granted_on.upper()} {name}" in message
    assert reason in message, "the refusal names what the privilege reaches"
    assert "other databases" not in message
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["read_only"] is False
    assert evidence["out_of_scope_writes"] == 0
    # ...with or without a scope, and on the source database itself.
    with pytest.raises(RoleNotReadOnly, match=shown):
        assert_role_read_only(cur)
    on_source = FakeCursor(READ_ONLY + [_grant(privilege, granted_on,
                                               name.replace("OTHER_DB",
                                                            "SALES_DB"))])
    with pytest.raises(RoleNotReadOnly, match=shown):
        assert_role_read_only(on_source, database="SALES_DB")


def test_a_write_bounded_by_its_object_is_still_excused_on_another_database():
    """The contrast: INSERT or OWNERSHIP on a table of another database
    reaches that table and nothing more, so the scope still only reports
    it. OWNERSHIP is a write on the object, not an escalation, on a source
    class -- and an escalation on a role, user or the account."""
    cur = FakeCursor(READ_ONLY + [_grant("OWNERSHIP", "DATABASE", "OTHER_DB"),
                                  _grant("OWNERSHIP", "TABLE", "OTHER_DB.S.T"),
                                  _grant("INSERT", "TABLE", "OTHER_DB.S.T"),
                                  _grant("CREATE TABLE", "SCHEMA", "OTHER_DB.S")])
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 4
    for granted_on, name in [("ROLE", "ANALYST"), ("USER", "SVC_OTHER"),
                             ("ACCOUNT", "ACME")]:
        cur = FakeCursor(READ_ONLY + [_grant("OWNERSHIP", granted_on, name)])
        with pytest.raises(RoleNotReadOnly, match=f"OWNERSHIP on {granted_on} "
                                                  f"{name} .*grant itself "
                                                  f"anything the object"):
            assert_role_read_only(cur, database="SALES_DB")


# --- governance objects: judged wherever they live --------------------------

@pytest.mark.parametrize("privilege,granted_on,name", [
    ("OWNERSHIP", "MASKING_POLICY", "OTHER_DB.GOV.MASK"),
    ("OWNERSHIP", "TAG", "OTHER_DB.GOV.PII"),
    ("OWNERSHIP", "ROW_ACCESS_POLICY", "OTHER_DB.GOV.RAP"),
    ("OWNERSHIP", "AGGREGATION_POLICY", "OTHER_DB.GOV.AGG"),
    ("OWNERSHIP", "PROJECTION_POLICY", "OTHER_DB.GOV.PP"),
    ("APPLY", "MASKING_POLICY", "OTHER_DB.GOV.MASK"),
    ("APPLY", "TAG", "OTHER_DB.GOV.PII"),
    ("OWNERSHIP", "NETWORK_RULE", "OTHER_DB.NET.EGRESS"),
    ("OWNERSHIP", "MASKING_POLICY", "SALES_DB.GOV.MASK"),
])
def test_owning_or_applying_a_policy_or_tag_in_any_database_is_a_write(
        privilege, granted_on, name):
    """A masking policy in OTHER_DB can be applied to SALES_DB columns, and
    its owner can change what those columns show. It is not source data of
    OTHER_DB; it governs the source, so the scope does not excuse it."""
    cur = FakeCursor(READ_ONLY + [_grant(privilege, granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert f"{privilege} on {granted_on} {name}" in message
    assert "whichever database" in message, "the reason names the reach"
    assert read_role_grants(cur, database="SALES_DB")["out_of_scope_writes"] == 0


def test_reading_a_tag_passes():
    """READ on a tag reads its values; it changes nothing."""
    cur = FakeCursor(READ_ONLY + [_grant("READ", "TAG", "OTHER_DB.GOV.PII")])
    assert assert_role_read_only(cur, database="SALES_DB")["read_only"]


# --- role classes: walked exactly, or refused --------------------------------

@pytest.mark.parametrize("granted_on", [" ROLE", "ROLE ", "role", " Role "])
def test_a_padded_role_class_is_still_an_inheritance_hop(granted_on):
    """`USAGE on ' ROLE' SECURITYADMIN` used to read as a plain USAGE on an
    unknown class and pass; the class is read with its padding removed,
    so the system role is refused and a custom role is walked."""
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", granted_on, "SECURITYADMIN")],
                     by_role={"SECURITYADMIN": []})
    with pytest.raises(RoleNotReadOnly, match="SECURITYADMIN.*is a system role"):
        assert_role_read_only(cur, database="SALES_DB")
    helper = FakeCursor(READ_ONLY + [_grant("USAGE", granted_on, "HELPER")],
                        by_role={"HELPER": [_grant("MANAGE GRANTS", "ACCOUNT",
                                                   "ACME")]})
    with pytest.raises(RoleNotReadOnly, match="MANAGE GRANTS on ACCOUNT ACME "
                                              "via HELPER"):
        assert_role_read_only(helper, database="SALES_DB")
    assert 'show grants to role "HELPER"' in helper.calls


def test_a_database_role_class_spelled_with_a_space_is_walked():
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", "DATABASE ROLE", "SALES_DB.DR")],
                     by_role={"SALES_DB.DR": [_grant("INSERT", "TABLE",
                                                     "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly, match="INSERT .*via database role "
                                              "SALES_DB.DR"):
        assert_role_read_only(cur, database="SALES_DB")
    assert 'show grants to database role "SALES_DB"."DR"' in cur.calls


@pytest.mark.parametrize("granted_on,name", [
    ("INSTANCE_ROLE", "SNOWFLAKE.LOCAL.BUDGET!ADMIN"),
    ("APPLICATION_ROLE", "SOME_APP.APP_ADMIN"),
    ("SERVICE_ROLE", "SALES_DB.PUBLIC.SVC!ALL_ENDPOINTS_USAGE"),
])
def test_usage_of_a_role_class_the_walk_does_not_follow_is_refused(
        granted_on, name):
    cur = FakeCursor(READ_ONLY + [_grant("USAGE", granted_on, name)])
    with pytest.raises(RoleNotReadOnly, match=f"USAGE on {granted_on} .*not "
                                              f"walked"):
        assert_role_read_only(cur, database="SALES_DB")
    # ...with or without a scope, and never counted as another database.
    with pytest.raises(RoleNotReadOnly, match="not walked"):
        assert_role_read_only(cur)


# --- rows the cursor hands back in a shape the check cannot read -----------

def test_a_grants_row_that_is_not_a_mapping_refuses_rather_than_crashes():
    cur = FakeCursor(READ_ONLY + [("2026", "INSERT", "TABLE",
                                   "SALES_DB.PUBLIC.ORDERS")])
    with pytest.raises(RoleNotReadOnly, match="cannot read") as caught:
        assert_role_read_only(cur, database="SALES_DB")
    assert isinstance(caught.value, AuthError)


# --- the legitimate role, in full, still passes -----------------------------

LEGITIMATE = READ_ONLY + [
    _grant("READ", "STAGE", "SALES_DB.PUBLIC.LANDING"),
    _grant("USAGE", "FILE_FORMAT", "SALES_DB.PUBLIC.CSV"),
    _grant("USAGE", "FUNCTION", "SALES_DB.PUBLIC.F(VARCHAR)"),
    _grant("USAGE", "PROCEDURE", "SALES_DB.PUBLIC.LOAD_ORDERS(VARCHAR)"),
    _grant("USAGE", "SEQUENCE", "SALES_DB.PUBLIC.SEQ"),
    _grant("SELECT", "MATERIALIZED_VIEW", "SALES_DB.PUBLIC.MV"),
    _grant("SELECT", "ICEBERG_TABLE", "OTHER_DB.PUBLIC.ICE"),
    _grant("MONITOR", "WAREHOUSE", "MIGRATE_WH"),
    _grant("USAGE", "INTEGRATION", "S3_INT"),
    _grant("MONITOR USAGE", "ACCOUNT", "ACME"),
    _grant("CREATE DATABASE", "ACCOUNT", "ACME"),
    _grant("IMPORTED PRIVILEGES", "DATABASE", "SNOWFLAKE"),
    _grant("MONITOR", "USER", "SVC"),
]


@pytest.mark.parametrize("database", ["SALES_DB", "sales_db", '"SALES_DB"', None])
def test_the_legitimate_migration_role_passes_with_or_without_a_scope(database):
    """Every grant the README asks the account owner to make, plus MONITOR
    on the service user (its own query history): read-only, together."""
    cur = FakeCursor(LEGITIMATE)
    evidence = assert_role_read_only(cur, database=database)
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 0
    assert evidence["grants_read"] == len(LEGITIMATE)


def test_monitor_on_a_user_passes_and_anything_else_on_one_does_not():
    """MONITOR on USER reads that user's query history and nothing else."""
    assert assert_role_read_only(
        FakeCursor(READ_ONLY + [_grant("MONITOR", "USER", "SVC")]),
        database="SALES_DB")["read_only"]
    cur = FakeCursor(READ_ONLY + [_grant("APPLYBUDGET", "USER", "SVC")])
    with pytest.raises(RoleNotReadOnly, match="APPLYBUDGET on USER SVC"):
        assert_role_read_only(cur, database="SALES_DB")


# --- owner's-rights objects: the run privilege reaches past the object ----

@pytest.mark.parametrize("granted_on,name", [
    ("TASK", "OTHER_DB.S.T"),
    ("PIPE", "OTHER_DB.S.P"),
    ("ALERT", "OTHER_DB.S.A"),
    ("DYNAMIC_TABLE", "OTHER_DB.S.DT"),
    ("DYNAMIC TABLE", "OTHER_DB.S.DT"),             # class with a space
    ("SERVICE", "OTHER_DB.S.SVC"),
    ("task", "OTHER_DB.S.T"),                       # folded class
    ("TASK", '"other_db"."s"."t"'),                 # quoted, other case
])
def test_operate_on_a_task_in_another_database_is_refused(granted_on, name):
    """A task runs with its OWNER's privileges, and OPERATE is what runs it
    (EXECUTE TASK, ALTER TASK ... RESUME); the same for a pipe's REFRESH or
    resume, an alert's, a dynamic table's or a service's refresh or resume.
    What the body does is set by the owner's grants, which the check never
    reads, so a task in OTHER_DB owned by a privileged role whose body
    writes SALES_DB is a source write the session can trigger, and the
    `database:` scope must not excuse it as a write on another database.
    It used to: out_of_scope_writes=1, PASS."""
    cur = FakeCursor(LEGITIMATE + [_grant("OPERATE", granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert f"OPERATE on {granted_on.upper().replace(' ', '_')} {name}" in message
    assert "OWNER's privileges" in message
    assert "other databases" not in message
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["read_only"] is False
    assert evidence["out_of_scope_writes"] == 0
    # ...and in the source database or with no scope it is a write as before.
    with pytest.raises(RoleNotReadOnly, match="OPERATE on"):
        assert_role_read_only(cur)
    on_source = FakeCursor(LEGITIMATE + [
        _grant("OPERATE", granted_on, "SALES_DB.PUBLIC.X")])
    with pytest.raises(RoleNotReadOnly, match="OPERATE on"):
        assert_role_read_only(on_source, database="SALES_DB")


def test_owning_or_monitoring_a_task_in_another_database_stays_bounded():
    """The contrast: OWNERSHIP of a task in OTHER_DB is a write on that
    task, and a task the role owns runs with the role's own privileges --
    the ones this check is proving read-only -- so the scope still only
    reports it; MONITOR on it is a read anywhere."""
    cur = FakeCursor(LEGITIMATE + [_grant("OWNERSHIP", "TASK", "OTHER_DB.S.T"),
                                   _grant("OWNERSHIP", "PIPE", "OTHER_DB.S.P"),
                                   _grant("MONITOR", "TASK", "OTHER_DB.S.T"),
                                   _grant("MONITOR", "PIPE", "OTHER_DB.S.P")])
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 2


@pytest.mark.parametrize("granted_on,name", [
    ("PROCEDURE", "OTHER_DB.S.LOAD(VARCHAR)"),
    ("PROCEDURE", "OTHER_DB.S.LOAD(VARCHAR, NUMBER)"),
    ("PROCEDURE", '"other_db"."s"."Load"(VARCHAR)'),
    ("NOTEBOOK", "OTHER_DB.S.NB"),
    ("STREAMLIT", "OTHER_DB.S.APP"),
])
def test_usage_of_a_procedure_outside_the_migration_database_is_refused(
        granted_on, name):
    """Discovery reads procedure DDL only inside `database:`, so USAGE of a
    procedure in another database serves nothing -- and an owner's-rights
    procedure there whose body writes SALES_DB is one CALL away. It used to
    pass as a plain read, not even counted among the out-of-scope rows."""
    cur = FakeCursor(LEGITIMATE + [_grant("USAGE", granted_on, name)])
    with pytest.raises(RoleNotReadOnly) as caught:
        assert_role_read_only(cur, database="SALES_DB")
    message = str(caught.value)
    assert f"USAGE on {granted_on} {name}" in message
    assert "OWNER's privileges" in message and "call" in message
    assert "other databases" not in message
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["read_only"] is False
    assert evidence["out_of_scope_writes"] == 0


@pytest.mark.parametrize("database", ["SALES_DB", "sales_db", None])
def test_usage_of_a_procedure_the_discovery_reads_still_passes(database):
    """In the migration's database, or with no scope, discovery needs the
    procedure DDL and USAGE passes as before (the README asks that no
    procedure be granted that is not needed)."""
    cur = FakeCursor(LEGITIMATE + [
        _grant("USAGE", "PROCEDURE", "SALES_DB.PUBLIC.P(VARCHAR, NUMBER)"),
        _grant("USAGE", "NOTEBOOK", "SALES_DB.PUBLIC.NB"),
        _grant("USAGE", "STREAMLIT", "SALES_DB.PUBLIC.APP"),
        _grant("MONITOR", "TASK", "SALES_DB.PUBLIC.T"),
        _grant("MONITOR", "PIPE", "SALES_DB.PUBLIC.P")])
    evidence = assert_role_read_only(cur, database=database)
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 0


# --- placing a grant is strict: a malformed name is judged inside ---------

@pytest.mark.parametrize("name", [
    "SALES_DB .PUBLIC.ORDERS",      # space after the database part
    " SALES_DB.PUBLIC.ORDERS",      # leading space
    "SALES_DB\t.PUBLIC.ORDERS",     # tab in the database part
    "SALES _DB.PUBLIC.ORDERS",      # space inside the database part
    "sales_db .public.orders",      # lower case, padded
    "OTHER_DB. PUBLIC.ORDERS",      # space in the schema part
    "OTHER _DB.PUBLIC.ORDERS",      # malformed database part, other name
])
def test_a_padded_unquoted_name_part_is_not_another_database(name):
    """An unquoted identifier cannot contain whitespace, so a row whose
    database or schema part does is malformed and cannot be placed: it is
    judged inside the scope. `SALES_DB .PUBLIC.ORDERS` used to compare as
    `SALES_DB ` != `SALES_DB` and be excused as another database."""
    cur = FakeCursor(LEGITIMATE + [_grant("INSERT", "TABLE", name)])
    with pytest.raises(RoleNotReadOnly, match="INSERT on TABLE"):
        assert_role_read_only(cur, database="SALES_DB")
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["out_of_scope_writes"] == 0


def test_a_quoted_name_part_may_contain_whitespace():
    """A quoted identifier IS its text, spaces included: `"SALES DB"` is a
    database that can be placed, inside or outside the scope as written."""
    cur = FakeCursor(LEGITIMATE + [_grant("INSERT", "TABLE",
                                          '"SALES DB"."PUBLIC"."ORDERS"')])
    with pytest.raises(RoleNotReadOnly, match="INSERT on TABLE"):
        assert_role_read_only(cur, database='"SALES DB"')
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 1


@pytest.mark.parametrize("granted_on,name", [
    ("TABLE", "OTHER_DB.ORDERS"),           # two parts on a three-part class
    ("TABLE", "OTHER_DB.S.T.EXTRA"),        # four parts
    ("VIEW", "OTHER_DB.V"),
    ("FUNCTION", "OTHER_DB.F(VARCHAR)"),
    ("SCHEMA", "OTHER_DB.S.X"),             # three parts on a two-part class
    ("DATABASE", "OTHER_DB.S"),             # two parts on a one-part class
])
def test_a_current_grant_with_the_wrong_number_of_name_parts_is_judged_inside(
        granted_on, name):
    """SHOW GRANTS always prints DB.SCHEMA.OBJECT for a table, DB.SCHEMA
    for a schema and DB for a database; a current grant whose name has
    another number of parts is malformed and cannot be placed with
    confidence. `INSERT on TABLE OTHER_DB.ORDERS` used to be placed by its
    first part and excused."""
    cur = FakeCursor(LEGITIMATE + [_grant("INSERT", granted_on, name)])
    with pytest.raises(RoleNotReadOnly, match="INSERT on"):
        assert_role_read_only(cur, database="SALES_DB")
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["out_of_scope_writes"] == 0


def test_a_future_grant_names_its_container_and_is_still_placed():
    """A future grant is printed on the container the objects will be
    created in -- `DB.<TABLE>` at database level, `DB.S.<TABLE>` at schema
    level -- so two or three parts place it; a current grant needs three."""
    cur = FakeCursor(LEGITIMATE, future_by_role={"READER": [
        _future("INSERT", "TABLE", "OTHER_DB.<TABLE>"),
        _future("INSERT", "TABLE", "OTHER_DB.S.<TABLE>"),
        _future("CREATE TABLE", "SCHEMA", "OTHER_DB.<SCHEMA>")]})
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert evidence["out_of_scope_writes"] == 3
    # ...a future row with a malformed container part is judged inside too.
    padded = FakeCursor(LEGITIMATE, future_by_role={"READER": [
        _future("INSERT", "TABLE", "SALES_DB .<TABLE>")]})
    with pytest.raises(RoleNotReadOnly, match="future INSERT on TABLE"):
        assert_role_read_only(padded, database="SALES_DB")


# --- a role grant that names no role is refused, not listed as "" ---------

@pytest.mark.parametrize("granted_on,name", [
    ("ROLE", None), ("ROLE", ""), ("ROLE", "   "), ("ROLE", '""'),
    ("ROLE", '"".""'), ("ROLE", '" "'),
    ("ROLE", "A.B"),                        # two parts on a one-part class
    ("DATABASE_ROLE", None), ("DATABASE_ROLE", ""), ("DATABASE_ROLE", '""'),
    ("DATABASE_ROLE", 'SALES_DB.""'),       # a blank part
    ("DATABASE_ROLE", "SALES_DB."), ("DATABASE_ROLE", ".WRITER"),
    ("DATABASE_ROLE", "WRITER"),            # one part on a two-part class
    ("DATABASE_ROLE", "SALES_DB.S.WRITER"), # three parts
])
def test_a_role_grant_that_names_no_role_is_refused_unlisted(granted_on, name):
    """`USAGE on ROLE` with a blank name used to issue `show grants to role
    ""`, which errors on Snowflake (fail closed) but walks nothing on a
    lenient cursor and passes. A name that is blank, has a blank part or
    does not have the parts its class prints (NAME, DB.NAME) names no
    role, so the row is refused as such and nothing is listed for it."""
    cur = FakeCursor(LEGITIMATE + [{"privilege": "USAGE",
                                    "granted_on": granted_on, "name": name}])
    with pytest.raises(RoleNotReadOnly, match="names no role"):
        assert_role_read_only(cur, database="SALES_DB")
    assert not any('""' in c for c in cur.calls), cur.calls
    assert not any(c.lower().startswith("show grants to database role")
                   for c in cur.calls), cur.calls
    assert len([c for c in cur.calls
                if c.lower().startswith("show grants to role")]) == 2, \
        "READER and PUBLIC only"
    evidence = read_role_grants(cur, database="SALES_DB")
    assert evidence["read_only"] is False
    assert evidence["write_grants"][0]["granted_on"] == granted_on
    assert "names no role" in evidence["write_grants"][0]["reason"]


# --- a system role is the STORED name; "sysadmin" is someone's role --------

@pytest.mark.parametrize("where", ["session", "secondary", "user"])
def test_a_role_created_as_quoted_lower_case_sysadmin_is_not_a_system_role(
        where):
    """`CREATE ROLE "sysadmin"` makes a role distinct from SYSADMIN, and
    `current_role()`, `current_secondary_roles()` and the user listing all
    print its stored name, `sysadmin`, unquoted. It used to be refused as
    a system role because the comparison folded it to upper case; it is a
    role like any other, read-only when its grants are."""
    if where == "session":
        cur = FakeCursor(LEGITIMATE, role="sysadmin", user_roles=["sysadmin"])
    elif where == "secondary":
        cur = FakeCursor(LEGITIMATE, secondary="sysadmin",
                         user_roles=["READER", "sysadmin"])
    else:
        cur = FakeCursor(LEGITIMATE, user_roles=["READER", "sysadmin"])
    evidence = assert_role_read_only(cur, database="SALES_DB")
    assert evidence["read_only"] is True
    assert 'show grants to role "sysadmin"' in cur.calls, "walked, not refused"
    # ...its grants are judged, so a write it holds still refuses the session.
    dirty = FakeCursor(LEGITIMATE, role="sysadmin", user_roles=["sysadmin"],
                       by_role={"sysadmin": LEGITIMATE + [
                           _grant("INSERT", "TABLE", "SALES_DB.PUBLIC.ORDERS")]})
    with pytest.raises(RoleNotReadOnly, match="INSERT on TABLE"):
        assert_role_read_only(dirty, database="SALES_DB")
    # ...and the stored upper-case name is the system role wherever it is.
    for upper in (FakeCursor(LEGITIMATE, role="SYSADMIN",
                             user_roles=["SYSADMIN"]),
                  FakeCursor(LEGITIMATE, secondary="SYSADMIN",
                             user_roles=["READER", "SYSADMIN"]),
                  FakeCursor(LEGITIMATE, user_roles=["READER", "SYSADMIN"])):
        with pytest.raises(RoleNotReadOnly, match="SYSADMIN.*is a system role"):
            assert_role_read_only(upper, database="SALES_DB")
        assert not any('"SYSADMIN"' in c for c in upper.calls), "never listed"
    # ...while a role whose name merely contains one is not a system role.
    assert assert_role_read_only(
        FakeCursor(LEGITIMATE, role="SYSADMIN_READER",
                   user_roles=["SYSADMIN_READER"]),
        database="SALES_DB")["read_only"]


def test_a_session_running_as_a_role_named_public_in_lower_case_is_a_user_role():
    """PUBLIC is the one role no user listing names, so a session running
    as PUBLIC is excused from finding it there. A role created as
    `"public"` is not PUBLIC: the user was granted it, the listing names
    it, and a listing that does not is cut or filtered."""
    cur = FakeCursor(LEGITIMATE, role="public", user_roles=["public"])
    assert assert_role_read_only(cur, database="SALES_DB")["read_only"]
    cut = FakeCursor(LEGITIMATE, role="public", user_rows=[])
    with pytest.raises(RoleNotReadOnly, match="do not name public"):
        assert_role_read_only(cut, database="SALES_DB")

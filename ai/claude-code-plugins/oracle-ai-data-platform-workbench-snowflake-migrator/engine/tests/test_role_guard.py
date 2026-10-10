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
    """Answers the two statements the guard sends; records every call."""

    def __init__(self, grants, *, role="READER", secondary="",
                 by_role=None, fail_grants=None):
        self.grants = grants
        self.role = role
        self.secondary = secondary
        self.by_role = by_role or {}
        self.fail_grants = fail_grants
        self.calls = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        low = " ".join(sql.split()).lower()
        if "current_role()" in low:
            return [{"R": self.role,
                     "S": json.dumps({"roles": self.secondary, "value": ""})}]
        if low.startswith("show grants to role"):
            if self.fail_grants:
                raise RuntimeError(self.fail_grants)
            name = sql.split("role", 1)[1].strip().strip('"')
            return self.by_role.get(name, self.grants)
        raise AssertionError(f"unexpected sql: {sql}")


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


def test_account_level_grants_are_not_source_writes():
    """CREATE DATABASE on the ACCOUNT creates something new; it does not
    write the source. Nor does MANAGE GRANTS, or anything on a warehouse."""
    cur = FakeCursor(READ_ONLY + [
        _grant("CREATE DATABASE", "ACCOUNT", "ACME"),
        _grant("MANAGE GRANTS", "ACCOUNT", "ACME")])
    assert assert_role_read_only(cur, database="SALES_DB")["read_only"]


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
    asked = [c for c in cur.calls if c.lower().startswith("show grants")]
    assert len(asked) == 2, "the primary and the one distinct secondary"
    assert any('"WRITER"' in c for c in asked), "role names are quoted"


def test_the_evidence_carries_no_credential_and_only_grant_facts():
    evidence = read_role_grants(FakeCursor(READ_ONLY), database="SALES_DB")
    assert set(evidence) == {"role", "secondary_roles", "database",
                             "grants_read", "by_privilege", "write_grants",
                             "out_of_scope_writes", "read_only"}

"""Connectivity and permission smoke test. Both ends injected."""
import pytest

from target.coords import resolve_target
from plan.smoke import PROBE_SCHEMA, run_smoke

TARGET = resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                        workspace="ws", cluster_id="cl", catalog="MYDB")


def sf_ok(sql, params=None):
    low = sql.lower()
    if "current_user" in low:
        return [{"U": "TESTUSER", "A": "TESTACCT01", "R": "AWS_US_EAST_2",
                 "ROLE": "ACCOUNTADMIN"}]
    if "show databases" in low:
        return [{"name": "MYDB"}]
    if "information_schema" in low:
        return [{"N": 7}]
    return [{"ok": 1}]


def aidp_ok(sql, params=None):
    low = sql.lower()
    if low.startswith("show schemas"):
        return [{"schema_name": PROBE_SCHEMA}]
    if low.startswith("show catalogs") or low.startswith("show databases"):
        return [{"catalog": "MYDB"}]
    return []


# --- source checks --------------------------------------------------------

def test_source_identity_reported():
    r = run_smoke(source_run_sql=sf_ok)
    assert r["source"]["reachable"] is True
    assert r["source"]["account"] == "TESTACCT01"
    assert r["source"]["role"] == "ACCOUNTADMIN"


def test_source_read_checks_all_pass():
    r = run_smoke(source_run_sql=sf_ok)
    names = {c["name"]: c for c in r["source"]["checks"]}
    assert names["list databases"]["ok"] is True
    assert names["read INFORMATION_SCHEMA"]["ok"] is True
    assert all(c["ok"] for c in r["source"]["checks"])


def test_source_failure_is_captured_not_raised():
    def broken(sql, params=None):
        if "information_schema" in sql.lower():
            raise RuntimeError("insufficient privileges")
        return sf_ok(sql, params)

    r = run_smoke(source_run_sql=broken)
    bad = next(c for c in r["source"]["checks"] if not c["ok"])
    assert "insufficient privileges" in bad["detail"]
    assert r["ok"] is False

def test_probe_schema_name_is_obviously_ours():
    assert "snowmig" in PROBE_SCHEMA.lower()


def test_information_schema_probe_is_qualified_with_a_database():
    # A fresh Snowflake session has no current database, so an unqualified
    # INFORMATION_SCHEMA reference fails with 090105 even for ACCOUNTADMIN.
    seen = []

    def rec(sql, params=None):
        seen.append(sql)
        return sf_ok(sql, params)

    run_smoke(source_run_sql=rec)
    probe = next(s for s in seen if "information_schema" in s.lower())
    assert '"MYDB".information_schema' in probe


def test_explicit_database_is_used_for_the_probe():
    seen = []

    def rec(sql, params=None):
        seen.append(sql)
        return sf_ok(sql, params)

    run_smoke(source_run_sql=rec, database="CHOSEN")
    assert any('"CHOSEN".information_schema' in s for s in seen)


def test_system_databases_are_not_chosen_as_the_probe_target():
    def only_system(sql, params=None):
        if "show databases" in sql.lower():
            return [{"name": "SNOWFLAKE"}, {"name": "SNOWFLAKE_SAMPLE_DATA"}]
        return sf_ok(sql, params)

    r = run_smoke(source_run_sql=only_system)
    bad = next(c for c in r["source"]["checks"] if not c["ok"])
    assert "no non-system database" in bad["detail"]


# ==========================================================================
# The write probe cleans up.
#
# It could not before because the no-DROP rule was applied to the DESTINATION.
# That rule is a SOURCE guarantee: nothing is ever written to or dropped from
# Snowflake. AIDP is where this plugin legitimately creates objects, so
# removing its own probe schema there is correct.
# ==========================================================================
def _target():
    return resolve_target(datalake_ocid="ocid1.aidataplatform.oc1.iad.a",
                          workspace="ws", cluster_id="cl", catalog="CAT")


class _Src:
    """Recording source double, so we can assert nothing but reads reach it."""

    def __init__(self):
        self.calls: list[str] = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        return sf_ok(sql, params)


def _src():
    return _Src()


class DestCall:
    """Catalog-API transport double: call(operation, **kwargs) -> dict."""

    def __init__(self, *, schemas=("bronze", "default"), fail=(),
                 created_visible=True, catalog_type="STANDARD"):
        self.ops: list[tuple] = []
        self.schemas = list(schemas)
        self.fail = set(fail)
        self.created_visible = created_visible
        self.catalog_type = catalog_type

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        if operation in self.fail:
            raise RuntimeError(f"denied: {operation}")
        if operation == "list_catalogs":
            return {"items": [{"displayName": "CAT",
                               "catalogType": self.catalog_type}]}
        if operation == "list_schemas":
            return {"items": [{"key": f'{kw["catalog"]}.{s}'}
                              for s in self.schemas]}
        if operation == "create_schema":
            if self.created_visible:
                self.schemas.append(kw["schema"])
            return {"key": f'{kw["catalog"]}.{kw["schema"]}'}
        if operation == "delete_schema":
            self.schemas = [s for s in self.schemas if s != kw["schema"]]
            return {}
        raise AssertionError(f"unexpected op {operation}")


def test_the_destination_read_check_lists_schemas():
    dest = DestCall()
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest)
    assert any(op == "list_schemas" for op, _ in dest.ops)
    assert not any("sql" in op for op, _ in dest.ops), \
        "the SQL endpoint does not exist; the smoke test must not use it"
    check = r["destination"]["checks"][0]
    assert check["ok"] is True
    assert "2" in check["detail"]


def test_a_denied_read_is_a_real_failure():
    dest = DestCall(fail={"list_schemas"})
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest)
    assert r["destination"]["checks"][0]["ok"] is False
    assert r["ok"] is False


def test_the_write_probe_creates_then_deletes_a_schema():
    dest = DestCall()
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest,
                  write_probe=True)
    ops = [op for op, _ in dest.ops]
    assert "create_schema" in ops and "delete_schema" in ops
    assert r["destination"]["write_verified"] is True
    assert r["destination"]["left_behind"] == []


def test_the_write_probe_uses_a_unique_name_each_run():
    # A failed create permanently poisons a name, so a fixed probe name would
    # be unusable ever after.
    a = DestCall(); b = DestCall()
    run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=a,
              write_probe=True)
    run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=b,
              write_probe=True)
    name_a = [kw["schema"] for op, kw in a.ops if op == "create_schema"][0]
    name_b = [kw["schema"] for op, kw in b.ops if op == "create_schema"][0]
    assert name_a != name_b
    assert name_a.startswith(PROBE_SCHEMA)


def test_a_create_that_does_not_become_visible_is_not_verified():
    # Creates are async and can fail silently, so "the call returned" is not
    # the claim.
    dest = DestCall(created_visible=False)
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest,
                  write_probe=True)
    assert r["destination"]["write_verified"] is False
    assert "not visible" in r["destination"]["write_note"]


def test_a_failed_cleanup_names_what_was_left():
    dest = DestCall(fail={"delete_schema"})
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest,
                  write_probe=True)
    assert r["destination"]["left_behind"]
    assert "remove it manually" in r["destination"]["write_note"]


def test_the_source_is_still_read_only_during_the_destination_probe():
    src = _src()
    run_smoke(source_run_sql=src, target=_target(), dest_call=DestCall(),
              write_probe=True)
    for call in src.calls:
        assert call.strip().split()[0].upper() in (
            "SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "EXPLAIN")


# ==========================================================================
# The write probe is catalog-type aware.
#
# An EXTERNAL catalog is a registered, read-only pointer at the live Snowflake
# source. Probing it for write access FAILS against a destination that works
# -- exactly the failure class this smoke test was rebuilt to prevent -- and
# since 0.16.0 EXTERNAL is the DEFAULT, so anyone running the pipeline in
# order hit that false FAIL and stopped.
# ==========================================================================

def test_the_write_probe_is_skipped_for_an_external_catalog():
    dest = DestCall(catalog_type="EXTERNAL")
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest,
                  write_probe=True)
    assert not any(op == "create_schema" for op, _ in dest.ops), \
        "a read-only catalog must never receive the probe"
    assert r["ok"] is True, "read-only by design is not a failure"
    assert r["destination"]["write_verified"] is False
    assert "EXTERNAL" in r["destination"]["write_note"]
    assert "not a failure" in r["destination"]["write_note"]


def test_the_catalog_type_is_reported_either_way():
    dest = DestCall(catalog_type="EXTERNAL")
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest)
    assert r["destination"]["catalog_type"] == "EXTERNAL"


def test_a_standard_catalog_still_gets_the_write_probe():
    dest = DestCall(catalog_type="STANDARD")
    run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest,
              write_probe=True)
    assert any(op == "create_schema" for op, _ in dest.ops)


def test_an_unresolvable_catalog_type_reads_unknown_and_still_probes():
    # "Could not look" is reported as unknown, never silently assumed -- and
    # the probe proceeds, which is the pre-guard behaviour.
    dest = DestCall(fail={"list_catalogs"})
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=dest,
                  write_probe=True)
    assert r["destination"]["catalog_type"] == "unknown"
    assert any(op == "create_schema" for op, _ in dest.ops)


# ==========================================================================
# The verdict is three-valued. `ok` means "no executed check failed"; it is
# not the claim that both ends were checked. Without target coordinates the
# destination never ran, and that used to render as PASS in the CLI, in
# SMOKE_TEST.md and on the stage board -- on the one stage whose job is to
# prove both ends before anything writes. A skipped check is not a pass.
# ==========================================================================

def test_a_skipped_destination_is_partial_not_pass():
    r = run_smoke(source_run_sql=sf_ok, target=None, dest_call=None)
    assert r["verdict"] == "PARTIAL"
    assert r["complete"] is False
    assert r["ok"] is True, "no executed check failed; skipped is not failed"
    assert r["destination"]["skipped"] is True


def test_a_skipped_destination_with_write_probe_is_still_partial():
    r = run_smoke(source_run_sql=sf_ok, target=None, dest_call=None,
                  write_probe=True)
    assert r["verdict"] == "PARTIAL"
    assert r["destination"]["write_verified"] is False


def test_a_source_failure_beats_partial():
    def broken(sql, params=None):
        if "information_schema" in sql.lower():
            raise RuntimeError("insufficient privileges")
        return sf_ok(sql, params)

    r = run_smoke(source_run_sql=broken, target=None)
    assert r["verdict"] == "FAIL" and r["ok"] is False


def test_an_unreachable_source_is_fail_not_partial():
    def down(sql, params=None):
        raise RuntimeError("network unreachable")

    r = run_smoke(source_run_sql=down)
    assert r["verdict"] == "FAIL"
    assert r["complete"] is False


def test_both_ends_checked_and_clean_is_pass():
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=DestCall())
    assert r["verdict"] == "PASS"
    assert r["complete"] is True and r["ok"] is True


def test_a_denied_destination_read_is_fail_not_partial():
    r = run_smoke(source_run_sql=sf_ok, target=_target(),
                  dest_call=DestCall(fail={"list_schemas"}))
    assert r["verdict"] == "FAIL"


def test_the_default_write_note_names_the_execute_gate():
    # The CLI runs the probe only with `--write-probe --execute`; a plain
    # `smoke` run must not tell the reader that `--write-probe` alone proves
    # write access.
    r = run_smoke(source_run_sql=sf_ok, target=_target(), dest_call=DestCall())
    assert "--write-probe --execute" in r["destination"]["write_note"]

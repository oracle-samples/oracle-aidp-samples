"""Registering the target catalog. EXTERNAL/SNOWFLAKE by default.

An EXTERNAL catalog is a read-only pointer at the live Snowflake source and
copies nothing. A STANDARD catalog is managed storage, so it is created only
when asked for by name, and only ever as the CONTAINER (runbook S4): its
tables belong on AIDP compute, where a failure is visible in the cluster's own
output instead of somewhere inside a series of control-plane HTTP calls.
"""
import pathlib

import pytest

from target.catalog_api import InvalidCatalogSpec, build_catalog_body
from target.catalog_provision import RefusedToExecute, ensure_catalog

CONNECTION = {"accountName": "ORG-ACC", "warehouseName": "WH",
              "databaseName": "SALES_DB", "userName": "SVC",
              "authenticationType": "KEY_PAIR", "privateKey": "-----BEGIN..."}


class Recorder:
    """Injected transport double. Records every operation."""

    def __init__(self, *, existing=()):
        self.ops: list[tuple] = []
        self.catalogs = [{"displayName": n, "key": n.lower(),
                          "catalogType": "EXTERNAL"} for n in existing]

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        if operation == "list_catalogs":
            return {"items": list(self.catalogs)}
        if operation == "create_catalog":
            name = kw["body"]["displayName"]
            self.catalogs.append({"displayName": name, "key": name.lower(),
                                  "catalogType": kw["body"]["catalogType"]})
            return {}
        raise AssertionError(f"unexpected operation {operation}")


# ------------------------------------------------------------ body shape

def test_external_body_carries_the_source_type_and_connection():
    body = build_catalog_body("sales_db", source_type="SNOWFLAKE",
                              connection=CONNECTION)
    assert body["catalogType"] == "EXTERNAL"
    assert body["sourceType"] == "SNOWFLAKE"
    # Live-verified nesting: a flat connectionDetails is a 400.
    assert body["connectionDetails"] == {"connectionProperties": CONNECTION}


def test_external_is_the_default_catalog_type():
    body = build_catalog_body("sales_db", source_type="SNOWFLAKE",
                              connection=CONNECTION)
    assert body["catalogType"] == "EXTERNAL"


def test_standard_body_carries_no_connection_and_no_source_type():
    # Managed storage has nothing to connect to. STANDARD is the runbook's
    # word; INTERNAL is what goes on the wire.
    body = build_catalog_body("lake", catalog_type="STANDARD")
    assert body["catalogType"] == "INTERNAL"
    assert "connectionDetails" not in body
    assert "sourceType" not in body


def test_external_without_a_connection_is_refused_not_defaulted():
    with pytest.raises(InvalidCatalogSpec, match="connectionDetails"):
        build_catalog_body("sales_db", source_type="SNOWFLAKE")


def test_external_without_a_source_type_is_refused():
    with pytest.raises(InvalidCatalogSpec, match="source_type"):
        build_catalog_body("sales_db", source_type=None, connection=CONNECTION)


def test_an_unknown_catalog_type_is_refused_rather_than_guessed():
    # INTERNAL used to be the example here, back when this module thought the
    # managed shape was called STANDARD. It is a real type; MANAGED is not.
    with pytest.raises(InvalidCatalogSpec, match="catalog_type"):
        build_catalog_body("x", catalog_type="MANAGED")


def test_internal_is_a_real_catalog_type():
    body = build_catalog_body("lake", catalog_type="INTERNAL")
    assert body["catalogType"] == "INTERNAL"
    assert "sourceType" not in body


# ------------------------------------------------------------- ensure

def test_an_absent_external_catalog_is_created():
    call = Recorder()
    res = ensure_catalog(display_name="sales_db", call=call,
                         connection=CONNECTION)
    assert res["action"] == "created"
    assert res["verified"] is True
    creates = [kw for op, kw in call.ops if op == "create_catalog"]
    assert len(creates) == 1
    assert creates[0]["body"]["catalogType"] == "EXTERNAL"
    assert creates[0]["body"]["sourceType"] == "SNOWFLAKE"


def test_a_catalog_this_migration_created_is_reused_and_never_re_created():
    # Re-POSTing an existing catalog re-triggers asynchronous work; the schema
    # path already learned that the hard way.
    call = Recorder(existing=["sales_db"])
    res = ensure_catalog(display_name="SALES_DB", call=call,
                         connection=CONNECTION, created_here=["sales_db"])
    assert res["action"] == "reused"
    assert res["reused_because"] == "created by this migration"
    assert [op for op, _ in call.ops] == ["list_catalogs"]


def test_a_catalog_this_migration_did_not_create_is_refused():
    """Same name, any case: without a record or --reuse-existing the
    migration would write into someone else's catalog."""
    call = Recorder(existing=["Sales_DB"])
    with pytest.raises(RefusedToExecute, match="--reuse-existing"):
        ensure_catalog(display_name="sales_db", call=call,
                       connection=CONNECTION, created_here=["other_cat"])
    assert [op for op, _ in call.ops] == ["list_catalogs"]


def test_reuse_existing_adopts_a_catalog_of_the_same_type():
    call = Recorder(existing=["sales_db"])
    res = ensure_catalog(display_name="sales_db", call=call,
                         connection=CONNECTION, reuse_existing=True)
    assert res["action"] == "reused"
    assert res["reused_because"] == "--reuse-existing"


@pytest.mark.parametrize("listed", ["INTERNAL", None])
def test_a_catalog_of_another_type_is_refused_even_with_reuse_existing(listed):
    """An EXTERNAL request adopting an INTERNAL catalog would register no
    Snowflake connection at all; a type the listing does not state cannot
    be checked, so it is refused the same way."""
    call = Recorder(existing=["sales_db"])
    call.catalogs[0]["catalogType"] = listed
    with pytest.raises(RefusedToExecute, match="NOT reused"):
        ensure_catalog(display_name="sales_db", call=call,
                       connection=CONNECTION, reuse_existing=True,
                       created_here=["sales_db"])
    assert not [op for op, _ in call.ops if op == "create_catalog"]


def test_a_catalog_that_never_appears_is_pending_not_created():
    class Blackhole(Recorder):
        def __call__(self, operation, **kw):
            self.ops.append((operation, kw))
            if operation == "list_catalogs":
                return {"items": []}
            return {}          # 202 Accepted, and nothing ever shows up

    res = ensure_catalog(display_name="sales_db", call=Blackhole(),
                         connection=CONNECTION, verify_delays=())
    assert res["action"] == "create_requested"
    assert res["verified"] is False


def test_standard_creates_the_container_and_says_so():
    call = Recorder()
    res = ensure_catalog(display_name="lake", call=call,
                         catalog_type="STANDARD", verify_delays=())
    assert res["catalog_type"] == "INTERNAL"
    assert res["action"] == "created"
    # The container existing must never read as the tables existing.
    assert res["container_only"] is True
    # The note has to route the operator to where the structure path
    # actually is: the S10 workflow, not the refused notebook upload.
    assert "snowmig_01_structure" in res["note"]
    assert "Generate the structure script" not in res["note"]
    assert "--upload" in res["note"] and "refused" in res["note"]
    assert "GAPS" not in res["note"]


def test_standard_never_carries_the_source_credential():
    # A connection passed alongside a STANDARD request is dropped, not
    # attached: managed storage has no source to point at.
    call = Recorder()
    ensure_catalog(display_name="lake", call=call, catalog_type="STANDARD",
                   source_type="SNOWFLAKE", connection=CONNECTION,
                   verify_delays=())
    body = [kw["body"] for op, kw in call.ops if op == "create_catalog"][0]
    assert "connectionDetails" not in body
    assert "sourceType" not in body


def test_external_still_reports_no_container_only_flag():
    call = Recorder()
    res = ensure_catalog(display_name="sales_db", call=call,
                         source_type="SNOWFLAKE", connection=CONNECTION,
                         verify_delays=())
    assert "container_only" not in res


def test_standard_is_an_alias_never_put_on_the_wire():
    # The API rejects catalogType=STANDARD with 400 InvalidParameter, so the
    # alias must be translated before the POST, not passed through.
    call = Recorder()
    ensure_catalog(display_name="lake", call=call, catalog_type="STANDARD",
                   verify_delays=())
    body = [kw["body"] for op, kw in call.ops if op == "create_catalog"][0]
    assert body["catalogType"] == "INTERNAL"


def test_an_unknown_catalog_type_is_refused():
    with pytest.raises(RefusedToExecute):
        ensure_catalog(display_name="lake", call=Recorder(),
                       catalog_type="MANAGED")


# ------------------------------------------------- asynchronous create

class SlowRecorder:
    """A transport where the create settles only after `appears_after` lists.

    This is the real behaviour: POST returns 202 Accepted with an empty body
    and the catalog shows up seconds later. A single immediate read-back
    reports every successful create as pending.
    """

    def __init__(self, *, appears_after: int, fail_lists: int = 0):
        self.ops: list[tuple] = []
        self.appears_after = appears_after
        self.fail_lists = fail_lists
        self.lists_after_create = 0
        self.created: str | None = None

    def __call__(self, operation, **kw):
        self.ops.append((operation, kw))
        if operation == "create_catalog":
            self.created = kw["body"]["displayName"]
            return {}
        if operation == "list_catalogs":
            if self.created is None:
                return {"items": []}
            self.lists_after_create += 1
            if self.lists_after_create <= self.fail_lists:
                raise RuntimeError("backend returned 503: transient")
            if self.lists_after_create >= self.appears_after:
                return {"items": [{"displayName": self.created,
                                   "key": self.created.lower(),
                                   "catalogType": "EXTERNAL"}]}
            return {"items": []}
        raise AssertionError(f"unexpected operation {operation}")


def test_a_create_that_settles_late_is_still_verified():
    call = SlowRecorder(appears_after=3)
    res = ensure_catalog(display_name="sales_db", call=call,
                         connection=CONNECTION, verify_delays=(0, 0, 0, 0))
    assert res["verified"] is True, "polling must outlast an async create"
    assert res["action"] == "created"
    assert res["key"] == "sales_db"


def test_a_transient_listing_failure_mid_poll_is_not_a_failed_create():
    call = SlowRecorder(appears_after=3, fail_lists=2)
    res = ensure_catalog(display_name="sales_db", call=call,
                         connection=CONNECTION, verify_delays=(0, 0, 0, 0))
    assert res["verified"] is True


def test_an_unreadable_listing_before_create_raises_rather_than_creating():
    """'We could not look' is not 'it is not there'. Creating on a failed read
    is how a duplicate gets made against a catalog that already exists."""

    def call(operation, **kw):
        if operation == "list_catalogs":
            raise RuntimeError("backend returned 401: NotAuthenticated")
        raise AssertionError(f"must not reach {operation} after a failed list")

    with pytest.raises(RuntimeError, match="401"):
        ensure_catalog(display_name="sales_db", call=call,
                       connection=CONNECTION, verify_delays=())


# ------------------------------------------------- runbook step labels
#
# The overview skill numbers the runbook: S3 registers the EXTERNAL source,
# S4 creates the INTERNAL target catalog. This module and the CLI both called
# the INTERNAL create S3, so the CLI's own output contradicted the commands
# at the very step they drive.

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_runbook_step_labels_agree_in_catalog_provision():
    text = (ROOT / "target" / "catalog_provision.py").read_text(encoding="utf-8")
    assert "runbook S3" not in text
    assert "runbook S4" in text


def test_runbook_step_labels_agree_in_the_cli():
    import argparse

    import snowmig

    doc = snowmig.cmd_catalog.__doc__
    assert "step S3" not in doc and "step S4" in doc, doc
    parser = snowmig.build_parser()
    subs = next(a for a in parser._actions
                if isinstance(a, argparse._SubParsersAction))
    catalog = subs.choices["catalog"]
    action = next(a for a in catalog._actions
                  if "--catalog-type" in a.option_strings)
    # `databases` is legitimately S3; only the INTERNAL create is checked.
    assert "runbook S3" not in action.help and "runbook S4" in action.help

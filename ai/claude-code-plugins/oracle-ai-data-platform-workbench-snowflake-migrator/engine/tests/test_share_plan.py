"""Outbound shares -> an AIDP Delta Sharing plan. Generated, never executed.

An outbound share is a live contract: a consumer account is querying it
now, and at cutover it keeps reading a source that has stopped moving. The
census names the share; nothing said what replaces it. AIDP's replacement
is Delta Sharing (shares, data assets, recipients). This module maps each
outbound share -- its objects and its consumer accounts -- onto that, and
says plainly what does not map:

  * a recipient is NOT a Snowflake account. Each consumer gets an
    activation and reads with a Delta Sharing client; their Snowflake
    queries against the share do not carry;
  * only a table the plan migrates has an AIDP asset to share. A refused
    object has nothing to share, a view is not a verified data asset, and
    an external/Iceberg table exists only after it is registered;
  * Snowflake applies masking and row-access policies to share consumers.
    Delta Sharing ships the table as stored, so a shared table with a
    policy exposure is HELD, not added -- publishing it would hand raw
    values to another organisation;
  * live facts: `GET /shares` and `GET /recipients` answer 200 on AIDP
    (`aidp delta-share list` / `list-recipients`). The mutating commands'
    bodies are not verified, so the plan names the command and what the
    body must hold, never an invented flag.

Reads Snowflake read-only: one SHOW SHARES, one DESCRIBE SHARE per
outbound share on an in-scope database.
"""
import copy
import json

import pytest

import snowmig
from emulation.snowflake_fake import ENTERPRISE_DB, enterprise_run_sql
from plan.build import build_plan
from snowflake_source.conn import assert_read_only
from snowflake_source.extract.catalog import build_inventory
from snowflake_source.extract.dependencies import extract_dependencies
from snowflake_source.extract.security import build_security
from target.share_plan import build_share_plan, render_share_plan


class _Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        return enterprise_run_sql(sql, params)


@pytest.fixture(scope="module")
def estate():
    inv = build_inventory(enterprise_run_sql, [ENTERPRISE_DB])
    deps = extract_dependencies(enterprise_run_sql, inv)
    return inv, build_plan(inv, deps), build_security(enterprise_run_sql, inv)


@pytest.fixture(scope="module")
def shared(estate):
    inv, plan, sec = estate
    run = _Recorder()
    return build_share_plan(run, inv, plan, security=sec), run


def _share(sp, name):
    return next(s for s in sp["shares"] if s["name"] == name)


def _asset(share, ident):
    return next(a for a in share["objects"] if a["source_identifier"] == ident)


def test_it_reads_only_and_only_what_it_needs(shared):
    _, run = shared
    flat = [" ".join(c.split()).lower() for c in run.calls]
    for sql in run.calls:
        assert_read_only(sql)
    assert sum(c.startswith("show shares") for c in flat) == 1
    # The inbound share is not ours to describe.
    assert [c for c in flat if c.startswith("describe share")] == [
        'describe share "ent_partner_share"']


def test_each_consumer_account_becomes_a_recipient(shared):
    sp, _ = shared
    share = _share(sp, "ENT_PARTNER_SHARE")
    assert share["aidp_share"] == "ent_partner_share"
    assert [(r["consumer_account"], r["recipient"]) for r in share["recipients"]] == [
        ("EMUORG.PARTNER_A", "partner_a"), ("EMUORG.PARTNER_B", "partner_b")]


def test_a_shared_table_with_a_policy_is_held_not_published(shared):
    sp, _ = shared
    share = _share(sp, "ENT_PARTNER_SHARE")
    customers = _asset(share, "SNOWENT.SALES.CUSTOMERS")
    assert customers["status"] == "hold_exposure"
    assert customers["target"] == "snowent.sales.customers"
    assert "MASK_EMAIL" in customers["detail"]
    orders = _asset(share, "SNOWENT.SALES.ORDERS")
    assert orders["status"] == "hold_exposure"
    assert "RAP_REGION" in orders["detail"]
    # Held assets are not in the steps an operator would run.
    added = [s for s in share["steps"] if s["command"] == "manage-data-asset"]
    assert added == []


def test_a_clean_shared_table_is_an_asset_to_add(estate):
    inv, plan, sec = estate
    sec = copy.deepcopy(sec)
    sec["exposures"] = [e for e in sec["exposures"]
                        if e["object"] != "SNOWENT.SALES.ORDERS"]
    sp = build_share_plan(enterprise_run_sql, inv, plan, security=sec)
    share = _share(sp, "ENT_PARTNER_SHARE")
    assert _asset(share, "SNOWENT.SALES.ORDERS")["status"] == "share"
    added = [s for s in share["steps"] if s["command"] == "manage-data-asset"]
    assert [s["subject"] for s in added] == ["snowent.sales.orders"]
    order = [s["command"] for s in share["steps"]]
    assert order.index("create") < order.index("manage-data-asset") \
        < order.index("create-recipient") < order.index("manage-access")


def test_a_refused_object_has_nothing_to_share(shared):
    sp, _ = shared
    view = _asset(_share(sp, "ENT_PARTNER_SHARE"), "SNOWENT.SALES.CUSTOMER_360_SV")
    assert view["status"] == "no_target"
    assert "secure view" in view["detail"]


def test_a_migrating_view_is_not_claimed_as_a_data_asset(estate):
    inv, _, sec = estate
    plan = build_plan(inv, extract_dependencies(enterprise_run_sql, inv),
                      secure_views="as-view")
    sp = build_share_plan(enterprise_run_sql, inv, plan, security=sec)
    view = _asset(_share(sp, "ENT_PARTNER_SHARE"), "SNOWENT.SALES.CUSTOMER_360_SV")
    assert view["status"] == "view_unverified"
    assert "materialise" in view["detail"]


def test_containers_are_not_assets(shared):
    sp, _ = shared
    share = _share(sp, "ENT_PARTNER_SHARE")
    assert {a["kind"] for a in share["objects"]} == {"TABLE", "VIEW"}
    assert share["containers"] == ["DATABASE SNOWENT", "SCHEMA SNOWENT.SALES"]


def test_the_inbound_share_is_listed_without_a_plan(shared):
    sp, _ = shared
    inbound = _share(sp, "WEATHER_SHARE")
    assert inbound["direction"] == "INBOUND"
    assert inbound["objects"] == [] and inbound["steps"] == []
    assert "provider" in inbound["note"]


def test_without_security_json_exposure_is_unknown_not_clean(estate):
    inv, plan, _ = estate
    sp = build_share_plan(enterprise_run_sql, inv, plan, security=None)
    orders = _asset(_share(sp, "ENT_PARTNER_SHARE"), "SNOWENT.SALES.ORDERS")
    assert orders["status"] == "hold_unchecked"
    assert "security" in orders["detail"]


def test_the_report_says_what_does_not_carry(shared):
    sp, _ = shared
    md = render_share_plan(sp)
    assert "Nothing here is executed" in md
    assert "not a Snowflake account" in md
    assert "aidp delta-share list --instance-id <DATALAKE_OCID>" in md
    assert "live-verified" in md
    for name in ("partner_a", "partner_b", "ent_partner_share"):
        assert name in md
    assert "HELD" in md


def test_the_cli_writes_both_artifacts(tmp_path, monkeypatch, estate):
    inv, plan, sec = estate
    for name, data in (("inventory.json", inv), ("plan.json", plan),
                       ("security.json", sec)):
        (tmp_path / name).write_text(json.dumps(data, default=str), encoding="utf-8")
    monkeypatch.setattr(snowmig, "_run_sql_from_args", lambda args: enterprise_run_sql)
    monkeypatch.setattr(snowmig, "_config_path", lambda args, **k: None)
    assert snowmig.main(["share-plan", "--out-dir", str(tmp_path)]) == 0
    data = json.loads((tmp_path / "share_plan.json").read_text(encoding="utf-8"))
    assert data["executed"] is False
    assert {s["name"] for s in data["shares"]} == {"ENT_PARTNER_SHARE", "WEATHER_SHARE"}
    assert (tmp_path / "SHARE_PLAN.md").is_file()

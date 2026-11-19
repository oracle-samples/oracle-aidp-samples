"""Warehouse inventory. SHOW always; ACCOUNT_USAGE metering when permitted."""
from fake_sql import FakeSql
from snowflake_source.extract.warehouses import extract_warehouses

SHOW = [
    {"name": "COMPUTE_WH", "size": "X-Small", "state": "SUSPENDED", "type": "STANDARD",
     "min_cluster_count": 1, "max_cluster_count": 1, "auto_suspend": 600,
     "auto_resume": "true", "owner": "ACCOUNTADMIN", "comment": None},
    {"name": "BIG_WH", "size": "4X-Large", "state": "STARTED", "type": "STANDARD",
     "min_cluster_count": 1, "max_cluster_count": 4, "auto_suspend": 60,
     "auto_resume": "true", "owner": "SYSADMIN", "comment": "etl"},
]
METER = [{"WAREHOUSE_NAME": "BIG_WH", "CREDITS": 1234.5, "DAYS": 30}]


def test_show_warehouses_always_works():
    out = extract_warehouses(FakeSql({"show warehouses": SHOW}))
    assert out["warehouse_count"] == 2
    assert out["metering_source"] == "unavailable"
    names = [w["name"] for w in out["warehouses"]]
    assert names == ["BIG_WH", "COMPUTE_WH"], "sorted by size descending"


def test_size_and_scaling_captured():
    out = extract_warehouses(FakeSql({"show warehouses": SHOW}))
    big = next(w for w in out["warehouses"] if w["name"] == "BIG_WH")
    assert big["size"] == "4X-Large"
    assert big["max_cluster_count"] == 4
    assert big["auto_suspend_seconds"] == 60


def test_max_concurrent_clusters_totalled():
    # The worst case a target must be able to absorb.
    out = extract_warehouses(FakeSql({"show warehouses": SHOW}))
    assert out["max_concurrent_clusters"] == 5


def test_metering_attached_when_account_usage_readable():
    out = extract_warehouses(FakeSql({"show warehouses": SHOW,
                                      "warehouse_metering_history": METER}))
    assert out["metering_source"] == "account_usage"
    big = next(w for w in out["warehouses"] if w["name"] == "BIG_WH")
    assert big["observed_credits"] == 1234.5
    assert big["observed_days"] == 30


def test_warehouse_without_metering_rows_is_explicitly_null():
    out = extract_warehouses(FakeSql({"show warehouses": SHOW,
                                      "warehouse_metering_history": METER}))
    small = next(w for w in out["warehouses"] if w["name"] == "COMPUTE_WH")
    assert small["observed_credits"] is None, "absent != zero"


def test_metering_denial_is_noted_not_fatal():
    class Denied(FakeSql):
        def __call__(self, sql, params=None):
            if "warehouse_metering_history" in sql.lower():
                raise RuntimeError("not authorized")
            return super().__call__(sql, params)

    out = extract_warehouses(Denied({"show warehouses": SHOW}))
    assert out["warehouse_count"] == 2
    assert out["metering_source"] == "unavailable"
    assert "not authorized" in out["metering_note"]


def test_no_visible_warehouse_is_noted_as_role_filtered():
    # SHOW WAREHOUSES needs no special privilege, but it returns only the
    # warehouses the role holds a privilege on. An empty list is a finding
    # about the role before it is a finding about the account.
    out = extract_warehouses(FakeSql({"show warehouses": []}))
    assert out["warehouse_count"] == 0
    assert "visible" in out["visibility_note"]
    assert "0 warehouse" in out["visibility_note"]
    seen = extract_warehouses(FakeSql({"show warehouses": SHOW}))
    assert seen["visibility_note"] == ""

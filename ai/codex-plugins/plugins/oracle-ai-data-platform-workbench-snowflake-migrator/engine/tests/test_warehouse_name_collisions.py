"""Two warehouses never share one cluster because their names fold alike.

Found in review. cluster_base_name strips _WH / _WAREHOUSE / -WH and folds
case, so COMPUTE_WH and COMPUTE both became `compute`. propose_all still
counted two clusters to create and provision built both targets without a
collision check: it created ONE cluster, then reported the second
warehouse as a failed step saying "a cluster of that name already exists
and was left untouched; it is not this migration's" -- false, it had just
created it. With --reuse-existing the two warehouses were silently merged.

Names are now disambiguated after naming, the same way in the proposal
and in provision: a base name two warehouses share, or one equal to the
migration cluster's, falls back to the full translated warehouse name,
then to a numbered suffix; each rename is noted.
"""
import pytest

from sizing.warehouse_map import cluster_names, propose_all
from target import provisioning
from target.provisioning import provision
from test_provisioning import Fake


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def _wh(name, size="X-Small"):
    return {"name": name, "size": size}


def test_names_that_fold_alike_are_disambiguated_in_the_proposal():
    sizing = propose_all([_wh("ETL_WH"), _wh("ETL_WAREHOUSE")])
    names = [p["target_cluster"] for p in sizing["proposals"]]
    assert len(set(names)) == 2, names
    assert sizing["clusters_to_create"] == 2
    assert all("collide" in p["notes"] for p in sizing["proposals"])


def test_a_unique_base_name_is_kept():
    assert cluster_names(["COMPUTE_WH", "BI_WH"]) == {
        "COMPUTE_WH": "compute", "BI_WH": "bi"}


def test_the_fallback_is_deterministic_and_then_numbered():
    assert cluster_names(["COMPUTE_WH", "COMPUTE"]) == {
        "COMPUTE": "compute", "COMPUTE_WH": "compute_wh"}
    assert cluster_names(["COMPUTE", "COMPUTE_WH"]) == cluster_names(
        ["COMPUTE_WH", "COMPUTE"]), "input order does not decide"
    folded = cluster_names(["etl", "ETL"])
    assert sorted(folded.values()) == ["etl", "etl_2"]


def test_the_migration_cluster_name_is_reserved():
    assert cluster_names(["MIGRATION_ASSETS_WH"],
                         reserved={"migration_assets"}) == {
        "MIGRATION_ASSETS_WH": "migration_assets_wh"}


def test_provision_creates_one_cluster_per_warehouse_and_blames_nobody():
    fake = Fake()
    res = provision(call=fake, workspace_name="acme", scripts=[],
                    execute=True, delays=(),
                    warehouse_clusters=[_wh("COMPUTE_WH"), _wh("COMPUTE")])
    created = [kw["body"]["displayName"] for op, kw in fake.ops
               if op == "create_cluster"]
    assert created == ["migration_assets", "compute_wh", "compute"]
    steps = [s for s in res["steps"] if s["step"] == "warehouse-cluster"]
    assert [s["action"] for s in steps] == ["created", "created"]
    assert not any("not this migration" in s["detail"] for s in steps)
    assert all(t["created"] for t in res["warehouse_clusters"])

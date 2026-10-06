"""Teardown acts only on compute this migration can PROVE it created.

Found in review. provision_result.json recorded a `key` for every cluster it
touched -- created, adopted with --reuse-existing, name_taken, and the
cluster `compute.warehouse_clusters: existing` maps every warehouse to --
and nothing said which of those it had created. teardown and the billing
report excluded only a `uses_existing` field that provision never wrote,
so `teardown --action delete --execute` deleted the operator's pre-existing
shared cluster (configured as "nothing is created or resized") and, after a
documented --reuse-existing adoption, other teams' clusters, labelled
"migration cluster".

Provenance is now positive: `created: true` is written on the create path
only, and a re-push into the same workspace carries it forward from the
earlier executed record. Anything else with a key is listed as "not this
migration's, left alone" and never stopped or deleted.
"""
import json

import pytest

import snowmig
from report.resources import build_resources
from target import provisioning
from target.provisioning import carry_forward, provision
from target.teardown import render_teardown, teardown
from test_provisioning import Fake

OCID = "ocid1.aidataplatform.oc1.iad.aaaafake"


class World(Fake):
    """The provisioning fake, plus cluster state for teardown to act on."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.destructive: list[tuple] = []

    def __call__(self, operation, **kw):
        if operation in ("stop_cluster", "delete_cluster"):
            self.ops.append((operation, kw))
            self.destructive.append((operation, kw["cluster"]))
            if operation == "delete_cluster":
                self.clusters = [c for c in self.clusters
                                 if c["key"] != kw["cluster"]]
            else:
                for c in self.clusters:
                    if c["key"] == kw["cluster"]:
                        c["state"] = "STOPPED"
            return {}
        return super().__call__(operation, **kw)


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)
    monkeypatch.setattr("target.teardown.time.sleep", lambda _s: None)


WAREHOUSES = [{"name": "COMPUTE_WH", "size": "X-Small"}]


def test_an_existing_mode_shared_cluster_is_never_stopped_or_deleted():
    world = World(clusters=("shared_prod",))
    prov = provision(call=world, workspace_name="acme", scripts=[],
                     execute=True, delays=(),
                     warehouse_clusters=WAREHOUSES,
                     warehouse_cluster_mode="existing",
                     existing_cluster_id="cl-shared_prod")
    wc = prov["warehouse_clusters"][0]
    assert wc["created"] is False and wc["uses_existing"] is True
    assert wc["name"] != "compute", \
        "the base name of COMPUTE_WH is not the existing cluster's name"

    res = teardown(world, prov, action="delete", execute=True, delays=(0,))
    assert ("delete_cluster", "cl-shared_prod") not in world.destructive
    assert ("stop_cluster", "cl-shared_prod") not in world.destructive
    assert [c for c in world.clusters if c["key"] == "cl-shared_prod"]
    assert {s["cluster"] for s in res["steps"]} == {"cl-migration_assets"}
    left = {c["cluster"] for c in res["left_alone"]}
    assert left == {"cl-shared_prod"}
    md = render_teardown(res)
    assert "cl-shared_prod" in md and "left alone" in md.lower()


def test_clusters_adopted_with_reuse_existing_are_not_the_migrations():
    world = World(workspaces=("acme",),
                  clusters=("migration_assets", "compute"))
    prov = provision(call=world, workspace_name="acme", scripts=[],
                     execute=True, delays=(), reuse_existing=True,
                     warehouse_clusters=WAREHOUSES)
    res = teardown(world, prov, action="delete", execute=True, delays=(0,))
    assert world.destructive == [], \
        "nothing here was created by this migration"
    assert res["steps"] == []
    assert {c["cluster"] for c in res["left_alone"]} == {
        "cl-migration_assets", "cl-compute"}


def test_the_billing_report_lists_only_what_was_created(tmp_path):
    world = World(clusters=("shared_prod",))
    prov = provision(call=world, workspace_name="acme", scripts=[],
                     execute=True, delays=(),
                     warehouse_clusters=WAREHOUSES,
                     warehouse_cluster_mode="existing",
                     existing_cluster_id="cl-shared_prod")
    (tmp_path / "provision_result.json").write_text(json.dumps(prov))
    keys = {r["key"] for r in build_resources(tmp_path)["resources"]
            if r["kind"] == "cluster"}
    assert keys == {"cl-migration_assets"}


def test_a_cluster_this_migration_created_is_torn_down():
    world = World()
    prov = provision(call=world, workspace_name="acme", scripts=[],
                     execute=True, delays=(), warehouse_clusters=WAREHOUSES)
    assert prov["cluster"]["created"] is True
    assert prov["warehouse_clusters"][0]["created"] is True
    teardown(world, prov, action="delete", execute=True, delays=(0,))
    assert sorted(world.destructive) == [("delete_cluster", "cl-compute"),
                                         ("delete_cluster",
                                          "cl-migration_assets")]


def test_a_re_push_into_the_same_workspace_keeps_the_created_flag():
    """The documented plan push is `--reuse-existing` into this migration's
    own workspace. Its clusters are found, so this push records them as
    reused; the earlier executed record proves they were created here."""
    world = World()
    first = provision(call=world, workspace_name="acme", scripts=[],
                      execute=True, delays=(), warehouse_clusters=WAREHOUSES)
    again = provision(call=world, workspace_name="acme", scripts=[],
                      execute=True, delays=(), reuse_existing=True,
                      warehouse_clusters=WAREHOUSES)
    assert again["cluster"]["created"] is False
    carry_forward(again, first)
    assert again["cluster"]["created"] is True
    assert again["warehouse_clusters"][0]["created"] is True
    res = teardown(world, again, action="stop", execute=True, delays=(0,))
    assert {s["cluster"] for s in res["steps"]} == {"cl-migration_assets",
                                                    "cl-compute"}


def test_a_record_from_another_workspace_proves_nothing_here():
    world = World()
    first = provision(call=world, workspace_name="acme", scripts=[],
                      execute=True, delays=())
    other = World(workspaces=("acme",), clusters=("migration_assets",))
    other.workspaces[0]["key"] = "ws-somebody-else"
    again = provision(call=other, workspace_name="acme", scripts=[],
                      execute=True, delays=(), reuse_existing=True)
    carry_forward(again, first)
    assert again["cluster"]["created"] is False


def test_a_record_written_before_provenance_uses_its_steps():
    """provision_result.json files already on disk carry no `created`
    field; their steps are the evidence, read the same positive way. A
    `reused` step is not proof of "somebody else's" (the documented re-push
    reuses the migration's own cluster): it is unknown, never touched, and
    a failed step (see test_teardown_legacy_provenance.py)."""
    legacy = {"dry_run": False, "workspace": {"key": "ws"},
              "cluster": {"name": "migration_assets", "key": "mc"},
              "warehouse_clusters": [
                  {"warehouse": "A_WH", "name": "a", "key": "wa"},
                  {"warehouse": "B_WH", "name": "b", "key": "wb"}],
              "steps": [{"step": "cluster", "action": "created",
                         "verified": True, "detail": "migration_assets"},
                        {"step": "warehouse-cluster", "action": "created",
                         "verified": True, "detail": "A_WH -> a"},
                        {"step": "warehouse-cluster", "action": "reused",
                         "verified": True, "detail": "B_WH -> b"}]}
    res = teardown(None, legacy, action="stop", execute=False)
    assert {s["cluster"] for s in res["steps"]
            if s["action"] == "would stop"} == {"mc", "wa"}
    assert [(s["cluster"], s["verified"]) for s in res["steps"]
            if s["action"] == "provenance_unknown"] == [("wb", False)]
    assert res["left_alone"] == []


def test_the_cli_re_push_carries_provenance_into_the_record(tmp_path,
                                                            monkeypatch):
    world = World()
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: world)
    base = ["provision", "--datalake-ocid", OCID, "--workspace-name", "acme",
            "--skip-libraries", "--out-dir", str(tmp_path), "--execute"]
    assert snowmig.main(base) == 0
    assert snowmig.main(base + ["--reuse-existing"]) == 0
    rec = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert rec["cluster"]["created"] is True

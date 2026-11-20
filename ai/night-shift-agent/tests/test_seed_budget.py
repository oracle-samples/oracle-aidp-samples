"""scripts/seed_budget.py: an estimate is only seeded from a cluster body that states its OCPUs. The platform client is
replaced by a stand-in; nothing here talks to a workspace."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


class ClusterIO:
    """The two calls the script makes: the cluster body and ledger SQL."""

    def __init__(self, cluster: dict) -> None:
        self.cluster = cluster
        self.sql: list[str] = []

    def get_cluster(self, key):
        return dict(self.cluster)

    def run_sql(self, sql, timeout=300):
        self.sql.append(sql)
        return []

    def close(self):
        pass


def _run_script(monkeypatch, cluster: dict, *argv: str):
    spec = importlib.util.spec_from_file_location("ns_seed_budget", ROOT / "scripts" / "seed_budget.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    io = ClusterIO(cluster)
    monkeypatch.setattr(mod, "AidpIO", lambda env, dry_run=True: io)
    monkeypatch.setattr(sys, "argv", ["seed_budget.py", *argv])
    return mod.main(), io


@pytest.mark.parametrize("cluster", [
    {"key": "c1", "driverConfig": {}, "workerConfig": {}},                                         # no shape at all
    {"key": "c1", "driverConfig": {"driverShapeConfig": {"ocpus": 4}}, "workerConfig": {}},        # a worker is assumed, its shape unknown
    {"key": "c1", "driverConfig": {"driverShape": "amd.generic"}, "workerConfig": {"workerShapeConfig": {"ocpus": 8}}},
], ids=["no-shape", "worker-ocpus-missing", "driver-ocpus-missing"])
def test_no_rows_are_seeded_when_the_cluster_body_does_not_state_its_ocpus(monkeypatch, capsys, cluster):
    """Zero-OCPU rows would read as a known budget with everything remaining, and a later run could not correct them
    (rows are insert-if-absent per day). One active worker is assumed, as the script does by default."""
    code, io = _run_script(monkeypatch, cluster, "--workers", "1")

    assert code != 0
    assert io.sql == []
    assert "ocpus" in capsys.readouterr().err


def test_a_cluster_body_with_ocpus_is_accepted(monkeypatch, capsys):
    cluster = {"key": "c1", "driverConfig": {"driverShapeConfig": {"ocpus": 4}},
               "workerConfig": {"workerShapeConfig": {"ocpus": 8}}}

    code, io = _run_script(monkeypatch, cluster, "--workers", "2")

    assert code == 0
    assert "driver 4 + 2 x 8 workers = 20" in capsys.readouterr().out
    assert all("budget_ledger" in s for s in io.sql)


def test_a_driver_only_estimate_needs_no_worker_shape(monkeypatch, capsys):
    cluster = {"key": "c1", "driverConfig": {"driverShapeConfig": {"ocpus": 4}}, "workerConfig": {}}

    code, _ = _run_script(monkeypatch, cluster, "--workers", "0")

    assert code == 0

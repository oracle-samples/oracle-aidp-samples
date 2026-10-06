"""The engine package tree exists and the fork's dead weight is gone."""
import importlib
import pathlib

import pytest

ENGINE = pathlib.Path(__file__).resolve().parents[1]

PACKAGES = [
    "snowflake_source",
    "snowflake_source.dialect",
    "snowflake_source.extract",
    "plan",
    "target",
    "report",
]

# Modules that must exist. The engine is now entirely first-party: the fork's
# OCI transport was removed once the aidp/oci CLI backends replaced it.
KEPT = [
    "snowmig.py",
    "snowflake_source/conn.py",
    "snowflake_source/dialect/types.py",
    "snowflake_source/dialect/views.py",
    "snowflake_source/extract/catalog.py",
    "plan/build.py",
    "target/executor.py",
    "target/runner.py",
    "target/notebook.py",
    "report/render.py",
]

# Dead weight for MVP-1: no notebook migration, no data movement, no dbutils.
PRUNED = [
    "scripts/job_migrate.py",
    "scripts/agent_migrate.py",
    "scripts/build_dag.py",
    "scripts/cell_analyzer.py",
    "scripts/fuse_scanner.py",
    "scripts/acceptance_contract.py",
    "scripts/extract_catalog_databricks.py",
    "aidp_compat",
    "schemas",
    "run_migration.sh",
    "setup.py",
    # The asyncio Jupyter-WebSocket transport. Dead once the aidp/oci CLI
    # backends replaced it; kept nothing that referenced it.
    "target/aidp_executor.py",
    "target/cluster_session.py",
    "target/cluster_lifecycle.py",
    "target/aidp_runner.py",
]


@pytest.mark.parametrize("pkg", PACKAGES)
def test_package_importable(pkg):
    assert importlib.import_module(pkg) is not None


@pytest.mark.parametrize("rel", KEPT)
def test_transport_modules_kept(rel):
    assert (ENGINE / rel).is_file(), f"{rel} must survive the prune"


@pytest.mark.parametrize("rel", PRUNED)
def test_dead_weight_pruned(rel):
    assert not (ENGINE / rel).exists(), f"{rel} is dead weight for MVP-1"


def test_no_websocket_transport_remains():
    """The WebSocket/asyncio path is gone, not merely unused.

    Leaving it in place implied a live transport that nothing called, and it was
    the only code that could execute arbitrary Python on a cluster.
    """
    for path in ENGINE.rglob("*.py"):
        if "tests" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for token in ("websocket", "asyncio", "wss://"):
            assert token not in text.lower(), f"{path.name} still mentions {token}"

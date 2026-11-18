"""Placeholder instance identifiers so the suite runs on a fresh checkout without a .env file, plus the duckdb ledger
fixture shared by the ledger and draft-pass tests. The suite never talks to the platform; a few fixtures build Actions
whose target is the configured cluster key, so the configuration needs some value for it."""
import os

for _k, _v in {"NS_REGION": "us-ashburn-1", "NS_DATA_LAKE_ID": "ocid1.aidataplatform.oc1.test.datalake", "NS_WORKSPACE_ID": "workspace-test",
               "NS_CLUSTER_ID": "cluster-test", "NS_OCI_PROFILE": "DEFAULT"}.items():
    os.environ.setdefault(_k, _v)

import duckdb  # noqa: E402
import pytest  # noqa: E402

from nightshift import ledger as L  # noqa: E402

from helpers import _DuckCon  # noqa: E402


@pytest.fixture
def ops_db():
    """Fresh duckdb database with the ops schema created from the ledger DDL (single-level schema name: duckdb has no
    ``default`` catalog). Yields ``(ops, con)``; ``con.execute`` translates Spark SQL to duckdb on the way in."""
    ops = "nightshift_ops"
    con = _DuckCon(duckdb.connect())
    for stmt in L.ddl_statements(ops):
        con.execute(stmt)
    yield ops, con
    con.close()

"""Phase 1 CLI behaviour for option-based load() reads (REF-271), with no model.

A read whose dataset is given through reader options instead of a positional
argument stays an unresolved finding, so it can never certify readiness.
Cases marked PENDING pin what a deterministic resolver must add: naming the
dataset and checking a Cassandra read at its Scylla mirror. The other cases
are contracts that hold now and must keep holding.
"""
import asyncio
import contextlib
import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import check_data_availability_for_workflow as checker
from data_check_extract import extract_from_notebook


PROVIDER = "org.apache.spark.sql.cassandra"
TABLES = ["offer_keys_mapping", "offer_id_keys_mapping", "offer_attributes"]
MIRRORS = [f"scylla_ds_offers_data.{table}" for table in TABLES]
# Spark 3.5's message for an absent table, as the existing probe receives it.
SPARK_MISSING_TABLE = ("[TABLE_OR_VIEW_NOT_FOUND] The table or view "
                       "`scylla_ds_offers_data`.`offer_keys_mapping` cannot be found.")

PENDING = pytest.mark.xfail(
    strict=True,
    reason="REF-271: option-based load() datasets are not resolved since the model "
           "analysis was removed; residual accepted until the deterministic resolver lands")


def notebook(*sources):
    return {"cells": [{"cell_type": "code", "source": source} for source in sources]}


def offers_notebook():
    """The DxGy incident shape: constants in one cell, three reads in another."""
    return notebook(
        'KEYSPACE = "ds_offers_data"\n'
        + "\n".join(f'TABLE_{i} = "{table}"' for i, table in enumerate(TABLES)),
        "\n".join(f'df_{i} = spark.read.format("{PROVIDER}").options('
                  f'keyspace=KEYSPACE, table=TABLE_{i}).load()' for i in range(3)),
    )


def findings(result):
    return (result["tables"] + result["paths"] + result["unresolved"]
            + result["unresolved_scopes"])


class MetadataSession:
    def __init__(self, error=None, **_kwargs):
        self.error = error
        self.queries = []
        self.closed = False

    async def connect(self):
        return None

    async def close(self):
        self.closed = True

    def sql(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return SimpleNamespace(collect=lambda: [{"col_name": "id", "data_type": "string"}])

    async def execute(self, code, timeout):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            # Checker-owned metadata probes only, never customer source.
            exec(compile(code, "<metadata-probe>", "exec"), {"spark": self})
        return {"status": "ok", "outputs": [{"type": "stream", "text": output.getvalue()}]}


def run_cli_report(monkeypatch, tmp_path, nb, session):
    """Real CLI main() with a downloaded notebook and a controlled metadata session."""
    monkeypatch.setattr(checker, "get_oci_signer", lambda *_args: ({}, object()))
    monkeypatch.setattr(checker, "_download_notebook_json", lambda *_args: nb)
    monkeypatch.setattr(checker, "AIDPSession", lambda **_kwargs: session)
    monkeypatch.setattr(checker, "_BUCKET_NS", {})
    bucket_file = tmp_path / "buckets.csv"
    bucket_file.write_text("s3_bucket,oci_bucket,oci_namespace\n")
    cfg = SimpleNamespace(
        aidp=SimpleNamespace(region="us-ashburn-1", lake_ocid="lake", workspace_id="ws",
                             oci_profile="DEFAULT"),
        bucket_mapping=SimpleNamespace(path=str(bucket_file)),
    )
    monkeypatch.setattr("aidp_config.load_config", lambda **_kwargs: cfg)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"jobs": [{"job_name": "fixture", "job_id": "123",
        "tasks": [{"task_key": "read", "notebook_path": "/Workspace/read.ipynb"}]}]}))
    output = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", ["check_data_availability_for_workflow.py",
        "--manifest", str(manifest), "--cluster", "approved-control", "--output", str(output)])
    asyncio.run(checker.main())
    assert session.closed
    return json.loads(output.read_text())


def test_unresolved_option_loads_never_certify_readiness(monkeypatch, tmp_path, capsys):
    session = MetadataSession()
    report = run_cli_report(monkeypatch, tmp_path, offers_notebook(), session)
    # No dataset is guessed: nothing is probed, and every read stays a finding.
    assert session.queries == []
    scopes = report["notebooks"]["/Workspace/read.ipynb"]["unresolved_scopes"]
    assert [scope["kind"] for scope in scopes] == ["UNRESOLVED_READ"] * 3
    assert report["summary"]["unresolved"] == 3
    output = capsys.readouterr().out
    assert "ALL DEPENDENCIES AVAILABLE" not in output
    assert "manual review required" in output


def test_commented_load_is_not_a_read():
    source = f'# spark.read.format("{PROVIDER}").options(keyspace="ks", table="t").load()'
    assert not findings(extract_from_notebook(notebook(source)))


@PENDING
@pytest.mark.parametrize("available", [True, False])
def test_dxgy_cassandra_reads_are_checked_at_their_scylla_mirror(
        monkeypatch, tmp_path, capsys, available):
    session = MetadataSession(None if available else Exception(SPARK_MISSING_TABLE))
    report = run_cli_report(monkeypatch, tmp_path, offers_notebook(), session)
    assert sorted(session.queries) == sorted(f"DESCRIBE TABLE {name}" for name in MIRRORS)
    assert report["summary"]["tables_found" if available else "tables_missing"] == 3
    requirements = report["data_migration"]["requirements"]
    assert {row["object_name"] for row in requirements} == set(MIRRORS)
    assert all(row["currently_exists"] is available for row in requirements)
    if not available:
        assert "data source(s) MISSING" in capsys.readouterr().out


@pytest.mark.parametrize("reader", [
    'format("jdbc").option("dbtable", "sales.orders")',
    f'format("{PROVIDER}").options(keyspace="ks", table="a`; DROP TABLE victim; --")',
    f'format("{PROVIDER}").options(keyspace="ks", table="order lines")',
    f'format("{PROVIDER}").options(keyspace="1ks", table="orders")',
])
def test_connector_names_without_a_mirror_never_reach_sql(monkeypatch, tmp_path, reader):
    session = MetadataSession()
    run_cli_report(monkeypatch, tmp_path, notebook(f"df = spark.read.{reader}.load()"), session)
    assert session.queries == []


def test_report_never_copies_connector_options(monkeypatch, tmp_path, capsys):
    nb = notebook(f'df = spark.read.format("{PROVIDER}")'
                  '.option("password", "never-log").load()')
    report = run_cli_report(monkeypatch, tmp_path, nb, MetadataSession())
    assert "never-log" not in json.dumps(report)
    assert "never-log" not in capsys.readouterr().out


def test_unknown_keyword_load_path_keeps_its_warning(monkeypatch, tmp_path, capsys):
    nb = notebook('p = dbutils.widgets.get("input_path")', "df = spark.read.load(path=p)")
    report = run_cli_report(monkeypatch, tmp_path, nb, MetadataSession())
    assert report["summary"]["unresolved"] >= 1
    assert "ALL DEPENDENCIES AVAILABLE" not in capsys.readouterr().out

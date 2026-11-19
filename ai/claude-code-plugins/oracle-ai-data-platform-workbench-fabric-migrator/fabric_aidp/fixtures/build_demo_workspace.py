#!/usr/bin/env python3
"""Generate the bundled demo Fabric Git export.

Run once and commit the result:
    python3 -m fabric_aidp.fixtures.build_demo_workspace

Every property this estate carries is deliberate and pinned by
tests/test_fixture_baseline.py -- read that file before changing anything here.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

MARK = "*" * 20
NS = "AcmeWS"


def _cells(meta, *cells) -> str:
    out = ["# Fabric notebook source", "", f"# METADATA {MARK}", "", "# META {"]
    body = json.dumps(meta, indent=2).splitlines()[1:-1]
    out += [f"# META {line}" for line in body]
    out += ["# META }", ""]
    for kind, text in cells:
        out += [f"# {kind} {MARK}", ""]
        out += text.split("\n") + [""]
    return "\n".join(out)


def _lakehouse_meta(name):
    return {"dependencies": {"lakehouse": {"default_lakehouse_name": name}}}


def _item(root: Path, name: str, kind: str, files: dict) -> None:
    directory = root / f"{name}.{kind}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / ".platform").write_text(json.dumps({
        "version": "2.0",
        "config": {"logicalId": f"00000000-0000-0000-0000-{abs(hash(name)) % 10**12:012d}"},
        "metadata": {"type": kind, "displayName": name,
                     "description": f"Acme demo {kind.lower()}"},
    }, indent=2) + "\n", encoding="utf-8")
    for filename, content in files.items():
        (directory / filename).write_text(content, encoding="utf-8")


def build(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)

    # --- notebooks -------------------------------------------------------
    _item(root, "01_Ingest_Claims", "Notebook", {"notebook-content.py": _cells(
        _lakehouse_meta("SalesLake"),
        ("MARKDOWN", "# # Ingest claims\n# Reads the daily drop and lands it."),
        ("CELL", 'raw = spark.read.parquet(\n'
                 f'    "abfss://{NS}@onelake.dfs.fabric.microsoft.com'
                 '/SalesLake.Lakehouse/Files/raw/claims")\n'
                 'display(raw)'),
        ("CELL", 'raw.write.mode("overwrite").saveAsTable("claims_daily")'))})

    _item(root, "02_Build_Aggregates", "Notebook", {"notebook-content.py": _cells(
        _lakehouse_meta("SalesLake"),
        ("CELL", 'daily = spark.table("claims_daily")\n'
                 'daily.groupBy("policy_no").count()'
                 '.write.saveAsTable("claims_agg")'))})

    _item(root, "03_Report_Claims", "Notebook", {"notebook-content.py": _cells(
        _lakehouse_meta("SalesLake"),
        ("CELL", 'agg = spark.table("claims_agg")\n'
                 'clean = spark.table("dbo.claim")\n'
                 'display(agg)'))})

    # Reads a shortcut by name -> NB11 must fire.
    _item(root, "04_Read_External", "Notebook", {"notebook-content.py": _cells(
        _lakehouse_meta("SalesLake"),
        ("CELL", 'external = spark.table("claims_raw_s3")\n'
                 'external.count()'))})

    # No default lakehouse binding -> NB13 must fire.
    _item(root, "05_Unbound_Writer", "Notebook", {"notebook-content.py": _cells(
        {},
        ("CELL", 'df.write.saveAsTable("orphan_output")'))})

    # A Spark-SQL cell -> no SQ rule may fire on it.
    _item(root, "06_Sql_Summary", "Notebook", {"notebook-content.py": _cells(
        _lakehouse_meta("SalesLake"),
        ("CELL", "%%sql\nSELECT policy_no, count(*) AS n\n"
                 "FROM dbo.claim\nGROUP BY policy_no"))})

    # --- warehouse -------------------------------------------------------
    warehouse = {}
    warehouse["claim.sql"] = (
        "CREATE TABLE [dbo].[claim] (\n"
        "    [claim id] BIGINT NOT NULL,\n"
        "    policy_no NVARCHAR(50) NOT NULL,\n"
        "    opened DATETIME2(7),\n"
        "    is_open BIT\n"
        ")\n")
    warehouse["payment.sql"] = (
        "CREATE TABLE dbo.payment (\n"
        "    id BIGINT IDENTITY(1,1) NOT NULL,\n"
        "    amount MONEY,\n"
        "    paid_on DATE,\n"
        "    CONSTRAINT pk_payment PRIMARY KEY NONCLUSTERED (id) NOT ENFORCED\n"
        ")\n")
    for extra in ("policy", "agent", "customer", "address", "product", "region"):
        warehouse[f"{extra}.sql"] = (
            f"CREATE TABLE dbo.{extra} (\n"
            f"    id BIGINT NOT NULL,\n"
            f"    name NVARCHAR(200)\n"
            ")\n")
    warehouse["v_open_claims.sql"] = (
        "CREATE VIEW dbo.v_open_claims AS\n"
        "SELECT TOP 100\n"
        "       [claim id],\n"
        "       ISNULL(policy_no, 'unknown') AS policy_no,\n"
        "       DATEDIFF(day, opened, GETDATE()) AS age_days,\n"
        "       IIF(is_open = 1, 'open', 'closed') AS status\n"
        "FROM dbo.claim\n"
        "ORDER BY opened\n")
    warehouse["v_agent_names.sql"] = (
        "CREATE VIEW dbo.v_agent_names AS\n"
        "SELECT id, 'Agent ' + name AS label FROM dbo.agent\n")
    for procedure in ("sp_load_claims", "sp_refresh_agg", "sp_purge"):
        warehouse[f"{procedure}.sql"] = (
            f"CREATE PROCEDURE dbo.{procedure} AS\n"
            "BEGIN\n"
            "    DECLARE @n INT;\n"
            "    SET @n = 0;\n"
            "    SELECT @n;\n"
            "END\n")
    _item(root, "AcmeDW", "Warehouse", warehouse)

    # --- lakehouses ------------------------------------------------------
    _item(root, "SalesLake", "Lakehouse", {
        "alm.settings.json": json.dumps(
            {"trackedObjectTypes": ["Shortcuts"]}, indent=2) + "\n",
        "shortcuts.metadata.json": json.dumps([
            {"path": "Tables", "name": "claims_raw_s3",
             "target": {"type": "AmazonS3", "amazonS3": {
                 "location": "https://acme-raw.s3.us-east-1.amazonaws.com",
                 "subpath": "/claims"}}},
            {"path": "Files", "name": "adls_landing",
             "target": {"type": "AdlsGen2", "adlsGen2": {
                 "location": "https://acmestore.dfs.core.windows.net",
                 "subpath": "/landing"}}},
            {"path": "Tables", "name": "shared_dim_date",
             "target": {"type": "OneLake", "oneLake": {
                 "workspaceId": "ws-shared", "itemId": "item-dims",
                 "path": "Tables/dim_date"}}},
        ], indent=2) + "\n"})

    # Tracking switched off -> coverage must read "unknown", never 0.
    _item(root, "ArchiveLake", "Lakehouse", {
        "alm.settings.json": json.dumps({"trackedObjectTypes": []}, indent=2) + "\n"})

    # --- pipeline and semantic model -------------------------------------
    _item(root, "Daily_Claims", "DataPipeline", {"pipeline-content.json": json.dumps({
        "properties": {"activities": [
            {"name": "RunIngest", "type": "TridentNotebook",
             "typeProperties": {"notebookName": "01_Ingest_Claims"}},
            # dependsOn is the interesting half of a pipeline: without it the
            # demo showed two tasks that both start at once, which is not what
            # the source pipeline does and not what this tool translates.
            {"name": "RunAggregates", "type": "TridentNotebook",
             "dependsOn": [{"activity": "RunIngest",
                            "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"notebookName": "02_Build_Aggregates"}},
            {"name": "RunReport", "type": "TridentNotebook",
             "dependsOn": [{"activity": "RunAggregates",
                            "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"notebookName": "03_Report_Claims"}},
        ]}}, indent=2) + "\n"})

    # --- dataflow (Power Query / M) --------------------------------------
    # Shapes taken from real Dataflow Gen2 exports: the destination is a
    # member attribute pointing at a sibling `*_DataDestination` query, and
    # the source table name lives in a navigation chain rather than on the
    # `Lakehouse.Contents` step itself.
    mashup = '''section Section1;

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_agent_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_agent = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "11111111-1111-1111-1111-111111111111"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  TableNavigation = Navigation_2{[Id = "agent", ItemKind = "Table"]}?[Data]?,
  #"Choose columns" = Table.SelectColumns(TableNavigation, {"agent_id", "first_name", "last_name", "region", "hired_on"}),
  #"Added full name" = Table.AddColumn(#"Choose columns", "full_name", each [first_name] & " " & [last_name]),
  #"Added hire year" = Table.AddColumn(#"Added full name", "hire_year", each Date.Year([hired_on])),
  #"Changed column type" = Table.TransformColumnTypes(#"Added hire year", {{"full_name", type text}, {"hire_year", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [region] <> null)
in
  #"Filtered rows";

shared dim_agent_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "11111111-1111-1111-1111-111111111111"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "22222222-2222-2222-2222-222222222222"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_agent", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;

shared load_cutoff = #date(2026, 1, 1);

[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ext_rates_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic"]]}]
shared ext_rates = let
  Source = Excel.Workbook(Web.Contents("https://example.invalid/rates.xlsx"), null, true),
  #"Promoted headers" = Table.PromoteHeaders(Source)
in
  #"Promoted headers";
'''
    _item(root, "Agents_Dim", "Dataflow", {
        "mashup.pq": mashup,
        "queryMetadata.json": json.dumps({"queriesMetadata": {
            "dim_agent": {"queryId": "3f1a2b4c-0001-4aaa-9bbb-000000000001",
                          "queryName": "dim_agent", "loadEnabled": True},
            "ext_rates": {"queryId": "3f1a2b4c-0002-4aaa-9bbb-000000000002",
                          "queryName": "ext_rates", "loadEnabled": True},
        }}, indent=2) + "\n"})

    _item(root, "Claims_Model", "SemanticModel", {})
    return root


def main() -> int:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else (
        Path(__file__).resolve().parent / "demo-workspace")
    build(target)
    print(f"wrote demo workspace to {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

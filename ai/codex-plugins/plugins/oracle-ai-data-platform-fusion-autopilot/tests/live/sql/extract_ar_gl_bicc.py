"""TC10h-7 prep — BICC extracts for AR (transactions, receipts) + GL (balances, COA, journals).

Run inside an AIDP notebook session attached to the `tpcds` cluster.
Source: demo-pod Fusion demo pod.
PVO names: live-confirmed against demo-pod BICC catalog 2026-04-30.

Output: bronze tables under fusion_catalog.bronze.*

Pattern mirrors the official Oracle AIDP sample notebook
(Read_Only_Ingestion_Connectors.ipynb) — uses the AIDP `aidataplatform` Spark
format handler with `fusion-bicc` connector.

Requires the bundle's BICC env vars set in the AIDP secrets (or pass inline):
  - FUSION_BICC_BASE_URL  — e.g. https://demo-pod-fa-ext.oracledemos.com
  - FUSION_BICC_USER      — <fusion-user>
  - FUSION_BICC_PASSWORD  — vault-managed
  - FUSION_BICC_EXTERNAL_STORAGE — name of BICC External Storage profile
"""
import os

CATALOG    = "fusion_catalog"
BRONZE     = "fusion_catalog.bronze"

EXTRACTS = [
    # AR
    {
        "id": "ar_invoices",
        "datastore": "FscmTopModelAM.FinExtractAM.ArBiccExtractAM.TransactionHeaderExtractPVO",
        "bronze_table": f"{BRONZE}.ar_invoices",
        "schema_offering": "Financial",
    },
    {
        "id": "ar_receipts",
        "datastore": "FscmTopModelAM.FinExtractAM.ArBiccExtractAM.ReceiptHeaderExtractPVO",
        "bronze_table": f"{BRONZE}.ar_receipts",
        "schema_offering": "Financial",
    },
    # GL
    {
        "id": "gl_period_balances",
        "datastore": "FscmTopModelAM.FinExtractAM.GlBiccExtractAM.BalanceExtractPVO",
        "bronze_table": f"{BRONZE}.gl_period_balances",
        "schema_offering": "Financial",
    },
    {
        "id": "gl_coa",
        "datastore": "FscmTopModelAM.FinExtractAM.GlBiccExtractAM.CodeCombinationExtractPVO",
        "bronze_table": f"{BRONZE}.gl_coa",
        "schema_offering": "Financial",
    },
    {
        "id": "gl_journal_headers",
        "datastore": "FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO",
        "bronze_table": f"{BRONZE}.gl_journal_headers",
        "schema_offering": "Financial",
    },
]

FUSION_URL  = os.environ["FUSION_BICC_BASE_URL"]
FUSION_USER = os.environ["FUSION_BICC_USER"]
FUSION_PWD  = os.environ["FUSION_BICC_PASSWORD"]
FUSION_ES   = os.environ["FUSION_BICC_EXTERNAL_STORAGE"]


def extract_one(spec: dict) -> int:
    print(f"\n=== Extracting {spec['id']}  ({spec['datastore']}) ===", flush=True)
    df = (
        spark.read.format("aidataplatform")
        .option("connector", "fusion-bicc")
        .option("fusion.url", FUSION_URL)
        .option("fusion.username", FUSION_USER)
        .option("fusion.password", FUSION_PWD)
        .option("fusion.external.storage", FUSION_ES)
        .option("datastore", spec["datastore"])
        .option("offering", spec["schema_offering"])
        .load()
    )
    n = df.count()
    print(f"  rows={n:,}  cols={len(df.columns)}", flush=True)
    df.write.format("delta").mode("overwrite").saveAsTable(spec["bronze_table"])
    print(f"  wrote {spec['bronze_table']}", flush=True)
    return n


if __name__ == "__main__":
    summary = {}
    for spec in EXTRACTS:
        try:
            summary[spec["id"]] = extract_one(spec)
        except Exception as e:
            print(f"  FAILED: {e}", flush=True)
            summary[spec["id"]] = -1
    print(f"\n=== Extract summary ===", flush=True)
    for k, v in summary.items():
        print(f"  {k:25s} = {v:>10,} rows" if v >= 0 else f"  {k:25s} = FAILED", flush=True)

# oracle-ai-data-platform-fusion-bundle

> **Productized Fusion → Oracle AI Data Platform pipeline.** Curated BICC extracts for Fusion ERP/HCM/SCM, bronze/silver/gold medallion in Delta, conformed COA/calendar/org/supplier/item dimensions, ready-made AR-aging / AP-aging / GL-balance / PO-backlog / Supplier-spend gold marts, and **Oracle Analytics Cloud (OAC) workbooks installable via OAC REST API**. End-user consumption via [OAC MCP (Preview)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html) chat in Claude / Cline / Copilot.
>
> Same pattern shown in the official Oracle blog [Bring Fusion Data into AIDP Workbench Using BICC](https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc), productized.

**Status**: alpha (`0.1.0a0`) — Tier-1 features complete and live-validated end-to-end against the saasfademo1 Fusion demo pod + `oacai.cealinfra.com` OAC instance (TC1, TC7, TC8, TC9, TC10/b/c/d/h/h-2 all PASS — see [tests/live/](tests/live/)). **132 unit tests passing.** OAC integration uses **only Oracle-documented public REST endpoints** (snapshot-based workbook delivery; the audit rejected per-workbook `.dva` imports as UI-only). CLI commands wired: `init`, `validate`, `bootstrap`, `catalog list/probe`, `run`, `status`, `dashboard install/validate/uninstall`, `dashboard mcp-config`.

**Positioning**: This bundle is **additive to and complementary with** Oracle's managed Fusion data offerings. It productizes Option 1 of the BICC blog's three-option architecture (BICC into AIDP for "Custom AI and ML, raw data access, data engineering"). Never positioned as a replacement for FDI, OAC, OTBI, BIP, or Data Transforms — different jobs, same Oracle ecosystem.

---

## What you get (per pdf1 §"What Can You Do Once the Data is in Oracle AI Data Platform")

1. **Custom ML/AI training** on operational ERP/HCM/SCM data (PySpark + Python in AIDP notebooks)
2. **Cross-source enrichment** — join Fusion data with non-Fusion sources via the AIDP `aidataplatform` connector family
3. **Medallion architecture** — bronze (raw audit) → silver (typed + dim-joined) → gold (business marts) in Delta
4. **GenAI agent grounding** — `ai_generate("which suppliers had >$1M Q1 spend?")` against gold marts via OCI Generative AI
5. **BI & reporting via JDBC** — OAC, Tableau, Power BI consume the gold layer
6. **Delta Sharing** (v3 roadmap) — share curated datasets with other teams or external partners

---

## Quickstart

```bash
# 1. Install the CLI on your laptop (development install from local source)
pip install -e .

# 2. Scaffold a bundle in your repo
mkdir my-fusion-lake && cd my-fusion-lake
aidp-fusion-bundle init

# 3. Probe prerequisites against your Fusion pod + AIDP workspace
aidp-fusion-bundle bootstrap --check-iam

# 4. Run the orchestrator (first time = full extract; subsequent = incremental)
aidp-fusion-bundle run --mode seed

# 5. Upload the bundle's snapshot .bar to your OCI Object Storage bucket
oci os object put --bucket-name aidp-fusion-bundle-bar \
                  --file ./bundle-v0.1.0a0.bar --name bundle-v0.1.0a0.bar

# 6. Install OAC connection + restore the snapshot (one-time per OAC instance)
aidp-fusion-bundle dashboard install --target oac \
  --oac-url https://your-oac.example.com \
  --bar-bucket aidp-fusion-bundle-bar --bar-uri bundle-v0.1.0a0.bar
# (See docs/oac_rest_api_setup.md for the full args + IAM/Resource Principal setup)

# 7. Print MCP config snippet for end users (paste into claude_desktop_config.json)
aidp-fusion-bundle dashboard mcp-config --oac-url https://your-oac.example.com \
  --oac-mcp-connect-js /path/to/oac-mcp-connect.js
```

After step 7, restart your AI client and ask "what's our AR aging?" — OAC MCP will route through `discoverData` → `describeData` → `executeLogicalSQL` against `fusion_catalog.gold.ar_aging`.

---

## Architecture

```
                                                  ┌──────────────────────────────┐
                                                  │   AIDP cluster (`tpcds`)     │
                                                  │   Spark Thrift JDBC endpoint │
                                                  │   schema=fusion_catalog.gold │
                                                  └──────────────┬───────────────┘
                                                                 │ JDBC
                                                                 ▼
┌─────────────────────────────────┐    REST API    ┌──────────────────────────────┐
│ aidp-fusion-bundle dashboard    │───────────────▶│       Oracle Analytics       │
│  install --target oac           │  (1) POST      │       Cloud (OAC)            │
│                                 │      /catalog/ │                              │
│  - POST /catalog/connections    │      conns     │  - data source: aidp_fusion  │
│  - POST /snapshots (.bar)       │  (2) POST      │  - workbooks: cfo_dashboard, │
│  - POST /system/.../restore     │      /snapshot │    ar_aging, ap_aging, ...   │
│  - GET  /workRequests/{id}      │      restore   │                              │
└─────────────────────────────────┘                └──────────────┬───────────────┘
                                                                 │ Logical SQL
                                                                 ▼
                                                  ┌──────────────────────────────┐
                                                  │   OAC MCP Server (Preview)   │
                                                  │   - discoverData             │
                                                  │   - describeData             │
                                                  │   - executeLogicalSQL        │
                                                  └──────────────┬───────────────┘
                                                                 │ MCP (stdio)
                                                                 ▼
                                                  ┌──────────────────────────────┐
                                                  │  End-user AI client          │
                                                  │  (Claude / Cline / Copilot)  │
                                                  │  "what's our AR aging?"      │
                                                  └──────────────────────────────┘
```

The bundle authors content in OAC (workbooks), captures it as a Custom snapshot (`.bar`) excluding per-customer secrets, ships the `.bar` as a release artifact, and installs via four documented public REST calls. End users consume via OAC MCP. AIDP serves the data via JDBC throughout.

---

## Curated PVO catalog (v1, ERP-Finance)

| Bundle id | Datastore | Source | Confirmed? |
|---|---|---|---|
| `erp_suppliers` | `FscmTopModelAM.SupplierExtractPVO` | pdf1 Step 3 | ✅ |
| `po_orders` | `FscmTopModelAM.PrcExtractPO` | pdf2 p2 default | ✅ |
| `scm_items` | `ItemExtractPVO` | pdf2 p2 default | ✅ |
| `hcm_worker_assignments` | `workerAssignmentExtracts` (saas-batch) | pdf2 p4 | ✅ (v2) |
| `gl_journal_lines` | `JournalLinesPVO` | placeholder | 🟡 verify-live |
| `gl_period_balances` | `GLBalancePVO` | placeholder | 🟡 verify-live |
| `gl_coa` | `ChartOfAccountsPVO` | placeholder | 🟡 verify-live |
| `ar_invoices` / `ar_receipts` / `ar_aging` | `AR*PVO` | placeholders | 🟡 verify-live |
| `ap_invoices` / `ap_payments` / `ap_aging` | `AP*PVO` | placeholders | 🟡 verify-live |
| `po_receipts` | `RcvShipmentLinePVO` | placeholder | 🟡 verify-live |

Run `aidp-fusion-bundle catalog probe --pod <url>` to reconcile placeholders against your live BICC console.

---

## Use cases

1. **New AIDP customer onboarding** — `bundle.yaml` with `examples/full_finance.yaml`, run orchestrator, walk away, return to a populated bronze + silver + gold + OAC workbooks.
2. **CFO demo in 30 minutes** — clone repo → `bootstrap` → `run --mode seed` → `dashboard install --target oac` → open OAC workbook → optionally chat via OAC MCP.
3. **Custom GenAI agents grounded on Fusion data** — `ai_generate("which suppliers had >$1M Q1 spend?")` against the bundle's curated gold marts via OCI Generative AI.
4. **Fusion-side of the SAP-modernization pattern** — Fusion data lands here; SAP data via parallel pipeline; both unified in AIDP gold layer.
5. **Build cross-source data products** — combine Fusion + Salesforce/Workday/S3/Postgres via the same `aidataplatform` connector family.
6. **Cross-module analytics** — order-to-cash health (AR × PO), commitments-vs-actuals (PO × GL), with conformed dimensions.
7. **Conformed dim reuse** — your existing AIDP notebooks join to `fusion_silver.dim_account` instead of re-deriving.
8. **Daily incremental refresh** — schedule the orchestrator as an AIDP job; bundle handles watermarks + Fusion's first-then-incremental BICC behavior.
9. **Fusion quarterly-update resilience** — schema-drift detection auto-evolves on adds, quarantines on remove/change.
10. **SOX-ready audit trail** — every load writes `_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`; Iceberg/Delta time-travel + audit columns satisfy auditors.
11. **Customer customizations** — extend `dim_account` for additional COA segments per `docs/customizing.md`; no fork needed.
12. **Pod migration** — change `fusion.serviceUrl` in `bundle.yaml`, re-run `seed`, bundle reloads everything against new pod.

---

## References

- **Plan**: [`oracle-ai-data-platform-fusion-bundle.md`](../../../../../.claude/plans/oracle-ai-data-platform-fusion-bundle.md)
- **Sibling plugin** (single-PVO connector skill): [`oracle-ai-data-platform-workbench-spark-connectors`](../oracle-ai-data-platform-workbench-spark-connectors/)
- **Official Oracle BICC blog**: https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc
- **Ateam saas-batch blog**: https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform
- **Official sample notebook**: [`Read_Only_Ingestion_Connectors.ipynb`](../../../data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb)
- **OAC MCP Preview**: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html
- **OAC MCP Server announcement**: https://blogs.oracle.com/analytics/oracle-analytics-cloud-mcp-server-bridging-enterprise-analytics-and-ai
- **Modernize SAP with AIDP + Fusion**: https://docs.oracle.com/en/solutions/modernize-sap-aidp-fusion/

---

## License

[MIT](LICENSE) © 2026 Ahmed Awan

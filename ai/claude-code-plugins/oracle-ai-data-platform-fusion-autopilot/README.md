# oracle-ai-data-platform-fusion-autopilot

> **Productized Fusion → Oracle AI Data Platform pipeline.** Curated BICC extracts for Fusion ERP/HCM/SCM, bronze/silver/gold medallion in Delta, conformed COA/calendar/org/supplier/item dimensions, ready-made AR-aging / AP-aging / GL-balance / PO-backlog / Supplier-spend gold marts, and **MCP-native Oracle Analytics Cloud (OAC) workbook authoring**. The legacy `.bar` REST install path still ships for snapshot-based deployments. End-user consumption via [OAC MCP (Preview)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html) chat in Claude / Cline / Copilot.
>
> Same pattern shown in the official Oracle blog [Bring Fusion Data into AIDP Workbench Using BICC](https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc), productized.

**Status**: alpha (`0.1.0a0`) — Tier-1 features complete and live-validated end-to-end against the demo-pod Fusion demo pod + multiple OAC instances. The bundle now uses a single content-pack execution path. **1360 unit + 12 architectural + 5 integration tests pass, plus the conversational skill family's own unit suites.** **Live-validated 2026-06-15** on the `fusion_autopilot_dev` cluster: a `mart-author` overlay seeded `gold.ar_invoice_summary` (49 rows) end-to-end, and OAC workbooks were created via the OAC MCP `save_catalog_content` write tool.

Primary CLI commands wired: `init`, `init-config`, `use-pack`, `validate`,
`bootstrap`, `catalog list/probe/probe-pvo`, `run`, `status`, `migrate-bundle`,
`content-pack list/info/validate`, `dashboard install/validate/uninstall`, and
`dashboard mcp-config/mcp-setup`. The supported Claude Code OAC setup path is
`dashboard mcp-setup`.

**Dashboard authoring is now MCP-native**: `oac-dataset-advisor` (intent → dataset, grounded in the live AIDP catalog) → `oac-dataset-setup` (guided manual OAC connection/dataset checkpoint + MCP verification) → `workbook-authoring` (generates schema-valid workbook JSON, writes via OAC MCP). The `.bar` `dashboard install` REST flow still ships as a legacy alternative.

Start with [docs/project_setup.md](docs/project_setup.md) for a fresh checkout
or customer bundle, then use [workflow.md](workflow.md) for the operator flow.
Use `/aidpf-error-triage` when a CLI run, bootstrap, validator, or diagnostic
artifact reports an `AIDPF-*` code; use
[docs/aidpf-error-codes.md](docs/aidpf-error-codes.md) for the full static
reference. The documentation map is [docs/README.md](docs/README.md).

**Positioning**: This bundle is **additive to and complementary with** Oracle's managed Fusion data offerings. It productizes Option 1 of the BICC blog's three-option architecture (BICC into AIDP for "Custom AI and ML, raw data access, data engineering"). Never positioned as a replacement for FDI, OAC, OTBI, BIP, or Data Transforms — different jobs, same Oracle ecosystem.

---

## What you get

1. **Custom ML/AI training** on operational ERP/HCM/SCM data (PySpark + Python in AIDP notebooks)
2. **Cross-source enrichment** — join Fusion data with non-Fusion sources via the AIDP `aidataplatform` connector family
3. **Medallion architecture** — bronze (raw audit) → silver (typed + dim-joined) → gold (business marts) in Delta
4. **GenAI agent grounding** — `ai_generate("which suppliers had >$1M Q1 spend?")` against gold marts via OCI Generative AI
5. **BI & reporting via JDBC** — OAC, Tableau, Power BI consume the gold layer
6. **Delta Sharing** (v3 roadmap) — share curated datasets with other teams or external partners

> **Current execution model (validated 2026-06-15)**:
> - **Single execution path** — bronze, silver, gold all dispatch
>   through the content-pack runner. The legacy `dimensions/dim_*.py`
>   + `transforms/gold/*.py` modules + the `--execution-backend` CLI
>   flag + the python_legacy adapter have been removed.
> - **Content pack ships at** `scripts/oracle_ai_data_platform_fusion_autopilot/content_packs/fusion-finance-starter/`
>   with per-file `bronze/<id>.yaml` (11 datasets), `silver/<id>.{yaml,sql}`
>   (3 dims), `gold/<id>.{yaml,sql}` (3 marts) — all customer-extensible
>   via overlay packs.
> - **OAC integration**: operator MCP setup, MCP-native workbook
>   authoring, `dashboard install` / `validate` / `uninstall` legacy
>   REST flow, live-validated on disposable OAC1.
> - **Customer extension**: `aidp-fusion-autopilot catalog probe-pvo
>   <id> --datastore X --bicc-schema Y --emit-pack-yaml <path>`
>   drafts a bronze YAML from a metadata-only BICC probe.
> - **1360 unit + 12 architectural + 5 integration tests pass.**

---

## Conversational skills (A-to-Z)

The plugin ships a family of Claude Code skills that drive the journey
conversationally — so a customer can go from a goal to live dashboards without
hand-running the CLI. The orchestrator routes through the rest:

| Skill | Role |
|---|---|
| **`aidp-fusion-autopilot`** | **Front door.** State a goal ("build a supplier-spend vs GL-balance dashboard"); it detects current state and drives the whole chain, pausing only for real decisions. |
| `aidp-fusion-config` | Resolve `aidp.config.yaml` coords from human-friendly names (no hand-copied OCIDs). |
| `aidp-fusion-bootstrap` | Guided bootstrap: validate prerequisites, run `bootstrap --check-iam`, pin tenant variation into `profiles/`, and route `AIDPF-2010/2011` to `medallion-author`. |
| `aidp-fusion-seed` | Natural-language → guarded `run --mode seed` (intent parse, precondition ladder, **fail-closed** destructive guard). |
| `aidp-fusion-incremental` | Natural-language → guarded `run --mode incremental` for day-2 refresh, with cursor, watermark, and drift-gate checks. |
| `aidp-fusion-status` | Read-only pipeline health — reconciles `fusion_autopilot_state` with the **live** catalog (HEALTHY / STALE / FAILED / DEFERRED / UNTRACKED …). |
| `aidpf-error-triage` | Read-only `AIDPF-*` failure router: explain the code, name the evidence, and hand off to the right recovery skill or command. |
| `fusion-drift-doctor` | Diagnoses schema/PVO/plan-hash drift gates and routes to bootstrap refresh, `medallion-author`, scoped reseed, or investigation. |
| `oac-dataset-advisor` | Dashboard intent → which OAC dataset to create, grounded in the **live AIDP gold layer** (never pack YAMLs). |
| `oac-dataset-setup` | Guided manual OAC AIDP connection/dataset creation, then MCP verification with `describe_data` before workbook authoring. |
| `mart-author` | When the gold layer can't serve a request, author a new mart additively (content-pack YAML+SQL overlay), inspecting the Fusion PVO source — never touching living delta. Wires the bundle via `use-pack`. |
| `medallion-author` | Tier-2 overlay for tenant variation (column aliases / semantic variants). |
| `incremental-mechanism` | Evidence-driven design aid for choosing key/timestamp/period-window incremental behavior for a node. |
| `workbook-authoring` | Generate schema-valid OAC workbook JSON and write it via OAC MCP. |
| `aidp-rest` | Internal/control-plane REST helper skill for OCI-signed AIDP workspace, cluster, and notebook-dispatch operations. |

`aidp-fusion-pipeline` remains the discovery/reference skill (positioning, gotchas,
when-NOT-to-use). The CLI stays the contract; skills are guarded wrappers around it.

---

## Quickstart

> **Conversational path:** state your goal to **`aidp-fusion-autopilot`** and it
> runs the steps below for you, pausing only for real decisions. The manual CLI
> quickstart here is what the autopilot automates. The full workflow is in
> [workflow.md](workflow.md). Fresh setup details are in
> [docs/project_setup.md](docs/project_setup.md).

**Route 1: Claude Code plugin.** Use this for normal customer/demo work. Run
these slash commands in Claude Code:

```text
/plugin marketplace add repo/oracle-ai-data-platform-fusion-autopilot
/plugin install oracle-ai-data-platform-fusion-autopilot@aidp-fusion-autopilot
```

> **First-run CLI provisioning (automatic).** Claude Code copies the plugin to a
> cache but never runs an install step, so the bundled `aidp-fusion-autopilot` CLI
> provisions itself on its **first invocation**: it installs its dependencies
> once into `~/.aidp-fusion-autopilot/pylib/<key>` (override the home with
> `AIDP_FUSION_HOME`) and runs from the plugin's own source thereafter. No
> `pip install` step is needed for the plugin route. Requirements:
> **Python ≥ 3.10 on `PATH`, with `pip` available** (the bootstrap installs via
> `<python> -m pip`; on minimal Debian/Homebrew/custom Pythons make sure pip is
> present). Set `AIDP_FUSION_NO_AUTOINSTALL=1` to opt out of
> the auto-install; the manual fallback it prints is
> `<your python ≥3.10> -m pip install --target <key dir> -r scripts/requirements.txt`
> (use that interpreter, not bare `pip`).

Create a clean customer project as a sibling of the plugin repo, then open
Claude Code from that customer directory:

```bash
cd Workspace
mkdir demo-fusion-cfo
cd demo-fusion-cfo
```

Then ask autopilot to drive the setup:

```text
/aidp-fusion-autopilot Build a CFO dashboard for supplier spend, AP aging, and GL balance using this Fusion tenant.
```

The skill installs/uses the bundled CLI if needed, scaffolds missing
`bundle.yaml`, `aidp.config.yaml`, and `.env`, then drives config, OAC MCP
setup, bootstrap, seed, dataset advice, and workbook authoring. It pauses for
secrets, Claude Code MCP reconnect, destructive seed confirmation, and the
manual OAC connection/dataset UI step.

**Route 2: manual CLI.** Use this when you want each command explicitly:

```bash
cd Workspace/oracle-ai-data-platform-fusion-autopilot
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

cd ..
mkdir demo-fusion-cfo
cd demo-fusion-cfo
aidp-fusion-autopilot init
aidp-fusion-autopilot init-config \
  --aidp-id <aidp-ocid> \
  --workspace "<workspace name>" \
  --cluster "<cluster name>"
aidp-fusion-autopilot validate
env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD \
  -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
  aidp-fusion-autopilot dashboard mcp-setup \
  --connector-js /path/to/oac-mcp-connect.js
aidp-fusion-autopilot bootstrap --check-iam
aidp-fusion-autopilot run --mode seed
```

Full first-run details, including the generated directory layout and OAC MCP
credential handling, are in [docs/project_setup.md](docs/project_setup.md).

Why the manual OAC step exists: OAC's public REST validator does not reliably
accept AIDP `idljdbc` first-connection creation, and OAC MCP can inspect and
save catalog content but does not expose dataset creation. See
[workflow.md](workflow.md#why-oac-connection-and-dataset-are-manual) for the
details.

Legacy snapshot deployment is still available with `dashboard install --target
oac` plus `.bar` restore. Use [docs/oac_rest_api_setup.md](docs/oac_rest_api_setup.md)
when you specifically need that path.

---

## Resuming an interrupted run (`--resume`)

A 25-minute pipeline can hit a transient BICC outage, a cluster auto-termination, or an operator Ctrl-C halfway through. Re-running from scratch eats ~14M row-writes and ~25 minutes of cluster time. `--resume` solves this — re-attempting only the failed/skipped steps under the original `run_id`.

```bash
# After an interrupted run, find the run_id you want to resume:
aidp-fusion-autopilot status      # surfaces the latest fusion_autopilot_state per dataset_id

# Resume by run_id. Normal laptop use dispatches to AIDP over REST; use
# --inline only from an AIDP notebook/runtime with Spark already available.
aidp-fusion-autopilot run --mode seed --resume <run_id>
```

What happens on resume:

- The orchestrator reads `fusion_autopilot_state` for `<run_id>`. Datasets whose latest terminal status is `success` or `resumed_skipped` carry forward without re-dispatch.
- All other datasets re-attempt under the **original `run_id`**, preserving the medallion `<layer>_run_id` audit invariant (one logical pipeline = one `run_id` across the resumed history).
- `preflight_bronze_schemas` only probes un-succeeded bronze nodes — already-succeeded schemas are pulled from the stored `plan_snapshot`.
- A drift gate compares the current plan + execution identity (Fusion pod URL, BICC storage, Fusion username, AIDP target paths, plugin version) against the stored hash. Any change raises `ResumeBundleMismatchError` pre-dispatch with the diff rendered: identity changes first, dataset changes second, hash echo last.
- Resume is supported on both normal REST dispatch and `--inline`; `--inline`
  is reserved for AIDP notebook/runtime sessions.

The state table becomes append-only on resumed runs — multiple rows per `(run_id, dataset_id)` are expected (failed attempt + carry-forward + eventual success). **Always read from the `fusion_autopilot_state_latest` Delta VIEW** (created automatically by `ensure_state_table`), which projects one row per `(run_id, dataset_id)` via `ROW_NUMBER() OVER (PARTITION BY run_id, dataset_id ORDER BY last_run_at DESC)`. See `LIMITS.md` §L-Resume for the full consumer-side contract.

If a `--resume` raises one of:

- `ResumeRunNotFoundError` — typo in run_id, or the state table was truncated.
- `ResumeRunNotResumableError` — the run predates fix21 (`plan_hash IS NULL`) or was written by a partially-migrated build (`plan_snapshot IS NULL`). The remediation is to re-run from scratch.
- `ResumeBundleMismatchError` — bundle drift. The error message names which identity field or dataset diverged.

…the CLI exits with code 2 and no traceback (all three classes subclass `OrchestratorConfigError`).

---

## Incremental refresh (`--mode incremental`)

`--mode seed` rebuilds bronze + silver + gold from scratch on every cycle —
full BICC extract, `CREATE OR REPLACE TABLE` where the content-pack strategy
requires it. Fine for a fresh-tenant first run; wasteful for a daily refresh
that touches the same large Fusion facts only a few thousand rows at a time.
Day-2 refresh uses `--mode incremental`:

- **Bronze** — BICC's `fusion.initial.extract-date` filter receives the prior run's safety-windowed watermark; the orchestrator `MERGE INTO bronze_target ... ON target.<natural_key> = src.<natural_key>` instead of `mode("overwrite")`. The overlap re-extracted by the safety window dedupes by natural key.
- **Silver `dim_supplier`, `dim_account` + Gold `gl_balance` (row-level)** — `MERGE INTO target USING (... WHERE bronze_extract_ts > <layer-local watermark>) ON target.<natural_key> = src.<natural_key>`. One bronze row changed → one silver/gold row updated.
- **Exempt marts (`supplier_spend`, `ap_aging`, `dim_calendar`)** — always run `CREATE OR REPLACE TABLE` regardless of mode. `supplier_spend`'s GROUP BY mixes a mutable fact attribute (`approval_status`) so partial-MERGE would leave stale aggregate rows on status flips. `ap_aging` buckets are `CURRENT_DATE()`-anchored — incremental MERGE would freeze the bucket assignment a row had on the last run, going stale by one day daily. `dim_calendar` is parameter-driven, no source watermark.

```bash
# First incremental run requires a prior --mode seed run to have populated each
# layer's last_watermark in fusion_autopilot_state. The orchestrator raises
# IncrementalCursorMissingError listing every silver/gold dataset that lacks one.
aidp-fusion-autopilot run --mode seed              # day 1
aidp-fusion-autopilot run --mode incremental       # day 2+
```

Prefer the conversational path? Use `/aidp-fusion-incremental`. If the run
fails with schema, PVO, fingerprint, or plan-hash drift, start with
`/aidpf-error-triage`; known drift gates route to `/fusion-drift-doctor`.

### Tuning the safety window — `bundle.incremental.watermark_safety_window_seconds`

The bronze cursor is stored as `extract_started_at − safety_window` (not `extract_started_at` directly) to absorb AIDP-vs-Fusion clock skew. Default is 3600s (one hour) — wider than typical NTP-synced drift between OCI-hosted AIDP and Fusion Cloud.

```yaml
# bundle.yaml — opt in only when needed
incremental:
  watermarkSafetyWindowSeconds: 7200   # widen to 2h if observed skew exceeds 1h
```

Validated `gt=0`. Setting `0` or a negative value is rejected at bundle load — those would erase the buffer or send a future-dated cursor to BICC.

### Clock-skew probe (per-tenant onboarding step)

Before flipping a new tenant to `--mode incremental`, run the TC28b clock-skew probe to confirm the safety window absorbs the observed skew comfortably. The probe is a single round-trip via `extract_pvo`:

```python
from datetime import datetime, timezone
from oracle_ai_data_platform_fusion_autopilot.extractors import bicc as bicc_mod
from oracle_ai_data_platform_fusion_autopilot.schema import fusion_catalog

pvo = fusion_catalog.get("erp_suppliers")
t0 = datetime.now(timezone.utc)
df = bicc_mod.extract_pvo(spark, pvo, fusion_service_url=..., username=..., password=..., fusion_external_storage=..., watermark=None)
_ = df.limit(1).count()
t1 = datetime.now(timezone.utc)
skew_seconds = (t1 - t0).total_seconds()
print(f"AIDP→BICC round-trip: {skew_seconds:.1f}s")
print(f"bundle.incremental.watermark_safety_window_seconds: {bundle.incremental.watermark_safety_window_seconds}")
assert skew_seconds < bundle.incremental.watermark_safety_window_seconds
```

If the assertion fails, widen `watermarkSafetyWindowSeconds` to comfortably exceed the observed skew before enabling incremental mode.

### Empty-delta + soft-fail operator playbook

Two cases land at the same place (preserved bronze cursor + a WARN-log marker):

- **Empty delta** — BICC's `fusion.initial.extract-date` filter returned zero rows. Expected and harmless on a no-op cycle (Fusion didn't change between runs). The bronze cursor is preserved (NOT advanced) so the next run picks up the same time window. State-table row is written with `status='success'` and the prior `last_watermark` value.
- **`watermark_read_soft_failed` WARN** — a transient metastore failure prevented reading the prior `fusion_autopilot_state` cursor. The orchestrator logs a structured WARN with the `watermark_read_soft_failed` marker key (set up alerts on this string) and proceeds with `prior_watermark=None`, falling back to a full extract for that node. Re-running the same `--mode incremental` command after the metastore recovers usually clears it. If the WARN persists across multiple runs, see `LIMITS.md §L6`.

Both signals show up in the orchestrator stdout under the same `[step]` line for the affected dataset — no separate audit table needed.

---

## Architecture

```
Fusion BICC PVOs
  -> AIDP bronze/silver/gold content-pack pipeline
  -> OAC AIDP connection + OAC dataset (guided by oac-dataset-setup, created manually in OAC UI)
  -> OAC workbook JSON authored by workbook-authoring
  -> OAC MCP save_catalog_content
  -> end-user OAC MCP chat in Claude / Cline / Copilot
```

The current dashboard path is MCP-native. `oac-dataset-advisor` grounds the
dataset recommendation in the live AIDP gold catalog; `oac-dataset-setup`
guides the manual OAC AIDP connection/dataset step and verifies it through MCP;
`workbook-authoring` binds workbook JSON to the dataset XSA reference and saves it via OAC MCP when
`save_catalog_content` is available.

The legacy `.bar` snapshot deployment remains available for packaged workbook
rollouts through documented OAC REST endpoints. See
[docs/oac_rest_api_setup.md](docs/oac_rest_api_setup.md) when that path is
explicitly needed.

---

## Curated PVO catalog (v1, ERP-Finance)

| Bundle id | Datastore | Source | Confirmed? |
|---|---|---|---|
| `erp_suppliers` | `SupplierExtractPVO` | Oracle BICC blog + live catalog | ✅ |
| `po_orders` | `PurchasingDocumentHeaderExtractPVO` | Oracle A-Team BICC pattern + live catalog | ✅ |
| `scm_items` | `ItemExtractPVO` | Oracle A-Team BICC pattern + live catalog | ✅ |
| `hcm_worker_assignments` | `workerAssignmentExtracts` (saas-batch) | Oracle A-Team saas-batch pattern | ✅ |
| `gl_journal_lines` | `JournalHeaderExtractPVO` | live BICC catalog | ✅ |
| `gl_period_balances` | `BalanceExtractPVO` | live BICC catalog | ✅ |
| `gl_coa` | `CodeCombinationExtractPVO` | live BICC catalog | ✅ |
| `ar_invoices` / `ar_receipts` | `TransactionHeaderExtractPVO` / `ReceiptHeaderExtractPVO` | live BICC catalog | ✅ |
| `ap_invoices` / `ap_payments` / `ap_aging_periods` | `InvoiceHeaderExtractPVO` / `PaymentHistoryDistributionExtractPVO` / `AgingPeriodHeaderExtractPVO` | live BICC catalog | ✅ |
| `po_receipts` | `ReceivingReceiptTransactionExtractPVO` | live BICC catalog | ✅ |

The table uses short display names. Run `aidp-fusion-autopilot catalog list` for
the exact AM-hierarchy datastore paths used by the CLI, and
`aidp-fusion-autopilot catalog probe --pod <url>` to reconcile the catalog against
the tenant's live BICC console.

---

## Use cases

### Supported now

1. **New AIDP customer onboarding** — create a customer bundle, run `bootstrap --check-iam`, seed the medallion layers, create the OAC AIDP connection/dataset, then author the workbook through OAC MCP.
2. **CFO demo path** — clone repo → set up OAC MCP with `dashboard mcp-setup` → `bootstrap` → `run --mode seed` → `oac-dataset-advisor` → `oac-dataset-setup` → `workbook-authoring` → open the OAC workbook → optionally hand off end-user MCP chat.
3. **Day-2 incremental refresh** — schedule `run --mode incremental` after the first seed; the bundle manages watermarks, BICC safety windows, exempt full-refresh marts, and cursor diagnostics.
4. **Fusion quarterly-update resilience** — route `AIDPF-*` failures through `aidpf-error-triage`; known schema/PVO/plan-hash drift routes to `fusion-drift-doctor`, bootstrap refresh, `medallion-author`, or scoped reseed.
5. **Customer custom marts** — use `mart-author` to add content-pack overlay YAML+SQL for tenant-specific analytics, then wire it with `use-pack` without forking the starter pack.
6. **Tenant semantic variation** — use `aidp-fusion-bootstrap` and `medallion-author` to pin profile evidence under `profiles/` and handle aliases or semantic variants additively.
7. **Custom GenAI agents grounded on Fusion data** — call OCI Generative AI from AIDP notebooks against curated gold marts, for example `ai_generate("which suppliers had >$1M Q1 spend?")`.
8. **SOX-ready audit trail** — every load writes `_extract_ts`, `_source_pvo`, `_run_id`, `_watermark_used`; Delta time travel plus audit columns supports reviewer evidence.

### Adjacent patterns

1. **Fusion-side of the SAP-modernization pattern** — Fusion data lands through this bundle; SAP data lands through a parallel pipeline; both unify in the AIDP gold layer.
2. **Cross-source data products** — combine Fusion gold marts with Salesforce, Workday, Amazon S3, Postgres, or other AIDP-connected sources in downstream AIDP notebooks and marts.
3. **Pod migration** — change `fusion.serviceUrl` in `bundle.yaml`, run bootstrap validation against the new pod, then reseed with a fresh run identity.

---

## References

- **Sibling plugin** (single-PVO connector skill): `oracle-ai-data-platform-workbench-spark-connectors`.
- **Official Oracle BICC blog**: https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc
- **Ateam saas-batch blog**: https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform
- **Official sample notebook**: `Read_Only_Ingestion_Connectors.ipynb` from the Oracle AIDP samples repository.
- **OAC MCP Preview**: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html
- **OAC MCP Server announcement**: https://blogs.oracle.com/analytics/oracle-analytics-cloud-mcp-server-bridging-enterprise-analytics-and-ai
- **Modernize SAP with AIDP + Fusion**: https://docs.oracle.com/en/solutions/modernize-sap-aidp-fusion/

---

## License

[MIT](LICENSE) © 2026 Oracle and/or its affiliates

---

## Credits

Built by the Oracle Forward Deployed Engineering (FDE) team:

- **Ahmed Awan** — Forward Deployed Engineer (FDE)
- **Oussama Lakrafi** — Forward Deployed Engineer (FDE)

Productized from the pattern in the official Oracle blog — [Bring Fusion Data into Oracle AI Data Platform Workbench Using BICC](https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc).

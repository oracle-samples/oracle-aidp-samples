---
name: aidp-fusion-pipeline
description: >-
  Guide Oracle Fusion ERP, HCM, or SCM ingestion into AIDP bronze, silver, and gold layers and onward to OAC analytics. Use when the user asks where to start, wants the full bundle workflow, or needs the correct focused Skill.
---

# `aidp-fusion-pipeline` — Fusion ERP/HCM/SCM → AIDP, batteries included

## Bundled CLI

Resolve `<plugin-root>` as the directory two levels above this `SKILL.md`.
On macOS/Linux, invoke `"<plugin-root>/bin/aidp-fusion-autopilot"`; on Windows,
invoke `"<plugin-root>\\bin\\aidp-fusion-autopilot.cmd"`. Command examples below
use `aidp-fusion-autopilot` as shorthand only—do not assume the plugin's `bin/`
directory is on `PATH`.


Productizes the official Oracle blog [Bring Fusion Data into Oracle AI Data Platform Workbench Using BICC](https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc) plus the ateam companion [How to Extract Fusion Data using Oracle AI Data Platform](https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform). The current path is: configure → connect OAC MCP → bootstrap → seed AIDP gold → advise the OAC dataset → use `oac-dataset-setup` for the governed manual OAC connection/dataset checkpoint → author the workbook via OAC MCP.

When a run, bootstrap, validation, dashboard, or workbook flow reports an
`AIDPF-*` code, start with `$aidpf-error-triage`.
It is read-only and routes the failure to the recovery skill that owns it.

## When to use

- User wants Fusion data in AIDP and asks "where do I start"
- User has BICC privileges and wants curated bronze/silver/gold layers without writing the pipeline
- User is preparing a CFO/analytics demo and needs OAC dashboards on Fusion data
- User wants to use [OAC MCP (Preview)](https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html) to chat with Fusion data via Codex/Cline/Copilot

## When NOT to use

- For a single one-off PVO read → use `$aidp-fusion-bicc` from the separate Oracle AIDP connector plugin.
- For Fusion REST queries with <50k rows → use `$aidp-fusion-rest` from that connector plugin.
- For EPM Cloud Planning data slices → use `$aidp-epm-cloud` from that connector plugin.
- For Essbase MDX → use `$aidp-essbase` from that connector plugin.

## Positioning

This bundle is **additive to and complementary with** Oracle's managed Fusion data offerings. It productizes Option 1 of pdf1's three-option architecture (BICC into AIDP for "Custom AI and ML, raw data access, data engineering"). Never positioned as a replacement for FDI, OAC, OTBI, BIP, or Data Transforms.

## What you get

Mirrors pdf1 §"What Can You Do Once the Data is in Oracle AI Data Platform":

1. **Custom ML/AI training** on operational ERP/HCM/SCM data (PySpark + Python in AIDP notebooks)
2. **Cross-source enrichment** — join Fusion data with non-Fusion sources via the `aidataplatform` connector family
3. **Medallion architecture** — bronze (raw audit) → silver (typed + dim-joined) → gold (business marts) in Delta
4. **GenAI agent grounding** — `ai_generate("which suppliers had >$1M Q1 spend?")` against gold marts via OCI Generative AI
5. **BI & reporting via JDBC** — OAC, Tableau, Power BI consume the gold layer
6. **Delta Sharing** (v3 roadmap) — share curated datasets with other teams or external partners

## Quickstart

> **Recommended route:** install the Codex plugin, open Codex from a
> clean customer bundle directory such as `Workspace/demo-fusion-cfo/`, then
> invoke `$aidp-fusion-autopilot` with the
> dashboard goal. Autopilot installs/uses the bundled CLI if needed, scaffolds
> customer files, and conducts the whole chain (configure → OAC MCP setup →
> bootstrap → seed → advise → dataset → workbook → optional MCP chat), pausing
> only for real decisions. Keep the customer directory separate from the plugin
> source. The manual quickstart below is the step-by-step path autopilot
> automates.

1. **Resolve the bundled CLI** as described in “Bundled CLI” above. The first
   invocation installs pinned third-party Python dependencies into plugin data;
   it does not modify the plugin package.

2. **Scaffold a bundle in your repo**:
   ```bash
   aidp-fusion-autopilot init
   ```
   Edits `bundle.yaml` and `aidp.config.yaml` to match your environment (Fusion pod URL, AIDP workspace, OAC URL, OCI Vault refs for credentials).

3. **Connect operator OAC MCP early**:
   ```bash
   env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
   aidp-fusion-autopilot dashboard mcp-setup \
     --connector-js <path to oac-mcp-connect.js>
   ```
   Run from the customer project directory. The `env -u ...` wrapper lets the
   local `.env` provide the OAC values instead of any global shell profile.
   Restart/reconnect Codex after this. Autopilot and workbook-authoring
   need OAC MCP for `search_catalog`, `describe_data`, and
   `save_catalog_content`. If setup happens mid-journey, autopilot writes
   `.aidp/autopilot/resume.md`; resume with: "Resume the Fusion dashboard
   workflow from .aidp/autopilot/resume.md."

4. **Probe prerequisites and pin tenant variation**:
   ```bash
   aidp-fusion-autopilot bootstrap --check-iam
   ```
   Prefer to drive this conversationally? Use
   `$aidp-fusion-bootstrap`. It confirms BICC role,
   BICC External Storage profile (set in BICC console), AIDP catalog, IAM policies,
   Vault access, and routes unresolved variation to `medallion-author`.

5. **Run the orchestrator**:
   ```bash
   aidp-fusion-autopilot run --mode seed     # first-time full extract
   aidp-fusion-autopilot run --mode incremental  # daily delta
   ```
   Prefer to drive this conversationally? `$aidp-fusion-seed`
   skill turns "seed", "seed supplier_spend", "seed just bronze", or "resume
   the seed" into the correct guarded `run --mode seed` invocation — it parses
   the scope, auto-satisfies preconditions (validate / `$aidp-fusion-bootstrap` / cluster),
   and **fail-closed-confirms** before overwriting populated silver/gold marts.
   If the run reports an `AIDPF-*` code, use
   `$aidpf-error-triage` before choosing a
   recovery path.

6. **Build dashboards (MCP-native — the current path).** Ask
   `$oac-dataset-advisor` what OAC dataset your
   goal needs (grounded in the **live** AIDP gold layer), use
   `$oac-dataset-setup` to guide the manual AIDP
   connection/dataset step and verify it through MCP, then have
   `$workbook-authoring` generate the
   visualization(s) and write them via the OAC MCP `save_catalog_content` tool.
   If the gold layer can't serve the goal,
   `$mart-author` authors a new mart (then `use-pack` +
   seed). *Legacy alternative:* the `.bar` snapshot `dashboard install` flow
   (snapshot register + restore via OAC REST) still ships — see
   `docs/oac_rest_api_setup.md` — but the MCP-native family above supersedes it
   for authoring.

7. **End users chat with the data** via OAC MCP. Set up the connector for
   Codex (non-interactive **basic auth**, the path that actually works in
   a terminal client):
   ```bash
   env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
   aidp-fusion-autopilot dashboard mcp-setup \
     --connector-js <path to oac-mcp-connect.js>
   ```
   Then ask "what's our AR aging?" and watch MCP call
   `search_catalog` → `describe_data` → `execute_logical_sql` against
   `fusion_catalog.gold.*`. **Scope the OAC user to least privilege** — the v1.4
   connector exposes catalog write/delete/ACL tools governed by that user's grants.

## Key gotchas

- **BICC role required** — Fusion user must hold `BIA_ADMINISTRATOR_DUTY` *or* `ORA_ASM_APPLICATION_IMPLEMENTATION_ADMIN_ABSTRACT`. Without it, `/biacm/api/v[12]/*` endpoints 302-redirect to IDCS. Bootstrap probes for this.
- **BICC External Storage profile** — must be configured **once in the BICC console** (admin task: BICC Console → Configure External Storage → OCI Object Storage Connection tab → bucket name + namespace + region + OCI username + auth token → Test Connection → Save). The `fusion.external.storage` Spark option references this BICC profile name. **There is no parallel AIDP-side registration.** Bundle does not provision the BICC profile; bootstrap verifies it exists.
- **First extract is slow** — BICC builds a full snapshot on first call; subsequent runs use `fusion.initial.extract-date` for incremental.
- **499 row/page hard cap on Fusion REST** (per MOS Doc ID 2429019.1) — bundle's REST fallback enforces this; anything >5k rows must use BICC.
- **OAC MCP (v1.4) is NOT read-only** — it exposes catalog **write** tools too. The bundle authors workbooks via `save_catalog_content` (live-verified 2026-06-15: created `gold_balance_2viz` on a real OAC). It still **cannot create datasets** (no create-dataset tool — dataset modeling is an OAC UI step), and the write/delete/ACL tools run with the connecting user's grants → use a least-privilege MCP user. (Supersedes the earlier "MCP is read-only" note.)
- **`POST /catalog/connections` REST validator does not bless AIDP `idljdbc`** — Oracle's validator falls through to generic Oracle DB schemas requiring `serviceName`/`password`/`connectionString`. The realistic flow is therefore: customer creates the connection via OAC UI once (using the 6-key JSON written by `--print-only`). Legacy `dashboard install` can re-use that connection via the precheck on subsequent `.bar` snapshot deployments.
- **Snapshot BAR URI shape is `file:///<folder>/<name>.bar`** — NOT `oci://...`, NOT bare object name, NOT the OCI Object Storage HTTPS URL.
- **OAC catalog browse needs `search=*`** — `GET /catalog?type=connections` (no search) returns a single-element TypeInfo header (`[{"type":"connections"}]`), NOT the actual list. Bundle's `list_connections` defaults `search="*"` so the precheck works.
- **Use ExtractPVOs for bulk, NOT OTBI reporting PVOs** — pdf1 Pro Tip; bundle's catalog refuses OTBI PVOs with a clear warning.

## References

- Companion connector capabilities are distributed separately; use the
  `$aidp-fusion-bicc`, `$aidp-fusion-rest`, `$aidp-epm-cloud`, or
  `$aidp-essbase` Skill when that plugin is installed.
- Official Oracle BICC blog: https://blogs.oracle.com/ai-data-platform/bring-fusion-data-into-oracle-ai-data-platform-workbench-using-bicc
- Ateam blog (saas-batch path): https://www.ateam-oracle.com/how-to-extract-fusion-data-using-oracle-ai-data-platform
- Official sample notebook: `oracle-aidp-samples/data-engineering/ingestion/Read_Only_Ingestion_Connectors.ipynb` (not bundled)
- OAC MCP Preview docs: https://docs.oracle.com/en/cloud/paas/analytics-cloud/acsdv/access-oracle-analytics-cloud-mcp-server-preview.html

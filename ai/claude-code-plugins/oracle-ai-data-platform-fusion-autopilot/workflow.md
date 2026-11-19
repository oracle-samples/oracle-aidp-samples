# Workflow

This plugin turns a business goal into a Fusion-backed analytics experience:

```text
Fusion BICC PVOs
  -> AIDP bronze/silver/gold content-pack pipeline
  -> OAC AIDP connection + dataset
  -> OAC workbook
  -> optional OAC MCP chat for end users
```

The preferred user entry point is `aidp-fusion-autopilot`. It conducts the
other skills and CLI commands, detects what is already done, and pauses only
where a human decision or external UI action is required.

## Current Execution Model

Phase 9 made the content-pack runner the single execution path. New bronze,
silver, and gold work is expressed as content-pack YAML/SQL, usually under an
overlay in `overlays/<name>/`. Do not add new legacy Python `dim_*.py` or gold
modules.

Every run-ready bundle needs:

- `bundle.yaml` with a `contentPack:` block.
- `aidp.config.yaml` with AIDP workspace and cluster coordinates.
- `profiles/<profile>.yaml`, created by `bootstrap`.
- A Fusion BICC user, BICC External Storage profile, and AIDP credential-store
  entry for the BICC password.

## Autopilot State Machine

Autopilot follows this order:

| Phase | Purpose | Driver | Stops for |
|---|---|---|---|
| 1. Config | Create/validate `bundle.yaml` and `aidp.config.yaml` | `aidp-fusion-autopilot init`, `/aidp-fusion-config` | Missing Fusion connectivity |
| 1b. OAC MCP connect | Make OAC tools available before OAC phases | `dashboard mcp-setup` | Claude Code restart/reconnect |
| 2. Bootstrap | Probe prerequisites and pin tenant variation | `/aidp-fusion-bootstrap` | Variation choices |
| 3. Seed | Materialize AIDP bronze/silver/gold | `/aidp-fusion-seed` | Destructive seed confirmation |
| 4. Advise dataset | Decide whether live gold covers the goal | `/oac-dataset-advisor` | True data gaps |
| 5. Author mart | Add missing analytical content when needed | `/mart-author`, then `use-pack` | Overlay approval |
| 6. OAC connection/dataset | Create the OAC data surface over AIDP gold | `/oac-dataset-setup` guiding OAC UI | Manual OAC action |
| 7. Workbook | Generate and save the workbook | `/workbook-authoring` | Replace existing workbook |
| 8. End-user MCP chat | Let downstream users query OAC from their clients | `docs/oac_mcp_setup.md` | Least-privilege OAC account |

Phase 1b is intentionally early. Autopilot and workbook authoring need OAC MCP
tools such as `search_catalog`, `describe_data`, and `save_catalog_content`.
After `dashboard mcp-setup`, Claude Code must restart or reconnect the MCP
server before those tools exist in the session. Doing that before bootstrap/seed
avoids stopping later in the OAC phases. Because restart can lose chat context,
autopilot must write `.aidp/autopilot/resume.md` before pausing.

Phase 8 is different from Phase 1b. Phase 1b equips the operator's Claude Code
session so autopilot can inspect and write OAC content. Phase 8 is the optional
handoff for end users who want their own Claude Desktop, Claude Code, Cline, or
Copilot client connected to OAC MCP.

## First-Time Setup

Recommended route: install the Claude Code plugin, open Claude Code from a
clean customer bundle directory, and let `aidp-fusion-autopilot` drive setup.

```text
/plugin marketplace add repo/oracle-ai-data-platform-fusion-autopilot
/plugin install oracle-ai-data-platform-fusion-autopilot@aidp-fusion-autopilot
```

Keep the customer implementation separate from the plugin source:

```bash
cd Workspace
mkdir demo-fusion-cfo
cd demo-fusion-cfo
```

Then start Claude Code from `demo-fusion-cfo/` and invoke:

```text
/aidp-fusion-autopilot Build a CFO dashboard for supplier spend, AP aging, and GL balance using this Fusion tenant.
```

Manual route: install the CLI from the plugin checkout, then scaffold the
customer project from the Phase 9 starter template:

```bash
cd Workspace/oracle-ai-data-platform-fusion-autopilot
pip install -e .

cd ..
mkdir demo-fusion-cfo
cd demo-fusion-cfo
aidp-fusion-autopilot init
```

Make sure the bundle uses the current content-pack shape. A starter bundle
declares:

```yaml
contentPack:
  name: fusion-finance-starter
  profile: finance-default
```

Resolve AIDP coordinates by name instead of hand-copying every key:

```bash
aidp-fusion-autopilot init-config \
  --aidp-id <aidp-or-datalake-ocid> \
  --workspace "<workspace display name>" \
  --cluster "<cluster display name>"
```

Fill the Fusion fields in `bundle.yaml`: `fusion.serviceUrl`,
`fusion.username`, `fusion.password`, and `fusion.externalStorage`. The BICC
External Storage profile is created in the Fusion BICC console, not by this
plugin.

Validate before touching live systems:

```bash
aidp-fusion-autopilot validate
```

## Phase 1b: Connect OAC MCP Early

Once the OAC URL and credentials are known, connect OAC MCP before continuing
to the long-running pipeline phases.

For Claude Code, use the supported setup command:

```bash
aidp-fusion-autopilot dashboard mcp-setup \
  --connector-js <path-to-oac-mcp-connect.js>
```

Then restart or reconnect Claude Code and verify that `oac-mcp-server` is
connected. Do not interpret a dead or unauthenticated MCP connection as "no OAC
dataset exists"; it is a connectivity failure, not catalog evidence.

Before stopping for the restart, write a non-secret resume checkpoint:

```bash
python3 skills/aidp-fusion-autopilot/write_resume_checkpoint.py \
  --workdir . \
  --goal "<dashboard goal>" \
  --phase "Step 1b OAC MCP connect" \
  --next-step "Reconnect Claude Code, verify oac-mcp-server, then resume autopilot" \
  --pending "OAC MCP liveness check"
```

After reconnect, paste:

```text
Resume the Fusion dashboard workflow from .aidp/autopilot/resume.md.
```

On resume, read the checkpoint, then re-probe live state. The checkpoint
preserves intent; it is not evidence that a dataset, workbook, or table exists.

Use a least-privilege OAC user. Connector v1.4 exposes catalog write, delete,
ACL, save, and export tools, and those tools run with the connecting user's
OAC grants.

## Bootstrap

Run bootstrap after config is valid, preferably through `/aidp-fusion-bootstrap`:

```bash
aidp-fusion-autopilot bootstrap
```

Bootstrap does two things:

- Probes prerequisites: bundle/config shape, BICC auth, BICC catalog
  reconciliation, AIDP REST reachability, and optional IAM checks.
- Resolves content-pack variation points against the tenant and writes
  `profiles/<profile>.yaml` plus the pinned schema snapshot.

If bootstrap cannot resolve a required column alias or semantic variant, route
to `/medallion-author`. That skill drafts an overlay extending the starter
pack's candidate lists. Bootstrap remains the only writer to `profiles/` and
`evidence/`.

## Seed AIDP Gold

First materialization is a seed:

```bash
aidp-fusion-autopilot run --mode seed
```

Normal laptop use dispatches a generated notebook to the AIDP cluster over
REST. Use `--inline` only inside an AIDP notebook/runtime where Spark and
AIDP runtime globals are already available.

Scoped examples:

```bash
aidp-fusion-autopilot run --mode seed --datasets supplier_spend
aidp-fusion-autopilot run --mode seed --layers bronze
aidp-fusion-autopilot run --mode seed --datasets ar_invoice_summary --layers gold
```

Seed can replace silver and gold targets. The conversational seed skill is
fail-closed: with the current CLI it cannot prove physical target tables are
empty from the laptop, so it requires confirmation before every real seed.
`fusion_autopilot_state` is run metadata, not proof that target tables are empty.

Preview a run without dispatching:

```bash
aidp-fusion-autopilot run --mode seed --dry-run
```

## Resume

If a run is interrupted, resume by run id:

```bash
aidp-fusion-autopilot run --mode seed --resume <run_id>
```

The resumed run reuses the original `run_id`, skips already-successful nodes,
and preserves the medallion audit invariant in `silver_run_id` and
`gold_run_id`.

## Incremental Refresh

After a successful seed, day-2 refresh uses:

```bash
aidp-fusion-autopilot run --mode incremental
```

Incremental requires prior watermarks in `fusion_autopilot_state`. If a node has
never been seeded, seed it first. If incremental fails with schema drift,
Fusion PVO drift, or plan-hash drift, route to `/fusion-drift-doctor`.

Common recovery routes:

- `AIDPF-2012`: bronze fingerprint drift, run `bootstrap --refresh`.
- `AIDPF-2072` or `AIDPF-4071`: Fusion PVO/source schema drift, diagnose with
  `/fusion-drift-doctor`, then `bootstrap --refresh` or `/medallion-author`.
- `AIDPF-4040`: plan-hash drift, usually re-seed the changed node or revert the
  unintended SQL/profile change.

When the code is not one of these common routes, start with
`/aidpf-error-triage`. For the full code table, see
`docs/aidpf-error-codes.md`.

## Advise the OAC Dataset

Before workbook authoring, use `/oac-dataset-advisor` to answer:

- Which live AIDP gold table(s) actually exist?
- Which columns satisfy the requested metrics, dimensions, and grain?
- Does an OAC dataset already exist over those tables?
- Is there a true gap that needs a new mart?

The advisor must use the live AIDP catalog as evidence. Content-pack YAML says
what could be built; it does not prove a table exists or that OAC can bind to
it.

If the live gold layer is empty but the pack can build the needed mart, seed
first. If neither live gold nor the pack can serve the request, use
`/mart-author`.

## Author a Missing Mart

When the advisor reports a true gap, `/mart-author` authors the smallest
additive change:

- Add a column to an existing node only if the grain/key stay unchanged.
- Add a new gold mart over existing bronze/silver when possible.
- Add new bronze only when the raw field is not extracted yet.
- Write only content-pack overlay YAML/SQL under `overlays/<name>/`.

For user-facing examples, including how to override shipped mart SQL, see
[docs/mart_overlay_authoring.md](docs/mart_overlay_authoring.md).

After authoring, validate and wire the overlay:

```bash
aidp-fusion-autopilot content-pack validate overlays/<name>
# --no-align keeps a narrow bundle / one-mart override as-is; the default
# --align broadens datasets/gold.marts to every node in the resolved pack
aidp-fusion-autopilot use-pack overlays/<name> --profile <profile>
aidp-fusion-autopilot validate
```

Then seed the new node and re-run the advisor against the live catalog.

## Why OAC Connection and Dataset Are Manual

The user must create the AIDP connection and the dataset in OAC manually today.
These are not optional paperwork; they are the OAC-side surface that workbooks
and MCP queries bind to.

### Why create the AIDP connection manually

OAC workbooks do not query the Delta tables directly from this plugin. They
query AIDP through an OAC connection, typically named `aidp_fusion_jdbc`, using
the AIDP JDBC/`idljdbc` connection shape.

The bundle can generate the six-key connection JSON:

```bash
aidp-fusion-autopilot dashboard install --target oac \
  --oac-url <oac-url> \
  --print-only \
  ...connection args...
```

But first-time connection creation should happen in the OAC UI:

```text
OAC -> Data -> Connections -> Create -> Oracle AI Data Platform
```

Reasons:

- OAC's public REST connection validator does not reliably accept the AIDP
  `idljdbc` connection type. It falls through to generic Oracle DB validation
  and asks for fields that do not apply to AIDP.
- The UI path is the supported path for the AIDP connector and accepts the
  generated JSON plus the private key PEM.
- After the connection exists, REST flows can reuse it by name through the
  precheck. The problematic first creation step is avoided.
- The connection is an external OAC object with credentials and grants. A human
  OAC admin should confirm where that credential lives and who can use it.

### Why create the OAC dataset manually

Workbook authoring binds to an OAC dataset or subject-area expression, not to a
raw Spark table path. The dataset gives the workbook an XSA reference such as:

```text
XSA('<dataset-uuid>'.'<dataset-name>')
```

Reasons:

- The OAC MCP connector can search, describe, query, and save catalog content,
  but it does not expose a create-dataset tool.
- The workbook generator needs the dataset metadata and XSA expression from
  OAC `describe_data`.
- Dataset creation is where the operator chooses the exact AIDP connection,
  gold table(s), columns, joins, and grants.
- For multi-table requests, the advisor should provide the table list, columns,
  and join key, but the OAC UI is where the dataset is actually modeled.
- This keeps OAC governance in OAC: users see only datasets and columns they
  are granted to see.

Create the dataset after the relevant gold table exists. The advisor should
hand the user a concrete spec:

```text
Dataset name: fusion_supplier_spend
Connection: aidp_fusion_jdbc
Table: fusion_catalog.gold.supplier_spend
Columns: vendor_id, supplier_name, currency_code, total_invoice_amount, invoice_count
```

For a multi-table dashboard, include each table and the join:

```text
Table A: fusion_catalog.gold.supplier_spend
Table B: fusion_catalog.gold.gl_balance
Join: currency_code = currency_code
```

Use `/oac-dataset-setup` for this checkpoint. It should turn the advisor
recommendation into exact OAC UI steps, pause while the operator creates the
connection/dataset, then use OAC MCP to find and describe the dataset before
handing off to `/workbook-authoring`.

## Workbook Authoring

Once the OAC dataset exists, `/workbook-authoring` generates workbook JSON,
runs deterministic validation, and saves via OAC MCP when
`save_catalog_content` is available.

The workbook authoring path needs:

- An OAC dataset or subject area.
- The dataset XSA expression and column metadata from `describe_data`.
- OAC MCP save capability, or a disk-only fallback when save is unavailable.

For update requests, workbook authoring should replace the resolved existing
workbook by id. For create requests, it should save a new workbook under the
chosen catalog folder.

## Returning User Fast Path

Autopilot is state-first. For a returning user asking for a dashboard over an
entity such as `dim_supplier` or `supplier_spend`, it should not redo completed
work.

Routing:

- If the profile is missing, bootstrap.
- If the live gold/silver table is missing, seed it.
- If the table exists but no OAC dataset exists, use `/oac-dataset-setup` to
  hand the user the OAC dataset spec and pause for the OAC UI action.
- If the dataset exists but no workbook exists, go straight to
  `/workbook-authoring`.
- If the workbook exists, return its path or `viewUrl` and ask whether the user
  wants to open, update, or create a new one.

Phase 1b still gates all OAC detection. If OAC MCP is down, fix the MCP
connection first; do not conclude that datasets or workbooks are absent.

## Optional End-User MCP Chat

After the workbook or dataset is available, end users can connect their own AI
clients to OAC MCP. Follow `docs/oac_mcp_setup.md`.

For Claude Code, the same non-interactive auth guidance applies:

```bash
aidp-fusion-autopilot dashboard mcp-setup \
  --connector-js <path-to-oac-mcp-connect.js>
```

Use least-privilege OAC accounts. OAC MCP v1.4 exposes write/delete/ACL tools
in addition to query tools, so permissions must be controlled by OAC grants.

## Safety Rules

- Do not claim a table exists from pack YAML. Use the live AIDP catalog.
- Do not claim an OAC dataset/workbook exists when MCP is disconnected.
- Before any MCP restart/reconnect pause, write `.aidp/autopilot/resume.md`.
- Do not seed populated targets without explicit confirmation.
- Do not bypass drift gates outside dev. Use `/fusion-drift-doctor`.
- Do not guess recovery from an unfamiliar `AIDPF-*` code. Use `/aidpf-error-triage`.
- Do not author new content in the shipped starter pack. Use overlays.
- Do not create or expose high-PII dashboard columns.
- Do not confuse operator MCP setup with end-user MCP rollout.

## Command Cheat Sheet

```bash
# Validate local config
aidp-fusion-autopilot validate

# Resolve AIDP config by names
aidp-fusion-autopilot init-config \
  --aidp-id <ocid> --workspace "<name>" --cluster "<name>"

# Set up operator OAC MCP for Claude Code, then restart/reconnect
aidp-fusion-autopilot dashboard mcp-setup \
  --connector-js <path-to-oac-mcp-connect.js>

# Bootstrap tenant variation
aidp-fusion-autopilot bootstrap

# Preview and run seed
aidp-fusion-autopilot run --mode seed --dry-run
aidp-fusion-autopilot run --mode seed

# Resume
aidp-fusion-autopilot run --mode seed --resume <run_id>

# Day-2 refresh
aidp-fusion-autopilot run --mode incremental

# Inspect and validate packs
aidp-fusion-autopilot content-pack list
aidp-fusion-autopilot content-pack info fusion-finance-starter
aidp-fusion-autopilot content-pack validate fusion-finance-starter

# Wire an overlay (add --no-align for a narrow bundle / one-mart override —
# keeps existing datasets/gold.marts instead of aligning to the full pack)
aidp-fusion-autopilot use-pack overlays/<name> --profile <profile>
```

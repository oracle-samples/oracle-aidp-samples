# Project Setup

Use this guide when starting from a fresh checkout or preparing a new customer
bundle. After setup is complete, continue with the operator workflow in
[../workflow.md](../workflow.md).

Scaffolded templates live under [../examples/](../examples/). For a first
customer project, `aidp-fusion-autopilot init` writes the current
[../examples/full-finance-starter/](../examples/full-finance-starter/) starter,
which enables the full tested finance starter scope by default.

## What You Are Setting Up

There are two working directories involved:

| Directory | Purpose |
|---|---|
| Plugin checkout | This repository. It provides the CLI, skills, content packs, and workbook tooling. |
| Customer bundle directory | A separate project created with `aidp-fusion-autopilot init`; it holds `bundle.yaml`, `aidp.config.yaml`, tenant profiles, evidence, and overlays. |

Keep them as siblings:

```text
Workspace/
  oracle-ai-data-platform-fusion-autopilot/   # plugin repo: CLI, skills, content packs
  demo-fusion-cfo/                         # customer/demo project: config + generated evidence
```

The plugin repo is the product. `demo-fusion-cfo/` is the customer
implementation. Do not author customer changes inside the plugin checkout or
inside the shipped starter pack. New customer medallion work belongs in overlays
under the customer bundle directory.

A clean customer bundle directory is empty, or at least has no existing
`bundle.yaml` or `aidp.config.yaml`; `aidp-fusion-autopilot init` writes those
files.

## Local Prerequisites

| Requirement | Why |
|---|---|
| Python 3.10+ | Required by `pyproject.toml`; tested with Python 3.10, 3.11, and 3.12 classifiers. |
| Node.js 18+ | Required by OAC MCP connector and workbook-authoring tools. |
| Git | Needed for a source checkout. |
| OCI CLI | Needed only for OCI-side checks, Object Storage work, or manual REST-dispatch troubleshooting. |
| Claude Code | Needed for the conversational skill/autopilot experience. |
| OAC MCP connector zip | Downloaded from OAC Profile -> MCP Connect. |

## Route 1: Claude Code Plugin

Recommended for users. Download the plugin, open Claude Code from a clean
customer bundle directory, and let `aidp-fusion-autopilot` drive setup and
delivery.

Run these slash commands in Claude Code:

```text
/plugin marketplace add repo/oracle-ai-data-platform-fusion-autopilot
/plugin install oracle-ai-data-platform-fusion-autopilot@aidp-fusion-autopilot
```

Then create or open the customer project as a sibling of the plugin repo:

```bash
cd Workspace
mkdir demo-fusion-cfo
cd demo-fusion-cfo
```

Start Claude Code from `demo-fusion-cfo/`, not from the plugin repo, and ask
autopilot to drive the work:

```text
/aidp-fusion-autopilot Build a CFO dashboard for supplier spend, AP aging, and GL balance using this Fusion tenant.
```

Autopilot will install/use the bundled CLI if `aidp-fusion-autopilot` is not on
`PATH`, scaffold missing customer files with `aidp-fusion-autopilot init`, route
config through `/aidp-fusion-config`, stage OAC MCP when needed, run
validation/bootstrap/seed, advise the OAC dataset, and hand off workbook
authoring. It pauses only for real external actions: secrets and tenant values,
Claude Code MCP restart, destructive seed confirmation, and the manual OAC
connection/dataset step.

## Route 2: Manual CLI Setup

Use this path when you want each command explicitly, or when you are developing
the plugin itself.

### Install The Plugin CLI

From this repository:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e .
```

For contributor/test work:

```bash
pip install -e '.[test]'
make test
```

Smoke-check the CLI:

```bash
aidp-fusion-autopilot --help
aidp-fusion-autopilot content-pack list
aidp-fusion-autopilot content-pack info fusion-finance-starter
```

## External Prerequisites

Before a real bootstrap or seed, confirm these exist:

| System | Required setup |
|---|---|
| Fusion BICC | Fusion user with BICC privileges. |
| Fusion BICC | External Storage profile configured in the BICC console. |
| AIDP | AI Data Platform OCID, workspace, and cluster. |
| AIDP | Credential-store entry for the Fusion BICC password. Default name: `fusion_bicc_password`; default key: `password`. |
| OAC | OAC instance where the AIDP connection, dataset, and workbook will live. |
| OAC | User for operator MCP setup; use least privilege, especially because OAC MCP v1.4 exposes write/delete/ACL tools. |

Create the AIDP credential-store entry before the first bootstrap or REST
dispatch. The cluster notebook reads it with
`aidputils.secrets.get(name=<biccSecretName>, key=<biccSecretKey>)`; by default
that means name `fusion_bicc_password` and key `password`. If you change
`biccSecretName` or `biccSecretKey` in `aidp.config.yaml`, the AIDP credential
store must use the same values.

For more REST job dispatch details, including AIDP credential-store setup, see
[rest_dispatch_setup.md](rest_dispatch_setup.md).

## Create A Customer Bundle

If you are using Route 1, create/open this directory and let
`/aidp-fusion-autopilot` run `aidp-fusion-autopilot init` when it detects missing
customer files. If you are using Route 2 manually, run the scaffold yourself.

Create the customer project outside the plugin checkout. For example:

```bash
# From Workspace/oracle-ai-data-platform-fusion-autopilot after installing the CLI:
cd ..
mkdir demo-fusion-cfo
cd demo-fusion-cfo
aidp-fusion-autopilot init
```

After init, the customer folder has:

```text
demo-fusion-cfo/
  bundle.yaml
  aidp.config.yaml
  .env
```

The default scaffold uses the content-pack shape and enables the full starter
finance scope:

```yaml
contentPack:
  name: fusion-finance-starter
  profile: finance-default
gold:
  marts:
    - supplier_spend
    - ap_aging
    - gl_balance
```

Resolve AIDP workspace and cluster coordinates:

```bash
aidp-fusion-autopilot init-config \
  --aidp-id <aidp-ocid> \
  --workspace "<workspace-name>" \
  --cluster "<cluster-name>"
```

If you do not know where to get the AIDP/Data Lake OCID, workspace name, or
cluster name, use the `/aidp-fusion-config` skill instead of guessing. It guides
the setup from human-friendly AIDP values and writes the resolved
`aiDataPlatformId`, `workspaceKey`, and `clusterKey` into `aidp.config.yaml`.

Then validate:

```bash
aidp-fusion-autopilot validate
```

Fill the generated files before bootstrap:

- `.env`: `FUSION_BICC_BASE_URL`, `FUSION_BICC_USER`,
  `FUSION_BICC_PASSWORD`, `FUSION_BICC_EXTERNAL_STORAGE`, `OAC_URL`,
  `OAC_MCP_USER`, and `OAC_MCP_PASSWORD`.
- `bundle.yaml`: project name, team, schemas, catalog, and any scoped starter
  marts that differ from the default.
- `aidp.config.yaml`: prefer `aidp-fusion-autopilot init-config`; do not
  hand-copy opaque workspace or cluster keys unless you must.

## Configure Operator OAC MCP

Set up OAC MCP before the OAC phases of autopilot:

```bash
env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD \
  -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
  aidp-fusion-autopilot dashboard mcp-setup \
  --connector-js <path-to-oac-mcp-connect.js>
```

Then restart or reconnect Claude Code and verify `oac-mcp-server` is connected.
Do not treat a disconnected MCP server as proof that an OAC dataset or workbook
does not exist.

Run this from the customer bundle directory. The command writes project-scoped
`.mcp.json` in `demo-fusion-cfo/`; OAC credentials stay under
`~/.oac-connect/` and must not be committed.

If MCP setup happens after the user has already stated a dashboard goal, write
the resume checkpoint before restarting:

```bash
python3 skills/aidp-fusion-autopilot/write_resume_checkpoint.py \
  --workdir . \
  --goal "<dashboard goal>" \
  --phase "OAC MCP reconnect required" \
  --next-step "Reconnect Claude Code, verify oac-mcp-server, then resume autopilot"
```

After reconnect, paste:

```text
Resume the Fusion dashboard workflow from .aidp/autopilot/resume.md.
```

Full setup and troubleshooting details are in
[oac_mcp_setup.md](oac_mcp_setup.md).

## Bootstrap

Run bootstrap after the bundle config and OAC MCP setup are ready:

```bash
aidp-fusion-autopilot bootstrap --check-iam
```

Bootstrap probes prerequisites and pins tenant variation into:

```text
profiles/<contentPack.profile>.yaml
```

If bootstrap reports an `AIDPF-*` code, use `/aidpf-error-triage` for
conversational routing, or [aidpf-error-codes.md](aidpf-error-codes.md) for the
full static reference.

## First Seed

Preview first:

```bash
aidp-fusion-autopilot run --mode seed --dry-run
```

Then run seed only after confirming the target is safe to populate:

```bash
aidp-fusion-autopilot run --mode seed
```

The seed skill is intentionally fail-closed because the current CLI cannot
prove a physical target is empty in every environment.

After bootstrap and seed, the customer project grows like this:

```text
demo-fusion-cfo/
  bundle.yaml
  aidp.config.yaml
  .env
  .mcp.json
  profiles/
    finance-default.yaml
  evidence/
  .aidp/
    autopilot/
      resume.md
  overlays/              # only if mart-author or medallion-author creates custom work
```

For a live demo, open Claude Code from `demo-fusion-cfo/`, not from the plugin
repo, so skills and CLI commands operate on the customer bundle by default.
Then use a prompt such as:

```text
/aidp-fusion-autopilot Build a CFO dashboard for supplier spend, AP aging, and GL balance using this Fusion tenant.
```

## OAC Connection And Dataset

After the needed AIDP gold table exists, use `/oac-dataset-setup` to guide the
manual OAC data-surface step.

First, generate the AIDP connection JSON:

```bash
aidp-fusion-autopilot dashboard install --target oac \
  --oac-url <oac-url> \
  --print-only \
  ...connection args...
```

Then in OAC:

```text
Data -> Connections -> Create -> Oracle AI Data Platform
```

Upload the generated JSON and private key PEM, then create the dataset over the
advised AIDP gold table(s). `/oac-dataset-setup` should verify the saved dataset
with OAC MCP before workbook authoring begins.

This step is manual for two reasons:

- OAC's public REST validator does not reliably accept first-time AIDP
  `idljdbc` connection creation.
- OAC MCP can search, describe, query, and save catalog content, but it does
  not expose a create-dataset tool.

The full explanation is in
[../workflow.md](../workflow.md#why-oac-connection-and-dataset-are-manual).

## Workbook Authoring

After `/oac-dataset-setup` verifies the OAC dataset, resume autopilot or use
`workbook-authoring`.
The skill should:

- find the dataset with OAC MCP,
- call `describe_data`,
- bind workbook JSON to the dataset XSA reference,
- save through `save_catalog_content` when the OAC user has that capability.

See [oac_workbook_authoring_e2e.md](oac_workbook_authoring_e2e.md) for the
binding and save mechanics.

## Day-2 Refresh

After a successful seed:

```bash
aidp-fusion-autopilot run --mode incremental
```

If a run is interrupted:

```bash
aidp-fusion-autopilot status
aidp-fusion-autopilot run --mode seed --resume <run_id>
```

Common drift and failure codes are documented in
[aidpf-error-codes.md](aidpf-error-codes.md).

## Setup Checklist

- Plugin CLI installed with `pip install -e .`.
- `aidp-fusion-autopilot content-pack list` works.
- Customer bundle created with `aidp-fusion-autopilot init`.
- `bundle.yaml` has a `contentPack` block.
- `aidp.config.yaml` resolves the AIDP workspace and cluster.
- Fusion BICC user and External Storage profile exist.
- AIDP credential-store entry for the Fusion BICC password exists
  (`fusion_bicc_password` / key `password`, unless overridden in
  `aidp.config.yaml`).
- OAC MCP connector is staged with `dashboard mcp-setup`.
- Claude Code has been restarted or reconnected and `oac-mcp-server` is live.
- `bootstrap --check-iam` completes.
- Seed dry-run is reviewed before real seed.
- OAC AIDP connection and dataset are created manually.
- Workbook authoring can describe the dataset and save catalog content.

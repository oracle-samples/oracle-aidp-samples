---
name: oac-dataset-setup
description: "Guide the manual Oracle Analytics Cloud dataset checkpoint for the Fusion AIDP bundle. Consume an `oac-dataset-advisor` recommendation, produce exact OAC UI steps for creating or reusing the AIDP connection and dataset over live AIDP gold tables, pause while the operator creates the dataset, then verify it with OAC MCP `search_catalog`, `find_matching_datasources`, and `describe_data` before handing off to `/workbook-authoring`. Use when the user says 'create the OAC dataset', 'set up the OAC dataset', 'I created the dataset, continue', 'verify this OAC dataset', 'dataset missing before workbook', or autopilot reaches the manual OAC connection/dataset phase. Does not create OAC datasets, does not author workbooks, and does not decide whether gold covers a dashboard request."
allowed-tools: Read, Bash, Glob, Grep, mcp__oac-mcp-server__oracle_analytics-search_catalog, mcp__oac-mcp-server__oracle_analytics-find_matching_datasources, mcp__oac-mcp-server__oracle_analytics-describe_data
---

# oac-dataset-setup - guided OAC dataset checkpoint

This skill owns the manual OAC data-surface step between
`/oac-dataset-advisor` and `/workbook-authoring`.

It does three things:

1. Turns the advisor's dataset recommendation into exact OAC UI instructions.
2. Pauses while the operator creates or updates the AIDP connection/dataset in OAC.
3. Verifies the resulting dataset through OAC MCP before workbook authoring.

It does not create the OAC connection or dataset automatically. Current OAC MCP
can search, describe, query, and save catalog content, but it does not expose a
create-dataset tool. First-time AIDP connection creation is also an OAC UI
step because the public REST validator does not reliably accept the AIDP
`idljdbc` connection type.

## When to use

- `/oac-dataset-advisor` returned a CREATE recommendation and the user needs OAC UI steps.
- Autopilot reaches the "OAC connection/dataset" pause.
- The user says they created a dataset and wants to continue.
- The user wants to verify an OAC dataset before `/workbook-authoring`.
- A workbook request is blocked because the target dataset is missing or ambiguous.

## When NOT to use

- Deciding whether live gold can serve the dashboard -> `/oac-dataset-advisor`.
- Building missing gold content -> `/mart-author`, then seed and re-run advisor.
- Creating the workbook -> `/workbook-authoring`.
- Setting up OAC MCP itself -> run the project-scoped `dashboard mcp-setup`
  command from the customer project directory.
- Running seed/incremental to materialize data -> `/aidp-fusion-seed` or `/aidp-fusion-incremental`.

## Required inputs

Start from an advisor recommendation. If the user has not provided one, run or
ask them to run `/oac-dataset-advisor` first.

The minimum dataset spec is:

```text
Dataset name:
OAC AIDP connection name:
Gold table or tables:
Columns to expose:
Join keys, when multi-table:
Filters or grants:
```

For single-table datasets, require a fully qualified gold table such as
`fusion_catalog.gold.supplier_spend`.

For multi-table datasets, require each table, the join key, and which columns
come from which table. If the join key is missing or weak, stop and route back
to `/oac-dataset-advisor` or `/mart-author`; do not invent a relationship.

## Workflow

### 1. Confirm the upstream recommendation

Before giving UI steps, restate the plan:

- dataset name,
- OAC connection name,
- gold table names,
- selected columns,
- join keys,
- filters and access grants,
- columns intentionally excluded for PII or governance.

If the recommendation says REUSE, skip creation and go to verification. If it
says GAP, do not create a dataset; route to `/mart-author` or seed first based
on the advisor's explanation.

### 2. Confirm OAC MCP is available for verification

Probe OAC MCP before and after manual work. A dead MCP connection is a
connectivity failure, not evidence that datasets are absent.

Use a cheap catalog search if the tool is available:

```text
oracle_analytics-search_catalog(type="datasets", search="*")
```

If OAC MCP is not connected, tell the user to run the supported setup path:

```bash
env -u OAC_URL -u OAC_MCP_USER -u OAC_MCP_PASSWORD -u OAC_ADMIN_USER -u OAC_ADMIN_PASSWORD \
aidp-fusion-autopilot dashboard mcp-setup \
  --connector-js <path-to-oac-mcp-connect.js>
```

Run it from the customer project directory so `.mcp.json` is wired there and
the local `.env` supplies `OAC_URL`, `OAC_MCP_USER`, and `OAC_MCP_PASSWORD`.
The `env -u ...` wrapper prevents a global shell OAC profile from overriding
the project `.env`.

Before stopping, write the resume checkpoint so the dataset plan survives the
restart:

```bash
python3 skills/aidp-fusion-autopilot/write_resume_checkpoint.py \
  --workdir . \
  --goal "<dashboard goal>" \
  --phase "OAC dataset setup - MCP reconnect required" \
  --next-step "Reconnect Claude Code, verify oac-mcp-server, then resume /oac-dataset-setup" \
  --pending "Verify OAC dataset with search_catalog and describe_data"
```

Then restart or reconnect Claude Code and paste:

```text
Resume the Fusion dashboard workflow from .aidp/autopilot/resume.md.
```

### 3. Prepare the AIDP connection instructions

If the OAC AIDP connection already exists, confirm the name and continue.

If it does not exist, generate or ask the user to generate the connection JSON:

```bash
aidp-fusion-autopilot dashboard install --target oac \
  --oac-url <oac-url> \
  --connection-name <connection-name> \
  --print-only \
  ...connection args...
```

Then give the OAC UI steps:

```text
OAC -> Data -> Connections -> Create -> Oracle AI Data Platform
```

Tell the operator to upload the generated connection JSON and private key PEM,
test the connection, save it with the planned connection name, and grant access
only to the users or groups that should build/read the dataset.

Do not use REST or MCP to create the first AIDP connection. Do not paste private
keys or connection secrets into chat.

### 4. Give exact OAC dataset creation steps

For a single-table recommendation:

```text
OAC -> Data -> Datasets -> Create Dataset
Connection: <connection-name>
Table: <catalog>.<gold-schema>.<table>
Columns: <advisor column list>
Save as: <dataset-name>
```

For a multi-table recommendation:

```text
OAC -> Data -> Datasets -> Create Dataset
Connection: <connection-name>
Tables:
- <catalog>.<gold-schema>.<table_a>
- <catalog>.<gold-schema>.<table_b>
Join:
- <table_a>.<key> = <table_b>.<key>
Columns:
- <table_a>: <columns>
- <table_b>: <columns>
Save as: <dataset-name>
```

Include these operator notes when relevant:

- Select only the columns recommended by the advisor.
- Keep high-PII columns out of the dataset unless explicitly approved.
- Set aggregation defaults for obvious measures, but do not invent business logic.
- Preserve column names when possible so workbook authoring can match them.
- If OAC shows unexpected data types or missing columns, stop and return to advisor/status instead of forcing the model.
- Apply OAC grants before declaring the dataset ready.

Pause after the steps:

```text
Create or update the dataset in OAC now. When it is saved, tell me the exact dataset name and catalog path if OAC shows one.
```

Before pausing for the OAC UI action, update `.aidp/autopilot/resume.md` with
the dataset name, connection name, table list, and next verification step. Do
not include connection secrets, private keys, or generated connection payloads.

### 5. Verify the saved dataset through OAC MCP

After the user says the dataset exists, verify it. Do not rely only on the
user's statement if MCP is available.

Search by exact name first:

```text
oracle_analytics-search_catalog(type="datasets", search="<dataset-name>")
```

If needed, use natural-language matching:

```text
oracle_analytics-find_matching_datasources(query="<business request or dataset name>")
```

If there are multiple candidates, list them and ask the user to choose the
exact catalog item. Do not guess.

Describe the selected dataset:

```text
oracle_analytics-describe_data(dataset="<resolved dataset id or path>")
```

Confirm:

- dataset name and catalog path or id,
- XSA expression or subject-area reference, when available,
- required columns are present,
- expected measures and dimensions are visible,
- no excluded high-PII columns were accidentally exposed,
- the connecting OAC user can see the dataset.

If a required column is missing, tell the user exactly which column is missing
and return to the OAC dataset edit step. If the dataset cannot be described
because of permissions, fix OAC grants before workbook authoring.

### 6. Hand off to workbook authoring

When verification passes, emit a compact handoff:

```text
dataset: Fusion CFO Gold Demo
path/id: <resolved catalog path or id>
subject area: XSA('<dataset-uuid>'.'<dataset-name>')
columns verified: supplier_name, currency_code, total_invoice_amount, ...
next: /workbook-authoring
```

Then call or recommend `/workbook-authoring` with the dataset name/path and the
original business dashboard request.

## Output modes

Use `CREATE` when the dataset does not exist yet:

```text
CREATE OAC dataset
connection: aidp_fusion_jdbc
dataset: Fusion CFO Gold Demo
tables: fusion_catalog.gold.supplier_spend
columns: supplier_name, currency_code, total_invoice_amount, invoice_count
pause: create it in OAC UI, then return for MCP verification
```

Use `REUSE` when a verified dataset already exists:

```text
REUSE OAC dataset
dataset: Fusion CFO Gold Demo
path/id: <resolved path or id>
verified by: describe_data
next: /workbook-authoring
```

Use `BLOCKED` when setup cannot safely continue:

```text
BLOCKED
reason: OAC MCP is disconnected
next: run the project-scoped dashboard mcp-setup command, reconnect Claude Code, then resume
```

## Safety invariants

- Never claim the OAC dataset or AIDP connection was created by the agent unless a tool returns proof.
- Never treat a dead or unauthenticated OAC MCP server as an empty OAC catalog.
- Never use pack YAML as proof that OAC can bind to a table; dataset setup depends on advisor live evidence.
- Never recommend exposing high-PII columns by default.
- Never paste private keys, OAuth tokens, passwords, or full connection payloads into chat.
- Do not proceed to `/workbook-authoring` until `describe_data` confirms the required dataset columns or MCP is explicitly unavailable and the user accepts disk-only fallback risk.

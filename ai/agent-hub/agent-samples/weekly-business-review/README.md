# Weekly Business Review SQLTool + A2UI v0.9 Agent

A high-code Oracle AIDP example that turns governed sales and customer-success data into an interactive Weekly Business Review. The agent queries five uploaded tables through predefined, read-only SQLTools, reasons across the returned results, and renders an A2UI v0.9 dashboard with filters, KPI cards, charts, tables, insights, recommendations, and follow-up actions.

The repository includes synthetic CSV data. It does not include a database, OCI credentials, tenancy identifiers, or a deployed Agent Hub configuration.

## Agent card description

> A SQLTool-backed Weekly Business Review agent for an enterprise SaaS product portfolio. It analyzes sales pipeline, subscription renewals, product adoption, and customer-support health, then produces interactive A2UI dashboards with filters, charts, account-level insights, risks, and recommended actions.

## How it works

```text
User request
    |
    v
OCI-hosted reasoning model
    |
    v
Six predefined AIDP SQLTools
    |
    v
Structured response plan
    |
    v
Python validation and A2UI rendering
```

The model decides which tools are needed and synthesizes the business narrative from their results. It does not generate or execute arbitrary SQL. Python validates the response plan, asks the model to repair an invalid plan, and only then converts the approved plan into A2UI operations.

The source tables contain weekly snapshots. When a request spans multiple weeks, each SQLTool selects the latest complete weekly snapshot inside the requested range and returns its reporting dates. Snapshot balances are not added across weeks.

When data is unavailable or no rows match the requested scope, the agent reports that state explicitly. It does not substitute fabricated fallback data.

## Capabilities

- Conversational questions and concise text responses
- Date, region, and segment filtering
- Executive KPI summaries
- Pipeline-by-stage analysis and charting
- Renewal-risk account prioritization
- Product-adoption and usage analysis
- Support-health and escalation analysis
- Cross-dataset insights and recommended actions
- Interactive A2UI follow-up buttons
- Diagnostic IDs for model, SQLTool, planning, and rendering failures

## Repository layout

```text
weekly-business-review-github/
|-- agent.py                     # Deployment entrypoint and complete agent
|-- requirements.txt             # Additional Python dependency
|-- test_agent_contract.py       # Offline regression and contract tests
|-- .env.example                 # Configuration names with placeholder values
|-- a2ui/                        # A2UI v0.9 manager, parser, schemas, and catalogs
|-- sample_data/                 # Synthetic CSV tables for the example
|-- THIRD_PARTY_NOTICES.md       # A2UI source and schema provenance
`-- LICENSES/                    # Third-party license text
```

## Sample customers

The sample data uses fictional, anonymized organizations:

- Account A
- Account B
- Account C
- Account D
- Account E
- Account F
- Account G
- Account H

## Prerequisites

- An Oracle AIDP workspace with Agent Hub access
- A catalog and schema available to the AIDP SQLTool connection
- Compute and read permissions for the uploaded tables
- Access to an OCI Generative AI model
- An Agent Hub renderer that supports the included A2UI catalog components

The AIDP runtime is expected to provide `aidputils`, LangChain, and LangGraph. `requirements.txt` lists the extra package used directly by this example.

Schema validation is required and fails closed. Deployments must install the declared minimum `jsonschema` and `referencing` versions; the agent will return an explicit rendering error if validation cannot run.

## Load the sample data

Upload each file in `sample_data/` as a table with the matching filename:

| File | Table |
|---|---|
| `weekly_business_metrics.csv` | `weekly_business_metrics` |
| `pipeline_by_stage.csv` | `pipeline_by_stage` |
| `renewal_risk_accounts.csv` | `renewal_risk_accounts` |
| `product_usage_by_account.csv` | `product_usage_by_account` |
| `support_health_by_account.csv` | `support_health_by_account` |

Preserve `week_start` and `week_end` as Oracle `DATE` or `TIMESTAMP` columns.

The tables cover weekly KPIs, pipeline by sales stage, renewal-risk accounts, product usage by account, and support health by account. `account_id` is stable across the renewal, usage, and support tables so the agent can reason across customer risk signals without generating arbitrary SQL.

## Configure the agent

The agent requires five deployment values:

| Value | Purpose |
|---|---|
| `WBR_CATALOG_KEY` | AIDP catalog containing the Weekly Business Review tables |
| `WBR_SCHEMA_KEY` | Schema containing the Weekly Business Review tables |
| `OCI_TENANCY_OCID` | OCI tenancy or compartment identifier accepted by your AIDP model runtime |
| `OCI_INFERENCE_ENDPOINT` | Endpoint for your OCI Generative AI deployment |
| `OCI_MODEL_ID` | Model identifier available in that deployment and region |

No concrete OCI identifier, endpoint, model, catalog, or schema is included in this repository.

To find the OCI identifier, sign in to the OCI Console, open **Identity & Security**, select **Compartments**, choose the compartment used by the AIDP deployment, and copy the identifier required by your model runtime. The environment-variable name follows this example's runtime contract; confirm whether your environment expects a tenancy or compartment OCID.

To find the inference endpoint and model, open the OCI Generative AI service, select the region and model deployment used by the agent, copy the inference endpoint, and copy the exact model identifier. Confirm that the endpoint region matches the deployed model.

To find the AIDP catalog and schema, open **Master catalog** in AIDP Workbench, select the catalog containing the uploaded CSV tables, expand the catalog, and select the schema. Confirm that all five required tables are visible and accessible to the deployed agent principal and compute.

Set these values in the deployment environment or the approved Agent Hub configuration mechanism:

```text
WBR_CATALOG_KEY=<catalog containing the five tables>
WBR_SCHEMA_KEY=<schema containing the five tables>
OCI_TENANCY_OCID=<your OCI tenancy OCID>
OCI_INFERENCE_ENDPOINT=<insert your OCI inference endpoint>
OCI_MODEL_ID=<insert your deployed model ID>
```

Do not commit real OCIDs, API keys, tokens, passwords, or client secrets. Use the AIDP Credential Store for secrets when a connected service requires them.

## Deploy to Agent Hub

Upload the following while preserving the `a2ui/` directory:

```text
agent.py
requirements.txt
a2ui/
```

## Validate locally

Install the declared dependencies and run the contract suite from this directory:

```bash
python3 -m pip install -r requirements.txt
python3 -m unittest -v test_agent_contract.py
```

The tests cover required schema validation, deterministic A2UI serialization, current-turn tool errors, latest-snapshot SQL semantics, reporting dates, and explicit action scope.

## Example prompts

Start by verifying the SQLTool connection:

> What Weekly Business Review regions, segments, and date ranges are available?

Then test the complete experience:

> Show the complete weekly business review for North America Enterprise from 2026-07-27 to 2026-08-02. Highlight the top risks and recommend three actions.

Additional coverage:

- `Show pipeline by stage for North America Enterprise from 2026-07-27 to 2026-08-02.`
- `Which renewal-risk accounts need executive attention for North America Enterprise from 2026-07-27 to 2026-08-02?`
- `Compare product usage and support health for North America Enterprise from 2026-07-27 to 2026-08-02. Which accounts need intervention?`
- `Show the complete review for EMEA Enterprise from 2026-07-27 to 2026-08-02.`
- `Show the complete review for EMEA Commercial from 2025-01-01 to 2025-01-07.`

The last prompt demonstrates the no-results path.

## Troubleshooting

If the agent reports missing configuration, add the listed SQL or model environment variables and redeploy.

If you see `AgentSetupFailed`, confirm the entry file, upload structure, required environment variables, and availability of the selected model.

For `AIDP_SQL_TOOL_CONNECTION_DATA_FETCH_ERROR`, verify the catalog connection, compute, permissions, and deployment-principal access.

For `AIDP_SQL_TOOL_QUERY_EXECUTION_ERROR`, confirm the table and column names and verify that `week_start` and `week_end` are available as `DATE` or `TIMESTAMP` values.

For model failures, confirm that the endpoint is valid for the selected region and that the model identifier is available to the deployment.

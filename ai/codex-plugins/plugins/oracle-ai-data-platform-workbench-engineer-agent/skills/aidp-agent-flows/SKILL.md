---
name: aidp-agent-flows
description: Discover, author, deploy, and run AIDP agent flows. Use when the user wants to list/inspect agent flows, or create/update/deploy/run a flow, manage sessions/guardrails, attach compute, or attach a remote MCP server to a flow (an MCP_TOOL node — AIDP's "Native MCP Client Support" LA feature, where the flow connects OUT to OAC/ADW/OIC/any MCP-compatible service). Everything runs over the Limited-Availability AgentFlows REST API via `oci raw-request`; verify live first. (An `aidp` MCP `list_agent_flows`, if configured, is an optional read accelerator only.)
---

# `aidp-agent-flows` — agent flow lifecycle

Discover, author, deploy, and run agent flows through the AIDP REST `AgentFlows` API via
`oci raw-request`. No MCP and no `ai-data-engineer-agent` repo are required.

> **CLI gap (no invented commands):** the official `aidp` CLI v1.0.0 has **no** agent-flow group (the
> Python SDK ships `agent_flow` models only). Agent-flow CRUD / deploy / run / sessions / guardrails all
> stay on the **REST API (LA `20240831`, may be unprovisioned)**. Do not assume an `aidp agent-flow`
> command exists — see [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md).

> **LA + verify-first (no-fabrication):** AgentFlows is **Limited Availability** on `20240831` (not in the
> GA `aiwap/` docs but still active). **Path is workspace-scoped:** `…/dataLakes/<ocid>/workspaces/<ws>/agentFlows`
> returned **200** live (2026-06-10, `<TENANCY>`); the *lake-level* `…/dataLakes/<ocid>/agentFlows` (no
> workspace) **404s** — that earlier 404 was the wrong path, not necessarily missing provisioning. Still
> confirm the route with a live read before any write (provisioning can vary by tenancy), and record results
> in `references/rest-endpoint-map.md`. Frame the whole lifecycle as LA, not GA.

## When to use
- "List/show agent flows" (read). "Create/update/deploy/run a flow", "manage flow sessions/guardrails",
  "attach compute to a flow" (write).
- "Connect my flow/agent to a remote MCP server / OAC MCP / ADW Select AI MCP / an MCP tool" → add an
  `MCP_TOOL` node (see the Native MCP Client Support section below).

## Auth + base URL
Use the auth ladder and base URL in [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md):
`https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<dataLakeOcid>/…`, `--profile DEFAULT`
(api_key); on 401/403/"Security Token" refresh `AIDP_SESSION` and retry with `--auth security_token`.

## Read / discover (REST — primary)
- `GET …/workspaces/<ws>/agentFlows` — enumerate flows (**workspace-scoped**; live **200** on `<TENANCY>`
  2026-06-10). `GET …/workspaces/<ws>/agentFlows/<key>` — flow detail for authoring.
- A **404** usually means you dropped the `…/workspaces/<ws>/` segment (the lake-level `…/agentFlows`
  doesn't exist) — re-check the path first; if the workspace-scoped path also 404s, AgentFlows isn't
  provisioned in this tenancy (see note above) — surface that rather than assuming the flow is missing.
- *Optional accelerator:* if an `aidp` MCP happens to be configured, `list_agent_flows` is a convenience
  read over the same control-plane. It is **not required** and not the primary path.

## Write (REST, LA `20240831`, via `oci raw-request`)
All flow/session paths are **workspace-scoped** (`…/dataLakes/<ocid>/workspaces/<ws>/…`); guardrails are lake-scoped.
- `…/workspaces/<ws>/agentFlows` (create / get / update / delete); `…/agentFlows/<key>/permissions`,
  `…/agentFlows/<key>/sessionMetrics`. Validate the definition before deploy.
- **Deploy is a workspace action:** `POST …/workspaces/<ws>/actions/deployAgentFlow` (and
  `…/actions/redeployAgentFlow`) — async (`202` + operation key); attach AI compute in the body.
- `…/workspaces/<ws>/agentFlowSessions` (create / run sessions); `…/agentFlowSessions/<key>/chatHistory`,
  `…/agentFlowSessions/<key>/traces/<traceKey>`.
- `…/agentFlowGuardrails` — **lake-scoped** (`…/dataLakes/<ocid>/agentFlowGuardrails`, not under a workspace).

## Attach a remote MCP server to a flow — Native MCP Client Support (LA)
AIDP's LA **Native MCP Client Support** lets a flow call **out** to a remote MCP server (OAC `/api/mcp`,
ADW Select AI MCP, OIC, or any MCP-compatible service) by adding an **`MCP_TOOL` node** to the flow.
AIDP is the **client** here — it does **not** host an MCP server (all standalone `/mcp` endpoints 404).
This is distinct from the optional **local-stdio `aidp` MCP** that can back this plugin's skills
(see [references/mcp-tool-map.md]($HOME/.aidp/references/mcp-tool-map.md)) — different thing, different direction.

> **Verified live 2026-06-10:** `GET …/workspaces/<ws>/agentFlows` → **200** on the `<TENANCY>` tenancy
> (IAD). That `agentFlows` collection is the surface that holds `MCP_TOOL` nodes; there is no separate
> `/mcp` path. Provisioning still varies by tenancy (a prior env returned 404 — see the LA note above), so
> read first.

**`MCP_TOOL` node shape** (official SDK `CreateMcpToolNodeDetails` → wire JSON; lives in the flow's node graph):
```json
{
  "type": "MCP_TOOL",
  "name": "oac-mcp",
  "description": "Read KPIs from OAC via MCP",
  "positionX": 0, "positionY": 0,
  "toolConfig": {
    "endpoint": "https://<oac-host>/api/mcp",
    "auth": { "authType": "OAUTH" },
    "allowedTools": [
      { "instruction": "use for KPI lookups", "argOverrides": {}, "tool": { "__": "McpToolObject from the fetch step" } }
    ],
    "customHeaders": {}
  },
  "toolKey": null, "inputSchema": {}, "configuration": {}
}
```
- **`toolConfig.endpoint`** — the remote MCP URL to connect to *(verified field)*.
- **`toolConfig.auth.authType`** ∈ **`NO_AUTH` · `BEARER_TOKEN` · `OAUTH` · `OCI_RESOURCE_PRINCIPAL`**
  *(verified enum)*. For `BEARER_TOKEN`, carry the token via `customHeaders`
  (`{"Authorization":"Bearer <…>"}`) or the matching auth subtype — **confirm the exact per-auth credential
  fields against the live API / SDK auth subtype before authoring; never inline a secret into a committed file**
  (prefer the credential store — see `aidp-credentials`).
- **`toolConfig.allowedTools[]`** — restrict which remote tools the flow may call:
  `{ instruction, argOverrides, tool }` *(verified shape)*.
- **`toolConfig.customHeaders`** — extra request headers (`dict[str,str]`).

**Introspect + test the remote MCP first** (bind real tool names, not guesses):
- **Fetch the MCP's objects** — `FetchMcpObjectsDetails { agentFlowId, type ("Tool"|"prompt"|"resource"),
  mcpTool, limit, page, paramValues }` → lists the tools/prompts/resources the remote MCP exposes; use the
  returned `McpToolObject`s to populate `allowedTools`.
- **Test connection / a tool** — `TestMcpConnection` / `TestMcpExternalTool { testType, externalToolName,
  paramValues }` before deploy.

**Author it** (REST via `oci raw-request`, LA `20240831`): create/update the flow with the `MCP_TOOL` node in
its node list at `…/workspaces/<ws>/agentFlows[/<key>]`, `validate`, then deploy per the Workflow below.
Persist the node payload to `.aidp/payloads/` and confirm first.

> **Verify-first (no-fabrication):** the `toolConfig` / `auth` / `allowedTools` field **names** above are
> from the official SDK models (`CreateMcpToolNodeDetails`, `McpToolConfiguration`, `Auth`,
> `AllowedToolDetails`) — confirmed. The full flow node-graph wrapper and the per-`authType` credential
> fields should be confirmed by round-tripping an existing flow (`GET …/agentFlows/<key>`) before a first
> write, and recorded in `rest-endpoint-map.md`.

## Publish a flow as an A2A agent (agent card) — outbound discovery (LA)
A2A publishing exposes a **deployed** flow as an agent that **external** agents can discover and call. This is
the **outbound-publishing** direction and is distinct from the two MCP/agent directions above:

| Mechanism | Direction | Role |
|---|---|---|
| `EXTERNAL_AGENT` node | **inbound** — your flow calls another agent | flow is the caller |
| `MCP_TOOL` node | **outbound** — your flow connects out to a remote MCP server | flow is an MCP **client** |
| **Agent card (A2A)** | **outbound publishing** — external agents discover + call your flow | flow is the **published agent** |

**Set it on the flow body**, not as a node: the create/update flow body carries a top-level
`agentCardConfig` field (wire `agentCardConfig`, type `AgentCardConfigDetail` — confirmed on
`create_agent_flow_details.py:99`, `update_agent_flow_details.py:97`, and read back on `agent_flow.py:186`).

**`AgentCardConfigDetail`** (verified wire fields — `agent_card_config_detail.py:60-68`):
```json
{
  "agentCardConfig": {
    "name": "supplier-spend-agent",
    "description": "Answers supplier-spend questions over the gold lakehouse",
    "version": "1.0.0",
    "documentationUrl": "https://…",
    "capabilities": { "isStreaming": true },
    "provider": { "organization": "Oracle", "url": "https://…" },
    "skills": [
      { "id": "spend_lookup", "name": "Supplier spend lookup",
        "description": "Returns spend by supplier", "tags": ["finance"], "examples": ["top 10 suppliers by spend"] }
    ]
  }
}
```
| Field | Wire | Type / shape |
|---|---|---|
| `name` *(required)* | `name` | str — human-readable agent name |
| `description` | `description` | str |
| `skills[]` | `skills` | `AgentCardSkillDetail { id*, name*, description, tags[], examples[] }` (`agent_card_skill_detail.py:50-56`) |
| `capabilities` | `capabilities` | `AgentCardCapabilitiesDetail { isStreaming }` bool (`agent_card_capabilities_detail.py:30-32`) |
| `version` | `version` | str |
| `provider` | `provider` | `AgentProvider { organization, url }` (`agent_provider.py:35-38`) |
| `documentationUrl` | `documentationUrl` | str |

**Preview the generated card before publishing** — the SDK exposes a preview action:
`PreviewAgentFlowAgentCardDetails { agentCardConfigDetails: AgentCardConfigDetail }` →
`AgentCardPreviewResponse { agentCardJson }` (a JSON-string A2A AgentCard)
(`preview_agent_flow_agent_card_details.py:30-34`, `agent_card_preview_response.py:30-32`). Use this to inspect the
exact A2A card the platform will publish before committing the flow update.

**Author it** (REST via `oci raw-request`, LA `20240831`): set `agentCardConfig` on the create/update flow body at
`…/workspaces/<ws>/agentFlows[/<key>]`, then deploy per the Workflow below (a card describes a **deployed** flow).
Persist the body to `.aidp/payloads/` and confirm first.

> **CLI gap + verify-first (no-fabrication):** the `agentCardConfig` field and all child field **names/types** above
> are from the SDK models cited inline — confirmed. There is **no** `aidp` CLI agent-card command (the v1.0.0 CLI
> README has no agent-card/a2a verb). The exact create/update **wrapper** (whether `agentCardConfig` rides inside
> `CreateAgentFlowDetails`/`UpdateAgentFlowDetails` as shown vs. a dedicated publish action) and the **route** that
> serves the published card to external agents are **verify-first** — confirm against a live
> `GET …/workspaces/<ws>/agentFlows/<key>` round-trip and record in `rest-endpoint-map.md` before a first write.

## Node types
A flow is a node graph; the 13 node types (START / AGENT / SUPERVISOR_AGENT / NESTED_AGENT_FLOW /
EXTERNAL_AGENT / HUMAN_IN_THE_LOOP / GUARDRAILS / SQL_TOOL / PROMPT_TOOL / RAG_TOOL / MCP_TOOL / HTTP_TOOL /
CUSTOM_TOOL) and their config shapes are enumerated in
[references/agent-flow-nodes.md]($HOME/.aidp/references/agent-flow-nodes.md). A tool node can inline its `toolConfig`
or reference a reusable standalone Tool by `toolKey` (`aidp-tools`).

## Author guardrails (safety policies) — LA
Guardrails are **lake-scoped** (`…/dataLakes/<ocid>/agentFlowGuardrails`, **live 200** 2026-06-10) and attach
to a flow as a `GUARDRAILS` node.

> **Defaults already there:** every fresh DataLake auto-provisions **5 default guardrail policies**, so a
> `GET …/agentFlowGuardrails` on a new instance returns 5 items (not 0). The defaults (verified 2026-06-12):
> | `policyType` | `scope` | `action` |
> |---|---|---|
> | `CONTENT_MODERATION` | `USER_REQUEST` | `BLOCK` |
> | `CONTENT_MODERATION` | `AGENT_RESPONSE` | `BLOCK` |
> | `PROMPT_ATTACKS_PREVENTION` | `USER_REQUEST` | `BLOCK` |
> | `PII_DETECTION` | `AGENT_RESPONSE` | `INFORM` |
> | `PII_DETECTION` | `USER_REQUEST` | `INFORM` |
> These are a security-first baseline — don't mistake them for leftover/test data, and account for them when
> diffing or round-tripping guardrails.

A `SafetyPolicy` (verified enums):
```json
{ "policyType": "PII_DETECTION", "policyName": "block_pii", "policyDescription": "…",
  "scope": "BOTH", "action": "BLOCK", "threshold": 0.8 }
```
- `policyType` ∈ `CONTENT_MODERATION` · `PROMPT_ATTACKS_PREVENTION` · `PII_DETECTION` · `DENIED_TOPICS` ·
  `WORD_FILTERS` · `CONTEXTUAL_GROUNDING` · `CUSTOM_POLICY`
- `scope` ∈ `USER_REQUEST` · `AGENT_RESPONSE` · `BOTH`  ·  `action` ∈ `BLOCK` · `INFORM` · `MASK`  ·  `threshold` float

**Per-`policyType` config (subtype models extend the base `SafetyPolicy`)** — each subtype carries the base
six fields above **plus** the extra child fields below. The 5 subtype models are `ContentModerationPolicy`,
`PromptAttacksPreventionPolicy`, `PiiDetectionPolicy`, `DeniedTopicsPolicy`, `WordFiltersPolicy` (`CONTEXTUAL_GROUNDING`
and `CUSTOM_POLICY` are base-only — no extra fields confirmed in the SDK):

| `policyType` (subtype model) | Extra wire fields | Element shape (child model) |
|---|---|---|
| `CONTENT_MODERATION` (`ContentModerationPolicy`) | `categories[]` | `ContentModerationCategoryConfig { category, isEnabled, threshold, action }` — `category` ∈ `HATE_SPEECH`·`HARASSMENT`·`VIOLENCE`·`SEXUAL`·`DEROGATORY`·`TOXIC`; `action` ∈ `BLOCK`·`INFORM`·`MASK` |
| `PROMPT_ATTACKS_PREVENTION` (`PromptAttacksPreventionPolicy`) | *(none — base fields only)* | — |
| `PII_DETECTION` (`PiiDetectionPolicy`) | `piiCategories[]`, `customPiiRules[]` | `PiiCategory { category, isEnabled, action, threshold }` — `category` ∈ `PERSON`·`ADDRESS`·`TELEPHONE_NUMBER`·`EMAIL`; `action` ∈ `BLOCK`·`INFORM`·`MASK`. `CustomPiiRule { name, pattern, prefix, suffix, isCaseSensitive, maxDistance, priority }` |
| `DENIED_TOPICS` (`DeniedTopicsPolicy`) | `topics[]` | `DeniedTopic { name, definition, examples[] }` |
| `WORD_FILTERS` (`WordFiltersPolicy`) | `words[]`, `regexPatterns[]` | both `list[str]` |

Example PII policy with a custom rule (base fields + child fields):
```json
{ "policyType": "PII_DETECTION", "policyName": "mask_pii", "scope": "BOTH", "action": "MASK", "threshold": 0.8,
  "piiCategories": [ { "category": "EMAIL", "isEnabled": true, "action": "MASK", "threshold": 0.7 } ],
  "customPiiRules": [ { "name": "emp_id", "pattern": "EMP-\\d{6}", "isCaseSensitive": false, "priority": 1 } ] }
```
Field **names/enums** above are from the SDK subtype + child models (`content_moderation_policy.py:64-71`,
`content_moderation_category_config.py:83-88`, `prompt_attacks_prevention_policy.py:59-66`,
`pii_detection_policy.py:69-78`, `pii_category.py:75-80`, `custom_pii_rule.py:60-68`, `denied_topics_policy.py:64-72`,
`denied_topic.py:40-44`, `word_filters_policy.py:69-78`) — confirmed. The discriminator/wrapper used to POST these
to `…/agentFlowGuardrails` (and whether subtypes nest under a `policies[]` array) should be confirmed by
round-tripping a live `GET …/agentFlowGuardrails` before a first write, and recorded in `rest-endpoint-map.md`.

High-code agents pass the same policies via `OCIAIConf(guardrails_config=…)` (`aidp-agent-highcode`).

## Workflow
1. **Discover** with `GET …/workspaces/<ws>/agentFlows`; **verify** the route with this read before any write
   (handle the LA 404 — check the workspace segment first). For a specific flow, `GET …/workspaces/<ws>/agentFlows/<key>`.
2. **Author**: create/update the flow definition; `validate` before deploy.
3. **Deploy**: create a deployment, attach AI compute; deploy is async (`202` + operation key) — poll to
   terminal via `aidp-observability` / the async-ops endpoint per `oci-raw-request.md`.
4. **Run/monitor**: create a session, run it, read traces / chat history.
5. Confirm before deploy/delete (deployment consumes compute).

## Guardrails
- Deploy/delete are heavy/outward — confirm first. Don't print any embedded secrets in a flow definition.
- No-fabrication: don't present an `agentFlows` endpoint as confirmed until a live 2xx (or documented 4xx)
  is recorded in `rest-endpoint-map.md`.
- For mutating ops (create / update / delete / deploy / run a flow or session), persist the request body to
  `.aidp/payloads/` and confirm first — see [references/payloads.md]($HOME/.aidp/references/payloads.md).

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) — official `aidp` CLI v1.0.0 has no agent-flow group
- [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) · [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md) · [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md)
- [references/payloads.md]($HOME/.aidp/references/payloads.md) — persist + confirm request bodies for mutating ops

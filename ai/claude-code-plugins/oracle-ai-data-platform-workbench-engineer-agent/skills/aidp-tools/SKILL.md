---
name: aidp-tools
description: Create and manage standalone reusable AIDP agent Tools — SQL, Prompt, RAG, HTTP, Custom, and MCP tools that can be shared across agent flows by key. Use when the user wants to define a reusable tool (not inline in one flow), list/create/update/delete a tool, test a tool, or build a tool an agent/agent-flow node will reference. For attaching a tool inside a single flow as a node, see aidp-agent-flows + references/agent-flow-nodes.md.
---

# `aidp-tools` — standalone reusable agent tools

A **standalone Tool** is a reusable tool object an agent or an agent-flow node references by `toolKey`,
instead of inlining the config in every flow. Workspace-scoped under the LA AgentFlows family.

> **Engine:** `oci raw-request --profile DEFAULT` (no CLI group in v1.0.0). **Workspace-scoped**, and the
> write path is **VERIFIED live 2026-06-10** (round-trip on `oaseceal`): `POST …/workspaces/<ws>/tools` → **200**
> (returns `key`, `toolConfig`, `toolProvider`, `toolType`, `inputSchema`, …); `DELETE …/tools/<key>` → **204**.

## When to use
- "Create/define a reusable tool", "make a SQL/Prompt/RAG/HTTP/MCP/Custom tool", list/update/delete/test tools.
- NOT a tool used only inside one flow (inline it as a node → `aidp-agent-flows`, `references/agent-flow-nodes.md`).

## Tool kinds (`toolType`)
**`SQL` · `PROMPT` · `RAG` · `HTTP` · `CUSTOM` · `MCP`** (verified enum; `NL_TO_SQL` also exists as a
tool-details model). Each carries a `toolConfig` of the matching type (e.g. `PromptToolConfiguration`
`{llm, promptText, modelSettings}`, `SqlToolConfiguration`, `RagToolConfiguration` → needs a KB, etc.).

## CRUD (`oci raw-request`)
| Op | Call |
|---|---|
| List | `GET …/workspaces/<ws>/tools` |
| Create | `POST …/workspaces/<ws>/tools` — body `{displayName, description, toolType, properties, inputSchema, toolConfig}` |
| Get / Update / Delete | `GET`/`PUT`/`DELETE …/workspaces/<ws>/tools/<key>` |
| Test | `POST …/workspaces/<ws>/tools/actions/test` (`TestMcpExternalTool`-style body) — *confirm shape live* |

**Verified create example** (CUSTOM, minimal — round-tripped):
```bash
oci raw-request --http-method POST --profile DEFAULT \
  --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/tools" \
  --request-body '{"displayName":"my_tool","description":"…","toolType":"CUSTOM","properties":{}}'
# → 200, returns "key"; delete with DELETE …/tools/<key> → 204
```
**Name rule (verified):** must start with a letter; only `_` is allowed as a special character (a leading `_`
or other punctuation → `400 InvalidParameter`).

## Wire it to an agent / flow
Reference the returned `key` as `toolKey` on a tool node (`SQL_TOOL`/`PROMPT_TOOL`/`RAG_TOOL`/`HTTP_TOOL`/
`CUSTOM_TOOL`/`MCP_TOOL`) — see [references/agent-flow-nodes.md](../../references/agent-flow-nodes.md). RAG
tools require a Knowledge Base first (`aidp-knowledge-bases`).

## Guardrails
- Mutation gate: create/update/delete change state — show the body, confirm first, persist to `.aidp/payloads/`
  ([references/payloads.md](../../references/payloads.md)). Don't inline secrets — use `aidp-credentials`.
- Per-type `toolConfig` field names: confirm against the SDK `create_*_tool_details` / a live `GET` of an
  existing tool before authoring a non-trivial config; don't fabricate config fields.

## References
- [aidp-agent-flows](../aidp-agent-flows/SKILL.md) · [references/agent-flow-nodes.md](../../references/agent-flow-nodes.md) · [aidp-knowledge-bases](../aidp-knowledge-bases/SKILL.md) (RAG) · [aidp-agent-highcode](../aidp-agent-highcode/SKILL.md) (code-first tools)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md) · [references/payloads.md](../../references/payloads.md)

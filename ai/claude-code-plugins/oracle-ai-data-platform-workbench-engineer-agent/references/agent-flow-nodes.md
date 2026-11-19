# AIDP agent-flow node types (reference)

An AIDP agent flow is a **node graph**. Each node is a `Create<X>NodeDetails` object (LA AgentFlows API,
`20240831`) placed in the flow's node list at `…/workspaces/<ws>/agentFlows` (see
[`../skills/aidp-agent-flows/SKILL.md`](../skills/aidp-agent-flows/SKILL.md)). Field **names** below are from
the official SDK `create_*_node_details` models; node-graph wrapper + structural-node configs should be
confirmed by round-tripping an existing flow (`GET …/agentFlows/<key>`) before a first write — no fabrication.

## Common node fields (all 13 types)
`type` (the discriminator, below), `nodeType`, `name`, `description`, `positionX`, `positionY`,
`isExpanded`, `parentNodeId`, `srcNodeId`, `configuration`, `nodeTypeId`.

## The 13 `type` values (`agent_flow_node.TYPE_*`)
| `type` | Role | Type-specific config (verified field names) |
|---|---|---|
| `START_NODE` | Flow entry point | — |
| `AGENT` | An LLM agent | `instructions`, `llm` (`LlmConfig`), `modelSettings`, `memory` (`MemoryConfiguration`), `tools[]` |
| `SUPERVISOR_AGENT` | Multi-agent orchestrator that routes to sub-agents | `instructions`, `llm` (`LlmConfig`), `modelSettings`, `memory` (`MemoryConfiguration`), `tools[]`, `toolReferences[]`, `supervisorConfig` (free-form dict — routing config) |
| `NESTED_AGENT_FLOW` | Embeds another agent flow as one node | `instructions`, `memory` (`MemoryConfiguration`), `nestedAgentFlowConfig` (free-form dict — nested-flow reference) |
| `EXTERNAL_AGENT` | Calls an agent hosted elsewhere | `instructions`, `externalAgentConfig` (free-form dict — endpoint/auth) |
| `HUMAN_IN_THE_LOOP` | Pauses for human input/approval | `humanInTheLoopConfig` (free-form dict — prompt/approval config); no `instructions`/`llm` |
| `GUARDRAILS` | Applies safety policies to the flow | `SafetyPolicy` list — see the guardrails section in `aidp-agent-flows` |
| `SQL_TOOL` | Run parameterized SQL as a tool | `toolKey`, `inputSchema`, `toolConfig` (`SqlToolConfiguration`) |
| `PROMPT_TOOL` | LLM prompt template as a tool | `toolConfig` (`PromptToolConfiguration`: `llm`, `promptText`, `modelSettings`) |
| `RAG_TOOL` | Retrieve from a Knowledge Base | `inputSchema` (`RagToolInputSchema`), `toolConfig` (`RagToolConfiguration`) — **needs a KB** (`aidp-knowledge-bases`) |
| `MCP_TOOL` | Call a remote MCP server | `toolConfig` (`McpToolConfiguration`: `endpoint`, `auth`, `allowedTools`, `customHeaders`) — see `aidp-agent-flows` MCP section |
| `HTTP_TOOL` | Call an HTTP endpoint as a tool | `toolConfig` (`HttpToolConfiguration`) |
| `CUSTOM_TOOL` | Custom code tool | `toolConfig` (`CustomToolConfiguration`) |

## Orchestration / structural nodes (the four `*Config` nodes)
`SUPERVISOR_AGENT`, `NESTED_AGENT_FLOW`, `EXTERNAL_AGENT`, and `HUMAN_IN_THE_LOOP` have **no dedicated
`Create<X>NodeDetails` model** in the SDK — only read-model node classes (`SupervisorAgentNode`,
`NestedAgentFlowNode`, `ExternalAgentNode`, `HumanInTheLoopNode`). Their distinguishing config is a single
free-form `*Config` map (SDK type `dict(str, str)`), so the field **names** above are verified but the **inner
keys** of each map are not enumerated by the model — **round-trip an existing flow of that type
(`GET …/agentFlows/<key>`) to learn them before a first write.** Field-name sources:
`supervisor_agent_node.py:142-166`, `nested_agent_flow_node.py:122-142`, `external_agent_node.py:117-136`,
`human_in_the_loop_node.py:112-130`.

## Inline tool config vs. referencing a standalone Tool
A tool node can either **inline** its `toolConfig`, or **reference a reusable standalone Tool** by `toolKey`
(create/manage those via [`../skills/aidp-tools/SKILL.md`](../skills/aidp-tools/SKILL.md), `…/workspaces/<ws>/tools`).
Use a standalone Tool when the same SQL/Prompt/RAG/HTTP/Custom/MCP tool is shared across flows.

## LLM config (`LlmConfig`, used by AGENT + PROMPT_TOOL)
`model_provider` (`generic`/`cohere`), `model_id` (e.g. `xai.grok-4`, `cohere.command-r-08-2024`),
`compartment_id`, `endpoint`, model args (temperature/max_tokens). List live models via `aidp-models-catalog`.

## Authoring flow
1. `GET …/workspaces/<ws>/agentFlows/<key>` to learn the exact node-graph wrapper (round-trip an existing flow).
2. Build the node list (each node a `Create*NodeDetails`); wire edges via `srcNodeId`/`parentNodeId`.
3. `validate` → deploy via `POST …/workspaces/<ws>/actions/deployAgentFlow` (async).
4. Persist the body to `.aidp/payloads/` and confirm before write (`../references/payloads.md`).

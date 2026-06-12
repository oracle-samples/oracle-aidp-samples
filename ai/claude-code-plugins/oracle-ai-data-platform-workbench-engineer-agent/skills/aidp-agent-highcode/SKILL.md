---
name: aidp-agent-highcode
description: Build AIDP agents in high-code Python with aidputils + LangGraph (the code-first alternative to the low-code agent-flow canvas). Use when the user wants to write an agent in Python, use LangGraph / create_react_agent / StateGraph, call aidputils (OCIAIConf, AIDPToolConf, init_oci_llm, create_langgraph_tool), build a custom or multi-agent supervisor flow in code, or asks "how do I code an AIDP agent". For the low-code/REST node-graph path use aidp-agent-flows.
---

# `aidp-agent-highcode` — code-first AIDP agents (aidputils + LangGraph)

The GA high-code path: write a Python agent class using **`aidputils`** (pre-installed in AI Compute;
**not** `pip`-installable locally; legacy name `aidp_flowutils`) on top of **LangGraph 1.x**. You author the
`.py` in the workspace (`aidp-workspace-files` / `aidp-notebooks`) and run it on AI Compute. Grounded in
AIDP_High_Code_Complete_Reference.md §4–12, §22.

## When to use
- "Write/code an AIDP agent", LangGraph, `create_react_agent`, `StateGraph`, custom tool logic, multi-agent
  supervisor in code, or anything `aidputils`.
- NOT the drag-and-drop / REST node graph → `aidp-agent-flows`. NOT building the RAG corpus → `aidp-knowledge-bases`.

## Imports (current `aidputils`; legacy `aidp_flowutils` still works)
```python
from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup
from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf, ModelArgs
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, MessagesState, START, END
from langchain_core.messages import HumanMessage
```

## The agent class contract (REQUIRED)
Every agent MUST implement `__init__` / `setup()` / `async invoke()`:
```python
class MyAgent:
    def __init__(self) -> None:
        self.agent = None                      # or self.graph = None

    def setup(self) -> None:                   # sync, called once: build LLM + tools + agent
        llm = init_oci_llm(OCIAIConf(
            model_provider="generic", model_id="xai.grok-4",
            compartment_id="ocid1.compartment.oc1..…",
            endpoint="https://inference.generativeai.us-ashburn-1.oci.oraclecloud.com",
            model_args=ModelArgs(temperature=0.7, max_tokens=4096),
            guardrails_config={"policies": []}, auth_type="SECURITY_TOKEN", auth_profile="DEFAULT"))
        tool = create_langgraph_tool(AIDPToolConf(
            name="summarizer", description="Summarize text",
            tool_class="PromptTool",            # or "SQLTool" / "RAGTool"
            conf={...}, params=[{"name":"text","type":"string","description":"…"}]).model_dump())
        self.agent = create_react_agent(llm, [tool])    # single-agent; StateGraph for multi-agent

    async def invoke(self, user_query: str, **kwargs):
        config = pre_invoke_setup(**kwargs)             # MUST be first line of every invoke
        message = {"messages": [dict(HumanMessage(content=user_query))]}
        return await self.agent.ainvoke(input=message, config=config)
```
Rules (HC ref §5): `setup()` is **synchronous**, runs once; `invoke()` is **async**, per query;
**`pre_invoke_setup(**kwargs)` must be the first call in `invoke()`**; input is always
`{"messages": [dict(HumanMessage(content=…))]}`.

## Building blocks
| Piece | API | Notes |
|---|---|---|
| LLM | `OCIAIConf(model_provider, model_id, compartment_id, endpoint, model_args, guardrails_config, auth_type, auth_profile)` → `init_oci_llm()` | Models: `xai.grok-4` (default), `xai.grok-4-fast-reasoning`, `cohere.command-r-08-2024` |
| Tools | `AIDPToolConf(name, description, tool_class, conf, params)` → `create_langgraph_tool(.model_dump())` | `tool_class` ∈ `PromptTool` / `SQLTool` / `RAGTool` (RAGTool needs a KB → `aidp-knowledge-bases`) |
| Single agent | `create_react_agent(llm, tools)` | LangGraph prebuilt |
| Multi-agent | `StateGraph(MessagesState)` + supervisor (HC ref §10) | nodes + `START`/`END` edges |
| Observability | `AIDPObservability` (HC ref §12) | tracing |

## Deploy
Place the agent `.py` in the workspace and designate the **entry file** + dependency files (HC ref §22),
then run/deploy on **AI Compute** (Preview — see `aidp-cluster-ops`). Deployment + sessions can also be
managed via the agent-flow deploy action (`aidp-agent-flows`).

> **Live-verified 2026-06-10 on de-agent — correction:** the agent-flow deploy action is gated and has a
> required-field contract. (1) The ENTIRE `agentFlows` write surface (create included) is gated on the
> DataLake's `aiFeatureStatus=Ready` — on a fresh datalake, create returns **409 IncorrectState
> `AiFeatureStatus=None`** ("try again later") until the Enable-AI-Feature workflow completes; this is a
> platform-provisioning state, not a body defect. (2) `POST …/actions/deployAgentFlow` requires a
> **`deploymentType`** enum (the `agentFlowKey` alone → 400 "deploymentType must not be null"); the valid value
> is NOT `AI_COMPUTE`/`SERVERLESS`/`DEDICATED`/`ON_DEMAND`/`QUICK_START`/`STANDARD`/`DEFAULT` (all 400 "Invalid
> DeploymentType") — pull the real enum from the SDK `DeploymentType`/`Deployment` models before deploying.

## Guardrails
- `aidputils` only exists inside AI Compute — examples are authored/edited via `aidp-workspace-files`, not run
  locally. Don't claim a local `pip install aidputils`.
- Don't fabricate `model_id`s/endpoints — list live via `aidp-models-catalog`. Attach safety policies via
  `guardrails_config` (enums in `aidp-agent-flows` guardrails section).

## References
- [aidp-agent-flows](../aidp-agent-flows/SKILL.md) (low-code/REST path + guardrail enums) · [aidp-knowledge-bases](../aidp-knowledge-bases/SKILL.md) (RAGTool corpus) · [aidp-tools](../aidp-tools/SKILL.md) (standalone tools) · [aidp-workspace-files](../aidp-workspace-files/SKILL.md) (author the .py) · [aidp-models-catalog](../aidp-models-catalog/SKILL.md)

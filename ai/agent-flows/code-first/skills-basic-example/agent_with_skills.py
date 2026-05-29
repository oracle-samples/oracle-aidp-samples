#!/usr/bin/env python3
"""
SchoolGradeAgentWithSkills

Variant of school_grade_agent.py that uses explicit skills metadata records.
Discovery is driven by the provided `skills` list and does not list/read files
at initialization time.
"""

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator, Dict, Union

from langchain.agents.factory import create_agent
from langchain_core.messages import BaseMessage, HumanMessage

from aidputils.agents.skills.discovery import discover_skill_catalog
from aidputils.agents.skills.middleware import SkillMiddleware
from aidputils.agents.skills.tools.factories import make_skill_tools
from aidputils.agents.toolkit.agent_helper import get_client, parse_stream_response
from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf
from aidputils.agents.toolkit.custom_oci_generative_ai import ChatOCIGenAI
from aidputils.agents.toolkit.tool_helper import create_langgraph_tool

logger = logging.getLogger("school_grade_agent_with_skill_discovery")
########## Checkpointer creation #############
try:
    from aidputils.agents.toolkit.memory_helper import get_checkpoint_saver
    checkpointer = get_checkpoint_saver('school_grade_agent_with_skill_discovery')
except Exception:
    # The memory_helper script is not found in compute, use the checkpointer from globals instead.
    checkpointer = globals().get("checkpointer", None)


guardrails_config = {"policies": []}


def _merge_tools(*tool_groups: list) -> list:
    out = []
    seen = set()
    for group in tool_groups:
        for t in group:
            name = getattr(t, "name", None)
            if name and name in seen:
                continue
            if name:
                seen.add(name)
            out.append(t)
    return out


def _build_llm_conf() -> OCIAIConf:
    return OCIAIConf(
        model_provider="generic",
        compartment_id="<your-compartment-ocid>",
        model_args={},
        endpoint="https://inference.generativeai.<your-oci-region>.oci.oraclecloud.com", # example region: us-phoenix-1
        model_id="<your-model>", # example: xai.grok-4.3
        guardrails_config=guardrails_config,
    )


class SchoolGradeAgentWithEmbededSkills:
    def __init__(self) -> None:
        self.agent = None
        self.llm_conf = _build_llm_conf()

        # SKILLs specific functions
        # Determine default skill search locations (project + user) 
        # Build a SkillCatalog from discovered directories 
        self.catalog = discover_skill_catalog(skill_folder_whitelist=None)
        #Append available skills summary and routing rules to system prompt. 
        # Provide factory helpers for workspace-driven middleware construction. 
        self.skill_middleware = SkillMiddleware(self.catalog)
        # This method returns the skill discovery tools – activate_skill, list_skill_files, 
        # load_skill_file, and run_skill_entrypoint. 
        # These tools can be used by the agent to activate and run different skills.
        self.tools = make_skill_tools(self.catalog)

        for info in self.catalog.list():
            logger.info("skill_id=%s name=%s desc=%s root=%s skill_file=%s", info.skill_id, info.name, info.description, info.root_dir, info.skill_file)

    def setup(self) -> None:
        oci_llm = ChatOCIGenAI(
            auth_type=self.llm_conf.auth_type,
            model_id=self.llm_conf.model_id,
            provider=self.llm_conf.model_provider,
            service_endpoint=self.llm_conf.endpoint,
            compartment_id=self.llm_conf.compartment_id,
            auth_profile=self.llm_conf.auth_profile,
            client=get_client(llm_conf=self.llm_conf),
            model_kwargs=self.llm_conf.model_args,
        )

        system_prompt = (
            "You are SchoolGradeAgentWithSkills."
        )

        logger.info("Initializing SchoolGradeAgentWithSkills with %s tools", len(self.tools))
        self.agent = create_agent(
            model=oci_llm,
            tools=self.tools,
            system_prompt=system_prompt,
            middleware=[self.skill_middleware],
        )
        logger.info("SchoolGradeAgentWithSkills setup complete")

    async def invoke(self, user_query: str, **kwargs) -> Union[Dict[str, Any], AsyncGenerator[BaseMessage, None]]:
        if self.agent is None:
            raise RuntimeError("Agent is not initialized. Call setup() first.")

        thread_id = kwargs.get("thread_id")
        stream = bool(kwargs.get("stream", False))

        user_message = HumanMessage(content=user_query)
        message = {"messages": [dict(user_message)]}
        config = {"configurable": {"thread_id": thread_id}}

        if stream:
            return self._stream_messages(input=message, config=config)

        agent_response = await self.agent.ainvoke(input=message, config=config)
        return {**agent_response, "messages": agent_response.get("messages", [])[-1:]}

    async def _stream_messages(self, input, config) -> AsyncGenerator[BaseMessage, None]:
        stream = self.agent.astream(input=input, config=config, stream_mode="messages")
        async for chunk in parse_stream_response(stream):
            yield chunk


async def _example() -> None:
    agent = SchoolGradeAgentWithEmbededSkills()
    agent.setup()
    result = await agent.invoke("What tools do you have?")
    logger.info("Agent result: %s", result)


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    
    asyncio.run(_example())
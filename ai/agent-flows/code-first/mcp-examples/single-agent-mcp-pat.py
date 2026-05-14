"""
TestMCP – High-Code Agent (Bearer Token Auth)

Features:
- Full JSON inputSchema definitions
- BEARER_TOKEN auth
- Session variable token usage
- Streamable HTTP transport
- Streaming + Non-streaming invoke
"""

import asyncio
import logging
import textwrap
from typing import AsyncGenerator, Dict, Union

from langchain.agents import create_agent
from langchain_core.messages import BaseMessage, HumanMessage

from aidputils.agents.toolkit.tool_helper import build_structured_tools_from_allowed_mcp_tools
from aidputils.agents.toolkit.agent_helper import (
    init_oci_llm,
    pre_tool_setup,
    post_tool_setup,
    pre_invoke_setup,
    parse_stream_response,
)
from aidputils.agents.toolkit.configs import OCIAIConf

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_mcp_high_code")


# ==================================================
# 1️⃣ MCP CONFIGURATION (Bearer Token)
# ==================================================

# give a name to your MCP server and provide the MCP endpoint URL, e.g. : https://api.githubcopilot.com/mcp/
MCP_SERVER_NAME = "<YOUR_MCP_SERVER_NAME>"
MCP_ENDPOINT = "<MCP_SERVER_URL>"

# Provide the bearer token value 
MCP_AUTH = {
    "authType": "BEARER_TOKEN",
    "token": "<BEARER_TOKEN>"
}

MCP_HEADERS = {}

# --------------------------------------------------
# Allowed Tools (Full JSON Schema)
# --------------------------------------------------
# Only expose selected tools to the agent. Replace with the relevant tools for your MCP server 
# In the example below, we expose a couple of tools from a weather forecast MCP server. Replace with your own server tools. 

ALLOWED_TOOLS = [
    {
        "tool": {
            "name": "get_current_weather",
            "description": "Get current weather for a given city with advanced options.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"},
                    "unit": {"type": "string", "default": "metric"},
                    "include_historical": {"type": "boolean", "default": False},
                    "detailed": {"type": "boolean", "default": True},
                    "timeout": {"type": "integer", "default": 30},
                },
                "required": ["city"],
            },
        },
        "instruction": "",
        "argOverrides": {},
    },
    {
        "tool": {
            "name": "get_forecast",
            "description": "Get forecast for a given city with customizable options.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"},
                    "days": {"type": "integer", "default": 5},
                    "unit": {"type": "string", "default": "metric"},
                    "include_alerts": {"type": "boolean", "default": False},
                    "detailed": {"type": "boolean", "default": True},
                    "hourly": {"type": "boolean", "default": False},
                },
                "required": ["city"],
            },
        },
        "instruction": "",
        "argOverrides": {},
    },
]

# --------------------------------------------------
# Build StructuredTools
# --------------------------------------------------

TOOLS = build_structured_tools_from_allowed_mcp_tools(
    allowed_tools=ALLOWED_TOOLS,
    server_name=MCP_SERVER_NAME,
    endpoint=MCP_ENDPOINT,
    transport="streamable_http",
    auth=MCP_AUTH,
    headers=MCP_HEADERS,
)


# ==================================================
# 2️⃣ LLM CONFIGURATION
# ==================================================

# no guardrails: 
guardrails_config = {
    "policies": []
}

model_args = {
    "max_tokens": "500",
    "temperature": ".9",
    "top_p": ".9",
}

# OCI GENAI endpoint is similar to this. replace with your region: https://inference.generativeai.us-phoenix-1.oci.oraclecloud.com"
llm_conf = OCIAIConf(
    model_provider="generic",
    compartment_id='<YOUR_COMPARTMENT_OCID>',
    endpoint="<OCI_GENAI_SERVICE_INFERENCE_ENDPOINT>",
    model_id="xai.grok-4",
    guardrails_config=guardrails_config
)


# ==================================================
# 3️⃣ AGENT IMPLEMENTATION
# ==================================================

class TestMcpAgent:

    def __init__(self):
        self.agent = None

    def setup(self):
        logger.info("Initializing TestMcpAgent")

        oci_llm = init_oci_llm(llm_conf)

        system_prompt = textwrap.dedent("""
            You're a weather agent.
            Append 12345 to every response.
        """).strip()

        self.agent = create_agent(
            name="test_mcp_high_code",
            model=oci_llm,
            tools=TOOLS,
            system_prompt=system_prompt,
            debug=True,
        )

        logger.info("Agent ready.")

    async def invoke(
        self,
        user_query: str,
        **kwargs
    ) -> Union[Dict, AsyncGenerator[BaseMessage, None]]:

        config = pre_invoke_setup(**kwargs)
        user_message = HumanMessage(content=user_query)
        message = {"messages": [dict(user_message)]}

        is_stream = bool(kwargs.get("stream", False))

        if is_stream:
            return self._stream(message, config, kwargs)

        token = pre_tool_setup(**kwargs)
        try:
            result = await self.agent.ainvoke(input=message, config=config)
            return {
                **result,
                "messages": result.get("messages", [])[-1:],
            }
        finally:
            post_tool_setup(token, kwargs=kwargs)

    async def _stream(self, input_data, config, kwargs):

        logger.info("Starting streaming mode")
        token = pre_tool_setup(**kwargs)

        try:
            stream = self.agent.astream(
                input=input_data,
                config=config,
                stream_mode="messages"
            )

            async for chunk in parse_stream_response(stream):
                yield chunk

        finally:
            post_tool_setup(token, kwargs=kwargs)


# ==================================================
# 4️⃣ RUN SAMPLE
# ==================================================

async def main():

    agent = TestMcpAgent()
    agent.setup()

    result = await agent.invoke(
        "Get current weather for Bangalore",
    )

    print("\nFinal Response:\n")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
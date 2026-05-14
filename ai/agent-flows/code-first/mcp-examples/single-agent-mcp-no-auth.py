"""
Weather MCP – High-Code Sample (Using Allowed MCP Tools)

Demonstrates:
- Single Agent
- Custom MCP Server
- Allowed Tool Filtering
- Structured Tool Building
- NO_AUTH transport
"""

import asyncio
import logging
from langchain.agents import create_agent
from langchain_core.messages import HumanMessage

from aidputils.agents.toolkit.configs import OCIAIConf
from aidputils.agents.toolkit.tool_helper import build_structured_tools_from_allowed_mcp_tools
from aidputils.agents.toolkit.agent_helper import (
    init_oci_llm,
    pre_tool_setup,
    post_tool_setup,
    pre_invoke_setup,
)

# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("weather_mcp_high_code")


# ==================================================
# 1️⃣ MCP SERVER CONFIGURATION
# ==================================================

# Give a name to your MCP server and provide the URL for it, e.g. : https://api.githubcopilot.com/mcp/
MCP_SERVER_NAME = "<MCP_SERVER_NAME>"
MCP_ENDPOINT = "<MCP_URL>"

# No auth required for this MCP server
MCP_AUTH = {
    "authType": "NO_AUTH"
}

# Only expose selected tools to the agent. Replace with the relevant tools for your MCP server 
# In the example below, we expose a couple of tools from a weather forecast MCP server. Replace with your own server tools. 
ALLOWED_MCP_TOOLS = [
    {
        "tool": {
            "name": "get_weather",
            "description": "Get current weather for a given city",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"},
                    "unit": {"type": "string"}
                },
                "required": ["city", "unit"]
            }
        },
        "instruction": "",
        "argOverrides": {}
    },
    {
        "tool": {
            "name": "weather_forecast",
            "description": "Get weather forecast for a city",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"}
                },
                "required": ["city"]
            }
        },
        "instruction": "",
        "argOverrides": {}
    }
]

# Build MCP tools from allowed list
TOOLS = build_structured_tools_from_allowed_mcp_tools(
    allowed_tools=ALLOWED_MCP_TOOLS,
    server_name=MCP_SERVER_NAME,
    endpoint=MCP_ENDPOINT,
    transport="streamable_http",
    auth=MCP_AUTH,
    headers={}
)


# ==================================================
# 2️⃣ LLM CONFIGURATION
# ==================================================

guardrails_config = {
    "name": "Default Guardrails",
    "description": "Minimal moderation setup",
    "policies": []
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

class WeatherMcpAgent:

    def __init__(self):
        self.agent = None

    def setup(self):
        logger.info("Initializing Weather MCP Agent")

        oci_llm = init_oci_llm(llm_conf)

        system_prompt = """
You are a weather assistant.

Rules:
- For current weather → call get_weather
- For forecast → call weather_forecast
- If user asks both → call both tools

Return a clear natural-language response.
"""

        self.agent = create_agent(
            name="weather_mcp_high_code",
            model=oci_llm,
            tools=TOOLS,
            system_prompt=system_prompt,
            debug=True
        )

        logger.info("Agent ready.")

    async def invoke(self, user_query: str, **kwargs):
        token = pre_tool_setup(**kwargs)
        config = pre_invoke_setup(**kwargs)

        try:
            message = {"messages": [dict(HumanMessage(content=user_query))]}
            return await self.agent.ainvoke(input=message, config=config)
        finally:
            post_tool_setup(token)


# ==================================================
# 4️⃣ RUN SAMPLE
# ==================================================

async def main():

    agent = WeatherMcpAgent()
    agent.setup()

    result = await agent.invoke(
        "What is the weather and forecast for Bangalore?"
    )

    print("\nFinal Response:\n")
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
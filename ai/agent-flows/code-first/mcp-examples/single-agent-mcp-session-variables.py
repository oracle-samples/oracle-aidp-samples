from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.agent_helper import (
    init_oci_llm,
    pre_tool_setup,
    post_tool_setup,
    pre_invoke_setup,
    parse_stream_response,
)
from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf
from langchain.agents import create_agent
from langchain_core.messages import BaseMessage, AIMessage, HumanMessage, SystemMessage
from typing import AsyncGenerator, Dict, Union
import logging
import json
import textwrap

AGENT_ID = "<YOUR_AGENT_ID>"
logger = logging.getLogger(AGENT_ID)

########## Checkpointer creation #############
try:
    from aidputils.agents.toolkit.memory_helper import get_checkpoint_saver
    checkpointer = get_checkpoint_saver(AGENT_ID)
except Exception:
    checkpointer = globals().get("checkpointer", None)
########## End Checkpointer creation #############


########## Guardrails Configuration ################
guardrails_config = {
    "name": "Default Guardrails",
    "description": "Minimal moderation setup",
    "policies": []
}
########## End Guardrails Configuration ############

########## Session Configuration ################ 
#session_config = {
#  "variables": {
#    "sessionvariables.cred.mcp.test_mcp.bearer": {
#      "name": "sessionvariables.cred.mcp.test_mcp.bearer",
#      "isRequired": True,
#      "shouldLog": False,
#      "isSystem": True
#    } 
#  } 
#}

########## End Session Configuration ############
########## Start Generated code for Agent Flow ################

from aidputils.agents.toolkit.tool_helper import build_structured_tools_from_allowed_mcp_tools

test_mcp_mcp_allowed_tools = [
  {
    "tool": {
      "name": "get_current_weather",
      "description": "Get current weather for a given city with advanced options.",
      "inputSchema": {
        "properties": {
          "city": {
            "type": "string"
          },
          "unit": {
            "default": "metric",
            "type": "string"
          },
          "include_historical": {
            "default": False,
            "type": "boolean"
          },
          "detailed": {
            "default": True,
            "type": "boolean"
          },
          "timeout": {
            "default": 30,
            "type": "integer"
          }
        },
        "required": [
          "city"
        ],
        "type": "object"
      }
    },
    "instruction": "",
    "argOverrides": {}
  },
  {
    "tool": {
      "name": "get_forecast",
      "description": "Get forecast for a given city with customizable options.",
      "inputSchema": {
        "properties": {
          "city": {
            "type": "string"
          },
          "days": {
            "default": 5,
            "type": "integer"
          },
          "unit": {
            "default": "metric",
            "type": "string"
          },
          "include_alerts": {
            "default": False,
            "type": "boolean"
          },
          "min_temp_threshold": {
            "type": "number"
          },
          "max_temp_threshold": {
            "type": "number"
          },
          "detailed": {
            "default": True,
            "type": "boolean"
          },
          "hourly": {
            "default": False,
            "type": "boolean"
          }
        },
        "required": [
          "city"
        ],
        "type": "object"
      }
    },
    "instruction": "",
    "argOverrides": {}
  }
]

test_mcp_mcp_custom_headers = {}

# FIXED TOKEN PLACEHOLDER
# The session variable cred.mcp.test_mcp.bearer was created in the Agent Studio under the Variables tab of your target agent. 
# the token value is provided at runtime through session variables. 
test_mcp_mcp_auth_config = {
  "authType": "BEARER_TOKEN",
  "token" : "{{sessionvariables.cred.mcp.test_mcp.bearer}}"
}


def fetch_mcp_tools_test_mcp():
    tools = build_structured_tools_from_allowed_mcp_tools(
        allowed_tools=test_mcp_mcp_allowed_tools,
        server_name="<YOUR_MCP_SERVER_NAME>",
        endpoint="<YOUR_MCP_SERVER_URL>",
        transport="streamable_http",
        auth=test_mcp_mcp_auth_config,
        headers=test_mcp_mcp_custom_headers
    )
    if not tools:
        return []
    return tools


##### Start tool List#############
tools_agent1 = []
tools_agent1.extend(fetch_mcp_tools_test_mcp())
##### End tool List#############


########## Generated code for OCI Gen AI LLM

model_args = {
  "max_tokens": 500,
  "temperature": 0.9,
  "top_p": 0.9
}

llm_conf = OCIAIConf(
    model_provider="generic",
    compartment_id='<YOUR_COMPARTMENT_OCID>',
    model_args=model_args,
    endpoint="<OCI_GENAI_SERVICE_INFERENCE_ENDPOINT>",
    model_id="xai.grok-4",
    guardrails_config=guardrails_config
)

## Agent class definition
class TestAf:

    def __init__(self) -> None:
        self.agent = None

    def setup(self) -> None:

        logger.info(llm_conf)

        oci_llm = init_oci_llm(llm_conf)

        system_prompt = textwrap.dedent("""
            ADD YOUR SYSTEM PROMPT HERE
        """).strip()

        try:
            if checkpointer:
                self.agent = create_agent(
                    name=AGENT_ID,
                    model=oci_llm,
                    tools=tools_agent1,
                    system_prompt=system_prompt,
                    debug=True,
                    checkpointer=checkpointer
                )
            else:
                self.agent = create_agent(
                    name=AGENT_ID,
                    model=oci_llm,
                    tools=tools_agent1,
                    system_prompt=system_prompt,
                    debug=True
                )

        except Exception as e:

            self.agent = create_agent(
                name=AGENT_ID,
                model=oci_llm,
                tools=tools_agent1,
                system_prompt=system_prompt,
                debug=True
            )

            logger.warning(f"Checkpointer could not be initialized {e}")

        logger.info(f"Setup for agent completed {self.agent}")


    async def invoke(
        self,
        user_query: str,
        **kwargs
    ) -> Union[Dict, AsyncGenerator[BaseMessage, None]]:

        config = pre_invoke_setup(**kwargs)

        user_message = HumanMessage(content=user_query)

        message = {
            "messages": [dict(user_message)]
        }

        is_stream = bool(kwargs.get("stream", False))

        if is_stream:
            return self._stream_messages(
                input=message,
                config=config,
                kwargs=kwargs
            )

        token = pre_tool_setup(**kwargs)

        try:

            agent_response = await self.agent.ainvoke(
                input=message,
                config=config
            )

            final_response = {
                **agent_response,
                "messages": agent_response.get("messages", [])[-1:]
            }

            return final_response

        except Exception:
            logger.exception("Exception while calling invoke")
            raise

        finally:
            post_tool_setup(token, kwargs=kwargs)


    async def _stream_messages(
        self,
        input,
        config,
        kwargs
    ) -> AsyncGenerator[BaseMessage, None]:

        """
        Stream messages as they are generated by the agent.
        """

        logger.info("Starting _stream_messages")

        token = pre_tool_setup(**kwargs)

        try:

            try:
                from aidputils.agents.auth.client.generative_ai_inference_v2_client import StreamingData
                stream = self.agent.astream(
                    input=input,
                    config=config,
                    stream_mode="messages"
                )

            except ImportError:

                stream = self.agent.astream(
                    input=input,
                    config=config
                )

            async for chunk in parse_stream_response(stream):
                yield chunk

        except Exception:

            logger.exception("Streaming error")
            raise

        finally:

            post_tool_setup(token, kwargs=kwargs)

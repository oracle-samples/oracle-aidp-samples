from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup
from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf, ModelArgs
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
import logging
import json

logger = logging.getLogger('agent_with_rag_tool')
checkpointer = globals().get("checkpointer", None)

##### REPLACE WITH YOUR TENANCY OCID and the region hosting the OCI GenAI model 
tenancy_ocid = "<your-tenancy-ocid>"
region = "<oci-region>"
catalog = "<your-catalog-name>"
schema = "<your-schema-name>"
knowledge_base = "<your-knowledge-base-name>"


########## Guardrails Configuration ################
guardrails_config = {
    "name" : "Default Guardrails",
    "description" : "Default empty guardrails configuration",
    "policies" : [ ]
  }
########## End Guardrails Configuration ############

##### Start RAG Tool configuration

rag_params = [ {
  "name" : "query",
  "type" : "string",
  "description" : "RAG query",
  "defaultValue" : "find matching doc artifacts for …"
} ]

conf = {
     "catalog": catalog,
     "schema": schema,
     "knowledgeBase": knowledge_base,
     "top_k": 5,
     "llm": {
       "model_id" : "xai.grok-4",
       "model_provider" : "generic",
       "compartment_id" : tenancy_ocid,
       "endpoint" : f"https://inference.generativeai.{region}.oci.oraclecloud.com"
     }
   }
rag_conf= AIDPToolConf(name="rag",
                                  description= "RAG Tool ",
                                  tool_class = "RAGTool", conf=conf, params=rag_params)
rag_tool = create_langgraph_tool(rag_conf.model_dump())


##### End RAG Tool configuration

##### Start tool List#############
tools_agent1 = [rag_tool]
##### End tool List#############


model_args = {}

llm_conf = OCIAIConf(model_provider='generic',
                     compartment_id=tenancy_ocid,
                     model_args=model_args,
                     endpoint=f"https://inference.generativeai.{region}.oci.oraclecloud.com",
                     model_id='xai.grok-4',
                     guardrails_config=guardrails_config)

## Agent class definition
class AgentWithRAGTool:
  def __init__(self) -> None:
    self.agent = None

  def setup(self) -> None:
    logger.info(llm_conf)
    oci_llm = init_oci_llm(llm_conf)
    system_prompt = """
Be a helpful assistant.
"""

    try:
      if checkpointer:
        self.agent = create_react_agent(model=oci_llm, tools=tools_agent1, prompt=system_prompt, debug=True, checkpointer= checkpointer)
      else:
        self.agent  = create_react_agent(model=oci_llm, tools=tools_agent1, prompt=system_prompt, debug=True)
    except Exception as e:
      # Fallback compile without checkpointer if wiring fails
      self.agent = create_react_agent(model=oci_llm, tools=tools_agent1, prompt=system_prompt, debug=True)
      logger.warning(f"Checkpointer could not be initialized {e}")
    logger.info(f"Setup for agent completed {self.agent}")

  async def invoke(self, user_query: str, **kwargs):
    config = pre_invoke_setup(**kwargs)
    user_message = HumanMessage(content=user_query)
    message = {"messages": [dict(user_message)]}
    try:
      return await self.agent.ainvoke(input=message, config = config)
    except Exception as e:
      import traceback
      logger.error(f"Exception while calling invoke {e}", exc_info=True)
      print("Stack trace:\n", traceback.format_exc())




import asyncio
async def main():
    # Instantiate and initialize the agent
    test_agent = AgentWithRAGTool()
    test_agent.setup()

    # You can customize this user query or prompt for input
    user_query = "Summarize SOX Compliance and Reconciliation at Acme Corp"

    # Run the asynchronous invoke method and print the result
    result = await test_agent.invoke(user_query)
    print("Agent response:", result)

if __name__ == "__main__":
    asyncio.run(main())
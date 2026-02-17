from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import StateGraph, MessagesState, START, END

def mock_llm(state: MessagesState):
    return {"messages": [{"role": "ai", "content": "hello world"}]}

class AgentBasic:
  def __init__(self) -> None:
    self.graph = None

  def setup(self) -> None:
    self.graph = StateGraph(MessagesState)
    self.graph.add_node(mock_llm)
    self.graph.add_edge(START, "mock_llm")
    self.graph.add_edge("mock_llm", END)
    self.graph =     self.graph.compile()
    system_prompt = "Be a helpful assistant."

  async def invoke(self, user_query: str, **kwargs):
    user_message = HumanMessage(content=user_query)
    messages = {"messages": [dict(user_message)]}
    try:
      return self.graph.invoke(messages)
    except Exception as e:
      import traceback
      logger.error(f"Exception while calling invoke {e}", exc_info=True)
      print("Stack trace:\n", traceback.format_exc())

import asyncio
async def main():
    test_agent = AgentBasic()
    test_agent.setup()
    result = await test_agent.invoke("Hi there")
    print("Agent response:", result)
if __name__ == "__main__":
    asyncio.run(main())
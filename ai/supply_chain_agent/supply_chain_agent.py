"""
Supply Chain Management Agent for Oracle AIDP
Multi-agent system for supply chain operations
"""

from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup
from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, MessagesState, END
from typing import TypedDict, Literal, List
from langchain_core.messages import BaseMessage
import logging

logger = logging.getLogger('supply_chain_agent')
checkpointer = globals().get("checkpointer", None)

# Configuration
COMPARTMENT_ID = '<your-tenancy-ocid>'
ENDPOINT = 'https://inference.generativeai.<oci-region>.oci.oraclecloud.com'
MODEL_ID = 'xai.grok-4'

guardrails_config = {
    "name": "Supply Chain Guardrails",
    "description": "Guardrails for supply chain operations",
    "policies": []
}

# State
class SupplyChainState(TypedDict):
    messages: List[BaseMessage]
    query: str
    route: Literal["inventory", "orders", "suppliers", "general"]
    analysis_result: str

# Tool 1: Inventory SQL Tool
inventory_sql_config = {
    "catalogKey": "your_catalog",
    "schemaKey": "your_schema",
    "query": """
    SELECT product_id, product_name, category, stock_qty, reorder_level, warehouse
    FROM inventory
    WHERE warehouse = {{warehouse}}
    ORDER BY stock_qty ASC
    """
}

inventory_sql_params = [{
    "name": "warehouse",
    "type": "string",
    "description": "Warehouse location",
    "defaultValue": "WH-001"
}]

inventory_tool_conf = AIDPToolConf(
    name="check_inventory",
    description="Check inventory levels and stock status",
    tool_class="SQLTool",
    conf=inventory_sql_config,
    params=inventory_sql_params
)
inventory_tool = create_langgraph_tool(inventory_tool_conf.model_dump())

# Tool 2: Orders SQL Tool
order_sql_config = {
    "catalogKey": "your_catalog",
    "schemaKey": "your_schema",
    "query": """
    SELECT order_id, customer_name, order_date, status, total_amount
    FROM orders
    WHERE status = {{status}}
    ORDER BY order_date DESC
    """
}

order_sql_params = [{
    "name": "status",
    "type": "string",
    "description": "Order status",
    "defaultValue": "IN_TRANSIT"
}]

order_tool_conf = AIDPToolConf(
    name="track_orders",
    description="Track order status and delivery",
    tool_class="SQLTool",
    conf=order_sql_config,
    params=order_sql_params
)
order_tool = create_langgraph_tool(order_tool_conf.model_dump())

# Tool 3: Suppliers SQL Tool
supplier_sql_config = {
    "catalogKey": "your_catalog",
    "schemaKey": "your_schema",
    "query": """
    SELECT supplier_id, supplier_name, category, rating, on_time_pct
    FROM suppliers
    WHERE rating >= {{min_rating}}
    ORDER BY on_time_pct DESC
    """
}

supplier_sql_params = [{
    "name": "min_rating",
    "type": "string",
    "description": "Minimum rating",
    "defaultValue": "7.0"
}]

supplier_tool_conf = AIDPToolConf(
    name="analyze_suppliers",
    description="Analyze supplier performance",
    tool_class="SQLTool",
    conf=supplier_sql_config,
    params=supplier_sql_params
)
supplier_tool = create_langgraph_tool(supplier_tool_conf.model_dump())

# LLM Configuration
model_args = {"temperature": 0.7, "max_tokens": 1500, "top_p": 0.95}
llm_conf = OCIAIConf(
    model_provider='generic',
    compartment_id=COMPARTMENT_ID,
    model_args=model_args,
    endpoint=ENDPOINT,
    model_id=MODEL_ID,
    guardrails_config=guardrails_config
)

# Agents
def supervisor(state: SupplyChainState) -> SupplyChainState:
    """Route queries to appropriate agents"""
    llm = init_oci_llm(llm_conf)
    messages = [
        HumanMessage(content=(
            "Route this query to: inventory, orders, suppliers, or general.\n"
            "Respond with ONLY the department name.\n\n"
            f"Query: {state['query']}"
        ))
    ]
    response = llm.invoke(messages)
    route = response.content.strip().lower()
    return {"route": route}

def inventory_agent(state: SupplyChainState) -> SupplyChainState:
    llm = init_oci_llm(llm_conf)
    agent = create_react_agent(model=llm, tools=[inventory_tool], debug=True)
    result = agent.invoke({"messages": [HumanMessage(content=state["query"])]})
    return {"analysis_result": result["messages"][-1].content}

def order_agent(state: SupplyChainState) -> SupplyChainState:
    llm = init_oci_llm(llm_conf)
    agent = create_react_agent(model=llm, tools=[order_tool], debug=True)
    result = agent.invoke({"messages": [HumanMessage(content=state["query"])]})
    return {"analysis_result": result["messages"][-1].content}

def supplier_agent(state: SupplyChainState) -> SupplyChainState:
    llm = init_oci_llm(llm_conf)
    agent = create_react_agent(model=llm, tools=[supplier_tool], debug=True)
    result = agent.invoke({"messages": [HumanMessage(content=state["query"])]})
    return {"analysis_result": result["messages"][-1].content}

def general_agent(state: SupplyChainState) -> SupplyChainState:
    llm = init_oci_llm(llm_conf)
    response = llm.invoke([HumanMessage(content=state["query"])])
    return {"analysis_result": response.content}

def final_agent(state: SupplyChainState) -> SupplyChainState:
    messages = state.get("messages", [])
    messages.append(HumanMessage(content=state["query"]))
    messages.append(HumanMessage(content=state.get("analysis_result", "")))
    return {**state, "messages": messages}

# Main Agent Class
class SupplyChainAgent:
    def __init__(self) -> None:
        self.graph = None
    
    def setup(self) -> None:
        builder = StateGraph(SupplyChainState)
        builder.add_node("supervisor", supervisor)
        builder.add_node("inventory_agent", inventory_agent)
        builder.add_node("order_agent", order_agent)
        builder.add_node("supplier_agent", supplier_agent)
        builder.add_node("general_agent", general_agent)
        builder.add_node("final_agent", final_agent)
        
        builder.set_entry_point("supervisor")
        builder.add_conditional_edges(
            "supervisor",
            lambda s: s["route"],
            {
                "inventory": "inventory_agent",
                "orders": "order_agent",
                "suppliers": "supplier_agent",
                "general": "general_agent"
            }
        )
        
        builder.add_edge("inventory_agent", "final_agent")
        builder.add_edge("order_agent", "final_agent")
        builder.add_edge("supplier_agent", "final_agent")
        builder.add_edge("general_agent", "final_agent")
        builder.add_edge("final_agent", END)
        
        if checkpointer:
            self.graph = builder.compile(checkpointer=checkpointer)
        else:
            self.graph = builder.compile()
        
        logger.info("Supply Chain Agent initialized")
    
    async def invoke(self, user_query: str, **kwargs):
        config = pre_invoke_setup(**kwargs)
        initial_state = {"query": user_query, "messages": []}
        try:
            return await self.graph.ainvoke(initial_state, config=config)
        except Exception as e:
            import traceback
            logger.error(f"Error: {e}", exc_info=True)
            print("Stack trace:\n", traceback.format_exc())

# Test
import asyncio
async def main():
    agent = SupplyChainAgent()
    agent.setup()
    
    test_queries = [
        "What's the inventory status in warehouse WH-001?",
        "Show me all delayed orders",
        "Which suppliers have the best performance?"
    ]
    
    print("=" * 70)
    print("SUPPLY CHAIN AGENT - TESTING")
    print("=" * 70)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n[Test {i}] Query: {query}")
        print("-" * 70)
        result = await agent.invoke(query)
        print(f"Response: {result.get('analysis_result', 'No response')}")
        print("=" * 70)

if __name__ == "__main__":
    asyncio.run(main())

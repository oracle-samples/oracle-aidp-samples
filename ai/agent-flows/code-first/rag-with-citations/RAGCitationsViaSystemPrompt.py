"""
RAGCitationsViaSystemPrompt — the prompt-only way to get cited answers.

Same goal as RAGProgramaticCitations.py (answer Oracle Fusion questions and show where
the facts came from) but the OPPOSITE philosophy: there is NO Python that reads
scores or rewrites citations. We hand the built-in RAGTool to the agent and rely
entirely on the system prompt to make the model cite the document_id and score of
the chunks it used.

How it works
------------
- The built-in RAGTool returns, for each query, a JSON payload that already
  contains a "retrieved_chunks" list; every chunk carries a "document_id", its
  "content", and a relevance "score" (0-1, higher = more relevant).
- That payload is placed verbatim into the conversation as a tool message, so the
  model can SEE the document_ids and scores.
- The system prompt instructs the model to answer only from those chunks and to
  format inline citations + a "Sources" list itself.

Trade-off
---------
Far less code, but the citations are only as good as the model: it may miscopy or
round scores, duplicate or skip sources, or drift from the requested format, and
nothing in code enforces or verifies them. Use this for a quick, low-effort
citation hint. Use RAGProgramaticCitations.py when citations must be accurate, deduped,
numbered, and available as machine-readable data.

Sequence of calls (one user turn)
---------------------------------
1. RAGCitationsViaSystemPrompt.setup()       — build a plain ReAct agent over the
                                                built-in RAGTool.
2. RAGCitationsViaSystemPrompt.invoke(query) — run the agent and return its result
                                                AS-IS (no post-processing).
"""

from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup
from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import AIMessage, HumanMessage
import logging

logger = logging.getLogger('rag_citations_via_system_prompt')
checkpointer = globals().get("checkpointer", None)

##### REPLACE with your OCI details and knowledge base coordinates #####
compartment_ocid = "<your-compartment-ocid>"
region = "<oci-region>"
catalog = "<your-catalog-name>"
schema = "<your-schema-name>"
knowledge_base = "<your-knowledge-base-name>"
# Pick a model that is available in YOUR region (availability is region-specific).
# See the OCI Generative AI docs for the model IDs offered in each region.
model_id = "<oci-genai-model-id>"
endpoint = f"https://inference.generativeai.{region}.oci.oraclecloud.com"

# The built-in RAGTool is handed to the agent DIRECTLY — no wrapper, no Python
# that touches scores or citations. Its JSON output (which includes each chunk's
# document_id and score) is what the model reads and cites.
rag_conf = AIDPToolConf(
    name="fusion_pdf_reader",
    description="Retrieves relevant chunks from Oracle Fusion PDFs to help answer user questions.",
    tool_class="RAGTool",
    conf={
        "catalog": catalog,
        "schema": schema,
        "knowledgeBase": knowledge_base,
        "top_k": 5,
        "llm": {
            "model_id": model_id,
            "model_provider": "generic",
            "compartment_id": compartment_ocid,
            "endpoint": endpoint
        }},
    params=[
        {
            "name": "query",
            "type": "string",
            "description": "The natural language question about Oracle Fusion Applications",
            "defaultValue": "find matching doc artifacts for …"
        }])

rag_tool = create_langgraph_tool(rag_conf.model_dump())
agent_tools = [rag_tool]

llm_conf = OCIAIConf(
    model_provider='generic',
    compartment_id=compartment_ocid,
    model_args={},
    endpoint=endpoint,
    model_id=model_id,
    guardrails_config={
        "name": "Default Guardrails",
        "description": "Default empty guardrails configuration",
        "policies": []}
    )

# All citation behaviour lives in this prompt. The model is told what fields the
# tool output contains and exactly how to render citations + a Sources list.
agent_system_prompt = """You are a helpful assistant for questions about Oracle's Fusion Applications.

Use the "fusion_pdf_reader" tool to retrieve supporting documentation. You may
call it more than once with different queries when needed. Each tool result is
JSON containing a "retrieved_chunks" list; every chunk has:
  - "document_id": the source document and chunk it came from,
  - "content": the text of the chunk,
  - "score": a relevance score between 0 and 1 (higher means more relevant).

When you answer:
1. Use ONLY the content of the retrieved chunks. If they do not contain the
   answer, say you could not find a confident answer in the knowledge base; do
   not use prior knowledge and do not speculate.
2. Cite each factual claim inline with a bracketed number, e.g. [1], [2].
3. End your reply with a "Sources" section. List each cited chunk on its own line
   in this exact format:
       [n] <document_id> — score <score rounded to 3 decimals>
4. Number the sources in the order they first appear in your answer, reuse the
   same number when you cite the same document again, and list each source once.
5. Only cite document_ids and scores that actually appear in the tool output —
   never invent or alter them.
"""


class RAGCitationsViaSystemPrompt:
    """The deployable AIDP agent flow (prompt-only citations).

    A plain ReAct agent over the built-in RAGTool. AIDP calls setup() once, then
    invoke() per user turn. There is no citation post-processing — the model
    produces the inline markers and the Sources list on its own.
    """

    def __init__(self) -> None:
        self.agent = None

    def setup(self) -> None:
        """Build the ReAct agent once: an OCI LLM + the built-in RAGTool + the
        citation-instructing system prompt. Uses AIDP's injected `checkpointer`
        for conversation memory when available, and falls back without it."""
        logger.info(llm_conf)
        oci_llm = init_oci_llm(llm_conf)
        try:
            if checkpointer:
                self.agent = create_react_agent(model=oci_llm, tools=agent_tools, prompt=agent_system_prompt, debug=True, checkpointer=checkpointer)
            else:
                self.agent = create_react_agent(model=oci_llm, tools=agent_tools, prompt=agent_system_prompt, debug=True)
        except Exception as e:
            # Fallback compile without checkpointer if wiring fails
            logger.warning(f"Checkpointer could not be initialized {e}")
            self.agent = create_react_agent(model=oci_llm, tools=agent_tools, prompt=agent_system_prompt, debug=True)
        logger.info(f"Setup for agent completed {self.agent}")

    async def invoke(self, user_query: str, **kwargs):
        """Handle one user turn: run the agent and return its result unchanged.
        The cited answer (inline [n] markers + Sources list) is whatever the model
        wrote — there is no code that verifies or reformats it."""
        config = pre_invoke_setup(**kwargs)
        user_message = HumanMessage(content=user_query)
        message = {"messages": [dict(user_message)]}
        try:
            return await self.agent.ainvoke(input=message, config=config)
        except Exception as e:
            import traceback
            logger.error(f"Exception while calling invoke {e}", exc_info=True)
            print("Stack trace:\n", traceback.format_exc())


import asyncio


async def main(user_query=""):
    """Local smoke test only. AIDP runs the agent by calling setup()/invoke()."""
    test_agent = RAGCitationsViaSystemPrompt()
    test_agent.setup()
    result = await test_agent.invoke(user_query)
    result["messages"][-1].pretty_print()


if __name__ == "__main__":
    asyncio.run(main(user_query="What is Balance Sheet Ratios? How is it calculated?"))

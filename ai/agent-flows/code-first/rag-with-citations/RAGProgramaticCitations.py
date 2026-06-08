"""
RAGProgramaticCitations — an AIDP agent that answers Oracle Fusion questions and backs
every claim with numbered, score-ranked source citations.

What it produces
----------------
A natural-language answer where each factual statement carries an inline marker
([1], [2], ...), followed by a "Sources" list mapping each number to its source
document and that document's retrieval (cosine-similarity) score — higher means
more relevant. The same citations are also attached to the result as structured
data (result["citations"]) for programmatic callers / UIs.

Why it is built this way
------------------------
- The RAG tool returns each retrieved chunk's similarity score, so we can show
  the user how strongly each cited fact is supported.
- Each chunk gets a stable "ref" derived from (document_id, score). This lets the
  model cite an exact piece of evidence even when the tool is called several
  times and the same document comes back with a different score each time.
- After the model answers, we renumber those refs to friendly [1], [2] markers
  and, by default, list only the sources actually cited (see CITE_ALL_RETRIEVED).

Sequence of calls (one user turn)
---------------------------------
1. RAGProgramaticCitations.setup()        — build the ReAct agent over fusion_pdf_reader.
2. RAGProgramaticCitations.invoke(query)  — run the agent, then post-process:
   a. agent.ainvoke(...)           — ReAct loop; the model calls fusion_pdf_reader
                                      (possibly more than once) and writes [ref=...].
   b. fusion_pdf_reader(query)      — retrieve, score-filter, tag each chunk's ref.
   c. append_citations_block(result):
        - collect_rag_citations()  — gather every cite-able chunk from the tool
                                     messages, deduped by ref.
        - renumber [ref=...] -> [n] in the answer (first-appearance order).
        - append the scored "Sources" list and set result["citations"].
"""

from aidputils.agents.toolkit.tool_helper import create_langgraph_tool
from aidputils.agents.toolkit.agent_helper import init_oci_llm, pre_invoke_setup

from aidputils.agents.toolkit.configs import AIDPToolConf, OCIAIConf
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
import logging
import json
import re
import hashlib

logger = logging.getLogger('rag_with_citations')
checkpointer = globals().get("checkpointer", None)

##### REPLACE with your OCI details and knowledge base coordinates #####
compartment_ocid = "<your-compartment-ocid>"
region = "<oci-region>"
catalog = "<your-catalog-name>"
schema = "<your-schema-name>"
knowledge_base = "<your-knowledge-base-name>"
# Pick model(s) available in YOUR region (availability is region-specific).
# See the OCI Generative AI docs for the model IDs offered in each region.
# Two models are configured independently:
#   - rag_tool_model_id — the LLM inside RAGTool. Its generated answer is
#     discarded (fusion_pdf_reader keeps only the retrieved chunks), so a fast,
#     inexpensive model is a good fit here and keeps each tool call quick.
#   - agent_model_id — the LLM the agent uses to compose the final cited answer
#     the user sees; prefer a stronger model here for answer quality.
rag_tool_model_id = "<oci-genai-model-id>"
agent_model_id = "<oci-genai-model-id>"
endpoint = f"https://inference.generativeai.{region}.oci.oraclecloud.com"

MIN_CHUNK_SCORE = 0.62
MIN_TOP_SCORE = 0.65
TOP_N_AFTER_FILTER = 5
SCORE_DECIMALS = 3
RAG_TOOL_NAME = "fusion_pdf_reader"

# Sources list policy:
#   False (default) — list only documents the model actually cited inline.
#   True            — list every retrieved+scored chunk: cited ones first (in
#                     citation order), then the remaining retrieved evidence by
#                     score descending.
CITE_ALL_RETRIEVED = False


def _make_ref(document_id: str, score: float) -> str:
    """Stable, collision-resistant citation handle for one (document, score) pair.

    The agent may call the RAG tool several times, and the SAME chunk can come
    back with a DIFFERENT score each time — so document_id alone is not a safe
    citation key. Hashing (document_id, score) gives each distinct retrieval its
    own ref: identical (document_id, score) pairs across calls collapse to one
    ref, while different scores for the same document stay distinguishable.
    """
    digest = hashlib.sha1(
        f"{document_id}|{round(float(score), 6)}".encode("utf-8")
    ).hexdigest()
    return "s" + digest[:8]


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
            "model_id": rag_tool_model_id,
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

_rag_underlying = create_langgraph_tool(rag_conf.model_dump())


@tool
async def fusion_pdf_reader(query: str) -> dict:
    """Retrieves relevant chunks from Oracle Fusion PDFs to help answer user questions.

    Returns the retrieved evidence — NOT a pre-written answer. The agent composes
    the response itself from the chunk contents and cites each one by its ref.

      - status="ok": retrieved_chunks = [{ref, document_id, score, content}, ...].
      - status="no_match": top score below the confidence threshold; the agent
        must refuse rather than fabricate.
    """
    raw = await _rag_underlying.ainvoke({"query": query})

    # Normalize RAGTool output (dict, MCP envelope, or JSON string) into a list of
    # retrieved chunks. We only need the retrieved evidence; RAGTool also returns
    # its own generated "answer", but we deliberately drop it — the agent reasons
    # over the chunks directly, which keeps each claim tied to a citable chunk.
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raw = {}
    if not isinstance(raw, dict):
        raw = {}
    if "retrieved_chunks" in raw:
        retrieved = raw.get("retrieved_chunks")
    else:
        retrieved = ((raw.get("result") or {}).get("structuredContent") or {}).get("retrieved_chunks")

    chunks = [
        c for c in (retrieved or [])
        if isinstance(c.get("score"), (int, float)) and c["score"] >= MIN_CHUNK_SCORE
    ]
    chunks.sort(key=lambda c: c["score"], reverse=True)
    chunks = chunks[:TOP_N_AFTER_FILTER]

    # Tag each chunk with a stable ref the model quotes inline. This is what makes
    # citations unambiguous across multiple tool calls (see _make_ref).
    for c in chunks:
        c["ref"] = _make_ref(c["document_id"], c["score"])

    top_score = chunks[0]["score"] if chunks else 0.0
    if not chunks or top_score < MIN_TOP_SCORE:
        return {
            "status": "no_match",
            "reason": f"Retrieval confidence too low (top score {top_score:.3f} < {MIN_TOP_SCORE}).",
        }

    return {"status": "ok", "retrieved_chunks": chunks}


agent_tools = [fusion_pdf_reader]


llm_conf = OCIAIConf(
    model_provider='generic',
    compartment_id=compartment_ocid,
    model_args={},
    endpoint=endpoint,
    model_id=agent_model_id,
    guardrails_config={
        "name": "Default Guardrails",
        "description": "Default empty guardrails configuration",
        "policies": []}
    )

agent_system_prompt = """Be a helpful assistant to answer questions related to Oracle's Fusion Applications.
- Use "fusion_pdf_reader" to find details of the metric including what it is and how it is calculated.
- You may call "fusion_pdf_reader" more than once with different queries when needed.

Rules for using fusion_pdf_reader:
1. If it returns status="ok", compose your answer using ONLY the content of the
   chunks it returned. Every chunk has a "ref" field. Cite each factual claim
   inline as [ref=<ref>], using the ref of the specific chunk that supports it.
2. If it returns status="no_match", reply EXACTLY:
   "I could not find a confident answer in the knowledge base for this question."
   Do not draw on prior knowledge. Do not speculate.
3. Only use refs that appear in the tool output — never invent one. When you make
   several calls, cite the ref from whichever call returned the supporting chunk.

Do NOT write a "Sources" or "References" section yourself — a scored Sources
table is appended automatically after your reply. Never write scores yourself.
"""


def collect_rag_citations(result: dict, tool_name: str = RAG_TOOL_NAME) -> list[dict]:
    """Gather every retrieved+scored chunk across ALL fusion_pdf_reader calls.

    Keyed by ref: the same (document_id, score) seen in multiple calls collapses
    to a single citation, while the same document at different scores is kept as
    distinct citations. Returned sorted by score descending.

    `result` is the LangGraph state the agent returns. We only care about the
    fusion_pdf_reader ToolMessages inside it. Shape (abridged, dummy values):

        result = {
          "messages": [
            HumanMessage(content="What is Balance Sheet Ratios?"),
            AIMessage(content="", tool_calls=[{"name": "fusion_pdf_reader", ...}]),
            ToolMessage(                       # what fusion_pdf_reader returned
              name="fusion_pdf_reader",
              content='{                       # a JSON string (LangGraph serializes it)
                "status": "ok",
                "retrieved_chunks": [
                  {"document_id": "erp.pdf#chunk_414", "score": 0.735,
                   "ref": "s294d835a", "content": "Current Ratio = ..."},
                  ...
                ]}'),
            AIMessage(content="Current Ratio ... [ref=s294d835a]."),   # final answer
          ]
        }

    So: walk messages -> keep fusion_pdf_reader ToolMessages -> JSON-decode their
    content -> read each chunk's document_id / score / ref.
    """
    by_ref = {}
    for msg in result.get("messages", []):
        if not isinstance(msg, ToolMessage) or getattr(msg, "name", None) != tool_name:
            continue
        # The ToolMessage carries this tool's own normalized output (a dict, or
        # its JSON-serialized form); read the retrieved_chunks back out.
        content = msg.content
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                continue
        if not isinstance(content, dict):
            continue
        for c in content.get("retrieved_chunks") or []:
            doc, score = c.get("document_id"), c.get("score")
            if doc is None or score is None:
                continue
            ref = c.get("ref") or _make_ref(doc, score)
            if ref in by_ref:
                continue
            by_ref[ref] = {"ref": ref, "document_id": doc, "score": float(score)}
    cites = list(by_ref.values())
    cites.sort(key=lambda c: c["score"], reverse=True)
    return cites


def append_citations_block(result: dict) -> None:
    """Turn the model's raw [ref=...] markers into reader-friendly [1], [2] numbers
    and append a scored "Sources" list to the final answer.

    What it does, end to end:
      1. Collect every cite-able chunk (deduped by ref) from the tool messages.
      2. Find the final assistant message and remove any Sources section the model
         may have written itself — we always substitute our own, authoritative one.
      3. Choose which sources to list: only those cited inline (default), or all
         retrieved chunks when CITE_ALL_RETRIEVED is True.
      4. Number them by first appearance, rewrite the inline markers to [n], append
         the scored Sources list, and expose the same data as result["citations"].
    """
    cites = collect_rag_citations(result)  # all retrieved+scored, score desc, deduped
    if not cites:
        return

    # The final assistant message holds the prose we annotate.
    target = next((m for m in reversed(result["messages"])
                   if isinstance(m, AIMessage) and m.content), None)
    if target is None:
        return

    # Inline markers the model was told to emit, e.g. "[ref=s294d835a]".
    inline_cite_re = re.compile(r"\[ref=([^\]]+)\]")

    # Defensively strip any "Sources"/"References"/"Citations" section the model
    # wrote on its own, so only our authoritative, scored block remains. We match
    # ONLY a real header line — either decorated (**Sources**, ## Sources) or the
    # bare keyword followed by a colon (Sources:) — anchored to the start of a line
    # (re.MULTILINE). A line that merely begins with the word as prose ("Sources of
    # this data are ...") is NOT a header, so it is left intact and the inline
    # [ref=...] citations within the answer are preserved.
    sources_section_re = re.compile(
        r"^[ \t]*(?:-{3,}[ \t]*\n[ \t]*)?"                 # optional '---' rule before the header
        r"(?:"
        r"(?:\#{1,6}[ \t]*(?:\*\*|__)?|(?:\*\*|__))[ \t]*(?:sources|references|citations)\b[^\n]*"  # decorated header (## or **)
        r"|(?:sources|references|citations)[ \t]*:[ \t]*"                                           # bare 'Sources:' header
        r")(?:\n.*)?\Z",                                   # then everything to the end of the answer
        flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )
    body = sources_section_re.sub("", target.content).rstrip()

    # refs in the order the model first cites them inline.
    cited_order, seen = [], set()
    for m in inline_cite_re.finditer(body):
        ref = m.group(1).strip()
        if ref not in seen:
            seen.add(ref)
            cited_order.append(ref)

    # Sources to render: cited ones in citation order; if the policy is to show
    # everything retrieved, append the uncited remainder by score descending.
    by_ref = {c["ref"]: c for c in cites}
    ordered = [by_ref[r] for r in cited_order if r in by_ref]
    if CITE_ALL_RETRIEVED:
        ordered += [c for c in cites if c["ref"] not in seen]
    if not ordered:
        target.content = body
        return

    # Number by appearance: [1] is the first source cited, and so on. The same
    # document retrieved at two scores keeps two refs, hence two numbered rows.
    # Uncited extras (only when CITE_ALL_RETRIEVED) continue the count after.
    number_by_ref = {}
    lines = ["", "---", "**Sources** (retrieval score):"]
    for i, c in enumerate(ordered, start=1):
        c["index"] = i
        number_by_ref.setdefault(c["ref"], i)
        lines.append(f"- [{i}] `{c['document_id']}` — score `{c['score']:.{SCORE_DECIMALS}f}`")

    # Rewrite each [ref=<X>] to its [n]. Unknown refs (not in any tool output, or
    # hallucinated) are left untouched so the discrepancy stays visible.
    def _to_number(m):
        n = number_by_ref.get(m.group(1).strip())
        return f"[{n}]" if n else m.group(0)
    target.content = inline_cite_re.sub(_to_number, body) + "\n".join(lines)

    # Same citations as structured data for programmatic callers / UIs. It also
    # carries each source's `ref` and number, which the prose Sources list omits.
    result["citations"] = [
        {"index": c["index"], "ref": c["ref"],
         "document_id": c["document_id"], "score": c["score"]}
        for c in ordered
    ]


class RAGProgramaticCitations:
    """The deployable AIDP agent flow.

    A ReAct agent over a single tool (fusion_pdf_reader) that answers Oracle
    Fusion questions and cites its sources with scores. AIDP calls setup() once,
    then invoke() for each user turn.
    """

    def __init__(self) -> None:
        self.agent = None

    def setup(self) -> None:
        """Build the ReAct agent once: an OCI LLM + the fusion_pdf_reader tool +
        the citation-rules system prompt. Uses AIDP's injected `checkpointer` for
        conversation memory when it is available, and falls back without it."""
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
        """Handle one user turn: run the ReAct agent (which may call
        fusion_pdf_reader one or more times), then post-process the answer to add
        numbered, scored citations. Returns the LangGraph result dict, with the
        cited sources also available as result["citations"]."""
        config = pre_invoke_setup(**kwargs)
        user_message = HumanMessage(content=user_query)
        message = {"messages": [dict(user_message)]}
        try:
            result = await self.agent.ainvoke(input=message, config=config)
            append_citations_block(result)
            return result
        except Exception as e:
            import traceback
            logger.error(f"Exception while calling invoke {e}", exc_info=True)
            print("Stack trace:\n", traceback.format_exc())


import asyncio


async def main(user_query=""):
    """Local smoke test only. AIDP runs the agent by calling setup()/invoke()
    directly, so this __main__ path is just for running the file by hand."""
    test_agent = RAGProgramaticCitations()
    test_agent.setup()
    result = await test_agent.invoke(user_query)

    # The deliverable: the answer with inline [n] markers and the scored Sources
    # block already embedded in the final message.
    result["messages"][-1].pretty_print()

    # The SAME citations as a structured object (adds each source's `ref`), shown
    # once here purely to illustrate that callers can consume result["citations"]
    # programmatically instead of parsing the prose.
    print("\nresult['citations'] =", json.dumps(result.get("citations", []), indent=2))


if __name__ == "__main__":
    asyncio.run(main(user_query="What is Balance Sheet Ratios? How is it calculated?"))

# Sample Output
"""
Balance Sheet Ratios refer to a set of financial metrics in Oracle Fusion ERP Analytics, including Current Ratio, Debt to Equity, EBIT YTD, and Interest Expense YTD [1][2].

These metrics are calculated as follows:
- Current Ratio: current assets / current liabilities [1].
- Debt to Equity: total liabilities / total shareholders funds, where total liabilities = current liabilities + long term liabilities [2].
- EBIT YTD: revenue YTD - cost of revenue YTD - sales & marketing expenses YTD - other operating expenses YTD - R&D expenses YTD - depreciation expenses YTD [2].
---
**Sources** (retrieval score):
- [1] `<volume-path>/reference-fusion-erp-analytics.pdf#chunk_414` — score `0.735`
- [2] `<volume-path>/reference-fusion-erp-analytics.pdf#chunk_416` — score `0.633`

result['citations'] = [
  {"index": 1, "ref": "s294d835a", "document_id": "<volume-path>/reference-fusion-erp-analytics.pdf#chunk_414", "score": 0.735},
  {"index": 2, "ref": "s3de193c3", "document_id": "<volume-path>/reference-fusion-erp-analytics.pdf#chunk_416", "score": 0.633}
]
"""

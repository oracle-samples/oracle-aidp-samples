# RAG Agent with Source Citations

Two high-code (`type=CODE`) agent flows that answer Oracle Fusion Applications
questions **and show where each fact came from**, including the retrieval
(similarity) score of every cited source. Both use the built-in **RAGTool**
against a knowledge base of Fusion PDFs; they differ only in **how the citations
are produced** — in code, or by the model.

| File | Citations produced by | Scores accessed in code? | Reliability |
|---|---|---|---|
| `RAGProgramaticCitations.py` | Python post-processing | **Yes** | High — deterministic, deduped, numbered, machine-readable |
| `RAGCitationsViaSystemPrompt.py` | The model, via the system prompt | **No** | Best-effort — depends on the model |

---

## Background: what RAGTool returns

For each query, the built-in RAGTool runs a vector search and returns a JSON
payload that includes a `retrieved_chunks` list. Every chunk carries:

- `document_id` — the source document/chunk it came from,
- `content` — the chunk text,
- `score` — a cosine-similarity relevance score in `[0, 1]` (higher = more
  relevant).

Both samples build their citations from that `document_id` + `score`. The only
question is who turns it into a clean, numbered "Sources" list.

---

## `RAGProgramaticCitations.py` — citations built in code (recommended)

A **ReAct agent over a thin wrapper tool** (`fusion_pdf_reader`) plus a
post-processing step. The wrapper and the post-processor do the work so the model
only has to drop a marker next to each claim.

**How it works (one user turn):**

1. `setup()` builds the ReAct agent over `fusion_pdf_reader`.
2. `invoke(query)` runs the agent, then calls `append_citations_block(result)`:
   - `fusion_pdf_reader(query)` calls the built-in RAGTool, keeps only chunks
     above a score threshold, and tags each with a stable **`ref`** derived from
     `(document_id, score)` (`_make_ref`). It returns the chunks only — the
     RAGTool's own generated answer is intentionally dropped so the agent reasons
     directly over the evidence.
   - The model composes the answer and cites each claim inline as `[ref=<ref>]`.
   - `collect_rag_citations()` gathers every cited-able chunk from **all** tool
     calls in the turn, deduped by `ref`.
   - The `[ref=...]` markers are renumbered to friendly `[1]`, `[2]` in first-
     appearance order, a scored **Sources** list is appended, and the same
     citations are attached to `result["citations"]` for programmatic callers.

**Why the `ref` matters:** an agent can call the RAG tool several times, and the
same chunk can come back with a *different* score each time. Keying citations on
`document_id` alone would conflate those; hashing `(document_id, score)` keeps a
re-retrieved-at-a-different-score chunk distinguishable, while identical
retrievals across calls collapse to one citation.

**Tuning knobs** (top of the file): `MIN_CHUNK_SCORE`, `MIN_TOP_SCORE`,
`TOP_N_AFTER_FILTER`, `SCORE_DECIMALS`, and `CITE_ALL_RETRIEVED` (list only the
sources actually cited — the default — or every retrieved chunk).

**Example output:**

```
Balance Sheet Ratios refer to a set of financial metrics in Oracle Fusion ERP
Analytics, including Current Ratio, Debt to Equity, and EBIT YTD [1][2].

- Current Ratio: current assets / current liabilities [1].
- Debt to Equity: total liabilities / total shareholders funds [2].
---
**Sources** (retrieval score):
- [1] `.../reference-fusion-erp-analytics.pdf#chunk_414` — score `0.735`
- [2] `.../reference-fusion-erp-analytics.pdf#chunk_416` — score `0.633`
```

The same citations are also available as structured data:

```python
result["citations"] == [
  {"index": 1, "ref": "s294d835a", "document_id": ".../chunk_414", "score": 0.735},
  {"index": 2, "ref": "s3de193c3", "document_id": ".../chunk_416", "score": 0.633},
]
```

---

## `RAGCitationsViaSystemPrompt.py` — citations via the prompt (simplest)

The built-in RAGTool is handed to the agent **directly**, with no wrapper and no
citation code. Because the tool's JSON output (with `document_id` and `score`)
lands in the conversation, the **system prompt** can instruct the model to:

- answer only from the retrieved chunks,
- cite each claim inline as `[1]`, `[2]`,
- end with a `Sources` list formatted as `[n] <document_id> — score <score>`.

`invoke()` returns the model's reply **as-is** — there is no code that verifies or
reformats the citations.

**Trade-offs vs. the code version:**

- ✅ Much less code; nothing to maintain beyond the prompt.
- ⚠️ The model may round/miscopy scores, duplicate or skip sources, or drift from
  the requested format.
- ⚠️ No deduplication across multiple tool calls and no machine-readable
  `result["citations"]`.

---

## When to use which

- **Use `RAGProgramaticCitations.py`** when citations are part of the product — they must
  be accurate, consistently formatted, deduplicated across multiple retrievals,
  and/or consumed by a UI or downstream system (via `result["citations"]`).
- **Use `RAGCitationsViaSystemPrompt.py`** for a quick prototype, a demo, or when
  approximate, model-authored citations are good enough and you want minimal code.

Both files are standalone agent flows — deploy **one** of them per agent flow.

---

## Deploy

1. Upload your Fusion PDFs to an AIDP Workbench standard catalog volume, and
   create a **knowledge base** over them (add the volume as a data source).
2. Create a new agent flow using the **code authoring** mode.
3. Upload the file you want to use (`RAGProgramaticCitations.py` **or**
   `RAGCitationsViaSystemPrompt.py`) to your agent flow folder.
4. In that file, replace the placeholders near the top: `<your-compartment-ocid>`,
   `<oci-region>`, `<your-catalog-name>`, `<your-schema-name>`,
   `<your-knowledge-base-name>`, and `<oci-genai-model-id>` (choose a model
   available in your region — OCI Generative AI model availability is
   region-specific). `RAGProgramaticCitations.py` sets the model in two places:
   `rag_tool_model_id` (RAGTool's internal synthesis, which is discarded — a
   fast, inexpensive model works well) and `agent_model_id` (composes the final
   cited answer — prefer a stronger model); both default to the
   `<oci-genai-model-id>` placeholder.
5. Set that file as the **entry file**.
6. Attach an AI compute.
7. Run the Python file, then test the agent from the **Test** tab of your agent
   flow (or deploy it as a TEST/PROD endpoint).

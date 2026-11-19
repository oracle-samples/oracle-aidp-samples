---
name: aidp-knowledge-bases
description: Build and manage AIDP Knowledge Bases for RAG — create a KB over catalog data, pick an embedding model + chunking, build an HNSW/IVF vector index, add data sources (volume/table), run ingestion jobs, and manage KB permissions. Use when the user wants RAG, a knowledge base, document/vector search, embeddings, semantic retrieval, or to feed a RAG_TOOL agent-flow node. This is the corpus/index layer; the agent that queries it is authored in aidp-agent-flows (RAG_TOOL) or aidp-agent-highcode.
---

# `aidp-knowledge-bases` — RAG corpus + vector index

A Knowledge Base is AIDP's managed RAG store: it embeds source data into a vector index you can retrieve from
(directly, or via a `RAG_TOOL` node in an agent flow). Runs over the **LA AgentFlows family** REST API.

> **Engine:** `oci raw-request --profile DEFAULT` (no CLI group for KB in v1.0.0). **Lake-scoped**, live-verified
> 2026-06-10: `GET …/dataLakes/<ocid>/knowledgeBases?catalogKey=<key>&schemaKey=<key>` → **400 InvalidParameter**
> means the route exists but needs **real catalog/schema keys** (resolve via `aidp-catalog-explore`); the
> workspace-scoped path 404s. Confirm with a live read before any write; record in `references/rest-endpoint-map.md`.
>
> **Live-verified 2026-06-10 on de-agent — correction:** a bare `GET …/knowledgeBases` (no query string)
> returns **400 InvalidParameter** requiring **both** `schemaKey` and `catalogKey` query params — the route is
> provisioned (lake-scoped), it is the missing params that 400, not a missing route.

## When to use
- "Build a RAG / knowledge base / vector index", "embed these docs/tables", "semantic search over X",
  or any prerequisite for a `RAG_TOOL` agent-flow node.
- NOT ad-hoc LLM-in-SQL (→ `aidp-ai-sql`); NOT the agent that *uses* the KB (→ `aidp-agent-flows` / `aidp-agent-highcode`).

## Create a KB (`CreateKnowledgeBaseDetails`, camelCase wire fields)
`POST …/dataLakes/<ocid>/knowledgeBases`
```json
{
  "displayName": "policy_kb",
  "description": "RAG over policy docs",
  "catalogKey": "<catalog-key>", "schemaKey": "<schema-key>",
  "workspaceKey": "<ws-key>", "clusterKey": "<cluster-key>",
  "type": "...", "modality": "...",
  "embeddingModelSourceType": "...", "embeddingModelName": "<embedding-model>",
  "chunkSize": 512, "chunkOverlap": 64,
  "sourceFilePattern": "*.pdf",
  "indexDetails": { "type": "HNSW", "distance": "COSINE", "neighbors": 32, "efConstruction": 200, "targetAccuracy": 95 }
}
```
- `indexDetails.type` ∈ **`HNSW` | `IVF`**. **Both** index types accept the full 7-value `distance` enum:
  `COSINE` | `DOT` | `EUCLIDEAN` | `HAMMING` | `JACCARD` | `L2_SQUARED` | `MANHATTAN`
  (SDK `KbVHnswIndexDetails` / `KbVIvfIndexDetails` `distance` setter `allowed_values` —
  `kb_v_hnsw_index_details.py:110`, `kb_v_ivf_index_details.py:110`). Tuning values below are **illustrative**.
- **HNSW tuning params** (SDK `KbVHnswIndexDetails`, camelCase wire fields, all `int` — `kb_v_hnsw_index_details.py:74-79`):

  | Field | Meaning |
  |---|---|
  | `neighbors` | max neighbors each vector can have on any layer (the HNSW *M* parameter) — `:146` |
  | `efConstruction` | max closest-vector candidates considered during index construction — `:170` |
  | `targetAccuracy` | target accuracy percentage 1–100 — `:122` |
- **IVF tuning params** (SDK `KbVIvfIndexDetails`, camelCase wire fields, all `int` — `kb_v_ivf_index_details.py:74-79`):

  | Field | Meaning |
  |---|---|
  | `neighborPartitions` | number of partitions (clusters) to divide the vector data into — `:146` |
  | `neighborPartitionProbes` | max partitions to probe during a search (higher = more accurate, slower) — `:170` |
  | `targetAccuracy` | target accuracy percentage 1–100 — `:122` |
- `embeddingModelName`/`SourceType` — list available embedding models via `aidp-models-catalog`
  (`modelType=EMBEDDING`); needs a RUNNING `clusterKey` for embedding compute.
- **Verify-first:** the field *names* above are from the SDK `CreateKnowledgeBaseDetails`; the create was not
  round-tripped (it triggers embedding compute). Confirm the embedding-model + `type`/`modality` enums against
  a live read / `aidp help` before a production create — do not invent values.

## Ingest + maintain
| Action | Endpoint / body |
|---|---|
| Add/remove a data source | `UpdateKnowledgeBaseAddSourceDetails` / `…DeleteSourceDetails` (source kind volume / table) |
| Run an ingestion job | `POST …/knowledgeBases/<key>/jobs` — `CreateKnowledgeBaseJobDetails {displayName, type, goal, sources, sourceKey, schedule}`; trigger runs via `…/jobs/<key>/jobRuns` |
| List job runs / status | `GET …/knowledgeBases/<key>/jobs/<key>/jobRuns` |
| Permissions | `assign`/`manage`/`revoke` KnowledgeBasePermission (`aidp-roles-access` for principals) |
| Update / delete KB | `PUT`/`DELETE …/knowledgeBases/<key>` |

## Wire it to an agent
Once the index is built, reference the KB from a **`RAG_TOOL`** node (`aidp-agent-flows`,
`references/agent-flow-nodes.md`) or from high-code (`aidp-agent-highcode`). The KB must exist + be ingested
before the RAG tool can retrieve.

## Guardrails
- Mutation gate: KB create/ingest consumes embedding compute — show the body, confirm first, persist to
  `.aidp/payloads/` ([references/payloads.md](../../references/payloads.md)).
- Resolve `catalogKey`/`schemaKey`/`clusterKey` (real keys) first via `aidp-catalog-explore` / `aidp-cluster-ops`.

## References
- [aidp-agent-flows](../aidp-agent-flows/SKILL.md) (RAG_TOOL consumer) · [aidp-models-catalog](../aidp-models-catalog/SKILL.md) (embedding models) · [aidp-catalog-explore](../aidp-catalog-explore/SKILL.md) (keys)
- [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md) · [references/payloads.md](../../references/payloads.md)

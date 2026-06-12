---
name: aidp-models-catalog
description: List the models available/installed in the AIDP DataLake and inspect their parameters, and browse the MLOps model registry. Use when the user asks "what models are available", needs a model name for ai_generate()/agent flows, wants a model's parameters, or wants to list registered models / versions. Registry browse uses the official `aidp` CLI (folded into the `mlops` group); the LA `Models` platform-catalog API via `oci raw-request` is verified and the no-CLI fallback.
---

# `aidp-models-catalog` — available models & parameters

Two distinct surfaces:
- **Platform model catalog** — what you can call from `ai_generate()` / agent flows (LA `Models` API).
- **MLOps/MLflow registry** — registered models + versions (the `aidp-mlops` group; see also `aidp-mlops`).

**CLI (preferred) — registry browse:** `aidp mlops <command> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`
- `aidp mlops list-registered-models | get-registered-model | list-model-versions`
- (The official CLI folds the model registry into the `mlops` group — there is no separate `models` group.)

**Fallback (no CLI) / platform catalog:** the LA `Models` REST API via `oci raw-request` (identical
endpoint + auth) — this is the verified path for resolving an `ai_generate()` model name.

> **Verify-first (no-fabrication):** the platform `Models` API is **Limited Availability** on `20240831`;
> the `?modelType=` list call is **LIVE-VERIFIED 200** below. The MLOps registry side is **Preview**
> (not yet probed). Confirm a name with a live read before relying on it; record in
> `references/rest-endpoint-map.md`.

## When to use
- "What models are available?", "what's the model name for `ai_generate`?", "show model parameters",
  "list registered models / model versions".

## Platform catalog — endpoints (DataLake-scoped; LA `20240831`)
Base: `https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/…`

- `GET /models?modelType=<GENERATIVE_AI|BASE|EMBEDDING|LLM>` — list installed models (**LIVE-VERIFIED 200**)
- `GET /models/{id}` — model detail
- `GET /models/{id}/modelParameters` — parameters

## Workflow
1. **Platform catalog:** `GET /models?modelType=GENERATIVE_AI` (auth ladder; on 401/403 → session refresh)
   → present available model names/ids. Repeat with `BASE` / `EMBEDDING` / `LLM` to enumerate other catalogs.
   `GET /models/{id}` / `…/modelParameters` for detail.
2. **Registry browse:** `aidp mlops list-registered-models` → `list-model-versions` (CLI preferred).
3. Feed the confirmed model name to `aidp-ai-sql` (`ai_generate('<model>', …)`) or `aidp-agent-flows`.

```bash
oci raw-request --http-method GET \
  --target-uri "https://aidp.us-ashburn-1.oci.oraclecloud.com/20240831/dataLakes/<DATALAKE_OCID>/models?modelType=GENERATIVE_AI" \
  --profile DEFAULT
```

## Notes
- Use this to **resolve a real model name** before `ai_generate` rather than guessing one.
- Read-only; no destructive ops.
- A bare `GET /models` may 400 on a required param — pass `modelType` (verified above) to enumerate.
- **`ai_generate` availability is independent of this catalog.** `GET /models?modelType=GENERATIVE_AI`
  can return `items: []` on a fresh instance while `ai_generate('openai.gpt-5.4', …)` still executes fine —
  the model is resolved at the **Spark engine** level, not via this REST catalog. An empty list here does
  **not** mean `ai_generate` is unavailable. Confirm AI-in-SQL with the smoke test in `aidp-ai-sql`
  (a trivial `SELECT ai_generate('<model>', 'hello')` cell), not with this endpoint.

## References
- [references/aidp-cli-map.md](../../references/aidp-cli-map.md) · [references/oci-raw-request.md](../../references/oci-raw-request.md) · [references/no-mcp-rest-map.md](../../references/no-mcp-rest-map.md) · [references/rest-endpoint-map.md](../../references/rest-endpoint-map.md) · pairs with `aidp-ai-sql`, `aidp-agent-flows`, `aidp-mlops`

# Accessing Session Variables in a High-Code Agent Flow

Session variables let a calling application pass **per-request, out-of-band
context** — such as `user_id`, `tenant_id`, or a topic — to a deployed agent
flow. The values travel in the request's `metadata` field, separate from the
user's message, and your code decides how to use each one: keep it server-side
(e.g. `user_id` for a lookup) or deliberately surface it to the model (e.g.
render `tenant_id` into the system prompt). This sample shows both.

This example shows the **three ways** to access a session variable inside a
high-code (`type=CODE`) agent flow, using one small blog-title agent.

## The three patterns

| Pattern | How | Used here for |
|---|---|---|
| **1. Programmatically** | `chat_context.session_context_var.get().get("sessionvariables.<name>")` (see `get_session_var`) | `user_id` — read inside the `fetch_user_id` tool, and available in `invoke()` |
| **2. Tool template** | a `{{sessionvariables.<name>}}` placeholder inside a built-in tool's config | `topic_name` — the `PromptTool` generates blog titles for it |
| **3. System prompt** | `create_get_dynamic_messages(template)` renders `{{sessionvariables.<name>}}` into the system prompt each turn | `tenant_id` |

Each session variable is delivered to your code as a `{"name", "value"}` dict
under the lowercase key `sessionvariables.<name>`.

## Deploy

1. Create a new agent flow using the **code authoring** mode.
2. Upload the contents of this folder to your agent flow folder.
3. In `agent.py`, replace the placeholders: `<your-compartment-ocid>`,
   `<oci-region>`, and `<oci-genai-model-id>`.
4. Set `agent.py` as the entry file.
5. Attach an AI compute.
6. Deploy the agent flow (TEST or PROD).

## Pass session variables when calling the endpoint

Put the variables in the top-level **`metadata`** object of the chat request,
using **lowercase `sessionvariables.<name>` keys with string values**:

```bash
oci raw-request --http-method POST \
  --target-uri "https://gateway.aidp.<oci-region>.oraclecloud.com/agentendpoint/<agent-flow-key>/chat" \
  --request-headers '{ "x-session-id": "my-session-key" }' \
  --request-body '{
    "isStreamEnabled": false,
    "input": [ { "role": "user", "content": [ { "type": "INPUT_TEXT", "text": "Generate my blog titles." } ] } ],
    "metadata": {
      "sessionvariables.user_id":    "U_102",
      "sessionvariables.tenant_id":  "acme",
      "sessionvariables.topic_name": "Kubernetes autoscaling"
    }
  }'
```

The agent replies with the `user_id` (Pattern 1), 2 blog titles for `topic_name`
(Pattern 2), and the `tenant_id` (Pattern 3).

Notes:
- **Keys must use the lowercase `sessionvariables.` prefix** and **string
  values** (an object value is stringified by the gateway).
- Session variables are **per request** — resend `metadata` on every turn.
  Add an `x-session-id` header to thread conversation history across turns.

## Running locally on AIDP Workbench

`agent.py` includes a `main()` that passes the variables directly as the
`session_variables` kwarg, so you can run the file from the AIDP Workbench code
editor to exercise the agent before deploying.

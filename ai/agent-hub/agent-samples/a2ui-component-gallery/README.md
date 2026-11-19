# A2UI Component Gallery

A code-first AIDP agent that renders an interactive gallery for A2UI v0.8 and
v0.9 clients. The model selects a component template; deterministic Python code
builds and validates every version-correct A2UI operation before it is returned.

The 31 v0.9 previews comprise the gallery/index surface plus all 30 component
types declared by the supplied complete catalog. The v0.8 compatibility path
renders its 29 supported component types; `OAPopup` is v0.9-only.

The sample is deliberately a gallery rather than a business workflow. Use it
to learn component shapes, inspect action payloads, and copy validated builder
patterns into your own agents.

## What this sample demonstrates

- Highest-mutually-supported A2UI v0.9 → v0.8 protocol negotiation.
- Version-correct surface creation and incremental updates.
- Client capability negotiation through `metadata.a2uiClientCapabilities`.
- Standard and Oracle Agent Hub component catalogs.
- Deterministic UI builders; the LLM never writes raw A2UI JSON.
- Schema validation and one repair attempt before returning a response.
- Action handling for component selection, chart controls, data-grid controls,
  buttons, and action cards.
- A plain-text fallback when a client explicitly advertises no A2UI support.

See [COMPONENTS.md](./COMPONENTS.md) for the complete component and action
reference.

## Files

```text
a2ui-component-gallery/
├── .gitignore               # Excludes the private deployment override
├── agent.py                 # AIDP entry point: A2UIComponentGallery
├── deployment_config.example.py
├── THIRD_PARTY_NOTICES.md   # A2UI provenance and attribution
├── LICENSES/                # Third-party license texts
├── requirements.txt
├── a2ui_sdk/                # Apache-2.0 A2UI schema/validation helpers
├── templates/               # Validated component templates and builders
└── tests/                   # Offline schema and behavior tests
```

The bundled A2UI helpers and v0.8/v0.9 schemas are adapted from the open-source
A2UI project and retain their upstream Apache-2.0 notices. See
[THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md) and
[LICENSES/Apache-2.0.txt](./LICENSES/Apache-2.0.txt) for provenance and license
terms. The A2UI transport uses plain dictionaries rather than a version-specific
`a2a-sdk` model, while preserving the documented A2A part shape.

## What you need to provide

This sample contains no tenancy, workspace, compute, database, bucket, or user
identifiers. Before deploying it, provide the following values for your own OCI
environment:

| Setting | Required | What to enter |
|---|---|---|
| `OCI_COMPARTMENT_ID` | Yes | The OCID of a compartment where your selected Generative AI model can be invoked. A tenancy OCID may be used only when the tenancy root is intentionally your compartment scope. |
| `OCI_REGION` | Yes | An OCI region where the selected Generative AI model is available, such as `us-ashburn-1`. |
| `OCI_GENAI_MODEL_ID` | Yes | A model ID available to your tenancy in that region. The sample default is `google.gemini-2.5-flash`. |
| `OCI_GENAI_ENDPOINT` | Usually no | Override only when you cannot use the endpoint derived from `OCI_REGION`. |

The preferred configuration is to set the first three values as environment
variables on the AI compute. If your AIDP environment does not expose compute
environment variables:

1. Copy `deployment_config.example.py` to `deployment_config.py`.
2. Replace `<your-compartment-ocid>` with your compartment OCID.
3. Upload `deployment_config.py` with the other agent files.
4. Keep it local. The sample `.gitignore` prevents accidental commits.

Do not put credentials, API keys, auth tokens, workspace IDs, compute IDs, or
private service URLs in either configuration file. This gallery does **not**
require a database, schema, Knowledge Base, Object Storage bucket, volume, or
MCP server.

## OCI permissions

The identity used by the agent compute must be allowed to invoke Generative AI
models in the configured compartment. IAM policy design varies by tenancy, so
this sample does not embed a tenancy-specific policy statement. Ask your OCI
administrator to grant the compute identity the minimum permission needed to
use the selected model.

## Deploy

1. Create a code-first agent flow named exactly `A2UIComponentGallery`.
2. Configure your compartment, region, and model as described above.
3. Upload this entire folder while preserving `a2ui_sdk/` and `templates/`.
4. Set `agent.py` as the entry file and `requirements.txt` as dependencies.
5. Attach an AIDP **AI compute** resource; regular OCI Compute is not sufficient.
6. Deploy to TEST first.
7. Send `What can you help me with?` and confirm HTTP 200 plus an AI message.
8. Send `Show me a demo` in an A2UI-capable client and confirm the gallery renders.
9. After TEST verification, deploy the same files to PROD.

The Python class name intentionally matches the required flow display name.

## Try it

- `What can I ask you?`
- `How are you today?`
- `Show me a demo.`
- `Show the A2UI component gallery.`
- `Preview an OAChart.`
- `Show the data grid component.`
- `Show a button component.`
- `What components are available?`

Ordinary conversation returns plain text and does not require A2UI capability
negotiation. Demo, gallery, component, and action requests render through A2UI.
The agent prefers v0.9 and falls back to v0.8 based on the client capabilities.
For those UI requests, a client can explicitly request text fallback with
`enabled: false` (an empty capability object is also treated as text-only):

```json
{
  "a2uiClientCapabilities": {"enabled": false}
}
```

The standard location is `metadata.a2uiClientCapabilities`. Current AIDP chat
gateways may carry arbitrary client metadata through
`metadata.sessionvariables.a2uiClientCapabilities`; the sample accepts both
transports and applies the same negotiation rules.

## Architecture

1. `A2UIComponentGallery.invoke()` routes help, conversation, UI, and action requests.
2. Help and greeting prompts return immediate text; other conversation uses a
   plain-text conversational graph.
3. Only UI and action requests negotiate the newest mutually supported A2UI
   protocol and catalog (v0.9 first, then v0.8).
4. The selector chooses one allowed template name; it never emits raw A2UI JSON.
5. `SurfaceBuilder` copies a trusted template and applies validated controls.
6. `A2uiValidator` validates the complete operations array before the parser
   wraps it in the AIDP A2UI response transport.

The action payload carries surface state, so the sample does not require
conversation memory. It still handles an injected checkpointer safely when the
runtime provides one.

## Troubleshooting

- **Agent not loaded**: confirm the flow is named `A2UIComponentGallery`, the
  folder structure was preserved, and runtime-managed LangChain packages were
  not added to `requirements.txt`.
- **Text appears instead of UI**: verify the client advertises an A2UI v0.8 or
  v0.9 catalog and did not send an empty `a2uiClientCapabilities` object.
- **Catalog negotiation failed**: make sure the client supports the catalog ID
  in `a2ui_sdk/v0_9/complete_catalog.json`.
- **Missing OCI compartment**: set `OCI_COMPARTMENT_ID` or create the private
  `deployment_config.py` override before starting the agent. Setup stops early
  with a configuration error rather than sending the placeholder to OCI.
- **Model call fails**: select a model available in the configured OCI region.
  Template selection falls back to `Text`, but setup still needs a valid model.

## Test locally

Install only this sample's dependencies, then run:

```bash
# run from this sample's directory (ai/agent-hub/agent-samples/a2ui-component-gallery/)
python -m unittest discover -s tests -v
```

The tests validate every component template, supported actions, malformed
actions, fallback selection, and explicit text-only capability handling.

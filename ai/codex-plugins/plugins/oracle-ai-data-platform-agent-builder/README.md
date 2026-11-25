# AIDP Agent Builder

**Experimental sample — 0.1.0-alpha.1.** Build an AIDP custom agent that serves
named business capabilities through a reusable UI, CLI and conversation.

Codex is the builder. It turns English component contracts into a high-code
agent, typed actions, generated Python tools and, when needed, a checkpointed
business process. The browser and CLI are shared clients; they do not execute
generated business Python or require a generated frontend for each agent.

`A2App` is this sample's application convention, not a published Oracle or A2UI
standard. It reuses A2A DataParts and A2UI 0.8 controls/events, adding business
component discovery, contracts, typed actions and exact process/revision rules.

## How this relates to the existing samples

| Existing sample | Reused foundation | What this builder adds |
| --- | --- | --- |
| [Workbench Engineer Agent](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/codex-plugins/plugins/oracle-ai-data-platform-workbench-engineer-agent) | High-code `aidputils` and LangGraph authoring | English business component contracts and a standard application interface |
| [Ask AIDP](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/codex-plugins/plugins/ask-aidp) | Public workspace upload, agent lifecycle and compute operations | Generated agent artifacts and publishing plans for those operations |
| [A2UI Component Gallery](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/agent-hub/agent-samples/a2ui-component-gallery) | Deterministic UI construction, action and catalog concepts | Application-level capability contracts and typed UI-independent commands |
| [Weekly Business Review](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/agent-hub/agent-samples/weekly-business-review) | Governed data tools and business analysis | A builder for different business processes, rather than one authored application |

The existing Python chat client and Agent Hub examples are useful references,
but are not interchangeable transports. The chat client uses Responses-style
chat; the hosted Agent Hub gallery documents a text adapter. This sample's
shared client requires native A2A Parts and its advertised custom catalog.

## Prerequisites and compatibility

- Codex with plugin support; Python 3.10 or later.
- For native calls, an installed OCI CLI and a working user-selected profile.
- For publishing, public AIDP workspace-upload and agent operations, preferably
  through Ask AIDP, or the corresponding installed AIDP CLI/SDK.
- A selected AIDP workspace and agent compute with the native capabilities
  required by the generated agent. `aidputils` is supplied by AIDP compute;
  do not install a guessed local package with that name.

The native profile must be qualified on the target environment: CODE entry
hooks, A2A DataPart forwarding, model/tool signing and, for continuing processes,
a durable checkpointer and explicit interrupt/continuation support. Public
high-code and Agent Hub examples do not establish all of these capabilities.
The client fails on incompatible responses; it does not silently replace Parts
with JSON text or move the business workflow into the local gateway.

Data access is an agent-owned tool selected from the target's actual
capabilities. Use supported SQL/RAG tools, or a verified DI endpoint when the
environment exposes one. This sample does not invent a public DI/DFL discovery
API or start a local data engine. No live tenancy or GenAI verification is
implied by the offline tests.

## Install

Register this repository's marketplace, then install the builder:

```sh
codex plugin marketplace add oracle-samples/oracle-aidp-samples \
  --ref main --sparse .agents --sparse ai/codex-plugins
codex plugin add oracle-ai-data-platform-agent-builder@oracle-aidp-codex
```

Before the contribution is merged, use a local checkout instead:

```sh
codex plugin marketplace add /absolute/path/to/oracle-aidp-samples/ai/codex-plugins
codex plugin add oracle-ai-data-platform-agent-builder@oracle-aidp-codex
```

Restart Codex and start a new chat after installation. Ask AIDP can be installed
separately from the same marketplace for discovery and publishing operations.

## Build an agent

Example prompts, submitted separately:

```text
Use AIDP Agent Builder. Build an agent that shows monthly profitability
and recommends product-strategy changes. Use my selected AIDP workspace
and the data tools available there.
```

```text
Add a review of products whose profit fell sharply. Analyze the eligible
products automatically using our documented decision rules, then show all
recommendations and flag ambiguous cases for optional human review.
```

```text
Let me ask a deeper question about one product and export the recommendation
table as a Markdown report. Build and validate the agent, then show the
deployment plan before publishing to my selected target.
```

The builder should discover business definitions where supported and clarify
material ambiguities. It saves contracts as requirements accumulate. One prompt
can describe multiple components; a later prompt can refine the same component.
No special prompt is needed to request UI/CLI modes, request envelopes or a
particular directory layout. Business actions that change external systems need
explicit contracts and authorization; recommendations alone do not execute them.

The included helper validates explicit source files, packages the matching
CUSTOM tools and prepares the public operations' request bodies:

```sh
python PLUGIN/scripts/build_agent.py validate SOURCE
python PLUGIN/scripts/build_agent.py package SOURCE --output BUILD
python PLUGIN/scripts/build_agent.py plan SOURCE --config TARGET.json --output BUILD
```

`PLUGIN` means the installed plugin directory and `SOURCE` the generated project.
The generated `app.json` describes the explicit file allowlist. See
[publishing](skills/aidp-agent-builder/references/publishing.md) for the source
manifest and operator sequence. `plan` prepares artifacts; it does not claim to
upload, create or deploy a cloud resource. Codex uses the existing public AIDP
operations after checking that plan and the user's authorization.

Current deployed-source bundles have Python/configuration, `contracts/`, the
tool ZIP, README and build metadata at their top level. Saved snapshots use
readable `versions/build-001/` names. Full revision IDs remain metadata. The
CODE process uses its deployed Python snapshot; it does not refetch a mutable
tool ZIP on every invocation. Retained snapshots count toward the platform's
deployment-package size limit.

## Run the shared UI and CLI

After actual deployment, copy its verified A2A endpoint and agent key into a
private client configuration. This is a template; use real selected values:

```json
{
  "endpoint": "https://AGENT_HOST.oraclecloud.com/agentendpoint/AGENT_KEY/a2a",
  "agentKey": "AGENT_KEY",
  "workspaceKey": "WORKSPACE_KEY",
  "runtimeRevision": "REVISION_FROM_BUILD_INFO",
  "definitionFile": "BUILD/workspace/component-definitions.json",
  "stateDirectory": ".local-agent-client",
  "ociProfile": "DEFAULT",
  "region": "YOUR_REGION",
  "title": "My business agent"
}
```

Definition/state paths are relative to the configuration file. The config schema
is [runtime/config.schema.json](runtime/config.schema.json). Use the endpoint
advertised by the deployment, not a guessed hostname or path.

```sh
python PLUGIN/runtime/client.py --config CLIENT.json serve
```

The loopback server prints Preview and Explain URLs. Keep it running, then use
the same private configuration from another terminal:

```sh
python PLUGIN/runtime/client.py --config CLIENT.json describe
python PLUGIN/runtime/client.py --config CLIENT.json invoke COMPONENT ACTION --arguments '{}'
python PLUGIN/runtime/client.py --config CLIENT.json chat 'Explain the saved recommendations.'
```

The gateway signs calls, carries Parts/task/continuation metadata and stores
private transport receipts. Native checkpoints own business state and process
progress. The shared renderer supports its bundled catalog, not arbitrary custom
A2UI schemas. Its progress polling leaves controls usable and preserves focused
input. Explain shows the declared component contracts; it is not proof that the
implementation satisfies every sentence.

## Change and republish

Describe the change in the builder chat. Update the existing contract and stable
component ID, its implementation and requirement mapping, then validate/package
and publish a new revision through public AIDP operations. Keep existing runs
pinned to their prior business rules; do not silently replace a running review's
policy. Verify real initialization, tool execution and state recovery before
reporting a release ready.

## Offline validation

```sh
python -m unittest discover -s PLUGIN/tests -v
python -m unittest discover -s PLUGIN/runtime/tests -v
node --test PLUGIN/runtime/tests/test_ui.mjs
```

These tests use explicit synthetic data/fake transports. They require no Oracle
credentials, cloud writes, customer datasets or GenAI calls. The shared JavaScript web client is included; Node is needed only for UI tests.

## License and privacy

[MIT](LICENSE), with retained [third-party notices](NOTICE.md) for A2UI schemas.
See [PRIVACY.md](PRIVACY.md) for local state and data flows. Do not commit private
target configuration, credentials, generated business data or client state.

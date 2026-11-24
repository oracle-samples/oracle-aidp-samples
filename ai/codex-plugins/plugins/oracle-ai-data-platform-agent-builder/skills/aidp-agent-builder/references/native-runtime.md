# Runtime compatibility and capability preflight

The full native A2App profile is experimental. A CODE agent or A2UI-capable client
does not establish all required features. Separate public sample behavior,
target-specific evidence and proposed features in the readiness record.

## Existing public building blocks

- [Ask AIDP](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/codex-plugins/plugins/ask-aidp)
  offers CLI/signed REST, workspace upload, agent lifecycle, bundles and compute.
  Prefer available tools over duplicate API logic; it is optional here.
- [Engineer Agent](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/codex-plugins/plugins/oracle-ai-data-platform-workbench-engineer-agent)
  documents generic high-code authoring and workbench operations.
- [A2UI Component Gallery](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/agent-hub/agent-samples/a2ui-component-gallery)
  contains v0.8/v0.9 schemas, deterministic builders and actions for the hosted
  Agent Hub renderer. It does not include a browser renderer implementation.
- [Weekly Business Review](https://github.com/oracle-samples/oracle-aidp-samples/tree/main/ai/agent-hub/agent-samples/weekly-business-review)
  demonstrates predefined read-only SQLTools and validated A2UI dashboards.

The gallery's public Agent Hub adapter serializes Parts through an `A2UI\n`
sentinel. That is not proof of native Parts or of the bundled client's custom
catalog being supported by Agent Hub. Retain relevant licenses/provenance when
copying public helpers.

## Choose entrypoint hooks deliberately

Public high-code examples use an agent class with synchronous `setup()` and
asynchronous `invoke()`, with `pre_invoke_setup(**kwargs)` supplying invocation
configuration. The runtime supplies `aidputils`; do not promise a public local
package-index installation.

Some application runtimes expose module hooks, `flow_setup`, typed A2A Parts
and other adapters. Generate that profile only where its hooks are verified.
Do not combine class/module entrypoints assuming the loader discovers either,
or assert one SDK interface exists in every deployment. A local stub is not
native qualification.

## Required capability evidence

Record the selected instance/workspace/compute and runtime version:

| Capability | Minimum proof |
| --- | --- |
| Agent lifecycle | Verified CLI/reference and accessible read operation |
| CODE hooks | Tiny agent loads with declared entrypoint/dependencies |
| A2A | Advertised Agent Card, matching command delivery and metadata |
| Native Parts | DataPart-only input and distinct typed business/UI output |
| Catalog | Actual client advertises and renders the selected catalog |
| Data tool | Bounded real query with source, scope, completeness and timing |
| Model | Real selected-model invocation in that runtime |
| Persistence, if needed | Native saver and state after reconnect |
| Human input, if needed | Python interrupt, matching resume/context and retained state |

Begin with deterministic probes so model latency cannot hide transport problems.
If a required feature is missing, retain the source/plan and report the exact
blocker. Building a business agent does not authorize changing service images,
platform feature flags, IAM or authentication to make a demo pass.

## Data tools and resources

Use the actual target's SQLTool, RAGTool, selected DI endpoint or other supported
service. Ontology/DI bindings are independent capabilities, neither assumed.
Label an explicitly selected compatibility mode when a requested route is absent.

Use typed complete aggregates for calculations; do not extract financial values
from prose or treat preview rows as a whole dataset. Retain arguments/query,
timestamp, source, request ID and completeness. Bound model inputs. Large data
needs an explicit paginated/reference contract outside model messages.

Native packaging snapshots source for deployment. Running processes do not
automatically execute later workspace edits. A tool ZIP is a matching build
artifact, not evidence of a live fetch on each invocation. Keep ZIP contents,
registrations and generated helpers consistent. Resolve resources according
to the target's loader: an entrypoint may be relocated, making a packaged helper
or resource location safer than the entry file's `__file__`. Never substitute
a developer-specific filesystem path or private service hostname.

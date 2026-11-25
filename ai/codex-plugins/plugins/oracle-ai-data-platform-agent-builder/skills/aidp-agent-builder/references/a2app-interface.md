# Experimental A2App interface

A2UI already defines visual controls, bindings and structured user actions.
A2A carries messages/tasks and typed Parts. This sample adds business capability
discovery/contracts, action schemas/effects, UI-independent results and exact
release/process/idempotency rules. A2App is this sample's convention.

Use the bundled protocol/runtime implementation as the exact wire authority.
Do not change its envelope/catalog ad hoc. Shared UI and CLI are standard
clients; new business capabilities change the agent and definitions, rather
than require a generated frontend.

## Components and actions

Each component has a stable machine ID, human label, English contract reference
and declared actions. Actions define arguments, effect and optional parent.
Include deterministic display. Resolve handlers through an explicit generated
registry, never an input-derived import or evaluation.

Copy `runtime/a2app_protocol.py` and `runtime/a2ui_components.py` from this plugin
into the generated `agent/` directory unchanged and list them in `app.json`.
Use their `parse_request`, `resolve`, `public_definition`, `response`,
`error_response`, `native_input` and `response_parts` functions for this profile.
The generated application, not the shared client, owns its handler registry.

The component-definition root contains exactly `protocol: "aidp.a2app"`,
`protocolVersion: 1`, `catalogId` and `components`. Use catalog ID
`https://aidp.local/catalogs/a2app-v2` for the supplied bounded renderer; that
is an opaque catalog identifier, not a service endpoint to contact. A component
contains `id`, `label`, `contract` and `actions`; its contract path is
`contracts/<stable-id>.md`. Actions contain `label`, `handler`,
`effect: read|process|artifact` and `inputSchema`, with optional `parentAction`.
Use closed object input schemas with supported primitive fields. Let the bundled
validator reject unsupported keywords/types rather than claiming full JSON Schema.

Expose model-free discovery/initialization. Discovery lists actual capabilities
without implementation secrets. Initialization returns initial views and current
state; it does not start analysis or execute a business action. Both `@app.describe`
and `@app.initialize` must include `result.definition` from `public_definition`.
The shared UI bootstraps with initialize and needs that definition before it can
render navigation and action controls. Include `process` only when the agent has
a continuing process; independent dashboard reads do not require one. Validate structured
commands before model reasoning. Unknown IDs return errors with possible matches;
a fuzzy suggestion is not permission to execute a corrected command. Unsupported
schema constructs fail validation rather than being silently ignored.

## Three modes

**UI:** use A2UI userAction events with component-scoped action names and resolved
bindings. Additional application metadata supplies release/process/idempotency
guarantees. Return the action's business result and supported presentation
messages. An action can update its affected surface rather than the whole app.

**CLI:** use explicit typed component/action commands with the same validation.
Return structured results and optional report artifacts. Retrieving a result
must not require decoding chart controls.

**Chat:** ordinary text remains natural-language conversation. Explain capabilities
and answer bounded questions. An invalid structured command must not silently
become an operational request. Default chat is read-only; mutations use declared
actions and explicit human authorization. If language is interpreted as an
action, summarize exact proposed arguments/effects before execution.

Channel labels are presentation metadata, not authentication. Bind process/run
identity to the native authenticated context, not an arbitrary client or model ID.

## Parts and catalogs

The experimental native profile uses DataParts with `application/json+a2ui`
for UI events/presentation and `application/json+aidp-a2app` for application
commands/results. Direct natural-language agent chat can use TextParts. The
shared UI/CLI instead sends a typed `type: "chat"` application command in an
application DataPart, preserving its channel/revision/context metadata. These
are two supported entry paths, not JSON text disguised as a DataPart. Use the
bundled normalizer and response adapter instead of emitting JSON-looking prose.

Prove structured input/metadata and native typed output on the selected target.
Advertise only the catalog the actual shared client renders. A matching MIME
type does not establish full A2UI support or hosted Agent Hub compatibility for
custom widgets. Custom display schemas cannot install renderer code.

For this renderer use A2UI0.8 `surfaceUpdate` followed by `beginRendering`, with
the business component ID as `surfaceId`, explicit child references and a
declared root ID. Standard controls are Column, Row, Text, Button, TextField,
CheckBox and MultipleChoice. The bundled display extensions are Metric,
BarChart, Markdown, Table and RuleSection. Prefer `action_nodes` to construct
bound form controls. See [client catalog](client-catalog.md) for exact Table,
chart and other display properties and a complete surface example.
Do not assume an Agent Hub `OAChart` or `OADataGrid` works
in this renderer. Replacing its catalog/visual implementation is a platform
change, not a new business component.

The public Agent Hub examples also expose a separate text adapter using an
`A2UI\n` sentinel. That does not prove native DataPart support. A deliberately
selected compatibility adapter must be labeled and tested; never downgrade
silently after native validation fails.

## State, effects and explanation

Distinguish read actions, process transitions and artifact generation. Mutations
carry idempotency and exact expected versions; case/draft/report edits may have
their own versions. Duplicate semantic requests replay; changed input with the
same key rejects. Read actions do not imply approval or clear mandatory flags.

Clients may retain context/task IDs and derived display caches. Those are not
authoritative business state or a replacement graph checkpoint. Aggregate and
bound data before model use. A large dataset needs an explicit paginated/reference
contract rather than an unbounded model message.

Explain derives from saved contracts, definitions, mapping and graph source.
Distinguish intended behavior from execution proof. Label tool/function/LLM
nodes, START/END and routing. Conditional-edge decisions drawn in a diagram are
not additional runtime nodes. Source values remain text; a catalog does not
authorize arbitrary HTML/JavaScript or remote renderer execution.

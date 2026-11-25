# Business components and contracts

An **agent definition** groups named business components. A **component** is a
capability such as a dashboard, exception review or service case, rather than
an individual visual widget. One component can expose many actions.

The English **component contract** records meaning, scope, rules and acceptance
conditions. The JSON **component definition** records IDs, action input shapes,
effects and supported presentations. Python implements the behavior. Release
them together and keep a requirement-to-function mapping for later explanation
and modification.

## Interpret several prompts together

Decompose where there is a distinct responsibility, presentation or independently
useful behavior. Preserve IDs while the user adds details. Adding a month
selector refines an existing monthly display. A separate entity investigation
is a new capability even if it uses the same measure. Clarify an ambiguous target.

Users need not supply transport, UI/CLI modes, packaging or persistence plumbing
in business prompts. Offer choices where helpful without forcing a fixed
sequence. Templates can suggest questions; they are not evidence that a field,
join or business rule exists in the selected data.

## Contract contents

Use stable requirement IDs and cover what materially applies:

- Purpose, behavior and completion/deliverable.
- Verified sources, grain, joins, definitions, calculations and scope.
- Inputs, actions, outputs and visual/catalog requirements.
- Rules, missing evidence, policy authority and allowed effects.
- Empty/error behavior and meaningful acceptance examples.
- For processes: state, progression, human choices and recovery.

Test business boundaries rather than generated wording. Link consequential
calculations, filters and workflow transitions to requirement IDs in Python.
Comments explain business choices; they need not restate every code line.

## Questions, recommendations and decisions

Keep these capabilities distinct:

- Investigation obtains available evidence and explains it.
- A temporary question digs deeper without changing contracts or saved decisions.
- A recommendation proposes a specific action under identified rules.
- A human disposition records an explicit decision about that proposal.
- Operational execution performs an authorized external-system action.

A dashboard can support deeper questions without acquiring an approval queue.
A repeated-item review owns progress, original proposals, human edits and its
report. Navigating a chart is usually a read, not a business transition.

A recommendation states action, reason, evidence, exact rule text/version and
unresolved prerequisites. Do not infer approved authority from historical data.
Missing inventory, obligations, consent or policy may prevent a plausible action.
Label demo policy and synthetic records; they cannot silently establish real
operational prerequisites.

Define any numeric support score and its derivation. Rule support differs from
predicted business success. Mandatory investigation flags survive table acceptance.
Viewing a row, asking or exporting does not mark it human-reviewed. Preserve
original AI proposals and evidence after edits.

## Action hierarchy and updates

Use optional parent/child metadata for drill-down navigation. An entity view
may have a time selector with Location and Promotion as sibling actions. Each
child carries complete entity/time inputs so a CLI can jump directly to it.
Validate references and reject cycles. A hierarchy is not an authorization rule
or mandatory sequence; those belong in workflow/state validation.

For a modification, update the existing contract, definitions, affected Python,
tests and requirement mapping. Validate shared-client compatibility before
publishing. A changed policy/calculation must not silently reinterpret completed
recommendations; preserve older runs and create a new revision/run when their
business meaning changes.

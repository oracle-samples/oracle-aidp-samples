# Continuing business workflows

Use a continuing graph for durable progression across steps, human interactions
or repeated items. Independent dashboard/read actions do not need that machinery.
Prefer fixed Python transitions for known rules and LLM nodes where reasoning
changes the outcome.

## State and persistence

Structured state describes the work: step, complete worklist, current item,
evidence, proposals, errors, human decisions and versioned receipts. A native
checkpointer persists it at graph boundaries. LLM chat history cannot establish
which items are finished or what a person approved.

Use the target's verified saver scoped to trusted application/revision and native
thread/context. Do not substitute a constant thread ID, arbitrary client namespace
or in-memory fallback for claimed durable state. If the runtime lacks persistence,
report that gap before presenting the workflow as deployed.

## Interrupts and loops

A Python node can issue a LangGraph interrupt directly; a model call is not
needed merely to request human input. Its payload may include status, evidence,
recommendation, versions and required fields. Resume through the verified native
adapter. A request can finish while the graph remains paused in its checkpoint;
a later request resumes it in the same trusted context.

Distinguish business-process state from one A2A task. A platform input-required
task and an explicit checkpoint-command continuation have different wire contracts.
Do not claim either works on a target without an actual probe.

Define the order required by the business. A service case may loop through
diagnosis/workarounds before return/exchange options. An exception review may
repeat evidence, reasoning and validation for each item. Both can branch and
pause without an LLM controlling every edge.

## Automatic collection review

For automation followed by optional human review:

1. Initialize the complete eligible worklist without starting analysis.
2. Start a native graph loop over pending items.
3. Obtain bounded complete evidence and run reasoning for each item.
4. Validate/persist a recommendation or explicit error before advancing.
5. Produce a summary and complete table after processing the list.
6. Offer optional questions, edits, acceptance and report export afterward.

Low support/ambiguity flags a row rather than demanding a click for each item,
unless the user specified that pause. Keep every eligible item and use a bounded
graph step limit adequate for the full list. Retry only failed items. A reconnect
reads checkpoint/task state rather than silently restarting the collection.

Publish any score's meaning. Rule support is not empirical business-success
probability or external-action authority. Missing mandatory evidence remains
visible after accepting a table as-is.

## Commands and deliverables

Validate native context, expected state/case/draft versions and duplicate command
IDs before transitions. Keep original AI proposals/provenance after edits.
Temporary questions do not change contracts or recommendations. Viewing rows
or accepting a table does not imply individual human review.

Generate Markdown deterministically from an exact saved table revision. Include
scope/rules, every item including errors/pending items, specific recommendations,
reasons, flags and human statuses. Mark partial work as partial and escape source
text. Notifications and external business execution require separate explicit
authorization and real configured tools.

Prove actual pause/resume, state after reconnect, duplicate/stale rejection,
partial progress and completion. A local graph test does not qualify the AIDP
runtime adapter.

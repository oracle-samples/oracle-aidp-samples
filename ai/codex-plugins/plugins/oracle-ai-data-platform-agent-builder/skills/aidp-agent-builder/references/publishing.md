# Package and publish

Use the bundled `scripts/build_agent.py` without another source checkout:

```text
python <plugin>/scripts/build_agent.py validate SOURCE
python <plugin>/scripts/build_agent.py package SOURCE --output DEST
python <plugin>/scripts/build_agent.py plan SOURCE --config CONFIG --output DEST
```

Consult its `--help` for current configuration/options. Keep editable source and
tests in the user's selected directory, not the installed plugin cache.

## Explicit source manifest

`app.json` is the source manifest. Its fields are:

```json
{
  "schemaVersion": 1,
  "id": "service-review-agent",
  "title": "Service Review Agent",
  "entrypoint": "agent/main.py",
  "requirements": "agent/requirements.txt",
  "definition": "agent/component-definitions.json",
  "contracts": ["contracts/service-review.md"],
  "files": [
    "agent/main.py",
    "agent/requirements.txt",
    "agent/component-definitions.json",
    "contracts/service-review.md"
  ]
}
```

This illustrates the manifest shape, not a business implementation starter.
List every generated helper, catalog, configuration, mapping and contract that
belongs in the package, including entrypoint/requirements/definition/contracts.
The helper uses this explicit relative-file allowlist rather than walking a
developer's private working directory. Keep credentials, completed target config,
private execution receipts and unbounded datasets outside it.

Validation checks artifact contracts, not business truth or runtime availability.
Packaging builds source/tools from the same listed files. Planning prepares
reviewable upload/create/deploy inputs; it sends no requests. The publisher
flattens the `agent/` prefix at the workspace agent home while retaining helper
subdirectories; `contracts/` stays readable beside the current code.

## Public operations

When installed, prefer Ask AIDP's `aidp_upload_workspace_code`,
`aidp_create_agent` and `aidp_deploy_agent`. Otherwise inspect installed public
`aidp-cli` help and SDK/reference for the target. Use argument arrays/body files.
Do not invent commands, deployment enums, endpoint paths or auth flags from an
old demo. API/service versions require explicit verification.

Reuse the existing authorized OCI profile/mode. Session commands use
`oci session refresh --profile NAME` and
`oci session authenticate --profile-name NAME`. Browser login may need a human;
report that separately from code failure. Never put operator credentials into
generated tools or browser data.

Honor existing authorization to build/deploy the identified agent. Prepare a
concrete plan; ask only when an operation expands scope or lacks permission.
A publish plan does not authorize unrelated agents, source writes, IAM changes
or platform-service patches.

## Current source, history and readiness

Keep current entrypoint, helpers, JSON definitions, requirements, contracts and
tool artifact in one readable home. If retaining saved builds, give directories
readable names with full source/runtime revision IDs in metadata. Keep current
and archived package references coherent. Never copy old history recursively
inside a new snapshot. Do not reset an existing process for file organization.

Check the real deployment and Agent Card/endpoint, not an old Active field.
Verify model-free initialization where designed, real bounded data, supported
typed UI/CLI actions and any promised native continuation. Keep failures/warnings
visible even when partial output exists.

Start the bundled shared runtime using its documented entrypoint. It hosts
transport/discovery/rendering for compatible agents; generated business Python
runs in AIDP. Return actual Preview/Explain URLs after the host starts. Separate
offline validation, a prepared plan, a native deployment and an exercised process.

Its concrete commands are:

```text
python <plugin>/runtime/client.py --config CLIENT_CONFIG serve
python <plugin>/runtime/client.py --config CLIENT_CONFIG describe
python <plugin>/runtime/client.py --config CLIENT_CONFIG invoke COMPONENT ACTION --arguments JSON
python <plugin>/runtime/client.py --config CLIENT_CONFIG chat TEXT
```

`serve` selects a free loopback port by default and prints the actual Preview
and Explain URLs. Keep it running for UI/CLI use. Add `--wait` to an invocation
to follow its declared status action; `--output FILE` saves an action-provided
Markdown report without overwriting an existing file. Do not assume that every
component exposes a start, status, edit or report action: discover its catalog.

For component changes, revise contract/definition, Python and mapping together,
validate, then republish. The shared client build stays unchanged when the new
agent remains within its supported catalog/action capabilities.

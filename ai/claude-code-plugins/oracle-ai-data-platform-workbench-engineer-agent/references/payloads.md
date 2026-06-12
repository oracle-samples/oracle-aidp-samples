# `.aidp/payloads/` — persist every mutation body (auditable + re-runnable)

Before running any **mutating** AIDP operation (create/update/delete/run, deploy, share, grant, …), write
the request body to a JSON file under **`.aidp/payloads/`**, then point the CLI / `oci raw-request` at that
file. This mirrors the official Oracle AI-Skill demo (which persists `create-…-job.json`,
`start-…-job-run.json`, etc. before invoking the CLI) and gives you an auditable, replayable trail.

Why: (1) the user can review the exact body before you run it (pairs with the confirm-before-mutate gate);
(2) failed/half-applied ops can be re-run from the saved file; (3) it documents what the agent did.

## Convention
- One file per mutation, named `<verb>-<resource>[-<name>].json` — e.g.
  `create-weatherdemo-cluster.json`, `create-weather-summary-job.json`, `start-weather-summary-job-run.json`,
  `create-share-supplier-spend.json`, `add-member-analyst.json`.
- `.aidp/` is git-ignored (per-project, may contain OCIDs) — never commit it.
- Show the file's contents to the user and **confirm** before running (especially deploy/purge/delete/grant).

## With the official `aidp` CLI (preferred)
Most `aidp` write commands take `--body <JSON>` (or `--body-file`/stdin). Persist, then pass it:
```bash
mkdir -p .aidp/payloads
cat > .aidp/payloads/create-weather-summary-job.json <<'JSON'
{ "displayName": "WeatherSummary_Job", "tasks": [ { "taskKey": "weather_summary_notebook",
  "type": "NOTEBOOK_TASK", "notebookPath": "/Workspace/WeatherDemo/WeatherSummary.ipynb",
  "dependsOn": [] } ], "clusterKey": "<CLUSTER_KEY>" }
JSON
aidp workflow create-job --instance-id <OCID> --auth api_key --profile DEFAULT \
  --body "$(cat .aidp/payloads/create-weather-summary-job.json)"
```

## With `oci raw-request` (fallback)
```bash
oci raw-request --http-method POST \
  --target-uri "https://aidp.<region>.oci.oraclecloud.com/20240831/dataLakes/<OCID>/workspaces/<WS>/jobs" \
  --request-body "file://.aidp/payloads/create-weather-summary-job.json" \
  --request-headers '{"content-type":"application/json"}' --profile DEFAULT
```

## Async (202) results
Save the returned operation/run key too (e.g. append to the payload file or a sibling
`<name>.run.json`), then poll to a terminal state (see `aidp-pipelines` / `aidp-observability`).

Applies to every mutating skill: `aidp-pipelines`, `aidp-cluster-ops`, `aidp-workspace-admin`,
`aidp-ingest-file-to-table`, `aidp-notebooks`/`aidp-workspace-files`, `aidp-data-sharing`,
`aidp-roles-access`, `aidp-credentials`, `aidp-agent-flows`, `aidp-bundle`, `aidp-mlops`, `aidp-audit`,
`aidp-user-settings`.

---
name: aidp-mlops
description: Track ML work in AIDP's MLflow-compatible MLOps — experiments, runs, metrics/params, registered models, and model versions. Use when the user wants to log/track experiments, list runs or metrics, register a model, transition a model-version stage, or query the model registry. Primary engine is the official `aidp` CLI (`aidp mlops …`); the same Preview MLflow REST API via `oci raw-request` is the no-CLI fallback. Verify live first.
---

# `aidp-mlops` — MLflow experiments & model registry (Preview)

Interact with AIDP's MLflow-compatible MLOps surface. This is AIDP-native MLOps — distinct from OCI Data
Science.

**CLI (preferred):** `aidp mlops <command> --instance-id <DATALAKE_OCID> --auth api_key --profile DEFAULT --region <r>`
- Experiments/runs: `aidp mlops list-experiments | create-experiment | list-experiment-runs | log-experiment-run-metric`
- Registry: `aidp mlops list-registered-models | create-registered-model | create-model-version | transition-model-version-stage`
- **Full surface = 46 subcommands** — see the grouped command index below (experiments / runs / registered-models / model-versions / tags / artifacts).

**Fallback (no CLI):** same MLflow REST surface via `oci raw-request` (identical endpoint + auth; see
`references/oci-raw-request.md`).

> **Preview + verify-first (no-fabrication):** MLOps is **Preview**. Probed 2026-06-10:
> `GET …/workspaces/<ws>/mlops/api/2.0/mlflow/experiments/search` → **404** — either MLOps isn't provisioned
> in this tenancy, or (likely) the MLflow endpoints are **POST-shaped** (`experiments/search` is a POST in
> MLflow). Confirm the working path/verb with a live `aidp mlops list-experiments` (CLI) before relying on
> the REST surface; record in `references/rest-endpoint-map.md`. Don't assert unverified endpoints.

## When to use
- "Track/log an experiment or run", "list runs/metrics", "register a model", "promote a model version",
  "query the model registry".

## Workflow
1. **Verify** with `aidp mlops list-experiments` (CLI) — or `experiments/search` (REST fallback; auth ladder).
2. Standard MLflow patterns: create/find an experiment → create a run → log params/metrics → register the
   model → create/transition a model version. Bodies follow the MLflow REST schema.
3. For training itself, run code in a notebook (`aidp-notebooks`) and log to MLOps from there.

**Mutating ops** (`create-experiment`, run logging, `create-registered-model`, `create-model-version`,
`transition-model-version-stage`): persist the body to `.aidp/payloads/` and confirm first
([references/payloads.md]($HOME/.aidp/references/payloads.md)).

### Full `aidp mlops` command index (46 subcommands)
All `(Preview)`. Positional args differ by group: **experiment/run/artifact** commands take `<DLID> <WS>`
(workspace-scoped); **registered-model/model-version** commands take **`<DLID>` only** (no `<WS>`). `--body
<JSON>` rows are mutating *or* POST-search reads — persist the body to `.aidp/payloads/` and confirm first
([references/payloads.md]($HOME/.aidp/references/payloads.md)). Field names below are confirmed from
`docs/cli/README.md` mlops section (lines 2455–4049). Source: CLI README §ML Ops.

**Experiments** (`<DLID> <WS>`)
| Command | Mutates? | Body / options (required *) |
|---|---|---|
| `list-experiments` | read (POST) | body `ListExperimentsDetails`: `filter` · `max_results` · `order_by[]` · `page_token` · `view_type` (`ACTIVE_ONLY`\|`DELETED_ONLY`\|`ALL`) |
| `get-experiment-by-id` | read | opt `--experiment-id*` (works on deleted experiments) |
| `get-experiment-by-name` | read | opt `--experiment-name*` (prefers active on name clash) |
| `create-experiment` | yes | body `CreateExperimentDetails`: `name*` · `artifact_location` · `tags[]` (each `ExperimentTag {key*,value}`) |
| `update-experiment` | yes | body `UpdateExperimentDetails`: `experiment_id*` · `new_name` |
| `delete-experiment` | yes | body `DeleteExperimentDetails`: `experiment_id*` |
| `restore-experiment` | yes | body `RestoreExperimentDetails`: `experiment_id*` |

**Experiment runs** (`<DLID> <WS>`)
| Command | Mutates? | Body / options (required *) |
|---|---|---|
| `list-experiment-runs` | read (POST) | body `ListExperimentRunsDetails`: `experiment_ids[]` · `filter` · `max_results` · `order_by[]` · `page_token` · `run_view_type` (`ACTIVE_ONLY`\|`DELETED_ONLY`\|`ALL`) |
| `get-experiment-run-by-id` | read | opt `--run-id*` |
| `get-experiment-run-metric-history` | read | opt `--run-id* --metric-key*` · `--page-token --max-results` |
| `list-artifacts` | read | opt `--run-id*` · `--path --page-token` |
| `list-logged-models` | read (POST) | body `ListLoggedModelsDetails`: `experiment_ids[]` · `filter` · `max_results` · `order_by[]` (each `LoggedModelOrder {field_name*,ascending}`) · `page_token` |
| `create-experiment-run` | yes | body `CreateExperimentRunDetails`: `experiment_id` · `run_name` · `start_time` · `tags[]` (`ExperimentRunTag {key,value}`) |
| `update-experiment-run` | yes | body `UpdateExperimentRunDetails`: `run_id*` · `end_time` · `run_name` · `status` (`RUNNING`\|`SCHEDULED`\|`FINISHED`\|`FAILED`\|`KILLED`\|`INTERNAL_ERROR`) |
| `delete-experiment-run` | yes | body `DeleteExperimentRunDetails`: `run_id*` |
| `restore-experiment-run` | yes | body `RestoreExperimentRunDetails`: `run_id*` |
| `log-experiment-run-metric` | yes | body `LogExperimentRunMetricDetails`: `key* run_id* timestamp* value*` · `step` |
| `log-experiment-run-param` | yes | body `LogExperimentRunParamDetails`: `key* run_id* value*` |
| `log-experiment-run-model` | yes | body `LogExperimentRunModelDetails`: `model_json* run_id*` |
| `log-experiment-run-inputs` | yes | body `LogExperimentRunInputsDetails`: `run_id*` · `dataset_inputs[]` (`DatasetInput {dataset*,tags[]}`; `Dataset {digest*,name*,source*,source_type*,profile,schema}`; `InputTag {key*,value*}`) |
| `log-experiment-run-batch` | yes | body `LogExperimentRunBatchDetails`: `run_id*` · `metrics[]` (`ExperimentRunMetric {key,value,timestamp,step}`) · `params[]` (`ExperimentRunParam {key,value}`) · `tags[]` (`ExperimentRunTag {key,value}`) |

**Experiment / run tags** (`<DLID> <WS>`)
| Command | Mutates? | Body (required *) |
|---|---|---|
| `set-experiment-tag` | yes | `SetExperimentTagDetails`: `experiment_id* key* value*` |
| `delete-experiment-tag` | yes | `DeleteExperimentTagDetails`: `experiment_id* key*` |
| `update-experiment-tags` | yes | `UpdateExperimentTagsDetails`: `experiment_id*` · `set_tags[]` (`ExperimentTag {key*,value}`) · `delete_tags[]` (`ExperimentTagKey {key*}`) |
| `set-experiment-run-tag` | yes | `SetExperimentRunTagDetails`: `run_id* key* value*` |
| `delete-experiment-run-tag` | yes | `DeleteExperimentRunTagDetails`: `run_id* key*` |
| `update-experiment-run-tags` | yes | `UpdateExperimentRunTagsDetails`: `run_id*` · `set_tags[]` (`ExperimentRunTag {key,value}`) · `delete_tags[]` (`ExperimentRunTagKey {key*}`) |

**Registered models** (`<DLID>` only — no `<WS>`)
| Command | Mutates? | Body / options (required *) |
|---|---|---|
| `list-registered-models` | read | opt `--filter --max-results` (default 100, max 1000) `--page-token --order-by` |
| `get-registered-model` | read | opt `--name*` |
| `create-registered-model` | yes | body `CreateRegisteredModelDetails`: `name*` · `description` · `deployment_job_id` · `tags[]` (`RegisteredModelTag {key,value}`) |
| `update-registered-model` | yes | body `UpdateRegisteredModelDetails`: `name*` · `description` · `deployment_job_id` |
| `rename-registered-model` | yes | body `RenameRegisteredModelDetails`: `name*` · `new_name` |
| `delete-registered-model` | yes | body `DeleteRegisteredModelDetails`: `name*` |
| `set-registered-model-tag` | yes | body `SetRegisteredModelTagDetails`: `name* key* value*` |
| `delete-registered-model-tag` | yes | body `DeleteRegisteredModelTagDetails`: `name* key*` |
| `update-registered-model-tags` | yes | body `UpdateRegisteredModelTagsDetails`: `name*` · `set_tags[]` (`RegisteredModelTag {key,value}`) · `delete_tags[]` (`RegisteredModelTagKey {key*}`) |

**Model versions** (`<DLID>` only — except `create-workspace-model-version` which is `<DLID> <WS>`)
| Command | Mutates? | Body / options (required *) |
|---|---|---|
| `list-model-versions` | read | opt `--filter --max-results --page-token --order-by` |
| `get-model-version` | read | opt `--name* --version*` |
| `create-model-version` | yes | body `CreateModelVersionDetails`: `name* source*` · `description` · `model_id` · `run_id` · `run_link` · `tags[]` (`ModelVersionTag {key,value}`) |
| `create-workspace-model-version` | yes | `<DLID> <WS>` · body `CreateModelVersionDetails` (same fields as above) |
| `update-model-version` | yes | body `UpdateModelVersionDetails`: `name* version*` · `description` |
| `delete-model-version` | yes | body `DeleteModelVersionDetails`: `name* version*` |
| `transition-model-version-stage` | yes (impactful) | body `TransitionModelVersionStageDetails`: `name* version* stage* archive_existing_versions*` — `stage` is a free string in the SDK (no enum constants); confirm valid stage names live before relying on them |
| `set-model-version-tag` | yes | body `SetModelVersionTagDetails`: `name* version* key* value*` |
| `delete-model-version-tag` | yes | body `DeleteModelVersionTagDetails`: `name* version* key*` |
| `update-model-version-tags` | yes | body `UpdateModelVersionTagsDetails`: `name* version*` · `set_tags[]` (`ModelVersionTag {key,value}`) · `delete_tags[]` (`ModelVersionTagKey {key*}`) |

> Timestamps (`start_time`/`end_time`/metric `timestamp`) are **Unix epoch milliseconds** (per SDK field docs).
> The 13 tag commands are: `set-`/`delete-`/`update-` × {`experiment`, `experiment-run`, `model-version`,
> `registered-model`} tag(s) — note `update-*-tags` (plural) takes batched `set_tags`/`delete_tags` arrays.

### Fallback (no CLI) — REST endpoints (workspace-scoped MLflow; Preview, try `20260430` → `20240831`)
Base: `…/workspaces/{ws}/mlops/api/2.0/mlflow/…`
- Experiments: `experiments/create|search|get`
- Runs: `runs/create|update|search|get` · `runs/log-metric|log-parameter|log-batch`
- Registered models: `registered-models/create|get|search`
- Model versions: `model-versions/create|get|search|transition-stage`

## Guardrails
- Stage transitions (e.g. to Production) are impactful — confirm.
- Preview surface may shift — cite live status; don't assert unverified endpoints.

## References
- [references/aidp-cli-map.md]($HOME/.aidp/references/aidp-cli-map.md) (skill → CLI map) ·
  [references/payloads.md]($HOME/.aidp/references/payloads.md) (persist + confirm bodies) ·
  [references/oci-raw-request.md]($HOME/.aidp/references/oci-raw-request.md) (base URL + auth ladder) ·
  [references/no-mcp-rest-map.md]($HOME/.aidp/references/no-mcp-rest-map.md) (control-plane runs MCP-free) ·
  [references/rest-endpoint-map.md]($HOME/.aidp/references/rest-endpoint-map.md) (record verified MLflow paths here) ·
  pairs with `aidp-notebooks` for the training run itself

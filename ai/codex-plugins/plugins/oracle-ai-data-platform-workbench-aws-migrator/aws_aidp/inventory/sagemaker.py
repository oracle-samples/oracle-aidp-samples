"""SageMaker inventory — notebook instances, training jobs, models, pipelines."""
from __future__ import annotations

from typing import Any

from aws_aidp.aws_client import AwsClient


def _notebook_instances(sm) -> list[dict]:
    out = []
    for page in sm.get_paginator("list_notebook_instances").paginate():
        for n in page.get("NotebookInstances", []):
            out.append({
                "name": n.get("NotebookInstanceName"),
                "status": n.get("NotebookInstanceStatus"),
                "instance_type": n.get("InstanceType"),
                "url": n.get("Url"),
            })
    return out


def _training_jobs(sm) -> list[dict]:
    out = []
    for page in sm.get_paginator("list_training_jobs").paginate(
        SortBy="CreationTime", SortOrder="Descending",
    ):
        for j in page.get("TrainingJobSummaries", []):
            out.append({
                "name": j.get("TrainingJobName"),
                "status": j.get("TrainingJobStatus"),
                "created_at": str(j.get("CreationTime", "")),
                "ended_at": str(j.get("TrainingEndTime", "")),
            })
    return out


def _models(sm) -> list[dict]:
    out = []
    for page in sm.get_paginator("list_models").paginate():
        for m in page.get("Models", []):
            out.append({"name": m.get("ModelName"), "created_at": str(m.get("CreationTime", ""))})
    return out


def _pipelines(sm) -> list[dict]:
    out = []
    for page in sm.get_paginator("list_pipelines").paginate():
        for p in page.get("PipelineSummaries", []):
            out.append({
                "name": p.get("PipelineName"),
                "display_name": p.get("PipelineDisplayName"),
                "status": p.get("PipelineStatus"),
                "created_at": str(p.get("CreationTime", "")),
            })
    return out


def scan(client: AwsClient) -> dict[str, Any]:
    sm = client.client("sagemaker")
    notebooks: list[dict] = []
    training: list[dict] = []
    models: list[dict] = []
    pipelines: list[dict] = []
    warnings: list[dict[str, str]] = []
    try:
        notebooks = _notebook_instances(sm)
    except Exception as e:
        warnings.append({"scope": "notebook_instances", "resource": "*", "error": str(e)})
    try:
        training = _training_jobs(sm)
    except Exception as e:
        warnings.append({"scope": "training_jobs", "resource": "*", "error": str(e)})
    try:
        models = _models(sm)
    except Exception as e:
        warnings.append({"scope": "models", "resource": "*", "error": str(e)})
    try:
        pipelines = _pipelines(sm)
    except Exception as e:
        warnings.append({"scope": "pipelines", "resource": "*", "error": str(e)})
    return {
        "summary": {
            "notebook_count": len(notebooks),
            "training_job_count": len(training),
            "model_count": len(models),
            "pipeline_count": len(pipelines),
        },
        "items": {
            "notebooks": notebooks,
            "training_jobs": training,
            "models": models,
            "pipelines": pipelines,
        },
        "warnings": warnings,
    }

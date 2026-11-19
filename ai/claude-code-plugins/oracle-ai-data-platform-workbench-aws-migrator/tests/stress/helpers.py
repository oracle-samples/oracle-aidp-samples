from __future__ import annotations

import json
import os
import subprocess
import sys
from copy import deepcopy
from importlib.resources import files
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_PATH = files("aws_aidp.fixtures").joinpath("demo-manifest.json")


def load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def run_cli(*args: str, cwd: Path | None = None, env: dict[str, str] | None = None):
    merged_env = os.environ.copy()
    merged_env["PYTHONPATH"] = str(REPO_ROOT)
    if env:
        merged_env.update(env)
    return subprocess.run(
        [sys.executable, "-m", "aws_aidp.cli", *args],
        cwd=cwd or REPO_ROOT,
        env=merged_env,
        capture_output=True,
        text=True,
        timeout=30,
    )


def athena_asset(
    *,
    asset_id: str = "athena.query.q1",
    name: str = "query_one",
    query: str = "SELECT 1",
) -> dict:
    return {
        "id": asset_id,
        "source": {
            "type": "athena_named_query",
            "id": asset_id.rsplit(".", 1)[-1],
            "name": name,
            "workgroup": "primary",
            "database": "default",
            "query": query,
        },
        "target": {"type": "aidp_saved_query", "name": name},
        "transform_chain": ["athena_to_spark_sql"],
    }


def glue_asset(
    *,
    asset_id: str = "glue.job.job1",
    name: str = "job_one",
    script: str = "print('ok')",
) -> dict:
    return {
        "id": asset_id,
        "source": {
            "type": "glue_job",
            "name": name,
            "script_location": f"s3://scripts/{name}.py",
            "script": script,
        },
        "target": {"type": "aidp_job", "name": name},
        "transform_chain": ["glue_to_spark"],
    }


def plan_with(*assets: dict, namespace: str = "testns") -> dict:
    return {
        "plan_id": "stress-plan",
        "target_aidp": {"namespace": namespace},
        "assets": [deepcopy(a) for a in assets],
    }

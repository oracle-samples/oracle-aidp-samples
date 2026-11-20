"""Placeholder notebook builder.

When a notebook can't be exported (or fails fingerprint validation after
upload), we still create a stand-in .ipynb at the target path so the AIDP
workflow can be built with valid notebookPath references. The stand-in:

  1. Has a markdown cell explaining the failure + remediation
  2. Has a code cell that raises RuntimeError on execution (so a workflow
     run never silently no-ops)
  3. Carries metadata.ai_dbx_migration.placeholder=true so re-runs can
     auto-replace it when the real export succeeds (without --overwrite)
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

SENTINEL_KEY = "ai_dbx_migration"


def is_placeholder(notebook_content: Optional[Dict[str, Any]]) -> bool:
    """Detect our sentinel marker on a notebook fetched from AIDP."""
    if not isinstance(notebook_content, dict):
        return False
    md = notebook_content.get("metadata") or {}
    mig = md.get(SENTINEL_KEY)
    return bool(mig and mig.get("placeholder") is True)


def new_run_id() -> str:
    return f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}-{uuid.uuid4().hex[:8]}"


def build_placeholder(
    dbx_path: str,
    aidp_path: str,
    dbx_job_id: str,
    run_id: str,
    failure_reason: str,
    failure_detail: str = "",
    remediation: str = "",
) -> Dict[str, Any]:
    """Construct the placeholder .ipynb content dict (ready for PUT)."""
    md_lines = [
        "# ⚠️  EXPORT PLACEHOLDER — NOT YET EXPORTED  ⚠️\n",
        "\n",
        "This notebook is a placeholder. The original notebook failed to export\n",
        "from Databricks.\n",
        "\n",
        f"**Original DBX path:** `{dbx_path}`  \n",
        f"**AIDP target path:** `{aidp_path}`  \n",
        f"**DBX job id:** `{dbx_job_id}`  \n",
        f"**Export run id:** `{run_id}`  \n",
        f"**Failure reason:** `{failure_reason}`  \n",
    ]
    if failure_detail:
        md_lines.append(f"**Detail:** {failure_detail}\n\n")
    md_lines.append("\n")
    md_lines.append("**To unblock:**\n")
    if remediation:
        for line in remediation.split("\n"):
            md_lines.append(f"- {line}\n")
    else:
        md_lines.append("1. Fix the export issue on the Databricks side (split the notebook, re-export from the DBX UI, etc.)\n")
        md_lines.append("2. Re-run the exporter. Placeholders are auto-replaced when the real export succeeds (no `--overwrite` needed).\n")

    raise_msg = (
        f"EXPORT PLACEHOLDER: This notebook was not actually exported from "
        f"Databricks. Original DBX path: {dbx_path}. Reason: {failure_reason}. "
        f"See cell 0 for remediation."
    )
    code_src = f'raise RuntimeError({raise_msg!r})\n'

    cells = [
        {
            "cell_type": "markdown",
            "id": "placeholder-md",
            "metadata": {},
            "source": md_lines,
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "id": "placeholder-raise",
            "metadata": {},
            "outputs": [],
            "source": [code_src],
        },
    ]

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"name": "python3", "display_name": "Python 3"},
            "language_info": {"name": "python"},
            SENTINEL_KEY: {
                "placeholder": True,
                "run_id": run_id,
                "dbx_source": dbx_path,
                "dbx_job_id": str(dbx_job_id),
                "failure_reason": failure_reason,
                "failure_detail": failure_detail[:1000] if failure_detail else "",
                "created_at": datetime.utcnow().isoformat() + "Z",
            },
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }

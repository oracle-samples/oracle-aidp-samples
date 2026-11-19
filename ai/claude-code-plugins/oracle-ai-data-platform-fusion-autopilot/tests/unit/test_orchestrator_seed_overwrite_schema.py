"""Regression — seed-mode bronze write must use overwriteSchema=true.

Per CLAUDE.md medallion invariant: ``CREATE OR REPLACE TABLE is for seed mode only``.
The bronze adapter's seed-mode write must include ``.option("overwriteSchema", "true")``
so a re-run can land on a target with a divergent prior schema (e.g. stale audit
columns from a half-completed run).

Caught live by TC26 probe ``run_id=023482f5-a613-4c62-8bed-b0b238c944f4``: Delta
threw ``[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields "_watermark_used"
and "_watermark_used"`` on the second seed-mode call against a table created by
an earlier extractors.bicc-attribute-error run.

Phase 9 follow-up: the v1 bronze write at ``orchestrator/__init__.py:289`` was
deleted along with ``_execute_node``. The invariant lives in the content-pack
bronze adapter (``orchestrator/builtins/bronze_extract_adapter.py``).
"""
from __future__ import annotations

from pathlib import Path

from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
    bronze_extract_adapter,
)


def test_seed_write_includes_overwrite_schema_option() -> None:
    """Static-source check — the bronze adapter's seed-mode write line must
    include overwriteSchema. Static check (not a runtime mock test) because the
    bug is about the call shape, not the call result — runtime mocks can hide it.
    """
    src = Path(bronze_extract_adapter.__file__).read_text()
    assert ".mode(\"overwrite\")" in src, "bronze write should still use mode('overwrite')"
    assert "\"overwriteSchema\", \"true\"" in src, (
        "seed-mode bronze write must include overwriteSchema=true; see CLAUDE.md "
        "medallion invariant + TC26 probe run_id=023482f5"
    )
    # The two must appear on the same logical write chain. Conservative check:
    # the overwriteSchema option must be within ~200 chars of the mode call.
    mode_idx = src.index(".mode(\"overwrite\")")
    schema_idx = src.index("\"overwriteSchema\", \"true\"")
    assert 0 < schema_idx - mode_idx < 300, (
        f"overwriteSchema option must be chained to the seed-mode write "
        f"(distance={schema_idx - mode_idx} chars)"
    )

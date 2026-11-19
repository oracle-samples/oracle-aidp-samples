"""CLI dry-run renderer — shows the content-pack plan with dispatch info.

User-facing surface: ``aidp-fusion-autopilot run --dry-run`` prints a list
of nodes that would dispatch + their implementation type so operators
can confirm the plan before flipping ``--dry-run`` off.

The renderer (``commands/run.py::_render_summary`` and friends) reads
``RunSummary.plan`` (a tuple of ``PlanNode`` instances) populated by
the content-pack runner.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock, patch

from oracle_ai_data_platform_fusion_autopilot.orchestrator import (
    _build_content_pack_dry_run_plan,
    _run_content_pack_backend,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack


PACK_YAML = """
id: phase5-dry-run-renderer-test
version: 1.0.0
description: Phase 5 dry-run renderer test pack
compatibility:
  pluginMinVersion: 0.3.0
profiles:
  finance-default:
    chartOfAccounts:
      balancingSegment: segment1
      costCenterSegment: segment2
      naturalAccountSegment: segment3
"""

NODE_YAML = """
id: dim_x
layer: silver
implementation:
  type: sql
  sql: silver/dim_x.sql
target: dim_x
dependsOn:
  bronze: []
refresh:
  seed:
    strategy: replace
outputSchema:
  columns:
    - name: x
      type: bigint
      nullable: false
      pii: none
"""


def _pack(tmp_path: pathlib.Path):
    pack_root = tmp_path / "pack"
    pack_root.mkdir()
    (pack_root / "pack.yaml").write_text(PACK_YAML, encoding="utf-8")
    silver = pack_root / "silver"
    silver.mkdir()
    (silver / "dim_x.yaml").write_text(NODE_YAML, encoding="utf-8")
    (silver / "dim_x.sql").write_text("SELECT 1\n", encoding="utf-8")
    return load_pack(pack_root)


def test_dry_run_summary_carries_populated_plan(tmp_path, monkeypatch):
    """The dry_run code path returns a RunSummary with a populated
    ``plan`` of PlanNodes — not the (legacy) empty tuple."""
    pack = _pack(tmp_path)
    # Build a synthetic bundle that points at the pack.
    bundle_yaml = (
        "apiVersion: aidp-fusion-autopilot/v1\n"
        "project: drytest\n"
        "fusion:\n"
        "  serviceUrl: https://example.com\n"
        "  username: u\n"
        "  password: p\n"
        "  externalStorage: s\n"
        "aidp:\n"
        "  catalog: c\n"
        "  bronzeSchema: b\n"
        "  silverSchema: s\n"
        "  goldSchema: g\n"
        "datasets:\n"
        "  - id: ap_invoices\n"
        "    mode: incremental\n"
        # Phase 9 cross-layer datasets[]: declare dim_x so the
        # bundle_scope picks it up as a silver root.
        "  - id: dim_x\n"
        "dimensions:\n  build: []\n"
        "gold:\n  marts: []\n"
        f"contentPack:\n"
        f"  name: phase5-dry-run-renderer-test\n"
        f"  profile: finance-default\n"
    )
    bundle_path = tmp_path / "bundle.yaml"
    bundle_path.write_text(bundle_yaml, encoding="utf-8")

    profile_dir = tmp_path / "profiles"
    profile_dir.mkdir()
    (profile_dir / "finance-default.yaml").write_text(
        "schemaVersion: 1\n"
        "tenant: drytest\n"
        "pinnedAt: 2026-06-08T00:00:00+00:00\n"
        "bronzeSchemaFingerprint: 'sha256:dry'\n",
        encoding="utf-8",
    )

    from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
        load_tenant_profile,
    )
    profile = load_tenant_profile(profile_dir / "finance-default.yaml")

    summary = _run_content_pack_backend(
        bundle_path=bundle_path,
        spark=None,
        mode="seed",
        datasets=None,
        layers=None,
        dry_run=True,
        resume_run_id=None,
        resolved_pack=pack,
        tenant_profile=profile,
    )
    assert summary.plan is not None
    assert len(summary.plan) > 0
    plan_ids = [pn.dataset_id for pn in summary.plan]
    assert "dim_x" in plan_ids

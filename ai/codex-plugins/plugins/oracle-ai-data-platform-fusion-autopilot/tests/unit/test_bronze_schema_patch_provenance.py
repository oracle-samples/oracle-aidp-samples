"""FR-9 provenance chain + FR-6 drift-truthfulness for bronze schemaPatches.

Covers: the marker-tolerant ``RunSummary.applied_schema_patches`` round
trip, the concrete provenance artifact (writer + the shared persister
branch), the run-summary console line, and the regression pin that the
PVO drift probe NEVER reads through a patched schema.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from rich.console import Console

from oracle_ai_data_platform_fusion_autopilot.schema.diagnostic_artifact import (
    persist_run_diagnostics,
    write_schema_patch_provenance,
)
from oracle_ai_data_platform_fusion_autopilot.schema.run_summary import (
    RunStep,
    RunSummary,
)

NOW = datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc)


def _summary(applied=None) -> RunSummary:
    return RunSummary(
        run_id="run-1",
        started_at=NOW,
        finished_at=NOW,
        bundle_project="proj",
        mode="seed",
        steps=(
            RunStep(
                run_id="run-1", dataset_id="scm_items", layer="bronze",
                mode="seed", status="success", row_count=7,
                duration_seconds=1.0, error_message=None,
                watermark_used=None, last_watermark=None,
                plan_hash=None, plan_snapshot=None,
            ),
        ),
        applied_schema_patches=applied,
    )


class TestMarkerRoundTrip:
    def test_round_trips_applied_patches(self) -> None:
        s = _summary({"scm_items": ("ItemBasePEOMaterialCost",)})
        restored = RunSummary.from_marker_dict(s.to_marker_dict())
        assert restored.applied_schema_patches == {
            "scm_items": ("ItemBasePEOMaterialCost",)
        }

    def test_absent_field_tolerated(self) -> None:
        payload = _summary().to_marker_dict()
        payload.pop("applied_schema_patches", None)  # old-marker shape
        restored = RunSummary.from_marker_dict(payload)
        assert restored.applied_schema_patches is None

    def test_success_step_error_message_stays_null(self) -> None:
        s = _summary({"scm_items": ("ItemBasePEOMaterialCost",)})
        assert s.steps[0].error_message is None


class TestProvenanceArtifact:
    def test_writer_shape_and_filename(self, tmp_path: Path) -> None:
        path = write_schema_patch_provenance(
            tmp_path, "run-1", {"scm_items": ("ItemBasePEOMaterialCost",)}
        )
        assert path.name == "schema-patches__run-1.json"
        payload = json.loads(path.read_text())
        assert payload["schemaVersion"] == 1
        assert payload["runId"] == "run-1"
        assert payload["appliedSchemaPatches"] == {
            "scm_items": ["ItemBasePEOMaterialCost"]
        }
        assert "generatedAt" in payload

    def test_persister_writes_when_present(self, tmp_path: Path) -> None:
        persist_run_diagnostics(
            tmp_path, _summary({"scm_items": ("ItemBasePEOMaterialCost",)})
        )
        target = (
            tmp_path / ".aidp" / "diagnostics" / "run-1"
            / "schema-patches__run-1.json"
        )
        assert target.exists()

    def test_persister_silent_when_absent(self, tmp_path: Path) -> None:
        persist_run_diagnostics(tmp_path, _summary(None))
        diag_root = tmp_path / ".aidp" / "diagnostics"
        assert not diag_root.exists() or not any(
            p.name.startswith("schema-patches__")
            for p in diag_root.rglob("*.json")
        )


class TestRunSummaryConsoleLine:
    def test_render_names_dataset_and_columns(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.run import (
            _render_summary,
        )

        console = Console(record=True, width=120)
        _render_summary(
            console, _summary({"scm_items": ("ItemBasePEOMaterialCost",)})
        )
        text = console.export_text()
        assert "scm_items" in text
        assert "ItemBasePEOMaterialCost" in text
        assert "schemaPatches" in text

    def test_render_silent_without_patches(self) -> None:
        from oracle_ai_data_platform_fusion_autopilot.commands.run import (
            _render_summary,
        )

        console = Console(record=True, width=120)
        _render_summary(console, _summary(None))
        assert "schemaPatches" not in console.export_text()


class TestDriftStaysTruthful:
    """FR-6: the PVO drift probe compares the CONNECTOR's schema — a patch
    must never mask real drift, so ``probe_bronze_schemas`` never passes a
    ``user_schema`` even when the bundle configures patches."""

    def test_probe_never_passes_user_schema(self, tmp_path, monkeypatch) -> None:
        from oracle_ai_data_platform_fusion_autopilot.extractors import (
            bicc as bicc_extractor,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.builtins import (
            bronze_extract_adapter,
        )
        from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
            load_pack,
        )

        pack_yaml = (
            "id: drift-probe-test\nversion: 1.0.0\n"
            "description: drift probe stays unpatched\n"
            "compatibility:\n  pluginMinVersion: 0.3.0\n"
        )
        node_yaml = (
            "id: scm_items\nlayer: bronze\nimplementation:\n"
            "  type: bronze_extract\n  datastore: ItemExtractPVO\n"
            "  biccSchema: Financial\n  incrementalCapable: false\n"
            "target: scm_items\ndependsOn:\n  bronze: []\n  silver: []\n"
            "refresh:\n  seed:\n    strategy: replace\n"
            "  incremental:\n    strategy: merge\n    watermark:\n"
            "      source: scm_items\n      column: L\n"
            "    naturalKey: [Id]\n"
            "outputSchema:\n  columns:\n"
            "    - { name: Id, type: long, nullable: true, pii: none }\n"
            "quality:\n  tests: []\n"
        )
        root = tmp_path / "pack"
        (root / "bronze").mkdir(parents=True)
        (root / "pack.yaml").write_text(pack_yaml)
        (root / "bronze" / "scm_items.yaml").write_text(node_yaml)
        pack = load_pack(root)

        captured: list[dict] = []

        def fake_extract(spark, descriptor, **kwargs):
            captured.append(kwargs)
            df = MagicMock()
            df.schema = MagicMock()
            return df

        monkeypatch.setattr(bicc_extractor, "extract_pvo", fake_extract)

        ds = MagicMock()
        ds.id = "scm_items"
        ds.schema_patches = {"ItemBasePEOMaterialCost": "bigint"}
        bundle = MagicMock()
        bundle.datasets = [ds]

        bronze_extract_adapter.probe_bronze_schemas(
            MagicMock(), pack=pack, bundle=bundle, resolved_password="pw",
        )
        assert captured, "probe did not reach the extractor"
        assert all("user_schema" not in kw for kw in captured)

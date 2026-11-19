"""Phase 9 Step 10: `aidp-fusion-autopilot catalog probe-pvo --emit-pack-yaml`.

Verifies the draft-YAML emitter writes a load-clean ``NodeYaml``-shape
with the discovered columns, the standard audit columns, and the
commented-out ``refresh.incremental`` TODOs the plan calls for.
"""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml as pyyaml
from click.testing import CliRunner

from oracle_ai_data_platform_fusion_autopilot import cli
from oracle_ai_data_platform_fusion_autopilot.commands.catalog import _spark_type_to_yaml
from oracle_ai_data_platform_fusion_autopilot.schema.medallion_pack import NodeYaml

# Tests that mock ``pyspark.sql.SparkSession`` need pyspark importable. The
# ``spark=no`` CI matrix leg does not install it, so skip those (the type-mapper
# and CLI-wireup tests below do not need Spark and still run).
_HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None


class TestSparkTypeMapper:
    def test_string_type(self):
        assert _spark_type_to_yaml("StringType") == "string"

    def test_long_type(self):
        assert _spark_type_to_yaml("LongType") == "long"

    def test_integer_type(self):
        assert _spark_type_to_yaml("IntegerType") == "integer"

    def test_timestamp_type(self):
        assert _spark_type_to_yaml("TimestampType") == "timestamp"

    def test_decimal_with_precision(self):
        assert _spark_type_to_yaml("DecimalType(28,8)") == "decimal(28,8)"


class _StubType:
    """Stand-in for a Spark StructField.dataType — str() returns the
    DDL-ish form (e.g. ``StringType``, ``DecimalType(28,8)``)."""

    def __init__(self, repr_str: str) -> None:
        self._repr = repr_str

    def __str__(self) -> str:
        return self._repr


class _StubField:
    def __init__(self, name: str, dtype: str, nullable: bool) -> None:
        self.name = name
        self.dataType = _StubType(dtype)
        self.nullable = nullable


class _StubSchema:
    def __init__(self, fields: list[_StubField]) -> None:
        self.fields = fields


def _mock_spark_with_schema(field_specs: list[tuple[str, str, bool]]) -> tuple[MagicMock, _StubSchema]:
    """Build a mock SparkSession.builder.getOrCreate() that returns a
    session whose extract_pvo→.schema yields the requested fields."""
    spark = MagicMock()
    spark.builder.getOrCreate.return_value = spark

    fields = [_StubField(name, dtype, nullable) for name, dtype, nullable in field_specs]
    return spark, _StubSchema(fields)


@pytest.fixture
def minimal_bundle(tmp_path: Path) -> Path:
    """Write a minimal bundle.yaml shell that probe-pvo can read for
    fusion credentials. Returns the path."""
    bundle_path = tmp_path / "bundle.yaml"
    bundle_path.write_text("""\
apiVersion: aidp-fusion-autopilot/v1
project: probe-test
fusion:
  serviceUrl: https://example.fa.us-phoenix-1.oraclecloud.com
  username: probe-user
  password: ${env:FUSION_BICC_PASSWORD}
  externalStorage: probe-storage
aidp:
  catalog: fusion_catalog
  bronzeSchema: bronze
  silverSchema: silver
  goldSchema: gold
datasets: []
""")
    return bundle_path


@pytest.mark.skipif(not _HAS_PYSPARK, reason="requires pyspark (spark=no CI leg)")
class TestProbePvoEmitsLoadCleanYaml:
    """End-to-end: invoke the emitter, parse the YAML, validate via NodeYaml."""

    def test_emits_full_yaml_with_audit_cols(
        self, tmp_path: Path, minimal_bundle: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "secret")

        spark, fake_schema = _mock_spark_with_schema([
            ("VENDORID", "LongType", True),
            ("SEGMENT1", "StringType", True),
            ("LASTUPDATEDATE", "TimestampType", True),
        ])
        out_yaml = tmp_path / "bronze" / "hr_workers.yaml"

        with patch("pyspark.sql.SparkSession", spark), \
             patch(
                 "oracle_ai_data_platform_fusion_autopilot.extractors.bicc.extract_pvo"
             ) as mock_extract:
            mock_df = MagicMock()
            mock_df.schema = fake_schema
            mock_extract.return_value = mock_df

            from oracle_ai_data_platform_fusion_autopilot.commands.catalog import (
                probe_pvo_emit_pack_yaml,
            )
            exit_code = probe_pvo_emit_pack_yaml(
                dataset_id="hr_workers",
                datastore="WorkerExtractPVO",
                bicc_schema="HCM",
                pvo_id="FscmTopModelAM.HcmTopAM.HrBiccExtractAM.WorkerExtractPVO",
                incremental_capable=True,
                emit_pack_yaml=str(out_yaml),
                bundle_path=str(minimal_bundle),
            )

        assert exit_code == 0
        assert out_yaml.exists()

        raw = pyyaml.safe_load(out_yaml.read_text(encoding="utf-8"))
        assert raw["id"] == "hr_workers"
        assert raw["layer"] == "bronze"
        assert raw["implementation"]["type"] == "bronze_extract"
        assert raw["implementation"]["datastore"] == "WorkerExtractPVO"
        assert raw["implementation"]["biccSchema"] == "HCM"
        assert raw["implementation"]["pvo_id"].endswith(".WorkerExtractPVO")
        assert raw["implementation"]["incrementalCapable"] is True

        # Discovered columns + audit cols.
        col_names = [c["name"] for c in raw["outputSchema"]["columns"]]
        assert "VENDORID" in col_names
        assert "SEGMENT1" in col_names
        assert "LASTUPDATEDATE" in col_names
        assert "_extract_ts" in col_names
        assert "_source_pvo" in col_names
        assert "_run_id" in col_names
        assert "_watermark_used" in col_names

        # refresh.incremental block is commented out (TODO markers).
        text = out_yaml.read_text(encoding="utf-8")
        assert "# incremental:" in text
        assert "TODO_WATERMARK_COLUMN" in text
        assert "TODO_NATURAL_KEY" in text

    def test_yaml_round_trips_through_pydantic(
        self, tmp_path: Path, minimal_bundle: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Generated YAML must round-trip through ``NodeYaml`` Pydantic
        validation without errors (catches schema/template drift)."""
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "secret")

        spark, fake_schema = _mock_spark_with_schema([
            ("PK_ID", "LongType", False),
            ("PAYLOAD", "StringType", True),
        ])
        out_yaml = tmp_path / "bronze" / "test_pvo.yaml"

        with patch("pyspark.sql.SparkSession", spark), \
             patch(
                 "oracle_ai_data_platform_fusion_autopilot.extractors.bicc.extract_pvo"
             ) as mock_extract:
            mock_extract.return_value = MagicMock(schema=fake_schema)

            from oracle_ai_data_platform_fusion_autopilot.commands.catalog import (
                probe_pvo_emit_pack_yaml,
            )
            probe_pvo_emit_pack_yaml(
                dataset_id="test_pvo",
                datastore="TestExtractPVO",
                bicc_schema="Financial",
                pvo_id=None,  # No pvo_id key emitted at all.
                incremental_capable=True,
                emit_pack_yaml=str(out_yaml),
                bundle_path=str(minimal_bundle),
            )

        raw = pyyaml.safe_load(out_yaml.read_text(encoding="utf-8"))
        # The refresh.incremental block is commented out, so Pydantic
        # validates the seed-only path — that's a load-clean shape.
        node = NodeYaml.model_validate(raw)
        assert node.layer == "bronze"
        assert node.implementation.type == "bronze_extract"
        assert node.implementation.datastore == "TestExtractPVO"

    def test_pvo_id_omitted_when_not_supplied(
        self, tmp_path: Path, minimal_bundle: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When --pvo-id is absent, the emitted YAML must OMIT the
        pvo_id key entirely (not emit as empty string or null).
        Empty value would trip a FALSE AIDPF-2080."""
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "secret")
        spark, fake_schema = _mock_spark_with_schema([("X", "LongType", True)])
        out_yaml = tmp_path / "bronze" / "no_pvo.yaml"

        with patch("pyspark.sql.SparkSession", spark), \
             patch(
                 "oracle_ai_data_platform_fusion_autopilot.extractors.bicc.extract_pvo"
             ) as mock_extract:
            mock_extract.return_value = MagicMock(schema=fake_schema)

            from oracle_ai_data_platform_fusion_autopilot.commands.catalog import (
                probe_pvo_emit_pack_yaml,
            )
            probe_pvo_emit_pack_yaml(
                dataset_id="no_pvo",
                datastore="NoPvoExtractPVO",
                bicc_schema="Financial",
                pvo_id=None,
                incremental_capable=True,
                emit_pack_yaml=str(out_yaml),
                bundle_path=str(minimal_bundle),
            )

        raw = pyyaml.safe_load(out_yaml.read_text(encoding="utf-8"))
        assert "pvo_id" not in raw["implementation"], (
            f"pvo_id should be absent, got: {raw['implementation']!r}"
        )

    def test_audit_columns_appended_at_end(
        self, tmp_path: Path, minimal_bundle: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FUSION_BICC_PASSWORD", "secret")
        spark, fake_schema = _mock_spark_with_schema([
            ("FIRST_COL", "StringType", True),
            ("LAST_DATA_COL", "LongType", True),
        ])
        out_yaml = tmp_path / "bronze" / "ordered.yaml"

        with patch("pyspark.sql.SparkSession", spark), \
             patch(
                 "oracle_ai_data_platform_fusion_autopilot.extractors.bicc.extract_pvo"
             ) as mock_extract:
            mock_extract.return_value = MagicMock(schema=fake_schema)

            from oracle_ai_data_platform_fusion_autopilot.commands.catalog import (
                probe_pvo_emit_pack_yaml,
            )
            probe_pvo_emit_pack_yaml(
                dataset_id="ordered",
                datastore="Ordered",
                bicc_schema="Financial",
                pvo_id=None,
                incremental_capable=True,
                emit_pack_yaml=str(out_yaml),
                bundle_path=str(minimal_bundle),
            )

        raw = pyyaml.safe_load(out_yaml.read_text(encoding="utf-8"))
        names = [c["name"] for c in raw["outputSchema"]["columns"]]
        # Audit columns must be at the end.
        assert names[-4:] == [
            "_extract_ts", "_source_pvo", "_run_id", "_watermark_used"
        ]

    def test_no_bundle_path_returns_error_code(self) -> None:
        """The emitter requires a bundle for BICC connectivity."""
        from oracle_ai_data_platform_fusion_autopilot.commands.catalog import (
            probe_pvo_emit_pack_yaml,
        )
        exit_code = probe_pvo_emit_pack_yaml(
            dataset_id="x",
            datastore="X",
            bicc_schema="Y",
            pvo_id=None,
            incremental_capable=True,
            emit_pack_yaml="/tmp/x.yaml",
            bundle_path=None,
        )
        assert exit_code == 2


class TestCliWireup:
    def test_help_message(self) -> None:
        result = CliRunner().invoke(cli.main, ["catalog", "probe-pvo", "--help"])
        assert result.exit_code == 0
        assert "--datastore" in result.output
        assert "--bicc-schema" in result.output
        assert "--emit-pack-yaml" in result.output
        assert "--pvo-id" in result.output

    def test_missing_required_datastore_fails(self) -> None:
        result = CliRunner().invoke(cli.main, [
            "catalog", "probe-pvo", "hr_workers",
            "--bicc-schema", "HCM",
            "--emit-pack-yaml", "/tmp/x.yaml",
        ])
        assert result.exit_code != 0
        assert "datastore" in result.output.lower()

    def test_missing_required_bicc_schema_fails(self) -> None:
        result = CliRunner().invoke(cli.main, [
            "catalog", "probe-pvo", "hr_workers",
            "--datastore", "X",
            "--emit-pack-yaml", "/tmp/x.yaml",
        ])
        assert result.exit_code != 0
        assert "bicc-schema" in result.output.lower() or "bicc_schema" in result.output.lower()

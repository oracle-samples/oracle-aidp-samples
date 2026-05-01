"""Unit tests for the BICC extractor option-builder.

Tests verify that the option dict matches the verbatim shape from the official
Oracle AIDP sample notebook (`Read_Only_Ingestion_Connectors.ipynb`) and from
pdf1 Step 3 — without invoking Spark.
"""

from __future__ import annotations

import pytest

from oracle_ai_data_platform_fusion_bundle.extractors.bicc import build_options_dict, extract_pvo
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import PvoEntry, PvoKind, get


class TestBuildOptionsDict:
    def test_full_extract_options_match_official_sample_shape(self) -> None:
        pvo = get("erp_suppliers")
        opts = build_options_dict(
            pvo,
            fusion_service_url="https://my-pod.fa.us-phoenix-1.oraclecloud.com",
            username="Casey.Brown",
            password="REDACTED",
            fusion_external_storage="my_external_storage",
        )
        # Per pdf1 Step 3 + Read_Only_Ingestion_Connectors.ipynb:
        assert opts == {
            "type": "FUSION_BICC",
            "fusion.service.url": "https://my-pod.fa.us-phoenix-1.oraclecloud.com",
            "user.name": "Casey.Brown",
            "password": "REDACTED",
            "schema": "Financial",                    # from PvoEntry.schema
            "fusion.external.storage": "my_external_storage",
            "datastore": "FscmTopModelAM.PrcExtractAM.PozBiccExtractAM.SupplierExtractPVO",
        }

    def test_incremental_adds_extract_date_option(self) -> None:
        pvo = get("po_orders")
        opts = build_options_dict(
            pvo,
            fusion_service_url="https://x.fa.us-ashburn-1.oraclecloud.com",
            username="user",
            password="pwd",
            fusion_external_storage="bucket",
            watermark="2026-04-01T00:00:00Z",
        )
        # Per pdf2 p2-3:
        assert opts["fusion.initial.extract-date"] == "2026-04-01T00:00:00Z"

    def test_schema_override_wins_over_pvo_default(self) -> None:
        pvo = get("erp_suppliers")
        opts = build_options_dict(
            pvo,
            fusion_service_url="https://x",
            username="u",
            password="p",
            fusion_external_storage="b",
            schema="ERP",                              # override
        )
        assert opts["schema"] == "ERP"

    def test_no_watermark_means_no_extract_date_key(self) -> None:
        pvo = get("erp_suppliers")
        opts = build_options_dict(
            pvo,
            fusion_service_url="https://x",
            username="u",
            password="p",
            fusion_external_storage="b",
        )
        assert "fusion.initial.extract-date" not in opts


class TestOtbiRefusal:
    def test_extract_pvo_rejects_otbi_pvo(self) -> None:
        otbi = PvoEntry(
            id="some_otbi_thing",
            datastore="OtbiAnalyticsThing",
            schema="Financial",
            bronze_table="fusion_catalog.bronze.some_otbi",
            description="OTBI reporting (NOT for bulk).",
            kind=PvoKind.OTBI,
        )
        # We don't actually invoke Spark here — the OTBI guard runs first.
        with pytest.raises(ValueError, match="OTBI"):
            extract_pvo(
                spark=None,  # type: ignore[arg-type]  # never reached
                pvo=otbi,
                fusion_service_url="https://x",
                username="u",
                password="p",
                fusion_external_storage="b",
            )

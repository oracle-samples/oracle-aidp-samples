"""Unit tests for the OAC connection JSON builder."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.oac.rest.connection import (
    AidpConnectionPayload,
    build_dsn,
    build_payload,
    render_template,
)


class TestBuildDsn:
    def test_default_format(self) -> None:
        dsn = build_dsn(region="us-ashburn-1", cluster_key="abc-123")
        assert dsn == (
            "jdbc:spark://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/default;"
            "SparkServerType=AIDP;httpPath=cliservice/abc-123"
        )

    def test_catalog_override(self) -> None:
        dsn = build_dsn(region="us-phoenix-1", cluster_key="k", catalog="fusion_catalog")
        assert "/fusion_catalog;" in dsn

    def test_gateway_override(self) -> None:
        dsn = build_dsn(region="us-ashburn-1", cluster_key="k", gateway_host="alt.host")
        assert dsn.startswith("jdbc:spark://alt.host/default;")


class TestBuildPayload:
    def test_six_keys_with_kebab_idl_ocid(self) -> None:
        p = build_payload(
            user_ocid="ocid1.user.oc1..u",
            tenancy_ocid="ocid1.tenancy.oc1..t",
            region="us-ashburn-1",
            fingerprint="aa:bb:cc",
            idl_ocid="ocid1.aidataplatform.oc1.iad..d",
            cluster_key="cl-key",
        )
        out = p.to_dict()
        assert set(out.keys()) == {"username", "tenancy", "region", "fingerprint", "idl-ocid", "dsn"}
        assert out["username"] == "ocid1.user.oc1..u"
        assert out["tenancy"] == "ocid1.tenancy.oc1..t"
        assert out["idl-ocid"] == "ocid1.aidataplatform.oc1.iad..d"
        assert "cliservice/cl-key" in out["dsn"]

    def test_to_json_roundtrip(self) -> None:
        p = build_payload(
            user_ocid="u", tenancy_ocid="t", region="us-ashburn-1",
            fingerprint="fp", idl_ocid="idl", cluster_key="ck",
        )
        loaded = json.loads(p.to_json())
        assert loaded == p.to_dict()


class TestRenderTemplate:
    def test_writes_json_to_disk(self, tmp_path: Path) -> None:
        p = build_payload(
            user_ocid="u", tenancy_ocid="t", region="us-ashburn-1",
            fingerprint="fp", idl_ocid="idl", cluster_key="ck",
        )
        out = render_template(p, tmp_path / "subdir" / "conn.json")
        assert out.exists()
        loaded = json.loads(out.read_text(encoding="utf-8"))
        assert loaded["username"] == "u"
        assert loaded["idl-ocid"] == "idl"

    def test_creates_parent_dir(self, tmp_path: Path) -> None:
        p = AidpConnectionPayload(
            username="u", tenancy="t", region="r", fingerprint="f",
            idl_ocid="i", dsn="d",
        )
        out = render_template(p, tmp_path / "deep" / "nested" / "x.json")
        assert out.exists()

"""Tests for `aidp-fusion-autopilot use-pack` — one-command bundle->overlay wiring.

Covers the comment-preserving text surgery (append/replace top-level blocks,
placeholder-password normalization) and an end-to-end wire against the shipped
fusion-finance-ar-ext overlay example.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO / "scripts"))

from oracle_ai_data_platform_fusion_autopilot.commands.use_pack import (  # noqa: E402
    _PLACEHOLDER_PW,
    _replace_or_append_top_level_block,
    use_pack,
)


def test_append_when_key_absent_preserves_everything():
    text = "project: x\n\nfusion:\n  username: u  # keep this comment\n"
    out = _replace_or_append_top_level_block(text, "contentPack", "contentPack:\n  name: p\n")
    assert "# keep this comment" in out          # untouched
    assert out.rstrip().endswith("name: p")        # appended
    assert "project: x" in out


def test_replace_existing_block_preserves_neighbours():
    text = (
        "project: x\n"
        "contentPack:\n  name: old\n  profile: old\n"
        "notifications:\n  onFailure: [a]\n"
    )
    out = _replace_or_append_top_level_block(text, "contentPack", "contentPack:\n  name: new\n")
    assert "name: new" in out and "name: old" not in out
    assert "project: x" in out                      # block before preserved
    assert "notifications:" in out                  # block after preserved


def test_placeholder_password_regex_matches_and_skips_env_form():
    assert _PLACEHOLDER_PW.search("  password: ${vault:ocid1.vaultsecret.oc1.iad.placeholder.fusion_password}")
    assert not _PLACEHOLDER_PW.search("  password: ${FUSION_BICC_PASSWORD}")
    assert not _PLACEHOLDER_PW.search("  password: ${vault:ocid1.vaultsecret.oc1.iad.real.fusion_password}")


def test_end_to_end_wire_against_ar_ext_overlay(tmp_path):
    overlay = _REPO / "overlays" / "fusion-finance-ar-ext"
    if not (overlay / "pack.yaml").exists():
        pytest.skip("fusion-finance-ar-ext overlay not present")
    bundle = tmp_path / "bundle.yaml"
    bundle.write_text(
        "apiVersion: aidp-fusion-autopilot/v1\nversion: \"0.2.0\"\nproject: t\n"
        "fusion:\n  serviceUrl: https://x\n  username: u\n"
        "  password: ${vault:ocid1.vaultsecret.oc1.iad.placeholder.fusion_password}\n"
        "  externalStorage: s\n"
        "aidp:\n  catalog: c\n  bronzeSchema: bronze\n  silverSchema: silver\n  goldSchema: gold\n"
        "datasets:\n  - id: ar_invoices\n    mode: full\n",
        encoding="utf-8",
    )
    rc = use_pack(bundle, str(overlay), "finance-default", align=True, fix_credentials=True)
    assert rc == 0
    import yaml
    raw = yaml.safe_load(bundle.read_text(encoding="utf-8"))
    assert raw["contentPack"]["name"] == "fusion-finance-ar-ext"   # overlay id, not base
    assert raw["contentPack"]["profile"] == "finance-default"
    assert "ar_invoice_summary" in raw["gold"]["marts"]            # aligned to merged pack
    assert raw["fusion"]["password"] == "${FUSION_BICC_PASSWORD}"  # normalized

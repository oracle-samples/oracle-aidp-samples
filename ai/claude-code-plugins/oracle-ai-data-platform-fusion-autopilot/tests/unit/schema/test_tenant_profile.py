"""Unit tests for ``schema/tenant_profile.py`` (Phase 2 Step 2).

Covers:
* ``TenantProfile`` Pydantic model — happy path + alias normalisation.
* ``load_tenant_profile`` — file loader; malformed YAML, unsupported
  schemaVersion, validation errors all carry the appropriate AIDPF code.
* ``load_tenant_profile_from_string`` — cluster-side string loader
  (used by the REST notebook bootstrap).
* ``compute_profile_hash`` — deterministic across runs; cosmetic
  YAML changes don't shift it; semantic value changes do.
* ``resolve_profile_path`` — returns the canonical
  ``<bundle.yaml.parent>/profiles/<name>.yaml`` shape.
"""

from __future__ import annotations

import pathlib
from datetime import datetime, timezone

import pytest

from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH,
    AIDPF_1051_TENANT_PROFILE_UNSUPPORTED_VERSION,
    TenantProfile,
    TenantProfileSchemaError,
    compute_profile_hash,
    load_tenant_profile,
    load_tenant_profile_from_string,
    resolve_profile_path,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


VALID_PROFILE_YAML = """
schemaVersion: 1
tenant: acme-prod
pinnedAt: 2026-06-01T12:00:00+00:00
bronzeSchemaFingerprint: "sha256:abc123"
resolved:
  column:
    invoice_currency_code: ApInvoicesInvoiceCurrencyCode
  semantic:
    cancelled_status: cancelled_date
profile:
  calendar:
    fiscalStartMonth: 4
    startDate: "2024-01-01"
  chartOfAccounts:
    balancingSegment: segment1
provenance:
  bootstrapRunId: "boot-2026-06-01-001"
"""


# ---------------------------------------------------------------------------
# TenantProfile model — happy path
# ---------------------------------------------------------------------------


class TestTenantProfileModel:
    def test_valid_profile_parses(self) -> None:
        profile = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        assert profile.schema_version == 1
        assert profile.tenant == "acme-prod"
        assert profile.bronze_schema_fingerprint == "sha256:abc123"

    def test_resolved_variation_points_populated(self) -> None:
        profile = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        assert profile.resolved.column == {
            "invoice_currency_code": "ApInvoicesInvoiceCurrencyCode"
        }
        assert profile.resolved.semantic == {"cancelled_status": "cancelled_date"}

    def test_pinnedAt_alias_normalised(self) -> None:
        profile = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        assert isinstance(profile.pinned_at, datetime)

    def test_freeform_profile_block_preserved(self) -> None:
        profile = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        # Nested dicts in `profile.<key>` are passed through as-is.
        assert profile.profile["calendar"]["fiscalStartMonth"] == 4
        assert profile.profile["chartOfAccounts"]["balancingSegment"] == "segment1"

    def test_minimal_profile_with_no_variation_points(self) -> None:
        """A pack that declares no variation points still needs a profile
        for tenant identity + fingerprint. Empty `resolved` is fine."""
        yaml_text = """
schemaVersion: 1
tenant: simple-tenant
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:deadbeef"
"""
        profile = load_tenant_profile_from_string(yaml_text)
        assert profile.resolved.column == {}
        assert profile.resolved.semantic == {}


# ---------------------------------------------------------------------------
# Loader error paths
# ---------------------------------------------------------------------------


class TestLoaderErrorPaths:
    def test_invalid_schema_version_rejected_with_1051(self) -> None:
        yaml_text = """
schemaVersion: 99
tenant: x
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:x"
"""
        with pytest.raises(TenantProfileSchemaError) as exc_info:
            load_tenant_profile_from_string(yaml_text)
        assert AIDPF_1051_TENANT_PROFILE_UNSUPPORTED_VERSION in str(exc_info.value)

    def test_missing_required_field_tenant_raises_1050(self) -> None:
        yaml_text = """
schemaVersion: 1
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:x"
"""
        with pytest.raises(TenantProfileSchemaError) as exc_info:
            load_tenant_profile_from_string(yaml_text)
        assert AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH in str(exc_info.value)
        assert "tenant" in str(exc_info.value)

    def test_missing_fingerprint_is_legacy_profile_not_error(self) -> None:
        """Phase 3c (ADR-0017 §11.6) — ``bronzeSchemaFingerprint`` is
        Optional. Profiles authored before Phase 3c bootstrap landed
        won't have it; the runtime drift gate treats them as legacy
        (skip with warning), not as a schema error. The field is the
        Phase 3c addition, not the v1 baseline."""
        yaml_text = """
schemaVersion: 1
tenant: x
pinnedAt: 2026-06-01T00:00:00+00:00
"""
        profile = load_tenant_profile_from_string(yaml_text)
        assert profile.tenant == "x"
        assert profile.bronze_schema_fingerprint is None

    def test_unknown_top_level_key_rejected(self) -> None:
        """``extra='forbid'`` rejects typos in top-level keys."""
        yaml_text = """
schemaVersion: 1
tenant: x
pinnedAt: 2026-06-01T00:00:00+00:00
bronzeSchemaFingerprint: "sha256:x"
frobnicate: true
"""
        with pytest.raises(TenantProfileSchemaError) as exc_info:
            load_tenant_profile_from_string(yaml_text)
        assert "frobnicate" in str(exc_info.value).lower() or "extra" in str(exc_info.value).lower()

    def test_malformed_yaml_raises_1050(self) -> None:
        yaml_text = "this: is\n  not: [valid"
        with pytest.raises(TenantProfileSchemaError) as exc_info:
            load_tenant_profile_from_string(yaml_text)
        assert AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH in str(exc_info.value)

    def test_top_level_must_be_mapping(self) -> None:
        yaml_text = "- not\n- a\n- mapping\n"
        with pytest.raises(TenantProfileSchemaError) as exc_info:
            load_tenant_profile_from_string(yaml_text)
        assert AIDPF_1050_TENANT_PROFILE_SCHEMA_MISMATCH in str(exc_info.value)


# ---------------------------------------------------------------------------
# File loader
# ---------------------------------------------------------------------------


class TestLoadTenantProfileFromFile:
    def test_round_trip_via_disk(self, tmp_path: pathlib.Path) -> None:
        f = tmp_path / "profile.yaml"
        f.write_text(VALID_PROFILE_YAML, encoding="utf-8")
        profile = load_tenant_profile(f)
        assert profile.tenant == "acme-prod"

    def test_error_message_carries_filename(self, tmp_path: pathlib.Path) -> None:
        f = tmp_path / "broken.yaml"
        f.write_text("schemaVersion: 1\ntenant: x\n", encoding="utf-8")
        with pytest.raises(TenantProfileSchemaError) as exc_info:
            load_tenant_profile(f)
        # Source filename appears in the error so an operator can find the file.
        assert str(f) in str(exc_info.value)


# ---------------------------------------------------------------------------
# compute_profile_hash determinism
# ---------------------------------------------------------------------------


class TestComputeProfileHash:
    def test_deterministic_across_runs(self) -> None:
        a = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        b = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        assert compute_profile_hash(a) == compute_profile_hash(b)

    def test_cosmetic_yaml_changes_dont_shift_hash(self) -> None:
        """Whitespace, comment, and key-ordering changes in the YAML are not
        semantic changes — the hash is computed from the validated model, so
        round-tripping through Pydantic + json.dumps(sort_keys=True) absorbs
        cosmetic differences.
        """
        a = load_tenant_profile_from_string(VALID_PROFILE_YAML)

        reordered_yaml = """
# leading comment

tenant: acme-prod
pinnedAt: 2026-06-01T12:00:00+00:00
schemaVersion: 1
profile:
  chartOfAccounts:
    balancingSegment: segment1
  calendar:
    startDate: "2024-01-01"
    fiscalStartMonth: 4
resolved:
  semantic:
    cancelled_status: cancelled_date
  column:
    invoice_currency_code: ApInvoicesInvoiceCurrencyCode
bronzeSchemaFingerprint: "sha256:abc123"
provenance:
  bootstrapRunId: "boot-2026-06-01-001"
"""
        b = load_tenant_profile_from_string(reordered_yaml)
        assert compute_profile_hash(a) == compute_profile_hash(b)

    def test_semantic_value_change_shifts_hash(self) -> None:
        """Changing a resolved variation-point pick MUST change the hash."""
        a = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        # Swap the semantic-variant pick from cancelled_date → cancelled_flag.
        b_yaml = VALID_PROFILE_YAML.replace("cancelled_date", "cancelled_flag")
        b = load_tenant_profile_from_string(b_yaml)
        assert compute_profile_hash(a) != compute_profile_hash(b)

    def test_profile_value_change_shifts_hash(self) -> None:
        """Changing a free-form ``profile.<key>`` value also shifts the hash."""
        a = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        b_yaml = VALID_PROFILE_YAML.replace("fiscalStartMonth: 4", "fiscalStartMonth: 7")
        b = load_tenant_profile_from_string(b_yaml)
        assert compute_profile_hash(a) != compute_profile_hash(b)

    def test_fingerprint_change_shifts_hash(self) -> None:
        a = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        b_yaml = VALID_PROFILE_YAML.replace("sha256:abc123", "sha256:zzz")
        b = load_tenant_profile_from_string(b_yaml)
        assert compute_profile_hash(a) != compute_profile_hash(b)


# ---------------------------------------------------------------------------
# resolve_profile_path
# ---------------------------------------------------------------------------


class TestResolveProfilePath:
    def test_returns_bundle_parent_profiles_dir(self, tmp_path: pathlib.Path) -> None:
        bundle_path = tmp_path / "project" / "bundle.yaml"
        bundle_path.parent.mkdir()
        bundle_path.write_text("# placeholder\n", encoding="utf-8")
        resolved = resolve_profile_path(bundle_path, "phase2-fixture")
        assert resolved == (tmp_path / "project" / "profiles" / "phase2-fixture.yaml").resolve()

    def test_does_not_require_file_to_exist(self, tmp_path: pathlib.Path) -> None:
        """``resolve_profile_path`` is purely path arithmetic; callers do
        the existence check separately so the missing-file error message
        can name the resolved path."""
        bundle_path = tmp_path / "bundle.yaml"
        # bundle.yaml itself doesn't exist; resolver still returns a path.
        resolved = resolve_profile_path(bundle_path, "no-such-profile")
        assert resolved == (tmp_path / "profiles" / "no-such-profile.yaml").resolve()


# ---------------------------------------------------------------------------
# Integration: built model is renderer-friendly
# ---------------------------------------------------------------------------


class TestIntegrationNotes:
    def test_profile_dotted_lookup_is_accessible_via_model_dump(self) -> None:
        """The SQL renderer's ``{{ profile.calendar.fiscalStartMonth }}``
        lookup walks ``profile.model_dump()`` — this test pins the shape."""
        profile = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        dumped = profile.model_dump(by_alias=True)
        # The free-form ``profile`` block lands at dumped["profile"].
        assert dumped["profile"]["calendar"]["fiscalStartMonth"] == 4

    def test_pinned_at_round_trip_with_timezone(self) -> None:
        profile = load_tenant_profile_from_string(VALID_PROFILE_YAML)
        assert profile.pinned_at.tzinfo is not None
        # Confirm UTC offset round-tripped.
        assert profile.pinned_at == datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

"""Unit tests for Phase 2 ContentPackSpec + Bundle.content_pack + resolve_content_pack_root.

Plan reference: Step 1.5 in docs/features/v2-phase-2-generic-sql-runner/plan.md.

Covers:
* v1 bundle (no contentPack) still validates.
* v2 bundle with contentPack validates (both YAML alias and Python attr names).
* Unknown nested keys under contentPack rejected (extra="forbid").
* resolve_content_pack_root: relative path resolved against bundle parent dir,
  absolute path used as-is, no-path → installed-pack lookup, missing installed
  pack → AIDPF-1037, resolved root without pack.yaml → AIDPF-1038, cwd
  independence for the relative case.
"""

from __future__ import annotations

import os
import pathlib

import pytest
import yaml
from pydantic import ValidationError

from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
    AIDPF_1037_INSTALLED_PACK_NOT_FOUND,
    AIDPF_1038_RESOLVED_ROOT_NO_PACK_YAML,
    Bundle,
    ContentPackRootInvalidError,
    ContentPackRootNotFoundError,
    ContentPackSpec,
    resolve_content_pack_root,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
EXAMPLES = REPO_ROOT / "examples"


# ---------------------------------------------------------------------------
# Bundle.content_pack schema
# ---------------------------------------------------------------------------


def _v1_minimal_bundle_yaml() -> dict:
    """Return a parsed v1 bundle (no contentPack:)."""
    raw = (EXAMPLES / "minimal_gl_only.yaml").read_text(encoding="utf-8")
    return yaml.safe_load(raw)


class TestBundleContentPackField:
    def test_v1_bundle_no_content_pack_validates(self) -> None:
        """v1 bundle (no contentPack) MUST still validate. content_pack defaults to None."""
        data = _v1_minimal_bundle_yaml()
        bundle = Bundle.model_validate(data)
        assert bundle.content_pack is None

    def test_v2_bundle_with_content_pack_validates_via_yaml_alias(self) -> None:
        """A bundle declaring `contentPack:` (YAML alias) parses correctly."""
        data = _v1_minimal_bundle_yaml()
        data["contentPack"] = {"name": "fusion-finance-starter", "profile": "demo"}
        bundle = Bundle.model_validate(data)
        assert bundle.content_pack is not None
        assert bundle.content_pack.name == "fusion-finance-starter"
        assert bundle.content_pack.profile == "demo"
        assert bundle.content_pack.path is None

    def test_v2_bundle_with_content_pack_validates_via_python_attr_name(self) -> None:
        """populate_by_name=True: also accepts snake_case `content_pack:` key."""
        data = _v1_minimal_bundle_yaml()
        data["content_pack"] = {"name": "fusion-finance-starter", "profile": "demo"}
        bundle = Bundle.model_validate(data)
        assert bundle.content_pack is not None
        assert bundle.content_pack.name == "fusion-finance-starter"

    def test_v2_bundle_with_path_override(self) -> None:
        """Customer overlay at a relative path validates."""
        data = _v1_minimal_bundle_yaml()
        data["contentPack"] = {
            "name": "acme-overlay",
            "path": "./overlays/acme",
            "profile": "acme-prod",
        }
        bundle = Bundle.model_validate(data)
        assert bundle.content_pack is not None
        assert bundle.content_pack.path == pathlib.Path("./overlays/acme")

    def test_unknown_nested_key_under_content_pack_rejected(self) -> None:
        """Strict mode: unknown nested keys raise ValidationError (extra='forbid')."""
        data = _v1_minimal_bundle_yaml()
        data["contentPack"] = {
            "name": "fusion-finance-starter",
            "profile": "demo",
            "frobnicate": True,  # unknown key
        }
        with pytest.raises(ValidationError) as exc_info:
            Bundle.model_validate(data)
        # The error must mention the offending key.
        assert "frobnicate" in str(exc_info.value)

    def test_content_pack_name_required(self) -> None:
        """`name` is required — missing → ValidationError."""
        data = _v1_minimal_bundle_yaml()
        data["contentPack"] = {"profile": "demo"}
        with pytest.raises(ValidationError):
            Bundle.model_validate(data)


# ---------------------------------------------------------------------------
# resolve_content_pack_root — three cases
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_project(tmp_path: pathlib.Path) -> pathlib.Path:
    """Construct a tmp project layout:

        tmp_path/
          project/
            bundle.yaml   (path doesn't matter for these tests)
          packs/
            foo/
              pack.yaml   (minimal — just needs to exist as a marker)
          empty_dir/      (no pack.yaml)
    """
    (tmp_path / "project").mkdir()
    (tmp_path / "project" / "bundle.yaml").write_text("# placeholder\n", encoding="utf-8")
    (tmp_path / "packs" / "foo").mkdir(parents=True)
    (tmp_path / "packs" / "foo" / "pack.yaml").write_text("id: foo\nversion: 1.0.0\n", encoding="utf-8")
    (tmp_path / "empty_dir").mkdir()
    return tmp_path


class TestResolveContentPackRoot:
    def test_relative_path_resolves_against_bundle_parent(
        self, tmp_project: pathlib.Path
    ) -> None:
        """`path: ../packs/foo` resolves relative to bundle.yaml's parent."""
        bundle_path = tmp_project / "project" / "bundle.yaml"
        spec = ContentPackSpec(name="foo", path=pathlib.Path("../packs/foo"))
        resolved = resolve_content_pack_root(bundle_path, spec)
        assert resolved == (tmp_project / "packs" / "foo").resolve()
        assert (resolved / "pack.yaml").exists()

    def test_absolute_path_used_as_is(self, tmp_project: pathlib.Path) -> None:
        """Absolute `path:` returned unchanged (after resolve())."""
        bundle_path = tmp_project / "project" / "bundle.yaml"
        abs_path = (tmp_project / "packs" / "foo").resolve()
        spec = ContentPackSpec(name="foo", path=abs_path)
        resolved = resolve_content_pack_root(bundle_path, spec)
        assert resolved == abs_path

    def test_relative_path_is_cwd_independent(
        self, tmp_project: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Relative resolution anchors to bundle parent, not cwd."""
        bundle_path = tmp_project / "project" / "bundle.yaml"
        spec = ContentPackSpec(name="foo", path=pathlib.Path("../packs/foo"))
        # Run once with cwd = /, once with cwd = tmp_path — same result.
        monkeypatch.chdir("/")
        resolved_a = resolve_content_pack_root(bundle_path, spec)
        monkeypatch.chdir(tmp_project)
        resolved_b = resolve_content_pack_root(bundle_path, spec)
        assert resolved_a == resolved_b
        assert resolved_a == (tmp_project / "packs" / "foo").resolve()

    def test_no_path_uses_installed_pack_lookup(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """`path: None` → looks under `<plugin>/content_packs/<name>/`."""
        # Build a fake installed-packs dir and patch INSTALLED_CONTENT_PACKS_DIR.
        installed_root = tmp_path / "fake_installed_packs"
        (installed_root / "fusion-finance-starter").mkdir(parents=True)
        (installed_root / "fusion-finance-starter" / "pack.yaml").write_text(
            "id: fusion-finance-starter\nversion: 1.0.0\n", encoding="utf-8"
        )

        from oracle_ai_data_platform_fusion_autopilot.commands import content_pack as cp_module
        monkeypatch.setattr(cp_module, "INSTALLED_CONTENT_PACKS_DIR", installed_root)

        # bundle_path can be anywhere — installed-pack lookup ignores it.
        bundle_path = tmp_path / "irrelevant" / "bundle.yaml"
        spec = ContentPackSpec(name="fusion-finance-starter")
        resolved = resolve_content_pack_root(bundle_path, spec)
        assert resolved == (installed_root / "fusion-finance-starter").resolve()

    def test_unknown_installed_pack_raises_1037(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
    ) -> None:
        """Missing installed pack → ContentPackRootNotFoundError carrying AIDPF-1037."""
        installed_root = tmp_path / "fake_installed_packs"
        installed_root.mkdir()  # empty

        from oracle_ai_data_platform_fusion_autopilot.commands import content_pack as cp_module
        monkeypatch.setattr(cp_module, "INSTALLED_CONTENT_PACKS_DIR", installed_root)

        spec = ContentPackSpec(name="does-not-exist")
        bundle_path = tmp_path / "irrelevant" / "bundle.yaml"
        with pytest.raises(ContentPackRootNotFoundError) as exc_info:
            resolve_content_pack_root(bundle_path, spec)
        assert AIDPF_1037_INSTALLED_PACK_NOT_FOUND in str(exc_info.value)
        assert "does-not-exist" in str(exc_info.value)

    def test_resolved_path_without_pack_yaml_raises_1038(
        self, tmp_project: pathlib.Path
    ) -> None:
        """Resolved root exists but no pack.yaml → ContentPackRootInvalidError (AIDPF-1038)."""
        bundle_path = tmp_project / "project" / "bundle.yaml"
        # Point at empty_dir which exists but has no pack.yaml.
        spec = ContentPackSpec(name="empty", path=pathlib.Path("../empty_dir"))
        with pytest.raises(ContentPackRootInvalidError) as exc_info:
            resolve_content_pack_root(bundle_path, spec)
        assert AIDPF_1038_RESOLVED_ROOT_NO_PACK_YAML in str(exc_info.value)

    def test_absolute_path_without_pack_yaml_also_raises_1038(
        self, tmp_project: pathlib.Path
    ) -> None:
        """1038 fires regardless of resolution shape (absolute path branch)."""
        bundle_path = tmp_project / "project" / "bundle.yaml"
        abs_empty = (tmp_project / "empty_dir").resolve()
        spec = ContentPackSpec(name="empty", path=abs_empty)
        with pytest.raises(ContentPackRootInvalidError):
            resolve_content_pack_root(bundle_path, spec)


# ---------------------------------------------------------------------------
# allowUnprovableCOA — StrictBool COA escape hatch (fail-fast-seed-validation)
# ---------------------------------------------------------------------------


class TestAllowUnprovableCoa:
    def test_omitted_defaults_false(self) -> None:
        """Omitted → False, no schema error (backward compatible)."""
        spec = ContentPackSpec(name="fusion-finance-starter")
        assert spec.allow_unprovable_coa is False

    @pytest.mark.parametrize("value", [True, False])
    def test_native_bool_accepted(self, value: bool) -> None:
        """Native true/false accepted, via both alias and attr name."""
        by_alias = ContentPackSpec(name="p", allowUnprovableCOA=value)
        by_attr = ContentPackSpec(name="p", allow_unprovable_coa=value)
        assert by_alias.allow_unprovable_coa is value
        assert by_attr.allow_unprovable_coa is value

    @pytest.mark.parametrize("value", [1, 0, "true", "false", "yes", "on"])
    def test_coercible_values_rejected(self, value: object) -> None:
        """StrictBool rejects every coercible value — no accidental enable of a
        correctness bypass."""
        with pytest.raises(ValidationError):
            ContentPackSpec(name="p", allowUnprovableCOA=value)

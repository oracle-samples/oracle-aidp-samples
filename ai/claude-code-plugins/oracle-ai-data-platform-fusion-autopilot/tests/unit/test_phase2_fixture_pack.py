"""Smoke tests for the Phase 2 fixture pack + project (Step 13).

The fixture pack lives at ``tests/fixtures/content_packs/phase2_test_pack/``
with a matching bundle + profile at ``tests/fixtures/projects/phase2_project/``.

These tests confirm:

* The fixture pack loads cleanly via load_pack / load_full_chain.
* The fixture bundle + profile validate end-to-end.
* resolve_content_pack_root with the fixture bundle resolves to the
  pack directory.
* stage_pack_files round-trips the fixture pack into materialise-able
  primitives (no path-traversal, no provenance mismatch).
* The fixture profile lives BESIDE the bundle, NOT inside the pack (per
  PLAN §9.5.7).

Heavy Spark-side integration tests live in tests/integration/ and run
under AIDP_FUSION_AUTOPILOT_RUN_SPARK_TESTS=1 (Step 13 plan note —
deferred to gated CI; pyspark optional).
"""

from __future__ import annotations

import pathlib

import yaml

from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import (
    load_full_chain,
    load_pack,
    make_filesystem_base_resolver,
)
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack_staging import (
    materialize_staged_pack,
    stage_pack_files,
)
from oracle_ai_data_platform_fusion_autopilot.schema.bundle import (
    Bundle,
    resolve_content_pack_root,
)
from oracle_ai_data_platform_fusion_autopilot.schema.tenant_profile import (
    load_tenant_profile,
    resolve_profile_path,
)


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURE_PACK = REPO_ROOT / "tests" / "fixtures" / "content_packs" / "phase2_test_pack"
FIXTURE_PROJECT = REPO_ROOT / "tests" / "fixtures" / "projects" / "phase2_project"
FIXTURE_BUNDLE = FIXTURE_PROJECT / "bundle.yaml"
FIXTURE_PROFILE = FIXTURE_PROJECT / "profiles" / "phase2-fixture.yaml"


def test_fixture_pack_exists_on_disk():
    """The fixtures committed by Step 13 are present."""
    assert FIXTURE_PACK.exists()
    assert (FIXTURE_PACK / "pack.yaml").exists()
    assert (FIXTURE_PACK / "silver" / "dim_thing.yaml").exists()
    assert (FIXTURE_PACK / "silver" / "dim_thing.sql").exists()


def test_fixture_pack_loads_via_load_pack():
    pack = load_pack(FIXTURE_PACK)
    assert pack.pack.id == "phase2-test-pack"
    assert pack.pack.version == "1.0.0"
    assert "dim_thing" in pack.silver


def test_fixture_pack_loads_via_load_full_chain():
    pack = load_full_chain(FIXTURE_PACK, base_resolver=make_filesystem_base_resolver(FIXTURE_PACK))
    assert pack.pack.id == "phase2-test-pack"


def test_fixture_bundle_validates():
    data = yaml.safe_load(FIXTURE_BUNDLE.read_text())
    bundle = Bundle.model_validate(data)
    assert bundle.content_pack is not None
    assert bundle.content_pack.name == "phase2-test-pack"
    assert bundle.content_pack.profile == "phase2-fixture"


def test_resolve_content_pack_root_returns_fixture_pack():
    data = yaml.safe_load(FIXTURE_BUNDLE.read_text())
    bundle = Bundle.model_validate(data)
    resolved = resolve_content_pack_root(FIXTURE_BUNDLE, bundle.content_pack)
    assert resolved == FIXTURE_PACK.resolve()


def test_fixture_profile_loads():
    profile = load_tenant_profile(FIXTURE_PROFILE)
    assert profile.tenant == "phase2-fixture-tenant"
    assert profile.bronze_schema_fingerprint == "sha256:phase2-fixture"


def test_resolve_profile_path_finds_fixture_profile():
    path = resolve_profile_path(FIXTURE_BUNDLE, "phase2-fixture")
    assert path == FIXTURE_PROFILE.resolve()
    assert path.exists()


def test_profile_lives_beside_bundle_not_inside_pack():
    """PLAN §9.5.7 — the profile MUST live next to bundle.yaml, never
    inside the pack directory."""
    assert FIXTURE_PROFILE.parent.parent == FIXTURE_BUNDLE.parent
    # NOT inside the pack.
    assert (FIXTURE_PACK / "profiles" / "phase2-fixture.yaml").exists() is False


def test_stage_pack_files_roundtrips():
    pack = load_pack(FIXTURE_PACK)
    files, manifest = stage_pack_files(pack)
    # Pack files staged + the SQL.
    assert "__layer_0__/pack.yaml" in files
    assert "__layer_0__/silver/dim_thing.yaml" in files
    assert "__layer_0__/silver/dim_thing.sql" in files
    # Manifest shape.
    assert manifest["entry_layer_index"] == 0

    # Round-trip via materialize.
    top_root, base_resolver = materialize_staged_pack(files, manifest)
    rebuilt = load_full_chain(top_root, base_resolver=base_resolver)
    assert rebuilt.pack.id == pack.pack.id
    assert set(rebuilt.silver) == set(pack.silver)

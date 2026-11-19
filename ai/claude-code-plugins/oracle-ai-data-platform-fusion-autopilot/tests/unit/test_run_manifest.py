"""Unit tests for the durable run-manifest + resume drift/mode logic (pure).

Feature: fail-fast-seed-validation. Covers mode resolution (Blocker 2), manifest
build/serialize/parse (AIDPF-4022), fingerprints, and the topology / node-def /
identity-profile / scope drift guards (AIDPF-1044/1047/1048/1049).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from oracle_ai_data_platform_fusion_autopilot.orchestrator import run_manifest as rm
from oracle_ai_data_platform_fusion_autopilot.orchestrator.content_pack import load_pack

PACK_ROOT = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "oracle_ai_data_platform_fusion_autopilot"
    / "content_packs"
    / "fusion-finance-starter"
)


# ---------------------------------------------------------------------------
# Mode resolution — Blocker 2
# ---------------------------------------------------------------------------


class TestResolveRunMode:
    def test_fresh_defaults_to_seed(self) -> None:
        assert rm.resolve_run_mode(None, is_resume=False) == "seed"

    def test_fresh_honours_explicit(self) -> None:
        assert rm.resolve_run_mode("incremental", is_resume=False) == "incremental"

    def test_manifest_adopted_when_omitted(self) -> None:
        assert (
            rm.resolve_run_mode(None, is_resume=True, manifest_mode="incremental")
            == "incremental"
        )

    def test_manifest_explicit_match_ok(self) -> None:
        assert (
            rm.resolve_run_mode("seed", is_resume=True, manifest_mode="seed") == "seed"
        )

    def test_manifest_explicit_conflict_1046(self) -> None:
        with pytest.raises(rm.ResumeModeConflictError):
            rm.resolve_run_mode("seed", is_resume=True, manifest_mode="incremental")

    def test_legacy_single_mode_adopted(self) -> None:
        assert (
            rm.resolve_run_mode(
                None, is_resume=True, historical_exec_modes=["incremental", "incremental"]
            )
            == "incremental"
        )

    def test_legacy_single_mode_explicit_conflict_1046(self) -> None:
        with pytest.raises(rm.ResumeModeConflictError):
            rm.resolve_run_mode(
                "seed", is_resume=True, historical_exec_modes=["incremental"]
            )

    @pytest.mark.parametrize("explicit", [None, "seed", "incremental"])
    def test_legacy_mixed_history_always_rejected(self, explicit) -> None:
        """The corruption case: mixed seed+incremental history is non-resumable
        REGARDLESS of an explicit --mode (an explicit mode is never an escape)."""
        with pytest.raises(rm.ResumeModeConflictError):
            rm.resolve_run_mode(
                explicit,
                is_resume=True,
                historical_exec_modes=["seed", "incremental", "seed"],
            )

    def test_legacy_audit_modes_excluded_from_mixture(self) -> None:
        """Audit modes (plan_hash_repin/fingerprint_skip) are NOT execution rows,
        so a single real mode + audit rows is not 'mixed'."""
        assert (
            rm.resolve_run_mode(
                None,
                is_resume=True,
                historical_exec_modes=["seed", "plan_hash_repin", "fingerprint_skip"],
            )
            == "seed"
        )

    def test_legacy_no_mode_requires_explicit(self) -> None:
        with pytest.raises(rm.ResumeModeConflictError):
            rm.resolve_run_mode(None, is_resume=True, historical_exec_modes=[])
        assert (
            rm.resolve_run_mode("seed", is_resume=True, historical_exec_modes=[])
            == "seed"
        )


# ---------------------------------------------------------------------------
# Manifest build / serialize / parse
# ---------------------------------------------------------------------------


def _manifest() -> dict:
    return rm.build_manifest(
        datasets=None,
        layers=["gold"],
        strict_scope=False,
        topology=[{"id": "a", "layer": "bronze", "deps": [], "sem": "s1"}],
        mode="seed",
        identity={"aidp.catalog": "c"},
        pack_fingerprint="pf",
        profile_hash="ph",
        allow_unprovable_coa=False,
        # v2 COA baseline (incremental-coa-chart-onboarding).
        coa_projection={"default": {}, "byChart": {}, "singletonAccepted": False},
        non_coa_semantic_hash="nch",
    )


class TestManifestRoundTrip:
    def test_build_has_all_required_fields(self) -> None:
        m = _manifest()
        for f in rm._REQUIRED_FIELDS_BY_VERSION[rm.MANIFEST_SCHEMA_VERSION]:
            assert f in m
        assert m["schemaVersion"] == rm.MANIFEST_SCHEMA_VERSION

    def test_round_trip(self) -> None:
        m = _manifest()
        assert rm.parse_manifest(rm.serialize_manifest(m)) == m

    @pytest.mark.parametrize("raw", [None, "", "{not json", "[]"])
    def test_malformed_fails_closed_4022(self, raw) -> None:
        with pytest.raises(rm.ManifestInvalidError):
            rm.parse_manifest(raw)

    @pytest.mark.parametrize(
        "policy",
        [
            {},  # allowUnprovableCOA missing
            {"allowUnprovableCOA": "false"},  # coercible string
            {"allowUnprovableCOA": 0},  # coercible int
            {"allowUnprovableCOA": 1},
            {"allowUnprovableCOA": None},
        ],
    )
    def test_exec_policy_non_native_bool_fails_closed_4022(self, policy) -> None:
        """Finding: allowUnprovableCOA must be present AND a native bool — a
        persisted 'false' must not be read as truthy and resume with the hatch
        enabled."""
        import json

        m = _manifest()
        m["exec_policy"] = policy
        with pytest.raises(rm.ManifestInvalidError):
            rm.parse_manifest(json.dumps(m))

    def test_exec_policy_native_bool_accepted(self) -> None:
        import json

        m = _manifest()
        m["exec_policy"] = {"allowUnprovableCOA": True}
        assert rm.parse_manifest(json.dumps(m))["exec_policy"] == {
            "allowUnprovableCOA": True
        }

    @pytest.mark.parametrize("bad_mode", ["garbage", "full", "SEED", "", "plan_hash_repin"])
    def test_invalid_mode_fails_closed_4022(self, bad_mode) -> None:
        """Finding 2: the parser must reject a mode outside EXECUTION_MODES, so a
        malformed value can never be adopted on resume and reach the destructive
        seed-overwrite vs incremental-merge branch."""
        import json

        m = _manifest()
        m["mode"] = bad_mode
        with pytest.raises(rm.ManifestInvalidError):
            rm.parse_manifest(json.dumps(m))

    def test_unknown_version_fails_closed_4022(self) -> None:
        import json

        m = _manifest()
        m["schemaVersion"] = 999
        with pytest.raises(rm.ManifestInvalidError):
            rm.parse_manifest(json.dumps(m))

    def test_missing_field_fails_closed_4022(self) -> None:
        import json

        m = _manifest()
        del m["topology"]
        with pytest.raises(rm.ManifestInvalidError):
            rm.parse_manifest(json.dumps(m))

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda m: m.update(resolver_inputs=[]),  # wrong type (list)
            lambda m: m["resolver_inputs"].update(strict_scope="false"),  # str bool
            lambda m: m["resolver_inputs"].update(datasets="gl_coa"),  # str not list
            lambda m: m.update(topology={}),  # not a list
            lambda m: m.update(topology=[{"id": "a"}]),  # entry missing fields
            lambda m: m.update(topology=[{"id": 1, "layer": "b", "deps": [], "sem": "s"}]),
            lambda m: m.update(mode=123),  # not a str
            lambda m: m.update(identity=[]),  # not an object
            lambda m: m.update(exec_policy="x"),  # not an object
            lambda m: m.update(pack_fingerprint=None),  # not a str
        ],
    )
    def test_malformed_nested_fields_fail_closed_4022(self, mutate) -> None:
        """Should-fix: nested type/shape violations fail closed with AIDPF-4022
        instead of passing parse then crashing on a later .get."""
        import json

        m = _manifest()
        mutate(m)
        with pytest.raises(rm.ManifestInvalidError):
            rm.parse_manifest(json.dumps(m))


# ---------------------------------------------------------------------------
# Fingerprints + topology (against the shipped pack)
# ---------------------------------------------------------------------------


_ACTIVE = "finance-default"  # the starter pack's active profile name


class TestFingerprints:
    def test_pack_fingerprint_stable(self) -> None:
        pack = load_pack(PACK_ROOT)
        fp1 = rm.compute_pack_fingerprint(pack, _ACTIVE)
        fp2 = rm.compute_pack_fingerprint(pack, _ACTIVE)
        assert fp1 == fp2 and len(fp1) == 64

    def test_pack_fingerprint_keys_calendar_by_active_profile(self) -> None:
        """Finding 4: the active pack-profile calendar MUST participate — keyed
        by the passed active-profile name (there is no pack.active_profile). A
        wrong/None key would hash the calendar as null and miss a mutation."""
        pack = load_pack(PACK_ROOT)
        # With the real active profile, the calendar is hashed; with None it is
        # null — so the two must differ (proves the calendar is included).
        assert rm.compute_pack_fingerprint(
            pack, _ACTIVE
        ) != rm.compute_pack_fingerprint(pack, None)

    def test_pack_fingerprint_changes_on_calendar_mutation(self) -> None:
        """Finding 4: mutating the active profile's calendar changes the pack
        fingerprint (→ AIDPF-1049 on resume)."""
        pack = load_pack(PACK_ROOT)
        before = rm.compute_pack_fingerprint(pack, _ACTIVE)
        prof = pack.pack.profiles[_ACTIVE]
        pack.pack.profiles[_ACTIVE] = prof.model_copy(update={"calendar": None})
        after = rm.compute_pack_fingerprint(pack, _ACTIVE)
        assert before != after

    def test_pack_fingerprint_changes_on_semantic_variant_edit(self) -> None:
        """Finding 5: a {{ semantic.* }} fragment edit (which a node's raw-SQL
        `sem` would miss) changes the pack fingerprint (→ AIDPF-1049)."""
        pack = load_pack(PACK_ROOT)
        before = rm.compute_pack_fingerprint(pack, _ACTIVE)
        # Drop the variant to simulate an edit to the referenced fragment set.
        pack.pack.semantic_variants = {}
        after = rm.compute_pack_fingerprint(pack, _ACTIVE)
        assert before != after

    def test_node_sem_changes_with_sql_bytes(self) -> None:
        pack = load_pack(PACK_ROOT)
        node = pack.silver["dim_account"]
        a = rm.compute_node_sem(node, sql_bytes=b"SELECT 1")
        b = rm.compute_node_sem(node, sql_bytes=b"SELECT 2")
        assert a != b

    def test_node_sem_changes_with_schema_override(self) -> None:
        pack = load_pack(PACK_ROOT)
        node = pack.silver["dim_account"]
        a = rm.compute_node_sem(node, sql_bytes=b"X", schema_override=None)
        b = rm.compute_node_sem(node, sql_bytes=b"X", schema_override="OTHER_TABLE")
        assert a != b

    def test_canonical_topology_sorted_and_edge_aware(self) -> None:
        pack = load_pack(PACK_ROOT)
        plan = list(pack.bronze.values()) + list(pack.silver.values())
        sem = {n.id: "x" for n in plan}
        topo = rm.canonical_topology(plan, sem_by_id=sem)
        assert [e["id"] for e in topo] == sorted(e["id"] for e in topo)


# ---------------------------------------------------------------------------
# Drift guards
# ---------------------------------------------------------------------------

_TOPO = [
    {"id": "gl_coa", "layer": "bronze", "deps": [], "sem": "s_glcoa"},
    {"id": "dim_account", "layer": "silver", "deps": ["gl_coa"], "sem": "s_dim"},
]


class TestDriftGuards:
    def test_topology_match_ok(self) -> None:
        rm.check_topology_drift(list(_TOPO), manifest_topology=_TOPO)

    def test_topology_edge_change_1044(self) -> None:
        drifted = [dict(e) for e in _TOPO]
        drifted[1] = {**drifted[1], "deps": []}  # dropped the gl_coa edge
        with pytest.raises(rm.ResumeTopologyDriftError):
            rm.check_topology_drift(drifted, manifest_topology=_TOPO)

    def test_topology_node_added_1044(self) -> None:
        drifted = _TOPO + [{"id": "z", "layer": "gold", "deps": [], "sem": "z"}]
        with pytest.raises(rm.ResumeTopologyDriftError):
            rm.check_topology_drift(drifted, manifest_topology=_TOPO)

    def test_node_def_sem_change_1049(self) -> None:
        drifted = [dict(e) for e in _TOPO]
        drifted[1] = {**drifted[1], "sem": "CHANGED"}
        with pytest.raises(rm.ResumeNodeDefinitionDriftError):
            rm.check_node_definition_drift(
                drifted, "pf", manifest_topology=_TOPO, manifest_pack_fingerprint="pf"
            )

    def test_pack_fingerprint_change_1049(self) -> None:
        with pytest.raises(rm.ResumeNodeDefinitionDriftError):
            rm.check_node_definition_drift(
                list(_TOPO), "NEW", manifest_topology=_TOPO,
                manifest_pack_fingerprint="OLD",
            )

    def test_node_def_match_ok(self) -> None:
        rm.check_node_definition_drift(
            list(_TOPO), "pf", manifest_topology=_TOPO, manifest_pack_fingerprint="pf"
        )

    def test_identity_drift_1048(self) -> None:
        m = _manifest()
        with pytest.raises(rm.ResumeIdentityProfileDriftError):
            rm.check_identity_profile_drift(
                current_identity={"aidp.catalog": "DIFFERENT"},
                current_profile_hash="ph",
                current_allow_unprovable_coa=False,
                manifest=m,
            )

    def test_profile_hash_drift_1048(self) -> None:
        m = _manifest()
        with pytest.raises(rm.ResumeIdentityProfileDriftError):
            rm.check_identity_profile_drift(
                current_identity={"aidp.catalog": "c"},
                current_profile_hash="CHANGED",
                current_allow_unprovable_coa=False,
                manifest=m,
            )

    def test_exec_policy_drift_1048(self) -> None:
        m = _manifest()
        with pytest.raises(rm.ResumeIdentityProfileDriftError):
            rm.check_identity_profile_drift(
                current_identity={"aidp.catalog": "c"},
                current_profile_hash="ph",
                current_allow_unprovable_coa=True,  # manifest had False
                manifest=m,
            )

    def test_identity_profile_match_ok(self) -> None:
        m = _manifest()
        rm.check_identity_profile_drift(
            current_identity={"aidp.catalog": "c"},
            current_profile_hash="ph",
            current_allow_unprovable_coa=False,
            manifest=m,
        )

    # --- COA-split (feature: incremental-coa-chart-onboarding) -------------

    def test_additive_coa_only_change_allowed(self) -> None:
        """Profile hash changed, non-COA hash UNCHANGED, verdict additive → allow."""
        m = _manifest()  # non_coa_semantic_hash="nch"
        rm.check_identity_profile_drift(
            current_identity={"aidp.catalog": "c"},
            current_profile_hash="CHANGED",
            current_allow_unprovable_coa=False,
            manifest=m,
            current_non_coa_semantic_hash="nch",  # matches manifest
            coa_verdict="additive",
        )

    def test_mutating_coa_only_change_1048(self) -> None:
        m = _manifest()
        with pytest.raises(rm.ResumeIdentityProfileDriftError):
            rm.check_identity_profile_drift(
                current_identity={"aidp.catalog": "c"},
                current_profile_hash="CHANGED",
                current_allow_unprovable_coa=False,
                manifest=m,
                current_non_coa_semantic_hash="nch",
                coa_verdict="mutating",
            )

    def test_non_coa_change_1048_even_if_verdict_additive(self) -> None:
        """Non-COA hash differs → 1048 regardless of the COA verdict."""
        m = _manifest()
        with pytest.raises(rm.ResumeIdentityProfileDriftError):
            rm.check_identity_profile_drift(
                current_identity={"aidp.catalog": "c"},
                current_profile_hash="CHANGED",
                current_allow_unprovable_coa=False,
                manifest=m,
                current_non_coa_semantic_hash="DIFFERENT",
                coa_verdict="additive",
            )

    def test_verdict_none_falls_back_to_conservative_1048(self) -> None:
        """Fail-closed: an unreadable protected set (verdict None) blocks."""
        m = _manifest()
        with pytest.raises(rm.ResumeIdentityProfileDriftError):
            rm.check_identity_profile_drift(
                current_identity={"aidp.catalog": "c"},
                current_profile_hash="CHANGED",
                current_allow_unprovable_coa=False,
                manifest=m,
                current_non_coa_semantic_hash="nch",
                coa_verdict=None,
            )


class TestScopeConflict:
    def test_omitted_filters_adopt_manifest(self) -> None:
        inputs = {"datasets": ["gl_coa"], "layers": None, "strict_scope": False}
        rm.check_scope_conflict(None, None, None, manifest_inputs=inputs)

    def test_exact_match_ok(self) -> None:
        inputs = {"datasets": ["gl_coa"], "layers": ["gold"], "strict_scope": True}
        rm.check_scope_conflict(["gl_coa"], ["gold"], True, manifest_inputs=inputs)

    def test_dataset_mismatch_1047(self) -> None:
        inputs = {"datasets": ["gl_coa"], "layers": None, "strict_scope": False}
        with pytest.raises(rm.ResumeScopeConflictError):
            rm.check_scope_conflict(["other"], None, None, manifest_inputs=inputs)

    def test_strict_scope_mismatch_1047(self) -> None:
        inputs = {"datasets": None, "layers": None, "strict_scope": False}
        with pytest.raises(rm.ResumeScopeConflictError):
            rm.check_scope_conflict(None, None, True, manifest_inputs=inputs)

"""Tests for dbx_export/cluster_shape_mapping.py — AIDP OCPU/memory normalization.

The expected values in these tests mirror the LIVE-VERIFIED limits captured on
2026-07-29 from the AIDP create-cluster API's own validation messages
(e.g. "Invalid driverShapeConfig.memoryInGBs. Valid values are: [32, 64, 128]."
for amd.generic at 4 OCPU). Re-derivation recipe: _meta.method in the table.
"""
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # for dbx_export

from dbx_export.cluster_shape_mapping import (  # noqa: E402
    ShapeCapacityError,
    ShapeMappingError,
    apply_learned_limits,
    load_shape_limits,
    normalize_shape_config,
    parse_limits_from_error,
    update_shape_limits,
)

# The committed, live-verified GLOBAL table (identical across all four
# probed regions — see _meta.verified_in_regions).
VERIFIED_TABLE = (Path(__file__).resolve().parent.parent
                  / "config" / "aidp_shape_limits.json")


@pytest.fixture(autouse=True)
def _limits_test_defaults(monkeypatch):
    """Deterministic limits context per test: no env var, and the default
    pointed at the committed verified table (tests override per-case)."""
    from dbx_export import cluster_shape_mapping as csm
    monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", VERIFIED_TABLE)
    monkeypatch.delenv("AIDP_SHAPE_LIMITS_PATH", raising=False)


# ---------------------------------------------------------------------------
# normalize_shape_config — discrete snap model
# ---------------------------------------------------------------------------

class TestNormalizeShapeConfig:
    def test_reported_failure_case_4_ocpu_16gb_raised_to_32gb(self):
        # The exact field case: 4 OCPU / 16 GB -> API says valid [32, 64, 128].
        result = normalize_shape_config(4, 16)
        assert result.ocpus == 4
        assert result.memory_gb == 32
        assert result.was_adjusted

    def test_valid_combos_unchanged(self):
        # All of these exist as real clusters on the verified instance.
        for oc, mem in [(1, 16), (2, 16), (2, 32), (4, 64), (8, 64), (16, 256)]:
            result = normalize_shape_config(oc, mem)
            assert (result.ocpus, result.memory_gb) == (oc, mem)
            assert not result.was_adjusted

    def test_memory_snaps_up_to_next_discrete_step(self):
        # 4 OCPU / 48 GB is INVALID (not just out of range) -> snap up to 64.
        result = normalize_shape_config(4, 48)
        assert result.memory_gb == 64

    def test_memory_above_max_raises(self):
        # 4 OCPU amd max is 128 GB. Clamping down would silently
        # under-provision (runtime OOM), so above-max must fail loudly.
        with pytest.raises(ShapeCapacityError, match="exceeds the maximum"):
            normalize_shape_config(4, 999)

    def test_1_ocpu_8gb_respects_global_floor(self):
        # API's list for 1 OCPU includes 8, but the >=16 global floor rejects it.
        result = normalize_shape_config(1, 8)
        assert result.memory_gb == 16

    def test_invalid_ocpu_count_snaps_up(self):
        # 3 and 6 are not valid OCPU counts -> snap to 4 and 8.
        assert normalize_shape_config(3, 32).ocpus == 4
        assert normalize_shape_config(6, 64).ocpus == 8

    def test_ocpus_above_shape_max_raises(self):
        with pytest.raises(ShapeCapacityError, match="exceeds the maximum"):
            normalize_shape_config(100, 512)                          # amd max 64
        with pytest.raises(ShapeCapacityError, match="exceeds the maximum"):
            normalize_shape_config(64, 512, shape="intel.generic")    # intel max 32

    def test_capacity_error_is_a_shape_mapping_error(self):
        # Callers that treat ShapeMappingError as "no usable table" must be
        # able to single out capacity errors — but generic handlers still work.
        assert issubclass(ShapeCapacityError, ShapeMappingError)

    def test_arm_has_no_1_ocpu(self):
        result = normalize_shape_config(1, 16, shape="arm.generic")
        assert result.ocpus == 2

    def test_arm_64_ocpu_above_512_memory_raises(self):
        # arm.generic 64 OCPU tops out at 512 GB (no 1024) — fail, not clamp.
        with pytest.raises(ShapeCapacityError, match="exceeds the maximum"):
            normalize_shape_config(64, 1024, shape="arm.generic")

    def test_unknown_shape_raises(self):
        with pytest.raises(ShapeMappingError):
            normalize_shape_config(2, 16, shape="does.not.exist")

    # NB: no GPU test on normalize_shape_config directly — the production GPU
    # policy lives in _shape_config (see TestGpuMinimumConfigPolicy below),
    # which forces the smallest gpu.a10 config regardless of what DBX asked.

    def test_describe_mentions_adjustment(self):
        assert "adjusted" in normalize_shape_config(4, 16).describe()
        assert "unchanged" in normalize_shape_config(2, 16).describe()


# ---------------------------------------------------------------------------
# Cluster-level settings
# ---------------------------------------------------------------------------

class TestConverterClusterLevelClamps:
    """Cluster-level bounds are enforced by the PRODUCTION path
    (convert_dbx_cluster's inline clamps), not by a separate helper."""

    @staticmethod
    def _convert(spec):
        from dbx_export.cluster_converter import convert_dbx_cluster
        return convert_dbx_cluster(spec, "t", prefix="ai_tool").aidp_payload

    def test_autoscale_ceiling_above_2000_clamped(self):
        # An autoscale MAX is an allowance, not a provision — capping it only
        # bounds elasticity (job degrades to slower, never breaks) -> clamp.
        p = self._convert({"node_type_id": "m5.xlarge",
                           "autoscale": {"min_workers": 2, "max_workers": 99999}})
        assert p["workerConfig"]["maxWorkerCount"] == 2000
        assert p["workerConfig"]["minWorkerCount"] == 2

    def test_fixed_size_above_2000_raises(self):
        # A fixed num_workers is a steady-state REQUEST — 2000 instead of
        # 3000 is a real capacity cut, so fail loudly instead of shrinking.
        with pytest.raises(ShapeCapacityError, match="fixed-size"):
            self._convert({"node_type_id": "m5.xlarge", "num_workers": 3000})

    def test_min_workers_above_2000_raises(self):
        # The autoscale MIN is a guaranteed floor (a provision): a valid DBX
        # range like [2500, 5000] cannot be satisfied under AIDP's 2000 cap.
        with pytest.raises(ShapeCapacityError, match="guaranteed floor"):
            self._convert({"node_type_id": "m5.xlarge",
                           "autoscale": {"min_workers": 2500,
                                         "max_workers": 5000}})

    def test_auto_termination_clamped_into_live_window(self):
        p = self._convert({"node_type_id": "m5.xlarge", "num_workers": 1,
                           "autotermination_minutes": 5})
        assert p["autoTerminationMinutes"] == 15
        p = self._convert({"node_type_id": "m5.xlarge", "num_workers": 1,
                           "autotermination_minutes": 100000})
        assert p["autoTerminationMinutes"] == 44640

    def test_dbx_zero_forces_aidp_minimum_15(self):
        # Policy (2026-07-31): DBX 0 ("never terminate") is NOT honored on
        # AIDP — always export always-on clusters is a cost-hygiene risk.
        # Force the AIDP minimum instead so operators explicitly raise it
        # in AIDP if a specific export truly needs longer sessions.
        p = self._convert({"node_type_id": "m5.xlarge", "num_workers": 1,
                           "autotermination_minutes": 0})
        assert p["autoTerminationMinutes"] == 15

    def test_missing_autotermination_forces_aidp_minimum_15(self):
        # Same policy for the "field not present" case — never leave the
        # AIDP cluster always-on because DBX left the field off.
        p = self._convert({"node_type_id": "m5.xlarge", "num_workers": 1})
        assert p["autoTerminationMinutes"] == 15

    def test_min_workers_above_max_raises_as_invalid_input(self):
        # Only reachable from a corrupt spec (DBX rejects min > max at
        # creation) — treat it as invalid input, never rewrite it downward.
        with pytest.raises(ShapeCapacityError, match="invalid autoscale range"):
            self._convert({"node_type_id": "m5.xlarge",
                           "autoscale": {"min_workers": 200,
                                         "max_workers": 100}})


class TestGpuMinimumConfigPolicy:
    """GPU is expensive — the exporter defaults every DBX GPU cluster to the
    SMALLEST valid gpu.a10 config (1 GPU / 15 OCPU / 240 GB) regardless of
    what DBX asked, with a log line telling the operator what DBX wanted so
    they can scale up deliberately in AIDP if truly needed.

    Rationale: AWS/Azure/GCP GPU families (T4, V100, A100, K80) differ
    substantially in per-GPU compute/memory profile from AIDP's only GA
    GPU shape (A10). Silent "match DBX capacity" risks bringing up an
    oversized very-expensive cluster the customer didn't intend.
    """

    @staticmethod
    def _convert(spec):
        from dbx_export.cluster_converter import convert_dbx_cluster
        return convert_dbx_cluster(spec, "gpu-test", prefix="ai_tool")

    def test_gpu_cluster_forced_to_minimum_config(self):
        # g4dn.12xlarge is 48 vCPU / 192 GB / 4 T4 GPUs — a big DBX GPU node.
        # After conversion, driver+worker MUST land on the AIDP minimum (1 GPU)
        # regardless of DBX's 4-GPU ask.
        conv = self._convert({
            "node_type_id": "g4dn.12xlarge",
            "num_workers": 2,
        })
        p = conv.aidp_payload
        for role_key in ("driverConfig", "workerConfig"):
            role = p[role_key]
            cfg = role.get("driverShapeConfig") if role_key == "driverConfig" \
                else role.get("workerShapeConfig")
            assert cfg["gpus"] == 1, f"{role_key} should have 1 GPU (minimum)"

    def test_gpu_shape_config_is_gpus_only(self):
        # REGRESSION (2026-08-03): gpu.a10 create REJECTS a driverShapeConfig
        # that pins ocpus/memoryInGBs ("gpus should be 1 or 2", misleading) —
        # it is a fixed bundle and AIDP fills OCPU/memory from the gpu count.
        # The payload must therefore send gpus ONLY for GPU shapes.
        conv = self._convert({"node_type_id": "g4dn.12xlarge", "num_workers": 1})
        p = conv.aidp_payload
        for role_key, cfg_key in (("driverConfig", "driverShapeConfig"),
                                  ("workerConfig", "workerShapeConfig")):
            cfg = p[role_key][cfg_key]
            assert cfg == {"gpus": 1}, f"{cfg_key} must be gpus-only, got {cfg}"
            assert "ocpus" not in cfg and "memoryInGBs" not in cfg

    def test_cpu_shape_config_still_has_ocpus_and_memory(self):
        # Guard the inverse: CPU shapes MUST keep ocpus/memoryInGBs (AIDP
        # requires them there) — the GPU gpus-only change must not leak to CPU.
        conv = self._convert({"node_type_id": "m5.xlarge", "num_workers": 1})
        cfg = conv.aidp_payload["driverConfig"]["driverShapeConfig"]
        assert "ocpus" in cfg and "memoryInGBs" in cfg and cfg.get("gpus") == 0

    def test_gpu_log_line_mentions_dbx_ask_and_minimum(self):
        # The log line must include what DBX asked (so the operator sees the
        # gap) and that it's defaulting to the minimum GPU config.
        conv = self._convert({
            "node_type_id": "g4dn.12xlarge",
            "num_workers": 1,
        })
        log = "\n".join(conv.shape_mapping_log_lines)
        assert "GPU shape — mapped to gpu.a10 with 1 GPU" in log
        assert "DBX asked for" in log
        assert "adjust in AIDP if higher capacity is needed" in log

    def test_gpu_log_line_present_for_both_driver_and_worker(self):
        # One log line per role — clear provenance for the operator.
        conv = self._convert({
            "node_type_id": "g4dn.12xlarge",
            "num_workers": 1,
        })
        log_text = "\n".join(conv.shape_mapping_log_lines)
        assert log_text.count("GPU shape — mapped to gpu.a10 with 1 GPU") == 2


# ---------------------------------------------------------------------------
# Databricks node-type conversion — through the REAL exporter path
# (convert_dbx_cluster -> resolve_shape -> _shape_config normalization).
# node_type_catalog.DBX_NODE_CATALOG is the single node-type source of truth.
# ---------------------------------------------------------------------------

def _convert_worker(node_type_id):
    from dbx_export.cluster_converter import convert_dbx_cluster
    conv = convert_dbx_cluster(
        {"node_type_id": node_type_id, "num_workers": 1}, "t", prefix="ai_tool")
    wc = conv.aidp_payload["workerConfig"]
    return wc["workerShape"], wc["workerShapeConfig"]


class TestDatabricksConversion:
    def test_shape_override_keeps_source_arch_truthful(self):
        # c6i.24xlarge IS Intel silicon; only the TARGET shape is overridden
        # to amd.generic (48 OCPUs exceed intel.generic's 32-OCPU ceiling).
        # The resolution must record both facts separately so the mapping
        # log never claims the DBX machine was AMD.
        from dbx_export.node_type_catalog import resolve_shape
        res = resolve_shape("c6i.24xlarge")
        assert res.dbx_arch == "Intel"            # source fact
        assert res.aidp_shape == "amd.generic"    # target decision
        assert res.ocpus == 48                    # ceil(96 vCPU / 2)
        assert "overridden" in res.note
        # And through the real converter: snaps onto a valid AMD combo.
        shape, cfg = _convert_worker("c6i.24xlarge")
        assert shape == "amd.generic"
        assert (cfg["ocpus"], cfg["memoryInGBs"]) == (64, 256)

    def test_i3_xlarge_maps_to_valid_shape(self):
        # i3.xlarge = Intel 4 vCPU / 30.5 GB -> 2 OCPU, memory snapped to 32.
        shape, cfg = _convert_worker("i3.xlarge")
        assert shape == "intel.generic"
        assert cfg["ocpus"] == 2
        assert cfg["memoryInGBs"] == 32

    def test_c5_2xlarge_previously_fell_to_tiny_fallback(self):
        # The reported divergence case: c5.2xlarge = Intel 8 vCPU / 16 GB.
        # Must resolve via the catalog (4 OCPU) with memory snapped to the
        # 4-OCPU floor (32) — NOT the unknown-type fallback (1 OCPU / 16 GB).
        shape, cfg = _convert_worker("c5.2xlarge")
        assert shape == "intel.generic"
        assert cfg["ocpus"] == 4
        assert cfg["memoryInGBs"] == 32

    def test_m5_xlarge_valid_without_adjustment(self):
        # m5.xlarge = Intel 4 vCPU / 16 GB -> 2 OCPU / 16 GB, already valid.
        shape, cfg = _convert_worker("m5.xlarge")
        assert shape == "intel.generic"
        assert cfg["ocpus"] == 2
        assert cfg["memoryInGBs"] == 16

    def test_arm_type_keeps_vcpu_count(self):
        # ARM (Graviton) vCPUs are physical cores: OCPU = vCPU (1:1).
        shape, cfg = _convert_worker("r6g.xlarge")  # ARM 4 vCPU / 32 GB
        assert shape == "arm.generic"
        assert cfg["ocpus"] == 4
        assert cfg["memoryInGBs"] == 32

    def test_no_second_node_type_catalog_exists(self):
        # Regression guard for the two-catalogs drift bug: the validity
        # module must not grow its own node-type table again.
        from dbx_export import cluster_shape_mapping as csm
        assert not hasattr(csm, "AWS_NODE_TYPES")
        assert not hasattr(csm, "from_databricks_spec")


# ---------------------------------------------------------------------------
# Driver/worker shape-family harmonization
#
# AIDP rejects a create whose driver and worker use different shape names:
#   "Invalid driver shape amd.generic and executor Shape intel.generic.
#    Both need to have a same shape name"
# Regression from the pilot deployment export of DBX job 700000000000005 (2026-08-03):
# driver c6i.24xlarge (48 OCPU) is overridden to amd.generic while the small
# worker c6i.2xlarge stays intel.generic → mismatched pair → HTTP 400.
# ---------------------------------------------------------------------------

class TestDriverWorkerShapeHarmonization:
    def _convert(self, spec):
        from dbx_export.cluster_converter import convert_dbx_cluster
        return convert_dbx_cluster(spec, "t", prefix="ai_tool")

    def test_examplecorp_job_700000000000005_mismatch_is_harmonized(self):
        # The exact reported spec: big Intel driver + small Intel worker.
        conv = self._convert({
            "driver_node_type_id": "c6i.24xlarge",   # -> amd.generic (override)
            "node_type_id": "c6i.2xlarge",           # -> intel.generic
            "autoscale": {"min_workers": 1, "max_workers": 3},
        })
        payload = conv.aidp_payload
        dc = payload["driverConfig"]
        wc = payload["workerConfig"]
        # Both roles now share ONE shape name — AIDP's requirement.
        assert dc["driverShape"] == wc["workerShape"] == "amd.generic"
        assert dc["driverNodeType"] == "amd.generic"
        assert payload["nodeType"] == "amd.generic"
        # Capacity preserved / snapped up on the common shape (never downsized).
        assert (dc["driverShapeConfig"]["ocpus"],
                dc["driverShapeConfig"]["memoryInGBs"]) == (64, 256)
        assert (wc["workerShapeConfig"]["ocpus"],
                wc["workerShapeConfig"]["memoryInGBs"]) == (4, 32)
        # The change is surfaced, not silent.
        assert any("shape-harmonize" in ln for ln in conv.shape_mapping_log_lines)

    def test_matching_x86_shapes_not_touched(self):
        # Both Intel and both within intel.generic's ceiling → stays intel.
        conv = self._convert({
            "driver_node_type_id": "m5.4xlarge",   # Intel 16 vCPU -> 8 OCPU
            "node_type_id": "m5.xlarge",           # Intel 4 vCPU -> 2 OCPU
            "num_workers": 2,
        })
        dc = conv.aidp_payload["driverConfig"]
        wc = conv.aidp_payload["workerConfig"]
        assert dc["driverShape"] == wc["workerShape"] == "intel.generic"
        assert not any("shape-harmonize" in ln
                       for ln in conv.shape_mapping_log_lines)

    def test_helper_noop_when_shapes_already_equal(self):
        from dbx_export.cluster_converter import harmonize_shape_family
        from dbx_export.node_type_catalog import resolve_shape
        d = resolve_shape("m5.4xlarge")
        w = resolve_shape("m5.xlarge")
        nd, nw, lines = harmonize_shape_family(d, w)
        assert nd.aidp_shape == nw.aidp_shape == "intel.generic"
        assert lines == []

    def test_helper_coerces_x86_mismatch_to_amd(self):
        from dbx_export.cluster_converter import harmonize_shape_family
        from dbx_export.node_type_catalog import resolve_shape
        d = resolve_shape("c6i.24xlarge")   # amd.generic (override)
        w = resolve_shape("m5.xlarge")      # intel.generic
        nd, nw, lines = harmonize_shape_family(d, w)
        assert nd.aidp_shape == nw.aidp_shape == "amd.generic"
        # Source arch stays truthful even after retargeting the shape.
        assert nw.dbx_arch == "Intel"
        assert "harmonized" in nw.note
        assert len(lines) == 1 and "shape-harmonize" in lines[0]

    def test_helper_raises_on_cross_family_mismatch(self):
        # ARM driver + Intel worker cannot be reconciled without changing the
        # instruction set — fail loudly rather than silently swap arch.
        from dbx_export.cluster_converter import harmonize_shape_family
        from dbx_export.node_type_catalog import resolve_shape
        d = resolve_shape("r6g.4xlarge")    # arm.generic
        w = resolve_shape("m5.xlarge")      # intel.generic
        with pytest.raises(ShapeCapacityError, match="different families"):
            harmonize_shape_family(d, w)


# ---------------------------------------------------------------------------
# Limits load/save round trip
# ---------------------------------------------------------------------------

class TestLimitsConfig:
    def test_repo_config_is_the_source_of_truth(self):
        # config/aidp_shape_limits.json must exist and carry the verified table.
        limits = load_shape_limits()
        assert limits["amd.generic"]["memory_by_ocpu"]["4"] == [32, 64, 128]

    def test_metadata_keys_filtered_out(self, tmp_path):
        cfg = tmp_path / "limits.json"
        cfg.write_text(json.dumps(
            {"_meta": {"verified": "x"},
             "amd.generic": {"ocpus_valid": [1, 2, 4]}}))
        limits = load_shape_limits(cfg)
        assert "_meta" not in limits
        assert limits["amd.generic"]["ocpus_valid"] == [1, 2, 4]

    def test_missing_file_raises_with_guidance(self, tmp_path):
        with pytest.raises(ShapeMappingError, match="Restore the committed"):
            load_shape_limits(tmp_path / "missing.json")

    def test_missing_file_optional_returns_empty(self, tmp_path):
        assert load_shape_limits(tmp_path / "missing.json", required=False) == {}

    def test_corrupt_file_raises(self, tmp_path):
        cfg = tmp_path / "limits.json"
        cfg.write_text("{not json")
        with pytest.raises(ShapeMappingError):
            load_shape_limits(cfg)

    def test_corrupt_file_raises_even_when_not_required(self, tmp_path):
        # A corrupt file must NEVER silently become an empty table — a later
        # save would clobber recoverable data.
        cfg = tmp_path / "limits.json"
        cfg.write_text("{not json")
        with pytest.raises(ShapeMappingError, match="CORRUPT"):
            load_shape_limits(cfg, required=False)

    def test_env_var_overrides_default_path(self, tmp_path, monkeypatch):
        cfg = tmp_path / "region.json"
        cfg.write_text(json.dumps({"amd.generic": {"ocpus_valid": [1]}}))
        monkeypatch.setenv("AIDP_SHAPE_LIMITS_PATH", str(cfg))
        assert load_shape_limits()["amd.generic"]["ocpus_valid"] == [1]

    def test_update_transaction_preserves_concurrent_writes(self, tmp_path):
        from dbx_export.cluster_shape_mapping import update_shape_limits
        cfg = tmp_path / "limits.json"
        # State on disk includes shape C written by "another process" after
        # this process last looked at the file — the transaction re-reads
        # under the lock, so C must survive.
        cfg.write_text(json.dumps({
            "_meta": {"verified": "x"},
            "shapeA.generic": {"ocpus_valid": [1]},
            "shapeC.generic": {"ocpus_valid": [4]},
        }))
        update_shape_limits(
            lambda limits: limits.update(
                {"shapeB.generic": {"ocpus_valid": [2]}}),
            cfg)
        raw = json.loads(cfg.read_text())
        assert set(raw) == {"_meta", "shapeA.generic", "shapeB.generic",
                            "shapeC.generic"}
        assert raw["_meta"]["verified"] == "x"

    def test_update_transaction_refuses_corrupt_file(self, tmp_path):
        from dbx_export.cluster_shape_mapping import update_shape_limits
        cfg = tmp_path / "limits.json"
        cfg.write_text("{not json")
        with pytest.raises(ShapeMappingError, match="CORRUPT"):
            update_shape_limits(lambda limits: None, cfg)
        assert cfg.read_text() == "{not json"  # untouched, not clobbered

    def test_update_preserves_metadata(self, tmp_path):
        cfg = tmp_path / "limits.json"
        cfg.write_text(json.dumps({"_meta": {"verified": "2026-07-29"},
                                   "amd.generic": {"ocpus_valid": [1]}}))
        update_shape_limits(
            lambda limits: limits.update({"amd.generic": {"ocpus_valid": [1, 2]}}),
            cfg)
        raw = json.loads(cfg.read_text())
        assert raw["_meta"]["verified"] == "2026-07-29"
        assert raw["amd.generic"]["ocpus_valid"] == [1, 2]

    def test_update_then_load_round_trip(self, tmp_path):
        cfg = tmp_path / "sub" / "limits.json"
        update_shape_limits(
            lambda limits: limits.update(
                {"amd.generic": {"ocpus_valid": [2], "memory_by_ocpu": {"2": [32]}}}),
            cfg)
        assert load_shape_limits(cfg)["amd.generic"]["ocpus_valid"] == [2]

    def test_normalize_honors_injected_limits(self):
        limits = {"amd.generic": {
            "ocpus_valid": [2], "memory_by_ocpu": {"2": [64]}}}
        result = normalize_shape_config(2, 16, limits=limits)
        assert result.memory_gb == 64

    def test_missing_memory_list_raises(self):
        limits = {"amd.generic": {"ocpus_valid": [2], "memory_by_ocpu": {}}}
        with pytest.raises(ShapeMappingError, match="no memory list"):
            normalize_shape_config(2, 16, limits=limits)


# ---------------------------------------------------------------------------
# 400-message parsing (self-healing) — real captured messages
# ---------------------------------------------------------------------------

class TestParseLimitsFromError:
    def test_real_memory_valid_values_message(self):
        out = parse_limits_from_error(
            '{ "code" : "InvalidParameter", "message" : '
            '"Invalid driverShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128]." }',
            ocpus=4)
        assert out["memory_valid_values"] == [32, 64, 128]

    def test_real_ocpus_valid_values_message(self):
        out = parse_limits_from_error(
            "Invalid driverShapeConfig.ocpus.  Valid values are: [1, 2, 4, 8, 16, 32, 64].")
        assert out["ocpus_valid"] == [1, 2, 4, 8, 16, 32, 64]

    def test_executor_variant_message(self):
        out = parse_limits_from_error(
            "Invalid executorShapeConfig.memoryInGBs.  Valid values are: [16, 32, 64].")
        assert out["memory_valid_values"] == [16, 32, 64]

    def test_global_bounds_messages(self):
        out = parse_limits_from_error(
            "driverConfig.driverShapeConfig.memoryInGBs must be greater than or equal to 16; "
            "workerConfig.workerShapeConfig.memoryInGBs must be less than or equal to 2000")
        assert out["memory_gb_min"] == 16
        assert out["memory_gb_max"] == 2000

    def test_unrelated_error_returns_empty(self):
        assert parse_limits_from_error("NotAuthorizedOrNotFound") == {}

    def test_apply_learned_limits_updates_table(self):
        limits = {"amd.generic": {"ocpus_valid": [1, 2],
                                  "memory_by_ocpu": {"2": [16]}}}
        changed = apply_learned_limits(
            limits, "amd.generic", 2, {"memory_valid_values": [16, 32, 64]})
        assert changed
        assert limits["amd.generic"]["memory_by_ocpu"]["2"] == [16, 32, 64]
        # idempotent second apply
        assert not apply_learned_limits(
            limits, "amd.generic", 2, {"memory_valid_values": [16, 32, 64]})


# ---------------------------------------------------------------------------
# Partial tables (built incrementally by the self-heal path)
# ---------------------------------------------------------------------------

class TestPartialTables:
    def test_memory_only_entry_keeps_requested_ocpus(self):
        # A self-heal 400 teaches one memory list but no OCPU list — the
        # requested OCPU count must be kept and only memory snapped.
        limits = {"gpu.a10": {"memory_by_ocpu": {"8": [64, 128]}}}
        result = normalize_shape_config(8, 100, shape="gpu.a10", limits=limits)
        assert result.ocpus == 8
        assert result.memory_gb == 128

    def test_unlearned_ocpu_count_still_raises(self):
        limits = {"gpu.a10": {"memory_by_ocpu": {"8": [64, 128]}}}
        with pytest.raises(ShapeMappingError, match="no memory list"):
            normalize_shape_config(16, 128, shape="gpu.a10", limits=limits)


# ---------------------------------------------------------------------------
# dbx_export create-payload self-heal
# ---------------------------------------------------------------------------

class TestLearnLimitsAndRenormalize:
    @staticmethod
    def _payload(shape="amd.generic", ocpus=4, mem=16):
        cfg = {"ocpus": ocpus, "gpus": 0, "memoryInGBs": mem}
        return {
            "displayName": "t",
            "driverConfig": {"driverShape": shape, "driverShapeConfig": dict(cfg)},
            "workerConfig": {"workerShape": shape, "workerShapeConfig": dict(cfg)},
        }

    def test_learns_from_400_and_renormalizes_both_roles(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")

        payload = self._payload(ocpus=4, mem=16)
        lines = learn_limits_and_renormalize(
            payload,
            'Invalid executorShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128].')
        assert lines  # something was learned
        assert payload["driverConfig"]["driverShapeConfig"]["memoryInGBs"] == 32
        assert payload["workerConfig"]["workerShapeConfig"]["memoryInGBs"] == 32
        # and the learned list was persisted for future clusters
        saved = json.loads((tmp_path / "limits.json").read_text())
        assert saved["amd.generic"]["memory_by_ocpu"]["4"] == [32, 64, 128]

    def test_unparseable_error_returns_empty(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        payload = self._payload()
        assert learn_limits_and_renormalize(payload, "NotAuthorizedOrNotFound") == []
        # payload untouched
        assert payload["workerConfig"]["workerShapeConfig"]["memoryInGBs"] == 16

    def test_ocpus_only_error_snaps_ocpus_without_memory_table(self, tmp_path, monkeypatch):
        # gpu.a10 has no memory table; AIDP validates OCPUs first, so the 400
        # carries ONLY the valid OCPU list. The heal must still mutate the
        # payload (snap OCPUs) — not signal a retry of an identical body.
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        payload = self._payload(shape="gpu.a10", ocpus=4, mem=64)
        err = "Invalid driverShapeConfig.ocpus.  Valid values are: [15, 30]."

        lines = learn_limits_and_renormalize(payload, err)
        assert lines  # retry justified...
        assert payload["driverConfig"]["driverShapeConfig"]["ocpus"] == 15  # ...because it mutated
        assert payload["workerConfig"]["workerShapeConfig"]["ocpus"] == 15
        # OCPU list persisted for future runs
        saved = json.loads((tmp_path / "limits.json").read_text())
        assert saved["gpu.a10"]["ocpus_valid"] == [15, 30]

        # Same error again on the already-snapped payload: nothing new to
        # learn, nothing mutates -> no retry (caller re-raises).
        assert learn_limits_and_renormalize(payload, err) == []
        assert payload["workerConfig"]["workerShapeConfig"]["ocpus"] == 15

    def test_bound_only_error_clamps_memory(self, tmp_path, monkeypatch):
        # ">= 16" style errors carry no list; the parsed bound must still be
        # applied to the payload so the retry is meaningful.
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        payload = self._payload(shape="gpu.a10", ocpus=15, mem=1)
        err = ("driverConfig.driverShapeConfig.memoryInGBs must be greater "
               "than or equal to 16")
        lines = learn_limits_and_renormalize(payload, err)
        assert lines
        assert payload["driverConfig"]["driverShapeConfig"]["memoryInGBs"] == 16
        assert payload["workerConfig"]["workerShapeConfig"]["memoryInGBs"] == 16

    def test_learning_without_mutation_does_not_retry(self, tmp_path, monkeypatch):
        # A new-to-the-file list whose values the payload already satisfies:
        # persist it, but an identical payload must NOT be retried.
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        payload = self._payload(shape="gpu.a10", ocpus=15, mem=64)
        err = "Invalid driverShapeConfig.ocpus.  Valid values are: [15, 30]."
        assert learn_limits_and_renormalize(payload, err) == []
        # ...even though the list WAS persisted for future clusters
        saved = json.loads((tmp_path / "limits.json").read_text())
        assert saved["gpu.a10"]["ocpus_valid"] == [15, 30]

    def test_memory_list_not_applied_to_role_with_different_ocpus(self, tmp_path, monkeypatch):
        # The memory list in a 400 is specific to the OCPU count it was about.
        # A driver with a different OCPU count must not blindly inherit it.
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        payload = self._payload(shape="gpu.a10", ocpus=30, mem=48)
        payload["workerConfig"]["workerShapeConfig"] = {
            "ocpus": 15, "gpus": 1, "memoryInGBs": 48}
        err = ("Invalid executorShapeConfig.memoryInGBs.  "
               "Valid values are: [64, 128].")  # learned for the worker's 15 OCPU
        lines = learn_limits_and_renormalize(payload, err)
        assert lines
        assert payload["workerConfig"]["workerShapeConfig"]["memoryInGBs"] == 64
        # driver is at 30 OCPU — the 15-OCPU list must not touch it
        assert payload["driverConfig"]["driverShapeConfig"]["memoryInGBs"] == 48


def _import_with_sdk_stubs(module_name, stubs):
    """Import a module with temporary stubs for heavy SDK imports that the
    logic under test never calls (oci/websocket/requests are only used at
    runtime). Stubs are removed from sys.modules afterwards so other test
    files can still import the real packages where they exist."""
    import importlib
    import types
    stubbed = []
    for mod in stubs:
        if mod not in sys.modules:
            sys.modules[mod] = types.ModuleType(mod)
            stubbed.append(mod)
    try:
        return importlib.import_module(module_name)
    finally:
        for mod in stubbed:
            sys.modules.pop(mod, None)


# ---------------------------------------------------------------------------
# Region derivation + limits-path resolution + table validation
# ---------------------------------------------------------------------------

class TestRegionDerivation:
    def test_full_region_ocid(self):
        from dbx_export.auth import region_from_ocid
        assert region_from_ocid(
            "ocid1.datalake.oc1.ap-mumbai-1.aaaa") == "ap-mumbai-1"

    def test_legacy_airport_code_ocid(self):
        from dbx_export.auth import region_from_ocid
        assert region_from_ocid("ocid1.datalake.oc1.iad.aaaa") == "us-ashburn-1"
        assert region_from_ocid("ocid1.datalake.oc1.bom.aaaa") == "ap-mumbai-1"

    def test_region_maps_never_drift(self):
        # Two REGION_MAP copies exist (dbx_export/auth.py for the exporter,
        # scripts/aidp_executor.py for the migrator). Both resolve lake OCIDs
        # to the AIDP API endpoint: if they disagree, the two tools talk to
        # different control planes for the same lake. This tripwire keeps
        # them identical.
        from dbx_export.auth import REGION_MAP as exporter_map
        aidp_executor = _import_with_sdk_stubs("aidp_executor",
                                               ("oci", "websocket"))
        assert exporter_map == aidp_executor.REGION_MAP

    def test_migrator_and_exporter_derive_same_region(self):
        from dbx_export.auth import region_from_ocid
        aidp_executor = _import_with_sdk_stubs("aidp_executor",
                                               ("oci", "websocket"))
        for ocid in ("ocid1.datalake.oc1.hyd.x",       # legacy code
                     "ocid1.datalake.oc1.ap-mumbai-1.x"):  # full name
            assert region_from_ocid(ocid) == \
                aidp_executor.get_region_from_ocid(ocid)

    def test_malformed_ocid_raises_in_both_helpers(self):
        # The region keys the persistent limits store — a silent default
        # region (the old "ap-mumbai-1" fallback) could label learned or
        # probed limits with the WRONG region. Both helpers must fail loud.
        from dbx_export.auth import region_from_ocid
        aidp_executor = _import_with_sdk_stubs("aidp_executor",
                                               ("oci", "websocket"))
        for bad in ("ocid1.datalake.oc1", "", "garbage", "a.b.c."):
            with pytest.raises(ValueError, match="malformed lake OCID"):
                region_from_ocid(bad)
            with pytest.raises(ValueError, match="malformed lake OCID"):
                aidp_executor.get_region_from_ocid(bad)


class TestLimitsPathResolution:
    def test_precedence_arg_env_default(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH",
                            tmp_path / "aidp_shape_limits.json")
        # 3rd: the committed global default
        assert csm._limits_path().name == "aidp_shape_limits.json"
        # 2nd: env var beats the default
        monkeypatch.setenv("AIDP_SHAPE_LIMITS_PATH", str(tmp_path / "env.json"))
        assert csm._limits_path().name == "env.json"
        # 1st: explicit arg beats env
        assert csm._limits_path(tmp_path / "arg.json").name == "arg.json"

    def test_committed_table_is_global_with_region_audit(self):
        # The committed store is ONE global file; provenance is recorded as
        # the list of regions the identical limits were probed in.
        raw = json.loads(VERIFIED_TABLE.read_text())
        assert VERIFIED_TABLE.name == "aidp_shape_limits.json"
        assert set(raw["_meta"]["verified_in_regions"]) == {
            "us-ashburn-1", "us-phoenix-1", "ap-mumbai-1", "ap-tokyo-1"}
        assert raw["_meta"]["verification"]["amd.generic"]["status"] == "verified"


class TestCheckLimitsTable:
    def _write(self, tmp_path, monkeypatch, content):
        from dbx_export import cluster_shape_mapping as csm
        table = tmp_path / "aidp_shape_limits.json"
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", table)
        if content is not None:
            table.write_text(content)
        return csm, table

    def test_usable(self, tmp_path, monkeypatch):
        csm, _ = self._write(tmp_path, monkeypatch, json.dumps(
            {"amd.generic": {"ocpus_valid": [1]}}))
        assert csm.check_limits_table()["status"] == "usable"

    def test_missing(self, tmp_path, monkeypatch):
        csm, _ = self._write(tmp_path, monkeypatch, None)
        assert csm.check_limits_table()["status"] == "missing"

    def test_empty_file_counts_as_missing(self, tmp_path, monkeypatch):
        # Present but without shape entries: same operator action as absent
        # (lazy learning), so same status.
        csm, _ = self._write(tmp_path, monkeypatch, json.dumps(
            {"_meta": {"verified_in_regions": ["us-ashburn-1"]}}))
        assert csm.check_limits_table()["status"] == "missing"

    def test_broken_never_overwritten(self, tmp_path, monkeypatch):
        csm, table = self._write(tmp_path, monkeypatch, "{not json")
        assert csm.check_limits_table()["status"] == "broken"
        assert table.read_text() == "{not json"


class TestPhase3CapacityRejectionHandling:
    """BOTH phase-3 catch sites (job_clusters loop AND existing_cluster_id
    loop) must convert ShapeCapacityError into a graceful FAILED status —
    one impossible cluster spec must never escape
    phase3_create_clusters_and_workflows() and kill the export, and no
    workflow may be created on top of the failure."""

    # r-fleet.xlarge is AMD (4 vCPU -> 2 OCPU); this table caps amd at
    # 1 OCPU, so conversion raises ShapeCapacityError before any create.
    TINY_TABLE = {"amd.generic": {"ocpus_valid": [1],
                                  "memory_by_ocpu": {"1": [16, 32]}}}

    def _shrink_table(self, monkeypatch, tmp_path):
        from dbx_export import cluster_shape_mapping as csm
        table = tmp_path / "aidp_shape_limits.json"
        table.write_text(json.dumps(self.TINY_TABLE))
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", table)

    def _fakes(self):
        import types

        class FakeAidp:
            def __init__(self):
                self.creates = 0
            def find_cluster_by_name(self, name):
                return None
            def find_workflow_by_name(self, name):
                return None
            def list_clusters(self):
                return []
            def create_cluster(self, payload):
                self.creates += 1
                raise AssertionError("create must never be attempted")

        class FakeDbx:
            def get_cluster(self, cid):
                return {"cluster_name": f"ex_{cid}",
                        "node_type_id": "r-fleet.xlarge", "num_workers": 1}

        def graph_for(settings):
            node = types.SimpleNamespace(
                job_def={"settings": settings}, fetch_error=None)
            return types.SimpleNamespace(jobs={"1": node}, files={})

        return FakeAidp(), FakeDbx(), graph_for

    def _run_phase3(self, main_mod, aidp, dbx, graph):
        from dbx_export.report import Report
        report = Report(run_id="t", dbx_root_job_id="1",
                        export_output="/x", prefix="ai_tool")
        # Must return normally — a ShapeCapacityError escaping here would
        # terminate the whole export instead of degrading to FAILED.
        main_mod.phase3_create_clusters_and_workflows(
            aidp=aidp, dbx=dbx, graph=graph,
            nb_target_paths={}, uri_to_aidp={},
            export_output="/x", prefix="ai_tool",
            overwrite_workflow=False, cluster_override_key=None,
            report=report)
        return report

    def test_job_clusters_site_converts_to_failed(self, tmp_path, monkeypatch):
        main_mod, _ = _import_main_module()
        self._shrink_table(monkeypatch, tmp_path)
        aidp, dbx, graph_for = self._fakes()
        graph = graph_for({
            "name": "job_a",
            "job_clusters": [{"job_cluster_key": "jc1",
                              "new_cluster": {"node_type_id": "r-fleet.xlarge",
                                              "num_workers": 1}}],
            "tasks": [],
        })
        report = self._run_phase3(main_mod, aidp, dbx, graph)
        assert aidp.creates == 0
        job = report.jobs[0]
        assert "FAILED (shape capacity" in job.cluster_status
        # cluster_create_failed + no resolvable cluster -> the workflow is
        # never created on top of the failure.
        assert job.status == "FAILED"
        assert job.failure_reason == "no_cluster_available"

    def test_existing_cluster_site_converts_to_failed(self, tmp_path, monkeypatch):
        main_mod, _ = _import_main_module()
        self._shrink_table(monkeypatch, tmp_path)
        aidp, dbx, graph_for = self._fakes()
        graph = graph_for({
            "name": "job_b",
            "job_clusters": [],
            "tasks": [{"task_key": "t1", "existing_cluster_id": "c-123"}],
        })
        report = self._run_phase3(main_mod, aidp, dbx, graph)
        assert aidp.creates == 0
        job = report.jobs[0]
        assert "FAILED (shape capacity" in job.cluster_status
        assert job.status == "FAILED"
        assert job.failure_reason == "no_cluster_available"


class TestStartupGateScope:
    """The startup table gate must only apply to runs that can create
    clusters — --notebook-only and --cluster-key never consume limits."""

    @staticmethod
    def _args(**kw):
        import types
        base = dict(notebook_only=False, cluster_key=None,
                    shape_limits_path=None,
                    lake_ocid="ocid1.datalake.oc1.ap-mumbai-1.x",
                    workspace_id="w")
        base.update(kw)
        return types.SimpleNamespace(**base)

    def _with_broken_table(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        table = tmp_path / "aidp_shape_limits.json"
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", table)
        table.write_text("{not json")

    def test_broken_table_fails_cluster_creating_run(self, tmp_path, monkeypatch):
        main_mod, _ = _import_main_module()
        self._with_broken_table(tmp_path, monkeypatch)
        rc = main_mod._limits_startup_gate(self._args())
        assert rc == main_mod.ExitCode.BAD_ARGS

    def test_notebook_only_skips_gate(self, tmp_path, monkeypatch):
        main_mod, _ = _import_main_module()
        self._with_broken_table(tmp_path, monkeypatch)
        assert main_mod._limits_startup_gate(
            self._args(notebook_only=True)) is None

    def test_cluster_key_skips_gate(self, tmp_path, monkeypatch):
        main_mod, _ = _import_main_module()
        self._with_broken_table(tmp_path, monkeypatch)
        assert main_mod._limits_startup_gate(
            self._args(cluster_key="existing-key")) is None

    def test_missing_table_lazy_continues(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        main_mod, _ = _import_main_module()
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH",
                            tmp_path / "aidp_shape_limits.json")
        assert main_mod._limits_startup_gate(self._args()) is None

    def test_shape_limits_path_flag_feeds_env_var(self, tmp_path, monkeypatch):
        # The CLI flag is implemented as a setter for AIDP_SHAPE_LIMITS_PATH
        # (one authoritative override channel, no module globals).
        main_mod, _ = _import_main_module()
        flag_file = tmp_path / "flag.json"
        flag_file.write_text(json.dumps({"amd.generic": {"ocpus_valid": [1]}}))
        monkeypatch.delenv("AIDP_SHAPE_LIMITS_PATH", raising=False)
        assert main_mod._limits_startup_gate(
            self._args(shape_limits_path=str(flag_file))) is None
        assert os.environ["AIDP_SHAPE_LIMITS_PATH"] == str(flag_file)
        monkeypatch.delenv("AIDP_SHAPE_LIMITS_PATH", raising=False)


# ---------------------------------------------------------------------------
# Runtime self-heal persists learned facts into the (global) table
# ---------------------------------------------------------------------------

class TestSelfHealPersistence:
    TABLE = {"amd.generic": {"ocpus_valid": [1, 2, 4],
                             "memory_by_ocpu": {"1": [16, 32]}}}

    @staticmethod
    def _payload(ocpus=4, mem=16):
        cfg = {"ocpus": ocpus, "gpus": 0, "memoryInGBs": mem}
        return {"displayName": "t",
                "driverConfig": {"driverShape": "amd.generic",
                                 "driverShapeConfig": dict(cfg)},
                "workerConfig": {"workerShape": "amd.generic",
                                 "workerShapeConfig": dict(cfg)}}

    def test_learned_facts_persist_and_mark_partial(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        table = tmp_path / "aidp_shape_limits.json"
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", table)
        table.write_text(json.dumps(self.TABLE))
        payload = self._payload()
        lines = learn_limits_and_renormalize(
            payload,
            "Invalid executorShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128].")
        assert lines
        assert payload["workerConfig"]["workerShapeConfig"]["memoryInGBs"] == 32
        raw = json.loads(table.read_text())
        assert raw["amd.generic"]["memory_by_ocpu"]["4"] == [32, 64, 128]
        assert raw["_meta"]["verification"]["amd.generic"]["status"] == "partial"

    def test_missing_table_created_lazily(self, tmp_path, monkeypatch):
        # No table at all: the first learned fact creates it (marked partial)
        # so later clusters in the run benefit.
        from dbx_export import cluster_shape_mapping as csm
        from dbx_export.cluster_converter import learn_limits_and_renormalize
        table = tmp_path / "aidp_shape_limits.json"
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", table)
        payload = self._payload()
        lines = learn_limits_and_renormalize(
            payload,
            "Invalid executorShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128].")
        assert lines
        raw = json.loads(table.read_text())
        assert raw["amd.generic"]["memory_by_ocpu"]["4"] == [32, 64, 128]
        assert raw["_meta"]["verification"]["amd.generic"]["status"] == "partial"

    def test_create_wrapper_heals_and_persists(self, tmp_path, monkeypatch):
        # End-to-end through the create wrapper: the 400 is learned,
        # persisted, and the corrected payload retried.
        from dbx_export import cluster_shape_mapping as csm
        main_mod, AidpApiError = _import_main_module()
        table = tmp_path / "aidp_shape_limits.json"
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", table)
        table.write_text(json.dumps(self.TABLE))

        class FakeAidp:
            def __init__(self):
                self.calls = []
            def create_cluster(self, p):
                cfg_w = p["workerConfig"]["workerShapeConfig"]
                self.calls.append(dict(cfg_w))
                if cfg_w["memoryInGBs"] not in (32, 64, 128):
                    raise AidpApiError(
                        "create_cluster", 400,
                        "Invalid executorShapeConfig.memoryInGBs.  Valid values are: [32, 64, 128].")
                return {"key": "ok"}

        fake = FakeAidp()
        result = main_mod._create_cluster_with_intel_fallback(
            fake, self._payload(), lambda *a: None, _Report())
        assert result == {"key": "ok"}
        assert len(fake.calls) == 2  # healed once
        raw = json.loads(table.read_text())
        assert raw["_meta"]["verification"]["amd.generic"]["status"] == "partial"
        assert raw["amd.generic"]["memory_by_ocpu"]["4"] == [32, 64, 128]


# ---------------------------------------------------------------------------
# Create wrapper: bounded multi-cycle limits healing
# ---------------------------------------------------------------------------

def _import_main_module():
    main_mod = _import_with_sdk_stubs(
        "dbx_export.__main__", ("oci", "websocket", "requests"))
    from dbx_export.aidp_client import AidpApiError
    return main_mod, AidpApiError


class _Report:
    def __init__(self):
        self.errors = []


def _gpu_payload(shape="gpu.a10", ocpus=4, mem=48):
    cfg = {"ocpus": ocpus, "gpus": 1, "memoryInGBs": mem}
    return {"displayName": "ai_tool_x",
            "driverConfig": {"driverShape": shape, "driverShapeConfig": dict(cfg)},
            "workerConfig": {"workerShape": shape, "workerShapeConfig": dict(cfg)}}


class TestCreateWithLimitsHeal:
    def test_two_cycle_convergence_for_unknown_shape(self, tmp_path, monkeypatch):
        # AIDP validates OCPUs before memory: an unknown shape needs cycle 1
        # (learn OCPU list) AND cycle 2 (learn memory list for the new count).
        # The old wrapper granted only one cycle — the memory 400 escaped.
        from dbx_export import cluster_shape_mapping as csm
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        main_mod, AidpApiError = _import_main_module()

        class FakeAidp:
            def __init__(self):
                self.calls = []
            def create_cluster(self, payload):
                cfg = payload["workerConfig"]["workerShapeConfig"]
                self.calls.append(dict(cfg))
                if cfg["ocpus"] not in (15, 30):
                    raise AidpApiError("create_cluster", 400,
                        "Invalid driverShapeConfig.ocpus.  Valid values are: [15, 30].")
                if cfg["memoryInGBs"] not in (240, 480):
                    raise AidpApiError("create_cluster", 400,
                        "Invalid driverShapeConfig.memoryInGBs.  Valid values are: [240, 480].")
                return {"key": "ok"}

        from dbx_export.report import Report
        fake, report = FakeAidp(), Report(run_id="t", dbx_root_job_id="j", export_output="/x", prefix="ai_tool")
        result = main_mod._create_cluster_with_intel_fallback(
            fake, _gpu_payload(), lambda *a: None, report)
        assert result == {"key": "ok"}
        assert [ (c["ocpus"], c["memoryInGBs"]) for c in fake.calls ] == [
            (4, 48), (15, 48), (15, 240)]
        # both learned lists persisted for future runs
        saved = json.loads((tmp_path / "limits.json").read_text())
        assert saved["gpu.a10"]["ocpus_valid"] == [15, 30]
        assert saved["gpu.a10"]["memory_by_ocpu"]["15"] == [240, 480]
        assert len(report.errors) == 2  # one entry per heal cycle
        # A SUCCESSFUL recovery must be counted as warnings, never errors.
        summary = report.summary()
        assert summary["errors"] == 0
        assert summary["warnings"] == 2

    def test_non_limits_error_raises_immediately(self, tmp_path, monkeypatch):
        from dbx_export import cluster_shape_mapping as csm
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        main_mod, AidpApiError = _import_main_module()

        class FakeAidp:
            def __init__(self):
                self.calls = []
            def create_cluster(self, payload):
                self.calls.append(1)
                raise AidpApiError("create_cluster", 404, "NotAuthorizedOrNotFound")

        fake = FakeAidp()
        with pytest.raises(AidpApiError):
            main_mod._create_cluster_with_intel_fallback(
                fake, _gpu_payload(), lambda *a: None, _Report())
        assert len(fake.calls) == 1  # no blind retries

    def test_heal_cycle_cap_stops_pathological_loops(self, tmp_path, monkeypatch):
        # An API that keeps moving the goalposts (always a new learnable list)
        # must be stopped by MAX_LIMITS_HEAL_CYCLES, not loop forever.
        from dbx_export import cluster_shape_mapping as csm
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        main_mod, AidpApiError = _import_main_module()

        class FakeAidp:
            def __init__(self):
                self.calls = []
            def create_cluster(self, payload):
                cfg = payload["workerConfig"]["workerShapeConfig"]
                self.calls.append(dict(cfg))
                raise AidpApiError("create_cluster", 400,
                    f"Invalid driverShapeConfig.memoryInGBs.  "
                    f"Valid values are: [{cfg['memoryInGBs'] + 16}].")

        fake = FakeAidp()
        with pytest.raises(AidpApiError):
            main_mod._create_cluster_with_intel_fallback(
                fake, _gpu_payload(), lambda *a: None, _Report())
        assert len(fake.calls) == main_mod.MAX_LIMITS_HEAL_CYCLES + 1

    def test_intel_fallback_replaces_all_shape_selectors(self, tmp_path, monkeypatch):
        # Uses a REAL convert_dbx_cluster() payload — which carries
        # driverConfig.driverNodeType in addition to driverShape/workerShape/
        # nodeType. The fallback must replace ALL of them; leaving
        # driverNodeType="intel.generic" sends an internally inconsistent
        # retry (AMD shape fields alongside an Intel selector).
        from dbx_export import cluster_shape_mapping as csm
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        main_mod, AidpApiError = _import_main_module()
        from dbx_export.cluster_converter import convert_dbx_cluster
        from dbx_export.report import Report

        payload = convert_dbx_cluster(
            {"node_type_id": "r5.xlarge", "num_workers": 1},  # Intel catalog entry
            "t", prefix="ai_tool").aidp_payload
        assert payload["driverConfig"]["driverNodeType"] == "intel.generic"  # precondition

        class FakeAidp:
            def __init__(self):
                self.calls = []
            def create_cluster(self, p):
                self.calls.append(json.loads(json.dumps(p)))  # deep snapshot
                if "intel.generic" in json.dumps(p):
                    raise AidpApiError(
                        "create_cluster", 400,
                        "intel.generic shape is not supported for driver shape")
                return {"key": "ok"}

        fake = FakeAidp()
        report = Report(run_id="t", dbx_root_job_id="j",
                        export_output="/x", prefix="ai_tool")
        result = main_mod._create_cluster_with_intel_fallback(
            fake, payload, lambda *a: None, report)
        assert result == {"key": "ok"}
        assert len(fake.calls) == 2
        retry = fake.calls[1]
        assert "intel.generic" not in json.dumps(retry)  # NO selector left behind
        assert retry["driverConfig"]["driverNodeType"] == "amd.generic"
        assert retry["driverConfig"]["driverShape"] == "amd.generic"
        assert retry["workerConfig"]["workerShape"] == "amd.generic"
        assert retry["nodeType"] == "amd.generic"

    def test_intel_fallback_retry_is_heal_capable(self, tmp_path, monkeypatch):
        # After the intel->amd shape-name swap, a limits 400 on the
        # substituted payload must still self-heal (was a bare retry before).
        from dbx_export import cluster_shape_mapping as csm
        monkeypatch.setattr(csm, "LIMITS_CONFIG_PATH", tmp_path / "limits.json")
        main_mod, AidpApiError = _import_main_module()

        class FakeAidp:
            def __init__(self):
                self.calls = []
            def create_cluster(self, payload):
                shape = payload["workerConfig"]["workerShape"]
                cfg = payload["workerConfig"]["workerShapeConfig"]
                self.calls.append((shape, cfg["memoryInGBs"]))
                if shape == "intel.generic":
                    raise AidpApiError("create_cluster", 400,
                        "intel.generic shape is not supported for driver shape")
                if cfg["memoryInGBs"] not in (16, 32, 64):
                    raise AidpApiError("create_cluster", 400,
                        "Invalid driverShapeConfig.memoryInGBs.  Valid values are: [16, 32, 64].")
                return {"key": "ok"}

        from dbx_export.report import Report
        payload = _gpu_payload(shape="intel.generic", ocpus=2, mem=17)
        payload["workerConfig"]["workerShapeConfig"]["gpus"] = 0
        payload["driverConfig"]["driverShapeConfig"]["gpus"] = 0
        fake, report = FakeAidp(), Report(run_id="t", dbx_root_job_id="j", export_output="/x", prefix="ai_tool")
        result = main_mod._create_cluster_with_intel_fallback(
            fake, payload, lambda *a: None, report)
        assert result == {"key": "ok"}
        assert fake.calls == [("intel.generic", 17),
                              ("amd.generic", 17), ("amd.generic", 32)]
        assert any("shape-fallback" in e for e in report.errors)
        assert any("shape-limits-learned" in e for e in report.errors)
        # Successful recoveries are warnings, not errors.
        summary = report.summary()
        assert summary["errors"] == 0
        assert summary["warnings"] == 2


# ---------------------------------------------------------------------------
# Sanity: EVERY catalog entry converts to a valid combo via the real path
# ---------------------------------------------------------------------------

def test_every_catalog_entry_converts_onto_valid_combos():
    """Sweep the entire node-type catalog through convert_dbx_cluster and
    assert each CPU shape lands on an AIDP-valid (OCPU, memory) combo.
    Catches both catalog gaps (tiny-default fallback) and normalization
    regressions in one pass. GPU entries are skipped — gpu.a10 has no
    validity table yet and passes through unvalidated by design."""
    from dbx_export.node_type_catalog import DBX_NODE_CATALOG
    limits = load_shape_limits()
    for node_type, entry in DBX_NODE_CATALOG.items():
        if entry["arch"] == "GPU":
            continue
        shape, cfg = _convert_worker(node_type)
        rules = limits[shape]
        assert cfg["ocpus"] in rules["ocpus_valid"], (node_type, shape, cfg)
        assert cfg["memoryInGBs"] in rules["memory_by_ocpu"][str(cfg["ocpus"])], (
            node_type, shape, cfg)

"""The generated bootstrap-probe cell must be VALID PYTHON in both variants
(round-1.5 finding: nothing previously caught a syntax break in the
generated-notebook string) and the COA-metadata block must be present
exactly when requested, fail-soft by construction.
"""

from __future__ import annotations

from oracle_ai_data_platform_fusion_autopilot.commands.cluster_bootstrap_probe import (
    _build_probe_cell,
)


class TestProbeCellCompiles:
    def test_v1_cell_is_valid_python(self) -> None:
        src = _build_probe_cell(tenant="acme")
        compile(src, "<probe-cell-v1>", "exec")
        # v1 cells stay entirely free of the COA block (byte-stability for
        # existing substring assertions); only the null default + the
        # version expression change.
        assert "cluster_fetch_kff_rows" not in src
        assert "_coa_meta = None" in src
        assert "markerVersion=2 if _coa_meta is not None else 1" in src

    def test_v2_cell_is_valid_python_and_fail_soft(self) -> None:
        src = _build_probe_cell(tenant="acme", resolve_coa_metadata=True)
        compile(src, "<probe-cell-v2>", "exec")
        # The shared derivation + the ONE probe definition — never a
        # cell-local reimplementation (round-8 / D-4).
        assert "cluster_fetch_kff_rows" in src
        assert "derive_arms(_kff_rows, candidate=BICC_ROW_CANDIDATE)" in src
        assert "coa_probe.probe_charts" in src
        # Fail-soft: fetch failure → coverage='skipped' + skipReason,
        # gl_coa unavailable (S5) → probeNote, never an abort.
        assert "coverage='skipped'" in src
        assert "gl_coa probe unavailable" in src
        # The v2 block runs unconditionally once emitted.
        assert "if True:" in src

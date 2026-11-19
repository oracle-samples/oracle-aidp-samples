"""P1.5ε §Step 4 — dispatch/notebook_builder.py tests.

These lock the run-cell contract that the laptop-side dispatcher and the
schema-side ``RunSummary.from_marker_dict`` depend on. The most important
one — ``test_run_cell_calls_to_marker_dict_not_asdict`` — prevents a
regression where someone hand-rolls a subset dict and silently drops
``schema_version`` / ``watermark_used`` / etc.
"""

from __future__ import annotations

import base64
import sys
from pathlib import Path
from subprocess import CompletedProcess
from unittest import mock

import pytest
from oracle_ai_data_platform_fusion_autopilot.dispatch.notebook_builder import (
    _CLUSTER_RUNTIME_IMPORTS,
    MARKER_BEGIN,
    MARKER_END,
    _build_runtime_dep_preflight,
    build_notebook,
)


@pytest.fixture
def wheel(tmp_path: Path) -> Path:
    p = tmp_path / "oracle_ai_data_platform_fusion_autopilot-0.2.0-py3-none-any.whl"
    p.write_bytes(b"PK\x03\x04 fake wheel bytes")
    return p


def _all_source(nb: dict) -> str:
    """Concatenate every code-cell source into one big string for substring
    assertions. Tests should match by substring; the exact whitespace
    layout is intentionally not asserted (it's an implementation detail
    of the template strings)."""
    out: list[str] = []
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            out.extend(cell["source"])
    return "".join(out)


class TestNotebookStructure:
    def test_nbformat_metadata(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="apiVersion: aidp-fusion-autopilot/v1\n",
            mode="seed",
            datasets=None,
            layers=None,
        )
        assert nb["nbformat"] == 4
        assert nb["nbformat_minor"] == 5
        # 1 markdown + 4 code cells.
        assert len(nb["cells"]) == 5
        cell_types = [c["cell_type"] for c in nb["cells"]]
        assert cell_types == ["markdown", "code", "code", "code", "code"]

    def test_title_in_markdown_cell(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            title="TC29 narrow probe",
        )
        md_source = "".join(nb["cells"][0]["source"])
        assert "TC29 narrow probe" in md_source


class TestInstallCell:
    def test_install_cell_inlines_wheel_base64(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        install = "".join(nb["cells"][1]["source"])
        expected_b64 = base64.b64encode(wheel.read_bytes()).decode()
        assert expected_b64 in install

    def test_install_cell_uses_wheel_filename(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        install = "".join(nb["cells"][1]["source"])
        assert wheel.name in install


class TestCredsCell:
    def test_default_secret_name_and_key(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        creds = "".join(nb["cells"][2]["source"])
        assert "name='fusion_bicc_password'" in creds
        assert "key='password'" in creds

    def test_secret_override(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            bicc_secret_name="custom_secret",
            bicc_secret_key="custom_key",
        )
        creds = "".join(nb["cells"][2]["source"])
        assert "name='custom_secret'" in creds
        assert "key='custom_key'" in creds

    def test_bundle_yaml_injected_via_repr(self, wheel: Path) -> None:
        # repr() preserves embedded newlines; the cluster-side write_text
        # gets the operator's bundle byte-for-byte.
        yaml_body = "apiVersion: aidp-fusion-autopilot/v1\nproject: test\n"
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml=yaml_body,
            mode="seed",
            datasets=None,
            layers=None,
        )
        creds = "".join(nb["cells"][2]["source"])
        assert repr(yaml_body) in creds

    def test_runtime_env_vars_injected_without_password_like_values(
        self, wheel: Path
    ) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            env_vars={
                "FUSION_BICC_BASE_URL": "https://fa.example.com",
                "FUSION_BICC_USER": "oac.operator",
                "FUSION_BICC_PASSWORD": "local-password",
                "OAC_OAUTH_CLIENT_SECRET": "local-secret",
                "OAC_URL": "https://oac.example.com",
            },
        )
        creds = "".join(nb["cells"][2]["source"])
        assert "FUSION_BICC_BASE_URL" in creds
        assert "https://fa.example.com" in creds
        assert "FUSION_BICC_USER" in creds
        assert "OAC_URL" in creds
        assert "runtime env loaded:" in creds
        assert "local-password" not in creds
        assert "OAC_OAUTH_CLIENT_SECRET" not in creds
        assert "local-secret" not in creds


class TestRunCell:
    def test_mode_injected(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="incremental",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "mode='incremental'" in run

    def test_datasets_filter_injected(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=["ap_invoices"],
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "datasets=['ap_invoices']" in run

    def test_layers_filter_injected(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=["gold"],
        )
        run = "".join(nb["cells"][3]["source"])
        assert "layers=['gold']" in run

    def test_none_filters_render_as_none_not_null(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "datasets=None" in run
        assert "layers=None" in run
        assert "datasets=null" not in run

    def test_resume_run_id_default_none(self, wheel: Path) -> None:
        # P1.5ε-fix5: default kwarg is None → run cell renders
        # `resume_run_id=None,` (fresh run, the common case). Regression
        # lock for the default behavior.
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "resume_run_id=None" in run

    def test_resume_run_id_literal_injected(self, wheel: Path) -> None:
        # P1.5ε-fix5: operator-supplied run_id is repr()-injected as a
        # quoted Python literal (same pattern as mode / datasets / layers).
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            resume_run_id="abc-123",
        )
        run = "".join(nb["cells"][3]["source"])
        assert "resume_run_id='abc-123'" in run

    def test_resume_run_id_repr_escapes_special_chars(self, wheel: Path) -> None:
        # Defensive — repr() must produce a well-formed Python literal
        # even if the run_id contains characters that would otherwise
        # break source parsing (quote, backslash). Asserts the generated
        # run cell parses cleanly with ast.parse.
        import ast

        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            resume_run_id="x'y\\z",
        )
        run = "".join(nb["cells"][3]["source"])
        # If the repr() escape is broken, ast.parse raises SyntaxError.
        ast.parse(run)

    def test_marker_emit_present(self, wheel: Path) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert MARKER_BEGIN in run
        assert MARKER_END in run

    def test_run_cell_calls_to_marker_dict_not_asdict(self, wheel: Path) -> None:
        # Locks the schema contract: the run-cell uses the canonical
        # serializer, NOT a hand-rolled dict literal or dataclasses.asdict
        # (which would drop schema_version + fail on datetime fields).
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "summary.to_marker_dict()" in run
        assert "dataclasses.asdict" not in run
        assert "asdict(summary)" not in run

    def test_resume_run_id_parameter_threads_into_run_cell(self, wheel: Path) -> None:
        """Phase 5 P1.5ε-fix5 — REST-dispatch resume is supported.
        The notebook cell threads ``resume_run_id`` into the cluster-
        side ``orchestrator.run(...)`` call so the resumed run adopts
        the supplied id.
        """
        import inspect

        # Locks the API: build_notebook accepts resume_run_id as a
        # keyword-only parameter with default None.
        sig = inspect.signature(build_notebook)
        assert "resume_run_id" in sig.parameters
        assert sig.parameters["resume_run_id"].default is None
        # When None (default), the cell still emits a literal `None`.
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "resume_run_id=None" in run
        # When set, the cell emits the literal id.
        nb_with_id = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
            resume_run_id="phase5-resume-id-007",
        )
        run_with_id = "".join(nb_with_id["cells"][3]["source"])
        assert "resume_run_id='phase5-resume-id-007'" in run_with_id

    def test_run_cell_catches_schema_drift_and_emits_drift_marker(
        self, wheel: Path
    ) -> None:
        """Phase 3c — the run cell must wrap ``orchestrator.run`` in
        ``try/except SchemaDriftDetectedError``, emit a discriminated
        marker (``_kind == "schema_drift"``) carrying the artifact JSON,
        and then re-raise. Without this the laptop dispatcher cannot
        translate cluster-side drift into exit 14."""
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="incremental",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        # Import is wired (must exist for the except clause to work).
        assert "SchemaDriftDetectedError" in run
        # Wrapped in try/except.
        assert "try:" in run
        assert "except SchemaDriftDetectedError" in run
        # Drift marker discriminator + artifact handoff.
        assert "_kind" in run and "schema_drift" in run
        assert "artifact_json" in run
        # Re-raise so the cluster cell ends in error state (matches the
        # marker-precedence contract in dispatch_via_rest).
        assert "raise" in run

    def test_force_fingerprint_skip_threaded_into_run_cell(
        self, wheel: Path
    ) -> None:
        """Review finding (P3c-review #1, BLOCKING): the
        ``--force-fingerprint-skip`` CLI flag must reach the
        cluster-side ``orchestrator.run(...)`` kwargs, not only the
        inline path. Without this, the REST-dispatch path silently
        enforces the gate and the audit-row promised in the PR is
        never written."""
        for ffs in (False, True):
            nb = build_notebook(
                wheel_path=wheel,
                bundle_yaml="",
                mode="incremental",
                datasets=None,
                layers=None,
                force_fingerprint_skip=ffs,
            )
            run = "".join(nb["cells"][3]["source"])
            assert f"force_fingerprint_skip={ffs!r}" in run

    def test_force_fingerprint_skip_default_is_false(self, wheel: Path) -> None:
        """Default must be False so existing callers don't accidentally
        bypass the gate."""
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="incremental",
            datasets=None,
            layers=None,
        )
        run = "".join(nb["cells"][3]["source"])
        assert "force_fingerprint_skip=False" in run

    def test_run_cell_compiles_as_python(self, wheel: Path) -> None:
        """The run cell source must be valid Python — a stray indentation
        bug in the try/except would surface as a SyntaxError on the
        cluster, masking the drift hand-off."""
        for backend in ("legacy-python", "content-pack"):
            nb = build_notebook(
                wheel_path=wheel,
                bundle_yaml="",
                mode="incremental",
                datasets=None,
                layers=None,
                execution_backend=backend,
                profile_yaml="x" if backend == "content-pack" else None,
                pack_files={"a": "b"} if backend == "content-pack" else None,
                pack_manifest={"k": "v"} if backend == "content-pack" else None,
            )
            # Index depends on backend (content-pack inserts a bootstrap cell).
            run_idx = 4 if backend == "content-pack" else 3
            run = "".join(nb["cells"][run_idx]["source"])
            compile(run, "<run_cell>", "exec")


class TestVerifyCell:
    def test_imports_load_bundle_from_schema_not_runtime(
        self, wheel: Path
    ) -> None:
        # The verify cell should use the canonical schema-level location
        # (P1.5ε §Step 1d), not the back-compat re-export.
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        verify = "".join(nb["cells"][4]["source"])
        assert "schema.bundle import load_bundle" in verify


class TestRuntimeDepPreflight:
    """Cluster-side runtime-dependency gate — the wheel installs with
    ``--no-deps`` so the generated notebook must validate pydantic/pyyaml are
    importable BEFORE the creds cell's ``import orchestrator``, else a clean
    AIDP runtime dies with a raw ImportError before any safety gate runs."""

    def test_install_cell_gates_before_orchestrator_import(
        self, wheel: Path
    ) -> None:
        nb = build_notebook(
            wheel_path=wheel,
            bundle_yaml="",
            mode="seed",
            datasets=None,
            layers=None,
        )
        install = "".join(nb["cells"][1]["source"])
        creds = "".join(nb["cells"][2]["source"])
        # The gate lives in the install cell (index 1) and validates every
        # import-critical dep; the orchestrator import is in the creds cell
        # (index 2) — so validation strictly precedes the fragile import.
        for import_name in _CLUSTER_RUNTIME_IMPORTS:
            assert repr(import_name) in install
        assert "DISPATCH_RUNTIME_DEPS_MISSING" in install
        assert "import oracle_ai_data_platform_fusion_autopilot" in creds or (
            "from oracle_ai_data_platform_fusion_autopilot import orchestrator"
            in creds
        )

    def test_gate_source_compiles(self) -> None:
        compile(_build_runtime_dep_preflight(), "<preflight>", "exec")

    @staticmethod
    def _exec_ns(run_result: CompletedProcess, tmp_path: Path):
        """Exec namespace with a fake ``subprocess`` (so we don't mock-patch
        the real one — that would collide with ``importlib.import_module``
        patching during target resolution) and a stubbed ``sys``."""
        run_mock = mock.Mock(return_value=run_result)
        fake_subprocess = mock.Mock()
        fake_subprocess.run = run_mock
        ns = {"subprocess": fake_subprocess, "sys": sys, "_target": tmp_path}
        return ns, run_mock

    def test_clean_runtime_fails_closed(self, tmp_path: Path) -> None:
        """Simulate a clean runtime (imports fail) with no reachable index
        (install fails) → the gate must raise the coded diagnostic, NOT let a
        later raw ImportError leak."""
        snippet = _build_runtime_dep_preflight()
        ns, _ = self._exec_ns(
            CompletedProcess([], 1, "", "no matching distribution"), tmp_path
        )
        with mock.patch("importlib.import_module", side_effect=ImportError("boom")):
            with pytest.raises(RuntimeError, match="DISPATCH_RUNTIME_DEPS_MISSING"):
                exec(snippet, ns)

    def test_present_runtime_passes(self, tmp_path: Path) -> None:
        """When every required dep imports, the gate is a no-op (no install
        attempt, no raise)."""
        snippet = _build_runtime_dep_preflight()
        ns, run_mock = self._exec_ns(CompletedProcess([], 0, "", ""), tmp_path)
        with mock.patch("importlib.import_module", return_value=object()):
            exec(snippet, ns)  # must not raise
        run_mock.assert_not_called()

    def test_self_heal_install_recovers(self, tmp_path: Path) -> None:
        """Deps missing initially but installable → gate installs the closure,
        re-validates green, and does not raise."""
        snippet = _build_runtime_dep_preflight()
        ns, run_mock = self._exec_ns(CompletedProcess([], 0, "", ""), tmp_path)
        calls = {"n": 0}

        def _flaky_import(_name):
            # First sweep (len(_CLUSTER_RUNTIME_IMPORTS) calls) fails; after
            # the install, the re-sweep succeeds.
            if calls["n"] < len(_CLUSTER_RUNTIME_IMPORTS):
                calls["n"] += 1
                raise ImportError("not yet")
            return object()

        with mock.patch("importlib.import_module", side_effect=_flaky_import):
            exec(snippet, ns)  # must not raise
        run_mock.assert_called_once()

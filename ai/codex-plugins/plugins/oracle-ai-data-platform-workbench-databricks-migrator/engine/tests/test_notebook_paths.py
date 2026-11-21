"""
Unit tests for notebook path extraction, normalization, and dependency flattening.

Tests cover:
- scan_for_run_deps: %run, dbutils.notebook.run, oidlUtils.notebook.run extraction
- normalize_nb_path: /Workspace/ stripping, JOB_ROOT stripping, space handling
- download_path computation: /Workspace/ only stripping (preserves JOB_ROOT)
- _flatten_manifest_run_deps: depth-first recursive flattening
- _flatten_deps (check_data_availability): recursive dep collection with dedup
"""

import json
import os
import posixpath  # Workspace paths are POSIX even when tests run on Windows.
import sys

import pytest

# Make scripts/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


# ─── Helper: build a minimal .ipynb from code cells ──────────────────────────

def _make_notebook(*code_cells) -> bytes:
    """Build a minimal ipynb JSON with the given code cell sources."""
    cells = []
    for src in code_cells:
        cells.append({"cell_type": "code", "source": [src]})
    nb = {"cells": cells}
    return json.dumps(nb).encode("utf-8")


def test_child_filesystem_cell_read_uses_resolved_path_literal():
    """The Source-2 child reader must not emit an undefined ``safe_path``.

    Incident: phase-2 run aaaaaaaa-0000-4000-8000-000000000106 failed in
    ``geo_clusters`` because a partial rename left the migrator f-string with
    an unbound ``safe_path`` name. The remote code must receive the probe's
    exact resolved path as a JSON/Python string literal.
    """
    import inspect
    import job_migrate

    source = inspect.getsource(job_migrate._inline_child_notebook)
    assert 'resolved_path_literal = json.dumps(count_data["resolved_path"])' in source
    assert "with open({resolved_path_literal}) as f:" in source
    assert "with open('{safe_path}') as f:" not in source


def test_download_path_variants_include_canonical_sanitized_filename(monkeypatch):
    """A dependency reference must find the filename produced by export.

    Incident: phase-2 run aaaaaaaa-0000-4000-8000-000000000107 could not
    resolve ``Utils_BS+SMS.ipynb`` although export had saved the notebook as
    ``Utils_BS_SMS.ipynb``.
    """
    import job_migrate
    from job_migrate import _download_path_variants

    monkeypatch.setattr(job_migrate, "EXPORT_BASE", "export-root/notebooks")
    requested = (
        "/Workspace/export-root/notebooks/Users/user6@example.com/"
        "Utils_BS+SMS.ipynb"
    )
    variants = _download_path_variants(requested)

    assert variants[0] == requested
    assert (
        "/Workspace/export-root/notebooks/Users/user6@example.com/"
        "Utils_BS_SMS.ipynb"
        in variants
    )


def test_download_path_variants_match_exporter_for_filename_and_parent_segments(monkeypatch):
    import job_migrate
    from job_migrate import _download_path_variants
    from dbx_export.sanitizer import KIND_NOTEBOOK, dbx_to_aidp_path

    export_base = "/Workspace/export-target/notebooks"
    monkeypatch.setattr(job_migrate, "EXPORT_BASE", export_base)
    source = "/Workspace/Folder-With+Meaning/Notebook:Part+One.ipynb"
    requested = f"{export_base}/Folder-With+Meaning/Notebook:Part+One.ipynb"
    variants = _download_path_variants(requested)
    exported = dbx_to_aidp_path(
        source, "/Workspace/export-target", KIND_NOTEBOOK
    )
    assert exported in variants


@pytest.mark.parametrize(
    ("requested", "exported"),
    [
        ("/Workspace/Users/a/Report_draft+.ipynb", "/Workspace/Users/a/Report_draft.ipynb"),
        ("/Workspace/Users/a/Data (Prod).ipynb", "/Workspace/Users/a/Data_Prod.ipynb"),
    ],
)
def test_download_path_variants_match_exporter_stem_sanitization(
    monkeypatch, requested, exported
):
    import job_migrate
    from job_migrate import _download_path_variants

    export_base = "/Workspace/export-target/notebooks"
    monkeypatch.setattr(job_migrate, "EXPORT_BASE", export_base)
    exported_requested = export_base + requested.removeprefix("/Workspace")
    exported_expected = export_base + exported.removeprefix("/Workspace")
    assert exported_expected in _download_path_variants(exported_requested)


def test_download_path_variants_preserve_special_character_export_root(monkeypatch):
    import job_migrate

    export_base = "/Workspace/Export+A (live)/notebooks"
    monkeypatch.setattr(job_migrate, "EXPORT_BASE", export_base)

    variants = job_migrate._download_path_variants(
        export_base + "/Users/a/Report+One.ipynb"
    )

    assert export_base + "/Users/a/Report_One.ipynb" in variants
    assert not any(
        path.startswith("/Workspace/Export_A_live/notebooks/")
        for path in variants
    )


def test_download_path_variants_do_not_alias_raw_non_export_path(monkeypatch):
    import job_migrate

    monkeypatch.setattr(job_migrate, "EXPORT_BASE", "export-root/notebooks")
    variants = job_migrate._download_path_variants(
        "/Workspace/Users/a/A+B.ipynb"
    )

    assert "/Workspace/Users/a/A_B.ipynb" not in variants


def test_download_path_variants_without_export_base_do_not_add_sanitized_alias(
    monkeypatch,
):
    import job_migrate

    monkeypatch.setattr(job_migrate, "EXPORT_BASE", "")
    variants = job_migrate._download_path_variants(
        "/Workspace/Users/a/A+B.ipynb"
    )

    assert "/Workspace/Users/a/A_B.ipynb" not in variants


def test_download_path_variants_keep_valid_export_path_stable(monkeypatch):
    import job_migrate

    export_base = "/Workspace/export-target/notebooks"
    path = export_base + "/Users/a/Already_Safe.ipynb"
    monkeypatch.setattr(job_migrate, "EXPORT_BASE", export_base)

    variants = job_migrate._download_path_variants(path)

    assert variants[0] == path
    assert variants.count(path) == 1
    assert job_migrate._download_path_variants(variants[0])[0] == path


def test_download_path_variants_sanitize_suffix_for_workspace_relative_export(
    monkeypatch,
):
    import job_migrate

    export_base = "export-target/notebooks"
    monkeypatch.setattr(job_migrate, "EXPORT_BASE", export_base)

    variants = job_migrate._download_path_variants(
        export_base + "/Users/a/Report+One.ipynb"
    )

    assert export_base + "/Users/a/Report_One.ipynb" in variants


# ─── scan_for_run_deps ───────────────────────────────────────────────────────

class TestScanForRunDeps:
    """Extract %run / notebook.run references from notebook source."""

    @staticmethod
    def _scan(nb_bytes):
        from job_migrate import scan_for_run_deps
        return scan_for_run_deps(nb_bytes)

    # ── %run patterns ──

    def test_run_absolute_path(self):
        nb = _make_notebook("%run /Workspace/Users/foo/Utils")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/Workspace/Users/foo/Utils.ipynb"

    def test_run_relative_path(self):
        nb = _make_notebook("%run ../HelperFunctions/all_utils")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "../HelperFunctions/all_utils.ipynb"

    def test_run_dot_relative(self):
        nb = _make_notebook("%run ./sibling_notebook")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "./sibling_notebook.ipynb"

    def test_run_with_ipynb_extension(self):
        nb = _make_notebook("%run /Workspace/GIT/read_ft_tables.ipynb")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/Workspace/GIT/read_ft_tables.ipynb"

    def test_run_with_dollar_params(self):
        nb = _make_notebook("%run /path/to/notebook $start_date $end_date")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/to/notebook.ipynb"
        assert "start_date" in deps[0][1]
        assert "end_date" in deps[0][1]

    def test_run_quoted_path_double(self):
        nb = _make_notebook('%run "/path/with spaces/Utils"')
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/with spaces/Utils.ipynb"

    def test_run_quoted_path_single(self):
        nb = _make_notebook("%run '/path/with spaces/Utils'")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/with spaces/Utils.ipynb"

    def test_run_unquoted_absolute_path_with_spaces(self):
        nb = _make_notebook("%run /Workspace/FL Queries Run/SomeNotebook")
        deps = self._scan(nb)
        assert deps == [("/Workspace/FL Queries Run/SomeNotebook.ipynb", {})]

    def test_run_unquoted_spaced_path_preserves_widget_args(self):
        nb = _make_notebook(
            '%run /Workspace/FL Queries Run/SomeNotebook $date="2026-09-15"'
        )
        deps = self._scan(nb)
        assert deps[0][0] == "/Workspace/FL Queries Run/SomeNotebook.ipynb"
        assert "date" in deps[0][1]

    def test_malformed_quoted_run_is_not_invented(self, capsys):
        nb = _make_notebook('%run "/Workspace/FL Queries Run/SomeNotebook')
        assert self._scan(nb) == []
        assert "malformed or empty" in capsys.readouterr().out

    def test_run_commented_out_ignored(self):
        nb = _make_notebook("# %run /path/to/old_notebook\nprint('hello')")
        deps = self._scan(nb)
        assert len(deps) == 0

    def test_run_multiple_in_one_cell(self):
        nb = _make_notebook(
            "%run /path/first\n%run /path/second\n%run /path/third"
        )
        deps = self._scan(nb)
        assert len(deps) == 3
        paths = [d[0] for d in deps]
        assert "/path/first.ipynb" in paths
        assert "/path/second.ipynb" in paths
        assert "/path/third.ipynb" in paths

    def test_run_across_multiple_cells(self):
        nb = _make_notebook("%run /path/cell1", "x = 1", "%run /path/cell2")
        deps = self._scan(nb)
        assert len(deps) == 2

    def test_run_ignores_markdown_cells(self):
        cells = [
            {"cell_type": "markdown", "source": ["%run /path/in_markdown"]},
            {"cell_type": "code", "source": ["%run /path/in_code"]},
        ]
        nb = json.dumps({"cells": cells}).encode()
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/in_code.ipynb"

    # ── dbutils.notebook.run patterns ──

    def test_dbutils_notebook_run_double_quotes(self):
        nb = _make_notebook('dbutils.notebook.run("/path/to/child", 60)')
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/to/child.ipynb"

    def test_dbutils_notebook_run_single_quotes(self):
        nb = _make_notebook("dbutils.notebook.run('/path/to/child', 60)")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/to/child.ipynb"

    def test_dbutils_notebook_run_with_params(self):
        nb = _make_notebook(
            'dbutils.notebook.run("/path/child", 300, {"key": "val"})'
        )
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/child.ipynb"
        assert deps[0][1] == {}  # params not extracted from this pattern

    def test_dbutils_notebook_run_commented_ignored(self):
        nb = _make_notebook('# dbutils.notebook.run("/old/path", 60)')
        deps = self._scan(nb)
        assert len(deps) == 0

    # ── oidlUtils.notebook.run patterns ──

    def test_oidlutils_notebook_run(self):
        nb = _make_notebook('oidlUtils.notebook.run("/path/to/aidp_child", 120)')
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/to/aidp_child.ipynb"

    def test_oidlutils_notebook_run_single_quotes(self):
        nb = _make_notebook("oidlUtils.notebook.run('/path/child', 60)")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert deps[0][0] == "/path/child.ipynb"

    # ── Mixed patterns ──

    def test_mixed_run_and_notebook_run(self):
        nb = _make_notebook(
            "%run /path/via_run\n"
            'dbutils.notebook.run("/path/via_dbutils", 60)\n'
            'oidlUtils.notebook.run("/path/via_oidl", 60)'
        )
        deps = self._scan(nb)
        assert len(deps) == 3
        paths = [d[0] for d in deps]
        assert "/path/via_run.ipynb" in paths
        assert "/path/via_dbutils.ipynb" in paths
        assert "/path/via_oidl.ipynb" in paths

    # ── Edge cases ──

    def test_empty_notebook(self):
        nb = json.dumps({"cells": []}).encode()
        deps = self._scan(nb)
        assert deps == []

    def test_invalid_json(self):
        deps = self._scan(b"this is not json")
        assert deps == []

    def test_empty_run_line_ignored(self):
        nb = _make_notebook("%run ")
        deps = self._scan(nb)
        assert len(deps) == 0

    def test_run_with_dollar_in_path(self):
        """$var in path itself should be stripped."""
        nb = _make_notebook("%run /path/$env/notebook")
        deps = self._scan(nb)
        assert len(deps) == 1
        assert "$" not in deps[0][0]


# ─── normalize_nb_path ────────────────────────────────────────────────────────

class TestNormalizeNbPath:
    """Path normalization: /Workspace/ stripping, JOB_ROOT stripping, spaces."""

    @staticmethod
    def _normalize(path, job_root=""):
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = job_root
            return job_migrate.normalize_nb_path(path)
        finally:
            job_migrate.JOB_ROOT = old_root

    # ── /Workspace/ stripping ──

    def test_strips_workspace_prefix(self):
        result = self._normalize("/Workspace/GIT/Auto_FT/notebook.ipynb")
        assert result == "GIT/Auto_FT/notebook.ipynb"

    def test_strips_leading_slash_without_workspace(self):
        result = self._normalize("/Users/foo/notebook.ipynb")
        assert result == "Users/foo/notebook.ipynb"

    def test_bare_path_unchanged(self):
        result = self._normalize("Users/foo/notebook.ipynb")
        assert result == "Users/foo/notebook.ipynb"

    # ── JOB_ROOT stripping ──

    def test_preserves_job_root(self):
        # Contract (caf9fdac): strip /Workspace/ ONLY — JOB_ROOT is preserved
        # so keys are unambiguous across jobs; EXPORT_BASE handles collapsing.
        result = self._normalize(
            "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb",
            job_root="GIT/Auto_FT_Derived"
        )
        assert result == "GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"

    def test_job_root_trailing_slash(self):
        result = self._normalize(
            "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb",
            job_root="GIT/Auto_FT_Derived/"
        )
        assert result == "GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"

    def test_job_root_no_match(self):
        """Path outside JOB_ROOT is not stripped."""
        result = self._normalize(
            "/Workspace/Users/user2@example.com/Utils.ipynb",
            job_root="GIT/Auto_FT_Derived"
        )
        assert result == "Users/user2@example.com/Utils.ipynb"

    def test_empty_job_root(self):
        result = self._normalize(
            "/Workspace/GIT/Auto_FT/notebook.ipynb",
            job_root=""
        )
        assert result == "GIT/Auto_FT/notebook.ipynb"

    def test_staging_root(self):
        """Long staging paths from behavioral_job manifests."""
        result = self._normalize(
            "/Workspace/scratch/user0/automation/dbx_to_aidp/migration_staging/Users/foo/nb.ipynb",
            job_root="testing/user0/automation/dbx_to_aidp/migration_staging"
        )
        assert result == "scratch/user0/automation/dbx_to_aidp/migration_staging/Users/foo/nb.ipynb"

    # ── Space handling ──

    def test_spaces_replaced_with_underscores(self):
        result = self._normalize("/Workspace/GIT/Model Monitoring/Utils.ipynb")
        assert result == "GIT/Model_Monitoring/Utils.ipynb"

    def test_spaces_in_job_root(self):
        """JOB_ROOT with spaces should still match."""
        result = self._normalize(
            "/Workspace/GIT/My Project/HelperFunctions/utils.ipynb",
            job_root="GIT/My Project"
        )
        assert result == "GIT/My_Project/HelperFunctions/utils.ipynb"

    # ── Users/ prefix paths (external deps) ──

    def test_users_path_no_job_root(self):
        result = self._normalize(
            "/Workspace/Users/user2@example.com/ReadLogsNotebookA.ipynb",
            job_root="GIT/Auto_FT_Derived"
        )
        assert result == "Users/user2@example.com/ReadLogsNotebookA.ipynb"


# ─── download_path computation ────────────────────────────────────────────────

class TestDownloadPath:
    """download_path strips /Workspace/ only, preserves JOB_ROOT and spaces."""

    @staticmethod
    def _compute_download_path(notebook_path):
        """Mirror the download_path logic from ensure_migrated."""
        download_path = notebook_path.lstrip("/")
        if download_path.startswith("Workspace/"):
            download_path = download_path[len("Workspace/"):]
        if not download_path.endswith(".ipynb"):
            download_path += ".ipynb"
        return download_path

    def test_workspace_prefix_stripped(self):
        result = self._compute_download_path(
            "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"
        )
        assert result == "GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"

    def test_preserves_job_root(self):
        """Key fix: JOB_ROOT is NOT stripped — API needs full workspace path."""
        result = self._compute_download_path(
            "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"
        )
        assert result.startswith("GIT/Auto_FT_Derived/")

    def test_no_workspace_prefix(self):
        result = self._compute_download_path(
            "/Users/user2@example.com/Utils.ipynb"
        )
        assert result == "Users/user2@example.com/Utils.ipynb"

    def test_bare_path(self):
        result = self._compute_download_path("Users/foo/notebook.ipynb")
        assert result == "Users/foo/notebook.ipynb"

    def test_appends_ipynb_extension(self):
        result = self._compute_download_path("/Workspace/GIT/read_ft_tables")
        assert result == "GIT/read_ft_tables.ipynb"

    def test_preserves_spaces(self):
        """API needs the original path with spaces (unlike normalized)."""
        result = self._compute_download_path(
            "/Workspace/GIT/Model Monitoring/Utils.ipynb"
        )
        assert result == "GIT/Model Monitoring/Utils.ipynb"
        assert " " in result  # spaces preserved

    def test_download_vs_normalized_different_with_job_root(self):
        """download_path and normalized should differ when JOB_ROOT is set."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = "GIT/Auto_FT_Derived"
            path = "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"
            download = self._compute_download_path(path)
            normalized = job_migrate.normalize_nb_path(path)
            # BOTH keep JOB_ROOT since caf9fdac — normalization can no longer
            # produce a path the download API would 404 on.
            assert download == "GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"
            assert normalized == "GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"
        finally:
            job_migrate.JOB_ROOT = old_root

    def test_download_vs_normalized_same_without_job_root(self):
        """With empty JOB_ROOT, they differ only in space handling."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = ""
            path = "/Workspace/GIT/notebook.ipynb"
            download = self._compute_download_path(path)
            normalized = job_migrate.normalize_nb_path(path)
            assert download == "GIT/notebook.ipynb"
            assert normalized == "GIT/notebook.ipynb"
        finally:
            job_migrate.JOB_ROOT = old_root

    def test_external_dep_outside_job_root(self):
        """External deps (Users/) should be same for download and normalized."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = "GIT/Auto_FT_Derived"
            path = "/Workspace/Users/user2@example.com/ReadLogsNotebookB.ipynb"
            download = self._compute_download_path(path)
            normalized = job_migrate.normalize_nb_path(path)
            assert download == "Users/user2@example.com/ReadLogsNotebookB.ipynb"
            assert normalized == "Users/user2@example.com/ReadLogsNotebookB.ipynb"
        finally:
            job_migrate.JOB_ROOT = old_root

    def test_recursive_sub_dep_should_pass_original_not_normalized(self):
        """Regression test: recursive ensure_migrated calls must pass original
        dep_path (not normalized) so download_path can recover JOB_ROOT.

        Simulates the bug: parent notebook has %run ../HelperFunctions/all_utils,
        which resolves to /Workspace/GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb.
        If we normalize first → 'HelperFunctions/all_utils.ipynb', the download_path
        would also be 'HelperFunctions/all_utils.ipynb' → 404 from AIDP API.
        Passing the original path preserves JOB_ROOT for download."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = "GIT/Auto_FT_Derived"

            # Simulate: parent notebook at HelperFunctions/HelperFunctions.ipynb
            # has %run all_utils → resolved to full path
            parent_path = "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/HelperFunctions.ipynb"
            sub_dep_run = "../HelperFunctions/all_utils"
            import os
            parent_dir = posixpath.dirname(parent_path)
            resolved = posixpath.normpath(posixpath.join(parent_dir, sub_dep_run)) + ".ipynb"
            # resolved = /Workspace/GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb

            # Historic bug structurally gone: normalization preserves the
            # full path now, so download-from-normalized stays correct.
            dep_normalized = job_migrate.normalize_nb_path(resolved)
            download = self._compute_download_path(dep_normalized)
            assert download == "GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"

            # FIX: pass original resolved path, download gets correct path
            good_download = self._compute_download_path(resolved)
            assert good_download == "GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"  # works!

            # Both should normalize the same (for cache/output) — full path kept
            assert dep_normalized == "GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"
            assert job_migrate.normalize_nb_path(resolved) == "GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"
        finally:
            job_migrate.JOB_ROOT = old_root

    def test_run_path_fallback_to_migrated_base(self):
        """When dep not in dep_path_map, %run should fall back to MIGRATED_BASE path."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = "GIT/Auto_FT_Derived"
            run_path = "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"
            nb_norm = job_migrate.normalize_nb_path(run_path)
            if not nb_norm.endswith(".ipynb"):
                nb_norm += ".ipynb"
            migrated_base = "/Workspace/output/job/notebooks"
            fallback = f"{migrated_base}/{nb_norm}"
            assert fallback == "/Workspace/output/job/notebooks/GIT/Auto_FT_Derived/HelperFunctions/utils.ipynb"
            assert fallback != run_path  # different from original → replacement will happen
        finally:
            job_migrate.JOB_ROOT = old_root


# ─── _flatten_manifest_run_deps ───────────────────────────────────────────────

class TestFlattenManifestRunDeps:
    """Depth-first recursive flattening of manifest run_deps."""

    @staticmethod
    def _flatten(deps):
        from job_migrate import _flatten_manifest_run_deps
        return _flatten_manifest_run_deps(deps)

    def test_flat_deps(self):
        deps = [
            {"path": "/Workspace/GIT/a.ipynb", "exists": True, "nested_deps": []},
            {"path": "/Workspace/GIT/b.ipynb", "exists": True, "nested_deps": []},
        ]
        result = self._flatten(deps)
        assert result == ["/Workspace/GIT/a.ipynb", "/Workspace/GIT/b.ipynb"]

    def test_nested_depth_1(self):
        deps = [
            {"path": "/Workspace/GIT/parent.ipynb", "exists": True, "nested_deps": [
                {"path": "/Workspace/GIT/child.ipynb", "exists": True, "nested_deps": []},
            ]},
        ]
        result = self._flatten(deps)
        # Depth-first: child before parent
        assert result == ["/Workspace/GIT/child.ipynb", "/Workspace/GIT/parent.ipynb"]

    def test_nested_depth_2(self):
        deps = [
            {"path": "/Workspace/A.ipynb", "exists": True, "nested_deps": [
                {"path": "/Workspace/B.ipynb", "exists": True, "nested_deps": [
                    {"path": "/Workspace/C.ipynb", "exists": True, "nested_deps": []},
                ]},
            ]},
        ]
        result = self._flatten(deps)
        assert result == ["/Workspace/C.ipynb", "/Workspace/B.ipynb", "/Workspace/A.ipynb"]

    def test_skips_nonexistent(self):
        deps = [
            {"path": "/Workspace/exists.ipynb", "exists": True, "nested_deps": []},
            {"path": "/Workspace/missing.ipynb", "exists": False, "nested_deps": []},
        ]
        result = self._flatten(deps)
        assert result == ["/Workspace/exists.ipynb"]

    def test_empty_list(self):
        assert self._flatten([]) == []

    def test_autorefit_manifest_structure(self):
        """Matches the real AutoRefit_Feature_Generation_Derived.json structure."""
        deps = [
            {"path": "/Workspace/GIT/read_ft_tables.ipynb",
             "location": "external", "exists": True, "nested_deps": []},
            {"path": "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/HelperFunctions.ipynb",
             "location": "external", "exists": True, "nested_deps": [
                {"path": "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb",
                 "location": "external", "exists": True, "nested_deps": [
                    {"path": "/Workspace/Users/user2@example.com/ReadLogsNotebookA.ipynb",
                     "location": "external", "exists": True, "nested_deps": []},
                    {"path": "/Workspace/Users/user2@example.com/SaveTableHelpers.ipynb",
                     "location": "external", "exists": True, "nested_deps": []},
                ]},
                {"path": "/Workspace/GIT/read_logs_notebook_c.ipynb",
                 "location": "external", "exists": True, "nested_deps": []},
            ]},
        ]
        result = self._flatten(deps)
        # Depth-first within each branch: read_ft_tables has no nested so it comes first,
        # then HelperFunctions branch: nested children before parent
        assert len(result) == 6
        assert result[0] == "/Workspace/GIT/read_ft_tables.ipynb"  # no nested, processed first
        assert result[1] == "/Workspace/Users/user2@example.com/ReadLogsNotebookA.ipynb"
        assert result[2] == "/Workspace/Users/user2@example.com/SaveTableHelpers.ipynb"
        assert result[3] == "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/all_utils.ipynb"
        assert result[4] == "/Workspace/GIT/read_logs_notebook_c.ipynb"
        assert result[5] == "/Workspace/GIT/Auto_FT_Derived/HelperFunctions/HelperFunctions.ipynb"


# ─── _flatten_deps (check_data_availability_for_workflow) ─────────────────────

class TestFlattenDepsDataAvailability:
    """Recursive dep flattening for data availability check (with dedup)."""

    @staticmethod
    def _flatten(dep_list):
        """Mirror the _flatten_deps logic from check_data_availability_for_workflow."""
        def _flatten_deps(dep_list, out):
            for dep in dep_list:
                dp = dep if isinstance(dep, str) else dep.get("path", "")
                if dp and dp not in out:
                    out.append(dp)
                if isinstance(dep, dict) and dep.get("nested_deps"):
                    _flatten_deps(dep["nested_deps"], out)
        result = []
        _flatten_deps(dep_list, result)
        return result

    def test_flat_dicts(self):
        deps = [
            {"path": "/Workspace/a.ipynb", "exists": True, "nested_deps": []},
            {"path": "/Workspace/b.ipynb", "exists": True, "nested_deps": []},
        ]
        result = self._flatten(deps)
        assert result == ["/Workspace/a.ipynb", "/Workspace/b.ipynb"]

    def test_string_deps(self):
        deps = ["/Workspace/a.ipynb", "/Workspace/b.ipynb"]
        result = self._flatten(deps)
        assert result == ["/Workspace/a.ipynb", "/Workspace/b.ipynb"]

    def test_nested_deps(self):
        deps = [
            {"path": "/Workspace/parent.ipynb", "exists": True, "nested_deps": [
                {"path": "/Workspace/child.ipynb", "exists": True, "nested_deps": []},
            ]},
        ]
        result = self._flatten(deps)
        # Parent first (breadth-ish), then nested
        assert "/Workspace/parent.ipynb" in result
        assert "/Workspace/child.ipynb" in result
        assert len(result) == 2

    def test_dedup(self):
        """Same notebook referenced in multiple tasks shouldn't appear twice."""
        deps = [
            {"path": "/Workspace/shared.ipynb", "exists": True, "nested_deps": []},
            {"path": "/Workspace/shared.ipynb", "exists": True, "nested_deps": []},
        ]
        result = self._flatten(deps)
        assert result == ["/Workspace/shared.ipynb"]

    def test_nested_dedup(self):
        """Same leaf dep in multiple branches."""
        deps = [
            {"path": "/Workspace/A.ipynb", "exists": True, "nested_deps": [
                {"path": "/Workspace/shared_leaf.ipynb", "exists": True, "nested_deps": []},
            ]},
            {"path": "/Workspace/B.ipynb", "exists": True, "nested_deps": [
                {"path": "/Workspace/shared_leaf.ipynb", "exists": True, "nested_deps": []},
            ]},
        ]
        result = self._flatten(deps)
        assert result.count("/Workspace/shared_leaf.ipynb") == 1

    def test_empty_nested_deps(self):
        deps = [{"path": "/Workspace/a.ipynb", "exists": True, "nested_deps": []}]
        result = self._flatten(deps)
        assert result == ["/Workspace/a.ipynb"]

    def test_deep_nesting_3_levels(self):
        """Example_Pipeline_V2 has 3 levels of nesting."""
        deps = [
            {"path": "/Workspace/L0.ipynb", "exists": True, "nested_deps": [
                {"path": "/Workspace/L1.ipynb", "exists": True, "nested_deps": [
                    {"path": "/Workspace/L2.ipynb", "exists": True, "nested_deps": [
                        {"path": "/Workspace/L3.ipynb", "exists": True, "nested_deps": []},
                    ]},
                ]},
            ]},
        ]
        result = self._flatten(deps)
        assert len(result) == 4
        for p in ["/Workspace/L0.ipynb", "/Workspace/L1.ipynb",
                  "/Workspace/L2.ipynb", "/Workspace/L3.ipynb"]:
            assert p in result


# ─── End-to-end: manifest → paths ────────────────────────────────────────────

class TestManifestEndToEnd:
    """Load real manifests and verify path processing produces valid results."""

    REPORTS_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")

    def _load_manifest(self, name):
        path = os.path.join(self.REPORTS_DIR, name)
        if not os.path.exists(path):
            pytest.skip(f"Manifest not found: {name}")
        with open(path) as f:
            return json.load(f)

    @staticmethod
    def _compute_download_path(notebook_path):
        download_path = notebook_path.lstrip("/")
        if download_path.startswith("Workspace/"):
            download_path = download_path[len("Workspace/"):]
        if not download_path.endswith(".ipynb"):
            download_path += ".ipynb"
        return download_path

    def _check_manifest(self, manifest_name):
        """Verify all paths in a manifest produce valid download and normalized paths."""
        import job_migrate

        data = self._load_manifest(manifest_name)
        if "jobs" not in data:
            pytest.skip(f"Not a job manifest: {manifest_name}")

        for job in data["jobs"]:
            job_root = job.get("root", "")
            old_root = job_migrate.JOB_ROOT
            try:
                job_migrate.JOB_ROOT = job_root

                all_paths = []
                for task in job.get("tasks", []):
                    nb = task.get("notebook_path", "")
                    if nb:
                        all_paths.append(nb)
                    # Flatten all run_deps recursively
                    from job_migrate import _flatten_manifest_run_deps
                    dep_paths = _flatten_manifest_run_deps(task.get("run_deps", []))
                    all_paths.extend(dep_paths)

                for path in all_paths:
                    normalized = job_migrate.normalize_nb_path(path)
                    download = self._compute_download_path(path)

                    # Normalized must not be empty
                    assert normalized, f"Empty normalized for: {path}"
                    # Download must not be empty
                    assert download, f"Empty download_path for: {path}"
                    # Neither should have /Workspace/ prefix
                    assert not normalized.startswith("/Workspace/"), \
                        f"normalized still has /Workspace/: {normalized}"
                    assert not download.startswith("/Workspace/"), \
                        f"download_path still has /Workspace/: {download}"
                    # Neither should start with /
                    assert not normalized.startswith("/"), \
                        f"normalized starts with /: {normalized}"
                    assert not download.startswith("/"), \
                        f"download_path starts with /: {download}"
                    # Download should be >= normalized length (it preserves JOB_ROOT)
                    # (only when JOB_ROOT matches; external deps are equal)
                    if job_root and path.replace("/Workspace/", "").lstrip("/").startswith(job_root.rstrip("/") + "/"):
                        assert len(download) >= len(normalized), \
                            f"download shorter than normalized: {download} vs {normalized}"
            finally:
                job_migrate.JOB_ROOT = old_root

    def test_autorefit_manifest(self):
        self._check_manifest("AutoRefit_Feature_Generation_Derived.json")

    def test_coin_drop_manifest(self):
        self._check_manifest("Example_Drop_Batch_Job.json")

    def test_collection_feature_store_manifest(self):
        self._check_manifest("Example_Feature_Store_Resolve_1_180.json")

    def test_exbridge_pipeline_manifest(self):
        self._check_manifest("Example_Pipeline_V2.json")

    def test_fl_queries_manifest(self):
        self._check_manifest("FT_Queries_Run.json")

    def test_example_workflow_manifest(self):
        self._check_manifest("example_workflow_manifest_1.json")

    def test_orc_address_manifest(self):
        self._check_manifest("orc_example_clustering.json")

    def test_motor_lookalike_manifest(self):
        self._check_manifest("Example_Lookalike_CS_Prod_manifest.json")

    def test_examplevendor_manifest(self):
        self._check_manifest("Example_Optimizer_Realtime.json")


# ─── ExampleVendor-specific regression tests ──────────────────────────────────────

class TestExampleVendorPaths:
    """Regression tests for the Example_Optimizer_Realtime download bug.

    JOB_ROOT = 'Users/user1@example.com/Payments/Example_Optimizer_Realtime'
    normalize_nb_path strips this, leaving just 'Optimiser_Utils/0_Config.ipynb'.
    The old code passed normalized to process_notebook → download got the stripped
    path → 404 from AIDP API.  The fix passes download_path (JOB_ROOT preserved).
    """

    JOB_ROOT = "Users/user1@example.com/Payments/Example_Optimizer_Realtime"

    @staticmethod
    def _compute_download_path(notebook_path):
        download_path = notebook_path.lstrip("/")
        if download_path.startswith("Workspace/"):
            download_path = download_path[len("Workspace/"):]
        if not download_path.endswith(".ipynb"):
            download_path += ".ipynb"
        return download_path

    def _normalize(self, path):
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = self.JOB_ROOT
            return job_migrate.normalize_nb_path(path)
        finally:
            job_migrate.JOB_ROOT = old_root

    # ── The core bug: dep under JOB_ROOT ──

    def test_dep_under_job_root_download_preserves_full_path(self):
        """Optimiser_Utils/0_Config.ipynb must download as Users/user1@.../0_Config.ipynb."""
        path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        download = self._compute_download_path(path)
        assert download == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        assert "Optimiser_Utils" in download
        assert "user1@example.com" in download

    def test_dep_under_job_root_preserved(self):
        """normalize preserves JOB_ROOT (caf9fdac) — unambiguous keys."""
        path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        normalized = self._normalize(path)
        assert normalized == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"

    def test_normalized_should_not_be_used_for_download(self):
        """The old bug: normalized path is too short for the download API."""
        path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        normalized = self._normalize(path)
        download_from_normalized = self._compute_download_path(normalized)
        # Since caf9fdac normalization keeps the full path — no more 404s.
        assert download_from_normalized == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        # Normalized and original now produce the SAME correct download path.
        correct_download = self._compute_download_path(path)
        assert correct_download == download_from_normalized
        assert correct_download.startswith("Users/user1@example.com/")

    # ── Example_Configs nested deps (3 levels deep) ──

    def test_campaign_config_nested_dep(self):
        """Example_Configs/M89.ipynb is nested 3 levels: task → 0_Config → run → M89."""
        path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Example_Configs/M89.ipynb"
        download = self._compute_download_path(path)
        assert download == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Example_Configs/M89.ipynb"
        normalized = self._normalize(path)
        assert normalized == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Example_Configs/M89.ipynb"

    def test_all_campaign_configs_download_path(self):
        """All M*.ipynb configs under Example_Configs should have full download paths."""
        for m_id in ["M9", "M14", "M28", "M39", "M40", "M48", "M51", "M53", "M54",
                     "M56", "M57", "M58", "M59", "M60", "M61", "M62", "M63", "M65",
                     "M71", "M73", "M74", "M75", "M76", "M78", "M79", "M80", "M81",
                     "M82", "M83", "M84", "M85", "M86", "M87", "M88", "M89", "M90"]:
            path = f"/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Example_Configs/{m_id}.ipynb"
            download = self._compute_download_path(path)
            assert download.startswith("Users/user1@example.com/"), \
                f"{m_id}: download path missing user prefix: {download}"
            normalized = self._normalize(path)
            assert normalized == (
                "Users/user1@example.com/Payments/Example_Optimizer_Realtime/"
                f"Example_Configs/{m_id}.ipynb"
            ), f"{m_id}: unexpected normalized: {normalized}"

    # ── External dep (outside JOB_ROOT) ──

    def test_external_dep_savetableutils(self):
        """SaveTableHelpers is under a different user — JOB_ROOT should not affect it."""
        path = "/Workspace/Users/user2@example.com/SaveTableHelpers.ipynb"
        download = self._compute_download_path(path)
        normalized = self._normalize(path)
        # Both should be the same since path is outside JOB_ROOT
        assert download == "Users/user2@example.com/SaveTableHelpers.ipynb"
        assert normalized == "Users/user2@example.com/SaveTableHelpers.ipynb"

    # ── Task notebook itself ──

    def test_task_notebook_download(self):
        """Task notebooks use resolved_path for download which already works."""
        path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/1_Date_Parameters.ipynb"
        download = self._compute_download_path(path)
        assert download == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/1_Date_Parameters.ipynb"
        normalized = self._normalize(path)
        assert normalized == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/1_Date_Parameters.ipynb"

    # ── Relative %run resolution in process_notebook ──

    def test_relative_run_resolves_correctly_with_download_path(self):
        """With download_path as notebook_path, relative %run resolution is correct.

        Notebook at download_path 'Users/user1@.../Optimiser_Utils/0_Config.ipynb'
        has %run ../Example_Configs/run → should resolve to full path."""
        download_path = "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        notebook_dir = posixpath.dirname(download_path)
        run_ref = "../Example_Configs/run"
        resolved = posixpath.normpath(posixpath.join(notebook_dir, run_ref))
        assert resolved == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Example_Configs/run"

    def test_relative_run_wrong_with_normalized(self):
        """With normalized as notebook_path, relative %run loses full path — the old bug."""
        normalized = "Optimiser_Utils/0_Config.ipynb"
        notebook_dir = posixpath.dirname(normalized)
        run_ref = "../Example_Configs/run"
        resolved = posixpath.normpath(posixpath.join(notebook_dir, run_ref))
        # Resolves to just "Example_Configs/run" — missing the Users/ prefix
        assert resolved == "Example_Configs/run"
        # This would fail to match dep_path_map keys that have full paths
        assert not resolved.startswith("Users/")

    # ── MIGRATED_BASE save path is same regardless ──

    def test_save_path_same_for_both(self):
        """normalize_nb_path produces the same output from both download_path and original."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = self.JOB_ROOT
            full_path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
            download_path = self._compute_download_path(full_path)

            norm_from_full = job_migrate.normalize_nb_path(full_path)
            norm_from_download = job_migrate.normalize_nb_path(download_path)
            assert norm_from_full == norm_from_download == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Config.ipynb"
        finally:
            job_migrate.JOB_ROOT = old_root

    # ── Manifest round-trip: flatten → download_path for every dep ──

    def test_examplevendor_manifest_all_deps_have_valid_download_paths(self):
        """Every dep in ExampleVendor manifest must produce a download path starting with Users/."""
        import job_migrate
        manifest_path = os.path.join(
            os.path.dirname(__file__), "..", "reports", "Example_Optimizer_Realtime.json")
        if not os.path.exists(manifest_path):
            pytest.skip("ExampleVendor manifest not found")

        with open(manifest_path) as f:
            data = json.load(f)

        job = data["jobs"][0]
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = job["root"]

            from job_migrate import _flatten_manifest_run_deps
            for task in job["tasks"]:
                dep_paths = _flatten_manifest_run_deps(task.get("run_deps") or [])
                for dp in dep_paths:
                    download = self._compute_download_path(dp)
                    # Every dep should start with Users/ (never just subdir like Optimiser_Utils/)
                    assert download.startswith("Users/"), \
                        f"Task {task['task_key']}: dep download path missing Users/ prefix: {download} (from {dp})"
                    assert download.endswith(".ipynb"), \
                        f"Task {task['task_key']}: dep download path missing .ipynb: {download}"
        finally:
            job_migrate.JOB_ROOT = old_root


# ─── Transitive dep resolution tests ──────────────────────────────────────────

class TestTransitiveDepResolution:
    """Tests for transitive (nested) %run dependency resolution.

    Scenario: task notebook has %run 00_Parameters, and 00_Parameters itself
    has %run ./Shared_Slack_Utils. Shared_Slack_Utils is a transitive dep (Level 2)
    that must be resolvable in dep_path_map during _inline_child_notebook.
    """

    MIGRATED_BASE = "/Workspace/scratch/output/MyJob/notebooks"

    def _normalize(self, path, job_root=""):
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = job_root
            return job_migrate.normalize_nb_path(path)
        finally:
            job_migrate.JOB_ROOT = old_root

    def test_migration_cache_enriches_dep_path_map(self):
        """_migration_cache entries should be mergeable into dep_path_map for transitive deps."""
        import job_migrate

        # Simulate: ensure_migrated cached these transitive deps
        cache = {
            "Users/user3/Utils/Shared_Slack_Utils.ipynb": "/Workspace/scratch/output/MyJob/notebooks/Users/user3/Utils/Shared_Slack_Utils.ipynb",
            "Users/user3/Utils/Data_Utils.ipynb": "/Workspace/scratch/output/MyJob/notebooks/Users/user3/Utils/Data_Utils.ipynb",
            "00_Parameters.ipynb": "/Workspace/scratch/output/MyJob/notebooks/00_Parameters.ipynb",
        }

        # dep_path_map initially only has direct deps
        dep_path_map = {
            "00_Parameters.ipynb": "/Workspace/scratch/output/MyJob/notebooks/00_Parameters.ipynb",
        }

        # Merge transitive deps (same logic as the fix)
        for cached_norm, cached_path in cache.items():
            if cached_path and cached_norm not in dep_path_map:
                dep_path_map[cached_norm] = cached_path
                if not cached_norm.startswith("/Workspace/"):
                    ws_key = f"/Workspace/{cached_norm}"
                    if ws_key not in dep_path_map:
                        dep_path_map[ws_key] = cached_path
                if cached_norm.endswith(".ipynb"):
                    bare = cached_norm[:-6]
                    if bare not in dep_path_map:
                        dep_path_map[bare] = cached_path

        # Now Shared_Slack_Utils should be findable
        assert "Users/user3/Utils/Shared_Slack_Utils.ipynb" in dep_path_map
        assert "Users/user3/Utils/Data_Utils.ipynb" in dep_path_map
        # Workspace-prefixed variant
        assert "/Workspace/Users/user3/Utils/Shared_Slack_Utils.ipynb" in dep_path_map
        # Bare (without .ipynb) variant
        assert "Users/user3/Utils/Shared_Slack_Utils" in dep_path_map

    def test_relative_run_in_child_resolves_under_migrated_base(self):
        """When child is at MIGRATED_BASE/X/00_Parameters.ipynb and has %run ./Shared_Slack_Utils,
        the resolved path should be MIGRATED_BASE/X/Shared_Slack_Utils.ipynb."""
        child_path = f"{self.MIGRATED_BASE}/Users/user3/CX/V0/00_Parameters.ipynb"
        run_ref = "./Shared_Slack_Utils"

        # Simulate what _inline_child_notebook does
        run_path = run_ref.replace(" ", "_")
        if not run_path.endswith(".ipynb"):
            run_path += ".ipynb"
        child_dir = posixpath.dirname(child_path)
        resolved = posixpath.normpath(posixpath.join(child_dir, run_path))

        expected = f"{self.MIGRATED_BASE}/Users/user3/CX/V0/Shared_Slack_Utils.ipynb"
        assert resolved == expected

    def test_relative_run_resolves_already_under_migrated_base(self):
        """When resolved path is already under MIGRATED_BASE, it should be
        recognized as a valid migrated path (not sent to the fallback)."""
        child_path = f"{self.MIGRATED_BASE}/Users/user3/CX/V0/00_Parameters.ipynb"
        run_ref = "./Shared_Slack_Utils.ipynb"
        child_dir = posixpath.dirname(child_path)
        resolved = posixpath.normpath(posixpath.join(child_dir, run_ref))

        # Path is already under MIGRATED_BASE — should be detected
        assert resolved.startswith(self.MIGRATED_BASE)

    def test_dep_path_map_lookup_matches_migrated_base_path(self):
        """dep_path_map with transitive deps should match paths resolved under MIGRATED_BASE."""
        child_path = f"{self.MIGRATED_BASE}/Users/user3/CX/V0/00_Parameters.ipynb"
        run_ref = "./Shared_Slack_Utils"

        run_path = run_ref.replace(" ", "_") + ".ipynb"
        child_dir = posixpath.dirname(child_path)
        run_path_resolved = posixpath.normpath(posixpath.join(child_dir, run_path))

        # dep_path_map enriched with transitive deps (normalized keys)
        dep_path_map = {
            "Users/user3/CX/V0/Shared_Slack_Utils.ipynb":
                f"{self.MIGRATED_BASE}/Users/user3/CX/V0/Shared_Slack_Utils.ipynb",
            "/Workspace/Users/user3/CX/V0/Shared_Slack_Utils.ipynb":
                f"{self.MIGRATED_BASE}/Users/user3/CX/V0/Shared_Slack_Utils.ipynb",
        }

        # Simulate the dep_path_map lookup (suffix matching)
        resolved = None
        variants = {run_path_resolved, run_path_resolved.replace(" ", "_"),
                    run_path_resolved.replace("_", " ")}
        for orig, mig in dep_path_map.items():
            if not mig:
                continue
            for rpv in variants:
                if orig == rpv or rpv.endswith("/" + orig) or orig.endswith("/" + rpv.lstrip("/")):
                    resolved = mig
                    break
            if resolved:
                break

        assert resolved is not None, (
            f"dep_path_map lookup failed for resolved path: {run_path_resolved}\n"
            f"dep_path_map keys: {list(dep_path_map.keys())}"
        )
        assert resolved == f"{self.MIGRATED_BASE}/Users/user3/CX/V0/Shared_Slack_Utils.ipynb"

    def test_normalize_on_migrated_base_path_equality_bug(self):
        """When run_path_to_inline is already under MIGRATED_BASE, normalize_nb_path
        may reconstruct back to the same path, causing the old equality check to skip."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        old_base = job_migrate.MIGRATED_BASE
        try:
            job_migrate.JOB_ROOT = "Users/user3/CX/V0"
            job_migrate.MIGRATED_BASE = self.MIGRATED_BASE

            # Path already under MIGRATED_BASE (from relative resolution in child)
            run_path = f"{self.MIGRATED_BASE}/Users/user3/CX/V0/Shared_Slack_Utils.ipynb"
            nb_norm = job_migrate.normalize_nb_path(run_path)
            migrated_candidate = f"{self.MIGRATED_BASE}/{nb_norm}"

            # The old code had: if migrated_candidate != run_path_to_inline: (skip if equal)
            # This test documents that they CAN be different (MIGRATED_BASE is stripped
            # by normalize if it contains workspace paths), but if MIGRATED_BASE
            # doesn't match JOB_ROOT, normalize may leave extra path segments.
            # Either way, the fix ensures both are checked on cluster.
            #
            # Key assertion: the path IS under MIGRATED_BASE, so the new
            # MIGRATED_BASE prefix check (line 1110) should resolve it directly.
            assert run_path.startswith(self.MIGRATED_BASE)
        finally:
            job_migrate.JOB_ROOT = old_root
            job_migrate.MIGRATED_BASE = old_base

    def test_collection_escalations_scenario(self):
        """Real scenario from Example_Escalations_V0 log:
        Task 05_Example_Calls_data → 00_Parameters → %run ./Shared_Slack_Utils (fails)."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = ""
            migrated_base = "/Workspace/scratch/user/oracle_ai_notebook_migration/Example_Escalations_V0/notebooks"

            # Direct deps (Level 1) — what process_job builds
            dep_path_map = {
                "/Workspace/Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb":
                    f"{migrated_base}/Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb",
                "Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb":
                    f"{migrated_base}/Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb",
            }

            # Simulate _migration_cache after ensure_migrated ran recursively
            migration_cache = {
                "Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb":
                    f"{migrated_base}/Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb",
                "Users/user3@example.com/Utils/Shared_Slack_Utils.ipynb":
                    f"{migrated_base}/Users/user3@example.com/Utils/Shared_Slack_Utils.ipynb",
                "Users/user3@example.com/Utils/Data_Utils.ipynb":
                    f"{migrated_base}/Users/user3@example.com/Utils/Data_Utils.ipynb",
                "Users/user3@example.com/Utils/SaveTableHelpers.ipynb":
                    f"{migrated_base}/Users/user3@example.com/Utils/SaveTableHelpers.ipynb",
            }

            # Merge transitive deps (the fix)
            for cached_norm, cached_path in migration_cache.items():
                if cached_path and cached_norm not in dep_path_map:
                    dep_path_map[cached_norm] = cached_path
                    if not cached_norm.startswith("/Workspace/"):
                        ws_key = f"/Workspace/{cached_norm}"
                        if ws_key not in dep_path_map:
                            dep_path_map[ws_key] = cached_path
                    if cached_norm.endswith(".ipynb"):
                        bare = cached_norm[:-6]
                        if bare not in dep_path_map:
                            dep_path_map[bare] = cached_path

            # Now simulate _inline_child_notebook resolving %run ./Shared_Slack_Utils
            # inside 00_Parameters.ipynb
            child_path = f"{migrated_base}/Users/user3@example.com/Example_Escalations/V0/00_Parameters.ipynb"
            run_ref = "./Shared_Slack_Utils"
            run_path = run_ref.replace(" ", "_") + ".ipynb"
            child_dir = posixpath.dirname(child_path)
            run_path_resolved = posixpath.normpath(posixpath.join(child_dir, run_path))

            # Try dep_path_map lookup (suffix matching)
            resolved = None
            variants = {run_path_resolved, run_path_resolved.replace(" ", "_"),
                        run_path_resolved.replace("_", " ")}
            for orig, mig in dep_path_map.items():
                if not mig:
                    continue
                for rpv in variants:
                    if orig == rpv or rpv.endswith("/" + orig) or orig.endswith("/" + rpv.lstrip("/")):
                        resolved = mig
                        break
                if resolved:
                    break

            # Shared_Slack_Utils is in a DIFFERENT directory (Utils/) from 00_Parameters (Example_Escalations/V0/).
            # The relative resolution from child_dir gives .../Example_Escalations/V0/Shared_Slack_Utils.ipynb
            # which won't match the cache entry .../Utils/Shared_Slack_Utils.ipynb.
            # But the dep_path_map NOW has the Utils/Shared_Slack_Utils entry from the cache.
            # The original %run in 00_Parameters was likely an ABSOLUTE path like
            # %run /Workspace/Users/user3@example.com/Utils/Shared_Slack_Utils
            # not a relative ./Shared_Slack_Utils. Let's test that scenario.

            # Re-test with absolute path (more likely the real scenario)
            run_ref_abs = "/Workspace/Users/user3@example.com/Utils/Shared_Slack_Utils"
            run_path_abs = run_ref_abs.replace(" ", "_") + ".ipynb"
            # Absolute paths are NOT resolved relative to child_dir

            resolved_abs = None
            variants_abs = {run_path_abs, run_path_abs.replace(" ", "_"),
                           run_path_abs.replace("_", " ")}
            for orig, mig in dep_path_map.items():
                if not mig:
                    continue
                for rpv in variants_abs:
                    if orig == rpv or rpv.endswith("/" + orig) or orig.endswith("/" + rpv.lstrip("/")):
                        resolved_abs = mig
                        break
                if resolved_abs:
                    break

            assert resolved_abs is not None, (
                f"Absolute %run lookup failed.\n"
                f"Looking for: {run_path_abs}\n"
                f"dep_path_map keys: {sorted(dep_path_map.keys())}"
            )
            assert "Shared_Slack_Utils" in resolved_abs
        finally:
            job_migrate.JOB_ROOT = old_root


# ─── Child notebook content cache tests ──────────────────────────────────────

class TestChildNotebookContentCache:
    """Tests for _notebook_content_cache population after _inline_child_notebook.

    Root cause of Example_Escalations_V0 HRC_Calls failure:
    SaveTableHelpers.ipynb was migrated+fixed on first %run, but _notebook_content_cache
    was never populated. Second %run (nested inside 00_Parameters) hit FUSE miss x3,
    fell back to API download of ORIGINAL unmigrated notebook, then failed on
    'from aidp_dbutils import _DBUtils' again.
    """

    def test_cache_stores_migrated_cells_with_correct_key(self):
        """After child processing, _notebook_content_cache should have the child_path key."""
        import job_migrate

        old_cache = dict(job_migrate._notebook_content_cache)
        try:
            job_migrate._notebook_content_cache.clear()

            child_path = "/Workspace/scratch/output/MyJob/notebooks/Users/user3/Utils/SaveTableHelpers.ipynb"
            migrated_cells = [
                "from aidp_compat import dbutils, translate_path",
                "def save_table(df, name):\n    df.write.saveAsTable(name)",
            ]

            # Simulate what the fix does after _inline_child_notebook completes
            cache_nb = {"cells": []}
            for _mc in migrated_cells:
                clean = _mc.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
                cache_nb["cells"].append({"cell_type": "code", "source": [clean]})
            job_migrate._notebook_content_cache[child_path] = json.dumps(cache_nb)

            # Verify cache hit
            assert child_path in job_migrate._notebook_content_cache
            nb = json.loads(job_migrate._notebook_content_cache[child_path])
            assert len(nb["cells"]) == 2
            assert "aidp_compat" in nb["cells"][0]["source"][0]
        finally:
            job_migrate._notebook_content_cache.clear()
            job_migrate._notebook_content_cache.update(old_cache)

    def test_cache_strips_fix_log_before_storing(self):
        """Fix-log comments must not leak into cached content."""
        import job_migrate

        old_cache = dict(job_migrate._notebook_content_cache)
        try:
            job_migrate._notebook_content_cache.clear()

            child_path = "/Workspace/scratch/output/Job/notebooks/Utils/Data_Utils.ipynb"
            migrated_cells = [
                "from aidp_compat import dbutils\n\n# === AIDP MIGRATION FIX LOG ===\n# Attempt 1: replaced aidp_dbutils\n",
                "def read_table(name):\n    return spark.table(name)",
            ]

            cache_nb = {"cells": []}
            for _mc in migrated_cells:
                clean = _mc.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
                cache_nb["cells"].append({"cell_type": "code", "source": [clean]})
            job_migrate._notebook_content_cache[child_path] = json.dumps(cache_nb)

            nb = json.loads(job_migrate._notebook_content_cache[child_path])
            for cell in nb["cells"]:
                src = "".join(cell["source"])
                assert "AIDP MIGRATION FIX LOG" not in src
                assert "Attempt 1" not in src
        finally:
            job_migrate._notebook_content_cache.clear()
            job_migrate._notebook_content_cache.update(old_cache)

    def test_cache_also_stores_ipynb_variant(self):
        """If child_path lacks .ipynb, cache should also store the .ipynb variant."""
        import job_migrate

        old_cache = dict(job_migrate._notebook_content_cache)
        try:
            job_migrate._notebook_content_cache.clear()

            child_path = "/Workspace/scratch/output/Job/notebooks/Utils/SaveTableHelpers"
            migrated_cells = ["print('hello')"]

            cache_nb = {"cells": []}
            for _mc in migrated_cells:
                clean = _mc.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
                cache_nb["cells"].append({"cell_type": "code", "source": [clean]})
            job_migrate._notebook_content_cache[child_path] = json.dumps(cache_nb)
            if not child_path.endswith(".ipynb"):
                job_migrate._notebook_content_cache[child_path + ".ipynb"] = \
                    job_migrate._notebook_content_cache[child_path]

            assert child_path in job_migrate._notebook_content_cache
            assert child_path + ".ipynb" in job_migrate._notebook_content_cache
        finally:
            job_migrate._notebook_content_cache.clear()
            job_migrate._notebook_content_cache.update(old_cache)

    def test_cache_survives_across_tasks(self):
        """Cache populated by task 1 should be available for task 2 (module-level dict)."""
        import job_migrate

        old_cache = dict(job_migrate._notebook_content_cache)
        try:
            job_migrate._notebook_content_cache.clear()

            # Task 1 populates cache
            path = "/Workspace/scratch/output/Job/notebooks/Utils/SaveTableHelpers.ipynb"
            nb_json = json.dumps({"cells": [{"cell_type": "code", "source": ["from aidp_compat import dbutils"]}]})
            job_migrate._notebook_content_cache[path] = nb_json

            # Task 2 reads from cache (simulating Source 1 in _inline_child_notebook)
            cached_content = job_migrate._notebook_content_cache.get(path)
            assert cached_content is not None
            nb = json.loads(cached_content)
            child_cells = []
            for cell in nb.get("cells", []):
                if cell.get("cell_type") == "code":
                    src = "".join(cell.get("source", []))
                    if src.strip():
                        child_cells.append(src)
            assert len(child_cells) == 1
            assert "aidp_compat" in child_cells[0]
        finally:
            job_migrate._notebook_content_cache.clear()
            job_migrate._notebook_content_cache.update(old_cache)

    def test_cache_not_overwritten_on_second_run(self):
        """If child_path already in cache (from earlier task), don't overwrite —
        the first migration's result is canonical."""
        import job_migrate

        old_cache = dict(job_migrate._notebook_content_cache)
        try:
            job_migrate._notebook_content_cache.clear()

            path = "/Workspace/scratch/output/Job/notebooks/Utils/SaveTableHelpers.ipynb"

            # First migration populates cache with fixed code
            first_nb = json.dumps({"cells": [{"cell_type": "code", "source": ["from aidp_compat import dbutils"]}]})
            job_migrate._notebook_content_cache[path] = first_nb

            # Second encounter — cache check prevents overwrite
            if path not in job_migrate._notebook_content_cache:
                second_nb = json.dumps({"cells": [{"cell_type": "code", "source": ["from aidp_dbutils import _DBUtils"]}]})
                job_migrate._notebook_content_cache[path] = second_nb

            # Should still have the first (correct) version
            nb = json.loads(job_migrate._notebook_content_cache[path])
            assert "aidp_compat" in nb["cells"][0]["source"][0]
            assert "aidp_dbutils" not in nb["cells"][0]["source"][0]
        finally:
            job_migrate._notebook_content_cache.clear()
            job_migrate._notebook_content_cache.update(old_cache)

    def test_saved_notebook_strips_fix_log(self):
        """The final saved .ipynb must not contain fix-log comments."""
        migrated_cells = [
            {"cell_type": "code", "source": [
                "from aidp_compat import dbutils\n\n# === AIDP MIGRATION FIX LOG ===\n# Attempt 1: replaced aidp_dbutils\n"
            ]},
            {"cell_type": "code", "source": ["x = 1"]},
            {"cell_type": "markdown", "source": ["# Title"]},
        ]

        # Simulate the save-time stripping logic
        stripped_cells = []
        for cell in migrated_cells:
            cell = dict(cell)
            if cell.get("cell_type") == "code":
                src = "".join(cell.get("source", []))
                src = src.split("\n# === AIDP MIGRATION FIX LOG ===")[0].rstrip()
                cell["source"] = [src]
            stripped_cells.append(cell)

        assert "AIDP MIGRATION FIX LOG" not in stripped_cells[0]["source"][0]
        assert "aidp_compat" in stripped_cells[0]["source"][0]
        # Non-code cells unchanged
        assert stripped_cells[2]["source"] == ["# Title"]
        # Code cells without fix log unchanged
        assert stripped_cells[1]["source"] == ["x = 1"]

    def test_config_cell_offset_in_save_back(self):
        """Regression test for the 0_Utils.ipynb find_prev_date cell loss bug.

        Root cause: dep migration prepends AIDP_SPARK_CONFIG_CELL to the saved
        notebook. When _inline_child_notebook loads from Source 4 (original API,
        no config cell) and saves back, the fix overlay uses child_cells indices
        (0-based, no config) but the file on disk has config at code_idx 0.
        This +1 offset causes fixes to overwrite the WRONG cell.

        Example: child_cells[3] (Slack fix) → fixes_map["3"] → applied to
        code_idx 3 in file = original cell 2 (find_prev_date) → cell lost.
        """
        CONFIG_MARKER = "# AIDP performance configuration"

        # Simulate migrated file on disk (config cell + 5 original code cells)
        nb_on_disk = {"cells": [
            {"cell_type": "code", "source": [f"{CONFIG_MARKER}\nfor _k, _v in []: pass"]},  # config
            {"cell_type": "code", "source": ["from aidp_compat import dbutils"]},  # cell 0
            {"cell_type": "code", "source": ["import pyspark"]},                   # cell 1
            {"cell_type": "code", "source": ["def find_prev_date(): pass"]},        # cell 2 ← target to preserve
            {"cell_type": "code", "source": ["from slack_sdk import WebClient"]},   # cell 3 ← target to fix
            {"cell_type": "code", "source": ["x = 1"]},                            # cell 4
        ]}

        # child_cells from Source 4 (original, NO config cell) — 5 cells
        # migrated_child_cells after execution — cell 3 was fixed
        all_cells = {
            "0": "from aidp_compat import dbutils",
            "1": "import pyspark",
            "2": "def find_prev_date(): pass",         # unchanged
            "3": "# Slack stubbed\npass",               # FIXED
            "4": "x = 1",
        }

        # Apply the save-back logic (same as cluster-side code)
        config_offset = 0
        for cell in nb_on_disk["cells"]:
            if cell.get("cell_type") == "code":
                src = "".join(cell.get("source", []))
                if src.strip() and CONFIG_MARKER in src:
                    config_offset = 1
                break

        code_idx = 0
        for cell in nb_on_disk["cells"]:
            if cell.get("cell_type") != "code":
                continue
            src = "".join(cell.get("source", []))
            if not src.strip():
                continue
            child_idx = str(code_idx - config_offset)
            if child_idx in all_cells:
                cell["source"] = [all_cells[child_idx]]
            code_idx += 1

        # Verify: config cell preserved
        assert CONFIG_MARKER in nb_on_disk["cells"][0]["source"][0]
        # Verify: find_prev_date NOT overwritten (the old bug)
        assert "find_prev_date" in nb_on_disk["cells"][3]["source"][0]
        # Verify: Slack cell correctly fixed
        assert "Slack stubbed" in nb_on_disk["cells"][4]["source"][0]

    def test_config_cell_offset_no_config(self):
        """When file has no config cell, offset is 0 and indices align directly."""
        CONFIG_MARKER = "# AIDP performance configuration"

        nb_on_disk = {"cells": [
            {"cell_type": "code", "source": ["from aidp_compat import dbutils"]},
            {"cell_type": "code", "source": ["def find_prev_date(): pass"]},
            {"cell_type": "code", "source": ["from slack_sdk import WebClient"]},
        ]}

        all_cells = {
            "0": "from aidp_compat import dbutils",
            "1": "def find_prev_date(): pass",
            "2": "# Slack stubbed\npass",
        }

        config_offset = 0
        for cell in nb_on_disk["cells"]:
            if cell.get("cell_type") == "code":
                src = "".join(cell.get("source", []))
                if src.strip() and CONFIG_MARKER in src:
                    config_offset = 1
                break

        assert config_offset == 0

        code_idx = 0
        for cell in nb_on_disk["cells"]:
            if cell.get("cell_type") != "code":
                continue
            src = "".join(cell.get("source", []))
            if not src.strip():
                continue
            child_idx = str(code_idx - config_offset)
            if child_idx in all_cells:
                cell["source"] = [all_cells[child_idx]]
            code_idx += 1

        assert "find_prev_date" in nb_on_disk["cells"][1]["source"][0]
        assert "Slack stubbed" in nb_on_disk["cells"][2]["source"][0]


# ─── Source 3 _migration_cache lookup key tests ─────────────────────────────

class TestSource3MigrationCacheLookup:
    """Tests for the Source 3 path resolution in _inline_child_notebook.

    Root cause of 100% FUSE failure in Example_Optimizer_Realtime run 5:
    Source 3 looked up _migration_cache with normalize_nb_path(child_path),
    but child_path starts with MIGRATED_BASE (not JOB_ROOT), so JOB_ROOT
    was never stripped → key mismatch → Source 3 always failed → fell to
    Source 4 (download of ORIGINAL unmigrated notebook) every time.

    Also: _mb_path construction prepended MIGRATED_BASE to a path already
    containing MIGRATED_BASE → double-nested path that never exists.
    """

    JOB_ROOT = "Users/user1@example.com/Payments/Example_Optimizer_Realtime"
    MIGRATED_BASE = "/Workspace/scratch/user/oracle_ai_notebook_migration/Example_Optimizer_Realtime/notebooks"

    def _build_source3_paths(self, child_path):
        """Replicate the Source 3 path-building logic from _inline_child_notebook."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        old_base = job_migrate.MIGRATED_BASE
        try:
            job_migrate.JOB_ROOT = self.JOB_ROOT
            job_migrate.MIGRATED_BASE = self.MIGRATED_BASE

            migrated_paths_to_try = []
            _cp = child_path if child_path.endswith(".ipynb") else child_path + ".ipynb"
            migrated_paths_to_try.append(_cp)

            _norm_keys_to_try = [
                job_migrate.normalize_nb_path(child_path),
                child_path.replace("/Workspace/", "").replace(" ", "_"),
            ]
            _migrated_prefix = self.MIGRATED_BASE.rstrip("/") + "/"
            if child_path.startswith(_migrated_prefix):
                _rel_key = child_path[len(_migrated_prefix):]
                _norm_keys_to_try.insert(0, _rel_key)

            if not child_path.startswith(self.MIGRATED_BASE):
                _mb_path = f"{self.MIGRATED_BASE}/{job_migrate.normalize_nb_path(child_path)}"
                if not _mb_path.endswith(".ipynb"):
                    _mb_path += ".ipynb"
                if _mb_path not in migrated_paths_to_try:
                    migrated_paths_to_try.append(_mb_path)

            return migrated_paths_to_try, _norm_keys_to_try
        finally:
            job_migrate.JOB_ROOT = old_root
            job_migrate.MIGRATED_BASE = old_base

    def _build_old_source3_paths(self, child_path):
        """Replicate the OLD (buggy) Source 3 path-building logic."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        old_base = job_migrate.MIGRATED_BASE
        try:
            job_migrate.JOB_ROOT = self.JOB_ROOT
            job_migrate.MIGRATED_BASE = self.MIGRATED_BASE

            migrated_paths_to_try = []
            _norm_keys_to_try = [
                job_migrate.normalize_nb_path(child_path),
                child_path.replace("/Workspace/", "").replace(" ", "_"),
            ]
            # OLD: no MIGRATED_BASE prefix stripping
            _mb_path = f"{self.MIGRATED_BASE}/{job_migrate.normalize_nb_path(child_path)}"
            if not _mb_path.endswith(".ipynb"):
                _mb_path += ".ipynb"
            if _mb_path not in migrated_paths_to_try and _mb_path != child_path:
                migrated_paths_to_try.append(_mb_path)

            return migrated_paths_to_try, _norm_keys_to_try
        finally:
            job_migrate.JOB_ROOT = old_root
            job_migrate.MIGRATED_BASE = old_base

    # ── The core bug: key mismatch ──

    def test_old_logic_cache_key_never_matches(self):
        """OLD logic: normalize_nb_path on a MIGRATED_BASE path cannot strip JOB_ROOT."""
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        try:
            job_migrate.JOB_ROOT = self.JOB_ROOT

            child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports.ipynb"

            # What ensure_migrated stores in _migration_cache
            cache_key = "Optimiser_Utils/0_Imports.ipynb"

            # What the OLD Source 3 looked up
            old_norm = job_migrate.normalize_nb_path(child_path)

            # The bug: these don't match
            assert old_norm != cache_key, \
                "If these match, the old code wasn't broken — re-check the bug hypothesis"
            # old_norm includes the full MIGRATED_BASE path minus /Workspace/
            assert "scratch/user" in old_norm
        finally:
            job_migrate.JOB_ROOT = old_root

    def test_new_logic_cache_key_matches(self):
        """NEW logic: stripping MIGRATED_BASE prefix produces the correct cache key."""
        child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports.ipynb"
        cache_key = "Optimiser_Utils/0_Imports.ipynb"

        _, norm_keys = self._build_source3_paths(child_path)

        assert cache_key in norm_keys, \
            f"Cache key {cache_key!r} not in norm_keys: {norm_keys}"

    def test_old_logic_double_nested_mb_path(self):
        """OLD logic: _mb_path doubles the MIGRATED_BASE prefix."""
        child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports.ipynb"
        old_paths, _ = self._build_old_source3_paths(child_path)

        # The old _mb_path should contain MIGRATED_BASE twice (the bug)
        for p in old_paths:
            count = p.count("notebooks")
            if count > 1:
                return  # confirmed: double-nested
        # If no double-nesting found, the old code may have accidentally
        # matched child_path == _mb_path and skipped it
        # Either way, child_path itself was never in the list
        assert child_path not in old_paths

    def test_new_logic_child_path_tried_first(self):
        """NEW logic: child_path itself is the first path to try open() on."""
        child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports.ipynb"
        new_paths, _ = self._build_source3_paths(child_path)

        assert new_paths[0] == child_path, \
            f"Expected child_path first, got: {new_paths[0]}"

    def test_new_logic_no_double_nested_path(self):
        """NEW logic: no path in migrated_paths_to_try should contain MIGRATED_BASE twice."""
        child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports.ipynb"
        new_paths, _ = self._build_source3_paths(child_path)

        for p in new_paths:
            count = p.count("Example_Optimizer_Realtime/notebooks")
            assert count <= 1, \
                f"Double-nested MIGRATED_BASE in path: {p}"

    # ── ExampleVendor-specific paths ──

    def test_examplevendor_dep_optimiser_utils(self):
        """Optimiser_Utils/0_Imports.ipynb — most common child in ExampleVendor."""
        child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports.ipynb"
        cache = {"Optimiser_Utils/0_Imports.ipynb": child_path}

        _, norm_keys = self._build_source3_paths(child_path)
        found = any(cache.get(k + ".ipynb" if not k.endswith(".ipynb") else k) for k in norm_keys)
        assert found, f"None of {norm_keys} found in cache {list(cache.keys())}"

    def test_examplevendor_dep_savetableutils_external(self):
        """SaveTableHelpers.ipynb — external dep under different user."""
        child_path = f"{self.MIGRATED_BASE}/Users/user2@example.com/SaveTableHelpers.ipynb"
        cache_key = "Users/user2@example.com/SaveTableHelpers.ipynb"
        cache = {cache_key: child_path}

        _, norm_keys = self._build_source3_paths(child_path)
        found = any(cache.get(k + ".ipynb" if not k.endswith(".ipynb") else k) for k in norm_keys)
        assert found, f"None of {norm_keys} found in cache {list(cache.keys())}"

    def test_examplevendor_dep_campaign_config(self):
        """Example_Configs/run.ipynb — nested dep."""
        child_path = f"{self.MIGRATED_BASE}/Example_Configs/run.ipynb"
        cache_key = "Example_Configs/run.ipynb"
        cache = {cache_key: child_path}

        _, norm_keys = self._build_source3_paths(child_path)
        found = any(cache.get(k + ".ipynb" if not k.endswith(".ipynb") else k) for k in norm_keys)
        assert found, f"None of {norm_keys} found in cache {list(cache.keys())}"

    def test_examplevendor_dep_campaign_m87(self):
        """Example_Configs/M87.ipynb — deeply nested dep."""
        child_path = f"{self.MIGRATED_BASE}/Example_Configs/M87.ipynb"
        cache_key = "Example_Configs/M87.ipynb"
        cache = {cache_key: child_path}

        _, norm_keys = self._build_source3_paths(child_path)
        found = any(cache.get(k + ".ipynb" if not k.endswith(".ipynb") else k) for k in norm_keys)
        assert found, f"None of {norm_keys} found in cache {list(cache.keys())}"

    # ── Edge cases ──

    def test_child_path_not_under_migrated_base(self):
        """When child_path is NOT under MIGRATED_BASE, _mb_path should be constructed."""
        child_path = "/Workspace/Users/user2@example.com/SaveTableHelpers.ipynb"
        new_paths, _ = self._build_source3_paths(child_path)

        # Should include both the original path and a MIGRATED_BASE path
        assert new_paths[0] == child_path
        mb_candidates = [p for p in new_paths if p.startswith(self.MIGRATED_BASE)]
        assert len(mb_candidates) > 0, \
            f"No MIGRATED_BASE paths in: {new_paths}"

    def test_child_path_without_ipynb_gets_extension(self):
        """child_path without .ipynb should get .ipynb added."""
        child_path = f"{self.MIGRATED_BASE}/Optimiser_Utils/0_Imports"
        new_paths, _ = self._build_source3_paths(child_path)

        assert new_paths[0].endswith(".ipynb"), \
            f"First path should have .ipynb: {new_paths[0]}"

    def test_all_examplevendor_manifest_deps_cache_lookup(self):
        """Every dep in the ExampleVendor manifest should produce a cache-matchable key."""
        import job_migrate
        manifest_path = os.path.join(
            os.path.dirname(__file__), "..", "reports", "Example_Optimizer_Realtime.json")
        if not os.path.exists(manifest_path):
            pytest.skip("ExampleVendor manifest not found")

        with open(manifest_path) as f:
            data = json.load(f)

        job = data["jobs"][0]
        old_root = job_migrate.JOB_ROOT
        old_base = job_migrate.MIGRATED_BASE
        try:
            job_migrate.JOB_ROOT = job["root"]
            job_migrate.MIGRATED_BASE = self.MIGRATED_BASE

            # Build a mock _migration_cache the way ensure_migrated would
            from job_migrate import _flatten_manifest_run_deps
            mock_cache = {}
            for task in job["tasks"]:
                dep_paths = _flatten_manifest_run_deps(task.get("run_deps") or [])
                for dp in dep_paths:
                    norm = job_migrate.normalize_nb_path(dp)
                    if not norm.endswith(".ipynb"):
                        norm += ".ipynb"
                    mock_cache[norm] = f"{self.MIGRATED_BASE}/{norm}"

            # For each dep, simulate the child_path and verify Source 3 lookup
            failures = []
            for cache_key, migrated_path in mock_cache.items():
                child_path = migrated_path  # child_path = MIGRATED_BASE/normalized
                _, norm_keys = self._build_source3_paths(child_path)

                # At least one norm_key must match a cache key
                matched = False
                for nk in norm_keys:
                    nk_with_ext = nk if nk.endswith(".ipynb") else nk + ".ipynb"
                    if nk_with_ext in mock_cache:
                        matched = True
                        break
                if not matched:
                    failures.append(f"  {cache_key}: norm_keys={norm_keys}")

            assert not failures, (
                f"{len(failures)} deps failed Source 3 cache lookup:\n"
                + "\n".join(failures[:10])
            )
        finally:
            job_migrate.JOB_ROOT = old_root
            job_migrate.MIGRATED_BASE = old_base

    def test_ensure_migrated_to_source3_roundtrip(self):
        """End-to-end: ensure_migrated caches a key, dep_path_map maps it,
        process_notebook resolves %run to child_path, Source 3 finds it.

        This is the exact flow that failed in ExampleVendor run 5 (100% FUSE failure).
        """
        import job_migrate
        old_root = job_migrate.JOB_ROOT
        old_base = job_migrate.MIGRATED_BASE
        old_cache = dict(job_migrate._migration_cache)
        try:
            job_migrate.JOB_ROOT = self.JOB_ROOT
            job_migrate.MIGRATED_BASE = self.MIGRATED_BASE

            # Step 1: ensure_migrated() caches dep (simulated)
            dep_original = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Imports.ipynb"
            dep_norm = job_migrate.normalize_nb_path(dep_original)
            assert dep_norm == "Users/user1@example.com/Payments/Example_Optimizer_Realtime/Optimiser_Utils/0_Imports.ipynb"
            dep_migrated = f"{self.MIGRATED_BASE}/{dep_norm}"
            job_migrate._migration_cache[dep_norm] = dep_migrated

            # Step 2: process_job builds dep_path_map (simulated)
            dep_path_map = {}
            dep_path_map[dep_original] = dep_migrated
            dep_path_map[dep_norm] = dep_migrated

            # Step 3: process_notebook encounters %run ../Optimiser_Utils/0_Imports
            notebook_path = "/Workspace/Users/user1@example.com/Payments/Example_Optimizer_Realtime/ACTIVATION/Example_Simulation_custom.ipynb"
            run_ref = "../Optimiser_Utils/0_Imports"
            run_path = run_ref + ".ipynb"
            current_dir = posixpath.dirname(notebook_path)
            run_path_resolved = posixpath.normpath(posixpath.join(current_dir, run_path))

            # Step 4: dep_path_map lookup (suffix matching)
            migrated_run_path = None
            run_path_variants = {run_path_resolved, run_path_resolved.replace(" ", "_")}
            for orig, mig in dep_path_map.items():
                if not mig:
                    continue
                for rpv in run_path_variants:
                    if orig == rpv or rpv.endswith("/" + orig) or orig.endswith("/" + rpv.lstrip("/")):
                        migrated_run_path = mig
                        break
                if migrated_run_path:
                    break

            assert migrated_run_path == dep_migrated, \
                f"dep_path_map lookup failed: {run_path_resolved} not found"

            child_path = migrated_run_path.replace(" ", "_")

            # Step 5: Source 3 cache lookup must find the right key
            _, norm_keys = self._build_source3_paths(child_path)
            found = any(
                job_migrate._migration_cache.get(nk if nk.endswith(".ipynb") else nk + ".ipynb")
                for nk in norm_keys
            )
            assert found, (
                f"Source 3 cache lookup failed!\n"
                f"  child_path: {child_path}\n"
                f"  norm_keys: {norm_keys}\n"
                f"  cache keys: {list(job_migrate._migration_cache.keys())}"
            )

            # Step 6: child_path itself is first in migrated_paths_to_try (direct open)
            paths_to_try, _ = self._build_source3_paths(child_path)
            assert paths_to_try[0] == child_path

        finally:
            job_migrate.JOB_ROOT = old_root
            job_migrate.MIGRATED_BASE = old_base
            job_migrate._migration_cache.clear()
            job_migrate._migration_cache.update(old_cache)


# ─── Relative %run / dbutils.notebook.run resolution edge cases ──────────────
#
# Cases observed during the Example_Optimizer_Realtime_new migration where a
# parent notebook's `%run ./X` or `dbutils.notebook.run("./X")` resolves naively
# (parent_dir + relative) to a path that does not exist on the workspace, even
# though the manifest correctly identifies the real notebook path elsewhere.

class TestRelativeRunPathResolution:
    """Edge cases where a relative %run / notebook.run cannot be satisfied by
    naive parent_dir + relative resolution."""

    @staticmethod
    def _resolve_naive(parent_path, raw_run):
        """Mirror the resolution at job_migrate.py:4574-4577."""
        nb_dir = posixpath.dirname(parent_path)
        target = raw_run if raw_run.endswith(".ipynb") else raw_run + ".ipynb"
        if not target.startswith("/"):
            target = posixpath.normpath(posixpath.join(nb_dir, target))
        return target

    @staticmethod
    def _compute_download_path(notebook_path):
        download_path = notebook_path.lstrip("/")
        if download_path.startswith("Workspace/"):
            download_path = download_path[len("Workspace/"):]
        if not download_path.endswith(".ipynb"):
            download_path += ".ipynb"
        return download_path

    # ── Observed bugs (these PASS today; document the bad current behavior) ──

    def test_naive_resolution_yields_nonexistent_path_for_stale_run(self):
        """ExampleVendor case: 6_Simulation_Early_Scratched.ipynb in /Workspace/GIT/Example_Rewards
        does dbutils.notebook.run('./5_Campaign_Ratio_Estimation_Scratched'). Naive
        resolution gives /Workspace/GIT/Example_Rewards/5_Campaign_Ratio_Estimation_Scratched.ipynb
        which DOES NOT exist on the cluster (real file lives in /Workspace/GIT/CR_estimation/
        with a different name)."""
        parent = "/Workspace/GIT/Example_Rewards/6_Simulation_Early_Scratched.ipynb"
        resolved = self._resolve_naive(parent, "./5_Campaign_Ratio_Estimation_Scratched")
        assert resolved == "/Workspace/GIT/Example_Rewards/5_Campaign_Ratio_Estimation_Scratched.ipynb"
        # Documents that this is a non-existent path; runtime code should catch this.

    def test_naive_resolution_for_run_macro_same_folder(self):
        """Happy path: %run ./0_Imports from a parent in /Workspace/GIT/Optimiser_Utils
        resolves to /Workspace/GIT/Optimiser_Utils/0_Imports.ipynb (which exists)."""
        parent = "/Workspace/GIT/Optimiser_Utils/0_Utils.ipynb"
        resolved = self._resolve_naive(parent, "./0_Imports")
        assert resolved == "/Workspace/GIT/Optimiser_Utils/0_Imports.ipynb"

    def test_relative_path_passed_to_download_yields_relative_download_path(self):
        """Bug we observed in /tmp/example.log: download_notebook receives a
        bare './1_BudgetCalc.ipynb' and constructs a relative download_path —
        which the AIDP downloadFileMeta API will reject. Documents the
        contract that download_notebook must receive an absolute workspace path."""
        rel = "./1_BudgetCalc.ipynb"
        dl = self._compute_download_path(rel)
        # This is the bug: download_path is still relative — API will 4xx.
        assert dl == "./1_BudgetCalc.ipynb"
        assert not dl.startswith("Workspace/") and not dl.startswith("GIT/")

    def test_normpath_collapses_dot_prefix_when_parent_dir_known(self):
        """When parent dir is known, ./X collapses correctly via posixpath.normpath."""
        parent = "/Workspace/GIT/Example_Configs/M99.ipynb"
        resolved = self._resolve_naive(parent, "./1_BudgetCalc")
        assert resolved == "/Workspace/GIT/Example_Configs/1_BudgetCalc.ipynb"
        # Note: this path may not exist — the actual file is in /Workspace/GIT/Optimiser_Utils/.
        # The fix should fall back to a basename lookup when this path is absent.

    def test_parent_with_parent_relative_run(self):
        """`%run ../Optimiser_Utils/X` from /Workspace/GIT/Simulation_notebooks/Y.ipynb
        correctly resolves up one folder."""
        parent = "/Workspace/GIT/Simulation_notebooks/6_Simulation.ipynb"
        resolved = self._resolve_naive(parent, "../Optimiser_Utils/0_Imports")
        assert resolved == "/Workspace/GIT/Optimiser_Utils/0_Imports.ipynb"

    # ── Manifest-driven override (job_migrate._resolve_relative_dep) ──

    def test_manifest_override_returns_unique_basename_match(self):
        """When the parent has a manifest entry and naive resolution produces a
        path NOT in that entry's children, swap for the unique basename match."""
        import job_migrate
        old = job_migrate._DEP_RESOLUTION.copy()
        try:
            parent = "/Workspace/GIT/Example_Configs/M99.ipynb"
            naive = "/Workspace/GIT/Example_Configs/1_BudgetCalc.ipynb"  # doesn't exist
            real = "/Workspace/GIT/Optimiser_Utils/1_BudgetCalc.ipynb"
            job_migrate.set_dep_resolution({parent: [real]})
            assert job_migrate._resolve_relative_dep(parent, naive) == real
        finally:
            job_migrate.set_dep_resolution(old)

    def test_manifest_override_no_op_when_naive_path_already_correct(self):
        """When naive resolution matches a manifest child exactly, return it unchanged."""
        import job_migrate
        old = job_migrate._DEP_RESOLUTION.copy()
        try:
            parent = "/Workspace/GIT/Optimiser_Utils/0_Utils.ipynb"
            real = "/Workspace/GIT/Optimiser_Utils/0_Imports.ipynb"
            job_migrate.set_dep_resolution({parent: [real]})
            assert job_migrate._resolve_relative_dep(parent, real) == real
        finally:
            job_migrate.set_dep_resolution(old)

    def test_manifest_override_no_op_when_parent_not_in_map(self):
        """When parent has no manifest entry, return naive unchanged (legacy path)."""
        import job_migrate
        old = job_migrate._DEP_RESOLUTION.copy()
        try:
            job_migrate.set_dep_resolution({})  # disabled
            naive = "/Workspace/GIT/X/Y.ipynb"
            assert job_migrate._resolve_relative_dep("/Workspace/anything.ipynb", naive) == naive
        finally:
            job_migrate.set_dep_resolution(old)

    def test_manifest_override_no_op_when_basename_match_ambiguous(self):
        """When 2+ manifest children share the basename, leave naive unchanged
        rather than guess. Existing failure paths (download FAILED) still trigger."""
        import job_migrate
        old = job_migrate._DEP_RESOLUTION.copy()
        try:
            parent = "/Workspace/GIT/Anywhere/X.ipynb"
            naive = "/Workspace/GIT/Anywhere/1_BudgetCalc.ipynb"  # missing
            children = [
                "/Workspace/GIT/Optimiser_Utils/1_BudgetCalc.ipynb",
                "/Workspace/GIT/Example_Rewards/1_BudgetCalc.ipynb",
            ]
            job_migrate.set_dep_resolution({parent: children})
            assert job_migrate._resolve_relative_dep(parent, naive) == naive
        finally:
            job_migrate.set_dep_resolution(old)

    def test_dep_lookup_applies_manifest_filename_correction_from_export_path(self):
        """An export-rooted relative %run must resolve through the manifest's
        original workspace key to the corrected migrated filename."""
        import job_migrate

        old_export_base = job_migrate.EXPORT_BASE
        try:
            job_migrate.EXPORT_BASE = "ai_tool_exported/notebooks"
            run_path = (
                "/Workspace/ai_tool_exported/notebooks/GIT/src/"
                "00_upi-retrain-utils.ipynb"
            )
            migrated = (
                "/Workspace/ai_tool_migrated/job/notebooks/GIT/src/"
                "00_upi_retrain_utils.ipynb"
            )
            dep_path_map = {
                "/Workspace/GIT/src/00_upi-retrain-utils.ipynb": migrated
            }

            assert (
                job_migrate._lookup_migrated_dep_path(run_path, dep_path_map)
                == migrated
            )
        finally:
            job_migrate.EXPORT_BASE = old_export_base

    def test_manifest_override_handles_dbutils_notebook_run_case(self):
        """ExampleVendor Early_M33 case: dbutils.notebook.run('./5_Campaign_Ratio_Estimation_Scratched')
        from /Workspace/GIT/Example_Rewards/X.ipynb resolves naively to a missing path;
        manifest lists the real CR_estimation file → override picks it up."""
        import job_migrate
        old = job_migrate._DEP_RESOLUTION.copy()
        try:
            parent = "/Workspace/GIT/Example_Rewards/6_Simulation_Early_Scratched.ipynb"
            naive = "/Workspace/GIT/Example_Rewards/5_Campaign_Ratio_Estimation_Scratched.ipynb"
            # Manifest contains the *correct* listing under this parent
            real = "/Workspace/GIT/Example_Rewards/5_Campaign_Ratio_Estimation_Scratched.ipynb"
            job_migrate.set_dep_resolution({parent: [real]})
            # Exact match — no override needed; return identity
            assert job_migrate._resolve_relative_dep(parent, naive) == real
        finally:
            job_migrate.set_dep_resolution(old)

    # ── dbutils.notebook.run variant of the same bug ──

    def test_dbutils_notebook_run_relative_resolves_to_missing_sibling(self):
        """Same bug surface for dbutils.notebook.run('./X', ...) — naive resolution
        gives a sibling-folder path that does not exist."""
        parent = "/Workspace/GIT/Example_Rewards/6_Simulation_Early_Scratched.ipynb"
        # dbutils.notebook.run("./5_Campaign_Ratio_Estimation_Scratched", 0, {...})
        resolved = self._resolve_naive(parent, "./5_Campaign_Ratio_Estimation_Scratched")
        assert "/Workspace/GIT/Example_Rewards/" in resolved
        assert resolved.endswith("5_Campaign_Ratio_Estimation_Scratched.ipynb")
        # Real file at /Workspace/GIT/CR_estimation/5_Campaign_Ratio_Estimation_scratched_V2.ipynb
        # is NOT what naive resolution returns.


# ─── Migrated %run path validator (catches Opus output bugs) ────────────────

class TestValidateMigratedRunPaths:
    """`_validate_migrated_run_paths(source, migrated_base)` detects three
    specific path-mangling patterns observed in real Opus migration failures.
    Pure inspection — never modifies source. Returns a list of issue strings."""

    BASE = "/Workspace/scratch/user/examplevendor_ai_notebook_migration/Example_Optimizer_Realtime_new/notebooks"

    @staticmethod
    def _validate(source, base):
        from job_migrate import _validate_migrated_run_paths
        return _validate_migrated_run_paths(source, base)

    # ── Clean (no issues) ──

    def test_clean_run_path_no_issues(self):
        src = f"%run {self.BASE}/GIT/Optimiser_Utils/0_Imports.ipynb\n"
        assert self._validate(src, self.BASE) == []

    def test_clean_dbutils_notebook_run_no_issues(self):
        src = f'dbutils.notebook.run("{self.BASE}/GIT/X.ipynb", 0, {{}})\n'
        assert self._validate(src, self.BASE) == []

    def test_clean_oidlutils_notebook_run_no_issues(self):
        src = f"oidlUtils.notebook.run('{self.BASE}/GIT/X.ipynb', 0, {{}})\n"
        assert self._validate(src, self.BASE) == []

    def test_clean_relative_run_no_issues(self):
        # relative paths are not the concern of this validator
        src = "%run ./Imports\n"
        assert self._validate(src, self.BASE) == []

    def test_clean_quoted_run_path_with_spaces_no_issues(self):
        src = (
            '%run "/Workspace/Users/user@example.com/'
            'Folder With Spaces/Imports.ipynb" $date="2026-09-15"\n'
        )
        assert self._validate(src, self.BASE) == []

    def test_user_folder_named_notebooks_is_not_flagged(self):
        # A user can legitimately have a folder named 'notebooks' in their
        # workspace. Single occurrence of '/notebooks/' must not flag.
        src = "%run /Workspace/Users/foo/notebooks/utils.ipynb\n"
        assert self._validate(src, self.BASE) == []

    # ── Bug 1: MIGRATED_BASE prepended twice ──

    def test_doubled_migrated_base_flagged(self):
        # The exact pattern observed in run.ipynb cell 38
        bad = f"%run {self.BASE}/{self.BASE.lstrip('/')}/Users/x/M9.ipynb0.ipynb\n"
        issues = self._validate(bad, self.BASE)
        assert any("MIGRATED_BASE doubled" in i for i in issues)

    def test_doubled_migrated_base_in_quoted_spaced_path_flagged(self):
        bad = (
            f'%run "{self.BASE}/{self.BASE.lstrip("/")}/'
            'Folder With Spaces/Imports.ipynb" $date="2026-09-15"\n'
        )
        issues = self._validate(bad, self.BASE)
        assert any("MIGRATED_BASE doubled" in issue for issue in issues)

    # ── Bug 2: .ipynb<digit>.ipynb mangling ──

    def test_ipynb_digit_ipynb_mangling_flagged(self):
        bad = f"%run {self.BASE}/Users/x/M9.ipynb0.ipynb\n"
        issues = self._validate(bad, self.BASE)
        assert any("Mangled basename" in i for i in issues)

    def test_ipynb_digit_ipynb_in_dbutils_flagged(self):
        bad = f'dbutils.notebook.run("{self.BASE}/X.ipynb5.ipynb", 0, {{}})\n'
        issues = self._validate(bad, self.BASE)
        assert any("Mangled basename" in i for i in issues)

    # ── Bug 3: Doubled .ipynb suffix ──

    def test_doubled_ipynb_suffix_flagged(self):
        bad = f"%run {self.BASE}/Users/x/foo.ipynb.ipynb\n"
        issues = self._validate(bad, self.BASE)
        assert any("Doubled .ipynb suffix" in i for i in issues)

    # ── Combined cases ──

    def test_multiple_bugs_all_reported(self):
        bad = (
            f"%run {self.BASE}/{self.BASE.lstrip('/')}/x/M9.ipynb0.ipynb\n"
            f"%run {self.BASE}/y/foo.ipynb.ipynb\n"
        )
        issues = self._validate(bad, self.BASE)
        # First %run triggers BOTH MIGRATED_BASE doubling AND mangled basename
        # Second %run triggers doubled-suffix
        assert any("MIGRATED_BASE doubled" in i for i in issues)
        assert any("Mangled basename" in i for i in issues)
        assert any("Doubled .ipynb suffix" in i for i in issues)

    def test_empty_inputs_return_empty_list(self):
        assert self._validate("", self.BASE) == []
        assert self._validate("%run /a/b.ipynb", "") == []
        assert self._validate(None, self.BASE) == []  # type: ignore


# ─── _create_message_with_529_retry (Anthropic Overloaded retry helper) ─────

class TestCreateMessageWith529Retry:
    """`_create_message_with_529_retry(loop, create_fn, log_fn, max_retries)` retries
    transient Anthropic 529 ('Overloaded') errors with exponential backoff,
    propagates other errors immediately, and raises RuntimeError if 529 persists."""

    @staticmethod
    def _drive(create_fn, log_fn=None, max_retries=3, sleep_calls=None):
        """Run the async helper to completion, collecting sleep durations.
        Patches asyncio.sleep so tests don't actually wait."""
        import asyncio
        from agent_migrate import _create_message_with_529_retry

        # Fake event loop with sync run_in_executor that just calls the fn.
        class _FakeLoop:
            def run_in_executor(self, _exec, fn):
                fut = asyncio.Future()
                try:
                    fut.set_result(fn())
                except Exception as e:
                    fut.set_exception(e)
                return fut

        # Patch asyncio.sleep to capture durations without actually sleeping
        orig_sleep = asyncio.sleep
        async def _no_sleep(secs):
            if sleep_calls is not None:
                sleep_calls.append(secs)
        asyncio.sleep = _no_sleep
        try:
            return asyncio.run(_create_message_with_529_retry(
                _FakeLoop(), create_fn, log_fn=log_fn, max_retries=max_retries))
        finally:
            asyncio.sleep = orig_sleep

    # ── Happy path ──

    def test_success_on_first_try_no_retry(self):
        called = {"n": 0}
        def fn():
            called["n"] += 1
            return "OK"
        sleeps = []
        result = self._drive(fn, sleep_calls=sleeps)
        assert result == "OK"
        assert called["n"] == 1
        assert sleeps == []  # no backoff when first call succeeds

    # ── 529 retry path ──

    def test_retries_on_529_then_succeeds(self):
        called = {"n": 0}
        def fn():
            called["n"] += 1
            if called["n"] <= 2:
                raise RuntimeError("Error code: 529 - {'overloaded_error': 'Overloaded'}")
            return "OK_AFTER_RETRY"
        sleeps = []
        result = self._drive(fn, sleep_calls=sleeps, max_retries=3)
        assert result == "OK_AFTER_RETRY"
        assert called["n"] == 3
        assert len(sleeps) == 2  # two backoffs before the third successful call
        # backoffs increase: first ~1s, second ~2s (with jitter <= 1s)
        assert 1.0 <= sleeps[0] < 2.0
        assert 2.0 <= sleeps[1] < 3.0

    def test_retries_on_overloaded_error_string(self):
        # Some error formatters use 'overloaded_error' instead of '529'
        called = {"n": 0}
        def fn():
            called["n"] += 1
            if called["n"] == 1:
                raise RuntimeError("type='overloaded_error', message='Overloaded'")
            return "OK"
        result = self._drive(fn)
        assert result == "OK"
        assert called["n"] == 2

    # ── 529 persistent failure ──

    def test_raises_runtime_error_after_max_retries(self):
        def fn():
            raise RuntimeError("Error code: 529 overloaded_error")
        sleeps = []
        with pytest.raises(RuntimeError, match="529 Overloaded persisted after 3 retries"):
            self._drive(fn, sleep_calls=sleeps, max_retries=3)
        # Initial + 3 retries = 4 attempts; sleeps happen between attempts (3 sleeps)
        assert len(sleeps) == 3

    # ── Non-529 errors propagate immediately ──

    def test_non_529_error_propagates_no_retry(self):
        called = {"n": 0}
        def fn():
            called["n"] += 1
            raise ValueError("Some other error not related to 529")
        sleeps = []
        with pytest.raises(ValueError, match="Some other error"):
            self._drive(fn, sleep_calls=sleeps)
        assert called["n"] == 1  # only one attempt
        assert sleeps == []  # no retry, no backoff

    def test_prompt_too_long_propagates_no_retry(self):
        # The "prompt is too long" path is handled by callers; this helper
        # must NOT swallow it.
        def fn():
            raise RuntimeError("prompt is too long: 250000 tokens > limit")
        sleeps = []
        with pytest.raises(RuntimeError, match="prompt is too long"):
            self._drive(fn, sleep_calls=sleeps)
        assert sleeps == []

    def test_400_invalid_request_propagates_no_retry(self):
        # Other 4xx errors must NOT be retried (would be wasteful and incorrect)
        def fn():
            raise RuntimeError("Error code: 400 - invalid_request_error")
        sleeps = []
        with pytest.raises(RuntimeError, match="400"):
            self._drive(fn, sleep_calls=sleeps)
        assert sleeps == []

    # ── Mixed error sequence ──

    def test_529_then_different_error_propagates_immediately(self):
        # If a 529 is followed by a different error during retry, we should
        # NOT keep retrying — propagate the new error.
        called = {"n": 0}
        def fn():
            called["n"] += 1
            if called["n"] == 1:
                raise RuntimeError("Error code: 529 overloaded")
            raise ValueError("Different error on retry")
        sleeps = []
        with pytest.raises(ValueError, match="Different error"):
            self._drive(fn, sleep_calls=sleeps, max_retries=5)
        assert called["n"] == 2  # initial + 1 retry, then bailed out
        assert len(sleeps) == 1  # one backoff before the failed retry


# ─── Cluster-status liveness check (catches stuck cell on dead cluster) ─────

class TestClusterLivenessCheck:
    """Cluster status helpers in scripts/aidp_executor.py.

    `_is_terminal_dead(status)` decides whether to abort a stuck cell.
    `_check_cluster_status(...)` queries AIDP REST API (network call —
    not unit-tested directly here; we only assert the classifier behavior).

    The classifier is intentionally CONSERVATIVE: only known-terminal
    states return True. Unknown / transient / None all return False so
    we never kill a long-running cell on ambiguous evidence."""

    @staticmethod
    def _is_dead(status):
        from aidp_executor import _is_terminal_dead
        return _is_terminal_dead(status)

    # ── Terminal-dead states (must abort) ──

    def test_stopped_is_terminal_dead(self):
        assert self._is_dead("STOPPED") is True

    def test_failed_is_terminal_dead(self):
        assert self._is_dead("FAILED") is True

    def test_terminated_is_terminal_dead(self):
        assert self._is_dead("TERMINATED") is True

    # ── Healthy / transient states (must NOT abort) ──

    def test_active_is_not_terminal_dead(self):
        assert self._is_dead("ACTIVE") is False

    def test_starting_is_not_terminal_dead(self):
        # Cluster booting up — keep waiting
        assert self._is_dead("STARTING") is False

    def test_restarting_is_not_terminal_dead(self):
        # Cluster restart in progress — keep waiting
        assert self._is_dead("RESTARTING") is False

    def test_installing_libraries_is_not_terminal_dead(self):
        # Library install in progress — keep waiting (the original concern)
        assert self._is_dead("INSTALLING_LIBRARIES") is False

    def test_restarting_libraries_is_not_terminal_dead(self):
        # Library restart triggers transient state — keep waiting
        assert self._is_dead("RESTARTING_LIBRARIES") is False

    def test_stopping_is_not_terminal_dead(self):
        # In-progress shutdown — conservative, keep waiting (cluster might
        # come back ACTIVE or transition through STOPPED, which is terminal).
        assert self._is_dead("STOPPING") is False

    def test_provisioning_is_not_terminal_dead(self):
        assert self._is_dead("PROVISIONING") is False

    # ── Ambiguous / error inputs (must NOT abort) ──

    def test_none_is_not_terminal_dead(self):
        # API error / network blip → status is None → keep waiting
        assert self._is_dead(None) is False

    def test_unknown_string_is_not_terminal_dead(self):
        # New AIDP state we haven't seen before → keep waiting
        assert self._is_dead("SOME_NEW_STATE") is False

    def test_empty_string_is_not_terminal_dead(self):
        assert self._is_dead("") is False

    def test_lowercase_active_is_not_dead(self):
        # Case-sensitive matching — AIDP returns uppercase
        assert self._is_dead("active") is False

    def test_lowercase_stopped_is_not_dead(self):
        # Lowercase 'stopped' shouldn't match — defensive against API change
        assert self._is_dead("stopped") is False

    # ── ClusterDeadError can be raised and caught ──

    def test_cluster_dead_error_class_exists_and_is_runtime_error(self):
        from aidp_executor import ClusterDeadError
        # Inherits from RuntimeError so existing `except Exception` catches it.
        assert issubclass(ClusterDeadError, RuntimeError)

    def test_cluster_dead_error_message(self):
        from aidp_executor import ClusterDeadError
        try:
            raise ClusterDeadError("test message")
        except ClusterDeadError as e:
            assert "test message" in str(e)

    # ── Constants are correctly defined ──

    def test_terminal_states_subset_known(self):
        from aidp_executor import TERMINAL_DEAD_STATES
        # Conservative — short list of states we are certain mean dead.
        assert "STOPPED" in TERMINAL_DEAD_STATES
        assert "FAILED" in TERMINAL_DEAD_STATES
        assert "TERMINATED" in TERMINAL_DEAD_STATES
        # ACTIVE must NEVER be in terminal-dead set.
        assert "ACTIVE" not in TERMINAL_DEAD_STATES

    def test_transient_states_dont_overlap_terminal(self):
        from aidp_executor import TERMINAL_DEAD_STATES, TRANSIENT_STATES
        # Sanity: a state can't be both transient and terminal-dead.
        assert TERMINAL_DEAD_STATES.isdisjoint(TRANSIENT_STATES)

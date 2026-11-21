"""Save-back supplies OciFs for explicit consumers, never chooses the listing API.

Validates:
  - Unresolved dbutils.fs.ls calls stay unchanged for execution/model repair
  - Explicit OciFs.ls consumers retain their helper dependency (the P0 lesson)
  - Config path is parameterized (no hardcoded the pilot deployment path in the emitted helper)
  - Idempotent: no-op on a second pass
  - Python cells with `dbutils.fs.ls` remain UNTOUCHED (aidp_compat handles them)

The on-cluster Scala semantics of the helper itself (FileNotFoundException on
missing path, FileInfo shape, prefix vs object handling) were validated live
during design — see /tmp/test_scala_sdk_helper_v2.py in the design doc.

Usage:
    pytest tests/test_scala_oci_fs_ls_helper.py -v
"""
import ast
import os
import sys
import types

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO, "scripts/job_migrate.py")


def _extract(names):
    with open(SRC, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    kept = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Assign)):
            name = getattr(node, "name", None) or (
                node.targets[0].id if (isinstance(node, ast.Assign)
                                       and isinstance(node.targets[0], ast.Name))
                else None
            )
            if name in names:
                kept.append(node)
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name in ("re",):
                    kept.append(node); break
    mod = types.ModuleType("m")
    mod.__file__ = SRC
    src = ast.unparse(ast.Module(body=kept, type_ignores=[]))
    exec(src, mod.__dict__)
    return mod


_JM = _extract({
    "_AIDP_IMPORT_LINE",
    "OCI_CONFIG_PATH",
    "_oci_fs_ls_helper_source",
    "_SCALA_OCIFS_LS_RE",
    "_cell_looks_scala",
    "_scala_active_ranges",
    "_has_active_ocifs_ls",
    "_ensure_oci_fs_ls_helper",
})
_JM.tprint = print  # stub the module's progress logger
# Post-Session-1: OCI_CONFIG_PATH is None until main() populates it from the
# OCI_CONFIG_FILE env var. Stub with a fixture value so the extracted module's
# helper-injection code path finds it defined.
_JM.OCI_CONFIG_PATH = "/Workspace/test/oci_config"
# re is used in the extracted namespace; ensure importable
import re as _re_mod
_JM.re = _re_mod


def _cell(src):
    return {"cell_type": "code", "source": [src]}


def _md(src):
    return {"cell_type": "markdown", "source": [src]}


def _has_helper(cells):
    """After the P1c REPL-redef fix, the helper is wrapped in `object OciFs`
    (not top-level `def _oci_fs_ls`). Check for the object marker."""
    return any(
        "object OciFs" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )


def _has_rewrite(cells):
    return any(
        "OciFs.ls(" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    ) and not any(
        "dbutils.fs.ls(" in "".join(c.get("source", []))
        for c in cells if c.get("cell_type") == "code"
    )


# ═══════════════════════════════════════════════════════════════════════
def test_helper_source_interpolates_config_path():
    out = _JM._oci_fs_ls_helper_source("/some/other/Config")
    assert '"/some/other/Config"' in out, "config path must be interpolated"
    assert "connection-keys" not in out, \
        "hardcoded the pilot deployment path leaked into template"


def test_helper_source_uses_default_when_not_overridden():
    default = _JM.OCI_CONFIG_PATH
    out = _JM._oci_fs_ls_helper_source(default)
    assert f'"{default}"' in out


def test_helper_source_declares_expected_symbols():
    out = _JM._oci_fs_ls_helper_source("/x/y")
    for sym in ("object OciFs", "case class FileInfo", "def ls(uri", "lazy val client",
                "FileNotFoundException", "%scala"):
        assert sym in out, f"missing {sym!r} in helper source"


def test_helper_without_config_fails_loudly():
    with pytest.raises(RuntimeError, match="OCI config file path"):
        _JM._oci_fs_ls_helper_source(None)


# ═══════════════════════════════════════════════════════════════════════
def test_no_op_when_no_scala_dbutils_ls():
    cells = [_cell("import numpy as np"), _cell('print("hi")')]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert len(got) == 2, "should not have inserted a helper cell"
    assert not _has_helper(got)


def test_no_op_on_python_dbutils_fs_ls():
    """Python cells that use dbutils.fs.ls via aidp_compat MUST NOT trigger
    the Scala helper. That's aidp_compat's job."""
    cells = [_cell("dbutils.fs.ls('/foo')")]  # NO %scala prefix → python
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert not _has_helper(got)
    # source still references dbutils.fs.ls (not rewritten)
    assert "dbutils.fs.ls" in "".join(got[0].get("source", []))


@pytest.mark.parametrize("source", [
    '%scala\n// OciFs.ls("comment")\nprintln("ready")',
    '%scala\n/* outer /* nested */ OciFs.ls("comment") */\nprintln("ready")',
    '%scala\nval example = "OciFs.ls(path)"',
    '%scala\nval example = """OciFs.ls(path)"""',
    'OciFs.ls("python-object")',
])
def test_inert_or_python_ocifs_reference_does_not_inject(source):
    cells = [_cell(source)]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert len(got) == 1 and "".join(got[0]["source"]) == source
    assert not _has_helper(got)


@pytest.mark.parametrize("path", [
    "dbfs:/FileStore/output", "/dbfs/FileStore/output",
    "/Volumes/catalog/schema/artifacts/output", "oci://bucket@namespace/output",
])
def test_unresolved_scala_dbutils_call_is_never_rewritten(path):
    source = f'%scala\nval path = "{path}"\nval x = dbutils.fs.ls(path).toString'
    cells = [_cell(source)]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert len(got) == 1
    assert "".join(got[0]["source"]) == source
    assert not _has_helper(got)


def test_preserves_multiple_unresolved_calls_across_cells():
    cells = [
        _cell("%scala\nval a = dbutils.fs.ls(p1)"),
        _cell("%scala\nval b = dbutils.fs.ls(p2)"),
        _cell("%scala\nval c = if (dbutils.fs.ls(p3).nonEmpty) 1 else 0"),
    ]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert len(got) == len(cells)
    assert not _has_helper(got)
    assert all("dbutils.fs.ls(" in "".join(c["source"]) for c in got)


def test_idempotent_on_second_pass():
    """Running twice must not double-insert the helper or re-rewrite."""
    cells = [_cell("%scala\nval x = OciFs.ls(p)")]
    first = _JM._ensure_oci_fs_ls_helper(list(cells))
    n1 = len(first)
    second = _JM._ensure_oci_fs_ls_helper(list(first))
    assert len(second) == n1, "helper duplicated on second pass"
    # Only ONE helper cell in the final list
    helper_count = sum(1 for c in second if c.get("cell_type") == "code"
                       and "object OciFs" in "".join(c.get("source", [])))
    assert helper_count == 1


def test_ignores_markdown_cells():
    cells = [
        _md("# some markdown\nOciFs.ls('x')"),
        _cell("%scala\nOciFs.ls(p)"),
    ]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert _has_helper(got)
    # Markdown untouched
    assert got[0] == cells[0]


def test_helper_inserted_before_first_code_cell():
    """Helper must run before any user Scala cell that calls OciFs.ls."""
    cells = [_cell("%scala\nprintln(OciFs.ls(x).size)")]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    # The injected cell contains `object OciFs`; the user cell contains
    # `OciFs.ls(`. Helper's index must precede user's.
    helper_idx = next(i for i, c in enumerate(got)
                      if "object OciFs" in "".join(c.get("source", [])))
    user_idx = next(i for i, c in enumerate(got)
                    if "OciFs.ls(x)" in "".join(c.get("source", []))
                    and "object OciFs" not in "".join(c.get("source", [])))
    assert helper_idx < user_idx, \
        f"helper at index {helper_idx} must come before user cell at {user_idx}"


def test_matches_only_word_boundary():
    """Similarly named receivers must not request an unrelated OCI helper."""
    # These should NOT match:
    for src in [
        "%scala\nval x = xdbutils.fs.ls(p)",
        "%scala\nval x = mydbutils.fs.ls(p)",
        "%scala\nval x = MyOciFs.ls(p)",
    ]:
        cells = [_cell(src)]
        got = _JM._ensure_oci_fs_ls_helper(list(cells))
        assert not _has_helper(got), \
            f"false-positive rewrite on {src!r}"


def test_matches_across_whitespace():
    source = "%scala\nval x = OciFs.ls  (path)"
    cells = [_cell(source)]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert _has_helper(got), "helper should trigger on `OciFs.ls   (`"
    user = "".join(got[1].get("source", []))
    assert user == source


def test_custom_config_path_flows_through():
    """When _ensure_oci_fs_ls_helper is called with a specific config_path,
    the injected helper embeds that exact path (parameterization works)."""
    cells = [_cell("%scala\nval x = OciFs.ls(p)")]
    got = _JM._ensure_oci_fs_ls_helper(list(cells), config_path="/customer/keys/config")
    helper_src = "".join(got[0].get("source", []))
    assert '"/customer/keys/config"' in helper_src


# ═══════════════════════════════════════════════════════════════════════
# P0 lesson: a model-selected OciFs call still needs its runtime definition.
# ═══════════════════════════════════════════════════════════════════════
def test_p0_injection_triggers_when_cells_have_ocifs_ls_and_no_object():
    """The P0 scenario — an explicit helper consumer must retain its definition."""
    cells = [_cell('%scala\nval x = OciFs.ls("oci://a@b/c/")')]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert _has_helper(got), \
        "P0 regression: cell with OciFs.ls but no object OciFs was NOT injected"
    # Helper cell prepended, user cell shifts to index 1
    assert len(got) == 2
    assert "object OciFs" in "".join(got[0].get("source", []))


def test_p0_no_double_inject_when_object_already_present():
    """Positive-idempotent path — a cell already carrying the object OciFs
    definition (from prior save-back run) must not trigger another injection."""
    cells = [
        _cell('%scala\nobject OciFs { def ls(uri: String) = Seq.empty }'),
        _cell('%scala\nval x = OciFs.ls("oci://a@b/c/")'),
    ]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    assert len(got) == 2, "should NOT have inserted a second helper cell"
    # Count `object OciFs` occurrences across cells — must remain 1
    occurrences = sum(
        "object OciFs" in "".join(c.get("source", []))
        for c in got if c.get("cell_type") == "code"
    )
    assert occurrences == 1, f"expected 1 object OciFs cell, got {occurrences}"


def test_p0_mixed_dbutils_and_ocifs_triggers_once():
    """One helper for the explicit consumer; unresolved neighbours stay intact."""
    cells = [
        _cell('%scala\nval a = dbutils.fs.ls(p1)'),
        _cell('%scala\nval b = OciFs.ls(p2)'),
    ]
    got = _JM._ensure_oci_fs_ls_helper(list(cells))
    injections = sum(
        "object OciFs" in "".join(c.get("source", []))
        for c in got if c.get("cell_type") == "code"
    )
    assert injections == 1
    # Unresolved first cell must not acquire an OCI-only API during save-back.
    assert "%scala\nval a = dbutils.fs.ls(p1)" == "".join(got[1]["source"])
    # Second cell unchanged
    assert "OciFs.ls(p2)" in "".join(got[2].get("source", []))

"""Regression tests for persisted imports used by migrated Pandas displays."""

import asyncio
import json
import os
import sys
import types
from types import SimpleNamespace

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


@pytest.fixture(scope="module")
def jm():
    import job_migrate

    return job_migrate


def _cell(source):
    return {"cell_type": "code", "source": [source], "metadata": {}}


def _source(cell):
    return "".join(cell["source"])


def test_persists_import_for_converted_pandas_display(jm):
    cells = [
        _cell(
            "# pdf3.display()  # Original\n"
            "Idisplay(pdf3)  # Oracle tool modification: pandas DataFrame "
            "rendered via IPython.display"
        )
    ]

    finalized = jm._ensure_idisplay_import(cells)

    assert _source(finalized[0]).splitlines()[0].startswith(
        "from IPython.display import display as Idisplay"
    )
    assert "# pdf3.display()  # Original" in _source(finalized[0])
    assert "Idisplay(pdf3)" in _source(finalized[0])


def test_import_is_independent_of_existing_aidp_compat_import(jm):
    cells = [
        _cell("from aidp_compat import translate_path\nIdisplay(pdf3)")
    ]

    finalized = jm._ensure_idisplay_import(cells)

    assert "from IPython.display import display as Idisplay" in _source(finalized[0])
    assert "from aidp_compat import translate_path" in _source(finalized[0])


@pytest.mark.parametrize(
    ("magic", "expected_prefix"),
    [
        ("%python", ["%python"]),
        (
            "%pyspark",
            [
                "%python",
                "# Oracle tool modification: select AIDP Python interpreter; original:",
                "# %pyspark",
            ],
        ),
    ],
)
def test_python_magic_canonicalization_preserves_docstring_and_future_import(
    jm, magic, expected_prefix
):
    cells = [
        _cell(
            f"{magic}\n"
            '"""module documentation"""\n'
            "from __future__ import annotations\n"
            "Idisplay(pdf3)"
        )
    ]

    # The explicit Python magic must override a Scala notebook default.
    finalized = jm._ensure_idisplay_import(cells, native_lang="scala")
    lines = _source(finalized[0]).splitlines()

    assert lines[:len(expected_prefix)] == expected_prefix
    body_at = len(expected_prefix)
    assert lines[body_at] == '"""module documentation"""'
    assert lines[body_at + 1] == "from __future__ import annotations"
    assert lines[body_at + 2].startswith(
        "from IPython.display import display as Idisplay"
    )


@pytest.mark.parametrize("magic", ["%python", "%pyspark"])
def test_python_magic_ast_source_masks_declaration_with_trailing_comment(jm, magic):
    source = f"{magic} # explicit Python cell\nvalue = 1\n"

    parseable = jm._python_cell_ast_source(source)

    assert len(parseable.splitlines()[0]) == len(source.splitlines()[0])
    compile(parseable, "<python-magic-cell>", "exec")


def test_ignores_comments_and_strings(jm):
    cells = [_cell("# Idisplay(pdf3)\nmessage = 'Idisplay(pdf3)'")]

    assert jm._ensure_idisplay_import(cells) == cells


def test_is_idempotent_and_preserves_cell_count(jm):
    cells = [_cell("Idisplay(pdf3)")]

    once = jm._ensure_idisplay_import(cells)
    twice = jm._ensure_idisplay_import(once)

    assert twice == once
    assert len(twice) == 1
    assert _source(twice[0]).count(
        "from IPython.display import display as Idisplay"
    ) == 1


def test_child_source_shape_gets_same_persisted_import(jm):
    sources = ["# pdf3.display()  # Original\nIdisplay(pdf3)"]
    child_sources = jm._finalize_child_artifact_sources(sources)

    assert len(child_sources) == len(sources)
    assert "from IPython.display import display as Idisplay" in child_sources[0]


@pytest.mark.parametrize(
    ("magic", "expected_prefix"),
    [
        ("%python", ["%python"]),
        (
            "%pyspark",
            [
                "%python",
                "# Oracle tool modification: select AIDP Python interpreter; original:",
                "# %pyspark",
            ],
        ),
    ],
)
def test_scala_default_child_finalizer_canonicalizes_python_magic_and_import_order(
    jm, magic, expected_prefix
):
    child_sources = jm._finalize_child_artifact_sources(
        [f"{magic}\nIdisplay(pdf3)\n"], native_lang="scala"
    )
    lines = child_sources[0].splitlines()

    assert lines[0] == "%python"
    for expected_line in expected_prefix[1:]:
        assert expected_line in lines
    import_at = next(
        index for index, line in enumerate(lines)
        if line.startswith("from IPython.display import display as Idisplay")
    )
    body_at = lines.index("Idisplay(pdf3)")
    assert lines[import_at].startswith(
        "from IPython.display import display as Idisplay"
    )
    assert import_at < body_at


@pytest.mark.parametrize("preamble", ["%%time\n", "%load_ext autoreload\n", "!pwd\n", "%config InlineBackend.figure_format = 'retina'\n"])
def test_ipython_preambles_receive_import_before_first_use(jm, preamble):
    cells = [_cell(preamble + "Idisplay(pdf3)\n")]

    finalized = jm._ensure_idisplay_import(cells)
    source = _source(finalized[0])

    if preamble.startswith("%%"):
        assert source.startswith(preamble)
    assert source.index("from IPython.display import display as Idisplay") < source.index(
        "Idisplay(pdf3)"
    )
    jm._assert_final_notebook_cells(finalized)


def test_later_import_does_not_satisfy_first_use(jm):
    cells = [
        _cell("Idisplay(first)\n"),
        _cell("from IPython.display import display as Idisplay\nIdisplay(second)\n"),
    ]

    finalized = jm._ensure_idisplay_import(cells)

    assert _source(finalized[0]).startswith(
        "from IPython.display import display as Idisplay"
    )


@pytest.mark.parametrize(
    "source",
    [
        "if False:\n    from IPython.display import display as Idisplay\nIdisplay(pdf3)\n",
        "try:\n    from IPython.display import display as Idisplay\nexcept ImportError:\n    pass\nIdisplay(pdf3)\n",
        "def bind_display():\n    from IPython.display import display as Idisplay\nIdisplay(pdf3)\n",
        "class DisplayBinder:\n    from IPython.display import display as Idisplay\nIdisplay(pdf3)\n",
        "for _ in []:\n    from IPython.display import display as Idisplay\nIdisplay(pdf3)\n",
    ],
)
def test_nested_import_does_not_satisfy_first_use(jm, source):
    finalized = jm._ensure_idisplay_import([_cell(source)])

    assert _source(finalized[0]).startswith(
        "from IPython.display import display as Idisplay"
    )


def test_same_cell_import_after_use_does_not_satisfy_first_use(jm):
    source = (
        "Idisplay(first)\n"
        "from IPython.display import display as Idisplay\n"
    )

    finalized = jm._ensure_idisplay_import([_cell(source)])

    assert _source(finalized[0]).startswith(
        "from IPython.display import display as Idisplay"
    )


def test_top_level_import_before_use_is_accepted_unchanged(jm):
    source = "from IPython.display import display as Idisplay\nIdisplay(pdf3)\n"
    cells = [_cell(source)]

    assert jm._ensure_idisplay_import(cells) == cells


def test_conditional_import_false_pass_reproduction_executes_after_fix(jm, monkeypatch):
    display_module = types.ModuleType("IPython.display")
    display_module.display = lambda value: value
    ipython_module = types.ModuleType("IPython")
    ipython_module.display = display_module
    monkeypatch.setitem(sys.modules, "IPython", ipython_module)
    monkeypatch.setitem(sys.modules, "IPython.display", display_module)

    cells = [_cell(
        "if False:\n"
        "    from IPython.display import display as Idisplay\n"
        "Idisplay('resolved')\n"
    )]

    finalized = jm._ensure_idisplay_import(cells)
    namespace = {}
    exec(compile(_source(finalized[0]), "<idisplay-finalizer>", "exec"), namespace)

    assert callable(namespace["Idisplay"])


def test_native_scala_notebook_finalizes_explicit_python_cell(jm):
    cells = [
        _cell("val untouched = 1"),
        _cell("%python\nIdisplay(pdf3)\n"),
    ]

    finalized = jm._ensure_idisplay_import(cells, native_lang="scala")

    assert _source(finalized[0]) == "val untouched = 1"
    assert _source(finalized[1]).splitlines()[0] == "%python"
    assert "from IPython.display import display as Idisplay" in _source(finalized[1])


def test_crlf_is_preserved(jm):
    cells = [_cell("%%time\r\nIdisplay(pdf3)\r\n")]

    source = _source(jm._ensure_idisplay_import(cells)[0])

    assert "\n" not in source.replace("\r\n", "")


def test_python_marker_keeps_following_cell_magic_first(jm):
    cells = [_cell("%python\n%%time\nIdisplay(pdf3)\n")]

    source = _source(jm._ensure_idisplay_import(cells)[0])
    lines = source.splitlines()

    assert lines[0] == "%python"
    assert lines[1] == "%%time"
    assert lines[2].startswith("from IPython.display import display as Idisplay")
    assert lines[3] == "Idisplay(pdf3)"


def test_unparseable_python_fails_with_artifact_validation_error(jm):
    cells = [_cell("if True print('broken')\nIdisplay(pdf3)\n")]

    with pytest.raises(jm.FinalArtifactValidationError, match="no safe Python cell"):
        jm._ensure_idisplay_import(cells)


def test_child_finalizer_save_failure_is_error_and_cache_is_not_published(
    jm, monkeypatch, tmp_path
):
    path = tmp_path / "child.ipynb"
    original = {
        "cells": [{
            "cell_type": "code",
            "source": ["Idisplay(pdf3)"],
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        }],
        "metadata": {},
    }
    original_json = json.dumps(original)
    path.write_text(original_json, encoding="utf-8")

    async def summarize(*args, **kwargs):
        return "fixture display"

    class Session:
        cluster_id = "fixture"

        async def execute(self, code, **kwargs):
            if "__AIDP_CHILD_COUNT__" in code:
                return {"status": "ok", "outputs": [{
                    "type": "stream", "name": "stdout",
                    "text": "__AIDP_CHILD_COUNT__1\n",
                }]}
            if "all_cells = payload.get" in code:
                return {"status": "error", "outputs": []}
            return {"status": "ok", "outputs": []}

    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "_notebook_content_cache", {str(path): original_json})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])

    result = asyncio.run(jm._inline_child_notebook(
        str(path),
        Session(),
        SimpleNamespace(log=lambda *args: None),
        "fixture-job",
        "",
        "",
        "",
        {},
    ))

    assert result["status"] == "error"
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert jm._notebook_content_cache[str(path)] == original_json


def test_changed_child_without_migrated_destination_fails_before_cache_publish(
    jm, monkeypatch, tmp_path
):
    path = tmp_path / "outside" / "child.ipynb"
    path.parent.mkdir()
    original = {
        "cells": [{"cell_type": "code", "source": ["Idisplay(pdf3)"],
                   "metadata": {}, "outputs": [], "execution_count": None}],
        "metadata": {},
    }
    original_json = json.dumps(original)
    path.write_text(original_json, encoding="utf-8")

    async def summarize(*args, **kwargs):
        return "fixture display"

    class Session:
        cluster_id = "fixture"

        async def execute(self, code, **kwargs):
            return {"status": "ok", "outputs": []}

    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path / "migrated"))
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "_notebook_content_cache", {str(path): original_json})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])

    result = asyncio.run(jm._inline_child_notebook(
        str(path), Session(), SimpleNamespace(log=lambda *args: None),
        "fixture-job", "", "", "", {},
    ))

    assert result["status"] == "error"
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert jm._notebook_content_cache[str(path)] == original_json


def test_changed_non_slack_child_requires_verified_cell_count(
    jm, monkeypatch, tmp_path
):
    path = tmp_path / "child.ipynb"
    original = {
        "cells": [{"cell_type": "code", "source": ["Idisplay(pdf3)"],
                   "metadata": {}, "outputs": [], "execution_count": None}],
        "metadata": {},
    }
    original_json = json.dumps(original)
    path.write_text(original_json, encoding="utf-8")

    async def summarize(*args, **kwargs):
        return "fixture display"

    class Session:
        cluster_id = "fixture"

        async def execute(self, code, **kwargs):
            # Execution succeeds, but the count probe never emits its marker.
            return {"status": "ok", "outputs": []}

    monkeypatch.setattr(jm, "MIGRATED_BASE", str(tmp_path))
    monkeypatch.setattr(jm, "MIGRATE_OUTPUT", str(tmp_path / "output"))
    monkeypatch.setattr(jm, "DIRECT_EXECUTE", True)
    monkeypatch.setattr(jm, "STRICT_CELL_GATING", False)
    monkeypatch.setattr(jm, "_summarize_cell_code", summarize)
    monkeypatch.setattr(jm, "_notebook_content_cache", {str(path): original_json})
    monkeypatch.setattr(jm, "_cell_history", [])
    monkeypatch.setattr(jm, "_current_cell_notes", [])

    result = asyncio.run(jm._inline_child_notebook(
        str(path), Session(), SimpleNamespace(log=lambda *args: None),
        "fixture-job", "", "", "", {},
    ))

    assert result["status"] == "error"
    assert result["kind"] == "CHILD_PERSISTENCE"
    assert jm._notebook_content_cache[str(path)] == original_json


def test_parent_upload_failure_cannot_report_success(jm):
    source = open(jm.__file__, encoding="utf-8").read()
    block = source[source.index("Upload setup failed:"):
                   source.index("if redaction_error:", source.index("Upload setup failed:"))]

    assert "if not upload_ok:" in block
    assert "Failed to upload finalized notebook artifacts" in block

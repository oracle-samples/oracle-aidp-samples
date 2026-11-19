"""Regression coverage for comments inserted above cell magics."""

import os
import sys

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


def test_path_annotation_does_not_turn_run_magic_into_python(jm):
    run_source = (
        "# Oracle tool modification: path → migrated dependency\n"
        "%run /Workspace/migrated/utils_np.ipynb"
    )
    cells = [
        _cell(run_source),
        _cell('print(dbutils.fs.ls("/tmp"))'),
    ]

    imported = jm._ensure_dbutils_import(cells)
    finalized = jm._finalize_leading_cell_magics(imported)

    assert "from aidp_compat import" not in _source(finalized[0])
    assert "from aidp_compat import" in _source(finalized[1])
    assert _source(finalized[0]).splitlines() == [
        "%run /Workspace/migrated/utils_np.ipynb",
        "# Oracle tool modification: path → migrated dependency",
    ]
    jm._assert_final_notebook_cells(finalized)


def test_comment_prefixed_python_magic_is_promoted_before_dbutils_import(jm):
    source = (
        "# Oracle tool modification: retained annotation\n"
        "%python\n"
        "value = dbutils.widgets.get('value')"
    )

    imported = jm._ensure_dbutils_import([_cell(source)])
    finalized = jm._finalize_leading_cell_magics(imported)
    delivered = _source(finalized[0])

    assert delivered.splitlines()[0] == "%python"
    assert "from aidp_compat import" in delivered.splitlines()[1]
    assert "retained annotation" in delivered
    jm._assert_final_notebook_cells(finalized)


def test_child_artifact_finalizer_restores_run_magic_to_line_one(jm):
    source = (
        "# Oracle tool modification: path → migrated dependency\n"
        "%run /Workspace/migrated/utils_np.ipynb"
    )

    messages = []
    finalized = jm._finalize_child_artifact_sources(
        [source], log_fn=messages.append
    )

    assert finalized[0].splitlines() == [
        "%run /Workspace/migrated/utils_np.ipynb",
        "# Oracle tool modification: path → migrated dependency",
    ]
    assert messages == [
        "Restored leading cell magic in 1 final artifact cell(s) after "
        "annotation rewrites"
    ]


def test_finalizer_reports_every_dropped_tool_import_to_log_and_notes(jm):
    source = (
        "import pandas as pd  # Oracle tool modification: added missing import\n"
        "from collections import OrderedDict  # Oracle tool modification: added missing import\n"
        "%sql\n"
        "SELECT 1"
    )
    logs = []
    notes = []

    finalized = jm._finalize_leading_cell_magics(
        [_cell(source)], log_fn=logs.append, note_fn=notes.append
    )

    assert _source(finalized[0]) == "%sql\nSELECT 1"
    assert notes == [
        "Cell 0: removed tool-generated import invalid in %sql: "
        "import pandas as pd  # Oracle tool modification: added missing import",
        "Cell 0: removed tool-generated import invalid in %sql: "
        "from collections import OrderedDict  # Oracle tool modification: added missing import",
    ]
    assert logs[:-1] == notes
    assert logs[-1] == (
        "Restored leading cell magic in 1 final artifact cell(s) after "
        "annotation rewrites"
    )


def test_finalizer_reports_unresolved_advisory_without_mutating_source(jm):
    source = "value = 1\n%sql\nSELECT 1"
    logs = []
    notes = []

    finalized = jm._finalize_leading_cell_magics(
        [_cell(source)],
        log_fn=logs.append,
        note_fn=notes.append,
        native_lang="python",
    )

    expected = (
        "Cell 0: UNRESOLVED leading cell magic: %sql sits below code this "
        "tool did not write and cannot safely reorder — the cell will not be "
        "read as %sql"
    )
    assert _source(finalized[0]) == source
    assert logs == [expected]
    assert notes == [expected]


def test_child_finalizer_routes_drop_events_to_returned_note_sink(jm):
    source = (
        "import pandas as pd  # Oracle tool modification: added missing import\n"
        "%sql\n"
        "SELECT 1"
    )
    logs = []
    notes = []

    finalized = jm._finalize_child_artifact_sources(
        [source], log_fn=logs.append, note_fn=notes.append
    )

    assert finalized == ["%sql\nSELECT 1"]
    assert notes == [
        "Cell 0: removed tool-generated import invalid in %sql: "
        "import pandas as pd  # Oracle tool modification: added missing import"
    ]
    assert logs[0] == notes[0]


def test_parent_and_child_runtime_calls_wire_finalizer_notes(jm):
    source = open(jm.__file__, encoding="utf-8").read()
    child = source[
        source.index("async def _inline_child_notebook("):
        source.index("async def process_notebook(")
    ]
    parent = source[source.index("async def process_notebook("):]

    assert "note_fn=child_notes.append" in child
    assert "note_fn=migration_notes.append" in parent


def test_final_magic_promotion_is_idempotent_and_python_is_unchanged(jm):
    cells = [
        _cell(
            "# Oracle tool modification: path → migrated dependency\n"
            "%run /Workspace/migrated/utils_np.ipynb"
        ),
        _cell("# ordinary comment\nvalue = 1"),
    ]

    once = jm._finalize_leading_cell_magics(cells)
    twice = jm._finalize_leading_cell_magics(once)

    assert twice == once
    assert _source(once[1]) == _source(cells[1])


@pytest.mark.parametrize(
    "source",
    [
        "%%time\nvalue = 1",
        "%timeit sum(range(10))",
        "%env SAMPLE=value\nprint('ok')",
        "value = 1\n!pip install example-package",
    ],
)
def test_final_validation_accepts_ipython_magics(jm, source):
    cells = [_cell(source)]
    jm._assert_final_notebook_cells(cells)
    assert _source(cells[0]) == source


@pytest.mark.parametrize("magic", ["%md", "%md-sandbox"])
def test_final_validation_skips_markdown_magic_cells(jm, magic):
    source = f"{magic}\nExample command:\n!pip install demo_pkg"

    jm._assert_final_notebook_cells([_cell(source)])


def test_final_validation_exempts_only_exact_reported_invalid_source(jm):
    source = "try:\n    # pip install was neutralised\nSENTINEL = 42"

    jm._assert_final_notebook_cells(
        [_cell(source)], allowed_invalid_sources={source}
    )

    with pytest.raises(jm.FinalArtifactValidationError):
        jm._assert_final_notebook_cells(
            [_cell(source + "\nBROKEN =")], allowed_invalid_sources={source}
        )


def test_line_magic_does_not_hide_invalid_surrounding_python(jm):
    cells = [_cell("%env SAMPLE=value\nif broken syntax")]

    with pytest.raises(jm.FinalArtifactValidationError):
        jm._assert_final_notebook_cells(cells)


@pytest.mark.parametrize(
    "source",
    [
        'msg = ("hello %s"\n    % name)',
        "if (a\n    != b):\n    pass",
        'log.info("value=%s"\n    % (value,))',
        'value = """line one\n!"""',
    ],
)
def test_operator_first_continuations_are_not_masked_as_magics(jm, source):
    cells = [_cell(source)]

    jm._assert_final_notebook_cells(cells)

    assert _source(cells[0]) == source


def test_real_mid_cell_magic_is_masked_at_logical_statement_start(jm):
    source = "value = 1\n%timeit sum(range(10))\nprint(value)"

    jm._assert_final_notebook_cells([_cell(source)])


def test_shell_escape_with_shell_only_backslash_survives_tokenizer_failure(jm):
    source = "value = 1\n!find /tmp -name \\*.parquet\nprint(value)"

    jm._assert_final_notebook_cells([_cell(source)])


def test_fallback_does_not_mask_operator_continuation(jm, monkeypatch):
    source = "value = (10\n    % 3)"

    def broken_tokenizer(_readline):
        raise jm.tokenize.TokenError("forced", (1, 0))
        yield  # pragma: no cover - retain generator protocol

    monkeypatch.setattr(jm.tokenize, "generate_tokens", broken_tokenizer)

    assert jm._logical_ipython_magic_lines(source) == set()


@pytest.mark.parametrize(
    "comment",
    ["# see foo(", "# trailing \\", "# items +", "# values ["],
)
def test_fallback_ignores_continuation_tokens_inside_comments(
    jm, monkeypatch, comment
):
    source = f"value = 2 {comment}\n%sql select 1"

    def broken_tokenizer(_readline):
        raise jm.tokenize.TokenError("forced", (1, 0))
        yield  # pragma: no cover

    monkeypatch.setattr(jm.tokenize, "generate_tokens", broken_tokenizer)
    assert jm._logical_ipython_magic_lines(source) == {2}

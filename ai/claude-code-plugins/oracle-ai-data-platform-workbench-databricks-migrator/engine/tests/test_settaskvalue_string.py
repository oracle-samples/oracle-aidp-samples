"""Regression tests for AIDP task-value String coercion."""

import ast
import inspect
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


@pytest.fixture(scope="module")
def jm():
    import job_migrate
    return job_migrate


def _active_source(source, language="python"):
    marker = "//" if language == "scala" else "#"
    lines = []
    for line in source.splitlines():
        if line.lstrip().startswith(marker):
            continue
        lines.append(line.split(f"  {marker} Oracle tool modification:", 1)[0])
    return "\n".join(lines)


@pytest.mark.parametrize("value", ["None", "flag", "42", 'f"{db}.scores"'])
def test_python_task_values_are_stringified(jm, value):
    source = f'oidlUtils.parameters.setTaskValue("key", {value})'
    rewritten = jm._rewrite_dbutils_api_renames(source)
    assert _active_source(rewritten) == (
        f'oidlUtils.parameters.setTaskValue("key", str({value}))'
    )
    # Audit comments are applied once, at statement granularity, by the
    # artifact finalizer rather than inside an expression splice.
    assert "Oracle tool modification" not in rewritten


def test_databricks_setter_is_renamed_and_stringified(jm):
    source = 'dbutils.jobs.taskValues.set("fl_score_table", None)'
    rewritten = jm._rewrite_dbutils_api_renames(source)
    assert _active_source(rewritten) == (
        'oidlUtils.parameters.setTaskValue("fl_score_table", str(None))'
    )
    assert "Oracle tool modification" not in rewritten


@pytest.mark.parametrize(
    "source, expected_fragment",
    [
        ('if enabled:\n    dbutils.jobs.taskValues.set("k", flag)',
         '    oidlUtils.parameters.setTaskValue("k", str(flag))'),
        ('def publish():\n    dbutils.jobs.taskValues.set("k", flag)',
         '    oidlUtils.parameters.setTaskValue("k", str(flag))'),
        ('try:\n    dbutils.jobs.taskValues.set("k", flag)\nexcept Exception:\n    pass',
         '    oidlUtils.parameters.setTaskValue("k", str(flag))'),
        ('for flag in flags:\n    dbutils.jobs.taskValues.set("k", flag)',
         '    oidlUtils.parameters.setTaskValue("k", str(flag))'),
        ('dbutils.jobs.taskValues.set("k", flag); print("kept")',
         'oidlUtils.parameters.setTaskValue("k", str(flag)); print("kept")'),
        ('result = dbutils.jobs.taskValues.set("k", flag)',
         'result = oidlUtils.parameters.setTaskValue("k", str(flag))'),
        ('dbutils.notebook.exit(dbutils.jobs.taskValues.set("k", flag))',
         'aidputils.notebook.exit(oidlUtils.parameters.setTaskValue("k", str(flag)))'),
    ],
)
def test_artifact_annotation_preserves_statement_structure(jm, source, expected_fragment):
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )[0]["source"][0]

    active = _active_source(finalized)
    compile(active, "<finalized>", "exec")
    assert expected_fragment in active
    assert "dbutils.jobs.taskValues.set" in finalized
    assert finalized.count("original before") == 1


def test_crlf_annotation_preserves_newline_style(jm):
    source = (
        "if enabled:\r\n"
        '    dbutils.jobs.taskValues.set("k", flag)\r\n'
        "    print('kept')\r\n"
    )
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )[0]["source"][0]

    assert "\r\n" in finalized
    assert "\r\r\n" not in finalized
    compile(_active_source(finalized), "<finalized-crlf>", "exec")


def test_scala_exit_and_setter_are_both_rewritten(jm):
    source = (
        "%scala\n"
        'dbutils.jobs.taskValues.set("flag", enabled)\n'
        'dbutils.notebook.exit("done")'
    )
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )[0]["source"][0]
    active = _active_source(finalized, "scala")

    assert 'oidlUtils.parameters.setTaskValue("flag", String.valueOf(enabled))' in active
    assert 'oidlUtils.notebook.exit("done")' in active
    assert "dbutils.notebook.exit" not in active


def test_python_and_scala_rewrites_are_idempotent(jm):
    python_source = 'oidlUtils.parameters.setTaskValue("count", str(count))'
    scala_source = '%scala\ndbutils.jobs.taskValues.set("flag", enabled)'
    scala_expected = (
        '%scala\noidlUtils.parameters.setTaskValue("flag", String.valueOf(enabled))'
    )
    assert jm._rewrite_dbutils_api_renames(python_source) == python_source
    rewritten = jm._rewrite_dbutils_api_renames(scala_source)
    assert _active_source(rewritten, "scala") == scala_expected
    assert jm._rewrite_dbutils_api_renames(rewritten) == rewritten


def test_final_save_repairs_late_setter_without_mutating_input(jm):
    cells = [{"cell_type": "code", "source": [
        'oidlUtils.parameters.setTaskValue("a", None)'
    ]}]
    finalized = jm._finalize_taskvalue_setters(cells)
    assert _active_source(finalized[0]["source"][0]) == (
        'oidlUtils.parameters.setTaskValue("a", str(None))'
    )
    assert 'oidlUtils.parameters.setTaskValue("a", None)' in finalized[0]["source"][0]
    assert "# Oracle tool modification: original before" in finalized[0]["source"][0]
    assert jm._finalize_taskvalue_setters(finalized) == finalized
    assert cells[0]["source"] == [
        'oidlUtils.parameters.setTaskValue("a", None)'
    ]
    jm._assert_taskvalue_setters_finalized(finalized)


@pytest.mark.parametrize(
    "value_expression, expected",
    [
        ('"""line one\nline two"""', "line one\nline two"),
        ('f"""line {number}\nnext"""', "line 7\nnext"),
        ('[\n    "one",\n    "two",\n]', "['one', 'two']"),
        ('{\n    "answer": 42,\n}', "{'answer': 42}"),
        ('json.dumps({\n    "answer": 42,\n})', '{"answer": 42}'),
        ('"|".join([\n    "one",\n    "two",\n])', "one|two"),
        ('(\n    "implicit "\n    "concatenation"\n)', "implicit concatenation"),
    ],
)
def test_multiline_taskvalue_payload_is_preserved_and_executable(
    jm, value_expression, expected
):
    source = (
        "import json\n"
        "number = 7\n"
        "oidlUtils.parameters.setTaskValue(\n"
        '    "key",\n'
        f"    {value_expression},\n"
        ")"
    )
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )
    captured = {}
    bridge = SimpleNamespace(
        parameters=SimpleNamespace(
            setTaskValue=lambda key, value: captured.update({key: value})
        )
    )

    exec(finalized[0]["source"][0], {"oidlUtils": bridge})

    assert captured == {"key": expected}
    assert jm._finalize_taskvalue_setters(finalized) == finalized
    jm._assert_taskvalue_setters_finalized(finalized)


def test_scala_multiline_setter_ignores_argument_comments(jm):
    source = (
        "%scala\n"
        "oidlUtils.parameters.setTaskValue(\n"
        '  "key", // value follows\n'
        '  """line one\nline two"""\n'
        ")"
    )

    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}], native_lang="scala"
    )
    active = _active_source(finalized[0]["source"][0], "scala")

    assert "String.valueOf" in active
    assert '"""line one\nline two"""' in active
    jm._assert_taskvalue_setters_finalized(finalized, native_lang="scala")


@pytest.mark.parametrize("magic", ["%md", "%sql", "%r", "%sh"])
def test_finalizer_does_not_rewrite_non_python_cells(jm, magic):
    source = magic + '\noidlUtils.parameters.setTaskValue("k", value)'
    cells = [{"cell_type": "code", "source": [source]}]

    finalized = jm._finalize_taskvalue_setters(cells)

    assert finalized == cells


def test_magic_detection_requires_a_token_boundary(jm):
    source = (
        '%runtime_magic\n'
        'oidlUtils.parameters.setTaskValue("k", value)'
    )

    assert jm._cell_language(source) == "python"


def test_finalizer_uses_notebook_native_scala_language(jm):
    cells = [{"cell_type": "code", "source": [
        'oidlUtils.parameters.setTaskValue("k", value)'
    ]}]

    finalized = jm._finalize_taskvalue_setters(cells, native_lang="scala")

    assert _active_source(finalized[0]["source"][0], "scala") == (
        'oidlUtils.parameters.setTaskValue("k", String.valueOf(value))'
    )
    assert "// Oracle tool modification: original before" in finalized[0]["source"][0]


@pytest.mark.parametrize(
    "source",
    [
        'text = \'oidlUtils.parameters.setTaskValue("k", value)\'',
        '"""oidlUtils.parameters.setTaskValue("k", value)"""',
        '# oidlUtils.parameters.setTaskValue("k", value)',
    ],
)
def test_finalizer_ignores_strings_docstrings_and_comments(jm, source):
    cells = [{"cell_type": "code", "source": [source]}]

    assert jm._finalize_taskvalue_setters(cells) == cells


def test_final_artifact_validation_rejects_syntax_errors(jm):
    cells = [{"cell_type": "code", "source": ['ruta_año = "ok"\nthis is (']}]

    with pytest.raises(jm.FinalArtifactValidationError, match="invalid Python"):
        jm._assert_final_notebook_cells(cells)


def test_child_artifact_sources_receive_the_same_finalizers(jm):
    sources = [
        'location = "/FileStore/tables/x"',
        'value = "/dbfs" + location',
        'oidlUtils.parameters.setTaskValue("path", value)',
    ]

    finalized = jm._finalize_child_artifact_sources(sources)

    assert [_active_source(source) for source in finalized] == [
        'location = "/Volumes/default/default/dbfs/FileStore/tables/x"',
        "value = location",
        'oidlUtils.parameters.setTaskValue("path", str(value))',
    ]
    assert all("Oracle tool modification:" in source for source in finalized)


def test_child_artifact_sources_respect_native_scala(jm):
    finalized = jm._finalize_child_artifact_sources(
        ['oidlUtils.parameters.setTaskValue("k", value)'],
        native_lang="scala",
    )

    assert [_active_source(source, "scala") for source in finalized] == [
        'oidlUtils.parameters.setTaskValue("k", String.valueOf(value))'
    ]


def test_raw_pyspark_child_uses_python_finalizers_before_canonicalization(jm):
    finalized = jm._finalize_child_artifact_sources(
        [
            '%pyspark\nlocation = "/FileStore/tables/x"\n',
            '%pyspark\noidlUtils.parameters.setTaskValue("k", value)\n',
        ],
        native_lang="scala",
    )

    assert all(source.startswith("%python\n") for source in finalized)
    assert all("# %pyspark" in source for source in finalized)
    assert "/Volumes/default/default/dbfs/FileStore/tables/x" in finalized[0]
    active_setter = _active_source(finalized[1])
    assert 'oidlUtils.parameters.setTaskValue("k", str(value))' in active_setter
    assert "String.valueOf(value)" not in active_setter


def test_child_artifact_sources_detect_comment_prefixed_scala_magic(jm):
    finalized = jm._finalize_child_artifact_sources(
        [
            '# Oracle tool modification: audit annotation\n'
            '%scala\n'
            'oidlUtils.parameters.setTaskValue("k", value)\n'
        ],
        native_lang="python",
    )

    source = finalized[0]
    assert source.startswith("%scala\n")
    assert 'oidlUtils.parameters.setTaskValue("k", String.valueOf(value))' in (
        _active_source(source, "scala")
    )
    jm._assert_taskvalue_setters_finalized(
        [{"cell_type": "code", "source": [source]}],
        native_lang="python",
    )


def test_artifact_validation_error_is_reraised_by_upload_guard(jm):
    tree = ast.parse(inspect.getsource(jm.process_notebook))
    handlers = [
        handler
        for node in ast.walk(tree)
        if isinstance(node, ast.Try)
        for handler in node.handlers
        if isinstance(handler.type, ast.Name)
        and handler.type.id == "FinalArtifactValidationError"
    ]

    assert handlers
    assert any(isinstance(node, ast.Raise) for node in ast.walk(handlers[0]))


def test_dependency_preparation_uses_shared_fail_closed_helper(jm):
    source = inspect.getsource(jm._process_job_inner)

    # Task scan, manifest walk, and resumed-task descendant verification all
    # use the same fail-closed helper.
    assert source.count("await _prepare_task_dep(") == 3


@pytest.mark.parametrize(
    "source",
    [
        'result = \\\n+    dbutils.jobs.taskValues.set("k", flag)',
        'dbutils.jobs.taskValues.set(\\\n+    "k", flag)',
        'dbutils.jobs.taskValues.set(\n    "k",\n    str(\\\n+        flag),\n)',
    ],
)
def test_taskvalue_annotation_preserves_backslash_continuations(jm, source):
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )
    active = _active_source(finalized[0]["source"][0])

    compile(active, "<taskvalue-backslash>", "exec")
    assert "oidlUtils.parameters.setTaskValue" in active
    jm._assert_taskvalue_setters_finalized(finalized)


@pytest.mark.parametrize(
    "source",
    [
        'dbutils.jobs.taskValues.set(\n    "k", value=flag,  # operator\'s note\n)',
        'oidlUtils.parameters.setTaskValue(\n    "k", value=flag,  # note\n)',
    ],
)
def test_inline_comments_do_not_bypass_taskvalue_contract(jm, source):
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )
    active = _active_source(finalized[0]["source"][0])

    assert 'oidlUtils.parameters.setTaskValue("k", str(flag))' in active
    jm._assert_taskvalue_setters_finalized(finalized)


@pytest.mark.parametrize(
    "source",
    [
        'dbutils.jobs.taskValues.set("k", flag, unknown=True)',
        'dbutils.jobs.taskValues.set(*args)',
        'oidlUtils.parameters.setTaskValue(**kwargs)',
    ],
)
def test_unprovable_taskvalue_shapes_fail_closed(jm, source):
    cells = [{"cell_type": "code", "source": [source]}]

    with pytest.raises(
        jm.FinalArtifactValidationError, match="unresolved task-value setter"
    ):
        jm._assert_taskvalue_setters_finalized(cells)


def test_finalizer_uses_specific_exit_audit_reason(jm):
    source = 'dbutils.notebook.exit("done")'
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )[0]["source"][0]

    assert "translated Databricks notebook exit to the AIDP runtime" in finalized
    assert "coerced cross-task value" not in finalized


def test_finalizer_uses_specific_legacy_dbutils_audit_reason(jm):
    source = "from aidp_dbutils import _DBUtils\ndbutils = _DBUtils()"
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )[0]["source"][0]

    assert "replaced the legacy aidp_dbutils shim with aidp_compat" in finalized
    assert "coerced cross-task value" not in finalized


@pytest.mark.parametrize(
    "source",
    [
        'x = 1\n!ls *\ndbutils.jobs.taskValues.set("k", flag)',
        'dbutils.jobs.taskValues.set("k", flag)\n!find /dbfs -name \\*.parquet',
        '!ls *\noidlUtils.parameters.setTaskValue("fl_score_table", None)',
    ],
)
def test_shell_escapes_do_not_bypass_taskvalue_finalization(jm, source):
    finalized = jm._finalize_taskvalue_setters(
        [{"cell_type": "code", "source": [source]}]
    )
    active = finalized[0]["source"][0]

    assert "oidlUtils.parameters.setTaskValue" in active
    assert "str(" in active
    jm._assert_taskvalue_setters_finalized(finalized)


def test_taskvalue_tokenizer_failure_fails_closed(jm, monkeypatch):
    source = 'dbutils.jobs.taskValues.set("k", flag)'

    def broken_tokenizer(_readline):
        raise jm.tokenize.TokenError("forced", (1, 0))
        yield  # pragma: no cover

    monkeypatch.setattr(jm.tokenize, "generate_tokens", broken_tokenizer)
    with pytest.raises(
        jm.FinalArtifactValidationError, match="unresolved task-value setter"
    ):
        jm._assert_taskvalue_setters_finalized(
            [{"cell_type": "code", "source": [source]}]
        )


@pytest.mark.parametrize(
    "prefix",
    [
        "val quote = '\"'\n",
        "val clean = value.replace('\"', \"\")\n",
        "val clean = value.filter(_ != '\"')\n",
    ],
)
def test_scala_char_literals_do_not_hide_taskvalue_setter(jm, prefix):
    cells = [{
        "cell_type": "code",
        "source": [
            "%scala\n" + prefix
            + 'oidlUtils.parameters.setTaskValue("k", flag)'
        ],
    }]

    finalized = jm._finalize_taskvalue_setters(cells, native_lang="scala")
    assert 'String.valueOf(flag)' in finalized[0]["source"][0]
    jm._assert_taskvalue_setters_finalized(finalized, native_lang="scala")

"""Regression: the widgets.get -> compound-read rewrite must emit a 2-arg
oidlUtils.parameters.getParameter(name, default).

AIDP's JVM getParameter requires TWO args; a 1-arg getParameter("name") raises
`Py4JException: Method getParameter([String]) does not exist` at job-run time
(observed on the LPP config_url cell, 2026-07-23). Guards both the deterministic
emitter in _preprocess_cell_source and the prompt guidance.

Run: python -m pytest tests/test_getparameter_2arg.py -v
"""
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


@pytest.fixture(scope="module")
def jm():
    import job_migrate
    return job_migrate


# bare 1-arg getParameter("x") — the crashing form (arg not followed by a comma)
_ONE_ARG = re.compile(r'getParameter\(\s*(["\'][^"\']+["\']|\w+)\s*\)')


class TestDeterministicEmitter:
    def test_widget_rewrite_uses_aidputils(self, jm):
        # Python dbutils.widgets.get → native aidputils.widgets.get (Databricks-
        # behaviour match). aidputils.widgets.get RAISES on a missing widget
        # (live-verified 2026-08-07 on cluster ca991533: IllegalArgumentException
        # "Widget not found") — so an absent param is a VISIBLE failure, not a silent
        # "" default. This preserves the #32/#33 safety property (missing → visible
        # failure) via raise, replacing the old `getParameter(k,"") or None` compound.
        out = jm._preprocess_cell_source('config_url = dbutils.widgets.get("config_url")')
        assert 'aidputils.widgets.get("config_url")' in out
        # emitted form takes NO default arg → cannot silently default to ""
        assert not _ONE_ARG.search(out), f"stale getParameter leaked: {out!r}"

    def test_variable_arg_widget(self, jm):
        out = jm._preprocess_cell_source('x = dbutils.widgets.get(key_var)')
        assert "aidputils.widgets.get(key_var)" in out   # variable key → native aidputils form
        assert not _ONE_ARG.search(out)


class TestTaskValuesGetSentinel:
    """taskValues.get → getParameter: the Databricks `debugValue` is debug-only
    and must NOT become the runtime default (it would silently mask a missing
    cross-task value). A real `default` is kept; otherwise the sentinel
    `__PARAM_NOT_SET__` is emitted. Always 2-arg (AIDP requirement)."""

    def test_debugvalue_replaced_with_sentinel(self, jm):
        src = ('x = dbutils.jobs.taskValues.get(taskKey="FL_table_scoring", '
               'key="fl_score_table", debugValue="test1")')
        out = jm._rewrite_dbutils_api_renames(src)
        assert out.strip() == ('x = oidlUtils.parameters.getParameter('
                               '"fl_score_table", "__PARAM_NOT_SET__")')
        assert "test1" not in out            # debugValue must not survive

    def test_real_default_is_kept(self, jm):
        src = 'y = dbutils.jobs.taskValues.get(taskKey="A", key="k", default="real")'
        out = jm._rewrite_dbutils_api_renames(src)
        assert out.strip() == 'y = oidlUtils.parameters.getParameter("k", "real")'

    @pytest.mark.parametrize("default", ["1700000000000016", "1.5", "True", "fallback"])
    def test_non_string_default_uses_jvm_string_overload(self, jm, default):
        source = (
            'value = dbutils.jobs.taskValues.get(taskKey="producer", '
            f'key="scoring_timestamp", default={default})'
        )
        converted = jm._rewrite_dbutils_api_renames(source)
        assert converted == (
            'value = oidlUtils.parameters.getParameter('
            f'"scoring_timestamp", str({default}))'
        )
        assert jm._rewrite_dbutils_api_renames(converted) == converted

    @pytest.mark.parametrize("default", ["1700000000000016", "False", "fallback"])
    def test_already_migrated_default_is_repaired_at_final_save(self, jm, default):
        source = f'x = oidlUtils.parameters.getParameter("k", {default})'
        cells = [{"cell_type": "code", "source": [source]}]
        finalized = jm._finalize_taskvalue_setters(cells)
        saved = finalized[0]["source"][0]
        assert f'oidlUtils.parameters.getParameter("k", str({default}))' in saved
        assert source in saved  # audit comment preserves the customer input
        assert jm._finalize_taskvalue_setters(finalized) == finalized
        jm._assert_final_notebook_cells(finalized)

    def test_child_artifact_getparameter_default_is_repaired(self, jm):
        final = jm._finalize_child_artifact_sources([
            'x = oidlUtils.parameters.getParameter("k", 42)'
        ])
        assert 'oidlUtils.parameters.getParameter("k", str(42))' in final[0]

    @pytest.mark.parametrize("default", ['"real"', "f\"{prefix}-real\"", "str(fallback)"])
    def test_string_defaults_remain_unchanged(self, jm, default):
        source = f'x = oidlUtils.parameters.getParameter("k", {default})'
        assert jm._rewrite_dbutils_api_renames(source) == source

    def test_scala_default_uses_string_value_of(self, jm):
        source = '%scala\nval x = oidlUtils.parameters.getParameter("k", 42L)'
        converted = jm._rewrite_dbutils_api_renames(source)
        assert converted == (
            '%scala\nval x = oidlUtils.parameters.getParameter("k", String.valueOf(42L))'
        )
        assert jm._rewrite_dbutils_api_renames(converted) == converted

    def test_comments_and_non_python_cells_are_unchanged(self, jm):
        commented = '# oidlUtils.parameters.getParameter("k", 42)'
        markdown = '%md\noidlUtils.parameters.getParameter("k", 42)'
        assert jm._rewrite_dbutils_api_renames(commented) == commented
        assert jm._rewrite_dbutils_api_renames(markdown) == markdown

    def test_no_default_gets_sentinel_not_one_arg(self, jm):
        # Regression: previously emitted a bare 1-arg getParameter("only_key"),
        # which raises Py4JException on AIDP. Must be 2-arg with the sentinel.
        src = 'z = dbutils.jobs.taskValues.get(taskKey="A", key="only_key")'
        out = jm._rewrite_dbutils_api_renames(src)
        assert out.strip() == ('z = oidlUtils.parameters.getParameter('
                               '"only_key", "__PARAM_NOT_SET__")')
        assert not _ONE_ARG.search(out)      # no crashing 1-arg form


class TestPromptsTwoArg:
    def test_cell_migrate_prompt_uses_aidputils(self, jm):
        # prompt guidance directs Python widgets.get to the native aidputils form
        assert 'aidputils.widgets.get("name")' in jm.CELL_MIGRATE_PROMPT

    def test_cell_migrate_prompt_keeps_parameter_default_string_typed(self, jm):
        assert "getParameter's default must be a JVM String" in jm.CELL_MIGRATE_PROMPT

    def test_fix_prompt_uses_aidputils(self):
        import agent_migrate
        assert 'aidputils.widgets.get("name")' in agent_migrate.FIX_PROMPT or \
               'aidputils.widgets.get("name")' in agent_migrate.MIGRATION_PROMPT


def test_none(jm):
    assert jm._taskvalue_string_default("None") == "None"
    src = 'x = dbutils.jobs.taskValues.get(taskKey="t", key="k", default=None)'
    out = jm._rewrite_dbutils_api_renames(src)
    assert out == 'x = oidlUtils.parameters.getParameter("k", None)'
    cells = jm._finalize_taskvalue_setters([{"cell_type": "code", "source": [out]}])
    assert out in cells[0]["source"][0]


def test_scala_null_default_stays_unwrapped(jm):
    assert jm._taskvalue_string_default("null", is_scala=True) == "null"
    src = '%scala\nval x = oidlUtils.parameters.getParameter("k", null)'
    assert jm._rewrite_dbutils_api_renames(src) == src
    cells = jm._finalize_taskvalue_setters([{"cell_type": "code", "source": [src]}])
    assert src in cells[0]["source"][0]

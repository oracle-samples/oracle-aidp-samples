"""A workflow parameter the notebook cannot read is refused, never guessed.

Since the stage notebooks read a job TASK's parameters over their PARAMS
literals, a task parameter is the last word on what a run does. The PARAMS
cell used to read a switch as `value.lower() in ('true','yes','on','1')`,
so ANY other text -- `y`, `Y`, a typo like `ture`, `maybe` -- became False
and the flag was dropped from ARGV. With `dry-run` that is the dangerous
direction: a baked `dry-run: True` plus a task parameter `dry-run=y` ran
01_create_structure or 02_copy_schema FOR REAL -- schemas and tables
created, rows copied or overwritten -- and nothing reported an error.
provision's own `--stage-param` coercion already refused such a value as
"a typo that must not be guessed either way"; the notebook now does too.

A choice value (`mode=apend`) used to pass the cell untouched and fail
only later, in argparse on the cluster; it is refused here, by name.
"""
import pytest

from target.stage_notebooks import STAGES, build_stage_notebook


def _stage(key):
    return next(s for s in STAGES if s.key == key)


def _run_params_cell(stage_key, workflow, overrides=None):
    nb = build_stage_notebook(
        _stage(stage_key),
        overrides={"target-catalog": "lake", **(overrides or {})})
    source = "".join(nb["cells"][1]["source"])

    class _Params:
        @staticmethod
        def getParameter(name, default):
            return workflow.get(name, default)

    scope = {"oidlUtils": type("U", (), {"parameters": _Params})}
    exec(compile(source, "<params>", "exec"), scope)
    return scope


@pytest.mark.parametrize("stage_key,extra", [
    ("copy_schema", {"schema": "SALES"}),
    ("structure", {}),
])
@pytest.mark.parametrize("text", ["y", "Y", "ture", "maybe", "enabled"])
def test_an_unreadable_dry_run_is_refused_not_read_as_a_write(
        stage_key, extra, text):
    # provision baked a dry run; the task parameter is a typo.
    with pytest.raises(ValueError, match="dry-run") as exc:
        _run_params_cell(stage_key, {**extra, "dry-run": text},
                         overrides={"dry-run": True})
    assert repr(text) in str(exc.value)


@pytest.mark.parametrize("text,expected", [
    ("true", True), ("TRUE", True), ("yes", True), ("on", True), ("1", True),
    ("false", False), ("No", False), ("off", False), ("0", False),
])
def test_the_words_provision_accepts_are_still_accepted(text, expected):
    g = _run_params_cell("copy_schema", {"schema": "SALES", "dry-run": text})
    assert g["PARAMS"]["dry-run"] is expected
    assert ("--dry-run" in g["ARGV"]) is expected


def test_a_misspelt_mode_is_refused_before_argv_is_built():
    with pytest.raises(ValueError, match="mode") as exc:
        _run_params_cell("copy_schema", {"schema": "SALES", "mode": "apend"})
    message = str(exc.value)
    assert "'apend'" in message
    assert "skip-existing" in message and "overwrite" in message


def test_a_mode_of_the_other_stage_is_refused_by_this_one():
    # `overwrite` is a 02 mode; 01's modes are ddl-plan/ctas/manifest.
    with pytest.raises(ValueError, match="mode"):
        _run_params_cell("structure", {"mode": "overwrite"})


def test_a_valid_mode_and_source_mode_pass_through():
    g = _run_params_cell("copy_schema", {"schema": "SALES",
                                         "mode": "overwrite",
                                         "source-mode": "external-catalog"})
    argv = g["ARGV"]
    assert argv[argv.index("--mode") + 1] == "overwrite"
    assert argv[argv.index("--source-mode") + 1] == "external-catalog"

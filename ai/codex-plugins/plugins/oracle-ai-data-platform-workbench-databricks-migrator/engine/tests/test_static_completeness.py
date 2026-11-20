"""Candidate parity captured from 656230a before adding the optional channel."""
import ast
import json
from pathlib import Path

import pytest

from dbx_export.dependency_resolver import (
    _StaticEvaluation, _static_values, _static_value_signature,
)

BASELINE = json.loads((Path(__file__).parent / "fixtures/static_candidate_baseline.json").read_text())


@pytest.mark.parametrize("case", BASELINE, ids=lambda case: f'{case["expression"]}-{case["config_paths"]}')
@pytest.mark.parametrize("tracked", [False, True])
def test_additive_candidates_match_pre_redesign_baseline(case, tracked):
    state = _StaticEvaluation() if tracked else None
    values = _static_values(ast.parse(case["expression"], mode="eval").body,
                            {"ROOT": ["/repo"], "VALUES": list(range(70))}, {"root": "/repo"},
                            config_paths=case["config_paths"], completeness=state)
    assert repr([_static_value_signature(value) for value in values]) == case["signature"]
    assert [type(value).__name__ for value in values] == case["types"]


@pytest.mark.parametrize("expression,complete", [
    ('"/repo" or runtime_root()', True),
    ('"" and runtime_root()', True),
    ('"" or runtime_root()', False),
    ('"/repo" if flag else runtime_root()', False),
    ('{"a": "/repo", "other": runtime_root()}["a"]', True),
    ('{"a": "/repo", "other": runtime_root()}[runtime_choice()]', False),
    ('{"a": "/repo", "other": "/other"}[runtime_choice()]', True),
    ('{"a": {"root": "/repo", "metadata": runtime_root()}}["a"]["root"]', True),
    ('{"a": "/repo", **runtime_mapping()}["a"]', False),
    ('{**runtime_mapping(), "a": "/repo"}["a"]', True),
    ('("/repo" if flag else 12).strip()', False),
    ('"{}".format("/repo" if flag else 12)', True),
    ('("/repo" if flag else 12) + "/config"', False),
    ('("/repo" if flag else 12).startswith("/")', False),
    ('("{root}" if flag else "{missing}").format(root="/repo")', False),
    ('["/repo"][4]', False),
    ('{"a": "/repo"}["missing"]', False),
    ('"{}/config".format(VALUES)', False),
])
def test_completeness_matches_evaluated_paths(expression, complete):
    state = _StaticEvaluation()
    _static_values(ast.parse(expression, mode="eval").body, {"VALUES": list(range(70))}, {},
                   config_paths=True, completeness=state)
    assert state.complete == complete


@pytest.mark.parametrize("config_paths", [False, True])
@pytest.mark.parametrize("expression,complete", [
    ('f"{ROOT}"', True),
    ('f"{ROOT:}"', True),
    ('f"{ROOT!s}"', True),
    ('f"{ROOT!s:}"', True),
    ('f"{ROOT:{empty}}"', True),
    ('f"{ROOT:{empty if flag else runtime_format()}}"', False),
    ('f"{ROOT:.8}"', False),
    ('f"{ROOT!s:.8}"', False),
    ('f"{ROOT!r}"', False),
    ('f"{ROOT!a}"', False),
])
def test_formatted_value_options_preserve_candidates_without_false_completeness(expression, complete, config_paths):
    node = ast.parse(expression, mode="eval").body
    env = {"ROOT": ["../config"], "empty": [""]}
    expected = _static_values(node, env, {}, config_paths=config_paths)
    state = _StaticEvaluation()
    assert _static_values(node, env, {}, config_paths=config_paths, completeness=state) == expected == ["../config"]
    assert state.complete == complete


@pytest.mark.parametrize("expression,complete", [
    ('str(b"../config", encoding="utf-8")', False),
    ('str(b"../config", errors="ignore")', False),
    ('str(b"../config", **{"encoding": "utf-8"})', False),
    ('os.path.join("../config", ignored="value")', False),
    ('posixpath.join("../config", **runtime_keywords())', False),
    ('dbutils.widgets.get("root", ignored="value")', False),
    ('dbutils.widgets.getArgument("root", defaultValue="../other")', False),
    ('"{root}".format(root="../config", **runtime_keywords())', False),
    ('"{root}".format(root="../config", **{})', False),
    ('os.path.join(a="../config")', False),
    ('str("../config")', True),
    ('os.path.join("..", "config")', True),
    ('"{root}".format(root="../config")', True),
    ('"{root}".format(root="../config", unused="known")', True),
    ('dbutils.widgets.getArgument("root", runtime_default())', True),
])
@pytest.mark.parametrize("config_paths", [False, True])
def test_ignored_call_keywords_only_affect_opt_in_completeness(expression, complete, config_paths):
    node = ast.parse(expression, mode="eval").body
    bindings = {"root": "../config"}
    expected = _static_values(node, {}, bindings, config_paths=config_paths)
    state = _StaticEvaluation()
    actual = _static_values(node, {}, bindings, config_paths=config_paths, completeness=state)
    assert actual == expected
    assert state.complete == complete

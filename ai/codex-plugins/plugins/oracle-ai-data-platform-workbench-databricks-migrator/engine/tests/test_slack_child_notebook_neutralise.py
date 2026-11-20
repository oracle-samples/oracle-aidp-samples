"""The %run child-inline path must neutralise external posts too.

Reviewed on PR #82: `_inline_child_notebook` contained NO neutralisation call
of any kind — neither the regex path nor the AST one — so a child notebook that
posts to Slack posted for real during migration. Both reviewers found it
independently; it is pre-existing on the merge base, not a PR #82 regression.

These are STRUCTURAL tests. `_inline_child_notebook` is a ~450-line async
function that needs a live cluster session, so end-to-end execution is not
available here. What is asserted instead is the property whose ABSENCE was the
bug: that the neutraliser is invoked over every child cell before the execution
loop reaches any of them, and that the parent's binding state is threaded in.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "scripts" / "job_migrate.py"


@pytest.fixture(scope="module")
def inline_child_fn() -> ast.AST:
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == "_inline_child_notebook"):
            return node
    pytest.fail("_inline_child_notebook not found")


def _called_names(node: ast.AST) -> set:
    out = set()
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Call):
            continue
        fn = sub.func
        out.add(fn.id if isinstance(fn, ast.Name) else getattr(fn, "attr", ""))
    return out


def test_child_cells_are_neutralised(inline_child_fn):
    """The property whose absence let a child notebook post to production."""
    called = _called_names(inline_child_fn)
    assert "_ast_neutralize_cell" in called, (
        "%run children execute unneutralised — a child notebook posting to "
        "Slack will post for real during migration")


def test_child_has_a_regex_fallback_for_unparseable_cells(inline_child_fn):
    """A cell that will not parse must never be silently skipped."""
    assert "_neutralize_slack_execution" in _called_names(inline_child_fn)


def test_child_neutralisation_precedes_the_execution_loop(inline_child_fn):
    """Ordering is the whole point: neutralising after the loop has started
    means the first cells have already run — and posted."""
    neutralise_lines = [
        sub.lineno for sub in ast.walk(inline_child_fn)
        if isinstance(sub, ast.Call)
        and getattr(sub.func, "id", "") == "_ast_neutralize_cell"
    ]
    exec_loops = [
        sub.lineno for sub in ast.walk(inline_child_fn)
        if isinstance(sub, ast.For)
        and isinstance(sub.target, ast.Tuple)
        and any(getattr(el, "id", "") == "child_source" for el in sub.target.elts)
    ]
    assert neutralise_lines, "no neutralisation call in the child path"
    assert exec_loops, "child execution loop not found — did it get renamed?"
    assert min(neutralise_lines) < min(exec_loops), (
        "child cells are neutralised only after execution has begun")


def test_parent_slack_bindings_are_threaded_into_the_child(inline_child_fn):
    """The child runs in the PARENT's namespace, so a client bound in the
    parent is in scope. Without the parameter, a child calling a
    parent-constructed client is judged against an empty binding set."""
    args = inline_child_fn.args
    names = [a.arg for a in list(args.args) + list(args.kwonlyargs)]
    assert "slack_bindings" in names


def test_every_call_site_passes_slack_bindings():
    """A call site that forgets the argument silently degrades that child to a
    fresh binding set — the failure would be invisible."""
    tree = ast.parse(SRC.read_text(encoding="utf-8"))
    sites = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "_inline_child_notebook"
    ]
    assert sites, "no _inline_child_notebook call sites found"
    missing = [
        node.lineno for node in sites
        if "slack_bindings" not in {kw.arg for kw in node.keywords}
    ]
    assert not missing, f"call sites without slack_bindings: {missing}"

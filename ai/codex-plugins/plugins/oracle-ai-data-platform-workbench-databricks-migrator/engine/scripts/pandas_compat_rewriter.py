"""Deterministic pandas compatibility rewrites used by notebook migration.

The migration engine executes Databricks notebooks on pandas 2.x.  pandas 2.x
removed ``DataFrame.append``; this module rewrites only calls whose receiver can
be identified as DataFrame-like from assignments in the same cell.  Ordinary
Python ``list.append`` calls are deliberately left untouched.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass


_PANDAS_CONSTRUCTORS = {"DataFrame", "Series", "concat", "crosstab"}
_DATAFRAME_METHODS = {
    "merge",
    "join",
    "drop",
    "reset_index",
    "rename",
    "sort_values",
    "to_frame",
}


@dataclass(frozen=True)
class PandasRewriteReport:
    replacements: int = 0


def _source_segment(source: str, node: ast.AST) -> str:
    return ast.get_source_segment(source, node) or ""


def _target_names(target: ast.AST) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for item in target.elts for name in _target_names(item)]
    return []


def _dataframe_assignment_history(tree: ast.AST) -> dict[str, list[tuple[int, bool]]]:
    """Track the last known type of each assigned name in source order.

    Provenance is based on the AST shape of the complete RHS.  In particular,
    a list literal containing a DataFrame expression is still a list, so the
    common ``frames = [df.drop(...)]`` accumulator is never classified as a
    DataFrame.
    """
    history: dict[str, list[tuple[int, bool]]] = {}

    def known_dataframe(name: str, before_line: int) -> bool:
        prior = [state for line, state in history.get(name, []) if line < before_line]
        return prior[-1] if prior else False

    def is_dataframe_expr(value: ast.AST, before_line: int) -> bool:
        if isinstance(value, ast.Name):
            return known_dataframe(value.id, before_line)
        if isinstance(value, (ast.Attribute, ast.Subscript)):
            return is_dataframe_expr(value.value, before_line)
        if not isinstance(value, ast.Call):
            return False
        func = value.func
        if isinstance(func, ast.Attribute):
            if func.attr == "toPandas":
                return True
            if func.attr in _DATAFRAME_METHODS:
                return True
            if isinstance(func.value, ast.Name) and func.value.id in {"pd", "pandas"}:
                return func.attr in _PANDAS_CONSTRUCTORS or func.attr.startswith("read_")
            # A method call is DataFrame-producing only when its receiver is
            # already proven DataFrame-like.
            return is_dataframe_expr(func.value, before_line)
        return False

    assignments = sorted(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr))
        ),
        key=lambda node: (node.lineno, node.col_offset),
    )
    for node in assignments:
        value = node.value
        state = is_dataframe_expr(value, node.lineno)
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            for name in _target_names(target):
                history.setdefault(name, []).append((node.lineno, state))
    return history


def _assigned_dataframe_names(tree: ast.AST, source: str) -> set[str]:
    """Return names whose final assignment is proven DataFrame-like.

    Kept as a small compatibility surface for existing callers/tests; rewrite
    decisions use the line-aware history above.
    """
    del source
    history = _dataframe_assignment_history(tree)
    return {name for name, states in history.items() if states and states[-1][1]}


def rewrite_removed_dataframe_append(source: str) -> tuple[str, PandasRewriteReport]:
    """Rewrite safely identified ``DataFrame.append`` calls to ``pd.concat``.

    The replacement preserves pandas' old defaults and supported keyword
    arguments.  Unsupported signatures are retained so migration fails honestly
    instead of guessing at semantics.
    """
    if ".append(" not in source or source.lstrip().startswith(("%scala", "%sql", "%r")):
        return source, PandasRewriteReport()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return source, PandasRewriteReport()

    dataframe_history = _dataframe_assignment_history(tree)
    lines = source.splitlines(keepends=True)
    offsets = []
    offset = 0
    for line in lines:
        offsets.append(offset)
        offset += len(line)

    def absolute(line: int, column: int) -> int:
        return offsets[line - 1] + column

    replacements: list[tuple[int, int, str]] = []
    supported_keywords = {"ignore_index", "verify_integrity", "sort"}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "append" or len(node.args) != 1:
            continue
        if any(keyword.arg not in supported_keywords for keyword in node.keywords):
            continue
        receiver = node.func.value
        receiver_source = _source_segment(source, receiver)

        def receiver_is_dataframe_expr(value: ast.AST) -> bool:
            if isinstance(value, ast.Name):
                prior = [
                    state
                    for line, state in dataframe_history.get(value.id, [])
                    if line < node.lineno
                ]
                return prior[-1] if prior else False
            if isinstance(value, (ast.Attribute, ast.Subscript)):
                return receiver_is_dataframe_expr(value.value)
            if not isinstance(value, ast.Call):
                return False
            func = value.func
            if not isinstance(func, ast.Attribute):
                return False
            if func.attr == "toPandas":
                return True
            if func.attr in _DATAFRAME_METHODS:
                return True
            if isinstance(func.value, ast.Name) and func.value.id in {"pd", "pandas"}:
                return func.attr in _PANDAS_CONSTRUCTORS or func.attr.startswith("read_")
            return receiver_is_dataframe_expr(func.value)

        receiver_is_dataframe = receiver_is_dataframe_expr(receiver)
        if not receiver_is_dataframe:
            continue
        other_source = _source_segment(source, node.args[0])
        if not receiver_source or not other_source:
            continue
        keyword_source = "".join(
            f", {keyword.arg}={_source_segment(source, keyword.value)}"
            for keyword in node.keywords
        )
        replacement = (
            "pd.concat(["
            + receiver_source
            + ", "
            + other_source
            + "]"
            + keyword_source
            + ")"
        )
        replacements.append(
            (
                absolute(node.lineno, node.col_offset),
                absolute(node.end_lineno, node.end_col_offset),
                replacement,
            )
        )

    rewritten = source
    for start, end, replacement in sorted(replacements, reverse=True):
        rewritten = rewritten[:start] + replacement + rewritten[end:]
    return rewritten, PandasRewriteReport(replacements=len(replacements))


def preserve_valid_python_candidate(previous: str, candidate: str) -> tuple[str, str | None]:
    """Reject prose/markdown or syntactically invalid Python model responses."""
    stripped = candidate.lstrip()
    if not stripped:
        return previous, "empty response"
    if stripped.startswith(("%", "!")):
        return candidate, None
    try:
        # Run c8fc04e0 (2026-09-28): Slack message f-strings contained ```.
        # A substring fence check rejected valid S3->OCI conversions and restored
        # the original AWS writes. Python syntax distinguishes string/comment
        # data from actual markdown wrappers; validate before labeling fences.
        compile(candidate, "<migration-candidate>", "exec")
        if "```" in candidate:
            # PR #173: a fenced explanation can compile as comments/docstrings
            # yet erase the prior write cell. Accept fence data only alongside
            # statements, not a module made entirely of inert string literals.
            body = ast.parse(candidate).body
            if not any(not (isinstance(node, ast.Expr)
                           and isinstance(node.value, ast.Constant)
                           and isinstance(node.value.value, str)) for node in body):
                return previous, "fence-bearing response has no executable statements"
    except (SyntaxError, ValueError) as exc:
        if "```" in candidate:
            return previous, "markdown fence remained in code response"
        return previous, f"invalid Python candidate: {exc}"
    return candidate, None

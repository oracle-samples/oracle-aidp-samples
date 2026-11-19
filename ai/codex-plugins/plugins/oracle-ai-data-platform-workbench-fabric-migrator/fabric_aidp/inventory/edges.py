"""Extract cross-item references from notebook source, without executing it.

Three edge kinds:
  %run <Notebook>                                   -> "run"
  notebookutils.notebook.run("<Notebook>", …)       -> "notebook_run"
  notebookutils.notebook.runMultiple([…]) or ({…})  -> "run_multiple"

A target that is not a plain string literal (a variable, an f-string) is
recorded in "unresolved" rather than dropped, so the report can tell a
reviewer that a dependency exists but its name could not be determined
statically.
"""
from __future__ import annotations

import ast
import re

_RUN_MAGIC_RE = re.compile(r"^[ \t]*%run[ \t]+(?P<rest>\S.*?)[ \t]*$", re.MULTILINE)
_NB_RUN_RE = re.compile(r"(?:notebookutils|mssparkutils)\.notebook\.run\s*\(")
_RUN_MULTIPLE_RE = re.compile(
    r"(?:notebookutils|mssparkutils)\.notebook\.runMultiple\s*\(")
_QUOTED_RE = re.compile(r"""^(?P<q>['"])(?P<val>(?:\\.|(?!(?P=q)).)*)(?P=q)$""", re.S)
_OPENERS, _CLOSERS = "([{", ")]}"


def string_literal_value(text):
    """The value of a plain string literal, or None if it is not one.

    f-strings, byte strings and raw strings are deliberately rejected: their
    value is not statically known (f) or not a notebook name (b).
    """
    if not isinstance(text, str):
        return None
    stripped = text.strip()
    match = _QUOTED_RE.match(stripped)
    if not match:
        return None
    return match.group("val")


def first_argument(text: str, open_index: int):
    """Source text of the first argument of the call whose '(' is at open_index.

    Returns None for an unterminated call — consuming the rest of the file as
    an argument would be worse than reporting nothing.
    """
    depth = 0
    start = open_index + 1
    index = open_index
    quote = None
    while index < len(text):
        char = text[index]
        if quote is not None:
            if char == "\\":
                index += 2
                continue
            if char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char in _OPENERS:
            depth += 1
        elif char in _CLOSERS:
            depth -= 1
            if depth == 0:
                return text[start:index].strip()
        elif char == "," and depth == 1:
            return text[start:index].strip()
        index += 1
    return None


def _without_leading_flags(rest: str) -> str:
    """`rest` with any leading `-x` / `--xyz` switches taken off.

    `%run -b Child` used to yield the notebook name `-b`: the flag was
    read as the name, the real edge to `Child` was lost, and the plan
    grew a dangling dependency on a notebook called `-b`. One line of
    source turned a true edge into a false one, which is worse than
    missing it.

    Only *leading* switches are dropped. A name is the first token that
    is not one, and anything after it is a parameter map rather than part
    of the edge.
    """
    while True:
        stripped = rest.lstrip()
        if not stripped.startswith("-"):
            return stripped
        head, _, tail = stripped.partition(" ")
        if not tail.strip():
            return ""
        rest = tail


def _run_magic_target(rest: str):
    """The notebook name from a `%run` line's argument text."""
    literal = string_literal_value(rest)
    if literal is not None:
        return literal
    rest = _without_leading_flags(rest)
    if not rest:
        return None
    # Unquoted form: the name runs to the first whitespace. Anything after it
    # is a parameter map, which is not part of the dependency edge.
    quoted = _QUOTED_RE.match(rest)
    if quoted:
        return quoted.group("val")
    if rest.startswith(("'", '"')):
        closing = rest.find(rest[0], 1)
        if closing > 0:
            return rest[1:closing]
    return rest.split()[0] if rest.split() else None


def run_multiple_targets(argument: str):
    """The notebooks one `runMultiple(...)` argument names, or None.

    `None` means the argument was not a literal this can read -- a
    variable, an f-string, a comprehension -- and the caller records it as
    unresolved. An empty list means it *was* read and named nobody.

    Both documented shapes are accepted, because both are what people
    write:

      runMultiple(["Child1", "Child2"])              a plain list
      runMultiple({"activities": [{"name": "a",      the DAG form, where
                                   "path": "Child1", the notebook is under
                                   "dependencies": []}]})   `path`

    Read with `ast.literal_eval`, so nothing in the notebook is executed
    to find out.
    """
    try:
        value = ast.literal_eval(argument)
    except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
        return None

    def _name(entry):
        if isinstance(entry, str):
            return entry.strip()
        if isinstance(entry, dict):
            # `path` first: the DAG form's `name` is the activity's label
            # and may be anything, while `path` is the notebook.
            for key in ("path", "name"):
                candidate = entry.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()
        return ""

    if isinstance(value, (list, tuple)):
        entries = list(value)
    elif isinstance(value, dict) and isinstance(value.get("activities"), list):
        entries = value["activities"]
    else:
        # A literal of a shape this does not know. Saying "no notebooks"
        # would be a claim; saying "could not tell" is the truth.
        return None
    return [name for name in (_name(entry) for entry in entries) if name]


def extract_notebook_edges(source) -> dict:
    run, notebook_run, run_multiple, unresolved = set(), set(), set(), set()
    if isinstance(source, str) and source:
        for match in _RUN_MAGIC_RE.finditer(source):
            target = _run_magic_target(match.group("rest"))
            if target:
                run.add(target)
        for match in _NB_RUN_RE.finditer(source):
            argument = first_argument(source, match.end() - 1)
            if argument is None or not argument:
                continue
            literal = string_literal_value(argument)
            if literal is not None:
                notebook_run.add(literal)
            else:
                unresolved.add(argument)
        # Written after the `run` loop and matched separately: `.run\s*\(`
        # does not match `.runMultiple(`, so a notebook that launched ten
        # children with one call produced no edges at all and no warning.
        for match in _RUN_MULTIPLE_RE.finditer(source):
            argument = first_argument(source, match.end() - 1)
            if argument is None or not argument:
                continue
            targets = run_multiple_targets(argument)
            if targets is None:
                unresolved.add(argument)
            else:
                run_multiple.update(targets)
    return {
        "run": sorted(run),
        "notebook_run": sorted(notebook_run),
        "run_multiple": sorted(run_multiple),
        "unresolved": sorted(unresolved),
    }

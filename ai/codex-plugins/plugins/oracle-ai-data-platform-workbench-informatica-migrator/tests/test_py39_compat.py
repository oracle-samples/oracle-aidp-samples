"""The package stays importable on the Python it advertises (SEC-AIDP-SAMPLES-002 follow-up).

``pyproject.toml`` and ``setup.py`` both say ``>=3.9``. A ``str | None``
annotation is evaluated when the ``def`` runs unless the module starts with
``from __future__ import annotations``, so on 3.9 ``import infa2aidp.cli``
died with ``TypeError: unsupported operand type(s) for |`` before a single
argument was parsed -- the security fixes in the CLI were unreachable for
exactly the users the metadata invites. The suite runs on newer interpreters,
where nothing notices; this walks the tree with the 3.9 grammar instead of
waiting for a 3.9 user to report it.

Per file:
1. it parses with ``ast.parse(..., feature_version=(3, 9))`` -- no ``match``,
   no ``except*``, nothing the 3.9 parser rejects;
2. if any annotation (parameter, return, variable, dataclass field) contains
   ``|``, the module has the future import;
3. nothing relies on 3.10+ at runtime, where the future import cannot help:
   ``A | B`` outside an annotation (``isinstance``/``issubclass`` targets,
   module- or class-level aliases), ``get_type_hints()``/``__annotations__``
   on deferred ``|`` annotations, names that first appeared in a later
   stdlib (``typing.ParamSpec``, ``tomllib``, ``enum.StrEnum``, ...),
   ``dataclass(slots=, kw_only=)``, ``zip(strict=)``, ``int.bit_count()``.

This is a static gate on syntax, annotations and a table of known 3.10+
names, not an interpreter: a construct missing from the table passes.
``INCOMPATIBLE_PROBE`` below is the negative fixture -- every construct the
gate knows is planted once and must be reported; ``COMPATIBLE_CONTROL``
holds the legitimate look-alikes (flag ``|``, guarded imports,
``TYPE_CHECKING`` blocks) that must stay silent. Both are text, not modules,
so the walker never sees them as files.

Fixtures and notebooks are inputs, not modules, and are not walked.

Runs under ``pytest``, ``python -m unittest`` and directly as
``PYTHONPATH=. python tests/test_py39_compat.py``, like the sibling tests.
"""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_PARTS = {"fixtures", "node_modules", "__pycache__", "build", "dist"}
PY39 = (3, 9)

# Names first available after 3.9, by module. ``from M import N`` and ``M.N``
# (after ``import M``) are both reported unless shielded (see _shielded).
NEW_IMPORTS = {
    "typing": {
        "ParamSpec": "3.10", "TypeAlias": "3.10", "TypeGuard": "3.10",
        "Concatenate": "3.10", "is_typeddict": "3.10",
        "Self": "3.11", "Required": "3.11", "NotRequired": "3.11",
        "Unpack": "3.11", "LiteralString": "3.11", "Never": "3.11",
        "TypeVarTuple": "3.11", "assert_never": "3.11", "assert_type": "3.11",
        "reveal_type": "3.11", "override": "3.12", "TypeAliasType": "3.12",
    },
    "dataclasses": {"KW_ONLY": "3.10"},
    "itertools": {"pairwise": "3.10", "batched": "3.12"},
    "enum": {"StrEnum": "3.11", "ReprEnum": "3.11", "verify": "3.11",
             "member": "3.11", "nonmember": "3.11"},
    "datetime": {"UTC": "3.11"},
    "contextlib": {"aclosing": "3.10", "chdir": "3.11"},
    "asyncio": {"TaskGroup": "3.11", "timeout": "3.11", "Runner": "3.11"},
    "inspect": {"get_annotations": "3.10"},
}
NEW_MODULES = {"tomllib": "3.11", "wsgiref.types": "3.11"}
NEW_BUILTINS = {"aiter": "3.10", "anext": "3.10", "EncodingWarning": "3.10",
                "ExceptionGroup": "3.11", "BaseExceptionGroup": "3.11"}
NEW_METHODS = {"bit_count": "3.10"}
NEW_KEYWORDS = {
    ("zip", "strict"): "3.10",
    ("dataclass", "slots"): "3.10",
    ("dataclass", "kw_only"): "3.10",
    ("field", "kw_only"): "3.10",
    ("bisect_left", "key"): "3.10", ("bisect_right", "key"): "3.10",
    ("insort_left", "key"): "3.10", ("insort_right", "key"): "3.10",
}
# Operands that make a module-level ``A | B`` a type union rather than a
# flag/set/dict merge: builtin type names, ``None``, anything subscripted
# (``list[str]``), CamelCase names.
BUILTIN_TYPES = {
    "str", "int", "float", "complex", "bool", "bytes", "bytearray",
    "memoryview", "list", "dict", "set", "frozenset", "tuple", "type",
    "object", "range", "slice",
}


def _python_files():
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT)
        if any(part in SKIP_PARTS or part.startswith(".") for part in rel.parts):
            continue
        yield path


def _annotations(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            extra = [a for a in (args.vararg, args.kwarg) if a is not None]
            for arg in args.posonlyargs + args.args + args.kwonlyargs + extra:
                if arg.annotation is not None:
                    yield arg.annotation
            if node.returns is not None:
                yield node.returns
        elif isinstance(node, ast.AnnAssign):
            yield node.annotation


def _has_union(node):
    return any(isinstance(n, ast.BinOp) and isinstance(n.op, ast.BitOr) for n in ast.walk(node))


def _has_future_annotations(tree):
    return any(
        isinstance(node, ast.ImportFrom)
        and node.module == "__future__"
        and any(alias.name == "annotations" for alias in node.names)
        for node in tree.body
    )


def _camel(name):
    return name[:1].isupper() and not name.isupper()


def _type_like(node):
    if isinstance(node, ast.Constant):
        return node.value is None
    if isinstance(node, ast.Name):
        return node.id in BUILTIN_TYPES or _camel(node.id)
    if isinstance(node, ast.Attribute):
        return _camel(node.attr)
    if isinstance(node, ast.Subscript):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _type_like(node.left) or _type_like(node.right)
    return False


def _mentions(node, *names):
    return any(
        (isinstance(n, ast.Name) and n.id in names)
        or (isinstance(n, ast.Attribute) and n.attr in names)
        for n in ast.walk(node)
    )


def _catches_import_error(handler):
    if handler.type is None:
        return True
    return _mentions(handler.type, "ImportError", "ModuleNotFoundError", "Exception")


def _shielded(tree):
    """Nodes under a guard that already knows about the version split.

    ``try: import X / except ImportError``, ``if sys.version_info ...`` and
    ``if TYPE_CHECKING:`` are the three idioms a 3.9-aware module uses; what
    is inside them is the author's business, not this gate's.
    """
    shielded = set()
    for node in ast.walk(tree):
        bodies = []
        if isinstance(node, ast.Try) and any(_catches_import_error(h) for h in node.handlers):
            bodies.append(node.body)
        elif isinstance(node, ast.If):
            if _mentions(node.test, "version_info"):
                bodies.append(node.body)
                bodies.append(node.orelse)
            elif _mentions(node.test, "TYPE_CHECKING"):
                bodies.append(node.body)
        for body in bodies:
            for stmt in body:
                shielded.update(id(n) for n in ast.walk(stmt))
    return shielded


def _runtime_scope(tree):
    """Statements executed at import time: module and class bodies, through
    control flow, but never into a function or lambda."""
    stack = list(tree.body)
    while stack:
        stmt = stack.pop()
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        yield stmt
        for field_name in ("body", "orelse", "finalbody", "handlers"):
            stack.extend(getattr(stmt, field_name, []) or [])


def _call_name(node):
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    return getattr(func, "attr", None)


def _problems(path, source):
    rel = path.relative_to(ROOT).as_posix()
    try:
        ast.parse(source, filename=rel, feature_version=PY39)
    except SyntaxError as exc:
        return [f"{rel}:{exc.lineno}: does not parse as Python 3.9 ({exc.msg})"]
    tree = ast.parse(source, filename=rel)
    found = []
    future = _has_future_annotations(tree)
    union_annotation = any(_has_union(ann) for ann in _annotations(tree))
    if union_annotation and not future:
        for ann in _annotations(tree):
            if _has_union(ann):
                found.append(
                    f"{rel}:{ann.lineno}: '|' annotation evaluated at def time; "
                    "add 'from __future__ import annotations'"
                )
                break
    shielded = _shielded(tree)
    plain_modules = set()

    for stmt in _runtime_scope(tree):
        if id(stmt) in shielded:
            continue
        value = getattr(stmt, "value", None)
        if isinstance(stmt, (ast.Assign, ast.AnnAssign)) and value is not None and _type_like(value):
            if isinstance(value, ast.BinOp) and isinstance(value.op, ast.BitOr):
                found.append(
                    f"{rel}:{stmt.lineno}: 'A | B' built at import time; the future import "
                    "does not cover values -- use typing.Union/Optional"
                )

    for node in ast.walk(tree):
        if id(node) in shielded:
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in NEW_MODULES:
                    found.append(f"{rel}:{node.lineno}: '{alias.name}' is {NEW_MODULES[alias.name]}+")
                if alias.asname is None and alias.name in NEW_IMPORTS:
                    plain_modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module in NEW_MODULES:
                found.append(f"{rel}:{node.lineno}: '{node.module}' is {NEW_MODULES[node.module]}+")
            table = NEW_IMPORTS.get(node.module, {})
            for alias in node.names:
                if alias.name in table:
                    found.append(
                        f"{rel}:{node.lineno}: {node.module}.{alias.name} is {table[alias.name]}+"
                    )

    for node in ast.walk(tree):
        if id(node) in shielded:
            continue
        if isinstance(node, ast.Attribute):
            base = node.value
            if isinstance(base, ast.Name) and base.id in plain_modules:
                version = NEW_IMPORTS[base.id].get(node.attr)
                if version:
                    found.append(f"{rel}:{node.lineno}: {base.id}.{node.attr} is {version}+")
            if node.attr == "__annotations__" and future and union_annotation:
                found.append(
                    f"{rel}:{node.lineno}: __annotations__ evaluated on 3.9 would see "
                    "the deferred '|' annotations in this module"
                )
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in NEW_BUILTINS:
            found.append(f"{rel}:{node.lineno}: builtin {node.id} is {NEW_BUILTINS[node.id]}+")
        elif isinstance(node, ast.Call):
            name = _call_name(node)
            if name in ("isinstance", "issubclass") and len(node.args) >= 2 and _has_union(node.args[1]):
                found.append(f"{rel}:{node.lineno}: {name}() with 'A | B' needs a tuple on 3.9")
            if name in NEW_METHODS and isinstance(node.func, ast.Attribute):
                found.append(f"{rel}:{node.lineno}: .{name}() is {NEW_METHODS[name]}+ (int.{name})")
            if name == "get_type_hints" and future and union_annotation:
                found.append(
                    f"{rel}:{node.lineno}: get_type_hints() on 3.9 evaluates the deferred "
                    "'|' annotations in this module"
                )
            for kw in node.keywords:
                version = NEW_KEYWORDS.get((name, kw.arg))
                if version:
                    found.append(f"{rel}:{node.lineno}: {name}({kw.arg}=) is {version}+")
    found.sort(key=lambda p: int(p[len(rel) + 1:].split(":", 1)[0]))
    return found


# Every construct the gate knows, planted once. Parses on any Python >= 3.9
# grammar; each line marked below must be reported.
INCOMPATIBLE_PROBE = '''\
"""Parses on the 3.9 grammar; fails at import or at runtime on 3.9."""
from __future__ import annotations

import tomllib
import typing
from dataclasses import dataclass, field, KW_ONLY
from enum import StrEnum
from itertools import pairwise
from typing import ParamSpec, TypeAlias, Self, get_type_hints

Maybe = str | None
Alias: TypeAlias = int | str
P = typing.ParamSpec("P")


class Config:
    Mode = int | str


@dataclass(slots=True, kw_only=True)
class D:
    name: str | None = None
    tags: list[str] = field(default_factory=list, kw_only=True)


def f(x, pair):
    if issubclass(type(x), int | str):
        return (5).bit_count()
    if isinstance(x, int | str):
        return None
    hints = get_type_hints(D)
    anns = D.__annotations__
    return list(zip(x, pair, strict=True)), hints, anns, ExceptionGroup, aiter, anext
'''

# What the probe must produce: (line number, substring of the message).
INCOMPATIBLE_EXPECTED = [
    (4, "'tomllib' is 3.11+"),
    (6, "dataclasses.KW_ONLY is 3.10+"),
    (7, "enum.StrEnum is 3.11+"),
    (8, "itertools.pairwise is 3.10+"),
    (9, "typing.ParamSpec is 3.10+"),
    (9, "typing.TypeAlias is 3.10+"),
    (9, "typing.Self is 3.11+"),
    (11, "'A | B' built at import time"),
    (12, "'A | B' built at import time"),
    (13, "typing.ParamSpec is 3.10+"),
    (17, "'A | B' built at import time"),
    (20, "dataclass(slots=) is 3.10+"),
    (20, "dataclass(kw_only=) is 3.10+"),
    (23, "field(kw_only=) is 3.10+"),
    (27, "issubclass() with 'A | B'"),
    (28, ".bit_count() is 3.10"),
    (29, "isinstance() with 'A | B'"),
    (31, "get_type_hints() on 3.9"),
    (32, "__annotations__ evaluated on 3.9"),
    (33, "zip(strict=) is 3.10+"),
    (33, "builtin ExceptionGroup is 3.11+"),
    (33, "builtin aiter is 3.10+"),
    (33, "builtin anext is 3.10+"),
]

# Legitimate look-alikes of everything above. Must produce no report.
COMPATIBLE_CONTROL = '''\
"""Every idiom the gate must not mistake for a 3.10+ dependency."""
from __future__ import annotations

import re
import sys
from typing import TYPE_CHECKING, Optional, Union

try:
    import tomllib
except ImportError:
    tomllib = None

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 10):
    from itertools import pairwise
else:
    def pairwise(it):
        return zip(it, it[1:])

if TYPE_CHECKING:
    Maybe = str | None

FLAGS = re.I | re.M
MERGED = {"a": 1} | {"b": 2}
SPARK_BUILTINS = ["bit_count", "aiter"]
Legacy = Optional[str]


def g(x, pair, items) -> str | None:
    if isinstance(x, (int, str)) or issubclass(type(x), (int, str)):
        return None
    mask = x.FLAG_A | x.FLAG_B
    return str(mask), list(zip(x, pair)), Union[int, str]
'''


class Py39CompatTests(unittest.TestCase):
    def test_tree_is_python_39_compatible(self):
        files = list(_python_files())
        # The walk itself is the control: a wrong ROOT or an over-eager skip
        # must fail here, not pass by looking at nothing.
        self.assertGreater(len(files), 50, f"walked only {len(files)} files under {ROOT}")
        problems = []
        for path in files:
            problems.extend(_problems(path, path.read_text(encoding="utf-8")))
        self.assertFalse(
            problems,
            f"{len(problems)} Python 3.9 incompatibilities while requires-python is >=3.9:\n  "
            + "\n  ".join(problems),
        )

    def test_probe_file_is_not_walked(self):
        # The probes live in this module as text; if someone ever writes them
        # out as .py next to the tests, the tree test above goes red.
        self.assertFalse([p for p in _python_files() if "probe" in p.name.lower()])

    def test_gate_reports_every_planted_incompatibility(self):
        ast.parse(INCOMPATIBLE_PROBE, feature_version=PY39)  # the grammar alone lets it through
        problems = _problems(ROOT / "tests" / "_probe.py", INCOMPATIBLE_PROBE)
        for lineno, needle in INCOMPATIBLE_EXPECTED:
            with self.subTest(line=lineno, expect=needle):
                self.assertTrue(
                    any(p.startswith(f"tests/_probe.py:{lineno}:") and needle in p for p in problems),
                    "not reported; got:\n  " + "\n  ".join(problems),
                )
        self.assertEqual(len(problems), len(INCOMPATIBLE_EXPECTED), "\n".join(problems))

    def test_gate_stays_silent_on_guarded_and_lookalike_code(self):
        problems = _problems(ROOT / "tests" / "_control.py", COMPATIBLE_CONTROL)
        self.assertEqual(problems, [])

    def test_gate_still_catches_the_original_bugs(self):
        no_future = "def f(x: str | None) -> int | None:\n    return None\n"
        problems = _problems(ROOT / "tests" / "_nf.py", no_future)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("add 'from __future__ import annotations'", problems[0])
        match_stmt = "match x:\n    case 1:\n        pass\n"
        problems = _problems(ROOT / "tests" / "_m.py", match_stmt)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("does not parse as Python 3.9", problems[0])


if __name__ == "__main__":
    unittest.main()

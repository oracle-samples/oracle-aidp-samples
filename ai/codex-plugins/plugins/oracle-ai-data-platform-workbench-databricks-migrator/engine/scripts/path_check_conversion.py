"""REF-158: keep Python path checks working after the S3/DBFS literal sweep.

job_migrate rewrites every S3/DBFS string in a cell to its OCI/Volume form
(``_apply_s3_translations`` + ``_apply_dbfs_translations``). A check such as
``path.startswith(GOLD_PREFIXES)`` then compares a runtime value that is still
``s3://...`` with a rewritten prefix and never matches (REF-158,
Example_Monitoring_Generic_Job). The sweep stays as it is; this module converts
the VALUE at each such check with the mapping the sweep applied to the check's
own strings, so both sides of the comparison use the migrated form.

Contract (pinned by tests/test_path_check_conversion.py):

* When the check, as the sweep left it, already succeeds on the value,
  nothing changes. Otherwise the value changes only when it starts with
  (prefix checks) or equals (``==``, ``!=``, membership) one of the check's
  strings that the sweep rewrote; it then becomes what the sweep made of that
  string. Every other value passes through unchanged.
* A value is converted only where every place it goes can be seen, so the
  value that passes the check is the value that is used. Two cases qualify;
  every other check keeps the sweep's result:
  - A value the function builds itself (from a call such as ``spark.sql``, a
    literal, or a string method on such a value; never a parameter, a global
    or an enclosing function's variable) is converted at the check, by an
    assignment expression where the check reads it, so ``and``/``or``,
    ``elif`` and ``while`` evaluate as before. When the check decides an
    ``if``, only the branch a converted value takes counts: a rebinding there
    ends the conversion, and a branch that always leaves the function
    (``return``/``raise``, no ``try``/``with`` around it) keeps it from the
    code after the ``if``. While converted, the value is used only by another
    converted check, a check against strings not left in S3/DBFS form, a
    slice by the matched prefix, ``return``, logging, or as the path of a
    Spark read/write or ``open``. No other variable holding its unconverted
    form is read once a converted value can be running, except in an error
    message, in logging, or after it is assigned again from the checked
    variable; the check's yes/no result decides control flow in the function
    and goes nowhere else; no nested function or generator can read the value
    while it is converted; and the function does not evaluate the value's
    source (e.g. a second ``DESCRIBE``) again.
  - A path helper (a top-level function that checks its parameter before
    anything else reads it, returns it or a slice of it from the branch a
    converted value takes, and never raises or returns yes/no) keeps the
    sweep's result. Instead, each call to it in the same cell converts the
    argument before the helper sees it, when the helper is bound once in the
    notebook and nothing else in the notebook reads that argument (another
    key of the same dict, or displaying it, is fine). Callers this module
    cannot see therefore behave exactly as with the sweep alone.
* Check strings resolve only from literals, f-strings whose fields are such
  strings, a ``for`` loop over literals, a local assigned once, or a notebook
  constant: a name bound exactly once at module level across the notebook's
  Python cells, by a top-level assignment of a string or a tuple/list/set of
  strings, that nothing can rebind later.
* Every check that compares against rewritten strings but is not converted
  keeps the sweep's result, and a note reports it, as does every call of a
  path helper that is not converted.
* The generated code uses builtins only (no runtime import), keeps its
  original strings split so a later sweep cannot rewrite them, and the edited
  statement keeps its original lines commented above it (safety contract #2);
  later passes over the cell leave those comment lines verbatim.
"""
from __future__ import annotations

import ast
import io
import re
import tokenize
from collections import Counter
from dataclasses import dataclass, field
from typing import (Callable, Dict, FrozenSet, Iterable, Iterator, List, NamedTuple, Optional,
                    Sequence, Set, Tuple)

Translate = Callable[[str], str]
Pair = Tuple[str, str]  # (original string, the sweep's rewrite of it)
Position = Tuple[int, int]  # (cell index, line)

TAG = "# Oracle tool modification:"
ORIGINAL_MARK = f"{TAG} original before path-check conversion"
REASON = ("path check: its S3/DBFS strings were migrated, so the value is "
          "migrated the same way before the comparison")
# Method -> how the original check reads the value (used to test it first).
_PREFIX_METHODS = {"startswith": "startswith", "removeprefix": "startswith", "replace": "contains"}
_SUFFIX_METHODS = frozenset({"endswith", "removesuffix"})
_STR_METHODS = frozenset(_PREFIX_METHODS) | _SUFFIX_METHODS
# Calls that match a value against a pattern this module does not convert.
_PATTERN_FUNCTIONS = {
    "re": frozenset({"match", "search", "fullmatch", "sub", "subn", "split", "findall",
                     "finditer", "compile"}),
    "fnmatch": frozenset({"fnmatch", "fnmatchcase", "filter"}),
    "str": _STR_METHODS,
}
# Rebinding any module name at runtime: exec, or writes through these.
_NAMESPACE_FUNCTIONS = frozenset({"globals", "vars", "locals"})
_NAMESPACE_WRITERS = frozenset({"update", "setdefault", "pop", "popitem", "clear", "__setitem__",
                                "__delitem__"})
# In-place changes to a list/set/dict constant.
_MUTATORS = frozenset({"append", "extend", "insert", "remove", "pop", "clear", "add", "update",
                       "discard", "sort", "reverse", "setdefault", "popitem", "__setitem__",
                       "__delitem__", "__iadd__"})
_COLLECTION_CALLS = frozenset({"tuple", "list", "set", "frozenset", "sorted"})
_LOG_METHODS = frozenset({"debug", "info", "warning", "warn", "error", "exception", "critical", "log"})
# Builtins the generated expression relies on.
_HELPER_BUILTINS = frozenset({"isinstance", "str", "next", "len"})
# String methods whose result is made from the string they are called on.
_STR_TRANSFORMS = frozenset({
    "replace", "strip", "lstrip", "rstrip", "lower", "upper", "casefold", "removeprefix",
    "removesuffix", "format", "format_map", "expandtabs", "title", "capitalize", "swapcase",
    "zfill", "center", "ljust", "rjust", "translate", "split", "rsplit", "partition",
    "rpartition", "splitlines", "join", "encode"})
# Spark reader/writer methods whose first argument is the storage path: the
# write guard's job_migrate._WRITE_PATH_TERMINALS, plus `load`.
_STORAGE_METHODS = frozenset({"json", "parquet", "csv", "orc", "text", "save", "saveAsTextFile", "load"})
# Calls whose result is a yes/no answer.
_VERDICT_CALLS = frozenset({"bool", "isinstance", "issubclass", "callable", "any", "all", "hasattr"})
# Strings in S3/DBFS form; one the sweep leaves unchanged marks code that
# still expects the original form.
_SOURCE_FORMS = ("s3://", "s3a://", "s3n://", "dbfs:", "/dbfs")
# Exact-match conversion inlines the strings; anything this long is not a path.
_MAX_EXACT_STRING = 512
_OLD, _NEW = "_aidp_old", "_aidp_new"
_ARG = "_aidp_v"  # a helper's argument, in the conversion generated at its call
_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)
_SCOPE_NODES = _FUNCTION_NODES + (ast.ClassDef,) + _COMPREHENSIONS
_LOOPS = (ast.For, ast.AsyncFor, ast.While)
# Statements that can catch an exception (or suppress it) and carry on.
_CATCHING = (ast.Try, ast.With, ast.AsyncWith) + ((ast.TryStar,) if hasattr(ast, "TryStar") else ())
_MAGIC_LINE_RE = re.compile(r"^([ \t]*)(?:%|!(?!=))", re.M)
_RUN_LINE_RE = re.compile(r"^[ \t]*%run\b", re.M)
_IDENTIFIER_RE = re.compile(r"[A-Za-z_]\w*")
_LINE_RE = re.compile(r"[^\r\n]*(?:\r\n|\r|\n)|[^\r\n]+$")
# Positions where the sweep's patterns (s3://, s3a://, /dbfs, dbfs:) start
# inside a string; splitting the literal there leaves nothing to rewrite.
_SWEEP_SPLIT_RE = re.compile(r"(?<=s)(?=3)|(?<=d)(?=bfs)")


def _mask_magics(source: str) -> str:
    """Comment out IPython magic/shell lines; keeps every offset unchanged."""
    return _MAGIC_LINE_RE.sub(lambda m: m.group(1) + "#", source)


def _lines(text: str) -> List[str]:
    """Physical lines as the parser counts them: str.splitlines also splits
    on form feeds and other separators the parser treats as text."""
    return _LINE_RE.findall(text)


def _is_original_comment(line: str) -> bool:
    return line.lstrip().startswith("#") and line.rstrip("\r\n").endswith(ORIGINAL_MARK)


def sweep_keeping_originals(source: str, translate: Translate) -> str:
    """`translate(source)`, except the original lines this module kept as
    comments: the sweep is not comment-aware and would rewrite them."""
    swept = translate(source)
    before, after = _lines(source), _lines(swept)
    if len(before) != len(after):
        return swept
    return "".join(b if _is_original_comment(b) else a for b, a in zip(before, after))


def _caching(translate: Translate) -> Translate:
    cache: Dict[str, str] = {}

    def cached(text: str) -> str:
        if text not in cache:
            cache[text] = translate(text)
        return cache[text]

    return cached


def _source_form(text: str) -> bool:
    return any(form in text for form in _SOURCE_FORMS)


@dataclass(frozen=True)
class _Strings:
    """Strings a check compares against, each with the sweep's rewrite."""
    pairs: Tuple[Pair, ...]
    is_collection: bool
    constants: FrozenSet[str] = frozenset()  # notebook constants they came from

    @property
    def rewritten(self) -> Tuple[Pair, ...]:
        return tuple(p for p in self.pairs if p[0] != p[1])

    @property
    def keeps_source_form(self) -> bool:
        """Some string stays in S3/DBFS form after the sweep."""
        return any(o == m and _source_form(o) for o, m in self.pairs)


def _pair(node: ast.Constant, source: str, translate: Translate) -> Optional[Pair]:
    """The literal's value and what the sweep turns that literal's source into."""
    segment = ast.get_source_segment(source, node)
    if segment is None:
        return None
    try:
        migrated = ast.literal_eval("(" + translate(segment) + "\n)")
    except (SyntaxError, ValueError):
        return None
    return (node.value, migrated) if isinstance(migrated, str) else None


def _literal(node: ast.AST, source: str, translate: Translate) -> Optional[_Strings]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        pair = _pair(node, source, translate)
        return None if pair is None else _Strings((pair,), False)
    if (isinstance(node, (ast.Tuple, ast.List, ast.Set)) and node.elts
            and all(isinstance(e, ast.Constant) and isinstance(e.value, str) for e in node.elts)):
        pairs = [_pair(e, source, translate) for e in node.elts]
        return None if None in pairs else _Strings(tuple(pairs), True)
    return None


def _segment_rewritten(node: ast.AST, source: str, translate: Translate) -> bool:
    segment = ast.get_source_segment(source, node)
    return segment is not None and translate(segment) != segment


def _mentions_rewritten(node: ast.AST, source: str, translate: Translate,
                        names: FrozenSet[str] = frozenset()) -> bool:
    """A rewritten literal or f-string, or a name from `names`, anywhere in `node`."""
    for child in ast.walk(node):
        if isinstance(child, (ast.Constant, ast.JoinedStr)) and (
                isinstance(child, ast.JoinedStr) or isinstance(child.value, str)):
            if _segment_rewritten(child, source, translate):
                return True
        elif isinstance(child, ast.Name) and child.id in names:
            return True
    return False


def _own_nodes(scope_node: ast.AST, stop: Tuple[type, ...] = _SCOPE_NODES) -> Iterator[ast.AST]:
    """Nodes evaluated in `scope_node`'s own scope (not inside `stop` nodes)."""
    stack = list(scope_node.body) if isinstance(scope_node.body, list) else [scope_node.body]
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, stop):
            continue
        stack.extend(ast.iter_child_nodes(node))


def _target_names(target: ast.AST) -> Set[str]:
    return {n.id for n in ast.walk(target) if isinstance(n, ast.Name)}


@dataclass(eq=False)
class _Scope:
    kind: str  # "module" | "function" | "lambda" | "class" | "comprehension"
    node: ast.AST
    parent: Optional["_Scope"]
    bindings: Counter = field(default_factory=Counter)
    declared: Set[str] = field(default_factory=set)  # global/nonlocal here
    definitions: Set[str] = field(default_factory=set)  # bound by import/def/class

    def is_local(self, name: str) -> bool:
        return self.bindings[name] > 0 and name not in self.declared


class _ScopeVisitor(ast.NodeVisitor):
    """Python's binding rules, enough to tell where each name lives."""

    def __init__(self, tree: ast.Module) -> None:
        self.module = _Scope("module", tree, None)
        self.scope = self.module
        self.scope_of: Dict[int, _Scope] = {}
        self.rebinding_declared: Set[str] = set()  # names in any global/nonlocal
        self.mutated: Set[str] = set()  # changed in place (append, x[k] = ...)
        self.star_lines: List[int] = []
        self.dynamic = False
        self.function_loads: Set[str] = set()
        for stmt in tree.body:
            self.visit(stmt)

    def _effective(self, scope: _Scope) -> _Scope:
        while scope.kind == "comprehension":
            scope = scope.parent
        return scope

    def _bind(self, name: str, walrus: bool = False, definition: bool = False) -> None:
        scope = self._effective(self.scope) if walrus else self.scope
        scope.bindings[name] += 1
        if definition:
            scope.definitions.add(name)

    def _push(self, kind: str, node: ast.AST) -> None:
        self.scope = _Scope(kind, node, self.scope)

    def _pop(self) -> None:
        self.scope = self.scope.parent

    def _outer_parts(self, args: ast.arguments) -> None:
        for default in list(args.defaults) + [d for d in args.kw_defaults if d is not None]:
            self.visit(default)
        for arg in self._all_args(args):
            if arg.annotation is not None:
                self.visit(arg.annotation)

    @staticmethod
    def _all_args(args: ast.arguments) -> List[ast.arg]:
        extra = [a for a in (args.vararg, args.kwarg) if a is not None]
        return args.posonlyargs + args.args + args.kwonlyargs + extra

    def visit_FunctionDef(self, node: ast.AST) -> None:
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._outer_parts(node.args)
        if node.returns is not None:
            self.visit(node.returns)
        self._bind(node.name, definition=True)
        self._push("function", node)
        for arg in self._all_args(node.args):
            self._bind(arg.arg)
        for stmt in node.body:
            self.visit(stmt)
        self._pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self._outer_parts(node.args)
        self._push("lambda", node)
        for arg in self._all_args(node.args):
            self._bind(arg.arg)
        self.visit(node.body)
        self._pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for child in node.decorator_list + node.bases + node.keywords:
            self.visit(child)
        self._bind(node.name, definition=True)
        self._push("class", node)
        for stmt in node.body:
            self.visit(stmt)
        self._pop()

    def _comprehension(self, node: ast.AST) -> None:
        self.visit(node.generators[0].iter)
        self._push("comprehension", node)
        for index, generator in enumerate(node.generators):
            if index:
                self.visit(generator.iter)
            self.visit(generator.target)
            for condition in generator.ifs:
                self.visit(condition)
        for part in ((node.key, node.value) if isinstance(node, ast.DictComp) else (node.elt,)):
            self.visit(part)
        self._pop()

    visit_ListComp = visit_SetComp = visit_DictComp = visit_GeneratorExp = _comprehension

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self._bind(node.id)
        elif self._effective(self.scope).kind != "module" or self._in_generator():
            # Code that runs when called: a function, or a generator.
            self.function_loads.add(node.id)

    def _in_generator(self) -> bool:
        scope = self.scope
        while scope.kind == "comprehension":
            if isinstance(scope.node, ast.GeneratorExp):
                return True
            scope = scope.parent
        return False

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self.visit(node.value)
        self._bind(node.target.id, walrus=True)

    def _declare(self, node: ast.AST) -> None:
        self.scope.declared.update(node.names)
        self.rebinding_declared.update(node.names)

    visit_Global = visit_Nonlocal = _declare

    def _imports(self, node: ast.AST) -> None:
        for alias in node.names:
            if alias.name == "*":
                self.star_lines.append(node.lineno)
            else:
                self._bind((alias.asname or alias.name).split(".")[0], definition=True)

    visit_Import = visit_ImportFrom = _imports

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self._bind(node.name)
        self.generic_visit(node)

    def _match_capture(self, node: ast.AST) -> None:
        name = getattr(node, "name", None) or getattr(node, "rest", None)
        if name:
            self._bind(name)
        self.generic_visit(node)

    visit_MatchAs = visit_MatchStar = visit_MatchMapping = _match_capture

    @staticmethod
    def _namespace_call(node: ast.AST) -> bool:
        return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in _NAMESPACE_FUNCTIONS)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            if self._namespace_call(node.value):
                self.dynamic = True
            elif isinstance(node.value, ast.Name):
                self.mutated.add(node.value.id)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name) and func.id == "exec":
            self.dynamic = True
        if isinstance(func, ast.Attribute):
            if func.attr in _NAMESPACE_WRITERS and self._namespace_call(func.value):
                self.dynamic = True
            if func.attr in _MUTATORS and isinstance(func.value, ast.Name):
                self.mutated.add(func.value.id)
        self.scope_of[id(node)] = self.scope
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        self.scope_of[id(node)] = self.scope
        self.generic_visit(node)


def _parse(source: str) -> Optional[ast.Module]:
    try:
        return ast.parse(_mask_magics(source))
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        return None


def _compiles(source: str) -> bool:
    try:
        compile(_mask_magics(source), "<cell>", "exec", dont_inherit=True)
        return True
    except (SyntaxError, ValueError, RecursionError, MemoryError):
        return False


@dataclass(frozen=True)
class _CellFacts:
    """What one cell contributes to the notebook-wide constant table."""
    bindings: Counter
    candidates: Dict[str, Tuple[int, _Strings]]  # name -> (line, strings)
    poisoned: FrozenSet[str]  # rebound via global/nonlocal, or changed in place
    rebinder_lines: Tuple[int, ...]  # star imports and %run: may rebind any name
    dynamic: bool  # exec, or writes through globals()/vars()/locals()
    affected: FrozenSet[str]  # module names assigned a value with a rewritten literal
    affected_attrs: FrozenSet[str]  # class attributes assigned such a value
    function_loads: FrozenSet[str]
    definitions: FrozenSet[str]  # module names bound by import/def/class
    functions: Dict[str, int]  # top-level function -> line of its def
    # (name, key, kind) -> how often module-level code reads `name`: as
    # name["key"] / name.get("key") ("key"), on its own line or printed
    # ("display", key None), or any other way ("other", key None).
    accesses: Counter


def _unreadable_facts(source: str) -> _CellFacts:
    # Bindings in a cell that cannot be read are unknown: every name it
    # mentions may be rebound or read there.
    names = frozenset(_IDENTIFIER_RE.findall(source))
    return _CellFacts(Counter({n: 1 for n in names}), {}, names, (), False,
                      frozenset(), frozenset(), names, frozenset(), {},
                      Counter({(n, None, "other"): 1 for n in names}))


def _access(name: ast.Name, parents: Dict[int, ast.AST]) -> Tuple[Optional[str], str]:
    """How module-level code reads `name` (see _CellFacts.accesses)."""
    parent = parents.get(id(name))
    if isinstance(parent, ast.Subscript) and parent.value is name:
        key = parent.slice
        if (isinstance(parent.ctx, ast.Load) and isinstance(key, ast.Constant)
                and isinstance(key.value, str)):
            return key.value, "key"
        return None, "other"
    if isinstance(parent, ast.Attribute) and parent.value is name and parent.attr == "get":
        call = parents.get(id(parent))
        if (isinstance(call, ast.Call) and call.func is parent and call.args
                and isinstance(call.args[0], ast.Constant) and isinstance(call.args[0].value, str)):
            return call.args[0].value, "key"
        return None, "other"
    if isinstance(parent, ast.Expr):
        return None, "display"
    if (isinstance(parent, ast.Call) and any(a is name for a in parent.args)
            and isinstance(parent.func, ast.Name) and parent.func.id in ("print", "display")):
        return None, "display"
    return None, "other"


def _module_accesses(tree: ast.Module) -> Counter:
    """Module-level reads of each name, by key (see _CellFacts.accesses).
    Function, lambda and class bodies are left out: _ScopeVisitor records
    those reads in function_loads."""
    parents = {id(child): node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
    counts: Counter = Counter()
    stack: List[ast.AST] = list(tree.body)
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            args = node.args
            stack += list(args.defaults) + [d for d in args.kw_defaults if d is not None]
            if not isinstance(node, ast.Lambda):
                stack += list(node.decorator_list)
                stack += [a.annotation for a in _ScopeVisitor._all_args(args) if a.annotation]
                stack += [node.returns] if node.returns is not None else []
            continue
        if isinstance(node, ast.ClassDef):
            stack += list(node.decorator_list) + list(node.bases) + [k.value for k in node.keywords]
            continue
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            counts[(node.id,) + _access(node, parents)] += 1
        stack.extend(ast.iter_child_nodes(node))
    return counts


def _cell_facts(source: str, translate: Translate) -> _CellFacts:
    tree = _parse(source)
    if tree is None:
        return _unreadable_facts(source)
    try:
        visitor = _ScopeVisitor(tree)
        accesses = _module_accesses(tree)
    except RecursionError:
        return _unreadable_facts(source)
    candidates, affected, affected_attrs = {}, set(), set()
    for stmt in tree.body:
        if isinstance(stmt, ast.ClassDef):
            for inner in stmt.body:
                if isinstance(inner, (ast.Assign, ast.AnnAssign)) and inner.value is not None:
                    if _mentions_rewritten(inner.value, source, translate):
                        targets = inner.targets if isinstance(inner, ast.Assign) else [inner.target]
                        affected_attrs.update(n for t in targets for n in _target_names(t))
            continue
        if not isinstance(stmt, (ast.Assign, ast.AnnAssign, ast.AugAssign)) or stmt.value is None:
            continue
        targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
        names = {n for t in targets for n in _target_names(t)}
        if _mentions_rewritten(stmt.value, source, translate):
            affected.update(names)
        if (not isinstance(stmt, ast.AugAssign) and len(targets) == 1
                and isinstance(targets[0], ast.Name)):
            value = _literal(stmt.value, source, translate)
            if value is not None:
                candidates[targets[0].id] = (stmt.lineno, value)
    run_lines = [source.count("\n", 0, m.start()) + 1 for m in _RUN_LINE_RE.finditer(source)]
    return _CellFacts(
        visitor.module.bindings, candidates,
        frozenset(visitor.rebinding_declared | visitor.mutated),
        tuple(sorted(visitor.star_lines + run_lines)), visitor.dynamic,
        frozenset(affected), frozenset(affected_attrs), frozenset(visitor.function_loads),
        frozenset(visitor.module.definitions),
        {s.name: s.lineno for s in tree.body if isinstance(s, (ast.FunctionDef, ast.AsyncFunctionDef))},
        accesses)


@dataclass(frozen=True)
class _Table:
    """Notebook constants: names bound once at module level, to strings."""
    index: int  # the cell being converted
    constants: Dict[str, _Strings]
    excluded: Dict[str, Tuple[_Strings, str]]  # name -> (strings, why not a constant)
    module_bindings: Counter
    poisoned: FrozenSet[str]
    dynamic: bool
    affected: FrozenSet[str]
    affected_attrs: FrozenSet[str]
    function_loads: FrozenSet[str]
    definitions: FrozenSet[str]
    # Top-level functions bound exactly once in the notebook, by their def,
    # that nothing can rebind later.
    stable_functions: FrozenSet[str]
    accesses: Counter  # module-level reads across the notebook (see _CellFacts)


class _Site(NamedTuple):
    check: ast.AST
    subject: ast.AST
    pattern: ast.AST
    value: Optional[_Strings]
    kind: str  # "startswith" | "contains" | "equals" | "member" | "suffix"


@dataclass
class _Edit:
    name: str
    mode: str  # "prefix" | "exact"
    guard: str  # the original check, applied to the value as it is
    statement: ast.stmt
    scope: _Scope
    check: ast.AST
    subject: ast.Name
    pattern: ast.AST
    # The branch a converted value takes, and its statement rebinding the value.
    kill: Optional[Tuple[List[ast.stmt], ast.stmt]] = None
    exits: bool = False  # that branch always leaves the function
    pairs: List[Pair] = field(default_factory=list)
    constants: Set[str] = field(default_factory=set)
    # Converting at a call: the path helper called, and its check's kind.
    # `subject` is then the call's argument and `name` the helper's parameter.
    helper: Optional[str] = None
    site_kind: str = ""
    # The notebook variable (and key) only that call reads, when there is one.
    holder: Optional[Tuple[str, Optional[str]]] = None


@dataclass(frozen=True)
class Conversion:
    source: str
    notes: Tuple[str, ...]
    used: Dict[str, Tuple[Pair, ...]]  # notebook constant -> pairs it supplied
    # Notebook variables (and keys) that converted calls assume nothing else reads.
    holders: Tuple[Tuple[str, Optional[str]], ...] = ()


class _CellConverter:
    """Plans the conversions for one cell (original, pre-sweep source)."""

    def __init__(self, source: str, tree: ast.Module, table: _Table, translate: Translate) -> None:
        self.source, self.tree, self.table, self.translate = source, tree, table, translate
        self.scopes = _ScopeVisitor(tree)
        self.parents: Dict[int, ast.AST] = {
            id(child): node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        self.edits: Dict[Tuple[int, int, int, int], _Edit] = {}
        self.notes: List[Tuple[int, str]] = []
        self.used: Dict[str, Tuple[Pair, ...]] = {}
        self.handled: Set[int] = set()
        # Nodes inside a conversion an earlier pass generated.
        self.handled.update(id(inner) for node in ast.walk(tree)
                            if isinstance(node, ast.NamedExpr) and _OLD in ast.unparse(node.value)
                            for inner in ast.walk(node.value))
        self.handled.update(id(inner) for node in ast.walk(tree)
                            if isinstance(node, ast.GeneratorExp) and any(
                                isinstance(t, ast.Name) and t.id == _ARG
                                for g in node.generators for t in ast.walk(g.target))
                            for inner in ast.walk(node))
        # Every name this cell binds anywhere (a parameter, a local, a def...).
        self.bound_anywhere: Set[str] = (
            {n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and not isinstance(n.ctx, ast.Load)}
            | {n.arg for n in ast.walk(tree) if isinstance(n, ast.arg)}
            | {n.name for n in ast.walk(tree) if isinstance(n, (
                ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.ExceptHandler)) and n.name}
            | {(a.asname or a.name).split(".")[0] for n in ast.walk(tree)
               if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names})
        self._assigned_cache: Dict[Tuple[int, str], Optional[ast.AST]] = {}
        self._rebinds_cache: Dict[Tuple[int, str], bool] = {}
        self._bindings_cache: Dict[int, List[Tuple[Set[str], Optional[ast.AST], Position, ast.AST]]] = {}

    def _ancestors(self, node: ast.AST) -> Iterator[ast.AST]:
        while id(node) in self.parents:
            node = self.parents[id(node)]
            yield node

    def _note(self, line: int, text: str) -> None:
        self.notes.append((line, text))

    def _shadowed(self, names: Iterable[str], scope: _Scope) -> List[str]:
        return sorted(n for n in names if self.table.module_bindings[n] or any(
            s.bindings[n] for s in self._scope_chain(scope)))

    # ── resolving the strings a check compares against ──────────────────
    def _resolve(self, node: ast.AST, scope: _Scope, depth: int = 0) -> Optional[_Strings]:
        if depth > 4:
            return None
        value = _literal(node, self.source, self.translate)
        if value is not None:
            return value
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)) and node.elts:
            parts = [self._resolve(e, scope, depth + 1) for e in node.elts]
            if any(p is None or p.is_collection for p in parts):
                return None
            return _Strings(tuple(pair for p in parts for pair in p.pairs), True,
                            frozenset().union(*(p.constants for p in parts)))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self._resolve(node.left, scope, depth + 1), self._resolve(node.right, scope, depth + 1)
            if left is None or right is None or left.is_collection != right.is_collection:
                return None
            constants = left.constants | right.constants
            if left.is_collection:
                return _Strings(left.pairs + right.pairs, True, constants)
            (lo, lm), (ro, rm) = left.pairs[0], right.pairs[0]
            return _Strings(((lo + ro, lm + rm),), False, constants)
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id in _COLLECTION_CALLS and len(node.args) == 1 and not node.keywords
                and not self._shadowed([node.func.id], scope)):
            value = self._resolve(node.args[0], scope, depth + 1)
            return value if value is not None and value.is_collection else None
        if isinstance(node, ast.JoinedStr):
            return self._resolve_fstring(node, scope, depth)
        if not isinstance(node, ast.Name):
            return None
        loop = self._enclosing_loop(node)
        if loop is not None:
            value = self._resolve(loop.iter, scope, depth + 1)
            if value is None or not value.is_collection:
                return None  # a loop over one string yields its characters
            return _Strings(value.pairs, False, value.constants)
        innermost = True
        while scope.kind != "module":
            if node.id in scope.declared:
                return None  # global/nonlocal: rebound from a function
            if node.id in scope.bindings:
                if scope.kind in ("function", "lambda"):
                    assigned = self._assigned_once(scope, node.id)
                    return None if assigned is None else self._resolve(assigned, scope, depth + 1)
                if scope.kind == "comprehension" or innermost:
                    return None  # a comprehension variable or class attribute
            innermost = False
            scope = scope.parent  # class bodies are not visible from methods
        value = self.table.constants.get(node.id)
        if value is None:
            return None
        return _Strings(value.pairs, value.is_collection, frozenset({node.id}))

    def _resolve_fstring(self, node: ast.JoinedStr, scope: _Scope, depth: int) -> Optional[_Strings]:
        """An f-string whose fields are plain names of single strings, e.g.
        f"s3://{BUCKET}/": its value as written and after the sweep rewrites
        its text (both evaluated with the fields' values at that point)."""
        fields: Dict[str, Pair] = {}
        constants: FrozenSet[str] = frozenset()
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                if not _plain_field(part):
                    return None
                value = self._resolve(part.value, scope, depth + 1)
                if value is None or value.is_collection:
                    return None
                fields[part.value.id] = value.pairs[0]
                constants |= value.constants
        segment = ast.get_source_segment(self.source, node)
        try:
            swept = ast.parse("(" + self.translate(segment) + "\n)", mode="eval").body if segment else None
        except (SyntaxError, ValueError):
            return None
        if not isinstance(swept, ast.JoinedStr) or any(
                isinstance(p, ast.FormattedValue) and (not _plain_field(p) or p.value.id not in fields)
                for p in swept.values):
            return None

        def render(joined: ast.JoinedStr, side: int) -> str:
            return "".join(p.value if isinstance(p, ast.Constant) else fields[p.value.id][side]
                           for p in joined.values)

        return _Strings(((render(node, 0), render(swept, 1)),), False, constants)

    def _assigned_once(self, scope: _Scope, name: str) -> Optional[ast.AST]:
        """The value of the one assignment that binds `name` in `scope` itself."""
        key = (id(scope), name)
        if key not in self._assigned_cache:
            value = None
            parameters = {a.arg for a in _ScopeVisitor._all_args(scope.node.args)}
            if (scope.bindings[name] == 1 and name not in parameters
                    and name not in self.scopes.rebinding_declared):
                found = [n for n in _own_nodes(scope.node)
                         if (isinstance(n, ast.Assign) and len(n.targets) == 1
                             and isinstance(n.targets[0], ast.Name) and n.targets[0].id == name)
                         or (isinstance(n, ast.AnnAssign) and n.value is not None
                             and isinstance(n.target, ast.Name) and n.target.id == name)]
                value = found[0].value if len(found) == 1 else None
            self._assigned_cache[key] = value
        return self._assigned_cache[key]

    def _enclosing_loop(self, name: ast.Name) -> Optional[ast.AST]:
        """The `for name in ...` loop whose body evaluates `name`, when nothing
        else can rebind the loop variable while the body runs."""
        child = name
        for node in self._ancestors(name):
            if isinstance(node, _SCOPE_NODES):
                return None
            if (isinstance(node, (ast.For, ast.AsyncFor)) and isinstance(node.target, ast.Name)
                    and node.target.id == name.id):
                if not any(child is stmt for stmt in node.body) or self._rebinds(node, name.id):
                    return None
                at_module = not any(isinstance(a, _SCOPE_NODES) for a in self._ancestors(node))
                if name.id in self.scopes.rebinding_declared or (
                        at_module and (self.table.dynamic or name.id in self.table.poisoned)):
                    return None
                return node
            child = node
        return None

    def _rebinds(self, loop: ast.AST, name: str) -> bool:
        key = (id(loop), name)
        if key not in self._rebinds_cache:
            self._rebinds_cache[key] = any(_binds(node, name) for stmt in loop.body
                                           for node in ast.walk(stmt))
        return self._rebinds_cache[key]

    # ── is an operand a string the sweep rewrote? ───────────────────────
    def _affected(self, node: ast.AST, scope: _Scope) -> bool:
        if isinstance(node, ast.JoinedStr):
            return _segment_rewritten(node, self.source, self.translate) or any(
                self._affected(part.value, scope) for part in node.values
                if isinstance(part, ast.FormattedValue))
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return _segment_rewritten(node, self.source, self.translate)
        if isinstance(node, ast.Name):
            return self._name_affected(node, scope)
        if isinstance(node, ast.Attribute):
            return node.attr in self.table.affected_attrs
        if isinstance(node, ast.Subscript):
            return self._affected(node.value, scope)
        if isinstance(node, ast.BinOp):
            return self._affected(node.left, scope) or self._affected(node.right, scope)
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            return any(self._affected(e, scope) for e in node.elts)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.args:
            return node.func.id in _COLLECTION_CALLS and self._affected(node.args[0], scope)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            return node.func.attr in ("keys", "values", "items") and self._affected(node.func.value, scope)
        return False

    def _mentions(self, node: ast.AST) -> bool:
        return _mentions_rewritten(node, self.source, self.translate, self.table.affected)

    def _name_affected(self, name: ast.Name, scope: _Scope) -> bool:
        for node in self._ancestors(name):
            if isinstance(node, _SCOPE_NODES):
                break
            if isinstance(node, (ast.For, ast.AsyncFor)) and name.id in _target_names(node.target):
                return self._mentions(node.iter)
        if scope.kind in ("function", "lambda") and scope.is_local(name.id):
            if self._default_affected(name.id, scope):
                return True
            return any(self._mentions(node.value) for node in _own_nodes(scope.node)
                       if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign))
                       and node.value is not None
                       and name.id in {n for t in (node.targets if isinstance(node, ast.Assign)
                                                   else [node.target]) for n in _target_names(t)})
        return name.id in self.table.affected

    def _default_affected(self, name: str, scope: _Scope) -> bool:
        args = scope.node.args
        positional = args.posonlyargs + args.args
        defaults = dict(zip([a.arg for a in positional[len(positional) - len(args.defaults):]],
                            args.defaults))
        defaults.update((a.arg, d) for a, d in zip(args.kwonlyargs, args.kw_defaults) if d is not None)
        return name in defaults and self._mentions(defaults[name])

    # ── finding checks ──────────────────────────────────────────────────
    def _sites(self) -> Iterator[_Site]:
        for node in ast.walk(self.tree):
            scope = self.scopes.scope_of.get(id(node))
            if scope is None or id(node) in self.handled:
                continue
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr in _STR_METHODS and node.args):
                kind = _PREFIX_METHODS.get(node.func.attr, "suffix")
                pattern = node.args[0]
                yield _Site(node, node.func.value, pattern, self._resolve(pattern, scope), kind)
            elif isinstance(node, ast.Compare) and len(node.ops) == 1:
                op, left, right = node.ops[0], node.left, node.comparators[0]
                if isinstance(op, (ast.Eq, ast.NotEq)):
                    right_value = self._resolve(right, scope)
                    if right_value is not None:
                        yield _Site(node, left, right, right_value, "equals")
                    else:
                        yield _Site(node, right, left, self._resolve(left, scope), "equals")
                elif isinstance(op, (ast.In, ast.NotIn)):
                    right_value = self._resolve(right, scope)
                    if right_value is not None and right_value.is_collection:
                        yield _Site(node, left, right, right_value, "member")
                    else:
                        left_value = self._resolve(left, scope)
                        if left_value is not None and not left_value.is_collection:
                            yield _Site(node, right, left, left_value, "contains")

    def _refusal(self, site: _Site) -> Optional[str]:
        """Why converting this check could change behaviour, or None."""
        check, subject, value = site.check, site.subject, site.value
        if site.kind == "suffix":
            return "suffix checks are not converted"
        if site.kind == "contains" and value.is_collection:
            return "the replaced value is not a single string"
        if site.kind == "equals" and value.is_collection:
            return "it compares a value with a collection"
        if site.kind in ("equals", "member") and any(
                len(o) > _MAX_EXACT_STRING or "\n" in o for o, _ in value.rewritten):
            return "it compares against a long or multi-line string"
        if not isinstance(subject, ast.Name):
            return f"`{ast.unparse(subject)[:60]}` is not a plain variable"
        if subject.id in (_OLD, _NEW):
            return f"`{subject.id}` is a name the conversion itself uses"
        for node in self._ancestors(check):
            if isinstance(node, (ast.JoinedStr, ast.FormattedValue)):
                return "the check is inside an f-string"
            if isinstance(node, _COMPREHENSIONS):
                return "the check is inside a comprehension"
        scope = self.scopes.scope_of[id(check)]
        if scope.kind == "lambda":
            return "the check is inside a lambda"
        if scope.kind == "class":
            return "the check is in a class body"
        if scope.kind == "module":
            return ("the check is not inside a function, so later cells and notebooks that "
                    "%run this one can read the value")
        if not scope.is_local(subject.id):
            return f"`{subject.id}` is not a local variable of its function"
        shadowed = self._shadowed(_HELPER_BUILTINS, scope)
        if shadowed:
            return f"`{shadowed[0]}` is redefined in this notebook"
        return None

    @staticmethod
    def _scope_chain(scope: _Scope) -> Iterator[_Scope]:
        while scope is not None:
            yield scope
            scope = scope.parent

    def _statement(self, node: ast.AST) -> ast.stmt:
        for ancestor in self._ancestors(node):
            if isinstance(ancestor, ast.stmt):
                return ancestor
        raise ValueError("check outside any statement")

    def _guard(self, site: _Site) -> str:
        """The original check applied to the value as it is (single line)."""
        name = site.subject.id
        segment = ast.get_source_segment(self.source, site.pattern)
        pattern = segment if segment and "\n" not in segment and "\r" not in segment \
            else ast.unparse(site.pattern)
        return {
            "startswith": f"{name}.startswith({pattern})",
            "contains": f"({pattern}) in {name}",
            "equals": f"{name} == ({pattern})",
            "member": f"{name} in ({pattern})",
        }[site.kind]

    def plan(self) -> None:
        sites = list(self._sites())
        self.sites_by_subject = {id(s.subject): s for s in sites}
        helpers: Dict[int, Tuple[_Scope, List[_Site]]] = {}
        for site in sites:
            check, subject, value = site.check, site.subject, site.value
            scope = self.scopes.scope_of[id(check)]
            if value is None:
                self._note_unresolved(site)
                continue
            self.handled.add(id(check))
            if not value.rewritten:
                continue
            if isinstance(subject, ast.NamedExpr) and _OLD in ast.unparse(subject):
                continue  # converted by an earlier pass
            if isinstance(subject, ast.Name) and self._resolve(subject, scope) is not None:
                continue  # constant compared with constant: no runtime value
            why = self._refusal(site)
            if why:
                self._refused(check, why, value.rewritten)
                continue
            if subject.id in self._parameters(scope):
                # The caller holds the value too: only its calls can be converted.
                helpers.setdefault(id(scope.node), (scope, []))[1].append(site)
                continue
            key = (subject.lineno, subject.col_offset, subject.end_lineno, subject.end_col_offset)
            mode = "exact" if site.kind in ("equals", "member") else "prefix"
            kill = self._kill(check, subject.id)
            edit = self.edits.setdefault(key, _Edit(
                subject.id, mode, self._guard(site), self._statement(check), scope, check,
                subject, site.pattern, kill, kill is None and self._exits(check, scope)))
            edit.pairs += [p for p in value.rewritten if p not in edit.pairs]
            edit.constants |= value.constants
        self._drop_unsafe_persistence()
        for scope, helper_sites in helpers.values():
            self._plan_helper(scope, helper_sites)
        for edit in self.edits.values():
            for name in edit.constants:
                self.used[name] = self.table.constants[name].pairs
        self._note_unhandled()

    def _refused(self, check: ast.AST, why: str, pairs: Sequence[Pair]) -> None:
        self._note(check.lineno, f"path check NOT converted ({why}); it compares against "
                   f"{_describe(pairs)} and may no longer match — verify")

    # ── a converted value stays bound: every later use must accept it ───
    def _drop_unsafe_persistence(self) -> None:
        groups: Dict[Tuple[int, str], List[Tuple[Tuple[int, int, int, int], _Edit]]] = {}
        for key, edit in self.edits.items():
            groups.setdefault((id(edit.scope), edit.name), []).append((key, edit))
        for members in groups.values():
            problem = self._persistence_problem([edit for _, edit in members])
            if problem:
                for key, edit in members:
                    del self.edits[key]
                    self._refused(edit.check, problem, edit.pairs)

    def _persistence_problem(self, edits: List[_Edit]) -> Optional[str]:
        """Why converting `name` at `edits` could let a value other than the
        converted one be used, or give the converted one to code expecting
        the S3/DBFS form; None when neither can happen."""
        name, scope = edits[0].name, edits[0].scope
        subjects = {id(e.subject) for e in edits}
        patterns = {e.pattern.id for e in edits if isinstance(e.pattern, ast.Name)}
        # Code that runs when called (a closure, a generator) sees the value
        # whenever it is called while the value is converted.
        deferred = self._deferred_read(name, scope) is not None
        names = [n for n in ast.walk(scope.node) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)]
        for edit in edits:
            why = self._ownership(edit)
            if why:
                return why
            if self._verdict_leaves(edit.check):
                return "its yes/no result is used as a value, which can leave the function"
            if edit.exits and not self._returns_converted(edit.check, name):
                return ("the branch a converted value takes returns something other than the "
                        "value, so the result of the check leaves the function")
            window = self._window(edit)
            if deferred and (window is None or self._calls_out(window, name)):
                return (f"`{name}` is also read by a nested function or generator, which may run "
                        "while it is converted")
            for use in self._uses_while_converted(edit, names, name, scope):
                if (use.id == name and id(use) not in subjects
                        and not self._accepts_migrated(use, scope, patterns)):
                    return (f"`{name}` is also used at line {use.lineno} in a way that may "
                            "expect its S3/DBFS form")
            start = (edit.subject.lineno, edit.subject.col_offset)
            sources, copies, origins = self._family(name, scope, start, patterns)
            reached = self._reached(edit, scope)
            for use in reached:
                if (isinstance(use, ast.Name) and isinstance(use.ctx, ast.Load)
                        and use.id in sources | copies
                        and not self._unconverted_read_is_harmless(use, name, scope, start)):
                    if use.id in sources:
                        return (f"`{name}` is a copy of `{use.id}`, which is used unconverted "
                                f"after the check (line {use.lineno})")
                    return (f"`{use.id}` keeps a copy of `{name}` from before the check and is "
                            f"used after it (line {use.lineno})")
            for node in reached:
                if isinstance(node, ast.Call) and ast.unparse(node) in origins:
                    return (f"the function evaluates `{name}`'s source again at line "
                            f"{node.lineno}, and that value is not converted")
        return None

    # ── does the function own the value it checks? ──────────────────────
    def _ownership(self, edit: _Edit) -> Optional[str]:
        """None when the function builds the value its check reads itself;
        otherwise why another place may hold the unconverted value. Only the
        bindings that can reach the check count: those before it, or in a
        loop around it."""
        scope, start = edit.scope, (edit.subject.lineno, edit.subject.col_offset)
        loops = self._loops_around([edit], scope, edit.name)
        bindings = [(targets, value) for targets, value, position, node in self._bindings_of(scope)
                    if position < start or any(self._inside(node, loop) for loop in loops)]
        parameters = self._parameters(scope)
        seen: Set[str] = set()
        frontier = [edit.name]
        while frontier:
            current = frontier.pop()
            if current in seen:
                continue
            seen.add(current)
            if current in parameters:
                return (f"`{current}` comes from the caller, which keeps its own unconverted "
                        "value")
            if not scope.is_local(current):
                return self._shared(current, scope)
            values = [value for targets, value in bindings if current in targets]
            if not values or any(value is None for value in values):
                return f"`{current}` is bound in a way whose value cannot be followed"
            for value in values:
                frontier.extend(self._lineage(value))
        return None

    @staticmethod
    def _shared(name: str, scope: _Scope) -> str:
        """Why `name`, which the function does not bind, can be read by other
        code too. (A kernel API's result, e.g. from spark.sql, never names
        its object: see _call_lineage.)"""
        if name in scope.declared:
            return f"`{name}` is a global or nonlocal variable"
        for outer in _CellConverter._scope_chain(scope.parent):
            if outer.kind in ("function", "lambda") and outer.is_local(name):
                return f"`{name}` comes from an enclosing function"
        return f"`{name}` is a notebook or kernel variable that other code can read"

    def _kernel_object(self, name: str) -> bool:
        """Provided by the kernel (`spark`, `dbutils`, builtins): bound nowhere
        in this notebook."""
        return (name not in self.bound_anywhere and self.table.module_bindings[name] == 0
                and name not in self.table.definitions)

    def _lineage(self, node: ast.AST, depth: int = 0) -> Set[str]:
        """Names whose values `node`'s value is made from: the receiver of a
        string method, the leading part of a concatenation or f-string, the
        container of a subscript, the arguments of a function. A kernel API
        call (`spark.sql(...)`) returns new data."""
        if depth > 24:
            return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        inner = depth + 1
        if isinstance(node, ast.Constant):
            return set()
        if isinstance(node, ast.Name):
            return {node.id}
        if isinstance(node, ast.JoinedStr):
            first = node.values[0] if node.values else None
            return self._lineage(first.value, inner) if isinstance(first, ast.FormattedValue) else set()
        if isinstance(node, ast.BinOp):
            return self._lineage(node.left, inner)
        if isinstance(node, (ast.Subscript, ast.Attribute, ast.Starred)):
            return self._lineage(node.value, inner)
        if isinstance(node, ast.NamedExpr):
            return self._lineage(node.value, inner) | {node.target.id}
        if isinstance(node, ast.IfExp):
            return self._lineage(node.body, inner) | self._lineage(node.orelse, inner)
        if isinstance(node, (ast.BoolOp, ast.Dict, ast.Tuple, ast.List, ast.Set)):
            parts = node.values if isinstance(node, (ast.BoolOp, ast.Dict)) else node.elts
            return set().union(*(self._lineage(p, inner) for p in parts if p is not None))
        if isinstance(node, _COMPREHENSIONS):
            # Elements come from the iterables (the conditions only filter).
            targets = {n for g in node.generators for n in _target_names(g.target)}
            parts = [g.iter for g in node.generators] + (
                [node.key, node.value] if isinstance(node, ast.DictComp) else [node.elt])
            return set().union(*(self._lineage(p, inner) for p in parts)) - targets
        if isinstance(node, ast.Call):
            return self._call_lineage(node, inner)
        if isinstance(node, (ast.Compare, ast.UnaryOp)):
            return set()  # a yes/no or a number, not a path
        return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}

    def _call_lineage(self, call: ast.Call, depth: int) -> Set[str]:
        arguments = set().union(*(self._lineage(a, depth) for a in
                                  [*call.args, *(k.value for k in call.keywords)]))
        func = call.func
        if isinstance(func, ast.Attribute):
            receiver = func.value
            if func.attr in _STR_TRANSFORMS:
                made = self._lineage(receiver, depth)
                if func.attr == "join" or (func.attr in ("format", "format_map")
                                           and not _leads_with_text(receiver)):
                    made |= arguments
                return made
            root = _root_name(receiver)
            if root is not None and self._kernel_object(root):
                return set()
            if root is not None and root in self.table.definitions:
                return arguments  # a module's function, or a class
            return self._lineage(receiver, depth) | arguments
        if isinstance(func, ast.Name):
            if self._kernel_object(func.id) or func.id in self.table.definitions:
                return arguments
            return arguments | {func.id}
        return self._lineage(func, depth) | arguments

    def _bindings_of(self, scope: _Scope) -> List[Tuple[Set[str], Optional[ast.AST], Position, ast.AST]]:
        """(names, value, position, node) for each binding in `scope` itself;
        value None when it cannot be followed (import, def, except, match)."""
        key = id(scope)
        if key not in self._bindings_cache:
            found = []
            for node in _own_nodes(scope.node):
                if isinstance(node, ast.Assign):
                    bound = [(set().union(*(_bound_names(t) for t in node.targets)), node.value)]
                elif isinstance(node, (ast.AnnAssign, ast.AugAssign)) and node.value is not None:
                    bound = [(_bound_names(node.target), node.value)]
                elif isinstance(node, ast.NamedExpr):
                    bound = [({node.target.id}, node.value)]
                elif isinstance(node, (ast.For, ast.AsyncFor)):
                    bound = [(_bound_names(node.target), node.iter)]
                elif isinstance(node, (ast.With, ast.AsyncWith)):
                    bound = [(_bound_names(i.optional_vars), i.context_expr)
                             for i in node.items if i.optional_vars is not None]
                elif isinstance(node, _COMPREHENSIONS):
                    # An assignment expression in a comprehension binds here.
                    bound = [({n.target.id}, None) for n in ast.walk(node)
                             if isinstance(n, ast.NamedExpr)]
                elif isinstance(node, (ast.ExceptHandler, ast.FunctionDef, ast.AsyncFunctionDef,
                                       ast.ClassDef, ast.MatchAs, ast.MatchStar)) and node.name:
                    bound = [({node.name}, None)]
                elif isinstance(node, ast.MatchMapping) and node.rest:
                    bound = [({node.rest}, None)]
                elif isinstance(node, (ast.Import, ast.ImportFrom)):
                    bound = [({(a.asname or a.name).split(".")[0] for a in node.names}, None)]
                else:
                    continue
                position = (getattr(node, "lineno", 0), getattr(node, "col_offset", 0))
                found += [(targets, value, position, node) for targets, value in bound]
            self._bindings_cache[key] = found
        return self._bindings_cache[key]

    def _family(self, name: str, scope: _Scope, start: Position,
                patterns: Set[str]) -> Tuple[Set[str], Set[str], Set[str]]:
        """At the check: the function's variables `name` was built from, the
        variables built from it (or them), and the source calls of its value."""
        bindings = [(targets, value) for targets, value, position, _ in self._bindings_of(scope)
                    if value is not None and position < start]

        def variable(other: str) -> bool:
            return other != name and other not in patterns and scope.is_local(other)

        sources: Set[str] = set()
        origins: Set[str] = set()
        frontier = [name]
        while frontier:
            current = frontier.pop()
            for targets, value in bindings:
                if current not in targets:
                    continue
                origins |= {ast.unparse(c) for c in ast.walk(value)
                            if isinstance(c, ast.Call) and _source_call(c)}
                for other in self._lineage(value):
                    if variable(other) and other not in sources:
                        sources.add(other)
                        frontier.append(other)
        held = sources | {name}
        copies: Set[str] = set()
        changed = True
        while changed:
            changed = False
            for targets, value in bindings:
                if self._lineage(value) & (held | copies):
                    new = {t for t in targets if variable(t) and t not in held | copies}
                    if new:
                        copies |= new
                        changed = True
        return sources, copies, origins

    def _reached(self, edit: _Edit, scope: _Scope) -> List[ast.AST]:
        """Nodes that can run after the check once a converted value takes
        it: the branch that leaves the function; or the branch that rebinds
        the value and everything after its `if` (the other variables still
        hold the unconverted value); or everything after the check."""
        start = (edit.subject.lineno, edit.subject.col_offset)
        taken = self._taken_branch(edit.check)
        if edit.exits and taken is not None:
            return [n for statement in taken[1] for n in ast.walk(statement)]
        loops = self._loops_around([edit], scope, edit.name)
        nodes = [n for n in ast.walk(scope.node) if hasattr(n, "lineno")]
        if edit.kill is not None and taken is not None:
            branch, statements = taken
            end = (branch.end_lineno, branch.end_col_offset)
            return ([n for statement in statements for n in ast.walk(statement)]
                    + [n for n in nodes if (n.lineno, n.col_offset) >= end
                       or any(self._inside(n, loop) for loop in loops)])
        return [n for n in nodes if (n.lineno, n.col_offset) > start
                or any(self._inside(n, loop) for loop in loops)]

    def _unconverted_read_is_harmless(self, use: ast.Name, name: str, scope: _Scope,
                                      start: Position) -> bool:
        """`use` reads a variable still holding the unconverted value only in
        an error message or logging, or after it is assigned again from
        `name`."""
        if self._in_error_message(use) or self._logged(use):
            return True
        for statement in self._simple_assignments(scope, use.id):
            if ((statement.lineno, statement.col_offset) <= start
                    or name not in self._lineage(statement.value)):
                continue
            block = self._block(statement)
            if block is None:
                continue
            index = next(i for i, s in enumerate(block) if s is statement)
            for ancestor in (use, *self._ancestors(use)):
                position = next((i for i, s in enumerate(block) if s is ancestor), None)
                if position is not None:
                    if position > index:
                        return True
                    break
        return False

    def _simple_assignments(self, scope: _Scope, name: str) -> List[ast.stmt]:
        return [n for n in _own_nodes(scope.node)
                if (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name
                                                      for t in n.targets))
                or (isinstance(n, ast.AnnAssign) and n.value is not None
                    and isinstance(n.target, ast.Name) and n.target.id == name)]

    def _block(self, statement: ast.stmt) -> Optional[List[ast.stmt]]:
        """The statement list `statement` is in."""
        parent = self.parents.get(id(statement))
        for part in ("body", "orelse", "finalbody"):
            block = getattr(parent, part, None)
            if isinstance(block, list) and any(s is statement for s in block):
                return block
        return None

    def _in_error_message(self, use: ast.AST) -> bool:
        """`use` is only formatted into the exception a `raise` creates."""
        calls = 0
        for node in self._ancestors(use):
            if isinstance(node, ast.Call):
                calls += 1
            if isinstance(node, ast.Raise):
                return calls <= 1
            if isinstance(node, ast.stmt):
                return False
        return False

    def _logged(self, use: ast.AST) -> bool:
        """`use` is printed or logged (directly, or as an f-string field)."""
        parent = self.parents.get(id(use))
        if isinstance(parent, ast.FormattedValue):
            parent = self.parents.get(id(self.parents.get(id(parent))))
        return isinstance(parent, ast.Call) and use not in (parent.func,) and (
            (isinstance(parent.func, ast.Name) and parent.func.id == "print")
            or (isinstance(parent.func, ast.Attribute) and parent.func.attr in _LOG_METHODS))

    def _verdict_leaves(self, check: ast.AST) -> bool:
        """The check's yes/no result is used other than to decide an
        `if`/`while`/`assert` (the string results of replace/removeprefix
        are values, not answers)."""
        if isinstance(check, ast.Call) and check.func.attr in ("replace", "removeprefix"):
            return False
        node = check
        while True:
            parent = self.parents.get(id(node))
            if (isinstance(parent, ast.BoolOp)
                    or (isinstance(parent, ast.UnaryOp) and isinstance(parent.op, ast.Not))):
                node = parent
                continue
            break
        return not (isinstance(parent, (ast.If, ast.While, ast.Assert)) and parent.test is node)

    def _returns_converted(self, check: ast.AST, name: str) -> bool:
        """The branch a converted value takes at `check` ends by returning
        that value, or a path made from it (not a yes/no answer)."""
        taken = self._taken_branch(check)
        final = taken[1][-1] if taken and taken[1] else None
        return (isinstance(final, ast.Return) and final.value is not None
                and not _verdict_value(final.value)
                and any(isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Load)
                        for n in ast.walk(final.value)))

    # ── a path helper: converted at its calls ──────────────────────────
    @staticmethod
    def _parameters(scope: _Scope) -> Set[str]:
        if scope.kind not in ("function", "lambda"):
            return set()
        return {a.arg for a in _ScopeVisitor._all_args(scope.node.args)}

    def _plan_helper(self, scope: _Scope, sites: List[_Site]) -> None:
        """Convert the argument at each call of a path helper whose check of
        its parameter `sites` hold; the helper keeps the sweep's result."""
        function, site = scope.node, sites[0]
        param = site.subject.id
        pairs = list(dict.fromkeys(p for s in sites for p in s.value.rewritten))
        why = self._helper_problem(function, scope, sites)
        converted = 0
        for call in [] if why else self._calls_to(function):
            argument = self._argument_of(call, function, param)
            if argument is not None and any(isinstance(n, ast.Name) and n.id == _ARG
                                            for n in ast.walk(argument)):
                converted += 1  # converted by an earlier pass
                continue
            problem = self._call_problem(call, function, param, argument)
            if problem:
                self._note(call.lineno, f"call to `{function.name}` NOT converted ({problem}); its "
                           f"check of `{param}` compares against {_describe(pairs)} and may no "
                           "longer match — verify")
                continue
            call_scope = self.scopes.scope_of[id(call)]
            key = (argument.lineno, argument.col_offset, argument.end_lineno, argument.end_col_offset)
            self.edits[key] = _Edit(
                param, "exact" if site.kind in ("equals", "member") else "prefix", "",
                self._statement(call), call_scope, site.check, argument, site.pattern,
                pairs=list(pairs), constants=set(site.value.constants), helper=function.name,
                site_kind=site.kind,
                holder=self._holder(argument) if call_scope.kind == "module" else None)
            converted += 1
        if why:
            self._refused(site.check, why, pairs)
        elif not converted:
            self._refused(site.check, f"`{param}` comes from the caller, and no call to "
                          f"`{function.name}` in this cell can be converted", pairs)

    def _helper_problem(self, function: ast.AST, scope: _Scope,
                        sites: List[_Site]) -> Optional[str]:
        """Why the check of `function`'s parameter cannot be converted at its
        calls, or None."""
        site = sites[0]
        param = site.subject.id
        caller = f"`{param}` comes from the caller"
        if (not isinstance(function, ast.FunctionDef) or scope.parent is None
                or scope.parent.kind != "module"):
            return f"{caller}, and its function is not a top-level `def`"
        if function.decorator_list:
            return f"{caller}, and `{function.name}` is decorated"
        args = function.args
        if param not in {a.arg for a in args.posonlyargs + args.args + args.kwonlyargs}:
            return f"{caller} through `*` or `**`"
        if len({id(s.check) for s in sites}) > 1:
            return f"{caller}, and `{function.name}` checks it more than once"
        own = list(_own_nodes(function))
        if any(isinstance(n, (ast.Raise, ast.Assert)) for n in own):
            return (f"{caller}, and `{function.name}` can raise, so a caller may rely on it as "
                    "a validation")
        if any(isinstance(n, (ast.Yield, ast.YieldFrom, ast.Await)) for n in own):
            return f"{caller}, and `{function.name}` is a generator or coroutine"
        if any(isinstance(n, ast.Return) and _verdict_value(n.value) for n in own):
            return f"{caller}, and `{function.name}` returns a yes/no answer"
        taken = self._taken_branch(site.check)
        if (taken is None or not self._exits(site.check, scope)
                or not self._returns_converted(site.check, param)):
            return (f"{caller}, and the branch a converted value takes does not return it from "
                    f"`{function.name}`")
        start = (site.subject.lineno, site.subject.col_offset)
        early = [n for n in own if isinstance(n, ast.Name) and n.id == param
                 and isinstance(n.ctx, ast.Load) and (n.lineno, n.col_offset) < start]
        if early:
            return f"{caller}, and `{function.name}` reads it before the check (line {early[0].lineno})"
        patterns = {site.pattern.id} if isinstance(site.pattern, ast.Name) else set()
        statements = taken[1]
        for use in (n for s in statements for n in ast.walk(s)
                    if isinstance(n, ast.Name) and n.id == param and isinstance(n.ctx, ast.Load)):
            if not self._accepts_migrated(use, scope, patterns):
                return (f"{caller}, and `{function.name}` uses it at line {use.lineno} in a way "
                        "that may expect its S3/DBFS form")
        if self._deferred_read(param, scope) is not None and self._calls_out(statements, param):
            return (f"{caller}, and a nested function or generator in `{function.name}` may read "
                    "it while it is converted")
        return None

    def _calls_to(self, function: ast.FunctionDef) -> Iterator[ast.Call]:
        """Calls in this cell that name `function` and reach its def."""
        for node in ast.walk(self.tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == function.name):
                continue
            scope = self.scopes.scope_of.get(id(node))
            if scope is None or any(s.kind != "module" and s.bindings[function.name]
                                    for s in self._scope_chain(scope)):
                continue  # another binding of that name
            yield node

    @staticmethod
    def _argument_of(call: ast.Call, function: ast.FunctionDef, param: str) -> Optional[ast.AST]:
        positional = [a.arg for a in function.args.posonlyargs + function.args.args]
        if param in positional and positional.index(param) < len(call.args):
            return call.args[positional.index(param)]
        return next((k.value for k in call.keywords if k.arg == param), None)

    def _call_problem(self, call: ast.Call, function: ast.FunctionDef, param: str,
                      argument: Optional[ast.AST]) -> Optional[str]:
        """Why converting the argument of this call could change what any code
        other than the helper sees, or None."""
        if function.name not in self.table.stable_functions:
            return f"`{function.name}` is bound more than once in this notebook, or can be rebound later"
        if any(isinstance(a, ast.Starred) for a in call.args) or any(k.arg is None for k in call.keywords):
            return "the call unpacks its arguments"
        if argument is None:
            return f"the call does not pass `{param}`"
        if self._inside(call, function):
            return f"the call is inside `{function.name}` itself"
        for node in self._ancestors(call):
            if isinstance(node, (ast.JoinedStr, ast.FormattedValue)):
                return "the call is inside an f-string"
            if isinstance(node, _COMPREHENSIONS):
                return "the call is inside a comprehension"
            if isinstance(node, ast.Lambda):
                return "the call is inside a lambda"
        scope = self.scopes.scope_of[id(call)]
        if scope.kind == "class":
            return "the call is in a class body"
        if scope.kind == "module" and (call.lineno, call.col_offset) < (function.lineno, function.col_offset):
            return f"the call comes before `{function.name}` is defined"
        shadowed = self._shadowed(_HELPER_BUILTINS, scope)
        if shadowed:
            return f"`{shadowed[0]}` is redefined in this notebook"
        return self._argument_problem(argument, scope)

    def _argument_problem(self, argument: ast.AST, scope: _Scope) -> Optional[str]:
        """Why other code could still read the argument unconverted, or None:
        nothing but this call may read the variable (or dict key) it is."""
        holder = self._holder(argument)
        if holder is None:
            shared = sorted(n for n in self._lineage(argument) if self._holds_data(n, scope))
            return (f"its argument is made from `{shared[0]}`, which other code can still read"
                    if shared else None)
        if scope.kind == "module":
            return _read_elsewhere(self.table, holder)
        root = holder[0]
        function = next(s for s in self._scope_chain(scope) if s.kind in ("function", "lambda"))
        if root in self._parameters(function):
            return f"`{root}` comes from the caller of the function that makes the call"
        if not function.is_local(root):
            return f"`{root}` is a variable other code can read"
        again = [n for n in ast.walk(function.node) if isinstance(n, ast.Name) and n.id == root
                 and isinstance(n.ctx, ast.Load) and n is not argument
                 and not self._inside(n, argument)]
        if again:
            return f"`{root}` is read again at line {again[0].lineno}"
        return None

    def _holds_data(self, name: str, scope: _Scope) -> bool:
        """`name` is a variable (not a kernel object, an import, a def or a class)."""
        if any(s.kind in ("function", "lambda") and s.is_local(name) for s in self._scope_chain(scope)):
            return True
        return not self._kernel_object(name) and name not in self.table.definitions

    @staticmethod
    def _holder(argument: ast.AST) -> Optional[Tuple[str, Optional[str]]]:
        """(variable, key) when the argument is `x`, `x["key"]` or `x.get("key")`."""
        if isinstance(argument, ast.Name):
            return argument.id, None
        if (isinstance(argument, ast.Subscript) and isinstance(argument.value, ast.Name)
                and isinstance(argument.slice, ast.Constant) and isinstance(argument.slice.value, str)):
            return argument.value.id, argument.slice.value
        if (isinstance(argument, ast.Call) and isinstance(argument.func, ast.Attribute)
                and argument.func.attr == "get" and isinstance(argument.func.value, ast.Name)
                and argument.args and isinstance(argument.args[0], ast.Constant)
                and isinstance(argument.args[0].value, str)):
            return argument.func.value.id, argument.args[0].value
        return None

    def _taken_branch(self, check: ast.AST) -> Optional[Tuple[ast.If, List[ast.stmt]]]:
        """The `if` whose test a converted value decides, and the branch that
        value takes. The conversion makes the check true (false for `!=` and
        `not in`); `not`, and an `or`/`and` the check decides, are followed."""
        if isinstance(check, ast.Compare):
            outcome = not isinstance(check.ops[0], (ast.NotEq, ast.NotIn))
        elif (isinstance(check, ast.Call) and check.func.attr == "startswith"
              and len(check.args) == 1 and not check.keywords):
            outcome = True
        else:
            return None  # a string result (replace, removeprefix), or start/end positions
        node = check
        while True:
            parent = self.parents.get(id(node))
            if isinstance(parent, ast.UnaryOp) and isinstance(parent.op, ast.Not):
                outcome = not outcome
            elif not (isinstance(parent, ast.BoolOp) and isinstance(parent.op, ast.Or) == outcome):
                break
            node = parent
        if isinstance(parent, ast.If) and parent.test is node:
            return parent, parent.body if outcome else parent.orelse
        return None

    def _kill(self, check: ast.AST, name: str) -> Optional[Tuple[List[ast.stmt], ast.stmt]]:
        """The branch a converted value takes, when it rebinds `name` at its
        top level (a recognizer that translates it): the conversion ends there."""
        taken = self._taken_branch(check)
        for stmt in taken[1] if taken else []:
            targets = (stmt.targets if isinstance(stmt, ast.Assign)
                       else [stmt.target] if isinstance(stmt, ast.AnnAssign) and stmt.value else [])
            if any(isinstance(t, ast.Name) and t.id == name for t in targets):
                return taken[1], stmt
        return None

    def _exits(self, check: ast.AST, scope: _Scope) -> bool:
        """Every run of the branch a converted value takes ends the function
        call: it ends in `return`/`raise`, cannot break out of a loop, and no
        `try`/`with` around it can catch the exit and carry on."""
        taken = self._taken_branch(check)
        if scope.kind != "function" or taken is None:
            return False
        branch, statements = taken
        if not statements or not isinstance(statements[-1], (ast.Return, ast.Raise)):
            return False
        if any(isinstance(n, (ast.Break, ast.Continue)) for s in statements for n in ast.walk(s)):
            return False
        for node in self._ancestors(branch):
            if node is scope.node:
                return True
            if isinstance(node, _CATCHING):
                return False
        return False

    def _window(self, edit: _Edit) -> Optional[List[ast.stmt]]:
        """The statements that run while the value is converted, when that ends
        in the branch the check decides; None: it stays converted after it."""
        if edit.kill is not None:
            statements, rebinding = edit.kill
            return statements[:statements.index(rebinding) + 1]
        return self._taken_branch(edit.check)[1] if edit.exits else None

    @staticmethod
    def _calls_out(statements: List[ast.stmt], name: str) -> bool:
        """`statements` may run other code: a call other than `len` or a method
        of the (string) value itself, or a suspension point."""
        for node in (n for s in statements for n in ast.walk(s)):
            if isinstance(node, (ast.Await, ast.Yield, ast.YieldFrom)):
                return True
            if isinstance(node, ast.Call) and not (
                    (isinstance(node.func, ast.Name) and node.func.id == "len")
                    or (isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
                        and node.func.value.id == name)):
                return True
        return False

    def _deferred_read(self, name: str, scope: _Scope) -> Optional[ast.Name]:
        """A read of the function's `name` in code that runs when called, not
        where it is written: a nested function or lambda, or a generator
        expression past its first iterable."""
        for node in ast.walk(scope.node):
            if (isinstance(node, ast.Name) and node.id == name and isinstance(node.ctx, ast.Load)
                    and self._runs_later(node, scope.node)):
                return node
        return None

    def _runs_later(self, load: ast.Name, top: ast.AST) -> bool:
        later = False
        for node in self._ancestors(load):
            if node is top:
                return later
            if isinstance(node, _FUNCTION_NODES):
                if _binds_locally(node, load.id):
                    return False  # the nested function's own variable
                later = True
            elif isinstance(node, _COMPREHENSIONS):
                first = node.generators[0].iter
                if load is first or self._inside(load, first):
                    continue  # evaluated at once, in the enclosing scope
                if any(load.id in _target_names(g.target) for g in node.generators):
                    return False
                later = later or isinstance(node, ast.GeneratorExp)
        return later

    def _uses_while_converted(self, edit: _Edit, names: List[ast.Name], name: str,
                              scope: _Scope) -> List[ast.Name]:
        """Loads that can see the converted value of `name`."""
        window = self._window(edit)
        if window is not None:
            return [n for stmt in window for n in ast.walk(stmt)
                    if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)]
        start = (edit.subject.lineno, edit.subject.col_offset)
        loops = self._loops_around([edit], scope, name)
        return [n for n in names if (n.lineno, n.col_offset) > start
                or any(self._inside(n, loop) for loop in loops)]

    def _loops_around(self, edits: List[_Edit], scope: _Scope, name: str) -> List[ast.AST]:
        """Loops in `scope` re-running a conversion while `name` keeps its value."""
        loops = []
        for edit in edits:
            for node in self._ancestors(edit.check):
                if node is scope.node:
                    break
                if isinstance(node, _LOOPS) and not (
                        isinstance(node, (ast.For, ast.AsyncFor)) and name in _target_names(node.target)):
                    loops.append(node)
        return loops

    def _inside(self, node: ast.AST, container: ast.AST) -> bool:
        return any(ancestor is container for ancestor in self._ancestors(node))

    def _accepts_migrated(self, use: ast.Name, scope: _Scope, patterns: Set[str]) -> bool:
        """`use` behaves the same for the migrated value as the check intends."""
        parent = self.parents.get(id(use))
        site = self.sites_by_subject.get(id(use))
        if site is not None:  # another check: fine unless it expects the S3/DBFS form
            return site.value is not None and not site.value.keeps_source_form
        if isinstance(parent, (ast.Return, ast.Yield, ast.YieldFrom)) and parent.value is use:
            return True
        if isinstance(parent, ast.Subscript) and parent.value is use and isinstance(parent.slice, ast.Slice):
            bound = parent.slice.lower
            if parent.slice.upper is None and parent.slice.step is None and (
                    isinstance(bound, ast.Call) and isinstance(bound.func, ast.Name)
                    and bound.func.id == "len" and len(bound.args) == 1):
                prefix = bound.args[0]
                if isinstance(prefix, ast.Name) and prefix.id in patterns:
                    return True
                value = self._resolve(prefix, scope)
                return value is not None and not value.keeps_source_form
            return False
        if isinstance(parent, ast.Call) and self._storage_path(parent, use, scope):
            return True
        return self._logged(use)

    def _storage_path(self, call: ast.Call, use: ast.Name, scope: _Scope) -> bool:
        """`use` is the path a Spark read/write or `open` uses."""
        func = call.func
        if isinstance(func, ast.Attribute):
            if func.attr in _STORAGE_METHODS and (
                    (call.args and call.args[0] is use)
                    or any(k.arg == "path" and k.value is use for k in call.keywords)):
                return True
            return (func.attr == "option" and len(call.args) == 2 and call.args[1] is use
                    and isinstance(call.args[0], ast.Constant) and call.args[0].value == "path")
        return (isinstance(func, ast.Name) and func.id == "open" and bool(call.args)
                and call.args[0] is use and not self._shadowed(["open"], scope))

    # ── reporting what is not converted ─────────────────────────────────
    def _note_unresolved(self, site: _Site) -> None:
        operands = [site.pattern, site.subject] if site.kind == "equals" else [site.pattern]
        for name in (n.id for o in operands for n in ast.walk(o) if isinstance(n, ast.Name)):
            if name in self.table.excluded and self.table.excluded[name][0].rewritten:
                strings, why = self.table.excluded[name]
                self.handled.add(id(site.check))
                self._refused(site.check, f"`{name}` holds rewritten strings but {why}",
                              strings.rewritten)
                return

    def _note_unhandled(self) -> None:
        """Report checks this module does not convert that still compare
        against strings the sweep rewrote."""
        for node in ast.walk(self.tree):
            scope = self.scopes.scope_of.get(id(node))
            if scope is None or id(node) in self.handled:
                continue
            operands, why = self._check_operands(node)
            if operands and any(self._affected(o, scope) for o in operands):
                self._note(node.lineno, f"path check NOT converted ({why}); it compares "
                           "against rewritten S3/DBFS strings and may no longer match — verify")

    @staticmethod
    def _check_operands(node: ast.AST) -> Tuple[List[ast.AST], str]:
        unknown = "the strings it compares against are not known statically"
        if isinstance(node, ast.Compare):
            if not all(isinstance(op, (ast.Eq, ast.NotEq, ast.In, ast.NotIn)) for op in node.ops):
                return [], ""
            return [node.left, *node.comparators], unknown
        if not isinstance(node, ast.Call):
            return [], ""
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in _STR_METHODS and node.args and not (
                isinstance(func.value, ast.Name) and func.value.id == "str"):
            return [func.value, node.args[0]], unknown
        owner = func.value.id if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) else None
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
        if owner in _PATTERN_FUNCTIONS and name in _PATTERN_FUNCTIONS[owner]:
            return list(node.args), f"`{owner}.{name}` matches are not converted"
        if owner is None and name in _PATTERN_FUNCTIONS["fnmatch"]:
            return list(node.args), f"`{name}` matches are not converted"
        return [], ""


def _plain_field(part: ast.FormattedValue) -> bool:
    return part.conversion == -1 and part.format_spec is None and isinstance(part.value, ast.Name)


def _bound_names(target: ast.AST) -> Set[str]:
    """Names an assignment target binds (not those it subscripts or reads)."""
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        return set().union(*(_bound_names(e) for e in target.elts))
    if isinstance(target, ast.Starred):
        return _bound_names(target.value)
    return set()


def _root_name(node: ast.AST) -> Optional[str]:
    """The name an attribute/subscript/call chain starts from."""
    while isinstance(node, (ast.Attribute, ast.Subscript, ast.Call)):
        node = node.func if isinstance(node, ast.Call) else node.value
    return node.id if isinstance(node, ast.Name) else None


def _leads_with_text(node: ast.AST) -> bool:
    """A format string that starts with literal text, not with a field."""
    return (isinstance(node, ast.Constant) and isinstance(node.value, str)
            and (not node.value.startswith("{") or node.value.startswith("{{")))


# Calls that only inspect a value; evaluating them again reads nothing new.
_INSPECTING_CALLS = frozenset({"len", "isinstance", "str", "repr", "int", "float", "bool", "type",
                               "print"})


def _source_call(call: ast.Call) -> bool:
    """A call that can produce the value it is part of (not a string method
    on it, nor an inspection of it)."""
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr not in _STR_TRANSFORMS
    return not (isinstance(func, ast.Name) and func.id in _INSPECTING_CALLS)


def _read_elsewhere(table: _Table, holder: Tuple[str, Optional[str]]) -> Optional[str]:
    """Why the notebook reads `holder` (a variable, or one key of it)
    somewhere besides the one call that passes it, or None."""
    name, key = holder
    if name in table.function_loads:
        return f"`{name}` is also read inside a function"
    reads = [(k, kind, count) for (n, k, kind), count in table.accesses.items()
             if n == name and count > 0]
    if key is None:
        if sum(count for _, kind, count in reads if kind != "display") != 1:
            return f"`{name}` is also read elsewhere in this notebook"
        return None
    if any(kind == "other" for _, kind, _ in reads):
        return f"`{name}` is also used as a whole elsewhere in this notebook"
    if sum(count for k, kind, count in reads if kind == "key" and k == key) != 1:
        return f"`{name}[{key!r}]` is also read elsewhere in this notebook"
    return None


def _verdict_value(node: Optional[ast.AST]) -> bool:
    """`node` evaluates to a yes/no answer."""
    if isinstance(node, (ast.Compare, ast.BoolOp)):
        return True
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return True
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return True
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return ((isinstance(func, ast.Name) and func.id in _VERDICT_CALLS)
            or (isinstance(func, ast.Attribute) and func.attr in ("startswith", "endswith")))


def _binds(node: ast.AST, name: str) -> bool:
    if isinstance(node, ast.Name) and node.id == name and not isinstance(node.ctx, ast.Load):
        return True
    if isinstance(node, ast.arg) and node.arg == name:
        return True
    if isinstance(node, (ast.Global, ast.Nonlocal)) and name in node.names:
        return True
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return name in {(a.asname or a.name).split(".")[0] for a in node.names}
    return isinstance(node, (ast.ExceptHandler, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)) and node.name == name


def _binds_locally(function: ast.AST, name: str) -> bool:
    """`name` is a variable of `function` itself (not global/nonlocal)."""
    own = list(_own_nodes(function))
    if any(isinstance(n, (ast.Global, ast.Nonlocal)) and name in n.names for n in own):
        return False
    return (name in {a.arg for a in _ScopeVisitor._all_args(function.args)}
            or any(_binds(n, name) for n in own))


def _describe(pairs: Sequence[Pair]) -> str:
    original, migrated = pairs[0]
    more = f" (+{len(pairs) - 1} more)" if len(pairs) > 1 else ""
    return f"rewritten {original!r} → {migrated!r}{more}"


def _protected(text: str) -> str:
    """`text` as adjacent literals the S3/DBFS sweep cannot match."""
    cuts = [0] + [m.start() for m in _SWEEP_SPLIT_RE.finditer(text)] + [len(text)]
    parts = [text[a:b] for a, b in zip(cuts, cuts[1:]) if b > a]
    return " ".join(repr(part) for part in parts) or "''"


def pairs_source(pairs: Sequence[Pair]) -> str:
    """The pairs as a tuple literal, longest original first."""
    ordered = sorted(pairs, key=lambda p: -len(p[0]))
    return "(" + ", ".join(f"({_protected(o)}, {m!r})" for o, m in ordered) + ",)"


def converter_expression(name: str, mode: str, pairs: Sequence[Pair], guard: str) -> str:
    """`(name := ...)`: `name` as it is when not a string or when `guard` (the
    original check) already holds, otherwise `name` converted with `pairs`."""
    if mode == "prefix":
        element, condition = f"{_NEW} + {name}[len({_OLD}):]", f"{name}.startswith({_OLD})"
    else:
        element, condition = _NEW, f"{name} == {_OLD}"
    convert = (f"next(({element} for {_OLD}, {_NEW} in {pairs_source(pairs)} "
               f"if {condition}), {name})")
    return f"({name} := {name} if not isinstance({name}, str) or {guard} else {convert})"


def call_site_expression(argument: str, mode: str, pairs: Sequence[Pair], kind: str) -> str:
    """`argument` (source text, evaluated once) as a helper's check of kind
    `kind` would need it: unchanged when not a string or when that check, as
    the sweep left it, already accepts it; otherwise converted with `pairs`."""
    value = _ARG
    migrated = ", ".join(_protected(m) for m in sorted({m for _, m in pairs}, key=lambda m: -len(m)))
    if kind == "contains":
        guard = "(" + " or ".join(f"{_protected(m)} in {value}" for m in
                                  sorted({m for _, m in pairs}, key=lambda m: -len(m))) + ")"
    elif mode == "exact":
        guard = f"{value} in ({migrated},)"
    else:
        guard = f"{value}.startswith(({migrated},))"
    if mode == "prefix":
        element, condition = f"{_NEW} + {value}[len({_OLD}):]", f"{value}.startswith({_OLD})"
    else:
        element, condition = _NEW, f"{value} == {_OLD}"
    convert = (f"next(({element} for {_OLD}, {_NEW} in {pairs_source(pairs)} "
               f"if {condition}), {value})")
    return (f"next(({value} if not isinstance({value}, str) or {guard} else {convert}) "
            f"for {value} in ({argument},))")


def _code_tokens(source: str) -> List[Tuple[int, str]]:
    skip = {tokenize.COMMENT, tokenize.NL, tokenize.ENCODING}
    tokens = tokenize.generate_tokens(io.StringIO(_mask_magics(source)).readline)
    return [(t.type, t.string) for t in tokens if t.type not in skip]


def _reason(exc: Exception) -> str:
    return str(exc.args[0]) if exc.args else type(exc).__name__


def convert_cell(source: str, table: _Table, translate: Translate) -> Conversion:
    """The sweep's result for `source`, with values converted at path checks."""
    swept = sweep_keeping_originals(source, translate)
    magic_notes = _magic_line_notes(source, translate)
    tree = _parse(source)
    try:
        converter = None if tree is None else _CellConverter(source, tree, table, translate)
        if converter is not None:
            converter.plan()
    except RecursionError:
        converter = None
    if converter is None:
        notes = () if swept == source else (
            "path checks not analysed: the cell cannot be parsed as Python; S3/DBFS "
            "strings were rewritten — verify any check against them",)
        return Conversion(swept, notes, {})
    if not converter.edits:
        return Conversion(swept, _ordered(converter.notes + magic_notes), converter.used)
    try:
        converted, inserted = _apply(source, converter.edits, translate)
    except (SyntaxError, ValueError, tokenize.TokenError) as exc:
        note = (0, f"path checks NOT converted in this cell ({_reason(exc)}); S3/DBFS strings "
                   "were rewritten — verify every check against them")
        return Conversion(swept, _ordered(converter.notes + magic_notes + [note]), {})
    notes = [(key[0], _converted_note(e)) for key, e in converter.edits.items()]

    def final_line(line: int) -> int:  # line numbers in the saved cell
        return line + sum(n for first, n in inserted.items() if first <= line)

    holders = tuple(sorted({e.holder for e in converter.edits.values() if e.holder is not None},
                           key=lambda h: (h[0], h[1] or "")))
    return Conversion(
        converted, _ordered([(final_line(l), t) for l, t in converter.notes + magic_notes + notes]),
        converter.used, holders)


def _converted_note(edit: _Edit) -> str:
    if edit.helper is not None:
        return (f"path check converted: this call to `{edit.helper}` migrates its `{edit.name}` "
                "argument before the helper checks it, when it is a string the check does not "
                f"already accept ({_describe(edit.pairs)}); `{edit.helper}` itself keeps the "
                "sweep's result for its other callers")
    return (f"path check converted: `{edit.name}`, when it is a string the check does "
            "not already accept, is migrated before the comparison and "
            + ("the branch then rebinds it" if edit.kill
               else "the branch then leaves the function" if edit.exits
               else "stays migrated after it")
            + f" ({_describe(edit.pairs)})")


def _magic_line_notes(source: str, translate: Translate) -> List[Tuple[int, str]]:
    """Magic lines are masked from the analysis; report those the sweep rewrites."""
    notes = []
    for number, line in enumerate(_lines(source), 1):
        if _MAGIC_LINE_RE.match(line) and translate(line) != line:
            notes.append((number, "an IPython magic line uses rewritten S3/DBFS strings and "
                                  "is not analysed — verify"))
    return notes


def _ordered(notes: List[Tuple[int, str]]) -> Tuple[str, ...]:
    return tuple(f"line {line}: {text}" if line else text for line, text in sorted(notes))


def _apply(source: str, edits: Dict[Tuple[int, int, int, int], _Edit],
           translate: Translate) -> Tuple[str, Dict[int, int]]:
    """The converted, swept cell and the comment lines inserted per line."""
    lines = _lines(source)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line))

    def offset(lineno: int, col: int) -> int:  # ast columns are UTF-8 byte offsets
        return starts[lineno - 1] + len(lines[lineno - 1].encode()[:col].decode())

    pieces, cursor = [], 0
    for (l1, c1, l2, c2), edit in sorted(edits.items()):
        protected = pairs_source(edit.pairs)
        if translate(protected) != protected:
            raise ValueError("the generated conversion would itself be rewritten")
        start, end = offset(l1, c1), offset(l2, c2)
        if start < cursor:
            raise ValueError("two conversions overlap")
        if edit.helper is not None:
            replacement = call_site_expression(source[start:end], edit.mode, edit.pairs, edit.site_kind)
        else:
            replacement = converter_expression(edit.name, edit.mode, edit.pairs, edit.guard)
        pieces += [source[cursor:start], replacement]
        cursor = end
    edited = "".join(pieces) + source[cursor:]
    if _parse(edited) is None or (_compiles(source) and not _compiles(edited)):
        raise ValueError("the converted cell does not compile")
    swept = sweep_keeping_originals(edited, translate)
    out = _lines(swept)
    if len(out) != len(lines):
        raise ValueError("the sweep changed the cell's line count")
    blocks: Dict[int, int] = {}  # first line of statement -> last edited line
    for (l1, _c1, l2, _c2), edit in edits.items():
        first = min([edit.statement.lineno] + [d.lineno for d in getattr(
            edit.statement, "decorator_list", [])])
        blocks[first] = max(blocks.get(first, first), l2)
    inserted: Dict[int, int] = {}
    for first, last in sorted(blocks.items(), reverse=True):
        indent = re.match(r"[ \t]*", lines[first - 1]).group(0)
        newline = "\r\n" if lines[first - 1].endswith("\r\n") else "\n"
        comment = [f"{indent}# {line.strip()}  {ORIGINAL_MARK}{newline}"
                   for line in lines[first - 1:last]]
        out[first - 1:first - 1] = comment + [f"{indent}{TAG} {REASON}{newline}"]
        inserted[first] = len(comment) + 1
    result = "".join(out)
    if _code_tokens(result) != _code_tokens(swept):
        raise ValueError("the original-line comments would change the code")
    return result, inserted


class NotebookPathChecks:
    """Constant table for one notebook's Python cells, kept current as cells
    are converted (a migrated cell replaces its original in the table)."""

    def __init__(self, cell_sources: Sequence[str], translate: Translate) -> None:
        self._translate = _caching(translate)
        self._cells: Dict[int, _CellFacts] = {}
        self._original: Dict[int, _CellFacts] = {}
        self._bindings: Counter = Counter()
        self._poisoned: Counter = Counter()
        self._affected: Counter = Counter()
        self._affected_attrs: Counter = Counter()
        self._function_loads: Counter = Counter()
        self._definitions: Counter = Counter()
        self._accesses: Counter = Counter()
        self._dynamic = 0
        for index, source in enumerate(cell_sources):
            facts = _cell_facts(source, self._translate)
            self._original[index] = facts
            self._account(index, facts)
        self._used: Dict[int, Dict[str, Tuple[Pair, ...]]] = {}
        self._holders: Dict[int, List[Tuple[str, Optional[str]]]] = {}

    def _account(self, index: int, facts: _CellFacts, sign: int = 1) -> None:
        """Add (sign=1) or remove (sign=-1) one cell's facts from the totals."""
        if sign > 0:
            self._cells[index] = facts
        update = Counter.update if sign > 0 else Counter.subtract
        update(self._bindings, facts.bindings)
        update(self._poisoned, Counter(facts.poisoned))
        update(self._affected, Counter(facts.affected))
        update(self._affected_attrs, Counter(facts.affected_attrs))
        update(self._function_loads, Counter(facts.function_loads))
        update(self._definitions, Counter(facts.definitions))
        update(self._accesses, facts.accesses)
        self._dynamic += sign * int(facts.dynamic)

    def _table(self, index: int) -> _Table:
        def present(counter: Counter) -> FrozenSet[str]:
            return frozenset(n for n, c in counter.items() if c > 0)

        poisoned = present(self._poisoned)
        rebinders = [(i, line) for i, f in self._cells.items() for line in f.rebinder_lines]
        definitions: Dict[str, List[Tuple[int, int, _Strings]]] = {}
        for i, facts in self._cells.items():
            for name, (line, value) in facts.candidates.items():
                definitions.setdefault(name, []).append((i, line, value))
        constants, excluded = {}, {}
        for name, found in definitions.items():
            i, line, value = found[0]
            original = self._original.get(i)
            before = original.candidates.get(name) if original else None
            why = None
            if self._dynamic:
                why = "the notebook rebinds names dynamically (exec or writes through globals())"
            elif len(found) != 1 or self._bindings[name] != 1:
                why = "it is bound more than once in this notebook"
            elif name in poisoned:
                why = "it is rebound through global/nonlocal or changed in place"
            elif any(position > (i, line) for position in rebinders):
                why = "a later star import or %run can rebind it"
            elif (before is not None and before[1].rewritten
                  and [o for o, _ in before[1].pairs] != [o for o, _ in value.pairs]):
                why = "the migration model rewrote its strings itself"
                value = before[1]
            if why:
                excluded[name] = (value, why)
            else:
                constants[name] = value
        functions: Dict[str, List[Tuple[int, int]]] = {}
        for i, facts in self._cells.items():
            for name, line in facts.functions.items():
                functions.setdefault(name, []).append((i, line))
        stable = frozenset(
            name for name, found in functions.items()
            if len(found) == 1 and self._bindings[name] == 1 and name not in poisoned
            and not self._dynamic and not any(position > found[0] for position in rebinders))
        return _Table(index, constants, excluded, self._bindings, poisoned, self._dynamic > 0,
                      present(self._affected), present(self._affected_attrs),
                      present(self._function_loads), present(self._definitions), stable,
                      self._accesses)

    def translate_cell(self, index: Optional[int], source: str) -> Tuple[str, List[str]]:
        """Sweep `source` (cell `index`) with path-check conversion; notes report
        every conversion and every check left as the sweep makes it."""
        if index is None:
            return NotebookPathChecks([source], self._translate).translate_cell(0, source)
        facts = _cell_facts(source, self._translate)
        if self._cells.get(index) != facts:
            if index in self._cells:
                self._account(index, self._cells[index], sign=-1)
            self._account(index, facts)
        table = self._table(index)
        self._used.pop(index, None)
        self._holders.pop(index, None)
        notes = self._stale_uses(table)
        result = convert_cell(source, table, self._translate)
        if result.used:
            self._used[index] = result.used
        if result.holders:
            self._holders[index] = list(result.holders)
        return result.source, notes + list(result.notes)

    def _stale_uses(self, table: _Table) -> List[str]:
        notes = []
        for cell, uses in self._used.items():
            for name, pairs in list(uses.items()):
                current = table.constants.get(name)
                if current is None or current.pairs != pairs:
                    notes.append(
                        f"cell {cell} converted a path check with the earlier value of "
                        f"`{name}`, which this cell's migration changed — verify cell {cell}")
                    del uses[name]
        for cell, holders in self._holders.items():
            for holder in list(holders):
                if _read_elsewhere(table, holder):
                    name, key = holder
                    text = name if key is None else f"{name}[{key!r}]"
                    notes.append(
                        f"cell {cell} converted a call assuming only that call reads `{text}`; "
                        f"this cell's migration reads it too — verify cell {cell}")
                    holders.remove(holder)
        return notes

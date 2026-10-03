"""Static name-resolution check for generated PySpark code.

``ast.parse()`` only confirms code is syntactically valid Python -- it says
nothing about whether every name used is actually defined before use. A
notebook can be perfectly valid Python and still raise ``NameError`` the
moment it runs. That's exactly what happened with the ``df_final = df``
bug found via the offline corpus: ``orders_transform.xml``'s
generated notebook ends with ``df_final = df`` where ``df`` is never
assigned anywhere in that notebook (only ``df_source`` is) -- syntactically
perfect, dead on execution.

``unresolved_names()`` walks generated code in the order Python would
actually execute it, tracking every name that gets bound (assignment,
import, ``except ... as e:``, comprehension targets, ...), and flags any
name read in ``Load`` context before it was ever bound anywhere earlier in
the script. It is used by ``demo.sh``'s verify step and by
``tests/test_transformation_converter.py`` / ``tests/test_migrator.py``
-- and is meant to be reused by any future validator that needs the same
check (e.g. an AIDP-runtime validity validator), which is why it lives in
its own module rather than inline in a shell script.

This is deliberately a straight-line, best-effort check, not a general
Python name-resolution/type-checking engine. Generated notebook cells are
simple: assignments, expression statements, ``assert``, ``try/except``,
comprehensions, and the occasional debug ``if``. Branches are checked
independently against the same pre-branch state (so a name bound in only
one branch never leaks into the other's check) and their bindings are
unioned back afterward, which is the conservative direction: prefer a
false positive (noisy, harmless) over a false negative (a crashing
notebook that silently passes).
"""
from __future__ import annotations

import ast
import builtins as _builtins

# Names available in every generated notebook without an explicit binding
# in the script itself: Python builtins, the two dunders the setup cell's
# `logging.getLogger(__name__)` relies on, and the AIDP notebook runtime's
# own pre-injected globals (the generated setup cell also assigns `spark`
# explicitly via SparkSession.builder.getOrCreate(), so this entry is a
# defensive belt-and-suspenders in case a future template relies on a
# kernel-provided `spark` without re-assigning it -- same reasoning for
# `dbutils`, which AIDP/Databricks-style notebooks may inject).
KNOWN_GLOBALS: frozenset[str] = (
    frozenset(vars(_builtins))
    # oidlUtils: injected into every AIDP notebook kernel (job parameters,
    # notebook.run/exit); the parameters cell reads job parameters with it.
    | frozenset({"__name__", "__file__", "__builtins__", "spark", "dbutils", "oidlUtils"})
)

# `from <module> import *` brings in an unbounded set of names we can't
# discover just by parsing -- so wildcard imports are looked up by module
# name against a curated allowlist. `pyspark.sql.types` is the only
# wildcard import the notebook templates ever use (in the setup cell).
# Any OTHER wildcard import is deliberately left unresolved: we'd rather
# flag a false positive on an unrecognized `import *` than silently trust
# it and risk masking a real bug (see module docstring).
_WILDCARD_IMPORTS: dict[str, frozenset[str]] = {
    "pyspark.sql.types": frozenset({
        "DataType", "NullType", "AtomicType", "NumericType", "IntegralType",
        "FractionalType", "StringType", "CharType", "VarcharType",
        "BinaryType", "BooleanType", "DateType", "TimestampType",
        "TimestampNTZType", "DecimalType", "DoubleType", "FloatType",
        "ByteType", "IntegerType", "LongType", "ShortType", "ArrayType",
        "MapType", "StructField", "StructType", "DayTimeIntervalType",
        "YearMonthIntervalType", "CalendarIntervalType",
    }),
}


def _bind_target(target: ast.expr, into: set[str]) -> None:
    """Add every plain name a Store-context target binds into *into*.

    Handles ``x = ...``, tuple/list unpacking (``a, b = ...``), and
    starred targets (``a, *rest = ...``). Attribute/Subscript targets
    (``obj.attr = ...`` / ``obj[i] = ...``) don't introduce a new name, so
    they're intentionally skipped.
    """
    if isinstance(target, ast.Name):
        into.add(target.id)
    elif isinstance(target, (ast.Tuple, ast.List)):
        for elt in target.elts:
            _bind_target(elt, into)
    elif isinstance(target, ast.Starred):
        _bind_target(target.value, into)


class _SequentialChecker(ast.NodeVisitor):
    """Walk statements in execution order, tracking bound names as it goes."""

    def __init__(self, known_globals: frozenset[str]) -> None:
        self.assigned: set[str] = set(known_globals)
        self.bad: list[str] = []

    # ---- expression-level check -------------------------------------

    def _check_expr_loads(self, expr: ast.AST) -> None:
        """Flag any Load-context Name in *expr* not yet in self.assigned.

        Comprehension targets (``for c in ...`` inside a list/set/dict
        comprehension or generator expression) are scoped to that
        comprehension only, so they're collected first and treated as
        locally available for the duration of this single check.
        """
        local: set[str] = set()
        for node in ast.walk(expr):
            if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
                for gen in node.generators:
                    _bind_target(gen.target, local)
            elif isinstance(node, ast.Lambda):
                # A lambda's parameters are bound inside its body only:
                # ``sorted(cols, key=lambda c: c.upper())`` used to report
                # ``c`` as read-before-assigned.
                a = node.args
                for arg in [*a.posonlyargs, *a.args, *a.kwonlyargs]:
                    local.add(arg.arg)
                if a.vararg:
                    local.add(a.vararg.arg)
                if a.kwarg:
                    local.add(a.kwarg.arg)
            elif isinstance(node, ast.NamedExpr):
                # ``(n := f())`` binds n in the enclosing scope, for the rest
                # of the expression and everything after it.
                _bind_target(node.target, local)
                _bind_target(node.target, self.assigned)
        for node in ast.walk(expr):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                if node.id not in self.assigned and node.id not in local:
                    self.bad.append(node.id)

    def visit_sequence(self, stmts: list[ast.stmt]) -> None:
        for stmt in stmts:
            self.visit(stmt)

    # ---- statement handlers, in execution order ----------------------

    def visit_Expr(self, node: ast.Expr) -> None:
        self._check_expr_loads(node.value)

    def visit_Assert(self, node: ast.Assert) -> None:
        self._check_expr_loads(node.test)
        if node.msg:
            self._check_expr_loads(node.msg)

    def visit_Return(self, node: ast.Return) -> None:
        if node.value:
            self._check_expr_loads(node.value)

    def visit_Assign(self, node: ast.Assign) -> None:
        self._check_expr_loads(node.value)
        for t in node.targets:
            _bind_target(t, self.assigned)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        # The target is read AND written (x += 1 needs x already bound).
        self._check_expr_loads(node.target)
        self._check_expr_loads(node.value)
        _bind_target(node.target, self.assigned)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value:
            self._check_expr_loads(node.value)
        _bind_target(node.target, self.assigned)

    def visit_Import(self, node: ast.Import) -> None:
        for a in node.names:
            self.assigned.add((a.asname or a.name).split(".")[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if any(a.name == "*" for a in node.names):
            self.assigned.update(_WILDCARD_IMPORTS.get(node.module or "", frozenset()))
        else:
            for a in node.names:
                self.assigned.add(a.asname or a.name)

    def visit_Try(self, node: ast.Try) -> None:
        self.visit_sequence(node.body)
        for handler in node.handlers:
            if handler.type:
                self._check_expr_loads(handler.type)
            if handler.name:
                self.assigned.add(handler.name)
            self.visit_sequence(handler.body)
            if handler.name:
                # The exception variable doesn't leak past its handler in
                # real Python (it's implicitly deleted). Always drop it --
                # erring toward a false positive if that name is reused
                # elsewhere is the safe direction here.
                self.assigned.discard(handler.name)
        self.visit_sequence(node.orelse)
        self.visit_sequence(node.finalbody)

    def visit_If(self, node: ast.If) -> None:
        self._check_expr_loads(node.test)
        # Check each branch against the SAME pre-branch state (so one
        # branch's bindings can't hide a real bug in the other), then
        # union whatever either branch bound back into scope afterward.
        snapshot = set(self.assigned)
        self.visit_sequence(node.body)
        body_assigned = self.assigned
        self.assigned = set(snapshot)
        self.visit_sequence(node.orelse)
        self.assigned = self.assigned | body_assigned

    def visit_For(self, node: ast.For) -> None:
        self._check_expr_loads(node.iter)
        _bind_target(node.target, self.assigned)
        self.visit_sequence(node.body)
        self.visit_sequence(node.orelse)

    def visit_While(self, node: ast.While) -> None:
        self._check_expr_loads(node.test)
        self.visit_sequence(node.body)
        self.visit_sequence(node.orelse)

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            self._check_expr_loads(item.context_expr)
            if item.optional_vars is not None:
                _bind_target(item.optional_vars, self.assigned)
        self.visit_sequence(node.body)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        # Generated notebooks never define functions -- a function body is
        # its own scope anyway, so we don't recurse into it.
        self.assigned.add(node.name)

    visit_AsyncFunctionDef = visit_FunctionDef  # type: ignore[assignment]

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.assigned.add(node.name)

    def generic_visit(self, node: ast.AST) -> None:
        # Fallback for statement types not special-cased above (Pass,
        # Raise, Global, Nonlocal, Delete, ...): conservatively check for
        # Load-context names without trying to bind anything new.
        if isinstance(node, ast.stmt):
            self._check_expr_loads(node)
        else:
            super().generic_visit(node)


def unresolved_names(code: str) -> list[str]:
    """Return the sorted, deduplicated list of names read before ever
    being assigned anywhere earlier in *code*.

    An empty list means every name is either a known global/builtin, an
    AIDP-runtime-provided name, or was bound (assignment, import,
    ``except ... as e:``, comprehension target, ...) by an earlier
    statement in the same script. A non-empty list is a real `NameError`
    waiting to happen at execution time -- something ``ast.parse()``
    cannot detect, because the code is syntactically valid either way.
    """
    tree = ast.parse(code)
    checker = _SequentialChecker(KNOWN_GLOBALS)
    checker.visit_sequence(tree.body)
    return sorted(set(checker.bad))


def abandoned_dataframes(code: str) -> list[str]:
    """DataFrame variables that are assigned and then never read.

    A dropped join leaves exactly this footprint. When a Joiner's
    master/detail side cannot be resolved, the converter refuses to guess,
    emits ``REVIEW REQUIRED`` and passes the detail side through -- correct
    behaviour. But the *other* side has already been prepared into its own
    variable, and that variable is then never used. Downstream code still
    references columns that only existed on the abandoned side, so the
    notebook raises ``cannot resolve <column>`` at runtime.

    ``unresolved_names`` cannot see this. The abandoned variable is
    assigned, so no Python name is read before assignment, and the
    downstream column lives inside a string (``F.col("DEPARTMENT_NAME")``)
    rather than in a Python identifier. Both checks pass; Spark fails.

    Found on a live AIDP cluster: ``m_EmployeeSummary`` generated a notebook
    with ``df_in_jnr_empdept_sq_departments`` assigned once and never read,
    zero ``.join(`` calls, and a ``groupBy("DEPARTMENT_NAME")`` over the
    employees side alone. It was reported as converted, counted in
    ``notebooks=12 error=0``, and passed the offline verify.

    Scope is deliberately narrow: only ``df_in_*`` names, which is how the
    generator names a per-consumer input copy it prepared for a specific
    downstream transformation. If such a copy is never read, the consumer
    it was built for did not consume it, and that is a broken dataflow.

    Router group DataFrames (``df_<group>``) are NOT reported, because an
    unconsumed group is legitimate: in ``data_quality_route.xml`` only the
    ``valid`` group has outgoing connectors, so ``df_invalid_currency``,
    ``df_negative_amount`` and ``df_null_amount`` are correctly unused. A
    first version of this check reported all ``df*`` names and flagged
    those three -- it would have been another instrument crying wolf.

    This check therefore finds abandoned *inputs*, not every unused
    DataFrame. It does not attempt to verify that downstream column
    references resolve; that needs a schema, which this module does not
    have.
    """
    tree = ast.parse(code)
    assigned: dict[str, int] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id.startswith("df_in_"):
                    assigned.setdefault(t.id, getattr(node, "lineno", 0))
    read: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            read.add(node.id)
    # A variable that is only ever the target of its own augmenting
    # reassignment (df = df.filter(...)) reads itself, so it is in `read`
    # already and will not be reported.
    return sorted(n for n in assigned if n not in read)

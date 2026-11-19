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
import re

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


# ── Credential literals ──────────────────────────────────────────────

# The words that make an identifier a credential name, and the words that,
# FOLLOWING one of them, make it the name of or a fact about a credential
# instead: ``password_env`` holds the name of a variable, ``token_url`` an
# address, ``PASSWORD_HASH`` a digest, ``secret_ocid`` a reference. An
# identifier is split on ``_``, ``.``, ``[``/``"`` and camelCase boundaries,
# so ``dbPassword``, ``PASSWORD_PROD``, ``os.environ["ADW_PASSWORD"]`` and
# ``spark.sql.catalog.adw.password`` all count. A first version anchored the
# match at the END of the name (``..._password$``), which the realistic
# "make it run" edits -- ``PASSWORD_PROD = "..."``, ``dbPassword = "..."``
# -- all slipped past.
_SECRET_WORDS = frozenset({
    "password", "passwd", "passphrase", "pwd", "secret", "token", "apikey",
})
_NOT_A_SECRET_AFTER = frozenset({
    "env", "envvar", "var", "variable", "name", "file", "path", "url", "uri",
    "endpoint", "type", "kind", "id", "ocid", "arn", "ref", "reference",
    "hash", "hashed", "digest", "salt", "len", "length", "min", "max",
    "hint", "prompt", "label", "column", "col", "field", "header", "scope",
    "store", "vault", "provider", "source", "lookup", "version", "expiry",
    "expires", "expiration", "ttl", "lifetime", "count", "attempts",
    "policy", "required", "enabled", "flag",
})
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_NOT_ALNUM = re.compile(r"[^A-Za-z0-9]+")


def is_secret_name(name: str) -> bool:
    """Whether *name* -- a keyword, assignment target, dict key or option
    key -- names a credential rather than something about one."""
    segments: list[str] = []
    for seg in _NOT_ALNUM.split(_CAMEL_BOUNDARY.sub("_", name)):
        seg = seg.lower()
        if not seg:
            continue
        if seg == "key" and segments and segments[-1] == "api":
            segments[-1] = "apikey"        # api + key is one word written as two
        else:
            segments.append(seg)
    for i, seg in enumerate(segments):
        if seg in _SECRET_WORDS:
            following = segments[i + 1] if i + 1 < len(segments) else None
            if following is None or following not in _NOT_A_SECRET_AFTER:
                return True
    return False


# A value that only LOOKS like a literal because the template left a hole
# in it: ``{...}`` and ``${...}`` placeholders, ``<...>`` prompts, ``%s``.
_NOT_A_PLACEHOLDER = r"[^\s;&\"'{}$<%]+"

# A "value" that is not a credential however it is keyed: a number, SQL
# NULL/TRUE/FALSE, a ``?`` or ``:name`` bind, or anything with a call,
# subscript or placeholder in it (``os.environ[ADW_PASSWORD]``).
_VALUE_IS_NOT_A_SECRET = re.compile(
    r"(?is)^(?:\d+(?:\.\d+)?|null|none|true|false|\?|:\w+|.*[\[\(\{<%$].*)$"
)

# Only a string shaped like a URL or a connection string is read for
# ``password=`` / ``token=`` pairs. Generated notebooks are full of SQL --
# a SQL override's ``WHERE TOKEN = 1`` or ``SET pwd=NULL`` is neither, and
# a first version of this rule failed a whole migration on a column that
# happened to be called TOKEN.
_CONNECTION_STRING_SHAPE = re.compile(
    r"://"                              # a URL
    r"|^\s*[A-Za-z][A-Za-z0-9+.-]*:\S"  # a scheme, e.g. jdbc:oracle:thin:
    r"|;\s*[A-Za-z_][\w ]*="            # ;Key=value (ODBC, SQL Server, ADO)
    r"|[?&]\w+="                        # ?key=value&key=value
)
_SQL_STATEMENT = re.compile(
    r"(?i)^\s*(?:select|with|insert|update|merge|delete|create|alter|drop|truncate)\b"
)

# Credential shapes that live INSIDE a string literal rather than beside
# one. Each pattern requires an actual value after the separator, so the
# constant half of an f-string such as ``"...;password="`` followed by a
# ``{os.environ[...]}`` hole never matches. The third element says whether
# the pattern applies only to connection-string-shaped text.
_CREDENTIAL_IN_STRING: tuple[tuple[str, "re.Pattern[str]", bool], ...] = (
    ("URL embeds user:password@",
     re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/:@\"']+:[^\s/@\"']+@"), False),
    ("JDBC thin URL embeds user/password@",
     re.compile(r"(?i)jdbc:oracle:thin:[^@\s/\"']+/[^@\s\"']+@"), False),
    ("connection string embeds password=",
     re.compile(r"(?i)(?:^|[;?&,\s])(?:password|passwd|pwd)\s*=\s*(?P<value>"
                + _NOT_A_PLACEHOLDER + ")"), True),
    ("URL or connection string embeds token=/api_key=",
     re.compile(r"(?i)(?:^|[;?&,\s])(?:access_token|auth_token|api_?key|token)\s*=\s*(?P<value>"
                + _NOT_A_PLACEHOLDER + ")"), True),
    ("Authorization header literal",
     re.compile(r"(?i)\bauthorization\b\s*[:=]\s*(?:basic|bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), False),
)
_AUTH_HEADER_NAME = re.compile(r"(?i)^authorization$")
_AUTH_HEADER_VALUE = re.compile(r"(?i)^(?:basic|bearer)\s+\S{8,}")

# ``name = "value"``, ``"name": "value"`` or ``name=value`` in text that is
# not Python: a markdown cell, or a code cell that does not parse.
_CREDENTIAL_IN_TEXT = re.compile(
    r"""(?x)
    (?<![\w.\]])
    (?P<name>[A-Za-z_][\w.]*(?:\[\s*["']?\w+["']?\s*\])?)["']?
    \s*[:=]\s*
    (?:["'](?P<quoted>[^"'\s{}$<%][^"']*)["']|(?P<bare>[^\s;&"'{}$<%,]+))
    """
)

# Methods whose positional ``(key, value)`` pair sets a configuration or
# header entry. Any other two-literal call is not one: the generators emit
# ``withColumnRenamed("PWD", "PWD_OLD")`` for a port that happens to be
# called PWD, and that is a column, not a credential.
_PAIR_SETTERS = frozenset({
    "option", "config", "conf", "set", "setdefault", "setProperty", "setopt",
    "setenv", "putenv", "put", "add_header", "add", "update", "insert",
})


def _str_const(node: ast.AST) -> "str | None":
    """The text of a non-empty string or bytes literal -- folding adjacent
    literals joined with ``+`` (``"tig" + "er"``) into one -- else None."""
    if isinstance(node, ast.Constant):
        if isinstance(node.value, str) and node.value:
            return node.value
        if isinstance(node.value, bytes) and node.value:
            return node.value.decode("latin-1")
        return None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _str_const(node.left), _str_const(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _target_name(node: ast.expr) -> "str | None":
    """The name an assignment target binds: ``x``, ``obj.attr``, or the
    string key of ``os.environ["X"]`` / ``props["x"]``."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _str_const(node.slice)
    return None


def _assignment_pairs(target: ast.expr, value: ast.expr) -> "list[tuple[str, ast.expr]]":
    """``(name, value node)`` pairs an assignment binds. A tuple/list
    target is unpacked against a tuple/list value element-wise, so
    ``user, password = "scott", "..."`` pairs ``password`` with its
    literal; a starred target or a length mismatch is skipped, not
    guessed."""
    if isinstance(target, (ast.Tuple, ast.List)):
        if (isinstance(value, (ast.Tuple, ast.List)) and len(value.elts) == len(target.elts)
                and not any(isinstance(e, ast.Starred) for e in target.elts)):
            return [p for t, v in zip(target.elts, value.elts) for p in _assignment_pairs(t, v)]
        return []
    name = _target_name(target)
    return [(name, value)] if name else []


def _call_pairs(node: ast.Call) -> "list[tuple[ast.expr, ast.expr]]":
    """The ``(key, value)`` node pairs a setter-style call carries:
    positional ``.option("password", v)``, keyword ``.option(key="password",
    value=v)`` and the mixed ``.option("password", value=v)``."""
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
    if name not in _PAIR_SETTERS:
        return []
    pairs = list(zip(node.args, node.args[1:]))
    kw = {k.arg: k.value for k in node.keywords if k.arg}
    if "value" in kw:
        if "key" in kw:
            pairs.append((kw["key"], kw["value"]))
        elif len(node.args) == 1:
            pairs.append((node.args[0], kw["value"]))
    return pairs


def _credential_shapes_in_text(text: str) -> "list[str]":
    """Which of the in-string credential shapes *text* carries."""
    connection_like = bool(_CONNECTION_STRING_SHAPE.search(text)) and not _SQL_STATEMENT.match(text)
    found = []
    for what, pattern, connection_strings_only in _CREDENTIAL_IN_STRING:
        if connection_strings_only and not connection_like:
            continue
        for m in pattern.finditer(text):
            value = m.groupdict().get("value")
            if value is not None and _VALUE_IS_NOT_A_SECRET.match(value):
                continue
            found.append(what)
            break
    return found


def hardcoded_credentials(code: str) -> list[str]:
    """Credential literals in generated code, one line per finding.

    The generators write every credential as a runtime lookup --
    ``password=os.environ["ADW_PASSWORD"]`` -- and a notebook is reviewed,
    committed and deployed as a file, so a literal in its place is a secret
    checked into the migration output. An LLM asked to "make it run" will
    do exactly that, and nothing downstream would have noticed: the
    notebook parses, every name resolves, and the write succeeds.

    Flags, by shape:

    - a keyword argument, assignment (plain, annotated, augmented, tuple
      unpacking, ``os.environ[...]`` / ``props[...]`` subscript), dict
      entry or setter-style ``.option()``/``.config()``/``.set()`` key pair
      -- positional or ``key=``/``value=`` -- whose name is a credential
      name (:func:`is_secret_name`) and whose value is a non-empty string
      or bytes literal, adjacent-literal concatenation included;
    - a string literal that embeds credentials in a URL or connection
      string (``user:pass@``, ``jdbc:oracle:thin:user/pass@``,
      ``password=``/``pwd=``/``token=`` with a real value in something
      shaped like a URL or connection string);
    - an ``Authorization: Basic/Bearer <literal>`` header, as a string, a
      dict/header pair or a ``headers["Authorization"] = ...`` assignment.

    Runtime lookups (``os.environ[...]``, ``dbutils.secrets.get(...)``,
    f-string holes) are not literals and are not flagged; an empty string
    is not a credential either; SQL text is not a connection string.
    Usernames and hostnames are configuration, not secrets, and are
    deliberately not a rule here. Findings name the line and the shape --
    never the value, since a validator that echoes the secret into
    ``broken_notebooks.md`` has only moved the leak. Code that does not
    parse, and markdown, go through :func:`credential_literals_in_text`.
    """
    tree = ast.parse(code)
    found: set[tuple[int, str]] = set()

    def flag(node: ast.AST, what: str) -> None:
        found.add((getattr(node, "lineno", 0), what))

    def key_value(key: "str | None", val: "str | None", val_node: ast.AST, shape: str) -> None:
        if not (key and val):
            return
        if is_secret_name(key):
            flag(val_node, shape.format(key=key))
        elif _AUTH_HEADER_NAME.match(key) and _AUTH_HEADER_VALUE.match(val):
            flag(val_node, "Authorization header literal")

    for node in ast.walk(tree):
        if isinstance(node, ast.keyword):
            if node.arg:
                key_value(node.arg, _str_const(node.value), node.value, "{key}= is a string literal")
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if node.value is not None:
                for t in targets:
                    for name, val in _assignment_pairs(t, node.value):
                        key_value(name, _str_const(val), node, "{key} is assigned a string literal")
        elif isinstance(node, ast.Dict):
            for k, v in zip(node.keys, node.values):
                if k is not None:
                    key_value(_str_const(k), _str_const(v), v, '"{key}" key holds a string literal')
        elif isinstance(node, ast.Call):
            for k, v in _call_pairs(node):
                key_value(_str_const(k), _str_const(v), v, '"{key}" is paired with a string literal')
        text = _str_const(node)
        if text:
            for what in _credential_shapes_in_text(text):
                flag(node, what)
    return [f"line {ln}: {what}" for ln, what in sorted(found)]


def credential_literals_in_text(text: str) -> list[str]:
    """The regex half of :func:`hardcoded_credentials`, for text that is not
    Python: a markdown cell, or a code cell that does not parse.

    A syntax error used to end the credential check -- the notebook was
    reported as "does not parse" and the password beside the typo shipped
    -- and markdown cells were never read at all. This pass needs no syntax
    tree: per line, a ``name = "value"`` / ``"name": "value"`` /
    ``name=value`` pair whose name is a credential name and whose value is
    a real one, plus the same URL / connection-string / Authorization
    shapes as the code rule. Findings name the line and the shape, never
    the value.
    """
    found: set[tuple[int, str]] = set()
    for ln, line in enumerate(text.splitlines(), 1):
        for m in _CREDENTIAL_IN_TEXT.finditer(line):
            value = m.group("quoted") or m.group("bare")
            if is_secret_name(m.group("name")) and not _VALUE_IS_NOT_A_SECRET.match(value):
                found.add((ln, f"{m.group('name')} is given a literal value"))
        for what in _credential_shapes_in_text(line):
            found.add((ln, what))
    return [f"line {ln}: {what}" for ln, what in sorted(found)]

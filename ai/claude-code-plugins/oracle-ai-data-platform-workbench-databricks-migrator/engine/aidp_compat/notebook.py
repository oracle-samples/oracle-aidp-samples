"""
AIDP Notebook Utils - Replacement for dbutils.notebook
Uses the same exec()-based approach proven to work on AIDP
(using exec()-based notebook execution, as in dbutils-compat shims).

Every cell passes the mandatory sandbox gate in ``aidp_compat.notebook_policy``
before it is executed: a sandbox (catalog, schema, object-storage prefix,
network posture) must be declared for the run, and each cell is AST-checked
for environment reads, process/network imports, destructive filesystem calls
and writes outside the sandbox. Refusals raise ``PermissionError`` naming the
notebook, cell, rule and remediation, and are recorded in the policy log.

While the cells run, a runtime SQL guard (:class:`SqlRuntimeGuard`) is
installed on the session's SQL entry points (``spark.sql`` / ``sqlContext.sql``
and ``spark.table`` on every session object found under the Spark root names,
the ``pyspark`` ``SparkSession`` / ``SQLContext`` / ``DataFrame`` /
``DataFrameReader`` classes, ``functions.expr`` and the DataFrame-API Java
calls ``functions.reflect`` / ``java_method`` / ``try_reflect`` /
``call_function``): every statement is re-classified by
``notebook_policy.classify_sql`` on its FINAL text (after pyspark's ``{name}``
keyword substitution) before Spark executes it, so a ``spark.sql`` statement
with run-time fragments that the static gate could only judge by its literal
head cannot smuggle a refused form past the gate. The guard is removed when
``run`` returns.
"""

import functools
import importlib
import inspect
import os
import sys
import json
import nbformat
from typing import Dict, Any, List, Optional, Tuple

from aidp_compat.notebook_policy import (
    SPARK_ROOT_NAMES,
    SandboxPolicy,
    ScanContext,
    assert_sql_allowed,
    assert_sql_expression_allowed,
    assert_sql_function_allowed,
    assert_table_read_allowed,
    enforce_cell_policy,
    record_skipped_cell,
    require_policy,
)


# pyspark classes / modules the guard patches for the duration of a run when
# pyspark is importable (each entry is skipped silently when it is not).
_PYSPARK_SESSION_CLASSES = (
    ("pyspark.sql", "SparkSession"),
    ("pyspark.sql", "SQLContext"),
    ("pyspark.sql.connect.session", "SparkSession"),
)
_PYSPARK_FRAME_CLASSES = (
    ("pyspark.sql", "DataFrame"),
    ("pyspark.sql.classic.dataframe", "DataFrame"),
    ("pyspark.sql.connect.dataframe", "DataFrame"),
)
_PYSPARK_READER_CLASSES = (
    ("pyspark.sql", "DataFrameReader"),
    ("pyspark.sql.connect.readwriter", "DataFrameReader"),
)
_PYSPARK_FUNCTION_MODULES = ("pyspark.sql.functions", "pyspark.sql.connect.functions")
_FRAME_EXPRESSION_METHODS = ("selectExpr", "filter", "where")
# functions.<name>(..) entry points that call arbitrary Java with no SQL string.
_JVM_FUNCTIONS = ("reflect", "java_method", "try_reflect")
_CALL_FUNCTION = "call_function"


def _import_optional(module_name: str):
    """``importlib.import_module`` that returns ``None`` for anything missing or broken."""
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None


class SqlRuntimeGuard:
    """Re-classify every SQL string on its way to Spark while a notebook runs.

    Patches, for the duration of a ``with`` block (or between :meth:`install`
    and :meth:`restore`):

    * ``<obj>.sql`` and ``<obj>.table`` on every session object bound in
      ``caller_globals`` under a Spark root name (``spark``, ``sqlContext``,
      ...), both the instance attribute and its class's own method;
    * ``SparkSession.sql`` / ``SQLContext.sql`` and ``DataFrameReader.table``
      of the importable ``pyspark`` classes (so ``SparkSession.sql(spark, q)``
      and the ``sql()`` magic helper that calls ``getOrCreate()`` internally
      are covered too);
    * ``DataFrame.selectExpr`` / ``filter`` / ``where`` (string form),
      ``functions.expr`` and the Java-call functions ``functions.reflect`` /
      ``java_method`` / ``try_reflect`` / ``call_function`` of the importable
      ``pyspark`` modules, plus every name in ``caller_globals`` bound to one
      of those original functions.

    Each wrapper runs :func:`~aidp_compat.notebook_policy.assert_sql_allowed`
    (statements, after the same ``{name}`` keyword substitution pyspark
    performs), :func:`~aidp_compat.notebook_policy.assert_sql_expression_allowed`
    (expressions), :func:`~aidp_compat.notebook_policy.assert_table_read_allowed`
    (table names) or :func:`~aidp_compat.notebook_policy.assert_sql_function_allowed`
    (function names) with the run's :class:`SandboxPolicy`; a refusal is
    written to the policy log and raised as ``PolicyViolation`` before Spark
    sees the text. Without ``pyspark`` the class patches are no-ops; the
    instance patches work on any object with a callable ``sql`` / ``table``
    (the tests drive them with fakes). ``patched`` lists the entry points that
    were wrapped.
    """

    def __init__(self, policy: SandboxPolicy, caller_globals: Dict[str, Any], notebook_path: str = ""):
        self._policy = policy
        self._globals = caller_globals
        self._notebook_path = notebook_path
        self.cell_index: Optional[int] = None
        # (owner, attribute, original value, owner had its own attribute, owner is a mapping)
        self._patches: List[Tuple[Any, str, Any, bool, bool]] = []
        self.patched: List[str] = []
        self._patched_ids: set = set()

    # -- wrappers ----------------------------------------------------------
    def _check_statement(self, operation: str, args: tuple, kwargs: dict) -> None:
        statement = args[0] if args else kwargs.get("sqlQuery", kwargs.get("query"))
        # pyspark renders every keyword other than args= into the statement's {name}
        # fields before parsing: the final text is judged after that substitution.
        template = {k: v for k, v in kwargs.items() if k not in ("sqlQuery", "query", "args")}
        assert_sql_allowed(statement, operation=operation, policy=self._policy,
                           notebook_path=self._notebook_path, cell_index=self.cell_index,
                           template_kwargs=template)

    def _check_table(self, operation: str, args: tuple, kwargs: dict) -> None:
        name = args[0] if args else kwargs.get("tableName")
        assert_table_read_allowed(name, operation=operation, policy=self._policy,
                                  notebook_path=self._notebook_path, cell_index=self.cell_index)

    def _check_function(self, operation: str, name: Any) -> None:
        assert_sql_function_allowed(name, operation=operation, policy=self._policy,
                                    notebook_path=self._notebook_path, cell_index=self.cell_index)

    def _check_expression(self, operation: str, value: Any) -> None:
        if isinstance(value, str):
            assert_sql_expression_allowed(value, operation=operation, policy=self._policy,
                                          notebook_path=self._notebook_path, cell_index=self.cell_index)

    def _wrap_sql_bound(self, original, shown: str):
        guard = self

        def guarded_sql(*args, **kwargs):
            guard._check_statement(shown, args, kwargs)
            return original(*args, **kwargs)

        guarded_sql.__wrapped__ = original
        return guarded_sql

    def _wrap_sql_method(self, original, shown: str):
        guard = self

        def guarded_sql(self_, *args, **kwargs):
            guard._check_statement(shown, args, kwargs)
            return original(self_, *args, **kwargs)

        guarded_sql.__wrapped__ = original
        return guarded_sql

    def _wrap_frame_method(self, original, name: str, shown: str):
        guard = self

        def guarded_method(self_, *args, **kwargs):
            if name == "selectExpr":
                exprs = args
                if len(args) == 1 and isinstance(args[0], (list, tuple)):
                    exprs = tuple(args[0])
                for e in exprs:
                    guard._check_expression(shown, e)
            else:
                condition = args[0] if args else kwargs.get("condition")
                guard._check_expression(shown, condition)
            return original(self_, *args, **kwargs)

        guarded_method.__wrapped__ = original
        return guarded_method

    def _wrap_expr(self, original, shown: str):
        guard = self

        def guarded_expr(*args, **kwargs):
            value = args[0] if args else kwargs.get("str")
            guard._check_expression(shown, value)
            return original(*args, **kwargs)

        guarded_expr.__wrapped__ = original
        return guarded_expr

    def _wrap_table_bound(self, original, shown: str):
        guard = self

        def guarded_table(*args, **kwargs):
            guard._check_table(shown, args, kwargs)
            return original(*args, **kwargs)

        guarded_table.__wrapped__ = original
        return guarded_table

    def _wrap_table_method(self, original, shown: str):
        guard = self

        def guarded_table(self_, *args, **kwargs):
            guard._check_table(shown, args, kwargs)
            return original(self_, *args, **kwargs)

        guarded_table.__wrapped__ = original
        return guarded_table

    def _wrap_function(self, original, name: str, shown: str):
        """``functions.reflect`` / ``java_method`` / ``try_reflect`` are refused outright;
        ``call_function(funcName, ..)`` is judged by the name it is given."""
        guard = self

        def guarded_function(*args, **kwargs):
            if name == _CALL_FUNCTION:
                guard._check_function(shown, args[0] if args else kwargs.get("funcName"))
            else:
                guard._check_function(shown, name)
            return original(*args, **kwargs)

        guarded_function.__wrapped__ = original
        return guarded_function

    # -- patch bookkeeping -------------------------------------------------
    def _patch(self, owner: Any, attr: str, replacement: Any, shown: str, mapping: bool = False) -> None:
        key = (id(owner), attr)
        if key in self._patched_ids:
            return
        if mapping:
            had_own = attr in owner
            original = owner.get(attr)
            owner[attr] = replacement
        else:
            had_own = attr in getattr(owner, "__dict__", {})
            original = owner.__dict__[attr] if had_own else getattr(owner, attr, None)
            setattr(owner, attr, replacement)
        self._patches.append((owner, attr, original, had_own, mapping))
        self._patched_ids.add(key)
        self.patched.append(shown)

    def _guard_session_object(self, name: str, obj: Any) -> None:
        if obj is None or inspect.ismodule(obj):
            return
        wrappers = (("sql", self._wrap_sql_bound, self._wrap_sql_method),
                    ("table", self._wrap_table_bound, self._wrap_table_method))
        if inspect.isclass(obj):
            for attr, _bound_wrapper, method_wrapper in wrappers:
                fn = obj.__dict__.get(attr)
                if callable(fn):
                    self._patch(obj, attr, method_wrapper(fn, f"{name}.{attr}"), f"{name}.{attr}")
            return
        if not callable(getattr(obj, "sql", None)):
            return
        cls = type(obj)
        for attr, bound_wrapper, method_wrapper in wrappers:
            bound = getattr(obj, attr, None)
            if not callable(bound):
                continue
            self._patch(obj, attr, bound_wrapper(bound, f"{name}.{attr}"), f"{name}.{attr}")
            fn = cls.__dict__.get(attr)
            if callable(fn):
                self._patch(cls, attr, method_wrapper(fn, f"{name}.{attr}"), f"{cls.__name__}.{attr}")

    def install(self) -> "SqlRuntimeGuard":
        # 1. session objects / classes bound in the executing namespace
        for name in sorted(SPARK_ROOT_NAMES):
            if name in self._globals:
                try:
                    self._guard_session_object(name, self._globals[name])
                except Exception:
                    pass        # an exotic object that refuses attribute assignment: nothing to wrap
        # 2. the pyspark session classes (every session, including ones made later)
        for module_name, class_name in _PYSPARK_SESSION_CLASSES:
            mod = _import_optional(module_name)
            cls = getattr(mod, class_name, None) if mod is not None else None
            if not inspect.isclass(cls):
                continue
            fn = cls.__dict__.get("sql")
            if callable(fn):
                self._patch(cls, "sql", self._wrap_sql_method(fn, f"{class_name}.sql"),
                            f"{module_name}.{class_name}.sql")
            fn = cls.__dict__.get("table")
            if callable(fn):
                self._patch(cls, "table", self._wrap_table_method(fn, f"{class_name}.table"),
                            f"{module_name}.{class_name}.table")
        # 3. DataFrame expression methods and DataFrameReader.table
        for module_name, class_name in _PYSPARK_FRAME_CLASSES:
            mod = _import_optional(module_name)
            cls = getattr(mod, class_name, None) if mod is not None else None
            if not inspect.isclass(cls):
                continue
            for method in _FRAME_EXPRESSION_METHODS:
                fn = cls.__dict__.get(method)
                if callable(fn):
                    self._patch(cls, method, self._wrap_frame_method(fn, method, f"DataFrame.{method}"),
                                f"{module_name}.{class_name}.{method}")
        for module_name, class_name in _PYSPARK_READER_CLASSES:
            mod = _import_optional(module_name)
            cls = getattr(mod, class_name, None) if mod is not None else None
            fn = cls.__dict__.get("table") if inspect.isclass(cls) else None
            if callable(fn):
                self._patch(cls, "table", self._wrap_table_method(fn, f"{class_name}.table"),
                            f"{module_name}.{class_name}.table")
        # 4. functions.expr and the Java-call functions, in the module and under every
        #    name the namespace bound them to (from pyspark.sql.functions import reflect)
        originals: List[Tuple[Any, Any]] = []       # (original function, wrapper factory)
        for module_name in _PYSPARK_FUNCTION_MODULES:
            mod = _import_optional(module_name)
            if mod is None:
                continue
            fn = getattr(mod, "expr", None)
            if callable(fn):
                originals.append((fn, self._wrap_expr))
                self._patch(mod, "expr", self._wrap_expr(fn, "functions.expr"), f"{module_name}.expr")
            for fname in _JVM_FUNCTIONS + (_CALL_FUNCTION,):
                fn = getattr(mod, fname, None)
                if callable(fn):
                    originals.append((fn, functools.partial(self._wrap_function_named, fname)))
                    self._patch(mod, fname, self._wrap_function(fn, fname, f"functions.{fname}"),
                                f"{module_name}.{fname}")
        if originals:
            for gname, gval in list(self._globals.items()):
                for fn, factory in originals:
                    if gval is fn:
                        self._patch(self._globals, gname, factory(gval, f"{gname}()"), f"<namespace>.{gname}",
                                    mapping=True)
                        break
        return self

    def _wrap_function_named(self, name: str, original, shown: str):
        return self._wrap_function(original, name, shown)

    def restore(self) -> None:
        for owner, attr, original, had_own, mapping in reversed(self._patches):
            try:
                if mapping:
                    if had_own:
                        owner[attr] = original
                    else:
                        owner.pop(attr, None)
                elif had_own:
                    setattr(owner, attr, original)
                else:
                    delattr(owner, attr)
            except Exception:
                pass
        self._patches = []
        self._patched_ids = set()

    def __enter__(self) -> "SqlRuntimeGuard":
        return self.install()

    def __exit__(self, *exc) -> bool:
        self.restore()
        return False


class NotebookExit(Exception):
    """Custom exception for notebook exit with value."""
    def __init__(self, value: str):
        self.value = value
        super().__init__(value)


# Module-level current notebook directory — set by the migration bootstrap so that
# dbutils.notebook.run("../relative/path") resolves relative to the calling notebook's
# directory rather than the workspace root.
_current_notebook_dir: Optional[str] = None


def set_notebook_dir(path: str) -> None:
    """Set the directory of the currently-executing notebook.

    Called by the migration bootstrap before executing each notebook's cells:
        from aidp_compat.notebook import set_notebook_dir
        set_notebook_dir("/Users/user@example.com/path/to/notebook_dir")
    """
    global _current_notebook_dir
    _current_notebook_dir = path


class _NotebookContext:
    """Mock for dbutils.notebook.entry_point...getContext()."""
    def toJson(self):
        import json
        return json.dumps({
            'workspaceid': '<WORKSPACE_ID>',
            'clusterId': '<CLUSTER_ID>',
            'orgId': '0',
            'notebookPath': '',
            'notebookId': '',
        })


class _NotebookHandle:
    """Mock for dbutils.notebook.entry_point...notebook()."""
    def getContext(self):
        return _NotebookContext()


class _EntryPoint:
    """Mock for dbutils.notebook.entry_point."""
    def getDbutils(self):
        return self
    def notebook(self):
        return _NotebookHandle()


class AIDPNotebookUtils:
    """Drop-in replacement for dbutils.notebook."""

    def __init__(self, spark=None, workspace_root: str = "/Workspace",
                 policy: Optional[SandboxPolicy] = None):
        self._spark = spark
        self._workspace_root = workspace_root
        # Explicit policy for this instance; otherwise the process-wide policy
        # (set_sandbox_policy) or the AIDP_SANDBOX_* environment is used.
        self._policy = policy

    @property
    def entry_point(self):
        """Mock for dbutils.notebook.entry_point chain."""
        return _EntryPoint()

    def _resolve_path(self, path: str) -> str:
        """Resolve notebook path to actual filesystem path."""
        # Add .ipynb if needed
        if not path.endswith(".ipynb"):
            path += ".ipynb"

        # Translate /Workspace to actual workspace root
        if path.startswith("/Workspace"):
            path = path.replace("/Workspace", self._workspace_root, 1)

        # Handle relative paths — resolve against the current notebook's directory if known,
        # otherwise fall back to the workspace root.
        if not path.startswith("/"):
            base = _current_notebook_dir if _current_notebook_dir else self._workspace_root
            path = os.path.normpath(os.path.join(base, path))

        return path

    def exit(self, value: str):
        """Exit the notebook with a return value."""
        raise NotebookExit(str(value))

    def run(self, path: str, timeout_seconds: int = 0,
            timeout: int = 0,
            arguments: Optional[Dict[str, Any]] = None) -> str:
        """Run another notebook and return its exit value.

        Uses exec() on notebook cells - proven to work on AIDP.
        This matches the dbutils-compat shim approach.

        Raises:
            SandboxUndeclaredError: no sandbox policy is declared (nothing runs).
            PolicyViolation: a cell was refused by the notebook policy; the
                message names the notebook path, cell index, rule id and
                remediation. Cells before it have already executed.
        """
        # Fail closed: the sandbox must be declared before anything executes.
        policy = self._policy or require_policy()

        notebook_path = self._resolve_path(path)

        if not os.path.exists(notebook_path):
            raise FileNotFoundError(
                f"Notebook not found: {notebook_path} (original: {path})"
            )

        # Read the notebook
        nb = nbformat.read(notebook_path, as_version=4)

        # Use the IPython kernel's namespace so variables propagate back
        # This mimics Databricks %run behavior (shared namespace)
        try:
            from IPython import get_ipython
            ip = get_ipython()
            if ip is not None:
                caller_globals = ip.user_ns
            else:
                import inspect
                caller_globals = inspect.stack()[1][0].f_globals
        except Exception:
            import inspect
            try:
                caller_globals = inspect.stack()[1][0].f_globals
            except Exception:
                caller_globals = globals()

        # Set widget parameters
        if arguments:
            try:
                from aidp_compat import dbutils as _db
                for key, value in arguments.items():
                    _db.widgets.text(key, str(value))
            except Exception as e:
                print(f"[notebook.run] warning: could not set widget parameters: {e}")

        # Import / shim aliases and string constants seen so far in this run,
        # so `import os as o` in one cell still gates `o.environ` in the next.
        # The runtime SQL guard below is active for every cell of the run, so
        # the static gate may admit a read statement with run-time fragments
        # (recorded as unverified_sql_read); the guard judges the final text.
        scan_context = ScanContext(runtime_sql_guard=True)

        # Runtime SQL guard: wrap the session's SQL entry points for the
        # duration of this run only, so the migrator's own code is unaffected.
        guard = SqlRuntimeGuard(policy, caller_globals, notebook_path=notebook_path)
        guard.install()

        # Execute each code cell in the caller's namespace
        try:
            for cell_index, cell in enumerate(nb.cells):
                if cell.cell_type == "code":
                    source = cell.source.strip()

                    if not source:
                        continue

                    # Skip magic commands that exec() can't handle. The whole
                    # cell is skipped: no line of it is compiled or executed.
                    # The skip is recorded in the policy log.
                    if source.startswith("%") or source.startswith("!"):
                        record_skipped_cell(source, notebook_path=notebook_path, cell_index=cell_index)
                        continue

                    # Mandatory policy gate at the execution sink: parse the
                    # cell and refuse anything that leaves the sandbox
                    # (PermissionError with notebook path, cell index, rule,
                    # remediation). Allowed exceptions are logged, not raised.
                    enforce_cell_policy(
                        source,
                        notebook_path=notebook_path,
                        cell_index=cell_index,
                        policy=policy,
                        context=scan_context,
                    )
                    guard.cell_index = cell_index
                    exec(compile(source, notebook_path, "exec"), caller_globals)

        except NotebookExit as e:
            return str(e.value)
        finally:
            guard.restore()

        return "ok"

    def help(self, method: str = None):
        print("dbutils.notebook - AIDP Notebook Utils")
        print("  exit(value) - Exit notebook with return value")
        print("  run(path, timeout, arguments) - Run another notebook")

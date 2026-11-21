"""Pure-Python extraction logic for the data-availability check.

Runs LOCALLY (no cluster). `check_data_availability_for_workflow.py` imports
this module directly, downloads each notebook via AIDP `downloadFileMeta`,
and calls `extract_from_notebook(nb_json)` on the parsed result. Cluster is
only used later for the existence checks (paths on Object Storage, table
lookups) that require the Spark session.

Public API:
  strip_comments(code, lang)     -> code with comments removed but strings/URLs preserved
  extract_from_source(src, lang) -> dict of paths/tables/unresolved/spark_reads/spark_writes/boto3_s3
  extract_from_notebook(nb_json) -> aggregated dict for the whole notebook

Bugs this module fixes vs. the historical cluster-side scanner:
  #1 Old scanner did `re.sub(r'//[^\n]*', '', code)` which killed every
     `s3://…`, `oci://…`, `dbfs://…` URL — the `//` after the scheme was
     treated as a Scala line comment.
  #2 Old scanner only matched `spark.read/write` with LITERAL string args.
     `spark.read.load(CUST_DEVICE_PATH)` was invisible.
  #3 `boto3.client('s3')` wasn't detected at all.
  #4 Chained calls like `spark.read.format("hudi").option(...).load(var)`
     didn't match — the read regex only allowed a single optional `.format`.
  #5 `.write.mode("overwrite").parquet(path)` — dominant PySpark write
     pattern — didn't match at all.
  #6 Triple-quoted string bodies were deleted, losing URLs inside
     `spark.sql(\"\"\"...\"\"\")`.
  #7 Comment strip was not quote-aware — `x = "abc#def"` produced `x = "abc`.
  #8 `fr"..."` / `rf"..."` string prefixes were misclassified.
"""
import ast
import calendar
import json
import posixpath
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, FrozenSet, List, Mapping, Optional, Tuple


# ─── STRING PREFIX HANDLING ──────────────────────────────────────────
# Python string prefixes: any combination of r/R, b/B, f/F, u/U, up to 3 chars.
# https://docs.python.org/3/reference/lexical_analysis.html#string-and-bytes-literals
_STRING_PREFIX_RE = re.compile(r'[rRbBfFuU]{0,3}$')


def _at_string_start(code: str, i: int) -> int:
    """If `code[i]` is a quote possibly preceded by a valid string prefix,
    return the length of the prefix (0-3). Otherwise return -1.

    Rule: Python allows [rR] [bB] [fF] [uU] in any order, 0-3 total, followed
    by a quote. Bare quote (no prefix) is length 0 — still a string start.
    """
    if i >= len(code) or code[i] not in ('"', "'"):
        return -1
    # Look back up to 3 chars for a valid prefix
    look_start = max(0, i - 3)
    prefix = code[look_start:i]
    m = _STRING_PREFIX_RE.search(prefix)
    if m:
        # Prefix must be immediately adjacent to the quote AND not preceded
        # by an identifier char (otherwise `myfr'x'` would false-positive
        # as a prefixed string starting at 'r').
        pre_len = len(m.group(0))
        prefix_start = i - pre_len
        if prefix_start > 0:
            prior = code[prefix_start - 1]
            if prior.isalnum() or prior == '_':
                return 0  # not a prefix — quote stands alone
        return pre_len
    return 0


# ─── COMMENT-STRIPPING STATE MACHINE ─────────────────────────────────


def strip_comments(code: str, lang: str = "python") -> str:
    """Strip source comments while preserving string literals and URLs.

    State machine walks the source char-by-char and tracks whether we're
    currently inside a single/double/triple-quoted string. Comments are only
    stripped OUTSIDE string boundaries.

      lang="python"  : strip `#` line comments. `//` is NOT stripped
                       (Python integer division, not a comment).
      lang="scala"   : strip `//` line comments AND `/* … */` block
                       comments. `#` is NOT stripped (not a Scala comment).
      lang="sql"     : strip `--` line comments AND `/* … */` block
                       comments. `#` is stripped (some SQL dialects).
      lang="r"       : strip `#` line comments.
      lang=""/unknown: strip `#` (Python-style), skip `//` (Python-ish
                       default — safer to preserve than to destroy).

    This fixes the historical bug where the scanner destroyed every URL
    like `s3://…` — the `//` was treated as a Scala comment. Now:
      - Python cells: `//` is never a comment (integer division).
      - Scala cells: `//` inside `scheme://…` is protected by the string
        state machine — we only see it AFTER the enclosing quote closes.

    Triple-quoted string bodies are PRESERVED (previously stripped), so
    URLs inside `spark.sql(\"\"\"…s3://bucket/…\"\"\")` still surface.
    """
    if not code:
        return code

    out: List[str] = []
    i = 0
    n = len(code)
    in_str: str = ""  # "" (not in string), "'", '"', "'''", '"""'

    strip_hash = lang in ("", "python", "r", "sql")
    # Python integer division `x // y` — do NOT treat `//` as a comment
    strip_slash_slash = lang in ("scala",)
    strip_dash_dash = lang in ("sql",)
    strip_block_c = lang in ("scala", "sql")

    while i < n:
        c = code[i]

        # (1) In-string state — emit chars until the closing quote is found.
        if in_str:
            # Escape handling: `\<any>` inside string is a 2-char sequence
            # and cannot end the string on the second char.
            if c == '\\' and i + 1 < n:
                out.append(c)
                out.append(code[i + 1])
                i += 2
                continue
            out.append(c)
            # Triple-quote close?
            if len(in_str) == 3 and code[i:i + 3] == in_str:
                # We just emitted the first `'` — emit the next two, close.
                if i + 1 < n:
                    out.append(code[i + 1])
                if i + 2 < n:
                    out.append(code[i + 2])
                i += 3
                in_str = ""
                continue
            # Single-quote close?
            if len(in_str) == 1 and c == in_str:
                in_str = ""
            i += 1
            continue

        # (2) Not in a string. Check for comment starts first (they cannot
        #     be preceded by a valid string prefix — those need a quote next).
        if strip_hash and c == '#':
            # Python `#` line comment
            j = code.find('\n', i)
            if j == -1:
                break  # end-of-source — everything left is comment
            i = j
            continue
        if strip_slash_slash and c == '/' and i + 1 < n and code[i + 1] == '/':
            j = code.find('\n', i)
            if j == -1:
                break
            i = j
            continue
        if strip_dash_dash and c == '-' and i + 1 < n and code[i + 1] == '-':
            j = code.find('\n', i)
            if j == -1:
                break
            i = j
            continue
        if strip_block_c and c == '/' and i + 1 < n and code[i + 1] == '*':
            end = code.find('*/', i + 2)
            if end == -1:
                break
            i = end + 2
            continue

        # (3) Check for string start (with optional prefix).
        if c in ('"', "'"):
            pre_len = _at_string_start(code, i)
            # pre_len is 0 or more; either way we enter a string here
            if pre_len < 0:
                # not a quote — shouldn't happen given c check above
                out.append(c)
                i += 1
                continue
            # We've already emitted code up to before the prefix in prior
            # iterations, so we don't need to backtrack — just emit the quote.
            # Triple-quote check
            if code[i:i + 3] in ('"""', "'''"):
                in_str = code[i:i + 3]
                out.append(code[i])
                out.append(code[i + 1])
                out.append(code[i + 2])
                i += 3
                continue
            in_str = c
            out.append(c)
            i += 1
            continue

        # (4) Regular source char — emit and advance.
        out.append(c)
        i += 1

    return ''.join(out)


# ─── URL SCHEME PATTERNS ─────────────────────────────────────────────
_S3_URL_RE      = re.compile(r's3a?://[\w\-\.]+/[\w\-\./]*')
_OCI_URL_RE     = re.compile(r'oci://[\w\-\.]+@[\w]+/[\w\-\./]*')
_DBFS_URL_RE    = re.compile(r'dbfs:/[\w\-\./]+')
_MNT_URL_RE     = re.compile(r'/mnt/[\w\-\./]+')
_VOLUMES_URL_RE = re.compile(r'/Volumes/[\w\-\./]+')

_URL_SCANNERS = [
    ("s3",      _S3_URL_RE),
    ("oci",     _OCI_URL_RE),
    ("dbfs",    _DBFS_URL_RE),
    ("mnt",     _MNT_URL_RE),
    ("volumes", _VOLUMES_URL_RE),
]

# f-string URL literal (all Python prefix variants: f, F, rf, fr, Fr, Rf, etc.)
_FSTRING_URL_RE = re.compile(
    r'(?:[fF][rR]?|[rR][fF])["\']((?:s3|oci|dbfs|/mnt|/Volumes)[^"\']*)["\']'
)


# ─── VARIABLE-BASED SPARK READ / WRITE ───────────────────────────────
# Real customer forms these capture that the OLD scanner missed:
#   spark.read.load(var)
#   spark.read.parquet(var)
#   spark.read.format("hudi").load(var)
#   spark.read.format("parquet").option("k","v").option("k2","v2").load(var)
#   spark.read.option("k","v").parquet(var)
#   df.write.mode("overwrite").parquet(var)              ← dominant write
#   df.write.mode("append").partitionBy("dt").parquet(var)
#   df.write.format("delta").mode("overwrite").save(var)
#
# The `(?:\.\w+\s*\([^()]*\))*` middle allows zero-or-more chained method
# calls like `.format(...)`, `.option(...)`, `.mode(...)`, `.partitionBy(...)`
# between `.read`/`.write` and the terminal API. `[^()]*` is deliberately
# ungreedy on nested parens — realistic chained args are simple literals.
_READ_APIS  = "load|parquet|json|csv|orc|text|table|jdbc|delta|avro"
_WRITE_APIS = "save|saveAsTable|parquet|json|csv|orc|text|insertInto|jdbc|delta|avro"

_SPARK_READ_RE = re.compile(
    rf'spark\.read(?:\s*\.\w+\s*\([^()]*\))*\s*\.({_READ_APIS})\s*\(\s*([^)]*?)\s*\)',
    re.DOTALL,
)

# A temporary view is an in-notebook runtime object, not an external catalog
# dependency.  Capture both Python and Scala spellings so a later
# ``spark.read.table("viewName")`` is not sent to the pre-migration table
# availability probe.
_TEMP_VIEW_RE = re.compile(
    r"\.(?P<api>createOrReplaceTempView|createTempView|createGlobalTempView)"
    r"\s*\(\s*['\"](?P<name>[^'\"]+)['\"]\s*\)"
)

_DATA_PATH_PREFIXES_FOR_CLASSIFICATION = (
    "oci://", "s3://", "s3a://", "dbfs:/", "file:/",
    "/Volumes/", "/Workspace/", "/dbfs/", "/mnt/",
)


def _looks_like_data_path(value: Any) -> bool:
    """Return True when a resolved Spark argument is clearly a path.

    Runtime parameters sometimes feed one branch to ``spark.read.table`` and
    another to ``read.csv``/``read.parquet``.  When static control-flow cannot
    select the branch, the resolved value still tells us whether the object is
    a filesystem path.  This prevents values such as ``/Volumes/.../x.csv``
    from being probed with ``DESCRIBE TABLE``.
    """
    if not isinstance(value, str):
        return False
    candidate = value.strip()
    if not candidate:
        return False
    return (
        candidate.startswith(_DATA_PATH_PREFIXES_FOR_CLASSIFICATION)
        or "://" in candidate
        or bool(re.match(r"^[A-Za-z]:[\\/]", candidate))
    )
_SPARK_WRITE_RE = re.compile(
    rf'\.write(?:\s*\.\w+\s*\([^()]*\))*\s*\.({_WRITE_APIS})\s*\(\s*([^)]*?)\s*\)',
    re.DOTALL,
)


def _arg_is_literal_string(arg: str) -> bool:
    r"""Return True if the captured arg is a string literal.

    Accepts every valid Python prefix combination: r, R, b, B, f, F, u, U
    (0-3 chars) followed by a quote (single, double, or triple).
    """
    a = arg.strip()
    if not a:
        return False
    if a[0] in ('"', "'"):
        return True
    # Check for prefix (up to 3 chars) followed by a quote
    m = re.match(r'^[rRbBfFuU]{1,3}["\']', a)
    return m is not None


def _url_match_is_dynamic_fragment(
    source: str,
    start: int,
    end: int,
    dynamic_fstring_spans: List[Tuple[int, int]],
) -> bool:
    """Return whether a URL-regex match is only part of a dynamic path.

    Besides f-strings, customer notebooks commonly build paths with a literal
    prefix followed by concatenation, for example
    ``"s3://bucket/user-" + user_id``.  Treating that prefix as a complete
    path creates a false MISSING row in the migration tracker.
    """
    if any(span_start <= start and end <= span_end
           for span_start, span_end in dynamic_fstring_spans):
        return True

    # The URL regex stops immediately before the closing quote.  If that
    # string literal is then concatenated, %-formatted, or .format()-expanded,
    # the match is a prefix rather than a concrete path.
    suffix = source[end:]
    return re.match(
        r"^(?:\"\"\"|'''|\"|')\s*(?:\+|%|\.format\s*\()",
        suffix,
        re.DOTALL,
    ) is not None


# ─── BOTO3 S3 CALLS ──────────────────────────────────────────────────
_BOTO3_S3_RE = re.compile(r"boto3\.(?:client|resource)\s*\(\s*['\"]s3['\"]")


# ─── TABLE PATTERNS ──────────────────────────────────────────────────
_SQL_KW_TABLE_RE = re.compile(
    r'(?:FROM|JOIN|INTO|TABLE|OVERWRITE)\s+(\w+\.\w+)',
    re.IGNORECASE,
)
_SPARK_TABLE_LITERAL_RE = re.compile(r"""spark\.(?:sql|table|read\.table)\s*\(\s*['"]([\w]+\.[\w]+)""")
_SAVE_AS_TABLE_RE       = re.compile(r"""\.saveAsTable\s*\(\s*['"]([\w]+\.[\w]+)""")
_INSERT_INTO_RE         = re.compile(r"""\.insertInto\s*\(\s*['"]([\w]+\.[\w]+)""")

_SQL_TABLE_SKIP_LHS = frozenset(("self", "spark", "dbutils", "sc", "np", "pd", "plt"))


# ─── IMPORT SKIP-LISTS ───────────────────────────────────────────────
_SKIP_PY = frozenset({
    "os", "sys", "json", "re", "math", "time", "datetime",
    "collections", "functools", "itertools", "typing", "abc",
    "io", "copy", "pickle", "csv", "pathlib", "shutil", "hashlib",
    "base64", "uuid", "socket", "http", "urllib", "logging",
    "warnings", "subprocess", "multiprocessing", "threading",
    "importlib", "operator", "string", "struct", "enum",
    "contextlib", "textwrap", "traceback", "inspect", "gc",
    "signal", "tempfile", "glob", "fnmatch", "stat",
    "builtins", "__future__", "array", "bisect", "heapq",
    "decimal", "fractions", "random", "statistics",
    "pprint", "reprlib", "types", "weakref",
    "codecs", "unicodedata", "locale", "gettext",
    "argparse", "configparser", "secrets", "hmac",
    "xml", "html", "email", "mimetypes",
    "concurrent", "asyncio", "queue",
    "ctypes", "platform", "sysconfig",
    "unittest", "doctest", "pdb",
    "zipfile", "gzip", "bz2", "lzma", "tarfile",
    "sqlite3", "dbm", "shelve",
    "IPython", "ipykernel", "ipywidgets",
    "pyspark", "pandas", "numpy", "scipy", "sklearn",
    "matplotlib", "seaborn", "plotly",
    "pyarrow", "koalas", "delta",
    "mlflow", "hyperopt",
    "requests", "boto3", "botocore",
    "oci", "azure", "google",
    "py4j", "six", "dateutil", "pytz",
    "setuptools", "pip", "pkg_resources", "wheel",
})
_JVM_ROOTS = frozenset({"java", "javax", "scala", "com", "org", "net", "io", "akka"})
_MAGIC_LANG = {"py": "python", "python": "python", "scala": "scala", "sql": "sql", "r": "r"}


# Structured reference extraction -------------------------------------------------
#
# The legacy regex scanner above deliberately remains in place: it is broad and is
# useful for surfacing unresolved references. The AST analyser below adds execution
# semantics that regex cannot provide: variable/f-string resolution, dataframe
# lineage, filters, runtime dates, and custom createTable writes.

_UNKNOWN = object()

# Upper bound on elements a statically-evaluated comprehension may produce
# (REF-256). Real per-partition path lists are tiny (years/months); a huge
# result signals generated data the scanner should not expand — fail closed
# to unresolved rather than flooding the tracker with thousands of entries.
_COMPREHENSION_ELEMENT_CAP = 256

# Iteration-work bound for comprehension evaluation (REF-256 review
# 2026-10-27): the element cap alone counts only EMITTED results, so nested
# literal iterables behind a false filter could do unbounded Cartesian work.
_COMPREHENSION_WORK_CAP = 2048

# Spark readers whose positional arguments are all paths. In Spark 3.5 ONLY
# parquet has the variadic *paths signature; orc(path, mergeSchema=...) and
# text(paths, wholetext=...) take a path-or-list first argument followed by
# positional OPTIONS, and load(path, format, schema) likewise — all of those
# keep the legacy single-argument evaluation (review rounds 2026-10-27).
_VARIADIC_PATH_READ_METHODS = frozenset({"parquet"})


@dataclass(frozen=True)
class ModuleNamespace:
    """Module-level names of an imported workspace module.

    ``values`` holds the statically known values; a name in ``names`` with no
    value is bound to something unknown (a function, a class, a runtime value).
    ``input_gaps`` retains registry/file read boundaries without assuming how
    an imported helper will be called. It does not alter symbol resolution.
    """
    values: Mapping[str, Any]
    names: FrozenSet[str]
    input_gaps: Tuple[Mapping[str, Any], ...] = field(default=(), compare=False)

    def star_names(self) -> List[str]:
        public = self.values.get("__all__")
        if isinstance(public, (list, tuple)):
            return [name for name in public if isinstance(name, str)]
        return sorted(name for name in self.names if not name.startswith("_"))


def _scope_bindings(statements: List[ast.stmt]) -> Tuple[List[str], List[Optional[str]]]:
    """Names bound in one scope by ``statements``, once per binding site,
    and the modules of star imports among them. Nested function and class
    bodies are separate scopes: their names count, their bodies do not."""
    names: List[str] = []
    star_modules: List[Optional[str]] = []

    def visit(node: ast.AST) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                names.append(child.name)
                continue
            if isinstance(child, ast.Lambda):
                continue
            if isinstance(child, ast.Name) and isinstance(child.ctx, (ast.Store, ast.Del)):
                names.append(child.id)
            elif isinstance(child, (ast.Import, ast.ImportFrom)):
                for alias in child.names:
                    if alias.name == "*":
                        star_modules.append(getattr(child, "module", None)
                                            if not getattr(child, "level", 0) else None)
                    else:
                        names.append(alias.asname or alias.name.split(".")[0])
            elif isinstance(child, ast.ExceptHandler) and child.name:
                names.append(child.name)
            visit(child)

    visit(ast.Module(body=list(statements), type_ignores=[]))
    return names, star_modules


def _rebound_globals(trees: List[ast.Module],
                     import_namespaces: Mapping[str, Optional["ModuleNamespace"]]) -> FrozenSet[str]:
    """Module-scope names bound more than once (counting ``global``
    declarations inside functions). A function body may run after any of
    those bindings, so it cannot rely on the value seen at its definition."""
    counts: Dict[str, int] = {}
    for tree in trees:
        names, star_modules = _scope_bindings(tree.body)
        for module in star_modules:
            namespace = import_namespaces.get(module) if module else None
            if namespace is not None:
                names.extend(namespace.star_names())
        names.extend(
            name for node in ast.walk(tree) if isinstance(node, ast.Global)
            for name in node.names)
        for name in names:
            counts[name] = counts.get(name, 0) + 1
    return frozenset(name for name, count in counts.items() if count > 1)


def _expression_scopes(node: ast.AST) -> Dict[int, Tuple[FrozenSet[str], bool]]:
    """For each call in an expression that runs inside a lambda or a
    comprehension: the names those scopes bind (parameters, loop targets),
    and whether the call may run later than the statement (a lambda body or
    a generator expression). Such a call cannot see those names' outer
    bindings, nor names bound in an enclosing class body. Keyed by
    ``id(call)``; calls that run in the enclosing scope itself (including a
    comprehension's first iterable) are absent."""
    scopes: Dict[int, Tuple[FrozenSet[str], bool]] = {}

    def visit(child: ast.AST, hidden: FrozenSet[str], nested: bool, deferred: bool) -> None:
        if isinstance(child, ast.Lambda):
            arguments = child.args
            for default in arguments.defaults + [d for d in arguments.kw_defaults if d]:
                visit(default, hidden, nested, deferred)
            parameters = {
                arg.arg for arg in (arguments.posonlyargs + arguments.args
                                    + arguments.kwonlyargs)
            } | {arg.arg for arg in (arguments.vararg, arguments.kwarg) if arg}
            visit(child.body, hidden | parameters, True, True)
            return
        if isinstance(child, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)):
            generators = child.generators
            visit(generators[0].iter, hidden, nested, deferred)
            inner = hidden | {
                name.id for generator in generators for name in ast.walk(generator.target)
                if isinstance(name, ast.Name)
            }
            lazy = deferred or isinstance(child, ast.GeneratorExp)
            for index, generator in enumerate(generators):
                if index:
                    visit(generator.iter, inner, True, lazy)
                for condition in generator.ifs:
                    visit(condition, inner, True, lazy)
            results = ((child.key, child.value) if isinstance(child, ast.DictComp)
                       else (child.elt,))
            for result in results:
                visit(result, inner, True, lazy)
            return
        if isinstance(child, ast.Call) and nested:
            scopes[id(child)] = (hidden, deferred)
        for grandchild in ast.iter_child_nodes(child):
            visit(grandchild, hidden, nested, deferred)

    visit(node, frozenset(), False, False)
    return scopes


def _parse_python(source: str) -> Optional[ast.Module]:
    lines = source.splitlines()
    if lines and lines[0].lstrip().startswith("%python"):
        source = "\n".join(lines[1:])
    try:
        return ast.parse(source)
    except (SyntaxError, ValueError):
        return None


def _same_binding(left: Any, right: Any) -> bool:
    try:
        return type(left) is type(right) and bool(left == right)
    except Exception:
        return False
_READ_METHODS = frozenset({"load", "parquet", "json", "csv", "orc", "text", "table", "jdbc", "delta", "avro"})
_WRITE_METHODS = frozenset({"save", "saveAsTable", "insertInto", "parquet", "json", "csv", "orc", "text", "delta", "avro"})
_PYTHON_FILE_READ_METHODS = frozenset({
    "read_csv", "read_excel", "read_feather", "read_fwf", "read_html",
    "read_json", "read_orc", "read_parquet", "read_pickle", "read_sas",
    "read_stata", "read_table", "read_xml",
})
_REGISTRY_METADATA_READ_METHODS = frozenset({
    "get_registered_model", "get_model_version",
    "search_registered_models", "search_model_versions",
})
_MLFLOW_CLIENT_ARTIFACT_METHODS = frozenset({
    "download_artifacts", "list_artifacts",
})
_MLFLOW_ARTIFACT_FUNCTIONS = frozenset({
    "download_artifacts", "list_artifacts", "load_dict", "load_image",
    "load_text",
})
_DATA_PATH_PREFIXES = (
    "s3://", "s3a://", "oci://", "dbfs:/", "/dbfs/", "/FileStore/",
    "/mnt/", "/Volumes/", "/Workspace/",
)
# Readers given an in-memory buffer read no file.
_IN_MEMORY_BUFFERS = frozenset({"io.StringIO", "io.BytesIO", "StringIO", "BytesIO"})
_FILTER_METHODS = frozenset({"filter", "where"})
_LINEAGE_METHODS = frozenset({
    "alias", "cache", "checkpoint", "coalesce", "distinct", "drop",
    "dropDuplicates", "dropna", "fillna", "hint", "limit", "localCheckpoint",
    "orderBy", "persist", "repartition", "repartitionByRange", "sample",
    "select", "selectExpr", "sort", "sortWithinPartitions", "toDF",
    "transform", "union", "unionByName", "withColumn", "withColumnRenamed",
    "withColumns",
})
# Schema/plan introspection reads no rows; these must not force a full-table
# migration for an otherwise scoped source.
_METADATA_ONLY_METHODS = frozenset({"explain", "printSchema", "unpersist"})
_DATE_TEXT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SIMPLE_FILTER_RE = re.compile(
    r"(?P<column>[A-Za-z_][A-Za-z0-9_\.]*)\s*"
    r"(?P<operator>BETWEEN|==|=|>=|<=|>|<)\s*"
    r"(?P<first>'\d{4}[-_]\d{2}[-_]\d{2}'|\"\d{4}[-_]\d{2}[-_]\d{2}\")"
    r"(?:\s+AND\s+(?P<second>'\d{4}[-_]\d{2}[-_]\d{2}'|\"\d{4}[-_]\d{2}[-_]\d{2}\"))?",
    re.IGNORECASE,
)
_LIKE_DATE_FILTER_RE = re.compile(
    r"(?P<column>[A-Za-z_][A-Za-z0-9_\.]*)\s+LIKE\s+"
    r"['\"]%(?P<value>\d{4}[-_]\d{2}[-_]\d{2})%['\"]",
    re.IGNORECASE,
)


class _MonthDelta:
    def __init__(self, months: int = 0, days: int = 0):
        self.months = months
        self.days = days


def _add_months(value: Any, months: int) -> Any:
    if not isinstance(value, (date, datetime)):
        return _UNKNOWN
    absolute = value.year * 12 + value.month - 1 + months
    year, month_index = divmod(absolute, 12)
    month = month_index + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _literal(value: Any) -> str:
    if isinstance(value, datetime):
        value = value.date().isoformat()
    elif isinstance(value, date):
        value = value.isoformat()
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def _call_name(node: ast.AST) -> str:
    parts: List[str] = []
    current = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
    return ".".join(reversed(parts))


def _file_read_api_name(name: str) -> Optional[str]:
    """Identify supported file-read syntax without resolving its arguments."""
    if name in {"open", "builtins.open", "io.open", "glob.glob", "glob.iglob"}:
        return name
    if (name.startswith(("pd.", "pandas."))
            and name.split(".")[-1] in _PYTHON_FILE_READ_METHODS):
        return name
    return None


def _python_file_read_api(call: ast.Call) -> Optional[str]:
    return _file_read_api_name(_call_name(call.func))


def _column_name(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Call) and _call_name(node.func).split(".")[-1] == "col" and node.args:
        value = node.args[0]
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _metadata_from_filters(filters: List[str]) -> Dict[str, Any]:
    """Derive tracker-compatible partition/date fields from filters."""
    result: Dict[str, Any] = {}
    bounds: Dict[str, Dict[str, str]] = {}
    for expression in filters:
        like_date = _LIKE_DATE_FILTER_RE.search(expression)
        if like_date:
            result.setdefault(
                "partition_column", like_date.group("column").rsplit(".", 1)[-1]
            )
            result.setdefault("partition_value", like_date.group("value"))
        for match in _SIMPLE_FILTER_RE.finditer(expression):
            column = match.group("column").rsplit(".", 1)[-1]
            operator = match.group("operator").upper()
            first = match.group("first")[1:-1]
            second = match.group("second")
            if operator == "BETWEEN" and second:
                result.setdefault("partition_column", column)
                result.setdefault("start_date", first.replace("_", "-"))
                result.setdefault("end_date", second[1:-1].replace("_", "-"))
            elif operator in ("=", "=="):
                result.setdefault("partition_column", column)
                result.setdefault("partition_value", first)
            elif operator in (">", ">="):
                bounds.setdefault(column, {})["start_date"] = first.replace("_", "-")
            elif operator in ("<", "<="):
                bounds.setdefault(column, {})["end_date"] = first.replace("_", "-")
    if bounds:
        column, values = next(
            ((c, v) for c, v in bounds.items() if len(v) == 2),
            next(iter(bounds.items())),
        )
        result.setdefault("partition_column", column)
        for key, value in values.items():
            result.setdefault(key, value)
    return result


class _PythonReferenceAnalyzer:
    """Best-effort, non-executing analyser for PySpark data references."""

    def __init__(self, runtime_parameters: Optional[Mapping[str, Any]] = None,
                 initial_symbols: Optional[Mapping[str, Any]] = None,
                 notebook_dir: Optional[str] = None,
                 import_namespaces: Optional[Mapping[str, Optional[ModuleNamespace]]] = None):
        self.widget_defaults: Dict[str, Any] = {}
        self.removed_widgets: set[str] = set()
        self.widgets_unknown = False
        self.widgets_ambiguous = False
        self.runtime_parameters = dict(runtime_parameters or {})
        # Workspace modules a ``from <module> import`` can bind from: a
        # namespace, or None when the module exists but could not be analysed.
        # Modules absent here are installed packages; their names are unknown.
        self.import_namespaces = dict(import_namespaces or {})
        # Names bound at module scope, with or without a known value.
        self.bound_names: set[str] = set()
        # Module-scope names bound more than once (see `_rebound_globals`);
        # unknown inside function bodies. Set by the caller after a pre-pass.
        self.rebound_globals: FrozenSet[str] = frozenset()
        self._scope_depth = 0
        # While walking a class body: the (env, lineage) of the scope around
        # it. Names bound in a class body are invisible to the functions,
        # classes, lambdas and comprehensions nested in it.
        self._class_scope: Optional[Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]]]] = None
        # Working directory of the notebook at run time, when known (the
        # saved notebook changes into the folder of a Git repository
        # notebook). Relative reads resolve against it; without it they are
        # reported unresolved.
        self.notebook_dir = notebook_dir.rstrip("/") if notebook_dir else None
        self.env: Dict[str, Any] = dict(initial_symbols or {})
        self.env.update({
            key: value for key, value in self.runtime_parameters.items()
            if value is not None
            and not (isinstance(value, str) and not value.strip())
            and not self._sensitive_widget(key)
        })
        self.lineage: Dict[str, List[Dict[str, Any]]] = {}
        self.references: List[Dict[str, Any]] = []
        self.unresolved_references: List[Dict[str, Any]] = []
        self.writes: List[Dict[str, Any]] = []
        self.mlflow_artifacts: List[Dict[str, Any]] = []
        self.unresolved_mlflow_artifacts: List[Dict[str, Any]] = []
        self.analysis_errors: List[Dict[str, Any]] = []
        self.notebook_runs: List[Dict[str, Any]] = []
        self.not_analyzed: List[Dict[str, Any]] = []
        self.unresolved_loads: List[Dict[str, Any]] = []
        self._visited_eval_calls: set[ast.Call] = set()
        self._accounted_input_calls: set[ast.Call] = set()
        self._inactive_input_calls: set[ast.Call] = set()
        self.mlflow_clients: set[str] = set()
        self.resolved_arguments: set[str] = set()
        # Shared iteration budget for the comprehension evaluator; owned by
        # the outermost _eval_comprehension call (None between evaluations).
        self._comprehension_work: Optional[List[int]] = None
        self.cell_number = 0
        self.anchor_parameter: Optional[str] = None
        self.anchor = self._runtime_anchor()

    def _runtime_anchor(self) -> Optional[datetime]:
        for key in ("execution_date_ist", "execution_date", "run_date", "processing_date", "as_of_date"):
            raw = self.runtime_parameters.get(key)
            if raw is None:
                continue
            text = str(raw).replace("_", "-")
            try:
                parsed = datetime.fromisoformat(text)
                self.anchor_parameter = key
                return parsed
            except ValueError:
                continue
        # Static analysis must not invent a date.  Doing so turns a missing
        # workflow parameter into a plausible-looking, but incorrect, scope.
        return None

    def _eval(self, node: Optional[ast.AST]) -> Any:
        if node is None:
            return _UNKNOWN
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            return self.env.get(node.id, _UNKNOWN)
        if isinstance(node, ast.Dict):
            keys = [self._eval(k) for k in node.keys]
            values = [self._eval(v) for v in node.values]
            if any(x is _UNKNOWN for x in keys + values):
                return _UNKNOWN
            return dict(zip(keys, values))
        if isinstance(node, (ast.List, ast.Tuple)):
            values = [self._eval(x) for x in node.elts]
            if any(x is _UNKNOWN for x in values):
                return _UNKNOWN
            # Tuple literals may be dictionary keys; keep them hashable.
            return tuple(values) if isinstance(node, ast.Tuple) else values
        if isinstance(node, ast.ListComp):
            # Eager in Python: safe to materialize at the binding site.
            return self._eval_comprehension(node)
        # ast.GeneratorExp is deliberately NOT evaluated here: its body runs
        # at CONSUMPTION time, so a name rebound between binding and the read
        # would make an eagerly-captured value wrong (review 2026-10-27).
        # Inline generator arguments are evaluated at their consumption site
        # in _new_multi_path_read; stored generators stay unresolved.
        if isinstance(node, ast.JoinedStr):
            parts: List[str] = []
            for item in node.values:
                if isinstance(item, ast.Constant):
                    parts.append(str(item.value))
                elif isinstance(item, ast.FormattedValue):
                    value = self._eval(item.value)
                    if value is _UNKNOWN or (isinstance(value, str) and not value.strip()):
                        return _UNKNOWN
                    # Apply conversion and format spec with real format()
                    # semantics: str(value) alone turned month={m:02d} into
                    # month=1 — a WRONG concrete identity (review 2026-10-27;
                    # pre-existing for scalar f-strings, widened by REF-256
                    # comprehension support). Fail closed on any error.
                    if item.conversion == 115:
                        value = str(value)
                    elif item.conversion == 114:
                        value = repr(value)
                    elif item.conversion == 97:
                        value = ascii(value)
                    spec = ""
                    if item.format_spec is not None:
                        spec = self._eval(item.format_spec)
                        if not isinstance(spec, str):
                            return _UNKNOWN
                    try:
                        parts.append(format(value, spec))
                    except Exception:
                        return _UNKNOWN
            return "".join(parts)
        if isinstance(node, ast.Subscript):
            value, key = self._eval(node.value), self._eval(node.slice)
            try:
                return value[key]
            except Exception:
                return _UNKNOWN
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            value = self._eval(node.operand)
            return -value if isinstance(value, (int, float)) else _UNKNOWN
        if isinstance(node, ast.BinOp):
            left, right = self._eval(node.left), self._eval(node.right)
            if left is _UNKNOWN or right is _UNKNOWN:
                return _UNKNOWN
            try:
                if isinstance(node.op, ast.Add):
                    if isinstance(right, _MonthDelta):
                        shifted = _add_months(left, right.months)
                        return shifted + timedelta(days=right.days) if shifted is not _UNKNOWN else _UNKNOWN
                    return left + right
                if isinstance(node.op, ast.Sub):
                    if isinstance(right, _MonthDelta):
                        shifted = _add_months(left, -right.months)
                        return shifted - timedelta(days=right.days) if shifted is not _UNKNOWN else _UNKNOWN
                    return left - right
            except Exception:
                return _UNKNOWN
        if not isinstance(node, ast.Call):
            return _UNKNOWN

        name = _call_name(node.func)
        short = name.split(".")[-1]
        args = [self._eval(x) for x in node.args]
        kwargs = {kw.arg: self._eval(kw.value) for kw in node.keywords if kw.arg}
        # A **expansion has kw.arg None and VANISHES from the filtered dict,
        # bypassing every argument-shape check (int(p, **{"base": 16}) read
        # as decimal — review 2026-10-28). No branch models it: decline.
        if any(kw.arg is None for kw in node.keywords):
            return _UNKNOWN
        if short in ("today", "now") and name.rsplit(".", 1)[0].split(".")[-1] in ("datetime", "date"):
            return self.anchor if self.anchor is not None else _UNKNOWN
        if short == "relativedelta":
            # Decline shapes this branch does not model: ignoring e.g.
            # years=/weeks= (or positional args) silently produced a WRONG
            # delta (reviewer-flagged residual class, 2026-10-28).
            if node.args or set(kwargs) - {"months", "days"}:
                return _UNKNOWN
            months = kwargs.get("months", 0)
            days = kwargs.get("days", 0)
            if not all(isinstance(v, int) and not isinstance(v, bool)
                       for v in (months, days)):
                # Fractional/other-typed deltas are not modelled by
                # _MonthDelta — truncating them selects wrong partitions.
                return _UNKNOWN
            return _MonthDelta(months, days)
        if short == "timedelta":
            if node.args or set(kwargs) - {"days"}:
                return _UNKNOWN
            days = kwargs.get("days", 0)
            if not isinstance(days, (int, float)) or isinstance(days, bool):
                # int() truncation turned days=0.5 into 0 — a WRONG date
                # partition (review 2026-10-28). Pass the value untouched.
                return _UNKNOWN
            try:
                return timedelta(days=days)
            except (TypeError, ValueError, OverflowError):
                return _UNKNOWN
        is_widget_get = (
            short == "get"
            and node.args
            and ("dbutils.widgets" in name or "aidputils.widgets" in name)
        )
        is_parameter_get = (
            short == "getParameter"
            and node.args
            and ("parameters" in name or name == "getParameter")
        )
        if node.args and (
            short == "getArgument" or is_widget_get or is_parameter_get
        ):
            key = args[0]
            if key is _UNKNOWN:
                return _UNKNOWN
            if self._sensitive_widget(str(key)):
                return _UNKNOWN
            default = args[1] if len(args) > 1 else _UNKNOWN
            value = self.runtime_parameters.get(str(key), _UNKNOWN)
            if is_widget_get:
                # An explicit blank runtime input is invalid regardless of
                # uncertain widget existence/defaults. Preserve it for the
                # Spark-read finding; nonblank values still need known state.
                if isinstance(value, str) and not value.strip():
                    return value
                if (str(key) in self.removed_widgets
                        or (self.widgets_unknown and str(key) not in self.widget_defaults)):
                    return _UNKNOWN
                return self.widget_defaults.get(str(key), default) if value is _UNKNOWN else value
            if (
                value is _UNKNOWN
                or value is None
                or (isinstance(value, str) and not value.strip())
            ):
                return default
            return value
        if (name.endswith("json.loads") and len(args) == 1
                and not node.keywords and isinstance(args[0], str)):
            try:
                return json.loads(args[0])
            except Exception:
                return _UNKNOWN

        # REF-256 review (2026-10-27, reproduced): below this point every
        # supported call coerces arguments (str()/int()/dict lookups), and
        # coercing the _UNKNOWN sentinel mints WRONG concrete values —
        # p.strip(unknown_suffix) returned a concrete-looking path once
        # comprehension evaluation made such expressions reachable. A call
        # with ANY unresolved argument is unresolved, full stop. The
        # widget/parameter branches above are exempt by design: their
        # _UNKNOWN *default* argument is the documented lookup-miss signal.
        if any(a is _UNKNOWN for a in args) or any(
                v is _UNKNOWN for v in kwargs.values()):
            return _UNKNOWN
        receiver = self._eval(node.func.value) if isinstance(node.func, ast.Attribute) else _UNKNOWN
        if (short == "strip" and isinstance(receiver, str)
                and len(args) <= 1 and not kwargs
                and all(isinstance(a, str) for a in args)):
            # No str() coercion: strip(None) means whitespace-strip in
            # Python, but coercion stripped the characters of "None"
            # ("stone".strip(None) -> "st" — review 2026-10-28).
            try:
                return receiver.strip(*args)
            except Exception:
                return _UNKNOWN
        if short in ("startswith", "endswith") and isinstance(receiver, str):
            if not args or kwargs or any(arg is _UNKNOWN for arg in args):
                return _UNKNOWN
            try:
                method = getattr(receiver, short)
                return method(*args)
            except (TypeError, ValueError):
                return _UNKNOWN
        if (short == "get" and isinstance(receiver, Mapping)
                and len(args) in (1, 2) and not kwargs):
            return receiver.get(args[0], args[1] if len(args) > 1 else None)
        if (short == "strftime" and isinstance(receiver, (date, datetime))
                and len(args) == 1 and not kwargs
                and isinstance(args[0], str)):
            return receiver.strftime(args[0])
        if short == "replace" and receiver is not _UNKNOWN:
            # Exact argument shapes only: str.replace with a COUNT argument
            # ('aaa'.replace('a','b',1)) and datetime.replace with positional
            # args were silently mis-evaluated by dropping the extra argument
            # (reviewer-flagged residual class, 2026-10-28).
            try:
                if (isinstance(receiver, str) and len(args) == 2
                        and not kwargs
                        and all(isinstance(a, str) for a in args)):
                    return receiver.replace(args[0], args[1])
                if isinstance(receiver, (date, datetime)) and not args:
                    return receiver.replace(**{k: int(v) for k, v in kwargs.items()})
            except Exception:
                return _UNKNOWN
            return _UNKNOWN
        if short in ("str", "int", "float") and len(args) == 1 and not kwargs:
            # int(p, base=16) / int(p, 16) evaluated as int(p) — a WRONG
            # concrete value (the reviewer's accepted-residual example, now fixed):
            # only the exact single-argument cast is modelled.
            try:
                return {"str": str, "int": int, "float": float}[short](args[0])
            except Exception:
                return _UNKNOWN
        return _UNKNOWN

    def _eval_comprehension(self, node: ast.AST) -> Any:
        """Evaluate a list/generator comprehension over fully-resolved
        literal iterables.

        REF-256: concrete input paths built per-partition via a comprehension
        (``[f"s3://…/year={y}" for y in years]`` with ``years = [2025]``)
        were left DYNAMIC_REFERENCE/unresolved, so the CUR input never became
        a verified tracker entry and Phase 2 failed on unmigrated data (job
        800000000000006, both cost-alerting notebooks, cell 3).

        Fail-closed by construction — a wrong concrete path is worse than an
        honest gap, so ANY of the following returns _UNKNOWN (today's
        unresolved behaviour), never a partial or guessed list: an iterable
        that does not evaluate to a list/tuple, an ``if`` whose value is not
        a strict bool, an async generator, an unpacking-shape mismatch, an
        element that evaluates unresolved, or exceeding the element cap.
        Set/dict comprehensions stay unsupported (report ordering must be
        deterministic). Comprehension variables shadow outer bindings during
        evaluation and are restored afterwards — they never leak into env.
        """
        generators = getattr(node, "generators", [])
        if not generators:
            return _UNKNOWN
        # Review 2026-10-27 (executed false-PASS repro): inside a CLASS body
        # Python evaluates the comprehension body against the ENCLOSING
        # scope — class-level names are invisible to it — while this
        # evaluator's env holds the class-local bindings. Resolving there
        # certified a wrong-but-existing path end-to-end ('ready to
        # migrate'). Per the review verdict we decline what the scope model
        # cannot prove: class-body comprehensions stay unresolved (exactly
        # the base behaviour before REF-256).
        if self._class_scope is not None:
            return _UNKNOWN
        missing = object()
        target_names = [
            name.id
            for generator in generators
            for name in ast.walk(generator.target)
            if isinstance(name, ast.Name)
        ]
        saved = {name: self.env.get(name, missing) for name in target_names}

        class _Unresolved(Exception):
            pass

        def bind(target: ast.AST, value: Any) -> None:
            if isinstance(target, ast.Name):
                self.env[target.id] = value
                return
            if isinstance(target, (ast.Tuple, ast.List)):
                if not isinstance(value, (list, tuple)) or len(value) != len(target.elts):
                    raise _Unresolved
                for element, item in zip(target.elts, value):
                    bind(element, item)
                return
            raise _Unresolved

        results: List[Any] = []
        # One work budget per OUTERMOST comprehension, shared by every
        # nested comprehension it evaluates (iterables/elements): a fresh
        # counter per recursive call let nested forms multiply total work
        # while each counter stayed under the cap (review 2026-10-27).
        owns_budget = self._comprehension_work is None
        if owns_budget:
            self._comprehension_work = [0]
        work = self._comprehension_work

        def expand(index: int) -> None:
            if index == len(generators):
                if len(results) >= _COMPREHENSION_ELEMENT_CAP:
                    raise _Unresolved
                value = self._eval(node.elt)
                if value is _UNKNOWN:
                    raise _Unresolved
                results.append(value)
                return
            generator = generators[index]
            if getattr(generator, "is_async", 0):
                raise _Unresolved
            iterable = self._eval(generator.iter)
            if not isinstance(iterable, (list, tuple)):
                raise _Unresolved
            for item in iterable:
                work[0] += 1
                if work[0] > _COMPREHENSION_WORK_CAP:
                    raise _Unresolved
                bind(generator.target, item)
                keep = True
                for condition in generator.ifs:
                    verdict = self._eval(condition)
                    if not isinstance(verdict, bool):
                        raise _Unresolved
                    if not verdict:
                        keep = False
                        break
                if keep:
                    expand(index + 1)

        try:
            expand(0)
            return results
        except _Unresolved:
            return _UNKNOWN
        finally:
            if owns_budget:
                self._comprehension_work = None
            for name, value in saved.items():
                if value is missing:
                    self.env.pop(name, None)
                else:
                    self.env[name] = value

    @staticmethod
    def _sensitive_widget(name: str) -> bool:
        key = re.sub(r"[^a-z0-9]", "", name.lower())
        return key.endswith(("apikey", "password", "token", "clientsecret", "privatekey"))

    def _widget_state(self):
        return (dict(self.widget_defaults), set(self.removed_widgets),
                self.widgets_unknown, self.widgets_ambiguous)

    def _restore_widgets(self, state):
        defaults, removed, unknown, ambiguous = state
        self.widget_defaults, self.removed_widgets = dict(defaults), set(removed)
        self.widgets_unknown = unknown
        self.widgets_ambiguous = ambiguous

    def _invalidate_widgets(self, *, ambiguous=True):
        # An opaque mutation/control-flow path cannot select one definite
        # widget state. Later unconditional registration can establish a
        # particular widget again; other reads remain explicitly unresolved.
        self.widget_defaults.clear()
        self.removed_widgets.clear()
        self.widgets_unknown = True
        self.widgets_ambiguous = ambiguous

    @staticmethod
    def _widget_method(call):
        prefix, _, method = _call_name(call.func).rpartition(".")
        if (prefix in ("dbutils.widgets", "aidputils.widgets")
                and method in ("text", "dropdown", "combobox", "multiselect", "remove", "removeAll")):
            return method
        return None

    def _record_widget_statement(self, statement):
        """Only a direct statement-value call proves a widget mutation runs.

        The ordinary call scanner discovers dependencies; its ast.walk order
        is not Python evaluation order. Expressions owned by this statement
        therefore invalidate widget state unless the whole value is the one
        mutation call. Nested statements are handled by _walk_statement's
        existing control-flow traversal, not executed by this discovery pass.
        """
        def calls(node):
            for child in ast.iter_child_nodes(node):
                if isinstance(child, ast.stmt):
                    continue
                if isinstance(child, ast.Call) and self._widget_method(child):
                    yield child
                yield from calls(child)

        mutations = list(calls(statement))
        if not mutations:
            return
        value = (statement.value if isinstance(statement, (ast.Expr, ast.Assign, ast.AnnAssign))
                 else None)
        if len(mutations) == 1 and mutations[0] is value:
            self._record_widget_call(value)
        else:
            self._invalidate_widgets()

    def _record_widget_call(self, call):
        method = self._widget_method(call)
        if method == "removeAll":
            self._invalidate_widgets(ambiguous=False)
            return
        if method not in ("text", "dropdown", "combobox", "multiselect", "remove"):
            return
        key_node = call.args[0] if call.args else next(
            (kw.value for kw in call.keywords if kw.arg == "name"), None)
        key = self._eval(key_node)
        if method == "remove":
            if isinstance(key, str):
                self.widget_defaults.pop(key, None)
                self.removed_widgets.add(key)
            else:
                self._invalidate_widgets()
            return
        if not isinstance(key, str) or self._sensitive_widget(key):
            return
        default_node = call.args[1] if len(call.args) > 1 else next(
            (kw.value for kw in call.keywords if kw.arg == "defaultValue"), None)
        was_removed = key in self.removed_widgets
        self.removed_widgets.discard(key)
        # Re-registering a still-present widget retains its existing value;
        # remove/removeAll above clear that binding before recreation.
        default = self._eval(default_node)
        if self.widgets_ambiguous and not was_removed:
            # The widget may still exist with its pre-branch value, in which
            # case registration does not replace it. Keep that ambiguity.
            default = _UNKNOWN
        self.widget_defaults.setdefault(key, default)

    def _blank_input(self, call, value, argument):
        if not isinstance(value, str) or value.strip():
            return False
        error = {"type": "INVALID_REQUIRED_INPUT", "cell_number": self.cell_number,
                 "line_number": getattr(call, "lineno", 0),
                 "message": "Blank table/path consumed by Spark read",
                 "argument_expression": ast.unparse(argument)}
        if error not in self.analysis_errors:
            self.analysis_errors.append(error)
        return True

    def _render_filter(self, node: ast.AST) -> Optional[str]:
        value = self._eval(node)
        if isinstance(value, str):
            return value
        if isinstance(node, ast.BoolOp):
            op = " AND " if isinstance(node.op, ast.And) else " OR "
            rendered = [self._render_filter(v) for v in node.values]
            if all(rendered):
                return op.join(f"({x})" for x in rendered if x)
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.BitAnd, ast.BitOr)):
            left = self._render_filter(node.left)
            right = self._render_filter(node.right)
            if left and right:
                op = "AND" if isinstance(node.op, ast.BitAnd) else "OR"
                return f"({left}) {op} ({right})"
        if isinstance(node, ast.Compare) and len(node.ops) == len(node.comparators) == 1:
            column = _column_name(node.left)
            rhs_node = node.comparators[0]
            rhs = self._eval(rhs_node)
            operators = {ast.Eq: "=", ast.NotEq: "!=", ast.Gt: ">", ast.GtE: ">=", ast.Lt: "<", ast.LtE: "<=", ast.In: "IN", ast.NotIn: "NOT IN"}
            operator = operators.get(type(node.ops[0]))
            if column and operator and rhs is not _UNKNOWN:
                if operator in ("IN", "NOT IN") and isinstance(rhs, (list, tuple)):
                    return f"{column} {operator} ({', '.join(_literal(x) for x in rhs)})"
                return f"{column} {operator} {_literal(rhs)}"
            rhs_column = (
                _column_name(rhs_node)
                if (
                    isinstance(rhs_node, ast.Attribute)
                    or (
                        isinstance(rhs_node, ast.Call)
                        and _call_name(rhs_node.func).split(".")[-1]
                        in {"col", "column"}
                    )
                )
                else None
            )
            if column and operator and rhs_column:
                return f"{column} {operator} {rhs_column}"
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            short = node.func.attr
            column = _column_name(node.func.value)
            args = [self._eval(x) for x in node.args]
            if short == "between" and column and len(args) == 2 and all(x is not _UNKNOWN for x in args):
                return f"{column} BETWEEN {_literal(args[0])} AND {_literal(args[1])}"
            if short == "isin" and column and all(x is not _UNKNOWN for x in args):
                return f"{column} IN ({', '.join(_literal(x) for x in args)})"
            if short == "contains" and column and len(args) == 1 and args[0] is not _UNKNOWN:
                escaped = str(args[0]).replace("'", "''")
                return f"{column} LIKE '%{escaped}%'"
            if short in {"isNull", "isNotNull"} and column and not args:
                operator = "IS NULL" if short == "isNull" else "IS NOT NULL"
                return f"{column} {operator}"
        try:
            return ast.unparse(node)
        except Exception:
            return None

    def _filter_values_resolved(self, node: ast.AST) -> bool:
        """Return false when a filter depends on an unknown runtime value."""
        def explicit_column(value: ast.AST) -> bool:
            # A bare Name on the comparator side is normally a Python/runtime
            # variable (``col("day") == run_date``), not a Spark column.  Only
            # explicit ``col(...)``/``column(...)`` or ``df.column`` forms are
            # safe to classify as a column-to-column predicate.
            if isinstance(value, ast.Call):
                return (
                    _call_name(value.func).split(".")[-1] in {"col", "column"}
                    and _column_name(value) is not None
                )
            return isinstance(value, ast.Attribute) and _column_name(value) is not None

        if isinstance(node, ast.BoolOp):
            return all(self._filter_values_resolved(value) for value in node.values)
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.BitAnd, ast.BitOr)):
            return (
                self._filter_values_resolved(node.left)
                and self._filter_values_resolved(node.right)
            )
        if isinstance(node, ast.Compare):
            # Column-to-column predicates do not contain a runtime value to
            # resolve.  Treat the comparator as structural rather than
            # demoting an otherwise static partition/date scope.
            return all(
                explicit_column(value)
                or self._eval(value) is not _UNKNOWN
                for value in node.comparators
            )
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in {"between", "isin", "contains"}:
                return all(self._eval(value) is not _UNKNOWN for value in node.args)
            if node.func.attr in {"isNull", "isNotNull"}:
                return not node.args and _column_name(node.func.value) is not None
        # A literal SQL predicate string is already fully specified.  Other
        # expressions cannot be proven safe for scoped migration.
        return isinstance(self._eval(node), str)

    def _format_from_chain(self, node: ast.AST) -> Optional[str]:
        current = node
        while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
            if current.func.attr == "format" and current.args:
                value = self._eval(current.args[0])
                return str(value) if value is not _UNKNOWN else None
            current = current.func.value
        return None

    def _is_spark_read(self, call: ast.Call) -> bool:
        if not isinstance(call.func, ast.Attribute) or call.func.attr not in _READ_METHODS:
            return False
        try:
            text = ast.unparse(call.func.value).replace(" ", "")
        except Exception:
            text = _call_name(call.func.value)
        return text == "spark.read" or text.startswith("spark.read.")

    def _is_spark_write(self, call: ast.Call) -> bool:
        if not isinstance(call.func, ast.Attribute) or call.func.attr not in _WRITE_METHODS:
            return False
        try:
            text = ast.unparse(call.func.value).replace(" ", "")
        except Exception:
            text = _call_name(call.func.value)
        return ".write" in text

    def _record_python_file_read(self, call: ast.Call) -> None:
        """Record data/workspace files read outside Spark.

        Only built-in ``open`` in a readable mode and pandas ``read_*`` APIs
        are considered. A path with a known data/workspace prefix is recorded
        as is; a relative path is resolved against ``notebook_dir``. A
        relative path without a known working directory, or a path that is
        not statically resolvable, is recorded as an unresolved read so the
        report asks for review instead of silently omitting the input. Local
        absolute paths (``/tmp``) and URLs stay outside the data check.
        """
        name = _call_name(call.func)
        is_open = name in {"open", "builtins.open", "io.open"}
        is_glob = name in {"glob.glob", "glob.iglob"}
        if _python_file_read_api(call) is None:
            return
        if not call.args:
            self._record_unanalyzed_read(call, "keyword/path argument")
            return
        self._accounted_input_calls.add(call)

        if is_open:
            mode_node = call.args[1] if len(call.args) > 1 else None
            for keyword in call.keywords:
                if keyword.arg == "mode":
                    mode_node = keyword.value
                    break
            mode = self._eval(mode_node) if mode_node is not None else "r"
            if isinstance(mode, str) and not (
                "r" in mode or "+" in mode
            ):
                return

        argument = call.args[0]
        value = self._eval(argument)
        reference = {
            "object_type": "path",
            "usage": "read",
            "cell_number": self.cell_number,
            "line_number": getattr(call, "lineno", 0),
            "api": "open" if is_open else name,
            "argument_expression": ast.unparse(argument),
        }
        if is_glob and (len(call.args) != 1 or call.keywords):
            self._add_unresolved_file_read(
                reference, "glob options/root scope require manual review", required_gap=True)
            return
        if is_glob and isinstance(value, str) and "{" in value:
            self._add_unresolved_file_read(
                reference, "Python brace patterns differ from the shared Hadoop glob probe; manual review required",
                required_gap=True)
            return
        if isinstance(value, str) and value.startswith(_DATA_PATH_PREFIXES):
            self._add_file_read(reference, value)
        elif isinstance(value, str) and value.strip():
            if value.startswith(("/", "~")) or "://" in value:
                return
            if not self.notebook_dir:
                self._add_unresolved_file_read(
                    reference, "relative path; the run-time working directory "
                    "is known only for Git repository notebooks")
                return
            resolved = posixpath.normpath(posixpath.join(self.notebook_dir, value))
            if not resolved.startswith("/Workspace/"):
                self._add_unresolved_file_read(
                    reference, f"relative path resolves outside /Workspace "
                    f"from {self.notebook_dir}")
                return
            self._add_file_read(
                {**reference, "relative_path": value,
                 "base_directory": self.notebook_dir},
                resolved,
            )
        elif value is _UNKNOWN and not (
            isinstance(argument, ast.Call)
            and _call_name(argument.func) in _IN_MEMORY_BUFFERS
        ):
            self._add_unresolved_file_read(
                reference, "path is not statically resolvable")

    def _add_file_read(self, reference: Dict[str, Any], path: str) -> None:
        self.resolved_arguments.add(reference["argument_expression"])
        self.references.append({**reference, "object_name": path})

    def _add_unresolved_file_read(self, reference: Dict[str, Any], reason: str,
                                  *, required_gap: bool = False) -> None:
        self.unresolved_references.append(
            {**reference, "object_name": None, "reason": reason})
        if required_gap:
            self.not_analyzed.append({**reference, "kind": "unsupported_read", "reason": reason})

    @staticmethod
    def _mlflow_artifact_type(artifact_path: str, api: str) -> str:
        """Return an existing tracker object type for an MLflow artifact."""
        if api.endswith(("load_dict", "load_image", "load_text")):
            return "FILE"
        if api.endswith("load_model") or api.endswith("list_artifacts"):
            return "DIRECTORY"
        clean = str(artifact_path or "").split("?", 1)[0].rstrip("/")
        leaf = clean.rsplit("/", 1)[-1]
        return "FILE" if re.search(r"\.[A-Za-z0-9][A-Za-z0-9._-]*$", leaf) else "DIRECTORY"

    def _record_metadata_read(self, call: ast.Call) -> None:
        """Expose service-read boundaries without guessing helper semantics.

        A method spelling alone cannot establish receiver identity. Keep an
        unbound/parameter receiver explicitly uncertain. Known string arguments
        are candidates, not certified registry identities; no service is contacted.
        """
        name = _call_name(call.func)
        _, _, method = name.rpartition(".")
        if method not in _REGISTRY_METADATA_READ_METHODS:
            return
        self._accounted_input_calls.add(call)
        finding = {
            "kind": "mlflow_registry",
            "cell_number": self.cell_number,
            "line_number": getattr(call, "lineno", 0),
            "api": method,
            "reason": ("Potential registry metadata read requires manual verification of client identity, "
                       "model name, versions/tags/pointers and availability."),
        }
        if method in {
                "get_registered_model", "get_model_version"}:
            argument = call.args[0] if call.args else self._mlflow_keyword(call, "name")
            value = self._eval(argument)
            if isinstance(value, str) and value.strip():
                finding["candidate_name"] = value
        if finding not in self.not_analyzed:
            self.not_analyzed.append(finding)

    @staticmethod
    def _mlflow_keyword(call: ast.Call, name: str) -> Optional[ast.AST]:
        return next((kw.value for kw in call.keywords if kw.arg == name), None)

    @staticmethod
    def _mlflow_uri_has_unresolved_identifier(artifact_uri: str) -> bool:
        """Reject f-string placeholders such as ``runs:/None/model_files``."""
        for prefix in ("runs:/", "models:/"):
            if artifact_uri.startswith(prefix):
                identifier = artifact_uri[len(prefix):].lstrip("/").split("/", 1)[0]
                return identifier.strip().casefold() in {"", "none", "null", "unknown"}
        return False

    def _record_mlflow_artifact_read(self, call: ast.Call) -> None:
        """Capture MLflow run/model artifacts without downloading them.

        Supported forms include ``MlflowClient.download_artifacts``,
        ``mlflow.artifacts.download_artifacts``/``list_artifacts`` and
        MLflow flavor ``load_model`` calls. Runtime widget parameters are
        resolved through the same symbol table used for Spark/file paths.
        """
        name = _call_name(call.func)
        short = name.split(".")[-1]
        receiver = name.rsplit(".", 1)[0] if "." in name else ""
        is_client_call = (
            short in _MLFLOW_CLIENT_ARTIFACT_METHODS
            and receiver in self.mlflow_clients
        )
        is_artifacts_call = (
            short in _MLFLOW_ARTIFACT_FUNCTIONS
            and receiver == "mlflow.artifacts"
        )
        is_model_load = (
            short == "load_model"
            and receiver.startswith("mlflow.")
        )
        if not (is_client_call or is_artifacts_call or is_model_load):
            return

        run_node: Optional[ast.AST] = None
        path_node: Optional[ast.AST] = None
        uri_node: Optional[ast.AST] = None
        if is_client_call:
            run_node = self._mlflow_keyword(call, "run_id")
            path_node = self._mlflow_keyword(call, "path") or self._mlflow_keyword(
                call, "artifact_path"
            )
            if run_node is None and call.args:
                run_node = call.args[0]
            if path_node is None and len(call.args) > 1:
                path_node = call.args[1]
        else:
            uri_node = (
                self._mlflow_keyword(call, "artifact_uri")
                or self._mlflow_keyword(call, "model_uri")
            )
            run_node = self._mlflow_keyword(call, "run_id")
            path_node = self._mlflow_keyword(call, "artifact_path")
            if uri_node is None and call.args:
                uri_node = call.args[0]

        run_id = self._eval(run_node) if run_node is not None else _UNKNOWN
        artifact_path = self._eval(path_node) if path_node is not None else _UNKNOWN
        artifact_uri = self._eval(uri_node) if uri_node is not None else _UNKNOWN

        if (
            artifact_uri is _UNKNOWN
            and isinstance(run_id, str) and run_id.strip()
            and isinstance(artifact_path, str)
        ):
            suffix = artifact_path.strip().lstrip("/")
            artifact_uri = f"runs:/{run_id.strip()}" + (f"/{suffix}" if suffix else "")

        expression_node = uri_node or path_node or run_node
        expression = ast.unparse(expression_node) if expression_node is not None else name
        base = {
            "usage": "read",
            "cell_number": self.cell_number,
            "line_number": getattr(call, "lineno", 0),
            "api": name,
            "argument_expression": expression,
        }
        if not isinstance(artifact_uri, str) or not artifact_uri.strip():
            self.unresolved_mlflow_artifacts.append({
                **base,
                "run_id_expression": ast.unparse(run_node) if run_node is not None else None,
                "artifact_path_expression": ast.unparse(path_node) if path_node is not None else None,
            })
            return

        artifact_uri = artifact_uri.strip()
        if self._mlflow_uri_has_unresolved_identifier(artifact_uri):
            self.unresolved_mlflow_artifacts.append({
                **base,
                "artifact_uri": artifact_uri,
                "run_id_expression": ast.unparse(run_node) if run_node is not None else None,
                "artifact_path_expression": ast.unparse(path_node) if path_node is not None else None,
            })
            return
        if not artifact_uri.startswith(("runs:/", "models:/")):
            # Explicit artifact URIs may point directly at an object store.
            # Keep them visible as MLflow dependencies without reclassifying
            # ordinary local destination paths.
            if "://" not in artifact_uri and not artifact_uri.startswith("file:"):
                self.unresolved_mlflow_artifacts.append({
                    **base, "artifact_uri": artifact_uri,
                })
                return

        parsed_run_id = run_id if isinstance(run_id, str) else None
        parsed_path = artifact_path if isinstance(artifact_path, str) else ""
        if artifact_uri.startswith("runs:/"):
            remainder = artifact_uri[len("runs:/"):].lstrip("/")
            parts = remainder.split("/", 1)
            parsed_run_id = parts[0] or parsed_run_id
            parsed_path = parts[1] if len(parts) > 1 else parsed_path
        self.mlflow_artifacts.append({
            **base,
            "artifact_uri": artifact_uri,
            "run_id": parsed_run_id,
            "artifact_path": parsed_path,
            "object_type": self._mlflow_artifact_type(parsed_path or artifact_uri, name),
        })

    def _new_read(
        self, call: ast.Call, *, record: bool = True,
    ) -> List[Dict[str, Any]]:
        method = call.func.attr if isinstance(call.func, ast.Attribute) else ""
        if not call.args:
            # Option-configured loads have no positional path. analyze_cell
            # inventories these separately, including reader aliases, so each
            # one remains an explicit unresolved finding.
            return []
        # REF-256: reads over MULTIPLE concrete paths — a star-expanded
        # resolved list (spark.read.parquet(*paths)) or a multi-path call
        # (parquet(a, b); the legacy single-arg evaluation silently dropped
        # every path after the first). All-or-nothing: if ANY element is
        # unresolved the whole call stays one unresolved reference (the
        # legacy shape) — a partial path set would overstate tracker
        # coverage. Tables are never multi-path; `table` keeps the legacy
        # single-argument flow below.
        if method in _VARIADIC_PATH_READ_METHODS and (
            len(call.args) > 1 or isinstance(call.args[0], ast.Starred)
        ):
            return self._new_multi_path_read(call, method, record=record)
        value = self._eval(call.args[0])
        if self._blank_input(call, value, call.args[0]):
            value = _UNKNOWN
        object_type = (
            "table"
            if method == "table" and not _looks_like_data_path(value)
            else "path"
        )
        argument_expression = ast.unparse(call.args[0])
        ref: Dict[str, Any] = {"object_type": object_type,
                              "object_name": value if isinstance(value, str) and value else None,
                              "usage": "read", "cell_number": self.cell_number,
                              "line_number": getattr(call, "lineno", 0),
                              "api": method, "argument_expression": argument_expression}
        source_format = self._format_from_chain(call)
        if not source_format and method not in ("load", "table"):
            source_format = method
        if source_format:
            ref["source_format"] = source_format
        if ref["object_name"]:
            self.resolved_arguments.add(argument_expression)
            if record:
                self.references.append(ref)
        else:
            if record:
                self.unresolved_references.append(ref)
        return [ref]

    def _new_multi_path_read(
        self, call: ast.Call, method: str, *, record: bool = True,
    ) -> List[Dict[str, Any]]:
        """Resolve a path read whose arguments expand to several paths
        (REF-256). Emits one reference per concrete path only when EVERY
        positional argument — including every element of a star-expanded
        sequence — evaluates to a non-empty string; otherwise one unresolved
        reference for the whole call (the legacy shape)."""
        values: List[str] = []
        resolved = True
        for argument in call.args:
            if isinstance(argument, ast.Starred):
                # An inline generator expression is consumed HERE, at this
                # exact statement, so evaluating it now matches Python's
                # timing; a stored generator (reached via Name -> env) is
                # not evaluated and stays unresolved (review 2026-10-27).
                if isinstance(argument.value, ast.GeneratorExp):
                    sequence = self._eval_comprehension(argument.value)
                else:
                    sequence = self._eval(argument.value)
                # An EMPTY resolved expansion is valid Python contributing
                # zero arguments — parquet(p, *[]) == parquet(p). Requiring
                # truthiness here unresolved the whole call and LOST the
                # known concrete read for p (review 2026-10-27, executed
                # base/head regression).
                blanks = (isinstance(sequence, (list, tuple)) and any(
                    self._blank_input(call, value, argument) for value in sequence))
                if (isinstance(sequence, (list, tuple)) and not blanks
                        and all(isinstance(v, str) and v for v in sequence)):
                    values.extend(sequence)
                else:
                    resolved = False
                    break
            else:
                value = self._eval(argument)
                if self._blank_input(call, value, argument):
                    resolved = False
                    break
                if isinstance(value, str) and value:
                    values.append(value)
                else:
                    resolved = False
                    break
        argument_expression = ", ".join(ast.unparse(a) for a in call.args)
        source_format = self._format_from_chain(call)
        if not source_format and method not in ("load", "table"):
            source_format = method
        base: Dict[str, Any] = {"object_type": "path", "usage": "read",
                                "cell_number": self.cell_number,
                                "line_number": getattr(call, "lineno", 0),
                                "api": method,
                                "argument_expression": argument_expression}
        if source_format:
            base["source_format"] = source_format
        if not resolved or not values:
            # not resolved: some element is unprovable. resolved-but-empty:
            # every argument expanded to nothing (parquet(*empty)) — there is
            # no input to certify, so the call stays an explicit gap.
            ref = {**base, "object_name": None}
            if record:
                self.unresolved_references.append(ref)
            return [ref]
        self.resolved_arguments.add(argument_expression)
        refs = [{**base, "object_name": path} for path in values]
        if record:
            self.references.extend(refs)
        return refs

    def _sql_refs(self, call: ast.Call) -> List[Dict[str, Any]]:
        if _call_name(call.func) != "spark.sql" or not call.args:
            return []
        sql = self._eval(call.args[0])
        if not isinstance(sql, str):
            return []
        writes = bool(re.search(r"\b(?:CREATE\s+TABLE|INSERT\s+(?:INTO|OVERWRITE))\b", sql, re.I))
        refs: List[Dict[str, Any]] = []
        where = re.search(r"\bWHERE\b(?P<where>.*?)(?:\bGROUP\s+BY\b|\bORDER\s+BY\b|\bLIMIT\b|$)", sql, re.I | re.S)
        filters = [" ".join(where.group("where").split())] if where else []
        for table in _SQL_KW_TABLE_RE.findall(sql):
            usage = "write_create" if writes and re.search(r"\b(?:TABLE|INTO|OVERWRITE)\s+" + re.escape(table), sql, re.I) else "read"
            ref = {"object_type": "table", "object_name": table, "usage": usage,
                   "cell_number": self.cell_number,
                   "line_number": getattr(call, "lineno", 0), "api": "sql"}
            if usage == "read":
                if filters:
                    ref["filters"] = filters
                    ref.update(_metadata_from_filters(filters))
                    ref["migration_filters"] = [
                        item for item in filters if _metadata_from_filters([item])
                    ]
                self.references.append(ref)
                refs.append(ref)
            else:
                self.writes.append(ref)
        return refs

    def _lineage_from_expr(
        self, node: ast.AST, *, record: bool = True,
    ) -> List[Dict[str, Any]]:
        if isinstance(node, ast.Name):
            return self.lineage.get(node.id, [])
        # ``df.write`` is an attribute between the source DataFrame and write
        # builder calls such as ``df.write.mode(...).saveAsTable(...)``.
        # Preserve the source lineage so a write consumer is correctly treated
        # as requiring the complete input DataFrame.
        if isinstance(node, ast.Attribute) and node.attr == "write":
            return self._lineage_from_expr(node.value, record=False)
        if isinstance(node, (ast.BinOp, ast.Compare, ast.BoolOp, ast.UnaryOp)):
            # Arithmetic/comparison contexts (``n = df.count() + 1``,
            # ``if df.count() > 0``) carry terminal consumers, not lineage.
            for child in ast.iter_child_nodes(node):
                self._lineage_from_expr(child, record=record)
            return []
        if not isinstance(node, ast.Call):
            return []
        # Inspect supported eval strings as syntax; never execute them.
        if _call_name(node.func) == "eval":
            self._visited_eval_calls.add(node)
            expression = self._eval(node.args[0]) if node.args else _UNKNOWN
            parsed = None
            reason = "eval input could not be resolved from runtime parameters"
            default_namespace = len(node.args) == 1 and not node.keywords
            if not default_namespace:
                reason = "explicit eval namespaces or argument forms are unsupported"
            elif isinstance(expression, str) and len(expression) > 10000:
                reason = "eval input exceeds the 10000-character static analysis limit"
            elif isinstance(expression, str):
                try:
                    parsed = ast.parse(expression.strip(), mode="eval").body
                except (SyntaxError, ValueError, RecursionError):
                    reason = "eval input is not a supported Python expression"
                else:
                    reason = "eval expression is outside the supported table-read subset"
            # Start with the direct table-read pattern used by this workflow.
            if (isinstance(parsed, ast.Call)
                    and _call_name(parsed.func) in ("spark.read.table", "spark.table")
                    and len(parsed.args) == 1 and not parsed.keywords):
                ast.copy_location(parsed, node)
                return self._lineage_from_expr(parsed, record=record)
            # Gaps must survive lineage-only traversal as well.
            self._record_eval_gap(node, reason)
            return []
        if self._is_spark_read(node):
            return self._new_read(node, record=record)
        sql_refs = self._sql_refs(node)
        if sql_refs:
            return sql_refs
        if _call_name(node.func) == "spark.table" and node.args:
            fake = ast.Call(func=ast.Attribute(value=ast.Attribute(value=ast.Name(id="spark"), attr="read"), attr="table"), args=node.args, keywords=node.keywords)
            ast.copy_location(fake, node)
            return self._new_read(fake, record=record)
        if not isinstance(node.func, ast.Attribute):
            # A plain-name wrapper call (``print(df.count())``, ``display(df)``,
            # ``len(df.toPandas())``) transforms no lineage, but its arguments
            # can terminally consume a lineage-bearing frame — ``display(df)``
            # reads the complete object just as ``df.count()`` does.
            for arg in list(node.args) + [kw.value for kw in node.keywords]:
                inner = self._lineage_from_expr(arg, record=record)
                if record and inner and isinstance(arg, ast.Name):
                    self._record_terminal_consumption(inner)
            return []
        method = node.func.attr
        # A source call inside a chain is only lineage until the outermost
        # operation is known. Recording it here fabricated a full-table read
        # for every directly chained filter.
        base = self._lineage_from_expr(node.func.value, record=False)
        if method in _FILTER_METHODS and node.args and base:
            expression = self._render_filter(node.args[0])
            filter_is_resolved = self._filter_values_resolved(node.args[0])
            scoped: List[Dict[str, Any]] = []
            for original in base:
                clone = dict(original)
                clone["filters"] = list(original.get("filters", []))
                if expression and expression not in clone["filters"]:
                    clone["filters"].append(expression)
                clone.update(_metadata_from_filters(clone["filters"]))
                clone["migration_filters"] = [
                    item for item in clone["filters"]
                    if _metadata_from_filters([item])
                ]
                clone["filter_resolution"] = {
                    "status": "resolved" if filter_is_resolved else "unresolved",
                    "runtime_anchor": (
                        self.anchor.date().isoformat()
                        if self.anchor is not None else None
                    ),
                    "runtime_parameter": self.anchor_parameter,
                }
                if record:
                    if clone.get("object_name") and filter_is_resolved:
                        self.references.append(clone)
                    else:
                        self.unresolved_references.append(clone)
                scoped.append(clone)
            return scoped
        if method in _LINEAGE_METHODS or method == "join":
            joined = list(base)
            if method == "join" and node.args:
                joined.extend(self._lineage_from_expr(node.args[0], record=False))
            if record:
                for ref in joined:
                    resolution = (ref.get("filter_resolution") or {}).get("status")
                    if ref.get("object_name") and resolution != "unresolved":
                        self.references.append(ref)
                    else:
                        self.unresolved_references.append(ref)
            return joined
        if method in _METADATA_ONLY_METHODS:
            return base
        if record:
            self._record_terminal_consumption(base)
        return base

    def _record_eval_gap(self, node: ast.Call, reason: str) -> None:
        expression = ast.unparse(node)
        finding = {
            "kind": "dynamic_eval",
            "cell_number": self.cell_number,
            "line_number": getattr(node, "lineno", None),
            "column_number": getattr(node, "col_offset", None),
            "expression": expression if len(expression) <= 1000 else expression[:997] + "...",
            "reason": reason,
        }
        if finding not in self.not_analyzed:
            self.not_analyzed.append(finding)

    def _record_terminal_consumption(
        self, base: List[Dict[str, Any]],
    ) -> None:
        # Terminal actions (count, collect, show, write builders, etc.)
        # consume their complete incoming lineage. Mark an unfiltered
        # lineage reference so result() does not discard it merely because
        # another branch applies a narrower filter to the same source.
        for ref in base:
            clone = dict(ref)
            if not clone.get("filters"):
                clone["_full_consumer"] = True
            resolution = (clone.get("filter_resolution") or {}).get("status")
            if clone.get("object_name") and resolution != "unresolved":
                self.references.append(clone)
            else:
                self.unresolved_references.append(clone)

    def _record_write(self, call: ast.Call) -> None:
        name = _call_name(call.func)
        short = name.split(".")[-1]
        usage = "write_insert" if short == "insertInto" else "write_create"
        object_type = "table" if short in ("saveAsTable", "insertInto") else "path"
        if object_type == "path":
            usage = "write"
        value = self._eval(call.args[0]) if call.args else _UNKNOWN
        write_mode = None
        if short.lower() == "createtable":
            object_type, usage = "table", "write_create"
            strings = [self._eval(x) for x in call.args]
            strings = [x for x in strings if isinstance(x, str) and x]
            value = next((x for x in strings if re.fullmatch(r"[A-Za-z_]\w*\.[A-Za-z_]\w*", x)), _UNKNOWN)
            if value is _UNKNOWN and len(strings) >= 2:
                table, schema = strings[0], strings[1]
                if re.fullmatch(r"[A-Za-z_]\w*", table) and re.fullmatch(r"[A-Za-z_]\w*", schema):
                    value = f"{schema}.{table}"
            mode_value = self._eval(call.args[3]) if len(call.args) > 3 else _UNKNOWN
            for keyword in call.keywords:
                if keyword.arg in {"write_mode", "mode"}:
                    mode_value = self._eval(keyword.value)
            if isinstance(mode_value, str) and mode_value.strip():
                write_mode = mode_value.strip().lower()
        elif self._is_spark_write(call):
            for node in ast.walk(call.func.value):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "mode" and node.args):
                    mode_value = self._eval(node.args[0])
                    if isinstance(mode_value, str) and mode_value.strip():
                        write_mode = mode_value.strip().lower()
                        break
        if isinstance(value, str) and value:
            reference = {"object_type": object_type, "object_name": value,
                         "usage": usage, "cell_number": self.cell_number,
                         "line_number": getattr(call, "lineno", 0), "api": short}
            if write_mode:
                reference["write_mode"] = write_mode
            self.writes.append(reference)

    def _record_orchestration_call(self, call: ast.Call) -> None:
        """Capture child notebook parameters and expose opaque child jobs."""
        name = _call_name(call.func)
        if name.endswith(".notebook.run"):
            path = self._eval(call.args[0]) if call.args else _UNKNOWN
            params_node = call.args[2] if len(call.args) > 2 else None
            for keyword in call.keywords:
                if keyword.arg in {"arguments", "params", "notebook_params"}:
                    params_node = keyword.value
            parameters = self._eval(params_node) if params_node else {}
            base = {
                "kind": "notebook_run",
                "cell_number": self.cell_number,
                "line_number": getattr(call, "lineno", 0),
                "expression": ast.unparse(call),
            }
            if isinstance(path, str) and path.strip() and isinstance(parameters, dict):
                self.notebook_runs.append({
                    **base,
                    "path": path.strip(),
                    "parameters": parameters,
                })
            else:
                self.not_analyzed.append({
                    **base,
                    "reason": (
                        "dynamic notebook path"
                        if not isinstance(path, str) or not path.strip()
                        else "dynamic notebook parameters"
                    ),
                })
            return

        # A child workflow runs outside this notebook's data-flow scope.  Keep
        # that gap visible instead of implying its notebooks were analyzed.
        if name.split(".")[-1] in {
            "run_now", "runNow", "trigger_job", "triggerJob", "job_call",
            "grid_search_call", "trigger_run", "run_job_and_wait",
        }:
            self.not_analyzed.append({
                "kind": "child_job",
                "cell_number": self.cell_number,
                "line_number": getattr(call, "lineno", 0),
                "expression": ast.unparse(call),
                "reason": "triggered child workflow is not in this notebook manifest",
            })
            return
        if name in {"requests.post", "request.post", "session.post"} and call.args:
            endpoint = self._eval(call.args[0])
            if isinstance(endpoint, str) and any(
                marker in endpoint for marker in ("/jobs/run-now", "/jobRuns")
            ):
                self.not_analyzed.append({
                    "kind": "child_job",
                    "cell_number": self.cell_number,
                    "line_number": getattr(call, "lineno", 0),
                    "expression": ast.unparse(call),
                    "reason": "triggered child workflow is not in this notebook manifest",
                })

    def _bind(self, name: str, value: Any) -> None:
        if value is _UNKNOWN:
            self.env.pop(name, None)
        else:
            self.env[name] = value
        if self._scope_depth == 0:
            self.bound_names.add(name)

    def _bind_unknown_targets(self, target: Optional[ast.AST]) -> None:
        if target is None:
            return
        for node in ast.walk(target):
            if isinstance(node, ast.Name):
                self._bind(node.id, _UNKNOWN)

    def _bind_import(self, statement: ast.ImportFrom) -> None:
        namespace = None
        if statement.level == 0 and statement.module:
            namespace = self.import_namespaces.get(statement.module)
        for alias in statement.names:
            if alias.name == "*":
                # A star import binds exactly the module's public names. For an
                # installed package or a module that could not be analysed
                # those are unknown; the latter is reported by the resolver.
                if namespace is not None:
                    for name in namespace.star_names():
                        self._bind(name, namespace.values.get(name, _UNKNOWN))
                continue
            value = (namespace.values.get(alias.name, _UNKNOWN)
                     if namespace is not None else _UNKNOWN)
            self._bind(alias.asname or alias.name, value)

    def _forget(self, statements: List[ast.stmt]) -> None:
        """Drop every binding ``statements`` may make: code entered after
        some of them ran (a handler, a later loop iteration) cannot know
        which of their values are in place."""
        names, star_modules = _scope_bindings(statements)
        if any(self._widget_method(node) for statement in statements
               for node in ast.walk(statement) if isinstance(node, ast.Call)):
            self._invalidate_widgets()
        for name in names:
            self.env.pop(name, None)
        for module in star_modules:
            namespace = self.import_namespaces.get(module) if module else None
            if namespace is None:
                self.env.clear()
                return
            for name in namespace.star_names():
                self.env.pop(name, None)

    def _record_calls(self, node: ast.AST, record: Callable[[ast.Call], Any]) -> None:
        """Apply ``record`` to every call in ``node``, each resolving names in
        the scope it runs in: a call inside a lambda or comprehension cannot
        see the outer bindings of the names that scope binds, nor names bound
        in an enclosing class body. A call that may run later (lambda body,
        generator expression) does not trust a global bound more than once,
        as for a function body."""
        scopes = _expression_scopes(node)
        for call in (n for n in ast.walk(node) if isinstance(n, ast.Call)):
            scope = scopes.get(id(call))
            if scope is None:
                record(call)
                continue
            hidden, deferred = scope
            if deferred:
                hidden = hidden | self.rebound_globals
            saved_env = self.env
            visible = self._class_scope[0] if self._class_scope is not None else self.env
            self.env = {name: value for name, value in visible.items() if name not in hidden}
            widget_start = self._widget_state()
            try:
                record(call)
            finally:
                if self._widget_state() != widget_start:
                    self._invalidate_widgets()
                self.env = saved_env

    def _walk_block(self, statements: List[ast.stmt]) -> None:
        for child in statements:
            self._walk_statement(child)

    def _record_unsupported_reads(self, statement: ast.stmt) -> None:
        """Retain read boundaries where statement bindings are not analyzed.

        Do not infer identities from the surrounding environment: a match case
        or another unsupported scope can bind the same names differently.
        """
        for call in (node for node in ast.walk(statement) if isinstance(node, ast.Call)):
            self._record_unanalyzed_read(call, type(statement).__name__)

    def _record_unanalyzed_read(self, call: ast.Call, position: str) -> None:
        if call in self._accounted_input_calls or call in self._inactive_input_calls:
            return
        method = _call_name(call.func).rpartition(".")[2]
        api = method if method in _REGISTRY_METADATA_READ_METHODS else _python_file_read_api(call)
        if api is None:
            return
        self._accounted_input_calls.add(call)
        if api in {"open", "builtins.open", "io.open"}:
            mode = next((kw.value for kw in call.keywords if kw.arg == "mode"),
                        call.args[1] if len(call.args) > 1 else None)
            if (isinstance(mode, ast.Constant) and isinstance(mode.value, str)
                    and "r" not in mode.value and "+" not in mode.value):
                return
        argument = call.args[0] if call.args else None
        if (_file_read_api_name(api) is not None and isinstance(argument, ast.Call)
                and _call_name(argument.func) in _IN_MEMORY_BUFFERS):
            return
        self.not_analyzed.append({
            "kind": "unsupported_read", "cell_number": self.cell_number,
            "line_number": getattr(call, "lineno", 0), "api": api,
            "statement_type": position,
            "reason": "Read argument or scope is not analyzed; input identity requires manual review.",
        })

    def _walk_alternatives(self, runs: List[Callable[[], None]]) -> None:
        """Walk control-flow paths of which any may run; keep only bindings
        that are identical on every path. A name bound differently (or only
        on some paths) becomes unknown, so a later read reports it unresolved
        instead of silently taking one path's value."""
        start = dict(self.env)
        widget_start = self._widget_state()
        outcomes, widget_outcomes = [], []
        for run in runs:
            self.env = dict(start)
            self._restore_widgets(widget_start)
            run()
            outcomes.append(self.env)
            widget_outcomes.append(self._widget_state())
        if all(state == widget_outcomes[0] for state in widget_outcomes):
            self._restore_widgets(widget_outcomes[0])
        else:
            self._invalidate_widgets()
        first, rest = outcomes[0], outcomes[1:]
        self.env = {
            key: value for key, value in first.items()
            if all(key in other and _same_binding(other[key], value) for other in rest)
        }

    def _walk_statement(self, statement: ast.stmt) -> None:
        self._record_widget_statement(statement)
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            # A def/class body has its own scope: its bindings never become
            # notebook (or module) globals, except names it declares global,
            # whose value is then unknown (the body may or may not run).
            saved_lineage, saved_env = dict(self.lineage), dict(self.env)
            saved_class_scope = self._class_scope
            declared_global = {
                name for node in ast.walk(statement)
                if isinstance(node, ast.Global) for name in node.names
            }
            if self._class_scope is not None:
                # Defined in a class body: it sees the scope around the
                # class, not the class's own names.
                self.env, self.lineage = (dict(self._class_scope[0]),
                                          dict(self._class_scope[1]))
            if isinstance(statement, ast.ClassDef):
                # A ``global``/``nonlocal`` name assigned in the class body
                # changes the outer binding its nested scopes read.
                outer_rebinds = declared_global | {
                    name for node in ast.walk(statement)
                    if isinstance(node, ast.Nonlocal) for name in node.names
                }
                self._class_scope = (
                    {k: v for k, v in self.env.items() if k not in outer_rebinds},
                    {k: v for k, v in self.lineage.items() if k not in outer_rebinds},
                )
            else:
                self._class_scope = None
                # A function body runs when called: its parameters and every
                # name it binds are local (unbound until assigned), and a
                # global bound more than once may hold any of its values.
                arguments = statement.args
                parameters = [
                    arg.arg for arg in (arguments.posonlyargs + arguments.args
                                        + arguments.kwonlyargs)
                ] + [arg.arg for arg in (arguments.vararg, arguments.kwarg) if arg]
                local_names, _ = _scope_bindings(statement.body)
                for name in set(parameters) | set(local_names) | self.rebound_globals:
                    self.env.pop(name, None)
            widget_start = self._widget_state()
            self._scope_depth += 1
            try:
                self._walk_block(statement.body)
            finally:
                self._scope_depth -= 1
                self._class_scope = saved_class_scope
            if self._widget_state() != widget_start:
                self._invalidate_widgets()
            self.lineage, self.env = saved_lineage, saved_env
            for name in sorted(declared_global):
                self._bind(name, _UNKNOWN)
            self._bind(statement.name, _UNKNOWN)
            return
        if isinstance(statement, ast.ImportFrom):
            self._bind_import(statement)
            return
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                self._bind(alias.asname or alias.name.split(".")[0], _UNKNOWN)
            return
        if isinstance(statement, ast.Delete):
            self._record_unsupported_reads(statement)
            for target in statement.targets:
                self._bind_unknown_targets(target)
            return
        if isinstance(statement, ast.AugAssign):
            self._record_unsupported_reads(statement)
            self._bind_unknown_targets(statement.target)
            return
        if isinstance(statement, ast.With):
            for item in statement.items:
                self._record_calls(item.context_expr, self._record_python_file_read)
                self._record_calls(item.context_expr, self._record_metadata_read)
                self._bind_unknown_targets(item.optional_vars)
            self._walk_block(statement.body)
            return
        if isinstance(statement, ast.If):
            # Honour guards when their result is known from workflow/runtime
            # parameters. Walking both branches used to apply assignments from
            # a branch that would never execute. For example, an already-AIDP
            # path containing ``/dbfs/`` was corrupted by this Databricks-only
            # normalization guard:
            #
            #   if path.startswith("/dbfs/"):
            #       path = path.replace("/dbfs/", "dbfs:/")
            #
            # Unknown conditions retain the conservative historical behaviour
            # and scan both branches.
            # The test expression itself can terminally consume a frame
            # (``if df.count() > 0:``) — record that before branch selection.
            self._lineage_from_expr(statement.test, record=True)
            self._record_calls(statement.test, self._record_metadata_read)
            condition = self._eval(statement.test)
            if isinstance(condition, bool):
                inactive = statement.orelse if condition else statement.body
                self._inactive_input_calls.update(
                    node for child in inactive for node in ast.walk(child)
                    if isinstance(node, ast.Call))
                self._walk_block(statement.body if condition else statement.orelse)
            else:
                self._walk_alternatives([
                    lambda: self._walk_block(statement.body),
                    lambda: self._walk_block(statement.orelse),
                ])
            return
        if isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
            if isinstance(statement, ast.While):
                self._lineage_from_expr(statement.test, record=True)
                self._record_calls(statement.test, self._record_metadata_read)
            else:
                self._record_calls(statement.iter, self._record_metadata_read)

            def run_loop() -> None:
                # A later iteration starts from what earlier ones bound.
                self._forget(statement.body)
                self._bind_unknown_targets(getattr(statement, "target", None))
                self._walk_block(statement.body)

            # The body runs zero or more times; the else block after it.
            self._walk_alternatives([lambda: None, run_loop])
            self._walk_block(statement.orelse)
            return
        if isinstance(statement, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            def run_handler(handler: ast.ExceptHandler) -> Callable[[], None]:
                def run() -> None:
                    # The exception may come from any statement of the
                    # body, after any of its bindings.
                    self._forget(statement.body)
                    if handler.name:
                        self._bind(handler.name, _UNKNOWN)
                    self._walk_block(handler.body)
                return run

            self._walk_alternatives(
                [lambda: (self._walk_block(statement.body),
                          self._walk_block(statement.orelse))]
                + [run_handler(handler) for handler in statement.handlers]
            )
            self._walk_block(statement.finalbody)
            return
        if isinstance(statement, ast.Return):
            # A returned pandas read is still an input read. Preserve this
            # function's scope; do not fold it in the final notebook environment.
            if statement.value is not None:
                self._record_calls(statement.value, self._record_python_file_read)
                self._record_calls(statement.value, self._record_metadata_read)
            return
        value_node = statement.value if isinstance(statement, (ast.Assign, ast.AnnAssign, ast.Expr)) else None
        if value_node is None:
            self._record_unsupported_reads(statement)
            # Unhandled compound statements do not enter the traversal above.
            # Their nested mutations cannot leave an earlier state certified.
            if any(isinstance(node, ast.Call) and self._widget_method(node)
                   for node in ast.walk(statement)):
                self._invalidate_widgets()
            return
        def record(call: ast.Call) -> None:
            if _call_name(call.func) in ("dbutils.secrets.get", "aidputils.secrets.get"):
                self.not_analyzed.append({
                    "kind": "credential_availability", "cell_number": self.cell_number,
                    "line_number": getattr(call, "lineno", 0),
                    "reason": "Credential availability requires validation in the target job context; static data checks cannot verify it",
                })
            short = _call_name(call.func).split(".")[-1]
            if self._is_spark_write(call) or short.lower() == "createtable":
                self._record_write(call)
            self._record_python_file_read(call)
            self._record_mlflow_artifact_read(call)
            self._record_metadata_read(call)
            self._record_orchestration_call(call)

        self._record_calls(value_node, record)
        lineage = self._lineage_from_expr(value_node)
        if (isinstance(statement, ast.Expr) and isinstance(value_node, ast.Name)
                and lineage):
            # A bare DataFrame expression displays the frame in a notebook —
            # a terminal consumer of the complete object.
            self._record_terminal_consumption(lineage)
        value = self._eval(value_node)
        targets = statement.targets if isinstance(statement, ast.Assign) else ([statement.target] if isinstance(statement, ast.AnnAssign) else [])
        for target in targets:
            if isinstance(target, ast.Name):
                if (
                    isinstance(value_node, ast.Call)
                    and _call_name(value_node.func).split(".")[-1] == "MlflowClient"
                ):
                    self.mlflow_clients.add(target.id)
                self._bind(target.id, value)
                if lineage:
                    self.lineage[target.id] = lineage
            else:
                # Unpacking / attribute / subscript targets: any name bound
                # here gets a value this analyser does not track.
                self._bind_unknown_targets(
                    target if isinstance(target, (ast.Tuple, ast.List, ast.Starred)) else None)

    def analyze_cell(self, source: str, cell_number: int) -> None:
        self.cell_number = cell_number
        self._visited_eval_calls.clear()
        self._accounted_input_calls.clear()
        self._inactive_input_calls.clear()
        lines = source.splitlines()
        line_offset = 0
        if lines and lines[0].lstrip().startswith("%python"):
            source = "\n".join(lines[1:])
            line_offset = 1
        try:
            tree = ast.parse(source)
        except (SyntaxError, ValueError) as exc:
            self.analysis_errors.append({
                "type": "SYNTAX_ERROR",
                "cell_number": cell_number,
                "line_number": getattr(exc, "lineno", None),
                "message": str(exc),
            })
            return
        for call in ast.walk(tree):
            if (isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "load" and not call.args):
                self.unresolved_loads.append({
                    "site_id": f"{cell_number}:{call.lineno + line_offset}:{call.col_offset}",
                    "cell_number": cell_number,
                    "line_number": call.lineno + line_offset,
                    "column_number": call.col_offset,
                    "api": "load",
                    # Location identifies the source without copying connector
                    # options (which may contain credentials) into reports.
                    "expression": "load()",
                })
        for statement in tree.body:
            self._walk_statement(statement)
        # Account for every recognized read in the original AST, including
        # annotations, targets, handler types, keywords and control expressions.
        # Unvisited calls are never evaluated using a later/outer binding.
        parents = {child: parent for parent in ast.walk(tree)
                   for child in ast.iter_child_nodes(parent)}
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            parent = parents.get(call)
            while parent is not None and not isinstance(parent, ast.stmt):
                parent = parents.get(parent)
            self._record_unanalyzed_read(call, type(parent).__name__)
        # Return/lambda/assert and other unvisited positions must not silently
        # certify readiness. Do not fold them using the final notebook scope.
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and _call_name(node.func) == "eval"
                    and node not in self._visited_eval_calls):
                self._record_eval_gap(
                    node, "eval occurs outside supported dependency traversal; input is unverified")

    def result(self) -> Dict[str, Any]:
        reads: List[Dict[str, Any]] = []
        # A base DataFrame assignment followed only by a derived filter has
        # the same source provenance in both references.  Suppress precisely
        # that intermediate source record; do not suppress an independent
        # full read of the same object at another source location.
        scoped_origins = {
            (
                ref.get("object_type"), ref.get("object_name"),
                ref.get("cell_number"), ref.get("line_number"),
                ref.get("argument_expression"),
            )
            for ref in self.references if ref.get("filters")
        }
        full_consumed_origins = {
            (
                ref.get("object_type"), ref.get("object_name"),
                ref.get("cell_number"), ref.get("line_number"),
                ref.get("argument_expression"),
            )
            for ref in self.references if ref.get("_full_consumer")
        }
        seen = set()
        for ref in self.references:
            origin = (
                ref.get("object_type"), ref.get("object_name"),
                ref.get("cell_number"), ref.get("line_number"),
                ref.get("argument_expression"),
            )
            if (origin in scoped_origins and origin not in full_consumed_origins
                    and not ref.get("filters")):
                continue
            clean_ref = dict(ref)
            clean_ref.pop("_full_consumer", None)
            key = json.dumps(clean_ref, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                reads.append(clean_ref)
        writes: List[Dict[str, Any]] = []
        seen.clear()
        for ref in self.writes:
            key = json.dumps(ref, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                writes.append(ref)
        unresolved_scopes: List[Dict[str, Any]] = []
        unresolved_scoped_keys = {
            (r.get("object_type"), r.get("argument_expression"))
            for r in self.unresolved_references if r.get("filters")
        }
        seen.clear()
        for ref in self.unresolved_references:
            key0 = (ref.get("object_type"), ref.get("argument_expression"))
            if key0 in unresolved_scoped_keys and not ref.get("filters"):
                continue
            clean_ref = dict(ref)
            clean_ref.pop("_full_consumer", None)
            key = json.dumps(clean_ref, sort_keys=True, default=str)
            if key not in seen:
                seen.add(key)
                unresolved_scopes.append(clean_ref)
        for site in self.unresolved_loads:
            unresolved_scopes.append({
                **site, "kind": "UNRESOLVED_READ",
                "reason": "Option-based or aliased load: input dataset not resolved; "
                          "manual review required",
            })
        resolved_symbols = {}
        for key, value in self.env.items():
            if isinstance(value, datetime):
                value = value.isoformat()
            elif isinstance(value, date):
                value = value.isoformat()
            try:
                json.dumps(value)
                resolved_symbols[key] = value
            except (TypeError, ValueError):
                pass
        return {"read_references": reads, "write_references": writes,
                "unresolved_scopes": unresolved_scopes,
                "unresolved_loads": self.unresolved_loads,
                "mlflow_artifacts": self.mlflow_artifacts,
                "unresolved_mlflow_artifacts": self.unresolved_mlflow_artifacts,
                "resolved_arguments": sorted(self.resolved_arguments),
                "runtime_anchor": (
                    self.anchor.date().isoformat()
                    if self.anchor is not None else None
                ),
                "runtime_anchor_parameter": self.anchor_parameter,
                "resolved_symbols": resolved_symbols,
                "analysis_errors": self.analysis_errors,
                "notebook_runs": self.notebook_runs,
                "not_analyzed": self.not_analyzed}


# ─── EXTRACTION API ──────────────────────────────────────────────────


def extract_from_source(src: str, cell_lang: str = "python", *,
                        pending_load_positions=None) -> Dict[str, List[Any]]:
    """Extract data-availability signals from a single (already-comment-stripped)
    cell source.

    Returns:
        paths          : list of literal scheme:// URLs / /mnt paths
        tables         : list of `schema.table` references
        unresolved     : list of unresolved refs (f-string with vars OR
                          spark.read/write with variable path)
        spark_reads    : list of {"api": ..., "arg": ..., "arg_is_literal": bool}
        spark_writes   : same shape as spark_reads
        boto3_s3       : list of one-line context strings for each sighting
    """
    out: Dict[str, List[Any]] = {
        "paths": [], "tables": [], "unresolved": [],
        "spark_reads": [], "spark_writes": [], "boto3_s3": [],
    }

    # Find dynamic f-string URL spans first.  The broad literal URL regexes
    # otherwise capture only the static prefix before the first interpolation,
    # e.g. ``.../load_date={day}/`` as ``.../load_date``.  That incomplete
    # prefix is not a concrete dependency and must not be probed or inserted
    # into the migration tracker.
    dynamic_url_spans = []
    for match in _FSTRING_URL_RE.finditer(src):
        value = match.group(1)
        if "{" in value:
            dynamic_url_spans.append(match.span(1))
            out["unresolved"].append(value)

    # (1) Literal URL paths. Exclude matches contained in dynamic f-strings;
    # those were recorded as unresolved above.
    for _label, regex in _URL_SCANNERS:
        for match in regex.finditer(src):
            start, end = match.span()
            if _url_match_is_dynamic_fragment(
                src, start, end, dynamic_url_spans
            ):
                continue
            out["paths"].append(match.group(0))

    # (2) Dynamic f-string URLs were captured as unresolved before the literal
    # scan so their static prefixes could be excluded safely.

    # (3) Variable-based spark.read.<api>(<arg>) → unresolved
    for m in _SPARK_READ_RE.finditer(src):
        api = m.group(1)
        arg = (m.group(2) or "").strip()
        is_lit = _arg_is_literal_string(arg)
        out["spark_reads"].append({
            "api": api, "arg": arg[:120], "arg_is_literal": is_lit,
        })
        prefix = src[:m.start()]
        position = (prefix.count("\n") + 1, len(prefix.rsplit("\n", 1)[-1].encode("utf-8")))
        pending = api == "load" and position in (pending_load_positions or ())
        if arg and not is_lit and not pending:
            out["unresolved"].append(f"spark.read.{api}({arg[:80]})")

    # (4) Variable-based .write.<api>(<arg>) → unresolved
    for m in _SPARK_WRITE_RE.finditer(src):
        api = m.group(1)
        arg = (m.group(2) or "").strip()
        is_lit = _arg_is_literal_string(arg)
        out["spark_writes"].append({
            "api": api, "arg": arg[:120], "arg_is_literal": is_lit,
        })
        if arg and not is_lit:
            out["unresolved"].append(f".write.{api}({arg[:80]})")

    # (5) boto3.client('s3') / .resource('s3') — hard migration signal
    for m in _BOTO3_S3_RE.finditer(src):
        line_start = src.rfind('\n', 0, m.start()) + 1
        line_end = src.find('\n', m.end())
        if line_end == -1:
            line_end = len(src)
        out["boto3_s3"].append(src[line_start:line_end].strip()[:120])

    # (6) Tables via SQL keywords (skip Python imports on same line)
    for line in src.splitlines():
        stripped = line.strip()
        if re.match(r'^(?:from|import)\s+', stripped, re.IGNORECASE):
            continue
        for m in _SQL_KW_TABLE_RE.findall(line):
            if m.split(".")[0] not in _SQL_TABLE_SKIP_LHS:
                out["tables"].append(m)

    # (7) Tables via Spark API with literal argument
    for m in _SPARK_TABLE_LITERAL_RE.findall(src):
        out["tables"].append(m)
    for m in _SAVE_AS_TABLE_RE.findall(src):
        out["tables"].append(m)
    for m in _INSERT_INTO_RE.findall(src):
        out["tables"].append(m)

    return out


def _cell_language(default_lang: str, raw_src: str) -> str:
    """Return the effective cell language given the notebook default and any
    leading `%magic` in the cell source."""
    first_line = next((l.strip() for l in raw_src.splitlines() if l.strip()), "")
    lm = re.match(r'^%(\w+)', first_line)
    if lm:
        return _MAGIC_LANG.get(lm.group(1).lower(), lm.group(1).lower())
    return default_lang


def _recover_magic_wrapped(raw_src: str) -> str:
    """Databricks exports sometimes wrap a non-default-language cell body as
    `# MAGIC <code>` on each line. Recover the real code."""
    if "# MAGIC" not in raw_src and "#MAGIC" not in raw_src:
        return raw_src
    return "\n".join(
        (re.match(r'^#\s*MAGIC ?(.*)$', l).group(1)
         if re.match(r'^#\s*MAGIC', l) else l)
        for l in raw_src.splitlines()
    )


def extract_from_notebook(nb: Dict[str, Any],
                          runtime_parameters: Optional[Mapping[str, Any]] = None,
                          initial_symbols: Optional[Mapping[str, Any]] = None,
                          notebook_dir: Optional[str] = None,
                          import_namespaces: Optional[Mapping[str, Optional[ModuleNamespace]]] = None,
                          ) -> Dict[str, Any]:
    """Aggregate `extract_from_source` across all code cells in a parsed
    notebook JSON. Preserves the report schema of the original scanner
    (adds `spark_reads`, `spark_writes`, `boto3_s3`). ``notebook_dir`` is the
    run-time working directory used to resolve relative file reads;
    ``import_namespaces`` are the workspace modules its imports bind from."""
    default_lang = (
        nb.get("metadata", {})
          .get("language_info", {})
          .get("name", "") or "python"
    ).lower()

    agg: Dict[str, List[Any]] = {
        "paths": [], "tables": [], "unresolved": [],
        "python_imports": [], "scala_imports": [],
        "spark_reads": [], "spark_writes": [], "boto3_s3": [],
        "cells_source": "", "temporary_views": [],
    }
    analyzer = _PythonReferenceAnalyzer(
        runtime_parameters, initial_symbols, notebook_dir, import_namespaces)
    trees = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        raw = _recover_magic_wrapped("".join(cell.get("source", [])))
        if _cell_language(default_lang, raw) == "python":
            tree = _parse_python(strip_comments(raw, lang="python"))
            if tree is not None:
                trees.append(tree)
    analyzer.rebound_globals = _rebound_globals(trees, analyzer.import_namespaces)

    for cell_number, cell in enumerate(nb.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue

        raw_src = "".join(cell.get("source", []))
        raw_src = _recover_magic_wrapped(raw_src)
        cell_lang = _cell_language(default_lang, raw_src)

        # State-machine comment strip (per-cell language)
        src = strip_comments(raw_src, lang=cell_lang)
        agg["cells_source"] += src + "\n"
        for match in _TEMP_VIEW_RE.finditer(src):
            agg["temporary_views"].append({
                "name": match.group("name"),
                "cell_number": cell_number,
                "api": match.group("api"),
            })
        if cell_lang == "python":
            analyzer.analyze_cell(src, cell_number)

        pending = {(site["line_number"], site["column_number"])
                   for site in analyzer.unresolved_loads if site["cell_number"] == cell_number}
        cell_out = extract_from_source(src, cell_lang, pending_load_positions=pending)
        for k in ("paths", "tables", "unresolved",
                  "spark_reads", "spark_writes", "boto3_s3"):
            agg[k].extend(cell_out[k])

        # Imports (per-cell language)
        for line in src.splitlines():
            stripped = line.strip()
            if cell_lang == "python":
                py_from = re.match(r'^from\s+([\w\.]+)\s+import', stripped)
                py_imp = re.match(r'^import\s+([\w\.]+)', stripped)
                if py_from:
                    top = py_from.group(1).split(".")[0]
                    if top not in _SKIP_PY and top not in _JVM_ROOTS:
                        agg["python_imports"].append(py_from.group(1))
                elif py_imp:
                    top = py_imp.group(1).split(".")[0]
                    if top not in _SKIP_PY and top not in _JVM_ROOTS:
                        agg["python_imports"].append(py_imp.group(1))
            elif cell_lang == "scala":
                sc_imp = re.match(r'^import\s+([\w\.]+)', stripped)
                if sc_imp:
                    agg["scala_imports"].append(sc_imp.group(1))
            # PySpark → JVM access, any cell language
            for jvm_m in re.findall(r'spark\._jvm\.([\w\.]+)', stripped):
                agg["scala_imports"].append(jvm_m)

    # Order-preserving dedup
    def _dedup(xs: List[Any]) -> List[Any]:
        seen = set()
        result = []
        for x in xs:
            key = repr(x) if isinstance(x, dict) else x
            if key not in seen:
                seen.add(key)
                result.append(x)
        return result

    for k in ("paths", "tables", "unresolved",
              "python_imports", "scala_imports",
              "spark_reads", "spark_writes", "boto3_s3",
              "temporary_views"):
        agg[k] = _dedup(agg[k])

    structured = analyzer.result()
    agg.update(structured)

    # Add evaluated variable/f-string references. The legacy URL regex sees
    # only literal base paths; these are the actual runtime inputs/outputs.
    for ref in structured["read_references"] + structured["write_references"]:
        target = "tables" if ref["object_type"] == "table" else "paths"
        if ref["object_name"] not in agg[target]:
            agg[target].append(ref["object_name"])

    return agg


def python_import_froms(nb: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the ``from <module> import <names>`` statements that bind
    module-level names in a notebook's Python cells, in source order.

    Imports inside function or class bodies bind no notebook globals and are
    left out. Cells that do not parse are skipped here; `extract_from_notebook`
    already reports them as SYNTAX_ERROR.
    """
    default_lang = (
        nb.get("metadata", {})
          .get("language_info", {})
          .get("name", "") or "python"
    ).lower()
    found: List[Dict[str, Any]] = []

    def visit(statements: List[ast.stmt], cell_number: int) -> None:
        for statement in statements:
            if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if isinstance(statement, ast.ImportFrom):
                found.append({
                    "module": statement.module,
                    "names": [(alias.name, alias.asname) for alias in statement.names],
                    "level": statement.level,
                    "cell_number": cell_number,
                })
                continue
            for field in ("body", "orelse", "finalbody"):
                visit(getattr(statement, field, None) or [], cell_number)
            for handler in getattr(statement, "handlers", None) or []:
                visit(handler.body, cell_number)

    for cell_number, cell in enumerate(nb.get("cells", []), start=1):
        if cell.get("cell_type") != "code":
            continue
        raw_src = _recover_magic_wrapped("".join(cell.get("source", [])))
        if _cell_language(default_lang, raw_src) != "python":
            continue
        source = strip_comments(raw_src, lang="python")
        lines = source.splitlines()
        if lines and lines[0].lstrip().startswith("%python"):
            source = "\n".join(lines[1:])
        try:
            tree = ast.parse(source)
        except (SyntaxError, ValueError):
            continue
        visit(tree.body, cell_number)
    return found


def module_namespace(source: str,
                     import_namespaces: Optional[Mapping[str, Optional[ModuleNamespace]]] = None,
                     ) -> Tuple[Optional[ModuleNamespace], List[Dict[str, Any]]]:
    """Return the module-level namespace of a ``.py`` module's source, plus
    its analysis errors. A module that does not parse has no namespace."""
    analyzer = _PythonReferenceAnalyzer(import_namespaces=import_namespaces)
    tree = _parse_python(source)
    if tree is not None:
        analyzer.rebound_globals = _rebound_globals([tree], analyzer.import_namespaces)
    analyzer.analyze_cell(source, 1)
    if analyzer.analysis_errors:
        return None, analyzer.analysis_errors
    values = {name: analyzer.env[name] for name in analyzer.bound_names if name in analyzer.env}
    input_gaps = {}
    for finding in analyzer.not_analyzed + analyzer.references + analyzer.unresolved_references:
        api = finding.get("api", "")
        if (finding.get("kind") not in {"mlflow_registry", "unsupported_read"}
                and _file_read_api_name(api) is None):
            continue
        key = (api, finding.get("line_number"))
        input_gaps[key] = {
            "kind": "module_input", "api": api,
            "module_line_number": finding.get("line_number"),
            "reason": "Imported source contains an input read; caller bindings require manual review.",
        }
    return ModuleNamespace(values=values, names=frozenset(analyzer.bound_names),
                           input_gaps=tuple(input_gaps.values())), []

"""
AIDP Notebook Policy - mandatory sandbox gate for notebook execution
=====================================================================
Runtime enforcement that sits next to the execution sink in
``aidp_compat.notebook`` (``dbutils.notebook.run``) and the write paths the
compat layer owns (``dbutils.fs.rm/mv/cp/put``, ``safe_io`` Spark writes).

Static analysis elsewhere in the migrator can be bypassed by a missed rule or
a hand-edited notebook; this module makes the boundary hold at the point
where code actually runs.

Two pieces:

1. ``SandboxPolicy`` - WHAT the run is allowed to touch. It must be declared
   before any notebook code executes (fail closed), either from the
   environment::

       AIDP_SANDBOX_CATALOG=default
       AIDP_SANDBOX_SCHEMA=migration_sandbox
       AIDP_SANDBOX_PREFIX=oci://sandbox-bucket@namespace/migration/
       AIDP_SANDBOX_ALLOW_NETWORK=0          # optional, default off

   or programmatically via :func:`set_sandbox_policy`.

2. ``enforce_cell_policy`` - an AST gate applied to every non-magic code cell
   before ``exec``. It refuses (``PolicyViolation``, a ``PermissionError``)
   environment reads, process/network imports, destructive filesystem calls,
   dynamic code execution, and writes whose target is outside the sandbox.
   Dynamic (non-literal) write targets are refused as well unless the rule id
   is listed in ``AIDP_NOTEBOOK_POLICY_ALLOW`` (a reviewed exception).

Every refusal and every allowed exception is appended to an in-process policy
log (:func:`get_policy_log`) and, when ``AIDP_NOTEBOOK_POLICY_LOG`` names a
file, to that JSONL file, so the migration report can include it.
"""

import ast
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# ── Environment contract ───────────────────────────────────────────────
ENV_SANDBOX_CATALOG = "AIDP_SANDBOX_CATALOG"
ENV_SANDBOX_SCHEMA = "AIDP_SANDBOX_SCHEMA"
ENV_SANDBOX_PREFIX = "AIDP_SANDBOX_PREFIX"
ENV_SANDBOX_ALLOW_NETWORK = "AIDP_SANDBOX_ALLOW_NETWORK"
ENV_POLICY_ALLOW = "AIDP_NOTEBOOK_POLICY_ALLOW"
ENV_POLICY_LOG = "AIDP_NOTEBOOK_POLICY_LOG"

# ── Rule ids ───────────────────────────────────────────────────────────
RULE_IMPORT_PROCESS = "NBP-IMPORT-PROCESS"      # subprocess / ctypes / pty
RULE_IMPORT_NETWORK = "NBP-IMPORT-NETWORK"      # socket / requests / urllib / http.client / paramiko
RULE_OS_ENVIRON = "NBP-OS-ENVIRON"              # os.environ / os.getenv / os.putenv
RULE_OS_PROCESS = "NBP-OS-PROCESS"              # os.system / os.popen / os.exec* / os.spawn*
RULE_SHUTIL_RMTREE = "NBP-SHUTIL-RMTREE"
RULE_BUILTIN_EXEC = "NBP-BUILTIN-EXEC"          # eval / exec / compile / __import__
RULE_OPEN_PATH = "NBP-OPEN-PATH"                # open(<abs literal outside sandbox>)
RULE_OPEN_DYNAMIC = "NBP-OPEN-DYNAMIC"          # open(<non-literal>, "w"/"a"/"x"/"+")
RULE_DBFS_PATH = "NBP-DBFS-PATH"                # dbutils.fs.rm/mv/cp/put literal outside prefix
RULE_DBFS_DYNAMIC = "NBP-DBFS-DYNAMIC"
RULE_TABLE_TARGET = "NBP-TABLE-TARGET"          # saveAsTable / insertInto literal outside catalog.schema
RULE_TABLE_DYNAMIC = "NBP-TABLE-DYNAMIC"
RULE_SQL_TARGET = "NBP-SQL-TARGET"              # spark.sql DDL/DML literal outside catalog.schema / prefix
RULE_SQL_DYNAMIC = "NBP-SQL-DYNAMIC"
RULE_PATH_WRITE = "NBP-PATH-WRITE"              # df.write.parquet/csv/json/orc/text/save literal outside prefix
RULE_PATH_WRITE_DYNAMIC = "NBP-PATH-WRITE-DYNAMIC"
RULE_RUNTIME_PATH = "NBP-RUNTIME-PATH"          # compat helper asked to write outside the prefix
RULE_RUNTIME_TABLE = "NBP-RUNTIME-TABLE"        # compat helper asked to write outside catalog.schema

ALL_RULES = (
    RULE_IMPORT_PROCESS, RULE_IMPORT_NETWORK, RULE_OS_ENVIRON, RULE_OS_PROCESS,
    RULE_SHUTIL_RMTREE, RULE_BUILTIN_EXEC, RULE_OPEN_PATH, RULE_OPEN_DYNAMIC,
    RULE_DBFS_PATH, RULE_DBFS_DYNAMIC, RULE_TABLE_TARGET, RULE_TABLE_DYNAMIC,
    RULE_SQL_TARGET, RULE_SQL_DYNAMIC, RULE_PATH_WRITE, RULE_PATH_WRITE_DYNAMIC,
    RULE_RUNTIME_PATH, RULE_RUNTIME_TABLE,
)

_PROCESS_MODULES = frozenset({"subprocess", "ctypes", "pty"})
_NETWORK_MODULES = frozenset({"socket", "requests", "urllib", "paramiko", "http.client"})
_OS_ENV_ATTRS = frozenset({"environ", "getenv", "putenv", "unsetenv", "environb", "getenvb"})
_OS_PROCESS_ATTRS = frozenset({"system", "popen", "posix_spawn", "posix_spawnp", "startfile"})
_OS_PROCESS_PREFIXES = ("exec", "spawn")
_DYNAMIC_BUILTINS = frozenset({"eval", "exec", "compile", "__import__"})
_DBFS_WRITE_METHODS = frozenset({"rm", "mv", "cp", "put"})
_TABLE_WRITE_METHODS = frozenset({"saveAsTable", "insertInto"})
_PATH_WRITE_METHODS = frozenset({"parquet", "csv", "json", "orc", "text", "save"})
_SQL_METHODS = frozenset({"sql"})

# SQL statements that create, replace, mutate or drop a table/schema. The
# first capture group is the object name (optionally backtick-quoted parts).
_SQL_IDENT = r"((?:`[^`]+`|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:`[^`]+`|[A-Za-z_][\w$]*)){0,2})"
_SQL_WRITE_RE = re.compile(
    r"^\s*(?:"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?(?:TABLE|VIEW|SCHEMA|DATABASE)(?:\s+IF\s+NOT\s+EXISTS)?"
    r"|INSERT\s+(?:INTO|OVERWRITE)(?:\s+TABLE)?"
    r"|DROP\s+(?:TABLE|VIEW|SCHEMA|DATABASE)(?:\s+IF\s+EXISTS)?"
    r"|ALTER\s+(?:TABLE|VIEW|SCHEMA|DATABASE)"
    r"|TRUNCATE\s+TABLE"
    r"|MERGE\s+INTO"
    r"|DELETE\s+FROM"
    r"|UPDATE"
    r")\s+" + _SQL_IDENT,
    re.IGNORECASE | re.DOTALL,
)
_SQL_WRITE_VERB_RE = re.compile(
    r"^\s*(?:CREATE|INSERT|DROP|ALTER|TRUNCATE|MERGE|DELETE|UPDATE)\b",
    re.IGNORECASE,
)
# Session-scoped objects are not catalog writes: CREATE [OR REPLACE] [GLOBAL] TEMP[ORARY] VIEW.
_SQL_TEMP_VIEW_RE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\b", re.IGNORECASE
)


def _is_sql_write_verb(text: str) -> bool:
    if _SQL_TEMP_VIEW_RE.match(text):
        return False
    return bool(_SQL_WRITE_VERB_RE.match(text))
_SQL_LOCATION_RE = re.compile(r"\bLOCATION\s+'([^']+)'", re.IGNORECASE)
_SQL_SCHEMA_STMT_RE = re.compile(
    r"^\s*(?:CREATE|DROP|ALTER)\s+(?:OR\s+REPLACE\s+)?(?:SCHEMA|DATABASE)\b", re.IGNORECASE
)
_SQL_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)


# ── Errors ─────────────────────────────────────────────────────────────
class NotebookPolicyError(PermissionError):
    """Base class for every refusal raised by the notebook policy."""

    def __init__(self, message: str, *, notebook_path: str = "", cell_index: Optional[int] = None,
                 rule_id: str = "", target: str = "", remediation: str = ""):
        super().__init__(message)
        self.notebook_path = notebook_path
        self.cell_index = cell_index
        self.rule_id = rule_id
        self.target = target
        self.remediation = remediation


class SandboxUndeclaredError(NotebookPolicyError):
    """No sandbox policy has been declared; nothing may execute."""


class PolicyViolation(NotebookPolicyError):
    """A notebook cell or compat helper tried to leave the sandbox."""


# ── Policy ─────────────────────────────────────────────────────────────
def _norm_prefix(prefix: str) -> str:
    p = prefix.strip()
    if not p:
        return p
    # Treat a prefix as a directory boundary so "oci://b@ns/sandbox" does not
    # also admit "oci://b@ns/sandbox-prod/...". A local staging prefix may
    # already end in the host separator.
    return p if p.endswith(("/", "\\")) else p + "/"


def _strip_quotes(part: str) -> str:
    part = part.strip()
    if len(part) >= 2 and part[0] == part[-1] and part[0] in "`\"'":
        part = part[1:-1]
    return part.strip()


def _split_table_name(name: str) -> List[str]:
    """Split ``cat.sch.tbl`` into parts, honouring backtick-quoted parts."""
    parts: List[str] = []
    buf = ""
    quoted = False
    for ch in name.strip():
        if ch == "`":
            quoted = not quoted
            continue
        if ch == "." and not quoted:
            parts.append(buf.strip())
            buf = ""
            continue
        buf += ch
    parts.append(buf.strip())
    return [p for p in parts if p]


@dataclass(frozen=True)
class SandboxPolicy:
    """Boundary a migration run is allowed to write inside of.

    Args:
        catalog: Sandbox catalog (AIDP uses ``default`` uniformly).
        schema: Sandbox schema inside ``catalog``.
        prefix: Object-storage prefix every path write must sit under
            (``oci://bucket@namespace/path/``). Additional prefixes, for
            example a FUSE ``/Volumes/...`` staging area, can be supplied via
            ``extra_prefixes`` or as a comma list in ``AIDP_SANDBOX_PREFIX``.
        allow_network: Permit network-capable imports (``requests``,
            ``socket``, ``urllib``, ``http.client``, ``paramiko``).
            Process/FFI imports stay refused regardless.
        allowed_rules: Rule ids that are a reviewed exception for this run
            (``AIDP_NOTEBOOK_POLICY_ALLOW``). A matching violation is logged
            as ``allowed_exception`` instead of refused.
        extra_prefixes: Additional path prefixes considered inside the sandbox.
    """

    catalog: str
    schema: str
    prefix: str
    allow_network: bool = False
    allowed_rules: Tuple[str, ...] = ()
    extra_prefixes: Tuple[str, ...] = ()

    def __post_init__(self):
        missing = [n for n, v in (("catalog", self.catalog), ("schema", self.schema),
                                  ("prefix", self.prefix)) if not (v or "").strip()]
        if missing:
            raise ValueError(f"SandboxPolicy requires non-empty {', '.join(missing)}")
        object.__setattr__(self, "catalog", self.catalog.strip().strip("`").lower())
        object.__setattr__(self, "schema", self.schema.strip().strip("`").lower())
        object.__setattr__(self, "prefix", _norm_prefix(self.prefix))
        object.__setattr__(self, "extra_prefixes",
                           tuple(_norm_prefix(p) for p in self.extra_prefixes if p.strip()))
        object.__setattr__(self, "allowed_rules",
                           tuple(sorted({r.strip() for r in self.allowed_rules if r.strip()})))

    # -- construction -------------------------------------------------
    @classmethod
    def from_env(cls, environ: Optional[Dict[str, str]] = None) -> "SandboxPolicy":
        """Build the policy from ``AIDP_SANDBOX_*``; raise if undeclared."""
        env = os.environ if environ is None else environ
        catalog = env.get(ENV_SANDBOX_CATALOG, "").strip()
        schema = env.get(ENV_SANDBOX_SCHEMA, "").strip()
        prefixes = [p.strip() for p in env.get(ENV_SANDBOX_PREFIX, "").split(",") if p.strip()]
        missing = [name for name, val in ((ENV_SANDBOX_CATALOG, catalog),
                                          (ENV_SANDBOX_SCHEMA, schema),
                                          (ENV_SANDBOX_PREFIX, prefixes)) if not val]
        if missing:
            raise SandboxUndeclaredError(
                "Notebook execution refused: no sandbox policy declared. Set "
                + ", ".join(missing)
                + " (or call aidp_compat.notebook_policy.set_sandbox_policy(...)) "
                "before running notebooks. The migration tool's cluster bootstrap "
                "declares these automatically; a scheduled job must declare them "
                "in its environment.",
                rule_id="NBP-SANDBOX-UNDECLARED",
                remediation="Declare AIDP_SANDBOX_CATALOG, AIDP_SANDBOX_SCHEMA and "
                            "AIDP_SANDBOX_PREFIX for the run.",
            )
        allow_network = env.get(ENV_SANDBOX_ALLOW_NETWORK, "").strip().lower() in ("1", "true", "yes")
        allowed = tuple(r.strip() for r in env.get(ENV_POLICY_ALLOW, "").split(",") if r.strip())
        return cls(catalog=catalog, schema=schema, prefix=prefixes[0],
                   allow_network=allow_network, allowed_rules=allowed,
                   extra_prefixes=tuple(prefixes[1:]))

    # -- membership ---------------------------------------------------
    @property
    def prefixes(self) -> Tuple[str, ...]:
        return (self.prefix,) + self.extra_prefixes

    def path_in_sandbox(self, path: str) -> bool:
        """True when ``path`` is the sandbox prefix or sits under it."""
        if not isinstance(path, str) or not path.strip():
            return False
        p = path.strip()
        for pref in self.prefixes:
            if p == pref.rstrip("/\\") or p.startswith(pref):
                return True
        return False

    def table_in_sandbox(self, name: str) -> bool:
        """True when a 2- or 3-part table name resolves to ``catalog.schema``.

        A 1-part name depends on the session's current database, which the
        policy cannot see; it is treated as outside (fail closed).
        """
        if not isinstance(name, str):
            return False
        parts = [p.lower() for p in _split_table_name(name)]
        if len(parts) == 3:
            return parts[0] == self.catalog and parts[1] == self.schema
        if len(parts) == 2:
            return parts[0] == self.schema
        return False

    def schema_in_sandbox(self, name: str) -> bool:
        """True when a schema reference (``cat.sch`` or ``sch``) is the sandbox schema."""
        parts = [p.lower() for p in _split_table_name(name or "")]
        if len(parts) == 2:
            return parts[0] == self.catalog and parts[1] == self.schema
        if len(parts) == 1:
            return parts[0] == self.schema
        return False

    def is_allowed_exception(self, rule_id: str) -> bool:
        return rule_id in self.allowed_rules

    def describe(self) -> Dict[str, Any]:
        return {
            "catalog": self.catalog,
            "schema": self.schema,
            "prefix": self.prefix,
            "extra_prefixes": list(self.extra_prefixes),
            "allow_network": self.allow_network,
            "allowed_rules": list(self.allowed_rules),
        }


# ── Policy registry (explicit object wins over the environment) ────────
_explicit_policy: Optional[SandboxPolicy] = None
_lock = threading.Lock()


def set_sandbox_policy(policy: Optional[SandboxPolicy]) -> None:
    """Declare (or clear with ``None``) the policy for this process."""
    global _explicit_policy
    with _lock:
        _explicit_policy = policy
    if policy is not None:
        _record("sandbox_declared", rule="", target=policy.prefix, detail=policy.describe())


def current_policy() -> Optional[SandboxPolicy]:
    """Return the declared policy, or ``None`` when nothing is declared."""
    if _explicit_policy is not None:
        return _explicit_policy
    try:
        return SandboxPolicy.from_env()
    except SandboxUndeclaredError:
        return None


def require_policy() -> SandboxPolicy:
    """Return the declared policy or raise :class:`SandboxUndeclaredError`."""
    if _explicit_policy is not None:
        return _explicit_policy
    return SandboxPolicy.from_env()


# ── Policy log ─────────────────────────────────────────────────────────
_policy_log: List[Dict[str, Any]] = []


def _record(event: str, *, rule: str, target: str, notebook_path: str = "",
            cell_index: Optional[int] = None, remediation: str = "",
            detail: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event": event,
        "notebook_path": notebook_path,
        "cell_index": cell_index,
        "rule": rule,
        "target": target,
        "remediation": remediation,
    }
    if detail:
        entry["detail"] = detail
    with _lock:
        _policy_log.append(entry)
    log_file = os.environ.get(ENV_POLICY_LOG, "").strip()
    if log_file:
        try:
            with open(log_file, "a", encoding="utf-8", newline="\n") as fh:
                fh.write(json.dumps(entry, sort_keys=True) + "\n")
        except OSError:
            pass  # the in-process log is authoritative; the file is best effort
    return entry


def get_policy_log() -> List[Dict[str, Any]]:
    """Copy of every refusal / allowed exception / declaration so far."""
    with _lock:
        return [dict(e) for e in _policy_log]


def clear_policy_log() -> None:
    with _lock:
        _policy_log.clear()


def policy_log_json() -> str:
    return json.dumps(get_policy_log(), sort_keys=True)


def policy_log_markdown(entries: Optional[Iterable[Dict[str, Any]]] = None) -> str:
    """Render the policy log as a Markdown table for the migration report."""
    rows = list(get_policy_log() if entries is None else entries)
    if not rows:
        return ""
    out = ["| Event | Notebook | Cell | Rule | Target | Remediation |",
           "|---|---|---|---|---|---|"]
    for e in rows:
        cell = "" if e.get("cell_index") is None else str(e.get("cell_index"))
        out.append("| {event} | `{nb}` | {cell} | {rule} | `{target}` | {rem} |".format(
            event=e.get("event", ""),
            nb=e.get("notebook_path", "") or "-",
            cell=cell,
            rule=e.get("rule", "") or "-",
            target=str(e.get("target", "")).replace("|", "\\|") or "-",
            rem=str(e.get("remediation", "")).replace("|", "\\|") or "-",
        ))
    return "\n".join(out) + "\n"


# ── AST gate ───────────────────────────────────────────────────────────
@dataclass
class Violation:
    rule: str
    detail: str
    target: str = ""
    lineno: int = 0
    remediation: str = ""


def _literal_str(node: Optional[ast.AST]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _joined_literal_head(node: ast.AST) -> Optional[str]:
    """Leading literal text of an f-string / ``"lit" + x`` concatenation."""
    if isinstance(node, ast.JoinedStr):
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                return part.value
            return ""
        return ""
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
        return _joined_literal_head(node.left) if not isinstance(node.left, ast.Constant) \
            else _literal_str(node.left)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
            and node.func.attr == "format":
        return _literal_str(node.func.value)
    return None


def _attr_chain(node: ast.AST) -> List[str]:
    """``a.b.c`` -> ["a", "b", "c"]; calls in the chain are skipped (``df.write.mode(..).parquet``)."""
    parts: List[str] = []
    cur = node
    while True:
        if isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
        elif isinstance(cur, ast.Call):
            cur = cur.func
        elif isinstance(cur, ast.Name):
            parts.append(cur.id)
            break
        else:
            break
    parts.reverse()
    return parts


def _module_root(name: str) -> str:
    return name.split(".", 1)[0]


def _is_abs_path(path: str) -> bool:
    """POSIX-absolute, URI (``scheme://``) or host-absolute path.

    Notebooks run on Linux clusters, so a leading ``/`` is absolute even when
    this module is imported on Windows (where ``os.path.isabs`` disagrees).
    """
    return path.startswith("/") or "://" in path or os.path.isabs(path)


def _is_prohibited_module(name: str, policy: SandboxPolicy) -> Optional[Tuple[str, str]]:
    root = _module_root(name)
    if root in _PROCESS_MODULES:
        return RULE_IMPORT_PROCESS, root
    if root in _NETWORK_MODULES or name in _NETWORK_MODULES:
        if policy.allow_network:
            return None
        return RULE_IMPORT_NETWORK, root if root in _NETWORK_MODULES else name
    return None


def _first_arg(call: ast.Call, kw: Sequence[str]) -> Optional[ast.AST]:
    if call.args:
        return call.args[0]
    for k in call.keywords:
        if k.arg in kw:
            return k.value
    return None


def _open_mode(call: ast.Call) -> str:
    if len(call.args) >= 2:
        return _literal_str(call.args[1]) or ""
    for k in call.keywords:
        if k.arg == "mode":
            return _literal_str(k.value) or ""
    return "r"


def _sql_write_target(sql: str) -> Tuple[Optional[str], bool, Optional[str]]:
    """Return (object name or None, is_schema_statement, LOCATION path or None).

    ``object name`` is None when the statement is not a write/DDL statement.
    """
    text = _SQL_COMMENT_RE.sub(" ", sql).strip()
    if not text or not _is_sql_write_verb(text):
        return None, False, None
    m = _SQL_WRITE_RE.match(text)
    loc = _SQL_LOCATION_RE.search(text)
    location = loc.group(1) if loc else None
    if not m:
        if _SQL_WRITE_VERB_RE.match(text):
            # A write verb we recognise but could not parse a target for.
            return "", False, location
        return None, False, location
    return m.group(1), bool(_SQL_SCHEMA_STMT_RE.match(text)), location


class _PolicyScanner(ast.NodeVisitor):
    def __init__(self, policy: SandboxPolicy):
        self.policy = policy
        self.violations: List[Violation] = []
        self._seen: set = set()

    def _add(self, rule: str, node: ast.AST, detail: str, target: str = "", remediation: str = ""):
        lineno = getattr(node, "lineno", 0)
        key = (rule, lineno, target)
        if key in self._seen:
            # os.environ.get(...) visits both the Call and the inner Attribute;
            # report the line once.
            return
        self._seen.add(key)
        self.violations.append(Violation(rule=rule, detail=detail, target=target,
                                         lineno=lineno, remediation=remediation))

    # -- imports ------------------------------------------------------
    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            hit = _is_prohibited_module(alias.name, self.policy)
            if hit:
                rule, mod = hit
                self._add(rule, node, f"import of prohibited module '{alias.name}'", target=mod,
                          remediation=self._import_remediation(rule))
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        mod = node.module or ""
        hit = _is_prohibited_module(mod, self.policy) if mod else None
        if hit:
            rule, root = hit
            self._add(rule, node, f"import from prohibited module '{mod}'", target=root,
                      remediation=self._import_remediation(rule))
        else:
            names = {a.name for a in node.names}
            if mod == "http" and "client" in names and not self.policy.allow_network:
                self._add(RULE_IMPORT_NETWORK, node, "import of prohibited module 'http.client'",
                          target="http.client", remediation=self._import_remediation(RULE_IMPORT_NETWORK))
            if mod == "os":
                env_hits = names & _OS_ENV_ATTRS
                if env_hits:
                    self._add(RULE_OS_ENVIRON, node, f"import of os.{sorted(env_hits)[0]}",
                              target=f"os.{sorted(env_hits)[0]}",
                              remediation="Pass values through dbutils.widgets / job parameters "
                                          "instead of reading the process environment.")
                proc_hits = {n for n in names if n in _OS_PROCESS_ATTRS or n.startswith(_OS_PROCESS_PREFIXES)}
                if proc_hits:
                    self._add(RULE_OS_PROCESS, node, f"import of os.{sorted(proc_hits)[0]}",
                              target=f"os.{sorted(proc_hits)[0]}",
                              remediation="Shell/process execution is not available inside the sandbox.")
            if mod == "shutil" and "rmtree" in names:
                self._add(RULE_SHUTIL_RMTREE, node, "import of shutil.rmtree", target="shutil.rmtree",
                          remediation="Use dbutils.fs.rm on a sandbox path instead.")
        self.generic_visit(node)

    @staticmethod
    def _import_remediation(rule: str) -> str:
        if rule == RULE_IMPORT_NETWORK:
            return ("Network access is off for this run. Set AIDP_SANDBOX_ALLOW_NETWORK=1 "
                    "after review, or remove the network call.")
        return "Process and FFI modules are not available inside the sandbox; remove the import."

    # -- attribute access --------------------------------------------
    def visit_Attribute(self, node: ast.Attribute):
        chain = _attr_chain(node)
        if len(chain) >= 2 and chain[0] == "os":
            attr = chain[1]
            if attr in _OS_ENV_ATTRS:
                self._add(RULE_OS_ENVIRON, node, f"access to os.{attr}", target=f"os.{attr}",
                          remediation="Pass values through dbutils.widgets / job parameters "
                                      "instead of reading the process environment.")
            elif attr in _OS_PROCESS_ATTRS or attr.startswith(_OS_PROCESS_PREFIXES):
                self._add(RULE_OS_PROCESS, node, f"access to os.{attr}", target=f"os.{attr}",
                          remediation="Shell/process execution is not available inside the sandbox.")
        if chain[:2] == ["shutil", "rmtree"]:
            self._add(RULE_SHUTIL_RMTREE, node, "access to shutil.rmtree", target="shutil.rmtree",
                      remediation="Use dbutils.fs.rm on a sandbox path instead.")
        self.generic_visit(node)

    # -- calls ----------------------------------------------------------
    def visit_Call(self, node: ast.Call):
        func = node.func
        if isinstance(func, ast.Name):
            if func.id in _DYNAMIC_BUILTINS:
                self._add(RULE_BUILTIN_EXEC, node, f"call to builtin {func.id}()", target=func.id,
                          remediation="Dynamic code execution is refused; inline the code as a cell.")
            elif func.id == "open":
                self._check_open(node)
            elif func.id == "sql":
                self._check_sql(node, "sql")
        elif isinstance(func, ast.Attribute):
            chain = _attr_chain(func)
            attr = func.attr
            if len(chain) >= 3 and chain[-2] == "fs" and attr in _DBFS_WRITE_METHODS:
                self._check_dbfs(node, ".".join(chain[-3:]), attr)
            elif attr in _TABLE_WRITE_METHODS and "write" in chain:
                self._check_table_write(node, attr)
            elif attr in _SQL_METHODS and len(chain) >= 2:
                self._check_sql(node, ".".join(chain[-2:]))
            elif attr in _PATH_WRITE_METHODS and "write" in chain:
                self._check_path_write(node, attr)
            elif attr == "open" and chain[:1] == ["builtins"]:
                self._check_open(node)
        self.generic_visit(node)

    def _check_open(self, node: ast.Call):
        arg = _first_arg(node, ("file",))
        if arg is None:
            return
        lit = _literal_str(arg)
        mode = _open_mode(node)
        if lit is not None:
            if _is_abs_path(lit) and not self.policy.path_in_sandbox(lit):
                self._add(RULE_OPEN_PATH, node, f"open() on absolute path outside the sandbox prefix",
                          target=lit,
                          remediation=f"Read/write under the sandbox prefix {self.policy.prefix} "
                                      "or use a relative path.")
            return
        if any(ch in mode for ch in "wax+"):
            self._add(RULE_OPEN_DYNAMIC, node, "open() for writing with a non-literal path",
                      target="<dynamic>",
                      remediation="Use a literal sandbox path, or list NBP-OPEN-DYNAMIC in "
                                  "AIDP_NOTEBOOK_POLICY_ALLOW after review.")

    def _check_dbfs(self, node: ast.Call, call_name: str, method: str):
        # rm(path) / put(path, ...) -> arg0 ; cp(src, dst) -> dst ; mv(src, dst) -> src AND dst
        if method == "mv":
            targets = list(node.args[:2])
            for k in node.keywords:
                if k.arg in ("src", "dst", "source", "destination"):
                    targets.append(k.value)
        elif method == "cp":
            targets = [node.args[1]] if len(node.args) >= 2 else \
                [k.value for k in node.keywords if k.arg in ("dst", "destination")]
        else:
            t = _first_arg(node, ("path", "file", "dir"))
            targets = [t] if t is not None else []
        if not targets:
            return
        for t in targets:
            lit = _literal_str(t)
            if lit is None:
                self._add(RULE_DBFS_DYNAMIC, node, f"{call_name}() with a non-literal path target",
                          target="<dynamic>",
                          remediation="Use a literal sandbox path, or list NBP-DBFS-DYNAMIC in "
                                      "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
            elif not self.policy.path_in_sandbox(lit):
                self._add(RULE_DBFS_PATH, node, f"{call_name}() target outside the sandbox prefix",
                          target=lit,
                          remediation=f"Only paths under {self.policy.prefix} may be removed, "
                                      "moved or written during this run.")

    def _check_table_write(self, node: ast.Call, method: str):
        arg = _first_arg(node, ("name", "tableName"))
        if arg is None:
            return
        lit = _literal_str(arg)
        if lit is None:
            self._add(RULE_TABLE_DYNAMIC, node, f".{method}() with a non-literal table name",
                      target="<dynamic>",
                      remediation="Use a literal sandbox table, or list NBP-TABLE-DYNAMIC in "
                                  "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
        elif not self.policy.table_in_sandbox(lit):
            self._add(RULE_TABLE_TARGET, node, f".{method}() target outside {self.policy.catalog}.{self.policy.schema}",
                      target=lit,
                      remediation=f"Write to {self.policy.catalog}.{self.policy.schema}.<table> "
                                  "(use the 2- or 3-part name).")

    def _check_path_write(self, node: ast.Call, method: str):
        arg = _first_arg(node, ("path",))
        if arg is None:
            return  # e.g. .save() with no path -> saveAsTable-style / option("path") not supported
        lit = _literal_str(arg)
        if lit is None:
            self._add(RULE_PATH_WRITE_DYNAMIC, node, f".write.{method}() with a non-literal path",
                      target="<dynamic>",
                      remediation="Use a literal sandbox path, or list NBP-PATH-WRITE-DYNAMIC in "
                                  "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
        elif not self.policy.path_in_sandbox(lit):
            self._add(RULE_PATH_WRITE, node, f".write.{method}() target outside the sandbox prefix",
                      target=lit,
                      remediation=f"Write under {self.policy.prefix}.")

    def _check_sql(self, node: ast.Call, call_name: str):
        arg = _first_arg(node, ("sqlQuery", "query"))
        if arg is None:
            return
        lit = _literal_str(arg)
        if lit is None:
            head = _joined_literal_head(arg)
            if head and _is_sql_write_verb(head):
                self._add(RULE_SQL_DYNAMIC, node, f"{call_name}() DDL/DML with a non-literal target",
                          target="<dynamic>",
                          remediation="Use a literal sandbox table, or list NBP-SQL-DYNAMIC in "
                                      "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
            return
        name, is_schema_stmt, location = _sql_write_target(lit)
        if location is not None and not self.policy.path_in_sandbox(location):
            self._add(RULE_SQL_TARGET, node, f"{call_name}() LOCATION outside the sandbox prefix",
                      target=location, remediation=f"Use a LOCATION under {self.policy.prefix}.")
        if name is None:
            return
        if name == "":
            self._add(RULE_SQL_TARGET, node, f"{call_name}() write statement whose target could not be resolved",
                      target=lit.strip().split("\n", 1)[0][:80],
                      remediation="Qualify the target as <catalog>.<schema>.<table> inside the sandbox.")
            return
        inside = self.policy.schema_in_sandbox(name) if is_schema_stmt else self.policy.table_in_sandbox(name)
        if not inside:
            self._add(RULE_SQL_TARGET, node,
                      f"{call_name}() DDL/DML target outside {self.policy.catalog}.{self.policy.schema}",
                      target=name,
                      remediation=f"Target {self.policy.catalog}.{self.policy.schema}.<table> "
                                  "(1-part names are treated as outside the sandbox).")


def scan_source(source: str, *, filename: str = "<cell>", policy: Optional[SandboxPolicy] = None) -> List[Violation]:
    """Parse ``source`` and return every policy violation (empty when clean).

    ``SyntaxError`` propagates: a cell that cannot be parsed is not executed.
    """
    pol = policy or require_policy()
    tree = ast.parse(source, filename=filename)
    scanner = _PolicyScanner(pol)
    scanner.visit(tree)
    scanner.violations.sort(key=lambda v: (v.lineno, v.rule))
    return scanner.violations


def enforce_cell_policy(source: str, *, notebook_path: str, cell_index: int,
                        policy: Optional[SandboxPolicy] = None) -> List[Violation]:
    """Gate one code cell. Returns the allowed exceptions; raises on refusal.

    Raises:
        SandboxUndeclaredError: no policy declared.
        PolicyViolation: the first refused violation (all are logged first).
    """
    pol = policy or require_policy()
    violations = scan_source(source, filename=str(notebook_path), policy=pol)
    refused: List[Violation] = []
    allowed: List[Violation] = []
    for v in violations:
        if pol.is_allowed_exception(v.rule):
            allowed.append(v)
            _record("allowed_exception", rule=v.rule, target=v.target, notebook_path=str(notebook_path),
                    cell_index=cell_index, remediation=v.remediation,
                    detail={"line": v.lineno, "detail": v.detail})
        else:
            refused.append(v)
            _record("refused", rule=v.rule, target=v.target, notebook_path=str(notebook_path),
                    cell_index=cell_index, remediation=v.remediation,
                    detail={"line": v.lineno, "detail": v.detail})
    if refused:
        v = refused[0]
        more = f" (+{len(refused) - 1} more)" if len(refused) > 1 else ""
        raise PolicyViolation(
            f"{notebook_path} cell {cell_index} line {v.lineno}: [{v.rule}] {v.detail}"
            f" (target: {v.target or '-'}){more}. Remediation: {v.remediation}",
            notebook_path=str(notebook_path), cell_index=cell_index, rule_id=v.rule,
            target=v.target, remediation=v.remediation,
        )
    return allowed


# ── Runtime assertions for compat helpers ──────────────────────────────
def assert_path_in_sandbox(path: str, *, operation: str, policy: Optional[SandboxPolicy] = None) -> str:
    """Refuse ``operation`` unless ``path`` is under the sandbox prefix. Returns ``path``."""
    pol = policy or require_policy()
    if not pol.path_in_sandbox(path):
        remediation = f"Only paths under {pol.prefix} may be written by {operation} during this run."
        _record("runtime_refused", rule=RULE_RUNTIME_PATH, target=str(path), remediation=remediation,
                detail={"operation": operation})
        raise PolicyViolation(
            f"{operation}: target outside the sandbox prefix: {path}. Remediation: {remediation}",
            rule_id=RULE_RUNTIME_PATH, target=str(path), remediation=remediation,
        )
    return path


def assert_table_in_sandbox(table_name: str, *, operation: str, policy: Optional[SandboxPolicy] = None) -> str:
    """Refuse ``operation`` unless ``table_name`` resolves to ``catalog.schema``."""
    pol = policy or require_policy()
    if not pol.table_in_sandbox(table_name):
        remediation = (f"Only tables in {pol.catalog}.{pol.schema} may be written by {operation} "
                       "during this run (1-part names are treated as outside).")
        _record("runtime_refused", rule=RULE_RUNTIME_TABLE, target=str(table_name), remediation=remediation,
                detail={"operation": operation})
        raise PolicyViolation(
            f"{operation}: table outside the sandbox schema: {table_name}. Remediation: {remediation}",
            rule_id=RULE_RUNTIME_TABLE, target=str(table_name), remediation=remediation,
        )
    return table_name


__all__ = [
    "SandboxPolicy", "NotebookPolicyError", "SandboxUndeclaredError", "PolicyViolation", "Violation",
    "set_sandbox_policy", "current_policy", "require_policy",
    "scan_source", "enforce_cell_policy",
    "assert_path_in_sandbox", "assert_table_in_sandbox",
    "get_policy_log", "clear_policy_log", "policy_log_json", "policy_log_markdown",
    "ALL_RULES",
]

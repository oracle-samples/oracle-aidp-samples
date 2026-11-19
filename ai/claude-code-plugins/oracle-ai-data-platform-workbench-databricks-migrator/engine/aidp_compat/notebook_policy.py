"""
AIDP Notebook Policy - mandatory sandbox gate for notebook execution
=====================================================================
Runtime enforcement that sits next to the execution sink in
``aidp_compat.notebook`` (``dbutils.notebook.run``) and the write paths the
compat layer owns (``dbutils.fs.rm/mv/cp/put/mkdirs``, ``safe_io`` writes,
``dbutils.secrets``).

Static analysis elsewhere in the migrator can be bypassed by a missed rule or
a hand-edited notebook; this module makes the boundary hold at the point
where code actually runs.

Two pieces:

1. ``SandboxPolicy`` - WHAT the run is allowed to touch. It must be declared
   before any notebook code executes (fail closed), either from the
   environment::

       AIDP_SANDBOX_CATALOG=default
       AIDP_SANDBOX_SCHEMA=migration_sandbox
       AIDP_SANDBOX_PREFIX=oci://sandbox-bucket@namespace/migration/,/tmp/
       AIDP_SANDBOX_ALLOW_NETWORK=0          # optional, default off

   or programmatically via :func:`set_sandbox_policy`. The declaration is a
   one-shot snapshot: the first policy resolved for the process is frozen,
   later environment changes are ignored by the runtime assertions, and a
   redeclaration with a different policy is refused (``NBP-POLICY-TAMPER``).

2. ``enforce_cell_policy`` - an AST gate applied to every non-magic code cell
   before ``exec``. It refuses (``PolicyViolation``, a ``PermissionError``)
   environment reads, process/network imports, destructive filesystem calls,
   dynamic code execution, access to the policy machinery itself, and writes
   whose target is outside the sandbox. Targets that are plain names bound to
   a string literal earlier in the notebook are resolved; any other
   non-literal target is refused unless the rule id is listed in
   ``AIDP_NOTEBOOK_POLICY_ALLOW`` (a reviewed exception). Import aliases
   (``import os as o``) and shim aliases (``fs = dbutils.fs``) are tracked
   across the cells of one run.

   The gate is deny-by-default for every route to the same capabilities, not
   only the direct spelling: the builtins namespace reached through
   ``__builtins__`` / ``builtins`` / ``globals()`` / ``locals()`` / ``vars()``
   / ``getattr``, dynamic attribute names on protected modules, dunder and
   frame introspection chains (``__subclasses__``, ``__globals__``,
   ``f_globals``, ...), the IPython shell API and ``%``/``!`` lines, the
   Spark JVM / py4j bridge (``spark._jvm``, ``sc._jsc``), deserialisers that
   execute code (``pickle.loads``, ``yaml.load`` without a safe loader),
   process / debugger / profiler modules, and names that were bound to any of
   these and later rebound to something the gate cannot resolve. One rule
   table serves every spelling of a module attribute: the direct chain, an
   alias (``import posix as p`` is ``os``), a ``from m import n`` name and
   ``getattr(m, "literal")``. ``str.format`` templates (literal, bound once,
   or wrapped in ``textwrap.dedent`` / ``.strip()``) are walked field by field
   against the dunder read allowlist. Library readers (``pd.read_csv``,
   ``spark.read.csv``, ``np.load``, ``sc.textFile``, ...) with a literal
   local-filesystem path outside the sandbox are refused like ``open()``.

   Three scope decisions are allowed and recorded in the policy log instead of
   refused: a reader path the gate cannot resolve (``unverified_read_path``;
   reading source data through variables is the normal migration case, and
   object-storage / catalog reads are never path-checked; ``<object>.open(p)``
   on a receiver that is neither the builtin nor a ``Path`` is recorded the
   same way when the receiver is a name the notebook bound, while
   ``open(path_var)`` itself stays refused in every mode and so does
   ``thing.open(p)`` on a receiver the run never bound), a ``spark.sql``
   statement with run-time fragments whose literal head verb is a read verb,
   only while the runtime SQL guard of ``dbutils.notebook.run`` is active
   (``unverified_sql_read``; the guard re-classifies the final text before
   Spark executes it, so a bare static scan fails closed instead) and
   ``DataFrame.pipe(name)`` with a bare name the gate cannot type on a bound,
   non-Spark receiver (``unverified_pipe_callable``; ``RDD.pipe(cmd)``, a
   string-typed argument and a receiver the run never bound are refused).

   Routes are closed as classes, not spellings: a gated module or Spark
   object handed around as a value (``[os][0]``, ``run(os)``, ``return os``,
   ``lambda: os``), a process sink name on a receiver the gate cannot resolve
   (``helper().system``, ``obj.popen``), string-keyed accessors
   (``operator.attrgetter`` / ``methodcaller``, ``functools.reduce(getattr)``,
   ``inspect.getattr_static`` / ``getmembers``), re-rooting at a module named
   mid-chain (``shutil.os.system``, ``pd.io.common.os.environ``), deny-by-default
   deserialisers (the pickle family except the dump side, ``torch.load`` at
   any depth, ``numpy`` ``allow_pickle``, every unsafe yaml loader /
   constructor / hook / subclass), ``globals()[k]`` with a tainted key or a
   dereferenced result, and ``open(*a, **kw)``. A list literal is folded into
   a path only while its name is used for nothing but ``sep.join(name)`` (an
   in-place mutation, alias or helper call makes the join dynamic); loaders
   gated on a keyword fail closed when the switch is positional or hidden in
   ``*args`` / ``**kwargs`` (``np.load(p, None, True)``, ``torch.load(p,
   pickle_module=..)``); the state of a gated module is read-only
   (``yaml.SafeLoader.yaml_constructors[..] = h``, ``.update(..)``,
   ``setattr``; ``sys.path`` and ``torch`` flags excepted); a process sink
   name or a ``_private`` attribute on an imported module the gate has no
   rules for is refused (``mymod.system``, ``mymod._run``); ``site`` / ``pip``
   load code from paths and are refused like ``addPyFile``. Library writers
   are the mirror of the readers (``np.save``, ``torch.save``, ``joblib.dump``,
   ``scipy.io.savemat``, ``cv2.imwrite``, ``plt.savefig``, ``pq.write_table``,
   ``df.to_csv`` / ``to_parquet`` / ``write_parquet`` / ``savefig`` on any
   receiver, ``wb.save`` / ``model.save`` with a literal): a literal local path
   outside the sandbox is refused like ``df.write.parquet``, and a path the
   gate cannot resolve is refused too (writes fail closed; an in-memory buffer
   or ``ExcelWriter`` handle the notebook bound is not a path). ``tempfile``
   creators with a literal ``dir=`` / absolute ``prefix=`` (or
   ``tempfile.tempdir =``) outside the sandbox are filesystem writes; live
   frame dumps (``faulthandler``, ``traceback.walk_stack`` / ``walk_tb``,
   ``capture_locals=True``, ``Task.get_stack``) are introspection routes.

The gate is a static check of the forms listed in SUPPORTED_OPERATIONS.md
section 8; it is not a Python sandbox, and a static gate over a full Python
runtime is a mitigation layer: the platform-level control is the IAM scope of
the cluster the notebook runs on. The runtime assertions
(:func:`assert_path_in_sandbox`, :func:`assert_table_in_sandbox`) are the
enforcement layer for every write that goes through the compat helpers.

Every refusal, allowed exception, unverified read and sandbox declaration is
appended to an in-process policy log (:func:`get_policy_log`) and, when
``AIDP_NOTEBOOK_POLICY_LOG`` names a file, to that JSONL file, so the
migration report can include it.
"""

import ast
import json
import os
import posixpath
import re
import string
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# ── Environment contract ───────────────────────────────────────────────
ENV_SANDBOX_CATALOG = "AIDP_SANDBOX_CATALOG"
ENV_SANDBOX_SCHEMA = "AIDP_SANDBOX_SCHEMA"
ENV_SANDBOX_PREFIX = "AIDP_SANDBOX_PREFIX"
ENV_SANDBOX_ALLOW_NETWORK = "AIDP_SANDBOX_ALLOW_NETWORK"
ENV_POLICY_ALLOW = "AIDP_NOTEBOOK_POLICY_ALLOW"
ENV_POLICY_LOG = "AIDP_NOTEBOOK_POLICY_LOG"

# ── Rule ids ───────────────────────────────────────────────────────────
RULE_IMPORT_PROCESS = "NBP-IMPORT-PROCESS"      # subprocess / ctypes / pty / multiprocessing
RULE_IMPORT_NETWORK = "NBP-IMPORT-NETWORK"      # socket / requests / urllib / http.client / paramiko / ...
RULE_OS_ENVIRON = "NBP-OS-ENVIRON"              # os.environ / os.getenv / os.putenv / from os import *
RULE_OS_PROCESS = "NBP-OS-PROCESS"              # os.system / os.popen / os.exec* / os.spawn* / os.fork / asyncio subprocess
RULE_SHUTIL_RMTREE = "NBP-SHUTIL-RMTREE"
RULE_BUILTIN_EXEC = "NBP-BUILTIN-EXEC"          # eval / exec / compile / __import__
RULE_INDIRECT = "NBP-INDIRECT"                  # importlib / builtins / sys.modules / getattr(os, ...)
RULE_POLICY_TAMPER = "NBP-POLICY-TAMPER"        # aidp_compat internals, set_sandbox_policy, module attribute assignment
RULE_OPEN_PATH = "NBP-OPEN-PATH"                # open() / pathlib read-write on absolute path outside sandbox
RULE_OPEN_DYNAMIC = "NBP-OPEN-DYNAMIC"          # open() / pathlib read-write with an unresolvable path
RULE_FS_PATH = "NBP-FS-PATH"                    # os.remove/rename/mkdir..., pathlib unlink/rmdir/mkdir..., shutil.move/copy
RULE_FS_DYNAMIC = "NBP-FS-DYNAMIC"
RULE_DBFS_PATH = "NBP-DBFS-PATH"                # dbutils.fs.rm/mv/cp/put/mkdirs literal outside prefix
RULE_DBFS_DYNAMIC = "NBP-DBFS-DYNAMIC"
RULE_TABLE_TARGET = "NBP-TABLE-TARGET"          # saveAsTable / insertInto / writeTo literal outside catalog.schema
RULE_TABLE_DYNAMIC = "NBP-TABLE-DYNAMIC"
RULE_SQL_TARGET = "NBP-SQL-TARGET"              # spark.sql DDL/DML literal outside catalog.schema / prefix
RULE_SQL_DYNAMIC = "NBP-SQL-DYNAMIC"            # spark.sql with an unresolvable statement
RULE_SQL_REFUSED = "NBP-SQL-REFUSED"            # spark.sql statement outside the SQL read / in-sandbox write allowlist
RULE_PATH_WRITE = "NBP-PATH-WRITE"              # df.write.parquet/csv/json/orc/text/save/option("path") literal outside prefix
RULE_PATH_WRITE_DYNAMIC = "NBP-PATH-WRITE-DYNAMIC"
RULE_RUNTIME_PATH = "NBP-RUNTIME-PATH"          # compat helper asked to write outside the prefix
RULE_RUNTIME_TABLE = "NBP-RUNTIME-TABLE"        # compat helper asked to write outside catalog.schema
RULE_RUNTIME_SECRET = "NBP-RUNTIME-SECRET"      # dbutils.secrets scope/key outside the planned allowlist
RULE_DUNDER_ESCAPE = "NBP-DUNDER-ESCAPE"        # __subclasses__ / __globals__ / __dict__ / frame attributes (f_globals, tb_frame)
RULE_IPYTHON_ESCAPE = "NBP-IPYTHON-ESCAPE"      # get_ipython / InteractiveShell / IPython imports / %magic lines inside a cell body
RULE_DESERIALIZE = "NBP-DESERIALIZE"            # pickle / marshal / shelve / dill / joblib loads, yaml.load without SafeLoader, torch.load

ALL_RULES = (
    RULE_IMPORT_PROCESS, RULE_IMPORT_NETWORK, RULE_OS_ENVIRON, RULE_OS_PROCESS,
    RULE_SHUTIL_RMTREE, RULE_BUILTIN_EXEC, RULE_INDIRECT, RULE_POLICY_TAMPER,
    RULE_OPEN_PATH, RULE_OPEN_DYNAMIC, RULE_FS_PATH, RULE_FS_DYNAMIC,
    RULE_DBFS_PATH, RULE_DBFS_DYNAMIC, RULE_TABLE_TARGET, RULE_TABLE_DYNAMIC,
    RULE_SQL_TARGET, RULE_SQL_DYNAMIC, RULE_SQL_REFUSED, RULE_PATH_WRITE, RULE_PATH_WRITE_DYNAMIC,
    RULE_RUNTIME_PATH, RULE_RUNTIME_TABLE, RULE_RUNTIME_SECRET,
    RULE_DUNDER_ESCAPE, RULE_IPYTHON_ESCAPE, RULE_DESERIALIZE,
)

_PROCESS_MODULES = frozenset({
    "subprocess", "ctypes", "pty", "multiprocessing", "cffi", "signal", "resource",
    "webbrowser", "antigravity", "pexpect", "sh", "plumbum", "fabric", "invoke", "psutil",
    "_posixsubprocess", "pydoc",
})
_NETWORK_MODULES = frozenset({
    "socket", "requests", "urllib", "paramiko", "http.client",
    "urllib3", "httpx", "aiohttp", "ftplib", "smtplib", "pycurl", "telnetlib",
    "poplib", "imaplib", "nntplib", "websocket", "websockets", "grpc", "xmlrpc",
    "socketserver", "ssl", "asyncore", "asynchat",
    # Servers bind a listening socket: the stdlib HTTP / WSGI / SMTP servers and
    # the common Python web servers and app frameworks that start one.
    "http.server", "wsgiref.simple_server", "smtpd",
    "flask", "fastapi", "starlette", "uvicorn", "gunicorn", "hypercorn", "tornado", "werkzeug", "bottle",
    "cherrypy", "waitress", "twisted", "sanic", "gradio", "streamlit", "dash", "zmq",
})
# The dotted entries above (``http.client``, ``http.server``, ``wsgiref.simple_server``):
# ``import http`` is harmless (``http.HTTPStatus``) but the ``http.client`` /
# ``http.server`` chain under it, ``from http import server`` and
# ``import http.server`` reach the same module and are judged by the same rule.
_NETWORK_SUBMODULES = frozenset(tuple(m.split(".")) for m in _NETWORK_MODULES if "." in m)
_INDIRECT_MODULES = frozenset({"importlib", "builtins", "pkgutil", "zipimport", "imp", "faulthandler"})
# gc.collect() is routine notebook hygiene; the object-graph walkers reach every live object.
_GC_INDIRECT_ATTRS = frozenset({"get_objects", "get_referrers", "get_referents"})
# Modules whose purpose is to run code handed to them as a string / docstring / frame.
_EXEC_MODULES = frozenset({"pdb", "bdb", "code", "codeop", "runpy", "doctest", "timeit", "cProfile",
                           "profile", "trace"})
# Modules that load code from a path while processing it: ``site.addsitedir``
# executes every ``.pth`` file in the directory, ``pip`` installs and imports
# arbitrary distributions, ``sitecustomize`` / ``usercustomize`` run on import.
# The same class as ``sc.addPyFile`` / ``addJar``. (``sys.path.insert`` is NOT in
# this class: the migrator itself puts its module mirror on ``sys.path``.)
_CODE_LOAD_MODULES = frozenset({"site", "pip", "ensurepip", "sitecustomize", "usercustomize"})
# ``site`` is a plausible variable name (``site = row["site"]``), so only its
# code-loading attributes are refused when the import is out of view; the
# other code-load modules are refused at any attribute.
_SITE_CODE_LOAD_ATTRS = frozenset({"addsitedir", "addpackage", "execsitecustomize", "execusercustomize"})
_CODE_LOAD_ROOT_REFUSED = _CODE_LOAD_MODULES - frozenset({"site"})
_JVM_BRIDGE_MODULES = frozenset({"py4j"})
_IPYTHON_MODULES = frozenset({"IPython", "ipykernel"})
_OS_ENV_ATTRS = frozenset({"environ", "getenv", "putenv", "unsetenv", "environb", "getenvb"})
_OS_PROCESS_ATTRS = frozenset({"system", "popen", "posix_spawn", "posix_spawnp", "startfile",
                               "fork", "forkpty", "kill", "killpg", "abort"})
_OS_PROCESS_PREFIXES = ("exec", "spawn")
# Process / environment / introspection sinks whose names have no benign
# homonym in notebook code. On a receiver the gate cannot resolve (a call
# result, a subscript, an unknown name) these are refused outright (fail
# closed); ``load`` / ``remove`` / ``mkdir`` / ``open`` are deliberately NOT
# here (``list.remove``, ``Path.mkdir``, ``loader.load`` are everyday code).
_SINK_ATTRS = frozenset({
    "system", "popen", "posix_spawn", "posix_spawnp", "startfile", "fork", "forkpty", "putenv", "unsetenv",
    "execl", "execle", "execlp", "execlpe", "execv", "execve", "execvp", "execvpe",
    "spawnl", "spawnle", "spawnlp", "spawnlpe", "spawnv", "spawnve", "spawnvp", "spawnvpe",
    "pipepager", "tempfilepager", "getattr_static", "getmembers", "get_stack",
})
# A plain ``import m`` of a module the gate has no rules for does not vouch
# for its attributes: ``mymod.system(..)`` may be ``os`` re-exported or
# shadowed, so a sink name on such a module is refused too. The stdlib
# homonyms with a documented benign meaning are the only exception.
_SINK_HOMONYMS = frozenset({("platform", "system")})
# Methods that mutate a registry (dict / list) in place. On a chain rooted at
# a gated module (``yaml.SafeLoader.yaml_constructors.update(..)``) they
# rewrite the module's state the gate reasons about and are refused.
_REGISTRY_MUTATORS = frozenset({"update", "setdefault", "pop", "popitem", "clear", "append", "insert", "extend",
                                "__setitem__", "__delitem__", "__ior__", "__iadd__"})
# ``sys.path`` is the one gated-module list notebook code may mutate: the
# migrator itself puts its module mirror on ``sys.path`` (``sys.path.insert``).
_MUTABLE_GATED_CHAINS = frozenset({("sys", "path")})
# ``torch.serialization`` hooks that widen what ``weights_only=True`` may unpickle.
_TORCH_HOOK_ATTRS = frozenset({"add_safe_globals", "safe_globals", "clear_safe_globals"})
# Callees that may take a gated module / Spark object as an argument: the
# call is judged (getattr / setattr / delattr / vars) or cannot hand the
# object back (print, repr, type, isinstance, ...).
_VALUE_SAFE_CALLEES = frozenset({"getattr", "setattr", "delattr", "hasattr", "vars", "dir", "isinstance",
                                 "issubclass", "type", "id", "repr", "str", "print", "help", "callable",
                                 "display"})
# String-keyed attribute accessors: judged like getattr with a literal name.
_ATTR_ACCESSOR_CALLS = frozenset({("operator", "attrgetter"), ("operator", "methodcaller")})
_ATTR_RESOLVING_NAMES = frozenset({"getattr", "setattr", "delattr", "vars"})
# RDD-producing steps: ``.pipe(cmd)`` on such a chain runs a shell command on the executors.
_RDD_CHAIN_ATTRS = frozenset({"parallelize", "rdd", "emptyRDD", "textFile", "wholeTextFiles", "binaryFiles",
                              "binaryRecords", "sequenceFile", "hadoopFile", "newAPIHadoopFile", "pickleFile"})
# The platform modules ``os`` re-exports: ``posix.system`` is ``os.system``. An
# import of either is tracked as an alias of ``os`` so every os rule applies.
_OS_EQUIVALENT_MODULES = frozenset({"posix", "nt"})
_ASYNCIO_PROCESS_ATTRS = frozenset({"create_subprocess_shell", "create_subprocess_exec", "subprocess"})
# The same capability on an event-loop object (``loop.subprocess_exec``); the
# loop has no module root to key on, so the method names are refused anywhere.
_ASYNCIO_LOOP_PROCESS_ATTRS = frozenset({"subprocess_exec", "subprocess_shell"})
_ASYNCIO_NETWORK_ATTRS = frozenset({"open_connection", "start_server", "create_connection",
                                    "open_unix_connection", "start_unix_server", "create_datagram_endpoint"})
_SYS_INDIRECT_ATTRS = frozenset({"modules", "settrace", "setprofile", "addaudithook", "call_tracing",
                                 "meta_path", "path_hooks", "path_importer_cache"})
_INSPECT_FRAME_ATTRS = frozenset({"currentframe", "stack", "trace", "getouterframes", "getinnerframes"})
# ``traceback`` helpers that hand live frame objects out (``print_exc`` /
# ``format_exc`` / ``extract_stack`` return text / summaries and stay allowed);
# ``capture_locals=True`` on any traceback helper copies every frame's locals.
_TRACEBACK_FRAME_ATTRS = frozenset({"walk_stack", "walk_tb"})
_TYPES_EXEC_ATTRS = frozenset({"FunctionType", "CodeType", "LambdaType"})
# ``concurrent.futures.ThreadPoolExecutor`` is fine; the process pool forks workers.
_PROCESS_POOL_NAMES = frozenset({"ProcessPoolExecutor"})
# Stdlib primitives equivalent to builtin open(): (module, callable) -> first argument is the path.
_IO_OPEN_CALLS = frozenset({
    ("io", "open"), ("io", "FileIO"), ("io", "open_code"), ("os", "open"), ("codecs", "open"),
    ("gzip", "open"), ("gzip", "GzipFile"), ("bz2", "open"), ("bz2", "BZ2File"),
    ("lzma", "open"), ("lzma", "LZMAFile"), ("tarfile", "open"), ("tarfile", "TarFile"),
    ("zipfile", "ZipFile"), ("tokenize", "open"), ("fileinput", "input"), ("fileinput", "FileInput"),
    ("sqlite3", "connect"), ("dbm", "open"), ("linecache", "getline"), ("linecache", "getlines"),
})
_IO_MODULES = frozenset(m for m, _ in _IO_OPEN_CALLS)
# Deserialisers that construct arbitrary objects (and therefore run code) from bytes.
# The gate refuses the *load* side only; pickle.dump / safe_io.safe_pickle_dump stay allowed.
_PICKLE_MODULES = frozenset({"pickle", "_pickle", "cPickle", "dill", "cloudpickle"})
# The pickle family is deny-by-default: only the dump side is allowed.
_PICKLE_DUMP_ALLOW = frozenset({"dump", "dumps", "Pickler", "HIGHEST_PROTOCOL", "DEFAULT_PROTOCOL",
                                "PickleError", "PicklingError", "UnpicklingError", "PickleBuffer"})
_DESERIALIZE_ATTRS = {
    "marshal": frozenset({"load", "loads"}),
    "shelve": frozenset({"open", "Shelf", "DbfilenameShelf", "BsdDbShelf"}),
    "joblib": frozenset({"load"}),
    "jsonpickle": frozenset({"decode", "loads"}),
}
# yaml: refused outright at any depth. ``load`` / ``load_all`` are judged at the
# call (``Loader=`` must resolve to a safe loader; ``load_all`` is refused).
_YAML_REFUSED_ATTRS = frozenset({"unsafe_load", "unsafe_load_all", "full_load", "full_load_all", "load_all",
                                 "Loader", "UnsafeLoader", "FullLoader", "Constructor", "FullConstructor",
                                 "UnsafeConstructor", "loader", "constructor", "add_constructor",
                                 "add_multi_constructor"})
_YAML_REFUSED_PREFIXES = ("Full", "Unsafe")
# ``add_constructor`` on any receiver registers code that runs while parsing.
_YAML_HOOK_ATTRS = frozenset({"add_constructor", "add_multi_constructor"})
_DESERIALIZE_MODULES = _PICKLE_MODULES | frozenset(_DESERIALIZE_ATTRS) | frozenset({"torch", "yaml"})
_YAML_SAFE_LOADERS = frozenset({"SafeLoader", "CSafeLoader", "BaseLoader", "CBaseLoader"})
# Star imports that would bind the gated API under unqualified names the scanner cannot see.
_STAR_IMPORT_REFUSED = frozenset({"sys", "shutil", "inspect", "types", "asyncio", "concurrent"}) \
    | _IO_MODULES | _DESERIALIZE_MODULES | _INDIRECT_MODULES | _EXEC_MODULES | _CODE_LOAD_MODULES
_OS_FS_ATTRS = frozenset({"remove", "unlink", "rmdir", "removedirs", "rename", "renames", "replace",
                          "truncate", "mkdir", "makedirs", "link", "symlink"})
_OS_FS_TWO_ARG = frozenset({"rename", "renames", "replace", "link", "symlink"})
_SHUTIL_FS_ATTRS = frozenset({"move", "copy", "copy2", "copyfile", "copytree"})
_PATHLIB_IO_ATTRS = frozenset({"read_text", "read_bytes", "write_text", "write_bytes"})
_PATHLIB_FS_ATTRS = frozenset({"unlink", "rmdir", "mkdir", "touch", "symlink_to", "hardlink_to"})
_PATHLIB_RESOLVED_ONLY_ATTRS = frozenset({"open", "rename", "replace"})   # too generic without a Path receiver
# The pathlib classes, the steps that keep the path a Path(..) was built from
# (judged on that literal), the steps that derive another Path from one (an
# unresolvable Path: fail closed) and the Path-valued classmethods.
_PATH_CLASSES = frozenset({"Path", "PurePath", "PosixPath", "WindowsPath", "PurePosixPath", "PureWindowsPath"})
_PATH_IDENTITY_STEPS = frozenset({"resolve", "absolute", "expanduser"})
_PATH_DERIVED_STEPS = frozenset({"parent", "parents", "joinpath", "with_name", "with_suffix", "with_stem",
                                 "with_segments", "relative_to", "readlink", "rename", "replace",
                                 "iterdir", "glob", "rglob", "walk"})
_PATH_CLASS_CTORS = frozenset({"cwd", "home"})
_DYNAMIC_BUILTINS = frozenset({"eval", "exec", "compile", "__import__", "breakpoint"})
_INTROSPECT_BUILTINS = frozenset({"getattr", "vars"})
_MUTATE_BUILTINS = frozenset({"setattr", "delattr"})
_NAMESPACE_BUILTINS = frozenset({"globals", "locals", "vars"})    # no-arg call = the frame namespace
_BUILTINS_NAMESPACE_ROOTS = frozenset({"__builtins__", "builtins"})
# Builtins the gate reasons about. Reaching one of them through the builtins
# namespace, an alias or a bare reference (``map(open, ...)``) is checked
# exactly like the direct call, or refused when it cannot be checked.
_DANGEROUS_BUILTINS = _DYNAMIC_BUILTINS | _INTROSPECT_BUILTINS | _MUTATE_BUILTINS | _NAMESPACE_BUILTINS \
    | frozenset({"open"})
# Dunder attributes notebook code may read. Everything else (``__subclasses__``,
# ``__globals__``, ``__dict__``, ``__code__``, ``__reduce__``, ...) is an
# introspection chain out of the sandbox. Dunder *definitions* in class
# bodies are untouched: this list governs attribute access only.
_DUNDER_ALLOW = frozenset({
    "__name__", "__doc__", "__version__", "__class__", "__module__", "__qualname__", "__all__",
    "__annotations__", "__slots__", "__fields__", "__array__",
    "__init__", "__repr__", "__str__", "__len__", "__iter__", "__next__", "__getitem__", "__setitem__",
    "__delitem__", "__contains__", "__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__", "__hash__",
    "__bool__", "__enter__", "__exit__", "__add__", "__radd__", "__sub__", "__rsub__", "__mul__", "__rmul__",
    "__matmul__", "__truediv__", "__floordiv__", "__mod__", "__pow__", "__neg__", "__pos__", "__abs__",
    "__and__", "__or__", "__xor__", "__invert__", "__lshift__", "__rshift__", "__round__", "__format__",
})
# Non-dunder attributes that walk frames, tracebacks, generators or code objects.
_FRAME_ATTRS = frozenset({
    "f_globals", "f_locals", "f_builtins", "f_back", "f_code", "f_trace", "tb_frame", "tb_next",
    "gi_frame", "gi_code", "gi_yieldfrom", "cr_frame", "cr_code", "ag_frame", "ag_code",
    "co_code", "co_consts", "co_names", "func_globals", "func_code", "im_func", "im_self", "mro",
})
# Spark session / context objects and the private py4j handles that lead to the JVM.
_SPARK_ROOTS = frozenset({"spark", "sc", "sqlContext", "sqlCtx", "SparkSession", "SparkContext",
                          "SQLContext", "HiveContext", "pyspark"})
_JVM_BRIDGE_ATTRS = frozenset({
    "_jvm", "_gateway", "_jsc", "_jsparkSession", "_jvm_gateway", "_jconf", "_jdf", "_jrdd", "_jc",
    "_jwrite", "_jreader", "_jspark", "_jsqlContext", "_jcatalog", "_sc", "_gateway_client", "_java_gateway",
})
_SPARK_CODE_LOAD_ATTRS = frozenset({"addPyFile", "addJar", "addArchive", "addFile"})
_SPARK_JVM_UDF_ATTRS = frozenset({"registerJavaFunction", "registerJavaUDAF"})
# A fresh / fetched session object would not carry the instance-level runtime SQL
# guard that dbutils.notebook.run installs on the session it finds.
_SPARK_SESSION_FACTORY_ATTRS = frozenset({"newSession", "getOrCreate", "getActiveSession"})
# Public alias for the runtime SQL guard: the names under which a session object
# is looked up in the executing namespace.
SPARK_ROOT_NAMES = _SPARK_ROOTS
_PY4J_NAMES = frozenset({"JavaGateway", "GatewayClient", "GatewayParameters", "java_import", "JavaObject"})
_IPYTHON_NAMES = frozenset({"get_ipython", "InteractiveShell", "TerminalInteractiveShell", "ZMQInteractiveShell"})
# get_ipython().<attr>: shell / magic execution is reported as process execution,
# every other use of the shell object as an IPython escape.
_IPYTHON_SHELL_ATTRS = frozenset({"system", "system_raw", "system_piped", "getoutput",
                                  "run_line_magic", "run_cell_magic", "magic"})
_SHELL_MAGICS = frozenset({"sh", "bash", "sx", "system", "script", "pip", "conda", "cmd", "powershell",
                           "perl", "ruby", "scala", "r", "fs", "env", "set_env", "killbgscripts"})
_MAGIC_LINE_RE = re.compile(r"^\s*(%%?|!)\s*([A-Za-z_][\w.]*)?")
_DBFS_WRITE_METHODS = frozenset({"rm", "mv", "cp", "put", "mkdirs"})
_TABLE_WRITE_METHODS = frozenset({"saveAsTable", "insertInto", "writeTo", "toTable"})
_PATH_WRITE_METHODS = frozenset({"parquet", "csv", "json", "orc", "text", "save"})
# ``DataStreamWriter.start(path)`` is the streaming terminal; ``start`` is a path
# write only under a ``writeStream`` chain (``thread.start()`` is everyday code).
_STREAM_PATH_WRITE_METHODS = frozenset({"start"})
_WRITER_OPTION_METHODS = frozenset({"option", "options"})
# Writer options that name a location the write creates files under.
_WRITER_PATH_OPTIONS = frozenset({"path", "checkpointLocation"})
# ``df.write`` and ``df.writeStream`` are both writers: the path / table / option
# rules apply under either entry (``writeTo`` is judged at its own call).
_SPARK_WRITER_CHAIN = frozenset({"write", "writeStream"})
_SQL_METHODS = frozenset({"sql"})
# Local file READS through library readers. A LITERAL local-filesystem path
# outside the sandbox prefix is refused (NBP-OPEN-PATH, like open()); a path
# the gate cannot resolve is allowed (reading source data through variables is
# the normal migration case) and recorded as ``unverified_read_path``.
# Object-storage URIs (oci://, abfss://, s3://, ...) and catalog tables are
# not local paths: reading source data is the point of a migration and the
# cluster's IAM scope is the control there.
_DATAFRAME_LIB_ROOTS = frozenset({"pandas", "polars", "modin", "dask", "cudf", "geopandas", "xarray", "pyarrow",
                                  "vaex", "datatable"})
_DATAFRAME_READ_PREFIXES = ("read_", "scan_", "open_")
_DATAFRAME_NON_PATH_READERS = frozenset({"read_sql", "read_sql_query", "read_sql_table", "read_clipboard", "read_gbq"})
_LIB_READER_CALLS = frozenset({
    ("numpy", "loadtxt"), ("numpy", "genfromtxt"), ("numpy", "fromfile"), ("numpy", "load"), ("numpy", "memmap"),
    ("pyarrow", "memory_map"), ("pyarrow", "OSFile"), ("pyarrow", "input_stream"),
    ("pyarrow", "parquet", "ParquetFile"), ("pyarrow", "parquet", "ParquetDataset"), ("pyarrow", "dataset", "dataset"),
    ("PIL", "Image", "open"), ("cv2", "imread"), ("h5py", "File"), ("scipy", "io", "loadmat"),
    ("xml", "etree", "ElementTree", "parse"), ("openpyxl", "load_workbook"), ("xlrd", "open_workbook"),
    ("imageio", "imread"), ("tifffile", "imread"), ("netCDF4", "Dataset"), ("rasterio", "open"), ("zarr", "open"),
    ("librosa", "load"), ("soundfile", "read"),
})
_SPARK_READER_CHAIN = frozenset({"read", "readStream"})
_SPARK_IO_CHAIN = _SPARK_READER_CHAIN | frozenset({"write", "writeStream", "writeTo"})
# DataFrameReader / DataFrameWriter / DataFrameWriterV2 / DataStreamWriter builder
# methods: ``df.write.mode(..).option(..)`` still names the writer, so a name bound
# to such a chain (``wr = df.write.mode('overwrite')``, ``rd = spark.read.format('csv')``,
# ``wr = df.coalesce(1).write``) is followed as an alias of it and the terminal call
# on the name (``wr.parquet(p)``, ``wr.saveAsTable(t)``, ``rd.load(p)``) is judged
# like the single-statement chain. A terminal (``parquet`` / ``save`` / ``load`` /
# ``start`` / ``saveAsTable``) ends the builder: its result is a DataFrame / None.
_SPARK_IO_BUILDER_METHODS = frozenset({
    "mode", "format", "option", "options", "partitionBy", "bucketBy", "sortBy", "schema", "using",
    "tableProperty", "partitionedBy", "clusterBy", "outputMode", "trigger", "queryName", "foreach", "foreachBatch",
})
_SPARK_PATH_READ_METHODS = frozenset({"parquet", "csv", "json", "orc", "text", "load", "textFile", "xml", "avro"})
_SC_PATH_READ_METHODS = frozenset({"textFile", "wholeTextFiles", "binaryFiles", "binaryRecords", "sequenceFile",
                                   "hadoopFile", "newAPIHadoopFile", "pickleFile"})
_READER_PATH_KWARGS = ("path", "paths", "filepath_or_buffer", "path_or_buf", "io", "source", "file", "filename",
                       "fname", "name", "filename_or_obj", "fp")
# Local file WRITES through library writers: the mirror of the reader table.
# A literal local path outside the sandbox prefix is refused (NBP-PATH-WRITE,
# like ``df.write.parquet``) and so is a path the gate cannot resolve
# (NBP-PATH-WRITE-DYNAMIC): the read-side scope decision does not extend to
# writes, which fail closed at every sink. Relative literal paths are
# tolerated like ``open()``. Entry: chain -> (positional index of the path,
# keyword names). ``mlflow.<flavor>.save_model(obj, path)`` is matched by
# suffix because the flavour segment varies.
_LIB_WRITER_CALLS: Dict[Tuple[str, ...], Tuple[int, Tuple[str, ...]]] = {
    ("numpy", "save"): (0, ("file",)), ("numpy", "savez"): (0, ("file",)),
    ("numpy", "savez_compressed"): (0, ("file",)), ("numpy", "savetxt"): (0, ("fname",)),
    ("torch", "save"): (1, ("f",)), ("torch", "jit", "save"): (1, ("f",)), ("torch", "serialization", "save"): (1, ("f",)),
    ("joblib", "dump"): (1, ("filename",)),
    ("scipy", "io", "savemat"): (0, ("file_name",)), ("scipy", "io", "wavfile", "write"): (0, ("filename",)),
    ("cv2", "imwrite"): (0, ("filename",)),
    ("matplotlib", "pyplot", "savefig"): (0, ("fname",)), ("matplotlib", "pyplot", "imsave"): (0, ("fname",)),
    ("imageio", "imwrite"): (0, ("uri",)), ("imageio", "imsave"): (0, ("uri",)), ("imageio", "mimwrite"): (0, ("uri",)),
    ("imageio", "volwrite"): (0, ("uri",)),
    ("tifffile", "imwrite"): (0, ("file",)), ("tifffile", "imsave"): (0, ("file",)),
    ("soundfile", "write"): (0, ("file",)),
    ("pyarrow", "parquet", "write_table"): (1, ("where",)), ("pyarrow", "parquet", "write_to_dataset"): (1, ("root_path",)),
    ("pyarrow", "feather", "write_feather"): (1, ("dest",)), ("pyarrow", "csv", "write_csv"): (1, ("output_file",)),
    ("pyarrow", "output_stream"): (0, ("source",)),
    ("pandas", "ExcelWriter"): (0, ("path",)), ("pandas", "HDFStore"): (0, ("path",)),
    ("zarr", "save"): (0, ("store",)), ("zarr", "save_array"): (0, ("store",)), ("zarr", "save_group"): (0, ("store",)),
    ("plotly", "io", "write_image"): (1, ("file",)), ("plotly", "io", "write_html"): (1, ("file",)),
    ("plotly", "io", "write_json"): (1, ("file",)),
    ("dask", "dataframe", "to_parquet"): (1, ("path",)), ("dask", "dataframe", "to_csv"): (1, ("filename",)),
    ("xarray", "save_mfdataset"): (1, ("paths",)),
}
# Receiver-typed writers whose first argument is a path (or a buffer): pandas /
# polars / xarray / geopandas frame methods, matplotlib ``savefig``, numpy
# ``tofile``, plotly ``write_html`` / ``write_image``. The receiver's type is
# unknown but the method name is unambiguous: a literal local path outside the
# prefix is NBP-PATH-WRITE, a path the gate cannot resolve is
# NBP-PATH-WRITE-DYNAMIC unless it is a write handle the notebook bound
# (``buf = io.StringIO(); df.to_csv(buf)``, ``with pd.ExcelWriter(p) as w``) or
# ``sys.stdout``; no path argument (``df.to_csv()`` returns text) is fine.
_FRAME_WRITER_ATTRS = frozenset({
    "to_csv", "to_parquet", "to_pickle", "to_json", "to_hdf", "to_excel", "to_feather", "to_orc", "to_stata",
    "to_xml", "to_netcdf", "to_zarr", "to_file", "tofile", "savefig", "write_html", "write_image",
    "write_csv", "write_parquet", "write_json", "write_ndjson", "write_ipc", "write_avro", "write_excel",
    "write_delta",
})
_FRAME_WRITER_KWARGS = ("path_or_buf", "path", "fname", "file", "filename", "buf", "excel_writer", "path_or_buffer",
                        "store", "fid")
# ``.save(path)`` and its kin are too generic to type (an ORM row, a Keras /
# openpyxl / PIL / transformers object): a literal local path outside the prefix
# is refused, a path the gate cannot resolve is left alone.
_GENERIC_SAVE_ATTRS = frozenset({"save", "save_model", "save_pretrained", "save_weights", "export"})
_GENERIC_SAVE_KWARGS = ("path", "filename", "fname", "file", "filepath", "save_directory", "export_dir")
# Constructors of in-memory buffers / writer handles: a name bound once to one
# (assignment or ``with .. as name``) is a legitimate non-path writer argument.
_WRITE_HANDLE_CTORS = frozenset({"StringIO", "BytesIO", "ExcelWriter", "HDFStore"})
_STREAM_CHAINS = frozenset({("sys", "stdout"), ("sys", "stderr")})
# ``tempfile`` creators: attr -> (positional slot of ``prefix``, positional slot of ``dir``).
# A literal ``dir`` outside the sandbox creates files there; an absolute ``prefix``
# escapes ``dir`` because ``os.path.join`` discards the directory before it. The
# default directory and a ``dir`` the gate cannot resolve stay allowed.
_TEMPFILE_CREATORS = {
    "NamedTemporaryFile": (5, 6), "TemporaryFile": (5, 6), "SpooledTemporaryFile": (6, 7),
    "TemporaryDirectory": (1, 2), "mkstemp": (1, 2), "mkdtemp": (1, 2), "mktemp": (1, 2),
}
# str.format templates: methods that return the receiver's text unchanged in
# shape, and the stdlib helpers that do, so ``textwrap.dedent("...").format(x)``
# is checked as the literal.
_STR_PRESERVING_METHODS = frozenset({"strip", "lstrip", "rstrip", "upper", "lower", "title", "capitalize",
                                     "swapcase", "expandtabs", "casefold"})
_STR_PRESERVING_CALLS = frozenset({("textwrap", "dedent"), ("inspect", "cleandoc")})
# Informational policy-log events (not refusals, not rule ids).
NOTE_UNVERIFIED_READ_PATH = "unverified_read_path"      # library reader with a path the gate cannot resolve
NOTE_UNVERIFIED_SQL_READ = "unverified_sql_read"        # spark.sql read statement with run-time fragments
NOTE_UNVERIFIED_PIPE = "unverified_pipe_callable"       # <non-Spark>.pipe(name) with a name the gate cannot type
NOTE_UNVERIFIED_FORMAT = "unverified_format"            # spark.read/write.format(<dynamic>) while allow_network is off
POLICY_NOTES = (NOTE_UNVERIFIED_READ_PATH, NOTE_UNVERIFIED_SQL_READ, NOTE_UNVERIFIED_PIPE, NOTE_UNVERIFIED_FORMAT)

# Names whose attributes notebook code must not rebind or introspect: the
# interpreter and the compat shims the sandbox is built from.
_PROTECTED_ROOTS = frozenset({"os", "sys", "builtins", "__builtins__", "importlib", "shutil", "aidp_compat",
                              "dbutils", "spark", "sc", "get_ipython", "io", "pathlib", "inspect", "types", "gc"})
# Modules the gate reasons about: a name bound to one of them (``import os``,
# ``from pickle import loads``, ``import sys as s``) is "tainted" for the run.
# While it stays resolvable the alias is followed; once rebound to something
# the gate cannot resolve, every later use of the name is refused (fail closed).
_WATCHED_MODULES = _PROCESS_MODULES | _NETWORK_MODULES | _INDIRECT_MODULES | _EXEC_MODULES | _JVM_BRIDGE_MODULES \
    | _IPYTHON_MODULES | _IO_MODULES | _DESERIALIZE_MODULES | _PROTECTED_ROOTS | _OS_EQUIVALENT_MODULES \
    | _CODE_LOAD_MODULES \
    | frozenset({"asyncio", "concurrent", "yaml", "aidp_compat", "operator", "functools"})
# A chain segment that names one of these after any root re-roots the chain
# there (``shutil.os.system``, ``pd.io.common.os.environ``, ``self.spark._jvm``):
# the module rules apply from that segment on. Modules whose names are common
# attribute names with no module rules of their own (``signal``, ``code``,
# ``trace``, ``profile``, ``resource``, ``types``, ...) are left out.
_REROOT_NAMES = frozenset({
    "os", "posix", "nt", "sys", "shutil", "subprocess", "importlib", "builtins", "__builtins__", "ctypes",
    "socket", "asyncio", "operator", "inspect", "functools", "pydoc", "io", "pathlib", "gc", "marshal", "shelve",
    "yaml", "torch", "aidp_compat", "dbutils", "get_ipython", "faulthandler",
}) | _PICKLE_MODULES | frozenset({"joblib", "jsonpickle"}) | (_SPARK_ROOTS - frozenset({"pyspark"}))
# Process modules whose name is not a plausible variable name: an attribute
# chain rooted there is refused even when the import is out of view.
_PROCESS_ROOT_REFUSED = _PROCESS_MODULES - frozenset({"signal", "resource", "sh", "invoke", "fabric", "plumbum",
                                                      "antigravity"})
# Names that must not be handed around as a value (list / dict / tuple element,
# argument, default, return value, lambda body, conditional, walrus, ...): a
# gated module, a protected root or a Spark object. The tolerated positions
# are the root of an attribute chain the gate judges, the whole right-hand
# side of a plain name assignment (tainted) and the object argument of the
# introspection builtins (judged).
_VALUE_GUARDED = _WATCHED_MODULES | _SPARK_ROOTS | _PROTECTED_ROOTS
# Single names that may be aliased (``sp = spark``, ``f = getattr``, ``b = __builtins__``)
# and that taint the alias for the rest of the run.
_ALIASABLE_SINGLETONS = _PROTECTED_ROOTS | _SPARK_ROOTS | _DANGEROUS_BUILTINS | _PY4J_NAMES | _IPYTHON_NAMES \
    | _PROCESS_POOL_NAMES | _WATCHED_MODULES
_COMPAT_INTERNAL_MODULES = frozenset({"notebook_policy", "fs", "safe_io", "secrets", "notebook",
                                      "dbutils_shim", "credentials"})
_COMPAT_PROTECTED_NAMES = frozenset({
    "SandboxPolicy", "set_sandbox_policy", "clear_policy_log", "require_policy", "current_policy",
    "assert_path_in_sandbox", "assert_table_in_sandbox", "enforce_cell_policy", "scan_source",
    "AIDPNotebookUtils", "AIDPFileSystemUtils", "AIDPSecretsUtils",
}) | _COMPAT_INTERNAL_MODULES

# ── SQL allowlist (see classify_sql) ───────────────────────────────────
# ``spark.sql`` / ``sql`` statements are judged by an ALLOWLIST, not by a list
# of write verbs: a statement is accepted only when its top-level verb is a
# known read form, or a known write form whose target is inside the sandbox.
# Keyword analysis runs on a MASKED copy of the statement (comments removed,
# every string literal and backtick identifier replaced by a numbered
# placeholder) so quoted text cannot steer it; the placeholders are looked up
# where a quoted value matters (paths, table names).
_SQL_IDENT = r"((?:`[^`]+`|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:`[^`]+`|[A-Za-z_][\w$]*)){0,2})"
_SQL_IDENT_RE = re.compile(r"\s*" + _SQL_IDENT)
_SQL_WORD_RE = re.compile(r"[A-Za-z_][\w$]*")
_SQL_STR_MARK = "\x00"
_SQL_IDENT_MARK = "\x01"
_SQL_STR_PLACEHOLDER_RE = re.compile("'\x00(\\d+)\x00'")
_SQL_IDENT_PLACEHOLDER_RE = re.compile("`\x01(\\d+)\x01`")
# Verbs that read: allowed without a target check.
_SQL_READ_VERBS = frozenset({"SELECT", "VALUES", "TABLE", "SHOW", "DESCRIBE", "DESC", "CACHE", "UNCACHE"})
# A dynamic statement (run-time fragments) is allowed only when its literal
# head verb is one of these AND the runtime SQL guard is active for the run.
_SQL_DYNAMIC_READ_VERBS = _SQL_READ_VERBS | frozenset({"REFRESH", "CLEAR", "EXPLAIN"})
_SQL_EXPLAIN_MODES = frozenset({"EXTENDED", "CODEGEN", "COST", "FORMATTED", "ANALYZE", "LOGICAL"})
_SQL_CREATE_FLAGS = frozenset({"GLOBAL", "TEMP", "TEMPORARY", "EXTERNAL", "MATERIALIZED", "STREAMING", "LIVE"})
_SQL_TEMP_FLAGS = frozenset({"TEMP", "TEMPORARY"})
_SQL_TABLE_OBJECTS = frozenset({"TABLE", "VIEW"})
_SQL_SCHEMA_OBJECTS = frozenset({"SCHEMA", "DATABASE", "NAMESPACE"})
_SQL_CATALOG_OBJECTS = _SQL_TABLE_OBJECTS | _SQL_SCHEMA_OBJECTS
# CREATE [OR REPLACE] [GLOBAL] TEMP[ORARY] VIEW: the literal head of a dynamic statement.
_SQL_TEMP_VIEW_HEAD_RE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:GLOBAL\s+)?TEMP(?:ORARY)?\s+VIEW\b", re.IGNORECASE
)
# Refused ANYWHERE in a statement (CTEs, subqueries, expressions), case-insensitively:
# the Java-call functions and the Hive script transform (TRANSFORM (..) [ROW FORMAT ..]
# USING 'cmd' runs a shell command; in Spark SQL a string literal after USING occurs
# nowhere else). A function name may be spelt as a backtick-quoted identifier
# (`reflect`(..)), so the masked placeholders followed by ``(`` are resolved too.
_SQL_JVM_FUNCTIONS = frozenset({"reflect", "java_method", "try_reflect"})
_SQL_JVM_CALL_RE = re.compile(r"\b(reflect|java_method|try_reflect)\s*\(", re.IGNORECASE)
_SQL_QUOTED_CALL_RE = re.compile("`" + _SQL_IDENT_MARK + r"(\d+)" + _SQL_IDENT_MARK + r"`\s*\(")
_SQL_QUOTED_JVM_CALL_RAW_RE = re.compile(r"`\s*(reflect|java_method|try_reflect)\s*`\s*\(", re.IGNORECASE)
_SQL_SCRIPT_TRANSFORM_RE = re.compile(r"\bUSING\s+'\x00\d+\x00'", re.IGNORECASE)
_SQL_SCRIPT_TRANSFORM_RAW_RE = re.compile(r"\bUSING\s+['\"]", re.IGNORECASE)
# IDENTIFIER(<expr>) resolves a table / column / function name at run time (also from
# a parameter marker bound through args=): the gate cannot see what it names.
_SQL_IDENTIFIER_CLAUSE_RE = re.compile(r"\bIDENTIFIER\s*\(", re.IGNORECASE)
# <provider>.`<path>` path tables. Spark resolves EVERY 2-part name whose first part
# is a data-source short name (text / csv / delta / .., quoted or not) or a provider
# class (org.apache.spark.sql.execution.datasources.text.TextFileFormat) as a path
# table, so the rule is structural: any 1-part or dotted first part followed by a
# backtick identifier that is a path (see _sql_ident_is_path), whatever the first part.
_SQL_PART = r"(?:`\x01\d+\x01`|[A-Za-z_][\w$]*)"
_SQL_PATH_TABLE_RE = re.compile(_SQL_PART + r"(?:\s*\.\s*" + _SQL_PART + r")*\s*\.\s*`\x01(\d+)\x01`")
# A backtick identifier that starts like a local path, in a raw fragment of a dynamic
# statement (the fragment may begin with the closing backtick of a run-time provider:
# f"SELECT * FROM `{fmt}`.`/etc/passwd`" -> "`.`/etc/passwd`").
_SQL_BACKTICK_PATH_RAW_RE = re.compile(r"`\s*((?:/|~|[A-Za-z]:[\\/]|\\\\|(?:file|dbfs|hdfs|local):/)[^`]*)`",
                                       re.IGNORECASE)
_SQL_PATH_TABLE_NAME_RE = re.compile(r"^(.+?)\s*\.\s*`(.+)`$", re.DOTALL)
# LOCATION '<path>' (a write location) and COPY INTO .. FROM '<path>' (a read source;
# anchored on the COPY verb, so TRIM(LEADING '/' FROM '/tmp/x') and the other
# <function> .. FROM '<string>' forms are not path sources), on the masked text.
_SQL_LOCATION_RE = re.compile(r"\bLOCATION\s+'\x00(\d+)\x00'", re.IGNORECASE)
_SQL_COPY_FROM_RE = re.compile(r"\bFROM\s+\(?\s*'\x00(\d+)\x00'", re.IGNORECASE)
_SQL_OPTIONS_RE = re.compile(r"\bOPTIONS\s*\(([^()]*)\)", re.IGNORECASE)
_SQL_OPTION_ITEM_RE = re.compile(r"\s*(`\x01\d+\x01`|'\x00\d+\x00'|[\w.]+)\s*=?\s*('\x00(\d+)\x00')")
# CREATE .. USING <provider>: the data source behind a table / temp view.
_SQL_USING_PROVIDER_RE = re.compile(r"\bUSING\s+(`\x01\d+\x01`|[A-Za-z_][\w.$]*)", re.IGNORECASE)
# ALTER TABLE / VIEW <name> RENAME TO <name>: the destination is a write target too
# (RENAME TO PARTITION (..) renames a partition in place).
_SQL_RENAME_TO_RE = re.compile(r"\bRENAME\s+TO\s+(?!PARTITION\b)", re.IGNORECASE)
# Set operators / clauses that may follow a parenthesised query term.
_SQL_AFTER_PAREN_QUERY = frozenset({"UNION", "INTERSECT", "EXCEPT", "MINUS", "ORDER", "LIMIT", "SORT", "CLUSTER",
                                    "DISTRIBUTE", "WINDOW", "OFFSET"})
# DESCRIBE DETAIL / HISTORY '<path>': Delta metadata of a path.
_SQL_DESCRIBE_PATH_WORDS = frozenset({"DETAIL", "HISTORY"})
# Data-source providers that open a network connection (``spark.read.format("jdbc")``,
# ``CREATE TEMP VIEW v USING kafka``, ``spark.readStream.format("socket")``) and are
# refused while ``allow_network`` is False, like the network imports; a dotted provider
# class is refused as well (it may open anything). The platform's own
# ``aidataplatform`` connector is not on the list: its connections are catalog
# objects governed by the platform, not by the notebook.
_NETWORK_PROVIDERS = frozenset({
    "jdbc", "kafka", "socket", "mongodb", "mongo", "cassandra", "redshift", "snowflake", "bigquery", "sqlserver",
    "mysql", "postgresql", "postgres", "oracle", "elasticsearch", "es", "kinesis", "eventhubs", "pulsar", "redis",
    "salesforce", "http", "https", "rest", "neo4j", "couchbase", "hbase", "solr", "ftp", "sftp", "cosmos.oltp",
})
_SPARK_NETWORK_IO_METHODS = frozenset({"jdbc"})
_SPARK_PROVIDER_METHODS = frozenset({"format", "using"})
# Python twins of SQL session-state and DDL forms: USE (spark.catalog.setCurrentCatalog /
# setCurrentDatabase), SET (spark.conf.set / unset, sqlContext.setConf) and CREATE
# TABLE .. LOCATION / USING (spark.catalog.createTable / createExternalTable).
_SPARK_CATALOG_USE_ATTRS = frozenset({"setCurrentCatalog", "setCurrentDatabase"})
_SPARK_CATALOG_CREATE_ATTRS = frozenset({"createTable", "createExternalTable"})
_SPARK_CONF_SET_ATTRS = frozenset({"set", "unset"})
# pyspark.sql.functions entry points that call arbitrary Java with no SQL string
# (reflect / java_method / try_reflect; call_function("reflect", ..) by name).
_SPARK_JVM_FUNCTION_ATTRS = frozenset(_SQL_JVM_FUNCTIONS)
_SPARK_CALL_FUNCTION_ATTRS = frozenset({"call_function"})
# Table reads (spark.table / spark.read.table / spark.readStream.table): a path table
# literal is judged like the SQL form.
_SPARK_TABLE_READ_ATTRS = frozenset({"table"})
# SET / RESET: a short allowlist of harmless performance / session confs; anything
# that smells of credentials, the filesystem, the JVM or the Databricks runtime is
# refused whatever the allowlist says.
_SQL_SET_ALLOWED_CONFS = frozenset({
    "spark.sql.shuffle.partitions", "spark.sql.autobroadcastjointhreshold", "spark.sql.session.timezone",
    "spark.sql.ansi.enabled", "spark.sql.casesensitive", "spark.sql.legacy.timeparserpolicy",
    "spark.sql.sources.partitionoverwritemode", "spark.sql.broadcasttimeout",
    "spark.sql.files.maxpartitionbytes", "spark.sql.parquet.compression.codec",
})
_SQL_SET_ALLOWED_CONF_PREFIXES = ("spark.sql.adaptive.",)
_SQL_SET_DENY_RE = re.compile(
    r"credential|secret|key|token|password|hadoop|fs\.|spark\.databricks|spark\.driver|spark\.executor|"
    r"extraJavaOptions|extraClassPath|spark\.sql\.extensions", re.IGNORECASE
)
_SQL_SET_TIME_ZONE_RE = re.compile(r"^SET\s+TIME\s+ZONE\b", re.IGNORECASE)
_SQL_SET_KEY_RE = re.compile(r"^(SET|RESET)\s+(?!VAR\b|VARIABLE\b)([\w.\-]+)\s*(?:=|$)", re.IGNORECASE | re.DOTALL)
_URI_RE = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*)://([^/\\]*)([/\\].*)?$", re.DOTALL)
_SEGMENT_SPLIT_RE = re.compile(r"[\\/]+")
# Host-absolute forms regardless of the host this module runs on: a Windows
# drive (``C:\``, ``C:/``), a UNC path (``\\server\share``) and a ``file:`` URI.
_WINDOWS_PATH_RE = re.compile(r"^(?:[A-Za-z]:[\\/]|\\\\)")
# ``file:`` plus the cluster-filesystem schemes ``dbfs:`` (the FUSE mount ``/dbfs``),
# ``hdfs:`` and ``local:``: every one of them names the cluster, not object storage.
_LOCAL_URI_RE = re.compile(r"^(file|dbfs|hdfs|local):/+", re.IGNORECASE)
# ``{0.attr[key].attr}``: the attribute / index steps after the argument name.
_FORMAT_FIELD_STEP_RE = re.compile(r"\.([^.\[]*)|\[([^\]]*)\]")


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


# ── Path canonicalisation ──────────────────────────────────────────────
def _has_parent_segment(path: str) -> bool:
    return any(seg == ".." for seg in _SEGMENT_SPLIT_RE.split(path))


def _norm_path(path: str) -> Optional[str]:
    """Comparison form of ``path``; ``None`` when it contains a ``..`` segment.

    URIs (``scheme://authority/object``) get a posix-normalised object path,
    POSIX-absolute paths (the cluster's ``/Volumes``, ``/tmp``) are
    normalised as POSIX even on a Windows host, host paths use the host rules.
    A trailing separator is preserved so prefixes stay directory boundaries.
    """
    p = path.strip()
    if not p or _has_parent_segment(p):
        return None
    trailing = p.endswith(("/", "\\"))
    m = _URI_RE.match(p)
    if m:
        scheme, authority, obj = m.groups()
        obj = posixpath.normpath((obj or "/").replace("\\", "/"))
        normed = f"{scheme.lower()}://{authority}{obj}"
    elif p.startswith("/"):
        normed = posixpath.normpath(p.replace("\\", "/"))
    else:
        normed = os.path.normcase(os.path.normpath(p))
    if trailing and not normed.endswith(("/", "\\")):
        normed += "/" if (m or p.startswith("/")) else os.sep
    return normed


def _norm_prefix(prefix: str) -> str:
    p = prefix.strip()
    if not p:
        return p
    if _has_parent_segment(p):
        raise ValueError(f"sandbox prefix must not contain a '..' segment: {prefix}")
    # Treat a prefix as a directory boundary so "oci://b@ns/sandbox" does not
    # also admit "oci://b@ns/sandbox-prod/...". A local staging prefix may
    # already end in the host separator.
    return p if p.endswith(("/", "\\")) else p + "/"


def _under_prefix(norm_path: str, prefix: str) -> bool:
    norm_pref = _norm_path(prefix)
    if not norm_pref:
        return False
    return norm_path == norm_pref.rstrip("/\\") or norm_path.startswith(norm_pref)


def _real_under_prefix(path: str, prefix: str) -> bool:
    """POSIX: follow symlinks so a link inside the prefix cannot point outside.

    ``realpath`` resolves the existing components of a not-yet-created path.
    Skipped on Windows, where ``realpath`` may expand short names on one side
    only and the cluster paths this module guards are POSIX anyway.
    """
    if os.name != "posix" or "://" in path:
        return True
    real = os.path.realpath(path)
    real_pref = os.path.realpath(prefix.rstrip("/"))
    return real == real_pref or real.startswith(real_pref + "/")


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
            example the FUSE ``/Volumes/default/default/dbfs/`` staging area
            or ``/tmp/``, can be supplied via ``extra_prefixes`` or as a comma
            list in ``AIDP_SANDBOX_PREFIX``.
        allow_network: Permit network-capable imports (``requests``,
            ``socket``, ``urllib``, ``http.client``, ``paramiko``, ...).
            Process/FFI imports stay refused regardless.
        allowed_rules: Rule ids that are a reviewed exception for this run
            (``AIDP_NOTEBOOK_POLICY_ALLOW``). A matching violation is logged
            as ``allowed_exception`` instead of refused.
        extra_prefixes: Additional path prefixes considered inside the sandbox.

    No prefix may contain a ``..`` segment, and no path containing one is ever
    inside the sandbox.
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
        """True when ``path`` is a sandbox prefix or sits under one.

        The path is canonicalised first (``//``, ``/./``); a ``..`` segment
        anywhere makes it outside, and on POSIX a symlink inside the prefix
        pointing outside is outside as well.
        """
        if not isinstance(path, str) or not path.strip():
            return False
        normed = _norm_path(path)
        if normed is None:
            return False
        for pref in self.prefixes:
            if _under_prefix(normed, pref):
                return _real_under_prefix(path.strip(), pref)
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


# ── Policy registry: one-shot snapshot for the process ─────────────────
_frozen_policy: Optional[SandboxPolicy] = None
_lock = threading.Lock()


def set_sandbox_policy(policy: Optional[SandboxPolicy]) -> None:
    """Declare the policy for this process.

    The first declaration (explicit, or implicit through the first
    :func:`require_policy` that resolves ``AIDP_SANDBOX_*``) is frozen. A
    later call with the same policy is a no-op (bootstrap replay); a call
    with a different policy, or with ``None`` once something is declared,
    is refused as ``NBP-POLICY-TAMPER`` so notebook code cannot widen or
    clear the sandbox it runs under.
    """
    global _frozen_policy
    with _lock:
        frozen = _frozen_policy
        if frozen is None:
            if policy is None:
                return
            _frozen_policy = policy
        elif policy == frozen:
            return
    if frozen is not None:
        _refuse_tamper("set_sandbox_policy",
                       "<cleared>" if policy is None else policy.prefix,
                       "the sandbox is frozen for the process once declared")
    _record("sandbox_declared", rule="", target=policy.prefix,
            detail=dict(policy.describe(), source="explicit"))


def current_policy() -> Optional[SandboxPolicy]:
    """Return the declared policy, or ``None`` when nothing is declared."""
    try:
        return require_policy()
    except SandboxUndeclaredError:
        return None


def require_policy() -> SandboxPolicy:
    """Return the frozen policy, resolving (and freezing) it from the
    environment on first use; raise :class:`SandboxUndeclaredError` if none."""
    global _frozen_policy
    pol = _frozen_policy
    if pol is not None:
        return pol
    pol = SandboxPolicy.from_env()
    with _lock:
        if _frozen_policy is None:
            _frozen_policy = pol
            declared = True
        else:
            pol = _frozen_policy
            declared = False
    if declared:
        _record("sandbox_declared", rule="", target=pol.prefix,
                detail=dict(pol.describe(), source="environment"))
    return pol


def _reset_policy_state() -> None:
    """Forget the frozen policy. Test fixtures only; notebook code cannot
    reach this (``aidp_compat.notebook_policy`` imports are refused)."""
    global _frozen_policy
    with _lock:
        _frozen_policy = None


def _refuse_tamper(operation: str, target: str, why: str) -> None:
    remediation = ("The sandbox is declared once per run by the migration bootstrap "
                   "(AIDP_SANDBOX_*); widen it there after review, not from notebook code.")
    _record("runtime_refused", rule=RULE_POLICY_TAMPER, target=target, remediation=remediation,
            detail={"operation": operation, "why": why})
    raise PolicyViolation(
        f"{operation}: refused, {why} (target: {target}). Remediation: {remediation}",
        rule_id=RULE_POLICY_TAMPER, target=target, remediation=remediation,
    )


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


@dataclass
class ScanContext:
    """Alias and constant state carried across the cells of one notebook run.

    ``aliases`` maps a name to the dotted chain it stands for (``o`` ->
    ``("os",)`` after ``import os as o``; ``fs`` -> ``("dbutils", "fs")``
    after ``fs = dbutils.fs``). ``constants`` maps a name bound exactly once
    to the string-valued node it was bound to (``None`` = rebound, unknown).
    ``identities`` are names bound by a plain ``import name`` of a watched
    module; ``tainted`` are names that were ever bound to a watched module,
    protected root, Spark object or dangerous builtin. A tainted name that is
    neither an alias nor an identity any more (rebound to something the gate
    cannot resolve) is refused wherever it is used.
    """

    aliases: Dict[str, Tuple[str, ...]] = field(default_factory=dict)
    constants: Dict[str, Optional[ast.AST]] = field(default_factory=dict)
    identities: Set[str] = field(default_factory=set)
    tainted: Set[str] = field(default_factory=set)
    # Names bound by ``import m`` of a module the gate has no rules for
    # (``platform``, ``numpy``): a receiver the gate counts as resolved.
    imported: Set[str] = field(default_factory=set)
    # Names bound by ``def`` in any cell (a callable ``.pipe()`` argument).
    functions: Set[str] = field(default_factory=set)
    # Names bound once to an RDD-producing chain (``rdd = sc.parallelize(..)``).
    rdd_names: Set[str] = field(default_factory=set)
    # Every name bound anywhere in the run (assignment, import, def, parameter,
    # loop / with / except target). A receiver the run never bound is a name
    # the gate cannot account for (``thing.open(p)``, ``thing.pipe(fn)``).
    bound: Set[str] = field(default_factory=set)
    # Resolved roots of the imported modules the gate has no rules for
    # (``import helpers as h`` -> ``helpers``): their ``_private`` attributes
    # are refused like those of a gated module.
    modules: Set[str] = field(default_factory=set)
    # Names bound once to an in-memory buffer / writer handle (``buf = io.StringIO()``,
    # ``with pd.ExcelWriter(p) as w``): a non-path argument for the frame writers.
    handles: Set[str] = field(default_factory=set)
    # Names bound to a pathlib.Path value (``Path(..)``, a Path-returning step on
    # one, ``Path.cwd()`` / ``Path.home()``, an ``iterdir`` / ``glob`` loop target,
    # a Path-annotated parameter): the node when bound once at module level,
    # ``None`` when the gate cannot resolve the path (rebound, loop / parameter).
    # ``.open`` / ``.rename`` / ``.replace`` on such a name are Path operations.
    paths: Dict[str, Optional[ast.AST]] = field(default_factory=dict)
    # True while ``dbutils.notebook.run`` has its runtime SQL guard installed on
    # the session's SQL entry points: a ``spark.sql`` statement with run-time
    # fragments whose literal head is a read verb is then allowed (and recorded
    # as ``unverified_sql_read``) because the guard re-classifies the final text
    # before Spark executes it. Off by default: a bare static scan fails closed.
    runtime_sql_guard: bool = False

    def forget(self, name: str) -> None:
        """``name`` is (re)bound in the current cell: drop what it stood for."""
        self.bound.add(name)
        self.aliases.pop(name, None)
        self.identities.discard(name)
        self.imported.discard(name)
        self.functions.discard(name)
        self.rdd_names.discard(name)
        self.handles.discard(name)
        self.paths.pop(name, None)
        self.constants[name] = None

    def is_bound(self, name: str) -> bool:
        return name in self.bound or name in self.aliases or name in self.identities or name in self.imported

    def is_unresolved_taint(self, name: str) -> bool:
        return name in self.tainted and name not in self.aliases and name not in self.identities

    def resolve(self, chain: Sequence[str]) -> List[str]:
        if chain and chain[0] in self.aliases:
            return list(self.aliases[chain[0]]) + list(chain[1:])
        return list(chain)


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


def _is_stringish(node: ast.AST) -> bool:
    return _literal_str(node) is not None or _joined_literal_head(node) is not None


def _is_str_sequence(node: ast.AST) -> bool:
    """``["/etc", "passwd"]`` / ``("a", "b")``: a literal sequence of strings (``sep.join(parts)`` folds)."""
    return isinstance(node, (ast.List, ast.Tuple)) and bool(node.elts) \
        and all(_literal_str(e) is not None for e in node.elts)


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


def _pure_chain(node: ast.AST) -> Optional[List[str]]:
    """``a.b.c`` -> ["a", "b", "c"]; ``None`` when the chain contains a call or subscript."""
    parts: List[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if not isinstance(cur, ast.Name):
        return None
    parts.append(cur.id)
    parts.reverse()
    return parts


def _module_root(name: str) -> str:
    return name.split(".", 1)[0]


def _is_dunder(name: str) -> bool:
    return len(name) > 4 and name.startswith("__") and name.endswith("__")


def _magic_violation(source: str) -> Optional["Violation"]:
    """Violation for the first ``%magic`` / ``%%cell-magic`` / ``!command``
    line in a cell body that does not parse as Python, or ``None``.

    Such a cell can never be ``exec``'d (the line is a ``SyntaxError``), so
    this only turns the error into a logged refusal. A continuation line that
    merely starts with the ``%`` operator inside a cell that is *already*
    broken is reported the same way; it would not have run either.
    """
    for lineno, line in enumerate(source.split("\n"), start=1):
        m = _MAGIC_LINE_RE.match(line)
        if not m:
            continue
        marker, word = m.group(1), (m.group(2) or "")
        if marker == "!":
            target = "!" + (word or "<command>")
            return Violation(rule=RULE_OS_PROCESS, lineno=lineno, target=target,
                             detail=f"shell escape line {target!r} inside a code cell",
                             remediation=_REM_PROC)
        target = marker + word
        if word.split(".", 1)[0] in _SHELL_MAGICS:
            return Violation(rule=RULE_OS_PROCESS, lineno=lineno, target=target,
                             detail=f"shell / package magic {target!r} inside a code cell",
                             remediation=_REM_PROC)
        return Violation(rule=RULE_IPYTHON_ESCAPE, lineno=lineno, target=target,
                         detail=f"IPython magic {target!r} inside a code cell",
                         remediation=_REM_IPYTHON)
    return None


def _is_abs_path(path: str) -> bool:
    """POSIX-absolute, home-relative (``~``), URI (``scheme://``) or host-absolute path.

    Notebooks run on Linux clusters, so a leading ``/`` is absolute even when
    this module is imported on Windows (where ``os.path.isabs`` disagrees);
    Windows drive / UNC forms are absolute even when checked on Linux.
    """
    return path.startswith(("/", "~")) or "://" in path or bool(_WINDOWS_PATH_RE.match(path)) \
        or os.path.isabs(path)


def _is_local_path(path: str) -> bool:
    """A literal that names the cluster's local filesystem: POSIX-absolute
    (``/etc``, ``/dbfs``, ``/Workspace``, ``/tmp``, ``/Volumes``), ``~``, a
    Windows drive / UNC path or a ``file:`` URI. Object-storage URIs
    (``oci://``, ``abfss://``, ``s3://``, ...) and relative paths are not."""
    p = path.strip()
    return p.startswith(("/", "~")) or bool(_WINDOWS_PATH_RE.match(p)) or bool(_LOCAL_URI_RE.match(p))


def _local_path_for_check(path: str) -> str:
    """``file:///tmp/x`` -> ``/tmp/x``, ``dbfs:/mnt/x`` -> ``/dbfs/mnt/x``, ``hdfs:///x`` /
    ``local:/x`` -> ``/x`` so the sandbox-prefix comparison sees the path."""
    p = path.strip()
    m = _LOCAL_URI_RE.match(p)
    if not m:
        return p
    return ("/dbfs/" if m.group(1).lower() == "dbfs" else "/") + p[m.end():]


def _sql_ident_is_path(ident: str) -> bool:
    """A backtick identifier that names a path rather than an object: a local
    path, a URI or anything with a separator in it (``parquet.`data/in```)."""
    s = ident.strip()
    return bool(s) and (_is_local_path(s) or "://" in s or "/" in s or "\\" in s)


def _sql_path_table_target(name: str) -> Optional[str]:
    """The path behind a path-table name (``delta.`/tmp/x```, ```text`.`/etc/passwd```,
    ``<provider.class>.`oci://..```), ``None`` for an object name."""
    m = _SQL_PATH_TABLE_NAME_RE.match(name.strip())
    if m and _sql_ident_is_path(m.group(2)):
        return m.group(2)
    return None


def _sql_provider_refused(provider: str, policy: SandboxPolicy) -> Optional[str]:
    """Detail when ``provider`` (a ``USING`` / ``.format(..)`` data source) opens a
    network connection while the sandbox does not allow network access."""
    if policy.allow_network:
        return None
    p = provider.strip().strip("`'\"").lower()
    if p in _NETWORK_PROVIDERS:
        return f"data source provider {p!r} opens a network connection"
    if "." in p:
        return f"data source provider class {provider.strip()!r} is not a built-in short name and may open anything"
    return None


def _format_field_escape(template: str) -> Optional[str]:
    """First ``str.format`` replacement field of ``template`` that walks a
    refused dunder / frame attribute (``{0.__class__.__mro__}``), with its
    braces, or ``None``. Honours ``_DUNDER_ALLOW``; index steps (``[key]``)
    are item lookups, not attribute access. Nested fields inside a format
    spec are formatted with the same arguments and are walked too.
    """
    try:
        parsed = list(string.Formatter().parse(template))
    except ValueError:
        return None   # str.format raises the same error before evaluating anything
    for _literal, field_name, format_spec, _conversion in parsed:
        if field_name is None:
            continue
        steps_from = len(field_name)
        for i, ch in enumerate(field_name):
            if ch in ".[":
                steps_from = i
                break
        for m in _FORMAT_FIELD_STEP_RE.finditer(field_name, steps_from):
            attr = m.group(1)
            if attr is not None and ((_is_dunder(attr) and attr not in _DUNDER_ALLOW) or attr in _FRAME_ATTRS):
                return "{" + field_name + "}"
        if format_spec and "{" in format_spec:
            nested = _format_field_escape(format_spec)
            if nested is not None:
                return nested
    return None


def _import_chain(module_name: str) -> Tuple[str, ...]:
    """Dotted import name as a chain, with ``posix`` / ``nt`` canonicalised to ``os``."""
    parts = module_name.split(".")
    if parts and parts[0] in _OS_EQUIVALENT_MODULES:
        parts[0] = "os"
    return tuple(parts)


def _module_level_statements(tree: ast.Module) -> Iterable[ast.stmt]:
    """Statements that run in the cell's own namespace: the top level and the
    bodies of compound statements that do not open a scope (``for`` /
    ``while`` / ``if`` / ``with`` / ``try``). Function and class bodies are
    not descended into: a binding there is a different name."""
    stack: List[ast.stmt] = list(reversed(tree.body))
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        children: List[ast.stmt] = []
        for attr in ("body", "orelse", "finalbody"):
            children.extend(getattr(node, attr, None) or [])
        for handler in getattr(node, "handlers", None) or []:
            children.extend(handler.body)
        stack.extend(reversed(children))


def _is_prohibited_module(name: str, policy: SandboxPolicy) -> Optional[Tuple[str, str]]:
    root = _module_root(name)
    if root in _PROCESS_MODULES or name.startswith("concurrent.futures.process"):
        return RULE_IMPORT_PROCESS, root if root in _PROCESS_MODULES else "concurrent.futures.process"
    if root in _INDIRECT_MODULES:
        return RULE_INDIRECT, root
    if root in _EXEC_MODULES or root in _CODE_LOAD_MODULES:
        return RULE_BUILTIN_EXEC, root
    if root in _JVM_BRIDGE_MODULES:
        return RULE_POLICY_TAMPER, root
    if root in _IPYTHON_MODULES:
        return RULE_IPYTHON_ESCAPE, root
    if root in _NETWORK_MODULES or name in _NETWORK_MODULES:
        if policy.allow_network:
            return None
        return RULE_IMPORT_NETWORK, root if root in _NETWORK_MODULES else name
    if root.startswith("_") and root != "__future__":
        # Private interpreter modules (_io, _pickle, _posixsubprocess, _ctypes, ...)
        # are the unguarded twins of the public ones above.
        return RULE_INDIRECT, root
    return None


def _first_arg(call: ast.Call, kw: Sequence[str]) -> Optional[ast.AST]:
    if call.args:
        return call.args[0]
    for k in call.keywords:
        if k.arg in kw:
            return k.value
    return None


# ── SQL classifier ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class SqlVerdict:
    """Result of :func:`classify_sql` / :func:`classify_sql_expression`.

    ``kind`` is ``"read"`` (allowed, no target check), ``"write"`` (allowed,
    target inside the sandbox) or ``"refused"``. A refused verdict carries the
    rule id (``NBP-SQL-REFUSED``, ``NBP-SQL-TARGET`` or ``NBP-OPEN-PATH``),
    the target, a detail and a remediation; ``verb`` is the top-level verb
    phrase the classifier resolved (``"WITH"`` is never a verb: the CTE list
    is walked to the statement behind it).
    """

    allowed: bool
    kind: str
    verb: str = ""
    rule: str = ""
    target: str = ""
    detail: str = ""
    remediation: str = ""


_REM_SQL_REFUSED = ("The SQL gate is an allowlist: read statements (SELECT / VALUES / TABLE / WITH .. SELECT / "
                    "SHOW / DESCRIBE / EXPLAIN / CACHE / UNCACHE / REFRESH TABLE / CREATE TEMP VIEW) and writes "
                    "to tables in the sandbox schema are accepted; everything else is refused. Express the step in "
                    "PySpark, or ask for the sandbox to be widened after review.")
_REM_SQL_JVM = ("reflect() / java_method() call arbitrary Java from SQL and are refused anywhere in a statement; "
                "use a built-in Spark SQL function or a Python UDF.")
_REM_SQL_SCRIPT = ("TRANSFORM (..) USING '<cmd>' runs a shell command on the executors and is refused; "
                   "express the step as a Python UDF or a DataFrame operation.")
_REM_SQL_DYNAMIC = ("Pass a literal statement (or a name bound once to one). A statement with run-time fragments "
                    "is accepted only inside dbutils.notebook.run, where the runtime SQL guard re-classifies the "
                    "final text, and only when its literal head is a read verb (SELECT / VALUES / TABLE / "
                    "WITH .. SELECT / SHOW / DESCRIBE / EXPLAIN / CACHE / UNCACHE / REFRESH TABLE / CREATE TEMP "
                    "VIEW); otherwise list NBP-SQL-DYNAMIC in AIDP_NOTEBOOK_POLICY_ALLOW after review.")
_REM_SQL_PATH_READ = ("SQL path sources (<format>.`<path>`, COPY INTO .. FROM '<path>', OPTIONS (path ..)) may name "
                      "object storage (oci://, abfss://, s3://, ..) or a path under the sandbox prefix; a "
                      "local-filesystem path outside it is refused.")
_REM_SQL_IDENTIFIER = ("Spell the table / column / function name literally; IDENTIFIER(..) and parameter markers in "
                       "name position cannot be judged by the gate.")
_REM_NETWORK_SOURCE = ("Network data sources (jdbc / kafka / socket / .. and provider classes) are refused while the "
                       "sandbox does not allow network access (AIDP_SANDBOX_ALLOW_NETWORK); read source data from "
                       "object storage or a catalog table, or ask for the network posture to be widened after review.")
_REM_SQL_SET = ("SET / RESET is limited to a short allowlist of performance / session confs "
                "(spark.sql.shuffle.partitions, spark.sql.adaptive.*, spark.sql.autoBroadcastJoinThreshold, "
                "spark.sql.session.timeZone, SET TIME ZONE, ..); everything else, and a bare SET / RESET, is "
                "refused. Configure the cluster instead.")


class _MaskedSql:
    """``statement`` with comments removed and every string literal / backtick
    identifier replaced by a numbered placeholder.

    Keyword analysis runs on :attr:`text`; :meth:`string` / :meth:`ident`
    return the quoted value behind a placeholder and :meth:`unmask` restores
    a fragment (a table name) to its original spelling. ``''`` / ``""`` /
    ``\\'`` escapes inside literals and doubled backticks are honoured. A raw
    literal (``r'..'`` / ``R".."``, the prefix at a word boundary) has no
    escape processing in Spark, so ``r'\\'`` is the one-character string ``\\``
    and whatever follows its closing quote is live SQL: it is masked as such.
    """

    __slots__ = ("text", "strings", "idents")

    def __init__(self, sql: str):
        out: List[str] = []
        strings: List[str] = []
        idents: List[str] = []
        i, n = 0, len(sql)
        while i < n:
            ch = sql[i]
            nxt = sql[i + 1] if i + 1 < n else ""
            if ch == "-" and nxt == "-":
                j = sql.find("\n", i)
                i = n if j < 0 else j
                out.append(" ")
                continue
            if ch == "/" and nxt == "*":
                j = sql.find("*/", i + 2)
                i = n if j < 0 else j + 2
                out.append(" ")
                continue
            if ch in "'\"":
                raw = i > 0 and sql[i - 1] in "rR" and (i < 2 or not (sql[i - 2].isalnum() or sql[i - 2] in "_$"))
                j = i + 1
                buf: List[str] = []
                while j < n:
                    c = sql[j]
                    if c == "\\" and j + 1 < n and not raw:
                        buf.append(sql[j + 1])
                        j += 2
                        continue
                    if c == ch:
                        if j + 1 < n and sql[j + 1] == ch and not raw:
                            buf.append(ch)
                            j += 2
                            continue
                        break
                    buf.append(c)
                    j += 1
                strings.append("".join(buf))
                out.append("'" + _SQL_STR_MARK + str(len(strings) - 1) + _SQL_STR_MARK + "'")
                i = j + 1
                continue
            if ch == "`":
                j = i + 1
                buf = []
                while j < n:
                    c = sql[j]
                    if c == "`":
                        if j + 1 < n and sql[j + 1] == "`":
                            buf.append("`")
                            j += 2
                            continue
                        break
                    buf.append(c)
                    j += 1
                idents.append("".join(buf))
                out.append("`" + _SQL_IDENT_MARK + str(len(idents) - 1) + _SQL_IDENT_MARK + "`")
                i = j + 1
                continue
            out.append(ch)
            i += 1
        self.text = "".join(out).strip()
        self.strings = strings
        self.idents = idents

    def string(self, index: str) -> str:
        return self.strings[int(index)]

    def ident(self, index: str) -> str:
        return self.idents[int(index)]

    def unmask(self, fragment: str) -> str:
        fragment = _SQL_STR_PLACEHOLDER_RE.sub(lambda m: "'" + self.string(m.group(1)) + "'", fragment)
        return _SQL_IDENT_PLACEHOLDER_RE.sub(lambda m: "`" + self.ident(m.group(1)) + "`", fragment)


def _sql_words(text: str, count: int) -> List[str]:
    """Up to ``count`` leading words of ``text``, upper-cased."""
    words: List[str] = []
    pos = 0
    while len(words) < count:
        m = _SQL_WORD_RE.match(text, pos)
        if not m:
            break
        words.append(m.group(0).upper())
        pos = m.end()
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return words


def _sql_after_words(text: str, count: int) -> str:
    """``text`` with its first ``count`` words (and the following whitespace) removed."""
    pos = 0
    for _ in range(count):
        m = _SQL_WORD_RE.match(text, pos)
        if not m:
            return ""
        pos = m.end()
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return text[pos:]


def _sql_matching_paren(text: str, open_index: int) -> int:
    """Index of the ``)`` matching the ``(`` at ``open_index`` of masked ``text``, or -1."""
    depth = 0
    for j in range(open_index, len(text)):
        if text[j] == "(":
            depth += 1
        elif text[j] == ")":
            depth -= 1
            if depth == 0:
                return j
    return -1


def _sql_skip_ctes(text: str) -> Optional[str]:
    """``text`` follows a ``WITH``: return the statement behind the CTE list.

    Walks ``name [(cols)] AS [NOT MATERIALIZED] ( .. ) [, ..]`` tracking the
    parenthesis depth (strings are masked, so quoted parentheses do not
    count). ``None`` when the list is unbalanced or never ends.
    """
    pos = 0
    n = len(text)
    while True:
        open_ = text.find("(", pos)
        if open_ < 0:
            return None
        depth = 0
        j = open_
        while j < n:
            if text[j] == "(":
                depth += 1
            elif text[j] == ")":
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if j >= n:
            return None
        rest = text[j + 1:].lstrip()
        if not rest:
            return None
        if rest.startswith(","):
            pos = j + 1
            continue
        if _sql_words(rest, 1) in (["AS"], ["NOT"], ["MATERIALIZED"]):
            pos = j + 1          # that was the CTE's column list; its body follows
            continue
        return rest


def _sql_resolve_verb(text: str) -> Tuple[str, str]:
    """``(top-level verb, statement text from that verb on)`` for masked ``text``.

    ``WITH`` is walked past its CTE list, ``EXPLAIN [mode]`` past its modes,
    so the verb is the one Spark executes (``WITH x AS (..) INSERT ..`` is an
    ``INSERT``). ``verb`` is ``""`` when it cannot be determined.
    """
    t = text.lstrip()
    for _ in range(8):
        if t.startswith("("):
            # (SELECT ..) UNION ALL (SELECT ..) / (SELECT ..) ORDER BY ..: a parenthesised
            # query term; the verb is the one inside (a write inside parentheses is still
            # that write). Anything else after the closing parenthesis is undeterminable.
            close = _sql_matching_paren(t, 0)
            if close < 0:
                return "", t
            rest = t[close + 1:].lstrip()
            following = _sql_words(rest, 1)
            if rest and not rest.startswith("(") and (not following or following[0] not in _SQL_AFTER_PAREN_QUERY):
                return "", t
            t = t[1:close].strip()
            continue
        m = _SQL_WORD_RE.match(t)
        if not m:
            return "", t
        verb = m.group(0).upper()
        if verb == "WITH":
            rest = _sql_skip_ctes(t[m.end():])
            if rest is None:
                return "", t
            t = rest
            continue
        if verb == "EXPLAIN":
            rest = t[m.end():].lstrip()
            words = _sql_words(rest, 1)
            while words and words[0] in _SQL_EXPLAIN_MODES:
                rest = _sql_after_words(rest, 1)
                words = _sql_words(rest, 1)
            t = rest
            continue
        return verb, t
    return "", t


def _sql_ident_at(masked: _MaskedSql, text: str) -> Optional[str]:
    """The (unmasked) 1- to 3-part object name at the start of masked ``text``."""
    m = _SQL_IDENT_RE.match(text)
    if not m:
        return None
    return masked.unmask(m.group(1)).strip()


def _sql_refused(rule: str, verb: str, target: str, detail: str, remediation: str) -> SqlVerdict:
    return SqlVerdict(allowed=False, kind="refused", verb=verb, rule=rule, target=target,
                      detail=detail, remediation=remediation)


def _sql_jvm_call(masked: _MaskedSql) -> Optional[str]:
    """The Java-call function named anywhere in the statement (``reflect`` /
    ``java_method`` / ``try_reflect``), spelt plainly or as a backtick-quoted
    identifier followed by ``(``; ``None`` when there is none."""
    m = _SQL_JVM_CALL_RE.search(masked.text)
    if m:
        return m.group(1).lower()
    for qm in _SQL_QUOTED_CALL_RE.finditer(masked.text):
        ident = masked.ident(qm.group(1)).strip().lower()
        if ident in _SQL_JVM_FUNCTIONS:
            return ident
    return None


def _sql_refused_token(masked: _MaskedSql) -> Optional[Tuple[str, str, str]]:
    """``(target, detail, remediation)`` for a token refused anywhere in a statement."""
    name = _sql_jvm_call(masked)
    if name:
        return name + "(", f"{name}() calls arbitrary Java from SQL", _REM_SQL_JVM
    if _SQL_SCRIPT_TRANSFORM_RE.search(masked.text):
        return "TRANSFORM ... USING", "Hive script transform (TRANSFORM .. USING '<cmd>') runs a shell command", \
            _REM_SQL_SCRIPT
    return None


def _sql_path_tables(masked: _MaskedSql, text: Optional[str] = None) -> List[str]:
    """Every path-table path (``<provider>.`<path>```) in masked ``text`` (default: the statement)."""
    paths: List[str] = []
    for m in _SQL_PATH_TABLE_RE.finditer(masked.text if text is None else text):
        ident = masked.ident(m.group(1))
        if _sql_ident_is_path(ident):
            paths.append(ident)
    return paths


def _sql_option_paths(masked: _MaskedSql) -> List[str]:
    """Every ``OPTIONS (.. path '<p>' ..)`` / ``OPTIONS ('path' = '<p>')`` value."""
    paths: List[str] = []
    for om in _SQL_OPTIONS_RE.finditer(masked.text):
        for item in om.group(1).split(","):
            im = _SQL_OPTION_ITEM_RE.match(item)
            if not im:
                continue
            key = masked.unmask(im.group(1)).strip().strip("`'\"").lower()
            if key == "path":
                paths.append(masked.string(im.group(3)))
    return paths


def _sql_judge_paths(policy: SandboxPolicy, verb: str, write_paths: Sequence[str],
                     read_paths: Sequence[str]) -> Optional[SqlVerdict]:
    """A write location must sit under the prefix; a read source may be object
    storage or a path under the prefix but not a local path outside it."""
    for p in write_paths:
        if not policy.path_in_sandbox(p):
            return _sql_refused(RULE_SQL_TARGET, verb, p, "LOCATION / OPTIONS path outside the sandbox prefix",
                                f"Use a LOCATION under {policy.prefix}.")
    for p in read_paths:
        if _is_local_path(p) and not policy.path_in_sandbox(_local_path_for_check(p)):
            return _sql_refused(RULE_OPEN_PATH, verb, p,
                                "SQL path source on the cluster's local filesystem outside the sandbox prefix",
                                _REM_SQL_PATH_READ)
    return None


def _sql_conf_allowed(key: str) -> bool:
    k = key.strip().strip("`").lower()
    if _SQL_SET_DENY_RE.search(k):
        return False
    return k in _SQL_SET_ALLOWED_CONFS or k.startswith(_SQL_SET_ALLOWED_CONF_PREFIXES)


def _sql_judge_target(policy: SandboxPolicy, verb: str, name: Optional[str], schema_object: bool,
                      shown: str) -> Optional[SqlVerdict]:
    """Refuse a write whose object name is missing or outside the sandbox."""
    if not name:
        return _sql_refused(RULE_SQL_TARGET, verb, shown, "write statement whose target could not be resolved",
                            f"Qualify the target as {policy.catalog}.{policy.schema}.<table> inside the sandbox.")
    path = _sql_path_table_target(name)
    if path is not None:
        # INSERT INTO delta.`/path` / OPTIMIZE delta.`oci://..`: a write to a path.
        if not policy.path_in_sandbox(_local_path_for_check(path)):
            return _sql_refused(RULE_SQL_TARGET, verb, path, "path-table write target outside the sandbox prefix",
                                f"Write under {policy.prefix}.")
        return None
    inside = policy.schema_in_sandbox(name) if schema_object else policy.table_in_sandbox(name)
    if not inside:
        return _sql_refused(RULE_SQL_TARGET, verb, name,
                            f"DDL/DML target outside {policy.catalog}.{policy.schema}",
                            f"Target {policy.catalog}.{policy.schema}.<table> "
                            "(1-part names are treated as outside the sandbox).")
    return None


def _sql_create_object(text: str) -> Tuple[Set[str], str, str]:
    """``CREATE [OR REPLACE] <flags> <object> ..`` -> (flags, object word, text after the object)."""
    rest = _sql_after_words(text, 1)                   # past CREATE
    if _sql_words(rest, 2) == ["OR", "REPLACE"]:
        rest = _sql_after_words(rest, 2)
    flags: Set[str] = set()
    words = _sql_words(rest, 1)
    while words and words[0] in _SQL_CREATE_FLAGS:
        flags.add(words[0])
        rest = _sql_after_words(rest, 1)
        words = _sql_words(rest, 1)
    obj = words[0] if words else ""
    return flags, obj, _sql_after_words(rest, 1)


def classify_sql(statement: str, policy: SandboxPolicy) -> SqlVerdict:
    """Allowlist verdict for one ``spark.sql`` / ``sql`` statement.

    Allowed as a read (no target check): ``SELECT``, ``VALUES``, ``TABLE``,
    ``WITH .. SELECT``, ``SHOW *``, ``DESCRIBE`` / ``DESC``, ``EXPLAIN
    <allowed statement>``, ``CACHE`` / ``UNCACHE TABLE``, ``CLEAR CACHE``,
    ``REFRESH TABLE`` / ``REFRESH FUNCTION``, ``CREATE [OR REPLACE] [GLOBAL]
    TEMP[ORARY] VIEW`` without ``LOCATION``, ``USE`` of the sandbox catalog /
    schema, ``SET`` / ``RESET`` of an allowlisted conf, ``SET TIME ZONE``.
    Allowed as a write when the target is inside ``catalog.schema`` (a path
    target under the prefix): ``CREATE`` / ``REPLACE`` ``TABLE`` / ``VIEW`` /
    ``SCHEMA``, ``INSERT INTO`` / ``OVERWRITE [TABLE]``, ``MERGE``, ``UPDATE``,
    ``DELETE``, ``DROP``, ``ALTER``, ``TRUNCATE``, ``OPTIMIZE``, ``VACUUM``,
    ``RESTORE``, ``MSCK REPAIR TABLE``, ``ANALYZE TABLE(S)``, ``COPY INTO``,
    ``CREATE TABLE .. CLONE``; ``LOCATION`` / ``OPTIONS (path ..)`` must sit
    under the prefix. The verb behind a ``WITH`` CTE list is the statement's
    verb, so ``WITH x AS (..) INSERT INTO ..`` is judged as the write it is.

    Refused: a text with more than one statement; ``reflect(`` /
    ``java_method(`` / ``try_reflect(`` (plain or as a backtick-quoted
    function name) / ``TRANSFORM .. USING '<cmd>'`` anywhere; ``IDENTIFIER(``
    anywhere (``NBP-SQL-DYNAMIC``: the name is resolved at run time); path
    sources (``<provider>.`<path>``` whatever the provider, ``COPY INTO ..
    FROM '<path>'``, ``OPTIONS (path ..)`` on a read, ``DESCRIBE DETAIL /
    HISTORY '<path>'``) naming a local-filesystem path outside the prefix
    (``NBP-OPEN-PATH``; object-storage URIs stay allowed for reads); a
    ``USING <provider>`` that opens a network connection while the sandbox
    allows none (``NBP-IMPORT-NETWORK``); ``ADD`` / ``LIST`` ``JAR`` / ``FILE``
    / ``ARCHIVE``, ``CREATE`` / ``DROP FUNCTION``, ``GRANT`` / ``REVOKE``,
    ``CALL``, ``DECLARE`` / ``EXECUTE IMMEDIATE`` / ``BEGIN .. END``, ``INSERT
    OVERWRITE [LOCAL] DIRECTORY``, ``LOAD DATA [LOCAL] INPATH``, ``SET`` /
    ``RESET`` outside the conf allowlist, ``USE`` of anything but the sandbox,
    ``FROM .. INSERT`` multi-insert and every other verb (``NBP-SQL-REFUSED``);
    a write target outside the sandbox, including the destination of ``ALTER
    .. RENAME TO`` (``NBP-SQL-TARGET``).
    """
    if not isinstance(statement, str):
        return _sql_refused(RULE_SQL_REFUSED, "", "<non-string>", "statement is not a string", _REM_SQL_REFUSED)
    masked = _MaskedSql(statement)
    text = masked.text
    shown = statement.strip().split("\n", 1)[0][:80]
    if not text:
        return _sql_refused(RULE_SQL_REFUSED, "", "<empty>", "empty statement", _REM_SQL_REFUSED)
    body = text.rstrip()
    if body.endswith(";"):
        body = body[:-1].rstrip()
    if ";" in body:
        return _sql_refused(RULE_SQL_REFUSED, "", "<multiple statements>",
                            "more than one statement in one spark.sql() call", _REM_SQL_REFUSED)
    token = _sql_refused_token(masked)
    if token is not None:
        target, detail, remediation = token
        return _sql_refused(RULE_SQL_REFUSED, "", target, detail, remediation)
    if _SQL_IDENTIFIER_CLAUSE_RE.search(body):
        return _sql_refused(RULE_SQL_DYNAMIC, "", "IDENTIFIER(",
                            "IDENTIFIER(..) resolves a table / column / function name at run time (also from a "
                            "parameter marker), so the gate cannot see what it names", _REM_SQL_IDENTIFIER)
    verb, stmt = _sql_resolve_verb(body)
    if not verb:
        return _sql_refused(RULE_SQL_REFUSED, "", shown, "statement whose leading keyword could not be determined",
                            _REM_SQL_REFUSED)
    words = _sql_words(stmt, 4)
    phrase = " ".join(words[:2])

    # Path sources shared by reads and writes.
    path_tables = _sql_path_tables(masked, body)
    locations = [masked.string(m.group(1)) for m in _SQL_LOCATION_RE.finditer(body)]
    from_paths = [masked.string(m.group(1)) for m in _SQL_COPY_FROM_RE.finditer(body)] if verb == "COPY" else []
    if verb in ("DESCRIBE", "DESC") and words[1:2] and words[1] in _SQL_DESCRIBE_PATH_WORDS:
        from_paths += [masked.string(m.group(1)) for m in _SQL_STR_PLACEHOLDER_RE.finditer(stmt)]
    option_paths = _sql_option_paths(masked)

    def provider_refused(kind_verb: str) -> Optional[SqlVerdict]:
        pm = _SQL_USING_PROVIDER_RE.search(stmt)
        if pm is None:
            return None
        provider = masked.unmask(pm.group(1)).strip("`")
        detail = _sql_provider_refused(provider, policy)
        if detail is None:
            return None
        return _sql_refused(RULE_IMPORT_NETWORK, kind_verb, provider, detail, _REM_NETWORK_SOURCE)

    def read(kind_verb: str) -> SqlVerdict:
        bad = _sql_judge_paths(policy, kind_verb, locations, path_tables + from_paths + option_paths)
        return bad or SqlVerdict(allowed=True, kind="read", verb=kind_verb)

    def write(kind_verb: str, name: Optional[str], schema_object: bool = False) -> SqlVerdict:
        target_path = _sql_path_table_target(name) if name else None
        sources = [p for p in path_tables if p != target_path] + from_paths
        bad = _sql_judge_paths(policy, kind_verb, list(locations) + list(option_paths), sources)
        if bad is not None:
            return bad
        bad = _sql_judge_target(policy, kind_verb, name, schema_object, shown)
        return bad or SqlVerdict(allowed=True, kind="write", verb=kind_verb, target=name or "")

    def refused(kind_verb: str, detail: str, remediation: str = _REM_SQL_REFUSED,
                rule: str = RULE_SQL_REFUSED, target: str = "") -> SqlVerdict:
        return _sql_refused(rule, kind_verb, target or kind_verb, detail, remediation)

    if verb in _SQL_READ_VERBS:
        return read(verb)
    if verb == "CLEAR" and words[1:2] == ["CACHE"]:
        return read("CLEAR CACHE")
    if verb == "REFRESH":
        if words[1:2] in (["TABLE"], ["FUNCTION"]):
            return read(phrase)
        return refused(phrase or verb, "REFRESH of a path is not in the SQL read allowlist")
    if verb == "USE":
        rest = stmt
        kind = "USE"
        if words[1:2] in (["CATALOG"], ["SCHEMA"], ["DATABASE"], ["NAMESPACE"]):
            kind = phrase
            rest = _sql_after_words(stmt, 2)
        else:
            rest = _sql_after_words(stmt, 1)
        name = _sql_ident_at(masked, rest)
        if not name:
            return refused(kind, "USE without a resolvable name")
        parts = [p.lower() for p in _split_table_name(name)]
        if parts == [policy.catalog] or policy.schema_in_sandbox(name):
            return SqlVerdict(allowed=True, kind="read", verb=kind, target=name)
        return refused(kind, f"USE outside the sandbox {policy.catalog}.{policy.schema}", rule=RULE_SQL_TARGET,
                       target=name, remediation=f"USE {policy.catalog} or USE {policy.catalog}.{policy.schema}.")
    if verb in ("SET", "RESET"):
        if _SQL_SET_TIME_ZONE_RE.match(stmt):
            return SqlVerdict(allowed=True, kind="read", verb="SET TIME ZONE")
        km = _SQL_SET_KEY_RE.match(stmt)
        if km and _sql_conf_allowed(km.group(2)):
            return SqlVerdict(allowed=True, kind="read", verb=verb, target=km.group(2))
        key = km.group(2) if km else ""
        return refused(verb, f"{verb} of a conf outside the allowlist" if key else f"bare {verb}", _REM_SQL_SET,
                       target=f"{verb} {key}".strip())
    if verb == "CREATE":
        flags, obj, rest = _sql_create_object(stmt)
        if obj == "FUNCTION":
            return refused("CREATE FUNCTION", "CREATE FUNCTION registers a JVM class or a Python/SQL body as a "
                                              "function", _REM_SQL_REFUSED)
        if flags & _SQL_TEMP_FLAGS and obj in _SQL_TABLE_OBJECTS:
            # Session-scoped; a LOCATION on it is judged as a write location, OPTIONS (path) as a read
            # source, USING <provider> like a reader format.
            kind = "CREATE TEMP VIEW"
            bad = provider_refused(kind) or _sql_judge_paths(policy, kind, locations,
                                                             path_tables + from_paths + option_paths)
            return bad or SqlVerdict(allowed=True, kind="read", verb=kind)
        if obj in _SQL_CATALOG_OBJECTS:
            if _sql_words(rest, 3) == ["IF", "NOT", "EXISTS"]:
                rest = _sql_after_words(rest, 3)
            if obj in _SQL_TABLE_OBJECTS:
                bad = provider_refused(f"CREATE {obj}")
                if bad is not None:
                    return bad
            return write(f"CREATE {obj}", _sql_ident_at(masked, rest), obj in _SQL_SCHEMA_OBJECTS)
        return refused(f"CREATE {obj}".strip(), f"CREATE {obj or '<?>'} is not in the SQL allowlist")
    if verb == "REPLACE" and words[1:2] == ["TABLE"]:
        return write("REPLACE TABLE", _sql_ident_at(masked, _sql_after_words(stmt, 2)))
    if verb == "INSERT":
        if words[1:2] in (["INTO"], ["OVERWRITE"]):
            rest = _sql_after_words(stmt, 2)
            if words[1] == "OVERWRITE" and words[2:3] in (["DIRECTORY"], ["LOCAL"]):
                return refused("INSERT OVERWRITE DIRECTORY", "INSERT OVERWRITE [LOCAL] DIRECTORY writes files to a "
                                                             "path", _REM_SQL_REFUSED)
            if words[2:3] == ["TABLE"]:
                rest = _sql_after_words(rest, 1)
            return write(phrase, _sql_ident_at(masked, rest))
        return refused(phrase or verb, "INSERT form not in the SQL allowlist")
    if verb == "MERGE":
        rest = _sql_after_words(stmt, 1)
        if _sql_words(rest, 3) == ["WITH", "SCHEMA", "EVOLUTION"]:
            rest = _sql_after_words(rest, 3)
        if _sql_words(rest, 1) != ["INTO"]:
            return refused("MERGE", "MERGE form not in the SQL allowlist")
        return write("MERGE INTO", _sql_ident_at(masked, _sql_after_words(rest, 1)))
    if verb == "UPDATE":
        return write("UPDATE", _sql_ident_at(masked, _sql_after_words(stmt, 1)))
    if verb == "DELETE":
        if words[1:2] != ["FROM"]:
            return refused("DELETE", "DELETE form not in the SQL allowlist")
        return write("DELETE FROM", _sql_ident_at(masked, _sql_after_words(stmt, 2)))
    if verb in ("DROP", "ALTER"):
        rest = _sql_after_words(stmt, 1)
        obj_words = _sql_words(rest, 1)
        if obj_words == ["TEMPORARY"]:
            rest = _sql_after_words(rest, 1)
            obj_words = _sql_words(rest, 1)
        obj = obj_words[0] if obj_words else ""
        if obj == "FUNCTION":
            return refused(f"{verb} FUNCTION", f"{verb} FUNCTION changes the session's function registry")
        if obj not in _SQL_CATALOG_OBJECTS:
            return refused(f"{verb} {obj}".strip(), f"{verb} {obj or '<?>'} is not in the SQL allowlist")
        rest = _sql_after_words(rest, 1)
        if verb == "DROP" and _sql_words(rest, 2) == ["IF", "EXISTS"]:
            rest = _sql_after_words(rest, 2)
        verdict = write(f"{verb} {obj}", _sql_ident_at(masked, rest), obj in _SQL_SCHEMA_OBJECTS)
        rm = _SQL_RENAME_TO_RE.search(rest) if verdict.allowed and verb == "ALTER" else None
        if rm is not None:
            # ALTER TABLE sbx.t RENAME TO prod.t moves the object: the destination is a write target too
            # (a 1-part destination resolves against the current schema and is outside, like any 1-part name).
            bad = _sql_judge_target(policy, f"ALTER {obj} RENAME TO", _sql_ident_at(masked, rest[rm.end():]),
                                    obj in _SQL_SCHEMA_OBJECTS, shown)
            if bad is not None:
                return bad
        return verdict
    if verb == "TRUNCATE":
        if words[1:2] != ["TABLE"]:
            return refused("TRUNCATE", "TRUNCATE form not in the SQL allowlist")
        return write("TRUNCATE TABLE", _sql_ident_at(masked, _sql_after_words(stmt, 2)))
    if verb in ("OPTIMIZE", "VACUUM"):
        rest = _sql_after_words(stmt, 1)
        sm = _SQL_STR_PLACEHOLDER_RE.match(rest)
        if sm:
            # VACUUM '/path' / OPTIMIZE '/path': a write to a path.
            path = masked.string(sm.group(1))
            bad = _sql_judge_paths(policy, verb, [path], [])
            return bad or SqlVerdict(allowed=True, kind="write", verb=verb, target=path)
        return write(verb, _sql_ident_at(masked, rest))
    if verb == "RESTORE":
        rest = _sql_after_words(stmt, 2 if words[1:2] == ["TABLE"] else 1)
        return write("RESTORE TABLE", _sql_ident_at(masked, rest))
    if verb == "MSCK":
        if words[1:3] != ["REPAIR", "TABLE"]:
            return refused("MSCK", "MSCK form not in the SQL allowlist")
        return write("MSCK REPAIR TABLE", _sql_ident_at(masked, _sql_after_words(stmt, 3)))
    if verb == "ANALYZE":
        if words[1:2] == ["TABLE"]:
            return write("ANALYZE TABLE", _sql_ident_at(masked, _sql_after_words(stmt, 2)))
        if words[1:3] == ["TABLES", "IN"]:
            return write("ANALYZE TABLES", _sql_ident_at(masked, _sql_after_words(stmt, 3)), True)
        return refused("ANALYZE", "ANALYZE TABLES without a schema works on the session's current database",
                       rule=RULE_SQL_TARGET, target=shown,
                       remediation=f"ANALYZE TABLES IN {policy.catalog}.{policy.schema} ..")
    if verb == "COPY":
        if words[1:2] != ["INTO"]:
            return refused("COPY", "COPY form not in the SQL allowlist")
        return write("COPY INTO", _sql_ident_at(masked, _sql_after_words(stmt, 2)))
    if verb == "ADD":
        return refused(phrase or verb, "ADD JAR / FILE / ARCHIVE loads outside code or files into the session")
    if verb == "LIST":
        return refused(phrase or verb, "LIST JAR / FILE enumerates the session's resources")
    if verb in ("GRANT", "REVOKE", "DENY"):
        return refused(verb, f"{verb} changes privileges")
    if verb == "CALL":
        return refused(verb, "CALL runs a stored procedure")
    if verb in ("DECLARE", "EXECUTE", "BEGIN", "FOR", "IF", "WHILE", "REPEAT", "LOOP", "CASE", "LEAVE",
                "ITERATE", "SIGNAL"):
        return refused(verb, f"SQL scripting ({verb} ..) builds and runs statements at run time")
    if verb == "LOAD":
        return refused("LOAD DATA", "LOAD DATA [LOCAL] INPATH reads a path into a table")
    if verb == "FROM":
        return refused("FROM", "FROM .. INSERT / FROM .. SELECT multi-insert syntax is not in the SQL allowlist")
    return refused(verb, f"statement verb {verb} is not in the SQL read allowlist")


def classify_sql_expression(expression: str, policy: SandboxPolicy) -> SqlVerdict:
    """Verdict for a SQL *expression* string (``selectExpr`` / ``filter`` /
    ``where`` / ``functions.expr``): refuses ``reflect(`` / ``java_method(``
    and the script transform; path tables cannot occur in an expression."""
    if not isinstance(expression, str):
        return SqlVerdict(allowed=True, kind="read", verb="<expr>")
    masked = _MaskedSql(expression)
    token = _sql_refused_token(masked)
    if token is not None:
        target, detail, remediation = token
        return _sql_refused(RULE_SQL_REFUSED, "<expr>", target, detail, remediation)
    return SqlVerdict(allowed=True, kind="read", verb="<expr>")


def classify_sql_function(name: Any, policy: SandboxPolicy) -> SqlVerdict:
    """Verdict for a function called BY NAME from the DataFrame API
    (``functions.reflect`` / ``java_method`` / ``try_reflect``, or
    ``functions.call_function(name, ..)``): the Java-call functions are refused,
    and so is a name that is not a string (the gate cannot see it)."""
    if not isinstance(name, str):
        return _sql_refused(RULE_SQL_REFUSED, "<function>", "<non-string>",
                            "call_function() with a function name the guard cannot read", _REM_SQL_JVM)
    fn = name.strip().strip("`").lower()
    if fn in _SQL_JVM_FUNCTIONS:
        return _sql_refused(RULE_SQL_REFUSED, "<function>", fn + "(", f"{fn}() calls arbitrary Java from the "
                                                                       "DataFrame API", _REM_SQL_JVM)
    return SqlVerdict(allowed=True, kind="read", verb="<function>", target=fn)


class _SqlTemplateFormatter(string.Formatter):
    """``string.Formatter`` that renders a ``spark.sql`` template the way pyspark's
    ``SQLStringFormatter`` does: plain values become text (``str(value)``); a
    pyspark object (``Column`` / ``DataFrame``, which pyspark turns into an
    expression / a temp view name) becomes a neutral identifier. A field with
    attribute / index steps (``{t.x}`` / ``{t[0]}``) is refused: the gate does
    not walk the notebook's objects."""

    def get_field(self, field_name: str, args: Sequence[Any], kwargs: Mapping[str, Any]) -> Tuple[Any, str]:
        if not field_name.isidentifier():
            raise ValueError(f"template field {field_name!r} is not a plain name")
        value = kwargs[field_name]
        if type(value).__module__.split(".")[0] == "pyspark":
            return "_aidp_param_" + field_name, field_name
        return value, field_name

    def format_field(self, value: Any, format_spec: str) -> str:
        return str(value) if format_spec == "" else format(value, format_spec)


def render_sql_template(statement: str, values: Mapping[str, Any]) -> Optional[str]:
    """The text ``spark.sql(statement, **values)`` hands to Spark: pyspark (3.4+)
    substitutes every ``{name}`` field of the statement with the keyword
    argument before parsing, so the arguments ARE statement text. ``None``
    when the template cannot be rendered (an unknown or positional field, a
    field with attribute steps): the caller fails closed."""
    try:
        return _SqlTemplateFormatter().vformat(statement, (), dict(values))
    except (KeyError, IndexError, ValueError, AttributeError, TypeError):
        return None


def sql_template_fragments(statement: str) -> List[str]:
    """The literal pieces of a ``spark.sql`` template between its ``{name}`` fields
    (``{{`` / ``}}`` are literal braces), in order; the first is the head."""
    out: List[str] = []
    buf: List[str] = []
    try:
        for literal, field_name, _spec, _conv in string.Formatter().parse(statement):
            buf.append(literal)
            if field_name is not None:
                out.append("".join(buf))
                buf = []
    except ValueError:
        return []                   # malformed template: no determinable head
    out.append("".join(buf))
    return out


def _sql_head_verb(head: str) -> str:
    """Top-level verb of the literal head of a dynamic statement (``""`` if undeterminable).

    A head that is just ``EXPLAIN`` (``'EXPLAIN ' + q``) is an ``EXPLAIN``:
    whatever follows is planned, never executed. A head whose CTE list is not
    complete (``f"WITH x AS ({sub}) .."``) has no determinable verb.
    """
    text = _MaskedSql(head).text
    if _SQL_TEMP_VIEW_HEAD_RE.match(text):
        return "CREATE TEMP VIEW"
    verb, _rest = _sql_resolve_verb(text)
    if not verb and _sql_words(text, 1) == ["EXPLAIN"]:
        return "EXPLAIN"
    return verb


def _sql_fragment_refused(fragment: str, policy: SandboxPolicy, last: bool = True) -> Optional[SqlVerdict]:
    """A literal fragment of a dynamic statement that already carries a refused
    token (Java call, script transform, a local path table outside the prefix,
    a statement separator). Fragments are partial, so they are judged raw; a
    trailing ``;`` is tolerated only on the ``last`` fragment."""
    m = _SQL_JVM_CALL_RE.search(fragment) or _SQL_QUOTED_JVM_CALL_RAW_RE.search(fragment)
    if m:
        name = m.group(1).lower() + "("
        return _sql_refused(RULE_SQL_REFUSED, "", name, f"{name}) calls arbitrary Java from SQL", _REM_SQL_JVM)
    if _SQL_SCRIPT_TRANSFORM_RAW_RE.search(fragment):
        return _sql_refused(RULE_SQL_REFUSED, "", "TRANSFORM ... USING",
                            "Hive script transform (TRANSFORM .. USING '<cmd>') runs a shell command", _REM_SQL_SCRIPT)
    if _SQL_IDENTIFIER_CLAUSE_RE.search(fragment):
        return _sql_refused(RULE_SQL_DYNAMIC, "", "IDENTIFIER(",
                            "IDENTIFIER(..) resolves a table / column / function name at run time (also from a "
                            "parameter marker), so the gate cannot see what it names", _REM_SQL_IDENTIFIER)
    # A backtick identifier that is a path, whatever precedes it (the provider may be
    # a run-time fragment: f"SELECT * FROM `{fmt}`.`/etc/passwd`"), is a path table.
    for pm in _SQL_BACKTICK_PATH_RAW_RE.finditer(fragment):
        path = pm.group(1)
        if _is_local_path(path) and not policy.path_in_sandbox(_local_path_for_check(path)):
            return _sql_refused(RULE_OPEN_PATH, "", path,
                                "SQL path source on the cluster's local filesystem outside the sandbox prefix",
                                _REM_SQL_PATH_READ)
    body = _MaskedSql(fragment).text.rstrip()
    if last and body.endswith(";"):
        body = body[:-1]
    if ";" in body:
        return _sql_refused(RULE_SQL_REFUSED, "", "<multiple statements>",
                            "more than one statement in one spark.sql() call", _REM_SQL_REFUSED)
    return None


def _ends_with_literal(node: ast.AST) -> bool:
    """Does the statement's text END with a literal fragment? (``'.. ;' + q`` does not:
    the ``;`` is a separator before run-time text, not a trailing terminator.)"""
    if isinstance(node, ast.JoinedStr):
        return bool(node.values) and isinstance(node.values[-1], ast.Constant)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _ends_with_literal(node.right)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        return _ends_with_literal(node.left)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        return _ends_with_literal(node.func.value)
    return isinstance(node, ast.Constant) and isinstance(node.value, str)


def _literal_fragments(node: ast.AST, include_format_args: bool = True) -> List[str]:
    """Every string constant of an f-string / concatenation / ``%`` / ``.format(..)``
    statement, in statement order (``.format`` arguments last, when included)."""
    out: List[str] = []
    if isinstance(node, ast.JoinedStr):
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                out.append(part.value)
            elif isinstance(part, ast.FormattedValue):
                out.extend(_literal_fragments(part.value, include_format_args))
    elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
        out.extend(_literal_fragments(node.left, include_format_args))
        out.extend(_literal_fragments(node.right, include_format_args))
    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        out.extend(_literal_fragments(node.func.value, include_format_args))
        if include_format_args:
            for a in node.args:
                out.extend(_literal_fragments(a, include_format_args))
    elif isinstance(node, ast.Constant) and isinstance(node.value, str):
        out.append(node.value)
    return out


_REM_ENV = "Pass values through dbutils.widgets / job parameters instead of reading the process environment."
_REM_PROC = "Shell/process execution is not available inside the sandbox."
_REM_TAMPER = ("The policy, the compat shims and the interpreter modules are read-only for notebook code; "
               "change the sandbox through AIDP_SANDBOX_* / AIDP_NOTEBOOK_POLICY_ALLOW after review.")
_REM_INDIRECT = ("Indirect access to the interpreter (importlib, builtins / __builtins__, globals() / locals() / "
                 "vars(), sys.modules, getattr with a dynamic name, frames, a rebound alias) is refused; "
                 "import the module you need directly and call it by name so the gate can see it.")
_REM_DUNDER = ("Dunder and frame introspection chains (__subclasses__, __globals__, __dict__, __code__, f_globals, "
               "tb_frame, ...) are refused; use the public API of the object.")
_REM_FORMAT = ("Write the format template as a literal in the notebook (or a name bound once to one): a template "
               "built at run time can address attributes of the formatted objects (__class__.__mro__, "
               "__globals__, ...), so an unresolvable template with run-time arguments is refused.")
_REM_READ_PATH = ("Read source data from object storage (oci://...), a catalog table, a relative path or a path under "
                  "the sandbox prefix; a literal local-filesystem path outside it is refused.")
_REM_IPYTHON = ("The IPython shell API (get_ipython, InteractiveShell, %magics, !commands) is not available inside "
                "the sandbox; express the step in plain Python or dbutils.")
_REM_JVM = ("The Spark JVM / py4j bridge (spark._jvm, sc._jsc, df._jdf, py4j) and private Spark attributes are "
            "not available inside the sandbox; use the public PySpark API or dbutils.fs.")
_REM_SESSION = ("spark.newSession(), SparkSession.builder..getOrCreate() and SparkSession.getActiveSession() hand "
                "out a session object without the runtime SQL guard; use the `spark` session the run provides.")
_REM_DESERIALIZE = ("Deserialising bytes with pickle / marshal / shelve / dill / joblib / jsonpickle, yaml.load without "
                    "SafeLoader or torch.load without weights_only=True executes code; use a data format "
                    "(parquet, json, yaml.safe_load, weights_only=True) or list NBP-DESERIALIZE in "
                    "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
_REM_VALUE = ("A gated module, interpreter root or Spark object may only be the root of an attribute chain, the "
              "whole right-hand side of a plain name assignment or the object argument of getattr / setattr / "
              "vars; handing it around as a value (list / dict element, argument, default, return value, lambda, "
              "conditional, walrus, with-item, loop iterable) hides the chain from the gate and is refused.")
_REM_SINK = ("A process / environment / introspection method name on an object the gate cannot resolve is "
             "refused; call it on the module directly so the gate can judge it, or rename your own method.")
_REM_PIPE = ("RDD.pipe(cmd) runs a shell command on the executors and is not available inside the sandbox; "
             "DataFrame.pipe(func) is fine when the argument is a function, not a string.")
_REM_CODE_LOAD = "Dependencies are installed as cluster libraries; do not load code from paths at run time."
_REM_PRIVATE = ("Private (_name) attributes of an imported module are not its public API and may re-export the "
                "interpreter modules the gate refuses; use the module's public API.")
_REM_MUTATE = ("The state of a gated module (a deserialiser's constructor registry, an interpreter table, a Spark "
               "object) is read-only for notebook code; configure it through the public API or the run's "
               "AIDP_* settings after review.")
_REM_LOADER_ARGS = ("Pass the loader's safety switch as a literal keyword the gate can see (allow_pickle=False, "
                    "weights_only=True, Loader=yaml.SafeLoader) instead of positionally or through *args / **kwargs.")
_REM_RECEIVER = ("The receiver of this call was never bound in the notebook, so the gate cannot tell what it is; "
                 "bind it in the notebook (import the library, assign the object) so the call can be judged.")
_REM_WRITE_PATH = ("Library writers (np.save, torch.save, joblib.dump, df.to_csv / to_parquet, plt.savefig, "
                   "cv2.imwrite, wb.save, ...) must write under the sandbox prefix or to a relative path; a path "
                   "the gate cannot resolve is refused like df.write.*(path_var): bind it once to a literal sandbox "
                   "path, write through df.write / dbutils.fs, or list NBP-PATH-WRITE-DYNAMIC in "
                   "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
_REM_TEMPFILE = ("tempfile creates files where dir= / prefix= (or tempfile.tempdir) point; keep the default "
                 "temporary directory or use a literal directory under the sandbox prefix.")
_REM_FRAMES = ("Live frames and their locals (faulthandler, traceback.walk_stack / walk_tb, capture_locals=True, "
               "Task.get_stack) are not available inside the sandbox; traceback.print_exc / format_exc / "
               "extract_stack return text and stay allowed.")


class _PolicyScanner(ast.NodeVisitor):
    def __init__(self, policy: SandboxPolicy, context: Optional[ScanContext] = None):
        self.policy = policy
        self.ctx = context if context is not None else ScanContext()
        self.violations: List[Violation] = []
        # Informational entries (``rule`` holds the POLICY_NOTES event name):
        # allowed operations the gate could not verify, for the policy log.
        self.notes: List[Violation] = []
        self._seen: set = set()
        # Node ids a more specific check has already judged (``__builtins__['open'](..)``
        # is reported as open(), not additionally as namespace access).
        self._consumed: Set[int] = set()
        # Name nodes that are the callee of a direct call (``open(p)`` vs ``map(open, ps)``).
        self._call_funcs: Set[int] = set()
        # Parent of every node in the cell (``prepare``), for position checks.
        self._parents: Dict[int, ast.AST] = {}

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

    def _note(self, event: str, node: ast.AST, detail: str, target: str = "") -> None:
        lineno = getattr(node, "lineno", 0)
        key = (event, lineno, target)
        if key in self._seen:
            return
        self._seen.add(key)
        self.notes.append(Violation(rule=event, detail=detail, target=target, lineno=lineno))

    # -- pre-pass: bindings, aliases, constants -------------------------
    def prepare(self, tree: ast.Module) -> None:
        """Record what this cell binds so later checks can resolve names.

        A name bound more than once (anywhere in the cell) is unknown; a
        top-level ``name = <string literal / f-string / concat>`` bound once
        is a constant; ``import m as a`` / ``from m import n as a`` /
        ``a = dbutils.fs`` are aliases.
        """
        bound: Dict[str, int] = {}

        def bind(name: str) -> None:
            bound[name] = bound.get(name, 0) + 1

        def taint_assignment(targets: Sequence[ast.AST], value: Optional[ast.AST]) -> None:
            # ``x = spark`` / ``f = getattr`` / ``b = __builtins__`` / ``o = os``: the
            # target name now stands for a protected object, wherever it goes next.
            if value is None:
                return
            chain = _pure_chain(value)
            if chain is None or self.ctx.resolve(chain)[0] not in _ALIASABLE_SINGLETONS:
                return
            for t in targets:
                for elt in (t.elts if isinstance(t, (ast.Tuple, ast.List)) else [t]):
                    if isinstance(elt, ast.Name):
                        self.ctx.tainted.add(elt.id)

        # Names bound anywhere in the cell to a pathlib.Path value (``q = Path(p).resolve()``,
        # ``for f in Path(d).iterdir()``, ``def run(src: Path)``): ``q.open()`` / ``.rename()``
        # / ``.replace()`` on them are Path operations whatever else the name was, so the
        # three generic method names cannot slip through as str / DataFrame / zip methods.
        path_tainted: Set[str] = set()

        def path_taint(target: ast.AST) -> None:
            for elt in (target.elts if isinstance(target, (ast.Tuple, ast.List)) else [target]):
                if isinstance(elt, ast.Name):
                    path_tainted.add(elt.id)
                    self.ctx.paths.setdefault(elt.id, None)

        def path_assignment(targets: Sequence[ast.AST], value: Optional[ast.AST]) -> None:
            if value is None:
                return
            for t in targets:
                if isinstance(t, (ast.Tuple, ast.List)) and isinstance(value, ast.Tuple) \
                        and len(t.elts) == len(value.elts):
                    for elt, val in zip(t.elts, value.elts):        # a, b = Path(x), Path(y)
                        if self._path_receiver(val)[0]:
                            path_taint(elt)
                elif self._path_receiver(value)[0]:
                    path_taint(t)

        defined: Set[str] = set()
        # Names read in any position other than the operand of ``sep.join(name)``:
        # a list literal bound to such a name may have been mutated in place
        # (``p[0] = ..``, ``p.append(..)``, ``p.insert(..)``, ``mutate(p)``,
        # ``q = p; q[0] = ..``), so it is no longer the literal it was bound to.
        loaded_elsewhere: Set[str] = set()
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self._parents[id(child)] = node
            if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
                bind(node.id)
            elif isinstance(node, ast.Name) and not self._is_join_operand(node):
                loaded_elsewhere.add(node.id)
            elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                bind(node.name)
                if not isinstance(node, ast.ClassDef):
                    defined.add(node.name)
            elif isinstance(node, ast.arg):
                bind(node.arg)
                if node.annotation is not None and self._is_path_class(node.annotation):
                    path_taint(ast.Name(id=node.arg, ctx=ast.Store()))      # def run(src: Path)
            elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
                if self._path_receiver(node.iter)[0]:
                    path_taint(node.target)                                 # for f in Path(d).iterdir()
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    local = alias.asname or _module_root(alias.name)
                    bind(local)
                    if _module_root(alias.name) in _WATCHED_MODULES:
                        self.ctx.tainted.add(local)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name != "*":
                        bind(alias.asname or alias.name)
                        root = _module_root(node.module or "")
                        if root in _WATCHED_MODULES and root != "aidp_compat":
                            # (compat helpers keep their own names; _check_compat_import governs them)
                            self.ctx.tainted.add(alias.asname or alias.name)
            elif isinstance(node, ast.ExceptHandler) and node.name:
                bind(node.name)
            elif isinstance(node, ast.Global):
                for n in node.names:
                    bind(n)
            elif isinstance(node, ast.Assign):
                taint_assignment(node.targets, node.value)
                path_assignment(node.targets, node.value)
            elif isinstance(node, (ast.AnnAssign, ast.NamedExpr)):
                taint_assignment([node.target], node.value)
                path_assignment([node.target], node.value)
                if isinstance(node, ast.AnnAssign) and self._is_path_class(node.annotation):
                    path_taint(node.target)                                 # p: Path = ..
        for name in bound:
            self.ctx.forget(name)
        for name in path_tainted:
            self.ctx.paths[name] = None       # a Path the gate cannot resolve until bound once below
        self.ctx.functions.update(n for n in defined if bound.get(n, 0) == 1)
        for node in _module_level_statements(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    local = alias.asname or _module_root(alias.name)
                    if bound.get(local, 0) != 1:
                        continue
                    chain = _import_chain(alias.name)
                    if chain[0] not in _WATCHED_MODULES:
                        self.ctx.imported.add(local)            # import platform / import pandas as pd
                        self.ctx.modules.add(chain[0])
                    if alias.asname:
                        self.ctx.aliases[local] = chain
                    elif chain[0] != local:
                        self.ctx.aliases[local] = chain[:1]      # import posix -> an alias of os
                    else:
                        self.ctx.identities.add(local)
            elif isinstance(node, ast.ImportFrom) and node.module:
                if _module_root(node.module) == "aidp_compat":
                    continue  # helpers keep their own names (dbutils, translate_path, safe_*)
                if _import_chain(node.module)[0] not in _WATCHED_MODULES:
                    self.ctx.modules.add(_import_chain(node.module)[0])
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    local = alias.asname or alias.name
                    if bound.get(local, 0) == 1:
                        self.ctx.aliases[local] = _import_chain(node.module) + (alias.name,)
            elif isinstance(node, ast.Assign) and len(node.targets) == 1 \
                    and isinstance(node.targets[0], ast.Name):
                name = node.targets[0].id
                if bound.get(name, 0) != 1:
                    continue
                chain = _pure_chain(node.value)
                # Resolved first: ``w2 = wr`` where ``wr`` stands for ``df.write.mode(..)``, or
                # ``p = op`` where ``op`` stands for ``os.path``, is an alias of the same chain.
                resolved = tuple(self.ctx.resolve(chain)) if chain is not None else None
                if resolved is not None and (len(resolved) >= 2 or resolved[0] in _ALIASABLE_SINGLETONS):
                    self.ctx.aliases[name] = resolved
                    if resolved[0] in _ALIASABLE_SINGLETONS:
                        self.ctx.tainted.add(name)
                    if isinstance(node.value, ast.Name):
                        # ``f = open`` is followed as an alias; the bare reference
                        # on the right-hand side is not also a loose builtin.
                        self._consumed.add(id(node.value))
                elif _is_stringish(node.value) or self._path_ctor_arg(node.value) is not None \
                        or _is_str_sequence(node.value) or self._fold_literal(node.value) is not None:
                    # a literal, f-string / concat, Path("x"), ["a", "b"], "/".join([..]) or os.path.join(..)
                    self.ctx.constants[name] = node.value
                else:
                    builder = self._spark_io_builder_chain(node.value)
                    if builder is not None:
                        # wr = df.write.mode('overwrite') / rd = spark.read.format('csv') /
                        # wr = df.coalesce(1).write: the name is the reader / writer being built
                        self.ctx.aliases[name] = tuple(builder)
                if isinstance(node.value, ast.Call):
                    vchain = self._chain(node.value.func)
                    if vchain and (vchain[0] in _SPARK_ROOTS or any(p in _RDD_CHAIN_ATTRS for p in vchain)):
                        self.ctx.rdd_names.add(name)         # rdd = sc.parallelize(..).map(..)
                    if self._is_write_handle_ctor(node.value):
                        self.ctx.handles.add(name)           # buf = io.StringIO()
                if self._path_receiver(node.value)[0]:
                    # q = Path(p).resolve() / Path('/etc') / 'x' / Path.home(): the name is that
                    # Path; its literal (if any) is folded where q.open() / .rename() is judged
                    self.ctx.paths[name] = node.value
            elif isinstance(node, (ast.With, ast.AsyncWith)):
                for item in node.items:
                    target = item.optional_vars
                    if isinstance(target, ast.Name) and bound.get(target.id, 0) == 1 \
                            and self._is_write_handle_ctor(item.context_expr):
                        self.ctx.handles.add(target.id)      # with pd.ExcelWriter(p) as writer:
        for name in loaded_elsewhere:
            # A mutable literal is folded only while its name is used for nothing
            # but ``sep.join(name)``; any other use (this or an earlier cell's
            # binding) may have changed it, and the stale literal must not be
            # what the sandbox check sees (fail closed: the join is then dynamic).
            if isinstance(self.ctx.constants.get(name), (ast.List, ast.Set, ast.Dict)):
                self.ctx.constants[name] = None

    def _is_write_handle_ctor(self, node: ast.AST) -> bool:
        """``io.StringIO()`` / ``BytesIO()`` / ``pd.ExcelWriter(..)`` / ``pd.HDFStore(..)``: a
        buffer or writer handle (the handle's own path is judged where it is built)."""
        if not isinstance(node, ast.Call):
            return False
        chain = self._chain(node.func)
        return bool(chain) and chain[-1] in _WRITE_HANDLE_CTORS

    def _spark_io_builder_chain(self, node: ast.AST) -> Optional[List[str]]:
        """The chain when ``node`` is a Spark reader / writer still being built:
        ``df.write.mode(..).option(..)``, ``df.coalesce(1).write``,
        ``spark.read.format(..)``, ``df.writeTo(t).using(..)``, ``df.writeStream.format(..)``
        (every segment after the ``read`` / ``write`` entry is a builder method); ``None``
        for a terminal (``..load(p)`` / ``..parquet(p)`` / ``..start()`` yield a DataFrame /
        None / a query, not the writer) and for anything that is not a Spark IO chain."""
        if not isinstance(node, (ast.Call, ast.Attribute)):
            return None
        chain = self._chain(node)
        for i, part in enumerate(chain[1:], start=1):
            if part in _SPARK_IO_CHAIN:
                return chain if all(p in _SPARK_IO_BUILDER_METHODS for p in chain[i + 1:]) else None
        return None

    def _is_write_handle(self, arg: ast.AST) -> bool:
        """A writer argument that is not a path: a handle the notebook bound, an inline
        buffer constructor, or ``sys.stdout`` / ``sys.stderr``."""
        if isinstance(arg, ast.Name):
            return arg.id in self.ctx.handles
        if isinstance(arg, ast.Call):
            return self._is_write_handle_ctor(arg)
        if isinstance(arg, ast.Attribute):
            return tuple(self._chain(arg)) in _STREAM_CHAINS
        return False

    def _is_join_operand(self, node: ast.Name) -> bool:
        """``sep.join(node)``: the one read of a list literal that cannot mutate it."""
        parent = self._parents.get(id(node))
        return isinstance(parent, ast.Call) and isinstance(parent.func, ast.Attribute) \
            and parent.func.attr == "join" and len(parent.args) == 1 and parent.args[0] is node

    def _resolve_value(self, node: Optional[ast.AST]) -> Optional[ast.AST]:
        """Follow a plain name to the string node it was bound to, if known."""
        if isinstance(node, ast.Name):
            return self.ctx.constants.get(node.id)
        return node

    def _resolve_literal(self, node: Optional[ast.AST]) -> Optional[str]:
        """Literal text behind ``node``: a constant, a name bound once, ``Path("x")``,
        ``"a" + "b"``, ``"/".join(["a", "b"])`` or ``os.path.join("a", "b")`` over literals."""
        node = self._resolve_value(node)
        if node is None:
            return None
        arg = self._path_ctor_arg(node)
        if arg is not None:
            node = self._resolve_value(arg)
        return self._fold_literal(node)

    def _chain(self, node: ast.AST) -> List[str]:
        return self.ctx.resolve(_attr_chain(node))

    def _is_path_class(self, node: Optional[ast.AST]) -> bool:
        """``Path`` / ``pathlib.Path`` / ``PurePosixPath`` .. (also through an alias)."""
        if not isinstance(node, (ast.Name, ast.Attribute)):
            return False
        chain = self._chain(node)
        return bool(chain) and chain[-1] in _PATH_CLASSES and (len(chain) == 1 or chain[0] == "pathlib")

    def _path_ctor_arg(self, node: ast.AST) -> Optional[ast.AST]:
        """``Path("x")`` / ``pathlib.Path("x")`` -> the path argument node."""
        if isinstance(node, ast.Call) and self._is_path_class(node.func):
            return _first_arg(node, ()) if node.args else None
        return None

    def _path_receiver(self, node: Optional[ast.AST],
                       seen: Optional[Set[str]] = None) -> Tuple[bool, Optional[ast.AST]]:
        """Is ``node`` a ``pathlib.Path`` value, and which constructor argument fixes its path?

        ``(True, arg)``: ``Path(arg)``, or ``.resolve()`` / ``.absolute()`` /
        ``.expanduser()`` (on an absolute literal) on one, directly or through a
        name bound once to it: the path is judged on ``arg``.
        ``(True, None)``: a Path whose path the gate cannot resolve: ``Path()`` /
        ``Path.cwd()`` / ``Path.home()``, ``.parent`` / ``.parents[i]`` /
        ``.joinpath()`` / ``.with_name()`` / ``Path(..) / x`` / .., a rebound or
        loop-bound name, a Path-annotated parameter (fail closed).
        ``(False, None)``: not a Path the gate can see.
        """
        if node is None:
            return False, None
        if isinstance(node, ast.Name):
            if node.id in self.ctx.paths:
                bound = self.ctx.paths[node.id]
                seen = set() if seen is None else seen
                if bound is None or node.id in seen:
                    return True, None
                seen.add(node.id)
                return self._path_receiver(bound, seen)
            bound = self.ctx.constants.get(node.id)          # P = Path('/x') is also a constant
            arg = self._path_ctor_arg(bound) if bound is not None else None
            return (True, arg) if arg is not None else (False, None)
        if isinstance(node, ast.Call):
            if self._is_path_class(node.func):
                return True, (_first_arg(node, ()) if node.args else None)   # Path(x) / Path()
            if isinstance(node.func, ast.Attribute):
                attr = node.func.attr
                if attr in _PATH_CLASS_CTORS and self._is_path_class(node.func.value):
                    return True, None                                        # Path.cwd() / Path.home()
                if attr in _PATH_IDENTITY_STEPS or attr in _PATH_DERIVED_STEPS:
                    is_path, arg = self._path_receiver(node.func.value, seen)
                    if not is_path:
                        return False, None
                    if attr in _PATH_DERIVED_STEPS:
                        return True, None                                    # Path(x).joinpath(y) / .with_name(..)
                    if attr == "expanduser":
                        lit = self._resolve_literal(arg) if arg is not None else None
                        if lit is None or lit.startswith("~"):
                            return True, None                                # Path('~u/x').expanduser(): the home is unknown
                    return True, arg                                         # Path(x).resolve() / .absolute()
            return False, None
        if isinstance(node, ast.Attribute) and node.attr in _PATH_DERIVED_STEPS:
            return (True, None) if self._path_receiver(node.value, seen)[0] else (False, None)   # Path(x).parent
        if isinstance(node, ast.Subscript):
            return (True, None) if self._path_receiver(node.value, seen)[0] else (False, None)   # Path(x).parents[1]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
            if self._path_receiver(node.left, seen)[0] or self._path_receiver(node.right, seen)[0]:
                return True, None                                            # Path('/etc') / 'passwd'
        return False, None

    def _is_protected_root(self, chain: Sequence[str]) -> bool:
        """Interpreter / compat / shim roots (after alias resolution)."""
        return bool(chain) and chain[0] in _PROTECTED_ROOTS

    # -- indirection: builtins namespace, getattr routes, chain rules ----
    def _call_name(self, func: ast.AST) -> Optional[str]:
        """Single name a call target stands for after alias resolution (``g`` -> ``globals``)."""
        if isinstance(func, ast.Name):
            resolved = self.ctx.resolve([func.id])
            return resolved[0] if len(resolved) == 1 else None
        return None

    def _is_namespace_call(self, node: ast.AST) -> bool:
        """``globals()`` / ``locals()`` / ``vars()`` without arguments: the frame namespace."""
        return isinstance(node, ast.Call) and not node.args and not node.keywords \
            and self._call_name(node.func) in _NAMESPACE_BUILTINS

    def _is_builtins_namespace(self, node: ast.AST) -> bool:
        """True when ``node`` evaluates to the builtins module or its dict.

        ``__builtins__``, ``builtins`` (also through an alias),
        ``globals()["__builtins__"]`` / ``locals()[..]`` / ``vars()[..]``,
        ``<ns>.__dict__``, ``vars(<ns>)`` and ``getattr(<ns>, "__dict__")``.
        """
        if isinstance(node, ast.Name):
            return self.ctx.resolve([node.id])[0] in _BUILTINS_NAMESPACE_ROOTS
        if isinstance(node, ast.Attribute):
            return node.attr == "__dict__" and self._is_builtins_namespace(node.value)
        if isinstance(node, ast.Subscript):
            return _literal_str(node.slice) == "__builtins__" and self._is_namespace_call(node.value)
        if isinstance(node, ast.Call):
            fn = self._call_name(node.func)
            if fn == "vars" and len(node.args) == 1:
                return self._is_builtins_namespace(node.args[0])
            if fn == "getattr" and len(node.args) >= 2 and _literal_str(node.args[1]) == "__dict__":
                return self._is_builtins_namespace(node.args[0])
        return False

    def _route_nodes(self, node: ast.AST) -> List[ast.AST]:
        """Nodes that make up a builtins-namespace route (to mark as consumed)."""
        out: List[ast.AST] = []
        cur: Optional[ast.AST] = node
        while cur is not None:
            out.append(cur)
            if isinstance(cur, (ast.Subscript, ast.Attribute)):
                cur = cur.value
            elif isinstance(cur, ast.Call):
                out.append(cur.func)
                cur = cur.args[0] if cur.args else None
            else:
                break
        return out

    def _consume(self, nodes: Iterable[ast.AST]) -> None:
        for n in nodes:
            self._consumed.add(id(n))

    def _consume_base(self, func: ast.AST) -> None:
        """Mark the calls and the root name under an attribute chain as judged."""
        cur = func
        while True:
            if isinstance(cur, ast.Attribute):
                cur = cur.value
            elif isinstance(cur, ast.Call):
                self._consumed.add(id(cur))
                cur = cur.func
            elif isinstance(cur, ast.Name):
                self._consumed.add(id(cur))
                return
            else:
                return

    def _indirect_route(self, func: ast.AST) -> Optional[Tuple[List[str], str, List[ast.AST], bool]]:
        """Resolve a call target reached through the builtins namespace or ``getattr``.

        Returns ``(chain, route text, nodes to consume, via_builtins)``:
        ``__builtins__["open"]`` -> ``(["open"], ...)``; ``getattr(os, "system")``
        -> ``(["os", "system"], ...)``. ``chain`` is empty when the name is not
        a literal. ``None`` when ``func`` is an ordinary name or attribute.
        """
        if isinstance(func, ast.Subscript) and self._is_builtins_namespace(func.value):
            key = _literal_str(func.slice)
            text = "__builtins__[<dynamic>]" if key is None else f"__builtins__[{key!r}]"
            return ([key] if key is not None else []), text, self._route_nodes(func), True
        if isinstance(func, ast.Attribute) and self._is_builtins_namespace(func.value):
            return [func.attr], f"__builtins__.{func.attr}", self._route_nodes(func), True
        if isinstance(func, ast.Call) and self._call_name(func.func) == "getattr" and len(func.args) >= 2:
            target, key = func.args[0], _literal_str(func.args[1])
            if self._is_builtins_namespace(target):
                text = "getattr(__builtins__, <dynamic>)" if key is None else f"getattr(__builtins__, {key!r})"
                return ([key] if key is not None else []), text, self._route_nodes(func), True
            base = self._chain(target) if isinstance(target, (ast.Name, ast.Attribute, ast.Call)) else []
            if key is not None and base:
                # The inner getattr(...) is still visited (protected root / dynamic
                # name / dunder checks); the outer call is checked as base.key(...).
                return base + [key], f"getattr({'.'.join(base)}, {key!r})", [], False
        return None

    def _attr_rule(self, chain: Sequence[str]) -> Optional[Tuple[str, str, str, str]]:
        """Chain-level refusal ``(rule, target, detail, remediation)`` or ``None``.

        Applied to every attribute chain, to aliases that stand for one
        (``from pickle import loads``) and to ``getattr(x, "literal")``.
        """
        if len(chain) < 2:
            return None
        root = chain[0]
        for i, part in enumerate(chain[1:], start=1):
            if (_is_dunder(part) and part not in _DUNDER_ALLOW) or part in _FRAME_ATTRS:
                target = ".".join(chain[:i + 1])
                return RULE_DUNDER_ESCAPE, target, f"introspection chain {target}", _REM_DUNDER
        for i, part in enumerate(chain[1:], start=1):
            if part in _JVM_BRIDGE_ATTRS or (root in _SPARK_ROOTS and part.startswith("_")):
                target = ".".join(chain[:i + 1])
                return RULE_POLICY_TAMPER, target, f"Spark JVM bridge access {target}", _REM_JVM
            if root in _PROTECTED_ROOTS and part.startswith("_"):
                target = ".".join(chain[:i + 1])
                return RULE_POLICY_TAMPER, target, f"access to a private attribute of {root} ({target})", _REM_TAMPER
            if root in _WATCHED_MODULES and part.startswith("_") and not _is_dunder(part):
                # pickle._loads, torch.serialization._load, yaml._x: the unguarded twin of the public API.
                target = ".".join(chain[:i + 1])
                rule = RULE_DESERIALIZE if root in _DESERIALIZE_MODULES else RULE_INDIRECT
                return rule, target, f"access to a private attribute of {root} ({target})", \
                    _REM_DESERIALIZE if rule == RULE_DESERIALIZE else _REM_INDIRECT
            if part in _YAML_HOOK_ATTRS:
                target = ".".join(chain[:i + 1])
                return RULE_DESERIALIZE, target, f"{target}() registers a constructor that runs while parsing", \
                    _REM_DESERIALIZE
            if part in ("Unpickler", "_Unpickler"):
                target = ".".join(chain[:i + 1])
                return RULE_DESERIALIZE, target, f"deserialisation via {target}", _REM_DESERIALIZE
            reroot = part.lstrip("_") if not _is_dunder(part) else part
            if reroot in _REROOT_NAMES and not (root in _SPARK_ROOTS and reroot in _SPARK_ROOTS):
                # shutil.os.system / pd.io.common.os.environ / self.spark._jvm, the private
                # re-exports tempfile._os / random._os, and a re-export back to the chain's own
                # root (os.path.os.system, os.path.genericpath.os.environ, also through
                # ``import os.path as op``): the chain is re-rooted at the module it names and
                # the module rules apply from there (pyspark.sql.SparkSession is already judged
                # by the Spark rules). The re-rooted chain is strictly shorter, so this ends.
                return self._attr_rule([reroot] + list(chain[i + 1:]))
        # Module rules. One table for every entry point: a direct attribute
        # chain, an alias that stands for one (``from os import system``,
        # ``import posix as p``), ``getattr(mod, "literal")`` and an import.
        if root in _PROCESS_ROOT_REFUSED:
            target = f"{root}.{chain[1]}"
            return RULE_OS_PROCESS, target, f"access to the process module attribute {target}", _REM_PROC
        if root in _CODE_LOAD_ROOT_REFUSED or (root == "site" and chain[1] in _SITE_CODE_LOAD_ATTRS):
            target = f"{root}.{chain[1]}"
            return RULE_BUILTIN_EXEC, target, f"{target}() loads code from a path at run time", _REM_CODE_LOAD
        if root == "os":
            if chain[1] in _OS_ENV_ATTRS:
                return RULE_OS_ENVIRON, f"os.{chain[1]}", f"access to os.{chain[1]}", _REM_ENV
            if chain[1] in _OS_PROCESS_ATTRS or chain[1].startswith(_OS_PROCESS_PREFIXES):
                return RULE_OS_PROCESS, f"os.{chain[1]}", f"access to os.{chain[1]}", _REM_PROC
        if root == "shutil" and chain[1] == "rmtree":
            return RULE_SHUTIL_RMTREE, "shutil.rmtree", "access to shutil.rmtree", \
                "Use dbutils.fs.rm on a sandbox path instead."
        if root in _INDIRECT_MODULES:
            return RULE_INDIRECT, f"{root}.{chain[1]}", f"access to {root}.{chain[1]}", _REM_INDIRECT
        if root == "asyncio" and chain[1] in _ASYNCIO_PROCESS_ATTRS:
            return RULE_OS_PROCESS, f"asyncio.{chain[1]}", f"access to asyncio.{chain[1]}", _REM_PROC
        if chain[-1] in _ASYNCIO_LOOP_PROCESS_ATTRS:
            target = ".".join(chain)
            return RULE_OS_PROCESS, target, f"{target}() starts a subprocess through the event loop", _REM_PROC
        if root == "aidp_compat":
            target = ".".join(chain[:3])
            return RULE_POLICY_TAMPER, target, f"attribute access on the aidp_compat package ({target})", _REM_TAMPER
        if root in _SPARK_ROOTS and chain[-1] in _SPARK_CODE_LOAD_ATTRS:
            target = ".".join(chain)
            what = "distributes a file" if chain[-1] == "addFile" else "loads code"
            return RULE_BUILTIN_EXEC, target, f"{target}() {what} into the session", _REM_CODE_LOAD
        if root in _SPARK_ROOTS and chain[-1] in _SPARK_JVM_FUNCTION_ATTRS:
            target = ".".join(chain)
            return RULE_SQL_REFUSED, target, f"{target}() calls arbitrary Java from the DataFrame API", _REM_SQL_JVM
        if root in _SPARK_ROOTS and chain[-1] in _SPARK_JVM_UDF_ATTRS:
            target = ".".join(chain)
            return RULE_POLICY_TAMPER, target, f"{target}() registers a JVM class", _REM_JVM
        if root in _SPARK_ROOTS and chain[-1] in _SPARK_SESSION_FACTORY_ATTRS:
            target = ".".join(chain)
            return RULE_POLICY_TAMPER, target, \
                f"{target}() creates or fetches a session object that would not carry the runtime SQL guard", \
                _REM_SESSION
        if root == "get_ipython":
            target = ".".join(chain)
            if chain[-1] in _IPYTHON_SHELL_ATTRS:
                return RULE_OS_PROCESS, target, f"IPython shell escape {target}", _REM_PROC
            return RULE_IPYTHON_ESCAPE, target, f"IPython shell object access {target}", _REM_IPYTHON
        if root == "sys" and chain[1] in _SYS_INDIRECT_ATTRS:
            return RULE_INDIRECT, f"sys.{chain[1]}", f"access to sys.{chain[1]}", _REM_INDIRECT
        if root == "gc" and chain[1] in _GC_INDIRECT_ATTRS:
            return RULE_INDIRECT, f"gc.{chain[1]}", f"live object graph access via gc.{chain[1]}", _REM_INDIRECT
        if root == "inspect" and chain[1] in _INSPECT_FRAME_ATTRS:
            return RULE_INDIRECT, f"inspect.{chain[1]}", f"frame access via inspect.{chain[1]}", _REM_INDIRECT
        if root == "traceback" and chain[1] in _TRACEBACK_FRAME_ATTRS:
            return RULE_INDIRECT, f"traceback.{chain[1]}", f"frame access via traceback.{chain[1]}", _REM_FRAMES
        if root == "types" and chain[1] in _TYPES_EXEC_ATTRS:
            return RULE_BUILTIN_EXEC, f"types.{chain[1]}", f"code object construction via types.{chain[1]}", \
                "Dynamic code execution is refused; inline the code as a cell."
        if any(part in _PROCESS_POOL_NAMES for part in chain):
            return RULE_IMPORT_PROCESS, "concurrent.futures.ProcessPoolExecutor", \
                "process pool executor forks worker processes", \
                "Process and FFI modules are not available inside the sandbox; use ThreadPoolExecutor."
        if root == "asyncio" and chain[1] in _ASYNCIO_NETWORK_ATTRS and not self.policy.allow_network:
            return RULE_IMPORT_NETWORK, f"asyncio.{chain[1]}", f"network access via asyncio.{chain[1]}", \
                self._import_remediation(RULE_IMPORT_NETWORK)
        if (root, chain[1]) in _NETWORK_SUBMODULES and not self.policy.allow_network:
            # import http; http.client.HTTPSConnection(..) / http.server.HTTPServer(..),
            # import wsgiref; wsgiref.simple_server.make_server(..), from http import server
            target = f"{root}.{chain[1]}"
            return RULE_IMPORT_NETWORK, target, f"access to the network module {target}", \
                self._import_remediation(RULE_IMPORT_NETWORK)
        if root in _PICKLE_MODULES and chain[1] not in _PICKLE_DUMP_ALLOW:
            # deny-by-default: everything but the dump side constructs objects from bytes
            return RULE_DESERIALIZE, f"{root}.{chain[1]}", f"deserialisation via {root}.{chain[1]}", _REM_DESERIALIZE
        if root in _DESERIALIZE_ATTRS and chain[1] in _DESERIALIZE_ATTRS[root]:
            return RULE_DESERIALIZE, f"{root}.{chain[1]}", f"deserialisation via {root}.{chain[1]}", _REM_DESERIALIZE
        if root == "yaml" and (chain[1] in _YAML_REFUSED_ATTRS or chain[1].startswith(_YAML_REFUSED_PREFIXES)):
            target = f"yaml.{chain[1]}"
            return RULE_DESERIALIZE, target, f"{target} is an unsafe yaml loader / constructor entry point", \
                _REM_DESERIALIZE
        if root == "yaml" and chain[1] == "load":
            return RULE_DESERIALIZE, "yaml.load", "yaml.load without a safe Loader", _REM_DESERIALIZE
        if root == "torch" and any(part in _TORCH_HOOK_ATTRS for part in chain[1:]):
            target = ".".join(chain)
            return RULE_DESERIALIZE, target, f"{target}() widens what weights_only=True may unpickle", _REM_DESERIALIZE
        if root == "torch" and "load" in chain[1:]:
            target = ".".join(chain)
            return RULE_DESERIALIZE, target, f"{target} without weights_only=True", _REM_DESERIALIZE
        if chain[-1] == "read_pickle":
            target = ".".join(chain)
            return RULE_DESERIALIZE, target, f"deserialisation via {target}", _REM_DESERIALIZE
        return None

    def _add_rule(self, node: ast.AST, hit: Optional[Tuple[str, str, str, str]]) -> bool:
        if hit is None:
            return False
        rule, target, detail, remediation = hit
        before = len(self.violations)
        self._add(rule, node, detail, target=target, remediation=remediation)
        return len(self.violations) > before

    # -- imports ------------------------------------------------------
    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            if _module_root(alias.name) == "aidp_compat":
                self._add(RULE_POLICY_TAMPER, node, f"import of '{alias.name}' binds a compat module object",
                          target=alias.name, remediation=_REM_TAMPER)
                continue
            hit = _is_prohibited_module(alias.name, self.policy)
            if hit:
                rule, mod = hit
                self._add(rule, node, f"import of prohibited module '{alias.name}'", target=mod,
                          remediation=self._import_remediation(rule))
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        mod = node.module or ""
        names = {a.name for a in node.names}
        if _module_root(mod) == "aidp_compat":
            self._check_compat_import(node, mod, names)
            self.generic_visit(node)
            return
        hit = _is_prohibited_module(mod, self.policy) if mod else None
        if hit:
            rule, root = hit
            self._add(rule, node, f"import from prohibited module '{mod}'", target=root,
                      remediation=self._import_remediation(rule))
        else:
            chain = list(_import_chain(mod)) if mod else []       # from posix import system -> os.system
            # (``from http import client`` / ``server``, ``from wsgiref import simple_server``
            # are judged by the names loop below through _NETWORK_SUBMODULES in _attr_rule.)
            if chain == ["os"] and "*" in names:
                self._add(RULE_OS_ENVIRON, node, "star import of os brings os.environ into the namespace",
                          target="os.*", remediation=_REM_ENV)
            if chain == ["sys"] and "*" in names:
                self._add(RULE_INDIRECT, node, "import of sys.modules", target="sys.modules",
                          remediation=_REM_INDIRECT)
            # Every name that stands for a gated chain (``from os import system``,
            # ``from pickle import loads``, ``from inspect import currentframe``,
            # ``from concurrent.futures import ProcessPoolExecutor``) is judged
            # as that chain at import time, by the same table as attribute access.
            for name in sorted(names - {"*"}):
                if (mod, name) in (("yaml", "load"), ("torch", "load")):
                    continue  # judged at the call (Loader= / weights_only=)
                if chain and not self._add_rule(node, self._attr_rule(chain + [name])) \
                        and name.startswith("_") and not _is_dunder(name) and chain[0] != "__future__":
                    # from tempfile import _os / from mymod import _helper: a private name of
                    # any module is not its public API (the gated modules are judged above)
                    self._add(RULE_INDIRECT, node, f"import of the private name {mod}.{name}",
                              target=f"{mod}.{name}", remediation=_REM_PRIVATE)
            if "*" in names and chain != ["os"] and _module_root(mod) in _STAR_IMPORT_REFUSED:
                self._add(RULE_INDIRECT, node, f"star import of {mod} binds its API under unqualified names",
                          target=mod + ".*", remediation=_REM_INDIRECT)
        self.generic_visit(node)

    def _check_compat_import(self, node: ast.AST, mod: str, names: Set[str]) -> None:
        parts = mod.split(".")
        sub = parts[1] if len(parts) > 1 else ""
        if sub == "notebook_policy":
            self._add(RULE_POLICY_TAMPER, node, "import from aidp_compat.notebook_policy",
                      target=mod, remediation=_REM_TAMPER)
            return
        if "*" in names:
            self._add(RULE_POLICY_TAMPER, node, f"star import from {mod} binds the policy API",
                      target=mod + ".*", remediation=_REM_TAMPER)
            return
        if sub and sub not in _COMPAT_INTERNAL_MODULES:
            return  # helper modules (safe_io functions, path_translator, ...) are fine by name
        for name in sorted(names):
            if name in _COMPAT_PROTECTED_NAMES or name.startswith("_"):
                self._add(RULE_POLICY_TAMPER, node, f"import of {mod}.{name}", target=f"{mod}.{name}",
                          remediation=_REM_TAMPER)

    @staticmethod
    def _import_remediation(rule: str) -> str:
        if rule == RULE_IMPORT_NETWORK:
            return ("Network access is off for this run. Set AIDP_SANDBOX_ALLOW_NETWORK=1 "
                    "after review, or remove the network call.")
        if rule == RULE_INDIRECT:
            return _REM_INDIRECT
        return "Process and FFI modules are not available inside the sandbox; remove the import."

    # -- attribute access --------------------------------------------
    def visit_Attribute(self, node: ast.Attribute):
        if id(node) in self._consumed:
            self.generic_visit(node)
            return
        chain = self._chain(node)
        if len(chain) < 2:
            # Attribute on an expression the chain cannot name (``().__class__``,
            # ``x[0].__dict__``): the attribute itself is still judged.
            self._add_rule(node, self._attr_rule(["<expr>", node.attr]))
            if not self._receiver_resolved(node):
                self._check_sink_attr(node, "<expr>")
        else:
            if not self._add_rule(node, self._attr_rule(chain)):
                # the rule table did not judge the chain: the private-attribute and
                # sink rules on receivers the gate cannot vouch for apply
                self._check_private_module_attr(node, chain)
                if not self._receiver_resolved(node):
                    self._check_sink_attr(node, ".".join(chain[:-1]))
        self.generic_visit(node)

    def _check_private_module_attr(self, node: ast.AST, chain: Sequence[str]) -> None:
        """``mymod._run(..)`` / ``np._core``: a ``_private`` (non-dunder) attribute of
        an imported module the gate has no rules for. The gated modules' private
        attributes are refused by the rule table; a module the gate does not know
        may re-export the same interpreter internals under a private name, so the
        same rule applies to every imported module."""
        if chain[0] not in self.ctx.modules:
            return
        for i, part in enumerate(chain[1:], start=1):
            if part.lstrip("_") in _REROOT_NAMES:
                return      # tempfile._os / mymod._subprocess: the re-rooting rule judges the chain
            if part.startswith("_") and not _is_dunder(part):
                target = ".".join(chain[:i + 1])
                self._add(RULE_INDIRECT, node, f"access to a private attribute of the imported module {chain[0]} "
                          f"({target})", target=target, remediation=_REM_PRIVATE)
                return

    def _receiver_resolved(self, node: ast.Attribute) -> bool:
        """True when the receiver of ``node`` is a plain name chain the gate knows:
        a module it has rules for, an import / shim alias, a Spark root, or an
        imported module the gate has no rules for when the attribute is not a
        process sink (``import mymod; mymod.system(..)`` is refused like
        ``obj.system(..)``: the module may re-export ``os``; the stdlib homonyms
        in ``_SINK_HOMONYMS`` are the exception). Call results, subscripts,
        unknown names and chains through a ``_private`` segment are not."""
        chain = self._chain(node.value)      # calls skipped: spark.sparkContext.getConf().x is still spark's
        if not chain:
            if isinstance(node.value, ast.Subscript):
                # sys.modules[k].x / __builtins__[k].x: the subscript itself is judged by the table
                inner = self._chain(node.value.value)
                return bool(inner) and inner[0] in _VALUE_GUARDED
            return False                     # lambda / conditional / literal receiver
        root = chain[0]
        if any(part.startswith("_") and not _is_dunder(part) for part in chain[1:]):
            return False                     # mod._private.system: the private segment is opaque
        if root in _VALUE_GUARDED or root in _IPYTHON_NAMES or root in _PY4J_NAMES or root in _DATAFRAME_LIB_ROOTS:
            return True                      # the rule table judges the chain
        if _pure_chain(node.value) is None:
            return False                     # call result on an unknown root: helper().system
        if root in self.ctx.aliases or root in self.ctx.identities:
            if root in self.ctx.imported:
                # import mymod: a module the gate has no rules for vouches for nothing
                return (root, node.attr) in _SINK_HOMONYMS or node.attr not in _SINK_ATTRS
            return True
        return False

    def _check_sink_attr(self, node: ast.Attribute, shown: str) -> None:
        """A process / environment / introspection sink name on a receiver the
        gate cannot resolve (``helper().system``, ``obj.popen``, ``x[0].fork``)
        is refused: the receiver may be any of the gated modules."""
        if node.attr in _SINK_ATTRS:
            self._add(RULE_INDIRECT, node, f"{node.attr}() on a receiver the gate cannot resolve ({shown})",
                      target=f"{shown}.{node.attr}", remediation=_REM_SINK)

    def _value_position_ok(self, node: ast.Name) -> bool:
        """Positions in which a gated module / Spark object may appear: the root
        of an attribute chain, the callee of a call, the whole right-hand side of
        a plain name assignment, an argument of an introspection / display
        builtin, or a bare expression statement."""
        parent = self._parents.get(id(node))
        if parent is None or isinstance(parent, ast.Expr):
            return True
        if isinstance(parent, ast.Attribute):
            return parent.value is node
        if isinstance(parent, ast.Call):
            return parent.func is node or self._call_name(parent.func) in _VALUE_SAFE_CALLEES \
                or self._chain(parent.func) == ["inspect", "getattr_static"]      # judged like getattr
        if isinstance(parent, ast.Assign):
            return parent.value is node and all(isinstance(t, ast.Name) for t in parent.targets)
        if isinstance(parent, ast.AnnAssign):
            return parent.value is node and isinstance(parent.target, ast.Name)
        return False

    def visit_ClassDef(self, node: ast.ClassDef):
        for base in node.bases:
            chain = self._chain(base) if isinstance(base, (ast.Name, ast.Attribute)) else []
            if chain and chain[0] == "yaml" and len(chain) >= 2 and (
                    chain[-1].endswith(("Loader", "Constructor")) or chain[1] in ("loader", "constructor")):
                self._add(RULE_DESERIALIZE, node, f"class {node.name} subclasses the yaml loader {'.'.join(chain)}; "
                          "a subclass can register constructors that run while parsing",
                          target=".".join(chain), remediation=_REM_DESERIALIZE)
        self.generic_visit(node)

    # -- assignments to module / shim attributes ------------------------
    def _check_mutation_target(self, target: ast.AST, what: str) -> None:
        base = target
        keys: List[Optional[str]] = []
        while isinstance(base, ast.Subscript):
            keys.append(_literal_str(base.slice))
            base = base.value
        if keys and self._is_builtins_namespace(base):
            self._add(RULE_POLICY_TAMPER, target, f"{what} to the builtins namespace",
                      target="__builtins__", remediation=_REM_TAMPER)
            self._consume(self._route_nodes(target))
            return
        if keys and self._is_namespace_call(base):
            key = keys[-1]
            if key is None or _is_dunder(key) or key in _PROTECTED_ROOTS or key in _SPARK_ROOTS \
                    or key in _DANGEROUS_BUILTINS:
                self._add(RULE_POLICY_TAMPER, target,
                          f"{what} to a protected name through globals()/locals() ({key or '<dynamic>'})",
                          target=f"globals()[{key!r}]" if key is not None else "globals()[<dynamic>]",
                          remediation=_REM_TAMPER)
            self._consume(self._route_nodes(target))
            return
        if isinstance(base, ast.Name) and keys and self.ctx.resolve([base.id])[0] in _PROTECTED_ROOTS:
            chain = self.ctx.resolve([base.id])
            self._add(RULE_POLICY_TAMPER, target, f"{what} to an item of {chain[0]}",
                      target=".".join(chain), remediation=_REM_TAMPER)
            return
        if isinstance(base, ast.Call) and self._call_name(base.func) == "getattr" and len(base.args) >= 2 \
                and _literal_str(base.args[1]) is not None and isinstance(base.args[0], (ast.Name, ast.Attribute)):
            # getattr(yaml.SafeLoader, "yaml_constructors")["!x"] = h: judged as the chain it names
            chain = self._chain(base.args[0]) + [_literal_str(base.args[1])]
        elif isinstance(base, ast.Attribute):
            chain = self._chain(base)
        else:
            return
        if self._is_protected_root(chain) or (chain and chain[0] in _SPARK_ROOTS):
            self._add(RULE_POLICY_TAMPER, target, f"{what} to an attribute of {chain[0]} ({'.'.join(chain)})",
                      target=".".join(chain), remediation=_REM_TAMPER)
        elif self._is_gated_mutation(chain, item=bool(keys)):
            # yaml.SafeLoader.yaml_constructors["!x"] = h / L.yaml_constructors[..] = h (alias followed) /
            # yaml.SafeLoader.yaml_constructors = {..}: the registry a safe loader consults is rewritten
            rule = RULE_DESERIALIZE if chain[0] in _DESERIALIZE_MODULES else RULE_POLICY_TAMPER
            self._add(rule, target, f"{what} to the state of the gated module {chain[0]} ({'.'.join(chain)})",
                      target=".".join(chain), remediation=_REM_MUTATE)

    @staticmethod
    def _is_gated_mutation(chain: Sequence[str], item: bool) -> bool:
        """A store into the state of a gated module: an item of any attribute of
        it (``yaml.SafeLoader.yaml_constructors[k]``), or an attribute of it
        (``yaml.SafeLoader.yaml_constructors = ..``). ``torch`` is the one gated
        module whose public flag attributes notebooks set as everyday
        configuration (``torch.backends.cudnn.benchmark = True``); its
        serialisation namespace and its registries (item stores) stay refused."""
        if len(chain) < 2 or chain[0] not in _WATCHED_MODULES:
            return False
        if chain[0] == "torch" and not item and "serialization" not in chain:
            return False
        return True

    def visit_Assign(self, node: ast.Assign):
        for t in node.targets:
            elts = t.elts if isinstance(t, (ast.Tuple, ast.List)) else [t]
            for elt in elts:
                self._check_mutation_target(elt, "assignment")
                if isinstance(elt, ast.Attribute) and self._chain(elt) == ["tempfile", "tempdir"]:
                    # tempfile.tempdir = d: every later temporary file is created under d
                    self._check_fs_target(node, node.value, "tempfile.tempdir =")
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign):
        self._check_mutation_target(node.target, "augmented assignment")
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign):
        self._check_mutation_target(node.target, "assignment")
        self.generic_visit(node)

    def visit_Delete(self, node: ast.Delete):
        for t in node.targets:
            self._check_mutation_target(t, "deletion")
        self.generic_visit(node)

    # -- names and subscripts: the builtins namespace and loose builtins ----
    def visit_Name(self, node: ast.Name):
        if isinstance(node.ctx, ast.Load) and id(node) not in self._consumed:
            name = node.id
            resolved = self.ctx.resolve([name])
            root = resolved[0]
            if self.ctx.is_unresolved_taint(name):
                self._add(RULE_INDIRECT, node,
                          f"name '{name}' stood for a protected object earlier in this run and was rebound "
                          "to a value the gate cannot resolve", target=name, remediation=_REM_INDIRECT)
            elif len(resolved) >= 2:
                self._add_rule(node, self._attr_rule(resolved))      # from pickle import loads; loads(...)
            elif root in _BUILTINS_NAMESPACE_ROOTS:
                self._add(RULE_INDIRECT, node, "access to the builtins namespace", target=root,
                          remediation=_REM_INDIRECT)
            elif root in _IPYTHON_NAMES:
                self._add(RULE_IPYTHON_ESCAPE, node, f"IPython shell access via {root}", target=root,
                          remediation=_REM_IPYTHON)
            elif root in _PY4J_NAMES:
                self._add(RULE_POLICY_TAMPER, node, f"py4j gateway access via {root}", target=root,
                          remediation=_REM_JVM)
            elif root in _PROCESS_POOL_NAMES:
                self._add(RULE_IMPORT_PROCESS, node, "process pool executor forks worker processes",
                          target="concurrent.futures.ProcessPoolExecutor",
                          remediation="Process and FFI modules are not available inside the sandbox; "
                                      "use ThreadPoolExecutor.")
            elif root in _DANGEROUS_BUILTINS and id(node) not in self._call_funcs:
                self._add(RULE_INDIRECT, node, f"builtin {root} referenced without being called directly",
                          target=root, remediation=_REM_INDIRECT)
            if len(resolved) == 1 and root in _VALUE_GUARDED and root != "get_ipython" \
                    and not self.ctx.is_unresolved_taint(name) and not self._value_position_ok(node):
                # [os][0].system(..), run(os), (os if c else sys), {'a': os}['a'], return os,
                # lambda: os, dict(m=os), for m in (os,), with os as m, f(m=os), (m := os), *[os]
                self._add(RULE_INDIRECT, node, f"module / protected object '{root}' passed as a value",
                          target=root, remediation=_REM_VALUE)
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare):
        # 'my_var' in globals(): a membership test on the frame namespace with a
        # literal, non-protected key returns a bool and exposes nothing.
        key = _literal_str(node.left)
        if key is not None and len(node.comparators) == 1 and isinstance(node.ops[0], (ast.In, ast.NotIn)) \
                and self._is_namespace_call(node.comparators[0]) and not _is_dunder(key) \
                and key not in _VALUE_GUARDED and key not in _DANGEROUS_BUILTINS:
            self._consume(self._route_nodes(node.comparators[0]))
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript):
        if id(node) not in self._consumed:
            if self._is_builtins_namespace(node.value):
                key = _literal_str(node.slice)
                self._add(RULE_INDIRECT, node,
                          "non-literal key on the builtins namespace" if key is None
                          else f"indirect builtin access to {key}",
                          target="__builtins__[<dynamic>]" if key is None else f"__builtins__[{key!r}]",
                          remediation=_REM_INDIRECT)
                self._consume(self._route_nodes(node.value))
            elif self._is_namespace_call(node.value):
                key = _literal_str(node.slice)
                parent = self._parents.get(id(node))
                dereferenced = (isinstance(parent, (ast.Attribute, ast.Subscript)) and parent.value is node) \
                    or (isinstance(parent, ast.Call) and parent.func is node)
                if key is not None and not _is_dunder(key) and key not in _VALUE_GUARDED \
                        and key not in _DANGEROUS_BUILTINS and key not in self.ctx.tainted \
                        and key not in self.ctx.aliases and key not in self.ctx.identities and not dereferenced:
                    # globals()["my_var"]: the one tolerated use of the frame namespace, a plain
                    # read of a literal key that is not a protected / tainted / alias name and
                    # whose result is not dereferenced (globals()["o"].x is unresolvable: refused).
                    self._consume(self._route_nodes(node.value))
        self.generic_visit(node)

    # -- calls ----------------------------------------------------------
    def visit_Call(self, node: ast.Call):
        if id(node) in self._consumed:
            self.generic_visit(node)
            return
        func = node.func
        route = self._indirect_route(func)
        if route is not None:
            chain, text, nodes, via_builtins = route
            before = len(self.violations)
            if len(chain) == 1:
                self._check_name_call(node, chain[0])
            elif len(chain) >= 2:
                self._check_attribute_call(node, chain, chain[-1])
            if via_builtins and len(self.violations) == before:
                self._add(RULE_INDIRECT, node, f"indirect builtin access via {text}", target=text,
                          remediation=_REM_INDIRECT)
            self._consume(nodes)
        elif isinstance(func, ast.Name):
            self._call_funcs.add(id(func))
            resolved = self.ctx.resolve([func.id])
            if self.ctx.is_unresolved_taint(func.id):
                pass  # visit_Name refuses the rebound alias
            elif len(resolved) == 1:
                self._check_name_call(node, resolved[0], bare=func.id)
            else:
                self._check_attribute_call(node, resolved, resolved[-1])
        elif isinstance(func, ast.Attribute):
            self._check_attribute_call(node, self._chain(func), func.attr)
            self._check_format_call(node, func)
        self.generic_visit(node)

    def _check_name_call(self, node: ast.Call, name: str, bare: Optional[str] = None) -> None:
        """A call to a single (alias-resolved) name: builtins and well-known globals."""
        if name in _DYNAMIC_BUILTINS or (bare in _DYNAMIC_BUILTINS):
            self._add(RULE_BUILTIN_EXEC, node, f"call to builtin {name}()", target=name,
                      remediation="Dynamic code execution is refused; inline the code as a cell.")
        elif name in _NAMESPACE_BUILTINS and not node.args and not node.keywords:
            self._add(RULE_INDIRECT, node, f"{name}() exposes the interpreter namespace", target=f"{name}()",
                      remediation=_REM_INDIRECT)
        elif name in _INTROSPECT_BUILTINS or name in _MUTATE_BUILTINS:
            self._check_introspection(node, name)
        elif name == "open":
            self._check_open(node)
        elif name == "sql":
            self._check_sql(node, "sql")
        elif name in _IPYTHON_NAMES:
            self._add(RULE_IPYTHON_ESCAPE, node, f"IPython shell access via {name}()", target=name,
                      remediation=_REM_IPYTHON)
        elif name in _PY4J_NAMES:
            self._add(RULE_POLICY_TAMPER, node, f"py4j gateway access via {name}()", target=name,
                      remediation=_REM_JVM)
        elif name in _PROCESS_POOL_NAMES:
            self._add(RULE_IMPORT_PROCESS, node, "process pool executor forks worker processes",
                      target="concurrent.futures.ProcessPoolExecutor",
                      remediation="Process and FFI modules are not available inside the sandbox; "
                                  "use ThreadPoolExecutor.")

    # -- str.format templates ----------------------------------------------
    def _fold_literal(self, node: Optional[ast.AST]) -> Optional[str]:
        """Literal text of ``node``: a constant, a name bound once to one, ``"a" + "b"``."""
        node = self._resolve_value(node)
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = self._fold_literal(node.left), self._fold_literal(node.right)
            return None if left is None or right is None else left + right
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "join" and len(node.args) == 1 and not node.keywords:
                # "/".join(["/etc", "passwd"]) / "/".join(parts) with parts bound once to a literal list
                sep = self._fold_literal(node.func.value)
                seq = self._resolve_value(node.args[0])
                if sep is not None and seq is not None and _is_str_sequence(seq):
                    return sep.join(_literal_str(e) for e in seq.elts)
            elif self._chain(node.func) == ["os", "path", "join"] and node.args and not node.keywords:
                parts = [self._fold_literal(a) for a in node.args]
                if all(p is not None for p in parts):
                    return posixpath.join(*parts)
        return _literal_str(node)

    def _resolve_template(self, node: Optional[ast.AST]) -> Optional[str]:
        """The format template behind ``node``, through text-preserving wrappers
        (``textwrap.dedent("...")``, ``"...".strip()``), or ``None``."""
        while isinstance(self._resolve_value(node), ast.Call):
            call = self._resolve_value(node)
            fn = call.func
            if isinstance(fn, ast.Attribute) and fn.attr in _STR_PRESERVING_METHODS:
                node = fn.value
            elif tuple(self._chain(fn)) in _STR_PRESERVING_CALLS and call.args:
                node = call.args[0]
            else:
                return None
        return self._fold_literal(node)

    def _check_format_call(self, node: ast.Call, func: ast.Attribute) -> None:
        """``<template>.format(..)`` / ``.format_map(..)``, ``str.format(t, ..)``,
        ``string.Formatter().format / vformat(t, ..)``: a replacement field can
        walk attributes of the formatted objects (``{0.__class__.__mro__}``)."""
        chain = self._chain(func)
        if func.attr in ("format", "format_map") and chain == ["str", func.attr]:
            if node.args:
                self._check_format_template(node, node.args[0], node.args[1:], node.keywords)
            return
        if func.attr in ("format", "vformat") and chain[:2] == ["string", "Formatter"]:
            if node.args:
                self._check_format_template(node, node.args[0], node.args[1:], node.keywords)
            return
        if func.attr not in ("format", "format_map"):
            return
        if any(part in _SPARK_IO_CHAIN for part in chain[:-1]):
            return   # DataFrameReader / DataFrameWriter .format("delta")
        self._check_format_template(node, func.value, node.args, node.keywords)

    def _check_format_template(self, node: ast.Call, template: ast.AST, args: Sequence[ast.AST],
                               keywords: Sequence[ast.keyword]) -> None:
        text = self._resolve_template(template)
        if text is not None:
            field = _format_field_escape(text)
            if field is not None:
                self._add(RULE_DUNDER_ESCAPE, node, "dunder / frame attribute lookup inside a str.format() field",
                          target=field[:80], remediation=_REM_DUNDER)
            return
        # Unresolvable template. With only literal arguments nothing of the
        # notebook's objects is reachable; with a run-time argument the
        # template decides which of its attributes are walked: fail closed.
        dynamic = any(isinstance(a, ast.Starred) or not isinstance(self._resolve_value(a), ast.Constant)
                      for a in args)
        dynamic = dynamic or any(k.arg is None or not isinstance(self._resolve_value(k.value), ast.Constant)
                                 for k in keywords)
        if dynamic:
            self._add(RULE_DUNDER_ESCAPE, node, "str.format() on a template that cannot be resolved statically",
                      target="<dynamic>", remediation=_REM_FORMAT)

    def _check_attribute_call(self, node: ast.Call, chain: List[str], attr: str) -> None:
        if chain[:1] == ["get_ipython"] and len(chain) >= 2:
            self._add_rule(node, self._attr_rule(chain))
            self._consume_base(node.func)
            return
        if attr == "pipe":
            self._check_pipe(node, chain)
        if len(chain) == 2 and tuple(chain) in _ATTR_ACCESSOR_CALLS:
            self._check_attr_accessor(node, chain)
            return
        if chain == ["functools", "reduce"] and node.args:
            fn = node.args[0]
            fn_name = self._call_name(fn) if isinstance(fn, ast.Name) else None
            fn_chain = tuple(self._chain(fn)) if isinstance(fn, (ast.Attribute, ast.Name)) else ()
            if fn_name in _ATTR_RESOLVING_NAMES or fn_chain in _ATTR_ACCESSOR_CALLS:
                self._add(RULE_INDIRECT, node, "functools.reduce() with an attribute-resolving function walks a "
                          "dotted path the gate cannot see", target="functools.reduce(getattr, ...)",
                          remediation=_REM_INDIRECT)
                return
        if chain[:1] == ["inspect"] and attr == "getattr_static":
            self._check_introspection(node, "getattr")
            self._consume_base(node.func)
            return
        if chain[:1] == ["inspect"] and attr == "getmembers":
            self._add(RULE_INDIRECT, node, "inspect.getmembers() enumerates every attribute of an object, "
                      "private and dunder names included", target="inspect.getmembers", remediation=_REM_INDIRECT)
            return
        if attr in _REGISTRY_MUTATORS and len(chain) >= 3 and chain[0] in _WATCHED_MODULES \
                and tuple(chain[:2]) not in _MUTABLE_GATED_CHAINS:
            # yaml.SafeLoader.yaml_constructors.update({..}) / .setdefault / .pop / sys.meta_path.append:
            # the registry a gated module consults is rewritten in place (sys.path is the exception)
            shown = ".".join(chain)
            rule = RULE_DESERIALIZE if chain[0] in _DESERIALIZE_MODULES else RULE_POLICY_TAMPER
            self._add(rule, node, f"{shown}() mutates the state of the gated module {chain[0]}",
                      target=shown, remediation=_REM_MUTATE)
            return
        if attr == "load" and isinstance(node.func, ast.Attribute) and not self._receiver_resolved(node.func) \
                and not any(part in _SPARK_IO_CHAIN for part in chain[:-1]):
            # <unresolved>.load(p, **kw) / .load(p, None, True): the receiver may be numpy / torch /
            # any keyword-gated loader, and the switch is hidden in the splat or passed positionally
            if self._refuse_loader_args(node, ".".join(chain), positional_switch=2):
                return
        for k in node.keywords:
            # numpy / torch loader switches, keyed on the keyword so the receiver does not matter
            if k.arg == "allow_pickle" and not (isinstance(k.value, ast.Constant) and k.value.value is False):
                literal_true = isinstance(k.value, ast.Constant) and k.value.value is True
                self._add(RULE_DESERIALIZE, node, f"{'.'.join(chain)}(allow_pickle=...) not the literal False",
                          target="numpy.load(allow_pickle=True)" if literal_true
                          else f"{'.'.join(chain)}(allow_pickle=<dynamic>)", remediation=_REM_DESERIALIZE)
                return
            if k.arg == "weights_only" and attr == "load" \
                    and not (isinstance(k.value, ast.Constant) and k.value.value is True):
                self._add(RULE_DESERIALIZE, node, f"{'.'.join(chain)}(weights_only=...) not the literal True",
                          target=".".join(chain), remediation=_REM_DESERIALIZE)
                return
            if k.arg == "capture_locals" and not (isinstance(k.value, ast.Constant) and k.value.value is False):
                # traceback.StackSummary.extract(.., capture_locals=True) / TracebackException(..): every
                # frame's locals are copied into the summary, keyed on the keyword so the receiver does not matter
                shown = ".".join(chain)
                self._add(RULE_INDIRECT, node, f"{shown}(capture_locals=...) copies the locals of every frame",
                          target=f"{shown}(capture_locals=True)", remediation=_REM_FRAMES)
                return
        if len(chain) == 2 and (chain[0], attr) in _IO_OPEN_CALLS:
            call_name = f"{chain[0]}.{attr}()"
            if (chain[0], attr) in (("io", "open"), ("codecs", "open")):
                self._check_open(node, call_name)       # mode-aware, like builtin open()
                return
            if self._refuse_splat(node, call_name):
                return
            arg = _first_arg(node, ("file", "filename", "name", "path", "database", "files"))
            if arg is not None:
                self._check_io_target(node, arg, call_name)
            return
        reader = self._reader_call_name(chain, attr)
        if reader is not None:
            arg = _first_arg(node, _READER_PATH_KWARGS)
            if arg is not None:
                self._check_read_path(node, arg, reader)
        writer = self._writer_call_spec(chain, attr)
        if writer is not None:
            shown, index, kwargs = writer
            self._check_lib_write(node, shown, index, kwargs, dynamic_ok=False)
            return
        if chain[:1] == ["tempfile"] and attr in _TEMPFILE_CREATORS:
            self._check_tempfile(node, attr)
            return
        if chain[:1] == ["os"] and attr in ("chdir", "fchdir"):
            # Relative paths are tolerated for open(); the working directory must therefore stay put.
            lit = self._resolve_literal(_first_arg(node, ("path",)))
            if lit is None:
                self._add(RULE_FS_DYNAMIC, node, f"os.{attr}() with a path that cannot be resolved statically",
                          target="<dynamic>", remediation="Keep the working directory; use absolute sandbox paths.")
            elif not (_is_abs_path(lit) and self.policy.path_in_sandbox(lit)):
                self._add(RULE_FS_PATH, node, f"os.{attr}() target outside the sandbox prefix", target=lit,
                          remediation=f"Only directories under {self.policy.prefix} may become the working directory.")
            return
        if self._check_deserialize_call(node, chain, attr):
            return
        if len(chain) >= 3 and chain[-2] == "fs" and attr in _DBFS_WRITE_METHODS:
            self._check_dbfs(node, ".".join(chain[-3:]), attr)
        elif attr in _TABLE_WRITE_METHODS and (any(p in _SPARK_WRITER_CHAIN for p in chain) or attr == "writeTo"):
            self._check_table_write(node, attr)
        elif attr in _SQL_METHODS and len(chain) >= 2:
            self._check_sql(node, ".".join(chain[-2:]))
        elif attr in _SPARK_TABLE_READ_ATTRS and (chain[0] in _SPARK_ROOTS
                                                  or any(p in _SPARK_READER_CHAIN for p in chain[:-1])):
            self._check_table_read(node, ".".join(chain))          # spark.table(..) / spark.read.table(..)
        elif chain[0] in _SPARK_ROOTS and len(chain) >= 3 and chain[-2] == "catalog" \
                and attr in _SPARK_CATALOG_USE_ATTRS:
            self._check_catalog_use(node, ".".join(chain), attr)   # the USE rule
        elif chain[0] in _SPARK_ROOTS and len(chain) >= 3 and chain[-2] == "catalog" \
                and attr in _SPARK_CATALOG_CREATE_ATTRS:
            self._check_catalog_create_table(node, ".".join(chain))     # CREATE TABLE .. USING .. LOCATION ..
        elif chain[0] in _SPARK_ROOTS and ((len(chain) >= 3 and chain[-2] == "conf" and attr in _SPARK_CONF_SET_ATTRS)
                                           or attr == "setConf"):
            self._check_conf_set(node, ".".join(chain))            # the SET rule
        elif attr in _SPARK_PROVIDER_METHODS and any(p in _SPARK_IO_CHAIN for p in chain[:-1]):
            self._check_provider(node, _first_arg(node, ("source", "provider")), ".".join(chain))
        elif attr in _SPARK_NETWORK_IO_METHODS and any(p in _SPARK_IO_CHAIN for p in chain[:-1]) \
                and not self.policy.allow_network:
            shown = ".".join(chain)
            self._add(RULE_IMPORT_NETWORK, node, f"{shown}() opens a network connection", target=shown,
                      remediation=_REM_NETWORK_SOURCE)
        elif attr in _SPARK_CALL_FUNCTION_ATTRS and chain[0] in _SPARK_ROOTS:
            self._check_call_function(node, ".".join(chain))
        elif attr in _PATH_WRITE_METHODS and any(p in _SPARK_WRITER_CHAIN for p in chain):
            self._check_path_write(node, attr, "writeStream" if "writeStream" in chain else "write")
        elif attr in _STREAM_PATH_WRITE_METHODS and "writeStream" in chain:
            self._check_path_write(node, attr, "writeStream")       # df.writeStream...start(path)
        elif attr in _WRITER_OPTION_METHODS and (any(p in _SPARK_WRITER_CHAIN for p in chain) or "writeTo" in chain):
            self._check_writer_option(node, attr, "writeStream" if "writeStream" in chain else "write")
        elif attr in _WRITER_OPTION_METHODS and any(part in _SPARK_READER_CHAIN for part in chain):
            self._check_reader_option(node, attr, ".".join(chain))
        elif attr in _FRAME_WRITER_ATTRS and not any(part in _SPARK_IO_CHAIN for part in chain[:-1]):
            # df.to_csv(p) / pl_df.write_parquet(p) / fig.savefig(p) / arr.tofile(p): unambiguous writers
            self._check_lib_write(node, ".".join(chain), 0, _FRAME_WRITER_KWARGS, dynamic_ok=False)
        elif attr in _GENERIC_SAVE_ATTRS and not any(part in _SPARK_IO_CHAIN for part in chain[:-1]):
            # wb.save(p) / model.save(p) / img.save(p): the receiver cannot be typed, a literal path can be judged
            self._check_lib_write(node, ".".join(chain), 0, _GENERIC_SAVE_KWARGS, dynamic_ok=True)
        elif attr == "open" and chain[:1] == ["builtins"]:
            self._check_open(node)
        elif chain[:1] == ["os"] and attr in _OS_FS_ATTRS:
            self._check_fs_call(node, f"os.{attr}", two_arg=attr in _OS_FS_TWO_ARG)
        elif chain[:1] == ["os"] and chain[1:2] == ["path"] and attr in ("unlink", "remove"):
            self._check_fs_call(node, f"os.path.{attr}")
        elif chain[:1] == ["shutil"] and attr in _SHUTIL_FS_ATTRS:
            self._check_fs_call(node, f"shutil.{attr}", two_arg=True, dst_only=attr != "move")
        elif attr in _PATHLIB_IO_ATTRS or attr in _PATHLIB_FS_ATTRS or attr in _PATHLIB_RESOLVED_ONLY_ATTRS:
            handled = self._check_pathlib(node, attr)
            if not handled and attr == "open" and chain[0] not in _WATCHED_MODULES:
                # <anything>.open(path): a literal local path is checked, a dynamic one recorded
                self._check_receiver_open(node, chain)

    def _refuse_splat(self, node: ast.Call, call_name: str) -> bool:
        """``open(*args)`` / ``open(**kw)``: path and mode are invisible; refused (fail closed)."""
        if any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords):
            self._add(RULE_OPEN_DYNAMIC, node, f"{call_name} with *args / **kwargs hides the path and mode",
                      target="<dynamic>", remediation="Pass the path (and mode) as plain arguments the gate can see.")
            return True
        return False

    def _is_string_typed(self, node: ast.AST) -> bool:
        """A string-valued argument: literal, f-string, concatenation / ``%`` with a literal,
        ``.format(..)`` / ``.join(..)`` result, or a name bound once to one of these."""
        node = self._resolve_value(node)
        if node is None:
            return False
        if _is_stringish(node):
            return True
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ("format", "join"):
            return True
        return isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod) and _is_stringish(node.left)

    def _check_pipe(self, node: ast.Call, chain: List[str]) -> None:
        """``.pipe(cmd)`` on an RDD runs ``cmd`` in a shell on every executor.

        Refused when the receiver chain is rooted at a Spark object or passes
        through an RDD-producing step (``parallelize``, ``textFile``, ``.rdd``,
        a name bound to one), or when the argument is string-typed on any
        receiver. A callable-looking argument (lambda, a ``def`` in the run, an
        attribute chain) on a non-Spark receiver is ``DataFrame.pipe(func)``.
        Residual: a bare name the gate cannot type on an unknown receiver is
        allowed and recorded (``unverified_pipe_callable``).
        """
        shown = ".".join(chain)
        arg = _first_arg(node, ("command", "func"))
        if arg is None:
            return
        spark_receiver = chain[0] in _SPARK_ROOTS or chain[0] in self.ctx.rdd_names or "sparkContext" in chain \
            or any(part in _RDD_CHAIN_ATTRS for part in chain[:-1])
        if spark_receiver or self._is_string_typed(arg):
            self._add(RULE_OS_PROCESS, node, f"{shown}() pipes data through a shell command on the executors",
                      target=f"{shown}()", remediation=_REM_PIPE)
            return
        if isinstance(arg, ast.Name) and arg.id not in self.ctx.functions and arg.id not in self.ctx.aliases:
            if not self._receiver_accounted(chain):
                # thing.pipe(cmd): the receiver was never bound in the notebook, so it cannot be
                # typed as a DataFrame at all, and the argument may be a command string
                self._add(RULE_OS_PROCESS, node, f"{shown}() on a receiver the notebook never bound, with an "
                          "argument the gate cannot type", target=f"{shown}()", remediation=_REM_RECEIVER)
                return
            self._note(NOTE_UNVERIFIED_PIPE, node,
                       f"{shown}({arg.id}) with an argument the gate cannot type; allowed as DataFrame.pipe(func) "
                       "and unverified", target=f"{shown}({arg.id})")

    def _check_attr_accessor(self, node: ast.Call, chain: List[str]) -> None:
        """``operator.attrgetter("a.b")`` / ``operator.methodcaller("name", ..)``:
        judged like ``getattr`` with a literal name. A non-literal name is
        refused; every dotted segment of a literal one is run through the
        dunder / frame / private / JVM-bridge / sink rules. The object the
        getter is applied to is judged where it appears (a gated module or
        Spark object handed to the getter is a value, refused there)."""
        shown = ".".join(chain)
        names = node.args if chain[1] == "attrgetter" else node.args[:1]
        if not names:
            return
        for name_node in names:
            lit = _literal_str(name_node)
            if lit is None:
                self._add(RULE_INDIRECT, node, f"{shown}() with an attribute name that is not a literal",
                          target=f"{shown}(<dynamic>)", remediation=_REM_INDIRECT)
                return
            for seg in lit.split("."):
                if self._add_rule(node, self._attr_rule(["<expr>", seg])):
                    return
                if seg in _SINK_ATTRS or (seg.startswith("_") and not _is_dunder(seg)):
                    self._add(RULE_INDIRECT, node, f"{shown}({lit!r}) reaches a process sink / private attribute",
                              target=f"{shown}({lit!r})", remediation=_REM_INDIRECT)
                    return

    def _check_receiver_open(self, node: ast.Call, chain: List[str]) -> None:
        """``<receiver>.open(path, ..)`` on a receiver that is not a Path / stdlib
        module: a literal local path outside the sandbox is refused, a dynamic
        one is allowed and recorded (object-store filesystems, zip members)."""
        shown = ".".join(chain)
        if self._refuse_splat(node, f"{shown}()"):
            return
        arg = _first_arg(node, ("file", "path", "name"))
        if arg is None:
            return
        lit = self._resolve_literal(arg)
        if lit is not None:
            if _is_local_path(lit) and not self.policy.path_in_sandbox(_local_path_for_check(lit)):
                self._add(RULE_OPEN_PATH, node, f"{shown}() on a local path outside the sandbox prefix", target=lit,
                          remediation=_REM_READ_PATH)
            return
        head = self._literal_head(arg)
        if head and _is_local_path(head) and not self.policy.path_in_sandbox(_local_path_for_check(head)):
            self._add(RULE_OPEN_PATH, node, f"{shown}() on a path under a local prefix outside the sandbox",
                      target=head, remediation=_REM_READ_PATH)
            return
        if not self._receiver_accounted(chain):
            # thing.open(path_var): neither the receiver nor the path is anything the notebook
            # bound; the gate cannot tell it from the builtin or a Path, so it is judged as one
            self._add(RULE_OPEN_DYNAMIC, node, f"{shown}() on a receiver the notebook never bound, with a path "
                      "that cannot be resolved statically", target=f"{shown}(<dynamic>)",
                      remediation=_REM_RECEIVER)
            return
        self._note(NOTE_UNVERIFIED_READ_PATH, node,
                   f"{shown}() with a path that cannot be resolved statically; the open is allowed and unverified "
                   "(the cluster's IAM scope is the control)", target=f"{shown}(<dynamic>)")

    def _receiver_accounted(self, chain: Sequence[str]) -> bool:
        """The root of a receiver chain is something the run bound (an import, an
        assignment, a parameter, a loop / with target) or a library root the gate
        knows. An unbound bare name is a value the gate never saw come into being."""
        return bool(chain) and (self.ctx.is_bound(chain[0]) or chain[0] in _DATAFRAME_LIB_ROOTS
                                or chain[0] in _VALUE_GUARDED)

    def _literal_head(self, arg: ast.AST) -> Optional[str]:
        resolved = self._resolve_value(arg)
        return _joined_literal_head(resolved) if resolved is not None else None

    def _check_introspection(self, node: ast.Call, builtin: str) -> None:
        arg = node.args[0] if node.args else None
        if arg is None:
            return
        chain = self._chain(arg) if isinstance(arg, (ast.Attribute, ast.Name, ast.Call)) else []
        mutate = builtin in _MUTATE_BUILTINS
        name = _literal_str(node.args[1]) if len(node.args) >= 2 else None
        # A Spark object tolerates getattr with a literal, public attribute name
        # (``getattr(spark, "version")``); the name is then judged like
        # ``spark.version`` below. Dynamic names, private / dunder names, vars()
        # and every mutation stay refused. The interpreter / compat roots have
        # no legitimate reflective use in a notebook: refused whatever the name.
        spark_obj = bool(chain) and chain[0] in _SPARK_ROOTS
        spark_refused = spark_obj and (mutate or builtin == "vars" or name is None or name.startswith("_"))
        # vars(<any gated module>) dumps its namespace: refused. getattr(<gated module>, "literal")
        # is judged by the rule table below (getattr(pickle, "loads") is pickle.loads).
        # setattr / delattr on any gated module (setattr(yaml.SafeLoader, "yaml_constructors", {..}))
        # rewrites state the gate reasons about: refused like an attribute assignment.
        protected = (self._is_protected_root(chain)
                     or ((builtin == "vars" or mutate) and bool(chain) and chain[0] in _WATCHED_MODULES)) \
            and not spark_obj
        if protected or spark_refused or self._is_builtins_namespace(arg):
            rule = RULE_POLICY_TAMPER if mutate else RULE_INDIRECT
            shown = ".".join(chain) if chain else "__builtins__"
            self._add(rule, node, f"{builtin}() on {shown}", target=shown,
                      remediation=_REM_TAMPER if rule == RULE_POLICY_TAMPER else _REM_INDIRECT)
            return
        if builtin == "vars" or len(node.args) < 2:
            return
        shown = ".".join(chain) if chain else "<expr>"
        if name is None:
            # getattr(obj, "sys" + "tem") / setattr(obj, key, v): the attribute name is built at run time.
            self._add(RULE_INDIRECT, node, f"{builtin}() with an attribute name that is not a literal",
                      target=f"{builtin}({shown}, <dynamic>)", remediation=_REM_INDIRECT)
        elif (_is_dunder(name) and name not in _DUNDER_ALLOW) or name in _FRAME_ATTRS:
            self._add(RULE_DUNDER_ESCAPE, node, f"{builtin}() reaching {shown}.{name}",
                      target=f"{shown}.{name}", remediation=_REM_DUNDER)
        elif name in _JVM_BRIDGE_ATTRS:
            self._add(RULE_POLICY_TAMPER, node, f"{builtin}() reaching the Spark JVM bridge {shown}.{name}",
                      target=f"{shown}.{name}", remediation=_REM_JVM)
        elif chain:
            self._add_rule(node, self._attr_rule(chain + [name]))

    def _check_deserialize_call(self, node: ast.Call, chain: List[str], attr: str) -> bool:
        """Call-level judgement of loaders whose safety depends on an argument.

        Returns True when the call was judged here (safe or refused).
        """
        root = chain[0]
        if root == "yaml" and attr == "load" and len(chain) == 2:
            # Safe only when the Loader resolves, through aliases and from-imports, to
            # exactly yaml.SafeLoader / CSafeLoader / BaseLoader / CBaseLoader. A bare
            # name the gate cannot resolve (``from yaml import Loader as SafeLoader``
            # resolves to yaml.Loader) is not safe. ``load_all`` is refused outright.
            loader = node.args[1] if len(node.args) >= 2 else None
            for k in node.keywords:
                if k.arg == "Loader":
                    loader = k.value
            lchain = self._chain(loader) if isinstance(loader, (ast.Name, ast.Attribute)) else []
            if len(lchain) == 2 and lchain[0] == "yaml" and lchain[1] in _YAML_SAFE_LOADERS:
                self._consumed.add(id(node.func))
            else:
                self._add(RULE_DESERIALIZE, node, "yaml.load() without a safe Loader", target="yaml.load",
                          remediation=_REM_DESERIALIZE)
                self._consumed.add(id(node.func))
            return True
        if root == "torch" and attr == "load":
            # torch.load / torch.serialization.load / torch.jit.load: any depth under torch.
            # Safe only with the literal weights_only=True AND no pickle_module (keyword or the
            # third positional): an explicit unpickler is honoured for the non-tensor payload.
            target = ".".join(chain)
            self._consumed.add(id(node.func))
            if self._refuse_loader_args(node, target, positional_switch=2):
                return True
            safe = any(k.arg == "weights_only" and isinstance(k.value, ast.Constant) and k.value.value is True
                       for k in node.keywords)
            if not safe:
                self._add(RULE_DESERIALIZE, node, f"{target}() without weights_only=True", target=target,
                          remediation=_REM_DESERIALIZE)
            elif any(k.arg == "pickle_module" for k in node.keywords):
                self._add(RULE_DESERIALIZE, node, f"{target}() with an explicit pickle_module", target=target,
                          remediation=_REM_DESERIALIZE)
            return True
        if root == "numpy" and attr == "load" and len(chain) == 2:
            # allow_pickle must be absent or the literal False: as a keyword (the generic
            # keyword rule in _check_attribute_call refuses every other value), as the
            # third positional argument, or hidden in *args / **kwargs (refused).
            self._refuse_loader_args(node, ".".join(chain), positional_switch=2)
            return True
        return False

    def _refuse_loader_args(self, node: ast.Call, shown: str, positional_switch: int) -> bool:
        """Loader arguments the gate cannot judge: ``*args`` / ``**kwargs`` (the
        safety switch may be inside) or the switch passed positionally with
        anything but the literal ``False`` / ``None`` (``np.load(p, None, True)``,
        ``torch.load(p, None, custom_pickle)``). Returns True when refused."""
        if any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords):
            self._add(RULE_DESERIALIZE, node, f"{shown}() with *args / **kwargs hides the loader's safety switch",
                      target=f"{shown}(<dynamic>)", remediation=_REM_LOADER_ARGS)
            return True
        if len(node.args) > positional_switch:
            switch = node.args[positional_switch]
            if not (isinstance(switch, ast.Constant) and switch.value in (False, None)):
                self._add(RULE_DESERIALIZE, node, f"{shown}() with the loader's safety switch passed positionally",
                          target=f"{shown}(<positional>)", remediation=_REM_LOADER_ARGS)
                return True
        return False

    def _check_open(self, node: ast.Call, call_name: str = "open()"):
        """``open(file, ..)`` and its twins ``io.open`` / ``codecs.open``.

        ``*args`` / ``**kwargs`` are refused (the path is invisible). A literal
        or foldable path (``"a" + "b"``, ``"/".join([..])``, ``os.path.join(..)``,
        a name bound once) is checked against the sandbox, the literal head of
        an f-string / concatenation is checked as a directory, and a path the
        gate cannot resolve is refused in every mode (``NBP-OPEN-DYNAMIC``): a
        read of an arbitrary local file leaves the sandbox as much as a write,
        and the library-reader scope decision does not extend to ``open()``.
        """
        if self._refuse_splat(node, call_name):
            return
        arg = _first_arg(node, ("file",))
        if arg is None:
            return
        self._check_io_target(node, arg, call_name)

    # -- library readers: local path literals ------------------------------
    @staticmethod
    def _reader_call_name(chain: List[str], attr: str) -> Optional[str]:
        """Dotted name when ``chain`` is a known path reader, else ``None``.

        pandas-style ``read_* / scan_* / open_*`` on a dataframe library,
        the loaders in ``_LIB_READER_CALLS``, ``spark.read[Stream].<fmt>/load``
        and ``sparkContext.textFile``-style RDD readers.
        """
        if len(chain) < 2:
            return None
        root = chain[0]
        if root in _DATAFRAME_LIB_ROOTS and attr.startswith(_DATAFRAME_READ_PREFIXES) \
                and attr not in _DATAFRAME_NON_PATH_READERS:
            return ".".join(chain)
        if tuple(chain) in _LIB_READER_CALLS:
            return ".".join(chain)
        if attr in _SPARK_PATH_READ_METHODS and any(part in _SPARK_READER_CHAIN for part in chain[:-1]):
            return ".".join(chain)
        if attr in _SC_PATH_READ_METHODS and (root in _SPARK_ROOTS or "sparkContext" in chain):
            return ".".join(chain)
        return None

    # -- library writers: local path literals and dynamic targets ------------
    @staticmethod
    def _writer_call_spec(chain: List[str], attr: str) -> Optional[Tuple[str, int, Tuple[str, ...]]]:
        """``(dotted name, positional index of the path, keyword names)`` when
        ``chain`` is a module-rooted writer in ``_LIB_WRITER_CALLS`` (or
        ``mlflow.<flavor>.save_model``), else ``None``."""
        if len(chain) < 2:
            return None
        spec = _LIB_WRITER_CALLS.get(tuple(chain))
        if spec is None and chain[0] == "mlflow" and attr == "save_model":
            spec = (1, ("path",))
        if spec is None:
            return None
        return ".".join(chain), spec[0], spec[1]

    @staticmethod
    def _positional_or_keyword(node: ast.Call, index: int, kwargs: Sequence[str]) -> Optional[ast.AST]:
        """Argument at positional slot ``index`` (when no ``*args`` precedes it) or under one of ``kwargs``."""
        if len(node.args) > index and not any(isinstance(a, ast.Starred) for a in node.args[:index + 1]):
            return node.args[index]
        for k in node.keywords:
            if k.arg in kwargs:
                return k.value
        return None

    def _check_lib_write(self, node: ast.Call, shown: str, index: int, kwargs: Sequence[str],
                         dynamic_ok: bool) -> None:
        """A library writer's path argument. Literal (or foldable) local path
        outside the sandbox -> ``NBP-PATH-WRITE``; the literal head of an
        f-string / concatenation outside -> ``NBP-PATH-WRITE``; a path the gate
        cannot resolve -> ``NBP-PATH-WRITE-DYNAMIC`` unless ``dynamic_ok`` (the
        generic ``.save`` receivers) or the argument is a write handle the
        notebook bound. ``*args`` / ``**kwargs`` hide the path and are refused
        for the unambiguous writers. ``None`` / no argument is not a path."""
        call_name = f"{shown}()"
        if not dynamic_ok and (any(isinstance(a, ast.Starred) for a in node.args)
                               or any(k.arg is None for k in node.keywords)):
            self._add(RULE_PATH_WRITE_DYNAMIC, node, f"{call_name} with *args / **kwargs hides the path",
                      target="<dynamic>", remediation=_REM_WRITE_PATH)
            return
        arg = self._positional_or_keyword(node, index, kwargs)
        if arg is None or (isinstance(arg, ast.Constant) and arg.value is None):
            return
        lit = self._resolve_literal(arg)
        if lit is not None:
            if _is_abs_path(lit) and not self.policy.path_in_sandbox(lit):
                self._add(RULE_PATH_WRITE, node, f"{call_name} writes a path outside the sandbox prefix", target=lit,
                          remediation=_REM_WRITE_PATH)
            return
        head = self._literal_head(arg)
        if head and _is_local_path(head) and not self.policy.path_in_sandbox(_local_path_for_check(head)):
            self._add(RULE_PATH_WRITE, node, f"{call_name} writes under a local prefix outside the sandbox",
                      target=head, remediation=_REM_WRITE_PATH)
            return
        if dynamic_ok or self._is_write_handle(arg):
            return
        self._add(RULE_PATH_WRITE_DYNAMIC, node, f"{call_name} with a path that cannot be resolved statically",
                  target="<dynamic>", remediation=_REM_WRITE_PATH)

    def _check_tempfile(self, node: ast.Call, attr: str) -> None:
        """``tempfile.<creator>(.., prefix=, dir=)``: a literal ``dir`` (or an
        absolute ``prefix``, which ``os.path.join`` lets escape ``dir``) outside
        the sandbox is ``NBP-FS-PATH``; the default directory and a value the
        gate cannot resolve stay allowed (in-sandbox temp usage is everyday code)."""
        prefix_pos, dir_pos = _TEMPFILE_CREATORS[attr]
        shown = f"tempfile.{attr}()"
        for label, pos in (("prefix", prefix_pos), ("dir", dir_pos)):
            arg = self._positional_or_keyword(node, pos, (label,))
            if arg is None or (isinstance(arg, ast.Constant) and arg.value is None):
                continue
            lit = self._resolve_literal(arg)
            if lit is not None:
                if _is_abs_path(lit) and not self.policy.path_in_sandbox(lit):
                    self._add(RULE_FS_PATH, node, f"{shown} creates files under {label}= outside the sandbox prefix",
                              target=lit, remediation=_REM_TEMPFILE)
                continue
            head = self._literal_head(arg)
            if head and _is_local_path(head) and not self.policy.path_in_sandbox(_local_path_for_check(head)):
                self._add(RULE_FS_PATH, node, f"{shown} creates files under a {label}= prefix outside the sandbox",
                          target=head, remediation=_REM_TEMPFILE)

    def _check_reader_option(self, node: ast.Call, method: str, call_name: str) -> None:
        if method == "option":
            if len(node.args) >= 2 and _literal_str(node.args[0]) == "path":
                self._check_read_path(node, node.args[1], call_name + "('path')")
            return
        for k in node.keywords:
            if k.arg == "path":
                self._check_read_path(node, k.value, call_name + "(path=)")

    def _check_read_path(self, node: ast.Call, arg: ast.AST, call_name: str) -> None:
        """Literal local path outside the sandbox -> refused; object-storage /
        relative literal -> fine; unresolvable -> allowed, recorded."""
        lit = self._resolve_literal(arg)
        if lit is None:
            self._note(NOTE_UNVERIFIED_READ_PATH, node,
                       f"{call_name}() with a path that cannot be resolved statically; the read is allowed "
                       "and unverified (the cluster's IAM scope is the control)",
                       target=f"{call_name}(<dynamic>)")
            return
        if _is_local_path(lit) and not self.policy.path_in_sandbox(_local_path_for_check(lit)):
            self._add(RULE_OPEN_PATH, node, f"{call_name}() reads a local path outside the sandbox prefix",
                      target=lit, remediation=_REM_READ_PATH)

    def _check_io_target(self, node: ast.Call, arg: ast.AST, call_name: str) -> None:
        lit = self._resolve_literal(arg)
        if lit is not None:
            if _is_abs_path(lit) and not self.policy.path_in_sandbox(lit):
                self._add(RULE_OPEN_PATH, node, f"{call_name} on absolute path outside the sandbox prefix",
                          target=lit,
                          remediation=f"Read/write under the sandbox prefix {self.policy.prefix} "
                                      "or use a relative path.")
            return
        head = self._literal_head(arg)
        if head and _is_local_path(head) and not self.policy.path_in_sandbox(_local_path_for_check(head)):
            # open(f"/etc/{name}") / open("/Volumes/prod/" + x): the literal head fixes the directory
            self._add(RULE_OPEN_PATH, node, f"{call_name} on a path under a local prefix outside the sandbox",
                      target=head,
                      remediation=f"Read/write under the sandbox prefix {self.policy.prefix} "
                                  "or use a relative path.")
            return
        self._add(RULE_OPEN_DYNAMIC, node, f"{call_name} with a path that cannot be resolved statically",
                  target="<dynamic>",
                  remediation="Use a literal sandbox path (or a name bound once to one), or list "
                              "NBP-OPEN-DYNAMIC in AIDP_NOTEBOOK_POLICY_ALLOW after review.")

    def _check_pathlib(self, node: ast.Call, attr: str) -> bool:
        """Returns True when the call was judged as a ``pathlib`` operation."""
        receiver = node.func.value if isinstance(node.func, ast.Attribute) else None
        if receiver is None:
            return False
        # Any receiver rooted at a Path (constructor, Path-returning step, Path.cwd() /
        # home(), a name bound to one) is a Path: a literal the identity steps keep is
        # judged, everything else is an unresolvable path and fails closed. Only a
        # receiver that is not a Path at all leaves .open / .rename / .replace to the
        # generic (str / DataFrame / zip member) handling.
        is_path, path_arg = self._path_receiver(receiver)
        resolved_receiver = path_arg is not None
        if attr in _PATHLIB_RESOLVED_ONLY_ATTRS and not is_path:
            return False  # str.replace / df.rename / zipfile.open ... not a path operation
        call_name = f"Path.{attr}()"
        if attr in _PATHLIB_IO_ATTRS or attr == "open":
            if resolved_receiver:
                if self._refuse_splat(node, call_name):
                    return True
                self._check_io_target(node, path_arg, call_name)
            else:
                self._add(RULE_OPEN_DYNAMIC, node, f"{call_name} on a path that cannot be resolved statically",
                          target="<dynamic>",
                          remediation="Build the Path from a literal sandbox path, or list NBP-OPEN-DYNAMIC "
                                      "in AIDP_NOTEBOOK_POLICY_ALLOW after review.")
            return True
        targets: List[Optional[ast.AST]] = [path_arg]
        if attr in ("rename", "replace", "symlink_to", "hardlink_to") and node.args:
            targets.append(node.args[0])
        for t in targets:
            self._check_fs_target(node, t, call_name)
        return True

    def _check_fs_call(self, node: ast.Call, call_name: str, two_arg: bool = False, dst_only: bool = False) -> None:
        args = list(node.args)
        for k in node.keywords:
            if k.arg in ("src", "dst", "path", "name", "src_dir_fd", "dst_dir_fd"):
                if k.arg in ("src", "path", "name"):
                    args.insert(0, k.value)
                elif k.arg == "dst":
                    args.append(k.value)
        if not args:
            return
        if two_arg:
            targets = args[1:2] if dst_only else args[:2]
        else:
            targets = args[:1]
        for t in targets:
            self._check_fs_target(node, t, call_name)

    def _check_fs_target(self, node: ast.Call, arg: Optional[ast.AST], call_name: str) -> None:
        lit = self._resolve_literal(arg) if arg is not None else None
        if lit is None:
            self._add(RULE_FS_DYNAMIC, node, f"{call_name} with a path that cannot be resolved statically",
                      target="<dynamic>",
                      remediation="Use a literal sandbox path, or list NBP-FS-DYNAMIC in "
                                  "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
        elif _is_abs_path(lit) and not self.policy.path_in_sandbox(lit):
            self._add(RULE_FS_PATH, node, f"{call_name} target outside the sandbox prefix", target=lit,
                      remediation=f"Only paths under {self.policy.prefix} may be created, removed or "
                                  "renamed during this run.")

    def _check_dbfs(self, node: ast.Call, call_name: str, method: str):
        # rm(path) / put(path, ...) / mkdirs(path) -> arg0 ; cp(src, dst) -> dst ; mv(src, dst) -> src AND dst
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
            lit = self._resolve_literal(t)
            if lit is None:
                self._add(RULE_DBFS_DYNAMIC, node, f"{call_name}() with a non-literal path target",
                          target="<dynamic>",
                          remediation="Use a literal sandbox path, or list NBP-DBFS-DYNAMIC in "
                                      "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
            elif not self.policy.path_in_sandbox(lit):
                self._add(RULE_DBFS_PATH, node, f"{call_name}() target outside the sandbox prefix",
                          target=lit,
                          remediation=f"Only paths under {self.policy.prefix} may be removed, "
                                      "moved, created or written during this run.")

    def _check_table_write(self, node: ast.Call, method: str):
        arg = _first_arg(node, ("name", "tableName", "table"))
        if arg is None:
            return
        lit = self._resolve_literal(arg)
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

    def _check_path_write(self, node: ast.Call, method: str, entry: str = "write"):
        arg = _first_arg(node, ("path",))
        if arg is None:
            return  # e.g. .save() / .start() with no path -> table-style; option("path") is checked separately
        self._check_path_write_target(node, arg, f".{entry}.{method}()")

    def _check_writer_option(self, node: ast.Call, method: str, entry: str = "write") -> None:
        """``.option("path" | "checkpointLocation", p)`` / ``.options(path=p, checkpointLocation=p)``:
        a location the writer creates files under, judged like the terminal's path."""
        if method == "option":
            key = _literal_str(node.args[0]) if len(node.args) >= 2 else None
            if key in _WRITER_PATH_OPTIONS:
                self._check_path_write_target(node, node.args[1], f".{entry}.option({key!r})")
            return
        for k in node.keywords:
            if k.arg in _WRITER_PATH_OPTIONS:
                self._check_path_write_target(node, k.value, f".{entry}.options({k.arg}=)")

    def _check_path_write_target(self, node: ast.Call, arg: ast.AST, call_name: str) -> None:
        lit = self._resolve_literal(arg)
        if lit is None:
            self._add(RULE_PATH_WRITE_DYNAMIC, node, f"{call_name} with a non-literal path",
                      target="<dynamic>",
                      remediation="Use a literal sandbox path, or list NBP-PATH-WRITE-DYNAMIC in "
                                  "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
        elif not self.policy.path_in_sandbox(lit):
            self._add(RULE_PATH_WRITE, node, f"{call_name} target outside the sandbox prefix",
                      target=lit,
                      remediation=f"Write under {self.policy.prefix}.")

    def _check_sql(self, node: ast.Call, call_name: str):
        # spark.sql(*a) / spark.sql(**kw): neither the statement nor the template arguments
        # can be seen; a statement with no visible positional / keyword text likewise.
        splat = any(isinstance(a, ast.Starred) for a in node.args) or any(k.arg is None for k in node.keywords)
        arg = _first_arg(node, ("sqlQuery", "query"))
        if arg is None or splat:
            self._add(RULE_SQL_DYNAMIC, node, f"{call_name}() with *args / **kwargs or no visible statement: the "
                      "statement and its template arguments cannot be classified statically",
                      target="<dynamic>", remediation=_REM_SQL_DYNAMIC)
            return
        resolved = self._resolve_value(arg)
        lit = _literal_str(resolved)
        # pyspark (3.4+) substitutes every {name} field of the statement with the keyword
        # arguments other than args= (str(value)) BEFORE Spark parses it, so they are
        # statement text: rendered here when every value is a literal the gate can see,
        # otherwise the template is a statement with run-time fragments.
        template_kw = [k for k in node.keywords if k.arg not in ("sqlQuery", "query", "args")]
        if lit is not None and template_kw:
            values = {k.arg: self._resolve_scalar(k.value) for k in template_kw}
            if all(v is not None for v in values.values()):
                rendered = render_sql_template(lit, values)
                if rendered is None:
                    self._add(RULE_SQL_DYNAMIC, node, f"{call_name}() template that cannot be rendered with its "
                              "keyword arguments", target="<dynamic>", remediation=_REM_SQL_DYNAMIC)
                    return
                lit = rendered
            else:
                fragments = sql_template_fragments(lit)
                self._judge_dynamic_sql(node, call_name, fragments[0] if fragments else None, fragments, [],
                                        ends_literal=bool(fragments and fragments[-1]))
                return
        if lit is None:
            head = _joined_literal_head(resolved) if resolved is not None else None
            template = _literal_fragments(resolved, include_format_args=False) if resolved is not None else []
            args = _literal_fragments(resolved)[len(template):] if resolved is not None else []
            self._judge_dynamic_sql(node, call_name, head, template, args,
                                    ends_literal=resolved is not None and _ends_with_literal(resolved))
            return
        verdict = classify_sql(lit, self.policy)
        if not verdict.allowed:
            self._add(verdict.rule, node, f"{call_name}() {verdict.detail}", target=verdict.target,
                      remediation=verdict.remediation)

    def _resolve_scalar(self, node: ast.AST) -> Optional[str]:
        """Text of a literal keyword value the way pyspark renders it: a string literal (or a
        name bound once to one, a literal concatenation) or a number / boolean constant."""
        lit = self._resolve_literal(node)
        if lit is not None:
            return lit
        value = self._resolve_value(node)
        if isinstance(value, ast.Constant) and isinstance(value.value, (int, float, bool)):
            return str(value.value)
        return None

    def _judge_dynamic_sql(self, node: ast.Call, call_name: str, head: Optional[str], template: Sequence[str],
                           args: Sequence[str], ends_literal: bool) -> None:
        """A ``spark.sql`` statement with run-time fragments (f-string, concatenation,
        ``%``, ``.format(..)``, a ``{name}`` template with non-literal keyword
        arguments). It cannot be classified here; it is allowed only when the
        runtime SQL guard is active for the run (``dbutils.notebook.run``
        re-classifies the final text before Spark executes it) AND its literal
        head verb is a read verb AND no literal fragment already carries a
        refused token."""
        if head is None or not head.strip():
            self._add(RULE_SQL_DYNAMIC, node, f"{call_name}() statement that cannot be resolved statically",
                      target="<dynamic>", remediation=_REM_SQL_DYNAMIC)
            return
        head_text = _MaskedSql(head).text
        verb = _sql_head_verb(head)
        fragment_hit = None
        for i, fragment in enumerate(list(template) + list(args)):
            fragment_hit = _sql_fragment_refused(fragment, self.policy, last=(ends_literal and i == len(template) - 1))
            if fragment_hit is not None:
                break
        if not self.ctx.runtime_sql_guard:
            self._add(RULE_SQL_DYNAMIC, node,
                      f"{call_name}() statement with run-time fragments; the runtime SQL guard is not active "
                      "for this scan, so it cannot be classified", target="<dynamic>",
                      remediation=_REM_SQL_DYNAMIC)
        elif fragment_hit is not None:
            self._add(fragment_hit.rule, node, f"{call_name}() {fragment_hit.detail}",
                      target=fragment_hit.target, remediation=fragment_hit.remediation)
        elif verb in _SQL_DYNAMIC_READ_VERBS or verb == "CREATE TEMP VIEW":
            self._note(NOTE_UNVERIFIED_SQL_READ, node,
                       f"{call_name}() read statement with run-time fragments; allowed, the runtime SQL guard "
                       "re-classifies the final statement before Spark executes it", target=head_text[:80])
        else:
            self._add(RULE_SQL_DYNAMIC, node,
                      f"{call_name}() statement whose literal head is not a read verb", target="<dynamic>",
                      remediation=_REM_SQL_DYNAMIC)

    # -- the Python twins of the SQL forms: table reads, USE, SET, CREATE TABLE .. LOCATION / USING ----
    def _check_table_read(self, node: ast.Call, shown: str) -> None:
        """``spark.table(name)`` / ``spark.read.table(name)``: a path-table literal
        (``text.`/etc/passwd```) is judged like the SQL form; a dynamic name is a
        read the gate cannot verify and is recorded."""
        arg = _first_arg(node, ("tableName",))
        if arg is None:
            return
        lit = self._resolve_literal(arg)
        if lit is None:
            self._note(NOTE_UNVERIFIED_READ_PATH, node,
                       f"{shown}() with a table name that cannot be resolved statically; the read is allowed "
                       "and unverified (the cluster's IAM scope is the control)", target=f"{shown}(<dynamic>)")
            return
        path = _sql_path_table_target(lit)
        if path is not None and _is_local_path(path) and not self.policy.path_in_sandbox(_local_path_for_check(path)):
            self._add(RULE_OPEN_PATH, node, f"{shown}() reads a path table on the cluster's local filesystem "
                      "outside the sandbox prefix", target=path, remediation=_REM_SQL_PATH_READ)

    def _check_catalog_use(self, node: ast.Call, shown: str, attr: str) -> None:
        """``spark.catalog.setCurrentCatalog`` / ``setCurrentDatabase``: the ``USE`` rule."""
        lit = self._resolve_literal(_first_arg(node, ("catalogName", "dbName")))
        if lit is None:
            self._add(RULE_SQL_DYNAMIC, node, f"{shown}() with a name that cannot be resolved statically",
                      target="<dynamic>", remediation=f"Pass the sandbox name literally ({self.policy.catalog} / "
                                                      f"{self.policy.schema}).")
            return
        if attr == "setCurrentCatalog":
            inside = [p.lower() for p in _split_table_name(lit)] == [self.policy.catalog]
        else:
            inside = self.policy.schema_in_sandbox(lit)
        if not inside:
            self._add(RULE_SQL_TARGET, node, f"{shown}() moves the session's current catalog / schema outside "
                      f"the sandbox {self.policy.catalog}.{self.policy.schema} (2-part table names would resolve "
                      "there)", target=lit,
                      remediation=f"Only {self.policy.catalog} / {self.policy.schema} may become current.")

    def _check_conf_set(self, node: ast.Call, shown: str) -> None:
        """``spark.conf.set`` / ``unset``, ``sqlContext.setConf``: the ``SET`` conf allowlist."""
        key = self._resolve_literal(_first_arg(node, ("key",)))
        if key is None:
            self._add(RULE_SQL_DYNAMIC, node, f"{shown}() with a conf key that cannot be resolved statically",
                      target="<dynamic>", remediation=_REM_SQL_SET)
        elif not _sql_conf_allowed(key):
            self._add(RULE_SQL_REFUSED, node, f"{shown}() of a conf outside the allowlist", target=key,
                      remediation=_REM_SQL_SET)

    def _check_provider(self, node: ast.Call, arg: Optional[ast.AST], shown: str) -> None:
        """``.format(<provider>)`` / ``.using(<provider>)`` on a Spark reader / writer, ``source=``
        of ``spark.catalog.createTable``: a network data source is refused while the sandbox
        allows no network access; a provider the gate cannot resolve is recorded."""
        if arg is None or self.policy.allow_network:
            return
        lit = self._resolve_literal(arg)
        if lit is None:
            self._note(NOTE_UNVERIFIED_FORMAT, node,
                       f"{shown}() with a data source provider that cannot be resolved statically; allowed and "
                       "unverified (the cluster's network egress is the control)", target=f"{shown}(<dynamic>)")
            return
        detail = _sql_provider_refused(lit, self.policy)
        if detail is not None:
            self._add(RULE_IMPORT_NETWORK, node, f"{shown}() {detail}", target=lit, remediation=_REM_NETWORK_SOURCE)

    def _check_catalog_create_table(self, node: ast.Call, shown: str) -> None:
        """``spark.catalog.createTable(name, path=, source=)`` / ``createExternalTable(name, path, source)``:
        the Python twin of ``CREATE TABLE <name> USING <source> LOCATION '<path>'``."""
        name = self._positional_or_keyword(node, 0, ("tableName",))
        if name is not None:
            lit = self._resolve_literal(name)
            if lit is None:
                self._add(RULE_TABLE_DYNAMIC, node, f"{shown}() with a non-literal table name", target="<dynamic>",
                          remediation="Use a literal sandbox table, or list NBP-TABLE-DYNAMIC in "
                                      "AIDP_NOTEBOOK_POLICY_ALLOW after review.")
            elif not self.policy.table_in_sandbox(lit):
                self._add(RULE_TABLE_TARGET, node, f"{shown}() target outside {self.policy.catalog}."
                          f"{self.policy.schema}", target=lit,
                          remediation=f"Create {self.policy.catalog}.{self.policy.schema}.<table> "
                                      "(use the 2- or 3-part name).")
        path = self._positional_or_keyword(node, 1, ("path",))
        if path is not None:
            self._check_path_write_target(node, path, f"{shown}(path=)")
        self._check_provider(node, self._positional_or_keyword(node, 2, ("source",)), shown)

    def _check_call_function(self, node: ast.Call, shown: str) -> None:
        """``functions.call_function(name, ..)``: the Java-call functions by name, or a name the gate cannot see."""
        arg = _first_arg(node, ("funcName",))
        lit = self._resolve_literal(arg) if arg is not None else None
        verdict = classify_sql_function(lit, self.policy)
        if not verdict.allowed:
            self._add(verdict.rule, node, f"{shown}() {verdict.detail}", target=verdict.target,
                      remediation=verdict.remediation)


def scan_source(source: str, *, filename: str = "<cell>", policy: Optional[SandboxPolicy] = None,
                context: Optional[ScanContext] = None) -> List[Violation]:
    """Parse ``source`` and return every policy violation (empty when clean).

    ``context`` carries import/shim aliases and single-assignment string
    constants from earlier cells of the same run. ``SyntaxError``
    propagates: a cell that cannot be parsed is not executed, except that a
    cell broken by an IPython ``%magic`` / ``!command`` line is reported as
    a refusal (``NBP-OS-PROCESS`` / ``NBP-IPYTHON-ESCAPE``) so the attempt
    shows up in the policy log.
    """
    violations, _notes = _scan(source, filename=filename, policy=policy or require_policy(), context=context)
    return violations


def _scan(source: str, *, filename: str, policy: SandboxPolicy,
          context: Optional[ScanContext]) -> Tuple[List[Violation], List[Violation]]:
    """``(violations, notes)`` for one cell; see :func:`scan_source`."""
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        magic = _magic_violation(source)
        if magic is None:
            raise
        return [magic], []
    scanner = _PolicyScanner(policy, context)
    scanner.prepare(tree)
    scanner.visit(tree)
    scanner.violations.sort(key=lambda v: (v.lineno, v.rule))
    scanner.notes.sort(key=lambda v: (v.lineno, v.rule))
    return scanner.violations, scanner.notes


def enforce_cell_policy(source: str, *, notebook_path: str, cell_index: int,
                        policy: Optional[SandboxPolicy] = None,
                        context: Optional[ScanContext] = None) -> List[Violation]:
    """Gate one code cell. Returns the allowed exceptions; raises on refusal.

    Allowed-but-unverified operations (a library reader with a dynamic path,
    a ``spark.sql`` read statement with run-time fragments while
    ``context.runtime_sql_guard`` is set) are recorded in the policy log under
    the :data:`POLICY_NOTES` events before the verdict.

    Raises:
        SandboxUndeclaredError: no policy declared.
        PolicyViolation: the first refused violation (all are logged first).
    """
    pol = policy or require_policy()
    violations, notes = _scan(source, filename=str(notebook_path), policy=pol, context=context)
    for n in notes:
        _record(n.rule, rule="", target=n.target, notebook_path=str(notebook_path), cell_index=cell_index,
                detail={"line": n.lineno, "detail": n.detail})
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
    """Refuse ``operation`` unless ``path`` is under the sandbox prefix. Returns ``path``.

    Paths with a ``..`` segment are refused outright; URIs and local paths are
    canonicalised before the comparison (see :meth:`SandboxPolicy.path_in_sandbox`).
    """
    pol = policy or require_policy()
    if not pol.path_in_sandbox(path):
        why = " (parent-directory segment)" if isinstance(path, str) and _has_parent_segment(path) else ""
        remediation = f"Only paths under {pol.prefix} may be written by {operation} during this run."
        _record("runtime_refused", rule=RULE_RUNTIME_PATH, target=str(path), remediation=remediation,
                detail={"operation": operation})
        raise PolicyViolation(
            f"{operation}: target outside the sandbox prefix{why}: {path}. Remediation: {remediation}",
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


def _raise_runtime_verdict(verdict: SqlVerdict, *, operation: str, notebook_path: str, cell_index: Optional[int],
                           **detail: Any) -> None:
    """Record a refused runtime verdict (``runtime_refused``) and raise it before Spark sees the text."""
    _record("runtime_refused", rule=verdict.rule, target=verdict.target, notebook_path=notebook_path,
            cell_index=cell_index, remediation=verdict.remediation,
            detail=dict({"operation": operation, "detail": verdict.detail}, **detail))
    raise PolicyViolation(
        f"{operation}: [{verdict.rule}] {verdict.detail} (target: {verdict.target or '-'}). "
        f"Remediation: {verdict.remediation}",
        notebook_path=notebook_path, cell_index=cell_index, rule_id=verdict.rule, target=verdict.target,
        remediation=verdict.remediation,
    )


def assert_sql_allowed(statement: str, *, operation: str, policy: Optional[SandboxPolicy] = None,
                       notebook_path: str = "", cell_index: Optional[int] = None,
                       template_kwargs: Optional[Mapping[str, Any]] = None) -> SqlVerdict:
    """Runtime SQL guard: classify the FINAL statement text on its way to Spark.

    Installed by ``dbutils.notebook.run`` on the session's ``sql`` entry
    points for the duration of the run. ``template_kwargs`` are the keyword
    arguments of the ``spark.sql`` call other than ``args``: pyspark (3.4+)
    renders them into the statement's ``{name}`` fields before Spark parses
    it, so the statement is classified AFTER the same substitution (see
    :func:`render_sql_template`; a template that cannot be rendered is
    refused). A refused verdict is appended to the policy log
    (``runtime_refused``) and raised as :class:`PolicyViolation` before Spark
    sees the statement; an allowed verdict is returned.
    """
    pol = policy or require_policy()
    shown = statement.strip().split("\n", 1)[0][:80] if isinstance(statement, str) else "<non-string>"
    if template_kwargs and isinstance(statement, str):
        rendered = render_sql_template(statement, template_kwargs)
        if rendered is None:
            _raise_runtime_verdict(
                _sql_refused(RULE_SQL_DYNAMIC, "", "<dynamic>",
                             "spark.sql template that cannot be rendered with its keyword arguments",
                             _REM_SQL_DYNAMIC),
                operation=operation, notebook_path=notebook_path, cell_index=cell_index, statement=shown)
        statement = rendered
    verdict = classify_sql(statement, pol)
    if not verdict.allowed:
        _raise_runtime_verdict(verdict, operation=operation, notebook_path=notebook_path, cell_index=cell_index,
                               statement=shown)
    return verdict


def assert_sql_expression_allowed(expression: str, *, operation: str, policy: Optional[SandboxPolicy] = None,
                                  notebook_path: str = "", cell_index: Optional[int] = None) -> SqlVerdict:
    """Runtime guard for SQL expression strings (``DataFrame.selectExpr`` /
    ``filter`` / ``where``, ``functions.expr``): refuses ``reflect(`` /
    ``java_method(`` / the script transform, see :func:`classify_sql_expression`."""
    pol = policy or require_policy()
    verdict = classify_sql_expression(expression, pol)
    if not verdict.allowed:
        _raise_runtime_verdict(verdict, operation=operation, notebook_path=notebook_path, cell_index=cell_index,
                               expression=expression.strip()[:80])
    return verdict


def assert_table_read_allowed(name: Any, *, operation: str, policy: Optional[SandboxPolicy] = None,
                              notebook_path: str = "", cell_index: Optional[int] = None) -> Optional[str]:
    """Runtime guard for ``spark.table(name)`` / ``spark.read.table(name)``: a
    path-table name (``text.`/etc/passwd```, whatever the provider) on the
    cluster's local filesystem outside the prefix is refused like the SQL
    form. Returns the path when the name is a path table, else ``None``."""
    pol = policy or require_policy()
    if not isinstance(name, str):
        return None
    path = _sql_path_table_target(name)
    if path is not None and _is_local_path(path) and not pol.path_in_sandbox(_local_path_for_check(path)):
        _raise_runtime_verdict(
            _sql_refused(RULE_OPEN_PATH, "<table>", path,
                         "path table on the cluster's local filesystem outside the sandbox prefix", _REM_SQL_PATH_READ),
            operation=operation, notebook_path=notebook_path, cell_index=cell_index, table=name[:80])
    return path


def assert_sql_function_allowed(name: Any, *, operation: str, policy: Optional[SandboxPolicy] = None,
                                notebook_path: str = "", cell_index: Optional[int] = None) -> SqlVerdict:
    """Runtime guard for the DataFrame-API Java calls (``functions.reflect`` /
    ``java_method`` / ``try_reflect``, ``call_function(name, ..)``), see
    :func:`classify_sql_function`."""
    pol = policy or require_policy()
    verdict = classify_sql_function(name, pol)
    if not verdict.allowed:
        _raise_runtime_verdict(verdict, operation=operation, notebook_path=notebook_path, cell_index=cell_index,
                               function=str(name)[:80])
    return verdict


def record_secret_refusal(target: str, *, operation: str, remediation: str) -> None:
    """Log a ``dbutils.secrets`` allowlist refusal (raised by ``aidp_compat.secrets``)."""
    _record("runtime_refused", rule=RULE_RUNTIME_SECRET, target=target, remediation=remediation,
            detail={"operation": operation})


def record_skipped_cell(source: str, *, notebook_path: str, cell_index: int) -> None:
    """Log a magic-led cell (``%sql ...`` / ``!cmd`` on its first line).

    ``dbutils.notebook.run`` skips such a cell as a whole: no line of it is
    compiled or executed. The skip is recorded so the migration report shows
    which cells never ran.
    """
    head = source.strip().split("\n", 1)[0][:80]
    _record("skipped_magic", rule="", target=head, notebook_path=str(notebook_path), cell_index=cell_index,
            remediation="Magic-led cells are not executed by dbutils.notebook.run; express the step in plain "
                        "Python or dbutils.")


__all__ = [
    "SandboxPolicy", "NotebookPolicyError", "SandboxUndeclaredError", "PolicyViolation", "Violation",
    "ScanContext",
    "set_sandbox_policy", "current_policy", "require_policy",
    "scan_source", "enforce_cell_policy",
    "SqlVerdict", "classify_sql", "classify_sql_expression", "classify_sql_function", "render_sql_template",
    "sql_template_fragments", "assert_sql_allowed", "assert_sql_expression_allowed", "assert_table_read_allowed",
    "assert_sql_function_allowed", "SPARK_ROOT_NAMES",
    "assert_path_in_sandbox", "assert_table_in_sandbox", "record_secret_refusal", "record_skipped_cell",
    "get_policy_log", "clear_policy_log", "policy_log_json", "policy_log_markdown",
    "ALL_RULES", "POLICY_NOTES",
]

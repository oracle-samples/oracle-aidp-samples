"""Deterministic DBFS/FileStore Spark-read -> oci:// redirect for large single files.

WHY
---
On AIDP, `/Volumes/{catalog}/{schema}/dbfs` is a FUSE mount. Spark reads a *large
single file* there as ONE non-splittable partition, which grinds and/or hits the
JVM 2 GB single-partition cap. Observed in top5_v5_inference `rule_engine`: a
2.92 GB scored CSV written by an upstream task read from `/Volumes` as `numTasks=1`
(non-splittable) and failed. The SAME bytes read through the native `oci://`
object-storage connector split into many partitions and read fine (verified live:
48 tasks, 2,467,716 rows).

WHAT
----
Redirect **only large single DBFS Spark reads** to the `oci://` volume backing.
Small files, directories (parquet/hudi/delta part-dirs, which Spark already splits),
non-DBFS paths, and everything that is not a Spark *read* are left exactly as today
(they keep `/Volumes` / their original path). Writes, `pandas.to_csv`, `open()` are
never touched.

GENERIC / NO HARDCODE
---------------------
The `oci://` backing is resolved per-instance at runtime via
`DESCRIBE VOLUME {catalog}.{schema}.dbfs` (the `storageLocation` field) -> each AIDP
instance returns its own `oci://bucket@namespace/root`. `catalog`/`schema` come from
the existing `AIDP_DBFS_CATALOG` / `AIDP_DBFS_SCHEMA` env convention (default
`default`/`default`) already used for the `/Volumes/{catalog}/{schema}/dbfs`
translation. Nothing customer- or region-specific is baked in.

The runtime helper (`DBFS_READ_HELPER_SRC`) is injected into the migrated notebook
so the saved notebook is self-contained and portable; the wrap transform
(`wrap_spark_reads`) is applied by the migration tool. Both live here (a tool-side
utility module) rather than in the `aidp_compat` cluster library.
"""
from __future__ import annotations

import re
from typing import Optional

# 1 GiB default. Per the design decision, 2 GB is not a hard line: redirecting a file
# somewhat below the 2 GB single-partition cap to oci:// is harmless because the
# native oci:// read is a fully-supported Spark read at any size. A 1 GiB gate leaves
# comfortable margin below the cap while never touching ordinary small reads.
DEFAULT_THRESHOLD_BYTES = 1 << 30


# ── Pure path helpers (unit-testable, no cluster) ────────────────────────────
def dbfs_subpath(path: str, catalog: str = "default", schema: str = "default") -> Optional[str]:
    """If `path` is a DBFS path, return the sub-path below the dbfs root; else None.

    Handles every DBFS spelling the tool may encounter:
      dbfs:/x                                  -> x
      /dbfs/x                                  -> x
      /FileStore/x                             -> FileStore/x   (FileStore lives under dbfs)
      /Volumes/{catalog}/{schema}/dbfs/x       -> x             (already-translated form)
    Non-DBFS paths (oci://, s3://, plain local, /mnt, arbitrary) -> None.
    """
    if not isinstance(path, str):
        return None
    for pre in ("dbfs:/", "/dbfs/"):
        if path.startswith(pre):
            return path[len(pre):]
    if path.startswith("/FileStore/"):
        return "FileStore/" + path[len("/FileStore/"):]
    m = re.match(rf"^/Volumes/{re.escape(catalog)}/{re.escape(schema)}/dbfs/(.*)$", path)
    if m:
        return m.group(1)
    return None


def volumes_path(subpath: str, catalog: str = "default", schema: str = "default") -> str:
    """The /Volumes FUSE path for a dbfs sub-path."""
    return f"/Volumes/{catalog}/{schema}/dbfs/{subpath}"


def oci_path(subpath: str, backing: str) -> str:
    """The oci:// path for a dbfs sub-path given the resolved volume backing."""
    return backing.rstrip("/") + "/" + subpath


# ── Runtime helper injected into the migrated notebook ───────────────────────
# Self-contained + self-resolving (DESCRIBE VOLUME) so the saved notebook runs
# standalone on any AIDP instance. Mirrors the pure helpers above.
DBFS_READ_HELPER_SRC = '''
# Oracle tool modification: route LARGE single DBFS/FileStore Spark reads to the
# oci:// volume backing (native connector = splittable); small files, directories,
# and non-DBFS paths pass through unchanged (normal /Volumes behaviour). The
# /Volumes FUSE mount reads a large single file as ONE non-splittable partition and
# hits the 2 GB single-partition cap; oci:// splits it. Backing resolved per-instance
# via DESCRIBE VOLUME (generic; no hardcoded bucket/namespace).
import os as _os, re as _re
_AIDP_DBFS_OCI_BACKING = None
def _aidp_read_dbfs(p, _threshold=1073741824):
    global _AIDP_DBFS_OCI_BACKING
    if not isinstance(p, str) or p.startswith("oci://") or p.startswith("s3"):
        return p
    _cat = _os.environ.get("AIDP_DBFS_CATALOG", "default")
    _sch = _os.environ.get("AIDP_DBFS_SCHEMA", "default")
    _sub = None
    for _pre in ("dbfs:/", "/dbfs/"):
        if p.startswith(_pre):
            _sub = p[len(_pre):]; break
    if _sub is None and p.startswith("/FileStore/"):
        _sub = "FileStore/" + p[len("/FileStore/"):]
    if _sub is None:
        _m = _re.match(r"^/Volumes/" + _re.escape(_cat) + r"/" + _re.escape(_sch) + r"/dbfs/(.*)$", p)
        if _m:
            _sub = _m.group(1)
    if _sub is None:
        return p  # not a DBFS path -> unchanged
    _vol = "/Volumes/%s/%s/dbfs/%s" % (_cat, _sch, _sub)
    # size gate: only redirect a LARGE SINGLE FILE (dirs of parts already split fine)
    try:
        if not (_os.path.isfile(_vol) and _os.path.getsize(_vol) > _threshold):
            return _vol
    except Exception:
        return _vol
    if _AIDP_DBFS_OCI_BACKING is None:
        try:
            _rows = spark.sql("DESCRIBE VOLUME %s.%s.dbfs" % (_cat, _sch)).collect()
            _AIDP_DBFS_OCI_BACKING = next(
                (str(_r.asDict().get("storageLocation")) for _r in _rows
                 if str(_r.asDict().get("storageLocation", "")).startswith("oci://")), "")
        except Exception:
            _AIDP_DBFS_OCI_BACKING = ""
    if not _AIDP_DBFS_OCI_BACKING:
        return _vol  # couldn't resolve backing -> fall back to /Volumes
    return _AIDP_DBFS_OCI_BACKING.rstrip("/") + "/" + _sub
'''

HELPER_MARKER = "_aidp_read_dbfs"   # presence => helper already injected / wrapped


# ── Source transform: wrap Spark-read path args with the helper ──────────────
# Match a Spark read call opener: spark.read.csv( , spark.read.parquet( ,
# spark.read.json( , spark.read.load( , spark.read.orc( , spark.read.text( ,
# optionally preceded by any chain of .format(...)/.option(...)/.options(...)/
# .schema(...) modifiers (the dominant real-world idiom). `.read.` via a variable
# is intentionally NOT matched, to keep the transform conservative/unambiguous.
# The single-arg modifier bodies use `[^()]*` (no nested parens): a nested call
# like .option("k", f()) defeats the match and the read is left unwrapped — a
# SAFE miss (falls through to Opus) rather than a mis-wrap.
_SPARK_READ_OPEN_RE = re.compile(
    r'spark\.read'
    r'(?:\s*\.\s*(?:format|option|options|schema)\s*\([^()]*\))*'
    r'\s*\.\s*(?:csv|parquet|json|load|orc|text)\s*\(')

# Sentinels used only inside the mask (never emitted): string CONTENT -> 'X',
# comment CONTENT -> '\x01'. Both are 1:1 length-preserving so offsets into the
# mask map directly onto `source`.
_MASK_STR = "X"
_MASK_CMT = "\x01"


def _mask_strings_comments(source: str) -> str:
    """Return a same-length mask of `source` with string-literal content replaced
    by 'X' and `#`-comment content by '\\x01', so opener/paren scanning sees only
    real CODE structure. Delimiters/parens/commas in code are preserved verbatim;
    matching therefore never fires inside a string literal (fixes a read pattern
    that merely appears inside a string) and an inline `#` comment inside a read
    arg is detectable (so we can skip wrapping it rather than emit a SyntaxError).
    Handles ' " ''' and \"\"\" plus escaped quotes. Not language-aware beyond
    Python string/# rules — sufficient because the wrap only runs on Python cells.
    """
    n = len(source)
    res = list(source)
    i = 0
    instr = None                          # active string delimiter (', ", ''' , """)
    while i < n:
        ch = source[i]
        if instr:
            if source.startswith(instr, i) and (len(instr) == 3 or source[i - 1] != "\\"):
                i += len(instr)           # closing delimiter — leave verbatim
                instr = None
                continue
            if ch != "\n":
                res[i] = _MASK_STR
            i += 1
            continue
        if ch == "#":                     # comment to end of line
            while i < n and source[i] != "\n":
                res[i] = _MASK_CMT
                i += 1
            continue
        for q in ('"""', "'''"):
            if source.startswith(q, i):
                instr = q                 # opening triple — leave verbatim
                i += len(q)
                break
        else:
            if ch in ("'", '"'):
                instr = ch                # opening single/double — leave verbatim
                i += 1
                continue
            i += 1
    return "".join(res)


def wrap_spark_reads(source: str) -> str:
    """Wrap the FIRST argument of each Spark read call with `_aidp_read_dbfs(...)`.

    Fail-SAFE + idempotent. Openers are located in a string/comment MASK, so a
    read pattern appearing inside a string literal is never touched, and paren/
    comma counting for the first-arg walk ignores parens/commas inside nested
    strings. If the first argument contains an inline `#` comment (which would
    make the wrap emit invalid Python) or is already wrapped, the read is left
    UNCHANGED (falls through to Opus). Only the first (path) argument is wrapped;
    header=/schema=/etc. kwargs are untouched. Non-read code is not matched. Safe
    on any source — the helper is a pass-through for non-DBFS / small / dir paths.
    """
    if not source or "spark.read" not in source:
        return source
    mask = _mask_strings_comments(source)
    out = []
    i = 0
    n = len(source)
    for m in _SPARK_READ_OPEN_RE.finditer(mask):   # openers in CODE only
        open_end = m.end()                          # index just after the '('
        if m.start() < i:                           # overlaps an already-emitted wrap
            continue
        # walk the first argument on the MASK (nested-string parens/commas are 'X')
        k = open_end
        depth = 0
        while k < n:
            ch = mask[k]
            if ch == "(":
                depth += 1
            elif ch == ")":
                if depth == 0:
                    break                           # end of a single-arg call
                depth -= 1
            elif ch == "," and depth == 0:
                break                               # end of first argument
            k += 1
        arg = source[open_end:k]
        arg_mask = mask[open_end:k]
        stripped = arg.strip()
        out.append(source[i:open_end])              # everything up to & incl. the '('
        if (not stripped
                or stripped.startswith(HELPER_MARKER + "(")   # already wrapped
                or _MASK_CMT in arg_mask):                    # inline comment -> unsafe to wrap
            out.append(arg)                         # leave unchanged (fail-safe)
        else:
            lead = arg[:len(arg) - len(arg.lstrip())]
            trail = arg[len(arg.rstrip()):]
            out.append(f"{lead}{HELPER_MARKER}({stripped}){trail}")
        i = k
    out.append(source[i:])
    return "".join(out)

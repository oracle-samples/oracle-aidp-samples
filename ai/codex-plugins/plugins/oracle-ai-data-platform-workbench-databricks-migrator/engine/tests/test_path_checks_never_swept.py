"""REF-158: Python path checks are migrated by the model, never by the literal sweep.

The literal sweep (`_apply_s3_translations` + `_apply_dbfs_translations`)
rewrites every S3/DBFS string, including the strings a check uses to
RECOGNIZE an input form. On Python it broke path checks in customer and model
code alike (Example_Monitoring_Generic_Job, job 500000000000008: Case A, the
location SaveTableHelpers reads itself; Case B, Data_Aggregation's path helper).
Python cells are therefore never swept (`_sweep_storage_literals`): the model
migrates them, and what it returns is what runs and what is saved.

This is the regression corpus of those shapes and of every shape PR #243's
reviews raised. A case runs the Databricks original and the literal sweep on
the same inputs; the expected result is the original's result migrated by the
sweep (for an input already in AIDP form, the sweep's own result). The cell
loop itself is driven in test_path_recognition_migration.
"""
import ast
import itertools
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import job_migrate as jm


NAMESPACE = "test_namespace"
VOLUME = "/Volumes/default/default/dbfs/"
GOLD_OCI = f"oci://oci-example-gold@{NAMESPACE}/"


@pytest.fixture(autouse=True)
def _mapping(monkeypatch):
    monkeypatch.setattr(jm, "_ns_for_oci_bucket", lambda bucket: NAMESPACE)
    for key in ("AIDP_DBFS_CATALOG", "AIDP_DBFS_SCHEMA", "AIDP_DBFS_PREFIX"):
        monkeypatch.delenv(key, raising=False)


def sweep(source):
    """The literal sweep as it ran on Python cells before REF-158."""
    return jm._apply_dbfs_translations(jm._apply_s3_translations(source))


class _Rows(list):
    def collect(self):
        return self


class _Spark:
    """The kernel's `spark`: DESCRIBE and a table's files answer with the
    location under test."""

    def __init__(self, location):
        self.location = location

    def sql(self, query):
        # DESCRIBE DETAIL's `location`, or DESCRIBE EXTENDED's Location row.
        return _Rows([{"location": self.location, "col_name": "Location",
                       "data_type": self.location}])

    def table(self, name):
        return type("Frame", (), {"inputFiles": lambda frame: [self.location]})()


# A table's location as Model_Monitoring's SaveTableHelpers reads it: the
# function builds the value itself, so no caller holds it.
LOCATION = 'spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]["location"]'
# A path helper's call, on the input under test.
CALL = "RESULT = {}(PATH_INPUT)\n"

# REF-158 Case A as reported: the location is normalised, checked against
# the legacy aliases, and returned.
CHECK_FN = f'''def check(table):
    table_path = {LOCATION}
    table_path = table_path.replace("s3a://", "s3://").replace("s3n://", "s3://")
    if not table_path.startswith(GOLD_BUCKET_LEGACY_PREFIXES):
        raise ValueError("wrong bucket")
    return table_path
'''
# The same check on a parameter: the caller keeps its unconverted value.
CHECK_ARGUMENT = '''def check(table_path):
    table_path = table_path.replace("s3a://", "s3://").replace("s3n://", "s3://")
    if not table_path.startswith(GOLD_BUCKET_LEGACY_PREFIXES):
        raise ValueError("wrong bucket")
    return table_path
'''
GOLD_CONST = ('GOLD_BUCKET_LEGACY_PREFIXES = ("s3://example-gold/", '
              '"s3a://example-gold/", "s3n://example-gold/")\n')
DBFS_LOOP = f'''def to_local(path):
    for prefix in ("dbfs:/", "/dbfs/", "{VOLUME}"):
        if path.startswith(prefix):
            return "{VOLUME}" + path[len(prefix):]
    return path
'''
# REF-158 Case B as Model_Monitoring's Data_Aggregation writes it: each check
# returns at once.
PANDAS_PATH = '''def to_pandas_path(path):
    if '/dbfs/FileStore/' in path:
        return path
    if 'dbfs:' in path:
        return path.replace('dbfs:', '/dbfs')
    if path.startswith('/FileStore'):
        return '/dbfs' + path
'''
# A model translation of it: every DBFS form reaches the Volume.
PANDAS_PATH_TRANSLATED = f'''def to_pandas_path(path):
    # Oracle tool modification: DBFS paths live under the AIDP Volume
    for prefix in ("dbfs:/", "/dbfs/"):
        if path.startswith(prefix):
            return "{VOLUME}" + path[len(prefix):]
    if path.startswith("/FileStore/"):
        return "{VOLUME}" + path[1:]
    return path
'''


# REF-158 Case A as Model_Monitoring's current SaveTableHelpers writes it: the
# check reads a normalised copy of the location, the write uses the location.
def _save_table(prefix_line, translation=""):
    return ('GOLD_BUCKET = "example-gold"\n'
            'GOLD_OCI_PREFIX = "oci://oci-example-gold@test_namespace/"\n'
            "def save(table):\n"
            f"    table_path = {LOCATION}\n"
            '    normalised_path = table_path.replace("s3a://", "s3://").replace("s3n://", "s3://")\n'
            f"    {prefix_line}\n" + translation +
            "    if not normalised_path.startswith(allowed_prefix):\n"
            '        raise ValueError("Write blocked")\n'
            + ("    table_path = normalised_path\n" if translation else "") +
            '    return "written", table_path\n')


SAVE_FSTRING = _save_table('allowed_prefix = f"s3://{GOLD_BUCKET}/"')
SAVE_LITERAL = _save_table('allowed_prefix = "s3://example-gold/"')
# The model's repair: translate the location, then validate the OCI form.
SAVE_TRANSLATED = _save_table(
    "allowed_prefix = GOLD_OCI_PREFIX",
    '    gold_s3_prefix = f"s3://{GOLD_BUCKET}/"\n'
    "    if normalised_path.startswith(gold_s3_prefix):\n"
    "        normalised_path = GOLD_OCI_PREFIX + normalised_path[len(gold_s3_prefix):]\n")


def _located(body, setup=""):
    """`def f(table)`: reads its location itself into `path`, then `body`."""
    return setup + f"def f(table):\n    path = {LOCATION}\n" + body


# The shapes of the PR #243 reviews: each checks a value something else still
# holds (a caller, a closure, an alias), so a tool that rewrote the check
# admitted an unconverted write or changed a result the sweep gets right.
# (source, function, runtime input)
REVIEW_SHAPES = {
    "boolean validator": ('''def allowed(path):
    return path.startswith("s3://example-gold/")

def save(path):
    if not allowed(path):
        raise ValueError("Write blocked")
    writer(path)
''', "save", "s3://example-gold/table/"),
    "reverse alias": ('''def save(path):
    original = path
    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    writer(original)
    return path
''', "save", "s3://example-gold/table/"),
    "ignored converted return": ('''def validate(path):
    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    return path

def save(path):
    validate(path)
    writer(path)
''', "save", "s3://example-gold/table/"),
    "boolean-expression return": ('''def validate(path):
    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    return path.startswith("oci://")

def save(path):
    validate(path)
    writer(path)
''', "save", "s3://example-gold/table/"),
    "closure defined before the check": ('''def bucket_name(path):
    def extract():
        return path.split("/")[2]

    if path.startswith("s3://example-gold/"):
        pass
    return extract()
''', "bucket_name", "s3://example-gold/"),
    # Third round (B1): the alias is held in a dict or an attribute.
    "dict alias": ('''def save(path):
    held = {}
    held["raw"] = path
    if path.startswith("s3://example-gold/"):
        writer(held["raw"])
''', "save", "s3://example-gold/tbl/part1"),
    "attribute alias": ('''class Held:
    pass

def save(path):
    ctx = Held()
    ctx.raw = path
    if path.startswith("s3://example-gold/"):
        writer(ctx.raw)
''', "save", "s3://example-gold/tbl/part1"),
}
# B1 as the third review wrote it: the function reads the location itself.
B1_LOCATED = f'''def append_gold(df, table):
    path = {LOCATION}
    held = {{}}
    held["raw"] = path
    if path.startswith("s3://example-gold/"):
        df.write.mode("append").save(held["raw"])
'''

# (title, cells, function, input kind). Kind "location": the input is the
# location the function reads itself; function "module": the cells' RESULT.
CASES = [
    ("Case A, same cell", [GOLD_CONST + CHECK_FN], "check", "location"),
    ("Case A, constant in a later cell", [CHECK_FN, GOLD_CONST], "check", "location"),
    ("Case A, constant in an earlier cell", [GOLD_CONST, "x = 1\n", CHECK_FN], "check", "location"),
    ("Case A, local prefix", [f'''TARGET = "oci://oci-example-gold@test_namespace/"
def check(table):
    path = {LOCATION}
    path = path.replace("s3a://", "s3://").replace("s3n://", "s3://")
    prefix = "s3://example-gold/"
    if not path.startswith(prefix):
        raise ValueError("wrong bucket")
    return TARGET + path[len(prefix):]
'''], "check", "location"),
    ("Case A, literal + len (model reply)", [f'''TARGET = "oci://oci-example-gold@test_namespace/"
def check(table):
    path = {LOCATION}
    path = path.replace("s3a://", "s3://").replace("s3n://", "s3://")
    if path.startswith("s3://example-gold/"):
        path = TARGET + path[len("s3://example-gold/"):]
    if not path.startswith(TARGET):
        raise ValueError("wrong bucket")
    return path
'''], "check", "location"),
    ("SaveTableHelpers: the model's translation", [SAVE_TRANSLATED], "save", "location"),
    # The model's rewrite of DESCRIBE DETAIL (Delta-only on AIDP).
    ("location: DESCRIBE EXTENDED rows (model reply)", ['''def f(table):
    rows = spark.sql(f"DESCRIBE EXTENDED {table}").collect()
    path = next(r["data_type"] for r in rows if str(r["col_name"]) == "Location")
    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    return path
'''], "f", "location"),
    # The unconverted original is only named in an error message and a log line.
    ("location: original only in messages", [_located('''    normalised = path.replace("s3a://", "s3://")
    if not normalised.startswith("s3://example-gold/"):
        raise ValueError(f"wrong bucket: {path}")
    print("validated", path)
    return normalised
''')], "f", "location"),
    ("star import before the constant", ["from string import *\n", GOLD_CONST + CHECK_FN],
     "check", "location"),
    ("Case B, DBFS loop", [DBFS_LOOP + CALL.format("to_local")], "module", "path"),
    ("Case B, prefixes in another cell", [f'DBFS_PREFIXES = ("dbfs:/", "/dbfs/", "{VOLUME}")\n',
        f'''def to_local(path):
    for prefix in DBFS_PREFIXES:
        if path.startswith(prefix):
            return "{VOLUME}" + path[len(prefix):]
    return path
''' + CALL.format("to_local")], "module", "path"),
    ("Case B, early return", [PANDAS_PATH + CALL.format("to_pandas_path")], "module", "path"),
    # The closure only runs after the branch a converted value leaves by.
    ("closure called only past the early return", ['''def f(path):
    bucket = lambda: path.split("/")[2]
    if "/dbfs/FileStore/" in path:
        return path
    return bucket()
''' + CALL.format("f")], "module", "path"),
    ("location: equality decides the branch", [_located('''    if path == "s3://example-gold/t":
        return path
    return "other"
''')], "f", "location"),
    ("location: membership gate", [_located('''    if path not in ALLOWED:
        raise ValueError("Write blocked")
    return path
''', 'ALLOWED = ("s3://example-gold/t", "s3a://example-gold/a/b.parquet")\n')], "f", "location"),
    ("location: substring", [_located('''    if "s3://example-gold/" in path:
        return path
    return None
''')], "f", "location"),
    ("location: removeprefix", [_located('''    return path.removeprefix("s3://example-gold/")
''')], "f", "location"),
    ("location: f-string over a constant", [_located('''    if path.startswith(f"{BASE}a/"):
        return path
    return "other"
''', 'BASE = "s3://example-gold/"\n')], "f", "location"),
    ("location: loop over concatenated constants", [_located('''    for prefix in GOLD + LEGACY:
        if path.startswith(prefix):
            return path
    return "other"
''', 'GOLD = ("s3://example-gold/",)\nLEGACY = ("s3a://example-gold/",)\n')], "f", "location"),
    ("location: prefix passed through tuple()", [_located('''    if path.startswith(tuple(GOLD)):
        return path
    return "other"
''', 'GOLD = ["s3://example-gold/", "s3a://example-gold/"]\n')], "f", "location"),
    ("location: annotated local prefix", [_located('''    prefix: str = "s3://example-gold/"
    if path.startswith(prefix):
        return path
    return "other"
''')], "f", "location"),
    ("location: while loop strip", [_located('''    while path.startswith("s3://example-gold/"):
        path = path[len("s3://example-gold/"):]
    return path
''')], "f", "location"),
    ("location: short-circuit guard", [_located('''    if table and path.startswith("s3://example-gold/"):
        return path
    return "skip"
''')], "f", "location"),
    ("location: implicitly concatenated prefix", [_located('''    if path.startswith("s3://" "example-gold/"):
        return path
    return "other"
''')], "f", "location"),
    # Values used again after the check, in the form the source wrote.
    ("location: later check on the raw scheme", [_located('''    if path.startswith("s3://example-gold/"):
        print("gold layer")
    if path.startswith("s3://") or path.startswith("s3a://"):
        return "s3"
    return "local"
''')], "f", "location"),
    ("location: later lookup keyed by the raw path", [_located('''    if not path.startswith("s3://example-gold/"):
        raise ValueError("wrong bucket")
    return REGISTRY.get(path, "unknown")
''', 'REGISTRY = {"s3://example-gold/t": "owner-a"}\n')], "f", "location"),
    ("argument: short-circuit guard", ['''def f(flag, v):
    if flag and v.startswith("s3://example-gold/"):
        return "gold"
    return "skip"
'''], "f", "flagged"),
    ("argument: removeprefix", ['''def f(path):
    return path.removeprefix("s3://example-gold/")
'''], "f", "path"),
    ("argument: not in", ['''ALLOWED = ("s3://example-gold/t", "s3://example-silver/t")
def f(path):
    return path not in ALLOWED
'''], "f", "path"),
    ("argument: while loop strip", ['''def strip(path):
    while path.startswith("s3://example-gold/"):
        path = path[len("s3://example-gold/"):]
    return path
'''], "strip", "path"),
    ("argument: elif chain + loop variable", ['''def route(paths):
    res = []
    for p in paths:
        if p.startswith("s3://example-gold/"):
            res.append("gold")
        elif p.startswith("s3://example-silver/"):
            res.append("silver")
        else:
            res.append("other")
    return res
'''], "route", "list"),
    ("argument: equality and membership", ['''ALLOWED = ("s3://example-gold/t", "s3a://example-gold/a/b.parquet")
def f(path):
    return (path == "s3://example-gold/", path in ALLOWED, path != "s3://example-silver/t")
'''], "f", "path"),
    ("argument: substring", ['''def f(path):
    return "s3://example-gold/" in path
'''], "f", "path"),
    ("module-level check", ['path = PATH_INPUT\nRESULT = path.startswith("s3://example-gold/")\n'],
     "module", "path"),
    ("argument: later dynamically built prefix", ['''def f(path, bucket="example-gold"):
    is_gold = path.startswith("s3://example-gold/")
    return is_gold, path.startswith("s3://" + bucket + "/")
'''], "f", "path"),
    ("argument: parameter shadowed only in a nested function", ['''def under(path, prefix="s3://"):
    def default_root():
        prefix = "s3://example-gold/"
        return prefix
    return path.startswith(prefix)
'''], "under", "path"),
    ("argument: f-string over a constant", ['''BASE = "s3://example-gold/"
def f(path):
    return path.startswith(f"{BASE}a/")
'''], "f", "path"),
    ("argument: early return before a raw parse", ['''def layer(path):
    if path.startswith("s3://example-gold/"):
        return "gold"
    return path.split("/")[2]
'''], "layer", "path"),
    ("argument: Case A on a parameter", [GOLD_CONST + CHECK_ARGUMENT], "check", "path"),
    *[(f"review: {title}", [source], function, "path")
      for title, (source, function, _) in REVIEW_SHAPES.items()],
]

# Shapes whose result the literal sweep alone gets wrong.
BROKEN_BY_SWEEP = [
    "Case A, same cell", "Case A, constant in a later cell", "Case A, constant in an earlier cell",
    "Case A, local prefix", "Case A, literal + len (model reply)",
    "SaveTableHelpers: the model's translation", "star import before the constant",
    "location: DESCRIBE EXTENDED rows (model reply)", "location: original only in messages",
    "Case B, DBFS loop", "Case B, prefixes in another cell", "Case B, early return",
    "closure called only past the early return",
    "location: equality decides the branch", "location: membership gate", "location: substring",
    "location: removeprefix", "location: f-string over a constant",
    "location: loop over concatenated constants", "location: prefix passed through tuple()",
    "location: annotated local prefix", "location: while loop strip",
    "location: short-circuit guard",
]

# More check shapes from the reviews of the removed check converter.
MORE_SHAPES = [
    ("constant reassigned in another cell", [GOLD_CONST + CHECK_FN,
        'GOLD_BUCKET_LEGACY_PREFIXES = GOLD_BUCKET_LEGACY_PREFIXES + ("s3://example-extra/",)\n']),
    ("comprehension", [GOLD_CONST + '''def a(paths):
    return [p for p in paths if p.startswith(GOLD_BUCKET_LEGACY_PREFIXES)]
''']),
    ("lambda", [GOLD_CONST + '''def b(paths):
    return list(filter(lambda q: q.startswith(GOLD_BUCKET_LEGACY_PREFIXES), paths))
''']),
    ("global value", [GOLD_CONST + '''path = dbutils.widgets.get("path")
def c():
    return path.startswith(GOLD_BUCKET_LEGACY_PREFIXES)
''']),
    ("attribute", [GOLD_CONST + '''def d(row):
    return row.path.startswith(GOLD_BUCKET_LEGACY_PREFIXES)
''']),
    ("name bound only in a nested function", [GOLD_CONST + '''def outer():
    def inner():
        path = "x"
    return path.startswith(GOLD_BUCKET_LEGACY_PREFIXES)
''']),
    ("helper builtin redefined", [GOLD_CONST + "len = 3\n" + CHECK_FN]),
    ("suffix check", ['''def g(path):
    return path.endswith("/dbfs/"), path.removesuffix("/dbfs/")
''']),
    ("exact match against a long string", [f'''TEMPLATE = "s3://example-gold/{"x" * 600}"
def h(text):
    return text == TEMPLATE
''']),
    ("unbound str method", [GOLD_CONST + '''def e(path):
    return str.startswith(path, GOLD_BUCKET_LEGACY_PREFIXES)
''']),
    ("regex match", ['''import re
def r(path):
    return re.match(r"^s3://example-gold/", path)
''']),
    ("glob match", ['''from fnmatch import fnmatch
def g(path):
    return fnmatch(path, "s3://example-gold/*")
''']),
    ("indexed constant", [GOLD_CONST + '''def i(path):
    return path.startswith(GOLD_BUCKET_LEGACY_PREFIXES[0])
''']),
    ("dict-held prefix", ['''PREFIXES = {"gold": "s3://example-gold/"}
def d(path):
    return path.startswith(PREFIXES["gold"])
''']),
    ("parameter default", ['''def p(path, prefix="s3://example-gold/"):
    return path.startswith(prefix)
''']),
    ("chained comparison", ['''def c(path, other):
    return path == other == "s3://example-gold/t"
''']),
    ("unparseable cell mentions the constant", [GOLD_CONST + CHECK_FN,
        "GOLD_BUCKET_LEGACY_PREFIXES = (\n"]),
    ("dynamic rebinding", [GOLD_CONST + CHECK_FN, 'globals()["X"] = 1\n']),
    ("star import after the constant", [GOLD_CONST + CHECK_FN, "from config import *\n"]),
    ("%run after the constant", [GOLD_CONST + CHECK_FN, "%run ./config\n"]),
    ("constant changed in place", ['ALLOWED = ["s3://example-gold/t"]\nALLOWED.append("s3://example-silver/t")\n'
                                   'def f(path):\n    return path in ALLOWED\n']),
    ("prefix assigned in both branches", ['''def f(path, legacy):
    if legacy:
        prefix = "s3a://example-gold/"
    else:
        prefix = "s3://example-gold/"
    return path.startswith(prefix)
''']),
    ("loop over dict items", ['''ROOTS = {"s3://example-gold/": "gold"}
def f(path):
    for prefix, name in ROOTS.items():
        if path.startswith(prefix):
            return name
''']),
    ("f-string prefix", ['''def f(path, bucket):
    return path.startswith(f"s3://{bucket}/")
''']),
    ("class attribute prefix", ['''class Paths:
    GOLD = "s3://example-gold/"
def f(path):
    return path.startswith(Paths.GOLD)
''']),
    ("check on a magic line", ['%time ok = PATH.startswith("s3://example-gold/")\n']),
    # Notebook-level values: later cells and notebooks that %run this one read them.
    ("module-level check", ['path = PATH_INPUT\nRESULT = path.startswith("s3://example-gold/")\n']),
    ("module-level rebinding", ['''path = PATH_INPUT
if path.startswith("s3://example-gold/"):
    path = "oci://oci-example-gold@test_namespace/" + path[len("s3://example-gold/"):]
''']),
    ("module-level check on a location a later cell parses", [
        f'table = "ds.t"\npath = {LOCATION}\nif path.startswith("s3://example-gold/"):\n'
        '    print("gold", path)\n', 'BUCKET = path.split("/")[2]\n']),
    # A parameter: the caller still holds the value.
    ("Case A on a parameter", [GOLD_CONST + CHECK_ARGUMENT]),
    ("argument validated but not returned", ['''def validate(table_path):
    if not table_path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
''']),
    ("loop variable checked, iterable used after", ['''def check_all(paths):
    for p in paths:
        if not p.startswith("s3://example-gold/"):
            raise ValueError("Write blocked")
    return paths
''']),
    *[(f"review: {title}", [source]) for title, (source, _, _) in REVIEW_SHAPES.items()],
    # A location the function reads itself, used again after the check.
    ("SaveTableHelpers: f-string prefix, copy checked", [SAVE_FSTRING]),
    ("SaveTableHelpers: literal prefix, copy checked", [SAVE_LITERAL]),
    ("location: yes/no result returned", [_located('''    return path.startswith("s3://example-gold/")
''')]),
    ("location: yes/no result stored", [_located('''    ok = path.startswith("s3://example-gold/")
    if ok:
        return path
    return None
''')]),
    ("location: label returned from the branch", [_located('''    if path.startswith("s3://example-gold/"):
        return "gold"
    return "other"
''')]),
    ("location: copy saved before the check", [_located('''    original = path
    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    return original
''')]),
    ("location: part saved before the check", [_located('''    bucket = path.split("/")[2]
    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    return bucket
''')]),
    ("location: read again from its source", [_located(f'''    if not path.startswith("s3://example-gold/"):
        raise ValueError("Write blocked")
    return {LOCATION}
''')]),
    ("location: parsed after the check", [_located('''    if not path.startswith("s3://example-gold/"):
        raise ValueError("wrong bucket")
    return path.split("/")[2]
''')]),
    ("location: read by a nested function", [_located('''    record = lambda: path.split("/")[2]
    if not path.startswith("s3://example-gold/"):
        raise ValueError("wrong bucket")
    return record(), path
''')]),
    ("location: from a notebook variable", ['''def f():
    path = PATH_INPUT
    if path.startswith("s3://example-gold/"):
        return path
    return None
''']),
    ("location: built from a parameter", ['''def f(base):
    path = base + "/t"
    if path.startswith("s3://example-gold/"):
        return path
    return None
''']),
    ("location: read from a parameter's method", ['''def f(cfg):
    path = cfg.get("path")
    if path.startswith("s3://example-gold/"):
        return path
    return None
''']),
    ("location: from an enclosing function", [f'''def outer(table):
    path = {LOCATION}
    def inner():
        p = path
        if p.startswith("s3://example-gold/"):
            return p
        return None
    return inner()
''']),
    # Path helpers and their calls.
    ("helper without a call in its cell", [PANDAS_PATH]),
    ("helper that can raise", ['''def h(path):
    if '/dbfs/FileStore/' in path:
        return path
    raise ValueError("not a FileStore path")
''' + CALL.format("h")]),
    ("helper returning yes/no", ['''def h(path):
    if '/dbfs/FileStore/' in path:
        return path
    return False
''' + CALL.format("h")]),
    ("helper reading its parameter first", ['''def h(path):
    print(path)
    if '/dbfs/FileStore/' in path:
        return path
    return None
''' + CALL.format("h")]),
    ("helper normalising its parameter first", ['''def h(path):
    path = path.strip()
    if '/dbfs/FileStore/' in path:
        return path
    return None
''' + CALL.format("h")]),
    ("helper with a decorator", ['''@staticmethod
def h(path):
    if '/dbfs/FileStore/' in path:
        return path
    return None
''' + CALL.format("h")]),
    ("helper called twice with one argument", [PANDAS_PATH + "A = to_pandas_path(PATH_INPUT)\n"
                                               "B = to_pandas_path(PATH_INPUT)\n"]),
    ("helper argument read again in a later cell", [
        PANDAS_PATH + 'CFG = {"k": PATH_INPUT}\nRESULT = to_pandas_path(CFG["k"])\n',
        'AGAIN = CFG["k"]\n']),
    ("helper argument's dict used whole", [
        PANDAS_PATH + 'CFG = {"k": PATH_INPUT}\nRESULT = to_pandas_path(CFG["k"])\n',
        'SIZE = len(CFG)\n']),
    ("helper argument read inside a function", [
        PANDAS_PATH + 'CFG = {"k": PATH_INPUT}\nRESULT = to_pandas_path(CFG["k"])\n',
        'def later():\n    return CFG["k"]\n']),
    ("helper redefined in another cell", [PANDAS_PATH + CALL.format("to_pandas_path"),
                                          "def to_pandas_path(path):\n    return path\n"]),
    ("helper call inside a comprehension", [
        PANDAS_PATH + "RESULT = [to_pandas_path(p) for p in [PATH_INPUT]]\n"]),
    ("helper call before its definition", [CALL.format("to_pandas_path") + PANDAS_PATH]),
    ("helper call from a function, on its parameter", [
        PANDAS_PATH + "def read(p):\n    return to_pandas_path(p)\n"]),
]


def _as_migrated(value):
    if isinstance(value, (list, tuple)):
        return type(value)(_as_migrated(v) for v in value)
    return ast.literal_eval(sweep(repr(value))) if isinstance(value, str) else value


def _load(cells, value=None):
    namespace = {"PATH_INPUT": value, "spark": _Spark(value)}
    for cell in cells:
        exec(cell, namespace)
    return namespace


def _call(cells, function, value, kind="path"):
    try:
        if function == "module":
            return ("ok", _load(cells, value)["RESULT"])
        if kind == "location":
            return ("ok", _load(cells, value)[function]("ds.t"))
        if isinstance(value, tuple):
            return ("ok", _load(cells)[function](*value))
        return ("ok", _load(cells)[function](value))
    except Exception as exc:  # the comparison includes the failure itself
        return ("err", type(exc).__name__, str(exc))


BUCKETS = ["example-gold", "example-silver", "other", "example-gold-lookalike", "example-gol"]
SUFFIXES = ["t", "a/b.parquet", "", "x/s3://y/"]
PATHS = [f"{s}://{b}/{x}" for s, b, x in itertools.product(["s3", "s3a", "s3n"], BUCKETS, SUFFIXES)]
PATHS += [f"{p}FileStore/{x}" for p, x in itertools.product(["dbfs:/", "/dbfs/", VOLUME], ["plan.md", ""])]
PATHS += ["/tmp/x", "", "relative/path", "S3://example-gold/t", None, 7,
          "s3://example-gold/s3://example-gold/x"]


def _inputs(kind):
    aidp = [_as_migrated(f"s3://{b}/{x}") for b, x in itertools.product(BUCKETS, SUFFIXES)]
    paths = PATHS + aidp
    if kind == "flagged":
        # flag=False with a non-string value: evaluating it would raise.
        return [(False, None)] + [(True, p) for p in paths if isinstance(p, str)]
    if kind == "list":
        return [[p] for p in paths if isinstance(p, str)] + [paths[:12]]
    return paths


def _is_aidp_form(value):
    values = value if isinstance(value, (list, tuple)) else [value]
    return any(isinstance(v, str) and v.startswith(("oci://", "/Volumes/")) for v in values)


def _right(cells, function, value, kind):
    """The original's result migrated by the sweep; for an input already in
    AIDP form (which the original never sees), the sweep's own result."""
    if _is_aidp_form(value):
        return _call([sweep(c) for c in cells], function, value, kind)
    original = _call(cells, function, value, kind)
    return ("ok", _as_migrated(original[1])) if original[0] == "ok" else original


CORPUS = [(title, cells) for title, cells, _, _ in CASES] + MORE_SHAPES


@pytest.mark.parametrize("title,cells", CORPUS, ids=[c[0] for c in CORPUS])
def test_python_cells_are_never_swept(title, cells):
    assert [jm._sweep_storage_literals(cell) for cell in cells] == cells


@pytest.mark.parametrize("title", BROKEN_BY_SWEEP)
def test_the_sweep_breaks_every_isu158_shape(title):
    # Why Python is never swept: on some input the swept cell returns
    # something other than the original's result in AIDP form.
    _, cells, function, kind = next(c for c in CASES if c[0] == title)
    swept = [sweep(cell) for cell in cells]
    assert any(_call(swept, function, value, kind) != _right(cells, function, value, kind)
               for value in _inputs(kind))


@pytest.mark.parametrize("source", [
    '%scala\nval p = "s3://example-gold/t"',
    "%sql\nSELECT * FROM parquet.`s3://example-gold/t`",
], ids=["scala", "sql"])
def test_other_languages_keep_the_sweep(source):
    assert jm._sweep_storage_literals(source) == sweep(source) != source


def test_the_original_save_table_blocks_the_aidp_location():
    # On AIDP the metastore returns the oci:// location, so the original
    # fails when it runs, which hands the real error to the fix loop.
    gold = GOLD_OCI + "tables/ds/t/"
    for source in (SAVE_FSTRING, SAVE_LITERAL):
        assert _call([source], "save", gold, "location") == ("err", "ValueError", "Write blocked")


def test_the_models_save_table_translation_writes_every_alias_to_oci():
    gold = GOLD_OCI + "tables/ds/t/"
    for scheme in ("s3", "s3a", "s3n"):
        assert _call([SAVE_TRANSLATED], "save", f"{scheme}://example-gold/tables/ds/t/",
                     "location") == ("ok", ("written", gold))
    assert _call([SAVE_TRANSLATED], "save", gold, "location") == ("ok", ("written", gold))
    for wrong in ("s3://other/tables/t/", "s3://example-gold-lookalike/t/"):
        assert _call([SAVE_TRANSLATED], "save", wrong, "location") == (
            "err", "ValueError", "Write blocked")


def test_the_models_pandas_path_translation_reaches_the_volume_from_every_dbfs_form():
    cells = [PANDAS_PATH_TRANSLATED + CALL.format("to_pandas_path")]
    for form in ("/dbfs/FileStore/", "dbfs:/FileStore/", "/FileStore/", VOLUME + "FileStore/"):
        assert _call(cells, "module", form + "models/f.csv") == (
            "ok", VOLUME + "FileStore/models/f.csv")

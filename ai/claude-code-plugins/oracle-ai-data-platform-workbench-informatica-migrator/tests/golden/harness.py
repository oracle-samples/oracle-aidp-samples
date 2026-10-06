"""Golden-output execution harness: run a generated notebook, compare rows.

The rest of the suite asserts the *text* the generator emits, and the live
runs on AIDP showed why that is not enough: six of twelve corpus notebooks
the offline suite called clean failed on a real cluster. This harness asks
the question that matters -- given these source rows, does the migrated
notebook write the rows Informatica would have written?

A case is a directory under ``tests/golden/cases/<name>/``:

``case.json``
    ``export`` (file name of the PowerCenter XML / IDMC JSON in the same
    directory), optional ``mapping`` (which mapping's notebook to run when
    the export holds several), ``params`` ({"NAME": "value"} set as
    ``spark.conf migration.<name>`` -- the job-parameter path), optional
    ``session_start`` (ISO timestamp pinned as SESSSTARTTIME), and
    ``notes`` -- the Informatica behaviour the case pins, in words.
``seed.json``
    ``{table: {"schema": "<Spark DDL>", "rows": [[...], ...]}}`` for every
    source, lookup table and pre-existing target. ``table`` matches the
    name the notebook reads on its LAST component, case-insensitively, so
    ``SALES_DB.SALES.ORDERS`` in the notebook is seeded by ``ORDERS``.
``expected.json``
    ``{target: {"rows": [{col: value}], "ignore": [col, ...]}}`` -- the
    rows Informatica writes, worked out by hand from its documented
    semantics. Rows compare order-insensitively over the columns named in
    the expected rows; ``ignore`` names columns whose value is
    non-deterministic (a SYSDATE audit stamp).

What runs for real: every generated cell, on a local Spark of the version
AIDP runs (3.5), with the ANSI pins the notebook sets. What is stood in:
the storage layer, because a laptop has no Delta and (on Windows) cannot
write a local warehouse. ``saveAsTable`` / ``insertInto`` land in temp
views, ``delta.tables.DeltaTable`` is an in-memory MERGE with Delta's
semantics (including its refusal of a target row matched by two source
rows), and the Sequence Generator's counter table is an in-memory dict.
The catalog-type assertion cell is skipped -- it asserts a property of
the deployment (is the target a managed Delta table), not of the logic.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import sys
import types
from decimal import Decimal
from pathlib import Path

_TABLE_REF = re.compile(
    r'spark\.table\("([^"]+)"\)|saveAsTable\("([^"]+)"\)|DeltaTable\.forName\(spark, "([^"]+)"\)'
    r'|target="([^"]+)"|tableExists\("([^"]+)"\)|insertInto\("([^"]+)"\)'
)


# Table names inside spark.sql("...") text -- a Lookup SQL override, a
# qualifier override -- are seeded and rewritten like the quoted refs above.
_SQL_CALL = re.compile(r'spark\.sql\((?:f?"""(.*?)"""|f?"([^"]*)")\)', re.DOTALL)
_SQL_TABLE = re.compile(r'(?i)\b(?:FROM|JOIN)\s+([A-Za-z_][\w$#]*(?:\.[A-Za-z_][\w$#]*)*)')


def _sql_refs(src: str) -> set:
    refs = set()
    for m in _SQL_CALL.finditer(src):
        sql = m.group(1) if m.group(1) is not None else m.group(2)
        refs.update(t.group(1) for t in _SQL_TABLE.finditer(sql))
    return refs


def _rewrite_sql(src: str, views: dict) -> str:
    def fix(m):
        text = m.group(0)
        for ref in sorted(views, key=len, reverse=True):
            text = re.sub(rf"(?<![\w.]){re.escape(ref)}(?![\w])", views[ref], text)
        return text
    return _SQL_CALL.sub(fix, src)


class GoldenRunError(AssertionError):
    """A generated cell raised: the notebook would fail on the cluster too."""


def _view_name(ref: str) -> str:
    return "g_" + re.sub(r"[^0-9a-zA-Z]+", "_", ref).strip("_").lower()


# ---------------------------------------------------------------------------
# Storage stand-ins
# ---------------------------------------------------------------------------

# Every temp view the harness has created. Existence is answered from here,
# never from spark.catalog: the catalog creates a warehouse directory on first
# use, which on Windows needs Hadoop's winutils. Generated notebooks call
# spark.catalog.tableExists themselves, so _Stands patches it to read this.
_VIEWS: set = set()


def _exists(name: str) -> bool:
    return name in _VIEWS


def _materialize(spark, df, name: str) -> None:
    """Snapshot ``df`` into temp view ``name`` (collect + recreate, so a
    view never refers to its own previous plan)."""
    spark.createDataFrame(df.collect(), df.schema).createOrReplaceTempView(name)
    _VIEWS.add(name)


class _Merge:
    def __init__(self, table: "FakeDeltaTable", source, condition: str):
        self.table, self.source, self.condition = table, source, condition
        self.matched: list = []
        self.not_matched: list = []

    def whenMatchedUpdateAll(self, condition=None):
        self.matched.append(("update_all", condition, None))
        return self

    def whenMatchedUpdate(self, condition=None, set=None):  # noqa: A002 (Delta's own keyword)
        self.matched.append(("update", condition, set or {}))
        return self

    def whenMatchedDelete(self, condition=None):
        self.matched.append(("delete", condition, None))
        return self

    def whenNotMatchedInsertAll(self, condition=None):
        self.not_matched.append(("insert_all", condition, None))
        return self

    def whenNotMatchedInsert(self, condition=None, values=None):
        self.not_matched.append(("insert", condition, values or {}))
        return self

    def execute(self):
        self.table._apply(self)


class FakeDeltaTable:
    """``delta.tables.DeltaTable`` over a temp view, MERGE semantics only."""

    def __init__(self, spark, name: str):
        self._spark, self._name, self._alias = spark, name, "t"

    @classmethod
    def forName(cls, spark, name):  # noqa: N802 (Delta's API name)
        if not _exists(name):
            raise RuntimeError(f"[DELTA_MISSING_DELTA_TABLE] `{name}` is not a Delta table.")
        return cls(spark, name)

    def alias(self, a):
        self._alias = a
        return self

    def toDF(self):  # noqa: N802
        return self._spark.table(self._name)

    def merge(self, source, condition):
        return _Merge(self, source, condition)

    def _apply(self, m: _Merge) -> None:
        from pyspark.sql import functions as F

        spark, ta = self._spark, self._alias
        target = spark.table(self._name)
        cols = target.columns
        t = target.withColumn("__rid", F.monotonically_increasing_id()).alias(ta)
        s = m.source
        # Delta refuses a source column typed differently from the target
        # column (a BIGINT sequence value into a DECIMAL(10,0) key failed on
        # AIDP with DELTA_FAILED_TO_MERGE_FIELDS); so does this stand-in.
        from pyspark.sql.types import NullType
        ttypes = {f.name.lower(): f.dataType for f in target.schema.fields}
        for f in s.schema.fields:
            want = ttypes.get(f.name.lower())
            if want is not None and not isinstance(f.dataType, NullType) and f.dataType != want:
                raise RuntimeError(
                    f"[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields {f.name!r}: "
                    f"source {f.dataType.simpleString()} vs target {want.simpleString()}"
                )
        joined = t.join(s, F.expr(m.condition), "inner")

        dup = joined.groupBy(f"{ta}.__rid").count().filter("count > 1").limit(1).collect()
        if dup:
            raise RuntimeError(
                "[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] a target row matched "
                "more than one source row -- the merge is ambiguous and Delta refuses it."
            )

        pieces = []
        handled = None
        for kind, cond, spec in m.matched:
            rows = joined if cond is None else joined.filter(F.expr(cond))
            if handled is not None:
                # Delta applies the FIRST matching WHEN MATCHED clause per row.
                rows = rows.filter(~F.col(f"{ta}.__rid").isin([r[0] for r in handled.collect()]))
            if kind == "update_all":
                missing = [c for c in cols if c not in s.columns]
                if missing:
                    raise RuntimeError(f"UPDATE SET * needs source columns {missing}")
                pieces.append(rows.select([F.col(f"s.{c}").alias(c) for c in cols]))
            elif kind == "update":
                pieces.append(rows.select([
                    (F.expr(spec[c]) if c in spec else F.col(f"{ta}.{c}")).alias(c) for c in cols]))
            ids = rows.select(F.col(f"{ta}.__rid").alias("__rid"))
            handled = ids if handled is None else handled.union(ids)
        matched_ids = joined.select(F.col(f"{ta}.__rid").alias("__rid")).distinct()
        if m.matched and handled is not None:
            untouched_matched = t.join(matched_ids, "__rid", "left_semi").join(handled, "__rid", "left_anti")
            pieces.append(untouched_matched.select(cols))
        elif not m.matched:
            pieces.append(t.join(matched_ids, "__rid", "left_semi").select(cols))
        pieces.append(t.join(matched_ids, "__rid", "left_anti").select(cols))

        unmatched_src = s.join(t, F.expr(m.condition), "left_anti")
        for kind, cond, spec in m.not_matched:
            rows = unmatched_src if cond is None else unmatched_src.filter(F.expr(cond))
            if kind == "insert_all":
                missing = [c for c in cols if c not in s.columns]
                if missing:
                    raise RuntimeError(f"INSERT * needs source columns {missing}")
                pieces.append(rows.select([F.col(c) for c in cols]))
            else:
                pieces.append(rows.select([
                    (F.expr(spec[c]) if c in spec else F.lit(None).cast(target.schema[c].dataType)).alias(c)
                    for c in cols]))

        result = pieces[0]
        for p in pieces[1:]:
            result = result.unionByName(p)
        _materialize(spark, result.select([F.col(c).cast(target.schema[c].dataType) for c in cols]), self._name)


class _MemorySequenceBackend:
    def __init__(self, start=1):
        self._start, self._current = start, {}

    def reserve_block(self, name, count, increment):
        cur = self._current.get(name)
        first = self._start if cur is None else cur + increment
        self._current[name] = first + (count - 1) * increment
        return first


class _Stands:
    """Install and remove every stand-in in one place."""

    def __init__(self, spark):
        self.spark = spark
        self._undo: list = []

    def __enter__(self):
        from pyspark.sql.readwriter import DataFrameWriter
        import importlib
        # infa_compat re-exports a *function* named sequence, which shadows the
        # submodule attribute -- fetch the module itself.
        seq_mod = importlib.import_module("infa_compat.sequence")

        spark = self.spark

        orig_mode = DataFrameWriter.mode

        def mode(self_w, saveMode):  # noqa: N803
            self_w._golden_mode = (saveMode or "errorifexists").lower()
            return orig_mode(self_w, saveMode)

        def save_as_table(self_w, name, format=None, mode=None, partitionBy=None, **_):  # noqa: A002
            m = (mode or getattr(self_w, "_golden_mode", "errorifexists")).lower()
            df = self_w._df
            exists = _exists(name)
            if exists and m in ("error", "errorifexists", "default"):
                raise RuntimeError(f"[TABLE_OR_VIEW_ALREADY_EXISTS] {name}")
            if exists and m == "ignore":
                return
            if exists and m == "append":
                # Delta refuses an append whose column types differ (without
                # mergeSchema), and so does this stand-in.
                from pyspark.sql.types import NullType
                have = {f.name.lower(): f.dataType for f in spark.table(name).schema.fields}
                for f in df.schema.fields:
                    want = have.get(f.name.lower())
                    if want is not None and (isinstance(f.dataType, NullType) or f.dataType != want):
                        raise RuntimeError(
                            f"[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields {f.name!r}: "
                            f"appended {f.dataType.simpleString()} vs table {want.simpleString()}")
                df = spark.table(name).unionByName(df)
            _materialize(spark, df, name)

        def insert_into(self_w, name, overwrite=None):
            if _exists(name) and not overwrite:
                _materialize(spark, spark.table(name).unionByName(self_w._df), name)
            else:
                _materialize(spark, self_w._df, name)

        orig_backend = seq_mod.get_sequence_backend
        backend = _MemorySequenceBackend()

        def get_backend(target_catalog_type, spark=None, **kwargs):
            backend._start = kwargs.get("start", backend._start)
            return backend

        delta_pkg = types.ModuleType("delta")
        delta_tables = types.ModuleType("delta.tables")
        delta_tables.DeltaTable = FakeDeltaTable
        delta_pkg.tables = delta_tables

        saved = {k: sys.modules.get(k) for k in ("delta", "delta.tables")}
        sys.modules["delta"], sys.modules["delta.tables"] = delta_pkg, delta_tables
        from pyspark.sql.catalog import Catalog

        def table_exists(self_c, tableName, dbName=None):  # noqa: N803
            return _exists(tableName)

        import infa_compat
        names_mod = importlib.import_module("infa_compat._names")

        def delta_name(spark_, name):  # the view registry is unqualified
            return name

        stands = [(Catalog, "tableExists", table_exists),
                  (infa_compat, "delta_name", delta_name),
                  (names_mod, "delta_name", delta_name),
                  (DataFrameWriter, "mode", mode),
                  (DataFrameWriter, "saveAsTable", save_as_table),
                  (DataFrameWriter, "insertInto", insert_into),
                  (seq_mod, "get_sequence_backend", get_backend)]
        if os.name == "nt":
            # Writing files through Hadoop needs winutils on Windows. There
            # (only) a flat-file target is written by Python's csv module with
            # Spark's CSV writer defaults: minimal quoting, NULL as an empty
            # field, one header line when header=true. Elsewhere Spark writes.
            orig_option = DataFrameWriter.option

            def option(self_w, key, value):
                opts = getattr(self_w, "_golden_opts", {})
                opts[str(key).lower()] = value
                self_w._golden_opts = opts
                return orig_option(self_w, key, value)

            def csv(self_w, path, **kwargs):
                import csv as _csv
                opts = {**getattr(self_w, "_golden_opts", {}), **{k.lower(): v for k, v in kwargs.items()}}
                df = self_w._df
                out = Path(path)
                if getattr(self_w, "_golden_mode", "") != "append" and out.exists():
                    for f in out.iterdir():
                        f.unlink()
                out.mkdir(parents=True, exist_ok=True)
                sep = str(opts.get("sep", ","))
                quote = str(opts.get("quote", '"')) or '"'
                n = len(list(out.glob("part-*")))
                with open(out / f"part-{n:05d}.csv", "w", encoding="utf-8", newline="") as fh:
                    w = _csv.writer(fh, delimiter=sep, quotechar=quote, quoting=_csv.QUOTE_MINIMAL,
                                    lineterminator="\n")
                    if str(opts.get("header", False)).lower() == "true":
                        w.writerow(df.columns)
                    for r in df.collect():
                        w.writerow(["" if v is None else v for v in r])

            stands += [(DataFrameWriter, "option", option), (DataFrameWriter, "csv", csv)]
        for obj, attr, new in stands:
            self._undo.append((obj, attr, getattr(obj, attr)))
            setattr(obj, attr, new)
        self._undo.append(("modules", saved, None))
        return self

    def __exit__(self, *exc):
        for obj, attr, old in reversed(self._undo):
            if obj == "modules":
                for k, v in attr.items():
                    if v is None:
                        sys.modules.pop(k, None)
                    else:
                        sys.modules[k] = v
            else:
                setattr(obj, attr, old)
        return False


# ---------------------------------------------------------------------------
# Seeding, running, comparing
# ---------------------------------------------------------------------------

def _coerce(value, dtype):
    from pyspark.sql import types as T

    if value is None:
        return None
    if isinstance(dtype, T.DateType):
        return _dt.date.fromisoformat(value)
    if isinstance(dtype, T.TimestampType):
        return _dt.datetime.fromisoformat(value)
    if isinstance(dtype, T.DecimalType):
        return Decimal(str(value))
    if isinstance(dtype, (T.DoubleType, T.FloatType)):
        return float(value)
    if isinstance(dtype, (T.IntegerType, T.LongType, T.ShortType)):
        return int(value)
    return value


def _file_key(name: str) -> str:
    """The SRCFILE_/TGTFILE_ parameter suffix the generator derives from a
    flat-file source or target name, lower-cased as spark.conf keys are."""
    return re.sub(r"[^0-9A-Za-z]+", "_", name).strip("_").lower()


def _seed(spark, seed: dict, refs: set[str]) -> dict[str, str]:
    """Create a temp view per seeded table; returns {notebook ref: view}."""
    from pyspark.sql import types as T  # noqa: F401

    by_last = {k.split(".")[-1].upper(): v for k, v in seed.items()}
    mapping = {}
    for ref in refs:
        mapping[ref] = _view_name(ref)
        spec = by_last.get(ref.split(".")[-1].upper())
        _VIEWS.discard(mapping[ref])
        if spec is None:
            continue
        schema = spark.createDataFrame([], spec["schema"]).schema
        rows = [tuple(_coerce(v, f.dataType) for v, f in zip(r, schema.fields)) for r in spec["rows"]]
        spark.createDataFrame(rows, schema).createOrReplaceTempView(mapping[ref])
        _VIEWS.add(mapping[ref])
    return mapping


def _normalize(v):
    # Numbers compare by value whatever their Python type: an Oracle
    # NUMBER(10,0) key arrives as Decimal('4'), the expected file says 4.
    if isinstance(v, bool):
        return v
    if isinstance(v, (Decimal, float, int)):
        return round(float(v), 6)
    if isinstance(v, (_dt.datetime, _dt.date)):
        return v.isoformat()
    return v


def run_case(spark, case_dir: Path, tmp_path: Path) -> dict:
    """Migrate the case's export, run its notebook, return {target: rows}."""
    import logging
    from infa2aidp.migrator import run_migration

    logging.disable(logging.CRITICAL)
    case = json.loads((case_dir / "case.json").read_text(encoding="utf-8"))
    seed = json.loads((case_dir / "seed.json").read_text(encoding="utf-8"))
    expected = json.loads((case_dir / "expected.json").read_text(encoding="utf-8"))

    out = tmp_path / "out"
    run_migration([str(case_dir / case["export"])], str(out), use_llm=False,
                  skip_lineage=True, skip_optimize=True, score_confidence=False)
    notebooks = sorted(out.glob("*/*.ipynb"))
    if case.get("mapping"):
        notebooks = [n for n in notebooks if n.stem == f"nb_{case['mapping']}"]
    if len(notebooks) != 1:
        raise AssertionError(f"expected one notebook, found {[n.name for n in notebooks]}")
    nb = json.loads(notebooks[0].read_text(encoding="utf-8"))
    cells = [(i, "".join(c["source"])) for i, c in enumerate(nb["cells"]) if c["cell_type"] == "code"]

    refs = set()
    for _, src in cells:
        for m in _TABLE_REF.finditer(src):
            refs.add(next(g for g in m.groups() if g))
        refs |= _sql_refs(src)
    views = _seed(spark, seed, refs)

    for name, value in case.get("params", {}).items():
        spark.conf.set(f"migration.{name.lower()}", str(value))
    # Flat files: a seed with "lines" is written to a file whose path the
    # notebook reads through its SRCFILE_<SOURCE> parameter; an expected
    # target with "lines" is written to a directory set as TGTFILE_<TARGET>.
    file_confs = []
    for name, spec in seed.items():
        if spec.get("lines") is not None:
            path = tmp_path / f"src_{_file_key(name)}.txt"
            path.write_text("".join(ln + "\n" for ln in spec["lines"]), encoding="utf-8")
            file_confs.append((f"migration.srcfile_{_file_key(name)}", str(path)))
    out_dirs = {}
    for name, spec in expected.items():
        if spec.get("lines") is not None:
            out_dirs[name] = tmp_path / f"tgt_{_file_key(name)}"
            file_confs.append((f"migration.tgtfile_{_file_key(name)}", str(out_dirs[name])))
    for key, value in file_confs:
        spark.conf.set(key, value)
    pinned = case.get("session_start")

    ns = {"__name__": "__golden__", "spark": spark}
    try:
        with _Stands(spark):
            for idx, src in cells:
                if "_assumption_targets" in src:
                    continue  # deployment property, not logic -- see module docstring
                for ref in sorted(views, key=len, reverse=True):
                    src = src.replace(f'"{ref}"', f'"{views[ref]}"')
                src = _rewrite_sql(src, views)
                if pinned:
                    src = src.replace("_SESSION_START_TIME = datetime.now()",
                                      f"_SESSION_START_TIME = datetime.fromisoformat({pinned!r})")
                try:
                    exec(compile(src, f"{notebooks[0].name}:cell{idx}", "exec"), ns)
                except Exception as e:  # noqa: BLE001 -- report the cell, whatever it raised
                    first = (str(e).strip().splitlines() or [""])[0]
                    raise GoldenRunError(f"{notebooks[0].name} cell {idx} raised "
                                         f"{type(e).__name__}: {first[:400]}\n--- cell ---\n{src[:1500]}") from e
    finally:
        for name in case.get("params", {}):
            spark.conf.unset(f"migration.{name.lower()}")
        for key, _ in file_confs:
            spark.conf.unset(key)

    results = {}
    for target, spec in expected.items():
        if target in out_dirs:
            d = out_dirs[target]
            # Read with Python, not Spark: listing a directory through Hadoop
            # needs its native layer on Windows. part-* skips _SUCCESS/.crc.
            results[target] = None if not d.exists() else [
                line for f in sorted(d.glob("part-*"))
                for line in f.read_text(encoding="utf-8").splitlines()]
            continue
        ref = next((r for r in views if r.split(".")[-1].upper() == target.split(".")[-1].upper()), None)
        if ref is None or not _exists(views[ref]):
            results[target] = None
            continue
        results[target] = [r.asDict() for r in spark.table(views[ref]).collect()]
    return results


def compare(expected: dict, actual: dict) -> list[str]:
    """Differences between expected and actual rows, as readable lines."""
    problems = []
    for target, spec in expected.items():
        got = actual.get(target)
        if got is None:
            problems.append(f"{target}: nothing was written")
            continue
        if spec.get("lines") is not None:
            # A flat-file target: its lines, order-insensitive.
            if sorted(got) != sorted(spec["lines"]):
                problems.append(f"{target}: file lines differ\n  expected {sorted(spec['lines'])}\n"
                                f"  written  {sorted(got)}")
            continue
        ignore = set(spec.get("ignore", []))
        want_rows = spec["rows"]
        cols = sorted({c for r in want_rows for c in r} - ignore) if want_rows else []
        if got and cols:
            missing = [c for c in cols if c not in got[0]]
            if missing:
                problems.append(f"{target}: columns missing from the written rows: {missing}")
                continue

        def key(r):
            return tuple(repr(_normalize(r.get(c))) for c in cols)

        want = sorted((key(r) for r in want_rows))
        have = sorted((key(r) for r in got))
        if want != have:
            extra = [dict(zip(cols, k)) for k in have if k not in want]
            lost = [dict(zip(cols, k)) for k in want if k not in have]
            problems.append(f"{target}: {len(have)} rows written, {len(want)} expected\n"
                            f"  expected but missing: {lost[:8]}\n  written but unexpected: {extra[:8]}")
    return problems


def cases_root() -> Path:
    return Path(__file__).parent / "cases"

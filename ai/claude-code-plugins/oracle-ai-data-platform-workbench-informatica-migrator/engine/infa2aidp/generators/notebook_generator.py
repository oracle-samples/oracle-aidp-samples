"""Generate AIDP notebooks from parsed Informatica mappings.

Produces Jupyter notebooks (.ipynb) executable on AIDP,
with optional Databricks .py format support.
"""

import json
from datetime import datetime, timezone
from typing import Optional

from ..spark_target import TARGET_SPARK_STR
from ..models import (
    DataFlowDirection,
    FieldMapping,
    LoadStrategy,
    Mapping,
    Session,
    SourceDefinition,
    TargetDefinition,
    Transformation,
    TransformationField,
    TransformationType,
)
from ..properties import get_ci
from .write_strategies import AdwWriteStrategy, get_write_strategy

import re as _re

CELL_SEP = "# COMMAND ----------"

# The four Informatica Update Strategy codes (Class C) --
# detecting any of these in a target's resolved update-strategy
# expression routes the write through infa_compat.apply_update_strategy()/
# write_update_strategy() rather than the generic keys-based MERGE default.
_DD_STRATEGY_TOKENS = ("DD_INSERT", "DD_UPDATE", "DD_DELETE", "DD_REJECT")


def _param_kind(datatype: str) -> str:
    """Informatica port datatype -> the Python kind ``_param`` returns."""
    d = (datatype or "").lower()
    if "date" in d or "time" in d:
        return "datetime"
    if d in ("integer", "small integer", "smallint", "bigint", "int"):
        return "integer"
    if d in ("decimal", "double", "real", "number", "numeric", "float"):
        return "decimal"
    return "string"


def _safe_name(name: str) -> str:
    """Convert a transformation/group name to a safe Python variable suffix."""
    return _re.sub(r'[^a-zA-Z0-9]', '_', name).lower().strip('_')


class NotebookGenerator:
    """Generates executable AIDP Python notebooks from Informatica mappings."""

    def __init__(self, target_catalog_type: str = "delta"):
        """
        Args:
            target_catalog_type: Which :mod:`write_strategies.WriteStrategy`
                to use for every target write cell this generator produces
                -- "delta" (managed Delta, the default) or "adw" (external
                ADW/ALH/ATP catalog). Always an explicit input threaded from
                config/CLI through ``run_migration`` -- never inferred from
                a table name or connection string.
        """
        self.target_catalog_type = (target_catalog_type or "delta").strip().lower()
        self._write_strategy = get_write_strategy(self.target_catalog_type)

    def generate(
        self,
        mapping: Mapping,
        session: Session,
        conversion_result: dict,
        output_format: str = "ipynb",
    ) -> str:
        """Return a complete AIDP notebook as a string.

        Args:
            mapping: Parsed Informatica mapping.
            session: Session metadata (connections, parameters).
            conversion_result: Dict with converted PySpark snippets keyed
                by transformation name. Expected keys:
                - "transformations": dict[str, str] of name -> PySpark code
                - "source_reads": list[str] of source read snippets
                - "target_write": str of target write snippet
            output_format: "ipynb" (Jupyter notebook, default) or "py" (Databricks .py)
        """
        cells: list[str] = []

        cells.append(self._header_cell(mapping))
        cells.append(self._infa_compat_header_cell())
        cells.append(self._setup_cell())
        cells.append(self._catalog_assumption_cell(mapping))
        cells.append(self._parameters_cell(mapping, session))
        pre_sql = self._session_sql_cell(session, "pre")
        if pre_sql:
            cells.append(pre_sql)
        # Target load order groups are separate pipelines that the session
        # runs one after another, each written before the next starts. So a
        # mapping with several groups is generated group by group: a later
        # group's lookup on an earlier group's target then reads it after
        # that target is written (a parent dimension loaded before the fact
        # that looks up its keys), as the session does.
        for part in self._load_order_parts(mapping):
            cells.extend(self._pipeline_cells(part, session, conversion_result))
        post_sql = self._session_sql_cell(session, "post")
        if post_sql:
            cells.append(post_sql)
        cells.append(self._summary_cell(mapping))
        cells = self._alias_unwritten_main_chain(cells)

        if output_format == "ipynb":
            return self._assemble_ipynb(cells, mapping)
        return self._assemble(cells)

    @staticmethod
    def _alias_unwritten_main_chain(cells: list) -> list:
        """Bind the bare name ``df`` when something reads it and nothing
        writes it.

        ``df`` is a real variable in this generator: a transformation that
        is the sole reader of the main chain off a source continues in place
        and the plan assigns ``out = "df"``. But the Sequence Generator path
        records ``df_out = "df"`` as a placeholder to be re-pointed at its
        consumer's input, and several paths reached a consumer without the
        re-point happening -- so the notebook read ``df`` that nothing had
        assigned and raised NameError before touching data. Several
        notebooks seen during breadth testing did exactly this.

        Rather than keep patching the bookkeeping branch by branch, this
        binds ``df`` to the main chain's first source variable when, and
        only when, the assembled notebook reads it unwritten. ``df_source``
        is the main chain by construction, which is the same thing the
        in-place branch means by ``df``.

        A no-op on a notebook that already assigns ``df``, and a no-op when
        nothing reads it, so it cannot change output that was already
        correct. It is a safety net, not a substitute for the bookkeeping
        being right: ``migrate`` still reports any notebook that comes out
        broken.
        """
        from .code_validation import unresolved_names

        code = "\n".join(c for c in cells if isinstance(c, str))
        try:
            missing = set(unresolved_names(code))
        except SyntaxError:
            return cells          # a parse failure is reported elsewhere
        if "df" not in missing or "df_source" not in code:
            return cells

        alias = ("# `df` is the main chain. The plan left it unbound on this "
                 "path (a\n"
                 "# Sequence Generator placeholder that was never re-pointed), and "
                 "a\n"
                 "# later cell reads it, so it is bound here to the main chain's\n"
                 "# source. See NotebookGenerator._alias_unwritten_main_chain.\n"
                 "df = df_source")
        out = list(cells)
        for i, c in enumerate(out):
            if isinstance(c, str) and "df_source = spark.table(" in c:
                out.insert(i + 1, alias)
                return out
        for i, c in enumerate(out):
            if isinstance(c, str) and "df_source" in c:
                out.insert(i + 1, alias)
                return out
        return cells

    # ------------------------------------------------------------------
    # Cell builders
    # ------------------------------------------------------------------

    def _pipeline_cells(self, mapping: Mapping, session, conversion_result: dict) -> list[str]:
        cells = list(self._source_cells(mapping, session, conversion_result))
        cells.extend(self._transformation_cells(mapping, conversion_result))
        cells.append(self._validation_cell(mapping))
        if len(mapping.targets) > 1:
            for idx, target in enumerate(mapping.targets):
                cells.append(self._target_write_cell_for(mapping, target, conversion_result, idx, session))
        else:
            cells.append(self._target_write_cell(mapping, conversion_result, session))
        return cells

    @staticmethod
    def _load_order_parts(mapping: Mapping) -> list:
        """The mapping split into its target load order groups, lowest ORDER
        first -- each group's targets with every instance upstream of them
        -- or ``[mapping]`` when there is one group (or the pipelines are
        not separable)."""
        import dataclasses

        order = getattr(mapping, "target_load_order", None) or {}
        groups: dict = {}
        for t in mapping.targets:
            groups.setdefault(order.get(t.name, 1), []).append(t)
        if len(groups) < 2 or not mapping.connectors:
            return [mapping]
        back: dict = {}
        for c in mapping.connectors:
            back.setdefault(c.to_instance, set()).add(c.from_instance)
        connected = {c.from_instance for c in mapping.connectors} | {c.to_instance for c in mapping.connectors}
        parts, claimed = [], set()
        for key in sorted(groups):
            names, stack = set(), [t.name for t in groups[key]]
            while stack:
                n = stack.pop()
                if n in names:
                    continue
                names.add(n)
                stack.extend(back.get(n, ()))
            if names & claimed - {t.name for t in mapping.targets}:
                return [mapping]      # a transformation feeds two groups: one pipeline
            claimed |= names
            txs = [tx for tx in mapping.transformations
                   if tx.name in names or tx.name not in connected]   # unconnected lookups: everywhere
            parts.append(dataclasses.replace(
                mapping,
                targets=list(groups[key]),
                transformations=txs,
                sources=[s for s in mapping.sources if s.name in names],
                connectors=[c for c in mapping.connectors if c.to_instance in names],
            ))
        return parts

    def _header_cell(self, mapping: Mapping) -> str:
        source_names = ", ".join(s.name for s in mapping.sources) or "N/A"
        target_names = ", ".join(t.table_name or t.name for t in mapping.targets) or "N/A"
        notes = ""
        if mapping.notes:
            notes = "# MAGIC \n# MAGIC **Parser notes**\n" + "".join(
                f"# MAGIC - {n}\n" for n in mapping.notes
            )
        return (
            "# MAGIC %md\n"
            f"# MAGIC # {mapping.name}\n"
            f"# MAGIC **Auto-migrated from Informatica** | "
            f"Folder: {mapping.folder or 'Migrated'}\n"
            f"# MAGIC \n"
            f"# MAGIC - Source: `{source_names}`\n"
            f"# MAGIC - Target: `{target_names}`\n"
            f"# MAGIC - Migrated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"{notes}"
            f"# MAGIC \n"
            f"# MAGIC > **Auto-generated — review before production use**"
        )

    @staticmethod
    def _infa_compat_header_cell() -> str:
        """Deterministically insert an infa_compat import + version-pin cell.

        Mirrors ``LLMNotebookGenerator._ensure_infa_compat_header`` (spec
        S7, "Accepted costs: version skew") -- every generated notebook,
        rule-based or LLM, must fail loudly at execution time if the
        cluster's installed ``infa_compat`` differs from the one this
        notebook was generated against, rather than silently changing
        SCD2/lookup/sequence/update-strategy semantics. Inserted here in
        Python (not left for a per-transformation cell to remember) so
        every rule-based notebook carries it regardless of which Class C
        constructs that particular mapping happens to use.
        """
        import infa_compat as _infa_compat_runtime

        generated_against = _infa_compat_runtime.__version__
        return (
            "import infa_compat\n"
            f"_INFA_COMPAT_GENERATED_AGAINST = {generated_against!r}\n"
            "if infa_compat.__version__ != _INFA_COMPAT_GENERATED_AGAINST:\n"
            "    raise RuntimeError(\n"
            "        f\"This notebook was generated against infa_compat \"\n"
            "        f\"{_INFA_COMPAT_GENERATED_AGAINST!r}, but the cluster has \"\n"
            "        f\"{infa_compat.__version__!r} installed. Version skew can \"\n"
            "        f\"silently change SCD2/lookup/sequence/update-strategy \"\n"
            "        f\"semantics -- reinstall the matching infa_compat build \"\n"
            "        f\"before re-running.\"\n"
            "    )"
        )

    def _catalog_assumption_cell(self, mapping: Mapping) -> str:
        """Fail loudly, before any write happens, if a target's runtime
        catalog type differs from ``self.target_catalog_type`` -- the type
        this notebook was generated for.

        a Delta-generated notebook run
        against ADW (or vice versa) must not half-execute and leave a
        partly written target -- e.g. the ADW staging+MERGE path has
        already overwritten a staging table by the time a Delta-only
        ``DeltaTable.forName()`` call would raise, or a Delta MERGE call
        raises after upstream transformation cells already ran. Catching
        the mismatch here, before the write cell(s), means the notebook
        stops clean instead of mid-write.
        """
        if not mapping.targets:
            return (
                "# No target defined -- nothing to assert a catalog-type "
                "assumption against."
            )

        tables = []
        for target in mapping.targets:
            if getattr(target, "flat_file", None):
                continue  # a file has no catalog type
            tgt_parts = []
            if target.db_name:
                tgt_parts.append(target.db_name)
            if target.owner:
                tgt_parts.append(target.owner)
            tgt_parts.append(target.warehouse_table or target.table_name or target.name)
            tables.append(".".join(tgt_parts))

        lines = [
            "# Assert the runtime catalog type matches what this notebook was",
            f"# generated for ({self.target_catalog_type!r}) -- before any write,",
            "# not mid-write. A managed-Delta-only merge call fails at runtime",
            "# against an external catalog; an ADW staging+MERGE run against a",
            "# managed Delta table would run needlessly and could leave a stray",
            "# staging table behind. Either way, half-executing and leaving the",
            "# target partly written is worse than failing here.",
            f"_expected_catalog_type = {self.target_catalog_type!r}",
            "_assumption_targets = [",
        ]
        for table in tables:
            lines.append(f'    "{table}",')
        lines.append("]")
        lines.extend([
            "for _tbl in _assumption_targets:",
            "    if not spark.catalog.tableExists(_tbl):",
            "        # First run on a fresh workspace: the target does not exist",
            "        # yet, so there is no catalog type to assert against -- the",
            "        # write cell creates it. Without this branch DESCRIBE DETAIL",
            "        # raised, _fmt became '' and every first run failed here",
            "        # (seen on AIDP Spark 3.5.0, 2026-09-24).",
            '        logger.warning(f"Target {_tbl} does not exist yet -- it will be created by the write cell")',
            "        continue",
            "    try:",
            '        _fmt = (spark.sql(f"DESCRIBE DETAIL {_tbl}").select("format").first()[0] or "").lower()',
            "    except Exception:",
            "        # DESCRIBE DETAIL is Delta-specific; a table that isn't a",
            "        # managed Delta table (e.g. an ADW-backed external catalog",
            "        # entry) raises here rather than returning a format -- that",
            "        # itself means \"not delta\".",
            '        _fmt = ""',
            '    _is_delta = _fmt == "delta"',
            '    if _expected_catalog_type == "delta" and not _is_delta:',
            "        raise RuntimeError(",
            f'            f"Notebook generated for target_catalog_type=\'delta\' but "',
            '            f"{_tbl} is not a Delta table at runtime (format={_fmt!r}). "',
            '            f"Re-run the migrator with --target-catalog-type matching "',
            '            f"this table\'s real catalog before executing this notebook."',
            "        )",
            '    if _expected_catalog_type == "adw" and _is_delta:',
            "        raise RuntimeError(",
            f'            f"Notebook generated for target_catalog_type=\'adw\' but "',
            '            f"{_tbl} is a managed Delta table at runtime. Re-run the "',
            '            f"migrator with --target-catalog-type delta instead."',
            "        )",
        ])
        return "\n".join(lines)

    @staticmethod
    def _setup_cell() -> str:
        return (
            "from pyspark.sql import SparkSession, functions as F, Window\n"
            "from pyspark.sql.types import *\n"
            "from datetime import datetime\n"
            "import logging\n"
            "\n"
            'logging.basicConfig(level=logging.INFO)\n'
            'logger = logging.getLogger(__name__)\n'
            "\n"
            "spark = SparkSession.builder.getOrCreate()\n"
            "\n"
            "# The oldest Spark this notebook supports. A FLOOR, not a match:\n"
            "# the converter emits only constructs 3.5 and 4.x both accept, and\n"
            "# pins the behavioural flags below, so this file is valid here and\n"
            "# on anything newer. The deployer reads this line and refuses a\n"
            "# cluster older than it.\n"
            f'_GENERATED_FOR_SPARK_MIN = "{TARGET_SPARK_STR}"\n'
            "\n"
            "# Informatica evaluates expressions permissively: a failed cast, a\n"
            "# divide by zero and a numeric overflow all yield NULL (or the\n"
            "# port default) and a row error, not an aborted run. Spark's ANSI\n"
            "# mode raises instead. Migrated logic was written against the\n"
            "# permissive behaviour, so ANSI is pinned off here to preserve it.\n"
            "#\n"
            "# Pinned rather than inherited on purpose. Spark 3.5 defaults ANSI\n"
            "# off and Spark 4 defaults it ON, so a notebook that relies on the\n"
            "# cluster default silently changes behaviour when the runtime is\n"
            "# upgraded underneath it -- rows that used to be NULL become an\n"
            "# aborted job, and nothing in the notebook changed to explain it.\n"
            'spark.conf.set("spark.sql.ansi.enabled", "false")\n'
            "\n"
            "# Same reasoning for writes: Spark 4 defaults storeAssignmentPolicy\n"
            "# to ANSI, which raises when a value does not fit the target\n"
            "# column. Informatica truncates or nulls and logs a row error.\n"
            'spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")\n'
            "\n"
            "# Informatica's SESSSTARTTIME / $$$SessStartTime: ONE value for the\n"
            "# whole run, bound here, never re-evaluated per row.\n"
            "_SESSION_START_TIME = datetime.now()\n"
            "\n"
            "def _rename_cols(df, pairs):\n"
            "    \"\"\"Apply connector renames [(from, to), ...] at once.\n"
            "\n"
            "    A downstream port receives only what its connector carries, so a\n"
            "    column already named like a rename target is the stale one and is\n"
            "    dropped -- e.g. an Expression's input-only CUST_NAME when the\n"
            "    connector feeds CUST_NAME from CUST_NAME_CLEAN. withColumnRenamed\n"
            "    would leave two CUST_NAME columns (AMBIGUOUS_REFERENCE) and apply\n"
            "    a swap in the wrong order. A pair whose source column is absent is\n"
            "    skipped, as withColumnRenamed would.\"\"\"\n"
            "    active = {f: t for f, t in pairs if f in df.columns}\n"
            "    shadowed = set(active.values()) - set(active)\n"
            "    return df.select([F.col(c).alias(active.get(c, c)) for c in df.columns if c not in shadowed])"
        )

    @staticmethod
    def _referenced_params(mapping: Mapping) -> set[str]:
        """``$$NAME`` references in every expression/condition of the mapping."""
        names: set[str] = set()
        for tx in mapping.transformations:
            texts = [
                tx.filter_condition, tx.join_condition, tx.lookup_condition,
                tx.lookup_sql, tx.sql_override, tx.source_filter,
                tx.update_strategy_expression, tx.lookup_source_filter,
            ]
            texts.extend(g.get("condition", "") for g in (tx.router_groups or []) if isinstance(g, dict))
            texts.extend(f.expression for f in tx.fields if isinstance(f, TransformationField))
            for text in texts:
                if text:
                    names.update(_re.findall(r"(?<!\$)\$\$(\w+)", text))
        return names

    def _parameters_cell(self, mapping: Mapping, session: Session) -> str:
        """Define ``_param(name)`` for the ``$$PARAM`` references the
        expression converter emits, with the mapping's DEFAULTVALUEs.

        Values are read, in order, from AIDP job/task parameters
        (``oidlUtils.parameters.getParameter(name, default)`` -- job run >
        task > job), from ``spark.conf`` under ``migration.<name>``
        (lower-case), then from the mapping's DEFAULTVALUE. AIDP does NOT
        copy job parameters into ``spark.conf`` (verified 2026-09-25), so a
        ``spark.conf``-only lookup silently ignored every per-run override
        and ran with the default. The previous cell emitted one Python assignment per
        parameter named after the raw key -- for a session ATTRIBUTE like
        "Treat source rows as" that was ``TREAT SOURCE ROWS AS = ...``, a
        SyntaxError that made every notebook for a real session invalid.
        Names are never used as identifiers here.
        """
        defaults: dict[str, str] = {}
        for key, default in {**mapping.parameters, **session.parameters}.items():
            name = key.lstrip("$")
            if name and _re.fullmatch(r"\w+", name):
                defaults[name] = str(default)
        for name in sorted(self._referenced_params(mapping)):
            defaults.setdefault(name, None)  # referenced, no declared default
        defaults.setdefault("BATCH_ID", "0")
        types: dict[str, str] = {}
        for key, dtype in (getattr(mapping, "parameter_types", None) or {}).items():
            name = key.lstrip("$")
            kind = _param_kind(dtype)
            if name in defaults and kind != "string":
                types[name] = kind

        lines = [
            "# Migration parameters (Informatica $$variables). Override any of",
            "# them with an AIDP job or task parameter named migration.<name>",
            "# (lower-case) or <NAME>, or with spark.conf \"migration.<name>\";",
            "# otherwise the mapping's DEFAULTVALUE applies.",
            "_PARAM_DEFAULTS = {",
        ]
        for name in sorted(defaults):
            lines.append(f"    {name!r}: {defaults[name]!r},")
        lines.append("}")
        lines.append("# Declared MAPPINGVARIABLE datatypes (string when absent).")
        lines.append("_PARAM_TYPES = {")
        for name in sorted(types):
            lines.append(f"    {name!r}: {types[name]!r},")
        lines.append("}")
        lines.extend([
            "",
            "_UNSET = '__infa2aidp_unset__'",
            "",
            "def _typed(name, text):",
            "    # A parameter's value is its declared type: '02/01/2026 00:00:00' for",
            "    # a date/time parameter is a timestamp. Informatica's default",
            "    # date/time text is MM/DD/YYYY HH24:MI:SS[.US].",
            "    kind = _PARAM_TYPES.get(name, 'string')",
            "    if text is None or kind == 'string':",
            "        return text",
            "    s = str(text).strip()",
            "    if kind == 'datetime':",
            "        for fmt in ('%m/%d/%Y %H:%M:%S.%f', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M',",
            "                    '%m/%d/%Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):",
            "            try:",
            "                return datetime.strptime(s, fmt)",
            "            except ValueError:",
            "                pass",
            "        raise ValueError(f'Informatica parameter $${name} = {s!r} is not a date/time')",
            "    if kind == 'integer':",
            "        return int(float(s))",
            "    from decimal import Decimal",
            "    return Decimal(s)",
            "",
            "def _param_text(name, default=None):",
            "    # The raw text, as Informatica substitutes it into SQL (Source Filter,",
            "    # SQL override): a date parameter goes in as written, not reformatted.",
            "    return _param_raw(name, default)",
            "",
            "def _param(name, default=None):",
            "    return _typed(name, _param_raw(name, default))",
            "",
            "def _param_raw(name, default=None):",
            "    value = None",
            "    try:",
            "        # AIDP job/task parameters. getParameter needs the two-argument",
            "        # form: with one argument it raises for an unset name.",
            "        for _key in (f\"migration.{name.lower()}\", name):",
            "            _v = oidlUtils.parameters.getParameter(_key, _UNSET)  # noqa: F821",
            "            if _v != _UNSET:",
            "                value = _v",
            "                break",
            "    except Exception:",
            "        # Not on AIDP (no oidlUtils), or the lookup failed (e.g. outside",
            "        # a job run): fall back to spark.conf, then the default.",
            "        value = None",
            "    if value is None:",
            '        value = spark.conf.get(f"migration.{name.lower()}", None)',
            "    if value is None:",
            "        value = _PARAM_DEFAULTS.get(name, default)",
            "    if value is None:",
            '        raise KeyError(f"Informatica parameter $${name} has no value: set an AIDP job '
            'parameter migration.{name.lower()} (or spark.conf migration.{name.lower()}) '
            'before running this notebook")',
            "    return value",
            "",
            "def _sql_lit(value):",
            '    """A parameter value as a SQL string literal for F.expr()/spark.sql().',
            "",
            "    Backslash-escaped, because that is Spark's literal syntax: Oracle's",
            "    doubled quote reads in Spark as two adjacent literals ('it''s' is",
            "    'its'), and an unescaped backslash starts an escape sequence.",
            '    """',
            "    return \"'\" + str(value).replace(chr(92), chr(92) * 2).replace(\"'\", chr(92) + \"'\") + \"'\"",
        ])
        return "\n".join(lines)

    @staticmethod
    def _session_sql_cell(session: Session, when: str) -> str:
        """Emit a session's pre- or post-SQL, or nothing if it has none.

        Informatica runs these against the source or target connection
        around the data movement: a pre-SQL might disable a constraint or
        truncate a staging table, a post-SQL might rebuild an index or
        stamp a control table. They were parsed into the Session model and
        never reached a generator, so the side effect disappeared and the
        migrated job looked complete while doing less than the original.

        Emitted commented-out on purpose. The statement is written in the
        source database's dialect against a connection this notebook does
        not hold, so running it unchanged through spark.sql() would either
        fail or -- worse -- succeed against the wrong system. The operator
        has to decide where it runs. Silently dropping it was the bug;
        silently running it would be a different one.
        """
        raw = (session.pre_sql if when == "pre" else session.post_sql) or ""
        raw = raw.strip()
        if not raw:
            return ""
        label = "PRE-SESSION" if when == "pre" else "POST-SESSION"
        placement = (
            "before the source is read" if when == "pre"
            else "after the target write completes"
        )
        lines = [
            f"# {label} SQL from Informatica session '{session.name}'",
            f"# Informatica ran this {placement}, against the session's own",
            "# connection -- not against Spark. It is reproduced here rather than",
            "# dropped, and left COMMENTED OUT because the statement is in the",
            "# source database's dialect and this notebook does not hold that",
            "# connection. Decide where it should run: a JDBC call from the",
            "# driver, a separate job task, or a DBA step outside the pipeline.",
            "#",
            "# REVIEW REQUIRED: session SQL not executed by this notebook.",
        ]
        lines += ["# " + ln for ln in raw.splitlines()]
        return "\n".join(lines)

    def _source_cells(
        self,
        mapping: Mapping,
        session: Session,
        conversion_result: dict,
    ) -> list[str]:
        """One cell per source definition."""
        cells: list[str] = []
        custom_reads = conversion_result.get("source_reads", [])

        for idx, src in enumerate(mapping.sources):
            if idx < len(custom_reads):
                cells.append(custom_reads[idx])
                continue

            cells.append(self._default_source_read(mapping, src, session, idx))

        if not cells:
            # The export carries no usable source definition. A placeholder
            # read used to be emitted silently, and the rest of the notebook
            # was then generated against it: downstream cells referenced
            # df_source_1, df_source_2 ... that no cell ever assigned, and a
            # Joiner prepared both sides while only one was reachable -- so
            # the join was dropped and the notebook raised NameError. Found
            # where a mapping-only export omits its source definitions.
            #
            # A notebook built on a placeholder cannot run whatever else is
            # done to it, so it says so instead of looking finished. The
            # placeholders are still bound, one per source the mapping
            # references, so the failure is the explicit refusal below rather
            # than an incidental NameError further down.
            # Count Source Qualifiers, not source DEFINITIONS: this branch
            # runs precisely because the definitions are missing, while the
            # dataflow still references one df_source per SQ. Binding only
            # df_source left df_source_1 unassigned downstream.
            n_sqs = sum(1 for tx in (mapping.transformations or [])
                        if tx.type in (TransformationType.SOURCE_QUALIFIER,
                                       TransformationType.APPLICATION_SOURCE_QUALIFIER,
                                       TransformationType.MQ_SOURCE_QUALIFIER,
                                       TransformationType.XML_SOURCE_QUALIFIER))
            n_sources = max(1, len(mapping.sources or []), n_sqs)
            placeholder = [
                "# REVIEW REQUIRED: no usable source definition in the export.",
                "#",
                "# Every source read below is a placeholder, so this notebook",
                "# cannot produce correct data. Re-export the mapping WITH its",
                "# source definitions (a mapping-only export omits them), or",
                "# replace each placeholder with the real table or file read.",
                "",
            ]
            for idx in range(n_sources):
                var = "df_source" if idx == 0 else f"df_source_{idx}"
                src_name = ""
                try:
                    src_name = getattr(mapping.sources[idx], "name", "") or ""
                except Exception:
                    pass
                label = f"  # stands in for {src_name}" if src_name else ""
                placeholder.append(
                    f'{var} = spark.sql("SELECT 1 AS placeholder"){label}')
            placeholder += [
                "",
                'raise NotImplementedError(',
                '    "REVIEW REQUIRED: the export carries no source definitions, "',
                '    "so every read in this notebook is a placeholder. Re-export "',
                '    "the mapping with its sources, or wire the reads by hand."',
                ')',
            ]
            cells.append("\n".join(placeholder))
        return cells

    @staticmethod
    def _file_param(prefix: str, name: str) -> str:
        return f"{prefix}_{_re.sub(r'[^0-9A-Za-z]+', '_', name).strip('_').upper()}"

    @staticmethod
    def _ff_quote(ff: dict) -> str:
        q = (ff.get("QUOTE_CHARACTER") or "DOUBLE").upper()
        return {"DOUBLE": '"', "SINGLE": "'", "NONE": ""}.get(q, '"')

    def _flat_file_read(self, mapping: Mapping, src, session: Session, df_name: str) -> list[str]:
        """Read a DATABASETYPE="Flat File" source.

        The path is the session File Reader's directory + filename, which
        on AIDP has to become a volume or object-storage path: it is read
        through ``_param_text("SRCFILE_<SOURCE>")`` so a job parameter sets
        it per run. Every column is read as text and cast to the declared
        type, as the Integration Service does; an empty field is NULL.
        """
        ff = src.flat_file
        opts = {}
        if session is not None:
            for key in [src.name] + [c.to_instance for c in mapping.connectors if c.from_instance == src.name]:
                opts = session.source_file_options.get(key) or opts
        directory = (opts.get("Source file directory") or "$PMSourceFileDir/").replace("\\", "/")
        filename = opts.get("Source filename") or f"{src.name}.dat"
        default_path = directory.rstrip("/") + "/" + filename
        param = self._file_param("SRCFILE", src.name)
        fields = [f for f in src.fields if isinstance(f, FieldMapping)]
        names = [f.source_field for f in fields]
        try:
            skip = int(ff.get("SKIPROWS") or 0)
        except ValueError:
            skip = 0
        lines = [
            f"# Flat file source: {src.name} "
            f"({'delimited' if str(ff.get('DELIMITED', 'YES')).upper() == 'YES' else 'fixed-width'}"
            f", {skip} header row(s) skipped)",
            f"# Set job parameter migration.{param.lower()} to the file's AIDP path (a volume or",
            f"# object-storage URI); the session read {default_path}.",
            f'_path_{df_name} = _param_text("{param}", {default_path!r})',
        ]
        if str(ff.get("DELIMITED", "YES")).upper() == "YES":
            delim = ff.get("DELIMITERS") or ","
            if len(delim) > 1:
                lines.append(f"# REVIEW REQUIRED: several delimiter characters {delim!r}; only "
                             f"{delim[0]!r} is used below.")
                delim = delim[0]
            schema = ", ".join(f"`{n}` STRING" for n in names)
            lines.append(
                f"{df_name} = (spark.read.option(\"sep\", {delim!r}).option(\"quote\", {self._ff_quote(ff)!r})"
                f".option(\"header\", {'True' if skip >= 1 else 'False'}).option(\"mode\", \"PERMISSIVE\")"
                f".schema({schema!r}).csv(_path_{df_name}))"
            )
            if skip > 1:
                lines.append(f"# REVIEW REQUIRED: SKIPROWS={skip}; only the first row is skipped above.")
        else:
            lines.append(f"_raw = spark.read.text(_path_{df_name})")
            if skip:
                lines.append(
                    f"_raw = _raw.rdd.zipWithIndex().filter(lambda r: r[1] >= {skip})"
                    f".map(lambda r: r[0]).toDF(_raw.schema)"
                )
            parts = []
            offset = 0
            for f in fields:
                length = f.physical_length or f.precision
                start = f.physical_offset if (f.physical_offset or f is fields[0]) else offset
                parts.append(f'F.substring("value", {start + 1}, {length}).alias("{f.source_field}")')
                offset = start + length
            lines.append(f"{df_name} = _raw.select({', '.join(parts)})")
        casts = []
        for f in fields:
            c = f'F.col("{f.source_field}")'
            dt = (f.datatype or "").lower()
            if dt in ("number", "decimal", "numeric", "double", "float", "real", "integer", "bigint",
                      "small integer", "int"):
                c = f"F.trim({c})"
                if dt in ("double", "float", "real"):
                    c = f"{c}.cast('double')"
                elif f.scale or dt in ("number", "decimal", "numeric"):
                    c = f"{c}.cast(DecimalType({f.precision or 18}, {f.scale}))"
                else:
                    c = f"{c}.cast('long')"
            elif "date" in dt or "time" in dt:
                masks = ["MM/dd/yyyy HH:mm:ss", "MM/dd/yyyy", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"]
                c = "F.coalesce(" + ", ".join(f"F.to_timestamp(F.trim({c}), {m!r})" for m in masks) + ")"
            elif str(ff.get("STRIPTRAILINGBLANKS", "NO")).upper() == "YES" or \
                    str(ff.get("DELIMITED", "YES")).upper() != "YES":
                c = f"F.rtrim({c})"
            casts.append(f'{c}.alias("{f.source_field}")')
        lines.append(f"# Declared types (the file is text); date/time accepts MM/DD/YYYY[ HH24:MI:SS].")
        lines.append(f"{df_name} = {df_name}.select({', '.join(casts)})")
        return lines

    def _flat_file_write(self, target, df_var: str, session: Optional[Session], cols: list) -> list[str]:
        """Write a flat-file target: delimited text at the session writer's
        Output file directory + filename, set per run through
        ``_param_text("TGTFILE_<TARGET>")``. Date/time columns are written
        in Informatica's default MM/DD/YYYY HH24:MI:SS. Spark writes a
        directory of part files; ``coalesce(1)`` makes it one part."""
        ff = target.flat_file
        opts = (session.target_load_options.get(target.name) if session is not None else None) or {}
        directory = (opts.get("Output file directory") or "$PMTargetFileDir/").replace("\\", "/")
        filename = opts.get("Output filename") or f"{target.name}.out"
        param = self._file_param("TGTFILE", target.name)
        header = "field names" in (opts.get("Header Options") or "").lower()
        mode = "append" if str(opts.get("Append if Exists", "NO")).upper() == "YES" else "overwrite"
        delim = (ff.get("DELIMITERS") or ",")[:1]
        default_path = directory.rstrip("/") + "/" + filename
        return [
            f"    # Flat file target {target.name}: set job parameter migration.{param.lower()} to the",
            f"    # AIDP path to write (the session wrote {default_path}).",
            f'    _tgt_path = _param_text("{param}", {default_path!r})',
            f"    _ff = {df_var}.select({cols!r})" if cols else f"    _ff = {df_var}",
            "    _ff = _ff.select([F.date_format(F.col(_c), 'MM/dd/yyyy HH:mm:ss').alias(_c) "
            "if _t == 'timestamp' else F.col(_c).cast('string').alias(_c) for _c, _t in _ff.dtypes])",
            f"    (_ff.coalesce(1).write.mode({mode!r}).option(\"sep\", {delim!r})"
            f".option(\"quote\", {self._ff_quote(ff) or chr(34)!r}).option(\"header\", {header})"
            f".csv(_tgt_path))",
        ]

    def _default_source_read(
        self,
        mapping: Mapping,
        src: SourceDefinition,
        session: Session,
        idx: int,
    ) -> str:
        df_name = f"df_source_{idx}" if idx > 0 else "df_source"
        if getattr(src, "flat_file", None):
            return "\n".join(self._flat_file_read(mapping, src, session, df_name))
        # Build fully qualified table name: schema.table or database.schema.table
        table_parts = []
        if src.db_name:
            table_parts.append(src.db_name)
        if src.owner:
            table_parts.append(src.owner)
        table_parts.append(src.table_name or src.name)
        table = ".".join(table_parts)
        lines: list[str] = []

        # All sources are accessed via AIDP external catalog — 3-part name
        # No JDBC connections needed (source is registered in AIDP)
        lines.append(f"# Read source: {table}")

        # The Source Qualifier that reads this source decides WHICH rows are
        # read (SQL override, Source Filter, Select Distinct, User Defined
        # Join). Found via the mapping's CONNECTORs (source instance ->
        # qualifier), falling back to the "Source Table" attribute. The read
        # itself is emitted by the shared converter helper so the notebook
        # and the comparison report agree.
        from ..converters.transformation_converter import TransformationConverter

        sq_tx = None
        fed_sqs = {
            c.to_instance for c in mapping.connectors if c.from_instance == src.name
        }
        for tx in mapping.transformations:
            if tx.type != TransformationType.SOURCE_QUALIFIER:
                continue
            sq_table = get_ci(tx.properties, "source table")
            if tx.name in fed_sqs or (
                sq_table and sq_table.upper() == (src.table_name or src.name).upper()
            ):
                sq_tx = tx
                break

        if sq_tx is not None:
            # Name the Source Qualifier on the read cell. The converter's own
            # first line (`# Source Qualifier: <name>`) is dropped below, so
            # without this the qualifier's name appears NOWHERE in the
            # notebook: a reviewer cannot tie the read back to the export,
            # and the source-fidelity check reported every Source Qualifier
            # as missing -- ten false positives on the bundled exports alone,
            # which trains the reader to skim the report.
            lines[-1] = f"# Read source: {table} (Source Qualifier: {sq_tx.name})"
            # Every source in the mapping, bare name -> catalog-qualified
            # name. A SQL override carries Oracle's unqualified names and
            # Spark needs qualified ones, so without this map an override
            # that JOINs cannot be run at all.
            source_tables = {}
            # ... and its columns: a User Defined Join is written with an
            # explicit select list, since SELECT * repeats every join key.
            source_columns = {}
            for s in mapping.sources:
                parts = [p for p in (s.db_name, s.owner) if p]
                parts.append(s.table_name or s.name)
                source_tables[(s.table_name or s.name).upper()] = ".".join(parts)
                source_columns[(s.table_name or s.name).upper()] = [
                    f.source_field for f in s.fields if getattr(f, "source_field", "")
                ]
            lines.extend(TransformationConverter().source_read_lines(
                sq_tx, table, df_name, source_tables, source_columns)[1:])
        else:
            lines.append(f'{df_name} = spark.table("{table}")')

        lines.append(f'if spark.conf.get("migration.debug", "false") == "true":')
        lines.append(f'    logger.info(f"Source rows ({table}): {{{df_name}.count()}}")')
        return "\n".join(lines)

    def _transformation_cells(
        self,
        mapping: Mapping,
        conversion_result: dict,
    ) -> list[str]:
        """Generate transformation cells following the CONNECTOR-defined data flow DAG.

        Core design:
        1. Build instance-level DAG from CONNECTOR elements
        2. Topologically sort transformations
        3. Track a single `df_out[instance]` variable per transformation output
        4. ALWAYS re-generate code with correct input/output df names
           (never patch pre-generated code — that's fragile and error-prone)
        5. Handle parallel branches, multi-input (Joiner/Union), and
           multi-output (Router → multiple targets)
        """
        from collections import defaultdict
        from ..converters.transformation_converter import TransformationConverter

        cells: list[str] = []
        converter = TransformationConverter()

        # Unconnected Lookups (no connector in or out) are called from
        # expressions with :LKP.NAME(...); they are not dataflow nodes.
        connected_names = {c.from_instance for c in mapping.connectors} | {
            c.to_instance for c in mapping.connectors}
        unconnected_lookups = {
            tx.name.upper(): tx for tx in mapping.transformations
            if tx.type == TransformationType.LOOKUP and tx.name not in connected_names
            and mapping.connectors
        }

        # Build lookups
        tx_map = {tx.name: tx for tx in mapping.transformations
                  if tx.name.upper() not in unconnected_lookups}
        # A connected Lookup whose condition names its own ports is emitted
        # port-exact (qualified columns); mark it so connectors rename the
        # qualified names.
        _probe = TransformationConverter()
        for _tx in tx_map.values():
            if _tx.type == TransformationType.LOOKUP and _tx.lookup_condition:
                from ..converters.transformation_converter import lookup_prefix
                _tx._port_exact = _probe._lookup_plan(
                    _tx, _tx.lookup_table or _tx.properties.get("lookup_table", ""), {},
                    lookup_prefix(_tx)) is not None
        src_map = {s.name: s for s in mapping.sources}
        tgt_map = {t.name: t for t in mapping.targets}
        src_def_names = set(src_map.keys())
        tgt_names = set(tgt_map.keys())

        # Build DAG from connectors (instance-level, deduplicated)
        fwd: dict[str, set[str]] = defaultdict(set)
        back: dict[str, set[str]] = defaultdict(set)
        for conn in mapping.connectors:
            fwd[conn.from_instance].add(conn.to_instance)
            back[conn.to_instance].add(conn.from_instance)

        # Transformation order is only as trustworthy
        # as the CONNECTOR edges it was derived from. Detect both the fully
        # missing case (no edges among the transformations at all -- the
        # linear fallback below) and the partial case (edges exist but
        # touch only some of the transformations -- a real DAG walk that
        # can still be confidently wrong). Reuse the same
        # "# REVIEW REQUIRED: ..." marker established for an
        # unresolvable Joiner master side rather than inventing a second
        # mechanism.
        sq_names_all = {
            tx.name for tx in mapping.transformations
            if tx.type == TransformationType.SOURCE_QUALIFIER
        }
        process_set_all = set(tx_map.keys()) - sq_names_all - tgt_names
        order_review = self._order_underdetermined_review_item(mapping, process_set_all)

        if not mapping.connectors:
            cells = self._transformation_cells_linear(mapping, conversion_result)
            if order_review:
                cells.insert(0, order_review)
            return cells

        # Map Source Qualifier instances to their df variable names
        # These match what _source_cells generates: df_source, df_source_1, ...
        sq_list = [tx.name for tx in mapping.transformations
                   if tx.type == TransformationType.SOURCE_QUALIFIER]
        df_out: dict[str, str] = {}
        for idx, sq in enumerate(sq_list):
            df_out[sq] = f"df_source_{idx}" if idx > 0 else "df_source"

        # Topological sort (Kahn's) — only non-SQ, non-source-def, non-target
        process_set = process_set_all
        in_deg: dict[str, int] = {}
        for name in process_set:
            cnt = 0
            for pred in back.get(name, set()):
                if pred in process_set:
                    cnt += 1
            in_deg[name] = cnt

        queue = sorted(n for n, d in in_deg.items() if d == 0)
        topo: list[str] = []
        while queue:
            node = queue.pop(0)
            topo.append(node)
            for succ in sorted(fwd.get(node, set())):
                if succ in in_deg:
                    in_deg[succ] -= 1
                    if in_deg[succ] == 0:
                        queue.append(succ)
                        queue.sort()
        # Append any remaining (cycle safety)
        for n in sorted(in_deg):
            if n not in topo:
                topo.append(n)

        # ── Phase 1: Assign df variable names ──
        # A transformation writes its output IN PLACE over its input
        # variable only when it is the sole reader of that input -- nothing
        # else (no other transformation, no target write at the end) still
        # needs the old value. Otherwise it gets its own df_<name>. The
        # previous rules shared "df" down every chain and across parallel
        # branches: two Filters fanning out of one Expression ran one after
        # the other, four Joiners into four targets all wrote "df" (every
        # target got the last Joiner's rows), and an Aggregator right after
        # a Router aggregated all rows instead of its group's.
        self._assign_df_names(mapping, topo, tx_map, back, fwd, df_out,
                              sq_list, src_def_names, tgt_names)

        # ── Phase 2: Generate cells ──
        if order_review:
            cells.append(order_review)

        # Sequence Generators: {name: [init lines, assign template, emitted?]}
        seq_parts: dict[str, list] = {}

        for name in topo:
            tx = tx_map.get(name)
            if not tx:
                continue

            # A Sequence Generator adds NEXTVAL to the input of EACH consumer
            # (one sequence object, so the branches share one counter);
            # emitted once, before the first consumer, it only numbered one
            # branch -- SCD2's "new" and "changed" inserts both need keys.
            for seq_name in sorted(seq_parts):
                if name in fwd.get(seq_name, set()):
                    cells.append(self._seq_consumer_cell(
                        seq_name, name, self._input_var.get(name, df_out.get(seq_name, "df")),
                        seq_parts, mapping))

            # Predecessors are needed before input_df resolution for a
            # Joiner: master/detail is resolved from these via
            # `_resolve_joiner_sides`, never from positional order
            #.
            all_preds = self._get_resolved_predecessors(name, back, df_out,
                                                         sq_list, src_def_names, fwd)

            # Add missing target columns BEFORE Router (so both paths get
            # them) -- on the DataFrame that actually feeds the Router, not
            # a hard-coded `df` that may never have been assigned.
            if tx.type == TransformationType.ROUTER:
                router_in = self._resolve_input_df(name, tx, back, fwd, df_out,
                                                   sq_list, src_def_names)
                missing = self._missing_target_columns_cell(mapping, router_in)
                if missing:
                    cells.append(missing)

            joiner_master = joiner_detail = None
            if tx.type == TransformationType.JOINER and len(all_preds) >= 2:
                joiner_master, joiner_detail = self._resolve_joiner_sides(
                    tx, all_preds, mapping
                )

            if joiner_detail is not None:
                input_df = dict(all_preds)[joiner_detail]
            elif name in self._input_var:
                input_df = self._input_var[name]
            else:
                # Find input df: the output of the upstream transformation
                input_df = self._resolve_input_df(name, tx, back, fwd, df_out,
                                                  sq_list, src_def_names)
            output_df = df_out.get(name, "df")

            # Build extra_inputs for multi-input transforms
            extra_inputs = {}

            if tx.type == TransformationType.JOINER and len(all_preds) >= 2:
                # Which input each of the Joiner's ports is actually fed
                # from, read off the CONNECTORs. The condition parser
                # otherwise relies on the Designer's "master written first"
                # convention, which an export can contradict: a condition
                # written `DEPARTMENT_ID = DEPARTMENT_ID_D` where
                # DEPARTMENT_ID is fed by the DETAIL side produced a join
                # referencing each column on the wrong DataFrame.
                detail_pred = joiner_detail
                if detail_pred is None:
                    detail_pred = next(
                        (n for n, d in all_preds if d == input_df), None
                    )
                sides = {}
                for c in mapping.connectors:
                    if c.to_instance != name:
                        continue
                    if c.from_instance == detail_pred:
                        sides[c.to_field] = "detail"
                    elif detail_pred is not None:
                        sides[c.to_field] = "master"
                if sides:
                    extra_inputs["__port_sides__"] = sides

                if joiner_master is not None:
                    extra_inputs[joiner_master] = dict(all_preds)[joiner_master]
                else:
                    # Neither the Master Source property nor any port-level
                    # MASTER/ISMASTER flag identified a master side. Do not
                    # guess the DIRECTION -- an inverted Master/Detail Outer
                    # Join silently changes which rows survive.
                    #
                    # But direction only matters for an OUTER join. An inner
                    # join is symmetric: a.join(b, cond, "inner") and
                    # b.join(a, cond, "inner") contain the same rows. So the
                    # other input is still handed over, under a key that says
                    # the order is unknown, and the converter joins when the
                    # join type is inner and refuses when it is not. Before
                    # this, an export with no MASTER flag lost the join
                    # entirely even when the join type made the direction
                    # irrelevant.
                    extra_inputs["__master_unresolved__"] = "1"
                    others = [d for n, d in all_preds if d != input_df]
                    if others:
                        extra_inputs["__unordered_second__"] = others[0]

            elif tx.type == TransformationType.EXPRESSION and unconnected_lookups:
                extra_inputs["__unconnected_lookups__"] = unconnected_lookups

            if tx.type == TransformationType.UNION:
                for pred_name, pred_df in all_preds[1:]:
                    extra_inputs[pred_name] = pred_df
                branches = self._union_branches(tx, name, mapping, tx_map, df_out)
                if branches:
                    extra_inputs["__union_branches__"] = branches

            elif tx.type == TransformationType.LOOKUP:
                # {lookup input port: upstream column} so the join key in
                # the lookup condition (the lookup's OWN port, e.g.
                # IN_CUST_ID) resolves to the column the pipeline actually
                # carries (CUST_ID).
                extra_inputs["__port_map__"] = {
                    c.to_field: self._pipeline_column(tx_map.get(c.from_instance), c.from_field)
                    for c in mapping.connectors
                    if c.to_instance == name and c.from_field and c.to_field
                }
                lkp_table = (tx.lookup_table or "").split(".")[-1].upper()
                target_tables = {
                    (t.warehouse_table or t.table_name or t.name).split(".")[-1].upper()
                    for t in mapping.targets
                }
                if lkp_table and lkp_table in target_tables and not self._loaded_earlier(
                        mapping, name, lkp_table, fwd, tgt_names):
                    extra_inputs["__snapshot__"] = True

            # ── Consumer-side connector renames ──
            # A connector FROMFIELD -> TOFIELD with different names means
            # this transformation's port is called TOFIELD while the
            # upstream DataFrame's column is called FROMFIELD. The rename is
            # applied on THIS consumer's copy of the input, so a predecessor
            # shared by two consumers with different port names is not
            # mutated for both (which is what the previous producer-side
            # rename did), and so a Source Qualifier's columns can be
            # renamed at all (SQ instances are never "processed", so their
            # producer-side renames were never emitted). Lookups resolve
            # their ports through __port_map__ instead; Unions keep the
            # converter's own column handling.
            rename_cell_lines: list[str] = []
            merged = self._merged_preds.get(name)
            if merged and tx.type not in (TransformationType.LOOKUP, TransformationType.UNION,
                                          TransformationType.SEQUENCE_GENERATOR):
                # Row-aligned inputs from several upstream instances (SQ and
                # a Lookup on it, say) all arrive in ONE DataFrame, so every
                # connector's rename is applied to it at once. Renaming per
                # predecessor made one copy per predecessor and used one.
                renames = []
                for pred_name in merged:
                    for pair in self._incoming_renames(name, pred_name, mapping, tx_map):
                        if pair not in renames:
                            renames.append(pair)
                if renames:
                    renamed_var = f"df_in_{_safe_name(name)}"
                    rename_cell_lines.append(
                        f"# {name}: rename {', '.join(merged)} columns to this transformation's port names"
                    )
                    rename_cell_lines.append(f"{renamed_var} = _rename_cols({input_df}, {renames!r})")
                    input_df = renamed_var
            elif tx.type not in (TransformationType.LOOKUP, TransformationType.UNION,
                                 TransformationType.SEQUENCE_GENERATOR):
                for pred_name, pred_df in all_preds:
                    pred_tx = tx_map.get(pred_name)
                    src_df = pred_df
                    if pred_tx is not None and pred_tx.type == TransformationType.ROUTER:
                        src_df = self._find_router_group_df(name, pred_name, pred_tx,
                                                            mapping.connectors, df_out)
                    renames = self._incoming_renames(name, pred_name, mapping, tx_map)
                    if not renames:
                        continue
                    renamed_var = f"df_in_{_safe_name(name)}"
                    if len(all_preds) > 1:
                        renamed_var += f"_{_safe_name(pred_name)}"
                    expr = f"_rename_cols({src_df}, {renames!r})"
                    rename_cell_lines.append(
                        f"# {name}: rename {pred_name}'s columns to this transformation's port names"
                    )
                    rename_cell_lines.append(f"{renamed_var} = {expr}")
                    if input_df == src_df:
                        input_df = renamed_var
                    for k, v in list(extra_inputs.items()):
                        # "__" keys are sentinels, not DataFrames -- except
                        # __unordered_second__, which IS one. Skipping it
                        # left the Joiner reading the RAW predecessor while
                        # the renamed copy sat unused, so the join ran
                        # against pre-rename column names.
                        if v == src_df and (
                            not k.startswith("__") or k == "__unordered_second__"
                        ):
                            extra_inputs[k] = renamed_var
            if rename_cell_lines:
                cells.append("\n".join(rename_cell_lines))

            # ALWAYS re-generate code with correct df names
            code_lines = converter.convert(tx, input_df=input_df,
                                           output_df=output_df,
                                           extra_inputs=extra_inputs)
            code = "\n".join(code_lines)

            if tx.type == TransformationType.SEQUENCE_GENERATOR:
                seq_lines = converter.convert(tx, input_df="__SEQ_IN__", output_df="__SEQ_IN__",
                                              extra_inputs=extra_inputs)
                assign = [ln for ln in seq_lines if ".assign(" in ln]
                init = [ln for ln in seq_lines if ".assign(" not in ln]
                seq_parts[name] = [init, assign[-1] if assign else "", False]
                continue

            # ── Producer-side renames, kept ONLY for connectors into a Union
            # (the Union converter relies on its inputs already carrying its
            # port names). Everything else is renamed consumer-side above.
            renames = [
                (f, t) for f, t in self._get_connector_renames(name, mapping)
                if any(c.from_instance == name and c.from_field == f and c.to_field == t
                       and tx_map.get(c.to_instance) is not None
                       and tx_map[c.to_instance].type == TransformationType.UNION
                       # A Union whose ports carry GROUPs selects each input
                       # group's columns by the connectors' FROMFIELD itself
                       # (_union_branches); renaming them here as well made
                       # the Union look for TXN_ID in a DataFrame that now
                       # called it TXN_ID1.
                       and not self._union_has_groups(tx_map[c.to_instance])
                       for c in mapping.connectors)
            ]
            if renames and tx.type != TransformationType.ROUTER:
                rename_lines = ["# Column renames from CONNECTOR mappings (Union inputs)"]
                rename_lines.append(f"{output_df} = _rename_cols({output_df}, {renames!r})")
                code += "\n" + "\n".join(rename_lines)

            cell = f"# {tx.type.value}: {name}\n{code}"
            cells.append(cell)

        # Kept for the write cells: which DataFrame each instance produced.
        self._df_out = dict(df_out)
        self._tx_map = dict(tx_map)

        # A Sequence Generator wired straight into a target: onto the
        # DataFrame that feeds that target.
        for seq_name in sorted(seq_parts):
            for tgt in sorted(fwd.get(seq_name, set()) & tgt_names):
                cells.append(self._seq_consumer_cell(
                    seq_name, tgt, df_out.get(seq_name, "df"), seq_parts, mapping))

        # ── Phase 3: Post-processing ──
        # SCD2 branch merge
        has_router = any(tx.type == TransformationType.ROUTER
                         for tx in mapping.transformations)
        has_scd2 = has_router and self._has_scd2_pattern(mapping)
        if has_scd2:
            cells.append(self._scd2_merge_branches_cell(mapping))

        # The variable the last real transformation in topological order
        # actually wrote to -- the only safe name to keep operating on.
        last_written_df = "df_source"
        for name in reversed(topo):
            tx = tx_map.get(name)
            if tx:
                last_written_df = df_out.get(name, last_written_df)
                break

        # Missing target columns — only add if no Router (otherwise they were
        # added before the Router in Phase 2). Added to the DataFrame the
        # pipeline is actually on: a hard-coded `df` here raised NameError
        # for the most common shape (SQ -> Expression -> Target stays on
        # df_source) whenever the target had a LOAD_DATE-style column.
        if not has_router:
            missing = self._missing_target_columns_cell(mapping, last_written_df)
            if missing:
                cells.append(missing)

        # Assign df_final. Three cases, in order of precedence -- none of
        # them may assume a hardcoded variable name without checking what
        # was actually written, because Phase 1/2 don't guarantee any
        # particular transformation ends up writing to a variable literally
        # named "df" (see the NameError bug this replaced: df_final = df
        # where nothing in the notebook had ever assigned `df`).
        if has_scd2:
            # The SCD2 merge cell just above explicitly assigned `df`
            # (the unioned new+changed branches) -- that IS correct here.
            cells.append("# Assign final DataFrame\ndf_final = df")
        elif has_router:
            # Router fans out into per-group variables (df_<group>) -- it
            # never writes to a variable of its own. Resolve the DataFrame
            # that actually feeds the (first) target through the connectors
            # (Router port GROUP -> group DataFrame); fall back to the first
            # group, treated as the primary/valid path, when the target's
            # feed cannot be traced. Without this, df_final would silently
            # fall back to whatever fed the Router -- the unfiltered set.
            valid_df = ""
            if mapping.targets:
                valid_df = self._df_feeding(mapping.targets[0].name, mapping, tx_map, df_out) or ""
            if not valid_df:
                valid_df = "df"
                for tx in mapping.transformations:
                    if tx.type == TransformationType.ROUTER and tx.router_groups:
                        first_group = tx.router_groups[0].get("name", "")
                        valid_df = f"df_{_safe_name(first_group)}"
                        break
            cells.append(
                f"# Assign final DataFrame (the Router group that feeds the target -- "
                f"see per-target write cells for the others)\ndf_final = {valid_df}"
            )
        elif cells:
            # The DataFrame that feeds the target, traced through the
            # connectors; else the variable the last transformation wrote.
            feeding = ""
            if len(mapping.targets) == 1:
                feeding = self._df_feeding(mapping.targets[0].name, mapping, tx_map, df_out) or ""
            cells.append(f"# Assign final DataFrame\ndf_final = {feeding or last_written_df}")
        else:
            cells.append("# No transformations\ndf_final = df_source")

        # The target's own port names: rename the final DataFrame's columns
        # to what the target definition calls them (connectors into the
        # target instance), so the target-column select below can find them.
        if mapping.targets and len(mapping.targets) == 1 and not has_scd2:
            target_renames = self._target_renames(mapping.targets[0].name, mapping, tx_map)
            if target_renames:
                cells.append(f"# Rename to the target's column names\n"
                             f"df_final = _rename_cols(df_final, {target_renames!r})")

        return cells

    # ------------------------------------------------------------------
    # Connector-based wiring helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _pipeline_column(producer_tx, port: str) -> str:
        """The column a producer's output port is carried as: a connected
        Lookup's ports are qualified (see TransformationConverter.
        _lookup_plan) until a connector renames them."""
        if (producer_tx is not None and producer_tx.type == TransformationType.LOOKUP
                and getattr(producer_tx, "_port_exact", False)):
            from ..converters.transformation_converter import lookup_prefix
            return f"{lookup_prefix(producer_tx)}{port}"
        return port

    @staticmethod
    def _seq_consumer_cell(seq_name: str, consumer: str, var: str, seq_parts: dict,
                           mapping: Mapping) -> str:
        init, assign, emitted = seq_parts[seq_name]
        lines = [f"# Sequence Generator: {seq_name} -> {consumer}"]
        if not emitted:
            lines.extend(ln for ln in init if not ln.startswith("# Sequence Generator"))
            seq_parts[seq_name][2] = True
        if assign:
            lines.append(assign.replace("__SEQ_IN__", var))
        for c in mapping.connectors:
            if (c.from_instance == seq_name and c.to_instance == consumer
                    and c.from_field and c.to_field and c.to_field != c.from_field):
                lines.append(f'{var} = {var}.withColumnRenamed("{c.from_field}", "{c.to_field}")')
                break
        return "\n".join(lines)

    @staticmethod
    def _loaded_earlier(mapping, lookup: str, table: str, fwd, tgt_names) -> bool:
        """True when ``table`` (a target of this mapping) is loaded in an
        EARLIER target load order group than every target this lookup
        feeds. Then the lookup must read it after that group is written --
        not snapshot it before any write, which is right only when the
        lookup's own targets are the ones being written (SCD2 on itself)."""
        order = getattr(mapping, "target_load_order", None) or {}
        if not order:
            return False
        fed, seen, stack = set(), set(), [lookup]
        while stack:
            n = stack.pop()
            for s in fwd.get(n, set()):
                if s in seen:
                    continue
                seen.add(s)
                (fed.add(s) if s in tgt_names else stack.append(s))
        writers = [t.name for t in mapping.targets
                   if (t.warehouse_table or t.table_name or t.name).split(".")[-1].upper() == table]
        if not fed or not writers:
            return False
        return max(order.get(w, 1) for w in writers) < min(order.get(f, 1) for f in fed)

    @staticmethod
    def _union_has_groups(tx) -> bool:
        return any(isinstance(f, TransformationField) and f.direction == DataFlowDirection.INPUT
                   and f.group for f in tx.fields)

    def _union_branches(self, tx, name, mapping, tx_map, df_out):
        """``[(input DataFrame, [(upstream column or None, output port)])]``,
        one per Union input group in export order.

        A Union's input groups each carry the output ports in the same
        order (ORDER_ID1, CUST_ID1, AMT1 | ORDER_ID2, ...), and the
        connectors say which upstream column feeds each group port. Output
        port i takes group port i. An unconnected group port is NULL. None
        when the export does not carry port GROUPs.
        """
        fields = [f for f in tx.fields if isinstance(f, TransformationField)]
        outputs = [f.name for f in fields if f.direction == DataFlowDirection.OUTPUT]
        groups: list[str] = []
        for f in fields:
            if f.direction == DataFlowDirection.INPUT and f.group and f.group not in groups:
                groups.append(f.group)
        if not groups or not outputs:
            return None
        feeds = {c.to_field: c for c in mapping.connectors if c.to_instance == name}
        branches = []
        for g in groups:
            gports = [f.name for f in fields if f.direction == DataFlowDirection.INPUT and f.group == g]
            conns = [feeds.get(p) for p in gports]
            preds = {c.from_instance for c in conns if c is not None}
            if not preds:
                continue          # a group nothing is connected to adds no rows
            if len(preds) > 1:
                return None       # one group fed by two instances: not row-aligned
            pred = preds.pop()
            ptx = tx_map.get(pred)
            if ptx is not None and ptx.type == TransformationType.ROUTER and ptx.router_groups:
                pred_df = self._find_router_group_df(name, pred, ptx, mapping.connectors, df_out)
            else:
                pred_df = df_out.get(pred, "df")
            cols = []
            for i, out_port in enumerate(outputs):
                c = conns[i] if i < len(conns) else None
                from_col = None
                if c is not None:
                    from_col = self._pipeline_column(ptx, c.from_field)
                    if ptx is not None and ptx.type == TransformationType.ROUTER:
                        from_col = self._router_column_of_port(ptx, c.from_field)
                cols.append((from_col, out_port))
            branches.append((pred_df, cols))
        return branches or None

    def _assign_df_names(self, mapping, topo, tx_map, back, fwd, df_out,
                         sq_list, src_def_names, tgt_names) -> None:
        """Fill ``df_out`` (the variable each instance's output lives in)
        and ``self._input_var`` / ``self._merged_preds`` for Phase 2.

        Readers are counted from the connectors: a producer's output is
        needed by every instance a connector leads to, and a target reads
        at the very end. A variable is reused only when its holder has no
        other reader. A transformation fed by several row-aligned
        instances (an Expression fed by a Source Qualifier and a Lookup on
        it) reads the one whose lineage already carries the others.
        """
        from collections import defaultdict

        self._input_var: dict[str, str] = {}
        self._merged_preds: dict[str, list[str]] = {}
        # Router group -> its consumers, via the GROUP of the ports each
        # connector leaves from.
        readers: dict[str, set] = defaultdict(set)
        for c in mapping.connectors:
            src = c.from_instance
            rtx = tx_map.get(src)
            if rtx is not None and rtx.type == TransformationType.ROUTER and rtx.router_groups:
                group_df = self._find_router_group_df(c.to_instance, src, rtx, mapping.connectors, df_out)
                readers[f"RTR_GROUP_VAR:{group_df}"].add(c.to_instance)
            readers[src].add(c.to_instance)

        # var -> instances still to read its CURRENT value (a target reads
        # at the very end), and var -> instances whose columns it carries.
        var_pending: dict[str, set] = {}
        var_lineage: dict[str, set] = {}
        processed: set = set()
        lineage: dict[str, set] = {}
        for sq in sq_list:
            lineage[sq] = {sq} | {s for s in back.get(sq, set()) if s in src_def_names}
            if sq in df_out:
                var_pending[df_out[sq]] = set(readers.get(sq, set()))
                var_lineage[df_out[sq]] = set(lineage[sq])

        def reader_key(producer: str, var: str) -> str:
            rtx = tx_map.get(producer)
            if rtx is not None and rtx.type == TransformationType.ROUTER and rtx.router_groups:
                return f"RTR_GROUP_VAR:{var}"
            return producer

        def pending(var: str, reading: str) -> set:
            return {r for r in var_pending.get(var, set()) if r not in processed and r != reading}

        def is_live(var: str, reading: str) -> bool:
            return bool(pending(var, reading))

        def write(name: str, out: str, in_var: Optional[str], base_lineage: set) -> None:
            if out == in_var:
                var_pending[out] = (var_pending.get(out, set()) - {name}) | set(readers.get(name, set()))
            else:
                var_pending[out] = set(readers.get(name, set()))
            lineage[name] = set(base_lineage) | {name}
            var_lineage[out] = set(lineage[name])
            df_out[name] = out
            used.add(out)

        used = set(df_out.values())

        def fresh(name: str) -> str:
            base = f"df_{_safe_name(name)}"
            var, i = base, 1
            while var in used:
                i += 1
                var = f"{base}_{i}"
            used.add(var)
            return var

        def var_of(pred: str, consumer: str) -> str:
            ptx = tx_map.get(pred)
            if ptx is not None and ptx.type == TransformationType.ROUTER and ptx.router_groups:
                return self._find_router_group_df(consumer, pred, ptx, mapping.connectors, df_out)
            return df_out.get(pred, "df")

        def preds_of(name: str) -> list[str]:
            out = []
            for pred in sorted(back.get(name, set())):
                if pred in src_def_names:
                    sq = self._find_sq_predecessor(name, back, fwd, sq_list, src_def_names)
                    if sq:
                        pred = sq
                    else:
                        continue
                if pred in df_out or pred in tx_map:
                    if pred not in out:
                        out.append(pred)
            return out

        seq_preds_of: dict[str, list[str]] = defaultdict(list)
        for name in topo:
            tx = tx_map.get(name)
            if not tx:
                continue
            if tx.type == TransformationType.SEQUENCE_GENERATOR:
                for succ in fwd.get(name, set()):
                    seq_preds_of[succ].append(name)
                df_out[name] = "df"  # re-pointed at its consumer's input below
                processed.add(name)
                continue

            preds = [p for p in preds_of(name)
                     if not (tx_map.get(p) and tx_map[p].type == TransformationType.SEQUENCE_GENERATOR)]

            if tx.type == TransformationType.ROUTER:
                # preds here excludes Sequence Generators, so a Router fed
                # only by one would fall through to a bare "df".
                if preds:
                    in_var = var_of(preds[0], name)
                else:
                    seq_up = seq_preds_of.get(name) or []
                    in_var = (var_of(seq_up[0], name) if seq_up else "df_source")
                self._input_var[name] = in_var
                df_out[name] = in_var
                base = var_lineage.get(in_var, lineage.get(preds[0], {preds[0]})) if preds else set()
                lineage[name] = set(base) | {name}
                if tx.router_groups:
                    for g in tx.router_groups:
                        gname = g.get("name", "")
                        gvar = f"df_{_safe_name(gname)}"
                        df_out[f"RTR_GROUP_{name}_{gname}"] = gvar
                        var_pending[gvar] = set(readers.get(f"RTR_GROUP_VAR:{gvar}", set()))
                        var_lineage[gvar] = set(lineage[name])
                        used.add(gvar)
                # Re-point any Sequence Generator feeding this Router, the
                # same way the main path does below. A Sequence Generator
                # records df_out = "df" to be re-pointed at its consumer's
                # input, and this branch returned before doing it -- so the
                # sequence kept the bare name "df" and every consumer of it,
                # including the Router's own per-consumer rename copy, read a
                # variable nothing assigns.
                for _s in seq_preds_of.get(name, []):
                    df_out[_s] = in_var
                processed.add(name)
                continue

            multi_input = tx.type in (TransformationType.JOINER, TransformationType.UNION)
            if multi_input or not preds:
                out = "df" if not is_live("df", name) else fresh(name)
                if not preds:
                    self._input_var[name] = "df_source"
                base = set().union(*(var_lineage.get(var_of(p, name), lineage.get(p, {p})) for p in preds)) \
                    if preds else set()
                write(name, out, None, base)
                processed.add(name)
                continue

            primary = preds[0]
            if len(preds) > 1:
                covering = [p for p in preds
                            if all(o == p or o in lineage.get(p, {p}) for o in preds)]
                if covering:
                    primary = covering[0]
                    self._merged_preds[name] = preds
                else:
                    self._merged_preds[name] = [primary]
            in_var = var_of(primary, name)
            self._input_var[name] = in_var

            ptx = tx_map.get(primary)
            primary_is_sq = ptx is not None and ptx.type == TransformationType.SOURCE_QUALIFIER
            others = pending(in_var, name)
            side_branch = tx.type == TransformationType.AGGREGATOR and any(
                tx_map.get(s) is not None and tx_map[s].type == TransformationType.LOOKUP
                for s in fwd.get(name, set())
            )
            # A port-exact Lookup only ADDS qualified columns and keeps every
            # row, so it can run in place even when others read the same
            # DataFrame -- which is what lets several Lookups on one Source
            # Qualifier, all feeding one target, meet in one DataFrame.
            column_adding = tx.type == TransformationType.LOOKUP and getattr(tx, "_port_exact", False)
            if side_branch:
                out = fresh(name)
            elif column_adding:
                out = in_var
            elif not others and not primary_is_sq:
                out = in_var                      # sole reader: continue in place
            elif not others and primary_is_sq and not is_live("df", name):
                out = "df"                        # the main chain off a source
            else:
                out = fresh(name)
            base = var_lineage.get(in_var, lineage.get(primary, {primary}))
            write(name, out, in_var, base)
            for s in seq_preds_of.get(name, []):
                df_out[s] = in_var
            processed.add(name)
        self._lineage = lineage

        # A Sequence Generator wired straight into a target adds its column
        # to whatever else feeds that target.
        for name in topo:
            tx = tx_map.get(name)
            if not tx or tx.type != TransformationType.SEQUENCE_GENERATOR:
                continue
            if any(s in tx_map for s in fwd.get(name, set())):
                continue
            for tgt in sorted(fwd.get(name, set()) & tgt_names):
                others = [p for p in preds_of(tgt) if p != name
                          and not (tx_map.get(p) and tx_map[p].type == TransformationType.SEQUENCE_GENERATOR)]
                if others:
                    df_out[name] = var_of(others[0], tgt)
                    break

    @staticmethod
    def _router_group_of_port(router_tx: Transformation, port: str) -> str:
        """The output group a Router port belongs to ("" if the export
        carries no GROUP attribute on its ports)."""
        for f in router_tx.fields:
            if isinstance(f, TransformationField) and f.name == port and f.group:
                if f.group.upper() != "INPUT":
                    return f.group
        return ""

    @staticmethod
    def _router_column_of_port(router_tx: Transformation, port: str) -> str:
        """The column a Router OUTPUT port carries in the group DataFrame:
        its REF_FIELD (the input port it copies), else the port itself."""
        for f in router_tx.fields:
            if isinstance(f, TransformationField) and f.name == port:
                return f.ref_field or port
        return port

    def _incoming_renames(
        self, consumer: str, pred_name: str, mapping: Mapping, tx_map: dict
    ) -> list[tuple[str, str]]:
        """``(upstream column, consumer port)`` pairs for the connectors
        ``pred_name -> consumer`` whose names differ, Router REF_FIELD-aware
        and de-duplicated."""
        pred_tx = tx_map.get(pred_name)
        renames: list[tuple[str, str]] = []
        seen: set = set()
        for c in mapping.connectors:
            if c.from_instance != pred_name or c.to_instance != consumer:
                continue
            from_col = self._pipeline_column(pred_tx, c.from_field)
            if pred_tx is not None and pred_tx.type == TransformationType.ROUTER:
                from_col = self._router_column_of_port(pred_tx, c.from_field)
            if not from_col or not c.to_field or from_col == c.to_field:
                continue
            if (from_col, c.to_field) in seen:
                continue
            seen.add((from_col, c.to_field))
            renames.append((from_col, c.to_field))
        return renames

    def _target_renames(self, target_name: str, mapping: Mapping, tx_map: dict) -> list[tuple[str, str]]:
        preds = {c.from_instance for c in mapping.connectors if c.to_instance == target_name}
        renames: list[tuple[str, str]] = []
        for pred in sorted(preds):
            renames.extend(self._incoming_renames(target_name, pred, mapping, tx_map))
        return renames

    def _df_feeding(self, instance: str, mapping: Mapping, tx_map: dict, df_out: dict) -> Optional[str]:
        """The DataFrame variable that feeds ``instance`` (a target or a
        transformation), traced through the connectors: a Router
        predecessor resolves to the group DataFrame its ports belong to;
        any other predecessor to the DataFrame it produced."""
        preds = sorted({c.from_instance for c in mapping.connectors if c.to_instance == instance})
        # Row-aligned inputs (a target fed by a Source Qualifier AND an
        # Expression on it): the one whose lineage carries the others.
        lineage = getattr(self, "_lineage", {}) or {}
        covering = [p for p in preds if all(o == p or o in lineage.get(p, {p}) for o in preds)]
        if covering:
            preds = covering + [p for p in preds if p not in covering]
        for pred in preds:
            pred_tx = tx_map.get(pred)
            if pred_tx is not None and pred_tx.type == TransformationType.ROUTER:
                return self._find_router_group_df(instance, pred, pred_tx, mapping.connectors, df_out)
            if pred in df_out:
                return df_out[pred]
        return None

    @staticmethod
    def _find_router_group_df(
        downstream_name: str,
        router_name: str,
        router_tx: Transformation,
        connectors: list,
        df_out: dict,
    ) -> str:
        """Determine which Router group's DataFrame feeds a downstream transform.

        Uses CONNECTOR field names to match: Router conditions create split DFs
        named df_{group_name}. The connectors from Router → downstream tell us
        which group's output feeds this transform.

        For multi-target patterns:
        - RTR → UPD_INSERT_VALID → Target1 (valid group)
        - RTR → EXP_REJECT_REASON → Target2 (reject group)
        """
        if not router_tx.router_groups:
            return "df"

        # Authoritative: the GROUP attribute on the Router ports the
        # connectors leave from. PowerCenter names every output port's
        # group (and the input port it copies via REF_FIELD), so the
        # downstream instance's feeding group is in the export -- the name
        # and position heuristics below only apply to fixtures without it.
        port_groups = {
            NotebookGenerator._router_group_of_port(router_tx, c.from_field)
            for c in connectors
            if c.from_instance == router_name and c.to_instance == downstream_name
        }
        port_groups.discard("")
        if len(port_groups) == 1:
            return f"df_{_safe_name(port_groups.pop())}"

        # Check if this downstream is mentioned in the Router's TABLEATTRIBUTE groups
        # by looking at which connectors flow from Router to this downstream
        # and matching the field set against the group conditions
        groups = router_tx.router_groups

        # Simple heuristic: if there are exactly 2 groups and 2 downstream paths,
        # the first group (valid) goes to the first downstream, second (reject) to second
        downstream_nodes = set()
        for conn in connectors:
            if conn.from_instance == router_name:
                downstream_nodes.add(conn.to_instance)

        downstream_list = sorted(downstream_nodes)

        # Try to match by name patterns
        downstream_lower = downstream_name.lower()
        for g in groups:
            gname = g.get("name", "").lower()
            gname_safe = _safe_name(g.get("name", ""))
            # Check if downstream name suggests valid/reject/error path
            if ("reject" in gname and "reject" in downstream_lower) or \
               ("error" in gname and "error" in downstream_lower) or \
               ("invalid" in gname and ("reject" in downstream_lower or "error" in downstream_lower)):
                return f"df_{gname_safe}"
            if ("valid" in gname and "valid" in downstream_lower) or \
               ("valid" in gname and "insert" in downstream_lower and "reject" not in downstream_lower):
                return f"df_{gname_safe}"

        # Fallback: match by position in downstream list
        if len(groups) >= 2 and len(downstream_list) >= 2:
            idx = downstream_list.index(downstream_name) if downstream_name in downstream_list else 0
            if idx < len(groups):
                gname_safe = _safe_name(groups[idx].get("name", ""))
                return f"df_{gname_safe}"

        # Last resort: use first group
        return f"df_{_safe_name(groups[0].get('name', ''))}"

    def _find_sq_predecessor(self, name: str, back, fwd, sq_list, src_defs) -> Optional[str]:
        """Find the Source Qualifier that feeds a transformation (possibly via source def)."""
        for pred in back.get(name, set()):
            if pred in sq_list:
                return pred
            if pred in src_defs:
                # Source def → find which SQ it feeds
                for sq in sq_list:
                    if pred in back.get(sq, set()) or sq in fwd.get(pred, set()):
                        return sq
        return None

    @staticmethod
    def _resolve_joiner_sides(tx: Transformation, all_preds: list, mapping: Mapping):
        """Resolve which predecessor of a Joiner is the master side and
        which is the detail side.

        Two independent signals are tried, in order (spec
        Sec 28 item 8; mirrors the Rust reference implementation, src/recognize.rs's
        ``joiner_detail_upstream``):

        1. The transformation-level "Master Source" property
           (spelling-tolerant), matched against each predecessor's
           instance name.
        2. The port-level MASTER/ISMASTER (PowerCenter) / master, isMaster,
           portGroup=="master" (IICS) flag on the Joiner's OWN input
           fields, cross-referenced against the mapping's CONNECTORs to
           classify each predecessor as feeding only master ports (->
           master) or at least one non-master port (-> detail).

        Returns ``(master_pred_name, detail_pred_name)``, or ``(None,
        None)`` if neither signal cleanly identifies exactly one master
        and at least one detail. Never infers master/detail from
        connector/predecessor ORDER -- that is exactly the defect this
        resolution replaces (a Master Outer Join silently inverts
        whenever the detail source happens to be wired first).
        """
        pred_names = [p for p, _ in all_preds]

        # Signal 1: the "Master Source" property, matched by substring
        # against a predecessor's instance name.
        master_source = get_ci(tx.properties, "Master Source", "master_source")
        if master_source:
            matches = [p for p in pred_names if master_source in p]
            if len(matches) == 1:
                master = matches[0]
                detail = next((p for p in pred_names if p != master), None)
                if detail is not None:
                    return master, detail

        # Signal 2: the port-level MASTER/ISMASTER flag on this Joiner's
        # own fields, cross-referenced against CONNECTORs feeding it.
        master_fields = {
            f.name.upper()
            for f in tx.fields
            if isinstance(f, TransformationField) and f.is_master
        }
        if master_fields:
            incoming = [c for c in mapping.connectors if c.to_instance == tx.name]

            def classify(pred_name: str) -> Optional[bool]:
                conns = [c for c in incoming if c.from_instance == pred_name]
                if not conns:
                    return None
                return all(c.to_field.upper() in master_fields for c in conns)

            masters = [p for p in pred_names if classify(p) is True]
            details = [p for p in pred_names if classify(p) is False]
            if len(masters) == 1 and details:
                return masters[0], details[0]

        return None, None

    def _resolve_input_df(self, name, tx, back, fwd, df_out, sq_list, src_defs) -> str:
        """Determine the input DataFrame variable for a transformation.

        Critical: for transforms downstream of a Router, input_df must be the
        Router group's split df (e.g., df_valid_claims), NOT the pre-router df.
        """
        if tx.type == TransformationType.SEQUENCE_GENERATOR:
            # A Sequence Generator assigns NEXTVAL in place, so its input is
            # its output. The default used to be the bare name "df", which
            # nothing assigns when the upstream chain wrote into df_source:
            # the sequence emitted `df_source = _seq.assign(df_source, ...)`
            # while a downstream consumer's rename copy read `df`, and the
            # notebook died with NameError. Found during breadth testing.
            resolved = df_out.get(name)
            if resolved:
                return resolved
            preds = self._get_resolved_predecessors(
                name, back, df_out, sq_list, src_defs, fwd)
            return preds[0][1] if preds else "df_source"

        preds = self._get_resolved_predecessors(name, back, df_out, sq_list, src_defs, fwd)
        if not preds:
            return "df_source"

        # For Joiner: detail is the first non-master source. This is only a
        # fallback for the degenerate case of fewer than two resolved
        # predecessors -- the normal (>=2 predecessor) case is resolved by
        # `_resolve_joiner_sides` in `_transformation_cells` before this
        # method is ever called, using the port-level MASTER/ISMASTER flag
        # as well as this property.
        if tx.type == TransformationType.JOINER:
            master = get_ci(tx.properties, "Master Source", "master_source")
            for pred_name, pred_df in preds:
                if master and master not in pred_name:
                    return pred_df

        # If this transform's assigned output df is a Router group df, use
        # that same df as input -- operate in place on the split df.
        #
        # The guard is that the variable must actually BE one of this
        # transformation's resolved predecessors. The condition used to be
        # `startswith("df_")`, which matches every df_<name> variable and
        # not just a Router group, so a transformation whose output happened
        # to be named df_new read df_new as its input -- a name nothing had
        # assigned. The notebook then opened with
        #
        #     df_new = df_new.withColumn("CURRENT_FLAG", F.lit("Y"))
        #
        # and died with a NameError before touching any data. Found during
        # breadth testing; the Router case it was written for is already
        # handled by _find_router_group_df in _get_resolved_predecessors.
        assigned_out = df_out.get(name, "df")
        pred_dfs = {pdf for _, pdf in preds}
        if (assigned_out.startswith("df_") and assigned_out != "df"
                and assigned_out in pred_dfs):
            return assigned_out

        return preds[0][1]

    def _get_resolved_predecessors(self, name, back, df_out, sq_list, src_defs, fwd) -> list:
        """Get list of (pred_name, pred_df) for a transformation's inputs."""
        result = []
        for pred in sorted(back.get(name, set())):
            if pred in df_out:
                result.append((pred, df_out[pred]))
            elif pred in src_defs:
                sq = self._find_sq_predecessor(name, back, fwd, sq_list, src_defs)
                if sq and sq in df_out:
                    result.append((sq, df_out[sq]))
        return result

    @staticmethod
    def _get_connector_renames(instance_name: str, mapping: Mapping) -> list[tuple[str, str]]:
        """Find column renames needed based on CONNECTOR FROMFIELD → TOFIELD.

        When a connector maps FROMFIELD=X to TOFIELD=Y where X != Y,
        a .withColumnRenamed(X, Y) is needed.

        Skips:
        - Same-name pass-throughs
        - Joiner port suffixes (_M, _D, _1, _2)
        - LKP_ prefixed (handled by lookup converter)
        - O_ prefixed going to Union INPUT ports (handled by Union converter)
        - OUT_ prefixed Joiner output ports (not actual DataFrame columns)
        - Renames where the source column starts with prefixes that are
          Informatica internal port names, not actual DataFrame columns
        """
        import re

        # Identify the transformation type for this instance
        tx_type = None
        for tx in mapping.transformations:
            if tx.name == instance_name:
                tx_type = tx.type
                break

        # Identify downstream instance types to skip Union INPUT port renames
        downstream_types = {}
        for tx in mapping.transformations:
            downstream_types[tx.name] = tx.type

        renames = []
        seen = set()
        for conn in mapping.connectors:
            if conn.from_instance != instance_name:
                continue
            from_f = conn.from_field
            to_f = conn.to_field
            if from_f == to_f:
                continue

            # Skip Joiner port suffix patterns
            base_from = re.sub(r'_[MDmd]$|_[12]$|_IN$|_OUT$', '', from_f)
            base_to = re.sub(r'_[MDmd]$|_[12]$|_IN$|_OUT$', '', to_f)
            if base_from == base_to:
                continue

            # A connector INTO a Lookup's input port is a join-key alias, not
            # a rename of the pipeline column: the Lookup cell resolves the
            # port to the upstream column itself (see __port_map__). Renaming
            # the pipeline column here would break every other consumer of
            # the same upstream column.
            if downstream_types.get(conn.to_instance) == TransformationType.LOOKUP:
                continue

            # LKP_ → non-LKP_ is a real rename needed for downstream references
            # (e.g., LKP_EXCHANGE_RATE → EXCHANGE_RATE, LKP_ADJUSTER_NAME → ADJUSTER_NAME)
            if from_f.startswith("LKP_") and not to_f.startswith("LKP_"):
                # This IS a rename we need — don't skip
                pass
            elif from_f.startswith("LKP_") or to_f.startswith("LKP_"):
                continue

            # Skip O_ prefixed renames — handled by Union converter's select+alias
            # Both: O_ going TO a Union (input), and O_ coming FROM a Union (output)
            if from_f.startswith("O_"):
                continue

            # Skip OUT_ prefixed (Joiner output port names — not actual DF columns)
            if from_f.startswith("OUT_"):
                continue

            # Skip NEXTVAL (handled by Sequence Generator logic)
            if from_f == "NEXTVAL":
                continue

            # Deduplicate
            key = (from_f, to_f)
            if key in seen:
                continue
            seen.add(key)
            renames.append((from_f, to_f))
        return renames

    @staticmethod
    def _find_seq_target_col(seq_name: str, mapping: Mapping, fwd) -> Optional[str]:
        """Find what column name the Sequence Generator's NEXTVAL maps to in the target."""
        # Look at connectors from this seq gen to find the target field name
        for conn in mapping.connectors:
            if conn.from_instance == seq_name and conn.from_field == "NEXTVAL":
                return conn.to_field
        # Check if any target has a _SK column
        for target in mapping.targets:
            for f in target.fields:
                fname = f.target_field if isinstance(f, FieldMapping) else (f.get("target_field") or f.get("name", ""))
                if fname.upper().endswith(("_SK", "_WID", "_SID")):
                    return fname
        return None

    @staticmethod
    def _order_underdetermined_review_item(
        mapping: Mapping,
        process_set: set,
    ) -> Optional[str]:
        """Flag when transformation order cannot be
        reliably derived from CONNECTOR edges.

        Two cases, both underdetermined:
        - No CONNECTOR edges exist between any two transformations in
          `process_set` at all -- the caller falls back to document
          order (or an alphabetically-tie-broken topo sort with every
          node at in-degree 0), and that order is unverified.
        - CONNECTOR edges exist but touch only *some* of the
          transformations in `process_set` -- a real DAG walk still
          happens, but the transformations with no edge of their own can
          land anywhere the tie-break puts them. This partial case is
          the nastier one: an order derived from a partial DAG can be
          confidently wrong, unlike an obviously-absent one.

        Returns a `# REVIEW REQUIRED: ...` comment string (the same
        marker mechanism used for an unresolvable Joiner
        master/detail side) naming the mapping and the affected
        transformations, or None if the order is fully determined.

        A single transformation (or none) has no ordering to get wrong,
        so it is never flagged.
        """
        if len(process_set) < 2:
            return None

        tx_edges = [
            (c.from_instance, c.to_instance)
            for c in mapping.connectors
            if c.from_instance in process_set and c.to_instance in process_set
        ]

        if not tx_edges:
            return (
                f"# REVIEW REQUIRED: transformation order for mapping "
                f"'{mapping.name}' could not be derived from CONNECTOR "
                f"edges -- none were found among {sorted(process_set)}. "
                f"The order below falls back to document/name order and "
                f"is UNVERIFIED -- check it against the source mapping "
                f"before relying on this notebook."
            )

        covered = {n for edge in tx_edges for n in edge}
        missing = process_set - covered
        if missing:
            return (
                f"# REVIEW REQUIRED: transformation order for mapping "
                f"'{mapping.name}' is only PARTIALLY derived from "
                f"CONNECTOR edges -- {sorted(missing)} have no connector "
                f"edge to/from another transformation in this mapping, "
                f"so their position in the order below is UNVERIFIED and "
                f"may be confidently wrong. Check it against the source "
                f"mapping before relying on this notebook."
            )

        return None

    def _transformation_cells_linear(
        self,
        mapping: Mapping,
        conversion_result: dict,
    ) -> list[str]:
        """Fallback: linear processing when no connectors available."""
        cells: list[str] = []
        converted = conversion_result.get("transformations", {})
        first_tx = True

        for tx in mapping.transformations:
            if tx.type == TransformationType.SOURCE_QUALIFIER:
                continue
            code = converted.get(tx.name, "")
            if code:
                if first_tx:
                    code = f"df = df_source\n{code}"
                    first_tx = False
                cell = f"# {tx.type.value}: {tx.name}\n{code}"
            else:
                if first_tx:
                    cell = "df = df_source\n" + self._stub_transformation(tx, mapping)
                    first_tx = False
                else:
                    cell = self._stub_transformation(tx, mapping)
            cells.append(cell)

        has_router = any(tx.type == TransformationType.ROUTER for tx in mapping.transformations)
        if has_router and self._has_scd2_pattern(mapping):
            cells.append(self._scd2_merge_branches_cell(mapping))

        missing_cols_cell = self._missing_target_columns_cell(mapping)
        if missing_cols_cell:
            cells.append(missing_cols_cell)

        if cells:
            cells.append("# Assign final DataFrame\ndf_final = df")
        else:
            cells.append("# No transformations — pass-through mapping\ndf_final = df_source")
        return cells

    def _missing_target_columns_cell(self, mapping: Mapping, df_var: str = "df") -> Optional[str]:
        """Generate withColumn calls for target columns with no source expression.

        Common examples: LOAD_DATE, CURRENT_FLAG, EFF_START_DATE defaults.
        ``df_var`` is the DataFrame the pipeline is currently on.
        """
        if not mapping.targets:
            return None
        df = df_var

        target = mapping.targets[0]
        target_cols = set()
        for f in target.fields:
            if isinstance(f, FieldMapping):
                target_cols.add(f.target_field.upper())
            elif isinstance(f, dict):
                target_cols.add((f.get("target_field") or f.get("name", "")).upper())

        # Columns produced by transformations
        produced_cols = set()
        for tx in mapping.transformations:
            for f in tx.fields:
                if hasattr(f, 'name'):
                    produced_cols.add(f.name.upper())
                elif isinstance(f, dict):
                    produced_cols.add((f.get("name", "")).upper())
        # Also add source columns
        for src in mapping.sources:
            for f in src.fields:
                if isinstance(f, FieldMapping):
                    produced_cols.add(f.source_field.upper())
                elif isinstance(f, dict):
                    produced_cols.add((f.get("name", "")).upper())

        missing = target_cols - produced_cols
        if not missing:
            return None

        lines = ["# Add missing target columns with default values"]
        for col in sorted(missing):
            col_upper = col.upper()
            if "LOAD_DATE" in col_upper or "LOAD_DT" in col_upper or "ETL_DATE" in col_upper:
                lines.append(f'{df} = {df}.withColumn("{col}", F.lit(_SESSION_START_TIME))')
            elif "CURRENT_FLAG" in col_upper or "CURRENT_FLG" in col_upper:
                lines.append(f'{df} = {df}.withColumn("{col}", F.lit("Y"))')
            elif "EFF_START" in col_upper or "EFFECTIVE_FROM" in col_upper:
                lines.append(f'{df} = {df}.withColumn("{col}", F.lit(_SESSION_START_TIME))')
            elif "EFF_END" in col_upper or "EFFECTIVE_TO" in col_upper:
                lines.append(f'{df} = {df}.withColumn("{col}", F.lit(None).cast("timestamp"))')
            elif "CREATE_DATE" in col_upper or "CREATED" in col_upper:
                lines.append(f'{df} = {df}.withColumn("{col}", F.lit(_SESSION_START_TIME))')
            elif "UPDATE_DATE" in col_upper or "UPDATED" in col_upper:
                lines.append(f'{df} = {df}.withColumn("{col}", F.lit(_SESSION_START_TIME))')
            # Don't add defaults for unknown columns — they'll get NULL naturally

        return "\n".join(lines) if len(lines) > 1 else None

    @staticmethod
    def _has_update_strategy(mapping: Mapping) -> bool:
        return any(tx.type == TransformationType.UPDATE_STRATEGY for tx in mapping.transformations)

    def _stub_transformation(self, tx: Transformation, mapping: Optional[Mapping] = None) -> str:
        """Generate a placeholder cell for an unconverted transformation."""
        lines = [
            f"# {tx.type.value}: {tx.name}",
            f"# TODO: Review and complete this transformation",
        ]
        if tx.type == TransformationType.FILTER:
            lines.append(
                f'df_final = df_source.filter("{tx.filter_condition}")'
                if tx.filter_condition
                else "# df_final = df_source.filter(<condition>)"
            )
        elif tx.type == TransformationType.AGGREGATOR:
            group_cols = ", ".join(f'"{c}"' for c in tx.group_by_fields) or '"<group_col>"'
            lines.append(f"df_final = df_source.groupBy({group_cols}).agg()")
        elif tx.type == TransformationType.JOINER:
            lines.append(
                f"# Join type: {tx.join_type}\n"
                f"# Condition: {tx.join_condition}\n"
                f"df_final = df_source  # TODO: implement join"
            )
        elif tx.type == TransformationType.LOOKUP:
            lines.append(
                f'# Lookup table: {tx.lookup_table}\n'
                f'# Condition: {tx.lookup_condition}\n'
                f'df_lookup = spark.table("{tx.lookup_table}")\n'
                f"df_final = df_source  # TODO: implement lookup join"
            )
        elif tx.type == TransformationType.EXPRESSION:
            lines.append("df_final = df_source  # TODO: add expression columns")
        elif tx.type == TransformationType.SEQUENCE_GENERATOR:
            # Surrogate key column name comes from the mapping's own target
            # definition (via connectors / _SK-style naming), not a fixed
            # convention -- fall back to a generic placeholder if unknown.
            seq_col = self._find_seq_target_col(tx.name, mapping, None) if mapping else None
            seq_col = seq_col or "SURROGATE_KEY"
            lines.append(
                "df_final = df_source.withColumn(\n"
                f'    "{seq_col}", F.monotonically_increasing_id()\n'
                ")"
            )
        elif tx.type == TransformationType.UPDATE_STRATEGY:
            lines.append(
                f"# Strategy expression: {tx.update_strategy_expression}\n"
                "df_final = df_source  # TODO: implement update strategy"
            )
        elif tx.type == TransformationType.SORTER:
            # A key is either a plain field-name string or a {"field",
            # "direction"} dict (xml_parser.py's per-key SORTKEY parsing,
            #) -- normalize to the field name here. This
            # low-code fallback generator has never rendered direction
            # (only TransformationConverter._sorter does); that stays
            # out of scope for this fix, which is only making the dict
            # shape not crash a ", ".join() that used to assume a plain
            # string.
            names = [
                (k.get("field", k.get("name", "")) if isinstance(k, dict) else str(k))
                for k in tx.sort_keys
            ]
            sort_cols = ", ".join(f'"{c}"' for c in names) or '"<sort_col>"'
            lines.append(f"df_final = df_source.orderBy({sort_cols})")
        elif tx.type == TransformationType.RANK:
            lines.append(
                "df_final = df_source.withColumn(\n"
                '    "rank", F.row_number().over(Window.orderBy("<rank_col>"))\n'
                ")"
            )
        elif tx.type == TransformationType.UNION:
            lines.append("df_final = df_source  # TODO: union with other dataframes")
        elif tx.type == TransformationType.ROUTER:
            lines.append("df_final = df_source  # TODO: split into router groups")
        else:
            lines.append("df_final = df_source  # TODO: implement transformation")

        return "\n".join(lines)

    def _validation_cell(self, mapping: Mapping) -> str:
        if len(mapping.targets) > 1:
            # Each target's write cell selects and writes its own copy; the
            # target columns of targets[0] selected from df_final failed for
            # any other target's columns (and mixed every target's keys).
            names = ", ".join(t.name for t in mapping.targets)
            return (
                f"# {len(mapping.targets)} targets ({names}): each write cell below selects\n"
                "# its target's columns from the DataFrame that feeds it. Nothing is\n"
                "# counted or cached here: that would evaluate a later load order\n"
                "# group (and its lookups) before an earlier group is written.\n"
                "row_count = None"
            )
        key_columns = self._key_columns(mapping)
        target = mapping.targets[0] if mapping.targets else None

        if key_columns:
            key_list = ", ".join(f'"{c}"' for c in key_columns)
        elif target and target.fields:
            # No declared key on the target -- fall back to its own first
            # column rather than assuming a fixed surrogate-key convention.
            first = target.fields[0]
            fname = (
                first.target_field if isinstance(first, FieldMapping)
                else (first.get("target_field") or first.get("name", ""))
            )
            key_list = f'"{fname}"' if fname else '"<key_column>"'
        else:
            key_list = '"<key_column>"'

        lines = []

        # Select target columns FIRST, then cache the slimmer DataFrame
        if target:
            target_cols = [f.target_field for f in target.fields] if target.fields else []
            if target_cols:
                # Build fully qualified target name for comment
                tgt_parts = []
                if target.db_name:
                    tgt_parts.append(target.db_name)
                if target.owner:
                    tgt_parts.append(target.owner)
                tgt_parts.append(target.warehouse_table or target.table_name or target.name)
                tgt_name = ".".join(tgt_parts)
                # Keep the Update Strategy's routing column through the
                # select -- the write cell partitions on it. Dropping it here
                # was what turned every DD_DELETE/DD_REJECT row into an upsert.
                if self._has_update_strategy(mapping) and "DD_STRATEGY" not in target_cols:
                    target_cols = target_cols + ["DD_STRATEGY"]
                col_list = ", ".join(f'"{c}"' for c in target_cols)
                lines.append(f"# Select target columns for {tgt_name}")
                lines.append(f"df_final = df_final.select({col_list})")
                lines.append("")

        lines.extend([
            "# Cache the final DataFrame to avoid multiple full scans",
            "df_final.cache()",
            "row_count = df_final.count()",
            "if row_count == 0:",
            "    # An empty batch is a normal outcome for an incremental load;",
            "    # the Informatica session succeeded with 0 rows and so does this.",
            '    logger.warning("No rows to write for this run -- check source data and filters if that is unexpected")',
            f'logger.info(f"Final row count: {{row_count}}")',
            "",
            f"key_columns = [{key_list}]",
            "null_counts = df_final.select(",
            '    [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in key_columns]',
            ")",
            "null_counts.show()",
        ])

        return "\n".join(lines)

    def _resolve_load_strategy(
        self, target: TargetDefinition, session: Optional[Session]
    ) -> tuple[LoadStrategy, str]:
        """The load strategy for ``target`` and where it came from.

        The SESSION is the export's record of how a target is loaded
        ("Treat source rows as" + the target instance's Insert/Update/
        Delete/Truncate flags); the definition-level default applies only
        when the session says nothing.
        """
        if session is not None:
            resolved = session.load_strategy_for(target.name)
            if resolved is not None:
                return resolved, f"session {session.name} target properties"
        return target.load_strategy, "mapping default (no session-level load options in the export)"

    def _emit_update_strategy_write(
        self, table: str, keys: list[str], df_var: str,
        update_columns: Optional[list[str]] = None,
    ) -> list[str]:
        """Route a DataFrame tagged with DD_STRATEGY through
        infa_compat.apply_update_strategy / write_update_strategy.
        Lines are pre-indented for the caller's ``try:`` block.
        """
        lines: list[str] = []
        if not keys:
            # No key columns to build a MERGE match condition from --
            # never silently downgrade to a full-table overwrite (would
            # delete rows the mapping meant to preserve).
            lines.append(f"    # REVIEW REQUIRED: Update Strategy routing into `{table}` has no")
            lines.append("    # key columns to build a MERGE match condition from -- add key")
            lines.append("    # columns to the mapping's target definition before running this.")
            lines.append(
                f'    raise NotImplementedError('
                f'"REVIEW REQUIRED: no key columns for Update Strategy routing '
                f'into `{table}`")'
            )
            return lines
        reject_table = f"{table}_REJECTS"
        lines.append("    import infa_compat")
        lines.append(
            f'    _dd_result = infa_compat.apply_update_strategy({df_var}, strategy_col="DD_STRATEGY")'
        )
        if self.target_catalog_type == "adw":
            lines.extend(AdwWriteStrategy._setup_lines())
            lines.append("    import oracledb")
            lines.append("    _dd_conn = oracledb.connect(")
            lines.append('        user=os.environ["ADW_USER"],')
            lines.append('        password=os.environ["ADW_PASSWORD"],')
            lines.append('        dsn=os.environ["ADW_TNS_SERVICE"],')
            lines.append("        config_dir=_adw_tns_admin,")
            lines.append("        wallet_location=_adw_tns_admin,")
            lines.append(
                '        wallet_password=os.environ.get("ADW_WALLET_PASSWORD", os.environ["ADW_PASSWORD"]),'
            )
            lines.append("    )")
            lines.append("    try:")
            lines.append(
                f"        infa_compat.write_update_strategy(\n"
                f"            _dd_result, target=\"{table}\", keys={keys!r},\n"
                f'            reject_sink="{reject_table}", spark=spark,\n'
                f'            target_catalog_type="adw", jdbc_options=_adw_opts,\n'
                f'            adw_connection=_dd_conn, staging_table="{table}_UPD_STG",\n'
                f"        )"
            )
            lines.append("    finally:")
            lines.append("        _dd_conn.close()")
        else:
            cols_arg = f"        update_columns={update_columns!r},\n" if update_columns else ""
            lines.append(
                f"    infa_compat.write_update_strategy(\n"
                f"        _dd_result, target=\"{table}\", keys={keys!r},\n"
                f'        reject_sink="{reject_table}", spark=spark,\n'
                f'        target_catalog_type="delta",\n'
                f"{cols_arg}"
                f"    )"
            )
        return lines

    def _target_write_cell(
        self,
        mapping: Mapping,
        conversion_result: dict,
        session: Optional[Session] = None,
    ) -> str:
        custom_write = conversion_result.get("target_write")
        if custom_write:
            return f"# Target write\n{custom_write}"

        if not mapping.targets:
            return "# No target defined — add write logic here"

        target = mapping.targets[0]
        # Build fully qualified target table name
        tgt_parts = []
        if target.db_name:
            tgt_parts.append(target.db_name)
        if target.owner:
            tgt_parts.append(target.owner)
        tgt_parts.append(target.warehouse_table or target.table_name or target.name)
        table = ".".join(tgt_parts)
        strategy, strategy_source = self._resolve_load_strategy(target, session)
        key_columns = self._key_columns(mapping)

        # Target columns already selected in validation cell
        lines = []
        lines.append(f"# Write to target: {table}")
        lines.append(f"# Load strategy: {strategy.value} (from {strategy_source})")
        lines.append("try:")
        if getattr(target, "flat_file", None):
            cols = [f.target_field if isinstance(f, FieldMapping) else f.get("target_field", "")
                    for f in target.fields]
            lines.extend(self._flat_file_write(target, "df_final", session, cols))
        elif self._has_update_strategy(mapping):
            # An Update Strategy transformation decides per row (Informatica
            # "Treat source rows as: Data driven"). The generic keyed MERGE
            # used to run here regardless, upserting DD_DELETE rows and
            # inserting DD_REJECT rows.
            keys = self._update_keys(target, key_columns)
            lines.extend(self._emit_update_strategy_write(
                table, keys, "df_final", self._connected_columns(mapping, target, keys)))
        else:
            lines.extend(self._write_strategy.emit_write(mapping, target, table, strategy, key_columns))

        lines.append(f'    logger.info("Write to {table} complete")')
        lines.append("except Exception as e:")
        lines.append(f'    logger.error(f"Failed to write to {table}: {{e}}")')
        lines.append("    raise")

        return "\n".join(lines)

    def _update_keys(self, target: TargetDefinition, declared: list[str]) -> list[str]:
        """Keys for an Update Strategy write: the target definition's
        primary key, which is what Informatica's UPDATE/DELETE use. The
        natural-key preference below dropped a declared CUST_SK, so an SCD2
        mapping's new version (same CUST_ID, new CUST_SK) matched the old
        row and was never inserted."""
        return list(declared) if declared else self._natural_keys(target, declared)

    @staticmethod
    def _connected_columns(mapping: Mapping, target: TargetDefinition, keys: list[str]) -> list[str]:
        """The target columns a connector feeds, in target order."""
        fed = {c.to_field for c in mapping.connectors if c.to_instance == target.name}
        cols = []
        for f in target.fields:
            name = f.target_field if isinstance(f, FieldMapping) else (f.get("target_field") or f.get("name", ""))
            if name in fed and name not in keys:
                cols.append(name)
        return cols

    @staticmethod
    def _natural_keys(target: TargetDefinition, all_keys: list[str]) -> list[str]:
        """Prefer natural keys over surrogate keys (_SK/_WID/_SID) for a
        MERGE match; fall back to _ID / discriminator columns, then to the
        declared keys."""
        natural_keys = [k for k in all_keys
                        if not k.upper().endswith(("_SK", "_WID", "_SID"))]
        if not natural_keys:
            surrogate = {k.upper() for k in all_keys if k.upper().endswith(("_SK", "_WID", "_SID"))}
            for f in target.fields:
                fname = f.target_field if isinstance(f, FieldMapping) else (f.get("target_field") or f.get("name", ""))
                fupper = fname.upper()
                if fupper in surrogate:
                    continue
                if fupper.endswith("_ID"):
                    natural_keys.append(fname)
                elif fupper.endswith(("_SYSTEM", "_SOURCE")) and fupper not in [k.upper() for k in natural_keys]:
                    natural_keys.insert(0, fname)
        return natural_keys if natural_keys else list(all_keys)

    @staticmethod
    def _scd2_branch_dfs(mapping: Mapping, group_names: list) -> tuple:
        """(new_df, changed_df, reason_it_failed) for an SCD2 Router.

        Matched by what each group's condition TESTS, never by its name.
        The previous code read

            new_df = "df_new_records" if "new_records" in group_names else "df_new"

        which assumed one naming convention and, for every other one, fell
        back to ``df_new``/``df_changed`` -- names nothing in the notebook
        ever assigns. An export naming its groups ``Insert``/``Update``,
        which is the commoner Informatica convention, produced a notebook
        that died with NameError before reading a row.

        The semantics are stable even though the names are not:

        * the NEW branch tests the lookup key for NULL -- no match in the
          dimension, so the row is new;
        * the CHANGED branch tests a lookup value for inequality -- matched,
          but something differs, so a new version is needed.

        Returns a reason instead of a guess when either branch cannot be
        identified, so the caller can emit a REVIEW marker rather than code
        that cannot run.
        """
        router = next((tx for tx in mapping.transformations
                       if tx.type == TransformationType.ROUTER and tx.router_groups),
                      None)
        if router is None:
            return "", "", "No Router with output groups was found."

        new_grp = changed_grp = ""
        for g in router.router_groups:
            if not isinstance(g, dict):
                continue
            gname = (g.get("name") or "").strip()
            if not gname or (g.get("type") or "").upper().startswith("INPUT"):
                continue
            cond = (g.get("condition") or "").upper()
            if not cond:
                continue
            is_null_test = "ISNULL" in cond or "IS NULL" in cond
            is_diff_test = ("!=" in cond or "<>" in cond)
            if is_null_test and not is_diff_test and not new_grp:
                new_grp = gname
            elif is_diff_test and not changed_grp:
                changed_grp = gname

        if not new_grp or not changed_grp:
            missing = []
            if not new_grp:
                missing.append("no group tests the lookup key for NULL (the NEW branch)")
            if not changed_grp:
                missing.append("no group tests a lookup value for inequality "
                               "(the CHANGED branch)")
            return "", "", "; ".join(missing) + "."
        return f"df_{new_grp.lower()}", f"df_{changed_grp.lower()}", ""

    def _scd2_merge_branches_cell(self, mapping: Mapping) -> str:
        """Generate cell that merges Router branches for SCD2.

        After Router creates df_new_records and df_changed_records,
        we combine them for the target write. The write cell handles:
        - Expiring old records (using df_changed_records)
        - Generating surrogate keys
        - Appending new rows

        We do NOT overwrite EFF_START_DATE, EFF_END_DATE, or CURRENT_FLAG here
        — those were already correctly set by the Expression transformation.
        """
        # Find router group names
        group_names = []
        for tx in mapping.transformations:
            if tx.type == TransformationType.ROUTER and tx.router_groups:
                for g in tx.router_groups:
                    group_names.append(g.get("name", ""))

        new_df, changed_df, why = self._scd2_branch_dfs(mapping, group_names)
        if why:
            return "\n".join([
                "# SCD2: merge Router branches (new + changed records)",
                f"# Router groups found: {', '.join(group_names) or '(none)'}",
                "",
                f"raise NotImplementedError(",
                f"    \"REVIEW REQUIRED: cannot tell which Router group carries NEW rows \"",
                f"    \"and which carries CHANGED rows for this SCD2 mapping. {why} \"",
                f"    \"Informatica names these groups freely -- Insert/Update, \"",
                f"    \"New/Changed, new_records/changed_records -- so they are matched \"",
                f"    \"by what their conditions test, not by name. Wire the union by \"",
                f"    \"hand: the NEW branch is the group whose condition finds no \"",
                f"    \"lookup match, the CHANGED branch the one that found a match \"",
                f"    \"with different values.\"",
                f")",
            ])

        # Find CURRENT_FLAG column name
        current_flag_col = "CURRENT_FLAG"
        for target in mapping.targets:
            cf = self._find_field(target, ["CURRENT_FLAG", "CURRENT_FLG", "IS_CURRENT"])
            if cf:
                current_flag_col = cf
                break

        lines = [
            "# SCD2: Merge Router branches (new + changed records)",
            f"# Router created: {', '.join('df_' + g for g in group_names)}",
            "",
            f"# Set CURRENT_FLAG = 'Y' on all rows being inserted (new + changed versions)",
            f'{new_df} = {new_df}.withColumn("{current_flag_col}", F.lit("Y"))',
            f'{changed_df} = {changed_df}.withColumn("{current_flag_col}", F.lit("Y"))',
            "",
            f"# Combine new + changed branches for target insert",
            f"df = {new_df}.unionByName({changed_df}, allowMissingColumns=True)",
        ]

        return "\n".join(lines)

    @staticmethod
    def _has_scd2_pattern(mapping: Mapping) -> bool:
        """Detect SCD2 pattern from transformation names and types."""
        for tx in mapping.transformations:
            name_upper = tx.name.upper()
            if "SCD2" in name_upper or "SCD_TYPE2" in name_upper:
                return True
            if tx.type == TransformationType.ROUTER:
                for group in (tx.router_groups or []):
                    gname = (group.get("name", "") or "").upper()
                    if "NEW" in gname or "CHANGED" in gname or "EXPIRE" in gname:
                        return True
        return False

    @staticmethod
    def _find_field(target: TargetDefinition, candidates: list[str]) -> str:
        """Find a field name from candidates in the target definition."""
        target_fields = set()
        for f in target.fields:
            if isinstance(f, FieldMapping):
                target_fields.add(f.target_field.upper())
            elif isinstance(f, dict):
                target_fields.add((f.get("target_field") or f.get("name", "")).upper())
        for candidate in candidates:
            if candidate.upper() in target_fields:
                return candidate
        return candidates[0]  # default to first candidate

    @staticmethod
    def _summary_cell(mapping: Mapping) -> str:
        target = mapping.targets[0] if mapping.targets else None
        table = (target.warehouse_table or target.table_name or target.name) if target else "N/A"
        return (
            '# Release cached DataFrame\n'
            'df_final.unpersist()\n'
            '\n'
            'logger.info("Migration complete")\n'
            'logger.info(f"Rows processed: {row_count}")\n'
            f'logger.info(f"Target table: {table}")'
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _target_write_cell_for(
        self,
        mapping: Mapping,
        target: TargetDefinition,
        conversion_result: dict,
        idx: int,
        session: Optional[Session] = None,
    ) -> str:
        """Generate write cell for a specific target (multi-target support)."""
        tgt_parts = []
        if target.db_name:
            tgt_parts.append(target.db_name)
        if target.owner:
            tgt_parts.append(target.owner)
        tgt_parts.append(target.warehouse_table or target.table_name or target.name)
        table = ".".join(tgt_parts)

        # Determine which DataFrame feeds this target: trace the connectors
        # into the target instance back to the DataFrame its predecessor
        # produced (a Router predecessor resolves to the group its ports
        # belong to). The previous "target i gets Router group i" pairing
        # was positional and wrote the wrong group's rows to a target
        # whenever the export's group order and target order disagreed.
        df_out = getattr(self, "_df_out", {}) or {}
        tx_map = getattr(self, "_tx_map", None) or {tx.name: tx for tx in mapping.transformations}
        df_var = self._df_feeding(target.name, mapping, tx_map, df_out) or ""
        if not df_var:
            df_var = "df_final"
            for tx in mapping.transformations:
                if tx.type == TransformationType.ROUTER and tx.router_groups:
                    # Fallback only: first target gets first group, second gets second, etc.
                    if idx < len(tx.router_groups):
                        group_name = tx.router_groups[idx].get("name", "")
                        df_var = f"df_{_safe_name(group_name)}"

        # Select only the columns defined in the target schema
        target_cols = []
        for f in target.fields:
            if isinstance(f, FieldMapping):
                target_cols.append(f.target_field)
            elif isinstance(f, dict):
                target_cols.append(f.get("target_field") or f.get("name", ""))

        # Work on this target's own copy, renamed to the target's port names.
        pre_lines: list[str] = []
        target_var = f"df_tgt_{_safe_name(target.name)}"
        tgt_renames = self._target_renames(target.name, mapping, tx_map)
        expr = f"_rename_cols({df_var}, {tgt_renames!r})" if tgt_renames else df_var
        pre_lines.append(f"{target_var} = {expr}")
        df_var = target_var

        # Determine the update strategy for this target path
        # by checking which Update Strategy transformation feeds this target
        update_strategy = ""
        from collections import defaultdict as _dd
        _fwd = _dd(set)
        for conn in mapping.connectors:
            _fwd[conn.from_instance].add(conn.to_instance)
        for tx in mapping.transformations:
            if tx.type == TransformationType.UPDATE_STRATEGY:
                # Check if this UPD feeds this target (directly or via next hop)
                if target.name in _fwd.get(tx.name, set()):
                    update_strategy = (tx.update_strategy_expression or "").upper()
                    break
                # Check via intermediate transforms
                for succ in _fwd.get(tx.name, set()):
                    if target.name in _fwd.get(succ, set()):
                        update_strategy = (tx.update_strategy_expression or "").upper()
                        break

        has_update_strategy = bool(update_strategy) or (
            self._has_update_strategy(mapping) and not mapping.connectors
        )
        strategy, strategy_source = self._resolve_load_strategy(target, session)

        lines = [f"# Write to target {idx + 1}: {table}"]
        lines.append(f"# Load strategy: {strategy.value} (from {strategy_source})")
        lines.extend(pre_lines)
        if target_cols:
            if has_update_strategy and "DD_STRATEGY" not in target_cols:
                target_cols = target_cols + ["DD_STRATEGY"]
            col_list = ", ".join(f'"{c}"' for c in target_cols)
            # A target column no connector feeds is NULL, as Informatica
            # writes it (an SCD2 expire path connects only the key, the end
            # date and the flag); selecting it by name failed. Typed as the
            # target declares it: an untyped NULL is a VOID column, which
            # Delta will not store.
            from .ddl_generator import _ddl_type
            types = {}
            for f in target.fields:
                if isinstance(f, FieldMapping):
                    types[f.target_field] = _ddl_type(f.datatype, f.precision, f.scale)[0]
            type_map = {c: types.get(c, "string") for c in target_cols if c != "DD_STRATEGY"}
            lines.append(f"# Select only target-defined columns (unconnected ones are NULL)")
            lines.append(f"_types_{df_var} = {type_map!r}")
            lines.append(
                f"{df_var} = {df_var}.select(*[F.col(_c) if _c in {df_var}.columns "
                f"else F.lit(None).cast(_types_{df_var}.get(_c, 'string')).alias(_c) for _c in [{col_list}]])"
            )
        lines.append("try:")

        all_keys = self._key_columns_for_target(target)
        keys = self._natural_keys(target, all_keys)

        if getattr(target, "flat_file", None):
            lines.extend(self._flat_file_write(
                target, df_var, session, [c for c in target_cols if c != "DD_STRATEGY"]))
        elif has_update_strategy:
            keys = self._update_keys(target, all_keys)
            # DD_INSERT/DD_UPDATE/DD_DELETE/DD_REJECT routing is Class C
            # -- call infa_compat.apply_update_strategy +
            # write_update_strategy() instead of hand-rolling per-branch
            # MERGE logic here. Assumes the Update Strategy transformation's
            # own cell (transformation_converter.py's _update_strategy) has
            # already tagged {df_var} with a "DD_STRATEGY" column of
            # infa_compat.UpdateStrategyCode values.
            lines.extend(self._emit_update_strategy_write(
                table, keys, df_var, self._connected_columns(mapping, target, keys)))
        elif keys and strategy != LoadStrategy.INSERT:
            # A keyed load strategy (upsert/update/delete) MERGEs on the
            # target's keys. INSERT used to be upgraded to UPSERT here for
            # multi-target mappings only, so a re-run updated rows the source
            # meant to append.
            lines.extend(self._write_strategy.emit_write(
                None, None, table, strategy, keys, df_var=df_var,
            ))
        else:
            lines.extend(self._write_strategy.emit_write(
                None, None, table, strategy, [], df_var=df_var,
            ))

        lines.append(f'    logger.info("Write to {table} complete")')
        lines.append("except Exception as e:")
        lines.append(f'    logger.error(f"Failed to write to {table}: {{e}}")')
        lines.append("    raise")
        return "\n".join(lines)

    @staticmethod
    def _key_columns_for_target(target: TargetDefinition) -> list[str]:
        """Extract key columns from a specific target."""
        keys = []
        for fld in target.fields:
            if isinstance(fld, FieldMapping) and fld.is_key:
                keys.append(fld.target_field)
            elif isinstance(fld, dict) and fld.get("is_key"):
                keys.append(fld.get("target_field", fld.get("name", "")))
        return keys

    @staticmethod
    def _key_columns(mapping: Mapping) -> list[str]:
        """Extract key columns from target field definitions."""
        keys: list[str] = []
        for target in mapping.targets:
            for fld in target.fields:
                if isinstance(fld, FieldMapping) and fld.is_key:
                    keys.append(fld.target_field)
                elif isinstance(fld, dict) and fld.get("is_key"):
                    keys.append(fld.get("target_field", fld.get("name", "")))
        # Two targets sharing a key name must not produce a duplicate.
        return list(dict.fromkeys(keys))

    @staticmethod
    def _assemble(cells: list[str]) -> str:
        """Join cells into a Databricks .py notebook string."""
        parts = ["# Databricks notebook source"]
        for cell in cells:
            parts.append(CELL_SEP)
            parts.append(cell)
        return "\n\n".join(parts) + "\n"

    @staticmethod
    def _assemble_ipynb(cells: list[str], mapping: Mapping) -> str:
        """Build a Jupyter .ipynb notebook (JSON format)."""
        nb_cells = []
        for cell_content in cells:
            # Detect markdown cells (start with # MAGIC %md)
            if cell_content.strip().startswith("# MAGIC %md"):
                # Convert MAGIC markdown to plain markdown
                md_lines = []
                for line in cell_content.split("\n"):
                    line = line.strip()
                    if line.startswith("# MAGIC %md"):
                        continue  # skip the directive line
                    elif line.startswith("# MAGIC "):
                        md_lines.append(line[8:])  # strip "# MAGIC "
                    elif line == "# MAGIC":
                        md_lines.append("")
                    else:
                        md_lines.append(line)
                nb_cells.append({
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": [l + "\n" for l in md_lines],
                })
            else:
                # Code cell
                source_lines = [l + "\n" for l in cell_content.split("\n")]
                # Remove trailing empty newline
                if source_lines and source_lines[-1].strip() == "":
                    source_lines[-1] = source_lines[-1].rstrip("\n")
                nb_cells.append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": source_lines,
                })

        notebook = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {
                    "name": "python",
                    "version": "3.10.0",
                    "mimetype": "text/x-python",
                    "file_extension": ".py",
                },
                "infa2aidp": {"mapping": mapping.name},
            },
            "cells": nb_cells,
        }
        return json.dumps(notebook, indent=1, ensure_ascii=False)

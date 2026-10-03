"""Optimization rules that analyze generated PySpark code and suggest improvements.

Each rule is self-contained: it inspects the notebook code as a string using
regex/pattern matching and returns a list of OptimizationSuggestion objects.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod

from .models import OptimizationSuggestion, OptimizationType


class OptimizationRule(ABC):
    """Base class for optimization rules."""

    name: str = ""
    description: str = ""

    @abstractmethod
    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Broadcast join
# ---------------------------------------------------------------------------

class BroadcastJoinRule(OptimizationRule):
    """Detect joins where one side is small enough to broadcast.

    Looks for join patterns involving lookup DataFrames (lkp_ / df_lookup)
    that don't already use F.broadcast(). Dimension/lookup tables are
    typically small enough to broadcast.
    """

    name = "Broadcast Join"
    description = "Add broadcast hint for small lookup tables to avoid shuffle joins."

    # Matches: .join(lkp_foo, ...) or .join(df_lookup, ...)
    _JOIN_RE = re.compile(
        r"\.join\(\s*((?:lkp_|df_lookup)\w*)\s*,",
        re.MULTILINE,
    )
    # Matches existing broadcast usage
    _BROADCAST_RE = re.compile(r"F\.broadcast\(", re.MULTILINE)

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        suggestions: list[OptimizationSuggestion] = []

        for match in self._JOIN_RE.finditer(code):
            df_name = match.group(1)
            # Check this specific join isn't already broadcast
            line_start = code.rfind("\n", 0, match.start()) + 1
            line_end = code.find("\n", match.end())
            line = code[line_start:line_end] if line_end != -1 else code[line_start:]

            if "F.broadcast(" in line or "broadcast(" in line:
                continue

            line_number = code[:match.start()].count("\n") + 1
            original = f".join({df_name},"
            optimized = f".join(F.broadcast({df_name}),"

            suggestions.append(OptimizationSuggestion(
                rule_name=self.name,
                optimization_type=OptimizationType.BROADCAST_JOIN,
                description=f"Broadcast {df_name} to avoid shuffle join (lookup tables are typically small).",
                original_code=original,
                optimized_code=optimized,
                estimated_improvement="2-10x faster join",
                priority="HIGH",
                auto_applicable=True,
                line_number=line_number,
                applies_to=df_name,
            ))

        return suggestions


# ---------------------------------------------------------------------------
# Predicate pushdown
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Cache reuse
# ---------------------------------------------------------------------------

class CacheReuseRule(OptimizationRule):
    """Detect DataFrames read multiple times that should be cached.

    If a DataFrame variable appears in more than two join/operation contexts,
    caching avoids recomputation.
    """

    name = "Cache Reuse"
    description = "Cache DataFrames that are referenced multiple times."

    _DF_ASSIGN_RE = re.compile(r'^(df_\w+)\s*=', re.MULTILINE)

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        suggestions: list[OptimizationSuggestion] = []

        # Find all df_ variable assignments
        assigned: dict[str, int] = {}
        for match in self._DF_ASSIGN_RE.finditer(code):
            name = match.group(1)
            assigned[name] = code[:match.start()].count("\n") + 1

        for df_name, def_line in assigned.items():
            if f"{df_name}.cache()" in code or f"{df_name}.persist()" in code:
                continue

            # Count usages beyond the assignment itself
            usage_count = len(re.findall(rf'\b{re.escape(df_name)}\b', code)) - 1
            if usage_count > 2:
                suggestions.append(OptimizationSuggestion(
                    rule_name=self.name,
                    optimization_type=OptimizationType.CACHE_REUSE,
                    description=(
                        f"{df_name} is referenced {usage_count} times. "
                        f"Cache it after creation and unpersist after last use."
                    ),
                    original_code=f"{df_name} = ...",
                    optimized_code=f"{df_name} = ...\n{df_name}.cache()\n# ... later ...\n{df_name}.unpersist()",
                    estimated_improvement="Avoid recomputation, 2-5x for complex pipelines",
                    priority="MEDIUM",
                    auto_applicable=False,
                    line_number=def_line,
                    applies_to=df_name,
                ))

        return suggestions


# ---------------------------------------------------------------------------
# Column pruning
# ---------------------------------------------------------------------------

class ColumnPruningRule(OptimizationRule):
    """Detect when more columns are read than needed.

    If a source read uses .table() without a .select() immediately after,
    and downstream code only uses specific columns, suggest pruning.
    """

    name = "Column Pruning"
    description = "Select only needed columns at source to reduce memory and I/O."

    _TABLE_READ_RE = re.compile(
        r'(\w+)\s*=\s*spark\.table\("([^"]+)"\)',
    )

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        suggestions: list[OptimizationSuggestion] = []

        reads: list[tuple[str, str, int]] = []  # (df_name, source, line)
        for match in self._TABLE_READ_RE.finditer(code):
            reads.append((match.group(1), match.group(2), code[:match.start()].count("\n") + 1))

        for df_name, source, line_number in reads:
            # Check if there's already a .select() within the next few lines
            assign_end = code.find(f"{df_name} =")
            if assign_end == -1:
                continue
            next_chunk = code[assign_end:assign_end + 500]
            if ".select(" in next_chunk.split("\n")[0]:
                continue

            # Find columns referenced downstream via F.col("...") on this df
            col_refs = set(re.findall(r'F\.col\("(\w+)"\)', code))
            if col_refs and len(col_refs) < 20:
                col_list = ", ".join(f'"{c}"' for c in sorted(col_refs))
                suggestions.append(OptimizationSuggestion(
                    rule_name=self.name,
                    optimization_type=OptimizationType.COLUMN_PRUNING,
                    description=(
                        f"Read from {source} selects all columns. Add .select() to "
                        f"read only the {len(col_refs)} columns used downstream."
                    ),
                    original_code=f'{df_name} = spark.table("{source}")',
                    optimized_code=f'{df_name} = spark.table("{source}").select({col_list})',
                    estimated_improvement="Reduced memory and faster shuffles",
                    priority="MEDIUM",
                    auto_applicable=False,  # column list may need refinement
                    line_number=line_number,
                    applies_to=source,
                ))

        return suggestions


# ---------------------------------------------------------------------------
# Repartition
# ---------------------------------------------------------------------------

class RepartitionRule(OptimizationRule):
    """Suggest repartitioning for join-heavy pipelines.

    If there are multiple joins on the same key column, repartitioning
    by that key before the join chain reduces shuffles.
    """

    name = "Repartition"
    description = "Repartition by join key before multi-join pipelines."

    _JOIN_ON_RE = re.compile(
        r'\.join\(.*?,\s*(?:on=)?\[?"(\w+)"',
        re.MULTILINE,
    )

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        suggestions: list[OptimizationSuggestion] = []

        # Count join key usage
        key_counts: dict[str, int] = {}
        for match in self._JOIN_ON_RE.finditer(code):
            key = match.group(1)
            key_counts[key] = key_counts.get(key, 0) + 1

        for key, count in key_counts.items():
            if count < 2:
                continue

            suggestions.append(OptimizationSuggestion(
                rule_name=self.name,
                optimization_type=OptimizationType.REPARTITION,
                description=(
                    f"Column \"{key}\" is used as join key {count} times. "
                    f"Repartition source DataFrame by \"{key}\" before the join chain "
                    f"to co-locate data and eliminate repeated shuffles."
                ),
                original_code="",
                optimized_code=f'df_source = df_source.repartition("{key}")',
                estimated_improvement=f"Eliminate {count - 1} shuffle(s), ~{count}x faster joins",
                priority="MEDIUM",
                auto_applicable=False,
                line_number=0,
                applies_to=key,
            ))

        return suggestions


# ---------------------------------------------------------------------------
# Delta optimize
# ---------------------------------------------------------------------------

class DeltaOptimizeRule(OptimizationRule):
    """Add Delta Lake optimization hints after writes.

    Suggest OPTIMIZE + ZORDER for tables, and liquid clustering hints for
    large tables. For SCD tables, suggest partitioning by date column.
    """

    name = "Delta Optimize"
    description = "Add Delta Lake OPTIMIZE/ZORDER after writes."

    _SAVE_RE = re.compile(
        r'\.saveAsTable\("([^"]+)"\)',
    )
    _MERGE_RE = re.compile(
        r'DeltaTable\.forName\(spark,\s*"([^"]+)"\)',
    )
    _SCD_MARKERS = re.compile(
        r'EFFECTIVE_TO_DATE|SCD_TYPE2|CURRENT_FLG',
    )

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        suggestions: list[OptimizationSuggestion] = []

        tables: set[str] = set()
        for match in self._SAVE_RE.finditer(code):
            tables.add(match.group(1))
        for match in self._MERGE_RE.finditer(code):
            tables.add(match.group(1))

        if not tables:
            return suggestions

        is_scd = bool(self._SCD_MARKERS.search(code))

        for table in sorted(tables):
            line_number = code.find(table)
            line_number = code[:line_number].count("\n") + 1 if line_number != -1 else 0

            optimize_code = (
                f'spark.sql("OPTIMIZE {table}")\n'
                f'# For frequently filtered columns, add ZORDER:\n'
                f'# spark.sql("OPTIMIZE {table} ZORDER BY (key_col)")'
            )

            if is_scd:
                optimize_code += (
                    f'\n# SCD table detected — consider partitioning by date:\n'
                    f'# .partitionBy("EFFECTIVE_FROM_DATE")'
                )

            suggestions.append(OptimizationSuggestion(
                rule_name=self.name,
                optimization_type=OptimizationType.DELTA_OPTIMIZE,
                description=(
                    f"Run OPTIMIZE on {table} after write to compact small files. "
                    f"Add ZORDER on frequently queried columns for faster reads."
                ),
                original_code="",
                optimized_code=optimize_code,
                estimated_improvement="10-50x faster downstream reads",
                priority="MEDIUM",
                auto_applicable=False,
                line_number=line_number,
                applies_to=table,
            ))

        return suggestions


# ---------------------------------------------------------------------------
# Adaptive Query Execution (AQE)
# ---------------------------------------------------------------------------

class AQERule(OptimizationRule):
    """Suggest enabling Adaptive Query Execution for complex pipelines.

    AQE dynamically adjusts query plans based on runtime statistics,
    coalesces small partitions, and optimizes skewed joins.
    """

    name = "Adaptive Query Execution"
    description = "Enable AQE for dynamic query optimization."

    _AQE_SETTING_RE = re.compile(r'spark\.sql\.adaptive\.enabled')
    # "Complex" = 2+ joins or an aggregator
    _JOIN_RE = re.compile(r'\.join\(')
    _AGG_RE = re.compile(r'\.agg\(|\.groupBy\(')

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        if self._AQE_SETTING_RE.search(code):
            return []

        join_count = len(self._JOIN_RE.findall(code))
        has_agg = bool(self._AGG_RE.search(code))

        if join_count < 2 and not has_agg:
            return []

        return [OptimizationSuggestion(
            rule_name=self.name,
            optimization_type=OptimizationType.AQE,
            description=(
                "Enable Adaptive Query Execution. This pipeline has "
                f"{join_count} join(s) and {'aggregations' if has_agg else 'no aggregations'} — "
                f"AQE will dynamically optimize partitioning and handle skew."
            ),
            original_code="spark = SparkSession.builder.getOrCreate()",
            optimized_code=(
                "spark = SparkSession.builder.getOrCreate()\n"
                'spark.conf.set("spark.sql.adaptive.enabled", "true")\n'
                'spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")\n'
                'spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")'
            ),
            estimated_improvement="20-50% faster for complex pipelines",
            priority="MEDIUM",
            auto_applicable=True,
            line_number=1,
            applies_to="spark_config",
        )]


# ---------------------------------------------------------------------------
# UDF elimination
# ---------------------------------------------------------------------------

class UDFEliminationRule(OptimizationRule):
    """Detect Python UDFs that may have built-in Spark equivalents.

    Python UDFs break Catalyst optimization, force row-at-a-time
    processing, and prevent predicate pushdown. Replacing them with
    built-in Spark functions yields major performance gains.
    """

    name = "UDF Elimination"
    description = "Replace Python UDFs with built-in Spark functions where possible."

    _UDF_DECORATOR_RE = re.compile(r'@(?:F\.)?udf', re.MULTILINE)
    _UDF_CALL_RE = re.compile(r'F\.udf\(', re.MULTILINE)

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        suggestions: list[OptimizationSuggestion] = []

        for pattern in [self._UDF_DECORATOR_RE, self._UDF_CALL_RE]:
            for match in pattern.finditer(code):
                line_number = code[:match.start()].count("\n") + 1

                # Extract the UDF context (a few lines around it)
                line_start = code.rfind("\n", 0, match.start()) + 1
                line_end = code.find("\n", match.end())
                original_line = code[line_start:line_end] if line_end != -1 else code[line_start:]

                suggestions.append(OptimizationSuggestion(
                    rule_name=self.name,
                    optimization_type=OptimizationType.UDF_ELIMINATION,
                    description=(
                        "Python UDF detected. UDFs disable Catalyst optimization and "
                        "force serialization between JVM and Python. Check if a built-in "
                        "Spark SQL function (F.when, F.regexp_replace, F.coalesce, etc.) "
                        "can replace this UDF."
                    ),
                    original_code=original_line.strip(),
                    optimized_code="# Replace with built-in: F.when(), F.coalesce(), F.regexp_replace(), etc.",
                    estimated_improvement="10-100x faster (avoids Python serialization)",
                    priority="HIGH",
                    auto_applicable=False,
                    line_number=line_number,
                    applies_to="udf",
                ))

        return suggestions


# ---------------------------------------------------------------------------
# Parallelism for independent branches
# ---------------------------------------------------------------------------

class ParallelismRule(OptimizationRule):
    """Suggest parallel execution for independent transformation branches.

    If the code has router-style outputs (multiple independent DataFrames
    being written to different targets), they can run concurrently.
    """

    name = "Parallel Execution"
    description = "Run independent transformation branches in parallel."

    _SAVE_RE = re.compile(r'(\w+)\.write\..*?\.saveAsTable\("([^"]+)"\)', re.DOTALL)

    def analyze(self, code: str, context: dict | None = None) -> list[OptimizationSuggestion]:
        writes = list(self._SAVE_RE.finditer(code))
        if len(writes) < 2:
            return []

        targets = [m.group(2) for m in writes]
        target_list = ", ".join(targets)

        return [OptimizationSuggestion(
            rule_name=self.name,
            optimization_type=OptimizationType.COALESCE,  # closest type
            description=(
                f"Found {len(writes)} independent write targets ({target_list}). "
                f"Execute them in parallel using concurrent.futures.ThreadPoolExecutor."
            ),
            original_code="# Sequential writes",
            optimized_code=(
                "from concurrent.futures import ThreadPoolExecutor\n\n"
                "def write_target(df, table):\n"
                "    df.write.mode(\"overwrite\").saveAsTable(table)\n\n"
                "with ThreadPoolExecutor(max_workers=" + str(len(writes)) + ") as pool:\n"
                "    futures = [\n"
                + "".join(f'        pool.submit(write_target, {m.group(1)}, "{m.group(2)}"),\n' for m in writes)
                + "    ]\n"
                "    for f in futures:\n"
                "        f.result()  # raise if any failed"
            ),
            estimated_improvement=f"~{len(writes)}x faster total write time",
            priority="LOW",
            auto_applicable=False,
            line_number=0,
            applies_to="parallel_writes",
        )]


# ---------------------------------------------------------------------------
# All rules, in evaluation order
# ---------------------------------------------------------------------------

ALL_RULES: list[type[OptimizationRule]] = [
    BroadcastJoinRule,
    CacheReuseRule,
    ColumnPruningRule,
    RepartitionRule,
    DeltaOptimizeRule,
    AQERule,
    UDFEliminationRule,
    ParallelismRule,
]

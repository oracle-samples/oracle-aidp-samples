"""
BenchmarkResult — holds aggregate metrics and persists to AIDP Unified Catalog.

Implements:
- D1: saveAsTable() to AIDP Unified Catalog (Iceberg, two tables)
- Spec §4.5.1: Two-table schema — query_results (per-query) + run_summary (per-run)
- Spec §4.6.1: GeoMean as primary aggregate; P50/P90/P95/P99 latency
- OPEN-3 decision: fresh `aidp_benchmark` database, CREATE IF NOT EXISTS in save()
"""
from __future__ import annotations

import logging
import math
import statistics
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class QueryResult:
    """One row in aidp_benchmark.query_results."""

    query_name: str
    elapsed_seconds: float
    status: str                 # "PASS" | "FAIL" | "TIMEOUT" | "ERROR"
    rows_returned: int = 0
    error_message: str = ""
    execution_plan: str = ""    # EXPLAIN EXTENDED text


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run.

    Persisted as two Iceberg tables on .save():
      aidp_benchmark.run_summary    — one row, this run's aggregates
      aidp_benchmark.query_results  — N rows, one per query in this run
    """

    run_id: str
    benchmark_type: str          # "power" | "throughput"
    scale_factor: int
    engine_version: str
    queries_source: str
    start_time: datetime
    end_time: datetime
    query_results: list[QueryResult] = field(default_factory=list)

    # Computed aggregates.
    geomean_seconds: Optional[float] = None
    p50_seconds: Optional[float] = None
    p90_seconds: Optional[float] = None
    p95_seconds: Optional[float] = None
    p99_seconds: Optional[float] = None

    # ── Constructors ────────────────────────────────────────────────────

    @classmethod
    def from_query_results(
        cls,
        per_query: list[QueryResult],
        scale_factor: int,
        engine_version: str,
        queries_source: str,
        benchmark_type: str = "power",
    ) -> "BenchmarkResult":
        """Build a BenchmarkResult from per-query results, computing aggregates."""
        now = datetime.now(timezone.utc)
        elapsed = [q.elapsed_seconds for q in per_query if q.status == "PASS"]
        result = cls(
            run_id=str(uuid.uuid4()),
            benchmark_type=benchmark_type,
            scale_factor=scale_factor,
            engine_version=engine_version,
            queries_source=queries_source,
            start_time=now,    # caller can adjust if needed
            end_time=now,
            query_results=list(per_query),
        )
        if elapsed:
            result.geomean_seconds = math.exp(sum(math.log(v) for v in elapsed) / len(elapsed))
            sorted_e = sorted(elapsed)
            result.p50_seconds = statistics.median(sorted_e)
            result.p90_seconds = _percentile(sorted_e, 0.90)
            result.p95_seconds = _percentile(sorted_e, 0.95)
            result.p99_seconds = _percentile(sorted_e, 0.99)
        return result

    # ── Public API ──────────────────────────────────────────────────────

    @property
    def queries_passed(self) -> int:
        return sum(1 for q in self.query_results if q.status == "PASS")

    @property
    def queries_failed(self) -> int:
        return sum(1 for q in self.query_results if q.status != "PASS")

    def summary(self) -> str:
        """Human-readable summary card."""
        lines = [
            "=" * 64,
            f" BENCHMARK RESULT  run_id={self.run_id[:8]}…  type={self.benchmark_type}",
            f"   scale_factor={self.scale_factor}  queries_source={self.queries_source}",
            f"   engine={self.engine_version}",
            "=" * 64,
            f"   Queries passed: {self.queries_passed}/{len(self.query_results)}",
            f"   Queries failed: {self.queries_failed}",
        ]
        if self.geomean_seconds is not None:
            lines.append(f"   GeoMean:        {self.geomean_seconds:.2f}s")
            lines.append(f"   P50:            {self.p50_seconds:.2f}s")
            lines.append(f"   P95:            {self.p95_seconds:.2f}s")
            lines.append(f"   P99:            {self.p99_seconds:.2f}s")
        lines += [
            "=" * 64,
            "   Results are not comparable to official TPC Benchmark Results.",
        ]
        return "\n".join(lines)

    def save(self, database: str = "aidp_benchmark", spark=None) -> None:
        """Persist this run to two Iceberg tables in the AIDP Unified Catalog.

        Per D1 — uses Spark's saveAsTable() so AIDP's pre-configured catalog
        does the routing. Customer doesn't need to know which catalog type
        (Iceberg REST, HMS, etc.) — the Spark session already points at it.

        Idempotent on `CREATE DATABASE IF NOT EXISTS`. The DROP DATABASE
        first-run cleanup is a separate one-time step (notebook cell, not
        called here automatically).
        """
        if spark is None:
            from .benchmark import _get_spark
            spark = _get_spark()

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        # Build summary row.
        summary_row = {
            "run_id":             self.run_id,
            "benchmark_type":     self.benchmark_type,
            "scale_factor":       self.scale_factor,
            "engine_version":     self.engine_version,
            "queries_source":     self.queries_source,
            "queries_total":      len(self.query_results),
            "queries_passed":     self.queries_passed,
            "queries_failed":     self.queries_failed,
            "geomean_seconds":    float(self.geomean_seconds) if self.geomean_seconds else None,
            "p50_seconds":        float(self.p50_seconds) if self.p50_seconds else None,
            "p95_seconds":        float(self.p95_seconds) if self.p95_seconds else None,
            "p99_seconds":        float(self.p99_seconds) if self.p99_seconds else None,
            "start_time":         self.start_time.isoformat(),
            "end_time":           self.end_time.isoformat(),
        }

        # Build per-query rows.
        query_rows = [{
            "run_id":           self.run_id,
            "query_name":       q.query_name,
            "elapsed_seconds":  float(q.elapsed_seconds),
            "status":           q.status,
            "rows_returned":    int(q.rows_returned),
            "error_message":    q.error_message,
            "execution_plan":   q.execution_plan,
        } for q in self.query_results]

        # Write Iceberg tables.
        (spark.createDataFrame([summary_row])
              .write.mode("append").format("iceberg")
              .saveAsTable(f"{database}.run_summary"))

        if query_rows:
            (spark.createDataFrame(query_rows)
                  .write.mode("append").format("iceberg")
                  .saveAsTable(f"{database}.query_results"))

        logger.info("Saved run %s to %s.{run_summary,query_results}", self.run_id, database)


def _percentile(sorted_values: list[float], pct: float) -> float:
    """Linear-interpolation percentile, pct in [0, 1]."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * pct
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)

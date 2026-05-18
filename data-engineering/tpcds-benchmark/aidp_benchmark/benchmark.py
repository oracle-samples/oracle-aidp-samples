"""
AIDPBenchmark — runs TPC-DS queries against generated data and produces a BenchmarkResult.

Implements:
- D2: Spark session entry point — pre-injected `spark` first, fallback to getOrCreate
- D3: OCI HDFS Connector for reading parquet (AIDP) — falls back to direct path on local
- D5 (decision-log): spark.sql(sql).count() — never .collect()
- D11: Bundled v4.0.0_spark queries (loaded from package data)
- Spec §4.4.1: Power test — 99 (=103 with a/b) queries sequential, GeoMean
- Spec §4.4.2: Throughput test — stub raising NotImplementedError until MVP 2 (per OPEN-4)
"""
from __future__ import annotations

import logging
import time
from importlib import resources
from pathlib import Path
from typing import Optional

from .generator import TPCDSDataGenerator, TPCDS_TABLES
from .results import BenchmarkResult, QueryResult

logger = logging.getLogger(__name__)


def _get_spark():
    """Return a SparkSession.

    Per D2: try the pre-injected `spark` from the notebook environment first
    (AIDP/Databricks/Jupyter kernels expose this), then fall back to
    SparkSession.builder.getOrCreate() for scripts and CI.
    """
    import builtins
    pre = getattr(builtins, "spark", None)
    if pre is not None:
        return pre

    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _load_bundled_queries() -> dict[str, str]:
    """Read the 103 v4.0.0_spark .sql files bundled in the package.

    Returns: {qname: sql_text}. Files are named q1.sql … q99.sql with
    a/b splits for q14, q23, q24, q39 (q14_a.sql, q14_b.sql, …).
    """
    queries: dict[str, str] = {}
    pkg = resources.files("aidp_benchmark") / "queries" / "v4_0_0_spark"
    for entry in pkg.iterdir():
        if entry.name.endswith(".sql"):
            queries[entry.name[:-4]] = entry.read_text()
    return queries


class AIDPBenchmark:
    """Run TPC-DS benchmark against generated data.

    Public surface (matches spec §4.2):
        bench = AIDPBenchmark(generator)
        result = bench.run_power_test()         # 103 queries sequential
        # result = bench.run_throughput_test()  # MVP 2 — raises NotImplementedError

    Constructor parameters (subset for V1.1):
        generator: an existing TPCDSDataGenerator (post .generate())
        timeout_seconds: per-query timeout (default 3600, spec §4.10.1)
        warmup_iterations: cold-pass count before measurement (default 0; recommended 1)
    """

    def __init__(
        self,
        generator: TPCDSDataGenerator,
        timeout_seconds: int = 3600,
        warmup_iterations: int = 0,
    ) -> None:
        self.generator = generator
        self.timeout_seconds = timeout_seconds
        self.warmup_iterations = warmup_iterations

        self._spark = None
        self._queries: Optional[dict[str, str]] = None

    # ── Public API ──────────────────────────────────────────────────────

    def run_power_test(self) -> BenchmarkResult:
        """Power test: all 103 queries sequentially, GeoMean is the metric.

        Returns a BenchmarkResult with per-query timings and aggregates.
        """
        spark = self._spark_session()
        queries = self._queries_loaded()
        self._register_temp_views(spark)

        # Optional warmup pass (results discarded). Spec §4.4.3.
        for i in range(self.warmup_iterations):
            logger.info("Warmup pass %d/%d", i + 1, self.warmup_iterations)
            self._run_once(spark, queries, record=False)

        # Measured run.
        per_query: list[QueryResult] = self._run_once(spark, queries, record=True)
        return BenchmarkResult.from_query_results(
            per_query,
            scale_factor=self.generator.scale_factor,
            engine_version=spark.version,
            queries_source="v4_0_0_spark",
            benchmark_type="power",
        )

    def run_throughput_test(self, streams: int = 4) -> BenchmarkResult:
        """V1.1 MVP 2 — not yet implemented.

        Per OPEN-4: V1.1 supports both modes, but ship power test first; throughput
        test (parallel streams, total wall-time metric) lands in MVP 2.
        """
        raise NotImplementedError(
            "run_throughput_test() ships in V1.1 MVP 2 — start with run_power_test() "
            "and we'll add throughput after the power-test baseline is stable on AIDP."
        )

    # ── Internals ───────────────────────────────────────────────────────

    def _spark_session(self):
        if self._spark is None:
            self._spark = _get_spark()
            logger.info("Spark version=%s  master=%s",
                        self._spark.version, self._spark.sparkContext.master)
        return self._spark

    def _queries_loaded(self) -> dict[str, str]:
        if self._queries is None:
            self._queries = _load_bundled_queries()
            logger.info("Loaded %d bundled v4.0.0_spark queries", len(self._queries))
        return self._queries

    def _register_temp_views(self, spark) -> None:
        """Read each Parquet from OCI bucket → temp view.

        Uses the OCI HDFS Connector path (oci://bucket@namespace/...). AIDP
        Spark sessions have ResourcePrincipal-based auth wired into Hadoop FS
        already (per D3 — proven in PoC Cell 5).
        """
        loaded = []
        for table in TPCDS_TABLES:
            uri = self.generator.bucket_uri(f"{table}.parquet")
            try:
                spark.read.parquet(uri).createOrReplaceTempView(table)
                loaded.append(table)
            except Exception as e:
                logger.error("Failed to register %s from %s: %s", table, uri, e)
                raise
        logger.info("%d temp views registered from %s", len(loaded),
                    self.generator.bucket_uri())

    def _run_once(self, spark, queries: dict[str, str], record: bool) -> list[QueryResult]:
        """Run all 103 queries once. Returns per-query results when record=True."""
        results: list[QueryResult] = []
        total = len(queries)
        for i, qname in enumerate(sorted(queries), 1):
            sql = queries[qname]
            start = time.time()
            status = "PASS"
            error_msg = ""
            rows = 0
            plan = ""
            try:
                df = spark.sql(sql)
                rows = df.count()  # D5 — never .collect() (driver crash at scale)
                if record:
                    # EXPLAIN EXTENDED for the result. Cheap; runs against the catalyst plan only.
                    try:
                        plan = "\n".join([r[0] for r in spark.sql(f"EXPLAIN EXTENDED {sql}").collect()])
                    except Exception:
                        plan = ""
            except Exception as e:
                status = "FAIL"
                error_msg = str(e).split("\n", 1)[0][:300]
                rows = 0

            elapsed = round(time.time() - start, 3)
            log_marker = "PASS" if status == "PASS" else "FAIL"
            logger.info("  [%3d/%d] %-8s %8.3fs  %s%s",
                        i, total, qname, elapsed, log_marker,
                        f"  {error_msg}" if error_msg else "")

            if record:
                results.append(QueryResult(
                    query_name=qname,
                    elapsed_seconds=elapsed,
                    status=status,
                    rows_returned=rows,
                    error_message=error_msg,
                    execution_plan=plan,
                ))
        return results

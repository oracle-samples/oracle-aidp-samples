"""
aidp_benchmark — AIDP TPC-DS Benchmark Toolkit (V1.2 — portable across OCI tenancies).

Public API:
    TPCDSDataGenerator    — generate + upload TPC-DS data to OCI
    AIDPBenchmark         — run power test against generated data
    BenchmarkResult       — per-run metrics + Unified Catalog persistence
    QueryResult           — per-query metrics inside a BenchmarkResult

Quick start (inside AIDP Workbench):
    from aidp_benchmark import TPCDSDataGenerator, AIDPBenchmark

    gen = TPCDSDataGenerator(scale_factor=1)
    gen.generate()                              # auto-detect tenancy, generate, upload

    result = AIDPBenchmark(gen).run_power_test()
    print(result.summary())
    result.save()                               # persist to aidp_benchmark.{run_summary, query_results}
"""
from .benchmark import AIDPBenchmark
from .config import AIDPEnvironment, detect_environment, ensure_bucket
from .generator import TPCDSDataGenerator, GenerationResult
from .results import BenchmarkResult, QueryResult

__version__ = "0.1.2"

__all__ = [
    "AIDPBenchmark",
    "AIDPEnvironment",
    "BenchmarkResult",
    "GenerationResult",
    "QueryResult",
    "TPCDSDataGenerator",
    "detect_environment",
    "ensure_bucket",
    "__version__",
]

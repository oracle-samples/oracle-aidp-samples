"""Unit test: BenchmarkResult.geomean_seconds computes correctly."""
import math

from aidp_benchmark.results import BenchmarkResult, QueryResult


def _make_result(elapsed_values: list[float]) -> BenchmarkResult:
    queries = [
        QueryResult(query_name=f"q{i}", elapsed_seconds=v, status="PASS")
        for i, v in enumerate(elapsed_values, start=1)
    ]
    return BenchmarkResult.from_query_results(
        per_query=queries,
        scale_factor=1,
        engine_version="test",
        queries_source="test",
    )


def test_geomean_three_values():
    # GeoMean(1, 2, 4) = (1 * 2 * 4)^(1/3) = 8^(1/3) = 2.0
    result = _make_result([1.0, 2.0, 4.0])
    assert result.geomean_seconds is not None
    assert math.isclose(result.geomean_seconds, 2.0, rel_tol=1e-9)


def test_geomean_uniform_values():
    # GeoMean(5, 5, 5) = 5
    result = _make_result([5.0, 5.0, 5.0])
    assert math.isclose(result.geomean_seconds, 5.0, rel_tol=1e-9)


def test_summary_contains_disclaimer():
    result = _make_result([1.0, 2.0, 4.0])
    summary = result.summary()
    assert "not comparable to official TPC Benchmark Results" in summary

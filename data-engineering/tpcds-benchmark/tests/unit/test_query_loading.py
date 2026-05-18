"""Unit test: _load_bundled_queries() returns exactly 103 entries."""
from aidp_benchmark.benchmark import _load_bundled_queries


def test_query_count():
    queries = _load_bundled_queries()
    assert len(queries) == 103, f"Expected 103 queries, got {len(queries)}"


def test_query_names_are_strings():
    queries = _load_bundled_queries()
    for name, sql in queries.items():
        assert isinstance(name, str) and len(name) > 0
        assert isinstance(sql, str) and len(sql) > 0

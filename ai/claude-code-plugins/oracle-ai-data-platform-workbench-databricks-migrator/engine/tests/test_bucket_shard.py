"""Tests for aidp_compat.bucket_shard."""
from __future__ import annotations

import os
import sys
from collections import Counter

import pytest

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "aidp_compat"))

from bucket_shard import BucketRouter, parse_oci_uri  # type: ignore[import-not-found]


# --------------------------- construction -----------------------------------

def test_router_rejects_bad_prefix():
    with pytest.raises(ValueError):
        BucketRouter(prefix="", num_shards=4, namespace="ns")
    with pytest.raises(ValueError):
        BucketRouter(prefix="bad/slash", num_shards=4, namespace="ns")
    with pytest.raises(ValueError):
        BucketRouter(prefix="bad@at", num_shards=4, namespace="ns")


def test_router_rejects_bad_num_shards():
    with pytest.raises(ValueError):
        BucketRouter(prefix="examplecorp", num_shards=0, namespace="ns")
    with pytest.raises(ValueError):
        BucketRouter(prefix="examplecorp", num_shards=100, namespace="ns", shard_width=2)


def test_router_rejects_missing_namespace():
    with pytest.raises(ValueError):
        BucketRouter(prefix="examplecorp", num_shards=4, namespace="")


# --------------------------- shard_index ------------------------------------

def test_shard_index_in_range():
    r = BucketRouter(prefix="examplecorp", num_shards=16, namespace="ns")
    for k in ["a", "b", "Example_Optimizer_Realtime", "x" * 100]:
        idx = r.shard_index(k)
        assert 0 <= idx < 16


def test_shard_index_is_deterministic():
    r = BucketRouter(prefix="examplecorp", num_shards=16, namespace="ns")
    assert r.shard_index("Example_Optimizer_Realtime") == r.shard_index("Example_Optimizer_Realtime")


def test_shard_index_handles_non_string():
    r = BucketRouter(prefix="examplecorp", num_shards=4, namespace="ns")
    # Integer key should be coerced
    assert 0 <= r.shard_index(42) < 4  # type: ignore[arg-type]


def test_shard_index_distribution_reasonable():
    """Hash distribution across 1000 keys should land within ~3x of mean per bucket."""
    r = BucketRouter(prefix="examplecorp", num_shards=16, namespace="ns")
    keys = [f"job_{i:04d}" for i in range(1000)]
    counts = Counter(r.shard_index(k) for k in keys)
    mean = 1000 / 16  # 62.5
    # No bucket should be empty, none should hold > 3x mean (very loose bound)
    assert len(counts) == 16
    for n in counts.values():
        assert 0 < n < 3 * mean


# --------------------------- bucket_for -------------------------------------

def test_bucket_for_uses_zero_padding():
    r = BucketRouter(prefix="mig-shard", num_shards=16, namespace="ns")
    name = r.bucket_for("anything")
    assert name.startswith("mig-shard-")
    suffix = name.rsplit("-", 1)[1]
    assert len(suffix) == 2
    assert suffix.isdigit()


def test_bucket_for_custom_shard_width():
    r = BucketRouter(prefix="examplecorp", num_shards=200, namespace="ns", shard_width=3)
    name = r.bucket_for("k")
    suffix = name.rsplit("-", 1)[1]
    assert len(suffix) == 3


def test_all_buckets_returns_full_pool():
    r = BucketRouter(prefix="mig-shard", num_shards=4, namespace="ns")
    assert r.all_buckets() == ["mig-shard-00", "mig-shard-01", "mig-shard-02", "mig-shard-03"]


# --------------------------- route_uri --------------------------------------

def test_route_uri_rewrites_bucket_only():
    r = BucketRouter(prefix="mig-shard", num_shards=16, namespace="exmplnspace1")
    rewritten = r.route_uri(
        "oci://mig-shard@exmplnspace1/feature_store/app_v162/x.parquet",
        shard_key="Example_Optimizer_Realtime",
    )
    parsed = parse_oci_uri(rewritten)
    assert parsed["bucket"].startswith("mig-shard-")
    assert parsed["namespace"] == "exmplnspace1"
    assert parsed["path"] == "/feature_store/app_v162/x.parquet"


def test_route_uri_default_key_uses_bucket_path():
    r = BucketRouter(prefix="mig-shard", num_shards=16, namespace="ns")
    a = r.route_uri("oci://b@ns/path/x")
    b = r.route_uri("oci://b@ns/path/x")
    assert a == b


def test_route_uri_rejects_non_oci():
    r = BucketRouter(prefix="mig-shard", num_shards=4, namespace="ns")
    with pytest.raises(ValueError):
        r.route_uri("s3://bucket/key")
    with pytest.raises(ValueError):
        r.route_uri("/Volumes/path/file")


def test_route_uri_preserves_root_path():
    r = BucketRouter(prefix="mig-shard", num_shards=4, namespace="ns")
    rewritten = r.route_uri("oci://mig-shard@ns", shard_key="k")
    parsed = parse_oci_uri(rewritten)
    assert parsed["path"] == "/"


# --------------------------- parse_oci_uri ----------------------------------

def test_parse_oci_uri_components():
    p = parse_oci_uri("oci://b1@ns1/some/path/file.parquet")
    assert p == {"bucket": "b1", "namespace": "ns1", "path": "/some/path/file.parquet"}


def test_parse_oci_uri_bare():
    p = parse_oci_uri("oci://b1@ns1")
    assert p["path"] == "/"


def test_parse_oci_uri_rejects_non_oci():
    with pytest.raises(ValueError):
        parse_oci_uri("https://example.com/foo")

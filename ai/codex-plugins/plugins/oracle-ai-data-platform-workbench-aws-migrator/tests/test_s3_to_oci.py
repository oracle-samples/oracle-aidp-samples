"""Tests for the S3 → OCI rclone transfer generator.

    python3 -m tests.test_s3_to_oci
"""
from __future__ import annotations

from aws_aidp.translate.s3_to_oci import build_transfer


def test_generates_rclone_copy():
    r = build_transfer("my-bucket", "my-bucket", "ns123", aws_region="eu-west-1")
    s = r.translated_sql
    assert "rclone" in s and "copy" in s
    assert "awssrc:my-bucket" in s
    assert "ocidest:my-bucket" in s
    assert "type = oracleobjectstorage" in s   # native OCI backend, not a custom copier
    assert "type = s3" in s
    assert "namespace = ns123" in s
    assert "region = eu-west-1" in s


def test_flags_prereqs():
    r = build_transfer("b", "b", "ns")
    assert r.flags >= 1
    assert any(f.rule == "transfer_prereqs" for f in r.findings)
    assert r.needs_manual_review


def test_reports_the_transfer_as_a_change():
    r = build_transfer("b", "b", "ns")
    assert r.changes >= 1
    assert any(f.rule == "rclone_transfer" for f in r.findings)


def _run_all():
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"  ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")


if __name__ == "__main__":
    _run_all()

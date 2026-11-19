"""Classify a migration report into PASS / REVIEW / SKIP / FAIL."""
from fabric_aidp.verify.checker import format_verify, verify

__all__ = ["format_verify", "verify"]

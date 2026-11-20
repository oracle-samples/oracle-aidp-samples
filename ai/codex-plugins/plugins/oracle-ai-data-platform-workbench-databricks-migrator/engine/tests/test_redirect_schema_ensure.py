"""Write-redirect schema ensure: cold-start hardening.

Three live incidents (2026-09-03..10) define the contract:
  - cold clusters answer the CREATE+exists probe in >60s (warm: 23-33s) —
    the old 60s/attempt timeout aborted runs whose CREATE actually landed;
  - a cold kernel can answer databaseExists=False for a schema that has
    existed since 2026-05 (single-catalog lake, confirmed via metadata API),
    then True 25s later — a False answer must be re-verified with a bounded
    delay before being believed;
  - absence after both create forms + re-verifies still raises (the
    data-safety hole of a missing redirect target stays fail-loud).
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import job_migrate as jm


class _Session:
    """Fake kernel session returning scripted exists-answers per execute."""

    def __init__(self, answers):
        self.answers = list(answers)   # each: True/False/Exception
        self.calls = []                # (code_hint, timeout)

    @property
    def cluster_id(self):
        return "test-cluster-000000000000"

    async def execute(self, code, timeout=None):
        self.calls.append(("create" if "CREATE SCHEMA" in code else "exists", timeout))
        a = self.answers.pop(0) if self.answers else False
        if isinstance(a, Exception):
            raise a
        return {"outputs": [{"type": "stream",
                             "text": f"REDIRECT_SCHEMA_EXISTS={a}"}]}


@pytest.fixture(autouse=True)
def _redirect_env(monkeypatch):
    monkeypatch.setattr(jm, "_REDIRECT_TABLE_PREFIX", "default.migration_sandbox_overwrite")
    monkeypatch.setattr(jm, "_redirect_schema_ensured_clusters", set())

    async def fake_sleep(_):
        return None
    monkeypatch.setattr(jm.asyncio, "sleep", fake_sleep)


def test_immediate_true_verifies_in_one_call():
    s = _Session([True])
    asyncio.run(jm._ensure_redirect_schema(s))
    assert [c[0] for c in s.calls] == ["create"]
    assert s.calls[0][1] == 180  # the cold-cluster floor, not the old 60


def test_cold_false_negative_absorbed_by_reverify():
    """The 2026-09-10 incident shape: exists=False right after create,
    True on a later check — must verify WITHOUT falling through to the
    plain-create attempt or raising."""
    s = _Session([False, False, True])
    asyncio.run(jm._ensure_redirect_schema(s))
    kinds = [c[0] for c in s.calls]
    assert kinds == ["create", "exists", "exists"]  # one create, re-verifies only
    assert "test-cluster-000000000000" in jm._redirect_schema_ensured_clusters


def test_qualified_timeout_falls_back_to_plain():
    """The 60s-timeout incident shape: first attempt raises, plain succeeds."""
    s = _Session([TimeoutError("execute timed out"), True])
    asyncio.run(jm._ensure_redirect_schema(s))
    kinds = [c[0] for c in s.calls]
    assert kinds == ["create", "create"]


def test_genuine_absence_still_raises_loudly():
    s = _Session([False] * 20)
    with pytest.raises(RuntimeError, match="could not be created/verified"):
        asyncio.run(jm._ensure_redirect_schema(s))
    # both create forms attempted, each followed by 3 bounded re-verifies
    kinds = [c[0] for c in s.calls]
    assert kinds.count("create") == 2
    assert kinds.count("exists") == 6


def test_second_call_same_cluster_is_cached_noop():
    s = _Session([True])
    asyncio.run(jm._ensure_redirect_schema(s))
    n = len(s.calls)
    asyncio.run(jm._ensure_redirect_schema(s))
    assert len(s.calls) == n  # no further kernel traffic

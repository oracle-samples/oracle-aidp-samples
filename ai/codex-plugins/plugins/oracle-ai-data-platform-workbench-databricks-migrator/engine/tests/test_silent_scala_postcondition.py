"""Fix B2 — silent %scala/%sql failure detection via declared-side-effect
post-condition.

B2 root cause (reverse_geocoding, top5_v5_inference): AIDP returns status='ok'
even when a %scala cell throws, and the failure text does not always match the
"Command ID .. failed with" / "Failing command:" output-scan wrappers. A cell
that declared `gl_data_1.createOrReplaceTempView("gl_data_1")` but silently
failed looked OK, and the error surfaced misleadingly on the downstream reader
(TABLE_OR_VIEW_NOT_FOUND). The post-condition verifies the declared temp view
actually registered; if not, the cell is marked failed so the existing retry
loop re-runs it (clearing a transient) instead of cascading.

These are source-level + mocked-session unit checks — no cluster, no Opus.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from job_migrate import _declared_registrations, _verify_registered  # noqa: E402



def _run(coro):
    return asyncio.run(coro)


class _FakeSession:
    """Minimal stand-in for a cluster session: execute() returns AIDP-shaped
    outputs carrying the TVCHK markers, or raises to exercise fail-open."""

    def __init__(self, markers: dict, raise_exc: bool = False):
        self._markers = markers            # name -> bool (omit a name to drop its marker)
        self._raise = raise_exc

    async def execute(self, code, timeout=60):
        if self._raise:
            raise RuntimeError("kernel unreachable")
        text = "".join(f"TVCHK::{n}::{present}\n" for n, present in self._markers.items())
        return {"status": "ok", "outputs": [{"type": "stream", "name": "stdout", "text": text}]}


class TestDeclaredRegistrations(unittest.TestCase):
    """Parsing the names a cell claims to register (reuses the temp-view regexes)."""

    def test_scala_create_or_replace_temp_view(self):
        src = ('%scala\n'
               'val gl_data=spark.read.parquet("oci://b@ns/p")\n'
               'gl_data.createOrReplaceTempView("gl_data")\n'
               'val gl_data_1=spark.sql("SELECT * FROM gl_data")\n'
               'gl_data_1.createOrReplaceTempView("gl_data_1")')
        self.assertEqual(_declared_registrations(src), {"gl_data", "gl_data_1"})

    def test_global_and_register_temp_table(self):
        src = ('df.createOrReplaceGlobalTempView("gv")\n'
               'other.registerTempTable("rt")')
        self.assertEqual(_declared_registrations(src), {"gv", "rt"})

    def test_sql_create_temp_view(self):
        src = "CREATE OR REPLACE TEMP VIEW myv AS SELECT 1"
        self.assertIn("myv", _declared_registrations(src))

    def test_dotted_name_takes_leaf(self):
        # createOrReplaceTempView("schema.name") — temp views are unqualified;
        # we track the leaf so the catalog check matches.
        src = 'x.createOrReplaceTempView("schema.leafname")'
        self.assertEqual(_declared_registrations(src), {"leafname"})

    def test_no_registration_returns_empty(self):
        self.assertEqual(_declared_registrations('spark.read.table("x").count()'), set())

    def test_empty_source(self):
        self.assertEqual(_declared_registrations(""), set())
        self.assertEqual(_declared_registrations(None), set())

    # ── Reviewer #1: commented-out registrations must NOT be declared ────────
    def test_scala_commented_registration_ignored(self):
        # a passing cell whose ONLY match is commented-out must declare nothing,
        # else B2 false-fails it and hands working Scala to Opus.
        src = ('%scala\n'
               '// legacy: resultDF.createOrReplaceTempView("legacy_view")\n'
               'df.createOrReplaceTempView("active_view")')
        self.assertEqual(_declared_registrations(src), {"active_view"})

    def test_python_hash_commented_registration_ignored(self):
        src = ('# old_df.createOrReplaceTempView("old_v")\n'
               'df.createOrReplaceTempView("new_v")')
        self.assertEqual(_declared_registrations(src), {"new_v"})

    def test_sql_dashdash_commented_registration_ignored(self):
        src = ('-- CREATE OR REPLACE TEMP VIEW commented_v AS SELECT 1\n'
               'CREATE OR REPLACE TEMP VIEW real_v AS SELECT 2')
        self.assertEqual(_declared_registrations(src), {"real_v"})

    def test_fully_commented_declares_nothing(self):
        src = '// df.createOrReplaceTempView("only_commented")'
        self.assertEqual(_declared_registrations(src), set())


class TestVerifyRegistered(unittest.TestCase):
    """Post-condition catalog check with a mocked session."""

    def test_false_marker_is_flagged_missing(self):
        # gl_data_1 silently failed to register -> flagged; gl_data present -> not.
        s = _FakeSession({"gl_data_1": False, "gl_data": True})
        self.assertEqual(_run(_verify_registered(s, {"gl_data_1", "gl_data"})), {"gl_data_1"})

    def test_all_present_none_missing(self):
        s = _FakeSession({"a": True, "b": True})
        self.assertEqual(_run(_verify_registered(s, {"a", "b"})), set())

    def test_failopen_when_marker_absent(self):
        # AIDP dropped the 'a' marker in stream output -> do NOT invent a failure.
        s = _FakeSession({"b": True})
        self.assertEqual(_run(_verify_registered(s, {"a", "b"})), set())

    def test_failopen_on_probe_exception(self):
        s = _FakeSession({}, raise_exc=True)
        self.assertEqual(_run(_verify_registered(s, {"a"})), set())

    def test_empty_names_no_probe(self):
        s = _FakeSession({})
        self.assertEqual(_run(_verify_registered(s, set())), set())

    def test_non_identifier_names_ignored(self):
        # defensive: only \w+ names are probed
        s = _FakeSession({"good": False})
        self.assertEqual(_run(_verify_registered(s, {"good", "bad.name", ""})), {"good"})


if __name__ == "__main__":
    unittest.main()

"""What a PASS verdict is allowed to claim.

`verify` grades an asset PASS when the translator raised no flags.  That
establishes "no known issue was detected" -- it does not establish that the
SQL runs, because nothing parses or executes the artifact.  The Athena
translator has no parser, and acquiring one offline is not possible without
a target catalog for name resolution.

Overstating PASS as "runnable" is what turns every uncovered construct into a
false clean result, so these tests pin the wording at the three places a user
actually reads it: the CLI summary, the Markdown report and the HTML report.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from aws_aidp.migrate.html_report import render
from aws_aidp.verify.checker import format_verify

_OVERCLAIMS = ("runnable", "ready", "guaranteed", "verified to run")


def _verify_output(passes: int = 3) -> str:
    return format_verify({
        "summary": {"PASS": passes, "REVIEW": 1, "SKIP": 0, "FAIL": 0},
        "rows": [
            {"asset_id": "athena.query.q1", "verdict": "PASS",
             "changes": 1, "flags": 0, "note": ""},
            {"asset_id": "athena.query.q2", "verdict": "REVIEW",
             "changes": 0, "flags": 1, "note": ""},
        ],
    })


class VerifySummaryClaimTests(unittest.TestCase):
    def test_summary_states_that_pass_is_not_execution_verified(self):
        text = _verify_output().lower()
        self.assertIn("not execution-verified", text)

    def test_summary_does_not_claim_pass_means_runnable(self):
        text = _verify_output().lower()
        for word in _OVERCLAIMS:
            with self.subTest(word=word):
                self.assertNotIn(word, text)

    def test_counts_are_still_reported(self):
        text = _verify_output()
        self.assertIn("PASS:   3", text)
        self.assertIn("REVIEW: 1", text)


class HtmlReportClaimTests(unittest.TestCase):
    def _report(self) -> dict:
        return {
            "report_id": "r1",
            "plan_id": "p1",
            "mode": "demo",
            "filter": "all",
            "migrated_at": "2026-09-14T00:00:00Z",
            "counts": {"ok": 2, "needs_manual_review": 1,
                       "planned": 0, "skipped": 0, "error": 0},
            "results": [
                {"asset_id": "athena.query.q1", "kind": "athena_query",
                 "status": "ok", "changes": 1, "flags": 0,
                 "output_path": "athena/q1.spark.sql", "findings": [],
                 "source_sql": "SELECT 1", "translated_sql": "SELECT 1"},
            ],
        }

    def _html(self) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            out = render(self._report(), Path(tmp) / "report.html")
            return Path(out).read_text(encoding="utf-8")

    def test_html_card_does_not_label_a_clean_result_ready(self):
        self.assertNotIn("ready", self._html().lower())

    def test_html_states_the_limit_of_a_clean_result(self):
        self.assertIn("not execution-verified", self._html().lower())

    def test_html_is_written_as_utf8(self):
        """The header carries a non-ASCII arrow.

        Windows' default cp1252 raises UnicodeEncodeError on it, and the
        caller wraps render() in `except Exception: pass` -- so without an
        explicit encoding the report silently never appears on Windows.
        The Windows CI lane is the real enforcement; this pins the intent.
        """
        with tempfile.TemporaryDirectory() as tmp:
            out = render(self._report(), Path(tmp) / "report.html")
            raw = Path(out).read_bytes()
            raw.decode("utf-8")                      # must be valid utf-8
            self.assertIn("→".encode("utf-8"), raw)


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Fix C (core) — _detect_connection_died must catch BOTH forms of a WS drop.

Root cause (top5_v5_inference rule_engine cell 46): AIDP surfaced the WS drop as a
STRUCTURED error field ({'type':'error','ename':'ConnectionDied',
'evalue':'WebSocket connection died during execution'}) with an EMPTY rendered
output/issue_summary. The original detector only string-grepped output/issue_summary,
so the structured-only drop slipped past → the skip-handler never fired → Opus/
fixup_cell ground for ~27 min rebuilding wiped kernel state. This guards that the
structured form is now detected (while keeping the text form).
"""

from __future__ import annotations

import pathlib
import sys
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from job_migrate import _detect_connection_died  # noqa: E402


class TestDetectConnectionDied(unittest.TestCase):
    def test_text_in_output(self):
        self.assertTrue(_detect_connection_died("… ConnectionDied …", "", []))

    def test_text_in_issue_summary(self):
        self.assertTrue(_detect_connection_died("", "WebSocket connection died during execution", []))
        self.assertTrue(_detect_connection_died("", "ConnectionDied", []))

    def test_structured_ename_with_empty_text(self):
        # THE BUG CASE: rendered output + issue_summary empty, error only in raw_outputs
        raw = [{"type": "error", "ename": "ConnectionDied",
                "evalue": "WebSocket connection died during execution"}]
        self.assertTrue(_detect_connection_died("", "", raw))

    def test_structured_evalue_only(self):
        raw = [{"type": "error", "ename": "Py4JError",
                "evalue": "… WebSocket connection died …"}]
        self.assertTrue(_detect_connection_died("", "", raw))

    def test_unrelated_error_is_false(self):
        raw = [{"type": "error", "ename": "AnalysisException",
                "evalue": "[TABLE_OR_VIEW_NOT_FOUND] gl_data_1"}]
        self.assertFalse(_detect_connection_died("", "Error pattern in output: 'Traceback'", raw))

    def test_all_empty_is_false(self):
        self.assertFalse(_detect_connection_died("", "", []))
        self.assertFalse(_detect_connection_died(None, None, None))

    def test_non_dict_raw_outputs_no_crash(self):
        self.assertFalse(_detect_connection_died("", "", ["str-output", None, 42]))


if __name__ == "__main__":
    unittest.main()

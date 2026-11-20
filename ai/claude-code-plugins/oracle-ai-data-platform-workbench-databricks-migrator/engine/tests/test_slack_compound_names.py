r"""Fix 7o''' (2026-07-28) — Slack Tier-1 broadening for compound verb+slack names.

Root cause: the original Tier-1 pattern `\bsend_slack\s*\(` required the literal
prefix `send_slack`, so the pilot deployment's customer helper `send_data_to_slack(...)` slipped
through — verb (`send`) and target (`slack`) are separated by `_data_to_`.

The Example_Txn_Budget_Monitoring/01_metric_deviation_tracking Cell 10 was:

    send_data_to_slack(
        file_name=None,
        comment=result_str,
        c_id=get_slack_channel_test()
    )

This fired an actual Slack notification during migration validation. The fix
broadens Tier-1 to catch verb+slack and slack+verb compound names.

These tests extract `_SLACK_TIER1_RE` and `_neutralize_slack_execution` by
AST-parsing the source (avoids importing the heavy module).
"""

from __future__ import annotations

import ast
import os
import re
import sys
import textwrap
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB_MIGRATE = os.path.join(REPO, "scripts", "job_migrate.py")


def _extract_slack_pieces():
    """Extract Slack helpers from job_migrate.py without importing the module."""
    src = open(JOB_MIGRATE).read()
    tree = ast.parse(src)

    wanted_names = {
        "_SLACK_TIER1_RE",
        "_SLACK_TIER2_RE",
        "_SLACK_EVIDENCE_MARKERS",
        "_SLACK_ID_RE",
        "_SLACK_CLIENT_BINDING_RE",
        "_SLACK_RECEIVER_HINT_RE",
    }
    wanted_funcs = {
        "_active_lines",
        "_has_slack_evidence",
        "_get_receiver_before",
        "_neutralize_slack_execution",
        "_match_close_paren",
    }

    pieces = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in wanted_names:
                    pieces.append(ast.get_source_segment(src, node))
        elif isinstance(node, ast.FunctionDef):
            if node.name in wanted_funcs:
                pieces.append(ast.get_source_segment(src, node))

    ns: dict = {
        "re": re,
        "List": list,
        "Optional": type(None),
        "Tuple": tuple,
    }
    # Provide the module-level Optional/Tuple that appear in annotations.
    from typing import List, Optional, Tuple  # noqa: F401
    ns["List"] = List
    ns["Optional"] = Optional
    ns["Tuple"] = Tuple

    exec("\n\n".join(pieces), ns)
    return ns


class TestCompoundVerbSlackCallsMatchTier1(unittest.TestCase):
    """Positive: the broadened Tier-1 pattern MUST catch compound names."""

    @classmethod
    def setUpClass(cls):
        cls.ns = _extract_slack_pieces()
        cls.tier1 = cls.ns["_SLACK_TIER1_RE"]
        cls.neutralize = staticmethod(cls.ns["_neutralize_slack_execution"])

    # ────────────── verb + slack name shapes ──────────────

    def test_send_data_to_slack_regex_matches(self):
        self.assertIsNotNone(self.tier1.search("send_data_to_slack(x, y)"))

    def test_send_data_to_slack_gets_commented(self):
        """The exact ExampleVendor Cell 10 shape — multi-line."""
        src = textwrap.dedent("""\
            send_data_to_slack(
                file_name=None,
                comment=result_str,
                c_id=get_slack_channel_test()
            )
        """)
        result, n = self.neutralize(src, include_generic=False)
        self.assertGreater(n, 0, "Multi-line send_data_to_slack(...) must be commented")
        # ALL lines of the call, not just the header
        self.assertIn("# send_data_to_slack(", result)
        self.assertIn("# file_name=None,", result)
        self.assertIn("# c_id=get_slack_channel_test()", result)
        self.assertIn("# )", result)

    def test_send_slack_still_matches(self):
        """Backwards compat — the original `send_slack(` MUST still match."""
        self.assertIsNotNone(self.tier1.search("send_slack('hi')"))

    def test_send_slack_msg_matches(self):
        self.assertIsNotNone(self.tier1.search("send_slack_msg(text)"))

    def test_post_to_slack_matches(self):
        self.assertIsNotNone(self.tier1.search("post_to_slack(payload)"))

    def test_notify_slack_channel_matches(self):
        self.assertIsNotNone(self.tier1.search("notify_slack_channel(ch)"))

    def test_push_slack_matches(self):
        self.assertIsNotNone(self.tier1.search("push_slack(msg)"))

    def test_publish_to_slack_matches(self):
        self.assertIsNotNone(self.tier1.search("publish_to_slack(evt)"))

    def test_broadcast_slack_matches(self):
        self.assertIsNotNone(self.tier1.search("broadcast_slack(all_users)"))

    def test_deliver_via_slack_matches(self):
        # verb + `_via_` + slack = still verb\w*slack
        self.assertIsNotNone(self.tier1.search("deliver_via_slack(m)"))

    # ────────────── slack + verb name shapes ──────────────

    def test_slack_send_matches(self):
        self.assertIsNotNone(self.tier1.search("slack_send(x)"))

    def test_slack_notify_matches(self):
        self.assertIsNotNone(self.tier1.search("slack_notify(m)"))

    def test_slack_notification_matches(self):
        self.assertIsNotNone(self.tier1.search("slack_notification(m)"))

    def test_slack_message_matches(self):
        self.assertIsNotNone(self.tier1.search("slack_message(m)"))

    def test_slack_alert_still_matches(self):
        """Backwards compat."""
        self.assertIsNotNone(self.tier1.search("slack_alert('warning')"))

    def test_slack_api_matches(self):
        self.assertIsNotNone(self.tier1.search("slack_api(payload)"))

    def test_slack_call_matches(self):
        self.assertIsNotNone(self.tier1.search("slack_call(method, args)"))


class TestNegativeCasesNotOverMatched(unittest.TestCase):
    """Negative: things that LOOK like Slack but aren't sending — must NOT match."""

    @classmethod
    def setUpClass(cls):
        cls.ns = _extract_slack_pieces()
        cls.tier1 = cls.ns["_SLACK_TIER1_RE"]
        cls.neutralize = staticmethod(cls.ns["_neutralize_slack_execution"])

    def test_get_slack_channel_test_not_matched(self):
        """`get_slack_channel_test()` READS a channel ID — not a send verb."""
        self.assertIsNone(self.tier1.search("get_slack_channel_test()"))

    def test_get_slack_config_not_matched(self):
        self.assertIsNone(self.tier1.search("get_slack_config()"))

    def test_read_slack_settings_not_matched(self):
        """`read` isn't in the send-verb list."""
        self.assertIsNone(self.tier1.search("read_slack_settings()"))

    def test_load_slack_creds_not_matched(self):
        self.assertIsNone(self.tier1.search("load_slack_creds()"))

    def test_slack_config_variable_not_matched(self):
        """`slack_config` is a variable/attr access, not a call."""
        self.assertIsNone(self.tier1.search("val = slack_config['token']"))

    def test_slack_channel_id_not_matched(self):
        """Attribute access on a slack-named variable — not a call."""
        self.assertIsNone(self.tier1.search("cid = slack_channel_id"))

    def test_slack_helper_module_import_not_matched(self):
        """Import lines never match Tier-1 (no `(` follows)."""
        self.assertIsNone(self.tier1.search("from slack_helpers import send_msg"))

    def test_function_definition_call_line_matches_but_def_scope_protects(self):
        """Regex matches the string, but scope tracker skips lines inside def block."""
        src = textwrap.dedent("""\
            def send_data_to_slack(msg):
                # module-scope call INSIDE the def — must NOT be commented
                internal_send_data_to_slack(msg)
        """)
        _, n = self.neutralize(src, include_generic=False)
        self.assertEqual(n, 0,
            "Calls INSIDE a def block must NOT be commented (scope tracker's job).")


class TestScopeTrackingWithCompoundNames(unittest.TestCase):
    """Verify scope tracker still works — def block content is preserved."""

    @classmethod
    def setUpClass(cls):
        cls.ns = _extract_slack_pieces()
        cls.neutralize = staticmethod(cls.ns["_neutralize_slack_execution"])

    def test_module_scope_commented_def_body_preserved(self):
        src = textwrap.dedent("""\
            def send_data_to_slack(msg, channel):
                # This is the CUSTOMER'S HELPER — do not touch its body
                r = requests.post('http://internal/slack', json={'m': msg})
                return r.status_code

            # Module-scope call — this MUST be commented
            send_data_to_slack('hello', 'C123')
        """)
        result, n = self.neutralize(src, include_generic=False)
        self.assertGreater(n, 0)
        # Definition body preserved
        self.assertIn("r = requests.post", result)
        # Module-scope call commented
        self.assertIn("# send_data_to_slack('hello', 'C123')", result)


class TestExactExampleVendorCell10Reproduces(unittest.TestCase):
    """END-TO-END regression: the LITERAL cell content that fired the Slack
    notification during Example_Txn_Budget_Monitoring/01_metric_deviation_tracking.

    From /tmp/example.log at 16:34:35:
        [WS] send execute_request msg_id=cedd9bb8 code='send_data_to_slack(
            file_name=None,
            comment=result_str,
            c_id=get_slack_channel_test()
        )'

    This test EXECUTES the neutralization function against that exact cell
    content and asserts the send is commented. If this test passes, the
    ExampleVendor Task 1 Cell 10 case cannot regress silently.
    """

    @classmethod
    def setUpClass(cls):
        cls.ns = _extract_slack_pieces()

    def test_examplevendor_cell10_literal_shape_gets_neutralized(self):
        # EXACT text from the log — do not reformat
        cell_source = (
            'send_data_to_slack(\n'
            '    file_name=None, \n'
            '    comment=result_str, \n'
            '    c_id=get_slack_channel_test()\n'
            ')\n'
        )
        neutralize = self.ns["_neutralize_slack_execution"]
        has_ev = self.ns["_has_slack_evidence"]

        # Simulate the exact call path in job_migrate.py line ~8535:
        #     _has_slack_ev = _has_slack_evidence(source)
        #     _neutralized, n = _neutralize_slack_execution(source, include_generic=_has_slack_ev)
        _has_slack_ev = has_ev(cell_source)
        neutralized, n_commented = neutralize(cell_source, include_generic=_has_slack_ev)

        # 1) At least one line commented — the actual bug we're fixing
        self.assertGreater(
            n_commented, 0,
            "REGRESSION: send_data_to_slack(...) not commented. "
            "This is the exact case that caused Slack noise on 2026-07-28. "
            f"Actual neutralized source:\n{neutralized}"
        )

        # 2) All FOUR lines of the multi-line call commented
        self.assertIn("# send_data_to_slack(", neutralized)
        self.assertIn("# file_name=None,", neutralized)
        self.assertIn("# comment=result_str,", neutralized)
        self.assertIn("# c_id=get_slack_channel_test()", neutralized)
        self.assertIn("# )", neutralized)

        # 3) The Slack SEND cannot survive as active code — nothing outside
        # a comment should call send_data_to_slack
        active_lines = [
            ln for ln in neutralized.splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
        for ln in active_lines:
            self.assertNotIn(
                "send_data_to_slack(", ln,
                f"REGRESSION: send_data_to_slack still active on line: {ln!r}"
            )


class TestHasSlackEvidenceCoverage(unittest.TestCase):
    """Evidence detection MUST also fire for compound-name calls so Tier-2 (`.send(`,
    `.api_call(`, `.post_message(`) engages in mixed cells. Since Tier-1 now catches
    the compound-name call itself, evidence isn't strictly required for the direct
    call — but a cell that has `send_data_to_slack(...)` alongside a Kafka
    `producer.send(...)` should still gate Tier-2 correctly."""

    @classmethod
    def setUpClass(cls):
        cls.ns = _extract_slack_pieces()

    def test_compound_name_call_triggers_evidence_via_tier1_marker_family(self):
        """A cell with only `send_data_to_slack(...)` has the substring `slack`
        (once compound Tier-1 catches it, Tier-2 gating is not the failure mode).
        This test just verifies existing evidence markers still work."""
        has_ev = self.ns["_has_slack_evidence"]
        self.assertTrue(has_ev("from slack_sdk import WebClient\nclient = WebClient(t)"))
        self.assertTrue(has_ev("hooks.slack.com/services/T00/B00/x"))


if __name__ == "__main__":
    unittest.main()

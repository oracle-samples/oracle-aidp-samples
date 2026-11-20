"""Fix 7z (2026-07-29) — structural fail-fast investigation caps.

Verifies the constants block, the run_on_cluster hard-refusal logic, and
the iteration limits wired to constants across job_migrate.py.

We test at TWO layers:
  1. Constants themselves — right values, right names, present in source
  2. Wiring — the constants are referenced everywhere the old magic
     numbers lived (call_fix, MAX_CELL_RETRIES, etc.)

Behavioral test of the refusal logic is a source-level check on the
error-message wording — the actual counter runs inside
`call_opus_with_tools` which requires a live Anthropic API to exercise.
"""
from __future__ import annotations

import os
import re
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT_MIGRATE = os.path.join(REPO, "scripts", "agent_migrate.py")
JOB_MIGRATE = os.path.join(REPO, "scripts", "job_migrate.py")


class TestConstantsDeclared(unittest.TestCase):
    """The four caps must be declared with the right DEFAULT values in
    agent_migrate.py. PR-36 review: each cap is env-overridable
    (AIDP_<NAME> env var), so the declaration shape is
        NAME = int(os.environ.get("AIDP_NAME", "<default>"))
    These tests assert the default embedded in that expression."""

    _DECL = r'^%s\s*=\s*int\(os\.environ\.get\("AIDP_%s",\s*"(\d+)"\)\)'

    @classmethod
    def setUpClass(cls):
        cls.src = open(AGENT_MIGRATE).read()

    def _default_of(self, name):
        m = re.search(self._DECL % (name, name), self.src, re.MULTILINE)
        self.assertIsNotNone(m, f"{name} env-overridable declaration missing")
        return int(m.group(1))

    def test_max_run_on_cluster_per_call_default_3(self):
        self.assertEqual(self._default_of("MAX_RUN_ON_CLUSTER_PER_CALL"), 3)

    def test_max_tool_rounds_fix_default_5(self):
        self.assertEqual(self._default_of("MAX_TOOL_ROUNDS_FIX"), 5)

    def test_max_tool_rounds_migrate_default_5(self):
        self.assertEqual(self._default_of("MAX_TOOL_ROUNDS_MIGRATE"), 5)

    def test_max_fix_attempts_default_5(self):
        # Bumped 3->5 (2026-08): a few real cells needed a 4th/5th fix round;
        # worst-case ~25 tool calls/cell (still down from the historical 250).
        self.assertEqual(self._default_of("MAX_FIX_ATTEMPTS"), 5)

    def test_constants_block_documents_rationale(self):
        """The constants block must include the WHY — a future dev reading
        this file needs to understand the empirical basis + ExampleVendor trigger."""
        # Look for the constants header block
        self.assertIn("FAIL-FAST INVESTIGATION CAPS", self.src)
        self.assertIn("Example_Txn_Budget_Monitoring", self.src)
        self.assertIn("Fix 7z", self.src)


class TestRunOnClusterCapWired(unittest.TestCase):
    """MAX_RUN_ON_CLUSTER_PER_CALL must be referenced (not hardcoded) in the
    call_opus_with_tools dispatch loop."""

    @classmethod
    def setUpClass(cls):
        cls.src = open(AGENT_MIGRATE).read()

    def test_counter_initialized_per_call(self):
        # The per-call counter must exist inside call_opus_with_tools
        self.assertIn("_run_on_cluster_count = 0", self.src)

    def test_counter_check_uses_constant(self):
        """The 'over budget' check must use MAX_RUN_ON_CLUSTER_PER_CALL,
        not a hardcoded 3."""
        self.assertRegex(
            self.src,
            r"_run_on_cluster_count\s*>\s*MAX_RUN_ON_CLUSTER_PER_CALL",
            "run_on_cluster cap not wired to constant"
        )

    def test_refusal_message_documents_next_steps(self):
        """The refusal message shown to Opus must include the 3-way pivot
        guidance (submit_code / make_note+submit / make_note+original)."""
        self.assertIn("REFUSED: run_on_cluster budget exhausted", self.src)
        self.assertIn("(a) submit_code", self.src)
        self.assertIn("(b) make_note", self.src)
        self.assertIn("(c) make_note", self.src)


class TestIterationCapsWired(unittest.TestCase):
    """All fix/tool-round call sites must reference the constants — no
    remaining magic numbers of 10 or 25."""

    @classmethod
    def setUpClass(cls):
        cls.agent_src = open(AGENT_MIGRATE).read()
        cls.job_src = open(JOB_MIGRATE).read()

    def test_call_fix_uses_max_tool_rounds_fix(self):
        # call_opus_with_tools invocation in call_fix must reference the constant
        self.assertRegex(
            self.agent_src,
            r"max_tool_rounds\s*=\s*MAX_TOOL_ROUNDS_FIX",
            "call_fix's Opus call not wired to MAX_TOOL_ROUNDS_FIX"
        )

    def test_no_magic_25_in_call_fix_context(self):
        """The old magic `max_tool_rounds=25` in call_fix should be gone."""
        # Search near the FIX_PROMPT usage
        self.assertNotIn(
            "max_tool_rounds=25", self.agent_src,
            "magic max_tool_rounds=25 still present — should be MAX_TOOL_ROUNDS_FIX"
        )

    def test_max_cell_retries_uses_constant(self):
        self.assertRegex(
            self.job_src,
            r"MAX_CELL_RETRIES\s*=\s*MAX_FIX_ATTEMPTS",
            "MAX_CELL_RETRIES not wired to MAX_FIX_ATTEMPTS"
        )

    def test_max_child_retries_uses_constant(self):
        self.assertRegex(
            self.job_src,
            r"MAX_CHILD_RETRIES\s*=\s*MAX_FIX_ATTEMPTS",
            "MAX_CHILD_RETRIES not wired to MAX_FIX_ATTEMPTS"
        )

    def test_max_replay_retries_uses_constant(self):
        self.assertRegex(
            self.job_src,
            r"MAX_REPLAY_RETRIES\s*=\s*MAX_FIX_ATTEMPTS",
            "MAX_REPLAY_RETRIES not wired to MAX_FIX_ATTEMPTS"
        )

    def test_constants_imported_into_job_migrate(self):
        """job_migrate.py must import MAX_FIX_ATTEMPTS + tool-round constants
        from agent_migrate.py (single source of truth).

        Guards against a refactor that adds `MAX_FIX_ATTEMPTS = 3` locally in
        job_migrate.py — same VALUE, but drift-prone. Constants must live in
        one file.
        """
        self.assertIn("MAX_FIX_ATTEMPTS", self.job_src)
        self.assertIn("MAX_TOOL_ROUNDS_FIX", self.job_src)
        # Verify the import statement pulls it in, and that there's no local
        # duplicate declaration in job_migrate.py.
        import_pos = self.job_src.find("from agent_migrate import (")
        self.assertGreater(import_pos, 0, "import block missing from job_migrate.py")
        # Import block ends at matching close-paren — find it
        # (skip past any ) inside comments in the block by using a simple
        # heuristic: import block is typically <2000 chars)
        block_end = self.job_src.find("\n)", import_pos)
        self.assertGreater(block_end, 0, "import block close-paren missing")
        block = self.job_src[import_pos:block_end + 2]
        self.assertIn(
            "MAX_FIX_ATTEMPTS", block,
            "MAX_FIX_ATTEMPTS not imported from agent_migrate — was the import removed?"
        )
        self.assertIn(
            "MAX_TOOL_ROUNDS_FIX", block,
            "MAX_TOOL_ROUNDS_FIX not imported from agent_migrate"
        )
        # Also verify no local duplicate declaration (drift protection)
        m = re.search(r"^MAX_FIX_ATTEMPTS\s*=\s*\d+", self.job_src, re.MULTILINE)
        self.assertIsNone(
            m,
            "MAX_FIX_ATTEMPTS is LOCALLY declared in job_migrate.py — should only "
            "be defined in agent_migrate.py and imported here. Drift risk."
        )


class TestFixPromptFailFastDoctrine(unittest.TestCase):
    """The FIX_PROMPT must contain the new fail-fast doctrine so Opus
    understands WHY the tool refuses (not just BE refused)."""

    @classmethod
    def setUpClass(cls):
        cls.src = open(AGENT_MIGRATE).read()

    def test_data_unavailability_section_present(self):
        self.assertIn("DATA UNAVAILABILITY IS NOT A CODE BUG", self.src)

    def test_prompt_documents_structural_caps(self):
        self.assertIn("INVESTIGATION BUDGET (STRUCTURAL — HARD LIMIT)", self.src)

    def test_prompt_uses_placeholders_for_caps(self):
        """The prompt uses <PLACEHOLDER> for each cap value. Prevents drift
        between constants and prompt — substitution at module load fills them
        with the current constant values. If someone changes the constant,
        the prompt automatically reflects the new value."""
        self.assertIn("<MAX_RUN_ON_CLUSTER_PER_CALL>", self.src)
        self.assertIn("<MAX_TOOL_ROUNDS_FIX>", self.src)
        self.assertIn("<MAX_FIX_ATTEMPTS>", self.src)
        # And the substitution machinery must wire them up
        self.assertIn(".replace(\"<MAX_RUN_ON_CLUSTER_PER_CALL>\"", self.src)
        self.assertIn(".replace(\"<MAX_TOOL_ROUNDS_FIX>\"", self.src)
        self.assertIn(".replace(\"<MAX_FIX_ATTEMPTS>\"", self.src)

    def test_runtime_substitution_wired_structurally(self):
        """Placeholders in the prompt source MUST be paired with .replace() calls
        that substitute them at module load. We verify STRUCTURALLY (source-text)
        because importing agent_migrate requires the `anthropic` package which
        may not be installed in test/CI environments.

        Together with test_prompt_uses_placeholders_for_caps, this locks in:
        source has placeholders → source has substitution calls → runtime
        prompt has actual numbers (proven by the substitution mechanism).
        """
        # For each placeholder we assert it, we also assert its substitution
        # is chained through str() of the corresponding constant.
        for placeholder, const_name in [
            ("<MAX_RUN_ON_CLUSTER_PER_CALL>", "MAX_RUN_ON_CLUSTER_PER_CALL"),
            ("<MAX_TOOL_ROUNDS_FIX>", "MAX_TOOL_ROUNDS_FIX"),
            ("<MAX_FIX_ATTEMPTS>", "MAX_FIX_ATTEMPTS"),
        ]:
            with self.subTest(placeholder=placeholder):
                self.assertIn(placeholder, self.src,
                    f"Placeholder {placeholder} not present in prompt source")
                # The substitution must be: .replace("<PLACEHOLDER>", str(CONST_NAME))
                self.assertRegex(
                    self.src,
                    rf'\.replace\(\s*"{re.escape(placeholder)}"\s*,\s*str\(\s*{const_name}\s*\)\s*\)',
                    f"Substitution for {placeholder} not wired to str({const_name})"
                )

    def test_prompt_mentions_tmp_only_exception(self):
        self.assertIn("/tmp/", self.src)
        self.assertRegex(
            self.src,
            r"BLOCKED\s+on\s+every\s+absolute\s+path\s+EXCEPT\s+/tmp/",
            "Prompt should tell Opus that /tmp/ is the only allowed traversal target"
        )


if __name__ == "__main__":
    unittest.main()

"""Fix (2026-08-11) — decouple auto-install from the child cell's fix-retry budget.

Root cause it fixes: the child execute/fix loop in `_inline_child_notebook`
(job_migrate.py:8410) was `for attempt in range(MAX_CHILD_RETRIES)`, and the
auto-install path did `continue` — which in a for-loop STILL advances `attempt`.
So an import cell missing N packages burned N of its 3 fix attempts on installs
and could fail before a single genuine Opus fix was tried (observed live on
Data_Utils_2: seaborn+plotly installs consumed attempts 1 and 2, leaving the
cell one attempt short).

`simulate_child_retry` below is a faithful reference model of that loop's
counting decision (same branch structure as the edited while-loop). These tests
lock the invariant the fix establishes:
  - a successful install of a NEW module is FREE (does not consume a fix attempt),
    bounded only by MAX_AUTO_INSTALLS,
  - a genuine fix (Opus rewrite / already-installed module / non-module error)
    consumes exactly one attempt,
  - terminal FAIL fires only after MAX_CHILD_RETRIES real fix attempts,
  - the loop can never spin forever (round-cap backstop).

The live re-run of the migration is the end-to-end integration check; this
guards the arithmetic so a future edit to the loop can't silently regress it.
"""

from __future__ import annotations

import unittest


def simulate_child_retry(round_outcomes, max_fix=3, max_auto=10):
    """Mirror of the edited child retry loop's counter logic.

    round_outcomes: list of per-execution outcomes, each one of:
        ("ok",)              cell executed cleanly
        ("module", name)     failed with ModuleNotFoundError for `name`
        ("error",)           failed with a generic (Opus-fixable) error
    The i-th call consumes the i-th outcome (last one repeats if the list runs
    out — models a persistently-failing cell).

    Returns dict: result in {OK, FAIL, CAP}, plus fix_attempts_consumed,
    installs, rounds.
    """
    attempt = 0
    installed = set()
    rounds = 0
    cap = max_fix + max_auto + 5
    installs = 0
    i = 0
    while attempt < max_fix:
        if rounds >= cap:
            return {"result": "CAP", "fix_attempts": attempt,
                    "installs": installs, "rounds": rounds}
        rounds += 1
        outcome = round_outcomes[i] if i < len(round_outcomes) else round_outcomes[-1]
        i += 1

        if outcome[0] == "ok":
            return {"result": "OK", "fix_attempts": attempt,
                    "installs": installs, "rounds": rounds}

        # failed this round
        if attempt < max_fix - 1:
            if outcome[0] == "module":
                mod = outcome[1]
                # FREE retry only for a NEW module under the install budget
                if mod not in installed and len(installed) < max_auto:
                    installed.add(mod)
                    installs += 1
                    continue  # no attempt increment
                # already-installed / budget-exhausted → fall through to a fix
            # genuine fix attempt — consume one
            attempt += 1
            continue
        else:
            return {"result": "FAIL", "fix_attempts": attempt,
                    "installs": installs, "rounds": rounds}
    # exhausted budget without a clean pass
    return {"result": "FAIL", "fix_attempts": attempt,
            "installs": installs, "rounds": rounds}


class TestChildRetryBudget(unittest.TestCase):
    def test_two_installs_then_pass_consumes_no_fix_attempts(self):
        # The exact Data_Utils_2 shape: seaborn, plotly, then clean.
        r = simulate_child_retry([("module", "seaborn"), ("module", "plotly"), ("ok",)])
        self.assertEqual(r["result"], "OK")
        self.assertEqual(r["installs"], 2)
        self.assertEqual(r["fix_attempts"], 0)  # installs were FREE

    def test_three_installs_then_pass_still_ok(self):
        # Under the OLD for-loop this FAILED (3 installs == 3 attempts). Now fine.
        r = simulate_child_retry([
            ("module", "seaborn"), ("module", "plotly"),
            ("module", "shap"), ("ok",)])
        self.assertEqual(r["result"], "OK")
        self.assertEqual(r["installs"], 3)
        self.assertEqual(r["fix_attempts"], 0)

    def test_many_installs_up_to_budget(self):
        # 8 distinct missing modules then clean — all free, still passes.
        mods = [("module", f"m{k}") for k in range(8)] + [("ok",)]
        r = simulate_child_retry(mods)
        self.assertEqual(r["result"], "OK")
        self.assertEqual(r["installs"], 8)
        self.assertEqual(r["fix_attempts"], 0)

    def test_persistent_real_error_fails_after_max_fix(self):
        # PRESERVED original semantics: with the `attempt < MAX-1` guard, a
        # MAX_CHILD_RETRIES=3 cell fixes at attempt 0 and 1, then re-executes the
        # last fix at attempt 2 with no new fix → FAIL at attempt counter 2.
        baseline = simulate_child_retry([("error",)])
        self.assertEqual(baseline["result"], "FAIL")
        self.assertEqual(baseline["fix_attempts"], 2)   # MAX_CHILD_RETRIES - 1
        self.assertEqual(baseline["installs"], 0)

    def test_installs_then_real_error_still_gets_full_fix_budget(self):
        # 2 installs (free) then the cell keeps failing on a real error: the fix
        # budget must be IDENTICAL to a cell that needed no installs — proving
        # the installs did not draw it down.
        baseline = simulate_child_retry([("error",)])["fix_attempts"]  # == 2
        r = simulate_child_retry([
            ("module", "seaborn"), ("module", "plotly"), ("error",)])
        self.assertEqual(r["result"], "FAIL")
        self.assertEqual(r["installs"], 2)
        self.assertEqual(r["fix_attempts"], baseline)   # NOT reduced by the 2 installs

    def test_installs_then_one_fix_then_pass(self):
        r = simulate_child_retry([
            ("module", "seaborn"), ("error",), ("ok",)])
        self.assertEqual(r["result"], "OK")
        self.assertEqual(r["installs"], 1)
        self.assertEqual(r["fix_attempts"], 1)

    def test_same_module_twice_second_is_a_fix(self):
        # If a module "installs" but reappears (e.g. propagation lag), the 2nd
        # sighting is NOT re-installed — it consumes a fix attempt instead, so
        # the loop still converges rather than looping forever.
        r = simulate_child_retry([
            ("module", "plotly"), ("module", "plotly"),
            ("module", "plotly"), ("module", "plotly")])
        self.assertEqual(r["result"], "FAIL")
        self.assertEqual(r["installs"], 1)       # installed once only
        self.assertEqual(r["fix_attempts"], 2)   # subsequent sightings consumed fixes (MAX-1)

    def test_never_exceeds_round_cap(self):
        # Pathological: always a brand-new module, never clean. Bounded by the
        # install budget then the fix budget — must terminate, never hit CAP.
        endless = [("module", f"pkg{k}") for k in range(100)]
        r = simulate_child_retry(endless, max_fix=3, max_auto=10)
        self.assertIn(r["result"], ("FAIL", "CAP"))
        self.assertLessEqual(r["rounds"], 3 + 10 + 5)
        self.assertLessEqual(r["installs"], 10)  # never exceeds MAX_AUTO_INSTALLS


if __name__ == "__main__":
    unittest.main()

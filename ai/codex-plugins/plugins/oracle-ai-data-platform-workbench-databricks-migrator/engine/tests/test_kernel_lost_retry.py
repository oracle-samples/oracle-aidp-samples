"""Fix C (recovery half) — whole-task retry-once-then-fail on mid-task kernel loss.

When a ConnectionDied wipes the kernel mid-task, ClusterSession creates a fresh
session (_state_loss_count++), process_notebook returns status
CONNECTION_DIED_KERNEL_LOST, and `_run_task_with_kernel_retry` re-runs the WHOLE
task ONCE on a clean kernel; if it recurs, the migration FAILs (submitter restarts).
A same-kernel WS reconnect never yields that status, so the long-shuffle skip-cell
path is unaffected.

Unit checks on the retry state machine with a fake run_once (no cluster, no Opus).
"""

from __future__ import annotations

import asyncio
import pathlib
import sys
import unittest


REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import job_migrate  # noqa: E402
from job_migrate import _run_task_with_kernel_retry, _kernel_state_lost  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class TestKernelStateLostGate(unittest.TestCase):
    """Fix C gate — the decision that a mid-task kernel wipe warrants a whole-task
    retry. MUST NOT fire when the --skip-cells-on-connection-died handler already
    handled the drop (long-shuffle regression the review caught)."""

    def test_true_when_state_lost_in_execution_and_not_skipped(self):
        self.assertTrue(_kernel_state_lost(run_all=True, state_loss_delta=1,
                                           connection_died_skipped=False))

    def test_false_when_skip_cells_fired(self):
        # REGRESSION GUARD: a long shuffle trips both skip-cells AND a fresh-session
        # (execution-hung escalation). Must defer to skip-cells, NOT retry+FAIL.
        self.assertFalse(_kernel_state_lost(run_all=True, state_loss_delta=1,
                                            connection_died_skipped=True))

    def test_false_when_no_state_loss(self):
        self.assertFalse(_kernel_state_lost(run_all=True, state_loss_delta=0,
                                            connection_died_skipped=False))

    def test_false_when_not_execution_mode(self):
        # code-only deps never execute cells → can't lose kernel state
        self.assertFalse(_kernel_state_lost(run_all=False, state_loss_delta=3,
                                            connection_died_skipped=False))


def _factory(statuses):
    """Return (run_once, calls) where run_once yields the next status each call."""
    seq = list(statuses)
    calls = {"n": 0}

    async def run_once():
        i = calls["n"]
        calls["n"] += 1
        st = seq[i] if i < len(seq) else seq[-1]
        return {"path": "/nb.ipynb", "task": "t", "status": st}

    return run_once, calls


LOST = "CONNECTION_DIED_KERNEL_LOST"


class TestKernelLostRetry(unittest.TestCase):
    def test_clean_pass_runs_once(self):
        run_once, calls = _factory(["PASS"])
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(calls["n"], 1)  # no retry when not kernel-lost

    def test_partial_not_retried(self):
        run_once, calls = _factory(["PARTIAL"])
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "PARTIAL")
        self.assertEqual(calls["n"], 1)

    def test_fail_not_retried(self):
        # a normal FAIL must NOT trigger the whole-task retry (only kernel-loss does)
        run_once, calls = _factory(["FAIL"])
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "FAIL")
        self.assertEqual(calls["n"], 1)

    def test_kernel_lost_then_recovers(self):
        run_once, calls = _factory([LOST, "PASS"])
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "PASS")     # retry recovered
        self.assertEqual(calls["n"], 2)             # ran exactly twice

    def test_kernel_lost_twice_fails_migration(self):
        run_once, calls = _factory([LOST, LOST])
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "FAIL")     # fail after one retry
        self.assertEqual(calls["n"], 2)             # exactly ONE retry, no infinite loop
        self.assertEqual(res["path"], "/nb.ipynb")
        self.assertEqual(res["task"], "t")
        self.assertIn("restart", res["error"].lower())

    def test_kernel_lost_then_partial_is_kept(self):
        # retry that lands on PARTIAL (not lost) is accepted, not failed
        run_once, calls = _factory([LOST, "PARTIAL"])
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "PARTIAL")
        self.assertEqual(calls["n"], 2)

    def test_none_result_safe(self):
        async def run_once():
            return None
        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertIsNone(res)  # non-dict / None first result returned unchanged


class TestRetryPrunesCellHistory(unittest.TestCase):
    """Fix C #2 — the retry must drop the failed attempt's _cell_history entries so
    the clean-kernel retry isn't biased by post-wipe workaround code."""

    def setUp(self):
        self._saved = list(job_migrate._cell_history)
        job_migrate._cell_history.clear()

    def tearDown(self):
        job_migrate._cell_history.clear()
        job_migrate._cell_history.extend(self._saved)

    def test_failed_attempt_history_pruned_before_retry(self):
        job_migrate._cell_history.append({"pre": "existing"})  # 1 pre-existing entry
        calls = {"n": 0}

        async def run_once():
            calls["n"] += 1
            # each attempt appends 5 cell-history entries (simulating a task run)
            for k in range(5):
                job_migrate._cell_history.append({"attempt": calls["n"], "cell": k})
            return {"status": "CONNECTION_DIED_KERNEL_LOST" if calls["n"] == 1 else "PASS"}

        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(calls["n"], 2)
        # 1 pre-existing + 5 from the (successful) retry; the 5 failed-attempt
        # entries were pruned. No entry from attempt 1 should survive.
        self.assertEqual(len(job_migrate._cell_history), 6)
        self.assertFalse(any(e.get("attempt") == 1 for e in job_migrate._cell_history))
        self.assertTrue(any(e.get("pre") == "existing" for e in job_migrate._cell_history))


class TestRetryResetsInlineCache(unittest.TestCase):
    """Fix C #1 — the retry must reset _inlined_nb_run_children so notebook.run
    children are RE-inlined on the fresh kernel (their globals were wiped)."""

    def setUp(self):
        self._saved_hist = list(job_migrate._cell_history)
        self._saved_inl = set(job_migrate._inlined_nb_run_children)
        job_migrate._cell_history.clear()
        job_migrate._inlined_nb_run_children.clear()

    def tearDown(self):
        job_migrate._cell_history.clear()
        job_migrate._cell_history.extend(self._saved_hist)
        job_migrate._inlined_nb_run_children.clear()
        job_migrate._inlined_nb_run_children.update(self._saved_inl)

    def test_inline_cache_cleared_before_retry(self):
        # simulate: attempt 1 inlines a child (adds to the set) then loses the kernel
        calls = {"n": 0}

        async def run_once():
            calls["n"] += 1
            if calls["n"] == 1:
                job_migrate._inlined_nb_run_children.add("/child_setup.ipynb")
                return {"status": "CONNECTION_DIED_KERNEL_LOST"}
            # retry: the set must be empty here so the child gets re-inlined
            self.assertNotIn("/child_setup.ipynb", job_migrate._inlined_nb_run_children)
            return {"status": "PASS"}

        res = _run(_run_task_with_kernel_retry(run_once, "t", "/nb.ipynb"))
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(calls["n"], 2)


if __name__ == "__main__":
    unittest.main()

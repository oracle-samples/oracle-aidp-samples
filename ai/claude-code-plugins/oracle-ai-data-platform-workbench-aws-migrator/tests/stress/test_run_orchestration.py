from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from aws_aidp.run.jobruns import poll_until_terminal, run_until_green
from aws_aidp.state import RunStore
from tests.stress.helpers import REPO_ROOT


def run_state(status, *tasks):
    return {
        "state": {"status": status},
        "tasks": [
            {"taskKey": key, "state": {"status": task_status}}
            for key, task_status in tasks
        ],
    }


def current_api_run_state(status, *tasks):
    task_to_run = {key: f"task-run-{index}" for index, (key, _) in enumerate(tasks)}
    summaries = {
        f"task-run-{index}": {"state": {"status": task_status}}
        for index, (_, task_status) in enumerate(tasks)
    }
    return {
        "state": {"status": status},
        "taskToTaskRunMap": task_to_run,
        "taskRunSummaryMap": summaries,
        "tasks": [{"taskKey": key, "type": "PYTHON_TASK"} for key, _ in tasks],
    }


class FakeAidpClient:
    def __init__(self, runs):
        self.runs = list(runs)
        self.last = self.runs[-1]
        self.repairs = []

    def create_job_run(self, job_key, parameters=None):
        self.created = (job_key, parameters)
        return "run-1"

    def get_job_run(self, run_key):
        if self.runs:
            self.last = self.runs.pop(0)
        return self.last

    def repair_job_run(self, run_key, task_keys):
        self.repairs.append((run_key, list(task_keys)))
        return {}


class RunOrchestrationTests(unittest.TestCase):
    def test_poll_records_transitions_and_success(self):
        client = FakeAidpClient([
            run_state("PENDING", ("extract", "PENDING")),
            run_state("RUNNING", ("extract", "RUNNING")),
            run_state("SUCCEEDED", ("extract", "SUCCEEDED")),
        ])
        messages = []
        with tempfile.TemporaryDirectory() as tmp, patch("time.sleep", return_value=None):
            store = RunStore("job", root=tmp)
            result = poll_until_terminal(
                client, "run-1", poll_interval=0, timeout=5,
                log=messages.append, store=store,
            )
            self.assertEqual(result["state"]["status"], "SUCCEEDED")
            self.assertEqual(len(store.history("run-1")), 3)
        self.assertTrue(any("RUNNING" in line for line in messages))

    def test_run_repairs_only_failed_tasks(self):
        client = FakeAidpClient([
            run_state("FAILED", ("extract", "SUCCEEDED"), ("load", "FAILED")),
            run_state("SUCCEEDED", ("extract", "SUCCEEDED"), ("load", "SUCCEEDED")),
        ])
        with tempfile.TemporaryDirectory() as tmp, patch("time.sleep", return_value=None):
            outcome = run_until_green(
                client, "job", max_repairs=2, poll_interval=0, timeout=5,
                store=RunStore("job", root=tmp),
            )
        self.assertEqual(outcome.final_status, "SUCCEEDED")
        self.assertEqual(outcome.repair_count, 1)
        self.assertEqual(client.repairs, [("run-1", ["load"])])

    def test_max_repairs_stops(self):
        client = FakeAidpClient([
            run_state("FAILED", ("load", "FAILED")),
            run_state("FAILED", ("load", "FAILED")),
            run_state("FAILED", ("load", "FAILED")),
        ])
        with tempfile.TemporaryDirectory() as tmp, patch("time.sleep", return_value=None):
            outcome = run_until_green(
                client, "job", max_repairs=2, poll_interval=0, timeout=5,
                store=RunStore("job", root=tmp),
            )
        self.assertEqual(outcome.final_status, "FAILED")
        self.assertEqual(outcome.repair_count, 2)
        self.assertEqual(outcome.failed_tasks, ["load"])

    def test_invalid_timeout_is_rejected(self):
        client = FakeAidpClient([run_state("UNKNOWN")])
        with self.assertRaisesRegex(ValueError, "timeout"):
            poll_until_terminal(client, "run-1", poll_interval=0, timeout=-1)

    def test_unknown_run_status_fails_closed(self):
        client = FakeAidpClient([run_state("NEW_OR_UNRECOGNIZED")])
        with self.assertRaisesRegex(RuntimeError, "unknown AIDP status"):
            poll_until_terminal(client, "run-1", poll_interval=0, timeout=5)

    def test_negative_poll_interval_is_rejected(self):
        client = FakeAidpClient([run_state("PENDING")])
        with self.assertRaisesRegex(ValueError, "poll_interval"):
            poll_until_terminal(client, "run-1", poll_interval=-1, timeout=5)

    def test_all_terminal_non_green_states_return_without_repair(self):
        for status in (
            "CANCELED", "CANCELLED", "TIMED_OUT", "SKIPPED", "INTERNAL_ERROR",
            "BLOCKED", "UPSTREAM_CANCELED", "UPSTREAM_FAILED", "EXCLUDED",
        ):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                client = FakeAidpClient([run_state(status)])
                with patch("time.sleep", return_value=None):
                    outcome = run_until_green(
                        client, "job", max_repairs=2, poll_interval=0, timeout=5,
                        store=RunStore("job", root=tmp),
                    )
                self.assertEqual(outcome.final_status, status)
                self.assertEqual(outcome.repair_count, 0)

    def test_missing_task_status_does_not_crash_logging(self):
        client = FakeAidpClient([{
            "state": {"status": "SUCCEEDED"},
            "tasks": [{"taskKey": None, "state": {}}],
        }])
        messages = []
        result = poll_until_terminal(client, "run-1", poll_interval=0, timeout=5, log=messages.append)
        self.assertEqual(result["state"]["status"], "SUCCEEDED")
        self.assertTrue(any("UNKNOWN" in line for line in messages))

    def test_failed_task_keys_are_deduplicated(self):
        client = FakeAidpClient([
            run_state("FAILED", ("load", "FAILED"), ("load", "ERROR")),
            run_state("SUCCEEDED", ("load", "SUCCEEDED")),
        ])
        with tempfile.TemporaryDirectory() as tmp, patch("time.sleep", return_value=None):
            outcome = run_until_green(
                client, "job", max_repairs=1, poll_interval=0, timeout=5,
                store=RunStore("job", root=tmp),
            )
        self.assertEqual(outcome.final_status, "SUCCEEDED")
        self.assertEqual(client.repairs, [("run-1", ["load"])])

    def test_negative_repair_budget_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "max_repairs"):
            run_until_green(FakeAidpClient([run_state("FAILED")]), "job", max_repairs=-1)

    def test_current_oracle_task_summary_shape_repairs_only_failed_tasks(self):
        client = FakeAidpClient([
            current_api_run_state("FAILED", ("extract", "SUCCESS"), ("load", "FAILED")),
            current_api_run_state("SUCCESS", ("extract", "SUCCESS"), ("load", "SUCCESS")),
        ])
        with tempfile.TemporaryDirectory() as tmp, patch("time.sleep", return_value=None):
            outcome = run_until_green(
                client, "job", max_repairs=1, poll_interval=0, timeout=5,
                store=RunStore("job", root=tmp),
            )
        self.assertEqual(outcome.final_status, "SUCCESS")
        self.assertEqual(client.repairs, [("run-1", ["load"])])

    def test_run_store_rejects_path_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                RunStore("../escape", root=tmp)

    def test_run_store_rejects_traversal_in_run_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore("job", root=tmp)
            with self.assertRaises(ValueError):
                store.append("../escape", {"event": "bad"})

    def test_concurrent_run_store_appends_remain_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            def append(worker):
                # Independent instances must coordinate through the path lock.
                store = RunStore("job", root=tmp)
                for sequence in range(100):
                    store.append("run", {"worker": worker, "sequence": sequence})

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(append, range(8)))
            store = RunStore("job", root=tmp)
            events = store.history("run")
            self.assertEqual(len(events), 800)

    def test_concurrent_process_appends_remain_valid(self):
        code = (
            "import sys\n"
            "from aws_aidp.state import RunStore\n"
            "store = RunStore('job', root=sys.argv[1])\n"
            "for sequence in range(25):\n"
            "    store.append('run', {'worker': sys.argv[2], 'sequence': sequence})\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            processes = [
                subprocess.Popen(
                    [sys.executable, "-c", code, tmp, str(worker)],
                    cwd=REPO_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                for worker in range(4)
            ]
            for process in processes:
                stdout, stderr = process.communicate(timeout=30)
                self.assertEqual(process.returncode, 0, stdout + stderr)
            events = RunStore("job", root=tmp).history("run")
            self.assertEqual(len(events), 100)

    def test_state_directory_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "state"
            outside = Path(tmp) / "outside"
            root.mkdir()
            outside.mkdir()
            try:
                (root / "job").symlink_to(outside, target_is_directory=True)
            except (OSError, NotImplementedError):
                self.skipTest("directory symlinks are not available")
            with self.assertRaisesRegex(ValueError, "symlink"):
                RunStore("job", root=root)

    def test_state_file_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore("job", root=tmp)
            target = Path(tmp) / "outside.jsonl"
            target.write_text("unchanged")
            path = store._path("run")
            try:
                path.symlink_to(target)
            except (OSError, NotImplementedError):
                self.skipTest("file symlinks are not available")
            with self.assertRaisesRegex(ValueError, "symlink"):
                store.append("run", {"event": "bad"})
            self.assertEqual(target.read_text(), "unchanged")

    def test_run_store_rejects_nonportable_and_control_character_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            for key in (" job", "job\nname", "CON", "job:name", "x" * 201):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    RunStore(key, root=tmp)

    def test_event_must_be_json_object_and_timestamp_cannot_be_spoofed(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore("job", root=tmp)
            with self.assertRaisesRegex(ValueError, "JSON object"):
                store.append("run", ["bad"])
            store.append("run", {"event": "ok", "ts": "spoofed"})
            event = store.history("run")[0]
            self.assertNotEqual(event["ts"], "spoofed")
            self.assertRegex(event["ts"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_corrupt_history_has_clear_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore("job", root=tmp)
            store._path("run").write_text('{"ok": true}\nnot-json\n')
            with self.assertRaisesRegex(ValueError, "invalid run history"):
                store.history("run")

    def test_non_object_history_event_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore("job", root=tmp)
            store._path("run").write_text('["not", "an", "event"]\n')
            with self.assertRaisesRegex(ValueError, "event must be a JSON object"):
                store.history("run")

    def test_non_utf8_history_has_clear_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RunStore("job", root=tmp)
            store._path("run").write_bytes(b"\xff\xfe")
            with self.assertRaisesRegex(ValueError, "not UTF-8"):
                store.history("run")


if __name__ == "__main__":
    unittest.main()

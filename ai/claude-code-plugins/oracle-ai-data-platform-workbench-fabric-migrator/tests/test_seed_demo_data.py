"""scripts/seed_demo_data.py: the demo's sample data, so its job runs end to end.

The demo job reads files a real Fabric workspace would hold and a warehouse
table a real migration would have loaded; on a fresh AIDP account neither
exists and the job dies at its first read on BucketNotFound. The seed supplies
both, reading every location from the migration's own output.

These tests need no cluster. What was run on one, 2026-10-01: the bundled
estate migrated, published and seeded, then its job ran green end to end --
setup, ingest, aggregate and report all SUCCESS, the ingest read the 20 seeded
rows, the report showed 4 policies of 5 -- and the SQL notebook succeeded
against the table the seed built from the migrated DDL.
"""
import importlib.util
import io
import json
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.cli import main
from fabric_aidp.fixtures import demo_workspace_path
from fabric_aidp.publish.aidp_client import AidpClient

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "seed_demo_data", ROOT / "scripts" / "seed_demo_data.py")
seed = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(seed)


def _migrate(tmp: Path, *, namespace="testns", catalog="demo_cat") -> Path:
    out = tmp / "migrated"
    with redirect_stdout(io.StringIO()):
        main(["inventory", str(demo_workspace_path()), "-o", str(tmp / "inv.json")])
        main(["plan", str(tmp / "inv.json"), "-o", str(tmp / "plan.json"),
              "--namespace", namespace, "--catalog", catalog])
        main(["migrate", str(tmp / "plan.json"), "-o", str(out)])
    return out


class ReadsTheMigrationsOwnOutputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        cls.out = _migrate(Path(cls._tmp.name))
        cls.inputs = seed.read_inputs(cls.out)

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_bucket_and_path_come_from_the_ingest_notebook(self):
        self.assertEqual(self.inputs["bucket"], "AcmeWS_SalesLake_Lakehouse")
        self.assertEqual(self.inputs["namespace"], "testns")
        self.assertEqual(self.inputs["uri"],
                         "oci://AcmeWS_SalesLake_Lakehouse@testns/Files/raw/claims")

    def test_the_uri_is_the_one_the_notebook_actually_reads(self):
        text = (self.out / seed.INGEST_NOTEBOOK).read_text(encoding="utf-8")
        self.assertIn(self.inputs["uri"], text)

    def test_the_claim_table_and_ddl_are_the_migrated_ones(self):
        self.assertEqual(self.inputs["claim_table"], "demo_cat.AcmeDW.claim")
        ddl = (self.out / seed.CLAIM_DDL).read_text(encoding="utf-8")
        self.assertIn(self.inputs["claim_ddl"], ddl)

    def test_the_setup_statements_come_from_setup_catalogs_sql(self):
        self.assertIn("CREATE SCHEMA IF NOT EXISTS demo_cat.SalesLake",
                      self.inputs["setup"])
        self.assertIn("CREATE CATALOG IF NOT EXISTS demo_cat", self.inputs["setup"])


class RefusalTests(unittest.TestCase):
    def test_a_directory_that_is_not_demo_output_is_refused(self):
        with TemporaryDirectory() as tmp:
            with self.assertRaises(seed.SeedError):
                seed.read_inputs(tmp)

    def test_a_plan_without_a_namespace_is_refused_with_the_reason(self):
        """Without --namespace the path keeps a placeholder: no bucket."""
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "notebooks").mkdir()
            (out / "warehouse").mkdir()
            (out / seed.INGEST_NOTEBOOK).write_text("x = spark.read.parquet('Files/raw')")
            (out / seed.CLAIM_DDL).write_text("CREATE TABLE c.s.t (a INT)")
            with self.assertRaises(seed.SeedError) as caught:
                seed.read_inputs(out)
            self.assertIn("--namespace", str(caught.exception))

    def test_live_mode_needs_the_workspace_and_cluster(self):
        with TemporaryDirectory() as tmp:
            out = _migrate(Path(tmp))
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.assertEqual(seed.main([str(out)]), 2)

    def test_create_bucket_needs_a_compartment(self):
        with TemporaryDirectory() as tmp:
            out = _migrate(Path(tmp))
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                self.assertEqual(seed.main([str(out), "--workspace-key", "k",
                                            "--cluster-key", "c", "--create-bucket"]), 2)


class TheDataTests(unittest.TestCase):
    def test_twenty_rows_over_four_policies_of_five(self):
        """The shape the demo job was proved green with on AIDP."""
        rows = seed.seed_rows()
        self.assertEqual(len(rows), 20)
        by_policy = {}
        for _, policy, _, _ in rows:
            by_policy[policy] = by_policy.get(policy, 0) + 1
        self.assertEqual(by_policy, {"P1": 5, "P2": 5, "P3": 5, "P4": 5})

    def test_it_is_deterministic(self):
        self.assertEqual(seed.seed_rows(), seed.seed_rows())


class TheNotebookTests(unittest.TestCase):
    """Run the generated cells against a fake Spark, both ways round."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        cls.inputs = seed.read_inputs(_migrate(Path(cls._tmp.name)))

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _run(self, *, existing_rows, table_exists, overwrite):
        log = []

        class Frame:
            def __init__(self, n=0): self.n = n
            def count(self): return self.n
            @property
            def write(self): return self
            def mode(self, m): log.append(("mode", m)); return self
            def parquet(self, uri): log.append(("write_parquet", uri))

        class Reader:
            def parquet(self, uri):
                if not existing_rows and ("write_parquet", uri) not in log:
                    raise ValueError("no such path")
                return Frame(existing_rows or 20)

        class Catalog:
            def tableExists(self, name): return table_exists

        class Spark:
            read = Reader()
            catalog = Catalog()
            def sql(self, statement): log.append(("sql", statement.split()[0:2]))
            def createDataFrame(self, rows): return Frame(len(rows))
            def table(self, name): return Frame(20)

        import sys, types
        pyspark = types.ModuleType("pyspark")
        pyspark_sql = types.ModuleType("pyspark.sql")
        pyspark_sql.Row = lambda **kw: kw
        sys.modules.setdefault("pyspark", pyspark)
        saved = sys.modules.get("pyspark.sql")
        sys.modules["pyspark.sql"] = pyspark_sql
        try:
            nb = json.loads(seed.seed_notebook(self.inputs, overwrite=overwrite))
            env = {"spark": Spark()}
            with redirect_stdout(io.StringIO()):
                for cell in nb["cells"]:
                    exec(compile(cell["source"], "seed", "exec"), env)
        finally:
            if saved is not None:
                sys.modules["pyspark.sql"] = saved
            else:
                sys.modules.pop("pyspark.sql", None)
        return log

    def test_an_empty_path_and_no_table_are_both_written(self):
        log = self._run(existing_rows=0, table_exists=False, overwrite=False)
        self.assertIn(("write_parquet", self.inputs["uri"]), log)
        self.assertIn(("sql", ["CREATE", "TABLE"]), log)
        self.assertIn(("sql", ["INSERT", "INTO"]), log)

    def test_existing_files_are_kept_by_default(self):
        """The first live run overwrote a colleague's files at this path."""
        log = self._run(existing_rows=17, table_exists=False, overwrite=False)
        self.assertNotIn(("write_parquet", self.inputs["uri"]), log)

    def test_an_existing_table_is_kept_by_default(self):
        log = self._run(existing_rows=0, table_exists=True, overwrite=False)
        self.assertNotIn(("sql", ["DROP", "TABLE"]), log)
        self.assertNotIn(("sql", ["INSERT", "INTO"]), log)

    def test_overwrite_replaces_both(self):
        log = self._run(existing_rows=17, table_exists=True, overwrite=True)
        self.assertIn(("write_parquet", self.inputs["uri"]), log)
        self.assertIn(("sql", ["DROP", "TABLE"]), log)

    def test_the_schemas_are_created_first(self):
        log = self._run(existing_rows=0, table_exists=False, overwrite=False)
        first = next(i for i, entry in enumerate(log) if entry[0] != "sql"
                     or entry[1][0] != "CREATE" or entry[1][1] not in ("SCHEMA", "CATALOG"))
        self.assertTrue(all(log[i][1][1] in ("SCHEMA", "CATALOG") for i in range(first)))
        self.assertGreater(first, 0)


class DryRunTests(unittest.TestCase):
    def test_a_dry_run_sends_nothing_and_writes_the_notebook(self):
        with TemporaryDirectory() as tmp:
            out = _migrate(Path(tmp))
            with redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(seed.main([str(out), "--dry-run"]), 0)
            self.assertTrue((out / seed.SEED_NOTEBOOK).is_file())
            self.assertIn("nothing was sent", printed.getvalue())


class ClientRunMethodsTests(unittest.TestCase):
    """The three AidpClient methods the seed added. `publish` uses none."""

    def _client(self, responses):
        client = AidpClient(workspace_key="ws")
        calls = []

        def fake_run(args, body=None):
            calls.append((args, body))
            return responses.pop(0)
        client._run = fake_run
        return client, calls

    def test_run_job_sends_the_job_key_and_returns_the_run_key(self):
        client, calls = self._client([{"data": {"key": "run-1"}}])
        self.assertEqual(client.run_job("job-1"), "run-1")
        self.assertEqual(calls[0][0][:2], ["workflow", "create-job-run"])
        self.assertEqual(calls[0][1], {"jobKey": "job-1"})

    def test_task_runs_reads_the_status_from_state(self):
        """It sits at state.status; a poller reading `status` saw None."""
        client, calls = self._client([{"data": {"items": [
            {"taskKey": "a", "key": "tr-1",
             "state": {"status": "SUCCESS", "stateMessage": "ok"}}]}}])
        self.assertEqual(client.task_runs("run-1"), [("a", "SUCCESS", "ok", "tr-1")])
        self.assertIn("--sort-by", calls[0][0])

    def test_cluster_problem_accepts_a_listed_cluster(self):
        client, _ = self._client([{"data": {"items": [{"key": "ck", "displayName": "c"}]}}])
        self.assertEqual(client.cluster_problem("ck"), "")

    def test_cluster_problem_names_the_default_compute(self):
        client, _ = self._client([
            {"data": {"items": [{"key": "ck", "displayName": "fabricTest"}]}},
            {"data": {"key": "dflt", "displayName": "Default Master Catalog Compute"}}])
        problem = client.cluster_problem("dflt")
        self.assertIn("Default Master Catalog Compute", problem)
        self.assertIn("fabricTest (ck)", problem)

    def test_cluster_problem_names_an_unknown_key(self):
        client, _ = self._client([
            {"data": {"items": [{"key": "ck", "displayName": "fabricTest"}]}},
            {"data": {"key": "dflt"}}])
        self.assertIn("not one of this workspace's clusters", client.cluster_problem("zzz"))

    def test_task_output_sends_the_empty_body_the_api_requires(self):
        client, calls = self._client([{"data": {"output": "text"}}])
        client.task_output("tr-1")
        self.assertEqual(calls[0][1], {})


class ReviewFindingsTests(unittest.TestCase):
    """Three findings from testing #60 on a fresh catalog."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        cls.out = _migrate(Path(cls._tmp.name))

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def _fake_client(self, existing_jobs=(), fail_with=None, job_cluster="ck",
                     job_path="/Workspace/me/zz_seed_demo_data.ipynb",
                     cluster_issue="", task=("seed", "SUCCESS", "ok", "tr-1"), output=""):
        calls = []

        class Fake:
            def __init__(self, **kw): pass
            def cluster_problem(self, key): return cluster_issue
            def put_notebook(self, path, text): calls.append(("put", path))
            def list_jobs(self):
                if fail_with: raise fail_with
                return list(existing_jobs)
            def get_job(self, key):
                return {"tasks": [{"notebookPath": job_path,
                                   "cluster": {"clusterKey": job_cluster}}]}
            def update_job(self, key, definition):
                calls.append(("update", key,
                              definition["tasks"][0]["cluster"]["clusterKey"]))
            def create_job(self, definition):
                calls.append(("create", definition["name"])); return "new-job"
            def run_job(self, key): calls.append(("run", key)); return "run-1"
            def task_runs(self, run): return [task]
            def task_output(self, key): return output
        return Fake, calls

    def _main(self, fake, extra=()):
        from unittest import mock
        argv = [str(self.out), "--workspace-key", "ws", "--cluster-key", "ck",
                "--prefix", "me", *extra]
        with mock.patch("fabric_aidp.publish.aidp_client.AidpClient", fake), \
                redirect_stdout(io.StringIO()) as out, redirect_stderr(io.StringIO()) as err:
            code = seed.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_an_existing_seed_job_is_reused_not_recreated(self):
        """Was: a re-run after a partial failure was refused with
        JOB_VALIDATE_0031, the job name being taken."""
        fake, calls = self._fake_client(existing_jobs=[
            {"name": "me_seed_demo_data", "key": "old-job"},
            {"name": "someone_elses_job", "key": "other"}])
        code, out, _ = self._main(fake)
        self.assertEqual(code, 0)
        self.assertNotIn("create", [c[0] for c in calls])
        self.assertIn(("run", "old-job"), calls)
        self.assertIn("reusing job", out)

    def test_a_reused_job_on_the_same_cluster_is_not_touched(self):
        fake, calls = self._fake_client(existing_jobs=[
            {"name": "me_seed_demo_data", "key": "old-job"}])
        self._main(fake)
        self.assertNotIn("update", [c[0] for c in calls])

    def test_a_reused_job_is_moved_to_the_cluster_this_run_names(self):
        """Found in review: reuse kept the cluster the job was created with,
        so a re-run with a different --cluster-key ran on the old one."""
        fake, calls = self._fake_client(existing_jobs=[
            {"name": "me_seed_demo_data", "key": "old-job"}], job_cluster="old-cluster")
        code, out, _ = self._main(fake)
        self.assertEqual(code, 0)
        self.assertIn(("update", "old-job", "ck"), calls)
        self.assertLess(calls.index(("update", "old-job", "ck")), calls.index(("run", "old-job")))
        self.assertIn("was old-cluster", out)

    def test_a_reused_job_pointing_at_another_notebook_path_is_updated(self):
        fake, calls = self._fake_client(existing_jobs=[
            {"name": "me_seed_demo_data", "key": "old-job"}],
            job_path="/Workspace/elsewhere/zz_seed_demo_data.ipynb")
        self._main(fake)
        self.assertIn("update", [c[0] for c in calls])

    def test_with_no_existing_job_one_is_created(self):
        fake, calls = self._fake_client()
        code, _, _ = self._main(fake)
        self.assertEqual(code, 0)
        self.assertIn(("create", "me_seed_demo_data"), calls)
        self.assertIn(("run", "new-job"), calls)

    def test_a_job_with_a_similar_name_is_not_mistaken_for_ours(self):
        fake, calls = self._fake_client(existing_jobs=[
            {"name": "me_seed_demo_data_old", "key": "nope"}])
        self._main(fake)
        self.assertIn(("create", "me_seed_demo_data"), calls)

    def test_an_aidp_error_is_one_clean_line_not_a_traceback(self):
        from fabric_aidp.publish.aidp_client import AidpError
        fake, _ = self._fake_client(
            fail_with=AidpError("AIDP 409 JOB_VALIDATE_0031: job already exists"))
        code, _, err = self._main(fake)
        self.assertEqual(code, 1)
        self.assertIn("JOB_VALIDATE_0031", err)
        self.assertNotIn("Traceback", err)
        self.assertEqual(len(err.strip().splitlines()), 1)

    def test_a_missing_aidp_cli_is_one_clean_line_too(self):
        from fabric_aidp.publish.aidp_client import AidpUnavailable
        fake, _ = self._fake_client(fail_with=AidpUnavailable("'aidp' not found on PATH"))
        code, _, err = self._main(fake)
        self.assertEqual(code, 1)
        self.assertNotIn("Traceback", err)

    def test_a_bug_is_still_a_traceback(self):
        """Only AIDP's own errors are turned into a message; anything else is
        a defect in this script and must stay loud."""
        fake, _ = self._fake_client(fail_with=KeyError("bug"))
        with self.assertRaises(KeyError):
            self._main(fake)

    def test_a_cluster_that_cannot_run_jobs_is_refused_before_upload(self):
        fake, calls = self._fake_client(
            cluster_issue="cluster x is this workspace's Default Master Catalog Compute")
        code, _, err = self._main(fake)
        self.assertEqual(code, 2)
        self.assertIn("Default Master Catalog Compute", err)
        self.assertEqual(calls, [])

    def test_a_failed_task_prints_one_line_and_no_empty_json(self):
        """Found in review: an all-null output was printed raw under the error."""
        message = ("WORKFLOW_EXECUTION_0055 - Cluster x is not in Active state\n"
                   "Timestamp: 2026-10-01T17:36:28Z\nClient version: sdk")
        nulls = json.dumps({"key": None, "taskType": "NOTEBOOK_TASK",
                            "errorTrace": None, "data": None})
        fake, _ = self._fake_client(task=("seed", "FAILED", message, "tr-1"), output=nulls)
        code, out, _ = self._main(fake)
        self.assertEqual(code, 1)
        self.assertIn("WORKFLOW_EXECUTION_0055", out)
        self.assertNotIn("Timestamp", out)
        self.assertNotIn('"key": null', out)

    def test_a_failed_task_shows_the_real_error_text_when_there_is_some(self):
        output = json.dumps({"errorTrace": None, "data": [{"value": json.dumps(
            {"cells": [{"outputs": [{"ename": "AnalysisException",
                                     "evalue": "TABLE_OR_VIEW_NOT_FOUND x"}]}]})}]})
        fake, _ = self._fake_client(task=("seed", "FAILED", "failed", "tr-1"), output=output)
        _, out, _ = self._main(fake)
        self.assertIn("TABLE_OR_VIEW_NOT_FOUND", out)

    def test_overwrite_warns_that_the_bucket_is_shared(self):
        """The bucket name comes from the Fabric path, so everyone running
        the demo in one tenancy reads the same one."""
        fake, _ = self._fake_client()
        _, out, _ = self._main(fake, extra=["--overwrite"])
        self.assertIn("same for everyone", out)

    def test_no_warning_without_overwrite(self):
        fake, _ = self._fake_client()
        _, out, _ = self._main(fake)
        self.assertNotIn("same for everyone", out)


if __name__ == "__main__":
    unittest.main()

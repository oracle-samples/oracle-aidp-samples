"""publish is the one part of this tool that writes to a live workspace.

These tests never contact one. They cover the planning and the refusals --
the parts that decide what would be sent -- with a fake client standing in
for AIDP. The live path was exercised by hand against a real workspace:
6 notebooks uploaded, a 3-task job created, its dependsOn chain intact on
the cluster, and a second run correctly refusing to overwrite it.
"""
import ast
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from unittest import mock

from fabric_aidp.publish import PublishError, plan_publish, publish
from fabric_aidp.publish import publisher as publisher_mod
from fabric_aidp.publish import aidp_client
from fabric_aidp.publish.aidp_client import AidpClient, AidpError
from fabric_aidp.publish.to_ipynb import to_ipynb

STAR = "*" * 20
NOTEBOOK = (f"# Fabric notebook source\n\n# MARKDOWN {STAR}\n\n# # Title\n\n"
            f"# CELL {STAR}\n\ndf = spark.table('t')\n")


# A migrated notebook with no `# Fabric notebook source` header: migrate
# grades it REVIEW (NB06) and writes it verbatim, so publish is handed it.
NO_HEADER = f"# CELL {STAR}\n\nx = spark.table('claims')\n"


def _unconvertible(out, name="03_No_Header", *, in_job=False):
    """Add a notebook to_ipynb cannot read to a `_migration` directory."""
    (out / "notebooks" / f"{name}.py").write_text(NO_HEADER, encoding="utf-8")
    report = json.loads((out / "report.json").read_text(encoding="utf-8"))
    report["results"].append({"asset_id": f"notebook.{name}", "kind": "notebook",
                              "status": "needs_manual_review",
                              "output_path": f"notebooks/{name}.py"})
    (out / "report.json").write_text(json.dumps(report), encoding="utf-8")
    if in_job:
        (out / "jobs" / "Bad.job.json").write_text(json.dumps({
            "name": "Bad", "tasks": [
                {"type": "NOTEBOOK_TASK", "taskKey": "a",
                 "notebookPath": "/Workspace/01_Ingest.py", "source": "WORKSPACE"},
                {"type": "NOTEBOOK_TASK", "taskKey": "c",
                 "notebookPath": f"/Workspace/{name}.py", "source": "WORKSPACE"}]}),
            encoding="utf-8")
        report["results"].append({"asset_id": "pipeline.Bad", "kind": "pipeline_job",
                                  "status": "ok", "output_path": "jobs/Bad.job.json"})
        (out / "report.json").write_text(json.dumps(report), encoding="utf-8")
    return out


def _migration(tmp, *, jobs=True, notebooks=("01_Ingest", "02_Agg")):
    out = Path(tmp)
    (out / "notebooks").mkdir(parents=True)
    rows = []
    for name in notebooks:
        (out / "notebooks" / f"{name}.py").write_text(NOTEBOOK, encoding="utf-8")
        rows.append({"asset_id": f"notebook.{name}", "kind": "notebook", "status": "ok",
                     "output_path": f"notebooks/{name}.py"})
    if jobs:
        rows.append({"asset_id": "pipeline.Daily", "kind": "pipeline_job",
                     "status": "needs_manual_review", "output_path": "jobs/Daily.job.json"})
    (out / "report.json").write_text(json.dumps({"complete": True, "results": rows}),
                                     encoding="utf-8")
    if jobs:
        (out / "jobs").mkdir()
        (out / "jobs" / "Daily.job.json").write_text(json.dumps({
            "name": "Daily", "maxConcurrentRuns": 1, "tasks": [
                {"type": "NOTEBOOK_TASK", "taskKey": "a",
                 "notebookPath": "/Workspace/01_Ingest.py", "source": "WORKSPACE"},
                {"type": "NOTEBOOK_TASK", "taskKey": "b",
                 "notebookPath": "/Workspace/02_Agg.py", "source": "WORKSPACE",
                 "dependsOn": [{"taskKey": "a"}]}]}), encoding="utf-8")
    return out


class ToIpynbTests(unittest.TestCase):
    """A NOTEBOOK_TASK needs a notebook; the `.py` uploads as a file, which a
    task cannot run."""

    def test_cells_are_split_on_the_fabric_markers(self):
        nb = json.loads(to_ipynb(NOTEBOOK))
        self.assertEqual([c["cell_type"] for c in nb["cells"]], ["markdown", "code"])

    def test_the_code_survives_intact(self):
        nb = json.loads(to_ipynb(NOTEBOOK))
        code = "".join(nb["cells"][1]["source"])
        self.assertEqual(code.strip(), "df = spark.table('t')")

    def test_markdown_loses_its_comment_marker(self):
        nb = json.loads(to_ipynb(NOTEBOOK))
        self.assertEqual("".join(nb["cells"][0]["source"]).strip(), "# Title")

    def test_the_result_is_a_valid_notebook_document(self):
        nb = json.loads(to_ipynb(NOTEBOOK))
        self.assertEqual(nb["nbformat"], 4)
        self.assertIn("kernelspec", nb["metadata"])

    def test_every_cell_carries_an_id_because_4_5_requires_one(self):
        """The output declared `nbformat_minor` 5 and wrote no `id`, which is
        schema-invalid by its own declaration."""
        nb = json.loads(to_ipynb(NOTEBOOK))
        self.assertEqual(nb["nbformat_minor"], 5)
        ids = [c.get("id") for c in nb["cells"]]
        self.assertTrue(all(ids), f"cells without an id: {ids}")
        self.assertEqual(len(set(ids)), len(ids), f"duplicate cell ids: {ids}")

    def test_the_ids_are_the_same_on_a_second_conversion(self):
        """A random id would make every re-publish a whole-file diff."""
        first = [c["id"] for c in json.loads(to_ipynb(NOTEBOOK))["cells"]]
        second = [c["id"] for c in json.loads(to_ipynb(NOTEBOOK))["cells"]]
        self.assertEqual(first, second)

    def test_a_parameters_cell_keeps_the_parameters_tag(self):
        """papermill and Fabric both key off this exact tag; without it the
        notebook still runs and always takes the defaults."""
        source = (f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\n"
                  f"env = 'dev'\n")
        cell = json.loads(to_ipynb(source))["cells"][0]
        self.assertEqual(cell["metadata"].get("tags"), ["parameters"])

    def test_a_cells_fabric_metadata_survives(self):
        source = (f"# Fabric notebook source\n\n# CELL {STAR}\n\n"
                  f"%%sql\nSELECT 1\n\n# METADATA {STAR}\n\n"
                  f'# META {{"language": "sparksql"}}\n')
        cell = json.loads(to_ipynb(source))["cells"][0]
        self.assertEqual(cell["metadata"].get("fabric"), {"language": "sparksql"})


class ToIpynbFromIpynbTests(unittest.TestCase):
    """`migrate` writes every notebook artifact under a `.py` name whatever it
    read, so `to_ipynb` is handed Jupyter JSON for any estate exported that
    way -- 39 of the 49 notebooks in microsoft/fabric-toolbox. Rebuilding such
    a document out of Fabric blocks corrupted it."""

    SOURCE = json.dumps({
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"language_info": {"name": "python"},
                     "kernelspec": {"name": "python3"},
                     "widgets": {"state": {}, "version_major": 2}},
        "cells": [
            {"cell_type": "markdown", "id": "abc12345", "metadata": {},
             "source": ["# Heading\n", "text\n"]},
            {"cell_type": "code", "id": "def67890",
             "metadata": {"tags": ["parameters"],
                          "jupyter": {"source_hidden": True}},
             "execution_count": 3,
             "outputs": [{"output_type": "stream", "name": "stdout",
                          "text": ["stale\n"]}],
             "source": ["p = 1\n"]}]}, indent=2)

    def setUp(self):
        self.out = json.loads(to_ipynb(self.SOURCE))

    def test_a_markdown_heading_keeps_its_hash(self):
        """It came back as `Heading`: the `# ` was read as Fabric's comment
        marker, so every heading in every .ipynb was demoted to body text."""
        self.assertEqual(self.out["cells"][0]["source"], ["# Heading\n", "text\n"])

    def test_the_cell_ids_are_the_ones_that_came_in(self):
        self.assertEqual([c["id"] for c in self.out["cells"]],
                         ["abc12345", "def67890"])

    def test_the_parameters_tag_survives(self):
        self.assertEqual(self.out["cells"][1]["metadata"]["tags"], ["parameters"])

    def test_cell_metadata_survives(self):
        self.assertEqual(self.out["cells"][1]["metadata"]["jupyter"],
                         {"source_hidden": True})

    def test_top_level_widget_metadata_survives(self):
        self.assertIn("widgets", self.out["metadata"])

    def test_the_trailing_newline_of_the_last_source_line_survives(self):
        """`p = 1` in a cell tagged `parameters` is now re-read from the task
        -- the `.ipynb` half, which used to get none of it -- so this cell's
        source grows. The invariant the test is about is unchanged, and
        stated more exactly than before: the input's own line is byte-identical
        at the front, every line but the last carries its own newline, and the
        last carries none.
        """
        source = self.out["cells"][1]["source"]
        self.assertEqual(source[0], "p = 1\n")
        self.assertTrue(all(line.endswith("\n") for line in source[:-1]), source)
        self.assertFalse(source[-1].endswith("\n"), source[-1])

    def test_stale_run_state_is_cleared(self):
        """Deliberate, and the one thing not carried over: those outputs were
        computed by a Fabric session against Fabric tables, before this tool
        rewrote the code to read OCI buckets instead."""
        self.assertEqual(self.out["cells"][1]["outputs"], [])
        self.assertIsNone(self.out["cells"][1]["execution_count"])

    def test_a_4_5_notebook_missing_an_id_gains_one(self):
        source = json.dumps({"nbformat": 4, "nbformat_minor": 5, "metadata": {},
                             "cells": [{"cell_type": "code", "metadata": {},
                                        "source": ["x = 1"]}]})
        self.assertTrue(json.loads(to_ipynb(source))["cells"][0].get("id"))

    def test_a_pre_4_5_notebook_is_not_given_an_id_it_has_no_schema_for(self):
        source = json.dumps({"nbformat": 4, "nbformat_minor": 4, "metadata": {},
                             "cells": [{"cell_type": "code", "metadata": {},
                                        "source": ["x = 1"]}]})
        self.assertNotIn("id", json.loads(to_ipynb(source))["cells"][0])


class AidpSqlMagicTests(unittest.TestCase):
    """AIDP skips a `%%sql` cell without an error and runs a `%sql` one.

    Measured on a live workspace: `%%sql CREATE TABLE ...` created nothing,
    `%%sql SELECT * FROM <missing table>` raised nothing, the job reported
    SUCCESS; as `%sql` both behaved as SQL."""

    SQL_NOTEBOOK = (f"# Fabric notebook source\n\n# CELL {STAR}\n\n"
                    f"%%sql\nSELECT policy_no, count(*) AS n\nFROM default.AcmeDW.claim\n"
                    f"GROUP BY policy_no\n\n# CELL {STAR}\n\n"
                    f"x = '%%sql is only a magic on a first line'\n")

    def _code(self, text):
        return ["".join(c["source"]) for c in json.loads(text)["cells"]
                if c["cell_type"] == "code"]

    def test_a_fabric_sql_cell_is_written_with_the_magic_aidp_runs(self):
        sql, _ = self._code(to_ipynb(self.SQL_NOTEBOOK))
        self.assertTrue(sql.startswith("%sql\n"), sql)
        self.assertIn("FROM default.AcmeDW.claim", sql)

    def test_only_a_first_line_magic_changes(self):
        _, python = self._code(to_ipynb(self.SQL_NOTEBOOK))
        self.assertEqual(python, "x = '%%sql is only a magic on a first line'")

    def test_a_single_percent_cell_and_a_lookalike_are_left_alone(self):
        for first in ("%sql", "%%sqlx", "%%spark"):
            with self.subTest(first=first):
                src = f"# Fabric notebook source\n\n# CELL {STAR}\n\n{first}\nSELECT 1\n"
                self.assertTrue(self._code(to_ipynb(src))[0].startswith(first + "\n"))

    def test_an_ipynb_input_gets_the_same_fix(self):
        # Most real exports are notebook-content.ipynb, not the .py form.
        source = json.dumps({"cells": [
            {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": None,
             "source": ["%%sql\n", "SELECT 1"]},
            {"cell_type": "code", "metadata": {}, "outputs": [], "execution_count": None,
             "source": "%%sql\nSELECT 2"}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
        self.assertEqual([c.split("\n")[0] for c in self._code(to_ipynb(source))],
                         ["%sql", "%sql"])


class AidpSqlMagicSpellingTests(unittest.TestCase):
    """Which spellings are rewritten, and the shape the rewrite lands in.

    The class above establishes *that* `%%sql` is corrected. This one pins
    the edges the review asked about: case, `%%tsql`, `%%sparksql`, and the
    multi-line cell a real migration emits.
    """

    def _first_code(self, text):
        return next("".join(c["source"]) if isinstance(c["source"], list)
                    else c["source"]
                    for c in json.loads(text)["cells"]
                    if c["cell_type"] == "code")

    def _published(self, first_line, body="SELECT 1"):
        src = (f"# Fabric notebook source\n\n# CELL {STAR}\n\n"
               f"{first_line}\n{body}\n")
        return self._first_code(to_ipynb(src))

    def test_the_magic_is_rewritten_whatever_its_case(self):
        """The translator routes `%%SQL` and `%%Sql` to its SQL path --
        `_SQL_MAGIC_RE` is IGNORECASE -- so publish has to see them too.
        Zero uppercase variants in the shipped corpora; this is hardening."""
        for first in ("%%SQL", "%%Sql", "%%sQl"):
            with self.subTest(first=first):
                self.assertTrue(self._published(first).startswith("%sql\n"))

    def test_tsql_is_mapped_rather_than_passed_through(self):
        """By the time a `%%tsql` cell reaches publish the translator has
        already converted the body to Spark SQL, so the T-SQL label is
        stale and AIDP has no `%tsql` to map it to."""
        for first in ("%%tsql", "%%TSQL"):
            with self.subTest(first=first):
                self.assertTrue(self._published(first).startswith("%sql\n"))

    def test_sparksql_is_left_alone_because_it_is_not_a_sql_cell_here(self):
        """Not an oversight. Fabric spells a Spark SQL cell `%%sql` and
        records `sparksql` as a `# META` *language*; `%%sparksql` is matched
        by neither the translator's SQL routing nor anything else, so there
        is no SQL cell here for publish to correct."""
        self.assertTrue(self._published("%%sparksql").startswith("%%sparksql\n"))

    def test_publish_rewrites_exactly_what_the_translator_calls_sql(self):
        """The ratchet between the two modules. A spelling the translator
        sends down its SQL path and publish leaves alone is the defect this
        PR fixes, reappearing under a different spelling."""
        from fabric_aidp.translate import fabric_notebook_to_spark as translator
        for first in ("%%sql", "%%SQL", "%%Sql", "%%tsql", "%%TSQL",
                      "%%sparksql", "%%sqlx", "%%spark", "%sql", "%%html"):
            with self.subTest(first=first):
                published = self._published(first)
                routed = bool(translator._SQL_MAGIC_RE.match(first))
                # "Did the line change", not "does it start with %sql" --
                # a `%sql` cell already does and would pass for free.
                rewritten = not published.startswith(first + "\n")
                self.assertEqual(routed, rewritten)
                if rewritten:
                    self.assertTrue(published.startswith("%sql\n"), published)

    def test_the_real_multi_line_shape_keeps_the_magic_alone_on_line_one(self):
        """The shape every real migrated SQL cell has, and the one no probe
        has run on AIDP: `%sql` by itself, the SQL below it. Pinned so that
        if the line-magic reading turns out to be the right one -- and the
        body has to be folded onto the magic line -- this test is what fails
        and says where."""
        fixture = (Path(__file__).resolve().parent.parent / "fabric_aidp"
                   / "fixtures" / "demo-workspace"
                   / "06_Sql_Summary.Notebook" / "notebook-content.py")
        cell = json.loads(to_ipynb(fixture.read_text(encoding="utf-8")))["cells"][0]
        self.assertEqual(cell["source"][0], "%sql\n")
        self.assertTrue(cell["source"][1].startswith("SELECT policy_no"))
        # And nothing carries the language, so `metadata.fabric` could not
        # have served as the hook instead of the magic text.
        self.assertEqual(cell["metadata"], {})

    def test_an_ipynb_input_gets_the_case_and_tsql_fix_too(self):
        """`_from_ipynb` is a separate path from `_cell`, so every spelling
        has to be re-checked on it rather than assumed."""
        source = json.dumps({"cells": [
            {"cell_type": "code", "metadata": {}, "outputs": [],
             "execution_count": None, "source": ["%%SQL\n", "SELECT 1"]},
            {"cell_type": "code", "metadata": {}, "outputs": [],
             "execution_count": None, "source": "%%tsql\nSELECT 2"},
            {"cell_type": "code", "metadata": {}, "outputs": [],
             "execution_count": None, "source": ["%%sparksql\n", "SELECT 3"]}],
            "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
        firsts = ["".join(c["source"]).split("\n")[0]
                  for c in json.loads(to_ipynb(source))["cells"]]
        self.assertEqual(firsts, ["%sql", "%sql", "%%sparksql"])

    def test_a_rewritten_cell_id_matches_the_source_it_labels(self):
        """The id is derived after the rewrite, so publishing the artifact
        and then converting what came back does not renumber the cell."""
        once = to_ipynb(f"# Fabric notebook source\n\n# CELL {STAR}\n\n"
                        f"%%sql\nSELECT 1\n")
        twice = to_ipynb(once)
        self.assertEqual([c["id"] for c in json.loads(once)["cells"]],
                         [c["id"] for c in json.loads(twice)["cells"]])


class PlanTests(unittest.TestCase):
    def test_every_notebook_is_planned_as_an_ipynb(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), cluster_key="cl-1")
        self.assertEqual([Path(u["remote"]).suffix for u in planned["notebooks"]],
                         [".ipynb", ".ipynb"])

    def test_the_prefix_reaches_paths_and_job_names(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), prefix="mo", cluster_key="cl-1")
        self.assertTrue(all("/mo/" in u["remote"] for u in planned["notebooks"]))
        self.assertEqual(planned["jobs"][0]["name"], "mo_Daily")

    def test_task_paths_are_rewritten_to_where_the_notebook_will_land(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), prefix="mo", cluster_key="cl-1")
        paths = [t["notebookPath"] for t in planned["jobs"][0]["definition"]["tasks"]]
        self.assertEqual(paths, ["/Workspace/mo/01_Ingest.ipynb",
                                 "/Workspace/mo/02_Agg.ipynb"])

    def test_depends_on_is_preserved(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), cluster_key="cl-1")
        tasks = planned["jobs"][0]["definition"]["tasks"]
        self.assertEqual(tasks[1]["dependsOn"], [{"taskKey": "a"}])

    def test_the_cluster_key_is_attached_to_every_task(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), cluster_key="cl-1")
        for task in planned["jobs"][0]["definition"]["tasks"]:
            self.assertEqual(task["cluster"], {"clusterKey": "cl-1"})


class RefusalTests(unittest.TestCase):
    def test_a_job_naming_a_notebook_we_did_not_produce_is_refused(self):
        with TemporaryDirectory() as tmp:
            out = _migration(tmp, notebooks=("01_Ingest",))
            planned = plan_publish(out, cluster_key="cl-1")
        self.assertEqual(planned["jobs"], [])
        self.assertIn("02_Agg", str(planned["blocked"]))

    def test_no_cluster_key_refuses_the_job_rather_than_creating_a_dead_one(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp))
        self.assertEqual(planned["jobs"], [])
        self.assertIn("cluster", str(planned["blocked"]))

    def test_a_directory_without_a_report_is_refused(self):
        with TemporaryDirectory() as tmp:
            (Path(tmp) / "notebooks").mkdir()
            with self.assertRaises(PublishError) as caught:
                plan_publish(tmp)
        self.assertIn("report.json", str(caught.exception))

    def test_a_missing_directory_is_refused(self):
        with self.assertRaises(PublishError):
            plan_publish("/nonexistent/migration")

    def test_an_artifact_the_report_does_not_list_is_not_published(self):
        # migrate never deletes: a job file left by an earlier run, for a
        # pipeline that is now blocked, must not be created.
        with TemporaryDirectory() as tmp:
            out = _migration(tmp)
            (out / "notebooks" / "99_Stale.py").write_text(NOTEBOOK, encoding="utf-8")
            report = json.loads((out / "report.json").read_text(encoding="utf-8"))
            report["results"] = [r for r in report["results"] if r["kind"] != "pipeline_job"]
            report["results"].append({"asset_id": "pipeline.Daily", "kind": "pipeline_job",
                                      "status": "blocked"})
            (out / "report.json").write_text(json.dumps(report), encoding="utf-8")
            planned = plan_publish(out, cluster_key="cl-1")
        self.assertEqual(planned["jobs"], [])
        self.assertNotIn("99_Stale", str(planned["notebooks"]))

    def test_a_prefix_aidp_would_reject_is_refused_before_anything_is_sent(self):
        with TemporaryDirectory() as tmp:
            for prefix in ("ahmed.s", "1st", "my-team"):
                with self.subTest(prefix=prefix), self.assertRaises(PublishError):
                    plan_publish(_migration(Path(tmp) / prefix), prefix=prefix,
                                 cluster_key="cl-1")

    def test_a_prefix_with_a_trailing_newline_is_refused(self):
        with TemporaryDirectory() as tmp:
            with self.assertRaises(PublishError):
                plan_publish(_migration(tmp), prefix="mo\n", cluster_key="cl-1")

    def test_every_pipeline_sharing_a_job_name_is_refused(self):
        with TemporaryDirectory() as tmp:
            out = _migration(tmp)
            report = json.loads((out / "report.json").read_text(encoding="utf-8"))
            job = (out / "jobs" / "Daily.job.json").read_text(encoding="utf-8")
            (out / "jobs" / "Daily_2.job.json").write_text(job, encoding="utf-8")
            report["results"].append({"asset_id": "pipeline.Daily 2", "kind": "pipeline_job",
                                      "status": "ok", "output_path": "jobs/Daily_2.job.json"})
            (out / "report.json").write_text(json.dumps(report), encoding="utf-8")
            planned = plan_publish(out, cluster_key="cl-1")
        self.assertEqual(planned["jobs"], [])
        self.assertIn("Daily.job.json, Daily_2.job.json", str(planned["blocked"]))

    def test_an_interrupted_migration_is_refused(self):
        with TemporaryDirectory() as tmp:
            out = _migration(tmp)
            (out / ".fabric-aidp-migration-in-progress").write_text("x", encoding="utf-8")
            with self.assertRaises(PublishError) as caught:
                plan_publish(out, cluster_key="cl-1")
        self.assertIn("interrupted", str(caught.exception))

    def test_a_report_path_outside_the_migration_is_refused(self):
        with TemporaryDirectory() as tmp:
            out = _migration(tmp, jobs=False, notebooks=())
            (out / "report.json").write_text(json.dumps({"results": [
                {"kind": "notebook", "status": "ok", "output_path": "../x.py"}]}),
                encoding="utf-8")
            with self.assertRaises(PublishError):
                plan_publish(out, cluster_key="cl-1")

    def test_planning_contacts_nothing_and_needs_no_credentials(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), cluster_key="cl-1")
        self.assertFalse(planned.get("applied"))


class UnconvertibleNotebookTests(unittest.TestCase):
    """`publish --apply` crashed with IpynbParseError ("not valid JSON:
    Expecting value: line 1 column 1") after uploading ~30 notebooks and
    creating no job, because the conversion ran inside the upload loop."""

    def test_it_is_refused_in_the_plan_with_the_reason(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_unconvertible(_migration(tmp)), prefix="mo",
                                   cluster_key="cl-1")
        self.assertEqual([u["remote"] for u in planned["notebooks"]],
                         ["/Workspace/mo/01_Ingest.ipynb", "/Workspace/mo/02_Agg.ipynb"])
        [refused] = planned["refused_notebooks"]
        self.assertEqual(refused["notebook"], "03_No_Header.py")
        self.assertIn("cannot be converted to .ipynb", refused["reason"])
        self.assertIn("not valid JSON", refused["reason"])
        # A job that does not use it is untouched.
        self.assertEqual([j["name"] for j in planned["jobs"]], ["mo_Daily"])

    def test_a_job_that_runs_it_is_refused_and_says_why(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_unconvertible(_migration(tmp), in_job=True),
                                   prefix="mo", cluster_key="cl-1")
        self.assertEqual([j["name"] for j in planned["jobs"]], ["mo_Daily"])
        [blocked] = planned["blocked"]
        self.assertEqual(blocked["job"], "Bad")
        self.assertIn("03_No_Header.py", blocked["reason"])
        self.assertIn("cannot be converted", blocked["reason"])

    def test_apply_never_sends_it_and_publishes_the_rest(self):
        fake = _FakeWorkspace()
        with TemporaryDirectory() as tmp:
            out = _unconvertible(_migration(tmp), in_job=True)
            with mock.patch.object(publisher_mod, "AidpClient", fake):
                result = publish(out, workspace_key="ws", cluster_key="cl-1",
                                 prefix="mo", apply=True)
        self.assertEqual(fake.uploaded, ["/Workspace/mo/01_Ingest.ipynb",
                                         "/Workspace/mo/02_Agg.ipynb"])
        self.assertEqual(fake.created, ["mo_Daily"])
        self.assertEqual(len(result["refused_notebooks"]), 1)

    def _cli(self, argv):
        import contextlib, io
        from fabric_aidp.cli import main
        buffer = io.StringIO()
        with mock.patch.object(publisher_mod, "AidpClient", _FakeWorkspace()), \
                contextlib.redirect_stdout(buffer):
            code = main(argv)
        return code, buffer.getvalue()

    def test_apply_exits_1_like_any_other_refusal(self):
        with TemporaryDirectory() as tmp:
            out = _unconvertible(_migration(tmp))
            code, text = self._cli(["publish", str(out), "--apply",
                                    "--workspace-key", "ws", "--cluster-key", "cl-1",
                                    "--prefix", "mo"])
        self.assertEqual(code, 1, text)
        self.assertIn("REFUSED  notebook 03_No_Header.py", text)
        self.assertIn("uploaded 2/3 notebook(s)", text)

    def test_the_dry_run_shows_it_and_exits_0(self):
        with TemporaryDirectory() as tmp:
            out = _unconvertible(_migration(tmp))
            code, text = self._cli(["publish", str(out), "--cluster-key", "cl-1",
                                    "--prefix", "mo"])
        self.assertEqual(code, 0, text)
        self.assertIn("REFUSED  notebook 03_No_Header.py", text)
        self.assertIn("would upload 2 notebook(s) and create 1 job(s); 1 refused.", text)


class NonDefaultWorkspaceRootTests(unittest.TestCase):
    """`workspace_root` moves where notebooks LAND; it does not move where the
    migration's job files say they ARE.

    A `.job.json` names its notebooks by the path `migrate` wrote --
    `runner.WORKSPACE_PREFIX`, "/Workspace/<name>.py" -- whatever root
    publish is later given. So the three lookups publish checks job tasks
    against (uploaded, unconvertible, parameters read back) have to be keyed
    on that path, and only the upload target on `workspace_root`. Keying
    them on `workspace_root`, as review suggested, makes every job under a
    non-default root "point at notebooks this migration did not produce";
    each assertion below fails that way under that change (tried).
    """

    ROOT = "/Workspace/team"

    def test_jobs_still_resolve_and_land_under_the_root(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), prefix="mo", cluster_key="cl-1",
                                   workspace_root=self.ROOT)
        self.assertEqual(planned["blocked"], [])
        self.assertEqual([u["remote"] for u in planned["notebooks"]],
                         ["/Workspace/team/mo/01_Ingest.ipynb",
                          "/Workspace/team/mo/02_Agg.ipynb"])
        paths = [t["notebookPath"] for t in planned["jobs"][0]["definition"]["tasks"]]
        self.assertEqual(paths, ["/Workspace/team/mo/01_Ingest.ipynb",
                                 "/Workspace/team/mo/02_Agg.ipynb"])

    def test_an_unconvertible_notebook_is_still_the_reason_its_job_is_refused(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_unconvertible(_migration(tmp), in_job=True),
                                   prefix="mo", cluster_key="cl-1",
                                   workspace_root=self.ROOT)
        [blocked] = planned["blocked"]
        self.assertEqual(blocked["job"], "Bad")
        self.assertIn("cannot be converted", blocked["reason"])
        self.assertIn("03_No_Header.py", blocked["reason"])

    def test_the_parameter_cross_check_still_runs(self):
        cross = PublishParameterCrossCheckTests()
        with TemporaryDirectory() as tmp:
            planned = plan_publish(cross._out(tmp), prefix="mo", cluster_key="cl-1",
                                   workspace_root=self.ROOT)
        self.assertEqual(
            [f["rule"] for f in planned["findings"]
             if f["rule"] == "NB37_TASK_PARAMETER_IGNORED"],
            ["NB37_TASK_PARAMETER_IGNORED"])

    def test_the_lookup_key_is_the_root_migrate_writes(self):
        # Two constants with one meaning, in two modules. Tied here so a
        # change to one cannot silently strand every job.
        from fabric_aidp.migrate import runner
        self.assertEqual(publisher_mod.MIGRATED_ROOT, runner.WORKSPACE_PREFIX)


class PrefixRequiredTests(unittest.TestCase):
    """An empty --prefix skipped validation (`if prefix and ...`) and is the
    CLI default: `--apply` without it put every notebook straight into
    /Workspace and created jobs under their bare pipeline names."""

    def _cli(self, argv):
        import contextlib, io
        from fabric_aidp.cli import main
        fake = _FakeWorkspace()
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(publisher_mod, "AidpClient", fake), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(argv)
        return code, out.getvalue(), err.getvalue(), fake

    def test_apply_without_a_prefix_sends_nothing(self):
        fake = _FakeWorkspace()
        with TemporaryDirectory() as tmp:
            with mock.patch.object(publisher_mod, "AidpClient", fake), \
                    self.assertRaises(PublishError) as caught:
                publish(_migration(tmp), workspace_key="ws", cluster_key="cl-1",
                        apply=True)
        self.assertIn("--prefix", str(caught.exception))
        self.assertEqual((fake.uploaded, fake.created), ([], []))

    def test_the_cli_names_the_flag_and_exits_2(self):
        with TemporaryDirectory() as tmp:
            code, _, err, fake = self._cli(
                ["publish", str(_migration(tmp)), "--apply", "--workspace-key", "ws",
                 "--cluster-key", "cl-1"])
        self.assertEqual(code, 2)
        self.assertIn("--prefix is required with --apply", err)
        self.assertEqual((fake.uploaded, fake.created), ([], []))

    def test_a_dry_run_without_a_prefix_still_plans_but_warns(self):
        with TemporaryDirectory() as tmp:
            code, out, _, _ = self._cli(
                ["publish", str(_migration(tmp)), "--cluster-key", "cl-1"])
        self.assertEqual(code, 0)
        self.assertIn("warning: no --prefix", out)
        self.assertIn("/Workspace/01_Ingest.ipynb", out)

    def test_a_dry_run_with_a_prefix_does_not_warn(self):
        with TemporaryDirectory() as tmp:
            _, out, _, _ = self._cli(
                ["publish", str(_migration(tmp)), "--cluster-key", "cl-1",
                 "--prefix", "mo"])
        self.assertNotIn("warning", out)


class _PagedClient(AidpClient):
    """Serves list-jobs from canned pages keyed by the --page token."""

    def __init__(self, pages):
        super().__init__(workspace_key="ws")
        self.pages, self.calls = pages, []

    def _run(self, args, body=None):
        self.calls.append(list(args))
        token = json.loads(args[args.index("--page") + 1]) if "--page" in args else None
        items, next_token = self.pages[token]
        headers = {"opc-next-page": next_token} if next_token else {}
        return {"status": 200, "headers": headers, "data": {"items": items}}


class ClientTests(unittest.TestCase):
    def test_list_jobs_follows_every_page(self):
        # tpcds holds 62 jobs; the API's default page is 25. A page-1-only
        # read reported an existing job as absent and create-job then failed.
        client = _PagedClient({None: ([{"name": "a"}], "p2"),
                               "p2": ([{"name": "b"}], "p3"),
                               "p3": ([{"name": "mo_Daily"}], None)})
        self.assertEqual([j["name"] for j in client.list_jobs()],
                         ["a", "b", "mo_Daily"])
        self.assertEqual(len(client.calls), 3)
        self.assertIn("--limit", client.calls[0])

    def test_option_values_reach_the_cli_as_strings(self):
        # aidp-cli json.loads every option value: a bare `--path Infinity`
        # lists folder "inf", finds nothing, and the overwrite check fails open.
        seen = []

        class Recorder(AidpClient):
            def _run(self, args, body=None):
                seen.append(list(args))
                return {"status": 200, "headers": {}, "data": {"items": []}}

        Recorder(workspace_key="ws").folder_names("/Workspace/Infinity")
        path = seen[0][seen[0].index("--path") + 1]
        self.assertEqual(json.loads(path), "Infinity")

    def test_an_error_body_on_stderr_keeps_its_code_and_message(self):
        done = mock.Mock(returncode=1, stdout="", stderr=json.dumps(
            {"status": 409, "code": "JOB_VALIDATE_0031", "message": "exists"}))
        with mock.patch.object(aidp_client.shutil, "which", return_value="aidp"),                 mock.patch.object(aidp_client.subprocess, "run", return_value=done):
            with self.assertRaises(AidpError) as caught:
                AidpClient(workspace_key="ws").list_jobs()
        self.assertIn("AIDP 409 JOB_VALIDATE_0031: exists", str(caught.exception))


class _FakeWorkspace:
    """Stands in for AidpClient inside publish(): records every write."""

    def __init__(self, *, folders=None, jobs=(), fail_reads=False, cluster_issue=""):
        self.folders, self.jobs, self.fail_reads = folders or {}, list(jobs), fail_reads
        self.cluster_issue = cluster_issue
        self.uploaded, self.created = [], []

    def __call__(self, **kwargs):
        return self

    def available(self):
        return True

    def folder_names(self, folder):
        if self.fail_reads:
            raise AidpError("AIDP 500 InternalError", status=500)
        return set(self.folders.get(folder, ()))

    def list_jobs(self):
        return [{"name": n} for n in self.jobs]

    def cluster_problem(self, key):
        return self.cluster_issue

    def put_notebook(self, path, text):
        self.uploaded.append(path)

    def create_job(self, definition):
        self.created.append(definition["name"])
        return "job-key"


class ApplyTests(unittest.TestCase):
    def _apply(self, fake, tmp):
        with mock.patch.object(publisher_mod, "AidpClient", fake):
            return publish(_migration(tmp), workspace_key="ws", cluster_key="cl-1",
                           prefix="mo", apply=True)

    def test_a_cluster_that_cannot_run_jobs_is_refused_before_anything_is_sent(self):
        """Found in review: the Default Master Catalog Compute key was
        accepted and the job failed only at run time, after every upload."""
        fake = _FakeWorkspace(cluster_issue="cluster x is this workspace's "
                                            "Default Master Catalog Compute")
        with TemporaryDirectory() as tmp:
            with self.assertRaises(PublishError) as caught:
                self._apply(fake, tmp)
        self.assertIn("Default Master Catalog Compute", str(caught.exception))
        self.assertIn("nothing was sent", str(caught.exception))
        self.assertEqual((fake.uploaded, fake.created), ([], []))

    def test_a_clean_workspace_gets_every_notebook_and_the_job(self):
        fake = _FakeWorkspace()
        with TemporaryDirectory() as tmp:
            self._apply(fake, tmp)
        self.assertEqual(fake.uploaded, ["/Workspace/mo/01_Ingest.ipynb",
                                         "/Workspace/mo/02_Agg.ipynb"])
        self.assertEqual(fake.created, ["mo_Daily"])

    def test_an_existing_notebook_is_never_overwritten(self):
        # `aidp notebook update-content` replaced a pre-existing notebook on a
        # live workspace without a word; publish has to check first.
        fake = _FakeWorkspace(folders={"/Workspace/mo": {"01_Ingest.ipynb"}})
        with TemporaryDirectory() as tmp:
            result = self._apply(fake, tmp)
        self.assertEqual(fake.uploaded, ["/Workspace/mo/02_Agg.ipynb"])
        self.assertEqual(result["notebooks"][0]["status"], "skipped")
        # The job would run whatever notebook is already at that path.
        self.assertEqual(fake.created, [])
        self.assertEqual(result["jobs"][0]["status"], "refused")

    def test_a_second_run_changes_nothing(self):
        fake = _FakeWorkspace(folders={"/Workspace/mo": {"01_Ingest.ipynb", "02_Agg.ipynb"}},
                              jobs=["mo_Daily"])
        with TemporaryDirectory() as tmp:
            result = self._apply(fake, tmp)
        self.assertEqual((fake.uploaded, fake.created), ([], []))
        self.assertIn("already exists", result["jobs"][0]["reason"])

    def test_a_failed_job_can_be_finished_by_reusing_this_runs_notebooks(self):
        # Notebooks uploaded, create-job failed: a plain re-run refuses the job
        # forever. The explicit flag lets it point at the notebooks that exist.
        fake = _FakeWorkspace(folders={"/Workspace/mo": {"01_Ingest.ipynb", "02_Agg.ipynb"}})
        with TemporaryDirectory() as tmp:
            with mock.patch.object(publisher_mod, "AidpClient", fake):
                result = publish(_migration(tmp), workspace_key="ws", cluster_key="cl-1",
                                 prefix="mo", apply=True, reuse_existing=True)
        self.assertEqual((fake.uploaded, fake.created), ([], ["mo_Daily"]))
        self.assertEqual(result["jobs"][0]["status"], "created")

    def test_an_unreadable_workspace_sends_nothing(self):
        fake = _FakeWorkspace(fail_reads=True)
        with TemporaryDirectory() as tmp:
            with self.assertRaises(PublishError):
                self._apply(fake, tmp)
        self.assertEqual((fake.uploaded, fake.created), ([], []))

    def _cli(self, fake, out):
        import contextlib, io
        from fabric_aidp.cli import main
        with mock.patch.object(publisher_mod, "AidpClient", fake),                 contextlib.redirect_stdout(io.StringIO()):
            return main(["publish", str(out), "--apply", "--workspace-key", "ws",
                         "--cluster-key", "cl-1", "--prefix", "mo"])

    def test_apply_exits_non_zero_when_a_job_is_refused(self):
        # It exited 0 with "created 0/0 job(s)", so CI went green.
        fake = _FakeWorkspace(folders={"/Workspace/mo": {"01_Ingest.ipynb"}})
        with TemporaryDirectory() as tmp:
            self.assertEqual(self._cli(fake, _migration(tmp)), 1)

    def test_an_idempotent_re_run_exits_zero(self):
        fake = _FakeWorkspace(folders={"/Workspace/mo": {"01_Ingest.ipynb", "02_Agg.ipynb"}},
                              jobs=["mo_Daily"])
        with TemporaryDirectory() as tmp:
            self.assertEqual(self._cli(fake, _migration(tmp)), 0)

    def test_object_paths_drop_the_workspace_mount(self):
        self.assertEqual(AidpClient.object_path("/Workspace/mo/a.ipynb"), "mo/a.ipynb")
        self.assertEqual(AidpClient.object_path("/Workspace"), "")
        self.assertEqual(AidpClient.object_path("/Workspace/"), "")


class ParametersCellTests(unittest.TestCase):
    """AIDP gives a notebook its task parameters through
    oidlUtils.parameters.getParameter and runs a Fabric parameters cell as
    written. Measured live: with run_date=2026-09-01 and limit=100 on the
    task, the cell's own `run_date = "2000-01-01"` and `limit = 5` were what
    the notebook used -- tagged `parameters` or not -- until the cell re-read
    them; then it saw '2026-09-01' and 100, and an unpassed `ratio = 0.5` and
    `full = False` kept their defaults, typed."""

    CELL = (f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\n"
            f"run_date = \"2000-01-01\"\nlimit = 5\nratio = 0.5\nfull = False\n"
            f"regions = [\"a\"]\nstart = run_date\n\n# CELL {STAR}\n\nprint(run_date)\n")

    def _cells(self, text):
        return ["".join(c["source"]) for c in json.loads(to_ipynb(text))["cells"]]

    def test_each_literal_is_re_read_from_the_task_with_its_type(self):
        # Through `_aidp_parameter`, not a bare `oidlUtils` reference: see
        # OidlUtilsGuardTests for why the indirection is there.
        cell = self._cells(self.CELL)[0]
        self.assertIn("run_date = _aidp_parameter('run_date', '2000-01-01')", cell)
        self.assertIn("limit = int(_aidp_parameter('limit', '5'))", cell)
        self.assertIn("ratio = float(_aidp_parameter('ratio', '0.5'))", cell)
        self.assertIn("full = str(_aidp_parameter('full', 'False'))"
                      ".strip().lower() in ('true', '1')", cell)
        self.assertIn("oidlUtils.parameters.getParameter(name, default)", cell)

    def test_the_fabric_defaults_stay_and_come_first(self):
        cell = self._cells(self.CELL)[0]
        self.assertTrue(cell.startswith('run_date = "2000-01-01"\nlimit = 5\n'), cell)

    def test_non_literal_assignments_are_not_parameters(self):
        cell = self._cells(self.CELL)[0]
        self.assertNotIn("getParameter('regions'", cell)
        self.assertNotIn("getParameter('start'", cell)

    def test_the_result_still_compiles(self):
        compile(self._cells(self.CELL)[0], "<parameters>", "exec")

    def test_an_ordinary_cell_and_an_unparseable_parameters_cell_are_untouched(self):
        broken = f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\nx = (\n"
        self.assertEqual(self._cells(broken)[0], "x = (")
        self.assertEqual(self._cells(self.CELL)[1], "print(run_date)")


class WidenedParameterShapeTests(unittest.TestCase):
    """The two halves of this feature disagreed about what a literal is.

    `_parameters` in the pipeline translator carries anything Fabric wrote as
    a JSON scalar. `_parameter_lines` here matched only `ast.Assign` -> one
    `ast.Name` -> `ast.Constant`, and `offset = -1` is not an `ast.Constant`
    -- it is `UnaryOp(USub, Constant(1))`. So the job supplied `offset`, the
    notebook ignored it, and nothing said so: precisely the defect this PR
    exists to close, for every negative default. `limit: int = 5` is an
    `AnnAssign` and was missed for the same reason.

    MEASURED before this, one activity passing offset=-5 and limit=7 against
    a cell holding `offset = -1` and `limit: int = 5`:

        job       parameters: offset=-5, limit=7
        notebook  offset stays -1, limit stays 5   -- both ignored

    Neither shape appears in the vendored corpus, which is why no corpus
    test caught it; the halves were out of step all the same.
    """

    def _cell(self, body):
        text = (f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\n{body}\n")
        return "".join(json.loads(to_ipynb(text))["cells"][0]["source"])

    def test_a_negative_int_default_is_re_read(self):
        self.assertIn("offset = int(_aidp_parameter('offset', '-1'))",
                      self._cell("offset = -1"))

    def test_a_negative_float_default_is_re_read(self):
        self.assertIn("ratio = float(_aidp_parameter('ratio', '-0.5'))",
                      self._cell("ratio = -0.5"))

    def test_an_explicitly_positive_default_is_re_read(self):
        self.assertIn("offset = int(_aidp_parameter('offset', '1'))",
                      self._cell("offset = +1"))

    def test_an_annotated_assignment_is_re_read(self):
        self.assertIn("limit = int(_aidp_parameter('limit', '5'))",
                      self._cell("limit: int = 5"))

    def test_a_bare_annotation_declares_nothing_to_re_read(self):
        """`limit: int` binds no value, so there is no default to fall back
        to and nothing Fabric could have overridden."""
        self.assertNotIn("_aidp_parameter", self._cell("limit: int"))

    def test_not_is_not_a_sign(self):
        """`flag = not True` is an expression, not a signed literal."""
        self.assertNotIn("_aidp_parameter('flag'", self._cell("flag = not True"))

    def test_a_negated_bool_is_not_treated_as_a_number(self):
        """`-True` is 1 in Python and nobody writes it meaning a bool."""
        self.assertNotIn("_aidp_parameter('flag'", self._cell("flag = -True"))

    def test_a_target_that_is_not_a_name_is_not_a_parameter(self):
        """Fabric overrides a parameter by name. `cfg.limit` and `a, b` have
        none, so neither is one."""
        cell = self._cell("cfg = object()\ncfg.limit = 5\na, b = 1, 2")
        self.assertNotIn("_aidp_parameter('a'", cell)
        self.assertNotIn("_aidp_parameter('limit'", cell)

    def test_the_result_compiles_and_runs_on_its_defaults(self):
        cell = self._cell("offset = -1\nlimit: int = 5\nname = 'x'\nflag = False")
        namespace = {}
        exec(compile(cell, "<parameters>", "exec"), namespace)
        self.assertEqual(
            {k: namespace[k] for k in ("offset", "limit", "name", "flag")},
            {"offset": -1, "limit": 5, "name": "x", "flag": False})


class OidlUtilsGuardTests(unittest.TestCase):
    """A bare `oidlUtils` reference kills the notebook everywhere but AIDP.

    AIDP injects `oidlUtils` with no import, the way Fabric injects
    `notebookutils`. That was measured live and is not in question. What is
    in question is the *other* runtime: off AIDP the name is simply absent,
    so a bare reference raises NameError in the notebook's first cell and
    ends a notebook that, before the migration, ran perfectly well on its
    defaults. That is a worse failure than the one being fixed, and it lands
    on every reviewer who opens the artifact locally to check it -- which is
    this tool's whole pitch.

    MEASURED, the parameters cell of the demo notebook, executed with no
    oidlUtils in scope:

        bare reference   NameError: name 'oidlUtils' is not defined
        guarded          run_date='2000-01-01', limit=5   -- the Fabric defaults

    The repo's precedent for exactly this shape is NB08's DISPLAY_SHIM,
    which is deliberately defensive -- duck-typed rather than
    isinstance-checked, `IPython.display` imported inside a try -- and names
    its divergences in its own docstring. This matches it.
    """

    CELL = (f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\n"
            "run_date = \"2000-01-01\"\nlimit = 5\n")

    def _cell(self):
        return "".join(json.loads(to_ipynb(self.CELL))["cells"][0]["source"])

    def test_the_cell_runs_with_no_oidlutils_in_scope(self):
        namespace = {}
        exec(compile(self._cell(), "<parameters>", "exec"), namespace)
        self.assertEqual(namespace["run_date"], "2000-01-01")
        self.assertEqual(namespace["limit"], 5)

    def test_a_bare_reference_would_have_raised(self):
        """The failure mode, stated so it cannot quietly come back."""
        with self.assertRaises(NameError):
            exec("x = oidlUtils.parameters.getParameter('a', 'b')", {})

    def test_the_task_value_wins_when_oidlutils_is_there(self):
        class _Parameters:
            @staticmethod
            def getParameter(name, default):
                return {"run_date": "2026-09-01", "limit": "100"}.get(name, default)

        class _Utils:
            parameters = _Parameters()

        namespace = {"oidlUtils": _Utils()}
        exec(compile(self._cell(), "<parameters>", "exec"), namespace)
        self.assertEqual(namespace["run_date"], "2026-09-01")
        self.assertEqual(namespace["limit"], 100)

    def test_a_half_present_oidlutils_falls_back_rather_than_dying(self):
        """`AttributeError` is the other way "this runtime has no oidlUtils"
        arrives: the name bound to something without `parameters`."""
        namespace = {"oidlUtils": object()}
        exec(compile(self._cell(), "<parameters>", "exec"), namespace)
        self.assertEqual(namespace["run_date"], "2000-01-01")

    def test_an_error_from_getparameter_itself_is_not_swallowed(self):
        """Only NameError and AttributeError. A getParameter that fails for
        its own reasons must still be seen, not silently defaulted."""
        class _Parameters:
            @staticmethod
            def getParameter(name, default):
                raise RuntimeError("AIDP said no")

        class _Utils:
            parameters = _Parameters()

        with self.assertRaises(RuntimeError):
            exec(compile(self._cell(), "<parameters>", "exec"),
                 {"oidlUtils": _Utils()})

    def test_the_helper_is_defined_before_it_is_called(self):
        cell = self._cell()
        self.assertLess(cell.index("def _aidp_parameter("),
                        cell.index("run_date = _aidp_parameter("))

    def test_the_comment_says_why_the_re_reads_are_appended(self):
        """Review note 9. Appending leaves `start = run_date` stale, which is
        right only if Fabric's own overrides also arrive below this cell --
        they do, papermill injects them as the next cell -- and the reason
        has to be written down or the next reader will "fix" it."""
        cell = self._cell()
        self.assertIn("papermill", cell)
        self.assertIn("cell *below* this one", cell)


class ParametersCellFindingsTests(unittest.TestCase):
    """`to_ipynb` decided things in silence because it had nowhere to say them.

    It is the second translator in this tool and it runs *after* `verify`, so
    a parameters cell it could not parse and a parameter it could not re-read
    were both a silent pass-through -- and a test asserted the silence. Now
    it takes a findings list, `publish` collects it, and `publish` prints it
    before anything is sent.
    """

    def _findings(self, body):
        text = f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\n{body}\n"
        findings = []
        to_ipynb(text, findings=findings)
        return findings

    def test_an_unparseable_parameters_cell_is_named_not_passed_over(self):
        """Compare NB07_CELL_UNTOKENIZABLE, which names the construct."""
        [finding] = self._findings("x = (")
        self.assertEqual(finding.rule, "NB36_PARAMETER_CELL_UNPARSEABLE")
        self.assertEqual(finding.severity, "flag")
        self.assertIn("does not parse", finding.detail)
        self.assertIn("at line 1", finding.detail)
        self.assertIn("no task parameter reaches it", finding.detail)
        # CPython's own wording, whatever it is on this interpreter, rather
        # than a copy of it. 3.9 says "unexpected EOF while parsing" here and
        # 3.10 says "'(' was never closed"; pinning either would pass on one
        # interpreter and fail on another, and the floor is 3.9.
        with self.assertRaises(SyntaxError) as raised:
            ast.parse("x = (")
        self.assertIn(raised.exception.msg, finding.detail)

    def test_the_unparseable_cell_is_still_emitted_exactly_as_written(self):
        text = f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\nx = (\n"
        self.assertEqual(
            "".join(json.loads(to_ipynb(text))["cells"][0]["source"]), "x = (")

    def test_each_construct_that_cannot_be_re_read_is_named(self):
        cases = {"regions = ['a']": "a list",
                 "cfg = {'a': 1}": "a dict",
                 "pair = (1, 2)": "a tuple",
                 "nothing = None": "None",
                 "tag = f'{x}'": "an f-string",
                 "start = other": "a reference to another name",
                 "now = time()": "a call",
                 "total = 1 + 2": "an expression"}
        for body, phrase in cases.items():
            with self.subTest(body=body):
                [finding] = self._findings(body)
                self.assertEqual(finding.rule, "NB35_PARAMETER_NOT_RE_READ")
                self.assertEqual(finding.severity, "flag")
                self.assertIn(phrase, finding.detail)
                self.assertIn("is ignored here", finding.detail)

    def test_a_cell_of_literals_is_silent(self):
        self.assertEqual(self._findings("a = 1\nb = 'x'\nc = -2\nd: int = 3"), [])

    def test_a_notebook_with_no_parameters_cell_is_silent(self):
        findings = []
        to_ipynb(NOTEBOOK, findings=findings)
        self.assertEqual(findings, [])

    def test_omitting_the_findings_list_is_still_supported(self):
        """Every older caller passes nothing and must be unaffected."""
        self.assertIn('"cells"', to_ipynb(NOTEBOOK))


class IpynbParametersCellTests(unittest.TestCase):
    """The `.ipynb` half got none of this, and nothing said so.

    `migrate` writes every notebook artifact under a `.py` name whatever it
    read, and `ipynb_format` labels every code cell "CELL" -- never
    "PARAMETERS CELL". So the marker the `# CELL` format keys off does not
    exist on this path, and for the four-in-five of real exports that are
    Jupyter JSON the task's parameters arrived and were ignored, at PASS.

    MEASURED on the two-cell document below, before this:

        cell tagged `parameters`, source `run_date = "2000-01-01"`
        emitted source: unchanged, no finding, notebook uses 2000-01-01

    papermill's tag is what says "this is the parameters cell" here, #27
    deliberately preserves it, and it is what this keys off -- so both
    formats now behave the same way rather than one of them silently not.
    """

    def _doc(self, source, tags=("parameters",)):
        return json.dumps({
            "nbformat": 4, "nbformat_minor": 5, "metadata": {},
            "cells": [{"cell_type": "code", "id": "p",
                       "metadata": {"tags": list(tags)},
                       "execution_count": 2, "outputs": [{"output_type": "stream"}],
                       "source": source},
                      {"cell_type": "code", "id": "b", "metadata": {},
                       "execution_count": None, "outputs": [],
                       "source": ["print(run_date)\n"]}]})

    def _cell(self, source, **kw):
        return "".join(json.loads(to_ipynb(self._doc(source, **kw)))["cells"][0]["source"])

    def test_a_tagged_cell_is_re_read_the_same_way_the_py_format_is(self):
        cell = self._cell(['run_date = "2000-01-01"\n', "offset = -1"])
        self.assertIn("run_date = _aidp_parameter('run_date', '2000-01-01')", cell)
        self.assertIn("offset = int(_aidp_parameter('offset', '-1'))", cell)

    def test_the_cells_own_lines_come_first_and_unchanged(self):
        cell = self._cell(['run_date = "2000-01-01"\n', "offset = -1"])
        self.assertTrue(cell.startswith('run_date = "2000-01-01"\noffset = -1\n'), cell)

    def test_an_untagged_cell_is_left_alone(self):
        self.assertEqual(self._cell(["run_date = 'x'"], tags=()), "run_date = 'x'")

    def test_the_other_cells_are_untouched(self):
        out = json.loads(to_ipynb(self._doc(["run_date = 'x'"])))
        self.assertEqual(out["cells"][1]["source"], ["print(run_date)\n"])

    def test_the_rest_of_the_document_still_round_trips(self):
        """#27's invariants, re-asserted over a document this now edits."""
        out = json.loads(to_ipynb(self._doc(["run_date = 'x'"])))
        self.assertEqual([c["id"] for c in out["cells"]], ["p", "b"])
        self.assertEqual(out["cells"][0]["metadata"]["tags"], ["parameters"])
        self.assertEqual(out["cells"][0]["outputs"], [])
        self.assertIsNone(out["cells"][0]["execution_count"])

    def test_what_cannot_be_re_read_here_is_reported_too(self):
        findings = []
        to_ipynb(self._doc(["regions = ['a']"]), findings=findings)
        self.assertEqual([f.rule for f in findings], ["NB35_PARAMETER_NOT_RE_READ"])

    def test_the_result_compiles(self):
        compile(self._cell(['run_date = "2000-01-01"\n', "limit = 5"]),
                "<parameters>", "exec")


class PublishParameterCrossCheckTests(unittest.TestCase):
    """The one place in the tool holding both halves of this feature at once.

    The pipeline translator puts a parameter in the job knowing nothing about
    the notebook. The notebook re-reads what its own parameters cell declares
    knowing nothing about the job. Everywhere else the two live in different
    files and cannot disagree out loud -- `publish` reads both, so it is
    where the disagreement becomes visible.

    MEASURED before this, a job passing run_date and regions to a notebook
    whose cell reads back only run_date: `publish` planned the upload and the
    job, printed nothing, and exited 0. `regions` was supplied by AIDP and
    ignored by the notebook.
    """

    CELL = (f"# Fabric notebook source\n\n# PARAMETERS CELL {STAR}\n\n"
            "run_date = \"2000-01-01\"\nregions = [\"a\"]\n\n"
            f"# CELL {STAR}\n\nprint(run_date)\n")

    def _out(self, tmp):
        out = Path(tmp)
        (out / "notebooks").mkdir(parents=True)
        (out / "notebooks" / "01_Ingest.py").write_text(self.CELL, encoding="utf-8")
        (out / "jobs").mkdir()
        (out / "jobs" / "Daily.job.json").write_text(json.dumps({
            "name": "Daily", "tasks": [{
                "type": "NOTEBOOK_TASK", "taskKey": "a",
                "notebookPath": "/Workspace/01_Ingest.py", "source": "WORKSPACE",
                "parameters": [{"name": "run_date", "value": "2026-09-01"},
                               {"name": "regions", "value": "b"}]}]}),
            encoding="utf-8")
        (out / "report.json").write_text(json.dumps({"complete": True, "results": [
            {"asset_id": "notebook.01_Ingest", "kind": "notebook", "status": "ok",
             "output_path": "notebooks/01_Ingest.py"},
            {"asset_id": "pipeline.Daily", "kind": "pipeline_job", "status": "ok",
             "output_path": "jobs/Daily.job.json"}]}), encoding="utf-8")
        return out

    def _plan(self):
        with TemporaryDirectory() as tmp:
            return plan_publish(self._out(tmp), prefix="mo", cluster_key="cl-1")

    def test_a_parameter_the_notebook_does_not_read_is_named(self):
        ignored = [f for f in self._plan()["findings"]
                   if f["rule"] == "NB37_TASK_PARAMETER_IGNORED"]
        self.assertEqual(len(ignored), 1)
        self.assertIn("'regions'", ignored[0]["detail"])
        self.assertIn("has no effect", ignored[0]["detail"])
        # The migrated artifact's own name, not the prefixed one: the
        # finding is about Daily.job.json, which is what a reader has open.
        self.assertEqual(ignored[0]["job"], "Daily")

    def test_a_parameter_the_notebook_does_read_is_not_named(self):
        details = " ".join(f["detail"] for f in self._plan()["findings"]
                           if f["rule"] == "NB37_TASK_PARAMETER_IGNORED")
        self.assertNotIn("'run_date'", details)

    def test_the_cells_own_finding_travels_with_the_notebook_that_made_it(self):
        [note] = [f for f in self._plan()["findings"]
                  if f["rule"] == "NB35_PARAMETER_NOT_RE_READ"]
        self.assertEqual(note["notebook"], "01_Ingest.py")
        self.assertIn("'regions'", note["detail"])

    def test_the_dry_run_prints_them_and_counts_them(self):
        printed = []
        with TemporaryDirectory() as tmp:
            publish(self._out(tmp), workspace_key="ws", cluster_key="cl-1",
                    prefix="mo", log=printed.append)
        text = "\n".join(printed)
        self.assertIn("NB37_TASK_PARAMETER_IGNORED", text)
        self.assertIn("NB35_PARAMETER_NOT_RE_READ", text)
        self.assertIn("2 finding(s) from the .ipynb conversion", text)

    def test_a_job_and_notebook_that_agree_produce_nothing(self):
        with TemporaryDirectory() as tmp:
            planned = plan_publish(_migration(tmp), prefix="mo", cluster_key="cl-1")
        self.assertEqual(planned["findings"], [])



class SqlCellNeedsAMagicTests(unittest.TestCase):
    """A SQL cell with no magic at all was published as a Python cell.

    `_aidp_sql_magic` corrects a magic that is present. Three shapes carry
    none, and the translator has already applied its SQL rules to all three
    -- so the tool knows they are SQL and then wrote them into a
    `cell_type: "code"` cell under a Python kernelspec. An AIDP
    NOTEBOOK_TASK runs that as Python and dies on a SyntaxError, so a T-SQL
    notebook could never run after migration.

    Measured on this tree before the fix:

        notebook-content.sql        ["SELECT * FROM dbo.claim LIMIT 5"]
        cell `language: tsql`       ["SELECT * FROM dbo.claim LIMIT 5"]
        cell `language: sparksql`   ["SELECT * FROM claim"]

    `%sql` on its own line with the body below is the shape measured to work
    on the AIDP cluster; the one-liner is the shape that silently does
    nothing. See `_aidp_sql_magic` for that run.
    """

    HDR = "# Fabric notebook source\n\n# CELL ********************\n\n"

    def _cells(self, source, findings=None):
        return json.loads(to_ipynb(source, findings=findings))["cells"]

    def _with_language(self, body, language):
        return (self.HDR + body + "\n\n# METADATA ********************\n\n"
                + '# META {"language": "%s"}\n' % language)

    def test_a_tsql_notebooks_cell_gets_the_magic(self):
        cells = self._cells("-- Fabric notebook source\n\n"
                            "-- CELL ********************\n\nSELECT 1\n")
        self.assertEqual(cells[0]["source"], ["%sql\n", "SELECT 1"])

    def test_a_cell_recorded_as_tsql_gets_it(self):
        cells = self._cells(self._with_language("SELECT 1", "tsql"))
        self.assertEqual(cells[0]["source"], ["%sql\n", "SELECT 1"])

    def test_a_cell_recorded_as_sparksql_gets_it(self):
        cells = self._cells(self._with_language("SELECT 1", "sparksql"))
        self.assertEqual(cells[0]["source"], ["%sql\n", "SELECT 1"])

    def test_the_magic_is_on_its_own_line_not_folded_onto_the_body(self):
        """The cluster run: `%sql` alone on line 1 with SQL below created the
        table; `%sql CREATE TABLE ...` on one line created nothing and raised
        nothing. Folding these together would be a silent no-op."""
        cells = self._cells(self._with_language("SELECT 1\nFROM t", "sparksql"))
        self.assertEqual(cells[0]["source"][0], "%sql\n")
        self.assertNotIn("SELECT", cells[0]["source"][0])

    def test_a_python_cell_is_untouched(self):
        cells = self._cells(self.HDR + "x = 1\n")
        self.assertEqual(cells[0]["source"], ["x = 1"])

    def test_a_cell_that_already_has_a_magic_gets_no_second_one(self):
        """`_aidp_sql_magic` runs first. A second `%sql` would make the
        first line data."""
        # A bare `%%sql`, which is what `to_ipynb` actually receives: the
        # translator has already stripped the `# MAGIC ` prefixes by then.
        cells = self._cells(self.HDR + "%%sql\nSELECT 1\n")
        self.assertEqual(cells[0]["source"], ["%sql\n", "SELECT 1"])

    def test_a_markdown_cell_never_gets_it(self):
        cells = self._cells("-- Fabric notebook source\n\n"
                            "-- MARKDOWN ********************\n\n-- # Title\n")
        self.assertEqual(cells[0]["cell_type"], "markdown")
        self.assertNotIn("%sql\n", cells[0]["source"])

    def test_it_says_so_rather_than_doing_it_silently(self):
        findings = []
        self._cells(self._with_language("SELECT 1", "sparksql"), findings)
        rules = [f.rule for f in findings]
        self.assertIn("NB38_SQL_CELL_NEEDS_MAGIC", rules)
        detail = next(f.detail for f in findings
                      if f.rule == "NB38_SQL_CELL_NEEDS_MAGIC")
        self.assertIn("sparksql", detail)
        self.assertIn("as Python", detail)

    def test_the_ipynb_path_gets_it_too(self):
        """39 of 49 notebooks in microsoft/fabric-toolbox arrive as .ipynb,
        and `_from_ipynb` has twice been the half that silently missed a
        fix the block path got."""
        src = json.dumps({
            "nbformat": 4, "nbformat_minor": 5,
            "metadata": {"language_info": {"name": "python"}},
            "cells": [{"cell_type": "code", "id": "a1",
                       "metadata": {"fabric": {"language": "sparksql"}},
                       "source": ["SELECT 1"]}]})
        self.assertEqual(self._cells(src)[0]["source"], ["%sql\n", "SELECT 1"])

    def test_an_ipynb_recorded_as_sql_at_the_notebook_level_gets_it(self):
        src = json.dumps({
            "nbformat": 4, "nbformat_minor": 5,
            "metadata": {"language_info": {"name": "tsql"}},
            "cells": [{"cell_type": "code", "id": "a1", "metadata": {},
                       "source": ["SELECT 1"]}]})
        self.assertEqual(self._cells(src)[0]["source"], ["%sql\n", "SELECT 1"])

class SqlMagicShapeTests(unittest.TestCase):
    """The two magic shapes AIDP does not run, measured on the cluster
    2026-09-30: `%sql <stmt>` on one line reported SUCCESS and created
    nothing; an indented `  %sql` ran the cell as Python and failed."""

    def _first_cell(self, body):
        src = f"# Fabric notebook source\n\n# CELL {STAR}\n\n{body}\n"
        return "".join(json.loads(to_ipynb(src))["cells"][0]["source"])

    def test_a_one_liner_puts_the_statement_below_the_magic(self):
        self.assertEqual(self._first_cell("%%sql SELECT 2"), "%sql\nSELECT 2")

    def test_an_indented_magic_is_written_at_column_zero(self):
        self.assertEqual(self._first_cell("  %%SQL\nSELECT 3"), "%sql\nSELECT 3")

    def test_a_one_liner_with_more_lines_keeps_them_in_order(self):
        self.assertEqual(self._first_cell("%%sql SELECT a\nFROM t"), "%sql\nSELECT a\nFROM t")

    def test_the_working_shape_is_unchanged(self):
        self.assertEqual(self._first_cell("%%sql\nSELECT 1"), "%sql\nSELECT 1")

    def test_an_ipynb_list_source_one_liner_is_split_too(self):
        source = json.dumps({"cells": [{"cell_type": "code", "metadata": {}, "outputs": [],
                                        "execution_count": None, "source": ["%%sql SELECT 4"]}],
                             "metadata": {}, "nbformat": 4, "nbformat_minor": 5})
        self.assertEqual("".join(json.loads(to_ipynb(source))["cells"][0]["source"]),
                         "%sql\nSELECT 4")


if __name__ == "__main__":
    unittest.main()

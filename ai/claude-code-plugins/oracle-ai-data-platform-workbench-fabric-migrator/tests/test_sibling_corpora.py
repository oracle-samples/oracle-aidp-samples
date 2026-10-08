"""Run the translators over three corpora of real, third-party Fabric exports.

Real third-party Fabric exports, vendored under `tests/fixtures/real/corpora/`
-- but only the subset whose source repository carries a permissive licence.
Each source repo's licence was checked before anything was copied; 71 of the
original 149 files are deliberately absent because 59 carry no licence at all,
2 are GPL-2.0, and 10 have a licence that cannot be identified. See the NOTICE
there.

Because they ship, these tests run for everyone rather than skipping.
FABRIC_SIBLING_FIXTURES still overrides the path if you hold the fuller set
locally.

They earn their keep: wiring them up found a Synapse-header notebook this tool
rejected outright, 2 notebook references hidden inside ForEach loops, and 17
pipeline-to-pipeline ordering edges it never captured.
"""
from __future__ import annotations

import glob
import json
import os
import pathlib
import tempfile
import unittest

from fabric_aidp.inventory import pipeline as pipeline_mod
from fabric_aidp.publish.to_ipynb import task_parameters_read, to_ipynb
from fabric_aidp.inventory.git_workspace import discover_items
from fabric_aidp.translate import fabric_notebook_to_spark as nb2spark
from fabric_aidp.translate import pipeline_to_aidp_job as p2j
from fabric_aidp.translate import tsql_to_spark_sql as tsql

# Vendored in this repository -- but only the 78 of 149 whose source repo
# carries a permissive licence. See tests/fixtures/real/corpora/NOTICE for what
# was left out and why. FABRIC_SIBLING_FIXTURES points at the fuller set if you
# have it locally; the ratchets below are sized to what ships.
FIXTURES = pathlib.Path(os.environ.get(
    "FABRIC_SIBLING_FIXTURES",
    pathlib.Path(__file__).resolve().parent / "fixtures" / "real" / "corpora")
).expanduser()

# Ratchets. Raise them when coverage improves; never lower one to make a
# change pass -- that is the whole point of having them.
MIN_NOTEBOOKS = 25
MIN_WAREHOUSE = 9
MIN_PIPELINES = 30
MIN_ACTIVITIES = 108
MIN_PIPELINE_EDGES = 17
MIN_JOBS_EMITTED = 15
MIN_JOB_TASKS = 20


def _corpus(name, pattern):
    return sorted(glob.glob(str(FIXTURES / name / pattern)))


requires_corpora = unittest.skipUnless(
    (FIXTURES / "notebooks").is_dir(), f"sibling fixtures not present at {FIXTURES}")


@requires_corpora
class NotebookCorpusTests(unittest.TestCase):
    """32 real notebook-content.py files from public repositories."""

    @classmethod
    def setUpClass(cls):
        cls.results = []
        for path in _corpus("notebooks", "[0-9]*.py"):
            source = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")
            cls.results.append((os.path.basename(path),
                                nb2spark.translate(source, namespace="ns")))

    def test_the_corpus_is_present_and_whole(self):
        self.assertGreaterEqual(len(self.results), MIN_NOTEBOOKS)

    def test_every_notebook_parses(self):
        bad = [name for name, r in self.results
               if any(f.rule == "NB06_UNPARSEABLE" for f in r.findings)]
        self.assertEqual(bad, [], f"unparseable: {bad}")

    def test_no_notebook_crashes_the_translator(self):
        # translate() catching everything is the contract; this asserts it.
        self.assertEqual(len(self.results), len(_corpus("notebooks", "[0-9]*.py")))

    def test_output_is_never_silently_empty(self):
        empty = [name for name, r in self.results if not r.translated_sql.strip()]
        self.assertEqual(empty, [], f"empty output: {empty}")


@requires_corpora
class WarehouseCorpusTests(unittest.TestCase):
    """48 real Warehouse objects. Exercises the rules ported back from the
    sibling after its live-tenant run -- SQ75, SQ02 and SQ74 fire 23 times
    between them here, and every one of those was invalid SQL before."""

    @classmethod
    def setUpClass(cls):
        cls.results = []
        for path in _corpus("warehouse", "[0-9]*.sql"):
            sql = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")
            cls.results.append((os.path.basename(path), tsql.translate(sql, kind="other")))

    def test_the_corpus_is_present_and_whole(self):
        self.assertGreaterEqual(len(self.results), MIN_WAREHOUSE)

    def test_no_object_crashes_the_rules(self):
        self.assertEqual(len(self.results), len(_corpus("warehouse", "[0-9]*.sql")))

    def test_no_object_translates_to_nothing(self):
        empty = [name for name, r in self.results
                 if not r.translated_sql.strip() and r.source_sql.strip()]
        self.assertEqual(empty, [], f"translated away entirely: {empty}")

    def test_real_objects_exercise_the_rules(self):
        fired = {f.rule for _, r in self.results for f in r.findings}
        self.assertTrue(fired, "48 real Warehouse objects and no rule fired")

    @unittest.skipUnless(os.environ.get("FABRIC_SIBLING_FIXTURES"),
                         "needs the fuller corpus; the vendored subset does not "
                         "contain these constructs")
    def test_the_ported_rules_fire_on_the_fuller_corpus(self):
        """SQ75/SQ02/SQ74 fire 23 times across the full 48 Warehouse objects.

        They cannot be asserted against what ships: every file containing an
        ALTER TABLE ADD CONSTRAINT, a GO or a WITH SCHEMABINDING came from a
        repository with no licence, so none of them could be vendored. The
        rules themselves are covered unconditionally in test_tsql_rules.py;
        this only checks they still fire on real-world input.
        """
        fired = {f.rule for _, r in self.results for f in r.findings}
        for rule in ("SQ75_ALTER_ADD_CONSTRAINT", "SQ02_GO_BATCH",
                     "SQ74_SCHEMABINDING"):
            self.assertIn(rule, fired, f"{rule} never fired on the real corpus")


@requires_corpora
class PipelineCorpusTests(unittest.TestCase):
    """30 real pipelines. The activity count is the regression guard: a
    top-level-only walk saw 89 and missed everything inside a ForEach."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        root = pathlib.Path(cls._tmp.name)
        for path in _corpus("pipelines", "[0-9]*.json"):
            stem = pathlib.Path(path).stem
            item = root / f"pl_{stem}.DataPipeline"
            item.mkdir()
            (item / ".platform").write_text(json.dumps({
                "metadata": {"type": "DataPipeline", "displayName": f"pl_{stem}"},
                "config": {"logicalId": stem}}), encoding="utf-8")
            (item / "pipeline-content.json").write_text(
                pathlib.Path(path).read_text(encoding="utf-8", errors="replace"),
                encoding="utf-8")
        cls.result = pipeline_mod.scan(discover_items(root))

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_every_pipeline_is_scanned(self):
        self.assertGreaterEqual(self.result["summary"]["pipeline_count"], MIN_PIPELINES)

    def test_nested_activities_are_counted(self):
        self.assertGreaterEqual(self.result["summary"]["activity_count"], MIN_ACTIVITIES)

    def test_pipeline_to_pipeline_edges_are_captured(self):
        edges = sum(len(p.get("pipeline_refs") or [])
                    for p in self.result["items"]["pipelines"])
        self.assertGreaterEqual(edges, MIN_PIPELINE_EDGES)

    def test_half_the_real_pipelines_become_aidp_jobs(self):
        """15 of 30 are notebook-only and translate whole. The other 15 hold a
        Copy, Lookup or ExecutePipeline and are blocked by name rather than
        emitted as a job that silently does less than the original."""
        emitted = tasks = 0
        for pl in self.result["items"]["pipelines"]:
            paths = {n: f"/Workspace/{n}.py" for n in pl["notebook_refs"]}
            r = p2j.translate(pl, notebook_paths=paths, cluster_key="cl-1")
            if r.translated_sql:
                emitted += 1
                tasks += len(json.loads(r.translated_sql)["tasks"])
        self.assertGreaterEqual(emitted, MIN_JOBS_EMITTED)
        self.assertGreaterEqual(tasks, MIN_JOB_TASKS)

    def test_every_emitted_job_is_valid_json(self):
        for pl in self.result["items"]["pipelines"]:
            paths = {n: f"/Workspace/{n}.py" for n in pl["notebook_refs"]}
            r = p2j.translate(pl, notebook_paths=paths, cluster_key="cl-1")
            if r.translated_sql:
                with self.subTest(pipeline=pl["name"]):
                    payload = json.loads(r.translated_sql)
                    keys = [t["taskKey"] for t in payload["tasks"]]
                    self.assertEqual(len(keys), len(set(keys)), "duplicate taskKey")
                    for task in payload["tasks"]:
                        for dep in task.get("dependsOn", []):
                            self.assertIn(dep["taskKey"], keys,
                                          "dependsOn names a task not in the job")

    def test_no_notebook_parameter_is_dropped_silently(self):
        """12 of the 15 emitted jobs pass 76 notebook parameters. All 76 were
        dropped with no finding; each must now be carried or flagged."""
        jobs_with_parameters = 0
        for pl in self.result["items"]["pipelines"]:
            paths = {n: f"/Workspace/{n}.py" for n in pl["notebook_refs"]}
            r = p2j.translate(pl, notebook_paths=paths, cluster_key="cl-1")
            if not r.translated_sql:
                continue
            source = sum(len(a.get("parameters") or {}) for a in pl["activities"])
            tasks = json.loads(r.translated_sql)["tasks"]
            carried = sum(len(t.get("parameters", [])) for t in tasks)
            flagged = sum(1 for f in r.findings if f.rule == "PL23_PARAMETER_EXPRESSION")
            jobs_with_parameters += bool(source)
            with self.subTest(pipeline=pl["name"]):
                self.assertEqual(carried + flagged, source)
                for task in tasks:
                    if not task.get("dependsOn"):
                        # AIDP JOB_VALIDATE_0028, live.
                        self.assertEqual(task["runIf"], "ALL_SUCCESS")
        self.assertGreaterEqual(jobs_with_parameters, 12)

    def test_a_blocked_pipeline_names_the_activity_type(self):
        blocked = []
        for pl in self.result["items"]["pipelines"]:
            paths = {n: f"/Workspace/{n}.py" for n in pl["notebook_refs"]}
            r = p2j.translate(pl, notebook_paths=paths, cluster_key="cl-1")
            if not r.translated_sql:
                blocked.append((pl["name"], str(r.findings)))
        self.assertTrue(blocked, "expected some pipelines to be blocked")
        for name, findings in blocked:
            with self.subTest(pipeline=name):
                self.assertIn("PL9", findings)

    def test_no_pipeline_failed_to_read(self):
        bad = [p["name"] for p in self.result["items"]["pipelines"]
               if p.get("content_error")]
        self.assertEqual(bad, [], f"unreadable: {bad}")


@requires_corpora
class ParametersCellCorpusTests(unittest.TestCase):
    """What the parameters re-read does, and does not do, over real notebooks.

    The numbers are exact rather than ratchets, for the reason
    FixtureCoverageTests gives: a ratchet cannot tell "the rewrite got
    better" from "a notebook stopped having a parameters cell". They are
    re-taken on every run, and any change to either half of the parameter
    work moves one of them.

    The 42 matter most. Each is a name in a parameters cell that this cannot
    re-read, so a job passing that name is ignored by the notebook. That was
    silent -- the two halves out of step, at PASS -- and it is now a finding
    per name. Widening `_literal` to cover signed numbers and `AnnAssign`
    moved nothing here: neither shape occurs in the corpus, which is exactly
    why no corpus test caught the disagreement and why the unit tests in
    tests/test_publish.py carry it instead.
    """

    RE_READ = 111
    NOT_RE_READ = 42
    NOTEBOOKS_WITH_A_PARAMETERS_CELL = 14
    NOTEBOOKS_WITH_SOMETHING_UNREADABLE = 10

    @classmethod
    def setUpClass(cls):
        cls.rows = []
        for path in _corpus("notebooks", "[0-9]*.py"):
            source = pathlib.Path(path).read_text(encoding="utf-8", errors="replace")
            migrated = nb2spark.translate(source, namespace="ns").translated_sql
            findings = []
            document = json.loads(to_ipynb(migrated, findings=findings))
            cls.rows.append((os.path.basename(path), migrated, document, findings,
                             task_parameters_read(migrated)))

    def test_every_name_it_can_re_read_is_re_read(self):
        self.assertEqual(sum(len(read) for *_, read in self.rows), self.RE_READ)

    def test_every_name_it_cannot_is_reported_rather_than_dropped(self):
        unreadable = [f for *_, findings, _ in self.rows for f in findings
                      if f.rule == "NB35_PARAMETER_NOT_RE_READ"]
        self.assertEqual(len(unreadable), self.NOT_RE_READ)
        self.assertTrue(all(f.severity == "flag" for f in unreadable))

    def test_the_corpus_holds_the_parameters_cells_these_counts_come_from(self):
        with_cell = [name for name, migrated, *_ in self.rows
                     if "PARAMETERS CELL" in migrated]
        self.assertEqual(len(with_cell), self.NOTEBOOKS_WITH_A_PARAMETERS_CELL)
        named = {name for name, _, _, findings, _ in self.rows if findings}
        self.assertEqual(len(named), self.NOTEBOOKS_WITH_SOMETHING_UNREADABLE)

    def test_no_parameters_cell_in_the_corpus_fails_to_parse(self):
        bad = [(name, f.detail) for name, _, _, findings, _ in self.rows
               for f in findings if f.rule == "NB36_PARAMETER_CELL_UNPARSEABLE"]
        self.assertEqual(bad, [])

    def test_every_parameters_cell_it_rewrote_still_compiles(self):
        """The rewrite splices executable Python into a real notebook. If any
        of it did not compile, the cell would die where it used to run."""
        for name, _, document, _, read in self.rows:
            for cell in document["cells"]:
                tags = (cell.get("metadata") or {}).get("tags") or []
                if cell["cell_type"] != "code" or "parameters" not in tags:
                    continue
                with self.subTest(notebook=name):
                    compile("".join(cell["source"]), name, "exec")

    def test_the_conversion_adds_no_new_non_compiling_cell(self):
        """30 code cells in this corpus do not compile as Python and did not
        before -- Fabric magics, mostly. The rewrite must not add a 31st."""
        broken = [(name, i) for name, _, document, _, _ in self.rows
                  for i, cell in enumerate(document["cells"])
                  if cell["cell_type"] == "code" and not _compiles(cell)]
        self.assertEqual(len(broken), 30, broken)


def _compiles(cell) -> bool:
    try:
        compile("".join(cell["source"]), "<cell>", "exec")
    except SyntaxError:
        return False
    return True


if __name__ == "__main__":
    unittest.main()

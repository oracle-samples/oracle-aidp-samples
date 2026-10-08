import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import notebook as notebook_mod
from fabric_aidp.inventory.git_workspace import discover_items

CONTENT = (
    "# Fabric notebook source\n"
    "\n"
    "# METADATA ********************\n"
    "\n"
    "# META {\n"
    '# META   "kernel_info": { "name": "synapse_pyspark" },\n'
    '# META   "dependencies": { "lakehouse": {\n'
    '# META     "default_lakehouse": "2925655f-0293-4f32-8bc6-86ab989099a7",\n'
    '# META     "default_lakehouse_name": "Sales" } }\n'
    "# META }\n"
    "\n"
    "# CELL ********************\n"
    "\n"
    "%run Common_Utils\n"
    'notebookutils.notebook.run("Refresh_Dims", 300)\n'
)


# Both measured on the live probe estate: a trailing comma, once in a cell's
# METADATA block and once in the notebook-level one.
BAD_CELL_META = (
    "# Fabric notebook source\n\n"
    "# METADATA ********************\n\n"
    "# META {\n"
    '# META   "dependencies": {\n'
    '# META     "lakehouse": { "default_lakehouse_name": "SalesLake" }\n'
    "# META   }\n"
    "# META }\n\n"
    "# CELL ********************\n\n"
    "# MAGIC %%sql\n"
    "# MAGIC SELECT * FROM claims\n\n"
    "# METADATA ********************\n\n"
    "# META {\n"
    '# META   "language": "sparksql",\n'
    "# META }\n"
)
BAD_NOTEBOOK_META = (
    "# Fabric notebook source\n\n"
    "# METADATA ********************\n\n"
    "# META {\n"
    '# META   "a": 1,\n'
    "# META }\n\n"
    "# CELL ********************\n\n"
    "x = 1\n"
)


def _notebook(root: Path, display: str, body: str, ext: str = "py") -> None:
    d = root / f"{display}.Notebook"
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps({
        "config": {"logicalId": f"id-{display}"},
        "metadata": {"type": "Notebook", "displayName": display},
    }), encoding="utf-8")
    (d / f"notebook-content.{ext}").write_text(body, encoding="utf-8")


class ScanTests(unittest.TestCase):
    def _scan(self, build):
        with TemporaryDirectory() as t:
            root = Path(t)
            build(root)
            return notebook_mod.scan(discover_items(root))

    def test_record_carries_name_language_and_content(self):
        out = self._scan(lambda r: _notebook(r, "Ingest", CONTENT))
        rec = out["items"]["notebooks"][0]
        self.assertEqual(rec["name"], "Ingest")
        self.assertEqual(rec["language"], "python")
        self.assertIn("%run Common_Utils", rec["content"])
        self.assertEqual(rec["content_file"], "notebook-content.py")

    def test_default_lakehouse_name_is_preferred_over_the_guid(self):
        out = self._scan(lambda r: _notebook(r, "Ingest", CONTENT))
        self.assertEqual(out["items"]["notebooks"][0]["default_lakehouse"], "Sales")

    def test_edges_are_attached(self):
        out = self._scan(lambda r: _notebook(r, "Ingest", CONTENT))
        edges = out["items"]["notebooks"][0]["edges"]
        self.assertEqual(edges["run"], ["Common_Utils"])
        self.assertEqual(edges["notebook_run"], ["Refresh_Dims"])

    def test_sql_notebook_is_detected_from_the_file_extension(self):
        body = "# Fabric notebook source\n\n# CELL ********************\n\nSELECT 1\n"
        out = self._scan(lambda r: _notebook(r, "Query", body, ext="sql"))
        self.assertEqual(out["items"]["notebooks"][0]["language"], "sql")

    def test_unparseable_notebook_is_recorded_not_fatal(self):
        out = self._scan(lambda r: _notebook(r, "Broken", "not a fabric notebook\n"))
        rec = out["items"]["notebooks"][0]
        self.assertIn("parse_error", rec)
        self.assertEqual(rec["edges"], {"run": [], "notebook_run": [],
                                        "run_multiple": [], "unresolved": []})
        self.assertEqual(out["summary"]["parse_error_count"], 1)

    def test_missing_content_file_is_recorded(self):
        def build(root):
            d = root / "Empty.Notebook"
            d.mkdir()
            (d / ".platform").write_text(json.dumps({
                "config": {"logicalId": "x"},
                "metadata": {"type": "Notebook", "displayName": "Empty"},
            }), encoding="utf-8")
        out = self._scan(build)
        self.assertIn("parse_error", out["items"]["notebooks"][0])

    def test_malformed_meta_is_one_notebooks_parse_error_not_the_sources(self):
        # One trailing comma in one notebook's `# META` escaped the
        # per-notebook guard: the notebook source read FAILED and the other
        # notebooks were gone from the manifest, with inventory exiting 0.
        def build(root):
            _notebook(root, "A", CONTENT)
            _notebook(root, "B", CONTENT)
            _notebook(root, "BadCell", BAD_CELL_META)
            _notebook(root, "BadNb", BAD_NOTEBOOK_META)
        out = self._scan(build)
        self.assertNotIn("error", out["summary"])
        by_name = {r["name"]: r for r in out["items"]["notebooks"]}
        self.assertEqual(sorted(by_name), ["A", "B", "BadCell", "BadNb"])
        self.assertEqual(out["summary"]["parse_error_count"], 2)
        self.assertEqual(out["summary"]["language_counts"], {"python": 2})
        self.assertNotIn("parse_error", by_name["A"])
        self.assertIn("malformed # META JSON", by_name["BadCell"]["parse_error"])
        self.assertIn("cell 1", by_name["BadCell"]["parse_error"])
        self.assertIn("notebook-level", by_name["BadNb"]["parse_error"])
        # Content is kept, so migrate can still say what is wrong with it.
        self.assertIn("SELECT * FROM claims", by_name["BadCell"]["content"])

    def test_a_bad_cell_meta_is_caught_even_when_the_language_is_known(self):
        # kernel_info answers the language before any cell's META is read,
        # so only an eager check sees the bad block -- and the translator
        # reads it later, via language_for, when it unwraps the magic cell.
        body = BAD_CELL_META.replace(
            '# META   "dependencies": {',
            '# META   "kernel_info": { "name": "synapse_pyspark" },\n'
            '# META   "dependencies": {')
        out = self._scan(lambda r: _notebook(r, "BadCell", body))
        self.assertIn("cell 1", out["items"]["notebooks"][0]["parse_error"])

    def test_summary_counts_by_language(self):
        def build(root):
            _notebook(root, "A", CONTENT)
            _notebook(root, "B", CONTENT)
            _notebook(root, "Q",
                      "# Fabric notebook source\n\n# CELL ********************\n\nSELECT 1\n",
                      ext="sql")
        out = self._scan(build)
        self.assertEqual(out["summary"]["notebook_count"], 3)
        self.assertEqual(out["summary"]["language_counts"], {"python": 2, "sql": 1})

    def test_non_notebook_items_are_ignored(self):
        def build(root):
            _notebook(root, "A", CONTENT)
            d = root / "Sales.Lakehouse"
            d.mkdir()
            (d / ".platform").write_text(json.dumps({
                "config": {"logicalId": "lh"},
                "metadata": {"type": "Lakehouse", "displayName": "Sales"},
            }), encoding="utf-8")
        out = self._scan(build)
        self.assertEqual(out["summary"]["notebook_count"], 1)

    def test_no_notebooks_yields_an_empty_but_valid_shape(self):
        out = notebook_mod.scan([])
        self.assertEqual(out, {
            "summary": {"notebook_count": 0, "language_counts": {}, "parse_error_count": 0},
            "items": {"notebooks": []},
        })


IPYNB = json.dumps({
    "nbformat": 4, "nbformat_minor": 5,
    "metadata": {"language_info": {"name": "python"},
                 "dependencies": {"lakehouse": {
                     "default_lakehouse": "2925655f-0293-4f32-8bc6-86ab989099a7",
                     "default_lakehouse_name": "Sales"}}},
    "cells": [{"cell_type": "code", "id": "a1", "metadata": {},
               "source": ["%run Common_Utils\n",
                          'notebookutils.notebook.run("Refresh_Dims", 300)\n']}],
}, indent=2)


class IpynbScanTests(unittest.TestCase):
    """Most real exports are .ipynb -- 39 of the 49 notebooks in
    microsoft/fabric-toolbox -- and the scan was written against the `.py`
    file text. Two defects follow, one fatal and one silent, and neither
    shows on the bundled demo because every notebook in it is `.py`."""

    def _scan(self, body=IPYNB):
        with TemporaryDirectory() as t:
            root = Path(t)
            _notebook(root, "Ingest", body, ext="ipynb")
            return notebook_mod.scan(discover_items(root))

    def test_an_ipynb_notebook_is_scanned_at_all(self):
        """`inventory/notebook.py` reads `nb.default_lakehouse_id` inside a
        guard that catches only parse errors, and `IpynbNotebook` had no
        such property. The AttributeError escaped to the per-source catch in
        manifest.py, the whole notebook source reported FAILED, and plan,
        migrate and verify saw no notebooks at all -- each exiting 0. One
        .ipynb in an estate was enough to lose every notebook in it."""
        out = self._scan()
        self.assertEqual(out["summary"]["notebook_count"], 1)
        self.assertEqual(out["summary"]["parse_error_count"], 0)

    def test_the_lakehouse_binding_is_read(self):
        record = self._scan()["items"]["notebooks"][0]
        self.assertEqual(record["default_lakehouse"], "Sales")
        self.assertEqual(record["default_lakehouse_id"],
                         "2925655f-0293-4f32-8bc6-86ab989099a7")

    def test_a_synapse_era_binding_is_read_too(self):
        """`FabricNotebook` reads the `synapse` spelling as well as
        `dependencies`; this class read only `dependencies`."""
        body = json.dumps({
            "nbformat": 4, "nbformat_minor": 5,
            "metadata": {"synapse": {"lakehouse": {
                "default_lakehouse_name": "Legacy"}}},
            "cells": [{"cell_type": "code", "metadata": {}, "source": ["x = 1"]}]})
        self.assertEqual(
            self._scan(body)["items"]["notebooks"][0]["default_lakehouse"],
            "Legacy")

    def test_the_same_edges_as_the_py_spelling_of_the_same_notebook(self):
        """The edge scan read the file text. For an .ipynb that text is
        JSON: `%run Common_Utils` no longer starts its line, so the edge
        vanished, and the `notebook.run` argument arrived JSON-escaped, so
        it was not a string literal this could read and the edge landed in
        `unresolved` as a fragment of JSON."""
        with TemporaryDirectory() as t:
            root = Path(t)
            _notebook(root, "Py", CONTENT)
            _notebook(root, "Ipynb", IPYNB, ext="ipynb")
            records = {r["name"]: r["edges"]
                       for r in notebook_mod.scan(
                           discover_items(root))["items"]["notebooks"]}
        self.assertEqual(records["Ipynb"], records["Py"])
        # `run_multiple` arrived with the runMultiple fix on main; an
        # .ipynb has to produce the same keys as the .py, empty included.
        self.assertEqual(records["Ipynb"],
                         {"run": ["Common_Utils"],
                          "notebook_run": ["Refresh_Dims"],
                          "run_multiple": [], "unresolved": []})

    def test_no_json_fragment_lands_in_unresolved(self):
        record = self._scan()["items"]["notebooks"][0]
        self.assertEqual(record["edges"]["unresolved"], [])

class TableReadWriteRecordTests(unittest.TestCase):
    """The writes were found by `build_catalog` and never written down
    per notebook, and the reads were never found at all -- so the plan
    had no way to order a reader after its producer."""

    BODY = ("# Fabric notebook source\n\n"
            "# CELL ********************\n\n"
            'daily = spark.table("claims_daily")\n'
            'daily.write.saveAsTable("claims_agg")\n')

    def _record(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _notebook(root, "Build", self.BODY)
            return notebook_mod.scan(discover_items(root))["items"]["notebooks"][0]

    def test_the_writes_are_recorded(self):
        self.assertEqual(self._record()["writes"], ["claims_agg"])

    def test_the_reads_are_recorded(self):
        self.assertEqual(self._record()["reads"], ["claims_daily"])

    def test_a_notebook_that_will_not_parse_records_empty_lists(self):
        """Same contract as `edges`: a record that exists and says
        nothing, never a missing key."""
        with TemporaryDirectory() as t:
            root = Path(t)
            _notebook(root, "Bad", BAD_CELL_META)
            record = notebook_mod.scan(
                discover_items(root))["items"]["notebooks"][0]
            self.assertEqual((record["reads"], record["writes"]), ([], []))


class NotebookGuardStructureTests(unittest.TestCase):
    """Every read of the parsed notebook in `scan` happens inside its guard.

    A lazily-decoding property (`notebook_meta`, `meta_for`, and everything
    built on them: `default_lakehouse`, `default_lakehouse_id`,
    `lakehouse_binding`, `language_for`) read after the per-notebook `try`
    lets one malformed `# META` escape the scan and take every notebook with
    it. `check_meta` makes that safe today, so no estate can show the next
    such read going in the wrong place -- the #17/#19 merge nearly did. So
    this is checked on the source: outside the `try` that parses it, `nb`
    may only be handed to `_cell_source`, which reads the code blocks'
    text and decodes nothing (checked below on a notebook whose every META
    block is malformed).

    Bite-proofed by moving `nb.default_lakehouse_id` back to the record
    assignment after the guard, where the merge first put it: this fails
    naming line and attribute.
    """

    ALLOWED_OUTSIDE = {"_cell_source"}

    @classmethod
    def setUpClass(cls):
        import ast
        import inspect
        cls.ast = ast
        cls.tree = ast.parse(inspect.getsource(notebook_mod))
        cls.scan = next(node for node in ast.walk(cls.tree)
                        if isinstance(node, ast.FunctionDef) and node.name == "scan")

    def _guard(self):
        ast = self.ast
        for node in ast.walk(self.scan):
            if isinstance(node, ast.Try) and any(
                    isinstance(sub, ast.Call) and getattr(sub.func, "id", None) == "parse_any"
                    for stmt in node.body for sub in ast.walk(stmt)):
                return node
        self.fail("scan() has no try block around parse_any")

    def test_the_notebook_is_read_only_inside_its_guard(self):
        ast = self.ast
        guard = self._guard()
        inside = {id(sub) for stmt in guard.body for sub in ast.walk(stmt)}
        parent = {}
        for node in ast.walk(self.scan):
            for child in ast.iter_child_nodes(node):
                parent[id(child)] = node
        escaped = []
        for node in ast.walk(self.scan):
            if not (isinstance(node, ast.Name) and node.id == "nb"
                    and isinstance(node.ctx, ast.Load)) or id(node) in inside:
                continue
            up = parent[id(node)]
            if (isinstance(up, ast.Call) and node in up.args
                    and getattr(up.func, "id", None) in self.ALLOWED_OUTSIDE):
                continue
            what = ("nb.%s" % up.attr if isinstance(up, ast.Attribute)
                    else ast.dump(up)[:60])
            escaped.append("line %d: %s" % (node.lineno, what))
        self.assertEqual(escaped, [],
                         "read after the per-notebook guard in scan(); move it "
                         "inside the try that calls parse_any")

    def test_what_is_allowed_outside_decodes_no_metadata(self):
        from fabric_aidp.translate.notebook_format import (
            NotebookParseError, parse_notebook,
        )
        bad = ("# Fabric notebook source\n\n"
               "# METADATA ********************\n\n# META {bad,}\n\n"
               "# CELL ********************\n\nx = 1\n\n"
               "# METADATA ********************\n\n# META {bad,}\n")
        nb = parse_notebook(bad)
        # Sanity: the notebook really would raise on a lazy decode.
        with self.assertRaises(NotebookParseError):
            nb.notebook_meta
        self.assertEqual(notebook_mod._cell_source(nb).strip(), "x = 1")


if __name__ == "__main__":
    unittest.main()

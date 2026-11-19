"""The HTML report.

It exists because both sibling migrators ship one and this tool's whole
premise was to match the AWS migrator's shape. It was simply never built.

The tests below are mostly about what the sibling project's first outside
tester complained of: a report that named items but never the files they
became. Everything needed was already in report.json and just never rendered.
"""
import html
import json
import re
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.migrate.html_report import render

REPORT = {
    "plan_id": "20260925T000000Z-abc",
    "migrated_at": "2026-09-25T00:00:00Z",
    "counts": {"ok": 1, "needs_manual_review": 1, "blocked": 1, "planned": 1},
    "results": [
        {"asset_id": "notebook.Ingest", "kind": "notebook", "status": "ok",
         "changes": 2, "flags": 0, "output_path": "notebooks/Ingest.py",
         "findings": [{"rule": "NB01_ONELAKE_PATH", "detail": "remapped a path",
                       "severity": "rewrite"}],
         "source_sql": "spark.read.parquet('abfss://x')",
         "translated_sql": "spark.read.parquet('oci://x')"},
        {"asset_id": "warehouse.DW.table.dbo.claim", "kind": "warehouse_table",
         "status": "needs_manual_review", "changes": 0, "flags": 1,
         "output_path": "warehouse/dbo.claim.spark.sql",
         "findings": [{"rule": "SQ60_TYPE", "detail": "MONEY rounding differs",
                       "severity": "flag"}],
         "source_sql": "CREATE TABLE dbo.claim (amt MONEY)",
         "translated_sql": "CREATE TABLE dbo.claim (amt DECIMAL(19,4))"},
        {"asset_id": "pipeline.Daily", "kind": "pipeline_job", "status": "blocked",
         "findings": [{"rule": "PL90_UNSUPPORTED_ACTIVITY",
                       "detail": "activity 'Copy in' is a Copy, which has no "
                                 "AIDP job equivalent", "severity": "flag"}]},
        {"asset_id": "semanticmodel.Sales", "kind": "aidp_semantic_model",
         "status": "planned", "note": "no translator for this asset type"},
    ],
}


def _render():
    tmp = TemporaryDirectory()
    path = render(REPORT, Path(tmp.name) / "report.html")
    return tmp, path.read_text(encoding="utf-8")


def _asset_card(html_text: str, asset_id: str) -> str:
    """The one rendered asset block for `asset_id`, or an assertion failure.

    An assertion about a row has to be made against that row. Asserting a bare
    word against the whole document silently asserts nothing, because the
    document also carries a stylesheet and page chrome that the renderer emits
    unconditionally: measured 2026-09-29, a report rendered with **zero**
    results still contains the word "blocked" twice --
    `.asset.blocked .head{...}` and `.badge.blocked{...}` -- so
    `assertIn("blocked", html)` could not fail, whatever the renderer did with
    blocked assets.

    Each asset block starts with the same opening tag, so splitting on it
    bounds every card without parsing the document.
    """
    marker = '<div class="asset '
    wanted = '<span class="id">%s</span>' % html.escape(asset_id)
    for part in html_text.split(marker)[1:]:
        block = marker + part
        if wanted in block:
            return block
    raise AssertionError(
        "the report has no rendered asset block for %r -- it was dropped, "
        "which is the failure this file exists to catch" % asset_id)


def _why_block(card: str) -> str:
    """The refusal explanation inside one asset card, or "".

    Scoped on purpose. The detail text of a blocked row is rendered twice --
    once by the generic findings list and once by the refusal block -- so
    matching it against the whole card passes with the refusal block deleted
    (measured 2026-09-29: stubbing `_why_blocked` to return "" left
    `assertIn("has no", html)` passing).
    """
    found = re.search(r'<div class="why">(.*?)</div>', card, re.S)
    return found.group(1) if found else ""


class RenderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp, cls.html = _render()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_it_is_a_standalone_document_with_no_external_assets(self):
        self.assertTrue(self.html.startswith("<!doctype html>"))
        self.assertIn("<style>", self.html)
        for remote in ("http://", "https://", "src="):
            with self.subTest(remote=remote):
                self.assertNotIn(remote, self.html)

    def test_every_asset_appears(self):
        for asset in ("notebook.Ingest", "warehouse.DW.table.dbo.claim",
                      "pipeline.Daily", "semanticmodel.Sales"):
            with self.subTest(asset=asset):
                self.assertIn(asset, self.html)

    def test_a_blocked_asset_is_not_quietly_omitted(self):
        """Dropping refusals would make the migration read cleaner than it is.

        `_asset_card` fails if the row is absent; the badge assertion fails if
        it is present but not shown as refused.
        """
        card = _asset_card(self.html, "pipeline.Daily")
        self.assertIn('<span class="badge blocked">blocked</span>', card)

    def test_a_blocked_asset_says_why(self):
        why = _why_block(_asset_card(self.html, "pipeline.Daily"))
        self.assertIn("Refused rather than part-translated", why)
        self.assertIn("has no AIDP job equivalent", why)

    def test_findings_carry_their_rule_id(self):
        self.assertIn("NB01_ONELAKE_PATH", self.html)
        self.assertIn("SQ60_TYPE", self.html)


class OutputLinkTests(unittest.TestCase):
    """The sibling project's first outside tester filed exactly this: the
    report named items but never the files they became."""

    @classmethod
    def setUpClass(cls):
        cls._tmp, cls.html = _render()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_each_written_asset_links_to_its_file(self):
        links = re.findall(r'wrote <a href="([^"]+)"', self.html)
        self.assertEqual(sorted(links),
                         ["notebooks/Ingest.py", "warehouse/dbo.claim.spark.sql"])

    def test_links_are_relative_so_they_work_beside_the_report(self):
        for link in re.findall(r'wrote <a href="([^"]+)"', self.html):
            with self.subTest(link=link):
                self.assertFalse(link.startswith(("/", "http")))

    def test_a_path_with_a_hash_or_space_is_url_escaped(self):
        """Two real dataflow queries are named `#"New Users"`, so the artifact
        path contains a `#`. HTML-escaping leaves it alone and a browser reads
        it as a fragment, truncating the link -- silently, since the text
        beside it still looks right."""
        report = dict(REPORT, results=[dict(
            REPORT["results"][0],
            output_path='dataflows/x.#_New Users_.py')])
        with TemporaryDirectory() as tmp:
            html_text = render(report, Path(tmp) / "r.html").read_text(encoding="utf-8")
        href = re.search(r'wrote <a href="([^"]+)"', html_text).group(1)
        self.assertNotIn("#", href)
        self.assertNotIn(" ", href)
        self.assertIn("%23", href)
        self.assertIn("%20", href)

    def test_the_visible_link_text_stays_readable(self):
        report = dict(REPORT, results=[dict(
            REPORT["results"][0], output_path='dataflows/x.#_New Users_.py')])
        with TemporaryDirectory() as tmp:
            html_text = render(report, Path(tmp) / "r.html").read_text(encoding="utf-8")
        self.assertIn("New Users", html_text)

    def test_a_blocked_asset_says_no_file_was_written(self):
        self.assertIn("no file written", self.html)


class SafetyTests(unittest.TestCase):
    def test_content_is_html_escaped(self):
        report = dict(REPORT, results=[dict(
            REPORT["results"][0],
            asset_id="<script>alert(1)</script>",
            translated_sql="a < b && c > d")])
        with TemporaryDirectory() as tmp:
            html = render(report, Path(tmp) / "r.html").read_text(encoding="utf-8")
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_it_is_written_as_utf8(self):
        with TemporaryDirectory() as tmp:
            path = render(REPORT, Path(tmp) / "r.html")
            path.read_text(encoding="utf-8")   # raises if it is not

    def test_an_empty_report_still_renders(self):
        with TemporaryDirectory() as tmp:
            html = render({"results": [], "counts": {}},
                          Path(tmp) / "r.html").read_text(encoding="utf-8")
        self.assertIn("No assets in this run", html)

    def test_a_report_missing_optional_keys_does_not_crash(self):
        with TemporaryDirectory() as tmp:
            render({"results": [{"asset_id": "x", "status": "ok"}]},
                   Path(tmp) / "r.html")


class RunnerIntegrationTests(unittest.TestCase):
    def test_migrate_writes_report_html_beside_the_json(self):
        from fabric_aidp.fixtures import demo_workspace_path
        from fabric_aidp.inventory import build_manifest
        from fabric_aidp.migrate import migrate
        from fabric_aidp.plan.planner import build_plan
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            plan = build_plan(build_manifest(demo_workspace_path()))
            migrate(plan, out_dir=out)
            self.assertTrue((out / "report.html").is_file())
            self.assertTrue((out / "report.json").is_file())

    def test_a_renderer_failure_does_not_lose_the_migration(self):
        """report.json is the record; the HTML is a convenience."""
        from unittest import mock
        from fabric_aidp.fixtures import demo_workspace_path
        from fabric_aidp.inventory import build_manifest
        from fabric_aidp.migrate import migrate
        from fabric_aidp.plan.planner import build_plan
        with TemporaryDirectory() as tmp:
            out = Path(tmp)
            plan = build_plan(build_manifest(demo_workspace_path()))
            with mock.patch("fabric_aidp.migrate.html_report.render",
                            side_effect=RuntimeError("boom")):
                report = migrate(plan, out_dir=out)
            self.assertTrue((out / "report.json").is_file())
            self.assertTrue(report["results"])


ERROR_REPORT = {
    "plan_id": "p", "migrated_at": "2026-09-25T00:00:00Z",
    "counts": {"error": 1},
    "results": [{"asset_id": "notebook.Boom", "kind": "fabric_notebook",
                 "status": "error",
                 "error": "OCI namespace must be lowercase letters, digits, "
                          "underscores or hyphens"}],
}


class ErroredAssetTests(unittest.TestCase):
    """An errored asset rendered as an empty card.

    `error` is the only field an error row carries -- the runner writes no
    findings for one -- and the template referenced neither it nor anything
    derived from it. The reader got the asset id, a red badge, an empty
    findings list, and "no artifact for this asset type", which is not what
    happened: this asset type has artifacts, and this one failed to produce
    its own.
    """

    @classmethod
    def setUpClass(cls):
        cls._tmp = TemporaryDirectory()
        cls.html = render(ERROR_REPORT,
                          Path(cls._tmp.name) / "report.html").read_text(
                              encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def test_the_error_message_is_rendered(self):
        self.assertIn("OCI namespace must be lowercase", self.html)

    def test_it_does_not_claim_the_asset_type_has_no_artifacts(self):
        self.assertNotIn("no artifact for this asset type", self.html)


class StackedPanesTests(unittest.TestCase):
    """Source above translation, full width, long panes folded.

    Side by side gave each pane ~500px of an 1100px page. A Dataflow's
    translation is 193-220 lines against a 15-line M query, so nothing lines
    up across the columns and the width was all cost: the source wrapped and
    the translation was clipped mid-line.
    """

    def _render(self, results):
        with TemporaryDirectory() as tmp:
            out = render({"plan_id": "p", "migrated_at": "t", "counts": {},
                          "results": results}, Path(tmp) / "r.html")
            return out.read_text(encoding="utf-8")

    def _row(self, source, translated):
        return {"asset_id": "dataflow.x", "kind": "dataflow_query",
                "status": "ok", "findings": [], "source_sql": source,
                "translated_sql": translated}

    def test_the_panes_are_one_column(self):
        page = self._render([self._row("a", "b")])
        self.assertIn(".cols{display:grid;grid-template-columns:1fr;", page)
        self.assertNotIn("grid-template-columns:1fr 1fr", page)

    def test_the_page_is_wide_enough_for_code(self):
        self.assertIn(".wrap{max-width:1400px", self._render([self._row("a", "b")]))

    def test_source_comes_before_translation(self):
        page = self._render([self._row("SOURCE_TEXT", "TRANSLATED_TEXT")])
        self.assertLess(page.index("SOURCE_TEXT"), page.index("TRANSLATED_TEXT"))

    def test_a_short_pane_is_not_folded(self):
        page = self._render([self._row("one\ntwo", "\n".join(["x"] * 30))])
        self.assertNotIn("<details", page)

    def test_a_long_pane_shows_a_preview_and_folds_the_rest(self):
        long = "\n".join(f"line {i}" for i in range(1, 201))
        page = self._render([self._row("m", long)])
        preview = re.search(r'<pre class="after preview">(.*?)</pre>', page, re.S)
        folded = re.search(r'<details class="more">.*?<pre class="after">(.*?)</pre>',
                           page, re.S)
        self.assertIsNotNone(preview)
        self.assertIsNotNone(folded)
        self.assertEqual(preview.group(1).splitlines(),
                         [f"line {i}" for i in range(1, 26)])
        self.assertEqual(folded.group(1).splitlines(),
                         [f"line {i}" for i in range(26, 201)])
        self.assertIn("Show all 200 lines (175 more)", page)

    def test_the_preview_does_not_share_a_class_with_the_title_bar(self):
        """`.asset .head` styles the asset's title bar. The preview was
        `class="after head"` for one render and inherited the bar's cream
        background, so the code came out grey on cream. Caught by looking at
        the page, which is why it is pinned here."""
        page = self._render([self._row("m", "\n".join(["z"] * 100))])
        for tag in re.findall(r"<pre[^>]*>", page):
            with self.subTest(tag=tag):
                self.assertNotRegex(tag, r'class="[^"]*\bhead\b')

    def test_every_line_is_rendered_exactly_once(self):
        """Preview plus remainder, not preview plus a second full copy."""
        long = "\n".join(f"uniq{i}x" for i in range(1, 101))
        page = self._render([self._row("m", long)])
        for i in (1, 25, 26, 100):
            with self.subTest(line=i):
                self.assertEqual(page.count(f"uniq{i}x"), 1)

    def test_the_fold_needs_no_script(self):
        """The report is a standalone file: saved, mailed, opened offline."""
        long = "\n".join(["y"] * 100)
        self.assertNotIn("<script", self._render([self._row("m", long)]))

    def test_folded_text_is_still_escaped(self):
        long = "\n".join(["ok"] * 40 + ["<b>not markup</b>"])
        page = self._render([self._row("m", long)])
        self.assertIn("&lt;b&gt;not markup&lt;/b&gt;", page)
        self.assertNotIn("<b>not markup</b>", page)


if __name__ == "__main__":
    unittest.main()

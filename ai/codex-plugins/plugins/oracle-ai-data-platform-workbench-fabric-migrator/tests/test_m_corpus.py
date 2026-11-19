import os
import tempfile
import unittest
from pathlib import Path

from fabric_aidp.translate import m_nav, m_parser
from fabric_aidp.translate import m_to_pyspark as m

CORPUS = Path(os.environ.get(
    "FABRIC_M_CORPUS",
    Path.home() / "workspace-dfl/fabric-aidp-migrator/fixtures/corpus")).expanduser()
REPORT = Path(__file__).resolve().parents[1] / "docs" / "m-coverage.md"

# Ratchet. Raise these when coverage improves; never lower them to make a
# change pass -- that is the whole point of having them.
MIN_FILES_PARSED = 39
# Was 22. Two of those (011 and 034 `orders`) were emitted only because a
# nested Table.AddColumn was silently dropped; the generated scripts failed
# with UNRESOLVED_COLUMN. They are blocked now, which is the correct outcome.
MIN_EMITTED = 20
MIN_IN_SCOPE_PIPELINES = 37
# The spec (docs/superpowers/specs/2026-09-28-m-value-semantics-design.md,
# section 9) makes "the clean count stays at 2" an explicit acceptance
# criterion, reasoned out by hand: neither clean query uses `type text`, so
# neither picks up an M28_TEXT_RENDERING flag. That reasoning has no test
# behind it. M28 is exactly the kind of flag that can zero this out -- it
# fires on rendering a column as text, which is common -- so a future change
# that applies it one query wider than today would drive `clean` to 0 with
# every other ratchet in this file still green. Pinning it here closes that
# gap.
# Lowered from 2 to 1 on 2026-09-29, deliberately, and this is the one kind
# of reason that justifies lowering a ratchet: the count fell because a
# finding became MORE severe on evidence, not because output got worse.
# M34_NAME_NOT_ADDRESSABLE was `info` while only Spark's Hive-compatible
# catalog had been measured and the AIDP catalog had not. The AIDP cluster
# then answered -- `saveAsTable` of a back-quoted hyphenated name is
# rejected on fabricTest (Spark 3.5.0) -- so it is a flag, and the query
# carrying such a name is genuinely not clean. Holding the floor at 2 would
# have meant keeping a finding at the wrong severity to protect a number.
# A floor may fall for that reason and for no other; if `clean` drops
# without a severity moving up on a measurement, that is a regression.
MIN_CLEAN = 1
# An emitted query that writes nowhere is a Dataflow that computes a result
# and discards it, and it graded the same as one that lands its table. Before
# `BindToDefaultDestination` was honoured, 5 of the 20 emitted queries carried
# a `saveAsTable`; 15 ended on `result = <frame>`, and 9 of those 15 were in a
# file that declares a default destination. Honouring it takes the writers to
# 14. This is the ratchet that would have caught the defect.
MIN_WRITING = 14


# Marks where the generated table ends and hand-written prose ("Known
# divergences", "Runtime floor") begins. A literal, fixed string -- not
# derived from the generated prose's own wording. It used to be: reword one
# word of the generated closing sentence, rerun the corpus test, and the
# entire hand-written section vanished with no error and a passing test,
# because the marker moved (or disappeared) along with the wording it was
# cut from. This sentinel is never itself regenerated or reworded.
COVERAGE_SENTINEL = (
    "<!-- everything below this line is hand-written and preserved across "
    "regeneration -->\n")


def _coverage_table(files_parsed, counts):
    """The generated part of docs/m-coverage.md. Always safe to overwrite --
    nothing hand-written lives here."""
    in_scope = counts["clean"] + counts["flagged"] + counts["blocked"]
    emitted = counts["clean"] + counts["flagged"]
    return (
        "# M → PySpark coverage\n\n"
        f"Measured over {files_parsed} real Dataflow Gen2 exports "
        "(referenced, not vendored — see the spec, §10.1).\n\n"
        "| Bucket | Count |\n|---|---|\n"
        f"| clean (emitted, no flags) | {counts['clean']} |\n"
        f"| flagged (emitted, needs a human) | {counts['flagged']} |\n"
        f"| blocked (in scope, no mapping) | {counts['blocked']} |\n"
        f"| out of scope (connector named) | {counts['out_of_scope']} |\n"
        f"| destination helpers (suppressed) | {counts['helper']} |\n"
        f"| parameter queries (constants) | {counts['parameter']} |\n"
        f"| members not read (no `let`, not a value) "
        f"| {counts['unread_member']} |\n\n"
        f"**In-scope pipelines: {in_scope}. Emitted: {emitted} "
        f"({100 * emitted // max(in_scope, 1)}%).**\n\n"
        "Emitted files are `ast.parse`d before they are written. None has been\n"
        "executed on a cluster; this table measures translation, not correctness.\n"
        "\n" + COVERAGE_SENTINEL)


def write_coverage_report(report_path, files_parsed, counts):
    """Regenerate `report_path`'s numeric table from a fresh corpus run,
    keeping whatever hand-written prose already follows COVERAGE_SENTINEL.

    A file that already exists, has content, but carries no sentinel is
    refused rather than overwritten: that shape means either the sentinel
    was stripped by a hand-edit or the file predates it, and in both cases
    silently resetting the file to just the table is the exact class of
    silent data loss this function exists to prevent.
    """
    hand_written = ""
    if report_path.is_file():
        existing = report_path.read_text(encoding="utf-8")
        if existing.strip():
            cut = existing.find(COVERAGE_SENTINEL)
            if cut == -1:
                raise AssertionError(
                    "%s exists and is non-empty but has no preservation "
                    "sentinel (%r); refusing to regenerate it in case it "
                    "holds hand-written prose this would silently erase -- "
                    "restore the sentinel (see git history) before rerunning"
                    % (report_path, COVERAGE_SENTINEL.strip()))
            hand_written = existing[cut + len(COVERAGE_SENTINEL):]
    report_path.parent.mkdir(parents=True, exist_ok=True)
    # `Path.write_text(newline=...)` is 3.10+, and this project supports 3.9 --
    # on 3.9 the keyword raised TypeError and three corpus tests errored, so the
    # declared floor was not actually green. `open` takes newline everywhere.
    with report_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(_coverage_table(files_parsed, counts) + hand_written)


class CoverageReportWritingTests(unittest.TestCase):
    """write_coverage_report's own contract, against temp files -- no corpus
    or parser needed, so this runs even where CorpusCoverageTests is skipped."""

    COUNTS = {"clean": 1, "flagged": 0, "blocked": 0,
              "out_of_scope": 0, "helper": 0, "parameter": 0,
              "unread_member": 0}

    def test_hand_written_prose_survives_a_regeneration(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "m-coverage.md"
            write_coverage_report(report, 39, self.COUNTS)
            report.write_text(
                report.read_text(encoding="utf-8")
                + "\n## Known divergences\n\nHAND-WRITTEN, MUST SURVIVE.\n",
                encoding="utf-8")
            write_coverage_report(report, 40, self.COUNTS)  # counts changed
            self.assertIn("HAND-WRITTEN, MUST SURVIVE.",
                          report.read_text(encoding="utf-8"))
            self.assertIn("40 real Dataflow", report.read_text(encoding="utf-8"))

    def test_a_report_with_no_sentinel_fails_loudly(self):
        # A file that exists, has content, and predates the sentinel (or had
        # it stripped) must refuse rather than silently reset to just the
        # table -- that reset is the bug this function exists to prevent.
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "m-coverage.md"
            original = "# some pre-existing report with no sentinel\n"
            report.write_text(original, encoding="utf-8")
            with self.assertRaisesRegex(AssertionError, "no preservation sentinel"):
                write_coverage_report(report, 39, self.COUNTS)
            # Refusing means refusing -- the file is untouched, not half-written.
            self.assertEqual(report.read_text(encoding="utf-8"), original)

    def test_a_fresh_report_with_no_prior_content_works(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = Path(tmp) / "nested" / "m-coverage.md"
            write_coverage_report(report, 39, self.COUNTS)
            self.assertTrue(report.is_file())
            self.assertIn(COVERAGE_SENTINEL, report.read_text(encoding="utf-8"))


def _catalog(queries):
    """Resolve every lakehouse GUID the corpus mentions, so `clean` is reachable.

    Pooled across all 39 files rather than built per file, which raises the
    question of whether a destination GUID in one file is being resolved by
    a read in another. MEASURED, and it is not:

      21 distinct lakehouse GUIDs; only 2 appear in more than one file, and
      each of those is resolvable from a read inside every file that uses
      it.
      2 files (015.pq, 023.pq) do have a destination GUID their own reads
      do not cover -- and neither GUID appears anywhere in the pool either,
      so pooling gives them nothing.
      Running the whole corpus with a per-file catalog gives byte-identical
      buckets: clean 2, flagged 18, blocked 17, out of scope 24, helpers
      38, parameters 4, members not read 8.

    So the pooling is inert today. It is left pooled rather than changed on
    no evidence; if a file is ever added whose destination GUID is resolved
    only by another file's read, the counts above move and this comment is
    where to look.
    """
    catalog = {}
    for query in queries:
        for index in m_nav.lakehouse_read_indexes(query.get("steps") or []):
            read = m_nav.fold_lakehouse_chain(query["steps"], index)
            if read.lakehouse_id:
                catalog.setdefault(read.lakehouse_id, "lh_" + read.lakehouse_id[:8])
    return catalog


@unittest.skipUnless(CORPUS.is_dir(), f"corpus not present at {CORPUS}")
@unittest.skipUnless(m_parser.parser_available(), "Node + mparse not installed")
class CorpusCoverageTests(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.parsed, cls.failed = [], []
        for path in sorted(CORPUS.glob("*.pq")):
            try:
                cls.parsed.append((path.name, m_parser.parse_file(path)))
            except m_parser.MParseError as exc:
                cls.failed.append((path.name, str(exc)))

        every = [q for _, doc in cls.parsed for q in doc["queries"]]
        catalog = _catalog(every)
        cls.counts = {"clean": 0, "flagged": 0, "blocked": 0,
                      "out_of_scope": 0, "helper": 0, "parameter": 0,
                      "unread_member": 0}
        cls.uncompilable = []
        cls.writing = []
        cls.emitted_without_a_write = []
        cls.mapped = []
        for name, doc in cls.parsed:
            by_name = {q["name"]: q for q in doc["queries"]}
            section = doc.get("section_attrs") or ""
            for query in doc["queries"]:
                kind = m.classify(query, section)
                if kind != "pipeline":
                    cls.counts[kind] += 1
                    continue
                result = m.translate_query(query, queries_by_name=by_name,
                                            lakehouses=catalog, source=name,
                                            section_attrs=section)
                # The ast.parse gate is a refusal now, not an exception, so
                # it arrives as a finding like every other refusal. Collected
                # here so the named test below fails on it rather than the
                # blocked count quietly absorbing it.
                cls.uncompilable.extend(
                    (name, query["name"], f.detail[:200]) for f in result.findings
                    if f.rule == "M31_UNCOMPILABLE_OUTPUT")
                if not result.translated_sql:
                    bucket = ("out_of_scope"
                              if any(f.rule == "M91_UNSUPPORTED_CONNECTOR"
                                     for f in result.findings) else "blocked")
                    cls.counts[bucket] += 1
                elif result.flags:
                    cls.counts["flagged"] += 1
                else:
                    cls.counts["clean"] += 1
                if result.translated_sql:
                    target = (cls.writing if "saveAsTable" in result.translated_sql
                              else cls.emitted_without_a_write)
                    target.append((name, query["name"]))
                    mapping = m_nav.column_mapping(query.get("attrs"))
                    if mapping:
                        cls.mapped.append(
                            (name, query["name"], len(mapping),
                             ".select(F.col(" in result.translated_sql))

    def test_every_corpus_file_parses(self):
        self.assertEqual(self.failed, [])
        self.assertGreaterEqual(len(self.parsed), MIN_FILES_PARSED)

    def test_emitted_coverage_does_not_regress(self):
        emitted = self.counts["clean"] + self.counts["flagged"]
        self.assertGreaterEqual(
            emitted, MIN_EMITTED,
            f"emitted dropped to {emitted}; counts={self.counts}")

    def test_in_scope_pipeline_count_does_not_regress(self):
        in_scope = (self.counts["clean"] + self.counts["flagged"]
                    + self.counts["blocked"])
        self.assertGreaterEqual(in_scope, MIN_IN_SCOPE_PIPELINES)

    def test_clean_pipeline_count_does_not_regress(self):
        self.assertGreaterEqual(
            self.counts["clean"], MIN_CLEAN,
            f"clean dropped to {self.counts['clean']}; counts={self.counts}")

    def test_emitted_queries_that_write_do_not_regress(self):
        self.assertGreaterEqual(
            len(self.writing), MIN_WRITING,
            f"only {len(self.writing)} emitted quer(ies) write anything; "
            f"the rest end on `result = <frame>` and discard it: "
            f"{self.emitted_without_a_write}")

    def test_no_bound_query_is_emitted_without_a_write(self):
        """`[BindToDefaultDestination = true]` says where the query writes.
        Emitting it with no write is the silent-drop failure; refusing it is
        acceptable, emitting a DataFrame nobody stores is not."""
        bound = []
        for name, query_name in self.emitted_without_a_write:
            attrs = next(q.get("attrs") or "" for file_name, doc in self.parsed
                         if file_name == name
                         for q in doc["queries"] if q["name"] == query_name)
            if "BindToDefaultDestination = true" in attrs:
                bound.append((name, query_name))
        self.assertEqual(bound, [])

    def test_every_declared_column_mapping_reaches_the_generated_file(self):
        """6 corpus files declare `ColumnSettings = [Mappings = {...}]`, 136
        mappings over 8 destinations. An emitted query that ignores its mapping
        writes the whole source frame and gives the target the wrong shape."""
        self.assertTrue(self.mapped, "no corpus query with a mapping emitted")
        self.assertEqual(
            [row for row in self.mapped if not row[3]], [],
            "emitted with a declared column mapping and no projection")

    def test_no_emitted_file_fails_to_compile(self):
        # translate_query refuses rather than returning code it cannot parse;
        # setUpClass collects every M31_UNCOMPILABLE_OUTPUT here. A non-empty
        # list is a defect in this translator, not in the corpus.
        self.assertEqual(self.uncompilable, [])

    def test_helpers_are_suppressed_not_emitted(self):
        self.assertGreater(self.counts["helper"], 0)

    def test_coverage_report_is_written(self):
        write_coverage_report(REPORT, len(self.parsed), self.counts)
        self.assertTrue(REPORT.is_file())


if __name__ == "__main__":
    unittest.main()

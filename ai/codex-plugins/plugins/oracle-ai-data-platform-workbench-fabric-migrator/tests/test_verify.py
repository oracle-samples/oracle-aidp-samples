import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.verify.checker import format_verify, verify


def _report(results, **over):
    counts = {}
    for row in results:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    base = {"report_id": "r1", "complete": True, "plan_id": "p1", "mode": "demo",
            "filter": None, "counts": counts, "results": results}
    base.update(over)
    return base


def _write(tmp: Path, report, artifacts=("notebooks/Ingest.py",)):
    for rel in artifacts:
        path = tmp / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
    path = tmp / "report.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


OK_ROW = {"asset_id": "notebook.Ingest", "kind": "notebook", "status": "ok",
          "changes": 2, "flags": 0, "output_path": "notebooks/Ingest.py"}


class VerdictTests(unittest.TestCase):
    def _verify(self, results, **kw):
        with TemporaryDirectory() as t:
            return verify(_write(Path(t), _report(results)), **kw)

    def test_ok_becomes_pass(self):
        self.assertEqual(self._verify([OK_ROW])["summary"]["PASS"], 1)

    def test_needs_manual_review_becomes_review(self):
        row = dict(OK_ROW, status="needs_manual_review", flags=1)
        self.assertEqual(self._verify([row])["summary"]["REVIEW"], 1)

    def test_planned_and_skipped_become_skip(self):
        rows = [{"asset_id": "a", "kind": "k", "status": "planned"},
                {"asset_id": "b", "kind": "k", "status": "skipped"}]
        self.assertEqual(self._verify(rows)["summary"]["SKIP"], 2)

    def test_error_becomes_fail(self):
        rows = [{"asset_id": "a", "kind": "k", "status": "error", "error": "boom"}]
        self.assertEqual(self._verify(rows)["summary"]["FAIL"], 1)

    def test_ok_with_flags_is_downgraded_to_fail(self):
        row = dict(OK_ROW, flags=2)
        result = self._verify([row])
        self.assertEqual(result["summary"]["FAIL"], 1)
        self.assertIn("flags are present", result["rows"][0]["note"])

    def test_unknown_status_becomes_fail(self):
        rows = [{"asset_id": "a", "kind": "k", "status": "weird"}]
        self.assertEqual(self._verify(rows)["summary"]["FAIL"], 1)

    def test_missing_asset_id_becomes_fail(self):
        rows = [{"kind": "k", "status": "ok", "flags": 0,
                 "output_path": "notebooks/Ingest.py"}]
        self.assertEqual(self._verify(rows)["summary"]["FAIL"], 1)

    def test_negative_flags_become_fail(self):
        self.assertEqual(self._verify([dict(OK_ROW, flags=-1)])["summary"]["FAIL"], 1)

    def test_corrupt_row_is_counted_not_dropped(self):
        with TemporaryDirectory() as t:
            report = _report([OK_ROW])
            report["results"].append("not a dict")
            report["counts"]["ok"] = 1
            result = verify(_write(Path(t), report))
        self.assertEqual(result["summary"]["FAIL"], 1)


class ArtifactTests(unittest.TestCase):
    def test_missing_artifact_is_fail(self):
        with TemporaryDirectory() as t:
            path = _write(Path(t), _report([OK_ROW]), artifacts=())
            result = verify(path)
        self.assertEqual(result["summary"]["FAIL"], 1)
        self.assertIn("missing", result["rows"][0]["note"])

    def test_absolute_output_path_is_fail(self):
        with TemporaryDirectory() as t:
            row = dict(OK_ROW, output_path="/etc/passwd")
            result = verify(_write(Path(t), _report([row])))
        self.assertIn("relative", result["rows"][0]["note"])

    def test_escaping_output_path_is_fail(self):
        with TemporaryDirectory() as t:
            row = dict(OK_ROW, output_path="../outside.py")
            result = verify(_write(Path(t), _report([row])))
        self.assertIn("escapes", result["rows"][0]["note"])


class IntegrityTests(unittest.TestCase):
    def test_in_progress_marker_blocks_verification(self):
        with TemporaryDirectory() as t:
            tmp = Path(t)
            path = _write(tmp, _report([OK_ROW]))
            (tmp / ".fabric-aidp-migration-in-progress").write_text("x", encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                verify(path)
        self.assertIn("incomplete", str(ctx.exception))

    def test_report_without_complete_is_rejected(self):
        with TemporaryDirectory() as t:
            path = _write(Path(t), _report([OK_ROW], complete=False))
            with self.assertRaises(ValueError):
                verify(path)

    def test_count_mismatch_is_rejected(self):
        with TemporaryDirectory() as t:
            report = _report([OK_ROW])
            report["counts"]["ok"] = 99
            with self.assertRaises(ValueError) as ctx:
                verify(_write(Path(t), report))
        self.assertIn("count mismatch", str(ctx.exception))

    def test_unknown_filter_is_rejected(self):
        with TemporaryDirectory() as t:
            path = _write(Path(t), _report([OK_ROW]))
            with self.assertRaises(ValueError):
                verify(path, filter_kind="nope")

    def test_filter_selects_by_asset_id_prefix(self):
        rows = [OK_ROW, {"asset_id": "warehouse.table.dbo.c", "kind": "warehouse_table",
                         "status": "planned"}]
        with TemporaryDirectory() as t:
            result = verify(_write(Path(t), _report(rows)), filter_kind="warehouse")
        self.assertEqual([r["asset_id"] for r in result["rows"]],
                         ["warehouse.table.dbo.c"])


class ClaimTests(unittest.TestCase):
    """The PASS wording must never inflate. This is a release gate."""

    def _text(self):
        with TemporaryDirectory() as t:
            return format_verify(verify(_write(Path(t), _report([OK_ROW]))))

    def test_states_pass_is_not_execution_verified(self):
        self.assertIn("not execution-verified", self._text())

    def test_never_claims_the_artifact_runs(self):
        lowered = self._text().lower()
        for phrase in ("ready to run", "will run", "verified working",
                       "guaranteed", "production-ready"):
            self.assertNotIn(phrase, lowered)

    def test_counts_are_still_reported(self):
        self.assertIn("PASS:", self._text())




class BlockedVerdictTests(unittest.TestCase):
    """A blocked object is an honest refusal, not a tool failure.

    The migrate runner emits `status: "blocked"` when a translator exists but
    the object uses something out of scope -- Excel, Snowflake, an
    untranslatable expression. verify had never been taught the status, so it
    fell through to the default and reported FAIL: the tool accusing itself of
    breaking when it had in fact done the right thing. No fixture produced a
    blocked row until the demo estate gained a Dataflow.
    """

    def _verify(self, status):
        report = {"report_id": "r", "complete": True, "counts": {status: 1},
                  "results": [{"asset_id": "dataflow.X.q",
                               "kind": "dataflow_query", "status": status,
                               "findings": [{"rule": "M91_UNSUPPORTED_CONNECTOR",
                                             "detail": "Excel.Workbook",
                                             "severity": "flag"}]}]}
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            return verify(path)

    def test_blocked_is_review_not_fail(self):
        summary = self._verify("blocked")["summary"]
        self.assertEqual(summary["FAIL"], 0)
        self.assertEqual(summary["REVIEW"], 1)

    def test_blocked_is_not_skip(self):
        """SKIP would read as 'nothing to do'; there is a whole object to port."""
        self.assertEqual(self._verify("blocked")["summary"]["SKIP"], 0)

    def test_a_real_error_is_still_fail(self):
        self.assertEqual(self._verify("error")["summary"]["FAIL"], 1)


class NamespacePlaceholderTests(unittest.TestCase):
    """`plan` once, `migrate` many times: verify is the last place that can
    notice a placeholder namespace baked into a shipped artifact.

    The runner flags it (NS01), so a report it wrote never says `ok` for one.
    A report from an older migrate, or a hand-edited one, still can -- and an
    `ok` row whose artifact cannot resolve is the same class of internal
    contradiction as an `ok` row with manual-review flags, which is already
    FAIL.
    """

    PLACEHOLDER = "<your-oci-namespace>"

    def _verify(self, status, body, **kw):
        with TemporaryDirectory() as t:
            tmp = Path(t)
            (tmp / "notebooks").mkdir(parents=True)
            (tmp / "notebooks" / "Ingest.py").write_text(body, encoding="utf-8")
            row = dict(OK_ROW, status=status,
                       flags=1 if status == "needs_manual_review" else 0)
            path = tmp / "report.json"
            path.write_text(json.dumps(_report([row])), encoding="utf-8")
            return verify(path, **kw)

    def test_ok_with_a_placeholder_in_the_artifact_is_fail(self):
        result = self._verify("ok", f'spark.read.parquet("oci://Sales@'
                                    f'{self.PLACEHOLDER}/Files/x")\n')
        self.assertEqual(result["summary"]["FAIL"], 1)
        self.assertIn("namespace", result["rows"][0]["note"])

    def test_a_clean_artifact_still_passes(self):
        result = self._verify("ok", 'spark.read.parquet("oci://Sales@acmens/x")\n')
        self.assertEqual(result["summary"]["PASS"], 1)

    def test_a_review_row_says_why_rather_than_being_downgraded(self):
        """It is already REVIEW; the reader still has to be told which of the
        flags is the unusable target."""
        result = self._verify("needs_manual_review",
                              f"oci://Sales@{self.PLACEHOLDER}/x\n")
        self.assertEqual(result["summary"]["REVIEW"], 1)
        self.assertIn("namespace", result["rows"][0]["note"])


class FilterHidesNothingThatFailsTests(unittest.TestCase):
    """A filtered `verify` could exit 0 on a migration that has errors.

    `_row_source` attributed a row two ways: the `<source>.` prefix of its
    asset_id, or a `kind` that starts with a source name. An error row
    carries the plan's own type as its kind -- `fabric_notebook`, which
    starts with none of them -- so only the prefix could match it, and a row
    with no usable prefix matched nothing and was dropped by every filter.
    Dropping a FAIL is the case that matters: `verify --filter notebook`
    printed FAIL 0 and exited 0 with an errored asset in the report.
    """

    def _verify(self, rows, **kw):
        with TemporaryDirectory() as t:
            return verify(_write(Path(t), _report(rows)), **kw)

    def test_an_error_row_is_attributed_by_its_plan_source_type(self):
        rows = [{"asset_id": "notebook.Boom", "kind": "fabric_notebook",
                 "status": "error", "error": "boom"}]
        result = self._verify(rows, filter_kind="notebook")
        self.assertEqual([r["asset_id"] for r in result["rows"]], ["notebook.Boom"])

    def test_a_warehouse_error_row_is_attributed_too(self):
        rows = [{"asset_id": "warehouse.W.table.dbo.c",
                 "kind": "fabric_warehouse_table", "status": "error",
                 "error": "boom"}]
        self.assertEqual(self._verify(rows, filter_kind="warehouse")["summary"]["FAIL"], 1)

    def test_a_row_with_no_attributable_source_is_not_hidden_when_it_fails(self):
        rows = [OK_ROW, {"asset_id": "<missing-id>", "kind": "unknown",
                         "status": "error", "error": "boom"}]
        result = self._verify(rows, filter_kind="notebook")
        self.assertEqual(result["summary"]["FAIL"], 1)
        self.assertIn("<missing-id>", [r["asset_id"] for r in result["rows"]])

    def test_a_passing_row_outside_the_filter_is_still_hidden(self):
        """The filter must still filter: only FAIL is exempt."""
        rows = [OK_ROW, {"asset_id": "pipeline.P", "kind": "pipeline_job",
                         "status": "blocked"}]
        result = self._verify(rows, filter_kind="notebook")
        self.assertEqual([r["asset_id"] for r in result["rows"]], ["notebook.Ingest"])

    def test_an_unreadable_item_row_is_not_hidden_when_it_fails(self):
        """`unreadable.<path>` and `unsupported.<type>.<name>` belong to no
        source at all -- no scanner claimed them."""
        rows = [{"asset_id": "unreadable.Odd.Notebook", "kind": "unreadable_item",
                 "status": "error", "error": "boom"}]
        self.assertEqual(self._verify(rows, filter_kind="notebook")["summary"]["FAIL"], 1)


class FilterHidesNothingItCannotAttributeTests(unittest.TestCase):
    """The FAIL half of the rule above was closed and the rest was not.

    `unreadable.<path>` (INV01) and `unsupported.<type>.<name>` (INV02) are
    refusals, and the runner writes them `blocked`, which verify reads as
    REVIEW -- not FAIL. `_row_source` returns None for both, because they
    belong to no slice by design. So `verify --filter <anything>` dropped
    them, in all six slices, and there was no slice left to see them in.

    MEASURED on a report holding one PASS notebook and those two refusals:

        verify, no filter                PASS 1  REVIEW 2
        verify --filter notebook         PASS 1  REVIEW 0
        verify --filter warehouse        PASS 0  REVIEW 0
        ... and the same for the other four

    The comment beside the filter already claimed this case was covered --
    it names "an `unreadable.`/`unsupported.` asset" as the thing that
    "vanished from every filtered view" -- and only the FAIL branch was
    written. The rule is the same one, so the code now matches the comment:
    a row this tool cannot attribute to any slice is never hidden.
    """

    UNREADABLE = {"asset_id": "unreadable.Broken.Report",
                  "kind": "unreadable_item", "status": "blocked",
                  "findings": [{"rule": "INV01_ITEM_UNREADABLE",
                                "detail": "identity could not be read",
                                "severity": "flag"}]}
    UNSUPPORTED = {"asset_id": "unsupported.Dashboard.Dash",
                   "kind": "unsupported_item", "status": "blocked",
                   "findings": [{"rule": "INV02_ITEM_TYPE_UNSUPPORTED",
                                 "detail": "no scanner for 'Dashboard'",
                                 "severity": "flag"}]}

    SLICES = ("notebook", "warehouse", "lakehouse", "pipeline",
              "semanticmodel", "dataflow")

    def _verify(self, rows, **kw):
        with TemporaryDirectory() as t:
            return verify(_write(Path(t), _report(rows)), **kw)

    def test_every_slice_still_shows_both_refusals(self):
        rows = [OK_ROW, self.UNREADABLE, self.UNSUPPORTED]
        for slice_name in self.SLICES:
            with self.subTest(filter=slice_name):
                result = self._verify(rows, filter_kind=slice_name)
                shown = [r["asset_id"] for r in result["rows"]]
                self.assertIn("unreadable.Broken.Report", shown)
                self.assertIn("unsupported.Dashboard.Dash", shown)
                self.assertEqual(result["summary"]["REVIEW"], 2)

    def test_the_row_says_why_a_filtered_view_is_showing_it(self):
        result = self._verify([self.UNREADABLE], filter_kind="warehouse")
        note = result["rows"][0]["note"]
        self.assertIn("--filter warehouse", note)
        self.assertIn("no source slice", note)
        # the reason it was refused still comes first
        self.assertTrue(note.startswith("identity could not be read"), note)

    def test_an_unfiltered_verify_adds_no_such_sentence(self):
        result = self._verify([OK_ROW, self.UNREADABLE])
        for row in result["rows"]:
            self.assertNotIn("--filter", row["note"] or "")

    def test_a_row_that_does_belong_to_another_slice_is_still_hidden(self):
        """The filter must still filter. Only a row that belongs to no slice
        is exempt, and `pipeline.P` belongs to one."""
        rows = [OK_ROW, {"asset_id": "pipeline.P", "kind": "pipeline_job",
                         "status": "blocked"}]
        result = self._verify(rows, filter_kind="notebook")
        self.assertEqual([r["asset_id"] for r in result["rows"]],
                         ["notebook.Ingest"])
        self.assertEqual(result["summary"]["REVIEW"], 0)

    def test_the_exit_code_question_a_filtered_verify_answers(self):
        """FAIL stays 0 for a refusal -- `blocked` is REVIEW, not FAIL, and
        that grading is deliberate. What changes is that the operator can
        see there are two, from whichever slice they asked for."""
        result = self._verify([OK_ROW, self.UNREADABLE, self.UNSUPPORTED],
                              filter_kind="dataflow")
        self.assertEqual(result["summary"]["FAIL"], 0)
        self.assertEqual(result["summary"]["REVIEW"], 2)


class UnclaimedArtifactsReachVerifyTests(unittest.TestCase):
    """verify is the last thing read before `publish`, so it says when the
    directory holds files the report does not vouch for.

    Not a verdict. Running a second `--filter` into one output directory is
    a workflow this tool recommends (PL17 says to), so failing it would fail
    correct use. The count is carried through from the report rather than
    re-derived: the runner is the thing that knows what it wrote.
    """

    def _verify(self, report, **kw):
        with TemporaryDirectory() as t:
            return verify(_write(Path(t), report), **kw)

    def test_a_report_with_none_says_nothing(self):
        result = self._verify(_report([OK_ROW]))
        self.assertEqual(result["unclaimed_artifacts"], [])
        self.assertNotIn("not written by", format_verify(result))

    def test_a_report_that_names_some_prints_each_one(self):
        result = self._verify(_report(
            [OK_ROW], unclaimed_artifacts=["warehouse/a.spark.sql",
                                           "warehouse/b.spark.sql"]))
        text = format_verify(result)
        self.assertIn("2 file(s)", text)
        self.assertIn("warehouse/a.spark.sql", text)
        self.assertIn("warehouse/b.spark.sql", text)

    def test_it_does_not_change_any_verdict(self):
        result = self._verify(_report(
            [OK_ROW], unclaimed_artifacts=["warehouse/a.spark.sql"]))
        self.assertEqual(result["summary"],
                         {"PASS": 1, "REVIEW": 0, "SKIP": 0, "FAIL": 0})

    def test_a_report_from_an_older_migrate_has_no_such_key(self):
        report = _report([OK_ROW])
        report.pop("unclaimed_artifacts", None)
        self.assertEqual(self._verify(report)["unclaimed_artifacts"], [])

    def test_a_non_list_value_is_ignored_rather_than_crashing(self):
        result = self._verify(_report([OK_ROW], unclaimed_artifacts="oops"))
        self.assertEqual(result["unclaimed_artifacts"], [])


class BlockedRowSaysWhyTests(unittest.TestCase):
    """`verify` printed `REVIEW pipeline.Refused` and nothing else.

    A blocked row carries its reason in `findings`, and the note was built
    from `note`/`error` only, so the one line telling the reader what to do
    about the refusal was dropped.
    """

    def _rows(self, row):
        with TemporaryDirectory() as t:
            return verify(_write(Path(t), _report([row])))["rows"]

    def test_the_refusal_reason_reaches_the_row(self):
        rows = self._rows({
            "asset_id": "pipeline.Daily", "kind": "pipeline_job",
            "status": "blocked",
            "findings": [{"rule": "PL90_UNSUPPORTED_ACTIVITY",
                          "detail": "activity 'Copy in' is a Copy",
                          "severity": "flag"}]})
        self.assertIn("is a Copy", rows[0]["note"])

    def test_it_reaches_the_printed_report(self):
        result = {"summary": {"PASS": 0, "REVIEW": 1, "SKIP": 0, "FAIL": 0},
                  "rows": self._rows({
                      "asset_id": "pipeline.Daily", "kind": "pipeline_job",
                      "status": "blocked",
                      "findings": [{"rule": "PL90_UNSUPPORTED_ACTIVITY",
                                    "detail": "activity 'Copy in' is a Copy",
                                    "severity": "flag"}]})}
        self.assertIn("is a Copy", format_verify(result))


if __name__ == "__main__":
    unittest.main()

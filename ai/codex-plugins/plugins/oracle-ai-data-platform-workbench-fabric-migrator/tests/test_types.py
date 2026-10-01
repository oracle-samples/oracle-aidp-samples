import errno
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from fabric_aidp.translate.types import Finding, TranslationResult
from fabric_aidp._atomic import _temp_path, write_text_atomic, write_json_atomic


class FindingTests(unittest.TestCase):
    def test_str_includes_severity_and_rule(self):
        f = Finding(rule="R01", detail="did a thing", severity="rewrite")
        self.assertEqual(str(f), "[rewrite] R01: did a thing")


class TranslationResultTests(unittest.TestCase):
    def _result(self, *findings):
        return TranslationResult(source_sql="a", translated_sql="b",
                                 findings=list(findings))

    def test_counts_rewrites_and_flags_separately(self):
        r = self._result(
            Finding("R01", "x", "rewrite"),
            Finding("R02", "y", "flag"),
            Finding("R03", "z", "rewrite"),
        )
        self.assertEqual(r.changes, 2)
        self.assertEqual(r.flags, 1)

    def test_needs_manual_review_only_when_flagged(self):
        self.assertFalse(self._result(Finding("R01", "x", "rewrite")).needs_manual_review)
        self.assertTrue(self._result(Finding("R02", "y", "flag")).needs_manual_review)

    def test_findings_default_to_empty_and_are_not_shared(self):
        a = TranslationResult(source_sql="", translated_sql="")
        b = TranslationResult(source_sql="", translated_sql="")
        a.findings.append(Finding("R01", "x", "flag"))
        self.assertEqual(b.findings, [])


class AtomicWriteTests(unittest.TestCase):
    def test_text_write_replaces_existing_content(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "sub" / "out.txt"
            write_text_atomic(p, "first")
            write_text_atomic(p, "second")
            self.assertEqual(p.read_text(encoding="utf-8"), "second")

    def test_no_temp_files_left_behind(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "out.txt"
            write_text_atomic(p, "x")
            self.assertEqual([q.name for q in Path(d).iterdir()], ["out.txt"])

    def test_json_write_is_indented_and_utf8(self):
        with TemporaryDirectory() as d:
            p = Path(d) / "out.json"
            write_json_atomic(p, {"k": "café"})
            text = p.read_text(encoding="utf-8")
            self.assertIn('\n  "k"', text)
            self.assertIn("café", text)


class TempNameLengthTests(unittest.TestCase):
    """Writing through a temp file costs name budget, and the bill was large.

    `.{name}.{pid}.{uuid4().hex}.tmp` put roughly 44 characters on top of the
    final filename, 32 of them the uuid hex. Measured 2026-09-29 on macOS,
    NAME_MAX 255, pid 543: a 224-character name that `Path.write_text`
    accepts directly became a 266-character temp name and failed with
    [Errno 63] File name too long. On Windows the default MAX_PATH of 260
    makes the same gap bite on the whole path.
    """

    #: The most name budget the atomic writer is allowed to spend. Stated as a
    #: constant on purpose: deriving it from the code under test would make
    #: this assertion true by construction. 24 leaves room for a pid of up to
    #: nine digits on top of the fixed `.` `.` `.` `.tmp` and the 8-hex token.
    BUDGET = 24

    @staticmethod
    def _longest_name_here(directory: Path) -> int:
        """The longest filename this filesystem and platform actually take.

        Probed rather than assumed: NAME_MAX is 255 on macOS and Linux, but on
        Windows it is the 260-character whole path that runs out first, and
        that depends on where the temp directory is.
        """
        longest = 0
        for length in range(16, 256, 8):
            probe = directory / ("a" * length)
            try:
                probe.write_text("p", encoding="utf-8")
            except OSError:
                break
            probe.unlink()
            longest = length
        return longest

    def test_the_writer_costs_at_most_24_characters_of_name_budget(self):
        with TemporaryDirectory() as d:
            directory = Path(d)
            longest = self._longest_name_here(directory)
            self.assertGreater(longest, self.BUDGET,
                               "no usable name budget to test with")
            path = directory / ("a" * (longest - self.BUDGET))
            write_text_atomic(path, "kept")
            self.assertEqual(path.read_text(encoding="utf-8"), "kept")

    def test_the_suffix_is_a_short_token_not_a_full_uuid(self):
        temp = _temp_path(Path("/out") / "dbo.claim.spark.sql")
        overhead = len(temp.name) - len("dbo.claim.spark.sql")
        self.assertLessEqual(overhead, self.BUDGET)
        self.assertRegex(temp.name, r"^\.dbo\.claim\.spark\.sql\.\d+\.[0-9a-f]{8}\.tmp$")

    def test_two_writes_in_one_process_do_not_pick_the_same_temp_name(self):
        path = Path("/out") / "report.json"
        self.assertNotEqual(_temp_path(path).name, _temp_path(path).name)


class TempWriteFailureTests(unittest.TestCase):
    """What the failure says when the temp file cannot be created.

    Before: `[Errno 63] File name too long: '<the temp path>'` on POSIX, and
    on Windows `[WinError 3] The system cannot find the path specified` --
    which reads as a missing output directory. Neither said the final name
    was fine and only the temporary one was not.
    """

    def _raise_from_open(self, exc):
        with TemporaryDirectory() as d:
            path = Path(d) / "report.json"
            with mock.patch.object(Path, "open", side_effect=exc):
                with self.assertRaises(OSError) as caught:
                    write_text_atomic(path, "x")
            self.assertFalse(path.exists())
            self.assertEqual([q.name for q in Path(d).iterdir()], [])
        return caught.exception

    def test_a_length_failure_says_it_was_the_temp_name(self):
        error = self._raise_from_open(
            OSError(errno.ENAMETOOLONG, "File name too long"))
        message = str(error)
        self.assertEqual(error.errno, errno.ENAMETOOLONG)
        self.assertIn("could not create the temporary file", message)
        self.assertIn("characters against the final path", message)
        self.assertIn("length limit that the final name clears", message)
        self.assertRegex(message, r"\.report\.json\.\d+\.[0-9a-f]{8}\.tmp")

    def test_a_windows_path_not_found_gets_the_same_explanation(self):
        """ERROR_PATH_NOT_FOUND surfaces as ENOENT, and the directory the
        reader would then go looking for is not the thing that is missing."""
        message = str(self._raise_from_open(
            OSError(errno.ENOENT, "The system cannot find the path specified")))
        self.assertIn("Its directory exists", message)
        self.assertIn("length limit that the final name clears", message)

    def test_the_real_filesystem_failure_carries_the_explanation(self):
        """Not mocked. The temp name is too long to `unlink` as well as to
        create, and a raise from the `finally` cleanup displaces the
        exception in flight -- so with the old cleanup this arrived as a bare
        `[Errno 63] File name too long` from `temp.unlink()`.
        """
        with TemporaryDirectory() as d:
            directory = Path(d)
            longest = TempNameLengthTests._longest_name_here(directory)
            self.assertGreater(longest, 0, "no usable name budget to test with")
            # The longest name the filesystem takes: any temp suffix overflows.
            path = directory / ("a" * longest)
            with self.assertRaises(OSError) as caught:
                write_text_atomic(path, "x")
            message = str(caught.exception)
            self.assertIn("could not create the temporary file", message)
            self.assertIn("length limit that the final name clears", message)
            self.assertFalse(path.exists())

    def test_a_permission_failure_is_not_blamed_on_the_name_length(self):
        """Every OSError here names the temp file, but only a length-shaped
        one is explained as a length problem. Saying otherwise would move the
        misdirection rather than remove it."""
        message = str(self._raise_from_open(
            OSError(errno.EACCES, "Permission denied")))
        self.assertIn("could not create the temporary file", message)
        self.assertIn("Permission denied", message)
        self.assertNotIn("length limit", message)


if __name__ == "__main__":
    unittest.main()

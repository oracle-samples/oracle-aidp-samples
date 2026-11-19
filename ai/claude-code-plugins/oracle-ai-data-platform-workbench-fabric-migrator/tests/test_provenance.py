"""Every vendored file is traceable to a commit, and licensed.

These fixtures are other people's work, copied verbatim. Two things have to
stay true, and neither is self-evident from looking at the tree:

  * **A file can be traced to the exact commit it came from.** The manifests
    used to record `/blob/HEAD/` URLs. HEAD moves, files are renamed and
    deleted, so within a year such a link points at whatever that path holds
    or at nothing, and the record stops being evidence.
  * **Every repository we copied from has its licence reproduced.** MIT's one
    condition is the copyright and permission notice. Naming the licence is
    not reproducing it.

`scripts/verify_provenance.py` re-checks the stronger claim -- that each file
is byte-identical to its recorded commit -- against GitHub. These tests need
no network: they guard the bookkeeping, so a file added without a record, or
a URL quietly reverted to HEAD, fails here.
"""
from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

REAL = Path(__file__).resolve().parent / "fixtures" / "real"
CORPORA = REAL / "corpora"
LICENCES = REAL / "THIRD_PARTY_LICENSES.md"
_SHA = re.compile(r"^[0-9a-f]{40}$")


def _manifests():
    paths = sorted(CORPORA.glob("*/PROVENANCE.json"))
    legacy = REAL / "PROVENANCE.json"
    if legacy.is_file():
        paths.append(legacy)
    return [(p, json.loads(p.read_text(encoding="utf-8"))) for p in paths]


def _entries():
    for path, payload in _manifests():
        for entry in payload.get("files", []):
            yield path, entry


def _vendored_files():
    """Every file under tests/fixtures/real/ that is not our own bookkeeping."""
    ours = {"NOTICE", "THIRD_PARTY_LICENSES.md", "PROVENANCE.json"}
    for path in REAL.rglob("*"):
        if path.is_file() and path.name not in ours:
            yield path


class PinningTests(unittest.TestCase):
    def test_no_manifest_url_points_at_head(self):
        offenders = [f"{p.parent.name}/{e['file']}" for p, e in _entries()
                     if "/blob/HEAD/" in e.get("url", "")]
        self.assertEqual(offenders, [], "provenance reverted to a moving target")

    def test_every_entry_records_a_commit(self):
        missing = [f"{p.parent.name}/{e['file']}" for p, e in _entries()
                   if not _SHA.match(str(e.get("sha", "")))]
        self.assertEqual(missing, [])

    def test_the_url_and_the_sha_agree(self):
        """A hand-edited URL that no longer matches its sha is worse than
        either alone, because both look authoritative."""
        for path, entry in _entries():
            with self.subTest(file=entry["file"]):
                self.assertIn(entry["sha"], entry["url"])

    def test_the_url_names_the_repo_and_the_path(self):
        for path, entry in _entries():
            with self.subTest(file=entry["file"]):
                self.assertIn(entry["repo"], entry["url"])
                self.assertTrue(entry["url"].endswith(entry["path"]))


class CoverageTests(unittest.TestCase):
    def test_every_vendored_file_has_a_provenance_entry(self):
        recorded = set()
        for path, entry in _entries():
            base = path.parent
            recorded.add((base / entry["file"]).resolve())
        on_disk = {p.resolve() for p in _vendored_files()}
        unrecorded = sorted(str(p.relative_to(REAL)) for p in on_disk - recorded)
        self.assertEqual(unrecorded, [], "vendored without recording where it came from")

    def test_every_recorded_file_is_actually_here(self):
        missing = []
        for path, entry in _entries():
            if not (path.parent / entry["file"]).is_file():
                missing.append(entry["file"])
        self.assertEqual(missing, [])

    def test_the_counts_in_the_manifests_are_true(self):
        for path, payload in _manifests():
            with self.subTest(manifest=path.parent.name):
                self.assertEqual(payload.get("count"), len(payload["files"]))


class LicenceTests(unittest.TestCase):
    """MIT's one condition is the copyright and permission notice. Recording
    the licence's *name* is not reproducing it -- that gap was a release
    blocker once already."""

    @classmethod
    def setUpClass(cls):
        cls.text = LICENCES.read_text(encoding="utf-8")

    def test_every_repo_we_copied_from_has_its_licence_reproduced(self):
        repos = {e["repo"] for _, e in _entries()}
        missing = sorted(r for r in repos if f"## {r}" not in self.text)
        self.assertEqual(missing, [])

    def test_each_section_carries_a_copyright_line(self):
        sections = re.split(r"^## ", self.text, flags=re.M)[1:]
        for section in sections:
            name = section.splitlines()[0].strip()
            with self.subTest(repo=name):
                self.assertRegex(section, r"(?i)copyright")

    ALLOWED = frozenset({"MIT", "Apache-2.0", "BSD-3-Clause", "BSD-2-Clause",
                         "MPL-2.0"})

    def test_every_section_declares_a_licence_we_may_redistribute(self):
        """A GPL file in an MIT repository is a licence conflict, not a
        paperwork problem: 2 of the 149 candidate files were GPL-2.0 and were
        deliberately not vendored, along with 59 carrying no licence at all.

        This reads each section's declared licence rather than grepping the
        bodies. Grepping fails: MPL-2.0's own text names the GPL and the
        AGPL when it defines a Secondary License, so a body search flags a
        licence we deliberately allow.
        """
        declared = re.findall(r"^Licence: ([A-Za-z0-9.-]+)", self.text, re.M)
        self.assertEqual(len(declared), len(re.findall(r"^## ", self.text, re.M)),
                         "a section declares no licence")
        forbidden = sorted({d for d in declared if d not in self.ALLOWED})
        self.assertEqual(forbidden, [])


if __name__ == "__main__":
    unittest.main()

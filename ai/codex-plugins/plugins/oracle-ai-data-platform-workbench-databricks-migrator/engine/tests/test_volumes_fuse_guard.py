"""2026-07-28: /Volumes added to the FUSE-mount recursive-scan guard in
agent_migrate.py.

Root cause: prior guard covered `/Workspace` and `/aidp` only. A run in
Example_Txn_Budget_Monitoring had Opus fire a `run_on_cluster` diagnostic:

    import os, glob
    for pat in ['/Volumes/**/hourly_monitoring_config.ini',
                '/Volumes/**/control_plane/*']:
        print(pat, glob.glob(pat, recursive=True)[:20])

The `/Volumes` FUSE mount walks the same way as `/Workspace` — millions of
entries, HTTP roundtrip per readdir. The kernel hung for 20 minutes, then
after Python-side timeout the kernel was still stuck (AIDP does not expose
/kernels/{id}/interrupt) so all subsequent execute_requests queued behind it
indefinitely.

This file exercises the four block sites in agent_migrate.py to prove they
now cover `/Volumes` alongside `/Workspace` and `/aidp`.

We test the block RULE (the regex-based check inside run_on_cluster) rather
than the full tool-loop — the check is a pure function of the code string
and its policy decision, no cluster required.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT_MIGRATE = os.path.join(REPO, "scripts", "agent_migrate.py")


def _check_block_rule(code: str) -> str:
    """Reproduce the FUSE-mount guard logic from agent_migrate.py so we can
    unit-test the policy without importing the module (which pulls OCI SDK).

    This intentionally MIRRORS the code in agent_migrate.py — if the source
    changes, this must be kept in sync. Structural test at the bottom
    (`test_source_uses_volumes_in_all_four_sites`) verifies the source-of-
    truth still contains the `/Volumes` alternation, catching accidental
    regressions where someone deletes it here or in the source.
    """
    _re_scan = re
    _active_code = "\n".join(
        ln for ln in (code or "").splitlines()
        if ln.strip() and not ln.strip().lstrip().startswith("#")
    )
    if _re_scan.search(r'glob\.glob\s*\([^)]*recursive\s*=\s*True', _active_code) and \
       _re_scan.search(r'["\']/(Workspace|aidp|Volumes)', _active_code):
        return "BLOCKED: recursive_glob"
    if _re_scan.search(r'os\.walk\s*\(\s*[\'"](/(Workspace|aidp|Volumes))', _active_code):
        return "BLOCKED: os_walk"

    _mentions_fuse_mount_path = bool(
        _re_scan.search(r'/(Workspace|aidp|Volumes)\b', _active_code)
    )
    if _mentions_fuse_mount_path and _re_scan.search(
        r'\bgrep\b[^\n]*?(?:-[rR]|["\']-[rR])[rRlLnN]*\b', _active_code,
    ):
        return "BLOCKED: recursive_grep"
    if _mentions_fuse_mount_path and _re_scan.search(r'\bfind\b', _active_code):
        return "BLOCKED: find"
    if _mentions_fuse_mount_path and _re_scan.search(r'\bls\b[^\n]*?-R[a-zA-Z]*\b', _active_code):
        return "BLOCKED: ls_recursive"
    if _re_scan.search(r'\.rglob\s*\(', _active_code):
        return "BLOCKED: rglob"
    if _re_scan.search(r"""\.glob\s*\(\s*["'][^"']*\*\*""", _active_code):
        return "BLOCKED: pathlib_glob_star_star"
    if _re_scan.search(r'''glob\.glob\s*\(\s*["'][^"']*/(Workspace|aidp|Volumes)[^"']*\*\*''', _active_code):
        return "BLOCKED: glob_glob_explicit_star_star"
    return "ALLOWED"


class TestVolumesRecursiveGlobBlocked(unittest.TestCase):
    """Positive: recursive scans on /Volumes MUST block."""

    def test_glob_recursive_true_on_volumes(self):
        """The exact case from the Example_Txn_Budget_Monitoring hang."""
        code = (
            "import glob\n"
            "glob.glob('/Volumes/default/default/dbfs/FileStore/**/hourly_config.ini', recursive=True)"
        )
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_glob_recursive_true_shallow_volumes_path(self):
        code = "glob.glob('/Volumes/**/x', recursive=True)"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_os_walk_on_volumes(self):
        code = "import os\nfor root, _, _ in os.walk('/Volumes/default'): print(root)"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_grep_r_on_volumes(self):
        code = "import subprocess\nsubprocess.run(['grep', '-r', 'pattern', '/Volumes/default/'])"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_find_on_volumes(self):
        code = "subprocess.run(['find', '/Volumes/default/', '-name', '*.pkl'])"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_ls_recursive_on_volumes(self):
        code = "subprocess.run(['ls', '-R', '/Volumes/default'])"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_glob_glob_explicit_star_star_volumes(self):
        code = "import glob\nglob.glob('/Volumes/foo/**/bar.txt')"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))


class TestExistingWorkspaceAidpStillBlocked(unittest.TestCase):
    """Backwards-compat: /Workspace and /aidp MUST STILL block."""

    def test_glob_recursive_true_on_workspace_still_blocks(self):
        code = "import glob\nglob.glob('/Workspace/foo/**/*.ipynb', recursive=True)"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_os_walk_on_aidp_still_blocks(self):
        code = "import os\nfor r, _, _ in os.walk('/aidp/libraries'): pass"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))

    def test_grep_r_on_workspace_still_blocks(self):
        code = "subprocess.run(['grep', '-r', 'foo', '/Workspace/'])"
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))


class TestSingleDirVolumesAllowed(unittest.TestCase):
    """Negative: single-directory, single-file reads on /Volumes are FINE.
    We only ban RECURSIVE traversal, not the FUSE mount itself."""

    def test_os_path_exists_on_volumes(self):
        code = "import os\nprint(os.path.exists('/Volumes/default/default/foo.txt'))"
        self.assertEqual(_check_block_rule(code), "ALLOWED")

    def test_os_listdir_on_volumes_single_dir(self):
        code = "import os\nprint(os.listdir('/Volumes/default/default/dbfs/FileStore'))"
        self.assertEqual(_check_block_rule(code), "ALLOWED")

    def test_open_read_on_volumes(self):
        code = "with open('/Volumes/default/x.txt') as f: print(f.read())"
        self.assertEqual(_check_block_rule(code), "ALLOWED")

    def test_glob_glob_non_recursive_single_star(self):
        """Bounded single-level `*` glob is allowed (walks ONE directory)."""
        # No ** and no recursive=True — the checks only fire on recursive shapes
        code = "import glob\nglob.glob('/Volumes/default/default/*.txt')"
        self.assertEqual(_check_block_rule(code), "ALLOWED")

    def test_pathlib_glob_star_bounded_allowed(self):
        code = "from pathlib import Path\nlist(Path('/Volumes/default').glob('*'))"
        self.assertEqual(_check_block_rule(code), "ALLOWED")


class TestLocalTmpBehavior(unittest.TestCase):
    """/tmp/ is local disk, NOT a FUSE mount.

    Behavior on /tmp/ inherited from the existing guard (unchanged by the
    /Volumes fix):
      - os.walk('/tmp/') → ALLOWED (path check gates on Workspace|aidp|Volumes)
      - glob.glob('/tmp/**') → BLOCKED (the blanket `.glob('**')` rule fires
        regardless of path prefix — over-blocking on purpose, since Opus can
        misuse variables that resolve to FUSE)
    These aren't behaviors we're changing here — they document the pre-existing
    guard's coverage so a future refactor of `.glob('**')` rule doesn't silently
    regress them.
    """

    def test_os_walk_on_tmp_allowed(self):
        code = "import os\nfor r, _, _ in os.walk('/tmp/my_workdir'): pass"
        self.assertEqual(_check_block_rule(code), "ALLOWED")

    def test_glob_recursive_on_tmp_blocked_by_blanket_rule(self):
        """The blanket `.glob('**')` rule fires — over-blocks /tmp but this
        is existing behavior, not something the /Volumes fix changes."""
        code = "import glob\nglob.glob('/tmp/**/*.log', recursive=True)"
        # We just care that /tmp isn't ALLOWED through the /Volumes-specific
        # sites — the blanket blocker is fine.
        self.assertTrue(_check_block_rule(code).startswith("BLOCKED"))


class TestSourceHasVolumesAtAllFourSites(unittest.TestCase):
    """Structural guard: verify agent_migrate.py source itself lists /Volumes
    in the FUSE-mount alternation at all four block sites. Catches accidental
    revert of the fix (e.g. someone rewrites the regex and drops /Volumes).

    Use plain substring containment — the alternation appears verbatim as
    `Workspace|aidp|Volumes` in every one of these sites in the source.
    """

    @classmethod
    def setUpClass(cls):
        cls.src = open(AGENT_MIGRATE).read()

    def test_workspace_aidp_volumes_alternation_appears_multiple_times(self):
        """The 3-way alternation must appear at least 4 times in the source
        (once per site: recursive-glob path check, os.walk check, mentions-
        fuse-mount check, glob.glob '/**' check)."""
        count = self.src.count("Workspace|aidp|Volumes")
        self.assertGreaterEqual(
            count, 4,
            f"Workspace|aidp|Volumes alternation appears only {count} times — "
            "expected >=4 (one per block site). Someone may have reverted the "
            "/Volumes coverage — investigate before merging."
        )

    def test_no_2way_workspace_aidp_regressed(self):
        """After the fix, `Workspace|aidp` (2-way, without Volumes) should
        appear ZERO times in a regex literal (r'/(Workspace|aidp)...') — every
        regex was extended to the 3-way form. Prose comments are fine."""
        # Search for the 2-way form inside regex parens `(Workspace|aidp)`.
        pattern = re.compile(r"\(Workspace\|aidp\)")
        matches = pattern.findall(self.src)
        self.assertEqual(
            len(matches), 0,
            f"Regex `(Workspace|aidp)` (2-way, no Volumes) appears {len(matches)} "
            "times — every occurrence should now be `(Workspace|aidp|Volumes)`. "
            "Someone re-introduced the pre-fix 2-way alternation."
        )

    def test_mentions_fuse_mount_path_renamed(self):
        """Variable renamed from _mentions_workspace_path to _mentions_fuse_mount_path
        for accuracy (it now covers 3 FUSE mounts, not just Workspace)."""
        self.assertIn("_mentions_fuse_mount_path", self.src)
        # Old name should be gone (in code — comments/prose can still reference).
        # Match the assignment specifically.
        old_assignment = re.search(
            r"^\s*_mentions_workspace_path\s*=", self.src, re.MULTILINE
        )
        self.assertIsNone(
            old_assignment,
            "Old `_mentions_workspace_path =` assignment still present — "
            "rename to `_mentions_fuse_mount_path` incomplete."
        )


if __name__ == "__main__":
    unittest.main()

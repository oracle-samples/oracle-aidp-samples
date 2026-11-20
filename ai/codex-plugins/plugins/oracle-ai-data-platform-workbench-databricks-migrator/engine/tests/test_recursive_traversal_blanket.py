"""Fix 7z (2026-07-29) — BLANKET fail-fast rule: recursive traversal is
restricted to /tmp/ ONLY on the cluster.

This test exercises the blanket check in `agent_migrate.py`'s
`run_on_cluster` handler. The check applies AFTER the specific
`/Workspace|/aidp|/Volumes` rules, so it catches everything else —
`/mnt/`, `/data/`, `/opt/`, and any future mount AIDP adds — without
needing a code change.

The check reproduces the exact production logic (structural mirror pattern):
if the source is edited, this test must be kept in sync (guarded by
`test_source_has_blanket_rule` at the bottom).
"""
from __future__ import annotations

import os
import re
import unittest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGENT_MIGRATE = os.path.join(REPO, "scripts", "agent_migrate.py")


def _check_blanket_rule(code: str) -> str:
    """Reproduce ONLY the blanket rule from agent_migrate.py (not the specific
    /Workspace|/aidp|/Volumes rules which would fire first). We simulate the
    check AFTER the specific ones have already returned no match.
    """
    _active_code = "\n".join(
        ln for ln in (code or "").splitlines()
        if ln.strip() and not ln.strip().lstrip().startswith("#")
    )
    _RECURSIVE_PRIMITIVE_RE = re.compile(
        r'(glob\.glob\s*\([^)]*recursive\s*=\s*True'
        r'|os\.walk\s*\('
        r'|\.rglob\s*\('
        r'|\.glob\s*\(\s*["\'][^"\']*\*\*'
        r'|\bgrep\b[^\n]*?-[rR]'
        r'|\bfind\b(?!\s*\()'   # shell find; excludes Python str.find(...) calls
        r'|\bls\b[^\n]*?-R'
        r')'
    )
    _ABS_PATH_NOT_TMP_RE = re.compile(r'''["']/(?!tmp[/"'])[a-zA-Z][^"']*''')
    if _RECURSIVE_PRIMITIVE_RE.search(_active_code) and \
       _ABS_PATH_NOT_TMP_RE.search(_active_code):
        return "BLOCKED"
    return "ALLOWED"


class TestBlanketBlockedOnFutureMounts(unittest.TestCase):
    """Positive: recursive traversal on mounts NOT explicitly enumerated
    should still be blocked by the blanket rule."""

    def test_recursive_glob_on_mnt_blocked(self):
        code = "import glob\nglob.glob('/mnt/foo/**/*.log', recursive=True)"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_os_walk_on_data_blocked(self):
        code = "import os\nfor r,_,_ in os.walk('/data/customer'): pass"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_rglob_on_opt_blocked(self):
        code = "from pathlib import Path\nlist(Path('/opt/tools').rglob('*.py'))"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_pathlib_glob_star_star_on_etc_blocked(self):
        code = "from pathlib import Path\nlist(Path('/etc').glob('**/*.conf'))"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_grep_r_on_home_blocked(self):
        code = "subprocess.run(['grep', '-r', 'x', '/home/user'])"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_find_on_var_blocked(self):
        code = "subprocess.run(['find', '/var/log', '-name', '*.log'])"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_ls_recursive_on_usr_blocked(self):
        code = "subprocess.run(['ls', '-R', '/usr/local'])"
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")


class TestBlanketAllowsTmp(unittest.TestCase):
    """Negative: /tmp/ recursive traversal should be ALLOWED (UUID-unique
    subfolder pattern needs discovery)."""

    def test_glob_recursive_on_tmp_allowed(self):
        code = "import glob\nglob.glob('/tmp/**/*.txt', recursive=True)"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_os_walk_on_tmp_allowed(self):
        code = "import os\nfor r,_,_ in os.walk('/tmp/uuid-abc-123'): pass"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_rglob_on_tmp_allowed(self):
        code = "from pathlib import Path\nlist(Path('/tmp').rglob('*'))"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_find_on_tmp_allowed(self):
        code = "subprocess.run(['find', '/tmp/workdir', '-name', '*.pkl'])"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")


class TestBlanketAllowsNonTraversal(unittest.TestCase):
    """Negative: single-path checks and file reads should never be blocked
    regardless of path — they're bounded operations."""

    def test_os_path_exists_on_mnt_allowed(self):
        code = "os.path.exists('/mnt/foo/bar.txt')"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_open_read_on_data_allowed(self):
        code = "with open('/data/report.csv') as f: r = f.read()"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_os_listdir_single_dir_allowed(self):
        code = "os.listdir('/opt/tools')"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_shutil_copy_allowed(self):
        code = "shutil.copy('/tmp/a', '/mnt/b')"
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")


class TestBlanketMixedPaths(unittest.TestCase):
    """When code has BOTH a /tmp/ AND a non-/tmp/ path, block should fire
    (safety-first — any recursive walk that touches a FUSE mount is
    dangerous, even if it also touches /tmp/)."""

    def test_walk_mixed_tmp_and_mnt_blocks(self):
        code = "for d in ['/tmp/x', '/mnt/y']:\n    for r,_,_ in os.walk(d): pass"
        # Even though /tmp is present, /mnt/y triggers the block. Safety wins.
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")


class TestSourceHasBlanketRule(unittest.TestCase):
    """Structural guard: the source file must still contain the blanket
    rule regex + block message. If a refactor removes it, this test fails
    and the reviewer must justify why."""

    @classmethod
    def setUpClass(cls):
        cls.src = open(AGENT_MIGRATE).read()

    def test_blanket_regex_declared(self):
        # Local pattern strings inside the run_on_cluster handler. Names use
        # snake_case (pat) since they're not module-level compiled regexes —
        # Python's re module caches compilation internally.
        self.assertIn("_recursive_primitive_pat", self.src)
        self.assertIn("_abs_path_not_tmp_pat", self.src)

    def test_blanket_block_message_present(self):
        self.assertIn("recursive traversal restricted to /tmp/", self.src)

    def test_blanket_appears_after_specific_rules(self):
        """The blanket rule must appear AFTER the specific Workspace|aidp|Volumes
        rules — this ordering preserves better error messages for known mounts."""
        specific_pos = self.src.find("BLOCKED: `glob.glob('/Workspace|/aidp|/Volumes/**'")
        blanket_pos = self.src.find("recursive traversal restricted to /tmp/")
        self.assertGreater(specific_pos, 0, "specific /Workspace|/aidp|/Volumes rule not found")
        self.assertGreater(blanket_pos, specific_pos,
                           "blanket rule must appear AFTER specific rules for better error messages")


class TestFindMethodCallExcluded(unittest.TestCase):
    """PR-36 review fix: `\\bfind\\b` must NOT match Python's str.find(...) —
    a bounded string operation. Before the (?!\\s*\\() guard, ANY diagnostic
    using str.find() plus an absolute path literal was falsely BLOCKED, and
    the refusal still consumed one MAX_RUN_ON_CLUSTER_PER_CALL budget slot."""

    def test_str_find_method_with_abs_path_allowed(self):
        code = (
            'with open("/Volumes/default/default/dbfs/config.ini") as f:\n'
            '    text = f.read()\n'
            'pos = text.find("threshold")\n'
        )
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_str_find_spaced_call_allowed(self):
        # `find (` with whitespace before the paren is still call syntax
        code = 'idx = name.find ("x")\nprint("/data/out.csv")'
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_shell_find_in_subprocess_list_still_blocked(self):
        code = 'import subprocess\nsubprocess.run(["find", "/Volumes/default", "-name", "*.ini"])'
        self.assertEqual(_check_blanket_rule(code), "BLOCKED")

    def test_known_gap_midstring_path_not_caught(self):
        """KNOWN GAP (pre-existing, unchanged by the find-guard fix): the
        abs-path detector requires the path literal to start immediately
        after a quote (`"/data/..."`). A path embedded mid-string —
        `os.system("find /data ...")` — is not quote-adjacent, so the
        blanket rule does not fire. Same class as the documented
        variable-driven-path gap in the PR body. This test pins the current
        behavior; if the detector is ever widened, flip the expectation."""
        # Security note: the os.system(...) below is a STRING FIXTURE fed to a
        # regex classifier — it is never executed. It exists precisely because
        # os.system is the risky pattern the production rule inspects.
        code = 'import os\nos.system("find /data -name config.ini")'
        self.assertEqual(_check_blanket_rule(code), "ALLOWED")

    def test_production_regex_has_the_guard(self):
        """Structural: the production pattern carries the (?!\\s*\\() guard at
        BOTH find sites (the FUSE-gated rule and the blanket rule)."""
        with open(AGENT_MIGRATE, encoding="utf-8") as f:
            src = f.read()
        self.assertEqual(src.count(r"\bfind\b(?!\s*\()"), 2,
                         "expected the str.find() exclusion guard at both find-rule sites")


class TestCapsEnvOverridable(unittest.TestCase):
    """PR-36 review fix: the Fix-7z caps must be overridable per-run via env
    vars so a hard notebook can get deeper investigation without a code edit."""

    def test_defaults_and_env_override(self):
        import subprocess, sys, json
        probe = (
            "import sys, json; sys.path.insert(0, r'%s');"
            "import agent_migrate as a;"
            "print(json.dumps([a.MAX_FIX_ATTEMPTS, a.MAX_TOOL_ROUNDS_FIX,"
            " a.MAX_TOOL_ROUNDS_MIGRATE, a.MAX_RUN_ON_CLUSTER_PER_CALL]))"
            % os.path.join(REPO, "scripts")
        )
        env = dict(os.environ, PYTHONUTF8="1")
        out = subprocess.run([sys.executable, "-c", probe], env=env,
                             capture_output=True, text=True)
        self.assertEqual(json.loads(out.stdout.strip().splitlines()[-1]), [5, 5, 5, 3])

        env2 = dict(env, AIDP_MAX_FIX_ATTEMPTS="10", AIDP_MAX_TOOL_ROUNDS_FIX="25",
                    AIDP_MAX_TOOL_ROUNDS_MIGRATE="10", AIDP_MAX_RUN_ON_CLUSTER_PER_CALL="6")
        out2 = subprocess.run([sys.executable, "-c", probe], env=env2,
                              capture_output=True, text=True)
        self.assertEqual(json.loads(out2.stdout.strip().splitlines()[-1]), [10, 25, 10, 6])


if __name__ == "__main__":
    unittest.main()

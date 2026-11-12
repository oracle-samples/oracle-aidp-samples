import io
import os
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from fabric_aidp._env import load_dotenv


class DotenvTests(unittest.TestCase):
    def _load(self, body):
        with TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=True):
            path = Path(tmp) / ".env"
            path.write_text(body, encoding="utf-8")
            load_dotenv(path)
            return dict(os.environ)

    def test_the_tools_own_keys_are_loaded(self):
        env = self._load("OCI_NAMESPACE=ns\nAIDP_WORKSPACE_KEY='ws'\n")
        self.assertEqual((env["OCI_NAMESPACE"], env["AIDP_WORKSPACE_KEY"]), ("ns", "ws"))

    def test_other_keys_are_ignored(self):
        # NODE_OPTIONS=--require ./x.js would run code inside the M parser.
        env = self._load("NODE_OPTIONS=--require ./x.js\nPATH=/tmp\nOCI_NAMESPACE=ns\n")
        self.assertNotIn("NODE_OPTIONS", env)
        self.assertNotIn("PATH", env)

    def test_a_bom_does_not_eat_the_first_key(self):
        with TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=True):
            path = Path(tmp) / ".env"
            path.write_bytes(b"\xef\xbb\xbfOCI_NAMESPACE=ns\n")
            load_dotenv(path)
            self.assertEqual(os.environ.get("OCI_NAMESPACE"), "ns")


class UntrustedDotenvTests(unittest.TestCase):
    """A .env is read from wherever the CLI runs -- an export someone sent,
    a shared work root -- so it is untrusted input. Flagged, unfixed, in the
    oracle-samples security review (#146): the loader took any AIDP_/OCI_/
    FABRIC_ key, and those values also reach the `aidp` CLI `publish` runs."""

    def _load(self, body, *, mode=0o600, shell=None):
        with TemporaryDirectory() as tmp, mock.patch.dict(os.environ, shell or {}, clear=True):
            path = Path(tmp) / ".env"
            path.write_text(body, encoding="utf-8")
            path.chmod(mode)
            with redirect_stderr(io.StringIO()) as err:
                load_dotenv(path)
            return dict(os.environ), err.getvalue()

    def test_keys_that_redirect_requests_or_credentials_are_not_taken(self):
        env, err = self._load("AIDP_ENDPOINT=https://evil.example\n"
                              "OCI_CONFIG_FILE=/tmp/their-key\n"
                              "AIDP_SECRET_TOKEN=s3cret\nOCI_NAMESPACE=ns\n")
        for key in ("AIDP_ENDPOINT", "OCI_CONFIG_FILE", "AIDP_SECRET_TOKEN"):
            with self.subTest(key=key):
                self.assertNotIn(key, env)
                self.assertIn(key, err)
        self.assertEqual(env["OCI_NAMESPACE"], "ns")

    def test_auth_mode_and_region_come_only_from_the_shell(self):
        """Review of oracle-samples #152: a planted OCI_CLI_AUTH=
        instance_principal on an OCI compute host would make `publish
        --apply` act as the machine's identity, not the user's API key -- the
        same credential swap the allowlist exists to stop. OCI_CLI_REGION is
        milder but the same kind. Both still work exported in the shell."""
        env, err = self._load("OCI_CLI_AUTH=instance_principal\n"
                              "OCI_CLI_REGION=us-ashburn-1\nOCI_NAMESPACE=ns\n")
        for key in ("OCI_CLI_AUTH", "OCI_CLI_REGION"):
            with self.subTest(key=key):
                self.assertNotIn(key, env)
                self.assertIn(key, err)
        self.assertNotIn("instance_principal", err)
        env, _ = self._load("", shell={"OCI_CLI_AUTH": "api_key"})
        self.assertEqual(env["OCI_CLI_AUTH"], "api_key")

    def test_the_warning_names_keys_never_values(self):
        _, err = self._load("AIDP_SECRET_TOKEN=s3cret-value\nAIDP_ENDPOINT=https://evil.example\n")
        self.assertNotIn("s3cret-value", err)
        self.assertNotIn("evil.example", err)

    def test_every_allowed_key_is_taken(self):
        from fabric_aidp._env import ALLOWED_KEYS
        env, err = self._load("".join(f"{k}=v\n" for k in sorted(ALLOWED_KEYS)))
        self.assertEqual({k: env.get(k) for k in ALLOWED_KEYS}, {k: "v" for k in ALLOWED_KEYS})
        self.assertEqual(err, "")

    def test_a_shell_value_is_never_overridden(self):
        env, _ = self._load("OCI_NAMESPACE=from-file\n", shell={"OCI_NAMESPACE": "from-shell"})
        self.assertEqual(env["OCI_NAMESPACE"], "from-shell")

    def test_a_stray_unrelated_key_is_ignored_silently(self):
        env, err = self._load("NODE_OPTIONS=--require ./x.js\nOCI_NAMESPACE=ns\n")
        self.assertNotIn("NODE_OPTIONS", env)
        self.assertEqual(err, "")

    def test_a_file_anyone_can_write_is_not_read_at_all(self):
        """On Windows st_mode carries no ACL bits, so the check is skipped by
        design and the file IS read -- asserted, not skipped, so this test
        says something true on every platform. A private group does not
        excuse world-write."""
        for mode in (0o602, 0o666):
            for private in (True, False):
                with self.subTest(mode=oct(mode), private_group=private), \
                        mock.patch("fabric_aidp._env._group_is_private",
                                   return_value=private):
                    env, err = self._load("OCI_NAMESPACE=ns\n", mode=mode)
                    if os.name == "nt":
                        self.assertEqual(env.get("OCI_NAMESPACE"), "ns")
                        continue
                    self.assertNotIn("OCI_NAMESPACE", env)
                    self.assertIn("anyone can write to it", err)

    def test_group_write_is_refused_only_when_the_group_is_shared(self):
        """Review of oracle-samples #152: Ubuntu and Fedora give each user a
        private group and a umask of 002, so a .env made in an editor is
        -rw-rw-r--. Refusing every group-writable file skipped a normal .env
        there. Group-write is fine when the group holds only the owner and
        refused otherwise."""
        for private in (True, False):
            with self.subTest(private_group=private), \
                    mock.patch("fabric_aidp._env._group_is_private",
                               return_value=private):
                env, err = self._load("OCI_NAMESPACE=ns\n", mode=0o664)
                if os.name == "nt" or private:
                    self.assertEqual(env.get("OCI_NAMESPACE"), "ns")
                    self.assertEqual(err, "")
                    continue
                self.assertNotIn("OCI_NAMESPACE", env)
                self.assertIn("other users in its group can write to it", err)

    def test_the_check_reads_the_open_file_not_the_path_again(self):
        """Review of oracle-samples #152: the check stat()ed the path and the
        read reopened it, so in a directory others can write the file could
        be swapped in between. The check now judges the handle it reads. A
        path stat reporting a safe file must not rescue an unsafe one. On
        Windows there is no permission check at all, so the file is read."""
        if os.name == "nt":
            env, _ = self._load("OCI_NAMESPACE=ns\n", mode=0o666)
            self.assertEqual(env.get("OCI_NAMESPACE"), "ns")
            return
        safe = os.stat_result((0o100600, 0, 0, 1, os.getuid(), os.getgid(),
                               16, 0, 0, 0))
        with mock.patch("pathlib.Path.stat", return_value=safe), \
                mock.patch("os.stat", return_value=safe):
            env, err = self._load("OCI_NAMESPACE=ns\n", mode=0o666)
        self.assertNotIn("OCI_NAMESPACE", env)
        self.assertIn("anyone can write to it", err)

    def test_a_symlinked_dotenv_is_not_followed(self):
        """POSIX only, by design: Windows has no O_NOFOLLOW, and making a
        symlink there needs a privilege most accounts lack."""
        if os.name == "nt":
            self.assertIsNone(getattr(os, "O_NOFOLLOW", None))
            return
        with TemporaryDirectory() as tmp, mock.patch.dict(os.environ, {}, clear=True):
            target = Path(tmp) / "elsewhere"
            target.write_text("OCI_NAMESPACE=ns\n", encoding="utf-8")
            target.chmod(0o600)
            link = Path(tmp) / ".env"
            link.symlink_to(target)
            with redirect_stderr(io.StringIO()) as err:
                load_dotenv(link)
            self.assertNotIn("OCI_NAMESPACE", os.environ)
        self.assertIn("symbolic link", err.getvalue())

    def test_a_file_another_user_owns_is_not_read_at_all(self):
        if not hasattr(os, "getuid"):  # Windows: no owner check, file is read
            env, _ = self._load("OCI_NAMESPACE=ns\n")
            self.assertEqual(env.get("OCI_NAMESPACE"), "ns")
            return
        with mock.patch("os.getuid", return_value=os.getuid() + 1):
            env, err = self._load("OCI_NAMESPACE=ns\n")
        self.assertNotIn("OCI_NAMESPACE", env)
        self.assertIn("owned by another user", err)

    def test_a_readable_private_file_is_read(self):
        env, err = self._load("OCI_NAMESPACE=ns\n", mode=0o644)
        self.assertEqual(env.get("OCI_NAMESPACE"), "ns")
        self.assertEqual(err, "")


class PrivateGroupTests(unittest.TestCase):
    """`_group_is_private` asks: is the owner the only member of the file's
    group, counting both the group's member list and every account whose
    primary group it is?

    The group database is replaced with fake `grp`/`pwd` modules, so these
    run the same on every platform, Windows included, where the real
    modules do not exist."""

    def _ask(self, members, primaries, *, known=True):
        from types import SimpleNamespace
        from fabric_aidp._env import _group_is_private
        users = [SimpleNamespace(pw_name=name, pw_uid=1000 + i, pw_gid=gid)
                 for i, (name, gid) in enumerate(primaries)]

        def getgrgid(gid):
            if not known:
                raise KeyError(gid)
            return SimpleNamespace(gr_name="g", gr_gid=gid, gr_mem=members)

        fake_grp = SimpleNamespace(getgrgid=getgrgid)
        fake_pwd = SimpleNamespace(
            getpwuid=lambda uid: next(u for u in users if u.pw_uid == uid),
            getpwall=lambda: users)
        with mock.patch.dict("sys.modules", {"grp": fake_grp, "pwd": fake_pwd}):
            return _group_is_private(500, users[0].pw_uid)

    def test_a_user_private_group_counts(self):
        # Ubuntu/Fedora: group "alice", no listed members, alice's primary.
        self.assertTrue(self._ask([], [("alice", 500), ("bob", 501)]))

    def test_the_owner_listed_alone_counts(self):
        self.assertTrue(self._ask(["alice"], [("alice", 20), ("bob", 501)]))

    def test_another_listed_member_does_not(self):
        self.assertFalse(self._ask(["alice", "bob"], [("alice", 500), ("bob", 501)]))

    def test_another_account_with_it_as_primary_does_not(self):
        # macOS "staff": everyone's primary group.
        self.assertFalse(self._ask([], [("alice", 500), ("bob", 500)]))

    def test_a_group_nobody_has_as_primary_or_member_does_not(self):
        self.assertFalse(self._ask([], [("alice", 20), ("bob", 501)]))

    def test_an_unknown_group_does_not(self):
        self.assertFalse(self._ask([], [("alice", 500)], known=False))

    def test_without_posix_groups_nothing_counts_as_private(self):
        from fabric_aidp._env import _group_is_private
        with mock.patch.dict("sys.modules", {"grp": None, "pwd": None}):
            self.assertFalse(_group_is_private(0, 0))

if __name__ == "__main__":
    unittest.main()

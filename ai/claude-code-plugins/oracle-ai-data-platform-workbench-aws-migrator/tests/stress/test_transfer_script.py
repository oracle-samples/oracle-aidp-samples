"""The generated S3 → OCI transfer script is executable code and must be safe.

Three things are pinned here, end to end through `migrate` and `verify`:

- a bucket name carrying shell syntax is refused before any script exists, so
  the asset lands as `error` → FAIL and no `.transfer.sh` is written;
- a valid transfer still produces a reviewable script whose temp rclone config
  is removed by an EXIT trap, including when rclone fails or the job is
  interrupted (exercised under a real bash when one is available);
- the verify summary keeps saying that PASS is not execution-verified;
- the file `migrate` actually writes has LF endings on every platform, so the
  syntax and shellcheck checks run against that artifact and not only against
  a rendering the test wrote itself.

`shellcheck` is run when it is installed and skipped otherwise.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from aws_aidp.migrate import migrate
from aws_aidp.translate.s3_to_oci import COMPARTMENT_OCID_PLACEHOLDER, _render_script
from aws_aidp.verify import verify
from aws_aidp.verify.checker import format_verify
from tests.stress.helpers import plan_with

HOSTILE_BUCKETS = (
    "acme$(touch pwned)",
    "acme`touch pwned`",
    "acme'; rm -rf / #",
    'acme" ; rm -rf / #',
    "acme;rm -rf /",
    "acme data",
    "acme\nRCLONE\nrm -rf /\n",
)


def s3_asset(*, name: str = "acme-raw-data", region: str = "us-east-1",
             namespace: str | None = None) -> dict:
    target = {"type": "oci_bucket", "name": name}
    if namespace is not None:
        target["namespace"] = namespace
    return {
        "id": f"s3.bucket.{name}",
        "source": {"type": "s3_bucket", "name": name, "region": region},
        "target": target,
        "transform_chain": ["copy_s3_to_oci"],
    }


def _bash() -> str | None:
    """A bash that can run the generated script, or None.

    On Windows the WSL launcher in System32 is also called bash.exe; it is not
    usable for this and is skipped in favour of Git Bash when present.
    """
    found = shutil.which("bash")
    if found and os.name == "nt" and "system32" in found.lower():
        return None
    return found


def _migrate_s3(plan: dict, out: Path) -> dict:
    return migrate(plan, out_dir=out, filter_kind="s3", demo=True)


def _migrated_script(directory: Path) -> Path:
    """Run `migrate` for the default bucket and return the script it wrote."""
    out = directory / "out"
    (row,) = _migrate_s3(plan_with(s3_asset()), out)["results"]
    return out / row["output_path"]


class RefusedTransferTests(unittest.TestCase):
    def test_hostile_bucket_name_is_refused_and_no_script_is_written(self):
        for name in HOSTILE_BUCKETS:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "out"
                report = _migrate_s3(plan_with(s3_asset(name=name)), out)
                (row,) = report["results"]
                self.assertEqual(row["status"], "error")
                self.assertIn("refusing to generate transfer script", row["error"])
                self.assertFalse((out / "transfer").exists())
                self.assertEqual(report["counts"]["error"], 1)
                self.assertFalse(any(p.suffix == ".sh" for p in out.rglob("*")))

    def test_hostile_namespace_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            plan = plan_with(s3_asset(), namespace="ns\nRCLONE\n[evil]")
            (row,) = _migrate_s3(plan, out)["results"]
            self.assertEqual(row["status"], "error")
            self.assertIn("namespace", row["error"])
            self.assertFalse((out / "transfer").exists())

    def test_refused_transfer_verifies_as_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            _migrate_s3(plan_with(s3_asset(name="acme;rm -rf /")), out)
            result = verify(out / "report.json", filter_kind="s3")
            self.assertEqual(result["summary"]["FAIL"], 1)
            self.assertEqual(result["rows"][0]["verdict"], "FAIL")


class GeneratedTransferTests(unittest.TestCase):
    def test_valid_bucket_produces_trap_protected_script(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            report = _migrate_s3(plan_with(s3_asset()), out)
            (row,) = report["results"]
            self.assertEqual(row["status"], "needs_manual_review")
            script = (out / row["output_path"]).read_text(encoding="utf-8")
            self.assertIn("trap 'rm -f \"$CONF\"' EXIT", script)
            self.assertIn("awssrc:acme-raw-data", script)
            self.assertIn("namespace = testns", script)

    def test_written_script_has_lf_endings_on_every_platform(self):
        # The renderer emits LF; a default-newline write would turn that into
        # CRLF on Windows and the script would then fail under a Linux bash.
        with tempfile.TemporaryDirectory() as tmp:
            data = _migrated_script(Path(tmp)).read_bytes()
            self.assertNotIn(b"\r", data)
            self.assertTrue(data.startswith(b"#!/usr/bin/env bash\n"), data[:40])

    def test_verify_summary_still_says_pass_is_not_execution_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            _migrate_s3(plan_with(s3_asset()), out)
            result = verify(out / "report.json", filter_kind="s3")
            self.assertEqual(result["rows"][0]["verdict"], "REVIEW")
            text = format_verify(result)
            self.assertIn("not execution-verified", text)
            self.assertIn("Nothing parses or runs the artifact", text)


@unittest.skipUnless(_bash(), "bash is not available")
class BashBehaviourTests(unittest.TestCase):
    """Run the generated script under bash with a stub rclone on PATH."""

    @staticmethod
    def _write_script(directory: Path, **overrides) -> Path:
        values = dict(bucket="acme-raw-data", oci_bucket="acme-raw-data", ns="testns",
                      aws_region="us-east-1", oci_region="us-ashburn-1",
                      oci_profile="DEFAULT", compartment_ocid=COMPARTMENT_OCID_PLACEHOLDER)
        values.update(overrides)
        path = directory / "acme-raw-data.transfer.sh"
        path.write_text(_render_script(**values), encoding="utf-8", newline="\n")
        return path

    @staticmethod
    def _stub_rclone(directory: Path, body: str) -> Path:
        stub_dir = directory / "bin"
        stub_dir.mkdir()
        stub = stub_dir / "rclone"
        stub.write_text("#!/usr/bin/env bash\n" + textwrap.dedent(body), encoding="utf-8",
                        newline="\n")
        stub.chmod(0o755)
        return stub_dir

    def _run(self, directory: Path, script: Path, stub_dir: Path) -> subprocess.CompletedProcess:
        record = directory / "conf-path.txt"
        # The wrapper reads the config path the stub recorded and reports, after
        # the script has exited, whether the trap removed the file.
        wrapper = directory / "wrapper.sh"
        wrapper.write_text(textwrap.dedent("""\
            #!/usr/bin/env bash
            bash "$1"; rc=$?
            conf="$(cat "$RECORD")"
            if [ -e "$conf" ]; then echo "CONF_PRESENT"; else echo "CONF_REMOVED"; fi
            exit "$rc"
            """), encoding="utf-8", newline="\n")
        env = dict(os.environ)
        env["PATH"] = os.pathsep.join([str(stub_dir), env.get("PATH", "")])
        env["RECORD"] = record.as_posix()
        return subprocess.run(
            [_bash(), wrapper.as_posix(), script.as_posix()],
            capture_output=True, text=True, env=env, timeout=60, cwd=str(directory),
        )

    def test_syntax_check_passes_for_generated_and_hostile_renderings(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            # The file migrate() writes comes first: it is the artifact a user
            # actually runs, so its line endings matter as much as its content.
            scripts = [_migrated_script(directory), self._write_script(directory)]
            for index, name in enumerate(HOSTILE_BUCKETS):
                if "\n" in name:
                    continue
                path = directory / f"hostile-{index}.sh"
                path.write_text(
                    _render_script(name, name, "testns", "us-east-1", "us-ashburn-1",
                                   "DEFAULT", COMPARTMENT_OCID_PLACEHOLDER),
                    encoding="utf-8", newline="\n",
                )
                scripts.append(path)
            for path in scripts:
                with self.subTest(script=path.name):
                    proc = subprocess.run([_bash(), "-n", path.as_posix()],
                                          capture_output=True, text=True, timeout=60)
                    self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_shellcheck_passes_when_installed(self):
        shellcheck = shutil.which("shellcheck")
        if not shellcheck:
            self.skipTest("shellcheck is not installed")
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for script in (_migrated_script(directory), self._write_script(directory)):
                with self.subTest(script=script.relative_to(directory).as_posix()):
                    proc = subprocess.run([shellcheck, "-s", "bash", str(script)],
                                          capture_output=True, text=True, timeout=60)
                    self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_trap_removes_config_when_rclone_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            script = self._write_script(directory)
            stub_dir = self._stub_rclone(directory, """\
                # rclone --config <conf> <verb> ...
                printf '%s' "$2" > "$RECORD"
                [ "$3" = "mkdir" ] && exit 0
                echo "simulated rclone failure" >&2
                exit 3
                """)
            proc = self._run(directory, script, stub_dir)
            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("CONF_REMOVED", proc.stdout)
            self.assertNotIn("CONF_PRESENT", proc.stdout)

    def test_trap_removes_config_when_job_is_interrupted(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            script = self._write_script(directory)
            stub_dir = self._stub_rclone(directory, """\
                printf '%s' "$2" > "$RECORD"
                [ "$3" = "mkdir" ] && exit 0
                # Simulate Ctrl-C reaching the job: the script and the running
                # copy both receive SIGINT.
                kill -INT $PPID $$
                sleep 5
                exit 0
                """)
            proc = self._run(directory, script, stub_dir)
            self.assertNotEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            self.assertIn("CONF_REMOVED", proc.stdout)

    def test_hostile_rendering_passes_name_through_as_one_argument(self):
        # Even bypassing validation, the quoted argument reaches rclone intact
        # and nothing in it is executed.
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name = "acme$(touch pwned)`touch pwned2`;touch pwned3"
            script = self._write_script(directory, bucket=name, oci_bucket=name)
            stub_dir = self._stub_rclone(directory, """\
                printf '%s' "$2" > "$RECORD"
                printf '%s\\n' "$@" > "$RECORD.args"
                exit 0
                """)
            proc = self._run(directory, script, stub_dir)
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            args = (directory / "conf-path.txt.args").read_text(encoding="utf-8").splitlines()
            self.assertIn(f"awssrc:{name}", args)
            self.assertIn(f"ocidest:{name}", args)
            for leaked in ("pwned", "pwned2", "pwned3"):
                self.assertFalse((directory / leaked).exists(), leaked)
            self.assertIn("CONF_REMOVED", proc.stdout)


if __name__ == "__main__":
    unittest.main()

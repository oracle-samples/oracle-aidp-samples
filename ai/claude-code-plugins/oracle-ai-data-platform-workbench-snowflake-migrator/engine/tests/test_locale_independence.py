"""The CLI must not depend on the operator's locale.

Every report carries non-ASCII (→ — ·). Before this test existed, every
read_text()/write_text() in the engine used the platform default encoding,
so on Windows (cp1252) `snowmig.py demo` died with UnicodeEncodeError and 34
tests failed, while the same tree passed under PYTHONUTF8=1. This runs the
demo in a subprocess with the most hostile locale the interpreter allows on
every platform: UTF-8 mode off, C-locale coercion off, and ASCII for the
standard streams. Passing means the engine chose its encodings itself.
"""
import os
import pathlib
import subprocess
import sys

ENGINE = pathlib.Path(__file__).resolve().parents[1] / "snowmig.py"


def _hostile_env() -> dict:
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("PYTHONUTF8", "PYTHONIOENCODING", "LC_", "LANG"))}
    env.update({
        "PYTHONUTF8": "0",
        "PYTHONCOERCECLOCALE": "0",
        "PYTHONIOENCODING": "ascii:strict",
        "LC_ALL": "C",
        "LANG": "C",
    })
    return env


def test_demo_runs_under_an_ascii_locale(tmp_path):
    out = tmp_path / "demo"
    proc = subprocess.run(
        [sys.executable, str(ENGINE), "demo", "--out-dir", str(out)],
        capture_output=True, check=False, env=_hostile_env())
    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")
    assert proc.returncode == 0, f"rc={proc.returncode}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}"
    assert "UnicodeEncodeError" not in stderr and "UnicodeDecodeError" not in stderr
    # The artifacts are UTF-8 regardless of locale, and really do carry non-ASCII.
    inventory = (out / "INVENTORY.md").read_bytes()
    inventory.decode("utf-8")
    assert any(b > 0x7F for b in inventory), "the fixture lost its non-ASCII; the test no longer proves anything"
    # And the streams did not fall back to '?' for the whole line either.
    assert "->" in stdout or "→" in stdout


def test_plan_reads_artifacts_written_by_another_locale(tmp_path):
    """Stage N+1 must read what stage N wrote even if the two ran under
    different locales (a Windows laptop writing, a Linux box planning)."""
    out = tmp_path / "run"
    first = subprocess.run(
        [sys.executable, str(ENGINE), "demo", "--out-dir", str(out)],
        capture_output=True, check=False, env=_hostile_env())
    assert first.returncode == 0, first.stderr.decode("utf-8", errors="replace")
    env = dict(os.environ, PYTHONUTF8="1")
    second = subprocess.run(
        [sys.executable, str(ENGINE), "summary", "--out-dir", str(out)],
        capture_output=True, check=False, env=env)
    assert second.returncode == 0, second.stderr.decode("utf-8", errors="replace")


def test_stages_echo_survives_an_ascii_stdout(tmp_path):
    """The rendered board is re-printed line by line after it is written.

    Writing the file with an explicit encoding is not enough: `stages`,
    `deploy` (dry run) and `preflight` echo the same markdown to stdout, and
    a Windows pipe without UTF-8 mode is cp1252, so the first ⚠️ row raised
    UnicodeEncodeError -- caught by main() as a ValueError and reported as
    `error: 'charmap' codec can't encode ...`, exit 1, with STAGES.md
    already on disk. A passing board must reach the operator, not only the
    file."""
    out = tmp_path / "demo"
    demo = subprocess.run(
        [sys.executable, str(ENGINE), "demo", "--out-dir", str(out)],
        capture_output=True, check=False, env=_hostile_env())
    assert demo.returncode == 0, demo.stderr.decode("utf-8", errors="replace")
    proc = subprocess.run(
        [sys.executable, str(ENGINE), "stages", "--out-dir", str(out)],
        capture_output=True, check=False, env=_hostile_env())
    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")
    assert proc.returncode == 0, f"rc={proc.returncode}\n{stderr}"
    assert "charmap" not in stderr and "codec" not in stderr
    # The echoed board, including its flagged rows, reached stdout.
    assert "| Stage |" in stdout
    assert "Needs attention" in stdout

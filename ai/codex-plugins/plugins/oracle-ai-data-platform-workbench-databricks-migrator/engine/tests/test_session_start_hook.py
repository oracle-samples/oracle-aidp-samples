"""Pins for the SessionStart staging hook (hooks/session_start.py).

Upgrade contract: a 0.2.x staging (which has NO manifest — the 0.2.x hook never
wrote one) is pruned against the embedded legacy bundle list, so deleted 0.2.x
modules can never stay importable; deployment-owned files always survive; and
manifest entries can never escape the staging directory.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

HOOK = Path(__file__).resolve().parents[2] / "hooks" / "session_start.py"


def _run(home):
    env = dict(os.environ, HOME=str(home))
    return subprocess.run([sys.executable, str(HOOK)], env=env,
                          capture_output=True, text=True, timeout=120)


def test_legacy_020_staging_is_pruned_without_manifest(tmp_path):
    staged = tmp_path / ".aidp-migrator" / "engine"
    (staged / "scripts").mkdir(parents=True)
    # a 0.2.x-bundle file the 0.3.0 engine no longer ships
    (staged / "setup.py").write_text("# stale 0.2.x")
    (staged / "scripts" / "openai_compat_runner.py").write_text("# not in legacy list")
    # deployment-owned content
    (staged / "aidp_migrator.yaml").write_text("x")
    r = _run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert not (staged / "setup.py").exists(), "stale 0.2.x bundle file must be pruned"
    assert (staged / "aidp_migrator.yaml").exists(), "deployment files must survive"
    assert (staged / "scripts" / "openai_compat_runner.py").exists(), \
        "files outside the legacy list are deployment-owned on a manifest-less staging"
    # the run writes a manifest, so the next prune is manifest-scoped
    assert (tmp_path / ".aidp-migrator" / ".staged_manifest.json").is_file()


def test_malformed_manifest_does_not_block_staging(tmp_path):
    root = tmp_path / ".aidp-migrator"
    root.mkdir()
    (root / ".staged_manifest.json").write_text("null")
    r = _run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert (root / "engine" / "scripts").is_dir(), "staging must proceed despite a bad manifest"


def test_traversal_entries_are_ignored(tmp_path):
    root = tmp_path / ".aidp-migrator"
    (root / "engine").mkdir(parents=True)
    outside = tmp_path / "precious.txt"
    outside.write_text("keep me")
    (root / ".staged_manifest.json").write_text(json.dumps(["../precious.txt", "/etc/passwd"]))
    r = _run(tmp_path)
    assert r.returncode == 0, r.stderr
    assert outside.exists(), "a ..-entry must never delete outside the staging dir"

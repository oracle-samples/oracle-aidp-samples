"""Snapshot test for the v1 registry transcription.

Re-runs `scripts/dev/transcribe_v1_registry.py` in a subprocess and asserts
the output matches `tests/fixtures/v1_registry_snapshot.yaml` byte-for-byte.

CI fails if:

* The v1 registry's blob hash drifts (transcription script aborts).
* The transcription script's output diverges from the committed snapshot
  (drift in the transcription logic itself).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = PLUGIN_ROOT / "scripts" / "dev" / "transcribe_v1_registry.py"
SNAPSHOT = PLUGIN_ROOT / "tests" / "fixtures" / "v1_registry_snapshot.yaml"


def test_transcription_matches_snapshot() -> None:
    """Re-running the transcription produces byte-identical YAML."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT)],
        cwd=PLUGIN_ROOT,
        capture_output=True,
        # The script emits UTF-8 (non-ASCII identifiers like "P1.5ε"); decode
        # as UTF-8 so a non-UTF-8 locale (Windows cp1252) doesn't mangle it.
        encoding="utf-8",
        check=False,
    )
    if result.returncode != 0:
        # v1 blob hash drifted or git is unavailable — surface stderr for triage.
        raise AssertionError(
            f"transcribe_v1_registry.py failed (exit {result.returncode}):\n"
            f"STDERR:\n{result.stderr}"
        )
    expected = SNAPSHOT.read_text(encoding="utf-8")
    assert result.stdout == expected, (
        "Transcription output drifted from committed snapshot. Regenerate:\n"
        f"  python scripts/dev/transcribe_v1_registry.py > tests/fixtures/v1_registry_snapshot.yaml"
    )

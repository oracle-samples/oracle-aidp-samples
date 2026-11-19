"""Regression — extractors submodules must be accessible without mock side-effects.

The orchestrator at ``orchestrator/__init__.py:275`` calls
``extractors.bicc.extract_pvo(...)`` via dotted attribute access. Python doesn't
auto-import submodules on parent-package import, so this access only works if
``extractors/__init__.py`` explicitly re-exports them.

Mock-heavy unit tests miss this: ``mock.patch("…extractors.bicc.extract_pvo")``
does ``import …extractors.bicc`` to resolve the patch target, which imports the
submodule as a side effect — masking the bug. Live TC26 (run 1095b5b3) caught it.

The regression test runs in a subprocess to guarantee a fresh interpreter with
no prior imports.
"""
from __future__ import annotations

import subprocess
import sys


def test_extractors_submodules_importable_without_mocks() -> None:
    code = (
        "from oracle_ai_data_platform_fusion_autopilot import extractors\n"
        "assert hasattr(extractors, 'bicc'), 'extractors.bicc not auto-imported'\n"
        "assert hasattr(extractors, 'rest'), 'extractors.rest not auto-imported'\n"
        "assert hasattr(extractors, 'saas_batch_rest'), 'extractors.saas_batch_rest not auto-imported'\n"
        "from oracle_ai_data_platform_fusion_autopilot.extractors import bicc\n"
        "assert callable(bicc.extract_pvo), 'bicc.extract_pvo not callable'\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"extractors submodules not importable: {result.stderr}"
    )

"""Pack-level report rendering helper for industry E2E packs.

Each pack produces exactly one ``report.html`` per run via a pack-scoped
autouse finalizer (see ``tests/test_e2e_industries/conftest.py``). This
helper:

1. Creates the output file's parent directory (the writer at
   ``qualifire/reporting/html_report.py:241`` calls
   ``Path(output_path).write_text(...)`` with no ``mkdir``, so callers
   must create parents — see plan §5).
2. Calls ``HealthReporter(storage).generate(days=30)``.
3. Writes HTML via ``generate_health_html(report, output_path=...)``.
4. Returns the HTML string so the caller can assert non-empty.

The function is deliberately narrow: no try/except wrapping, no
best-effort fallbacks. A failure here should fail the pack loudly so
the report-generation assertion in the finalizer surfaces the problem.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from qualifire.reporting.health import HealthReporter
from qualifire.reporting.html_report import generate_health_html


def render_report(storage: Any, out_path: str | Path, *, days: int = 30) -> str:
    """Render a pack-level health report to ``out_path`` and return the HTML.

    Caller (typically a pack-scoped autouse finalizer in
    ``tests/test_e2e_industries/conftest.py``) is responsible for
    asserting the returned HTML is non-empty and that ``out_path``
    exists on disk with non-zero size.
    """
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    report = HealthReporter(storage).generate(days=days)
    return generate_health_html(report, output_path=str(path))

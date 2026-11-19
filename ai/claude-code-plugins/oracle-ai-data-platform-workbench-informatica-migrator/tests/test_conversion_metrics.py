"""Conversion metrics, pinned so tuning is distinguishable from noise.

These tests exist for two reasons.

The first is a floor. The rates below are the published numbers; if a change
drops them, that is a regression and this says so rather than letting it
pass unremarked.

The second is the reason the module exists at all. A notebook that reads a
placeholder and writes nothing emits no manual-intervention markers, so a
marker-only metric scores it as a perfect conversion. One notebook in the
bundled corpus does exactly that. ``converted`` -- markers AND wiring -- is
the honest headline, and ``test_a_stub_is_not_counted_as_converted`` is the
assertion that keeps it honest.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from infa2aidp import metrics

ROOT = Path(__file__).resolve().parent.parent
CORPUS = ROOT / "tests" / "fixtures" / "corpus"

# Published in the README. Raise these when the engine improves; never
# lower them to make a change pass.
MIN_ZERO_TOUCH = 27.0
MIN_RUNNABLE = 27.0
MIN_CONVERTED = 18.0
MAX_TOTAL_MARKERS = 13


@pytest.fixture(scope="module")
def corpus_metrics(tmp_path_factory):
    out = tmp_path_factory.mktemp("metrics_out")
    proc = subprocess.run(
        [sys.executable, "-m", "infa2aidp.cli", "migrate",
         "-i", str(CORPUS), "-o", str(out)],
        cwd=ROOT, capture_output=True, text=True, timeout=600,
        env={**__import__("os").environ, "PYTHONPATH": str(ROOT / "engine")},
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    return metrics.measure(str(out))


def test_every_corpus_mapping_produced_a_notebook(corpus_metrics):
    assert corpus_metrics.total == 11


def test_zero_touch_rate_does_not_regress(corpus_metrics):
    assert corpus_metrics.zero_touch_rate >= MIN_ZERO_TOUCH, corpus_metrics.render()


def test_runnable_rate_does_not_regress(corpus_metrics):
    """Reads a real source table and writes a target.

    Necessary for a migration to be real, and independent of markers: a
    notebook can be marker-free and still be wired to nothing.
    """
    assert corpus_metrics.runnable_rate >= MIN_RUNNABLE, corpus_metrics.render()


def test_converted_rate_does_not_regress(corpus_metrics):
    """The honest headline: no markers AND wired end to end."""
    assert corpus_metrics.converted_rate >= MIN_CONVERTED, corpus_metrics.render()


def test_marker_total_does_not_regress(corpus_metrics):
    """The continuous signal.

    Zero-touch is binary per mapping, so fixing four of a mapping's five
    markers moves nothing. This moves, which is what makes it usable for
    steering.
    """
    assert corpus_metrics.total_markers <= MAX_TOTAL_MARKERS, corpus_metrics.render()


def test_a_stub_is_not_counted_as_converted():
    """A marker-free notebook wired to nothing must not score as converted.

    This is the defect the module was written after finding: a notebook
    reading `SELECT 1 AS placeholder` and writing no target emits no
    markers, so a marker-only metric called it a perfect conversion.
    """
    stub = metrics.NotebookMetric(
        name="stub.ipynb", markers=0, reads_source=False, writes_target=False
    )
    assert stub.zero_touch is True
    assert stub.runnable is False
    assert stub.converted is False


def test_wired_but_marked_is_not_counted_as_converted():
    m = metrics.NotebookMetric(
        name="x.ipynb", markers=2, reads_source=True, writes_target=True
    )
    assert m.runnable is True
    assert m.converted is False


def test_render_names_the_stubs(corpus_metrics):
    """The table has to make a stub visible, not just count it."""
    table = corpus_metrics.render()
    assert "STUB" in table or corpus_metrics.runnable_rate == 100.0


def test_metrics_report_is_printed(corpus_metrics, capsys):
    """Print the table so a test run shows the current distribution."""
    with capsys.disabled():
        print("\n" + corpus_metrics.render())

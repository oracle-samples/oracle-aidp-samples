"""Conversion metrics over a directory of generated notebooks.

Why more than one number
========================
The headline metric -- "what fraction of mappings need no human before they
run" -- is binary per mapping, which makes it useless for steering. Fix four
of a mapping's five markers and it does not move, so tuning is
indistinguishable from noise.

Worse, a marker count alone can be gamed by accident. A notebook that reads
a placeholder and writes nothing has no markers to emit, so it scores as a
perfect conversion while being a stub. That is not hypothetical: it is how
this module came to exist.

So three rates are reported, and the honest headline is the third:

``zero_touch``
    No manual-intervention marker. Necessary, not sufficient.
``runnable``
    Reads a real source table and writes a target. Also necessary, also not
    sufficient -- it says the notebook is wired end to end, not that it
    computes the right answer.
``converted``
    Both. The only one worth quoting without a caveat attached.

plus ``markers`` as a distribution, which is the continuous signal: it moves
when a single marker is fixed, so it can tell tuning from noise.

None of these measures correctness. ``source_fidelity`` is the check for
that, and even it compares representation rather than results.
"""
from __future__ import annotations

import glob
import json
import os
import re
import statistics
from dataclasses import dataclass, field

# Markers the generators emit where a human must intervene.
_MARKER = re.compile(r"REVIEW REQUIRED|#\s*TODO|#\s*MANUAL")
# A real read: a catalog-addressed table, not the placeholder the generator
# emits when the export carries no source instance metadata.
_REAL_READ = re.compile(r"spark\.table\(")
_PLACEHOLDER = re.compile(r"placeholder", re.IGNORECASE)
# A real write, in any of the shapes the write strategies emit.
_REAL_WRITE = re.compile(r"saveAsTable|\.merge\(|insertInto|write_to_adw")


@dataclass
class NotebookMetric:
    name: str
    markers: int
    reads_source: bool
    writes_target: bool

    @property
    def zero_touch(self) -> bool:
        return self.markers == 0

    @property
    def runnable(self) -> bool:
        return self.reads_source and self.writes_target

    @property
    def converted(self) -> bool:
        return self.zero_touch and self.runnable


@dataclass
class CorpusMetrics:
    notebooks: list[NotebookMetric] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.notebooks)

    def _rate(self, attr: str) -> float:
        if not self.notebooks:
            return 0.0
        return 100.0 * sum(getattr(n, attr) for n in self.notebooks) / self.total

    @property
    def zero_touch_rate(self) -> float:
        return self._rate("zero_touch")

    @property
    def runnable_rate(self) -> float:
        return self._rate("runnable")

    @property
    def converted_rate(self) -> float:
        return self._rate("converted")

    @property
    def marker_counts(self) -> list[int]:
        return sorted(n.markers for n in self.notebooks)

    @property
    def total_markers(self) -> int:
        return sum(n.markers for n in self.notebooks)

    @property
    def mean_markers(self) -> float:
        return statistics.mean(self.marker_counts) if self.notebooks else 0.0

    @property
    def median_markers(self) -> float:
        return statistics.median(self.marker_counts) if self.notebooks else 0.0

    @property
    def max_markers(self) -> int:
        return max(self.marker_counts) if self.notebooks else 0

    def render(self) -> str:
        """A table for a human, not a log line."""
        if not self.notebooks:
            return "No notebooks found."
        w = max(len(n.name) for n in self.notebooks)
        lines = [
            "Conversion metrics",
            "==================",
            "",
            f"{'notebook':{w}}  {'markers':>7}  {'reads':>6}  {'writes':>6}  verdict",
            f"{'-' * w}  {'-' * 7}  {'-' * 6}  {'-' * 6}  {'-' * 9}",
        ]
        for n in sorted(self.notebooks, key=lambda x: (-x.markers, x.name)):
            verdict = "converted" if n.converted else (
                "STUB" if n.zero_touch and not n.runnable else "needs work"
            )
            lines.append(
                f"{n.name:{w}}  {n.markers:>7}  "
                f"{'yes' if n.reads_source else 'NO':>6}  "
                f"{'yes' if n.writes_target else 'NO':>6}  {verdict}"
            )
        lines += [
            "",
            f"zero-touch (no markers)        : {self._c('zero_touch')}/{self.total}"
            f" = {self.zero_touch_rate:.0f}%",
            f"runnable (real read AND write) : {self._c('runnable')}/{self.total}"
            f" = {self.runnable_rate:.0f}%",
            f"CONVERTED (both)               : {self._c('converted')}/{self.total}"
            f" = {self.converted_rate:.0f}%",
            "",
            f"markers: {self.total_markers} total, mean {self.mean_markers:.1f}, "
            f"median {self.median_markers:.1f}, max {self.max_markers}",
            f"distribution: {self.marker_counts}",
        ]
        return "\n".join(lines)

    def _c(self, attr: str) -> int:
        return sum(getattr(n, attr) for n in self.notebooks)


def _notebook_code(path: str) -> str:
    if path.endswith(".ipynb"):
        nb = json.load(open(path, encoding="utf-8"))
        return "\n".join("".join(c.get("source", [])) for c in nb.get("cells", []))
    return open(path, encoding="utf-8").read()


def measure(output_dir: str) -> CorpusMetrics:
    """Measure every generated notebook under *output_dir*."""
    metrics = CorpusMetrics()
    paths = sorted(
        glob.glob(os.path.join(output_dir, "**", "*.ipynb"), recursive=True)
    ) or sorted(glob.glob(os.path.join(output_dir, "**", "nb_*.py"), recursive=True))

    for p in paths:
        code = _notebook_code(p)
        metrics.notebooks.append(
            NotebookMetric(
                name=os.path.basename(p),
                markers=len(_MARKER.findall(code)),
                # A placeholder read is the generator saying "the export gave
                # me no source instance", so it is explicitly not a real read.
                reads_source=bool(_REAL_READ.search(code))
                and not _PLACEHOLDER.search(code),
                writes_target=bool(_REAL_WRITE.search(code)),
            )
        )
    return metrics

"""Fingerprint + identity validation.

Two checks:
  cell_fingerprint(nb)     — SHA-256 over (cell_type, normalized_source) tuples.
                             Used to compare DBX-exported notebook against the
                             round-tripped AIDP read-back.
  file_sha256(bytes)       — straightforward content hash for jars/init scripts.

Normalization is deliberately minimal: strip trailing whitespace per line,
rstrip the cell. We do NOT collapse blank lines or normalize indentation —
those changes are semantically meaningful in Python.
"""

from __future__ import annotations

import ast
import hashlib
from typing import Any, Dict, List, Tuple


def _norm_source(source) -> str:
    """Notebook 'source' can be a list[str] (with newlines) or a single str."""
    if isinstance(source, list):
        s = "".join(source)
    else:
        s = source or ""
    lines = [line.rstrip() for line in s.split("\n")]
    return "\n".join(lines).rstrip()


def cell_fingerprint(cells: List[Dict[str, Any]]) -> str:
    """SHA-256 over (cell_type, normalized_source) per cell, in order."""
    h = hashlib.sha256()
    for c in cells:
        ct = c.get("cell_type", "")
        src = _norm_source(c.get("source"))
        h.update(ct.encode())
        h.update(b"\x00")
        h.update(src.encode())
        h.update(b"\x01")
    return "sha256:" + h.hexdigest()


def file_sha256(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


def cell_diff(a_cells: List[Dict[str, Any]], b_cells: List[Dict[str, Any]], max_report: int = 3) -> List[Dict[str, Any]]:
    """Return up to `max_report` mismatched cells for the report.

    Each entry: {"index", "a_type", "b_type", "a_preview", "b_preview"} — for
    debugging fingerprint failures.
    """
    out = []
    for i, (a, b) in enumerate(zip(a_cells, b_cells)):
        if a.get("cell_type") != b.get("cell_type") or _norm_source(a.get("source")) != _norm_source(b.get("source")):
            out.append({
                "index": i,
                "a_type": a.get("cell_type"),
                "b_type": b.get("cell_type"),
                "a_preview": _norm_source(a.get("source"))[:200],
                "b_preview": _norm_source(b.get("source"))[:200],
            })
            if len(out) >= max_report:
                return out
    # Length mismatch
    if len(a_cells) != len(b_cells):
        out.append({
            "index": min(len(a_cells), len(b_cells)),
            "a_type": "<length>",
            "b_type": "<length>",
            "a_preview": f"len={len(a_cells)}",
            "b_preview": f"len={len(b_cells)}",
        })
    return out


def python_ast_equiv(a_src: str, b_src: str) -> bool:
    """Return True if both sources parse to identical Python ASTs.

    Returns False on any SyntaxError — caller decides whether that counts as
    a mismatch or just a non-Python cell to skip.
    """
    try:
        return ast.dump(ast.parse(a_src)) == ast.dump(ast.parse(b_src))
    except SyntaxError:
        return False


def parse_notebook_bytes(content: bytes) -> Dict[str, Any]:
    """Parse .ipynb bytes; raise ValueError on malformed JSON / wrong shape."""
    import json
    try:
        nb = json.loads(content.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"malformed JSON: {e}")
    except UnicodeDecodeError as e:
        raise ValueError(f"non-utf8 bytes: {e}")
    if not isinstance(nb, dict):
        raise ValueError(f"expected dict, got {type(nb).__name__}")
    if nb.get("nbformat") != 4:
        raise ValueError(f"expected nbformat=4, got {nb.get('nbformat')!r}")
    if not isinstance(nb.get("cells"), list):
        raise ValueError("missing or non-list 'cells'")
    return nb

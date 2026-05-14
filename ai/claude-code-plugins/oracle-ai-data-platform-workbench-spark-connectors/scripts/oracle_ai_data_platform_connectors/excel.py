"""Excel ingestion helper using only the Python standard library.

The two paths the official AIDP sample documents (the Crealytics
``com.crealytics.spark.excel`` JAR and ``pandas.read_excel``+ ``openpyxl``)
both require an install — a cluster JAR or a Python wheel. AIDP clusters
commonly have neither (PyPI is unreachable; Maven Central is, but the
Crealytics dependency closure is large).

This module is a third path: parse the .xlsx with stdlib ``zipfile`` +
``xml.etree.ElementTree``. .xlsx is a ZIP of XML files; sharedStrings.xml +
worksheets/sheet1.xml together carry the cell data we need for read-only
ingestion. No extra deps — works on any AIDP cluster regardless of PyPI /
Maven access.

Limitations:
* Read-only. There's no stdlib path to write .xlsx without ``openpyxl`` or
  similar; if you need to write Excel, install ``openpyxl`` or use one of
  the JAR-based options.
* Reads the FIRST sheet only (``xl/worksheets/sheet1.xml``). For multi-sheet
  workbooks, list the sheets first and pass the right path.
* Cell type coercion is "best effort" — numeric cells are int or float;
  shared-string cells are strings; booleans are converted; everything else
  passes through as the raw text.
"""

from __future__ import annotations

import zipfile
import xml.etree.ElementTree as ET
from typing import Any, List, Optional


_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def _parse_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    """Return the workbook's shared-strings table, or [] if absent."""
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    out: List[str] = []
    for si in root.iter(f"{_NS}si"):
        # Each <si> may have one <t> directly or several <r><t> runs;
        # concatenate all <t> nodes in document order.
        out.append("".join((t.text or "") for t in si.iter(f"{_NS}t")))
    return out


def _coerce_cell(text: Optional[str]) -> Any:
    if text is None:
        return None
    if "." in text:
        try:
            return float(text)
        except ValueError:
            return text
    try:
        return int(text)
    except ValueError:
        return text


def read_xlsx_stdlib(
    path: str,
    *,
    sheet_path: str = "xl/worksheets/sheet1.xml",
) -> List[List[Any]]:
    """Read a small .xlsx file using only the Python standard library.

    Args:
        path: Filesystem path to the .xlsx (Volume-mounted or ``/tmp/...``).
        sheet_path: Internal path of the sheet inside the .xlsx zip.
            Default is the first sheet. For other sheets, look at the
            workbook's ``xl/workbook.xml`` to find the right name.

    Returns:
        List of rows, each row is a list of cell values. The first row is
        commonly the header; callers typically pop it.

    Example:
        >>> rows = read_xlsx_stdlib("/Volumes/.../data.xlsx")
        >>> header, *body = rows
        >>> df = spark.createDataFrame(body, schema=header)
    """
    with zipfile.ZipFile(path) as z:
        strings = _parse_shared_strings(z)
        if sheet_path not in z.namelist():
            raise FileNotFoundError(
                f"Sheet not found in xlsx: {sheet_path}. "
                f"Available: {[n for n in z.namelist() if n.startswith('xl/worksheets/')]}"
            )
        sheet = ET.fromstring(z.read(sheet_path))

    rows: List[List[Any]] = []
    for r in sheet.iter(f"{_NS}row"):
        row: List[Any] = []
        for c in r.findall(f"{_NS}c"):
            t = c.get("t", "n")
            v = c.find(f"{_NS}v")
            if v is None or v.text is None:
                row.append(None)
                continue
            if t == "s":
                row.append(strings[int(v.text)])
            elif t == "b":
                row.append(bool(int(v.text)))
            elif t == "inlineStr":
                # Inline string — embedded <is><t>...</t></is>
                is_node = c.find(f"{_NS}is")
                if is_node is not None:
                    row.append(
                        "".join((tn.text or "") for tn in is_node.iter(f"{_NS}t"))
                    )
                else:
                    row.append(_coerce_cell(v.text))
            else:
                row.append(_coerce_cell(v.text))
        rows.append(row)
    return rows

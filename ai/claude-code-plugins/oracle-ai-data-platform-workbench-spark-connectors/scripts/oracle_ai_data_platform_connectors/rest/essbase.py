"""Essbase 21c REST + MDX helpers.

Essbase is REST-only over HTTP Basic. The MDX endpoint accepts a query and
returns a tabular result that maps cleanly onto a Spark DataFrame.
"""

from __future__ import annotations

from typing import Any, Optional


def execute_mdx(
    session: Any,
    base_url: str,
    application: str,
    cube: str,
    mdx_query: str,
    *,
    timeout: int = 120,
) -> dict:
    """Execute an MDX query against an Essbase cube.

    Args:
        session: ``requests.Session`` with HTTP Basic auth.
        base_url: Essbase REST base, e.g.
            ``https://<host>:9000/essbase/rest/v1``.
        application: Application name (e.g. ``"Sample"``).
        cube: Cube/database name (e.g. ``"Basic"``).
        mdx_query: The MDX SELECT statement.
        timeout: Per-request timeout (seconds).

    Returns:
        Parsed JSON response containing axes and cells.
    """
    base_url = base_url.rstrip("/")
    url = (
        f"{base_url}/applications/{application}/databases/{cube}/data"
    )
    body = {"query": mdx_query, "queryType": "MDX"}
    response = session.post(url, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()


def mdx_result_to_spark_dataframe(spark: Any, mdx_response: dict):
    """Convert an Essbase MDX response into a Spark DataFrame.

    Essbase returns axes (rows / columns / pages / pov) plus cell values.
    The helper flattens to one row per cell with one column per axis dimension.
    """
    import pandas as pd

    axes = mdx_response.get("axes", [])
    cells = mdx_response.get("cells", [])

    # Axes order (per Essbase REST): [ROWS, COLUMNS, PAGES, POV].
    rows_axis = axes[0] if len(axes) > 0 else {"tuples": []}
    cols_axis = axes[1] if len(axes) > 1 else {"tuples": []}

    row_tuples = rows_axis.get("tuples", [])
    col_tuples = cols_axis.get("tuples", [])

    # cells is a flat list, ordered (row * n_cols) + col.
    n_cols = max(len(col_tuples), 1)
    out: list = []
    for idx, cell in enumerate(cells):
        row_idx = idx // n_cols
        col_idx = idx % n_cols
        record: dict = {"value": cell.get("value")}
        if row_idx < len(row_tuples):
            for i, member in enumerate(row_tuples[row_idx].get("members", [])):
                record[f"row_{i}"] = member.get("name")
        if col_idx < len(col_tuples):
            for i, member in enumerate(col_tuples[col_idx].get("members", [])):
                record[f"column_{i}"] = member.get("name")
        out.append(record)

    if not out:
        return spark.createDataFrame([], "value STRING")
    return spark.createDataFrame(pd.DataFrame(out))

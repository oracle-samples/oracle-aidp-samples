"""Oracle EPM Cloud Planning REST helpers.

Exports MDX-style data slices via the documented
``/HyperionPlanning/rest/v3/applications/{app}/plantypes/{planType}/exportdataslice``
endpoint and turns the result into a Spark DataFrame.

v0.1 ships HTTP Basic only (default for current EPM Cloud, works today).
v0.2 adds OAuth via JWT client-credentials (helper already in
``oracle_ai_data_platform_connectors.auth.user_principal.oauth_token``).
"""

from __future__ import annotations

from typing import Any, List, Optional


def list_applications(session: Any, base_url: str) -> List[dict]:
    """Sanity-check call: lists Planning applications visible to the caller.

    Use this as a pre-flight to detect 401s early without paying for a full
    MDX export. Returns the ``items`` list from the response.
    """
    base_url = base_url.rstrip("/")
    response = session.get(
        f"{base_url}/HyperionPlanning/rest/v3/applications",
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("items", [])


def export_data_slice(
    session: Any,
    base_url: str,
    application: str,
    plan_type: str,
    grid_definition: dict,
    *,
    timeout: int = 120,
) -> dict:
    """Run a Planning data-slice export.

    Args:
        session: ``requests.Session`` with Basic auth. The username MUST be
            in identity-domain form: ``tenancy.user@domain``
            (e.g. ``epmloaner622.first.last@oracle.com``).
        base_url: EPM pod base URL
            (``https://epm-<id>.epm.<region>.ocs.oraclecloud.com``).
        application: Planning application name (e.g. ``"Vision"``).
        plan_type: Plan type within the app (e.g. ``"Plan1"``).
        grid_definition: A dict matching the ``gridDefinition`` shape:
            ``{"suppressMissingBlocks": True, "pov": {...}, "columns": [...], "rows": [...]}``.
            POV members must be **leaf-level** — use ``IChildren(parent)`` etc.
            to expand parents server-side.
        timeout: Per-request timeout in seconds. EPM exports can be slow for
            large slices; 120s is conservative.

    Returns:
        The parsed JSON response (``{"pov": [...], "columns": [...], "rows": [...]}``).
    """
    base_url = base_url.rstrip("/")
    url = (
        f"{base_url}/HyperionPlanning/rest/v3/"
        f"applications/{application}/plantypes/{plan_type}/exportdataslice"
    )
    body = {
        "exportPlanningData": False,
        "gridDefinition": grid_definition,
    }
    response = session.post(url, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()


def slice_to_long_dataframe(spark: Any, slice_response: dict):
    """Convert an EPM data-slice response into a long-format Spark DataFrame.

    Output columns: ``pov_<dim>...``, ``column_<dim>...``, ``row_<dim>...``,
    ``value`` (string — the raw EPM value, including ``"#Missing"``).

    Useful when you don't know up-front how many columns/rows the slice
    contains and want a tidy long form to aggregate from.
    """
    import pandas as pd

    pov_dims: list = slice_response.get("pov", [])
    columns_grid: list = slice_response.get("columns", [])
    rows: list = slice_response.get("rows", [])

    out_records: list = []
    # columns_grid is a list of [member, member, ...] one per column.
    # rows is a list of {"headers": [...], "data": [...]}.
    for row in rows:
        row_headers = row.get("headers", [])
        for col_index, value in enumerate(row.get("data", [])):
            col_headers = (
                columns_grid[col_index]
                if col_index < len(columns_grid)
                else []
            )
            record: dict = {"value": value}
            for i, member in enumerate(pov_dims):
                record[f"pov_{i}"] = member
            for i, member in enumerate(col_headers):
                record[f"column_{i}"] = member
            for i, member in enumerate(row_headers):
                record[f"row_{i}"] = member
            out_records.append(record)

    if not out_records:
        return spark.createDataFrame([], "value STRING")
    pdf = pd.DataFrame(out_records)
    return spark.createDataFrame(pdf)

"""Helpers that turn raw system-table rows into the shapes the
notebooks and ad-hoc scripts want to plot or render against.

Two entry points:

* :func:`load_health_dataframe` — coerced pandas DataFrame for
  matplotlib / Plotly charts (typed timestamps, numeric metric_value,
  derived ``day`` and uppercased ``severity`` columns).
* :func:`build_result_from_system_table` — reconstruct a
  :class:`qualifire.core.models.QualifireResult` from a stored run so
  the static HTML report (``generate_html_report``) can render it
  without re-executing the engine.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


def load_health_dataframe(storage: Any, days: int = 30) -> Any:
    """Read the last ``days`` days from ``storage`` and return a
    coerced pandas DataFrame.

    Adds three derived columns the chart helpers expect:

    * ``run_timestamp`` / ``partition_ts`` parsed via ``pd.to_datetime``
    * ``metric_value`` cast to numeric (NaN for unparseable cells)
    * ``day`` — date portion of ``run_timestamp``
    * ``severity`` — uppercase ``validation_status``, default
      ``"UNKNOWN"`` when missing

    Returns an empty DataFrame (still callable on the chart helpers)
    when the system table has no rows in the window.
    """
    import pandas as pd

    rows = storage.read_health_data(days=days)
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["run_timestamp"] = pd.to_datetime(df.get("run_timestamp"), errors="coerce")
    if "partition_ts" in df.columns:
        df["partition_ts"] = pd.to_datetime(df["partition_ts"], errors="coerce")
    else:
        df["partition_ts"] = pd.NaT
    df["metric_value"] = pd.to_numeric(df.get("metric_value"), errors="coerce")
    df["day"] = df["run_timestamp"].dt.date
    # ``severity`` is always a Series (uppercase, no missing values).
    # The else-branch covers the bizarre case of a custom storage whose
    # ``read_health_data`` payload omits ``validation_status`` entirely;
    # we still hand chart helpers a Series so dtype-sensitive operations
    # (.fillna(), .value_counts(), groupby keys) don't trip on a scalar.
    if "validation_status" in df.columns:
        severity = df["validation_status"].fillna("UNKNOWN").astype(str).str.upper()
    else:
        severity = pd.Series(["UNKNOWN"] * len(df), index=df.index, dtype="object")
    df["severity"] = severity
    return df


def build_result_from_system_table(
    storage: Any,
    *,
    run_id: str | None = None,
    days: int = 30,
) -> Any:
    """Reconstruct a ``QualifireResult`` from system-table rows.

    Selects the run identified by ``run_id``; when ``run_id`` is None,
    picks the most recent one (max ``run_timestamp``). Useful when you
    want :func:`qualifire.reporting.html_report.generate_html_report`'s
    per-run HTML view but only have the persisted history available
    (e.g. an analyst opening the dashboard after the engine that
    produced the data has shut down).

    Args:
        storage: Any SystemTableStorage exposing ``read_health_data``.
        run_id: Specific run to reconstruct. When None, the latest one
            in the window is used.
        days: Lookback window when ``run_id`` is None — keeps the
            history scan bounded on large stores.

    Returns:
        A :class:`qualifire.core.models.QualifireResult` carrying
        :class:`~qualifire.core.models.DatasetResult` / ``ValidationResult``
        rows reconstructed from the persisted columns.

    Raises:
        ValueError: storage has no rows in the window (or for the
            given ``run_id``).
    """
    from qualifire.core.models import (
        DatasetResult,
        QualifireResult,
        Severity,
        ValidationResult,
    )

    rows = storage.read_health_data(days=days)
    if not rows:
        raise ValueError(
            f"system table has no rows in the last {days} days — "
            "nothing to reconstruct."
        )

    if run_id is None:
        # Pick the most recent run by run_timestamp. Empty string
        # sorts before any ISO timestamp so rows missing run_timestamp
        # don't crowd out a real winner.
        run_id = max(
            (r.get("run_id") for r in rows if r.get("run_id")),
            key=lambda rid: max(
                (r.get("run_timestamp") or "")
                for r in rows
                if r.get("run_id") == rid
            ),
        )

    run_rows = [r for r in rows if r.get("run_id") == run_id]
    if not run_rows:
        raise ValueError(f"run_id={run_id!r} not found in storage")

    by_ds: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in run_rows:
        by_ds[r.get("dataset_name") or "(unknown)"].append(r)

    datasets: list[DatasetResult] = []
    for ds_name, ds_rows in by_ds.items():
        validations: list[ValidationResult] = []
        # Every backend's ``read_health_data`` already filters
        # ``WHERE record_type = 'validation'`` — we don't have to
        # filter again here. If a future widening of that read
        # contract surfaces collection rows, the right move is a
        # deliberate decision in this function (e.g. emit them as
        # ``CollectionResult``s on ``DatasetResult.collection_results``),
        # not a defensive skip that silently drops them.
        for r in ds_rows:
            try:
                sev = Severity[(r.get("validation_status") or "PASS").upper()]
            except KeyError:
                sev = Severity.PASS
            full_name = r.get("validation_name") or ""
            validations.append(ValidationResult(
                validation_name=full_name,
                validation_type=r.get("validation_type") or "",
                severity=sev,
                message=r.get("validation_message") or "",
                validation_base_name=full_name.split(".", 1)[0] or "unknown",
                metric_name=r.get("metric_name"),
                actual_value=r.get("metric_value"),
                actual_value_text=r.get("actual_value_text"),
                dimension_value=r.get("dimension_value"),
            ))
        first = ds_rows[0]
        ts = first.get("run_timestamp")
        try:
            run_ts = datetime.fromisoformat(ts) if ts else datetime.now()
        except (TypeError, ValueError):
            run_ts = datetime.now()
        # Surface partition_ts from the first row — every row in this
        # group shares a (dataset, run_id) and therefore a partition.
        # Best-effort isoformat parse; malformed / NULL values yield
        # ``None`` and the notifier omits the line silently.
        pts_raw = first.get("partition_ts")
        try:
            partition_ts = (
                datetime.fromisoformat(pts_raw) if pts_raw else None
            )
        except (TypeError, ValueError):
            partition_ts = None
        datasets.append(DatasetResult(
            dataset_name=ds_name,
            table=first.get("table_name"),
            validation_results=validations,
            run_id=run_id,
            run_timestamp=run_ts,
            partition_ts=partition_ts,
        ))

    return QualifireResult(
        owner=run_rows[0].get("owner") or "",
        bu=run_rows[0].get("bu") or "",
        run_id=run_id,
        datasets=datasets,
    )

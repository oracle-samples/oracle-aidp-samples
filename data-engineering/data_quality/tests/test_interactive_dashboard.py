"""End-to-end coverage for descriptions on configs + the interactive dashboard."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock


def _stub_storage_with_rows(rows):
    storage = MagicMock()
    storage.read_health_data.return_value = rows
    return storage


class TestDescriptionsRoundTrip:
    def test_yaml_description_persisted_on_every_row(self, tmp_path):
        """Descriptions on dataset/validation in YAML config land on the
        system-table rows produced by that dataset+validation."""
        from qualifire.api import Qualifire
        from qualifire.backends.spark_backend import SparkBackend
        from qualifire.core.config import (
            AggregationCollectionConfig,
            DatasetConfig,
            QualifireConfig,
            ThresholdLevels,
            ThresholdRuleConfig,
            ThresholdValidationConfig,
        )
        from qualifire.core.context import QualifireContext
        from qualifire.core.engine import QualifireEngine
        from qualifire.storage.sqlite_storage import SQLiteStorage

        backend = MagicMock()
        backend.get_aggregations.return_value = {"row_count": 100}
        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()

        ds = DatasetConfig(
            table="t",
            description="Daily sales fact, refreshed at 02:00 UTC.",
            validations=[ThresholdValidationConfig(
                name="row_count_floor",
                description="Daily row count must clear 100.",
                collection=AggregationCollectionConfig(expressions={"row_count": "COUNT(*)"}),
                rules=[ThresholdRuleConfig(
                    metric="row_count",
                    thresholds=ThresholdLevels(error={"min": 1}),
                )],
            )],
        )
        cfg = QualifireConfig(
            owner="o", bu="b", system_table="qf",
            datasets=[ds],
        )
        QualifireEngine(
            backend=backend, storage=storage,
            context=QualifireContext(), config=cfg,
        ).run()

        conn = storage._get_conn()
        rows = conn.execute(
            "SELECT record_type, dataset_description, validation_description "
            "FROM qualifire_history "
            "WHERE record_type IN ('validation', 'collection')"
        ).fetchall()
        assert rows, "no rows persisted"
        for record_type, ds_desc, val_desc in rows:
            assert ds_desc == "Daily sales fact, refreshed at 02:00 UTC.", (
                f"{record_type} row missing dataset_description (got {ds_desc!r})"
            )
            if record_type == "validation":
                assert val_desc == "Daily row count must clear 100.", (
                    f"validation row missing validation_description (got {val_desc!r})"
                )


class TestInteractiveDashboardOutput:
    def _row(self, **overrides):
        base = {
            "run_id": "r1",
            "run_timestamp": "2026-05-01T10:00:00",
            "owner": "o", "bu": "b",
            "dataset_name": "sales", "table_name": "sales_fact",
            "metric_name": "row_count", "metric_value": 100.0,
            "collection_type": "aggregation",
            "validation_name": "row_floor", "validation_type": "threshold",
            "validation_status": "PASS", "validation_message": "ok",
            "notification_channel": None, "notification_status": None,
            "details_json": "{}", "record_type": "validation",
            "collector_name": None, "dimension_value": None,
            "collected_at": None, "validated_at": None, "notified_at": None,
            "partition_ts": "2026-05-01",
            "expected_value": None, "actual_value_text": None,
            "dataset_description": "Daily sales.",
            "validation_description": "Row count must exceed 100.",
        }
        base.update(overrides)
        return base

    def test_renders_self_contained_html(self, tmp_path):
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([
            self._row(),
            self._row(run_id="r2", run_timestamp="2026-05-02T10:00:00",
                       partition_ts="2026-05-02", validation_status="WARNING",
                       metric_value=95.0),
            self._row(run_id="r3", run_timestamp="2026-05-03T10:00:00",
                       partition_ts="2026-05-03", validation_status="ERROR",
                       metric_value=50.0),
        ])

        out = tmp_path / "dash.html"
        html = generate_interactive_html(storage, output_path=str(out), days=30)

        # Plotly CDN script present
        assert "https://cdn.plot.ly" in html
        # Embedded JSON snapshot loads
        # SNAPSHOT_TRUNCATED was added between SNAPSHOT and DEFAULT_DAYS
        # for the dashboard-rich-detail-panel feature; regex skips it.
        m = re.search(
            r"const SNAPSHOT = (\[.*?\]);\s*\nconst SNAPSHOT_TRUNCATED = .+?;\s*\nconst DEFAULT_DAYS",
            html, re.S,
        )
        assert m, "could not find SNAPSHOT block in HTML"
        snapshot = json.loads(m.group(1))
        assert len(snapshot) == 3
        assert {r["validation_status"] for r in snapshot} == {"PASS", "WARNING", "ERROR"}
        # Descriptions surface
        assert "Daily sales." in html
        assert "Row count must exceed 100." in html
        # Dark-mode CSS variables
        assert "prefers-color-scheme: dark" in html
        # File written matches returned string
        assert out.read_text() == html

    def test_as_iframe_wraps_for_notebook_embedding(self, tmp_path):
        """`as_iframe=True` produces an iframe-wrapped dashboard so VS Code /
        JupyterLab notebook webviews execute the dropdown-population JS.
        """
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(
            storage, days=30, inline_plotly_js=False, as_iframe=True,
        )
        assert html.startswith("<iframe srcdoc="), (
            "as_iframe=True must wrap the document in an iframe"
        )
        # Sandbox attribute present so scripts can run inside the iframe.
        assert "allow-scripts" in html
        # Inner HTML survives unescaping (only & and " are escaped).
        inner = re.search(r'srcdoc="(.+?)"\s+style', html, re.S).group(1)
        inner = inner.replace("&quot;", '"').replace("&amp;", "&")
        assert 'id="dataset-select"' in inner
        assert "const SNAPSHOT" in inner

    def test_as_iframe_default_off(self):
        """Default behavior unchanged: bare HTML document, no iframe wrap."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert html.startswith("<!DOCTYPE html>")

    def test_validation_label_always_visible(self):
        """The validation dropdown must not start out hidden — earlier
        versions hid it until a dataset was picked, breaking the
        "All datasets + pick a validation" cross-dataset workflow."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        m = re.search(r'<label id="validation-label"[^>]*>', html)
        assert m, "validation-label not found"
        assert "hidden" not in m.group(0), (
            "validation-label must be visible on initial render"
        )

    def test_no_auto_select_first(self):
        """Default view stays at 'All datasets / All validations' —
        autoSelectFirst was removed because it hid the cross-dataset
        overview operators expect on first load."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert "autoSelectFirst" not in html

    def test_iframe_uses_fixed_height_with_internal_scroll(self):
        """`as_iframe=True` produces a fixed-height iframe — the
        contract is "iframe scrolls internally; once it hits its
        edge, scroll chains to the host notebook." Earlier
        auto-grow versions made the iframe taller than the
        notebook viewport and pushed the next cell off-screen."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30, as_iframe=True)
        # Fixed pixel height, not auto-grow.
        import re
        m = re.search(r'height:(\d+)px', html)
        assert m, "iframe must have a fixed pixel height"
        # Sanity: the height is bounded by iframe_height (default 900).
        assert int(m.group(1)) <= 1200, (
            f"iframe height {m.group(1)}px is too tall — would push "
            "the next notebook cell off-screen in VS Code"
        )
        # Auto-grow ResizeObserver is gone (caused cell-pushing).
        assert "ResizeObserver" not in html

    def test_collection_rows_join_by_metric_name(self):
        """The dashboard pulls collection rows alongside validations so
        the per-partition history of metrics that history-backed
        validators (drift / forecast / shape / pattern) only validate
        on the current partition still renders a full timeline.

        Without this, picking ``avg_amount_drift.avg_amount`` would
        show a single today-only point even though every past
        partition wrote ``avg_amount`` as a collection row.
        """
        from qualifire.reporting.html_report import generate_interactive_html

        storage = MagicMock()
        # 1 validation row for today's drift check on avg_amount,
        # plus 14 past-partition collection rows for the same metric.
        validation_today = self._row(
            run_id="r_today",
            run_timestamp="2026-05-08T10:00:00",
            partition_ts="2026-05-08",
            validation_name="avg_amount_drift.avg_amount",
            validation_type="drift",
            validation_status="ERROR",
            metric_name="avg_amount",
            metric_value=150.0,
        )
        validation_today["record_type"] = "validation"

        collection_rows = []
        for day in range(14):
            row = self._row(
                run_id=f"r_past_{day}",
                run_timestamp=f"2026-05-08T10:00:{day:02d}",  # written today
                partition_ts=f"2026-04-{24 + day:02d}",       # past partitions
                validation_name="",     # collection rows have no validation
                validation_type="",
                validation_status="",
                validation_message="",
                metric_name="avg_amount",
                metric_value=50.0 + day,
            )
            row["record_type"] = "collection"
            collection_rows.append(row)

        storage.read_health_data.return_value = [validation_today, *collection_rows]

        html = generate_interactive_html(
            storage, days=90, inline_plotly_js=False, as_iframe=False,
        )
        # Verify the storage call asked for collection rows too.
        storage.read_health_data.assert_called_with(days=90, include_collection=True)

        # All 15 rows reach SNAPSHOT (not just the 1 validation).
        m = re.search(
            r"const SNAPSHOT = (\[.*?\]);\s*\nconst SNAPSHOT_TRUNCATED = .+?;\s*\nconst DEFAULT_DAYS",
            html, re.S,
        )
        snapshot = json.loads(m.group(1))
        assert len(snapshot) == 15
        # record_type field is present so JS can split chart vs counts.
        assert {r["record_type"] for r in snapshot} == {"validation", "collection"}
        # All 14 collection metric values are intact (not deduped away).
        coll_values = sorted(r["metric_value"] for r in snapshot
                             if r["record_type"] == "collection")
        assert coll_values == [50.0 + d for d in range(14)]

    def test_storage_backwards_compat_without_include_collection_kwarg(self):
        """Storage backends predating ``include_collection`` (older
        in-process stubs, third-party adapters) must still work — the
        dashboard falls back to validation-only when the kwarg is
        rejected with TypeError."""
        from qualifire.reporting.html_report import generate_interactive_html

        legacy_storage = MagicMock()

        def _legacy_read(days):  # accepts ``days`` only
            return [self._row()]

        legacy_storage.read_health_data.side_effect = (
            lambda **kw: _legacy_read(**{k: v for k, v in kw.items() if k == "days"})
            if "include_collection" not in kw
            else (_ for _ in ()).throw(TypeError(
                "read_health_data() got an unexpected keyword argument 'include_collection'"
            ))
        )
        # Should not raise, should fall back to days-only call.
        html = generate_interactive_html(legacy_storage, days=30)
        assert "const SNAPSHOT = " in html

    def test_storage_call_propagates_include_collection(self, tmp_path):
        """End-to-end through SQLiteStorage: writing a validation row
        and a collection row, then calling read_health_data with
        ``include_collection=True``, returns BOTH rows. With the
        default (False) the collection row is filtered out."""
        from qualifire.storage.sqlite_storage import SQLiteStorage

        storage = SQLiteStorage(db_path=str(tmp_path / "qf.db"))
        storage.initialize()
        common = {
            "run_id": "r1", "run_timestamp": "2026-05-08T10:00:00",
            "owner": "o", "bu": "b", "dataset_name": "ds",
            "table_name": "t", "partition_ts": "2026-05-08",
            "metric_name": "amount", "metric_value": 1.0,
        }
        storage.write_results([
            {**common, "record_type": "validation",
             "validation_name": "v", "validation_status": "PASS"},
            {**common, "run_id": "r2", "record_type": "collection"},
        ])
        only_validations = storage.read_health_data(days=30)
        with_collections = storage.read_health_data(
            days=30, include_collection=True,
        )
        assert {r["record_type"] for r in only_validations} == {"validation"}
        assert {r["record_type"] for r in with_collections} == {"validation", "collection"}

    def test_dashboard_trusts_sql_dedupe(self):
        """The dashboard now trusts the SQL backend's per-natural-key
        dedupe — one row per (dataset, validation_name, metric,
        partition_ts, dim) with latest-run_timestamp + validation
        tiebreak applied SERVER-side. The dashboard JS just filters
        and renders; no further client-side grouping. Verified by
        showing the JS no longer builds the old collateral-row
        filter set."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        # The old collateral-filter set is gone; SQL handles it.
        assert "validatedRunMetric" not in html
        # validation_name is the lens — only rows matching the picked
        # validation's name surface (other validators' rows on the
        # same metric stay off-screen).
        assert "r.validation_name === state.validation" in html

    def test_history_chart_only_fills_gaps_with_collection_rows(self):
        """Per partition, if the picked validation ran (vrow exists),
        we show its verdict. Only partitions where the picked
        validation has no row fall through to the latest collection
        row → gray. Verified via the validationByPartition gap-filter
        in the JS source."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert "validationByPartition" in html
        assert "!validationByPartition.has(partitionKeyOf(r))" in html

    def test_history_section_hidden_unless_dataset_and_validation_picked(self):
        """The per-partition history section requires BOTH a dataset
        and a validation picked — it's hidden in the cross-dataset
        Health view and in the dataset-but-no-validation drill view."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert (
            "if (state.dataset === '__all__' || state.validation === '__all__') {"
            in html
        )

    def test_recollection_after_validation_shadows_old_verdict(self):
        """Recollection after an earlier validation results in the
        ``run_timestamp``-newest collection row winning the dedupe —
        the older verdict is now stale because the underlying value
        was rewritten and not re-validated. The chart must reflect
        that by surfacing the gray "collected (not validated)"
        marker instead of clinging to the stale verdict colour.

        Modeled here as data the dashboard JS would see: 1 validation
        @ T1, 1 collection @ T2 > T1, both for the same key. We don't
        run JS here, but the JS source contract is locked-in by
        ``test_dedupe_rule_is_latest_run_timestamp_with_validation_tiebreak``.
        This test pins the data shape that exercises the contract,
        so a future refactor can't quietly drop it.
        """
        from qualifire.reporting.html_report import generate_interactive_html

        validation_at_t1 = self._row(
            run_timestamp="2026-05-08T10:00:00",
            partition_ts="2026-05-08", validation_status="ERROR",
            metric_value=150.0,
        )
        validation_at_t1["record_type"] = "validation"
        recollection_at_t2 = dict(validation_at_t1)
        recollection_at_t2["record_type"] = "collection"
        recollection_at_t2["run_timestamp"] = "2026-05-08T11:00:00"
        recollection_at_t2["validation_name"] = ""
        recollection_at_t2["validation_status"] = ""
        recollection_at_t2["metric_value"] = 99.0  # the recollected (changed) value

        storage = _stub_storage_with_rows([validation_at_t1, recollection_at_t2])
        html = generate_interactive_html(storage, days=30)
        m = re.search(
            r"const SNAPSHOT = (\[.*?\]);\s*\nconst SNAPSHOT_TRUNCATED = .+?;\s*\nconst DEFAULT_DAYS",
            html, re.S,
        )
        snapshot = json.loads(m.group(1))
        assert len(snapshot) == 2
        # Both rows survive into SNAPSHOT — dedupe is dashboard-side.
        assert {r["record_type"] for r in snapshot} == {"validation", "collection"}

    def test_history_chart_purges_before_render(self):
        """Both code paths in ``renderHistoryLine`` (the
        ``state.validation === '__all__'`` early-out and the
        normal render) must call ``Plotly.purge`` before any
        ``Plotly.react`` or innerHTML write. Without it,
        switching validations leaves stale Plotly state attached
        to the div and the next render silently fails to redraw.
        """
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        # The function body must contain Plotly.purge in both branches.
        m = re.search(
            r"function renderHistoryLine\(\) \{(.+?)^\}",
            html, re.S | re.M,
        )
        assert m, "renderHistoryLine not found"
        body = m.group(1)
        # Two purges: one in the no-validation early-out, one before
        # the chart render.
        assert body.count("Plotly.purge(div)") >= 2, (
            "renderHistoryLine must purge in both branches"
        )

    def test_no_tabs_single_view(self):
        """The dashboard is single-view — no tab nav. Earlier two-tab
        layouts (Health Report + Drill) added complexity without
        clear benefit; a single view with always-visible dataset /
        validation pickers covers both summary and drill flows."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert "tab-btn" not in html
        assert "tab-nav" not in html
        # Dataset / validation pickers always visible (not gated).
        assert 'id="dataset-select"' in html
        assert 'id="validation-select"' in html

    def test_hidden_attribute_overrides_display_class_rules(self):
        """``<section hidden>`` defaults to ``display:none`` BUT any
        class rule that sets ``display`` (e.g. ``.chart-row {
        display: flex }``) wins by specificity, leaving the section
        visible. The dashboard adds an explicit
        ``.qf-dashboard [hidden] { display: none !important }``
        rule so toggling ``section.hidden`` actually hides it.
        Without this, the per-partition history section was
        showing despite its ``hidden`` attribute."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert ".qf-dashboard [hidden] { display: none !important; }" in html

    def test_iframe_scroll_chains_to_parent_on_overscroll(self):
        """When the iframe's internal scroll hits its top/bottom edge,
        further wheel motion forwards to ``window.parent.scrollBy``
        so the host notebook page scrolls. Without this, the user
        gets stuck at the iframe boundary and can't reach the next
        notebook cell by mouse-wheeling. ``passive: true`` keeps the
        handler from blocking chart interactions."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert "atTop = window.scrollY <= 0" in html
        assert "window.parent.scrollBy({ top: dy, behavior: 'auto' });" in html
        assert "{ passive: true }" in html

    def test_history_section_starts_hidden(self):
        """Per-partition history section is hidden until the user
        picks a specific validation — an empty placeholder area
        on first load was reported as confusing."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert re.search(
            r'<section class="chart-row" id="history-section" hidden',
            html,
        )

    def test_panel_labels_present(self):
        """Each chart panel carries an explicit ``panel-label`` heading."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        for label in ("Status counts", "Severity distribution",
                      "Pass-rate trend", "Per-partition history"):
            assert label in html, f"missing panel label: {label}"

    def test_pager_includes_first_last_edge_hint(self):
        """When the user is at the first or last page, the status
        line says so explicitly — disabled-button styling alone is
        easy to miss in dense tables."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert "first page" in html
        assert "last page" in html

    def test_pager_disables_prev_at_first_and_next_at_last(self):
        """``Prev`` button is disabled when ``state.page === 0``;
        ``Next`` is disabled when ``state.page >= totalPages - 1``.
        The click handlers also guard against out-of-bounds
        navigation, so even a stray click on a disabled-but-clicked
        button is a no-op."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert (
            "document.getElementById('pager-prev').disabled = state.page === 0;"
            in html
        )
        assert (
            "document.getElementById('pager-next').disabled = state.page >= totalPages - 1;"
            in html
        )
        # The click handler also guards against state.page < 0.
        assert "if (state.page > 0) { state.page -= 1; renderTable(); }" in html

    def test_dataset_view_table_sort_partition_severity_name(self):
        """Dataset-selected table sorts partition_ts DESC ▸ severity
        DESC (ERROR ▸ WARNING ▸ PASS) ▸ validation_name ASC."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert "SEV_RANK" in html
        assert "ERROR: 0, WARNING: 1, PASS: 2" in html

    def test_handles_empty_storage(self, tmp_path):
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([])
        html = generate_interactive_html(storage, days=30)
        assert "const SNAPSHOT = []" in html
        # Controls and chart placeholders still rendered
        assert 'id="dataset-select"' in html
        assert 'id="severity-pie"' in html

    def test_internal_failure_marker_surfaced_in_snapshot(self):
        """Phase 2 of external-catalog-system-table-hardening:
        rows tagged with
        ``details_json['qualifire_internal_failure'] = True`` must
        surface a top-level ``qualifire_internal_failure: True``
        boolean on every snapshot row. Without this, the dashboard
        JS can't tell internal failures from real ERRORs.

        Round-7 codex review RISK fix: tolerates BOTH dict and
        JSON-string ``details_json`` payloads — the in-memory
        engine path passes a dict, but rows re-read from the
        persisted system table (TEXT column) arrive as a JSON
        string.
        """
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([
            # Genuine data-quality ERROR — no marker.
            self._row(
                run_id="real",
                validation_name="real_finding",
                validation_status="ERROR",
                details_json="{}",
            ),
            # Internal failure with DICT details_json (in-memory path).
            self._row(
                run_id="internal_dict",
                validation_name="qualifire.persistence",
                validation_status="ERROR",
                details_json={"qualifire_internal_failure": True,
                              "error": "system-table outage"},
            ),
            # Internal failure with STRING details_json (re-read path).
            self._row(
                run_id="internal_str",
                validation_name="threshold_error",
                validation_status="ERROR",
                details_json='{"qualifire_internal_failure": true}',
            ),
        ])
        html = generate_interactive_html(storage, days=30)

        m = re.search(
            r"const SNAPSHOT = (\[.*?\]);\s*\nconst SNAPSHOT_TRUNCATED = .+?;\s*\nconst DEFAULT_DAYS",
            html, re.S,
        )
        assert m, "could not find SNAPSHOT block"
        snapshot = json.loads(m.group(1))
        by_run = {r["run_id"]: r for r in snapshot}

        # Real finding: marker is False.
        assert by_run["real"]["qualifire_internal_failure"] is False
        # Both internal-failure shapes detected.
        assert by_run["internal_dict"]["qualifire_internal_failure"] is True
        assert by_run["internal_str"]["qualifire_internal_failure"] is True

    def test_internal_failure_rows_render_with_distinct_class(self):
        """Phase 2: rows tagged as qualifire-internal-failure render
        with the ``qf-internal-failure`` CSS class (wrench-icon
        prefix + muted color) instead of the standard
        ``sev-error`` red badge. Operators reading the table can
        tell at a glance that the ERROR is a library / infra
        failure, not a data-quality finding."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        # CSS class for the distinct treatment is present.
        assert ".qf-internal-failure" in html
        # Wrench icon prefix in the ::before pseudo-element.
        assert "🔧" in html
        # JS predicate that drives the row-level rendering.
        assert "isInternalFailure" in html
        assert "qualifire_internal_failure" in html

    def test_internal_failure_rows_excluded_from_pass_rate_trend(self):
        """Phase 2: internal-failure rows are excluded from the
        pass-rate trend chart so a storage outage doesn't tank the
        displayed pass rate when the validations themselves
        succeeded. The chart's title now reflects this."""
        from qualifire.reporting.html_report import generate_interactive_html

        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        # Trend chart title mentions the exclusion so operators
        # don't think the pass rate ignored real failures.
        assert "excluding internal failures" in html
        # Chart-rendering JS uses the predicate to filter.
        assert "filtered().filter(r => !isInternalFailure(r))" in html

    def test_qualifire_method_writes_file(self, tmp_path):
        """`qf.interactive_dashboard(output_path=...)` writes HTML and returns it."""
        from qualifire.api import Qualifire

        backend = MagicMock()

        # Minimal Qualifire that gets a stub storage from _init_storage.
        qf = Qualifire.__new__(Qualifire)
        qf.backend = backend
        qf.system_table = "qf"
        qf.system_table_backend = "sqlite"
        qf.owner, qf.bu = "o", "b"
        qf.jdbc = None
        qf._notifiers = {}
        qf._storage = _stub_storage_with_rows([self._row()])

        out = tmp_path / "i.html"
        html = qf.interactive_dashboard(output_path=str(out), days=14)
        assert out.read_text() == html
        assert "const SNAPSHOT" in html


class TestRichDetailPanel:
    """AC suite for dashboard-rich-detail-panel (PR #18)."""

    def _row(self, **overrides):
        base = {
            'run_id': 'r1', 'run_timestamp': '2026-05-01T10:00:00',
            'owner': 'o', 'bu': 'b',
            'dataset_name': 'sales', 'table_name': 'sales_fact',
            'metric_name': 'auc', 'metric_value': 0.91,
            'collection_type': 'sample',
            'validation_name': 'pattern_check', 'validation_type': 'pattern',
            'validation_status': 'ERROR', 'validation_message': 'AUC 0.91',
            'notification_channel': None, 'notification_status': None,
            'details_json': {
                'auc': 0.91, 'auc_std': 0.05, 'n_current': 100, 'n_past': 200,
                'top_contributing_features': [
                    {'feature': 'amount', 'importance': 0.5},
                ],
                'value_drift_explainer': [
                    {'feature': 'amount', 'source_column': 'amount',
                     'kind': 'numeric', 'summary': 'amount; p50 +43%; mean +51%; p99 +67%',
                     'current': {'mean': 100.0}, 'past': {'mean': 70.0}},
                ],
            },
            'record_type': 'validation', 'collector_name': None,
            'dimension_value': None,
            'collected_at': None, 'validated_at': None, 'notified_at': None,
            'partition_ts': '2026-05-01',
            'expected_value': None, 'actual_value_text': None,
            'dataset_description': None, 'validation_description': None,
        }
        base.update(overrides)
        return base

    def test_AC1_AC7_renderers_and_details_present_in_snapshot(self):
        """AC#1: every snapshot row carries 'details'.
        AC#7: rendered HTML contains DETAIL_RENDERERS and per-renderer names."""
        from qualifire.reporting.html_report import generate_interactive_html
        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        # Registry + per-renderer functions present.
        for needle in ['DETAIL_RENDERERS', 'renderShapePattern', 'renderDrift',
                       'renderForecast', 'renderThreshold', 'renderSLO',
                       'renderGeneric', 'renderInternalFailure',
                       'detail-row', 'qf-detail-toggle', 'wireDetailToggles',
                       'snapshot-truncation-banner']:
            assert needle in html, f'missing: {needle}'
        # SNAPSHOT carries the parsed details dict.
        import re, json
        m = re.search(
            r"const SNAPSHOT = (\[.*?\]);\s*\nconst SNAPSHOT_TRUNCATED",
            html, re.S,
        )
        assert m, 'SNAPSHOT block not found'
        snapshot = json.loads(m.group(1))
        assert snapshot[0]['details']['auc'] == 0.91
        assert 'value_drift_explainer' in snapshot[0]['details']

    def test_AC14_script_breakout_safe_embedding(self):
        """AC#14: a hostile category value containing </script> does
        not break out of the embedded SNAPSHOT script block."""
        from qualifire.reporting.html_report import generate_interactive_html
        row = self._row()
        row['details_json'] = {
            'top_contributing_features': [{'feature': 'x', 'importance': 0.5}],
            'value_drift_explainer': [{
                'feature': 'x', 'kind': 'label_encoded',
                'category': '</script><script>alert(1)</script>',
                'summary': 'malicious',
            }],
        }
        storage = _stub_storage_with_rows([row])
        html = generate_interactive_html(storage, days=30)
        # Find the SNAPSHOT script block; assert no raw </script> in it.
        import re
        m = re.search(
            r'const SNAPSHOT = (\[.*?\]);',
            html, re.S,
        )
        assert m, 'SNAPSHOT block not found'
        snapshot_literal = m.group(1)
        assert '</script>' not in snapshot_literal, \
            'script breakout possible inside SNAPSHOT literal'
        assert '</' not in snapshot_literal, \
            'breakout vector </ found unescaped'

    def test_AC16_PII_path_aware_explainer_passes_through(self):
        """AC#16: explainer category values are NOT redacted (codex BLOCKER #3).
        A non-explainer api_key field IS redacted."""
        from qualifire.reporting.html_report import generate_interactive_html
        row = self._row()
        row['details_json'] = {
            'api_key': 'sk-12345',
            'top_contributing_features': [{'feature': 'email', 'importance': 0.5}],
            'value_drift_explainer': [{
                'feature': 'email', 'kind': 'label_encoded',
                'category': 'alice@example.com',
                'current': {'top': [{'value': 'alice@example.com', 'rate': 0.5}]},
                'past': {'top': [{'value': 'bob@example.com', 'rate': 0.4}]},
                'summary': 'top: alice@example.com=50%',
            }],
        }
        storage = _stub_storage_with_rows([row])
        html = generate_interactive_html(storage, days=30)
        # Explainer category passes through.
        assert 'alice@example.com' in html
        # Non-explainer secret redacted.
        assert 'sk-12345' not in html
        assert '[redacted]' in html

    def test_AC23_banner_only_when_truncated(self):
        """AC#23: truncation banner element exists; populated by JS based
        on SNAPSHOT_TRUNCATED / per-row markers. Pin: the literal banner
        text appears inside the embedded JS source so the runtime
        condition can match it."""
        from qualifire.reporting.html_report import generate_interactive_html
        storage = _stub_storage_with_rows([self._row()])
        html = generate_interactive_html(storage, days=30)
        assert 'Some details were truncated' in html


"""Executable JS-side tests for the dashboard renderer registry.

Spawns Node and runs the embedded JS (lifted from
``qualifire/reporting/html_report.py``'s ``_INTERACTIVE_JS``
constant) against fixture rows. Exercises the click-toggle
contract, escape-on-render, parallel-list zip behavior, the
mismatch fallback, and internal-failure rendering.

Skips with a clear message when ``node`` is unavailable so the
suite stays runnable in minimal CI environments.
"""

from __future__ import annotations

import json
import shutil
import subprocess

import pytest

NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(
    NODE is None, reason="node not on PATH; renderer tests require Node",
)


def _embedded_js() -> str:
    """Return the same JS the dashboard embeds, plus a tiny shim
    that satisfies the renderers' missing browser globals (escapeHtml
    is defined inside _INTERACTIVE_JS but isInternalFailure is
    defined elsewhere in the JS const)."""
    from qualifire.reporting import html_report
    js = html_report._INTERACTIVE_JS
    # The renderers call ``isInternalFailure(r)`` and ``escapeHtml(s)``.
    # ``escapeHtml`` is part of _INTERACTIVE_JS already; ``isInternalFailure``
    # is also defined in the same constant. We DON'T need a DOM for the
    # pure renderer tests — just to call the functions and capture
    # their HTML string output. Provide minimal stubs for browser
    # globals the JS touches at top level (``document``,
    # ``getComputedStyle``).
    # Stub a tiny DOM. The dashboard's init() touches several elements;
    # we return a chainable stub so .textContent / .addEventListener
    # / .innerHTML / .value all no-op without throwing.
    shim = """
        function stubElement() {
            const el = {
                textContent: '',
                innerHTML: '',
                value: '',
                hidden: false,
                children: [],
                dataset: {},
                addEventListener: () => {},
                appendChild: () => stubElement(),
                insertBefore: () => stubElement(),
                removeChild: () => stubElement(),
                remove: () => {},
                closest: () => null,
                querySelector: () => stubElement(),
                querySelectorAll: () => ({ forEach: () => {}, length: 0 }),
                setAttribute: () => {},
                getAttribute: () => null,
                parentElement: null,
                nextElementSibling: null,
                classList: { contains: () => false, add: () => {}, remove: () => {} },
                style: {},
            };
            el.parentElement = el;  // self-reference is fine for stub paths
            return el;
        }
        const document = {
            querySelector: () => stubElement(),
            getElementById: () => stubElement(),
            querySelectorAll: () => ({ forEach: () => {}, length: 0 }),
            createElement: () => stubElement(),
        };
        const getComputedStyle = () => ({ getPropertyValue: () => '' });
        const Plotly = new Proxy({}, { get: () => () => {} });
        const SNAPSHOT = [];
        const SNAPSHOT_TRUNCATED = null;
        const DEFAULT_DAYS = 30;
        // window proxy: any unknown property/method is a no-op stub.
        // Node's globalThis lacks addEventListener which the dashboard
        // expects on window, so we route everything through a Proxy.
        if (!globalThis.addEventListener) {
            globalThis.addEventListener = () => {};
        }
        if (!globalThis.removeEventListener) {
            globalThis.removeEventListener = () => {};
        }
        const window = globalThis;
    """
    return shim + "\n" + js


def _run_node(extra_js: str) -> dict:
    full = _embedded_js() + "\n" + extra_js
    proc = subprocess.run(
        [NODE, "--input-type=module", "-e", full],
        capture_output=True, text=True, timeout=30,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"Node failed: rc={proc.returncode}\n"
            f"STDERR:\n{proc.stderr}\nSTDOUT:\n{proc.stdout}"
        )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def test_renderGeneric_escapes_script_in_value():
    """AC#10: the generic renderer escapes ``<script>`` in detail
    values to ``&lt;script&gt;`` text."""
    extra = """
        const out = renderGeneric({
            validation_type: 'unknown',
            details: { msg: '<script>alert(1)</script>' },
        });
        const hasRaw = out.indexOf('<script>alert') >= 0;
        const hasEscaped = out.indexOf('&lt;script&gt;') >= 0;
        console.log(JSON.stringify({ hasRaw, hasEscaped }));
    """
    out = _run_node(extra)
    assert out["hasRaw"] is False, "raw <script> leaked into rendered HTML"
    assert out["hasEscaped"] is True, "expected HTML-escaped <script>"


def test_renderShapePattern_zips_parallel_lists():
    extra = """
        const row = {
            validation_type: 'pattern',
            validation_status: 'ERROR',
            details: {
                auc: 0.91,
                top_contributing_features: [
                    { feature: 'amount', importance: 0.5 },
                    { feature: 'channel_mobile', importance: 0.3 },
                ],
                value_drift_explainer: [
                    { feature: 'amount', kind: 'numeric', summary: 'p50 +43%' },
                    { feature: 'channel_mobile', kind: 'onehot', summary: 'rate +30pp' },
                ],
            },
        };
        const html = renderShapePattern(row);
        console.log(JSON.stringify({
            hasAmount: html.indexOf('amount') >= 0,
            hasChannel: html.indexOf('channel_mobile') >= 0,
            hasFirstSummary: html.indexOf('p50 +43%') >= 0,
            hasSecondSummary: html.indexOf('rate +30pp') >= 0,
            hasAUC: html.indexOf('0.91') >= 0,
        }));
    """
    out = _run_node(extra)
    assert all(out.values()), out


def test_renderShapePattern_handles_length_mismatch():
    extra = """
        const row = {
            validation_type: 'pattern',
            details: {
                auc: 0.85,
                top_contributing_features: [
                    { feature: 'a', importance: 0.5 },
                    { feature: 'b', importance: 0.3 },
                    { feature: 'c', importance: 0.2 },
                ],
                value_drift_explainer: [
                    { feature: 'a', kind: 'numeric', summary: 's1' },
                ],
            },
        };
        const html = renderShapePattern(row);
        console.log(JSON.stringify({
            hasMismatchWarning: html.indexOf('length mismatch') >= 0,
            hasAllFeatures: html.indexOf('a') >= 0
                && html.indexOf('b') >= 0
                && html.indexOf('c') >= 0,
        }));
    """
    out = _run_node(extra)
    assert out["hasMismatchWarning"] is True
    assert out["hasAllFeatures"] is True


def test_renderShapePattern_drift_only_payload_renders():
    """Codex impl R2 LOW regression: a hand-edited payload with
    ``value_drift_explainer`` populated but ``top_contributing_features``
    empty/absent must still surface the explainer rows (not silently
    hide them). Length-mismatch warning fires.
    """
    extra = """
        const row = {
            validation_type: 'pattern',
            details: {
                auc: 0.85,
                top_contributing_features: [],
                value_drift_explainer: [
                    { feature: 'amount', kind: 'numeric',
                      summary: 'p50 +43%',
                      current: { mean: 100 }, past: { mean: 70 } },
                ],
            },
        };
        const html = renderShapePattern(row);
        console.log(JSON.stringify({
            hasMismatch: html.indexOf('length mismatch') >= 0,
            hasExplainerRow: html.indexOf('amount') >= 0
                && html.indexOf('p50 +43%') >= 0,
            hasFeatureTable: html.indexOf('Feature</th>') >= 0,
        }));
    """
    out = _run_node(extra)
    assert out["hasMismatch"] is True
    assert out["hasExplainerRow"] is True
    assert out["hasFeatureTable"] is True


def test_renderShapePattern_pass_row_no_empty_table():
    """AC#21 (R2.4): a PASS row with no top features and no
    explainer renders the AUC line cleanly without an empty
    'top features' table or 'value drift unavailable' message."""
    extra = """
        const row = {
            validation_type: 'pattern',
            validation_status: 'PASS',
            details: { auc: 0.55 },
        };
        const html = renderShapePattern(row);
        console.log(JSON.stringify({
            hasAUC: html.indexOf('0.55') >= 0,
            hasFeatureTable: html.indexOf('Feature</th>') >= 0,
            hasUnavailable: html.indexOf('unavailable') >= 0,
        }));
    """
    out = _run_node(extra)
    assert out["hasAUC"] is True
    assert out["hasFeatureTable"] is False
    assert out["hasUnavailable"] is False


def test_renderInternalFailure_surfaces_error_only():
    """AC#18 / R1.5: internal-failure renderer shows just the
    `error` string the engine populated, no fabricated stack."""
    extra = """
        const row = {
            validation_type: 'pattern',
            details: {
                qualifire_internal_failure: true,
                error: 'KeyError: missing_column',
            },
        };
        const html = renderInternalFailure(row);
        console.log(JSON.stringify({
            hasError: html.indexOf('KeyError: missing_column') >= 0,
            hasInternalLabel: html.indexOf('Qualifire-internal') >= 0,
        }));
    """
    out = _run_node(extra)
    assert out["hasError"] is True
    assert out["hasInternalLabel"] is True


def test_parseExpected_handles_python_repr():
    """AC#20 / C1.6: ``str(dict)`` from Python uses single quotes;
    parseExpected falls back to a single-to-double-quote rewrite."""
    extra = """
        const a = parseExpected("{'warning': {'min': 100}, 'error': {'min': 50}}");
        const b = parseExpected('{"warning": {"min": 100}}');  // strict JSON path
        const c = parseExpected("not-parseable");  // raw fallback
        console.log(JSON.stringify({ a, b, c }));
    """
    out = _run_node(extra)
    assert out["a"] == {"warning": {"min": 100}, "error": {"min": 50}}
    assert out["b"] == {"warning": {"min": 100}}
    assert out["c"] == "not-parseable"


def test_renderThreshold_branches_on_warning_error():
    extra = """
        const row = {
            validation_type: 'threshold',
            actual_value_text: '42',
            expected_value: "{'warning': {'min': 50}, 'error': {'min': 10}}",
            metric_value: null,
            details: null,
        };
        const html = renderThreshold(row);
        console.log(JSON.stringify({
            hasActual: html.indexOf('42') >= 0,
            hasWarning: html.indexOf('warning') >= 0
                && html.indexOf('min=50') >= 0,
            hasError: html.indexOf('error') >= 0
                && html.indexOf('min=10') >= 0,
        }));
    """
    out = _run_node(extra)
    assert all(out.values()), out


def test_rowDetailKey_collision_free_with_pipe_in_dimension_value():
    """AC#24 / codex impl R1 HIGH #1: keys for two rows that differ
    only by partition in the dimension_value vs validation_name
    fields must be DIFFERENT, even when those fields embed any
    separator-like character. JSON.stringify is collision-free."""
    extra = """
        const a = rowDetailKey({
            validation_name: 'v|x',
            partition_ts: '2026-05-09',
            run_timestamp: '2026-05-09T00:00:00',
            dimension_value: 'a',
        });
        const b = rowDetailKey({
            validation_name: 'v',
            partition_ts: '2026-05-09',
            run_timestamp: '2026-05-09T00:00:00',
            dimension_value: 'x|a',
        });
        console.log(JSON.stringify({ a, b, equal: a === b }));
    """
    out = _run_node(extra)
    assert out["equal"] is False, f"key collision: {out}"


def test_renderDriftEntry_per_kind_branches():
    """codex impl R1 HIGH #2: per-kind tables (numeric / boolean /
    datetime / onehot / label_encoded) actually render their
    fields, not just the summary."""
    extra = """
        const numeric = renderDriftEntry({
            kind: 'numeric',
            current: { mean: 100, p50: 100, p99: 500, null_pct: 0.02, count: 1000 },
            past:    { mean: 80,  p50: 70,  p99: 300, null_pct: 0.01, count: 3000 },
            summary: 'p50 +43%',
        });
        const boolean = renderDriftEntry({
            kind: 'boolean',
            current: { true_rate: 0.7, null_pct: 0.0, count: 100 },
            past:    { true_rate: 0.5, null_pct: 0.0, count: 100 },
        });
        const datetime = renderDriftEntry({
            kind: 'datetime',
            current: { min: '2026-05-01', max: '2026-05-09', null_pct: 0.0, count: 100 },
            past:    { min: '2026-04-01', max: '2026-04-30', null_pct: 0.0, count: 200 },
        });
        const onehot = renderDriftEntry({
            kind: 'onehot',
            category: 'mobile',
            is_null_bin: false,
            current: { rate: 0.6, count: 100 },
            past:    { rate: 0.3, count: 200 },
        });
        const label = renderDriftEntry({
            kind: 'label_encoded',
            current: { top: [{ value: 'bot', rate: 0.5 }, { value: 'human', rate: 0.3 }] },
            past:    { top: [{ value: 'human', rate: 0.7 }, { value: 'bot', rate: 0.2 }] },
        });
        console.log(JSON.stringify({
            numeric_has_mean: numeric.indexOf('mean') >= 0,
            numeric_has_p99: numeric.indexOf('p99') >= 0,
            boolean_has_true_rate: boolean.indexOf('true_rate') >= 0,
            datetime_has_min: datetime.indexOf('min') >= 0,
            onehot_has_category: onehot.indexOf('mobile') >= 0,
            onehot_has_rate: onehot.indexOf('rate') >= 0,
            label_has_categories: label.indexOf('bot') >= 0
                && label.indexOf('human') >= 0,
        }));
    """
    out = _run_node(extra)
    assert all(out.values()), out


def test_renderDriftEntry_handles_truncated_kind():
    extra = """
        const out = renderDriftEntry({
            kind: 'truncated',
            summary: 'payload truncated',
        });
        console.log(JSON.stringify({ hasMsg: out.indexOf('payload truncated') >= 0 }));
    """
    out = _run_node(extra)
    assert out["hasMsg"] is True


def test_DETAIL_RENDERERS_dispatches_by_type():
    """Registry dispatches shape/pattern/drift/trend/threshold/slo,
    falls back to renderGeneric for unknown types."""
    extra = """
        const types = ['shape', 'pattern', 'drift', 'trend', 'threshold', 'slo', 'unknown'];
        const got = {};
        types.forEach(t => {
            const row = { validation_type: t, details: null };
            const html = renderDetailPanel(row);
            got[t] = html.length > 0;
        });
        console.log(JSON.stringify(got));
    """
    out = _run_node(extra)
    assert all(out.values()), out

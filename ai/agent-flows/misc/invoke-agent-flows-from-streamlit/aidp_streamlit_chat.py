from __future__ import annotations

import os
import json
import re
from datetime import datetime
from typing import Any, Dict, Optional, List
from pathlib import Path

import streamlit as st
import oci
import requests
import pandas as pd
from streamlit_tree_select import tree_select
from aidp_chat import AIDPChatClient, chat, chat_stream, get_last_response_id


# ============== Helpers for templates and configuration ==============

def load_templates_from_file() -> tuple[Dict[str, str], str]:
    """
    Load prompt templates from JSON file.
    Supports multiple file locations for standalone and Docker deployments.
    Returns a tuple of (templates dict, source path string).
    """
    # Default fallback templates
    default_templates = {
        "Generate SQL Query": "Generate a SQL query to {task}.\n\nTable schema:\n{schema}\n\nRequirements:\n{requirements}",
        "Explain Model Results": "Explain the results from this model:\n\nModel type: {model_type}\nMetrics: {metrics}\nPredictions: {predictions}\n\nWhat do these results indicate?",
        "Data Quality Check": "Analyze this data for quality issues:\n\nDataset: {dataset_name}\nSample data:\n{sample_data}\n\nCheck for: missing values, outliers, data types, duplicates, and anomalies.",
        "Feature Engineering": "Suggest feature engineering approaches for:\n\nTarget variable: {target}\nAvailable features: {features}\nProblem type: {problem_type}\n\nWhat features should I create or transform?",
    }

    # Try multiple file locations (for Docker and local)
    possible_paths = [
        Path("prompt_templates.json"),  # Current directory
        Path(__file__).parent / "prompt_templates.json",  # Same directory as script
        Path("/app/prompt_templates.json"),  # Docker mount location
        Path(os.getenv("TEMPLATES_PATH", "")) / "prompt_templates.json" if os.getenv("TEMPLATES_PATH") else None,
    ]

    # Remove None values
    possible_paths = [p for p in possible_paths if p is not None]

    for template_path in possible_paths:
        try:
            if template_path.exists():
                with open(template_path, 'r') as f:
                    data = json.load(f)

                # Extract templates (handle both simple and structured format)
                templates = {}
                for name, content in data.items():
                    if isinstance(content, dict) and "template" in content:
                        templates[name] = content["template"]
                    elif isinstance(content, str):
                        templates[name] = content
                    else:
                        templates[name] = str(content)

                print(f"✓ Loaded {len(templates)} templates from {template_path}")
                return templates, str(template_path)

        except Exception as e:
            print(f"Could not load templates from {template_path}: {e}")
            continue

    print("Using default templates (no template file found)")
    return default_templates, "built-in defaults"


# ============== Helpers for response parsing ==============

def export_conversation_as_json() -> str:
    """Export the current conversation as JSON."""
    export_data = {
        "exported_at": datetime.now().isoformat(),
        "messages": st.session_state.messages,
        "last_response_id": st.session_state.last_response_id,
        "trace_history": st.session_state.trace_history
    }
    return json.dumps(export_data, indent=2)


def export_conversation_as_markdown() -> str:
    """Export the current conversation as Markdown."""
    lines = [
        f"# Chat Conversation",
        f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        ""
    ]

    for i, msg in enumerate(st.session_state.messages, 1):
        role = msg["role"].title()
        content = msg["content"]
        lines.append(f"## Message {i} - {role}")
        lines.append("")
        lines.append(content)
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def render_message_with_copy(content: str, role: str, message_idx: int):
    """Render a message with copy button and syntax highlighting for code blocks."""

    # Split content into code and non-code sections
    parts = re.split(r'(```[\s\S]*?```)', content)

    for part_idx, part in enumerate(parts):
        if part.startswith('```') and part.endswith('```'):
            # Extract code block
            code_content = part[3:-3]

            # Try to detect language from first line
            lines = code_content.split('\n', 1)
            if len(lines) > 1 and lines[0].strip() and not ' ' in lines[0].strip():
                language = lines[0].strip()
                code = lines[1]
            else:
                language = "python"  # Default language
                code = code_content

            # Render code block with built-in copy functionality
            st.code(code, language=language, line_numbers=True)
        else:
            # Regular markdown content
            if part.strip():
                st.markdown(part)

    # Initialize copy state
    copy_key = f"show_copy_{role}_{message_idx}"
    if copy_key not in st.session_state:
        st.session_state[copy_key] = False

    # Simple copy button that shows a copyable text area
    col1, col2 = st.columns([0.9, 0.1])
    with col2:
        if st.button("📋", key=f"copy_btn_{role}_{message_idx}", help="Copy message", use_container_width=True):
            st.session_state[copy_key] = not st.session_state[copy_key]

    # Show copyable text area if button was clicked
    if st.session_state[copy_key]:
        st.text_area(
            "Select all (Ctrl/Cmd+A) and copy (Ctrl/Cmd+C):",
            value=content,
            height=100,
            key=f"copyable_{role}_{message_idx}",
        )


def response_to_markdown(resp) -> str:
    """
    Convert the AIDP chat response JSON into a single markdown string.
    Mirrors print_response_content() but returns text instead of printing.
    """
    try:
        data = resp.json()
    except Exception:
        return "(invalid response)"

    outputs = data.get("output") or []
    parts: List[str] = []
    for aoutput in outputs:
        contents = aoutput.get("content") or []
        for c in contents:
            # AIDP returns {"type": "...", "text": "..."} for text chunks
            ttype = c.get("type")
            if ttype is not None and ttype == "trace":
              continue
            text = c.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "\n\n".join(parts).strip() if parts else ""


# -------- Traces rendering helpers --------
def _fmt_duration(val: Optional[float], unit: str = "ms") -> str:
    """
    Format a duration in milliseconds as ms or seconds.
    """
    try:
        f = float(val)
        #if unit == "s":
        #    return f"{f/1000:.2f} s"
        return f"{f:.0f} s"
    except Exception:
        return "—"


def _heat_emoji(pct: Optional[float]) -> str:
    """
    Return a heat emoji representing the relative duration.
    pct is 0..1
    """
    if pct is None:
        return "▫️"
    if pct >= 0.75:
        return "🟥"
    if pct >= 0.5:
        return "🟧"
    if pct >= 0.25:
        return "🟨"
    return "🟩"


def _span_times_and_dur(s) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (start_ms, end_ms, duration_s) as floats when possible.
    """
    dur = s.get("duration_s")
    start = s.get("startTime")
    end = s.get("endTime")
    try:
        if dur is None and start is not None and end is not None:
            dur = (end - start) / 1000000000
        dur_f = float(dur) if dur is not None else None
    except Exception:
        dur_f = None
    try:
        start_f = float(start/1000000000) if start is not None else None
    except Exception:
        start_f = None
    try:
        end_f = float(end/1000000000) if end is not None else None
    except Exception:
        end_f = None
    return start_f, end_f, dur_f


# ===== Tree-select helpers =====
def _get_span_id(s: dict) -> Optional[str]:
    try:
        sid = s.get("id") or s.get("spanId") or s.get("span_id") or s.get("spanID")
        return str(sid) if sid is not None else None
    except Exception:
        return None


def _get_parent_id(s: dict) -> Optional[str]:
    try:
        pid = (
            s.get("parentSpanId")
            or s.get("parent_span_id")
            or s.get("parentSpanID")
            or s.get("parent_id")
        )
        return str(pid) if pid is not None else None
    except Exception:
        return None


def _span_attrs_text(s: dict) -> str:
    attrs = s.get("attributes") or s.get("metadata") or s.get("tags") or {}
    if isinstance(attrs, dict) and attrs:
        try:
            return " ".join([f"{k} {v}" for k, v in attrs.items()])
        except Exception:
            return str(attrs)
    return ""


def _span_display_name(s: dict) -> str:
    return s.get("spanName") or "trace"


def _span_label_for_select(s: dict, total_ms: Optional[float], duration_unit: str) -> str:
    name = _span_display_name(s)
    _, _, dur = _span_times_and_dur(s)
    pct = None
    if total_ms and dur is not None and total_ms > 0:
        try:
            pct = max(0.0, min(1.0, float(dur) / float(total_ms)))
        except Exception:
            pct = None
    heat = _heat_emoji(pct)
    sid = _get_span_id(s)
    id_snip = f" • id={sid}" if sid is not None else ""
    label = f"{heat} {name}{id_snip} • {_fmt_duration(dur, duration_unit)}"
    if pct is not None:
        label += f" ({int(pct * 100)}%)"
    return label


def _matches_filters(s: dict, name_filter: str, attr_filter: str) -> bool:
    nfilter = (name_filter or "").strip().lower()
    afilter = (attr_filter or "").strip().lower()
    if not nfilter and not afilter:
        return True
    name = _span_display_name(s).lower()
    attrs_text = _span_attrs_text(s).lower()
    if nfilter and nfilter not in name:
        return False
    if afilter and afilter not in attrs_text:
        return False
    return True


def _extract_kids(s: dict) -> List[dict]:
    kids = s.get("children") or s.get("spans") or []
    return kids if isinstance(kids, list) else []


def _build_tree_nodes_for_select(
    trace: dict,
    spans: List[dict],
    total_ms: Optional[float],
    sort_children: bool,
    name_filter: str,
    attr_filter: str,
    duration_unit: str,
) -> tuple[List[dict], Dict[str, dict]]:
    """
    Build nodes and an index for streamlit-tree-select from either:
      - nested spans (with 'children' or 'spans'), or
      - flat spans (with 'id'/'spanId' and 'parentSpanId'/parent_span_id)
    Returns (nodes, span_index)
    """
    if not isinstance(spans, list):
        return [], {}

    # Determine shape: nested vs flat
    # Spans are provided as a flat list; enforce flat-building path
    has_nested = False

    # Index of span_id -> span object (stringified ids)
    span_index: Dict[str, dict] = {}

    def sort_kids(kids: List[dict]) -> List[dict]:
        if not sort_children:
            return kids
        try:
            return sorted(kids, key=lambda x: (_span_times_and_dur(x)[2] or 0.0), reverse=True)
        except Exception:
            return kids

    def build_node_from_span(s: dict, get_children_fn) -> Optional[dict]:
        sid = _get_span_id(s)
        if sid is None:
            # Synthesize a stable id from name and timing if missing
            _, _, dur = _span_times_and_dur(s)
            sid = f"{_span_display_name(s)}:{str(dur)}:{id(s)}"
        sid = str(sid)
        span_index[sid] = s

        children_src = sort_kids(get_children_fn(s))
        child_nodes: List[dict] = []
        for c in children_src:
            cn = build_node_from_span(c, get_children_fn)
            if cn is not None:
                child_nodes.append(cn)

        # Keep node if it matches filters or has any kept children
        keep = _matches_filters(s, name_filter, attr_filter) or bool(child_nodes)
        if not keep:
            return None

        return {
            "label": _span_label_for_select(s, total_ms, duration_unit),
            "value": sid,
            "children": child_nodes,
        }

    nodes: List[dict] = []

    # Flat shape: build adjacency lists
    id_map: Dict[str, dict] = {}
    children_map: Dict[str, List[dict]] = {}
    roots: List[dict] = []

    for s in spans:
        sid = _get_span_id(s)
        sid = str(sid) if sid is not None else f"noid:{id(s)}"
        id_map[sid] = s
        children_map.setdefault(sid, [])

    for s in spans:
        sid = _get_span_id(s)
        sid = str(sid) if sid is not None else f"noid:{id(s)}"
        pid = _get_parent_id(s)
        pid = str(pid) if pid is not None else None
        if pid and pid == sid:
            roots.append(s)
        else:
          if pid and pid in id_map:
            children_map[pid].append(s)
          else:
            roots.append(s)

    # Sort child lists if requested
    for pid, kids in list(children_map.items()):
        children_map[pid] = sort_kids(kids)

    def get_children_from_flat(x: dict) -> List[dict]:
        xid = _get_span_id(x)
        xid = str(xid) if xid is not None else f"noid:{id(x)}"
        return children_map.get(xid, [])

    for r in roots:
        n = build_node_from_span(r, get_children_from_flat)
        if n is not None:
            nodes.append(n)

    return nodes, span_index

def _render_spans(
    spans: List[dict],
    level: int = 0,
    total_ms: Optional[float] = None,
    name_filter: str = "",
    attr_filter: str = "",
    sort_children: bool = True,
    show_attrs_default: bool = False,
    expanded_depth: int = 1,
    duration_unit: str = "ms",
):
    """
    Render spans as an interactive, hierarchical tree using expanders, with
    duration badges, relative progress indicators, and filtering.
    """
    if not isinstance(spans, list):
        return

    nfilter = (name_filter or "").strip().lower()
    afilter = (attr_filter or "").strip().lower()

    def _span_name(s: dict) -> str:
        return s.get("spanName") or "trace"

    # Local helper to compute child order
    def _children_for(s: dict) -> List[dict]:
        kids = s.get("children") or s.get("spans") or []
        kids = kids if isinstance(kids, list) else []
        if sort_children:
            try:
                kids = sorted(
                    kids,
                    key=lambda x: (_span_times_and_dur(x)[2] or 0.0),
                    reverse=True,
                )
            except Exception:
                pass
        return kids

    for s in spans:
        span_id = s.get("id")
        parent_span_id = s.get("parentSpanId")
        parent_trace_id = s.get("parentTraceId")
        if not isinstance(s, dict):
            continue

        name = _span_name(s)
        attrs = s.get("attributes") or s.get("metadata") or s.get("tags") or {}
        attr_text = ""
        if isinstance(attrs, dict) and attrs:
            try:
                # flatten attribute values for simple substring search
                attr_text = " ".join([str(k) + " " + str(v) for k, v in attrs.items()])
            except Exception:
                attr_text = str(attrs)

        # Filtering by name and attributes
        if nfilter and nfilter not in name.lower():
            continue
        if afilter and afilter not in attr_text.lower():
            continue

        start, end, dur = _span_times_and_dur(s)
        pct = None
        if total_ms and dur is not None and total_ms > 0:
            try:
                pct = max(0.0, min(1.0, float(dur) / float(total_ms)))
            except Exception:
                pct = None

        heat = _heat_emoji(pct)
        label = f"{'  ' * level}{heat} {name} • {_fmt_duration(dur, duration_unit)}"
        if pct is not None:
            label += f" ({int(pct * 100)}%)"

        # Expand based on chosen depth
        with st.expander(label, expanded=(level <= int(expanded_depth))):
            # Progress bar relative to total
            if pct is not None:
                st.progress(pct, text=f"{int(pct * 100)}% of overall trace")
            else:
                st.caption("No total duration available for relative progress")

            # Quick details row
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Duration", _fmt_duration(dur, duration_unit))

            def _fmt_edge(v: Optional[float]) -> str:
                if not isinstance(v, (int, float)):
                    return "—"
                return f"{v/1000:.2f} s" if duration_unit == "s" else f"{v:.0f} ms"

            with c2:
                st.metric("Start", _fmt_edge(start))
            with c3:
                st.metric("End", _fmt_edge(end))

            # Attributes / metadata
            if isinstance(attrs, dict) and attrs:
                with st.expander("Attributes", expanded=bool(show_attrs_default)):
                    st.json(attrs)

            # Children
            children = _children_for(s)
            if children:
                _render_spans(
                    children,
                    level=level + 1,
                    total_ms=total_ms,
                    name_filter=name_filter,
                    attr_filter=attr_filter,
                    sort_children=sort_children,
                    show_attrs_default=show_attrs_default,
                    expanded_depth=expanded_depth,
                    duration_unit=duration_unit,
                )


def render_trace_summary(data: Dict[str, Any], headers: Dict[str, Any]):
    # Heuristics for common OpenAI-style fields
    model = (
        (data.get("model") if isinstance(data, dict) else None)
        or (data.get("request", {}) if isinstance(data, dict) else {}).get("model")
        or headers.get("openai-model")
        or "—"
    )

    # Token usage: try OpenAI/Responses-like shapes
    usage = {}
    if isinstance(data, dict):
        usage = (
            data.get("usage")
            or data.get("token_usage")
            or (data.get("response", {}) if isinstance(data.get("response"), dict) else {}).get("usage")
            or {}
        )
    prompt_tokens = usage.get("prompt_tokens") or usage.get("inputTokens")
    completion_tokens = usage.get("completion_tokens") or usage.get("outputTokens")
    total_tokens = usage.get("totalTokens") or (
        (prompt_tokens or 0) + (completion_tokens or 0) if (prompt_tokens is not None and completion_tokens is not None) else None
    )

    # Latency: prefer header, else payload, else derive from spans if available
    latency_ms = headers.get("openai-processing-ms")
    if latency_ms is None and isinstance(data, dict):
        latency_ms = data.get("latency_ms")
    if latency_ms is None and isinstance(data, dict):
        # Derive from top-level spans if they have start/end
        spans = data.get("spans") or (data.get("trace", {}) or {}).get("spans") or (data.get("observability", {}) or {}).get("spans")
        try:
            if isinstance(spans, list) and spans:
                starts = []
                ends = []
                for s in spans:
                    if not isinstance(s, dict):
                        continue
                    stv = s.get("startTime") / 1000000000
                    etv = s.get("endTime") / 1000000000
                    if stv is not None:
                        starts.append(float(stv))
                    if etv is not None:
                        ends.append(float(etv))
                if starts and ends:
                    latency_ms = max(ends) - min(starts)
        except Exception:
            pass

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Model", model)
    with col2:
        st.metric("Latency", f"{float(latency_ms):.0f} ms" if latency_ms is not None else "—")
    with col3:
        st.metric("Prompt tokens", f"{int(prompt_tokens)}" if prompt_tokens is not None else "—")
    with col4:
        st.metric("Completion tokens", f"{int(completion_tokens)}" if completion_tokens is not None else "—")
    with col5:
        st.metric("Total tokens", f"{int(total_tokens)}" if total_tokens is not None else "—")


def render_traces_panel():
    st.markdown("### Traces & Spans")

    hist = st.session_state.get("trace_history") or []
    if not hist:
        st.caption("No traces yet.")
        return

    # Turn selector
    labels: List[str] = []
    for i, item in enumerate(hist):
        iid = (item.get("id") or (item.get("data") or {}).get("id") or "n/a")
        labels.append(f"{i+1}: id={iid}")
    idx = st.selectbox(
        "Turn",
        options=list(range(len(labels))),
        index=len(labels) - 1,
        format_func=lambda i: labels[i],
    )

    item = hist[idx]
    data = item.get("data") or {}
    headers = item.get("headers") or {}

    hdr_trace = headers.get("x-trace-id") or headers.get("x-request-id") or headers.get("openai-trace-id")
    if hdr_trace:
        st.caption(f"Header Trace: {hdr_trace}")

    # Summary metrics panel (model, latency, tokens)
    render_trace_summary(data, headers)

    # Try to locate spans in common shapes
    spans = None
    traces = None
    traceId = None
    if isinstance(data, dict):
        if isinstance(data.get("output"), list) and len(data.get("output")) > 0:
          outp = data.get("output")[0]
          if isinstance(outp.get("content"), list) and len(outp.get("content")) > 0:
            content_item = outp.get("content")[0]
            if isinstance(content_item, dict):
              traces = content_item.get("traces")
              if isinstance(traces, dict):
                traceId = traces.get("id")
                spans = traces.get("spans")
        elif isinstance(data.get("trace"), dict) and isinstance(data["trace"].get("spans"), list):
            spans = data["trace"]["spans"]
        elif isinstance(data.get("observability"), dict) and isinstance(data["observability"].get("spans"), list):
            spans = data["observability"]["spans"]

    # Compute total duration for relative progress (prefer spans window, else latency header)
    total_ms: Optional[float] = None
    try:
        if isinstance(spans, list) and spans:
            starts: List[float] = []
            ends: List[float] = []
            for s in spans:
                stv, etv, _ = _span_times_and_dur(s)
                if stv is not None:
                    starts.append(float(stv))
                if etv is not None:
                    ends.append(float(etv))
            if starts and ends:
                total_ms = max(ends) - min(starts)
        if total_ms is None:
            lm = headers.get("openai-processing-ms")
            if lm is None and isinstance(data, dict):
                lm = data.get("latency_ms")
            total_ms = float(lm) if lm is not None else None
    except Exception:
        total_ms = None

    if spans:
        st.markdown("#### Spans")

        # UI controls for the spans view
        c1, c2, c3 = st.columns(3)
        with c1:
            name_filter = st.text_input("Filter spans by name", value="")
            attr_filter = st.text_input("Filter spans by attributes", value="")
            view_mode = st.radio("View", options=["Tree", "Table"], index=0, horizontal=True)
        with c2:
            sort_children = st.checkbox("Sort children by duration", value=True)
            show_attrs_default = st.checkbox("Expand attributes by default", value=False)
        with c3:
            expanded_depth = st.slider("Default expanded depth", min_value=0, max_value=4, value=1)
            duration_unit = st.selectbox("Duration unit", options=["ms", "s"], index=0)

        st.caption("Span intensity legend: 🟩 fast • 🟨 moderate • 🟧 slow • 🟥 very slow")
        if view_mode == "Tree":
            # Build tree nodes for streamlit-tree-select (supports nested or flat span lists)
            nodes, span_index = _build_tree_nodes_for_select(
                trace=traces,
                spans=spans,
                total_ms=total_ms,
                sort_children=sort_children,
                name_filter=name_filter,
                attr_filter=attr_filter,
                duration_unit=duration_unit,
            )

            if not nodes:
                st.info("No spans match current filters.")
            else:
                selection = tree_select(
                    nodes,
                    key=f"span_tree_{idx}",
                )
                try:
                    if isinstance(selection, dict):
                        selected = selection.get("selected")
                        checked = selection.get("checked", [])
                    else:
                        selected = None
                        checked = selection or []
                except Exception:
                    selected = None
                    checked = []

                # Prefer a single selected node if available; otherwise use checked list
                ids_to_show = [selected] if selected else checked

                st.caption(f"Selected spans: {len(ids_to_show)}")
                for sid in ids_to_show:
                    s = span_index.get(str(sid)) or span_index.get(sid)
                    if not isinstance(s, dict):
                        continue
                    name = _span_display_name(s)
                    stv, etv, durv = _span_times_and_dur(s)
                    pct = None
                    if total_ms and durv is not None and total_ms > 0:
                        try:
                            pct = max(0.0, min(1.0, float(durv) / float(total_ms)))
                        except Exception:
                            pct = None
                    heat = _heat_emoji(pct)
                    st.subheader(f"{heat} {name}")
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.metric("Duration", _fmt_duration(durv, duration_unit))
                    def _fmt_edge(v: Optional[float]) -> str:
                        if not isinstance(v, (int, float)):
                            return "—"
                        return f"{v/1000:.2f} s" if duration_unit == "s" else f"{v:.0f} ms"
                    with c2:
                        st.metric("Start", _fmt_edge(stv))
                    with c3:
                        st.metric("End", _fmt_edge(etv))
                    attrs = s.get("attributes") or s.get("metadata") or s.get("tags") or {}
                    if isinstance(attrs, dict) and attrs:
                        with st.expander("Attributes", expanded=bool(show_attrs_default)):
                            st.json(attrs)
        else:
            # Flatten to a tabular view
            def _flatten(sp_list: List[dict], depth: int = 0, parent: str = "") -> List[Dict[str, Any]]:
                rows: List[Dict[str, Any]] = []
                for s in sp_list:
                    if not isinstance(s, dict):
                        continue
                    name = s.get("spanName") or "trace"
                    stv, etv, durv = _span_times_and_dur(s)
                    pct = (durv / total_ms * 100.0) if (durv is not None and total_ms) else None
                    attrs = s.get("attributes") or s.get("metadata") or s.get("tags") or {}
                    intensity = _heat_emoji((pct / 100.0) if isinstance(pct, (int, float)) else None)
                    row = {
                        "depth": depth,
                        "name": name,
                        "duration_s": round(durv, 2) if isinstance(durv, (int, float)) else None,
                        "percent_of_total": round(pct, 1) if isinstance(pct, (int, float)) else None,
                        "start_ms": round(stv, 2) if isinstance(stv, (int, float)) else None,
                        "end_ms": round(etv, 2) if isinstance(etv, (int, float)) else None,
                        "attributes_count": len(attrs) if isinstance(attrs, dict) else 0,
                        "parent": parent or "",
                        "intensity": intensity,
                    }
                    # Filters
                    if name_filter and name_filter.lower() not in name.lower():
                        pass
                    elif attr_filter and attr_filter.lower() not in str(attrs).lower():
                        pass
                    else:
                        rows.append(row)
                    # Recurse
                    kids = s.get("children") or s.get("spans") or []
                    kids = kids if isinstance(kids, list) else []
                    rows.extend(_flatten(kids, depth + 1, parent=name))
                return rows

            table_rows = _flatten(spans, 0, "")
            df = pd.DataFrame(table_rows)

            try:
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "percent_of_total": st.column_config.ProgressColumn(
                            "Percent of total",
                            format="%.1f%%",
                            min_value=0,
                            max_value=100,
                        ),
                        "intensity": st.column_config.Column("Intensity"),
                    },
                )
            except Exception:
                # Fallback without column_config (older Streamlit)
                st.dataframe(df, use_container_width=True)

            # Top spans summary
            with st.expander("Top spans by total duration", expanded=False):
                if not df.empty and "duration_s" in df and "name" in df:
                    agg = (
                        df[["name", "duration_s"]]
                        .dropna()
                        .groupby("name", as_index=False)["duration_s"]
                        .sum()
                        .sort_values("duration_s", ascending=False)
                        .head(10)
                    )
                    st.bar_chart(agg, x="name", y="duration_s", use_container_width=True)
                    st.caption("Aggregated duration across all occurrences of a span name in the selected turn.")

    else:
        st.caption("No spans found in response.")

    with st.expander("Raw trace payload", expanded=False):
        st.json(data)
    with st.expander("Response headers", expanded=False):
        st.code(headers, language="json")

# ============== Streamlit App ==============

st.set_page_config(page_title="Chat application", page_icon="", layout="wide")
st.title("Chat with your AIDP agent")
st.caption("Interactive chat with AIDP agent.")

# Lightweight theming for span expanders and copy buttons
st.markdown(
    """
    <style>
    /* Subtle card-like styling for expanders */
    div[data-testid="stExpander"] {
        border: 1px solid rgba(125,125,125,.15);
        border-radius: 10px;
        margin: 6px 0;
    }
    div[data-testid="stExpander"] > details > summary {
        border-radius: 10px;
        background: rgba(125, 125, 125, 0.06);
        padding: 8px 12px;
    }
    /* Copy button styling */
    .stButton > button {
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        transform: scale(1.05);
    }
    /* Code block styling */
    .stCodeBlock {
        margin: 1rem 0;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# -------- Session State Defaults --------
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "Hello! Ask me anything to start a chat session."}
    ]

if "last_response_id" not in st.session_state:
    st.session_state.last_response_id = None

if "client" not in st.session_state:
    st.session_state.client = None

if "client_params" not in st.session_state:
    st.session_state.client_params = {}

if "trace_history" not in st.session_state:
    st.session_state.trace_history = []

if "streaming_enabled" not in st.session_state:
    st.session_state.streaming_enabled = True

if "prompt_templates" not in st.session_state:
    # Load templates from file (works for both standalone and Docker)
    templates, source = load_templates_from_file()
    st.session_state.prompt_templates = templates
    st.session_state.templates_source = source

if "search_query" not in st.session_state:
    st.session_state.search_query = ""

if "search_filter" not in st.session_state:
    st.session_state.search_filter = "All"

if "selected_template" not in st.session_state:
    st.session_state.selected_template = None

if "send_template_trigger" not in st.session_state:
    st.session_state.send_template_trigger = False

# -------- Sidebar: Configuration & Controls --------
with st.sidebar:
    st.markdown("## Configuration")

    # Default to the URL used in the example file, but allow override via env
    DEFAULT_AIDP_URL = os.getenv(
        "AIDP_CHAT_URL",
        "",
    )
    aidp_url = st.text_input("AIDP Chat URL", value=DEFAULT_AIDP_URL, help="Full /chat endpoint URL")

    # OCI Configuration (for local mode only)
    st.markdown("#### OCI Authentication")
    st.caption("Leave blank for auto-detection. Container mode uses resource principal automatically.")

    config_file = st.text_input(
        "OCI Config File (optional)",
        value=os.getenv("OCI_CONFIG_FILE", ""),
        help="Leave blank to use default (~/.oci/config). Ignored when running in OCI Container Instance.",
    )
    config_profile = st.text_input(
        "OCI Profile",
        value=os.getenv("OCI_CONFIG_PROFILE", "DEFAULT"),
        help="Profile name from OCI config. Auth type (API key/security token) is auto-detected.",
    )

    st.markdown("#### Response Mode")
    st.session_state.streaming_enabled = st.checkbox(
        "Enable Streaming",
        value=st.session_state.streaming_enabled,
        help="Stream responses token-by-token for a more interactive experience."
    )

    col1, col2, = st.columns(2)

    with col1:
      init_clicked = st.button("Init Client", type="primary")
    with col2:
      new_chat = st.button("New Chat", help="Start a new chat: clears history and last_response_id")

    st.markdown("---")


    #if st.button("New Chat", help="Start a new chat: clears history and last_response_id"):
    if new_chat:
        st.session_state.messages = [
            {"role": "assistant", "content": "New chat started. How can I help?"}
        ]
        st.session_state.last_response_id = None
        st.session_state.trace_history = []
        st.rerun()

    st.markdown("### Status")

    # Show authentication mode
    if os.getenv('OCI_RESOURCE_PRINCIPAL_VERSION'):
        st.success("🔐 Auth Mode: Resource Principal (Container)")
    else:
        profile_name = config_profile.strip() if config_profile and config_profile.strip() else "DEFAULT"
        st.info(f"🔐 Auth Mode: OCI Profile ({profile_name})")
        st.caption("Auth type (API key/security token) is auto-detected from profile.")

    st.write(f"Last Response ID: {st.session_state.last_response_id or 'None'}")

    # Conversation stats
    num_messages = len(st.session_state.messages)
    num_user_msgs = sum(1 for m in st.session_state.messages if m["role"] == "user")
    num_assistant_msgs = sum(1 for m in st.session_state.messages if m["role"] == "assistant")

    stats_col1, stats_col2, stats_col3 = st.columns(3)
    with stats_col1:
        st.metric("Total", num_messages)
    with stats_col2:
        st.metric("User", num_user_msgs)
    with stats_col3:
        st.metric("Assistant", num_assistant_msgs)

    if aidp_url.strip() and not aidp_url.strip().endswith("/chat"):
        st.warning("The URL does not end with /chat. Ensure you are pointing to the /chat endpoint.")

    if st.session_state.client is not None:
        with st.expander("Active client params", expanded=False):
            st.code(st.session_state.client_params, language="json")

    st.markdown("---")

    # Prompt Templates
    st.markdown("### 📝 Prompt Templates")

    # Help text with link to guide
    with st.expander("ℹ️ How to use templates", expanded=False):
        st.markdown("""
        **Quick Guide:**
        1. Select a template from dropdown
        2. Fill in the variables
        3. Click "Use Template"
        4. Click "Send" to submit

        **Tips:**
        - Be specific with your data
        - Include units ($, %, etc.)
        - Ask follow-up questions

        📚 See [GETTING_STARTED.md](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/ai/agent-flows/misc/invoke-agent-flows-from-streamlit/GETTING_STARTED.md) for detailed examples!
        """)

    # Show templates source and reload button
    source_col1, source_col2 = st.columns([0.7, 0.3])
    with source_col1:
        source = st.session_state.get("templates_source", "unknown")
        if source == "built-in defaults":
            st.caption("📦 Using built-in templates")
        else:
            st.caption(f"📁 Loaded from: `{source}`")
    with source_col2:
        if st.button("🔄 Reload", key="reload_templates", help="Reload templates from file", use_container_width=True):
            templates, source = load_templates_from_file()
            st.session_state.prompt_templates = templates
            st.session_state.templates_source = source
            st.success(f"Reloaded {len(templates)} templates")
            st.rerun()

    template_names = list(st.session_state.prompt_templates.keys())

    selected_template = st.selectbox(
        "Choose a template",
        options=["-- Select Template --"] + template_names,
        key="template_selector"
    )

    if selected_template != "-- Select Template --":
        template_text = st.session_state.prompt_templates[selected_template]

        # Extract variables from template (e.g., {text}, {code})
        variables = re.findall(r'\{(\w+)\}', template_text)

        if variables:
            st.caption("Fill in template variables:")
            variable_values = {}
            for var in variables:
                variable_values[var] = st.text_area(
                    f"{var}:",
                    height=80,
                    key=f"template_var_{var}_{selected_template}"
                )

            if st.button("📤 Use Template", use_container_width=True):
                # Replace variables in template
                filled_template = template_text
                for var, value in variable_values.items():
                    filled_template = filled_template.replace(f"{{{var}}}", value)

                st.session_state.selected_template = filled_template
                st.rerun()
        else:
            if st.button("📤 Use Template", use_container_width=True):
                st.session_state.selected_template = template_text
                st.rerun()

    # Add new template
    with st.expander("➕ Add New Template"):
        new_template_name = st.text_input("Template Name")
        new_template_text = st.text_area(
            "Template Text (use {variable} for placeholders)",
            height=100,
            placeholder="Example: Explain {concept} in simple terms"
        )
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Save", use_container_width=True):
                if new_template_name and new_template_text:
                    st.session_state.prompt_templates[new_template_name] = new_template_text
                    st.success(f"Saved '{new_template_name}'")
                    st.rerun()
                else:
                    st.error("Name and text required")
        with col2:
            if st.button("🗑️ Delete Selected", use_container_width=True):
                if selected_template != "-- Select Template --" and selected_template in st.session_state.prompt_templates:
                    del st.session_state.prompt_templates[selected_template]
                    st.success(f"Deleted '{selected_template}'")
                    st.rerun()

    st.markdown("---")

    # Export functionality
    st.markdown("### Export Conversation")
    export_col1, export_col2 = st.columns(2)

    with export_col1:
        if st.button("📄 Export as JSON", help="Download conversation as JSON", use_container_width=True):
            json_data = export_conversation_as_json()
            st.download_button(
                label="⬇️ Download JSON",
                data=json_data,
                file_name=f"conversation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json",
                use_container_width=True
            )

    with export_col2:
        if st.button("📝 Export as Markdown", help="Download conversation as Markdown", use_container_width=True):
            md_data = export_conversation_as_markdown()
            st.download_button(
                label="⬇️ Download MD",
                data=md_data,
                file_name=f"conversation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                mime="text/markdown",
                use_container_width=True
            )

    st.markdown("---")
    render_traces_panel()

# -------- Client Initialization --------

def build_client(aidp_url: str, profile: str, cfg_file: str) -> Optional[AIDPChatClient]:
    """
    Build AIDP chat client with automatic authentication detection.
    - In OCI Container Instance: uses resource principal
    - Locally: uses OCI profile (API key or security token auto-detected)
    """
    try:
        config_file_path = cfg_file.strip() if cfg_file and cfg_file.strip() else None
        profile_name = profile.strip() if profile and profile.strip() else "DEFAULT"

        client = AIDPChatClient(aidp_url, config_file=config_file_path, profile=profile_name)
        return client
    except Exception as e:
        st.error(f"Failed to initialize AIDP client. Check OCI credentials and URL.\n{e}")
        return None


def ensure_client_ready():
    desired_params = {
        "aidp_url": aidp_url.strip(),
        "profile": config_profile.strip(),
        "cfg_file": (config_file or "").strip(),
    }
    # Rebuild client if missing
    client = build_client(**desired_params)
    if client:
            st.session_state.client = client
            st.session_state.client_params = desired_params


if init_clicked:
    st.session_state.messages = [
        {"role": "assistant", "content": "New chat started. How can I help?"}
    ]
    st.session_state.last_response_id = None
    st.session_state.trace_history = []
    ensure_client_ready()
    st.rerun()

# -------- Main Chat Pane --------

# Helper functions for search
def matches_search(msg, query, role_filter):
    """Check if message matches search criteria."""
    if role_filter != "All":
        if msg["role"] != role_filter.lower():
            return False

    if query:
        return query.lower() in msg["content"].lower()

    return True

def highlight_text(text, query):
    """Highlight matching text in message."""
    if not query:
        return text

    # Case-insensitive replace with highlighting
    # Using markdown highlighting with backticks and bold
    pattern = re.compile(re.escape(query), re.IGNORECASE)
    highlighted = pattern.sub(lambda m: f"**`{m.group(0)}`**", text)
    return highlighted

# Search and Filter functionality
with st.expander("🔍 Search & Filter Messages", expanded=bool(st.session_state.search_query or st.session_state.search_filter != "All")):
    search_col1, search_col2, search_col3 = st.columns([0.5, 0.25, 0.25])

    with search_col1:
        search_query = st.text_input(
            "Search messages",
            value=st.session_state.search_query,
            placeholder="Type to search (case-insensitive)...",
            key="search_input",
            help="Search through all messages in the conversation"
        )
        st.session_state.search_query = search_query

    with search_col2:
        search_filter = st.selectbox(
            "Filter by role",
            options=["All", "User", "Assistant"],
            index=["All", "User", "Assistant"].index(st.session_state.search_filter),
            key="role_filter",
            help="Filter messages by sender"
        )
        st.session_state.search_filter = search_filter

    with search_col3:
        if st.button("🔄 Clear", use_container_width=True, help="Clear all filters"):
            st.session_state.search_query = ""
            st.session_state.search_filter = "All"
            st.rerun()

    # Count matching messages
    matching_messages = [msg for msg in st.session_state.messages if matches_search(msg, search_query, search_filter)]
    if search_query or search_filter != "All":
        st.success(f"✅ Found {len(matching_messages)} of {len(st.session_state.messages)} message(s)")

# Render chat history
for idx, msg in enumerate(st.session_state.messages):
    # Apply search filter
    if not matches_search(msg, search_query, search_filter):
        continue

    with st.chat_message("user" if msg["role"] == "user" else "assistant"):
        # Display assistant messages with copy buttons and syntax highlighting
        if msg["role"] == "assistant":
            # Highlight search terms in assistant messages
            highlighted_content = highlight_text(msg["content"], search_query)
            render_message_with_copy(highlighted_content, msg["role"], idx)
        else:
            # Highlight search terms in user messages
            highlighted_content = highlight_text(msg["content"], search_query)
            st.markdown(highlighted_content)

# Show selected template preview with send button
if st.session_state.selected_template:
    st.info("✨ Template ready! Click 'Send Template' or type your own message:")
    template_col1, template_col2, template_col3 = st.columns([0.7, 0.15, 0.15])
    with template_col1:
        st.code(st.session_state.selected_template, language=None)
    with template_col2:
        if st.button("📤 Send", key="send_template", help="Send template as message", use_container_width=True):
            st.session_state.send_template_trigger = True
            st.rerun()
    with template_col3:
        if st.button("❌ Clear", key="clear_template", help="Clear template", use_container_width=True):
            st.session_state.selected_template = None
            st.rerun()

user_input = st.chat_input("Type your message")

# Handle template send trigger
if st.session_state.send_template_trigger and st.session_state.selected_template:
    user_input = st.session_state.selected_template
    st.session_state.selected_template = None
    st.session_state.send_template_trigger = False

if user_input:
    # Display user message immediately
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.write(user_input)

    # Ensure client is ready (lazy init if user didn't press the button)
    ensure_client_ready()
    if st.session_state.client is None:
        with st.chat_message("assistant"):
            st.error("Client is not initialized. Check sidebar settings and click 'Initialize/Update Client'.")
    else:
        try:
            if st.session_state.streaming_enabled:
                # ============ STREAMING MODE ============
                with st.chat_message("assistant"):
                    # Create a placeholder for streaming content
                    message_placeholder = st.empty()
                    full_response = ""

                    # Create streaming response object
                    stream = chat_stream(
                        st.session_state.client,
                        user_input,
                        st.session_state.last_response_id
                    )

                    # Collect chunks and display progressively
                    for chunk in stream:
                        full_response += chunk
                        message_placeholder.markdown(full_response + "▌")

                    # Final update without cursor
                    message_placeholder.empty()

                # Render final response with copy button and syntax highlighting
                render_message_with_copy(full_response, "assistant", len(st.session_state.messages))

                # Get the complete response data after streaming completes
                response_data = stream.get_response_data()

                # Capture trace history
                st.session_state.trace_history.append({
                    "id": response_data.get("id"),
                    "data": response_data,
                    "headers": response_data.get("headers", {})
                })

                # Update history
                st.session_state.messages.append({"role": "assistant", "content": full_response})

                # Update last_response_id for subsequent turns
                st.session_state.last_response_id = response_data.get("id")

            else:
                # ============ NON-STREAMING MODE ============
                # First call passes None; subsequent calls pass last_response_id
                resp = chat(st.session_state.client, user_input, st.session_state.last_response_id)

                # Extract and show assistant markdown
                assistant_md = response_to_markdown(resp) or "(no content)"
                with st.chat_message("assistant"):
                    render_message_with_copy(assistant_md, "assistant", len(st.session_state.messages))

                # Capture raw response JSON and headers for traces
                try:
                    _data = resp.json()
                except Exception:
                    _data = {}
                st.session_state.trace_history.append({
                    "id": _data.get("id"),
                    "data": _data,
                    "headers": dict(getattr(resp, "headers", {}))
                })

                # Update history
                st.session_state.messages.append({"role": "assistant", "content": assistant_md})

                # Update last_response_id for subsequent turns
                st.session_state.last_response_id = get_last_response_id(resp)

        except requests.exceptions.HTTPError as he:
            with st.chat_message("assistant"):
                r = he.response
                if r is not None:
                    st.error(f"HTTP {r.status_code} error")
                    st.write("Response headers:")
                    st.code(dict(r.headers), language="json")
                    st.write("Response body:")
                    st.code(r.text, language="json")
                else:
                    st.error(f"HTTP error: {he}")
        except Exception as e:
            with st.chat_message("assistant"):
                st.error(f"Request failed: {e}")

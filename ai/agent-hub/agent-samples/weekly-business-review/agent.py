from __future__ import annotations

import inspect
from importlib import metadata as importlib_metadata
import json
import logging
import os
import re
import uuid
from dataclasses import dataclass
from typing import Any

try:
    from langchain_core.messages import AIMessage, HumanMessage
except ModuleNotFoundError:
    @dataclass
    class AIMessage:
        content: str

    @dataclass
    class HumanMessage:
        content: str


logger = logging.getLogger("weekly_business_review_sqltool")
checkpointer = globals().get("checkpointer", None)

TENANCY_OCID = os.getenv("OCI_TENANCY_OCID", "").strip()
OCI_ENDPOINT = os.getenv("OCI_INFERENCE_ENDPOINT", "").strip()
MODEL_ID = os.getenv("OCI_MODEL_ID", "").strip()
CATALOG_KEY = os.getenv("WBR_CATALOG_KEY", "").strip()
SCHEMA_KEY = os.getenv("WBR_SCHEMA_KEY", "").strip()
A2UI_VERSION = "0.9"
DEFAULT_CATALOG_ID = "/a2ui_specification/2.0.0/agent_hub_a2ui_custom_component_catalog.json"
ROOT_ID = "root"
ALLOWED_TEMPLATES = {
    "weekly_business_review_summary",
    "pipeline_by_stage",
    "renewal_risk_accounts",
    "product_usage_by_account",
    "support_health_by_account",
}
ALLOWED_REGIONS = ["North America", "EMEA", "APAC"]
ALLOWED_SEGMENTS = ["Enterprise", "Commercial", "Strategic"]
ALLOWED_SCREEN_TYPES = {"overview", "pipeline", "renewal_risk", "usage", "support", "no_results"}
ALLOWED_CHART_TYPES = {"line", "bar", "area", "pie"}
CHART_COLORS = ["#F97316", "#0EA5E9", "#10B981", "#EAB308", "#EF4444"]
DEFAULT_FILTERS = {
    "start_date": "2026-07-27",
    "end_date": "2026-08-02",
    "region": "North America",
    "segment": "Enterprise",
}
MIN_SCHEMA_DEPENDENCIES = {
    "jsonschema": (4, 18, 0),
    "referencing": (0, 30, 0),
}


def pre_invoke_setup(**kwargs: Any) -> Any:
    """Forward Agent Hub session setup when the AIDP helper package is present."""

    try:
        from aidputils.agents.toolkit.agent_helper import pre_invoke_setup as aidp_pre_invoke_setup
    except ModuleNotFoundError:
        return None
    return aidp_pre_invoke_setup(**kwargs)


def build_llm() -> Any:
    """Build the OCI-backed planner used by the deployed WBR agent."""

    required_configuration = {
        "OCI_TENANCY_OCID": TENANCY_OCID,
        "OCI_INFERENCE_ENDPOINT": OCI_ENDPOINT,
        "OCI_MODEL_ID": MODEL_ID,
    }
    missing = [name for name, value in required_configuration.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing required OCI model configuration: " + ", ".join(missing)
        )

    from aidputils.agents.toolkit.agent_helper import init_oci_llm
    from aidputils.agents.toolkit.configs import OCIAIConf

    llm_conf = OCIAIConf(
        model_provider="generic",
        compartment_id=TENANCY_OCID,
        model_args={},
        endpoint=OCI_ENDPOINT,
        model_id=MODEL_ID,
        guardrails_config={
            "name": "Default Guardrails",
            "description": "Default empty guardrails configuration",
            "policies": [],
        },
    )
    llm = init_oci_llm(llm_conf)
    if llm is None:
        raise RuntimeError("init_oci_llm returned None.")
    return llm


def _parameter(name: str, description: str, default: str) -> dict[str, str]:
    return {
        "name": name,
        "type": "string",
        "description": description,
        "defaultValue": default,
    }


def _query_parameters() -> list[dict[str, str]]:
    return [
        _parameter("start_date", "Inclusive period start in YYYY-MM-DD format", DEFAULT_FILTERS["start_date"]),
        _parameter("end_date", "Inclusive period end in YYYY-MM-DD format", DEFAULT_FILTERS["end_date"]),
        _parameter("region", f"Exact region: {', '.join(ALLOWED_REGIONS)}", DEFAULT_FILTERS["region"]),
        _parameter("segment", f"Exact segment: {', '.join(ALLOWED_SEGMENTS)}", DEFAULT_FILTERS["segment"]),
    ]


def _sql_tool(name: str, description: str, query: str, params: list[dict[str, str]]) -> Any:
    from aidputils.agents.toolkit.configs import AIDPToolConf
    from aidputils.agents.toolkit.tool_helper import create_langgraph_tool

    configuration = AIDPToolConf(
        name=name,
        description=description,
        tool_class="SQLTool",
        conf={"catalogKey": CATALOG_KEY, "schemaKey": SCHEMA_KEY, "query": query},
        params=params,
    )
    return create_langgraph_tool(configuration.model_dump())


def _latest_snapshot_where(table_name: str, row_alias: str) -> str:
    """Return a bounded filter that selects one latest complete weekly snapshot."""

    return (
        f"{row_alias}.week_start = (SELECT MAX(latest_row.week_start) FROM {table_name} latest_row "
        "WHERE latest_row.week_start >= TO_DATE({{start_date}}, 'YYYY-MM-DD') "
        "AND latest_row.week_end <= TO_DATE({{end_date}}, 'YYYY-MM-DD') "
        "AND latest_row.region = {{region}} AND latest_row.segment = {{segment}}) "
        f"AND {row_alias}.region = {{{{region}}}} AND {row_alias}.segment = {{{{segment}}}} "
    )


def build_tools() -> list[Any]:
    if not CATALOG_KEY:
        raise RuntimeError("WBR_CATALOG_KEY must be configured for this deployment.")
    if not SCHEMA_KEY:
        raise RuntimeError("WBR_SCHEMA_KEY must be configured for this deployment.")
    params = _query_parameters()
    return [
        _sql_tool(
            "list_wbr_scopes",
            "List available date ranges, regions, and segments. Use when requested filters return no rows or the user asks what data is available.",
            "SELECT region, segment, MIN(week_start) AS first_week, MAX(week_end) AS last_week "
            "FROM weekly_business_metrics GROUP BY region, segment ORDER BY region, segment "
            "FETCH FIRST 30 ROWS ONLY",
            [],
        ),
        _sql_tool(
            "get_weekly_business_summary",
            "Return executive WBR KPIs from the latest complete weekly snapshot inside a bounded date range, region, and segment. For a full WBR, call this and all four detail tools before synthesizing insights.",
            "SELECT summary_row.region, summary_row.segment, summary_row.week_start AS start_date, "
            "summary_row.week_end AS end_date, summary_row.active_accounts, summary_row.pipeline_usd, "
            "summary_row.renewal_risk_usd, summary_row.product_active_users, summary_row.support_sla_pct, "
            "summary_row.open_escalations, summary_row.nps FROM weekly_business_metrics summary_row WHERE "
            + _latest_snapshot_where("weekly_business_metrics", "summary_row")
            + "FETCH FIRST 1 ROW ONLY",
            params,
        ),
        _sql_tool(
            "get_pipeline_by_stage",
            "Return dated opportunity counts and pipeline value by stage from the latest complete weekly snapshot inside the requested WBR scope.",
            "SELECT pipeline_row.week_start, pipeline_row.week_end, pipeline_row.stage, "
            "pipeline_row.pipeline_usd, pipeline_row.opportunity_count FROM pipeline_by_stage pipeline_row WHERE "
            + _latest_snapshot_where("pipeline_by_stage", "pipeline_row")
            + "ORDER BY pipeline_row.pipeline_usd DESC FETCH FIRST 12 ROWS ONLY",
            params,
        ),
        _sql_tool(
            "get_renewal_risk_accounts",
            "Return dated renewal-risk accounts, ARR exposure, risk reason, owner, and days to renewal from the latest complete weekly snapshot inside the requested scope.",
            "SELECT renewal_row.week_start, renewal_row.week_end, renewal_row.account_id, "
            "renewal_row.account_name, renewal_row.arr_usd, renewal_row.risk_level, renewal_row.risk_reason, "
            "renewal_row.days_to_renewal, renewal_row.owner FROM renewal_risk_accounts renewal_row WHERE "
            + _latest_snapshot_where("renewal_risk_accounts", "renewal_row")
            + "ORDER BY renewal_row.arr_usd DESC FETCH FIRST 12 ROWS ONLY",
            params,
        ),
        _sql_tool(
            "get_product_usage_by_account",
            "Return dated active users, workflow runs, feature adoption, and usage movement by account from the latest complete weekly snapshot inside the requested scope.",
            "SELECT usage_row.week_start, usage_row.week_end, usage_row.account_id, usage_row.account_name, "
            "usage_row.active_users, usage_row.workflow_runs, usage_row.feature_adoption_pct, "
            "usage_row.usage_delta_pct FROM product_usage_by_account usage_row WHERE "
            + _latest_snapshot_where("product_usage_by_account", "usage_row")
            + "ORDER BY usage_row.usage_delta_pct ASC FETCH FIRST 12 ROWS ONLY",
            params,
        ),
        _sql_tool(
            "get_support_health_by_account",
            "Return dated ticket volume, priority cases, SLA breaches, response time, and open escalations by account from the latest complete weekly snapshot inside the requested scope.",
            "SELECT support_row.week_start, support_row.week_end, support_row.account_id, "
            "support_row.account_name, support_row.tickets_opened, support_row.p1_p2_tickets, "
            "support_row.sla_breaches, support_row.avg_response_hours, support_row.open_escalations "
            "FROM support_health_by_account support_row WHERE "
            + _latest_snapshot_where("support_health_by_account", "support_row")
            + "ORDER BY support_row.open_escalations DESC FETCH FIRST 12 ROWS ONLY",
            params,
        ),
    ]


def _create_react_agent(model: Any, tools: list[Any]) -> Any:
    from langgraph.prebuilt import create_react_agent

    options: dict[str, Any] = {
        "model": model,
        "tools": tools,
        "prompt": build_system_prompt(),
        "debug": True,
    }
    if checkpointer:
        options["checkpointer"] = checkpointer
    try:
        return create_react_agent(**options)
    except Exception:
        if "checkpointer" not in options:
            raise
        logger.warning("Checkpointer initialization failed; retrying without it.", exc_info=True)
        options.pop("checkpointer", None)
        return create_react_agent(**options)


def response_text(response: Any) -> str:
    if isinstance(response, str):
        return response.strip()
    if isinstance(response, dict):
        messages = response.get("messages")
        if isinstance(messages, list) and messages:
            return response_text(messages[-1])
        content = response.get("content")
        if isinstance(content, str):
            return content.strip()
    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts).strip()
    return ""


def extract_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    start = text.find("{")
    if start < 0:
        raise ValueError("Response does not contain a JSON object.")
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                parsed = json.loads(re.sub(r",(?=\s*[}\]])", "", text[start : index + 1]))
                if not isinstance(parsed, dict):
                    raise ValueError("Response plan must be a JSON object.")
                return parsed
    raise ValueError("Response contains incomplete JSON.")


def _required_text(value: Any, field: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string.")
    cleaned = value.strip()
    if len(cleaned) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters.")
    return cleaned


def validate_response_plan(plan: dict[str, Any]) -> dict[str, Any]:
    mode = plan.get("mode")
    if mode not in {"text", "a2ui"}:
        raise ValueError("mode must be text or a2ui.")
    _required_text(plan.get("message"), "message", 700 if mode == "text" else 500)
    if mode == "text":
        return plan

    screen = plan.get("screen")
    if not isinstance(screen, dict) or screen.get("type") not in ALLOWED_SCREEN_TYPES:
        raise ValueError("A2UI plan requires a supported screen.type.")
    _required_text(screen.get("title"), "screen.title", 100)
    if screen.get("subtitle") is not None:
        _required_text(screen["subtitle"], "screen.subtitle", 180)

    filters = screen.get("filters")
    if not isinstance(filters, dict) or set(filters) != set(DEFAULT_FILTERS):
        raise ValueError("screen.filters must contain start_date, end_date, region, and segment.")
    if filters["region"] not in ALLOWED_REGIONS or filters["segment"] not in ALLOWED_SEGMENTS:
        raise ValueError("screen.filters contains an unsupported region or segment.")
    for field in ("start_date", "end_date"):
        if not isinstance(filters[field], str) or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", filters[field]):
            raise ValueError(f"screen.filters.{field} must use YYYY-MM-DD.")
    if filters["start_date"] > filters["end_date"]:
        raise ValueError("screen.filters.start_date must not be after end_date.")

    metrics = screen.get("metrics", [])
    if not isinstance(metrics, list) or len(metrics) > 4:
        raise ValueError("screen.metrics must contain at most four items.")
    for index, metric in enumerate(metrics):
        if not isinstance(metric, dict):
            raise ValueError(f"metrics[{index}] must be an object.")
        _required_text(metric.get("label"), f"metrics[{index}].label", 50)
        _required_text(str(metric.get("value", "")), f"metrics[{index}].value", 40)
        if metric.get("caption") is not None:
            _required_text(metric["caption"], f"metrics[{index}].caption", 100)

    chart = screen.get("chart")
    if chart is not None:
        if not isinstance(chart, dict) or chart.get("type") not in ALLOWED_CHART_TYPES:
            raise ValueError("screen.chart has an unsupported chart type.")
        _required_text(chart.get("title"), "screen.chart.title", 100)
        items = chart.get("items")
        if not isinstance(items, list) or not items or len(items) > 18:
            raise ValueError("screen.chart.items must contain 1 to 18 items.")
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(f"chart.items[{index}] must be an object.")
            _required_text(item.get("label"), f"chart.items[{index}].label", 60)
            if not isinstance(item.get("value"), (int, float)) or isinstance(item.get("value"), bool):
                raise ValueError(f"chart.items[{index}].value must be numeric.")
            if item.get("series") is not None:
                _required_text(item["series"], f"chart.items[{index}].series", 60)

    table = screen.get("table")
    if table is not None:
        if not isinstance(table, dict):
            raise ValueError("screen.table must be an object.")
        _required_text(table.get("title"), "screen.table.title", 100)
        columns = table.get("columns")
        rows = table.get("rows")
        if not isinstance(columns, list) or not columns or len(columns) > 6:
            raise ValueError("screen.table.columns must contain 1 to 6 columns.")
        if not isinstance(rows, list) or len(rows) > 12:
            raise ValueError("screen.table.rows must contain at most 12 rows.")
        keys = []
        for index, column in enumerate(columns):
            if not isinstance(column, dict):
                raise ValueError(f"table.columns[{index}] must be an object.")
            key = _required_text(column.get("key"), f"table.columns[{index}].key", 40)
            if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", key):
                raise ValueError(f"Invalid table column key: {key}.")
            keys.append(key)
            _required_text(column.get("label"), f"table.columns[{index}].label", 60)
        for index, row in enumerate(rows):
            if not isinstance(row, dict) or not isinstance(row.get("id"), (str, int)):
                raise ValueError(f"table.rows[{index}] requires id.")
            for key in keys:
                if key in row and not isinstance(row[key], (str, int, float, bool)):
                    raise ValueError(f"table.rows[{index}].{key} must be scalar.")

    for field, limit in (("insights", 4), ("recommendations", 4)):
        sections = screen.get(field, [])
        if not isinstance(sections, list) or len(sections) > limit:
            raise ValueError(f"screen.{field} must contain at most {limit} items.")
        for index, section in enumerate(sections):
            if not isinstance(section, dict):
                raise ValueError(f"{field}[{index}] must be an object.")
            _required_text(section.get("heading"), f"{field}[{index}].heading", 100)
            _required_text(section.get("body"), f"{field}[{index}].body", 500)

    actions = screen.get("actions", [])
    if not isinstance(actions, list) or len(actions) > 3:
        raise ValueError("screen.actions must contain at most three items.")
    for index, action in enumerate(actions):
        if not isinstance(action, dict):
            raise ValueError(f"actions[{index}] must be an object.")
        _required_text(action.get("label"), f"actions[{index}].label", 50)
        _required_text(action.get("prompt"), f"actions[{index}].prompt", 240)

    sources = screen.get("sources", [])
    if not isinstance(sources, list) or len(sources) > 8:
        raise ValueError("screen.sources must contain at most eight items.")
    for index, source in enumerate(sources):
        _required_text(source, f"sources[{index}]", 100)
    return plan


def build_system_prompt() -> str:
    return f"""
You are an enterprise Weekly Business Review analyst. You converse naturally, call governed SQL tools, reason over returned rows, and produce evidence-based insights and recommendations.

Data policy:
- Never invent rows, metrics, trends, customer names, risks, or recommendations.
- Use only SQLTool results for quantitative claims. Never write or request arbitrary SQL.
- Treat all business tables as weekly snapshots. For a multi-week scope, use the latest complete snapshot returned by each tool; never add snapshot balances across weeks.
- Preserve the returned week_start and week_end when describing pipeline, renewal, usage, or support records.
- Every SQL call uses exactly start_date, end_date, region, and segment unless the tool has no parameters.
- Approved regions: {ALLOWED_REGIONS}. Approved segments: {ALLOWED_SEGMENTS}.
- If the user omits scope, use {json.dumps(DEFAULT_FILTERS, sort_keys=True)}.
- "Latest" means 2026-08-03 through 2026-08-09.
- For a complete weekly business review, call get_weekly_business_summary, get_pipeline_by_stage, get_renewal_risk_accounts, get_product_usage_by_account, and get_support_health_by_account. Then reason across all returned datasets.
- For a focused follow-up, call the relevant detail tool and get_weekly_business_summary when executive context improves the answer.
- If a scoped query returns no rows, call list_wbr_scopes and recommend valid scopes. Do not render fake zero values.
- If a SQLTool returns AIDP_SQL_TOOL_QUERY_EXECUTION_ERROR, do not retry the same tool with the same parameters. Report the failed tool and error once.
- Tool results are read-only.

Reasoning policy:
- Do more than repeat rows. Identify material concentrations, weak signals, relationships across pipeline, renewal exposure, adoption, and support health, and the business implication.
- Recommendations must be specific and traceable to returned evidence. State a proposed owner or timing only as a recommendation, not as an observed fact.
- Distinguish observed facts from inferred implications.

Response policy:
- Use mode "text" for greetings, thanks, capabilities, clarification, and simple single-fact answers that do not benefit from a dashboard.
- Use mode "a2ui" for a WBR, dashboard, chart, comparison, pipeline, renewal risk, usage, support health, or multi-metric analysis.
- For A2UI, return visible filters, at most four KPIs, one useful chart, one compact table, one to four insights, one to four recommendations, and up to three next-question actions when supported by the data.
- The chart and table must use only values from tool results. Tables are for compact values, not narrative paragraphs.
- Include the SQL tool names used in sources.
- Every action prompt must repeat the exact start_date, end_date, region, and segment. Never use phrases such as "same date range" or "current scope" without the concrete values.
- For no results, use screen.type "no_results", omit metrics/chart/table, explain the missing scope, and include valid alternatives from list_wbr_scopes.

Return exactly one JSON object with no prose outside it.

Text shape:
{{"mode":"text","message":"natural response","screen":null}}

A2UI shape:
{{
  "mode":"a2ui",
  "message":"one or two sentence executive takeaway, maximum 500 characters",
  "screen":{{
    "type":"overview | pipeline | renewal_risk | usage | support | no_results",
    "title":"short title",
    "subtitle":"scope or business context",
    "filters":{{"start_date":"2026-07-27","end_date":"2026-08-02","region":"North America","segment":"Enterprise"}},
    "metrics":[{{"label":"Pipeline","value":"$8.4M","caption":"useful context only"}}],
    "chart":{{"title":"Pipeline by stage","type":"bar | line | area | pie","x_axis":"Stage","y_axis":"Pipeline USD","items":[{{"label":"Proposal","value":1200000,"series":"Pipeline"}}]}},
    "table":{{"title":"Priority accounts","columns":[{{"key":"account","label":"Account"}}],"rows":[{{"id":"row-1","account":"Example"}}]}},
    "insights":[{{"heading":"Observed signal","body":"Evidence-based interpretation with values."}}],
    "recommendations":[{{"heading":"Recommended action","body":"Specific action tied to evidence; proposed owner and timing where useful."}}],
    "actions":[{{"label":"Review renewal risk","prompt":"Show renewal risk for North America Enterprise from 2026-07-27 to 2026-08-02"}}],
    "sources":["get_weekly_business_summary","get_pipeline_by_stage"]
  }}
}}
""".strip()


def _path(path: str) -> dict[str, str]:
    return {"path": path}


def _component(component_id: str, name: str, props: dict[str, Any], weight: int | float | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {"id": component_id, "component": name}
    item.update(props)
    if weight is not None:
        item["weight"] = weight
    return item


def _text(component_id: str, value: str, variant: str = "body") -> dict[str, Any]:
    return _component(component_id, "Text", {"text": value, "variant": variant})


def _column(component_id: str, children: list[str], weight: int | float | None = None) -> dict[str, Any]:
    return _component(component_id, "Column", {"children": children, "justify": "start", "align": "stretch"}, weight)


def _row(component_id: str, children: list[str], distribution: str = "spaceBetween") -> dict[str, Any]:
    return _component(component_id, "Row", {"children": children, "justify": distribution, "align": "stretch"})


def _card(component_id: str, child: str, weight: int | float | None = None) -> dict[str, Any]:
    return _component(component_id, "Card", {"child": child}, weight)


def _event_context(context: dict[str, Any] | list[dict[str, Any]] | None) -> dict[str, Any]:
    if isinstance(context, dict):
        return dict(context)
    event_context: dict[str, Any] = {}
    for item in context or []:
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            continue
        value = item.get("value")
        if isinstance(value, dict) and set(value) == {"literalString"}:
            value = value["literalString"]
        event_context[item["key"]] = value
    return event_context


def _button(component_id: str, label_id: str, label: str, name: str, context: dict[str, Any] | list[dict[str, Any]], primary: bool = False) -> list[dict[str, Any]]:
    return [
        _component(
            component_id,
            "Button",
            {
                "child": label_id,
                "variant": "primary" if primary else "default",
                "action": {"event": {"name": name, "context": _event_context(context)}},
            },
        ),
        _text(label_id, label),
    ]


def _filter_context(filters: dict[str, str], template_id: str) -> dict[str, Any]:
    return {
        "template_id": template_id,
        "start_date": _path("/wbrFilters/start_date"),
        "end_date": _path("/wbrFilters/end_date"),
        "region": _path("/wbrFilters/region_selection/0"),
        "segment": _path("/wbrFilters/segment_selection/0"),
        "current_scope": json.dumps(filters, sort_keys=True),
    }


def _template_for_screen(screen_type: str) -> str:
    return {
        "overview": "weekly_business_review_summary",
        "pipeline": "pipeline_by_stage",
        "renewal_risk": "renewal_risk_accounts",
        "usage": "product_usage_by_account",
        "support": "support_health_by_account",
        "no_results": "weekly_business_review_summary",
    }[screen_type]


def _scoped_follow_up(prompt: str, filters: dict[str, str]) -> dict[str, str]:
    base = prompt.strip().rstrip(".")
    base = re.sub(
        r"\s+for (?:the )?(?:same date range(?:, region,? and segment)?|current scope)$",
        "",
        base,
        flags=re.IGNORECASE,
    )
    if len(base) > 120:
        base = base[:117].rstrip() + "..."
    scoped_prompt = (
        f"{base}. Scope: start_date={filters['start_date']}, end_date={filters['end_date']}, "
        f"region={filters['region']}, segment={filters['segment']}."
    )
    return {
        "prompt": scoped_prompt,
        "start_date": filters["start_date"],
        "end_date": filters["end_date"],
        "region": filters["region"],
        "segment": filters["segment"],
    }


def _catalog_id() -> str:
    try:
        from a2ui.manager import A2uiSchemaManager

        return A2uiSchemaManager(version=A2UI_VERSION).get_selected_catalog().catalog_id
    except Exception:
        return DEFAULT_CATALOG_ID


def render_operations(plan: dict[str, Any]) -> list[dict[str, Any]]:
    validate_response_plan(plan)
    screen = plan["screen"]
    filters = screen["filters"]
    surface_id = f"weekly-business-review-{uuid.uuid4().hex[:10]}"
    components: list[dict[str, Any]] = []
    root_children = ["screen_title"]
    components.append(_column(ROOT_ID, root_children))
    components.append(_text("screen_title", screen["title"], "h2"))
    if screen.get("subtitle"):
        root_children.append("screen_subtitle")
        components.append(_text("screen_subtitle", screen["subtitle"], "body"))
    root_children.append("assistant_takeaway")
    components.append(_text("assistant_takeaway", plan["message"], "body"))

    metrics = screen.get("metrics", [])
    if metrics:
        metric_cards = []
        for index, metric in enumerate(metrics, 1):
            card_id = f"metric_{index}_card"
            column_id = f"metric_{index}_column"
            children = [f"metric_{index}_value", f"metric_{index}_label"]
            if metric.get("caption"):
                children.append(f"metric_{index}_caption")
            metric_cards.append(card_id)
            components.extend([
                _card(card_id, column_id, 1),
                _column(column_id, children),
                _text(f"metric_{index}_value", str(metric["value"]), "h3"),
                _text(f"metric_{index}_label", metric["label"], "body"),
            ])
            if metric.get("caption"):
                components.append(_text(f"metric_{index}_caption", metric["caption"], "caption"))
        root_children.extend(["snapshot_heading", "metrics_row"])
        components.extend([_text("snapshot_heading", "Executive snapshot", "h3"), _row("metrics_row", metric_cards)])

    filter_children = ["filters_label", "region_filter", "segment_filter", "start_date_label", "start_date_filter", "end_date_label", "end_date_filter", "apply_filters_button"]
    components.extend([
        _column("filter_column", filter_children),
        _text("filters_label", "Filters", "h3"),
        _component("region_filter", "OARadioSet", {
            "label": "Region",
            "options": [{"label": value, "value": value} for value in ALLOWED_REGIONS],
            "selections": {"path": "/wbrFilters/region_selection"},
            "direction": "column",
        }),
        _component("segment_filter", "OARadioSet", {
            "label": "Segment",
            "options": [{"label": value, "value": value} for value in ALLOWED_SEGMENTS],
            "selections": {"path": "/wbrFilters/segment_selection"},
            "direction": "column",
        }),
        _text("start_date_label", "Start date", "caption"),
        _component("start_date_filter", "DateTimeInput", {"value": {"path": "/wbrFilters/start_date"}, "enableDate": True, "enableTime": False}),
        _text("end_date_label", "End date", "caption"),
        _component("end_date_filter", "DateTimeInput", {"value": {"path": "/wbrFilters/end_date"}, "enableDate": True, "enableTime": False}),
    ])
    components.extend(_button(
        "apply_filters_button",
        "apply_filters_label",
        "Apply filters",
        "apply_wbr_filters",
        _filter_context(filters, _template_for_screen(screen["type"])),
        True,
    ))

    chart = screen.get("chart")
    if chart:
        chart_items = [
            {
                "id": index,
                "seriesId": item.get("series", "Value"),
                "groupId": item["label"],
                "label": "",
                "value": item["value"],
                "color": CHART_COLORS[(index - 1) % len(CHART_COLORS)],
            }
            for index, item in enumerate(chart["items"], 1)
        ]
        chart_props: dict[str, Any] = {
            "title": chart["title"],
            "type": chart["type"],
            "orientation": "vertical",
            "items": chart_items,
            "colors": CHART_COLORS,
            "xAxis": {"title": chart.get("x_axis", "Category")},
            "yAxis": {"title": chart.get("y_axis", "Value")},
        }
        root_children.extend(["analysis_heading", "chart_filter_row"])
        components.extend([
            _text("analysis_heading", chart["title"], "h3"),
            _row("chart_filter_row", ["chart_card", "filter_card"]),
            _card("chart_card", "analysis_chart", 3),
            _component("analysis_chart", "OAChart", chart_props),
            _card("filter_card", "filter_column", 1),
        ])
    else:
        root_children.extend(["filters_heading", "filter_card"])
        components.extend([_text("filters_heading", "Query filters", "h3"), _card("filter_card", "filter_column")])

    table = screen.get("table")
    if table:
        root_children.extend(["table_heading", "detail_grid"])
        components.extend([
            _text("table_heading", table["title"], "h3"),
            _component("detail_grid", "OADataGrid", {"columns": table["columns"], "rows": table["rows"], "maxRows": min(max(len(table["rows"]), 1), 10)}),
        ])

    for section_name, title in (("insights", "Insights"), ("recommendations", "Recommendations")):
        sections = screen.get(section_name, [])
        if not sections:
            continue
        heading_id = f"{section_name}_heading"
        list_id = f"{section_name}_list"
        root_children.extend([heading_id, list_id])
        components.append(_text(heading_id, title, "h3"))
        bullet_text = "\n".join(
            f"- {section['heading']}: {section['body']}" for section in sections
        )
        components.append(_text(list_id, bullet_text, "body"))

    actions = screen.get("actions", [])
    if actions:
        root_children.extend(["next_questions_divider", "next_questions_heading", "actions_row"])
        components.extend([
            _component("next_questions_divider", "Divider", {"axis": "horizontal"}),
            _text("next_questions_heading", "Next questions", "h3"),
        ])
        action_ids = []
        for index, action in enumerate(actions, 1):
            button_id = f"action_{index}_button"
            action_ids.append(button_id)
            action_context = _scoped_follow_up(action["prompt"], filters)
            components.extend(_button(
                button_id,
                f"action_{index}_label",
                action["label"],
                "ask_wbr_question",
                action_context,
            ))
        components.append(_row("actions_row", action_ids, "start"))

    sources = screen.get("sources", [])
    if sources:
        root_children.append("sources_text")
        components.append(_text("sources_text", "Sources: " + "; ".join(sources), "caption"))

    operations = [
        {
            "version": "v0.9",
            "createSurface": {
                "surfaceId": surface_id,
                "catalogId": _catalog_id(),
                "sendDataModel": True,
            },
        },
        {
            "version": "v0.9",
            "updateDataModel": {
                "surfaceId": surface_id,
                "path": "/wbrFilters",
                "value": {
                    "start_date": filters["start_date"],
                    "end_date": filters["end_date"],
                    "region": filters["region"],
                    "segment": filters["segment"],
                    "region_selection": [filters["region"]],
                    "segment_selection": [filters["segment"]],
                },
            },
        },
        {
            "version": "v0.9",
            "updateComponents": {"surfaceId": surface_id, "components": components},
        },
    ]
    validate_operations(operations)
    return operations


def _contains_legacy_literal(value: Any) -> bool:
    if isinstance(value, dict):
        return "literalString" in value or any(_contains_legacy_literal(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_legacy_literal(item) for item in value)
    return False


def _version_tuple(value: str) -> tuple[int, int, int]:
    parts = [int(part) for part in re.findall(r"\d+", value)[:3]]
    return tuple((parts + [0, 0, 0])[:3])


def _require_schema_validation_dependencies() -> None:
    for package, minimum in MIN_SCHEMA_DEPENDENCIES.items():
        minimum_text = ".".join(str(part) for part in minimum)
        try:
            installed = importlib_metadata.version(package)
        except importlib_metadata.PackageNotFoundError as exc:
            raise RuntimeError(
                f"A2UI schema validation requires {package}>={minimum_text}; the package is not installed."
            ) from exc
        if _version_tuple(installed) < minimum:
            raise RuntimeError(
                f"A2UI schema validation requires {package}>={minimum_text}; found {installed}."
            )


def validate_operations(operations: list[dict[str, Any]]) -> None:
    operation_names = [next((key for key in item if not key == "version"), "") for item in operations]
    if operation_names != ["createSurface", "updateDataModel", "updateComponents"]:
        raise ValueError("A2UI v0.9 operations must be createSurface, updateDataModel, then updateComponents.")
    for operation in operations:
        if operation.get("version") != "v0.9":
            raise ValueError("Every A2UI operation must include version v0.9.")
        if _contains_legacy_literal(operation):
            raise ValueError("A2UI v0.9 operations must not use legacy literalString wrappers.")

    create_surface = operations[0]["createSurface"]
    if not create_surface.get("surfaceId") or not create_surface.get("catalogId"):
        raise ValueError("createSurface requires surfaceId and catalogId.")

    data_update = operations[1]["updateDataModel"]
    if data_update.get("surfaceId") != create_surface["surfaceId"] or data_update.get("path") != "/wbrFilters":
        raise ValueError("updateDataModel must target the created surface and /wbrFilters.")
    data_value = data_update.get("value")
    if not isinstance(data_value, dict):
        raise ValueError("updateDataModel.value must contain the WBR filter object.")

    update_components = operations[2]["updateComponents"]
    if update_components.get("surfaceId") != create_surface["surfaceId"]:
        raise ValueError("updateComponents must target the created surface.")
    components = update_components.get("components")
    if not isinstance(components, list) or not components or components[0].get("id") != ROOT_ID:
        raise ValueError("updateComponents requires a root-first component list.")
    ids = [item.get("id") for item in components]
    if len(ids) != len(set(ids)):
        raise ValueError("A2UI component IDs must be unique.")
    referenced: set[str] = set()
    for item in components:
        name = item.get("component") if isinstance(item, dict) else None
        if not isinstance(name, str):
            raise ValueError("Each A2UI v0.9 component must use a flat string component name.")
        children = item.get("children")
        if isinstance(children, list):
            referenced.update(children)
        child = item.get("child")
        if isinstance(child, str):
            referenced.add(child)
        if name == "OAChart":
            for chart_item in item.get("items", []):
                missing = {"id", "seriesId", "groupId"} - set(chart_item)
                if missing:
                    raise ValueError(f"OAChart item missing hosted metadata: {sorted(missing)}")
        if name == "OADataGrid":
            if any("id" not in row for row in item.get("rows", [])):
                raise ValueError("OADataGrid rows must include id.")
    missing = referenced - set(ids)
    if missing:
        raise ValueError(f"A2UI components reference missing IDs: {sorted(missing)}")

    _require_schema_validation_dependencies()
    try:
        from a2ui.manager import A2uiSchemaManager
        A2uiSchemaManager(version=A2UI_VERSION).get_selected_catalog().validator.validate(
            operations,
            root_id=ROOT_ID,
        )
    except (ImportError, ModuleNotFoundError) as exc:
        raise RuntimeError(
            "A2UI schema validation is unavailable. Install jsonschema>=4.18.0 "
            "and referencing>=0.30.0."
        ) from exc


def serialize_operations(operations: list[dict[str, Any]]) -> dict[str, Any]:
    from a2ui.parser import build_text_response_from_operations

    return build_text_response_from_operations(operations)


def text_response(message: str) -> dict[str, Any]:
    return {"messages": [AIMessage(content=message)]}


def _message_value(message: Any, key: str, default: Any = None) -> Any:
    return message.get(key, default) if isinstance(message, dict) else getattr(message, key, default)


def _current_turn_messages(messages: list[Any]) -> list[Any]:
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        message_type = str(_message_value(message, "type", _message_value(message, "role", ""))).lower()
        if message_type in {"human", "user"} or message.__class__.__name__ == "HumanMessage":
            return messages[index + 1 :]
    return messages


def extract_tool_failures(result: Any) -> list[tuple[str, str]]:
    messages = result.get("messages", []) if isinstance(result, dict) else []
    failures = []
    current_messages = _current_turn_messages(messages) if isinstance(messages, list) else []
    for message in current_messages:
        message_type = str(_message_value(message, "type", _message_value(message, "role", ""))).lower()
        if message_type != "tool" and message.__class__.__name__ != "ToolMessage":
            continue
        content = response_text(message)
        status = str(_message_value(message, "status", "")).lower()
        failed = status in {"error", "failed", "failure"}
        try:
            payload = json.loads(content)
            failed = failed or (isinstance(payload, dict) and isinstance(payload.get("code"), int) and payload["code"] >= 400)
        except (ValueError, TypeError, json.JSONDecodeError):
            payload = None
        if failed or content.lower().startswith(("error:", "exception:")):
            failures.append((str(_message_value(message, "name", "unknown_tool")), content[:1200]))
    return failures


def _decode_action_value(value: Any) -> Any:
    if isinstance(value, dict):
        for key in ("literalString", "literal", "valueString", "valueNumber", "valueBoolean"):
            if key in value:
                return value[key]
        if "path" in value:
            return value["path"]
    return value


def _decode_action_context(context: Any) -> dict[str, Any]:
    if isinstance(context, dict):
        return {str(key): _decode_action_value(value) for key, value in context.items()}
    if not isinstance(context, list):
        return {}
    decoded: dict[str, Any] = {}
    for item in context:
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            continue
        decoded[item["key"]] = _decode_action_value(item.get("value"))
    return decoded


def _scalar_context_value(value: Any) -> str:
    if isinstance(value, list):
        value = next((item for item in value if item not in (None, "")), "")
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.startswith("/") else text


def _action_payload(candidate: Any) -> dict[str, Any] | None:
    if not isinstance(candidate, dict):
        return None
    action = candidate.get("userAction") or candidate.get("action") or candidate
    if not isinstance(action, dict):
        return None
    nested_user_action = action.get("userAction")
    if isinstance(nested_user_action, dict):
        action = nested_user_action
    event = action.get("event")
    if isinstance(event, dict):
        merged = dict(event)
        if "surfaceId" in action and "surfaceId" not in merged:
            merged["surfaceId"] = action["surfaceId"]
        action = merged
    return action if isinstance(action.get("name"), str) else None


def normalize_request(user_query: Any, kwargs: dict[str, Any]) -> str:
    candidates = [user_query, kwargs.get("userAction"), kwargs.get("action")]
    metadata = kwargs.get("metadata")
    if isinstance(metadata, dict):
        candidates.extend([metadata.get("userAction"), metadata.get("action"), metadata])
    for candidate in candidates:
        action = _action_payload(candidate)
        if not action:
            continue
        context = _decode_action_context(action.get("context"))
        action_name = action["name"]
        if action_name == "ask_wbr_question" and isinstance(context.get("prompt"), str) and context["prompt"].strip():
            return context["prompt"].strip()
        if action_name == "apply_wbr_filters":
            template_id = _scalar_context_value(context.get("template_id")) or "weekly_business_review_summary"
            start_date = _scalar_context_value(context.get("start_date"))
            end_date = _scalar_context_value(context.get("end_date"))
            region = _scalar_context_value(context.get("region"))
            segment = _scalar_context_value(context.get("segment"))
            prompt_by_template = {
                "weekly_business_review_summary": "Show the complete weekly business review",
                "pipeline_by_stage": "Show pipeline by stage",
                "renewal_risk_accounts": "Show renewal-risk accounts",
                "product_usage_by_account": "Show product usage by account",
                "support_health_by_account": "Show support health by account",
            }
            if all([start_date, end_date, region, segment]):
                prefix = prompt_by_template.get(template_id, "Show the weekly business review")
                return f"{prefix} for {region} {segment} from {start_date} to {end_date}."
        return "The user applied this Weekly Business Review action. Run the required SQL tools with the resolved filter values and return an updated analysis: " + json.dumps(
            {"name": action_name, "context": context}, ensure_ascii=True
        )
    return str(user_query or "").strip()


class WeeklyBusinessReviewSQLAgent:
    def __init__(self) -> None:
        self.llm: Any | None = None
        self.react_agent: Any | None = None

    def setup(self) -> None:
        # AIDP calls setup during deployment. Runtime-bound resources are created
        # on first invoke so a provider or catalog error can return a useful stage.
        logger.info("Weekly Business Review deployment setup completed.")

    def _initialize_runtime(self) -> str:
        stage = "OCI LLM initialization"
        if self.llm is None:
            self.llm = build_llm()
        if self.llm is None:
            raise RuntimeError("init_oci_llm returned None.")

        stage = "SQLTool configuration"
        tools = build_tools()
        if not tools:
            raise RuntimeError("No SQLTools were created.")

        stage = "ReAct agent initialization"
        self.react_agent = _create_react_agent(self.llm, tools)
        if self.react_agent is None:
            raise RuntimeError("create_react_agent returned None.")
        return stage

    async def _validated_plan(self, raw: str) -> dict[str, Any]:
        if self.llm is None:
            raise RuntimeError("Weekly Business Review LLM is not initialized.")
        current = raw
        for attempt in range(3):
            try:
                return validate_response_plan(extract_json_object(current))
            except Exception as exc:
                if attempt == 2:
                    raise ValueError(f"Response plan remained invalid after repair: {exc}") from exc
                prompt = f"""
Your previous Weekly Business Review response plan was invalid.
Validation error: {type(exc).__name__}: {exc}

Return one corrected JSON object only. Do not call tools again and do not add facts.
Preserve only verified values from the previous response. Keep the A2UI message under 500 characters, use no more than four metrics, eighteen chart items, twelve table rows, four insights, four recommendations, and three actions.

Previous response:
{current}
""".strip()
                corrected = self.llm.ainvoke(prompt)
                if inspect.isawaitable(corrected):
                    corrected = await corrected
                current = response_text(corrected)
        raise RuntimeError("Unreachable response-plan state.")

    async def invoke(self, user_query: Any = "", **kwargs: Any) -> dict[str, Any]:
        diagnostic_id = uuid.uuid4().hex[:10]
        stage = "request setup"
        try:
            config = pre_invoke_setup(**kwargs)
            stage = "runtime initialization"
            if self.react_agent is None:
                try:
                    self._initialize_runtime()
                except Exception as exc:
                    message = str(exc).lower()
                    if self.llm is None or "oci" in message or "model" in message or "inference" in message:
                        stage = "OCI LLM initialization"
                    elif "sqltool" in message or "catalog" in message or "schema" in message or "aidptoolconf" in message:
                        stage = "SQLTool configuration"
                    else:
                        stage = "ReAct agent initialization"
                    raise
            request = normalize_request(user_query, kwargs) or "Hello"
            stage = "SQLTool execution and analysis"
            result = await self.react_agent.ainvoke(
                input={"messages": [{"role": "user", "content": request}]},
                config=config,
            )
            failures = extract_tool_failures(result)
            if failures:
                detail = " | ".join(f"{name}: {error}" for name, error in failures)
                logger.error("WBR diagnostic_id=%s tool_failures=%s", diagnostic_id, detail)
                return text_response(
                    "I couldn't complete the Weekly Business Review because a data tool failed. "
                    f"Diagnostic ID: {diagnostic_id}. Failed tool: {detail}"
                )
            raw = response_text(result)
            if not raw:
                raise RuntimeError("The agent returned an empty final response.")
            stage = "response plan validation"
            plan = await self._validated_plan(raw)
            if plan["mode"] == "text":
                return text_response(plan["message"])
            stage = "A2UI rendering"
            return serialize_operations(render_operations(plan))
        except Exception as exc:
            logger.exception("WBR failed diagnostic_id=%s stage=%s", diagnostic_id, stage)
            return text_response(
                "I couldn't complete the Weekly Business Review request. "
                f"Diagnostic ID: {diagnostic_id}. Stage: {stage}. Error: {type(exc).__name__}: {exc}"
            )

    async def stream(self, user_query: Any = "", **kwargs: Any):
        yield await self.invoke(user_query, **kwargs)

    async def astream(self, user_query: Any = "", **kwargs: Any):
        async for chunk in self.stream(user_query, **kwargs):
            yield chunk


agent = WeeklyBusinessReviewSQLAgent()
root_agent = agent

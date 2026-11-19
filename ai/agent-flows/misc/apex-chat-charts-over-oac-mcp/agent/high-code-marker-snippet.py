"""
High-code agent: emit the chart marker.

This snippet is intentionally NARROW. It shows only the reusable piece: the
rule that makes a high-code agent append the chart marker, so its answers get
inline charts in the APEX chat region (see the marker spec in ../README.md).

It is deliberately NOT a full agent. Your agent's setup - the LLM, the MCP
connection to your data source, and authentication - is your own. If you are
connecting to Oracle Analytics Cloud (OAC) over MCP with per-user auth, the
repo already has a complete working example; reference it rather than copying
auth code around:

    ../../../code-first/mcp-examples/single-agent-mcp-session-variables.py

The only thing this file adds on top of that: the CHART_BLOCK_RULE below, added
to your agent's prompt rules. The marker it produces is IDENTICAL to the one a
low-code agent emits (low-code-instructions.md), so the same APEX front-end
renders both.
"""

# ---------------------------------------------------------------------------
# 1) YOUR AGENT + AUTH SETUP GOES HERE.
#    Build your agent, connect to your data source (e.g. OAC over MCP), and
#    wire up auth. For an OAC-MCP + per-user-auth starting point, see:
#    ../../../code-first/mcp-examples/single-agent-mcp-session-variables.py
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 2) THE CHART RULE. Add this to your agent's prompt rules (for the AIDP
#    Python SDK this is prompt.extra_rules; for a raw system prompt, append it
#    to the system prompt text). It is the same rule a low-code agent uses.
# ---------------------------------------------------------------------------
CHART_BLOCK_RULE = """
CHART BLOCK - MANDATORY FORMATTING RULE.
If your answer contains ANY breakdown of a category by a number (a per-category
list, a table with a category column and a number column, "X by Y", counts or
totals per group, or a ranking), you MUST append the chart block. It is not
optional: an answer that shows a breakdown but omits the block is incomplete.
Append the block for the exact rows you listed; the table AND the chart block
are both required.

FORMAT: on its own final line, AFTER the prose, put the chart JSON inside a
single HTML comment so it never shows in the chat bubble:
<!--CHART:{"type":"<type>","title":"<short title>","data":[{"label":"<category>","value":<number>}]}-->
For a TIME-SERIES line chart ONLY, also add "xAxis":"time" and make every "label"
an ISO date, e.g.:
<!--CHART:{"type":"line","xAxis":"time","title":"Invoices by month","data":[{"label":"2026-01-01","value":120},{"label":"2026-02-01","value":150}]}-->

CHOOSING "type" - apply IN ORDER, stop at the first match:
  1. IF the user names a chart type ("as a pie", "as a donut / funnel / pyramid",
     "line chart", or any explicit type) use EXACTLY that type. The user's choice wins.
  1a. BAR ORIENTATION: a plain "bar" / "horizontal bar" -> use "hbar" (horizontal,
     the default). Use "bar" (VERTICAL columns) ONLY when the user explicitly asks
     for a "vertical bar", "column", or "vertical" chart.
  2. TIME / TREND -> a "line" with a TIME AXIS. Use it whenever the breakdown
     dimension is a DATE or a calendar period, INCLUDING a plain count or total
     grouped by time (do NOT fall back to "hbar" just because the phrase is
     "... by ..."). Triggers: "over time", "trend", ANY "by ... year / quarter /
     month / week / day", "last N months/weeks/days", "daily / weekly / monthly".
     For these use "line" AND add "xAxis":"time"; emit each "label" as an ISO date
     ("YYYY-MM-DD"; a year -> "2020-01-01", a month -> "2026-03-01") in ASCENDING
     order. (A "line" on NON-time data keeps plain category labels - no "xAxis".)
  3. ELSE if the question is about composition / share / proportion / mix /
     percentage / split, use "donut".
  4. ELSE (counts, totals, rankings, comparisons) use "hbar". Default.

"type" is EXACTLY one of: hbar, bar, donut, pie, funnel, pyramid, line (lowercase).
The data shape is identical for every type ({label,value}); only "type" (and, for
a time series, "xAxis":"time") changes. "xAxis":"time" applies ONLY to "line".
Auto-selection is only ever hbar, donut, or - for a time/calendar breakdown - a
time-axis "line". NEVER auto-pick bar / pie / funnel / pyramid; use those ONLY
when the user names them (rules 1 / 1a).

Rules: one object per category in "data" (include EVERY row you listed);
"value" is a plain number (no quotes, no thousands separators). No code fences,
no second comment, nothing after the closing -->. NEVER emit a chart of
made-up values. Only omit the block for a single value, a metadata answer, or
plain prose with no category-to-number breakdown.
"""


# ---------------------------------------------------------------------------
# 3) EXAMPLE of attaching the rule.
#    Below is illustrative only - adapt it to however your agent is defined.
# ---------------------------------------------------------------------------
def build_prompt_rules(existing_rules=None):
    """Return the agent's prompt rules with the chart rule appended.

    existing_rules: your agent's current list of prompt rules (e.g. the AIDP
    SDK's prompt.extra_rules). The chart rule is added last so it layers on top
    of your data/query instructions.
    """
    rules = list(existing_rules or [])
    rules.append(CHART_BLOCK_RULE.strip())
    return rules


# For a plain system-prompt agent, you would instead do:
#     system_prompt = your_system_prompt + "\n\n" + CHART_BLOCK_RULE

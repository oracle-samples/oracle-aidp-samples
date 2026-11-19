# Low-code agent: chart instructions

Paste the block below into a **low-code AIDP agent's Instructions** field. It
teaches the agent to append the chart marker whenever its answer contains a
category-to-number breakdown. That marker is what the APEX chat region renders
inline (see the marker spec in [`../README.md`](../README.md)).

> This file is **only** the chart rule. Your agent still needs its own data
> instructions - how to find and query your data source (for example, an OAC
> dataset over MCP). Keep those as-is and add this rule on top. Where this text
> says "the data you just listed", it means the rows your agent already
> retrieved and showed; this rule never invents numbers.

---

```
CHART BLOCK - MANDATORY FORMATTING RULE.
If your answer contains ANY breakdown of a category by a number (a per-category
list, a table with a category column and a number column, "X by Y", counts or
totals per group, or a ranking), you MUST append the chart block. It is not
optional: an answer that shows a breakdown but omits the block is incomplete.
Append the block for the exact rows you listed - never skip it because you
"already showed a table"; the table AND the chart block are both required.

FORMAT: on its own final line, AFTER the prose, put the chart JSON inside a
single HTML comment so it never shows in the chat bubble:
<!--CHART:{"type":"<type>","title":"<short title>","data":[{"label":"<category>","value":<number>}]}-->
For a TIME-SERIES line chart ONLY, also add "xAxis":"time" and make every "label"
an ISO date, e.g.:
<!--CHART:{"type":"line","xAxis":"time","title":"Invoices by month","data":[{"label":"2026-01-01","value":120},{"label":"2026-02-01","value":150}]}-->

CHOOSING "type" - apply IN ORDER, stop at the first match:
  1. IF the user names a chart type ("as a pie", "as a donut / funnel / pyramid",
     "line chart", or any explicit type) use EXACTLY that type. The user's choice
     always wins.
  1a. BAR ORIENTATION: a plain "bar" / "bar chart" / "horizontal bar" -> use "hbar"
     (horizontal, the default). Use "bar" (VERTICAL columns) ONLY when the user
     explicitly asks for a "vertical bar", "column", or "vertical" chart.
  2. TIME / TREND -> a "line" with a TIME AXIS. Use it whenever the breakdown
     dimension is a DATE or a calendar period, INCLUDING a plain count or total
     grouped by time (a grouped count over time is STILL a time series - do NOT
     fall back to "hbar" just because the phrase is "... by ..."). Triggers:
     "over time", "trend", ANY "by ... year / quarter / month / week / day",
     "last N months/weeks/days", "daily / weekly / monthly". For these use "line"
     AND add "xAxis":"time"; emit each "label" as an ISO date ("YYYY-MM-DD"; use
     the FIRST day of the period - a year -> "2020-01-01", a month -> "2026-03-01")
     and list the points in ASCENDING chronological order. (A "line" the user asks
     for on NON-time data keeps plain category labels - do NOT add "xAxis".)
  3. ELSE if the question is about composition / share / proportion / mix /
     percentage / % / split, use "donut".
  4. ELSE (counts, totals, rankings, comparisons: "break down", "by", "per",
     "each", "how many", "top", "most") use "hbar". Default; when in doubt, "hbar".

"type" is EXACTLY one of: hbar, bar, donut, pie, funnel, pyramid, line (lowercase).
The data shape is identical for every type ({label,value}); only "type" (and, for
a time series, "xAxis":"time") changes. "xAxis":"time" applies ONLY to "line".
Auto-selection is only ever hbar, donut, or - for a time/calendar breakdown - a
time-axis "line". NEVER auto-pick bar / pie / funnel / pyramid; use those ONLY
when the user explicitly names them (rules 1 / 1a).

Rules: one object per category in "data" (include EVERY row you listed);
"value" is a plain number (no quotes, no thousands separators). Do NOT wrap the
block in code fences, do NOT emit a second comment, put NOTHING after the
closing -->. NEVER emit a chart of made-up values - if you could not retrieve
the data, say so plainly and omit the block. Only omit the block when the
answer is a single value, a metadata answer, or plain prose with no
category-to-number breakdown.
```

---

## Quick check

Ask the agent something like *"break down sales by region"*. The reply should
end with a line such as:

```
<!--CHART:{"type":"hbar","title":"Sales by region","data":[{"label":"North","value":120},{"label":"South","value":90},{"label":"East","value":140},{"label":"West","value":75}]}-->
```

Then ask a time question like *"new customers by signup year"*. That reply
should end with a time-series line marker:

```
<!--CHART:{"type":"line","xAxis":"time","title":"New customers by year","data":[{"label":"2022-01-01","value":40},{"label":"2023-01-01","value":65},{"label":"2024-01-01","value":88}]}-->
```

If you see those lines in the raw reply, the APEX region will turn them into
inline charts.

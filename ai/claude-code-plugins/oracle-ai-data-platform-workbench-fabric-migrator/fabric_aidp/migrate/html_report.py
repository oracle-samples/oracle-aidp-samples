"""Render a migration report dict -> a standalone HTML file.

Self-contained (inline CSS, no external assets) and styled to match the AWS
migrator's report, because both are the same kind of artifact in the same
marketplace set and a reviewer should not have to relearn the layout.

Two things this one does that the AWS version does not, because this tool has
two things that one does not:

  * **Every asset links to the file it became.** The sibling project shipped an
    HTML report that named items but never the files they produced, and the
    first outside tester filed exactly that as what blocked them. The paths
    were already in report.json; they were simply never rendered.
  * **Blocked assets are shown, with the reason.** A refusal is a result. An
    HTML report that silently omits the 15 pipelines this tool declined to
    translate would read as a cleaner migration than it is.
"""
from __future__ import annotations

import html
import urllib.parse
from pathlib import Path

_CSS = """
:root{--red:#C74634;--ink:#1B1B1B;--muted:#606060;--cream:#F6F3EE;
--green:#2E6E3E;--blue:#1F4E79;--amber:#8A5A00;--line:#E3DDD3;}
*{box-sizing:border-box}
body{font-family:-apple-system,Helvetica,Arial,sans-serif;color:var(--ink);
margin:0;background:#fff;line-height:1.5}
header{background:var(--red);color:#fff;padding:26px 40px}
header h1{margin:0;font-size:24px}
header .sub{opacity:.9;font-size:14px;margin-top:4px}
.wrap{max-width:1400px;margin:0 auto;padding:28px 40px}
.cards{display:flex;gap:14px;flex-wrap:wrap;margin:8px 0 28px}
.card{flex:1;min-width:150px;border:1px solid var(--line);border-radius:10px;
padding:16px 18px;background:var(--cream)}
.card .n{font-size:30px;font-weight:700}
.card .l{font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.card.ok .n{color:var(--green)} .card.review .n{color:var(--amber)}
.card.fail .n{color:var(--red)} .card.skip .n{color:var(--muted)}
.asset{border:1px solid var(--line);border-radius:12px;margin:16px 0;overflow:hidden}
.asset .head{display:flex;align-items:center;gap:12px;padding:14px 18px;
background:var(--cream);border-bottom:1px solid var(--line)}
.asset .head .id{font-family:Menlo,monospace;font-weight:600;font-size:14px}
.badge{font-size:11px;font-weight:700;padding:3px 9px;border-radius:20px;color:#fff;text-transform:uppercase}
.badge.ok{background:var(--green)} .badge.review{background:var(--amber)}
.badge.fail{background:var(--red)} .badge.skip{background:var(--muted)}
.kind{font-size:12px;color:var(--muted);margin-left:auto}
.body{padding:16px 18px}
.findings{margin:0 0 14px;padding:0;list-style:none;font-size:13px}
.findings li{padding:4px 0;border-bottom:1px dashed var(--line)}
.findings .rewrite{color:var(--green)} .findings .flag{color:var(--amber)}
.tag{font-family:Menlo,monospace;font-weight:700;font-size:11px;padding:1px 6px;
border-radius:4px;background:#eee;margin-right:6px}
.cols{display:grid;grid-template-columns:1fr;gap:18px}
.col h4{margin:0 0 6px;font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
pre{margin:0;background:#0e1220;color:#e8ecf3;border-radius:8px;padding:12px 14px;
overflow:auto;font-family:Menlo,monospace;font-size:12.5px;line-height:1.5;white-space:pre-wrap}
pre.before{border-left:4px solid var(--red)} pre.after{border-left:4px solid var(--green)}
pre.preview{border-bottom-left-radius:0;border-bottom-right-radius:0}
details.more pre{border-top-left-radius:0;border-top-right-radius:0;padding-top:0}
details.more summary{cursor:pointer;font-size:12px;color:var(--blue);padding:6px 2px}
details.more[open] summary{order:1}
details.more{display:flex;flex-direction:column}
.ai{margin-top:12px;border:1px dashed var(--amber);border-radius:8px;
padding:10px 12px;background:#fff9f0;font-size:13px}
.ai b{color:var(--amber)}
footer{color:var(--muted);font-size:12px;padding:20px 40px;border-top:1px solid var(--line)}
.asset.blocked .head{background:#fdf2f0}
.badge.blocked{background:var(--red)}
.files{margin:10px 0 0;font-size:13px}
.files a{color:var(--blue);font-family:Menlo,monospace;text-decoration:none}
.files a:hover{text-decoration:underline}
.files .none{color:var(--muted);font-style:italic}
.why{margin-top:10px;border-left:4px solid var(--red);background:#fdf2f0;
padding:10px 12px;font-size:13px;border-radius:0 8px 8px 0}
"""

_BADGE = {"ok": "ok", "needs_manual_review": "review", "planned": "skip",
          "skipped": "skip", "blocked": "blocked", "error": "fail"}
_LANG = {"notebook": "python", "dataflow_query": "python",
         "pipeline_job": "json", "shortcut": "markdown"}


# Stacked, not side by side. Side by side only pays when the two panes line
# up row for row, and a Dataflow never does: MEASURED on the demo, a 15-line
# M query becomes a 193-220 line PySpark file, mostly generated helpers. With
# the page capped at 1100px each column got ~500px, so the source wrapped
# into a wall and the translation was clipped mid-line.
#
# A long pane shows its first PREVIEW_LINES and folds the rest into a native
# <details> -- no script, so the report still works as a saved file or a
# mail attachment, and nothing is rendered twice. Below FOLD_AT lines there
# is nothing worth hiding: a toggle that hides three lines is just a click.
#
# The preview's class is `preview`, not `head`. It was `head` for one render,
# and `.asset .head` -- the asset's title bar -- matched it: the code block
# took the bar's cream background and flex layout, and the translation came
# out grey on cream, nearly unreadable. No test saw it; a screenshot did.
PREVIEW_LINES = 25
FOLD_AT = 35


def _pane(text, cls) -> str:
    lines = str(text or "").split("\n")
    if len(lines) < FOLD_AT:
        return f'<pre class="{cls}">{html.escape(str(text or ""))}</pre>'
    head = html.escape("\n".join(lines[:PREVIEW_LINES]))
    rest = html.escape("\n".join(lines[PREVIEW_LINES:]))
    return (f'<pre class="{cls} preview">{head}</pre>'
            f'<details class="more"><summary>Show all {len(lines)} lines '
            f'({len(lines) - PREVIEW_LINES} more)</summary>'
            f'<pre class="{cls}">{rest}</pre></details>')


def _badge(status: str) -> str:
    return _BADGE.get(status, "skip")


def _card(n, label, cls) -> str:
    return (f'<div class="card {cls}"><div class="n">{n}</div>'
            f'<div class="l">{label}</div></div>')


def _findings(rows) -> str:
    return "".join(
        f'<li><span class="tag">{html.escape(str(f.get("rule", "")))}</span>'
        f'<span class="{html.escape(str(f.get("severity", "")))}">'
        f'{html.escape(str(f.get("detail", "")))}</span></li>'
        for f in rows or [])


def _output_link(row) -> str:
    """A working relative link to what this asset became."""
    path = row.get("output_path")
    if path:
        # HTML-escaping is not URL-escaping. Two real dataflow queries are
        # named `#"New Users"`, so the artifact path contains a `#` -- which a
        # browser reads as the start of a fragment and truncates the link at.
        # Spaces need quoting too. The visible text stays readable.
        href = urllib.parse.quote(str(path))
        shown = html.escape(str(path))
        return (f'<div class="files">wrote <a href="{href}">{shown}</a></div>')
    if row.get("status") == "blocked":
        return '<div class="files none">no file written -- see the reason below</div>'
    if row.get("status") == "error":
        return ('<div class="files none">no file written -- this asset failed '
                'to translate; see the message below</div>')
    return '<div class="files none">no artifact for this asset type</div>'


def _why_blocked(row) -> str:
    reasons = [f.get("detail", "") for f in row.get("findings") or []
               if str(f.get("rule", "")).endswith(
                   ("UNSUPPORTED_CONNECTOR", "UNSUPPORTED_ACTIVITY",
                    "UNSUPPORTED_STEP", "EMPTY_PIPELINE"))]
    if not reasons:
        return ""
    body = "<br>".join(html.escape(r) for r in reasons)
    return (f'<div class="why"><b>Refused rather than part-translated.</b><br>'
            f'{body}</div>')


def _why_errored(row) -> str:
    """The message of a row that failed to translate.

    `error` is the only thing an error row carries -- the runner writes no
    findings for one -- and nothing here referenced it, so an errored asset
    rendered as a red badge over an empty card.
    """
    message = row.get("error")
    if row.get("status") != "error" or not message:
        return ""
    return (f'<div class="why"><b>This asset failed to translate.</b><br>'
            f'{html.escape(str(message))}</div>')


def render(report: dict, out_path) -> Path:
    out_path = Path(out_path)
    counts = report.get("counts", {}) or {}
    results = report.get("results", []) or []

    cards = "".join([
        _card(counts.get("ok", 0), "No issue found (PASS)", "ok"),
        _card(counts.get("needs_manual_review", 0), "Review", "review"),
        _card(counts.get("blocked", 0), "Refused", "fail"),
        _card(counts.get("planned", 0) + counts.get("skipped", 0), "Skip", "skip"),
        _card(counts.get("error", 0), "Failed", "fail"),
    ])

    blocks = []
    for row in results:
        status = row.get("status", "")
        badge = _badge(status)
        before = html.escape(str(row.get("source_sql", "")))
        after = html.escape(str(row.get("translated_sql", "")))
        panes = ""
        if before or after:
            panes = ('<div class="cols">'
                     f'<div class="col"><h4>Source (Fabric)</h4>'
                     f'{_pane(row.get("source_sql", ""), "before")}</div>'
                     f'<div class="col"><h4>Translated (AIDP)</h4>'
                     f'{_pane(row.get("translated_sql", ""), "after")}</div></div>')
        blocks.append(f"""
        <div class="asset {badge}">
          <div class="head">
            <span class="id">{html.escape(str(row.get("asset_id", "")))}</span>
            <span class="badge {badge}">{badge}</span>
            <span class="kind">{html.escape(str(row.get("kind", "")))} &middot;
              {row.get("changes", 0)} changes &middot; {row.get("flags", 0)} flags</span>
          </div>
          <div class="body">
            <ul class="findings">{_findings(row.get("findings"))}</ul>
            {_output_link(row)}
            {_why_blocked(row)}
            {_why_errored(row)}
            {panes}
          </div>
        </div>""")

    doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Fabric to AIDP - migration report</title><style>{_CSS}</style></head><body>
<header><h1>Microsoft Fabric &rarr; Oracle AIDP &mdash; migration report</h1>
<div class="sub">Plan {html.escape(str(report.get("plan_id", "")))} &middot;
{html.escape(str(report.get("migrated_at", "")))} &middot;
{len(results)} asset(s)</div>
</header>
<div class="wrap">
  <div class="cards">{cards}</div>
  {"".join(blocks) if blocks else "<p>No assets in this run.</p>"}
</div>
<footer><b>PASS = translated, no known issue detected &mdash; not execution-verified.</b>
Nothing here parses or runs the generated artifacts, so a construct no rule covers is
reported clean; review before running in production.<br>
<b>Refused</b> means the tool declined to translate an object it could not translate
faithfully. That is it working, not failing.<br>
Generated by fabric-aidp-migrator.</footer>
</body></html>"""

    # Explicit utf-8: the document carries non-ASCII, and the caller swallows
    # render failures, so a default cp1252 encoder would make the report
    # silently never appear on Windows.
    out_path.write_text(doc, encoding="utf-8")
    return out_path

"""Render a migration report dict → a clean, standalone HTML file.

Self-contained (inline CSS, no external assets), Oracle-styled. Shows summary
counts, and a per-asset before/after with findings and an AI-suggestion slot.
"""
from __future__ import annotations

import html
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
.wrap{max-width:1100px;margin:0 auto;padding:28px 40px}
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
.cols{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.col h4{margin:0 0 6px;font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
pre{margin:0;background:#0e1220;color:#e8ecf3;border-radius:8px;padding:12px 14px;
overflow:auto;font-family:Menlo,monospace;font-size:12.5px;line-height:1.5;white-space:pre-wrap}
pre.before{border-left:4px solid var(--red)} pre.after{border-left:4px solid var(--green)}
.ai{margin-top:12px;border:1px dashed var(--amber);border-radius:8px;
padding:10px 12px;background:#fff9f0;font-size:13px}
.ai b{color:var(--amber)}
footer{color:var(--muted);font-size:12px;padding:20px 40px;border-top:1px solid var(--line)}
@media(max-width:760px){.cols{grid-template-columns:1fr}}
"""


def _badge(status: str) -> str:
    return {"ok": "ok", "needs_manual_review": "review",
            "planned": "skip", "skipped": "skip", "error": "fail"}.get(status, "skip")


def _lang(kind: str) -> str:
    return {"glue_job": "python", "s3_bucket": "bash"}.get(kind or "", "sql")


def render(report: dict, out_path: Path) -> Path:
    counts = report.get("counts", {})
    results = report.get("results", [])

    def card(n, label, cls):
        return (f'<div class="card {cls}"><div class="n">{n}</div>'
                f'<div class="l">{label}</div></div>')

    cards = "".join([
        card(counts.get("ok", 0), "No issue found (PASS)", "ok"),
        card(counts.get("needs_manual_review", 0), "Review", "review"),
        card(counts.get("planned", 0) + counts.get("skipped", 0), "Skip / planned", "skip"),
        card(counts.get("error", 0), "Fail", "fail"),
    ])

    blocks = []
    for r in results:
        if r.get("status") not in ("ok", "needs_manual_review"):
            continue
        b = _badge(r["status"])
        lang = _lang(r.get("kind"))
        findings = "".join(
            f'<li><span class="tag">{html.escape(f["severity"])}</span>'
            f'<span class="{html.escape(f["severity"])}">{html.escape(f["detail"])}</span></li>'
            for f in r.get("findings", []))
        before = html.escape(r.get("source_sql", ""))
        after = html.escape(r.get("translated_sql", ""))
        # AI-suggestion slot (populated in a later phase when flags exist)
        ai = ""
        if r.get("flags"):
            sug = r.get("ai_suggestion")
            if sug:
                ai = (f'<div class="ai"><b>AI suggestion (review):</b> '
                      f'{html.escape(sug)}</div>')
            else:
                ai = ('<div class="ai"><b>Flagged for review.</b> An AI-assisted '
                      'rewrite suggestion can be generated here (AIDP ai_generate()).</div>')
        blocks.append(f"""
        <div class="asset">
          <div class="head">
            <span class="id">{html.escape(r["asset_id"])}</span>
            <span class="badge {b}">{b}</span>
            <span class="kind">{html.escape(r.get("kind",""))} · {r.get("changes",0)} changes · {r.get("flags",0)} flags</span>
          </div>
          <div class="body">
            <ul class="findings">{findings}</ul>
            <div class="cols">
              <div class="col"><h4>Source (AWS)</h4><pre class="before">{before}</pre></div>
              <div class="col"><h4>Translated (AIDP)</h4><pre class="after">{after}</pre></div>
            </div>
            {ai}
          </div>
        </div>""")

    doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Migration report</title><style>{_CSS}</style></head><body>
<header><h1>AWS → Oracle AIDP — migration report</h1>
<div class="sub">Plan {html.escape(str(report.get("plan_id","")))} · mode {html.escape(report.get("mode",""))} · {html.escape(report.get("migrated_at",""))}</div>
</header>
<div class="wrap">
  <div class="cards">{cards}</div>
  {''.join(blocks) if blocks else '<p>No translated assets in this run.</p>'}
</div>
<footer><b>PASS = translated, no known issue detected — not execution-verified.</b>
Nothing here parses or runs the generated artifacts, so a construct no rule covers is
reported clean; review them before running in production.<br>
Generated by aws-aidp-migrator · deterministic translation, honest flags.</footer>
</body></html>"""

    # Explicit utf-8: the document contains non-ASCII (the "→" in the header),
    # which raises UnicodeEncodeError under Windows' default cp1252.  The
    # caller swallows render failures, so without this the report silently
    # never appears on Windows.
    out_path.write_text(doc, encoding="utf-8")
    return out_path

"""Create (or refresh) the Night Shift approvals notebook in the Workbench: the place where a person reviews what the
agent drafted and lifts or refuses the quarantine, inside the product, with their own signed-in identity.

  python scripts/stage_approvals_notebook.py                              # Shared/nightshift/NightShift_Approvals.ipynb
  python scripts/stage_approvals_notebook.py --approval-id <id>           # pre-fill the first cell with a pending approval id
  python scripts/stage_approvals_notebook.py --ops default.nightshift_ops  # ledger location templated into every cell (default: NS_OPS)
  python scripts/stage_approvals_notebook.py --out notebooks/NightShift_Approvals.ipynb   # write locally, no client needed
  python scripts/stage_approvals_notebook.py --path Shared/x.ipynb

Every cell is driven by APPROVAL_ID in the first code cell, so each query is an equality lookup on the ledger (a
join-based evidence query is slow while a tick is replaying). Approving writes approvals.status = 'approved' with
approved_by = current_user() (the kernel's identity, not an attestation typed on a laptop); the next scheduled heartbeat
tick drains the row through the policy gate and writes the draft as a runbook row. Nothing here touches the control plane.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from nightshift.aidp_io import AidpIO  # noqa: E402
from nightshift.config import Env  # noqa: E402

NL = chr(10)

PARAM = '''# The approval under review: its id drives every cell below. Pick one from the Pending table in step 1.
import re as _re
APPROVAL_ID = "{approval_id}"
if APPROVAL_ID and not _re.fullmatch(r"[0-9a-f]{{16}}", APPROVAL_ID):
    raise ValueError("APPROVAL_ID must be the 16-hex-character id from the Pending table")
'''

EVIDENCE = '''# The verdicts behind it: the original statement and the draft, same five hazards, each on its own shallow clone of the table.
_ops = "{ops}"
_d = spark.sql(f"SELECT statement_sha256, verdict_id, table_name FROM {{_ops}}.repair_drafts WHERE approval_id = '{{APPROVAL_ID}}'").collect()
if not _d:
    raise ValueError(f"no draft row for approval {{APPROVAL_ID}}")
_sha, _vid, _table = _d[0]
print(_table)
_orig = spark.sql(f"SELECT 'original' AS which, verdict, diffs_json, elapsed_s FROM {{_ops}}.replay_verdicts WHERE statement_sha256 = '{{_sha}}' AND candidate NOT LIKE 'draft:%' ORDER BY created_at DESC LIMIT 1")
_draft = spark.sql(f"SELECT 'draft' AS which, verdict, diffs_json, elapsed_s FROM {{_ops}}.replay_verdicts WHERE verdict_id = '{{_vid}}'")
_orig.unionAll(_draft).show(truncate=False)
'''

DRAFT_TEXT = '''# The draft itself, as text. It becomes a runbook procedure if approved; it is never applied to anyone's job.
spark.sql(f"SELECT draft_id, model, validation, verdict, draft_sql FROM {ops}.repair_drafts WHERE approval_id = '{{APPROVAL_ID}}'").show(truncate=False)
'''

DECIDE = '''# Decide the approval named above. Set DECISION to "approve" or "reject", run the cell.
# The row is updated only if it is still pending; approved_by is the identity this kernel runs as.
DECISION = "approve"
NOTE = ""

_ops = "{ops}"
_status = {{"approve": "approved", "reject": "rejected"}}[DECISION]
# Works on its own too: if the first cell was not run in this kernel, the id staged into this notebook is used.
APPROVAL_ID = globals().get("APPROVAL_ID") or "{approval_id}"
if not APPROVAL_ID:
    raise ValueError("set APPROVAL_ID in the first cell")
_who = spark.sql("SELECT current_user()").collect()[0][0]
# Spark SQL escaping for the literals below: backslash doubled, quote backslash-escaped. A doubled quote ('') is NOT an
# escape in Spark (it reads as two adjacent literals and the quote is lost), so the ledger's own rule is used here.
_esc = lambda s: s.replace("\\\\", "\\\\\\\\").replace("'", "\\\\'")  # noqa: E731
_who_sql = _esc(_who)
_note_sql = _esc(NOTE)
print(f"deciding {{APPROVAL_ID}} as {{_who}}")
_row = spark.sql(f"SELECT status FROM {{_ops}}.approvals WHERE approval_id = '{{APPROVAL_ID}}'").collect()
if not _row:
    raise ValueError(f"no approval {{APPROVAL_ID}}")
if _row[0][0] != "pending":
    raise ValueError(f"approval {{APPROVAL_ID}} is {{_row[0][0]}}, not pending; nothing changed")
_suffix = f"{{_status}} by {{_who_sql}}: workbench" + (f"; {{_note_sql}}" if NOTE else "")
spark.sql(f"""UPDATE {{_ops}}.approvals
SET status = '{{_status}}', approved_by = '{{_who_sql}}', approved_at = current_timestamp(),
    note = CASE WHEN note IS NULL THEN '{{_suffix}}' ELSE CONCAT(note, ' | ', '{{_suffix}}') END
WHERE approval_id = '{{APPROVAL_ID}}' AND status = 'pending'""")
spark.sql(f"SELECT approval_id, status, approved_by, approved_at, expires_at FROM {{_ops}}.approvals WHERE approval_id = '{{APPROVAL_ID}}'").show(truncate=False)
print("approved: the next scheduled heartbeat tick (within 30 minutes) re-checks it through the gate and writes the runbook row"
      if _status == "approved" else "rejected: recorded; the draft is never applied")
'''


def cells(ops: str, approval_id: str = "") -> list[dict]:
    def md(text: str) -> dict:
        return {"cell_type": "markdown", "metadata": {}, "source": text}

    def code(text: str) -> dict:
        return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": text}

    def q(sql: str, n: int = 20) -> dict:
        return code('spark.sql("""' + sql + '""").show(' + str(n) + ', truncate=False)')

    pending = (
        f"SELECT a.approval_id, get_json_object(a.action_json, '$.kind') AS kind, get_json_object(a.action_json, '$.target') AS target,{NL}"
        f"       d.verdict, d.diff_rows, d.model, a.requested_at, a.expires_at, a.note{NL}"
        f"FROM {ops}.approvals a LEFT JOIN {ops}.repair_drafts d ON d.approval_id = a.approval_id{NL}"
        f"WHERE a.status = 'pending' ORDER BY a.requested_at"
    )
    intro = (
        "# Night Shift - approvals" + NL + NL +
        "What the agent drafted overnight and proved by replay, waiting for a person. Every row here is a `statement_repair` "
        "the policy gate quarantined: a replacement MERGE that graded replay_safe under late, duplicate, out-of-order, retried "
        "and corrected deliveries, where the original did not." + NL + NL +
        "Approving does not change anyone's job. The next scheduled heartbeat tick re-evaluates the approval through the same "
        "gate (the verdict must still be fresh) and writes the draft as a runbook row with your name on it."
    )
    after = "## 4. After the next tick" + NL + NL + "The runbook row carries who approved it and when, and the draft as its procedure."
    return [
        md(intro),
        code(PARAM.format(approval_id=approval_id)),                       # code 0
        md("## 1. Pending"),
        q(pending),                                                        # code 1
        md("## 2. The evidence: original and draft, same hazards, same table"),
        code(EVIDENCE.format(ops=ops)),                                    # code 2
        code(DRAFT_TEXT.format(ops=ops)),                                  # code 3
        md("## 3. Decide"),
        code(DECIDE.format(ops=ops, approval_id=approval_id)),            # code 4
        md(after),
        q(f"SELECT approved_at, approved_by, label, title, provenance, procedure FROM {ops}.runbooks ORDER BY approved_at DESC", 10),  # code 5
        q(f"SELECT approval_id, status, approved_by, approved_at, note FROM {ops}.approvals ORDER BY requested_at DESC", 10),         # code 6
    ]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="Shared/nightshift/NightShift_Approvals.ipynb")
    ap.add_argument("--approval-id", default="", help="pre-fill the first cell with this pending approval id")
    ap.add_argument("--ops", default=os.environ.get("NS_OPS", "default.nightshift_ops"),
                    help="ledger location (catalog.schema) templated into every cell; default NS_OPS or default.nightshift_ops")
    ap.add_argument("--out", default="", help="write the notebook to this local .ipynb file instead of saving it to the workspace (no client needed)")
    args = ap.parse_args()
    nb = {"cells": cells(args.ops, args.approval_id), "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3"}},
          "nbformat": 4, "nbformat_minor": 5}
    if args.out:
        pathlib.Path(args.out).write_text(json.dumps(nb, indent=1), encoding="utf-8")
        print("notebook written:", args.out)
        return 0
    io = AidpIO(Env(), dry_run=True)
    try:
        res = io.client.nb_save_notebook(args.path, nb)
        print("notebook saved:", args.path, json.dumps(res, default=str)[:160])
    finally:
        io.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

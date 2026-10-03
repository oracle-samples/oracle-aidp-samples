#!/usr/bin/env bash
# Paced local demo. Everything here runs offline: no cloud account, no
# cluster, no API key. Press Enter between sections so the screen is
# readable while you talk. Run with NOPAUSE=1 to play straight through.
set -u
cd "$(dirname "$0")"

B=$'\033[1m'; R=$'\033[0m'; C=$'\033[36m'; G=$'\033[32m'; Y=$'\033[33m'

step() { printf "\n${C}%s${R}\n${B}%s${R}\n\n" "────────────────────────────────────────────────────────────" "$1"; }
pause() { [ "${NOPAUSE:-0}" = "1" ] || { printf "\n${Y}[Enter]${R}"; read -r _; }; }

clear 2>/dev/null || true
cat <<'BANNER'
  Informatica  →  Oracle AI Data Platform
  ETL metadata in.  PySpark notebooks and AIDP jobs out.

  Everything in this demo runs offline.
BANNER
pause

step "1. What we start from — Informatica metadata"
echo "One Expression transformation, as PowerCenter stores it:"
echo
grep -A4 'NAME="EXP_TRANSFORM"' tests/fixtures/corpus/orders_transform.xml | head -6
pause

step "2. Migrate — 12 mappings to 12 PySpark notebooks"
./demo.sh 2>&1 | tail -12
pause

step "3. What came out — the same Expression, as runnable PySpark"
python3 - <<'PY'
import json, glob
p = glob.glob('/tmp/infa-aidp-demo/notebooks/SALES/nb_m_ORDERS_TRANSFORM.ipynb')
code = "\n".join("".join(c["source"]) for c in json.load(open(p[0]))["cells"]
                 if c["cell_type"] == "code")
for ln in code.splitlines():
    if "withColumn" in ln or "spark.table" in ln or "saveAsTable" in ln:
        print("   " + ln.strip()[:110])
PY
pause

step "4. The guards the generated notebook carries"
python3 - <<'PY'
import json, glob
p = glob.glob('/tmp/infa-aidp-demo/notebooks/SALES/nb_m_ORDERS_TRANSFORM.ipynb')
cells = json.load(open(p[0]))["cells"]
want = ("ansi.enabled", "_expected_catalog_type", "infa_compat.__version__")
for i, c in enumerate(cells):
    if c["cell_type"] != "code":
        continue
    src = "".join(c["source"])
    for w in want:
        if w in src:
            line = next(l for l in src.splitlines() if w in l)
            print(f"   cell {i:>2}  {line.strip()[:100]}")
            break
PY
echo
echo "   ANSI pinned          -> survives the Spark 4 default flip next week"
echo "   catalog type asserted -> refuses Delta code against an ADW catalog"
echo "   library version checked -> fails at the top, not mid-write"
pause

step "5. A real test — converted expressions executed on Spark, results asserted"
echo "Not 'does the code look right'. It runs it."
echo
python3 -m pytest tests/test_expression_execution.py -q 2>&1 | tail -4
pause

step "6. The check that caught a broken notebook on a live cluster"
echo "A Joiner whose master side cannot be resolved is skipped -- correctly."
echo "But the prepared side was then abandoned, so the notebook could not run."
echo "Four instruments passed it. This one does not:"
echo
python3 -m pytest tests/test_code_validation.py -q -k "abandoned or dropped_join or router" 2>&1 | tail -4
pause

step "7. The whole suite"
python3 -m pytest tests/ -q 2>&1 | tail -3
echo
python3 -m pytest tests/test_release_gate.py -q 2>&1 | grep -E "criteria|^GO|^NO-GO"
pause

step "Where this has actually run"
cat <<'CLOSE'
   Live on AIDP (Spark 3.5.0):
     12 notebooks uploaded, job created, re-deploy updates it
     job run SUCCESS -- read 2 Delta tables, filter, join, aggregate, MERGE
     output reconciled against numbers computed by hand before the run:

       Engineering  count=2  total=250000  avg=125000
       Finance      count=2  total=110000  avg=110000   (NULL salary: counted, not averaged)
       Sales        count=2  total=185000  avg=92500

   Not yet true:
     every fixture is synthetic -- no customer export parsed yet
     no test in the suite contacts AIDP; live runs are manual
     reconciled against hand-computed numbers, not an Informatica run
CLOSE
printf "\n${G}%s${R}\n\n" "done"

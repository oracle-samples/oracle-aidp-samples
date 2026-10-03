#!/usr/bin/env bash
# demo.sh - offline end-to-end smoke run. No IDMC trial, no AIDP cluster,
# no LLM provider key required. Exercises the rule-based (use_llm=False)
# translation path only, over a small committed fixture corpus that covers
# both source formats (PowerCenter XML and IICS/IDMC JSON).
#
# Prerequisite: `pip install -e .` from a clean checkout (no [llm] extra
# needed -- see pyproject.toml). Run from the repo root.
set -euo pipefail

export OUT="${OUT:-/tmp/infa-aidp-demo}"
rm -rf "$OUT"; mkdir -p "$OUT"
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/engine"

CORPUS=tests/fixtures/corpus
err=0

run() {  # run <label> <command...>
  local label="$1"; shift
  if "$@" > "$OUT/$label.log" 2>&1; then
    echo "  ok    $label"
  else
    echo "  FAIL  $label  (see $OUT/$label.log)"
    err=$((err+1))
  fi
}

echo "== analyze =="
run analyze python3 -m infa2aidp.cli analyze -i "$CORPUS" -o "$OUT/reports"

echo "== migrate (rule-based, no LLM) =="
run migrate python3 -m infa2aidp.cli migrate -i "$CORPUS" -o "$OUT/notebooks" --comparison

echo "== verify generated output =="
# Run directly (not through the run() wrapper) so the disclosed
# known-limitations list always reaches the console, pass or fail --
# it also gets teed into verify.log for the record.
if python3 - <<'PYEOF' 2>&1 | tee "$OUT/verify.log"
import ast, json, os, sys
from pathlib import Path

from infa2aidp.generators.code_validation import unresolved_names, abandoned_dataframes

out = Path(os.environ.get("OUT", "/tmp/infa-aidp-demo")) / "notebooks"
nbs = sorted(out.rglob("*.ipynb"))
assert nbs, "no notebooks generated"

# Fixtures known (from tests/fixtures/powercenter/) to carry real Informatica
# expressions -- used to prove expression conversion actually happened,
# not just that a comment was emitted.
EXPRESSION_FIXTURES = ("m_customer_enrichment", "m_order_to_cash")
REAL_CONVERSION_MARKERS = (
    "withColumn", "spark.table(", ".filter(", ".join(", ".groupBy(", ".agg(",
)

bad = []          # hard failures -> non-zero exit
notes = []         # known, disclosed limitations -> printed, not fatal
any_catalog_read = False
any_expression_hit = False

for nb in nbs:
    data = json.loads(nb.read_text())
    code = "\n".join(
        "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        for c in data["cells"] if c["cell_type"] == "code"
    )

    # 0. An abandoned per-consumer input copy means a dropped join: the
    #    side was prepared and never used, so downstream columns that only
    #    existed there cannot resolve at runtime. unresolved_names cannot
    #    see it -- the variable IS assigned, and the column lives in a
    #    string. Found on a live cluster.
    orphaned = abandoned_dataframes(code)
    if orphaned:
        bad.append(f"{nb.name}: input copy prepared and never used "
                   f"({', '.join(orphaned)}) -- a join was dropped, so this "
                   f"notebook cannot run")

    # 1. No JDBC leakage -- AIDP reads/writes go through the catalog, never
    #    a raw JDBC connection string.
    if "jdbc" in code.lower():
        bad.append(f"{nb.name}: JDBC string found in generated code")

    # 2. Generated code must actually be valid Python. A silent syntax
    #    error is the sharpest possible form of "hollow" -- worse than an
    #    empty notebook, because a file-existence check would still pass.
    try:
        ast.parse(code)
    except SyntaxError as e:
        bad.append(f"{nb.name}: generated code is not valid Python ({e})")

    # 2b. Valid syntax is not the same as valid at runtime. A name read
    #     before it was ever assigned anywhere earlier in the script is a
    #     guaranteed NameError the moment this notebook actually runs --
    #     ast.parse() above cannot see this, because the code IS
    #     syntactically correct. This caught a real bug: "df_final = df"
    #     where df was never assigned.
    unresolved = unresolved_names(code)
    if unresolved:
        bad.append(
            f"{nb.name}: unresolved name(s) {unresolved} -- read before "
            f"ever being assigned; this notebook would raise NameError at "
            f"runtime despite being syntactically valid Python"
        )

    # 3. Not suspiciously empty.
    if len(code.strip()) < 200:
        bad.append(f"{nb.name}: suspiciously short ({len(code.strip())} chars) -- looks hollow")

    # 4. Real transformation logic must be present -- not just boilerplate
    #    (imports / spark session / cache / logging) with no conversion.
    if not any(marker in code for marker in REAL_CONVERSION_MARKERS):
        bad.append(f"{nb.name}: no real conversion marker found ({REAL_CONVERSION_MARKERS}) -- looks hollow")

    # 5. A Router group that fell back to "# TODO: add condition" means its
    #    condition couldn't be resolved and the generator guessed "all
    #    rows" for that group -- i.e. every row gets duplicated into every
    #    downstream target. This used to be silent (a stub comment, not a
    #    failure). Found via the corpus; now a hard failure.
    if "# TODO: add condition" in code:
        bad.append(
            f"{nb.name}: unresolved Router group condition "
            f"('# TODO: add condition') -- rows would be duplicated into "
            f"every group instead of routed"
        )

    if "spark.table(" in code:
        any_catalog_read = True
    if any(f in nb.name for f in EXPRESSION_FIXTURES) and "withColumn" in code:
        any_expression_hit = True

    # No corpus fixture should hit these branches any more. Eight of them
    # used to: they were flattened copies of complete mappings sitting in
    # tests/fixtures/powercenter/, so the corpus exercised the no-metadata
    # path and never the read/write path -- which is why the published
    # conversion rate was measuring stubs. The complete copies are now in
    # the corpus. The no-metadata behaviour is still pinned, deliberately,
    # by flattened_no_instances.xml and
    # tests/test_missing_source_target_metadata.py. If a note below fires,
    # a corpus fixture has regressed to a stripped copy.
    if 'spark.sql("SELECT 1 AS placeholder")' in code:
        notes.append(f"{nb.name}: placeholder source read (no SOURCE/INSTANCE metadata in fixture)")
    if "No target defined" in code:
        notes.append(f"{nb.name}: no target write emitted (no TARGET/INSTANCE metadata in fixture)")

if not any_catalog_read:
    bad.append("corpus: no notebook contains a catalog-addressed spark.table() read")
if not any_expression_hit:
    bad.append(f"corpus: no known-expression fixture ({EXPRESSION_FIXTURES}) shows a converted withColumn")

print(f"notebooks={len(nbs)}")
if notes:
    print("KNOWN LIMITATIONS (disclosed, non-fatal):")
    for n in notes:
        print(f"  - {n}")

if bad:
    print("FAILURES:")
    print("\n".join(f"  - {b}" for b in bad))
    sys.exit(1)
PYEOF
then
  echo "  ok    verify"
else
  echo "  FAIL  verify  (see $OUT/verify.log)"
  err=$((err+1))
fi

echo
echo "notebooks=$(find "$OUT/notebooks" -name '*.ipynb' 2>/dev/null | wc -l | tr -d ' ') error=$err"
exit $err

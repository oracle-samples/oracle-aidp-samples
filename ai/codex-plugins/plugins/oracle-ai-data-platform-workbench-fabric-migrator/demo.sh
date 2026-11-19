#!/usr/bin/env bash
# End-to-end demo against every bundled input. No tenant, no credentials,
# no network.
set -euo pipefail

OUT="${1:-./demo-output}"
NS="${OCI_NAMESPACE:-demons}"
rm -rf "$OUT" && mkdir -p "$OUT"

# One input, staged by scripts/stage_demo_input.py: the synthetic Acme estate
# plus 78 real artifacts from 17 public repositories. Two demos confused people
# about which one counted, so there is one.
IN="${FABRIC_DEMO_INPUT:-./demo-input}"
if [ ! -d "$IN" ]; then
  echo "no $IN -- run 'make demo', or: python3 scripts/stage_demo_input.py" >&2
  exit 1
fi

echo "==> 1/4  inventory  ($IN)"
fabric-aidp inventory "$IN" -o "$OUT/inventory.json"

echo; echo "==> 2/4  plan"
fabric-aidp plan "$OUT/inventory.json" -o "$OUT/plan.json" --namespace "$NS"

echo; echo "==> 3/4  migrate  (offline; nothing is sent to AIDP)"
fabric-aidp migrate "$OUT/plan.json" -o "$OUT/migrated"

echo; echo "==> 4/4  verify"
fabric-aidp verify "$OUT/migrated"

echo
echo "Artifacts in $OUT/migrated:"
# *.job.json matters: it is the AIDP workflow job a pipeline becomes, and
# leaving it off this list made the feature look like it did not exist.
find "$OUT/migrated" -type f \( -name '*.py' -o -name '*.sql' -o -name '*.md' \
     -o -name '*.job.json' -o -name '*.html' \) | sort
echo
echo "Open the report:  open $OUT/migrated/report.html"
echo
echo "PASS means translated with no known issue detected -- not execution-verified."

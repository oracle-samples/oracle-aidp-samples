#!/usr/bin/env bash
# 5-minute live demo of aws-aidp.
# Run from the repo root: ./demo.sh
set -e

OUT=/tmp/aws-aidp-demo
rm -rf "$OUT" && mkdir -p "$OUT"

# Use a clearly-fake namespace in the demo so output looks intentional
# rather than the placeholder default.
export OCI_NAMESPACE="${OCI_NAMESPACE:-acme-demo-ns}"

bar() { printf '\n%s\n' "════════════════════════════════════════════════════════════"; }

bar; echo "  1/4  INVENTORY — scan AWS (fixture: Acme Insurance)"; bar
python3 -m aws_aidp.cli inventory --fixture demo -o "$OUT/inv.json"

bar; echo "  2/4  PLAN — manifest → mapping table"; bar
python3 -m aws_aidp.cli plan "$OUT/inv.json" -o "$OUT/plan.json"

bar; echo "  3/4  MIGRATE --filter athena --demo   (Athena SQL → Spark SQL)"; bar
python3 -m aws_aidp.cli migrate "$OUT/plan.json" --filter athena --demo -o "$OUT/migrated"

bar; echo "  3b   MIGRATE --filter glue --demo     (Glue ETL PySpark → Spark)"; bar
python3 -m aws_aidp.cli migrate "$OUT/plan.json" --filter glue --demo -o "$OUT/migrated-glue"

bar; echo "  4/4  VERIFY"; bar
echo "athena:"; python3 -m aws_aidp.cli verify "$OUT/migrated" --filter athena
echo; echo "glue:";   python3 -m aws_aidp.cli verify "$OUT/migrated-glue" --filter glue

bar; echo "  Sample review-gated Glue ETL (raw_to_curated_claims):"; bar
echo
echo "--- BEFORE (Glue PySpark) ---"
python3 -c "
import json
r = [x for x in json.load(open('$OUT/migrated-glue/report.json'))['results'] if 'raw_to_curated_claims' in x['asset_id']][0]
print(r['source_sql'])
"
echo
echo "--- AFTER (Spark on AIDP) ---"
python3 -c "
import json
r = [x for x in json.load(open('$OUT/migrated-glue/report.json'))['results'] if 'raw_to_curated_claims' in x['asset_id']][0]
print(r['translated_sql'])
"

bar; echo "  Sample PASS translation (1 of 20):"; bar
echo
echo "--- customer_revenue_monthly: BEFORE (Athena) ---"
python3 -c "
import json
nbs = json.load(open('$OUT/migrated/report.json'))['results']
r = [x for x in nbs if 'customer_revenue_monthly' in x.get('output_path','')][0]
print(r['source_sql'])
"
echo
echo "--- customer_revenue_monthly: AFTER (Spark SQL on AIDP) ---"
python3 -c "
import json
nbs = json.load(open('$OUT/migrated/report.json'))['results']
r = [x for x in nbs if 'customer_revenue_monthly' in x.get('output_path','')][0]
print(r['translated_sql'])
"
echo
bar; echo "  Artifacts at $OUT/"; bar
ls -la "$OUT"
echo
echo "  → $OUT/migrated/report.md        (Athena report)"
echo "  → $OUT/migrated-glue/report.md   (Glue report)  — open in any viewer"

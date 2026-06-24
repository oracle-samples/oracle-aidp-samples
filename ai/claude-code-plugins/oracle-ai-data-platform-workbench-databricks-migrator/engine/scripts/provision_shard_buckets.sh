#!/usr/bin/env bash
# provision_shard_buckets.sh
# ---------------------------------------------------------------------------
# Provision a pool of OCI Object Storage buckets for the Databricks->AIDP
# migration sharding strategy. Each shard absorbs its own per-bucket request
# budget so 2,500 parallel jobs do not collectively trip the OCI Java SDK
# CircuitBreaker on a single bucket.
#
# Idempotent: re-running skips buckets that already exist.
#
# Requires: oci-cli, jq.
#
# Usage:
#   ./provision_shard_buckets.sh \
#       --prefix customer-mig \
#       --count 16 \
#       --compartment ocid1.compartment.oc1..xxxxx \
#       --profile CUSTOMER \
#       --region <OCI_REGION>
#
# After running, configure the migration host:
#   export AIDP_SHARD_PREFIX=customer-mig
#   export AIDP_SHARD_COUNT=16
#   export AIDP_NAMESPACE=$(oci os ns get --profile CUSTOMER --query 'data' --raw-output)
#
# IAM note: the AIDP cluster's principal still needs a policy granting it
# 'manage objects' on the new buckets. See MIGRATION_THROTTLING.md.
# ---------------------------------------------------------------------------
set -euo pipefail

PREFIX=""
COUNT=""
COMPARTMENT=""
PROFILE="DEFAULT"
REGION=""
TIER="Standard"  # Standard | InfrequentAccess | Archive
DRY_RUN=0

usage() {
  cat <<EOF
Usage: $0 --prefix <name> --count <N> --compartment <ocid> [options]

Required:
  --prefix <name>          Bucket name prefix, e.g. 'customer-mig'
  --count <N>              Number of shard buckets (1..99)
  --compartment <ocid>     Target compartment OCID

Optional:
  --profile <name>         OCI CLI profile (default: DEFAULT)
  --region <region>        OCI region (default: profile default)
  --tier <Standard|...>    Storage tier (default: Standard)
  --dry-run                Print actions without creating buckets
EOF
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)      PREFIX="$2"; shift 2 ;;
    --count)       COUNT="$2"; shift 2 ;;
    --compartment) COMPARTMENT="$2"; shift 2 ;;
    --profile)     PROFILE="$2"; shift 2 ;;
    --region)      REGION="$2"; shift 2 ;;
    --tier)        TIER="$2"; shift 2 ;;
    --dry-run)     DRY_RUN=1; shift ;;
    -h|--help)     usage ;;
    *) echo "Unknown arg: $1"; usage ;;
  esac
done

[[ -z "$PREFIX" || -z "$COUNT" || -z "$COMPARTMENT" ]] && usage
[[ "$COUNT" =~ ^[0-9]+$ ]] || { echo "--count must be numeric"; exit 2; }
(( COUNT >= 1 && COUNT <= 99 )) || { echo "--count must be 1..99"; exit 2; }

OCI_ARGS=( --profile "$PROFILE" )
[[ -n "$REGION" ]] && OCI_ARGS+=( --region "$REGION" )

# Resolve namespace once
NAMESPACE=$(oci "${OCI_ARGS[@]}" os ns get --query 'data' --raw-output)
echo "[provision] namespace=$NAMESPACE compartment=$COMPARTMENT tier=$TIER"
echo "[provision] creating $COUNT buckets with prefix '$PREFIX'"

CREATED=0
SKIPPED=0
FAILED=0

for i in $(seq 0 $((COUNT - 1))); do
  NAME=$(printf "%s-%02d" "$PREFIX" "$i")

  # Check existence
  if oci "${OCI_ARGS[@]}" os bucket get \
        --bucket-name "$NAME" \
        --namespace-name "$NAMESPACE" \
        >/dev/null 2>&1; then
    echo "  [skip]   $NAME (exists)"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  if (( DRY_RUN )); then
    echo "  [dry]    would create $NAME"
    continue
  fi

  if oci "${OCI_ARGS[@]}" os bucket create \
        --name "$NAME" \
        --compartment-id "$COMPARTMENT" \
        --namespace-name "$NAMESPACE" \
        --storage-tier "$TIER" \
        >/dev/null; then
    echo "  [create] $NAME"
    CREATED=$((CREATED + 1))
  else
    echo "  [FAIL]   $NAME"
    FAILED=$((FAILED + 1))
  fi
done

echo
echo "[provision] done: created=$CREATED skipped=$SKIPPED failed=$FAILED"
echo "[provision] export this on the migration host:"
echo "    export AIDP_SHARD_PREFIX=$PREFIX"
echo "    export AIDP_SHARD_COUNT=$COUNT"
echo "    export AIDP_NAMESPACE=$NAMESPACE"

(( FAILED == 0 )) || exit 1

#!/usr/bin/env bash
# Upload a local landing-zone folder to an existing Object Storage bucket, under a prefix, keeping
# the folder layout. Files already in the bucket are never overwritten, so running it again after
# adding files uploads only the new ones.
#
#   BUCKET=my-landing-bucket PREFIX=landing ./upload_to_bucket.sh landing
#
# Uses the OCI CLI and your ~/.oci/config. Optional: OCI_PROFILE (default DEFAULT).
set -euo pipefail

LOCAL_FOLDER="${1:?usage: BUCKET=... PREFIX=... $0 <local folder>}"
# Stop with a message if BUCKET or PREFIX is not set.
: "${BUCKET:?set BUCKET to an existing bucket}"
: "${PREFIX:?set PREFIX, e.g. landing (the workflows need a prefix, not the bucket root)}"
PROFILE="${OCI_PROFILE:-DEFAULT}"
PREFIX="${PREFIX#/}"; PREFIX="${PREFIX%/}"      # "landing/" or "/landing" -> "landing"
[ -n "$PREFIX" ] || { echo "PREFIX must name a folder, e.g. landing (not the bucket root)" >&2; exit 1; }

NAMESPACE=$(oci os ns get --profile "$PROFILE" --query data --raw-output)

oci os object bulk-upload --bucket-name "$BUCKET" --src-dir "$LOCAL_FOLDER" \
    --object-prefix "$PREFIX/" --no-overwrite --profile "$PROFILE" >/dev/null

echo "objects under $PREFIX/:"
oci os object list --bucket-name "$BUCKET" --prefix "$PREFIX/" --all --profile "$PROFILE" \
    --query 'data[].name' --output table

echo
echo "SOURCE_ROOT for the setup workflow:  oci://$BUCKET@$NAMESPACE/$PREFIX/"

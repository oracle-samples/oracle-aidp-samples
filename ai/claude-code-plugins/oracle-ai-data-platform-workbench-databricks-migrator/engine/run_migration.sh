#!/bin/bash
# ============================================================================
# LEGACY — DEPRECATED ENTRY POINT. Do not use for new runs.
#
# This wrapper drives scripts/job_migrate.py directly (the legacy manifest
# path). The canonical runner is:
#
#   python3 scripts/job_migrate_from_workflow.py --config <customer>.yaml --job-key <workflow-key>
#
# (per-task clusters come from the workflow/manifest — never pass --cluster).
# This script is kept only for old runbooks.
# ============================================================================
#
# run_migration.sh — start a migration job with caffeinate (prevents macOS sleep)
#
# Usage:
#   ./run_migration.sh                                          # run all jobs
#   ./run_migration.sh --jobs ExampleJob        # specific job
#   ./run_migration.sh --jobs ExampleJob \
#       --start-task <task_id>  # resume from task
#
# Logs to a private temp file (override with MIGRATION_LOG=<path>); the path is printed below.

set -e

# Resolve all paths against this script's own directory, not the caller's cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"

echo "WARNING: run_migration.sh is a LEGACY, DEPRECATED entry point." >&2
echo "         Use the canonical runner instead:" >&2
echo "         python3 ${SCRIPT_DIR}/scripts/job_migrate_from_workflow.py --config <customer>.yaml --job-key <workflow-key>" >&2
echo "" >&2

LOG="${MIGRATION_LOG:-$(mktemp "${TMPDIR:-/tmp}/migration.XXXXXX")}"

echo "Starting migration — logging to $LOG"
echo "PID will be printed below. Kill with: kill <PID>"
echo ""

if [ "$#" -lt 1 ]; then
  echo "usage: $0 --manifest <path> [job_migrate.py args...]" >&2
  echo "       (legacy path; prefer job_migrate_from_workflow.py --config ... --job-key ...)" >&2
  exit 2
fi
caffeinate -i python3 -u "${SCRIPT_DIR}/scripts/job_migrate.py" \
  "$@" \
  > "$LOG" 2>&1 &

PID=$!
echo "PID: $PID"
echo "Tailing log (Ctrl+C to detach — job keeps running)..."
echo ""
sleep 3
tail -f "$LOG"

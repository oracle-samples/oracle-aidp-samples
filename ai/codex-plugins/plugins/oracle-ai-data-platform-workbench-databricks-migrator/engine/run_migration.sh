#!/bin/bash
# run_migration.sh â€” start a migration job with caffeinate (prevents macOS sleep)
#
# Usage:
#   ./run_migration.sh                                          # run all jobs
#   ./run_migration.sh --jobs ExampleJob        # specific job
#   ./run_migration.sh --jobs ExampleJob \
#       --start-task <task_id>  # resume from task
#
# Logs to a private temp file (override with MIGRATION_LOG=<path>); the path is printed below.

set -e

LOG="${MIGRATION_LOG:-$(mktemp "${TMPDIR:-/tmp}/migration.XXXXXX")}"

echo "Starting migration â€” logging to $LOG"
echo "PID will be printed below. Kill with: kill <PID>"
echo ""

caffeinate -i python3 -u $HOME/.aidp-migrator/engine/scripts/job_migrate.py \
  --manifest reports/example_job_manifest.json \
  "$@" \
  > "$LOG" 2>&1 &

PID=$!
echo "PID: $PID"
echo "Tailing log (Ctrl+C to detach â€” job keeps running)..."
echo ""
sleep 3
tail -f "$LOG"

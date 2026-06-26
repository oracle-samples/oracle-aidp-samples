#!/bin/bash
# run_migration.sh â€” start a migration job with caffeinate (prevents macOS sleep)
#
# Usage:
#   ./run_migration.sh                                          # run all jobs
#   ./run_migration.sh --jobs ExampleJob        # specific job
#   ./run_migration.sh --jobs ExampleJob \
#       --start-task Campaign_Ratio_Estimation_scratched_V2_Early  # resume from task
#
# Logs to /tmp/migration.log (overwrite). Tail with: tail -f /tmp/migration.log

set -e

LOG=/tmp/migration.log

echo "Starting migration â€” logging to $LOG"
echo "PID will be printed below. Kill with: pkill -f job_migrate.py"
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

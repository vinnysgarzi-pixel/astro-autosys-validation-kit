#!/usr/bin/env bash
# Sample shell workload for the script_runner DAG.
# Usage: sample_etl.sh <exit_code> <business_date>
# Writes to both stdout and stderr, then exits with the requested code —
# proving log capture and exit-code -> task-state mapping.

EXIT_CODE="${1:-0}"
BUSINESS_DATE="${2:-unknown}"

echo "STDOUT: sample_etl.sh starting for business date ${BUSINESS_DATE}"
echo "STDOUT: simulating extract/transform/load steps..."
sleep 2
echo "STDERR: this line goes to stderr and must be visible in the task log" >&2
echo "STDOUT: finished; exiting with code ${EXIT_CODE}"

exit "${EXIT_CODE}"

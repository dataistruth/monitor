#!/usr/bin/env bash
# Resolve e2e_hello_print → run-now → poll via REST API
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${1:-DEFAULT}"
exec python3 "$ROOT/e2e/scripts/run_job.py" --profile "$PROFILE"

#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${1:-DEFAULT}"
exec python3 "$ROOT/e2e/scripts/provision_pool.py" --profile "$PROFILE"

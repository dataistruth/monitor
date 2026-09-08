"""Monitor E2E — prints cluster key passed as first argument."""

import sys
from datetime import datetime, timezone

cluster_key = sys.argv[1] if len(sys.argv) > 1 else "UNKNOWN"
now = datetime.now(timezone.utc).isoformat()

print("=" * 60)
print("monitor E2E — hello_print")
print(f"cluster_key: {cluster_key}")
print(f"utc_time:    {now}")
print(f"argv:        {sys.argv}")
print("=" * 60)

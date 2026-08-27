#!/usr/bin/env bash
# Create all-purpose single-node j2 cluster (notebook attach) + ipacs_dev_team CAN_MANAGE.
#
# Usage:
#   ./api/create_all_purpose_single_j2_cluster.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLUSTER_ID="$(databricks clusters create \
  --json "@${SCRIPT_DIR}/cluster_create_all_purpose_single_j2.json" \
  | jq -r .cluster_id)"

if [[ -z "$CLUSTER_ID" || "$CLUSTER_ID" == "null" ]]; then
  echo "Cluster create failed — no cluster_id returned" >&2
  exit 1
fi

databricks permissions set clusters "$CLUSTER_ID" \
  --json "@${SCRIPT_DIR}/cluster_permissions_ipacs_dev_team.json"

echo "all_purpose cluster_id=$CLUSTER_ID"
echo "Wait for RUNNING, then notebook → Connect → Existing compute → All-purpose"

#!/usr/bin/env bash
# Delete all-purpose cluster by name (or CLUSTER_ID env var).
#
# Usage:
#   ./api/delete_all_purpose_single_j2_cluster.sh
#   CLUSTER_ID=0827-180538-8619uf18 ./api/delete_all_purpose_single_j2_cluster.sh
#   ./api/delete_all_purpose_single_j2_cluster.sh --permanent

set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-ipac_lab_single_j2_all_purpose}"
OLD_NAME="${OLD_NAME:-ipac_lab_single_j2_dedicated}"
PERMANENT=false

for arg in "$@"; do
  case "$arg" in
    --permanent) PERMANENT=true ;;
  esac
done

delete_one() {
  local id="$1"
  local state
  state="$(databricks clusters get "$id" -o json | jq -r .state)"
  echo "Deleting cluster_id=$id state=$state"

  if [[ "$state" == "TERMINATED" ]]; then
    if [[ "$PERMANENT" == true ]]; then
      databricks clusters permanent-delete "$id"
      echo "Permanently deleted $id"
    else
      echo "Already TERMINATED — use --permanent to remove record"
    fi
  else
    databricks clusters delete "$id"
    echo "Delete requested for $id"
  fi
}

if [[ -n "${CLUSTER_ID:-}" ]]; then
  delete_one "$CLUSTER_ID"
  exit 0
fi

for name in "$CLUSTER_NAME" "$OLD_NAME"; do
  id="$(databricks clusters list -o json | jq -r --arg n "$name" '
    .[] | select(.cluster_name == $n) | .cluster_id' | head -1)"
  if [[ -n "$id" && "$id" != "null" ]]; then
    delete_one "$id"
  fi
done

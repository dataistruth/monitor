#!/usr/bin/env python3
"""Provision e2e_standard_pool via Databricks REST API."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from databricks_client import DatabricksClient, resolve_auth

ROOT = Path(__file__).resolve().parents[1]
POOL_SPEC = ROOT / "config" / "instance_pool_azure.json"


def load_pool_spec(path: Path = POOL_SPEC) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def find_pool_id(pools: list, pool_name: str) -> str | None:
    for pool in pools:
        if pool.get("instance_pool_name") == pool_name:
            return pool.get("instance_pool_id")
    return None


def print_next_steps(pool_id: str, profile: str | None) -> None:
    profile_flag = f" --profile {profile}" if profile else ""
    print("\n--- Next: deploy e2e bundle ---")
    print(f"cd {ROOT}")
    print(
        f"databricks bundle validate --target dev{profile_flag} "
        f"--var standard_pool_id={pool_id}"
    )
    print(
        f"databricks bundle deploy --target dev{profile_flag} "
        f"--var standard_pool_id={pool_id}"
    )
    print("\n--- Then run job via API ---")
    print(f"python3 {ROOT}/scripts/run_job.py --profile {profile or 'DEFAULT'}")
    print(f"export STANDARD_POOL_ID={pool_id}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Provision e2e instance pool via REST API")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks profile (~/.databrickscfg)")
    parser.add_argument("--host")
    parser.add_argument("--token")
    parser.add_argument("--spec", type=Path, default=POOL_SPEC)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    spec = load_pool_spec(args.spec)
    pool_name = spec["instance_pool_name"]
    print(
        f"Pool spec: {pool_name} ({spec['node_type_id']}, "
        f"idle={spec['min_idle_instances']}, max={spec['max_capacity']})"
    )

    if args.dry_run:
        print("Dry run — POST /api/2.0/instance-pools/create")
        print(json.dumps(spec, indent=2))
        return 0

    host, token = resolve_auth(args.host, args.token, args.profile)
    client = DatabricksClient(host, token)

    existing = find_pool_id(client.list_instance_pools(), pool_name)
    if existing:
        print(f"Pool exists: {pool_name} -> {existing}")
        print_next_steps(existing, args.profile)
        return 0

    pool_id = client.create_instance_pool(spec)
    print(f"Created pool: {pool_name} -> {pool_id}")
    print_next_steps(pool_id, args.profile)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, KeyError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

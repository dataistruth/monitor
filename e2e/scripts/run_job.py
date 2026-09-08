#!/usr/bin/env python3
"""Resolve job by name → run-now → poll (Databricks REST API)."""

from __future__ import annotations

import argparse
import json
import sys

from databricks_client import DatabricksClient, resolve_auth

DEFAULT_JOB_NAME = "e2e_hello_print"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run monitor e2e job via REST API")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks profile")
    parser.add_argument("--host")
    parser.add_argument("--token")
    parser.add_argument("--job-name", default=DEFAULT_JOB_NAME)
    parser.add_argument("--job-id", type=int, help="Skip list; use this job_id")
    parser.add_argument("--poll-interval", type=int, default=15)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        job_id = args.job_id or 0
        print(f"Dry run — POST /api/2.1/jobs/run-now job_id={job_id} name={args.job_name}")
        return 0

    host, token = resolve_auth(args.host, args.token, args.profile)
    client = DatabricksClient(host, token)

    job_id = args.job_id or client.newest_job_id_by_name(args.job_name)
    print(f"job_name={args.job_name} job_id={job_id}")

    run_id = client.run_now(job_id)
    print(f"run_id={run_id}")

    final = client.poll_run(run_id, interval_seconds=args.poll_interval, timeout_seconds=args.timeout)
    print(json.dumps(final.get("state", {}), indent=2))

    result = final.get("state", {}).get("result_state")
    return 0 if result == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, FileNotFoundError, KeyError, RuntimeError, TimeoutError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

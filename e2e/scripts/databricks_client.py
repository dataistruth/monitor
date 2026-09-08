#!/usr/bin/env python3
"""Minimal Databricks REST client (stdlib only) for monitor e2e scripts."""

from __future__ import annotations

import configparser
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

DATABRICKS_CFG = Path.home() / ".databrickscfg"


def load_profile_auth(profile: str) -> tuple[str, str]:
    if not DATABRICKS_CFG.exists():
        raise FileNotFoundError(f"Missing {DATABRICKS_CFG}")
    parser = configparser.ConfigParser()
    parser.read(DATABRICKS_CFG)
    if profile not in parser:
        raise KeyError(f"Profile '{profile}' not found in {DATABRICKS_CFG}")
    section = parser[profile]
    host = section.get("host", "").strip()
    token = section.get("token", "").strip()
    if not host or not token:
        raise ValueError(f"Profile '{profile}' must define host and token")
    return host.rstrip("/"), token


def resolve_auth(
    host: str | None,
    token: str | None,
    profile: str | None,
) -> tuple[str, str]:
    if host and token:
        return host.rstrip("/"), token
    env_host = os.environ.get("DATABRICKS_HOST", "").strip()
    env_token = os.environ.get("DATABRICKS_TOKEN", "").strip()
    if env_host and env_token:
        return env_host.rstrip("/"), env_token
    if profile:
        return load_profile_auth(profile)
    raise ValueError(
        "Provide --host and --token, set DATABRICKS_HOST + DATABRICKS_TOKEN, or use --profile"
    )


class DatabricksClient:
    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self.token = token

    def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> Any:
        url = f"{self.host}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        data = None
        headers = {"Authorization": f"Bearer {self.token}"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {detail}") from exc

    def list_instance_pools(self) -> list[dict[str, Any]]:
        data = self.request("GET", "/api/2.0/instance-pools/list")
        if isinstance(data, list):
            return data
        return data.get("instance_pools", [])

    def create_instance_pool(self, spec: dict[str, Any]) -> str:
        data = self.request("POST", "/api/2.0/instance-pools/create", body=spec)
        pool_id = data.get("instance_pool_id")
        if not pool_id:
            raise RuntimeError(f"Create response missing instance_pool_id: {data}")
        return pool_id

    def list_jobs_by_name(self, name: str, limit: int = 100) -> list[dict[str, Any]]:
        data = self.request(
            "GET",
            "/api/2.1/jobs/list",
            query={"name": name, "limit": str(limit)},
        )
        return data.get("jobs", [])

    def newest_job_id_by_name(self, name: str) -> int:
        jobs = self.list_jobs_by_name(name)
        if not jobs:
            raise RuntimeError(f"No job found with name: {name}")
        jobs.sort(key=lambda j: j.get("created_time", 0), reverse=True)
        return int(jobs[0]["job_id"])

    def run_now(self, job_id: int) -> int:
        data = self.request("POST", "/api/2.1/jobs/run-now", body={"job_id": job_id})
        run_id = data.get("run_id")
        if run_id is None:
            raise RuntimeError(f"run-now missing run_id: {data}")
        return int(run_id)

    def get_run(self, run_id: int) -> dict[str, Any]:
        return self.request("GET", "/api/2.1/jobs/runs/get", query={"run_id": str(run_id)})

    def poll_run(
        self,
        run_id: int,
        interval_seconds: int = 15,
        timeout_seconds: int = 3600,
    ) -> dict[str, Any]:
        import time

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            run = self.get_run(run_id)
            life = run.get("state", {}).get("life_cycle_state")
            result = run.get("state", {}).get("result_state")
            print(f"run_id={run_id} life_cycle={life} result={result}")
            if life in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
                return run
            time.sleep(interval_seconds)
        raise TimeoutError(f"Run {run_id} did not finish within {timeout_seconds}s")

"""Shared helpers for the monitor refresh loop."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

_PLACEHOLDER = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")
_DEFAULTS = {"last_n_runs": "1000"}

DEFAULT_SQL_SCHEDULE = [
    {
        "file": "001_v_cluster_hourly.sql",
        "table": "v_cluster_hourly",
        "interval_seconds": 21600,
        "run_on_start": True,
    },
    {
        "file": "002_unified_runs.sql",
        "table": "unified_runs",
        "interval_seconds": 21600,
        "run_on_start": True,
    },
    {
        "file": "003_compute_usage_by_type.sql",
        "table": "compute_usage_by_type",
        "interval_seconds": 21600,
        "run_on_start": True,
    },
    {
        "file": "004_api_submitted_runs.sql",
        "table": "api_submitted_runs",
        "interval_seconds": 0,
        "run_on_start": True,
    },
    {
        "file": "005_api_submitted_run_params.sql",
        "table": "api_submitted_run_params",
        "interval_seconds": 0,
        "run_on_start": True,
    },
]


def resolve_config_dir(notebook_path: str) -> Path:
    parts = notebook_path.strip("/").split("/")
    if "src" in parts:
        src_idx = parts.index("src")
        cfg_parts = parts[:src_idx] + ["config"]
        return Path("/" + "/".join(cfg_parts))
    return Path("/Workspace") / "config"


def _parse_refresh_yaml(text: str) -> list[dict]:
    """Minimal parser for config/refresh.yml (no PyYAML required)."""
    items: list[dict] = []
    current: dict | None = None
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip() or line.strip() == "sql:":
            continue
        stripped = line.strip()
        if stripped.startswith("- file:"):
            if current:
                items.append(current)
            current = {
                "file": stripped.split(":", 1)[1].strip(),
                "table": "",
                "interval_seconds": 0,
                "run_on_start": True,
            }
            continue
        if current is None or ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key == "table":
            current["table"] = value
        elif key == "interval_seconds":
            current["interval_seconds"] = int(value)
        elif key == "run_on_start":
            current["run_on_start"] = value.lower() in {"true", "yes", "1"}
    if current:
        items.append(current)
    for item in items:
        if not item["table"]:
            item["table"] = Path(item["file"]).stem
    return items


def load_sql_schedule(config_path: Path | None) -> list[dict]:
    if config_path is None or not config_path.exists():
        return [dict(item) for item in DEFAULT_SQL_SCHEDULE]

    text = config_path.read_text(encoding="utf-8")
    items: list[dict] = []
    try:
        import yaml

        data = yaml.safe_load(text) or {}
        items = data.get("sql") or []
        parsed = []
        for item in items:
            parsed.append(
                {
                    "file": str(item["file"]),
                    "table": str(item.get("table") or Path(item["file"]).stem),
                    "interval_seconds": int(item.get("interval_seconds") or 0),
                    "run_on_start": bool(item.get("run_on_start", True)),
                }
            )
        items = parsed
    except Exception:
        items = _parse_refresh_yaml(text)

    if not items:
        return [dict(item) for item in DEFAULT_SQL_SCHEDULE]
    return items


def sql_is_due(
    last_ran_at: datetime | None,
    interval_seconds: int,
    run_on_start: bool,
    now: datetime,
) -> bool:
    if last_ran_at is None:
        return run_on_start
    if interval_seconds <= 0:
        return True
    return (now - last_ran_at).total_seconds() >= interval_seconds


def load_sql(path: Path, replacements: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    merged = {**_DEFAULTS, **replacements}
    for key, value in merged.items():
        text = text.replace("{{" + key + "}}", str(value))
    leftover = _PLACEHOLDER.findall(text)
    if leftover:
        raise ValueError(f"Unreplaced SQL placeholders in {path}: {leftover}")
    return text


def resolve_sql_dir(notebook_path: str) -> Path:
    """Resolve src/sql from a Workspace notebook path like /Workspace/.../src/notebooks/foo."""
    parts = notebook_path.strip("/").split("/")
    if "src" in parts:
        src_idx = parts.index("src")
        sql_parts = parts[:src_idx] + ["src", "sql"]
        return Path("/" + "/".join(sql_parts))
    return Path("/Workspace") / "src" / "sql"

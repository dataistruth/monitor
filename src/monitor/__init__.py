"""Shared helpers for the monitor refresh loop."""

from __future__ import annotations

import re
from pathlib import Path

_PLACEHOLDER = re.compile(r"\{\{([a-zA-Z0-9_]+)\}\}")
_DEFAULTS = {"last_n_runs": "100"}


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

"""Shared helpers for the monitor refresh loop."""

from __future__ import annotations

from pathlib import Path


def load_sql(path: Path, replacements: dict[str, str]) -> str:
    text = path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        text = text.replace(f"{{{{{key}}}}}", value)
    return text


def resolve_sql_dir(notebook_path: str) -> Path:
    """Resolve src/sql from a Workspace notebook path like /Workspace/.../src/notebooks/foo."""
    # notebook_path is typically /Workspace/<bundle>/files/src/notebooks/...
    parts = notebook_path.strip("/").split("/")
    if "src" in parts:
        src_idx = parts.index("src")
        sql_parts = parts[:src_idx] + ["src", "sql"]
        return Path("/" + "/".join(sql_parts))
    return Path("/Workspace") / "src" / "sql"

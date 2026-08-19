# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor refresh loop
# MAGIC
# MAGIC Loop sleep is `monitor_frequency_seconds`. Each SQL file has its own cadence
# MAGIC in `config/refresh.yml` (`interval_seconds`: 0 = every loop).
# MAGIC Cluster / process / DBU SQLs default to first loop, then every 6 hours.
# MAGIC Job API SQLs default to every loop.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "monitoring", "Schema")
dbutils.widgets.text("workspace_id", "", "Workspace ID")
dbutils.widgets.text("monitor_frequency_seconds", "300", "Loop frequency (seconds)")
dbutils.widgets.text("api_submit_last_n_runs", "1000", "Last N API-submitted runs")
dbutils.widgets.text("max_iterations", "0", "Max iterations (0 = forever)")

# COMMAND ----------

import importlib
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
repo_root = "/Workspace" + nb_path.rsplit("/src/", 1)[0]
src_root = f"{repo_root}/src"
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from monitor import (
    load_sql,
    load_sql_schedule,
    resolve_config_dir,
    resolve_sql_dir,
    sql_is_due,
)

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
workspace_id = dbutils.widgets.get("workspace_id").strip()
frequency_seconds = int(dbutils.widgets.get("monitor_frequency_seconds"))
last_n_runs = int(dbutils.widgets.get("api_submit_last_n_runs"))
max_iterations = int(dbutils.widgets.get("max_iterations"))

if not workspace_id:
    raise ValueError("workspace_id is required for cluster monitoring SQL.")
if frequency_seconds < 60:
    raise ValueError("monitor_frequency_seconds must be >= 60.")
if last_n_runs < 1:
    raise ValueError("api_submit_last_n_runs must be >= 1.")

sql_dir = resolve_sql_dir(f"/Workspace{nb_path}")
config_dir = resolve_config_dir(f"/Workspace{nb_path}")
schedule_path = config_dir / "refresh.yml"
schedule = load_sql_schedule(schedule_path)
last_ran: dict[str, datetime] = {}

replacements = {
    "catalog": catalog,
    "schema": schema,
    "workspace_id": workspace_id,
    "last_n_runs": str(last_n_runs),
}

print(f"SQL dir: {sql_dir}")
print(f"Schedule: {schedule_path}")
print(f"Target: {catalog}.{schema}")
print(
    f"Loop: {frequency_seconds}s | last_n_runs: {last_n_runs} | "
    f"max_iterations: {max_iterations or 'forever'}"
)
for item in schedule:
    cadence = "every loop" if item["interval_seconds"] <= 0 else f"{item['interval_seconds']}s"
    print(f"  {item['file']}: {cadence} (run_on_start={item['run_on_start']})")

# COMMAND ----------

iteration = 0
while True:
    iteration += 1
    now = datetime.now(timezone.utc)
    print(f"\n=== iteration {iteration} @ {now.isoformat()} ===")

    import monitor as monitor_pkg

    importlib.reload(monitor_pkg)
    load_sql = monitor_pkg.load_sql
    load_sql_schedule = monitor_pkg.load_sql_schedule
    sql_is_due = monitor_pkg.sql_is_due
    schedule = load_sql_schedule(schedule_path)

    for item in schedule:
        sql_file = item["file"]
        interval = int(item["interval_seconds"])
        if not sql_is_due(last_ran.get(sql_file), interval, item["run_on_start"], now):
            next_in = interval - (now - last_ran[sql_file]).total_seconds()
            print(f"Skip {sql_file} (next in {int(next_in)}s)")
            continue

        sql_path = sql_dir / sql_file
        spark.sql(load_sql(sql_path, replacements))
        last_ran[sql_file] = now
        print(f"Refreshed {catalog}.{schema}.{item['table']}")

    if max_iterations > 0 and iteration >= max_iterations:
        print(f"Reached max_iterations={max_iterations}; exiting.")
        break

    print(f"Sleeping {frequency_seconds}s...")
    time.sleep(frequency_seconds)

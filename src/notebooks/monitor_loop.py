# Databricks notebook source
# MAGIC %md
# MAGIC # Monitor refresh loop
# MAGIC
# MAGIC Refreshes cluster + process monitoring objects, then sleeps
# MAGIC `monitor_frequency_seconds` and repeats until cancelled (or max_iterations).

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "monitoring", "Schema")
dbutils.widgets.text("workspace_id", "", "Workspace ID")
dbutils.widgets.text("monitor_frequency_seconds", "300", "Loop frequency (seconds)")
dbutils.widgets.text("max_iterations", "0", "Max iterations (0 = forever)")

# COMMAND ----------

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

from monitor import load_sql, resolve_sql_dir

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
workspace_id = dbutils.widgets.get("workspace_id").strip()
frequency_seconds = int(dbutils.widgets.get("monitor_frequency_seconds"))
max_iterations = int(dbutils.widgets.get("max_iterations"))

if not workspace_id:
    raise ValueError("workspace_id is required for cluster monitoring SQL.")
if frequency_seconds < 60:
    raise ValueError("monitor_frequency_seconds must be >= 60.")

sql_dir = resolve_sql_dir(f"/Workspace{nb_path}")

replacements = {
    "catalog": catalog,
    "schema": schema,
    "workspace_id": workspace_id,
}

cluster_sql_path = sql_dir / "001_v_cluster_hourly.sql"
process_sql_path = sql_dir / "002_unified_runs.sql"
compute_usage_sql_path = sql_dir / "003_compute_usage_by_type.sql"

print(f"SQL dir: {sql_dir}")
print(f"Target: {catalog}.{schema}")
print(f"Frequency: {frequency_seconds}s | max_iterations: {max_iterations or 'forever'}")

# COMMAND ----------

iteration = 0
while True:
    iteration += 1
    started = datetime.now(timezone.utc).isoformat()
    print(f"\n=== iteration {iteration} @ {started} ===")

    spark.sql(load_sql(cluster_sql_path, replacements))
    print(f"Refreshed {catalog}.{schema}.v_cluster_hourly")

    spark.sql(load_sql(process_sql_path, replacements))
    print(f"Refreshed {catalog}.{schema}.unified_runs")

    spark.sql(load_sql(compute_usage_sql_path, replacements))
    print(f"Refreshed {catalog}.{schema}.compute_usage_by_type")

    if max_iterations > 0 and iteration >= max_iterations:
        print(f"Reached max_iterations={max_iterations}; exiting.")
        break

    print(f"Sleeping {frequency_seconds}s...")
    time.sleep(frequency_seconds)

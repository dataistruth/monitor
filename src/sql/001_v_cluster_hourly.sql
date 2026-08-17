-- Cluster hourly utilization from system.compute tables.
-- Placeholders: {{catalog}}, {{schema}}, {{workspace_id}}

CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}};

CREATE OR REPLACE VIEW {{catalog}}.{{schema}}.v_cluster_hourly AS
WITH latest_clusters AS (
  SELECT *
  FROM system.compute.clusters
  WHERE workspace_id = '{{workspace_id}}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, cluster_id
    ORDER BY change_time DESC
  ) = 1
)
SELECT
  n.workspace_id,
  n.cluster_id,
  c.cluster_name,
  c.cluster_source,
  c.owned_by,
  c.dbr_version,
  DATE_TRUNC('hour', n.start_time) AS hour_utc,

  COUNT(DISTINCT DATE_TRUNC('minute', n.start_time)) AS active_minutes,
  COUNT(DISTINCT n.instance_id) AS active_instances,
  SUM(TIMESTAMPDIFF(SECOND, n.start_time, n.end_time)) / 3600.0 AS node_hours,

  ROUND(AVG(n.cpu_user_percent + n.cpu_system_percent), 2) AS avg_cpu_pct,
  ROUND(MAX(n.cpu_user_percent + n.cpu_system_percent), 2) AS peak_cpu_pct,
  ROUND(AVG(n.mem_used_percent), 2) AS avg_memory_pct,
  ROUND(MAX(n.mem_used_percent), 2) AS peak_memory_pct,

  SUM(n.network_received_bytes) AS network_received_bytes,
  SUM(n.network_sent_bytes) AS network_sent_bytes
FROM system.compute.node_timeline n
LEFT JOIN latest_clusters c
  ON n.workspace_id = c.workspace_id
 AND n.cluster_id = c.cluster_id
WHERE n.workspace_id = '{{workspace_id}}'
  AND n.start_time >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
GROUP BY
  n.workspace_id,
  n.cluster_id,
  c.cluster_name,
  c.cluster_source,
  c.owned_by,
  c.dbr_version,
  DATE_TRUNC('hour', n.start_time);

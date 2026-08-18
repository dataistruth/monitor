-- Cluster hourly utilization from system.compute tables.
-- Placeholders: {{catalog}}, {{schema}}, {{workspace_id}}
-- Note: system.compute covers classic all-purpose/jobs clusters only (not serverless).

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
  CASE c.cluster_source
    WHEN 'UI' THEN 'ALL_PURPOSE'
    WHEN 'API' THEN 'ALL_PURPOSE'
    WHEN 'JOB' THEN 'JOBS'
    WHEN 'PIPELINE' THEN 'PIPELINE'
    WHEN 'PIPELINE_MAINTENANCE' THEN 'PIPELINE'
    ELSE COALESCE(c.cluster_source, 'OTHER')
  END AS compute_purpose,
  c.data_security_mode,
  CASE c.data_security_mode
    WHEN 'SINGLE_USER' THEN 'SINGLE_USER'
    WHEN 'LEGACY_SINGLE_USER' THEN 'SINGLE_USER'
    WHEN 'USER_ISOLATION' THEN 'STANDARD'
    WHEN 'NONE' THEN 'NO_ISOLATION_SHARED'
    WHEN 'LEGACY_PASSTHROUGH' THEN 'STANDARD_LEGACY'
    WHEN 'LEGACY_TABLE_ACL' THEN 'CUSTOM'
    ELSE COALESCE(c.data_security_mode, 'UNKNOWN')
  END AS access_mode,
  c.owned_by,
  c.dbr_version,
  c.driver_node_type,
  c.worker_node_type,
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
  c.data_security_mode,
  c.owned_by,
  c.dbr_version,
  c.driver_node_type,
  c.worker_node_type,
  DATE_TRUNC('hour', n.start_time);

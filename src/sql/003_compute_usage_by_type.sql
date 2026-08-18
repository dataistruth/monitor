-- Compute usage by type (Serverless / Jobs classic / All-purpose) from billing.
-- Placeholders: {{catalog}}, {{schema}}, {{workspace_id}}
-- Complements v_cluster_hourly, which only covers classic cluster node timelines.

CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.compute_usage_by_type AS
SELECT
  u.workspace_id,
  u.usage_date,
  CASE
    WHEN COALESCE(u.product_features.is_serverless, false)
      OR upper(u.sku_name) LIKE '%SERVERLESS%'
      THEN 'SERVERLESS'
    WHEN u.billing_origin_product = 'JOBS'
      THEN 'JOBS_CLASSIC'
    WHEN u.billing_origin_product IN ('INTERACTIVE', 'ALL_PURPOSE')
      THEN 'ALL_PURPOSE'
    WHEN u.billing_origin_product = 'SQL'
      THEN 'SQL_WAREHOUSE'
    ELSE COALESCE(u.billing_origin_product, 'OTHER')
  END AS compute_type,
  u.billing_origin_product,
  u.sku_name,
  COALESCE(u.usage_metadata.job_id, CAST(NULL AS STRING)) AS job_id,
  COALESCE(u.usage_metadata.job_name, CAST(NULL AS STRING)) AS job_name,
  COALESCE(u.usage_metadata.cluster_id, CAST(NULL AS STRING)) AS cluster_id,
  COALESCE(u.usage_metadata.notebook_id, CAST(NULL AS STRING)) AS notebook_id,
  COALESCE(u.identity_metadata.run_as, CAST(NULL AS STRING)) AS run_as,
  SUM(u.usage_quantity) AS usage_quantity,
  MAX(u.usage_unit) AS usage_unit,
  COUNT(*) AS usage_records
FROM system.billing.usage u
WHERE u.workspace_id = '{{workspace_id}}'
  AND u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND u.usage_unit = 'DBU'
GROUP BY
  u.workspace_id,
  u.usage_date,
  CASE
    WHEN COALESCE(u.product_features.is_serverless, false)
      OR upper(u.sku_name) LIKE '%SERVERLESS%'
      THEN 'SERVERLESS'
    WHEN u.billing_origin_product = 'JOBS'
      THEN 'JOBS_CLASSIC'
    WHEN u.billing_origin_product IN ('INTERACTIVE', 'ALL_PURPOSE')
      THEN 'ALL_PURPOSE'
    WHEN u.billing_origin_product = 'SQL'
      THEN 'SQL_WAREHOUSE'
    ELSE COALESCE(u.billing_origin_product, 'OTHER')
  END,
  u.billing_origin_product,
  u.sku_name,
  COALESCE(u.usage_metadata.job_id, CAST(NULL AS STRING)),
  COALESCE(u.usage_metadata.job_name, CAST(NULL AS STRING)),
  COALESCE(u.usage_metadata.cluster_id, CAST(NULL AS STRING)),
  COALESCE(u.usage_metadata.notebook_id, CAST(NULL AS STRING)),
  COALESCE(u.identity_metadata.run_as, CAST(NULL AS STRING));

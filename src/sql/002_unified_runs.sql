-- Unified process runs: jobs, pipelines, notebook adhoc.
-- Placeholders: {{catalog}}, {{schema}}

CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}};

CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.unified_runs AS
WITH latest_jobs AS (
  SELECT *
  FROM system.lakeflow.jobs
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, job_id
    ORDER BY change_time DESC
  ) = 1
),

latest_pipelines AS (
  SELECT *
  FROM system.lakeflow.pipelines
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, pipeline_id
    ORDER BY change_time DESC
  ) = 1
),

job_runs AS (
  SELECT
    r.account_id,
    r.workspace_id,
    'JOB' AS run_category,
    CAST(r.job_id AS STRING) AS entity_id,
    CAST(r.run_id AS STRING) AS run_id,
    COALESCE(j.name, r.run_name) AS entity_name,
    'RUN' AS run_grain,
    MIN(r.period_start_time) AS start_time,
    MAX(r.period_end_time) AS end_time,
    COALESCE(
      MAX_BY(r.result_state, r.period_end_time)
        FILTER (WHERE r.result_state IS NOT NULL),
      'RUNNING'
    ) AS status,
    MAX_BY(r.trigger_type, r.period_end_time) AS trigger_type,
    MAX_BY(r.run_type, r.period_end_time) AS execution_type,
    CAST(NULL AS STRING) AS notebook_id,
    CAST(NULL AS STRING) AS pipeline_id,
    MAX_BY(r.termination_code, r.period_end_time)
      FILTER (WHERE r.termination_code IS NOT NULL) AS termination_code,
    SUM(TIMESTAMPDIFF(
      SECOND, r.period_start_time, r.period_end_time
    )) AS duration_seconds,
    CAST(NULL AS STRING) AS executed_by,
    CAST(NULL AS STRING) AS command_text
  FROM system.lakeflow.job_run_timeline r
  LEFT JOIN latest_jobs j
    ON r.workspace_id = j.workspace_id
   AND r.job_id = j.job_id
  WHERE r.period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  GROUP BY
    r.account_id, r.workspace_id, r.job_id, r.run_id,
    r.run_name, j.name
),

pipeline_runs AS (
  SELECT
    p.account_id,
    p.workspace_id,
    'PIPELINE' AS run_category,
    CAST(p.pipeline_id AS STRING) AS entity_id,
    CAST(p.update_id AS STRING) AS run_id,
    lp.name AS entity_name,
    'UPDATE' AS run_grain,
    MIN(p.period_start_time) AS start_time,
    MAX(p.period_end_time) AS end_time,
    COALESCE(
      MAX_BY(p.result_state, p.period_end_time)
        FILTER (WHERE p.result_state IS NOT NULL),
      'RUNNING'
    ) AS status,
    MAX_BY(p.trigger_type, p.period_end_time) AS trigger_type,
    MAX_BY(p.update_type, p.period_end_time) AS execution_type,
    CAST(NULL AS STRING) AS notebook_id,
    CAST(p.pipeline_id AS STRING) AS pipeline_id,
    CAST(NULL AS STRING) AS termination_code,
    SUM(TIMESTAMPDIFF(
      SECOND, p.period_start_time, p.period_end_time
    )) AS duration_seconds,
    p.run_as_user_name AS executed_by,
    CAST(NULL AS STRING) AS command_text
  FROM system.lakeflow.pipeline_update_timeline p
  LEFT JOIN latest_pipelines lp
    ON p.workspace_id = lp.workspace_id
   AND p.pipeline_id = lp.pipeline_id
  WHERE p.period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
  GROUP BY
    p.account_id, p.workspace_id, p.pipeline_id, p.update_id,
    lp.name, p.run_as_user_name
),

notebook_adhoc AS (
  SELECT
    a.account_id,
    a.workspace_id,
    'NOTEBOOK_ADHOC' AS run_category,
    CAST(a.request_params['notebookId'] AS STRING) AS entity_id,
    CAST(a.request_params['commandId'] AS STRING) AS run_id,
    CONCAT(
      'Notebook ',
      CAST(a.request_params['notebookId'] AS STRING)
    ) AS entity_name,
    'CELL_COMMAND' AS run_grain,
    timestampadd(
      SECOND,
      -try_cast(a.request_params['executionTime'] AS BIGINT),
      a.event_time
    ) AS start_time,
    a.event_time AS end_time,
    CASE upper(a.request_params['status'])
      WHEN 'FINISHED'  THEN 'SUCCEEDED'
      WHEN 'SUCCESS'   THEN 'SUCCEEDED'
      WHEN 'FAILED'    THEN 'FAILED'
      WHEN 'ERROR'     THEN 'FAILED'
      WHEN 'CANCELED'  THEN 'CANCELED'
      WHEN 'CANCELLED' THEN 'CANCELED'
      ELSE upper(a.request_params['status'])
    END AS status,
    'INTERACTIVE' AS trigger_type,
    'ADHOC' AS execution_type,
    CAST(a.request_params['notebookId'] AS STRING) AS notebook_id,
    CAST(NULL AS STRING) AS pipeline_id,
    CAST(NULL AS STRING) AS termination_code,
    try_cast(a.request_params['executionTime'] AS BIGINT)
      AS duration_seconds,
    a.user_identity.email AS executed_by,
    CAST(a.request_params['commandText'] AS STRING) AS command_text
  FROM system.access.audit a
  WHERE a.service_name = 'notebook'
    AND a.action_name = 'runCommand'
    AND a.event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
)

SELECT * FROM job_runs
UNION ALL
SELECT * FROM pipeline_runs
UNION ALL
SELECT * FROM notebook_adhoc;

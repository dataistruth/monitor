-- Exploded parameters for the last 100 API-submitted runs.
-- Placeholders: {{catalog}}, {{schema}}, {{workspace_id}}

CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.api_submitted_run_params AS
WITH job_params AS (
  SELECT
    r.workspace_id,
    r.job_id,
    r.run_id,
    r.run_name,
    r.submit_channel,
    r.run_type,
    r.status,
    r.start_time,
    'JOB' AS parameter_scope,
    CAST(NULL AS STRING) AS task_key,
    p.key AS parameter_name,
    p.value AS parameter_value
  FROM {{catalog}}.{{schema}}.api_submitted_runs r
  LATERAL VIEW OUTER explode(COALESCE(r.job_parameters, map())) p AS key, value
  WHERE p.key IS NOT NULL
),

task_params AS (
  SELECT
    t.workspace_id,
    t.job_id,
    CAST(t.job_run_id AS STRING) AS run_id,
    r.run_name,
    r.submit_channel,
    r.run_type,
    r.status,
    r.start_time,
    'TASK' AS parameter_scope,
    t.task_key,
    p.key AS parameter_name,
    p.value AS parameter_value
  FROM system.lakeflow.job_task_run_timeline t
  INNER JOIN {{catalog}}.{{schema}}.api_submitted_runs r
    ON t.workspace_id = r.workspace_id
   AND CAST(t.job_run_id AS STRING) = r.run_id
  LATERAL VIEW OUTER explode(COALESCE(t.task_parameters, map())) p AS key, value
  WHERE t.workspace_id = '{{workspace_id}}'
    AND p.key IS NOT NULL
)

SELECT * FROM job_params
UNION ALL
SELECT * FROM task_params;

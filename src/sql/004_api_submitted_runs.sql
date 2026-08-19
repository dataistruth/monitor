-- API-submitted job runs (Submit Run / Run Now) — last {{last_n_runs}}.
-- Includes job_nm, parameters, status, DBU consumption, and clusters used.
-- Placeholders: {{catalog}}, {{schema}}, {{workspace_id}}, {{last_n_runs}}
--
-- SUBMIT_RUN = POST /api/2.x/jobs/runs/submit (e.g. Airflow DatabricksSubmitRunOperator)
-- ONETIME    = one-time manual or API trigger (e.g. Airflow DatabricksRunNowOperator)

CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.api_submitted_runs AS
WITH run_periods AS (
  SELECT
    r.workspace_id,
    r.job_id,
    r.run_id,
    r.run_name,
    r.run_type,
    MIN(r.period_start_time) AS start_time,
    MAX(r.period_end_time) AS end_time,
    COALESCE(
      MAX_BY(r.result_state, r.period_end_time)
        FILTER (WHERE r.result_state IS NOT NULL),
      'RUNNING'
    ) AS status,
    MAX_BY(r.trigger_type, r.period_end_time) AS trigger_type,
    MAX_BY(r.termination_code, r.period_end_time)
      FILTER (WHERE r.termination_code IS NOT NULL) AS termination_code,
    MAX_BY(r.job_parameters, r.period_end_time) AS job_parameters,
    SUM(TIMESTAMPDIFF(SECOND, r.period_start_time, r.period_end_time))
      AS duration_seconds
  FROM system.lakeflow.job_run_timeline r
  WHERE r.workspace_id = '{{workspace_id}}'
    AND r.period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
    AND (
      r.run_type = 'SUBMIT_RUN'
      OR r.trigger_type IN ('ONETIME', 'ONETIME_RETRY')
    )
  GROUP BY
    r.workspace_id,
    r.job_id,
    r.run_id,
    r.run_name,
    r.run_type
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY start_time DESC) AS rn
  FROM run_periods
),

latest_n AS (
  SELECT *
  FROM ranked
  WHERE rn <= {{last_n_runs}}
),

latest_clusters AS (
  SELECT *
  FROM system.compute.clusters
  WHERE workspace_id = '{{workspace_id}}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, cluster_id
    ORDER BY change_time DESC
  ) = 1
),

run_clusters AS (
  SELECT
    e.workspace_id,
    CAST(e.job_run_id AS STRING) AS run_id,
    array_join(collect_set(e.cid), ', ') AS cluster_ids,
    array_join(collect_set(c.cluster_name), ', ') AS cluster_names,
    array_join(collect_set(c.cluster_source), ', ') AS cluster_sources,
    array_join(
      collect_set(
        CASE c.data_security_mode
          WHEN 'SINGLE_USER' THEN 'SINGLE_USER'
          WHEN 'LEGACY_SINGLE_USER' THEN 'SINGLE_USER'
          WHEN 'USER_ISOLATION' THEN 'STANDARD'
          ELSE COALESCE(c.data_security_mode, 'UNKNOWN')
        END
      ),
      ', '
    ) AS cluster_access_modes
  FROM (
    SELECT
      t.workspace_id,
      t.job_run_id,
      cid
    FROM system.lakeflow.job_task_run_timeline t
    INNER JOIN latest_n l
      ON t.workspace_id = l.workspace_id
     AND CAST(t.job_run_id AS STRING) = CAST(l.run_id AS STRING)
    LATERAL VIEW OUTER explode(COALESCE(t.compute_ids, array())) lv AS cid
    WHERE t.workspace_id = '{{workspace_id}}'
  ) e
  LEFT JOIN latest_clusters c
    ON e.workspace_id = c.workspace_id
   AND e.cid = c.cluster_id
  GROUP BY e.workspace_id, CAST(e.job_run_id AS STRING)
),

-- Cluster-wide DBUs billed while the run was active. Use when per-run
-- attribution is unavailable (all-purpose compute has no job_run_id in billing).
-- This is cluster-level, so concurrent workloads are included.
run_cluster_window_dbus AS (
  SELECT
    e.workspace_id,
    e.run_id,
    ROUND(SUM(u.usage_quantity), 4) AS cluster_dbu_during_run
  FROM (
    SELECT
      l.workspace_id,
      CAST(l.run_id AS STRING) AS run_id,
      l.start_time,
      l.end_time,
      cid
    FROM system.lakeflow.job_task_run_timeline t
    INNER JOIN latest_n l
      ON t.workspace_id = l.workspace_id
     AND CAST(t.job_run_id AS STRING) = CAST(l.run_id AS STRING)
    LATERAL VIEW explode(COALESCE(t.compute_ids, array())) lv AS cid
    WHERE t.workspace_id = '{{workspace_id}}'
  ) e
  INNER JOIN system.billing.usage u
    ON u.workspace_id = e.workspace_id
   AND u.usage_metadata.cluster_id = e.cid
  WHERE u.usage_unit = 'DBU'
    AND u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    AND u.usage_start_time < e.end_time
    AND u.usage_end_time > e.start_time
  GROUP BY e.workspace_id, e.run_id
),

submitters AS (
  SELECT
    a.workspace_id,
    COALESCE(
      a.request_params['runId'],
      a.request_params['run_id'],
      a.request_params['jobRunId'],
      a.request_params['idInJob'],
      try_element_at(from_json(a.response.result, 'map<string,string>'), 'run_id'),
      try_element_at(from_json(a.response.result, 'map<string,string>'), 'runId')
    ) AS run_id,
    COALESCE(a.user_identity.email, a.user_identity.subject_name) AS submitted_by
  FROM system.access.audit a
  WHERE a.workspace_id = '{{workspace_id}}'
    AND a.service_name = 'jobs'
    AND a.action_name IN ('runNow', 'submitRun')
    AND a.event_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      a.workspace_id,
      COALESCE(
        a.request_params['runId'],
        a.request_params['run_id'],
        a.request_params['jobRunId'],
        a.request_params['idInJob'],
        try_element_at(from_json(a.response.result, 'map<string,string>'), 'run_id'),
        try_element_at(from_json(a.response.result, 'map<string,string>'), 'runId')
      )
    ORDER BY a.event_time DESC
  ) = 1
),

latest_jobs AS (
  SELECT
    workspace_id,
    job_id,
    run_as_user_name,
    creator_user_name
  FROM system.lakeflow.jobs
  WHERE workspace_id = '{{workspace_id}}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY workspace_id, job_id
    ORDER BY change_time DESC
  ) = 1
),

run_dbus AS (
  SELECT
    u.workspace_id,
    CAST(u.usage_metadata.job_run_id AS STRING) AS run_id,
    FIRST(u.identity_metadata.run_as, TRUE) AS billed_run_as,
    ROUND(SUM(u.usage_quantity), 4) AS dbu_consumed,
    array_join(collect_set(u.sku_name), ', ') AS sku_names,
    CASE
      WHEN MAX(
        CASE
          WHEN COALESCE(u.product_features.is_serverless, false)
            OR upper(u.sku_name) LIKE '%SERVERLESS%'
            THEN 1
          ELSE 0
        END
      ) = 1 THEN 'SERVERLESS'
      WHEN MAX(
        CASE
          WHEN upper(u.sku_name) LIKE '%JOBS%' THEN 1
          ELSE 0
        END
      ) = 1 THEN 'JOBS_COMPUTE'
      ELSE 'OTHER'
    END AS compute_billing_type
  FROM system.billing.usage u
  INNER JOIN latest_n l
    ON u.workspace_id = l.workspace_id
   AND CAST(u.usage_metadata.job_run_id AS STRING) = CAST(l.run_id AS STRING)
  WHERE u.workspace_id = '{{workspace_id}}'
    AND u.usage_date >= CURRENT_DATE() - INTERVAL 30 DAYS
    AND u.usage_unit = 'DBU'
  GROUP BY
    u.workspace_id,
    CAST(u.usage_metadata.job_run_id AS STRING)
)

SELECT
  l.workspace_id,
  l.job_id,
  CAST(l.run_id AS STRING) AS run_id,
  concat(
    CAST(l.job_id AS STRING),
    '-',
    CAST(l.run_id AS STRING),
    ' | ',
    substr(COALESCE(l.run_name, ''), 1, 50)
  ) AS job_nm,
  COALESCE(
    s.submitted_by,
    d.billed_run_as,
    j.run_as_user_name,
    j.creator_user_name
  ) AS submitted_by,
  l.run_name,
  l.run_type,
  CASE
    WHEN l.run_type = 'SUBMIT_RUN' THEN 'SUBMIT_RUN_API'
    WHEN l.trigger_type IN ('ONETIME', 'ONETIME_RETRY') THEN 'RUN_NOW_API'
    ELSE COALESCE(l.trigger_type, l.run_type)
  END AS submit_channel,
  l.trigger_type,
  l.status,
  l.termination_code,
  l.start_time,
  l.end_time,
  l.duration_seconds,
  l.job_parameters,
  to_json(l.job_parameters) AS job_parameters_json,
  array_join(map_keys(COALESCE(l.job_parameters, map())), ', ') AS parameter_keys,
  COALESCE(d.dbu_consumed, 0) AS dbu_consumed,
  COALESCE(w.cluster_dbu_during_run, 0) AS cluster_dbu_during_run,
  d.sku_names,
  d.compute_billing_type,
  rc.cluster_ids,
  rc.cluster_names,
  rc.cluster_sources,
  rc.cluster_access_modes,
  CASE
    WHEN d.compute_billing_type = 'SERVERLESS' THEN 'SERVERLESS'
    WHEN rc.cluster_names IS NOT NULL AND rc.cluster_names <> '' THEN rc.cluster_names
    WHEN rc.cluster_ids IS NOT NULL AND rc.cluster_ids <> '' THEN rc.cluster_ids
    ELSE 'UNKNOWN'
  END AS cluster_running_on,
  l.rn AS recency_rank
FROM latest_n l
LEFT JOIN run_dbus d
  ON l.workspace_id = d.workspace_id
 AND CAST(l.run_id AS STRING) = d.run_id
LEFT JOIN run_clusters rc
  ON l.workspace_id = rc.workspace_id
 AND CAST(l.run_id AS STRING) = rc.run_id
LEFT JOIN run_cluster_window_dbus w
  ON l.workspace_id = w.workspace_id
 AND CAST(l.run_id AS STRING) = w.run_id
LEFT JOIN submitters s
  ON l.workspace_id = s.workspace_id
 AND CAST(l.run_id AS STRING) = CAST(s.run_id AS STRING)
LEFT JOIN latest_jobs j
  ON CAST(l.workspace_id AS STRING) = CAST(j.workspace_id AS STRING)
 AND CAST(l.job_id AS STRING) = CAST(j.job_id AS STRING);

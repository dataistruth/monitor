# Monitor E2E — pool + JCS1/JCS2/JCS3 + run-now via API

End-to-end test for **instance pool → complex job clusters → run-now → poll** on Azure.

Lives under `spark/monitor/e2e/` (separate mini-bundle from main `monitor` bundle).

| Asset | Path |
|-------|------|
| Pool spec (Standard_D4ds_v5, idle 2, max 10) | `e2e/config/instance_pool_azure.json` |
| Job cluster variables | `e2e/resources/compute/cluster_variables.yml` |
| Hello job | `e2e/resources/jobs/hello_print_job.yml` |
| REST client | `e2e/scripts/databricks_client.py` |
| Provision pool API | `e2e/scripts/provision_pool.py` |
| Run-now + poll API | `e2e/scripts/run_job.py` |
| API wrappers (from repo root) | `api/e2e_provision_pool.sh`, `api/e2e_run_hello_print.sh` |

---

## Quick start (from repo root)

```bash
cd /Users/mukesh.singh/spark/monitor

# 1) Create pool via REST API
./api/e2e_provision_pool.sh DEFAULT
export STANDARD_POOL_ID="<from output>"

# 2) Deploy e2e bundle
cd e2e
databricks bundle deploy --target dev --profile DEFAULT \
  --var standard_pool_id="$STANDARD_POOL_ID"

# 3) run-now + poll via REST API
cd ..
./api/e2e_run_hello_print.sh DEFAULT
```

---

## Step 1 — Provision pool (REST API)

```bash
python3 e2e/scripts/provision_pool.py --profile DEFAULT
# or
./api/e2e_provision_pool.sh DEFAULT
```

API calls:

- `GET /api/2.0/instance-pools/list`
- `POST /api/2.0/instance-pools/create` (body: `e2e/config/instance_pool_azure.json`)

---

## Step 2 — Deploy bundle

```bash
cd e2e
databricks bundle validate --target dev --profile DEFAULT \
  --var standard_pool_id="$STANDARD_POOL_ID"

databricks bundle deploy --target dev --profile DEFAULT \
  --var standard_pool_id="$STANDARD_POOL_ID"
```

---

## Step 3 — Run job (REST API)

```bash
python3 e2e/scripts/run_job.py --profile DEFAULT
# or
./api/e2e_run_hello_print.sh DEFAULT
```

API calls:

1. `GET /api/2.1/jobs/list?name=e2e_hello_print` (newest `created_time`)
2. `POST /api/2.1/jobs/run-now` `{ "job_id": ... }`
3. `GET /api/2.1/jobs/runs/get?run_id=...` (poll until `TERMINATED`)

---

## curl equivalents

```bash
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."

# Create pool
curl -s -X POST "$DATABRICKS_HOST/api/2.0/instance-pools/create" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @e2e/config/instance_pool_azure.json | jq .

# run-now
curl -s -X POST "$DATABRICKS_HOST/api/2.1/jobs/run-now" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"job_id\": $JOB_ID}" | jq .
```

---

## Job YAML pattern (cluster spec outside job)

```yaml
job_clusters:
  - job_cluster_key: JCS2
    new_cluster: ${var.cluster_jcs2}
tasks:
  - job_cluster_key: JCS2
```

`run-now` uses this deployed definition — no `cluster_id` in the API body.

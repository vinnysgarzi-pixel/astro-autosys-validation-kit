"""Metrics & Reporting validation — legacy-scheduler-style operational reporting.

Validates:
  - Total jobs executed per week/month/year
  - Job success/failure rate trends over time
  - New jobs added over a given period / job growth trend
  - Run history export (CSV/API for external reporting)

The remaining rows (consolidated workload view, audit log export, change
history, SLA compliance) are validated through the Astro UI / audit log export /
Astro Observe — see docs/validation-matrix.md.
"""

import csv
import io
import json
from datetime import datetime, timedelta, timezone

from airflow.providers.http.hooks.http import HttpHook
from airflow.sdk import Param, Variable, dag, task

from include.validation_kit.config import (
    CONN_AIRFLOW_API,
    CONN_AWS,
    DEFAULT_BUCKET,
    KIT_TAG,
    VAR_BUCKET,
)

DOC = """
### What this validates
Everything legacy-scheduler-style reporting needs is available from the Airflow REST API.
This DAG queries the API for the last `lookback_days` and produces:

1. **Execution counts** — total DAG runs by state (success/failed/running).
2. **Success/failure rate** — percentage per DAG and overall.
3. **Job inventory & growth** — count of active DAGs (new-DAG trend comes from
   comparing successive report snapshots).
4. **Run history CSV** — dag_id, run_id, logical_date, start, end, duration,
   state — printed to the log and uploaded to
   `s3://<validation_bucket>/reports/` when `upload_to_s3=true`.

### Setup
Create HTTP connection `validation_airflow_api` in Vault:
  host = your Deployment API URL (e.g. https://<org>.astronomer.run/<deployment>),
  extra = `{"Authorization": "Bearer <deployment-api-token>"}` header via the
  connection's Headers/extra field.

### How to validate
Run several kit DAGs (some failing), trigger this report, and reconcile the
counts against the Astro UI. Schedule it weekly for the trend evidence.
"""


def _api(endpoint: str, params: dict | None = None) -> dict:
    hook = HttpHook(method="GET", http_conn_id=CONN_AIRFLOW_API)
    response = hook.run(endpoint=endpoint, data=params)
    return json.loads(response.text)


@dag(
    schedule=None,  # switch to "@weekly" once the API connection is configured
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "reporting", "metrics"],
    doc_md=DOC,
    params={
        "lookback_days": Param(7, type="integer"),
        "upload_to_s3": Param(False, type="boolean"),
    },
)
def metrics_report():

    @task
    def dag_inventory() -> dict:
        data = _api("/api/v2/dags", {"limit": 1000})
        dags = data.get("dags", [])
        active = [d["dag_id"] for d in dags if not d.get("is_paused")]
        print(f"JOB INVENTORY: total={len(dags)} active(unpaused)={len(active)}")
        return {"total_dags": len(dags), "active_dags": len(active)}

    @task
    def run_history(params: dict) -> list[dict]:
        since = (datetime.now(timezone.utc) - timedelta(days=params["lookback_days"])).isoformat()
        rows, offset = [], 0
        while True:
            data = _api(
                "/api/v2/dags/~/dagRuns",
                {"start_date_gte": since, "limit": 100, "offset": offset},
            )
            batch = data.get("dag_runs", [])
            for r in batch:
                start, end = r.get("start_date"), r.get("end_date")
                duration = None
                if start and end:
                    duration = (
                        datetime.fromisoformat(end) - datetime.fromisoformat(start)
                    ).total_seconds()
                rows.append(
                    {
                        "dag_id": r["dag_id"],
                        "run_id": r["dag_run_id"],
                        "logical_date": r.get("logical_date"),
                        "start_date": start,
                        "end_date": end,
                        "duration_s": duration,
                        "state": r.get("state"),
                        "run_type": r.get("run_type"),
                    }
                )
            offset += len(batch)
            if len(batch) < 100:
                break
        print(f"RUN HISTORY: {len(rows)} runs in the last {params['lookback_days']} days")
        return rows

    @task
    def compute_rates(rows: list[dict], inventory: dict) -> str:
        total = len(rows)
        by_state: dict[str, int] = {}
        for r in rows:
            by_state[r["state"]] = by_state.get(r["state"], 0) + 1
        success = by_state.get("success", 0)
        failed = by_state.get("failed", 0)
        finished = success + failed
        rate = (success / finished * 100) if finished else 0.0
        print("=== EXECUTION SUMMARY (legacy scheduler report equivalent) ===")
        print(f"total runs: {total} | by state: {by_state}")
        print(f"success rate (finished runs): {rate:.1f}%")
        print(f"dag inventory: {inventory}")

        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=[
                "dag_id", "run_id", "logical_date", "start_date",
                "end_date", "duration_s", "state", "run_type",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
        csv_text = buf.getvalue()
        print("=== RUN HISTORY CSV (first 20 lines) ===")
        print("\n".join(csv_text.splitlines()[:20]))
        return csv_text

    @task
    def export_csv(csv_text: str, params: dict, ds_nodash=None):
        if not params["upload_to_s3"]:
            print("upload_to_s3=false — CSV available in the previous task's log/XCom")
            return
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket = Variable.get(VAR_BUCKET, default=DEFAULT_BUCKET)
        key = f"reports/run_history_{ds_nodash}.csv"
        S3Hook(aws_conn_id=CONN_AWS).load_string(
            string_data=csv_text, key=key, bucket_name=bucket, replace=True
        )
        print(f"Report exported to s3://{bucket}/{key}")

    export_csv(compute_rates(run_history(), dag_inventory()))


metrics_report()

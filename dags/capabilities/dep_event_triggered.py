"""Dependency Modeling — event-driven / API-based triggering.

Validates:
  - Event-driven triggers (API, queue, webhook, database state)
  - External API-based job triggers (call internal services from DAGs)
  - Ad-hoc / manual trigger with runtime parameters (dag_run.conf)
"""

from datetime import datetime

from airflow.providers.http.operators.http import HttpOperator
from airflow.sdk import Param, dag, task

from include.validation_kit.config import CONN_INTERNAL_API, KIT_TAG

DOC = """
### What this validates
1. **Inbound**: this DAG is the target for API-based triggering (AutoSys
   `sendevent -E STARTJOB` equivalent). Trigger it from outside Airflow:

   ```bash
   curl -X POST "$AIRFLOW_API/dags/dep_event_triggered/dagRuns" \\
     -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \\
     -d '{"logical_date": null, "conf": {"source_system": "fraud-svc", "batch_id": "B-1234"}}'
   ```

   The `echo_trigger_payload` task proves the conf payload reached the run.
2. **Outbound**: `call_internal_api` shows a DAG calling an internal REST
   service via the `validation_internal_api` HTTP connection (managed in Astro),
   with retry-on-failure semantics.

### How to validate
1. Create a Deployment API token, POST the trigger above, and confirm the run
   appears with the conf visible in the run details + task log.
2. Point `validation_internal_api` at any internal endpoint (or httpbin for a
   smoke test) and confirm request/response logging and auth header handling.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "event-driven", "api-trigger"],
    doc_md=DOC,
    params={
        "source_system": Param("manual", type="string"),
        "batch_id": Param("", type="string"),
        "call_external_api": Param(False, type="boolean", description="Also exercise the outbound HTTP call"),
    },
)
def dep_event_triggered():

    @task
    def echo_trigger_payload(params: dict, run_id=None, dag_run=None):
        print(f"run_id={run_id} | run_type={dag_run.run_type if dag_run else 'n/a'}")
        print(f"conf received: source_system={params['source_system']} batch_id={params['batch_id']}")
        return params["batch_id"]

    @task.branch
    def maybe_call_api(params: dict) -> list[str]:
        return ["call_internal_api"] if params["call_external_api"] else []

    call_internal_api = HttpOperator(
        task_id="call_internal_api",
        http_conn_id=CONN_INTERNAL_API,
        endpoint="/status",
        method="GET",
        log_response=True,
        retries=2,
    )

    echo_trigger_payload() >> maybe_call_api() >> call_internal_api


dep_event_triggered()

"""Cross-Platform Integration — AutoSys <-> Airflow coexistence bridge.

Validates:
  - AutoSys <-> Airflow coexistence (trigger Airflow from AutoSys and vice-versa)
  - Cross-platform dependencies during migration
  - cross system upstream/downstream triggers
"""

from datetime import datetime

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Param, dag, task

from include.validation_kit.config import CONN_AUTOSYS_API, KIT_TAG

DOC = """
### What this validates — both migration directions

**Airflow → AutoSys** (this DAG):
- `start_autosys_job` POSTs a start-job event to the AutoSys REST API
  (`validation_autosys_api` connection — AutoSys Web Services / AAI endpoint).
  TODO: set the real endpoint path + payload for your AutoSys version.
- `wait_for_autosys_success` polls the job-status endpoint until SUCCESS,
  giving Airflow a true downstream dependency on an AutoSys job.

**AutoSys → Airflow** (documented, tested from the AutoSys side):
Create an AutoSys command job that calls the Airflow REST API:

```
insert_job: TRIGGER_ASTRO_DAG
job_type: CMD
command: curl -sf -X POST "$ASTRO_API/dags/dep_event_triggered/dagRuns" \\
  -H "Authorization: Bearer $ASTRO_TOKEN" -H "Content-Type: application/json" \\
  -d '{"conf": {"source_system": "autosys", "batch_id": "$AUTO_JOB_NAME"}}'
```

The curl exit code maps AutoSys job status to the trigger result.

### How to validate
1. Fill in the AutoSys API connection + endpoint paths, run this DAG, confirm
   the AutoSys job starts and the sensor completes when it succeeds.
2. Load the JIL above in AutoSys, force-start it, and confirm
   `dep_event_triggered` runs with the AutoSys-supplied conf.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "autosys-bridge", "coexistence"],
    doc_md=DOC,
    params={"autosys_job_name": Param("SAMPLE_TEST_JOB", type="string")},
)
def bridge_airflow_to_autosys():

    # TODO: adjust endpoint + payload to your AutoSys REST API version
    # (e.g. AutoSys Web Services: POST /AEWS/event with a FORCE_STARTJOB body).
    start_autosys_job = HttpOperator(
        task_id="start_autosys_job",
        http_conn_id=CONN_AUTOSYS_API,
        endpoint="/AEWS/event",
        method="POST",
        headers={"Content-Type": "application/json"},
        data='{"eventTypeStr": "FORCE_STARTJOB", "jobName": "{{ params.autosys_job_name }}"}',
        log_response=True,
        retries=2,
    )

    # TODO: confirm the status endpoint/response shape for your version.
    wait_for_autosys_success = HttpSensor(
        task_id="wait_for_autosys_success",
        http_conn_id=CONN_AUTOSYS_API,
        endpoint="/AEWS/jobruninfo/{{ params.autosys_job_name }}",
        response_check=lambda response: '"SUCCESS"' in response.text,
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",
    )

    @task
    def downstream_after_autosys(params: dict):
        print(f"AutoSys job {params['autosys_job_name']} succeeded — Airflow downstream proceeding")

    start_autosys_job >> wait_for_autosys_success >> downstream_after_autosys()


bridge_airflow_to_autosys()

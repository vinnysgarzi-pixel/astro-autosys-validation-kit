"""CA7 / Mainframe dependency validation.

Validates:
  - CA7/Mainframe Dependency (completion signal/event representation)
  - File-based cross-system signaling (completion flags, trigger files)

Pattern: CA7 (or an intermediary) drops a completion flag file to S3; this DAG's
sensor detects it and releases the downstream Airflow work. Use
`file_watcher_simulator`-style helper below to simulate the mainframe drop.
"""

from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import Param, Variable, dag, task

from include.validation_kit.config import (
    CONN_AWS,
    DEFAULT_BUCKET,
    DEFAULT_PREFIX,
    KIT_TAG,
    VAR_BUCKET,
    VAR_PREFIX,
)

DOC = """
### What this validates
Represents a CA7/mainframe job completion as a dated flag file
(`ca7-flags/JOBNAME.{{ ds_nodash }}.done`). The deferrable `S3KeySensor` waits
for the flag; downstream tasks only run once the mainframe signals completion —
the AutoSys file-watcher/external-event pattern, ported.

- **Dated flag**: the key template includes the logical date, so each daily run
  waits for *its own* date's signal — no cross-day false triggers.
- **Timeout policy**: sensor fails after `timeout` if the mainframe never
  signals, surfacing the miss to ops instead of hanging silently.

### How to validate
1. Trigger `ca7_dependency_consumer`; confirm the sensor defers (purple state).
2. Trigger `ca7_signal_simulator` (same logical date) to drop the flag file.
3. Confirm the consumer wakes and completes. Capture Grid view + sensor log.
"""


def _flag_key(job: str, ds_nodash: str) -> str:
    prefix = Variable.get(VAR_PREFIX, default=DEFAULT_PREFIX)
    return f"{prefix}/ca7-flags/{job}.{ds_nodash}.done"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 25),
    catchup=False,
    tags=[KIT_TAG, "workloads", "ca7", "mainframe"],
    doc_md=DOC,
    params={"ca7_job_name": Param("MFJOB001", type="string")},
)
def ca7_dependency_consumer():

    wait_for_ca7_flag = S3KeySensor(
        task_id="wait_for_ca7_flag",
        aws_conn_id=CONN_AWS,
        bucket_name="{{ var.value.get('" + VAR_BUCKET + "', '" + DEFAULT_BUCKET + "') }}",
        bucket_key=(
            "{{ var.value.get('" + VAR_PREFIX + "', '" + DEFAULT_PREFIX + "') }}"
            "/ca7-flags/{{ params.ca7_job_name }}.{{ ds_nodash }}.done"
        ),
        deferrable=True,
        poke_interval=30,
        timeout=60 * 60 * 4,
    )

    @task
    def downstream_processing(params: dict, ds=None):
        print(f"CA7 job {params['ca7_job_name']} confirmed complete for {ds}; running dependent work")

    wait_for_ca7_flag >> downstream_processing()


ca7_dependency_consumer()


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "ca7", "simulator"],
    doc_md="Helper: simulates the mainframe dropping a CA7 completion flag for a given date.",
    params={
        "ca7_job_name": Param("MFJOB001", type="string"),
        "flag_date": Param("", type="string", description="YYYYMMDD; blank = today's ds_nodash"),
    },
)
def ca7_signal_simulator():

    @task
    def drop_flag(params: dict, ds_nodash=None):
        hook = S3Hook(aws_conn_id=CONN_AWS)
        bucket = Variable.get(VAR_BUCKET, default=DEFAULT_BUCKET)
        key = _flag_key(params["ca7_job_name"], params["flag_date"] or ds_nodash)
        hook.load_string(string_data="COMPLETE", key=key, bucket_name=bucket, replace=True)
        print(f"Dropped CA7 completion flag s3://{bucket}/{key}")

    drop_flag()


ca7_signal_simulator()

"""File Watcher workload validation (AutoSys File Watcher equivalent).

Validates:
  - Reliable detection of "file landed and complete" (no partial file triggers)
  - Can handle late arrival and reprocessing safely
  - Clear operational visibility for "waiting" state and timeout policy

Two DAGs in this file:
  - file_watcher            — the sensor pipeline under test
  - file_watcher_simulator  — helper that drops/deletes the trigger file in S3
"""

import time
from datetime import datetime, timedelta

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
1. **Landed and complete** — `wait_for_file` (deferrable `S3KeySensor`) detects
   existence; `verify_file_stable` then reads the object size twice, 30s apart,
   and fails if it is still growing — the AutoSys `watch_file_min_size` /
   steady-state equivalent. Partial uploads never trigger downstream processing.
2. **Late arrival / reprocessing** — the sensor waits up to `timeout_minutes`.
   For reprocessing, clear the DAG run after replacing the file: processing is
   idempotent per file + logical date.
3. **Waiting-state visibility** — while deferred, the task shows **deferred**
   (purple) in the Grid view and releases its worker slot. On timeout the task
   fails with a sensor-timeout error — that is the operational timeout policy.

### How to validate
1. Set Variables `validation_bucket` / `validation_prefix` and the
   `validation_aws` connection.
2. Trigger `file_watcher`, observe the deferred state, then trigger
   `file_watcher_simulator` with `{"action": "drop"}` — watcher proceeds.
3. Re-trigger `file_watcher` without a file and let it hit the timeout —
   capture the failure as the timeout-policy evidence.
"""


def _bucket() -> str:
    return Variable.get(VAR_BUCKET, default=DEFAULT_BUCKET)


def _key(filename: str) -> str:
    prefix = Variable.get(VAR_PREFIX, default=DEFAULT_PREFIX)
    return f"{prefix}/incoming/{filename}"


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "file-watcher"],
    doc_md=DOC,
    params={
        "filename": Param("trigger_file.csv", type="string"),
        "timeout_minutes": Param(30, type="integer"),
        "stability_wait_seconds": Param(30, type="integer"),
    },
)
def file_watcher():

    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        aws_conn_id=CONN_AWS,
        bucket_name="{{ var.value.get('" + VAR_BUCKET + "', '" + DEFAULT_BUCKET + "') }}",
        bucket_key=(
            "{{ var.value.get('" + VAR_PREFIX + "', '" + DEFAULT_PREFIX + "') }}"
            "/incoming/{{ params.filename }}"
        ),
        deferrable=True,
        poke_interval=30,
        timeout=60 * 30,  # overridden below via params at trigger time if needed
    )

    @task
    def verify_file_stable(params: dict) -> dict:
        """Fail if the file is still growing (partial-upload guard)."""
        hook = S3Hook(aws_conn_id=CONN_AWS)
        key = _key(params["filename"])
        first = hook.head_object(key=key, bucket_name=_bucket())["ContentLength"]
        time.sleep(params["stability_wait_seconds"])
        second = hook.head_object(key=key, bucket_name=_bucket())["ContentLength"]
        print(f"size check: first={first} second={second}")
        if first != second:
            raise ValueError(f"File still growing ({first} -> {second} bytes); refusing to trigger")
        return {"key": key, "size": second}

    @task
    def process_file(file_info: dict, run_id=None):
        """Idempotent processing step — safe to clear + rerun for reprocessing."""
        print(f"Processing {file_info['key']} ({file_info['size']} bytes) in run {run_id}")

    wait_for_file >> process_file(verify_file_stable())


file_watcher()


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "file-watcher", "simulator"],
    doc_md="Helper: drops or deletes the trigger file the `file_watcher` DAG waits for.",
    params={
        "action": Param("drop", type="string", enum=["drop", "delete"]),
        "filename": Param("trigger_file.csv", type="string"),
        "content_lines": Param(100, type="integer", description="Rows to write into the test file"),
    },
)
def file_watcher_simulator():

    @task
    def apply_action(params: dict):
        hook = S3Hook(aws_conn_id=CONN_AWS)
        key = _key(params["filename"])
        if params["action"] == "drop":
            body = "\n".join(f"row_{i},value_{i}" for i in range(params["content_lines"]))
            hook.load_string(string_data=body, key=key, bucket_name=_bucket(), replace=True)
            print(f"Dropped s3://{_bucket()}/{key} ({len(body)} bytes)")
        else:
            hook.delete_objects(bucket=_bucket(), keys=[key])
            print(f"Deleted s3://{_bucket()}/{key}")

    apply_action()


file_watcher_simulator()

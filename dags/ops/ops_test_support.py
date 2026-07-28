"""Ops & Reliability test-support DAGs.

These DAGs generate the workload needed while running the infrastructure tests
in ops/runbooks/ (agent HA, failure detection, node drain, autoscaling,
concurrency). They validate nothing by themselves — they are the load you
observe while performing kubectl actions on the RE Agent.

Validates:
  - Agent HA / failure detection / maintenance windows -> ops_long_running_task
  - Concurrency & worker queue limits                  -> ops_pool_throttle
  - Agent autoscaling under load                       -> ops_load_generator
"""

import time
from datetime import datetime

from airflow.sdk import Param, dag, task

from include.validation_kit.config import KIT_TAG

LONG_DOC = """
### Use with runbooks: agent-ha, failure-detection, maintenance-window
Single task that runs for `duration_minutes`, logging a heartbeat every 15s.
Start it, then kill the worker pod / drain the node per the runbook and observe
detection, retry, and rescheduling behavior. `retries=3` so the task recovers
onto a healthy replica.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "ops", "ha-test"],
    doc_md=LONG_DOC,
    params={"duration_minutes": Param(10, type="integer")},
)
def ops_long_running_task():

    @task(retries=3)
    def run_long(params: dict, ti=None):
        total = params["duration_minutes"] * 60
        print(f"try={ti.try_number} hostname check — see which worker pod runs this")
        for elapsed in range(0, total, 15):
            print(f"heartbeat {elapsed}/{total}s")
            time.sleep(min(15, total - elapsed))
        print("completed without interruption")

    run_long()


ops_long_running_task()


POOL_DOC = """
### Use with runbook: concurrency
10 mapped tasks assigned to pool `validation_throttle`. Create the pool with
**5 slots** (Admin > Pools) before triggering. Expected: exactly 5 running,
5 queued, FIFO pickup as slots free — the AutoSys `max_load` equivalent.
Also use this DAG to verify `max_active_runs_per_dag` and global `parallelism`
per the runbook.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=2,
    tags=[KIT_TAG, "ops", "concurrency"],
    doc_md=POOL_DOC,
    params={"task_count": Param(10, type="integer"), "task_seconds": Param(60, type="integer")},
)
def ops_pool_throttle():

    @task
    def make_batch(params: dict) -> list[int]:
        return list(range(params["task_count"]))

    @task(pool="validation_throttle")
    def throttled_task(i: int, params: dict):
        print(f"task {i} acquired a pool slot")
        time.sleep(params["task_seconds"])

    throttled_task.expand(i=make_batch())


ops_pool_throttle()


LOAD_DOC = """
### Use with runbook: autoscaling
Generates `task_count` parallel CPU-busy tasks (default 50) to push worker CPU
past the HPA threshold. Watch `kubectl get hpa -w` and pod count while this
runs; after completion, verify scale-down after the stabilization window.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "ops", "load-generator", "autoscaling"],
    doc_md=LOAD_DOC,
    params={
        "task_count": Param(50, type="integer"),
        "busy_seconds": Param(120, type="integer"),
    },
)
def ops_load_generator():

    @task
    def make_load(params: dict) -> list[int]:
        return list(range(params["task_count"]))

    @task
    def busy_task(i: int, params: dict):
        """CPU-busy loop (not sleep) so HPA CPU metrics actually rise."""
        end = time.monotonic() + params["busy_seconds"]
        x = 0
        while time.monotonic() < end:
            x = (x * 31 + 7) % 1_000_003
        print(f"load task {i} finished (checksum {x})")

    busy_task.expand(i=make_load())


ops_load_generator()

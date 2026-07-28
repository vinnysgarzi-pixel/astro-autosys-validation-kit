"""Dependency Modeling — ExternalTaskSensor and time-based waits.

Validates:
  - Cross-DAG dependencies (ExternalTaskSensor approach)
  - Time-based wait conditions (wait until specific time before proceeding)
"""

from datetime import datetime, time, timedelta

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.time import TimeSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import dag, task

from include.validation_kit.config import KIT_TAG

EXT_DOC = """
### What this validates
`ExternalTaskSensor` — the polling alternative to Asset triggers for cross-DAG
dependencies (use Assets when you control both DAGs; use the sensor when you
need to wait on a specific task/date in a DAG you don't own).

This DAG waits for `dep_sequential`'s `task_c` **for the same logical date**.

### How to validate
1. Trigger `dep_sequential` and note its logical date.
2. Trigger this DAG with the SAME logical date (UI: "Trigger w/ config" >
   choose the date). Sensor goes deferred, then succeeds once task_c is done.
3. Trigger for a date where `dep_sequential` never ran — sensor times out
   after 30 min (failure = visible missed-dependency policy).
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "external-task-sensor"],
    doc_md=EXT_DOC,
)
def dep_external_task_sensor():

    wait_for_other_dag = ExternalTaskSensor(
        task_id="wait_for_dep_sequential",
        external_dag_id="dep_sequential",
        external_task_id="task_c",
        poke_interval=30,
        timeout=60 * 30,
        deferrable=True,
    )

    @task
    def downstream_work():
        print("dep_sequential.task_c confirmed complete for this logical date")

    wait_for_other_dag >> downstream_work()


dep_external_task_sensor()


TIME_DOC = """
### What this validates
Time-based waits (AutoSys `start_times` "wait until HH:MM even if dependencies
are met"):

- `wait_until_time` — TimeSensor holding until 06:30 UTC (deferrable: worker
  slot is released while waiting — check the Deferred state).
- `wait_fixed_delay` — TimeDeltaSensor holding 2 minutes after the run starts
  (settle-time pattern).

### How to validate
Trigger after 06:30 UTC: TimeSensor passes immediately. Trigger before:
it defers until 06:30. The 2-minute delta branch always waits exactly 2 min —
compare task start timestamps in the Gantt view.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "time-wait"],
    doc_md=TIME_DOC,
)
def dep_time_waits():

    wait_until_time = TimeSensor(
        task_id="wait_until_0630_utc",
        target_time=time(6, 30),
        deferrable=True,
    )

    wait_fixed_delay = TimeDeltaSensor(
        task_id="wait_2_minutes",
        delta=timedelta(minutes=2),
        deferrable=True,
    )

    @task
    def proceed_after_time():
        print("06:30 UTC gate passed")

    @task
    def proceed_after_delay():
        print("2-minute settle gate passed")

    wait_until_time >> proceed_after_time()
    wait_fixed_delay >> proceed_after_delay()


dep_time_waits()

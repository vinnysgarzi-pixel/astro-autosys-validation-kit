"""Scheduling & Calendar Management — holiday calendars and blackout windows.

Validates:
  - Business-day / holiday calendar support (equivalent to AutoSys calendars)
  - Blackout windows (prevent jobs from running during specific windows)
  - Skip on holiday / conditional schedule behavior

Two mechanisms are demonstrated:
  1. Custom Timetables (schedule-level skip — no run is even created), the
     closest equivalent to AutoSys run_calendar/exclude_calendar.
  2. A ShortCircuit holiday/blackout check (task-level skip — run is created
     but work is skipped), useful when downstream visibility of the skip is
     desired.

REMOTE EXECUTION REQUIREMENT — read before enabling the timetable DAGs
-----------------------------------------------------------------------
In Remote Execution mode there are TWO images:

  - the *server* image (Astro orchestration plane: scheduler + api-server),
    updated with `astro deploy`
  - the *client* image (Remote Execution Agents: workers, dag processor,
    triggerer), pushed to your registry with `astro remote deploy` and rolled
    out via `helm upgrade`

A custom Timetable is deserialized BY THE SCHEDULER, so the plugin
(plugins/calendar_timetables_plugin.py), the timetable code, and
include/data/holidays.csv must be present in the *server* image. If the
timetable DAGs reach the agents without a matching `astro deploy`, the
scheduler cannot deserialize them and its scheduler loop raises on every
cycle ("Failed to deserialize DAG ..." / "Dag not found in serialized_dag
table"), flipping the Deployment UNHEALTHY.

The timetable DAGs are therefore gated behind an environment variable and are
OFF by default. Enable them only after `astro deploy` has been run from this
project, by adding to the Agent Helm chart values.yaml (and, if desired, the
Deployment env in the Astro UI):

    commonEnv:
      - name: VALIDATION_ENABLE_CUSTOM_TIMETABLES
        value: "true"

Until then, holiday AND blackout semantics remain fully validated at task
level by `sched_holiday_shortcircuit` below, so the calendar test cases are
still addressed with the gate closed.
"""

import os
from datetime import date, datetime

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import dag, task

from include.validation_kit.config import KIT_TAG
from include.timetables.calendar_timetables import (
    BlackoutCronTimetable,
    HolidaySkipCronTimetable,
    load_holidays,
)

# Schedule-level (custom Timetable) DAGs are opt-in: they require the
# timetable plugin to exist in the SERVER image (see module docstring).
ENABLE_TIMETABLE_DAGS = (
    os.environ.get("VALIDATION_ENABLE_CUSTOM_TIMETABLES", "false").strip().lower()
    in ("1", "true", "yes")
)

TIMETABLE_DOC = """
### What this validates
Schedule-level calendar handling via a custom Timetable — the direct equivalent
of attaching an AutoSys calendar to a job. On a holiday (see
`include/data/holidays.csv`) **no DAG run is created at all**.

### Remote Execution prerequisite
The timetable classes must be deserializable by the Astro-managed scheduler:
run `astro deploy` from this project (server image), then set
`VALIDATION_ENABLE_CUSTOM_TIMETABLES=true` on the Agents (values.yaml
commonEnv) so these DAGs register.

### How to validate
1. Unpause and check **Next Run** in the UI: with a holiday tomorrow, next run
   skips to the following business day.
2. Add tomorrow's date to `include/data/holidays.csv`, redeploy (BOTH
   `astro deploy` and `astro remote deploy` + helm upgrade, so server and
   client images stay in sync), and confirm the next-run date moves.
3. `sched_blackout_timetable` additionally skips the year-end freeze window
   (Dec 24-31, 2026) defined in `BlackoutCronTimetable.BLACKOUT_RANGES`.
"""

if ENABLE_TIMETABLE_DAGS:

    @dag(
        schedule=HolidaySkipCronTimetable("0 7 * * 1-5", timezone="America/New_York"),
        start_date=datetime(2026, 7, 1),
        catchup=False,
        tags=[KIT_TAG, "capabilities", "scheduling", "calendar"],
        doc_md=TIMETABLE_DOC,
    )
    def sched_holiday_timetable():

        @task
        def business_day_work(ds=None):
            print(f"Running on business day {ds} (holiday calendar respected)")

        business_day_work()

    sched_holiday_timetable()

    @dag(
        schedule=BlackoutCronTimetable("0 7 * * *", timezone="America/New_York"),
        start_date=datetime(2026, 7, 1),
        catchup=False,
        tags=[KIT_TAG, "capabilities", "scheduling", "blackout"],
        doc_md=TIMETABLE_DOC,
    )
    def sched_blackout_timetable():

        @task
        def non_blackout_work(ds=None):
            print(f"Running on {ds} — outside holiday and blackout windows")

        non_blackout_work()

    sched_blackout_timetable()


BRANCH_DOC = """
### What this validates
Task-level holiday **and blackout-window** skip: the run **is** created every
day, but a `ShortCircuitOperator` checks the holiday calendar
(`include/data/holidays.csv`) and the blackout ranges
(`BlackoutCronTimetable.BLACKOUT_RANGES`) and skips all downstream tasks when
either applies. Downstream tasks show 'skipped' — an auditable record that the
date was intentionally not processed (vs the timetable approach where no run
exists).

This DAG uses only built-in scheduling, so it is safe in Remote Execution
without a server-image deploy, and it keeps both calendar test cases covered
while the custom-timetable DAGs are gated off.

### How to validate
1. Trigger manually on any day — downstream runs (today is not a holiday or
   blackout date).
2. Temporarily add today to `include/data/holidays.csv`, redeploy, trigger —
   downstream tasks show **skipped**.
3. Trigger with a logical date inside Dec 24-31, 2026 — downstream tasks show
   **skipped** (blackout window).
"""


@dag(
    schedule="0 7 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "scheduling", "calendar", "blackout"],
    doc_md=BRANCH_DOC,
)
def sched_holiday_shortcircuit():

    def _is_runnable_date(logical_date=None) -> bool:
        run_date = logical_date.date() if logical_date else date.today()
        if run_date in load_holidays():
            print(f"{run_date} is a holiday — skipping downstream tasks")
            return False
        if any(
            start <= run_date <= end
            for start, end in BlackoutCronTimetable.BLACKOUT_RANGES
        ):
            print(f"{run_date} is in a blackout window — skipping downstream tasks")
            return False
        print(f"{run_date} is a business day — proceeding")
        return True

    check_calendar = ShortCircuitOperator(
        task_id="check_calendar",
        python_callable=_is_runnable_date,
    )

    @task
    def daily_work(ds=None):
        print(f"Business-day work executed for {ds}")

    check_calendar >> daily_work()


sched_holiday_shortcircuit()

"""Scheduling & Calendar Management — holiday calendars and blackout windows.

Validates:
  - Business-day / holiday calendar support (equivalent to AutoSys calendars)
  - Blackout windows (prevent jobs from running during specific windows)
  - Skip on holiday / conditional schedule behavior

Two mechanisms are demonstrated:
  1. Custom Timetables (schedule-level skip — no run is even created), the
     closest equivalent to AutoSys run_calendar/exclude_calendar.
  2. A ShortCircuit holiday check (task-level skip — run is created but work is
     skipped), useful when downstream visibility of the skip is desired.
"""

from datetime import date, datetime

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import dag, task

from include.validation_kit.config import KIT_TAG
from include.timetables.calendar_timetables import (
    BlackoutCronTimetable,
    HolidaySkipCronTimetable,
    load_holidays,
)

TIMETABLE_DOC = """
### What this validates
Schedule-level calendar handling via a custom Timetable — the direct equivalent
of attaching an AutoSys calendar to a job. On a holiday (see
`include/data/holidays.csv`) **no DAG run is created at all**.

### How to validate
1. Unpause and check **Next Run** in the UI: with a holiday tomorrow, next run
   skips to the following business day.
2. Add tomorrow's date to `include/data/holidays.csv`, redeploy, and confirm
   the next-run date moves.
3. `sched_blackout_timetable` additionally skips the year-end freeze window
   (Dec 24-31, 2026) defined in `BlackoutCronTimetable.BLACKOUT_RANGES`.
"""


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
Task-level holiday skip: the run **is** created every day, but a
`ShortCircuitOperator` checks the holiday calendar and skips all downstream
tasks on holidays. Downstream tasks show 'skipped' — an auditable record that
the date was intentionally not processed (vs the timetable approach where no
run exists).

### How to validate
1. Trigger manually on any day — downstream runs (today is not a holiday).
2. Temporarily add today to `include/data/holidays.csv`, redeploy, trigger —
   downstream tasks show **skipped**.
"""


@dag(
    schedule="0 7 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "scheduling", "calendar"],
    doc_md=BRANCH_DOC,
)
def sched_holiday_shortcircuit():

    def _is_business_day(logical_date=None) -> bool:
        run_date = logical_date.date() if logical_date else date.today()
        if run_date in load_holidays():
            print(f"{run_date} is a holiday — skipping downstream tasks")
            return False
        print(f"{run_date} is a business day — proceeding")
        return True

    check_calendar = ShortCircuitOperator(
        task_id="check_calendar",
        python_callable=_is_business_day,
    )

    @task
    def daily_work(ds=None):
        print(f"Business-day work executed for {ds}")

    check_calendar >> daily_work()


sched_holiday_shortcircuit()

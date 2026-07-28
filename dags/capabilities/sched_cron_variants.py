"""Scheduling & Calendar Management — cron variants.

Validates:
  - Cron-based scheduling (daily, weekly, monthly, intraday)
  - Multiple schedules per workflow (e.g., run at 6AM and 6PM)

One tiny DAG is generated per schedule so run history can be compared against
the expected cron in the Astro UI. All are paused by default — unpause the ones
under test.
"""

from datetime import datetime

from airflow.sdk import dag, task

from include.validation_kit.config import KIT_TAG

SCHEDULES = {
    "daily_6am": "0 6 * * *",
    "weekly_mon_wed_fri": "0 6 * * 1,3,5",
    "monthly_first_day": "0 6 1 * *",
    "intraday_every_15min": "*/15 * * * *",
    # AutoSys start_times: "06:00,18:00" equivalent — one DAG, two triggers/day.
    "multi_time_6am_6pm": "0 6,18 * * *",
}

DOC_TEMPLATE = """
### What this validates
Cron `{cron}` — verify in **Browse > DAG Runs** that trigger times match the
expression exactly (AutoSys JIL `start_times`/`days_of_week` equivalent).

### How to validate
Unpause, let 2+ intervals elapse, compare run-history timestamps against the
cron. For the multi-time variant confirm two runs per day (06:00 and 18:00).
"""


def build_dag(name: str, cron: str):
    @dag(
        dag_id=f"sched_{name}",
        schedule=cron,
        start_date=datetime(2026, 7, 1),
        catchup=False,
        tags=[KIT_TAG, "capabilities", "scheduling"],
        doc_md=DOC_TEMPLATE.format(cron=cron),
    )
    def _sched_dag():
        @task
        def log_trigger_time(logical_date=None, run_id=None):
            print(f"cron='{cron}' fired | logical_date={logical_date} | run_id={run_id}")

        log_trigger_time()

    _sched_dag()


for _name, _cron in SCHEDULES.items():
    build_dag(_name, _cron)

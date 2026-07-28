"""Scheduling & Calendar Management — timezone-aware scheduling.

Validates:
  - Timezone-aware scheduling (multi-timezone support)

Two identical DAGs scheduled at "07:00" — one in US/Eastern (DST-aware), one in
UTC — to show trigger times respect the DAG's timezone (AutoSys `timezone` JIL
attribute equivalent).
"""

from datetime import datetime

import pendulum
from airflow.sdk import dag, task

from include.validation_kit.config import KIT_TAG

DOC = """
### What this validates
`start_date` carries a pendulum timezone; the cron is evaluated in that zone and
DST transitions are handled automatically (Eastern 07:00 stays 07:00 local
across the March/November shifts).

### How to validate
Unpause both DAGs. In the run history, `sched_tz_eastern` fires at 07:00
US/Eastern (11:00/12:00 UTC depending on DST) and `sched_tz_utc` at 07:00 UTC.
Compare the two runs' UTC timestamps — 4-5h apart.
"""


def build_tz_dag(dag_id: str, tz: str):
    @dag(
        dag_id=dag_id,
        schedule="0 7 * * *",
        start_date=pendulum.datetime(2026, 7, 1, tz=tz),
        catchup=False,
        tags=[KIT_TAG, "capabilities", "scheduling", "timezone"],
        doc_md=DOC,
    )
    def _tz_dag():
        @task
        def log_times(logical_date=None):
            local = logical_date.in_timezone(tz)
            print(f"tz={tz} | logical(UTC)={logical_date} | logical(local)={local}")

        log_times()

    _tz_dag()


build_tz_dag("sched_tz_eastern", "America/New_York")
build_tz_dag("sched_tz_utc", "UTC")

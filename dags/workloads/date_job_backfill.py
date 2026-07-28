"""Date Job workload validation — date-driven orchestration + backfill/catchup.

Validates:
  - Date-driven orchestration (daily/weekly/monthly)
  - Catchup/backfill behavior
  - Correct handling of missed days and reruns

Also validates:
  - Backfill / catch-up for missed runs (AutoSys has no native backfill)
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task

from include.validation_kit.config import KIT_TAG

DOC = """
### What this validates
This DAG is **date-driven**: every run processes its `logical_date` (the business
date), not the wall-clock date — the core concept that replaces AutoSys date jobs.

- `catchup=True` + a `start_date` in the past: when unpaused, Airflow
  automatically creates one run per missed daily interval (AutoSys requires
  manual force-starts per missed date).
- Reruns: clear any historical run and it re-executes for the *same* business
  date — deterministic reprocessing.

### How to validate
1. Unpause the DAG. Watch Airflow create backfill runs from the `start_date`
   forward, in order (`max_active_runs=1` keeps them sequential).
2. Confirm each run's log prints its own business date, not today's date.
3. Clear one historical run in the Grid view — confirm it reruns with the same
   `logical_date`.
4. CLI alternative: `astro deployment airflow run dags backfill ...` or the
   Airflow 3 UI backfill dialog — create a backfill for an arbitrary window.
"""


@dag(
    schedule="@daily",
    start_date=datetime(2026, 7, 20),
    catchup=True,
    max_active_runs=1,
    tags=[KIT_TAG, "workloads", "date-job", "backfill"],
    doc_md=DOC,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def date_job_backfill():

    @task
    def process_business_date(ds=None, logical_date=None, run_id=None):
        print(f"BUSINESS DATE: {ds} | logical_date={logical_date} | run_id={run_id}")
        print(f"Simulating date-partitioned processing for partition dt={ds}")
        return ds

    @task
    def verify_date_consistency(processed_date: str, ds=None):
        assert processed_date == ds, f"Date mismatch: {processed_date} != {ds}"
        print(f"Run processed its own interval correctly: {ds}")

    verify_date_consistency(process_business_date())


date_job_backfill()

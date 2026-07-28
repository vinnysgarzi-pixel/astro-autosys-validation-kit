"""Dependency Modeling — cross-DAG dependencies via Assets (data-aware scheduling).

Validates:
  - Cross-DAG dependencies (Dataset-based triggers or ExternalTaskSensor)
  - Job-to-job sequential dependency across workflows

AutoSys equivalent: `condition: success(job_in_other_box)`. In Airflow 3 the
producer updates an Asset; any DAG scheduled on that Asset triggers
automatically — visible in the UI's Assets view.
"""

from datetime import datetime

from airflow.sdk import Asset, dag, task

from include.validation_kit.config import KIT_TAG

daily_extract = Asset("validation://daily_extract")

PRODUCER_DOC = """
### What this validates
Producer side of an Asset-based cross-DAG dependency. When `produce_extract`
succeeds, the `validation://daily_extract` Asset is updated, which
auto-triggers `dep_asset_consumer` — no sensor, no polling.

### How to validate
1. Trigger this DAG. When it completes, `dep_asset_consumer` starts on its own.
2. Open the **Assets** view — the producer→asset→consumer graph is the
   dependency-visualization evidence.
3. Fail the producer (conf `{"fail": true}`) — consumer does NOT trigger.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "assets"],
    doc_md=PRODUCER_DOC,
    params={"fail": False},
)
def dep_asset_producer():

    @task(outlets=[daily_extract])
    def produce_extract(params: dict, ds=None):
        if params["fail"]:
            raise RuntimeError("Simulated extract failure — asset must NOT update")
        print(f"Extract for {ds} produced; asset {daily_extract.uri} updated")

    produce_extract()


dep_asset_producer()


@dag(
    schedule=[daily_extract],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "assets"],
    doc_md="Consumer: runs automatically whenever `dep_asset_producer` updates the asset.",
)
def dep_asset_consumer():

    @task
    def consume_extract(triggering_asset_events=None):
        for asset, events in (triggering_asset_events or {}).items():
            print(f"Triggered by asset {asset} with {len(events)} event(s)")
        print("Downstream processing of the extract complete")

    consume_extract()


dep_asset_consumer()

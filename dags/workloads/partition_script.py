"""Partition Script workload validation — dynamic per-partition processing.

Validates:
  - Partition scripts: runtime-determined partition fan-out, the common
    AutoSys pattern where a wrapper script enumerates partitions and
    launches child jobs

Also validates:
  - Dynamic scheduling based on runtime parameters or upstream outputs
    (Dynamic Task Mapping)
"""

from datetime import datetime

from airflow.sdk import Param, dag, task

from include.validation_kit.config import KIT_TAG

DOC = """
### What this validates
AutoSys handles per-partition work with wrapper scripts that generate/force-start
child jobs. In Airflow this is **Dynamic Task Mapping**: `list_partitions`
determines the partition set at runtime, and one `process_partition` task
instance is expanded per partition — visible individually in the Grid view,
each with its own log, retries, and state.

### How to validate
1. Trigger with default params — 4 mapped task instances appear under
   `process_partition`.
2. Re-trigger with `{"partitions": ["p1", ..., "p12"]}` — 12 instances, no code
   change. This is the runtime-determined parallelism AutoSys lacks.
3. Fail one partition (`{"fail_partition": "p2"}`) — only that mapped instance
   fails and can be cleared/rerun individually; the consolidation task waits.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "partition-script", "dynamic-task-mapping"],
    doc_md=DOC,
    params={
        "partitions": Param(
            ["region=NE", "region=SE", "region=MW", "region=W"],
            type="array",
            description="Partitions to process (simulates runtime discovery)",
        ),
        "fail_partition": Param("", type="string", description="Force this partition to fail"),
    },
)
def partition_script():

    @task
    def list_partitions(params: dict) -> list[str]:
        """Runtime partition discovery (in production: query catalog/S3 listing)."""
        print(f"Discovered partitions: {params['partitions']}")
        return params["partitions"]

    @task
    def process_partition(partition: str, params: dict, ds=None) -> str:
        if partition == params["fail_partition"]:
            raise RuntimeError(f"Simulated failure for partition {partition}")
        print(f"Processing partition {partition} for dt={ds}")
        return partition

    @task
    def consolidate(processed: list[str]):
        print(f"Fan-in complete — {len(processed)} partitions processed: {processed}")

    consolidate(process_partition.expand(partition=list_partitions()))


partition_script()

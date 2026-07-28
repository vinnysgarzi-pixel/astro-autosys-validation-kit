"""Framework jobs — EMR Serverless submission validation.

Validates:
  - Submit EMR Serverless job via Orchestrator API
  - Supports job parameterization (env, dataset date, partition)
  - Supports retries, timeouts, and failure handling
  - Captures run IDs and maps Airflow task <-> framework job
"""

from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.sdk import Param, Variable, dag, task

from include.validation_kit.config import (
    CONN_AWS,
    DEFAULT_BUCKET,
    KIT_TAG,
    VAR_EMR_APP_ID,
    VAR_EMR_EXEC_ROLE_ARN,
    VAR_BUCKET,
)

DOC = """
### What this validates
1. **EMR Serverless submission** — `EmrServerlessStartJobOperator` submits a Spark
   job to the application configured in the `validation_emr_serverless_app_id` Variable,
   using the `validation_aws` connection (pulled from Vault at runtime).
2. **Parameterization** — trigger this DAG with config, e.g.
   `{"env": "dev", "dataset_date": "2026-07-01", "partition": "region=NE"}`.
   The values flow into the Spark job as arguments via `dag_run.conf` / Params.
3. **Retries / timeouts / failure handling** — the submit task has
   `retries=2`, `retry_delay=1m`, and `execution_timeout=30m`. Kill the EMR job
   mid-run or point it at a bad script to observe retry + failure behavior.
4. **Run ID mapping** — the EMR `job_run_id` is pushed to XCom and logged next to
   the Airflow `run_id`/`task_id` by the `record_run_mapping` task, proving the
   Airflow task ↔ framework job correlation.

### How to validate
1. Set Variables `validation_emr_serverless_app_id`, `validation_emr_execution_role_arn`,
   `validation_bucket`, and create the `validation_aws` connection in Vault.
2. Trigger with custom conf. Confirm the job appears in the EMR Serverless console.
3. Open the `record_run_mapping` task log — capture the Airflow-run ↔ EMR-run-id
   line as evidence.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "framework", "emr-serverless"],
    doc_md=DOC,
    params={
        "env": Param("dev", type="string", description="Target environment"),
        "dataset_date": Param("{{ ds }}", type="string", description="Dataset/business date"),
        "partition": Param("", type="string", description="Optional partition spec"),
        "entry_point": Param(
            "s3://REPLACE-ME/scripts/sample_spark_job.py",
            type="string",
            description="S3 path of the Spark entrypoint script",
        ),
    },
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
)
def emr_serverless_job():

    @task
    def build_job_driver(params: dict) -> dict:
        """Assemble the sparkSubmit payload from runtime parameters."""
        bucket = Variable.get(VAR_BUCKET, default=DEFAULT_BUCKET)
        args = ["--env", params["env"], "--dataset-date", params["dataset_date"]]
        if params["partition"]:
            args += ["--partition", params["partition"]]
        return {
            "sparkSubmit": {
                "entryPoint": params["entry_point"],
                "entryPointArguments": args,
                "sparkSubmitParameters": (
                    f"--conf spark.hadoop.fs.s3a.buffer.dir=/tmp "
                    f"--conf spark.eventLog.dir=s3://{bucket}/emr-logs/"
                ),
            }
        }

    submit_emr_job = EmrServerlessStartJobOperator(
        task_id="submit_emr_job",
        aws_conn_id=CONN_AWS,
        application_id="{{ var.value." + VAR_EMR_APP_ID + " }}",
        execution_role_arn="{{ var.value." + VAR_EMR_EXEC_ROLE_ARN + " }}",
        job_driver=build_job_driver(),
        wait_for_completion=True,
        deferrable=True,
    )

    @task
    def record_run_mapping(emr_job_run_id: str, run_id: str, ti=None):
        """Log the Airflow-run <-> EMR-job-run correlation for evidence capture."""
        print(
            f"RUN MAPPING | airflow_run_id={run_id} | "
            f"task={ti.task_id} try={ti.try_number} | emr_job_run_id={emr_job_run_id}"
        )
        return {"airflow_run_id": run_id, "emr_job_run_id": emr_job_run_id}

    record_run_mapping(
        emr_job_run_id=submit_emr_job.output,
        run_id="{{ run_id }}",
    )


emr_serverless_job()

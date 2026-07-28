"""Talend workload validation — STUB, pending confirmation of your Talend invocation method.

Validates:
  - Can run Talend jobs using current deployment method (jar, command wrapper,
    API submission, etc.)
  - Supports parameter passing, return code mapping
  - Supports standard operational retries/alerts

TODO: confirm how Talend jobs are invoked today. This DAG
ships two candidate patterns behind a param switch; delete the one that does
not apply once confirmed:
  1. "command"  — run a Talend job launcher (jar / shell wrapper) via BashOperator.
  2. "api"      — submit an execution to Talend Management Console (TMC) via the
                  `validation_talend` HTTP connection.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

from include.validation_kit.config import CONN_TALEND, KIT_TAG

DOC = """
### Status: awaiting confirmation of Talend invocation method

Two candidate patterns are wired behind the `invocation_method` param:

- **command** — `BashOperator` runs a launcher command (edit `TALEND_COMMAND`
  below to the real jar/wrapper invocation). Talend context params are passed as
  `--context_param name=value` arguments; the process exit code maps directly to
  task success/failure and drives the configured retries.
- **api** — placeholder task showing where a TMC `POST /executions` call goes,
  using the `validation_talend` HTTP connection managed in Astro.

### How to validate (once method confirmed)
1. Set `invocation_method` and the real command/endpoint.
2. Trigger with `{"context_params": {"dataset_date": "2026-07-01"}}` — confirm
   params reach the Talend job.
3. Force a non-zero return code — confirm the task fails, retries 2x, and an
   alert fires (see reporting/alerts setup in README).
"""

# TODO: replace with the real launcher invocation.
TALEND_COMMAND = "echo 'REPLACE-ME: talend job launcher' && exit 0"


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "talend", "stub"],
    doc_md=DOC,
    params={
        "invocation_method": Param(
            "command", type="string", enum=["command", "api"],
            description="How to invoke the Talend job",
        ),
        "context_params": Param(
            {"dataset_date": "2026-07-01"}, type="object",
            description="Talend context parameters",
        ),
    },
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
)
def talend_job():

    @task.branch
    def choose_method(params: dict) -> str:
        return f"run_via_{params['invocation_method']}"

    run_via_command = BashOperator(
        task_id="run_via_command",
        bash_command=(
            TALEND_COMMAND
            + "{% for k, v in params.context_params.items() %}"
              " --context_param {{ k }}={{ v }}"
              "{% endfor %}"
        ),
    )

    @task
    def run_via_api(params: dict):
        """Placeholder for a TMC API submission.

        TODO: replace with HttpOperator against the TMC executions
        endpoint using the `validation_talend` connection, then poll the
        execution status and raise on failure so retries/alerts engage.
        """
        raise NotImplementedError(
            f"TMC API submission not yet configured (conn_id={CONN_TALEND}). "
            f"Would submit with context: {params['context_params']}"
        )

    choose_method() >> [run_via_command, run_via_api()]


talend_job()

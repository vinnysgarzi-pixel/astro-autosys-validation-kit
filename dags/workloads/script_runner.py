"""Script (Shell/Python) workload validation.

Validates:
  - Can run scripts in controlled runtime (dependency packaging, versions)
  - Stdout/stderr logs captured and visible to ops
  - Script exit codes properly mark task success/failure
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task

from include.validation_kit.config import KIT_TAG

DOC = """
### What this validates
1. **Controlled runtime** — `print_runtime_versions` prints the Python version and
   installed package versions inside the RE Agent worker, proving dependencies are
   packaged in the Astro Runtime image (see `requirements.txt` / `Dockerfile`).
2. **Stdout/stderr capture** — `run_shell_script` executes
   `include/scripts/sample_etl.sh`, which writes to both stdout and stderr.
   Both streams must appear in the task log in the Astro UI.
3. **Exit-code handling** — trigger with `{"exit_code": 3}` and the shell task
   fails (non-zero exit marks the task failed and fires retries); trigger with
   `{"exit_code": 0}` and it succeeds.

### How to validate
1. Trigger with default params — all tasks green; open logs and confirm both
   STDOUT and STDERR lines are visible.
2. Trigger with `{"exit_code": 3}` — confirm `run_shell_script` retries twice,
   then marks failed, and `python_script` is skipped (default trigger rule).
3. Screenshot the task log + Grid view as evidence.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "workloads", "scripts"],
    doc_md=DOC,
    params={
        "exit_code": Param(0, type="integer", description="Exit code for the shell script (non-zero = fail)"),
    },
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
)
def script_runner():

    @task
    def print_runtime_versions():
        import sys
        from importlib import metadata

        print(f"Python: {sys.version}")
        for pkg in ("apache-airflow", "boto3", "pendulum"):
            try:
                print(f"{pkg}=={metadata.version(pkg)}")
            except metadata.PackageNotFoundError:
                print(f"{pkg}: not installed")

    run_shell_script = BashOperator(
        task_id="run_shell_script",
        bash_command=(
            "bash /usr/local/airflow/include/scripts/sample_etl.sh "
            "{{ params.exit_code }} {{ ds }}"
        ),
    )

    @task
    def python_script(logical_date=None):
        """Simulates a Python ETL step; stdout lands in the task log."""
        print(f"Processing business date {logical_date} — Python script workload OK")
        return "done"

    print_runtime_versions() >> run_shell_script >> python_script()


script_runner()

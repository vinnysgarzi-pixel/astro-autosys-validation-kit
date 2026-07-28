"""Dependency Modeling — sequential dependencies, trigger rules, branching, fan-out/fan-in.

Validates:
  - Job-to-job sequential dependency (task A must complete before task B)
  - Conditional branching (if/else based on upstream result or return code)
  - Fan-out / fan-in (parallel execution then consolidation)
"""

from datetime import datetime

from airflow.sdk import Param, dag, task

from include.validation_kit.config import KIT_TAG

SEQ_DOC = """
### What this validates
- **Sequential**: A >> B >> C — B waits for A's success (`condition: success(jobA)`).
- **Failure propagation**: trigger with `{"fail_task": "task_a"}` — B and C show
  `upstream_failed`, matching AutoSys behavior where dependents never start.

### How to validate
Trigger with defaults (all green, strictly ordered start times in Gantt view),
then with `{"fail_task": "task_a"}` and confirm downstream never ran.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "sequential"],
    doc_md=SEQ_DOC,
    params={"fail_task": Param("", type="string", enum=["", "task_a", "task_b", "task_c"])},
)
def dep_sequential():

    def make_step(name: str):
        @task(task_id=name)
        def step(params: dict):
            if params["fail_task"] == name:
                raise RuntimeError(f"Simulated failure in {name}")
            print(f"{name} complete")

        return step()

    make_step("task_a") >> make_step("task_b") >> make_step("task_c")


dep_sequential()


BRANCH_DOC = """
### What this validates
Conditional branching on an upstream "return code" — the AutoSys
`exitcode(jobA) = <value>` pattern. `@task.branch` selects the success or
remediation path; the untaken branch shows **skipped**; `join` runs either way
via `trigger_rule="none_failed_min_one_success"`.

### How to validate
Trigger with `{"exit_code": 0}` → happy path runs, remediation skipped.
Trigger with `{"exit_code": 7}` → remediation runs, happy path skipped.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "branching"],
    doc_md=BRANCH_DOC,
    params={"exit_code": Param(0, type="integer")},
)
def dep_branching():

    @task
    def upstream_job(params: dict) -> int:
        code = params["exit_code"]
        print(f"upstream job finished with exit code {code}")
        return code

    @task.branch
    def route_on_exit_code(code: int) -> str:
        return "happy_path" if code == 0 else "remediation_path"

    @task
    def happy_path():
        print("exit code 0 — normal downstream processing")

    @task
    def remediation_path():
        print("non-zero exit code — remediation/alternate processing")

    @task(trigger_rule="none_failed_min_one_success")
    def join():
        print("flow consolidated after branch")

    route_on_exit_code(upstream_job()) >> [happy_path(), remediation_path()] >> join()


dep_branching()


FAN_DOC = """
### What this validates
Fan-out / fan-in: 5 tasks run in parallel, then a consolidation task waits for
ALL of them (AutoSys: independent jobs in a box + downstream with
`success(jobA) AND success(jobB) ...`).

### How to validate
Trigger; Gantt view shows the 5 workers overlapping and `consolidate` starting
only after the slowest finishes. Fail one (`{"fail_worker": 3}`) — consolidate
does not run.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "dependencies", "fan-out"],
    doc_md=FAN_DOC,
    params={"fail_worker": Param(-1, type="integer", description="Worker index to fail (0-4), -1 = none")},
)
def dep_fanout_fanin():

    @task
    def worker(i: int, params: dict) -> int:
        import time

        if i == params["fail_worker"]:
            raise RuntimeError(f"Simulated failure in worker_{i}")
        time.sleep(10 + i * 5)
        print(f"worker_{i} done")
        return i

    @task
    def consolidate(results: list[int]):
        print(f"fan-in complete; workers finished: {sorted(results)}")

    consolidate(worker.expand(i=list(range(5))))


dep_fanout_fanin()

"""Job Control & Operational Actions playground.

Validates:
  - ON-HOLD equivalent (pause DAG / pool-based task blocking)
  - ON-ICE equivalent (mark success to unblock dependents)
  - Force-start / ad-hoc trigger
  - Kill / terminate running job
  - Rerun / clear task state
  - Mark success / mark failed
  - Pause entire workflow vs pause individual task

One DAG with deliberately slow / controllable tasks so every operational action
can be exercised from the Astro UI and the behavior compared with AutoSys
sendevent commands.
"""

import time
from datetime import datetime, timedelta

from airflow.sdk import Param, dag, task

from include.validation_kit.config import KIT_TAG

DOC = """
### Operational test matrix (run each action against this DAG)

| AutoSys action | Astro equivalent | How to test here |
|---|---|---|
| `FORCE_STARTJOB` | Trigger button / `POST /dagRuns` | Trigger manually anytime; pass conf `{"sleep_seconds": 600}` |
| `HOLD_JOB` (workflow) | Pause DAG toggle | Pause; confirm no new scheduled runs; unpause resumes |
| `HOLD_JOB` (single task) | Pool with 0 slots | Set `long_running_step`'s pool `validation_gate` to 0 slots (Admin > Pools); task queues until slots restored |
| `KILLJOB` | Mark running task as Failed | While `long_running_step` runs, mark it failed — process terminates, downstream skipped |
| `CHANGE_STATUS -s INACTIVE` + rerun | Clear task | Clear `flaky_step` (with/without downstream) — re-executes |
| `CHANGE_STATUS -s SUCCESS` (≈ ON-ICE) | Mark task success | Mark `flaky_step` success while pending/failed — `final_step` proceeds (dependents treat it as success) |
| `CHANGE_STATUS -s FAILURE` | Mark task failed | Mark success-pending task failed — downstream skipped per trigger rule |

### Notes vs AutoSys
- ON-ICE's "dependents treat as success" has no single-toggle equivalent —
  mark-success is the operational replacement; document this gap for your migration plan.
- Kill: marking a running task failed terminates the process on the RE Agent;
  retries are NOT consumed by a manual fail (state is final unless cleared).
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "ops-actions"],
    doc_md=DOC,
    params={
        "sleep_seconds": Param(300, type="integer", description="Duration of the long-running step"),
        "fail_flaky_step": Param(False, type="boolean", description="Force flaky_step to fail"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
)
def ops_actions_playground():

    @task
    def start_marker(run_id=None):
        print(f"Run {run_id} started — use this run to exercise operational actions")

    @task(pool="validation_gate")
    def long_running_step(params: dict):
        """Kill target: mark this failed while running to test KILLJOB behavior.

        Pool `validation_gate` (create with >=1 slot; set to 0 to test
        task-level hold).
        """
        total = params["sleep_seconds"]
        for elapsed in range(0, total, 15):
            print(f"working... {elapsed}/{total}s")
            time.sleep(min(15, total - elapsed))
        print("long_running_step completed normally")

    @task
    def flaky_step(params: dict):
        """Mark-success / mark-failed / clear target."""
        if params["fail_flaky_step"]:
            raise RuntimeError("Simulated failure — now test clear / mark-success from the UI")
        print("flaky_step succeeded")

    @task
    def final_step():
        print("final_step ran — upstream states satisfied the default trigger rule")

    start_marker() >> long_running_step() >> flaky_step() >> final_step()


ops_actions_playground()

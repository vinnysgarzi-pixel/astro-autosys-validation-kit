"""Job Control — hold and release dependencies (manual gate).

Validates:
  - Hold and release dependencies — manually gate downstream tasks
    (AutoSys: hold a box job to prevent children from running, then release)
"""

from datetime import datetime

from airflow.sdk import Variable, dag, task

from include.validation_kit.config import KIT_TAG, VAR_APPROVAL_FLAG

DOC = """
### What this validates
A gate task polls the `validation_approval_flag` Variable; downstream work is
held until an operator (or an external system via the Variables API) sets it to
`approved` — the AutoSys hold/release pattern with an explicit, audited release
action.

### How to validate
1. Set Variable `validation_approval_flag` to `hold` (Admin > Variables).
2. Trigger the DAG — `wait_for_approval` stays in **up_for_reschedule**
   (visible waiting state, worker slot released between pokes).
3. Release: set the Variable to `approved` (UI, or
   `PATCH /variables/validation_approval_flag` from an external system).
4. Confirm downstream runs within one poke interval (30s), and the Variable
   change is in the audit log (who released, when).

Alternative hold patterns covered elsewhere: pool-with-0-slots
(`ops_actions_playground`) and pausing a downstream DAG.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "gate", "hold-release"],
    doc_md=DOC,
)
def gate_manual_approval():

    @task.sensor(poke_interval=30, timeout=60 * 60 * 2, mode="reschedule")
    def wait_for_approval() -> bool:
        flag = Variable.get(VAR_APPROVAL_FLAG, default="hold")
        print(f"approval flag = {flag!r} (set to 'approved' to release)")
        return str(flag).lower() == "approved"

    @task
    def held_work():
        print("Gate released — held downstream work now executing")

    @task
    def reset_gate():
        """Re-arm the gate so the next run holds again."""
        Variable.set(VAR_APPROVAL_FLAG, "hold")
        print("Gate reset to 'hold'")

    wait_for_approval() >> held_work() >> reset_gate()


gate_manual_approval()

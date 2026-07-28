# AutoSys → Astro Validation Kit

A ready-to-run set of Airflow 3 DAGs for validating an AutoSys → Astro
(Apache Airflow) migration. Every DAG demonstrates one migration concern —
workload types, scheduling & calendars, operational actions, dependency
modeling, secrets custody, or reporting — and documents its own test plan:
open any DAG in the Airflow UI and click **DAG Docs** for *What this
validates* and *How to validate*.

Built for [Astro Remote Execution](https://www.astronomer.io/docs/astro/remote-execution-agents)
(Astro Runtime 3.x / Airflow 3), but the DAGs run on any Airflow 3 deployment.

## What's included

```
dags/
  workloads/       script_runner           — shell/Python scripts, exit-code handling
                   date_job_backfill       — date-driven runs, catchup/backfill
                   partition_script        — dynamic per-partition fan-out
  capabilities/    sched_*                 — cron variants, holiday calendar,
                                             blackout windows, timezones
                   ops_actions_playground  — kill / hold / rerun / mark-success
                                             (sendevent equivalents, with a mapping table)
                   dep_*                   — sequential, Assets, ExternalTaskSensor,
                                             branching, fan-out/fan-in, time waits,
                                             API-triggered runs
                   bridge_autosys          — AutoSys ↔ Airflow coexistence (both directions)
                   gate_manual_approval    — hold/release gate via Variable
                   connections_variables_check — Astro-managed credential
                                             custody + rotation proof
  ops/             long-running / pool-throttle / CPU-load DAGs to run during
                   infrastructure tests (HA, drain, autoscaling)
  reporting/       metrics_report          — run counts, success rates, and
                                             run-history CSV via the Airflow REST API
include/
  validation_kit/config.py   ★ all connection IDs & Variable names (placeholders)
  timetables/                custom holiday/blackout timetables
  data/holidays.csv          editable holiday calendar
  scripts/                   sample shell workload
plugins/                     timetable plugin registration
```

## Quick start

Drop the three directories into an Astro project (or clone and add a
`Dockerfile` with `FROM astrocrpublic.azurecr.io/runtime:3.3-2`), add the
packages from `requirements.txt`, then:

```bash
astro dev start     # local
# or
astro deploy        # to an Astro deployment
```

Many DAGs need no configuration at all — start with `script_runner`,
`partition_script`, `date_job_backfill`, the `dep_*` DAGs, and
`ops_actions_playground`.

## Configuration

All external references are placeholders defined once in
`include/validation_kit/config.py`. Create what you need as you go in the
**Astro Environment Manager** (Astro UI → Deployment → Environment) or the
Airflow UI (Admin → Connections/Variables):

**Connections**:

| Connection ID | Type | Used by |
|---|---|---|
| `validation_aws` | aws | S3 report export, connection-retrieval check |
| `validation_autosys_api` | http | AutoSys bridge |
| `validation_internal_api` | http | API-call test (httpbin.org works) |
| `validation_airflow_api` | http | metrics report (deployment URL + API token) |

**Variables**: `validation_bucket`, `validation_test_secret`,
`validation_approval_flag`. Mark sensitive values as **secret** in Astro so
they are masked in the UI.

**Pools** (Admin → Pools): `validation_gate` (1 slot),
`validation_throttle` (5 slots).

All DAGs are tagged `validation-kit` plus a category tag for filtering.

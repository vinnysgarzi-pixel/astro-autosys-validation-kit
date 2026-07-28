"""Central configuration for the validation kit.

All external systems are referenced through the placeholder connection IDs and
variable names below. Create these connections and variables in the Astro
Environment Manager (or the Airflow UI) — the DAGs never hard-code credentials.

To change a connection ID, update it here once; every DAG imports from this
module.
"""

# ---------------------------------------------------------------------------
# Connection IDs (create in the Astro Environment Manager: Astro UI ->
# Deployment -> Environment -> Connections; or Airflow UI -> Admin -> Connections)
# ---------------------------------------------------------------------------

# AWS account used for the S3 report export and connection-retrieval check.
CONN_AWS = "validation_aws"

# HTTP connection for the AutoSys REST API (bridge / coexistence tests).
CONN_AUTOSYS_API = "validation_autosys_api"

# Generic internal REST API used for the external-API trigger capability test.
CONN_INTERNAL_API = "validation_internal_api"

# HTTP connection to this Deployment's Airflow REST API (host = deployment URL,
# password = Deployment API token). Used by the metrics/reporting DAG.
CONN_AIRFLOW_API = "validation_airflow_api"

# ---------------------------------------------------------------------------
# Airflow Variables (create in the Astro Environment Manager or the
# Airflow UI: Admin -> Variables; mark secret values as secret in Astro)
# ---------------------------------------------------------------------------

# S3 bucket used by the report-export task in the metrics DAG.
VAR_BUCKET = "validation_bucket"                # e.g. "my-validation-bucket"

# Variable name used by the connection/variable retrieval validation DAG.
VAR_TEST_SECRET = "validation_test_secret"

# Manual-approval gate flag (set to "approved" to release the gate DAG).
VAR_APPROVAL_FLAG = "validation_approval_flag"

# ---------------------------------------------------------------------------
# Defaults used when a Variable is not set (keeps DAGs parseable/testable
# before the environment is wired up)
# ---------------------------------------------------------------------------

DEFAULT_BUCKET = "REPLACE-ME-validation-bucket"

# Tag applied to every DAG in this project so they are easy to filter.
KIT_TAG = "validation-kit"

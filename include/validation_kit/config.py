"""Central configuration for the validation kit.

All external systems are referenced through the placeholder connection IDs and
variable names below. Create these connections in your secrets backend
(e.g. HashiCorp Vault — see README.md) — the DAGs never hard-code credentials.

To change a connection ID, update it here once; every DAG imports from this
module.
"""

# ---------------------------------------------------------------------------
# Connection IDs (create these in Vault under the configured connections_path)
# ---------------------------------------------------------------------------

# AWS account used for EMR Serverless, S3 sensors, and remote logging tests.
CONN_AWS = "validation_aws"

# HTTP connection pointing at a framework/orchestrator API base URL.
CONN_ORCHESTRATOR_API = "validation_orchestrator_api"

# HTTP connection for the AutoSys REST API (bridge / coexistence tests).
CONN_AUTOSYS_API = "validation_autosys_api"

# HTTP connection for Talend invocation (TMC API or internal wrapper service).
# NOTE: adjust to your Talend invocation method — see dags/workloads/talend_job.py
CONN_TALEND = "validation_talend"

# Generic internal REST API used for the external-API trigger capability test.
CONN_INTERNAL_API = "validation_internal_api"

# HTTP connection to this Deployment's Airflow REST API (host = deployment URL,
# password = Deployment API token). Used by the metrics/reporting DAG.
CONN_AIRFLOW_API = "validation_airflow_api"

# ---------------------------------------------------------------------------
# Airflow Variables (create in Vault under the configured variables_path,
# or in the Airflow UI for quick testing)
# ---------------------------------------------------------------------------

# S3 bucket used by file-watcher / CA7 flag-file / partition tests.
VAR_BUCKET = "validation_bucket"                # e.g. "my-validation-bucket"

# Prefix within the bucket where test files are dropped.
VAR_PREFIX = "validation_prefix"                # e.g. "astro-validation"

# EMR Serverless application ID + job execution role ARN.
VAR_EMR_APP_ID = "validation_emr_serverless_app_id"
VAR_EMR_EXEC_ROLE_ARN = "validation_emr_execution_role_arn"

# Secret name used by the Vault secret-retrieval validation DAG.
VAR_VAULT_TEST_SECRET = "validation_vault_test_secret"

# Manual-approval gate flag (set to "approved" to release the gate DAG).
VAR_APPROVAL_FLAG = "validation_approval_flag"

# ---------------------------------------------------------------------------
# Defaults used when a Variable is not set (keeps DAGs parseable/testable
# before the environment is wired up)
# ---------------------------------------------------------------------------

DEFAULT_BUCKET = "REPLACE-ME-validation-bucket"
DEFAULT_PREFIX = "astro-validation"

# Tag applied to every DAG in this project so they are easy to filter.
KIT_TAG = "validation-kit"

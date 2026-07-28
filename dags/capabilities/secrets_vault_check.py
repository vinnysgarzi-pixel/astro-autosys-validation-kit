"""Security & Access Controls — Vault secrets backend validation.

Validates:
  - Credential custody (secrets in your Vault; not stored in Astro)
  - Secrets rotation (rotate in Vault; next run picks up the new value)
"""

from datetime import datetime

from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.hook import BaseHook

from include.validation_kit.config import CONN_AWS, KIT_TAG, VAR_VAULT_TEST_SECRET

DOC = """
### What this validates
With the HashiCorp Vault secrets backend configured on the Deployment (see
README > "Secrets backend setup"), connections and variables resolve from
**Vault at runtime on the RE Agent** — credentials never land in the Astro
control plane or metadata DB.

- `fetch_connection` resolves the `validation_aws` connection and logs only
  non-sensitive fields (host/login/port) plus a boolean "password present".
- `fetch_variable` resolves `validation_vault_test_secret` and logs only its
  length + SHA-256 prefix — enough to prove retrieval and detect rotation
  without ever printing the value.

### How to validate
1. Store the connection + variable in Vault under the paths the backend is
   configured with; run the DAG; confirm both tasks succeed.
2. **Custody**: confirm the connection does NOT appear in the Airflow
   metadata DB / UI connection list (it resolves from Vault only).
3. **Rotation**: change the secret value in Vault, rerun, and confirm the
   logged SHA-256 prefix changes — new value picked up with no Airflow change.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "security", "vault"],
    doc_md=DOC,
)
def secrets_vault_check():

    @task
    def fetch_connection():
        conn = BaseHook.get_connection(CONN_AWS)
        print(
            f"conn_id={conn.conn_id} type={conn.conn_type} host={conn.host} "
            f"login={conn.login} port={conn.port} password_present={bool(conn.password)}"
        )

    @task
    def fetch_variable():
        import hashlib

        value = str(Variable.get(VAR_VAULT_TEST_SECRET))
        digest = hashlib.sha256(value.encode()).hexdigest()[:12]
        print(f"secret retrieved: length={len(value)} sha256_prefix={digest}")
        print("(value intentionally not printed — rotation is proven by the hash changing)")

    fetch_connection()
    fetch_variable()


secrets_vault_check()

"""Security & Access Controls — Astro-managed connections and variables.

Validates:
  - Credential custody (connections/variables managed centrally in Astro,
    never hard-coded in DAG code or the repo)
  - Secret rotation (update the value in Astro; next run picks it up with no
    code change)
"""

from datetime import datetime

from airflow.sdk import Variable, dag, task
from airflow.sdk.bases.hook import BaseHook

from include.validation_kit.config import CONN_AWS, KIT_TAG, VAR_TEST_SECRET

DOC = """
### What this validates
Connections and Variables are managed in the **Astro Environment Manager**
(Astro UI → Deployment → Environment) — or in the Airflow UI under
Admin → Connections/Variables — and resolved by name at runtime. DAG code
carries only the connection ID, so credentials never live in the repo.

- `fetch_connection` resolves the `validation_aws` connection and logs only
  non-sensitive fields (host/login/port) plus a boolean "password present" —
  proving resolution without exposing the credential.
- `fetch_variable` resolves `validation_test_secret` and logs only its
  length + SHA-256 prefix — enough to prove retrieval and detect rotation
  without ever printing the value. Mark the Variable as **secret** in Astro
  so its value is also masked in the UI.

### How to validate
1. Create the `validation_aws` connection and `validation_test_secret`
   Variable in the Astro Environment Manager (or Airflow UI); run the DAG;
   confirm both tasks succeed.
2. **Custody**: confirm the DAG code and repo contain only the connection ID —
   the credential exists solely in Astro's management plane.
3. **Rotation**: change the Variable's value in Astro, rerun, and confirm the
   logged SHA-256 prefix changes — new value picked up with no code change
   or deploy.
"""


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[KIT_TAG, "capabilities", "security", "connections"],
    doc_md=DOC,
)
def connections_variables_check():

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

        value = str(Variable.get(VAR_TEST_SECRET))
        digest = hashlib.sha256(value.encode()).hexdigest()[:12]
        print(f"secret retrieved: length={len(value)} sha256_prefix={digest}")
        print("(value intentionally not printed — rotation is proven by the hash changing)")

    fetch_connection()
    fetch_variable()


connections_variables_check()

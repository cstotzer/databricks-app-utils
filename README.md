# databricks-app-utils

A lightweight Python library for building Streamlit apps on Databricks. It handles everything that sits below the business logic: reading configuration, authenticating with Databricks, executing SQL, and loading query files. Application code should depend on these abstractions rather than touching the Databricks connector directly.

**License:** GPL-3.0

---

## Modules at a glance

| Module | Class / function | Responsibility |
|---|---|---|
| `settings.py` | `AppSettings` | Reads all configuration from environment variables / `.env` |
| `auth.py` | `DatabricksAuth`, `build_auth()` | Translates settings into an auth value object |
| `sql_client.py` | `SQLClient` | Executes SQL queries against a Databricks SQL Warehouse |
| `workspace_client.py` | `build_workspace_client()` | Builds a `databricks.sdk.WorkspaceClient` with shared auth. Requires `[sdk]`. |
| `query_registry.py` | `QueryRegistry`, `SqlQuery` | Loads and caches `.sql` files from a Python package |

---

## Settings management

`AppSettings` is a [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) model. It reads every value from environment variables and optionally from a `.env` file in the working directory. Unknown variables are silently ignored.

### Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABRICKS_SERVER_HOSTNAME` | ✅ | — | `adb-xxx.azuredatabricks.net` (no `https://`) |
| `DATABRICKS_HTTP_PATH` | ✅ | — | `/sql/1.0/warehouses/…` |
| `DATABRICKS_AUTH_METHOD` | | `obo` | `pat` \| `u2m` \| `u2m_persistent` \| `obo` |
| `DATABRICKS_PAT` | ✅ if `auth_method=pat` | — | Personal access token |
| `DATABRICKS_DEFAULT_CATALOG` | | `None` | Applied as `USE CATALOG` before each query |
| `DATABRICKS_DEFAULT_SCHEMA` | | `None` | Applied as `USE SCHEMA` before each query |
| `DATABRICKS_CONNECT_TIMEOUT_S` | | `30` | Connection timeout in seconds |
| `DATABRICKS_RETRY_ATTEMPTS` | | `1` | Extra attempts on transient failures |
| `DATABRICKS_RETRY_BACKOFF_S` | | `0.5` | Initial backoff between retries (doubles each attempt) |
| `QUERY_TAG` | | `streamlit-app` | Prepended as a SQL comment: `/* streamlit-app */` |

### `.env` file (recommended for local development)

Create a `.env` file in the project root (never commit it):

```dotenv
DATABRICKS_SERVER_HOSTNAME=adb-1234567890123456.7.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890
DATABRICKS_AUTH_METHOD=u2m
DATABRICKS_DEFAULT_CATALOG=my_catalog
DATABRICKS_DEFAULT_SCHEMA=my_schema
```

For PAT authentication, add:

```dotenv
DATABRICKS_AUTH_METHOD=pat
DATABRICKS_PAT=dapi0123456789abcdef
```

### Usage

```python
from databricks_app_utils.settings import AppSettings

settings = AppSettings()
print(settings.databricks_server_hostname)
print(settings.databricks_auth_method)   # AuthMethod.U2M
```

In a Streamlit app, wrap it with `@st.cache_resource` so settings are read only once per server process:

```python
@st.cache_resource
def get_settings() -> AppSettings:
    return AppSettings()
```

---

## Authentication

See [`docs/authentication.md`](authentication.md) for a full technical deep-dive. The summary is:

| Method | `DATABRICKS_AUTH_METHOD` | Best for |
|---|---|---|
| **PAT** | `pat` | CI/CD, service accounts |
| **U2M** | `u2m` | Local development — browser OAuth, in-memory token cache, when only `SQLClient` is used |
| **U2M Persistent** | `u2m_persistent` | Local development — browser OAuth, disk token cache, shared between `SQLClient` and `WorkspaceClient`. Requires `[sdk]`. |
| **OBO** | `obo` | Deployed Databricks Apps |

### Usage

`build_auth()` converts settings into a `DatabricksAuth` value object. You rarely need to call it directly — `SQLClient` takes one as a constructor argument.

```python
from databricks_app_utils.settings import AppSettings
from databricks_app_utils.auth import build_auth

settings = AppSettings()
auth = build_auth(settings)
```

#### PAT

```dotenv
DATABRICKS_AUTH_METHOD=pat
DATABRICKS_PAT=dapi0123456789abcdef
```

```python
auth = build_auth(settings)
# auth.method  == AuthMethod.PAT
# auth.access_token == "dapi…"
```

#### U2M (browser OAuth — in-memory)

```dotenv
DATABRICKS_AUTH_METHOD=u2m
```

No secrets needed. On the first query, a browser window opens for the user to log in. Subsequent queries within the same server process reuse the cached token silently. Token is **lost on restart**.

```python
auth = build_auth(settings)
# auth.method            == AuthMethod.U2M
# auth.oauth_persistence  ← in-memory OAuthPersistenceCache, held for process lifetime
```

#### U2M Persistent (browser OAuth — disk cache, shared with WorkspaceClient)

```dotenv
DATABRICKS_AUTH_METHOD=u2m_persistent
```

Like `u2m` but backed by the Databricks SDK's disk-based `TokenCache` (`~/.config/databricks-sdk-py/oauth/`). The browser only opens on first use; subsequent starts — even after a restart — load the cached token and refresh it silently. The same cache file is shared with `WorkspaceClient`, so the browser flow fires at most once across both clients.

Requires `databricks-sdk`: `pip install 'databricks-app-utils[sdk]'`

```python
auth = build_auth(settings)  # checks disk cache; opens browser only if needed
# auth.method               == AuthMethod.U2M_PERSISTENT
# auth.credentials_provider  ← wraps sdk Config.authenticate (auto-refreshing)

ws = build_workspace_client(settings=settings, auth=auth)
db = SQLClient(settings=settings, auth=auth)
# Both hit the same TokenCache file on disk
```

#### OBO (Databricks Apps)

```dotenv
DATABRICKS_AUTH_METHOD=obo
```

The token is read from the `X-Forwarded-Access-Token` request header on every query. The token provider must be injected at runtime from the Streamlit layer:

```python
auth = DatabricksAuth(
    method=AuthMethod.OBO,
    token_provider=lambda: st.context.headers["X-Forwarded-Access-Token"],
)
```

---

## Database interface

`SQLClient` is the single interface for all SQL execution. It opens a short-lived connection per query (robust against warehouse idle timeouts) and applies `USE CATALOG` / `USE SCHEMA` automatically when defaults are configured.

### Query methods

| Method | Returns | Use when |
|---|---|---|
| `query_polars(sql, params)` | `polars.DataFrame` | You need a DataFrame for display or transformation |
| `query_pandas(sql, params)` | `pandas.DataFrame` | Interoperability with pandas-based libraries |
| `query(sql, params)` | `list[dict]` | Lightweight lookups; no Arrow overhead |
| `merge_dataframe(df, table, id_cols)` | `None` | Upsert a DataFrame into a Delta table |

### Named parameters

Use `:name` syntax in SQL. Lists are automatically expanded for `IN` clauses:

```python
db.query_polars(
    "SELECT * FROM orders WHERE status = :status AND region IN :regions",
    params={"status": "shipped", "regions": ["EU", "US"]},
)
# Executes: SELECT * FROM orders WHERE status = ? AND region IN (?, ?)
```

### Polars query

```python
from databricks_app_utils.sql_client import SQLClient

df = db.query_polars("SELECT id, name FROM customers LIMIT :n", params={"n": 100})
# Returns a polars.DataFrame
```

### Pandas query

```python
df = db.query_pandas("SELECT id, name FROM customers LIMIT :n", params={"n": 100})
# Returns a pandas.DataFrame
```

### Plain dict query

```python
rows = db.query("SELECT state, COUNT(*) AS cnt FROM customers GROUP BY state")
# Returns [{"state": "CA", "cnt": 1234}, …]
```

### Upsert (MERGE)

Merge a DataFrame into a Delta table using one or more identity columns:

```python
import polars as pl

updates = pl.DataFrame({"id": [1, 2], "score": [9.5, 7.1]})

db.merge_dataframe(
    df=updates,
    target_table="customer_scores",
    id_columns=["id"],
)
```

Optionally, supply a `version_column` for optimistic locking — rows whose version has changed since the data was read are silently skipped:

```python
db.merge_dataframe(
    df=updates,
    target_table="customer_scores",
    id_columns=["id"],
    version_column="updated_at",
)
```

### Retry behaviour

`SQLClient` retries failed queries with exponential backoff. Configure via settings:

```dotenv
DATABRICKS_RETRY_ATTEMPTS=2      # 2 extra attempts (3 total)
DATABRICKS_RETRY_BACKOFF_S=1.0   # 1 s, then 2 s
```

### Wiring it up in Streamlit

```python
@st.cache_resource
def get_db() -> SQLClient:
    settings = get_settings()
    auth = build_auth(settings)
    return SQLClient(settings=settings, auth=auth)
```

---

## Workspace client

`build_workspace_client()` returns a fully configured `databricks.sdk.WorkspaceClient` using the same `DatabricksAuth` object as `SQLClient`. Requires `pip install 'databricks-app-utils[sdk]'`.

Because it returns the SDK's own `WorkspaceClient` directly, the full SDK surface — jobs, clusters, secrets, Unity Catalog, and everything else — is available with complete type information.

### Usage

```python
from databricks_app_utils.auth import build_auth
from databricks_app_utils.workspace_client import build_workspace_client

settings = AppSettings()
auth = build_auth(settings)
ws = build_workspace_client(settings, auth)

for job in ws.jobs.list():
    print(job.settings.name)

ws.secrets.put_secret(scope="my-scope", key="api-key", string_value="…")
ws.clusters.get(cluster_id="1234-567890-abc123")
```

All four auth methods are supported. For `u2m_persistent`, `SQLClient` and `WorkspaceClient` share the same disk-based token cache so the browser opens at most once across both:

```python
auth = build_auth(settings)   # DATABRICKS_AUTH_METHOD=u2m_persistent
db = SQLClient(settings=settings, auth=auth)
ws = build_workspace_client(settings, auth)
# Both resolve to the same TokenCache file — browser fires once
```

For OBO, inject the `token_provider` before building either client:

```python
auth = DatabricksAuth(
    method=AuthMethod.OBO,
    token_provider=lambda: st.context.headers["X-Forwarded-Access-Token"],
)
db = SQLClient(settings=settings, auth=auth)
ws = build_workspace_client(settings, auth)
```

### Wiring it up in Streamlit

```python
@st.cache_resource
def get_workspace() -> WorkspaceClient:
    settings = get_settings()
    auth = build_auth(settings)
    return build_workspace_client(settings, auth)
```

---

## Query registry

`QueryRegistry` loads `.sql` files from a Python package directory at runtime and caches them in memory. This keeps SQL out of Python source files and makes queries easy to find, review, and test independently.

### File layout

SQL files live under a queries sub-package inside your app and are organised into sub-packages:

```
src/<your_app>/queries/
├── __init__.py
└── customers/
    ├── list_customers.sql
    ├── list_customers_by_state.sql
    └── list_states.sql
```

### Loading a query

```python
from databricks_app_utils.query_registry import QueryRegistry

registry = QueryRegistry(package="your_app.queries")
q = registry.get("customers/list_customers")

print(q.name)   # "customers/list_customers"
print(q.sql)    # "SELECT customerid, first_name …\n"
```

The registry is lazy — a file is read from disk only on first access, then cached for the lifetime of the instance.

### Passing a query to `SQLClient`

```python
q = registry.get("customers/list_customers_by_state")
df = db.query_polars(q.sql, params={"states": ["CA", "NY"], "limit": 200})
```

### Wiring it up in Streamlit

```python
@st.cache_resource
def get_queries() -> QueryRegistry:
    return QueryRegistry(package="your_app.queries")
```

### Why GPL-3.0?

We believe in open source software and want to ensure that improvements to this library remain open and available to everyone. The GPL-3.0 license guarantees that all derivatives and modifications stay free and open source.

---

**Made with ❤️ by the contributors**
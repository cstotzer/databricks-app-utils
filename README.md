# Core Module

The `app.core` package is the foundation of the application. It handles everything that sits below the business logic: reading configuration, authenticating with Databricks, executing SQL, and loading query files. Application code should depend on these abstractions rather than touching the Databricks connector directly.

**License:** MIT

---

## Modules at a glance

| Module | Class / function | Responsibility |
|---|---|---|
| `settings.py` | `AppSettings` | Reads all configuration from environment variables / `.env` |
| `auth.py` | `DatabricksAuth`, `build_auth()` | Translates settings into an auth value object |
| `databricks_client.py` | `DatabricksClient` | Executes SQL queries against a Databricks SQL Warehouse |
| `query_registry.py` | `QueryRegistry`, `SqlQuery` | Loads and caches `.sql` files from a Python package |

---

## Settings management

`AppSettings` is a [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) model. It reads every value from environment variables (prefix `APP_`) and optionally from a `.env` file in the working directory. Unknown variables are silently ignored.

### Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `APP_DATABRICKS_SERVER_HOSTNAME` | ✅ | — | `adb-xxx.azuredatabricks.net` (no `https://`) |
| `APP_DATABRICKS_HTTP_PATH` | ✅ | — | `/sql/1.0/warehouses/…` |
| `APP_DATABRICKS_AUTH_METHOD` | | `obo` | `pat` \| `u2m` \| `obo` |
| `APP_DATABRICKS_PAT` | ✅ if `auth_method=pat` | — | Personal access token |
| `APP_DATABRICKS_DEFAULT_CATALOG` | | `None` | Applied as `USE CATALOG` before each query |
| `APP_DATABRICKS_DEFAULT_SCHEMA` | | `None` | Applied as `USE SCHEMA` before each query |
| `APP_DATABRICKS_CONNECT_TIMEOUT_S` | | `30` | Connection timeout in seconds |
| `APP_DATABRICKS_RETRY_ATTEMPTS` | | `1` | Extra attempts on transient failures |
| `APP_DATABRICKS_RETRY_BACKOFF_S` | | `0.5` | Initial backoff between retries (doubles each attempt) |
| `APP_QUERY_TAG` | | `streamlit-app` | Prepended as a SQL comment: `/* streamlit-app */` |

### `.env` file (recommended for local development)

Create a `.env` file in the project root (never commit it):

```dotenv
APP_DATABRICKS_SERVER_HOSTNAME=adb-1234567890123456.7.azuredatabricks.net
APP_DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abcdef1234567890
APP_DATABRICKS_AUTH_METHOD=u2m
APP_DATABRICKS_DEFAULT_CATALOG=my_catalog
APP_DATABRICKS_DEFAULT_SCHEMA=my_schema
```

For PAT authentication, add:

```dotenv
APP_DATABRICKS_AUTH_METHOD=pat
APP_DATABRICKS_PAT=dapi0123456789abcdef
```

### Usage

```python
from app.core.settings import AppSettings

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

| Method | `APP_DATABRICKS_AUTH_METHOD` | Best for |
|---|---|---|
| **PAT** | `pat` | CI/CD, service accounts |
| **U2M** | `u2m` | Local development — browser OAuth, zero secrets |
| **OBO** | `obo` | Deployed Databricks Apps |

### Usage

`build_auth()` converts settings into a `DatabricksAuth` value object. You rarely need to call it directly — `DatabricksClient` takes one as a constructor argument.

```python
from app.core.settings import AppSettings
from app.core.auth import build_auth

settings = AppSettings()
auth = build_auth(settings)
```

#### PAT

```dotenv
APP_DATABRICKS_AUTH_METHOD=pat
APP_DATABRICKS_PAT=dapi0123456789abcdef
```

```python
auth = build_auth(settings)
# auth.method  == AuthMethod.PAT
# auth.access_token == "dapi…"
```

#### U2M (browser OAuth — recommended for local dev)

```dotenv
APP_DATABRICKS_AUTH_METHOD=u2m
```

No secrets needed. On the first query, a browser window opens for the user to log in. Subsequent queries within the same server process reuse the cached token silently.

```python
auth = build_auth(settings)
# auth.method         == AuthMethod.U2M
# auth.oauth_persistence  ← in-memory OAuthPersistenceCache, held for process lifetime
```

#### OBO (Databricks Apps)

```dotenv
APP_DATABRICKS_AUTH_METHOD=obo
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

`DatabricksClient` is the single interface for all SQL execution. It opens a short-lived connection per query (robust against warehouse idle timeouts) and applies `USE CATALOG` / `USE SCHEMA` automatically when defaults are configured.

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
from app.core.databricks_client import DatabricksClient

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

`DatabricksClient` retries failed queries with exponential backoff. Configure via settings:

```dotenv
APP_DATABRICKS_RETRY_ATTEMPTS=2      # 2 extra attempts (3 total)
APP_DATABRICKS_RETRY_BACKOFF_S=1.0   # 1 s, then 2 s
```

### Wiring it up in Streamlit

```python
@st.cache_resource
def get_db() -> DatabricksClient:
    settings = get_settings()
    auth = build_auth(settings)
    return DatabricksClient(settings=settings, auth=auth)
```

---

## Query registry

`QueryRegistry` loads `.sql` files from a Python package directory at runtime and caches them in memory. This keeps SQL out of Python source files and makes queries easy to find, review, and test independently.

### File layout

SQL files live under `app/queries/` and are organised into sub-packages:

```
src/app/queries/
├── __init__.py
└── customers/
    ├── list_customers.sql
    ├── list_customers_by_state.sql
    └── list_states.sql
```

### Loading a query

```python
from app.core.query_registry import QueryRegistry

registry = QueryRegistry(package="app.queries")
q = registry.get("customers/list_customers")

print(q.name)   # "customers/list_customers"
print(q.sql)    # "SELECT customerid, first_name …\n"
```

The registry is lazy — a file is read from disk only on first access, then cached for the lifetime of the instance.

### Passing a query to `DatabricksClient`

```python
q = registry.get("customers/list_customers_by_state")
df = db.query_polars(q.sql, params={"states": ["CA", "NY"], "limit": 200})
```

### Wiring it up in Streamlit

```python
@st.cache_resource
def get_queries() -> QueryRegistry:
    return QueryRegistry(package="app.queries")
```

### Why GPL-3.0?

We believe in open source software and want to ensure that improvements to this library remain open and available to everyone. The GPL-3.0 license guarantees that all derivatives and modifications stay free and open source.

---

**Made with ❤️ by the contributors**
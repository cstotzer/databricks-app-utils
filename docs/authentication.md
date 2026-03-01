# Authentication — Technical Overview

This document describes the three authentication methods supported by `databricks-app-utils` (`src/databricks_app_utils/`), how each is configured, and the detailed inner workings of the U2M implementation.

---

## Architecture overview

Authentication is split across three files:

| File | Responsibility |
|---|---|
| `settings.py` | Reads configuration from environment / `.env` via Pydantic |
| `auth.py` | Translates settings into a `DatabricksAuth` value object |
| `databricks_client.py` | Consumes `DatabricksAuth` when opening each SQL connection |

`build_auth(settings)` is called exactly **once** per server process, inside `get_db()` which is decorated with `@st.cache_resource`. The resulting `DatabricksClient` (and the `DatabricksAuth` embedded inside it) is therefore a **process-level singleton** — it survives page refreshes and is shared across all Streamlit user sessions on the same server process.

```
AppSettings  ──►  build_auth()  ──►  DatabricksAuth
                                           │
                                           ▼
                               DatabricksClient  (cached by @st.cache_resource)
                                           │
                               ┌───────────┘ one new sql.connect() per query
                               ▼
                        databricks-sql-connector
```

---

## Method 1 — PAT (Personal Access Token)

**Environment variable:** `DATABRICKS_AUTH_METHOD=pat`  
**Required:** `DATABRICKS_PAT=<token>`

### How it works

A static token is read from settings at startup, stored in `DatabricksAuth.access_token`, and passed as `access_token=` to every `sql.connect()` call.

```
build_auth()
  └─► DatabricksAuth(method=PAT, access_token="dapi…")

_build_conn_kwargs()
  └─► {"access_token": "dapi…", …}
```

### Trade-offs

- Simplest to set up; suitable for CI/CD and service accounts.
- Token never refreshes automatically; rotating it requires a restart.
- Long-lived tokens are a security risk if leaked.

---

## Method 2 — OBO (On-Behalf-Of / Databricks Apps)

**Environment variable:** `DATABRICKS_AUTH_METHOD=obo`  
**Required:** deployed on Databricks Apps with OBO forwarding enabled.

### How it works

When a Databricks App serves a request, the platform injects the calling user's OAuth access token into the HTTP header `X-Forwarded-Access-Token`. The app reads this header **on every query** via `st.context.headers`.

```
build_auth()
  └─► DatabricksAuth(method=OBO, token_provider=None)   ← no provider yet

get_db()  (@st.cache_resource)
  └─► injects token_provider=_obo_token_provider         ← adds the callable

_obo_token_provider()                                     ← called per query
  └─► return st.context.headers["X-Forwarded-Access-Token"]

_build_conn_kwargs()
  └─► {"access_token": token_provider(), …}
```

The `token_provider` is a callable rather than a pre-read value because the header is request-scoped — it changes per user session and is only accessible during an active Streamlit render cycle.

### Trade-offs

- Zero credential management; tokens are issued and refreshed by the platform.
- Each query runs as the actual end-user (fine-grained access control).
- Only works when deployed on Databricks Apps; not usable for local development.

---

## Method 3 — U2M (User-to-Machine OAuth)

**Environment variable:** `DATABRICKS_AUTH_METHOD=u2m`

This is the recommended method for **local development**. It opens a browser-based OAuth login on the first query, then reuses the token for all subsequent queries within the same server process (no re-prompt on page refresh).

### Inner workings

#### Step 1 — `build_auth()` creates an `OAuthPersistenceCache`

`OAuthPersistenceCache` (from `databricks.sql.experimental.oauth_persistence`) is an in-memory dict that maps hostname → `OAuthToken(access_token, refresh_token)`. A single instance is created at startup and kept alive for the lifetime of the process inside the `@st.cache_resource`-cached `DatabricksClient`.

```python
# auth.py
return DatabricksAuth(
    method=AuthMethod.U2M,
    oauth_persistence=OAuthPersistenceCache(),   # ← one instance, lives forever
)
```

#### Step 2 — `_build_conn_kwargs()` configures each connection

Every query opens a short-lived `sql.connect()`. For U2M the connection is configured with:

```python
conn_kwargs["auth_type"] = "databricks-oauth"
conn_kwargs["experimental_oauth_persistence"] = self._a.oauth_persistence
```

#### Step 3 — The connector creates a `DatabricksOAuthProvider`

Internally, `databricks-sql-connector` instantiates a `DatabricksOAuthProvider` for each connection. Its `_initial_get_token()` method follows this logic:

```
_initial_get_token()
  ├─► oauth_persistence.read(hostname)
  │     ├─► token found and not expired  ──► use it directly  (no browser)
  │     ├─► token found but expired      ──► refresh via refresh_token
  │     │                                    oauth_persistence.persist(…)
  │     └─► no token                     ──► open browser OAuth flow
  │                                          oauth_persistence.persist(…)
  └─► access_token + refresh_token now held in memory
```

#### Step 4 — Token lifetime across connections

Because the **same `OAuthPersistenceCache` instance** is shared by every `sql.connect()` call (via the cached `DatabricksClient`), the token written after the first browser login is available to all subsequent connections:

```
Connection 1 (first query)
  └─► OAuthPersistenceCache.read()  →  empty  →  browser opens
  └─► OAuthPersistenceCache.persist(hostname, OAuthToken(access, refresh))

Connection 2 (same or later query / page refresh)
  └─► OAuthPersistenceCache.read()  →  OAuthToken found  →  no browser
  └─► (refresh silently if access_token expired, using refresh_token)
```

#### Sequence diagram

```
User (browser)          Streamlit server             Databricks
      │                        │                          │
      │── page load ──────────►│                          │
      │                        │ sql.connect() [conn 1]   │
      │                        │──────────────────────────►
      │                        │   OAuthPersistenceCache  │
      │                        │   is empty               │
      │◄── redirect to login ──│◄─────────────────────────│
      │── authorise ──────────►│                          │
      │                        │◄── access + refresh ─────│
      │                        │   persist to cache        │
      │◄── page rendered ──────│                          │
      │                        │                          │
      │── page refresh ───────►│                          │
      │                        │ sql.connect() [conn 2]   │
      │                        │   OAuthPersistenceCache  │
      │                        │   has token  ────────────►
      │◄── page rendered ──────│◄── query result ─────────│
```

### Benefits

- **Zero secrets management** — no tokens, passwords, or credentials of any kind need to be stored or distributed. The browser OAuth flow issues short-lived tokens on demand; nothing sensitive ever touches the filesystem or an environment variable.
- **Works on locked-down machines** — because no CLI tooling is required, apps can be developed and run on a Mobi-Laptop or any corporate device where installing and configuring the `databricks-cli` is restricted or simply impractical. The only requirement is a browser.
- **No `databricks-cli` dependency** — the previous SDK-based approach relied on the `databricks-cli` having been run (`databricks auth login`) to seed `~/.databrickscfg` or `~/.databricks/token-cache.json` before the app could start. The connector-native approach removes this prerequisite entirely.
- **Identity-aware access** — queries run under the developer's own Databricks identity, so Unity Catalog permissions, row-level security, and audit logs all reflect the actual user rather than a shared service-account token.
- **Automatic token refresh** — the refresh token is held in `OAuthPersistenceCache` for the lifetime of the server process. The connector silently refreshes the access token when it expires; the developer never needs to re-authenticate unless the process restarts.

#### Why not use the Databricks SDK `Config`?

The original implementation used `databricks.sdk.config.Config(host=…)` as the credential source. As of SDK v0.94, `Config.__init__` eagerly calls `init_auth()`, which tries to resolve credentials from the environment (env vars, `~/.databrickscfg`, managed identity, etc.) and raises `ValueError` if none are found. Because the SDK's default credential chain does not include interactive browser login, this fails immediately when no pre-configured credentials exist — before the app even renders.

The connector-native approach avoids this entirely: no SDK `Config` is created at startup. The browser flow is triggered lazily by the connector only when the first actual SQL connection is opened.

---

## Configuration reference

| Variable | Default | Description |
|---|---|---|
| `DATABRICKS_SERVER_HOSTNAME` | — | `adb-xxx.azuredatabricks.net` (no `https://`) |
| `DATABRICKS_HTTP_PATH` | — | `/sql/1.0/warehouses/…` |
| `DATABRICKS_AUTH_METHOD` | `obo` | `pat` \| `u2m` \| `obo` |
| `DATABRICKS_PAT` | `None` | Required when `auth_method=pat` |

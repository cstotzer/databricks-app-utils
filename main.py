import streamlit as st
from databricks.sdk import (
    WorkspaceClient,  # noqa: PLC0415 — optional dependency used in workspace_client.py
)

from databricks_app_utils.auth import AuthMethod, DatabricksAuth, build_auth
from databricks_app_utils.query_registry import QueryRegistry
from databricks_app_utils.settings import AppSettings
from databricks_app_utils.sql_client import SQLClient
from databricks_app_utils.workspace_client import build_workspace_client


@st.cache_resource
def get_settings() -> AppSettings:
    """Get application settings.

    Returns:
    --------
    AppSettings
        The application configuration settings.
    """
    return AppSettings()


def _obo_token_provider() -> str:
    # Works only when deployed on Databricks Apps with OBO enabled. Because the
    # user token is only available at runtime from request headers, we inject a
    # token provider function that fetches the token on demand from Streamlit's
    # request context.
    headers = st.context.headers
    return headers["X-Forwarded-Access-Token"]


@st.cache_resource
def get_sql_client() -> SQLClient:
    """Get the Databricks client.

    We cache the client as a Streamlit resource to reuse across interactions.

    Returns:
    --------
    SQLClient
        The Databricks client configured with appropriate authentication.
    """
    settings = get_settings()
    auth = build_auth(settings)

    # Inject OBO token provider at runtime if configured
    if auth.method == AuthMethod.OBO:
        auth = DatabricksAuth(
            method=auth.method, token_provider=_obo_token_provider
        )

    return SQLClient(settings=settings, auth=auth)


@st.cache_resource()
def get_workspace_client() -> WorkspaceClient:
    """Get the Databricks Workspace client.

    We cache the client as a Streamlit resource to reuse across interactions.

    Note: This requires the optional ``databricks-sdk`` dependency. If you
    haven't installed it yet, run:

        pip install 'databricks-app-utils[sdk]'

    The WorkspaceClient is only used in this skeleton for demonstration
    purposes (to fetch workspace info and run a secrets API example). In a
    production app, you might choose to use it more extensively for things like
    dynamic job execution, workspace metadata introspection, or secrets
    management. The authentication method is the same as for SQLClient, so if
    you're using OBO, the WorkspaceClient will also have the same token
    provider injected at runtime.

    Returns:
    --------
    WorkspaceClient
        The Databricks Workspace client configured with appropriate authentication.
    """
    settings = get_settings()
    auth = build_auth(settings)

    # Inject OBO token provider at runtime if configured
    if auth.method == AuthMethod.OBO:
        auth = DatabricksAuth(
            method=auth.method, token_provider=_obo_token_provider
        )

    return build_workspace_client(settings, auth)


def connection_self_test() -> None:
    """Run a connection self-test to verify Databricks connectivity.

    Displays the authentication method and provides a button to run
    a test query that validates the connection to Databricks.
    """
    settings = get_settings()
    db = get_sql_client()

    st.subheader("Connection self-test")
    st.caption(f"Auth method: `{settings.databricks_auth_method}`")

    # Run on demand (keeps startup fast and avoids unwanted OAuth prompts)
    if st.button("Run connection test", type="primary"):
        try:
            # Small introspection query
            res = db.query_polars(
                """
                SELECT
                  current_user()  AS current_user,
                  current_catalog() AS current_catalog,
                  current_schema()  AS current_schema
                """
            )
            st.success("Connection OK")
            st.dataframe(res.to_pandas())
        except Exception as e:  # noqa: BLE001 — catch-all for UI error display
            st.error("Connection test failed")
            st.exception(e)


st.title("Databricks + Polars Skeleton")

connection_self_test()

st.divider()


auth = build_auth(get_settings())

w = get_workspace_client()

st.subheader("Databricks Workspace Info")
try:
    ll = w.catalogs.list()

    for c in ll:
        st.write(f"Catalog: {c.name}")

except Exception as e:
    st.error("Failed to get workspace info")
    st.exception(e)

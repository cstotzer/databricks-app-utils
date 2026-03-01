import streamlit as st

from databricks_app_utils.auth import AuthMethod, DatabricksAuth, build_auth
from databricks_app_utils.databricks_client import DatabricksClient
from databricks_app_utils.query_registry import QueryRegistry
from databricks_app_utils.settings import AppSettings
from databricks_app_utils.workspace_client import WorkspaceClient


@st.cache_resource
def get_settings() -> AppSettings:
    """Get application settings.

    Returns:
    --------
    AppSettings
        The application configuration settings.
    """
    return AppSettings()


@st.cache_resource
def get_queries() -> QueryRegistry:
    """Get the query registry.

    Returns:
    --------
    QueryRegistry
        The query registry containing available queries.
    """
    return QueryRegistry(package="app.queries")


def _obo_token_provider() -> str:
    # Works only when deployed on Databricks Apps with OBO enabled.
    headers = st.context.headers
    return headers["X-Forwarded-Access-Token"]


@st.cache_resource
def get_db() -> DatabricksClient:
    """Get the Databricks client.

    Returns:
    --------
    DatabricksClient
        The Databricks client configured with appropriate authentication.
    """
    settings = get_settings()
    auth = build_auth(settings)

    # Inject OBO token provider at runtime if configured
    if auth.method == AuthMethod.OBO:
        auth = DatabricksAuth(
            method=auth.method, token_provider=_obo_token_provider
        )

    return DatabricksClient(settings=settings, auth=auth)


def connection_self_test() -> None:
    """Run a connection self-test to verify Databricks connectivity.

    Displays the authentication method and provides a button to run
    a test query that validates the connection to Databricks.
    """
    settings = get_settings()
    db = get_db()

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

wc = WorkspaceClient(settings=get_settings(), auth=auth)
st.subheader("Databricks Workspace Info")
try:
    w = wc.client

    ll = w.catalogs.list()

    for c in ll:
        st.write(f"Catalog: {c.name}")

except Exception as e:
    st.error("Failed to get workspace info")
    st.exception(e)

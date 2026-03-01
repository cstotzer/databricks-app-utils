from enum import StrEnum

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthMethod(StrEnum):
    """Authentication method for Databricks connection.

    Attributes:
    -----------
    PAT : str
        Personal Access Token authentication.
    U2M : str
        Databricks OAuth user-to-machine authentication (in-memory token cache).
    U2M_PERSISTENT : str
        Databricks OAuth user-to-machine via the Databricks SDK, with disk-based
        token persistence. Tokens survive process restarts and are shared between
        SQLClient and WorkspaceClient. Requires ``databricks-sdk``.
    OBO : str
        Databricks Apps on-behalf-of-user authentication.
    """

    PAT = "pat"
    U2M = "u2m"  # Databricks OAuth user-to-machine (in-memory)
    U2M_PERSISTENT = "u2m_persistent"  # SDK-backed, disk-persisted token cache
    OBO = "obo"  # Databricks Apps on-behalf-of-user


class AppSettings(BaseSettings):
    """Application settings for Databricks SQL Warehouse connection.

    Attributes:
    -----------
    databricks_server_hostname : str
        The Databricks server hostname (adb-...databricks.net without https://).
    databricks_http_path : str
        The HTTP path to the Databricks SQL Warehouse (/sql/1.0/warehouses/...).
    databricks_default_catalog : str | None
        Default catalog to use for queries.
    databricks_default_schema : str | None
        Default schema to use for queries.
    databricks_auth_method : AuthMethod
        Authentication method (PAT, U2M, or OBO).
    databricks_pat : SecretStr | None
        Personal access token (required for PAT authentication).
    databricks_connect_timeout_s : int
        Connection timeout in seconds.
    databricks_retry_attempts : int
        Number of retry attempts for failed connections.
    databricks_retry_backoff_s : float
        Backoff time in seconds between retry attempts.
    query_tag : str | None
        Optional query tag embedded as a SQL comment.
    """

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Databricks SQL Warehouse
    databricks_server_hostname: str = Field(
        ..., description="adb-...databricks.net (without https://)"
    )
    databricks_http_path: str = Field(
        ..., description="/sql/1.0/warehouses/..."
    )

    # Defaults applied via USE statements by the client
    databricks_default_catalog: str | None = None
    databricks_default_schema: str | None = None

    # Authentication
    databricks_auth_method: AuthMethod = AuthMethod.OBO
    databricks_pat: SecretStr | None = None  # required for PAT

    # Timeouts / retry
    databricks_connect_timeout_s: int = 30
    databricks_retry_attempts: int = 1
    databricks_retry_backoff_s: float = 0.5

    # Optional query tag (embedded as SQL comment)
    query_tag: str | None = "streamlit-app"

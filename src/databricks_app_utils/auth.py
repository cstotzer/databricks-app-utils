from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from databricks.sql.experimental.oauth_persistence import OAuthPersistenceCache

from .settings import AppSettings, AuthMethod

TokenProvider = Callable[[], str]
CredentialsProvider = Callable[
    [], Callable[[], dict[str, str]]
]  # called once by connector to get a HeaderFactory


@dataclass(frozen=True)
class DatabricksAuth:
    """Normalized authentication payload used by DatabricksClient."""

    method: AuthMethod
    access_token: str | None = None
    token_provider: TokenProvider | None = None  # used for OBO
    credentials_provider: CredentialsProvider | None = None  # custom override
    oauth_persistence: Any = None  # used for U2M (in-memory token cache)


def build_auth(settings: AppSettings) -> DatabricksAuth:
    """Build auth from settings.

    Note: For OBO, you typically inject a token_provider from the
    Streamlit layer, because the user token is only available at
    runtime from request headers. This function therefore returns an
    OBO auth object WITHOUT a token_provider.

    Returns:
        DatabricksAuth: Configured authentication object for Databricks.

    Raises:
        ValueError: If PAT auth method is selected but
            APP_DATABRICKS_PAT is not set.
        NotImplementedError: If an unsupported auth method is provided.
    """
    if settings.databricks_auth_method == AuthMethod.PAT:
        if settings.databricks_pat is None:
            raise ValueError(
                "APP_DATABRICKS_PAT must be set when"
                " APP_DATABRICKS_AUTH_METHOD=pat"
            )
        return DatabricksAuth(
            method=AuthMethod.PAT,
            access_token=settings.databricks_pat.get_secret_value(),
        )

    if settings.databricks_auth_method == AuthMethod.U2M:
        # Use the connector's native OAuth browser flow with an in-memory
        # token cache so the browser prompt only fires once per process
        # (i.e. once after a server restart, not on every page refresh).
        return DatabricksAuth(
            method=AuthMethod.U2M,
            oauth_persistence=OAuthPersistenceCache(),
        )

    if settings.databricks_auth_method == AuthMethod.OBO:
        # token_provider must be injected by the Streamlit layer (see app.py)
        return DatabricksAuth(method=AuthMethod.OBO)

    msg = f"Unsupported auth method: {settings.databricks_auth_method}"
    raise NotImplementedError(msg)

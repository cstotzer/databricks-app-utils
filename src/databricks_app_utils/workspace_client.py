from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from .settings import AppSettings, AuthMethod

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from .auth import DatabricksAuth

# HeaderFactory is the protocol expected by CredentialsStrategy.__call__
_HeaderFactory = Callable[[], dict[str, str]]


class _OboCredentialsStrategy:
    """Injects a fresh per-request token for OBO (Databricks Apps) auth."""

    def __init__(self, token_provider: Callable[[], str]) -> None:
        self._token_provider = token_provider

    def auth_type(self) -> str:
        return "obo"

    def __call__(self, config: object) -> _HeaderFactory:  # noqa: ARG002
        token_provider = self._token_provider

        def header_factory() -> dict[str, str]:
            return {"Authorization": f"Bearer {token_provider()}"}

        return header_factory


def build_workspace_client(
    settings: AppSettings, auth: DatabricksAuth
) -> WorkspaceClient:
    """Build a ``databricks.sdk.WorkspaceClient`` from settings and auth.

    Supports all four auth methods using the shared ``DatabricksAuth`` value
    object produced by ``build_auth()``.  For OBO a ``CredentialsStrategy``
    is injected so the client always fetches a fresh token from Streamlit
    request headers on every SDK call.

    Usage::

        from databricks_app_utils.auth import build_auth
        from databricks_app_utils.workspace_client import build_workspace_client

        settings = AppSettings()
        auth = build_auth(settings)
        ws = build_workspace_client(settings, auth)

        for job in ws.jobs.list():
            print(job.settings.name)

        ws.secrets.put_secret(scope="my-scope", key="my-key", string_value="…")

    Args:
        settings: Application settings containing Databricks configuration.
        auth: Authentication credentials produced by ``build_auth()``.

    Returns:
        A fully configured ``databricks.sdk.WorkspaceClient``.

    Raises:
        ImportError: If ``databricks-sdk`` is not installed.
        ValueError: If OBO auth is used without a ``token_provider``.
        NotImplementedError: If an unsupported auth method is configured.
    """
    try:
        from databricks.sdk import WorkspaceClient as SdkClient  # noqa: PLC0415
    except ImportError as e:
        raise ImportError(
            "databricks-sdk is required for build_workspace_client(); "
            "run: pip install 'databricks-app-utils[sdk]'"
        ) from e

    host = f"https://{settings.databricks_server_hostname}"

    if auth.method == AuthMethod.PAT:
        return SdkClient(host=host, token=auth.access_token)

    if auth.method in (AuthMethod.U2M, AuthMethod.U2M_PERSISTENT):
        # U2M: in-memory token cache (lost on restart).
        # U2M_PERSISTENT: disk-based TokenCache shared with SQLClient
        #   (same host + client_id hash → same cache file).
        return SdkClient(host=host, auth_type="external-browser")

    if auth.method == AuthMethod.OBO:
        if not auth.token_provider:
            raise ValueError(
                "OBO authentication requires DatabricksAuth.token_provider. "
                "Inject it from Streamlit (st.context.headers)."
            )
        return SdkClient(
            host=host,
            credentials_strategy=_OboCredentialsStrategy(auth.token_provider),
        )

    msg = f"Unsupported auth method: {auth.method}"
    raise NotImplementedError(msg)

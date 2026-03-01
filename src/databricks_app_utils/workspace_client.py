from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from .settings import AppSettings, AuthMethod

if TYPE_CHECKING:
    import databricks.sdk as sdk

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


class WorkspaceClient:
    """Databricks SDK WorkspaceClient with the same auth as DatabricksClient.

    Wraps ``databricks.sdk.WorkspaceClient`` and supports all three auth
    methods (PAT, U2M, OBO) using the shared ``DatabricksAuth`` value object.

    The underlying SDK client is created lazily on first access and cached for
    PAT and U2M. For OBO a ``CredentialsStrategy`` is used so the same cached
    client always fetches a fresh token from the request headers on every SDK
    call — consistent with how ``DatabricksClient`` handles OBO via
    ``token_provider()``.

    Usage::

        ws = WorkspaceClient(settings=settings, auth=auth)
        for job in ws.client.jobs.list():
            print(job.settings.name)
    """

    def __init__(self, settings: AppSettings, auth: DatabricksAuth) -> None:
        """Initialise with application settings and auth.

        Args:
            settings: Application settings containing Databricks configuration.
            auth: Authentication credentials and method.
        """
        self._s = settings
        self._a = auth
        self._client: sdk.WorkspaceClient | None = None

    @property
    def client(self) -> sdk.WorkspaceClient:
        """Lazily-built, cached ``databricks.sdk.WorkspaceClient``.

        Returns:
            A configured ``WorkspaceClient`` instance.
        """
        if self._client is None:
            self._client = self._build()
        return self._client

    def _build(self) -> sdk.WorkspaceClient:
        """Build a configured WorkspaceClient instance.

        Returns:
            A configured ``WorkspaceClient`` instance.

        Raises:
            ImportError: If ``databricks-sdk`` is not installed.
            ValueError: If OBO auth is used without a ``token_provider``.
            NotImplementedError: If an unsupported auth method is configured.
        """
        try:
            from databricks.sdk import (
                WorkspaceClient as SdkClient,  # noqa: PLC0415
            )
        except ImportError as e:
            raise ImportError(
                "databricks-sdk is required for WorkspaceClient; "
                "run: pip install databricks-sdk"
            ) from e

        host = f"https://{self._s.databricks_server_hostname}"

        if self._a.method == AuthMethod.PAT:
            return SdkClient(host=host, token=self._a.access_token)

        if self._a.method == AuthMethod.U2M:
            return SdkClient(host=host, auth_type="external-browser")

        if self._a.method == AuthMethod.OBO:
            if not self._a.token_provider:
                raise ValueError(
                    "OBO authentication requires DatabricksAuth."
                    "token_provider. Inject it from Streamlit "
                    "(st.context.headers)."
                )
            return SdkClient(
                host=host,
                credentials_strategy=_OboCredentialsStrategy(
                    self._a.token_provider
                ),
            )

        msg = f"Unsupported auth method: {self._a.method}"
        raise NotImplementedError(msg)

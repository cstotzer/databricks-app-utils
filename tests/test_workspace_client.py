"""Unit tests for build_workspace_client."""

from unittest.mock import MagicMock, patch

import pytest

from databricks_app_utils.auth import DatabricksAuth
from databricks_app_utils.settings import AppSettings, AuthMethod
from databricks_app_utils.workspace_client import (
    _OboCredentialsStrategy,
    build_workspace_client,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**overrides):
    base = dict(
        databricks_server_hostname="host.databricks.com",
        databricks_http_path="/sql/1.0/warehouses/abc",
        databricks_auth_method=AuthMethod.PAT,
    )
    base.update(overrides)
    return AppSettings.model_construct(**base)


def _patch_sdk():
    return patch("databricks.sdk.WorkspaceClient")


# ---------------------------------------------------------------------------
# PAT
# ---------------------------------------------------------------------------


def test_pat_builds_client_with_token():
    auth = DatabricksAuth(method=AuthMethod.PAT, access_token="my-pat")
    settings = _make_settings()
    with _patch_sdk() as mock_cls:
        build_workspace_client(settings, auth)
    mock_cls.assert_called_once_with(
        host="https://host.databricks.com", token="my-pat"
    )


# ---------------------------------------------------------------------------
# U2M
# ---------------------------------------------------------------------------


def test_u2m_builds_client_with_external_browser():
    auth = DatabricksAuth(method=AuthMethod.U2M)
    settings = _make_settings(databricks_auth_method=AuthMethod.U2M)
    with _patch_sdk() as mock_cls:
        build_workspace_client(settings, auth)
    mock_cls.assert_called_once_with(
        host="https://host.databricks.com", auth_type="external-browser"
    )


# ---------------------------------------------------------------------------
# U2M Persistent
# ---------------------------------------------------------------------------


def test_u2m_persistent_builds_client_with_external_browser():
    auth = DatabricksAuth(
        method=AuthMethod.U2M_PERSISTENT,
        credentials_provider=lambda: lambda: {},
    )
    settings = _make_settings(databricks_auth_method=AuthMethod.U2M_PERSISTENT)
    with _patch_sdk() as mock_cls:
        build_workspace_client(settings, auth)
    mock_cls.assert_called_once_with(
        host="https://host.databricks.com", auth_type="external-browser"
    )


# ---------------------------------------------------------------------------
# OBO
# ---------------------------------------------------------------------------


def test_obo_builds_client_with_credentials_strategy():
    auth = DatabricksAuth(
        method=AuthMethod.OBO, token_provider=lambda: "user-token"
    )
    settings = _make_settings(databricks_auth_method=AuthMethod.OBO)
    with _patch_sdk() as mock_cls:
        build_workspace_client(settings, auth)
    mock_cls.assert_called_once()
    call_kwargs = mock_cls.call_args.kwargs
    assert call_kwargs["host"] == "https://host.databricks.com"
    assert "credentials_strategy" in call_kwargs
    assert isinstance(call_kwargs["credentials_strategy"], _OboCredentialsStrategy)


def test_obo_raises_without_token_provider():
    auth = DatabricksAuth(method=AuthMethod.OBO)
    settings = _make_settings(databricks_auth_method=AuthMethod.OBO)
    with _patch_sdk():
        with pytest.raises(ValueError, match="token_provider"):
            build_workspace_client(settings, auth)


# ---------------------------------------------------------------------------
# _OboCredentialsStrategy
# ---------------------------------------------------------------------------


def test_obo_strategy_auth_type():
    strategy = _OboCredentialsStrategy(token_provider=lambda: "tok")
    assert strategy.auth_type() == "obo"


def test_obo_strategy_returns_bearer_header():
    strategy = _OboCredentialsStrategy(token_provider=lambda: "fresh-token")
    header_factory = strategy(config=None)
    assert header_factory() == {"Authorization": "Bearer fresh-token"}


def test_obo_strategy_calls_provider_on_each_request():
    tokens = iter(["token-1", "token-2"])
    strategy = _OboCredentialsStrategy(token_provider=lambda: next(tokens))
    header_factory = strategy(config=None)
    assert header_factory() == {"Authorization": "Bearer token-1"}
    assert header_factory() == {"Authorization": "Bearer token-2"}


# ---------------------------------------------------------------------------
# Unsupported auth method
# ---------------------------------------------------------------------------


def test_unsupported_auth_method_raises():
    # Bypass StrEnum validation to simulate an unknown value
    auth = MagicMock()
    auth.method = "unknown"
    auth.token_provider = None
    settings = _make_settings()
    with _patch_sdk():
        with pytest.raises(NotImplementedError):
            build_workspace_client(settings, auth)

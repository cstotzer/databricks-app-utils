import pytest

from databricks_app_utils.auth import DatabricksAuth, build_auth
from databricks_app_utils.settings import AppSettings, AuthMethod


def _settings(**overrides) -> AppSettings:
    base = dict(
        databricks_server_hostname="host.databricks.com",
        databricks_http_path="/sql/1.0/warehouses/abc",
    )
    base.update(overrides)
    return AppSettings.model_construct(**base)


def test_build_auth_pat():
    from pydantic import SecretStr

    settings = _settings(
        databricks_auth_method=AuthMethod.PAT,
        databricks_pat=SecretStr("mytoken"),
    )
    auth = build_auth(settings)
    assert auth.method == AuthMethod.PAT
    assert auth.access_token == "mytoken"


def test_build_auth_pat_missing_token():
    settings = _settings(
        databricks_auth_method=AuthMethod.PAT, databricks_pat=None
    )
    with pytest.raises(ValueError, match="APP_DATABRICKS_PAT"):
        build_auth(settings)


def test_build_auth_u2m():
    from unittest.mock import MagicMock, patch

    mock_config = MagicMock()
    mock_config.authenticate.return_value = lambda: {
        "Authorization": "Bearer test-token"
    }
    with patch("databricks_app_utils.auth.Config", return_value=mock_config):
        settings = _settings(databricks_auth_method=AuthMethod.U2M)
        auth = build_auth(settings)
    assert auth.method == AuthMethod.U2M
    assert auth.credentials_provider is not None
    assert auth.access_token is None
    assert auth.token_provider is None


def test_build_auth_obo():
    settings = _settings(databricks_auth_method=AuthMethod.OBO)
    auth = build_auth(settings)
    assert auth.method == AuthMethod.OBO
    assert auth.token_provider is None


def test_databricks_auth_frozen():
    auth = DatabricksAuth(method=AuthMethod.PAT, access_token="tok")
    with pytest.raises((AttributeError, TypeError)):
        auth.access_token = "other"  # type: ignore[misc]

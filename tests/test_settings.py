import pytest
from pydantic import ValidationError

from databricks_app_utils.settings import AppSettings, AuthMethod


def test_loads_from_env():
    settings = AppSettings()
    assert settings.databricks_server_hostname
    assert settings.databricks_http_path


def test_auth_method_enum():
    assert AuthMethod.PAT == "pat"
    assert AuthMethod.U2M == "u2m"
    assert AuthMethod.U2M_PERSISTENT == "u2m_persistent"
    assert AuthMethod.OBO == "obo"


def test_missing_required_fields():
    with pytest.raises(ValidationError):
        AppSettings(
            databricks_server_hostname=None,
            databricks_http_path=None,
            _env_file=None,
        )

import pytest

from databricks_app_utils.auth import build_auth
from databricks_app_utils.settings import AppSettings
from databricks_app_utils.sql_client import SQLClient


@pytest.fixture(scope="session", autouse=True)
def settings() -> AppSettings:
    return AppSettings(
        _env_file="test.env",
        _env_file_encoding="utf-8",
    )


@pytest.fixture
def sql_client(settings: AppSettings) -> SQLClient:
    auth = build_auth(settings)
    return SQLClient(settings, auth)

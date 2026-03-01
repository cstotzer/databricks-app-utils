"""Unit tests for QueryRegistry and SqlQuery."""

from unittest.mock import MagicMock, patch

import pytest

from databricks_app_utils.query_registry import QueryRegistry, SqlQuery

# ---------------------------------------------------------------------------
# SqlQuery dataclass
# ---------------------------------------------------------------------------


def test_sql_query_stores_name_and_sql():
    q = SqlQuery(name="customers/list", sql="SELECT * FROM customers\n")
    assert q.name == "customers/list"
    assert q.sql == "SELECT * FROM customers\n"


def test_sql_query_is_frozen():
    q = SqlQuery(name="q", sql="SELECT 1\n")
    with pytest.raises((AttributeError, TypeError)):
        q.name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# QueryRegistry.get — happy path
# ---------------------------------------------------------------------------


def _make_mock_file(text: str) -> MagicMock:
    mock_file = MagicMock()
    mock_file.read_text.return_value = text
    return mock_file


def _patch_resources(mock_file: MagicMock):
    mock_root = MagicMock()
    mock_root.joinpath.return_value = mock_file
    return patch("importlib.resources.files", return_value=mock_root), mock_root


def test_get_returns_sql_query():
    mock_file = _make_mock_file("SELECT * FROM customers\n")
    patcher, _ = _patch_resources(mock_file)
    with patcher:
        registry = QueryRegistry(package="app.queries")
        result = registry.get("customers/list")
    assert isinstance(result, SqlQuery)
    assert result.name == "customers/list"


def test_get_strips_whitespace_and_appends_newline():
    mock_file = _make_mock_file("  SELECT 1  \n\n")
    patcher, _ = _patch_resources(mock_file)
    with patcher:
        result = QueryRegistry(package="app.queries").get("q")
    assert result.sql == "SELECT 1\n"


def test_get_preserves_trailing_newline_if_already_present():
    mock_file = _make_mock_file("SELECT 1\n")
    patcher, _ = _patch_resources(mock_file)
    with patcher:
        result = QueryRegistry(package="app.queries").get("q")
    assert result.sql == "SELECT 1\n"


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def test_get_caches_result_on_second_call():
    mock_file = _make_mock_file("SELECT 1\n")
    patcher, _ = _patch_resources(mock_file)
    with patcher:
        registry = QueryRegistry(package="app.queries")
        result1 = registry.get("q")
        result2 = registry.get("q")
    mock_file.read_text.assert_called_once()
    assert result1 is result2


def test_different_keys_loaded_independently():
    mock_file_a = _make_mock_file("SELECT a\n")
    mock_file_b = _make_mock_file("SELECT b\n")
    mock_root = MagicMock()
    mock_root.joinpath.side_effect = lambda p: (
        mock_file_a if "query_a" in p else mock_file_b
    )
    with patch("importlib.resources.files", return_value=mock_root):
        registry = QueryRegistry(package="app.queries")
        a = registry.get("query_a")
        b = registry.get("query_b")
    assert a.sql == "SELECT a\n"
    assert b.sql == "SELECT b\n"


# ---------------------------------------------------------------------------
# Path construction
# ---------------------------------------------------------------------------


def test_get_passes_correct_package_to_resources():
    mock_file = _make_mock_file("SELECT 1\n")
    mock_root = MagicMock()
    mock_root.joinpath.return_value = mock_file
    with patch("importlib.resources.files", return_value=mock_root) as mock_files:
        QueryRegistry(package="my_app.queries").get("sub/query")
    mock_files.assert_called_once_with("my_app.queries")


def test_get_appends_sql_extension_to_path():
    mock_file = _make_mock_file("SELECT 1\n")
    mock_root = MagicMock()
    mock_root.joinpath.return_value = mock_file
    with patch("importlib.resources.files", return_value=mock_root):
        QueryRegistry(package="app.queries").get("customers/list_all")
    mock_root.joinpath.assert_called_once_with("customers/list_all.sql")

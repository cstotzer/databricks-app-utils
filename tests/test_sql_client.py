"""Unit tests for SQLClient using mocked connections."""

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from databricks_app_utils.auth import DatabricksAuth
from databricks_app_utils.sql_client import SQLClient
from databricks_app_utils.settings import AppSettings, AuthMethod

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**overrides):
    defaults = dict(
        databricks_server_hostname="host.databricks.com",
        databricks_http_path="/sql/1.0/warehouses/abc",
        databricks_auth_method=AuthMethod.PAT,
        databricks_pat=None,
        databricks_connect_timeout_s=30,
        databricks_retry_attempts=1,
        databricks_retry_backoff_s=0.5,
        databricks_default_catalog=None,
        databricks_default_schema=None,
        query_tag=None,
    )
    defaults.update(overrides)
    return AppSettings.model_construct(**defaults)


def _make_client(**settings_overrides) -> SQLClient:
    settings = _make_settings(**settings_overrides)
    auth = DatabricksAuth(method=AuthMethod.PAT, access_token="fake-token")
    return SQLClient(settings=settings, auth=auth)


def _mock_cursor(arrow_table=None, rows=None, description=None):
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    if arrow_table is not None:
        cursor.fetchall_arrow.return_value = arrow_table
    if rows is not None:
        cursor.fetchall.return_value = rows
    cursor.description = description or []
    return cursor


def _mock_conn(cursor):
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# query_polars
# ---------------------------------------------------------------------------


def test_query_polars_returns_dataframe():
    pl = pytest.importorskip("polars")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([1], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_polars("SELECT 1 AS n")
    assert isinstance(result, pl.DataFrame)
    assert result["n"][0] == 1


def test_query_polars_with_named_params():
    pytest.importorskip("polars")
    cursor = _mock_cursor(
        arrow_table=pa.table({"v": pa.array([42], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_polars(
            "SELECT :val AS v", params={"val": 42}
        )
    cursor.execute.assert_called_once_with("SELECT ? AS v", [42])
    assert result["v"][0] == 42


def test_query_polars_multiple_rows():
    pytest.importorskip("polars")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_polars("SELECT n FROM t")
    assert result.shape[0] == 5
    assert list(result["n"]) == [1, 2, 3, 4, 5]


def test_query_polars_in_list():
    pytest.importorskip("polars")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([2, 4], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_polars(
            "SELECT n FROM t WHERE n IN :vals", params={"vals": [2, 4]}
        )
    cursor.execute.assert_called_once_with(
        "SELECT n FROM t WHERE n IN (?, ?)", [2, 4]
    )
    assert sorted(result["n"].to_list()) == [2, 4]


def test_query_polars_in_list_single_value():
    pytest.importorskip("polars")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([3], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        _make_client().query_polars(
            "SELECT n FROM t WHERE n IN :vals", params={"vals": [3]}
        )
    cursor.execute.assert_called_once_with(
        "SELECT n FROM t WHERE n IN (?)", [3]
    )


def test_query_polars_in_list_mixed_params():
    pytest.importorskip("polars")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([6, 8], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_polars(
            "SELECT n FROM t WHERE n IN :vals AND n > :floor",
            params={"vals": [2, 4, 6, 8], "floor": 4},
        )
    cursor.execute.assert_called_once_with(
        "SELECT n FROM t WHERE n IN (?, ?, ?, ?) AND n > ?", [2, 4, 6, 8, 4]
    )
    assert sorted(result["n"].to_list()) == [6, 8]


# ---------------------------------------------------------------------------
# query_pandas
# ---------------------------------------------------------------------------


def test_query_pandas_returns_dataframe():
    pd = pytest.importorskip("pandas")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([1], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_pandas("SELECT 1 AS n")
    assert isinstance(result, pd.DataFrame)
    assert result["n"].iloc[0] == 1


def test_query_pandas_with_named_params():
    pytest.importorskip("pandas")
    cursor = _mock_cursor(
        arrow_table=pa.table({"v": pa.array([99], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_pandas(
            "SELECT :val AS v", params={"val": 99}
        )
    cursor.execute.assert_called_once_with("SELECT ? AS v", [99])
    assert result["v"].iloc[0] == 99


def test_query_pandas_multiple_rows():
    pytest.importorskip("pandas")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([10, 20, 30], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_pandas("SELECT n FROM t")
    assert result.shape[0] == 3
    assert list(result["n"]) == [10, 20, 30]


def test_query_pandas_in_list():
    pytest.importorskip("pandas")
    cursor = _mock_cursor(
        arrow_table=pa.table({"n": pa.array([1, 5], type=pa.int64())})
    )
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query_pandas(
            "SELECT n FROM t WHERE n IN :vals", params={"vals": [1, 5]}
        )
    cursor.execute.assert_called_once_with(
        "SELECT n FROM t WHERE n IN (?, ?)", [1, 5]
    )
    assert sorted(result["n"].tolist()) == [1, 5]


# ---------------------------------------------------------------------------
# query (plain Python dicts)
# ---------------------------------------------------------------------------

_DESC_A_B = [
    ("a", None, None, None, None, None, None),
    ("b", None, None, None, None, None, None),
]
_DESC_N = [("n", None, None, None, None, None, None)]
_DESC_V = [("v", None, None, None, None, None, None)]


def test_query_returns_list_of_dicts():
    cursor = _mock_cursor(rows=[(1, "hello")], description=_DESC_A_B)
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query("SELECT 1 AS a, 'hello' AS b")
    assert result == [{"a": 1, "b": "hello"}]


def test_query_multiple_rows():
    cursor = _mock_cursor(rows=[(1,), (2,), (3,)], description=_DESC_N)
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query("SELECT n FROM t")
    assert [r["n"] for r in result] == [1, 2, 3]


def test_query_with_named_params():
    cursor = _mock_cursor(rows=[(99,)], description=_DESC_V)
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query("SELECT :val AS v", params={"val": 99})
    cursor.execute.assert_called_once_with("SELECT ? AS v", [99])
    assert result[0]["v"] == 99


def test_query_with_in_list():
    cursor = _mock_cursor(rows=[(2,), (4,)], description=_DESC_N)
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query(
            "SELECT n FROM t WHERE n IN :vals", params={"vals": [2, 4]}
        )
    assert sorted(r["n"] for r in result) == [2, 4]


def test_query_empty_result():
    cursor = _mock_cursor(rows=[], description=_DESC_N)
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        result = _make_client().query("SELECT n FROM t WHERE 1=0")
    assert result == []


# ---------------------------------------------------------------------------
# USE CATALOG / USE SCHEMA
# ---------------------------------------------------------------------------


def test_use_catalog_applied():
    cursor = _mock_cursor(arrow_table=pa.table({"x": pa.array([1])}))
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        _make_client(databricks_default_catalog="my_catalog").query_polars(
            "SELECT 1 AS x"
        )
    calls = [c.args[0] for c in cursor.execute.call_args_list]
    assert any("USE CATALOG my_catalog" in c for c in calls)


def test_use_schema_applied():
    cursor = _mock_cursor(arrow_table=pa.table({"x": pa.array([1])}))
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        _make_client(databricks_default_schema="my_schema").query_polars(
            "SELECT 1 AS x"
        )
    calls = [c.args[0] for c in cursor.execute.call_args_list]
    assert any("USE SCHEMA my_schema" in c for c in calls)


def test_no_use_statements_when_no_defaults():
    cursor = _mock_cursor(arrow_table=pa.table({"x": pa.array([1])}))
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        _make_client().query_polars("SELECT 1 AS x")
    calls = [c.args[0] for c in cursor.execute.call_args_list]
    assert all("USE" not in c for c in calls)


# ---------------------------------------------------------------------------
# query_tag
# ---------------------------------------------------------------------------


def test_query_tag_prepended():
    cursor = _mock_cursor(arrow_table=pa.table({"x": pa.array([1])}))
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        _make_client(query_tag="my-tag").query_polars("SELECT 1 AS x")
    executed_sql = cursor.execute.call_args[0][0]
    assert executed_sql.startswith("/* my-tag */")


def test_no_query_tag_when_none():
    cursor = _mock_cursor(arrow_table=pa.table({"x": pa.array([1])}))
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        _make_client(query_tag=None).query_polars("SELECT 1 AS x")
    executed_sql = cursor.execute.call_args[0][0]
    assert not executed_sql.startswith("/*")


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


def test_retries_on_transient_error():
    pytest.importorskip("polars")
    good_cursor = _mock_cursor(arrow_table=pa.table({"n": pa.array([1])}))
    good_conn = _mock_conn(good_cursor)
    call_count = 0

    def connect_side_effect(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient error")
        return good_conn

    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        side_effect=connect_side_effect,
    ):
        with patch("time.sleep"):
            result = _make_client(databricks_retry_attempts=1).query_polars(
                "SELECT 1 AS n"
            )

    assert call_count == 2
    assert result["n"][0] == 1


def test_raises_after_max_retries():
    def always_fail(**kwargs):
        raise RuntimeError("always fails")

    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        side_effect=always_fail,
    ):
        with patch("time.sleep"):
            with pytest.raises(RuntimeError, match="always fails"):
                _make_client(databricks_retry_attempts=1).query_polars(
                    "SELECT 1"
                )


# ---------------------------------------------------------------------------
# OBO auth — missing token_provider raises early
# ---------------------------------------------------------------------------


def test_obo_raises_without_token_provider():
    settings = _make_settings(databricks_auth_method=AuthMethod.OBO)
    auth = DatabricksAuth(method=AuthMethod.OBO)
    client = SQLClient(settings=settings, auth=auth)

    cursor = _mock_cursor(arrow_table=pa.table({"x": pa.array([1])}))
    with patch(
        "databricks_app_utils.sql_client.sql.connect",
        return_value=_mock_conn(cursor),
    ):
        with pytest.raises(ValueError, match="token_provider"):
            client.query_polars("SELECT 1")

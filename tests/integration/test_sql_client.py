from databricks_app_utils.sql_client import SQLClient


def test_query(sql_client: SQLClient):
    result = sql_client.query("SELECT 1 AS num")
    assert result is not None
    assert len(result) == 1
    assert result[0]["num"] == 1


def test_query_polars(sql_client: SQLClient):
    result = sql_client.query_polars("SELECT 1 AS num")
    assert result is not None
    assert len(result) == 1
    assert result.shape == (1, 1)


def test_query_pandas(sql_client: SQLClient):
    result = sql_client.query_pandas("SELECT 1 AS num")
    assert result is not None
    assert len(result) == 1
    assert result.shape == (1, 1)

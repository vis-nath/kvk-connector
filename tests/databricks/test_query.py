import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from kavak_connector.exceptions import QueryError, AuthRequiredError


def _make_mock_conn(columns, rows):
    mock_cursor = MagicMock()
    mock_cursor.description = [(col, None, None, None, None, None, None) for col in columns]
    mock_cursor.fetchall.return_value = rows
    mock_cursor.__enter__ = lambda s: s
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


def test_query_returns_dataframe():
    mock_conn = _make_mock_conn(["id", "name"], [(1, "Kavak"), (2, "EaaS")])
    with patch("kavak_connector.databricks.query.sql") as mock_sql, \
         patch("kavak_connector.databricks.query.get_host", return_value="dbc-test.cloud.databricks.com"), \
         patch("kavak_connector.databricks.query.get_http_path", return_value="/sql/1.0/warehouses/abc"), \
         patch("kavak_connector.databricks.query.get_auth_method", return_value="token"), \
         patch("kavak_connector.databricks.query.get_token", return_value="dapi_test"):
        mock_sql.connect.return_value = mock_conn
        from kavak_connector.databricks.query import query
        result = query("SELECT id, name FROM t")
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["id", "name"]
    assert len(result) == 2


def test_query_uses_token_when_auth_method_is_token():
    mock_conn = _make_mock_conn(["x"], [(1,)])
    with patch("kavak_connector.databricks.query.sql") as mock_sql, \
         patch("kavak_connector.databricks.query.get_host", return_value="h"), \
         patch("kavak_connector.databricks.query.get_http_path", return_value="p"), \
         patch("kavak_connector.databricks.query.get_auth_method", return_value="token"), \
         patch("kavak_connector.databricks.query.get_token", return_value="dapi_test"):
        mock_sql.connect.return_value = mock_conn
        from kavak_connector.databricks.query import query
        query("SELECT 1 AS x")
    call_kwargs = mock_sql.connect.call_args[1]
    assert call_kwargs["access_token"] == "dapi_test"
    assert "auth_type" not in call_kwargs


def test_query_uses_oauth_when_auth_method_is_oauth():
    mock_conn = _make_mock_conn(["x"], [(1,)])
    with patch("kavak_connector.databricks.query.sql") as mock_sql, \
         patch("kavak_connector.databricks.query.get_host", return_value="h"), \
         patch("kavak_connector.databricks.query.get_http_path", return_value="p"), \
         patch("kavak_connector.databricks.query.get_auth_method", return_value="oauth"), \
         patch("kavak_connector.databricks.query.get_token", return_value=None):
        mock_sql.connect.return_value = mock_conn
        from kavak_connector.databricks.query import query
        query("SELECT 1 AS x")
    call_kwargs = mock_sql.connect.call_args[1]
    assert call_kwargs["auth_type"] == "external-browser"
    assert "access_token" not in call_kwargs


def test_query_raises_auth_error_on_401():
    with patch("kavak_connector.databricks.query.sql") as mock_sql, \
         patch("kavak_connector.databricks.query.get_host", return_value="h"), \
         patch("kavak_connector.databricks.query.get_http_path", return_value="p"), \
         patch("kavak_connector.databricks.query.get_auth_method", return_value="token"), \
         patch("kavak_connector.databricks.query.get_token", return_value="t"):
        mock_sql.connect.side_effect = Exception("401 Unauthorized: token expired")
        from kavak_connector.databricks.query import query
        with pytest.raises(AuthRequiredError):
            query("SELECT 1")


def test_query_raises_query_error_on_syntax():
    with patch("kavak_connector.databricks.query.sql") as mock_sql, \
         patch("kavak_connector.databricks.query.get_host", return_value="h"), \
         patch("kavak_connector.databricks.query.get_http_path", return_value="p"), \
         patch("kavak_connector.databricks.query.get_auth_method", return_value="token"), \
         patch("kavak_connector.databricks.query.get_token", return_value="t"):
        mock_sql.connect.side_effect = Exception("SYNTAX_ERROR: unexpected token")
        from kavak_connector.databricks.query import query
        with pytest.raises(QueryError):
            query("SELECT * FORM t")

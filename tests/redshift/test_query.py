import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from kavak_connector.exceptions import QueryError, AuthRequiredError

_CREDS = {"host": "h", "database": "d", "port": 5439, "user": "u", "password": "p"}


def _make_mock_conn(columns, rows):
    mock_cursor = MagicMock()
    mock_cursor.description = [(col, None, None, None, None, None, None) for col in columns]
    mock_cursor.fetchall.return_value = rows
    mock_cursor.__enter__ = lambda s: s
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


def test_query_returns_dataframe():
    mock_conn = _make_mock_conn(["vin", "price"], [("ABC123", 150000), ("XYZ999", 200000)])
    with patch("kavak_connector.redshift.query.redshift_connector") as mock_rc, \
         patch("kavak_connector.redshift.query.get_credentials", return_value=_CREDS):
        mock_rc.connect.return_value = mock_conn
        from kavak_connector.redshift.query import query
        result = query("SELECT vin, price FROM inventory")
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["vin", "price"]
    assert len(result) == 2


def test_query_raises_auth_error_on_password_failure():
    with patch("kavak_connector.redshift.query.redshift_connector") as mock_rc, \
         patch("kavak_connector.redshift.query.get_credentials", return_value=_CREDS):
        mock_rc.connect.side_effect = Exception("password authentication failed for user")
        from kavak_connector.redshift.query import query
        with pytest.raises(AuthRequiredError):
            query("SELECT 1")


def test_query_raises_query_error_on_syntax():
    with patch("kavak_connector.redshift.query.redshift_connector") as mock_rc, \
         patch("kavak_connector.redshift.query.get_credentials", return_value=_CREDS):
        mock_rc.connect.side_effect = Exception("syntax error at or near FROM")
        from kavak_connector.redshift.query import query
        with pytest.raises(QueryError):
            query("SELECT * FORM t")


def test_query_closes_connection_on_success():
    mock_conn = _make_mock_conn(["x"], [(1,)])
    with patch("kavak_connector.redshift.query.redshift_connector") as mock_rc, \
         patch("kavak_connector.redshift.query.get_credentials", return_value=_CREDS):
        mock_rc.connect.return_value = mock_conn
        from kavak_connector.redshift.query import query
        query("SELECT 1 AS x")
    mock_conn.close.assert_called_once()

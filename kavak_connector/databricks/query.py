import pandas as pd
import databricks.sql as sql
from .auth import get_host, get_http_path, get_auth_method, get_token
from ..exceptions import AuthRequiredError, QueryError

_AUTH_KEYWORDS = ("401", "403", "unauthorized", "token expired", "permission_denied", "expired")


def query(sql_str: str) -> pd.DataFrame:
    host = get_host()
    http_path = get_http_path()
    auth_method = get_auth_method()

    connect_kwargs: dict = dict(server_hostname=host, http_path=http_path)
    if auth_method == "token":
        connect_kwargs["access_token"] = get_token()
    else:
        connect_kwargs["auth_type"] = "external-browser"

    try:
        with sql.connect(**connect_kwargs) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_str)
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)
    except (AuthRequiredError, QueryError):
        raise
    except Exception as e:
        _handle_error(e)


def _handle_error(e: Exception) -> None:
    msg = str(e).lower()
    if any(k in msg for k in _AUTH_KEYWORDS):
        raise AuthRequiredError(str(e)) from e
    raise QueryError(str(e)) from e

import pandas as pd
import redshift_connector
from .auth import get_credentials
from ..exceptions import AuthRequiredError, QueryError

_AUTH_KEYWORDS = ("password authentication failed", "permission denied", "access denied", "ssl")


def query(sql_str: str) -> pd.DataFrame:
    creds = get_credentials()
    conn = None
    try:
        conn = redshift_connector.connect(
            host=creds["host"],
            database=creds["database"],
            port=creds["port"],
            user=creds["user"],
            password=creds["password"],
        )
        with conn.cursor() as cursor:
            cursor.execute(sql_str)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
    except (AuthRequiredError, QueryError):
        raise
    except Exception as e:
        _handle_error(e)
    finally:
        if conn is not None:
            conn.close()


def _handle_error(e: Exception) -> None:
    msg = str(e).lower()
    if any(k in msg for k in _AUTH_KEYWORDS):
        raise AuthRequiredError(str(e)) from e
    raise QueryError(str(e)) from e

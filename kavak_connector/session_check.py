from pathlib import Path
from .databricks.auth import get_auth_method, get_token

_DATABRICKS_CONFIG = Path.home() / ".kavak_connector" / "databricks.json"
_REDSHIFT_ENV = Path.home() / ".kavak_connector" / "redshift.env"
_REDSHIFT_REQUIRED = ("REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD")


def check_databricks() -> bool:
    """True if Databricks is configured and credentials are present."""
    if not _DATABRICKS_CONFIG.exists():
        return False
    if get_auth_method() == "token":
        return bool(get_token())
    return True  # oauth: browser opens automatically on first query


def check_redshift() -> bool:
    """True if Redshift .env exists and all required fields are set."""
    if not _REDSHIFT_ENV.exists():
        return False
    content = _REDSHIFT_ENV.read_text()
    return all(k in content for k in _REDSHIFT_REQUIRED)


def check_session() -> bool:
    """True if at least Databricks is configured and ready."""
    return check_databricks()

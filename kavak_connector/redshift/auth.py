import os
from pathlib import Path
from dotenv import load_dotenv
from ..exceptions import ConfigNotFoundError

_ENV_FILE = Path.home() / ".kavak_connector" / "redshift.env"
_REQUIRED = ("REDSHIFT_HOST", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD")


def get_credentials() -> dict:
    if not _ENV_FILE.exists():
        raise ConfigNotFoundError(
            f"Redshift config not found: {_ENV_FILE}\n"
            "Run the kavak-install skill to set it up."
        )
    load_dotenv(_ENV_FILE, override=False)
    missing = [k for k in _REQUIRED if not os.environ.get(k)]
    if missing:
        raise ConfigNotFoundError(f"Missing Redshift env vars: {missing}")
    return {
        "host": os.environ["REDSHIFT_HOST"],
        "database": os.environ["REDSHIFT_DATABASE"],
        "port": int(os.environ.get("REDSHIFT_PORT", "5439")),
        "user": os.environ["REDSHIFT_USER"],
        "password": os.environ["REDSHIFT_PASSWORD"],
    }

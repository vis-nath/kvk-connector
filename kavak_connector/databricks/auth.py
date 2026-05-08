import os
import json
from pathlib import Path
from dotenv import load_dotenv
from ..exceptions import ConfigNotFoundError

_CONFIG_DIR = Path.home() / ".kavak_connector"
_CONFIG_FILE = _CONFIG_DIR / "databricks.json"
_ENV_FILE = _CONFIG_DIR / "databricks.env"


def _load_config() -> dict:
    if not _CONFIG_FILE.exists():
        raise ConfigNotFoundError(
            f"Databricks config not found: {_CONFIG_FILE}\n"
            "Run the kavak-install skill to set it up."
        )
    with open(_CONFIG_FILE) as f:
        return json.load(f)


def _save_config(config: dict) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(json.dumps(config, indent=2))
    _CONFIG_FILE.chmod(0o600)


def get_host() -> str:
    return _load_config()["host"]


def get_http_path() -> str:
    return _load_config()["http_path"]


def get_auth_method() -> str:
    """Returns 'token' or 'oauth'."""
    return _load_config().get("auth_method", "token")


def set_auth_method(method: str) -> None:
    """Update auth_method in config. method must be 'token' or 'oauth'."""
    if method not in ("token", "oauth"):
        raise ValueError(f"auth_method must be 'token' or 'oauth', got: {method!r}")
    config = _load_config()
    config["auth_method"] = method
    _save_config(config)


def get_token() -> str | None:
    if _ENV_FILE.exists():
        load_dotenv(_ENV_FILE, override=False)
    return os.environ.get("DATABRICKS_TOKEN") or None


def save_token(token: str) -> None:
    """Write or overwrite DATABRICKS_TOKEN in the .env file."""
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _ENV_FILE.write_text(f"DATABRICKS_TOKEN={token}\n")
    _ENV_FILE.chmod(0o600)

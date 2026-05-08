import json
import pytest
from pathlib import Path
from kavak_connector.exceptions import ConfigNotFoundError


def test_get_host_reads_from_config(tmp_path, monkeypatch):
    config_file = tmp_path / "databricks.json"
    config_file.write_text(json.dumps({
        "host": "dbc-test.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/abc",
        "auth_method": "token"
    }))
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_FILE", config_file)
    from kavak_connector.databricks.auth import get_host
    assert get_host() == "dbc-test.cloud.databricks.com"


def test_get_http_path_reads_from_config(tmp_path, monkeypatch):
    config_file = tmp_path / "databricks.json"
    config_file.write_text(json.dumps({
        "host": "dbc-test.cloud.databricks.com",
        "http_path": "/sql/1.0/warehouses/abc",
        "auth_method": "token"
    }))
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_FILE", config_file)
    from kavak_connector.databricks.auth import get_http_path
    assert get_http_path() == "/sql/1.0/warehouses/abc"


def test_get_auth_method_reads_from_config(tmp_path, monkeypatch):
    config_file = tmp_path / "databricks.json"
    config_file.write_text(json.dumps({
        "host": "h", "http_path": "p", "auth_method": "oauth"
    }))
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_FILE", config_file)
    from kavak_connector.databricks.auth import get_auth_method
    assert get_auth_method() == "oauth"


def test_get_host_raises_when_config_missing(tmp_path, monkeypatch):
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_FILE",
                        tmp_path / "nonexistent.json")
    from kavak_connector.databricks.auth import get_host
    with pytest.raises(ConfigNotFoundError):
        get_host()


def test_get_token_returns_none_when_no_env_file(tmp_path, monkeypatch):
    monkeypatch.setattr("kavak_connector.databricks.auth._ENV_FILE",
                        tmp_path / "databricks.env")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    from kavak_connector.databricks.auth import get_token
    assert get_token() is None


def test_get_token_reads_from_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / "databricks.env"
    env_file.write_text("DATABRICKS_TOKEN=dapi_test_token\n")
    monkeypatch.setattr("kavak_connector.databricks.auth._ENV_FILE", env_file)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    from kavak_connector.databricks.auth import get_token
    assert get_token() == "dapi_test_token"


def test_save_token_writes_env_file(tmp_path, monkeypatch):
    env_file = tmp_path / "databricks.env"
    monkeypatch.setattr("kavak_connector.databricks.auth._ENV_FILE", env_file)
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_DIR", tmp_path)
    from kavak_connector.databricks.auth import save_token
    save_token("dapi_new_token")
    assert "DATABRICKS_TOKEN=dapi_new_token" in env_file.read_text()


def test_set_auth_method_updates_config(tmp_path, monkeypatch):
    config_file = tmp_path / "databricks.json"
    config_file.write_text(json.dumps({"host": "h", "http_path": "p", "auth_method": "token"}))
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_FILE", config_file)
    from kavak_connector.databricks.auth import set_auth_method
    set_auth_method("oauth")
    result = json.loads(config_file.read_text())
    assert result["auth_method"] == "oauth"


def test_set_auth_method_raises_on_invalid_value(tmp_path, monkeypatch):
    config_file = tmp_path / "databricks.json"
    config_file.write_text(json.dumps({"host": "h", "http_path": "p", "auth_method": "token"}))
    monkeypatch.setattr("kavak_connector.databricks.auth._CONFIG_FILE", config_file)
    from kavak_connector.databricks.auth import set_auth_method
    with pytest.raises(ValueError):
        set_auth_method("invalid")

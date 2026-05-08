import pytest
from kavak_connector.exceptions import ConfigNotFoundError


def test_get_credentials_returns_dict(tmp_path, monkeypatch):
    env_file = tmp_path / "redshift.env"
    env_file.write_text(
        "REDSHIFT_HOST=cluster.abc.us-east-1.redshift.amazonaws.com\n"
        "REDSHIFT_PORT=5439\n"
        "REDSHIFT_DATABASE=kavak\n"
        "REDSHIFT_USER=analyst\n"
        "REDSHIFT_PASSWORD=secret\n"
    )
    monkeypatch.setattr("kavak_connector.redshift.auth._ENV_FILE", env_file)
    for k in ("REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    from kavak_connector.redshift.auth import get_credentials
    creds = get_credentials()
    assert creds["host"] == "cluster.abc.us-east-1.redshift.amazonaws.com"
    assert creds["port"] == 5439
    assert creds["database"] == "kavak"
    assert creds["user"] == "analyst"
    assert creds["password"] == "secret"


def test_get_credentials_defaults_port_to_5439(tmp_path, monkeypatch):
    env_file = tmp_path / "redshift.env"
    env_file.write_text("REDSHIFT_HOST=h\nREDSHIFT_DATABASE=d\nREDSHIFT_USER=u\nREDSHIFT_PASSWORD=p\n")
    monkeypatch.setattr("kavak_connector.redshift.auth._ENV_FILE", env_file)
    for k in ("REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    from kavak_connector.redshift.auth import get_credentials
    assert get_credentials()["port"] == 5439


def test_get_credentials_raises_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setattr("kavak_connector.redshift.auth._ENV_FILE", tmp_path / "nonexistent.env")
    from kavak_connector.redshift.auth import get_credentials
    with pytest.raises(ConfigNotFoundError):
        get_credentials()


def test_get_credentials_raises_when_vars_missing(tmp_path, monkeypatch):
    env_file = tmp_path / "redshift.env"
    env_file.write_text("REDSHIFT_HOST=h\n")
    monkeypatch.setattr("kavak_connector.redshift.auth._ENV_FILE", env_file)
    for k in ("REDSHIFT_HOST", "REDSHIFT_PORT", "REDSHIFT_DATABASE", "REDSHIFT_USER", "REDSHIFT_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    from kavak_connector.redshift.auth import get_credentials
    with pytest.raises(ConfigNotFoundError, match="Missing"):
        get_credentials()

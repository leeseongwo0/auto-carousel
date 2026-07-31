from __future__ import annotations

from pathlib import Path

import pytest

from newsbot.config import Capability, ConfigError, load_config, validate_capabilities


def _channels_toml() -> str:
    return Path("config/channels.toml").read_text(encoding="utf-8")


def test_load_config_requires_exactly_six_enabled_channels(tmp_path: Path) -> None:
    path = tmp_path / "channels.toml"
    path.write_text(_channels_toml(), encoding="utf-8")

    config = load_config(path, environ={})

    assert len(config.channels) == 6
    assert len(config.enabled_channels) == 6
    assert config.database_path == Path("data/newsbot.sqlite")
    assert config.google_service_account_file is None
    assert config.google_sheets_spreadsheet_id is None

    valid = _channels_toml()
    last_channel = valid.rfind("[[channels]]")
    policy = valid.find("\n[policy]", last_channel)
    invalid = valid[:last_channel] + valid[policy:]
    path.write_text(invalid, encoding="utf-8")
    with pytest.raises(ConfigError) as error:
        load_config(path, environ={})
    assert str(error.value) == "configuration must define exactly six channels"


def test_capability_credentials_are_scoped_to_requested_adapter() -> None:
    validate_capabilities((Capability.FIXTURE_RUN, Capability.GENERATE_FAKE), environ={})

    with pytest.raises(ConfigError) as error:
        validate_capabilities(Capability.LIVE_COLLECTION, environ={})
    assert str(error.value) == (
        "missing required environment variables: TELEGRAM_API_HASH, TELEGRAM_API_ID, TELEGRAM_SESSION_PATH"
    )

    with pytest.raises(ConfigError) as error:
        validate_capabilities(Capability.GENERATE_OPENAI, environ={"OPENAI_MODEL": "local"})
    assert str(error.value) == (
        "missing required environment variables: OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_TIMEOUT_SECONDS"
    )
    with pytest.raises(ConfigError) as error:
        validate_capabilities(Capability.LIVE_SHEETS, environ={})
    assert str(error.value) == (
        "missing required environment variables: GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEETS_SPREADSHEET_ID"
    )

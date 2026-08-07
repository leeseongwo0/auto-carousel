from __future__ import annotations

from pathlib import Path

import pytest

from newsbot.config import (
    Capability,
    ConfigError,
    load_behavior_profile,
    load_config,
    validate_automation_bindings,
    validate_capabilities,
)


def _channels_toml() -> str:
    return Path("config/channels.toml").read_text(encoding="utf-8")


def _manual_profile_toml(source_count: int = 2) -> str:
    policy = _channels_toml()
    policy = policy[policy.index("[policy]") :]
    sources = "\n".join(
        "\n".join(
            (
                "[[sources]]",
                f'id = "source-{index}"',
                f'name = "Source {index}"',
                "enabled = true",
                "priority = 1",
                "source_quality = 0.5",
                'classification = "community"',
                'official_domains = ["example.com"]',
                "original_domains = []",
            )
        )
        for index in range(source_count)
    )
    return 'schema = "newsbot.behavior.v1"\noperation = "manual_local"\n' + sources + "\n" + policy


def test_behavior_profile_is_bounded_deterministic_and_local_only(tmp_path: Path) -> None:
    path = tmp_path / "profile.toml"
    path.write_text(_manual_profile_toml(), encoding="utf-8")
    profile = load_behavior_profile(path)
    assert profile.schema == "newsbot.behavior.v1"
    assert profile.operation == "manual_local"
    assert len(profile.sources) == 2
    assert profile.digest == load_behavior_profile(path).digest

    for count in (0, 33):
        path.write_text(_manual_profile_toml(count), encoding="utf-8")
        with pytest.raises(ConfigError, match="1 to 32"):
            load_behavior_profile(path)


def test_behavior_profile_rejects_remote_bindings_and_unsafe_source_values(tmp_path: Path) -> None:
    path = tmp_path / "profile.toml"
    profile = _manual_profile_toml()
    for unsafe in (
        profile + '\ndatabase_path = "private.sqlite"\n',
        profile.replace('id = "source-1"', 'id = "source-0"', 1),
        profile.replace('official_domains = ["example.com"]', 'official_domains = ["https://example.com"]', 1),
        profile.replace("enabled = true", "enabled = false", 1),
    ):
        path.write_text(unsafe, encoding="utf-8")
        with pytest.raises(ConfigError):
            load_behavior_profile(path)


def test_load_config_requires_exactly_five_enabled_channels(tmp_path: Path) -> None:
    path = tmp_path / "channels.toml"
    path.write_text(_channels_toml(), encoding="utf-8")

    config = load_config(path, environ={})

    assert len(config.channels) == 5
    assert len(config.enabled_channels) == 5
    assert {channel.handle for channel in config.enabled_channels} == {
        "community_feed",
        "research_forum",
        "news_publisher",
        "news_aggregator",
        "official_updates",
    }
    assert config.database_path == Path("data/newsbot.sqlite")
    assert config.google_service_account_file is None
    assert config.google_sheets_spreadsheet_id is None

    invalid = _channels_toml()
    last_channel = invalid.rfind("[[channels]]")
    invalid = invalid[:last_channel] + invalid[invalid.find("\n[policy]", last_channel) :]
    path.write_text(invalid, encoding="utf-8")
    with pytest.raises(ConfigError) as error:
        load_config(path, environ={})
    assert str(error.value) == "configuration must define exactly five channels"

    path.write_text(_channels_toml().replace("enabled = true", "enabled = false", 1), encoding="utf-8")
    with pytest.raises(ConfigError, match="all five configured channels"):
        load_config(path, environ={})

    path.write_text(
        _channels_toml().replace('id = "research_forum"', 'id = "COMMUNITY_FEED"', 1),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="ids and handles must be unique"):
        load_config(path, environ={})

    path.write_text(
        _channels_toml().replace('handle = "research_forum"', 'handle = "COMMUNITY_FEED"', 1),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="ids and handles must be unique"):
        load_config(path, environ={})

    assert validate_automation_bindings(config) is None


def test_load_config_accepts_private_five_channel_identities(tmp_path: Path) -> None:
    path = tmp_path / "private-channels.toml"
    config_text = _channels_toml()
    for index, public_handle in enumerate(
        (
            "community_feed",
            "research_forum",
            "news_publisher",
            "news_aggregator",
            "official_updates",
        ),
        start=1,
    ):
        config_text = config_text.replace(public_handle, f"private_source_{index}")
    path.write_text(config_text, encoding="utf-8")

    config = load_config(path, environ={})

    assert {channel.handle for channel in config.enabled_channels} == {
        f"private_source_{index}" for index in range(1, 6)
    }


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


def test_news_policy_is_required_strict_and_digest_sensitive(tmp_path: Path) -> None:
    path = tmp_path / "channels.toml"
    valid = _channels_toml()
    path.write_text(valid, encoding="utf-8")
    config = load_config(path, environ={})
    assert config.news_policy.version == "news_policy_v1"
    digest = config.digest

    path.write_text(valid.replace("activation_minutes = 60", "activation_minutes = 5"), encoding="utf-8")
    with pytest.raises(ConfigError, match="activation_minutes"):
        load_config(path, environ={})
    path.write_text(valid.replace("activation_minutes = 60\n", ""), encoding="utf-8")
    with pytest.raises(ConfigError, match="missing keys"):
        load_config(path, environ={})

    path.write_text(valid.replace("[news_policy]\n", "[news_policy]\nunapproved = true\n"), encoding="utf-8")
    with pytest.raises(ConfigError, match="unknown keys"):
        load_config(path, environ={})

    path.write_text(
        valid.replace(
            'event_markers_ko = ["출시",',
            'event_markers_ko = ["출시", "출시",',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="duplicate normalized"):
        load_config(path, environ={})

    path.write_text(valid.replace('"출시", "공개"', '"출시x", "공개"'), encoding="utf-8")
    assert load_config(path, environ={}).digest != digest

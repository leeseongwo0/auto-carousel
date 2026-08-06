"""Manual command parser and bounded import contracts."""

import json
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from newsbot import manual
from newsbot.cli import _assert_legacy_database_authority, build_parser
from newsbot.collectors.base import SourceObservation
from newsbot.config import BehaviorProfile, ManualSourceConfig, PolicyConfig
from newsbot.manual import _import_observations
from newsbot.manual_storage import ManualStorage
from newsbot.storage import Storage


def _profile() -> BehaviorProfile:
    return BehaviorProfile(
        "newsbot.behavior.v1",
        "manual_local",
        (ManualSourceConfig("source", "Source", True, 1, 1, "official", ("example.com",), ()),),
        PolicyConfig(),
    )


def _telethon_profile() -> BehaviorProfile:
    return BehaviorProfile(
        "newsbot.behavior.v1",
        "manual_local",
        (
            ManualSourceConfig("first", "First", True, 1, 1, "official", ("example.com",), (), "first_public"),
            ManualSourceConfig("second", "Second", True, 2, 1, "official", ("example.com",), (), "second_public"),
        ),
        PolicyConfig(),
    )


@pytest.mark.parametrize(
    "command, extra",
    [
        ("manual-candidates", ["--run-id", "1"]),
        ("manual-generate", ["--candidate-id", "1", "--provider", "fake"]),
        ("manual-draft", ["--generation-id", "1"]),
        ("manual-export", []),
    ],
)
def test_manual_materialization_commands_use_private_default_output(command: str, extra: list[str]) -> None:
    parser = build_parser()
    args = parser.parse_args([command, "--profile", "p.toml", "--state", "/home/user/private", *extra])
    assert args.output_dir is None


def test_import_rejects_oversized_record_before_state_open(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = json.dumps({"schema": "newsbot.manual.import.v1", "records": [{}] * 10_001}).encode()
    monkeypatch.setattr(ManualStorage, "read_private_input", lambda _path, *, max_bytes: raw)
    with pytest.raises(ValueError, match="record bounds"):
        _import_observations(Path("/unused/input.json"), _profile())


def test_import_rejects_content_without_echoing_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "private-source-content-must-not-be-disclosed"
    document = tmp_path / "input.json"
    document.write_text(
        json.dumps(
            {
                "schema": "newsbot.manual.import.v1",
                "records": [
                    {
                        "source_id": "source",
                        "post_id": "1",
                        "published_at": "not-a-timestamp",
                        "text": secret,
                        "urls": [],
                    }
                ],
            }
        )
    )
    raw = document.read_bytes()
    monkeypatch.setattr(ManualStorage, "read_private_input", lambda _path, *, max_bytes: raw)
    with pytest.raises(ValueError) as raised:
        _import_observations(document, _profile())
    assert secret not in str(raised.value)


def test_manual_telethon_collection_requires_explicit_external_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(manual, "_profile", lambda _args: _profile())

    def unexpected_capability_check(_capability: object) -> None:
        raise AssertionError("credential validation must follow external-handle validation")

    monkeypatch.setattr(manual, "validate_capabilities", unexpected_capability_check)

    with pytest.raises(ValueError, match="explicit public handles"):
        manual.manual_collect_telethon(Namespace(lookback_hours=1, page_limit=1, max_pages=1, deadline_seconds=1))


def test_manual_telethon_collection_is_capped_resumable_and_collection_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "manual.sqlite3"
    calls: list[tuple[str, int]] = []

    class FakeTelethon:
        async def latest_message_id(self, channel: object) -> int:
            return 2

        async def collect(self, channel: object, **kwargs: object) -> tuple[SourceObservation, ...]:
            source_id = str(channel.id)
            minimum = int(kwargs["min_message_id"])
            calls.append((source_id, minimum))
            observed = datetime.now(UTC) - timedelta(seconds=1)
            return tuple(
                SourceObservation(source_id, str(channel.handle), str(message_id), observed)
                for message_id in range(minimum + 1, 3)
            )

        async def close(self) -> None:
            return None

    def open_state(_args: object, *, bind: bool) -> tuple[object, Storage, BehaviorProfile]:
        assert not bind
        return object(), Storage.open(database), _telethon_profile()

    monkeypatch.setattr(manual, "_open", open_state)
    monkeypatch.setattr(manual, "_profile", lambda _args: _telethon_profile())
    monkeypatch.setattr(manual, "_close", lambda _state, storage, _args: storage.close())
    monkeypatch.setattr(manual, "TelethonCollector", lambda *_args, **_kwargs: FakeTelethon())
    monkeypatch.setattr(manual, "validate_capabilities", lambda _capability: None)
    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))
    args = Namespace(lookback_hours=1, page_limit=1, max_pages=1, deadline_seconds=10)

    assert manual.manual_collect_telethon(args) == 0
    assert calls == [("first", 0), ("second", 0)]
    with Storage.open(database) as storage:
        assert [
            (str(row["channel_id"]), int(row["next_message_id"]), int(row["page_complete"]))
            for row in storage.fetch_all(
                "SELECT channel_id,next_message_id,page_complete FROM collection_intervals ORDER BY channel_id"
            )
        ] == [("first", 1, 0), ("second", 1, 0)]
    assert manual.manual_collect_telethon(args) == 0
    assert calls == [("first", 0), ("second", 0), ("first", 1), ("second", 1)]
    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT COUNT(*) AS n FROM source_posts")["n"] == 4
        assert storage.fetch_one("SELECT COUNT(*) AS n FROM collection_intervals")["n"] == 2
        for table in ("candidate_evaluations", "generations", "manual_local_export_outbox"):
            assert storage.fetch_one(f"SELECT COUNT(*) AS n FROM {table}")["n"] == 0


def test_manual_telethon_collection_enforces_total_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = tmp_path / "manual.sqlite3"
    monotonic = iter((0.0, 2.0))

    def open_state(_args: object, *, bind: bool) -> tuple[object, Storage, BehaviorProfile]:
        assert not bind
        return object(), Storage.open(database), _telethon_profile()

    monkeypatch.setattr(manual, "_open", open_state)
    monkeypatch.setattr(manual, "_profile", lambda _args: _telethon_profile())
    monkeypatch.setattr(manual, "_close", lambda _state, storage, _args: storage.close())

    class FakeTelethon:
        async def latest_message_id(self, _channel: object) -> int:
            pytest.fail("deadline must stop collection before the first remote request")

        async def close(self) -> None:
            return None

    monkeypatch.setattr(manual, "TelethonCollector", lambda *_args, **_kwargs: FakeTelethon())
    monkeypatch.setattr(manual, "validate_capabilities", lambda _capability: None)
    monkeypatch.setattr(manual.time, "monotonic", lambda: next(monotonic, 2.0))
    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))

    with pytest.raises(TimeoutError, match="deadline exhausted"):
        manual.manual_collect_telethon(Namespace(lookback_hours=1, page_limit=1, max_pages=1, deadline_seconds=1))
    with Storage.open(database) as storage:
        for table in ("source_posts", "candidate_evaluations", "generations", "manual_local_export_outbox"):
            assert storage.fetch_one(f"SELECT COUNT(*) AS n FROM {table}")["n"] == 0


def test_legacy_preflight_refuses_manual_authority_without_migrating(tmp_path: Path) -> None:
    database = tmp_path / "manual.sqlite3"
    with Storage.open(database) as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", "a" * 64)
    with pytest.raises(RuntimeError, match="manual profile authority conflicts"):
        _assert_legacy_database_authority(database)
    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT COUNT(*) AS n FROM automation_cutovers")["n"] == 0

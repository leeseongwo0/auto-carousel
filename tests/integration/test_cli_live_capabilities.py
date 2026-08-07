from __future__ import annotations

import asyncio
import json
import sys
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot.approval.telegram import split_telegram_text
from newsbot.config import Capability, ConfigError, validate_capabilities
from newsbot.copywriting import adaptive_page_count
from newsbot.storage import Storage


@pytest.fixture(autouse=True)
def isolated_automation_locks(monkeypatch: pytest.MonkeyPatch) -> None:
    from newsbot import automation

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    monkeypatch.setattr(automation, "automation_lock", no_lock)


def test_adaptive_page_count_has_deterministic_bounded_extremes() -> None:
    assert adaptive_page_count(("brief",)) == 1
    assert adaptive_page_count(("sentence. " * 1000,)) == 8


def test_telegram_splitter_preserves_utf16_emoji_and_newlines() -> None:
    text = ("😀\n" * 3000) + "끝"
    chunks = split_telegram_text(text)
    assert "".join(chunks) == text
    assert all(len(chunk.encode("utf-16-le")) // 2 <= 4096 for chunk in chunks)


def test_optional_capabilities_fail_closed_independently() -> None:
    with pytest.raises(ConfigError, match="TELEGRAM_API_ID"):
        validate_capabilities(Capability.LIVE_COLLECTION, environ={})
    with pytest.raises(ConfigError, match="OPENAI_API_KEY"):
        validate_capabilities(Capability.GENERATE_OPENAI, environ={})


def test_sheets_capability_fails_closed_without_live_settings() -> None:
    with pytest.raises(ConfigError) as error:
        validate_capabilities(Capability.LIVE_SHEETS, environ={})
    assert str(error.value) == (
        "missing required environment variables: GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_SHEETS_SPREADSHEET_ID"
    )


def test_poll_generation_validates_bot_and_openai_capabilities_before_bot_effects(monkeypatch) -> None:
    from newsbot import cli

    calls: list[tuple[Capability, ...]] = []

    def fail_before_bot_effects(capabilities: Capability | list[Capability]) -> None:
        calls.append(tuple(capabilities) if isinstance(capabilities, list) else (capabilities,))
        raise ConfigError("missing required environment variables: OPENAI_API_KEY")

    monkeypatch.setattr(cli, "validate_capabilities", fail_before_bot_effects)
    monkeypatch.setattr(cli, "_config", lambda _: SimpleNamespace())
    with pytest.raises(SystemExit, match="2"):
        cli.main(["poll-approvals", "--process-generation", "--provider", "openai_compatible"])
    assert calls == [(Capability.APPROVE_POLL, Capability.GENERATE_OPENAI)]


def test_approval_poll_cursor_is_durable_monotonic_and_explicitly_overridable() -> None:
    from newsbot import cli

    with Storage.open(":memory:") as storage:
        assert cli._approval_poll_offset(storage, None) is None

        cli._advance_approval_poll_offset(storage, 10)
        assert cli._approval_poll_offset(storage, None) == 11

        cli._advance_approval_poll_offset(storage, 4)
        assert cli._approval_poll_offset(storage, None) == 11
        assert cli._approval_poll_offset(storage, 7) == 7


def test_capability_scoped_commands_appear_in_help_without_optional_imports(capsys) -> None:
    from newsbot import cli

    with pytest.raises(SystemExit, match="0"):
        cli.main(["--help"])
    help_text = capsys.readouterr().out
    assert "auth-telethon" in help_text
    assert "rank" in help_text
    assert "reconcile" in help_text


def test_base_cli_help_never_imports_google_packages(monkeypatch, capsys) -> None:
    from newsbot import cli

    monkeypatch.setitem(sys.modules, "google", None)
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["--help"])
    assert exit_info.value.code == 0
    help_text = capsys.readouterr().out
    assert "sheets-validate" in help_text
    assert "sheets-bootstrap" in help_text
    assert "sheets-deliver" in help_text
    assert "sheets-status" in help_text
    assert "sheets-reconcile" in help_text
    assert "sheets-retry-blocked" in help_text
    assert "repair-exports" not in help_text
    assert "--output" not in help_text


def test_live_sheets_commands_fail_before_google_import(monkeypatch) -> None:
    from newsbot import cli

    monkeypatch.setitem(sys.modules, "newsbot.sheets.google", None)
    with pytest.raises(SystemExit) as error:
        cli.main(["sheets-validate"])
    assert error.value.code == 2


def test_live_sheets_uses_one_validated_credential_snapshot(monkeypatch, tmp_path) -> None:
    from newsbot import cli
    from newsbot.sheets.google import GoogleSheetsAdapter

    credential_path = tmp_path / "service-account.json"
    config = SimpleNamespace(
        google_service_account_file=credential_path,
        google_sheets_spreadsheet_id="sheet",
    )
    credential_info = {
        "type": "service_account",
        "client_email": "bot@example.invalid",
    }
    reads = 0
    adapter = object()

    def read_once(path: Path) -> dict[str, str]:
        nonlocal reads
        reads += 1
        assert path == credential_path
        if reads > 1:
            raise AssertionError("credential file reopened")
        return credential_info

    def construct_from_snapshot(
        *,
        credential_info: dict[str, str],
        spreadsheet_id: str,
        deadline_monotonic: float | None = None,
    ) -> object:
        assert credential_info is credential_info_snapshot
        assert spreadsheet_id == "sheet"
        assert deadline_monotonic is None
        return adapter

    credential_info_snapshot = credential_info
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_: None)
    monkeypatch.setattr(cli, "_config", lambda _: config)
    monkeypatch.setattr(cli, "read_service_account_info", read_once)
    monkeypatch.setattr(
        GoogleSheetsAdapter,
        "from_credentials",
        staticmethod(construct_from_snapshot),
    )

    actual_config, actual_adapter, email = cli._live_sheets(SimpleNamespace())

    assert actual_config is config
    assert actual_adapter is adapter
    assert email == "bot@example.invalid"
    assert reads == 1


def test_live_sheets_redacts_credential_path(monkeypatch, tmp_path) -> None:
    from newsbot import cli
    from newsbot.secrets import SecretFileError

    credential_path = tmp_path / "private" / "service-account.json"
    config = SimpleNamespace(
        google_service_account_file=credential_path,
        google_sheets_spreadsheet_id="sheet",
    )
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_: None)
    monkeypatch.setattr(cli, "_config", lambda _: config)

    def fail_read(path: Path) -> dict[str, str]:
        raise SecretFileError(f"cannot safely open {path}")

    monkeypatch.setattr(cli, "read_service_account_info", fail_read)

    with pytest.raises(ConfigError) as error:
        cli._live_sheets(SimpleNamespace())

    assert str(error.value) == "Google Sheets credential file is invalid"
    assert str(credential_path) not in str(error.value)


def test_sheets_bootstrap_redacts_pre_dispatch_provider_errors(
    tmp_path,
    monkeypatch,
) -> None:
    from newsbot import cli
    from newsbot.sheets.google import GoogleSheetsAdapter

    secret = "https://sheets.googleapis.test/private-sheet?token=secret"
    credential_file = tmp_path / "service-account.json"
    credential_file.write_text(
        json.dumps(
            {
                "type": "service_account",
                "client_email": "bot@example.invalid",
            }
        ),
        encoding="utf-8",
    )
    credential_file.chmod(0o600)

    class FailingService:
        def get_document(self, _spreadsheet_id: str) -> object:
            raise RuntimeError(secret)

    config = SimpleNamespace(
        database_path=tmp_path / "newsbot.sqlite",
        google_service_account_file=credential_file,
        google_sheets_spreadsheet_id="sheet",
    )
    adapter = GoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=FailingService(),
        service_account_email="bot@example.invalid",
    )
    monkeypatch.setattr(
        cli,
        "_live_sheets",
        lambda _args: (config, adapter, "bot@example.invalid"),
    )
    monkeypatch.setattr(cli, "_config", lambda _args: config)

    with pytest.raises(RuntimeError) as error:
        cli.sheets_bootstrap(SimpleNamespace())

    assert str(error.value) == "Google Sheets preparation read failed"
    assert secret not in str(error.value)


def test_sheets_reconcile_settles_ambiguous_bootstrap_ready(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    from newsbot import cli
    from newsbot.handoffs import SheetHandoffService
    from newsbot.sheets.base import MetadataState, SheetProbe
    from newsbot.storage import Storage

    database = tmp_path / "bootstrap.sqlite"
    credential_file = tmp_path / "service-account.json"
    credential_file.write_text(
        json.dumps({"client_email": "bot@example.invalid"}),
        encoding="utf-8",
    )
    credential_file.chmod(0o600)
    now = "2026-07-30T12:00:00+00:00"
    later = "2026-07-30T12:05:00+00:00"
    fingerprint = "a" * 64
    with Storage.open(database) as storage:
        service = SheetHandoffService(storage)
        target = service.ensure_binding(
            binding_key="workplace",
            spreadsheet_id="sheet",
            sheet_id=0,
            oracle_fingerprint=fingerprint,
            now=now,
        )
        assert (
            service.ensure_bootstrap(
                target_binding_id=target,
                marker_value="schema",
                controls_fingerprint=fingerprint,
            )
            == "uninitialized"
        )
        lease = service.acquire_initial(
            None,
            operation_kind="bootstrap",
            target_binding_id=target,
            now=now,
            expires_at=later,
        )
        assert lease is not None
        assert service.record_preflight(lease, outcome="absent", now=now)
        assert service.mark_possibly_sent(
            lease,
            request_sha256=fingerprint,
            oracle_fingerprint=fingerprint,
            controls_fingerprint=fingerprint,
            credential_refreshed_at=now,
            credential_expires_at="2026-07-30T16:00:00+00:00",
            credential_scope_ok=True,
            now=now,
        )
        assert service.release_possibly_sent(lease, now=now)
        operation_id = lease.operation_id

    class ExactBootstrapAdapter:
        def probe_bootstrap(self, *, service_account_email: str) -> SheetProbe:
            assert service_account_email == "bot@example.invalid"
            return SheetProbe(metadata=MetadataState.EXACT)

    config = SimpleNamespace(
        database_path=database,
        google_service_account_file=credential_file,
    )
    monkeypatch.setattr(
        cli,
        "_live_sheets",
        lambda args: (config, ExactBootstrapAdapter(), "bot@example.invalid"),
    )

    assert cli.sheets_reconcile(SimpleNamespace(operation_id=operation_id)) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "ready"
    with Storage.open(database) as storage:
        assert (
            storage.fetch_one(
                "SELECT status FROM sheet_bootstraps WHERE target_binding_id=?",
                (target,),
            )["status"]
            == "ready"
        )


def test_reconcile_live_range_arguments_fail_closed_before_live_adapter_import(monkeypatch) -> None:
    from newsbot import cli

    monkeypatch.setattr(cli, "_live_collector", lambda *_: pytest.fail("live adapter must not be imported"))
    with pytest.raises(SystemExit, match="2"):
        cli.main(["reconcile-live", "--from-id", "10"])
    with pytest.raises(SystemExit, match="2"):
        cli.main(["reconcile-live", "--from-id", "10", "--to-id", "20", "--lookback-hours", "24"])


def test_targeted_live_reconcile_failure_is_nonzero_and_does_not_emit_digest(tmp_path, monkeypatch, capsys) -> None:
    from newsbot import cli
    from newsbot.storage import Storage

    database = tmp_path / "newsbot.sqlite"
    config = SimpleNamespace(
        database_path=database,
        output_dir=tmp_path / "output",
        enabled_channels=(SimpleNamespace(id="news_publisher", handle="news_publisher"),),
    )
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_: None)
    monkeypatch.setattr(cli, "_config", lambda _: config)
    monkeypatch.setattr(cli, "SessionStore", lambda _: SimpleNamespace(validate=lambda: tmp_path / "session"))
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))
    monkeypatch.setattr(cli, "_live_collector", lambda *_, **__: (object(), lambda: None))

    def fail_reconcile(*_: object, **__: object) -> int:
        raise RuntimeError("targeted reconciliation failed")

    monkeypatch.setattr(cli.DurableCollection, "reconcile_channel", fail_reconcile)

    with pytest.raises(SystemExit) as error:
        cli.main(["reconcile-live", "--channel", "news_publisher", "--lookback-hours", "24"])

    assert error.value.code == 2
    assert "targeted reconciliation failed" in capsys.readouterr().err
    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM digests")["count"] == 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM candidates")["count"] == 0


def test_exact_range_collector_uses_inclusive_bounds_without_newest_lookup() -> None:
    from newsbot.cli import _ExactRangeCollector

    calls: list[dict[str, object]] = []

    class Collector:
        def collect(self, _: object, **kwargs: object):
            calls.append(kwargs)
            return ()

    bounded = _ExactRangeCollector(Collector(), 10, 20)
    assert bounded.latest_message_id(object()) == 20
    assert bounded.collect(object(), min_message_id=0, max_message_id=20, limit=101) == ()
    assert calls == [{"min_message_id": 9, "max_message_id": 20, "limit": 101}]


def test_reconcile_fixture_requires_complete_exclusive_bounds_before_collector(monkeypatch) -> None:
    from newsbot import cli

    monkeypatch.setattr(cli, "FixtureCollector", lambda _: pytest.fail("fixture collector must not be constructed"))
    with pytest.raises(SystemExit, match="2"):
        cli.main(["reconcile", "--fixture", "ignored.json", "--channel", "news_publisher"])
    with pytest.raises(SystemExit, match="2"):
        cli.main(
            [
                "reconcile",
                "--fixture",
                "ignored.json",
                "--channel",
                "news_publisher",
                "--from-id",
                "10",
                "--to-id",
                "20",
                "--lookback-hours",
                "24",
            ]
        )


def test_auth_telethon_enforces_private_umask_and_restores_caller_umask(tmp_path, monkeypatch) -> None:
    from newsbot import cli

    state = {"mode": 0o022, "closed": False}

    def fake_umask(mode: int) -> int:
        previous = int(state["mode"])
        state["mode"] = mode
        return previous

    def authenticate() -> None:
        assert state["mode"] == 0o077

    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "newsbot.session"))
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_: None)
    monkeypatch.setattr(cli, "ensure_private_directory", lambda _: None)
    monkeypatch.setattr(cli.os, "umask", fake_umask)
    monkeypatch.setattr(cli, "SessionStore", lambda _: SimpleNamespace(validate=lambda: None))
    monkeypatch.setattr(
        cli,
        "_live_collector",
        lambda *_: (
            SimpleNamespace(authenticate=authenticate),
            lambda: state.__setitem__("closed", True),
        ),
    )

    assert cli.auth_telethon(SimpleNamespace()) == 0
    assert state == {"mode": 0o022, "closed": True}


def test_auth_telethon_fails_closed_before_adapter_import(monkeypatch) -> None:
    from newsbot import cli

    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.delenv("TELEGRAM_SESSION_PATH", raising=False)
    monkeypatch.setattr(cli, "_live_collector", lambda *_: pytest.fail("adapter must not be imported"))
    with pytest.raises(SystemExit, match="2"):
        cli.main(["auth-telethon"])


def test_collect_live_uses_one_loop_and_creates_provider_free_candidate_digest(tmp_path, monkeypatch, capsys) -> None:
    from newsbot import cli
    from newsbot.config import load_config
    from newsbot.storage import Storage

    now = datetime.now(UTC) - timedelta(minutes=1)
    loops: list[asyncio.AbstractEventLoop] = []

    class Message:
        id = 10
        date = now
        message = "Official team announced an AI technology release with independently useful product details, rollout scope, supported users, measured impact, and operational context for readers."
        edit_date = None
        action = None
        views = 100
        forwards = 1

    class TelegramClient:
        def __init__(self, *_: object) -> None:
            pass

        async def connect(self) -> None:
            loops.append(asyncio.get_running_loop())

        async def disconnect(self) -> None:
            loops.append(asyncio.get_running_loop())

        async def iter_messages(self, _: str, **request: object):
            loops.append(asyncio.get_running_loop())
            if request.get("limit") == 1 or request.get("min_id", 0) < Message.id <= request.get("max_id", Message.id):
                yield Message()

    config = SimpleNamespace(
        database_path=tmp_path / "newsbot.sqlite",
        output_dir=tmp_path / "output",
        digest="live-config",
        enabled_channels=(SimpleNamespace(id="official", handle="official"),),
        channels_by_id={"official": SimpleNamespace(source_quality=1, classification="official")},
        news_policy=load_config(Path("config/channels.toml"), environ={}).news_policy,
        policy=SimpleNamespace(
            version="candidate_policy_v1",
            novelty_window_days=7,
            topic_positive_phrases=(("ai", 1), ("technology", 0.5)),
            topic_exclusion_phrases=(),
            engagement_weights=(("views", 0.60), ("reactions", 0.25), ("forwards", 0.15)),
            engagement_saturation=(("views", 100000), ("reactions", 5000), ("forwards", 1000)),
            certainty_markers=(("rumor", 0.3), ("루머", 0.3), ("anonymous", 0.2)),
            certainty_categories=(("rumor", 0.3, ("rumor", "루머")), ("anonymous", 0.2, ("anonymous",))),
            certainty_penalties=(("conflicts", 0.5), ("missing_url", 0.2)),
            disclosure_markers=("[광고]", "sponsored"),
            referral_markers=("추천인", "referral"),
            min_total_score=0,
            min_topic_relevance=0,
            max_candidate_age_hours=72,
            future_tolerance_hours=2,
            min_semantic_chars=20,
            min_material_sentence_chars=10,
            freshness_horizon_hours=48,
            weight_map={
                "source_quality": 0.15,
                "freshness": 0.15,
                "engagement": 0.10,
                "topic_relevance": 0.25,
                "novelty": 0.15,
                "official_evidence": 0.15,
                "certainty": 0.05,
            },
        ),
    )
    monkeypatch.setitem(sys.modules, "telethon", SimpleNamespace(TelegramClient=TelegramClient))
    monkeypatch.setattr(cli, "_config", lambda _: config)
    monkeypatch.setattr(cli, "_provider_factory", lambda _: pytest.fail("live collection constructed a provider"))
    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "hash")
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))
    session_dir = tmp_path / "sessions"
    session_dir.mkdir(mode=0o700)
    session_path = session_dir / "session"
    session_path.write_bytes(b"session")
    session_path.chmod(0o600)
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(session_path))
    assert cli.main(["collect-live"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "pending_selection"
    assert "digest_path" not in result
    assert not config.output_dir.exists()
    with Storage.open(config.database_path) as storage:
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM candidates")["count"] == 1
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM digests")["count"] == 1
    assert len({id(loop) for loop in loops}) == 1


def test_durable_live_pipeline_binds_the_latest_observation_material_version_after_a_to_b_to_a(
    tmp_path,
) -> None:
    from newsbot.cli import DurableLivePipeline
    from newsbot.collectors.fixture import FixtureCollector
    from newsbot.runtime import FixtureClock
    from newsbot.storage import Storage, persist_observation

    fixture_path = tmp_path / "versions.json"
    fixture_path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "channel_id": "news_publisher",
                        "channel_handle": "news_publisher",
                        "id": "7",
                        "published_at": "2026-07-29T11:00:00Z",
                        "text": "A",
                        "engagement": {"views": 1},
                    }
                ],
                "edits": [
                    {"after_call": 2, "message": {"id": "7", "text": "B", "engagement": {"views": 2}}},
                    {"after_call": 3, "message": {"id": "7", "text": "A", "engagement": {"views": 3}}},
                ],
            }
        ),
        encoding="utf-8",
    )
    collector = FixtureCollector(fixture_path)
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        for _ in range(3):
            observation = collector.collect(SimpleNamespace(id="news_publisher", handle="news_publisher"))[0]
            with storage.transaction() as connection:
                persist_observation(connection, observation, datetime(2026, 7, 29, 12, tzinfo=UTC))
        latest = storage.latest_observations()
        bound = DurableLivePipeline(
            storage,
            SimpleNamespace(),
            lambda: pytest.fail("provider must not be constructed"),
            FixtureClock(),
        )._persist_sources(latest, FixtureClock().now())
        versions = storage.fetch_all("SELECT id, body FROM source_post_versions ORDER BY id")
        snapshots = storage.fetch_all("SELECT id, source_post_version_id FROM source_post_observations ORDER BY id")

    assert [row["body"] for row in versions] == ["A", "B"]
    assert [row["source_post_version_id"] for row in snapshots] == [
        versions[0]["id"],
        versions[1]["id"],
        versions[0]["id"],
    ]
    assert bound == {("news_publisher", "7"): snapshots[-1]["id"]}


@pytest.mark.parametrize("kind", ("missing", "world_readable", "symlink"))
def test_collect_live_rejects_unsafe_session_before_adapter_import(tmp_path, monkeypatch, capsys, kind) -> None:
    from newsbot import cli

    session_dir = tmp_path / "sessions"
    session_dir.mkdir(mode=0o700)
    session_dir.chmod(0o700)
    session_path = session_dir / "session"
    if kind == "world_readable":
        session_path.write_bytes(b"session")
        session_path.chmod(0o644)
    elif kind == "symlink":
        target = session_dir / "target"
        target.write_bytes(b"session")
        target.chmod(0o600)
        session_path.symlink_to(target)

    monkeypatch.setenv("TELEGRAM_API_ID", "1")
    monkeypatch.setenv("TELEGRAM_API_HASH", "live-hash-secret")
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(session_path))
    monkeypatch.setattr(cli, "_live_collector", lambda *_: pytest.fail("adapter import must not occur"))

    with pytest.raises(SystemExit) as error:
        cli.main(["collect-live"])

    assert error.value.code == 2
    assert "live-hash-secret" not in capsys.readouterr().err


def test_openai_capability_failure_is_recoverable_after_selection(tmp_path, monkeypatch, capsys) -> None:
    from newsbot import cli
    from newsbot.ai.fake import FakeGenerationProvider
    from newsbot.approval.scripted import ScriptedAction, ScriptedApprovalAdapter
    from newsbot.candidates import CandidateApprovalService
    from newsbot.collectors.fixture import FixtureCollector
    from newsbot.config import load_config
    from newsbot.pipeline import NewsPipeline
    from newsbot.runtime import FixtureClock
    from newsbot.storage import Storage

    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        """{"messages":[{"channel_id":"news_publisher","channel_handle":"news_publisher","id":"1","published_at":"2026-07-29T11:00:00Z","text":"Synthetic Publisher announced an AI technology release with independently useful product details, rollout scope, supported users, measured impact, and operational context for readers.","urls":["https://publisher.example/news"]}]}""",
        encoding="utf-8",
    )
    root = Path(__file__).parents[2]
    database = tmp_path / "newsbot.sqlite"
    output = tmp_path / "output"
    config = load_config(
        root / "config/channels.toml",
        cli_overrides={"database_path": database, "output_dir": output},
    )
    clock = FixtureClock(datetime(2026, 7, 29, 12, tzinfo=UTC))
    with Storage.open(database) as storage:
        pipeline = NewsPipeline(storage, config, FakeGenerationProvider(), clock)
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        stage = asyncio.run(pipeline.run_fixture(FixtureCollector(fixture), approval_service=service, actor_id=1))
        assert stage.selection_digest is not None
        candidate_id = int(stage.selection_digest.candidates[0]["candidate_id"])
        make = next(button for button in stage.selection_digest.buttons[candidate_id] if button.label == "[제작]")
        assert ScriptedApprovalAdapter(service).apply(ScriptedAction(make.token, 1, 1)).status == "queued"

    monkeypatch.setattr(cli, "_config", lambda _: config)
    monkeypatch.setenv("OPENAI_API_KEY", "openai-secret-value")
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_MODEL", raising=False)
    monkeypatch.delenv("OPENAI_TIMEOUT_SECONDS", raising=False)

    with pytest.raises(SystemExit) as error:
        cli.main(["generate-pending", "--candidate-id", str(candidate_id), "--provider", "openai_compatible"])

    assert error.value.code == 2
    assert "openai-secret-value" not in capsys.readouterr().err
    with Storage.open(database) as storage:
        failed = storage.fetch_one(
            "SELECT id, attempts, error_message, status FROM generation_jobs WHERE status='failed_recoverable'"
        )
        assert failed is not None
        assert failed["attempts"] == 1
        assert "openai-secret-value" not in str(failed["error_message"])
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM generations")["count"] == 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM export_outbox")["count"] == 0
        job_id = int(failed["id"])

    class OpenAICompatibleConfig:
        def __init__(self, **_: object) -> None:
            pass

    class OpenAICompatibleProvider:
        def __init__(self, _: object) -> None:
            pass

        async def generate(self, request: object):
            return await FakeGenerationProvider().generate(request)

    monkeypatch.setitem(
        sys.modules,
        "newsbot.ai.openai_compatible",
        SimpleNamespace(
            OpenAICompatibleConfig=OpenAICompatibleConfig,
            OpenAICompatibleProvider=OpenAICompatibleProvider,
        ),
    )
    monkeypatch.setenv("OPENAI_BASE_URL", "https://example.invalid")
    monkeypatch.setenv("OPENAI_MODEL", "test-model")
    monkeypatch.setenv("OPENAI_TIMEOUT_SECONDS", "1")

    assert cli.main(["generate-pending", "--candidate-id", str(candidate_id), "--provider", "openai_compatible"]) == 0
    with Storage.open(database) as storage:
        succeeded = storage.fetch_one("SELECT id, attempts, status FROM generation_jobs WHERE id=?", (job_id,))
        assert succeeded is not None
        assert succeeded["id"] == job_id
        assert succeeded["attempts"] == 2
        assert succeeded["status"] == "succeeded"
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM generations")["count"] == 1
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM export_outbox")["count"] == 0


def test_candidate_set_keys_are_portable_and_separate_material_from_observation_changes(tmp_path) -> None:
    from newsbot.ai.fake import FakeGenerationProvider
    from newsbot.candidates import CandidateApprovalService
    from newsbot.collectors.base import Engagement, SourceObservation
    from newsbot.collectors.fixture import FixtureCollector
    from newsbot.config import load_config
    from newsbot.pipeline import NewsPipeline
    from newsbot.runtime import FixtureClock
    from newsbot.storage import Storage, persist_observation

    root = Path(__file__).parents[2]
    fixture = root / "tests/fixtures/channel_messages.json"
    base = next(
        observation
        for observation in FixtureCollector(fixture).collect()
        if observation.channel_id == "official_updates" and observation.external_post_id == "101"
    )
    clock = FixtureClock(datetime(2026, 7, 29, 12, tzinfo=UTC))
    config = load_config(
        root / "config/channels.toml",
        cli_overrides={"database_path": tmp_path / "unused.sqlite", "output_dir": tmp_path / "output"},
    )

    def evaluate(storage: Storage, observation: SourceObservation, run_key: str) -> tuple[str, str]:
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        pipeline = NewsPipeline(storage, config, FakeGenerationProvider(), clock)
        stage = asyncio.run(pipeline.run((observation,), run_key=run_key, approval_service=service, actor_id=1))
        row = storage.fetch_one(
            "SELECT source_set_key, observation_set_key FROM candidate_evaluations WHERE run_id=?",
            (stage.run_id,),
        )
        return str(row["source_set_key"]), str(row["observation_set_key"])

    with (
        Storage.open(tmp_path / "first.sqlite") as first,
        Storage.open(tmp_path / "second.sqlite") as second,
    ):
        with second.transaction() as connection:
            persist_observation(
                connection,
                replace(base, external_post_id="decoy"),
                clock.now(),
                return_observation_id=True,
            )

        first_material, first_observation = evaluate(first, base, "portable-actual")
        second_material, second_observation = evaluate(second, base, "portable-actual")
        assert (first_material, first_observation) == (second_material, second_observation)

        refreshed = replace(
            base,
            observed_at=clock.now() + timedelta(minutes=1),
            engagement=Engagement(views=(base.engagement.views or 0) + 1),
        )
        refreshed_material, refreshed_observation = evaluate(first, refreshed, "engagement-refresh")
        assert refreshed_material == first_material
        assert refreshed_observation != first_observation

        edited = replace(base, text=base.text + " Material correction.")
        edited_material, edited_observation = evaluate(first, edited, "material-edit")
        assert edited_material != first_material
        assert edited_observation != first_observation

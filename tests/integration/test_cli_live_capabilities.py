from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot.approval.telegram import split_telegram_text
from newsbot.config import Capability, ConfigError, validate_capabilities
from newsbot.copywriting import adaptive_page_count


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


def test_capability_scoped_commands_appear_in_help_without_optional_imports(capsys) -> None:
    from newsbot import cli

    with pytest.raises(SystemExit, match="0"):
        cli.main(["--help"])
    help_text = capsys.readouterr().out
    assert "auth-telethon" in help_text
    assert "rank" in help_text
    assert "reconcile" in help_text


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
        enabled_channels=(SimpleNamespace(id="aipost", handle="aipost"),),
    )
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_: None)
    monkeypatch.setattr(cli, "_config", lambda _: config)
    monkeypatch.setattr(cli, "SessionStore", lambda _: SimpleNamespace(validate=lambda: tmp_path / "session"))
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))
    monkeypatch.setattr(cli, "_live_collector", lambda *_: (object(), lambda: None))

    def fail_reconcile(*_: object, **__: object) -> int:
        raise RuntimeError("targeted reconciliation failed")

    monkeypatch.setattr(cli.DurableCollection, "reconcile_channel", fail_reconcile)

    with pytest.raises(SystemExit) as error:
        cli.main(["reconcile-live", "--channel", "aipost", "--lookback-hours", "24"])

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
        cli.main(["reconcile", "--fixture", "ignored.json", "--channel", "aipost"])
    with pytest.raises(SystemExit, match="2"):
        cli.main(
            [
                "reconcile",
                "--fixture",
                "ignored.json",
                "--channel",
                "aipost",
                "--from-id",
                "10",
                "--to-id",
                "20",
                "--lookback-hours",
                "24",
            ]
        )


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
    from newsbot.storage import Storage

    now = datetime.now(UTC) - timedelta(minutes=1)
    loops: list[asyncio.AbstractEventLoop] = []

    class Message:
        id = 10
        date = now
        message = "AI technology update with enough material detail for deterministic local candidate ranking."
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
    assert Path(result["digest_path"]).is_file()
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
                        "channel_id": "aipost",
                        "channel_handle": "aipost",
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
            observation = collector.collect(SimpleNamespace(id="aipost", handle="aipost"))[0]
            with storage.transaction() as connection:
                persist_observation(connection, observation, datetime(2026, 7, 29, 12, tzinfo=UTC))
        latest = storage.latest_observations()
        bound = DurableLivePipeline(
            storage,
            SimpleNamespace(),
            tmp_path,
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
    assert bound == {("aipost", "7"): snapshots[-1]["id"]}


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
        """{"messages":[{"channel_id":"aipost","channel_handle":"aipost","id":"1","published_at":"2026-07-29T11:00:00Z","text":"AI technology update includes enough independently useful context for the editorial policy to evaluate this original publisher announcement.","urls":["https://aipost.kr/news"]}]}""",
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
        pipeline = NewsPipeline(storage, config, output, FakeGenerationProvider(), clock)
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        stage = asyncio.run(pipeline.run_fixture(FixtureCollector(fixture), approval_service=service, actor_id=1))
        candidate_id = int(stage.digest.candidates[0]["candidate_id"])
        make = next(button for button in stage.digest.buttons[candidate_id] if button.label == "[제작]")
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
        if observation.channel_id == "exilist_official" and observation.external_post_id == "101"
    )
    clock = FixtureClock(datetime(2026, 7, 29, 12, tzinfo=UTC))
    config = load_config(
        root / "config/channels.toml",
        cli_overrides={"database_path": tmp_path / "unused.sqlite", "output_dir": tmp_path / "output"},
    )

    def evaluate(storage: Storage, observation: SourceObservation, run_key: str) -> tuple[str, str]:
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        pipeline = NewsPipeline(storage, config, tmp_path / "output", FakeGenerationProvider(), clock)
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

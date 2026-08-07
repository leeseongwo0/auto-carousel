import asyncio
import json
import socket
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot import automation, cli
from newsbot.ai.fake import FakeGenerationProvider
from newsbot.approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from newsbot.candidates import CandidateApprovalService
from newsbot.cli import main
from newsbot.collectors.fixture import FixtureCollector
from newsbot.config import load_config
from newsbot.observability import status
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


def test_fixture_run_is_offline_and_idempotent(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    @contextmanager
    def no_lock(*_args, **_kwargs):
        yield

    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.delenv("TELEGRAM_API_ID", raising=False)
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        """{"messages":[{"channel_id":"official","channel_handle":"official","id":"1","published_at":"2026-07-29T11:00:00Z","text":"Official team announced a major AI technology release with independently useful product details, rollout scope, supported users, measured impact, and operational context for readers.","urls":["https://official.example/news"]}]}""",
        encoding="utf-8",
    )
    config = SimpleNamespace(
        digest="fixture-config",
        channels_by_id={
            "official": SimpleNamespace(
                source_quality=1, classification="official", official_domains=("official.example",), original_domains=()
            )
        },
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
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        clock = FixtureClock(datetime(2026, 7, 29, 12, tzinfo=UTC))
        pipeline = NewsPipeline(storage, config, FakeGenerationProvider(), clock)
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)

        first = asyncio.run(pipeline.run_fixture(FixtureCollector(fixture), approval_service=service, actor_id=1))
        second = asyncio.run(pipeline.run_fixture(FixtureCollector(fixture), approval_service=service, actor_id=1))

        assert first.selection_digest is not None
        assert second.selection_digest is not None
        assert second.selection_digest.id == first.selection_digest.id
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM generations")["count"] == 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM export_outbox")["count"] == 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM pipeline_events")["count"] == 0
        assert status(storage)["provider_calls"] == 0

        candidate_id = int(first.selection_digest.candidates[0]["candidate_id"])
        make = next(button for button in first.selection_digest.buttons[candidate_id] if button.label == "[제작]")
        adapter = ScriptedApprovalAdapter(service)
        assert adapter.apply(ScriptedAction(make.token, 1, 1)).status == "queued"
        generated = asyncio.run(pipeline.generate_selected(candidate_id, page_count=2))

        metrics = status(storage)
        assert metrics["provider_calls"] == 1
        assert metrics["provider_calls_before_selection"] == 0
        event = storage.fetch_one(
            "SELECT event.selection_id, event.generation_job_id, event.candidate_id, "
            "job.selection_id AS job_selection_id, selection.candidate_id AS selection_candidate_id "
            "FROM pipeline_events event "
            "JOIN generation_jobs job ON job.id=event.generation_job_id "
            "JOIN selections selection ON selection.id=event.selection_id"
        )
        assert event is not None
        assert int(event["candidate_id"]) == candidate_id
        assert event["selection_id"] == event["job_selection_id"]
        assert event["candidate_id"] == event["selection_candidate_id"]
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM export_outbox")["count"] == 0
        review = next(
            button
            for button in service.review_buttons(
                candidate_id,
                generated.generation_id,
                actor_id=1,
                source_version_ids=generated.source_version_ids,
            )
            if button.action.value == "reject"
        )
        assert adapter.apply(ScriptedAction(review.token, 1, 1)).status == "rejected"
        with pytest.raises(ValueError, match="not eligible for generation"):
            asyncio.run(pipeline.generate_selected(candidate_id, page_count=2))
        monkeypatch.setattr(cli, "validate_capabilities", lambda _: None)
        with pytest.raises(ValueError, match="current review draft"):
            cli.notify_review(
                SimpleNamespace(
                    db=tmp_path / "newsbot.sqlite",
                    candidate_id=candidate_id,
                    generation_id=generated.generation_id,
                    actor_id=1,
                )
            )


def test_scripted_fixture_rerun_reuses_ready_export(tmp_path, capsys, monkeypatch):
    original_socket = socket.socket

    def deny_network_socket(family=socket.AF_INET, *args: object, **kwargs: object):
        if family in (socket.AF_INET, socket.AF_INET6):
            raise AssertionError("fixture mode must not open network sockets")
        return original_socket(family, *args, **kwargs)

    monkeypatch.setattr(socket, "socket", deny_network_socket)

    @contextmanager
    def no_lock(*_args, **_kwargs):
        yield

    monkeypatch.setattr(automation, "automation_lock", no_lock)
    root = Path(__file__).parents[2]
    fixture = root / "tests/fixtures/channel_messages.json"
    database = tmp_path / "newsbot.sqlite"
    arguments = [
        "run-fixture",
        "--config",
        str(root / "config/channels.toml"),
        "--fixture",
        str(fixture),
        "--db",
        str(database),
        "--scripted-approve",
    ]
    before_files = set(tmp_path.iterdir())

    assert main(arguments) == 0
    first = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert set(tmp_path.iterdir()) - before_files == {database}
    with Storage.open(database) as storage:
        counts_before_rerun = {
            table: storage.fetch_one(f"SELECT COUNT(*) AS count FROM {table}")["count"]
            for table in (
                "candidates",
                "digests",
                "callback_tokens",
                "selections",
                "generation_jobs",
                "generations",
                "sheet_handoffs",
                "pipeline_events",
            )
        }
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM source_post_observations")["count"] > 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM collection_cursors")["count"] == 6
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM collection_intervals")["count"] == 0

    assert main(arguments) == 0
    repeated = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert repeated["status"] == "approved"
    assert repeated["reused"] is True
    assert repeated["handoff_id"] == first["handoff_id"]
    assert repeated["candidate_count"] == first["candidate_count"]
    assert repeated["digest_id"] == first["digest_id"]
    assert repeated["run_id"] == first["run_id"]
    with Storage.open(database) as storage:
        assert {
            table: storage.fetch_one(f"SELECT COUNT(*) AS count FROM {table}")["count"] for table in counts_before_rerun
        } == counts_before_rerun

    alternate_fixture = tmp_path / "alternate-fixture.json"
    alternate_fixture.write_bytes(fixture.read_bytes() + b"\n")
    alternate_arguments = list(arguments)
    alternate_arguments[4] = str(alternate_fixture)
    with pytest.raises(SystemExit):
        main(alternate_arguments)
    assert "fixture has no immediate candidate" in capsys.readouterr().err

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from newsbot.ai.fake import FakeGenerationProvider
from newsbot.approval.telegram import split_telegram_titles
from newsbot.automation import AutomationAuthority, AutomationDriftError, CutoverProposal, Frontier
from newsbot.candidates import CandidateApprovalService
from newsbot.cli import _notification_payload
from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.config import load_config
from newsbot.news_policy import NewsOutcome
from newsbot.observability import status
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage

NOW = datetime(2026, 8, 2, 12, tzinfo=UTC)
SEOUL = ZoneInfo("Asia/Seoul")


class StaticCollector:
    """Credential-free collector whose observations are fixed by the test."""

    def __init__(self, observations: tuple[SourceObservation, ...]) -> None:
        self.observations = observations
        self.calls = 0

    def collect(self) -> tuple[SourceObservation, ...]:
        self.calls += 1
        return self.observations


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def _activate_hourly(storage: Storage, config: object) -> AutomationAuthority:
    authority = AutomationAuthority(storage)
    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO sheet_target_bindings(id,target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint) "
            "VALUES(1,?,'workplace-template-v1',0,'workplace',?)",
            (_digest("target-ref"), _digest("target")),
        )
        connection.execute(
            "INSERT INTO sheet_bootstraps(target_binding_id,marker_value,controls_fingerprint,status,verified_at) "
            "VALUES(1,'marker',?,'ready',?)",
            (_digest("controls"), NOW.isoformat()),
        )
    proposal = CutoverProposal(
        proposal_id="hourly-noon-e2e-1",
        config_digest=_digest("config"),
        cursor_digest=_digest("cursors"),
        intervals_digest=_digest("intervals"),
        maxima=(0, 0, 0, 0, 0),
        approval_offset=0,
        target_id=1,
        target_fingerprint=_digest("target-ref"),
        release_digest=_digest("release"),
        audience_digest=_digest("1"),
        frontiers=tuple(Frontier(_digest(channel.id), 0, NOW) for channel in config.channels),
    )
    audience_binding_id = authority.record_audience_binding(
        bot_id_digest=_digest("bot"),
        token_hmac=_digest("token"),
        audience_hmac=_digest("audience"),
        version=1,
    )
    receipt = authority.persist_proposal(proposal, now=NOW)
    assert authority.apply_proposal(
        proposal.proposal_id,
        receipt,
        audience_binding_id=audience_binding_id,
        release_digest=_digest("release"),
        now=NOW,
        validate=lambda: True,
    ) == {"changed": True, "status": "active"}
    authority.activate_release(_digest("hourly-release"), config=config, now=NOW, validate=lambda: True)
    return authority


def _observation(
    external_id: int,
    channel_id: str,
    text: str,
    *,
    url: str | None = None,
) -> SourceObservation:
    return SourceObservation(
        channel_id=channel_id,
        channel_handle=channel_id,
        external_post_id=str(external_id),
        published_at=datetime(2026, 8, 3, 2, 15, tzinfo=UTC),
        text=text,
        urls=() if url is None else (UrlCandidate(url),),
    )


def test_active_release_routes_mixed_batch_and_seals_noon_without_callbacks(tmp_path: Path) -> None:
    config = load_config(Path("config/channels.toml"), environ={})
    event = (
        "AI launch announced. "
        + "The release adds reliable production capabilities for teams with operational detail. " * 2
    )
    trusted = (
        "Analysis of the AI platform. "
        + "The official team explains measured deployment effects, technical constraints, and customer impact in detail. "
        * 2
    )
    evidenced = (
        "Analysis according to official documentation. "
        + "The community report compares measured AI deployment results, constraints, and reproducible operational evidence. "
        * 2
    )
    ambiguous = (
        "Analysis of the AI platform. "
        + "The community compares measured deployment outcomes, technical constraints, and customer impact in detail. "
        * 2
    )
    non_news = "AI tutorial. " + "This instructional material explains routine usage without a current event. " * 2
    ranking_rejected = "AI launch"
    observations = (
        _observation(11, "official_updates", event),
        _observation(12, "official_updates", trusted),
        _observation(13, "community_feed", evidenced, url="https://evidence.example/report"),
        _observation(14, "community_feed", ambiguous),
        _observation(15, "community_feed", non_news),
        _observation(16, "community_feed", ranking_rejected),
    )

    with Storage.open(tmp_path / "hourly.sqlite") as storage:
        authority = _activate_hourly(storage, config)
        clock = FixtureClock(datetime(2026, 8, 3, 2, 30, tzinfo=UTC))
        provider = FakeGenerationProvider()
        pipeline = NewsPipeline(storage, config, provider, clock)
        service = CandidateApprovalService(storage, chat_id=7, authorized_user_ids={7}, now=clock.now)
        collector = StaticCollector(observations)

        stage = asyncio.run(pipeline.run_fixture(collector, approval_service=service, actor_id=7))
        replayed = asyncio.run(pipeline.run_fixture(collector, approval_service=service, actor_id=7))

        assert collector.calls == 2
        assert stage.selection_digest is not None
        assert len(stage.selection_digest.candidates) == 3
        assert replayed.selection_digest is not None
        assert replayed.selection_digest.id == stage.selection_digest.id
        assert replayed.selection_digest.candidates == stage.selection_digest.candidates
        assert set(replayed.selection_digest.buttons) == {
            int(candidate["candidate_id"]) for candidate in stage.selection_digest.candidates
        }
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM generations")["count"] == 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM generation_jobs")["count"] == 0
        assert status(storage)["provider_calls"] == 0
        assert (
            storage.fetch_one("SELECT COUNT(*) AS count FROM candidates WHERE status='pending_selection'")["count"] == 3
        )
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM candidates WHERE status='rejected'")["count"] == 3
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox WHERE notification_kind='candidate'"
            )["count"]
            == 3
        )
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox WHERE notification_kind='noon_digest'"
            )["count"]
            == 0
        )
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox outbox "
                "JOIN candidates candidate ON candidate.id=outbox.candidate_id "
                "WHERE outbox.notification_kind='candidate' AND candidate.status='rejected'"
            )["count"]
            == 0
        )
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS count FROM callback_tokens token "
                "JOIN candidates candidate ON candidate.id=CAST(json_extract(token.payload_json,'$.candidate_id') AS INTEGER) "
                "WHERE candidate.status='rejected'"
            )["count"]
            == 0
        )
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM callback_tokens")["count"] > 0
        assert {
            row["outcome"]: row["count"]
            for row in storage.fetch_all(
                "SELECT outcome,COUNT(*) AS count FROM news_policy_evaluations GROUP BY outcome"
            )
        } == {
            NewsOutcome.DEFINITE_NEWS.value: 2,
            NewsOutcome.TRUSTED_ANALYSIS.value: 1,
            NewsOutcome.AMBIGUOUS.value: 1,
            NewsOutcome.NON_NEWS.value: 2,
        }
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS count FROM news_policy_evaluations WHERE reason='ranking_ineligible'"
            )["count"]
            == 1
        )
        ambiguous_evidence = storage.fetch_one(
            "SELECT rationale_json FROM news_policy_evaluations WHERE outcome='ambiguous'"
        )
        assert ambiguous_evidence is not None
        rationale = json.loads(str(ambiguous_evidence["rationale_json"]))
        assert rationale["schema_version"] == "news-policy-rationale-v1"
        assert rationale["selected_source_id"] == "community_feed"
        selected = next(item for item in rationale["observations"] if item["external_post_id"] == "14")
        assert selected["classification"] == "community"
        assert selected["meaningful_analysis"] is True
        assert selected["eligible_external_url"] is False
        analysis_match = next(match for match in selected["marker_matches"] if match["category"] == "analysis")
        assert (
            ambiguous[analysis_match["start"] : analysis_match["end"]].casefold() == analysis_match["marker"].casefold()
        )

        callback_count = int(storage.fetch_one("SELECT COUNT(*) AS count FROM callback_tokens")["count"])
        authority.seal_noon_window(config, now=datetime(2026, 8, 3, 12, 59, 59, tzinfo=SEOUL))
        noon = storage.fetch_one(
            "SELECT id,created_at FROM telegram_notification_outbox WHERE notification_kind='noon_digest'"
        )
        assert noon is not None
        assert noon["created_at"] == "2026-08-03T03:59:59+00:00"
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM callback_tokens")["count"] == callback_count
        payload, markup = _notification_payload(storage, service, int(noon["id"]), actor_id=7)
        assert markup is None
        assert (
            payload
            == storage.fetch_one("SELECT normalized_title FROM ambiguous_digest_items ORDER BY id")["normalized_title"]
        )
        assert "제목:" not in payload
        assert "출처:" not in payload
        assert split_telegram_titles((payload,)) == (payload,)

        authority.seal_noon_window(config, now=datetime(2026, 8, 4, 13, 0, tzinfo=SEOUL))
        assert (
            storage.fetch_one("SELECT state FROM ambiguous_digest_windows WHERE scheduled_local_date='2026-08-04'")[
                "state"
            ]
            == "skipped"
        )
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox WHERE notification_kind='noon_digest'"
            )["count"]
            == 1
        )


def test_active_release_refuses_config_drift_before_pipeline_mutation(tmp_path: Path) -> None:
    config = load_config(Path("config/channels.toml"), environ={})
    drifted = replace(config, news_policy=replace(config.news_policy, version="drifted-policy"))
    with Storage.open(tmp_path / "drift.sqlite") as storage:
        _activate_hourly(storage, config)
        clock = FixtureClock(datetime(2026, 8, 3, 2, 30, tzinfo=UTC))
        pipeline = NewsPipeline(storage, drifted, FakeGenerationProvider(), clock)
        service = CandidateApprovalService(storage, chat_id=7, authorized_user_ids={7}, now=clock.now)

        with pytest.raises(AutomationDriftError, match="binding drifted"):
            asyncio.run(
                pipeline.run_fixture(
                    StaticCollector((_observation(99, "community_feed", "AI launch announced."),)),
                    approval_service=service,
                    actor_id=7,
                )
            )
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM runs")["count"] == 0
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM source_posts")["count"] == 0


def test_terminal_prior_binding_noon_history_is_noop_after_release_activation(tmp_path: Path) -> None:
    config = load_config(Path("config/channels.toml"), environ={})
    with Storage.open(tmp_path / "terminal-history.sqlite") as storage:
        authority = _activate_hourly(storage, config)
        authority.seal_noon_window(config, now=datetime(2026, 8, 3, 12, 30, tzinfo=SEOUL))
        original = storage.fetch_one(
            "SELECT state,config_binding_id FROM ambiguous_digest_windows WHERE scheduled_local_date='2026-08-03'"
        )
        assert original is not None and original["state"] == "empty"

        authority.activate_release(
            _digest("second-hourly-release"),
            config=config,
            now=NOW + timedelta(seconds=1),
            validate=lambda: True,
        )
        current = storage.fetch_one(
            "SELECT binding.id FROM automation_release_activations activation "
            "JOIN automation_release_config_bindings binding ON binding.activation_id=activation.id "
            "ORDER BY activation.id DESC LIMIT 1"
        )
        assert current is not None
        assert int(current["id"]) != int(original["config_binding_id"])

        authority.seal_noon_window(config, now=datetime(2026, 8, 3, 14, 0, tzinfo=SEOUL))
        assert (
            storage.fetch_one("SELECT state FROM ambiguous_digest_windows WHERE scheduled_local_date='2026-08-03'")[
                "state"
            ]
            == "empty"
        )


def test_noon_payload_rejects_forged_subject_before_chunking(tmp_path: Path) -> None:
    config = load_config(Path("config/channels.toml"), environ={})
    ambiguous = (
        "Analysis of the AI platform. "
        + "The community compares measured deployment outcomes, technical constraints, and customer impact in detail. "
        * 2
    )
    with Storage.open(tmp_path / "forged-noon.sqlite") as storage:
        _activate_hourly(storage, config)
        clock = FixtureClock(datetime(2026, 8, 3, 2, 30, tzinfo=UTC))
        service = CandidateApprovalService(storage, chat_id=7, authorized_user_ids={7}, now=clock.now)
        asyncio.run(
            NewsPipeline(storage, config, FakeGenerationProvider(), clock).run_fixture(
                StaticCollector((_observation(14, "community_feed", ambiguous),)),
                approval_service=service,
                actor_id=7,
            )
        )
        window = storage.fetch_one("SELECT id FROM ambiguous_digest_windows WHERE scheduled_local_date='2026-08-03'")
        audience = storage.fetch_one("SELECT audience_binding_id FROM automation_cutovers WHERE id=1")
        assert window is not None and audience is not None
        with storage.transaction() as connection:
            connection.execute(
                "UPDATE ambiguous_digest_windows SET state='queued' WHERE id=?",
                (int(window["id"]),),
            )
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,ambiguous_window_id,"
                "subject_digest,state,created_at"
                ") VALUES(?,1,'noon_digest',?,?,'pending',?)",
                (
                    int(audience["audience_binding_id"]),
                    int(window["id"]),
                    _digest("forged-noon-subject"),
                    clock.now().isoformat(),
                ),
            )
            notification_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        with pytest.raises(RuntimeError, match="noon notification binding drift"):
            _notification_payload(storage, service, notification_id, actor_id=7)

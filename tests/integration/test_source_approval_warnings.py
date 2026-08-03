import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from newsbot.ai.fake import FakeGenerationProvider
from newsbot.candidates import CandidateApprovalService
from newsbot.collectors.base import Engagement, SourceObservation
from newsbot.config import load_config
from newsbot.pipeline import NewsPipeline
from newsbot.ranking import evaluate_candidates
from newsbot.runtime import FixtureClock
from newsbot.storage import DurableCollection, Storage, persist_observation

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)
CHANNEL = SimpleNamespace(id="source", handle="source")


def _observation(*, post_id: str = "100", views: int | None = None, text: str | None = None) -> SourceObservation:
    return SourceObservation(
        channel_id="source",
        channel_handle="source",
        external_post_id=post_id,
        published_at=NOW - timedelta(hours=1),
        text=text or "Official launch is reportedly available now with enough material detail for ranking.",
        observed_at=NOW,
        engagement=Engagement(views=views, reactions=1, forwards=0),
        conflicts=("Source conflict",),
    )


def _pipeline_config() -> SimpleNamespace:
    return SimpleNamespace(
        digest="identity-test",
        channels_by_id={"source": SimpleNamespace(source_quality=1, official_domains=(), original_domains=())},
        news_policy=load_config(Path("config/channels.toml"), environ={}).news_policy,
        policy=SimpleNamespace(
            version="identity-test",
            novelty_window_days=7,
            weight_map={
                "source_quality": 0,
                "freshness": 0,
                "engagement": 1,
                "topic_relevance": 0,
                "novelty": 0,
                "official_evidence": 0,
                "certainty": 0,
            },
            engagement_weights={"views": 1, "reactions": 0, "forwards": 0},
            engagement_saturation={"views": 10, "reactions": 10, "forwards": 10},
            topic_positive_phrases={"launch": 1},
            topic_exclusion_phrases={},
            certainty_penalties={"conflicts": ".2", "missing_url": ".1"},
            certainty_markers={"reportedly": ".3"},
            certainty_categories=(("reported", ".3", ("reportedly",)),),
            min_total_score=0,
            min_topic_relevance=0,
        ),
    )


def test_fixture_engagement_snapshot_creates_a_new_reranked_evaluation_without_staling_review(tmp_path):
    first = _observation(post_id="100", views=1, text="Official launch alpha has material detail.")
    second = _observation(post_id="101", views=9, text="Official launch beta has material detail.")
    config = _pipeline_config()
    clock = FixtureClock(NOW)
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        pipeline = NewsPipeline(storage, config, FakeGenerationProvider(), clock)
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        initial = asyncio.run(
            pipeline.run_fixture(SimpleNamespace(collect=lambda: (first, second)), approval_service=service, actor_id=1)
        )
        initial_candidates = storage.fetch_all(
            "SELECT c.id, post.external_post_id, c.rank FROM candidates c "
            "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
            "JOIN candidate_sources cs ON cs.candidate_id=c.id "
            "JOIN source_post_versions version ON version.id=cs.source_post_version_id "
            "JOIN source_posts post ON post.id=version.source_post_id "
            "WHERE ce.run_id=? ORDER BY c.rank",
            (initial.run_id,),
        )
        reviewed_id = int(initial_candidates[0]["id"])
        with storage.transaction() as connection:
            connection.execute("UPDATE candidates SET status='pending_review' WHERE id=?", (reviewed_id,))
        reranked = asyncio.run(
            pipeline.run_fixture(
                SimpleNamespace(
                    collect=lambda: (
                        replace(first, engagement=Engagement(views=9, reactions=1, forwards=0)),
                        replace(second, engagement=Engagement(views=1, reactions=1, forwards=0)),
                    )
                ),
                approval_service=service,
                actor_id=1,
            )
        )
        reranked_candidates = storage.fetch_all(
            "SELECT post.external_post_id FROM candidates c "
            "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
            "JOIN candidate_sources cs ON cs.candidate_id=c.id "
            "JOIN source_post_versions version ON version.id=cs.source_post_version_id "
            "JOIN source_posts post ON post.id=version.source_post_id "
            "WHERE ce.run_id=? ORDER BY c.rank",
            (reranked.run_id,),
        )

        assert (
            storage.fetch_one("SELECT status FROM candidates WHERE id=?", (reviewed_id,))["status"] == "pending_review"
        )
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM source_post_versions")["count"] == 2
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM source_post_observations")["count"] == 4

    assert reranked.run_id != initial.run_id
    assert [row["external_post_id"] for row in initial_candidates] == ["101", "100"]
    assert [row["external_post_id"] for row in reranked_candidates] == ["100", "101"]


def test_material_edit_marks_bound_candidate_stale(tmp_path):
    first = _observation()
    changed = replace(
        first, text="Official launch has corrected material details.", observed_at=NOW + timedelta(minutes=1)
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        with storage.transaction() as connection:
            original_version = persist_observation(connection, first, NOW)
            connection.execute("INSERT INTO runs(run_key, mode, status) VALUES ('material-stale', 'fixture', 'done')")
            connection.execute(
                "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score) "
                "VALUES (1, ?, 'policy', '1.000000')",
                (original_version,),
            )
            evaluation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'pending_review', 1)",
                (evaluation_id,),
            )
            candidate_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
                (candidate_id, original_version),
            )
            changed_version = persist_observation(connection, changed, NOW + timedelta(minutes=1))

        pipeline = NewsPipeline(storage, object(), FakeGenerationProvider(), FixtureClock(NOW))
        pipeline._invalidate_revised_candidates(1, {("source", "100"): changed_version})

        assert changed_version != original_version
        assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "superseded"


def test_normal_overlap_is_anchored_to_the_committed_frontier(tmp_path):
    calls = []

    class Collector:
        def latest_message_id(self, _channel):
            return 200

        def collect(self, _channel, **kwargs):
            calls.append(kwargs)
            return ()

    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO collection_cursors(channel_id, published_at, external_post_id) VALUES (?, ?, ?)",
                (CHANNEL.id, NOW.isoformat(), "100"),
            )
        assert (
            DurableCollection(storage)
            .collect_channel(Collector(), CHANNEL, now=NOW + timedelta(hours=1))
            .cursor_promoted
        )

    assert calls[-1]["min_message_id"] == 0
    assert calls[-1]["max_message_id"] == 100
    assert calls[-1]["lower_bound"] == NOW - timedelta(hours=71)


def test_rich_snapshot_reuses_material_version_and_exposes_real_warning_evidence(tmp_path):
    first = _observation(views=1)
    changed_engagement = _observation(views=2)
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        with storage.transaction() as connection:
            version_one = persist_observation(connection, first, NOW)
            version_two = persist_observation(connection, changed_engagement, NOW + timedelta(minutes=1))
        snapshots = storage.fetch_all("SELECT source_post_version_id FROM source_post_observations ORDER BY id")

    assert version_one == version_two
    assert [row["source_post_version_id"] for row in snapshots] == [version_one, version_one]

    policy = SimpleNamespace(
        version="test",
        weight_map={
            "source_quality": 1,
            "freshness": 1,
            "engagement": 1,
            "topic_relevance": 1,
            "novelty": 1,
            "official_evidence": 1,
            "certainty": 1,
        },
        engagement_weights={"views": 1, "reactions": 0, "forwards": 0},
        engagement_saturation={"views": 10, "reactions": 10, "forwards": 10},
        topic_positive_phrases={"launch": 1},
        topic_exclusion_phrases={},
        certainty_penalties={"conflicts": ".2", "missing_url": ".1"},
        certainty_markers={"reportedly": ".3"},
        certainty_categories=(("reported", ".3", ("reportedly",)),),
        novelty_window_days=7,
    )
    config = SimpleNamespace(
        policy=policy,
        channels_by_id={"source": SimpleNamespace(source_quality=1, official_domains=(), original_domains=())},
    )
    warnings = evaluate_candidates((first,), config, NOW)[0].rationale["warnings"]
    assert {(warning["kind"], warning["reason"]) for warning in warnings} == {
        ("conflict", "source_conflict"),
        ("rumor", "certainty_marker"),
        ("uncertainty", "missing_evidence"),
    }
    assert next(warning for warning in warnings if warning["kind"] == "rumor")["spans"]

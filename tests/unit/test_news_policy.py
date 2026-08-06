from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.config import load_config
from newsbot.news_policy import NewsOutcome, evaluate_news_policy, normalize_match_text, observation_facts

POLICY = load_config(Path("config/channels.toml"), environ={})
NOW = datetime(2026, 8, 3, tzinfo=UTC)


def _observation(channel_id: str, text: str, *, urls: tuple[UrlCandidate, ...] = ()) -> SourceObservation:
    return SourceObservation(channel_id, channel_id, channel_id + text[:8], NOW, text=text, urls=urls)


@pytest.mark.parametrize(
    ("observation", "outcome", "reason"),
    [
        (
            _observation("official_updates", "제품을 출시했습니다. " + "가" * 81),
            NewsOutcome.DEFINITE_NEWS,
            "clean_event",
        ),
        (
            _observation("news_aggregator", "Service outage announced. " + "a" * 81),
            NewsOutcome.DEFINITE_NEWS,
            "clean_event",
        ),
        (
            _observation("official_updates", "분석 " + "가" * 80 + ". " + "나" * 80),
            NewsOutcome.TRUSTED_ANALYSIS,
            "trusted_source_analysis",
        ),
        (
            _observation("news_publisher", "research " + "a" * 80 + ". " + "b" * 80),
            NewsOutcome.TRUSTED_ANALYSIS,
            "trusted_source_analysis",
        ),
        (
            _observation(
                "community_feed", "분석 데이터 " + "가" * 80 + ". " + "나" * 80 + " https://example.com/report"
            ),
            NewsOutcome.DEFINITE_NEWS,
            "evidenced_analysis",
        ),
        (
            _observation("news_aggregator", "report according to " + "a" * 80 + ". " + "b" * 80),
            NewsOutcome.AMBIGUOUS,
            "policy_collision_or_insufficient_evidence",
        ),
        (
            _observation("official_updates", "analysis tutorial " + "a" * 80 + ". " + "b" * 80),
            NewsOutcome.AMBIGUOUS,
            "policy_collision_or_insufficient_evidence",
        ),
        (
            _observation("community_feed", "sponsored product launched " + "a" * 81),
            NewsOutcome.AMBIGUOUS,
            "policy_collision_or_insufficient_evidence",
        ),
        (_observation("community_feed", "사용법 가이드"), NewsOutcome.NON_NEWS, "negative_only"),
        (_observation("community_feed", "giveaway referral opinion"), NewsOutcome.NON_NEWS, "negative_only"),
        (
            _observation("community_feed", "발표 짧음"),
            NewsOutcome.AMBIGUOUS,
            "policy_collision_or_insufficient_evidence",
        ),
        (_observation("community_feed", "unmarked commentary"), NewsOutcome.AMBIGUOUS, "no_decisive_signal"),
    ],
)
def test_frozen_bilingual_policy_matrix(observation: SourceObservation, outcome: NewsOutcome, reason: str) -> None:
    result = evaluate_news_policy((observation,), POLICY, ranking_eligible=True)
    assert (result.outcome, result.reason) == (outcome, reason)


def test_matching_normalization_measurement_and_spans_are_stable() -> None:
    text = "발표\r\n  https://example.com/x @person #topic " + "가" * 81
    display, match = normalize_match_text(text)
    facts = observation_facts(_observation("community_feed", text), POLICY)
    assert display == text.replace("\r\n", "\n")
    assert match.startswith("발표 https://example.com/x")
    assert [(item.category, item.marker, item.start, item.end) for item in facts.matches] == [("event", "발표", 0, 2)]
    assert facts.semantic_chars == 81
    assert facts.material_context


def test_source_local_evidence_and_group_order_are_invariant() -> None:
    evidence = _observation("community_feed", "분석 데이터 " + "가" * 80 + ". " + "나" * 80)
    url = _observation("news_aggregator", "https://example.com/report")
    result = evaluate_news_policy((evidence, url), POLICY, ranking_eligible=True)
    reversed_result = evaluate_news_policy((url, evidence), POLICY, ranking_eligible=True)
    assert (result.outcome, result.reason, result.source_id) == (
        NewsOutcome.AMBIGUOUS,
        "policy_collision_or_insufficient_evidence",
        "community_feed",
    )
    assert reversed_result == result


def test_trusted_source_does_not_authorize_another_observation() -> None:
    official = _observation("official_updates", "metadata")
    community = _observation("community_feed", "analysis " + "a" * 80 + ". " + "b" * 80)
    result = evaluate_news_policy((official, community), POLICY, ranking_eligible=True)
    assert (result.outcome, result.reason) == (NewsOutcome.AMBIGUOUS, "policy_collision_or_insufficient_evidence")


def test_ranking_ineligibility_precedes_all_policy_signals() -> None:
    result = evaluate_news_policy(
        (_observation("official_updates", "출시 " + "가" * 81),), POLICY, ranking_eligible=False
    )
    assert (result.outcome, result.reason, result.source_id) == (NewsOutcome.NON_NEWS, "ranking_ineligible", None)


def test_unconfigured_source_fails_closed() -> None:
    with pytest.raises(ValueError, match="not configured"):
        evaluate_news_policy(
            (_observation("retired_channel", "released " + "x" * 100),),
            POLICY,
            ranking_eligible=True,
        )

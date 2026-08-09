from datetime import UTC, datetime, timedelta

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_policy import V2Outcome, evaluate_v2_policy

NOW = datetime(2026, 8, 9, tzinfo=UTC)


def obs(text: str, *, age: timedelta = timedelta(hours=1), url: bool = True) -> SourceObservation:
    urls = (UrlCandidate("https://publisher.example/story"),) if url else ()
    return SourceObservation("source", "source", "1", NOW - age, text=text, urls=urls)


@pytest.mark.parametrize(
    ("category", "text"),
    [
        ("price_investment", "Bitcoin price surged to a new all time high; technical analysis and target price."),
        ("exchange_token_promotion", "Exchange listing and airdrop points giveaway referral campaign."),
        ("marketing_promotion", "Sponsored paid promotion press release countdown teaser."),
        ("partnership", "The companies announced a strategic partnership and MOU."),
        ("opinion_rumor", "In my opinion, an anonymous source says this is probably a rumor."),
        ("minor_update", "A minor UI design update adds another language and fixes a small bug."),
    ],
)
def test_exclusion_categories_are_non_news(category: str, text: str) -> None:
    result = evaluate_v2_policy(obs("Bitcoin blockchain " + text + " " + "material " * 20), now=NOW)
    assert result.outcome is V2Outcome.NON_NEWS
    assert result.category == category


def test_significant_event_exceptions_precede_price_exclusion() -> None:
    result = evaluate_v2_policy(
        obs("Bitcoin price falls after regulators approved an ETF following a hack. " + "facts " * 20), now=NOW
    )
    assert result.outcome is V2Outcome.CANDIDATE
    assert result.reason == "clear_candidate"


@pytest.mark.parametrize(
    "text",
    [
        "Bitcoin exchange delisting follows a court-ordered market withdrawal. ",
        "Blockchain partnership integrates payments into a service available to customers. ",
    ],
)
def test_delisting_and_concrete_integration_exceptions(text: str) -> None:
    result = evaluate_v2_policy(obs(text + "details " * 20), now=NOW)
    assert result.outcome is V2Outcome.CANDIDATE


def test_important_unconfirmed_rumor_is_ambiguous() -> None:
    result = evaluate_v2_policy(
        obs("Rumor says regulators might ban Bitcoin after a possible security breach. " + "details " * 20), now=NOW
    )
    assert result.outcome is V2Outcome.AMBIGUOUS
    assert result.category == "regulation"


def test_minimum_topic_freshness_body_and_url_gates() -> None:
    base = "A concrete blockchain network event was confirmed with new facts. "
    assert evaluate_v2_policy(obs("A finance event " + "facts " * 20), now=NOW).category == "topic"
    assert evaluate_v2_policy(obs(base, age=timedelta(days=3)), now=NOW).category == "freshness"
    assert evaluate_v2_policy(obs("blockchain short", url=True), now=NOW, material_body_minimum=80).category == "body"
    assert evaluate_v2_policy(obs(base + "facts " * 20, url=False), now=NOW).category == "url"


def test_clear_candidate_requires_all_gates() -> None:
    result = evaluate_v2_policy(
        obs("A blockchain network outage was confirmed and service resumed. " + "details " * 20), now=NOW
    )
    assert result.outcome is V2Outcome.CANDIDATE
    assert result.category is None

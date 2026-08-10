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
        "Bitcoin traders faced $500 million in long-position liquidation after a price crash. ",
        "Crypto 시장에서 비트코인 가격 급락으로 롱 포지션 청산이 발생했고 투자자들은 차트를 분석했다. ",
    ],
)
def test_trading_liquidations_remain_price_investment_exclusions(text: str) -> None:
    result = evaluate_v2_policy(obs(text + "details " * 20), now=NOW)

    assert result.outcome is V2Outcome.NON_NEWS
    assert result.category == "price_investment"


@pytest.mark.parametrize(
    "text",
    [
        "Bitcoin service provider company entered court-supervised liquidation proceedings after insolvency. ",
        "Crypto 블록체인 기업이 법원 명령에 따라 청산 절차를 개시해 서비스를 종료한다. ",
    ],
)
def test_corporate_bankruptcy_and_liquidation_remain_significant_events(text: str) -> None:
    result = evaluate_v2_policy(obs(text + "details " * 20), now=NOW)

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


def test_korean_hype_opinion_is_non_news() -> None:
    text = "AI 업계에서 하이프를 쫓아야 할까, 무시해야 할까? 개인적인 분석과 소감입니다. " + "설명 " * 30
    result = evaluate_v2_policy(obs(text), now=NOW)
    assert result.outcome is V2Outcome.NON_NEWS
    assert result.category == "opinion_rumor"

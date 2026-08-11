from datetime import UTC, datetime, timedelta

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_policy import (
    SourceDisposition,
    V2Outcome,
    V2PolicyInput,
    evaluate_v2_content,
    evaluate_v2_policy,
)

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


def test_analyst_investment_cycle_outlook_is_non_news() -> None:
    text = (
        "🤑메모리 슈퍼사이클은 아직 초기 (Bernstein) source\n"
        "Bernstein은 AI 발전을 네 단계로 구분하며 HBM, DRAM, NAND, HDD 수요가 확산된다고 분석한다. "
        "현재를 고급 추론에서 에이전틱 AI로 넘어가는 구간으로 본다면 메모리 수요의 외연이 더 중요하다. "
        + "투자 사이클 설명 "
        * 20
    )

    result = evaluate_v2_policy(obs(text), now=NOW)

    assert result.outcome is V2Outcome.NON_NEWS
    assert result.category == "opinion_rumor"


@pytest.mark.parametrize(
    "text",
    [
        "An analyst documented that the blockchain company launched its AI payment product to customers today. ",
        "증권사 애널리스트가 블록체인 기업의 AI 결제 제품이 오늘 고객에게 정식 출시된 사실을 확인했다. ",
    ],
)
def test_analyst_attribution_does_not_mask_completed_factual_news(text: str) -> None:
    result = evaluate_v2_policy(obs(text + "verified deployment facts " * 20), now=NOW)

    assert result.outcome is V2Outcome.CANDIDATE
    assert result.reason == "clear_candidate"


def content(text: str, *, body: str | None = None) -> V2PolicyInput:
    return V2PolicyInput(text, NOW - timedelta(hours=1), "https://publisher.example/story", article_body=body)


def test_titles_do_not_satisfy_article_body_gate() -> None:
    result = evaluate_v2_content(content("AI blockchain", body=None), now=NOW)
    assert result == evaluate_v2_content(content("AI blockchain", body="short body"), now=NOW)
    assert result.category == "body"


@pytest.mark.parametrize(
    ("text", "outcome", "reason"),
    [
        (
            "블록체인 기업은 해킹되지 않았으며 서비스 장애도 없다. " + "확인된 사실 " * 30,
            V2Outcome.CANDIDATE,
            "clear_candidate",
        ),
        (
            "블록체인 해킹이 발생했다는 루머가 전해졌다. " + "확인된 사실 " * 30,
            V2Outcome.AMBIGUOUS,
            "important_unconfirmed",
        ),
        (
            "AI 블록체인 기업이 파트너십을 발표했지만 구체적 도입 계획은 없다. " + "설명 " * 30,
            V2Outcome.NON_NEWS,
            "partnership",
        ),
        (
            "AI blockchain partnership integrates payments for named customers today. " + "facts " * 30,
            V2Outcome.CANDIDATE,
            "clear_candidate",
        ),
    ],
)
def test_clause_aware_korean_and_mixed_context(text: str, outcome: V2Outcome, reason: str) -> None:
    result = evaluate_v2_content(content(text), now=NOW)
    assert (result.outcome, result.reason) == (outcome, reason)


def test_trusted_date_conflict_is_ambiguous_before_context_routing() -> None:
    value = V2PolicyInput(
        "AI blockchain outage confirmed. " + "facts " * 20,
        NOW - timedelta(hours=1),
        "https://publisher.example/story",
        source_date_conflict=True,
    )
    assert evaluate_v2_content(value, now=NOW).reason == "date_conflict"


def test_date_conflict_uses_fresh_telegram_date_not_stale_source_date() -> None:
    value = V2PolicyInput(
        "AI blockchain outage confirmed. " + "facts " * 20,
        NOW - timedelta(hours=1),
        "https://publisher.example/story",
        article_body="AI blockchain outage evidence. " + "facts " * 20,
        source_date=NOW - timedelta(days=3),
        source_date_conflict=True,
        source_disposition=SourceDisposition.SUCCESS,
    )
    result = evaluate_v2_content(value, now=NOW)
    assert (result.outcome, result.reason) == (
        V2Outcome.AMBIGUOUS,
        "date_conflict",
    )


@pytest.mark.parametrize(
    "sponsored,text",
    (
        (
            True,
            "AI blockchain company announced a confirmed security breach. ",
        ),
        (
            False,
            "Sponsored AI blockchain partnership deployed payments for named customers. ",
        ),
    ),
)
def test_promotion_and_material_exception_conflicts_are_ambiguous(
    sponsored: bool,
    text: str,
) -> None:
    value = V2PolicyInput(
        text + "verified facts " * 20,
        NOW - timedelta(hours=1),
        "https://publisher.example/story",
        article_body=text + "verified facts " * 20,
        sponsored=sponsored,
        source_disposition=SourceDisposition.SUCCESS,
    )
    result = evaluate_v2_content(value, now=NOW)
    assert (result.outcome, result.reason) == (
        V2Outcome.AMBIGUOUS,
        "context_conflict",
    )


@pytest.mark.parametrize(
    ("disposition", "expected"),
    (
        (
            SourceDisposition.SUCCESS,
            (V2Outcome.AMBIGUOUS, "source_body_insufficient"),
        ),
        (
            SourceDisposition.SOURCE_UNAVAILABLE,
            (V2Outcome.AMBIGUOUS, "source_unavailable"),
        ),
        (
            SourceDisposition.UNSAFE_SOURCE_URL,
            (V2Outcome.NON_NEWS, "unsafe_source_url"),
        ),
        (
            SourceDisposition.NO_ELIGIBLE_URL,
            (V2Outcome.NON_NEWS, "url_gate"),
        ),
    ),
)
def test_source_disposition_body_matrix_is_total(
    disposition: SourceDisposition,
    expected: tuple[V2Outcome, str],
) -> None:
    telegram = "AI blockchain network confirmed a product deployment. " + "facts " * 20
    value = V2PolicyInput(
        telegram,
        NOW - timedelta(hours=1),
        "https://publisher.example/story",
        article_body="tiny heading",
        source_disposition=disposition,
    )
    result = evaluate_v2_content(value, now=NOW)
    assert (result.outcome, result.reason) == expected


def test_korean_topic_boundaries_allow_particles_not_unrelated_compounds() -> None:
    accepted = evaluate_v2_content(
        content("블록체인은 규제기관의 승인을 받았다. " + "사실 " * 30),
        now=NOW,
    )
    rejected = evaluate_v2_content(
        content("이 문서는 블록체인화를 비유로 설명한다. " + "일반 내용 " * 30),
        now=NOW,
    )
    assert accepted.category != "topic"
    assert rejected.category == "topic"


def test_korean_topic_boundaries_allow_stacked_particles() -> None:
    result = evaluate_v2_content(
        content("가상자산에서도 규제기관의 승인이 발표됐다. " + "확인된 사실 " * 30),
        now=NOW,
    )
    assert result.category != "topic"


def test_all_structured_text_fields_are_nfc_normalized() -> None:
    decomposed = "인공지능"
    value = V2PolicyInput(
        "일반 기술 발표 " + "확인된 사실 " * 30,
        NOW - timedelta(hours=1),
        "https://publisher.example/story",
        preview_title=decomposed,
    )
    assert evaluate_v2_content(value, now=NOW).category != "topic"


def test_negation_is_scoped_to_the_nearby_event() -> None:
    result = evaluate_v2_content(
        content("블록체인 서비스 장애는 없었다. 규제기관이 비트코인 ETF를 승인했다. " + "확인된 사실 " * 30),
        now=NOW,
    )
    assert result.outcome is V2Outcome.CANDIDATE
    assert result.reason == "clear_candidate"

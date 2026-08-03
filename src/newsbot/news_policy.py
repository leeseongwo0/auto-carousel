"""Pure, deterministic ``news_policy_v1`` classification."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal

from .collectors.base import SourceObservation
from .dedupe import normalize_outbound_url

NEWS_POLICY_VERSION = "news_policy_v1"


class NewsOutcome(StrEnum):
    DEFINITE_NEWS = "definite_news"
    TRUSTED_ANALYSIS = "trusted_analysis"
    AMBIGUOUS = "ambiguous"
    NON_NEWS = "non_news"


@dataclass(frozen=True, slots=True)
class MarkerMatch:
    category: str
    marker: str
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class NewsPolicyResult:
    outcome: NewsOutcome
    reason: str
    source_id: str | None
    marker_matches: tuple[MarkerMatch, ...]


@dataclass(frozen=True, slots=True)
class ObservationPolicyFacts:
    observation: SourceObservation
    classification: str
    matches: tuple[MarkerMatch, ...]
    semantic_chars: int
    material_sentences: int
    material_context: bool
    meaningful_analysis: bool
    eligible_external_url: bool


def normalize_match_text(text: str) -> tuple[str, str]:
    """Return separately preserved display text and deterministic match copy."""
    display = unicodedata.normalize("NFC", text.replace("\r\n", "\n").replace("\r", "\n"))
    return display, " ".join(display.split()).casefold()


def _markers(policy: object, category: str) -> tuple[tuple[str, bool], ...]:
    return tuple((marker, korean) for korean in (True, False) for marker in getattr(policy, f"{category}_{'ko' if korean else 'en'}"))


def _match_markers(display: str, match: str, policy: object) -> tuple[MarkerMatch, ...]:
    # ``match`` is used for matching; display offsets remain stable for ordinary
    # NFC text.  Mapping collapsed whitespace back to its first display offset
    # keeps spans useful to approval renderers without changing display text.
    offset_map: list[int] = []
    rebuilt: list[str] = []
    in_space = False
    for index, char in enumerate(display):
        if char.isspace():
            if not in_space:
                rebuilt.append(" ")
                offset_map.append(index)
            in_space = True
        else:
            rebuilt.append(char.casefold())
            offset_map.extend([index] * len(char.casefold()))
            in_space = False
    if rebuilt and rebuilt[0] == " ":
        offset_map.pop(0)
        rebuilt.pop(0)
    if rebuilt and rebuilt[-1] == " ":
        offset_map.pop()
        rebuilt.pop()
    normalized = "".join(rebuilt)
    assert normalized == match
    found: list[MarkerMatch] = []
    for category in ("event_markers", "analysis_markers", "evidence_markers", "promotion_markers", "tutorial_markers", "reaction_markers"):
        for marker, korean in _markers(policy, category):
            needle = unicodedata.normalize("NFC", marker).casefold()
            pattern = re.escape(needle) if korean else rf"(?<!\w){re.escape(needle)}(?!\w)"
            for item in re.finditer(pattern, match):
                start = offset_map[item.start()]
                end = offset_map[item.end() - 1] + 1
                found.append(MarkerMatch(category.removesuffix("_markers"), marker, start, end))
    return tuple(sorted(found, key=lambda item: (item.category, item.start, item.end, item.marker)))


def _count_letters_numbers(text: str) -> int:
    return sum(unicodedata.category(char)[0] in {"L", "N"} for char in text)


def _measure(display: str, matches: Iterable[MarkerMatch]) -> tuple[int, tuple[int, ...]]:
    residual = display
    # Remove marker spans before variable-length URL/mention/hashtag substitutions
    # so the display offsets remain valid.
    for marker in sorted(matches, key=lambda item: (item.start, item.end), reverse=True):
        residual = residual[: marker.start] + " " * (marker.end - marker.start) + residual[marker.end :]
    residual = re.sub(r"https?://\S+|@[\w]+|#[\w]+", " ", residual, flags=re.IGNORECASE)
    semantic_chars = _count_letters_numbers(residual)
    sentences = tuple(_count_letters_numbers(sentence) for sentence in re.split(r"[.!?。！？\n]", residual))
    return semantic_chars, sentences


def _classifications(config: object | None) -> Mapping[str, str]:
    if config is None:
        return {}
    channels = getattr(config, "channels", ())
    if channels:
        return {str(channel.id): str(channel.classification) for channel in channels}
    channels_by_id = getattr(config, "channels_by_id", {})
    if isinstance(channels_by_id, Mapping):
        return {
            str(channel_id): str(getattr(channel, "classification", "community"))
            for channel_id, channel in channels_by_id.items()
        }
    return {}


def _policy(config: object) -> object:
    return getattr(config, "news_policy", config)


def observation_facts(observation: SourceObservation, config: object, *, classification: str | None = None) -> ObservationPolicyFacts:
    policy: Any = _policy(config)
    display, match = normalize_match_text(observation.text)
    matches = _match_markers(display, match, policy)
    semantic_chars, sentence_lengths = _measure(display, matches)
    material_sentences = sum(length >= policy.material_sentence_chars for length in sentence_lengths)
    material_context = semantic_chars >= policy.material_semantic_chars and material_sentences >= 1
    categories = {item.category for item in matches}
    meaningful_analysis = (
        "analysis" in categories
        and semantic_chars >= policy.analysis_semantic_chars
        and sum(length >= policy.analysis_sentence_chars for length in sentence_lengths) >= policy.analysis_min_sentences
    )
    urls = [match.group(0) for match in re.finditer(r"https?://\S+", display, flags=re.IGNORECASE)]
    urls.extend(candidate.url for candidate in observation.urls)
    eligible_external_url = any(normalize_outbound_url(url) is not None for url in urls)
    classifications = _classifications(config)
    resolved_classification = classification or classifications.get(observation.channel_id)
    if resolved_classification is None:
        raise ValueError(f"news policy source is not configured: {observation.channel_id}")
    return ObservationPolicyFacts(
        observation=observation,
        classification=resolved_classification,
        matches=matches,
        semantic_chars=semantic_chars,
        material_sentences=material_sentences,
        material_context=material_context,
        meaningful_analysis=meaningful_analysis,
        eligible_external_url=eligible_external_url,
    )


def evaluate_news_policy(
    observations: Sequence[SourceObservation], config: object, *, ranking_eligible: bool
) -> NewsPolicyResult:
    """Apply the acyclic policy table with predicates confined to each source."""
    if not ranking_eligible:
        return NewsPolicyResult(NewsOutcome.NON_NEWS, "ranking_ineligible", None, ())
    facts = tuple(
        sorted(
            (observation_facts(item, config) for item in observations),
            key=lambda fact: (fact.observation.channel_id, fact.observation.external_post_id),
        )
    )

    def has(fact: ObservationPolicyFacts, category: str) -> bool:
        return any(match.category == category for match in fact.matches)

    def negative(fact: ObservationPolicyFacts) -> bool:
        return any(has(fact, category) for category in ("promotion", "tutorial", "reaction"))

    clean = next((fact for fact in facts if has(fact, "event") and fact.material_context and not negative(fact)), None)
    if clean:
        return NewsPolicyResult(NewsOutcome.DEFINITE_NEWS, "clean_event", clean.observation.channel_id, clean.matches)
    trusted = next(
        (fact for fact in facts if fact.classification in {"official", "original_publisher"} and fact.meaningful_analysis and not negative(fact)),
        None,
    )
    if trusted:
        return NewsPolicyResult(NewsOutcome.TRUSTED_ANALYSIS, "trusted_source_analysis", trusted.observation.channel_id, trusted.matches)
    evidenced = next(
        (fact for fact in facts if fact.classification in {"community", "aggregator"} and fact.meaningful_analysis and has(fact, "evidence") and fact.eligible_external_url and not negative(fact)),
        None,
    )
    if evidenced:
        return NewsPolicyResult(NewsOutcome.DEFINITE_NEWS, "evidenced_analysis", evidenced.observation.channel_id, evidenced.matches)
    for fact in facts:
        event = has(fact, "event")
        analysis = fact.meaningful_analysis
        if (event or analysis) and negative(fact):
            return NewsPolicyResult(NewsOutcome.AMBIGUOUS, "policy_collision_or_insufficient_evidence", fact.observation.channel_id, fact.matches)
        if event and not fact.material_context:
            return NewsPolicyResult(NewsOutcome.AMBIGUOUS, "policy_collision_or_insufficient_evidence", fact.observation.channel_id, fact.matches)
        if analysis and fact.classification not in {"official", "original_publisher"} and (not has(fact, "evidence") or not fact.eligible_external_url):
            return NewsPolicyResult(NewsOutcome.AMBIGUOUS, "policy_collision_or_insufficient_evidence", fact.observation.channel_id, fact.matches)
    negative_only = next((fact for fact in facts if negative(fact)), None)
    if negative_only:
        return NewsPolicyResult(NewsOutcome.NON_NEWS, "negative_only", negative_only.observation.channel_id, negative_only.matches)
    return NewsPolicyResult(NewsOutcome.AMBIGUOUS, "no_decisive_signal", None, ())


def classify_news_title(title: str) -> Literal["definite_news", "trusted_analysis", "ambiguous", "non_news"]:
    """Compatibility classifier for callers that only have a title.

    Title-only inputs cannot satisfy material or evidence predicates; they are
    deliberately ambiguous unless a configured negative-only signal is present.
    """

    # This compatibility shim intentionally is not used by the pipeline.  It is
    # kept pure with a small in-memory policy equivalent for legacy title calls.
    class _Policy:
        event_markers_ko = ("출시", "공개", "발표", "업데이트", "지원 시작", "파트너십", "인수", "합병", "투자 유치", "서비스 종료", "장애", "해킹", "승인", "규제", "정책 변경", "가격 인상", "가격 인하")
        event_markers_en = ("launches", "launched", "released", "announced", "unveiled", "now available", "rolls out", "partnership", "acquires", "acquired", "merger", "raises funding", "shuts down", "outage", "breach", "hack", "approved", "regulation", "policy change", "price increase", "price cut")
        analysis_markers_ko = ("분석", "해설", "전망", "리뷰", "비교", "평가", "보고서", "연구")
        analysis_markers_en = ("analysis", "commentary", "outlook", "review", "comparison", "assessment", "report", "research")
        evidence_markers_ko = ("에 따르면", "공식 문서", "원문", "데이터", "통계", "실험", "벤치마크", "조사")
        evidence_markers_en = ("according to", "official documentation", "source document", "data", "statistics", "experiment", "benchmark", "survey")
        promotion_markers_ko = ("광고", "협찬", "할인", "쿠폰", "이벤트 참여", "추천인", "레퍼럴")
        promotion_markers_en = ("ad", "sponsored", "discount", "coupon", "giveaway", "referral", "affiliate", "promo code")
        tutorial_markers_ko = ("튜토리얼", "사용법", "하는 법", "가이드", "초보자")
        tutorial_markers_en = ("tutorial", "how to", "guide", "step by step", "beginner")
        reaction_markers_ko = ("내 생각", "개인 의견", "소감", "반응", "밈")
        reaction_markers_en = ("my take", "opinion", "thoughts", "reaction", "meme")
        material_semantic_chars = 80
        material_sentence_chars = 40
        analysis_semantic_chars = 160
        analysis_sentence_chars = 40
        analysis_min_sentences = 2

    display, match = normalize_match_text(title)
    matches = _match_markers(display, match, _Policy())
    if any(item.category in {"promotion", "tutorial", "reaction"} for item in matches):
        return NewsOutcome.NON_NEWS.value
    return NewsOutcome.AMBIGUOUS.value

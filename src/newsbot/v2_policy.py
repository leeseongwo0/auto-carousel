"""Small deterministic, exclusion-first selection policy for Newsbot V2."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from .collectors.base import SourceObservation
from .dedupe import select_outbound_url


class V2Outcome(StrEnum):
    CANDIDATE = "candidate"
    AMBIGUOUS = "ambiguous"
    NON_NEWS = "non_news"


# Public alias is convenient for callers that do not want the version prefix.
PolicyOutcome = V2Outcome
V2PolicyOutcome = V2Outcome
Outcome = V2Outcome

DEFAULT_TOPIC_MARKERS = (
    "ai",
    "openai",
    "artificial intelligence",
    "machine learning",
    "blockchain",
    "crypto",
    "cryptocurrency",
    "bitcoin",
    "ethereum",
    "web3",
    "defi",
    "token",
)

_SIGNIFICANT = {
    "regulation": r"\b(regulat(?:ion|ory|ed)|sec|lawsuit|legal action|court|ban)\b|규제|법원|소송|금지",
    "security": r"\b(hack(?:ed|ing)?|exploit(?:ed)?|breach|stolen|vulnerability)\b|해킹|취약점|탈취",
    "outage": r"\b(outage|downtime|halt(?:ed)?|shutdown)\b|장애|중단",
    "bankruptcy": r"\b(bankrupt(?:cy|ed)?|insolv(?:ent|ency))\b|파산",
    "etf": r"\b(etf|exchange.traded fund)\b|상장지수펀드",
}
_EXCLUSION_PATTERNS = {
    "price_investment": r"\b(price|prices|all.time high|ath|target price|forecast|long|short|funding rate|liquidat(?:ion|ed)|tvl|market cap|trading volume|whale wallet|token unlock|buy|sell|buying|selling|technical analysis|chart)\b|가격|신고가|급등|급락|목표가|전망|차트|기술적 분석|매수|매도|청산|펀딩비|거래량|고래|언락",
    "exchange_token_promotion": r"\b(list(?:ing|ed)?|delist(?:ing|ed)?|exchange support|airdrop|points program|meme coin|presale|staking reward|defi yield|referral|coupon|giveaway|token sale)\b|상장|상장폐지|에어드롭|포인트|밈코인|프리세일|스테이킹|수익률|레퍼럴|쿠폰|경품",
    "marketing_promotion": r"\b(sponsored|advertis(?:e|ed|ement)|partner content|paid promotion|press release|pre.?order|waitlist|beta sign.?up|countdown|teaser|campaign|event registration)\b|광고|협찬|홍보|보도자료|사전예약|대기자|베타 신청|카운트다운|티저|캠페인|참가 모집",
    "partnership": r"\b(partnership|partnered|memorandum of understanding|\bmou\b|collaborat(?:ion|e)|strategic alliance)\b|파트너십|업무협약|협력",
    "opinion_rumor": r"\b(opinion|i think|probably|might|could|rumou?r|unconfirmed|anonymous source|according to commenters|reaction|hype|investment thesis|outlook|supercycle)\b|의견|전망|루머|미확인|익명|커뮤니티 반응|소감|밈|하이프|쫓아야|무시해야|슈퍼사이클|예상(?:한다|했다)?|본다(?:면)?|판단(?:한다|했다)?",
    "minor_update": r"\b(ui|ux|design|bug fix|minor update|language support|roadmap|rebrand|branding|feature introduction)\b|UI|디자인|버그 수정|언어 지원|로드맵|브랜드명|기능 재소개",
}
_CORPORATE_LIQUIDATION = re.compile(
    r"\b(?:winding up|wind-up|dissolution|dissolved|liquidation proceedings?)\b"
    r"|(?:\b(?:company|companies|firm|corporation|business|entity|issuer)\b.{0,80}\bliquidat(?:ion|ed)\b)"
    r"|(?:\bliquidat(?:ion|ed)\b.{0,80}\b(?:company|companies|firm|corporation|business|entity|issuer)\b)"
    r"|(?:회사|법인|기업).{0,80}청산|청산.{0,80}(?:회사|법인|기업)|청산\s*(?:절차|개시|인|중)|해산",
    re.I,
)


_UNCERTAIN = re.compile(
    r"\b(rumou?r|unconfirmed|might|could|reportedly|allegedly|anonymous source|예고|미확인|루머|전해졌다)\b|루머|미확인",
    re.I,
)
_INTEGRATION = re.compile(
    r"\b(integrat(?:e|ed|ion)|deployed|distribution|available to users|customers?|payments?|data|infrastructure|exclusive|rollout|launch(?:ed)? on)\b|통합|배포|사용자|고객|결제|데이터|인프라|독점|유통|출시",
    re.I,
)


@dataclass(frozen=True, slots=True)
class V2PolicyResult:
    outcome: V2Outcome
    reason: str
    category: str | None = None

    @property
    def eligible(self) -> bool:
        return self.outcome is not V2Outcome.NON_NEWS


def _material_length(text: str) -> int:
    text = re.sub(r"https?://\S+|[@#][\w-]+", " ", text, flags=re.I)
    return sum(unicodedata.category(char)[0] in {"L", "N"} for char in text)


def _matches(text: str, patterns: dict[str, str]) -> list[str]:
    return [category for category, pattern in patterns.items() if re.search(pattern, text, re.I)]


def _significant_events(text: str) -> list[str]:
    significant = _matches(text, _SIGNIFICANT)
    if _CORPORATE_LIQUIDATION.search(text):
        significant.append("bankruptcy")
    return significant


def _has_topic(text: str, markers: Iterable[str]) -> bool:
    for marker in markers:
        normalized = str(marker).strip().casefold()
        if not normalized:
            continue
        if re.fullmatch(r"[a-z0-9]+", normalized):
            if re.search(rf"\b{re.escape(normalized)}\b", text, re.I):
                return True
        elif normalized in text:
            return True
    return False


def evaluate_v2_policy(
    observation: SourceObservation,
    *,
    now: datetime | None = None,
    current_time: datetime | None = None,
    topic_markers: Iterable[str] = DEFAULT_TOPIC_MARKERS,
    freshness_window: timedelta = timedelta(hours=24),
    material_body_minimum: int = 80,
    require_original_url: bool = True,
) -> V2PolicyResult:
    """Evaluate one observation without ranking, network access, or mutable state."""
    if now is not None and current_time is not None:
        raise ValueError("pass only one of now or current_time")
    now = current_time if current_time is not None else now
    current = (now or datetime.now(UTC)).astimezone(UTC)
    published = observation.published_at.astimezone(UTC)
    text = unicodedata.normalize("NFC", observation.text)
    lowered = text.casefold()
    significant = _significant_events(text)
    uncertain = bool(_UNCERTAIN.search(text))
    integration = bool(_INTEGRATION.search(text))
    if not _has_topic(lowered, topic_markers):
        return V2PolicyResult(V2Outcome.NON_NEWS, "topic_gate", "topic")
    age = current - published
    if age < timedelta(0) or age > freshness_window:
        return V2PolicyResult(V2Outcome.NON_NEWS, "freshness_gate", "freshness")
    if _material_length(text) < material_body_minimum:
        return V2PolicyResult(V2Outcome.NON_NEWS, "body_gate", "body")
    url, _ = select_outbound_url(observation)
    if require_original_url and not url:
        return V2PolicyResult(V2Outcome.NON_NEWS, "url_gate", "url")

    # Exceptions are deliberately evaluated before exclusions.
    exception = bool(significant) or (
        "partnership" in _matches(text, {"partnership": _EXCLUSION_PATTERNS["partnership"]}) and integration
    )
    if uncertain and significant:
        exception = False
    exclusions = _matches(text, _EXCLUSION_PATTERNS)
    if observation.sponsored and "marketing_promotion" not in exclusions:
        exclusions.append("marketing_promotion")
    if exclusions and not exception and not (uncertain and significant):
        if "partnership" in exclusions and integration:
            exception = True
        else:
            return V2PolicyResult(V2Outcome.NON_NEWS, exclusions[0], exclusions[0])

    if uncertain:
        return V2PolicyResult(
            V2Outcome.AMBIGUOUS, "important_unconfirmed", significant[0] if significant else "opinion_rumor"
        )
    return V2PolicyResult(V2Outcome.CANDIDATE, "clear_candidate", None)


# Short spelling retained as the primary API for simple integrations.
evaluate_policy = evaluate_v2_policy


class V2Policy:
    """Configurable policy object; useful when evaluating a batch with one clock."""

    def __init__(
        self,
        *,
        now: datetime | None = None,
        topic_markers: Iterable[str] = DEFAULT_TOPIC_MARKERS,
        freshness_window: timedelta = timedelta(hours=24),
        material_body_minimum: int = 80,
        require_original_url: bool = True,
    ) -> None:
        self._now = now
        self._topic_markers = tuple(topic_markers)
        self._freshness_window = freshness_window
        self._material_body_minimum = material_body_minimum
        self._require_original_url = require_original_url

    def evaluate(self, observation: SourceObservation) -> V2PolicyResult:
        return evaluate_v2_policy(
            observation,
            now=self._now,
            topic_markers=self._topic_markers,
            freshness_window=self._freshness_window,
            material_body_minimum=self._material_body_minimum,
            require_original_url=self._require_original_url,
        )

"""Pure deterministic V2 news policy, including Korean and mixed-language context."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from .collectors.base import SourceObservation
from .v2_article import material_character_count


class V2Outcome(StrEnum):
    CANDIDATE = "candidate"
    AMBIGUOUS = "ambiguous"
    NON_NEWS = "non_news"


class SourceDisposition(StrEnum):
    TELEGRAM_ONLY = "telegram_only"
    SUCCESS = "success"
    SOURCE_UNAVAILABLE = "source_unavailable"
    UNSAFE_SOURCE_URL = "unsafe_source_url"
    NO_ELIGIBLE_URL = "no_eligible_url"


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
    "인공지능",
    "생성형 AI",
    "블록체인",
    "암호화폐",
    "가상자산",
    "비트코인",
    "이더리움",
    "디파이",
    "토큰",
)

_SIGNIFICANT = {
    "regulation": r"\b(regulat(?:e[sd]?|ing|ion|ory)|sec|lawsuit|legal action|court|ban(?:ned|s|ning)?)\b|규제|법원|소송|금지",
    "security": r"\b(hack(?:ed|ing|s)?|exploit(?:ed|ing|s)?|breach(?:ed|es)?|stolen|vulnerabilit(?:y|ies))\b|해킹|취약점|탈취",
    "outage": r"\b(outage|downtime|halt(?:ed|ing|s)?|shutdown)\b|장애|중단",
    "bankruptcy": r"\b(bankrupt(?:cy|cies|ed)|insolv(?:ent|ency))\b|파산",
    "etf": r"\b(etf|exchange.traded fund)\b|상장지수펀드",
}
_EXCLUSIONS = {
    "price_investment": r"\b(price|prices|all.time high|ath|target price|forecast|long|short|funding rate|liquidat(?:ion|ed)|tvl|market cap|trading volume|whale wallet|token unlock|buy|sell|buying|selling|technical analysis|chart)\b|가격|신고가|급등|급락|목표가|전망|차트|기술적 분석|매수|매도|청산|펀딩비|거래량|고래|언락",
    "exchange_token_promotion": r"\b(list(?:ing|ed)?|delist(?:ing|ed)?|exchange support|airdrop|points program|meme coin|presale|staking reward|defi yield|referral|coupon|giveaway|token sale)\b|상장|상장폐지|에어드롭|포인트|밈코인|프리세일|스테이킹|수익률|레퍼럴|쿠폰|경품",
    "marketing_promotion": r"\b(sponsored|advertis(?:e|ed|ement)|partner content|paid promotion|press release|pre.?order|waitlist|beta sign.?up|countdown|teaser|campaign|event registration)\b|광고|협찬|홍보|보도자료|사전예약|대기자|베타 신청|카운트다운|티저|캠페인|참가 모집",
    "partnership": r"\b(partnership|partnered|memorandum of understanding|\bmou\b|collaborat(?:ion|e)|strategic alliance)\b|파트너십|업무협약|협력",
    "opinion_rumor": r"\b(opinion|i think|probably|might|could|rumou?r|unconfirmed|anonymous source|according to commenters|reaction|hype|investment thesis|outlook|supercycle)\b|의견|전망|루머|미확인|익명|커뮤니티 반응|소감|밈|하이프|쫓아야|무시해야|슈퍼사이클|예상(?:한다|했다)?|본다(?:면)?|판단(?:한다|했다)?",
    "minor_update": r"\b(ui|ux|design|bug fix|minor update|language support|roadmap|rebrand|branding|feature introduction)\b|UI|디자인|버그 수정|언어 지원|로드맵|브랜드명|기능 재소개",
}
_CORPORATE_LIQUIDATION = re.compile(
    r"\b(?:winding up|wind-up|dissolution|dissolved|liquidation proceedings?)\b|(?:\b(?:company|companies|firm|corporation|business|entity|issuer)\b.{0,80}\bliquidat(?:ion|ed)\b)|(?:\bliquidat(?:ion|ed)\b.{0,80}\b(?:company|companies|firm|corporation|business|entity|issuer)\b)|(?:회사|법인|기업).{0,80}청산|청산.{0,80}(?:회사|법인|기업)|청산\s*(?:절차|개시|인|중)|해산",
    re.I,
)
_ATTRIBUTION = re.compile(
    r"\b(rumou?r|unconfirmed|might|could|reportedly|allegedly|anonymous source)\b|루머|미확인|전해졌다|관계자에 따르면|보도에 따르면",
    re.I,
)
_NEGATION = re.compile(r"\b(no|not|never|without|den(?:y|ied))\b|아니|않|없|미발생|부인|무산", re.I)
_MATERIAL_PARTNERSHIP = re.compile(
    r"\b(integrat(?:e|ed|ion)|deployed|distribution|available to users|customers?|payments?|data|infrastructure|exclusive|rollout|launch(?:ed)? on|named users?)\b|통합|배포|사용자|고객|결제|데이터|인프라|독점|유통|출시|도입|계약",
    re.I,
)
_EVENT_SUBJECT = re.compile(
    r"\b(?:ai|openai|bitcoin|ethereum|blockchain|crypto|cryptocurrency|"
    r"token|network|protocol|exchange|company|firm|issuer|regulator|"
    r"agency|court|government|service|provider|users?|customers?)\b|"
    r"인공지능|블록체인|암호화폐|가상자산|비트코인|이더리움|토큰|"
    r"네트워크|프로토콜|거래소|기업|회사|법인|발행사|규제기관|"
    r"정부|법원|서비스|사용자|고객",
    re.I,
)
_EVENT_ACTION = re.compile(
    r"\b(?:announce(?:d|s|ing)?|approve(?:d|s|ing)?|fil(?:e|ed|es|ing)|"
    r"order(?:ed|s|ing)?|confirm(?:ed|s|ing)?|occur(?:red|s|ring)?|"
    r"suffer(?:ed|s|ing)?|resume(?:d|s|ing)?|halt(?:ed|s|ing)?|"
    r"ban(?:ned|s|ning)?|launch(?:ed|es|ing)?|deploy(?:ed|s|ing)?|"
    r"breach(?:ed|es|ing)?|hack(?:ed|s|ing)?|exploit(?:ed|s|ing)?|"
    r"steal(?:s|ing)?|stolen|enter(?:ed|s|ing)?|begin(?:s|ning)?|began|"
    r"open(?:ed|s|ing)?|close(?:d|s|ing)?|liquidat(?:ion|ed|es|ing)|"
    r"dissolv(?:e|ed|es|ing)|insolv(?:ent|ency)|bankrupt(?:cy|cies|ed))\b|"
    r"발표|승인|제소|명령|확인|발생|재개|중단|금지|출시|배포|"
    r"해킹|침해|탈취|개시|청산|해산|파산",
    re.I,
)
_KOREAN_PARTICLES = (
    "은",
    "는",
    "이",
    "가",
    "을",
    "를",
    "의",
    "과",
    "와",
    "에서",
    "으로",
    "로",
    "도",
    "에",
    "만",
)


@dataclass(frozen=True, slots=True)
class V2PolicyInput:
    telegram_text: str
    telegram_date: datetime
    display_url: str | None
    preview_title: str | None = None
    preview_description: str | None = None
    article_title: str | None = None
    article_body: str | None = None
    source_date: datetime | None = None
    source_date_conflict: bool = False
    sponsored: bool = False
    source_disposition: SourceDisposition = SourceDisposition.TELEGRAM_ONLY


@dataclass(frozen=True, slots=True)
class V2PolicyResult:
    outcome: V2Outcome
    reason: str
    category: str | None = None

    @property
    def eligible(self) -> bool:
        return self.outcome is not V2Outcome.NON_NEWS


def _clauses(text: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in re.split(r"(?:[.!?]|\n|;|지만|그러나|다만)+", text) if part.strip())


def _has_topic(text: str, markers: Iterable[str]) -> bool:
    for raw_marker in markers:
        marker = str(raw_marker).strip().casefold()
        if not marker:
            continue
        if re.fullmatch(r"[a-z0-9]+", marker):
            if re.search(rf"(?<![a-z0-9]){re.escape(marker)}(?![a-z0-9])", text, re.I):
                return True
            continue
        if re.search(r"[가-힣]", marker):
            particles = "|".join(map(re.escape, _KOREAN_PARTICLES))
            if re.search(
                rf"(?<![가-힣]){re.escape(marker)}"
                rf"(?=(?:(?:{particles})){{0,3}}(?:$|[^가-힣]))",
                text,
                re.I,
            ):
                return True
            continue
        if marker in text:
            return True
    return False


def _matches(text: str, patterns: dict[str, str]) -> list[str]:
    return [name for name, pattern in patterns.items() if re.search(pattern, text, re.I)]


def _event_is_negated(clause: str, match: re.Match[str]) -> bool:
    prefix = clause[max(0, match.start() - 24) : match.start()]
    suffix = clause[match.end() : match.end() + 16]
    return bool(_NEGATION.search(prefix) or _NEGATION.search(suffix))


def _significant_events(
    clause: str,
    nearby: str,
) -> list[str]:
    if not _EVENT_SUBJECT.search(nearby):
        return []
    categories: list[str] = []
    for category, pattern in _SIGNIFICANT.items():
        for match in re.finditer(pattern, clause, re.I):
            if not _event_is_negated(clause, match) and _EVENT_ACTION.search(nearby):
                categories.append(category)
                break
    liquidation = _CORPORATE_LIQUIDATION.search(clause)
    if liquidation is not None and not _event_is_negated(clause, liquidation) and _EVENT_ACTION.search(nearby):
        categories.append("bankruptcy")
    return categories


def _context(text: str) -> tuple[list[str], bool, bool]:
    """Return significant categories, attribution, and material partnership."""
    significant: list[str] = []
    attributed = False
    partnership_material = False
    clauses = _clauses(text)
    for index, clause in enumerate(clauses):
        nearby = " ".join(clauses[max(0, index - 1) : index + 2])
        events = _significant_events(clause, nearby)
        if events:
            significant.extend(events)
            attributed |= bool(_ATTRIBUTION.search(nearby))
        if (
            re.search(_EXCLUSIONS["partnership"], clause, re.I)
            and _MATERIAL_PARTNERSHIP.search(nearby)
            and not _NEGATION.search(nearby)
        ):
            partnership_material = True
    return list(dict.fromkeys(significant)), attributed, partnership_material


def evaluate_v2_content(
    value: V2PolicyInput,
    *,
    now: datetime | None = None,
    topic_markers: Iterable[str] = DEFAULT_TOPIC_MARKERS,
    freshness_window: timedelta = timedelta(hours=24),
    material_body_minimum: int = 80,
    require_original_url: bool = True,
) -> V2PolicyResult:
    """Evaluate structured Telegram/article evidence without network or state access."""
    current = (now or datetime.now(UTC)).astimezone(UTC)
    telegram_date = value.telegram_date.astimezone(UTC)
    normalized_parts = tuple(
        unicodedata.normalize("NFC", part)
        for part in (
            value.telegram_text,
            value.preview_title or "",
            value.preview_description or "",
            value.article_title or "",
            value.article_body or "",
        )
    )
    text, preview_title, preview_description, article_title, article_body = normalized_parts
    telegram_material = material_character_count(text)
    source_material = material_character_count(
        article_body,
        title=article_title,
    )
    context_article_body = (
        ""
        if (value.source_disposition is SourceDisposition.SUCCESS and source_material < material_body_minimum)
        else article_body
    )
    context = "\n".join(
        part
        for part in (
            text,
            preview_title,
            preview_description,
            article_title,
            context_article_body,
        )
        if part
    ).casefold()
    if not _has_topic(context, topic_markers):
        return V2PolicyResult(V2Outcome.NON_NEWS, "topic_gate", "topic")
    effective_date = (
        telegram_date if value.source_date_conflict or value.source_date is None else value.source_date.astimezone(UTC)
    )
    if effective_date > current or current - effective_date > freshness_window:
        return V2PolicyResult(V2Outcome.NON_NEWS, "freshness_gate", "freshness")
    if value.source_date_conflict:
        return V2PolicyResult(V2Outcome.AMBIGUOUS, "date_conflict", "date")
    if value.source_disposition in {
        SourceDisposition.NO_ELIGIBLE_URL,
        SourceDisposition.UNSAFE_SOURCE_URL,
    }:
        reason = "unsafe_source_url" if value.source_disposition is SourceDisposition.UNSAFE_SOURCE_URL else "url_gate"
        return V2PolicyResult(V2Outcome.NON_NEWS, reason, "url")
    if value.source_disposition is SourceDisposition.SOURCE_UNAVAILABLE:
        if telegram_material < material_body_minimum:
            return V2PolicyResult(V2Outcome.NON_NEWS, "body_gate", "body")
        source_overlay = "source_unavailable"
    elif value.source_disposition is SourceDisposition.SUCCESS:
        if source_material < material_body_minimum:
            if telegram_material < material_body_minimum:
                return V2PolicyResult(V2Outcome.NON_NEWS, "body_gate", "body")
            source_overlay = "source_body_insufficient"
        else:
            source_overlay = None
    else:
        if telegram_material < material_body_minimum:
            return V2PolicyResult(V2Outcome.NON_NEWS, "body_gate", "body")
        source_overlay = None
    if require_original_url and not value.display_url:
        return V2PolicyResult(V2Outcome.NON_NEWS, "url_gate", "url")
    significant, attributed, partnership_material = _context(context)
    exclusions = _matches(context, _EXCLUSIONS)
    if value.sponsored and "marketing_promotion" not in exclusions:
        exclusions.append("marketing_promotion")
    has_exception = bool(significant) or ("partnership" in exclusions and partnership_material)
    if "marketing_promotion" in exclusions and has_exception:
        return V2PolicyResult(
            V2Outcome.AMBIGUOUS,
            "context_conflict",
            "context",
        )
    if attributed and significant:
        return V2PolicyResult(
            V2Outcome.AMBIGUOUS,
            "important_unconfirmed",
            significant[0],
        )
    if source_overlay is not None:
        if exclusions and not has_exception:
            return V2PolicyResult(
                V2Outcome.NON_NEWS,
                exclusions[0],
                exclusions[0],
            )
        return V2PolicyResult(
            V2Outcome.AMBIGUOUS,
            source_overlay,
            "body",
        )
    if has_exception:
        return V2PolicyResult(V2Outcome.CANDIDATE, "clear_candidate")
    if exclusions:
        return V2PolicyResult(V2Outcome.NON_NEWS, exclusions[0], exclusions[0])
    return V2PolicyResult(V2Outcome.CANDIDATE, "clear_candidate")


def evaluate_v2_policy(
    observation: SourceObservation,
    *,
    now: datetime | None = None,
    topic_markers: Iterable[str] = DEFAULT_TOPIC_MARKERS,
    freshness_window: timedelta = timedelta(hours=24),
    material_body_minimum: int = 80,
    require_original_url: bool = True,
) -> V2PolicyResult:
    url = observation.urls[0].url if observation.urls else None
    return evaluate_v2_content(
        V2PolicyInput(observation.text, observation.published_at, url, sponsored=observation.sponsored),
        now=now,
        topic_markers=topic_markers,
        freshness_window=freshness_window,
        material_body_minimum=material_body_minimum,
        require_original_url=require_original_url,
    )


class V2Policy:
    def __init__(
        self,
        *,
        now: datetime | None = None,
        topic_markers: Iterable[str] = DEFAULT_TOPIC_MARKERS,
        freshness_window: timedelta = timedelta(hours=24),
        material_body_minimum: int = 80,
        require_original_url: bool = True,
    ) -> None:
        self._now, self._topic_markers, self._freshness_window = now, tuple(topic_markers), freshness_window
        self._material_body_minimum, self._require_original_url = material_body_minimum, require_original_url

    def evaluate(self, observation: SourceObservation) -> V2PolicyResult:
        return evaluate_v2_policy(
            observation,
            now=self._now,
            topic_markers=self._topic_markers,
            freshness_window=self._freshness_window,
            material_body_minimum=self._material_body_minimum,
            require_original_url=self._require_original_url,
        )

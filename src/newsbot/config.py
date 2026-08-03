"""Typed, fail-closed configuration and capability-scoped environment checks."""

from __future__ import annotations

import json
import os
import tomllib
import unicodedata
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    pass


class Capability(StrEnum):
    INIT_DB = "init-db"
    FIXTURE_RUN = "fixture-run"
    GENERATE_FAKE = "generate-fake"
    AUTH_TELETHON = "auth-telethon"
    LIVE_COLLECTION = "live-collection"
    LIVE_RECONCILE = "live-reconcile"
    NOTIFY_CANDIDATES = "notify-candidates"
    APPROVE_POLL = "approve-poll"
    GENERATE_OPENAI = "generate-openai"
    GENERATE_CODEX = "generate-codex"
    LIVE_SHEETS = "live-sheets"


_CHANNEL_KEYS = frozenset(
    {
        "id",
        "name",
        "handle",
        "enabled",
        "priority",
        "source_quality",
        "classification",
        "official_domains",
        "original_domains",
    }
)
_POLICY_KEYS = frozenset(
    {
        "version",
        "locale",
        "initial_lookback_hours",
        "max_candidate_age_hours",
        "future_tolerance_hours",
        "min_semantic_chars",
        "min_material_sentence_chars",
        "freshness_horizon_hours",
        "novelty_window_days",
        "min_topic_relevance",
        "min_total_score",
        "weights",
        "disclosure_markers",
        "referral_markers",
        "topic_positive_phrases",
        "topic_exclusion_phrases",
        "engagement_weights",
        "engagement_saturation",
        "certainty_markers",
        "certainty_penalties",
    }
)
_NEWS_POLICY_KEYS = frozenset(
    {
        "version",
        "timezone",
        "noon_hour",
        "noon_minute",
        "activation_minutes",
        "material_semantic_chars",
        "material_sentence_chars",
        "analysis_semantic_chars",
        "analysis_sentence_chars",
        "analysis_min_sentences",
        "event_markers_ko",
        "event_markers_en",
        "analysis_markers_ko",
        "analysis_markers_en",
        "evidence_markers_ko",
        "evidence_markers_en",
        "promotion_markers_ko",
        "promotion_markers_en",
        "tutorial_markers_ko",
        "tutorial_markers_en",
        "reaction_markers_ko",
        "reaction_markers_en",
    }
)
_NEWS_POLICY_APPROVED_INTS = {
    "noon_hour": 12,
    "noon_minute": 0,
    "activation_minutes": 60,
    "material_semantic_chars": 80,
    "material_sentence_chars": 40,
    "analysis_semantic_chars": 160,
    "analysis_sentence_chars": 40,
    "analysis_min_sentences": 2,
}
_REQUIRED_CHANNEL_HANDLES = frozenset(
    {"testingcatalog", "ai_masters_community", "aipost", "coinnesskr", "exilist_official", "dolbikong"}
)
_REQUIRED_WEIGHTS = frozenset(
    {"source_quality", "freshness", "engagement", "topic_relevance", "novelty", "official_evidence", "certainty"}
)
_REQUIRED_ENGAGEMENT_KEYS = frozenset({"views", "reactions", "forwards"})
_REQUIRED_CERTAINTY_PENALTIES = frozenset({"conflicts", "missing_url"})
_SUM_TOLERANCE = Decimal("0.000001")
_APPROVED_POLICY_VALUES: dict[str, int | Decimal] = {
    "initial_lookback_hours": 24,
    "max_candidate_age_hours": 72,
    "future_tolerance_hours": 2,
    "min_semantic_chars": 80,
    "min_material_sentence_chars": 40,
    "freshness_horizon_hours": 48,
    "novelty_window_days": 7,
    "min_topic_relevance": Decimal("0.20"),
    "min_total_score": Decimal("0.55"),
}
_CERTAINTY_CATEGORIES: tuple[tuple[str, Decimal, tuple[str, ...]], ...] = (
    ("rumor", Decimal("0.30"), ("rumor", "alleged", "루머", "설")),
    ("anonymous", Decimal("0.20"), ("anonymous", "unattributed", "익명")),
)


@dataclass(frozen=True, slots=True)
class ChannelConfig:
    id: str
    name: str
    handle: str
    enabled: bool
    priority: int = 0
    source_quality: Decimal = Decimal("1")
    classification: str = "official"
    official_domains: tuple[str, ...] = ()
    original_domains: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PolicyConfig:
    version: str = "candidate_policy_v1"
    locale: str = "ko-KR"
    initial_lookback_hours: int = 24
    max_candidate_age_hours: int = 72
    future_tolerance_hours: int = 2
    min_semantic_chars: int = 80
    min_material_sentence_chars: int = 40
    freshness_horizon_hours: int = 48
    novelty_window_days: int = 7
    min_topic_relevance: Decimal = Decimal("0.20")
    min_total_score: Decimal = Decimal("0.55")
    weights: tuple[tuple[str, Decimal], ...] = (
        ("source_quality", Decimal("0.15")),
        ("freshness", Decimal("0.15")),
        ("engagement", Decimal("0.10")),
        ("topic_relevance", Decimal("0.25")),
        ("novelty", Decimal("0.15")),
        ("official_evidence", Decimal("0.15")),
        ("certainty", Decimal("0.05")),
    )
    disclosure_markers: tuple[str, ...] = (
        "[광고]",
        "(광고)",
        "광고:",
        "유료광고",
        "협찬",
        "sponsored",
        "sponsored:",
        "advertisement",
        "ad:",
    )
    referral_markers: tuple[str, ...] = ("추천인", "레퍼럴", "referral", "affiliate", "promo code", "쿠폰")
    topic_positive_phrases: tuple[tuple[str, Decimal], ...] = (
        ("ai", Decimal(".6")),
        ("artificial intelligence", Decimal(".4")),
        ("인공지능", Decimal(".6")),
        ("technology", Decimal(".2")),
        ("crypto", Decimal(".2")),
    )
    topic_exclusion_phrases: tuple[tuple[str, Decimal], ...] = ()
    engagement_weights: tuple[tuple[str, Decimal], ...] = (
        ("views", Decimal(".60")),
        ("reactions", Decimal(".25")),
        ("forwards", Decimal(".15")),
    )
    engagement_saturation: tuple[tuple[str, Decimal], ...] = (
        ("views", Decimal("100000")),
        ("reactions", Decimal("5000")),
        ("forwards", Decimal("1000")),
    )
    certainty_markers: tuple[tuple[str, Decimal], ...] = (
        ("rumor", Decimal(".3")),
        ("alleged", Decimal(".3")),
        ("루머", Decimal(".3")),
        ("설", Decimal(".3")),
        ("anonymous", Decimal(".2")),
        ("unattributed", Decimal(".2")),
        ("익명", Decimal(".2")),
    )
    certainty_penalties: tuple[tuple[str, Decimal], ...] = (
        ("conflicts", Decimal(".5")),
        ("missing_url", Decimal(".2")),
    )
    certainty_categories: tuple[tuple[str, Decimal, tuple[str, ...]], ...] = _CERTAINTY_CATEGORIES

    @property
    def weight_map(self) -> dict[str, Decimal]:
        return dict(self.weights)

@dataclass(frozen=True, slots=True)
class NewsPolicyConfig:
    version: str
    timezone: str
    noon_hour: int
    noon_minute: int
    activation_minutes: int
    material_semantic_chars: int
    material_sentence_chars: int
    analysis_semantic_chars: int
    analysis_sentence_chars: int
    analysis_min_sentences: int
    event_markers_ko: tuple[str, ...]
    event_markers_en: tuple[str, ...]
    analysis_markers_ko: tuple[str, ...]
    analysis_markers_en: tuple[str, ...]
    evidence_markers_ko: tuple[str, ...]
    evidence_markers_en: tuple[str, ...]
    promotion_markers_ko: tuple[str, ...]
    promotion_markers_en: tuple[str, ...]
    tutorial_markers_ko: tuple[str, ...]
    tutorial_markers_en: tuple[str, ...]
    reaction_markers_ko: tuple[str, ...]
    reaction_markers_en: tuple[str, ...]



@dataclass(frozen=True, slots=True)
class AppConfig:
    channels: tuple[ChannelConfig, ...]
    policy: PolicyConfig
    database_path: Path
    google_service_account_file: Path | None
    google_sheets_spreadsheet_id: str | None
    config_path: Path
    news_policy: NewsPolicyConfig

    @property
    def enabled_channels(self) -> tuple[ChannelConfig, ...]:
        return tuple(channel for channel in self.channels if channel.enabled)

    @property
    def channels_by_id(self) -> Mapping[str, ChannelConfig]:
        """Canonical configured channel lookup for fail-closed consumers."""
        return {channel.id: channel for channel in self.channels}

    @property
    def digest(self) -> str:
        payload = {
            "channels": [
                {
                    "id": channel.id,
                    "name": channel.name,
                    "handle": channel.handle,
                    "enabled": channel.enabled,
                    "priority": channel.priority,
                    "source_quality": str(channel.source_quality),
                    "classification": channel.classification,
                    "official_domains": channel.official_domains,
                    "original_domains": channel.original_domains,
                }
                for channel in self.channels
            ],
            "policy": {
                "version": self.policy.version,
                "locale": self.policy.locale,
                "initial_lookback_hours": self.policy.initial_lookback_hours,
                "max_candidate_age_hours": self.policy.max_candidate_age_hours,
                "future_tolerance_hours": self.policy.future_tolerance_hours,
                "min_semantic_chars": self.policy.min_semantic_chars,
                "min_material_sentence_chars": self.policy.min_material_sentence_chars,
                "freshness_horizon_hours": self.policy.freshness_horizon_hours,
                "novelty_window_days": self.policy.novelty_window_days,
                "min_topic_relevance": str(self.policy.min_topic_relevance),
                "min_total_score": str(self.policy.min_total_score),
                "disclosure_markers": self.policy.disclosure_markers,
                "referral_markers": self.policy.referral_markers,
                "weights": [(key, str(value)) for key, value in self.policy.weights],
                "topic_positive_phrases": [(key, str(value)) for key, value in self.policy.topic_positive_phrases],
                "topic_exclusion_phrases": [(key, str(value)) for key, value in self.policy.topic_exclusion_phrases],
                "engagement_weights": [(key, str(value)) for key, value in self.policy.engagement_weights],
                "engagement_saturation": [(key, str(value)) for key, value in self.policy.engagement_saturation],
                "certainty_markers": [(key, str(value)) for key, value in self.policy.certainty_markers],
                "certainty_penalties": [(key, str(value)) for key, value in self.policy.certainty_penalties],
                "certainty_categories": [
                    (category, str(penalty), markers) for category, penalty, markers in self.policy.certainty_categories
                ],
            },
                "news_policy": {
                    name: getattr(self.news_policy, name)
                    for name in sorted(_NEWS_POLICY_KEYS)
                },
        }
        return sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":")).encode()
        ).hexdigest()


def validate_automation_bindings(config: AppConfig) -> None:
    """Reassert the immutable six-channel worker contract at each entrypoint."""
    channels = config.enabled_channels
    handles = frozenset(channel.handle.casefold() for channel in channels)
    if len(channels) != 6 or handles != _REQUIRED_CHANNEL_HANDLES:
        raise ConfigError("automation requires exactly the six audited enabled channels")


def _decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ConfigError(f"{name} must be a decimal") from error
    if not parsed.is_finite():
        raise ConfigError(f"{name} must be finite")
    return parsed


def _bounded_int(value: Any, name: str, low: int, high: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ConfigError(f"{name} must be an integer from {low} to {high}")
    return value


def _string_list(value: Any, name: str, *, required: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ConfigError(f"{name} must be a list of non-empty strings")
    result = tuple(unicodedata.normalize("NFC", item.strip()) for item in value)
    if required and not result:
        raise ConfigError(f"{name} must not be empty")
    if len({item.casefold() for item in result}) != len(result):
        raise ConfigError(f"{name} contains duplicate normalized values")
    return result


def _parse_channel(raw: Any, index: int) -> ChannelConfig:
    if not isinstance(raw, dict):
        raise ConfigError(f"channels[{index}] must be a table")
    unknown = set(raw) - _CHANNEL_KEYS
    if unknown:
        raise ConfigError(f"channels[{index}] has unknown keys: {', '.join(sorted(unknown))}")
    required = (
        "id",
        "name",
        "handle",
        "enabled",
        "priority",
        "source_quality",
        "classification",
        "official_domains",
        "original_domains",
    )
    if any(not isinstance(raw.get(key), str) or not raw[key].strip() for key in required[:3]) or not isinstance(
        raw.get("enabled"), bool
    ):
        raise ConfigError(f"channels[{index}] requires non-empty id, name, handle, and boolean enabled")
    if any(key not in raw for key in required[4:]):
        raise ConfigError(f"channels[{index}] must define source quality, classification, and domain lists")
    classification = raw["classification"]
    if classification not in {"official", "original_publisher", "aggregator", "community"}:
        raise ConfigError(f"channels[{index}].classification is invalid")
    quality = _decimal(raw["source_quality"], f"channels[{index}].source_quality")
    if not Decimal("0") <= quality <= Decimal("1"):
        raise ConfigError(f"channels[{index}].source_quality must be from 0 to 1")
    return ChannelConfig(
        id=raw["id"].strip(),
        name=raw["name"].strip(),
        handle=raw["handle"].strip().lstrip("@"),
        enabled=raw["enabled"],
        priority=_bounded_int(raw["priority"], f"channels[{index}].priority", 0, 1_000_000),
        source_quality=quality,
        classification=classification,
        official_domains=_string_list(raw["official_domains"], f"channels[{index}].official_domains"),
        original_domains=_string_list(raw["original_domains"], f"channels[{index}].original_domains"),
    )


def _decimal_terms(
    raw: Any,
    name: str,
    *,
    required: frozenset[str] | None = None,
    allow_empty: bool = False,
    unit_interval: bool = True,
) -> tuple[tuple[str, Decimal], ...]:
    if not isinstance(raw, dict) or (not raw and not allow_empty):
        raise ConfigError(f"{name} must be a {'non-empty ' if not allow_empty else ''}table")
    normalized = [unicodedata.normalize("NFC", key.strip()).casefold() for key in raw if isinstance(key, str)]
    if len(normalized) != len(raw) or any(not key for key in normalized) or len(set(normalized)) != len(normalized):
        raise ConfigError(f"{name} contains duplicate or invalid normalized terms")
    if required is not None and set(raw) != required:
        raise ConfigError(f"{name} must define every required key exactly once")
    values = tuple(
        (unicodedata.normalize("NFC", key.strip()), _decimal(value, f"{name}.{key}")) for key, value in raw.items()
    )
    if unit_interval and any(not Decimal("0") <= value <= Decimal("1") for _, value in values):
        raise ConfigError(f"{name} values must be in [0, 1]")
    return values


def _parse_policy(raw: Any) -> PolicyConfig:
    if not isinstance(raw, dict):
        raise ConfigError("policy must be a table")
    unknown = set(raw) - _POLICY_KEYS
    missing = _POLICY_KEYS - set(raw)
    if unknown or missing:
        parts = []
        if unknown:
            parts.append("unknown keys: " + ", ".join(sorted(unknown)))
        if missing:
            parts.append("missing keys: " + ", ".join(sorted(missing)))
        raise ConfigError("policy has " + "; ".join(parts))
    weights = _decimal_terms(raw["weights"], "policy.weights", required=_REQUIRED_WEIGHTS)
    if any(value <= 0 for _, value in weights):
        raise ConfigError("policy.weights values must be positive")
    if abs(sum(value for _, value in weights) - Decimal("1")) > _SUM_TOLERANCE:
        raise ConfigError(f"policy.weights must sum to 1 within {_SUM_TOLERANCE}")
    engagement_weights = _decimal_terms(
        raw["engagement_weights"], "policy.engagement_weights", required=_REQUIRED_ENGAGEMENT_KEYS
    )
    if any(value <= 0 for _, value in engagement_weights):
        raise ConfigError("policy.engagement_weights values must be positive")
    if abs(sum(value for _, value in engagement_weights) - Decimal("1")) > _SUM_TOLERANCE:
        raise ConfigError(f"policy.engagement_weights must sum to 1 within {_SUM_TOLERANCE}")
    engagement_saturation = _decimal_terms(
        raw["engagement_saturation"],
        "policy.engagement_saturation",
        required=_REQUIRED_ENGAGEMENT_KEYS,
        unit_interval=False,
    )
    if any(value <= 0 or value > Decimal("1000000000") for _, value in engagement_saturation):
        raise ConfigError("policy.engagement_saturation values must be from 0 (exclusive) to 1000000000")
    policy = PolicyConfig(
        version=raw["version"],
        locale=raw["locale"],
        initial_lookback_hours=raw["initial_lookback_hours"],
        max_candidate_age_hours=raw["max_candidate_age_hours"],
        future_tolerance_hours=raw["future_tolerance_hours"],
        min_semantic_chars=raw["min_semantic_chars"],
        min_material_sentence_chars=raw["min_material_sentence_chars"],
        freshness_horizon_hours=raw["freshness_horizon_hours"],
        novelty_window_days=raw["novelty_window_days"],
        min_topic_relevance=_decimal(raw["min_topic_relevance"], "policy.min_topic_relevance"),
        min_total_score=_decimal(raw["min_total_score"], "policy.min_total_score"),
        weights=weights,
        disclosure_markers=_string_list(raw["disclosure_markers"], "policy.disclosure_markers", required=True),
        referral_markers=_string_list(raw["referral_markers"], "policy.referral_markers", required=True),
        topic_positive_phrases=_decimal_terms(raw["topic_positive_phrases"], "policy.topic_positive_phrases"),
        topic_exclusion_phrases=_decimal_terms(
            raw["topic_exclusion_phrases"], "policy.topic_exclusion_phrases", allow_empty=True
        ),
        engagement_weights=engagement_weights,
        engagement_saturation=engagement_saturation,
        certainty_markers=_decimal_terms(raw["certainty_markers"], "policy.certainty_markers"),
        certainty_penalties=_decimal_terms(
            raw["certainty_penalties"], "policy.certainty_penalties", required=_REQUIRED_CERTAINTY_PENALTIES
        ),
    )
    marker_penalties = dict(policy.certainty_markers)
    configured_markers = {marker.casefold() for marker in marker_penalties}
    category_markers = {marker.casefold() for _, _, markers in policy.certainty_categories for marker in markers}
    if configured_markers != category_markers:
        raise ConfigError("policy.certainty_markers must define the explicit certainty categories")
    for category, penalty, markers in policy.certainty_categories:
        if any(marker_penalties[marker] != penalty for marker in markers):
            raise ConfigError(f"policy.certainty_markers.{category} aliases must use the approved category penalty")
    if (
        not isinstance(policy.version, str)
        or not policy.version
        or not isinstance(policy.locale, str)
        or not policy.locale
    ):
        raise ConfigError("policy version and locale must be non-empty strings")
    if policy.version != "candidate_policy_v1":
        raise ConfigError("policy.version must be candidate_policy_v1")
    _bounded_int(policy.initial_lookback_hours, "policy.initial_lookback_hours", 1, 168)
    _bounded_int(policy.max_candidate_age_hours, "policy.max_candidate_age_hours", 1, 336)
    _bounded_int(policy.future_tolerance_hours, "policy.future_tolerance_hours", 0, 24)
    _bounded_int(policy.min_semantic_chars, "policy.min_semantic_chars", 20, 500)
    _bounded_int(policy.min_material_sentence_chars, "policy.min_material_sentence_chars", 10, 300)
    _bounded_int(policy.freshness_horizon_hours, "policy.freshness_horizon_hours", 1, 336)
    _bounded_int(policy.novelty_window_days, "policy.novelty_window_days", 1, 90)
    if policy.min_material_sentence_chars > policy.min_semantic_chars:
        raise ConfigError("policy.min_material_sentence_chars cannot exceed policy.min_semantic_chars")
    if any(value <= 0 for _, value in policy.topic_positive_phrases):
        raise ConfigError("policy.topic_positive_phrases values must be positive")
    if "ai" not in {term.casefold() for term, _ in policy.topic_positive_phrases}:
        raise ConfigError("policy.topic_positive_phrases must include the core term 'ai'")
    if any(value < 0 for _, value in policy.topic_exclusion_phrases):
        raise ConfigError("policy.topic_exclusion_phrases values must be non-negative")
    if any(value <= 0 for _, value in policy.certainty_markers):
        raise ConfigError("policy.certainty_markers values must be positive")
    if any(value <= 0 for _, value in policy.certainty_penalties):
        raise ConfigError("policy.certainty_penalties values must be positive")
    if any(not Decimal("0") <= value <= Decimal("1") for value in (policy.min_topic_relevance, policy.min_total_score)):
        raise ConfigError("policy thresholds must be in [0, 1]")
    for name, approved in _APPROVED_POLICY_VALUES.items():
        if getattr(policy, name) != approved:
            raise ConfigError(f"policy.{name} must equal approved candidate_policy_v1 value {approved}")
    return policy


def _parse_news_policy(raw: Any) -> NewsPolicyConfig:
    if not isinstance(raw, dict):
        raise ConfigError("news_policy must be a table")
    unknown = set(raw) - _NEWS_POLICY_KEYS
    missing = _NEWS_POLICY_KEYS - set(raw)
    if unknown or missing:
        parts = []
        if unknown:
            parts.append("unknown keys: " + ", ".join(sorted(unknown)))
        if missing:
            parts.append("missing keys: " + ", ".join(sorted(missing)))
        raise ConfigError("news_policy has " + "; ".join(parts))
    if raw["version"] != "news_policy_v1":
        raise ConfigError("news_policy.version must be news_policy_v1")
    if raw["timezone"] != "Asia/Seoul":
        raise ConfigError("news_policy.timezone must be Asia/Seoul")
    values = {
        name: _bounded_int(raw[name], f"news_policy.{name}", 0, 100_000)
        for name in _NEWS_POLICY_APPROVED_INTS
    }
    for name, approved in _NEWS_POLICY_APPROVED_INTS.items():
        if values[name] != approved:
            raise ConfigError(f"news_policy.{name} must equal approved news_policy_v1 value {approved}")
    if values["material_sentence_chars"] > values["material_semantic_chars"]:
        raise ConfigError("news_policy.material_sentence_chars cannot exceed material_semantic_chars")
    if values["analysis_sentence_chars"] > values["analysis_semantic_chars"]:
        raise ConfigError("news_policy.analysis_sentence_chars cannot exceed analysis_semantic_chars")
    marker_names = tuple(name for name in _NEWS_POLICY_KEYS if name.endswith("_ko") or name.endswith("_en"))
    markers = {name: _string_list(raw[name], f"news_policy.{name}", required=True) for name in marker_names}
    return NewsPolicyConfig(
        version=raw["version"],
        timezone=raw["timezone"],
        noon_hour=values["noon_hour"],
        noon_minute=values["noon_minute"],
        activation_minutes=values["activation_minutes"],
        material_semantic_chars=values["material_semantic_chars"],
        material_sentence_chars=values["material_sentence_chars"],
        analysis_semantic_chars=values["analysis_semantic_chars"],
        analysis_sentence_chars=values["analysis_sentence_chars"],
        analysis_min_sentences=values["analysis_min_sentences"],
        event_markers_ko=markers["event_markers_ko"],
        event_markers_en=markers["event_markers_en"],
        analysis_markers_ko=markers["analysis_markers_ko"],
        analysis_markers_en=markers["analysis_markers_en"],
        evidence_markers_ko=markers["evidence_markers_ko"],
        evidence_markers_en=markers["evidence_markers_en"],
        promotion_markers_ko=markers["promotion_markers_ko"],
        promotion_markers_en=markers["promotion_markers_en"],
        tutorial_markers_ko=markers["tutorial_markers_ko"],
        tutorial_markers_en=markers["tutorial_markers_en"],
        reaction_markers_ko=markers["reaction_markers_ko"],
        reaction_markers_en=markers["reaction_markers_en"],
    )


def load_config(
    path: str | Path = "config/channels.toml",
    *,
    environ: Mapping[str, str] | None = None,
    cli_overrides: Mapping[str, str | Path] | None = None,
) -> AppConfig:
    """Load TOML config; CLI values override environment, which overrides defaults."""
    config_path = Path(path)
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"cannot read configuration: {config_path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"invalid TOML in {config_path}: {error}") from error
    unknown = set(raw) - {"channels", "policy", "news_policy"}
    if unknown:
        raise ConfigError(f"configuration has unknown keys: {', '.join(sorted(unknown))}")
    channels_raw = raw.get("channels")
    if not isinstance(channels_raw, list):
        raise ConfigError("configuration requires [[channels]]")
    if len(channels_raw) != 6:
        raise ConfigError("configuration must define exactly six channels")
    channels = tuple(_parse_channel(channel, index) for index, channel in enumerate(channels_raw))
    handles = {channel.handle.casefold() for channel in channels}
    if len({channel.id.casefold() for channel in channels}) != 6 or len(handles) != 6:
        raise ConfigError("configuration channel ids and handles must be unique")
    if handles != _REQUIRED_CHANNEL_HANDLES:
        raise ConfigError("configuration must define the six audited channel handles")
    if len(tuple(channel for channel in channels if channel.enabled)) != 6:
        raise ConfigError("all six configured channels must be enabled")
    env = os.environ if environ is None else environ
    overrides = {} if cli_overrides is None else cli_overrides
    database = (
        overrides.get("database_path")
        or overrides.get("database")
        or env.get("NEWSBOT_DATABASE")
        or "data/newsbot.sqlite"
    )
    service_account_file = overrides.get("google_service_account_file") or env.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    spreadsheet_id = overrides.get("google_sheets_spreadsheet_id") or env.get("GOOGLE_SHEETS_SPREADSHEET_ID")
    if spreadsheet_id is not None:
        spreadsheet_id = str(spreadsheet_id).strip() or None
    return AppConfig(
        channels=channels,
        policy=_parse_policy(raw.get("policy")),
        news_policy=_parse_news_policy(raw.get("news_policy")),
        database_path=Path(database),
        google_service_account_file=Path(service_account_file) if service_account_file else None,
        google_sheets_spreadsheet_id=spreadsheet_id,
        config_path=config_path,
    )


def validate_capabilities(
    capabilities: Capability | Iterable[Capability], *, environ: Mapping[str, str] | None = None
) -> None:
    """Raise one error listing only credentials required by selected capabilities."""
    requested = {capabilities} if isinstance(capabilities, Capability) else set(capabilities)
    requirements: dict[Capability, tuple[str, ...]] = {
        Capability.AUTH_TELETHON: ("TELEGRAM_API_ID", "TELEGRAM_API_HASH"),
        Capability.LIVE_COLLECTION: ("TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_SESSION_PATH"),
        Capability.LIVE_RECONCILE: ("TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_SESSION_PATH"),
        Capability.NOTIFY_CANDIDATES: ("TELEGRAM_BOT_TOKEN", "NEWSBOT_APPROVER_CHAT_ID", "NEWSBOT_APPROVER_USER_IDS"),
        Capability.APPROVE_POLL: ("TELEGRAM_BOT_TOKEN", "NEWSBOT_APPROVER_CHAT_ID", "NEWSBOT_APPROVER_USER_IDS"),
        Capability.GENERATE_OPENAI: ("OPENAI_BASE_URL", "OPENAI_API_KEY", "OPENAI_MODEL", "OPENAI_TIMEOUT_SECONDS"),
        Capability.LIVE_SHEETS: ("GOOGLE_SERVICE_ACCOUNT_FILE", "GOOGLE_SHEETS_SPREADSHEET_ID"),
    }
    env = os.environ if environ is None else environ
    missing = sorted(
        {name for capability in requested for name in requirements.get(capability, ()) if not env.get(name, "").strip()}
    )
    if missing:
        raise ConfigError("missing required environment variables: " + ", ".join(missing))

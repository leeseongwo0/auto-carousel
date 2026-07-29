"""Pure, deterministic ``candidate_policy_v1`` eligibility and scoring."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Any, TypedDict
from urllib.parse import parse_qsl, urlsplit

from .collectors.base import SourceObservation
from .dedupe import content_identity, select_outbound_url, story_identity

POLICY_VERSION = "candidate_policy_v1"
D = Decimal
_SELECTED_HISTORY_STATUSES = frozenset({"selected", "selected_generation_pending", "pending_review", "approved"})
_DEFERRED_HISTORY_STATUSES = frozenset({"deferred", "rejected"})


@dataclass(frozen=True, slots=True)
class Evaluation:
    story_key: str
    content_key: str
    observations: tuple[SourceObservation, ...]
    eligible: bool
    primary_reason: str | None
    reasons: tuple[str, ...]
    components: Mapping[str, Decimal]
    total: Decimal | None
    rationale: Mapping[str, Any]
    published_at_latest: datetime


class _StoryItem(TypedDict):
    row: SourceObservation
    source: dict[str, Any]
    display: str
    match: str
    semantic: str
    url: str | None
    age: Decimal


class CandidateRationale(TypedDict):
    schema_version: str
    policy_version: str
    policy_config_digest: str | None
    story_key: str
    content_key: str
    evaluated_at: str
    sources: list[dict[str, Any]]
    components: dict[str, dict[str, Any]]
    weights: dict[str, str]
    weighted_components: dict[str, str]
    formulas: dict[str, str]
    missing_flags: dict[str, Any]
    threshold_evidence: dict[str, Any]
    tie_break_evidence: dict[str, Any]
    warnings: list[dict[str, Any]]


def _value(source: object | None, name: str, default: object) -> object:
    value: object = source.get(name, default) if isinstance(source, Mapping) else getattr(source, name, default)
    return value


def _domain_names(value: object) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Iterable):
        raise TypeError("domain names must be an iterable of strings")
    domains: list[str] = []
    for domain in value:
        if not isinstance(domain, str):
            raise TypeError("domain names must be strings")
        domains.append(domain)
    return tuple(domains)


def _policy(config: object | None, name: str, default: Any) -> Any:
    policy = _value(config, "policy", config) if config is not None else None
    return _value(policy, name, default)


def _weights(config: object | None) -> Mapping[str, Decimal]:
    raw = _policy(config, "weight_map", None)
    if raw is None:
        raw = _policy(config, "weights", None)
    if raw is None:
        raise ValueError("ranking requires explicit policy weights")
    values = dict(raw)
    names = {
        "quality": "source_quality",
        "freshness": "freshness",
        "engagement": "engagement",
        "topic": "topic_relevance",
        "novelty": "novelty",
        "evidence": "official_evidence",
        "certainty": "certainty",
    }
    if set(values) != set(names.values()):
        raise ValueError("ranking policy weights are incomplete")
    return {name: _decimal(values[configured]) for name, configured in names.items()}


def _decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else D(str(value))


def _text(text: str) -> tuple[str, str, str]:
    display = unicodedata.normalize("NFC", text.replace("\r\n", "\n").replace("\r", "\n"))
    match = " ".join(display.split()).casefold()
    semantic = re.sub(r"https?://\S+|#[\w]+|@[\w]+", " ", display, flags=re.I)
    semantic = "".join(character if character.isalnum() or character.isspace() else " " for character in semantic)
    return display, match, " ".join(semantic.split())


def _phrase_matches(text: str, phrases: Mapping[str, Any]) -> tuple[Decimal, list[str]]:
    total = D("0")
    found: list[str] = []
    for phrase, raw_weight in sorted(phrases.items(), key=lambda item: item[0].casefold()):
        needle = unicodedata.normalize("NFC", phrase).casefold()
        # Word boundary for Latin terms; direct containment keeps Korean phrases useful.
        pattern = rf"(?<!\w){re.escape(needle)}(?!\w)" if needle.isascii() else re.escape(needle)
        if re.search(pattern, text):
            total += _decimal(raw_weight)
            found.append(phrase)
    return min(D("1"), total), found


def _phrase_evidence(text: str, phrases: Mapping[str, Any], found: Iterable[str]) -> list[dict[str, Any]]:
    return [
        {"phrase": phrase, "weight": _decimal_string(_decimal(phrases[phrase])), "spans": _marker_spans(text, phrase)}
        for phrase in found
    ]


def _marker_spans(text: str, marker: str) -> list[dict[str, int]]:
    """Return display-text offsets for lexical marker occurrences."""
    needle = unicodedata.normalize("NFC", marker).casefold().strip()
    if not needle:
        return []
    suffix_boundary = r"(?!\w)" if needle[-1].isalnum() or needle[-1] == "_" else ""
    pattern = rf"(?<!\w){re.escape(needle)}{suffix_boundary}"
    return [{"start": match.start(), "end": match.end()} for match in re.finditer(pattern, text.casefold())]


def _disclosure_match(line: str, marker: str) -> bool:
    return bool(_marker_spans(line, marker) and _marker_spans(line, marker)[0]["start"] == 0)


def _residual_material_text(text: str, markers: Iterable[str]) -> str:
    residual = re.sub(r"https?://\S+|#[\w]+|@[\w]+", " ", text, flags=re.I)
    for marker in markers:
        residual = re.sub(
            rf"(?<!\w){re.escape(unicodedata.normalize('NFC', marker))}(?!\w)",
            " ",
            residual,
            flags=re.I,
        )
    residual = re.sub(r"\b(?:coupon|promo(?:\s+code)?|code)\s*[:#-]?\s*[\w-]+\b", " ", residual, flags=re.I)
    residual = re.sub(
        r"\b(?:click\s+here|learn\s+more|sign\s+up|join\s+now|buy\s+now|purchase\s+now|order\s+now|shop\s+now)\b|(?:지금\s*)?(?:가입|구독|구매|주문)(?:하세요|하기)?|자세히\s*보기|바로가기",
        " ",
        residual,
        flags=re.I,
    )
    return " ".join(re.sub(r"[^\w\s]", " ", residual).split())


def _channel(config: object | None, channel_id: str) -> object | None:
    channels = _value(config, "channels_by_id", None)
    if isinstance(channels, Mapping):
        return channels.get(channel_id)
    entries = _value(config, "channels", ())
    if not isinstance(entries, Iterable):
        return None
    for entry in entries:
        if _value(entry, "id", None) == channel_id:
            return entry if isinstance(entry, object) else None
    return None


def _has_material_sentence(text: str, minimum: int) -> bool:
    return any(len(sentence.strip()) >= minimum for sentence in re.split(r"[.!?。！？\n]", text))


def _engagement(observation: SourceObservation, config: object | None) -> tuple[Decimal, bool, dict[str, Any]]:
    values = observation.engagement
    weights = dict(_policy(config, "engagement_weights", ()))
    saturation = dict(_policy(config, "engagement_saturation", ()))
    names_and_values = (("views", values.views), ("reactions", values.reactions), ("forwards", values.forwards))
    if set(weights) != {name for name, _ in names_and_values} or set(saturation) != {
        name for name, _ in names_and_values
    }:
        raise ValueError("ranking policy engagement constants are incomplete")
    constants = {
        name: {
            "weight": _decimal_string(_decimal(weights[name])),
            "saturation": _decimal_string(_decimal(saturation[name])),
        }
        for name, _ in names_and_values
    }
    if all(value is None for _, value in names_and_values):
        return D(".25"), True, {"constants": constants, "contributions": [], "all_missing_default": "0.25"}
    contributions: list[dict[str, str]] = []
    with localcontext() as context:
        context.prec = 40
        score = D("0")
        for name, value in names_and_values:
            if value is None:
                continue
            ratio = min(D("1"), (D(1) + D(value)).ln() / (D(1) + _decimal(saturation[name])).ln())
            contribution = _decimal(weights[name]) * ratio
            score += contribution
            contributions.append(
                {
                    "metric": name,
                    "raw": str(value),
                    "ratio": _decimal_string(ratio),
                    "contribution": _decimal_string(contribution),
                }
            )
    return score, False, {"constants": constants, "contributions": contributions, "all_missing_default": None}


def _history_novelty(history: Iterable[object], story: str, content: str) -> tuple[Decimal, tuple[int, ...], str]:
    rows = [entry for entry in history if _value(entry, "story_key", _value(entry, "story_identity", None)) == story]
    matching_ids = tuple(
        int(candidate_id)
        for row in rows
        if isinstance((candidate_id := _value(row, "candidate_id", _value(row, "id", None))), (int, str))
    )
    if any(
        _value(row, "content_key", _value(row, "content_identity", None)) == content
        and _value(row, "status", "") in _SELECTED_HISTORY_STATUSES
        for row in rows
    ):
        return D("0"), matching_ids, "selected_content"
    if any(_value(row, "status", "") in _DEFERRED_HISTORY_STATUSES for row in rows):
        return D(".20"), matching_ids, "deferred_or_rejected_story"
    if rows:
        return D(".40"), matching_ids, "prior_story_material_edit"
    return D("1"), (), "no_prior_story"


def _source_identity(observation: SourceObservation) -> dict[str, str]:
    return {"channel_id": observation.channel_id, "external_post_id": observation.external_post_id}


def _observation_identity(observation: SourceObservation) -> dict[str, str | None]:
    return {
        "channel_id": observation.channel_id,
        "external_post_id": observation.external_post_id,
        "observed_at": observation.observed_at.astimezone(UTC).isoformat() if observation.observed_at else None,
    }


def _source_key(item: Mapping[str, Any]) -> tuple[str, str]:
    source = item["source"]["material_identity"]
    return source["channel_id"], source["external_post_id"]


def _raw_referral_query(observation: SourceObservation) -> dict[str, str] | None:
    candidates = [candidate.url for candidate in observation.urls]
    candidates.extend(re.findall(r"https?://\S+", observation.text, flags=re.I))
    for raw_url in candidates:
        try:
            query = parse_qsl(urlsplit(raw_url).query, keep_blank_values=True)
        except ValueError:
            continue
        for key, value in query:
            folded_key = key.casefold()
            folded_value = value.casefold()
            if (
                folded_key == "ref"
                or folded_key.startswith("ref_")
                or any(marker in folded_key or marker in folded_value for marker in ("affiliate", "coupon", "promo"))
            ):
                return {"key": key, "value": value}
    return None


def _decimal_string(value: Decimal) -> str:
    return str(value)


def _component(
    value: Decimal,
    inputs: list[dict[str, Any]],
    *,
    winner: dict[str, Any] | None = None,
    worst: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"value": _decimal_string(value), "inputs": inputs}
    if winner is not None:
        result["winner_source"] = winner["material_identity"]
        result["winner_material_identity"] = winner["material_identity"]
        result["winner_observation_identity"] = winner["observation_identity"]
    if worst is not None:
        result["worst_source"] = worst["material_identity"]
        result["worst_material_identity"] = worst["material_identity"]
        result["worst_observation_identity"] = worst["observation_identity"]
    return result


def _hard_evaluation(
    row: SourceObservation,
    story: str,
    content: str,
    source: Mapping[str, Any],
    reasons: list[str],
    config: object | None,
) -> Evaluation:
    ordered = (
        "unknown_channel",
        "service_message",
        "empty_record",
        "explicit_ad",
        "referral_only",
        "low_value",
        "published_window",
    )
    filters = [reason for reason in ordered if reason in reasons]
    raw_config_digest = _value(config, "digest", None)
    rationale: CandidateRationale = {
        "schema_version": "candidate_rationale_v1",
        "policy_version": _policy(config, "version", POLICY_VERSION),
        "policy_config_digest": str(raw_config_digest) if raw_config_digest is not None else None,
        "story_key": story,
        "content_key": content,
        "evaluated_at": row.observed_at.astimezone(UTC).isoformat()
        if row.observed_at
        else row.published_at.astimezone(UTC).isoformat(),
        "sources": [dict(source)],
        "components": {},
        "weights": {},
        "weighted_components": {},
        "formulas": {},
        "missing_flags": {"hard_rejected": True},
        "threshold_evidence": {"hard_filter_order": list(ordered), "hard_rejections": filters},
        "tie_break_evidence": {"source_order": [_observation_identity(row)]},
        "warnings": [],
    }
    return Evaluation(story, content, (row,), False, filters[0], tuple(filters), {}, None, rationale, row.published_at)


def evaluate_candidates(
    observations: Sequence[SourceObservation],
    config: object | None,
    evaluated_at: datetime,
    history: Iterable[object] = (),
) -> tuple[Evaluation, ...]:
    """Evaluate immutable observations with no clock, database, network, or AI input."""
    if evaluated_at.tzinfo is None:
        raise ValueError("evaluated_at must be timezone-aware")
    evaluated_at = evaluated_at.astimezone(UTC)
    grouped: dict[str, list[_StoryItem]] = {}
    result: list[Evaluation] = []
    for row in observations:
        story, content = story_identity(row), content_identity(row)
        display, match, semantic = _text(row.text)
        url, url_source = select_outbound_url(row)
        material_preview = any(candidate.title or candidate.description for candidate in row.urls if url)
        nonservice_media = any(not media.is_service for media in row.media)
        media_caption = any(len(_text(media.caption or "")[2]) >= 40 for media in row.media if not media.is_service)
        reasons: list[str] = []
        channel = _channel(config, row.channel_id)
        if channel is None:
            reasons.append("unknown_channel")
        if row.kind != "message":
            reasons.append("service_message")
        if not semantic and not url and not nonservice_media and not material_preview:
            reasons.append("empty_record")
        lines = [line.strip().casefold() for line in display.splitlines() if line.strip()][:2]
        disclosure_evidence = [
            {"line": index, "marker": marker, "spans": _marker_spans(line, marker)}
            for index, line in enumerate(lines, start=1)
            for marker in _policy(config, "disclosure_markers", ())
            if _disclosure_match(line, marker)
        ]
        if row.sponsored or disclosure_evidence:
            reasons.append("explicit_ad")
        referral_markers = tuple(_policy(config, "referral_markers", ()))
        referral_marker = next((marker for marker in referral_markers if _marker_spans(display, marker)), None)
        raw_referral_query = _raw_referral_query(row)
        residual_material = _residual_material_text(display, referral_markers)
        if (referral_marker or raw_referral_query) and not _has_material_sentence(
            residual_material, int(_policy(config, "min_material_sentence_chars", 40))
        ):
            reasons.append("referral_only")
        if (
            len(residual_material) < int(_policy(config, "min_semantic_chars", 80))
            and not material_preview
            and not media_caption
            and not _has_material_sentence(residual_material, int(_policy(config, "min_material_sentence_chars", 40)))
        ):
            reasons.append("low_value")
        age = max(D("0"), D(str((evaluated_at - row.published_at.astimezone(UTC)).total_seconds())) / D("3600"))
        future = D(str((row.published_at.astimezone(UTC) - evaluated_at).total_seconds())) / D("3600")
        if future > _decimal(_policy(config, "future_tolerance_hours", 2)) or age > _decimal(
            _policy(config, "max_candidate_age_hours", 72)
        ):
            reasons.append("published_window")
        source = {
            "material_identity": _source_identity(row),
            "observation_identity": _observation_identity(row),
            "timestamps": {
                "published_at": row.published_at.astimezone(UTC).isoformat(),
                "edited_at": row.edited_at.astimezone(UTC).isoformat() if row.edited_at else None,
                "observed_at": row.observed_at.astimezone(UTC).isoformat() if row.observed_at else None,
            },
            "freshness": {
                "age_hours": _decimal_string(age),
                "horizon_hours": _decimal_string(_decimal(_policy(config, "freshness_horizon_hours", 48))),
            },
            "engagement": {
                "raw": {
                    "views": row.engagement.views,
                    "reactions": row.engagement.reactions,
                    "forwards": row.engagement.forwards,
                },
                "missing_flags": {
                    "views": row.engagement.views is None,
                    "reactions": row.engagement.reactions is None,
                    "forwards": row.engagement.forwards is None,
                },
            },
            "filters": {
                "hard_rejections": reasons,
                "accepted": not reasons,
                "sponsored_flag": bool(row.sponsored),
                "disclosure_evidence": disclosure_evidence,
            },
            "outbound_url": url,
            "outbound_url_source": url_source,
            "raw_referral_query": raw_referral_query,
            "referral_marker": referral_marker,
            "residual_material_text": residual_material,
            "missing_flags": {
                "outbound_url": url is None,
                "engagement": all(
                    value is None for value in (row.engagement.views, row.engagement.reactions, row.engagement.forwards)
                ),
                "material_preview": not material_preview,
                "media_caption": not media_caption,
            },
        }
        item: _StoryItem = {
            "row": row,
            "source": source,
            "display": display,
            "match": match,
            "semantic": semantic,
            "url": url,
            "age": age,
        }
        if reasons:
            result.append(_hard_evaluation(row, story, content, source, reasons, config))
        else:
            grouped.setdefault(story, []).append(item)

    positive = dict(_policy(config, "topic_positive_phrases", ()))
    negative = dict(_policy(config, "topic_exclusion_phrases", ()))
    if not positive:
        raise ValueError("ranking requires explicit positive topic phrases")
    weights = _weights(config)
    for story, items in grouped.items():
        items.sort(key=lambda item: (content_identity(item["row"]), *_source_key(item)))
        rows = [item["row"] for item in items]
        content = min(content_identity(row) for row in rows)
        latest = max(row.published_at for row in rows)
        warnings: list[dict[str, Any]] = []
        quality_values = [
            _decimal(
                _value(
                    _channel(config, item["row"].channel_id),
                    "source_quality",
                    _value(_channel(config, item["row"].channel_id), "quality", D(".5")),
                )
            )
            for item in items
        ]
        for item, value in zip(items, quality_values, strict=True):
            item["source"]["quality_config"] = {"source_quality": _decimal_string(value)}
        horizon = _decimal(_policy(config, "freshness_horizon_hours", 48))
        freshness_values = [max(D("0"), D("1") - item["age"] / horizon) for item in items]
        engagement_values = [_engagement(item["row"], config) for item in items]
        for item, (_, missing, detail) in zip(items, engagement_values, strict=True):
            item["source"]["engagement"]["scoring"] = detail
            item["source"]["engagement"]["all_missing"] = missing
        topic_values: list[Decimal] = []
        evidence_values: list[Decimal] = []
        certainty_values: list[Decimal] = []
        for item in items:
            pos, found = _phrase_matches(item["match"], positive)
            neg, excluded = _phrase_matches(item["match"], negative)
            topic = pos * max(D("0"), D("1") - neg)
            topic_values.append(topic)
            item["source"]["topic"] = {
                "positive_matches": _phrase_evidence(item["display"], positive, found),
                "exclusion_matches": _phrase_evidence(item["display"], negative, excluded),
                "positive_value": _decimal_string(pos),
                "exclusion_value": _decimal_string(neg),
                "value": _decimal_string(topic),
            }
            channel = _channel(config, item["row"].channel_id)
            classification = _value(channel, "classification", "community")
            domains = _domain_names(_value(channel, "official_domains", ()))
            original_domains = _domain_names(_value(channel, "original_domains", ()))
            host = urlsplit(item["url"]).hostname or "" if item["url"] else ""
            official_domain = any(host == domain or host.endswith("." + domain) for domain in domains)
            original_domain = any(host == domain or host.endswith("." + domain) for domain in original_domains)
            evidence = (
                D("1")
                if classification == "official" or official_domain
                else D(".8")
                if classification == "original_publisher" or original_domain
                else D(".5")
                if classification == "aggregator" and item["url"]
                else D(".3")
                if item["url"]
                else D("0")
            )
            evidence_values.append(evidence)
            penalties = dict(_policy(config, "certainty_penalties", ()))
            configured_markers = dict(_policy(config, "certainty_markers", ()))
            conflict_penalty = _decimal(penalties["conflicts"]) if item["row"].conflicts else D("0")
            category_hits: list[tuple[str, Decimal, list[dict[str, Any]]]] = []
            categories = _policy(config, "certainty_categories", None)
            if categories is None:
                raise ValueError("ranking policy requires explicit certainty categories")
            for category, penalty, aliases in categories:
                matches = [
                    {"marker": marker, "spans": spans}
                    for marker in aliases
                    if marker in configured_markers and (spans := _marker_spans(item["display"], marker))
                ]
                if matches:
                    category_hits.append((category, _decimal(penalty), matches))
            missing_penalty = _decimal(penalties["missing_url"]) if not item["url"] else D("0")
            certainty = max(
                D("0"),
                D("1")
                - min(
                    D("1"),
                    conflict_penalty + sum((penalty for _, penalty, _ in category_hits), D("0")) + missing_penalty,
                ),
            )
            certainty_values.append(certainty)
            item["source"]["evidence"] = {
                "classification": classification,
                "official_domain_match": official_domain,
                "original_domain_match": original_domain,
                "reason": (
                    "official_classification_or_domain"
                    if classification == "official" or official_domain
                    else "original_publisher_classification_or_domain"
                    if classification == "original_publisher" or original_domain
                    else "aggregator_with_url"
                    if classification == "aggregator" and item["url"]
                    else "url_present"
                    if item["url"]
                    else "no_url"
                ),
                "value": _decimal_string(evidence),
            }
            item["source"]["certainty"] = {
                "conflict_penalty": _decimal_string(conflict_penalty),
                "category_penalties": [
                    {"category": category, "penalty": _decimal_string(penalty), "matches": matches}
                    for category, penalty, matches in category_hits
                ],
                "missing_url_penalty": _decimal_string(missing_penalty),
                "value": _decimal_string(certainty),
            }
            for conflict in item["row"].conflicts:
                warnings.append(
                    {
                        "kind": "conflict",
                        "detail": conflict,
                        "reason": "source_conflict",
                        "source": item["source"]["observation_identity"],
                        "spans": [],
                        "penalty": _decimal_string(conflict_penalty),
                    }
                )
            for category, penalty, matches in category_hits:
                for marker_match in matches:
                    warnings.append(
                        {
                            "kind": "rumor",
                            "detail": marker_match["marker"],
                            "category": category,
                            "reason": "certainty_marker",
                            "source": item["source"]["observation_identity"],
                            "spans": marker_match["spans"],
                            "penalty": _decimal_string(penalty),
                        }
                    )
            if missing_penalty:
                warnings.append(
                    {
                        "kind": "uncertainty",
                        "detail": "Missing linked evidence",
                        "reason": "missing_evidence",
                        "source": item["source"]["observation_identity"],
                        "spans": [],
                        "penalty": _decimal_string(missing_penalty),
                    }
                )

        story_items = tuple(items)

        def pick(
            values: list[Decimal],
            *,
            worst: bool = False,
            _story_items: tuple[_StoryItem, ...] = story_items,
        ) -> tuple[Decimal, dict[str, str]]:
            target = min(values) if worst else max(values)
            index = min(
                (index for index, value in enumerate(values) if value == target),
                key=lambda candidate: _source_key(_story_items[candidate]),
            )
            return target, _story_items[index]["source"]

        quality, quality_source = pick(quality_values)
        freshness, freshness_source = pick(freshness_values)
        engagement, engagement_source = pick([value for value, _, _ in engagement_values])
        topic, topic_source = pick(topic_values)
        evidence, evidence_source = pick(evidence_values)
        certainty, certainty_source = pick(certainty_values, worst=True)
        novelty, matched_history_ids, novelty_reason = _history_novelty(history, story, content)
        components = {
            "quality": quality,
            "freshness": freshness,
            "engagement": engagement,
            "topic": topic,
            "novelty": novelty,
            "evidence": evidence,
            "certainty": certainty,
        }
        total_raw = sum((weights[name] * value for name, value in components.items()), D("0"))
        total = total_raw.quantize(D(".000001"), rounding=ROUND_HALF_EVEN)
        eligibility_reasons = tuple(
            reason
            for reason, failed in (
                ("topic_floor", topic < _decimal(_policy(config, "min_topic_relevance", D(".20")))),
                ("score_floor", total_raw < _decimal(_policy(config, "min_total_score", D(".55")))),
            )
            if failed
        )

        def component_inputs(
            values: list[Decimal],
            _story_items: tuple[_StoryItem, ...] = story_items,
        ) -> list[dict[str, object]]:
            return [
                {
                    "material_identity": item["source"]["material_identity"],
                    "observation_identity": item["source"]["observation_identity"],
                    "value": _decimal_string(value),
                }
                for item, value in zip(_story_items, values, strict=True)
            ]

        raw_config_digest = _value(config, "digest", None)
        rationale: CandidateRationale = {
            "schema_version": "candidate_rationale_v1",
            "policy_version": _policy(config, "version", POLICY_VERSION),
            "policy_config_digest": str(raw_config_digest) if raw_config_digest is not None else None,
            "story_key": story,
            "content_key": content,
            "evaluated_at": evaluated_at.isoformat(),
            "sources": [item["source"] for item in items],
            "components": {
                "quality": _component(quality, component_inputs(quality_values), winner=quality_source),
                "freshness": _component(freshness, component_inputs(freshness_values), winner=freshness_source),
                "engagement": _component(
                    engagement, component_inputs([value for value, _, _ in engagement_values]), winner=engagement_source
                ),
                "topic": _component(topic, component_inputs(topic_values), winner=topic_source),
                "novelty": _component(
                    novelty,
                    [
                        {
                            "matched_candidate_ids": list(matched_history_ids),
                            "reason": novelty_reason,
                            "window_days": _policy(config, "novelty_window_days", None),
                        }
                    ],
                ),
                "evidence": _component(evidence, component_inputs(evidence_values), winner=evidence_source),
                "certainty": _component(certainty, component_inputs(certainty_values), worst=certainty_source),
            },
            "formulas": {
                "engagement": "sum(weight * min(1, ln(1 + raw_metric) / ln(1 + saturation_metric))) for observed metrics; all missing = 0.25",
                "freshness": "max(0, 1 - age_hours / freshness_horizon_hours)",
                "total": "sum(weight * component), quantized to 0.000001 with ROUND_HALF_EVEN",
            },
            "weights": {name: _decimal_string(value) for name, value in weights.items()},
            "weighted_components": {name: _decimal_string(weights[name] * value) for name, value in components.items()},
            "missing_flags": {"engagement_all_missing": all(flag for _, flag, _ in engagement_values)},
            "threshold_evidence": {
                "hard_filter_order": [
                    "unknown_channel",
                    "service_message",
                    "empty_record",
                    "explicit_ad",
                    "referral_only",
                    "low_value",
                    "published_window",
                ],
                "source_filters": [
                    {
                        "observation_identity": item["source"]["observation_identity"],
                        "hard_rejections": item["source"]["filters"]["hard_rejections"],
                        "accepted": item["source"]["filters"]["accepted"],
                    }
                    for item in items
                ],
                "topic": {
                    "value": _decimal_string(topic),
                    "minimum": _decimal_string(_decimal(_policy(config, "min_topic_relevance", D(".20")))),
                    "passed": "topic_floor" not in eligibility_reasons,
                },
                "total": {
                    "unquantized_value": _decimal_string(total_raw),
                    "value": _decimal_string(total),
                    "minimum": _decimal_string(_decimal(_policy(config, "min_total_score", D(".55")))),
                    "passed": "score_floor" not in eligibility_reasons,
                },
            },
            "tie_break_evidence": {
                "ranking_keys": {
                    "score": _decimal_string(total),
                    "published_at_latest": latest.astimezone(UTC).isoformat(),
                    "content_key": content,
                },
                "source_order": [item["source"]["observation_identity"] for item in items],
                "config": {
                    "freshness_horizon_hours": _decimal_string(horizon),
                    "min_topic_relevance": _decimal_string(_decimal(_policy(config, "min_topic_relevance", D(".20")))),
                    "min_total_score": _decimal_string(_decimal(_policy(config, "min_total_score", D(".55")))),
                },
            },
            "warnings": warnings,
        }
        result.append(
            Evaluation(
                story,
                content,
                tuple(rows),
                not eligibility_reasons,
                eligibility_reasons[0] if eligibility_reasons else None,
                eligibility_reasons,
                components,
                total,
                rationale,
                latest,
            )
        )
    eligible = sorted(
        (item for item in result if item.eligible),
        key=lambda item: (-(item.total or D("0")), -item.published_at_latest.timestamp(), item.content_key),
    )
    ineligible = sorted(
        (item for item in result if not item.eligible), key=lambda item: (item.primary_reason or "", item.content_key)
    )
    return tuple(eligible + ineligible)

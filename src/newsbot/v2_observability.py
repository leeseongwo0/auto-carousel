"""Bounded, redacted observability primitives for the isolated V2 workflow.

This module deliberately has no I/O, configuration, or workflow dependencies.  Callers
may pass untrusted identifiers, URLs, redirect chains, and exceptions, but events retain
only fixed labels and one-way, truncated fingerprints.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from hashlib import sha256
from types import MappingProxyType
from typing import Protocol


class MetricName(StrEnum):
    POLICY_DECISION = "policy_decision"
    FETCH = "fetch"
    KEY = "key"
    EFFECT = "effect"
    QUEUE = "queue"
    LANGUAGE = "language"
    COMPACTION = "compaction"
    ALERT = "alert"


class Outcome(StrEnum):
    CANDIDATE = "candidate"
    AMBIGUOUS = "ambiguous"
    NON_NEWS = "non_news"


class Reason(StrEnum):
    NEWS = "news"
    TOPIC_GATE = "topic_gate"
    FRESHNESS_GATE = "freshness_gate"
    DATE_CONFLICT = "date_conflict"
    BODY_GATE = "body_gate"
    URL_GATE = "url_gate"
    MARKETING_PROMOTION = "marketing_promotion"
    CONTEXT_CONFLICT = "context_conflict"
    IMPORTANT_UNCONFIRMED = "important_unconfirmed"
    SOURCE_BODY_INSUFFICIENT = "source_body_insufficient"
    FETCH_FAILED = "fetch_failed"
    MANUAL_REVIEW = "manual_review"
    CLEAR_CANDIDATE = "clear_candidate"
    SOURCE_UNAVAILABLE = "source_unavailable"
    UNSAFE_SOURCE_URL = "unsafe_source_url"
    PRICE_INVESTMENT = "price_investment"
    EXCHANGE_TOKEN_PROMOTION = "exchange_token_promotion"
    PARTNERSHIP = "partnership"
    OPINION_RUMOR = "opinion_rumor"
    MINOR_UPDATE = "minor_update"


class FetchResult(StrEnum):
    SUCCESS = "success"
    BLOCKED = "blocked"
    TRANSIENT_FAILURE = "transient_failure"
    PERMANENT_FAILURE = "permanent_failure"
    REJECTED = "rejected"


class KeyKind(StrEnum):
    OBSERVATION = "observation"
    REVISION = "revision"
    CANDIDATE = "candidate"
    DRAFT = "draft"
    CALLBACK = "callback"
    REQUEST = "request"
    STORY = "story"


class EffectStage(StrEnum):
    CANDIDATE_NOTIFICATION = "candidate_notification"
    DRAFT_GENERATION = "draft_generation"
    DRAFT_NOTIFICATION = "draft_notification"
    SHEETS_DELIVERY = "sheets_delivery"


class EffectStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    CONFIRMED = "confirmed"
    AMBIGUOUS = "ambiguous"
    FAILED = "failed"


class Queue(StrEnum):
    ENRICHMENT = "enrichment"
    CANDIDATE_REVIEW = "candidate_review"
    DRAFT_REVIEW = "draft_review"
    MANUAL_REVIEW = "manual_review"
    CODEX = "codex"
    SHEETS = "sheets"


class LanguagePath(StrEnum):
    KOREAN = "korean"
    ENGLISH = "english"
    MIXED = "mixed"
    OTHER = "other"


class CompactionTable(StrEnum):
    ENRICHMENT_ATTEMPTS = "enrichment_attempts"
    REMOTE_EFFECTS = "remote_effects"
    CALLBACKS = "callbacks"
    CODEX_ATTEMPTS = "codex_attempts"
    MANUAL_REVIEWS = "manual_reviews"
    OBSERVATION_REVISIONS = "observation_revisions"
    DRAFTS = "drafts"
    CODEX_REQUESTS = "codex_requests"


class CompactionResult(StrEnum):
    DRY_RUN = "dry_run"
    COMPACTED = "compacted"
    NOTHING_TO_COMPACT = "nothing_to_compact"
    REJECTED = "rejected"


class ImmediateAlert(StrEnum):
    PRIVATE_HARNESS_HIT = "private_harness_hit"
    DUPLICATE_CLAIM = "duplicate_claim"
    CONFIRMED_EFFECT_REATTEMPT = "confirmed_effect_reattempt"
    MIGRATION_RETENTION_MISMATCH = "migration_retention_mismatch"
    IDENTITY_CONFLICT = "identity_conflict"


class ThresholdAlert(StrEnum):
    FETCH_BLOCKED = "fetch_blocked"
    FETCH_TRANSIENT = "fetch_transient"
    DATABASE_GROWTH = "database_growth"
    OLDEST_QUEUE_AGE = "oldest_queue_age"
    OLDEST_MANUAL_REVIEW_AGE = "oldest_manual_review_age"


class ExceptionKind(StrEnum):
    NONE = "none"
    TIMEOUT = "timeout"
    VALUE = "value"
    OSError = "os_error"
    UNEXPECTED = "unexpected"


_FINGERPRINT_LENGTH = 16
_MAX_REDIRECTS = 16


def fingerprint(value: object) -> str:
    """Return a fixed-length, one-way identifier for untrusted input."""
    return sha256(str(value).encode("utf-8", "replace")).hexdigest()[:_FINGERPRINT_LENGTH]


def _exception_kind(error: BaseException | None) -> ExceptionKind:
    if error is None:
        return ExceptionKind.NONE
    if isinstance(error, TimeoutError):
        return ExceptionKind.TIMEOUT
    if isinstance(error, ValueError):
        return ExceptionKind.VALUE
    if isinstance(error, OSError):
        return ExceptionKind.OSError
    return ExceptionKind.UNEXPECTED


_ALLOWED_LABELS: dict[MetricName, dict[str, tuple[type[StrEnum], ...]]] = {
    MetricName.POLICY_DECISION: {"outcome": (Outcome,), "reason": (Reason,)},
    MetricName.FETCH: {"result": (FetchResult,)},
    MetricName.KEY: {"kind": (KeyKind,)},
    MetricName.EFFECT: {"stage": (EffectStage,), "status": (EffectStatus,)},
    MetricName.QUEUE: {"queue": (Queue,)},
    MetricName.LANGUAGE: {"path": (LanguagePath,)},
    MetricName.COMPACTION: {"table": (CompactionTable,), "result": (CompactionResult,)},
    MetricName.ALERT: {"alert": (ImmediateAlert, ThresholdAlert)},
}


@dataclass(frozen=True, slots=True)
class RedactedEvent:
    """A serializable event containing only bounded labels and fingerprints."""

    metric: MetricName
    labels: Mapping[str, StrEnum]
    entity_fingerprint: str | None = None
    domain_fingerprint: str | None = None
    redirect_count: int = 0
    redirect_digest: str | None = None
    exception_kind: ExceptionKind = ExceptionKind.NONE

    def __post_init__(self) -> None:
        if not isinstance(self.metric, MetricName):
            raise TypeError("metric must be a MetricName")
        if self.redirect_count < 0:
            raise ValueError("redirect count cannot be negative")
        if not isinstance(self.exception_kind, ExceptionKind):
            raise TypeError("exception kind must be an ExceptionKind")
        for value in (self.entity_fingerprint, self.domain_fingerprint, self.redirect_digest):
            if value is not None and (
                not isinstance(value, str)
                or len(value) != _FINGERPRINT_LENGTH
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError("fingerprints must be truncated SHA-256 hex")
        allowed = _ALLOWED_LABELS[self.metric]
        if set(self.labels) != set(allowed):
            raise ValueError(f"invalid labels for metric {self.metric.value}")
        for name, value in self.labels.items():
            if not isinstance(value, allowed[name]):
                raise TypeError(f"invalid value for {name}")
        object.__setattr__(self, "labels", MappingProxyType(dict(self.labels)))

    def as_dict(self) -> dict[str, object]:
        """Return JSON-ready safe fields; raw caller input is never retained."""
        result: dict[str, object] = {
            "metric": self.metric.value,
            "labels": {name: value.value for name, value in sorted(self.labels.items())},
            "redirect_count": self.redirect_count,
            "exception_kind": self.exception_kind.value,
        }
        if self.entity_fingerprint is not None:
            result["entity_fingerprint"] = self.entity_fingerprint
        if self.domain_fingerprint is not None:
            result["domain_fingerprint"] = self.domain_fingerprint
        if self.redirect_digest is not None:
            result["redirect_digest"] = self.redirect_digest
        return result


def event(
    metric: MetricName,
    *,
    labels: Mapping[str, StrEnum] | None = None,
    entity: object | None = None,
    domain: object | None = None,
    redirects: Sequence[object] = (),
    error: BaseException | None = None,
) -> RedactedEvent:
    """Build an event without retaining raw entities, domains, redirects, or errors."""
    if not isinstance(metric, MetricName):
        raise TypeError("metric must be a MetricName")
    safe_labels = dict(labels or {})
    redirect_values = tuple(redirects[:_MAX_REDIRECTS])
    return RedactedEvent(
        metric=metric,
        labels=safe_labels,
        entity_fingerprint=None if entity is None else fingerprint(entity),
        domain_fingerprint=None if domain is None else fingerprint(domain),
        redirect_count=len(redirect_values),
        redirect_digest=None if not redirect_values else fingerprint("\x1f".join(map(str, redirect_values))),
        exception_kind=_exception_kind(error),
    )


class ObservabilitySink(Protocol):
    def emit(self, event: RedactedEvent) -> None: ...


class NoopObservabilitySink:
    def emit(self, event: RedactedEvent) -> None:
        """Intentionally discard an already-redacted event."""
        if not isinstance(event, RedactedEvent):
            raise TypeError("sink accepts only RedactedEvent values")


class LoggingObservabilitySink:
    """Emit only pre-redacted structured events to the service journal."""

    def __init__(
        self,
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger("newsbot.v2.observability")

    def emit(self, event: RedactedEvent) -> None:
        if not isinstance(event, RedactedEvent):
            raise TypeError("sink accepts only RedactedEvent values")
        payload = json.dumps(
            event.as_dict(),
            sort_keys=True,
            separators=(",", ":"),
        )
        if event.metric is MetricName.ALERT:
            self._logger.critical(payload)
        else:
            self._logger.info(payload)


@dataclass(slots=True)
class InMemoryObservabilitySink:
    events: list[RedactedEvent] = field(default_factory=list)

    def emit(self, event: RedactedEvent) -> None:
        if not isinstance(event, RedactedEvent):
            raise TypeError("sink accepts only RedactedEvent values")
        self.events.append(event)


@dataclass(frozen=True, slots=True)
class ThresholdSnapshot:
    """Counters for one completed 15-minute fetch window and current V2 storage health."""

    fetch_total: int
    fetch_blocked: int = 0
    fetch_transient: int = 0
    database_bytes: int = 0
    wal_bytes: int = 0
    seven_day_storage_baseline_bytes: int = 1
    oldest_queue_age_seconds: int = 0
    oldest_manual_review_age_seconds: int = 0

    def __post_init__(self) -> None:
        values = (
            self.fetch_total,
            self.fetch_blocked,
            self.fetch_transient,
            self.database_bytes,
            self.wal_bytes,
            self.oldest_queue_age_seconds,
            self.oldest_manual_review_age_seconds,
        )
        if min(values) < 0:
            raise ValueError("threshold values cannot be negative")
        if (
            self.fetch_blocked > self.fetch_total
            or self.fetch_transient > self.fetch_total
            or self.fetch_blocked + self.fetch_transient > self.fetch_total
        ):
            raise ValueError("fetch result components cannot exceed fetch total")
        if self.seven_day_storage_baseline_bytes <= 0:
            raise ValueError("seven-day storage baseline must be positive")


def evaluate_thresholds(snapshot: ThresholdSnapshot) -> tuple[ThresholdAlert, ...]:
    """Return stable alerts for strict AC14 boundaries.

    Fetch counters are ratios from one completed 15-minute window. Combined
    database-plus-WAL growth is compared with the modeled seven-day storage
    baseline. Exact 20%, 2x, and 24-hour values do not alert; only values above
    them do.
    """
    checks = (
        (
            ThresholdAlert.FETCH_BLOCKED,
            snapshot.fetch_blocked * 5 > snapshot.fetch_total,
        ),
        (
            ThresholdAlert.FETCH_TRANSIENT,
            snapshot.fetch_transient * 5 > snapshot.fetch_total,
        ),
        (
            ThresholdAlert.DATABASE_GROWTH,
            snapshot.database_bytes + snapshot.wal_bytes > 2 * snapshot.seven_day_storage_baseline_bytes,
        ),
        (ThresholdAlert.OLDEST_QUEUE_AGE, snapshot.oldest_queue_age_seconds > 86_400),
        (
            ThresholdAlert.OLDEST_MANUAL_REVIEW_AGE,
            snapshot.oldest_manual_review_age_seconds > 86_400,
        ),
    )
    return tuple(alert for alert, triggered in checks if triggered)

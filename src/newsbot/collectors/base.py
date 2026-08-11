"""Normalized, provider-neutral source observations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

MessageKind = Literal["message", "service", "deleted", "unsupported"]


@dataclass(frozen=True, slots=True)
class UrlCandidate:
    """An outbound URL together with its Telegram-provided provenance."""

    url: str
    source: Literal["preview", "entity", "bare"] = "bare"
    title: str | None = None
    description: str | None = None
    occurrence: int = 0


@dataclass(frozen=True, slots=True)
class Engagement:
    """Observed Telegram counters; ``None`` means unknown, never zero."""

    views: int | None = None
    reactions: int | None = None
    forwards: int | None = None

    def __post_init__(self) -> None:
        for name in ("views", "reactions", "forwards"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be nonnegative")


@dataclass(frozen=True, slots=True)
class Media:
    kind: str
    caption: str | None = None
    identity: str | None = None
    is_service: bool = False


@dataclass(frozen=True, slots=True)
class SourceObservation:
    """Immutable normalized snapshot consumed by identity and ranking code."""

    channel_id: str
    channel_handle: str
    external_post_id: str
    published_at: datetime
    text: str = ""
    edited_at: datetime | None = None
    observed_at: datetime | None = None
    preview_title: str | None = None
    preview_description: str | None = None
    kind: MessageKind = "message"
    sponsored: bool = False
    urls: tuple[UrlCandidate, ...] = ()
    media: tuple[Media, ...] = ()
    engagement: Engagement = field(default_factory=Engagement)
    conflicts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.channel_id or not self.external_post_id:
            raise ValueError("channel_id and external_post_id are required")
        for name in ("published_at", "edited_at", "observed_at"):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware")


class Collector(Protocol):
    """A credential boundary: collection yields observations and nothing else."""

    def collect(self, channel: object, **kwargs: object) -> Sequence[SourceObservation]: ...

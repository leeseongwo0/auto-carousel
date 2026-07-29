"""Validated, traceable editorial copy for a selected news candidate."""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass


class CopyValidationError(ValueError):
    """Raised when generated copy cannot enter human review."""


@dataclass(frozen=True, slots=True)
class FactReference:
    """A generated factual unit's reference to a server-owned claim."""

    claim_id: str
    source_version_id: int

    def __post_init__(self) -> None:
        if not self.claim_id:
            raise CopyValidationError("claim_id must not be empty")
        if self.source_version_id <= 0:
            raise CopyValidationError("source_version_id must be positive")


@dataclass(frozen=True, slots=True)
class FactualUnit:
    """One factual statement and its exact source-claim references."""

    text: str
    references: tuple[FactReference, ...]


@dataclass(frozen=True, slots=True)
class CoverPage:
    title: str
    subtitle: str = ""
    factual_units: tuple[FactualUnit, ...] = ()


@dataclass(frozen=True, slots=True)
class BodyPage:
    subtitle: str
    body: str
    factual_units: tuple[FactualUnit, ...] = ()


@dataclass(frozen=True, slots=True)
class Caption:
    """Standalone caption. Its size is deliberately not constrained here."""

    hook: str
    context: str
    details: str
    implications: str
    questions: str
    hashtags: tuple[str, ...]

    @property
    def text(self) -> str:
        parts = (self.hook, self.context, self.details, self.implications, self.questions)
        return "\n\n".join((*parts, " ".join(self.hashtags)))


@dataclass(frozen=True, slots=True)
class CopyDraft:
    """One cover and zero to seven body pages, plus an independent caption."""

    cover: CoverPage
    bodies: tuple[BodyPage, ...]
    caption: Caption
    draft: bool = True
    source_reported: bool = True

    @property
    def page_count(self) -> int:
        return 1 + len(self.bodies)

    @staticmethod
    def adaptive_page_count(source_texts: Iterable[str]) -> int:
        return adaptive_page_count(source_texts)


def adaptive_page_count(source_texts: Iterable[str]) -> int:
    """Choose a bounded, repeatable page count from selected source material."""
    texts = tuple(text for text in source_texts if isinstance(text, str))
    characters = sum(len(text.strip()) for text in texts)
    sentences = sum(sum(text.count(marker) for marker in (".", "!", "?", "…", "\n")) for text in texts)
    return min(8, max(1, len(texts), (characters + 599) // 600, (sentences + 2) // 3))


def _require_nfc(value: str, field: str) -> None:
    if not isinstance(value, str):
        raise CopyValidationError(f"{field} must be text")
    if unicodedata.normalize("NFC", value) != value:
        raise CopyValidationError(f"{field} must be NFC normalized")
    if value != value.strip():
        raise CopyValidationError(f"{field} must not have outer whitespace")


def _validate_text(value: str, field: str, limit: int | None = None) -> None:
    _require_nfc(value, field)
    if limit is not None and len(value) > limit:
        raise CopyValidationError(f"{field} exceeds {limit} Unicode code points")


def _validate_units(
    units: Iterable[FactualUnit],
    allowed_claim_sources: dict[str, int],
    field: str,
) -> None:
    values = tuple(units)
    if not values:
        raise CopyValidationError(f"{field}.factual_units must not be empty")
    for unit_index, unit in enumerate(values):
        _validate_text(unit.text, f"{field}.factual_units[{unit_index}].text")
        if not unit.text:
            raise CopyValidationError(f"{field}.factual_units[{unit_index}].text must not be empty")
        if not unit.references:
            raise CopyValidationError(f"{field}.factual_units[{unit_index}] has no source reference")
        for reference in unit.references:
            source_version_id = allowed_claim_sources.get(reference.claim_id)
            if source_version_id is None:
                raise CopyValidationError(f"{field} references an unknown claim")
            if source_version_id != reference.source_version_id:
                raise CopyValidationError(f"{field} claim source does not match")


def validate_copy(
    draft: CopyDraft,
    *,
    allowed_claim_sources: dict[str, int],
    expected_page_count: int | None = None,
) -> CopyDraft:
    """Validate pagination, editorial limits, and claim/source reference integrity.

    Limits intentionally use Python ``len`` after NFC validation: that counts
    Unicode code points, including spaces and newlines, rather than bytes or
    UTF-16 units.  Callers must split captions only at Telegram transport time.
    """

    if not isinstance(draft, CopyDraft):
        raise CopyValidationError("draft must be a CopyDraft")
    if not draft.draft or not draft.source_reported:
        raise CopyValidationError("draft/source_reported markers are required")
    if not 1 <= draft.page_count <= 8:
        raise CopyValidationError("a draft must have 1 through 8 total pages")
    if expected_page_count is not None and draft.page_count != expected_page_count:
        raise CopyValidationError("draft page count does not match its generation request")

    _validate_text(draft.cover.title, "cover.title")
    _validate_text(draft.cover.subtitle, "cover.subtitle", 35)
    _validate_units(draft.cover.factual_units, allowed_claim_sources, "cover")
    for index, body in enumerate(draft.bodies):
        _validate_text(body.subtitle, f"bodies[{index}].subtitle", 35)
        _validate_text(body.body, f"bodies[{index}].body", 240)
        _validate_units(body.factual_units, allowed_claim_sources, f"bodies[{index}]")

    for name, value in (
        ("caption.hook", draft.caption.hook),
        ("caption.context", draft.caption.context),
        ("caption.details", draft.caption.details),
        ("caption.implications", draft.caption.implications),
        ("caption.questions", draft.caption.questions),
    ):
        _validate_text(value, name)
        if not value:
            raise CopyValidationError(f"{name} must not be empty")
    if not draft.caption.hashtags:
        raise CopyValidationError("caption.hashtags must not be empty")
    for index, hashtag in enumerate(draft.caption.hashtags):
        _validate_text(hashtag, f"caption.hashtags[{index}]")
        if not hashtag or not hashtag.startswith("#"):
            raise CopyValidationError("caption hashtags must start with '#'")
    return draft

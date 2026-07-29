"""Generation provider boundary.

Importing this module never imports an AI SDK or opens a network connection.  A
provider is constructed only by the selected generation-job processor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from newsbot.copywriting import CopyDraft


@dataclass(frozen=True, slots=True)
class FactClaim:
    """An immutable, server-owned factual packet a provider may only cite."""

    id: str
    source_version_id: int
    source_identity: str
    material_identity: str
    observation_identity: str
    captured_at: str
    source_url: str | None
    evidence: str
    evidence_spans: tuple[tuple[int, int], ...]
    conflicts: tuple[str, ...]
    uncertainty: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.id.startswith("claim_"):
            raise ValueError("claim id must be content-addressed")
        if self.source_version_id <= 0:
            raise ValueError("source_version_id must be positive")
        if not all((self.source_identity, self.material_identity, self.observation_identity, self.captured_at)):
            raise ValueError("fact identity and capture time must not be empty")
        if not self.evidence:
            raise ValueError("evidence must not be empty")
        if not self.evidence_spans or any(
            start < 0 or end <= start or end > len(self.evidence) for start, end in self.evidence_spans
        ):
            raise ValueError("evidence spans must be non-empty ranges within evidence")


@dataclass(frozen=True, slots=True)
class GenerationRequest:
    """The immutable, selection-bound input passed to a provider."""

    candidate_id: int
    source_version_ids: tuple[int, ...]
    page_count: int
    facts: tuple[FactClaim, ...]
    locale: str = "ko-KR"

    def __post_init__(self) -> None:
        if self.candidate_id <= 0:
            raise ValueError("candidate_id must be positive")
        if not self.source_version_ids or any(item <= 0 for item in self.source_version_ids):
            raise ValueError("source_version_ids must contain positive identifiers")
        if tuple(sorted(set(self.source_version_ids))) != self.source_version_ids:
            raise ValueError("source_version_ids must be sorted and unique")
        if not 1 <= self.page_count <= 8:
            raise ValueError("page_count must be between 1 and 8")
        allowed_sources = set(self.source_version_ids)
        if any(fact.source_version_id not in allowed_sources for fact in self.facts):
            raise ValueError("facts must belong to the selected source versions")


class ProviderError(RuntimeError):
    """A recoverable provider transport or response failure."""


@runtime_checkable
class GenerationProvider(Protocol):
    """Port for explicitly selected generation capabilities."""

    async def generate(self, request: GenerationRequest) -> CopyDraft:
        """Generate a draft for one already-selected generation job."""

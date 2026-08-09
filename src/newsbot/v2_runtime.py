"""Credential-free orchestration boundary for the Newsbot V2 workflow.

Adapters are deliberately callbacks: production integrations can be supplied by a
caller, while tests can use deterministic functions without Telegram or Sheets.
"""

from __future__ import annotations

from collections.abc import Callable
from enum import StrEnum
from typing import Protocol

from .collectors.base import SourceObservation
from .v2_workflow import V2Candidate, V2Draft, V2State, V2Workflow


class ClearNetworkFailure(RuntimeError):
    """A typed, retryable failure with no remote side effect."""


class AmbiguousRemoteEffect(RuntimeError):
    """The remote result is unknown; retrying could duplicate an effect."""


class RemoteEffect(StrEnum):
    CONFIRMED = "confirmed"
    AMBIGUOUS = "ambiguous"


class CandidateNotifier(Protocol):
    def __call__(self, candidate: V2Candidate) -> bool | RemoteEffect: ...


class DraftGenerator(Protocol):
    def __call__(self, candidate: V2Candidate) -> str: ...


class DraftNotifier(Protocol):
    def __call__(self, draft: V2Draft) -> bool | RemoteEffect: ...


class SheetsDeliverer(Protocol):
    def __call__(self, draft: V2Draft) -> RemoteEffect | bool: ...


type CandidateNotification = CandidateNotifier
type DraftGeneration = DraftGenerator
type FinalDraftNotification = DraftNotifier
type SheetsDelivery = SheetsDeliverer


class V2Runtime:
    """Drive one candidate through both approvals and Sheets delivery.

    A callback returning ``RemoteEffect.AMBIGUOUS`` (or raising
    :class:`AmbiguousRemoteEffect`) is never retried and moves the entity to
    ``manual_review``. Only :class:`ClearNetworkFailure` is retried, up to the
    configured bound.
    """

    def __init__(
        self,
        workflow: V2Workflow,
        *,
        notify_candidate: CandidateNotifier,
        generate_draft: DraftGenerator,
        notify_draft: DraftNotifier,
        deliver_sheets: SheetsDeliverer,
        max_retries: int = 1,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self.workflow = workflow
        self.notify_candidate = notify_candidate
        self.generate_draft = generate_draft
        self.notify_draft = notify_draft
        self.deliver_sheets = deliver_sheets
        self.max_retries = max_retries

    def process_observation(self, observation: SourceObservation) -> V2Candidate | V2Draft | None:
        candidate = self.workflow.record_observation(observation)
        if candidate is None:
            return None
        return self.run(candidate.id)

    def run(self, candidate_id: str) -> V2Candidate | V2Draft:
        candidate = self.workflow.get_candidate(candidate_id)
        if candidate.state in (V2State.MANUAL_REVIEW, V2State.SHEET_DELIVERED):
            return candidate

        if candidate.state == V2State.PENDING_CANDIDATE:
            result = self._remote(self.notify_candidate, candidate, candidate.id, "candidate_notification")
            if result is None:
                return self.workflow.mark_manual_review(candidate.id, "candidate notification unclear")
            if result is False:
                return candidate
            candidate = self.workflow.approve_candidate(candidate.id)

        if candidate.state == V2State.CANDIDATE_APPROVED:
            try:
                content = self._generate(candidate)
            except (AmbiguousRemoteEffect, ClearNetworkFailure) as exc:
                reason = (
                    "draft generation unclear"
                    if isinstance(exc, AmbiguousRemoteEffect)
                    else "draft generation network failure"
                )
                return self.workflow.mark_manual_review(candidate.id, reason)
            draft = self.workflow.create_draft(candidate.id, content)
        else:
            draft = next(iter(self._drafts_for(candidate.id)), None)
            if draft is None:
                return self.workflow.mark_manual_review(candidate.id, "missing draft")

        if draft.state == V2State.DRAFT_PENDING_APPROVAL:
            result = self._remote(self.notify_draft, draft, draft.id, "draft_notification")
            if result is None:
                return self.workflow.mark_manual_review(draft.id, "draft notification unclear")
            if result is False:
                return draft
            draft = self.workflow.approve_draft(draft.id)

        if draft.state == V2State.DRAFT_APPROVED:
            result = self._remote(self.deliver_sheets, draft, draft.id, "sheets_delivery")
            if result is None:
                return self.workflow.mark_manual_review(draft.id, "sheets delivery unclear")
            if result is False:
                return self.workflow.mark_manual_review(draft.id, "sheets delivery not confirmed")
            return self.workflow.mark_sheet_delivered(draft.id)
        return draft

    def _drafts_for(self, candidate_id: str) -> list[V2Draft]:
        candidate = self.workflow.get_candidate(candidate_id)
        if candidate.state == V2State.CANDIDATE_APPROVED:
            return []
        draft = self.workflow.get_draft_for_candidate(candidate_id)
        return [] if draft is None else [draft]

    def _generate(self, candidate: V2Candidate) -> str:
        for attempt in range(self.max_retries + 1):
            self.workflow.record_remote_attempt(candidate.id, "draft_generation")
            try:
                content = self.generate_draft(candidate)
                self.workflow.settle_remote_effect(candidate.id, "draft_generation", "confirmed")
                return content
            except AmbiguousRemoteEffect as exc:
                self.workflow.settle_remote_effect(candidate.id, "draft_generation", "ambiguous", str(exc))
                raise
            except ClearNetworkFailure as exc:
                self.workflow.settle_remote_effect(candidate.id, "draft_generation", "failed", str(exc))
                if attempt >= self.max_retries:
                    raise
        raise AssertionError("unreachable")

    def _remote(self, callback: Callable, value, entity_id: str, stage: str) -> bool | None:
        for attempt in range(self.max_retries + 1):
            self.workflow.record_remote_attempt(entity_id, stage)
            try:
                result = callback(value)
                if result is RemoteEffect.AMBIGUOUS:
                    self.workflow.settle_remote_effect(entity_id, stage, "ambiguous")
                    return None
                self.workflow.settle_remote_effect(entity_id, stage, "confirmed" if result else "failed")
                return bool(result)
            except AmbiguousRemoteEffect as exc:
                self.workflow.settle_remote_effect(entity_id, stage, "ambiguous", str(exc))
                return None
            except ClearNetworkFailure as exc:
                self.workflow.settle_remote_effect(entity_id, stage, "failed", str(exc))
                if attempt >= self.max_retries:
                    return None
        return None


Runtime = V2Runtime
V2Orchestrator = V2Runtime

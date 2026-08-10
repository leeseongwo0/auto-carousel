"""Production-facing ports for the isolated asynchronous Newsbot V2 flow.

This module deliberately does not import ``NewsPipeline``, ranking, or news policy.
It records a send receipt before any later approval callback can advance the state.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import secrets
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from .ai.base import FactClaim, GenerationProvider, GenerationRequest
from .ai.codex_cli import CodexCliProvider
from .approval.base import hash_callback_token
from .collectors.base import SourceObservation
from .copywriting import CopyDraft
from .sheets.base import DeliveryOutcome, SheetDelivery
from .v2_runtime import AmbiguousRemoteEffect, ClearNetworkFailure
from .v2_workflow import V2Candidate, V2Draft, V2State, V2Workflow


class CandidateNotificationPort(Protocol):
    def __call__(self, candidate: V2Candidate, callback_token: str) -> str | bool: ...


class DraftNotificationPort(Protocol):
    def __call__(self, draft: V2Draft, callback_token: str) -> str | bool: ...


class GenerationPort(Protocol):
    def __call__(self, candidate: V2Candidate) -> str: ...


class SheetsDeliveryPort(Protocol):
    def __call__(self, draft: V2Draft) -> SheetDelivery | bool: ...


class V2LiveWorkflow:
    """Stateful V2 adapter coordinator with asynchronous capability settlement."""

    def __init__(
        self,
        workflow: V2Workflow,
        *,
        notify_candidate: CandidateNotificationPort,
        generate_draft: GenerationPort,
        notify_draft: DraftNotificationPort,
        deliver_sheets: SheetsDeliveryPort,
        max_retries: int = 1,
        callback_ttl: timedelta = timedelta(days=1),
    ) -> None:
        if max_retries < 0 or callback_ttl <= timedelta():
            raise ValueError("invalid live workflow limits")
        self.workflow = workflow
        self.notify_candidate = notify_candidate
        self.generate_draft = generate_draft
        self.notify_draft = notify_draft
        self.deliver_sheets = deliver_sheets
        self.max_retries = max_retries
        self.callback_ttl = callback_ttl

    def process_observation(self, observation: SourceObservation) -> V2Candidate | V2Draft | None:
        candidate = self.workflow.record_observation(observation)
        return None if candidate is None else self.run(candidate.id)

    def run(self, candidate_id: str) -> V2Candidate | V2Draft:
        candidate = self.workflow.get_candidate(candidate_id)
        if candidate.state in {V2State.PENDING_CANDIDATE, V2State.MANUAL_REVIEW, V2State.SHEET_DELIVERED}:
            if candidate.state == V2State.PENDING_CANDIDATE:
                return self._notify_candidate(candidate)
            return candidate
        if candidate.state == V2State.CANDIDATE_APPROVED:
            generated = self._generate(candidate)
            if isinstance(generated, V2Candidate):
                return generated
            draft = generated
        else:
            existing_draft = self.workflow.get_draft_for_candidate(candidate_id)
            if existing_draft is None:
                return self.workflow.mark_manual_review(candidate_id, "approved candidate has no draft")
            draft = existing_draft
        if draft.state == V2State.DRAFT_PENDING_APPROVAL:
            return self._notify_draft(draft)
        if draft.state == V2State.DRAFT_APPROVED:
            return self._deliver(draft)
        return draft

    def settle_callback(self, token: str, stage: str) -> V2Candidate | V2Draft | None:
        """Authenticate and consume a capability; plaintext entity IDs never settle V2."""
        if stage not in {"candidate", "draft"}:
            return None
        try:
            token_hash = hash_callback_token(token)
        except ValueError:
            return None
        entity_id = self.workflow.consume_callback(token_hash, stage, self._now())
        if entity_id is None:
            return None
        if stage == "candidate":
            self.workflow.approve_candidate(entity_id)
            return self.run(entity_id)
        draft = self.workflow.approve_draft(entity_id)
        return self.run(draft.candidate_id)

    def _notify_candidate(self, candidate: V2Candidate) -> V2Candidate:
        if self._prior_remote(candidate.id, "candidate_notification"):
            return self.workflow.get_candidate(candidate.id)
        token = self._issue_callback(candidate.id, "candidate")
        outcome = self._effect(candidate.id, "candidate_notification", lambda: self.notify_candidate(candidate, token))
        if outcome is None:
            return cast(
                V2Candidate,
                self.workflow.mark_manual_review(candidate.id, "candidate notification outcome ambiguous"),
            )
        return self.workflow.get_candidate(candidate.id)

    def _generate(self, candidate: V2Candidate) -> V2Candidate | V2Draft:
        if self._prior_remote(candidate.id, "draft_generation"):
            draft = self.workflow.get_draft_for_candidate(candidate.id)
            return draft or self.workflow.mark_manual_review(candidate.id, "generation receipt has no draft")
        outcome = self._effect(candidate.id, "draft_generation", lambda: self.generate_draft(candidate))
        if outcome is None or not isinstance(outcome, str) or not outcome:
            return self.workflow.mark_manual_review(candidate.id, "draft generation outcome ambiguous")
        return self.workflow.create_draft(candidate.id, outcome)

    def _notify_draft(self, draft: V2Draft) -> V2Draft:
        if self._prior_remote(draft.id, "draft_notification"):
            return self.workflow.get_draft(draft.id)
        token = self._issue_callback(draft.id, "draft")
        outcome = self._effect(draft.id, "draft_notification", lambda: self.notify_draft(draft, token))
        if outcome is None:
            return cast(V2Draft, self.workflow.mark_manual_review(draft.id, "draft notification outcome ambiguous"))
        return self.workflow.get_draft(draft.id)

    def _deliver(self, draft: V2Draft) -> V2Draft:
        if self._prior_remote(draft.id, "sheets_delivery"):
            return self.workflow.get_draft(draft.id)
        outcome = self._effect(draft.id, "sheets_delivery", lambda: self.deliver_sheets(draft))
        if outcome is None or outcome is False:
            return cast(V2Draft, self.workflow.mark_manual_review(draft.id, "sheets delivery outcome ambiguous"))
        if isinstance(outcome, SheetDelivery) and outcome.outcome is not DeliveryOutcome.APPLIED:
            return cast(V2Draft, self.workflow.mark_manual_review(draft.id, f"sheets delivery {outcome.outcome}"))
        return self.workflow.mark_sheet_delivered(draft.id)

    def _prior_remote(self, entity_id: str, stage: str) -> bool:
        receipt = self.workflow.remote_effect(entity_id, stage)
        if receipt is None:
            return False
        if receipt["status"] == "ambiguous":
            self.workflow.mark_manual_review(entity_id, f"{stage} outcome ambiguous")
            return True
        return receipt["status"] == "confirmed"

    def _issue_callback(self, entity_id: str, stage: str) -> str:
        token = secrets.token_urlsafe(32)
        self.workflow.issue_callback(
            hash_callback_token(token), entity_id, stage, (datetime.now(UTC) + self.callback_ttl).isoformat()
        )
        return token

    def _effect(self, entity_id: str, stage: str, call: Callable[[], Any]) -> Any | None:
        for attempt in range(self.max_retries + 1):
            self.workflow.record_remote_attempt(entity_id, stage)
            try:
                value = call()
                if value is False:
                    self.workflow.settle_remote_effect(entity_id, stage, "ambiguous")
                    return None
                receipt = value if isinstance(value, str) else "confirmed"
                self.workflow.settle_remote_effect(entity_id, stage, "confirmed", receipt_id=receipt)
                return value
            except ClearNetworkFailure as exc:
                self.workflow.settle_remote_effect(entity_id, stage, "failed", str(exc))
                if attempt == self.max_retries:
                    return None
            except Exception as exc:
                self.workflow.settle_remote_effect(entity_id, stage, "ambiguous", type(exc).__name__)
                return None
        return None

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()


class V2CodexGenerator:
    """Build one evidence-bound request for the fixed privilege-separated Codex runner."""

    def __init__(self, provider: GenerationProvider | None = None) -> None:
        self.provider = provider or CodexCliProvider()

    def __call__(self, candidate: V2Candidate) -> str:
        text = str(candidate.observation.get("text", "")).strip()
        if not text:
            raise AmbiguousRemoteEffect("V2 candidate has no generation evidence")
        urls = candidate.observation.get("urls", [])
        source_url = str(urls[0]) if isinstance(urls, list) and urls else None
        source_version_id = self._positive_id(f"source:{candidate.channel_id}:{candidate.external_post_id}")
        claim_id = (
            "claim_"
            + hashlib.sha256(f"{candidate.channel_id}:{candidate.external_post_id}:{text}".encode()).hexdigest()
        )
        claim = FactClaim(
            id=claim_id,
            source_version_id=source_version_id,
            source_identity=candidate.channel_id,
            material_identity=f"{candidate.channel_id}:{candidate.external_post_id}",
            observation_identity=f"{candidate.channel_id}:{candidate.external_post_id}",
            captured_at=str(candidate.observation["published_at"]),
            source_url=source_url,
            evidence=text,
            evidence_spans=((0, len(text)),),
            conflicts=(),
            uncertainty=(),
        )
        request = GenerationRequest(
            candidate_id=self._positive_id(f"candidate:{candidate.id}"),
            source_version_ids=(source_version_id,),
            page_count=CopyDraft.adaptive_page_count((text,)),
            facts=(claim,),
            locale="ko-KR",
            flexible_page_count=True,
        )
        draft = asyncio.run(self.provider.generate(request))
        return json.dumps(asdict(draft), ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _positive_id(value: str) -> int:
        return int(hashlib.sha256(value.encode()).hexdigest()[:15], 16) or 1


class GoogleSheetsV2Delivery:
    """Adapt the established prepared-mutation contract to a V2 exact draft."""

    def __init__(self, adapter: Any, values: Callable[[V2Draft], Sequence[str]] | None = None) -> None:
        self.adapter = adapter
        self.values = values or (lambda draft: (draft.content,))

    def __call__(self, draft: V2Draft) -> SheetDelivery:
        import hashlib

        canonical = draft.content.encode("utf-8")
        prepared = self.adapter.prepare_delivery(
            export_id=draft.id,
            canonical_sha256=hashlib.sha256(canonical).hexdigest(),
            values=self.values(draft),
        )
        if not prepared.metadata_value:
            raise AmbiguousRemoteEffect("Sheets delivery lacks an idempotency marker")
        probe = self.adapter.probe(metadata_value=prepared.metadata_value)
        if probe.metadata.value == "exact":
            return SheetDelivery(DeliveryOutcome.APPLIED)
        if probe.metadata.value != "absent":
            return SheetDelivery(DeliveryOutcome.AMBIGUOUS)
        self.adapter.arm_prepared_dispatch()
        return cast(SheetDelivery, self.adapter.dispatch_prepared(prepared))


class TelegramV2Notifier:
    """One-attempt V2 approval sender backed by the existing Telegram transport."""

    def __init__(self, adapter: Any, *, deadline_seconds: float = 30) -> None:
        if deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")
        self.adapter = adapter
        self.deadline_seconds = deadline_seconds

    def candidate(self, candidate: V2Candidate, token: str) -> str:
        return self._send(f"V2 candidate {candidate.id}\n{candidate.observation['text']}", token)

    def draft(self, draft: V2Draft, token: str) -> str:
        return self._send(f"V2 exact draft {draft.id}\n{draft.content}", token)

    def _send(self, text: str, token: str) -> str:
        from .approval.telegram import TelegramDeadline

        result = self.adapter.send_message_once(
            text,
            markup={"inline_keyboard": [[{"text": "Approve", "callback_data": token}]]},
            deadline=TelegramDeadline.after(self.deadline_seconds),
        )
        if not result.accepted:
            raise AmbiguousRemoteEffect(result.safe_code or "Telegram send not confirmed")
        assert result.message_id is not None
        return str(result.message_id)


class TelethonV2Collector:
    """Collect exactly the configured handles through the existing Telethon adapter."""

    def __init__(self, collector: Any, handles: Sequence[str]) -> None:
        self.collector = collector
        self.handles = tuple(handle.lstrip("@") for handle in handles if handle.strip())
        if not self.handles:
            raise ValueError("at least one configured Telegram handle is required")

    async def collect(self) -> tuple[SourceObservation, ...]:
        observations: list[SourceObservation] = []
        for handle in self.handles:
            result = self.collector.collect(handle)
            if inspect.isawaitable(result):
                result = await result
            observations.extend(result)
        return tuple(observations)

    def collect_sync(self) -> tuple[SourceObservation, ...]:
        return asyncio.run(self.collect())

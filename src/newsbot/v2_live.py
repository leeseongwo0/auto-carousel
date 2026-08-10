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

from .ai.structured_copy import draft_from_mapping
from .approval.base import hash_callback_token
from .collectors.base import SourceObservation
from .copywriting import validate_copy
from .sheets.base import DeliveryOutcome, MetadataState, SheetDelivery
from .sheets.schema import project_handoff
from .v2_codex import exact_review_text
from .v2_runtime import AmbiguousRemoteEffect, ClearNetworkFailure
from .v2_workflow import V2Candidate, V2Draft, V2State, V2Workflow, V2WorkflowError


class SheetsClearPreDispatchNetworkError(RuntimeError):
    """Sheets transport proved it failed before any remote dispatch."""


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
        receipt = self.workflow.remote_effect(draft.id, "sheets_delivery")
        if receipt is not None:
            if receipt["status"] == "ambiguous":
                return cast(
                    V2Draft,
                    self.workflow.mark_manual_review(draft.id, "sheets_delivery outcome ambiguous"),
                )
            if receipt["status"] == "confirmed":
                return self.workflow.mark_sheet_delivered(draft.id)
            if receipt["status"] == "pending":
                return cast(
                    V2Draft,
                    self.workflow.mark_manual_review(draft.id, "sheets_delivery interrupted with unknown outcome"),
                )
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
        if receipt["status"] == "pending":
            self.workflow.mark_manual_review(entity_id, f"{stage} interrupted with unknown outcome")
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
                if isinstance(value, SheetDelivery) and value.outcome is not DeliveryOutcome.APPLIED:
                    self.workflow.settle_remote_effect(
                        entity_id,
                        stage,
                        "ambiguous",
                        detail=f"delivery outcome {value.outcome.value}",
                    )
                    return value
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


def v2_draft_handoff_values(draft: V2Draft, approved_date: str) -> tuple[str, ...]:
    """Project the immutable V2 CopyDraft JSON onto the frozen A:V Sheets schema."""
    try:
        parsed = draft_from_mapping(json.loads(draft.content))
        pages = [(parsed.cover.title, parsed.cover.subtitle)] + [(body.subtitle, body.body) for body in parsed.bodies]
        allowed_claim_sources = {
            reference.claim_id: reference.source_version_id
            for units in (parsed.cover.factual_units, *(body.factual_units for body in parsed.bodies))
            for factual_unit in units
            for reference in factual_unit.references
        }
        validate_copy(parsed, allowed_claim_sources=allowed_claim_sources)
        return project_handoff(
            approved_date=approved_date,
            page_count=parsed.page_count,
            category=parsed.category,
            caption=parsed.caption.text,
            pages=pages,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("invalid V2 exact draft projection") from exc


def v2_sheet_export_id(draft_id: str) -> str:
    """Map an immutable V2 draft identity onto the frozen Sheets export namespace."""
    if not draft_id:
        raise ValueError("draft_id must not be empty")
    return "exp_" + hashlib.sha256(f"newsbot-v2-draft:{draft_id}".encode()).hexdigest()[:32]


def _v2_sheets_receipt_detail(previous: str, phase: str, **updates: object) -> str:
    try:
        detail = json.loads(previous)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise V2WorkflowError("invalid Sheets delivery receipt detail") from exc
    if not isinstance(detail, dict):
        raise V2WorkflowError("invalid Sheets delivery receipt detail")
    detail.update(updates)
    detail["phase"] = phase
    return json.dumps(detail, sort_keys=True)


def deliver_v2_google_sheets(
    workflow: V2Workflow,
    draft: V2Draft,
    adapter: Any,
    values: Sequence[str],
    *,
    lease_seconds: float,
) -> V2Draft:
    """Deliver exactly once behind a durable claim and post-marker fencing boundary."""
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be positive")
    if draft.state == V2State.SHEET_DELIVERED:
        return draft
    if draft.state != V2State.DRAFT_APPROVED:
        raise V2WorkflowError("V2 Google Sheets delivery requires draft_approved")

    existing = workflow.remote_effect(draft.id, "sheets_delivery")
    if existing is not None and existing["status"] != "failed":
        return _recover_v2_sheets_receipt(workflow, draft, existing)

    canonical_sha256 = hashlib.sha256(draft.content.encode("utf-8")).hexdigest()
    lease_expires_at = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
    claim_detail = json.dumps(
        {
            "lease_expires_at": lease_expires_at,
            "owner": secrets.token_hex(16),
            "phase": "preparing",
            "request_sha256": canonical_sha256,
        },
        sort_keys=True,
    )
    if not workflow.claim_remote_effect(draft.id, "sheets_delivery", claim_detail):
        receipt = workflow.remote_effect(draft.id, "sheets_delivery")
        if receipt is None:
            raise V2WorkflowError("Sheets delivery claim disappeared")
        if receipt["status"] == "failed":
            return cast(
                V2Draft,
                workflow.mark_manual_review(draft.id, "sheets_delivery retry is not authorized"),
            )
        return _recover_v2_sheets_receipt(workflow, draft, receipt)

    try:
        prepared = adapter.prepare_delivery(
            export_id=v2_sheet_export_id(draft.id),
            canonical_sha256=canonical_sha256,
            values=values,
        )
        if not prepared.metadata_value:
            raise V2WorkflowError("Sheets delivery lacks an idempotency marker")
    except SheetsClearPreDispatchNetworkError as exc:
        return _settle_v2_sheets_pre_dispatch_failure(
            workflow,
            draft,
            claim_detail,
            exc,
            clear_network=True,
        )
    except Exception as exc:
        return _settle_v2_sheets_pre_dispatch_failure(workflow, draft, claim_detail, exc)

    prepared_detail = _v2_sheets_receipt_detail(
        claim_detail,
        "claimed",
        metadata_value=prepared.metadata_value,
        request_sha256=prepared.request_sha256,
    )
    if not workflow.update_remote_effect_claim(draft.id, "sheets_delivery", claim_detail, prepared_detail):
        return cast(
            V2Draft,
            workflow.mark_manual_review(draft.id, "sheets_delivery interrupted during preparation"),
        )
    claim_detail = prepared_detail
    if prepared.metadata is MetadataState.EXACT:
        settled = workflow.settle_remote_effect_claim(
            draft.id,
            "sheets_delivery",
            claim_detail,
            "confirmed",
            detail=_v2_sheets_receipt_detail(claim_detail, "settled", outcome="exact"),
            receipt_id=prepared.metadata_value,
        )
        if not settled:
            return cast(
                V2Draft,
                workflow.mark_manual_review(draft.id, "sheets_delivery interrupted before exact settlement"),
            )
        return workflow.mark_sheet_delivered(draft.id)
    if prepared.metadata is not MetadataState.ABSENT:
        return _settle_v2_sheets_ambiguous(
            workflow,
            draft,
            claim_detail,
            f"preflight metadata {prepared.metadata.value}",
        )
    try:
        attestation = adapter.dispatch_credential_attestation()
        adapter.arm_prepared_dispatch()
    except SheetsClearPreDispatchNetworkError as exc:
        return _settle_v2_sheets_pre_dispatch_failure(
            workflow,
            draft,
            claim_detail,
            exc,
            clear_network=True,
        )
    except Exception as exc:
        return _settle_v2_sheets_pre_dispatch_failure(workflow, draft, claim_detail, exc)

    possibly_sent_detail = _v2_sheets_receipt_detail(
        claim_detail,
        "possibly_sent",
        attestation=asdict(attestation),
    )
    if not workflow.update_remote_effect_claim(
        draft.id,
        "sheets_delivery",
        claim_detail,
        possibly_sent_detail,
    ):
        return cast(
            V2Draft,
            workflow.mark_manual_review(draft.id, "sheets_delivery interrupted before dispatch"),
        )

    try:
        outcome = cast(SheetDelivery, adapter.dispatch_prepared(prepared))
    except Exception as exc:
        return _settle_v2_sheets_ambiguous(
            workflow,
            draft,
            possibly_sent_detail,
            f"dispatch raised {type(exc).__name__}",
        )
    if outcome.outcome is not DeliveryOutcome.APPLIED:
        return _settle_v2_sheets_ambiguous(
            workflow,
            draft,
            possibly_sent_detail,
            f"dispatch returned {outcome.outcome.value}",
        )
    settled = workflow.settle_remote_effect_claim(
        draft.id,
        "sheets_delivery",
        possibly_sent_detail,
        "confirmed",
        detail=_v2_sheets_receipt_detail(
            possibly_sent_detail,
            "settled",
            outcome=outcome.outcome.value,
        ),
        receipt_id=prepared.metadata_value,
    )
    if not settled:
        raise V2WorkflowError("Sheets delivery claim changed after dispatch")
    return workflow.mark_sheet_delivered(draft.id)


def recover_v2_google_sheets_delivery(workflow: V2Workflow, draft: V2Draft) -> V2Draft:
    """Recover a durable Sheets receipt without constructing a dispatch adapter."""
    receipt = workflow.remote_effect(draft.id, "sheets_delivery")
    if receipt is None or receipt["status"] == "failed":
        raise V2WorkflowError("Sheets delivery has no durable recovery receipt")
    return _recover_v2_sheets_receipt(workflow, draft, receipt, allow_active_pending=True)


def _recover_v2_sheets_receipt(
    workflow: V2Workflow,
    draft: V2Draft,
    receipt: dict[str, object],
    *,
    allow_active_pending: bool = False,
) -> V2Draft:
    status = str(receipt["status"])
    if status == "confirmed":
        return workflow.mark_sheet_delivered(draft.id)
    if status == "ambiguous":
        return cast(
            V2Draft,
            workflow.mark_manual_review(draft.id, "sheets_delivery outcome ambiguous"),
        )
    if status != "pending":
        raise V2WorkflowError(f"unsupported Sheets delivery receipt status: {status}")
    detail = str(receipt["detail"])
    try:
        lease_expires_at = datetime.fromisoformat(str(json.loads(detail)["lease_expires_at"]))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise V2WorkflowError("invalid pending Sheets delivery receipt") from exc
    if lease_expires_at > datetime.now(UTC):
        if allow_active_pending:
            return draft
        raise V2WorkflowError("Sheets delivery is already in progress")
    settled = workflow.settle_remote_effect_claim(
        draft.id,
        "sheets_delivery",
        detail,
        "ambiguous",
        detail=_v2_sheets_receipt_detail(detail, "expired_pending"),
    )
    if not settled:
        raise V2WorkflowError("Sheets delivery receipt changed during recovery")
    return cast(
        V2Draft,
        workflow.mark_manual_review(draft.id, "sheets_delivery expired while possibly sent"),
    )


def _settle_v2_sheets_ambiguous(
    workflow: V2Workflow,
    draft: V2Draft,
    expected_detail: str,
    reason: str,
) -> V2Draft:
    settled = workflow.settle_remote_effect_claim(
        draft.id,
        "sheets_delivery",
        expected_detail,
        "ambiguous",
        detail=_v2_sheets_receipt_detail(expected_detail, "ambiguous", reason=reason),
    )
    if not settled:
        raise V2WorkflowError("Sheets delivery claim changed during ambiguous settlement")
    return cast(V2Draft, workflow.mark_manual_review(draft.id, reason))


def _settle_v2_sheets_pre_dispatch_failure(
    workflow: V2Workflow,
    draft: V2Draft,
    expected_detail: str,
    error: Exception,
    *,
    clear_network: bool = False,
) -> V2Draft:
    """Persist a proven pre-dispatch failure; only its first network failure may retry."""
    detail = _v2_sheets_receipt_detail(
        expected_detail,
        "pre_dispatch_failed",
        error=type(error).__name__,
        failure="clear_pre_dispatch_network" if clear_network else "terminal_pre_dispatch_failure",
    )
    if not workflow.settle_remote_effect_claim(
        draft.id,
        "sheets_delivery",
        expected_detail,
        "failed",
        detail=detail,
    ):
        return cast(
            V2Draft,
            workflow.mark_manual_review(draft.id, "sheets_delivery interrupted during pre-dispatch failure"),
        )
    receipt = workflow.remote_effect(draft.id, "sheets_delivery")
    attempts = None if receipt is None else receipt.get("attempts")
    if clear_network and isinstance(attempts, int) and not isinstance(attempts, bool) and attempts < 2:
        return workflow.get_draft(draft.id)
    return cast(
        V2Draft,
        workflow.mark_manual_review(draft.id, f"sheets_delivery pre-dispatch failed: {type(error).__name__}"),
    )


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
        return self._send(exact_review_text(draft.id, draft.content), token)

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

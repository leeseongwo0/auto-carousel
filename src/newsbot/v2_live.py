"""Production-facing ports for the isolated asynchronous Newsbot V2 flow.

This module deliberately does not import ``NewsPipeline``, ranking, or news policy.
It records a send receipt before any later approval callback can advance the state.
"""

from __future__ import annotations

import hashlib
import html
import json
import secrets
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from .ai.structured_copy import draft_from_mapping
from .approval.base import hash_callback_token
from .copywriting import validate_copy
from .sheets.base import DeliveryOutcome, MetadataState, SheetDelivery
from .sheets.schema import project_handoff
from .v2_codex import exact_review_text
from .v2_workflow import V2Candidate, V2Draft, V2State, V2Workflow, V2WorkflowError


class ClearNetworkFailure(RuntimeError):
    """A typed failure proven to have occurred before remote dispatch."""


class AmbiguousRemoteEffect(RuntimeError):
    """A remote outcome that must never be retried blindly."""


class SheetsClearPreDispatchNetworkError(RuntimeError):
    """Sheets transport proved it failed before any remote dispatch."""


class CandidateNotificationPort(Protocol):
    def __call__(self, candidate: V2Candidate, callback_token: str) -> str | bool: ...


class DraftNotificationPort(Protocol):
    def __call__(self, draft: V2Draft, callback_token: str) -> str | bool: ...


class V2LiveWorkflow:
    """Notification-only coordinator for the two human approval gates."""

    def __init__(
        self,
        workflow: V2Workflow,
        *,
        notify_candidate: CandidateNotificationPort,
        notify_draft: DraftNotificationPort,
        callback_ttl: timedelta = timedelta(days=1),
    ) -> None:
        if callback_ttl <= timedelta():
            raise ValueError("invalid live workflow limits")
        self.workflow = workflow
        self.notify_candidate = notify_candidate
        self.notify_draft = notify_draft
        self.callback_ttl = callback_ttl

    def run(self, candidate_id: str) -> V2Candidate | V2Draft:
        candidate = self.workflow.get_candidate(candidate_id)
        if candidate.state == V2State.PENDING_CANDIDATE:
            return self._notify_candidate(candidate)
        draft = self.workflow.get_draft_for_candidate(candidate_id)
        if (
            candidate.state == V2State.DRAFT_PENDING_APPROVAL
            and draft is not None
            and draft.state == V2State.DRAFT_PENDING_APPROVAL
        ):
            return self._notify_draft(draft)
        return draft or candidate

    def settle_callback(
        self,
        token: str,
        stage: str,
    ) -> V2Candidate | V2Draft | None:
        """Authenticate and atomically settle one approval capability."""
        if stage not in {"candidate", "draft"}:
            return None
        try:
            token_hash = hash_callback_token(token)
        except ValueError:
            return None
        settled = self.workflow.settle_callback_any(
            token_hash,
            self._now(),
            expected_stage=stage,
        )
        if settled is None:
            return None
        entity_id, settled_stage = settled
        if settled_stage == "candidate":
            return self.workflow.get_candidate(entity_id)
        return self.workflow.get_draft(entity_id)

    def _notify_candidate(self, candidate: V2Candidate) -> V2Candidate:
        if self._prior_remote(candidate.id, "candidate_notification"):
            return self.workflow.get_candidate(candidate.id)
        outcome = self._notify_with_capability(
            candidate.id,
            "candidate",
            "candidate_notification",
            lambda token: self.notify_candidate(candidate, token),
        )
        if outcome is None:
            return cast(
                V2Candidate,
                self.workflow.mark_manual_review(
                    candidate.id,
                    "candidate notification outcome ambiguous",
                ),
            )
        return self.workflow.get_candidate(candidate.id)

    def _notify_draft(self, draft: V2Draft) -> V2Draft:
        if self._prior_remote(draft.id, "draft_notification"):
            return self.workflow.get_draft(draft.id)
        outcome = self._notify_with_capability(
            draft.id,
            "draft",
            "draft_notification",
            lambda token: self.notify_draft(draft, token),
        )
        if outcome is None:
            return cast(
                V2Draft,
                self.workflow.mark_manual_review(
                    draft.id,
                    "draft notification outcome ambiguous",
                ),
            )
        return self.workflow.get_draft(draft.id)

    def _prior_remote(self, entity_id: str, stage: str) -> bool:
        receipt = self.workflow.remote_effect(entity_id, stage)
        if receipt is None:
            return False
        status = receipt["status"]
        if status == "ambiguous":
            self.workflow.mark_manual_review(entity_id, f"{stage} outcome ambiguous")
            return True
        if status == "pending":
            self.workflow.mark_manual_review(entity_id, f"{stage} interrupted with unknown outcome")
            return True
        if status == "failed":
            self.workflow.mark_manual_review(entity_id, f"{stage} failed before dispatch")
            return True
        return status == "confirmed"

    def _notify_with_capability(
        self,
        entity_id: str,
        callback_stage: str,
        remote_stage: str,
        call: Callable[[str], Any],
    ) -> Any | None:
        token = secrets.token_urlsafe(32)
        token_hash = hash_callback_token(token)
        claim_detail = json.dumps(
            {
                "operation": remote_stage,
                "owner": secrets.token_hex(16),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        claimed = self.workflow.claim_notification(
            entity_id=entity_id,
            callback_stage=callback_stage,
            token_hash=token_hash,
            expires_at=(datetime.now(UTC) + self.callback_ttl).isoformat(),
            claim_detail=claim_detail,
        )
        if not claimed:
            return False
        try:
            value = call(token)
            if value is False:
                self.workflow.settle_remote_effect_claim(
                    entity_id,
                    remote_stage,
                    claim_detail,
                    "ambiguous",
                    detail="remote_outcome_unconfirmed",
                )
                return None
            receipt_id = value if isinstance(value, str) else "confirmed"
            if not self.workflow.settle_remote_effect_claim(
                entity_id,
                remote_stage,
                claim_detail,
                "confirmed",
                detail="remote_outcome_confirmed",
                receipt_id=receipt_id,
            ):
                return None
            return value
        except ClearNetworkFailure:
            self.workflow.settle_remote_effect_claim(
                entity_id,
                remote_stage,
                claim_detail,
                "failed",
                detail="clear_pre_dispatch_network",
            )
            return None
        except Exception:
            self.workflow.settle_remote_effect_claim(
                entity_id,
                remote_stage,
                claim_detail,
                "ambiguous",
                detail="unexpected_remote_exception",
            )
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


def _safe_telegram_text(value: object) -> str:
    """Render untrusted evidence as inert plain text for Telegram."""
    text = str(value or "")
    text = "".join(char for char in text if char in "\n\t" or not unicodedata.category(char).startswith("C"))
    return html.escape(text, quote=False)


def _truncate_utf8(text: str, limit: int = 3_800) -> str:
    if limit < 0:
        raise ValueError("limit must be nonnegative")
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    if limit == 0:
        return ""
    suffix = "…"
    if limit < len(suffix.encode("utf-8")):
        return encoded[:limit].decode("utf-8", errors="ignore")
    budget = limit - len(suffix.encode("utf-8"))
    clipped = encoded[:budget].decode("utf-8", errors="ignore").rstrip()
    return f"{clipped}{suffix}"


def _bounded_metadata(value: object, limit: int) -> str:
    return _truncate_utf8(_safe_telegram_text(value), limit)


def render_v2_candidate_message(candidate: V2Candidate, evidence: Mapping[str, object]) -> str:
    """Format only the immutable revision/snapshot bound to a candidate."""
    revision = evidence.get("revision")
    snapshot = evidence.get("snapshot")
    if not isinstance(revision, Mapping) or not isinstance(snapshot, Mapping):
        raise V2WorkflowError("candidate notification requires bound revision and snapshot evidence")
    observation = revision.get("payload")
    if not isinstance(observation, Mapping):
        raise V2WorkflowError("candidate notification has invalid bound revision evidence")
    provenance = snapshot.get("provenance")
    source_date = snapshot.get("source_date")
    date_status = "conflict" if snapshot.get("source_date_conflict") else (source_date or "Telegram fallback")
    source = observation.get("channel_handle") or observation.get("channel_id") or candidate.channel_id
    title = snapshot.get("title") or observation.get("preview_title") or "No title"
    body = snapshot.get("body") or observation.get("preview_description") or observation.get("text") or "No body"
    link = (
        snapshot.get("canonical_url") or snapshot.get("final_url") or snapshot.get("requested_url") or "No public URL"
    )
    provenance_text = (
        json.dumps(provenance, ensure_ascii=False, sort_keys=True) if isinstance(provenance, Mapping) else "unavailable"
    )
    duplicate = evidence.get("duplicate") or snapshot.get("duplicate") or "none"
    metadata = (
        ("Candidate", candidate.id, 128),
        ("Outcome", candidate.policy_outcome, 128),
        ("Reason", candidate.policy_reason, 256),
        ("Duplicate", duplicate, 256),
        ("Source", source, 512),
        ("Link", link, 1_024),
        ("Telegram date", observation.get("published_at") or "unknown", 128),
        ("Source date", date_status, 128),
        ("Provenance", provenance_text, 768),
    )
    fixed = "\n".join(f"{label}: {_bounded_metadata(value, limit)}" for label, value, limit in metadata)
    remaining = 3_800 - len(fixed.encode("utf-8")) - len(b"\nTitle: \nBody: ")
    if remaining < 0:
        raise V2WorkflowError("candidate notification metadata exceeds Telegram limit")
    bounded_title = _truncate_utf8(_safe_telegram_text(title), min(768, remaining))
    remaining -= len(bounded_title.encode("utf-8"))
    bounded_body = _truncate_utf8(_safe_telegram_text(body), remaining)
    return f"{fixed}\nTitle: {bounded_title}\nBody: {bounded_body}"


class TelegramV2Notifier:
    """One-attempt V2 approval sender backed by the existing Telegram transport."""

    def __init__(
        self,
        adapter: Any,
        *,
        candidate_evidence: Callable[[str], Mapping[str, object]],
        deadline_seconds: float = 30,
    ) -> None:
        if deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")
        self.adapter = adapter
        self.candidate_evidence = candidate_evidence
        self.deadline_seconds = deadline_seconds

    def candidate(self, candidate: V2Candidate, token: str) -> str:
        return self._send(render_v2_candidate_message(candidate, self.candidate_evidence(candidate.id)), token)

    def draft(self, draft: V2Draft, token: str) -> str:
        return self._send(exact_review_text(draft.id, draft.content), token)

    def _send(self, text: str, token: str) -> str:
        from .approval.telegram import TelegramDeadline

        result = self.adapter.send_message_once(
            text,
            markup={"inline_keyboard": [[{"text": "Approve", "callback_data": token}]]},
            deadline=TelegramDeadline.after(self.deadline_seconds),
        )
        if not result.accepted or result.message_id is None:
            raise AmbiguousRemoteEffect(result.safe_code or "Telegram send not confirmed")
        return str(result.message_id)

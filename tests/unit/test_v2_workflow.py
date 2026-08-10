import json
from datetime import UTC, datetime, timedelta

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_codex import prepare_generation
from newsbot.v2_workflow import V2State, V2Workflow, V2WorkflowError


def observation(post_id="1", text=None):
    return SourceObservation(
        channel_id="channel",
        channel_handle="handle",
        external_post_id=post_id,
        published_at=datetime.now(UTC),
        text=text
        or "OpenAI announced a major integration with enterprise data infrastructure available to users. "
        "The deployment affects customers and includes important security details for the ecosystem.",
        urls=(UrlCandidate("https://example.test/story"),),
    )


def test_initialization_and_duplicate_observation(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        first = workflow.record_observation(observation())
        again = workflow.record_observation(observation())
        assert first.id == again.id
        assert first.state == V2State.PENDING_CANDIDATE
        assert len(workflow.list_candidates()) == 1


def test_both_approval_gates_and_sheet_delivery(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        with pytest.raises(V2WorkflowError):
            workflow.create_draft(candidate.id, "not yet")
        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, "card-news copy")
        with pytest.raises(V2WorkflowError):
            workflow.mark_sheet_delivered(draft.id)
        workflow.approve_draft(draft.id)
        delivered = workflow.mark_sheet_delivered(draft.id)
        assert delivered.state == V2State.SHEET_DELIVERED
        assert workflow.mark_sheet_delivered(draft.id).state == V2State.SHEET_DELIVERED


def test_invalid_transition_and_manual_review(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        with pytest.raises(V2WorkflowError):
            workflow.approve_draft("missing")
        workflow.mark_manual_review(candidate.id, "ambiguous remote effect")
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW
        with pytest.raises(V2WorkflowError):
            workflow.approve_candidate(candidate.id)


def test_non_news_does_not_create_candidate(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        assert workflow.record_observation(observation(text="bitcoin price chart buy sell")) is None
        assert workflow.list_candidates() == []


def _prepared(workflow, candidate):
    approved = workflow.approve_candidate(candidate.id)
    prepared = prepare_generation(approved)
    return prepared, workflow.prepare_codex_request(
        candidate.id,
        prepared.request_bytes,
        prepared.request_digest,
    )


def _valid_draft_output(fact) -> bytes:
    unit = {
        "text": "검증된 사실입니다.",
        "references": [{"claim_id": fact.id, "source_version_id": fact.source_version_id}],
    }
    return json.dumps(
        {
            "bodies": [],
            "caption": {
                "context": "맥락입니다.",
                "details": "세부 내용입니다.",
                "hashtags": ["#AI"],
                "hook": "핵심 소식입니다.",
                "implications": "영향이 있습니다.",
                "questions": "어떻게 달라질까요?",
            },
            "category": "AI",
            "cover": {
                "factual_units": [unit],
                "subtitle": "검증된 발표",
                "title": "새로운 발표",
            },
            "draft": True,
            "source_reported": True,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def test_codex_request_state_identity_attempt_cap_and_pending_restart(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        payload = b"request"
        digest = __import__("hashlib").sha256(payload).hexdigest()
        with pytest.raises(V2WorkflowError):
            workflow.prepare_codex_request(candidate.id, payload, digest)
        workflow.approve_candidate(candidate.id)
        request = workflow.prepare_codex_request(candidate.id, payload, digest)
        assert request.request_bytes == payload
        with pytest.raises(V2WorkflowError):
            workflow.prepare_codex_request(candidate.id, b"other", digest)
        attempt = workflow.begin_codex_attempt(candidate.id, digest)
        with pytest.raises(V2WorkflowError, match="interrupted pending"):
            workflow.begin_codex_attempt(candidate.id, digest)
        workflow.settle_codex_attempt_failure(attempt.id, "clear_pre_dispatch_network", retryable=True)
        second = workflow.begin_codex_attempt(candidate.id, digest)
        assert (
            workflow.settle_codex_attempt_failure(second.id, "clear_pre_dispatch_network", retryable=True)
            == "terminal_failed"
        )
        assert len(workflow.list_codex_attempts(candidate.id)) == 2
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW
        with pytest.raises(V2WorkflowError, match="cannot launch"):
            workflow.begin_codex_attempt(candidate.id, digest)


def test_interrupted_codex_attempt_reconciles_to_manual_review_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        _prepared_generation, request = _prepared(workflow, candidate)
        workflow.begin_codex_attempt(candidate.id, request.digest)

    with V2Workflow(db) as reopened:
        assert reopened.reconcile_interrupted_codex_requests() == 1
        assert reopened.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW
        assert reopened.get_codex_request(candidate.id).status == "terminal_failed"
        assert reopened.list_codex_attempts(candidate.id)[0].status == "terminal_failed"
        assert reopened.next_codex_candidate() is None


def test_prepared_codex_request_is_safely_resumable_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        prepared, request = _prepared(workflow, candidate)

    with V2Workflow(db) as reopened:
        selected = reopened.next_codex_candidate()
        assert selected is not None
        assert selected.id == candidate.id
        same = reopened.prepare_codex_request(
            candidate.id,
            prepared.request_bytes,
            prepared.request_digest,
        )
        assert same.digest == request.digest
        assert reopened.begin_codex_attempt(candidate.id, same.digest).number == 1


def test_codex_success_is_atomic_with_draft_and_receipts(tmp_path):
    import hashlib

    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        prepared_generation, request = _prepared(workflow, candidate)
        attempt = workflow.begin_codex_attempt(candidate.id, request.digest)
        output = _valid_draft_output(prepared_generation.request.facts[0])
        draft = workflow.commit_codex_success(attempt.id, output, hashlib.sha256(output).hexdigest())
        assert draft.content == output.decode()
        assert workflow.get_candidate(candidate.id).state == V2State.DRAFT_PENDING_APPROVAL
        assert workflow.get_codex_request(candidate.id).status == "succeeded"
        assert workflow.list_codex_attempts(candidate.id)[0].status == "succeeded"
        with pytest.raises(V2WorkflowError):
            workflow.commit_codex_success(attempt.id, output, hashlib.sha256(output).hexdigest())


def test_telegram_cursor_handoff_merges_and_runtime_advances_monotonically(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        assert workflow.telegram_next_offset() is None
        assert workflow.handoff_telegram_cursor(10) == 10
        assert workflow.handoff_telegram_cursor(12) == 12
        assert workflow.handoff_telegram_cursor(11) == 12
        assert workflow.advance_telegram_cursor(9) == 12
        assert workflow.advance_telegram_cursor(13) == 13
        with pytest.raises(ValueError):
            workflow.handoff_telegram_cursor(-1)

    with (
        V2Workflow(tmp_path / "unseeded.sqlite") as unseeded,
        pytest.raises(V2WorkflowError, match="handoff is required"),
    ):
        unseeded.advance_telegram_cursor(1)


def test_remote_effect_selectors_include_durable_recovery_receipts(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        assert workflow.next_candidate_pending_notification().id == candidate.id
        workflow.record_remote_attempt(candidate.id, "candidate_notification")
        assert workflow.next_candidate_pending_notification().id == candidate.id
        workflow.settle_remote_effect(candidate.id, "candidate_notification", "confirmed")
        assert workflow.next_candidate_pending_notification() is None

        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, "copy")
        assert workflow.next_draft_pending_notification().id == draft.id
        workflow.record_remote_attempt(draft.id, "draft_notification")
        assert workflow.next_draft_pending_notification().id == draft.id
        workflow.settle_remote_effect(draft.id, "draft_notification", "ambiguous")
        assert workflow.next_draft_pending_notification().id == draft.id
        workflow.approve_draft(draft.id)
        assert workflow.next_draft_approved_sheets_delivery().id == draft.id
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        assert workflow.next_draft_approved_sheets_delivery().id == draft.id
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed")
        assert workflow.next_draft_approved_sheets_delivery().id == draft.id


def test_expired_approval_capabilities_move_each_gate_to_manual_review(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation("candidate"))
        assert candidate is not None
        expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        workflow.issue_callback("a" * 64, candidate.id, "candidate", expired)
        assert workflow.reconcile_expired_approval_capabilities(datetime.now(UTC).isoformat()) == 1
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW

        approved = workflow.record_observation(observation("draft"))
        assert approved is not None
        workflow.approve_candidate(approved.id)
        draft = workflow.create_draft(approved.id, "copy")
        workflow.issue_callback("b" * 64, draft.id, "draft", expired)
        assert workflow.reconcile_expired_approval_capabilities(datetime.now(UTC).isoformat()) == 1
        assert workflow.get_candidate(approved.id).state == V2State.MANUAL_REVIEW
        assert workflow.get_draft(draft.id).state == V2State.MANUAL_REVIEW

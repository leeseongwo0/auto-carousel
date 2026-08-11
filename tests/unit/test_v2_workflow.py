import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_article import (
    body_identity,
    material_character_count,
)
from newsbot.v2_codex import prepare_generation
from newsbot.v2_workflow import V2State, V2Workflow, V2WorkflowError
from tests.v2_support import create_candidate


def observation(post_id="1", text=None):
    return SourceObservation(
        channel_id="channel",
        channel_handle="handle",
        external_post_id=post_id,
        published_at=datetime.now(UTC),
        text=text
        or "OpenAI announced a major integration with enterprise data infrastructure available to users. "
        "The deployment affects customers and includes important security details for the ecosystem.",
        urls=(UrlCandidate(f"https://example.test/story/{post_id}"),),
    )


def test_initialization_and_duplicate_observation(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        first = create_candidate(workflow, observation())
        again = create_candidate(workflow, observation())
        assert first.id == again.id
        assert first.state == V2State.PENDING_CANDIDATE
        assert len(workflow.list_candidates()) == 1


def test_post_claim_edit_is_retained_without_reevaluation_or_resend(
    tmp_path,
) -> None:
    with V2Workflow(
        tmp_path / "v2.sqlite",
        mode="create",
    ) as workflow:
        candidate = create_candidate(
            workflow,
            observation(),
        )
        assert candidate is not None
        binding = workflow._db.execute(
            "SELECT revision_id FROM v2_candidate_bindings WHERE candidate_id=?",
            (candidate.id,),
        ).fetchone()
        original_revision_id = int(
            binding["revision_id"],
        )

        edited = observation(
            text=("OpenAI corrected material details in the enterprise integration announcement. ") * 5,
        )
        revised = workflow.record_revision(edited)

        assert revised.id != original_revision_id
        assert (
            workflow.claim_enrichment(
                "post-claim-edit",
                revised.id,
            )
            is None
        )
        assert [item.id for item in workflow.list_candidates()] == [candidate.id]
        current_binding = workflow._db.execute(
            "SELECT revision_id FROM v2_candidate_bindings WHERE candidate_id=?",
            (candidate.id,),
        ).fetchone()
        assert int(current_binding["revision_id"]) == original_revision_id
        assert (
            workflow._db.execute(
                "SELECT COUNT(*) FROM v2_callbacks",
            ).fetchone()[0]
            == 0
        )
        assert (
            workflow._db.execute(
                "SELECT COUNT(*) FROM v2_remote_effects",
            ).fetchone()[0]
            == 0
        )


def test_schema_keeps_only_current_policy_outcome_and_reason(
    tmp_path,
) -> None:
    with V2Workflow(
        tmp_path / "v2.sqlite",
        mode="create",
    ) as workflow:
        tables = {
            str(row["name"])
            for row in workflow._db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            )
        }
        assert "v2_policy_versions" not in tables
        assert "v2_policy_history" not in tables
        columns = {
            str(row["name"])
            for row in workflow._db.execute(
                "PRAGMA table_info(v2_candidates)",
            )
        }
        assert {
            "policy_outcome",
            "policy_reason",
        } <= columns
        assert (
            not {
                "policy_version",
                "policy_history",
            }
            & columns
        )


def test_snapshot_identity_accepts_only_current_body_contract() -> None:
    body = "A completed factual article body with material evidence. " * 8
    material_count = material_character_count(body)
    digest = body_identity(body)
    assert digest is not None
    assert V2Workflow._snapshot_keys(
        {
            "body": body,
            "title": None,
            "body_hash": digest,
            "material_count": material_count,
        }
    ) == [("article_body_v1", digest)]
    with pytest.raises(
        V2WorkflowError,
        match="body identity mismatch",
    ):
        V2Workflow._snapshot_keys(
            {
                "body": body,
                "title": None,
                "body_hash": "0" * 64,
                "material_count": material_count,
            }
        )


def test_persisted_order_and_due_timestamps_are_fixed_utc(
    tmp_path,
) -> None:
    published = datetime.fromisoformat("2030-01-02T09:00:00+09:00")
    observed = datetime.fromisoformat("2030-01-01T20:00:01-04:00")
    source = SourceObservation(
        channel_id="channel",
        channel_handle="handle",
        external_post_id="offset",
        published_at=published,
        observed_at=observed,
        text="Completed factual news event. " * 10,
        urls=(UrlCandidate("https://example.test/story/offset"),),
    )
    with V2Workflow(
        tmp_path / "offset.sqlite",
        mode="create",
    ) as workflow:
        revision = workflow.record_revision(source)
        assert revision.ordered_at == "2030-01-02T00:00:00.000000+00:00"
        persisted = workflow._db.execute(
            "SELECT observed_at FROM v2_observation_revisions WHERE id=?",
            (revision.id,),
        ).fetchone()
        assert persisted["observed_at"] == "2030-01-02T00:00:01.000000+00:00"
        lease = workflow.claim_enrichment(
            "offset-worker",
            revision.id,
            now="2030-01-02T09:00:02+09:00",
        )
        assert lease is not None
        workflow.settle_enrichment(
            lease,
            {"result": "transient_failure"},
            transient=True,
            now="2030-01-01T19:00:03-05:00",
        )
        attempt = workflow._db.execute(
            "SELECT settled_at,next_retry_at FROM v2_enrichment_attempts WHERE id=?",
            (lease.id,),
        ).fetchone()
        assert attempt["settled_at"] == "2030-01-02T00:00:03.000000+00:00"
        assert attempt["next_retry_at"] == "2030-01-02T00:00:33.000000+00:00"
        assert (
            workflow.claim_enrichment(
                "too-early",
                revision.id,
                now="2030-01-02T09:00:32+09:00",
            )
            is None
        )
        assert (
            workflow.claim_enrichment(
                "due",
                revision.id,
                now="2030-01-01T19:00:33-05:00",
            )
            is not None
        )


def test_enrichment_leases_are_single_owner_and_never_dispatch_three_times(
    tmp_path,
) -> None:
    started = datetime.now(UTC)
    with V2Workflow(
        tmp_path / "v2.sqlite",
        mode="create",
    ) as workflow:
        revision = workflow.record_revision(
            observation(),
        )
        first = workflow.claim_enrichment(
            "worker-a",
            revision.id,
            now=started.isoformat(),
        )
        assert first is not None
        assert (
            workflow.claim_enrichment(
                "worker-b",
                revision.id,
                now=started.isoformat(),
            )
            is None
        )
        assert workflow.mark_enrichment_dispatched(
            first,
            now=started.isoformat(),
        )
        workflow.settle_enrichment(
            first,
            {"result": "transient_failure"},
            transient=True,
            now=started.isoformat(),
        )

        due = (started + timedelta(seconds=31)).isoformat()
        second = workflow.claim_enrichment(
            "worker-b",
            revision.id,
            now=due,
        )
        assert second is not None
        assert second.attempt_number == 2
        assert workflow.mark_enrichment_dispatched(
            second,
            now=due,
        )
        workflow.settle_enrichment(
            second,
            {"result": "transient_failure"},
            transient=True,
            now=due,
        )

        assert (
            workflow.claim_enrichment(
                "worker-c",
                revision.id,
                now=(started + timedelta(minutes=5)).isoformat(),
            )
            is None
        )
        rows = workflow._db.execute(
            "SELECT attempt_number,status FROM v2_enrichment_attempts ORDER BY attempt_number",
        ).fetchall()
        assert [tuple(row) for row in rows] == [
            (1, "retryable"),
            (2, "terminal"),
        ]


def test_both_approval_gates_and_sheet_delivery(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        with pytest.raises(V2WorkflowError):
            workflow.create_draft(candidate.id, "not yet")
        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, "card-news copy")
        with pytest.raises(V2WorkflowError):
            workflow.mark_sheet_delivered(draft.id)
        workflow.approve_draft(draft.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed", receipt_id="receipt")
        assert workflow.verify_invariants()["delivered_marker_mismatches"] == 1
        delivered = workflow.mark_sheet_delivered(draft.id)
        assert delivered.state == V2State.SHEET_DELIVERED
        assert workflow.verify_invariants()["delivered_marker_mismatches"] == 0
        workflow.mark_manual_review(
            draft.id,
            "late operator annotation",
        )
        assert workflow.get_candidate(candidate.id).state == V2State.SHEET_DELIVERED
        assert workflow.get_draft(draft.id).state == V2State.SHEET_DELIVERED
        assert workflow.mark_sheet_delivered(draft.id).state == V2State.SHEET_DELIVERED


def test_invalid_transition_and_manual_review(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        with pytest.raises(V2WorkflowError):
            workflow.approve_draft("missing")
        workflow.mark_manual_review(candidate.id, "ambiguous remote effect")
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW
        with pytest.raises(V2WorkflowError):
            workflow.approve_candidate(candidate.id)


def test_non_news_does_not_create_candidate(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        assert create_candidate(workflow, observation(text="bitcoin price chart buy sell")) is None
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
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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

    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        assert workflow.telegram_next_offset() is None
        assert workflow.handoff_telegram_cursor(10) == 10
        assert workflow.handoff_telegram_cursor(12) == 12
        assert workflow.handoff_telegram_cursor(11) == 12
        assert workflow.advance_telegram_cursor(9) == 12
        assert workflow.advance_telegram_cursor(13) == 13
        with pytest.raises(ValueError):
            workflow.handoff_telegram_cursor(-1)

    with (
        V2Workflow(tmp_path / "unseeded.sqlite", mode="create") as unseeded,
        pytest.raises(V2WorkflowError, match="handoff is required"),
    ):
        unseeded.advance_telegram_cursor(1)


def test_edit_scan_cursor_survives_reopen_and_promotes_only_when_complete(
    tmp_path,
):
    db = tmp_path / "edit-scan.sqlite"
    started = datetime.now(UTC)
    first_edit = SourceObservation(
        channel_id="channel",
        channel_handle="handle",
        external_post_id="5",
        published_at=started - timedelta(hours=1),
        edited_at=started + timedelta(minutes=1),
        text="first edited evidence",
    )
    second_edit = SourceObservation(
        channel_id="channel",
        channel_handle="handle",
        external_post_id="3",
        published_at=started - timedelta(hours=2),
        edited_at=started + timedelta(minutes=2),
        text="older page edited evidence",
    )

    with V2Workflow(db, mode="create") as workflow:
        workflow.record_edit_sweep_page(
            "channel",
            [first_edit],
            next_before_message_id=4,
            scan_started_at=started.isoformat(),
            complete=False,
        )
        assert workflow.edit_scan_state("channel") == (
            4,
            started.isoformat(),
        )
        assert workflow.channel_cursor("channel")[1] is None

    with V2Workflow(db, mode="runtime") as workflow:
        workflow.record_edit_sweep_page(
            "channel",
            [second_edit],
            next_before_message_id=None,
            scan_started_at=started.isoformat(),
            complete=True,
        )
        assert workflow.edit_scan_state("channel") == (None, None)
        watermark = workflow.channel_cursor("channel")[1]
        assert watermark == (
            second_edit.edited_at,
            int(second_edit.external_post_id),
        )


def test_remote_effect_selectors_include_durable_recovery_receipts(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed", receipt_id="receipt")
        assert workflow.next_draft_approved_sheets_delivery().id == draft.id


def test_expired_approval_capabilities_move_each_gate_to_manual_review(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation("candidate"))
        assert candidate is not None
        expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        assert workflow.claim_notification(
            entity_id=candidate.id,
            callback_stage="candidate",
            token_hash="a" * 64,
            expires_at=expired,
            claim_detail="test_expired_candidate",
        )
        assert workflow.reconcile_expired_approval_capabilities(datetime.now(UTC).isoformat()) == 1
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW

        approved = create_candidate(workflow, observation("draft"))
        assert approved is not None
        workflow.approve_candidate(approved.id)
        draft = workflow.create_draft(approved.id, "copy")
        assert workflow.claim_notification(
            entity_id=draft.id,
            callback_stage="draft",
            token_hash="b" * 64,
            expires_at=expired,
            claim_detail="test_expired_draft",
        )
        assert workflow.reconcile_expired_approval_capabilities(datetime.now(UTC).isoformat()) == 1
        assert workflow.get_candidate(approved.id).state == V2State.MANUAL_REVIEW
        assert workflow.get_draft(draft.id).state == V2State.MANUAL_REVIEW


@pytest.mark.parametrize(
    ("table", "condition"),
    [
        ("v2_drafts", "NEW.state='sheet_delivered'"),
        ("v2_candidates", "NEW.state='sheet_delivered'"),
        ("v2_stories", "NEW.delivered_at IS NOT NULL"),
        ("v2_story_claims", "NEW.delivered_at IS NOT NULL"),
    ],
)
def test_delivery_markers_rollback_together_on_each_failure(
    table,
    condition,
    tmp_path,
):
    with V2Workflow(tmp_path / f"{table}.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation("delivery"))
        assert candidate is not None
        workflow.approve_candidate(candidate.id)
        pending = workflow.create_draft(candidate.id, "copy")
        draft = workflow.approve_draft(pending.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(
            draft.id,
            "sheets_delivery",
            "confirmed",
            receipt_id="sheet-row",
        )
        workflow._db.execute(
            f"CREATE TEMP TRIGGER inject_delivery_failure "
            f"BEFORE UPDATE ON {table} "
            f"WHEN {condition} "
            "BEGIN SELECT RAISE(ABORT,'injected'); END"
        )
        with pytest.raises(sqlite3.IntegrityError, match="injected"):
            workflow.mark_sheet_delivered(draft.id)

        candidate_row = workflow._db.execute(
            "SELECT state FROM v2_candidates WHERE id=?",
            (candidate.id,),
        ).fetchone()
        draft_row = workflow._db.execute(
            "SELECT state FROM v2_drafts WHERE id=?",
            (draft.id,),
        ).fetchone()
        markers = workflow._db.execute(
            "SELECT s.delivered_at,claim.delivered_at "
            "FROM v2_candidate_bindings b "
            "JOIN v2_stories s ON s.id=b.story_id "
            "JOIN v2_story_claims claim "
            "ON claim.story_id=s.id AND claim.candidate_id=b.candidate_id "
            "WHERE b.candidate_id=?",
            (candidate.id,),
        ).fetchone()
        assert candidate_row["state"] == V2State.DRAFT_APPROVED
        assert draft_row["state"] == V2State.DRAFT_APPROVED
        assert tuple(markers) == (None, None)
        effect = workflow.remote_effect(draft.id, "sheets_delivery")
        assert effect is not None
        assert effect["status"] == "confirmed"
        assert effect["receipt_id"] == "sheet-row"


def test_story_eligibility_blocks_every_downstream_surface(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        pending = create_candidate(workflow, observation("held"))
        assert pending is not None
        workflow._db.execute(
            "UPDATE v2_candidate_bindings SET held=1 WHERE candidate_id=?",
            (pending.id,),
        )
        workflow._db.commit()
        with pytest.raises(V2WorkflowError):
            workflow.approve_candidate(pending.id)
        with pytest.raises(V2WorkflowError):
            workflow.claim_notification(
                entity_id=pending.id,
                callback_stage="candidate",
                token_hash="f" * 64,
                expires_at=datetime.now(UTC).isoformat(),
                claim_detail="blocked_test_claim",
            )
        with pytest.raises(V2WorkflowError):
            workflow.record_remote_attempt(
                pending.id,
                "candidate_notification",
            )
        assert workflow.next_candidate_pending_notification() is None

        codex = create_candidate(workflow, observation("codex"))
        assert codex is not None
        workflow.approve_candidate(codex.id)
        workflow._db.execute(
            "UPDATE v2_stories SET quarantined_at=? WHERE id=("
            "SELECT story_id FROM v2_candidate_bindings "
            "WHERE candidate_id=?)",
            (datetime.now(UTC).isoformat(), codex.id),
        )
        workflow._db.commit()
        payload = b"request"
        with pytest.raises(V2WorkflowError):
            workflow.prepare_codex_request(
                codex.id,
                payload,
                hashlib.sha256(payload).hexdigest(),
            )

        draft_candidate = create_candidate(
            workflow,
            observation("draft-gate"),
        )
        assert draft_candidate is not None
        workflow.approve_candidate(draft_candidate.id)
        pending_draft = workflow.create_draft(
            draft_candidate.id,
            "draft",
        )
        workflow._db.execute(
            "UPDATE v2_stories SET delivered_at=? WHERE id=("
            "SELECT story_id FROM v2_candidate_bindings "
            "WHERE candidate_id=?)",
            (
                datetime.now(UTC).isoformat(),
                draft_candidate.id,
            ),
        )
        workflow._db.commit()
        with pytest.raises(V2WorkflowError):
            workflow.approve_draft(pending_draft.id)
        assert workflow.next_draft_pending_notification() is None


def test_notification_claim_is_single_winner_across_workers(tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as first:
        candidate = create_candidate(
            first,
            observation("notify-race"),
        )
        assert candidate is not None
        with V2Workflow(db, mode="runtime") as second:
            expires = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
            assert first.claim_notification(
                entity_id=candidate.id,
                callback_stage="candidate",
                token_hash="1" * 64,
                expires_at=expires,
                claim_detail="owner:first",
            )
            assert not second.claim_notification(
                entity_id=candidate.id,
                callback_stage="candidate",
                token_hash="2" * 64,
                expires_at=expires,
                claim_detail="owner:second",
            )
            assert (
                first._db.execute(
                    "SELECT COUNT(*) FROM v2_remote_effects WHERE entity_id=? AND stage='candidate_notification'",
                    (candidate.id,),
                ).fetchone()[0]
                == 1
            )
            assert (
                first._db.execute(
                    "SELECT COUNT(*) FROM v2_callbacks WHERE entity_id=?",
                    (candidate.id,),
                ).fetchone()[0]
                == 1
            )


def test_manual_review_wins_before_every_outbound_claim(tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as first:
        candidate = create_candidate(first, observation("manual-candidate"))
        assert candidate is not None

        with V2Workflow(db, mode="runtime") as second:
            second.mark_manual_review(candidate.id, "operator hold")
            with pytest.raises(V2WorkflowError, match="state changed"):
                first.claim_notification(
                    entity_id=candidate.id,
                    callback_stage="candidate",
                    token_hash="3" * 64,
                    expires_at=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    claim_detail="owner:stale-candidate-worker",
                )

        draft_candidate = create_candidate(first, observation("manual-draft"))
        assert draft_candidate is not None
        first.approve_candidate(draft_candidate.id)
        draft = first.create_draft(draft_candidate.id, "exact draft")

        with V2Workflow(db, mode="runtime") as second:
            second.mark_manual_review(draft.id, "operator hold")
            with pytest.raises(V2WorkflowError, match="state changed"):
                first.record_remote_attempt(draft.id, "draft_notification")
        codex_candidate = create_candidate(first, observation("manual-codex"))
        assert codex_candidate is not None
        first.approve_candidate(codex_candidate.id)
        request_bytes = b'{"candidate":"manual-codex"}'
        request_digest = hashlib.sha256(request_bytes).hexdigest()
        first.prepare_codex_request(
            codex_candidate.id,
            request_bytes,
            request_digest,
        )

        with V2Workflow(db, mode="runtime") as second:
            second.mark_manual_review(codex_candidate.id, "operator hold")
            with pytest.raises(V2WorkflowError, match="state changed"):
                first.begin_codex_attempt(
                    codex_candidate.id,
                    request_digest,
                )

        sheets_candidate = create_candidate(first, observation("manual-sheets"))
        assert sheets_candidate is not None
        first.approve_candidate(sheets_candidate.id)
        sheets_draft = first.create_draft(sheets_candidate.id, "exact draft")
        first.approve_draft(sheets_draft.id)

        with V2Workflow(db, mode="runtime") as second:
            second.mark_manual_review(sheets_draft.id, "operator hold")
            with pytest.raises(V2WorkflowError, match="state changed"):
                first.claim_remote_effect(
                    sheets_draft.id,
                    "sheets_delivery",
                    '{"owner":"stale-sheets-worker"}',
                )

        assert first._db.execute("SELECT COUNT(*) FROM v2_callbacks").fetchone()[0] == 0
        assert first._db.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0] == 0
        assert first._db.execute("SELECT COUNT(*) FROM v2_codex_attempts").fetchone()[0] == 0


def test_migrate_mode_refuses_missing_database_without_creating_it(tmp_path):
    database = tmp_path / "missing.sqlite"

    with pytest.raises(sqlite3.OperationalError):
        V2Workflow(database, mode="migrate")

    assert not database.exists()

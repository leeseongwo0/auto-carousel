from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from newsbot.collectors.base import SourceObservation
from newsbot.v2_article import (
    body_identity,
    material_character_count,
)
from newsbot.v2_codex import prepare_generation
from newsbot.v2_policy import V2Outcome, V2PolicyResult
from newsbot.v2_workflow import V2Workflow, V2WorkflowError

OBSERVATION_TIME = datetime.now(UTC)


def _make_candidate(
    workflow: V2Workflow,
    post_id: str,
    url: str,
):
    revision = workflow.record_revision(observation(post_id))
    lease = workflow.claim_enrichment(post_id, revision.id)
    assert lease is not None
    candidate = workflow.finalize_enrichment(
        lease,
        snapshot(url),
        V2PolicyResult(V2Outcome.CANDIDATE, "news"),
    )
    assert candidate is not None
    return revision, candidate


def _mark_delivered(
    workflow: V2Workflow,
    candidate_id: str,
    delivered_at: str,
):
    workflow.approve_candidate(candidate_id)
    pending = workflow.create_draft(candidate_id, "retention draft")
    draft = workflow.approve_draft(pending.id)
    workflow.record_remote_attempt(draft.id, "sheets_delivery")
    workflow.settle_remote_effect(
        draft.id,
        "sheets_delivery",
        "confirmed",
        receipt_id=f"sheet:{draft.id}",
    )
    delivered = workflow.mark_sheet_delivered(draft.id)
    workflow._db.execute(
        "UPDATE v2_candidates SET created_at=?,updated_at=? WHERE id=?",
        (delivered_at, delivered_at, candidate_id),
    )
    workflow._db.execute(
        "UPDATE v2_drafts SET created_at=?,updated_at=? WHERE id=?",
        (delivered_at, delivered_at, draft.id),
    )
    workflow._db.execute(
        "UPDATE v2_stories SET delivered_at=? WHERE id=("
        "SELECT story_id FROM v2_candidate_bindings "
        "WHERE candidate_id=?)",
        (delivered_at, candidate_id),
    )
    workflow._db.execute(
        "UPDATE v2_story_claims SET delivered_at=? WHERE candidate_id=?",
        (delivered_at, candidate_id),
    )
    workflow._db.execute(
        "UPDATE v2_remote_effects SET updated_at=? WHERE entity_id=? AND stage='sheets_delivery'",
        (delivered_at, draft.id),
    )
    workflow._db.commit()
    return delivered


def _claim_candidate_callback(
    workflow: V2Workflow,
    candidate_id: str,
    token_hash: str,
    expires_at: str,
    *,
    confirmed: bool,
) -> None:
    assert workflow.claim_notification(
        entity_id=candidate_id,
        callback_stage="candidate",
        token_hash=token_hash,
        expires_at=expires_at,
        claim_detail=f"test_claim:{token_hash[:8]}",
    )
    if confirmed:
        workflow.settle_remote_effect(
            candidate_id,
            "candidate_notification",
            "confirmed",
            "remote_outcome_confirmed",
            receipt_id=f"message:{token_hash[:8]}",
        )


def observation(post_id: str, text: str = "material news") -> SourceObservation:
    return SourceObservation("channel", "handle", post_id, OBSERVATION_TIME, text=text)


def snapshot(url: str) -> dict[str, object]:
    body = url + "a" * 200
    return {
        "result": "success",
        "requested_url": url,
        "final_url": url,
        "canonical_url": url,
        "title": None,
        "body": body,
        "body_hash": body_identity(body),
        "material_count": material_character_count(
            body,
        ),
    }


SCALE_OBSERVATIONS = 2_420


def _selection_plan(
    workflow: V2Workflow,
    selector,
    statement_marker: str,
):
    statements: list[str] = []
    workflow._db.set_trace_callback(statements.append)
    try:
        selected = selector()
    finally:
        workflow._db.set_trace_callback(None)
    statement = next(statement for statement in statements if statement_marker in statement)
    plan = tuple(str(row["detail"]) for row in workflow._db.execute("EXPLAIN QUERY PLAN " + statement))
    return selected, statement, plan


def _searches_index(
    plan: tuple[str, ...],
    table: str,
    index: str,
) -> bool:
    return any(detail.startswith(f"SEARCH {table} ") and index in detail for detail in plan)


def _uses_index(plan: tuple[str, ...], index: str) -> bool:
    return any(index in detail for detail in plan)


def _insert_selection_scale(workflow: V2Workflow, created: datetime) -> None:
    observations = []
    candidates = []
    for index in range(SCALE_OBSERVATIONS):
        identity = f"selection:{index:04d}"
        timestamp = (created + timedelta(seconds=index)).isoformat()
        observations.append((identity, "selection", str(index), "{}", timestamp))
        candidates.append(
            (
                f"selection-candidate-{index:04d}",
                identity,
                "pending_candidate" if index % 2 == 0 else "manual_review",
                "candidate",
                "news",
                timestamp,
                timestamp,
            )
        )
    workflow._db.executemany(
        "INSERT INTO v2_observations VALUES(?,?,?,?,?)",
        observations,
    )
    workflow._db.executemany(
        "INSERT INTO v2_candidates VALUES(?,?,?,?,?,?,?)",
        candidates,
    )
    workflow._db.commit()


def test_finalization_claim_is_bound_and_duplicate_is_suppressed(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        first = workflow.record_revision(observation("1"))
        lease = workflow.claim_enrichment("one")
        assert lease is not None and lease.revision_id == first.id
        candidate = workflow.finalize_enrichment(
            lease, snapshot("https://example.test/a"), V2PolicyResult(V2Outcome.CANDIDATE, "news")
        )
        assert candidate is not None
        assert workflow.candidate_evidence(candidate.id)["snapshot"]["canonical_url"] == "https://example.test/a"
        workflow.record_revision(observation("2"))
        second = workflow.claim_enrichment("two")
        assert second is not None
        assert (
            workflow.finalize_enrichment(
                second, snapshot("https://example.test/a"), V2PolicyResult(V2Outcome.CANDIDATE, "news")
            )
            is None
        )


def test_unchanged_revision_fast_path_and_keyset_page(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        one = workflow.record_revision(observation("1"))
        assert workflow.record_revision(observation("1")).id == one.id
        initial_lease = workflow.claim_enrichment("1")
        assert initial_lease is not None and initial_lease.revision_id == one.id
        workflow.finalize_enrichment(
            initial_lease,
            snapshot("https://example.test/1"),
            V2PolicyResult(V2Outcome.CANDIDATE, "news"),
        )
        for post_id in ("2", "3"):
            revision = workflow.record_revision(observation(post_id))
            lease = workflow.claim_enrichment(post_id)
            assert lease is not None and lease.revision_id == revision.id
            workflow.finalize_enrichment(
                lease, snapshot(f"https://example.test/{post_id}"), V2PolicyResult(V2Outcome.CANDIDATE, "news")
            )
        items, cursor = workflow.status_page(limit=1)
        assert len(items) == 1 and cursor is not None
        later, _ = workflow.status_page(limit=200, cursor=cursor)
        assert all(item.id != items[0].id for item in later)
        assert "after_created_at" not in cursor
        with pytest.raises(ValueError, match="invalid status cursor"):
            workflow.status_page(limit=1, cursor=cursor, state="manual_review")
        with pytest.raises(ValueError, match="invalid status cursor"):
            workflow.status_page(limit=1, cursor="{not-opaque}")
        with pytest.raises(ValueError, match="invalid candidate state"):
            workflow.status_page(state="not-a-state")


def test_status_keyset_is_bounded_indexed_and_duplicate_free_at_ten_x(tmp_path) -> None:
    created = datetime(2027, 1, 1, tzinfo=UTC)
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        _insert_selection_scale(workflow, created)

        (first, cursor), statement, default_plan = _selection_plan(
            workflow,
            lambda: workflow.status_page(limit=50),
            "FROM v2_candidates c",
        )
        assert len(first) == 50
        assert cursor is not None
        assert "LIMIT 51" in statement
        assert _uses_index(default_plan, "v2_candidates_created")
        assert not any("TEMP B-TREE" in detail for detail in default_plan)

        (state_first, state_cursor), statement, state_plan = _selection_plan(
            workflow,
            lambda: workflow.status_page(
                limit=50,
                state="pending_candidate",
            ),
            "FROM v2_candidates c",
        )
        assert len(state_first) == 50
        assert state_cursor is not None
        assert "LIMIT 51" in statement
        assert _searches_index(
            state_plan,
            "c",
            "v2_candidates_state_created",
        )
        assert not any("TEMP B-TREE" in detail or "SCAN c" in detail for detail in state_plan)

        ids = [item.id for item in state_first]
        while state_cursor is not None:
            page, state_cursor = workflow.status_page(
                limit=50,
                cursor=state_cursor,
                state="pending_candidate",
            )
            assert len(page) <= 50
            ids.extend(item.id for item in page)
        assert len(ids) == SCALE_OBSERVATIONS // 2
        assert len(set(ids)) == len(ids)


def test_status_keyset_is_honest_under_concurrent_insertions(
    tmp_path,
) -> None:
    with V2Workflow(
        tmp_path / "dynamic-keyset.sqlite",
        mode="create",
    ) as workflow:
        existing: list[str] = []
        for label, created_at in (
            ("first", "2027-01-01T00:00:01+00:00"),
            ("middle", "2027-01-01T00:00:02+00:00"),
            ("last", "2027-01-01T00:00:03+00:00"),
        ):
            _, candidate = _make_candidate(
                workflow,
                label,
                f"https://example.test/{label}",
            )
            existing.append(candidate.id)
            workflow._db.execute(
                "UPDATE v2_candidates SET created_at=?,updated_at=? WHERE id=?",
                (created_at, created_at, candidate.id),
            )
        workflow._db.commit()

        first_page, cursor = workflow.status_page(
            limit=1,
        )
        assert cursor is not None
        assert [item.id for item in first_page] == [existing[0]]

        inserted: dict[str, str] = {}
        for label, created_at in (
            ("below", "2027-01-01T00:00:00+00:00"),
            ("above", "2027-01-01T00:00:04+00:00"),
        ):
            _, candidate = _make_candidate(
                workflow,
                label,
                f"https://example.test/{label}",
            )
            inserted[label] = candidate.id
            workflow._db.execute(
                "UPDATE v2_candidates SET created_at=?,updated_at=? WHERE id=?",
                (created_at, created_at, candidate.id),
            )
        workflow._db.commit()

        continued, _ = workflow.status_page(
            limit=200,
            cursor=cursor,
        )
        continued_ids = {item.id for item in continued}
        assert inserted["below"] not in continued_ids
        assert inserted["above"] in continued_ids
        restarted, _ = workflow.status_page(limit=200)
        assert inserted["below"] in {item.id for item in restarted}
        with pytest.raises(
            ValueError,
            match="between 1 and 200",
        ):
            workflow.status_page(limit=201)


def test_enrichment_and_outbound_selectors_stay_bounded_at_scale(
    tmp_path,
) -> None:
    created = datetime(2027, 1, 1, tzinfo=UTC)
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        _insert_selection_scale(workflow, created)
        _, pending_candidate = _make_candidate(
            workflow,
            "outbound-candidate",
            "https://example.test/outbound-candidate",
        )
        _, draft_candidate = _make_candidate(
            workflow,
            "outbound-draft",
            "https://example.test/outbound-draft",
        )
        workflow.approve_candidate(draft_candidate.id)
        draft = workflow.create_draft(draft_candidate.id, "selection draft")
        workflow.approve_draft(draft.id)
        _, draft_notification_candidate = _make_candidate(
            workflow,
            "outbound-draft-notification",
            "https://example.test/outbound-draft-notification",
        )
        workflow.approve_candidate(
            draft_notification_candidate.id,
        )
        draft_notification = workflow.create_draft(
            draft_notification_candidate.id,
            "notification draft",
        )
        _, codex_candidate = _make_candidate(
            workflow,
            "outbound-codex",
            "https://example.test/outbound-codex",
        )
        workflow.approve_candidate(codex_candidate.id)
        unclaimed = workflow.record_revision(observation("enrichment-selector"))

        lease, statement, lease_plan = _selection_plan(
            workflow,
            lambda: workflow.claim_enrichment("selection-owner"),
            "FROM v2_observation_revisions r INDEXED BY v2_revisions_due_order",
        )
        assert lease is not None
        assert lease.revision_id == unclaimed.id
        assert "LIMIT 1" in statement
        assert _uses_index(
            lease_plan,
            "v2_revisions_due_order",
        )
        assert not any("TEMP B-TREE" in detail for detail in lease_plan)

        candidate, statement, candidate_plan = _selection_plan(
            workflow,
            workflow.next_candidate_pending_notification,
            "SELECT candidate.id FROM v2_candidates candidate",
        )
        assert candidate is not None
        assert candidate.id == pending_candidate.id
        assert "LIMIT 1" in statement
        assert _searches_index(
            candidate_plan,
            "candidate",
            "v2_candidates_state_created",
        )
        assert not any("TEMP B-TREE" in detail or "SCAN candidate" in detail for detail in candidate_plan)

        pending_draft, statement, pending_draft_plan = _selection_plan(
            workflow,
            workflow.next_draft_pending_notification,
            "SELECT draft.id FROM v2_drafts draft",
        )
        assert pending_draft is not None
        assert pending_draft.id == draft_notification.id
        assert "LIMIT 1" in statement
        assert _searches_index(
            pending_draft_plan,
            "draft",
            "v2_drafts_state_created",
        )
        assert not any("TEMP B-TREE" in detail or "SCAN draft" in detail for detail in pending_draft_plan)

        codex, statement, codex_plan = _selection_plan(
            workflow,
            workflow.next_codex_candidate,
            "SELECT candidate.id FROM v2_candidates candidate",
        )
        assert codex is not None
        assert codex.id == codex_candidate.id
        assert "LIMIT 1" in statement
        assert _searches_index(
            codex_plan,
            "candidate",
            "v2_candidates_state_created",
        )
        assert not any("TEMP B-TREE" in detail or "SCAN candidate" in detail for detail in codex_plan)

        selected_draft, statement, draft_plan = _selection_plan(
            workflow,
            workflow.next_draft_approved_sheets_delivery,
            "SELECT draft.id FROM v2_drafts draft",
        )
        assert selected_draft is not None
        assert selected_draft.id == draft.id
        assert "LIMIT 1" in statement
        assert _searches_index(
            draft_plan,
            "draft",
            "v2_drafts_state_created",
        )
        assert not any("TEMP B-TREE" in detail or "SCAN draft" in detail for detail in draft_plan)


def test_compaction_effect_selector_stays_bounded_and_indexed(tmp_path) -> None:
    current = datetime(2030, 1, 1, tzinfo=UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "compaction.sqlite", mode="create") as workflow:
        revision, candidate = _make_candidate(
            workflow,
            "compaction-selector",
            "https://example.test/compaction-selector",
        )
        _mark_delivered(workflow, candidate.id, old)
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, revision.id),
        )
        workflow._db.commit()

        plan, statement, effects_plan = _selection_plan(
            workflow,
            lambda: workflow.compact(
                batch_size=7,
                dry_run=True,
                now=current.isoformat(),
            ),
            "WHERE status='confirmed' AND updated_at",
        )
        assert plan["eligible"] <= 7
        assert "LIMIT 6" in statement
        assert _searches_index(
            effects_plan,
            "v2_remote_effects",
            "v2_effects_retention",
        )
        assert not any("TEMP B-TREE" in detail or "SCAN v2_remote_effects" in detail for detail in effects_plan)


def test_compaction_is_bounded_idempotent_and_preserves_protected_evidence(tmp_path) -> None:
    current = datetime.now(UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        eligible_revision, eligible = _make_candidate(
            workflow,
            "delivered",
            "https://example.test/delivered",
        )
        _mark_delivered(workflow, eligible.id, old)
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, eligible_revision.id),
        )

        protected_revision, protected = _make_candidate(
            workflow,
            "protected",
            "https://example.test/protected",
        )
        workflow.record_remote_attempt(
            protected.id,
            "candidate_notification",
        )
        _mark_delivered(workflow, protected.id, old)
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, protected_revision.id),
        )

        manual_revision, manual = _make_candidate(
            workflow,
            "manual",
            "https://example.test/manual",
        )
        workflow.mark_manual_review(manual.id, "operator")
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, manual_revision.id),
        )
        workflow._db.commit()

        dry_run = workflow.compact(dry_run=True, now=current.isoformat())
        assert dry_run["hot_cold"] == [eligible_revision.id]
        assert protected_revision.id not in dry_run["hot_cold"]
        assert manual_revision.id not in dry_run["hot_cold"]

        applied = workflow.compact(now=current.isoformat())
        assert applied["hot_cold"] == dry_run["hot_cold"]
        assert workflow.compact(now=current.isoformat())["eligible"] == 0
        assert workflow.candidate_evidence(eligible.id)["revision"]["payload"] == {}
        assert workflow.candidate_evidence(protected.id)["revision"]["payload"] != {}
        assert workflow.candidate_evidence(manual.id)["revision"]["payload"] != {}
        assert workflow.verify_invariants() == {
            "candidate_binding_mismatches": 0,
            "delivered_marker_mismatches": 0,
            "tombstone_digest_mismatches": 0,
        }


def test_compaction_covers_terminal_non_candidate_and_seven_day_superseded_payload(tmp_path) -> None:
    current = datetime.now(UTC)
    old_31 = (current - timedelta(days=31)).isoformat()
    old_8 = (current - timedelta(days=8)).isoformat()
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        current_revision = workflow.record_revision(observation("noncandidate", text="unchanged non candidate"))
        noncandidate_lease = workflow.claim_enrichment(
            "noncandidate",
            current_revision.id,
        )
        assert noncandidate_lease is not None
        workflow.finalize_enrichment(
            noncandidate_lease,
            {"result": "permanent_failure"},
            V2PolicyResult(V2Outcome.NON_NEWS, "body_gate"),
        )
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old_31, current_revision.id),
        )

        first = workflow.record_revision(observation("edited", text="old revision"))
        edited = SourceObservation(
            "channel",
            "handle",
            "edited",
            datetime.now(UTC),
            text="new revision",
            edited_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        second = workflow.record_revision(edited)
        assert second.id != first.id
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old_8, first.id),
        )
        workflow._db.commit()

        dry_run = workflow.compact(dry_run=True, now=current.isoformat())
        assert current_revision.id in dry_run["hot_cold"]
        assert first.id in dry_run["superseded"]
        workflow.compact(now=current.isoformat())
        assert (
            workflow._db.execute(
                "SELECT payload FROM v2_observation_revisions WHERE id=?",
                (current_revision.id,),
            ).fetchone()[0]
            == "{}"
        )
        assert (
            workflow._db.execute(
                "SELECT payload FROM v2_observation_revisions WHERE id=?",
                (first.id,),
            ).fetchone()[0]
            == "{}"
        )


def test_callback_compaction_requires_terminal_workflow_without_unknown_effect(tmp_path) -> None:
    current = datetime.now(UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        _revision, delivered = _make_candidate(
            workflow,
            "callback-delivered",
            "https://example.test/callback-delivered",
        )
        _claim_candidate_callback(
            workflow,
            delivered.id,
            "d" * 64,
            current.isoformat(),
            confirmed=True,
        )
        _mark_delivered(workflow, delivered.id, old)
        workflow._db.execute(
            "UPDATE v2_callbacks SET expires_at=?,consumed_at=? WHERE token_hash=?",
            (old, old, "d" * 64),
        )

        _revision, protected = _make_candidate(
            workflow,
            "callback-protected",
            "https://example.test/callback-protected",
        )
        _claim_candidate_callback(
            workflow,
            protected.id,
            "p" * 64,
            current.isoformat(),
            confirmed=False,
        )
        workflow.mark_manual_review(protected.id, "operator")
        workflow._db.execute(
            "UPDATE v2_callbacks SET expires_at=?,consumed_at=? WHERE token_hash=?",
            (old, old, "p" * 64),
        )
        workflow._db.commit()

        _revision, related = _make_candidate(
            workflow,
            "callback-related-effect",
            "https://example.test/callback-related-effect",
        )
        _claim_candidate_callback(
            workflow,
            related.id,
            "r" * 64,
            current.isoformat(),
            confirmed=True,
        )
        workflow.approve_candidate(related.id)
        related_draft = workflow.create_draft(
            related.id,
            "draft",
        )
        workflow.approve_draft(related_draft.id)
        workflow.record_remote_attempt(
            related_draft.id,
            "sheets_delivery",
        )
        workflow.mark_manual_review(
            related_draft.id,
            "operator",
        )
        workflow._db.execute(
            "UPDATE v2_callbacks SET expires_at=?,consumed_at=? WHERE token_hash=?",
            (old, old, "r" * 64),
        )
        workflow._db.commit()

        assert workflow.compact(dry_run=True, now=current.isoformat())["callbacks"] == 1
        workflow.compact(now=current.isoformat())
        remaining = {row[0] for row in workflow._db.execute("SELECT token_hash FROM v2_callbacks ORDER BY token_hash")}
        assert remaining == {"p" * 64, "r" * 64}


def test_compaction_shares_batch_and_cold_projects_raw_article_data(
    tmp_path,
) -> None:
    current = datetime.now(UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        revision, candidate = _make_candidate(
            workflow,
            "bounded-cold",
            "https://example.test/secret?token=raw",
        )
        _claim_candidate_callback(
            workflow,
            candidate.id,
            "c" * 64,
            current.isoformat(),
            confirmed=True,
        )
        _mark_delivered(workflow, candidate.id, old)
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, revision.id),
        )
        workflow._db.execute(
            "UPDATE v2_callbacks SET expires_at=?,consumed_at=? WHERE token_hash=?",
            (old, old, "c" * 64),
        )
        binding = workflow._db.execute(
            "SELECT snapshot_id FROM v2_candidate_bindings WHERE candidate_id=?",
            (candidate.id,),
        ).fetchone()
        workflow._db.execute(
            "UPDATE v2_article_snapshots SET snapshot=? WHERE id=?",
            (
                json.dumps(
                    {
                        "result": "success",
                        "requested_url": ("https://example.test/secret?token=raw"),
                        "final_url": ("https://example.test/final?token=raw"),
                        "canonical_url": ("https://example.test/canonical?token=raw"),
                        "body": "sensitive body",
                        "body_hash": "b" * 64,
                        "material_count": 200,
                        "provenance": {
                            "requested_url_hash": "r" * 64,
                            "final_url_hash": "f" * 64,
                            "canonical_url_hash": "c" * 64,
                            "redirect_count": 1,
                            "result": "success",
                        },
                    },
                    sort_keys=True,
                ),
                binding["snapshot_id"],
            ),
        )
        workflow._db.commit()

        touched = []
        for _ in range(10):
            plan = workflow.compact(
                batch_size=1,
                now=current.isoformat(),
            )
            if plan["eligible"] == 0:
                break
            touched.append(plan["compacted"])
        assert touched and all(count == 1 for count in touched)
        assert (
            workflow.compact(
                batch_size=1,
                now=current.isoformat(),
            )["eligible"]
            == 0
        )

        cold_snapshot = json.loads(
            workflow._db.execute(
                "SELECT snapshot FROM v2_article_snapshots WHERE id=?",
                (binding["snapshot_id"],),
            ).fetchone()[0]
        )
        encoded = json.dumps(cold_snapshot, sort_keys=True)
        assert "https://" not in encoded
        assert "token=raw" not in encoded
        assert "sensitive body" not in encoded
        assert cold_snapshot["body_hash"] == "b" * 64
        effect = workflow._db.execute(
            "SELECT detail,receipt_id FROM v2_remote_effects WHERE stage='sheets_delivery'"
        ).fetchone()
        assert effect["detail"].startswith("compacted:")
        assert effect["receipt_id"]


def _codex_output(fact) -> bytes:
    factual_unit = {
        "text": "Verified fact.",
        "references": [
            {
                "claim_id": fact.id,
                "source_version_id": fact.source_version_id,
            }
        ],
    }
    return json.dumps(
        {
            "bodies": [],
            "caption": {
                "context": "Context.",
                "details": "Details.",
                "hashtags": ["#AI"],
                "hook": "Hook.",
                "implications": "Implications.",
                "questions": "Questions?",
            },
            "category": "AI",
            "cover": {
                "factual_units": [factual_unit],
                "subtitle": "Verified subtitle",
                "title": "Verified title",
            },
            "draft": True,
            "source_reported": True,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def test_old_delivered_binding_hold_does_not_block_compaction(tmp_path) -> None:
    current = datetime(2030, 1, 1, tzinfo=UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "held-delivered.sqlite", mode="create") as workflow:
        revision, candidate = _make_candidate(
            workflow,
            "held-delivered",
            "https://example.test/held-delivered",
        )
        _mark_delivered(workflow, candidate.id, old)

        # A stale cutover hold is injected to model an interrupted hold/release cycle.
        workflow._db.execute(
            "UPDATE v2_candidate_bindings SET held=1,hold_reason='cutover_hold' WHERE candidate_id=?",
            (candidate.id,),
        )
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, revision.id),
        )
        workflow._db.commit()

        assert workflow.compact(now=current.isoformat())["hot_cold"] == [revision.id]
        assert workflow.candidate_evidence(candidate.id)["revision"]["payload"] == {}


def test_expired_unconsumed_terminal_callback_compacts(tmp_path) -> None:
    current = datetime(2030, 1, 1, tzinfo=UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "expired-callback.sqlite", mode="create") as workflow:
        _revision, candidate = _make_candidate(
            workflow,
            "expired-callback",
            "https://example.test/expired-callback",
        )
        _claim_candidate_callback(
            workflow,
            candidate.id,
            "e" * 64,
            old,
            confirmed=True,
        )
        workflow.mark_manual_review(candidate.id, "terminal operator review")

        plan = workflow.compact(now=current.isoformat())

        assert plan["callbacks"] == 1
        assert (
            workflow._db.execute(
                "SELECT 1 FROM v2_callbacks WHERE token_hash=?",
                ("e" * 64,),
            ).fetchone()
            is None
        )


def test_delivered_codex_and_draft_compact_to_digests_without_resend(tmp_path) -> None:
    database = tmp_path / "delivered-codex.sqlite"
    current = datetime(2030, 1, 1, tzinfo=UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(database, mode="create") as workflow:
        revision, candidate = _make_candidate(
            workflow,
            "delivered-codex",
            "https://example.test/delivered-codex",
        )
        workflow.approve_candidate(candidate.id)
        prepared = prepare_generation(workflow.get_candidate(candidate.id))
        request = workflow.prepare_codex_request(
            candidate.id,
            prepared.request_bytes,
            prepared.request_digest,
        )
        attempt = workflow.begin_codex_attempt(candidate.id, request.digest)
        output = _codex_output(prepared.request.facts[0])
        draft = workflow.commit_codex_success(
            attempt.id,
            output,
            hashlib.sha256(output).hexdigest(),
        )
        workflow.approve_draft(draft.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(
            draft.id,
            "sheets_delivery",
            "confirmed",
            receipt_id=f"sheet:{draft.id}",
        )
        workflow.mark_sheet_delivered(draft.id)
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, revision.id),
        )
        workflow._db.execute(
            "UPDATE v2_candidates SET created_at=?,updated_at=? WHERE id=?",
            (old, old, candidate.id),
        )
        workflow._db.execute(
            "UPDATE v2_drafts SET created_at=?,updated_at=? WHERE id=?",
            (old, old, draft.id),
        )
        workflow._db.execute(
            "UPDATE v2_codex_requests SET created_at=?,updated_at=? WHERE digest=?",
            (old, old, request.digest),
        )
        workflow._db.execute(
            "UPDATE v2_remote_effects SET updated_at=? WHERE entity_id=? AND stage='sheets_delivery'",
            (old, draft.id),
        )
        workflow._db.commit()

        plan = workflow.compact(now=current.isoformat())
        assert plan["drafts"] == 1
        assert plan["codex_requests"] == 1
        row = workflow._db.execute(
            "SELECT content FROM v2_drafts WHERE id=?",
            (draft.id,),
        ).fetchone()
        assert row["content"] == "compacted:" + hashlib.sha256(output).hexdigest()
        compacted = workflow._db.execute(
            "SELECT request_bytes,output_bytes FROM v2_codex_requests WHERE digest=?",
            (request.digest,),
        ).fetchone()
        assert compacted["request_bytes"] == (
            b"compacted:" + hashlib.sha256(prepared.request_bytes).hexdigest().encode()
        )
        assert compacted["output_bytes"] == (b"compacted:" + hashlib.sha256(output).hexdigest().encode())

    with V2Workflow(database, mode="runtime") as reopened:
        assert reopened.next_draft_approved_sheets_delivery() is None
        assert reopened.mark_sheet_delivered(draft.id).id == draft.id
        assert reopened.get_candidate(candidate.id).state == "sheet_delivered"


def test_compaction_preserves_retry_and_manual_evidence(tmp_path) -> None:
    current = datetime(2030, 1, 1, tzinfo=UTC)
    old = (current - timedelta(days=31)).isoformat()
    with V2Workflow(tmp_path / "protected-evidence.sqlite", mode="create") as workflow:
        retry_revision, retry_candidate = _make_candidate(
            workflow,
            "retry-evidence",
            "https://example.test/retry-evidence",
        )
        workflow.record_remote_attempt(retry_candidate.id, "candidate_notification")
        workflow.settle_remote_effect(
            retry_candidate.id,
            "candidate_notification",
            "failed",
            "retryable evidence",
        )
        _mark_delivered(workflow, retry_candidate.id, old)
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, retry_revision.id),
        )
        workflow._db.execute(
            "UPDATE v2_remote_effects SET updated_at=? WHERE entity_id=? AND stage='candidate_notification'",
            (old, retry_candidate.id),
        )

        manual_revision, manual_candidate = _make_candidate(
            workflow,
            "manual-evidence",
            "https://example.test/manual-evidence",
        )
        manual_draft = _mark_delivered(workflow, manual_candidate.id, old)
        workflow.mark_manual_review(manual_draft.id, "operator evidence")
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=? WHERE id=?",
            (old, manual_revision.id),
        )
        workflow._db.commit()

        workflow.compact(now=current.isoformat())

        retry_effect = workflow._db.execute(
            "SELECT detail FROM v2_remote_effects WHERE entity_id=? AND stage='candidate_notification'",
            (retry_candidate.id,),
        ).fetchone()
        assert retry_effect["detail"] == "retryable evidence"
        assert workflow.get_draft(manual_draft.id).content == "retention draft"


def test_status_aggregate_requires_baseline_caps_and_uses_queue_indexes(tmp_path) -> None:
    current = datetime(2030, 1, 1, tzinfo=UTC)
    timestamp = current.isoformat()
    cap = V2Workflow.STATUS_AGGREGATE_CAP
    with V2Workflow(tmp_path / "aggregate.sqlite", mode="create") as workflow:
        revision = workflow.record_revision(observation("transient-attempt"))
        lease = workflow.claim_enrichment("transient-attempt", revision.id)
        assert lease is not None
        assert (
            workflow.settle_enrichment(
                lease,
                {"result": "transient_failure"},
                transient=True,
                now=timestamp,
            )
            is None
        )
        with pytest.raises(
            V2WorkflowError,
            match="seven-day storage baseline is required",
        ):
            workflow.status_aggregate(now=timestamp)

        aggregate_observations = []
        pending_candidates = []
        draft_candidates = []
        drafts = []
        for index in range(cap + 1):
            identity = f"aggregate-pending:{index:05d}"
            pending_candidates.append(
                (
                    f"aggregate-pending-candidate-{index:05d}",
                    identity,
                    "pending_candidate",
                    "candidate",
                    "news",
                    timestamp,
                    timestamp,
                )
            )
            aggregate_observations.append(
                (
                    identity,
                    "aggregate",
                    str(index),
                    "{}",
                    timestamp,
                )
            )
            draft_identity = f"aggregate-draft:{index:05d}"
            candidate_id = f"aggregate-draft-candidate-{index:05d}"
            draft_candidates.append(
                (
                    candidate_id,
                    draft_identity,
                    "draft_pending_approval",
                    "candidate",
                    "news",
                    timestamp,
                    timestamp,
                )
            )
            aggregate_observations.append(
                (
                    draft_identity,
                    "aggregate",
                    str(index),
                    "{}",
                    timestamp,
                )
            )
            drafts.append(
                (
                    f"aggregate-draft-{index:05d}",
                    candidate_id,
                    "pending review",
                    "draft_pending_approval",
                    timestamp,
                    timestamp,
                )
            )
        workflow._db.executemany(
            "INSERT INTO v2_observations VALUES(?,?,?,?,?)",
            aggregate_observations,
        )
        workflow._db.executemany(
            "INSERT INTO v2_candidates VALUES(?,?,?,?,?,?,?)",
            pending_candidates + draft_candidates,
        )
        workflow._db.executemany(
            "INSERT INTO v2_drafts VALUES(?,?,?,?,?,?)",
            drafts,
        )
        workflow._db.commit()

        statements: list[str] = []
        workflow._db.set_trace_callback(statements.append)
        try:
            aggregate = workflow.status_aggregate(
                seven_day_storage_baseline_bytes=1,
                now=(current + timedelta(seconds=1)).isoformat(),
            )
        finally:
            workflow._db.set_trace_callback(None)

        assert aggregate["states"]["pending_candidate"] == cap
        assert aggregate["queues"]["draft_review"] == cap
        assert aggregate["fetch_15m"]["transient_failure"] == 1
        assert set(aggregate["aggregate_truncated"]) >= {
            "state:pending_candidate",
            "queue:draft_review",
        }

        candidate_plans = [
            tuple(str(row["detail"]) for row in workflow._db.execute("EXPLAIN QUERY PLAN " + statement))
            for statement in statements
            if statement.startswith("SELECT COUNT(*) FROM (SELECT 1 FROM v2_candidates")
            or statement.startswith("SELECT created_at FROM v2_candidates")
        ]
        draft_plans = [
            tuple(str(row["detail"]) for row in workflow._db.execute("EXPLAIN QUERY PLAN " + statement))
            for statement in statements
            if statement.startswith("SELECT COUNT(*) FROM (SELECT 1 FROM v2_drafts")
        ]
        assert candidate_plans and draft_plans
        for plan in [*candidate_plans, *draft_plans]:
            assert not any("TEMP B-TREE" in detail for detail in plan)
        assert all(
            _uses_index(plan, "v2_candidates_state_created")
            and not any("SCAN v2_candidates" in detail for detail in plan)
            for plan in candidate_plans
        )
        assert all(
            _uses_index(plan, "v2_drafts_state_created") and not any("SCAN v2_drafts" in detail for detail in plan)
            for plan in draft_plans
        )

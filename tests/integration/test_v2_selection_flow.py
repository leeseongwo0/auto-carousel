from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_article import (
    ArticleResult,
    ArticleSnapshot,
    body_identity,
    material_character_count,
)
from newsbot.v2_cli import _drain_enrichment_queue, _finalize_observation
from newsbot.v2_policy import V2Outcome
from newsbot.v2_workflow import V2State, V2Workflow, V2WorkflowError

NOW = datetime.now(UTC)
MATERIAL = "Bitcoin blockchain regulation entered into force. " * 5
ARTICLE = "Bitcoin blockchain regulation entered into force for named service operators. " * 5


def observation(post_id: str, *urls: str, text: str = MATERIAL) -> SourceObservation:
    return SourceObservation(
        channel_id="channel",
        channel_handle="source",
        external_post_id=post_id,
        published_at=NOW,
        observed_at=NOW,
        text=text,
        urls=tuple(UrlCandidate(url, occurrence=index) for index, url in enumerate(urls)),
    )


def article(url: str, result: ArticleResult = ArticleResult.SUCCESS, *, body: str = ARTICLE) -> ArticleSnapshot:
    return ArticleSnapshot(
        result=result,
        requested_url=url,
        final_url=url,
        canonical_url=url,
        body=body,
        body_hash=body_identity(body),
        material_count=material_character_count(body),
    )


class ScriptedTransport:
    def __init__(self, outcomes: dict[str, ArticleSnapshot]) -> None:
        self.outcomes = outcomes
        self.calls: list[str] = []

    def fetch(self, requested_url: str, *, telegram_date: datetime) -> ArticleSnapshot:
        assert telegram_date == NOW
        self.calls.append(requested_url)
        return self.outcomes[requested_url]


def finalized_candidate(
    workflow: V2Workflow,
    post_id: str,
    *,
    url: str | None = None,
    body: str | None = None,
):
    url = url or f"https://example.test/{post_id}"
    body = body or f"Bitcoin blockchain regulation evidence {post_id}. " * 8
    candidate = _finalize_observation(
        workflow,
        observation(post_id, url),
        ScriptedTransport({url: article(url, body=body)}),
        owner=post_id,
    )
    assert candidate is not None
    return candidate


def held_candidate_ids(workflow: V2Workflow, candidate_ids: list[str]) -> set[str]:
    rows = workflow._db.execute(
        "SELECT candidate_id FROM v2_candidate_bindings WHERE held=1 AND candidate_id IN ({})".format(
            ",".join("?" for _ in candidate_ids)
        ),
        candidate_ids,
    ).fetchall()
    return {str(row["candidate_id"]) for row in rows}


def test_cutover_hold_manifest_is_sorted_stable_and_releases_clean_candidates(tmp_path) -> None:
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidates = [
            finalized_candidate(workflow, "second"),
            finalized_candidate(workflow, "first"),
        ]

        manifest = workflow.hold_notification_eligible_candidates()
        assert manifest["ids"] == sorted(candidate.id for candidate in candidates)
        assert manifest["digest"] == workflow.release_manifest_digest(manifest["items"])
        assert workflow.hold_notification_eligible_candidates() == manifest
        assert held_candidate_ids(workflow, manifest["ids"]) == set(manifest["ids"])

        released = workflow.release_held_candidates(list(reversed(manifest["ids"])), str(manifest["digest"]))
        assert released == {
            "ids": [],
            "items": [],
            "digest": workflow.release_manifest_digest([]),
        }
        assert held_candidate_ids(workflow, manifest["ids"]) == set()
        assert workflow.next_candidate_pending_notification() is not None


def test_cutover_release_rejects_post_manifest_truth_changes_atomically(tmp_path) -> None:
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        remote = finalized_candidate(workflow, "remote")
        manual = finalized_candidate(workflow, "manual")
        quarantined = finalized_candidate(workflow, "quarantined")
        quarantine_support = finalized_candidate(
            workflow,
            "quarantine-support",
            body="Ethereum blockchain security support evidence. " * 8,
        )
        clean = finalized_candidate(workflow, "clean")
        manifest = workflow.hold_notification_eligible_candidates()
        assert set(manifest["ids"]) == {
            remote.id,
            manual.id,
            quarantined.id,
            quarantine_support.id,
            clean.id,
        }

        assert workflow.record_remote_attempt(remote.id, "cutover_reconciliation") == 1
        workflow.mark_manual_review(manual.id, "post-manifest review required")
        conflict_url = "https://example.test/quarantined"
        assert (
            _finalize_observation(
                workflow,
                observation("quarantine-conflict", conflict_url),
                ScriptedTransport(
                    {
                        conflict_url: article(
                            conflict_url,
                            body="Ethereum blockchain security support evidence. " * 8,
                        )
                    }
                ),
                owner="quarantine-conflict",
            )
            is None
        )
        story = workflow._db.execute(
            "SELECT s.quarantined_at FROM v2_stories s "
            "JOIN v2_candidate_bindings b ON b.story_id=s.id "
            "WHERE b.candidate_id=?",
            (quarantined.id,),
        ).fetchone()
        assert story["quarantined_at"] is not None
        support_story = workflow._db.execute(
            "SELECT s.quarantined_at FROM v2_stories s "
            "JOIN v2_candidate_bindings b ON b.story_id=s.id "
            "WHERE b.candidate_id=?",
            (quarantine_support.id,),
        ).fetchone()
        assert support_story["quarantined_at"] is not None

        for candidate_id in (remote.id, manual.id, quarantined.id):
            with pytest.raises(V2WorkflowError, match="not safely releasable"):
                workflow.release_held_candidates([candidate_id], sha256(candidate_id.encode()).hexdigest())

        with pytest.raises(V2WorkflowError, match="not safely releasable"):
            workflow.release_held_candidates(list(manifest["ids"]), str(manifest["digest"]))
        assert held_candidate_ids(workflow, manifest["ids"]) == set(manifest["ids"])


def test_cutover_release_rejects_evidence_changes_after_manifest(tmp_path) -> None:
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = finalized_candidate(workflow, "evidence-change")
        manifest = workflow.hold_notification_eligible_candidates()
        binding = workflow._db.execute(
            "SELECT snapshot_id FROM v2_candidate_bindings WHERE candidate_id=?",
            (candidate.id,),
        ).fetchone()
        assert binding is not None
        workflow._db.execute(
            "UPDATE v2_article_snapshots SET snapshot=? WHERE id=?",
            ('{"changed":true}', int(binding["snapshot_id"])),
        )
        workflow._db.commit()

        with pytest.raises(V2WorkflowError, match="manifest mismatch"):
            workflow.release_held_candidates(
                list(manifest["ids"]),
                str(manifest["digest"]),
            )
        assert held_candidate_ids(workflow, list(manifest["ids"])) == set(manifest["ids"])


def test_effectful_candidates_are_excluded_from_new_cutover_manifest(tmp_path) -> None:
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        clean = finalized_candidate(workflow, "clean")
        remote = finalized_candidate(workflow, "remote")
        callback = finalized_candidate(workflow, "callback")
        manual = finalized_candidate(workflow, "manual")
        delivered = finalized_candidate(workflow, "delivered")
        quarantined = finalized_candidate(workflow, "quarantined")
        quarantine_support = finalized_candidate(
            workflow,
            "quarantine-support",
            body="Ethereum blockchain security support evidence. " * 8,
        )

        workflow.record_remote_attempt(remote.id, "cutover_reconciliation")
        assert workflow.claim_notification(
            entity_id=callback.id,
            callback_stage="candidate",
            token_hash="c" * 64,
            expires_at=(NOW + timedelta(hours=1)).isoformat(),
            claim_detail="candidate notification dispatched",
        )
        workflow.mark_manual_review(manual.id, "manual review required")

        workflow.approve_candidate(delivered.id)
        draft = workflow.create_draft(
            delivered.id,
            json.dumps(
                {
                    "draft": True,
                    "source_reported": True,
                    "category": "AI",
                    "cover": {
                        "title": "T",
                        "subtitle": "S",
                        "factual_units": [],
                    },
                    "bodies": [],
                    "caption": {
                        "hook": "H",
                        "context": "C",
                        "details": "D",
                        "implications": "I",
                        "questions": "Q",
                        "hashtags": [],
                    },
                }
            ),
        )
        workflow.approve_draft(draft.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed", receipt_id="sheet-row")
        assert workflow.mark_sheet_delivered(draft.id).state == V2State.SHEET_DELIVERED.value

        conflict_url = "https://example.test/quarantined"
        assert (
            _finalize_observation(
                workflow,
                observation("quarantine-conflict", conflict_url),
                ScriptedTransport(
                    {
                        conflict_url: article(
                            conflict_url,
                            body="Ethereum blockchain security support evidence. " * 8,
                        )
                    }
                ),
                owner="quarantine-conflict",
            )
            is None
        )
        story = workflow._db.execute(
            "SELECT s.quarantined_at FROM v2_stories s "
            "JOIN v2_candidate_bindings b ON b.story_id=s.id "
            "WHERE b.candidate_id=?",
            (quarantined.id,),
        ).fetchone()
        assert story["quarantined_at"] is not None
        support_story = workflow._db.execute(
            "SELECT s.quarantined_at FROM v2_stories s "
            "JOIN v2_candidate_bindings b ON b.story_id=s.id "
            "WHERE b.candidate_id=?",
            (quarantine_support.id,),
        ).fetchone()
        assert support_story["quarantined_at"] is not None

        manifest = workflow.hold_notification_eligible_candidates()
        assert manifest["ids"] == [clean.id]
        assert quarantine_support.id not in manifest["ids"]
        assert held_candidate_ids(workflow, [clean.id]) == {clean.id}


def test_permanent_url_failure_falls_through_but_transient_does_not(tmp_path) -> None:
    first = "https://first.example/news"
    second = "https://second.example/news"
    with V2Workflow(tmp_path / "permanent.sqlite", mode="create") as workflow:
        transport = ScriptedTransport(
            {
                first: article(first, ArticleResult.PERMANENT_FAILURE),
                second: article(second),
            }
        )
        candidate = _finalize_observation(workflow, observation("1", first, second), transport, owner="worker")
        assert candidate is not None
        assert transport.calls == [first, second]

    with V2Workflow(tmp_path / "transient.sqlite", mode="create") as workflow:
        transport = ScriptedTransport(
            {
                first: article(first, ArticleResult.TRANSIENT_FAILURE),
                second: article(second),
            }
        )
        assert _finalize_observation(workflow, observation("1", first, second), transport, owner="worker") is None
        assert transport.calls == [first]
        attempt = workflow._db.execute("SELECT status,attempt_number FROM v2_enrichment_attempts").fetchone()
        assert tuple(attempt) == ("retryable", 1)


def test_title_only_and_unavailable_source_follow_outcome_matrix(tmp_path) -> None:
    url = "https://example.test/news"
    with V2Workflow(tmp_path / "short.sqlite", mode="create") as workflow:
        transport = ScriptedTransport({url: article(url, body="Short title")})
        candidate = _finalize_observation(workflow, observation("1", url), transport, owner="worker")
        assert candidate is not None
        assert candidate.policy_outcome == V2Outcome.AMBIGUOUS.value
        assert candidate.policy_reason == "source_body_insufficient"

    with V2Workflow(tmp_path / "missing.sqlite", mode="create") as workflow:
        transport = ScriptedTransport({})
        assert _finalize_observation(workflow, observation("1"), transport, owner="worker") is None
        assert workflow.list_candidates() == []
        assert workflow._db.execute("SELECT COUNT(*) FROM v2_stories").fetchone()[0] == 0


def test_unchanged_observation_skips_fetch_and_delivered_repost_is_suppressed(tmp_path) -> None:
    url = "https://example.test/news"
    transport = ScriptedTransport({url: article(url)})
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = _finalize_observation(workflow, observation("1", url), transport, owner="worker")
        assert candidate is not None
        assert _finalize_observation(workflow, observation("1", url), transport, owner="worker") is None
        assert transport.calls == [url]
        assert (
            _finalize_observation(
                workflow,
                observation("1", url, text=MATERIAL + " corrected"),
                transport,
                owner="worker",
            )
            is None
        )
        assert transport.calls == [url]
        assert (
            workflow._db.execute("SELECT COUNT(*) FROM v2_observation_revisions WHERE identity='channel:1'").fetchone()[
                0
            ]
            == 2
        )
        assert workflow._db.execute("SELECT COUNT(*) FROM v2_enrichment_attempts").fetchone()[0] == 1

        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(
            candidate.id,
            '{"draft":true,"source_reported":true,"category":"AI","cover":{"title":"T","subtitle":"S","factual_units":[]},"bodies":[],"caption":{"hook":"H","context":"C","details":"D","implications":"I","questions":"Q","hashtags":[]}}',
        )
        workflow.approve_draft(draft.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed", receipt_id="sheet-row")
        delivered = workflow.mark_sheet_delivered(draft.id)
        assert delivered.state == V2State.SHEET_DELIVERED.value

        assert _finalize_observation(workflow, observation("2", url), transport, owner="worker") is None
        assert len(workflow.list_candidates()) == 1


def test_multiple_claimed_story_matches_quarantine_without_new_candidate(
    tmp_path,
) -> None:
    first_url = "https://example.test/first"
    second_url = "https://example.test/second"
    first_body = "Bitcoin blockchain regulation first report. " * 8
    second_body = "Ethereum blockchain security second report. " * 8
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        first = _finalize_observation(
            workflow,
            observation("first", first_url),
            ScriptedTransport(
                {
                    first_url: article(
                        first_url,
                        body=first_body,
                    )
                }
            ),
            owner="first",
        )
        second = _finalize_observation(
            workflow,
            observation("second", second_url),
            ScriptedTransport(
                {
                    second_url: article(
                        second_url,
                        body=second_body,
                    )
                }
            ),
            owner="second",
        )
        assert first is not None and second is not None

        conflict = _finalize_observation(
            workflow,
            observation("conflict", first_url),
            ScriptedTransport(
                {
                    first_url: article(
                        first_url,
                        body=second_body,
                    )
                }
            ),
            owner="conflict",
        )
        assert conflict is None
        assert len(workflow.list_candidates()) == 2
        rows = workflow._db.execute(
            "SELECT DISTINCT s.quarantined_at,b.held "
            "FROM v2_stories s "
            "JOIN v2_candidate_bindings b ON b.story_id=s.id "
            "WHERE b.candidate_id IN (?,?)",
            (first.id, second.id),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["quarantined_at"] is not None
        assert rows[0]["held"] == 1
        assert workflow.next_candidate_pending_notification() is None


def test_selected_redirect_aliases_converge_but_unselected_url_does_not(
    tmp_path,
) -> None:
    requested = "https://example.test/requested"
    final = "https://example.test/final"
    canonical = "https://example.test/canonical"
    unselected = "https://example.test/unselected"
    first_body = "Bitcoin blockchain regulation primary evidence. " * 8
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        first = _finalize_observation(
            workflow,
            observation("first", requested, unselected),
            ScriptedTransport(
                {
                    requested: ArticleSnapshot(
                        ArticleResult.SUCCESS,
                        requested,
                        final_url=final,
                        canonical_url=canonical,
                        body=first_body,
                        body_hash=body_identity(first_body),
                        material_count=material_character_count(first_body),
                    )
                }
            ),
            owner="first",
        )
        assert first is not None

        final_repost = _finalize_observation(
            workflow,
            observation("final-repost", final),
            ScriptedTransport(
                {
                    final: article(
                        final,
                        body=("Ethereum blockchain distinct evidence. " * 8),
                    )
                }
            ),
            owner="final-repost",
        )
        assert final_repost is None

        unselected_post = _finalize_observation(
            workflow,
            observation("unselected", unselected),
            ScriptedTransport(
                {
                    unselected: article(
                        unselected,
                        body=("AI blockchain separate deployment evidence. " * 8),
                    )
                }
            ),
            owner="unselected",
        )
        assert unselected_post is not None
        assert len(workflow.list_candidates()) == 2


def test_due_enrichment_survives_restart_and_stale_content_skips_network(
    tmp_path,
) -> None:
    url = "https://example.test/retry"
    database = tmp_path / "v2.sqlite"
    transient = ScriptedTransport({url: article(url, ArticleResult.TRANSIENT_FAILURE)})
    with V2Workflow(database, mode="create") as workflow:
        assert (
            _finalize_observation(
                workflow,
                observation("retry", url),
                transient,
                owner="first-run",
            )
            is None
        )
        workflow._db.execute(
            "UPDATE v2_enrichment_attempts SET next_retry_at=? WHERE status='retryable'",
            (datetime.now(UTC).isoformat(),),
        )
        workflow._db.commit()

    success = ScriptedTransport({url: article(url)})
    with V2Workflow(database, mode="runtime") as workflow:
        candidates = _drain_enrichment_queue(
            workflow,
            success,
            owner="restart",
            limit=10,
        )
        assert len(candidates) == 1
        assert workflow._db.execute("SELECT COUNT(*) FROM v2_enrichment_attempts").fetchone()[0] == 2
        assert success.calls == [url]

        stale_url = "https://example.test/stale"
        stale = SourceObservation(
            channel_id="channel",
            channel_handle="source",
            external_post_id="stale",
            published_at=datetime.now(UTC) - timedelta(hours=25),
            observed_at=datetime.now(UTC),
            text=MATERIAL,
            urls=(UrlCandidate(stale_url),),
        )
        forbidden = ScriptedTransport({})
        assert (
            _finalize_observation(
                workflow,
                stale,
                forbidden,
                owner="stale",
            )
            is None
        )
        assert forbidden.calls == []
        disposition = workflow._db.execute(
            "SELECT outcome,reason FROM v2_observation_dispositions WHERE identity='channel:stale'"
        ).fetchone()
        assert tuple(disposition) == ("non_news", "freshness_gate")


def test_source_failure_preserves_topic_and_sponsorship_exclusions(
    tmp_path,
) -> None:
    url = "https://example.test/unavailable"
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        unavailable = ScriptedTransport({url: article(url, ArticleResult.PERMANENT_FAILURE)})
        off_topic = observation(
            "off-topic",
            url,
            text="A local restaurant changed its menu and opening hours. " * 4,
        )
        assert (
            _finalize_observation(
                workflow,
                off_topic,
                unavailable,
                owner="policy",
            )
            is None
        )
        reason = workflow._db.execute(
            "SELECT reason FROM v2_observation_dispositions WHERE identity='channel:off-topic'"
        ).fetchone()[0]
        assert reason == "topic_gate"

        sponsored = SourceObservation(
            channel_id="channel",
            channel_handle="source",
            external_post_id="sponsored",
            published_at=NOW,
            observed_at=NOW,
            text="Sponsored Bitcoin blockchain platform campaign. " * 5,
            sponsored=True,
            urls=(UrlCandidate(url),),
        )
        assert (
            _finalize_observation(
                workflow,
                sponsored,
                unavailable,
                owner="policy",
            )
            is None
        )
        reason = workflow._db.execute(
            "SELECT reason FROM v2_observation_dispositions WHERE identity='channel:sponsored'"
        ).fetchone()[0]
        assert reason == "marketing_promotion"

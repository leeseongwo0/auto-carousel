from datetime import UTC, datetime

import pytest

from newsbot.collectors.base import SourceObservation, UrlCandidate
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

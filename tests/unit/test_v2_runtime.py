from datetime import datetime, timezone

from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_runtime import (
    AmbiguousRemoteEffect,
    ClearNetworkFailure,
    RemoteEffect,
    V2Runtime,
)
from newsbot.v2_workflow import V2State, V2Workflow


def observation():
    return SourceObservation(
        channel_id="channel", channel_handle="handle", external_post_id="1",
        published_at=datetime.now(timezone.utc),
        text="OpenAI announced a major integration with enterprise data infrastructure available to users.",
        urls=(UrlCandidate("https://example.test/story"),),
    )


def runtime(db, events, *, candidate=lambda _: True, draft=lambda _: True, sheets=lambda _: True, generate=lambda _: "copy", max_retries=1):
    workflow = V2Workflow(db)
    return workflow, V2Runtime(
        workflow, notify_candidate=lambda value: (events.append("candidate"), candidate(value))[1],
        generate_draft=lambda value: (events.append("generate"), generate(value))[1],
        notify_draft=lambda value: (events.append("draft"), draft(value))[1],
        deliver_sheets=lambda value: (events.append("sheets"), sheets(value))[1],
        max_retries=max_retries,
    )


def test_runtime_orders_both_approvals_before_delivery(tmp_path):
    events = []
    workflow, runner = runtime(tmp_path / "v2.sqlite", events)
    result = runner.process_observation(observation())
    assert result.state == V2State.SHEET_DELIVERED
    assert events == ["candidate", "generate", "draft", "sheets"]
    workflow.close()


def test_clear_network_failure_retries_only_with_bound(tmp_path):
    events, calls = [], []

    def notify(_):
        calls.append(1)
        raise ClearNetworkFailure()

    workflow, runner = runtime(tmp_path / "v2.sqlite", events, candidate=notify, max_retries=2)
    result = runner.process_observation(observation())
    assert result.state == V2State.MANUAL_REVIEW
    assert len(calls) == 3
    workflow.close()


def test_ambiguous_remote_result_is_manual_review_without_resend(tmp_path):
    events, calls = [], []

    def sheets(_):
        calls.append(1)
        raise AmbiguousRemoteEffect()

    workflow, runner = runtime(tmp_path / "v2.sqlite", events, sheets=sheets)
    result = runner.process_observation(observation())
    assert result.state == V2State.MANUAL_REVIEW
    assert calls == [1]
    runner.run(result.candidate_id)
    assert calls == [1]
    workflow.close()


def test_explicit_ambiguous_effect_is_not_retried(tmp_path):
    events, calls = [], []
    workflow, runner = runtime(tmp_path / "v2.sqlite", events, sheets=lambda _: (calls.append(1), RemoteEffect.AMBIGUOUS)[1])
    result = runner.process_observation(observation())
    assert result.state == V2State.MANUAL_REVIEW
    assert calls == [1]
    workflow.close()


def test_remote_effect_receipts_survive_reopen(tmp_path):
    events = []
    workflow, runner = runtime(tmp_path / "v2.sqlite", events)
    result = runner.process_observation(observation())
    candidate = workflow.get_candidate(result.candidate_id)
    assert workflow.remote_effect(candidate.id, "candidate_notification")["status"] == "confirmed"
    assert workflow.remote_effect(result.id, "sheets_delivery")["status"] == "confirmed"
    workflow.close()
    reopened = V2Workflow(tmp_path / "v2.sqlite")
    assert reopened.remote_effect(result.id, "sheets_delivery")["attempts"] == 1
    reopened.close()

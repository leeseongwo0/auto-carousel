from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from newsbot import v2_cli
from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_live import TelethonV2Collector, V2LiveWorkflow
from newsbot.v2_workflow import V2State, V2Workflow


def observation() -> SourceObservation:
    return SourceObservation(
        channel_id="channel",
        channel_handle="configured",
        external_post_id="1",
        published_at=datetime.now(UTC),
        text="OpenAI announced an enterprise infrastructure integration available to customers worldwide today with documented production rollout details.",
        urls=(UrlCandidate("https://example.test/source"),),
    )


def test_authenticated_callbacks_gate_generation_and_sheets(tmp_path):
    tokens, events = [], []
    workflow = V2Workflow(tmp_path / "v2.sqlite")
    live = V2LiveWorkflow(
        workflow,
        notify_candidate=lambda _candidate, token: (
            tokens.append(token),
            events.append("candidate"),
            "candidate-message",
        )[2],
        generate_draft=lambda _candidate: (events.append("generate"), "exact draft")[1],
        notify_draft=lambda _draft, token: (tokens.append(token), events.append("draft"), "draft-message")[2],
        deliver_sheets=lambda _draft: (events.append("sheets"), True)[1],
    )
    candidate = live.process_observation(observation())
    assert candidate and candidate.state == V2State.PENDING_CANDIDATE
    assert events == ["candidate"]
    assert live.settle_callback(candidate.id, "candidate") is None
    draft = live.settle_callback(tokens.pop(0), "candidate")
    assert draft and draft.state == V2State.DRAFT_PENDING_APPROVAL
    assert events == ["candidate", "generate", "draft"]
    delivered = live.settle_callback(tokens.pop(0), "draft")
    assert delivered and delivered.state == V2State.SHEET_DELIVERED
    assert events == ["candidate", "generate", "draft", "sheets"]
    workflow.close()


def test_ambiguous_delivery_is_never_resent_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    tokens, sends = [], []
    workflow = V2Workflow(db)
    live = V2LiveWorkflow(
        workflow,
        notify_candidate=lambda _candidate, token: (tokens.append(token), "candidate")[1],
        generate_draft=lambda _candidate: "draft",
        notify_draft=lambda _draft, token: (tokens.append(token), "draft")[1],
        deliver_sheets=lambda _draft: (sends.append(1), False)[1],
    )
    candidate = live.process_observation(observation())
    assert candidate
    draft = live.settle_callback(tokens.pop(0), "candidate")
    assert draft
    result = live.settle_callback(tokens.pop(0), "draft")
    assert result and result.state == V2State.MANUAL_REVIEW
    workflow.close()
    reopened = V2Workflow(db)
    again = V2LiveWorkflow(
        reopened,
        notify_candidate=lambda *_: True,
        generate_draft=lambda _: "x",
        notify_draft=lambda *_: True,
        deliver_sheets=lambda _: sends.append(2),
    )
    assert again.run(candidate.id).state == V2State.MANUAL_REVIEW
    assert sends == [1]
    reopened.close()


def test_collector_uses_only_configured_handles():
    class Collector:
        def __init__(self):
            self.handles = []

        async def collect(self, handle):
            self.handles.append(handle)
            return ()

    collector = Collector()
    assert TelethonV2Collector(collector, ("@first", "second")).collect_sync() == ()
    assert collector.handles == ["first", "second"]


def test_live_collection_is_bounded(monkeypatch, tmp_path):
    calls = []

    class Collector:
        def __init__(self, *_args):
            pass

        async def collect(self, handle, **kwargs):
            calls.append((handle, kwargs))
            return ()

        async def close(self):
            pass

    monkeypatch.setattr(v2_cli, "TelethonCollector", Collector)
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_API_ID", "1")
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_API_HASH", "hash")
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_SESSION", "session")
    monkeypatch.setenv("NEWSBOT_V2_TELEGRAM_HANDLES", "first,second")
    args = SimpleNamespace(db=tmp_path / "v2.sqlite", lookback_hours=12, limit=7)
    assert v2_cli.collect_live(args) == 0
    assert [handle for handle, _kwargs in calls] == ["first", "second"]
    assert all(kwargs["limit"] == 7 for _handle, kwargs in calls)
    assert all(datetime.now(UTC) - kwargs["lower_bound"] < timedelta(hours=13) for _handle, kwargs in calls)

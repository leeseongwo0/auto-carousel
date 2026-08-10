import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot import v2_cli
from newsbot.approval.base import hash_callback_token
from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.sheets.base import (
    DeliveryOutcome,
    DispatchCredentialAttestation,
    MetadataState,
    PreparedSheetMutation,
    SheetDelivery,
)
from newsbot.v2_live import (
    TelethonV2Collector,
    V2LiveWorkflow,
    deliver_v2_google_sheets,
    v2_draft_handoff_values,
    v2_sheet_export_id,
)
from newsbot.v2_workflow import V2State, V2Workflow, V2WorkflowError


def observation() -> SourceObservation:
    return SourceObservation(
        channel_id="channel",
        channel_handle="configured",
        external_post_id="1",
        published_at=datetime.now(UTC),
        text="OpenAI announced an enterprise infrastructure integration available to customers worldwide today with documented production rollout details.",
        urls=(UrlCandidate("https://example.test/source"),),
    )


def exact_draft_content() -> str:
    return json.dumps(
        {
            "cover": {
                "title": "규제 변화",
                "subtitle": "송금 대기 제도",
                "factual_units": [
                    {
                        "text": "검증된 규제 변화입니다.",
                        "references": [{"claim_id": "claim-1", "source_version_id": 1}],
                    }
                ],
            },
            "bodies": [
                {
                    "subtitle": "핵심 내용",
                    "body": "검증된 규제 변화입니다.",
                    "factual_units": [
                        {
                            "text": "검증된 규제 변화입니다.",
                            "references": [{"claim_id": "claim-1", "source_version_id": 1}],
                        }
                    ],
                }
            ],
            "caption": {
                "hook": "새로운 규제입니다.",
                "context": "브라질의 제도 변화입니다.",
                "details": "송금에 대기 시간이 적용됩니다.",
                "implications": "이용자 보호에 영향을 줍니다.",
                "questions": "다른 국가에도 확산될까요?",
                "hashtags": ["#블록체인"],
            },
            "category": "Blockchain",
            "draft": True,
            "source_reported": True,
        },
        ensure_ascii=False,
    )


def approved_draft(workflow: V2Workflow, post_id: str = "1"):
    source = observation()
    if post_id != source.external_post_id:
        source = SourceObservation(
            channel_id=source.channel_id,
            channel_handle=source.channel_handle,
            external_post_id=post_id,
            published_at=source.published_at,
            text=source.text,
            urls=source.urls,
        )
    candidate = workflow.record_observation(source)
    assert candidate is not None
    workflow.approve_candidate(candidate.id)
    pending = workflow.create_draft(candidate.id, exact_draft_content())
    return candidate, workflow.approve_draft(pending.id)


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


class ScriptedSheetsAdapter:
    def __init__(
        self,
        metadata: MetadataState,
        outcome: DeliveryOutcome = DeliveryOutcome.APPLIED,
    ):
        self.metadata = metadata
        self.outcome = outcome
        self.prepared = False
        self.armed = False
        self.dispatched = False
        self.export_ids = []

    def prepare_delivery(self, *, export_id, canonical_sha256, values):
        self.prepared = True
        self.export_ids.append(export_id)
        assert export_id.startswith("exp_")
        assert len(export_id) == 36
        assert export_id
        assert len(canonical_sha256) == 64
        assert len(values) == 22
        return PreparedSheetMutation(
            {},
            "request-sha",
            metadata=self.metadata,
            metadata_value=f"newsbot-v2:{export_id}",
        )

    def dispatch_credential_attestation(self):
        return DispatchCredentialAttestation(
            refreshed_at="2026-08-09T00:00:00+00:00",
            expires_at="2026-08-09T01:00:00+00:00",
            scope_ok=True,
        )

    def arm_prepared_dispatch(self):
        self.armed = True

    def dispatch_prepared(self, _prepared):
        self.dispatched = True
        return SheetDelivery(self.outcome)


@pytest.mark.parametrize(
    ("metadata", "expected_state", "armed", "dispatched"),
    [
        (MetadataState.EXACT, V2State.SHEET_DELIVERED, False, False),
        (MetadataState.ABSENT, V2State.SHEET_DELIVERED, True, True),
        (MetadataState.CONFLICT, V2State.MANUAL_REVIEW, False, False),
    ],
)
def test_v2_sheets_delivery_honors_prepared_metadata(
    metadata,
    expected_state,
    armed,
    dispatched,
    tmp_path,
):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.approve_candidate(candidate.id)
        pending = workflow.create_draft(candidate.id, exact_draft_content())
        draft = workflow.approve_draft(pending.id)
        values = v2_draft_handoff_values(draft, "2026-08-09")
        assert len(values) == 22

        adapter = ScriptedSheetsAdapter(metadata)
        result = deliver_v2_google_sheets(
            workflow,
            draft,
            adapter,
            values,
            lease_seconds=120,
        )
        assert result.state == expected_state
        assert adapter.armed is armed
        assert adapter.dispatched is dispatched
        assert adapter.export_ids == [v2_sheet_export_id(draft.id)]
        receipt = workflow.remote_effect(draft.id, "sheets_delivery")
        assert receipt is not None
        detail = json.loads(str(receipt["detail"]))
        assert detail["request_sha256"] == "request-sha"
        assert detail["metadata_value"].startswith("newsbot-v2:")
        assert detail["owner"]
        if metadata is MetadataState.ABSENT:
            assert detail["attestation"]["scope_ok"] is True


def test_v2_sheet_export_identity_is_deterministic_and_namespaced():
    assert v2_sheet_export_id("draft") == v2_sheet_export_id("draft")
    assert v2_sheet_export_id("draft") != v2_sheet_export_id("other")
    with pytest.raises(ValueError, match="must not be empty"):
        v2_sheet_export_id("")


@pytest.mark.parametrize(
    "outcome",
    [DeliveryOutcome.AMBIGUOUS, DeliveryOutcome.BLOCKED, DeliveryOutcome.NOT_APPLIED],
)
def test_non_applied_sheets_outcome_is_durably_ambiguous(outcome, tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        _candidate, draft = approved_draft(workflow)
        values = v2_draft_handoff_values(draft, "2026-08-09")
        result = deliver_v2_google_sheets(
            workflow,
            draft,
            ScriptedSheetsAdapter(MetadataState.ABSENT, outcome),
            values,
            lease_seconds=120,
        )
        assert result.state == V2State.MANUAL_REVIEW
        receipt = workflow.remote_effect(draft.id, "sheets_delivery")
        assert receipt is not None
        assert receipt["status"] == "ambiguous"
        detail = json.loads(str(receipt["detail"]))
        assert detail["request_sha256"] == "request-sha"
        assert detail["metadata_value"].startswith("newsbot-v2:")
        assert detail["owner"]
        assert detail["attestation"]["scope_ok"] is True


def test_expired_pending_sheets_claim_becomes_manual_without_resend(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        _candidate, draft = approved_draft(workflow)
        detail = json.dumps(
            {
                "lease_expires_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                "phase": "possibly_sent",
            },
            sort_keys=True,
        )
        assert workflow.claim_remote_effect(draft.id, "sheets_delivery", detail)
        adapter = ScriptedSheetsAdapter(MetadataState.ABSENT)
        result = deliver_v2_google_sheets(
            workflow,
            draft,
            adapter,
            v2_draft_handoff_values(draft, "2026-08-09"),
            lease_seconds=120,
        )
        assert result.state == V2State.MANUAL_REVIEW
        assert not adapter.prepared
        assert not adapter.dispatched


def test_live_pending_sheets_claim_rejects_competing_sender(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        _candidate, draft = approved_draft(workflow)
        detail = json.dumps(
            {
                "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
                "phase": "possibly_sent",
            },
            sort_keys=True,
        )
        assert workflow.claim_remote_effect(draft.id, "sheets_delivery", detail)
        adapter = ScriptedSheetsAdapter(MetadataState.ABSENT)
        with pytest.raises(V2WorkflowError, match="already in progress"):
            deliver_v2_google_sheets(
                workflow,
                draft,
                adapter,
                v2_draft_handoff_values(draft, "2026-08-09"),
                lease_seconds=120,
            )
        assert not adapter.prepared
        assert not adapter.dispatched


def test_generic_pending_remote_attempt_is_not_resent(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.record_remote_attempt(candidate.id, "candidate_notification")
        sends = []
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=lambda *_: sends.append(1),
            generate_draft=lambda _: "unused",
            notify_draft=lambda *_: True,
            deliver_sheets=lambda _: True,
        )
        result = live.run(candidate.id)
        assert result.state == V2State.MANUAL_REVIEW
        assert sends == []


def test_generic_sheet_delivery_records_non_applied_as_ambiguous(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate, draft = approved_draft(workflow)
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=lambda *_: True,
            generate_draft=lambda _: "unused",
            notify_draft=lambda *_: True,
            deliver_sheets=lambda _: SheetDelivery(DeliveryOutcome.AMBIGUOUS),
        )
        result = live.run(candidate.id)
        assert result.state == V2State.MANUAL_REVIEW
        receipt = workflow.remote_effect(draft.id, "sheets_delivery")
        assert receipt is not None
        assert receipt["status"] == "ambiguous"


def test_google_sheets_cli_rejects_unapproved_draft_before_credentials(tmp_path, monkeypatch):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, "exact pending draft")

    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("GOOGLE_SHEETS_SPREADSHEET_ID", raising=False)
    with pytest.raises(Exception, match="requires draft_approved"):
        v2_cli.deliver_google_sheets(SimpleNamespace(db=db, draft_id=draft.id, deadline=120.0))


def test_confirmed_sheets_receipt_finishes_local_transition_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    sends = []
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, exact_draft_content())
        workflow.approve_draft(draft.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed", receipt_id="sheet-row")

    with V2Workflow(db) as reopened:
        live = V2LiveWorkflow(
            reopened,
            notify_candidate=lambda *_: True,
            generate_draft=lambda _: "unused",
            notify_draft=lambda *_: True,
            deliver_sheets=lambda _: sends.append(1),
        )
        result = live.run(candidate.id)
        assert result.state == V2State.SHEET_DELIVERED
        assert sends == []


def test_v2_telegram_tick_authorizes_callbacks_and_advances_cursor(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    token = "C" * 43
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.issue_callback(
            hash_callback_token(token),
            candidate.id,
            "candidate",
            (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        )
        workflow.handoff_telegram_cursor(40)

    calls = []

    class Adapter:
        def __init__(self, *_args):
            pass

        def send_message_once(self, *_args, **_kwargs):
            calls.append(("sendMessage", {}))
            return SimpleNamespace(accepted=True, message_id=1, safe_code=None)

        def _request(self, method, payload, **_kwargs):
            calls.append((method, payload))
            if method == "getUpdates":
                return {
                    "result": [
                        {
                            "update_id": 41,
                            "callback_query": {
                                "id": "callback-id",
                                "data": token,
                                "message": {"chat": {"id": 100}},
                                "from": {"id": 200},
                            },
                        },
                        {
                            "update_id": 42,
                            "callback_query": {
                                "data": token,
                                "message": {"chat": {"id": 100}},
                                "from": {"id": 999},
                            },
                        },
                    ]
                }
            return {"result": True}

    monkeypatch.setattr(v2_cli, "TelegramApprovalAdapter", Adapter)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "100")
    monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "200")
    args = SimpleNamespace(db=db, deadline=5.0, timeout=0)
    assert v2_cli.telegram_tick(args) == 0
    assert [method for method, _payload in calls] == ["sendMessage", "getUpdates", "answerCallbackQuery"]
    with V2Workflow(db) as workflow:
        assert workflow.get_candidate(candidate.id).state == V2State.CANDIDATE_APPROVED
        assert workflow.telegram_next_offset() == 43


def test_v2_callback_settlement_rejects_unauthorized_and_duplicates(tmp_path):
    token = "D" * 43
    with V2Workflow(tmp_path / "v2.sqlite") as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.issue_callback(
            hash_callback_token(token),
            candidate.id,
            "candidate",
            (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        )
        update = {
            "callback_query": {
                "data": token,
                "message": {"chat": {"id": 100}},
                "from": {"id": 200},
            }
        }
        assert v2_cli._telegram_callback_status(workflow, update, chat_id=100, user_ids={999}) is None
        assert (
            v2_cli._telegram_callback_status(workflow, update, chat_id=100, user_ids={200}) == "v2_candidate_approved"
        )
        assert v2_cli._telegram_callback_status(workflow, update, chat_id=100, user_ids={200}) is None


def test_v2_telegram_tick_prioritizes_draft_notifications(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        pending = workflow.record_observation(observation())
        assert pending is not None
        approved = workflow.record_observation(
            SourceObservation(
                channel_id="channel",
                channel_handle="configured",
                external_post_id="2",
                published_at=datetime.now(UTC),
                text=observation().text,
                urls=(UrlCandidate("https://example.test/source-2"),),
            )
        )
        assert approved is not None
        workflow.approve_candidate(approved.id)
        draft = workflow.create_draft(approved.id, exact_draft_content())
        workflow.handoff_telegram_cursor(0)

    selected = []

    class Adapter:
        def __init__(self, *_args):
            pass

        def _request(self, _method, _payload, **_kwargs):
            return {"result": []}

    class Live:
        def __init__(self, _workflow, **_kwargs):
            pass

        def run(self, candidate_id):
            selected.append(candidate_id)

    monkeypatch.setattr(v2_cli, "TelegramApprovalAdapter", Adapter)
    monkeypatch.setattr(v2_cli, "V2LiveWorkflow", Live)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "100")
    monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "200")
    assert v2_cli.telegram_tick(SimpleNamespace(db=db, deadline=5.0, timeout=0)) == 0
    assert selected == [draft.candidate_id]
    assert selected != [pending.id]


def test_v2_telegram_tick_refuses_before_cursor_handoff(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None

    calls = []

    class Adapter:
        def __init__(self, *_args):
            pass

        def send_message_once(self, *_args, **_kwargs):
            calls.append("send")
            raise AssertionError("notification must not run")

        def _request(self, *_args, **_kwargs):
            calls.append("poll")
            raise AssertionError("poll must not run")

    monkeypatch.setattr(v2_cli, "TelegramApprovalAdapter", Adapter)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "100")
    monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "200")
    with pytest.raises(V2WorkflowError, match="handoff is required"):
        v2_cli.telegram_tick(SimpleNamespace(db=db, deadline=5.0, timeout=0))
    assert calls == []


def test_v2_telegram_tick_recovers_crashed_notification_receipts_without_resend(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.record_remote_attempt(candidate.id, "candidate_notification")
        approved = workflow.record_observation(
            SourceObservation(
                channel_id="channel",
                channel_handle="configured",
                external_post_id="crashed-draft",
                published_at=datetime.now(UTC),
                text=observation().text,
                urls=(UrlCandidate("https://example.test/crashed-draft"),),
            )
        )
        assert approved is not None
        workflow.approve_candidate(approved.id)
        draft = workflow.create_draft(approved.id, exact_draft_content())
        workflow.record_remote_attempt(draft.id, "draft_notification")
        workflow.settle_remote_effect(draft.id, "draft_notification", "confirmed", receipt_id="remote-message")
        workflow.handoff_telegram_cursor(0)

    calls = []

    class Adapter:
        def __init__(self, *_args):
            pass

        def send_message_once(self, *_args, **_kwargs):
            calls.append("send")
            raise AssertionError("recovery must not resend")

        def _request(self, method, _payload, **_kwargs):
            calls.append(method)
            return {"result": []}

    monkeypatch.setattr(v2_cli, "TelegramApprovalAdapter", Adapter)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "100")
    monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "200")
    args = SimpleNamespace(db=db, deadline=5.0, timeout=0)
    assert v2_cli.telegram_tick(args) == 0
    assert calls == ["getUpdates"]
    with V2Workflow(db) as workflow:
        assert workflow.get_candidate(approved.id).state == V2State.DRAFT_PENDING_APPROVAL
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW
        assert workflow.get_draft(draft.id).state == V2State.DRAFT_PENDING_APPROVAL


def test_v2_telegram_tick_reconciles_expired_approval_capability(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        candidate = workflow.record_observation(observation())
        assert candidate is not None
        workflow.issue_callback(
            hash_callback_token("E" * 43),
            candidate.id,
            "candidate",
            (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        )
        workflow.handoff_telegram_cursor(0)

    class Adapter:
        def __init__(self, *_args):
            pass

        def send_message_once(self, *_args, **_kwargs):
            raise AssertionError("expired approval must not resend")

        def _request(self, _method, _payload, **_kwargs):
            return {"result": []}

    monkeypatch.setattr(v2_cli, "TelegramApprovalAdapter", Adapter)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "100")
    monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "200")
    assert v2_cli.telegram_tick(SimpleNamespace(db=db, deadline=5.0, timeout=0)) == 0
    with V2Workflow(db) as workflow:
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW


def test_deliver_google_sheets_next_recovers_crashed_receipts_without_dispatch(tmp_path, capsys):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        _candidate, confirmed = approved_draft(workflow)
        workflow.record_remote_attempt(confirmed.id, "sheets_delivery")
        workflow.settle_remote_effect(confirmed.id, "sheets_delivery", "confirmed", receipt_id="remote-row")
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out)["state"] == V2State.SHEET_DELIVERED
    with V2Workflow(db) as workflow:
        assert workflow.get_draft(confirmed.id).state == V2State.SHEET_DELIVERED

    with V2Workflow(db) as workflow:
        _candidate, pending = approved_draft(workflow, "pending-recovery")
        detail = json.dumps(
            {
                "lease_expires_at": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                "phase": "possibly_sent",
            },
            sort_keys=True,
        )
        assert workflow.claim_remote_effect(pending.id, "sheets_delivery", detail)
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out)["state"] == V2State.MANUAL_REVIEW
    with V2Workflow(db) as workflow:
        assert workflow.get_draft(pending.id).state == V2State.MANUAL_REVIEW


def test_deliver_google_sheets_next_reports_no_work(tmp_path, capsys):
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=tmp_path / "v2.sqlite", deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "no_work"}


def test_deliver_google_sheets_next_selects_one_approved_draft(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db) as workflow:
        _candidate, draft = approved_draft(workflow)

    delivered = []

    def deliver(workflow, selected, deadline):
        delivered.append((selected.id, deadline))
        return selected

    monkeypatch.setattr(v2_cli, "_deliver_google_sheets_draft", deliver)
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=12.0)) == 0
    assert delivered == [(draft.id, 12.0)]

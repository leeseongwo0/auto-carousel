import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot import v2_cli
from newsbot.approval.base import hash_callback_token
from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.collectors.telethon import EditSweepPage
from newsbot.sheets.base import (
    DeliveryOutcome,
    DispatchCredentialAttestation,
    MetadataState,
    PreparedSheetMutation,
    SheetDelivery,
)
from newsbot.v2_live import (
    AmbiguousRemoteEffect,
    SheetsClearPreDispatchNetworkError,
    TelegramV2Notifier,
    V2LiveWorkflow,
    deliver_v2_google_sheets,
    recover_v2_google_sheets_delivery,
    render_v2_candidate_message,
    v2_draft_handoff_values,
    v2_sheet_export_id,
)
from newsbot.v2_workflow import V2State, V2Workflow, V2WorkflowError
from tests.v2_support import create_candidate


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
            urls=(UrlCandidate(f"https://example.test/source-{post_id}"),),
        )
    candidate = create_candidate(workflow, source)
    assert candidate is not None
    workflow.approve_candidate(candidate.id)
    pending = workflow.create_draft(candidate.id, exact_draft_content())
    return candidate, workflow.approve_draft(pending.id)


def claim_test_capability(
    workflow: V2Workflow,
    entity_id: str,
    stage: str,
    token: str,
    expires_at: str,
    *,
    confirmed: bool,
) -> None:
    assert workflow.claim_notification(
        entity_id=entity_id,
        callback_stage=stage,
        token_hash=hash_callback_token(token),
        expires_at=expires_at,
        claim_detail=f"test_claim:{stage}",
    )
    if confirmed:
        workflow.settle_remote_effect(
            entity_id,
            ("candidate_notification" if stage == "candidate" else "draft_notification"),
            "confirmed",
            "remote_outcome_confirmed",
            receipt_id=f"message:{stage}",
        )


def test_approval_callbacks_do_not_run_generation_or_sheets(tmp_path):
    tokens: list[str] = []
    events: list[str] = []
    workflow = V2Workflow(tmp_path / "v2.sqlite", mode="create")
    live = V2LiveWorkflow(
        workflow,
        notify_candidate=lambda _candidate, token: (
            tokens.append(token),
            events.append("candidate"),
            "candidate-message",
        )[2],
        notify_draft=lambda _draft, token: (
            tokens.append(token),
            events.append("draft"),
            "draft-message",
        )[2],
    )
    candidate = create_candidate(workflow, observation())
    assert candidate and candidate.state == V2State.PENDING_CANDIDATE

    assert live.run(candidate.id).state == V2State.PENDING_CANDIDATE
    assert events == ["candidate"]
    assert live.settle_callback(candidate.id, "candidate") is None
    approved = live.settle_callback(tokens.pop(0), "candidate")
    assert approved and approved.state == V2State.CANDIDATE_APPROVED
    assert events == ["candidate"]

    draft = workflow.create_draft(candidate.id, exact_draft_content())
    assert live.run(candidate.id).id == draft.id
    assert events == ["candidate", "draft"]
    approved_draft_result = live.settle_callback(tokens.pop(0), "draft")
    assert approved_draft_result
    assert approved_draft_result.state == V2State.DRAFT_APPROVED
    assert events == ["candidate", "draft"]
    workflow.close()


def test_ambiguous_notification_is_never_resent_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    sends: list[int] = []
    workflow = V2Workflow(db, mode="create")
    candidate = create_candidate(workflow, observation())
    assert candidate is not None
    workflow.record_remote_attempt(candidate.id, "candidate_notification")
    workflow.close()

    with V2Workflow(db, mode="runtime") as reopened:
        live = V2LiveWorkflow(
            reopened,
            notify_candidate=lambda *_: sends.append(1),
            notify_draft=lambda *_: True,
        )
        assert live.run(candidate.id).state == V2State.MANUAL_REVIEW
        assert sends == []


def test_failed_notification_is_terminally_reconciled_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    sends: list[int] = []
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        workflow.record_remote_attempt(candidate.id, "candidate_notification")
        workflow.settle_remote_effect(
            candidate.id,
            "candidate_notification",
            "failed",
            detail="clear_pre_dispatch_network",
        )

    with V2Workflow(db, mode="runtime") as reopened:
        live = V2LiveWorkflow(
            reopened,
            notify_candidate=lambda *_: sends.append(1),
            notify_draft=lambda *_: True,
        )
        assert live.run(candidate.id).state == V2State.MANUAL_REVIEW
        assert reopened.next_candidate_pending_notification() is None
        assert sends == []


def test_failed_draft_notification_is_terminally_reconciled_after_reopen(tmp_path):
    db = tmp_path / "v2.sqlite"
    sends: list[int] = []
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, "exact draft")
        workflow.record_remote_attempt(draft.id, "draft_notification")
        workflow.settle_remote_effect(
            draft.id,
            "draft_notification",
            "failed",
            detail="clear_pre_dispatch_network",
        )

    with V2Workflow(db, mode="runtime") as reopened:
        live = V2LiveWorkflow(
            reopened,
            notify_candidate=lambda *_: True,
            notify_draft=lambda *_: sends.append(1),
        )
        assert live.run(candidate.id).state == V2State.MANUAL_REVIEW
        assert reopened.get_draft(draft.id).state == V2State.MANUAL_REVIEW
        assert reopened.next_draft_pending_notification() is None
        assert sends == []


def test_live_collection_is_bounded(monkeypatch, tmp_path):
    calls = []

    class Collector:
        def __init__(self, *_args):
            pass

        async def latest_message_id(self, handle):
            calls.append(("latest", handle, {}))
            return 10

        async def collect_ascending(self, handle, **kwargs):
            calls.append(("ascending", handle, kwargs))
            return ()

        async def collect_edit_sweep(self, handle, **kwargs):
            calls.append(("edit", handle, kwargs))
            return EditSweepPage((), None, True)

        async def close(self):
            pass

    monkeypatch.setattr(v2_cli, "TelethonCollector", Collector)
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_API_ID", "1")
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_API_HASH", "hash")
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_SESSION", "session")
    monkeypatch.setenv("NEWSBOT_V2_TELEGRAM_HANDLES", "first,second")
    with V2Workflow(tmp_path / "v2.sqlite", mode="create"):
        pass
    args = SimpleNamespace(db=tmp_path / "v2.sqlite", lookback_hours=12, limit=7)
    assert v2_cli.collect_live(args) == 0
    assert [(kind, handle) for kind, handle, _kwargs in calls] == [
        ("latest", "first"),
        ("ascending", "first"),
        ("edit", "first"),
        ("latest", "second"),
        ("ascending", "second"),
        ("edit", "second"),
    ]
    bounded = [kwargs for kind, _handle, kwargs in calls if kind in {"ascending", "edit"}]
    assert all(1 <= kwargs["limit"] <= args.limit for kwargs in bounded)
    assert max(kwargs["limit"] for kwargs in bounded) <= args.limit
    ascending = [kwargs for kind, _handle, kwargs in calls if kind == "ascending"]
    assert all(kwargs["lower_bound"].tzinfo is not None for kwargs in ascending)
    edits = [kwargs for kind, _handle, kwargs in calls if kind == "edit"]
    assert all(kwargs["before_message_id"] is None for kwargs in edits)
    assert all(kwargs["lower_bound"].tzinfo is not None for kwargs in edits)


def test_live_collection_stops_intake_when_due_backlog_reaches_limit(
    monkeypatch,
    tmp_path,
):
    calls: list[str] = []

    class Collector:
        def __init__(self, *_args):
            pass

        async def latest_message_id(self, _handle):
            calls.append("latest")
            raise AssertionError("collection must not start under backpressure")

        async def close(self):
            calls.append("close")

    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        for post_id in ("backlog-1", "backlog-2", "backlog-3"):
            workflow.record_revision(
                SourceObservation(
                    channel_id="channel",
                    channel_handle="configured",
                    external_post_id=post_id,
                    published_at=datetime.now(UTC),
                    text="material backlog",
                )
            )

    monkeypatch.setattr(v2_cli, "TelethonCollector", Collector)
    monkeypatch.setattr(
        v2_cli,
        "_drain_enrichment_queue",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_API_ID", "1")
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_API_HASH", "hash")
    monkeypatch.setenv("NEWSBOT_V2_TELETHON_SESSION", "session")
    monkeypatch.setenv(
        "NEWSBOT_V2_TELEGRAM_HANDLES",
        "first,second,third",
    )

    assert (
        v2_cli.collect_live(
            SimpleNamespace(
                db=db,
                lookback_hours=24,
                limit=3,
            )
        )
        == 0
    )
    assert calls == ["close"]


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
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
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
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
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
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
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


@pytest.mark.parametrize("phase", ["prepare", "attestation", "arm"])
def test_sheets_untyped_pre_dispatch_failure_is_terminal_across_restart(phase, tmp_path):
    db = tmp_path / f"{phase}.sqlite"

    class FailingAdapter(ScriptedSheetsAdapter):
        def prepare_delivery(self, **kwargs):
            if phase == "prepare":
                raise RuntimeError("local failure")
            return super().prepare_delivery(**kwargs)

        def dispatch_credential_attestation(self):
            if phase == "attestation":
                raise RuntimeError("credential failure")
            return super().dispatch_credential_attestation()

        def arm_prepared_dispatch(self):
            if phase == "arm":
                raise RuntimeError("arm failure")
            return super().arm_prepared_dispatch()

    with V2Workflow(db, mode="create") as workflow:
        _candidate, draft = approved_draft(workflow)
        adapter = FailingAdapter(MetadataState.ABSENT)
        result = deliver_v2_google_sheets(
            workflow, draft, adapter, v2_draft_handoff_values(draft, "2026-08-09"), lease_seconds=120
        )
        assert result.state == V2State.MANUAL_REVIEW
        receipt = workflow.remote_effect(draft.id, "sheets_delivery")
        assert receipt is not None
        assert receipt["status"] == "failed"
        assert json.loads(str(receipt["detail"]))["failure"] == "terminal_pre_dispatch_failure"
        assert not adapter.dispatched

    with V2Workflow(db, mode="runtime") as reopened:
        assert reopened.next_draft_approved_sheets_delivery() is None


def test_sheets_typed_clear_pre_dispatch_network_retries_once_without_dispatch(tmp_path):
    db = tmp_path / "clear-network.sqlite"

    class FailingAdapter(ScriptedSheetsAdapter):
        def prepare_delivery(self, **kwargs):
            self.prepared = True
            raise SheetsClearPreDispatchNetworkError()

    with V2Workflow(db, mode="create") as workflow:
        _candidate, draft = approved_draft(workflow)
        adapter = FailingAdapter(MetadataState.ABSENT)
        values = v2_draft_handoff_values(draft, "2026-08-09")
        assert (
            deliver_v2_google_sheets(workflow, draft, adapter, values, lease_seconds=120).state
            == V2State.DRAFT_APPROVED
        )
        first = workflow.remote_effect(draft.id, "sheets_delivery")
        assert first is not None and first["attempts"] == 1 and first["status"] == "failed"
        assert workflow.next_draft_approved_sheets_delivery().id == draft.id

    with V2Workflow(db, mode="runtime") as reopened:
        draft = reopened.next_draft_approved_sheets_delivery()
        assert draft is not None
        assert (
            deliver_v2_google_sheets(
                reopened,
                draft,
                FailingAdapter(MetadataState.ABSENT),
                v2_draft_handoff_values(draft, "2026-08-09"),
                lease_seconds=120,
            ).state
            == V2State.MANUAL_REVIEW
        )
        final = reopened.remote_effect(draft.id, "sheets_delivery")
        assert final is not None and final["attempts"] == 2 and final["status"] == "failed"
        assert reopened.next_draft_approved_sheets_delivery() is None


def test_sheets_clear_pre_dispatch_retry_becomes_terminal_when_bootstrap_fails_after_restart(monkeypatch, tmp_path):
    db = tmp_path / "clear-network-bootstrap.sqlite"

    class FailingAdapter(ScriptedSheetsAdapter):
        def prepare_delivery(self, **kwargs):
            self.prepared = True
            raise SheetsClearPreDispatchNetworkError()

    with V2Workflow(db, mode="create") as workflow:
        _candidate, draft = approved_draft(workflow)
        result = deliver_v2_google_sheets(
            workflow,
            draft,
            FailingAdapter(MetadataState.ABSENT),
            v2_draft_handoff_values(draft, "2026-08-09"),
            lease_seconds=120,
        )
        assert result.state == V2State.DRAFT_APPROVED
        first = workflow.remote_effect(draft.id, "sheets_delivery")
        assert first is not None and first["attempts"] == 1 and first["status"] == "failed"

    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("GOOGLE_SHEETS_SPREADSHEET_ID", raising=False)
    with V2Workflow(db, mode="runtime") as reopened:
        selected = reopened.next_draft_approved_sheets_delivery()
        assert selected is not None and selected.id == draft.id
        result = v2_cli._deliver_google_sheets_draft(reopened, selected, 120)
        assert result.state == V2State.MANUAL_REVIEW
        final = reopened.remote_effect(draft.id, "sheets_delivery")
        assert final is not None and final["attempts"] == 2 and final["status"] == "failed"
        detail = json.loads(str(final["detail"]))
        assert detail["failure"] == "terminal_pre_dispatch_failure"
        assert detail["phase"] == "bootstrap_failed"
        assert reopened.next_draft_approved_sheets_delivery() is None


@pytest.mark.parametrize(
    ("status", "detail", "attempts"),
    [
        ("ambiguous", "unknown post-dispatch outcome", 1),
        (
            "failed",
            json.dumps({"failure": "terminal_pre_dispatch_failure", "phase": "prepare_failed"}, sort_keys=True),
            1,
        ),
        ("failed", "clear_pre_dispatch_network", 2),
    ],
)
def test_terminal_sheets_receipt_reconciles_manual_after_crash_without_credentials(
    status, detail, attempts, monkeypatch, tmp_path, capsys
):
    db = tmp_path / f"terminal-{status}-{attempts}.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        _candidate, draft = approved_draft(workflow)
        for _ in range(attempts):
            workflow.record_remote_attempt(draft.id, "sheets_delivery")
            workflow.settle_remote_effect(draft.id, "sheets_delivery", status, detail=detail)

    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("GOOGLE_SHEETS_SPREADSHEET_ID", raising=False)
    with V2Workflow(db, mode="runtime") as reopened:
        selected = reopened.next_draft_approved_sheets_delivery()
        assert selected is not None
        assert v2_cli._deliver_google_sheets_draft(reopened, selected, 120).state == V2State.MANUAL_REVIEW
        receipt = reopened.remote_effect(draft.id, "sheets_delivery")
        assert receipt is not None and receipt["status"] == status and receipt["attempts"] == attempts

    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "no_work"}


def test_sheets_missing_credentials_settle_terminal_without_timer_loop(monkeypatch, tmp_path, capsys):
    db = tmp_path / "credentials.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        _candidate, draft = approved_draft(workflow)
    monkeypatch.delenv("GOOGLE_SERVICE_ACCOUNT_FILE", raising=False)
    monkeypatch.delenv("GOOGLE_SHEETS_SPREADSHEET_ID", raising=False)

    with V2Workflow(db, mode="runtime") as workflow:
        assert v2_cli._deliver_google_sheets_draft(workflow, draft, 120).state == V2State.MANUAL_REVIEW
        receipt = workflow.remote_effect(draft.id, "sheets_delivery")
        assert receipt is not None and receipt["status"] == "failed" and receipt["attempts"] == 1

    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "no_work"}


def test_generic_pending_remote_attempt_is_not_resent(tmp_path):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        workflow.record_remote_attempt(candidate.id, "candidate_notification")
        sends = []
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=lambda *_: sends.append(1),
            notify_draft=lambda *_: True,
        )
        result = live.run(candidate.id)
        assert result.state == V2State.MANUAL_REVIEW
        assert sends == []


def test_google_sheets_cli_rejects_unapproved_draft_before_credentials(tmp_path, monkeypatch):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        workflow.approve_candidate(candidate.id)
        draft = workflow.create_draft(candidate.id, exact_draft_content())
        workflow.approve_draft(draft.id)
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
        workflow.settle_remote_effect(draft.id, "sheets_delivery", "confirmed", receipt_id="sheet-row")

    with V2Workflow(db, mode="runtime") as reopened:
        result = recover_v2_google_sheets_delivery(
            reopened,
            reopened.get_draft(draft.id),
        )
        assert result.state == V2State.SHEET_DELIVERED
        assert sends == []


def test_v2_telegram_tick_authorizes_callbacks_and_advances_cursor(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    token = "C" * 43
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        claim_test_capability(
            workflow,
            candidate.id,
            "candidate",
            token,
            (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            confirmed=True,
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
    assert [method for method, _payload in calls] == ["getUpdates", "answerCallbackQuery"]
    with V2Workflow(db, mode="runtime") as workflow:
        assert workflow.get_candidate(candidate.id).state == V2State.CANDIDATE_APPROVED
        assert workflow.telegram_next_offset() == 43


def test_v2_callback_settlement_rejects_unauthorized_and_duplicates(tmp_path):
    token = "D" * 43
    with V2Workflow(tmp_path / "v2.sqlite", mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        claim_test_capability(
            workflow,
            candidate.id,
            "candidate",
            token,
            (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            confirmed=True,
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
    with V2Workflow(db, mode="create") as workflow:
        pending = create_candidate(workflow, observation())
        assert pending is not None
        approved = create_candidate(
            workflow,
            SourceObservation(
                channel_id="channel",
                channel_handle="configured",
                external_post_id="2",
                published_at=datetime.now(UTC),
                text=observation().text,
                urls=(UrlCandidate("https://example.test/source-2"),),
            ),
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
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
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
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        workflow.record_remote_attempt(candidate.id, "candidate_notification")
        approved = create_candidate(
            workflow,
            SourceObservation(
                channel_id="channel",
                channel_handle="configured",
                external_post_id="crashed-draft",
                published_at=datetime.now(UTC),
                text=observation().text,
                urls=(UrlCandidate("https://example.test/crashed-draft"),),
            ),
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
    with V2Workflow(db, mode="runtime") as workflow:
        assert workflow.get_candidate(approved.id).state == V2State.DRAFT_PENDING_APPROVAL
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW
        assert workflow.get_draft(draft.id).state == V2State.DRAFT_PENDING_APPROVAL


def test_v2_telegram_tick_reconciles_expired_approval_capability(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(workflow, observation())
        assert candidate is not None
        claim_test_capability(
            workflow,
            candidate.id,
            "candidate",
            "E" * 43,
            (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
            confirmed=False,
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
    with V2Workflow(db, mode="runtime") as workflow:
        assert workflow.get_candidate(candidate.id).state == V2State.MANUAL_REVIEW


def test_deliver_google_sheets_next_recovers_crashed_receipts_without_dispatch(tmp_path, capsys):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        _candidate, confirmed = approved_draft(workflow)
        workflow.record_remote_attempt(confirmed.id, "sheets_delivery")
        workflow.settle_remote_effect(confirmed.id, "sheets_delivery", "confirmed", receipt_id="remote-row")
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out)["state"] == V2State.SHEET_DELIVERED
    with V2Workflow(db, mode="runtime") as workflow:
        assert workflow.get_draft(confirmed.id).state == V2State.SHEET_DELIVERED

    with V2Workflow(db, mode="runtime") as workflow:
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
    with V2Workflow(db, mode="runtime") as workflow:
        assert workflow.get_draft(pending.id).state == V2State.MANUAL_REVIEW


def test_deliver_google_sheets_next_reports_no_work(tmp_path, capsys):
    with V2Workflow(tmp_path / "v2.sqlite", mode="create"):
        pass
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=tmp_path / "v2.sqlite", deadline=120.0)) == 0
    assert json.loads(capsys.readouterr().out) == {"status": "no_work"}


def test_deliver_google_sheets_next_selects_one_approved_draft(monkeypatch, tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        _candidate, draft = approved_draft(workflow)

    delivered = []

    def deliver(workflow, selected, deadline):
        delivered.append((selected.id, deadline))
        return selected

    monkeypatch.setattr(v2_cli, "_deliver_google_sheets_draft", deliver)
    assert v2_cli.deliver_google_sheets_next(SimpleNamespace(db=db, deadline=12.0)) == 0
    assert delivered == [(draft.id, 12.0)]


def test_candidate_message_uses_bound_evidence_escapes_and_utf8_bounds():
    candidate = SimpleNamespace(
        id="candidate-1",
        channel_id="channel",
        policy_outcome="AMBIGUOUS",
        policy_reason="source_unavailable",
    )
    hostile = ("<b>한국어 & markup</b>\x00" * 1_000) + "끝"
    text = render_v2_candidate_message(
        candidate,
        {
            "revision": {
                "payload": {
                    "channel_handle": "<source>",
                    "published_at": "2026-08-11T00:00:00+00:00",
                    "text": hostile,
                }
            },
            "snapshot": {
                "requested_url": "https://example.test/<unsafe>",
                "source_date_conflict": True,
                "provenance": {"result": "<success>", "redirect_count": 0},
                "duplicate": "same canonical story",
            },
        },
    )
    assert len(text.encode("utf-8")) <= 3_800
    assert "\x00" not in text
    assert "&lt;source&gt;" in text
    assert "Outcome: AMBIGUOUS" in text
    assert "Reason: source_unavailable" in text
    assert "Source date: conflict" in text
    assert "Duplicate: same canonical story" in text
    assert "Link: https://example.test/&lt;unsafe&gt;" in text
    assert 'Provenance: {"redirect_count": 0, "result": "&lt;success&gt;"}' in text
    assert "\nBody: " in text


def test_candidate_notifier_sends_one_approve_action_from_bound_evidence():
    sent = []

    class Adapter:
        def send_message_once(self, text, *, markup, deadline):
            sent.append((text, markup))
            return SimpleNamespace(accepted=True, message_id=1, safe_code=None)

    candidate = SimpleNamespace(
        id="candidate-1",
        channel_id="channel",
        policy_outcome="CANDIDATE",
        policy_reason="news",
    )
    notifier = TelegramV2Notifier(
        Adapter(),
        candidate_evidence=lambda _id: {
            "revision": {"payload": {"channel_id": "channel", "published_at": "2026-08-11T00:00:00+00:00"}},
            "snapshot": {"requested_url": "https://example.test/article"},
        },
    )
    assert notifier.candidate(candidate, "token") == "1"
    assert len(sent) == 1
    assert sent[0][1]["inline_keyboard"] == [[{"text": "Approve", "callback_data": "token"}]]


def test_candidate_notifier_rejects_accepted_result_without_message_id():
    class Adapter:
        def send_message_once(self, _text, *, markup, deadline):
            assert markup["inline_keyboard"]
            return SimpleNamespace(accepted=True, message_id=None, safe_code=None)

    candidate = SimpleNamespace(
        id="candidate-1",
        channel_id="channel",
        policy_outcome="CANDIDATE",
        policy_reason="news",
    )
    notifier = TelegramV2Notifier(
        Adapter(),
        candidate_evidence=lambda _id: {
            "revision": {"payload": {"channel_id": "channel", "published_at": "2026-08-11T00:00:00+00:00"}},
            "snapshot": {"requested_url": "https://example.test/article"},
        },
    )

    with pytest.raises(AmbiguousRemoteEffect, match="Telegram send not confirmed"):
        notifier.candidate(candidate, "token")


def test_v2_cli_status_and_operational_commands_remain_explicit():
    parser = v2_cli.build_parser()
    status = parser.parse_args(["--db", "v2.sqlite", "v2-status", "--limit", "50"])
    verify = parser.parse_args(["--db", "v2.sqlite", "verify-db"])
    compact = parser.parse_args(["--db", "v2.sqlite", "compact", "--dry-run"])
    assert status.handler is v2_cli.status and status.limit == 50
    assert verify.handler is v2_cli.verify_db
    assert compact.handler is v2_cli.compact_db and compact.dry_run is True


def test_v2_status_uses_one_bounded_keyset_page(monkeypatch, tmp_path, capsys):
    calls = []

    class Workflow:
        def __init__(self, _db, *, mode):
            assert mode == "runtime"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def status_page(self, limit, cursor, state):
            calls.append((limit, cursor, state))
            return (), "next-page"

        def status_aggregate(
            self,
            *,
            seven_day_storage_baseline_bytes=None,
        ):
            assert seven_day_storage_baseline_bytes is None
            return {"queues": {"enrichment": 1}}

    monkeypatch.setattr(v2_cli, "V2Workflow", Workflow)
    assert v2_cli.status(SimpleNamespace(db=tmp_path / "v2.sqlite", limit=50, cursor="cursor", state=None)) == 0
    assert calls == [(50, "cursor", None)]
    payload = json.loads(capsys.readouterr().out)
    assert payload["next_cursor"] == "next-page"
    assert payload["aggregate"] == {"queues": {"enrichment": 1}}


@pytest.mark.parametrize(
    ("invariants", "snapshot_hashes"),
    [
        ({"candidate_binding_mismatches": 1}, ("unchanged", "unchanged")),
        ({"candidate_binding_mismatches": 0}, ("before", "after")),
    ],
)
def test_validate_selection_reports_failure_for_invariants_or_production_hash_drift(
    monkeypatch, tmp_path, capsys, invariants, snapshot_hashes
):
    fixture = tmp_path / "selection.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "channel_id": "channel",
                    "channel_handle": "source",
                    "external_post_id": "42",
                    "published_at": "2026-08-11T08:00:00+00:00",
                    "text": "selection validation observation",
                    "urls": [],
                }
            ]
        ),
        encoding="utf-8",
    )

    class Workflow:
        def __init__(self, _db, *, mode):
            assert mode == "runtime"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def validation_counts(self):
            return {
                "observation_revisions": 0,
                "candidates": 0,
                "stories": 0,
                "remote_effects": 0,
                "callbacks": 0,
                "held_candidates": 0,
            }

        def verify_invariants(self):
            return invariants

    hashes = iter(snapshot_hashes)
    monkeypatch.setattr(v2_cli, "V2Workflow", Workflow)
    monkeypatch.setattr(v2_cli, "_backup_sqlite", lambda _source, _destination: None)
    monkeypatch.setattr(v2_cli, "_finalize_observation", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(v2_cli, "_sqlite_snapshot_hash", lambda *_args: next(hashes))

    assert v2_cli.validate_selection(SimpleNamespace(db=tmp_path / "v2.sqlite", fixture=fixture, no_send=True)) == 1
    assert json.loads(capsys.readouterr().out)["passed"] is False


def test_release_backlog_passes_reviewed_manifest_only(monkeypatch, tmp_path, capsys):
    items = [
        {
            "id": "candidate-1",
            "revision_digest": "revision",
            "snapshot_digest": "snapshot",
            "story_id": "story",
            "story_keys_digest": "keys",
        }
    ]
    digest = V2Workflow.release_manifest_digest(items)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "ids": ["candidate-1"],
                "items": items,
                "digest": digest,
            }
        ),
        encoding="utf-8",
    )
    releases = []

    class Workflow:
        release_manifest_digest = staticmethod(V2Workflow.release_manifest_digest)

        def __init__(self, _db, *, mode):
            assert mode == "runtime"

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def release_held_candidates(self, ids, digest):
            releases.append((ids, digest))
            return {"ids": [], "items": [], "digest": "empty"}

    monkeypatch.setattr(v2_cli, "V2Workflow", Workflow)
    assert v2_cli.release_backlog(SimpleNamespace(db=tmp_path / "v2.sqlite", manifest=manifest)) == 0
    assert releases == [(["candidate-1"], digest)]
    assert json.loads(capsys.readouterr().out) == {
        "ids": [],
        "items": [],
        "digest": "empty",
    }


def test_release_backlog_rejects_tampered_item_without_releasing(monkeypatch, tmp_path):
    items = [
        {
            "id": "candidate-1",
            "revision_digest": "revision",
            "snapshot_digest": "snapshot",
            "story_id": "story",
            "story_keys_digest": "keys",
        }
    ]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "ids": ["candidate-1"],
                "items": [{**items[0], "snapshot_digest": "tampered"}],
                "digest": V2Workflow.release_manifest_digest(items),
            }
        ),
        encoding="utf-8",
    )
    releases = []

    class Workflow:
        release_manifest_digest = staticmethod(V2Workflow.release_manifest_digest)

        def release_held_candidates(self, ids, digest):
            releases.append((ids, digest))

    monkeypatch.setattr(v2_cli, "V2Workflow", Workflow)

    with pytest.raises(ValueError, match="must be canonical and bound"):
        v2_cli.release_backlog(SimpleNamespace(db=tmp_path / "v2.sqlite", manifest=manifest))

    assert releases == []

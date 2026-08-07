from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot.approval.base import ApprovalAction, ApprovalStage, hash_callback_token
from newsbot.automation import AutomationAuthority, AutomationBusyError, AutomationDriftError, CutoverProposal, Frontier
from newsbot.config import load_config
from newsbot.storage import Storage


@pytest.fixture(autouse=True)
def isolated_cli_locks(monkeypatch: pytest.MonkeyPatch) -> None:
    from newsbot import automation

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    monkeypatch.setattr(automation, "automation_lock", no_lock)


def _digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def _now() -> datetime:
    return datetime(2026, 8, 2, tzinfo=UTC)


def _target(storage: Storage) -> int:
    with storage.transaction() as connection:
        cursor = connection.execute(
            "INSERT INTO sheet_target_bindings(target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint,created_at) "
            "VALUES(?,'workplace-template-v1',0,'workplace',?,?)",
            (_digest("target"), _digest("oracle"), _now().isoformat()),
        )
        connection.execute(
            "INSERT INTO sheet_bootstraps(target_binding_id,marker_value,controls_fingerprint,status,verified_at) "
            "VALUES(?, 'marker', ?, 'ready', ?)",
            (int(cursor.lastrowid), _digest("controls"), _now().isoformat()),
        )
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def _proposal(
    storage: Storage, proposal_id: str = "proposal-for-automation-tests"
) -> tuple[AutomationAuthority, int, str]:
    authority = AutomationAuthority(storage)
    config = load_config(Path("config/channels.toml"), environ={})
    apply_proposal = authority.apply_proposal

    def apply_with_config(*args: object, **kwargs: object) -> dict[str, object]:
        kwargs.setdefault("config", config)
        return apply_proposal(*args, **kwargs)  # type: ignore[arg-type]

    authority.apply_proposal = apply_with_config  # type: ignore[method-assign]
    target_id = _target(storage)
    audience_id = authority.record_audience_binding(
        bot_id_digest=_digest("bot"), token_hmac=_digest("token"), audience_hmac=_digest("audience"), version=1
    )
    now = _now()
    receipt = authority.persist_proposal(
        CutoverProposal(
            proposal_id=proposal_id,
            config_digest=_digest("config"),
            cursor_digest=_digest("cursor"),
            intervals_digest=_digest("intervals"),
            target_id=target_id,
            target_fingerprint=_digest("target"),
            release_digest=_digest("release"),
            audience_digest=_digest(str(audience_id)),
            maxima=(0, 0, 0, 0, 0),
            approval_offset=0,
            frontiers=tuple(
                Frontier(_digest(channel.id), index, now) for index, channel in enumerate(config.enabled_channels)
            ),
        ),
        now=now,
    )
    return authority, audience_id, receipt


def _candidate(storage: Storage, *, channel_id: str = "automation-test") -> int:
    with storage.transaction() as connection:
        connection.execute("INSERT INTO runs(run_key,mode,status) VALUES('automation-test','test','done')")
        connection.execute(
            "INSERT INTO source_posts(channel_id,external_post_id) VALUES(?, '1')",
            (channel_id,),
        )
        connection.execute("INSERT INTO source_post_versions(source_post_id,version_key,body) VALUES(1,'v1','body')")
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id,source_post_version_id,source_set_key,evaluator_version,score) "
            "VALUES(1,1,'source-set','test','1.000000')"
        )
        cursor = connection.execute("INSERT INTO candidates(evaluation_id,status) VALUES(1,'pending_selection')")
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def _deliver_callbacks(
    authority: AutomationAuthority,
    notification_id: int,
    callback_ids: tuple[int, ...],
) -> None:
    lease = authority.acquire_lease("telegram_dispatch", now=_now(), lease_seconds=60)
    authority.create_notification_chunks(notification_id, ((1, _digest("callback-template"), True),))
    claim = authority.claim_next_notification(lease, now=_now())
    assert claim is not None and claim.notification_id == notification_id
    chunk = authority.next_chunk(notification_id, lease, now=_now())
    assert chunk is not None
    attempt_id = authority.prepare_chunk_attempt(
        notification_id, chunk[0], _digest("callback-request"), lease, now=_now()
    )
    for callback_id in callback_ids:
        assert authority.link_callback(callback_id, notification_id, attempt_id, lease, now=_now())
    authority.mark_possibly_sent(attempt_id, lease, now=_now())
    authority.settle_attempt(attempt_id, "accepted", lease, now=_now(), accepted_message_id=7)
    assert authority.release_lease(lease, now=_now() + timedelta(seconds=1))


def test_cutover_preview_authority_captures_five_and_apply_is_exact_replay_safe() -> None:
    with Storage.open(":memory:") as storage:
        authority, audience_id, receipt = _proposal(storage)
        assert authority.safe_cutover() == {"active": False}
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM automation_proposal_frontiers")["count"] == 5

        applied = authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        replay = authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        assert applied == {"changed": True, "status": "active"}
        assert replay == {"changed": False, "status": "active"}

    with Storage.open(":memory:") as storage:
        authority, audience_id, receipt = _proposal(storage, "proposal-that-expires")
        with pytest.raises(AutomationDriftError, match="missing, expired, or for another release"):
            authority.apply_proposal(
                "proposal-that-expires",
                receipt,
                audience_binding_id=audience_id,
                release_digest=_digest("release"),
                now=_now() + timedelta(minutes=10),
                validate=lambda: True,
            )
        with pytest.raises(AutomationDriftError, match="cutover state drifted"):
            authority.apply_proposal(
                "proposal-that-expires",
                receipt,
                audience_binding_id=audience_id,
                release_digest=_digest("release"),
                now=_now(),
                validate=lambda: False,
            )


def test_authority_leases_contend_and_cleanup_durably() -> None:
    with Storage.open(":memory:") as storage:
        authority = AutomationAuthority(storage)
        lease = authority.acquire_lease("collect", now=_now(), lease_seconds=10, owner_token="first")
        with pytest.raises(AutomationBusyError, match="collect stream is leased"):
            authority.acquire_lease("collect", now=_now(), lease_seconds=10, owner_token="second")
        assert authority.release_lease(lease, now=_now() + timedelta(seconds=1), outcome="done")
        assert storage.fetch_one("SELECT 1 FROM automation_stream_leases WHERE stream='collect'") is None
        run = storage.fetch_one("SELECT outcome,finished_at FROM automation_stream_runs WHERE stream='collect'")
        assert run is not None and run["outcome"] == "done" and run["finished_at"] is not None


def test_notification_attempt_outcomes_are_durable_and_never_resend_ambiguous_work() -> None:
    with Storage.open(":memory:") as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        candidate_id = _candidate(storage)
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'source-set',?,'pending')",
                (audience_id, candidate_id, _digest("subject")),
            )
        notification = storage.fetch_one("SELECT id FROM telegram_notification_outbox")
        assert notification is not None
        notification_id = int(notification["id"])
        lease = authority.acquire_lease("telegram_dispatch", now=_now(), lease_seconds=30, owner_token="dispatch")
        assert authority.claim_next_notification(lease, now=_now()) is not None
        authority.create_notification_chunks(notification_id, ((1, _digest("one"), False), (1, _digest("two"), False)))
        first = authority.next_chunk(notification_id, lease, now=_now())
        assert first is not None
        accepted = authority.prepare_chunk_attempt(notification_id, first[0], _digest("request-1"), lease, now=_now())
        authority.mark_possibly_sent(accepted, lease, now=_now())
        authority.settle_attempt(accepted, "accepted", lease, now=_now(), accepted_message_id=7)
        second = authority.next_chunk(notification_id, lease, now=_now())
        assert second is not None and second[1] == 1
        rejected = authority.prepare_chunk_attempt(notification_id, second[0], _digest("request-2"), lease, now=_now())
        authority.settle_attempt(rejected, "trusted_rejected", lease, now=_now())
        state = storage.fetch_one("SELECT state FROM telegram_notification_outbox WHERE id=?", (notification_id,))
        assert state is not None and state["state"] == "partial_manual_required"
        assert authority.claim_next_notification(lease, now=_now()) is None
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'second-source-set',?,'pending')",
                (audience_id, candidate_id, _digest("second-subject")),
            )
        ambiguous_notification = storage.fetch_one(
            "SELECT id FROM telegram_notification_outbox WHERE source_set_key='second-source-set'"
        )
        assert ambiguous_notification is not None
        ambiguous_id = int(ambiguous_notification["id"])
        assert authority.claim_next_notification(lease, now=_now()) is not None
        authority.create_notification_chunks(ambiguous_id, ((1, _digest("ambiguous"), False),))
        chunk = authority.next_chunk(ambiguous_id, lease, now=_now())
        assert chunk is not None
        timeout = authority.prepare_chunk_attempt(ambiguous_id, chunk[0], _digest("timeout"), lease, now=_now())
        authority.mark_possibly_sent(timeout, lease, now=_now())
        authority.settle_attempt(timeout, "ambiguous", lease, now=_now())
        state = storage.fetch_one("SELECT state FROM telegram_notification_outbox WHERE id=?", (ambiguous_id,))
        assert state is not None and state["state"] == "ambiguous"
        assert authority.claim_next_notification(lease, now=_now()) is None

        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'third-source-set',?,'pending')",
                (audience_id, candidate_id, _digest("third-subject")),
            )
        rejected_notification = storage.fetch_one(
            "SELECT id FROM telegram_notification_outbox WHERE source_set_key='third-source-set'"
        )
        assert rejected_notification is not None
        rejected_id = int(rejected_notification["id"])
        assert authority.claim_next_notification(lease, now=_now()) is not None
        authority.create_notification_chunks(rejected_id, ((1, _digest("rejected"), False),))
        chunk = authority.next_chunk(rejected_id, lease, now=_now())
        assert chunk is not None
        rejected = authority.prepare_chunk_attempt(
            rejected_id, chunk[0], _digest("rejected-request"), lease, now=_now()
        )
        authority.settle_attempt(rejected, "trusted_rejected", lease, now=_now())
        state = storage.fetch_one("SELECT state FROM telegram_notification_outbox WHERE id=?", (rejected_id,))
        assert state is not None and state["state"] == "canceled"


def test_telegram_worker_rejects_audience_drift_before_cursor_and_advances_unlinked_callbacks(
    monkeypatch, tmp_path
) -> None:
    from newsbot import automation, cli
    from newsbot.approval import telegram

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    class FakeAdapter:
        def __init__(self, *_args: object) -> None:
            self.handled: list[object] = []

        def _request(self, method: str, _payload: object, **_kwargs: object) -> dict[str, object]:
            assert method == "getUpdates"
            return {
                "result": [
                    {"update_id": 2, "callback_query": {"data": _digest("expired")}},
                    {"update_id": 3, "callback_query": {"data": _digest("historical")}},
                    {"update_id": 4, "callback_query": {"data": _digest("unlinked")}},
                ]
            }

        def handle_update(self, update: object) -> None:
            self.handled.append(update)

    database = tmp_path / "cursor.sqlite"
    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_args: None)
    monkeypatch.setattr(cli, "_approval_service", lambda *_args: object())
    monkeypatch.setattr(telegram, "TelegramApprovalAdapter", FakeAdapter)
    monkeypatch.setattr(
        cli,
        "_runtime_audience",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("audience drift")),
    )
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )

    with pytest.raises(RuntimeError, match="audience drift"):
        cli.telegram_tick(SimpleNamespace(db=database, timeout=0, limit=10, deadline=1))
    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT 1 FROM telegram_update_cursors WHERE stream='approval'") is None

    monkeypatch.setattr(cli, "_runtime_audience", lambda *_args, **_kwargs: 1)
    assert cli.telegram_tick(SimpleNamespace(db=database, timeout=0, limit=10, deadline=1)) == 0
    with Storage.open(database) as storage:
        cursor = storage.fetch_one("SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'")
        assert cursor is not None and cursor["next_offset"] == 5


def test_telegram_worker_admits_raw_callback_for_sent_outbox_by_hashed_token(monkeypatch, tmp_path) -> None:
    from newsbot import automation, cli
    from newsbot.approval import telegram

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    raw_token = "AAAAAAAAAAAAAAAAAAAAAA"
    handled: list[object] = []

    class FakeAdapter:
        def __init__(self, *_args: object) -> None:
            pass

        def _request(self, method: str, _payload: object, **_kwargs: object) -> dict[str, object]:
            assert method == "getUpdates"
            return {"result": [{"update_id": 7, "callback_query": {"data": raw_token}}]}

        def handle_update(self, update: object, **_kwargs: object) -> None:
            handled.append(update)

    database = tmp_path / "callback-hash.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        candidate_id = _candidate(storage, channel_id="removed-source")
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'callback-source',?,'pending')",
                (audience_id, candidate_id, _digest("callback-subject")),
            )
            notification_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            callback = connection.execute(
                "INSERT INTO callback_tokens(token,action,payload_json) VALUES(?,'make','{}')",
                (hash_callback_token(raw_token),),
            )
            assert callback.lastrowid is not None
            callback_id = int(callback.lastrowid)
        _deliver_callbacks(authority, notification_id, (callback_id,))
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO automation_proposal_frontiers("
                "proposal_id,channel_key_digest,upper_message_id,captured_at"
                ") VALUES('proposal-for-automation-tests',?,?,?)",
                (_digest("removed-source"), 1, _now().isoformat()),
            )
        authority.activate_release(
            _digest("five-channel-release"),
            config=load_config(Path("config/channels.toml"), environ={}),
            now=_now() + timedelta(minutes=1),
            validate=lambda: True,
        )

    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_args: None)
    monkeypatch.setattr(cli, "_approval_service", lambda *_args: object())
    monkeypatch.setattr(telegram, "TelegramApprovalAdapter", FakeAdapter)
    monkeypatch.setattr(cli, "_runtime_audience", lambda *_args, **_kwargs: audience_id)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")

    assert cli.telegram_tick(SimpleNamespace(db=database, timeout=0, limit=10, deadline=1)) == 0
    assert len(handled) == 1
    with Storage.open(database) as storage:
        cursor = storage.fetch_one("SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'")
        assert cursor is not None and cursor["next_offset"] == 8


def test_post_cutover_defer_uses_poll_lease_and_resumes_under_dispatch_lease(tmp_path: Path) -> None:
    from newsbot import cli
    from newsbot.candidates import CandidateApprovalService

    database = tmp_path / "defer-worker.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        candidate_id = _candidate(storage)
        service = CandidateApprovalService(storage, chat_id=10, authorized_user_ids={20}, now=_now)
        digest = service.create_digest(1, actor_id=20)
        assert tuple(button.action for button in digest.buttons[candidate_id]) == (
            ApprovalAction.MAKE,
            ApprovalAction.REJECT,
            ApprovalAction.REFRESH,
        )
        candidate = next(value for value in digest.candidates if value["candidate_id"] == candidate_id)
        defer_token = service._button(
            digest.id,
            candidate_id,
            int(candidate["revision"]),
            tuple(candidate["source_version_ids"]),
            20,
            ApprovalStage.SELECTION,
            ApprovalAction.DEFER_6H,
            _now(),
            timedelta(hours=24),
            digest_revision=digest.revision,
        ).token
        make_token = next(
            button.token for button in digest.buttons[candidate_id] if button.action is ApprovalAction.MAKE
        )
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'defer-source',?,'pending')",
                (audience_id, candidate_id, _digest("defer-subject")),
            )
            notification_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            callback_ids = tuple(
                int(row["id"])
                for row in connection.execute(
                    "SELECT id FROM callback_tokens "
                    "WHERE CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER)=? ORDER BY id",
                    (candidate_id,),
                )
            )
        _deliver_callbacks(authority, notification_id, callback_ids)
        assert service.apply(make_token, chat_id=10, user_id=20).status == "stale"
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM decision_events")["count"] == 0
        poll = authority.acquire_lease("approval_poll", now=_now(), lease_seconds=60)
        assert service.apply(defer_token, chat_id=10, user_id=20, automation_lease=poll).status == "deferred"
        authority.release_lease(poll, now=_now(), outcome="done")

        due = _now() + timedelta(hours=6)
        dispatch = authority.acquire_lease("telegram_dispatch", now=due, lease_seconds=60)
        assert authority.resume_due_and_enqueue(dispatch, now=due) == (candidate_id,)
        assert (
            storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"]
            == "pending_selection"
        )
        resume = storage.fetch_one("SELECT id,state FROM telegram_notification_outbox WHERE notification_kind='resume'")
        assert resume is not None and resume["state"] == "pending"
        text, markup = cli._notification_payload(storage, service, int(resume["id"]), actor_id=20)
        assert text.startswith("제목:")
        assert markup is not None


def test_linked_post_cutover_callback_validity_tracks_notification_lifecycle(tmp_path: Path) -> None:
    from newsbot.candidates import CandidateApprovalService

    database = tmp_path / "lifecycle-callback.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        candidate_id = _candidate(storage)
        service = CandidateApprovalService(storage, chat_id=10, authorized_user_ids={20}, now=_now)
        digest = service.create_digest(1, actor_id=20)
        make_token = next(
            button.token for button in digest.buttons[candidate_id] if button.action is ApprovalAction.MAKE
        )
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'lifecycle-source',?,'pending')",
                (audience_id, candidate_id, _digest("lifecycle-subject")),
            )
            notification_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            callback_ids = tuple(
                int(row["id"])
                for row in connection.execute(
                    "SELECT id FROM callback_tokens "
                    "WHERE CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER)=? ORDER BY id",
                    (candidate_id,),
                )
            )
        _deliver_callbacks(authority, notification_id, callback_ids)
        late = _now() + timedelta(hours=25)
        late_service = CandidateApprovalService(storage, chat_id=10, authorized_user_ids={20}, now=lambda: late)
        poll = authority.acquire_lease("approval_poll", now=late, lease_seconds=60)
        assert (
            late_service.apply(
                make_token,
                chat_id=10,
                user_id=20,
                automation_lease=poll,
            ).status
            == "queued"
        )


def test_telegram_tick_abandons_before_send_when_callback_linking_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from newsbot import cli
    from newsbot.approval import telegram
    from newsbot.candidates import CandidateApprovalService

    database = tmp_path / "callback-link-failure.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        candidate_id = _candidate(storage)
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'link-source',?,'pending')",
                (audience_id, candidate_id, _digest("link-subject")),
            )

    sends: list[object] = []

    class FakeAdapter(telegram.TelegramApprovalAdapter):
        def _request(self, method: str, _payload: object, **_kwargs: object) -> dict[str, object]:
            assert method == "getUpdates"
            return {"result": []}

        def send_prepared_message_once(self, *_args: object, **_kwargs: object) -> object:
            sends.append("sent")
            raise AssertionError("remote send reached")

    original_link = AutomationAuthority.link_callback
    links = 0

    def partial_link(self, *args: object, **kwargs: object) -> bool:
        nonlocal links
        links += 1
        if links == 1:
            return original_link(self, *args, **kwargs)
        return False

    monkeypatch.setattr(AutomationAuthority, "link_callback", partial_link)
    monkeypatch.setattr(telegram, "TelegramApprovalAdapter", FakeAdapter)
    monkeypatch.setattr(
        cli,
        "_approval_service",
        lambda storage: CandidateApprovalService(storage, chat_id=10, authorized_user_ids={20}, now=_now),
    )
    monkeypatch.setattr(cli, "_runtime_audience", lambda *_args, **_kwargs: audience_id)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("NEWSBOT_CALLBACK_ACTOR_ID", "20")
    monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "10")
    monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "20")

    assert cli.telegram_tick(SimpleNamespace(db=database, timeout=0, limit=10, deadline=5)) == 0
    assert sends == []
    with Storage.open(database) as storage:
        attempt = storage.fetch_one("SELECT id,state FROM telegram_chunk_attempts ORDER BY id DESC LIMIT 1")
        assert attempt is not None and attempt["state"] == "abandoned_pre_marker"
        outbox = storage.fetch_one("SELECT state FROM telegram_notification_outbox ORDER BY id DESC LIMIT 1")
        assert outbox is not None and outbox["state"] == "pending"
        linked = storage.fetch_one(
            "SELECT revoked_at FROM callback_tokens WHERE chunk_attempt_id=?",
            (int(attempt["id"]),),
        )
        assert linked is not None and linked["revoked_at"] is not None


def test_active_cutover_rejects_runtime_sheets_target_drift(monkeypatch) -> None:
    from newsbot import cli
    from newsbot.config import ConfigError

    with Storage.open(":memory:") as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        monkeypatch.setenv("NEWSBOT_APPROVER_CHAT_ID", "10")
        monkeypatch.setenv("NEWSBOT_APPROVER_USER_IDS", "20")
        monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "different-target")
        with pytest.raises(ConfigError, match="active automation cutover"):
            cli._approval_service(storage)


def test_sheets_worker_recovers_target_before_finding_no_post_baseline_handoffs(monkeypatch, tmp_path) -> None:
    from newsbot import automation, cli, handoffs

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    database = tmp_path / "sheets.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )

    recovered: list[int] = []

    class RecoveryProbe:
        def __init__(self, _storage: Storage) -> None:
            pass

        def recover_expired_pre_marker(self, target_id: int, _now: str) -> None:
            recovered.append(target_id)

    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.setattr(handoffs, "SheetHandoffService", RecoveryProbe)
    assert cli.sheets_deliver_pending_once(SimpleNamespace(db=database)) == 0
    assert recovered == [1]


def test_sheets_preflight_deadline_stops_before_any_mutation(monkeypatch) -> None:
    from newsbot.sheets.google import GoogleSheetsAdapter, GoogleSheetsDeadlineExceeded

    clock = [0.0]

    class StalledReadService:
        def get_document(self, _spreadsheet_id: str) -> dict[str, object]:
            clock[0] = 2.0
            return {}

    monkeypatch.setattr("newsbot.sheets.google.time.monotonic", lambda: clock[0])
    adapter = GoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=StalledReadService(),
        deadline_monotonic=1.0,
    )

    with pytest.raises(GoogleSheetsDeadlineExceeded, match="worker deadline"):
        adapter.prepare_delivery(
            export_id="exp_" + "a" * 32,
            canonical_sha256="b" * 64,
            values=("2026-08-02",),
        )


def test_sheets_hard_read_deadline_returns_before_a_still_running_read() -> None:
    from newsbot.sheets.google import GoogleSheetsAdapter, GoogleSheetsDeadlineExceeded

    release = threading.Event()

    class BlockingReadService:
        def get_document(self, _spreadsheet_id: str) -> dict[str, object]:
            release.wait(1)
            return {}

    started = time.monotonic()
    adapter = GoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=BlockingReadService(),
        deadline_monotonic=started + 0.02,
    )
    try:
        with pytest.raises(GoogleSheetsDeadlineExceeded, match="worker deadline"):
            adapter.prepare_delivery(
                export_id="exp_" + "a" * 32,
                canonical_sha256="b" * 64,
                values=("value",),
            )
        assert time.monotonic() - started < 0.5
    finally:
        release.set()


def test_sheets_post_marker_deadline_is_ambiguous_without_a_second_mutation(monkeypatch) -> None:
    from newsbot.sheets.base import DeliveryOutcome, PreparedSheetMutation
    from newsbot.sheets.google import GoogleSheetsAdapter

    clock = [0.0]
    mutations: list[object] = []

    class PostMarkerStallService:
        def batch_update(self, _spreadsheet_id: str, body: object) -> None:
            mutations.append(body)
            clock[0] = 2.0

    monkeypatch.setattr("newsbot.sheets.google.time.monotonic", lambda: clock[0])
    adapter = GoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=PostMarkerStallService(),
        deadline_monotonic=1.0,
    )
    adapter.arm_prepared_dispatch()

    result = adapter.dispatch_prepared(
        PreparedSheetMutation(body={"requests": [{}]}, request_sha256="a" * 64, metadata_value="marker")
    )

    assert result.outcome is DeliveryOutcome.AMBIGUOUS
    assert len(mutations) == 1


def test_sheets_worker_reports_stalled_preflight_deadline(monkeypatch, tmp_path, capsys) -> None:
    from newsbot import automation, cli
    from newsbot.sheets.google import GoogleSheetsDeadlineExceeded

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    database = tmp_path / "sheets-deadline.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )

    dispatched: list[object] = []
    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.setattr(AutomationAuthority, "post_baseline_handoff_ids", lambda *_args: (1,))

    def stalled_preflight(_args: object) -> int:
        dispatched.append("preflight")
        raise GoogleSheetsDeadlineExceeded("Sheets worker deadline exceeded")

    monkeypatch.setattr(cli, "_sheets_deliver_unlocked", stalled_preflight)
    assert cli.sheets_deliver_pending_once(SimpleNamespace(db=database, deadline=1)) == 0
    assert dispatched == ["preflight"]
    assert json.loads(capsys.readouterr().out)["status"] == "deadline_exhausted"


def test_legacy_collection_is_disabled_after_cutover(monkeypatch, tmp_path: Path) -> None:
    from newsbot import cli

    database = tmp_path / "legacy-disabled.sqlite"
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )

    monkeypatch.setattr(
        cli,
        "_collect_live",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("legacy collector reached")),
    )
    with pytest.raises(RuntimeError, match="legacy command is disabled"):
        cli.collect_live(SimpleNamespace(db=database))


def test_fixture_and_bootstrap_mutators_refuse_active_cutover_before_effects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from newsbot import cli

    database = tmp_path / "legacy-mutators.sqlite"
    fixture = tmp_path / "fixture.json"
    fixture.write_text("{}", encoding="utf-8")
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        before = int(storage.fetch_one("SELECT COUNT(*) AS count FROM runs")["count"])

    config = SimpleNamespace(database_path=database, digest=_digest("config"), enabled_channels=())
    monkeypatch.setattr(cli, "_config", lambda _args: config)
    monkeypatch.setattr(
        cli,
        "_live_sheets",
        lambda _args: (_ for _ in ()).throw(AssertionError("Sheets provider reached")),
    )

    with pytest.raises(RuntimeError, match="fixture command is disabled"):
        cli.run_fixture(SimpleNamespace(fixture=fixture, scripted_approve=False))
    with pytest.raises(RuntimeError, match="Sheets bootstrap is disabled"):
        cli.sheets_bootstrap(SimpleNamespace())

    with Storage.open(database) as storage:
        assert int(storage.fetch_one("SELECT COUNT(*) AS count FROM runs")["count"]) == before


def test_notification_payload_uses_attested_actor_in_multi_user_audience(monkeypatch) -> None:
    from newsbot import cli
    from newsbot.candidates import CandidateApprovalService

    monkeypatch.setenv("NEWSBOT_CALLBACK_ACTOR_ID", "7")
    with Storage.open(":memory:") as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        candidate_id = _candidate(storage)
        service = CandidateApprovalService(storage, chat_id=10, authorized_user_ids={3, 7}, now=_now)
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO telegram_notification_outbox("
                "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
                ") VALUES(?,1,'candidate',?,'actor-source',?,'pending')",
                (audience_id, candidate_id, _digest("actor-subject")),
            )
            notification_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

        _text, markup = cli._notification_payload(
            storage,
            service,
            notification_id,
            actor_id=cli._callback_actor_id(service.authorized_user_ids),
        )
        assert markup is not None
        raw_token = str(markup["inline_keyboard"][0][0]["callback_data"])
        token = storage.fetch_one(
            "SELECT payload_json FROM callback_tokens WHERE token=?",
            (hash_callback_token(raw_token),),
        )
        assert token is not None
        assert json.loads(str(token["payload_json"]))["actor_id"] == 7


def test_automated_collection_rejects_partial_failure_before_ranking(monkeypatch, tmp_path) -> None:
    from newsbot import cli

    channels = (SimpleNamespace(id="first"), SimpleNamespace(id="second"))
    database = tmp_path / "partial-failure.sqlite"
    pipeline_constructed = False

    class FakeSessionStore:
        def __init__(self, _path: str) -> None:
            pass

        def validate(self) -> Path:
            return tmp_path / "session"

    class FakeCollection:
        def __init__(self, _storage: Storage) -> None:
            self.calls = 0

        def collect_channel(self, *_args: object, **_kwargs: object) -> SimpleNamespace:
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("channel unavailable")
            return SimpleNamespace(persisted=1)

    class ForbiddenPipeline:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            nonlocal pipeline_constructed
            pipeline_constructed = True

    monkeypatch.setattr(cli, "validate_capabilities", lambda *_args: None)
    monkeypatch.setattr(
        cli,
        "_config",
        lambda _args: SimpleNamespace(enabled_channels=channels, database_path=database),
    )
    monkeypatch.setattr(cli, "SessionStore", FakeSessionStore)
    monkeypatch.setattr(cli, "_live_collector", lambda *_args, **_kwargs: (object(), lambda: None))
    monkeypatch.setattr(cli, "DurableCollection", FakeCollection)
    monkeypatch.setattr(cli, "DurableLivePipeline", ForbiddenPipeline)
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))

    with pytest.raises(RuntimeError, match="automated collection failed for channels: second"):
        cli._collect_live(
            SimpleNamespace(
                page_size=1,
                max_pages=1,
                lookback_hours=24,
                fail_on_channel_error=True,
            ),
            reconcile=False,
        )

    assert pipeline_constructed is False


def test_five_channel_collection_preserves_removed_cursor_without_new_root(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from newsbot import cli

    enabled = tuple(SimpleNamespace(id=f"channel-{index}", enabled=True) for index in range(5))
    database = tmp_path / "five-channel.sqlite"
    collected: list[str] = []
    pipeline_observations: list[object] = []

    with Storage.open(database) as storage, storage.transaction() as connection:
        connection.execute(
            "INSERT INTO collection_cursors(channel_id,published_at,external_post_id) VALUES(?,?,?)",
            ("removed-source", _now().isoformat(), "99"),
        )

    class FakeSessionStore:
        def __init__(self, _path: str) -> None:
            pass

        def validate(self) -> Path:
            return tmp_path / "session"

    class FakeCollection:
        def __init__(self, _storage: Storage) -> None:
            pass

        def collect_channel(self, _collector: object, channel: object, **_kwargs: object) -> SimpleNamespace:
            collected.append(str(channel.id))
            return SimpleNamespace(persisted=0)

    class FakePipeline:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def run(self, observations: tuple[object, ...], **_kwargs: object) -> SimpleNamespace:
            pipeline_observations.extend(observations)
            return SimpleNamespace(selection_digest=None, routed_counts={}, run_id=1)

    config = SimpleNamespace(
        enabled_channels=enabled,
        channels=enabled,
        database_path=database,
        digest=_digest("five-channel-config"),
    )
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_args: None)
    monkeypatch.setattr(cli, "_config", lambda _args: config)
    monkeypatch.setattr(cli, "SessionStore", FakeSessionStore)
    monkeypatch.setattr(cli, "_live_collector", lambda *_args, **_kwargs: (object(), lambda: None))
    monkeypatch.setattr(cli, "DurableCollection", FakeCollection)
    monkeypatch.setattr(cli, "DurableLivePipeline", FakePipeline)
    monkeypatch.setenv("TELEGRAM_SESSION_PATH", str(tmp_path / "session"))

    assert (
        cli._collect_live(
            SimpleNamespace(
                page_size=1,
                max_pages=1,
                lookback_hours=24,
                fail_on_channel_error=True,
                deadline=60,
            ),
            reconcile=False,
        )
        == 0
    )

    assert collected == [channel.id for channel in enabled]
    assert pipeline_observations == []
    with Storage.open(database) as storage:
        cursor = storage.fetch_one(
            "SELECT published_at,external_post_id FROM collection_cursors WHERE channel_id='removed-source'"
        )
        assert cursor is not None
        assert cursor["external_post_id"] == "99"
        assert storage.fetch_one("SELECT 1 FROM source_posts WHERE channel_id='removed-source'") is None


def test_production_cutover_baseline_is_exact_and_rejects_drift() -> None:
    from newsbot import cli

    class BaselineStorage:
        page_count = 4

        def fetch_one(
            self,
            query: str,
            _parameters: object = (),
        ) -> dict[str, object]:
            if "FROM candidates candidate" in query:
                return {
                    "candidate_id": 12,
                    "generation_id": 1,
                    "generation_status": "current",
                    "page_count": self.page_count,
                    "handoff_id": 1,
                    "handoff_status": "delivered",
                }
            if "telegram_update_cursors" in query:
                return {"next_offset": 1}
            if "sheet_remote_operations" in query:
                return {"count": 1}
            raise AssertionError(query)

    storage = BaselineStorage()
    cli._require_production_cutover_baseline(storage)  # type: ignore[arg-type]

    storage.page_count = 3
    with pytest.raises(RuntimeError, match="production cutover baseline"):
        cli._require_production_cutover_baseline(storage)  # type: ignore[arg-type]


def test_material_edit_eligibility_uses_observation_timestamps() -> None:
    from newsbot.pipeline import NewsPipeline

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE source_posts (
            id INTEGER PRIMARY KEY, channel_id TEXT, external_post_id TEXT
        );
        CREATE TABLE source_post_versions (
            id INTEGER PRIMARY KEY, source_post_id INTEGER, observed_at TEXT, edited_at TEXT
        );
        CREATE TABLE source_post_observations (
            source_post_version_id INTEGER, observed_at TEXT, edited_at TEXT
        );
        CREATE TABLE automation_cutovers (
            id INTEGER PRIMARY KEY, proposal_id TEXT, activated_at TEXT
        );
        CREATE TABLE automation_proposal_frontiers (
            proposal_id TEXT, channel_key_digest TEXT, upper_message_id INTEGER
        );
        """
    )
    activated = "2026-08-02T00:00:00+00:00"
    connection.execute(
        "INSERT INTO automation_cutovers(id,proposal_id,activated_at) VALUES(1,'p',?)",
        (activated,),
    )
    connection.execute(
        "INSERT INTO automation_proposal_frontiers VALUES('p',?,10)",
        (_digest("channel"),),
    )
    connection.execute("INSERT INTO source_posts VALUES(1,'channel','5')")
    connection.execute(
        "INSERT INTO source_post_versions VALUES(1,1,'2026-08-03T00:00:00+00:00','2026-08-03T00:00:00+00:00')"
    )
    connection.execute(
        "INSERT INTO source_post_observations VALUES(1,'2026-08-01T00:00:00+00:00','2026-08-01T00:00:00+00:00')"
    )

    assert NewsPipeline._post_frontier_material(connection, (1,)) is False

    connection.execute("DELETE FROM source_post_observations")
    connection.execute(
        "INSERT INTO source_post_observations VALUES(1,'2026-08-03T00:00:00+00:00','2026-08-03T00:00:00+00:00')"
    )

    assert NewsPipeline._post_frontier_material(connection, (1,)) is True


def test_worker_no_work_handlers_are_redacted_and_do_not_construct_a_provider(monkeypatch, tmp_path, capsys) -> None:
    from newsbot import automation, cli
    from newsbot.approval import telegram

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    class FakeAdapter:
        def __init__(self, *_args: object) -> None:
            pass

        def _request(self, method: str, _payload: object, **_kwargs: object) -> dict[str, object]:
            assert method == "getUpdates"
            return {"result": []}

    def no_work_collect(*_args: object, **_kwargs: object) -> int:
        cli._print({"status": "no_work"})
        return 0

    database = tmp_path / "workers.sqlite"
    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_args: None)
    monkeypatch.setattr(cli, "_runtime_audience", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(cli, "_approval_service", lambda *_args: object())
    monkeypatch.setattr(cli, "_collect_live", no_work_collect)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(telegram, "TelegramApprovalAdapter", FakeAdapter)
    monkeypatch.setattr(cli, "_print", lambda value: print(value))
    from newsbot.config import load_config

    base_config = load_config(Path("config/channels.toml"), environ={})
    runtime_config = SimpleNamespace(
        digest=base_config.digest,
        enabled_channels=base_config.enabled_channels,
        database_path=database,
        news_policy=base_config.news_policy,
    )
    monkeypatch.setattr(cli, "_config", lambda _args: runtime_config)
    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )
        authority.activate_release(
            _digest("release"),
            config=runtime_config,
            now=_now() + timedelta(seconds=1),
            validate=lambda: True,
        )

    assert (
        cli.automation_collect_once(SimpleNamespace(db=database, config=Path("unused"), page_size=1, max_pages=1)) == 0
    )
    assert cli.telegram_tick(SimpleNamespace(db=database, timeout=0, limit=1, deadline=1)) == 0
    assert cli.sheets_deliver_pending_once(SimpleNamespace(db=database)) == 0
    output = capsys.readouterr().out
    assert "no_work" in output
    assert str(database) not in output
    assert "token" not in output.lower()


def test_telethon_stops_retrying_when_transport_timeout_exhausts_deadline(monkeypatch) -> None:
    from newsbot.collectors import telethon

    clock = [0.0]
    attempts = 0
    sleeps: list[float] = []

    async def fail() -> None:
        nonlocal attempts
        attempts += 1
        clock[0] = 10.0
        raise TimeoutError("transport timeout")

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(telethon.time, "monotonic", lambda: clock[0])
    collector = telethon.TelethonCollector(1, "hash", "session", sleeper=sleeper, deadline_at=10.0)

    with pytest.raises(TimeoutError, match="collection application deadline exhausted"):
        asyncio.run(collector._with_retry(fail))

    assert attempts == 1
    assert sleeps == []


def test_telethon_caps_flood_wait_to_remaining_deadline(monkeypatch) -> None:
    from newsbot.collectors import telethon

    class FloodWaitError(Exception):
        seconds = 60

    clock = [0.0]
    sleeps: list[float] = []

    async def fail() -> None:
        raise FloodWaitError()

    async def sleeper(delay: float) -> None:
        sleeps.append(delay)
        clock[0] = 10.0

    monkeypatch.setattr(telethon.time, "monotonic", lambda: clock[0])
    collector = telethon.TelethonCollector(
        1, "hash", "session", sleeper=sleeper, max_flood_wait_seconds=60, deadline_at=10.0
    )

    with pytest.raises(TimeoutError, match="collection application deadline exhausted"):
        asyncio.run(collector._with_retry(fail))

    assert sleeps == [10.0]


def test_automated_collection_requires_matching_active_cutover(monkeypatch, tmp_path: Path) -> None:
    from newsbot import automation, cli

    @contextmanager
    def no_lock(*_args: object, **_kwargs: object):
        yield

    database = tmp_path / "collect-cutover.sqlite"
    channels = tuple(SimpleNamespace(id=f"channel-{index}") for index in range(5))
    bound_config = SimpleNamespace(digest=_digest("config"), enabled_channels=channels, database_path=database)
    monkeypatch.setattr(automation, "automation_lock", no_lock)
    monkeypatch.setattr(cli, "validate_capabilities", lambda *_args: None)
    monkeypatch.setattr(cli, "_config", lambda _args: bound_config)
    monkeypatch.setattr(
        cli,
        "_collect_live",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("collector reached")),
    )
    args = SimpleNamespace(db=database, config=Path("unused"), page_size=1, max_pages=1)

    with pytest.raises(RuntimeError, match="requires an active cutover"):
        cli.automation_collect_once(args)

    with Storage.open(database) as storage:
        authority, audience_id, receipt = _proposal(storage)
        authority.apply_proposal(
            "proposal-for-automation-tests",
            receipt,
            audience_binding_id=audience_id,
            release_digest=_digest("release"),
            now=_now(),
            validate=lambda: True,
        )

    drifted_config = SimpleNamespace(
        digest=_digest("different-config"), enabled_channels=channels, database_path=database
    )
    monkeypatch.setattr(cli, "_config", lambda _args: drifted_config)
    with pytest.raises(RuntimeError, match="configuration drifted"):
        cli.automation_collect_once(args)

    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT 1 FROM automation_stream_leases WHERE stream='collect'") is None

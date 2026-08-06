from __future__ import annotations

import io
import json
import sqlite3
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from newsbot.approval import telegram
from newsbot.automation import AutomationAuthority


def _rate_limit(seconds: object) -> HTTPError:
    body = json.dumps({"ok": False, "parameters": {"retry_after": seconds}}).encode()
    return HTTPError("https://api.telegram.invalid", 429, "Too Many Requests", {}, io.BytesIO(body))


def test_short_multiline_message_is_not_split() -> None:
    assert telegram.split_telegram_text("제목: 뉴스\n출처: https://t.me/source/1") == (
        "제목: 뉴스\n출처: https://t.me/source/1",
    )


def test_long_poll_transport_timeout_includes_network_margin() -> None:
    assert telegram._transport_timeout("sendMessage", {"timeout": "50"}) == 20
    assert telegram._transport_timeout("getUpdates", {"timeout": "0"}) == 20
    assert telegram._transport_timeout("getUpdates", {"timeout": "20"}) == 30
    assert telegram._transport_timeout("getUpdates", {"timeout": "50"}) == 60
    assert telegram._transport_timeout("getUpdates", {"timeout": "invalid"}) == 20


def test_send_message_does_not_retry_rate_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def open_once(*_: object, **__: object) -> object:
        nonlocal calls
        calls += 1
        raise _rate_limit(2)

    monkeypatch.setattr(telegram, "_open_no_redirect", open_once)
    adapter = telegram.TelegramApprovalAdapter("token", SimpleNamespace(chat_id=-1001))

    with pytest.raises(HTTPError) as raised:
        adapter._request("sendMessage", {"chat_id": "-1001", "text": "candidate"})
    assert raised.value.code == 429
    assert calls == 1


def test_candidate_notification_contains_only_title_source_and_buttons() -> None:
    sent: list[tuple[str, object]] = []

    class Adapter(telegram.TelegramApprovalAdapter):
        def _request(self, method: str, payload: object) -> dict[str, object]:
            sent.append((method, payload))
            return {"ok": True}

    service = SimpleNamespace(chat_id=-1001)
    digest = SimpleNamespace(
        candidates=(
            {
                "candidate_id": 5,
                "title": "Anthropic 보안 평가 사고",
                "source_url": "https://t.me/news_publisher/7687",
                "score": "0.754928",
                "rationale": {"large": "internal metadata"},
            },
        ),
        buttons={5: (SimpleNamespace(label="[제작]", token="token"),)},
    )

    Adapter("token", service).send_candidate_digest(digest)

    assert len(sent) == 1
    method, payload = sent[0]
    assert method == "sendMessage"
    assert payload["text"] == "제목: Anthropic 보안 평가 사고\n출처: https://t.me/news_publisher/7687"
    assert "reply_markup" in payload
    assert "점수" not in payload["text"]
    assert "근거" not in payload["text"]
    assert "internal metadata" not in payload["text"]


def test_review_draft_formats_structured_copy_for_humans() -> None:
    draft = {
        "cover": {"title": "AI 증시 랠리", "subtitle": "BTC는 제한적 상승"},
        "bodies": [
            {
                "subtitle": "자금은 반도체로 향했습니다",
                "body": "비트코인으로 자금이 확산되지 않았다는 분석입니다.",
            }
        ],
        "caption": {
            "hook": "증시는 올랐지만 BTC 반등은 제한됐습니다.",
            "context": "특정 업종 중심의 랠리였습니다.",
            "details": "ETF 유입도 둔화됐습니다.",
            "implications": "새 촉매가 필요합니다.",
            "questions": "다음 촉매는 무엇일까요?",
            "hashtags": ["#비트코인", "#ETF"],
        },
        "category": "Blockchain",
        "claim_manifest": [
            {
                "claim_id": "internal-claim-id",
                "evidence": "long internal evidence",
                "source_url": "https://t.me/news_publisher/123",
            }
        ],
        "draft": True,
        "source_reported": True,
    }

    rendered = telegram.format_review_draft(json.dumps(draft, ensure_ascii=False))

    assert "[표지]\n제목: AI 증시 랠리\n부제: BTC는 제한적 상승" in rendered
    assert "[본문 1]\n소제목: 자금은 반도체로 향했습니다" in rendered
    assert "[캡션]\n훅: 증시는 올랐지만 BTC 반등은 제한됐습니다." in rendered
    assert "해시태그: #비트코인 #ETF" in rendered
    assert "[출처]\n- https://t.me/news_publisher/123" in rendered
    assert "검토 상태: 초안 / 출처 기반" in rendered
    assert "internal-claim-id" not in rendered
    assert "long internal evidence" not in rendered
    assert '"cover"' not in rendered


def test_review_draft_preserves_unstructured_legacy_text() -> None:
    assert telegram.format_review_draft("legacy draft") == "legacy draft"


def test_prepared_payload_is_the_exact_object_sent_once() -> None:
    seen: list[object] = []

    class Adapter(telegram.TelegramApprovalAdapter):
        def _request(
            self,
            method: str,
            payload: object,
            **kwargs: object,
        ) -> dict[str, object]:
            assert method == "sendMessage"
            assert "follow_redirects" not in kwargs
            assert kwargs["pace"] is False
            seen.append(payload)
            return {"ok": True, "result": {"message_id": 9}}

    adapter = Adapter("token", SimpleNamespace(chat_id=-1001))
    payload = adapter.prepare_message_payload(
        "제목",
        markup={"inline_keyboard": [[{"text": "[제작]", "callback_data": "raw-token"}]]},
    )

    result = adapter.send_prepared_message_once(payload, deadline=telegram.TelegramDeadline.after(5))

    assert result.accepted is True
    assert seen == [payload]
    assert seen[0] is payload


def test_prepared_rate_limit_makes_one_http_call_per_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def urlopen(*_: object, **__: object) -> object:
        nonlocal calls
        calls += 1
        raise _rate_limit(1)

    monkeypatch.setattr(telegram, "_open_no_redirect", urlopen)
    adapter = telegram.TelegramApprovalAdapter("token", SimpleNamespace(chat_id=-1001))

    result = adapter.send_prepared_message_once(
        {"chat_id": "-1001", "text": "candidate"}, deadline=telegram.TelegramDeadline.after(5)
    )

    assert result == telegram.TelegramRequestResult(False, safe_code="rate_limited")
    assert calls == 1


def test_prepared_redirect_is_ambiguous_without_following_location(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    redirected = HTTPError(
        "https://api.telegram.invalid",
        302,
        "Found",
        {"Location": "https://other.invalid"},
        io.BytesIO(b""),
    )

    def open_once(*_: object, **__: object) -> object:
        nonlocal calls
        calls += 1
        raise redirected

    monkeypatch.setattr(telegram, "_open_no_redirect", open_once)
    adapter = telegram.TelegramApprovalAdapter("token", SimpleNamespace(chat_id=-1001))

    result = adapter.send_prepared_message_once(
        {"chat_id": "-1001", "text": "candidate"},
        deadline=telegram.TelegramDeadline.after(5),
    )

    assert result == telegram.TelegramRequestResult(False, safe_code="transport_ambiguous")
    assert calls == 1


def test_prepared_http_408_is_ambiguous_without_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def open_once(*_: object, **__: object) -> object:
        nonlocal calls
        calls += 1
        raise HTTPError(
            "https://api.telegram.invalid",
            408,
            "Request Timeout",
            {},
            io.BytesIO(b""),
        )

    monkeypatch.setattr(telegram, "_open_no_redirect", open_once)
    adapter = telegram.TelegramApprovalAdapter("token", SimpleNamespace(chat_id=-1001))

    result = adapter.send_prepared_message_once(
        {"chat_id": "-1001", "text": "candidate"},
        deadline=telegram.TelegramDeadline.after(5),
    )

    assert result == telegram.TelegramRequestResult(False, safe_code="transport_ambiguous")
    assert calls == 1


def test_expired_prepared_attempt_is_abandoned_before_takeover() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE telegram_notification_outbox (
            id INTEGER PRIMARY KEY, state TEXT, claimed_at TEXT, terminal_at TEXT
        );
        CREATE TABLE telegram_notification_chunks (id INTEGER PRIMARY KEY, notification_id INTEGER);
        CREATE TABLE telegram_chunk_attempts (
            id INTEGER PRIMARY KEY, chunk_id INTEGER, owner_hash TEXT, fence INTEGER,
            state TEXT, settled_at TEXT
        );
        CREATE TABLE callback_tokens (
            id INTEGER PRIMARY KEY, chunk_attempt_id INTEGER, consumed_at TEXT, revoked_at TEXT
        );
        CREATE TABLE telegram_chunk_attempt_events (
            chunk_attempt_id INTEGER, event_kind TEXT, created_at TEXT
        );
        CREATE TABLE telegram_notification_events (
            notification_id INTEGER, chunk_attempt_id INTEGER, event_kind TEXT, created_at TEXT
        );
        """
    )
    connection.execute("INSERT INTO telegram_notification_outbox(id,state) VALUES(1,'sending')")
    connection.execute("INSERT INTO telegram_notification_chunks(id,notification_id) VALUES(1,1)")
    connection.execute(
        "INSERT INTO telegram_chunk_attempts(id,chunk_id,owner_hash,fence,state) VALUES(1,1,'expired',1,'prepared')"
    )
    connection.execute("INSERT INTO callback_tokens(id,chunk_attempt_id) VALUES(1,1)")
    now = datetime(2026, 8, 2, tzinfo=UTC)

    AutomationAuthority._recover_expired_prepared_attempts(connection, owner_hash="expired", fence=1, now=now)

    assert connection.execute("SELECT state FROM telegram_chunk_attempts WHERE id=1").fetchone()["state"] == (
        "abandoned_pre_marker"
    )
    assert (
        connection.execute("SELECT revoked_at FROM callback_tokens WHERE id=1").fetchone()["revoked_at"]
        == now.isoformat()
    )
    assert (
        connection.execute("SELECT state FROM telegram_notification_outbox WHERE id=1").fetchone()["state"] == "pending"
    )
    assert (
        connection.execute("SELECT event_kind FROM telegram_chunk_attempt_events WHERE chunk_attempt_id=1").fetchone()[
            "event_kind"
        ]
        == "abandoned_pre_marker"
    )


def test_expired_prepared_attempt_after_accepted_prefix_requires_manual_resolution() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE telegram_notification_outbox (
            id INTEGER PRIMARY KEY, state TEXT, claimed_at TEXT, terminal_at TEXT
        );
        CREATE TABLE telegram_notification_chunks (id INTEGER PRIMARY KEY, notification_id INTEGER);
        CREATE TABLE telegram_chunk_attempts (
            id INTEGER PRIMARY KEY, chunk_id INTEGER, owner_hash TEXT, fence INTEGER,
            state TEXT, settled_at TEXT
        );
        CREATE TABLE callback_tokens (
            id INTEGER PRIMARY KEY, chunk_attempt_id INTEGER, consumed_at TEXT, revoked_at TEXT
        );
        CREATE TABLE telegram_chunk_attempt_events (
            chunk_attempt_id INTEGER, event_kind TEXT, created_at TEXT
        );
        CREATE TABLE telegram_notification_events (
            notification_id INTEGER, chunk_attempt_id INTEGER, event_kind TEXT, created_at TEXT
        );
        """
    )
    connection.execute("INSERT INTO telegram_notification_outbox(id,state) VALUES(1,'sending')")
    connection.executemany(
        "INSERT INTO telegram_notification_chunks(id,notification_id) VALUES(?,1)",
        ((1,), (2,)),
    )
    connection.execute(
        "INSERT INTO telegram_chunk_attempts(id,chunk_id,owner_hash,fence,state) VALUES(1,1,'prior',1,'accepted')"
    )
    connection.execute(
        "INSERT INTO telegram_chunk_attempts(id,chunk_id,owner_hash,fence,state) VALUES(2,2,'expired',2,'prepared')"
    )
    connection.execute("INSERT INTO callback_tokens(id,chunk_attempt_id) VALUES(1,2)")
    now = datetime(2026, 8, 2, tzinfo=UTC)

    AutomationAuthority._recover_expired_prepared_attempts(
        connection,
        owner_hash="expired",
        fence=2,
        now=now,
    )

    notification = connection.execute(
        "SELECT state,terminal_at FROM telegram_notification_outbox WHERE id=1"
    ).fetchone()
    assert dict(notification) == {
        "state": "partial_manual_required",
        "terminal_at": now.isoformat(),
    }
    assert (
        connection.execute("SELECT event_kind FROM telegram_notification_events WHERE notification_id=1").fetchone()[
            "event_kind"
        ]
        == "partial_manual_required"
    )


def test_expired_callback_ack_does_not_hide_durable_approval() -> None:
    applied: list[str] = []

    class Service:
        chat_id = -1001

        def apply(self, token: str, *, chat_id: int, user_id: int) -> SimpleNamespace:
            applied.append(token)
            return SimpleNamespace(status="approved")

    class Adapter(telegram.TelegramApprovalAdapter):
        errors = [
            TimeoutError("Telegram request deadline exhausted"),
            HTTPError("https://api.telegram.invalid", 400, "Bad Request", {}, io.BytesIO(b"query is too old")),
        ]

        def _request(self, method: str, payload: object, **_kwargs: object) -> dict[str, object]:
            assert method == "answerCallbackQuery"
            raise self.errors.pop(0)

    update = {
        "callback_query": {
            "id": "expired",
            "data": "durable-token",
            "from": {"id": 42},
            "message": {"chat": {"id": -1001}},
        }
    }

    adapter = Adapter("token", Service())
    assert adapter.handle_update(update, deadline=telegram.TelegramDeadline.after(1)) == "approved"
    assert adapter.handle_update(update, deadline=telegram.TelegramDeadline.after(1)) == "approved"
    assert applied == ["durable-token", "durable-token"]

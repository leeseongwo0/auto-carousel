from __future__ import annotations

import io
import json
from types import SimpleNamespace
from urllib.error import HTTPError

import pytest

from newsbot.approval import telegram


class _Response:
    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"ok":true,"result":{}}'


def _rate_limit(seconds: object) -> HTTPError:
    body = json.dumps({"ok": False, "parameters": {"retry_after": seconds}}).encode()
    return HTTPError("https://api.telegram.invalid", 429, "Too Many Requests", {}, io.BytesIO(body))


def test_short_multiline_message_is_not_split() -> None:
    assert telegram.split_telegram_text("제목: 뉴스\n출처: https://t.me/source/1") == (
        "제목: 뉴스\n출처: https://t.me/source/1",
    )


def test_send_message_honors_retry_after_then_applies_group_pacing(monkeypatch: pytest.MonkeyPatch) -> None:
    responses: list[object] = [_rate_limit(2), _Response()]
    sleeps: list[float] = []

    def urlopen(*_: object, **__: object) -> object:
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(telegram, "urlopen", urlopen)
    monkeypatch.setattr(telegram.time, "sleep", sleeps.append)
    adapter = telegram.TelegramApprovalAdapter("token", SimpleNamespace(chat_id=-1001))

    assert adapter._request("sendMessage", {"chat_id": "-1001", "text": "candidate"})["ok"] is True
    assert responses == []
    assert sleeps == [2, telegram.TELEGRAM_GROUP_SEND_INTERVAL_SECONDS]


def test_retry_after_is_bounded_and_non_rate_limit_errors_are_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert telegram._retry_after(_rate_limit(999)) == telegram.TELEGRAM_MAX_RETRY_AFTER_SECONDS
    assert telegram._retry_after(_rate_limit("invalid")) == 1

    forbidden = HTTPError("https://api.telegram.invalid", 403, "Forbidden", {}, io.BytesIO(b"{}"))
    monkeypatch.setattr(telegram, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(forbidden))
    adapter = telegram.TelegramApprovalAdapter("token", SimpleNamespace(chat_id=-1001))
    with pytest.raises(HTTPError) as raised:
        adapter._request("sendMessage", {"chat_id": "-1001", "text": "candidate"})
    assert raised.value.code == 403


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
                "source_url": "https://t.me/aipost/7687",
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
    assert payload["text"] == "제목: Anthropic 보안 평가 사고\n출처: https://t.me/aipost/7687"
    assert "reply_markup" in payload
    assert "점수" not in payload["text"]
    assert "근거" not in payload["text"]
    assert "internal metadata" not in payload["text"]

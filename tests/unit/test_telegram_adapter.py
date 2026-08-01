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

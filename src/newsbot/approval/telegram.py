"""Small optional Telegram Bot API transport.

Importing this module is offline-safe: credentials and network access are used
only by explicit send/poll calls.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from newsbot.candidates import CandidateApprovalService, CandidateDigest

TELEGRAM_TEXT_LIMIT = 4096
TELEGRAM_GROUP_SEND_INTERVAL_SECONDS = 3.1
TELEGRAM_MAX_RETRY_AFTER_SECONDS = 60


def _retry_after(error: HTTPError) -> int:
    try:
        payload = json.loads(error.read(64 * 1024).decode("utf-8"))
        seconds = payload["parameters"]["retry_after"]
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return 1
    if not isinstance(seconds, int) or isinstance(seconds, bool):
        return 1
    return min(max(seconds, 1), TELEGRAM_MAX_RETRY_AFTER_SECONDS)


def _utf16_units(value: str) -> int:
    return len(value.encode("utf-16-le", "surrogatepass")) // 2


def split_telegram_text(text: str, *, limit: int = TELEGRAM_TEXT_LIMIT) -> tuple[str, ...]:
    """Split text without losing characters or exceeding Telegram's UTF-16 limit."""
    if limit < 1:
        raise ValueError("limit must be positive")
    if not text:
        return ("",)
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start
        units = 0
        last_newline: int | None = None
        while end < len(text):
            char_units = _utf16_units(text[end])
            if units + char_units > limit:
                break
            units += char_units
            end += 1
            if text[end - 1] == "\n":
                last_newline = end
        if end == start:
            raise ValueError("a single character exceeds the Telegram UTF-16 limit")
        split_at = last_newline if end < len(text) and last_newline is not None and last_newline > start else end
        chunks.append(text[start:split_at])
        start = split_at
    return tuple(chunks)


@dataclass(frozen=True, slots=True)
class TelegramApprovalAdapter:
    token: str
    service: CandidateApprovalService
    api_base: str = "https://api.telegram.org"

    def _request(self, method: str, payload: Mapping[str, str]) -> dict[str, object]:
        if not self.token:
            raise ValueError("Telegram bot token is required when invoking the adapter")
        body = urlencode(payload).encode("utf-8")
        request = Request(f"{self.api_base}/bot{self.token}/{method}", data=body, method="POST")
        decoded: object = None
        for attempt in range(3):
            try:
                with urlopen(request, timeout=20) as response:  # nosec B310: explicit Bot API endpoint
                    decoded = json.loads(response.read().decode("utf-8"))
                break
            except HTTPError as error:
                if error.code != 429 or attempt == 2:
                    raise
                time.sleep(_retry_after(error))
        if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
            raise RuntimeError("Telegram Bot API returned an invalid response")
        result: dict[str, object] = {}
        for key, value in decoded.items():
            result[key] = value
        if not result.get("ok"):
            raise RuntimeError("Telegram Bot API request failed")
        if method == "sendMessage":
            time.sleep(TELEGRAM_GROUP_SEND_INTERVAL_SECONDS)
        return result

    def _send_text(self, text: str, *, markup: Mapping[str, object] | None = None) -> None:
        chunks = split_telegram_text(text)
        for index, chunk in enumerate(chunks):
            payload = {"chat_id": str(self.service.chat_id), "text": chunk}
            if markup is not None and index == len(chunks) - 1:
                payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
            self._request("sendMessage", payload)

    def send_candidate_digest(self, digest: CandidateDigest) -> None:
        for candidate in digest.candidates:
            buttons = digest.buttons[int(candidate["candidate_id"])]
            markup = {
                "inline_keyboard": [[{"text": button.label, "callback_data": button.token}] for button in buttons]
            }
            self._send_text(f"제목: {candidate['title']}\n출처: {candidate['source_url']}", markup=markup)

    def send_review_draft(
        self,
        *,
        candidate_id: int,
        generation_id: int,
        source_version_ids: tuple[int, ...],
        draft_text: str,
        actor_id: int,
    ) -> None:
        buttons = self.service.review_buttons(
            candidate_id, generation_id, actor_id=actor_id, source_version_ids=source_version_ids
        )
        markup = {"inline_keyboard": [[{"text": button.label, "callback_data": button.token}] for button in buttons]}
        warnings = "\n".join(
            f"경고 ({warning['kind']}): {warning['detail']}"
            for warning in self.service.warnings_for_candidate(candidate_id)
        )
        binding = (
            f"검토 초안 #{generation_id}\n후보: #{candidate_id}\n"
            f"소스 리비전: {', '.join(map(str, source_version_ids))}\n"
            "신뢰 표시: draft=true, source_reported=true\n\n"
        )
        self._send_text(binding + (f"{warnings}\n\n" if warnings else "") + draft_text, markup=markup)

    def send_caption(self, caption_text: str) -> None:
        self._send_text(caption_text)

    def handle_update(self, update: dict[str, Any]) -> str | None:
        callback = update.get("callback_query")
        if not isinstance(callback, dict):
            return None
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        user = callback.get("from") or {}
        token = callback.get("data")
        if not isinstance(token, str) or not isinstance(chat.get("id"), int) or not isinstance(user.get("id"), int):
            return None
        result = self.service.apply(token, chat_id=chat["id"], user_id=user["id"])
        callback_id = callback.get("id")
        if isinstance(callback_id, str):
            self._request("answerCallbackQuery", {"callback_query_id": callback_id, "text": result.status})
        return result.status

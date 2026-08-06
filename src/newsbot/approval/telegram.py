"""Small optional Telegram Bot API transport.

Importing this module is offline-safe: credentials and network access are used
only by explicit send/poll calls.
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from newsbot.candidates import CandidateApprovalService, CandidateDigest

TELEGRAM_TEXT_LIMIT = 4096
TELEGRAM_GROUP_SEND_INTERVAL_SECONDS = 3.1
TELEGRAM_HTTP_TIMEOUT_SECONDS = 20
TELEGRAM_LONG_POLL_MARGIN_SECONDS = 10


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


_NO_REDIRECT_OPENER = build_opener(_NoRedirectHandler())


def _open_no_redirect(request: Request, *, timeout: float) -> Any:
    return _NO_REDIRECT_OPENER.open(request, timeout=timeout)


def _transport_timeout(method: str, payload: Mapping[str, str]) -> int:
    if method != "getUpdates":
        return TELEGRAM_HTTP_TIMEOUT_SECONDS
    try:
        long_poll = int(payload.get("timeout", "0"))
    except ValueError:
        long_poll = 0
    return max(TELEGRAM_HTTP_TIMEOUT_SECONDS, min(max(long_poll, 0), 50) + TELEGRAM_LONG_POLL_MARGIN_SECONDS)


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


def split_telegram_titles(titles: tuple[str, ...], *, limit: int = TELEGRAM_TEXT_LIMIT) -> tuple[str, ...]:
    """Pack immutable titles at title boundaries with newline separators."""
    if not titles:
        raise ValueError("noon digest requires titles")
    chunks: list[str] = []
    current = ""
    for title in titles:
        if not title or _utf16_units(title) > limit:
            raise ValueError("noon title exceeds Telegram UTF-16 limit")
        candidate = title if not current else current + "\n" + title
        if _utf16_units(candidate) > limit:
            chunks.append(current)
            current = title
        else:
            current = candidate
    if current:
        chunks.append(current)
    return tuple(chunks)

def format_review_draft(draft_text: str) -> str:
    """Render structured generation JSON as a concise Telegram review."""
    try:
        draft = json.loads(draft_text)
    except json.JSONDecodeError:
        return draft_text
    if not isinstance(draft, Mapping):
        return draft_text

    lines = ["제작 초안"]

    cover = draft.get("cover")
    if isinstance(cover, Mapping):
        lines.append("\n[표지]")
        _append_review_field(lines, "제목", cover.get("title"))
        _append_review_field(lines, "부제", cover.get("subtitle"))

    bodies = draft.get("bodies")
    if isinstance(bodies, list):
        for index, body in enumerate(bodies, 1):
            if not isinstance(body, Mapping):
                continue
            lines.append(f"\n[본문 {index}]")
            _append_review_field(lines, "소제목", body.get("subtitle"))
            _append_review_field(lines, "내용", body.get("body"))

    caption = draft.get("caption")
    if isinstance(caption, Mapping):
        lines.append("\n[캡션]")
        for label, key in (
            ("훅", "hook"),
            ("맥락", "context"),
            ("상세", "details"),
            ("의미", "implications"),
            ("질문", "questions"),
        ):
            _append_review_field(lines, label, caption.get(key))
        hashtags = caption.get("hashtags")
        if isinstance(hashtags, list):
            rendered = " ".join(value for value in hashtags if isinstance(value, str) and value.strip())
            _append_review_field(lines, "해시태그", rendered)

    _append_review_field(lines, "\n카테고리", draft.get("category"))
    sources = _review_source_urls(draft.get("claim_manifest"))
    if sources:
        lines.append("\n[출처]")
        lines.extend(f"- {url}" for url in sources)

    trust = []
    if draft.get("draft") is True:
        trust.append("초안")
    if draft.get("source_reported") is True:
        trust.append("출처 기반")
    if trust:
        lines.append(f"\n검토 상태: {' / '.join(trust)}")
    return "\n".join(lines)


def _append_review_field(lines: list[str], label: str, value: object) -> None:
    if isinstance(value, str) and value.strip():
        lines.append(f"{label}: {value.strip()}")


def _review_source_urls(manifest: object) -> tuple[str, ...]:
    if not isinstance(manifest, list):
        return ()
    urls: list[str] = []
    for claim in manifest:
        if not isinstance(claim, Mapping):
            continue
        url = claim.get("source_url")
        if isinstance(url, str) and url.strip() and url not in urls:
            urls.append(url)
    return tuple(urls)


@dataclass(frozen=True, slots=True)
class TelegramRequestResult:
    """A redacted, typed result suitable for durable dispatch code."""

    accepted: bool
    message_id: int | None = None
    safe_code: str | None = None


@dataclass(frozen=True, slots=True)
class TelegramDeadline:
    """One monotonic budget shared by a complete dispatch attempt."""

    expires_at: float

    @classmethod
    def after(cls, seconds: float) -> TelegramDeadline:
        if seconds <= 0:
            raise ValueError("deadline must be positive")
        return cls(time.monotonic() + seconds)

    def remaining(self) -> float:
        return self.expires_at - time.monotonic()


@dataclass(slots=True)
class TelegramApprovalAdapter:
    token: str
    service: CandidateApprovalService
    api_base: str = "https://api.telegram.org"

    def _request(
        self,
        method: str,
        payload: Mapping[str, str],
        *,
        deadline: TelegramDeadline | None = None,
        pace: bool = True,
    ) -> dict[str, object]:
        if not self.token:
            raise ValueError("Telegram bot token is required when invoking the adapter")
        body = urlencode(payload).encode("utf-8")
        request = Request(f"{self.api_base}/bot{self.token}/{method}", data=body, method="POST")
        decoded: object = None
        remaining = deadline.remaining() if deadline is not None else float("inf")
        if remaining <= 0:
            raise TimeoutError("Telegram request deadline exhausted")
        try:
            timeout = min(float(_transport_timeout(method, payload)), remaining)
            with _open_no_redirect(request, timeout=timeout) as response:  # nosec B310
                decoded = json.loads(response.read().decode("utf-8"))
        except HTTPError:
            raise
        if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
            raise RuntimeError("Telegram Bot API returned an invalid response")
        result = dict(decoded)
        if not result.get("ok"):
            raise RuntimeError("Telegram Bot API request failed")
        if method == "sendMessage" and pace:
            delay = TELEGRAM_GROUP_SEND_INTERVAL_SECONDS
            if deadline is None or delay < deadline.remaining():
                time.sleep(delay)
        return result

    def _send_text(self, text: str, *, markup: Mapping[str, object] | None = None) -> None:
        chunks = split_telegram_text(text)
        for index, chunk in enumerate(chunks):
            payload = {"chat_id": str(self.service.chat_id), "text": chunk}
            if markup is not None and index == len(chunks) - 1:
                payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
            self._request("sendMessage", payload)

    def prepare_message_payload(self, text: str, *, markup: Mapping[str, object] | None = None) -> dict[str, str]:
        if _utf16_units(text) > TELEGRAM_TEXT_LIMIT:
            raise ValueError("dispatch chunk exceeds Telegram UTF-16 limit")
        payload = {"chat_id": str(self.service.chat_id), "text": text}
        if markup is not None:
            payload["reply_markup"] = json.dumps(markup, ensure_ascii=False)
        return payload

    def pace_after_send(self, deadline: TelegramDeadline) -> None:
        delay = TELEGRAM_GROUP_SEND_INTERVAL_SECONDS
        if delay >= deadline.remaining():
            raise TimeoutError("Telegram request deadline exhausted")
        time.sleep(delay)

    def send_prepared_message_once(
        self,
        payload: Mapping[str, str],
        *,
        deadline: TelegramDeadline,
    ) -> TelegramRequestResult:
        """Send exactly one previously attested payload."""
        try:
            response = self._request(
                "sendMessage",
                payload,
                deadline=deadline,
                pace=False,
            )
        except TimeoutError:
            return TelegramRequestResult(False, safe_code="deadline_exhausted")
        except HTTPError as error:
            if error.code == 429:
                return TelegramRequestResult(False, safe_code="rate_limited")
            if error.code in {400, 401, 403, 404}:
                return TelegramRequestResult(False, safe_code="transport_rejected")
            return TelegramRequestResult(False, safe_code="transport_ambiguous")
        except (OSError, RuntimeError, ValueError):
            return TelegramRequestResult(False, safe_code="transport_ambiguous")
        result = response.get("result")
        message_id = result.get("message_id") if isinstance(result, dict) else None
        if not isinstance(message_id, int) or isinstance(message_id, bool) or message_id < 1:
            return TelegramRequestResult(False, safe_code="invalid_response")
        return TelegramRequestResult(True, message_id=message_id)

    def send_message_once(
        self,
        text: str,
        *,
        markup: Mapping[str, object] | None = None,
        deadline: TelegramDeadline,
    ) -> TelegramRequestResult:
        """Send exactly one already-selected chunk; callers never retry a subject."""
        payload = self.prepare_message_payload(text, markup=markup)
        return self.send_prepared_message_once(payload, deadline=deadline)

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
        self._send_text(
            binding + (f"{warnings}\n\n" if warnings else "") + format_review_draft(draft_text),
            markup=markup,
        )

    def send_caption(self, caption_text: str) -> None:
        self._send_text(caption_text)

    def handle_update(
        self,
        update: dict[str, Any],
        *,
        automation_lease: Any | None = None,
        deadline: TelegramDeadline | None = None,
    ) -> str | None:
        callback = update.get("callback_query")
        if not isinstance(callback, dict):
            return None
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        user = callback.get("from") or {}
        token = callback.get("data")
        if not isinstance(token, str) or not isinstance(chat.get("id"), int) or not isinstance(user.get("id"), int):
            return None
        if automation_lease is None:
            result = self.service.apply(token, chat_id=chat["id"], user_id=user["id"])
        else:
            result = self.service.apply(
                token,
                chat_id=chat["id"],
                user_id=user["id"],
                automation_lease=automation_lease,
            )
        callback_id = callback.get("id")
        if isinstance(callback_id, str):
            with suppress(Exception):
                self._request(
                    "answerCallbackQuery",
                    {"callback_query_id": callback_id, "text": result.status},
                    deadline=deadline,
                )
        return result.status

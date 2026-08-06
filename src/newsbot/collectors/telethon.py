"""Optional MTProto collector.  Importing this module never imports Telethon."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any, TypeVar

from .base import Engagement, Media, SourceObservation, UrlCandidate

_Result = TypeVar("_Result")


class TelethonRetryError(RuntimeError):
    """A bounded live-collection retry was exhausted or unsafe to wait for."""


class TelethonCollector:
    """Small lazy adapter for live collection.

    Telethon is deliberately imported only when a caller constructs this opt-in
    capability, so fixture/ranking commands work without the optional package.
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        session: str,
        *,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0,
        max_flood_wait_seconds: float = 60.0,
        deadline_at: float | None = None,
    ) -> None:
        if max_retries < 0 or retry_delay_seconds < 0 or max_flood_wait_seconds < 0:
            raise ValueError("Telethon retry limits must be nonnegative")
        self._api_id = api_id
        self._api_hash = api_hash
        self._session = session
        self._sleeper = sleeper
        self._max_retries = max_retries
        self._retry_delay_seconds = retry_delay_seconds
        self._max_flood_wait_seconds = max_flood_wait_seconds
        self._deadline_at = deadline_at
        self._client: Any | None = None

    async def _client_instance(self) -> Any:
        if self._client is None:
            try:
                from telethon import TelegramClient
            except ImportError as exc:
                raise RuntimeError("Telethon collection requires the telegram extra") from exc
            self._client = TelegramClient(self._session, self._api_id, self._api_hash)
        return self._client

    async def authenticate(self) -> None:
        """Interactively authorize this local session when Telethon requires it."""
        client = await self._client_instance()
        await client.connect()
        if not await client.is_user_authorized():
            await client.start()

    async def latest_message_id(self, channel: object) -> int | None:
        """Fetch exactly one newest message to fix a normal scan's upper bound."""
        handle = getattr(channel, "handle", str(channel)).lstrip("@")

        async def fetch() -> int | None:
            client = await self._client_instance()
            await client.connect()
            async for message in client.iter_messages(handle, limit=1):
                return int(message.id)
            return None

        return await self._with_retry(fetch)

    async def collect(
        self,
        channel: object,
        *,
        lower_bound: datetime | None = None,
        upper_bound: datetime | None = None,
        after: tuple[str, str] | None = None,
        min_message_id: int | None = None,
        max_message_id: int | None = None,
        limit: int | None = None,
    ) -> Sequence[SourceObservation]:
        handle = getattr(channel, "handle", str(channel)).lstrip("@")
        channel_id = str(getattr(channel, "id", handle))
        request: dict[str, Any] = {"reverse": True, "limit": limit}
        if min_message_id is not None:
            request["min_id"] = min_message_id
        if max_message_id is not None:
            request["max_id"] = max_message_id + 1
        if lower_bound is not None:
            request["offset_date"] = lower_bound.astimezone(UTC)

        async def fetch() -> tuple[SourceObservation, ...]:
            client = await self._client_instance()
            await client.connect()
            rows: list[SourceObservation] = []
            async for message in client.iter_messages(handle, **request):
                date = _as_utc(message.date)
                if upper_bound is not None and date > upper_bound.astimezone(UTC):
                    break
                if lower_bound is not None and date < lower_bound.astimezone(UTC):
                    continue
                if after is not None and (date.isoformat(), str(message.id)) <= after:
                    continue
                rows.append(_observation_from_message(message, channel_id, handle))
                if limit is not None and len(rows) >= limit:
                    break
            return tuple(rows)

        return await self._with_retry(fetch)

    async def _with_retry(self, operation: Callable[[], Awaitable[_Result]]) -> _Result:
        for attempt in range(self._max_retries + 1):
            remaining = self._remaining_seconds()
            if remaining is not None and remaining <= 0:
                raise TimeoutError("collection application deadline exhausted")
            try:
                if remaining is None:
                    return await operation()
                async with asyncio.timeout(remaining):
                    return await operation()
            except Exception as error:
                remaining = self._remaining_seconds()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("collection application deadline exhausted") from error
                flood_wait = _flood_wait_seconds(error)
                if flood_wait is not None:
                    if flood_wait > self._max_flood_wait_seconds:
                        raise TelethonRetryError(
                            f"Telethon FloodWait of {flood_wait:g}s exceeds the configured "
                            f"{self._max_flood_wait_seconds:g}s limit"
                        ) from error
                    delay = flood_wait
                elif _is_retryable(error):
                    delay = self._retry_delay_seconds
                else:
                    raise
                if attempt == self._max_retries:
                    raise TelethonRetryError(f"Telethon request failed after {attempt + 1} attempts") from error
                if remaining is not None:
                    delay = min(delay, remaining)
                await self._sleeper(delay)
        raise AssertionError("unreachable")

    def _remaining_seconds(self) -> float | None:
        return None if self._deadline_at is None else self._deadline_at - time.monotonic()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.disconnect()


def _flood_wait_seconds(error: Exception) -> float | None:
    if type(error).__name__ != "FloodWaitError":
        return None
    seconds = getattr(error, "seconds", None)
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or seconds < 0:
        return None
    return float(seconds)


def _is_retryable(error: Exception) -> bool:
    if isinstance(error, (TimeoutError, ConnectionError, OSError, asyncio.TimeoutError)):
        return True
    return type(error).__name__ in {
        "ServerError",
        "TimedOutError",
        "RpcCallFailError",
        "InterdcCallErrorError",
    }


_URL_PATTERN = re.compile(r"https?://[^\s<>()\[\]{}\"']+", re.IGNORECASE)


def _observation_from_message(message: Any, channel_id: str, handle: str) -> SourceObservation:
    text = str(getattr(message, "message", None) or "")
    action = getattr(message, "action", None)
    return SourceObservation(
        channel_id=channel_id,
        channel_handle=handle,
        external_post_id=str(message.id),
        published_at=_as_utc(message.date),
        text=text,
        edited_at=_as_utc(message.edit_date) if getattr(message, "edit_date", None) else None,
        observed_at=None,
        kind="service" if action is not None else "message",
        sponsored=bool(getattr(message, "sponsored", False)),
        urls=_urls_from_message(message, text),
        media=_media_from_message(message, text, action is not None),
        engagement=Engagement(
            views=_integer_or_none(getattr(message, "views", None)),
            reactions=_reaction_count(getattr(message, "reactions", None)),
            forwards=_integer_or_none(getattr(message, "forwards", None)),
        ),
    )


def _urls_from_message(message: Any, text: str) -> tuple[UrlCandidate, ...]:
    urls: list[UrlCandidate] = []
    entity_urls: set[str] = set()
    for entity in getattr(message, "entities", None) or ():
        value = getattr(entity, "url", None)
        if not value:
            offset = getattr(entity, "offset", None)
            length = getattr(entity, "length", None)
            if isinstance(offset, int) and isinstance(length, int):
                value = _entity_text(text, offset, length)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            entity_urls.add(value)
            urls.append(UrlCandidate(value, source="entity"))

    for match in _URL_PATTERN.finditer(text):
        value = match.group(0).rstrip(".,!?:;")
        if value not in entity_urls:
            urls.append(UrlCandidate(value, source="bare"))
    preview = getattr(message, "web_preview", None)
    if not getattr(preview, "url", None):
        preview = getattr(getattr(message, "media", None), "webpage", None)
    preview_url = getattr(preview, "url", None)
    if isinstance(preview_url, str) and preview_url.startswith(("http://", "https://")):
        urls.append(
            UrlCandidate(
                preview_url,
                source="preview",
                title=_string_or_none(getattr(preview, "title", None)),
                description=_string_or_none(getattr(preview, "description", None)),
            )
        )
    return tuple(urls)


def _entity_text(text: str, offset: int, length: int) -> str:
    return text.encode("utf-16-le")[offset * 2 : (offset + length) * 2].decode("utf-16-le")


def _media_from_message(message: Any, caption: str, is_service: bool) -> tuple[Media, ...]:
    media = getattr(message, "media", None)
    if media is None:
        return ()
    media_name = type(media).__name__
    kind = media_name.removeprefix("MessageMedia").lower() or "media"
    nested = getattr(media, "document", None) or getattr(media, "photo", None)
    identity = getattr(nested, "id", None)
    if identity is None:
        identity = getattr(media, "id", None)
    return (
        Media(
            kind=kind,
            caption=caption or None,
            identity=str(identity) if identity is not None else None,
            is_service=is_service,
        ),
    )


def _reaction_count(reactions: Any) -> int | None:
    if reactions is None:
        return None
    count = _integer_or_none(getattr(reactions, "count", None))
    if count is not None:
        return count
    results = getattr(reactions, "results", None)
    if results is None:
        return None
    total = 0
    for result in results:
        value = _integer_or_none(getattr(result, "count", None))
        if value is not None:
            total += value
    return total


def _integer_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

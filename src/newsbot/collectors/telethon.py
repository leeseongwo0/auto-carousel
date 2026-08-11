"""Optional MTProto collector.  Importing this module never imports Telethon."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar, cast

from .base import Engagement, Media, SourceObservation, UrlCandidate

_Result = TypeVar("_Result")


class TelethonRetryError(RuntimeError):
    """A bounded live-collection retry was exhausted or unsafe to wait for."""


@dataclass(frozen=True)
class EditSweepPage:
    observations: tuple[SourceObservation, ...]
    next_before_message_id: int | None
    complete: bool


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
            observed_at = datetime.now(UTC)
            async for message in client.iter_messages(handle, **request):
                date = _as_utc(getattr(message, "date", None), fallback=observed_at)
                if upper_bound is not None and date > upper_bound.astimezone(UTC):
                    break
                if lower_bound is not None and date < lower_bound.astimezone(UTC):
                    continue
                if after is not None and (date.isoformat(), str(message.id)) <= after:
                    continue
                rows.append(_observation_from_message(message, channel_id, handle, observed_at))
                if limit is not None and len(rows) >= limit:
                    break
            return tuple(rows)

        return await self._with_retry(fetch)

    async def collect_ascending(
        self,
        channel: object,
        *,
        after_message_id: int | None,
        upper_message_id: int,
        limit: int,
        lower_bound: datetime | None = None,
    ) -> Sequence[SourceObservation]:
        """Return one bounded, ascending new-message page within a fixed ID range."""
        if after_message_id is not None and after_message_id < 0:
            raise ValueError("after_message_id must be nonnegative")
        if upper_message_id < 1:
            raise ValueError("upper_message_id must be positive")
        if after_message_id is not None and after_message_id >= upper_message_id:
            return ()
        _require_page_limit(limit)
        return await self.collect(
            channel,
            min_message_id=after_message_id,
            max_message_id=upper_message_id,
            lower_bound=lower_bound,
            limit=limit,
        )

    async def collect_edit_sweep(
        self,
        channel: object,
        *,
        after: tuple[datetime, int] | None,
        before_message_id: int | None,
        lower_bound: datetime,
        limit: int,
    ) -> EditSweepPage:
        """Inspect one bounded newest-to-oldest page in a rotating history sweep."""
        _require_page_limit(limit)
        if lower_bound.tzinfo is None:
            raise ValueError("edit sweep lower bound must be timezone-aware")
        if before_message_id is not None and before_message_id <= 0:
            raise ValueError("edit sweep cursor must be a positive message ID")
        if after is not None:
            after_at, after_message_id = after
            if after_at.tzinfo is None or after_message_id < 0:
                raise ValueError("edit cursor must contain an aware timestamp and nonnegative message ID")
            cursor = (after_at.astimezone(UTC), after_message_id)
        else:
            cursor = None
        cutoff = lower_bound.astimezone(UTC)
        handle = getattr(channel, "handle", str(channel)).lstrip("@")
        channel_id = str(getattr(channel, "id", handle))

        async def fetch() -> EditSweepPage:
            client = await self._client_instance()
            await client.connect()
            observed_at = datetime.now(UTC)
            request: dict[str, object] = {"limit": limit}
            if before_message_id is not None:
                request["offset_id"] = before_message_id
            inspected_ids: list[int] = []
            observations: list[SourceObservation] = []
            complete = False
            async for message in client.iter_messages(handle, **request):
                message_id = int(message.id)
                inspected_ids.append(message_id)
                published_at = _as_utc(
                    getattr(message, "date", None),
                    fallback=observed_at,
                )
                if published_at < cutoff:
                    complete = True
                    break
                observation = _observation_from_message(
                    message,
                    channel_id,
                    handle,
                    observed_at,
                )
                if observation.edited_at is None:
                    continue
                if cursor is not None and _revision_cursor(observation) <= cursor:
                    continue
                observations.append(observation)
            if len(inspected_ids) < limit:
                complete = True
            observations.sort(key=_revision_cursor)
            next_before = None if complete or not inspected_ids else min(inspected_ids)
            return EditSweepPage(
                tuple(observations),
                next_before,
                complete,
            )

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


def _observation_from_message(
    message: Any,
    channel_id: str,
    handle: str,
    observed_at: datetime,
) -> SourceObservation:
    text = str(getattr(message, "message", None) or "")
    action = getattr(message, "action", None)
    deleted = _is_deleted_message(message)
    preview = _preview_from_message(message)
    preview_title = _string_or_none(getattr(preview, "title", None))
    preview_description = _string_or_none(getattr(preview, "description", None))
    published_at = _as_utc(getattr(message, "date", None), fallback=observed_at)
    return SourceObservation(
        channel_id=channel_id,
        channel_handle=handle,
        external_post_id=str(message.id),
        published_at=published_at,
        text=text,
        edited_at=_datetime_or_none(getattr(message, "edit_date", None)),
        observed_at=observed_at,
        preview_title=preview_title,
        preview_description=preview_description,
        kind="deleted" if deleted else "service" if action is not None else "message",
        sponsored=bool(getattr(message, "sponsored", False)),
        urls=_urls_from_message(message, text, preview),
        media=_media_from_message(message, text, action is not None),
        engagement=Engagement(
            views=_integer_or_none(getattr(message, "views", None)),
            reactions=_reaction_count(getattr(message, "reactions", None)),
            forwards=_integer_or_none(getattr(message, "forwards", None)),
        ),
    )


def _urls_from_message(message: Any, text: str, preview: Any | None = None) -> tuple[UrlCandidate, ...]:
    candidates: list[tuple[Literal["preview", "entity", "bare"], str, str | None, str | None]] = []
    if preview is None:
        preview = _preview_from_message(message)
    preview_url = getattr(preview, "url", None)
    if _is_http_url(preview_url):
        candidates.append(
            (
                "preview",
                cast(str, preview_url),
                _string_or_none(getattr(preview, "title", None)),
                _string_or_none(getattr(preview, "description", None)),
            )
        )

    entities: list[tuple[int, int, str]] = []
    for index, entity in enumerate(getattr(message, "entities", None) or ()):
        value = getattr(entity, "url", None)
        offset = getattr(entity, "offset", None)
        length = getattr(entity, "length", None)
        if not _is_http_url(value) and isinstance(offset, int) and isinstance(length, int):
            value = _entity_text(text, offset, length)
        if _is_http_url(value):
            entities.append(
                (
                    offset if isinstance(offset, int) else len(text.encode("utf-16-le")) // 2,
                    index,
                    cast(str, value),
                )
            )
    for _, _, value in sorted(entities):
        candidates.append(("entity", value, None, None))

    for match in _URL_PATTERN.finditer(text):
        candidates.append(("bare", match.group(0).rstrip(".,!?:;"), None, None))

    return tuple(
        UrlCandidate(url=value, source=source, title=title, description=description, occurrence=index)
        for index, (source, value, title, description) in enumerate(candidates)
    )


def _preview_from_message(message: Any) -> Any | None:
    preview = getattr(message, "web_preview", None)
    return preview if getattr(preview, "url", None) else getattr(getattr(message, "media", None), "webpage", None)


def _is_http_url(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(("http://", "https://"))


def _entity_text(text: str, offset: int, length: int) -> str:
    if offset < 0 or length < 0:
        return ""
    try:
        return text.encode("utf-16-le")[offset * 2 : (offset + length) * 2].decode("utf-16-le")
    except UnicodeDecodeError:
        return ""


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


def _require_page_limit(limit: int) -> None:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("page limit must be a positive integer")


def _revision_cursor(observation: SourceObservation) -> tuple[datetime, int]:
    return (observation.edited_at or observation.published_at, int(observation.external_post_id))


def _is_deleted_message(message: Any) -> bool:
    return type(message).__name__ in {"MessageDeleted", "MessageEmpty"}


def _datetime_or_none(value: Any) -> datetime | None:
    return _as_utc(value) if isinstance(value, datetime) else None


def _as_utc(value: datetime | None, *, fallback: datetime | None = None) -> datetime:
    if value is None:
        if fallback is None:
            raise ValueError("Telegram message timestamp is missing")
        return fallback.astimezone(UTC)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

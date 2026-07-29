"""Credential-free collector for checked-in JSON fixtures."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .base import Engagement, Media, SourceObservation, UrlCandidate


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("fixture timestamps must include a timezone")
    return parsed


def _urls(values: Any) -> tuple[UrlCandidate, ...]:
    if not values:
        return ()
    result: list[UrlCandidate] = []
    for value in values:
        if isinstance(value, str):
            result.append(UrlCandidate(value))
        elif isinstance(value, dict):
            result.append(
                UrlCandidate(
                    url=str(value["url"]),
                    source=value.get("source", "bare"),
                    title=value.get("title"),
                    description=value.get("description"),
                )
            )
        else:
            raise ValueError("fixture URL must be a string or object")
    return tuple(result)


class FixtureCollector:
    """Read deterministic local JSON; this collector never opens a socket."""

    def __init__(self, fixture_path: str | Path) -> None:
        self.fixture_path = Path(fixture_path)
        self._collect_calls = 0
        self._upper_calls = 0

    def latest_message_id(self, channel: object) -> int | None:
        """Return the scan upper bound before a fixture enumeration begins."""
        self._upper_calls += 1
        payload = json.loads(self.fixture_path.read_text(encoding="utf-8"))
        messages = self._messages(payload)
        channel_id = str(getattr(channel, "id", channel))
        ids: list[int] = []
        for item in messages:
            if not isinstance(item, dict) or str(item.get("channel_id", channel_id)) != channel_id:
                continue
            raw_id = item.get("external_post_id", item.get("id", ""))
            if not isinstance(raw_id, (int, str, bytes, bytearray)):
                raise ValueError("fixture message ID must be an integer")
            ids.append(int(raw_id))
        return max(ids) if ids else None

    def collect(
        self,
        channel: object | None = None,
        *,
        lower_bound: datetime | None = None,
        upper_bound: datetime | None = None,
        after: tuple[str, str] | None = None,
        min_message_id: int | None = None,
        max_message_id: int | None = None,
        limit: int | None = None,
        **_: object,
    ) -> Sequence[SourceObservation]:
        self._collect_calls += 1
        payload = json.loads(self.fixture_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and self._collect_calls in set(payload.get("crash_on_calls", ())):
            raise RuntimeError(f"fixture crash on collect call {self._collect_calls}")
        messages = self._messages(payload)
        if isinstance(payload, dict) and "pages" in payload:
            pages = payload["pages"]
            messages = pages[min(self._collect_calls - 1, len(pages) - 1)] if pages else []
        if not isinstance(messages, list):
            raise ValueError("fixture must be a list or an object with a messages list")
        messages = self._apply_edits(messages, payload)
        channel_id = getattr(channel, "id", None)
        channel_handle = getattr(channel, "handle", None)
        observations: list[SourceObservation] = []
        for item in messages:
            if not isinstance(item, dict):
                raise ValueError("fixture message must be an object")
            item_channel_id = str(item.get("channel_id", channel_id or ""))
            item_handle = str(item.get("channel_handle", item.get("channel", channel_handle or item_channel_id)))
            if channel_id is not None and item_channel_id != str(channel_id):
                continue
            metrics = item.get("engagement", {})
            media = tuple(
                Media(
                    str(entry.get("kind", "media")),
                    entry.get("caption"),
                    entry.get("identity"),
                    bool(entry.get("is_service", False)),
                )
                for entry in item.get("media", [])
            )
            observations.append(
                SourceObservation(
                    channel_id=item_channel_id,
                    channel_handle=item_handle,
                    external_post_id=str(item.get("external_post_id", item.get("id", ""))),
                    published_at=_datetime(item["published_at"]),
                    text=str(item.get("text", "")),
                    edited_at=_datetime(item["edited_at"]) if item.get("edited_at") else None,
                    observed_at=_datetime(item["observed_at"]) if item.get("observed_at") else None,
                    kind=item.get("kind", "message"),
                    sponsored=bool(item.get("sponsored", False)),
                    urls=_urls(item.get("urls", ())),
                    media=media,
                    engagement=Engagement(
                        metrics.get("views", item.get("views")),
                        metrics.get("reactions", item.get("reactions")),
                        metrics.get("forwards", item.get("forwards")),
                    ),
                    conflicts=tuple(str(value) for value in item.get("conflicts", ())),
                )
            )
        filtered = (
            row
            for row in observations
            if (lower_bound is None or row.published_at >= lower_bound)
            and (upper_bound is None or row.published_at <= upper_bound)
            and (after is None or (row.published_at.isoformat(), row.external_post_id) > after)
            and (min_message_id is None or int(row.external_post_id) > min_message_id)
            and (max_message_id is None or int(row.external_post_id) <= max_message_id)
        )
        ordered = tuple(sorted(filtered, key=lambda row: (int(row.external_post_id), row.channel_id)))
        return ordered if limit is None else ordered[:limit]

    def _apply_edits(self, messages: list[Any], payload: Any) -> list[Any]:
        if not isinstance(payload, dict):
            return messages
        updated = [dict(message) if isinstance(message, dict) else message for message in messages]
        for edit in payload.get("edits", ()):
            if not isinstance(edit, dict):
                continue
            edit_call = edit.get("after_call", edit.get("at_call", 1))
            if not isinstance(edit_call, (int, str)) or int(edit_call) > self._collect_calls:
                continue
            replacement = edit.get("message", edit)
            if not isinstance(replacement, dict):
                raise ValueError("fixture edit message must be an object")
            external_id = str(replacement.get("external_post_id", replacement.get("id", "")))
            for index, message in enumerate(updated):
                if (
                    isinstance(message, dict)
                    and str(message.get("external_post_id", message.get("id", ""))) == external_id
                ):
                    updated[index] = {**message, **replacement}
                    break
        return updated

    def _messages(self, payload: Any) -> list[Any]:
        """Add messages that arrive after a configured upper-bound request."""
        if not isinstance(payload, dict):
            return list(payload)
        messages = list(payload.get("messages", ()))
        for arrival in payload.get("arrivals", ()):
            if not isinstance(arrival, dict):
                raise ValueError("fixture arrival must be an object")
            if int(arrival.get("after_upper_call", 1)) <= self._upper_calls:
                message = arrival.get("message", arrival)
                if not isinstance(message, dict):
                    raise ValueError("fixture arrival message must be an object")
                messages.append(message)
        return messages

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot.collectors.telethon import (
    TelethonCollector,
    _observation_from_message,
    _urls_from_message,
)

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)


class MessageEmpty:
    def __init__(self, message_id: int) -> None:
        self.id = message_id
        self.date = None
        self.message = None
        self.action = None


def message(message_id: int, *, date: datetime = NOW, edited: datetime | None = None, action: object | None = None):
    return SimpleNamespace(
        id=message_id,
        date=date,
        edit_date=edited,
        message="post",
        action=action,
        entities=(),
        web_preview=None,
        media=None,
        sponsored=False,
        views=None,
        reactions=None,
        forwards=None,
    )


def test_url_order_preserves_preview_utf16_entity_offsets_and_bare_occurrences() -> None:
    text = "😀 https://second.example and https://first.example."
    first_offset = len("😀 https://second.example and ".encode("utf-16-le")) // 2
    second_offset = len("😀 ".encode("utf-16-le")) // 2
    source = SimpleNamespace(
        entities=(
            SimpleNamespace(offset=first_offset, length=len("https://first.example"), url=None),
            SimpleNamespace(offset=second_offset, length=len("https://second.example"), url=None),
        ),
        web_preview=SimpleNamespace(
            url="https://preview.example",
            title="Preview title",
            description="Preview description",
        ),
        media=None,
    )

    urls = _urls_from_message(source, text)

    assert [(url.source, url.url, url.occurrence) for url in urls] == [
        ("preview", "https://preview.example", 0),
        ("entity", "https://second.example", 1),
        ("entity", "https://first.example", 2),
        ("bare", "https://second.example", 3),
        ("bare", "https://first.example", 4),
    ]
    assert urls[0].title == "Preview title"
    assert urls[0].description == "Preview description"


def test_observation_preserves_preview_and_marks_service_and_deleted_messages() -> None:
    source = message(4, action=object())
    source.web_preview = SimpleNamespace(url="https://preview.example", title="Title", description="Description")
    service = _observation_from_message(source, "1", "channel", NOW)
    deleted = _observation_from_message(MessageEmpty(5), "1", "channel", NOW)

    assert (service.kind, service.preview_title, service.preview_description, service.observed_at) == (
        "service",
        "Title",
        "Description",
        NOW,
    )
    assert deleted.kind == "deleted"
    assert deleted.published_at == NOW
    assert deleted.observed_at == NOW


def test_collect_ascending_applies_fixed_id_bounds_and_observes_messages() -> None:
    calls: list[dict[str, object]] = []

    class Client:
        async def connect(self) -> None:
            pass

        async def iter_messages(self, handle: str, **kwargs: object):
            calls.append(kwargs)
            for row in (message(1), message(2), message(3)):
                if int(kwargs.get("min_id", 0)) < row.id < int(kwargs.get("max_id", row.id + 1)):
                    yield row

    collector = TelethonCollector(1, "hash", "session")
    collector._client = Client()

    rows = asyncio.run(collector.collect_ascending("channel", after_message_id=1, upper_message_id=3, limit=2))

    assert [row.external_post_id for row in rows] == ["2", "3"]
    assert calls == [{"reverse": True, "limit": 2, "min_id": 1, "max_id": 4}]
    assert all(row.observed_at is not None and row.observed_at.tzinfo is UTC for row in rows)


def test_edit_sweep_orders_total_cursor_and_excludes_equal_timestamp_cursor() -> None:
    class Client:
        async def connect(self) -> None:
            pass

        async def iter_messages(self, handle: str, **kwargs: object):
            yield message(3, edited=NOW + timedelta(minutes=1))
            yield message(2, edited=NOW)
            yield message(1, edited=NOW)

    collector = TelethonCollector(1, "hash", "session")
    collector._client = Client()

    page = asyncio.run(
        collector.collect_edit_sweep(
            "channel",
            after=(NOW, 1),
            before_message_id=None,
            lower_bound=NOW - timedelta(hours=1),
            limit=3,
        )
    )

    assert [row.external_post_id for row in page.observations] == ["2", "3"]
    assert page.next_before_message_id == 1
    assert not page.complete


def test_edit_sweep_pages_past_newest_messages_until_history_cutoff() -> None:
    calls: list[dict[str, object]] = []

    class Client:
        async def connect(self) -> None:
            pass

        async def iter_messages(self, handle: str, **kwargs: object):
            calls.append(kwargs)
            before = kwargs.get("offset_id")
            rows = (
                (
                    message(5, edited=NOW + timedelta(minutes=1)),
                    message(4),
                )
                if before is None
                else (
                    message(3, edited=NOW + timedelta(minutes=2)),
                    message(
                        2,
                        date=NOW - timedelta(hours=25),
                        edited=NOW,
                    ),
                )
            )
            for row in rows:
                yield row

    collector = TelethonCollector(1, "hash", "session")
    collector._client = Client()
    first = asyncio.run(
        collector.collect_edit_sweep(
            "channel",
            after=None,
            before_message_id=None,
            lower_bound=NOW - timedelta(hours=24),
            limit=2,
        )
    )
    second = asyncio.run(
        collector.collect_edit_sweep(
            "channel",
            after=None,
            before_message_id=first.next_before_message_id,
            lower_bound=NOW - timedelta(hours=24),
            limit=2,
        )
    )

    assert [row.external_post_id for row in first.observations] == ["5"]
    assert first.next_before_message_id == 4
    assert not first.complete
    assert [row.external_post_id for row in second.observations] == ["3"]
    assert second.next_before_message_id is None
    assert second.complete
    assert calls[1]["offset_id"] == 4


def test_retry_retries_connection_failures_without_eager_telethon_import() -> None:
    attempts = 0
    delays: list[float] = []

    async def sleeper(delay: float) -> None:
        delays.append(delay)

    class Client:
        async def connect(self) -> None:
            pass

        async def iter_messages(self, handle: str, **kwargs: object):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("temporary")
            yield message(1)

    collector = TelethonCollector(1, "hash", "session", sleeper=sleeper, max_retries=1, retry_delay_seconds=0.25)
    collector._client = Client()

    rows = asyncio.run(collector.collect_ascending("channel", after_message_id=None, upper_message_id=1, limit=1))

    assert [row.external_post_id for row in rows] == ["1"]
    assert delays == [0.25]


@pytest.mark.parametrize("limit", [0, -1])
def test_bounded_page_operations_reject_nonpositive_limits(limit: int) -> None:
    collector = TelethonCollector(1, "hash", "session")
    with pytest.raises(ValueError, match="page limit"):
        asyncio.run(collector.collect_ascending("channel", after_message_id=None, upper_message_id=1, limit=limit))

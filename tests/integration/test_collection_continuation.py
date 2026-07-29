from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot.collectors.fixture import FixtureCollector
from newsbot.collectors.telethon import _observation_from_message
from newsbot.storage import DurableCollection, Storage

NOW = datetime(2026, 7, 29, 12, tzinfo=UTC)
CHANNEL = SimpleNamespace(id="official-ai", handle="official_ai")


def _message(number: int, when: datetime, text: str | None = None) -> dict[str, str]:
    return {
        "id": str(number),
        "published_at": when.isoformat(),
        "text": text or f"message {number}",
    }


def _fixture(path, payload) -> FixtureCollector:
    import json

    path.write_text(json.dumps(payload), encoding="utf-8")
    return FixtureCollector(path)


def test_capped_pages_continue_before_promoting_cursor(tmp_path):
    fixture = _fixture(
        tmp_path / "messages.json",
        {"messages": [_message(index, NOW - timedelta(hours=6 - index)) for index in range(1, 6)]},
    )
    storage = Storage.open(tmp_path / "newsbot.sqlite")
    collection = DurableCollection(storage)

    assert not collection.collect_channel(fixture, CHANNEL, now=NOW, page_size=2).cursor_promoted
    assert storage.fetch_one("SELECT * FROM collection_cursors WHERE channel_id=?", (CHANNEL.id,)) is None
    assert not collection.collect_channel(fixture, CHANNEL, now=NOW + timedelta(hours=1), page_size=2).cursor_promoted
    completed = collection.collect_channel(fixture, CHANNEL, now=NOW + timedelta(hours=2), page_size=2)

    assert completed.cursor_promoted
    assert storage.fetch_one("SELECT COUNT(*) AS count FROM source_posts")["count"] == 5
    assert storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (CHANNEL.id,)) is None


def test_initial_floor_is_inclusive_and_survives_a_restart(tmp_path):
    fixture_path = tmp_path / "messages.json"
    fixture = _fixture(
        fixture_path,
        {
            "messages": [
                _message(1, NOW - timedelta(hours=24)),
                _message(2, NOW - timedelta(hours=25)),
                _message(3, NOW),
            ]
        },
    )
    database = tmp_path / "newsbot.sqlite"
    storage = Storage.open(database)
    collection = DurableCollection(storage)

    with pytest.raises(RuntimeError, match="deterministic collection crash"):
        collection.collect_channel(fixture, CHANNEL, now=NOW, page_size=1, crash_after_page=True)
    interval = storage.fetch_one(
        "SELECT floor_at, upper_bound_at FROM collection_intervals WHERE channel_id=?", (CHANNEL.id,)
    )
    assert interval["floor_at"] == (NOW - timedelta(hours=24)).isoformat()
    storage.close()

    with Storage.open(database) as restarted:
        result = DurableCollection(restarted).collect_channel(
            FixtureCollector(fixture_path), CHANNEL, now=NOW + timedelta(days=1), page_size=1
        )
        while not result.cursor_promoted:
            result = DurableCollection(restarted).collect_channel(
                FixtureCollector(fixture_path), CHANNEL, now=NOW + timedelta(days=1), page_size=1
            )
        posts = restarted.fetch_all("SELECT external_post_id FROM source_posts ORDER BY external_post_id")

    assert [row["external_post_id"] for row in posts] == ["1", "3"]


def test_edits_create_immutable_rich_snapshots(tmp_path):
    fixture = _fixture(
        tmp_path / "messages.json",
        {
            "messages": [_message(1, NOW - timedelta(minutes=30), "original")],
            "edits": [{"after_call": 2, "message": {"id": "1", "text": "edited", "edited_at": NOW.isoformat()}}],
        },
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        DurableCollection(storage).collect_channel(fixture, CHANNEL, now=NOW, page_size=10)
        DurableCollection(storage).collect_channel(fixture, CHANNEL, now=NOW + timedelta(hours=1), page_size=10)
        versions = storage.fetch_all("SELECT body FROM source_post_versions ORDER BY id")
        observations = storage.fetch_all(
            "SELECT source_post_version_id, channel_handle, engagement_json FROM source_post_observations ORDER BY id"
        )

    assert [row["body"] for row in versions] == ["original", "edited"]
    assert {row["channel_handle"] for row in observations} == {"official_ai"}
    assert [row["source_post_version_id"] for row in observations] == [1, 2]
    assert all(row["engagement_json"] == '{"views":null,"reactions":null,"forwards":null}' for row in observations)


def test_timestamp_only_edit_reuses_material_version_and_preserves_both_snapshots(tmp_path):
    fixture = _fixture(
        tmp_path / "messages.json",
        {
            "messages": [_message(1, NOW - timedelta(minutes=30), "unchanged")],
            "edits": [{"after_call": 2, "message": {"id": "1", "edited_at": NOW.isoformat()}}],
        },
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        DurableCollection(storage).collect_channel(fixture, CHANNEL, now=NOW, page_size=10)
        DurableCollection(storage).collect_channel(fixture, CHANNEL, now=NOW + timedelta(hours=1), page_size=10)
        versions = storage.fetch_all("SELECT id, body FROM source_post_versions ORDER BY id")
        snapshots = storage.fetch_all(
            "SELECT source_post_version_id, edited_at FROM source_post_observations ORDER BY id"
        )
        latest = storage.latest_observations()

    assert [(row["id"], row["body"]) for row in versions] == [(1, "unchanged")]
    assert [row["source_post_version_id"] for row in snapshots] == [1, 1]
    assert [row["edited_at"] for row in snapshots] == [None, NOW.isoformat()]
    assert latest[0].edited_at == NOW


def test_engagement_snapshots_reuse_the_material_source_version(tmp_path):
    fixture = _fixture(
        tmp_path / "messages.json",
        {
            "messages": [
                {
                    **_message(1, NOW - timedelta(minutes=30)),
                    "engagement": {"views": 10, "reactions": 2, "forwards": 1},
                }
            ],
            "edits": [
                {
                    "after_call": 2,
                    "message": {
                        "id": "1",
                        "engagement": {"views": 11, "reactions": 3, "forwards": 1},
                    },
                }
            ],
        },
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        result = DurableCollection(storage).collect_channel(fixture, CHANNEL, now=NOW, page_size=10)
        result = DurableCollection(storage).collect_channel(
            fixture, CHANNEL, now=NOW + timedelta(hours=1), page_size=10
        )
        versions = storage.fetch_all("SELECT id FROM source_post_versions")
        snapshots = storage.fetch_all(
            "SELECT source_post_version_id, engagement_json FROM source_post_observations ORDER BY id"
        )

    assert result.cursor_promoted
    assert len(versions) == 1
    assert [row["source_post_version_id"] for row in snapshots] == [versions[0]["id"], versions[0]["id"]]
    assert [row["engagement_json"] for row in snapshots] == [
        '{"views":10,"reactions":2,"forwards":1}',
        '{"views":11,"reactions":3,"forwards":1}',
    ]


def test_default_overlap_is_72_hours_and_the_latest_100_ids(tmp_path):
    calls = []
    upper_calls = []

    class InspectingCollector:
        def latest_message_id(self, channel):
            upper_calls.append(channel)
            return 250 if len(upper_calls) == 1 else 350

        def collect(self, channel, **kwargs):
            calls.append(kwargs)
            return ()

    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        collection = DurableCollection(storage)
        assert collection.collect_channel(InspectingCollector(), CHANNEL, now=NOW).cursor_promoted
        calls.clear()
        result = collection.collect_channel(InspectingCollector(), CHANNEL, now=NOW + timedelta(days=4))

    assert result.cursor_promoted
    assert calls == [
        {"lower_bound": None, "min_message_id": 250, "max_message_id": 350, "limit": 101},
        {
            "lower_bound": NOW + timedelta(days=4, hours=-72),
            "min_message_id": 150,
            "max_message_id": 250,
            "limit": 101,
        },
    ]


def test_mocked_telethon_provenance_mapping_is_complete():
    class MessageMediaDocument:
        document = SimpleNamespace(id=99)

    message = SimpleNamespace(
        id=7,
        date=NOW,
        edit_date=NOW + timedelta(minutes=1),
        message="Entity https://shown.example and bare https://bare.example.",
        entities=(
            SimpleNamespace(url="https://hidden.example"),
            SimpleNamespace(offset=7, length=21),
        ),
        media=MessageMediaDocument(),
        web_preview=SimpleNamespace(
            url="https://preview.example", title="Preview title", description="Preview description"
        ),
        reactions=SimpleNamespace(results=(SimpleNamespace(count=2), SimpleNamespace(count=3))),
        views=10,
        forwards=4,
        sponsored=True,
        action=SimpleNamespace(kind="service"),
    )

    observation = _observation_from_message(message, "channel-1", "channel_one")

    assert observation.channel_id == "channel-1"
    assert observation.external_post_id == "7"
    assert observation.published_at == NOW
    assert observation.edited_at == NOW + timedelta(minutes=1)
    assert observation.kind == "service"
    assert observation.sponsored
    assert {(url.url, url.source, url.title, url.description) for url in observation.urls} == {
        ("https://hidden.example", "entity", None, None),
        ("https://shown.example", "entity", None, None),
        ("https://bare.example", "bare", None, None),
        ("https://preview.example", "preview", "Preview title", "Preview description"),
    }
    assert observation.media[0].kind == "document"
    assert observation.media[0].caption == message.message
    assert observation.media[0].identity == "99"
    assert observation.media[0].is_service
    assert observation.engagement.views == 10
    assert observation.engagement.reactions == 5
    assert observation.engagement.forwards == 4


def test_deep_reconcile_never_moves_normal_cursor(tmp_path):
    fixture = _fixture(tmp_path / "messages.json", {"messages": [_message(1, NOW - timedelta(minutes=30))]})
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        collection = DurableCollection(storage)
        assert collection.collect_channel(fixture, CHANNEL, now=NOW, page_size=10).cursor_promoted
        before = tuple(
            storage.fetch_one(
                "SELECT published_at, external_post_id FROM collection_cursors WHERE channel_id=?", (CHANNEL.id,)
            )
        )
        collection.reconcile_channel(
            fixture,
            CHANNEL,
            lower_bound=NOW - timedelta(hours=1),
            upper_bound=NOW,
            page_size=1,
            max_pages=1,
        )
        after = tuple(
            storage.fetch_one(
                "SELECT published_at, external_post_id FROM collection_cursors WHERE channel_id=?", (CHANNEL.id,)
            )
        )

    assert after == before


def test_fixture_reconcile_is_bounded_and_cursor_neutral(tmp_path):
    from newsbot import cli

    fixture_path = tmp_path / "messages.json"
    _fixture(
        fixture_path,
        {"messages": [{**_message(1, NOW - timedelta(minutes=30)), "channel_id": "aipost"}]},
    )
    database = tmp_path / "newsbot.sqlite"
    output = tmp_path / "output"
    channel = SimpleNamespace(id="aipost", handle="aipost")
    with Storage.open(database) as storage:
        collection = DurableCollection(storage)
        collection.collect_channel(FixtureCollector(fixture_path), channel, now=NOW, page_size=10)
        before = tuple(
            storage.fetch_one(
                "SELECT published_at, external_post_id FROM collection_cursors WHERE channel_id=?", (channel.id,)
            )
        )
    _fixture(
        fixture_path,
        {
            "messages": [
                {**_message(1, NOW - timedelta(minutes=30)), "channel_id": "aipost"},
                {**_message(2, NOW - timedelta(minutes=20)), "channel_id": "aipost"},
            ]
        },
    )
    assert (
        cli.main(
            [
                "reconcile",
                "--fixture",
                str(fixture_path),
                "--channel",
                "aipost",
                "--from-id",
                "2",
                "--to-id",
                "2",
                "--page-size",
                "1",
                "--max-pages",
                "1",
                "--db",
                str(database),
                "--output",
                str(output),
            ]
        )
        == 0
    )

    with Storage.open(database) as storage:
        after = tuple(
            storage.fetch_one(
                "SELECT published_at, external_post_id FROM collection_cursors WHERE channel_id=?", (channel.id,)
            )
        )
        posts = storage.fetch_all("SELECT external_post_id FROM source_posts ORDER BY external_post_id")

    assert after == before
    assert [row["external_post_id"] for row in posts] == ["1", "2"]


def test_sparse_ids_and_arrivals_above_fixed_upper_wait_for_the_next_scan(tmp_path):
    fixture = _fixture(
        tmp_path / "messages.json",
        {
            "messages": [_message(2, NOW - timedelta(minutes=30)), _message(10, NOW - timedelta(minutes=20))],
            "arrivals": [
                {"after_upper_call": 2, "message": _message(11, NOW - timedelta(minutes=10))},
            ],
        },
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        collection = DurableCollection(storage)
        assert collection.collect_channel(fixture, CHANNEL, now=NOW, page_size=10).cursor_promoted
        first_scan = storage.fetch_all(
            "SELECT external_post_id FROM source_posts ORDER BY CAST(external_post_id AS INTEGER)"
        )
        assert [row["external_post_id"] for row in first_scan] == ["2", "10"]

        assert collection.collect_channel(
            fixture, CHANNEL, now=NOW + timedelta(minutes=1), page_size=10
        ).cursor_promoted
        second_scan = storage.fetch_all(
            "SELECT external_post_id FROM source_posts ORDER BY CAST(external_post_id AS INTEGER)"
        )

    assert [row["external_post_id"] for row in second_scan] == ["2", "10", "11"]


def test_empty_bootstrap_records_an_id_frontier_for_a_later_scan(tmp_path):
    fixture = _fixture(
        tmp_path / "messages.json",
        {"messages": [], "arrivals": [{"after_upper_call": 2, "message": _message(7, NOW)}]},
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        collection = DurableCollection(storage)
        assert collection.collect_channel(fixture, CHANNEL, now=NOW, page_size=10).cursor_promoted
        assert collection.collect_channel(
            fixture, CHANNEL, now=NOW + timedelta(minutes=1), page_size=10
        ).cursor_promoted
        posts = storage.fetch_all("SELECT external_post_id FROM source_posts")

    assert [row["external_post_id"] for row in posts] == ["7"]


def test_normal_overlap_revisits_an_edit_below_the_committed_frontier(tmp_path):
    fixture_path = tmp_path / "messages.json"
    fixture = _fixture(
        fixture_path,
        {"messages": [_message(10, NOW - timedelta(minutes=30), "original")]},
    )
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        collection = DurableCollection(storage)
        assert collection.collect_channel(fixture, CHANNEL, now=NOW, page_size=10).cursor_promoted
        _fixture(
            fixture_path,
            {"messages": [_message(10, NOW - timedelta(minutes=30), "edited")]},
        )
        assert collection.collect_channel(
            fixture, CHANNEL, now=NOW + timedelta(minutes=1), page_size=10
        ).cursor_promoted
        versions = storage.fetch_all("SELECT body FROM source_post_versions ORDER BY id")

    assert [row["body"] for row in versions] == ["original", "edited"]


def test_resumed_scan_rejects_adapter_rows_outside_fixed_message_id_bounds(tmp_path):
    initial = _fixture(
        tmp_path / "initial.json",
        {"messages": [_message(200, NOW - timedelta(minutes=2), "initial")]},
    )
    rogue_fixture = _fixture(
        tmp_path / "rogue.json",
        {
            "messages": [
                _message(50, NOW - timedelta(minutes=3), "below"),
                _message(201, NOW - timedelta(minutes=1), "inside"),
                _message(202, NOW, "upper"),
                _message(999, NOW, "above"),
            ]
        },
    )

    class IgnoringBounds:
        def latest_message_id(self, channel):
            return 202

        def collect(self, channel, **_):
            return rogue_fixture.collect(channel)

    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        collection = DurableCollection(storage)
        assert collection.collect_channel(initial, CHANNEL, now=NOW, page_size=10).cursor_promoted
        assert collection.collect_channel(
            IgnoringBounds(), CHANNEL, now=NOW + timedelta(minutes=1), page_size=10
        ).cursor_promoted
        external_ids = {
            row["external_post_id"] for row in storage.fetch_all("SELECT external_post_id FROM source_posts")
        }

    assert external_ids == {"200", "201", "202"}

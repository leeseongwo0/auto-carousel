"""SQLite persistence primitives for the local-first news bot."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import Any, cast

from .collectors.base import Engagement, Media, MessageKind, SourceObservation, UrlCandidate

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def aware_epoch_us(value: object) -> int | None:
    """Return a strict aware ISO timestamp as UTC microseconds, or ``None``."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        delta = parsed.astimezone(UTC) - _EPOCH
    except (OverflowError, ValueError):
        return None
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds


class Storage:
    """A small, explicit wrapper around one SQLite connection.

    Callers should group related writes with :meth:`transaction`; read helpers
    return ``sqlite3.Row`` objects so column access remains named and direct.
    """

    def __init__(self, database_path: str | Path) -> None:
        path = str(database_path)
        if path != ":memory:":
            Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = RLock()
        self._lease_authority_hash: str | None = None
        self._configure()

    @classmethod
    def open(cls, database_path: str | Path) -> Storage:
        """Open a database and apply all bundled migrations."""
        storage = cls(database_path)
        storage.migrate()
        return storage

    def _configure(self) -> None:
        with self._lock:
            self._connection.execute("PRAGMA foreign_keys = ON")
            self._connection.create_function(
                "sha256_hex",
                1,
                lambda value: sha256(bytes(value)).hexdigest(),
                deterministic=True,
            )
            self._connection.create_function(
                "lease_owner_hash",
                0,
                lambda: self._lease_authority_hash,
            )
            self._connection.create_function(
                "aware_epoch_us",
                1,
                aware_epoch_us,
                deterministic=True,
            )
            self._connection.execute("PRAGMA journal_mode = WAL").fetchone()
            self._connection.execute("PRAGMA synchronous = NORMAL")
            self._connection.execute("PRAGMA busy_timeout = 5000")

    def migrate(self) -> None:
        """Apply each numbered SQL migration once, in filename order."""
        migration_dir = Path(__file__).with_name("migrations")
        migrations = sorted(migration_dir.glob("[0-9][0-9][0-9]_*.sql"))
        if not migrations:
            raise RuntimeError(f"No migrations found in {migration_dir}")

        with self._lock:
            if self._connection.in_transaction:
                raise RuntimeError("Cannot migrate inside an active transaction")
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version TEXT PRIMARY KEY, "
                "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            applied = {row["version"] for row in self._connection.execute("SELECT version FROM schema_migrations")}
            for migration in migrations:
                if migration.name in applied:
                    continue
                script = migration.read_text(encoding="utf-8")
                if migration.name == "004_sheets_authority_upgrade.sql":
                    self._assert_sheets_authority_upgrade_supported(migration_dir / "003_sheets_handoff.sql")
                    handoff_columns = {
                        str(row["name"]) for row in self._connection.execute("PRAGMA table_info(sheet_handoffs)")
                    }
                    target_expression = (
                        "h.target_binding_id" if "target_binding_id" in handoff_columns else "b.target_binding_id"
                    )
                    script = script.replace("__HANDOFF_TARGET_EXPR__", target_expression)
                version = migration.name.replace("'", "''")
                foreign_keys_disabled = migration.name in {
                    "002_canonical_authority.sql",
                    "004_sheets_authority_upgrade.sql",
                }
                if migration.name == "002_canonical_authority.sql":
                    self._prepare_canonical_authority_upgrade()
                if foreign_keys_disabled:
                    self._connection.execute("PRAGMA foreign_keys = OFF")
                try:
                    if migration.name == "004_sheets_authority_upgrade.sql":
                        self._connection.executescript(f"BEGIN IMMEDIATE;\n{script}\n")
                        violations = self._connection.execute("PRAGMA foreign_key_check").fetchall()
                        if violations:
                            raise RuntimeError(f"{migration.name} introduced foreign key violations")
                        self._connection.execute(
                            "INSERT INTO schema_migrations (version) VALUES (?)",
                            (migration.name,),
                        )
                        self._connection.commit()
                    else:
                        self._connection.executescript(
                            "BEGIN IMMEDIATE;\n"
                            f"{script}\n"
                            "INSERT INTO schema_migrations (version) "
                            f"VALUES ('{version}');\n"
                            "COMMIT;"
                        )
                except BaseException:
                    self._connection.rollback()
                    raise
                finally:
                    if migration.name == "004_sheets_authority_upgrade.sql":
                        self._connection.execute("PRAGMA legacy_alter_table = OFF")
                    if foreign_keys_disabled:
                        self._connection.execute("PRAGMA foreign_keys = ON")
                    if migration.name == "002_canonical_authority.sql":
                        violations = self._connection.execute("PRAGMA foreign_key_check").fetchall()
                        if violations:
                            raise RuntimeError(f"{migration.name} introduced foreign key violations")

    def _prepare_canonical_authority_upgrade(self) -> None:
        """Add columns SQLite cannot add conditionally from a SQL migration."""
        callback_columns = {str(row["name"]) for row in self._connection.execute("PRAGMA table_info(callback_tokens)")}
        if "revoked_at" not in callback_columns:
            self._connection.execute("ALTER TABLE callback_tokens ADD COLUMN revoked_at TEXT")

    def _assert_sheets_authority_upgrade_supported(self, migration_path: Path) -> None:
        """Fail closed when an applied 003 is not the supported authority shape."""
        authority_tables = (
            "sheet_bootstraps",
            "sheet_handoff_bindings",
            "sheet_operation_events",
            "sheet_operation_leases",
            "sheet_operation_probes",
            "sheet_operation_settlements",
            "sheet_remote_operations",
            "sheet_target_bindings",
        )
        placeholders = ",".join("?" for _ in authority_tables)
        query = (
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE type IN ('table','trigger','index') AND tbl_name IN ("
            f"{placeholders}) AND sql IS NOT NULL ORDER BY type,name"
        )
        installed = [tuple(row) for row in self._connection.execute(query, authority_tables)]

        expected_connection = sqlite3.connect(":memory:")
        try:
            expected_connection.create_function(
                "sha256_hex",
                1,
                lambda value: sha256(bytes(value)).hexdigest(),
                deterministic=True,
            )
            expected_connection.create_function("lease_owner_hash", 0, lambda: None)
            expected_connection.create_function("aware_epoch_us", 1, aware_epoch_us, deterministic=True)
            expected_connection.executescript(
                "CREATE TABLE generations(id INTEGER PRIMARY KEY);"
                "CREATE TABLE decision_events(id INTEGER PRIMARY KEY);" + migration_path.read_text(encoding="utf-8")
            )
            expected = [tuple(row) for row in expected_connection.execute(query, authority_tables)]
        finally:
            expected_connection.close()
        if installed != expected:
            raise RuntimeError("unsupported applied 003 Sheets authority schema; automatic upgrade refused")

    def _authorize_lease(self, owner_token: str) -> None:
        """Authorize one transaction to write events for the named lease owner."""
        self._lease_authority_hash = sha256(owner_token.encode()).hexdigest()

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Yield the connection inside an all-or-nothing transaction."""
        begin = "BEGIN IMMEDIATE" if immediate else "BEGIN"
        with self._lock:
            if self._connection.in_transaction:
                raise RuntimeError("Nested transactions are not supported")
            self._connection.execute(begin)
            try:
                yield self._connection
                self._connection.commit()
            except BaseException:
                self._connection.rollback()
                raise
            finally:
                self._lease_authority_hash = None

    def execute(self, sql: str, parameters: Sequence[Any] = ()) -> sqlite3.Cursor:
        """Execute one statement. Writes require an explicit transaction."""
        with self._lock:
            if not self._connection.in_transaction and not sql.lstrip().upper().startswith("SELECT"):
                raise RuntimeError("Writes require storage.transaction()")
            return self._connection.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> sqlite3.Cursor:
        """Execute repeated writes inside an existing transaction."""
        with self._lock:
            if not self._connection.in_transaction:
                raise RuntimeError("Writes require storage.transaction()")
            return self._connection.executemany(sql, parameters)

    def fetch_one(self, sql: str, parameters: Sequence[Any] = ()) -> sqlite3.Row | None:
        """Return one row or ``None``."""
        with self._lock:
            row = self._connection.execute(sql, parameters).fetchone()
        if row is None:
            return None
        if not isinstance(row, sqlite3.Row):
            raise TypeError("SQLite connection must use sqlite3.Row row_factory")
        return row

    def fetch_all(self, sql: str, parameters: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Return all result rows."""
        with self._lock:
            return list(self._connection.execute(sql, parameters))

    def latest_observations(self) -> tuple[SourceObservation, ...]:
        """Rehydrate the newest immutable version of every durable source post."""
        rows = self.fetch_all(
            "SELECT post.channel_id, post.external_post_id, post.published_at AS post_published_at, "
            "version.body, version.kind, version.sponsored, version.urls_json, version.media_json, "
            "observation.channel_handle, observation.published_at AS observation_published_at, "
            "observation.edited_at, observation.engagement_json, observation.observed_at AS observation_observed_at, "
            "version.conflicts_json FROM source_posts post JOIN source_post_observations observation "
            "ON observation.id=(SELECT MAX(current.id) FROM source_post_observations current "
            "WHERE current.source_post_id=post.id) JOIN source_post_versions version "
            "ON version.id=observation.source_post_version_id "
            "ORDER BY post.channel_id, CAST(post.external_post_id AS INTEGER), post.external_post_id"
        )
        observations: list[SourceObservation] = []
        for row in rows:
            urls = json.loads(str(row["urls_json"]))
            media = json.loads(str(row["media_json"]))
            engagement = json.loads(str(row["engagement_json"])) if row["engagement_json"] else {}
            published_at = row["observation_published_at"] or row["post_published_at"]
            if not isinstance(published_at, str):
                raise RuntimeError("source observation has no published timestamp")
            kind = str(row["kind"])
            if kind not in {"message", "service", "deleted", "unsupported"}:
                raise RuntimeError("source version has an invalid message kind")
            observations.append(
                SourceObservation(
                    channel_id=str(row["channel_id"]),
                    channel_handle=str(row["channel_handle"]),
                    external_post_id=str(row["external_post_id"]),
                    published_at=_parse_timestamp(published_at),
                    text=str(row["body"]),
                    edited_at=_parse_timestamp(str(row["edited_at"])) if row["edited_at"] else None,
                    observed_at=_parse_timestamp(str(row["observation_observed_at"]))
                    if row["observation_observed_at"]
                    else None,
                    kind=cast(MessageKind, kind),
                    sponsored=bool(row["sponsored"]),
                    urls=tuple(UrlCandidate(**item) for item in urls),
                    media=tuple(Media(**item) for item in media),
                    engagement=Engagement(
                        views=engagement.get("views"),
                        reactions=engagement.get("reactions"),
                        forwards=engagement.get("forwards"),
                    ),
                    conflicts=tuple(str(item) for item in json.loads(str(row["conflicts_json"]))),
                )
            )
        return tuple(observations)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> Storage:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class CollectionResult:
    """The durable state reached by one bounded collection invocation."""

    persisted: int
    interval_complete: bool
    cursor_promoted: bool


class DurableCollection:
    """Persist cap-safe source collection without advancing a cursor prematurely."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def collect_channel(
        self,
        collector: Any,
        channel: object,
        *,
        now: datetime,
        page_size: int = 100,
        initial_lookback: timedelta = timedelta(hours=24),
        overlap: timedelta = timedelta(hours=72),
        overlap_message_ids: int = 100,
        max_overlap_pages: int = 10,
        crash_after_page: bool = False,
    ) -> CollectionResult:
        """Collect one fixed message-ID interval and promote it only when complete."""
        if page_size < 1 or max_overlap_pages < 1 or overlap_message_ids < 1:
            raise ValueError("page_size, max_overlap_pages, and overlap_message_ids must be positive")
        now = _utc(now)
        channel_id = str(getattr(channel, "id", channel))
        interval = self.storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (channel_id,))
        if interval is None:
            cursor = self.storage.fetch_one(
                "SELECT published_at, external_post_id FROM collection_cursors WHERE channel_id=?",
                (channel_id,),
            )
            floor = _parse_timestamp(cursor["published_at"]) if cursor is not None else now - initial_lookback
            base_message_id = _message_id(cursor["external_post_id"]) if cursor is not None else 0
            fixed_upper = collector.latest_message_id(channel)
            upper_message_id = base_message_id if fixed_upper is None else _message_id(fixed_upper)
            if upper_message_id < base_message_id:
                raise RuntimeError("collector newest message ID predates the collection frontier")
            with self.storage.transaction() as connection:
                connection.execute(
                    "INSERT INTO collection_intervals("
                    "channel_id, floor_at, upper_bound_at, base_message_id, upper_message_id, next_message_id, floor_applies"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        channel_id,
                        _timestamp(floor),
                        _timestamp(now),
                        base_message_id,
                        upper_message_id,
                        base_message_id,
                        int(cursor is None),
                    ),
                )
            interval = self.storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (channel_id,))
            assert interval is not None

        floor = _parse_timestamp(interval["floor_at"])
        upper = _parse_timestamp(interval["upper_bound_at"])
        base_message_id = int(interval["base_message_id"])
        upper_message_id = int(interval["upper_message_id"])
        next_message_id = int(interval["next_message_id"])
        if bool(interval["page_complete"]):
            page: tuple[Any, ...] = ()
        else:
            page_floor = floor if bool(interval["floor_applies"]) else None
            page, has_more = self._page(collector, channel, page_floor, next_message_id, upper_message_id, page_size)
            self._persist_page(page, now)
            with self.storage.transaction() as connection:
                if has_more:
                    if not page:
                        raise RuntimeError("collector reported another page without a continuation ID")
                    connection.execute(
                        "UPDATE collection_intervals SET next_message_id=?, updated_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                        (_message_id(page[-1].external_post_id), channel_id),
                    )
                else:
                    connection.execute(
                        "UPDATE collection_intervals SET page_complete=1, updated_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                        (channel_id,),
                    )

        if crash_after_page:
            raise RuntimeError("deterministic collection crash after committed page")

        interval = self.storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (channel_id,))
        assert interval is not None
        if not bool(interval["page_complete"]):
            return CollectionResult(len(page), False, False)

        overlap_floor = max(floor, upper - overlap) if bool(interval["floor_applies"]) else upper - overlap
        overlap_after = interval["overlap_next_message_id"]
        overlap_persisted, overlap_complete = self._reconcile(
            collector,
            channel,
            overlap_floor,
            max(0, base_message_id - overlap_message_ids) if overlap_after is None else int(overlap_after),
            base_message_id,
            page_size,
            max_overlap_pages,
            observed_at=now,
            interval_channel_id=channel_id,
        )
        if overlap_complete:
            with self.storage.transaction() as connection:
                connection.execute(
                    "INSERT INTO collection_cursors(channel_id, published_at, external_post_id) VALUES (?, ?, ?) "
                    "ON CONFLICT(channel_id) DO UPDATE SET published_at=excluded.published_at, "
                    "external_post_id=excluded.external_post_id, updated_at=CURRENT_TIMESTAMP",
                    (channel_id, _timestamp(upper), str(upper_message_id)),
                )
                connection.execute("DELETE FROM collection_intervals WHERE channel_id=?", (channel_id,))
            return CollectionResult(len(page) + overlap_persisted, True, True)
        return CollectionResult(len(page) + overlap_persisted, False, False)

    def reconcile_channel(
        self,
        collector: Any,
        channel: object,
        *,
        lower_bound: datetime,
        upper_bound: datetime,
        page_size: int = 100,
        max_pages: int = 10,
        observed_at: datetime | None = None,
    ) -> int:
        """Perform a bounded deep reconciliation without changing normal cursors."""
        if page_size < 1 or max_pages < 1:
            raise ValueError("page_size and max_pages must be positive")
        lower_bound = _utc(lower_bound)
        upper_bound = _utc(upper_bound)
        observed_at = _utc(observed_at) if observed_at is not None else datetime.now(UTC)
        if lower_bound > upper_bound:
            raise ValueError("reconciliation lower_bound must not exceed upper_bound")
        upper_message_id = collector.latest_message_id(channel)
        if upper_message_id is None:
            return 0
        persisted, _ = self._reconcile(
            collector,
            channel,
            lower_bound,
            0,
            _message_id(upper_message_id),
            page_size,
            max_pages,
            observed_at=observed_at,
            upper_bound=upper_bound,
        )
        return persisted

    def _reconcile(
        self,
        collector: Any,
        channel: object,
        floor: datetime,
        after_message_id: int,
        upper_message_id: int,
        page_size: int,
        max_pages: int,
        *,
        interval_channel_id: str | None = None,
        upper_bound: datetime | None = None,
        observed_at: datetime | None = None,
    ) -> tuple[int, bool]:
        observed_at = _utc(observed_at) if observed_at is not None else datetime.now(UTC)
        persisted = 0
        for _ in range(max_pages):
            page, has_more = self._page(
                collector,
                channel,
                floor,
                after_message_id,
                upper_message_id,
                page_size,
                upper_bound=upper_bound,
            )
            self._persist_page(page, observed_at)
            persisted += len(page)
            if not has_more:
                return persisted, True
            if not page:
                raise RuntimeError("collector reported another page without a continuation ID")
            after_message_id = _message_id(page[-1].external_post_id)
            if interval_channel_id is not None:
                with self.storage.transaction() as connection:
                    connection.execute(
                        "UPDATE collection_intervals SET overlap_next_message_id=?, "
                        "updated_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                        (after_message_id, interval_channel_id),
                    )
        return persisted, False

    def _page(
        self,
        collector: Any,
        channel: object,
        floor: datetime | None,
        after_message_id: int,
        upper_message_id: int,
        page_size: int,
        *,
        upper_bound: datetime | None = None,
    ) -> tuple[tuple[Any, ...], bool]:
        request: dict[str, Any] = {
            "lower_bound": floor,
            "min_message_id": after_message_id,
            "max_message_id": upper_message_id,
            "limit": page_size + 1,
        }
        if upper_bound is not None:
            request["upper_bound"] = upper_bound
        rows = collector.collect(channel, **request)
        ordered = tuple(
            sorted(
                (
                    row
                    for row in rows
                    if (floor is None or floor <= _utc(row.published_at))
                    and (upper_bound is None or _utc(row.published_at) <= upper_bound)
                    and after_message_id < _message_id(row.external_post_id) <= upper_message_id
                ),
                key=lambda row: _message_id(row.external_post_id),
            )
        )
        return ordered[:page_size], len(ordered) > page_size

    def _persist_page(self, observations: Sequence[Any], observed_at: datetime) -> None:
        with self.storage.transaction() as connection:
            for observation in observations:
                persist_observation(connection, observation, observed_at)


def persist_observation(
    connection: sqlite3.Connection,
    observation: Any,
    observed_at: datetime,
    *,
    return_observation_id: bool = False,
) -> int:
    published_at = _timestamp(_utc(observation.published_at))
    handle = observation.channel_handle.lstrip("@")
    source_url = f"https://t.me/{handle}/{observation.external_post_id}" if handle else None
    connection.execute(
        "INSERT OR IGNORE INTO source_posts(channel_id, external_post_id, published_at, source_url) VALUES (?, ?, ?, ?)",
        (observation.channel_id, observation.external_post_id, published_at, source_url),
    )
    post = connection.execute(
        "SELECT id FROM source_posts WHERE channel_id=? AND external_post_id=?",
        (observation.channel_id, observation.external_post_id),
    ).fetchone()
    assert post is not None
    material_payload = {
        "text": observation.text,
        "kind": observation.kind,
        "sponsored": observation.sponsored,
        "urls": [
            {"url": url.url, "source": url.source, "title": url.title, "description": url.description}
            for url in observation.urls
        ],
        "media": [
            {"kind": media.kind, "caption": media.caption, "identity": media.identity, "is_service": media.is_service}
            for media in observation.media
        ],
        "conflicts": list(observation.conflicts),
    }
    engagement_payload = {
        "views": observation.engagement.views,
        "reactions": observation.engagement.reactions,
        "forwards": observation.engagement.forwards,
    }
    version_key = _payload_hash(material_payload)
    connection.execute(
        "INSERT OR IGNORE INTO source_post_versions("
        "source_post_id, version_key, body, media_json, kind, sponsored, urls_json, conflicts_json"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            post["id"],
            version_key,
            observation.text,
            json.dumps(material_payload["media"], ensure_ascii=False, separators=(",", ":")),
            observation.kind,
            int(observation.sponsored),
            json.dumps(material_payload["urls"], ensure_ascii=False, separators=(",", ":")),
            json.dumps(material_payload["conflicts"], ensure_ascii=False, separators=(",", ":")),
        ),
    )
    version = connection.execute(
        "SELECT id FROM source_post_versions WHERE source_post_id=? AND version_key=?",
        (post["id"], version_key),
    ).fetchone()
    assert version is not None
    snapshot_payload = {
        "version_key": version_key,
        "channel_handle": observation.channel_handle,
        "published_at": published_at,
        "edited_at": _timestamp(_utc(observation.edited_at)) if observation.edited_at else None,
        "observed_at": _timestamp(_utc(observation.observed_at or observed_at)),
        "engagement": engagement_payload,
    }
    observation_key = _payload_hash(snapshot_payload)
    connection.execute(
        "INSERT OR IGNORE INTO source_post_observations("
        "source_post_id, source_post_version_id, observation_key, observed_at, channel_handle, published_at, "
        "edited_at, engagement_json"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            post["id"],
            version["id"],
            observation_key,
            snapshot_payload["observed_at"],
            observation.channel_handle,
            published_at,
            snapshot_payload["edited_at"],
            json.dumps(engagement_payload, ensure_ascii=False, separators=(",", ":")),
        ),
    )
    persisted = connection.execute(
        "SELECT id FROM source_post_observations WHERE source_post_id=? AND observation_key=?",
        (post["id"], observation_key),
    ).fetchone()
    assert persisted is not None
    return int(persisted["id"] if return_observation_id else version["id"])


def has_newer_material_source(connection: sqlite3.Connection, source_version_ids: Sequence[int]) -> bool:
    """Return whether bindings differ from each post's latest observation."""
    if not source_version_ids:
        return True
    marks = ",".join("?" for _ in source_version_ids)
    return (
        connection.execute(
            "SELECT 1 FROM source_post_versions bound "
            f"WHERE bound.id IN ({marks}) AND EXISTS ("
            "SELECT 1 FROM source_post_observations latest "
            "WHERE latest.source_post_id=bound.source_post_id "
            "AND latest.id=(SELECT MAX(current.id) FROM source_post_observations current "
            "WHERE current.source_post_id=bound.source_post_id) "
            "AND latest.source_post_version_id != bound.id) LIMIT 1",
            source_version_ids,
        ).fetchone()
        is not None
    )


def _payload_hash(payload: object) -> str:
    return sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("collection timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat()


def _parse_timestamp(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))


def _message_id(value: object) -> int:
    if not isinstance(value, (int, str, bytes, bytearray)):
        raise ValueError("collector message IDs must be integers")
    try:
        message_id = int(value)
    except ValueError as exc:
        raise ValueError("collector message IDs must be integers") from exc
    if message_id < 0:
        raise ValueError("collector message IDs must be nonnegative")
    return message_id

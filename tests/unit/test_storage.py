from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest

from newsbot.storage import Storage


def test_open_initializes_schema_and_applies_migration_once(tmp_path: Path) -> None:
    database = tmp_path / "nested" / "newsbot.sqlite"

    with Storage.open(database) as storage:
        tables = {row["name"] for row in storage.fetch_all("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert {
            "schema_migrations",
            "source_posts",
            "source_post_versions",
            "candidates",
            "export_outbox",
            "sheet_handoffs",
            "sheet_remote_operations",
            "sheet_operation_events",
            "sheet_operation_leases",
            "telegram_update_cursors",
            "automation_cutovers",
            "telegram_notification_outbox",
            "automation_stream_leases",
        } <= tables
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM schema_migrations")["count"] == 9

    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM schema_migrations")["count"] == 9


def test_manual_review_has_one_atomic_service_path() -> None:
    assert not hasattr(Storage, "record_manual_local_approval")
    assert not hasattr(Storage, "create_manual_local_exports")


def test_telegram_outbox_and_attempt_identity_are_immutable_to_direct_sql(tmp_path: Path) -> None:
    database = tmp_path / "newsbot.sqlite"
    with Storage.open(database):
        pass

    connection = sqlite3.connect(database)
    try:
        digest = "a" * 64
        connection.execute(
            "INSERT INTO telegram_notification_outbox("
            "id,audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
            ") VALUES(1,1,1,'candidate',1,'source-set',?,'pending')",
            (digest,),
        )
        connection.execute(
            "INSERT INTO telegram_notification_chunks("
            "id,notification_id,chunk_index,utf16_length,template_digest,has_buttons"
            ") VALUES(1,1,0,1,?,0)",
            (digest,),
        )
        connection.execute(
            "INSERT INTO telegram_chunk_attempts("
            "id,chunk_id,ordinal,owner_hash,fence,request_sha256,state,prepared_at"
            ") VALUES(1,1,1,'owner',1,?,'prepared','2026-08-02T00:00:00+00:00')",
            (digest,),
        )

        connection.execute(
            "UPDATE telegram_notification_outbox SET state='claimed',claimed_at='2026-08-02T00:01:00+00:00' WHERE id=1"
        )
        connection.execute(
            "UPDATE telegram_chunk_attempts SET state='possibly_sent',marked_at='2026-08-02T00:01:00+00:00' WHERE id=1"
        )
        for statement in (
            "UPDATE telegram_chunk_attempts SET marked_at='2026-08-02T00:02:00+00:00' WHERE id=1",
            "UPDATE telegram_chunk_attempts SET settled_at='2026-08-02T00:02:00+00:00' WHERE id=1",
            "UPDATE telegram_chunk_attempts SET accepted_message_id=9 WHERE id=1",
        ):
            with pytest.raises(
                sqlite3.IntegrityError,
                match="chunk attempt evidence requires state transition",
            ):
                connection.execute(statement)
        connection.execute(
            "UPDATE telegram_chunk_attempts "
            "SET state='accepted',accepted_message_id=9,settled_at='2026-08-02T00:02:00+00:00' "
            "WHERE id=1"
        )
        for statement in (
            "UPDATE telegram_chunk_attempts SET marked_at='2026-08-02T00:03:00+00:00' WHERE id=1",
            "UPDATE telegram_chunk_attempts SET settled_at='2026-08-02T00:03:00+00:00' WHERE id=1",
            "UPDATE telegram_chunk_attempts SET accepted_message_id=10 WHERE id=1",
        ):
            with pytest.raises(
                sqlite3.IntegrityError,
                match="chunk attempt evidence requires state transition",
            ):
                connection.execute(statement)

        for statement, parameters in (
            ("UPDATE telegram_notification_outbox SET audience_binding_id=2 WHERE id=1", ()),
            ("UPDATE telegram_notification_outbox SET cutover_id=2 WHERE id=1", ()),
            ("UPDATE telegram_notification_outbox SET subject_digest=? WHERE id=1", ("b" * 64,)),
        ):
            with pytest.raises(sqlite3.IntegrityError, match="notification identity is immutable"):
                connection.execute(statement, parameters)
        with pytest.raises(sqlite3.IntegrityError, match="notifications cannot be deleted"):
            connection.execute("DELETE FROM telegram_notification_outbox WHERE id=1")
        for statement, parameters in (
            ("UPDATE telegram_chunk_attempts SET chunk_id=2 WHERE id=1", ()),
            ("UPDATE telegram_chunk_attempts SET owner_hash='other-owner' WHERE id=1", ()),
            ("UPDATE telegram_chunk_attempts SET fence=2 WHERE id=1", ()),
            ("UPDATE telegram_chunk_attempts SET request_sha256=? WHERE id=1", ("b" * 64,)),
        ):
            with pytest.raises(sqlite3.IntegrityError, match="chunk attempt identity is immutable"):
                connection.execute(statement, parameters)
        with pytest.raises(sqlite3.IntegrityError, match="chunk attempts cannot be deleted"):
            connection.execute("DELETE FROM telegram_chunk_attempts WHERE id=1")
    finally:
        connection.close()


def test_storage_enforces_transaction_unique_and_foreign_key_constraints(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "newsbot.sqlite") as storage:
        with pytest.raises(RuntimeError, match="Writes require storage.transaction"):
            storage.execute(
                "INSERT INTO runs (run_key, mode, status) VALUES (?, ?, ?)", ("run-1", "fixture", "started")
            )

        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO source_posts (channel_id, external_post_id) VALUES (?, ?)", ("official-ai", "101")
            )

        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO source_posts (channel_id, external_post_id) VALUES (?, ?)", ("official-ai", "101")
            )

        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO source_post_versions (source_post_id, version_key, body) VALUES (?, ?, ?)",
                (999, "v1", "unlinked version"),
            )


def test_sheets_migration_preserves_populated_database(tmp_path: Path) -> None:
    database = tmp_path / "populated.sqlite"
    initial_sql = (Path(__file__).parents[2] / "src" / "newsbot" / "migrations" / "001_initial.sql").read_text(
        encoding="utf-8"
    )
    connection = sqlite3.connect(database)
    try:
        connection.executescript(initial_sql)
        connection.execute("INSERT INTO schema_migrations(version) VALUES ('001_initial.sql')")
        connection.execute("INSERT INTO runs(run_key, mode, status) VALUES ('existing', 'fixture', 'done')")
        connection.execute("INSERT INTO source_posts(channel_id, external_post_id) VALUES ('existing', '1')")
        connection.commit()
    finally:
        connection.close()

    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT run_key FROM runs WHERE id=1")["run_key"] == "existing"
        assert (
            storage.fetch_one("SELECT name FROM sqlite_master WHERE type='table' AND name='sheet_handoffs'") is not None
        )
        assert storage.fetch_all("PRAGMA foreign_key_check") == []


def test_hourly_news_migration_rejects_unsupported_007_before_ddl(tmp_path: Path) -> None:
    database = tmp_path / "unsupported-007.sqlite"
    migrations = Path(__file__).parents[2] / "src" / "newsbot" / "migrations"
    storage = Storage(database)
    try:
        for path in sorted(migrations.glob("00[1-7]_*.sql")):
            script = path.read_text(encoding="utf-8")
            if path.name == "002_canonical_authority.sql":
                storage._prepare_canonical_authority_upgrade()
                storage._connection.execute("PRAGMA foreign_keys=OFF")
            if path.name == "004_sheets_authority_upgrade.sql":
                script = script.replace("__HANDOFF_TARGET_EXPR__", "h.target_binding_id")
            storage._connection.executescript(script)
            storage._connection.execute("INSERT INTO schema_migrations(version) VALUES(?)", (path.name,))
            storage._connection.commit()
            if path.name == "002_canonical_authority.sql":
                storage._connection.execute("PRAGMA foreign_keys=ON")
        storage._connection.execute("DROP TRIGGER telegram_outbox_no_delete")
        storage._connection.commit()
    finally:
        storage.close()

    with pytest.raises(RuntimeError, match="Unsupported migration-007 outbox schema"):
        Storage.open(database)

    connection = sqlite3.connect(database)
    try:
        assert (
            connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version='008_hourly_news_eligibility.sql'"
            ).fetchone()
            is None
        )
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ambiguous_digest_windows'"
            ).fetchone()
            is None
        )
    finally:
        connection.close()


def test_constructor_phase_guard_names_connect_udf_pre_wal_post_wal_transactions_and_close(
    tmp_path: Path,
) -> None:
    events: list[tuple[str, str]] = []

    @contextmanager
    def guard(phase: str):
        events.append((phase, "before"))
        try:
            yield
        finally:
            events.append((phase, "after"))

    storage = Storage(tmp_path / "guarded.sqlite", phase_guard=guard)
    assert events == [
        ("connect", "before"),
        ("connect", "after"),
        ("udf", "before"),
        ("udf", "after"),
        ("pre_wal", "before"),
        ("pre_wal", "after"),
        ("post_wal", "before"),
        ("post_wal", "after"),
    ]

    assert storage.fetch_one("SELECT 1 AS value")["value"] == 1
    with storage.transaction() as connection:
        connection.execute("CREATE TEMP TABLE guard_probe(value INTEGER)")
    storage.close()

    assert events[-8:] == [
        ("fetch_one", "before"),
        ("fetch_one", "after"),
        ("transaction_begin", "before"),
        ("transaction_begin", "after"),
        ("transaction_commit", "before"),
        ("transaction_commit", "after"),
        ("close", "before"),
        ("close", "after"),
    ]


def test_constructor_guard_refuses_before_connect_without_creating_database(tmp_path: Path) -> None:
    database = tmp_path / "blocked.sqlite"

    @contextmanager
    def mismatch(phase: str):
        raise RuntimeError("state_path_changed")
        yield

    with pytest.raises(RuntimeError, match="state_path_changed"):
        Storage(database, phase_guard=mismatch)

    assert not database.exists()


def test_constructor_guard_closes_connection_when_post_connect_attestation_fails(tmp_path: Path) -> None:

    @contextmanager
    def guard(phase: str):
        yield
        if phase == "connect":
            raise RuntimeError("state_path_changed")

    with pytest.raises(RuntimeError, match="state_path_changed"):
        Storage(tmp_path / "guarded.sqlite", phase_guard=guard)

    with sqlite3.connect(tmp_path / "guarded.sqlite") as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] != "wal"


def test_constructor_guard_refuses_before_wal_configuration(tmp_path: Path) -> None:
    database = tmp_path / "blocked.sqlite"

    @contextmanager
    def mismatch(phase: str):
        if phase == "pre_wal":
            raise RuntimeError("state_path_changed")
        yield

    with pytest.raises(RuntimeError, match="state_path_changed"):
        Storage(database, phase_guard=mismatch)

    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] != "wal"


def test_constructor_guard_refuses_after_wal_configuration(tmp_path: Path) -> None:
    database = tmp_path / "blocked.sqlite"

    @contextmanager
    def mismatch(phase: str):
        yield
        if phase == "post_wal":
            raise RuntimeError("state_path_changed")

    with pytest.raises(RuntimeError, match="state_path_changed"):
        Storage(database, phase_guard=mismatch)

    with sqlite3.connect(database) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"

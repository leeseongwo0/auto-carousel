from __future__ import annotations

import sqlite3
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
        } <= tables
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM schema_migrations")["count"] == 5

    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM schema_migrations")["count"] == 5


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

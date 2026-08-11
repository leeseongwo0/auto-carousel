from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot import v2_cli
from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.v2_workflow import V2Workflow, V2WorkflowError
from tests.v2_support import create_candidate

_BASELINE_SCHEMA = """
CREATE TABLE v2_metadata (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
);
INSERT INTO v2_metadata(key,value)
VALUES('schema','newsbot-v2-workflow-v1');
CREATE TABLE v2_telegram_cursor (
    stream INTEGER PRIMARY KEY CHECK(stream=1),
    next_offset INTEGER NOT NULL CHECK(next_offset>=0)
);
CREATE TABLE v2_remote_effects (
    entity_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    detail TEXT NOT NULL DEFAULT '',
    receipt_id TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY(entity_id,stage)
);
CREATE TABLE v2_observations (
    identity TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    external_post_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE v2_candidates (
    id TEXT PRIMARY KEY,
    observation_identity TEXT NOT NULL UNIQUE
        REFERENCES v2_observations(identity),
    state TEXT NOT NULL,
    policy_outcome TEXT NOT NULL,
    policy_reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE v2_drafts (
    id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL UNIQUE REFERENCES v2_candidates(id),
    content TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE v2_manual_reviews (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(entity_id,reason)
);
CREATE TABLE v2_callbacks (
    token_hash TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    consumed_at TEXT
);
CREATE TABLE v2_codex_requests (
    digest TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL UNIQUE REFERENCES v2_candidates(id),
    request_bytes BLOB NOT NULL,
    status TEXT NOT NULL,
    output_bytes BLOB,
    output_digest TEXT,
    error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE v2_codex_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_digest TEXT NOT NULL REFERENCES v2_codex_requests(digest),
    number INTEGER NOT NULL,
    status TEXT NOT NULL,
    error_code TEXT,
    created_at TEXT NOT NULL,
    settled_at TEXT,
    UNIQUE(request_digest,number)
);
"""


def _create_receipt_first_baseline(path) -> tuple[str, str]:
    delivered = "365610e753af078c5674d2fb"
    pending = "1f3276d77e1a591a27933864"
    timestamp = "2026-08-09T01:00:00+00:00"
    canonical_url = "https://example.test/brazil-transfer-controls?utm_source=telegram"
    payloads = {
        delivered: {
            "channel_id": "the-block",
            "channel_handle": "the_block_crypto",
            "external_post_id": "delivered",
            "published_at": timestamp,
            "text": "Brazil tightens crypto transfer controls.",
            "urls": [{"url": canonical_url}],
        },
        pending: {
            "channel_id": "the-block",
            "channel_handle": "the_block_crypto",
            "external_post_id": "pending",
            "published_at": timestamp,
            "text": "Brazil tightens crypto transfer controls.",
            "urls": [{"url": ("https://example.test/brazil-transfer-controls")}],
        },
    }
    with sqlite3.connect(path) as connection:
        connection.executescript(_BASELINE_SCHEMA)
        for candidate_id, payload in payloads.items():
            identity = f"{payload['channel_id']}:{payload['external_post_id']}"
            connection.execute(
                "INSERT INTO v2_observations VALUES(?,?,?,?,?)",
                (
                    identity,
                    payload["channel_id"],
                    payload["external_post_id"],
                    json.dumps(payload, sort_keys=True),
                    timestamp,
                ),
            )
            state = "sheet_delivered" if candidate_id == delivered else "pending_candidate"
            connection.execute(
                "INSERT INTO v2_candidates VALUES(?,?,?,?,?,?,?)",
                (
                    candidate_id,
                    identity,
                    state,
                    "candidate",
                    "significant_event",
                    timestamp,
                    timestamp,
                ),
            )
        draft_id = "delivered-draft"
        connection.execute(
            "INSERT INTO v2_drafts VALUES(?,?,?,?,?,?)",
            (
                draft_id,
                delivered,
                "{}",
                "sheet_delivered",
                timestamp,
                timestamp,
            ),
        )
        connection.execute(
            "INSERT INTO v2_remote_effects VALUES(?,?,?,?,?,?,?)",
            (
                draft_id,
                "sheets_delivery",
                1,
                "confirmed",
                "remote_outcome_confirmed",
                "sheet-row-1",
                timestamp,
            ),
        )
        connection.execute(
            "INSERT INTO v2_remote_effects VALUES(?,?,?,?,?,?,?)",
            (
                pending,
                "candidate_notification",
                1,
                "confirmed",
                "remote_outcome_confirmed",
                "telegram-message-1",
                timestamp,
            ),
        )
        connection.execute(
            "INSERT INTO v2_callbacks VALUES(?,?,?,?,?)",
            (
                "a" * 64,
                pending,
                "candidate",
                "2026-08-10T01:00:00+00:00",
                timestamp,
            ),
        )
        connection.commit()
    return delivered, pending


def _create_duplicate_truth_baseline(
    path,
    left_truth: str,
    right_truth: str,
) -> tuple[str, str]:
    candidate_ids = ("candidate-a", "candidate-b")
    timestamp = "2026-08-09T01:00:00+00:00"
    with sqlite3.connect(path) as connection:
        connection.executescript(_BASELINE_SCHEMA)
        for index, (candidate_id, truth) in enumerate(
            zip(
                candidate_ids,
                (left_truth, right_truth),
                strict=True,
            )
        ):
            identity = f"source:{index}"
            payload = {
                "channel_id": "source",
                "external_post_id": str(index),
                "published_at": timestamp,
                "text": f"duplicate evidence {index}",
                "urls": [
                    {
                        "url": (
                            "https://example.test/shared-story?utm_source=test"
                            if index == 0
                            else "https://example.test/shared-story"
                        )
                    }
                ],
            }
            state = {
                "D": "sheet_delivered",
                "U": "pending_candidate",
                "S": "candidate_approved",
                "N": "pending_candidate",
            }[truth]
            connection.execute(
                "INSERT INTO v2_observations VALUES(?,?,?,?,?)",
                (
                    identity,
                    "source",
                    str(index),
                    json.dumps(payload, sort_keys=True),
                    timestamp,
                ),
            )
            connection.execute(
                "INSERT INTO v2_candidates VALUES(?,?,?,?,?,?,?)",
                (
                    candidate_id,
                    identity,
                    state,
                    "candidate",
                    "clear_candidate",
                    timestamp,
                    timestamp,
                ),
            )
            if truth == "D":
                draft_id = f"draft-{index}"
                connection.execute(
                    "INSERT INTO v2_drafts VALUES(?,?,?,?,?,?)",
                    (
                        draft_id,
                        candidate_id,
                        "{}",
                        "sheet_delivered",
                        timestamp,
                        timestamp,
                    ),
                )
                connection.execute(
                    "INSERT INTO v2_remote_effects VALUES(?,?,?,?,?,?,?)",
                    (
                        draft_id,
                        "sheets_delivery",
                        1,
                        "confirmed",
                        "remote_outcome_confirmed",
                        f"sheet-{index}",
                        timestamp,
                    ),
                )
            elif truth == "U":
                connection.execute(
                    "INSERT INTO v2_remote_effects VALUES(?,?,?,?,?,?,?)",
                    (
                        candidate_id,
                        "candidate_notification",
                        1,
                        "pending",
                        "possibly_sent",
                        "",
                        timestamp,
                    ),
                )
        connection.commit()
    return candidate_ids


def test_v2_open_modes_migration_and_readonly_verify(tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    before = db.read_bytes()
    with V2Workflow(db, mode="verify") as workflow:
        assert workflow.verify_invariants() == {
            "candidate_binding_mismatches": 0,
            "delivered_marker_mismatches": 0,
            "tombstone_digest_mismatches": 0,
        }
    assert db.read_bytes() == before
    with V2Workflow(db, mode="migrate"):
        pass


def test_migration_failure_keeps_pre_v2_schema(tmp_path, monkeypatch):
    db = tmp_path / "v1.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    connection = sqlite3.connect(db)
    for table in (
        "v2_observation_dispositions",
        "v2_compaction_tombstones",
        "v2_channel_gaps",
        "v2_candidate_bindings",
        "v2_story_tombstones",
        "v2_story_claims",
        "v2_story_keys",
        "v2_stories",
        "v2_revision_heads",
        "v2_article_snapshots",
        "v2_enrichment_attempts",
        "v2_observation_revisions",
        "v2_channel_cursors",
    ):
        connection.execute(f"DROP TABLE {table}")
    connection.execute("DELETE FROM v2_metadata WHERE key='schema_version'")
    connection.commit()
    connection.close()
    original = V2Workflow._create_delta

    def fail_after_delta(self):
        original(self)
        raise RuntimeError("injected")

    monkeypatch.setattr(V2Workflow, "_create_delta", fail_after_delta)
    with pytest.raises(RuntimeError, match="injected"):
        V2Workflow(db, mode="migrate")
    with sqlite3.connect(db) as check:
        assert check.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone() is None
    with pytest.raises(V2WorkflowError):
        V2Workflow(db, mode="runtime")


def test_v5_gap_rows_migrate_to_bounded_ranges(tmp_path):
    db = tmp_path / "v5.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    with sqlite3.connect(db) as connection:
        connection.execute("DROP TABLE v2_channel_gaps")
        connection.execute(
            """
            CREATE TABLE v2_channel_gaps (
                channel_id TEXT NOT NULL REFERENCES v2_channel_cursors(channel_id),
                message_id INTEGER NOT NULL,
                recorded_at TEXT NOT NULL,
                PRIMARY KEY(channel_id,message_id)
            )
            """
        )
        connection.execute(
            "INSERT INTO v2_channel_cursors(channel_id,new_message_high_water,updated_at) VALUES(?,?,?)",
            ("channel", 12, "2025-01-01T00:00:00+00:00"),
        )
        connection.executemany(
            "INSERT INTO v2_channel_gaps(channel_id,message_id,recorded_at) VALUES(?,?,?)",
            [
                ("channel", 7, "2025-01-01T00:00:00+00:00"),
                ("channel", 8, "2025-01-01T00:00:01+00:00"),
                ("channel", 9, "2025-01-01T00:00:02+00:00"),
                ("channel", 11, "2025-01-01T00:00:03+00:00"),
            ],
        )
        connection.execute("UPDATE v2_metadata SET value='5' WHERE key='schema_version'")
        connection.commit()

    with V2Workflow(db, mode="migrate"):
        pass

    with sqlite3.connect(db) as connection:
        assert connection.execute(
            "SELECT start_message_id,end_message_id FROM v2_channel_gaps ORDER BY start_message_id"
        ).fetchall() == [(7, 9), (11, 11)]


def test_large_sparse_channel_gap_uses_one_bounded_range(tmp_path):
    db = tmp_path / "v2.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        assert (
            workflow.record_new_message_page(
                "channel",
                [],
                upper_message_id=1_000_000_000,
                page_limit=100,
            )
            == ()
        )
        assert workflow.channel_cursor("channel") == (1_000_000_000, None)

    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT start_message_id,end_message_id FROM v2_channel_gaps").fetchall() == [
            (1, 1_000_000_000)
        ]


def test_open_modes_refuse_nonempty_mixed_and_future_databases_without_mutation(
    tmp_path,
):
    legacy = tmp_path / "legacy.sqlite"
    with sqlite3.connect(legacy) as connection:
        connection.execute("CREATE TABLE legacy_data(value TEXT)")
    legacy_before = legacy.read_bytes()
    with pytest.raises(V2WorkflowError, match="empty database"):
        V2Workflow(legacy, mode="create")
    assert legacy.read_bytes() == legacy_before

    mixed = tmp_path / "mixed.sqlite"
    with V2Workflow(mixed, mode="create"):
        pass
    with sqlite3.connect(mixed) as connection:
        connection.execute("CREATE TABLE legacy_data(value TEXT)")
    mixed_before = mixed.read_bytes()
    with pytest.raises(V2WorkflowError, match="mixed"):
        V2Workflow(mixed, mode="runtime")
    with pytest.raises(V2WorkflowError, match="mixed"):
        V2Workflow(mixed, mode="verify")
    assert mixed.read_bytes() == mixed_before

    future = tmp_path / "future.sqlite"
    with V2Workflow(future, mode="create"):
        pass
    with sqlite3.connect(future) as connection:
        connection.execute("UPDATE v2_metadata SET value='999' WHERE key='schema_version'")
    future_before = future.read_bytes()
    with pytest.raises(V2WorkflowError, match="unknown schema version"):
        V2Workflow(future, mode="migrate")
    assert future.read_bytes() == future_before

    missing_index = tmp_path / "missing-index.sqlite"
    with V2Workflow(missing_index, mode="create"):
        pass
    with sqlite3.connect(missing_index) as connection:
        connection.execute("DROP INDEX v2_revisions_created")
    missing_index_before = missing_index.read_bytes()
    with pytest.raises(V2WorkflowError, match="missing required"):
        V2Workflow(missing_index, mode="runtime")
    with pytest.raises(V2WorkflowError, match="missing required"):
        V2Workflow(missing_index, mode="verify")
    assert missing_index.read_bytes() == missing_index_before


def test_baseline_migration_uses_receipt_truth_and_holds_brazil_duplicate(
    tmp_path,
):
    db = tmp_path / "baseline.sqlite"
    delivered, pending = _create_receipt_first_baseline(db)
    with sqlite3.connect(db) as connection:
        effect_count = connection.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0]
        callback_count = connection.execute("SELECT COUNT(*) FROM v2_callbacks").fetchone()[0]

    with V2Workflow(db, mode="migrate") as workflow:
        rows = workflow._db.execute(
            "SELECT c.id,b.story_id,b.held,b.hold_reason,"
            "claim.candidate_id winner,s.delivered_at "
            "FROM v2_candidates c "
            "JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "JOIN v2_stories s ON s.id=b.story_id "
            "LEFT JOIN v2_story_claims claim ON claim.story_id=s.id "
            "WHERE c.id IN (?,?) ORDER BY c.id",
            (delivered, pending),
        ).fetchall()
        by_id = {row["id"]: row for row in rows}
        assert by_id[delivered]["story_id"] == by_id[pending]["story_id"]
        assert by_id[delivered]["winner"] == delivered
        assert by_id[delivered]["delivered_at"] is not None
        assert by_id[pending]["held"] == 1
        assert by_id[pending]["hold_reason"] == f"duplicate_of:{delivered}"
        assert workflow.get_candidate(pending).state == "manual_review"
        assert workflow.next_candidate_pending_notification() is None
        assert workflow.verify_invariants() == {
            "candidate_binding_mismatches": 0,
            "delivered_marker_mismatches": 0,
            "tombstone_digest_mismatches": 0,
        }
        assert workflow._db.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0] == effect_count
        assert workflow._db.execute("SELECT COUNT(*) FROM v2_callbacks").fetchone()[0] == callback_count

    with V2Workflow(db, mode="migrate") as workflow:
        assert (
            workflow._db.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone()[0]
            == V2Workflow.SCHEMA_VERSION
        )


def test_baseline_migration_rolls_back_every_receipt_first_change(
    tmp_path,
    monkeypatch,
):
    db = tmp_path / "baseline-failure.sqlite"
    _create_receipt_first_baseline(db)
    before = db.read_bytes()
    original = V2Workflow._migrate_baseline_receipt_first

    def fail_after_backfill(self):
        original(self)
        raise RuntimeError("migration failure after receipt backfill")

    monkeypatch.setattr(
        V2Workflow,
        "_migrate_baseline_receipt_first",
        fail_after_backfill,
    )
    with pytest.raises(
        RuntimeError,
        match="migration failure after receipt backfill",
    ):
        V2Workflow(db, mode="migrate")
    assert db.read_bytes() == before
    with sqlite3.connect(db) as connection:
        assert {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        } == {
            "v2_metadata",
            "v2_telegram_cursor",
            "v2_remote_effects",
            "v2_observations",
            "v2_candidates",
            "v2_drafts",
            "v2_manual_reviews",
            "v2_callbacks",
            "v2_codex_requests",
            "v2_codex_attempts",
        }
        assert connection.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone() is None


def test_baseline_migration_rejects_partial_known_duplicate_without_mutation(
    tmp_path,
):
    db = tmp_path / "partial-brazil.sqlite"
    delivered, pending = _create_receipt_first_baseline(db)
    with sqlite3.connect(db) as connection:
        identity = connection.execute(
            "SELECT observation_identity FROM v2_candidates WHERE id=?",
            (pending,),
        ).fetchone()[0]
        connection.execute(
            "DELETE FROM v2_callbacks WHERE entity_id=?",
            (pending,),
        )
        connection.execute(
            "DELETE FROM v2_remote_effects WHERE entity_id=?",
            (pending,),
        )
        connection.execute("DELETE FROM v2_candidates WHERE id=?", (pending,))
        connection.execute(
            "DELETE FROM v2_observations WHERE identity=?",
            (identity,),
        )
        connection.commit()
    before = db.read_bytes()

    with pytest.raises(
        V2WorkflowError,
        match="requires both known candidates",
    ):
        V2Workflow(db, mode="migrate")

    assert db.read_bytes() == before
    with sqlite3.connect(db) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM v2_candidates WHERE id=?",
                (delivered,),
            ).fetchone()[0]
            == 1
        )


def test_migration_validation_failure_rolls_back_version_and_delta(
    tmp_path,
    monkeypatch,
):
    db = tmp_path / "validation-failure.sqlite"
    _create_receipt_first_baseline(db)
    before = db.read_bytes()

    def reject_validation(self):
        raise V2WorkflowError("injected precommit validation failure")

    monkeypatch.setattr(
        V2Workflow,
        "_validate_migration_before_commit",
        reject_validation,
    )
    with pytest.raises(
        V2WorkflowError,
        match="injected precommit validation failure",
    ):
        V2Workflow(db, mode="migrate")

    assert db.read_bytes() == before


@pytest.mark.parametrize(
    ("left_truth", "right_truth", "winner", "quarantined"),
    [
        ("D", "D", None, True),
        ("D", "U", None, True),
        ("D", "S", "candidate-a", False),
        ("D", "N", "candidate-a", False),
        ("U", "D", None, True),
        ("U", "U", None, True),
        ("U", "S", None, True),
        ("U", "N", None, True),
        ("S", "D", "candidate-b", False),
        ("S", "U", None, True),
        ("S", "S", None, True),
        ("S", "N", "candidate-a", False),
        ("N", "D", "candidate-b", False),
        ("N", "U", None, True),
        ("N", "S", "candidate-b", False),
        ("N", "N", "candidate-b", False),
    ],
)
def test_baseline_duplicate_lattice_is_receipt_first(
    left_truth,
    right_truth,
    winner,
    quarantined,
    tmp_path,
):
    db = tmp_path / f"{left_truth}-{right_truth}.sqlite"
    candidate_ids = _create_duplicate_truth_baseline(
        db,
        left_truth,
        right_truth,
    )
    with sqlite3.connect(db) as connection:
        effects_before = connection.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0]

    with V2Workflow(db, mode="migrate") as workflow:
        rows = workflow._db.execute(
            "SELECT b.candidate_id,b.story_id,b.held,"
            "s.quarantined_at,claim.candidate_id winner "
            "FROM v2_candidate_bindings b "
            "JOIN v2_stories s ON s.id=b.story_id "
            "LEFT JOIN v2_story_claims claim "
            "ON claim.story_id=s.id "
            "WHERE b.candidate_id IN (?,?) "
            "ORDER BY b.candidate_id",
            candidate_ids,
        ).fetchall()
        assert len({row["story_id"] for row in rows}) == 1
        assert all(row["held"] == 1 for row in rows)
        assert all((row["quarantined_at"] is not None) is quarantined for row in rows)
        assert {row["winner"] for row in rows} == {winner}
        assert workflow._db.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0] == effects_before
        assert workflow.next_candidate_pending_notification() is None


@pytest.mark.parametrize("predecessor_version", ("3", "4", "5", "6"))
def test_predecessor_schema_versions_migrate_to_current_schema(
    tmp_path,
    predecessor_version,
):
    db = tmp_path / f"v{predecessor_version}.sqlite"
    with V2Workflow(db, mode="create") as workflow:
        candidate = create_candidate(
            workflow,
            SourceObservation(
                channel_id="predecessor-channel",
                channel_handle="predecessor",
                external_post_id=f"eligible-{predecessor_version}",
                published_at=datetime.now(UTC),
                text=(
                    "OpenAI announced a major enterprise integration with material security and infrastructure details."
                ),
                urls=(UrlCandidate(f"https://example.test/predecessor/{predecessor_version}"),),
            ),
        )
        assert candidate is not None
    with sqlite3.connect(db) as connection:
        connection.execute(
            "INSERT INTO v2_channel_cursors(channel_id,new_message_high_water,updated_at) VALUES(?,?,?)",
            ("predecessor-channel", 41, "2026-08-10T00:00:00+00:00"),
        )
        predecessor_columns = {
            "3": (
                "edit_scan_before_message_id",
                "edit_scan_started_at",
                "edit_scan_max_watermark",
                "edit_scan_max_message_id",
                "edit_sweep_message_id",
            ),
            "4": (
                "edit_scan_before_message_id",
                "edit_scan_started_at",
                "edit_scan_max_watermark",
                "edit_scan_max_message_id",
            ),
            "5": (),
            "6": (),
        }
        for column in predecessor_columns[predecessor_version]:
            connection.execute(f"ALTER TABLE v2_channel_cursors DROP COLUMN {column}")
        connection.execute(
            "UPDATE v2_metadata SET value=? WHERE key='schema_version'",
            (predecessor_version,),
        )

    with V2Workflow(db, mode="migrate") as workflow:
        assert (
            workflow._db.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone()[0]
            == V2Workflow.SCHEMA_VERSION
        )
        assert (
            workflow._db.execute(
                "SELECT new_message_high_water FROM v2_channel_cursors WHERE channel_id='predecessor-channel'"
            ).fetchone()[0]
            == 41
        )
        assert (
            workflow._db.execute(
                "SELECT held FROM v2_candidate_bindings WHERE candidate_id=?",
                (candidate.id,),
            ).fetchone()[0]
            == 1
        )
        assert workflow.next_candidate_pending_notification() is None


def test_migration_preflight_creates_verified_hashed_backup(tmp_path):
    db = tmp_path / "source.sqlite"
    backup = tmp_path / "source.backup.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    with sqlite3.connect(db) as connection:
        connection.execute(
            "INSERT INTO v2_channel_cursors(channel_id,new_message_high_water,updated_at) VALUES(?,?,?)",
            ("backup-channel", 23, "2026-08-10T00:00:00+00:00"),
        )

    preflight = v2_cli._migration_preflight(db, backup)

    assert preflight["backup"] == str(backup.resolve())
    assert preflight["backup_sha256"] == hashlib.sha256(backup.read_bytes()).hexdigest()
    assert preflight["shared_filesystem"] is True
    assert preflight["source_required_free_bytes"] == 4 * (preflight["source_bytes"] + preflight["wal_bytes"])
    assert preflight["backup_required_free_bytes"] == 0
    with sqlite3.connect(backup) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert (
            connection.execute(
                "SELECT new_message_high_water FROM v2_channel_cursors WHERE channel_id='backup-channel'"
            ).fetchone()[0]
            == 23
        )


def test_migration_preflight_rejects_existing_backup_without_mutation(tmp_path):
    db = tmp_path / "source.sqlite"
    backup = tmp_path / "existing.backup.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    backup.write_bytes(b"existing backup")
    source_before = db.read_bytes()
    backup_before = backup.read_bytes()

    with pytest.raises(V2WorkflowError, match="backup path already exists"):
        v2_cli._migration_preflight(db, backup)

    assert db.read_bytes() == source_before
    assert backup.read_bytes() == backup_before


def test_migration_preflight_rejects_insufficient_free_space_without_mutation(
    tmp_path,
    monkeypatch,
):
    db = tmp_path / "source.sqlite"
    backup = tmp_path / "source.backup.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    source_before = db.read_bytes()
    monkeypatch.setattr(
        v2_cli.shutil,
        "disk_usage",
        lambda _: SimpleNamespace(free=0),
    )

    with pytest.raises(V2WorkflowError, match="source filesystem lacks"):
        v2_cli._migration_preflight(db, backup)

    assert db.read_bytes() == source_before
    assert not backup.exists()


def test_migration_preflight_checks_backup_capacity_on_distinct_filesystem(
    tmp_path,
    monkeypatch,
):
    source_dir = tmp_path / "source"
    backup_dir = tmp_path / "backup"
    source_dir.mkdir()
    backup_dir.mkdir()
    database = source_dir / "source.sqlite"
    backup = backup_dir / "source.backup.sqlite"
    with V2Workflow(database, mode="create"):
        pass
    source_before = database.read_bytes()

    monkeypatch.setattr(v2_cli, "_same_filesystem", lambda *_: False)

    def disk_usage(path):
        free = 10**12 if Path(path).resolve() == source_dir.resolve() else 0
        return SimpleNamespace(free=free)

    monkeypatch.setattr(v2_cli.shutil, "disk_usage", disk_usage)

    with pytest.raises(V2WorkflowError, match="backup filesystem lacks"):
        v2_cli._migration_preflight(database, backup)

    assert database.read_bytes() == source_before
    assert not backup.exists()


def test_migration_preflight_rejects_corrupt_source_without_backup(
    tmp_path,
) -> None:
    database = tmp_path / "corrupt.sqlite"
    backup = tmp_path / "corrupt.backup.sqlite"
    database.write_bytes(b"not a SQLite database")
    before = database.read_bytes()

    with pytest.raises(
        V2WorkflowError,
        match="SQLite validation failed",
    ):
        v2_cli._migration_preflight(database, backup)

    assert database.read_bytes() == before
    assert not backup.exists()


def test_migration_deadline_rolls_back_predecessor_schema_and_data(
    tmp_path,
    monkeypatch,
):
    db = tmp_path / "deadline.sqlite"
    with V2Workflow(db, mode="create"):
        pass
    with sqlite3.connect(db) as connection:
        connection.execute(
            "INSERT INTO v2_channel_cursors(channel_id,new_message_high_water,updated_at) VALUES(?,?,?)",
            ("deadline-channel", 67, "2026-08-10T00:00:00+00:00"),
        )
        connection.execute("UPDATE v2_metadata SET value='6' WHERE key='schema_version'")
    before = db.read_bytes()
    clock = iter((100.0, 102.0))
    monkeypatch.setattr(
        "newsbot.v2_workflow.time.monotonic",
        lambda: next(clock),
    )

    with pytest.raises(V2WorkflowError, match="migration transaction deadline exceeded"):
        V2Workflow(db, mode="migrate", migration_deadline_seconds=1)

    assert db.read_bytes() == before
    with sqlite3.connect(db) as connection:
        assert connection.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone()[0] == "6"
        assert (
            connection.execute(
                "SELECT new_message_high_water FROM v2_channel_cursors WHERE channel_id='deadline-channel'"
            ).fetchone()[0]
            == 67
        )

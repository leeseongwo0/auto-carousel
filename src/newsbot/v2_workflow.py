"""Minimal, independent SQLite workflow for Newsbot V2."""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import sqlite3
import time
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, cast

from .ai.structured_copy import draft_from_mapping
from .collectors.base import SourceObservation
from .copywriting import validate_copy
from .v2_article import (
    UnsafeUrlError,
    body_identity,
    canonicalize_url,
    material_character_count,
)
from .v2_observability import (
    CompactionResult,
    CompactionTable,
    EffectStage,
    EffectStatus,
    FetchResult,
    ImmediateAlert,
    LoggingObservabilitySink,
    MetricName,
    ObservabilitySink,
    Queue,
    ThresholdSnapshot,
    evaluate_thresholds,
    event,
)
from .v2_observability import (
    Outcome as ObservedOutcome,
)
from .v2_observability import (
    Reason as ObservedReason,
)
from .v2_policy import V2Outcome


class V2State(StrEnum):
    PENDING_CANDIDATE = "pending_candidate"
    CANDIDATE_APPROVED = "candidate_approved"
    DRAFT_PENDING_APPROVAL = "draft_pending_approval"
    DRAFT_APPROVED = "draft_approved"
    SHEET_DELIVERED = "sheet_delivered"
    MANUAL_REVIEW = "manual_review"


class V2WorkflowError(RuntimeError):
    """Raised when an operation cannot be applied to the current state."""


def _inserted_id(cursor: sqlite3.Cursor) -> int:
    if cursor.lastrowid is None:
        raise V2WorkflowError("SQLite insert did not return a row ID")
    return int(cursor.lastrowid)


@dataclass(frozen=True)
class V2Candidate:
    id: str
    channel_id: str
    external_post_id: str
    state: str
    policy_outcome: str
    policy_reason: str
    observation: dict[str, Any]


@dataclass(frozen=True)
class V2Draft:
    id: str
    candidate_id: str
    content: str
    state: str


@dataclass(frozen=True)
class V2CodexRequest:
    digest: str
    candidate_id: str
    request_bytes: bytes
    status: str
    output_bytes: bytes | None
    output_digest: str | None


@dataclass(frozen=True)
class V2CodexAttempt:
    id: int
    request_digest: str
    number: int
    status: str
    error_code: str | None


@dataclass(frozen=True)
class V2Revision:
    id: int
    identity: str
    generation: int
    digest: str
    ordered_at: str
    is_desired: bool


@dataclass(frozen=True)
class V2EnrichmentLease:
    id: int
    revision_id: int
    generation: int
    attempt_number: int
    owner: str


@dataclass(frozen=True)
class V2StatusItem:
    id: str
    state: str
    revision_id: int | None
    policy_outcome: str
    policy_reason: str
    held: bool
    hold_reason: str | None
    created_at: str
    updated_at: str
    fetch_result: str | None
    fetch_attempts: int


class V2Workflow:
    """SQLite workflow isolated from the legacy Newsbot database."""

    SCHEMA_MARKER = "newsbot-v2-workflow-v1"
    SCHEMA_VERSION = "7"
    OUTBOUND_STAGES = {
        "candidate_notification",
        "draft_generation",
        "draft_notification",
        "sheets_delivery",
    }
    STATUS_AGGREGATE_CAP = 10_000

    def __init__(
        self,
        database: str | Path,
        *,
        mode: Literal["create", "runtime", "migrate", "verify"] = "runtime",
        observability: ObservabilitySink | None = None,
        migration_deadline_seconds: float | None = None,
    ):
        if mode not in {"create", "runtime", "migrate", "verify"}:
            raise ValueError("invalid V2 database open mode")
        if migration_deadline_seconds is not None:
            if mode != "migrate":
                raise ValueError("migration deadline is only valid in migrate mode")
            if migration_deadline_seconds <= 0:
                raise ValueError("migration deadline must be positive")
        self.database = str(database)
        if mode == "verify":
            uri = f"file:{Path(database).absolute()}?mode=ro"
            self._db = sqlite3.connect(uri, uri=True)
        elif mode in {"runtime", "migrate"}:
            uri = f"file:{Path(database).absolute()}?mode=rw"
            self._db = sqlite3.connect(uri, uri=True)
        else:
            self._db = sqlite3.connect(self.database)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._migration_deadline_at = (
            None if migration_deadline_seconds is None else time.monotonic() + migration_deadline_seconds
        )
        self._observability = observability if observability is not None else LoggingObservabilitySink()
        if mode == "verify":
            self._db.execute("PRAGMA query_only = ON")
            self._assert_v2_database()
        elif mode == "create":
            if self._table_names():
                raise V2WorkflowError("create mode requires an empty database")
            self.initialize()
        elif mode == "migrate":
            self.migrate()
        else:
            self._assert_v2_database()

    def _table_names(self) -> set[str]:
        return {
            str(row["name"])
            for row in self._db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='sqlite_sequence'")
        }

    @staticmethod
    def _baseline_table_names() -> set[str]:
        return {
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

    @classmethod
    def _required_table_names(cls) -> set[str]:
        return cls._baseline_table_names() | {
            "v2_observation_revisions",
            "v2_enrichment_attempts",
            "v2_article_snapshots",
            "v2_revision_heads",
            "v2_stories",
            "v2_story_keys",
            "v2_story_claims",
            "v2_story_tombstones",
            "v2_candidate_bindings",
            "v2_channel_cursors",
            "v2_channel_gaps",
            "v2_compaction_tombstones",
            "v2_observation_dispositions",
        }

    def close(self) -> None:
        self._db.close()

    def _emit_policy_and_fetch(
        self,
        *,
        entity: object,
        outcome: str,
        reason: str,
        fetch_result: str,
    ) -> None:
        try:
            observed_outcome = ObservedOutcome(outcome)
        except ValueError:
            observed_outcome = ObservedOutcome.NON_NEWS
        try:
            observed_reason = ObservedReason(reason)
        except ValueError:
            observed_reason = ObservedReason.MANUAL_REVIEW
        fetch_mapping = {
            "success": FetchResult.SUCCESS,
            "unsafe_url": FetchResult.BLOCKED,
            "transient_failure": FetchResult.TRANSIENT_FAILURE,
            "permanent_failure": FetchResult.PERMANENT_FAILURE,
        }
        self._observability.emit(
            event(
                MetricName.POLICY_DECISION,
                labels={
                    "outcome": observed_outcome,
                    "reason": observed_reason,
                },
                entity=entity,
            )
        )
        self._observability.emit(
            event(
                MetricName.FETCH,
                labels={
                    "result": fetch_mapping.get(
                        fetch_result,
                        FetchResult.REJECTED,
                    )
                },
                entity=entity,
            )
        )

    def _emit_effect(
        self,
        entity_id: str,
        stage: str,
        status: str,
    ) -> None:
        try:
            effect_stage = EffectStage(stage)
            effect_status = EffectStatus(status)
        except ValueError:
            return
        self._observability.emit(
            event(
                MetricName.EFFECT,
                labels={
                    "stage": effect_stage,
                    "status": effect_status,
                },
                entity=entity_id,
            )
        )

    def _emit_immediate_alert(
        self,
        alert: ImmediateAlert,
        *,
        entity: object | None = None,
    ) -> None:
        self._observability.emit(
            event(
                MetricName.ALERT,
                labels={"alert": alert},
                entity=entity,
            )
        )

    def _migration_retention_error(
        self,
        message: str,
    ) -> V2WorkflowError:
        self._emit_immediate_alert(
            ImmediateAlert.MIGRATION_RETENTION_MISMATCH,
        )
        return V2WorkflowError(message)

    def _emit_compaction_plan(
        self,
        plan: dict[str, Any],
        *,
        dry_run: bool,
    ) -> None:
        result = (
            CompactionResult.DRY_RUN
            if dry_run
            else (CompactionResult.COMPACTED if int(plan["compacted"]) > 0 else CompactionResult.NOTHING_TO_COMPACT)
        )
        tables = (
            (
                CompactionTable.OBSERVATION_REVISIONS,
                len(plan["hot_cold"]) + len(plan["superseded"]),
            ),
            (CompactionTable.CALLBACKS, int(plan["callbacks"])),
            (CompactionTable.REMOTE_EFFECTS, int(plan["effects"])),
            (CompactionTable.DRAFTS, int(plan["drafts"])),
            (
                CompactionTable.CODEX_REQUESTS,
                int(plan["codex_requests"]),
            ),
        )
        for table, count in tables:
            if count or result is CompactionResult.NOTHING_TO_COMPACT:
                self._observability.emit(
                    event(
                        MetricName.COMPACTION,
                        labels={
                            "table": table,
                            "result": result,
                        },
                    )
                )

    def __enter__(self) -> V2Workflow:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _assert_v2_database(self) -> None:
        tables = self._table_names()
        required = self._required_table_names()
        if not tables or "v2_metadata" not in tables:
            raise V2WorkflowError("refusing to open a database without a Newsbot V2 identity marker")
        if tables != required:
            raise V2WorkflowError("database has an invalid, mixed, future, or extra Newsbot V2 schema")
        marker = self._db.execute("SELECT value FROM v2_metadata WHERE key='schema'").fetchone()
        version = self._db.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone()
        if (
            marker is None
            or marker["value"] != self.SCHEMA_MARKER
            or version is None
            or version["value"] != self.SCHEMA_VERSION
        ):
            raise V2WorkflowError("database has an invalid, mixed, or migration-required Newsbot V2 schema")
        for table, expected_columns in self._required_column_names().items():
            actual_columns = {str(row["name"]) for row in self._db.execute(f"PRAGMA table_info({table})")}
            if actual_columns != expected_columns:
                raise V2WorkflowError(f"database has invalid columns for {table}")
        indexes = {
            str(row["name"])
            for row in self._db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if not self._required_index_names().issubset(indexes):
            raise V2WorkflowError("database is missing required Newsbot V2 indexes")

    @classmethod
    def _required_index_names(cls) -> set[str]:
        return {
            "v2_revisions_identity_order",
            "v2_enrichment_due",
            "v2_candidates_state_created",
            "v2_candidates_created",
            "v2_story_keys_story",
            "v2_callbacks_retention",
            "v2_effects_retention",
            "v2_drafts_state_created",
            "v2_codex_candidate_status",
            "v2_bindings_revision",
            "v2_channel_gaps_channel_message",
            "v2_revisions_created",
            "v2_callbacks_expiry",
            "v2_revisions_due_order",
            "v2_snapshots_created_result",
            "v2_enrichment_settled_status",
            "v2_manual_reviews_created",
            "v2_codex_status_created",
        }

    @classmethod
    def _required_column_names(cls) -> dict[str, set[str]]:
        return {
            "v2_metadata": {"key", "value"},
            "v2_telegram_cursor": {"stream", "next_offset"},
            "v2_remote_effects": {
                "entity_id",
                "stage",
                "attempts",
                "status",
                "detail",
                "receipt_id",
                "updated_at",
            },
            "v2_observations": {
                "identity",
                "channel_id",
                "external_post_id",
                "payload",
                "recorded_at",
            },
            "v2_candidates": {
                "id",
                "observation_identity",
                "state",
                "policy_outcome",
                "policy_reason",
                "created_at",
                "updated_at",
            },
            "v2_drafts": {
                "id",
                "candidate_id",
                "content",
                "state",
                "created_at",
                "updated_at",
            },
            "v2_manual_reviews": {
                "id",
                "entity_id",
                "reason",
                "created_at",
            },
            "v2_callbacks": {
                "token_hash",
                "entity_id",
                "stage",
                "expires_at",
                "consumed_at",
            },
            "v2_codex_requests": {
                "digest",
                "candidate_id",
                "request_bytes",
                "status",
                "output_bytes",
                "output_digest",
                "error_code",
                "created_at",
                "updated_at",
            },
            "v2_codex_attempts": {
                "id",
                "request_digest",
                "number",
                "status",
                "error_code",
                "created_at",
                "settled_at",
            },
            "v2_observation_revisions": {
                "id",
                "identity",
                "generation",
                "digest",
                "payload",
                "ordered_at",
                "observed_at",
                "created_at",
            },
            "v2_enrichment_attempts": {
                "id",
                "revision_id",
                "generation",
                "attempt_number",
                "owner",
                "status",
                "leased_until",
                "next_retry_at",
                "dispatched_at",
                "settled_at",
            },
            "v2_article_snapshots": {
                "id",
                "revision_id",
                "attempt_id",
                "snapshot",
                "result",
                "body_hash",
                "created_at",
            },
            "v2_observation_dispositions": {
                "identity",
                "revision_id",
                "outcome",
                "reason",
                "result",
                "retry_count",
                "url_hash",
                "body_hash",
                "updated_at",
            },
            "v2_revision_heads": {
                "identity",
                "revision_id",
                "generation",
                "digest",
                "ordered_at",
                "observed_at",
            },
            "v2_candidate_bindings": {
                "candidate_id",
                "revision_id",
                "snapshot_id",
                "story_id",
                "held",
                "hold_reason",
            },
            "v2_stories": {
                "id",
                "first_seen_at",
                "last_seen_at",
                "delivered_at",
                "quarantined_at",
                "tombstoned_at",
            },
            "v2_story_keys": {"kind", "key_digest", "story_id"},
            "v2_story_claims": {
                "story_id",
                "candidate_id",
                "revision_id",
                "snapshot_id",
                "created_at",
                "delivered_at",
            },
            "v2_story_tombstones": {
                "story_id",
                "winner_story_id",
                "reason",
                "created_at",
            },
            "v2_channel_cursors": {
                "channel_id",
                "new_message_high_water",
                "edit_sweep_watermark",
                "edit_sweep_message_id",
                "edit_scan_before_message_id",
                "edit_scan_started_at",
                "edit_scan_max_watermark",
                "edit_scan_max_message_id",
                "updated_at",
            },
            "v2_channel_gaps": {
                "channel_id",
                "start_message_id",
                "end_message_id",
                "recorded_at",
            },
            "v2_compaction_tombstones": {
                "subject_kind",
                "subject_id",
                "digest",
                "provenance",
                "compacted_at",
            },
        }

    def _validate_migration_before_commit(self) -> None:
        try:
            self._assert_v2_database()
        except V2WorkflowError as error:
            raise self._migration_retention_error(str(error)) from error
        for table, expected_columns in self._required_column_names().items():
            actual_columns = {str(row["name"]) for row in self._db.execute(f"PRAGMA table_info({table})")}
            if actual_columns != expected_columns:
                raise self._migration_retention_error(f"migration has invalid columns for {table}")
        indexes = {
            str(row["name"])
            for row in self._db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            )
        }
        missing_indexes = self._required_index_names() - indexes
        if missing_indexes:
            raise self._migration_retention_error(
                "migration is missing required indexes: " + ",".join(sorted(missing_indexes))
            )
        foreign_key_failures = self._db.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_failures:
            raise self._migration_retention_error("migration foreign key check failed")
        integrity = self._db.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or str(integrity[0]).lower() != "ok":
            raise self._migration_retention_error("migration integrity check failed")
        invariant_failures = self.verify_invariants()
        if any(invariant_failures.values()):
            raise self._migration_retention_error(
                "migration invariant failure: " + json.dumps(invariant_failures, sort_keys=True)
            )

    def _assert_migration_deadline(self) -> None:
        if self._migration_deadline_at is not None and time.monotonic() > self._migration_deadline_at:
            raise self._migration_retention_error("migration transaction deadline exceeded")

    def initialize(self) -> None:
        self._db.executescript("""
        CREATE TABLE IF NOT EXISTS v2_metadata (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        INSERT OR IGNORE INTO v2_metadata(key, value) VALUES ('schema', 'newsbot-v2-workflow-v1');
        CREATE TABLE IF NOT EXISTS v2_telegram_cursor (
            stream INTEGER PRIMARY KEY CHECK(stream=1),
            next_offset INTEGER NOT NULL CHECK(next_offset>=0)
        );
        CREATE TABLE IF NOT EXISTS v2_remote_effects (
            entity_id TEXT NOT NULL, stage TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending', detail TEXT NOT NULL DEFAULT '', receipt_id TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
            PRIMARY KEY (entity_id, stage)
        );
        CREATE TABLE IF NOT EXISTS v2_observations (
            identity TEXT PRIMARY KEY, channel_id TEXT NOT NULL, external_post_id TEXT NOT NULL,
            payload TEXT NOT NULL, recorded_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_candidates (
            id TEXT PRIMARY KEY, observation_identity TEXT NOT NULL UNIQUE REFERENCES v2_observations(identity),
            state TEXT NOT NULL, policy_outcome TEXT NOT NULL, policy_reason TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_drafts (
            id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL UNIQUE REFERENCES v2_candidates(id),
            content TEXT NOT NULL, state TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_manual_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT NOT NULL, reason TEXT NOT NULL,
            created_at TEXT NOT NULL, UNIQUE(entity_id, reason)
        );
        CREATE TABLE IF NOT EXISTS v2_callbacks (
            token_hash TEXT PRIMARY KEY, entity_id TEXT NOT NULL, stage TEXT NOT NULL,
            expires_at TEXT NOT NULL, consumed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS v2_codex_requests (
            digest TEXT PRIMARY KEY
                CHECK(length(digest)=64 AND digest NOT GLOB '*[^0-9a-f]*'),
            candidate_id TEXT NOT NULL UNIQUE REFERENCES v2_candidates(id),
            request_bytes BLOB NOT NULL CHECK(length(request_bytes)>0),
            status TEXT NOT NULL
                CHECK(status IN ('prepared','pending','retryable_failed','terminal_failed','succeeded')),
            output_bytes BLOB,
            output_digest TEXT
                CHECK(output_digest IS NULL OR (length(output_digest)=64 AND output_digest NOT GLOB '*[^0-9a-f]*')),
            error_code TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_codex_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_digest TEXT NOT NULL REFERENCES v2_codex_requests(digest),
            number INTEGER NOT NULL CHECK(number IN (1,2)),
            status TEXT NOT NULL
                CHECK(status IN ('pending','retryable_failed','terminal_failed','succeeded')),
            error_code TEXT,
            created_at TEXT NOT NULL,
            settled_at TEXT,
            UNIQUE(request_digest, number)
        );
        """)
        self._create_delta()
        self._create_collection_delta()
        self._db.execute(
            "INSERT OR REPLACE INTO v2_metadata(key,value) VALUES('schema_version',?)",
            (self.SCHEMA_VERSION,),
        )
        self._db.commit()

    def _create_delta(self) -> None:
        statements = """
        CREATE TABLE IF NOT EXISTS v2_observation_revisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identity TEXT NOT NULL REFERENCES v2_observations(identity),
            generation INTEGER NOT NULL,
            digest TEXT NOT NULL,
            payload TEXT NOT NULL,
            ordered_at TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(identity, generation), UNIQUE(identity, digest)
        );
        CREATE TABLE IF NOT EXISTS v2_enrichment_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            revision_id INTEGER NOT NULL REFERENCES v2_observation_revisions(id),
            generation INTEGER NOT NULL, attempt_number INTEGER NOT NULL CHECK(attempt_number IN (1,2)),
            owner TEXT, status TEXT NOT NULL CHECK(status IN ('leased','retryable','terminal','succeeded','interrupted_consumed')),
            leased_until TEXT, next_retry_at TEXT, dispatched_at TEXT, settled_at TEXT,
            UNIQUE(revision_id,generation,attempt_number)
        );
        CREATE TABLE IF NOT EXISTS v2_article_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, revision_id INTEGER NOT NULL REFERENCES v2_observation_revisions(id),
            attempt_id INTEGER NOT NULL UNIQUE REFERENCES v2_enrichment_attempts(id),
            snapshot TEXT NOT NULL, result TEXT NOT NULL, body_hash TEXT, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_observation_dispositions (
            identity TEXT PRIMARY KEY REFERENCES v2_observations(identity),
            revision_id INTEGER NOT NULL REFERENCES v2_observation_revisions(id),
            outcome TEXT NOT NULL,
            reason TEXT NOT NULL,
            result TEXT NOT NULL,
            retry_count INTEGER NOT NULL,
            url_hash TEXT,
            body_hash TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_revision_heads (
            identity TEXT PRIMARY KEY REFERENCES v2_observations(identity),
            revision_id INTEGER NOT NULL REFERENCES v2_observation_revisions(id),
            generation INTEGER NOT NULL, digest TEXT NOT NULL, ordered_at TEXT NOT NULL, observed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_candidate_bindings (
            candidate_id TEXT PRIMARY KEY REFERENCES v2_candidates(id),
            revision_id INTEGER NOT NULL REFERENCES v2_observation_revisions(id),
            snapshot_id INTEGER REFERENCES v2_article_snapshots(id),
            story_id TEXT REFERENCES v2_stories(id), held INTEGER NOT NULL DEFAULT 0,
            hold_reason TEXT
        );
        CREATE TABLE IF NOT EXISTS v2_stories (
            id TEXT PRIMARY KEY, first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
            delivered_at TEXT, quarantined_at TEXT, tombstoned_at TEXT
        );
        CREATE TABLE IF NOT EXISTS v2_story_keys (
            kind TEXT NOT NULL CHECK(kind IN ('canonical_url_v1','article_body_v1')),
            key_digest TEXT NOT NULL, story_id TEXT NOT NULL REFERENCES v2_stories(id),
            PRIMARY KEY(kind,key_digest)
        );
        CREATE TABLE IF NOT EXISTS v2_story_claims (
            story_id TEXT PRIMARY KEY REFERENCES v2_stories(id), candidate_id TEXT UNIQUE,
            revision_id INTEGER REFERENCES v2_observation_revisions(id), snapshot_id INTEGER REFERENCES v2_article_snapshots(id),
            created_at TEXT NOT NULL, delivered_at TEXT
        );
        CREATE TABLE IF NOT EXISTS v2_story_tombstones (
            story_id TEXT PRIMARY KEY, winner_story_id TEXT, reason TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS v2_channel_cursors (
            channel_id TEXT PRIMARY KEY,
            new_message_high_water INTEGER NOT NULL DEFAULT 0,
            edit_sweep_watermark TEXT,
            edit_sweep_message_id INTEGER NOT NULL DEFAULT 0,
            edit_scan_before_message_id INTEGER,
            edit_scan_started_at TEXT,
            edit_scan_max_watermark TEXT,
            edit_scan_max_message_id INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS v2_revisions_identity_order ON v2_observation_revisions(identity,ordered_at,generation,digest);
        CREATE INDEX IF NOT EXISTS v2_enrichment_due ON v2_enrichment_attempts(status,next_retry_at,id);
        CREATE INDEX IF NOT EXISTS v2_candidates_state_created ON v2_candidates(state,created_at,id);
        CREATE INDEX IF NOT EXISTS v2_candidates_created ON v2_candidates(created_at,id);
        CREATE INDEX IF NOT EXISTS v2_story_keys_story ON v2_story_keys(story_id);
        CREATE INDEX IF NOT EXISTS v2_callbacks_retention ON v2_callbacks(consumed_at,expires_at,entity_id,stage);
        CREATE INDEX IF NOT EXISTS v2_effects_retention ON v2_remote_effects(status,updated_at,entity_id,stage);
        CREATE INDEX IF NOT EXISTS v2_drafts_state_created ON v2_drafts(state,created_at,id);
        CREATE INDEX IF NOT EXISTS v2_codex_candidate_status ON v2_codex_requests(candidate_id,status);
        CREATE INDEX IF NOT EXISTS v2_bindings_revision ON v2_candidate_bindings(revision_id,snapshot_id);
        CREATE INDEX IF NOT EXISTS v2_revisions_created ON v2_observation_revisions(created_at,id);
        CREATE INDEX IF NOT EXISTS v2_revisions_due_order ON v2_observation_revisions(ordered_at,id);
        CREATE INDEX IF NOT EXISTS v2_callbacks_expiry ON v2_callbacks(expires_at,token_hash);
        CREATE INDEX IF NOT EXISTS v2_snapshots_created_result ON v2_article_snapshots(created_at,id,result);
        CREATE INDEX IF NOT EXISTS v2_enrichment_settled_status ON v2_enrichment_attempts(status,settled_at,id);
        CREATE INDEX IF NOT EXISTS v2_manual_reviews_created ON v2_manual_reviews(created_at,id);
        CREATE INDEX IF NOT EXISTS v2_codex_status_created ON v2_codex_requests(status,created_at,digest);
        CREATE TABLE IF NOT EXISTS v2_compaction_tombstones (
            subject_kind TEXT NOT NULL, subject_id TEXT NOT NULL, digest TEXT NOT NULL,
            provenance TEXT NOT NULL, compacted_at TEXT NOT NULL,
            PRIMARY KEY(subject_kind,subject_id)
        );
        """
        for statement in statements.split(";\n"):
            if statement.strip():
                self._db.execute(statement)

    def _create_collection_delta(self) -> None:
        self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS v2_channel_gaps (
                channel_id TEXT NOT NULL REFERENCES v2_channel_cursors(channel_id),
                start_message_id INTEGER NOT NULL CHECK(start_message_id>0),
                end_message_id INTEGER NOT NULL CHECK(end_message_id>=start_message_id),
                recorded_at TEXT NOT NULL,
                PRIMARY KEY(channel_id,start_message_id,end_message_id)
            )
            """
        )
        self._db.execute(
            "CREATE INDEX IF NOT EXISTS v2_channel_gaps_channel_message "
            "ON v2_channel_gaps(channel_id,start_message_id,end_message_id)"
        )

    def migrate(self) -> None:
        tables = self._table_names()
        baseline = self._baseline_table_names()
        required = self._required_table_names()
        if not baseline.issubset(tables):
            raise V2WorkflowError("migrate mode refuses an incomplete Newsbot V2 predecessor")
        if not tables.issubset(required):
            raise V2WorkflowError("migrate mode refuses a mixed or future database")
        marker = self._db.execute("SELECT value FROM v2_metadata WHERE key='schema'").fetchone()
        if marker is None or marker["value"] != self.SCHEMA_MARKER:
            raise V2WorkflowError("migrate mode refuses a mixed database")
        version = self._db.execute("SELECT value FROM v2_metadata WHERE key='schema_version'").fetchone()
        version_value = None if version is None else str(version["value"])
        if version_value == self.SCHEMA_VERSION:
            self._assert_v2_database()
            return
        if version_value not in {None, "3", "4", "5", "6"}:
            raise V2WorkflowError("migrate mode refuses an unknown schema version")
        if version_value in {"3", "4", "5", "6"}:
            try:
                self._db.execute("BEGIN EXCLUSIVE")
                columns = {row["name"] for row in self._db.execute("PRAGMA table_info(v2_channel_cursors)")}
                if "edit_sweep_message_id" not in columns:
                    self._db.execute(
                        "ALTER TABLE v2_channel_cursors ADD COLUMN edit_sweep_message_id INTEGER NOT NULL DEFAULT 0"
                    )
                gap_columns = {row["name"] for row in self._db.execute("PRAGMA table_info(v2_channel_gaps)")}
                for column, definition in (
                    ("edit_scan_before_message_id", "INTEGER"),
                    ("edit_scan_started_at", "TEXT"),
                    ("edit_scan_max_watermark", "TEXT"),
                    (
                        "edit_scan_max_message_id",
                        "INTEGER NOT NULL DEFAULT 0",
                    ),
                ):
                    if column not in columns:
                        self._db.execute(f"ALTER TABLE v2_channel_cursors ADD COLUMN {column} {definition}")
                if "message_id" in gap_columns:
                    self._db.execute("ALTER TABLE v2_channel_gaps RENAME TO v2_channel_gaps_legacy")
                    self._create_collection_delta()
                    self._db.execute(
                        """
                        INSERT INTO v2_channel_gaps(
                            channel_id,start_message_id,end_message_id,recorded_at
                        )
                        WITH ordered AS (
                            SELECT channel_id,message_id,recorded_at,
                                message_id - ROW_NUMBER() OVER (
                                    PARTITION BY channel_id ORDER BY message_id
                                ) AS island
                            FROM v2_channel_gaps_legacy
                        )
                        SELECT channel_id,MIN(message_id),MAX(message_id),
                            MIN(recorded_at)
                        FROM ordered
                        GROUP BY channel_id,island
                        """
                    )
                    self._db.execute("DROP TABLE v2_channel_gaps_legacy")
                else:
                    self._create_collection_delta()
                self._create_delta()
                self._hold_releasable_backlog()
                self._db.execute(
                    "INSERT OR REPLACE INTO v2_metadata(key,value) VALUES('schema_version',?)",
                    (self.SCHEMA_VERSION,),
                )
                self._assert_migration_deadline()
                self._validate_migration_before_commit()
                self._assert_migration_deadline()
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            self._assert_v2_database()
            return
        try:
            self._db.execute("BEGIN EXCLUSIVE")
            columns = {row["name"] for row in self._db.execute("PRAGMA table_info(v2_remote_effects)")}
            if "receipt_id" not in columns:
                self._db.execute("ALTER TABLE v2_remote_effects ADD COLUMN receipt_id TEXT NOT NULL DEFAULT ''")
            self._create_delta()
            self._create_collection_delta()
            self._migrate_baseline_receipt_first()
            self._hold_releasable_backlog()
            self._db.execute(
                "INSERT OR REPLACE INTO v2_metadata(key,value) VALUES('schema_version',?)",
                (self.SCHEMA_VERSION,),
            )
            self._assert_migration_deadline()
            self._validate_migration_before_commit()
            self._assert_migration_deadline()
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        self._assert_v2_database()

    @staticmethod
    def _migration_ordered_at(
        payload: dict[str, Any],
        recorded_at: str,
    ) -> str:
        for raw in (
            payload.get("edited_at"),
            payload.get("published_at"),
            recorded_at,
        ):
            if not isinstance(raw, str):
                continue
            try:
                value = datetime.fromisoformat(raw)
            except ValueError:
                continue
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            return value.astimezone(UTC).isoformat(
                timespec="microseconds",
            )
        raise V2WorkflowError("migration timestamp is invalid")

    @staticmethod
    def _migration_selected_url(
        payload: dict[str, Any],
    ) -> tuple[str | None, str | None]:
        urls = payload.get("urls")
        if not isinstance(urls, list):
            return None, None
        for item in urls:
            raw = item.get("url") if isinstance(item, dict) else item
            if not isinstance(raw, str):
                continue
            try:
                return raw, canonicalize_url(raw)
            except UnsafeUrlError:
                continue
        return None, None

    def _migration_candidate_truth(
        self,
        candidate_id: str,
    ) -> dict[str, bool]:
        rows = self._db.execute(
            "SELECT c.state,d.id draft_id,d.state draft_state "
            "FROM v2_candidates c "
            "LEFT JOIN v2_drafts d ON d.candidate_id=c.id "
            "WHERE c.id=? ORDER BY d.created_at,d.id",
            (candidate_id,),
        ).fetchall()
        if not rows:
            raise V2WorkflowError("migration candidate disappeared")
        draft_ids = [str(row["draft_id"]) for row in rows if row["draft_id"] is not None]
        entity_ids = [candidate_id, *draft_ids]
        placeholders = ",".join("?" for _ in entity_ids)
        effects = self._db.execute(
            f"SELECT stage,status,receipt_id FROM v2_remote_effects WHERE entity_id IN ({placeholders})",
            tuple(entity_ids),
        ).fetchall()
        manual = self._db.execute(
            f"SELECT 1 FROM v2_manual_reviews WHERE entity_id IN ({placeholders}) LIMIT 1",
            tuple(entity_ids),
        ).fetchone()
        candidate_state = str(rows[0]["state"])
        draft_states = [str(row["draft_state"]) for row in rows if row["draft_state"] is not None]
        candidate_delivered = candidate_state == V2State.SHEET_DELIVERED.value
        drafts_delivered = bool(draft_states) and all(state == V2State.SHEET_DELIVERED.value for state in draft_states)
        any_draft_delivered = any(state == V2State.SHEET_DELIVERED.value for state in draft_states)
        confirmed_sheet = any(
            effect["stage"] == "sheets_delivery" and effect["status"] == "confirmed" and bool(effect["receipt_id"])
            for effect in effects
        )
        delivered = confirmed_sheet or (candidate_delivered and drafts_delivered)
        ambiguous = (
            manual is not None
            or candidate_state == V2State.MANUAL_REVIEW.value
            or any(state == V2State.MANUAL_REVIEW.value for state in draft_states)
            or any(effect["status"] in {"pending", "ambiguous"} for effect in effects)
            or (candidate_delivered or any_draft_delivered)
            and not (candidate_delivered and drafts_delivered)
        )
        callback = self._db.execute(
            f"SELECT 1 FROM v2_callbacks WHERE entity_id IN ({placeholders}) LIMIT 1",
            tuple(entity_ids),
        ).fetchone()
        codex = self._db.execute(
            "SELECT 1 FROM v2_codex_requests WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone()
        effectful = (
            any(effect["status"] == "confirmed" for effect in effects)
            or callback is not None
            or codex is not None
            or bool(draft_ids)
            or candidate_state != V2State.PENDING_CANDIDATE.value
        )
        return {
            "D": delivered,
            "U": ambiguous,
            "S": effectful,
            "N": not delivered and not ambiguous and not effectful,
        }

    def _migrate_baseline_receipt_first(self) -> None:
        effect_count = int(self._db.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0])
        callback_count = int(self._db.execute("SELECT COUNT(*) FROM v2_callbacks").fetchone()[0])
        legacy = self._db.execute(
            "SELECT identity,payload,recorded_at FROM v2_observations ORDER BY recorded_at,identity"
        ).fetchall()
        candidates: dict[str, dict[str, Any]] = {}
        for row in legacy:
            payload_text = str(row["payload"])
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError as error:
                raise V2WorkflowError("legacy observation payload is invalid") from error
            if not isinstance(payload, dict):
                raise V2WorkflowError("legacy observation payload is not an object")
            digest = hashlib.sha256(payload_text.encode()).hexdigest()
            ordered_at = self._migration_ordered_at(
                payload,
                str(row["recorded_at"]),
            )
            revision_cursor = self._db.execute(
                "INSERT INTO v2_observation_revisions("
                "identity,generation,digest,payload,ordered_at,"
                "observed_at,created_at"
                ") VALUES(?,1,?,?,?,?,?)",
                (
                    row["identity"],
                    digest,
                    payload_text,
                    ordered_at,
                    row["recorded_at"],
                    row["recorded_at"],
                ),
            )
            revision_id = _inserted_id(revision_cursor)
            self._db.execute(
                "INSERT INTO v2_revision_heads("
                "identity,revision_id,generation,digest,ordered_at,observed_at"
                ") VALUES(?,?,?,?,?,?)",
                (
                    row["identity"],
                    revision_id,
                    1,
                    digest,
                    ordered_at,
                    row["recorded_at"],
                ),
            )
            candidate = self._db.execute(
                "SELECT id,state,policy_outcome,policy_reason,created_at "
                "FROM v2_candidates WHERE observation_identity=?",
                (row["identity"],),
            ).fetchone()
            requested_url, canonical_url = self._migration_selected_url(payload)
            attempt_status = "succeeded" if candidate is not None else "terminal"
            attempt_cursor = self._db.execute(
                "INSERT INTO v2_enrichment_attempts("
                "revision_id,generation,attempt_number,status,settled_at"
                ") VALUES(?,?,1,?,?)",
                (
                    revision_id,
                    1,
                    attempt_status,
                    row["recorded_at"],
                ),
            )
            attempt_id = _inserted_id(attempt_cursor)
            snapshot = {
                "result": "legacy_migration",
                "requested_url": requested_url,
                "final_url": requested_url,
                "canonical_url": canonical_url,
                "canonical_source": "requested" if canonical_url else None,
                "body": None,
                "body_hash": None,
                "material_count": 0,
                "provenance": {
                    "migration": "baseline_v1",
                    "normalizer_version": "canonical_url_v1",
                },
            }
            snapshot_cursor = self._db.execute(
                "INSERT INTO v2_article_snapshots("
                "revision_id,attempt_id,snapshot,result,body_hash,created_at"
                ") VALUES(?,?,?,'legacy_migration',NULL,?)",
                (
                    revision_id,
                    attempt_id,
                    json.dumps(
                        snapshot,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    row["recorded_at"],
                ),
            )
            snapshot_id = _inserted_id(snapshot_cursor)
            if candidate is None:
                self._db.execute(
                    "INSERT INTO v2_observation_dispositions("
                    "identity,revision_id,outcome,reason,result,retry_count,"
                    "url_hash,body_hash,updated_at"
                    ") VALUES(?,?,'non_news','legacy_non_candidate',"
                    "'legacy_migration',1,NULL,NULL,?)",
                    (
                        row["identity"],
                        revision_id,
                        row["recorded_at"],
                    ),
                )
                continue
            candidate_id = str(candidate["id"])
            keys = self._snapshot_keys(snapshot)
            candidates[candidate_id] = {
                "candidate_id": candidate_id,
                "revision_id": revision_id,
                "snapshot_id": snapshot_id,
                "keys": keys,
                "created_at": str(candidate["created_at"]),
                "state": str(candidate["state"]),
                "truth": self._migration_candidate_truth(candidate_id),
            }
            self._bind_candidate(candidate_id, revision_id, snapshot_id)
            self._db.execute(
                "INSERT INTO v2_observation_dispositions("
                "identity,revision_id,outcome,reason,result,retry_count,"
                "url_hash,body_hash,updated_at"
                ") VALUES(?,?,?,?, 'legacy_migration',1,?,NULL,?)",
                (
                    row["identity"],
                    revision_id,
                    candidate["policy_outcome"],
                    candidate["policy_reason"],
                    (None if canonical_url is None else hashlib.sha256(canonical_url.encode()).hexdigest()),
                    row["recorded_at"],
                ),
            )

        parent = {candidate_id: candidate_id for candidate_id in candidates}

        def find(candidate_id: str) -> str:
            while parent[candidate_id] != candidate_id:
                parent[candidate_id] = parent[parent[candidate_id]]
                candidate_id = parent[candidate_id]
            return candidate_id

        def union(left: str, right: str) -> None:
            left_root, right_root = find(left), find(right)
            if left_root != right_root:
                parent[max(left_root, right_root)] = min(left_root, right_root)

        key_owner: dict[tuple[str, str], str] = {}
        for candidate_id, info in candidates.items():
            for key in info["keys"]:
                owner = key_owner.setdefault(key, candidate_id)
                union(candidate_id, owner)

        groups: dict[str, list[str]] = {}
        for candidate_id in candidates:
            groups.setdefault(find(candidate_id), []).append(candidate_id)

        progress = {
            V2State.PENDING_CANDIDATE.value: 0,
            V2State.CANDIDATE_APPROVED.value: 1,
            V2State.DRAFT_PENDING_APPROVAL.value: 2,
            V2State.DRAFT_APPROVED.value: 3,
            V2State.MANUAL_REVIEW.value: 4,
            V2State.SHEET_DELIVERED.value: 5,
        }
        for member_ids in groups.values():
            member_ids.sort()
            infos = [candidates[candidate_id] for candidate_id in member_ids]
            all_keys = sorted({key for info in infos for key in info["keys"]})
            story_seed = "\n".join(
                [
                    *(f"{kind}:{digest}" for kind, digest in all_keys),
                    *member_ids,
                ]
            )
            story_id = hashlib.sha256(("migration-story\0" + story_seed).encode()).hexdigest()[:24]
            first_seen = min(info["created_at"] for info in infos)
            last_seen = max(info["created_at"] for info in infos)
            truths = {
                truth: [info["candidate_id"] for info in infos if info["truth"][truth]]
                for truth in ("D", "U", "S", "N")
            }
            quarantined = bool(truths["U"] or len(truths["D"]) > 1 or (not truths["D"] and len(truths["S"]) > 1))
            winner: str | None
            if quarantined:
                winner = None
            elif truths["D"]:
                winner = truths["D"][0]
            elif truths["S"]:
                winner = truths["S"][0]
            else:
                winner = max(
                    member_ids,
                    key=lambda candidate_id: (
                        progress.get(
                            str(candidates[candidate_id]["state"]),
                            -1,
                        ),
                        str(candidates[candidate_id]["created_at"]),
                        candidate_id,
                    ),
                )
            now = self._now()
            delivered_at = now if truths["D"] else None
            self._db.execute(
                "INSERT INTO v2_stories("
                "id,first_seen_at,last_seen_at,delivered_at,"
                "quarantined_at,tombstoned_at"
                ") VALUES(?,?,?,?,?,NULL)",
                (
                    story_id,
                    first_seen,
                    last_seen,
                    delivered_at,
                    now if quarantined else None,
                ),
            )
            self._db.executemany(
                "INSERT INTO v2_story_keys(kind,key_digest,story_id) VALUES(?,?,?)",
                [(kind, digest, story_id) for kind, digest in all_keys],
            )
            for candidate_id in member_ids:
                reason = (
                    "migration_receipt_conflict"
                    if quarantined
                    else ("pre_migration_backlog_hold" if candidate_id == winner else f"duplicate_of:{winner}")
                )
                self._db.execute(
                    "UPDATE v2_candidate_bindings SET story_id=?,held=1,hold_reason=? WHERE candidate_id=?",
                    (story_id, reason, candidate_id),
                )
                if candidate_id != winner:
                    self._db.execute(
                        "INSERT OR IGNORE INTO v2_manual_reviews(entity_id,reason,created_at) VALUES(?,?,?)",
                        (candidate_id, reason, now),
                    )
                    if candidates[candidate_id]["state"] != V2State.SHEET_DELIVERED.value:
                        self._db.execute(
                            "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                            (
                                V2State.MANUAL_REVIEW.value,
                                now,
                                candidate_id,
                            ),
                        )
                        self._db.execute(
                            "UPDATE v2_drafts "
                            "SET state=?,updated_at=? "
                            "WHERE candidate_id=? "
                            "AND state!='sheet_delivered'",
                            (
                                V2State.MANUAL_REVIEW.value,
                                now,
                                candidate_id,
                            ),
                        )
            if winner is not None:
                winner_info = candidates[winner]
                winner_delivered = bool(winner_info["truth"]["D"])
                self._db.execute(
                    "INSERT INTO v2_story_claims("
                    "story_id,candidate_id,revision_id,snapshot_id,"
                    "created_at,delivered_at"
                    ") VALUES(?,?,?,?,?,?)",
                    (
                        story_id,
                        winner,
                        winner_info["revision_id"],
                        winner_info["snapshot_id"],
                        now,
                        now if winner_delivered else None,
                    ),
                )
                if winner_delivered:
                    self._db.execute(
                        "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                        (V2State.SHEET_DELIVERED.value, now, winner),
                    )
                    self._db.execute(
                        "UPDATE v2_drafts SET state=?,updated_at=? WHERE candidate_id=?",
                        (V2State.SHEET_DELIVERED.value, now, winner),
                    )

        brazil_delivered = "365610e753af078c5674d2fb"  # pragma: allowlist secret
        brazil_pending = "1f3276d77e1a591a27933864"  # pragma: allowlist secret
        brazil_rows = self._db.execute(
            "SELECT c.id,b.story_id,b.held,claim.candidate_id winner,"
            "s.delivered_at FROM v2_candidates c "
            "JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "JOIN v2_stories s ON s.id=b.story_id "
            "LEFT JOIN v2_story_claims claim ON claim.story_id=s.id "
            "WHERE c.id IN (?,?) ORDER BY c.id",
            (brazil_delivered, brazil_pending),
        ).fetchall()
        if len(brazil_rows) == 1:
            raise V2WorkflowError("Brazil duplicate migration requires both known candidates")
        if len(brazil_rows) == 2:
            by_id = {str(row["id"]): row for row in brazil_rows}
            if (
                by_id[brazil_delivered]["story_id"] != by_id[brazil_pending]["story_id"]
                or by_id[brazil_delivered]["winner"] != brazil_delivered
                or by_id[brazil_delivered]["delivered_at"] is None
                or not by_id[brazil_pending]["held"]
            ):
                raise V2WorkflowError("Brazil duplicate migration assertion failed")

        if (
            int(self._db.execute("SELECT COUNT(*) FROM v2_remote_effects").fetchone()[0]) != effect_count
            or int(self._db.execute("SELECT COUNT(*) FROM v2_callbacks").fetchone()[0]) != callback_count
        ):
            raise V2WorkflowError("migration created remote effects or callbacks")

    def handoff_telegram_cursor(self, next_offset: int) -> int:
        """Merge the stopped legacy owner's final frontier without regression."""
        if isinstance(next_offset, bool) or not isinstance(next_offset, int) or next_offset < 0:
            raise ValueError("Telegram next offset must be a nonnegative integer")
        with self._db:
            self._db.execute(
                "INSERT INTO v2_telegram_cursor(stream,next_offset) VALUES(1,?) "
                "ON CONFLICT(stream) DO UPDATE SET next_offset=MAX(next_offset,excluded.next_offset)",
                (next_offset,),
            )
        current = self.telegram_next_offset()
        assert current is not None
        return current

    def telegram_next_offset(self) -> int | None:
        """Return the V2-owned Bot API stream cursor, when it has been seeded."""
        row = self._db.execute("SELECT next_offset FROM v2_telegram_cursor WHERE stream=1").fetchone()
        return None if row is None else int(row["next_offset"])

    def advance_telegram_cursor(self, next_offset: int) -> int:
        """Advance an explicitly handed-off V2 cursor monotonically."""
        if isinstance(next_offset, bool) or not isinstance(next_offset, int) or next_offset < 0:
            raise ValueError("Telegram next offset must be a nonnegative integer")
        with self._db:
            updated = self._db.execute(
                "UPDATE v2_telegram_cursor SET next_offset=MAX(next_offset,?) WHERE stream=1",
                (next_offset,),
            )
        if updated.rowcount != 1:
            raise V2WorkflowError("Telegram cursor handoff is required")
        current = self.telegram_next_offset()
        assert current is not None
        return current

    @staticmethod
    def _eligible_story_sql(
        candidate_alias: str,
        *,
        allow_confirmed_sheets: bool = False,
    ) -> str:
        if candidate_alias not in {"candidate", "c"}:
            raise ValueError("unsupported candidate SQL alias")
        confirmed_guard = (
            ""
            if allow_confirmed_sheets
            else (
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_drafts delivered_effect_draft "
                "JOIN v2_remote_effects delivered_effect "
                "ON delivered_effect.entity_id=delivered_effect_draft.id "
                "AND delivered_effect.stage='sheets_delivery' "
                "AND delivered_effect.status='confirmed' "
                "AND delivered_effect.receipt_id<>'' "
                "WHERE delivered_effect_draft.candidate_id="
                f"{candidate_alias}.id)"
            )
        )
        return (
            "EXISTS(SELECT 1 FROM v2_candidate_bindings eligible_binding "
            "JOIN v2_stories eligible_story "
            "ON eligible_story.id=eligible_binding.story_id "
            "JOIN v2_story_claims eligible_claim "
            "ON eligible_claim.story_id=eligible_story.id "
            f"AND eligible_claim.candidate_id={candidate_alias}.id "
            f"WHERE eligible_binding.candidate_id={candidate_alias}.id "
            "AND eligible_binding.held=0 "
            "AND eligible_story.delivered_at IS NULL "
            "AND eligible_story.quarantined_at IS NULL "
            "AND eligible_story.tombstoned_at IS NULL "
            "AND eligible_claim.delivered_at IS NULL "
            "AND EXISTS(SELECT 1 FROM v2_story_keys eligible_key "
            "WHERE eligible_key.story_id=eligible_story.id) "
            "AND NOT EXISTS(SELECT 1 FROM v2_drafts delivered_draft "
            "WHERE delivered_draft.candidate_id="
            f"{candidate_alias}.id "
            "AND delivered_draft.state='sheet_delivered')) "
            f"AND {candidate_alias}.state!='sheet_delivered' " + confirmed_guard
        )

    def _candidate_id_for_entity(self, entity_id: str) -> str:
        draft = self._db.execute(
            "SELECT candidate_id FROM v2_drafts WHERE id=?",
            (str(entity_id),),
        ).fetchone()
        return str(draft["candidate_id"]) if draft is not None else str(entity_id)

    def story_is_eligible(
        self,
        candidate_id: str,
        *,
        allow_confirmed_sheets: bool = False,
    ) -> bool:
        return (
            self._db.execute(
                "SELECT 1 FROM v2_candidates c WHERE c.id=? AND "
                + self._eligible_story_sql(
                    "c",
                    allow_confirmed_sheets=allow_confirmed_sheets,
                ),
                (str(candidate_id),),
            ).fetchone()
            is not None
        )

    def _require_story_eligible(
        self,
        candidate_id: str,
        *,
        allow_confirmed_sheets: bool = False,
    ) -> None:
        if not self.story_is_eligible(
            candidate_id,
            allow_confirmed_sheets=allow_confirmed_sheets,
        ):
            raise V2WorkflowError(
                "candidate story is delivered, quarantined, held, "
                "tombstoned, unclaimed, unbound, or has delivery evidence"
            )

    def _require_outbound_state(
        self,
        entity_id: str,
        stage: str,
    ) -> str:
        if stage in {
            "candidate_notification",
            "draft_generation",
        }:
            row = self._db.execute(
                "SELECT id,state FROM v2_candidates WHERE id=?",
                (entity_id,),
            ).fetchone()
            expected = (
                V2State.PENDING_CANDIDATE.value
                if stage == "candidate_notification"
                else V2State.CANDIDATE_APPROVED.value
            )
            if row is None or str(row["state"]) != expected:
                raise V2WorkflowError("outbound candidate state changed")
            return str(row["id"])
        if stage in {
            "draft_notification",
            "sheets_delivery",
        }:
            row = self._db.execute(
                "SELECT d.candidate_id,d.state draft_state,"
                "c.state candidate_state "
                "FROM v2_drafts d "
                "JOIN v2_candidates c ON c.id=d.candidate_id "
                "WHERE d.id=?",
                (entity_id,),
            ).fetchone()
            expected = (
                V2State.DRAFT_PENDING_APPROVAL.value if stage == "draft_notification" else V2State.DRAFT_APPROVED.value
            )
            if row is None or str(row["draft_state"]) != expected or str(row["candidate_state"]) != expected:
                raise V2WorkflowError("outbound draft state changed")
            return str(row["candidate_id"])
        return self._candidate_id_for_entity(entity_id)

    def next_candidate_pending_notification(self) -> V2Candidate | None:
        """Select the oldest candidate whose notification needs safe recovery or dispatch."""
        row = self._db.execute(
            "SELECT candidate.id FROM v2_candidates candidate "
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=candidate.id "
            "AND effect.stage='candidate_notification' "
            "WHERE candidate.state=? AND "
            + self._eligible_story_sql("candidate")
            + " AND (effect.status IS NULL OR effect.status!='confirmed') "
            "ORDER BY candidate.created_at,candidate.id LIMIT 1",
            (V2State.PENDING_CANDIDATE.value,),
        ).fetchone()
        return None if row is None else self.get_candidate(str(row["id"]))

    def next_draft_approved_sheets_delivery(self) -> V2Draft | None:
        """Select work that can settle locally or make its single permitted retry."""
        row = self._db.execute(
            "SELECT draft.id FROM v2_drafts draft "
            "JOIN v2_candidates candidate ON candidate.id=draft.candidate_id "
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=draft.id AND effect.stage='sheets_delivery' "
            "WHERE draft.state=? AND "
            + self._eligible_story_sql(
                "candidate",
                allow_confirmed_sheets=True,
            )
            + " AND (effect.status IS NULL OR "
            "effect.status IN ('confirmed','pending','ambiguous','failed')) "
            "ORDER BY draft.created_at,draft.id LIMIT 1",
            (V2State.DRAFT_APPROVED.value,),
        ).fetchone()
        return None if row is None else self.get_draft(str(row["id"]))

    @staticmethod
    def _is_clear_pre_dispatch_sheets_failure(detail: str) -> bool:
        try:
            parsed = json.loads(detail)
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        return isinstance(parsed, dict) and parsed.get("failure") == "clear_pre_dispatch_network"

    @staticmethod
    def _identity(observation: SourceObservation) -> str:
        return f"{observation.channel_id}:{observation.external_post_id}"

    @staticmethod
    def _utc_timestamp(
        value: datetime | str,
    ) -> str:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
        if parsed.tzinfo is None:
            raise ValueError("workflow timestamp must be timezone-aware")
        return parsed.astimezone(UTC).isoformat(
            timespec="microseconds",
        )

    @classmethod
    def _now(cls) -> str:
        return cls._utc_timestamp(datetime.now(UTC))

    @staticmethod
    def _payload(observation: SourceObservation) -> dict[str, Any]:
        return {
            "channel_id": observation.channel_id,
            "channel_handle": observation.channel_handle,
            "external_post_id": observation.external_post_id,
            "published_at": V2Workflow._utc_timestamp(
                observation.published_at,
            ),
            "text": observation.text,
            "preview_title": observation.preview_title,
            "preview_description": observation.preview_description,
            "kind": observation.kind,
            "sponsored": observation.sponsored,
            "urls": [asdict(url) for url in observation.urls],
            "media": [asdict(media) for media in observation.media],
            "engagement": asdict(observation.engagement),
            "conflicts": list(observation.conflicts),
        }

    def _candidate_row(self, candidate_id: str) -> sqlite3.Row:
        row = self._db.execute(
            "SELECT c.*,CASE WHEN r.payload IS NULL OR r.payload='{}' "
            "THEN o.payload ELSE r.payload END payload "
            "FROM v2_candidates c "
            "JOIN v2_observations o ON o.identity=c.observation_identity "
            "LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "LEFT JOIN v2_observation_revisions r ON r.id=b.revision_id "
            "WHERE c.id=?",
            (str(candidate_id),),
        ).fetchone()
        if not row:
            raise V2WorkflowError(f"unknown candidate: {candidate_id}")
        return cast(sqlite3.Row, row)

    def _bind_candidate(
        self, candidate_id: str, revision_id: int, snapshot_id: int | None = None, story_id: str | None = None
    ) -> None:
        if snapshot_id is not None:
            snapshot = self._db.execute(
                "SELECT revision_id FROM v2_article_snapshots WHERE id=?", (snapshot_id,)
            ).fetchone()
            if snapshot is None or int(snapshot["revision_id"]) != revision_id:
                raise V2WorkflowError("candidate binding snapshot must belong to its revision")
        existing = self._db.execute(
            "SELECT revision_id,snapshot_id,story_id FROM v2_candidate_bindings WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone()
        if existing is None:
            self._db.execute(
                "INSERT INTO v2_candidate_bindings(candidate_id,revision_id,snapshot_id,story_id) VALUES(?,?,?,?)",
                (candidate_id, revision_id, snapshot_id, story_id),
            )
            return
        if int(existing["revision_id"]) != revision_id:
            raise V2WorkflowError("candidate binding revision is immutable")
        if (
            existing["snapshot_id"] is not None
            and snapshot_id is not None
            and int(existing["snapshot_id"]) != snapshot_id
        ):
            raise V2WorkflowError("candidate binding snapshot is immutable")
        self._db.execute(
            "UPDATE v2_candidate_bindings SET snapshot_id=COALESCE(snapshot_id,?),story_id=COALESCE(?,story_id) WHERE candidate_id=?",
            (snapshot_id, story_id, candidate_id),
        )

    def record_remote_attempt(
        self,
        entity_id: str,
        stage: str,
    ) -> int:
        entity_id = str(entity_id)
        stage = str(stage)
        now = self._now()
        self._db.execute("BEGIN IMMEDIATE")
        try:
            if stage in self.OUTBOUND_STAGES:
                candidate_id = self._require_outbound_state(
                    entity_id,
                    stage,
                )
                self._require_story_eligible(candidate_id)
            inserted = self._db.execute(
                "INSERT OR IGNORE INTO v2_remote_effects("
                "entity_id,stage,attempts,status,detail,"
                "receipt_id,updated_at"
                ") VALUES(?,?,1,'pending','','',?)",
                (entity_id, stage, now),
            )
            if inserted.rowcount == 1:
                self._db.execute("COMMIT")
                return 1
            prior = self._db.execute(
                "SELECT attempts,status,detail FROM v2_remote_effects WHERE entity_id=? AND stage=?",
                (entity_id, stage),
            ).fetchone()
            if prior is not None and prior["status"] == "confirmed":
                self._emit_immediate_alert(
                    ImmediateAlert.CONFIRMED_EFFECT_REATTEMPT,
                    entity=entity_id,
                )
            if (
                prior is None
                or prior["status"] != "failed"
                or int(prior["attempts"]) >= 2
                or str(prior["detail"]) != "clear_pre_dispatch_network"
            ):
                raise V2WorkflowError("remote effect is not eligible for a safe retry")
            updated = self._db.execute(
                "UPDATE v2_remote_effects "
                "SET attempts=attempts+1,status='pending',"
                "detail='',receipt_id='',updated_at=? "
                "WHERE entity_id=? AND stage=? "
                "AND status='failed' AND attempts=? "
                "AND detail='clear_pre_dispatch_network'",
                (
                    now,
                    entity_id,
                    stage,
                    int(prior["attempts"]),
                ),
            )
            if updated.rowcount != 1:
                raise V2WorkflowError("remote effect retry CAS failed")
            self._db.execute("COMMIT")
            return int(prior["attempts"]) + 1
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def claim_remote_effect(
        self,
        entity_id: str,
        stage: str,
        detail: str,
    ) -> bool:
        """Claim an absent Sheets operation or its proven-safe retry."""
        entity_id = str(entity_id)
        stage = str(stage)
        now = self._now()
        self._db.execute("BEGIN IMMEDIATE")
        try:
            if stage in self.OUTBOUND_STAGES:
                candidate_id = self._require_outbound_state(
                    entity_id,
                    stage,
                )
                self._require_story_eligible(candidate_id)
            inserted = self._db.execute(
                "INSERT OR IGNORE INTO v2_remote_effects"
                "(entity_id,stage,attempts,status,detail,"
                "receipt_id,updated_at) "
                "VALUES(?,?,1,'pending',?,'',?)",
                (entity_id, stage, str(detail), now),
            )
            if inserted.rowcount == 1:
                self._db.execute("COMMIT")
                return True
            prior = self._db.execute(
                "SELECT attempts,status,detail FROM v2_remote_effects WHERE entity_id=? AND stage=?",
                (entity_id, stage),
            ).fetchone()
            if prior is not None and prior["status"] == "confirmed":
                self._emit_immediate_alert(
                    ImmediateAlert.CONFIRMED_EFFECT_REATTEMPT,
                    entity=entity_id,
                )
            if (
                prior is None
                or stage != "sheets_delivery"
                or prior["status"] != "failed"
                or int(prior["attempts"]) >= 2
                or not self._is_clear_pre_dispatch_sheets_failure(str(prior["detail"]))
            ):
                self._db.execute("ROLLBACK")
                return False
            retried = self._db.execute(
                "UPDATE v2_remote_effects "
                "SET attempts=attempts+1,status='pending',"
                "detail=?,receipt_id='',updated_at=? "
                "WHERE entity_id=? AND stage=? "
                "AND status='failed' AND attempts=? "
                "AND detail=?",
                (
                    str(detail),
                    now,
                    entity_id,
                    stage,
                    int(prior["attempts"]),
                    str(prior["detail"]),
                ),
            )
            if retried.rowcount != 1:
                self._db.execute("ROLLBACK")
                return False
            self._db.execute("COMMIT")
            return True
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def update_remote_effect_claim(
        self,
        entity_id: str,
        stage: str,
        expected_detail: str,
        new_detail: str,
    ) -> bool:
        """Advance one owned pending operation without permitting a competing writer."""
        with self._db:
            updated = self._db.execute(
                "UPDATE v2_remote_effects SET detail=?,updated_at=? "
                "WHERE entity_id=? AND stage=? AND status='pending' AND detail=?",
                (str(new_detail), self._now(), str(entity_id), str(stage), str(expected_detail)),
            )
        return updated.rowcount == 1

    def settle_remote_effect_claim(
        self,
        entity_id: str,
        stage: str,
        expected_detail: str,
        status: str,
        *,
        detail: str,
        receipt_id: str = "",
    ) -> bool:
        """Settle only the exact pending claim held by the caller."""
        if status not in {"confirmed", "ambiguous", "failed"}:
            raise ValueError("invalid remote effect status")
        if status == "confirmed" and str(stage) == "sheets_delivery" and not receipt_id:
            raise ValueError("confirmed Sheets delivery requires a receipt ID")
        with self._db:
            updated = self._db.execute(
                "UPDATE v2_remote_effects SET status=?,detail=?,receipt_id=?,updated_at=? "
                "WHERE entity_id=? AND stage=? AND status='pending' AND detail=?",
                (
                    status,
                    str(detail),
                    str(receipt_id),
                    self._now(),
                    str(entity_id),
                    str(stage),
                    str(expected_detail),
                ),
            )
        if updated.rowcount == 1:
            self._emit_effect(
                str(entity_id),
                str(stage),
                str(status),
            )
        return updated.rowcount == 1

    def settle_remote_effect(
        self, entity_id: str, stage: str, status: str, detail: str = "", receipt_id: str = ""
    ) -> None:
        if status not in {"confirmed", "ambiguous", "failed"}:
            raise ValueError("invalid remote effect status")
        if status == "confirmed" and str(stage) == "sheets_delivery" and not receipt_id:
            raise ValueError("confirmed Sheets delivery requires a receipt ID")
        with self._db:
            updated = self._db.execute(
                "UPDATE v2_remote_effects "
                "SET status=?,detail=?,receipt_id=?,updated_at=? "
                "WHERE entity_id=? AND stage=? AND status='pending'",
                (
                    status,
                    str(detail),
                    str(receipt_id),
                    self._now(),
                    str(entity_id),
                    str(stage),
                ),
            )
            if updated.rowcount != 1:
                raise V2WorkflowError("remote effect is not pending")
        self._emit_effect(
            str(entity_id),
            str(stage),
            str(status),
        )

    def remote_effect(self, entity_id: str, stage: str) -> dict[str, object] | None:
        row = self._db.execute(
            "SELECT * FROM v2_remote_effects WHERE entity_id=? AND stage=?", (str(entity_id), str(stage))
        ).fetchone()
        return None if row is None else dict(row)

    def claim_notification(
        self,
        *,
        entity_id: str,
        callback_stage: str,
        token_hash: str,
        expires_at: str,
        claim_detail: str,
    ) -> bool:
        """Atomically bind one outbound claim to its callback capability."""
        if callback_stage not in {"candidate", "draft"}:
            raise ValueError("invalid callback stage")
        if len(token_hash) != 64 or not claim_detail:
            raise ValueError("invalid notification claim")
        entity_id = str(entity_id)
        candidate_id = self._candidate_id_for_entity(entity_id)
        remote_stage = "candidate_notification" if callback_stage == "candidate" else "draft_notification"
        try:
            self._db.execute("BEGIN IMMEDIATE")
            self._require_story_eligible(candidate_id)
            self._require_outbound_state(
                entity_id,
                remote_stage,
            )
            inserted = self._db.execute(
                "INSERT OR IGNORE INTO v2_remote_effects("
                "entity_id,stage,attempts,status,detail,receipt_id,updated_at"
                ") VALUES(?,?,1,'pending',?,'',?)",
                (entity_id, remote_stage, claim_detail, self._now()),
            )
            if inserted.rowcount != 1:
                prior = self._db.execute(
                    "SELECT status FROM v2_remote_effects WHERE entity_id=? AND stage=?",
                    (entity_id, remote_stage),
                ).fetchone()
                if prior is not None and prior["status"] == "confirmed":
                    self._emit_immediate_alert(
                        ImmediateAlert.CONFIRMED_EFFECT_REATTEMPT,
                        entity=entity_id,
                    )
                self._db.execute("ROLLBACK")
                return False
            self._db.execute(
                "INSERT INTO v2_callbacks(token_hash,entity_id,stage,expires_at,consumed_at) VALUES(?,?,?,?,NULL)",
                (token_hash, entity_id, callback_stage, expires_at),
            )
            self._db.execute("COMMIT")
            return True
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def settle_callback_any(
        self,
        token_hash: str,
        now: str,
        *,
        expected_stage: str | None = None,
    ) -> tuple[str, str] | None:
        """Consume and apply one eligible approval capability atomically."""
        if expected_stage is not None and expected_stage not in {
            "candidate",
            "draft",
        }:
            raise ValueError("invalid callback stage")
        try:
            self._db.execute("BEGIN IMMEDIATE")
            clauses = [
                "token_hash=?",
                "consumed_at IS NULL",
                "expires_at>?",
            ]
            params: list[object] = [token_hash, now]
            if expected_stage is not None:
                clauses.append("stage=?")
                params.append(expected_stage)
            callback = self._db.execute(
                "SELECT entity_id,stage FROM v2_callbacks WHERE " + " AND ".join(clauses),
                tuple(params),
            ).fetchone()
            if callback is None:
                self._db.execute("ROLLBACK")
                return None
            entity_id = str(callback["entity_id"])
            stage = str(callback["stage"])
            if stage == "candidate":
                candidate_id = entity_id
                self._require_story_eligible(candidate_id)
                updated = self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.CANDIDATE_APPROVED.value,
                        now,
                        candidate_id,
                        V2State.PENDING_CANDIDATE.value,
                    ),
                )
            elif stage == "draft":
                draft = self._db.execute(
                    "SELECT candidate_id FROM v2_drafts WHERE id=? AND state=?",
                    (entity_id, V2State.DRAFT_PENDING_APPROVAL.value),
                ).fetchone()
                if draft is None:
                    self._db.execute("ROLLBACK")
                    return None
                candidate_id = str(draft["candidate_id"])
                self._require_story_eligible(candidate_id)
                updated = self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.DRAFT_APPROVED.value,
                        now,
                        candidate_id,
                        V2State.DRAFT_PENDING_APPROVAL.value,
                    ),
                )
                if updated.rowcount == 1:
                    updated = self._db.execute(
                        "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=? AND state=?",
                        (
                            V2State.DRAFT_APPROVED.value,
                            now,
                            entity_id,
                            V2State.DRAFT_PENDING_APPROVAL.value,
                        ),
                    )
            else:
                self._db.execute("ROLLBACK")
                return None
            if updated.rowcount != 1:
                self._db.execute("ROLLBACK")
                return None
            consumed = self._db.execute(
                "UPDATE v2_callbacks SET consumed_at=? WHERE token_hash=? AND consumed_at IS NULL",
                (now, token_hash),
            )
            if consumed.rowcount != 1:
                raise V2WorkflowError("callback capability CAS failed")
            self._db.execute("COMMIT")
            return entity_id, stage
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def reconcile_expired_approval_capabilities(self, now: str) -> int:
        """Move gates with no live approval capability to manual review."""
        reconciled = 0
        with self._db:
            expired = self._db.execute(
                "SELECT DISTINCT entity_id,stage FROM v2_callbacks callback "
                "WHERE consumed_at IS NULL AND expires_at<=? "
                "AND NOT EXISTS ("
                "SELECT 1 FROM v2_callbacks live WHERE live.entity_id=callback.entity_id "
                "AND live.stage=callback.stage AND live.consumed_at IS NULL AND live.expires_at>?"
                ")",
                (now, now),
            ).fetchall()
            for callback in expired:
                entity_id, stage = str(callback["entity_id"]), str(callback["stage"])
                if stage == "candidate":
                    updated = self._db.execute(
                        "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                        (V2State.MANUAL_REVIEW.value, now, entity_id, V2State.PENDING_CANDIDATE.value),
                    )
                elif stage == "draft":
                    draft = self._db.execute(
                        "SELECT candidate_id FROM v2_drafts WHERE id=? AND state=?",
                        (entity_id, V2State.DRAFT_PENDING_APPROVAL.value),
                    ).fetchone()
                    if draft is None:
                        continue
                    updated = self._db.execute(
                        "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                        (
                            V2State.MANUAL_REVIEW.value,
                            now,
                            str(draft["candidate_id"]),
                            V2State.DRAFT_PENDING_APPROVAL.value,
                        ),
                    )
                    if updated.rowcount == 1:
                        self._db.execute(
                            "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=?",
                            (V2State.MANUAL_REVIEW.value, now, entity_id),
                        )
                else:
                    continue
                if updated.rowcount == 1:
                    self._db.execute(
                        "INSERT OR IGNORE INTO v2_manual_reviews(entity_id,reason,created_at) VALUES(?,?,?)",
                        (entity_id, f"{stage} approval capability expired", now),
                    )
                    reconciled += 1
        return reconciled

    def get_candidate(self, candidate_id: str) -> V2Candidate:
        row = self._candidate_row(candidate_id)
        payload = json.loads(row["payload"])
        return V2Candidate(
            row["id"],
            payload["channel_id"],
            payload["external_post_id"],
            row["state"],
            row["policy_outcome"],
            row["policy_reason"],
            payload,
        )

    def get_draft(self, draft_id: str) -> V2Draft:
        row = self._db.execute(
            "SELECT id, candidate_id, content, state FROM v2_drafts WHERE id=?", (str(draft_id),)
        ).fetchone()
        if not row:
            raise V2WorkflowError(f"unknown draft: {draft_id}")
        return V2Draft(row["id"], row["candidate_id"], row["content"], row["state"])

    def draft_updated_at(self, draft_id: str) -> str:
        """Return the durable timestamp for the draft's current state."""
        row = self._db.execute("SELECT updated_at FROM v2_drafts WHERE id=?", (str(draft_id),)).fetchone()
        if not row:
            raise V2WorkflowError(f"unknown draft: {draft_id}")
        return str(row["updated_at"])

    def get_draft_for_candidate(self, candidate_id: str) -> V2Draft | None:
        row = self._db.execute("SELECT id FROM v2_drafts WHERE candidate_id=?", (str(candidate_id),)).fetchone()
        return None if row is None else self.get_draft(row["id"])

    def next_draft_pending_notification(self) -> V2Draft | None:
        """Select the oldest draft whose notification needs safe recovery or dispatch."""
        row = self._db.execute(
            "SELECT draft.id FROM v2_drafts draft "
            "JOIN v2_candidates candidate ON candidate.id=draft.candidate_id "
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=draft.id AND effect.stage='draft_notification' "
            "WHERE draft.state=? AND "
            + self._eligible_story_sql("candidate")
            + " AND (effect.status IS NULL OR effect.status!='confirmed') "
            "ORDER BY draft.created_at,draft.id LIMIT 1",
            (V2State.DRAFT_PENDING_APPROVAL.value,),
        ).fetchone()
        return None if row is None else self.get_draft(str(row["id"]))

    @staticmethod
    def _codex_request_from_row(row: sqlite3.Row) -> V2CodexRequest:
        return V2CodexRequest(
            str(row["digest"]),
            str(row["candidate_id"]),
            bytes(row["request_bytes"]),
            str(row["status"]),
            None if row["output_bytes"] is None else bytes(row["output_bytes"]),
            None if row["output_digest"] is None else str(row["output_digest"]),
        )

    def next_codex_candidate(self) -> V2Candidate | None:
        """Select one approved candidate with no request or one safe retry remaining."""
        row = self._db.execute(
            "SELECT candidate.id FROM v2_candidates candidate "
            "LEFT JOIN v2_codex_requests request "
            "ON request.candidate_id=candidate.id "
            "WHERE candidate.state=? AND " + self._eligible_story_sql("candidate") + " AND (request.digest IS NULL OR "
            "request.status IN ('prepared','retryable_failed')) "
            "ORDER BY candidate.created_at,candidate.id LIMIT 1",
            (V2State.CANDIDATE_APPROVED.value,),
        ).fetchone()
        return None if row is None else self.get_candidate(str(row["id"]))

    def reconcile_interrupted_codex_requests(self) -> int:
        """Fail closed after a prior activation left an attempt pending."""
        with self._db:
            rows = self._db.execute(
                "SELECT request.digest,request.candidate_id FROM v2_codex_requests request "
                "JOIN v2_candidates candidate ON candidate.id=request.candidate_id "
                "WHERE request.status='pending' AND candidate.state=?",
                (V2State.CANDIDATE_APPROVED.value,),
            ).fetchall()
            now = self._now()
            for row in rows:
                digest = str(row["digest"])
                candidate_id = str(row["candidate_id"])
                self._db.execute(
                    "UPDATE v2_codex_attempts SET status='terminal_failed',error_code='interrupted',settled_at=? "
                    "WHERE request_digest=? AND status='pending'",
                    (now, digest),
                )
                self._db.execute(
                    "UPDATE v2_codex_requests SET status='terminal_failed',error_code='interrupted',updated_at=? "
                    "WHERE digest=? AND status='pending'",
                    (now, digest),
                )
                self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.MANUAL_REVIEW.value,
                        now,
                        candidate_id,
                        V2State.CANDIDATE_APPROVED.value,
                    ),
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO v2_manual_reviews(entity_id,reason,created_at) VALUES(?,?,?)",
                    (candidate_id, "Codex generation interrupted with unknown outcome", now),
                )
        return len(rows)

    def get_codex_request(self, candidate_id: str) -> V2CodexRequest | None:
        row = self._db.execute("SELECT * FROM v2_codex_requests WHERE candidate_id=?", (str(candidate_id),)).fetchone()
        return None if row is None else self._codex_request_from_row(cast(sqlite3.Row, row))

    def prepare_codex_request(self, candidate_id: str, request_bytes: bytes, digest: str) -> V2CodexRequest:
        """Persist and bind exact canonical bytes before a launch may be recorded."""
        candidate_id, payload = str(candidate_id), bytes(request_bytes)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise V2WorkflowError("Codex request digest does not match request bytes")
        with self._db:
            existing = self.get_codex_request(candidate_id)
            if existing is not None:
                if existing.digest != digest or existing.request_bytes != payload:
                    raise V2WorkflowError("candidate is bound to different Codex request bytes")
                if existing.status == "pending":
                    raise V2WorkflowError("Codex request has an interrupted pending attempt")
                return existing
            candidate = self._candidate_row(candidate_id)
            self._require_story_eligible(candidate_id)
            if candidate["state"] != V2State.CANDIDATE_APPROVED.value or self.get_draft_for_candidate(candidate_id):
                raise V2WorkflowError("Codex request state gate failed")
            now = self._now()
            self._db.execute(
                "INSERT INTO v2_codex_requests VALUES (?,?,?,'prepared',NULL,NULL,NULL,?,?)",
                (digest, candidate_id, payload, now, now),
            )
        request = self.get_codex_request(candidate_id)
        assert request is not None
        return request

    def begin_codex_attempt(self, candidate_id: str, digest: str) -> V2CodexAttempt:
        """Append the pre-launch attempt receipt; interrupted pending work never restarts."""
        candidate_id = str(candidate_id)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            request = self.get_codex_request(candidate_id)
            if request is None or request.digest != digest:
                raise V2WorkflowError("Codex request identity mismatch")
            if request.status == "pending":
                raise V2WorkflowError("Codex request has an interrupted pending attempt")
            if request.status not in {"prepared", "retryable_failed"}:
                raise V2WorkflowError(f"cannot launch Codex request in status {request.status}")
            self._require_outbound_state(candidate_id, "draft_generation")
            self._require_story_eligible(candidate_id)
            if self.get_draft_for_candidate(candidate_id):
                raise V2WorkflowError("Codex launch state gate failed")
            count = int(
                self._db.execute(
                    "SELECT COUNT(*) AS count FROM v2_codex_attempts WHERE request_digest=?",
                    (digest,),
                ).fetchone()["count"]
            )
            if count >= 2:
                raise V2WorkflowError("Codex request attempt cap reached")
            now, number = self._now(), count + 1
            inserted = self._db.execute(
                "INSERT INTO v2_codex_attempts(request_digest,number,status,error_code,created_at,settled_at) "
                "VALUES(?,?,'pending',NULL,?,NULL)",
                (digest, number, now),
            )
            updated = self._db.execute(
                "UPDATE v2_codex_requests SET status='pending',updated_at=? WHERE digest=? AND status=?",
                (now, digest, request.status),
            )
            if updated.rowcount != 1:
                raise V2WorkflowError("Codex request launch CAS failed")
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise
        if inserted.lastrowid is None:
            raise V2WorkflowError("Codex attempt receipt was not created")
        return V2CodexAttempt(
            int(inserted.lastrowid),
            digest,
            number,
            "pending",
            None,
        )

    def settle_codex_attempt_failure(self, attempt_id: int, error_code: str, *, retryable: bool) -> str:
        """Settle an attempt; only a proven pre-dispatch network failure may retry."""
        if not error_code or any(character.isspace() for character in error_code):
            raise ValueError("Codex error code must be a nonempty safe token")
        with self._db:
            attempt = self._db.execute(
                "SELECT attempt.request_digest,attempt.number,request.candidate_id "
                "FROM v2_codex_attempts attempt "
                "JOIN v2_codex_requests request ON request.digest=attempt.request_digest "
                "WHERE attempt.id=? AND attempt.status='pending'",
                (attempt_id,),
            ).fetchone()
            if attempt is None:
                raise V2WorkflowError("Codex attempt is not pending")
            status = (
                "retryable_failed"
                if retryable and error_code == "clear_pre_dispatch_network" and int(attempt["number"]) < 2
                else "terminal_failed"
            )
            now = self._now()
            self._db.execute(
                "UPDATE v2_codex_attempts SET status=?,error_code=?,settled_at=? WHERE id=?",
                (status, error_code, now, attempt_id),
            )
            self._db.execute(
                "UPDATE v2_codex_requests SET status=?,error_code=?,updated_at=? WHERE digest=? AND status='pending'",
                (status, error_code, now, attempt["request_digest"]),
            )
            if status == "terminal_failed":
                candidate_id = str(attempt["candidate_id"])
                self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.MANUAL_REVIEW.value,
                        now,
                        candidate_id,
                        V2State.CANDIDATE_APPROVED.value,
                    ),
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO v2_manual_reviews(entity_id,reason,created_at) VALUES(?,?,?)",
                    (candidate_id, f"Codex generation failed: {error_code}", now),
                )
        return status

    def commit_codex_success(self, attempt_id: int, output_bytes: bytes, output_digest: str) -> V2Draft:
        """Atomically commit validated output, sole draft, receipts, and candidate transition."""
        output = bytes(output_bytes)
        if hashlib.sha256(output).hexdigest() != output_digest:
            raise V2WorkflowError("Codex output digest does not match output bytes")
        try:
            content = output.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise V2WorkflowError("Codex output is not UTF-8") from exc
        try:
            parsed = draft_from_mapping(json.loads(content))
            canonical = json.dumps(
                asdict(parsed),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise V2WorkflowError("Codex output is not a valid CopyDraft") from exc
        if canonical != content:
            raise V2WorkflowError("Codex output is not canonical")
        with self._db:
            attempt = self._db.execute(
                "SELECT request_digest FROM v2_codex_attempts WHERE id=? AND status='pending'", (attempt_id,)
            ).fetchone()
            if attempt is None:
                raise V2WorkflowError("Codex attempt is not pending")
            request = self._db.execute(
                "SELECT candidate_id,request_bytes FROM v2_codex_requests WHERE digest=? AND status='pending'",
                (attempt["request_digest"],),
            ).fetchone()
            if request is None:
                raise V2WorkflowError("Codex request is not pending")
            try:
                request_envelope = json.loads(bytes(request["request_bytes"]).decode("utf-8"))
                user_payload = request_envelope["user_payload"]
                evidence = user_payload["evidence"]
                allowed_claim_sources = {str(fact["id"]): int(fact["source_version_id"]) for fact in evidence}
                expected_page_count = (
                    None if user_payload["page_count_mode"] == "flexible" else int(user_payload["page_count"])
                )
                validate_copy(
                    parsed,
                    allowed_claim_sources=allowed_claim_sources,
                    expected_page_count=expected_page_count,
                )
            except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise V2WorkflowError("Codex request receipt is invalid") from exc
            candidate_id = str(request["candidate_id"])
            candidate = self._candidate_row(candidate_id)
            self._require_story_eligible(candidate_id)
            if candidate["state"] != V2State.CANDIDATE_APPROVED.value or self.get_draft_for_candidate(candidate_id):
                raise V2WorkflowError("Codex success state gate failed")
            now = self._now()
            draft_id = hashlib.sha256((candidate_id + "\0" + output_digest).encode()).hexdigest()
            self._db.execute(
                "INSERT INTO v2_drafts VALUES (?,?,?,?,?,?)",
                (draft_id, candidate_id, content, V2State.DRAFT_PENDING_APPROVAL.value, now, now),
            )
            self._db.execute(
                "UPDATE v2_codex_attempts SET status='succeeded',settled_at=? WHERE id=?", (now, attempt_id)
            )
            self._db.execute(
                "UPDATE v2_codex_requests SET status='succeeded',output_bytes=?,output_digest=?,"
                "error_code=NULL,updated_at=? WHERE digest=?",
                (output, output_digest, now, attempt["request_digest"]),
            )
            self._db.execute(
                "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                (V2State.DRAFT_PENDING_APPROVAL.value, now, candidate_id),
            )
        return self.get_draft(draft_id)

    def list_codex_attempts(self, candidate_id: str) -> list[V2CodexAttempt]:
        request = self.get_codex_request(candidate_id)
        if request is None:
            return []
        rows = self._db.execute(
            "SELECT id,request_digest,number,status,error_code FROM v2_codex_attempts "
            "WHERE request_digest=? ORDER BY number",
            (request.digest,),
        ).fetchall()
        return [
            V2CodexAttempt(
                int(row["id"]),
                str(row["request_digest"]),
                int(row["number"]),
                str(row["status"]),
                None if row["error_code"] is None else str(row["error_code"]),
            )
            for row in rows
        ]

    def list_candidates(self) -> list[V2Candidate]:
        rows = self._db.execute("SELECT id FROM v2_candidates ORDER BY created_at, id").fetchall()
        return [self.get_candidate(row["id"]) for row in rows]

    def approve_candidate(self, candidate_id: str) -> V2Candidate:
        row = self._candidate_row(candidate_id)
        state = row["state"]
        if (
            state == V2State.CANDIDATE_APPROVED.value
            or state == V2State.DRAFT_PENDING_APPROVAL.value
            or state == V2State.DRAFT_APPROVED.value
            or state == V2State.SHEET_DELIVERED.value
        ):
            return self.get_candidate(candidate_id)
        self._require_story_eligible(candidate_id)
        if state != V2State.PENDING_CANDIDATE.value:
            raise V2WorkflowError(f"cannot approve candidate in state {state}")
        with self._db:
            self._db.execute(
                "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                (V2State.CANDIDATE_APPROVED.value, self._now(), str(candidate_id)),
            )
        return self.get_candidate(candidate_id)

    def create_draft(self, candidate_id: str, content: str) -> V2Draft:
        row = self._candidate_row(candidate_id)
        self._require_story_eligible(candidate_id)
        if row["state"] != V2State.CANDIDATE_APPROVED.value:
            raise V2WorkflowError(f"cannot create draft in state {row['state']}")
        existing = self._db.execute("SELECT * FROM v2_drafts WHERE candidate_id=?", (str(candidate_id),)).fetchone()
        if existing:
            if existing["content"] != content:
                raise V2WorkflowError("draft already exists with different content")
            return V2Draft(existing["id"], existing["candidate_id"], existing["content"], existing["state"])
        draft_id = hashlib.sha256((str(candidate_id) + "\0" + content).encode()).hexdigest()[:24]
        now = self._now()
        with self._db:
            self._db.execute(
                "INSERT INTO v2_drafts VALUES (?,?,?,?,?,?)",
                (draft_id, str(candidate_id), content, V2State.DRAFT_PENDING_APPROVAL.value, now, now),
            )
            self._db.execute(
                "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                (V2State.DRAFT_PENDING_APPROVAL.value, now, str(candidate_id)),
            )
        return V2Draft(draft_id, str(candidate_id), content, V2State.DRAFT_PENDING_APPROVAL.value)

    def approve_draft(self, draft_id: str) -> V2Draft:
        row = self._db.execute("SELECT * FROM v2_drafts WHERE id=?", (str(draft_id),)).fetchone()
        if not row:
            raise V2WorkflowError(f"unknown draft: {draft_id}")
        candidate = self._candidate_row(row["candidate_id"])
        if row["state"] in (V2State.DRAFT_APPROVED.value, V2State.SHEET_DELIVERED.value):
            return V2Draft(row["id"], row["candidate_id"], row["content"], row["state"])
        self._require_story_eligible(str(row["candidate_id"]))
        if (
            row["state"] != V2State.DRAFT_PENDING_APPROVAL.value
            or candidate["state"] != V2State.DRAFT_PENDING_APPROVAL.value
        ):
            raise V2WorkflowError("cannot approve draft outside draft_pending_approval")
        with self._db:
            now = self._now()
            self._db.execute(
                "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=?",
                (V2State.DRAFT_APPROVED.value, now, str(draft_id)),
            )
            self._db.execute(
                "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                (V2State.DRAFT_APPROVED.value, now, row["candidate_id"]),
            )
        return V2Draft(row["id"], row["candidate_id"], row["content"], V2State.DRAFT_APPROVED.value)

    def mark_sheet_delivered(self, draft_id: str) -> V2Draft:
        draft_id = str(draft_id)
        current = self._db.execute(
            "SELECT d.*,c.state candidate_state,b.story_id,"
            "s.delivered_at story_delivered,claim.delivered_at claim_delivered,"
            "e.status effect_status,e.receipt_id "
            "FROM v2_drafts d "
            "JOIN v2_candidates c ON c.id=d.candidate_id "
            "LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "LEFT JOIN v2_stories s ON s.id=b.story_id "
            "LEFT JOIN v2_story_claims claim "
            "ON claim.story_id=b.story_id AND claim.candidate_id=c.id "
            "LEFT JOIN v2_remote_effects e "
            "ON e.entity_id=d.id AND e.stage='sheets_delivery' "
            "WHERE d.id=?",
            (draft_id,),
        ).fetchone()
        if current is None:
            raise V2WorkflowError(f"unknown draft: {draft_id}")
        if current["state"] == V2State.SHEET_DELIVERED.value:
            if (
                current["candidate_state"] != V2State.SHEET_DELIVERED.value
                or current["story_delivered"] is None
                or current["claim_delivered"] is None
                or current["effect_status"] != "confirmed"
                or not current["receipt_id"]
            ):
                raise V2WorkflowError("partial delivery truth requires manual review")
            return V2Draft(
                current["id"],
                current["candidate_id"],
                current["content"],
                current["state"],
            )

        self._require_story_eligible(
            str(current["candidate_id"]),
            allow_confirmed_sheets=True,
        )
        if (
            current["state"] != V2State.DRAFT_APPROVED.value
            or current["candidate_state"] != V2State.DRAFT_APPROVED.value
            or current["effect_status"] != "confirmed"
            or not current["receipt_id"]
            or current["story_id"] is None
        ):
            raise V2WorkflowError("exact confirmed Sheets evidence and delivery state are required")

        now = self._now()
        try:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute(
                "SELECT d.candidate_id,b.story_id,b.held,"
                "c.state candidate_state,d.state draft_state,"
                "s.delivered_at,s.quarantined_at,s.tombstoned_at,"
                "claim.candidate_id claimed,claim.delivered_at claim_delivered,"
                "e.status effect_status,e.receipt_id "
                "FROM v2_drafts d "
                "JOIN v2_candidates c ON c.id=d.candidate_id "
                "JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
                "JOIN v2_stories s ON s.id=b.story_id "
                "JOIN v2_story_claims claim "
                "ON claim.story_id=s.id AND claim.candidate_id=c.id "
                "JOIN v2_remote_effects e "
                "ON e.entity_id=d.id AND e.stage='sheets_delivery' "
                "WHERE d.id=?",
                (draft_id,),
            ).fetchone()
            if (
                row is None
                or row["held"]
                or row["candidate_state"] != V2State.DRAFT_APPROVED.value
                or row["draft_state"] != V2State.DRAFT_APPROVED.value
                or row["delivered_at"] is not None
                or row["quarantined_at"] is not None
                or row["tombstoned_at"] is not None
                or row["claimed"] != row["candidate_id"]
                or row["claim_delivered"] is not None
                or row["effect_status"] != "confirmed"
                or not row["receipt_id"]
            ):
                raise V2WorkflowError("delivery truth changed before atomic commit")
            candidate_id = str(row["candidate_id"])
            story_id = str(row["story_id"])
            updates = (
                self._db.execute(
                    "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.SHEET_DELIVERED.value,
                        now,
                        draft_id,
                        V2State.DRAFT_APPROVED.value,
                    ),
                ).rowcount,
                self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.SHEET_DELIVERED.value,
                        now,
                        candidate_id,
                        V2State.DRAFT_APPROVED.value,
                    ),
                ).rowcount,
                self._db.execute(
                    "UPDATE v2_stories SET delivered_at=? "
                    "WHERE id=? AND delivered_at IS NULL "
                    "AND quarantined_at IS NULL AND tombstoned_at IS NULL",
                    (now, story_id),
                ).rowcount,
                self._db.execute(
                    "UPDATE v2_story_claims SET delivered_at=? "
                    "WHERE story_id=? AND candidate_id=? "
                    "AND delivered_at IS NULL",
                    (now, story_id, candidate_id),
                ).rowcount,
            )
            if updates != (1, 1, 1, 1):
                raise V2WorkflowError("atomic delivery CAS failed")
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return V2Draft(
            draft_id,
            candidate_id,
            str(current["content"]),
            V2State.SHEET_DELIVERED.value,
        )

    def mark_manual_review(
        self,
        entity_id: str,
        reason: str,
    ) -> V2Candidate | V2Draft:
        entity_id = str(entity_id)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            draft = self._db.execute(
                "SELECT id,candidate_id,state FROM v2_drafts WHERE id=?",
                (entity_id,),
            ).fetchone()
            candidate_id = str(draft["candidate_id"]) if draft is not None else entity_id
            candidate = self._db.execute(
                "SELECT state FROM v2_candidates WHERE id=?",
                (candidate_id,),
            ).fetchone()
            if candidate is None:
                raise V2WorkflowError(f"unknown candidate: {candidate_id}")
            if draft is None:
                draft = self._db.execute(
                    "SELECT id,candidate_id,state FROM v2_drafts WHERE candidate_id=?",
                    (candidate_id,),
                ).fetchone()
            now = self._now()
            self._db.execute(
                "INSERT OR IGNORE INTO v2_manual_reviews(entity_id,reason,created_at) VALUES(?,?,?)",
                (entity_id, reason, now),
            )
            if candidate["state"] != V2State.SHEET_DELIVERED.value:
                candidate_changed = self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state!='sheet_delivered'",
                    (
                        V2State.MANUAL_REVIEW.value,
                        now,
                        candidate_id,
                    ),
                )
                if candidate_changed.rowcount != 1:
                    raise V2WorkflowError("candidate state changed during manual review")
                if draft is not None:
                    self._db.execute(
                        "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=? AND state!='sheet_delivered'",
                        (
                            V2State.MANUAL_REVIEW.value,
                            now,
                            draft["id"],
                        ),
                    )
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise
        if entity_id != candidate_id:
            return self.get_draft(entity_id)
        return self.get_candidate(entity_id)

    def channel_cursor(self, channel_id: str) -> tuple[int, tuple[datetime, int] | None]:
        row = self._db.execute(
            "SELECT new_message_high_water,edit_sweep_watermark,edit_sweep_message_id "
            "FROM v2_channel_cursors WHERE channel_id=?",
            (str(channel_id),),
        ).fetchone()
        if row is None:
            return 0, None
        watermark = None
        if row["edit_sweep_watermark"] is not None:
            watermark = (datetime.fromisoformat(str(row["edit_sweep_watermark"])), int(row["edit_sweep_message_id"]))
        return int(row["new_message_high_water"]), watermark

    def record_new_message_page(
        self, channel_id: str, observations: list[SourceObservation], *, upper_message_id: int, page_limit: int
    ) -> tuple[V2Revision, ...]:
        """Durably receipt one ascending page before moving its contiguous frontier."""
        if upper_message_id < 0 or page_limit < 1:
            raise ValueError("invalid collection page bounds")
        ids = [int(observation.external_post_id) for observation in observations]
        if (
            ids != sorted(ids)
            or len(set(ids)) != len(ids)
            or any(value < 1 or value > upper_message_id for value in ids)
        ):
            raise V2WorkflowError("new-message page must contain unique ascending IDs within its fixed bound")
        now = self._now()
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO v2_channel_cursors(channel_id,updated_at) VALUES(?,?)",
                (str(channel_id), now),
            )
            current = int(
                self._db.execute(
                    "SELECT new_message_high_water FROM v2_channel_cursors WHERE channel_id=?", (str(channel_id),)
                ).fetchone()[0]
            )
            revisions = tuple(self.record_revision(observation, transaction=False) for observation in observations)
            frontier = upper_message_id if len(observations) < page_limit else (ids[-1] if ids else current)
            frontier = max(current, frontier)
            gaps: list[tuple[str, int, int, str]] = []
            next_missing = current + 1
            for message_id in ids:
                if message_id <= current:
                    continue
                if message_id > frontier:
                    break
                if message_id > next_missing:
                    gaps.append((str(channel_id), next_missing, message_id - 1, now))
                next_missing = message_id + 1
            if next_missing <= frontier:
                gaps.append((str(channel_id), next_missing, frontier, now))
            self._db.executemany(
                "INSERT OR IGNORE INTO v2_channel_gaps("
                "channel_id,start_message_id,end_message_id,recorded_at"
                ") VALUES(?,?,?,?)",
                gaps,
            )
            self._db.execute(
                "UPDATE v2_channel_cursors SET new_message_high_water=?,updated_at=? WHERE channel_id=?",
                (frontier, now, str(channel_id)),
            )
        return revisions

    def edit_scan_state(
        self,
        channel_id: str,
    ) -> tuple[int | None, str | None]:
        row = self._db.execute(
            "SELECT edit_scan_before_message_id,edit_scan_started_at FROM v2_channel_cursors WHERE channel_id=?",
            (str(channel_id),),
        ).fetchone()
        if row is None:
            return None, None
        before = row["edit_scan_before_message_id"]
        started = row["edit_scan_started_at"]
        return (
            None if before is None else int(before),
            None if started is None else str(started),
        )

    def record_edit_sweep_page(
        self,
        channel_id: str,
        observations: list[SourceObservation],
        *,
        next_before_message_id: int | None,
        scan_started_at: str,
        complete: bool,
    ) -> tuple[V2Revision, ...]:
        """Persist one history page before moving or completing its scan cursor."""
        try:
            started = datetime.fromisoformat(scan_started_at)
        except ValueError as exc:
            raise ValueError("invalid edit scan start") from exc
        if started.tzinfo is None:
            raise ValueError("edit scan start must be timezone-aware")
        if complete and next_before_message_id is not None:
            raise ValueError("completed edit scan cannot retain a page cursor")
        if not complete and (next_before_message_id is None or next_before_message_id <= 0):
            raise ValueError("incomplete edit scan requires a positive cursor")
        ordered = sorted(
            observations,
            key=lambda item: (
                (item.edited_at or item.published_at).astimezone(UTC),
                int(item.external_post_id),
            ),
        )
        if observations != ordered or any(observation.edited_at is None for observation in observations):
            raise V2WorkflowError("edit-sweep page must contain ordered edited observations")
        now = self._now()
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO v2_channel_cursors(channel_id,edit_scan_started_at,updated_at) VALUES(?,?,?)",
                (str(channel_id), scan_started_at, now),
            )
            self._db.execute(
                "UPDATE v2_channel_cursors "
                "SET edit_scan_started_at=?,updated_at=? "
                "WHERE channel_id=? AND edit_scan_started_at IS NULL",
                (scan_started_at, now, str(channel_id)),
            )
            cursor = self._db.execute(
                "SELECT edit_sweep_watermark,edit_sweep_message_id,"
                "edit_scan_started_at,edit_scan_max_watermark,"
                "edit_scan_max_message_id "
                "FROM v2_channel_cursors WHERE channel_id=?",
                (str(channel_id),),
            ).fetchone()
            if cursor is None or cursor["edit_scan_started_at"] not in {
                None,
                scan_started_at,
            }:
                raise V2WorkflowError("edit scan changed concurrently")
            revisions = tuple(self.record_revision(observation, transaction=False) for observation in observations)
            staged: list[tuple[datetime, int]] = []
            for timestamp_key, message_key in (
                (
                    cursor["edit_sweep_watermark"],
                    cursor["edit_sweep_message_id"],
                ),
                (
                    cursor["edit_scan_max_watermark"],
                    cursor["edit_scan_max_message_id"],
                ),
            ):
                if timestamp_key is not None:
                    staged.append(
                        (
                            datetime.fromisoformat(str(timestamp_key)),
                            int(message_key),
                        )
                    )
            staged.extend(
                (
                    observation.edited_at.astimezone(UTC),
                    int(observation.external_post_id),
                )
                for observation in observations
                if observation.edited_at is not None
            )
            if complete:
                staged.append((started.astimezone(UTC), 0))
            maximum = max(staged) if staged else None
            if complete:
                assert maximum is not None
                changed = self._db.execute(
                    "UPDATE v2_channel_cursors SET "
                    "edit_sweep_watermark=?,edit_sweep_message_id=?,"
                    "edit_scan_before_message_id=NULL,"
                    "edit_scan_started_at=NULL,"
                    "edit_scan_max_watermark=NULL,"
                    "edit_scan_max_message_id=0,updated_at=? "
                    "WHERE channel_id=? "
                    "AND edit_scan_started_at IS ?",
                    (
                        self._utc_timestamp(maximum[0]),
                        maximum[1],
                        now,
                        str(channel_id),
                        scan_started_at,
                    ),
                )
            else:
                maximum_timestamp = None if maximum is None else self._utc_timestamp(maximum[0])
                maximum_message_id = 0 if maximum is None else maximum[1]
                changed = self._db.execute(
                    "UPDATE v2_channel_cursors SET "
                    "edit_scan_before_message_id=?,"
                    "edit_scan_started_at=?,"
                    "edit_scan_max_watermark=?,"
                    "edit_scan_max_message_id=?,updated_at=? "
                    "WHERE channel_id=? "
                    "AND edit_scan_started_at IS ?",
                    (
                        next_before_message_id,
                        scan_started_at,
                        maximum_timestamp,
                        maximum_message_id,
                        now,
                        str(channel_id),
                        cursor["edit_scan_started_at"],
                    ),
                )
            if changed.rowcount != 1:
                raise V2WorkflowError("edit scan cursor CAS failed")
        return revisions

    def record_revision(self, observation: SourceObservation, *, transaction: bool = True) -> V2Revision:
        identity = self._identity(observation)
        payload = self._payload(observation)
        payload["edited_at"] = self._utc_timestamp(observation.edited_at) if observation.edited_at else None
        payload["observed_at"] = self._utc_timestamp(observation.observed_at) if observation.observed_at else None
        # Collection times must not churn an otherwise identical Telegram intent.
        digest_payload = dict(payload)
        digest_payload.pop("observed_at")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256(
            json.dumps(digest_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        ).hexdigest()
        ordered = self._utc_timestamp(observation.edited_at or observation.published_at)
        observed = self._utc_timestamp(observation.observed_at or datetime.now(UTC))
        now = self._now()
        with self._db if transaction else nullcontext():
            self._db.execute(
                "INSERT OR IGNORE INTO v2_observations VALUES(?,?,?,?,?)",
                (identity, observation.channel_id, observation.external_post_id, encoded, now),
            )
            existing = self._db.execute(
                "SELECT r.*,h.revision_id=r.id desired FROM v2_observation_revisions r LEFT JOIN v2_revision_heads h ON h.identity=r.identity WHERE r.identity=? AND r.digest=?",
                (identity, digest),
            ).fetchone()
            if existing:
                self._db.execute("UPDATE v2_observations SET recorded_at=? WHERE identity=?", (now, identity))
                return V2Revision(
                    existing["id"],
                    identity,
                    existing["generation"],
                    digest,
                    existing["ordered_at"],
                    bool(existing["desired"]),
                )
            generation = int(
                self._db.execute(
                    "SELECT COALESCE(MAX(generation),0)+1 FROM v2_observation_revisions WHERE identity=?", (identity,)
                ).fetchone()[0]
            )
            result = self._db.execute(
                "INSERT INTO v2_observation_revisions(identity,generation,digest,payload,ordered_at,observed_at,created_at) VALUES(?,?,?,?,?,?,?)",
                (identity, generation, digest, encoded, ordered, observed, now),
            )
            revision_id = _inserted_id(result)
            head = self._db.execute("SELECT * FROM v2_revision_heads WHERE identity=?", (identity,)).fetchone()
            desired = head is None or (ordered, observed, generation, digest) > (
                head["ordered_at"],
                head["observed_at"],
                head["generation"],
                head["digest"],
            )
            if desired:
                self._db.execute(
                    "INSERT INTO v2_revision_heads(identity,revision_id,generation,digest,ordered_at,observed_at) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(identity) DO UPDATE SET revision_id=excluded.revision_id,generation=excluded.generation,digest=excluded.digest,ordered_at=excluded.ordered_at,observed_at=excluded.observed_at",
                    (identity, revision_id, generation, digest, ordered, observed),
                )
                self._db.execute(
                    "UPDATE v2_observations SET payload=?,recorded_at=? WHERE identity=?",
                    (encoded, now, identity),
                )
        return V2Revision(revision_id, identity, generation, digest, ordered, desired)

    def observation_has_claim(self, identity: str) -> bool:
        """Return whether an observation already owns immutable outbound work."""
        return (
            self._db.execute(
                "SELECT 1 FROM v2_candidates c "
                "LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
                "LEFT JOIN v2_story_claims claim "
                "ON claim.story_id=b.story_id AND claim.candidate_id=c.id "
                "WHERE c.observation_identity=? "
                "AND (claim.candidate_id IS NOT NULL OR c.state!=?) LIMIT 1",
                (str(identity), V2State.PENDING_CANDIDATE.value),
            ).fetchone()
            is not None
        )

    def enrichment_backlog_count(
        self,
        *,
        cap: int = 501,
        now: str | None = None,
    ) -> int:
        if cap < 1:
            raise ValueError("enrichment backlog cap must be positive")
        value = self._now() if now is None else self._utc_timestamp(now)
        row = self._db.execute(
            "SELECT COUNT(*) FROM ("
            "SELECT r.id FROM v2_observation_revisions r "
            "INDEXED BY v2_revisions_due_order "
            "JOIN v2_revision_heads h ON h.revision_id=r.id "
            "LEFT JOIN v2_enrichment_attempts a "
            "ON a.revision_id=r.id AND a.generation=h.generation "
            "AND a.attempt_number=("
            "SELECT MAX(x.attempt_number) "
            "FROM v2_enrichment_attempts x "
            "WHERE x.revision_id=r.id "
            "AND x.generation=h.generation) "
            "WHERE (a.id IS NULL OR "
            "(a.status='retryable' AND a.attempt_number<2 "
            "AND (a.next_retry_at IS NULL OR a.next_retry_at<=?)) OR "
            "(a.status='interrupted_consumed' "
            "AND a.attempt_number=1)) "
            "AND NOT EXISTS("
            "SELECT 1 FROM v2_candidates c "
            "LEFT JOIN v2_candidate_bindings b "
            "ON b.candidate_id=c.id "
            "LEFT JOIN v2_story_claims claim "
            "ON claim.story_id=b.story_id "
            "AND claim.candidate_id=c.id "
            "WHERE c.observation_identity=r.identity "
            "AND (claim.candidate_id IS NOT NULL "
            "OR c.state!='pending_candidate')) "
            "ORDER BY r.ordered_at,r.id LIMIT ?"
            ")",
            (value, cap),
        ).fetchone()
        assert row is not None
        return int(row[0])

    def claim_enrichment(
        self, owner: str, revision_id: int | None = None, now: str | None = None
    ) -> V2EnrichmentLease | None:
        if not owner:
            raise ValueError("enrichment owner must be nonempty")
        lease_owner = f"{owner}:{secrets.token_hex(16)}"
        current = datetime.now(UTC) if now is None else datetime.fromisoformat(self._utc_timestamp(now))
        value = self._utc_timestamp(current)
        with self._db:
            self._db.execute(
                "UPDATE v2_enrichment_attempts SET status='retryable',owner=NULL,leased_until=NULL WHERE status='leased' AND dispatched_at IS NULL AND leased_until<=?",
                (value,),
            )
            self._db.execute(
                "UPDATE v2_enrichment_attempts SET status='interrupted_consumed',settled_at=? WHERE status='leased' AND dispatched_at IS NOT NULL AND leased_until<=?",
                (value, value),
            )
            clauses = [
                "(a.id IS NULL OR "
                "(a.status='retryable' AND a.attempt_number<2 "
                "AND (a.next_retry_at IS NULL OR a.next_retry_at<=?)) OR "
                "(a.status='interrupted_consumed' AND a.attempt_number=1))",
                "NOT EXISTS("
                "SELECT 1 FROM v2_candidates c "
                "LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
                "LEFT JOIN v2_story_claims claim "
                "ON claim.story_id=b.story_id AND claim.candidate_id=c.id "
                "WHERE c.observation_identity=r.identity "
                "AND (claim.candidate_id IS NOT NULL "
                "OR c.state!='pending_candidate'))",
            ]
            params: list[object] = [value]
            if revision_id is not None:
                clauses.append("r.id=?")
                params.append(int(revision_id))
            row = self._db.execute(
                "SELECT r.id,h.generation,a.id attempt_id,a.attempt_number,a.status,a.settled_at "
                "FROM v2_observation_revisions r INDEXED BY v2_revisions_due_order "
                "JOIN v2_revision_heads h ON h.revision_id=r.id "
                "LEFT JOIN v2_enrichment_attempts a ON a.revision_id=r.id AND a.generation=h.generation "
                "AND a.attempt_number=(SELECT MAX(x.attempt_number) FROM v2_enrichment_attempts x WHERE x.revision_id=r.id AND x.generation=h.generation) "
                "WHERE " + " AND ".join(clauses) + " ORDER BY r.ordered_at,r.id LIMIT 1",
                tuple(params),
            ).fetchone()
            if row is None:
                return None
            if row["attempt_id"] is None:
                result = self._db.execute(
                    "INSERT INTO v2_enrichment_attempts(revision_id,generation,attempt_number,owner,status,leased_until) VALUES(?,?,1,?,'leased',?)",
                    (
                        row["id"],
                        row["generation"],
                        lease_owner,
                        self._utc_timestamp(current + timedelta(seconds=30)),
                    ),
                )
                lease_id, number = _inserted_id(result), 1
            else:
                if row["status"] == "interrupted_consumed" or row["settled_at"] is not None:
                    result = self._db.execute(
                        "INSERT INTO v2_enrichment_attempts(revision_id,generation,attempt_number,owner,status,leased_until) VALUES(?,?,2,?,'leased',?)",
                        (
                            row["id"],
                            row["generation"],
                            lease_owner,
                            self._utc_timestamp(current + timedelta(seconds=30)),
                        ),
                    )
                    lease_id, number = _inserted_id(result), 2
                else:
                    updated = self._db.execute(
                        "UPDATE v2_enrichment_attempts SET owner=?,status='leased',leased_until=?,next_retry_at=NULL WHERE id=? AND status='retryable' AND attempt_number=1 AND settled_at IS NULL",
                        (
                            lease_owner,
                            self._utc_timestamp(current + timedelta(seconds=30)),
                            row["attempt_id"],
                        ),
                    )
                    if updated.rowcount != 1:
                        return None
                    lease_id, number = int(row["attempt_id"]), int(row["attempt_number"])
        return V2EnrichmentLease(
            lease_id,
            int(row["id"]),
            int(row["generation"]),
            number,
            lease_owner,
        )

    def mark_enrichment_dispatched(self, lease: V2EnrichmentLease, now: str | None = None) -> bool:
        value = self._now() if now is None else self._utc_timestamp(now)
        with self._db:
            updated = self._db.execute(
                "UPDATE v2_enrichment_attempts SET dispatched_at=? WHERE id=? AND revision_id=? AND generation=? AND owner=? AND status='leased' AND dispatched_at IS NULL",
                (value, lease.id, lease.revision_id, lease.generation, lease.owner),
            )
        return updated.rowcount == 1

    def reconcile_expired_enrichment_leases(self, now: str | None = None) -> int:
        value = self._now() if now is None else self._utc_timestamp(now)
        with self._db:
            released = self._db.execute(
                "UPDATE v2_enrichment_attempts SET status='retryable',owner=NULL,leased_until=NULL WHERE status='leased' AND dispatched_at IS NULL AND leased_until<=?",
                (value,),
            ).rowcount
            consumed = self._db.execute(
                "UPDATE v2_enrichment_attempts SET status='interrupted_consumed',settled_at=? WHERE status='leased' AND dispatched_at IS NOT NULL AND leased_until<=?",
                (value, value),
            ).rowcount
        return released + consumed

    def settle_enrichment(
        self, lease: V2EnrichmentLease, snapshot: Any, *, transient: bool = False, now: str | None = None
    ) -> int | None:
        value = self._now() if now is None else self._utc_timestamp(now)
        data = asdict(snapshot) if hasattr(snapshot, "__dataclass_fields__") else dict(snapshot)
        with self._db:
            if (
                self._db.execute(
                    "SELECT 1 FROM v2_enrichment_attempts WHERE id=? AND revision_id=? AND generation=? AND owner=? AND status='leased'",
                    (lease.id, lease.revision_id, lease.generation, lease.owner),
                ).fetchone()
                is None
            ):
                return None
            if transient and lease.attempt_number == 1:
                self._db.execute(
                    "UPDATE v2_enrichment_attempts SET status='retryable',owner=NULL,settled_at=?,next_retry_at=? WHERE id=?",
                    (
                        value,
                        self._utc_timestamp(datetime.fromisoformat(value) + timedelta(seconds=30)),
                        lease.id,
                    ),
                )
                self._observability.emit(
                    event(
                        MetricName.FETCH,
                        labels={"result": FetchResult.TRANSIENT_FAILURE},
                        entity=lease.revision_id,
                    )
                )
                return None
            status = "succeeded" if data.get("result") == "success" else "terminal"
            self._db.execute(
                "UPDATE v2_enrichment_attempts SET status=?,settled_at=? WHERE id=?", (status, value, lease.id)
            )
            if status != "succeeded":
                return None
            result = self._db.execute(
                "INSERT INTO v2_article_snapshots(revision_id,attempt_id,snapshot,result,body_hash,created_at) VALUES(?,?,?,?,?,?)",
                (
                    lease.revision_id,
                    lease.id,
                    json.dumps(data, sort_keys=True, default=str),
                    data["result"],
                    data.get("body_hash"),
                    value,
                ),
            )
        return _inserted_id(result)

    def get_revision_observation(self, revision_id: int) -> dict[str, Any]:
        row = self._db.execute(
            "SELECT payload FROM v2_observation_revisions WHERE id=?", (int(revision_id),)
        ).fetchone()
        if row is None:
            raise V2WorkflowError(f"unknown revision: {revision_id}")
        payload = json.loads(str(row["payload"]))
        if not isinstance(payload, dict):
            raise V2WorkflowError("revision payload is not an object")
        return cast(dict[str, Any], payload)

    def candidate_evidence(self, candidate_id: str) -> dict[str, Any]:
        row = self._db.execute(
            "SELECT r.payload,s.snapshot FROM v2_candidate_bindings b JOIN v2_observation_revisions r ON r.id=b.revision_id "
            "LEFT JOIN v2_article_snapshots s ON s.id=b.snapshot_id WHERE b.candidate_id=?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise V2WorkflowError("candidate has no immutable evidence binding")
        return {
            "revision": {"payload": json.loads(row["payload"])},
            "snapshot": None if row["snapshot"] is None else json.loads(row["snapshot"]),
        }

    @staticmethod
    def _snapshot_keys(
        snapshot: dict[str, Any],
    ) -> list[tuple[str, str]]:
        """Bind only selected requested/final/canonical aliases and exact body."""
        urls = (
            snapshot.get("requested_url"),
            snapshot.get("final_url"),
            snapshot.get("canonical_url"),
        )
        body = snapshot.get("body")
        title = snapshot.get("title")
        body_hash = snapshot.get("body_hash")
        material_count = snapshot.get(
            "material_count",
        )
        if body is None:
            if body_hash is not None or material_count not in {
                None,
                0,
            }:
                raise V2WorkflowError("article snapshot body identity is incomplete")
        else:
            if not isinstance(body, str) or (title is not None and not isinstance(title, str)):
                raise V2WorkflowError("article snapshot body identity is invalid")
            expected_count = material_character_count(
                body,
                title=title,
            )
            expected_hash = body_identity(
                body,
                title=title,
            )
            if material_count != expected_count or body_hash != expected_hash:
                raise V2WorkflowError("article snapshot body identity mismatch")
        keys: list[tuple[str, str]] = []
        seen_urls: set[str] = set()
        for value in urls:
            if not isinstance(value, str) or not value:
                continue
            try:
                canonical = canonicalize_url(value)
            except UnsafeUrlError:
                continue
            if canonical in seen_urls:
                continue
            seen_urls.add(canonical)
            keys.append(
                (
                    "canonical_url_v1",
                    hashlib.sha256(canonical.encode()).hexdigest(),
                )
            )
        if isinstance(body_hash, str) and material_count is not None and int(material_count) >= 200:
            keys.append(
                (
                    "article_body_v1",
                    body_hash,
                )
            )
        return keys

    def _converge_stories(
        self,
        keys: list[tuple[str, str]],
        now: str,
        *,
        retry_unique_conflict: bool = True,
    ) -> tuple[str | None, bool]:
        """Resolve matching stories through one receipt-first evidence lattice."""
        if not keys:
            story_id = hashlib.sha256(("story\0" + now).encode()).hexdigest()[:24]
            self._db.execute(
                "INSERT INTO v2_stories(id,first_seen_at,last_seen_at) VALUES(?,?,?)", (story_id, now, now)
            )
            return story_id, False
        pairs = ",".join("(?,?)" for _ in keys)
        rows = self._db.execute(
            "SELECT DISTINCT s.id,s.first_seen_at,s.delivered_at,s.quarantined_at FROM v2_story_keys k "
            "JOIN v2_stories s ON s.id=k.story_id WHERE (k.kind,k.key_digest) IN (" + pairs + ")",
            tuple(part for key in keys for part in key),
        ).fetchall()
        story_ids = sorted({str(row["id"]) for row in rows})
        if not story_ids:
            story_id = hashlib.sha256(("story\0" + now + "\0" + keys[0][1]).encode()).hexdigest()[:24]
            self._db.execute(
                "INSERT INTO v2_stories(id,first_seen_at,last_seen_at) VALUES(?,?,?)",
                (story_id, now, now),
            )
            try:
                for kind, digest in keys:
                    self._db.execute(
                        "INSERT INTO v2_story_keys VALUES(?,?,?)",
                        (kind, digest, story_id),
                    )
            except sqlite3.IntegrityError:
                self._db.execute(
                    "DELETE FROM v2_stories WHERE id=? "
                    "AND NOT EXISTS(SELECT 1 FROM v2_candidate_bindings "
                    "WHERE story_id=?)",
                    (story_id, story_id),
                )
                if retry_unique_conflict:
                    return self._converge_stories(
                        keys,
                        now,
                        retry_unique_conflict=False,
                    )
                conflicts = self._db.execute(
                    "SELECT DISTINCT story_id FROM v2_story_keys WHERE (kind,key_digest) IN (" + pairs + ")",
                    tuple(part for key in keys for part in key),
                ).fetchall()
                self._db.executemany(
                    "UPDATE v2_stories SET quarantined_at=COALESCE(quarantined_at,?) WHERE id=?",
                    [(now, str(row["story_id"])) for row in conflicts],
                )
                self._emit_immediate_alert(
                    ImmediateAlert.IDENTITY_CONFLICT,
                    entity="story_convergence",
                )
                return None, True
            return story_id, False
        marks = ",".join("?" for _ in story_ids)
        evidence = self._db.execute(
            "SELECT b.story_id,b.candidate_id,c.state,d.state draft_state,"
            "claim.candidate_id claimed,claim.delivered_at claim_delivered,"
            "s.delivered_at story_delivered,"
            "s.quarantined_at quarantined_at,"
            "EXISTS(SELECT 1 FROM v2_remote_effects e WHERE e.entity_id IN (c.id,d.id) AND e.status IN ('pending','ambiguous')) unsafe,"
            "EXISTS(SELECT 1 FROM v2_manual_reviews m "
            "WHERE m.entity_id IN (c.id,d.id)) manual_unsafe,"
            "EXISTS(SELECT 1 FROM v2_remote_effects e WHERE e.entity_id=d.id AND e.stage='sheets_delivery' "
            "AND e.status='confirmed' AND e.receipt_id<>'') delivered_receipt,"
            "EXISTS(SELECT 1 FROM v2_callbacks cb WHERE cb.entity_id IN (c.id,d.id)) callback,"
            "EXISTS(SELECT 1 FROM v2_codex_requests q WHERE q.candidate_id=c.id) codex "
            "FROM v2_candidate_bindings b JOIN v2_candidates c ON c.id=b.candidate_id "
            "JOIN v2_stories s ON s.id=b.story_id LEFT JOIN v2_drafts d ON d.candidate_id=c.id "
            "LEFT JOIN v2_story_claims claim ON claim.story_id=b.story_id AND claim.candidate_id=c.id "
            "WHERE b.story_id IN (" + marks + ")",
            tuple(story_ids),
        ).fetchall()
        delivered: list[str] = []
        effectful: list[str] = []
        unsafe = any(row["quarantined_at"] is not None for row in rows)
        for row in evidence:
            candidate_id = str(row["candidate_id"])
            is_delivered = (
                bool(row["story_delivered"] or row["claim_delivered"] or row["delivered_receipt"])
                or row["state"] == V2State.SHEET_DELIVERED.value
                or row["draft_state"] == V2State.SHEET_DELIVERED.value
            )
            unsafe = unsafe or bool(row["unsafe"]) or bool(row["manual_unsafe"])
            if is_delivered:
                delivered.append(candidate_id)
            if is_delivered or row["claimed"] is not None or bool(row["callback"]) or bool(row["codex"]):
                effectful.append(candidate_id)
            if row["state"] != V2State.PENDING_CANDIDATE.value or row["draft_state"] is not None:
                effectful.append(candidate_id)
        delivered, effectful = sorted(set(delivered)), sorted(set(effectful))
        claimed_story_ids = {str(row["story_id"]) for row in evidence if row["claimed"] is not None}
        if unsafe or len(claimed_story_ids) > 1 or len(delivered) > 1 or (not delivered and len(effectful) > 1):
            self._db.executemany(
                "UPDATE v2_stories SET quarantined_at=COALESCE(quarantined_at,?) WHERE id=?",
                [(now, value) for value in story_ids],
            )
            self._db.executemany(
                "UPDATE v2_candidate_bindings SET held=1,hold_reason='story convergence requires manual review' WHERE story_id=?",
                [(value,) for value in story_ids],
            )
            self._emit_immediate_alert(
                ImmediateAlert.IDENTITY_CONFLICT,
                entity="story_convergence",
            )
            return None, True
        if delivered or effectful:
            winner = (delivered or effectful)[0]
            story_id = str(
                self._db.execute(
                    "SELECT story_id FROM v2_candidate_bindings WHERE candidate_id=?", (winner,)
                ).fetchone()["story_id"]
            )
        else:
            story_id = str(min(rows, key=lambda row: (str(row["first_seen_at"]), str(row["id"])))["id"])
        for loser in story_ids:
            if loser == story_id:
                continue
            self._db.execute(
                "DELETE FROM v2_story_keys WHERE story_id=? AND EXISTS (SELECT 1 FROM v2_story_keys w WHERE w.story_id=? AND w.kind=v2_story_keys.kind AND w.key_digest=v2_story_keys.key_digest)",
                (loser, story_id),
            )
            self._db.execute("UPDATE v2_story_keys SET story_id=? WHERE story_id=?", (story_id, loser))
            self._db.execute(
                "UPDATE v2_candidate_bindings SET story_id=?,held=1,hold_reason=COALESCE(hold_reason,'lost story convergence') WHERE story_id=?",
                (story_id, loser),
            )
            self._db.execute(
                "UPDATE v2_stories SET first_seen_at=MIN(first_seen_at,(SELECT first_seen_at FROM v2_stories WHERE id=?)),last_seen_at=MAX(last_seen_at,(SELECT last_seen_at FROM v2_stories WHERE id=?)),delivered_at=COALESCE(delivered_at,(SELECT delivered_at FROM v2_stories WHERE id=?)) WHERE id=?",
                (loser, loser, loser, story_id),
            )
            self._db.execute("DELETE FROM v2_story_claims WHERE story_id=?", (loser,))
            self._db.execute("UPDATE v2_stories SET tombstoned_at=? WHERE id=?", (now, loser))
            self._db.execute(
                "INSERT OR REPLACE INTO v2_story_tombstones VALUES(?,?,?,?)", (loser, story_id, "converged", now)
            )
        for kind, digest in keys:
            self._db.execute("INSERT OR IGNORE INTO v2_story_keys VALUES(?,?,?)", (kind, digest, story_id))
        if delivered:
            self._db.execute(
                "UPDATE v2_stories SET delivered_at=COALESCE(delivered_at,?) WHERE id=?",
                (now, story_id),
            )
        return story_id, False

    def finalize_enrichment(
        self, lease: V2EnrichmentLease, snapshot: Any, policy_result: Any | None = None
    ) -> V2Candidate | None:
        """Settle one exact lease, retain stale provenance, and claim through the evidence lattice."""
        data = asdict(snapshot) if hasattr(snapshot, "__dataclass_fields__") else dict(snapshot)
        value = getattr(policy_result, "outcome", data.get("policy_outcome", "non_news"))
        outcome = str(value.value if hasattr(value, "value") else value)
        reason = str(getattr(policy_result, "reason", data.get("policy_reason", "article_policy")))
        now = self._now()
        with self._db:
            attempt = self._db.execute(
                "SELECT a.*,r.identity,h.revision_id AS desired_revision_id,h.generation AS desired_generation FROM v2_enrichment_attempts a "
                "JOIN v2_observation_revisions r ON r.id=a.revision_id JOIN v2_revision_heads h ON h.identity=r.identity "
                "WHERE a.id=? AND a.owner=? AND a.revision_id=? AND a.generation=? AND a.status='leased'",
                (lease.id, lease.owner, lease.revision_id, lease.generation),
            ).fetchone()
            if attempt is None:
                return None
            attempt_status = "succeeded" if str(data.get("result", "success")) == "success" else "terminal"
            self._db.execute(
                "UPDATE v2_enrichment_attempts SET status=?,settled_at=? WHERE id=?",
                (attempt_status, now, lease.id),
            )
            snapshot_cursor = self._db.execute(
                "INSERT INTO v2_article_snapshots(revision_id,attempt_id,snapshot,result,body_hash,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    lease.revision_id,
                    lease.id,
                    json.dumps(data, sort_keys=True, default=str),
                    data.get("result", "success"),
                    data.get("body_hash"),
                    now,
                ),
            )
            snapshot_id = _inserted_id(snapshot_cursor)
            self._emit_policy_and_fetch(
                entity=attempt["identity"],
                outcome=outcome,
                reason=reason,
                fetch_result=str(data.get("result", "success")),
            )
            if (
                int(attempt["desired_revision_id"]) != lease.revision_id
                or int(attempt["desired_generation"]) != lease.generation
            ):
                return None
            canonical_url = data.get("canonical_url")
            url_hash = (
                hashlib.sha256(canonical_url.encode()).hexdigest()
                if isinstance(canonical_url, str) and canonical_url
                else None
            )
            body_hash = data.get("body_hash")
            self._db.execute(
                "INSERT INTO v2_observation_dispositions("
                "identity,revision_id,outcome,reason,result,retry_count,url_hash,body_hash,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(identity) DO UPDATE SET "
                "revision_id=excluded.revision_id,outcome=excluded.outcome,reason=excluded.reason,"
                "result=excluded.result,retry_count=excluded.retry_count,url_hash=excluded.url_hash,"
                "body_hash=excluded.body_hash,updated_at=excluded.updated_at",
                (
                    attempt["identity"],
                    lease.revision_id,
                    outcome,
                    reason,
                    str(data.get("result", "success")),
                    lease.attempt_number,
                    url_hash,
                    body_hash if isinstance(body_hash, str) else None,
                    now,
                ),
            )
            if outcome == V2Outcome.NON_NEWS.value:
                return None
            keys = self._snapshot_keys(data)
            if not keys:
                return None
            story_id, quarantined = self._converge_stories(
                keys,
                now,
            )
            if quarantined or story_id is None:
                return None
            if self._db.execute("SELECT 1 FROM v2_story_claims WHERE story_id=?", (story_id,)).fetchone():
                self._emit_immediate_alert(
                    ImmediateAlert.DUPLICATE_CLAIM,
                    entity=story_id,
                )
                return None
            candidate_id = hashlib.sha256(str(attempt["identity"]).encode()).hexdigest()[:24]
            self._db.execute(
                "INSERT OR IGNORE INTO v2_candidates VALUES(?,?,?,?,?,?,?)",
                (candidate_id, attempt["identity"], V2State.PENDING_CANDIDATE.value, outcome, reason, now, now),
            )
            self._bind_candidate(candidate_id, lease.revision_id, snapshot_id, story_id)
            self._db.execute(
                "INSERT INTO v2_story_claims(story_id,candidate_id,revision_id,snapshot_id,created_at) VALUES(?,?,?,?,?)",
                (story_id, candidate_id, lease.revision_id, snapshot_id, now),
            )
        return self.get_candidate(candidate_id)

    @staticmethod
    def _decode_status_cursor(cursor: str, state: str | None) -> tuple[str, str]:
        try:
            padding = b"=" * (-len(cursor) % 4)
            payload = base64.b64decode(
                cursor.encode("ascii") + padding,
                altchars=b"-_",
                validate=True,
            )
            decoded = json.loads(payload)
            if (
                not isinstance(decoded, dict)
                or set(decoded) != {"state", "after_created_at", "after_id"}
                or decoded["state"] != state
                or not isinstance(decoded["after_created_at"], str)
                or not isinstance(decoded["after_id"], str)
                or not decoded["after_created_at"]
                or not decoded["after_id"]
            ):
                raise ValueError
            datetime.fromisoformat(decoded["after_created_at"])
            return decoded["after_created_at"], decoded["after_id"]
        except (UnicodeError, ValueError, TypeError, json.JSONDecodeError) as error:
            raise ValueError("invalid status cursor") from error

    @staticmethod
    def _encode_status_cursor(state: str | None, created_at: str, candidate_id: str) -> str:
        payload = json.dumps(
            {
                "state": state,
                "after_created_at": created_at,
                "after_id": candidate_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")

    def _status_query(
        self,
        limit: int,
        cursor: str | None,
        state: str | None,
    ) -> tuple[str, tuple[object, ...]]:
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        if state is not None and state not in {item.value for item in V2State}:
            raise ValueError("invalid candidate state")
        after = self._decode_status_cursor(cursor, state) if cursor else None
        clauses: list[str] = []
        params: list[object] = []
        if state is not None:
            clauses.append("c.state=?")
            params.append(state)
        if after is not None:
            clauses.append("(c.created_at,c.id)>(?,?)")
            params.extend(after)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = (
            "SELECT c.*,b.revision_id,b.held,b.hold_reason,s.result fetch_result,"
            "(SELECT COUNT(*) FROM v2_enrichment_attempts a "
            "WHERE a.revision_id=b.revision_id) fetch_attempts "
            "FROM v2_candidates c "
            "LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "LEFT JOIN v2_article_snapshots s ON s.id=b.snapshot_id "
            f"{where} ORDER BY c.created_at,c.id LIMIT ?"
        )
        return query, (*params, limit + 1)

    def status_page(
        self,
        limit: int = 50,
        cursor: str | None = None,
        state: str | None = None,
    ) -> tuple[list[V2StatusItem], str | None]:
        query, params = self._status_query(limit, cursor, state)
        rows = self._db.execute(query, params).fetchall()
        items = [
            V2StatusItem(
                row["id"],
                row["state"],
                row["revision_id"],
                row["policy_outcome"],
                row["policy_reason"],
                bool(row["held"]),
                row["hold_reason"],
                row["created_at"],
                row["updated_at"],
                row["fetch_result"],
                int(row["fetch_attempts"]),
            )
            for row in rows[:limit]
        ]
        next_cursor = (
            None
            if len(rows) <= limit
            else self._encode_status_cursor(
                state,
                str(rows[limit - 1]["created_at"]),
                str(rows[limit - 1]["id"]),
            )
        )
        return items, next_cursor

    def status_aggregate(
        self,
        *,
        seven_day_storage_baseline_bytes: int | None = None,
        now: str | None = None,
    ) -> dict[str, object]:
        if seven_day_storage_baseline_bytes is None:
            raise V2WorkflowError("reviewed seven-day storage baseline is required")
        current = datetime.now(UTC) if now is None else datetime.fromisoformat(self._utc_timestamp(now))
        cap = self.STATUS_AGGREGATE_CAP
        truncated: set[str] = set()

        def bounded_count(
            name: str,
            table: str,
            where: str = "",
            params: tuple[object, ...] = (),
        ) -> int:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} {where} LIMIT ?)",
                (*params, cap + 1),
            ).fetchone()
            count = int(row[0])
            if count > cap:
                truncated.add(name)
                return cap
            return count

        candidate_states = tuple(state.value for state in V2State)
        state_counts = {
            state: bounded_count(
                f"state:{state}",
                "v2_candidates",
                "WHERE state=?",
                (state,),
            )
            for state in candidate_states
        }
        queue_counts = {
            Queue.ENRICHMENT: self.enrichment_backlog_count(
                cap=cap + 1,
            ),
            Queue.CANDIDATE_REVIEW: state_counts[V2State.PENDING_CANDIDATE.value],
            Queue.DRAFT_REVIEW: bounded_count(
                "queue:draft_review",
                "v2_drafts",
                "WHERE state='draft_pending_approval'",
            ),
            Queue.MANUAL_REVIEW: bounded_count(
                "queue:manual_review",
                "v2_manual_reviews",
            ),
            Queue.CODEX: bounded_count(
                "queue:codex",
                "v2_codex_requests",
                "WHERE status IN ('prepared','retryable_failed')",
            ),
            Queue.SHEETS: bounded_count(
                "queue:sheets",
                "v2_drafts",
                "WHERE state='draft_approved'",
            ),
        }
        if queue_counts[Queue.ENRICHMENT] > cap:
            queue_counts[Queue.ENRICHMENT] = cap
            truncated.add("queue:enrichment")
        for queue, count in queue_counts.items():
            if count:
                self._observability.emit(
                    event(
                        MetricName.QUEUE,
                        labels={"queue": queue},
                    )
                )

        fetch_window_start = self._utc_timestamp(current - timedelta(minutes=15))
        snapshot_rows = self._db.execute(
            "SELECT result FROM v2_article_snapshots WHERE created_at>=? ORDER BY created_at,id LIMIT ?",
            (fetch_window_start, cap + 1),
        ).fetchall()
        if len(snapshot_rows) > cap:
            truncated.add("fetch:snapshots")
            snapshot_rows = snapshot_rows[:cap]
        fetch_counts: dict[str, int] = {}
        for row in snapshot_rows:
            result = str(row["result"])
            fetch_counts[result] = fetch_counts.get(result, 0) + 1
        transient_rows = self._db.execute(
            "SELECT id FROM v2_enrichment_attempts "
            "WHERE status='retryable' AND settled_at>=? "
            "ORDER BY settled_at,id LIMIT ?",
            (fetch_window_start, cap + 1),
        ).fetchall()
        if len(transient_rows) > cap:
            truncated.add("fetch:transient_failure")
        transient_count = min(len(transient_rows), cap)
        if transient_count:
            fetch_counts[FetchResult.TRANSIENT_FAILURE.value] = (
                fetch_counts.get(
                    FetchResult.TRANSIENT_FAILURE.value,
                    0,
                )
                + transient_count
            )

        database_path = Path(self.database)
        database_bytes = database_path.stat().st_size if database_path.exists() else 0
        wal_path = Path(str(database_path) + "-wal")
        wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
        baseline = seven_day_storage_baseline_bytes

        oldest_values: list[str] = []
        for state in (
            V2State.PENDING_CANDIDATE.value,
            V2State.CANDIDATE_APPROVED.value,
            V2State.DRAFT_PENDING_APPROVAL.value,
            V2State.DRAFT_APPROVED.value,
        ):
            row = self._db.execute(
                "SELECT created_at FROM v2_candidates WHERE state=? ORDER BY created_at,id LIMIT 1",
                (state,),
            ).fetchone()
            if row is not None:
                oldest_values.append(str(row["created_at"]))
        for status in ("prepared", "retryable_failed"):
            row = self._db.execute(
                "SELECT created_at FROM v2_codex_requests WHERE status=? ORDER BY created_at,digest LIMIT 1",
                (status,),
            ).fetchone()
            if row is not None:
                oldest_values.append(str(row["created_at"]))
        oldest_queue = min(oldest_values) if oldest_values else None
        oldest_manual_row = self._db.execute(
            "SELECT created_at FROM v2_manual_reviews ORDER BY created_at,id LIMIT 1"
        ).fetchone()
        oldest_manual = None if oldest_manual_row is None else str(oldest_manual_row["created_at"])

        def age_seconds(timestamp: object) -> int:
            if timestamp is None:
                return 0
            value = datetime.fromisoformat(str(timestamp))
            if value.tzinfo is None:
                raise V2WorkflowError("status aggregate encountered a naive timestamp")
            return max(
                0,
                int((current.astimezone(UTC) - value.astimezone(UTC)).total_seconds()),
            )

        snapshot = ThresholdSnapshot(
            fetch_total=sum(fetch_counts.values()),
            fetch_blocked=fetch_counts.get("unsafe_url", 0),
            fetch_transient=fetch_counts.get(
                FetchResult.TRANSIENT_FAILURE.value,
                0,
            ),
            database_bytes=database_bytes,
            wal_bytes=wal_bytes,
            seven_day_storage_baseline_bytes=baseline,
            oldest_queue_age_seconds=age_seconds(oldest_queue),
            oldest_manual_review_age_seconds=age_seconds(oldest_manual),
        )
        alerts = evaluate_thresholds(snapshot)
        for alert in alerts:
            self._observability.emit(
                event(
                    MetricName.ALERT,
                    labels={"alert": alert},
                )
            )
        return {
            "states": state_counts,
            "queues": {queue.value: count for queue, count in queue_counts.items()},
            "fetch_15m": fetch_counts,
            "database_bytes": database_bytes,
            "wal_bytes": wal_bytes,
            "seven_day_storage_baseline_bytes": baseline,
            "oldest_queue_age_seconds": (snapshot.oldest_queue_age_seconds),
            "oldest_manual_review_age_seconds": (snapshot.oldest_manual_review_age_seconds),
            "aggregate_cap": cap,
            "aggregate_truncated": sorted(truncated),
            "alerts": [alert.value for alert in alerts],
        }

    def status_query_plan(
        self,
        limit: int = 50,
        cursor: str | None = None,
        state: str | None = None,
    ) -> tuple[str, ...]:
        query, params = self._status_query(limit, cursor, state)
        return tuple(str(row["detail"]) for row in self._db.execute("EXPLAIN QUERY PLAN " + query, params))

    def verify_invariants(self) -> dict[str, int]:
        delivered = self._db.execute(
            "SELECT COUNT(*) FROM v2_candidates c "
            "LEFT JOIN v2_drafts d ON d.candidate_id=c.id "
            "LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "LEFT JOIN v2_stories s ON s.id=b.story_id "
            "LEFT JOIN v2_story_claims claim "
            "ON claim.story_id=b.story_id AND claim.candidate_id=c.id "
            "LEFT JOIN v2_remote_effects e "
            "ON e.entity_id=d.id AND e.stage='sheets_delivery' "
            "WHERE s.quarantined_at IS NULL "
            "AND ("
            "c.state='sheet_delivered' "
            "OR d.state='sheet_delivered' "
            "OR (claim.candidate_id IS NOT NULL "
            "AND s.delivered_at IS NOT NULL) "
            "OR claim.delivered_at IS NOT NULL "
            "OR (e.status='confirmed' AND e.receipt_id<>'')) "
            "AND NOT ("
            "c.state='sheet_delivered' "
            "AND d.state='sheet_delivered' "
            "AND s.delivered_at IS NOT NULL "
            "AND claim.delivered_at IS NOT NULL "
            "AND e.status='confirmed' "
            "AND e.receipt_id<>'')"
        ).fetchone()[0]
        bindings = self._db.execute(
            "SELECT COUNT(*) FROM v2_candidates c LEFT JOIN v2_candidate_bindings b ON b.candidate_id=c.id WHERE b.candidate_id IS NULL OR b.revision_id IS NULL OR b.snapshot_id IS NULL"
        ).fetchone()[0]
        tombstones = self._db.execute(
            "SELECT COUNT(*) FROM v2_observation_revisions r WHERE r.payload='{}' AND NOT EXISTS(SELECT 1 FROM v2_compaction_tombstones t WHERE t.subject_kind='revision' AND t.subject_id=CAST(r.id AS TEXT) AND t.digest<>'')"
        ).fetchone()[0]
        return {
            "delivered_marker_mismatches": int(delivered),
            "candidate_binding_mismatches": int(bindings),
            "tombstone_digest_mismatches": int(tombstones),
        }

    def validation_counts(self) -> dict[str, int]:
        tables = (
            "v2_observation_revisions",
            "v2_candidates",
            "v2_remote_effects",
            "v2_callbacks",
            "v2_stories",
        )
        counts = {
            table.removeprefix("v2_"): int(self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }
        counts["held_candidates"] = int(
            self._db.execute("SELECT COUNT(*) FROM v2_candidate_bindings WHERE held=1").fetchone()[0]
        )
        return counts

    def _releasable_candidate_ids(
        self,
        *,
        held: bool,
    ) -> list[str]:
        rows = self._db.execute(
            "SELECT c.id FROM v2_candidates c "
            "JOIN v2_candidate_bindings b "
            "ON b.candidate_id=c.id "
            "JOIN v2_stories s ON s.id=b.story_id "
            "JOIN v2_story_claims claim "
            "ON claim.story_id=s.id AND claim.candidate_id=c.id "
            "WHERE c.state=? AND b.held=? "
            "AND b.revision_id IS NOT NULL "
            "AND b.snapshot_id IS NOT NULL "
            "AND s.delivered_at IS NULL "
            "AND s.quarantined_at IS NULL "
            "AND s.tombstoned_at IS NULL "
            "AND claim.delivered_at IS NULL "
            "AND EXISTS(SELECT 1 FROM v2_story_keys k "
            "WHERE k.story_id=s.id) "
            "AND NOT EXISTS(SELECT 1 FROM v2_drafts d "
            "WHERE d.candidate_id=c.id) "
            "AND NOT EXISTS(SELECT 1 FROM v2_codex_requests request "
            "WHERE request.candidate_id=c.id) "
            "AND NOT EXISTS(SELECT 1 FROM v2_manual_reviews review "
            "WHERE review.entity_id=c.id) "
            "AND NOT EXISTS(SELECT 1 FROM v2_callbacks callback "
            "WHERE callback.entity_id=c.id) "
            "AND NOT EXISTS(SELECT 1 FROM v2_remote_effects effect "
            "WHERE effect.entity_id=c.id) "
            "ORDER BY c.id",
            (
                V2State.PENDING_CANDIDATE.value,
                1 if held else 0,
            ),
        ).fetchall()
        return [str(row["id"]) for row in rows]

    def _release_manifest_items(
        self,
        ids: list[str],
    ) -> list[dict[str, str]]:
        items: list[dict[str, str]] = []
        for candidate_id in sorted(ids):
            row = self._db.execute(
                "SELECT b.story_id,r.digest revision_digest,"
                "snapshot.snapshot snapshot "
                "FROM v2_candidate_bindings b "
                "JOIN v2_observation_revisions r "
                "ON r.id=b.revision_id "
                "JOIN v2_article_snapshots snapshot "
                "ON snapshot.id=b.snapshot_id "
                "WHERE b.candidate_id=?",
                (candidate_id,),
            ).fetchone()
            if row is None:
                raise V2WorkflowError("held candidate evidence is incomplete")
            keys = self._db.execute(
                "SELECT kind,key_digest FROM v2_story_keys WHERE story_id=? ORDER BY kind,key_digest",
                (str(row["story_id"]),),
            ).fetchall()
            if not keys:
                raise V2WorkflowError("held candidate story has no durable key")
            key_digest = hashlib.sha256(
                "\n".join(f"{key['kind']}:{key['key_digest']}" for key in keys).encode()
            ).hexdigest()
            items.append(
                {
                    "id": candidate_id,
                    "revision_digest": str(row["revision_digest"]),
                    "snapshot_digest": hashlib.sha256(str(row["snapshot"]).encode()).hexdigest(),
                    "story_id": str(row["story_id"]),
                    "story_keys_digest": key_digest,
                }
            )
        return items

    @staticmethod
    def release_manifest_digest(
        items: list[dict[str, str]],
    ) -> str:
        return hashlib.sha256(
            json.dumps(
                items,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def _hold_releasable_backlog(self) -> list[str]:
        candidate_ids = self._releasable_candidate_ids(held=False)
        for candidate_id in candidate_ids:
            changed = self._db.execute(
                "UPDATE v2_candidate_bindings "
                "SET held=1,"
                "hold_reason=COALESCE("
                "hold_reason,'cutover_hold') "
                "WHERE candidate_id=? AND held=0",
                (candidate_id,),
            )
            if changed.rowcount != 1:
                raise V2WorkflowError("cutover hold changed concurrently")
        if self._releasable_candidate_ids(held=False):
            raise V2WorkflowError("migration left notification backlog unheld")
        return candidate_ids

    def hold_notification_eligible_candidates(
        self,
    ) -> dict[str, object]:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            self._hold_releasable_backlog()
            ids = self._releasable_candidate_ids(held=True)
            items = self._release_manifest_items(ids)
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return {
            "ids": ids,
            "items": items,
            "digest": self.release_manifest_digest(items),
        }

    def release_held_candidates(
        self,
        ids: list[str],
        manifest_digest: str,
    ) -> dict[str, object]:
        if len(ids) != len(set(ids)):
            raise V2WorkflowError("held candidate manifest contains duplicates")
        ordered_ids = sorted(ids)
        self._db.execute("BEGIN IMMEDIATE")
        try:
            safe = set(self._releasable_candidate_ids(held=True))
            if any(candidate_id not in safe for candidate_id in ordered_ids):
                raise V2WorkflowError("candidate is not safely releasable")
            current_items = self._release_manifest_items(ordered_ids)
            if manifest_digest != self.release_manifest_digest(current_items):
                raise V2WorkflowError("held candidate manifest mismatch")
            for candidate_id in ordered_ids:
                changed = self._db.execute(
                    "UPDATE v2_candidate_bindings SET held=0,hold_reason=NULL WHERE candidate_id=? AND held=1",
                    (candidate_id,),
                )
                if changed.rowcount != 1:
                    raise V2WorkflowError("held candidate release changed concurrently")
            remaining = self._releasable_candidate_ids(held=True)
            remaining_items = self._release_manifest_items(remaining)
            self._db.execute("COMMIT")
        except BaseException:
            self._db.execute("ROLLBACK")
            raise
        return {
            "ids": remaining,
            "items": remaining_items,
            "digest": self.release_manifest_digest(remaining_items),
        }

    @staticmethod
    def _redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
        value = dict(payload)
        for key in (
            "text",
            "title",
            "preview_title",
            "preview_description",
            "body",
            "urls",
            "url",
            "display_url",
            "full_display_url",
        ):
            value.pop(key, None)
        return value

    @staticmethod
    def _cold_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "result",
            "body_hash",
            "material_count",
            "canonical_source",
            "source_date",
            "source_date_evidence",
            "source_date_conflict",
        }
        cold = {key: snapshot[key] for key in allowed if key in snapshot}
        provenance = snapshot.get("provenance")
        if isinstance(provenance, dict):
            cold["provenance"] = {
                key: value
                for key, value in provenance.items()
                if key
                in {
                    "requested_url_hash",
                    "final_url_hash",
                    "canonical_url_hash",
                    "canonical_source",
                    "redirect_count",
                    "redirect_chain_digest",
                    "dns",
                    "peer",
                    "registrable_domain_hash",
                    "psl_version",
                    "fetched_at",
                    "status_class",
                    "mime",
                    "extractor_version",
                    "normalizer_version",
                    "body_hash",
                    "material_count",
                    "source_date_evidence",
                    "source_date_conflict",
                    "result",
                    "migration",
                }
            }
        return cold

    def _assert_compaction_invariants(self) -> None:
        failures = self.verify_invariants()
        if any(failures.values()):
            raise V2WorkflowError("compaction invariant failure: " + json.dumps(failures, sort_keys=True))
        active_evidence_loss = self._db.execute(
            "SELECT COUNT(*) FROM v2_candidates c "
            "JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "JOIN v2_observation_revisions r ON r.id=b.revision_id "
            "JOIN v2_article_snapshots snapshot ON snapshot.id=b.snapshot_id "
            "WHERE c.state!='sheet_delivered' "
            "AND (r.payload='{}' OR snapshot.snapshot='{}')"
        ).fetchone()[0]
        active_draft_loss = self._db.execute(
            "SELECT COUNT(*) FROM v2_drafts WHERE state!='sheet_delivered' AND content LIKE 'compacted:%'"
        ).fetchone()[0]
        codex_evidence_loss = self._db.execute(
            "SELECT COUNT(*) FROM v2_codex_requests request "
            "JOIN v2_candidates candidate "
            "ON candidate.id=request.candidate_id "
            "WHERE (request.status!='succeeded' "
            "OR candidate.state!='sheet_delivered') "
            "AND ("
            "hex(substr(request.request_bytes,1,10))"
            "=hex(CAST('compacted:' AS BLOB)) "
            "OR (request.output_bytes IS NOT NULL AND "
            "hex(substr(request.output_bytes,1,10))"
            "=hex(CAST('compacted:' AS BLOB))))"
        ).fetchone()[0]
        delivered_without_truth = self._db.execute(
            "SELECT COUNT(*) FROM v2_stories s "
            "WHERE s.delivered_at IS NOT NULL "
            "AND s.quarantined_at IS NULL "
            "AND ("
            "NOT EXISTS(SELECT 1 FROM v2_story_keys k "
            "WHERE k.story_id=s.id) OR "
            "NOT EXISTS(SELECT 1 FROM v2_story_claims claim "
            "WHERE claim.story_id=s.id "
            "AND claim.delivered_at IS NOT NULL)"
            ")"
        ).fetchone()[0]
        retry_evidence_loss = self._db.execute(
            "SELECT COUNT(*) FROM v2_remote_effects "
            "WHERE (status IN ('pending','ambiguous') "
            "OR (status='failed' AND attempts<2)) "
            "AND detail LIKE 'compacted:%'"
        ).fetchone()[0]
        live_callback_loss = self._db.execute(
            "SELECT COUNT(*) FROM v2_callbacks WHERE consumed_at IS NULL AND (length(token_hash)!=64 OR expires_at='')"
        ).fetchone()[0]
        outbound_marker_conflicts = self._db.execute(
            "SELECT COUNT(DISTINCT c.id) FROM v2_candidates c "
            "JOIN v2_candidate_bindings b ON b.candidate_id=c.id "
            "JOIN v2_stories s ON s.id=b.story_id "
            "LEFT JOIN v2_story_claims claim "
            "ON claim.story_id=s.id AND claim.candidate_id=c.id "
            "LEFT JOIN v2_drafts d ON d.candidate_id=c.id "
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=d.id AND effect.stage='sheets_delivery' "
            "WHERE c.state IN ("
            "'pending_candidate','candidate_approved',"
            "'draft_pending_approval','draft_approved') "
            "AND (s.delivered_at IS NOT NULL "
            "OR claim.delivered_at IS NOT NULL "
            "OR d.state='sheet_delivered' "
            "OR (effect.status='confirmed' AND effect.receipt_id<>''))"
        ).fetchone()[0]
        if (
            active_evidence_loss
            or active_draft_loss
            or codex_evidence_loss
            or delivered_without_truth
            or retry_evidence_loss
            or live_callback_loss
            or outbound_marker_conflicts
        ):
            raise self._migration_retention_error("compaction would erase protected evidence or expose delivered work")

    def compact(
        self,
        batch_size: int = 500,
        dry_run: bool = False,
        now: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= batch_size <= 500:
            raise ValueError("batch size must be between 1 and 500")
        current = datetime.now(UTC) if now is None else datetime.fromisoformat(self._utc_timestamp(now))
        cold = self._utc_timestamp(current - timedelta(days=30))
        stale = self._utc_timestamp(current - timedelta(days=7))
        started = time.monotonic()
        if not dry_run:
            self._db.execute("BEGIN IMMEDIATE")
        try:
            self._assert_compaction_invariants()
            delivered = self._db.execute(
                "SELECT r.id,r.identity,r.digest,r.ordered_at,"
                "r.payload,o.payload observation_payload,"
                "c.id candidate_id,c.policy_outcome,c.policy_reason,"
                "snap.id snapshot_id,snap.snapshot,snap.body_hash,"
                "a.status attempt_status "
                "FROM v2_observation_revisions r "
                "JOIN v2_candidates c "
                "ON c.observation_identity=r.identity "
                "JOIN v2_observations o ON o.identity=r.identity "
                "JOIN v2_candidate_bindings b "
                "ON b.candidate_id=c.id AND b.revision_id=r.id "
                "LEFT JOIN v2_article_snapshots snap "
                "ON snap.id=b.snapshot_id "
                "LEFT JOIN v2_enrichment_attempts a "
                "ON a.id=snap.attempt_id "
                "WHERE r.created_at<? "
                "AND c.state='sheet_delivered' "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_remote_effects e "
                "LEFT JOIN v2_drafts d ON d.id=e.entity_id "
                "WHERE (e.entity_id=c.id OR d.candidate_id=c.id) "
                "AND e.status IN ('pending','ambiguous')) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_manual_reviews m "
                "WHERE m.entity_id=c.id OR m.entity_id IN ("
                "SELECT d.id FROM v2_drafts d "
                "WHERE d.candidate_id=c.id)) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_callbacks cb "
                "WHERE cb.consumed_at IS NULL AND ("
                "cb.entity_id=c.id OR cb.entity_id IN ("
                "SELECT d.id FROM v2_drafts d "
                "WHERE d.candidate_id=c.id))) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_compaction_tombstones t "
                "WHERE t.subject_kind='revision' "
                "AND t.subject_id=CAST(r.id AS TEXT)) "
                "ORDER BY r.created_at,r.id LIMIT ?",
                (cold, batch_size),
            ).fetchall()
            remaining = batch_size - len(delivered)
            terminal = self._db.execute(
                "SELECT r.id,r.identity,r.digest,r.ordered_at,"
                "r.payload,o.payload observation_payload,"
                "NULL candidate_id,d.outcome policy_outcome,"
                "d.reason policy_reason,snap.id snapshot_id,"
                "snap.snapshot,snap.body_hash,"
                "a.status attempt_status "
                "FROM v2_observation_revisions r "
                "JOIN v2_revision_heads h ON h.revision_id=r.id "
                "JOIN v2_observations o ON o.identity=r.identity "
                "JOIN v2_observation_dispositions d "
                "ON d.identity=r.identity AND d.revision_id=r.id "
                "LEFT JOIN v2_candidates c "
                "ON c.observation_identity=r.identity "
                "LEFT JOIN v2_article_snapshots snap "
                "ON snap.revision_id=r.id "
                "LEFT JOIN v2_enrichment_attempts a "
                "ON a.id=snap.attempt_id "
                "WHERE c.id IS NULL AND d.outcome='non_news' "
                "AND r.created_at<? "
                "AND a.status IN ('terminal','succeeded') "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_compaction_tombstones t "
                "WHERE t.subject_kind='revision' "
                "AND t.subject_id=CAST(r.id AS TEXT)) "
                "ORDER BY r.created_at,r.id LIMIT ?",
                (cold, remaining),
            ).fetchall()
            remaining -= len(terminal)
            superseded = self._db.execute(
                "SELECT r.id,r.digest "
                "FROM v2_observation_revisions r "
                "LEFT JOIN v2_revision_heads h "
                "ON h.revision_id=r.id "
                "WHERE h.revision_id IS NULL AND r.created_at<? "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_candidate_bindings b "
                "WHERE b.revision_id=r.id) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_compaction_tombstones t "
                "WHERE t.subject_kind='revision' "
                "AND t.subject_id=CAST(r.id AS TEXT)) "
                "ORDER BY r.created_at,r.id LIMIT ?",
                (stale, remaining),
            ).fetchall()
            remaining -= len(superseded)
            callback_predicate = (
                "cb.expires_at<? "
                "AND ("
                "EXISTS(SELECT 1 FROM v2_candidates c "
                "WHERE c.id=cb.entity_id "
                "AND c.state IN ('sheet_delivered','manual_review')) "
                "OR EXISTS("
                "SELECT 1 FROM v2_drafts d "
                "JOIN v2_candidates c ON c.id=d.candidate_id "
                "WHERE d.id=cb.entity_id "
                "AND d.state IN ('sheet_delivered','manual_review') "
                "AND c.state IN ('sheet_delivered','manual_review'))"
                ") AND NOT EXISTS("
                "SELECT 1 FROM v2_remote_effects e "
                "WHERE e.status IN ('pending','ambiguous') AND ("
                "e.entity_id=cb.entity_id OR "
                "e.entity_id IN (SELECT d.id FROM v2_drafts d "
                "WHERE d.candidate_id=cb.entity_id) OR "
                "e.entity_id IN (SELECT d.candidate_id "
                "FROM v2_drafts d WHERE d.id=cb.entity_id)))"
            )
            callbacks = self._db.execute(
                "SELECT cb.token_hash FROM v2_callbacks cb "
                f"WHERE {callback_predicate} "
                "ORDER BY cb.expires_at,cb.token_hash LIMIT ?",
                (cold, remaining),
            ).fetchall()
            remaining -= len(callbacks)
            effects = self._db.execute(
                "SELECT entity_id,stage,detail "
                "FROM v2_remote_effects "
                "WHERE status='confirmed' AND updated_at<? "
                "AND detail NOT LIKE 'compacted:%' "
                "ORDER BY updated_at,entity_id,stage LIMIT ?",
                (cold, remaining),
            ).fetchall()
            remaining -= len(effects)
            drafts = self._db.execute(
                "SELECT d.id,d.content,d.candidate_id "
                "FROM v2_drafts d "
                "JOIN v2_candidates c ON c.id=d.candidate_id "
                "WHERE d.state='sheet_delivered' "
                "AND c.state='sheet_delivered' "
                "AND d.updated_at<? "
                "AND d.content NOT LIKE 'compacted:%' "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_manual_reviews m "
                "WHERE m.entity_id IN (d.id,d.candidate_id)) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_remote_effects e "
                "WHERE e.status IN ('pending','ambiguous') "
                "AND e.entity_id IN (d.id,d.candidate_id)) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_callbacks cb "
                "WHERE cb.entity_id IN (d.id,d.candidate_id) "
                "AND cb.consumed_at IS NULL AND cb.expires_at>=?) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_compaction_tombstones t "
                "WHERE t.subject_kind='draft' "
                "AND t.subject_id=d.id) "
                "ORDER BY d.updated_at,d.id LIMIT ?",
                (
                    cold,
                    self._utc_timestamp(current),
                    remaining,
                ),
            ).fetchall()
            remaining -= len(drafts)
            codex_requests = self._db.execute(
                "SELECT request.digest,request.candidate_id,"
                "request.request_bytes,request.output_bytes,"
                "request.output_digest,request.status "
                "FROM v2_codex_requests request "
                "JOIN v2_candidates c "
                "ON c.id=request.candidate_id "
                "WHERE c.state='sheet_delivered' "
                "AND request.status='succeeded' "
                "AND request.updated_at<? "
                "AND ("
                "hex(substr(request.request_bytes,1,10))"
                "<>hex(CAST('compacted:' AS BLOB)) "
                "OR (request.output_bytes IS NOT NULL AND "
                "hex(substr(request.output_bytes,1,10))"
                "<>hex(CAST('compacted:' AS BLOB)))) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_manual_reviews m "
                "WHERE m.entity_id=request.candidate_id) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_remote_effects e "
                "LEFT JOIN v2_drafts d ON d.id=e.entity_id "
                "WHERE e.status IN ('pending','ambiguous') "
                "AND (e.entity_id=request.candidate_id "
                "OR d.candidate_id=request.candidate_id)) "
                "AND NOT EXISTS("
                "SELECT 1 FROM v2_compaction_tombstones t "
                "WHERE t.subject_kind='codex_request' "
                "AND t.subject_id=request.digest) "
                "ORDER BY request.updated_at,request.digest "
                "LIMIT ?",
                (cold, remaining),
            ).fetchall()
            remaining -= len(codex_requests)
            rows = [*delivered, *terminal]
            touched = len(rows) + len(superseded) + len(callbacks) + len(effects) + len(drafts) + len(codex_requests)
            plan: dict[str, Any] = {
                "eligible": touched,
                "compacted": 0 if dry_run else touched,
                "hot_cold": [int(row["id"]) for row in rows],
                "superseded": [int(row["id"]) for row in superseded],
                "callbacks": len(callbacks),
                "effects": len(effects),
                "drafts": len(drafts),
                "codex_requests": len(codex_requests),
            }
            if dry_run:
                self._emit_compaction_plan(plan, dry_run=True)
                return plan

            for row in rows:
                if time.monotonic() - started > 2:
                    raise V2WorkflowError("compaction transaction deadline exceeded")
                snapshot = {} if row["snapshot"] is None else json.loads(str(row["snapshot"]))
                provenance = {
                    "revision_digest": row["digest"],
                    "ordered_at": row["ordered_at"],
                    "candidate_id": row["candidate_id"],
                    "outcome": row["policy_outcome"],
                    "reason": row["policy_reason"],
                    "attempt_status": row["attempt_status"],
                    "body_hash": row["body_hash"],
                    "material_count": snapshot.get(
                        "material_count",
                    ),
                }
                encoded = json.dumps(
                    provenance,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO v2_compaction_tombstones VALUES('revision',?,?,?,?)",
                    (
                        str(row["id"]),
                        hashlib.sha256(encoded.encode()).hexdigest(),
                        encoded,
                        self._utc_timestamp(current),
                    ),
                )
                changed = self._db.execute(
                    "UPDATE v2_observation_revisions SET payload='{}' WHERE id=? AND payload!='{}'",
                    (row["id"],),
                )
                if changed.rowcount != 1:
                    raise V2WorkflowError("compaction revision plan changed")
                observation_payload = json.loads(str(row["observation_payload"]))
                self._db.execute(
                    "UPDATE v2_observations SET payload=? WHERE identity=?",
                    (
                        json.dumps(
                            self._redact_payload(observation_payload),
                            sort_keys=True,
                        ),
                        row["identity"],
                    ),
                )
                if row["snapshot_id"] is not None:
                    self._db.execute(
                        "UPDATE v2_article_snapshots SET snapshot=? WHERE id=?",
                        (
                            json.dumps(
                                self._cold_snapshot(snapshot),
                                sort_keys=True,
                            ),
                            row["snapshot_id"],
                        ),
                    )
            for row in superseded:
                if time.monotonic() - started > 2:
                    raise V2WorkflowError("compaction transaction deadline exceeded")
                encoded = json.dumps(
                    {"revision_digest": row["digest"]},
                    sort_keys=True,
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO v2_compaction_tombstones VALUES('revision',?,?,?,?)",
                    (
                        str(row["id"]),
                        hashlib.sha256(encoded.encode()).hexdigest(),
                        encoded,
                        self._utc_timestamp(current),
                    ),
                )
                changed = self._db.execute(
                    "UPDATE v2_observation_revisions SET payload='{}' WHERE id=? AND payload!='{}'",
                    (row["id"],),
                )
                if changed.rowcount != 1:
                    raise V2WorkflowError("compaction superseded plan changed")
            for callback in callbacks:
                deleted = self._db.execute(
                    "DELETE FROM v2_callbacks WHERE token_hash=?",
                    (callback["token_hash"],),
                )
                if deleted.rowcount != 1:
                    raise V2WorkflowError("compaction callback plan changed")
            for effect in effects:
                detail = str(effect["detail"])
                changed = self._db.execute(
                    "UPDATE v2_remote_effects SET detail=? "
                    "WHERE entity_id=? AND stage=? "
                    "AND status='confirmed' AND detail=?",
                    (
                        "compacted:" + hashlib.sha256(detail.encode()).hexdigest(),
                        effect["entity_id"],
                        effect["stage"],
                        detail,
                    ),
                )
                if changed.rowcount != 1:
                    raise V2WorkflowError("compaction effect plan changed")
            for draft in drafts:
                content = str(draft["content"])
                content_digest = hashlib.sha256(content.encode()).hexdigest()
                compacted = "compacted:" + content_digest
                draft_provenance = json.dumps(
                    {
                        "candidate_id": draft["candidate_id"],
                        "content_digest": content_digest,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO v2_compaction_tombstones VALUES('draft',?,?,?,?)",
                    (
                        draft["id"],
                        hashlib.sha256(draft_provenance.encode()).hexdigest(),
                        draft_provenance,
                        self._utc_timestamp(current),
                    ),
                )
                changed = self._db.execute(
                    "UPDATE v2_drafts SET content=? WHERE id=? AND content=? AND state='sheet_delivered'",
                    (compacted, draft["id"], content),
                )
                if changed.rowcount != 1:
                    raise V2WorkflowError("compaction draft plan changed")
            for request in codex_requests:
                request_bytes = bytes(request["request_bytes"])
                output_bytes = None if request["output_bytes"] is None else bytes(request["output_bytes"])
                compacted_request = b"compacted:" + hashlib.sha256(request_bytes).hexdigest().encode()
                compacted_output = (
                    None if output_bytes is None else b"compacted:" + hashlib.sha256(output_bytes).hexdigest().encode()
                )
                codex_provenance = json.dumps(
                    {
                        "candidate_id": request["candidate_id"],
                        "request_digest": request["digest"],
                        "output_digest": request["output_digest"],
                        "status": request["status"],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                self._db.execute(
                    "INSERT OR IGNORE INTO v2_compaction_tombstones VALUES('codex_request',?,?,?,?)",
                    (
                        request["digest"],
                        hashlib.sha256(codex_provenance.encode()).hexdigest(),
                        codex_provenance,
                        self._utc_timestamp(current),
                    ),
                )
                changed = self._db.execute(
                    "UPDATE v2_codex_requests "
                    "SET request_bytes=?,output_bytes=? "
                    "WHERE digest=? AND request_bytes=? "
                    "AND ((output_bytes IS NULL AND ? IS NULL) "
                    "OR output_bytes=?) AND status='succeeded'",
                    (
                        compacted_request,
                        compacted_output,
                        request["digest"],
                        request_bytes,
                        output_bytes,
                        output_bytes,
                    ),
                )
                if changed.rowcount != 1:
                    raise V2WorkflowError("compaction Codex request plan changed")
            self._assert_compaction_invariants()
            if time.monotonic() - started > 2:
                raise V2WorkflowError("compaction transaction deadline exceeded")
            self._db.execute("COMMIT")
            self._emit_compaction_plan(plan, dry_run=False)
            return plan
        except BaseException:
            if not dry_run:
                self._db.execute("ROLLBACK")
            raise

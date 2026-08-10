"""Minimal, independent SQLite workflow for Newsbot V2."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from .collectors.base import SourceObservation
from .v2_policy import V2Outcome, V2Policy


class V2State(StrEnum):
    PENDING_CANDIDATE = "pending_candidate"
    CANDIDATE_APPROVED = "candidate_approved"
    DRAFT_PENDING_APPROVAL = "draft_pending_approval"
    DRAFT_APPROVED = "draft_approved"
    SHEET_DELIVERED = "sheet_delivered"
    MANUAL_REVIEW = "manual_review"


class V2WorkflowError(RuntimeError):
    """Raised when an operation cannot be applied to the current state."""


InvalidTransitionError = V2WorkflowError
WorkflowState = V2State


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


class V2Workflow:
    """A small state machine; this database is never the legacy Newsbot DB."""

    def __init__(
        self, database: str | Path | None = None, *, db_path: str | Path | None = None, policy: V2Policy | None = None
    ):
        if database is None:
            database = db_path
        if database is None:
            raise TypeError("database path is required")
        self.database = str(database)
        self.policy = policy or V2Policy()
        self._db = sqlite3.connect(self.database)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._assert_v2_database()
        self.initialize()

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> V2Workflow:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _assert_v2_database(self) -> None:
        tables = {
            row["name"] for row in self._db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        expected = {
            "sqlite_sequence",
            "v2_metadata",
            "v2_remote_effects",
            "v2_observations",
            "v2_candidates",
            "v2_drafts",
            "v2_manual_reviews",
            "v2_callbacks",
        }
        if tables and "v2_metadata" not in tables:
            raise V2WorkflowError("refusing to open a database without a Newsbot V2 identity marker")
        if "v2_metadata" in tables:
            marker = self._db.execute("SELECT value FROM v2_metadata WHERE key='schema'").fetchone()
            if marker is None or marker["value"] != "newsbot-v2-workflow-v1" or not tables.issubset(expected):
                raise V2WorkflowError("database has an invalid or mixed Newsbot V2 schema")

    def initialize(self) -> None:
        self._db.executescript("""
        CREATE TABLE IF NOT EXISTS v2_metadata (
            key TEXT PRIMARY KEY, value TEXT NOT NULL
        );
        INSERT OR IGNORE INTO v2_metadata(key, value) VALUES ('schema', 'newsbot-v2-workflow-v1');
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
        """)
        columns = {row["name"] for row in self._db.execute("PRAGMA table_info(v2_remote_effects)")}
        if "receipt_id" not in columns:
            self._db.execute("ALTER TABLE v2_remote_effects ADD COLUMN receipt_id TEXT NOT NULL DEFAULT ''")
        self._db.commit()

    @staticmethod
    def _identity(observation: SourceObservation) -> str:
        return f"{observation.channel_id}:{observation.external_post_id}"

    @staticmethod
    def _now() -> str:
        return datetime.now().astimezone().isoformat()

    @staticmethod
    def _payload(observation: SourceObservation) -> dict[str, Any]:
        return {
            "channel_id": observation.channel_id,
            "channel_handle": observation.channel_handle,
            "external_post_id": observation.external_post_id,
            "published_at": observation.published_at.isoformat(),
            "text": observation.text,
            "urls": [u.url for u in observation.urls],
        }

    def record_observation(self, observation: SourceObservation) -> V2Candidate | None:
        """Record one observation and create at most one eligible candidate."""
        identity = self._identity(observation)
        existing = self._db.execute("SELECT id FROM v2_candidates WHERE observation_identity=?", (identity,)).fetchone()
        if existing:
            return self.get_candidate(existing["id"])
        result = self.policy.evaluate(observation)
        now = self._now()
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO v2_observations VALUES (?,?,?,?,?)",
                (
                    identity,
                    observation.channel_id,
                    observation.external_post_id,
                    json.dumps(self._payload(observation), sort_keys=True),
                    now,
                ),
            )
            if result.outcome is V2Outcome.NON_NEWS:
                return None
            candidate_id = hashlib.sha256(identity.encode()).hexdigest()[:24]
            self._db.execute(
                "INSERT OR IGNORE INTO v2_candidates VALUES (?,?,?,?,?,?,?)",
                (
                    candidate_id,
                    identity,
                    V2State.PENDING_CANDIDATE.value,
                    result.outcome.value,
                    result.reason,
                    now,
                    now,
                ),
            )
        return self.get_candidate(candidate_id)

    def _candidate_row(self, candidate_id: str) -> sqlite3.Row:
        row = self._db.execute(
            "SELECT c.*,o.payload FROM v2_candidates c JOIN v2_observations o ON o.identity=c.observation_identity WHERE c.id=?",
            (str(candidate_id),),
        ).fetchone()
        if not row:
            raise V2WorkflowError(f"unknown candidate: {candidate_id}")
        return cast(sqlite3.Row, row)

    def record_remote_attempt(self, entity_id: str, stage: str) -> int:
        now = self._now()
        with self._db:
            self._db.execute(
                "INSERT INTO v2_remote_effects(entity_id,stage,attempts,status,detail,receipt_id,updated_at) VALUES(?,?,1,'pending','','',?) "
                "ON CONFLICT(entity_id,stage) DO UPDATE SET attempts=attempts+1,status='pending',updated_at=excluded.updated_at",
                (str(entity_id), str(stage), now),
            )
        row = self._db.execute(
            "SELECT attempts FROM v2_remote_effects WHERE entity_id=? AND stage=?", (str(entity_id), str(stage))
        ).fetchone()
        return int(row["attempts"])

    def settle_remote_effect(
        self, entity_id: str, stage: str, status: str, detail: str = "", receipt_id: str = ""
    ) -> None:
        if status not in {"confirmed", "ambiguous", "failed"}:
            raise ValueError("invalid remote effect status")
        with self._db:
            self._db.execute(
                "UPDATE v2_remote_effects SET status=?,detail=?,receipt_id=?,updated_at=? WHERE entity_id=? AND stage=?",
                (status, str(detail), str(receipt_id), self._now(), str(entity_id), str(stage)),
            )

    def remote_effect(self, entity_id: str, stage: str) -> dict[str, object] | None:
        row = self._db.execute(
            "SELECT * FROM v2_remote_effects WHERE entity_id=? AND stage=?", (str(entity_id), str(stage))
        ).fetchone()
        return None if row is None else dict(row)

    def issue_callback(self, token_hash: str, entity_id: str, stage: str, expires_at: str) -> None:
        """Persist only a callback capability digest, never its transport value."""
        if stage not in {"candidate", "draft"} or len(token_hash) != 64:
            raise ValueError("invalid callback receipt")
        with self._db:
            self._db.execute(
                "INSERT INTO v2_callbacks(token_hash,entity_id,stage,expires_at,consumed_at) VALUES(?,?,?,?,NULL)",
                (token_hash, str(entity_id), stage, expires_at),
            )

    def consume_callback(self, token_hash: str, stage: str, now: str) -> str | None:
        """Atomically consume an unexpired capability for its expected gate."""
        with self._db:
            row = self._db.execute(
                "SELECT entity_id FROM v2_callbacks WHERE token_hash=? AND stage=? "
                "AND consumed_at IS NULL AND expires_at>?",
                (token_hash, stage, now),
            ).fetchone()
            if row is None:
                return None
            self._db.execute("UPDATE v2_callbacks SET consumed_at=? WHERE token_hash=?", (now, token_hash))
        return str(row["entity_id"])

    def consume_callback_any(self, token_hash: str, now: str) -> tuple[str, str] | None:
        """Atomically consume an unexpired capability and return its bound entity and gate."""
        with self._db:
            row = self._db.execute(
                "SELECT entity_id,stage FROM v2_callbacks WHERE token_hash=? AND consumed_at IS NULL AND expires_at>?",
                (token_hash, now),
            ).fetchone()
            if row is None:
                return None
            self._db.execute("UPDATE v2_callbacks SET consumed_at=? WHERE token_hash=?", (now, token_hash))
        return str(row["entity_id"]), str(row["stage"])

    def settle_callback_any(self, token_hash: str, now: str) -> tuple[str, str] | None:
        """Consume and apply one bound approval gate in the same transaction."""
        with self._db:
            callback = self._db.execute(
                "SELECT entity_id,stage FROM v2_callbacks WHERE token_hash=? AND consumed_at IS NULL AND expires_at>?",
                (token_hash, now),
            ).fetchone()
            if callback is None:
                return None
            entity_id = str(callback["entity_id"])
            stage = str(callback["stage"])
            if stage == "candidate":
                updated = self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.CANDIDATE_APPROVED.value,
                        now,
                        entity_id,
                        V2State.PENDING_CANDIDATE.value,
                    ),
                )
                if updated.rowcount != 1:
                    return None
            elif stage == "draft":
                draft = self._db.execute(
                    "SELECT candidate_id FROM v2_drafts WHERE id=? AND state=?",
                    (entity_id, V2State.DRAFT_PENDING_APPROVAL.value),
                ).fetchone()
                if draft is None:
                    return None
                candidate_id = str(draft["candidate_id"])
                updated = self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=? AND state=?",
                    (
                        V2State.DRAFT_APPROVED.value,
                        now,
                        candidate_id,
                        V2State.DRAFT_PENDING_APPROVAL.value,
                    ),
                )
                if updated.rowcount != 1:
                    return None
                self._db.execute(
                    "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=?",
                    (V2State.DRAFT_APPROVED.value, now, entity_id),
                )
            else:
                return None
            self._db.execute("UPDATE v2_callbacks SET consumed_at=? WHERE token_hash=?", (now, token_hash))
        return entity_id, stage

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

    def get_draft_for_candidate(self, candidate_id: str) -> V2Draft | None:
        row = self._db.execute("SELECT id FROM v2_drafts WHERE candidate_id=?", (str(candidate_id),)).fetchone()
        return None if row is None else self.get_draft(row["id"])

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
        row = self._db.execute("SELECT * FROM v2_drafts WHERE id=?", (str(draft_id),)).fetchone()
        if not row:
            raise V2WorkflowError(f"unknown draft: {draft_id}")
        candidate = self._candidate_row(row["candidate_id"])
        if row["state"] == V2State.SHEET_DELIVERED.value:
            return V2Draft(row["id"], row["candidate_id"], row["content"], row["state"])
        if row["state"] != V2State.DRAFT_APPROVED.value or candidate["state"] != V2State.DRAFT_APPROVED.value:
            raise V2WorkflowError("cannot deliver sheet outside draft_approved")
        with self._db:
            now = self._now()
            self._db.execute(
                "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=?",
                (V2State.SHEET_DELIVERED.value, now, str(draft_id)),
            )
            self._db.execute(
                "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                (V2State.SHEET_DELIVERED.value, now, row["candidate_id"]),
            )
        return V2Draft(row["id"], row["candidate_id"], row["content"], V2State.SHEET_DELIVERED.value)

    def mark_manual_review(self, entity_id: str, reason: str) -> V2Candidate | V2Draft:
        entity_id = str(entity_id)
        draft = self._db.execute("SELECT * FROM v2_drafts WHERE id=?", (entity_id,)).fetchone()
        candidate_id = draft["candidate_id"] if draft else entity_id
        if draft is None:
            self._candidate_row(candidate_id)
            draft = self._db.execute("SELECT * FROM v2_drafts WHERE candidate_id=?", (candidate_id,)).fetchone()
        candidate = self._candidate_row(candidate_id)
        now = self._now()
        with self._db:
            self._db.execute(
                "INSERT OR IGNORE INTO v2_manual_reviews(entity_id,reason,created_at) VALUES(?,?,?)",
                (entity_id, reason, now),
            )
            if candidate["state"] != V2State.SHEET_DELIVERED.value:
                self._db.execute(
                    "UPDATE v2_candidates SET state=?,updated_at=? WHERE id=?",
                    (V2State.MANUAL_REVIEW.value, now, candidate_id),
                )
                if draft:
                    self._db.execute(
                        "UPDATE v2_drafts SET state=?,updated_at=? WHERE id=?",
                        (V2State.MANUAL_REVIEW.value, now, draft["id"]),
                    )
        if entity_id != candidate_id:
            state = (
                V2State.SHEET_DELIVERED.value
                if candidate["state"] == V2State.SHEET_DELIVERED.value
                else V2State.MANUAL_REVIEW.value
            )
            return V2Draft(draft["id"], draft["candidate_id"], draft["content"], state)
        return self.get_candidate(entity_id)

    # Convenient spelling for adapters.
    deliver_to_sheets = mark_sheet_delivered
    approve_final_draft = approve_draft
    store_draft = create_draft

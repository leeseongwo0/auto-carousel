"""Minimal, independent SQLite workflow for Newsbot V2."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from .ai.structured_copy import draft_from_mapping
from .collectors.base import SourceObservation
from .copywriting import validate_copy
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
        columns = {row["name"] for row in self._db.execute("PRAGMA table_info(v2_remote_effects)")}
        if "receipt_id" not in columns:
            self._db.execute("ALTER TABLE v2_remote_effects ADD COLUMN receipt_id TEXT NOT NULL DEFAULT ''")
        self._db.commit()

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

    def next_candidate_pending_notification(self) -> V2Candidate | None:
        """Select the oldest candidate whose notification needs safe recovery or dispatch."""
        row = self._db.execute(
            "SELECT candidate.id FROM v2_candidates candidate "
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=candidate.id AND effect.stage='candidate_notification' "
            "WHERE candidate.state=? "
            "ORDER BY candidate.created_at,candidate.id LIMIT 1",
            (V2State.PENDING_CANDIDATE.value,),
        ).fetchone()
        return None if row is None else self.get_candidate(str(row["id"]))

    def next_draft_approved_sheets_delivery(self) -> V2Draft | None:
        """Select the oldest approved draft, including one with a durable recovery receipt."""
        row = self._db.execute(
            "SELECT draft.id FROM v2_drafts draft "
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=draft.id AND effect.stage='sheets_delivery' "
            "WHERE draft.state=? "
            "ORDER BY draft.created_at,draft.id LIMIT 1",
            (V2State.DRAFT_APPROVED.value,),
        ).fetchone()
        return None if row is None else self.get_draft(str(row["id"]))

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

    def claim_remote_effect(self, entity_id: str, stage: str, detail: str) -> bool:
        """Atomically claim an absent or definitively failed remote operation."""
        now = self._now()
        with self._db:
            inserted = self._db.execute(
                "INSERT OR IGNORE INTO v2_remote_effects"
                "(entity_id,stage,attempts,status,detail,receipt_id,updated_at) "
                "VALUES(?,?,1,'pending',?,'',?)",
                (str(entity_id), str(stage), str(detail), now),
            )
            if inserted.rowcount == 1:
                return True
            retried = self._db.execute(
                "UPDATE v2_remote_effects SET attempts=attempts+1,status='pending',detail=?,"
                "receipt_id='',updated_at=? WHERE entity_id=? AND stage=? AND status='failed'",
                (str(detail), now, str(entity_id), str(stage)),
            )
            return retried.rowcount == 1

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
        return updated.rowcount == 1

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
            "LEFT JOIN v2_remote_effects effect "
            "ON effect.entity_id=draft.id AND effect.stage='draft_notification' "
            "WHERE draft.state=? "
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
            "LEFT JOIN v2_codex_requests request ON request.candidate_id=candidate.id "
            "WHERE candidate.state=? AND (request.digest IS NULL OR request.status IN ('prepared','retryable_failed')) "
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
        with self._db:
            request = self.get_codex_request(candidate_id)
            if request is None or request.digest != digest:
                raise V2WorkflowError("Codex request identity mismatch")
            if request.status == "pending":
                raise V2WorkflowError("Codex request has an interrupted pending attempt")
            if request.status not in {"prepared", "retryable_failed"}:
                raise V2WorkflowError(f"cannot launch Codex request in status {request.status}")
            candidate = self._candidate_row(str(candidate_id))
            if candidate["state"] != V2State.CANDIDATE_APPROVED.value or self.get_draft_for_candidate(
                str(candidate_id)
            ):
                raise V2WorkflowError("Codex launch state gate failed")
            count = int(
                self._db.execute(
                    "SELECT COUNT(*) AS count FROM v2_codex_attempts WHERE request_digest=?", (digest,)
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
            self._db.execute("UPDATE v2_codex_requests SET status='pending',updated_at=? WHERE digest=?", (now, digest))
        if inserted.lastrowid is None:
            raise V2WorkflowError("Codex attempt receipt was not created")
        return V2CodexAttempt(int(inserted.lastrowid), digest, number, "pending", None)

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

    prepare_generation_request = prepare_codex_request
    record_generation_attempt = begin_codex_attempt

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

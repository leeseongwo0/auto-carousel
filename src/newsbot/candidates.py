"""Candidate-only digest and two-stage approval state machine.

This module deliberately has no dependency on an AI provider.  Selecting a
candidate only writes a generation job; a worker is responsible for provider
invocation later.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, cast

from newsbot.approval.base import (
    ApprovalAction,
    ApprovalStage,
    CallbackBinding,
    hash_callback_token,
    issue_callback,
    matches_callback_token,
)
from newsbot.automation import StreamLease
from newsbot.copywriting import (
    BodyPage,
    Caption,
    CopyDraft,
    CopyValidationError,
    CoverPage,
    FactReference,
    FactualUnit,
    validate_copy,
)
from newsbot.exports import approval_outbox_intent
from newsbot.handoffs import SheetCategory, enqueue_sheet_handoff
from newsbot.storage import Storage, has_newer_material_source


@dataclass(frozen=True, slots=True)
class DigestButton:
    label: str
    token: str
    action: ApprovalAction


@dataclass(frozen=True, slots=True)
class CandidateDigest:
    id: int
    run_id: int
    revision: int
    candidates: tuple[dict[str, Any], ...]
    buttons: dict[int, tuple[DigestButton, ...]]


@dataclass(frozen=True, slots=True)
class ApprovalResult:
    status: str
    candidate_id: int | None = None
    selection_id: int | None = None
    generation_job_id: int | None = None


class CandidateApprovalService:
    """Persist candidate and draft decisions using the bundled SQLite schema."""

    def __init__(
        self,
        storage: Storage,
        *,
        chat_id: int,
        authorized_user_ids: set[int],
        now: Callable[[], datetime],
        sheet_target_binding_id: int | None = None,
    ) -> None:
        self.storage = storage
        self.chat_id = chat_id
        self.authorized_user_ids = frozenset(authorized_user_ids)
        self.now = now
        self.sheet_target_binding_id = sheet_target_binding_id

    def create_digest(
        self, run_id: int, *, actor_id: int, expires_in: timedelta = timedelta(hours=24)
    ) -> CandidateDigest:
        """Create a candidate-only digest.  It never reads or generates copy."""
        now = _utc(self.now())
        with self.storage.transaction() as connection:
            rows = list(
                connection.execute(
                    "SELECT c.id, c.rank, c.revision, ce.score, ce.rationale_json, "
                    "ce.source_post_version_id AS primary_source_id "
                    "FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
                    "WHERE ce.run_id=? AND c.status='pending_selection' ORDER BY c.rank ASC, c.id ASC",
                    (run_id,),
                )
            )
            for row in tuple(rows):
                candidate_id = int(row["id"])
                if self._has_newer_material_source(connection, self._source_ids(connection, candidate_id)):
                    connection.execute(
                        "UPDATE candidates SET status='superseded', revision=revision+1 "
                        "WHERE id=? AND status='pending_selection'",
                        (candidate_id,),
                    )
            rows = [
                row
                for row in rows
                if not self._has_newer_material_source(connection, self._source_ids(connection, int(row["id"])))
            ]
            bindings = {int(row["id"]): self._source_ids(connection, int(row["id"])) for row in rows}
            displays = {
                int(row["id"]): self._candidate_display(connection, int(row["primary_source_id"])) for row in rows
            }
            digest_material = [(int(row["id"]), int(row["revision"]), bindings[int(row["id"])]) for row in rows]
            key = "candidate-{}-{}".format(
                run_id,
                sha256(json.dumps(digest_material, separators=(",", ":")).encode()).hexdigest()[:24],
            )
            connection.execute(
                "INSERT OR IGNORE INTO digests(run_id, digest_key, status, title) VALUES (?, ?, 'active', ?)",
                (run_id, key, "Candidate selection"),
            )
            digest = connection.execute(
                "SELECT id FROM digests WHERE run_id=? AND digest_key=?",
                (run_id, key),
            ).fetchone()
            assert digest is not None
            digest_id = int(digest["id"])
        buttons: dict[int, tuple[DigestButton, ...]] = {}
        candidates: list[dict[str, Any]] = []
        for row in rows:
            candidate_id = int(row["id"])
            source_ids = bindings[candidate_id]
            candidates.append(
                {
                    "candidate_id": candidate_id,
                    "rank": row["rank"],
                    "score": row["score"],
                    "title": displays[candidate_id][0],
                    "source_url": displays[candidate_id][1],
                    "rationale": json.loads(row["rationale_json"]),
                    "warnings": _warnings(json.loads(row["rationale_json"])),
                    "source_version_ids": source_ids,
                    "revision": int(row["revision"]),
                }
            )
            buttons[candidate_id] = tuple(
                self._button(
                    digest_id,
                    candidate_id,
                    int(row["revision"]),
                    source_ids,
                    actor_id,
                    ApprovalStage.SELECTION,
                    action,
                    now,
                    expires_in,
                    digest_revision=1,
                )
                for action in (
                    ApprovalAction.MAKE,
                    ApprovalAction.DEFER_6H,
                    ApprovalAction.DEFER_24H,
                    ApprovalAction.DEFER_72H,
                    ApprovalAction.REJECT,
                    ApprovalAction.REFRESH,
                )
            )
        return CandidateDigest(digest_id, run_id, 1, tuple(candidates), buttons)

    def review_buttons(
        self,
        candidate_id: int,
        generation_id: int,
        *,
        actor_id: int,
        source_version_ids: tuple[int, ...],
        expires_in: timedelta = timedelta(hours=24),
    ) -> tuple[DigestButton, ...]:
        now = _utc(self.now())
        with self.storage.transaction() as connection:
            candidate = connection.execute(
                "SELECT status, revision FROM candidates WHERE id=?", (candidate_id,)
            ).fetchone()
            generation = connection.execute(
                "SELECT g.attempt, g.content_json, d.id AS digest_id, d.revision AS digest_revision, d.status AS digest_status "
                "FROM generations g "
                "JOIN generation_jobs j ON j.id=g.generation_job_id "
                "JOIN selections s ON s.id=j.selection_id "
                "JOIN digests d ON d.id=s.digest_id "
                "WHERE g.id=? AND g.status='current' AND s.candidate_id=?",
                (generation_id, candidate_id),
            ).fetchone()
            generation_sources = tuple(
                int(row["source_post_version_id"])
                for row in connection.execute(
                    "SELECT source_post_version_id FROM generation_sources WHERE generation_id=? ORDER BY source_post_version_id",
                    (generation_id,),
                )
            )
            if candidate is not None and self._has_newer_material_source(connection, generation_sources):
                raise ValueError("review source binding is stale")
            warning_digest = _warning_digest(self._warnings_for_candidate(connection, candidate_id))
        if (
            candidate is None
            or candidate["status"] != "pending_review"
            or generation is None
            or generation["digest_status"] != "selected"
            or generation_sources != tuple(sorted(source_version_ids))
        ):
            raise ValueError("review has no exact current generation binding")
        content_sha256 = sha256(str(generation["content_json"]).encode("utf-8")).hexdigest()
        return tuple(
            self._button(
                int(generation["digest_id"]),
                candidate_id,
                int(candidate["revision"]),
                generation_sources,
                actor_id,
                ApprovalStage.REVIEW,
                action,
                now,
                expires_in,
                generation_id,
                int(generation["attempt"]),
                content_sha256,
                warning_digest,
                int(generation["digest_revision"]),
            )
            for action in (
                ApprovalAction.APPROVE_HANDOFF,
                ApprovalAction.REGENERATE,
                ApprovalAction.REJECT,
            )
        )

    def apply(
        self,
        token: str,
        *,
        chat_id: int,
        user_id: int,
        automation_lease: StreamLease | None = None,
    ) -> ApprovalResult:
        """Apply one authorized callback. Stale/duplicate callbacks are harmless."""
        if chat_id != self.chat_id or user_id not in self.authorized_user_ids:
            return ApprovalResult("unauthorized")
        now = _utc(self.now())
        token_hash = hash_callback_token(token)
        row = self.storage.fetch_one("SELECT * FROM callback_tokens WHERE token=?", (token_hash,))
        if row is None:
            return ApprovalResult("stale")
        if not matches_callback_token(token, str(row["token"])):
            return ApprovalResult("stale")
        payload = json.loads(row["payload_json"])
        if int(payload["chat_id"]) != chat_id or int(payload["actor_id"]) != user_id:
            return ApprovalResult("unauthorized")
        if row["revoked_at"] is not None:
            return ApprovalResult("stale")
        if row["consumed_at"] is not None:
            return self._duplicate_result(int(payload["candidate_id"]))
        stage = ApprovalStage(payload["stage"])
        action = ApprovalAction(row["action"])
        candidate_id = int(payload["candidate_id"])
        with self.storage.transaction() as connection:
            fresh = connection.execute("SELECT * FROM callback_tokens WHERE token=?", (token_hash,)).fetchone()
            if fresh is None:
                return ApprovalResult("stale")
            if fresh["revoked_at"] is not None:
                return ApprovalResult("stale")
            if fresh["consumed_at"] is not None:
                return self._duplicate_result(candidate_id)
            cutover = connection.execute("SELECT id FROM automation_cutovers WHERE id=1").fetchone()
            if datetime.fromisoformat(str(fresh["expires_at"])) <= now and (
                cutover is None or fresh["notification_id"] is None
            ):
                return ApprovalResult("stale")
            if cutover is not None:
                notification = connection.execute(
                    "SELECT state FROM telegram_notification_outbox WHERE id=? AND cutover_id=1 "
                    "AND state IN ('sent','ambiguous','resolved_delivered')",
                    (fresh["notification_id"],),
                ).fetchone()
                if notification is None or automation_lease is None or automation_lease.stream != "approval_poll":
                    return ApprovalResult("stale")
                lease = connection.execute(
                    "SELECT 1 FROM automation_stream_leases WHERE stream='approval_poll' "
                    "AND owner_hash=? AND fence=? AND aware_epoch_us(expires_at)>aware_epoch_us(?)",
                    (
                        automation_lease.owner_hash,
                        automation_lease.fence,
                        now.isoformat(),
                    ),
                ).fetchone()
                if lease is None:
                    return ApprovalResult("stale")
            candidate = connection.execute("SELECT * FROM candidates WHERE id=?", (candidate_id,)).fetchone()
            if candidate is None:
                return ApprovalResult("stale")
            expected_status = "pending_selection" if stage is ApprovalStage.SELECTION else "pending_review"
            if candidate["status"] != expected_status:
                return ApprovalResult("stale")
            current_sources = self._source_ids(connection, candidate_id)
            if tuple(payload["source_version_ids"]) != current_sources:
                return ApprovalResult("stale")
            if self._has_newer_material_source(connection, current_sources):
                return ApprovalResult("stale")
            if int(payload["candidate_revision"]) != int(candidate["revision"]):
                return ApprovalResult("stale")
            digest_id = payload.get("digest_id")
            digest_revision = payload.get("digest_revision")
            if not isinstance(digest_id, int) or not isinstance(digest_revision, int):
                return ApprovalResult("stale")
            digest = connection.execute("SELECT status, revision FROM digests WHERE id=?", (digest_id,)).fetchone()
            expected_digest_status = "active" if stage is ApprovalStage.SELECTION else "selected"
            if (
                digest is None
                or digest["status"] != expected_digest_status
                or int(digest["revision"]) != digest_revision
            ):
                return ApprovalResult("stale")
            page_count: int | None = None
            generation: Any = None
            warnings: tuple[dict[str, Any], ...] = ()
            approval_category: str | None = None
            if stage is ApprovalStage.REVIEW:
                generation = connection.execute(
                    "SELECT g.id, g.attempt, g.content_json, j.requested_page_count FROM generations g "
                    "JOIN generation_jobs j ON j.id=g.generation_job_id "
                    "JOIN selections s ON s.id=j.selection_id "
                    "WHERE g.id=? AND g.status='current' AND s.candidate_id=? AND s.digest_id=?",
                    (payload["generation_id"], candidate_id, digest_id),
                ).fetchone()
                if generation is None:
                    return ApprovalResult("stale")
                if (
                    int(payload.get("generation_revision", 0)) != int(generation["attempt"])
                    or payload.get("content_sha256")
                    != sha256(str(generation["content_json"]).encode("utf-8")).hexdigest()
                ):
                    return ApprovalResult("stale")
                generation_sources = tuple(
                    int(row["source_post_version_id"])
                    for row in connection.execute(
                        "SELECT source_post_version_id FROM generation_sources "
                        "WHERE generation_id=? ORDER BY source_post_version_id",
                        (int(generation["id"]),),
                    )
                )
                if generation_sources != current_sources:
                    return ApprovalResult("stale")
                if payload.get("warning_digest") != _warning_digest(
                    self._warnings_for_candidate(connection, candidate_id)
                ):
                    return ApprovalResult("stale")
                if action is ApprovalAction.APPROVE_HANDOFF:
                    try:
                        self._validate_review_content(
                            connection,
                            str(generation["content_json"]),
                            current_sources,
                            generation["requested_page_count"],
                        )
                    except (CopyValidationError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                        return ApprovalResult("stale")
                    warnings = self._warnings_for_candidate(connection, candidate_id)
                    if payload.get("warning_digest") != _warning_digest(warnings):
                        return ApprovalResult("stale")
                    approval_category = json.loads(str(generation["content_json"]))["category"]
                if action in (ApprovalAction.PAGE_INCREMENT, ApprovalAction.PAGE_DECREMENT):
                    page_count = _page_count(generation["content_json"])
                    if page_count is None:
                        return ApprovalResult("stale")
                    page_count += 1 if action is ApprovalAction.PAGE_INCREMENT else -1
                    if not 1 <= page_count <= 8:
                        return ApprovalResult("stale")
            connection.execute("UPDATE callback_tokens SET consumed_at=? WHERE id=?", (now.isoformat(), fresh["id"]))
            run_id = self._run_id(connection, candidate_id)
            event_key = token_hash
            existing = connection.execute(
                "SELECT * FROM decision_events WHERE run_id=? AND event_key=?", (run_id, event_key)
            ).fetchone()
            if existing is not None:
                return ApprovalResult("duplicate", candidate_id)
            decision_payload = dict(payload)
            if approval_category is not None:
                decision_payload["category"] = approval_category
            connection.execute(
                "INSERT INTO decision_events(run_id, digest_id, selection_id, event_key, decision, actor, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    payload.get("digest_id"),
                    payload.get("selection_id"),
                    event_key,
                    action.value,
                    str(user_id),
                    json.dumps(decision_payload, sort_keys=True),
                ),
            )
            event_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            if action is not ApprovalAction.REFRESH:
                self._revoke_candidate_callbacks(connection, candidate_id, now)
            if action is ApprovalAction.REFRESH:
                return ApprovalResult("refreshed", candidate_id)
            if action in (ApprovalAction.DEFER_6H, ApprovalAction.DEFER_24H, ApprovalAction.DEFER_72H):
                deferred_until = now + _defer_interval(action)
                if cutover is not None:
                    if automation_lease is None or fresh["notification_id"] is None:
                        return ApprovalResult("stale")
                    connection.execute(
                        "INSERT INTO automation_defer_authority(notification_id,decision_event_id,candidate_id,stage,due_at,cutover_id) "
                        "VALUES(?,?,?,?,?,1)",
                        (
                            int(fresh["notification_id"]),
                            event_id,
                            candidate_id,
                            stage.value,
                            deferred_until.isoformat(),
                        ),
                    )
                connection.execute(
                    "UPDATE candidates SET status='deferred', deferred_stage=?, deferred_until=? WHERE id=?",
                    (stage.value, deferred_until.isoformat(), candidate_id),
                )
                return ApprovalResult("deferred", candidate_id)
            if action is ApprovalAction.REJECT:
                connection.execute("UPDATE candidates SET status='rejected' WHERE id=?", (candidate_id,))
                return ApprovalResult("rejected", candidate_id)
            if stage is ApprovalStage.SELECTION and action is ApprovalAction.MAKE:
                digest_id = int(payload["digest_id"])
                chosen = connection.execute(
                    "SELECT candidate_id FROM selections WHERE digest_id=?",
                    (digest_id,),
                ).fetchone()
                if chosen is not None and int(chosen["candidate_id"]) != candidate_id:
                    return ApprovalResult("stale")
                selection = connection.execute(
                    "SELECT id FROM selections WHERE digest_id=? AND candidate_id=?", (digest_id, candidate_id)
                ).fetchone()
                if selection is None:
                    connection.execute(
                        "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)",
                        (digest_id, candidate_id),
                    )
                    selection_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                else:
                    selection_id = int(selection["id"])
                job = connection.execute(
                    "SELECT id FROM generation_jobs WHERE selection_id=? AND job_kind='initial'", (selection_id,)
                ).fetchone()
                if job is None:
                    connection.execute(
                        "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) "
                        "VALUES (?, 'initial', 'queued', NULL)",
                        (selection_id,),
                    )
                    job_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                else:
                    job_id = int(job["id"])
                self._bind_job_sources(connection, job_id, current_sources)
                if cutover is not None:
                    connection.execute(
                        "INSERT OR IGNORE INTO automation_generation_authority("
                        "generation_job_id,selection_id,decision_event_id,cutover_id"
                        ") VALUES(?,?,?,1)",
                        (job_id, selection_id, event_id),
                    )
                connection.execute(
                    "UPDATE candidates SET status='selected_generation_pending' WHERE id=?", (candidate_id,)
                )
                connection.execute("UPDATE digests SET status='selected' WHERE id=? AND status='active'", (digest_id,))
                return ApprovalResult("queued", candidate_id, selection_id, job_id)
            if stage is ApprovalStage.REVIEW and action in (
                ApprovalAction.REGENERATE,
                ApprovalAction.PAGE_INCREMENT,
                ApprovalAction.PAGE_DECREMENT,
            ):
                selection_id = self._selection_id(connection, candidate_id)
                connection.execute(
                    "UPDATE generations SET status='superseded' WHERE status='current' AND generation_job_id IN "
                    "(SELECT id FROM generation_jobs WHERE selection_id=?)",
                    (selection_id,),
                )
                connection.execute(
                    "UPDATE generation_jobs SET status='superseded', finished_at=? WHERE selection_id=? "
                    "AND status IN ('queued', 'running', 'failed_recoverable')",
                    (now.isoformat(), selection_id),
                )
                kind = (
                    f"regenerate:{generation['id']}"
                    if action is ApprovalAction.REGENERATE
                    else f"page:{page_count}:{generation['id']}"
                )
                connection.execute(
                    "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) "
                    "VALUES (?, ?, 'queued', ?)",
                    (
                        selection_id,
                        kind,
                        page_count if page_count is not None else _page_count(generation["content_json"]),
                    ),
                )
                job_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                self._bind_job_sources(connection, job_id, current_sources)
                if cutover is not None:
                    connection.execute(
                        "INSERT INTO automation_generation_authority("
                        "generation_job_id,selection_id,decision_event_id,cutover_id"
                        ") VALUES(?,?,?,1)",
                        (job_id, selection_id, event_id),
                    )
                connection.execute(
                    "UPDATE candidates SET status='selected_generation_pending' WHERE id=?",
                    (candidate_id,),
                )
                return ApprovalResult("queued", candidate_id, selection_id, job_id)
            if stage is ApprovalStage.REVIEW and action is ApprovalAction.APPROVE_HANDOFF:
                assert approval_category in ("AI", "Blockchain")
                export_id, canonical_payload, _ = approval_outbox_intent(
                    candidate_id=candidate_id,
                    generation_id=int(generation["id"]),
                    approval_event_id=event_id,
                    source_version_ids=current_sources,
                    content_json=str(generation["content_json"]),
                    warnings=warnings,
                    source_versions=self._export_source_versions(connection, current_sources),
                    generation_revision=int(generation["attempt"]),
                    approval={
                        "action": action.value,
                        "actor": str(user_id),
                        "candidate_revision": int(candidate["revision"]),
                        "digest_revision": int(digest_revision),
                        "warning_digest": _warning_digest(warnings),
                        "category": approval_category,
                    },
                )
                if self.sheet_target_binding_id is None:
                    raise RuntimeError("pre-created Sheets target binding is required")
                enqueue_sheet_handoff(
                    connection,
                    generation_id=int(generation["id"]),
                    approval_event_id=event_id,
                    target_binding_id=self.sheet_target_binding_id,
                    export_id=export_id,
                    category=cast(SheetCategory, approval_category),
                    canonical_bytes=canonical_payload,
                    approved_at=now.isoformat(),
                    now=now.isoformat(),
                )
                connection.execute("UPDATE candidates SET status='approved' WHERE id=?", (candidate_id,))
                connection.execute("UPDATE digests SET status='approved' WHERE id=?", (digest_id,))
                return ApprovalResult("approved", candidate_id)
            return ApprovalResult("stale", candidate_id)

    def _duplicate_result(self, candidate_id: int) -> ApprovalResult:
        selection = self.storage.fetch_one(
            "SELECT id FROM selections WHERE candidate_id=? ORDER BY id DESC LIMIT 1",
            (candidate_id,),
        )
        if selection is None:
            return ApprovalResult("duplicate", candidate_id)
        job = self.storage.fetch_one(
            "SELECT id FROM generation_jobs WHERE selection_id=? AND job_kind='initial'",
            (int(selection["id"]),),
        )
        return ApprovalResult(
            "duplicate",
            candidate_id,
            int(selection["id"]),
            None if job is None else int(job["id"]),
        )

    def _button(
        self,
        digest_id: int | None,
        candidate_id: int,
        candidate_revision: int,
        source_ids: tuple[int, ...],
        actor_id: int,
        stage: ApprovalStage,
        action: ApprovalAction,
        now: datetime,
        expires_in: timedelta,
        generation_id: int | None = None,
        generation_revision: int | None = None,
        content_sha256: str | None = None,
        warning_digest: str | None = None,
        digest_revision: int | None = None,
    ) -> DigestButton:
        issued = issue_callback(
            stage=stage,
            action=action,
            binding=CallbackBinding(
                chat_id=self.chat_id,
                actor_id=actor_id,
                candidate_id=candidate_id,
                candidate_revision=candidate_revision,
                source_version_ids=tuple(sorted(source_ids)),
                digest_revision=digest_revision if digest_id else None,
                generation_id=generation_id,
                generation_revision=generation_revision,
            ),
            created_at=now,
            expires_at=now + expires_in,
        )
        payload = {
            "stage": stage.value,
            "chat_id": self.chat_id,
            "actor_id": actor_id,
            "candidate_id": candidate_id,
            "candidate_revision": candidate_revision,
            "source_version_ids": tuple(sorted(source_ids)),
            "digest_id": digest_id,
            "digest_revision": digest_revision if digest_id else None,
            "generation_id": generation_id,
            "generation_revision": generation_revision,
            "content_sha256": content_sha256,
            "warning_digest": warning_digest,
        }
        with self.storage.transaction() as connection:
            connection.execute(
                "INSERT INTO callback_tokens(token, action, payload_json, expires_at) VALUES (?, ?, ?, ?)",
                (
                    issued.record.token_hash,
                    action.value,
                    json.dumps(payload, sort_keys=True),
                    issued.record.expires_at.isoformat(),
                ),
            )
        return DigestButton(_label(action), issued.token, action)

    @staticmethod
    def _revoke_candidate_callbacks(connection: Any, candidate_id: int, now: datetime) -> None:
        connection.execute(
            "UPDATE callback_tokens SET revoked_at=? WHERE consumed_at IS NULL AND revoked_at IS NULL "
            "AND CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER)=?",
            (now.isoformat(), candidate_id),
        )

    def resume_due(self, now: datetime | None = None) -> tuple[int, ...]:
        """Restore due deferred candidates to the exact stage that was deferred when their sources remain current."""
        due_at = _utc(self.now() if now is None else now)
        resumed: list[int] = []
        with self.storage.transaction() as connection:
            if connection.execute("SELECT 1 FROM automation_cutovers WHERE id=1").fetchone() is not None:
                return ()
            rows = tuple(
                connection.execute(
                    "SELECT id, deferred_stage FROM candidates WHERE status='deferred' AND deferred_until <= ? "
                    "AND deferred_stage IN ('selection', 'review') ORDER BY id",
                    (due_at.isoformat(),),
                )
            )
            for row in rows:
                candidate_id = int(row["id"])
                if self._has_newer_material_source(connection, self._source_ids(connection, candidate_id)):
                    connection.execute(
                        "UPDATE candidates SET status='superseded', revision=revision+1, deferred_stage=NULL, deferred_until=NULL "
                        "WHERE id=? AND status='deferred'",
                        (candidate_id,),
                    )
                    connection.execute(
                        "UPDATE digests SET status='superseded', revision=revision+1 WHERE status IN ('active', 'selected') "
                        "AND run_id=?",
                        (self._run_id(connection, candidate_id),),
                    )
                    continue
                status = (
                    "pending_selection" if row["deferred_stage"] == ApprovalStage.SELECTION.value else "pending_review"
                )
                updated = connection.execute(
                    "UPDATE candidates SET status=?, deferred_stage=NULL, deferred_until=NULL "
                    "WHERE id=? AND status='deferred' AND deferred_until <= ?",
                    (status, candidate_id, due_at.isoformat()),
                )
                if updated.rowcount == 1:
                    connection.execute(
                        "UPDATE callback_tokens SET revoked_at=? WHERE revoked_at IS NULL "
                        "AND CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER)=?",
                        (due_at.isoformat(), candidate_id),
                    )
                    resumed.append(candidate_id)
        return tuple(resumed)

    def warnings_for_candidate(self, candidate_id: int) -> tuple[dict[str, str], ...]:
        row = self.storage.fetch_one(
            "SELECT ce.rationale_json FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
            (candidate_id,),
        )
        return () if row is None else _warnings(json.loads(row["rationale_json"]))

    @staticmethod
    def _warnings_for_candidate(connection: Any, candidate_id: int) -> tuple[dict[str, str], ...]:
        row = connection.execute(
            "SELECT ce.rationale_json FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
            (candidate_id,),
        ).fetchone()
        return () if row is None else _warnings(json.loads(row["rationale_json"]))

    @staticmethod
    def _validate_review_content(
        connection: Any, content_json: str, source_ids: tuple[int, ...], expected_page_count: Any
    ) -> None:
        payload = json.loads(content_json)
        if not isinstance(payload, dict):
            raise CopyValidationError("draft must be an object")
        if payload.get("draft") is not True or payload.get("source_reported") is not True:
            raise CopyValidationError("draft/source_reported markers are required")
        draft = CopyDraft(
            CoverPage(
                payload["cover"]["title"],
                payload["cover"]["subtitle"],
                _factual_units(payload["cover"]["factual_units"]),
            ),
            tuple(
                BodyPage(body["subtitle"], body["body"], _factual_units(body["factual_units"]))
                for body in payload["bodies"]
            ),
            Caption(**payload["caption"]),
            category=payload["category"],
            draft=payload["draft"],
            source_reported=payload["source_reported"],
        )
        references = tuple(
            reference
            for unit in (*draft.cover.factual_units, *(unit for body in draft.bodies for unit in body.factual_units))
            for reference in unit.references
        )
        allowed_sources = set(source_ids)
        if any(reference.source_version_id not in allowed_sources for reference in references):
            raise CopyValidationError("claim source does not match the selected source binding")
        validate_copy(
            draft,
            allowed_claim_sources={reference.claim_id: reference.source_version_id for reference in references},
            expected_page_count=None if expected_page_count is None else int(expected_page_count),
        )

    @staticmethod
    def _run_id(connection: Any, candidate_id: int) -> int:
        return int(
            connection.execute(
                "SELECT ce.run_id FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
                (candidate_id,),
            ).fetchone()[0]
        )

    @staticmethod
    def _source_ids(connection: Any, candidate_id: int) -> tuple[int, ...]:
        rows = tuple(
            int(row["source_post_version_id"])
            for row in connection.execute(
                "SELECT source_post_version_id FROM candidate_sources "
                "WHERE candidate_id=? ORDER BY source_post_version_id",
                (candidate_id,),
            )
        )
        if rows:
            return rows
        row = connection.execute(
            "SELECT ce.source_post_version_id FROM candidates c "
            "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
            (candidate_id,),
        ).fetchone()
        return () if row is None else (int(row["source_post_version_id"]),)

    @staticmethod
    def _candidate_display(connection: Any, source_version_id: int) -> tuple[str, str]:
        row = connection.execute(
            "SELECT version.body, version.urls_json, version.channel_handle, "
            "post.external_post_id, post.source_url "
            "FROM source_post_versions version "
            "JOIN source_posts post ON post.id=version.source_post_id "
            "WHERE version.id=?",
            (source_version_id,),
        ).fetchone()
        if row is None:
            return ("제목 없음", "링크 없음")

        title = ""
        try:
            urls = json.loads(str(row["urls_json"]))
        except json.JSONDecodeError:
            urls = []
        if isinstance(urls, list):
            title = next(
                (
                    str(item["title"]).strip()
                    for item in urls
                    if isinstance(item, dict) and isinstance(item.get("title"), str) and str(item["title"]).strip()
                ),
                "",
            )
        if not title:
            body = str(row["body"]).strip()
            title = " ".join((body.splitlines()[0] if body else "제목 없음").split())
        if len(title) > 100:
            title = title[:99].rstrip() + "…"

        source_url = str(row["source_url"] or "").strip()
        if not source_url:
            handle = str(row["channel_handle"] or "").strip().lstrip("@")
            external_post_id = str(row["external_post_id"] or "").strip()
            if handle and external_post_id:
                source_url = f"https://t.me/{handle}/{external_post_id}"
        return (title or "제목 없음", source_url or "링크 없음")

    @staticmethod
    def _export_source_versions(connection: Any, source_ids: tuple[int, ...]) -> tuple[dict[str, Any], ...]:
        if not source_ids:
            raise ValueError("approval has no source versions")
        marks = ",".join("?" for _ in source_ids)
        rows = connection.execute(
            "SELECT version.id AS source_version_id, post.channel_id, post.external_post_id, post.source_url, "
            "version.version_key, version.body, version.media_json, version.kind, version.sponsored, "
            "version.urls_json, version.conflicts_json, observation.observation_key, "
            "observation.observed_at AS captured_at, observation.engagement_json "
            "FROM source_post_versions version JOIN source_posts post ON post.id=version.source_post_id "
            "JOIN source_post_observations observation ON observation.id=("
            "SELECT current.id FROM source_post_observations current "
            "WHERE current.source_post_version_id=version.id ORDER BY current.id DESC LIMIT 1) "
            f"WHERE version.id IN ({marks}) ORDER BY version.id",
            source_ids,
        )
        values = tuple(
            {
                "source_version_id": int(row["source_version_id"]),
                "channel_id": str(row["channel_id"]),
                "external_post_id": str(row["external_post_id"]),
                "source_url": row["source_url"],
                "version_key": str(row["version_key"]),
                "body": str(row["body"]),
                "media": json.loads(str(row["media_json"])),
                "kind": str(row["kind"]),
                "sponsored": bool(row["sponsored"]),
                "urls": json.loads(str(row["urls_json"])),
                "conflicts": json.loads(str(row["conflicts_json"])),
                "observation_key": str(row["observation_key"]),
                "captured_at": str(row["captured_at"]),
                "engagement": json.loads(str(row["engagement_json"])),
                "uncertainty": ["source conflicts require corroboration"]
                if json.loads(str(row["conflicts_json"]))
                else [],
            }
            for row in rows
        )
        if len(values) != len(source_ids):
            raise ValueError("approval source version binding is incomplete")
        return values

    @staticmethod
    def _has_newer_material_source(connection: Any, source_ids: tuple[int, ...]) -> bool:
        return has_newer_material_source(connection, source_ids)

    @staticmethod
    def _bind_job_sources(connection: Any, job_id: int, source_ids: tuple[int, ...]) -> None:
        connection.executemany(
            "INSERT OR IGNORE INTO generation_sources(generation_job_id, generation_id, source_post_version_id) "
            "VALUES (?, NULL, ?)",
            ((job_id, source_id) for source_id in source_ids),
        )

    @staticmethod
    def _selection_id(connection: Any, candidate_id: int) -> int:
        row = connection.execute(
            "SELECT id FROM selections WHERE candidate_id=? ORDER BY id DESC LIMIT 1", (candidate_id,)
        ).fetchone()
        if row is None:
            raise ValueError("review has no selection")
        return int(row["id"])


def _factual_units(values: Any) -> tuple[FactualUnit, ...]:
    if not isinstance(values, list):
        raise CopyValidationError("factual_units must be a list")
    return tuple(
        FactualUnit(
            unit["text"],
            tuple(
                FactReference(reference["claim_id"], reference["source_version_id"]) for reference in unit["references"]
            ),
        )
        for unit in values
    )


def _warnings(rationale: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(rationale, dict):
        return ()
    production_rationale = rationale.get("rationale")
    if not isinstance(production_rationale, dict):
        return ()
    value = production_rationale.get("warnings")
    if not isinstance(value, list):
        return ()
    warnings: list[dict[str, Any]] = []
    for warning in value:
        if isinstance(warning, dict) and isinstance(warning.get("kind"), str):
            detail = warning.get("detail")
            if isinstance(detail, str) and detail.strip():
                warnings.append(dict(warning, detail=detail.strip()))
    return tuple(warnings)


def _warning_digest(warnings: tuple[dict[str, Any], ...]) -> str:
    return sha256(json.dumps(warnings, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _defer_interval(action: ApprovalAction) -> timedelta:
    return {
        ApprovalAction.DEFER_6H: timedelta(hours=6),
        ApprovalAction.DEFER_24H: timedelta(hours=24),
        ApprovalAction.DEFER_72H: timedelta(hours=72),
    }[action]


def _page_count(content_json: str) -> int | None:
    try:
        payload = json.loads(content_json)
    except (TypeError, json.JSONDecodeError):
        return None
    pages = payload.get("pages")
    if isinstance(pages, list):
        count = len(pages)
    else:
        cover = payload.get("cover")
        bodies = payload.get("bodies")
        count = 1 + len(bodies) if isinstance(cover, dict) and isinstance(bodies, list) else 0
    return count if 1 <= count <= 8 else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("clock must return timezone-aware time")
    return value.astimezone(UTC)


def _label(action: ApprovalAction) -> str:
    return {
        ApprovalAction.MAKE: "[제작]",
        ApprovalAction.DEFER_6H: "6시간 미루기",
        ApprovalAction.DEFER_24H: "24시간 미루기",
        ApprovalAction.DEFER_72H: "72시간 미루기",
        ApprovalAction.REJECT: "거절",
        ApprovalAction.REFRESH: "새로고침",
        ApprovalAction.APPROVE_HANDOFF: "승인",
        ApprovalAction.REGENERATE: "재생성",
        ApprovalAction.PAGE_INCREMENT: "페이지 +",
        ApprovalAction.PAGE_DECREMENT: "페이지 -",
    }[action]

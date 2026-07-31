"""Durable, fenced authority for Sheets bootstrap and delivery work."""

from __future__ import annotations

import secrets
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Literal, cast

from .sheets.base import SafeCode
from .storage import Storage, aware_epoch_us

SheetCategory = Literal["AI", "Blockchain"]
OperationOutcome = Literal[
    "applied",
    "reused",
    "rejected_retryable",
    "rejected_blocked",
    "abandoned_pre_marker",
    "duplicate_metadata",
    "conflicting_metadata",
    "schema_conflict",
    "local_corrupt",
    "operator_unresolved",
]
_CORRECTABLE_BLOCKERS = frozenset(
    {
        "template_drift",
        "metadata_conflict",
        "not_found",
        "invalid_request",
        "unauthenticated",
        "permission_denied",
        "schema_conflict",
    }
)


@dataclass(frozen=True, slots=True)
class SheetHandoff:
    id: int
    generation_id: int
    approval_event_id: int
    export_id: str
    category: SheetCategory
    canonical_bytes: bytes


@dataclass(frozen=True, slots=True)
class SheetLease:
    operation_id: int
    fence_version: int
    event_id: int
    lease_id: int
    owner_token: str
    lease_mode: Literal["mutate", "probe"]


def _hex(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def enqueue_sheet_handoff(
    connection: sqlite3.Connection,
    *,
    generation_id: int,
    approval_event_id: int,
    target_binding_id: int,
    canonical_bytes: bytes,
    approved_at: str,
    category: SheetCategory,
    export_id: str | None = None,
    marker_value: str | None = None,
    now: str | None = None,
) -> SheetHandoff:
    """Insert the one immutable approval-authorized handoff, idempotently."""
    if not canonical_bytes or not approved_at or category not in ("AI", "Blockchain"):
        raise ValueError("complete immutable handoff authority is required")
    digest = sha256(canonical_bytes).hexdigest()
    export_id = export_id or f"exp_{digest[:32]}"
    marker_value = marker_value or f"v1:{export_id}:{digest}"
    row = connection.execute(
        "SELECT h.*,b.target_binding_id FROM sheet_handoffs h "
        "LEFT JOIN sheet_handoff_bindings b ON b.handoff_id=h.id "
        "WHERE h.approval_event_id=?",
        (approval_event_id,),
    ).fetchone()
    if row is not None:
        if (
            row["generation_id"] == generation_id
            and row["export_id"] == export_id
            and row["canonical_sha256"] == digest
            and row["category"] == category
            and row["target_binding_id"] == target_binding_id
        ):
            return _handoff(row)
        raise ValueError("approval already has a different immutable sheet handoff")
    try:
        cursor = connection.execute(
            "INSERT INTO sheet_handoffs(generation_id,approval_event_id,target_binding_id,export_id,canonical_bytes,canonical_sha256,approved_at,category,initial_upload_status,marker_value,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?)",
            (
                generation_id,
                approval_event_id,
                target_binding_id,
                export_id,
                canonical_bytes,
                digest,
                approved_at,
                category,
                "X",
                marker_value,
                now or approved_at,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise ValueError("canonical handoff conflicts with existing authority") from exc
    created = connection.execute("SELECT * FROM sheet_handoffs WHERE id=?", (cursor.lastrowid,)).fetchone()
    assert created is not None
    connection.execute(
        "INSERT INTO sheet_handoff_bindings(handoff_id,target_binding_id,bound_at) VALUES(?,?,?)",
        (int(created["id"]), target_binding_id, now or approved_at),
    )
    return _handoff(created)


class SheetHandoffService:
    """All remote state changes are fenced by a retained, token-hashed lease."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def ensure_binding(
        self,
        *,
        binding_key: str,
        spreadsheet_id: str,
        sheet_id: int,
        now: str,
        oracle_fingerprint: str,
    ) -> int:
        """Create the immutable workplace target; configuration mismatch is rejected."""
        if binding_key != "workplace" or sheet_id != 0:
            raise ValueError("only workplace sheet 0 is supported")
        target_hash = sha256(spreadsheet_id.encode()).hexdigest()
        oracle = oracle_fingerprint
        if not _hex(oracle):
            raise ValueError("oracle fingerprint must be SHA-256")
        with self.storage.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM sheet_target_bindings "
                "WHERE schema_version='workplace-template-v1' AND sheet_id=0 "
                "AND sheet_title='workplace'"
            ).fetchone()
            if row is None:
                cursor = connection.execute(
                    "INSERT INTO sheet_target_bindings("
                    "target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint,created_at"
                    ") VALUES(?,'workplace-template-v1',0,'workplace',?,?)",
                    (target_hash, oracle, now),
                )
                assert cursor.lastrowid is not None
                return int(cursor.lastrowid)
            if row["target_ref_sha256"] != target_hash or row["oracle_fingerprint"] != oracle:
                raise ValueError("sheet target binding is immutable")
            return int(row["id"])

    def ensure_bootstrap(
        self,
        *,
        target_binding_id: int,
        marker_value: str,
        controls_fingerprint: str,
    ) -> str:
        """Create or validate the immutable bootstrap subject."""
        if not marker_value or not _hex(controls_fingerprint):
            raise ValueError("bootstrap identity is invalid")
        with self.storage.transaction() as connection:
            row = connection.execute(
                "SELECT marker_value, controls_fingerprint, status FROM sheet_bootstraps WHERE target_binding_id=?",
                (target_binding_id,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO sheet_bootstraps("
                    "target_binding_id,marker_value,controls_fingerprint,status"
                    ") VALUES(?,?,?,'uninitialized')",
                    (target_binding_id, marker_value, controls_fingerprint),
                )
                return "uninitialized"
            if row["marker_value"] != marker_value or row["controls_fingerprint"] != controls_fingerprint:
                raise ValueError("bootstrap identity is immutable")
            return str(row["status"])

    def retry_blocked(self, operation_id: int, *, now: str) -> bool:
        """Record an audited operator correction and reopen a safe not-applied subject."""
        if aware_epoch_us(now) is None:
            return False
        with self.storage.transaction() as connection:
            row = connection.execute(
                "SELECT o.operation_kind,o.handoff_id,o.target_binding_id,"
                "o.last_fence_version,o.status,o.outcome,o.certainty,"
                "CASE o.operation_kind WHEN 'delivery' THEN h.status ELSE b.status END "
                "AS subject_status,"
                "CASE o.operation_kind WHEN 'delivery' THEN h.safe_error_code "
                "ELSE b.safe_error_code END AS safe_error_code "
                "FROM sheet_remote_operations o "
                "LEFT JOIN sheet_handoffs h ON h.id=o.handoff_id "
                "LEFT JOIN sheet_bootstraps b ON b.target_binding_id=o.target_binding_id "
                "WHERE o.id=? AND o.finished_at IS NOT NULL "
                "AND (o.status='rejected_blocked' OR "
                "(o.status='blocked' AND o.outcome='schema_conflict' "
                "AND o.certainty='settled_not_applied')) "
                "AND NOT EXISTS(SELECT 1 FROM sheet_remote_operations open "
                "WHERE open.target_binding_id=o.target_binding_id "
                "AND open.finished_at IS NULL)",
                (operation_id,),
            ).fetchone()
            if row is None or row["subject_status"] != "blocked" or row["safe_error_code"] not in _CORRECTABLE_BLOCKERS:
                return False
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'operator_corrected',?,?)",
                (
                    operation_id,
                    row["last_fence_version"],
                    row["safe_error_code"],
                    now,
                ),
            )
            if row["operation_kind"] == "delivery":
                changed = connection.execute(
                    "UPDATE sheet_handoffs SET status='retryable',retry_at=? WHERE id=? AND status='blocked'",
                    (now, row["handoff_id"]),
                ).rowcount
            else:
                changed = connection.execute(
                    "UPDATE sheet_bootstraps SET status='retryable',retry_at=? "
                    "WHERE target_binding_id=? AND status='blocked'",
                    (now, row["target_binding_id"]),
                ).rowcount
            return changed == 1

    def acquire_initial(
        self,
        handoff_id: int | None,
        *,
        operation_kind: Literal["bootstrap", "delivery"],
        now: str,
        expires_at: str,
        target_binding_id: int | None = None,
    ) -> SheetLease | None:
        with self.storage.transaction() as connection:
            if operation_kind == "delivery":
                if handoff_id is None:
                    raise ValueError("delivery needs a handoff")
                binding = connection.execute(
                    "SELECT target_binding_id FROM sheet_handoff_bindings WHERE handoff_id=?",
                    (handoff_id,),
                ).fetchone()
                if binding is None:
                    return None
                bound_target_id = int(binding["target_binding_id"])
                if target_binding_id is not None and target_binding_id != bound_target_id:
                    raise ValueError("handoff is bound to a different Sheets target")
                target_binding_id = bound_target_id
            elif target_binding_id is None:
                raise ValueError("bootstrap needs a target binding")
            self._recover_expired_pre_marker(connection, target_binding_id, now)
            if operation_kind == "delivery":
                assert handoff_id is not None
                subject = connection.execute(
                    "SELECT 1 FROM sheet_handoffs WHERE id=? AND "
                    "(status='pending' OR (status='retryable' AND "
                    "aware_epoch_us(retry_at)<=aware_epoch_us(?)))",
                    (handoff_id, now),
                ).fetchone()
            else:
                subject = connection.execute(
                    "SELECT 1 FROM sheet_bootstraps WHERE target_binding_id=? AND "
                    "(status='uninitialized' OR (status='retryable' AND "
                    "aware_epoch_us(retry_at)<=aware_epoch_us(?)))",
                    (target_binding_id, now),
                ).fetchone()
            if subject is None:
                return None
            if connection.execute(
                "SELECT 1 FROM sheet_remote_operations WHERE target_binding_id=? AND finished_at IS NULL",
                (target_binding_id,),
            ).fetchone():
                return None
            lease = self._new_operation(
                connection,
                target_binding_id,
                operation_kind,
                handoff_id if operation_kind == "delivery" else None,
                now,
                expires_at,
            )
            self.storage._authorize_lease(lease.owner_token)
            if operation_kind == "delivery":
                assert handoff_id is not None
                if (
                    connection.execute(
                        "UPDATE sheet_handoffs SET status='delivering',retry_at=NULL,"
                        "safe_error_code=NULL,delivered_at=NULL WHERE id=? AND "
                        "(status='pending' OR (status='retryable' AND "
                        "aware_epoch_us(retry_at)<=aware_epoch_us(?)))",
                        (handoff_id, now),
                    ).rowcount
                    != 1
                ):
                    raise RuntimeError("sheet handoff changed during lease acquisition")
            elif (
                connection.execute(
                    "UPDATE sheet_bootstraps SET status='configuring',retry_at=NULL,"
                    "safe_error_code=NULL WHERE target_binding_id=? AND "
                    "(status='uninitialized' OR (status='retryable' AND "
                    "aware_epoch_us(retry_at)<=aware_epoch_us(?)))",
                    (target_binding_id, now),
                ).rowcount
                != 1
            ):
                raise RuntimeError("sheet bootstrap changed during lease acquisition")
            return lease

    def record_preflight(
        self,
        lease: SheetLease,
        *,
        outcome: Literal["exact", "absent", "conflict"],
        now: str,
    ) -> bool:
        if lease.lease_mode != "mutate":
            return False
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            operation = connection.execute(
                "SELECT 1 FROM sheet_remote_operations o "
                "JOIN sheet_operation_leases l ON l.id=? AND l.operation_id=o.id "
                "AND l.fence_version=o.last_fence_version AND l.status='active' "
                "WHERE o.id=? AND o.last_fence_version=? AND o.status='acquired' "
                "AND o.dispatch_at IS NULL AND o.finished_at IS NULL "
                "AND aware_epoch_us(l.acquired_at)<=aware_epoch_us(?) "
                "AND aware_epoch_us(?)<aware_epoch_us(l.expires_at) "
                "AND NOT EXISTS(SELECT 1 FROM sheet_operation_events e "
                "WHERE e.operation_id=o.id AND e.fence_version=o.last_fence_version "
                "AND e.event_kind IN ('preflight_exact','preflight_absent',"
                "'preflight_conflict'))",
                (lease.lease_id, lease.operation_id, lease.fence_version, now, now),
            ).fetchone()
            if operation is None:
                return False
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,?,?,?)",
                (
                    lease.operation_id,
                    lease.fence_version,
                    f"preflight_{outcome}",
                    None,
                    now,
                ),
            )
            return True

    def mark_possibly_sent(
        self,
        lease: SheetLease,
        *,
        request_sha256: str,
        oracle_fingerprint: str,
        controls_fingerprint: str,
        credential_refreshed_at: str,
        credential_expires_at: str,
        credential_scope_ok: bool,
        now: str,
    ) -> bool:
        refreshed_epoch = aware_epoch_us(credential_refreshed_at)
        expires_epoch = aware_epoch_us(credential_expires_at)
        now_epoch = aware_epoch_us(now)
        if (
            lease.lease_mode != "mutate"
            or not _hex(request_sha256)
            or not _hex(oracle_fingerprint)
            or not _hex(controls_fingerprint)
            or not credential_scope_ok
            or refreshed_epoch is None
            or expires_epoch is None
            or now_epoch is None
            or refreshed_epoch > now_epoch
            or expires_epoch < now_epoch + 255_000_000
        ):
            return False
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            lease_state = connection.execute(
                "SELECT o.target_binding_id,o.operation_kind,l.acquired_at,l.expires_at,"
                "e.id AS preflight_event_id,e.occurred_at AS preflight_at,"
                "t.oracle_fingerprint,b.controls_fingerprint,b.status AS bootstrap_status "
                "FROM sheet_remote_operations o "
                "JOIN sheet_operation_leases l ON l.operation_id=o.id "
                "AND l.fence_version=o.last_fence_version "
                "JOIN sheet_target_bindings t ON t.id=o.target_binding_id "
                "JOIN sheet_bootstraps b ON b.target_binding_id=o.target_binding_id "
                "LEFT JOIN sheet_operation_events e ON e.operation_id=o.id "
                "AND e.fence_version=o.last_fence_version "
                "AND e.event_kind='preflight_absent' "
                "WHERE o.id=? AND l.id=? ORDER BY e.id DESC LIMIT 1",
                (lease.operation_id, lease.lease_id),
            ).fetchone()
            if lease_state is None or lease_state["preflight_event_id"] is None:
                return False
            expected_bootstrap_status = "ready" if lease_state["operation_kind"] == "delivery" else "configuring"
            if (
                lease_state["oracle_fingerprint"] != oracle_fingerprint
                or lease_state["controls_fingerprint"] != controls_fingerprint
                or lease_state["bootstrap_status"] != expected_bootstrap_status
            ):
                return False
            lease_acquired_epoch = aware_epoch_us(lease_state["acquired_at"])
            lease_expires_epoch = aware_epoch_us(lease_state["expires_at"])
            preflight_epoch = aware_epoch_us(lease_state["preflight_at"])
            if (
                lease_acquired_epoch is None
                or lease_expires_epoch is None
                or preflight_epoch is None
                or preflight_epoch < lease_acquired_epoch
                or preflight_epoch > now_epoch
                or preflight_epoch >= lease_expires_epoch
            ):
                return False
            if lease_expires_epoch <= now_epoch:
                self._recover_expired_pre_marker(connection, int(lease_state["target_binding_id"]), now)
                return False
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'dispatch_marked',NULL,?)",
                (lease.operation_id, lease.fence_version, now),
            )
            changed = connection.execute(
                "UPDATE sheet_remote_operations SET status='possibly_sent',"
                "certainty='possibly_sent',dispatch_at=?,diagnostic_probe_at=?,"
                "request_sha256=?,preflight_event_id=?,preflight_fence_version=?,"
                "preflight_at=?,"
                "validated_oracle_fingerprint=?,validated_controls_fingerprint=?,"
                "credential_refreshed_at=?,credential_expires_at=?,credential_scope_ok=1 "
                "WHERE id=? AND last_fence_version=? AND status='acquired' "
                "AND finished_at IS NULL",
                (
                    now,
                    now,
                    request_sha256,
                    lease_state["preflight_event_id"],
                    lease.fence_version,
                    lease_state["preflight_at"],
                    oracle_fingerprint,
                    controls_fingerprint,
                    credential_refreshed_at,
                    credential_expires_at,
                    lease.operation_id,
                    lease.fence_version,
                ),
            ).rowcount
            if changed != 1:
                raise sqlite3.IntegrityError("lost dispatch fence")
            operation = connection.execute(
                "SELECT operation_kind,handoff_id,target_binding_id FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            ).fetchone()
            assert operation is not None
            if operation["operation_kind"] == "delivery":
                connection.execute(
                    "UPDATE sheet_handoffs SET status='ambiguous' WHERE id=? AND status='delivering'",
                    (operation["handoff_id"],),
                )
            else:
                connection.execute(
                    "UPDATE sheet_bootstraps SET status='ambiguous' WHERE target_binding_id=? AND status='configuring'",
                    (operation["target_binding_id"],),
                )
            return True

    def release_possibly_sent(self, lease: SheetLease, *, now: str) -> bool:
        if lease.lease_mode != "mutate":
            return False
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'transport_ambiguous',NULL,?)",
                (lease.operation_id, lease.fence_version, now),
            )
            return self._finish(connection, lease, "released", now, "transport_ambiguous")

    def acquire_probe(
        self,
        operation_id: int,
        *,
        expected_fence: int,
        now: str,
        expires_at: str,
    ) -> SheetLease | None:
        with self.storage.transaction() as connection:
            old = connection.execute(
                "SELECT * FROM sheet_remote_operations WHERE id=? "
                "AND last_fence_version=? AND status='possibly_sent' AND finished_at IS NULL",
                (operation_id, expected_fence),
            ).fetchone()
            if old is None:
                return None
            active = connection.execute(
                "SELECT * FROM sheet_operation_leases WHERE operation_id=? AND status='active'",
                (operation_id,),
            ).fetchone()
            if active is not None:
                expires_epoch = aware_epoch_us(active["expires_at"])
                now_epoch = aware_epoch_us(now)
                if expires_epoch is None or now_epoch is None or expires_epoch > now_epoch:
                    return None
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?,'probe_unavailable',NULL,?)",
                    (operation_id, expected_fence, now),
                )
                terminal_status = "expired"
                connection.execute(
                    "UPDATE sheet_operation_leases SET status=?,finished_at=?,"
                    "finish_reason='probe_unavailable' WHERE id=?",
                    (terminal_status, now, active["id"]),
                )
            new_fence = expected_fence + 1
            event_id = int(connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_events").fetchone()[0])
            lease_id = int(connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_leases").fetchone()[0])
            changed = connection.execute(
                "UPDATE sheet_remote_operations SET last_fence_version=?,"
                "current_acquired_event_id=?,current_lease_id=? "
                "WHERE id=? AND last_fence_version=? AND finished_at IS NULL",
                (new_fence, event_id, lease_id, operation_id, expected_fence),
            ).rowcount
            if changed != 1:
                return None
            return self._add_lease(
                connection,
                operation_id,
                new_fence,
                "probe",
                now,
                expires_at,
                event_id=event_id,
                lease_id=lease_id,
            )

    def record_probe(
        self,
        lease: SheetLease,
        *,
        outcome: Literal["exact", "absent", "duplicate", "conflict", "unavailable"],
        now: str,
        matching_marker_count: int | None = None,
        safe_detail: str | None = None,
    ) -> bool:
        del safe_detail
        if lease.lease_mode not in ("mutate", "probe"):
            return False
        counts = {"exact": 1, "absent": 0, "duplicate": 2, "conflict": 1, "unavailable": 0}
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,?,NULL,?)",
                (lease.operation_id, lease.fence_version, f"probe_{outcome}", now),
            )
            connection.execute(
                "INSERT INTO sheet_operation_probes("
                "operation_id,fence_version,result,matching_marker_count,observed_at"
                ") VALUES(?,?,?,?,?)",
                (
                    lease.operation_id,
                    lease.fence_version,
                    outcome,
                    counts[outcome] if matching_marker_count is None else matching_marker_count,
                    now,
                ),
            )
            return True

    def release_probe_unresolved(
        self, lease: SheetLease, *, outcome: Literal["absent", "unavailable"], now: str
    ) -> bool:
        """Release a read-only probe while preserving permanent no-resend ambiguity."""
        if lease.lease_mode != "probe":
            return False
        reason = f"probe_{outcome}"
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            if not connection.execute(
                "SELECT 1 FROM sheet_operation_events WHERE operation_id=? AND fence_version=? AND event_kind=?",
                (lease.operation_id, lease.fence_version, reason),
            ).fetchone():
                return False
            return self._finish(connection, lease, "released", now, reason)

    def settle_trusted_rejection(
        self,
        lease: SheetLease,
        *,
        retryable: bool,
        safe_code: str,
        now: str,
        retry_at: str | None = None,
    ) -> bool:
        """Settle post-marker rejection only for its still-active mutation owner."""
        if lease.lease_mode != "mutate":
            return False
        try:
            safe_code = SafeCode(safe_code).value
        except ValueError:
            return False
        retryable_codes = {SafeCode.ABORTED.value, SafeCode.RATE_LIMITED.value}
        blocked_codes = {
            SafeCode.TEMPLATE_DRIFT.value,
            SafeCode.METADATA_CONFLICT.value,
            SafeCode.NOT_FOUND.value,
            SafeCode.INVALID_REQUEST.value,
            SafeCode.UNAUTHENTICATED.value,
            SafeCode.PERMISSION_DENIED.value,
        }
        if (retryable and safe_code not in retryable_codes) or (not retryable and safe_code not in blocked_codes):
            return False
        if retryable:
            if retry_at is None:
                return False
            try:
                parsed_retry_at = datetime.fromisoformat(retry_at)
                parsed_now = datetime.fromisoformat(now)
                if parsed_retry_at.tzinfo is None or parsed_now.tzinfo is None or parsed_retry_at <= parsed_now:
                    return False
            except (TypeError, ValueError):
                return False
        elif retry_at is not None:
            return False
        outcome: Literal["rejected_retryable", "rejected_blocked"]
        outcome = "rejected_retryable" if retryable else "rejected_blocked"
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            operation = connection.execute(
                "SELECT handoff_id,target_binding_id,operation_kind "
                "FROM sheet_remote_operations WHERE id=? AND last_fence_version=? "
                "AND status='possibly_sent' AND certainty='possibly_sent' "
                "AND finished_at IS NULL",
                (lease.operation_id, lease.fence_version),
            ).fetchone()
            if operation is None:
                return False
            subject_status = "retryable" if retryable else "blocked"
            settlement_id = int(
                connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
            )
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?, 'trusted_rejection',?,?)",
                (lease.operation_id, lease.fence_version, safe_code, now),
            )
            if (
                connection.execute(
                    "UPDATE sheet_remote_operations SET status=?,certainty='settled_not_applied',"
                    "finished_at=?,outcome=?,safe_error_code=?,settlement_id=? "
                    "WHERE id=? AND last_fence_version=? AND status='possibly_sent' "
                    "AND certainty='possibly_sent' AND finished_at IS NULL",
                    (
                        outcome,
                        now,
                        outcome,
                        safe_code,
                        settlement_id,
                        lease.operation_id,
                        lease.fence_version,
                    ),
                ).rowcount
                != 1
            ):
                return False
            if operation["operation_kind"] == "delivery":
                self._settle_handoff(
                    connection,
                    int(operation["handoff_id"]),
                    outcome,
                    now,
                    retry_at=retry_at,
                    safe_error_code=safe_code,
                )
            else:
                self._settle_bootstrap(
                    connection,
                    int(operation["target_binding_id"]),
                    outcome,
                    now,
                    retry_at=retry_at,
                    safe_error_code=safe_code,
                )
            if not self._finish(connection, lease, "released", now, "trusted_rejection"):
                return False
            self._record_settlement(
                connection,
                settlement_id=settlement_id,
                operation_id=lease.operation_id,
                fence_version=lease.fence_version,
                operation_status=outcome,
                outcome=outcome,
                subject_status=subject_status,
                lease_status="released",
                lease_reason="trusted_rejection",
                now=now,
            )
            return True

    def finish(
        self,
        lease: SheetLease,
        *,
        outcome: OperationOutcome,
        now: str,
        remote_row: int | None = None,
    ) -> bool:
        del remote_row
        mapping = {
            "applied": ("applied", "settled_applied"),
            "reused": ("applied", "settled_applied"),
            "rejected_retryable": ("rejected_retryable", "settled_not_applied"),
            "rejected_blocked": ("rejected_blocked", "settled_not_applied"),
            "abandoned_pre_marker": ("abandoned_pre_marker", "not_dispatched"),
            "duplicate_metadata": ("blocked", "settled_applied"),
            "conflicting_metadata": ("blocked", "settled_applied"),
            "schema_conflict": ("blocked", "settled_not_applied"),
            "local_corrupt": ("corrupt", "settled_not_applied"),
            "operator_unresolved": ("manual_required", "operator_unresolved"),
        }
        if outcome in ("rejected_retryable", "rejected_blocked"):
            return False
        with self.storage.transaction() as connection:
            if not self._owns(connection, lease):
                return False
            operation = connection.execute(
                "SELECT operation_kind,handoff_id,target_binding_id,status "
                "FROM sheet_remote_operations "
                "WHERE id=? AND last_fence_version=? AND finished_at IS NULL",
                (lease.operation_id, lease.fence_version),
            ).fetchone()
            if operation is None:
                return False
            operation_status = str(operation["status"])
            evidence_kind: str | None = {
                ("acquired", "reused"): "preflight_exact",
                ("acquired", "duplicate_metadata"): "preflight_conflict",
                ("acquired", "conflicting_metadata"): "preflight_conflict",
                ("possibly_sent", "applied"): "probe_exact",
                ("possibly_sent", "duplicate_metadata"): "probe_duplicate",
                ("possibly_sent", "conflicting_metadata"): "probe_conflict",
                ("possibly_sent", "schema_conflict"): "probe_conflict",
                ("possibly_sent", "local_corrupt"): "probe_conflict",
            }.get((operation_status, outcome))
            if outcome == "operator_unresolved" and operation_status == "possibly_sent":
                evidence_ok = True
            elif evidence_kind is None:
                evidence_ok = False
            elif evidence_kind.startswith("preflight_"):
                evidence_ok = (
                    connection.execute(
                        "SELECT 1 FROM sheet_operation_events WHERE operation_id=? "
                        "AND fence_version=? AND event_kind=?",
                        (lease.operation_id, lease.fence_version, evidence_kind),
                    ).fetchone()
                    is not None
                )
            else:
                evidence_ok = (
                    connection.execute(
                        "SELECT 1 FROM sheet_operation_probes WHERE operation_id=? AND fence_version=? AND result=?",
                        (
                            lease.operation_id,
                            lease.fence_version,
                            evidence_kind.removeprefix("probe_"),
                        ),
                    ).fetchone()
                    is not None
                )
            if not evidence_ok:
                return False
            if operation["operation_kind"] == "delivery":
                subject_status = {
                    "applied": "delivered",
                    "reused": "delivered",
                    "duplicate_metadata": "blocked",
                    "conflicting_metadata": "blocked",
                    "schema_conflict": "blocked",
                    "local_corrupt": "corrupt",
                    "operator_unresolved": "manual_required",
                    "abandoned_pre_marker": "retryable",
                }[outcome]
            else:
                subject_status = {
                    "applied": "ready",
                    "reused": "ready",
                    "duplicate_metadata": "blocked",
                    "conflicting_metadata": "blocked",
                    "schema_conflict": "blocked",
                    "local_corrupt": "blocked",
                    "operator_unresolved": "manual_required",
                    "abandoned_pre_marker": "retryable",
                }[outcome]
            status, certainty = mapping[outcome]
            event = "operator_manual_required" if outcome == "operator_unresolved" else "finalized"
            settlement_id = int(
                connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
            )
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,?,?,?)",
                (lease.operation_id, lease.fence_version, event, outcome, now),
            )
            safe_code = None if outcome in ("applied", "reused") else outcome
            if (
                connection.execute(
                    "UPDATE sheet_remote_operations SET status=?,certainty=?,finished_at=?,"
                    "outcome=?,safe_error_code=?,settlement_id=? "
                    "WHERE id=? AND last_fence_version=? AND finished_at IS NULL",
                    (
                        status,
                        certainty,
                        now,
                        outcome,
                        safe_code,
                        settlement_id,
                        lease.operation_id,
                        lease.fence_version,
                    ),
                ).rowcount
                != 1
            ):
                return False
            if operation["operation_kind"] == "delivery":
                self._settle_handoff(connection, int(operation["handoff_id"]), outcome, now)
            else:
                self._settle_bootstrap(connection, int(operation["target_binding_id"]), outcome, now)
            if not self._finish(connection, lease, "released", now, event):
                return False
            self._record_settlement(
                connection,
                settlement_id=settlement_id,
                operation_id=lease.operation_id,
                fence_version=lease.fence_version,
                operation_status=status,
                outcome=outcome,
                subject_status=subject_status,
                lease_status="released",
                lease_reason=event,
                now=now,
            )
            return True

    @staticmethod
    def _record_settlement(
        connection: sqlite3.Connection,
        *,
        settlement_id: int,
        operation_id: int,
        fence_version: int,
        operation_status: str,
        outcome: str,
        subject_status: str,
        lease_status: str,
        lease_reason: str,
        now: str,
    ) -> None:
        connection.execute(
            "INSERT INTO sheet_operation_settlements("
            "id,operation_id,fence_version,operation_status,outcome,subject_status,"
            "lease_status,lease_reason,settled_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                settlement_id,
                operation_id,
                fence_version,
                operation_status,
                outcome,
                subject_status,
                lease_status,
                lease_reason,
                now,
            ),
        )

    @staticmethod
    def _settle_handoff(
        connection: sqlite3.Connection,
        handoff_id: int,
        outcome: OperationOutcome,
        now: str,
        *,
        retry_at: str | None = None,
        safe_error_code: str | None = None,
    ) -> None:
        if outcome in ("applied", "reused"):
            connection.execute(
                "UPDATE sheet_handoffs SET status='delivered',delivered_at=?,retry_at=NULL,"
                "safe_error_code=NULL WHERE id=? AND status IN ('delivering','ambiguous')",
                (now, handoff_id),
            )
        elif outcome in ("rejected_retryable", "abandoned_pre_marker"):
            connection.execute(
                "UPDATE sheet_handoffs SET status='retryable',retry_at=?,delivered_at=NULL,"
                "safe_error_code=? WHERE id=? AND status IN ('delivering','ambiguous')",
                (retry_at or now, safe_error_code or outcome, handoff_id),
            )
        elif outcome == "local_corrupt":
            connection.execute(
                "UPDATE sheet_handoffs SET status='corrupt',retry_at=NULL,delivered_at=NULL,"
                "safe_error_code=? WHERE id=? AND status IN ('delivering','ambiguous')",
                (outcome, handoff_id),
            )
        elif outcome == "operator_unresolved":
            connection.execute(
                "UPDATE sheet_handoffs SET status='manual_required',retry_at=NULL,"
                "delivered_at=NULL,safe_error_code=? "
                "WHERE id=? AND status IN ('delivering','ambiguous')",
                (outcome, handoff_id),
            )
        else:
            connection.execute(
                "UPDATE sheet_handoffs SET status='blocked',retry_at=NULL,delivered_at=NULL,"
                "safe_error_code=? WHERE id=? AND status IN ('delivering','ambiguous')",
                (safe_error_code or outcome, handoff_id),
            )

    @staticmethod
    def _settle_bootstrap(
        connection: sqlite3.Connection,
        target_binding_id: int,
        outcome: OperationOutcome,
        now: str,
        *,
        retry_at: str | None = None,
        safe_error_code: str | None = None,
    ) -> None:
        if outcome in ("applied", "reused"):
            connection.execute(
                "UPDATE sheet_bootstraps SET status='ready',verified_at=?,retry_at=NULL,"
                "safe_error_code=NULL WHERE target_binding_id=? "
                "AND status IN ('configuring','ambiguous')",
                (now, target_binding_id),
            )
        elif outcome in ("rejected_retryable", "abandoned_pre_marker"):
            connection.execute(
                "UPDATE sheet_bootstraps SET status='retryable',retry_at=?,verified_at=NULL,"
                "safe_error_code=? WHERE target_binding_id=? "
                "AND status IN ('configuring','ambiguous')",
                (retry_at or now, safe_error_code or outcome, target_binding_id),
            )
        elif outcome == "operator_unresolved":
            connection.execute(
                "UPDATE sheet_bootstraps SET status='manual_required',retry_at=NULL,"
                "verified_at=NULL,safe_error_code=? WHERE target_binding_id=? "
                "AND status IN ('configuring','ambiguous')",
                (outcome, target_binding_id),
            )
        else:
            connection.execute(
                "UPDATE sheet_bootstraps SET status='blocked',retry_at=NULL,verified_at=NULL,"
                "safe_error_code=? WHERE target_binding_id=? "
                "AND status IN ('configuring','ambiguous')",
                (safe_error_code or outcome, target_binding_id),
            )

    def _recover_expired_pre_marker(self, connection: sqlite3.Connection, target_binding_id: int, now: str) -> None:
        row = connection.execute(
            "SELECT o.id,o.operation_kind,o.handoff_id,o.last_fence_version,l.id AS lease_id "
            "FROM sheet_remote_operations o "
            "JOIN sheet_operation_leases l ON l.operation_id=o.id "
            "AND l.fence_version=o.last_fence_version AND l.status='active' "
            "WHERE o.target_binding_id=? AND o.status='acquired' "
            "AND o.dispatch_at IS NULL AND o.finished_at IS NULL "
            "AND l.lease_mode='mutate' "
            "AND aware_epoch_us(l.expires_at)<=aware_epoch_us(?)",
            (target_binding_id, now),
        ).fetchone()
        if row is None:
            return
        operation_id = int(row["id"])
        fence = int(row["last_fence_version"])
        connection.execute(
            "INSERT INTO sheet_operation_events("
            "operation_id,fence_version,event_kind,safe_code,occurred_at"
            ") VALUES(?,?,'lease_expired_pre_marker','abandoned_pre_marker',?)",
            (operation_id, fence, now),
        )
        settlement_id = int(
            connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
        )
        if (
            connection.execute(
                "UPDATE sheet_remote_operations SET status='abandoned_pre_marker',"
                "certainty='not_dispatched',finished_at=?,outcome='abandoned_pre_marker',"
                "safe_error_code='abandoned_pre_marker',settlement_id=? "
                "WHERE id=? AND last_fence_version=? AND status='acquired' "
                "AND dispatch_at IS NULL AND finished_at IS NULL",
                (now, settlement_id, operation_id, fence),
            ).rowcount
            != 1
        ):
            raise RuntimeError("expired pre-marker operation changed concurrently")
        if row["operation_kind"] == "delivery":
            self._settle_handoff(
                connection,
                int(row["handoff_id"]),
                "abandoned_pre_marker",
                now,
            )
        else:
            self._settle_bootstrap(
                connection,
                target_binding_id,
                "abandoned_pre_marker",
                now,
            )
        if (
            connection.execute(
                "UPDATE sheet_operation_leases SET status='expired',finished_at=?,"
                "finish_reason='lease_expired_pre_marker' "
                "WHERE id=? AND operation_id=? AND fence_version=? AND status='active'",
                (now, int(row["lease_id"]), operation_id, fence),
            ).rowcount
            != 1
        ):
            raise RuntimeError("expired pre-marker lease changed concurrently")
        self._record_settlement(
            connection,
            settlement_id=settlement_id,
            operation_id=operation_id,
            fence_version=fence,
            operation_status="abandoned_pre_marker",
            outcome="abandoned_pre_marker",
            subject_status="retryable",
            lease_status="expired",
            lease_reason="lease_expired_pre_marker",
            now=now,
        )

    def _new_operation(
        self, connection: sqlite3.Connection, target: int, kind: str, handoff: int | None, now: str, expires_at: str
    ) -> SheetLease:
        ordinal = int(
            connection.execute(
                "SELECT COALESCE(MAX(ordinal),0)+1 FROM sheet_remote_operations WHERE target_binding_id=?", (target,)
            ).fetchone()[0]
        )
        operation_id = int(
            connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_remote_operations").fetchone()[0]
        )
        event_id = int(connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_events").fetchone()[0])
        lease_id = int(connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_leases").fetchone()[0])
        connection.execute(
            "INSERT INTO sheet_remote_operations(id,target_binding_id,operation_kind,handoff_id,ordinal,status,certainty,last_fence_version,current_acquired_event_id,current_lease_id,acquired_at) VALUES(?,?,?,?,?,'acquired','not_dispatched',1,?,?,?)",
            (operation_id, target, kind, handoff, ordinal, event_id, lease_id, now),
        )
        return self._add_lease(
            connection, operation_id, 1, "mutate", now, expires_at, event_id=event_id, lease_id=lease_id
        )

    def _add_lease(
        self,
        connection: sqlite3.Connection,
        operation_id: int,
        fence: int,
        mode: Literal["mutate", "probe"],
        now: str,
        expires_at: str,
        *,
        event_id: int,
        lease_id: int,
    ) -> SheetLease:
        token = secrets.token_hex(32)
        self.storage._authorize_lease(token)
        connection.execute(
            "INSERT INTO sheet_operation_events(id,operation_id,fence_version,event_kind,safe_code,occurred_at) VALUES(?,?,?, 'acquired',NULL,?)",
            (event_id, operation_id, fence, now),
        )
        connection.execute(
            "INSERT INTO sheet_operation_leases(id,operation_id,fence_version,owner_token_hash,lease_mode,acquired_at,expires_at,status) VALUES(?,?,?,?,?,?,?,'active')",
            (lease_id, operation_id, fence, sha256(token.encode()).hexdigest(), mode, now, expires_at),
        )
        return SheetLease(operation_id, fence, event_id, lease_id, token, mode)

    def _owns(self, connection: sqlite3.Connection, lease: SheetLease) -> bool:
        self.storage._authorize_lease(lease.owner_token)
        return (
            connection.execute(
                "SELECT 1 FROM sheet_operation_leases WHERE id=? AND operation_id=? AND fence_version=? AND status='active' AND owner_token_hash=?",
                (
                    lease.lease_id,
                    lease.operation_id,
                    lease.fence_version,
                    sha256(lease.owner_token.encode()).hexdigest(),
                ),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _finish(
        connection: sqlite3.Connection, lease: SheetLease, status: Literal["released", "expired"], now: str, reason: str
    ) -> bool:
        return (
            connection.execute(
                "UPDATE sheet_operation_leases SET status=?,finished_at=?,finish_reason=? WHERE id=? AND operation_id=? AND fence_version=? AND status='active'",
                (status, now, reason, lease.lease_id, lease.operation_id, lease.fence_version),
            ).rowcount
            == 1
        )


def _handoff(row: sqlite3.Row) -> SheetHandoff:
    return SheetHandoff(
        int(row["id"]),
        int(row["generation_id"]),
        int(row["approval_event_id"]),
        str(row["export_id"]),
        cast(SheetCategory, row["category"]),
        bytes(row["canonical_bytes"]),
    )

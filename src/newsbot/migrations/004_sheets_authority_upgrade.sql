-- Upgrade databases that recorded an earlier revision of migration 003.
-- Storage replaces __HANDOFF_TARGET_EXPR__ according to the installed schema.
PRAGMA legacy_alter_table=ON;
ALTER TABLE sheet_handoffs RENAME TO sheet_handoffs_v3;
CREATE TABLE sheet_handoffs (
 id INTEGER PRIMARY KEY,
 generation_id INTEGER NOT NULL REFERENCES generations(id) ON DELETE RESTRICT,
 approval_event_id INTEGER NOT NULL UNIQUE REFERENCES decision_events(id) ON DELETE RESTRICT,
 target_binding_id INTEGER NOT NULL,
 export_id TEXT NOT NULL CHECK(length(export_id)=36 AND substr(export_id,1,4)='exp_' AND length(substr(export_id,5))=32 AND substr(export_id,5) NOT GLOB '*[^0-9a-f]*'),
 canonical_bytes BLOB NOT NULL CHECK(length(canonical_bytes)>0),
 canonical_sha256 TEXT NOT NULL CHECK(length(canonical_sha256)=64 AND canonical_sha256 NOT GLOB '*[^0-9a-f]*' AND canonical_sha256=sha256_hex(canonical_bytes)),
 approved_at TEXT NOT NULL, category TEXT NOT NULL CHECK(category IN ('AI','Blockchain')),
 initial_upload_status TEXT NOT NULL CHECK(initial_upload_status='X'),
 marker_value TEXT NOT NULL CHECK(marker_value='v1:' || export_id || ':' || canonical_sha256), status TEXT NOT NULL CHECK(status IN ('pending','retryable','delivering','ambiguous','delivered','blocked','corrupt','manual_required')),
 safe_error_code TEXT CHECK(safe_error_code IS NULL OR safe_error_code IN ('template_drift','metadata_conflict','not_found','invalid_request','unauthenticated','permission_denied','rate_limited','aborted','ambiguous','rejected_retryable','rejected_blocked','abandoned_pre_marker','duplicate_metadata','conflicting_metadata','schema_conflict','local_corrupt','operator_unresolved')), retry_at TEXT, delivered_at TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
 UNIQUE(generation_id,approval_event_id), UNIQUE(id,status), CHECK((status='retryable')=(retry_at IS NOT NULL)), CHECK(retry_at IS NULL OR aware_epoch_us(retry_at) IS NOT NULL), CHECK((status='delivered')=(delivered_at IS NOT NULL)), CHECK(status!='delivered' OR safe_error_code IS NULL), FOREIGN KEY(id,target_binding_id) REFERENCES sheet_handoff_bindings(handoff_id,target_binding_id) DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO sheet_handoffs(
 id,generation_id,approval_event_id,target_binding_id,export_id,canonical_bytes,canonical_sha256,
 approved_at,category,initial_upload_status,marker_value,status,safe_error_code,retry_at,delivered_at,created_at
)
SELECT h.id,h.generation_id,h.approval_event_id,__HANDOFF_TARGET_EXPR__,h.export_id,h.canonical_bytes,h.canonical_sha256,
 h.approved_at,h.category,h.initial_upload_status,h.marker_value,h.status,h.safe_error_code,h.retry_at,h.delivered_at,h.created_at
FROM sheet_handoffs_v3 h LEFT JOIN sheet_handoff_bindings b ON b.handoff_id=h.id;
DROP TABLE sheet_handoffs_v3;
PRAGMA legacy_alter_table=OFF;

CREATE TRIGGER handoff_no_delete BEFORE DELETE ON sheet_handoffs BEGIN SELECT RAISE(ABORT,'sheet handoff cannot be deleted'); END;
CREATE TRIGGER handoff_update BEFORE UPDATE ON sheet_handoffs BEGIN
 SELECT CASE WHEN NEW.generation_id!=OLD.generation_id OR NEW.approval_event_id!=OLD.approval_event_id OR NEW.target_binding_id!=OLD.target_binding_id OR NEW.export_id!=OLD.export_id OR NEW.canonical_bytes!=OLD.canonical_bytes OR NEW.canonical_sha256!=OLD.canonical_sha256 OR NEW.approved_at!=OLD.approved_at OR NEW.category!=OLD.category OR NEW.initial_upload_status!=OLD.initial_upload_status OR NEW.marker_value!=OLD.marker_value OR NEW.created_at!=OLD.created_at THEN RAISE(ABORT,'sheet handoff identity is immutable') END;
 SELECT CASE WHEN OLD.status IN ('delivered','corrupt','manual_required') THEN RAISE(ABORT,'terminal handoff is immutable') END;
 SELECT CASE WHEN NOT ((OLD.status IN ('pending','retryable') AND NEW.status='delivering') OR (OLD.status='delivering' AND NEW.status IN ('delivered','retryable','blocked','corrupt','ambiguous')) OR (OLD.status='ambiguous' AND NEW.status IN ('delivered','retryable','blocked','corrupt','manual_required')) OR (OLD.status='blocked' AND NEW.status='retryable')) THEN RAISE(ABORT,'illegal handoff transition') END;
 SELECT CASE WHEN OLD.status='ambiguous' AND NEW.status='retryable' AND NOT EXISTS(SELECT 1 FROM sheet_remote_operations o JOIN sheet_operation_events e ON e.operation_id=o.id AND e.fence_version=o.last_fence_version AND e.event_kind='trusted_rejection' WHERE o.handoff_id=OLD.id AND o.status='rejected_retryable' AND aware_epoch_us(NEW.retry_at)>aware_epoch_us(e.occurred_at)) THEN RAISE(ABORT,'retry deadline invalid') END;
 SELECT CASE WHEN OLD.status='blocked' AND NEW.status='retryable' AND NOT EXISTS(SELECT 1 FROM sheet_remote_operations o JOIN sheet_operation_events e ON e.operation_id=o.id AND e.fence_version=o.last_fence_version AND e.event_kind='operator_corrected' AND e.safe_code=OLD.safe_error_code AND e.occurred_at=NEW.retry_at WHERE o.handoff_id=OLD.id AND o.finished_at IS NOT NULL AND (o.status='rejected_blocked' OR (o.status='blocked' AND o.outcome='schema_conflict' AND o.certainty='settled_not_applied')) AND NOT EXISTS(SELECT 1 FROM sheet_remote_operations open WHERE open.target_binding_id=o.target_binding_id AND open.finished_at IS NULL)) THEN RAISE(ABORT,'operator correction invalid') END;
 SELECT CASE WHEN NOT (OLD.status='blocked' AND NEW.status='retryable') AND NOT EXISTS(
   SELECT 1 FROM sheet_remote_operations o JOIN sheet_operation_leases l ON l.operation_id=o.id AND l.fence_version=o.last_fence_version AND l.status='active'
   JOIN sheet_operation_events e ON e.operation_id=o.id AND e.fence_version=o.last_fence_version
   WHERE o.handoff_id=OLD.id AND (
    (OLD.status IN ('pending','retryable') AND NEW.status='delivering' AND (OLD.status='pending' OR aware_epoch_us(OLD.retry_at)<=aware_epoch_us(e.occurred_at)) AND l.lease_mode='mutate' AND o.status='acquired' AND e.event_kind='acquired')
    OR (OLD.status='delivering' AND NEW.status='ambiguous' AND l.lease_mode='mutate' AND o.status='possibly_sent' AND e.event_kind='dispatch_marked')
    OR (OLD.status='delivering' AND NEW.status='retryable' AND l.lease_mode='mutate' AND o.status='abandoned_pre_marker' AND e.event_kind='lease_expired_pre_marker')
    OR (OLD.status='delivering' AND NEW.status IN ('delivered','blocked','corrupt') AND e.event_kind='finalized')
    OR (OLD.status='ambiguous' AND NEW.status IN ('delivered','blocked','corrupt','manual_required') AND e.event_kind IN ('finalized','operator_manual_required'))
    OR (OLD.status='ambiguous' AND NEW.status='retryable' AND l.lease_mode='mutate' AND o.status='rejected_retryable' AND e.event_kind='trusted_rejection')
    OR (OLD.status='ambiguous' AND NEW.status='blocked' AND l.lease_mode='mutate' AND o.status='rejected_blocked' AND e.event_kind='trusted_rejection')
   )
 ) THEN RAISE(ABORT,'handoff transition lacks active lease event') END;
END;

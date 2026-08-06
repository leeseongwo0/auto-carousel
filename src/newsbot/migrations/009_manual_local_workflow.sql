-- Manual/local authority is additive and deliberately creates no work.
CREATE TABLE manual_profile_bindings (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    mode TEXT NOT NULL CHECK(mode = 'manual_local'),
    schema_version TEXT NOT NULL CHECK(schema_version = 'newsbot.behavior.v1'),
    profile_digest TEXT NOT NULL CHECK(length(profile_digest) = 64 AND profile_digest NOT GLOB '*[^0-9a-f]*'),
    bound_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE manual_local_decisions (
    id INTEGER PRIMARY KEY,
    profile_binding_id INTEGER NOT NULL DEFAULT 1 REFERENCES manual_profile_bindings(id) ON DELETE RESTRICT,
    generation_id INTEGER NOT NULL UNIQUE REFERENCES generations(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK(decision IN ('approve_local', 'regenerate', 'reject')),
    source_set_digest TEXT NOT NULL CHECK(length(source_set_digest) = 64 AND source_set_digest NOT GLOB '*[^0-9a-f]*'),
    decided_at TEXT NOT NULL
);
CREATE TABLE manual_candidate_decisions (
    id INTEGER PRIMARY KEY,
    profile_binding_id INTEGER NOT NULL DEFAULT 1 REFERENCES manual_profile_bindings(id) ON DELETE RESTRICT,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE RESTRICT,
    candidate_id INTEGER NOT NULL UNIQUE REFERENCES candidates(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK(decision IN ('select', 'reject')),
    source_set_digest TEXT NOT NULL CHECK(length(source_set_digest) = 64 AND source_set_digest NOT GLOB '*[^0-9a-f]*'),
    candidate_preview_receipt TEXT NOT NULL CHECK(length(candidate_preview_receipt) = 64 AND candidate_preview_receipt NOT GLOB '*[^0-9a-f]*'),
    decided_at TEXT NOT NULL
);

CREATE TABLE manual_local_export_outbox (
    id INTEGER PRIMARY KEY,
    profile_binding_id INTEGER NOT NULL DEFAULT 1 REFERENCES manual_profile_bindings(id) ON DELETE RESTRICT,
    approval_id INTEGER NOT NULL REFERENCES manual_local_decisions(id) ON DELETE RESTRICT,
    export_format TEXT NOT NULL CHECK(export_format IN ('json', 'markdown')),
    canonical_bytes BLOB NOT NULL,
    canonical_sha256 TEXT NOT NULL CHECK(length(canonical_sha256) = 64 AND canonical_sha256 NOT GLOB '*[^0-9a-f]*'),
    state TEXT NOT NULL DEFAULT 'ready' CHECK(state IN ('ready', 'materialized')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    materialized_at TEXT,
    UNIQUE(approval_id, export_format),
    CHECK((state = 'ready' AND materialized_at IS NULL) OR (state = 'materialized' AND materialized_at IS NOT NULL))
);

CREATE TRIGGER manual_profile_binding_immutable
BEFORE UPDATE ON manual_profile_bindings
BEGIN
    SELECT RAISE(ABORT, 'manual profile binding is immutable');
END;
CREATE TRIGGER manual_profile_binding_no_delete
BEFORE DELETE ON manual_profile_bindings
BEGIN
    SELECT RAISE(ABORT, 'manual profile binding cannot be deleted');
END;
-- Independently durable legacy automation authority roots. Shared collection
-- and generation work records remain available to the offline manual workflow.
CREATE TRIGGER manual_profile_binding_refuses_automation
BEFORE INSERT ON manual_profile_bindings
WHEN EXISTS(SELECT 1 FROM callback_tokens)
  OR EXISTS(SELECT 1 FROM decision_events)
  OR EXISTS(SELECT 1 FROM export_outbox)
  OR EXISTS(SELECT 1 FROM sheet_target_bindings)
  OR EXISTS(SELECT 1 FROM sheet_handoffs)
  OR EXISTS(SELECT 1 FROM sheet_remote_operations)
  OR EXISTS(SELECT 1 FROM sheet_operation_leases)
  OR EXISTS(SELECT 1 FROM generation_job_provider_bindings)
  OR EXISTS(SELECT 1 FROM generation_provider_attempt_classifications)
  OR EXISTS(SELECT 1 FROM generation_job_retry_state)
  OR EXISTS(SELECT 1 FROM generation_provider_controls WHERE paused_at IS NOT NULL)
  OR EXISTS(SELECT 1 FROM generation_provider_control_events)
  OR EXISTS(SELECT 1 FROM telegram_update_cursors)
  OR EXISTS(SELECT 1 FROM automation_cutover_proposals)
  OR EXISTS(SELECT 1 FROM telegram_audience_bindings)
  OR EXISTS(SELECT 1 FROM automation_cutovers)
  OR EXISTS(SELECT 1 FROM automation_release_activations)
  OR EXISTS(SELECT 1 FROM automation_release_config_bindings)
  OR EXISTS(SELECT 1 FROM automation_generation_authority)
  OR EXISTS(SELECT 1 FROM automation_defer_authority)
  OR EXISTS(SELECT 1 FROM telegram_notification_outbox)
  OR EXISTS(SELECT 1 FROM telegram_chunk_attempts)
  OR EXISTS(SELECT 1 FROM telegram_notification_events)
  OR EXISTS(SELECT 1 FROM automation_stream_leases)
  OR EXISTS(SELECT 1 FROM automation_stream_runs)
  OR EXISTS(SELECT 1 FROM automation_stream_events)
BEGIN
    SELECT RAISE(ABORT, 'manual profile conflicts with automation authority');
END;
CREATE TRIGGER manual_local_decision_immutable
BEFORE UPDATE ON manual_local_decisions
BEGIN
    SELECT RAISE(ABORT, 'manual local decision is immutable');
END;
CREATE TRIGGER manual_local_decision_no_delete
BEFORE DELETE ON manual_local_decisions
BEGIN
    SELECT RAISE(ABORT, 'manual local decisions cannot be deleted');
END;
CREATE TRIGGER manual_candidate_decision_immutable
BEFORE UPDATE ON manual_candidate_decisions
BEGIN
    SELECT RAISE(ABORT, 'manual candidate decision is immutable');
END;
CREATE TRIGGER manual_candidate_decision_no_delete
BEFORE DELETE ON manual_candidate_decisions
BEGIN
    SELECT RAISE(ABORT, 'manual candidate decisions cannot be deleted');
END;
CREATE TRIGGER manual_local_export_identity_immutable
BEFORE UPDATE OF id,profile_binding_id,approval_id,export_format,canonical_bytes,canonical_sha256,created_at ON manual_local_export_outbox
WHEN NEW.id IS NOT OLD.id OR NEW.profile_binding_id IS NOT OLD.profile_binding_id OR NEW.approval_id IS NOT OLD.approval_id OR NEW.export_format IS NOT OLD.export_format OR NEW.canonical_bytes IS NOT OLD.canonical_bytes OR NEW.canonical_sha256 IS NOT OLD.canonical_sha256 OR NEW.created_at IS NOT OLD.created_at
BEGIN
    SELECT RAISE(ABORT, 'manual local export identity is immutable');
END;
CREATE TRIGGER manual_local_export_no_delete
BEFORE DELETE ON manual_local_export_outbox
BEGIN
    SELECT RAISE(ABORT, 'manual local exports cannot be deleted');
END;
CREATE TRIGGER manual_local_export_transition
BEFORE UPDATE OF state,materialized_at ON manual_local_export_outbox
WHEN NOT (OLD.state = 'ready' AND NEW.state = 'materialized' AND NEW.materialized_at IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'invalid manual local export transition');
END;

CREATE TRIGGER manual_candidate_decision_coherent
BEFORE INSERT ON manual_candidate_decisions
WHEN manual_candidate_decision_authorized(
        NEW.run_id,
        NEW.candidate_id,
        NEW.decision,
        NEW.source_set_digest,
        NEW.candidate_preview_receipt,
        NEW.decided_at
    ) != 1
  OR length(NEW.candidate_preview_receipt) != 64
  OR NEW.candidate_preview_receipt GLOB '*[^0-9a-f]*'
  OR NOT EXISTS(SELECT 1 FROM manual_profile_bindings WHERE id=NEW.profile_binding_id)
  OR NOT EXISTS(
      SELECT 1 FROM candidates c JOIN candidate_evaluations e ON e.id=c.evaluation_id
      WHERE c.id=NEW.candidate_id AND e.run_id=NEW.run_id AND c.status='pending_selection'
  )
  OR NOT EXISTS(SELECT 1 FROM candidate_sources WHERE candidate_id=NEW.candidate_id)
  OR NEW.source_set_digest != (
      SELECT sha256_hex(CAST('[' || group_concat(source_post_version_id, ',') || ']' AS BLOB))
      FROM (
          SELECT source_post_version_id
          FROM candidate_sources
          WHERE candidate_id=NEW.candidate_id
          ORDER BY source_post_version_id
      )
  )
BEGIN
    SELECT RAISE(ABORT, 'manual candidate decision is incoherent');
END;
CREATE TRIGGER manual_review_decision_coherent
BEFORE INSERT ON manual_local_decisions
WHEN manual_review_decision_authorized(
        NEW.generation_id,
        NEW.decision,
        NEW.source_set_digest,
        NEW.decided_at
    ) != 1
  OR NOT EXISTS(SELECT 1 FROM manual_profile_bindings WHERE id=NEW.profile_binding_id)
  OR NOT EXISTS(
      SELECT 1
      FROM generations g
      JOIN generation_jobs j ON j.id=g.generation_job_id
      JOIN selections s ON s.id=j.selection_id
      JOIN candidates c ON c.id=s.candidate_id
      WHERE g.id=NEW.generation_id AND g.status='current' AND c.status='pending_review'
  )
  OR NOT EXISTS(
      SELECT 1 FROM generation_sources
      WHERE generation_id=NEW.generation_id
  )
  OR NEW.source_set_digest != (
      SELECT sha256_hex(CAST('[' || group_concat(source_post_version_id, ',') || ']' AS BLOB))
      FROM (
          SELECT source_post_version_id
          FROM generation_sources
          WHERE generation_id=NEW.generation_id
          ORDER BY source_post_version_id
      )
  )
  OR EXISTS(
      SELECT 1
      FROM generation_sources gs
      WHERE gs.generation_id=NEW.generation_id
        AND NOT EXISTS(
            SELECT 1 FROM candidate_sources cs
            JOIN generations g ON g.id=NEW.generation_id
            JOIN generation_jobs j ON j.id=g.generation_job_id
            JOIN selections s ON s.id=j.selection_id
            WHERE cs.candidate_id=s.candidate_id
              AND cs.source_post_version_id=gs.source_post_version_id
        )
  )
  OR EXISTS(
      SELECT 1
      FROM candidate_sources cs
      JOIN generations g ON g.id=NEW.generation_id
      JOIN generation_jobs j ON j.id=g.generation_job_id
      JOIN selections s ON s.id=j.selection_id
      WHERE cs.candidate_id=s.candidate_id
        AND NOT EXISTS(
            SELECT 1 FROM generation_sources gs
            WHERE gs.generation_id=NEW.generation_id
              AND gs.source_post_version_id=cs.source_post_version_id
        )
  )
BEGIN
    SELECT RAISE(ABORT, 'manual review decision is incoherent');
END;
CREATE TRIGGER manual_export_coherent
BEFORE INSERT ON manual_local_export_outbox
WHEN length(NEW.canonical_bytes)=0
  OR NEW.canonical_sha256 != sha256_hex(NEW.canonical_bytes)
  OR NOT EXISTS(SELECT 1 FROM manual_profile_bindings WHERE id=NEW.profile_binding_id)
  OR NOT EXISTS(
      SELECT 1 FROM manual_local_decisions d
      WHERE d.id=NEW.approval_id
        AND d.profile_binding_id=NEW.profile_binding_id
        AND d.decision='approve_local'
  )
BEGIN
    SELECT RAISE(ABORT, 'manual local export is incoherent');
END;
CREATE TRIGGER manual_candidate_approval_complete
BEFORE UPDATE OF status ON candidates
WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
  AND NEW.status='approved'
  AND (
      OLD.status!='pending_review'
      OR NOT EXISTS(
          SELECT 1
          FROM manual_local_decisions d
          JOIN generations g ON g.id=d.generation_id
          JOIN generation_jobs j ON j.id=g.generation_job_id
          JOIN selections s ON s.id=j.selection_id
          WHERE s.candidate_id=NEW.id AND d.decision='approve_local' AND g.status='current'
      )
      OR 2 != (
          SELECT count(*) FROM manual_local_export_outbox o
          JOIN manual_local_decisions d ON d.id=o.approval_id
          JOIN generations g ON g.id=d.generation_id
          JOIN generation_jobs j ON j.id=g.generation_job_id
          JOIN selections s ON s.id=j.selection_id
          WHERE s.candidate_id=NEW.id AND d.decision='approve_local'
      )
  )
BEGIN
    SELECT RAISE(ABORT, 'manual approval requires complete exports');
END;
CREATE TRIGGER manual_candidate_rejection_coherent
BEFORE UPDATE OF status ON candidates
WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
  AND NEW.status='rejected'
  AND NOT EXISTS(
      SELECT 1 FROM manual_candidate_decisions d
      WHERE d.candidate_id=NEW.id AND d.decision='reject'
      UNION ALL
      SELECT 1 FROM manual_local_decisions d
      JOIN generations g ON g.id=d.generation_id
      JOIN generation_jobs j ON j.id=g.generation_job_id
      JOIN selections s ON s.id=j.selection_id
      WHERE s.candidate_id=NEW.id AND d.decision='reject'
  )
BEGIN
    SELECT RAISE(ABORT, 'manual rejection is incoherent');
END;
CREATE TRIGGER manual_candidate_selection_coherent
BEFORE UPDATE OF status ON candidates
WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
  AND NEW.status='selected_generation_pending'
  AND NOT EXISTS(
      SELECT 1 FROM manual_candidate_decisions d
      WHERE d.candidate_id=NEW.id AND d.decision='select'
      UNION ALL
      SELECT 1 FROM manual_local_decisions d
      JOIN generations g ON g.id=d.generation_id
      JOIN generation_jobs j ON j.id=g.generation_job_id
      JOIN selections s ON s.id=j.selection_id
      WHERE s.candidate_id=NEW.id AND d.decision='regenerate'
  )
BEGIN
    SELECT RAISE(ABORT, 'manual selection is incoherent');
END;
CREATE TRIGGER automation_callback_refuses_manual_profile
BEFORE INSERT ON callback_tokens WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_decision_event_refuses_manual_profile
BEFORE INSERT ON decision_events WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_export_refuses_manual_profile
BEFORE INSERT ON export_outbox WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_target_refuses_manual_profile
BEFORE INSERT ON sheet_target_bindings WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_handoff_refuses_manual_profile
BEFORE INSERT ON sheet_handoffs WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_handoff_operation_refuses_manual_profile
BEFORE INSERT ON sheet_remote_operations WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_handoff_lease_refuses_manual_profile
BEFORE INSERT ON sheet_operation_leases WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_provider_refuses_manual_profile
BEFORE INSERT ON generation_job_provider_bindings WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_provider_attempt_classification_refuses_manual_profile
BEFORE INSERT ON generation_provider_attempt_classifications WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_job_retry_state_refuses_manual_profile
BEFORE INSERT ON generation_job_retry_state WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_provider_pause_refuses_manual_profile
BEFORE UPDATE OF paused_at ON generation_provider_controls
WHEN EXISTS(SELECT 1 FROM manual_profile_bindings) AND NEW.paused_at IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_provider_control_event_refuses_manual_profile
BEFORE INSERT ON generation_provider_control_events WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_telegram_cursor_refuses_manual_profile
BEFORE INSERT ON telegram_update_cursors WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_proposal_refuses_manual_profile
BEFORE INSERT ON automation_cutover_proposals WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_audience_refuses_manual_profile
BEFORE INSERT ON telegram_audience_bindings WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_cutover_refuses_manual_profile
BEFORE INSERT ON automation_cutovers WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_activation_refuses_manual_profile
BEFORE INSERT ON automation_release_activations WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_config_refuses_manual_profile
BEFORE INSERT ON automation_release_config_bindings WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_generation_refuses_manual_profile
BEFORE INSERT ON automation_generation_authority WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_defer_refuses_manual_profile
BEFORE INSERT ON automation_defer_authority WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_notification_refuses_manual_profile
BEFORE INSERT ON telegram_notification_outbox WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_chunk_refuses_manual_profile
BEFORE INSERT ON telegram_chunk_attempts WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_notification_event_refuses_manual_profile
BEFORE INSERT ON telegram_notification_events WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_stream_lease_refuses_manual_profile
BEFORE INSERT ON automation_stream_leases WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_stream_run_refuses_manual_profile
BEFORE INSERT ON automation_stream_runs WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
CREATE TRIGGER automation_stream_event_refuses_manual_profile
BEFORE INSERT ON automation_stream_events WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)
BEGIN SELECT RAISE(ABORT, 'automation authority conflicts with manual profile'); END;
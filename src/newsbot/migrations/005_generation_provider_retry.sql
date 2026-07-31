-- Durable Codex-only retry, pause, and audit authority.
-- The migration runner encloses this script in BEGIN IMMEDIATE/COMMIT.
CREATE TABLE generation_job_provider_bindings (
    generation_job_id INTEGER PRIMARY KEY REFERENCES generation_jobs(id) ON DELETE RESTRICT,
    provider_name TEXT NOT NULL CHECK(provider_name='codex_cli'),
    bound_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00','now'))
        CHECK(aware_epoch_us(bound_at) IS NOT NULL)
);
CREATE TRIGGER generation_job_provider_bindings_no_update
BEFORE UPDATE ON generation_job_provider_bindings
BEGIN SELECT RAISE(ABORT,'generation job provider binding is immutable'); END;
CREATE TRIGGER generation_job_provider_bindings_no_delete
BEFORE DELETE ON generation_job_provider_bindings
BEGIN SELECT RAISE(ABORT,'generation job provider binding is immutable'); END;
CREATE TABLE generation_provider_attempt_classifications (
    provider_attempt_id INTEGER PRIMARY KEY REFERENCES generation_provider_attempts(id) ON DELETE RESTRICT,
    provider_name TEXT NOT NULL CHECK(provider_name='codex_cli'),
    safe_code TEXT NOT NULL CHECK(safe_code IN (
        'codex_auth_unavailable','codex_runner_config','codex_timeout',
        'codex_input_limit','codex_output_limit','codex_busy','codex_nonzero',
        'codex_supervisor','codex_unknown_exit','codex_outer_timeout',
        'codex_invalid_draft','codex_runner_attestation'
    )),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) CHECK(aware_epoch_us(created_at) IS NOT NULL)
);

CREATE TABLE generation_provider_controls (
    provider_name TEXT PRIMARY KEY CHECK(provider_name='codex_cli'),
    paused_at TEXT CHECK(paused_at IS NULL OR aware_epoch_us(paused_at) IS NOT NULL),
    pause_reason_code TEXT CHECK(pause_reason_code IS NULL OR pause_reason_code IN (
        'codex_auth_unavailable','codex_runner_config','codex_supervisor',
        'codex_unknown_exit','codex_outer_timeout','codex_runner_attestation',
        'operator_security_hold','maintenance'
    )),
    resumed_at TEXT CHECK(resumed_at IS NULL OR aware_epoch_us(resumed_at) IS NOT NULL),
    control_version INTEGER NOT NULL DEFAULT 1 CHECK(control_version>0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) CHECK(aware_epoch_us(updated_at) IS NOT NULL),
    CHECK(
        (paused_at IS NULL AND pause_reason_code IS NULL)
        OR (paused_at IS NOT NULL AND pause_reason_code IS NOT NULL AND resumed_at IS NULL)
    )
);

CREATE TABLE generation_job_retry_state (
    generation_job_id INTEGER PRIMARY KEY REFERENCES generation_jobs(id) ON DELETE RESTRICT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0 CHECK(consecutive_failures>=0),
    held_at TEXT CHECK(held_at IS NULL OR aware_epoch_us(held_at) IS NOT NULL),
    hold_reason_code TEXT CHECK(hold_reason_code IS NULL OR hold_reason_code IN (
        'codex_busy','codex_timeout','codex_nonzero','codex_input_limit',
        'codex_output_limit','codex_invalid_draft','operator_review','poison_output'
    )),
    blocked_by_control_version INTEGER CHECK(blocked_by_control_version IS NULL OR blocked_by_control_version>0),
    blocked_by_safe_code TEXT CHECK(blocked_by_safe_code IS NULL OR blocked_by_safe_code IN (
        'codex_auth_unavailable','codex_runner_config','codex_supervisor',
        'codex_unknown_exit','codex_outer_timeout','codex_runner_attestation'
    )),
    retry_version INTEGER NOT NULL DEFAULT 1 CHECK(retry_version>0),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) CHECK(aware_epoch_us(updated_at) IS NOT NULL),
    CHECK((held_at IS NULL)=(hold_reason_code IS NULL)),
    CHECK((blocked_by_control_version IS NULL)=(blocked_by_safe_code IS NULL)),
    CHECK(held_at IS NULL OR blocked_by_control_version IS NULL)
);

CREATE TABLE generation_provider_control_events (
    id INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL UNIQUE CHECK(
        length(operation_id)=36 AND substr(operation_id,1,4)='cxo_' AND
        substr(operation_id,5) NOT GLOB '*[^0-9a-f]*'
    ),
    provider_name TEXT NOT NULL CHECK(provider_name='codex_cli'),
    action TEXT NOT NULL CHECK(action IN ('pause','resume')),
    reason_code TEXT NOT NULL CHECK(reason_code IN (
        'codex_auth_unavailable','codex_runner_config','codex_supervisor',
        'codex_unknown_exit','codex_outer_timeout','codex_runner_attestation',
        'operator_security_hold','maintenance','auth_restored','config_repaired',
        'attestation_passed','security_reviewed','maintenance_complete'
    )),
    actor_kind TEXT NOT NULL CHECK(actor_kind IN ('system','operator')),
    actor_id INTEGER CHECK(actor_id IS NULL OR actor_id>0),
    occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) CHECK(aware_epoch_us(occurred_at) IS NOT NULL),
    resulting_paused INTEGER NOT NULL CHECK(resulting_paused IN (0,1)),
    previous_control_version INTEGER NOT NULL CHECK(previous_control_version>0),
    resulting_control_version INTEGER NOT NULL CHECK(resulting_control_version=previous_control_version+1),
    control_version INTEGER NOT NULL CHECK(control_version=resulting_control_version)
);

CREATE TABLE generation_job_retry_events (
    id INTEGER PRIMARY KEY,
    generation_job_id INTEGER NOT NULL REFERENCES generation_jobs(id) ON DELETE RESTRICT,
    operation_id TEXT REFERENCES generation_provider_control_events(operation_id) ON DELETE RESTRICT,
    action TEXT NOT NULL CHECK(action IN ('hold','release')),
    reason_code TEXT NOT NULL CHECK(reason_code IN (
        'codex_busy','codex_timeout','codex_nonzero','codex_input_limit',
        'codex_output_limit','codex_invalid_draft','operator_review','poison_output',
        'recovery_succeeded','operator_reviewed','source_packet_reduced',
        'transient_cleared','provider_resumed'
    )),
    actor_kind TEXT NOT NULL CHECK(actor_kind IN ('system','operator')),
    actor_id INTEGER CHECK(actor_id IS NULL OR actor_id>0),
    occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00','now')) CHECK(aware_epoch_us(occurred_at) IS NOT NULL),
    resulting_held INTEGER NOT NULL CHECK(resulting_held IN (0,1)),
    resulting_consecutive_failures INTEGER NOT NULL CHECK(resulting_consecutive_failures>=0),
    previous_retry_version INTEGER NOT NULL CHECK(previous_retry_version>0),
    resulting_retry_version INTEGER NOT NULL CHECK(resulting_retry_version=previous_retry_version+1),
    control_version INTEGER CHECK(control_version IS NULL OR control_version>0),
    CHECK(
        (action='hold' AND resulting_held=1) OR
        (action='release' AND resulting_held=0)
    ),
    CHECK(
        (action='release' AND reason_code='provider_resumed' AND operation_id IS NOT NULL AND control_version IS NOT NULL)
        OR ((action!='release' OR reason_code!='provider_resumed') AND operation_id IS NULL AND control_version IS NULL)
    )
);

CREATE UNIQUE INDEX generation_job_retry_events_operation_job
ON generation_job_retry_events(operation_id,generation_job_id)
WHERE operation_id IS NOT NULL;
CREATE INDEX generation_provider_attempt_classifications_safe_code
ON generation_provider_attempt_classifications(safe_code,provider_attempt_id);
CREATE INDEX generation_job_retry_state_admission
ON generation_job_retry_state(held_at,blocked_by_control_version,generation_job_id);
CREATE INDEX generation_job_retry_events_job_version
ON generation_job_retry_events(generation_job_id,resulting_retry_version);
CREATE INDEX generation_provider_control_events_version
ON generation_provider_control_events(provider_name,resulting_control_version);

INSERT INTO generation_provider_controls(provider_name) VALUES ('codex_cli');

CREATE TRIGGER generation_provider_attempt_classifications_insert
BEFORE INSERT ON generation_provider_attempt_classifications
BEGIN
    SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM generation_provider_attempts
        WHERE id=NEW.provider_attempt_id AND terminal_outcome='failed'
    ) THEN RAISE(ABORT,'Codex classification requires failed provider attempt') END;
END;
CREATE TRIGGER generation_provider_attempt_classifications_no_update
BEFORE UPDATE ON generation_provider_attempt_classifications
BEGIN SELECT RAISE(ABORT,'provider attempt classification is immutable'); END;
CREATE TRIGGER generation_provider_attempt_classifications_no_delete
BEFORE DELETE ON generation_provider_attempt_classifications
BEGIN SELECT RAISE(ABORT,'provider attempt classification is immutable'); END;

CREATE TRIGGER generation_provider_controls_no_delete
BEFORE DELETE ON generation_provider_controls
BEGIN SELECT RAISE(ABORT,'provider control projection cannot be deleted'); END;
CREATE TRIGGER generation_provider_controls_update
BEFORE UPDATE ON generation_provider_controls
BEGIN
    SELECT CASE WHEN NEW.provider_name!=OLD.provider_name
        OR NEW.control_version!=OLD.control_version+1
        OR (OLD.paused_at IS NULL AND NOT (NEW.paused_at IS NOT NULL AND NEW.pause_reason_code IS NOT NULL AND NEW.resumed_at IS NULL))
        OR (OLD.paused_at IS NOT NULL AND NOT (NEW.paused_at IS NULL AND NEW.pause_reason_code IS NULL AND NEW.resumed_at IS NOT NULL))
    THEN RAISE(ABORT,'illegal provider control transition') END;
END;

CREATE TRIGGER generation_job_retry_state_no_delete
BEFORE DELETE ON generation_job_retry_state
BEGIN SELECT RAISE(ABORT,'job retry projection cannot be deleted'); END;
CREATE TRIGGER generation_job_retry_state_update
BEFORE UPDATE ON generation_job_retry_state
BEGIN
    SELECT CASE WHEN NEW.generation_job_id!=OLD.generation_job_id
        OR NEW.retry_version!=OLD.retry_version+1
    THEN RAISE(ABORT,'illegal job retry transition') END;
END;

CREATE TRIGGER generation_provider_control_events_insert
BEFORE INSERT ON generation_provider_control_events
BEGIN
    SELECT CASE WHEN (NEW.actor_kind='system') != (NEW.actor_id IS NULL)
    THEN RAISE(ABORT,'control event actor mismatch') END;
    SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM generation_provider_controls c
        WHERE c.provider_name=NEW.provider_name
          AND c.control_version=NEW.resulting_control_version
          AND ((NEW.action='pause' AND c.paused_at IS NOT NULL AND c.pause_reason_code=NEW.reason_code AND NEW.resulting_paused=1)
            OR (NEW.action='resume' AND c.paused_at IS NULL AND c.resumed_at IS NOT NULL AND NEW.resulting_paused=0))
    ) THEN RAISE(ABORT,'control event projection mismatch') END;
    SELECT CASE WHEN NOT (
        (NEW.action='pause' AND ((NEW.actor_kind='system' AND NEW.reason_code IN ('codex_auth_unavailable','codex_runner_config','codex_supervisor','codex_unknown_exit','codex_outer_timeout','codex_runner_attestation')) OR (NEW.actor_kind='operator' AND NEW.reason_code IN ('operator_security_hold','maintenance'))))
        OR
        (NEW.action='resume' AND NEW.actor_kind='operator' AND EXISTS(
            SELECT 1 FROM generation_provider_control_events p
            WHERE p.provider_name=NEW.provider_name AND p.action='pause'
              AND p.resulting_control_version=NEW.previous_control_version
              AND ((p.reason_code='codex_auth_unavailable' AND NEW.reason_code='auth_restored')
                OR (p.reason_code IN ('codex_runner_config','codex_supervisor','codex_unknown_exit','codex_outer_timeout') AND NEW.reason_code='config_repaired')
                OR (p.reason_code='codex_runner_attestation' AND NEW.reason_code='attestation_passed')
                OR (p.reason_code='operator_security_hold' AND NEW.reason_code='security_reviewed')
                OR (p.reason_code='maintenance' AND NEW.reason_code='maintenance_complete'))
        ))
    ) THEN RAISE(ABORT,'control event action or reason invalid') END;
END;
CREATE TRIGGER generation_provider_control_events_no_update
BEFORE UPDATE ON generation_provider_control_events
BEGIN SELECT RAISE(ABORT,'provider control event is immutable'); END;
CREATE TRIGGER generation_provider_control_events_no_delete
BEFORE DELETE ON generation_provider_control_events
BEGIN SELECT RAISE(ABORT,'provider control event is immutable'); END;

CREATE TRIGGER generation_job_retry_events_insert
BEFORE INSERT ON generation_job_retry_events
BEGIN
    SELECT CASE WHEN (NEW.actor_kind='system') != (NEW.actor_id IS NULL)
    THEN RAISE(ABORT,'retry event actor mismatch') END;
    SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM generation_job_retry_state s
        WHERE s.generation_job_id=NEW.generation_job_id
          AND s.retry_version=NEW.resulting_retry_version
          AND s.consecutive_failures=NEW.resulting_consecutive_failures
          AND ((NEW.resulting_held=1 AND s.held_at IS NOT NULL) OR (NEW.resulting_held=0 AND s.held_at IS NULL))
    ) THEN RAISE(ABORT,'retry event projection mismatch') END;
    SELECT CASE WHEN NOT (
        (NEW.action='hold' AND NEW.reason_code IN (
            'codex_busy','codex_timeout','codex_nonzero','codex_input_limit',
            'codex_output_limit','codex_invalid_draft','operator_review','poison_output'
        ))
        OR
        (NEW.action='release' AND NEW.reason_code IN (
            'recovery_succeeded','operator_reviewed','source_packet_reduced',
            'transient_cleared','provider_resumed'
        ))
    ) THEN RAISE(ABORT,'retry event action or reason invalid') END;
    SELECT CASE WHEN NEW.reason_code='provider_resumed' AND NOT EXISTS(
        SELECT 1 FROM generation_provider_control_events c
        WHERE c.operation_id=NEW.operation_id AND c.provider_name='codex_cli'
          AND c.action='resume' AND c.actor_kind=NEW.actor_kind
          AND c.actor_id IS NEW.actor_id AND c.control_version=NEW.control_version
    ) THEN RAISE(ABORT,'provider resume release linkage invalid') END;
    SELECT CASE WHEN NEW.reason_code='provider_resumed' AND NEW.actor_kind!='operator'
    THEN RAISE(ABORT,'provider resume release requires operator') END;
END;
CREATE TRIGGER generation_job_retry_events_no_update
BEFORE UPDATE ON generation_job_retry_events
BEGIN SELECT RAISE(ABORT,'job retry event is immutable'); END;
CREATE TRIGGER generation_job_retry_events_no_delete
BEFORE DELETE ON generation_job_retry_events
BEGIN SELECT RAISE(ABORT,'job retry event is immutable'); END;

PRAGMA foreign_key_check;

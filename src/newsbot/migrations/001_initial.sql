CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE runs (
    id INTEGER PRIMARY KEY,
    run_key TEXT NOT NULL UNIQUE,
    mode TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'done', 'ready', 'failed')),
    config_hash TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT,
    error_message TEXT
);

CREATE TABLE source_posts (
    id INTEGER PRIMARY KEY,
    channel_id TEXT NOT NULL,
    external_post_id TEXT NOT NULL,
    published_at TEXT,
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_url TEXT,
    UNIQUE (channel_id, external_post_id)
);

CREATE TABLE source_post_versions (
    id INTEGER PRIMARY KEY,
    source_post_id INTEGER NOT NULL REFERENCES source_posts(id) ON DELETE CASCADE,
    version_key TEXT NOT NULL,
    body TEXT NOT NULL,
    media_json TEXT NOT NULL DEFAULT '[]',
    observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    channel_handle TEXT NOT NULL DEFAULT '',
    published_at TEXT,
    edited_at TEXT,
    kind TEXT NOT NULL DEFAULT 'message',
    sponsored INTEGER NOT NULL DEFAULT 0 CHECK (sponsored IN (0, 1)),
    urls_json TEXT NOT NULL DEFAULT '[]',
    conflicts_json TEXT NOT NULL DEFAULT '[]',
    UNIQUE (source_post_id, version_key)
);

CREATE TRIGGER source_post_versions_immutable
BEFORE UPDATE ON source_post_versions
BEGIN
    SELECT RAISE(ABORT, 'source_post_versions are immutable');
END;

CREATE TABLE source_post_observations (
    id INTEGER PRIMARY KEY,
    source_post_id INTEGER NOT NULL REFERENCES source_posts(id) ON DELETE CASCADE,
    source_post_version_id INTEGER NOT NULL REFERENCES source_post_versions(id) ON DELETE RESTRICT,
    observation_key TEXT NOT NULL,
    observed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    channel_handle TEXT NOT NULL DEFAULT '',
    published_at TEXT,
    edited_at TEXT,
    engagement_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (source_post_id, observation_key)
);

CREATE TRIGGER source_post_observations_immutable
BEFORE UPDATE ON source_post_observations
BEGIN
    SELECT RAISE(ABORT, 'source_post_observations are immutable');
END;

CREATE TABLE collection_cursors (
    channel_id TEXT PRIMARY KEY,
    published_at TEXT NOT NULL,
    external_post_id TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE collection_intervals (
    channel_id TEXT PRIMARY KEY,
    floor_at TEXT NOT NULL,
    upper_bound_at TEXT NOT NULL,
    base_message_id INTEGER NOT NULL,
    upper_message_id INTEGER NOT NULL,
    next_message_id INTEGER NOT NULL,
    floor_applies INTEGER NOT NULL CHECK (floor_applies IN (0, 1)),
    next_published_at TEXT,
    next_external_post_id TEXT,
    overlap_next_published_at TEXT,
    overlap_next_external_post_id TEXT,
    overlap_next_message_id INTEGER,
    page_complete INTEGER NOT NULL DEFAULT 0 CHECK (page_complete IN (0, 1)),
    overlap_complete INTEGER NOT NULL DEFAULT 0 CHECK (overlap_complete IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_collection_intervals_open ON collection_intervals(page_complete, overlap_complete);

CREATE TABLE candidate_evaluations (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    source_post_version_id INTEGER NOT NULL REFERENCES source_post_versions(id) ON DELETE RESTRICT,
    source_set_key TEXT NOT NULL DEFAULT '',
    evaluator_version TEXT NOT NULL,
    score TEXT NOT NULL CHECK (score GLOB '[0-9]*.[0-9][0-9][0-9][0-9][0-9][0-9]'),
    rationale_json TEXT NOT NULL DEFAULT '{}',
    evaluated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, source_set_key, evaluator_version)
);

CREATE TABLE candidates (
    id INTEGER PRIMARY KEY,
    evaluation_id INTEGER NOT NULL UNIQUE REFERENCES candidate_evaluations(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('pending_selection', 'selected_generation_pending', 'pending_review', 'deferred', 'rejected', 'approved', 'superseded')),
    deferred_stage TEXT CHECK (deferred_stage IN ('selection', 'review')),
    deferred_until TEXT,
    rank INTEGER CHECK (rank IS NULL OR rank > 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (status != 'deferred' OR (deferred_stage IS NOT NULL AND deferred_until IS NOT NULL))
);

CREATE TABLE candidate_sources (
    candidate_id INTEGER NOT NULL REFERENCES candidates(id) ON DELETE CASCADE,
    source_post_version_id INTEGER NOT NULL REFERENCES source_post_versions(id) ON DELETE RESTRICT,
    PRIMARY KEY (candidate_id, source_post_version_id)
);

CREATE TABLE digests (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    digest_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'selected', 'approved', 'superseded')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    title TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, digest_key)
);

CREATE TABLE selections (
    id INTEGER PRIMARY KEY,
    digest_id INTEGER NOT NULL REFERENCES digests(id) ON DELETE CASCADE,
    candidate_id INTEGER NOT NULL REFERENCES candidates(id) ON DELETE RESTRICT,
    position INTEGER NOT NULL CHECK (position > 0),
    selected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (digest_id, position),
    UNIQUE (digest_id, candidate_id)
);

CREATE TABLE generation_jobs (
    id INTEGER PRIMARY KEY,
    selection_id INTEGER NOT NULL REFERENCES selections(id) ON DELETE CASCADE,
    job_kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'failed_recoverable', 'succeeded', 'superseded')),
    requested_page_count INTEGER CHECK (requested_page_count BETWEEN 1 AND 8),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_token TEXT,
    lease_expires_at TEXT,
    retry_at TEXT,
    requested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TEXT,
    finished_at TEXT,
    error_message TEXT,
    UNIQUE (selection_id, job_kind)
);

CREATE TABLE generation_provider_attempts (
    id INTEGER PRIMARY KEY,
    generation_job_id INTEGER NOT NULL REFERENCES generation_jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    terminal_outcome TEXT CHECK (terminal_outcome IN ('succeeded', 'failed', 'abandoned')),
    error_message TEXT,
    CHECK (
        (terminal_outcome IS NULL AND finished_at IS NULL AND error_message IS NULL)
        OR (terminal_outcome = 'succeeded' AND finished_at IS NOT NULL AND error_message IS NULL)
        OR (terminal_outcome IN ('failed', 'abandoned') AND finished_at IS NOT NULL)
    ),
    UNIQUE (generation_job_id, attempt)
);

CREATE TRIGGER generation_provider_attempts_finalize_open
BEFORE UPDATE ON generation_provider_attempts
WHEN OLD.terminal_outcome IS NOT NULL
    OR NEW.generation_job_id != OLD.generation_job_id
    OR NEW.attempt != OLD.attempt
    OR NEW.started_at != OLD.started_at
    OR NEW.terminal_outcome IS NULL
    OR NEW.finished_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'generation provider attempts may only be finalized once');
END;

CREATE UNIQUE INDEX idx_generation_provider_attempts_one_open
ON generation_provider_attempts(generation_job_id)
WHERE terminal_outcome IS NULL;

CREATE TABLE pipeline_events (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    selection_id INTEGER NOT NULL REFERENCES selections(id) ON DELETE RESTRICT,
    generation_job_id INTEGER NOT NULL REFERENCES generation_jobs(id) ON DELETE RESTRICT,
    candidate_id INTEGER NOT NULL REFERENCES candidates(id) ON DELETE RESTRICT,
    provider_attempt_id INTEGER NOT NULL UNIQUE REFERENCES generation_provider_attempts(id) ON DELETE RESTRICT,
    event_kind TEXT NOT NULL CHECK (event_kind IN ('provider_call')),
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pipeline_events_run_kind ON pipeline_events(run_id, event_kind);

CREATE TABLE generations (
    id INTEGER PRIMARY KEY,
    generation_job_id INTEGER NOT NULL REFERENCES generation_jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    status TEXT NOT NULL CHECK (status IN ('current', 'superseded')),
    content_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (generation_job_id, attempt)
);
CREATE TRIGGER generations_content_immutable
BEFORE UPDATE OF content_json ON generations
BEGIN
    SELECT RAISE(ABORT, 'generation content_json is immutable');
END;

CREATE TABLE generation_sources (
    generation_job_id INTEGER NOT NULL REFERENCES generation_jobs(id) ON DELETE CASCADE,
    generation_id INTEGER REFERENCES generations(id) ON DELETE CASCADE,
    source_post_version_id INTEGER NOT NULL REFERENCES source_post_versions(id) ON DELETE RESTRICT,
    PRIMARY KEY (generation_job_id, generation_id, source_post_version_id)
);

CREATE TABLE decision_events (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    digest_id INTEGER REFERENCES digests(id) ON DELETE SET NULL,
    selection_id INTEGER REFERENCES selections(id) ON DELETE SET NULL,
    event_key TEXT NOT NULL,
    decision TEXT NOT NULL,
    actor TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, event_key)
);

CREATE TABLE callback_tokens (
    id INTEGER PRIMARY KEY,
    token TEXT NOT NULL UNIQUE CHECK (
        length(token) = 64 AND token NOT GLOB '*[^0-9a-f]*'
    ),
    decision_event_id INTEGER REFERENCES decision_events(id) ON DELETE SET NULL,
    action TEXT NOT NULL CHECK (action IN ('make', 'defer_6h', 'defer_24h', 'defer_72h', 'reject', 'refresh', 'approve_handoff', 'regenerate', 'page_increment', 'page_decrement')),
    payload_json TEXT NOT NULL DEFAULT '{}',
    expires_at TEXT,
    consumed_at TEXT,
    revoked_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE export_outbox (
    id INTEGER PRIMARY KEY,
    digest_id INTEGER NOT NULL REFERENCES digests(id) ON DELETE CASCADE,
    generation_id INTEGER NOT NULL REFERENCES generations(id) ON DELETE CASCADE,
    approval_event_id INTEGER NOT NULL REFERENCES decision_events(id) ON DELETE RESTRICT,
    export_kind TEXT NOT NULL,
    export_id TEXT NOT NULL,
    canonical_bytes BLOB NOT NULL,
    sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'materializing', 'ready', 'corrupt')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered_at TEXT,
    UNIQUE (digest_id, export_kind),
    UNIQUE (generation_id, approval_event_id, export_kind)
);

CREATE INDEX idx_source_post_versions_source ON source_post_versions(source_post_id);
CREATE INDEX idx_source_post_observations_source_version ON source_post_observations(source_post_id, source_post_version_id);
CREATE INDEX idx_candidate_evaluations_run ON candidate_evaluations(run_id);
CREATE INDEX idx_candidates_status_rank ON candidates(status, rank);
CREATE INDEX idx_digests_run ON digests(run_id);
CREATE INDEX idx_candidate_sources_version ON candidate_sources(source_post_version_id);
CREATE INDEX idx_generation_sources_job ON generation_sources(generation_job_id);
CREATE INDEX idx_generation_jobs_status ON generation_jobs(status);
CREATE INDEX idx_callback_tokens_expiry ON callback_tokens(expires_at);
CREATE INDEX idx_callback_tokens_live ON callback_tokens(revoked_at, expires_at) WHERE consumed_at IS NULL AND revoked_at IS NULL;

CREATE TRIGGER callback_tokens_revoke_candidate_state
AFTER UPDATE OF status ON candidates
WHEN NEW.status != OLD.status
    AND NEW.status IN ('selected_generation_pending', 'deferred', 'rejected', 'approved', 'superseded')
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER) = NEW.id;
END;

CREATE TRIGGER callback_tokens_revoke_digest_state
AFTER UPDATE OF status, revision ON digests
WHEN NEW.status != OLD.status OR NEW.revision != OLD.revision
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.digest_id') AS INTEGER) = NEW.id;
END;

CREATE TRIGGER callback_tokens_revoke_superseded_generation
AFTER UPDATE OF status ON generations
WHEN OLD.status = 'current' AND NEW.status = 'superseded'
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.generation_id') AS INTEGER) = OLD.id;
END;

CREATE TRIGGER callback_tokens_revoke_superseded_source
AFTER INSERT ON source_post_versions
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER) IN (
            SELECT candidate_sources.candidate_id
            FROM candidate_sources
            JOIN source_post_versions bound ON bound.id=candidate_sources.source_post_version_id
            WHERE bound.source_post_id=NEW.source_post_id AND bound.id < NEW.id
        );
END;
CREATE INDEX idx_export_outbox_status ON export_outbox(status, created_at);

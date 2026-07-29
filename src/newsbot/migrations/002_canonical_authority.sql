-- The runner disables foreign keys for this table rebuild and verifies them before return.
CREATE TABLE candidate_evaluations_canonical (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    source_post_version_id INTEGER NOT NULL REFERENCES source_post_versions(id) ON DELETE RESTRICT,
    source_post_observation_id INTEGER REFERENCES source_post_observations(id) ON DELETE RESTRICT,
    source_set_key TEXT NOT NULL DEFAULT '',
    observation_set_key TEXT NOT NULL DEFAULT '',
    evaluator_version TEXT NOT NULL,
    score TEXT NOT NULL CHECK (
        typeof(score) = 'text'
        AND score GLOB '[0-9]*.[0-9][0-9][0-9][0-9][0-9][0-9]'
    ),
    rationale_json TEXT NOT NULL DEFAULT '{}',
    evaluated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, source_set_key, evaluator_version)
);

INSERT INTO candidate_evaluations_canonical(
    id, run_id, source_post_version_id, source_post_observation_id,
    source_set_key, observation_set_key, evaluator_version, score, rationale_json, evaluated_at
)
SELECT evaluation.id, evaluation.run_id, evaluation.source_post_version_id,
       (SELECT observation.id FROM source_post_observations observation
        WHERE observation.source_post_version_id=evaluation.source_post_version_id
        ORDER BY observation.id DESC LIMIT 1),
       evaluation.source_set_key, '', evaluation.evaluator_version,
       printf('%.6f', CAST(evaluation.score AS REAL)), evaluation.rationale_json, evaluation.evaluated_at
FROM candidate_evaluations evaluation;

DROP TABLE candidate_evaluations;
ALTER TABLE candidate_evaluations_canonical RENAME TO candidate_evaluations;

CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_observation
ON candidate_evaluations(source_post_observation_id);
CREATE INDEX IF NOT EXISTS idx_candidate_evaluations_run ON candidate_evaluations(run_id);
CREATE INDEX IF NOT EXISTS idx_callback_tokens_live
ON callback_tokens(revoked_at, expires_at) WHERE consumed_at IS NULL AND revoked_at IS NULL;

CREATE TRIGGER IF NOT EXISTS callback_tokens_revoke_candidate_state
AFTER UPDATE OF status ON candidates
WHEN NEW.status != OLD.status
    AND NEW.status IN ('selected_generation_pending', 'deferred', 'rejected', 'approved', 'superseded')
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER) = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS callback_tokens_revoke_digest_state
AFTER UPDATE OF status, revision ON digests
WHEN NEW.status != OLD.status OR NEW.revision != OLD.revision
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.digest_id') AS INTEGER) = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS callback_tokens_revoke_superseded_generation
AFTER UPDATE OF status ON generations
WHEN OLD.status = 'current' AND NEW.status = 'superseded'
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
        AND CAST(json_extract(payload_json, '$.generation_id') AS INTEGER) = OLD.id;
END;
CREATE TRIGGER IF NOT EXISTS callback_tokens_revoke_superseded_source
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
CREATE TRIGGER IF NOT EXISTS callback_tokens_revoke_superseded_source_observation
AFTER INSERT ON source_post_observations
BEGIN
    UPDATE callback_tokens SET revoked_at=CURRENT_TIMESTAMP
    WHERE consumed_at IS NULL AND revoked_at IS NULL
      AND CAST(json_extract(payload_json, '$.candidate_id') AS INTEGER) IN (
          SELECT candidate_sources.candidate_id
          FROM candidate_sources
          JOIN source_post_versions bound ON bound.id=candidate_sources.source_post_version_id
          WHERE bound.source_post_id=NEW.source_post_id
            AND bound.id != NEW.source_post_version_id
      );
END;

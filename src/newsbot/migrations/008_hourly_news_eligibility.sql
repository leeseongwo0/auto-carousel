-- Hourly news eligibility authority is additive. This migration deliberately creates no work.
CREATE TABLE automation_release_config_bindings (
    id INTEGER PRIMARY KEY,
    activation_id INTEGER NOT NULL UNIQUE REFERENCES automation_release_activations(id) ON DELETE RESTRICT,
    config_digest TEXT NOT NULL CHECK(length(config_digest)=64),
    news_policy_version TEXT NOT NULL,
    canonical_policy_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE news_policy_evaluations (
    id INTEGER PRIMARY KEY,
    candidate_evaluation_id INTEGER NOT NULL UNIQUE REFERENCES candidate_evaluations(id) ON DELETE RESTRICT,
    config_binding_id INTEGER NOT NULL REFERENCES automation_release_config_bindings(id) ON DELETE RESTRICT,
    outcome TEXT NOT NULL CHECK(outcome IN ('definite_news','trusted_analysis','ambiguous','non_news')),
    reason TEXT NOT NULL,
    rationale_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE ambiguous_digest_windows (
    id INTEGER PRIMARY KEY,
    scheduled_local_date TEXT NOT NULL UNIQUE,
    config_binding_id INTEGER NOT NULL REFERENCES automation_release_config_bindings(id) ON DELETE RESTRICT,
    opens_at TEXT NOT NULL,
    closes_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('collecting','queued','empty','skipped')),
    created_at TEXT NOT NULL,
    CHECK(aware_epoch_us(opens_at) IS NOT NULL),
    CHECK(aware_epoch_us(closes_at) IS NOT NULL),
    CHECK(date(scheduled_local_date) IS NOT NULL AND date(scheduled_local_date) = scheduled_local_date),
    CHECK(aware_epoch_us(closes_at) - aware_epoch_us(opens_at) = 3600000000),
    CHECK(scheduled_local_date = date(opens_at, '+9 hours')),
    CHECK(scheduled_local_date = date(closes_at, '+9 hours')),
    CHECK(aware_epoch_us(opens_at) = aware_epoch_us(scheduled_local_date || 'T12:00:00+09:00')),
    CHECK(aware_epoch_us(closes_at) = aware_epoch_us(scheduled_local_date || 'T13:00:00+09:00'))
);
CREATE TABLE ambiguous_digest_items (
    id INTEGER PRIMARY KEY,
    window_id INTEGER NOT NULL REFERENCES ambiguous_digest_windows(id) ON DELETE RESTRICT,
    news_policy_evaluation_id INTEGER NOT NULL REFERENCES news_policy_evaluations(id) ON DELETE RESTRICT,
    source_post_version_id INTEGER NOT NULL REFERENCES source_post_versions(id) ON DELETE RESTRICT,
    normalized_title TEXT NOT NULL,
    ordering_timestamp TEXT NOT NULL,
    story_key TEXT NOT NULL,
    content_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    UNIQUE(window_id, story_key)
);
CREATE TRIGGER automation_release_config_bindings_immutable BEFORE UPDATE ON automation_release_config_bindings BEGIN SELECT RAISE(ABORT,'release config bindings are immutable'); END;
CREATE TRIGGER automation_release_config_bindings_no_delete BEFORE DELETE ON automation_release_config_bindings BEGIN SELECT RAISE(ABORT,'release config bindings cannot be deleted'); END;
CREATE TRIGGER news_policy_evaluations_immutable BEFORE UPDATE ON news_policy_evaluations BEGIN SELECT RAISE(ABORT,'news policy evaluations are immutable'); END;
CREATE TRIGGER news_policy_evaluations_no_delete BEFORE DELETE ON news_policy_evaluations BEGIN SELECT RAISE(ABORT,'news policy evaluations cannot be deleted'); END;
CREATE TRIGGER ambiguous_digest_windows_identity_immutable BEFORE UPDATE OF id,scheduled_local_date,config_binding_id,opens_at,closes_at,created_at ON ambiguous_digest_windows WHEN NEW.id IS NOT OLD.id OR NEW.scheduled_local_date IS NOT OLD.scheduled_local_date OR NEW.config_binding_id IS NOT OLD.config_binding_id OR NEW.opens_at IS NOT OLD.opens_at OR NEW.closes_at IS NOT OLD.closes_at OR NEW.created_at IS NOT OLD.created_at BEGIN SELECT RAISE(ABORT,'ambiguous digest window identity is immutable'); END;
CREATE TRIGGER ambiguous_digest_windows_transitions BEFORE UPDATE OF state ON ambiguous_digest_windows WHEN NOT ((OLD.state='collecting' AND NEW.state IN ('queued','empty','skipped')) OR OLD.state=NEW.state) BEGIN SELECT RAISE(ABORT,'invalid ambiguous digest window transition'); END;
CREATE TRIGGER ambiguous_digest_windows_no_delete BEFORE DELETE ON ambiguous_digest_windows BEGIN SELECT RAISE(ABORT,'ambiguous digest windows cannot be deleted'); END;
CREATE TRIGGER ambiguous_digest_items_immutable BEFORE UPDATE ON ambiguous_digest_items BEGIN SELECT RAISE(ABORT,'ambiguous digest items are immutable'); END;
CREATE TRIGGER ambiguous_digest_items_no_delete BEFORE DELETE ON ambiguous_digest_items BEGIN SELECT RAISE(ABORT,'ambiguous digest items cannot be deleted'); END;

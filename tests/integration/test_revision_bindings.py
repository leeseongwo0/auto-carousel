import sqlite3
from datetime import UTC, datetime

from newsbot.approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from newsbot.candidates import CandidateApprovalService
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage, has_newer_material_source


def _candidate(storage: Storage) -> tuple[int, tuple[int, int]]:
    with storage.transaction() as connection:
        connection.execute("INSERT INTO runs(run_key, mode, status) VALUES ('revision', 'fixture', 'done')")
        run_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        versions = []
        for external_id in ("one", "two"):
            connection.execute(
                "INSERT INTO source_posts(channel_id, external_post_id) VALUES ('source', ?)", (external_id,)
            )
            post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', ?)",
                (post_id, external_id),
            )
            versions.append(int(connection.execute("SELECT last_insert_rowid()").fetchone()[0]))
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, source_set_key, evaluator_version, score) "
            "VALUES (?, ?, 'both-v1', 'policy', '1.000000')",
            (run_id, versions[0]),
        )
        evaluation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'pending_selection', 1)", (evaluation_id,)
        )
        candidate_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.executemany(
            "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
            ((candidate_id, version_id) for version_id in versions),
        )
    return candidate_id, tuple(versions)


def test_grouped_candidate_callback_binds_every_source_and_rejects_a_stale_edit(tmp_path) -> None:
    storage = Storage.open(":memory:")
    candidate_id, versions = _candidate(storage)
    service = CandidateApprovalService(
        storage, chat_id=10, authorized_user_ids={20}, now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now
    )
    digest = service.create_digest(1, actor_id=20)
    assert digest.candidates[0]["source_version_ids"] == versions
    make = next(button for button in digest.buttons[candidate_id] if button.action.value == "make")

    with storage.transaction() as connection:
        post_id = connection.execute(
            "SELECT source_post_id FROM source_post_versions WHERE id=?", (versions[1],)
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v2', 'edited')", (post_id,)
        )
        replacement = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute("UPDATE candidates SET status='superseded', revision=revision+1 WHERE id=?", (candidate_id,))
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, source_set_key, evaluator_version, score) VALUES (1, ?, 'both-v2', 'policy', '1.000000')",
            (versions[0],),
        )
        evaluation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'pending_selection', 1)", (evaluation_id,)
        )
        fresh_candidate = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.executemany(
            "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
            ((fresh_candidate, source_id) for source_id in (versions[0], replacement)),
        )

    assert ScriptedApprovalAdapter(service).apply(ScriptedAction(make.token, 10, 20)).status == "stale"
    assert (
        storage.fetch_one("SELECT status FROM candidates WHERE id=?", (fresh_candidate,))["status"]
        == "pending_selection"
    )


def test_terminal_revision_supersedes_jobs_and_preserves_old_generation() -> None:
    storage = Storage.open(":memory:")
    candidate_id, versions = _candidate(storage)
    with storage.transaction() as connection:
        connection.execute("INSERT INTO digests(run_id, digest_key, status) VALUES (1, 'd', 'selected')")
        digest_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest_id, candidate_id)
        )
        selection_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status) VALUES (?, 'initial', 'succeeded')",
            (selection_id,),
        )
        job_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generations(generation_job_id, attempt, status, content_json) VALUES (?, 1, 'current', '{}')",
            (job_id,),
        )
        generation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.executemany(
            "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) VALUES (?, ?, ?)",
            ((job_id, generation_id, source_id) for source_id in versions),
        )
        connection.execute("UPDATE candidates SET status='approved' WHERE id=?", (candidate_id,))

    # The pipeline's invalidation is deliberately source-version based: engagement-only snapshots create no version.
    pipeline = NewsPipeline(storage, object(), lambda: None, FixtureClock())
    pipeline._invalidate_revised_candidates(1, {("source", "two"): versions[1]})
    assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "approved"
    assert storage.fetch_one("SELECT status FROM generations WHERE id=?", (generation_id,))["status"] == "current"


def test_material_edit_invalidates_nonterminal_candidate_from_an_older_run() -> None:
    storage = Storage.open(":memory:")
    candidate_id, versions = _candidate(storage)
    service = CandidateApprovalService(
        storage, chat_id=10, authorized_user_ids={20}, now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now
    )
    digest = service.create_digest(1, actor_id=20)
    make = next(button for button in digest.buttons[candidate_id] if button.action.value == "make")

    with storage.transaction() as connection:
        post_id = int(
            connection.execute("SELECT source_post_id FROM source_post_versions WHERE id=?", (versions[1],)).fetchone()[
                0
            ]
        )
        connection.execute(
            "INSERT INTO source_post_observations(source_post_id, source_post_version_id, observation_key) "
            "VALUES (?, ?, 'cross-run-v1')",
            (post_id, versions[1]),
        )
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'cross-run-v2', 'edited')",
            (post_id,),
        )
        replacement = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_observations(source_post_id, source_post_version_id, observation_key) "
            "VALUES (?, ?, 'cross-run-v2')",
            (post_id, replacement),
        )
        replacement_observation = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute("INSERT INTO runs(run_key, mode, status) VALUES ('revision-2', 'fixture', 'done')")
        second_run_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])

    pipeline = NewsPipeline(storage, object(), lambda: None, FixtureClock())
    pipeline._invalidate_revised_candidates(second_run_id, {("source", "two"): replacement_observation})

    assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "superseded"
    assert storage.fetch_one("SELECT status FROM digests WHERE id=?", (digest.id,))["status"] == "superseded"
    assert ScriptedApprovalAdapter(service).apply(ScriptedAction(make.token, 10, 20)).status == "stale"


def test_a_to_b_to_a_uses_latest_observation_for_callback_currentness() -> None:
    storage = Storage.open(":memory:")
    candidate_id, versions = _candidate(storage)
    service = CandidateApprovalService(
        storage, chat_id=10, authorized_user_ids={20}, now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now
    )

    with storage.transaction() as connection:
        post_id = connection.execute(
            "SELECT source_post_id FROM source_post_versions WHERE id=?", (versions[0],)
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source_post_observations(source_post_id, source_post_version_id, observation_key) "
            "VALUES (?, ?, 'a')",
            (post_id, versions[0]),
        )
    digest_a = service.create_digest(1, actor_id=20)
    make_a = next(button for button in digest_a.buttons[candidate_id] if button.action.value == "make")

    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'b', 'B')", (post_id,)
        )
        version_b = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_observations(source_post_id, source_post_version_id, observation_key) "
            "VALUES (?, ?, 'b')",
            (post_id, version_b),
        )

    assert ScriptedApprovalAdapter(service).apply(ScriptedAction(make_a.token, 10, 20)).status == "stale"

    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO source_post_observations(source_post_id, source_post_version_id, observation_key) "
            "VALUES (?, ?, 'a-again')",
            (post_id, versions[0]),
        )
    with storage.transaction() as connection:
        assert not has_newer_material_source(connection, (versions[0],))


def test_legacy_score_schema_upgrades_without_losing_evaluation(tmp_path) -> None:
    database = tmp_path / "legacy.sqlite"
    connection = sqlite3.connect(database)
    connection.executescript(
        """
        CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT);
        INSERT INTO schema_migrations VALUES ('001_initial.sql', '2026-01-01T00:00:00+00:00');
        CREATE TABLE runs (id INTEGER PRIMARY KEY);
        CREATE TABLE source_posts (id INTEGER PRIMARY KEY);
        CREATE TABLE source_post_versions (id INTEGER PRIMARY KEY, source_post_id INTEGER, version_key TEXT, body TEXT);
        CREATE TABLE source_post_observations (
            id INTEGER PRIMARY KEY, source_post_id INTEGER, source_post_version_id INTEGER, observation_key TEXT
        );
        CREATE TABLE candidate_sources (candidate_id INTEGER, source_post_version_id INTEGER);
        CREATE TABLE candidates (id INTEGER PRIMARY KEY, status TEXT, revision INTEGER);
        CREATE TABLE digests (id INTEGER PRIMARY KEY, status TEXT, revision INTEGER);
        CREATE TABLE generations (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE callback_tokens (id INTEGER PRIMARY KEY, token TEXT, consumed_at TEXT, expires_at TEXT, payload_json TEXT);
        CREATE TABLE candidate_evaluations (
            id INTEGER PRIMARY KEY, run_id INTEGER, source_post_version_id INTEGER,
            source_set_key TEXT NOT NULL DEFAULT '', evaluator_version TEXT NOT NULL,
            score REAL NOT NULL, rationale_json TEXT NOT NULL DEFAULT '{}', evaluated_at TEXT NOT NULL,
            UNIQUE(run_id, source_set_key, evaluator_version)
        );
        INSERT INTO runs VALUES (1);
        INSERT INTO source_posts VALUES (1);
        INSERT INTO source_post_versions VALUES (1, 1, 'a', 'A');
        INSERT INTO candidate_evaluations VALUES (1, 1, 1, '', 'policy', 1.5, '{}', '2026-01-01T00:00:00+00:00');
        """
    )
    connection.commit()
    connection.close()

    storage = Storage.open(database)
    row = storage.fetch_one("SELECT score, typeof(score), source_post_observation_id FROM candidate_evaluations")
    assert tuple(row) == ("1.500000", "text", None)
    assert storage.fetch_one("SELECT 1 FROM pragma_table_info('callback_tokens') WHERE name='revoked_at'") is not None

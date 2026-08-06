from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from newsbot.storage import ManualProfileConflictError, Storage

DIGEST = "a" * 64
OTHER_DIGEST = "b" * 64


def test_manual_workflow_migration_is_no_work_and_binding_is_exact(tmp_path) -> None:
    database = tmp_path / "manual.sqlite3"
    with Storage.open(database) as storage:
        assert storage.fetch_one("SELECT 1 FROM manual_profile_bindings") is None
        assert storage.fetch_one("SELECT 1 FROM manual_local_decisions") is None
        assert storage.fetch_one("SELECT 1 FROM manual_local_export_outbox") is None

        binding = storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        assert binding.profile_digest == DIGEST
        assert storage.bind_manual_profile("newsbot.behavior.v1", DIGEST) == binding
        with pytest.raises(ManualProfileConflictError, match="does not match"):
            storage.bind_manual_profile("newsbot.behavior.v1", OTHER_DIGEST)

        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="automation authority conflicts"),
        ):
            connection.execute(
                "INSERT INTO automation_cutovers("
                "id,proposal_id,audience_binding_id,target_binding_id,release_digest,activated_at,"
                "baseline_candidate_id,baseline_generation_job_id,baseline_generation_id,"
                "baseline_decision_event_id,baseline_handoff_id,approval_offset"
                ") VALUES(1,'unbound',1,1,?,'2026-01-01T00:00:00+00:00',0,0,0,0,0,0)",
                (DIGEST,),
            )


def test_manual_binding_refuses_existing_automation_authority(tmp_path) -> None:
    database = tmp_path / "automation.sqlite3"
    now = datetime(2026, 1, 1, tzinfo=UTC)
    expires = now + timedelta(minutes=10)
    with Storage.open(database) as storage:
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO sheet_target_bindings("
                "target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint,created_at"
                ") VALUES(?,'workplace-template-v1',0,'workplace',?,?)",
                (DIGEST, "b" * 64, now.isoformat()),
            )
            target_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO telegram_audience_bindings("
                "bot_id_digest,token_hmac,audience_hmac,version,created_at"
                ") VALUES(?,?,?,?,?)",
                ("c" * 64, "d" * 64, "e" * 64, 1, now.isoformat()),
            )
            audience_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO automation_cutover_proposals("
                "id,created_at,expires_at,config_digest,frontiers_digest,cursor_digest,intervals_digest,"
                "candidate_max_id,generation_job_max_id,generation_max_id,decision_event_max_id,handoff_max_id,"
                "callback_offset,nonterminal_job_count,outbox_count,ready_target_id,ready_target_fingerprint,"
                "application_release_digest,audience_binding_digest,proposal_sha256"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "manual-migration-test",
                    now.isoformat(),
                    expires.isoformat(),
                    DIGEST,
                    "f" * 64,
                    "1" * 64,
                    "2" * 64,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    target_id,
                    "3" * 64,
                    "4" * 64,
                    "5" * 64,
                    "6" * 64,
                ),
            )
            connection.execute(
                "INSERT INTO automation_cutovers("
                "id,proposal_id,audience_binding_id,target_binding_id,release_digest,activated_at,"
                "baseline_candidate_id,baseline_generation_job_id,baseline_generation_id,"
                "baseline_decision_event_id,baseline_handoff_id,approval_offset"
                ") VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "manual-migration-test",
                    audience_id,
                    target_id,
                    "7" * 64,
                    now.isoformat(),
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                ),
            )
        with pytest.raises(ManualProfileConflictError, match="conflicts"):
            storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)


def test_legacy_stream_authority_refuses_manual_binding_after_reopen(tmp_path) -> None:
    database = tmp_path / "legacy-stream.sqlite3"
    with Storage.open(database) as storage, storage.transaction() as connection:
        connection.execute(
            "INSERT INTO automation_stream_leases(stream,owner_hash,fence,expires_at,acquired_at) "
            "VALUES('collect','legacy-owner',1,'2026-01-01T00:10:00+00:00','2026-01-01T00:00:00+00:00')"
        )

    with Storage.open(database) as storage:
        with pytest.raises(ManualProfileConflictError, match="conflicts"):
            storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        assert storage.fetch_one("SELECT 1 FROM manual_profile_bindings") is None


@pytest.mark.parametrize(
    ("table", "insert_sql"),
    (
        (
            "callback_tokens",
            "INSERT INTO callback_tokens(token,action) VALUES('" + DIGEST + "','make')",
        ),
        (
            "decision_events",
            "INSERT INTO decision_events(run_id,event_key,decision,actor) VALUES({run_id},'automation-root','approve','automation')",
        ),
        (
            "generation_job_provider_bindings",
            "INSERT INTO generation_job_provider_bindings(generation_job_id,provider_name) VALUES({job_id},'codex_cli')",
        ),
        (
            "export_outbox",
            "INSERT INTO export_outbox(digest_id,generation_id,approval_event_id,export_kind,export_id,canonical_bytes,sha256,payload_json,status) "
            "VALUES(1,1,1,'json','automation-root',X'01','" + DIGEST + "','{}','pending')",
        ),
        (
            "sheet_handoffs",
            "INSERT INTO sheet_handoffs(id,generation_id,approval_event_id,target_binding_id,export_id,canonical_bytes,canonical_sha256,approved_at,category,initial_upload_status,marker_value,status) "
            "VALUES(1,1,1,1,'exp_"
            + "a" * 32
            + "',X'01','"
            + DIGEST
            + "','2026-01-01T00:00:00+00:00','AI','X','v1:exp_"
            + "a" * 32
            + ":"
            + DIGEST
            + "','pending')",
        ),
        (
            "sheet_remote_operations",
            "INSERT INTO sheet_remote_operations(id,target_binding_id,operation_kind,ordinal,status,certainty,current_acquired_event_id,current_lease_id,acquired_at) "
            "VALUES(1,1,'bootstrap',1,'acquired','not_dispatched',1,1,'2026-01-01T00:00:00+00:00')",
        ),
        (
            "sheet_operation_leases",
            "INSERT INTO sheet_operation_leases(id,operation_id,fence_version,owner_token_hash,lease_mode,acquired_at,expires_at,status) "
            "VALUES(1,1,1,'"
            + sha256(b"lease-owner").hexdigest()
            + "','mutate','2026-01-01T00:00:00+00:00','2026-01-01T00:10:00+00:00','active')",
        ),
        (
            "sheet_target_bindings",
            "INSERT INTO sheet_target_bindings(target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint) "
            "VALUES('" + DIGEST + "','workplace-template-v1',0,'workplace','b" + "b" * 63 + "')",
        ),
        (
            "telegram_audience_bindings",
            "INSERT INTO telegram_audience_bindings(bot_id_digest,token_hmac,audience_hmac,version) "
            "VALUES('c" + "c" * 63 + "','d" + "d" * 63 + "','e" + "e" * 63 + "',1)",
        ),
        ("telegram_update_cursors", "INSERT INTO telegram_update_cursors(stream,next_offset) VALUES('approval',0)"),
        (
            "generation_job_retry_state",
            "INSERT INTO generation_job_retry_state(generation_job_id) VALUES({job_id})",
        ),
        (
            "generation_provider_attempt_classifications",
            "INSERT INTO generation_provider_attempt_classifications(provider_attempt_id,provider_name,safe_code) "
            "VALUES({attempt_id},'codex_cli','codex_timeout')",
        ),
        (
            "generation_provider_control_events",
            "INSERT INTO generation_provider_control_events(operation_id,provider_name,action,reason_code,actor_kind,actor_id,resulting_paused,previous_control_version,resulting_control_version,control_version) "
            "VALUES('cxo_" + "a" * 32 + "','codex_cli','pause','maintenance','operator',1,1,1,2,2)",
        ),
        (
            "automation_cutover_proposals",
            "INSERT INTO automation_cutover_proposals(id,created_at,expires_at,config_digest,frontiers_digest,cursor_digest,intervals_digest,candidate_max_id,generation_job_max_id,generation_max_id,decision_event_max_id,handoff_max_id,callback_offset,nonterminal_job_count,outbox_count,ready_target_id,ready_target_fingerprint,application_release_digest,audience_binding_digest,proposal_sha256) "
            "VALUES('automation-root','2026-01-01T00:00:00+00:00','2026-01-01T00:10:00+00:00','"
            + DIGEST
            + "','"
            + DIGEST
            + "','"
            + DIGEST
            + "','"
            + DIGEST
            + "',0,0,0,0,0,0,0,0,1,'"
            + DIGEST
            + "','"
            + DIGEST
            + "','"
            + DIGEST
            + "','"
            + DIGEST
            + "')",
        ),
        (
            "automation_cutovers",
            "INSERT INTO automation_cutovers(id,proposal_id,audience_binding_id,target_binding_id,release_digest,activated_at,baseline_candidate_id,baseline_generation_job_id,baseline_generation_id,baseline_decision_event_id,baseline_handoff_id,approval_offset) VALUES(1,'automation-root',1,1,'"
            + DIGEST
            + "','2026-01-01T00:00:00+00:00',0,0,0,0,0,0)",
        ),
        (
            "automation_release_activations",
            "INSERT INTO automation_release_activations(cutover_id,release_digest,activated_at) VALUES(1,'"
            + DIGEST
            + "','2026-01-01T00:00:00+00:00')",
        ),
        (
            "automation_release_config_bindings",
            "INSERT INTO automation_release_config_bindings(activation_id,config_digest,news_policy_version,canonical_policy_json,created_at) VALUES(1,'"
            + DIGEST
            + "','news-policy-v1','{}','2026-01-01T00:00:00+00:00')",
        ),
        (
            "automation_generation_authority",
            "INSERT INTO automation_generation_authority(generation_job_id,selection_id,decision_event_id,cutover_id) VALUES(1,1,1,1)",
        ),
        (
            "automation_defer_authority",
            "INSERT INTO automation_defer_authority(notification_id,decision_event_id,candidate_id,stage,due_at,cutover_id) VALUES(1,1,1,'selection','2026-01-01T00:00:00+00:00',1)",
        ),
        (
            "telegram_notification_outbox",
            "INSERT INTO telegram_notification_outbox(id,audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state) VALUES(1,1,1,'candidate',1,'automation-root','"
            + DIGEST
            + "','pending')",
        ),
        (
            "telegram_chunk_attempts",
            "INSERT INTO telegram_chunk_attempts(chunk_id,ordinal,owner_hash,fence,request_sha256,state,prepared_at) VALUES(1,1,'automation-root',1,'"
            + DIGEST
            + "','prepared','2026-01-01T00:00:00+00:00')",
        ),
        (
            "telegram_notification_events",
            "INSERT INTO telegram_notification_events(notification_id,event_kind) VALUES(1,'created')",
        ),
        (
            "automation_stream_events",
            "INSERT INTO automation_stream_events(stream_run_id,event_kind) VALUES(1,'started')",
        ),
        (
            "automation_stream_leases",
            "INSERT INTO automation_stream_leases(stream,owner_hash,fence,expires_at,acquired_at) "
            "VALUES('collect','legacy-owner',1,'2026-01-01T00:10:00+00:00','2026-01-01T00:00:00+00:00')",
        ),
        (
            "automation_stream_runs",
            "INSERT INTO automation_stream_runs(stream,owner_hash,fence,started_at) "
            "VALUES('collect','automation-root',1,'2026-01-01T00:00:00+00:00')",
        ),
    ),
)
def test_independent_authority_roots_conflict_in_both_orders(tmp_path, table: str, insert_sql: str) -> None:
    with Storage.open(tmp_path / f"root-before-{table}.sqlite3") as storage:
        insert_sql = _prepare_authority_root_insert(storage, insert_sql)
        _insert_authority_root(storage, table, insert_sql)
        assert storage.fetch_one(f"SELECT 1 FROM {table}") is not None
        _assert_named_manual_binding_refusal_term(storage, table)
        with pytest.raises(ManualProfileConflictError, match="conflicts"):
            storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)

    with Storage.open(tmp_path / f"manual-before-{table}.sqlite3") as storage:
        insert_sql = _prepare_authority_root_insert(storage, insert_sql)
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        _assert_named_manual_profile_reciprocal_trigger(storage, table)
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="automation authority conflicts"),
        ):
            _drop_prerequisite_barrier_for_manual_first(connection, table)
            connection.execute(insert_sql)


def test_neutral_provider_control_allows_manual_binding_but_pause_does_not(tmp_path) -> None:
    with Storage.open(tmp_path / "paused-provider-control.sqlite3") as storage:
        _pause_provider_control(storage)
        with pytest.raises(ManualProfileConflictError, match="conflicts"):
            storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)

    with Storage.open(tmp_path / "manual-provider-control.sqlite3") as storage:
        assert (
            storage.fetch_one("SELECT paused_at FROM generation_provider_controls WHERE provider_name='codex_cli'")[
                "paused_at"
            ]
            is None
        )
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        with pytest.raises(sqlite3.IntegrityError, match="automation authority conflicts"):
            _pause_provider_control(storage)


def _manual_candidate(storage: Storage) -> tuple[int, int, int]:
    with storage.transaction() as connection:
        run_id = int(
            connection.execute("INSERT INTO runs(run_key,mode,status) VALUES('manual-test','manual','done')").lastrowid
        )
        post_id = int(
            connection.execute(
                "INSERT INTO source_posts(channel_id,external_post_id) VALUES('manual-source','1')"
            ).lastrowid
        )
        version_id = int(
            connection.execute(
                "INSERT INTO source_post_versions(source_post_id,version_key,body) VALUES(?,'v1','body')",
                (post_id,),
            ).lastrowid
        )
        evaluation_id = int(
            connection.execute(
                "INSERT INTO candidate_evaluations(run_id,source_post_version_id,source_set_key,evaluator_version,score) "
                "VALUES(?,?, 'manual-source', 'manual-v1', '1.000000')",
                (run_id, version_id),
            ).lastrowid
        )
        candidate_id = int(
            connection.execute(
                "INSERT INTO candidates(evaluation_id,status) VALUES(?,'pending_selection')",
                (evaluation_id,),
            ).lastrowid
        )
        connection.execute(
            "INSERT INTO candidate_sources(candidate_id,source_post_version_id) VALUES(?,?)",
            (candidate_id, version_id),
        )
        return run_id, candidate_id, version_id


def _prepare_authority_root_insert(storage: Storage, insert_sql: str) -> str:
    if "{run_id}" not in insert_sql and "{job_id}" not in insert_sql and "{attempt_id}" not in insert_sql:
        return insert_sql

    run_id, candidate_id, _ = _manual_candidate(storage)
    if "{job_id}" not in insert_sql and "{attempt_id}" not in insert_sql:
        return insert_sql.format(run_id=run_id)

    with storage.transaction() as connection:
        digest_id = int(
            connection.execute(
                "INSERT INTO digests(run_id,digest_key,status) VALUES(?,'automation-root','selected')",
                (run_id,),
            ).lastrowid
        )
        selection_id = int(
            connection.execute(
                "INSERT INTO selections(digest_id,candidate_id,position) VALUES(?,?,1)",
                (digest_id, candidate_id),
            ).lastrowid
        )
        job_id = int(
            connection.execute(
                "INSERT INTO generation_jobs(selection_id,job_kind,status,requested_page_count) "
                "VALUES(?,'initial','queued',2)",
                (selection_id,),
            ).lastrowid
        )
        attempt_id = int(
            connection.execute(
                "INSERT INTO generation_provider_attempts("
                "generation_job_id,attempt,started_at,finished_at,terminal_outcome,error_message"
                ") VALUES(?,1,'2026-01-01T00:00:00+00:00','2026-01-01T00:00:01+00:00','failed','timeout')",
                (job_id,),
            ).lastrowid
        )
    return insert_sql.format(run_id=run_id, job_id=job_id, attempt_id=attempt_id)


def _insert_authority_root(storage: Storage, table: str, insert_sql: str) -> None:
    storage._connection.execute("PRAGMA foreign_keys=OFF")
    storage._connection.execute("PRAGMA ignore_check_constraints=ON")
    try:
        with storage.transaction() as connection:
            if table == "sheet_operation_leases":
                storage._authorize_lease("lease-owner", 1)
                connection.execute(
                    "INSERT INTO sheet_remote_operations("
                    "id,target_binding_id,operation_kind,ordinal,status,certainty,current_acquired_event_id,"
                    "current_lease_id,acquired_at"
                    ") VALUES(1,1,'bootstrap',1,'acquired','not_dispatched',1,1,'2026-01-01T00:00:00+00:00')"
                )
            if table == "generation_provider_control_events":
                connection.execute(
                    "UPDATE generation_provider_controls SET paused_at='2026-01-01T00:00:00+00:00', "
                    "pause_reason_code='maintenance',resumed_at=NULL,control_version=2,"
                    "updated_at='2026-01-01T00:00:00+00:00' WHERE provider_name='codex_cli'"
                )
            connection.execute(insert_sql)
            _neutralize_authority_root_prerequisites(connection, table)
    finally:
        storage._connection.execute("PRAGMA ignore_check_constraints=OFF")
        storage._connection.execute("PRAGMA foreign_keys=ON")


def _neutralize_authority_root_prerequisites(connection: sqlite3.Connection, table: str) -> None:
    """Leave only the named root relevant to the manual-binding refusal trigger."""
    if table == "generation_provider_control_events":
        connection.execute(
            "UPDATE generation_provider_controls SET paused_at=NULL,pause_reason_code=NULL,"
            "resumed_at='2026-01-01T00:00:00+00:00',control_version=3,"
            "updated_at='2026-01-01T00:00:00+00:00' "
            "WHERE provider_name='codex_cli'"
        )
        assert (
            connection.execute("SELECT 1 FROM generation_provider_controls WHERE paused_at IS NOT NULL").fetchone()
            is None
        )


def _assert_named_manual_binding_refusal_term(storage: Storage, table: str) -> None:
    if table not in {"sheet_operation_leases", "generation_provider_control_events"}:
        return

    trigger = storage.fetch_one(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='manual_profile_binding_refuses_automation'"
    )
    assert trigger is not None
    assert f"OR EXISTS(SELECT 1 FROM {table})" in trigger["sql"]


def _assert_named_manual_profile_reciprocal_trigger(storage: Storage, table: str) -> None:
    trigger_names = {
        "sheet_operation_leases": "automation_handoff_lease_refuses_manual_profile",
        "generation_provider_control_events": "automation_provider_control_event_refuses_manual_profile",
    }
    trigger_name = trigger_names.get(table)
    if trigger_name is None:
        return

    trigger = storage.fetch_one(
        "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
        (trigger_name,),
    )
    assert trigger is not None
    assert f"BEFORE INSERT ON {table} WHEN EXISTS(SELECT 1 FROM manual_profile_bindings)" in trigger["sql"]


def _drop_prerequisite_barrier_for_manual_first(connection: sqlite3.Connection, table: str) -> None:
    """Ensure the named reciprocal trigger is the first target-insert barrier."""
    prerequisite_triggers = {
        "sheet_operation_leases": "lease_insert",
        "generation_provider_control_events": "generation_provider_control_events_insert",
    }
    trigger = prerequisite_triggers.get(table)
    if trigger is not None:
        connection.execute(f"DROP TRIGGER {trigger}")


def _pause_provider_control(storage: Storage) -> None:
    with storage.transaction() as connection:
        connection.execute(
            "UPDATE generation_provider_controls SET paused_at='2026-01-01T00:00:00+00:00', "
            "pause_reason_code='maintenance',resumed_at=NULL,control_version=2,"
            "updated_at='2026-01-01T00:00:00+00:00' WHERE provider_name='codex_cli'"
        )


def test_manual_selection_is_idempotent_and_creates_no_remote_authority(tmp_path) -> None:
    with Storage.open(tmp_path / "selection.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        run_id, candidate_id, version_id = _manual_candidate(storage)
        _, receipt = storage.manual_candidate_preview(run_id)
        result = storage.apply_manual_candidate_decision(
            run_id, candidate_id, "select", "2026-01-01T00:00:00+00:00", receipt
        )
        assert result.generation_job_id is not None
        assert (
            storage.apply_manual_candidate_decision(
                run_id, candidate_id, "select", "2026-01-01T00:00:00+00:00", receipt
            )
            == result
        )
        assert (
            storage.fetch_one(
                "SELECT candidate_preview_receipt FROM manual_candidate_decisions WHERE candidate_id=?", (candidate_id,)
            )["candidate_preview_receipt"]
            == receipt
        )
        with pytest.raises(ManualProfileConflictError, match="does not match"):
            storage.apply_manual_candidate_decision(
                run_id, candidate_id, "select", "2026-01-01T00:00:00+00:00", OTHER_DIGEST
            )
        assert (
            storage.fetch_one(
                "SELECT source_post_version_id FROM generation_sources WHERE generation_job_id=? AND generation_id IS NULL",
                (result.generation_job_id,),
            )["source_post_version_id"]
            == version_id
        )
        for table in (
            "callback_tokens",
            "decision_events",
            "telegram_notification_outbox",
            "sheet_handoffs",
            "automation_cutovers",
        ):
            assert storage.fetch_one(f"SELECT 1 FROM {table}") is None


def _manual_review_candidate(storage: Storage) -> tuple[int, int]:
    run_id, candidate_id, version_id = _manual_candidate(storage)
    _, receipt = storage.manual_candidate_preview(run_id)
    selected = storage.apply_manual_candidate_decision(
        run_id, candidate_id, "select", "2026-01-01T00:00:00+00:00", receipt
    )
    assert selected.generation_job_id is not None
    with storage.transaction() as connection:
        connection.execute(
            "UPDATE generation_jobs SET status='succeeded' WHERE id=?",
            (selected.generation_job_id,),
        )
        generation_id = int(
            connection.execute(
                "INSERT INTO generations(generation_job_id,attempt,status,content_json) VALUES(?,1,'current','{}')",
                (selected.generation_job_id,),
            ).lastrowid
        )
        connection.execute(
            "INSERT INTO generation_sources(generation_job_id,generation_id,source_post_version_id) VALUES(?,?,?)",
            (selected.generation_job_id, generation_id, version_id),
        )
        connection.execute("UPDATE candidates SET status='pending_review' WHERE id=?", (candidate_id,))
    return candidate_id, generation_id


def test_manual_review_approval_is_atomic_idempotent_and_local_only(tmp_path) -> None:
    with Storage.open(tmp_path / "review.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        candidate_id, generation_id = _manual_review_candidate(storage)
        result = storage.apply_manual_review_decision(
            candidate_id,
            generation_id,
            "approve_local",
            "2026-01-01T00:00:00+00:00",
            b'{"ok":true}',
            b"# ok\n",
        )
        assert result.status == "approved"
        assert {export.export_format for export in result.exports} == {"json", "markdown"}
        assert (
            storage.apply_manual_review_decision(
                candidate_id,
                generation_id,
                "approve_local",
                "2026-01-01T00:00:00+00:00",
                b'{"ok":true}',
                b"# ok\n",
            ).exports
            == result.exports
        )
        assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "approved"
        assert storage.fetch_one("SELECT 1 FROM digests WHERE status='approved'") is None
        for table in (
            "callback_tokens",
            "decision_events",
            "telegram_notification_outbox",
            "sheet_handoffs",
            "automation_cutovers",
            "automation_generation_authority",
        ):
            assert storage.fetch_one(f"SELECT 1 FROM {table}") is None


def test_direct_sql_cannot_approve_partially_or_review_stale_generation(tmp_path) -> None:
    with Storage.open(tmp_path / "manual-sql.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        candidate_id, generation_id = _manual_review_candidate(storage)
        source_digest = storage.fetch_one(
            "SELECT sha256_hex(CAST('[' || group_concat(source_post_version_id, ',') || ']' AS BLOB)) AS digest "
            "FROM (SELECT source_post_version_id FROM generation_sources "
            "WHERE generation_id=? ORDER BY source_post_version_id)",
            (generation_id,),
        )["digest"]
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="manual review decision is incoherent"),
        ):
            connection.execute(
                "INSERT INTO manual_local_decisions("
                "profile_binding_id,generation_id,decision,source_set_digest,decided_at"
                ") VALUES(1,?,'reject',?,'2026-01-01T00:00:00+00:00')",
                (generation_id, source_digest),
            )
        assert storage.fetch_one("SELECT 1 FROM manual_local_decisions WHERE generation_id=?", (generation_id,)) is None

        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="complete exports"),
        ):
            connection.execute("UPDATE candidates SET status='approved' WHERE id=?", (candidate_id,))
        assert (
            storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "pending_review"
        )

        with storage.transaction() as connection:
            connection.execute("UPDATE generations SET status='superseded' WHERE id=?", (generation_id,))
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="review decision is incoherent"),
        ):
            connection.execute(
                "INSERT INTO manual_local_decisions(profile_binding_id,generation_id,decision,source_set_digest,decided_at) "
                "VALUES(1,?,'reject',?,'2026-01-01T00:00:00+00:00')",
                (generation_id, DIGEST),
            )
        assert storage.fetch_one("SELECT 1 FROM manual_local_decisions WHERE generation_id=?", (generation_id,)) is None


def test_direct_sql_forged_or_partial_candidate_decision_is_rejected(tmp_path) -> None:
    with Storage.open(tmp_path / "manual-forged-digest.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        run_id, candidate_id, version_id = _manual_candidate(storage)
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="manual candidate decision is incoherent"),
        ):
            connection.execute(
                "INSERT INTO manual_candidate_decisions("
                "profile_binding_id,run_id,candidate_id,decision,source_set_digest,candidate_preview_receipt,decided_at"
                ") VALUES(1,? ,?,'select',? ,?,'2026-01-01T00:00:00+00:00')",
                (run_id, candidate_id, DIGEST, DIGEST),
            )
        source_digest = storage.fetch_one(
            "SELECT sha256_hex(CAST('[' || ? || ']' AS BLOB)) AS digest",
            (version_id,),
        )["digest"]
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="manual candidate decision is incoherent"),
        ):
            connection.execute(
                "INSERT INTO manual_candidate_decisions("
                "profile_binding_id,run_id,candidate_id,decision,source_set_digest,candidate_preview_receipt,decided_at"
                ") VALUES(1,? ,?,'select',? ,?,'2026-01-01T00:00:00+00:00')",
                (run_id, candidate_id, source_digest, DIGEST),
            )
        assert (
            storage.fetch_one("SELECT 1 FROM manual_candidate_decisions WHERE candidate_id=?", (candidate_id,)) is None
        )
        _, receipt = storage.manual_candidate_preview(run_id)
        assert (
            storage.apply_manual_candidate_decision(
                run_id, candidate_id, "select", "2026-01-01T00:00:00+00:00", receipt
            ).candidate_id
            == candidate_id
        )


def test_manual_review_regeneration_creates_one_local_queued_job(tmp_path) -> None:
    with Storage.open(tmp_path / "regenerate.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", DIGEST)
        candidate_id, generation_id = _manual_review_candidate(storage)
        result = storage.apply_manual_review_decision(
            candidate_id, generation_id, "regenerate", "2026-01-01T00:00:00+00:00"
        )
        assert result.status == "selected_generation_pending"
        assert result.generation_job_id is not None
        assert (
            storage.fetch_one("SELECT status FROM generations WHERE id=?", (generation_id,))["status"] == "superseded"
        )
        assert (
            storage.fetch_one("SELECT status FROM generation_jobs WHERE id=?", (result.generation_job_id,))["status"]
            == "queued"
        )
        assert (
            storage.apply_manual_review_decision(
                candidate_id, generation_id, "regenerate", "2026-01-01T00:00:00+00:00"
            ).generation_job_id
            == result.generation_job_id
        )
        with pytest.raises(ManualProfileConflictError, match="does not match"):
            storage.apply_manual_review_decision(candidate_id, generation_id, "regenerate", "2026-01-01T00:00:01+00:00")

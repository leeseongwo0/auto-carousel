from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

from newsbot.automation import AutomationAuthority, AutomationBusyError, AutomationDriftError, CutoverProposal, Frontier
from newsbot.storage import Storage

NOW = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)


def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def proposal() -> CutoverProposal:
    return CutoverProposal(
        proposal_id="proposal-automation-0001",
        config_digest=digest("config"),
        cursor_digest=digest("cursors"),
        intervals_digest=digest("intervals"),
        maxima=(0, 0, 0, 0, 0),
        approval_offset=0,
        target_id=1,
        target_fingerprint=digest("target-ref"),
        release_digest=digest("release"),
        audience_digest=digest("1"),
        frontiers=tuple(Frontier(digest(f"channel-{index}"), index, NOW) for index in range(6)),
    )


def add_target(storage: Storage) -> None:
    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO sheet_target_bindings(id,target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint) "
            "VALUES(1,?,'workplace-template-v1',0,'workplace',?)",
            (digest("target-ref"), digest("target")),
        )
        connection.execute(
            "INSERT INTO sheet_bootstraps(target_binding_id,marker_value,controls_fingerprint,status,verified_at) "
            "VALUES(1,'marker',?,'ready',?)",
            (digest("controls"), NOW.isoformat()),
        )
        connection.execute(
            "INSERT INTO telegram_audience_bindings(id,bot_id_digest,token_hmac,audience_hmac,version) "
            "VALUES(1,?,?,?,1)",
            (digest("bot"), digest("token"), digest("audience-runtime")),
        )


def add_candidate(storage: Storage, *, status: str = "pending_selection") -> int:
    with storage.transaction() as connection:
        candidate_id = int(connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM candidates").fetchone()[0])
        connection.execute(
            "INSERT INTO runs(id,run_key,mode,status) VALUES(?,?, 'fixture','done')",
            (candidate_id, f"automation-fixture-{candidate_id}"),
        )
        connection.execute(
            "INSERT INTO source_posts(id,channel_id,external_post_id) VALUES(?, 'fixture', ?)",
            (candidate_id, str(candidate_id)),
        )
        connection.execute(
            "INSERT INTO source_post_versions(id,source_post_id,version_key,body) VALUES(?,?,?,'fixture body')",
            (candidate_id, candidate_id, f"v{candidate_id}"),
        )
        connection.execute(
            "INSERT INTO candidate_evaluations("
            "id,run_id,source_post_version_id,source_set_key,evaluator_version,score"
            ") VALUES(?,?,?,?, 'v1','1.000000')",
            (candidate_id, candidate_id, candidate_id, f"fixture-{candidate_id}"),
        )
        if status == "deferred":
            connection.execute(
                "INSERT INTO candidates(id,evaluation_id,status,deferred_stage,deferred_until) "
                "VALUES(?,?,'deferred','selection',?)",
                (candidate_id, candidate_id, (NOW + timedelta(hours=1)).isoformat()),
            )
        else:
            connection.execute(
                "INSERT INTO candidates(id,evaluation_id,status) VALUES(?,?,?)",
                (candidate_id, candidate_id, status),
            )
    return candidate_id


def activate(storage: Storage) -> tuple[AutomationAuthority, str, int]:
    authority = AutomationAuthority(storage)
    add_target(storage)
    maxima = tuple(
        int(storage.fetch_one(f"SELECT COALESCE(MAX(id),0) AS value FROM {table}")["value"])
        for table in ("candidates", "generation_jobs", "generations", "decision_events", "sheet_handoffs")
    )
    item = replace(proposal(), maxima=maxima)
    receipt = authority.persist_proposal(item, now=NOW)
    binding_id = authority.record_audience_binding(
        bot_id_digest=digest("bot"), token_hmac=digest("token"), audience_hmac=digest("audience-runtime"), version=1
    )
    assert authority.apply_proposal(
        item.proposal_id,
        receipt,
        audience_binding_id=binding_id,
        release_digest=digest("release"),
        now=NOW,
        validate=lambda: True,
    ) == {"changed": True, "status": "active"}
    return authority, receipt, binding_id


def insert_candidate_notification(storage: Storage, candidate_id: int, source_set_key: str = "source") -> int:
    with storage.transaction() as connection:
        assert AutomationAuthority.enqueue_candidate_notification(
            connection, candidate_id=candidate_id, source_set_key=source_set_key, subject_digest=digest(source_set_key)
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def test_migration_007_fresh_and_006_upgrade_create_no_work_and_enforce_authority_pragmas(tmp_path: Path) -> None:
    database = tmp_path / "fresh.sqlite"
    with Storage.open(database) as storage:
        assert {row["version"] for row in storage.fetch_all("SELECT version FROM schema_migrations")} == {
            f"{number:03d}_{name}.sql"
            for number, name in (
                (1, "initial"),
                (2, "canonical_authority"),
                (3, "sheets_handoff"),
                (4, "sheets_authority_upgrade"),
                (5, "generation_provider_retry"),
                (6, "telegram_update_cursor"),
                (7, "systemd_automation"),
            )
        }
        assert storage.fetch_one("PRAGMA journal_mode")[0].lower() == "wal"
        assert storage.fetch_one("PRAGMA synchronous")[0] == 2
        assert storage.fetch_one("PRAGMA foreign_keys")[0] == 1
        assert storage.fetch_one("PRAGMA busy_timeout")[0] == 5000
        for table in (
            "automation_cutover_proposals",
            "automation_proposal_frontiers",
            "telegram_audience_bindings",
            "automation_cutovers",
            "automation_release_activations",
            "telegram_notification_outbox",
            "telegram_notification_chunks",
            "automation_stream_leases",
            "telegram_chunk_attempts",
            "automation_stream_runs",
        ):
            assert storage.fetch_one(f"SELECT COUNT(*) AS count FROM {table}")["count"] == 0

    legacy = tmp_path / "migration-006.sqlite"
    storage = Storage(legacy)
    try:
        migrations = Path(__file__).parents[2] / "src" / "newsbot" / "migrations"
        for name in sorted(path.name for path in migrations.glob("00[1-6]_*.sql")):
            script = (migrations / name).read_text(encoding="utf-8")
            if name == "002_canonical_authority.sql":
                storage._prepare_canonical_authority_upgrade()
                storage._connection.execute("PRAGMA foreign_keys = OFF")
            if name == "004_sheets_authority_upgrade.sql":
                storage._assert_sheets_authority_upgrade_supported(migrations / "003_sheets_handoff.sql")
                script = script.replace("__HANDOFF_TARGET_EXPR__", "b.target_binding_id")
            storage._connection.executescript(script)
            storage._connection.execute("INSERT INTO schema_migrations(version) VALUES(?)", (name,))
            storage._connection.commit()
            if name == "002_canonical_authority.sql":
                storage._connection.execute("PRAGMA foreign_keys = ON")
        storage.migrate()
        assert (
            storage.fetch_one("SELECT 1 FROM schema_migrations WHERE version='007_systemd_automation.sql'") is not None
        )
        assert storage.fetch_one("SELECT COUNT(*) AS count FROM telegram_notification_outbox")["count"] == 0
        assert storage.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        storage.close()


def test_proposal_cutover_replay_drift_expiry_and_immutable_records(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "authority.sqlite") as storage:
        authority = AutomationAuthority(storage)
        add_target(storage)
        item = proposal()
        receipt = authority.persist_proposal(item, now=NOW)
        binding_id = authority.record_audience_binding(
            bot_id_digest=digest("bot"),
            token_hmac=digest("token"),
            audience_hmac=digest("audience-runtime"),
            version=1,
        )
        assert authority.apply_proposal(
            item.proposal_id,
            receipt,
            audience_binding_id=binding_id,
            release_digest=item.release_digest,
            now=NOW,
            validate=lambda: True,
        ) == {"changed": True, "status": "active"}
        assert authority.apply_proposal(
            item.proposal_id,
            receipt,
            audience_binding_id=binding_id,
            release_digest=item.release_digest,
            now=NOW,
            validate=lambda: False,
        ) == {"changed": False, "status": "active"}
        assert sorted(frontier.upper_message_id for frontier in authority.active_frontiers()) == list(range(6))
        with storage.transaction() as connection:
            for statement in (
                f"UPDATE automation_cutover_proposals SET config_digest='{digest('tampered-proposal')}' "
                "WHERE id='proposal-automation-0001'",
                "DELETE FROM automation_proposal_frontiers WHERE proposal_id='proposal-automation-0001'",
                "UPDATE telegram_audience_bindings SET version=2 WHERE id=1",
                f"UPDATE automation_cutovers SET release_digest='{digest('tampered-cutover')}' WHERE id=1",
                "DELETE FROM automation_cutovers WHERE id=1",
                f"UPDATE automation_release_activations SET release_digest='{digest('tampered-release')}' WHERE id=1",
                "DELETE FROM automation_release_activations WHERE id=1",
            ):
                with pytest.raises(sqlite3.IntegrityError):
                    connection.execute(statement)
        with pytest.raises(ValueError, match="exactly six"):
            authority.persist_proposal(replace(item, frontiers=item.frontiers[:-1]), now=NOW)

    with Storage.open(tmp_path / "drift.sqlite") as storage:
        authority = AutomationAuthority(storage)
        add_target(storage)
        receipt = authority.persist_proposal(proposal(), now=NOW)
        binding_id = authority.record_audience_binding(
            bot_id_digest=digest("bot"),
            token_hmac=digest("token"),
            audience_hmac=digest("audience-runtime"),
            version=1,
        )
        with pytest.raises(AutomationDriftError, match="cutover state drifted"):
            authority.apply_proposal(
                proposal().proposal_id,
                receipt,
                audience_binding_id=binding_id,
                release_digest=digest("release"),
                now=NOW,
                validate=lambda: False,
            )
        with pytest.raises(AutomationDriftError, match="expired"):
            authority.apply_proposal(
                proposal().proposal_id,
                receipt,
                audience_binding_id=binding_id,
                release_digest=digest("release"),
                now=NOW + timedelta(seconds=600),
                validate=lambda: True,
            )


def test_audience_bindings_are_append_only_versions_and_exact_replays(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "audiences.sqlite") as storage:
        authority = AutomationAuthority(storage)
        bot = digest("bot")
        first = authority.record_audience_binding(
            bot_id_digest=bot,
            token_hmac=digest("token-1"),
            audience_hmac=digest("audience-1"),
            version=authority.next_audience_version(bot),
        )
        assert authority.record_audience_binding(
            bot_id_digest=bot,
            token_hmac=digest("token-1"),
            audience_hmac=digest("audience-1"),
            version=99,
        ) == first
        second = authority.record_audience_binding(
            bot_id_digest=bot,
            token_hmac=digest("token-2"),
            audience_hmac=digest("audience-2"),
            version=authority.next_audience_version(bot),
        )
        assert second != first
        assert authority.next_audience_version(bot) == 3
        with pytest.raises(AutomationDriftError, match="version drifted"):
            authority.record_audience_binding(
                bot_id_digest=bot,
                token_hmac=digest("token-3"),
                audience_hmac=digest("audience-3"),
                version=2,
            )


def test_runtime_release_activations_form_an_immutable_append_only_chain(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "releases.sqlite") as storage:
        authority, _, _ = activate(storage)
        initial = storage.fetch_one(
            "SELECT id,prior_activation_id,release_digest FROM automation_release_activations ORDER BY id"
        )
        assert initial is not None
        assert initial["prior_activation_id"] is None
        assert initial["release_digest"] == digest("release")
        changed = authority.activate_release(digest("release-2"), now=NOW + timedelta(minutes=1), validate=lambda: True)
        assert changed["changed"] is True
        assert authority.activate_release(
            digest("release-2"), now=NOW + timedelta(minutes=2), validate=lambda: False
        ) == {"activation_id": changed["activation_id"], "changed": False, "status": "active"}
        rows = storage.fetch_all(
            "SELECT id,prior_activation_id,release_digest FROM automation_release_activations ORDER BY id"
        )
        assert len(rows) == 2
        assert rows[1]["prior_activation_id"] == rows[0]["id"]
        with storage.transaction() as connection:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE automation_release_activations SET release_digest=? WHERE id=?",
                    (digest("tampered"), rows[1]["id"]),
                )
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM automation_release_activations WHERE id=?", (rows[1]["id"],))


def test_stream_fences_audience_outbox_status_and_quiescence(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "leases.sqlite") as storage:
        authority, _, _ = activate(storage)
        candidate_id = add_candidate(storage)
        first = authority.acquire_lease("telegram_dispatch", now=NOW, lease_seconds=1, owner_token="first")
        with pytest.raises(AutomationBusyError):
            authority.acquire_lease("telegram_dispatch", now=NOW, lease_seconds=1, owner_token="second")
        second = authority.acquire_lease(
            "telegram_dispatch", now=NOW + timedelta(seconds=1), lease_seconds=60, owner_token="second"
        )
        assert second.fence == first.fence + 1
        with pytest.raises(AutomationBusyError, match="no longer current"):
            authority.claim_next_notification(first, now=NOW + timedelta(seconds=1))
        assert authority.release_lease(first, now=NOW + timedelta(seconds=1)) is False
        insert_candidate_notification(storage, candidate_id)
        with storage.transaction() as connection:
            assert not AutomationAuthority.enqueue_candidate_notification(
                connection, candidate_id=candidate_id, source_set_key="source", subject_digest=digest("changed")
            )
        assert authority.safe_cutover() == {"active": True}
        assert authority.safe_status() == {
            "cutover_active": True,
            "open_leases": 1,
            "open_runs": 1,
            "pending_notifications": 1,
            "ambiguous_notifications": 0,
            "partial_notifications": 0,
        }
        assert authority.quiescent() is False
        token_hmac, audience_hmac = AutomationAuthority.audience_hmac("secret", "-100", ("7", "2"), "7")
        assert token_hmac != audience_hmac
        assert not authority.validate_active_audience(
            bot_id_digest=digest("bot"), token_hmac=token_hmac, audience_hmac=audience_hmac
        )
        with pytest.raises(ValueError, match="distinct"):
            AutomationAuthority.audience_hmac("secret", "-100", ("7", "7"), "7")
        assert authority.release_lease(second, now=NOW + timedelta(seconds=2))
        third = authority.acquire_lease(
            "telegram_dispatch", now=NOW + timedelta(seconds=3), lease_seconds=60, owner_token="third"
        )
        assert third.fence == second.fence + 1
        assert authority.release_lease(third, now=NOW + timedelta(seconds=4))


def test_direct_sql_cannot_rewrite_stream_or_dispatch_authority(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "direct-sql.sqlite") as storage:
        authority, _, _ = activate(storage)
        candidate_id = add_candidate(storage)
        notification_id = insert_candidate_notification(storage, candidate_id)
        authority.create_notification_chunks(notification_id, ((1, digest("chunk"), True),))
        lease = authority.acquire_lease("telegram_dispatch", now=NOW, lease_seconds=60, owner_token="dispatcher")
        assert authority.claim_next_notification(lease, now=NOW) is not None
        chunk = authority.next_chunk(notification_id, lease, now=NOW)
        assert chunk is not None
        attempt_id = authority.prepare_chunk_attempt(notification_id, chunk[0], digest("request"), lease, now=NOW)

        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO callback_tokens(token,action,payload_json) VALUES(?, 'make', '{}')",
                (digest("direct-callback"),),
            )
            callback_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            for statement, parameters in (
                ("UPDATE automation_stream_runs SET fence=99 WHERE stream='telegram_dispatch'", ()),
                ("DELETE FROM automation_stream_runs WHERE stream='telegram_dispatch'", ()),
                (
                    "UPDATE telegram_notification_outbox SET subject_digest=? WHERE id=?",
                    (digest("rewrite"), notification_id),
                ),
                ("DELETE FROM telegram_notification_outbox WHERE id=?", (notification_id,)),
                ("UPDATE telegram_chunk_attempts SET accepted_message_id=7 WHERE id=?", (attempt_id,)),
                ("DELETE FROM telegram_chunk_attempts WHERE id=?", (attempt_id,)),
                ("UPDATE callback_tokens SET notification_id=? WHERE id=?", (notification_id, callback_id)),
            ):
                with pytest.raises(sqlite3.IntegrityError):
                    connection.execute(statement, parameters)

        assert authority.link_callback(callback_id, notification_id, attempt_id, lease, now=NOW)
        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE callback_tokens SET notification_id=NULL,chunk_attempt_id=NULL WHERE id=?", (callback_id,)
            )
        assert authority.release_lease(lease, now=NOW + timedelta(seconds=1))
        with storage.transaction() as connection:
            connection.execute(
                "UPDATE telegram_notification_outbox SET state='sent',terminal_at=? WHERE id=?",
                (NOW.isoformat(), notification_id),
            )
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE telegram_notification_outbox SET state='pending',terminal_at=NULL WHERE id=?",
                    (notification_id,),
                )


def test_chunk_crash_order_callbacks_resolution_and_no_resend(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "dispatch.sqlite") as storage:
        authority, _, _ = activate(storage)
        candidate_id = add_candidate(storage)
        notification_id = insert_candidate_notification(storage, candidate_id)
        authority.create_notification_chunks(
            notification_id,
            ((1, digest("one"), False), (4096, digest("two"), True)),
        )
        with pytest.raises(AutomationDriftError, match="chunk identity"):
            authority.create_notification_chunks(
                notification_id,
                ((1, digest("changed"), False), (4096, digest("two"), True)),
            )
        lease = authority.acquire_lease("telegram_dispatch", now=NOW, lease_seconds=60, owner_token="dispatcher")
        assert authority.claim_next_notification(lease, now=NOW).state == "claimed"
        first_chunk = authority.next_chunk(notification_id, lease, now=NOW)
        assert first_chunk is not None and first_chunk[1:] == (0, digest("one"), False)
        first_attempt = authority.prepare_chunk_attempt(
            notification_id, first_chunk[0], digest("request-1"), lease, now=NOW
        )
        authority.mark_possibly_sent(first_attempt, lease, now=NOW)
        authority.settle_attempt(first_attempt, "accepted", lease, now=NOW, accepted_message_id=99)
        second_chunk = authority.next_chunk(notification_id, lease, now=NOW)
        assert second_chunk is not None and second_chunk[1:] == (1, digest("two"), True)
        second_attempt = authority.prepare_chunk_attempt(
            notification_id, second_chunk[0], digest("request-2"), lease, now=NOW
        )
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO callback_tokens(token,action,payload_json) VALUES(?, 'make', '{\"candidate_id\": 1}')",
                (digest("callback"),),
            )
            callback_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        assert authority.link_callback(callback_id, notification_id, second_attempt, lease, now=NOW)
        authority.settle_attempt(second_attempt, "trusted_rejected", lease, now=NOW)
        assert (
            storage.fetch_one("SELECT state FROM telegram_notification_outbox WHERE id=?", (notification_id,))["state"]
            == "partial_manual_required"
        )
        with pytest.raises(AutomationDriftError, match="not dispatchable"):
            authority.prepare_chunk_attempt(notification_id, second_chunk[0], digest("resend"), lease, now=NOW)
        assert not authority.resolve_notification(
            notification_id,
            "ambiguous",
            "resolved_abandoned",
            actor_id=7,
            reason_code="operator_abandoned",
            now=NOW,
        )
        assert authority.resolve_notification(
            notification_id,
            "partial_manual_required",
            "resolved_abandoned",
            actor_id=7,
            reason_code="operator_abandoned",
            now=NOW,
        )
        assert (
            storage.fetch_one("SELECT revoked_at FROM callback_tokens WHERE id=?", (callback_id,))["revoked_at"]
            is not None
        )
        resolution = storage.fetch_one(
            "SELECT prior_state,resolution,actor_id,reason_code FROM telegram_notification_resolutions "
            "WHERE notification_id=?",
            (notification_id,),
        )
        assert resolution is not None
        assert tuple(resolution) == (
            "partial_manual_required",
            "resolved_abandoned",
            7,
            "operator_abandoned",
        )

        other_id = insert_candidate_notification(storage, candidate_id, "other-source")
        authority.create_notification_chunks(other_id, ((1, digest("three"), True),))
        assert authority.discover_notification(other_id, lease, now=NOW) is not None
        chunk = authority.next_chunk(other_id, lease, now=NOW)
        assert chunk is not None
        attempt = authority.prepare_chunk_attempt(other_id, chunk[0], digest("request-3"), lease, now=NOW)
        authority.mark_possibly_sent(attempt, lease, now=NOW)
        authority.settle_attempt(attempt, "ambiguous", lease, now=NOW)
        assert authority.resolve_notification(
            other_id,
            "ambiguous",
            "resolved_delivered",
            actor_id=7,
            reason_code="transport_verified",
            now=NOW,
        )
        assert authority.release_lease(lease, now=NOW + timedelta(seconds=1))


def test_claimed_notification_is_reclaimed_after_lease_takeover(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "claimed-takeover.sqlite") as storage:
        authority, _, _ = activate(storage)
        notification_id = insert_candidate_notification(storage, add_candidate(storage))
        first = authority.acquire_lease("telegram_dispatch", now=NOW, lease_seconds=1, owner_token="first")
        claim = authority.claim_next_notification(first, now=NOW)
        assert claim is not None and claim.state == "claimed"
        second = authority.acquire_lease(
            "telegram_dispatch",
            now=NOW + timedelta(seconds=1),
            lease_seconds=60,
            owner_token="second",
        )
        reclaimed = authority.claim_next_notification(second, now=NOW + timedelta(seconds=1))
        assert reclaimed is not None
        assert reclaimed.notification_id == notification_id
        assert reclaimed.state == "claimed"


def test_failed_dispatch_release_abandons_prepared_attempt_before_deleting_lease(
    tmp_path: Path,
) -> None:
    with Storage.open(tmp_path / "failed-release.sqlite") as storage:
        authority, _, _ = activate(storage)
        notification_id = insert_candidate_notification(storage, add_candidate(storage))
        lease = authority.acquire_lease(
            "telegram_dispatch",
            now=NOW,
            lease_seconds=60,
            owner_token="failed",
        )
        claim = authority.claim_next_notification(lease, now=NOW)
        assert claim is not None
        authority.create_notification_chunks(
            notification_id,
            ((12, digest("chunk"), True),),
        )
        chunk = authority.next_chunk(notification_id, lease, now=NOW)
        assert chunk is not None
        attempt_id = authority.prepare_chunk_attempt(
            notification_id,
            chunk[0],
            digest("request"),
            lease,
            now=NOW,
        )

        assert authority.release_lease(
            lease,
            now=NOW + timedelta(seconds=1),
            outcome="failed",
        )
        assert (
            storage.fetch_one(
                "SELECT state FROM telegram_chunk_attempts WHERE id=?",
                (attempt_id,),
            )["state"]
            == "abandoned_pre_marker"
        )
        assert (
            storage.fetch_one(
                "SELECT state FROM telegram_notification_outbox WHERE id=?",
                (notification_id,),
            )["state"]
            == "pending"
        )


def test_possibly_sent_takeover_becomes_ambiguous_without_redispatch(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "ambiguous-takeover.sqlite") as storage:
        authority, _, _ = activate(storage)
        candidate_id = add_candidate(storage)
        notification_id = insert_candidate_notification(storage, candidate_id)
        first = authority.acquire_lease("telegram_dispatch", now=NOW, lease_seconds=1, owner_token="first")
        claim = authority.claim_next_notification(first, now=NOW)
        assert claim is not None
        authority.create_notification_chunks(notification_id, ((12, digest("chunk"), True),))
        chunk = authority.next_chunk(notification_id, first, now=NOW)
        assert chunk is not None
        attempt_id = authority.prepare_chunk_attempt(notification_id, chunk[0], digest("request"), first, now=NOW)
        authority.mark_possibly_sent(attempt_id, first, now=NOW)

        second = authority.acquire_lease(
            "telegram_dispatch",
            now=NOW + timedelta(seconds=1),
            lease_seconds=60,
            owner_token="second",
        )
        recovered = authority.claim_next_notification(second, now=NOW + timedelta(seconds=1))
        assert recovered is not None and recovered.state == "sending"
        assert authority.recover_possibly_sent(notification_id, second, now=NOW + timedelta(seconds=1))
        assert (
            storage.fetch_one("SELECT state FROM telegram_chunk_attempts WHERE id=?", (attempt_id,))["state"]
            == "ambiguous"
        )
        assert (
            storage.fetch_one("SELECT state FROM telegram_notification_outbox WHERE id=?", (notification_id,))["state"]
            == "ambiguous"
        )
        assert authority.claim_next_notification(second, now=NOW + timedelta(seconds=1)) is None


def test_deferred_transition_is_legacy_only_before_cutover(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "deferred.sqlite") as storage:
        candidate_id = add_candidate(storage, status="deferred")
        with storage.transaction() as connection:
            connection.execute(
                "UPDATE candidates SET status='pending_selection', deferred_stage=NULL, deferred_until=NULL WHERE id=?",
                (candidate_id,),
            )
        with storage.transaction() as connection:
            connection.execute("UPDATE candidates SET status='rejected' WHERE id=?", (candidate_id,))
        authority, _, _ = activate(storage)
        post_cutover_candidate = add_candidate(storage, status="deferred")
        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError, match="automation authority"):
            connection.execute(
                "UPDATE candidates SET status='pending_selection', deferred_stage=NULL, deferred_until=NULL WHERE id=?",
                (post_cutover_candidate,),
            )
        assert authority.safe_status()["cutover_active"] is True

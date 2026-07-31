from __future__ import annotations

import sqlite3
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from newsbot.handoffs import SheetHandoffService, SheetLease, enqueue_sheet_handoff
from newsbot.storage import Storage

NOW = "2026-07-30T12:00:00+00:00"
LATER = "2026-07-30T12:05:00+00:00"
AFTER = "2026-07-30T12:10:00+00:00"
AFTER_LATER = "2026-07-30T12:15:00+00:00"
HASH = "a" * 64
CREDENTIAL_EXPIRES = "2026-07-30T16:00:00+00:00"


def _mark(
    service: SheetHandoffService,
    lease: SheetLease,
    *,
    now: str = NOW,
    preflight: bool = True,
    oracle_fingerprint: str = HASH,
    controls_fingerprint: str = HASH,
    credential_refreshed_at: str = NOW,
    credential_expires_at: str = CREDENTIAL_EXPIRES,
    credential_scope_ok: bool = True,
) -> bool:
    if preflight:
        assert service.record_preflight(lease, outcome="absent", now=now)
    return service.mark_possibly_sent(
        lease,
        request_sha256=HASH,
        oracle_fingerprint=oracle_fingerprint,
        controls_fingerprint=controls_fingerprint,
        credential_refreshed_at=credential_refreshed_at,
        credential_expires_at=credential_expires_at,
        credential_scope_ok=credential_scope_ok,
        now=now,
    )


def _handoff(
    storage: Storage,
    *,
    bootstrap_status: str = "ready",
) -> tuple[int, int]:
    """Create valid legacy parents; migration 003 must not fabricate/backfill them."""
    with storage.transaction() as c:
        c.execute("INSERT INTO runs(id,run_key,mode,status) VALUES(1,'r','fixture','done')")
        c.execute("INSERT INTO source_posts(id,channel_id,external_post_id) VALUES(1,'c','p')")
        c.execute("INSERT INTO source_post_versions(id,source_post_id,version_key,body) VALUES(1,1,'v','body')")
        c.execute(
            "INSERT INTO candidate_evaluations(id,run_id,source_post_version_id,evaluator_version,score) VALUES(1,1,1,'v','1.000000')"
        )
        c.execute("INSERT INTO candidates(id,evaluation_id,status) VALUES(1,1,'approved')")
        c.execute("INSERT INTO digests(id,run_id,digest_key,status) VALUES(1,1,'d','approved')")
        c.execute("INSERT INTO selections(id,digest_id,candidate_id,position) VALUES(1,1,1,1)")
        c.execute("INSERT INTO generation_jobs(id,selection_id,job_kind,status) VALUES(1,1,'carousel','succeeded')")
        c.execute(
            "INSERT INTO generations(id,generation_job_id,attempt,status,content_json) VALUES(1,1,1,'current','{}')"
        )
        c.execute(
            "INSERT INTO decision_events(id,run_id,event_key,decision,actor) VALUES(1,1,'approve','approve','test')"
        )
    service = SheetHandoffService(storage)
    target = service.ensure_binding(
        binding_key="workplace", spreadsheet_id="sheet", sheet_id=0, oracle_fingerprint=HASH, now=NOW
    )
    with storage.transaction() as c:
        handoff = enqueue_sheet_handoff(
            c,
            generation_id=1,
            approval_event_id=1,
            target_binding_id=target,
            export_id="exp_" + "1" * 32,
            canonical_bytes=b'{"canonical":true}',
            approved_at=NOW,
            category="AI",
            now=NOW,
        )
    with storage.transaction() as c:
        c.execute(
            "INSERT INTO sheet_bootstraps("
            "target_binding_id,marker_value,controls_fingerprint,status,verified_at"
            ") VALUES(?,?,?,?,?)",
            (
                target,
                "schema",
                HASH,
                bootstrap_status,
                NOW if bootstrap_status == "ready" else None,
            ),
        )
    return handoff.id, target


def test_unbound_handoff_cannot_bind_during_delivery(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        _, target = _handoff(storage)
        canonical = b'{"unbound":true}'
        canonical_sha = sha256(canonical).hexdigest()
        export_id = f"exp_{canonical_sha[:32]}"
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO generations("
                "id,generation_job_id,attempt,status,content_json"
                ") VALUES(2,1,2,'superseded','{}')"
            )
            connection.execute(
                "INSERT INTO decision_events("
                "id,run_id,event_key,decision,actor"
                ") VALUES(2,1,'approve-unbound','approve','test')"
            )
        insert_sql = (
            "INSERT INTO sheet_handoffs("
            "generation_id,approval_event_id,target_binding_id,export_id,"
            "canonical_bytes,canonical_sha256,approved_at,category,"
            "initial_upload_status,marker_value,status,created_at"
            ") VALUES(2,2,?,?,?,?,?,?,'X',?,'pending',?)"
        )
        insert_values = (
            target,
            export_id,
            canonical,
            canonical_sha,
            NOW,
            "AI",
            f"v1:{export_id}:{canonical_sha}",
            NOW,
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"), storage.transaction() as connection:
            connection.execute(insert_sql, insert_values)
        assert storage.fetch_one("SELECT 1 FROM sheet_handoffs WHERE approval_event_id=2") is None

        storage._connection.execute("PRAGMA foreign_keys=OFF")
        try:
            with storage.transaction() as connection:
                cursor = connection.execute(insert_sql, insert_values)
                unbound_handoff_id = int(cursor.lastrowid)
        finally:
            storage._connection.execute("PRAGMA foreign_keys=ON")

        service = SheetHandoffService(storage)
        assert (
            service.acquire_initial(
                unbound_handoff_id,
                operation_kind="delivery",
                target_binding_id=target,
                now=NOW,
                expires_at=LATER,
            )
            is None
        )
        assert (
            storage.fetch_one(
                "SELECT 1 FROM sheet_handoff_bindings WHERE handoff_id=?",
                (unbound_handoff_id,),
            )
            is None
        )
        assert (
            storage.fetch_one(
                "SELECT 1 FROM sheet_remote_operations WHERE handoff_id=?",
                (unbound_handoff_id,),
            )
            is None
        )
    finally:
        storage.close()


def test_numbered_upgrade_preserves_old_003_history_and_restores_authority(
    tmp_path: Path,
) -> None:
    database = tmp_path / "upgrade.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, target = _handoff(storage)
        service = SheetHandoffService(storage)
        mutate = service.acquire_initial(
            handoff_id,
            operation_kind="delivery",
            now=NOW,
            expires_at=LATER,
        )
        assert mutate is not None
        assert _mark(service, mutate)
        assert service.release_possibly_sent(mutate, now=NOW)

        migration = (Path(__file__).parents[2] / "src" / "newsbot" / "migrations" / "003_sheets_handoff.sql").read_text(
            encoding="utf-8"
        )
        start = migration.index("CREATE TABLE sheet_handoffs (")
        table_sql = migration[start : migration.index("\n);", start) + 3]
        table_sql = table_sql.replace(" target_binding_id INTEGER NOT NULL,\n", "").replace(
            ", FOREIGN KEY(id,target_binding_id) REFERENCES "
            "sheet_handoff_bindings(handoff_id,target_binding_id) "
            "DEFERRABLE INITIALLY DEFERRED",
            "",
        )

        connection = storage._connection
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("PRAGMA legacy_alter_table=ON")
        connection.execute("ALTER TABLE sheet_handoffs RENAME TO sheet_handoffs_current")
        connection.execute(table_sql)
        connection.execute(
            "INSERT INTO sheet_handoffs("
            "id,generation_id,approval_event_id,export_id,canonical_bytes,"
            "canonical_sha256,approved_at,category,initial_upload_status,"
            "marker_value,status,safe_error_code,retry_at,delivered_at,created_at"
            ") SELECT id,generation_id,approval_event_id,export_id,canonical_bytes,"
            "canonical_sha256,approved_at,category,initial_upload_status,"
            "marker_value,status,safe_error_code,retry_at,delivered_at,created_at "
            "FROM sheet_handoffs_current"
        )
        connection.execute("DROP TABLE sheet_handoffs_current")
        connection.execute("PRAGMA legacy_alter_table=OFF")
        connection.execute("DELETE FROM schema_migrations WHERE version='004_sheets_authority_upgrade.sql'")
        connection.commit()
        connection.execute("PRAGMA foreign_keys=ON")
    finally:
        storage.close()

    with Storage.open(database) as upgraded:
        columns = {str(row["name"]) for row in upgraded.fetch_all("PRAGMA table_info(sheet_handoffs)")}
        assert "target_binding_id" in columns
        retained = upgraded.fetch_one(
            "SELECT h.target_binding_id,h.status,o.status AS operation_status "
            "FROM sheet_handoffs h JOIN sheet_remote_operations o "
            "ON o.handoff_id=h.id WHERE h.id=?",
            (handoff_id,),
        )
        assert dict(retained) == {
            "target_binding_id": target,
            "status": "ambiguous",
            "operation_status": "possibly_sent",
        }
        assert upgraded.fetch_all("PRAGMA foreign_key_check") == []

        service = SheetHandoffService(upgraded)
        probe = service.acquire_probe(
            mutate.operation_id,
            expected_fence=1,
            now=NOW,
            expires_at=LATER,
        )
        assert probe is not None
        assert service.record_probe(probe, outcome="exact", now=NOW)
        assert service.finish(probe, outcome="applied", now=NOW)

        with upgraded.transaction() as connection:
            connection.execute(
                "INSERT INTO generations("
                "id,generation_job_id,attempt,status,content_json"
                ") VALUES(2,1,2,'superseded','{}')"
            )
            connection.execute(
                "INSERT INTO decision_events("
                "id,run_id,event_key,decision,actor"
                ") VALUES(2,1,'approve-after-upgrade','approve','test')"
            )
            approved = enqueue_sheet_handoff(
                connection,
                generation_id=2,
                approval_event_id=2,
                target_binding_id=target,
                canonical_bytes=b'{"after_upgrade":true}',
                approved_at=NOW,
                category="AI",
                now=NOW,
            )
        acquired = service.acquire_initial(
            approved.id,
            operation_kind="delivery",
            target_binding_id=target,
            now=NOW,
            expires_at=LATER,
        )
        assert acquired is not None
        assert upgraded.fetch_all("PRAGMA foreign_key_check") == []


def test_export_id_and_identity_are_enforced(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        with storage.transaction() as c, pytest.raises(sqlite3.IntegrityError):
            c.execute(
                "UPDATE sheet_handoffs SET export_id='exp_' || substr(export_id,5,31) || 'Z' WHERE id=?", (handoff_id,)
            )
        with storage.transaction() as c, pytest.raises(sqlite3.IntegrityError):
            c.execute("DELETE FROM sheet_handoffs WHERE id=?", (handoff_id,))
        assert storage.fetch_all("PRAGMA foreign_key_check") == []
    finally:
        storage.close()


def test_authority_upgrade_rolls_back_schema_and_ledger_on_fk_violation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "invalid-upgrade.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, target = _handoff(storage)
        connection = storage._connection
        connection.execute("PRAGMA foreign_keys=OFF")
        target_trigger_sql = str(
            connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='target_no_delete'"
            ).fetchone()[0]
        )
        connection.execute("DROP TRIGGER target_no_delete")
        connection.execute(
            "DELETE FROM sheet_target_bindings WHERE id=?",
            (target,),
        )
        connection.execute(target_trigger_sql)
        connection.execute("DELETE FROM schema_migrations WHERE version='004_sheets_authority_upgrade.sql'")
        connection.commit()
        connection.execute("PRAGMA foreign_keys=ON")
    finally:
        storage.close()

    with pytest.raises(
        RuntimeError,
        match="004_sheets_authority_upgrade.sql introduced foreign key violations",
    ):
        Storage.open(database)

    connection = sqlite3.connect(database)
    try:
        assert connection.execute(
            "SELECT 1 FROM sheet_handoffs WHERE id=?",
            (handoff_id,),
        ).fetchone() == (1,)
        assert (
            connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version='004_sheets_authority_upgrade.sql'"
            ).fetchone()
            is None
        )
        assert connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='handoff_no_delete'"
        ).fetchone() == (1,)
        assert (
            connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sheet_handoffs_v3'").fetchone()
            is None
        )
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="foreign key violations"):
        Storage.open(database)


@pytest.mark.parametrize(
    ("schema_mutation", "object_type", "object_name"),
    (
        ("DROP TRIGGER operation_update", "trigger", "operation_update"),
        (
            "DROP INDEX one_open_remote_operation_per_binding",
            "index",
            "one_open_remote_operation_per_binding",
        ),
    ),
)
def test_authority_upgrade_rejects_unsupported_old_003_fingerprint(
    tmp_path: Path,
    schema_mutation: str,
    object_type: str,
    object_name: str,
) -> None:
    database = tmp_path / "unsupported-upgrade.sqlite"
    storage = Storage.open(database)
    try:
        connection = storage._connection
        connection.execute(schema_mutation)
        connection.execute("DELETE FROM schema_migrations WHERE version='004_sheets_authority_upgrade.sql'")
        connection.commit()
    finally:
        storage.close()

    with pytest.raises(
        RuntimeError,
        match="unsupported applied 003 Sheets authority schema",
    ):
        Storage.open(database)

    connection = sqlite3.connect(database)
    try:
        assert (
            connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version='004_sheets_authority_upgrade.sql'"
            ).fetchone()
            is None
        )
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type=? AND name=?",
                (object_type, object_name),
            ).fetchone()
            is None
        )
        assert (
            connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='sheet_handoffs_v3'").fetchone()
            is None
        )
    finally:
        connection.close()


def test_two_connections_have_one_probe_fence_and_retained_history(tmp_path: Path) -> None:
    database = tmp_path / "handoff.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        mutate = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert mutate is not None
        assert _mark(service, mutate)
        assert service.release_possibly_sent(mutate, now=NOW)
        winner = service.acquire_probe(mutate.operation_id, expected_fence=1, now=NOW, expires_at=LATER)
        assert winner is not None
        other = Storage.open(database)
        try:
            assert (
                SheetHandoffService(other).acquire_probe(
                    mutate.operation_id, expected_fence=1, now=NOW, expires_at=LATER
                )
                is None
            )
        finally:
            other.close()
        assert service.record_probe(winner, outcome="exact", now=NOW)
        assert service.finish(winner, outcome="applied", now=NOW)
        lease = storage.fetch_one("SELECT status FROM sheet_operation_leases WHERE id=?", (mutate.lease_id,))
        assert lease is not None and lease["status"] == "released"
        with storage.transaction() as c, pytest.raises(sqlite3.IntegrityError):
            c.execute("DELETE FROM sheet_operation_events")
    finally:
        storage.close()


@pytest.mark.parametrize("operation_kind", ["delivery", "bootstrap"])
def test_fresh_worker_retires_expired_mutate_lease_and_reconciles(
    tmp_path: Path,
    operation_kind: str,
) -> None:
    database = tmp_path / "handoff.sqlite"
    storage = Storage.open(database)
    if operation_kind == "delivery":
        handoff_id, target = _handoff(storage)
    else:
        service = SheetHandoffService(storage)
        target = service.ensure_binding(
            binding_key="workplace",
            spreadsheet_id="sheet",
            sheet_id=0,
            oracle_fingerprint=HASH,
            now=NOW,
        )
        assert (
            service.ensure_bootstrap(
                target_binding_id=target,
                marker_value="schema",
                controls_fingerprint=HASH,
            )
            == "uninitialized"
        )
        handoff_id = None
    service = SheetHandoffService(storage)
    lease = service.acquire_initial(
        handoff_id,
        operation_kind=operation_kind,  # type: ignore[arg-type]
        target_binding_id=target,
        now=NOW,
        expires_at=LATER,
    )
    assert lease is not None
    assert _mark(service, lease)
    storage.close()

    storage = Storage.open(database)
    try:
        service = SheetHandoffService(storage)
        probe = service.acquire_probe(
            lease.operation_id,
            expected_fence=lease.fence_version,
            now=AFTER,
            expires_at=AFTER_LATER,
        )
        assert probe is not None
        assert dict(
            storage.fetch_one(
                "SELECT status,finish_reason FROM sheet_operation_leases WHERE id=?",
                (lease.lease_id,),
            )
        ) == {
            "status": "expired",
            "finish_reason": "probe_unavailable",
        }
        assert service.record_probe(probe, outcome="exact", now=AFTER)
        assert service.finish(probe, outcome="applied", now=AFTER)
        if operation_kind == "bootstrap":
            assert (
                storage.fetch_one(
                    "SELECT status FROM sheet_bootstraps WHERE target_binding_id=?",
                    (target,),
                )["status"]
                == "ready"
            )
    finally:
        storage.close()


@pytest.mark.parametrize("operation_kind", ["delivery", "bootstrap"])
def test_operator_correction_reopens_only_settled_not_applied_blocker(
    tmp_path: Path,
    operation_kind: str,
) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        if operation_kind == "delivery":
            handoff_id, target = _handoff(storage)
        else:
            service = SheetHandoffService(storage)
            target = service.ensure_binding(
                binding_key="workplace",
                spreadsheet_id="sheet",
                sheet_id=0,
                oracle_fingerprint=HASH,
                now=NOW,
            )
            assert (
                service.ensure_bootstrap(
                    target_binding_id=target,
                    marker_value="schema",
                    controls_fingerprint=HASH,
                )
                == "uninitialized"
            )
            handoff_id = None
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(
            handoff_id,
            operation_kind=operation_kind,  # type: ignore[arg-type]
            target_binding_id=target,
            now=NOW,
            expires_at=LATER,
        )
        assert lease is not None
        assert _mark(service, lease)
        assert service.settle_trusted_rejection(
            lease,
            retryable=False,
            safe_code="permission_denied",
            now=LATER,
        )
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="operator correction invalid"),
        ):
            table = "sheet_handoffs" if operation_kind == "delivery" else "sheet_bootstraps"
            key = "id" if operation_kind == "delivery" else "target_binding_id"
            subject_id = handoff_id if handoff_id is not None else target
            connection.execute(
                f"UPDATE {table} SET status='retryable',retry_at=? WHERE {key}=?",
                (AFTER, subject_id),
            )

        assert service.retry_blocked(lease.operation_id, now=AFTER)
        assert not service.retry_blocked(lease.operation_id, now=AFTER_LATER)
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS total FROM sheet_operation_events "
                "WHERE operation_id=? AND event_kind='operator_corrected'",
                (lease.operation_id,),
            )["total"]
            == 1
        )
        reacquired = service.acquire_initial(
            handoff_id,
            operation_kind=operation_kind,  # type: ignore[arg-type]
            target_binding_id=target,
            now=AFTER,
            expires_at=AFTER_LATER,
        )
        assert reacquired is not None
    finally:
        storage.close()


def test_direct_sql_cannot_forge_event_or_subject_transition(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE sheet_remote_operations SET status='blocked' WHERE id=?",
                (lease.operation_id,),
            )
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="lease finish lacks matching event"),
        ):
            connection.execute(
                "UPDATE sheet_operation_leases "
                "SET status='expired',finished_at=?,finish_reason='probe_unavailable' "
                "WHERE id=?",
                (AFTER, lease.lease_id),
            )
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="handoff transition lacks active lease event"),
        ):
            connection.execute("UPDATE sheet_handoffs SET status='ambiguous' WHERE id=?", (handoff_id,))
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="acquired event lacks owner authority"),
        ):
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?, 'acquired',NULL,?)",
                (lease.operation_id, lease.fence_version, LATER),
            )
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="event lacks current lease authority"),
        ):
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?, 'preflight_exact',NULL,?)",
                (lease.operation_id, lease.fence_version, NOW),
            )
        assert service.record_preflight(lease, outcome="exact", now=NOW)
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="event lacks current lease authority"),
        ):
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?, 'finalized',NULL,?)",
                (lease.operation_id, lease.fence_version, NOW),
            )
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="illegal operation transition"),
        ):
            connection.execute(
                "UPDATE sheet_remote_operations SET status='applied',"
                "certainty='settled_applied',finished_at=?,outcome='applied' WHERE id=?",
                (NOW, lease.operation_id),
            )
        with pytest.raises(  # noqa: SIM117 - commit-time deferred FK
            sqlite3.IntegrityError, match="FOREIGN KEY"
        ):
            with storage.transaction() as connection:
                assert service._owns(connection, lease)
                settlement_id = int(
                    connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?, 'finalized','reused',?)",
                    (lease.operation_id, lease.fence_version, LATER),
                )
                connection.execute(
                    "UPDATE sheet_remote_operations SET status='applied',"
                    "certainty='settled_applied',finished_at=?,outcome='reused',"
                    "settlement_id=? WHERE id=?",
                    (LATER, settlement_id, lease.operation_id),
                )
    finally:
        storage.close()


@pytest.mark.parametrize(
    "case",
    [
        "missing_preflight",
        "exact_preflight",
        "conflict_preflight",
        "oracle_mismatch",
        "controls_mismatch",
        "nonready_bootstrap",
        "future_refresh",
        "short_lived_credential",
        "wrong_scope",
    ],
)
def test_dispatch_requires_complete_fresh_attestation(
    tmp_path: Path,
    case: str,
) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(
            storage,
            bootstrap_status="uninitialized" if case == "nonready_bootstrap" else "ready",
        )
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        if case == "exact_preflight":
            assert service.record_preflight(lease, outcome="exact", now=NOW)
        elif case == "conflict_preflight":
            assert service.record_preflight(lease, outcome="conflict", now=NOW)
        elif case != "missing_preflight":
            assert service.record_preflight(lease, outcome="absent", now=NOW)

        assert not _mark(
            service,
            lease,
            preflight=False,
            oracle_fingerprint="b" * 64 if case == "oracle_mismatch" else HASH,
            controls_fingerprint="b" * 64 if case == "controls_mismatch" else HASH,
            credential_refreshed_at=LATER if case == "future_refresh" else NOW,
            credential_expires_at=(
                "2026-07-30T12:04:00+00:00" if case == "short_lived_credential" else CREDENTIAL_EXPIRES
            ),
            credential_scope_ok=case != "wrong_scope",
        )
        operation = storage.fetch_one(
            "SELECT status,dispatch_at,request_sha256 FROM sheet_remote_operations WHERE id=?",
            (lease.operation_id,),
        )
        assert dict(operation) == {
            "status": "acquired",
            "dispatch_at": None,
            "request_sha256": None,
        }
        assert (
            storage.fetch_one(
                "SELECT COUNT(*) AS total FROM sheet_operation_events "
                "WHERE operation_id=? AND event_kind='dispatch_marked'",
                (lease.operation_id,),
            )["total"]
            == 0
        )
    finally:
        storage.close()


def test_preflight_must_precede_dispatch_and_lease_expiry(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        future_preflight = "2026-07-30T12:04:00+00:00"
        assert service.record_preflight(lease, outcome="absent", now=future_preflight)
        assert not _mark(service, lease, preflight=False, now=NOW)
        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError, match="invalid dispatch event"):
            assert service._owns(connection, lease)
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'dispatch_marked',NULL,?)",
                (lease.operation_id, lease.fence_version, NOW),
            )
        assert (
            storage.fetch_one(
                "SELECT status FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            )["status"]
            == "acquired"
        )

        second_storage = Storage.open(tmp_path / "post-expiry.sqlite")
        try:
            second_handoff, _ = _handoff(second_storage)
            second_service = SheetHandoffService(second_storage)
            second_lease = second_service.acquire_initial(
                second_handoff,
                operation_kind="delivery",
                now=NOW,
                expires_at=LATER,
            )
            assert second_lease is not None
            with (
                second_storage.transaction() as connection,
                pytest.raises(
                    sqlite3.IntegrityError,
                    match="invalid immutable preflight decision",
                ),
            ):
                assert second_service._owns(connection, second_lease)
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?,'preflight_absent',NULL,?)",
                    (second_lease.operation_id, second_lease.fence_version, AFTER),
                )
            assert not second_service.record_preflight(second_lease, outcome="absent", now=AFTER)
            assert (
                second_storage.fetch_one(
                    "SELECT COUNT(*) AS total FROM sheet_operation_events "
                    "WHERE operation_id=? AND event_kind='preflight_absent'",
                    (second_lease.operation_id,),
                )["total"]
                == 0
            )
        finally:
            second_storage.close()
    finally:
        storage.close()


def test_sql_dispatch_attestation_rejects_mismatched_binding(
    tmp_path: Path,
) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert service.record_preflight(lease, outcome="absent", now=NOW)
        preflight = storage.fetch_one(
            "SELECT id,occurred_at FROM sheet_operation_events "
            "WHERE operation_id=? AND fence_version=? AND event_kind='preflight_absent'",
            (lease.operation_id, lease.fence_version),
        )
        assert preflight is not None

        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="dispatch attestation invalid"),
        ):
            assert service._owns(connection, lease)
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'dispatch_marked',NULL,?)",
                (lease.operation_id, lease.fence_version, NOW),
            )
            connection.execute(
                "UPDATE sheet_remote_operations SET status='possibly_sent',"
                "certainty='possibly_sent',dispatch_at=?,diagnostic_probe_at=?,"
                "request_sha256=?,preflight_event_id=?,preflight_fence_version=?,"
                "preflight_at=?,validated_oracle_fingerprint=?,"
                "validated_controls_fingerprint=?,credential_refreshed_at=?,"
                "credential_expires_at=?,credential_scope_ok=1 WHERE id=?",
                (
                    NOW,
                    NOW,
                    HASH,
                    preflight["id"],
                    lease.fence_version,
                    preflight["occurred_at"],
                    "b" * 64,
                    HASH,
                    NOW,
                    CREDENTIAL_EXPIRES,
                    lease.operation_id,
                ),
            )

        assert (
            storage.fetch_one(
                "SELECT status FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            )["status"]
            == "acquired"
        )
    finally:
        storage.close()


def test_valid_dispatch_persists_only_redacted_attestation(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert _mark(service, lease)
        operation = dict(
            storage.fetch_one(
                "SELECT * FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            )
        )
        assert operation["preflight_fence_version"] == lease.fence_version
        assert operation["validated_oracle_fingerprint"] == HASH
        assert operation["validated_controls_fingerprint"] == HASH
        assert operation["credential_refreshed_at"] == NOW
        assert operation["credential_expires_at"] == CREDENTIAL_EXPIRES
        assert operation["credential_scope_ok"] == 1
        sensitive_fragments = ("token", "email", "path", "spreadsheet", "request_body")
        assert not any(fragment in column for column in operation for fragment in sensitive_fragments)
    finally:
        storage.close()


def test_preflight_absent_or_conflict_cannot_settle_as_reused(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert service.record_preflight(lease, outcome="absent", now=NOW)
        assert not service.finish(lease, outcome="reused", now=NOW)
        assert not service.record_preflight(lease, outcome="conflict", now=LATER)
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="invalid immutable preflight decision"),
        ):
            assert service._owns(connection, lease)
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'preflight_conflict',NULL,?)",
                (lease.operation_id, lease.fence_version, LATER),
            )
        assert not service.finish(lease, outcome="reused", now=LATER)
        row = storage.fetch_one(
            "SELECT status,finished_at,settlement_id FROM sheet_remote_operations WHERE id=?",
            (lease.operation_id,),
        )
        assert dict(row) == {
            "status": "acquired",
            "finished_at": None,
            "settlement_id": None,
        }
    finally:
        storage.close()


def test_finalized_evidence_must_equal_settlement_outcome_after_reopen(
    tmp_path: Path,
) -> None:
    database = tmp_path / "handoff.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert service.record_preflight(lease, outcome="conflict", now=NOW)

        with pytest.raises(  # noqa: SIM117 - rollback the whole forged transaction
            sqlite3.IntegrityError, match="settlement evidence outcome mismatch"
        ):
            with storage.transaction() as connection:
                assert service._owns(connection, lease)
                settlement_id = int(
                    connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?, 'finalized','conflicting_metadata',?)",
                    (lease.operation_id, lease.fence_version, LATER),
                )
                connection.execute(
                    "UPDATE sheet_remote_operations SET status='applied',"
                    "certainty='settled_applied',finished_at=?,outcome='reused',"
                    "settlement_id=? WHERE id=?",
                    (LATER, settlement_id, lease.operation_id),
                )
                connection.execute(
                    "UPDATE sheet_handoffs SET status='delivered',delivered_at=?,safe_error_code=NULL WHERE id=?",
                    (LATER, handoff_id),
                )
                connection.execute(
                    "UPDATE sheet_operation_leases SET status='released',"
                    "finished_at=?,finish_reason='finalized' WHERE id=?",
                    (LATER, lease.lease_id),
                )
                connection.execute(
                    "INSERT INTO sheet_operation_settlements("
                    "id,operation_id,fence_version,operation_status,outcome,"
                    "subject_status,lease_status,lease_reason,settled_at)"
                    " VALUES(?,?,?,'applied','reused','delivered','released',"
                    "'finalized',?)",
                    (settlement_id, lease.operation_id, lease.fence_version, LATER),
                )

        expected = {
            "operation_status": "acquired",
            "handoff_status": "delivering",
            "lease_status": "active",
            "settlements": 0,
        }

        def state() -> dict[str, object]:
            return {
                "operation_status": storage.fetch_one(
                    "SELECT status FROM sheet_remote_operations WHERE id=?",
                    (lease.operation_id,),
                )["status"],
                "handoff_status": storage.fetch_one("SELECT status FROM sheet_handoffs WHERE id=?", (handoff_id,))[
                    "status"
                ],
                "lease_status": storage.fetch_one(
                    "SELECT status FROM sheet_operation_leases WHERE id=?",
                    (lease.lease_id,),
                )["status"],
                "settlements": storage.fetch_one("SELECT COUNT(*) AS total FROM sheet_operation_settlements")["total"],
            }

        assert state() == expected
        storage.close()
        storage = Storage.open(database)
        assert state() == expected
    finally:
        storage.close()


@pytest.mark.parametrize(
    ("retryable", "safe_code", "operation_status", "handoff_status"),
    [
        (True, "rate_limited", "rejected_retryable", "retryable"),
        (False, "permission_denied", "rejected_blocked", "blocked"),
    ],
)
def test_trusted_rejection_taxonomy_valid_paths_settle(
    tmp_path: Path,
    retryable: bool,
    safe_code: str,
    operation_status: str,
    handoff_status: str,
) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert _mark(service, lease)
        assert service.settle_trusted_rejection(
            lease,
            retryable=retryable,
            safe_code=safe_code,
            now=LATER,
            retry_at="2026-07-30T13:15:00+01:00" if retryable else None,
        )
        assert dict(
            storage.fetch_one(
                "SELECT status,safe_error_code FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            )
        ) == {
            "status": operation_status,
            "safe_error_code": safe_code,
        }
        assert dict(
            storage.fetch_one(
                "SELECT status,safe_error_code FROM sheet_handoffs WHERE id=?",
                (handoff_id,),
            )
        ) == {
            "status": handoff_status,
            "safe_error_code": safe_code,
        }
        if retryable:
            assert (
                service.acquire_initial(
                    handoff_id,
                    operation_kind="delivery",
                    now=LATER,
                    expires_at=AFTER_LATER,
                )
                is None
            )
            assert (
                service.acquire_initial(
                    handoff_id,
                    operation_kind="delivery",
                    now=AFTER_LATER,
                    expires_at="2026-07-30T15:00:00+00:00",
                )
                is not None
            )
    finally:
        storage.close()


@pytest.mark.parametrize(
    ("safe_code", "operation_status", "subject_status"),
    [
        ("permission_denied", "rejected_retryable", "retryable"),
        ("rate_limited", "rejected_blocked", "blocked"),
    ],
)
def test_trusted_rejection_taxonomy_mismatch_rolls_back(
    tmp_path: Path,
    safe_code: str,
    operation_status: str,
    subject_status: str,
) -> None:
    database = tmp_path / "handoff.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert _mark(service, lease)
        assert not service.settle_trusted_rejection(
            lease,
            retryable=operation_status == "rejected_retryable",
            safe_code=safe_code,
            now=LATER,
        )

        with pytest.raises(  # noqa: SIM117 - rollback the whole forged transaction
            sqlite3.IntegrityError, match="trusted rejection taxonomy mismatch"
        ):
            with storage.transaction() as connection:
                assert service._owns(connection, lease)
                settlement_id = int(
                    connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?, 'trusted_rejection',?,?)",
                    (lease.operation_id, lease.fence_version, safe_code, LATER),
                )
                connection.execute(
                    "UPDATE sheet_remote_operations SET status=?,"
                    "certainty='settled_not_applied',finished_at=?,outcome=?,"
                    "safe_error_code=?,settlement_id=? WHERE id=?",
                    (
                        operation_status,
                        LATER,
                        operation_status,
                        safe_code,
                        settlement_id,
                        lease.operation_id,
                    ),
                )
                connection.execute(
                    "UPDATE sheet_handoffs SET status=?,retry_at=?,safe_error_code=? WHERE id=?",
                    (
                        subject_status,
                        AFTER_LATER if subject_status == "retryable" else None,
                        safe_code,
                        handoff_id,
                    ),
                )
                connection.execute(
                    "UPDATE sheet_operation_leases SET status='released',"
                    "finished_at=?,finish_reason='trusted_rejection' WHERE id=?",
                    (LATER, lease.lease_id),
                )
                connection.execute(
                    "INSERT INTO sheet_operation_settlements("
                    "id,operation_id,fence_version,operation_status,outcome,"
                    "subject_status,lease_status,lease_reason,settled_at)"
                    " VALUES(?,?,?,?,?,?,'released','trusted_rejection',?)",
                    (
                        settlement_id,
                        lease.operation_id,
                        lease.fence_version,
                        operation_status,
                        operation_status,
                        subject_status,
                        LATER,
                    ),
                )

        def state() -> tuple[object, object, object, object]:
            return (
                storage.fetch_one(
                    "SELECT status FROM sheet_remote_operations WHERE id=?",
                    (lease.operation_id,),
                )["status"],
                storage.fetch_one("SELECT status FROM sheet_handoffs WHERE id=?", (handoff_id,))["status"],
                storage.fetch_one(
                    "SELECT status FROM sheet_operation_leases WHERE id=?",
                    (lease.lease_id,),
                )["status"],
                storage.fetch_one("SELECT COUNT(*) AS total FROM sheet_operation_settlements")["total"],
            )

        assert state() == ("possibly_sent", "ambiguous", "active", 0)
        storage.close()
        storage = Storage.open(database)
        assert state() == ("possibly_sent", "ambiguous", "active", 0)
    finally:
        storage.close()


@pytest.mark.parametrize(
    ("event_at", "retry_at", "error"),
    [
        (LATER, NOW, "retry deadline invalid"),
        (LATER, LATER, "retry deadline invalid"),
        (LATER, "malformed", "retry deadline invalid"),
        (LATER, "2026-07-30T13:05:00+01:00", "retry deadline invalid"),
        (LATER, "2026-07-30T12:15:00", "retry deadline invalid"),
        (LATER, "2026-02-30T12:15:00+00:00", "retry deadline invalid"),
        (NOW, AFTER, "finalization lacks matching active lease event"),
    ],
)
def test_retry_deadline_must_be_after_matching_rejection_and_rolls_back(
    tmp_path: Path,
    event_at: str,
    retry_at: str,
    error: str,
) -> None:
    database = tmp_path / "handoff.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert _mark(service, lease)

        with pytest.raises(  # noqa: SIM117 - rollback the whole forged transaction
            sqlite3.IntegrityError, match=error
        ):
            with storage.transaction() as connection:
                assert service._owns(connection, lease)
                settlement_id = int(
                    connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?,'trusted_rejection','rate_limited',?)",
                    (lease.operation_id, lease.fence_version, event_at),
                )
                connection.execute(
                    "UPDATE sheet_remote_operations SET "
                    "status='rejected_retryable',certainty='settled_not_applied',"
                    "finished_at=?,outcome='rejected_retryable',"
                    "safe_error_code='rate_limited',settlement_id=? WHERE id=?",
                    (LATER, settlement_id, lease.operation_id),
                )
                connection.execute(
                    "UPDATE sheet_handoffs SET status='retryable',retry_at=?,safe_error_code='rate_limited' WHERE id=?",
                    (retry_at, handoff_id),
                )
                connection.execute(
                    "UPDATE sheet_operation_leases SET status='released',"
                    "finished_at=?,finish_reason='trusted_rejection' WHERE id=?",
                    (LATER, lease.lease_id),
                )
                connection.execute(
                    "INSERT INTO sheet_operation_settlements("
                    "id,operation_id,fence_version,operation_status,outcome,"
                    "subject_status,lease_status,lease_reason,settled_at)"
                    " VALUES(?,?,?,'rejected_retryable','rejected_retryable',"
                    "'retryable','released','trusted_rejection',?)",
                    (settlement_id, lease.operation_id, lease.fence_version, LATER),
                )

        def state() -> tuple[object, object, object, object]:
            return (
                storage.fetch_one(
                    "SELECT status FROM sheet_remote_operations WHERE id=?",
                    (lease.operation_id,),
                )["status"],
                storage.fetch_one("SELECT status FROM sheet_handoffs WHERE id=?", (handoff_id,))["status"],
                storage.fetch_one(
                    "SELECT status FROM sheet_operation_leases WHERE id=?",
                    (lease.lease_id,),
                )["status"],
                storage.fetch_one("SELECT COUNT(*) AS total FROM sheet_operation_settlements")["total"],
            )

        assert state() == ("possibly_sent", "ambiguous", "active", 0)
        storage.close()
        storage = Storage.open(database)
        assert state() == ("possibly_sent", "ambiguous", "active", 0)
    finally:
        storage.close()


def test_settlement_rejects_self_consistent_wrong_outcome_matrix(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert lease is not None
        assert _mark(service, lease)
        assert service.record_probe(lease, outcome="exact", now=NOW)

        with pytest.raises(  # noqa: SIM117 - rollback the whole forged transaction
            sqlite3.IntegrityError, match="settlement outcome mismatch"
        ):
            with storage.transaction() as connection:
                assert service._owns(connection, lease)
                settlement_id = int(
                    connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM sheet_operation_settlements").fetchone()[0]
                )
                connection.execute(
                    "INSERT INTO sheet_operation_events("
                    "operation_id,fence_version,event_kind,safe_code,occurred_at"
                    ") VALUES(?,?, 'finalized','applied',?)",
                    (lease.operation_id, lease.fence_version, LATER),
                )
                connection.execute(
                    "UPDATE sheet_remote_operations SET status='applied',"
                    "certainty='settled_applied',finished_at=?,outcome='applied',"
                    "settlement_id=? WHERE id=?",
                    (LATER, settlement_id, lease.operation_id),
                )
                connection.execute(
                    "UPDATE sheet_handoffs SET status='blocked',safe_error_code='schema_conflict' WHERE id=?",
                    (handoff_id,),
                )
                connection.execute(
                    "UPDATE sheet_operation_leases SET status='released',"
                    "finished_at=?,finish_reason='finalized' WHERE id=?",
                    (LATER, lease.lease_id),
                )
                connection.execute(
                    "INSERT INTO sheet_operation_settlements("
                    "id,operation_id,fence_version,operation_status,outcome,"
                    "subject_status,lease_status,lease_reason,settled_at)"
                    " VALUES(?,?,?,'applied','applied','blocked','released',"
                    "'finalized',?)",
                    (settlement_id, lease.operation_id, lease.fence_version, LATER),
                )

        assert (
            storage.fetch_one(
                "SELECT status FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            )["status"]
            == "possibly_sent"
        )
        assert storage.fetch_one("SELECT status FROM sheet_handoffs WHERE id=?", (handoff_id,))["status"] == "ambiguous"
    finally:
        storage.close()


def test_stale_owner_probe_and_absence_never_restore_retryability(tmp_path: Path) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        mutate = service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER)
        assert mutate is not None
        assert service.record_preflight(mutate, outcome="absent", now=NOW)
        assert not _mark(
            service,
            replace(mutate, owner_token="not-the-owner"),
            preflight=False,
        )
        assert _mark(service, mutate, preflight=False)
        assert not service.settle_trusted_rejection(
            mutate,
            retryable=True,
            safe_code="credential text must never persist",
            now=NOW,
        )
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="probe lacks current owner event"),
        ):
            connection.execute(
                "INSERT INTO sheet_operation_probes("
                "operation_id,fence_version,result,matching_marker_count,observed_at"
                ") VALUES(?,?,'absent',0,?)",
                (mutate.operation_id, mutate.fence_version, NOW),
            )
        assert service.release_possibly_sent(mutate, now=NOW)
        with (
            storage.transaction() as connection,
            pytest.raises(sqlite3.IntegrityError, match="lease lacks owner authority"),
        ):
            connection.execute(
                "INSERT INTO sheet_operation_leases("
                "operation_id,fence_version,owner_token_hash,lease_mode,"
                "acquired_at,expires_at,status"
                ") VALUES(?,2,?,'probe',?,?,'active')",
                (mutate.operation_id, "a" * 64, NOW, LATER),
            )
        probe = service.acquire_probe(mutate.operation_id, expected_fence=1, now=NOW, expires_at=LATER)
        assert probe is not None
        assert service.record_probe(probe, outcome="absent", now=NOW)
        assert not service.settle_trusted_rejection(probe, retryable=True, safe_code="rateLimitExceeded", now=NOW)
        assert service.release_probe_unresolved(probe, outcome="absent", now=NOW)
        assert service.acquire_initial(handoff_id, operation_kind="delivery", now=NOW, expires_at=LATER) is None
        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError, match="retry deadline invalid"):
            connection.execute(
                "UPDATE sheet_handoffs SET status='retryable',retry_at=? WHERE id=? AND status='ambiguous'",
                (NOW, handoff_id),
            )
    finally:
        storage.close()


def test_fence_only_commit_rolls_back_and_reopens_exactly(tmp_path: Path) -> None:
    database = tmp_path / "handoff.sqlite"
    storage = Storage.open(database)
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        mutate = service.acquire_initial(
            handoff_id,
            operation_kind="delivery",
            now=NOW,
            expires_at=LATER,
        )
        assert mutate is not None
        assert _mark(service, mutate)
        assert service.release_possibly_sent(mutate, now=NOW)
        before = dict(
            storage.fetch_one(
                "SELECT * FROM sheet_remote_operations WHERE id=?",
                (mutate.operation_id,),
            )
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"), storage.transaction() as connection:
            connection.execute(
                "UPDATE sheet_remote_operations "
                "SET last_fence_version=last_fence_version+1,"
                "current_acquired_event_id=999999,current_lease_id=999999 "
                "WHERE id=?",
                (mutate.operation_id,),
            )
        assert (
            dict(
                storage.fetch_one(
                    "SELECT * FROM sheet_remote_operations WHERE id=?",
                    (mutate.operation_id,),
                )
            )
            == before
        )
        storage.close()
        storage = Storage.open(database)
        assert (
            dict(
                storage.fetch_one(
                    "SELECT * FROM sheet_remote_operations WHERE id=?",
                    (mutate.operation_id,),
                )
            )
            == before
        )
    finally:
        storage.close()


def test_expired_pre_marker_lease_is_abandoned_before_reacquisition(
    tmp_path: Path,
) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        first = service.acquire_initial(
            handoff_id,
            operation_kind="delivery",
            now=NOW,
            expires_at=LATER,
        )
        assert first is not None
        assert not service.record_preflight(first, outcome="absent", now=AFTER)
        assert not _mark(service, first, now=AFTER, preflight=False)

        second = service.acquire_initial(
            handoff_id,
            operation_kind="delivery",
            now=AFTER,
            expires_at=AFTER_LATER,
        )

        assert second is not None
        assert second.operation_id != first.operation_id
        operation = storage.fetch_one(
            "SELECT status,certainty,outcome FROM sheet_remote_operations WHERE id=?",
            (first.operation_id,),
        )
        assert dict(operation) == {
            "status": "abandoned_pre_marker",
            "certainty": "not_dispatched",
            "outcome": "abandoned_pre_marker",
        }
        lease = storage.fetch_one(
            "SELECT status,finish_reason FROM sheet_operation_leases WHERE id=?",
            (first.lease_id,),
        )
        assert dict(lease) == {
            "status": "expired",
            "finish_reason": "lease_expired_pre_marker",
        }
    finally:
        storage.close()


def test_offset_lease_expiry_blocks_service_and_direct_sql_dispatch(
    tmp_path: Path,
) -> None:
    storage = Storage.open(tmp_path / "handoff.sqlite")
    try:
        handoff_id, _ = _handoff(storage)
        service = SheetHandoffService(storage)
        lease = service.acquire_initial(
            handoff_id,
            operation_kind="delivery",
            now=NOW,
            expires_at="2026-07-30T13:05:00+01:00",
        )
        assert lease is not None

        assert service.record_preflight(lease, outcome="absent", now=NOW)
        after_expiry = "2026-07-30T12:06:00+00:00"
        with storage.transaction() as connection, pytest.raises(sqlite3.IntegrityError, match="invalid dispatch event"):
            assert service._owns(connection, lease)
            connection.execute(
                "INSERT INTO sheet_operation_events("
                "operation_id,fence_version,event_kind,safe_code,occurred_at"
                ") VALUES(?,?,'dispatch_marked',NULL,?)",
                (lease.operation_id, lease.fence_version, after_expiry),
            )

        assert not _mark(
            service,
            lease,
            now=after_expiry,
            preflight=False,
        )
        assert (
            storage.fetch_one(
                "SELECT status FROM sheet_remote_operations WHERE id=?",
                (lease.operation_id,),
            )["status"]
            == "abandoned_pre_marker"
        )
    finally:
        storage.close()

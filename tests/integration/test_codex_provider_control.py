import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot.cli import _control_operation
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)


def _job(storage: Storage, index: int, *, status: str = "queued") -> int:
    with storage.transaction() as db:
        db.execute("INSERT INTO runs(run_key, mode, status) VALUES (?, 'fixture', 'running')", (f"codex-{index}",))
        run = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute("INSERT INTO source_posts(channel_id, external_post_id) VALUES (?, ?)", (f"c{index}", str(index)))
        post = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', 'source')", (post,)
        )
        source = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score) VALUES (?, ?, 'v1', '1.000000')",
            (run, source),
        )
        evaluation = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'selected_generation_pending', 1)",
            (evaluation,),
        )
        candidate = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)", (candidate, source)
        )
        db.execute("INSERT INTO digests(run_id, digest_key, status) VALUES (?, ?, 'selected')", (run, f"codex-{index}"))
        digest = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute("INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest, candidate))
        selection = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) VALUES (?, 'initial', ?, 2)",
            (selection, status),
        )
        return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


def _pipeline(storage: Storage) -> NewsPipeline:
    return NewsPipeline(storage, SimpleNamespace(), lambda: None, FixtureClock(NOW))


def _bind(storage: Storage, job_id: int) -> None:
    with storage.transaction() as db:
        db.execute(
            "INSERT INTO generation_job_provider_bindings(generation_job_id, provider_name) VALUES (?, 'codex_cli')",
            (job_id,),
        )


def test_migration_005_fresh_schema_enforces_provider_fk_and_immutable_audit() -> None:
    storage = Storage.open(":memory:")
    job_id = _job(storage, 1)
    assert (
        storage.fetch_one("SELECT version FROM schema_migrations WHERE version='005_generation_provider_retry.sql'")
        is not None
    )
    with storage.transaction() as db:
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO generation_job_provider_bindings(generation_job_id, provider_name) VALUES (?, 'fake')",
                (job_id,),
            )
        db.execute(
            "INSERT INTO generation_job_provider_bindings(generation_job_id, provider_name) VALUES (?, 'codex_cli')",
            (job_id,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO generation_job_provider_bindings(generation_job_id, provider_name) VALUES (?, 'codex_cli')",
                (job_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute(
                "UPDATE generation_job_provider_bindings SET provider_name='codex_cli' WHERE generation_job_id=?",
                (job_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO generation_job_retry_events(generation_job_id, action, reason_code, actor_kind, actor_id, resulting_held, resulting_consecutive_failures, previous_retry_version, resulting_retry_version) VALUES (?, 'hold', 'codex_busy', 'system', 1, 1, 1, 1, 2)",
                (job_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO generation_job_retry_events(generation_job_id, action, reason_code, actor_kind, resulting_held, resulting_consecutive_failures, previous_retry_version, resulting_retry_version) VALUES (999, 'hold', 'codex_busy', 'system', 1, 1, 1, 1)"
            )


def test_selector_binds_one_unbound_and_prioritizes_expired_due_then_queued() -> None:
    storage = Storage.open(":memory:")
    queued = _job(storage, 1)
    due = _job(storage, 2, status="failed_recoverable")
    expired = _job(storage, 3, status="running")
    for job in (due, expired):
        _bind(storage, job)
    with storage.transaction() as db:
        db.execute("UPDATE generation_jobs SET retry_at=? WHERE id=?", ((NOW - timedelta(seconds=1)).isoformat(), due))
        db.execute(
            "UPDATE generation_jobs SET lease_expires_at=? WHERE id=?",
            ((NOW - timedelta(seconds=1)).isoformat(), expired),
        )
    pipeline = _pipeline(storage)
    assert pipeline.select_codex_job_id() == expired
    assert pipeline.select_codex_job_id() == expired
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_jobs SET status='failed_recoverable', lease_expires_at=NULL, retry_at=? WHERE id=?",
            ((NOW + timedelta(hours=1)).isoformat(), expired),
        )
    assert pipeline.select_codex_job_id() == due
    with storage.transaction() as db:
        db.execute("UPDATE generation_jobs SET retry_at=? WHERE id=?", ((NOW + timedelta(hours=1)).isoformat(), due))
    assert pipeline.select_codex_job_id() == queued
    assert (
        storage.fetch_one(
            "SELECT provider_name FROM generation_job_provider_bindings WHERE generation_job_id=?", (queued,)
        )["provider_name"]
        == "codex_cli"
    )


def test_production_selector_rejects_drift_before_provider_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from newsbot.automation import AutomationAuthority, AutomationDriftError

    storage = Storage.open(":memory:")
    job = _job(storage, 1)

    def reject_topology(*_args: object, **_kwargs: object) -> None:
        raise AutomationDriftError("runtime automation topology drifted")

    monkeypatch.setattr(AutomationAuthority, "validate_active_topology", reject_topology)

    with pytest.raises(AutomationDriftError, match="topology drifted"):
        _pipeline(storage).select_codex_job_id(production_config=SimpleNamespace())

    assert (
        storage.fetch_one(
            "SELECT 1 FROM generation_job_provider_bindings WHERE generation_job_id=?",
            (job,),
        )
        is None
    )
    assert storage.fetch_one("SELECT status FROM generation_jobs WHERE id=?", (job,))["status"] == "queued"
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM generation_provider_attempts")["n"] == 0

def test_selector_blocks_pause_hold_and_no_due_job() -> None:
    storage = Storage.open(":memory:")
    job = _job(storage, 1, status="failed_recoverable")
    _bind(storage, job)
    pipeline = _pipeline(storage)
    with storage.transaction() as db:
        db.execute("UPDATE generation_jobs SET retry_at=? WHERE id=?", ((NOW + timedelta(hours=1)).isoformat(), job))
    assert pipeline.select_codex_job_id() is None
    with storage.transaction() as db:
        db.execute("UPDATE generation_jobs SET status='queued' WHERE id=?", (job,))
        db.execute(
            "INSERT INTO generation_job_retry_state(generation_job_id, held_at, hold_reason_code) VALUES (?, ?, 'operator_review')",
            (job, NOW.isoformat()),
        )
    assert pipeline.select_codex_job_id() is None
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code='maintenance', control_version=2 WHERE provider_name='codex_cli'",
            (NOW.isoformat(),),
        )
    assert pipeline.select_codex_job_id() is None


def test_resume_count_is_immutable_provider_resumed_events_not_a_projection_column(
    tmp_path, monkeypatch, capsys
) -> None:
    path = tmp_path / "newsbot.db"
    storage = Storage.open(path)
    job = _job(storage, 1, status="failed_recoverable")
    _bind(storage, job)
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code='maintenance', control_version=2 WHERE provider_name='codex_cli'",
            (NOW.isoformat(),),
        )
        db.execute(
            "INSERT INTO generation_provider_control_events(operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, previous_control_version, resulting_control_version, control_version) VALUES ('cxo_00000000000000000000000000000001', 'codex_cli', 'pause', 'maintenance', 'operator', 7, 1, 1, 2, 2)"
        )
        db.execute(
            "INSERT INTO generation_job_retry_state(generation_job_id, blocked_by_control_version, blocked_by_safe_code) VALUES (?, 2, 'codex_runner_config')",
            (job,),
        )
    args = SimpleNamespace(actor_id=9, reason_code="maintenance_complete", expected_control_version=2, database=path)
    monkeypatch.setattr("newsbot.cli._database", lambda _args: path)
    assert _control_operation(args, "resume") == 0
    output = capsys.readouterr().out
    assert '"affected_job_count": 0' in output
    assert "affected_job_count" not in {
        row["name"] for row in storage.fetch_all("PRAGMA table_info(generation_provider_controls)")
    }


def test_manual_compatible_resume_releases_only_matching_jobs_and_replay_counts_events(
    tmp_path, monkeypatch, capsys
) -> None:
    path = tmp_path / "control.db"
    storage = Storage.open(path)
    matching = _job(storage, 1, status="failed_recoverable")
    incompatible = _job(storage, 2, status="failed_recoverable")
    for job in (matching, incompatible):
        _bind(storage, job)
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code='maintenance', control_version=2 WHERE provider_name='codex_cli'",
            (NOW.isoformat(),),
        )
        db.execute(
            "INSERT INTO generation_provider_control_events(operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, previous_control_version, resulting_control_version, control_version) VALUES ('cxo_00000000000000000000000000000002', 'codex_cli', 'pause', 'maintenance', 'operator', 7, 1, 1, 2, 2)"
        )
    monkeypatch.setattr("newsbot.cli._database", lambda _args: path)
    args = SimpleNamespace(actor_id=9, reason_code="maintenance_complete", expected_control_version=2)
    assert _control_operation(args, "resume") == 0
    output = capsys.readouterr().out
    assert '"affected_job_count": 0' in output
    assert storage.fetch_one("SELECT retry_at FROM generation_jobs WHERE id=?", (matching,))["retry_at"] is None
    assert storage.fetch_one("SELECT retry_at FROM generation_jobs WHERE id=?", (incompatible,))["retry_at"] is None


def test_control_event_is_immutable_and_bad_provider_actor_or_version_is_rejected() -> None:
    storage = Storage.open(":memory:")
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code='maintenance', control_version=2 WHERE provider_name='codex_cli'",
            (NOW.isoformat(),),
        )
        db.execute(
            "INSERT INTO generation_provider_control_events(operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, previous_control_version, resulting_control_version, control_version) VALUES ('cxo_00000000000000000000000000000003', 'codex_cli', 'pause', 'maintenance', 'operator', 3, 1, 1, 2, 2)"
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            db.execute("UPDATE generation_provider_control_events SET reason_code='maintenance' WHERE id=1")
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO generation_provider_control_events(operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, previous_control_version, resulting_control_version, control_version) VALUES ('cxo_00000000000000000000000000000004', 'fake', 'pause', 'maintenance', 'operator', 3, 1, 2, 3, 3)"
            )
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO generation_provider_control_events(operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, previous_control_version, resulting_control_version, control_version) VALUES ('cxo_00000000000000000000000000000005', 'codex_cli', 'resume', 'maintenance_complete', 'system', 3, 0, 2, 3, 3)"
            )


def test_compatible_resume_releases_same_control_version_and_exact_replay_uses_event_count(
    tmp_path, monkeypatch, capsys
) -> None:
    path = tmp_path / "compatible.db"
    storage = Storage.open(path)
    job = _job(storage, 3, status="failed_recoverable")
    _bind(storage, job)
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code='codex_runner_config', control_version=2 WHERE provider_name='codex_cli'",
            (NOW.isoformat(),),
        )
        db.execute(
            "INSERT INTO generation_provider_control_events(operation_id, provider_name, action, reason_code, actor_kind, resulting_paused, previous_control_version, resulting_control_version, control_version) VALUES ('cxo_00000000000000000000000000000006', 'codex_cli', 'pause', 'codex_runner_config', 'system', 1, 1, 2, 2)"
        )
        db.execute(
            "INSERT INTO generation_job_retry_state(generation_job_id, blocked_by_control_version, blocked_by_safe_code) VALUES (?, 2, 'codex_runner_config')",
            (job,),
        )
    monkeypatch.setattr("newsbot.cli._database", lambda _args: path)
    args = SimpleNamespace(actor_id=9, reason_code="config_repaired", expected_control_version=2)
    assert _control_operation(args, "resume") == 0
    assert '"affected_job_count": 1' in capsys.readouterr().out
    assert storage.fetch_one("SELECT retry_at FROM generation_jobs WHERE id=?", (job,))["retry_at"] is not None
    assert _control_operation(args, "resume") == 0
    assert '"affected_job_count": 1' in capsys.readouterr().out

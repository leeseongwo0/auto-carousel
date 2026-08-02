"""Redacted local status and inspection views for the news workflow."""

from __future__ import annotations

from typing import Any

from .storage import Storage


def status(storage: Storage) -> dict[str, int | bool]:
    """Return aggregate counters; no source text, credentials, tokens, or paths."""
    result: dict[str, int | bool] = {
        "runs": _count(storage, "runs"),
        "candidates": _count(storage, "candidates"),
        "selected": _count(storage, "candidates", "status = 'selected_generation_pending'"),
        "approved": _count(storage, "candidates", "status = 'approved'"),
        "generations": _count(storage, "generations", "status = 'current'"),
        "sheet_handoffs": _count(storage, "sheet_handoffs"),
        "sheet_delivered": _count(storage, "sheet_handoffs", "status = 'delivered'"),
        "sheet_ambiguous": _count(storage, "sheet_handoffs", "status = 'ambiguous'"),
        "legacy_local_exports": _ready_export_pairs(storage),
        "provider_calls": _provider_calls(storage),
        "provider_attempts": _count(storage, "generation_provider_attempts"),
        "provider_calls_before_selection": _provider_calls_before_selection(storage),
        "codex_paused": _count(
            storage, "generation_provider_controls", "provider_name = 'codex_cli' AND paused_at IS NOT NULL"
        ),
        "codex_held": _count(storage, "generation_job_retry_state", "held_at IS NOT NULL"),
        "codex_safe_code_events": _count(storage, "generation_provider_attempt_classifications"),
        "codex_control_events": _count(storage, "generation_provider_control_events"),
        "codex_retry_events": _count(storage, "generation_job_retry_events"),
    }
    result.update(automation_status(storage))
    return result


def inspect(storage: Storage, run_id: int) -> dict[str, Any]:
    """Return one run's redacted state and counts, never persisted payloads."""
    run = storage.fetch_one(
        "SELECT id, run_key, mode, status, started_at, finished_at FROM runs WHERE id = ?", (run_id,)
    )
    if run is None:
        raise LookupError(f"run {run_id} does not exist")
    return {
        "run": {key: run[key] for key in ("id", "run_key", "mode", "status", "started_at", "finished_at")},
        "candidates": _count(storage, "candidate_evaluations", "run_id = ?", (run_id,)),
        "approvals": _count(storage, "decision_events", "run_id = ? AND decision = 'approve_handoff'", (run_id,)),
        "sheet_handoffs": _run_sheet_handoffs(storage, run_id),
        "legacy_local_exports": _ready_export_pairs(storage, run_id),
        "provider_calls": _provider_calls(storage, run_id),
        "provider_attempts": _provider_attempts(storage, run_id),
        "provider_calls_before_selection": _provider_calls_before_selection(storage, run_id),
    }


def automation_status(storage: Storage) -> dict[str, int | bool]:
    """Return migration-007 aggregate health without exposing authority identity."""
    active = _count(storage, "automation_cutovers") == 1
    return {
        "automation_active": active,
        "automation_open_leases": _count(storage, "automation_stream_leases"),
        "automation_open_runs": _count(storage, "automation_stream_runs", "finished_at IS NULL"),
        "automation_pending_notifications": _count(
            storage, "telegram_notification_outbox", "state IN ('pending','claimed','sending')"
        ),
        "automation_ambiguous_notifications": _count(
            storage, "telegram_notification_outbox", "state IN ('ambiguous','partial_manual_required')"
        ),
        "automation_resolved_notifications": _count(
            storage, "telegram_notification_outbox", "state IN ('resolved_delivered','resolved_abandoned')"
        ),
        "automation_postbaseline_handoffs": _count(
            storage,
            "sheet_handoffs",
            "id > COALESCE((SELECT baseline_handoff_id FROM automation_cutovers WHERE id=1),"
            "9223372036854775807) AND status IN ('pending','retryable','delivering','ambiguous')",
        ),
    }


def _count(storage: Storage, table: str, condition: str = "", parameters: tuple[object, ...] = ()) -> int:
    suffix = f" WHERE {condition}" if condition else ""
    row = storage.fetch_one(f"SELECT COUNT(*) AS count FROM {table}{suffix}", parameters)
    assert row is not None
    return int(row["count"])


def _ready_export_pairs(storage: Storage, run_id: int | None = None) -> int:
    condition = "WHERE outbox.status='ready'"
    parameters: tuple[object, ...] = ()
    if run_id is not None:
        condition += " AND digest.run_id=?"
        parameters = (run_id,)
    row = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM ("
        "SELECT outbox.export_id FROM export_outbox outbox "
        "JOIN digests digest ON digest.id=outbox.digest_id "
        f"{condition} GROUP BY outbox.export_id "
        "HAVING COUNT(DISTINCT outbox.export_kind)=2"
        ")",
        parameters,
    )
    assert row is not None
    return int(row["count"])


def _run_sheet_handoffs(storage: Storage, run_id: int) -> int:
    row = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM sheet_handoffs handoff "
        "JOIN generations generation ON generation.id=handoff.generation_id "
        "JOIN generation_jobs job ON job.id=generation.generation_job_id "
        "JOIN selections selection ON selection.id=job.selection_id "
        "JOIN candidates candidate ON candidate.id=selection.candidate_id "
        "JOIN candidate_evaluations evaluation ON evaluation.id=candidate.evaluation_id "
        "WHERE evaluation.run_id=?",
        (run_id,),
    )
    assert row is not None
    return int(row["count"])


def _provider_calls(storage: Storage, run_id: int | None = None) -> int:
    condition = "event_kind = 'provider_call'"
    parameters: tuple[object, ...] = ()
    if run_id is not None:
        condition += " AND run_id = ?"
        parameters = (run_id,)
    return _count(storage, "pipeline_events", condition, parameters)


def _provider_attempts(storage: Storage, run_id: int | None = None) -> int:
    if run_id is None:
        return _count(storage, "generation_provider_attempts")
    row = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM generation_provider_attempts attempt "
        "JOIN generation_jobs job ON job.id=attempt.generation_job_id "
        "JOIN selections selection ON selection.id=job.selection_id "
        "JOIN candidates candidate ON candidate.id=selection.candidate_id "
        "JOIN candidate_evaluations evaluation ON evaluation.id=candidate.evaluation_id "
        "WHERE evaluation.run_id=?",
        (run_id,),
    )
    assert row is not None
    return int(row["count"])


def _provider_calls_before_selection(storage: Storage, run_id: int | None = None) -> int:
    condition = "event.event_kind = 'provider_call'"
    parameters: tuple[object, ...] = ()
    if run_id is not None:
        condition += " AND event.run_id = ?"
        parameters = (run_id,)
    row = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM pipeline_events event "
        "LEFT JOIN selections selection ON selection.id=event.selection_id "
        "LEFT JOIN generation_jobs job ON job.id=event.generation_job_id "
        "LEFT JOIN candidates candidate ON candidate.id=event.candidate_id "
        "LEFT JOIN candidate_evaluations evaluation ON evaluation.id=candidate.evaluation_id "
        f"WHERE {condition} AND ("
        "selection.id IS NULL OR job.id IS NULL OR candidate.id IS NULL OR evaluation.id IS NULL "
        "OR selection.candidate_id != event.candidate_id "
        "OR job.selection_id != event.selection_id "
        "OR evaluation.run_id != event.run_id "
        ")",
        parameters,
    )
    assert row is not None
    return int(row["count"])

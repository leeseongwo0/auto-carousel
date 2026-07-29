"""Redacted local status and inspection views for the news workflow."""

from __future__ import annotations

from typing import Any

from .storage import Storage


def status(storage: Storage) -> dict[str, int]:
    """Return aggregate counters; no source text, credentials, tokens, or paths."""
    return {
        "runs": _count(storage, "runs"),
        "candidates": _count(storage, "candidates"),
        "selected": _count(storage, "candidates", "status = 'selected_generation_pending'"),
        "approved": _count(storage, "candidates", "status = 'approved'"),
        "generations": _count(storage, "generations", "status = 'current'"),
        "ready_exports": _ready_export_pairs(storage),
        "provider_calls": _provider_calls(storage),
        "provider_attempts": _count(storage, "generation_provider_attempts"),
        "provider_calls_before_selection": _provider_calls_before_selection(storage),
    }


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
        "ready_exports": _ready_export_pairs(storage, run_id),
        "provider_calls": _provider_calls(storage, run_id),
        "provider_attempts": _provider_attempts(storage, run_id),
        "provider_calls_before_selection": _provider_calls_before_selection(storage, run_id),
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

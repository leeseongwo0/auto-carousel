"""Redacted local status and inspection views for the news workflow."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

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


def automation_status(storage: Storage, *, now: datetime | None = None) -> dict[str, int | bool]:
    """Return redacted automation and hourly-news aggregate health."""
    sampled_now = now or datetime.now(ZoneInfo("Asia/Seoul"))
    if sampled_now.tzinfo is None or sampled_now.utcoffset() is None:
        raise ValueError("automation status time must be timezone-aware")
    local = sampled_now.astimezone(ZoneInfo("Asia/Seoul"))
    active = _count(storage, "automation_cutovers") == 1
    current_binding_count = _current_config_binding_count(storage)
    collecting_windows = _count(storage, "ambiguous_digest_windows", "state = 'collecting'")
    collecting_open, collecting_stale = _collecting_window_health(storage, local)
    return {
        "automation_active": active,
        "news_policy_definite": _count(
            storage, "news_policy_evaluations", "outcome = 'definite_news'"
        ),
        "news_policy_trusted_analysis": _count(
            storage, "news_policy_evaluations", "outcome = 'trusted_analysis'"
        ),
        "news_policy_ambiguous": _count(
            storage, "news_policy_evaluations", "outcome = 'ambiguous'"
        ),
        "news_policy_non_news": _count(
            storage, "news_policy_evaluations", "outcome = 'non_news'"
        ),
        "news_policy_config_bindings": _count(storage, "automation_release_config_bindings"),
        "news_policy_current_config_binding_present": current_binding_count == 1,
        "news_policy_current_config_binding_count": current_binding_count,
        "noon_windows_collecting": collecting_windows,
        "noon_windows_collecting_open": collecting_open,
        "noon_windows_collecting_stale": collecting_stale,
        "noon_windows_queued": _count(
            storage, "ambiguous_digest_windows", "state = 'queued'"
        ),
        "noon_windows_empty": _count(
            storage, "ambiguous_digest_windows", "state = 'empty'"
        ),
        "noon_windows_skipped": _count(
            storage, "ambiguous_digest_windows", "state = 'skipped'"
        ),
        "noon_on_time_intent_commits": _count_on_time_noon_intents(storage),
        "noon_pending_notifications": _count(
            storage,
            "telegram_notification_outbox",
            "notification_kind = 'noon_digest' AND state IN ('pending','claimed','sending')",
        ),
        "noon_ambiguous_notifications": _count(
            storage,
            "telegram_notification_outbox",
            "notification_kind = 'noon_digest' AND state = 'ambiguous'",
        ),
        "noon_partial_manual_notifications": _count(
            storage,
            "telegram_notification_outbox",
            "notification_kind = 'noon_digest' AND state = 'partial_manual_required'",
        ),
        "noon_manual_notifications": _count(
            storage,
            "telegram_notification_outbox",
            "notification_kind = 'noon_digest' AND state IN ('ambiguous','partial_manual_required')",
        ),
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


def _current_config_binding_count(storage: Storage) -> int:
    row = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM automation_release_config_bindings binding "
        "WHERE binding.activation_id = ("
        "SELECT MAX(id) FROM automation_release_activations WHERE cutover_id=1"
        ")"
    )
    assert row is not None
    return int(row["count"])


def _count_on_time_noon_intents(storage: Storage) -> int:
    row = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM telegram_notification_outbox outbox "
        "JOIN ambiguous_digest_windows window ON window.id=outbox.ambiguous_window_id "
        "WHERE outbox.notification_kind='noon_digest' "
        "AND aware_epoch_us(outbox.created_at) >= aware_epoch_us(window.opens_at) "
        "AND aware_epoch_us(outbox.created_at) < aware_epoch_us(window.closes_at)"
    )
    assert row is not None
    return int(row["count"])
def _collecting_window_health(storage: Storage, local: datetime) -> tuple[int, int]:
    today = local.date().isoformat()
    open_condition = (
        "state = 'collecting' AND (scheduled_local_date > ? "
        "OR (scheduled_local_date = ? AND ? < 13))"
    )
    stale_condition = (
        "state = 'collecting' AND (scheduled_local_date < ? "
        "OR (scheduled_local_date = ? AND ? >= 13))"
    )
    return (
        _count(storage, "ambiguous_digest_windows", open_condition, (today, today, local.hour)),
        _count(storage, "ambiguous_digest_windows", stale_condition, (today, today, local.hour)),
    )



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

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot.ai.codex_cli import (
    CodexAuthUnavailableError,
    CodexBusyError,
    CodexInputLimitError,
    CodexNonzeroError,
    CodexTimeoutError,
)
from newsbot.ai.fake import FakeGenerationProvider
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage

NOW = datetime(2026, 7, 31, 12, tzinfo=UTC)


def _job(storage: Storage, index: int, *, status: str = "queued", page_count: int | None = 2) -> int:
    with storage.transaction() as db:
        db.execute("INSERT INTO runs(run_key, mode, status) VALUES (?, 'fixture', 'running')", (f"exact-{index}",))
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
        db.execute("INSERT INTO digests(run_id, digest_key, status) VALUES (?, ?, 'selected')", (run, f"exact-{index}"))
        digest = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute("INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest, candidate))
        selection = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) VALUES (?, 'initial', ?, ?)",
            (selection, status, page_count),
        )
        job = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.execute(
            "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) "
            "VALUES (?, NULL, ?)",
            (job, source),
        )
        return job


def _bind(storage: Storage, job_id: int) -> None:
    with storage.transaction() as db:
        db.execute(
            "INSERT INTO generation_job_provider_bindings(generation_job_id, provider_name) VALUES (?, 'codex_cli')",
            (job_id,),
        )


def _activate_cutover(storage: Storage, *, baseline_generation_job_id: int = 0) -> None:
    digest = "a" * 64
    now = NOW.isoformat()
    expires = (NOW + timedelta(minutes=10)).isoformat()
    with storage.transaction() as db:
        db.execute(
            "INSERT INTO sheet_target_bindings(target_ref_sha256,schema_version,sheet_id,sheet_title,oracle_fingerprint,created_at) "
            "VALUES(?,'workplace-template-v1',0,'workplace',?,?)",
            (digest, "b" * 64, now),
        )
        target_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.execute(
            "INSERT INTO telegram_audience_bindings(bot_id_digest,token_hmac,audience_hmac,version,created_at) "
            "VALUES(?,?,?,?,?)",
            ("c" * 64, "d" * 64, "e" * 64, 1, now),
        )
        audience_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.execute(
            "INSERT INTO automation_cutover_proposals("
            "id,created_at,expires_at,config_digest,frontiers_digest,cursor_digest,intervals_digest,"
            "candidate_max_id,generation_job_max_id,generation_max_id,decision_event_max_id,handoff_max_id,"
            "callback_offset,nonterminal_job_count,outbox_count,ready_target_id,ready_target_fingerprint,"
            "application_release_digest,audience_binding_digest,proposal_sha256"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "codex-authority-test",
                now,
                expires,
                digest,
                "f" * 64,
                "1" * 64,
                "2" * 64,
                0,
                baseline_generation_job_id,
                0,
                0,
                0,
                0,
                0,
                0,
                target_id,
                digest,
                "3" * 64,
                "4" * 64,
                "5" * 64,
            ),
        )
        db.execute(
            "INSERT INTO automation_cutovers("
            "id,proposal_id,audience_binding_id,target_binding_id,release_digest,activated_at,"
            "baseline_candidate_id,baseline_generation_job_id,baseline_generation_id,"
            "baseline_decision_event_id,baseline_handoff_id,approval_offset"
            ") VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "codex-authority-test",
                audience_id,
                target_id,
                "6" * 64,
                now,
                0,
                baseline_generation_job_id,
                0,
                0,
                0,
                0,
            ),
        )


def test_post_cutover_codex_refuses_jobs_without_immutable_generation_authority() -> None:
    storage = Storage.open(":memory:")
    unbound = _job(storage, 1)
    bound = _job(storage, 2)
    _bind(storage, bound)
    _activate_cutover(storage)

    pipeline = _pipeline(storage)
    assert pipeline.select_codex_job_id() is None
    assert (
        storage.fetch_one("SELECT 1 FROM generation_job_provider_bindings WHERE generation_job_id=?", (unbound,))
        is None
    )
    assert asyncio.run(pipeline.generate_codex_job_exact(bound)) is None
    assert storage.fetch_one("SELECT status FROM generation_jobs WHERE id=?", (bound,))["status"] == "queued"
    assert (
        storage.fetch_one(
            "SELECT COUNT(*) AS count FROM generation_provider_attempts WHERE generation_job_id=?", (bound,)
        )["count"]
        == 0
    )


class InspectingCodex:
    storage: Storage
    job_id: int
    calls = 0

    async def generate(self, request):
        type(self).calls += 1
        attempt = self.storage.fetch_one(
            "SELECT terminal_outcome FROM generation_provider_attempts WHERE generation_job_id=?", (self.job_id,)
        )
        event = self.storage.fetch_one(
            "SELECT event_kind FROM pipeline_events WHERE generation_job_id=? AND event_kind='provider_call'",
            (self.job_id,),
        )
        assert attempt is not None and attempt["terminal_outcome"] is None
        assert event is not None
        return await FakeGenerationProvider().generate(request)


class RaisingCodex:
    error: Exception

    async def generate(self, _request):
        raise self.error


class FlexibleCodex:
    async def generate(self, request):
        assert request.flexible_page_count is True
        return await FakeGenerationProvider().generate(
            type(request)(
                request.candidate_id,
                request.source_version_ids,
                3,
                request.facts,
                locale=request.locale,
            )
        )


def _pipeline(storage: Storage) -> NewsPipeline:
    return NewsPipeline(storage, SimpleNamespace(), lambda: None, FixtureClock(NOW))


def test_exact_job_leases_before_provider_call_and_never_substitutes_frozen_or_stale_job(monkeypatch) -> None:
    storage = Storage.open(":memory:")
    stale = _job(storage, 1, status="running")
    target = _job(storage, 2)
    _bind(storage, stale)
    _bind(storage, target)
    with storage.transaction() as db:
        db.execute(
            "UPDATE generation_jobs SET lease_expires_at=? WHERE id=?",
            ((NOW - timedelta(seconds=1)).isoformat(), stale),
        )
        db.execute(
            "UPDATE candidates SET status='rejected' WHERE id=(SELECT candidate_id FROM selections WHERE id=(SELECT selection_id FROM generation_jobs WHERE id=?))",
            (stale,),
        )
    InspectingCodex.storage = storage
    InspectingCodex.job_id = target
    InspectingCodex.calls = 0
    monkeypatch.setattr("newsbot.ai.codex_cli.CodexCliProvider", InspectingCodex)
    assert asyncio.run(_pipeline(storage).generate_codex_job_exact(stale)) is None
    result = asyncio.run(_pipeline(storage).generate_codex_job_exact(target))
    assert result is not None
    assert InspectingCodex.calls == 1
    assert storage.fetch_one("SELECT status FROM generation_jobs WHERE id=?", (stale,))["status"] == "running"
    assert (
        storage.fetch_one(
            "SELECT COUNT(*) AS count FROM generations WHERE generation_job_id=? AND status='current'", (target,)
        )["count"]
        == 1
    )


@pytest.mark.parametrize(
    ("error_type", "safe_code", "pause", "hold_after"),
    [
        (CodexAuthUnavailableError, "codex_auth_unavailable", True, None),
        (CodexBusyError, "codex_busy", False, 10),
        (CodexTimeoutError, "codex_timeout", False, 5),
        (CodexNonzeroError, "codex_nonzero", False, 5),
        (CodexInputLimitError, "codex_input_limit", False, 2),
    ],
)
def test_codex_failures_are_classified_and_apply_safe_retry_policy(
    monkeypatch, error_type, safe_code, pause, hold_after
) -> None:
    storage = Storage.open(":memory:")
    job = _job(storage, 1)
    _bind(storage, job)
    RaisingCodex.error = error_type()
    monkeypatch.setattr("newsbot.ai.codex_cli.CodexCliProvider", RaisingCodex)
    pipeline = _pipeline(storage)
    with pytest.raises(error_type):
        asyncio.run(pipeline.generate_codex_job_exact(job))
    classified = storage.fetch_one("SELECT safe_code FROM generation_provider_attempt_classifications")
    assert classified["safe_code"] == safe_code
    state = storage.fetch_one(
        "SELECT consecutive_failures, held_at, blocked_by_safe_code FROM generation_job_retry_state WHERE generation_job_id=?",
        (job,),
    )
    assert state["consecutive_failures"] == 1
    if pause:
        assert state["blocked_by_safe_code"] == safe_code
        assert (
            storage.fetch_one("SELECT paused_at FROM generation_provider_controls WHERE provider_name='codex_cli'")[
                "paused_at"
            ]
            is not None
        )
    else:
        assert state["held_at"] is None
        assert storage.fetch_one("SELECT retry_at FROM generation_jobs WHERE id=?", (job,))["retry_at"] is not None


def test_fake_and_openai_generation_do_not_create_codex_rows() -> None:
    storage = Storage.open(":memory:")
    job = _job(storage, 1)
    candidate = storage.fetch_one(
        "SELECT candidate_id FROM selections WHERE id=(SELECT selection_id FROM generation_jobs WHERE id=?)", (job,)
    )["candidate_id"]
    pipeline = NewsPipeline(storage, SimpleNamespace(), FakeGenerationProvider(), FixtureClock(NOW))
    asyncio.run(pipeline.generate_selected(int(candidate), page_count=2))
    assert storage.fetch_one("SELECT COUNT(*) AS count FROM generation_job_provider_bindings")["count"] == 0
    assert storage.fetch_one("SELECT COUNT(*) AS count FROM generation_provider_attempt_classifications")["count"] == 0


def test_same_job_exact_replay_returns_existing_current_generation(monkeypatch) -> None:
    storage = Storage.open(":memory:")
    job = _job(storage, 1)
    _bind(storage, job)
    monkeypatch.setattr("newsbot.ai.codex_cli.CodexCliProvider", FakeGenerationProvider)
    pipeline = _pipeline(storage)
    assert asyncio.run(pipeline.generate_codex_job_exact(job)) is not None
    assert asyncio.run(pipeline.generate_codex_job_exact(job)) is None
    assert (
        storage.fetch_one(
            "SELECT COUNT(*) AS count FROM generations WHERE generation_job_id=? AND status='current'", (job,)
        )["count"]
        == 1
    )


def test_initial_exact_job_allows_provider_selected_page_count(monkeypatch) -> None:
    storage = Storage.open(":memory:")
    job = _job(storage, 1, page_count=None)
    _bind(storage, job)
    monkeypatch.setattr("newsbot.ai.codex_cli.CodexCliProvider", FlexibleCodex)

    result = asyncio.run(_pipeline(storage).generate_codex_job_exact(job))

    assert result is not None
    assert result.draft.page_count == 3
    assert (
        storage.fetch_one(
            "SELECT requested_page_count FROM generation_jobs WHERE id=?",
            (job,),
        )["requested_page_count"]
        is None
    )

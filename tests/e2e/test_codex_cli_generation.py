import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from newsbot.ai.fake import FakeGenerationProvider
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


def _bound_job(storage: Storage) -> int:
    with storage.transaction() as db:
        db.execute("INSERT INTO runs(run_key, mode, status) VALUES ('codex-e2e', 'fixture', 'running')")
        run = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO source_posts(channel_id, external_post_id, source_url) VALUES ('fixture', '1', 'https://example.test')"
        )
        post = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'fixture-v1', 'offline source')",
            (post,),
        )
        source = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score) VALUES (?, ?, 'fixture', '1.000000')",
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
        db.execute("INSERT INTO digests(run_id, digest_key, status) VALUES (?, 'codex-e2e', 'selected')", (run,))
        digest = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute("INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest, candidate))
        selection = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) VALUES (?, 'initial', 'queued', 2)",
            (selection,),
        )
        job = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
        db.execute(
            "INSERT INTO generation_job_provider_bindings(generation_job_id, provider_name) VALUES (?, 'codex_cli')",
            (job,),
        )
        db.execute(
            "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) "
            "VALUES (?, NULL, ?)",
            (job, source),
        )
        return job


def test_offline_exact_codex_generation_retries_same_job_id_without_duplicate_current_generation(monkeypatch) -> None:
    storage = Storage.open(":memory:")
    job = _bound_job(storage)
    clock = FixtureClock(datetime(2026, 7, 31, 12, tzinfo=UTC))
    pipeline = NewsPipeline(storage, SimpleNamespace(), lambda: None, clock)
    monkeypatch.setattr("newsbot.ai.codex_cli.CodexCliProvider", FakeGenerationProvider)

    first = asyncio.run(pipeline.generate_codex_job_exact(job))
    retry = asyncio.run(pipeline.generate_codex_job_exact(job))

    assert first is not None
    assert retry is None
    assert (
        storage.fetch_one(
            "SELECT COUNT(*) AS count FROM generations WHERE generation_job_id=? AND status='current'", (job,)
        )["count"]
        == 1
    )
    assert (
        storage.fetch_one(
            "SELECT COUNT(*) AS count FROM generation_provider_attempts WHERE generation_job_id=?", (job,)
        )["count"]
        == 1
    )
    assert (
        storage.fetch_one(
            "SELECT terminal_outcome FROM generation_provider_attempts WHERE generation_job_id=?", (job,)
        )["terminal_outcome"]
        == "succeeded"
    )

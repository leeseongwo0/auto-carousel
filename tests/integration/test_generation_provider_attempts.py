import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from newsbot.ai.fake import FakeGenerationProvider
from newsbot.ai.openai_compatible import _draft_from_mapping
from newsbot.observability import inspect, status
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


class FailingProvider:
    async def generate(self, _request):
        raise RuntimeError("OPENAI_API_KEY=not-for-logs")


class CapturingProvider:
    def __init__(self) -> None:
        self.request = None

    async def generate(self, request):
        self.request = request
        return await FakeGenerationProvider().generate(request)


def _selected_job(storage: Storage) -> int:
    with storage.transaction() as connection:
        connection.execute("INSERT INTO runs(run_key, mode, status) VALUES ('attempts', 'fixture', 'running')")
        run_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_posts(channel_id, external_post_id, source_url) VALUES ('c', '1', 'https://example.test')"
        )
        post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', 'source')", (post_id,)
        )
        source_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score) VALUES (?, ?, 'v1', '1.000000')",
            (run_id, source_id),
        )
        evaluation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'selected_generation_pending', 1)",
            (evaluation_id,),
        )
        candidate_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
            (candidate_id, source_id),
        )
        connection.execute(
            "INSERT INTO digests(run_id, digest_key, status) VALUES (?, 'attempts', 'selected')", (run_id,)
        )
        digest_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest_id, candidate_id)
        )
        selection_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) VALUES (?, 'initial', 'queued', 2)",
            (selection_id,),
        )
    return candidate_id


def test_provider_attempts_preserve_failures_expired_leases_and_retry_success() -> None:
    storage = Storage.open(":memory:")
    clock = FixtureClock(datetime(2026, 7, 29, 12, tzinfo=UTC))
    candidate_id = _selected_job(storage)
    providers = iter((FailingProvider(), FakeGenerationProvider()))
    pipeline = NewsPipeline(storage, SimpleNamespace(), "output", lambda: next(providers), clock)

    with pytest.raises(RuntimeError, match="not-for-logs"):
        asyncio.run(pipeline.generate_selected(candidate_id, page_count=2))

    failed = storage.fetch_one(
        "SELECT terminal_outcome, error_message FROM generation_provider_attempts WHERE attempt=1"
    )
    assert failed["terminal_outcome"] == "failed"
    assert failed["error_message"] == "RuntimeError: generation failed"

    with storage.transaction() as connection:
        job_id = int(connection.execute("SELECT id FROM generation_jobs").fetchone()[0])
        connection.execute(
            "UPDATE generation_jobs SET status='running', attempts=2, lease_token='expired', lease_expires_at=? WHERE id=?",
            ((clock.now() - timedelta(seconds=1)).isoformat(), job_id),
        )
        connection.execute(
            "INSERT INTO generation_provider_attempts(generation_job_id, attempt, started_at) VALUES (?, 2, ?)",
            (job_id, clock.now().isoformat()),
        )

    generated = asyncio.run(pipeline.generate_selected(candidate_id, page_count=2))
    assert not generated.reused
    attempts = storage.fetch_all(
        "SELECT attempt, terminal_outcome, error_message FROM generation_provider_attempts ORDER BY attempt"
    )
    assert [(row["attempt"], row["terminal_outcome"]) for row in attempts] == [
        (1, "failed"),
        (2, "abandoned"),
        (3, "succeeded"),
    ]
    assert attempts[1]["error_message"] == "LeaseExpired: generation failed"
    assert status(storage)["provider_attempts"] == 3
    assert inspect(storage, 1)["provider_attempts"] == 3

    with (
        storage.transaction() as connection,
        pytest.raises(sqlite3.IntegrityError, match="may only be finalized once"),
    ):
        connection.execute("UPDATE generation_provider_attempts SET error_message='changed' WHERE attempt=1")


def _provider_draft() -> dict[str, object]:
    return {
        "draft": True,
        "source_reported": True,
        "cover": {"title": "제목", "subtitle": "", "factual_units": []},
        "bodies": [],
        "caption": {
            "hook": "훅",
            "context": "맥락",
            "details": "상세",
            "implications": "의미",
            "questions": "질문",
            "hashtags": ["#AI"],
        },
    }


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("draft", None),
        ("draft", False),
        ("draft", "true"),
        ("source_reported", None),
        ("source_reported", False),
        ("source_reported", "true"),
    ),
)
def test_provider_draft_requires_explicit_boolean_trust_markers(field: str, value: object) -> None:
    payload = _provider_draft()
    if value is None:
        payload.pop(field)
    else:
        payload[field] = value

    with pytest.raises(ValueError, match="boolean true|missing fields"):
        _draft_from_mapping(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("conflicts", True),
        ("corroboration", True),
        ("trust_override", True),
        ("cover.trust_override", True),
        ("body.corroboration", True),
        ("unit.unknown", True),
        ("reference.trust_override", True),
        ("caption.conflicts", True),
    ),
)
def test_provider_draft_rejects_unknown_fields_at_every_boundary(field: str, value: object) -> None:
    payload = _provider_draft()
    unit = {"text": "사실", "references": [{"claim_id": "claim_1", "source_version_id": 1}]}
    cover = payload["cover"]
    caption = payload["caption"]
    assert isinstance(cover, dict) and isinstance(caption, dict)
    cover["factual_units"] = [unit]
    payload["bodies"] = [{"subtitle": "본문", "body": "내용", "factual_units": [unit]}]
    if field in {"conflicts", "corroboration", "trust_override"}:
        payload[field] = value
    elif field == "cover.trust_override":
        cover["trust_override"] = value
    elif field == "body.corroboration":
        payload["bodies"][0]["corroboration"] = value
    elif field == "unit.unknown":
        unit["unknown"] = value
    elif field == "reference.trust_override":
        unit["references"][0]["trust_override"] = value
    else:
        caption["conflicts"] = value

    with pytest.raises(ValueError, match="invalid fields"):
        _draft_from_mapping(payload)


def test_generation_fact_packet_is_content_addressed_and_retains_server_trust_data() -> None:
    storage = Storage.open(":memory:")
    candidate_id = _selected_job(storage)
    provider = CapturingProvider()
    pipeline = NewsPipeline(
        storage,
        SimpleNamespace(),
        "output",
        provider,
        FixtureClock(datetime(2026, 7, 29, 12, tzinfo=UTC)),
    )

    asyncio.run(pipeline.generate_selected(candidate_id, page_count=2))

    fact = provider.request.facts[0]
    assert fact.id.startswith("claim_")
    assert fact.source_identity.startswith("src_")
    assert fact.material_identity.startswith("mat_")
    assert fact.observation_identity.startswith("obs_")
    assert fact.evidence_spans == ((0, len("source")),)
    assert fact.source_url == "https://example.test"
    assert fact.conflicts == ()
    assert fact.uncertainty == ()

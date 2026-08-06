import json
from datetime import UTC, datetime

import pytest

from newsbot.approval.base import hash_callback_token
from newsbot.approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from newsbot.candidates import CandidateApprovalService
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


def _candidate(
    storage: Storage,
    *,
    run_key: str = "approval",
    external_post_id: str = "1",
    source_url: str | None = None,
    body: str = "source",
    channel_handle: str = "",
    urls_json: str = "[]",
) -> int:
    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO runs(run_key, mode, status) VALUES (?, 'fixture', 'done')",
            (run_key,),
        )
        run_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO source_posts(channel_id, external_post_id, source_url) VALUES ('channel', ?, ?)",
            (external_post_id, source_url),
        )
        post_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO source_post_versions("
            "source_post_id, version_key, body, channel_handle, urls_json"
            ") VALUES (?, 'v1', ?, ?, ?)",
            (post_id, body, channel_handle, urls_json),
        )
        version_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score) VALUES (?, ?, 'policy', '0.900000')",
            (run_id, version_id),
        )
        evaluation_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'pending_selection', 1)", (evaluation_id,)
        )
        return int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])


def _review_generation(storage: Storage, candidate_id: int, *, page_count: int = 2) -> tuple[int, int]:
    with storage.transaction() as connection:
        run_id = connection.execute(
            "SELECT ce.run_id FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
            (candidate_id,),
        ).fetchone()[0]
        source_version_id = connection.execute(
            "SELECT ce.source_post_version_id FROM candidates c "
            "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
            (candidate_id,),
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO digests(run_id, digest_key, status) VALUES (?, ?, 'selected')",
            (run_id, f"review-{candidate_id}"),
        )
        digest_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)",
            (digest_id, candidate_id),
        )
        selection_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status) VALUES (?, 'initial:2', 'succeeded')",
            (selection_id,),
        )
        job_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        content_json = json.dumps({"pages": [{} for _ in range(page_count)]})
        connection.execute(
            "INSERT INTO generations(generation_job_id, attempt, status, content_json) VALUES (?, 1, 'current', ?)",
            (job_id, content_json),
        )
        generation_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) VALUES (?, ?, ?)",
            (job_id, generation_id, source_version_id),
        )
        connection.execute("UPDATE candidates SET status='pending_review' WHERE id=?", (candidate_id,))
        return int(generation_id), int(source_version_id)


def test_candidate_digest_projects_preview_title_and_telegram_source_only() -> None:
    storage = Storage.open(":memory:")
    _candidate(
        storage,
        source_url="https://t.me/news_publisher/7687",
        body="긴 본문은 Telegram 후보 메시지에 포함되면 안 됩니다.",
        channel_handle="news_publisher",
        urls_json=json.dumps([{"url": "https://example.test", "title": "Anthropic 보안 평가 사고"}]),
    )
    service = CandidateApprovalService(storage, chat_id=100, authorized_user_ids={7}, now=FixtureClock().now)

    candidate = service.create_digest(1, actor_id=7).candidates[0]

    assert candidate["title"] == "Anthropic 보안 평가 사고"
    assert candidate["source_url"] == "https://t.me/news_publisher/7687"


def test_selection_is_provider_free_and_idempotent() -> None:
    storage = Storage.open(":memory:")
    candidate_id = _candidate(storage)
    service = CandidateApprovalService(
        storage, chat_id=100, authorized_user_ids={7}, now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now
    )
    digest = service.create_digest(1, actor_id=7)
    repeated_digest = service.create_digest(1, actor_id=7)
    assert repeated_digest.id == digest.id
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM digests")["n"] == 1
    make = next(button for button in digest.buttons[candidate_id] if button.label == "[제작]")
    adapter = ScriptedApprovalAdapter(service)
    accepted = adapter.apply(ScriptedAction(make.token, 100, 7))
    repeated = adapter.apply(ScriptedAction(make.token, 100, 7))
    assert accepted.status == "queued"
    assert repeated.status == "duplicate"
    token_row = storage.fetch_one("SELECT token FROM callback_tokens WHERE token=?", (hash_callback_token(make.token),))
    assert token_row is not None
    assert token_row["token"] != make.token
    assert len(token_row["token"]) == 64
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM generation_jobs")["n"] == 1
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM export_outbox")["n"] == 0


def test_unauthorized_callback_does_not_select() -> None:
    storage = Storage.open(":memory:")
    candidate_id = _candidate(storage)
    service = CandidateApprovalService(storage, chat_id=100, authorized_user_ids={7}, now=FixtureClock().now)
    digest = service.create_digest(1, actor_id=7)
    token = next(button.token for button in digest.buttons[candidate_id] if button.label == "[제작]")
    assert service.apply(token, chat_id=100, user_id=99).status == "unauthorized"
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM generation_jobs")["n"] == 0


def test_review_controls_are_three_actions_and_bound_to_the_candidate() -> None:
    storage = Storage.open(":memory:")
    candidate_id = _candidate(storage)
    other_candidate_id = _candidate(storage, run_key="other", external_post_id="2")
    generation_id, source_version_id = _review_generation(storage, candidate_id)
    foreign_generation_id, _ = _review_generation(storage, other_candidate_id)
    service = CandidateApprovalService(storage, chat_id=100, authorized_user_ids={7}, now=FixtureClock().now)

    with pytest.raises(ValueError, match="exact current generation binding"):
        service.review_buttons(
            candidate_id,
            foreign_generation_id,
            actor_id=7,
            source_version_ids=(source_version_id,),
        )

    buttons = service.review_buttons(
        candidate_id,
        generation_id,
        actor_id=7,
        source_version_ids=(source_version_id,),
    )
    assert [button.action.value for button in buttons] == [
        "approve_handoff",
        "regenerate",
        "reject",
    ]
    assert [button.label for button in buttons] == ["승인", "재생성", "거절"]
    assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "pending_review"
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM generation_jobs WHERE job_kind LIKE 'page:%'")["n"] == 0
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM export_outbox")["n"] == 0

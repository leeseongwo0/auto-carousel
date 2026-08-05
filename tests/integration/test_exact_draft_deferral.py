import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from newsbot.approval.base import ApprovalAction, ApprovalStage, hash_callback_token
from newsbot.approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from newsbot.approval.telegram import TelegramApprovalAdapter
from newsbot.candidates import CandidateApprovalService
from newsbot.exports import approval_outbox_intent, generation_claim_payload
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


def _setup(
    storage: Storage, *, rationale: dict[object, object] | None = None, valid: bool = True
) -> tuple[int, int, int]:
    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO runs(run_key, mode, status) VALUES (?, 'fixture', 'done')",
            (f"exact-{connection.execute('SELECT COUNT(*) FROM runs').fetchone()[0]}",),
        )
        run_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_posts(channel_id, external_post_id) VALUES ('c', ?)",
            (str(connection.execute("SELECT COUNT(*) FROM source_posts").fetchone()[0]),),
        )
        post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', 'source')", (post_id,)
        )
        source_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_observations("
            "source_post_id, source_post_version_id, observation_key, observed_at, engagement_json"
            ") VALUES (?, ?, ?, '2026-07-29T00:00:00+00:00', '{}')",
            (post_id, source_id, f"obs-{source_id}"),
        )
        external_post_id = str(
            connection.execute("SELECT external_post_id FROM source_posts WHERE id=?", (post_id,)).fetchone()[0]
        )
        source_record = {
            "channel_id": "c",
            "external_post_id": external_post_id,
            "source_url": None,
            "version_key": "v1",
            "body": "source",
            "media": [],
            "kind": "message",
            "sponsored": False,
            "urls": [],
            "conflicts": [],
            "observation_key": f"obs-{source_id}",
            "captured_at": "2026-07-29T00:00:00+00:00",
            "engagement": {},
            "uncertainty": [],
        }
        claim = generation_claim_payload(source_record, source_id)
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score, rationale_json) VALUES (?, ?, 'v', '1.000000', ?)",
            (run_id, source_id, json.dumps(rationale or {})),
        )
        evaluation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidates(evaluation_id, status, rank) VALUES (?, 'pending_review', 1)", (evaluation_id,)
        )
        candidate_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
            (candidate_id, source_id),
        )
        connection.execute(
            "INSERT INTO digests(run_id, digest_key, status) VALUES (?, ?, 'selected')", (run_id, f"exact-{run_id}")
        )
        digest_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest_id, candidate_id)
        )
        selection_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) VALUES (?, 'initial', 'succeeded', 2)",
            (selection_id,),
        )
        job_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        body = "x" * (241 if not valid else 20)
        factual_unit = {
            "text": "Source supports this draft",
            "references": [{"claim_id": claim["claim_id"], "source_version_id": source_id}],
        }
        content = {
            "cover": {"title": "Title", "subtitle": "Sub", "factual_units": [factual_unit]},
            "bodies": [{"subtitle": "Body", "body": body, "factual_units": [factual_unit]}],
            "caption": {
                "hook": "Hook",
                "context": "Context",
                "details": "Details",
                "implications": "Implications",
                "questions": "Questions",
                "hashtags": ["#news"],
            },
            "draft": True,
            "source_reported": True,
            "claim_manifest": [claim],
        }
        connection.execute(
            "INSERT INTO generations(generation_job_id, attempt, status, content_json) VALUES (?, 1, 'current', ?)",
            (job_id, json.dumps(content, ensure_ascii=False)),
        )
        generation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) VALUES (?, ?, ?)",
            (job_id, generation_id, source_id),
        )
    return candidate_id, generation_id, source_id


def _service(storage: Storage, clock: FixtureClock) -> CandidateApprovalService:
    return CandidateApprovalService(storage, chat_id=10, authorized_user_ids={20}, now=clock.now)


def _button(
    service: CandidateApprovalService, candidate_id: int, generation_id: int, source_id: int, action: ApprovalAction
) -> str:
    return next(
        button.token
        for button in service.review_buttons(candidate_id, generation_id, actor_id=20, source_version_ids=(source_id,))
        if button.action is action
    )


def test_exact_review_fails_closed_for_immutable_hash_invalid_copy_and_sources() -> None:
    storage = Storage.open(":memory:")
    clock = FixtureClock(datetime(2026, 7, 29, tzinfo=UTC))
    candidate_id, generation_id, source_id = _setup(storage)
    service = _service(storage, clock)
    approve = _button(service, candidate_id, generation_id, source_id, ApprovalAction.APPROVE_HANDOFF)
    with storage.transaction() as connection:
        with pytest.raises(sqlite3.IntegrityError, match="content_json is immutable"):
            connection.execute("UPDATE generations SET content_json='{}' WHERE id=?", (generation_id,))
        token = hash_callback_token(approve)
        payload = json.loads(
            connection.execute("SELECT payload_json FROM callback_tokens WHERE token=?", (token,)).fetchone()[0]
        )
        payload["content_sha256"] = "0" * 64
        connection.execute("UPDATE callback_tokens SET payload_json=? WHERE token=?", (json.dumps(payload), token))
    assert service.apply(approve, chat_id=10, user_id=20).status == "stale"

    invalid_candidate, invalid_generation, invalid_source = _setup(storage, valid=False)
    invalid_token = _button(
        service, invalid_candidate, invalid_generation, invalid_source, ApprovalAction.APPROVE_HANDOFF
    )
    assert service.apply(invalid_token, chat_id=10, user_id=20).status == "stale"
    with storage.transaction() as connection:
        connection.execute("DELETE FROM candidate_sources WHERE candidate_id=?", (invalid_candidate,))
    assert service.apply(invalid_token, chat_id=10, user_id=20).status == "stale"
    source_candidate, source_generation, source_id = _setup(storage)
    source_token = _button(service, source_candidate, source_generation, source_id, ApprovalAction.APPROVE_HANDOFF)
    with storage.transaction() as connection:
        connection.execute("INSERT INTO source_posts(channel_id, external_post_id) VALUES ('other', '1')")
        post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', 'other')",
            (post_id,),
        )
        replacement_source = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
            (source_candidate, replacement_source),
        )
        connection.execute("UPDATE candidates SET revision=revision+1 WHERE id=?", (source_candidate,))
    assert service.apply(source_token, chat_id=10, user_id=20).status == "stale"


def test_review_callback_rejects_superseded_or_revision_mismatched_digest() -> None:
    storage = Storage.open(":memory:")
    clock = FixtureClock(datetime(2026, 7, 29, tzinfo=UTC))
    candidate_id, generation_id, source_id = _setup(storage)
    service = _service(storage, clock)
    revision_mismatch = _button(service, candidate_id, generation_id, source_id, ApprovalAction.APPROVE_HANDOFF)
    with storage.transaction() as connection:
        connection.execute("UPDATE digests SET revision=revision+1 WHERE id=1")
    assert service.apply(revision_mismatch, chat_id=10, user_id=20).status == "stale"

    superseded = _button(service, candidate_id, generation_id, source_id, ApprovalAction.APPROVE_HANDOFF)
    with storage.transaction() as connection:
        connection.execute("UPDATE digests SET status='superseded' WHERE id=1")
    assert service.apply(superseded, chat_id=10, user_id=20).status == "stale"


@pytest.mark.parametrize(
    "action, delay", [(ApprovalAction.DEFER_6H, 6), (ApprovalAction.DEFER_24H, 24), (ApprovalAction.DEFER_72H, 72)]
)
def test_deferral_resumes_exact_selection_stage(action: ApprovalAction, delay: int) -> None:
    storage = Storage.open(":memory:")
    clock = FixtureClock(datetime(2026, 7, 29, tzinfo=UTC))
    candidate_id, _, _ = _setup(storage)
    service = _service(storage, clock)
    with storage.transaction() as connection:
        connection.execute("UPDATE candidates SET status='pending_selection' WHERE id=?", (candidate_id,))
    digest = service.create_digest(1, actor_id=20)
    actions = tuple(button.action for button in digest.buttons[candidate_id])
    assert actions == (ApprovalAction.MAKE, ApprovalAction.REJECT, ApprovalAction.REFRESH)
    candidate = next(value for value in digest.candidates if value["candidate_id"] == candidate_id)
    selection_token = service._button(
        digest.id,
        candidate_id,
        int(candidate["revision"]),
        tuple(candidate["source_version_ids"]),
        20,
        ApprovalStage.SELECTION,
        action,
        clock.now(),
        timedelta(hours=24),
        digest_revision=digest.revision,
    ).token
    assert ScriptedApprovalAdapter(service).apply(ScriptedAction(selection_token, 10, 20)).status == "deferred"
    assert service.resume_due(clock.now()) == ()
    clock.advance(timedelta(hours=delay))
    assert service.resume_due(clock.now()) == (candidate_id,)
    assert service.resume_due(clock.now()) == ()
    assert (
        storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "pending_selection"
    )
    assert service.apply(selection_token, chat_id=10, user_id=20).status == "stale"


def test_server_warnings_are_rendered_in_telegram_and_exports() -> None:
    storage = Storage.open(":memory:")
    clock = FixtureClock(datetime(2026, 7, 29, tzinfo=UTC))
    candidate_id, generation_id, source_id = _setup(
        storage,
        rationale={
            "rationale": {
                "warnings": [
                    {"kind": "conflict", "detail": "Source conflict"},
                    {"kind": "rumor", "detail": "Unverified"},
                ]
            }
        },
    )
    service = _service(storage, clock)
    digest = service.create_digest(1, actor_id=20)
    sent: list[str] = []

    class Adapter(TelegramApprovalAdapter):
        def _request(self, method: str, payload: object) -> dict[str, object]:
            sent.append(str(payload))
            return {"ok": True}

    adapter = Adapter("token", service)
    adapter.send_candidate_digest(digest)
    adapter.send_review_draft(
        candidate_id=candidate_id,
        generation_id=generation_id,
        source_version_ids=(source_id,),
        draft_text="draft",
        actor_id=20,
    )
    assert any("Source conflict" in message and "Unverified" in message for message in sent)
    content = storage.fetch_one("SELECT content_json FROM generations WHERE id=?", (generation_id,))["content_json"]
    _, exported_json, exported_markdown = approval_outbox_intent(
        candidate_id=candidate_id,
        generation_id=generation_id,
        approval_event_id=1,
        source_version_ids=(source_id,),
        content_json=content,
        warnings=service.warnings_for_candidate(candidate_id),
        source_versions=(
            {
                "source_version_id": source_id,
                "channel_id": "c",
                "external_post_id": "0",
                "source_url": None,
                "version_key": "v1",
                "body": "source",
                "media": [],
                "kind": "message",
                "sponsored": False,
                "urls": [],
                "conflicts": [],
                "observation_key": "obs-v1",
                "captured_at": "2026-07-29T00:00:00+00:00",
                "engagement": {},
                "uncertainty": [],
            },
        ),
    )
    assert b"Source conflict" in exported_json and b"Unverified" in exported_json
    assert b"## Warnings" in exported_markdown and b"Source conflict" in exported_markdown

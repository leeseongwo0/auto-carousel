import json
import sqlite3
from datetime import UTC, datetime

import pytest

from newsbot.approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from newsbot.candidates import CandidateApprovalService
from newsbot.exports import generation_claim_payload
from newsbot.handoffs import SheetHandoffService
from newsbot.pipeline import NewsPipeline
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


def _review_state(storage: Storage, *, pages: int = 2, category: str | None = "AI") -> tuple[int, int, int]:
    with storage.transaction() as connection:
        connection.execute("INSERT INTO runs(run_key, mode, status) VALUES ('atomic', 'fixture', 'done')")
        connection.execute("INSERT INTO source_posts(channel_id, external_post_id) VALUES ('channel', '1')")
        post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', 'source')",
            (post_id,),
        )
        source_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_observations("
            "source_post_id, source_post_version_id, observation_key, observed_at, engagement_json"
            ") VALUES (?, ?, 'obs-v1', '2026-07-29T00:00:00+00:00', '{}')",
            (post_id, source_id),
        )
        source_record = {
            "channel_id": "channel",
            "external_post_id": "1",
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
        }
        claim = generation_claim_payload(source_record, source_id)
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score) "
            "VALUES (1, ?, 'policy', '1.000000')",
            (source_id,),
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
        connection.execute("INSERT INTO digests(run_id, digest_key, status) VALUES (1, 'review', 'selected')")
        digest_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest_id, candidate_id)
        )
        selection_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) VALUES (?, 'initial', 'succeeded', ?)",
            (selection_id, pages),
        )
        job_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        content = {
            "draft": True,
            "source_reported": True,
            **({"category": category} if category is not None else {}),
            "cover": {
                "title": "Title",
                "subtitle": "",
                "factual_units": [
                    {
                        "text": "Source fact",
                        "references": [{"claim_id": claim["claim_id"], "source_version_id": source_id}],
                    }
                ],
            },
            "bodies": [
                {
                    "subtitle": str(index),
                    "body": "Body",
                    "factual_units": [
                        {
                            "text": "Source fact",
                            "references": [{"claim_id": claim["claim_id"], "source_version_id": source_id}],
                        }
                    ],
                }
                for index in range(2, pages + 1)
            ],
            "caption": {
                "hook": "Caption",
                "context": "Context",
                "details": "Details",
                "implications": "Implications",
                "questions": "Questions",
                "hashtags": ["#news"],
            },
            "claim_manifest": [claim],
        }
        connection.execute(
            "INSERT INTO generations(generation_job_id, attempt, status, content_json) VALUES (?, 1, 'current', ?)",
            (job_id, json.dumps(content)),
        )
        generation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) VALUES (?, ?, ?)",
            (job_id, generation_id, source_id),
        )
    return candidate_id, generation_id, source_id


def _service(storage: Storage) -> CandidateApprovalService:
    target_binding_id = SheetHandoffService(storage).ensure_binding(
        binding_key="workplace",
        spreadsheet_id="sheet",
        sheet_id=0,
        oracle_fingerprint="a" * 64,
        now="2026-07-29T00:00:00+00:00",
    )
    return CandidateApprovalService(
        storage,
        chat_id=1,
        authorized_user_ids={2},
        now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now,
        sheet_target_binding_id=target_binding_id,
    )


def _button(
    service: CandidateApprovalService, candidate_id: int, generation_id: int, source_id: int, action: str
) -> str:
    return next(
        button.token
        for button in service.review_buttons(candidate_id, generation_id, actor_id=2, source_version_ids=(source_id,))
        if button.action.value == action
    )


def test_approval_commits_exactly_one_sheet_handoff_without_files(tmp_path) -> None:
    storage = Storage.open(":memory:")
    candidate_id, generation_id, source_id = _review_state(storage)
    service = _service(storage)
    token = _button(service, candidate_id, generation_id, source_id, "approve_handoff")
    adapter = ScriptedApprovalAdapter(service)
    assert adapter.apply(ScriptedAction(token, 1, 2)).status == "approved"
    assert adapter.apply(ScriptedAction(token, 1, 2)).status == "duplicate"
    assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "approved"
    handoff = storage.fetch_one(
        "SELECT category, canonical_bytes FROM sheet_handoffs WHERE generation_id=?",
        (generation_id,),
    )
    assert handoff["category"] == "AI"
    assert json.loads(bytes(handoff["canonical_bytes"]))["category"] == "AI"
    decision = storage.fetch_one("SELECT payload_json FROM decision_events WHERE decision='approve_handoff'")
    assert json.loads(decision["payload_json"])["category"] == "AI"
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM sheet_handoffs")["n"] == 1
    binding = storage.fetch_one(
        "SELECT b.target_binding_id FROM sheet_handoff_bindings b "
        "JOIN sheet_handoffs h ON h.id=b.handoff_id WHERE h.generation_id=?",
        (generation_id,),
    )
    assert binding is not None
    assert service.sheet_target_binding_id is not None
    assert int(binding["target_binding_id"]) == service.sheet_target_binding_id
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM export_outbox")["n"] == 0
    assert not list(tmp_path.iterdir())


def test_approval_binding_failure_rolls_back_and_target_cannot_rebind() -> None:
    storage = Storage.open(":memory:")
    candidate_id, generation_id, source_id = _review_state(storage)
    unbound = CandidateApprovalService(
        storage,
        chat_id=1,
        authorized_user_ids={2},
        now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now,
        sheet_target_binding_id=9999,
    )
    token = _button(unbound, candidate_id, generation_id, source_id, "approve_handoff")

    with pytest.raises(sqlite3.IntegrityError):
        unbound.apply(token, chat_id=1, user_id=2)

    assert storage.fetch_one("SELECT COUNT(*) AS n FROM decision_events")["n"] == 0
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM sheet_handoffs")["n"] == 0

    target_service = SheetHandoffService(storage)
    target_service.ensure_binding(
        binding_key="workplace",
        spreadsheet_id="sheet",
        sheet_id=0,
        oracle_fingerprint="a" * 64,
        now="2026-07-29T00:00:00+00:00",
    )
    with pytest.raises(ValueError, match="immutable"):
        target_service.ensure_binding(
            binding_key="workplace",
            spreadsheet_id="other-sheet",
            sheet_id=0,
            oracle_fingerprint="a" * 64,
            now="2026-07-29T00:00:01+00:00",
        )


def test_missing_category_cannot_commit_a_decision_or_handoff() -> None:
    storage = Storage.open(":memory:")
    candidate_id, generation_id, source_id = _review_state(storage, category=None)
    service = _service(storage)
    token = _button(service, candidate_id, generation_id, source_id, "approve_handoff")
    assert service.apply(token, chat_id=1, user_id=2).status == "stale"
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM decision_events")["n"] == 0
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM sheet_handoffs")["n"] == 0


def test_regenerate_supersedes_old_review_and_keeps_one_current_generation() -> None:
    storage = Storage.open(":memory:")
    candidate_id, generation_id, source_id = _review_state(storage)
    service = _service(storage)
    old_approval = _button(service, candidate_id, generation_id, source_id, "approve_handoff")
    regenerate = _button(service, candidate_id, generation_id, source_id, "regenerate")
    assert service.apply(regenerate, chat_id=1, user_id=2).status == "queued"
    assert service.apply(old_approval, chat_id=1, user_id=2).status == "stale"
    assert storage.fetch_one("SELECT status FROM generations WHERE id=?", (generation_id,))["status"] == "superseded"
    assert storage.fetch_one("SELECT COUNT(*) AS n FROM generations WHERE status='current'")["n"] == 0




def test_terminal_edit_retains_approved_attempt_and_allows_one_new_attempt() -> None:
    storage = Storage.open(":memory:")
    candidate_id, generation_id, source_id = _review_state(storage)
    with storage.transaction() as connection:
        connection.execute("UPDATE candidates SET status='approved' WHERE id=?", (candidate_id,))
        post_id = connection.execute(
            "SELECT source_post_id FROM source_post_versions WHERE id=?", (source_id,)
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v2', 'edited')", (post_id,)
        )
        replacement = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
    pipeline = NewsPipeline(storage, object(), lambda: None, FixtureClock())
    pipeline._invalidate_revised_candidates(1, {("channel", "1"): replacement})
    assert storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "approved"
    assert storage.fetch_one("SELECT status FROM generations WHERE id=?", (generation_id,))["status"] == "current"

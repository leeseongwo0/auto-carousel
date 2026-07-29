import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from newsbot.approval.base import ApprovalAction
from newsbot.candidates import CandidateApprovalService
from newsbot.cli import _ready_fixture_result, repair_exports
from newsbot.exports import generation_claim_payload, materialize_outbox
from newsbot.runtime import FixtureClock
from newsbot.storage import Storage


def _insert_generation(storage: Storage, name: str) -> tuple[int, int, int]:
    with storage.transaction() as connection:
        connection.execute("INSERT INTO runs(run_key, mode, status) VALUES (?, 'fixture', 'done')", (name,))
        run_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute("INSERT INTO source_posts(channel_id, external_post_id) VALUES ('channel', ?)", (name,))
        post_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_versions(source_post_id, version_key, body) VALUES (?, 'v1', 'source body')",
            (post_id,),
        )
        source_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO source_post_observations("
            "source_post_id, source_post_version_id, observation_key, observed_at, engagement_json"
            ") VALUES (?, ?, ?, '2026-07-29T00:00:00+00:00', '{}')",
            (post_id, source_id, f"obs-{name}"),
        )
        source_record = {
            "channel_id": "channel",
            "external_post_id": name,
            "source_url": None,
            "version_key": "v1",
            "body": "source body",
            "media": [],
            "kind": "message",
            "sponsored": False,
            "urls": [],
            "conflicts": [],
            "observation_key": f"obs-{name}",
            "captured_at": "2026-07-29T00:00:00+00:00",
            "engagement": {},
            "uncertainty": [],
        }
        claim = generation_claim_payload(source_record, source_id)
        connection.execute(
            "INSERT INTO candidate_evaluations(run_id, source_post_version_id, evaluator_version, score, rationale_json) "
            "VALUES (?, ?, 'v1', '1.000000', '{}')",
            (run_id, source_id),
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
        connection.execute("INSERT INTO digests(run_id, digest_key, status) VALUES (?, ?, 'selected')", (run_id, name))
        digest_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO selections(digest_id, candidate_id, position) VALUES (?, ?, 1)", (digest_id, candidate_id)
        )
        selection_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        connection.execute(
            "INSERT INTO generation_jobs(selection_id, job_kind, status, requested_page_count) "
            "VALUES (?, 'initial', 'succeeded', 2)",
            (selection_id,),
        )
        job_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        content = {
            "cover": {
                "title": "Title",
                "subtitle": "Subtitle",
                "factual_units": [
                    {
                        "text": "Source fact",
                        "references": [{"claim_id": claim["claim_id"], "source_version_id": source_id}],
                    }
                ],
            },
            "bodies": [
                {
                    "subtitle": "Body",
                    "body": "x" * 20,
                    "factual_units": [
                        {
                            "text": "Source fact",
                            "references": [{"claim_id": claim["claim_id"], "source_version_id": source_id}],
                        }
                    ],
                }
            ],
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
    return run_id, candidate_id, generation_id


def _approve(storage: Storage, candidate_id: int, generation_id: int) -> None:
    service = CandidateApprovalService(
        storage,
        chat_id=1,
        authorized_user_ids={1},
        now=FixtureClock(datetime(2026, 7, 29, tzinfo=UTC)).now,
    )
    source_ids = tuple(
        int(row["source_post_version_id"])
        for row in storage.fetch_all(
            "SELECT source_post_version_id FROM candidate_sources WHERE candidate_id=?", (candidate_id,)
        )
    )
    token = next(
        button.token
        for button in service.review_buttons(candidate_id, generation_id, actor_id=1, source_version_ids=source_ids)
        if button.action is ApprovalAction.APPROVE_HANDOFF
    )
    assert service.apply(token, chat_id=1, user_id=1).status == "approved"


def test_approval_outbox_identity_is_stable_across_clean_database_insertion_order(tmp_path):
    first = Storage.open(tmp_path / "first.sqlite")
    second = Storage.open(tmp_path / "second.sqlite")
    try:
        _, first_candidate, first_generation = _insert_generation(first, "actual")
        _insert_generation(second, "decoy")
        _, second_candidate, second_generation = _insert_generation(second, "actual")
        _approve(first, first_candidate, first_generation)
        _approve(second, second_candidate, second_generation)

        first_row = first.fetch_one(
            "SELECT export_id, canonical_bytes FROM export_outbox WHERE generation_id=? AND export_kind='json'",
            (first_generation,),
        )
        second_row = second.fetch_one(
            "SELECT export_id, canonical_bytes FROM export_outbox WHERE generation_id=? AND export_kind='json'",
            (second_generation,),
        )
        assert first_candidate != second_candidate
        assert first_generation != second_generation
        assert first_row["export_id"] == second_row["export_id"]
        assert first_row["canonical_bytes"] == second_row["canonical_bytes"]
        first_payload = json.loads(first_row["canonical_bytes"])
        assert first_payload["source_versions"][0]["source_version_identity"].startswith("srcv_")
        assert first_payload["generation_identity"].startswith("gen_")
        assert first_payload["approval_decision_identity"].startswith("dec_")
        assert '"source_version_id":' not in json.dumps(first_payload, sort_keys=True)
    finally:
        first.close()
        second.close()


def test_approval_preserves_the_generation_claim_after_observation_refresh(tmp_path):
    storage = Storage.open(tmp_path / "newsbot.sqlite")
    try:
        _, candidate_id, generation_id = _insert_generation(storage, "actual")
        generation = storage.fetch_one("SELECT content_json FROM generations WHERE id=?", (generation_id,))
        original_claim = json.loads(generation["content_json"])["claim_manifest"][0]
        source = storage.fetch_one(
            "SELECT version.source_post_id, version.id AS version_id "
            "FROM generation_sources binding "
            "JOIN source_post_versions version ON version.id=binding.source_post_version_id "
            "WHERE binding.generation_id=?",
            (generation_id,),
        )
        with storage.transaction() as connection:
            connection.execute(
                "INSERT INTO source_post_observations("
                "source_post_id, source_post_version_id, observation_key, observed_at, engagement_json"
                ") VALUES (?, ?, 'later-observation', '2026-07-30T00:00:00+00:00', '{\"views\":999}')",
                (source["source_post_id"], source["version_id"]),
            )

        _approve(storage, candidate_id, generation_id)

        outbox = storage.fetch_one(
            "SELECT canonical_bytes FROM export_outbox WHERE generation_id=? AND export_kind='json'",
            (generation_id,),
        )
        payload = json.loads(outbox["canonical_bytes"])
        portable_claim = {key: value for key, value in original_claim.items() if key != "source_version_id"}
        assert payload["claims"] == [portable_claim]
        provenance = payload["source_versions"][0]
        references = payload["pages"][0]["factual_units"][0]["references"]
        assert references == [
            {
                "claim_id": original_claim["claim_id"],
                "source_version_identity": provenance["source_version_identity"],
                "source_identity": original_claim["source_identity"],
                "material_identity": original_claim["material_identity"],
                "observation_identity": original_claim["observation_identity"],
            }
        ]
        assert provenance["claim_id"] == original_claim["claim_id"]
        assert provenance["observation_identity"] == original_claim["observation_identity"]
        assert provenance["captured_at"] == "2026-07-29T00:00:00+00:00"
        assert sum(claim["claim_id"] == references[0]["claim_id"] for claim in payload["claims"]) == 1
    finally:
        storage.close()


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_ready_handoff_recovers_missing_member_but_preserves_foreign_mismatch(tmp_path, damage):
    storage = Storage.open(tmp_path / "newsbot.sqlite")
    try:
        run_id, candidate_id, generation_id = _insert_generation(storage, "actual")
        _approve(storage, candidate_id, generation_id)
        pair = materialize_outbox(storage, tmp_path / "exports", generation_id)
        with storage.transaction() as connection:
            connection.execute("UPDATE runs SET status='ready' WHERE id=?", (run_id,))
        assert _ready_fixture_result(storage, run_id, tmp_path / "exports") is not None

        if damage == "missing":
            pair.markdown_path.unlink()
        else:
            pair.markdown_path.write_bytes(b"tampered")

        assert _ready_fixture_result(storage, run_id, tmp_path / "exports") is None
        statuses = {
            row["status"]
            for row in storage.fetch_all("SELECT status FROM export_outbox WHERE generation_id=?", (generation_id,))
        }
        if damage == "missing":
            assert statuses == {"pending"}
            assert repair_exports(SimpleNamespace(db=tmp_path / "newsbot.sqlite", output=tmp_path / "exports")) == 0
            assert pair.markdown_path.exists()
            assert _ready_fixture_result(storage, run_id, tmp_path / "exports") is not None
        else:
            assert statuses == {"corrupt"}
    finally:
        storage.close()

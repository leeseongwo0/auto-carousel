from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta

from newsbot import v2_cli
from newsbot.collectors.base import SourceObservation, UrlCandidate
from newsbot.sheets.base import (
    DeliveryOutcome,
    DispatchCredentialAttestation,
    MetadataState,
    PreparedSheetMutation,
    SheetDelivery,
)
from newsbot.v2_codex import prepare_generation
from newsbot.v2_live import (
    V2LiveWorkflow,
    deliver_v2_google_sheets,
    v2_draft_handoff_values,
)
from newsbot.v2_workflow import V2State, V2Workflow
from tests.v2_support import create_candidate


def test_validate_selection_replays_copy_three_times_without_external_effects(
    tmp_path,
    capsys,
) -> None:
    database = tmp_path / "newsbot-v2.sqlite"
    fixture = tmp_path / "selection.json"
    with V2Workflow(database, mode="create"):
        pass
    fixture.write_text(
        json.dumps(
            [
                {
                    "channel_id": "channel",
                    "channel_handle": "source",
                    "external_post_id": "42",
                    "published_at": "2026-08-11T08:00:00+00:00",
                    "observed_at": "2026-08-11T08:01:00+00:00",
                    "text": "Bitcoin blockchain regulation entered into force. " * 5,
                    "urls": [
                        {
                            "url": "https://example.test/news",
                            "source": "preview",
                            "occurrence": 0,
                        }
                    ],
                    "article_body": "Bitcoin blockchain regulation entered into force for service operators. " * 5,
                }
            ]
        ),
        encoding="utf-8",
    )
    before = hashlib.sha256(database.read_bytes()).hexdigest()

    assert (
        v2_cli.main(
            [
                "--db",
                str(database),
                "validate-selection",
                "--no-send",
                "--fixture",
                str(fixture),
            ]
        )
        == 0
    )

    report = json.loads(capsys.readouterr().out)
    assert report["no_send"] is True
    assert report["stable"] is True
    assert report["copy_stable_after_first"] is True
    assert report["before_db_hash"] == report["after_db_hash"]
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
    assert report["cycles"][0]["deltas"]["observation_revisions"] == 1
    assert report["cycles"][0]["deltas"]["candidates"] == 1
    assert report["cycles"][0]["deltas"]["stories"] == 1
    for cycle in report["cycles"]:
        assert cycle["external_effects"] == 0
        assert cycle["held_release"] is False
        assert cycle["invariants"] == {
            "candidate_binding_mismatches": 0,
            "delivered_marker_mismatches": 0,
            "tombstone_digest_mismatches": 0,
        }
    for cycle in report["cycles"][1:]:
        assert set(cycle["deltas"].values()) == {0}


def test_sqlite_backup_includes_committed_wal_rows(tmp_path) -> None:
    source = tmp_path / "source.sqlite"
    destination = tmp_path / "backup.sqlite"
    with V2Workflow(source, mode="create"):
        pass
    writer = sqlite3.connect(source)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute(
            "INSERT INTO v2_metadata(key,value) VALUES(?,?)",
            ("wal-only-proof", "present"),
        )
        writer.commit()
        v2_cli._backup_sqlite(source, destination)
    finally:
        writer.close()

    with sqlite3.connect(destination) as copied:
        assert copied.execute("SELECT value FROM v2_metadata WHERE key='wal-only-proof'").fetchone()[0] == "present"


def test_separated_v2_services_complete_delivery_compact_and_suppress_repost(
    tmp_path,
) -> None:
    database = tmp_path / "separated-services.sqlite"
    source = SourceObservation(
        channel_id="publisher",
        channel_handle="publisher",
        external_post_id="first",
        published_at=datetime.now(UTC),
        text=("Ethereum protocol security upgrade entered production for validators and users. ") * 5,
        urls=(UrlCandidate("https://example.test/protocol"),),
    )
    candidate_tokens: list[str] = []
    draft_tokens: list[str] = []

    class SheetsAdapter:
        def prepare_delivery(
            self,
            *,
            export_id,
            canonical_sha256,
            values,
        ):
            assert export_id
            assert len(canonical_sha256) == 64
            assert len(values) == 22
            return PreparedSheetMutation(
                {},
                canonical_sha256,
                metadata=MetadataState.ABSENT,
                metadata_value=f"newsbot-v2:{export_id}",
            )

        def dispatch_credential_attestation(self):
            return DispatchCredentialAttestation(
                refreshed_at="2026-08-11T00:00:00+00:00",
                expires_at="2026-08-11T01:00:00+00:00",
                scope_ok=True,
            )

        def arm_prepared_dispatch(self):
            return None

        def dispatch_prepared(self, _prepared):
            return SheetDelivery(DeliveryOutcome.APPLIED)

    with V2Workflow(database, mode="create") as workflow:
        candidate = create_candidate(workflow, source)
        assert candidate is not None
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=lambda _candidate, token: (
                candidate_tokens.append(token),
                "candidate-message",
            )[1],
            notify_draft=lambda _draft, token: (
                draft_tokens.append(token),
                "draft-message",
            )[1],
        )

        assert live.run(candidate.id).state == V2State.PENDING_CANDIDATE
        approved = live.settle_callback(
            candidate_tokens.pop(),
            "candidate",
        )
        assert approved is not None
        assert approved.state == V2State.CANDIDATE_APPROVED

        prepared = prepare_generation(workflow.get_candidate(candidate.id))
        request = workflow.prepare_codex_request(
            candidate.id,
            prepared.request_bytes,
            prepared.request_digest,
        )
        attempt = workflow.begin_codex_attempt(
            candidate.id,
            request.digest,
        )
        fact = prepared.request.facts[0]
        factual_unit = {
            "text": "검증된 프로토콜 보안 업그레이드입니다.",
            "references": [
                {
                    "claim_id": fact.id,
                    "source_version_id": fact.source_version_id,
                }
            ],
        }
        output = json.dumps(
            {
                "bodies": [],
                "caption": {
                    "context": "프로토콜 변경의 맥락입니다.",
                    "details": "검증된 변경 사항입니다.",
                    "hashtags": ["#Blockchain"],
                    "hook": "보안 업그레이드가 적용됐습니다.",
                    "implications": "검증자와 사용자에게 영향을 줍니다.",
                    "questions": "운영자는 무엇을 확인해야 할까요?",
                },
                "category": "Blockchain",
                "cover": {
                    "factual_units": [factual_unit],
                    "subtitle": "검증된 운영 변경",
                    "title": "프로토콜 보안 업그레이드",
                },
                "draft": True,
                "source_reported": True,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        draft = workflow.commit_codex_success(
            attempt.id,
            output,
            hashlib.sha256(output).hexdigest(),
        )

        assert live.run(candidate.id).id == draft.id
        approved_draft = live.settle_callback(
            draft_tokens.pop(),
            "draft",
        )
        assert approved_draft is not None
        assert approved_draft.state == V2State.DRAFT_APPROVED
        delivered = deliver_v2_google_sheets(
            workflow,
            approved_draft,
            SheetsAdapter(),
            v2_draft_handoff_values(
                approved_draft,
                "2026-08-11",
            ),
            lease_seconds=120,
        )
        assert delivered.state == V2State.SHEET_DELIVERED

        old = (datetime.now(UTC) - timedelta(days=31)).isoformat()
        workflow._db.execute(
            "UPDATE v2_observation_revisions SET created_at=?",
            (old,),
        )
        workflow._db.execute(
            "UPDATE v2_candidates SET created_at=?,updated_at=?",
            (old, old),
        )
        workflow._db.execute(
            "UPDATE v2_drafts SET created_at=?,updated_at=?",
            (old, old),
        )
        workflow._db.execute(
            "UPDATE v2_stories SET delivered_at=?",
            (old,),
        )
        workflow._db.execute(
            "UPDATE v2_story_claims SET delivered_at=?",
            (old,),
        )
        workflow._db.execute(
            "UPDATE v2_remote_effects SET updated_at=?",
            (old,),
        )
        workflow._db.commit()
        assert workflow.compact(now=datetime.now(UTC).isoformat())["compacted"] > 0

    repost = SourceObservation(
        channel_id="publisher",
        channel_handle="publisher",
        external_post_id="repost",
        published_at=datetime.now(UTC),
        text=source.text,
        urls=source.urls,
    )
    with V2Workflow(
        database,
        mode="runtime",
    ) as reopened:
        assert create_candidate(reopened, repost) is None
        assert len(reopened.list_candidates()) == 1
        assert reopened.verify_invariants() == {
            "candidate_binding_mismatches": 0,
            "delivered_marker_mismatches": 0,
            "tombstone_digest_mismatches": 0,
        }

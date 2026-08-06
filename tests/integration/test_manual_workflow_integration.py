"""Synthetic end-to-end checks for the local-only manual workflow."""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from argparse import Namespace
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from newsbot import manual
from newsbot.storage import Storage

_REPOSITORY = Path(__file__).resolve().parents[2]


def _private_workspace() -> Path:
    root = Path(tempfile.mkdtemp(prefix=".newsbot-manual-test-", dir=Path.home()))
    root.chmod(0o700)
    return root


def _write_profile(root: Path) -> Path:
    bundled = (_REPOSITORY / "config" / "channels.toml").read_text()
    policy = bundled[bundled.index("[policy]") :]
    profile = root / "profile.toml"
    profile.write_text(
        """schema = "newsbot.behavior.v1"
operation = "manual_local"

[[sources]]
id = "synthetic-source"
name = "Synthetic Source"
enabled = true
priority = 100
source_quality = 1.0
classification = "official"
official_domains = ["example.com"]
original_domains = []
"""
        + policy
    )
    return profile


def _args(profile: Path, state: Path, **extra: object) -> Namespace:
    return Namespace(profile=profile, state=str(state), database="newsbot.sqlite3", **extra)


def _remote_authority_counts(storage: Storage) -> dict[str, int]:
    tables = (
        "callback_tokens",
        "telegram_notification_outbox",
        "sheet_handoffs",
        "automation_cutovers",
        "automation_stream_runs",
    )
    return {table: int(storage.fetch_one(f"SELECT COUNT(*) AS n FROM {table}")["n"]) for table in tables}


def test_manual_local_workflow_is_private_durable_and_idempotent() -> None:
    root = _private_workspace()
    try:
        profile = _write_profile(root)
        state = root / "state"
        output = root / "output"
        output.mkdir(mode=0o700)

        assert manual.manual_init(_args(profile, state)) == 0
        database = state / "newsbot.sqlite3"
        assert database.exists()
        assert state.stat().st_mode & 0o777 == 0o700
        assert database.stat().st_mode & 0o777 == 0o600

        imported = root / "observations.json"
        imported.write_text(
            json.dumps(
                {
                    "schema": "newsbot.manual.import.v1",
                    "records": [
                        {
                            "source_id": "synthetic-source",
                            "post_id": "synthetic-1",
                            "published_at": datetime.now(UTC).isoformat(),
                            "text": (
                                "인공지능 연구팀이 합성 데이터만 사용하는 공개 평가 도구를 발표했습니다. "
                                "이 도구는 재현 가능한 로컬 실행과 검증 가능한 결과 내보내기를 지원하며, "
                                "개인정보나 외부 서비스 권한 없이 개발자가 안전하게 시험할 수 있도록 설계됐습니다."
                            ),
                            "urls": ["https://example.com/news/synthetic-1"],
                            "views": 1000,
                            "reactions": 100,
                            "forwards": 20,
                        }
                    ],
                }
            )
        )
        imported.chmod(0o600)
        assert manual.manual_import(_args(profile, state, input=imported)) == 0
        assert manual.manual_rank(_args(profile, state)) == 0

        with Storage.open(database) as storage:
            candidate = storage.fetch_one(
                "SELECT c.id,ce.run_id FROM candidates c "
                "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
                "WHERE c.status='pending_selection'"
            )
            assert candidate is not None
            candidate_id = int(candidate["id"])
            run_id = int(candidate["run_id"])
            assert _remote_authority_counts(storage) == {
                "callback_tokens": 0,
                "telegram_notification_outbox": 0,
                "sheet_handoffs": 0,
                "automation_cutovers": 0,
                "automation_stream_runs": 0,
            }
        with Storage.open(database) as storage:
            _, expected_receipt = storage.manual_candidate_preview(run_id)

        decision_args = _args(
            profile,
            state,
            run_id=run_id,
            candidate_id=candidate_id,
            decision="select",
            expected_receipt=expected_receipt,
        )
        assert manual.manual_candidate_decision(decision_args) == 0
        assert manual.manual_candidate_decision(decision_args) == 0
        with Storage.open(database) as storage:
            assert (
                storage.fetch_one(
                    "SELECT candidate_preview_receipt FROM manual_candidate_decisions WHERE candidate_id=?",
                    (candidate_id,),
                )["candidate_preview_receipt"]
                == expected_receipt
            )
        assert (
            manual.manual_generate(
                _args(
                    profile,
                    state,
                    candidate_id=candidate_id,
                    provider="fake",
                    page_count=2,
                    output_dir=str(output),
                )
            )
            == 0
        )

        with Storage.open(database) as storage:
            generation = storage.fetch_one(
                "SELECT g.id FROM generations g "
                "JOIN generation_jobs j ON j.id=g.generation_job_id "
                "JOIN selections s ON s.id=j.selection_id "
                "WHERE s.candidate_id=? AND g.status='current'",
                (candidate_id,),
            )
            assert generation is not None
            generation_id = int(generation["id"])
        assert manual.manual_draft(_args(profile, state, generation_id=generation_id, output_dir=str(output))) == 0

        draft_digest = (output / f"draft-{generation_id}.json").read_bytes()
        expected_draft_digest = __import__("hashlib").sha256(draft_digest).hexdigest()
        review_args = _args(
            profile,
            state,
            candidate_id=candidate_id,
            generation_id=generation_id,
            decision="approve-local",
            expected_draft_digest=expected_draft_digest,
        )
        assert manual.manual_review(review_args) == 0
        assert manual.manual_review(review_args) == 0
        assert manual.manual_export(_args(profile, state, output_dir=str(output))) == 0
        assert manual.manual_export(_args(profile, state, output_dir=str(output))) == 0

        with Storage.open(database) as storage:
            assert (
                storage.fetch_one("SELECT status FROM candidates WHERE id=?", (candidate_id,))["status"] == "approved"
            )
            assert (
                int(
                    storage.fetch_one(
                        "SELECT COUNT(*) AS n FROM manual_local_export_outbox WHERE state='materialized'"
                    )["n"]
                )
                == 2
            )
            assert _remote_authority_counts(storage) == {
                "callback_tokens": 0,
                "telegram_notification_outbox": 0,
                "sheet_handoffs": 0,
                "automation_cutovers": 0,
                "automation_stream_runs": 0,
            }
        assert len(tuple(output.glob("export-*"))) == 2
    finally:
        shutil.rmtree(root)


def test_constructor_guard_refuses_migration_before_schema_touch(tmp_path: Path) -> None:
    database = tmp_path / "blocked.sqlite3"

    @contextmanager
    def mismatch(phase: str) -> object:
        if phase == "migration_setup":
            raise RuntimeError("state_path_changed")
        yield

    try:
        Storage.open(database, phase_guard=mismatch)
    except RuntimeError as error:
        assert str(error) == "state_path_changed"
    else:
        raise AssertionError("pre-migration attestation must refuse migration")

    with sqlite3.connect(database) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
            ).fetchone()[0]
            == 0
        )


def test_candidate_preview_filters_mixed_status_and_preserves_bounds(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "preview.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", "a" * 64)
        with storage.transaction() as connection:
            run_id = int(
                connection.execute(
                    "INSERT INTO runs(run_key,mode,status) VALUES('preview-run','manual','done')"
                ).lastrowid
            )
            post_id = int(
                connection.execute(
                    "INSERT INTO source_posts(channel_id,external_post_id,source_url) "
                    "VALUES('synthetic-source','post-1','https://example.com/post-1')"
                ).lastrowid
            )
            version_id = int(
                connection.execute(
                    "INSERT INTO source_post_versions("
                    "source_post_id,version_key,body,published_at,urls_json"
                    ") VALUES(?,'v1',?,'2026-01-01T00:00:00+00:00',?)",
                    (post_id, "x" * 1_200, json.dumps([f"https://example.com/{index}" for index in range(10)])),
                ).lastrowid
            )
            connection.executemany(
                "INSERT INTO candidate_evaluations("
                "id,run_id,source_post_version_id,source_set_key,evaluator_version,score"
                ") VALUES(?,?,?,?,?,'1.000000')",
                (
                    (1, run_id, version_id, "pending", "manual-v1"),
                    (2, run_id, version_id, "rejected", "manual-v1"),
                ),
            )
            connection.executemany(
                "INSERT INTO candidates(id,evaluation_id,status,rank) VALUES(?,?,?,NULL)",
                ((1, 1, "pending_selection"), (2, 2, "rejected")),
            )
            connection.executemany(
                "INSERT INTO candidate_sources(candidate_id,source_post_version_id) VALUES(?,?)",
                ((1, version_id), (2, version_id)),
            )

        statements: list[str] = []
        storage._connection.set_trace_callback(statements.append)
        payload, receipt = storage.manual_candidate_preview(run_id)
        storage._connection.set_trace_callback(None)
        assert payload["receipt"] == receipt
        assert [candidate["id"] for candidate in payload["candidates"]] == [1]
        assert len(statements) == 2
        assert (
            sum("SELECT c.id,c.status,c.rank,ce.score FROM candidates c" in statement for statement in statements) == 1
        )
        assert (
            sum(
                "WITH selected AS (" in statement and "FROM selected JOIN candidate_sources cs" in statement
                for statement in statements
            )
            == 1
        )
        candidate = payload["candidates"][0]
        assert candidate["rank"] is None
        source = candidate["sources"][0]
        assert len(source["title"]) == 240
        assert len(source["context"]) == 1_000
        assert len(source["urls"]) == 8


def test_candidate_preview_refuses_more_than_ten_thousand_rows(tmp_path: Path) -> None:
    with Storage.open(tmp_path / "preview-overflow.sqlite3") as storage:
        storage.bind_manual_profile("newsbot.behavior.v1", "a" * 64)
        with storage.transaction() as connection:
            run_id = int(
                connection.execute(
                    "INSERT INTO runs(run_key,mode,status) VALUES('overflow-run','manual','done')"
                ).lastrowid
            )
            post_id = int(
                connection.execute(
                    "INSERT INTO source_posts(channel_id,external_post_id) VALUES('synthetic-source','post-1')"
                ).lastrowid
            )
            version_id = int(
                connection.execute(
                    "INSERT INTO source_post_versions(source_post_id,version_key,body) VALUES(?,'v1','body')",
                    (post_id,),
                ).lastrowid
            )
            rows = range(1, 10_002)
            connection.executemany(
                "INSERT INTO candidate_evaluations("
                "id,run_id,source_post_version_id,source_set_key,evaluator_version,score"
                ") VALUES(?,?,?,?,?,'1.000000')",
                ((index, run_id, version_id, f"source-{index}", "manual-v1") for index in rows),
            )
            connection.executemany(
                "INSERT INTO candidates(id,evaluation_id,status) VALUES(?,?,'pending_selection')",
                ((index, index) for index in range(1, 10_002)),
            )
            connection.executemany(
                "INSERT INTO candidate_sources(candidate_id,source_post_version_id) VALUES(?,?)",
                ((index, version_id) for index in range(1, 10_002)),
            )

        try:
            storage.manual_candidate_preview(run_id)
        except ValueError as error:
            assert str(error) == "manual candidate preview exceeds bounds"
        else:
            raise AssertionError("oversized candidate previews must fail closed")


def test_public_candidate_reject_and_review_reject_transitions() -> None:
    root = _private_workspace()
    try:
        profile = _write_profile(root)
        state = root / "state"
        output = root / "output"
        output.mkdir(mode=0o700)
        assert manual.manual_init(_args(profile, state)) == 0
        database = state / "newsbot.sqlite3"

        with Storage.open(database) as storage, storage.transaction() as connection:
            run_id = int(
                connection.execute(
                    "INSERT INTO runs(run_key,mode,status) VALUES('public-reject-run','manual','done')"
                ).lastrowid
            )
            post_id = int(
                connection.execute(
                    "INSERT INTO source_posts(channel_id,external_post_id) "
                    "VALUES('synthetic-source','public-reject-post')"
                ).lastrowid
            )
            version_id = int(
                connection.execute(
                    "INSERT INTO source_post_versions("
                    "source_post_id,version_key,body,published_at,urls_json"
                    ") VALUES(?,'v1',?,'2026-01-01T00:00:00+00:00','[\"https://example.com/reject\"]')",
                    (
                        post_id,
                        "인공지능 공개 평가 도구가 합성 입력을 검증하고 안전한 로컬 결과를 생성하도록 발표됐습니다.",
                    ),
                ).lastrowid
            )
            connection.executemany(
                "INSERT INTO candidate_evaluations("
                "id,run_id,source_post_version_id,source_set_key,evaluator_version,score"
                ") VALUES(?,?,?,?,?,'1.000000')",
                (
                    (1, run_id, version_id, "reject-candidate", "manual-v1"),
                    (2, run_id, version_id, "review-candidate", "manual-v1"),
                ),
            )
            connection.executemany(
                "INSERT INTO candidates(id,evaluation_id,status,rank) VALUES(?,?, 'pending_selection',?)",
                ((1, 1, 1), (2, 2, 2)),
            )
            connection.executemany(
                "INSERT INTO candidate_sources(candidate_id,source_post_version_id) VALUES(?,?)",
                ((1, version_id), (2, version_id)),
            )

        assert manual.manual_candidates(_args(profile, state, run_id=run_id, output_dir=str(output))) == 0
        first_preview = json.loads(next(output.glob(f"candidates-{run_id}-*.json")).read_text())
        assert (
            manual.manual_candidate_decision(
                _args(
                    profile,
                    state,
                    run_id=run_id,
                    candidate_id=1,
                    decision="reject",
                    expected_receipt=first_preview["receipt"],
                )
            )
            == 0
        )

        before = set(output.glob(f"candidates-{run_id}-*.json"))
        assert manual.manual_candidates(_args(profile, state, run_id=run_id, output_dir=str(output))) == 0
        second_preview = json.loads((set(output.glob(f"candidates-{run_id}-*.json")) - before).pop().read_text())
        assert (
            manual.manual_candidate_decision(
                _args(
                    profile,
                    state,
                    run_id=run_id,
                    candidate_id=2,
                    decision="select",
                    expected_receipt=second_preview["receipt"],
                )
            )
            == 0
        )
        assert (
            manual.manual_generate(
                _args(profile, state, candidate_id=2, provider="fake", page_count=1, output_dir=str(output))
            )
            == 0
        )
        with Storage.open(database) as storage:
            generation_id = int(
                storage.fetch_one(
                    "SELECT g.id FROM generations g "
                    "JOIN generation_jobs j ON j.id=g.generation_job_id "
                    "JOIN selections s ON s.id=j.selection_id "
                    "WHERE s.candidate_id=2 AND g.status='current'"
                )["id"]
            )
        assert manual.manual_draft(_args(profile, state, generation_id=generation_id, output_dir=str(output))) == 0
        draft_digest = __import__("hashlib").sha256((output / f"draft-{generation_id}.json").read_bytes()).hexdigest()
        assert (
            manual.manual_review(
                _args(
                    profile,
                    state,
                    candidate_id=2,
                    generation_id=generation_id,
                    decision="reject",
                    expected_draft_digest=draft_digest,
                )
            )
            == 0
        )
        with Storage.open(database) as storage:
            assert storage.fetch_one("SELECT status FROM candidates WHERE id=1")["status"] == "rejected"
            assert storage.fetch_one("SELECT status FROM candidates WHERE id=2")["status"] == "rejected"
            assert (
                storage.fetch_one(
                    "SELECT decision FROM manual_local_decisions WHERE generation_id=?", (generation_id,)
                )["decision"]
                == "reject"
            )
    finally:
        shutil.rmtree(root)

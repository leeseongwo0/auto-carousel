"""Offline-capable, selection-bound workflow from collection through approval."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from secrets import token_hex
from typing import Any, Protocol

from .ai.base import FactClaim, GenerationProvider, GenerationRequest
from .candidates import CandidateApprovalService, CandidateDigest
from .collectors.base import SourceObservation
from .copywriting import CopyDraft, adaptive_page_count, validate_copy
from .exports import source_claim_identity, source_identity, source_material_identity, source_observation_identity
from .ranking import Evaluation, evaluate_candidates
from .runtime import Clock
from .storage import Storage, has_newer_material_source, persist_observation


class FixtureObservationCollector(Protocol):
    """Collect deterministic observations without a channel argument."""

    def collect(self) -> Sequence[SourceObservation]: ...


@dataclass(frozen=True, slots=True)
class CandidateStageResult:
    run_id: int
    digest: CandidateDigest


@dataclass(frozen=True, slots=True)
class GenerationResult:
    candidate_id: int
    generation_id: int
    draft: CopyDraft
    source_version_ids: tuple[int, ...]
    reused: bool


class NewsPipeline:
    """Persist a deterministic candidate workflow; providers are called only after selection."""

    def __init__(
        self,
        storage: Storage,
        config: object,
        provider: GenerationProvider | Callable[[], GenerationProvider],
        clock: Clock,
    ) -> None:
        self.storage = storage
        self.config = config
        self._provider_factory = provider if callable(provider) else lambda: provider
        self.clock = clock

    async def run_fixture(
        self,
        collector: FixtureObservationCollector,
        *,
        approval_service: CandidateApprovalService,
        actor_id: int,
        run_key: str | None = None,
    ) -> CandidateStageResult:
        observations = tuple(collector.collect())
        fixture_key = run_key or _fixture_key(observations, self.config)
        return await self.run(observations, run_key=fixture_key, approval_service=approval_service, actor_id=actor_id)

    async def run(
        self,
        observations: Sequence[SourceObservation],
        *,
        run_key: str,
        approval_service: CandidateApprovalService,
        actor_id: int,
    ) -> CandidateStageResult:
        """Collect, rank, and expose candidates without selecting or generating."""
        now = _utc(self.clock.now())
        existing_run = self.storage.fetch_one("SELECT id, status FROM runs WHERE run_key=?", (run_key,))
        run_id = self._run_id(run_key, now)
        observation_ids = self._persist_sources(observations, now)
        self._invalidate_revised_candidates(run_id, observation_ids)
        canonical_observations = tuple(
            observation
            for observation in self.storage.latest_observations()
            if (observation.channel_id, observation.external_post_id) in observation_ids
        )
        evaluations = evaluate_candidates(
            canonical_observations, self.config, now, self._candidate_history(run_id, now)
        )
        self._persist_candidates(run_id, evaluations, canonical_observations, observation_ids, now)
        if (
            existing_run is not None
            and self.storage.fetch_one(
                "SELECT 1 FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
                "WHERE ce.run_id=? AND c.status='pending_selection'",
                (run_id,),
            )
            is None
        ):
            return CandidateStageResult(run_id, self._existing_digest(run_id))
        digest = approval_service.create_digest(run_id, actor_id=actor_id)
        return CandidateStageResult(run_id, digest)

    def _run_id(self, run_key: str, now: datetime) -> int:
        config_hash = getattr(self.config, "digest", None)
        with self.storage.transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO runs(run_key, mode, status, config_hash, started_at) VALUES (?, 'fixture', 'running', ?, ?)",
                (run_key, config_hash, now.isoformat()),
            )
            row = connection.execute("SELECT id FROM runs WHERE run_key = ?", (run_key,)).fetchone()
            assert row is not None
            return int(row["id"])

    def _existing_digest(self, run_id: int) -> CandidateDigest:
        row = self.storage.fetch_one(
            "SELECT id FROM digests WHERE run_id=? ORDER BY id DESC LIMIT 1",
            (run_id,),
        )
        if row is None:
            raise RuntimeError("ready fixture run has no candidate digest")
        return CandidateDigest(int(row["id"]), run_id, 1, (), {})

    def _persist_sources(self, observations: Sequence[SourceObservation], now: datetime) -> dict[tuple[str, str], int]:
        result: dict[tuple[str, str], int] = {}
        with self.storage.transaction() as connection:
            for observation in observations:
                key = (observation.channel_id, observation.external_post_id)
                result[key] = persist_observation(connection, observation, now, return_observation_id=True)
        return result

    def _invalidate_revised_candidates(self, run_id: int, source_ids: dict[tuple[str, str], int]) -> None:
        """Supersede nonterminal candidates whose bound material is no longer current."""
        observation_ids = tuple(sorted(set(source_ids.values())))
        if not observation_ids:
            return
        marks = ",".join("?" for _ in observation_ids)
        with self.storage.transaction() as connection:
            stale = list(
                connection.execute(
                    "SELECT DISTINCT c.id FROM candidates c "
                    "JOIN candidate_sources cs ON cs.candidate_id=c.id "
                    "JOIN source_post_versions old ON old.id=cs.source_post_version_id "
                    "JOIN source_post_observations current ON current.id IN (" + marks + ") "
                    "AND current.source_post_id=old.source_post_id "
                    "WHERE old.id != current.source_post_version_id "
                    "AND c.status NOT IN ('superseded', 'approved')",
                    observation_ids,
                )
            )
            candidate_ids = tuple(
                int(row["id"])
                for row in stale
                if has_newer_material_source(
                    connection,
                    _candidate_source_ids(connection, int(row["id"])),
                )
            )
            if not candidate_ids:
                return
            candidate_marks = ",".join("?" for _ in candidate_ids)
            connection.execute(
                f"UPDATE candidates SET status='superseded', revision=revision+1 WHERE id IN ({candidate_marks})",
                candidate_ids,
            )
            connection.execute(
                "UPDATE digests SET status='superseded', revision=revision+1 WHERE status='active' AND run_id IN ("
                "SELECT DISTINCT ce.run_id FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
                f"WHERE c.id IN ({candidate_marks}))",
                candidate_ids,
            )
            connection.execute(
                "UPDATE generation_jobs SET status='superseded', finished_at=? WHERE selection_id IN ("
                f"SELECT id FROM selections WHERE candidate_id IN ({candidate_marks})) "
                "AND status IN ('queued', 'running', 'failed_recoverable')",
                (_utc(self.clock.now()).isoformat(), *candidate_ids),
            )
            connection.execute(
                "UPDATE generations SET status='superseded' WHERE generation_job_id IN ("
                f"SELECT id FROM generation_jobs WHERE selection_id IN "
                f"(SELECT id FROM selections WHERE candidate_id IN ({candidate_marks}))) AND status='current'",
                candidate_ids,
            )

    def _candidate_history(self, run_id: int, evaluated_at: datetime) -> tuple[dict[str, object], ...]:
        """Read prior persisted identities inside the configured novelty window."""
        days = int(getattr(getattr(self.config, "policy", None), "novelty_window_days", 0))
        if days <= 0:
            raise ValueError("ranking requires a positive novelty window")
        cutoff = (evaluated_at - timedelta(days=days)).isoformat()
        rows = self.storage.fetch_all(
            "SELECT c.id AS candidate_id, c.status, ce.rationale_json FROM candidates c "
            "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
            "WHERE ce.run_id != ? AND ce.evaluated_at >= ? ORDER BY c.id",
            (run_id, cutoff),
        )
        history: list[dict[str, object]] = []
        for row in rows:
            try:
                rationale = json.loads(row["rationale_json"] or "{}")
            except json.JSONDecodeError:
                continue
            story_key = rationale.get("story_key")
            content_key = rationale.get("content_key")
            if isinstance(story_key, str) and isinstance(content_key, str):
                history.append(
                    {
                        "candidate_id": int(row["candidate_id"]),
                        "status": str(row["status"]),
                        "story_key": story_key,
                        "content_key": content_key,
                    }
                )
        return tuple(history)

    def _persist_candidates(
        self,
        run_id: int,
        evaluations: Sequence[Evaluation],
        observations: Sequence[SourceObservation],
        source_ids: dict[tuple[str, str], int],
        now: datetime,
    ) -> None:
        with self.storage.transaction() as connection:
            for rank, evaluation in enumerate(evaluations, start=1):
                observation_ids = tuple(
                    sorted(source_ids[(item.channel_id, item.external_post_id)] for item in evaluation.observations)
                )
                evidence_rows = tuple(
                    connection.execute(
                        "SELECT observation.source_post_version_id, observation.observation_key, version.version_key, "
                        "post.channel_id, post.external_post_id "
                        "FROM source_post_observations observation "
                        "JOIN source_post_versions version ON version.id=observation.source_post_version_id "
                        "JOIN source_posts post ON post.id=version.source_post_id "
                        f"WHERE observation.id IN ({','.join('?' for _ in observation_ids)})",
                        observation_ids,
                    )
                )
                version_ids = tuple(sorted(int(row["source_post_version_id"]) for row in evidence_rows))
                version_id = version_ids[0]
                source_records = sorted(
                    (
                        {
                            "schema_version": "newsbot-source-set-member-v1",
                            "channel_id": str(row["channel_id"]),
                            "external_post_id": str(row["external_post_id"]),
                            "version_key": str(row["version_key"]),
                        }
                        for row in evidence_rows
                    ),
                    key=lambda item: (item["channel_id"], item["external_post_id"], item["version_key"]),
                )
                observation_records = sorted(
                    (
                        {
                            "schema_version": "newsbot-observation-set-member-v1",
                            "channel_id": str(row["channel_id"]),
                            "external_post_id": str(row["external_post_id"]),
                            "version_key": str(row["version_key"]),
                            "observation_key": str(row["observation_key"]),
                        }
                        for row in evidence_rows
                    ),
                    key=lambda item: (
                        item["channel_id"],
                        item["external_post_id"],
                        item["version_key"],
                        item["observation_key"],
                    ),
                )
                source_set_key = sha256(
                    json.dumps(source_records, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                observation_set_key = sha256(
                    json.dumps(observation_records, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                rationale = {
                    "story_key": evaluation.story_key,
                    "content_key": evaluation.content_key,
                    "eligible": evaluation.eligible,
                    "reasons": evaluation.reasons,
                    "evidence_bindings": observation_records,
                    "rationale": _safe_rationale(evaluation.rationale),
                }
                evaluator_version = getattr(getattr(self.config, "policy", None), "version", "candidate_policy_v1")
                connection.execute(
                    "INSERT OR IGNORE INTO candidate_evaluations("
                    "run_id, source_post_version_id, source_post_observation_id, source_set_key, observation_set_key, "
                    "evaluator_version, score, rationale_json, evaluated_at"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        run_id,
                        version_id,
                        observation_ids[0],
                        source_set_key,
                        observation_set_key,
                        evaluator_version,
                        format(evaluation.total or Decimal("0"), ".6f"),
                        json.dumps(rationale, ensure_ascii=False, sort_keys=True, default=str),
                        now.isoformat(),
                    ),
                )
                row = connection.execute(
                    "SELECT id FROM candidate_evaluations WHERE run_id=? AND source_set_key=? AND evaluator_version=?",
                    (run_id, source_set_key, evaluator_version),
                ).fetchone()
                assert row is not None
                connection.execute(
                    "INSERT OR IGNORE INTO candidates(evaluation_id, status, rank) VALUES (?, ?, ?)",
                    (
                        row["id"],
                        "pending_selection" if evaluation.eligible else "rejected",
                        rank if evaluation.eligible else None,
                    ),
                )
                candidate = connection.execute(
                    "SELECT id FROM candidates WHERE evaluation_id=?", (row["id"],)
                ).fetchone()
                assert candidate is not None
                connection.executemany(
                    "INSERT OR IGNORE INTO candidate_sources(candidate_id, source_post_version_id) VALUES (?, ?)",
                    ((int(candidate["id"]), version_id) for version_id in version_ids),
                )

    async def generate_selected(self, candidate_id: int, *, page_count: int) -> GenerationResult:
        """Lease one selected job before constructing or calling its provider."""
        if not 1 <= page_count <= 8:
            raise ValueError("page_count must be between 1 and 8")
        now = _utc(self.clock.now())
        lease_token = token_hex(32)
        with self.storage.transaction() as connection:
            candidate = connection.execute(
                "SELECT c.status, ce.source_post_version_id, ce.run_id FROM candidates c "
                "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
                (candidate_id,),
            ).fetchone()
            source_ids = _candidate_source_ids(connection, candidate_id)
            if candidate is None:
                raise ValueError("candidate has no selected generation job")
            if candidate["status"] not in ("selected_generation_pending", "pending_review"):
                raise ValueError("candidate is not eligible for generation")
            job = connection.execute(
                "SELECT j.id, j.selection_id, j.job_kind, j.requested_page_count FROM generation_jobs j "
                "JOIN selections s ON s.id=j.selection_id WHERE s.candidate_id=? "
                "AND (j.status IN ('queued', 'failed_recoverable') OR "
                "(j.status='running' AND j.lease_expires_at < ?)) "
                "ORDER BY CASE j.job_kind WHEN 'initial' THEN 0 ELSE 1 END, j.id LIMIT 1",
                (candidate_id, now.isoformat()),
            ).fetchone()
            if job is None:
                generation = connection.execute(
                    "SELECT g.id, g.content_json FROM generations g JOIN generation_jobs j ON j.id=g.generation_job_id "
                    "JOIN selections s ON s.id=j.selection_id WHERE s.candidate_id=? AND g.status='current' "
                    "ORDER BY g.id DESC LIMIT 1",
                    (candidate_id,),
                ).fetchone()
                if generation is None:
                    raise ValueError("candidate has no queued generation job")
                return GenerationResult(
                    candidate_id,
                    int(generation["id"]),
                    _draft_from_payload(json.loads(generation["content_json"])),
                    source_ids,
                    True,
                )
            if (
                connection.execute(
                    "SELECT 1 FROM generation_job_provider_bindings WHERE generation_job_id=? AND provider_name='codex_cli'",
                    (int(job["id"]),),
                ).fetchone()
                is not None
            ):
                raise ValueError("Codex-bound generation jobs require generate-codex-once")
            job_sources = tuple(
                int(row["source_post_version_id"])
                for row in connection.execute(
                    "SELECT source_post_version_id FROM generation_sources "
                    "WHERE generation_job_id=? AND generation_id IS NULL ORDER BY source_post_version_id",
                    (int(job["id"]),),
                )
            )
            if job_sources and job_sources != source_ids:
                raise ValueError("generation job source binding is stale")
            requested_page_count = (
                int(job["requested_page_count"])
                if job["requested_page_count"] is not None
                else _job_page_count(str(job["job_kind"]), page_count)
            )
            lease_until = now + timedelta(minutes=5)
            connection.execute(
                "UPDATE generation_provider_attempts SET finished_at=?, terminal_outcome='abandoned', "
                "error_message='LeaseExpired: generation failed' WHERE generation_job_id=? "
                "AND terminal_outcome IS NULL",
                (now.isoformat(), int(job["id"])),
            )
            updated = connection.execute(
                "UPDATE generation_jobs SET status='running', attempts=attempts+1, lease_token=?, "
                "lease_expires_at=?, started_at=?, retry_at=NULL, error_message=NULL, "
                "requested_page_count=COALESCE(requested_page_count, ?) WHERE id=? "
                "AND (status IN ('queued', 'failed_recoverable') OR "
                "(status='running' AND lease_expires_at < ?))",
                (
                    lease_token,
                    lease_until.isoformat(),
                    now.isoformat(),
                    requested_page_count,
                    int(job["id"]),
                    now.isoformat(),
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("generation job lease was lost")
            attempt = int(
                connection.execute("SELECT attempts FROM generation_jobs WHERE id=?", (int(job["id"]),)).fetchone()[0]
            )
            connection.execute(
                "INSERT INTO generation_provider_attempts(generation_job_id, attempt, started_at) VALUES (?, ?, ?)",
                (int(job["id"]), attempt, now.isoformat()),
            )
            provider_attempt_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO pipeline_events("
                "run_id, selection_id, generation_job_id, candidate_id, provider_attempt_id, event_kind, created_at"
                ") VALUES (?, ?, ?, ?, ?, 'provider_call', ?)",
                (
                    int(candidate["run_id"]),
                    int(job["selection_id"]),
                    int(job["id"]),
                    candidate_id,
                    provider_attempt_id,
                    now.isoformat(),
                ),
            )
            connection.execute("UPDATE candidates SET status='selected_generation_pending' WHERE id=?", (candidate_id,))
        expected_page_count = requested_page_count
        facts = self._facts(source_ids)
        try:
            provider = self._provider_factory()
            draft = await provider.generate(GenerationRequest(candidate_id, source_ids, expected_page_count, facts))
            validate_copy(
                draft,
                allowed_claim_sources={fact.id: fact.source_version_id for fact in facts},
                expected_page_count=expected_page_count,
            )
            generation_payload = _draft_payload(draft)
            generation_payload["claim_manifest"] = [_fact_payload(fact) for fact in facts]
        except Exception as exc:
            finished_at = _utc(self.clock.now())
            with self.storage.transaction() as connection:
                connection.execute(
                    "UPDATE generation_provider_attempts SET finished_at=?, terminal_outcome='failed', error_message=? "
                    "WHERE id=? AND terminal_outcome IS NULL AND EXISTS ("
                    "SELECT 1 FROM generation_jobs WHERE id=? AND status='running' AND lease_token=?)",
                    (
                        finished_at.isoformat(),
                        _redacted_error(exc),
                        provider_attempt_id,
                        int(job["id"]),
                        lease_token,
                    ),
                )
                connection.execute(
                    "UPDATE generation_jobs SET status='failed_recoverable', finished_at=?, retry_at=?, "
                    "lease_token=NULL, lease_expires_at=NULL, error_message=? "
                    "WHERE id=? AND status='running' AND lease_token=?",
                    (
                        finished_at.isoformat(),
                        finished_at.isoformat(),
                        _redacted_error(exc),
                        int(job["id"]),
                        lease_token,
                    ),
                )
            raise
        with self.storage.transaction() as connection:
            current = connection.execute(
                "SELECT id, content_json FROM generations WHERE generation_job_id=? AND status='current'",
                (int(job["id"]),),
            ).fetchone()
            active = connection.execute(
                "SELECT 1 FROM generation_jobs WHERE id=? AND status='running' AND lease_token=?",
                (int(job["id"]), lease_token),
            ).fetchone()
            if active is None:
                if current is None:
                    raise RuntimeError("generation lease was lost before commit")
                return GenerationResult(
                    candidate_id,
                    int(current["id"]),
                    _draft_from_payload(json.loads(current["content_json"])),
                    source_ids,
                    True,
                )
            if current is None:
                attempt = int(
                    connection.execute("SELECT attempts FROM generation_jobs WHERE id=?", (int(job["id"]),)).fetchone()[
                        0
                    ]
                )
                connection.execute(
                    "INSERT INTO generations(generation_job_id, attempt, status, content_json, created_at) "
                    "VALUES (?, ?, 'current', ?, ?)",
                    (
                        int(job["id"]),
                        attempt,
                        json.dumps(generation_payload, ensure_ascii=False, sort_keys=True),
                        now.isoformat(),
                    ),
                )
                generation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
                connection.executemany(
                    "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) VALUES (?, ?, ?)",
                    ((int(job["id"]), generation_id, source_id) for source_id in source_ids),
                )
            else:
                generation_id = int(current["id"])
            finished_at = _utc(self.clock.now())
            connection.execute(
                "UPDATE generation_provider_attempts SET finished_at=?, terminal_outcome='succeeded' "
                "WHERE id=? AND terminal_outcome IS NULL AND EXISTS ("
                "SELECT 1 FROM generation_jobs WHERE id=? AND status='running' AND lease_token=?)",
                (finished_at.isoformat(), provider_attempt_id, int(job["id"]), lease_token),
            )
            connection.execute(
                "UPDATE generation_jobs SET status='succeeded', finished_at=?, lease_token=NULL, lease_expires_at=NULL "
                "WHERE id=? AND status='running' AND lease_token=?",
                (finished_at.isoformat(), int(job["id"]), lease_token),
            )
            connection.execute(
                "UPDATE candidates SET status='pending_review' WHERE id=? AND status='selected_generation_pending'",
                (candidate_id,),
            )
        return GenerationResult(candidate_id, generation_id, draft, source_ids, False)

    def select_codex_job_id(self) -> int | None:
        """Bind and return the globally highest-priority admissible Codex job."""
        now = _utc(self.clock.now()).isoformat()
        with self.storage.transaction() as connection:
            control = connection.execute(
                "SELECT paused_at FROM generation_provider_controls WHERE provider_name='codex_cli'"
            ).fetchone()
            if control is None or control["paused_at"] is not None:
                return None
            selected = self._selected_codex_job_id(connection, now)
            if selected is not None:
                return selected
            row = connection.execute(
                "SELECT j.id FROM generation_jobs j "
                "JOIN selections s ON s.id=j.selection_id "
                "JOIN candidates c ON c.id=s.candidate_id "
                "LEFT JOIN generation_job_provider_bindings b ON b.generation_job_id=j.id "
                "WHERE b.generation_job_id IS NULL AND j.status='queued' "
                "AND c.status IN ('selected_generation_pending','pending_review') "
                "ORDER BY j.requested_at, j.id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            selected = int(row["id"])
            connection.execute(
                "INSERT INTO generation_job_provider_bindings(generation_job_id,provider_name) VALUES (?,'codex_cli')",
                (selected,),
            )
            return selected

    @staticmethod
    def _selected_codex_job_id(connection: Any, now: str) -> int | None:
        row = connection.execute(
            "SELECT j.id FROM generation_jobs j "
            "JOIN generation_job_provider_bindings b "
            "ON b.generation_job_id=j.id AND b.provider_name='codex_cli' "
            "JOIN selections s ON s.id=j.selection_id "
            "JOIN candidates c ON c.id=s.candidate_id "
            "LEFT JOIN generation_job_retry_state r ON r.generation_job_id=j.id "
            "WHERE c.status IN ('selected_generation_pending','pending_review') "
            "AND COALESCE(r.held_at,'')='' AND r.blocked_by_control_version IS NULL AND ("
            "(j.status='running' AND j.lease_expires_at < ?) OR "
            "(j.status='failed_recoverable' AND j.retry_at IS NOT NULL AND j.retry_at <= ?) OR "
            "j.status='queued') "
            "ORDER BY CASE WHEN j.status='running' AND j.lease_expires_at < ? THEN 0 "
            "WHEN j.status='failed_recoverable' AND j.retry_at <= ? THEN 1 ELSE 2 END, "
            "CASE WHEN j.status='running' THEN j.lease_expires_at "
            "WHEN j.status='failed_recoverable' THEN j.retry_at ELSE j.requested_at END, "
            "j.attempts, j.id LIMIT 1",
            (now, now, now, now),
        ).fetchone()
        return None if row is None else int(row["id"])

    async def generate_codex_job_exact(self, generation_job_id: int) -> GenerationResult | None:
        """Generate exactly one pre-bound Codex job; never substitute another job."""
        now = _utc(self.clock.now())
        lease_token = token_hex(32)
        with self.storage.transaction() as connection:
            control = connection.execute(
                "SELECT paused_at FROM generation_provider_controls WHERE provider_name='codex_cli'"
            ).fetchone()
            if control is None or control["paused_at"] is not None:
                return None
            job = connection.execute(
                "SELECT j.id, j.selection_id, j.job_kind, j.requested_page_count, j.status, j.lease_expires_at, j.retry_at, s.candidate_id, ce.run_id "
                "FROM generation_jobs j JOIN generation_job_provider_bindings b "
                "ON b.generation_job_id=j.id AND b.provider_name='codex_cli' "
                "JOIN selections s ON s.id=j.selection_id JOIN candidates c ON c.id=s.candidate_id "
                "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
                "LEFT JOIN generation_job_retry_state r ON r.generation_job_id=j.id "
                "WHERE j.id=? AND c.status IN ('selected_generation_pending','pending_review') "
                "AND COALESCE(r.held_at,'')='' "
                "AND r.blocked_by_control_version IS NULL",
                (generation_job_id,),
            ).fetchone()
            if job is None:
                return None
            selected = self._selected_codex_job_id(connection, now.isoformat())
            if selected != generation_job_id:
                return None
            source_ids = _candidate_source_ids(connection, int(job["candidate_id"]))
            bound = tuple(
                int(row["source_post_version_id"])
                for row in connection.execute(
                    "SELECT source_post_version_id FROM generation_sources "
                    "WHERE generation_job_id=? AND generation_id IS NULL ORDER BY source_post_version_id",
                    (generation_job_id,),
                )
            )
            if not bound or bound != source_ids:
                return None
            due = (
                (job["status"] == "running" and job["lease_expires_at"] < now.isoformat())
                or (
                    job["status"] == "failed_recoverable"
                    and job["retry_at"] is not None
                    and job["retry_at"] <= now.isoformat()
                )
                or job["status"] == "queued"
            )
            if not due:
                return None
            flexible_page_count = str(job["job_kind"]) == "initial"
            if flexible_page_count:
                page_count = 8
                stored_page_count = None
            elif job["requested_page_count"] is None:
                marks = ",".join("?" for _ in source_ids)
                source_rows = connection.execute(
                    f"SELECT body FROM source_post_versions WHERE id IN ({marks}) ORDER BY id",
                    source_ids,
                )
                page_count = _job_page_count(
                    str(job["job_kind"]),
                    adaptive_page_count(str(row["body"]) for row in source_rows),
                )
                stored_page_count = page_count
            else:
                page_count = int(job["requested_page_count"])
                stored_page_count = page_count
            if not 1 <= page_count <= 8:
                return None
            if job["status"] == "running":
                connection.execute(
                    "UPDATE generation_provider_attempts SET finished_at=?, terminal_outcome='abandoned', "
                    "error_message='LeaseExpired: generation failed' WHERE generation_job_id=? AND terminal_outcome IS NULL",
                    (now.isoformat(), generation_job_id),
                )
            lease_until = now + timedelta(minutes=5)
            leased = connection.execute(
                "UPDATE generation_jobs SET status='running', attempts=attempts+1, lease_token=?, "
                "lease_expires_at=?, started_at=?, retry_at=NULL, error_message=NULL, "
                "requested_page_count=? WHERE id=? "
                "AND ((status='running' AND lease_expires_at < ?) OR "
                "(status='failed_recoverable' AND retry_at <= ?) OR status='queued')",
                (
                    lease_token,
                    lease_until.isoformat(),
                    now.isoformat(),
                    stored_page_count,
                    generation_job_id,
                    now.isoformat(),
                    now.isoformat(),
                ),
            )
            if leased.rowcount != 1:
                return None
            attempt = int(
                connection.execute("SELECT attempts FROM generation_jobs WHERE id=?", (generation_job_id,)).fetchone()[
                    0
                ]
            )
            connection.execute(
                "INSERT INTO generation_provider_attempts(generation_job_id, attempt, started_at) VALUES (?, ?, ?)",
                (generation_job_id, attempt, now.isoformat()),
            )
            provider_attempt_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.execute(
                "INSERT INTO pipeline_events(run_id, selection_id, generation_job_id, candidate_id, provider_attempt_id, event_kind, created_at) "
                "VALUES (?, ?, ?, ?, ?, 'provider_call', ?)",
                (
                    int(job["run_id"]),
                    int(job["selection_id"]),
                    generation_job_id,
                    int(job["candidate_id"]),
                    provider_attempt_id,
                    now.isoformat(),
                ),
            )
        facts = self._facts(source_ids)
        try:
            from .ai.codex_cli import CodexCliProvider

            draft = await CodexCliProvider().generate(
                GenerationRequest(
                    int(job["candidate_id"]),
                    source_ids,
                    page_count,
                    facts,
                    flexible_page_count=flexible_page_count,
                )
            )
            validate_copy(
                draft,
                allowed_claim_sources={fact.id: fact.source_version_id for fact in facts},
                expected_page_count=None if flexible_page_count else page_count,
            )
            payload = _draft_payload(draft)
            payload["claim_manifest"] = [_fact_payload(fact) for fact in facts]
        except Exception as exc:
            await self._settle_codex_failure(generation_job_id, lease_token, provider_attempt_id, exc)
            raise
        with self.storage.transaction() as connection:
            active = connection.execute(
                "SELECT 1 FROM generation_jobs WHERE id=? AND status='running' AND lease_token=?",
                (generation_job_id, lease_token),
            ).fetchone()
            if active is None:
                return None
            connection.execute(
                "INSERT INTO generations(generation_job_id, attempt, status, content_json, created_at) VALUES (?, ?, 'current', ?, ?)",
                (generation_job_id, attempt, json.dumps(payload, ensure_ascii=False, sort_keys=True), now.isoformat()),
            )
            generation_id = int(connection.execute("SELECT last_insert_rowid()").fetchone()[0])
            connection.executemany(
                "INSERT INTO generation_sources(generation_job_id, generation_id, source_post_version_id) VALUES (?, ?, ?)",
                ((generation_job_id, generation_id, source_id) for source_id in source_ids),
            )
            finished = _utc(self.clock.now()).isoformat()
            connection.execute(
                "UPDATE generation_provider_attempts SET finished_at=?, terminal_outcome='succeeded' WHERE id=? AND terminal_outcome IS NULL",
                (finished, provider_attempt_id),
            )
            connection.execute(
                "UPDATE generation_jobs SET status='succeeded', finished_at=?, lease_token=NULL, lease_expires_at=NULL WHERE id=? AND lease_token=?",
                (finished, generation_job_id, lease_token),
            )
            retry = connection.execute(
                "SELECT consecutive_failures, retry_version, held_at FROM generation_job_retry_state WHERE generation_job_id=?",
                (generation_job_id,),
            ).fetchone()
            if retry is not None and (int(retry["consecutive_failures"]) or retry["held_at"] is not None):
                connection.execute(
                    "UPDATE generation_job_retry_state SET consecutive_failures=0, held_at=NULL, hold_reason_code=NULL, retry_version=retry_version+1, updated_at=? WHERE generation_job_id=?",
                    (finished, generation_job_id),
                )
                connection.execute(
                    "INSERT INTO generation_job_retry_events(generation_job_id, action, reason_code, actor_kind, resulting_held, resulting_consecutive_failures, previous_retry_version, resulting_retry_version) "
                    "VALUES (?, 'release', 'recovery_succeeded', 'system', 0, 0, ?, ?)",
                    (generation_job_id, int(retry["retry_version"]), int(retry["retry_version"]) + 1),
                )
            connection.execute(
                "UPDATE candidates SET status='pending_review' WHERE id=? AND status='selected_generation_pending'",
                (int(job["candidate_id"]),),
            )
        return GenerationResult(int(job["candidate_id"]), generation_id, draft, source_ids, False)

    async def _settle_codex_failure(
        self,
        generation_job_id: int,
        lease_token: str,
        provider_attempt_id: int,
        exc: Exception,
    ) -> None:
        safe_code = _codex_safe_code(exc)
        pause_codes = frozenset(
            {
                "codex_auth_unavailable",
                "codex_runner_config",
                "codex_supervisor",
                "codex_unknown_exit",
                "codex_outer_timeout",
                "codex_runner_attestation",
            }
        )
        deterministic_codes = frozenset({"codex_input_limit", "codex_output_limit", "codex_invalid_draft"})
        now = _utc(self.clock.now())
        now_text = now.isoformat()
        with self.storage.transaction() as connection:
            finalized = connection.execute(
                "UPDATE generation_provider_attempts SET finished_at=?, terminal_outcome='failed', "
                "error_message=? WHERE id=? AND terminal_outcome IS NULL AND EXISTS ("
                "SELECT 1 FROM generation_jobs WHERE id=? AND status='running' AND lease_token=?)",
                (
                    now_text,
                    _codex_error_message(safe_code),
                    provider_attempt_id,
                    generation_job_id,
                    lease_token,
                ),
            )
            if finalized.rowcount != 1:
                raise RuntimeError("Codex generation lease was lost during settlement")
            connection.execute(
                "INSERT INTO generation_provider_attempt_classifications("
                "provider_attempt_id, provider_name, safe_code"
                ") VALUES (?, 'codex_cli', ?)",
                (provider_attempt_id, safe_code),
            )
            state = connection.execute(
                "SELECT * FROM generation_job_retry_state WHERE generation_job_id=?",
                (generation_job_id,),
            ).fetchone()
            if state is None:
                connection.execute(
                    "INSERT INTO generation_job_retry_state(generation_job_id) VALUES (?)",
                    (generation_job_id,),
                )
                state = connection.execute(
                    "SELECT * FROM generation_job_retry_state WHERE generation_job_id=?",
                    (generation_job_id,),
                ).fetchone()
            assert state is not None
            failures = int(state["consecutive_failures"]) + 1
            previous_retry_version = int(state["retry_version"])
            retry_version = previous_retry_version + 1
            attempt_row = connection.execute(
                "SELECT attempt FROM generation_provider_attempts WHERE id=?",
                (provider_attempt_id,),
            ).fetchone()
            assert attempt_row is not None
            jitter = (
                int.from_bytes(
                    sha256(f"{generation_job_id}:{int(attempt_row['attempt'])}".encode()).digest()[:2],
                    "big",
                )
                % 31
            )
            if safe_code in pause_codes:
                control = connection.execute(
                    "SELECT control_version FROM generation_provider_controls "
                    "WHERE provider_name='codex_cli' AND paused_at IS NULL"
                ).fetchone()
                if control is None:
                    raise RuntimeError("Codex provider control changed during settlement")
                control_version = int(control["control_version"]) + 1
                updated = connection.execute(
                    "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code=?, "
                    "resumed_at=NULL, control_version=?, updated_at=? "
                    "WHERE provider_name='codex_cli' AND paused_at IS NULL",
                    (now_text, safe_code, control_version, now_text),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("Codex provider control changed during settlement")
                connection.execute(
                    "UPDATE generation_job_retry_state SET consecutive_failures=?, "
                    "blocked_by_control_version=?, blocked_by_safe_code=?, retry_version=?, updated_at=? "
                    "WHERE generation_job_id=?",
                    (
                        failures,
                        control_version,
                        safe_code,
                        retry_version,
                        now_text,
                        generation_job_id,
                    ),
                )
                connection.execute(
                    "INSERT INTO generation_provider_control_events("
                    "operation_id, provider_name, action, reason_code, actor_kind, resulting_paused, "
                    "previous_control_version, resulting_control_version, control_version"
                    ") VALUES (?, 'codex_cli', 'pause', ?, 'system', 1, ?, ?, ?)",
                    (
                        "cxo_" + token_hex(16),
                        safe_code,
                        control_version - 1,
                        control_version,
                        control_version,
                    ),
                )
                retry_at: str | None = None
            else:
                held = (
                    (safe_code == "codex_busy" and failures >= 10)
                    or (safe_code in {"codex_timeout", "codex_nonzero"} and failures >= 5)
                    or (safe_code in deterministic_codes and failures >= 2)
                )
                if held:
                    connection.execute(
                        "UPDATE generation_job_retry_state SET consecutive_failures=?, held_at=?, "
                        "hold_reason_code=?, retry_version=?, updated_at=? WHERE generation_job_id=?",
                        (failures, now_text, safe_code, retry_version, now_text, generation_job_id),
                    )
                    connection.execute(
                        "INSERT INTO generation_job_retry_events("
                        "generation_job_id, action, reason_code, actor_kind, resulting_held, "
                        "resulting_consecutive_failures, previous_retry_version, resulting_retry_version"
                        ") VALUES (?, 'hold', ?, 'system', 1, ?, ?, ?)",
                        (
                            generation_job_id,
                            safe_code,
                            failures,
                            previous_retry_version,
                            retry_version,
                        ),
                    )
                    retry_at = None
                else:
                    if safe_code == "codex_busy":
                        delay = min(900, 30 * (2 ** (failures - 1))) + jitter
                    elif safe_code in deterministic_codes:
                        delay = 300 if failures == 1 else 1800
                    else:
                        delay = min(3600, 60 * (2 ** (failures - 1))) + jitter
                    retry_at = (now + timedelta(seconds=delay)).isoformat()
                    connection.execute(
                        "UPDATE generation_job_retry_state SET consecutive_failures=?, "
                        "retry_version=?, updated_at=? WHERE generation_job_id=?",
                        (failures, retry_version, now_text, generation_job_id),
                    )
            settled = connection.execute(
                "UPDATE generation_jobs SET status='failed_recoverable', finished_at=?, retry_at=?, "
                "lease_token=NULL, lease_expires_at=NULL, error_message=? "
                "WHERE id=? AND status='running' AND lease_token=?",
                (
                    now_text,
                    retry_at,
                    _codex_error_message(safe_code),
                    generation_job_id,
                    lease_token,
                ),
            )
            if settled.rowcount != 1:
                raise RuntimeError("Codex generation lease was lost during settlement")

    def _facts(self, source_ids: tuple[int, ...]) -> tuple[FactClaim, ...]:
        marks = ",".join("?" for _ in source_ids)
        rows = self.storage.fetch_all(
            f"SELECT version.id, version.version_key, version.body, version.media_json, version.kind, version.sponsored, "
            f"version.urls_json, version.conflicts_json, post.channel_id, post.external_post_id, post.source_url, "
            f"COALESCE(observation.observation_key, version.version_key) AS observation_key, "
            f"COALESCE(observation.observed_at, version.observed_at) AS captured_at, "
            f"COALESCE(observation.engagement_json, '{{}}') AS engagement_json "
            f"FROM source_post_versions version JOIN source_posts post ON post.id=version.source_post_id "
            f"LEFT JOIN source_post_observations observation ON observation.id=("
            f"SELECT current.id FROM source_post_observations current "
            f"WHERE current.source_post_version_id=version.id ORDER BY current.observed_at DESC, current.id DESC LIMIT 1) "
            f"WHERE version.id IN ({marks}) ORDER BY version.id",
            source_ids,
        )
        facts: list[FactClaim] = []
        for row in rows:
            conflicts = tuple(str(item) for item in json.loads(str(row["conflicts_json"])))
            uncertainty = ("source conflicts require corroboration",) if conflicts else ()
            source = {
                "channel_id": str(row["channel_id"]),
                "external_post_id": str(row["external_post_id"]),
                "source_url": row["source_url"],
                "version_key": str(row["version_key"]),
                "body": str(row["body"]),
                "media": json.loads(str(row["media_json"])),
                "kind": str(row["kind"]),
                "sponsored": bool(row["sponsored"]),
                "urls": json.loads(str(row["urls_json"])),
                "conflicts": list(conflicts),
                "observation_key": str(row["observation_key"]),
                "captured_at": str(row["captured_at"]),
                "engagement": json.loads(str(row["engagement_json"])),
                "uncertainty": list(uncertainty),
            }
            source_key = source_identity(source)
            material_key = source_material_identity(source)
            observation_key = source_observation_identity(source)
            evidence = str(row["body"])
            evidence_spans = ((0, len(evidence)),)
            claim_id = source_claim_identity(source)
            facts.append(
                FactClaim(
                    claim_id,
                    int(row["id"]),
                    source_key,
                    material_key,
                    observation_key,
                    source["captured_at"],
                    None if source["source_url"] is None else str(source["source_url"]),
                    evidence,
                    evidence_spans,
                    conflicts,
                    uncertainty,
                )
            )
        if len(facts) != len(source_ids):
            raise ValueError("generation source binding is incomplete")
        return tuple(facts)


def _candidate_source_ids(connection: Any, candidate_id: int) -> tuple[int, ...]:
    rows = tuple(
        int(row["source_post_version_id"])
        for row in connection.execute(
            "SELECT source_post_version_id FROM candidate_sources WHERE candidate_id=? ORDER BY source_post_version_id",
            (candidate_id,),
        )
    )
    if rows:
        return rows
    row = connection.execute(
        "SELECT ce.source_post_version_id FROM candidates c "
        "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
        (candidate_id,),
    ).fetchone()
    return () if row is None else (int(row["source_post_version_id"]),)


def _fixture_key(observations: Sequence[SourceObservation], config: object) -> str:
    payload = b"".join(_source_payload(item) for item in observations) + str(getattr(config, "digest", "")).encode()
    return "fixture-" + sha256(payload).hexdigest()


def _source_payload(item: SourceObservation) -> bytes:
    return json.dumps(
        {
            "channel_id": item.channel_id,
            "channel_handle": item.channel_handle,
            "external_post_id": item.external_post_id,
            "published_at": item.published_at.isoformat(),
            "edited_at": item.edited_at.isoformat() if item.edited_at else None,
            "observed_at": item.observed_at.isoformat() if item.observed_at else None,
            "text": item.text,
            "kind": item.kind,
            "sponsored": item.sponsored,
            "urls": [
                {"url": url.url, "source": url.source, "title": url.title, "description": url.description}
                for url in item.urls
            ],
            "media": [
                {
                    "kind": media.kind,
                    "caption": media.caption,
                    "identity": media.identity,
                    "is_service": media.is_service,
                }
                for media in item.media
            ],
            "engagement": {
                "views": item.engagement.views,
                "reactions": item.engagement.reactions,
                "forwards": item.engagement.forwards,
            },
            "conflicts": list(item.conflicts),
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _job_page_count(job_kind: str, fallback: int) -> int:
    if not job_kind.startswith("page:"):
        return fallback
    try:
        page_count = int(job_kind.removeprefix("page:"))
    except ValueError as exc:
        raise ValueError("generation job has an invalid page count") from exc
    if not 1 <= page_count <= 8:
        raise ValueError("generation job page count is outside the supported range")
    return page_count


def _redacted_error(exc: BaseException) -> str:
    """Persist a safe failure class, never provider response or credentials."""
    return f"{type(exc).__name__}: generation failed"


def _codex_safe_code(exc: Exception) -> str:
    names = {
        "CodexAuthUnavailableError": "codex_auth_unavailable",
        "CodexRunnerConfigError": "codex_runner_config",
        "CodexTimeoutError": "codex_timeout",
        "CodexInputLimitError": "codex_input_limit",
        "CodexOutputLimitError": "codex_output_limit",
        "CodexBusyError": "codex_busy",
        "CodexNonzeroError": "codex_nonzero",
        "CodexSupervisorError": "codex_supervisor",
        "CodexUnknownExitError": "codex_unknown_exit",
        "CodexOuterTimeoutError": "codex_outer_timeout",
        "CodexInvalidDraftError": "codex_invalid_draft",
        "CodexRunnerAttestationError": "codex_runner_attestation",
    }
    return names.get(type(exc).__name__, "codex_unknown_exit")


_CODEX_ERROR_NAMES = {
    "codex_auth_unavailable": "CodexAuthUnavailableError",
    "codex_runner_config": "CodexRunnerConfigError",
    "codex_timeout": "CodexTimeoutError",
    "codex_input_limit": "CodexInputLimitError",
    "codex_output_limit": "CodexOutputLimitError",
    "codex_busy": "CodexBusyError",
    "codex_nonzero": "CodexNonzeroError",
    "codex_supervisor": "CodexSupervisorError",
    "codex_unknown_exit": "CodexUnknownExitError",
    "codex_outer_timeout": "CodexOuterTimeoutError",
    "codex_invalid_draft": "CodexInvalidDraftError",
    "codex_runner_attestation": "CodexRunnerAttestationError",
}


def _codex_error_message(safe_code: str) -> str:
    return f"{_CODEX_ERROR_NAMES[safe_code]}: generation failed"


def _safe_rationale(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str, ensure_ascii=False))


def _fact_payload(fact: FactClaim) -> dict[str, Any]:
    return {
        "schema_version": "newsbot-generation-claim-v1",
        "claim_id": fact.id,
        "source_version_id": fact.source_version_id,
        "source_identity": fact.source_identity,
        "material_identity": fact.material_identity,
        "observation_identity": fact.observation_identity,
        "captured_at": fact.captured_at,
        "source_url": fact.source_url,
        "evidence": fact.evidence,
        "evidence_spans": [list(span) for span in fact.evidence_spans],
        "conflicts": list(fact.conflicts),
        "uncertainty": list(fact.uncertainty),
    }


def _draft_payload(draft: CopyDraft) -> dict[str, Any]:
    return {
        "cover": {
            "title": draft.cover.title,
            "subtitle": draft.cover.subtitle,
            "factual_units": _units(draft.cover.factual_units),
        },
        "bodies": [
            {"subtitle": body.subtitle, "body": body.body, "factual_units": _units(body.factual_units)}
            for body in draft.bodies
        ],
        "caption": {
            "hook": draft.caption.hook,
            "context": draft.caption.context,
            "details": draft.caption.details,
            "implications": draft.caption.implications,
            "questions": draft.caption.questions,
            "hashtags": list(draft.caption.hashtags),
        },
        "category": draft.category,
        "draft": draft.draft,
        "source_reported": draft.source_reported,
    }


def _units(units: Any) -> list[dict[str, Any]]:
    return [
        {
            "text": unit.text,
            "references": [
                {"claim_id": ref.claim_id, "source_version_id": ref.source_version_id} for ref in unit.references
            ],
        }
        for unit in units
    ]


def _draft_from_payload(value: dict[str, Any]) -> CopyDraft:
    from .copywriting import BodyPage, Caption, CoverPage, FactReference, FactualUnit

    def make_units(units: list[dict[str, Any]]) -> tuple[FactualUnit, ...]:
        return tuple(
            FactualUnit(
                unit["text"],
                tuple(FactReference(ref["claim_id"], ref["source_version_id"]) for ref in unit["references"]),
            )
            for unit in units
        )

    if value.get("draft") is not True or value.get("source_reported") is not True:
        raise ValueError("draft/source_reported markers are required")
    return CopyDraft(
        CoverPage(value["cover"]["title"], value["cover"]["subtitle"], make_units(value["cover"]["factual_units"])),
        tuple(BodyPage(body["subtitle"], body["body"], make_units(body["factual_units"])) for body in value["bodies"]),
        Caption(**value["caption"]),
        category=value["category"],
        draft=value["draft"],
        source_reported=value["source_reported"],
    )

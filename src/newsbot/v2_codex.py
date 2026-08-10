"""Durable V2 binding between approved candidates and the fixed Codex provider."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Protocol

from .ai.base import FactClaim, GenerationRequest, ProviderError
from .ai.codex_cli import (
    CodexAuthUnavailableError,
    CodexBusyError,
    CodexCliProvider,
    CodexInputLimitError,
    CodexInvalidDraftError,
    CodexNonzeroError,
    CodexOuterTimeoutError,
    CodexOutputLimitError,
    CodexRunnerAttestationError,
    CodexRunnerConfigError,
    CodexSupervisorError,
    CodexTimeoutError,
    CodexUnknownExitError,
    PreparedCodexGeneration,
)
from .approval.telegram import split_telegram_text
from .copywriting import CopyDraft, CopyValidationError, validate_copy
from .v2_workflow import V2Candidate, V2Draft, V2Workflow


class V2CodexErrorCode(StrEnum):
    AUTH_UNAVAILABLE = "auth_unavailable"
    BUSY = "busy"
    TIMEOUT = "timeout"
    OUTER_TIMEOUT = "outer_timeout"
    NONZERO = "nonzero"
    INPUT_LIMIT = "input_limit"
    OUTPUT_LIMIT = "output_limit"
    INVALID_DRAFT = "invalid_draft"
    RUNNER_CONFIG = "runner_config"
    RUNNER_ATTESTATION = "runner_attestation"
    SUPERVISOR = "supervisor"
    UNKNOWN_EXIT = "unknown_exit"
    PROVIDER_ERROR = "provider_error"
    UNEXPECTED = "unexpected"
    CLEAR_PRE_DISPATCH_NETWORK = "clear_pre_dispatch_network"


@dataclass(frozen=True, slots=True)
class V2CodexFailure:
    code: V2CodexErrorCode
    retryable: bool


class CodexClearPreDispatchNetworkError(ProviderError):
    """A provider proved its network transport failed before dispatching the request."""

    def __init__(self) -> None:
        ProviderError.__init__(self, "Codex network transport failed before dispatch")



@dataclass(frozen=True, slots=True)
class V2PreparedGeneration:
    candidate_id: str
    request: GenerationRequest
    prepared: PreparedCodexGeneration

    @property
    def request_bytes(self) -> bytes:
        return self.prepared.payload

    @property
    def request_digest(self) -> str:
        return self.prepared.sha256


class PreparedCodexProvider(Protocol):
    def prepare(self, request: GenerationRequest) -> PreparedCodexGeneration: ...

    async def generate_prepared(self, prepared: PreparedCodexGeneration) -> CopyDraft: ...


def _sha256_integer(value: bytes) -> int:
    """Use every SHA-256 bit; do not truncate V2 fact identifiers."""
    return int.from_bytes(hashlib.sha256(value).digest(), "big")


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build_generation_request(candidate: V2Candidate) -> GenerationRequest:
    """Build the complete deterministic provider request from V2-owned observation evidence."""
    observation = candidate.observation
    observation_identity = f"v2:{candidate.id}"
    evidence = str(observation["text"])
    material = _canonical_json(
        {
            "channel_id": str(observation["channel_id"]),
            "external_post_id": str(observation["external_post_id"]),
            "published_at": str(observation["published_at"]),
            "text": evidence,
            "urls": list(observation.get("urls", [])),
        }
    )
    material_identity = "v2-material:" + hashlib.sha256(material).hexdigest()
    source_identity = "v2-source:" + str(observation["channel_id"])
    source_version_id = _sha256_integer(observation_identity.encode("utf-8"))
    claim_id = (
        "claim_"
        + hashlib.sha256(
            observation_identity.encode("utf-8")
            + b"\0"
            + material_identity.encode("ascii")
            + b"\0"
            + evidence.encode("utf-8")
        ).hexdigest()
    )
    published_at = str(observation["published_at"])
    return GenerationRequest(
        candidate_id=_sha256_integer(candidate.id.encode("utf-8")),
        source_version_ids=(source_version_id,),
        page_count=CopyDraft.adaptive_page_count((evidence,)),
        facts=(
            FactClaim(
                id=claim_id,
                source_version_id=source_version_id,
                source_identity=source_identity,
                material_identity=material_identity,
                observation_identity=observation_identity,
                captured_at=published_at,
                source_url=(str(observation["urls"][0]) if observation.get("urls") else None),
                evidence=evidence,
                evidence_spans=((0, len(evidence)),),
                conflicts=(),
                uncertainty=(),
            ),
        ),
        flexible_page_count=True,
    )


def prepare_generation(candidate: V2Candidate, provider: PreparedCodexProvider | None = None) -> V2PreparedGeneration:
    """Return the only bytes a V2 worker may persist and subsequently launch."""
    request = build_generation_request(candidate)
    prepared = (provider or CodexCliProvider()).prepare(request)
    return V2PreparedGeneration(candidate.id, request, prepared)


def canonical_validated_output(draft: CopyDraft) -> tuple[bytes, str]:
    """Serialize a provider-validated CopyDraft into its durable exact JSON receipt."""
    output = _canonical_json(asdict(draft))
    return output, hashlib.sha256(output).hexdigest()


def exact_review_text(draft_id: str, content: str) -> str:
    """Return the sole exact-draft Telegram approval representation."""
    return f"V2 exact draft {draft_id}\n{content}"


def validate_exact_review_content(content: str) -> None:
    """Reject output that cannot fit one atomic Telegram approval message."""
    if len(split_telegram_text(exact_review_text("0" * 64, content))) != 1:
        raise CopyValidationError("exact V2 draft exceeds one Telegram message")


def classify_provider_error(error: BaseException) -> V2CodexFailure:
    """Return only bounded safe codes; exception text is never persisted."""
    mapping: tuple[tuple[type[BaseException], V2CodexErrorCode, bool], ...] = (
        (
            CodexClearPreDispatchNetworkError,
            V2CodexErrorCode.CLEAR_PRE_DISPATCH_NETWORK,
            True,
        ),
        (CodexBusyError, V2CodexErrorCode.BUSY, False),
        (CodexTimeoutError, V2CodexErrorCode.TIMEOUT, False),
        (CodexOuterTimeoutError, V2CodexErrorCode.OUTER_TIMEOUT, False),
        (CodexNonzeroError, V2CodexErrorCode.NONZERO, False),
        (CodexAuthUnavailableError, V2CodexErrorCode.AUTH_UNAVAILABLE, False),
        (CodexInputLimitError, V2CodexErrorCode.INPUT_LIMIT, False),
        (CodexOutputLimitError, V2CodexErrorCode.OUTPUT_LIMIT, False),
        (CodexInvalidDraftError, V2CodexErrorCode.INVALID_DRAFT, False),
        (CodexRunnerConfigError, V2CodexErrorCode.RUNNER_CONFIG, False),
        (CodexRunnerAttestationError, V2CodexErrorCode.RUNNER_ATTESTATION, False),
        (CodexSupervisorError, V2CodexErrorCode.SUPERVISOR, False),
        (CopyValidationError, V2CodexErrorCode.INVALID_DRAFT, False),
        (CodexUnknownExitError, V2CodexErrorCode.UNKNOWN_EXIT, False),
        (ProviderError, V2CodexErrorCode.PROVIDER_ERROR, False),
    )
    for error_type, code, retryable in mapping:
        if isinstance(error, error_type):
            return V2CodexFailure(code, retryable)
    return V2CodexFailure(V2CodexErrorCode.UNEXPECTED, False)


class V2CodexWorker:
    """Later worker seam: persistence always surrounds the exact fixed-provider launch."""

    def __init__(self, workflow: V2Workflow, provider: PreparedCodexProvider | None = None) -> None:
        self.workflow = workflow
        self.provider = provider or CodexCliProvider()

    async def generate_next(self) -> V2Draft | None:
        if self.workflow.reconcile_interrupted_codex_requests():
            return None
        candidate = self.workflow.next_codex_candidate()
        return None if candidate is None else await self.generate(candidate.id)

    async def generate(self, candidate_id: str) -> V2Draft:
        candidate = self.workflow.get_candidate(candidate_id)
        try:
            prepared = prepare_generation(candidate, self.provider)
        except Exception as error:
            failure = classify_provider_error(error)
            self.workflow.mark_manual_review(
                candidate.id,
                f"Codex request preparation failed: {failure.code.value}",
            )
            raise
        self.workflow.prepare_codex_request(candidate.id, prepared.request_bytes, prepared.request_digest)
        attempt = self.workflow.begin_codex_attempt(candidate.id, prepared.request_digest)
        try:
            draft = await self.provider.generate_prepared(prepared.prepared)
            validate_copy(
                draft,
                allowed_claim_sources={fact.id: fact.source_version_id for fact in prepared.request.facts},
                expected_page_count=None if prepared.request.flexible_page_count else prepared.request.page_count,
            )
            output, digest = canonical_validated_output(draft)
            validate_exact_review_content(output.decode("utf-8"))
        except BaseException as error:
            failure = classify_provider_error(error)
            self.workflow.settle_codex_attempt_failure(attempt.id, failure.code.value, retryable=failure.retryable)
            raise
        return self.workflow.commit_codex_success(attempt.id, output, digest)


__all__ = [
    "V2CodexErrorCode",
    "V2CodexFailure",
    "CodexClearPreDispatchNetworkError",
    "V2CodexWorker",
    "V2PreparedGeneration",
    "build_generation_request",
    "canonical_validated_output",
    "exact_review_text",
    "classify_provider_error",
    "prepare_generation",
    "validate_exact_review_content",
]

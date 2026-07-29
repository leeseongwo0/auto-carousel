"""Approval action and secure callback primitives.

Raw Telegram callback capabilities are transient transport values.  Persistence
receives only their SHA-256 digest and revision-bound metadata.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum


class ApprovalStage(StrEnum):
    SELECTION = "selection"
    REVIEW = "review"


class ApprovalAction(StrEnum):
    MAKE = "make"
    DEFER_6H = "defer_6h"
    DEFER_24H = "defer_24h"
    DEFER_72H = "defer_72h"
    REJECT = "reject"
    REFRESH = "refresh"
    APPROVE_HANDOFF = "approve_handoff"
    REGENERATE = "regenerate"
    PAGE_INCREMENT = "page_increment"
    PAGE_DECREMENT = "page_decrement"


_SELECTION_ACTIONS = frozenset(
    {
        ApprovalAction.MAKE,
        ApprovalAction.DEFER_6H,
        ApprovalAction.DEFER_24H,
        ApprovalAction.DEFER_72H,
        ApprovalAction.REJECT,
        ApprovalAction.REFRESH,
    }
)
_REVIEW_ACTIONS = frozenset(
    {
        ApprovalAction.APPROVE_HANDOFF,
        ApprovalAction.REGENERATE,
        ApprovalAction.PAGE_INCREMENT,
        ApprovalAction.PAGE_DECREMENT,
        ApprovalAction.DEFER_6H,
        ApprovalAction.DEFER_24H,
        ApprovalAction.DEFER_72H,
        ApprovalAction.REJECT,
        ApprovalAction.REFRESH,
    }
)


@dataclass(frozen=True, slots=True)
class CallbackBinding:
    """All mutable objects a callback must match before it can take effect."""

    chat_id: int
    actor_id: int
    candidate_id: int
    candidate_revision: int
    source_version_ids: tuple[int, ...]
    digest_revision: int | None = None
    generation_id: int | None = None
    generation_revision: int | None = None

    def __post_init__(self) -> None:
        if self.chat_id == 0 or self.actor_id == 0 or self.candidate_id <= 0:
            raise ValueError("chat, actor, and candidate identifiers are required")
        if self.candidate_revision <= 0:
            raise ValueError("candidate_revision must be positive")
        if tuple(sorted(set(self.source_version_ids))) != self.source_version_ids:
            raise ValueError("source_version_ids must be sorted and unique")
        if any(value <= 0 for value in self.source_version_ids):
            raise ValueError("source_version_ids must be positive")
        if self.digest_revision is not None and self.digest_revision <= 0:
            raise ValueError("digest_revision must be positive")
        if self.generation_id is not None and self.generation_id <= 0:
            raise ValueError("generation_id must be positive")
        if self.generation_revision is not None and self.generation_revision <= 0:
            raise ValueError("generation_revision must be positive")


@dataclass(frozen=True, slots=True)
class CallbackTokenRecord:
    """Hash-only callback persistence shape; it has no raw token field."""

    token_hash: str
    stage: ApprovalStage
    action: ApprovalAction
    binding: CallbackBinding
    created_at: datetime
    expires_at: datetime
    consumed_at: datetime | None = None
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", self.token_hash):
            raise ValueError("token_hash must be a SHA-256 hexadecimal digest")
        _require_utc(self.created_at, "created_at")
        _require_utc(self.expires_at, "expires_at")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")
        if self.action not in _actions_for_stage(self.stage):
            raise ValueError("action is not valid for callback stage")

    def is_active(self, now: datetime) -> bool:
        _require_utc(now, "now")
        return self.consumed_at is None and self.revoked_at is None and now < self.expires_at

    def persistence_values(self) -> dict[str, object]:
        """Return only serializable hash-bound values safe to persist."""
        return asdict(self)


@dataclass(frozen=True, slots=True)
class IssuedCallback:
    """The raw value is for immediate callback transport only."""

    token: str
    record: CallbackTokenRecord


def issue_callback(
    *,
    stage: ApprovalStage,
    action: ApprovalAction,
    binding: CallbackBinding,
    created_at: datetime,
    expires_at: datetime,
) -> IssuedCallback:
    """Issue an unpadded base64url callback token with at least 128 bits entropy."""
    _require_utc(created_at, "created_at")
    _require_utc(expires_at, "expires_at")
    if expires_at <= created_at:
        raise ValueError("expires_at must be after created_at")
    if action not in _actions_for_stage(stage):
        raise ValueError("action is not valid for callback stage")
    # 32 random bytes gives 256 bits and encodes as 43 unpadded URL-safe bytes.
    token = secrets.token_urlsafe(32)
    if len(token.encode("utf-8")) > 64:  # Defensive invariant for Telegram callbacks.
        raise RuntimeError("callback token exceeds Telegram callback-data limit")
    return IssuedCallback(
        token=token,
        record=CallbackTokenRecord(
            token_hash=hash_callback_token(token),
            stage=stage,
            action=action,
            binding=binding,
            created_at=created_at,
            expires_at=expires_at,
        ),
    )


def hash_callback_token(token: str) -> str:
    """Return the only form of a raw callback token that may be persisted."""
    if not token or not re.fullmatch(r"[A-Za-z0-9_-]+", token):
        raise ValueError("callback token must be unpadded base64url")
    try:
        decoded = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
    except ValueError as exc:
        raise ValueError("callback token must be valid base64url") from exc
    if len(decoded) < 16:
        raise ValueError("callback token must contain at least 128 bits")
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def matches_callback_token(token: str, token_hash: str) -> bool:
    """Constant-time comparison for a presented transient token."""
    return hmac.compare_digest(hash_callback_token(token), token_hash)


def _actions_for_stage(stage: ApprovalStage) -> frozenset[ApprovalAction]:
    return _SELECTION_ACTIONS if stage is ApprovalStage.SELECTION else _REVIEW_ACTIONS


def _require_utc(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must be UTC-aware")

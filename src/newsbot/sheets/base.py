"""Google-free Sheets delivery contracts.

An outcome is deliberately conservative: callers may issue a new mutation only
for ``NOT_APPLIED`` established before or by a trusted definitive rejection.
``AMBIGUOUS`` is permanently probe-only for the operation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol


class SafeCode(StrEnum):
    TEMPLATE_DRIFT = "template_drift"
    METADATA_CONFLICT = "metadata_conflict"
    NOT_FOUND = "not_found"
    INVALID_REQUEST = "invalid_request"
    UNAUTHENTICATED = "unauthenticated"
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMITED = "rate_limited"
    ABORTED = "aborted"
    AMBIGUOUS = "ambiguous"


class DeliveryOutcome(StrEnum):
    APPLIED = "delivered"
    NOT_APPLIED = "not_applied"
    BLOCKED = "blocked"
    AMBIGUOUS = "ambiguous"


class MetadataState(StrEnum):
    ABSENT = "absent"
    EXACT = "exact"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class SheetProbe:
    """A bounded, read-only result. It never authorizes a resend by itself."""

    metadata: MetadataState
    safe_code: SafeCode | None = None
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class SheetDelivery:
    outcome: DeliveryOutcome
    safe_code: SafeCode | None = None
    metadata: MetadataState | None = None
    retry_after_seconds: int | None = None


@dataclass(frozen=True, slots=True)
class DispatchCredentialAttestation:
    """Redacted credential facts required before crossing the durable marker."""

    refreshed_at: str
    expires_at: str
    scope_ok: bool


@dataclass(frozen=True, slots=True)
class PreparedSheetMutation:
    """A fully validated immutable request, ready for the post-marker send."""
    body: Mapping[str, object]
    request_sha256: str
    metadata: MetadataState = MetadataState.ABSENT
    metadata_value: str | None = None


class SheetsAdapter(Protocol):
    """The state service depends only on this interface, not Google libraries."""

    def probe(self, *, metadata_value: str) -> SheetProbe: ...
    def probe_bootstrap(self, *, service_account_email: str) -> SheetProbe: ...

    def prepare_bootstrap(self, *, service_account_email: str) -> PreparedSheetMutation: ...
    def dispatch_prepared_bootstrap(self, prepared: PreparedSheetMutation) -> SheetDelivery: ...
    def arm_prepared_dispatch(self) -> None: ...
    def dispatch_credential_attestation(self) -> DispatchCredentialAttestation: ...
    def prepare_delivery(
        self, *, export_id: str, canonical_sha256: str, values: Sequence[str]
    ) -> PreparedSheetMutation: ...
    def dispatch_prepared(self, prepared: PreparedSheetMutation) -> SheetDelivery: ...



class SheetsService(Protocol):
    """Small structural protocol used by scripted/offline service doubles."""

    def batch_update(self, spreadsheet_id: str, body: Mapping[str, object]) -> object: ...

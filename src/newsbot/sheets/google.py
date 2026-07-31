"""Optional Google Sheets adapter with one-attempt, no-resend delivery semantics."""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from math import ceil
from typing import Any

from .base import (
    DeliveryOutcome,
    DispatchCredentialAttestation,
    MetadataState,
    PreparedSheetMutation,
    SafeCode,
    SheetDelivery,
    SheetProbe,
)
from .schema import (
    DELIVERY_METADATA_KEY,
    MAX_MARKERS,
    build_bootstrap_request,
    build_delivery_request,
    delivery_metadata_value,
    next_value_row,
    validate_workplace,
)

_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
_MAX_ERROR_BYTES = 1_048_576
_WHOLE_CALL_SECONDS = 195
_CONNECT_SECONDS = 10


class GoogleSheetsAdapter:
    """Live adapter. Google dependencies are imported only by ``from_credentials``.

    A constructed instance accepts an injected service for offline/scripted tests.
    It makes precisely one ``batchUpdate`` call per ``deliver`` invocation.
    """

    def __init__(
        self,
        *,
        spreadsheet_id: str,
        service: object,
        mutation_service: object | None = None,
        service_account_email: str | None = None,
        credential_refreshed_at: str | None = None,
        credential_expires_at: str | None = None,
        credential_scope_ok: bool = False,
    ) -> None:
        if not spreadsheet_id:
            raise ValueError("spreadsheet_id is required")
        self._spreadsheet_id = spreadsheet_id
        self._service: Any = service
        self._mutation_service: Any = mutation_service or service
        self._service_account_email = service_account_email
        self._dispatch_armed = False
        self._credential_attestation = (
            DispatchCredentialAttestation(
                credential_refreshed_at,
                credential_expires_at,
                credential_scope_ok,
            )
            if credential_refreshed_at is not None and credential_expires_at is not None
            else None
        )

    @classmethod
    def from_credentials(
        cls,
        *,
        credential_info: Mapping[str, str],
        spreadsheet_id: str,
    ) -> GoogleSheetsAdapter:
        """Construct the optional client lazily from one validated credential snapshot."""
        if not spreadsheet_id:
            raise ValueError("spreadsheet_id is required")
        if credential_info.get("type") != "service_account" or not credential_info.get("client_email"):
            raise ValueError("invalid service-account credential data")
        try:
            import httplib2  # type: ignore[import-untyped]
            from google.auth.transport.requests import Request
            from google.oauth2.service_account import Credentials
            from googleapiclient.discovery import build  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError("Google Sheets support requires the sheets extra") from exc
        try:
            credentials = Credentials.from_service_account_info(  # type: ignore[no-untyped-call]
                dict(credential_info), scopes=[_SHEETS_SCOPE]
            )
            credentials.refresh(Request())
            if _token_lifetime_short(credentials.expiry):
                credentials.refresh(Request())
            if not credentials.token:
                raise RuntimeError("credential refresh returned no access token")
            if _token_lifetime_short(credentials.expiry):
                raise RuntimeError("credential token lifetime is too short")
            read_service = build(
                "sheets",
                "v4",
                credentials=credentials,
                cache_discovery=False,
                static_discovery=True,
            )
            mutation_http = _SingleAttemptHttp(httplib2.Http(timeout=_CONNECT_SECONDS), credentials.token)
            mutation_service = build(
                "sheets",
                "v4",
                http=mutation_http,
                cache_discovery=False,
                static_discovery=True,
            )
            mutation_service._newsbot_one_attempt_http = mutation_http
        except Exception as exc:
            raise RuntimeError("Google Sheets credential initialization failed") from exc
        refreshed_at = datetime.now(UTC)
        expiry = credentials.expiry
        assert expiry is not None
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        return cls(
            spreadsheet_id=spreadsheet_id,
            service=read_service,
            mutation_service=mutation_service,
            service_account_email=credential_info["client_email"],
            credential_refreshed_at=refreshed_at.isoformat(),
            credential_expires_at=expiry.isoformat(),
            credential_scope_ok=bool(credentials.has_scopes([_SHEETS_SCOPE])),
        )

    def prepare_bootstrap(self, *, service_account_email: str) -> PreparedSheetMutation:
        """Fresh-read the exact bootstrap delta before its dispatch marker."""
        document = self._read_document_for_preparation()
        validate_workplace(
            document,
            bootstrap=True,
            service_account_email=service_account_email,
            controls_required=False,
            strict_existing_values=True,
        )
        body = build_bootstrap_request(service_account_email=service_account_email, document=document)
        request_sha256 = hashlib.sha256(
            json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return PreparedSheetMutation(body=body, request_sha256=request_sha256)

    def dispatch_prepared_bootstrap(self, prepared: PreparedSheetMutation) -> SheetDelivery:
        """Post-marker bootstrap send and exact control verification."""
        if not prepared.body.get("requests"):
            return SheetDelivery(
                DeliveryOutcome.APPLIED,
                metadata=MetadataState.EXACT,
            )
        try:
            self._batch_update(prepared.body)
        except Exception as exc:
            return self._settle_exception(exc)
        try:
            document = self._read_document()
            validate_workplace(
                document,
                bootstrap=True,
                service_account_email=self._service_account_email,
                controls_required=True,
                strict_existing_values=True,
            )
        except Exception:
            return SheetDelivery(
                DeliveryOutcome.AMBIGUOUS,
                SafeCode.AMBIGUOUS,
            )
        return SheetDelivery(
            DeliveryOutcome.APPLIED,
            metadata=MetadataState.EXACT,
        )

    def prepare_delivery(
        self, *, export_id: str, canonical_sha256: str, values: Sequence[str]
    ) -> PreparedSheetMutation:
        """Fresh-read and fully validate before the durable dispatch marker."""
        marker = delivery_metadata_value(export_id, canonical_sha256)
        document = self._read_document_for_preparation()
        validate_workplace(
            document,
            bootstrap=True,
            service_account_email=self._service_account_email,
            controls_required=True,
        )
        metadata = self._metadata_state(document, marker)
        if metadata is MetadataState.ABSENT:
            next_value_row(document)
        body = build_delivery_request(export_id=export_id, canonical_sha256=canonical_sha256, values=values)
        request_sha256 = hashlib.sha256(
            json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return PreparedSheetMutation(
            body=body,
            request_sha256=request_sha256,
            metadata=metadata,
            metadata_value=marker,
        )

    def dispatch_prepared(self, prepared: PreparedSheetMutation) -> SheetDelivery:
        """Post-marker send followed only by a read-only identity probe."""
        if prepared.metadata_value is None:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.INVALID_REQUEST)
        try:
            self._batch_update(prepared.body)
        except Exception as exc:
            return self._settle_exception(exc)
        after = self.probe(metadata_value=prepared.metadata_value)
        if after.metadata is MetadataState.EXACT:
            return SheetDelivery(
                DeliveryOutcome.APPLIED,
                metadata=MetadataState.EXACT,
            )
        if after.metadata in {MetadataState.DUPLICATE, MetadataState.CONFLICT}:
            return SheetDelivery(
                DeliveryOutcome.BLOCKED,
                SafeCode.METADATA_CONFLICT,
                after.metadata,
            )
        return SheetDelivery(
            DeliveryOutcome.AMBIGUOUS,
            after.safe_code or SafeCode.AMBIGUOUS,
            after.metadata,
        )

    def dispatch_credential_attestation(self) -> DispatchCredentialAttestation:
        """Return redacted facts required by the durable dispatch CAS."""
        if self._credential_attestation is None:
            raise RuntimeError("dispatch credentials are not attested")
        return self._credential_attestation

    def arm_prepared_dispatch(self) -> None:
        """Start the absolute mutation deadline before the durable marker."""
        if self._dispatch_armed:
            raise RuntimeError("prepared dispatch is already armed")
        self._dispatch_armed = True
        transport = getattr(self._mutation_service, "_newsbot_one_attempt_http", None)
        if transport is not None:
            transport.arm()

    def probe_bootstrap(self, *, service_account_email: str) -> SheetProbe:
        """Read-only proof of exact controls after an ambiguous bootstrap."""
        try:
            document = self._read_document()
            validate_workplace(
                document,
                bootstrap=True,
                service_account_email=service_account_email,
                controls_required=False,
                strict_existing_values=True,
            )
            request = build_bootstrap_request(
                service_account_email=service_account_email,
                document=document,
            )
            metadata = MetadataState.ABSENT if request.get("requests") else MetadataState.EXACT
            return SheetProbe(metadata=metadata)
        except ValueError:
            return SheetProbe(
                metadata=MetadataState.CONFLICT,
                safe_code=SafeCode.TEMPLATE_DRIFT,
            )
        except Exception:
            return SheetProbe(
                metadata=MetadataState.ABSENT,
                safe_code=SafeCode.AMBIGUOUS,
            )

    def probe(self, *, metadata_value: str) -> SheetProbe:
        try:
            document = self._read_document()
            validate_workplace(
                document,
                bootstrap=True,
                service_account_email=self._service_account_email,
                controls_required=True,
            )
            metadata = self._metadata_state(document, metadata_value)
            if metadata is MetadataState.ABSENT:
                next_value_row(document)
            return SheetProbe(metadata=metadata)
        except ValueError:
            return SheetProbe(metadata=MetadataState.CONFLICT, safe_code=SafeCode.TEMPLATE_DRIFT)
        except Exception:
            return SheetProbe(metadata=MetadataState.ABSENT, safe_code=SafeCode.AMBIGUOUS)

    def _read_document(self) -> Mapping[str, Any]:
        direct = getattr(self._service, "get_document", None)
        if callable(direct):
            result = direct(self._spreadsheet_id)
        else:
            request = self._service.spreadsheets().get(
                spreadsheetId=self._spreadsheet_id,
                includeGridData=True,
                fields="sheets(properties,data(startRow,startColumn,rowData(values(userEnteredValue,note,dataValidation))),merges,protectedRanges(range,description,warningOnly,editors(users,groups,domainUsersCanEdit))),developerMetadata(metadataKey,metadataValue,visibility,location)",
            )
            result = request.execute(num_retries=0)
        if not isinstance(result, Mapping):
            raise ValueError("invalid Sheets read response")
        return result

    def _read_document_for_preparation(self) -> Mapping[str, Any]:
        try:
            return self._read_document()
        except Exception as exc:
            raise RuntimeError("Google Sheets preparation read failed") from exc

    def _batch_update(self, body: Mapping[str, object]) -> object:
        if not self._dispatch_armed:
            raise RuntimeError("prepared dispatch is not armed")
        transport = getattr(self._mutation_service, "_newsbot_one_attempt_http", None)
        try:
            direct = getattr(self._mutation_service, "batch_update", None)
            if callable(direct):
                return direct(self._spreadsheet_id, body)
            request = self._mutation_service.spreadsheets().batchUpdate(spreadsheetId=self._spreadsheet_id, body=body)
            return request.execute(num_retries=0)
        finally:
            self._dispatch_armed = False
            if transport is not None:
                transport.disarm()

    @staticmethod
    def _metadata_state(document: Mapping[str, Any], expected: str) -> MetadataState:
        metadata = document.get("developerMetadata", [])
        if not isinstance(metadata, list) or len(metadata) > MAX_MARKERS:
            return MetadataState.CONFLICT
        identity = expected.rsplit(":", 1)[0] + ":"
        matching = [
            item
            for item in metadata
            if isinstance(item, Mapping)
            and item.get("metadataKey") == DELIVERY_METADATA_KEY
            and isinstance(item.get("metadataValue"), str)
            and item["metadataValue"].startswith(identity)
        ]
        exact = [item for item in matching if item.get("metadataValue") == expected and _document_location(item)]
        if len(exact) == 1 and len(matching) == 1:
            return MetadataState.EXACT
        if len(exact) > 1:
            return MetadataState.DUPLICATE
        if matching:
            return MetadataState.CONFLICT
        return MetadataState.ABSENT

    @staticmethod
    def _settle_exception(exc: Exception) -> SheetDelivery:
        status, reasons = _safe_google_error(exc)
        # Only a completely parsed, non-redirected rejection can establish no apply.
        if status == 409 and "ABORTED" in reasons:
            return SheetDelivery(
                DeliveryOutcome.NOT_APPLIED,
                SafeCode.ABORTED,
                retry_after_seconds=1,
            )
        rate_reasons = {"RESOURCE_EXHAUSTED", "rateLimitExceeded", "userRateLimitExceeded", "quotaExceeded"}
        if status == 429 and reasons & rate_reasons:
            retry_after = _safe_retry_after(exc)
            if retry_after is not None:
                return SheetDelivery(
                    DeliveryOutcome.NOT_APPLIED,
                    SafeCode.RATE_LIMITED,
                    retry_after_seconds=retry_after,
                )
        if status == 403 and reasons & rate_reasons:
            return SheetDelivery(
                DeliveryOutcome.NOT_APPLIED,
                SafeCode.RATE_LIMITED,
                retry_after_seconds=60,
            )
        if status == 400:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.INVALID_REQUEST)
        if status == 401:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.UNAUTHENTICATED)
        if status == 403:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.PERMISSION_DENIED)
        if status == 404:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.NOT_FOUND)
        return SheetDelivery(DeliveryOutcome.AMBIGUOUS, SafeCode.AMBIGUOUS)


class _LowLevelSendGuard:
    def __init__(self) -> None:
        self.sent = False

    def claim(self) -> None:
        if self.sent:
            raise RuntimeError("mutation transport attempted an internal replay")
        self.sent = True


class _OneSendConnection:
    def __init__(self, connection: object, guard: _LowLevelSendGuard) -> None:
        self._connection = connection
        self._guard = guard

    def request(self, *args: object, **kwargs: object) -> object:
        self._guard.claim()
        return self._connection.request(*args, **kwargs)  # type: ignore[attr-defined]

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


class _SingleAttemptHttp:
    """Frozen-token mutation transport with one request and an absolute watchdog."""

    def __init__(self, http: object, access_token: str) -> None:
        self._http: Any = http
        self._access_token = access_token
        self._used = False
        self._deadline: float | None = None
        self._expired = False
        self._watchdog: threading.Timer | None = None
        self._low_level_guard = _LowLevelSendGuard()
        connection_request = getattr(self._http, "_conn_request", None)
        if callable(connection_request):

            def guarded_connection_request(connection: object, *args: object, **kwargs: object) -> object:
                return connection_request(
                    _OneSendConnection(connection, self._low_level_guard),
                    *args,
                    **kwargs,
                )

            self._http._conn_request = guarded_connection_request  # type: ignore[attr-defined]

    def arm(self) -> None:
        if self._deadline is not None:
            raise RuntimeError("mutation transport already armed")
        self._deadline = time.monotonic() + _WHOLE_CALL_SECONDS
        self._watchdog = threading.Timer(_WHOLE_CALL_SECONDS, self._expire)
        self._watchdog.daemon = True
        self._watchdog.start()

    def disarm(self) -> None:
        if self._watchdog is not None:
            self._watchdog.cancel()
        self._watchdog = None
        self._deadline = None

    def _expire(self) -> None:
        self._expired = True
        connections = getattr(self._http, "connections", {})
        if isinstance(connections, Mapping):
            for connection in connections.values():
                close = getattr(connection, "close", None)
                if callable(close):
                    close()

    def _remaining(self) -> float:
        if self._deadline is None:
            return 0
        return self._deadline - time.monotonic()

    def request(
        self,
        uri: str,
        method: str = "GET",
        body: object = None,
        headers: Mapping[str, str] | None = None,
        **kwargs: object,
    ) -> tuple[object, bytes]:
        if self._used:
            raise RuntimeError("mutation transport invoked more than once")
        self._used = True
        if self._deadline is None or self._expired or time.monotonic() >= self._deadline:
            raise RuntimeError("mutation whole-call deadline exceeded")
        request_headers = dict(headers or {})
        request_headers["authorization"] = f"Bearer {self._access_token}"
        request_headers.pop("Authorization", None)
        remaining = self._remaining()
        if remaining <= 0:
            raise RuntimeError("mutation whole-call deadline exceeded")
        # httplib2 applies this setting to fresh DNS/TCP/TLS connections. It
        # also caps every socket primitive below the current whole-call budget.
        self._http.timeout = min(_CONNECT_SECONDS, remaining)
        response, content = self._http.request(
            uri, method=method, body=body, headers=request_headers, redirections=0, **kwargs
        )
        if self._expired or time.monotonic() >= self._deadline:
            raise RuntimeError("mutation whole-call deadline exceeded")
        status = getattr(response, "status", None)
        if isinstance(status, int) and 300 <= status < 400:
            raise RuntimeError("redirect response is ambiguous")
        if not isinstance(content, bytes) or len(content) > _MAX_ERROR_BYTES:
            raise RuntimeError("oversized or malformed response is ambiguous")
        return response, content


def _token_lifetime_short(expiry: datetime | None) -> bool:
    if expiry is None:
        return True
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=UTC)
    return expiry <= datetime.now(UTC) + timedelta(seconds=255)


def _document_location(item: Mapping[str, Any]) -> bool:
    location = item.get("location")
    return (
        item.get("visibility") == "DOCUMENT"
        and isinstance(location, Mapping)
        and location.get("spreadsheet") is True
        and location.get("locationType", "SPREADSHEET") == "SPREADSHEET"
    )


def _safe_google_error(exc: Exception) -> tuple[int | None, set[str]]:
    """Parse bounded structured error facts, never exposing raw provider content."""
    response = getattr(exc, "resp", None)
    status = getattr(response, "status", None)
    if not isinstance(status, int) or status < 400 or 300 <= status < 400:
        return None, set()
    content = getattr(exc, "content", None)
    if not isinstance(content, (bytes, bytearray)) or len(content) > _MAX_ERROR_BYTES:
        return None, set()
    try:
        decoded = json.loads(bytes(content).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, set()
    if not isinstance(decoded, Mapping) or not isinstance(decoded.get("error"), Mapping):
        return None, set()
    error = decoded["error"]
    if error.get("code") != status:
        return None, set()
    reasons = {
        str(entry.get("reason"))
        for entry in error.get("errors", [])
        if isinstance(entry, Mapping) and isinstance(entry.get("reason"), str)
    }
    reason = error.get("status")
    if isinstance(reason, str):
        reasons.add(reason)
    return (status, reasons) if reasons else (None, set())


def _safe_retry_after(exc: Exception) -> int | None:
    """Return a bounded delay without retaining or exposing provider headers."""
    response = getattr(exc, "resp", None)
    getter = getattr(response, "get", None)
    if not callable(getter):
        return None
    raw = getter("retry-after") or getter("Retry-After")
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if len(value) > 128:
        return None
    if value.isdecimal():
        if len(value) > 5:
            return None
        seconds = int(value)
        return seconds if 1 <= seconds <= 86_400 else None
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    seconds = ceil((parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds())
    return seconds if 1 <= seconds <= 86_400 else None

"""Offline contract tests for the one-attempt Google Sheets adapter."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot.sheets.base import DeliveryOutcome, MetadataState, SafeCode, SheetDelivery
from newsbot.sheets.google import (
    _CONNECT_SECONDS,
    _WHOLE_CALL_SECONDS,
    GoogleSheetsAdapter,
    _safe_google_error,
    _safe_retry_after,
    _SingleAttemptHttp,
    _token_lifetime_short,
)
from newsbot.sheets.schema import (
    _CELLS,
    _MERGES,
    MAX_VALUE_ROW,
    build_bootstrap_request,
    build_delivery_request,
    delivery_metadata_value,
    next_value_row,
    project_handoff,
)


class TestGoogleSheetsAdapter(GoogleSheetsAdapter):
    """Test-only convenience facade; production exposes prepare/arm/dispatch only."""

    __test__ = False

    def bootstrap(self, *, service_account_email: str) -> SheetDelivery:
        try:
            prepared = self.prepare_bootstrap(service_account_email=service_account_email)
        except ValueError:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.TEMPLATE_DRIFT)
        except Exception:
            return SheetDelivery(DeliveryOutcome.AMBIGUOUS, SafeCode.AMBIGUOUS)
        if not prepared.body.get("requests"):
            return SheetDelivery(DeliveryOutcome.APPLIED, metadata=MetadataState.EXACT)
        self.arm_prepared_dispatch()
        return self.dispatch_prepared_bootstrap(prepared)

    def deliver(
        self,
        *,
        export_id: str,
        canonical_sha256: str,
        values: tuple[str, ...],
    ) -> SheetDelivery:
        try:
            prepared = self.prepare_delivery(
                export_id=export_id,
                canonical_sha256=canonical_sha256,
                values=values,
            )
        except ValueError:
            return SheetDelivery(DeliveryOutcome.BLOCKED, SafeCode.TEMPLATE_DRIFT)
        except Exception:
            return SheetDelivery(DeliveryOutcome.AMBIGUOUS, SafeCode.AMBIGUOUS)
        if prepared.metadata is MetadataState.EXACT:
            return SheetDelivery(DeliveryOutcome.APPLIED, metadata=MetadataState.EXACT)
        if prepared.metadata in {MetadataState.DUPLICATE, MetadataState.CONFLICT}:
            return SheetDelivery(
                DeliveryOutcome.BLOCKED,
                SafeCode.METADATA_CONFLICT,
                prepared.metadata,
            )
        self.arm_prepared_dispatch()
        return self.dispatch_prepared(prepared)


class FakeService:
    def __init__(self, *, with_controls: bool = True) -> None:
        rows = []
        for oracle_row in _CELLS + ((None, None, None, "AI", "O") + (None,) * 17,):
            rows.append(
                {
                    "values": [
                        {} if value is None else {"userEnteredValue": {"stringValue": value}} for value in oracle_row
                    ]
                }
            )
        self.document = {
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "workplace",
                        "gridProperties": {"rowCount": 1000, "columnCount": 28},
                    },
                    "merges": [
                        {
                            "startRowIndex": a,
                            "endRowIndex": b,
                            "startColumnIndex": c,
                            "endColumnIndex": d,
                        }
                        for a, b, c, d in _MERGES
                    ],
                    "data": [{"rowData": rows}],
                }
            ],
            "developerMetadata": [],
        }
        self.calls: list[dict[str, object]] = []
        self.apply = True
        self.failure: Exception | None = None
        if with_controls:
            adapter = TestGoogleSheetsAdapter(
                spreadsheet_id="sheet",
                service=self,
                service_account_email="bot@example.invalid",
            )
            assert adapter.bootstrap(service_account_email="bot@example.invalid").outcome is DeliveryOutcome.APPLIED
            self.calls.clear()

    def get_document(self, spreadsheet_id: str) -> dict[str, object]:
        assert spreadsheet_id == "sheet"
        return deepcopy(self.document)

    def batch_update(self, spreadsheet_id: str, body: object) -> object:
        assert spreadsheet_id == "sheet"
        self.calls.append(deepcopy(body))
        if self.failure:
            raise self.failure
        if self.apply:
            requests = body["requests"]
            metadata = requests[0]["createDeveloperMetadata"]["developerMetadata"]
            self.document["developerMetadata"].append(metadata)
            if len(requests) == 4:
                sheet = self.document["sheets"][0]
                rows = sheet["data"][0]["rowData"]
                while len(rows) < 1000:
                    rows.append({"values": []})
                category_rule = requests[1]["setDataValidation"]["rule"]
                upload_rule = requests[2]["setDataValidation"]["rule"]
                for row in rows[3:1000]:
                    while len(row["values"]) < 5:
                        row["values"].append({})
                    row["values"][3]["dataValidation"] = deepcopy(category_rule)
                    row["values"][4]["dataValidation"] = deepcopy(upload_rule)
                sheet["protectedRanges"] = [requests[3]["addProtectedRange"]["protectedRange"]]
            elif len(requests) == 2 and "appendCells" in requests[1]:
                appended = deepcopy(requests[1]["appendCells"]["rows"][0])
                rows = self.document["sheets"][0]["data"][0]["rowData"]
                row_index = next_value_row(self.document) - 1
                target = rows[row_index]
                while len(target["values"]) < len(appended["values"]):
                    target["values"].append({})
                for index, cell in enumerate(appended["values"]):
                    target["values"][index]["userEnteredValue"] = cell["userEnteredValue"]
        return {"replies": [{}, {}]}


def test_checked_in_workplace_fixture_matches_frozen_oracle() -> None:
    fixture_path = Path(__file__).parents[1] / "fixtures" / "workplace_template.json"
    fixture = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert tuple(tuple(row) for row in fixture["cells"]) == _CELLS
    assert tuple(tuple(item) for item in fixture["merges"]) == _MERGES
    assert (
        tuple(fixture["bootstrap_row_4"])
        == (
            None,
            None,
            None,
            "AI",
            "O",
        )
        + (None,) * 17
    )


def _values() -> tuple[str, ...]:
    return project_handoff(
        approved_date="2026-07-30", page_count=1, category="AI", caption="caption", pages=[("title", "subtitle")]
    )


def _adapter(service: FakeService) -> TestGoogleSheetsAdapter:
    return TestGoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=service,
        service_account_email="bot@example.invalid",
    )


def test_unarmed_prepared_dispatch_never_touches_transport() -> None:
    service = FakeService()
    adapter = GoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=service,
        service_account_email="bot@example.invalid",
    )
    prepared = adapter.prepare_delivery(
        export_id="exp_" + "a" * 32,
        canonical_sha256="b" * 64,
        values=_values(),
    )

    result = adapter.dispatch_prepared(prepared)

    assert result.outcome is DeliveryOutcome.AMBIGUOUS
    assert service.calls == []


def test_delivery_is_one_atomic_metadata_then_typed_append() -> None:
    service = FakeService()
    result = _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())
    assert result.outcome is DeliveryOutcome.APPLIED
    assert len(service.calls) == 1
    requests = service.calls[0]["requests"]
    assert [next(iter(request)) for request in requests] == ["createDeveloperMetadata", "appendCells"]
    append = requests[1]["appendCells"]
    assert append["sheetId"] == 0 and append["fields"] == "userEnteredValue"
    assert len(append["rows"]) == 1 and len(append["rows"][0]["values"]) == 22
    assert all(set(cell["userEnteredValue"]) == {"stringValue"} for cell in append["rows"][0]["values"])
    assert append["rows"][0]["values"][0] == {"userEnteredValue": {"stringValue": ""}}
    row_five = service.document["sheets"][0]["data"][0]["rowData"][4]["values"]
    assert tuple(cell.get("userEnteredValue", {}).get("stringValue", "") for cell in row_five[:22]) == _values()
    assert next_value_row(service.document) == 6


def test_literal_blank_row_does_not_advance_append_placement() -> None:
    service = FakeService()
    blank_row = service.document["sheets"][0]["data"][0]["rowData"][4]
    for cell in blank_row["values"][:22]:
        cell["userEnteredValue"] = {"stringValue": ""}

    assert next_value_row(service.document) == 5


def test_template_drift_blocks_before_mutation() -> None:
    service = FakeService()
    service.document["sheets"][0]["data"][0]["rowData"][0]["values"][1] = {"userEnteredValue": {"stringValue": "wrong"}}
    result = _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())
    assert (result.outcome, result.safe_code, service.calls) == (DeliveryOutcome.BLOCKED, SafeCode.TEMPLATE_DRIFT, [])


def test_exact_marker_reuses_and_conflicting_marker_blocks_without_write() -> None:
    service = FakeService()
    marker = delivery_metadata_value("exp_" + "a" * 32, "b" * 64)
    service.document["developerMetadata"].append(
        {
            "metadataKey": "newsbot_workplace_handoff_v1",
            "metadataValue": marker,
            "visibility": "DOCUMENT",
            "location": {"spreadsheet": True},
        }
    )
    assert (
        _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values()).outcome
        is DeliveryOutcome.APPLIED
    )
    assert service.calls == []
    service.document["developerMetadata"][-1]["metadataValue"] = "v1:exp_" + "a" * 32 + ":" + "c" * 64
    result = _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())
    assert (result.outcome, result.safe_code, service.calls) == (
        DeliveryOutcome.BLOCKED,
        SafeCode.METADATA_CONFLICT,
        [],
    )


def test_human_owned_cells_do_not_affect_marker_reuse() -> None:
    service = FakeService()
    adapter = _adapter(service)
    assert (
        adapter.deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values()).outcome
        is DeliveryOutcome.APPLIED
    )
    service.document["sheets"][0]["data"][0]["rowData"].append(
        {"values": [{"userEnteredValue": {"stringValue": "human edit"}}]}
    )
    assert (
        adapter.deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values()).outcome
        is DeliveryOutcome.APPLIED
    )
    assert len(service.calls) == 1


def test_row_four_drift_blocks_delivery_before_mutation() -> None:
    service = FakeService()
    service.document["sheets"][0]["data"][0]["rowData"][3]["values"] = [
        {"userEnteredValue": {"stringValue": "human-owned"}} for _ in range(22)
    ]

    result = _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())

    assert result.outcome is DeliveryOutcome.BLOCKED
    assert service.calls == []


def test_ambiguous_transport_never_causes_second_attempt() -> None:
    service = FakeService()
    service.failure = OSError("connection lost after send")
    result = _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())
    assert (result.outcome, result.safe_code) == (DeliveryOutcome.AMBIGUOUS, SafeCode.AMBIGUOUS)
    assert len(service.calls) == 1


def test_request_builder_is_independently_bounded() -> None:
    request = build_delivery_request(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())
    assert len(request["requests"]) == 2
    assert _adapter(FakeService()).probe(metadata_value="x").metadata is MetadataState.ABSENT


def test_bootstrap_batch_is_fixed_and_has_no_value_mutation() -> None:
    request = build_bootstrap_request(service_account_email="bot@example.invalid")
    requests = request["requests"]
    assert [next(iter(item)) for item in requests] == [
        "createDeveloperMetadata",
        "setDataValidation",
        "setDataValidation",
        "addProtectedRange",
    ]
    assert all("updateCells" not in item and "appendCells" not in item for item in requests)
    assert requests[1]["setDataValidation"]["range"] == {
        "sheetId": 0,
        "startRowIndex": 3,
        "endRowIndex": 1000,
        "startColumnIndex": 3,
        "endColumnIndex": 4,
    }
    assert requests[3]["addProtectedRange"]["protectedRange"]["editors"] == {
        "users": ["bot@example.invalid"],
        "groups": [],
        "domainUsersCanEdit": False,
    }


def test_bootstrap_rereads_api_shaped_cell_controls() -> None:
    service = FakeService(with_controls=False)
    result = _adapter(service).bootstrap(service_account_email="bot@example.invalid")
    assert result.outcome is DeliveryOutcome.APPLIED
    assert len(service.calls) == 1
    sheet = service.document["sheets"][0]
    assert "dataValidations" not in sheet
    assert sheet["data"][0]["rowData"][3]["values"][3]["dataValidation"]["condition"] == {
        "type": "ONE_OF_LIST",
        "values": [{"userEnteredValue": "AI"}, {"userEnteredValue": "Blockchain"}],
    }


def test_bootstrap_post_send_validation_failure_is_ambiguous() -> None:
    class InvalidAfterMutationService(FakeService):
        def batch_update(self, spreadsheet_id: str, body: object) -> object:
            result = super().batch_update(spreadsheet_id, body)
            self.document["sheets"][0]["data"][0]["rowData"][3]["values"][3]["dataValidation"]["condition"]["type"] = (
                "INVALID"
            )
            return result

    service = InvalidAfterMutationService(with_controls=False)

    result = _adapter(service).bootstrap(service_account_email="bot@example.invalid")

    assert (result.outcome, result.safe_code, result.metadata) == (
        DeliveryOutcome.AMBIGUOUS,
        SafeCode.AMBIGUOUS,
        None,
    )
    assert len(service.calls) == 1


def test_bootstrap_probe_distinguishes_exact_absent_and_conflict() -> None:
    exact = _adapter(FakeService()).probe_bootstrap(service_account_email="bot@example.invalid")
    absent = _adapter(FakeService(with_controls=False)).probe_bootstrap(service_account_email="bot@example.invalid")
    conflict_service = FakeService()
    conflict_service.document["developerMetadata"][0]["metadataValue"] = "wrong"
    conflict = _adapter(conflict_service).probe_bootstrap(service_account_email="bot@example.invalid")

    assert (exact.metadata, exact.safe_code) == (MetadataState.EXACT, None)
    assert (absent.metadata, absent.safe_code) == (MetadataState.ABSENT, None)
    assert (conflict.metadata, conflict.safe_code) == (
        MetadataState.CONFLICT,
        SafeCode.TEMPLATE_DRIFT,
    )


def test_bootstrap_blocks_invalid_existing_upload_status_without_mutation() -> None:
    service = FakeService(with_controls=False)
    service.document["sheets"][0]["data"][0]["rowData"].append(
        {
            "values": [
                {},
                {},
                {},
                {},
                {"userEnteredValue": {"stringValue": "INVALID"}},
            ]
        }
    )

    result = _adapter(service).bootstrap(service_account_email="bot@example.invalid")

    assert (result.outcome, result.safe_code) == (
        DeliveryOutcome.BLOCKED,
        SafeCode.TEMPLATE_DRIFT,
    )
    assert service.calls == []


def test_bootstrap_reuses_google_normalized_controls_without_mutation() -> None:
    service = FakeService(with_controls=False)
    adapter = _adapter(service)
    assert adapter.bootstrap(service_account_email="bot@example.invalid").outcome is DeliveryOutcome.APPLIED
    service.document["developerMetadata"][0]["location"]["locationType"] = "SPREADSHEET"
    protection = service.document["sheets"][0]["protectedRanges"][0]
    protection["range"].pop("sheetId")
    protection.pop("warningOnly")
    protection["editors"].pop("groups")
    protection["editors"].pop("domainUsersCanEdit")
    protection["editors"]["users"].append("owner@example.invalid")

    result = adapter.bootstrap(service_account_email="bot@example.invalid")

    assert result.outcome is DeliveryOutcome.APPLIED
    assert len(service.calls) == 1


def test_naive_google_token_expiry_is_interpreted_as_utc() -> None:
    long_lived_naive = (datetime.now(UTC) + timedelta(minutes=10)).replace(tzinfo=None)
    assert not _token_lifetime_short(long_lived_naive)


def test_google_error_parser_returns_only_allowlisted_structured_facts() -> None:
    secret = "spreadsheet-id token private-key manuscript"
    error = RuntimeError(secret)
    error.resp = SimpleNamespace(status=403)  # type: ignore[attr-defined]
    error.content = json.dumps(  # type: ignore[attr-defined]
        {
            "error": {
                "code": 403,
                "message": secret,
                "status": "PERMISSION_DENIED",
                "errors": [{"reason": "forbidden", "message": secret}],
            }
        }
    ).encode()

    result = _safe_google_error(error)

    assert result == (403, {"PERMISSION_DENIED", "forbidden"})
    assert secret not in repr(result)


def test_google_retry_after_parser_bounds_numeric_delay() -> None:
    class Response(dict[str, str]):
        status = 429

    error = RuntimeError("secret response")
    error.resp = Response({"retry-after": "120"})  # type: ignore[attr-defined]

    assert _safe_retry_after(error) == 120


def test_google_retry_after_rejects_invalid_and_429_fails_closed() -> None:
    class Response(dict[str, str]):
        status = 429

    for value in ("", "0", "86401", "9" * 10_000, "x" * 10_000, "not-a-date"):
        error = RuntimeError("secret response")
        error.resp = Response({"retry-after": value})  # type: ignore[attr-defined]
        assert _safe_retry_after(error) is None

    error = RuntimeError("secret response")
    error.resp = Response()  # type: ignore[attr-defined]
    error.content = json.dumps(  # type: ignore[attr-defined]
        {
            "error": {
                "code": 429,
                "status": "RESOURCE_EXHAUSTED",
                "errors": [{"reason": "rateLimitExceeded"}],
            }
        }
    ).encode()

    result = GoogleSheetsAdapter._settle_exception(error)

    assert result.outcome is DeliveryOutcome.AMBIGUOUS
    assert result.safe_code is SafeCode.AMBIGUOUS
    assert result.retry_after_seconds is None
    error.resp = Response({"retry-after": "120"})  # type: ignore[attr-defined]
    retryable = GoogleSheetsAdapter._settle_exception(error)
    assert retryable.outcome is DeliveryOutcome.NOT_APPLIED
    assert retryable.safe_code is SafeCode.RATE_LIMITED
    assert retryable.retry_after_seconds == 120


def test_non_429_rejection_never_reads_retry_after() -> None:
    class Response:
        status = 403

        def get(self, key):
            raise AssertionError(f"header read: {key}")

    error = RuntimeError("secret response")
    error.resp = Response()  # type: ignore[attr-defined]
    error.content = json.dumps(  # type: ignore[attr-defined]
        {
            "error": {
                "code": 403,
                "status": "PERMISSION_DENIED",
                "errors": [{"reason": "rateLimitExceeded"}],
            }
        }
    ).encode()

    result = GoogleSheetsAdapter._settle_exception(error)

    assert result.outcome is DeliveryOutcome.NOT_APPLIED
    assert result.safe_code is SafeCode.RATE_LIMITED
    assert result.retry_after_seconds == 60


def test_mutation_transport_arms_absolute_watchdog_and_uses_connect_bound() -> None:
    class Http:
        timeout = None

        def request(self, uri, **kwargs):
            self.uri = uri
            self.kwargs = kwargs
            return type("Response", (), {"status": 200})(), b"{}"

    http = Http()
    transport = _SingleAttemptHttp(http, "token")
    transport.arm()
    try:
        assert transport._watchdog is not None
        assert transport._watchdog.interval == _WHOLE_CALL_SECONDS
        transport.request("https://example.invalid", headers={"Authorization": "wrong"})
    finally:
        transport.disarm()
    assert http.timeout <= _CONNECT_SECONDS
    assert http.kwargs["redirections"] == 0
    assert http.kwargs["headers"]["authorization"] == "Bearer token"


def test_mutation_transport_blocks_internal_connection_replay() -> None:
    class Connection:
        sock = object()

        def __init__(self) -> None:
            self.requests = 0

        def request(self, *args, **kwargs) -> None:
            self.requests += 1

        def close(self) -> None:
            pass

        def connect(self) -> None:
            pass

    class RetryingHttp:
        timeout = None
        connections: dict[str, object] = {}

        def __init__(self) -> None:
            self.connection = Connection()

        def _conn_request(self, connection, request_uri, method, body, headers):
            connection.request(method, request_uri, body, headers)
            connection.close()
            connection.connect()
            connection.request(method, request_uri, body, headers)
            return type("Response", (), {"status": 200})(), b"{}"

        def request(self, uri, method="GET", body=None, headers=None, **kwargs):
            return self._conn_request(
                self.connection,
                uri,
                method,
                body,
                headers,
            )

    http = RetryingHttp()
    transport = _SingleAttemptHttp(http, "token")
    transport.arm()
    try:
        with pytest.raises(RuntimeError, match="internal replay"):
            transport.request("https://example.invalid", method="POST", body=b"{}")
    finally:
        transport.disarm()
    assert http.connection.requests == 1


def test_preparation_read_failure_is_redacted() -> None:
    secret = "https://sheets.googleapis.test/private-sheet?token=secret"
    service = FakeService(with_controls=False)

    def fail_read(_spreadsheet_id: str) -> object:
        raise RuntimeError(secret)

    service.get_document = fail_read  # type: ignore[method-assign]
    adapter = GoogleSheetsAdapter(
        spreadsheet_id="sheet",
        service=service,
        service_account_email="bot@example.invalid",
    )

    with pytest.raises(RuntimeError) as bootstrap_error:
        adapter.prepare_bootstrap(service_account_email="bot@example.invalid")
    with pytest.raises(RuntimeError) as delivery_error:
        adapter.prepare_delivery(
            export_id="exp_" + "a" * 32,
            canonical_sha256="b" * 64,
            values=_values(),
        )

    assert str(bootstrap_error.value) == "Google Sheets preparation read failed"
    assert str(delivery_error.value) == "Google Sheets preparation read failed"
    assert secret not in str(bootstrap_error.value)
    assert secret not in str(delivery_error.value)


def test_capacity_preflight_blocks_before_mutation() -> None:
    service = FakeService()
    rows = service.document["sheets"][0]["data"][0]["rowData"]
    for row in rows[4:MAX_VALUE_ROW]:
        row["values"][0]["userEnteredValue"] = {"stringValue": "occupied"}
    result = _adapter(service).deliver(export_id="exp_" + "a" * 32, canonical_sha256="b" * 64, values=_values())
    assert (result.outcome, result.safe_code, service.calls) == (
        DeliveryOutcome.BLOCKED,
        SafeCode.TEMPLATE_DRIFT,
        [],
    )

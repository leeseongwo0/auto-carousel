"""Frozen ``workplace`` schema, projection, and request builders (no Google imports)."""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

WORKPLACE_TITLE = "workplace"
WORKPLACE_SHEET_ID = 0
WORKPLACE_SCHEMA = "workplace-template-v1"
DELIVERY_METADATA_KEY = "newsbot_workplace_handoff_v1"
MAX_CELL_UTF16 = 50_000
MAX_REQUEST_BYTES = 1_900_000
MAX_MARKERS = 25_000
MAX_VALUE_ROW = 1_000
MAX_METADATA_KEY_BYTES = 64
MAX_METADATA_VALUE_BYTES = 160
SCHEMA_METADATA_KEY = "newsbot_workplace_schema_v1"
SCHEMA_METADATA_VALUE_SUFFIX = ":controls-v1"
PROTECTION_DESCRIPTION = "newsbot-workplace-v1:immutable-A-D"
_CATEGORY_VALUES = ("AI", "Blockchain")
_UPLOAD_VALUES = ("O", "X")

_CELLS: tuple[tuple[str | None, ...], ...] = (
    (None, "기본 정보", None, None, None, None, None, None, "p2", None, None, None, None, None, None, None, None, None, None, None, None, None),
    (None, None, None, None, None, "캡션", "p1", None, None, None, "p3", None, "p4", None, "p5", None, "p6", None, "p7", None, "p8", None),
    (None, "일자", "페이지 수", "분류(AI/Blockchain)", "업로드여부", None, "제목", "부제", "소제목", "본문", "소제목", "본문", "소제목", "본문", "소제목", "본문", "소제목", "본문", "소제목", "본문", "소제목", "본문"),
)
_MERGES = ((0, 2, 1, 5), (0, 2, 8, 10), (1, 3, 5, 6), (1, 2, 6, 8), (1, 2, 10, 12), (1, 2, 12, 14), (1, 2, 14, 16), (1, 2, 16, 18), (1, 2, 18, 20), (1, 2, 20, 22))
_ORACLE = {"schema": WORKPLACE_SCHEMA, "title": WORKPLACE_TITLE, "sheet_id": 0, "cells": _CELLS, "merges": sorted(_MERGES), "notes_policy": "absent", "formulas_policy": "absent"}
_ORACLE_BYTES = json.dumps(_ORACLE, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
WORKPLACE_ORACLE_FINGERPRINT = hashlib.sha256(_ORACLE_BYTES).hexdigest()


def _require_string(value: object, field: str) -> str:
    if not isinstance(value, str) or value != unicodedata.normalize("NFC", value):
        raise ValueError(f"{field} must be an NFC string")
    if "\0" in value or any(0xD800 <= ord(char) <= 0xDFFF for char in value):
        raise ValueError(f"{field} contains an invalid Unicode scalar")
    if len(value.encode("utf-16-le")) // 2 > MAX_CELL_UTF16:
        raise ValueError(f"{field} exceeds the cell limit")
    return value


def delivery_metadata_value(export_id: str, canonical_sha256: str) -> str:
    _require_string(export_id, "export_id")
    if len(export_id) != 36 or not export_id.startswith("exp_") or any(char not in "0123456789abcdef" for char in export_id[4:]):
        raise ValueError("export_id must be exp_ followed by 32 lowercase hexadecimal characters")
    if len(canonical_sha256) != 64 or any(char not in "0123456789abcdef" for char in canonical_sha256):
        raise ValueError("canonical_sha256 must be lowercase SHA-256")
    value = f"v1:{export_id}:{canonical_sha256}"
    if len(value.encode()) > MAX_METADATA_VALUE_BYTES or len(DELIVERY_METADATA_KEY.encode()) > MAX_METADATA_KEY_BYTES:
        raise ValueError("metadata value exceeds limit")
    return value

def schema_metadata_value() -> str:
    value = f"{WORKPLACE_ORACLE_FINGERPRINT}{SCHEMA_METADATA_VALUE_SUFFIX}"
    if len(SCHEMA_METADATA_KEY.encode()) > MAX_METADATA_KEY_BYTES or len(value.encode()) > MAX_METADATA_VALUE_BYTES:
        raise AssertionError("schema metadata bounds")
    return value


def build_bootstrap_request(
    *, service_account_email: str, document: Mapping[str, Any] | None = None
) -> dict[str, object]:
    """Build a non-value-mutating batch for only the missing fixed controls."""
    _require_string(service_account_email, "service_account_email")
    if not service_account_email:
        raise ValueError("service_account_email is required")
    category_range = {"sheetId": WORKPLACE_SHEET_ID, "startRowIndex": 3, "endRowIndex": MAX_VALUE_ROW, "startColumnIndex": 3, "endColumnIndex": 4}
    upload_range = {"sheetId": WORKPLACE_SHEET_ID, "startRowIndex": 3, "endRowIndex": MAX_VALUE_ROW, "startColumnIndex": 4, "endColumnIndex": 5}
    protected_range = {"sheetId": WORKPLACE_SHEET_ID, "startRowIndex": 3, "endRowIndex": MAX_VALUE_ROW, "startColumnIndex": 0, "endColumnIndex": 4}

    def rule(options: tuple[str, ...]) -> dict[str, object]:
        return {
            "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": value} for value in options]},
            "strict": True,
            "showCustomUi": True,
        }

    metadata_present = validations_complete = protection_present = False
    if document is not None:
        target = _target_sheet(document)
        metadata_present, validations_complete, protection_present = _validate_controls(
            document, target, service_account_email, require_complete=False
        )
    requests: list[dict[str, object]] = []
    if not metadata_present:
        requests.append({"createDeveloperMetadata": {"developerMetadata": {
            "metadataKey": SCHEMA_METADATA_KEY, "metadataValue": schema_metadata_value(),
            "visibility": "DOCUMENT", "location": {"spreadsheet": True},
        }}})
    if not validations_complete:
        requests.extend((
            {"setDataValidation": {"range": category_range, "rule": rule(_CATEGORY_VALUES)}},
            {"setDataValidation": {"range": upload_range, "rule": rule(_UPLOAD_VALUES)}},
        ))
    if not protection_present:
        requests.append({"addProtectedRange": {"protectedRange": {
            "range": protected_range, "description": PROTECTION_DESCRIPTION, "warningOnly": False,
            "editors": {"users": [service_account_email], "groups": [], "domainUsersCanEdit": False},
        }}})
    return {"requests": requests}


def project_handoff(*, approved_date: str, page_count: int, category: str, caption: str, pages: Sequence[tuple[str, str]]) -> tuple[str, ...]:
    """Return the literal 22-string A:V projection; unused page pairs are blank."""
    if not isinstance(approved_date, str):
        raise ValueError("approved_date must be YYYY-MM-DD")
    try:
        if date.fromisoformat(approved_date).isoformat() != approved_date:
            raise ValueError
    except ValueError as exc:
        raise ValueError("approved_date must be YYYY-MM-DD") from exc
    if not 1 <= page_count <= 8 or len(pages) != page_count:
        raise ValueError("page_count must be 1..8 and match pages")
    if category not in {"AI", "Blockchain"}:
        raise ValueError("category must be AI or Blockchain")
    values = ["", approved_date, str(page_count), category, "X", caption]
    for title, subtitle in pages:
        values.extend((title, subtitle))
    values.extend([""] * (22 - len(values)))
    result = tuple(_require_string(value, f"cell {index}") for index, value in enumerate(values))
    if len(result) != 22:
        raise AssertionError("projection must be A:V")
    return result


def build_delivery_request(*, export_id: str, canonical_sha256: str, values: Sequence[str]) -> dict[str, object]:
    """Build the sole allowed atomic mutation: metadata then typed append."""
    marker = delivery_metadata_value(export_id, canonical_sha256)
    if len(values) != 22:
        raise ValueError("delivery projection must contain exactly 22 strings")
    typed = [{"userEnteredValue": {"stringValue": _require_string(value, f"cell {index}")}} for index, value in enumerate(values)]
    body: dict[str, object] = {"requests": [
        {"createDeveloperMetadata": {"developerMetadata": {"metadataKey": DELIVERY_METADATA_KEY, "metadataValue": marker, "visibility": "DOCUMENT", "location": {"spreadsheet": True}}}},
        {"appendCells": {"sheetId": WORKPLACE_SHEET_ID, "rows": [{"values": typed}], "fields": "userEnteredValue"}},
    ]}
    if len(json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()) > MAX_REQUEST_BYTES:
        raise ValueError("request exceeds byte limit")
    return body


def validate_workplace(
    document: Mapping[str, Any],
    *,
    bootstrap: bool = False,
    service_account_email: str | None = None,
    controls_required: bool = True,
    strict_existing_values: bool = False,
) -> str:
    """Fail closed unless sheetId 0 has the frozen A1:V3 oracle.

    Delivery deliberately validates only the immutable oracle.  Bootstrap additionally
    validates row four and the exact controls it is about to reuse.
    """
    sheets = document.get("sheets")
    if not isinstance(sheets, list):
        raise ValueError("missing sheets")
    target = _target_sheet(document)
    props = target.get("properties", {})
    if props.get("title") != WORKPLACE_TITLE or props.get("gridProperties", {}).get("rowCount", 0) < 4 or props.get("gridProperties", {}).get("columnCount", 0) < 22:
        raise ValueError("workplace target drift")
    if sorted(_merge_tuple(item) for item in target.get("merges", [])) != sorted(_MERGES):
        raise ValueError("workplace merge drift")
    rows = _sheet_rows(target)
    actual = tuple(tuple(_cell_value(rows, row, col) for col in range(22)) for row in range(3))
    if actual != _CELLS:
        raise ValueError("workplace header drift")
    if bootstrap:
        row_four = tuple(_cell_value(rows, 3, column) for column in range(22))
        if row_four != (None, None, None, "AI", "O") + (None,) * 17:
            raise ValueError("workplace bootstrap row four drift")
        if strict_existing_values:
            for row in range(3, len(rows)):
                upload_status = _cell_value(rows, row, 4)
                if upload_status not in {None, *_UPLOAD_VALUES}:
                    raise ValueError("workplace upload status drift")
        _validate_controls(
            document,
            target,
            service_account_email,
            require_complete=controls_required,
        )
    return WORKPLACE_ORACLE_FINGERPRINT


def next_value_row(document: Mapping[str, Any]) -> int:
    """Return the append row based only on nonblank entered values/formulas."""
    sheets = document.get("sheets")
    if not isinstance(sheets, list):
        raise ValueError("missing sheets")
    target = next((sheet for sheet in sheets if isinstance(sheet, Mapping) and sheet.get("properties", {}).get("sheetId") == 0), None)
    if not isinstance(target, Mapping):
        raise ValueError("workplace sheetId 0 missing")
    last = 0
    for block in target.get("data", []):
        if not isinstance(block, Mapping):
            continue
        start_row = block.get("startRow", block.get("startRowIndex", 0))
        start_column = block.get("startColumn", block.get("startColumnIndex", 0))
        if not isinstance(start_row, int) or not isinstance(start_column, int):
            raise ValueError("invalid grid data")
        rows = block.get("rowData", [])
        if not isinstance(rows, list):
            raise ValueError("invalid grid data")
        for offset, row in enumerate(rows):
            values = row.get("values", []) if isinstance(row, Mapping) else []
            if not isinstance(values, list):
                raise ValueError("invalid row data")
            if start_column >= 22:
                continue
            for _col, cell in enumerate(values[: 22 - start_column]):
                if isinstance(cell, Mapping) and _has_entered_value(cell):
                    last = max(last, start_row + offset + 1)
                    break
    candidate = last + 1
    if candidate > MAX_VALUE_ROW:
        raise ValueError("workplace value row capacity exceeded")
    return candidate


def _sheet_rows(target: Mapping[str, Any]) -> list[object]:
    data = target.get("data", [])
    if not isinstance(data, list) or not data or not isinstance(data[0], Mapping):
        return []
    if data[0].get("startRow", data[0].get("startRowIndex", 0)) != 0 or data[0].get("startColumn", data[0].get("startColumnIndex", 0)) != 0:
        raise ValueError("oracle grid starts at unexpected offset")
    rows = data[0].get("rowData", [])
    return rows if isinstance(rows, list) else []


def _has_entered_value(cell: Mapping[str, Any]) -> bool:
    value = cell.get("userEnteredValue")
    if not isinstance(value, Mapping):
        return False
    if "formulaValue" in value:
        return True
    if "stringValue" in value:
        return bool(value["stringValue"])
    return bool(value)


def _rule_is(value: object, options: tuple[str, ...]) -> bool:
    return value == {
        "condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": item} for item in options]},
        "strict": True,
        "showCustomUi": True,
    }


def _grid_validations(target: Mapping[str, Any]) -> dict[tuple[int, int], object]:
    result: dict[tuple[int, int], object] = {}
    data = target.get("data", [])
    if not isinstance(data, list):
        raise ValueError("invalid grid data")
    for block in data:
        if not isinstance(block, Mapping):
            raise ValueError("invalid grid data")
        start_row = block.get("startRow", block.get("startRowIndex", 0))
        start_column = block.get("startColumn", block.get("startColumnIndex", 0))
        rows = block.get("rowData", [])
        if not isinstance(start_row, int) or not isinstance(start_column, int) or not isinstance(rows, list):
            raise ValueError("invalid grid data")
        for row_offset, row in enumerate(rows):
            values = row.get("values", []) if isinstance(row, Mapping) else []
            if not isinstance(values, list):
                raise ValueError("invalid grid data")
            for column_offset, cell in enumerate(values):
                if not isinstance(cell, Mapping):
                    raise ValueError("invalid grid data")
                if cell.get("dataValidation") is not None:
                    coordinate = (start_row + row_offset, start_column + column_offset)
                    if coordinate in result:
                        raise ValueError("overlapping validation grid data")
                    result[coordinate] = cell["dataValidation"]
    return result

def _target_sheet(document: Mapping[str, Any]) -> Mapping[str, Any]:
    sheets = document.get("sheets")
    if not isinstance(sheets, list):
        raise ValueError("missing sheets")
    target = next(
        (
            sheet
            for sheet in sheets
            if isinstance(sheet, Mapping)
            and sheet.get("properties", {}).get("sheetId") == WORKPLACE_SHEET_ID
        ),
        None,
    )
    if not isinstance(target, Mapping):
        raise ValueError("workplace sheetId 0 missing")
    return target


def _validate_controls(
    document: Mapping[str, Any],
    target: Mapping[str, Any],
    service_account_email: str | None,
    *,
    require_complete: bool,
) -> tuple[bool, bool, bool]:
    metadata = document.get("developerMetadata", [])
    if not isinstance(metadata, list):
        raise ValueError("invalid developer metadata")
    schema_entries = [item for item in metadata if isinstance(item, Mapping) and item.get("metadataKey") == SCHEMA_METADATA_KEY]
    if schema_entries and (
        len(schema_entries) != 1
        or schema_entries[0].get("metadataKey") != SCHEMA_METADATA_KEY
        or schema_entries[0].get("metadataValue") != schema_metadata_value()
        or schema_entries[0].get("visibility") != "DOCUMENT"
        or not _document_location_is_spreadsheet(schema_entries[0].get("location"))
    ):
        raise ValueError("workplace schema metadata drift")

    validations = _grid_validations(target)
    expected_validations = {
        **{(row, 3): _CATEGORY_VALUES for row in range(3, MAX_VALUE_ROW)},
        **{(row, 4): _UPLOAD_VALUES for row in range(3, MAX_VALUE_ROW)},
    }
    if not set(validations) <= set(expected_validations):
        raise ValueError("workplace validation range drift")
    if any(
        not _rule_is(rule, expected_validations[coordinate])
        for coordinate, rule in validations.items()
    ):
        raise ValueError("workplace validation drift")
    validations_complete = set(validations) == set(expected_validations)
    metadata_present = bool(schema_entries)

    protections = target.get("protectedRanges", [])

    expected_range = {
        "startRowIndex": 3,
        "endRowIndex": 1000,
        "startColumnIndex": 0,
        "endColumnIndex": 4,
    }
    if not isinstance(protections, list):
        raise ValueError("invalid protected ranges")
    protection_present = bool(protections)
    protection = protections[0] if len(protections) == 1 else None
    protection_range = protection.get("range") if isinstance(protection, Mapping) else None
    editors = protection.get("editors", {}) if isinstance(protection, Mapping) else None
    if protection_present and (
        service_account_email is None
        or not isinstance(protection, Mapping)
        or not isinstance(protection_range, Mapping)
        or protection_range.get("sheetId", 0) != WORKPLACE_SHEET_ID
        or {key: protection_range.get(key) for key in expected_range} != expected_range
        or protection.get("description") != PROTECTION_DESCRIPTION
        or protection.get("warningOnly", False) is not False
        or not isinstance(editors, Mapping)
        or service_account_email not in editors.get("users", [])
        or editors.get("groups", []) != []
        or editors.get("domainUsersCanEdit", False) is not False
    ):
        raise ValueError("workplace protection drift")
    if require_complete and not (
        metadata_present and validations_complete and protection_present
    ):
        raise ValueError("workplace controls incomplete")
    return metadata_present, validations_complete, protection_present

def _document_location_is_spreadsheet(value: object) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("spreadsheet") is True
        and value.get("locationType", "SPREADSHEET") == "SPREADSHEET"
    )


def _cell_value(rows: object, row: int, column: int) -> str | None:
    if not isinstance(rows, list) or row >= len(rows) or not isinstance(rows[row], Mapping):
        return None
    values = rows[row].get("values", [])
    if not isinstance(values, list) or column >= len(values) or not isinstance(values[column], Mapping):
        return None
    cell = values[column]
    if "note" in cell:
        raise ValueError("notes are forbidden in oracle")
    value = cell.get("userEnteredValue")
    if isinstance(value, Mapping) and "formulaValue" in value:
        raise ValueError("formulas are forbidden in oracle")
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {"stringValue"}:
        raise ValueError("oracle value type drift")
    return _require_string(value["stringValue"], "oracle value")


def _merge_tuple(value: object) -> tuple[int, int, int, int]:
    if not isinstance(value, Mapping):
        raise ValueError("invalid merge")
    try:
        return (int(value["startRowIndex"]), int(value["endRowIndex"]), int(value["startColumnIndex"]), int(value["endColumnIndex"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("invalid merge") from exc

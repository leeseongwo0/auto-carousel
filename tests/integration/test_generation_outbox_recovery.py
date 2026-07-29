import json

import pytest

from newsbot.exports import canonical_json_bytes, canonical_markdown_bytes, materialize_export


def _payload(export_id: str) -> tuple[bytes, bytes]:
    payload = {
        "export_schema_version": "newsbot-export-v1",
        "candidate_id": 1,
        "generation_id": 1,
        "approval_event_id": 1,
        "source_version_ids": [1],
        "pages": [{"title": "Durable", "subtitle": "", "factual_units": []}],
        "caption": {"text": "caption", "hashtags": []},
        "draft": True,
        "source_reported": True,
        "export_id": export_id,
    }
    return canonical_json_bytes(payload), canonical_markdown_bytes(payload)


def test_materializer_repairs_a_partial_pair_idempotently(tmp_path):
    export_id = "exp_" + "a" * 32
    json_bytes, markdown_bytes = _payload(export_id)

    first = materialize_export(tmp_path, export_id, json_bytes, markdown_bytes)
    first.markdown_path.unlink()
    repaired = materialize_export(tmp_path, export_id, json_bytes, markdown_bytes)

    assert repaired.json_path.read_bytes() == json_bytes
    assert repaired.markdown_path.read_bytes() == markdown_bytes
    assert repaired.json_path.stat().st_mode & 0o777 == 0o600
    assert json.loads(repaired.json_path.read_text(encoding="utf-8"))["export_id"] == export_id


def test_materializer_preserves_an_unexpected_member_and_reports_corruption(tmp_path):
    export_id = "exp_" + "b" * 32
    json_bytes, markdown_bytes = _payload(export_id)
    json_path = tmp_path / f"{export_id}.json"
    json_path.write_bytes(b"foreign bytes")

    with pytest.raises(FileExistsError):
        materialize_export(tmp_path, export_id, json_bytes, markdown_bytes)

    assert json_path.read_bytes() == b"foreign bytes"
    assert not (tmp_path / f"{export_id}.md").exists()

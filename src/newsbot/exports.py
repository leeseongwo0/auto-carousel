"""Canonical, crash-safe export materialization."""

from __future__ import annotations

import json
import os
import re
import tempfile
from contextlib import suppress
from dataclasses import asdict, dataclass, is_dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeGuard

from .storage import Storage

if TYPE_CHECKING:
    from _typeshed import DataclassInstance


@dataclass(frozen=True, slots=True)
class ExportPair:
    export_id: str
    json_path: Path
    markdown_path: Path
    json_digest: str
    markdown_digest: str


def canonical_json_bytes(value: Any) -> bytes:
    """Encode JSON once, with a stable representation suitable for hashing."""
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default).encode(
            "utf-8"
        )
        + b"\n"
    )


def canonical_markdown_bytes(value: dict[str, Any]) -> bytes:
    """Render the review-approved payload without timestamps or local paths."""
    pages = value["pages"]
    lines = [f"# {pages[0]['title']}"]
    if pages[0].get("subtitle"):
        lines.extend(("", pages[0]["subtitle"]))
    for index, page in enumerate(pages[1:], start=2):
        lines.extend(("", f"## {index}. {page['subtitle']}", "", page["body"]))
    caption = value["caption"]
    lines.extend(("", "## Caption", "", caption["text"]))
    warnings = value.get("warnings", [])
    if warnings:
        lines.extend(("", "## Warnings"))
        lines.extend(f"- {warning['kind']}: {warning['detail']}" for warning in warnings)
    return ("\n".join(lines) + "\n").encode("utf-8")


def source_version_identity(source: dict[str, Any]) -> str:
    """Return the portable identity of one immutable source version."""
    material = {
        "source_schema_version": "newsbot-source-version-v1",
        "channel_id": source["channel_id"],
        "external_post_id": source["external_post_id"],
        "source_url": source.get("source_url"),
        "version_key": source["version_key"],
        "body": source["body"],
        "media": source["media"],
        "kind": source["kind"],
        "sponsored": bool(source["sponsored"]),
        "urls": source["urls"],
        "conflicts": source["conflicts"],
    }
    return "srcv_" + sha256(canonical_json_bytes(material)).hexdigest()[:32]


def source_identity(source: dict[str, Any]) -> str:
    """Return the portable identity of the nonlocal source record."""
    return (
        "src_"
        + sha256(
            canonical_json_bytes(
                {
                    "source_schema_version": "newsbot-source-v1",
                    "channel_id": source["channel_id"],
                    "external_post_id": source["external_post_id"],
                }
            )
        ).hexdigest()[:32]
    )


def source_material_identity(source: dict[str, Any]) -> str:
    """Return the portable identity of immutable source material."""
    return (
        "mat_"
        + sha256(
            canonical_json_bytes(
                {
                    "source_material_schema_version": "newsbot-source-material-v1",
                    "source_identity": source_identity(source),
                    "version_key": source["version_key"],
                    "body": source["body"],
                    "media": source["media"],
                    "kind": source["kind"],
                    "sponsored": bool(source["sponsored"]),
                    "urls": source["urls"],
                    "conflicts": source["conflicts"],
                }
            )
        ).hexdigest()[:32]
    )


def source_observation_identity(source: dict[str, Any]) -> str:
    """Return the portable identity of the exact captured source observation."""
    material_identity = source_material_identity(source)
    return (
        "obs_"
        + sha256(
            canonical_json_bytes(
                {
                    "source_observation_schema_version": "newsbot-source-observation-v1",
                    "source_identity": source_identity(source),
                    "material_identity": material_identity,
                    "observation_key": source["observation_key"],
                    "captured_at": source["captured_at"],
                    "engagement": source.get("engagement"),
                }
            )
        ).hexdigest()[:32]
    )


def source_claim_identity(source: dict[str, Any]) -> str:
    """Return the portable identity of the server-owned evidence claim."""
    evidence = str(source["body"])
    conflicts = tuple(str(value) for value in source.get("conflicts", ()))
    uncertainty = tuple(str(value) for value in source.get("uncertainty", ()))
    return (
        "claim_"
        + sha256(
            canonical_json_bytes(
                {
                    "source_claim_schema_version": "newsbot-source-claim-v1",
                    "material_identity": source_material_identity(source),
                    "observation_identity": source_observation_identity(source),
                    "evidence": evidence,
                    "evidence_spans": [[0, len(evidence)]],
                    "conflicts": conflicts,
                    "uncertainty": uncertainty,
                }
            )
        ).hexdigest()[:32]
    )


def generation_claim_payload(source: dict[str, Any], source_version_id: int) -> dict[str, Any]:
    evidence = str(source["body"])
    conflicts = [str(value) for value in source.get("conflicts", ())]
    uncertainty = [str(value) for value in source.get("uncertainty", ())]
    return {
        "schema_version": "newsbot-generation-claim-v1",
        "claim_id": source_claim_identity(source),
        "source_version_id": source_version_id,
        "source_identity": source_identity(source),
        "material_identity": source_material_identity(source),
        "observation_identity": source_observation_identity(source),
        "captured_at": source["captured_at"],
        "source_url": source.get("source_url"),
        "evidence": evidence,
        "evidence_spans": [[0, len(evidence)]],
        "conflicts": conflicts,
        "uncertainty": uncertainty,
    }


def approved_candidate_content_identity(content: dict[str, Any]) -> str:
    """Return the portable identity of the approved candidate content."""
    return (
        "cand_"
        + sha256(
            canonical_json_bytes(
                {
                    "approved_candidate_content_schema_version": "newsbot-approved-candidate-content-v1",
                    "content": content,
                }
            )
        ).hexdigest()[:32]
    )


def _source_provenance(source: dict[str, Any], claim: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_identity": source_identity(source),
        "source_version_identity": source_version_identity(source),
        "material_identity": source_material_identity(source),
        "source_url": source["source_url"],
        "channel_id": source["channel_id"],
        "external_post_id": source["external_post_id"],
        "version_key": source["version_key"],
        "urls": source["urls"],
        "conflicts": source["conflicts"],
        "observation_identity": claim["observation_identity"],
        "claim_id": claim["claim_id"],
        "captured_at": claim["captured_at"],
        "body": source["body"],
        "evidence": claim["evidence"],
        "evidence_spans": claim["evidence_spans"],
        "uncertainty": claim["uncertainty"],
    }


def generation_content_identity(content: dict[str, Any], revision: int) -> str:
    """Return the portable identity of a generated content revision."""
    return (
        "gen_"
        + sha256(
            canonical_json_bytes(
                {
                    "generation_schema_version": "newsbot-generation-v1",
                    "revision": revision,
                    "content": content,
                }
            )
        ).hexdigest()[:32]
    )


def approval_decision_identity(
    *, generation_identity: str, source_version_identities: tuple[str, ...], approval: dict[str, Any]
) -> str:
    """Return the portable identity of an approval decision, without row IDs."""
    return (
        "dec_"
        + sha256(
            canonical_json_bytes(
                {
                    "decision_schema_version": "newsbot-approval-v1",
                    "generation_identity": generation_identity,
                    "source_version_identities": sorted(source_version_identities),
                    "approval": approval,
                }
            )
        ).hexdigest()[:32]
    )


def approval_outbox_intent(
    *,
    candidate_id: int,
    generation_id: int,
    approval_event_id: int,
    source_version_ids: tuple[int, ...],
    content_json: str,
    warnings: tuple[dict[str, str], ...] = (),
    source_versions: tuple[dict[str, Any], ...] = (),
    generation_revision: int = 1,
    approval: dict[str, Any] | None = None,
) -> tuple[str, bytes, bytes]:
    """Build immutable, content-addressed export bytes for an approval."""
    # SQLite identities are outbox metadata only, never portable bytes.
    _ = candidate_id, generation_id, approval_event_id, source_version_ids
    content = json.loads(content_json)
    sources_by_local_id = {int(source["source_version_id"]): source for source in source_versions}
    raw_manifest = content.get("claim_manifest")
    if not isinstance(raw_manifest, list) or not raw_manifest:
        raise ValueError("generation claim manifest is required")
    claims_by_local_id: dict[int, dict[str, Any]] = {}
    required_claim_keys = {
        "schema_version",
        "claim_id",
        "source_version_id",
        "source_identity",
        "material_identity",
        "observation_identity",
        "captured_at",
        "source_url",
        "evidence",
        "evidence_spans",
        "conflicts",
        "uncertainty",
    }
    for raw_claim in raw_manifest:
        if not isinstance(raw_claim, dict) or set(raw_claim) != required_claim_keys:
            raise ValueError("generation claim manifest shape is invalid")
        source_id = int(raw_claim["source_version_id"])
        source = sources_by_local_id.get(source_id)
        if source is None or source_id in claims_by_local_id:
            raise ValueError("generation claim manifest source binding is invalid")
        if raw_claim["schema_version"] != "newsbot-generation-claim-v1":
            raise ValueError("generation claim manifest version is invalid")
        if raw_claim["source_identity"] != source_identity(source) or raw_claim[
            "material_identity"
        ] != source_material_identity(source):
            raise ValueError("generation claim manifest identity is invalid")
        claims_by_local_id[source_id] = raw_claim
    if set(claims_by_local_id) != set(source_version_ids):
        raise ValueError("generation claim manifest is incomplete")
    provenance_by_local_id = {
        source_id: _source_provenance(source, claims_by_local_id[source_id])
        for source_id, source in sources_by_local_id.items()
    }
    source_identities = tuple(
        provenance["source_version_identity"]
        for provenance in sorted(provenance_by_local_id.values(), key=lambda item: item["source_version_identity"])
    )
    portable_claims = sorted(
        (
            {key: value for key, value in claim.items() if key != "source_version_id"}
            for claim in claims_by_local_id.values()
        ),
        key=lambda claim: str(claim["claim_id"]),
    )
    if "cover" in content:
        pages = [
            {
                "title": content["cover"]["title"],
                "subtitle": content["cover"]["subtitle"],
                "factual_units": _content_addressed_units(content["cover"]["factual_units"], provenance_by_local_id),
            },
            *[
                {
                    "subtitle": body["subtitle"],
                    "body": body["body"],
                    "factual_units": _content_addressed_units(body["factual_units"], provenance_by_local_id),
                }
                for body in content["bodies"]
            ],
        ]
        caption = {
            "text": "\n\n".join(
                (
                    content["caption"]["hook"],
                    content["caption"]["context"],
                    content["caption"]["details"],
                    content["caption"]["implications"],
                    content["caption"]["questions"],
                    " ".join(content["caption"]["hashtags"]),
                )
            ),
            "hashtags": content["caption"]["hashtags"],
        }
    else:
        pages = [
            {**page, "factual_units": _content_addressed_units(page.get("factual_units", []), provenance_by_local_id)}
            for page in content["pages"]
        ]
        caption = content["caption"]
    portable_content = {
        "pages": pages,
        "caption": caption,
        "draft": content.get("draft") is True,
        "source_reported": content.get("source_reported") is True,
    }
    approved_content_identity = approved_candidate_content_identity(portable_content)
    generation_identity = generation_content_identity(portable_content, generation_revision)
    decision_identity = approval_decision_identity(
        generation_identity=generation_identity,
        source_version_identities=source_identities,
        approval={} if approval is None else approval,
    )
    semantic = {
        "export_schema_version": "newsbot-export-v3",
        "source_versions": sorted(provenance_by_local_id.values(), key=lambda item: item["source_version_identity"]),
        "claims": portable_claims,
        "approved_candidate_content_identity": approved_content_identity,
        "generation_identity": generation_identity,
        "approval_decision_identity": decision_identity,
        "pages": pages,
        "caption": caption,
        "warnings": sorted((dict(warning) for warning in warnings), key=canonical_json_bytes),
        "draft": True,
        "source_reported": True,
    }
    export_id = "exp_" + sha256(canonical_json_bytes(semantic)).hexdigest()[:32]
    payload = {**semantic, "export_id": export_id}
    return (
        export_id,
        canonical_json_bytes(payload),
        (f"<!-- export_id: {export_id} -->\n".encode() + canonical_markdown_bytes(payload)),
    )


def _content_addressed_reference(
    reference: dict[str, Any], provenance_by_local_id: dict[int, dict[str, Any]]
) -> dict[str, str]:
    source_id = int(reference["source_version_id"])
    provenance = provenance_by_local_id.get(source_id)
    if provenance is None or reference["claim_id"] != provenance["claim_id"]:
        raise ValueError("factual reference does not resolve to the generation claim manifest")
    return {
        "claim_id": str(reference["claim_id"]),
        "source_version_identity": str(provenance["source_version_identity"]),
        "source_identity": str(provenance["source_identity"]),
        "material_identity": str(provenance["material_identity"]),
        "observation_identity": str(provenance["observation_identity"]),
    }


def _content_addressed_units(
    units: list[dict[str, Any]], provenance_by_local_id: dict[int, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Preserve server-issued claim identities with actionable source provenance."""
    return [
        {
            "text": unit["text"],
            "references": [
                _content_addressed_reference(reference, provenance_by_local_id)
                for reference in unit.get("references", [])
            ],
        }
        for unit in units
    ]


def materialize_export(
    output_dir: str | Path, export_id: str, canonical_json: bytes, canonical_markdown: bytes
) -> ExportPair:
    """Atomically write a JSON/Markdown pair without replacing foreign content."""
    if re.fullmatch(r"exp_[0-9a-f]{32}", export_id) is None:
        raise ValueError("export_id must use the exp_ prefix followed by 32 lowercase hexadecimal characters")
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / f"{export_id}.json"
    markdown_path = directory / f"{export_id}.md"
    for path, payload in ((json_path, canonical_json), (markdown_path, canonical_markdown)):
        if path.exists() and path.read_bytes() != payload:
            raise FileExistsError(f"refusing to overwrite unexpected export content: {path}")
        if not path.exists():
            _atomic_write(path, payload)
        if path.read_bytes() != payload:
            raise OSError(f"export verification failed for {path}")
    return ExportPair(export_id, json_path, markdown_path, _digest(canonical_json), _digest(canonical_markdown))


def materialize_outbox(storage: Storage, output_dir: str | Path, generation_id: int) -> ExportPair:
    """Materialize the byte authority committed in SQLite, including crash recovery."""
    rows = storage.fetch_all("SELECT * FROM export_outbox WHERE generation_id=? ORDER BY export_kind", (generation_id,))
    try:
        export_id, payloads = _validated_outbox_pair(rows)
    except (TypeError, ValueError):
        _mark_outbox_corrupt(storage, generation_id)
        raise RuntimeError("export outbox canonical bytes are invalid") from None
    with storage.transaction() as connection:
        connection.execute(
            "UPDATE export_outbox SET status='materializing', attempts=attempts+1 "
            "WHERE generation_id=? AND status IN ('pending', 'materializing', 'ready')",
            (generation_id,),
        )
    try:
        pair = materialize_export(output_dir, export_id, payloads["json"], payloads["markdown"])
    except (FileExistsError, OSError):
        _mark_outbox_corrupt(storage, generation_id)
        raise
    with storage.transaction() as connection:
        connection.execute(
            "UPDATE export_outbox SET status='ready', delivered_at=CURRENT_TIMESTAMP WHERE generation_id=?",
            (generation_id,),
        )
    return pair


def verify_ready_outbox(storage: Storage, output_dir: str | Path, generation_id: int) -> ExportPair | None:
    """Return a ready pair only when SQLite authority and both files still verify."""
    rows = storage.fetch_all("SELECT * FROM export_outbox WHERE generation_id=? ORDER BY export_kind", (generation_id,))
    if len(rows) != 2 or any(str(row["status"]) != "ready" for row in rows):
        return None
    try:
        export_id, payloads = _validated_outbox_pair(rows)
        directory = Path(output_dir)
        pair = ExportPair(
            export_id,
            directory / f"{export_id}.json",
            directory / f"{export_id}.md",
            _digest(payloads["json"]),
            _digest(payloads["markdown"]),
        )
        missing = not pair.json_path.exists() or not pair.markdown_path.exists()
        if missing:
            with storage.transaction() as connection:
                connection.execute(
                    "UPDATE export_outbox SET status='pending' WHERE generation_id=? AND status='ready'",
                    (generation_id,),
                )
            return None
        if pair.json_path.read_bytes() != payloads["json"] or pair.markdown_path.read_bytes() != payloads["markdown"]:
            raise ValueError("export files do not match SQLite authority")
    except (OSError, TypeError, ValueError):
        _mark_outbox_corrupt(storage, generation_id)
        return None
    return pair


def _validated_outbox_pair(rows: list[Any]) -> tuple[str, dict[str, bytes]]:
    if len(rows) != 2 or {str(row["export_kind"]) for row in rows} != {"json", "markdown"}:
        raise ValueError("generation has no complete export outbox intent")
    export_id = str(rows[0]["export_id"])
    if re.fullmatch(r"exp_[0-9a-f]{32}", export_id) is None or any(str(row["export_id"]) != export_id for row in rows):
        raise ValueError("export outbox has inconsistent export identities")
    payloads = {str(row["export_kind"]): bytes(row["canonical_bytes"]) for row in rows}
    if any(_digest(payloads[str(row["export_kind"])]) != str(row["sha256"]) for row in rows):
        raise ValueError("export outbox digest mismatch")
    try:
        payload = json.loads(payloads["json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("export outbox JSON is invalid") from exc
    if not isinstance(payload, dict) or payload.get("export_id") != export_id:
        raise ValueError("export outbox JSON identity is invalid")
    if canonical_json_bytes(payload) != payloads["json"]:
        raise ValueError("export outbox JSON is not canonical")
    expected_markdown = f"<!-- export_id: {export_id} -->\n".encode() + canonical_markdown_bytes(payload)
    if payloads["markdown"] != expected_markdown:
        raise ValueError("export outbox Markdown is not canonical")
    return export_id, payloads


def _mark_outbox_corrupt(storage: Storage, generation_id: int) -> None:
    with storage.transaction() as connection:
        connection.execute("UPDATE export_outbox SET status='corrupt' WHERE generation_id=?", (generation_id,))


def _atomic_write(path: Path, payload: bytes) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(temporary_name)
        raise


def _digest(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def _json_default(value: object) -> object:
    if _is_dataclass_instance(value):
        return asdict(value)
    raise TypeError(f"cannot encode {type(value).__name__} as canonical JSON")


def _is_dataclass_instance(value: object) -> TypeGuard[DataclassInstance]:
    return is_dataclass(value) and not isinstance(value, type)

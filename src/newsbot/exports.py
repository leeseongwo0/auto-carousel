"""Canonical payload and provenance identities for durable delivery."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Any, TypeGuard

if TYPE_CHECKING:
    from _typeshed import DataclassInstance


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
    category = content.get("category")
    if category is not None and category not in ("AI", "Blockchain"):
        raise ValueError("approved content category must be exactly 'AI' or 'Blockchain'")
    portable_content = {
        **({"category": category} if category is not None else {}),
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
        **({"category": category} if category is not None else {}),
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


def _json_default(value: object) -> object:
    if _is_dataclass_instance(value):
        return asdict(value)
    raise TypeError(f"cannot encode {type(value).__name__} as canonical JSON")


def _is_dataclass_instance(value: object) -> TypeGuard[DataclassInstance]:
    return is_dataclass(value) and not isinstance(value, type)

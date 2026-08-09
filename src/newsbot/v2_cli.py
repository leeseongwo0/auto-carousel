"""Operational commands for the independent Newsbot V2 workflow.

The commands intentionally accept explicit database paths. V2 never opens or
migrates the legacy Newsbot database.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .collectors.base import SourceObservation, UrlCandidate
from .v2_runtime import V2Runtime
from .v2_workflow import V2Workflow


def _observation(value: dict[str, Any]) -> SourceObservation:
    published = datetime.fromisoformat(str(value["published_at"]))
    if published.tzinfo is None:
        published = published.replace(tzinfo=UTC)
    urls = tuple(UrlCandidate(str(url)) for url in value.get("urls", ()))
    return SourceObservation(
        channel_id=str(value["channel_id"]),
        channel_handle=str(value.get("channel_handle", value["channel_id"])),
        external_post_id=str(value["external_post_id"]),
        published_at=published,
        text=str(value.get("text", "")),
        urls=urls,
    )


def collect_fixture(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("fixture must contain a JSON array")
    candidates: list[str] = []
    with V2Workflow(args.db) as workflow:
        for item in payload:
            if not isinstance(item, dict):
                raise ValueError("each fixture item must be an object")
            candidate = workflow.record_observation(_observation(item))
            if candidate is not None and candidate.id not in candidates:
                candidates.append(candidate.id)
    print(json.dumps({"candidates": candidates, "count": len(candidates)}, ensure_ascii=False))
    return 0


def run_fixture(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("fixture must contain a JSON array")
    results: list[dict[str, str]] = []
    with V2Workflow(args.db) as workflow:
        runtime = V2Runtime(
            workflow,
            notify_candidate=lambda candidate: True,
            generate_draft=lambda candidate: json.dumps(
                {"draft": True, "source_reported": True, "text": candidate.observation["text"]},
                ensure_ascii=False,
            ),
            notify_draft=lambda draft: True,
            deliver_sheets=lambda draft: True,
        )
        for item in payload:
            if not isinstance(item, dict):
                raise ValueError("each fixture item must be an object")
            result = runtime.process_observation(_observation(item))
            if result is not None:
                results.append({"id": result.id, "state": result.state})
    print(json.dumps({"results": results, "count": len(results)}, ensure_ascii=False))
    return 0


def status(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        candidates = workflow.list_candidates()
        print(json.dumps({"candidates": [asdict(candidate) for candidate in candidates]}, ensure_ascii=False))
    return 0


def approve_candidate(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        candidate = workflow.approve_candidate(args.candidate_id)
        print(json.dumps({"id": candidate.id, "state": candidate.state}))
    return 0


def create_draft(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        draft = workflow.create_draft(args.candidate_id, args.content)
        print(json.dumps({"id": draft.id, "candidate_id": draft.candidate_id, "state": draft.state}))
    return 0


def approve_draft(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        draft = workflow.approve_draft(args.draft_id)
        print(json.dumps({"id": draft.id, "state": draft.state}))
    return 0


def deliver_sheet(args: argparse.Namespace) -> int:
    """Advance only after the Sheets adapter has confirmed delivery."""
    with V2Workflow(args.db) as workflow:
        draft = workflow.mark_sheet_delivered(args.draft_id)
        print(json.dumps({"id": draft.id, "state": draft.state}))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="newsbot-v2")
    parser.add_argument("--db", type=Path, required=True)
    commands = parser.add_subparsers(dest="command", required=True)

    e2e = commands.add_parser("run-fixture", help="run the credential-free V2 approval and delivery flow")
    e2e.add_argument("--fixture", type=Path, required=True)
    e2e.set_defaults(handler=run_fixture)

    fixture = commands.add_parser("collect-fixture")
    fixture.add_argument("--fixture", type=Path, required=True)
    fixture.set_defaults(handler=collect_fixture)

    view = commands.add_parser("status")
    view.set_defaults(handler=status)

    candidate = commands.add_parser("approve-candidate")
    candidate.add_argument("candidate_id")
    candidate.set_defaults(handler=approve_candidate)

    draft = commands.add_parser("create-draft")
    draft.add_argument("candidate_id")
    draft.add_argument("content")
    draft.set_defaults(handler=create_draft)

    approve = commands.add_parser("approve-draft")
    approve.add_argument("draft_id")
    approve.set_defaults(handler=approve_draft)

    deliver = commands.add_parser("deliver-sheet")
    deliver.add_argument("draft_id")
    deliver.set_defaults(handler=deliver_sheet)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())

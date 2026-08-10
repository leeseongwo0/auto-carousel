"""Operational commands for the independent Newsbot V2 workflow.

The commands intentionally accept explicit database paths. V2 never opens or
migrates the legacy Newsbot database.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from .collectors.base import SourceObservation, UrlCandidate
from .collectors.telethon import TelethonCollector
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


def collect_live(args: argparse.Namespace) -> int:
    """Collect configured private-environment Telegram handles into only the V2 DB."""
    api_id = os.environ["NEWSBOT_V2_TELETHON_API_ID"]
    api_hash = os.environ["NEWSBOT_V2_TELETHON_API_HASH"]
    session = os.environ["NEWSBOT_V2_TELETHON_SESSION"]
    handles = tuple(
        item.strip().lstrip("@") for item in os.environ["NEWSBOT_V2_TELEGRAM_HANDLES"].split(",") if item.strip()
    )
    if not handles:
        raise ValueError("NEWSBOT_V2_TELEGRAM_HANDLES must contain at least one handle")
    collector = TelethonCollector(int(api_id), api_hash, session)

    async def collect() -> list[SourceObservation]:
        try:
            observations: list[SourceObservation] = []
            lower_bound = datetime.now(UTC) - timedelta(hours=args.lookback_hours)
            for handle in handles:
                observations.extend(await collector.collect(handle, lower_bound=lower_bound, limit=args.limit))
            return observations
        finally:
            await collector.close()

    observations = asyncio.run(collect())
    candidates: list[str] = []
    with V2Workflow(args.db) as workflow:
        for observation in observations:
            candidate = workflow.record_observation(observation)
            if candidate is not None:
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


def generate_codex(args: argparse.Namespace) -> int:
    """Generate one approved V2 candidate through the fixed Codex runner and notify its exact draft."""
    from types import SimpleNamespace

    from .approval.telegram import TelegramApprovalAdapter
    from .sheets.base import SheetDelivery
    from .v2_live import (
        CandidateNotificationPort,
        DraftNotificationPort,
        SheetsDeliveryPort,
        TelegramV2Notifier,
        V2CodexGenerator,
        V2LiveWorkflow,
    )
    from .v2_runtime import AmbiguousRemoteEffect
    from .v2_workflow import V2Candidate, V2Draft

    def refuse_candidate(_candidate: V2Candidate, _token: str) -> str | bool:
        raise AmbiguousRemoteEffect("unexpected V2 candidate notification transition")

    def refuse_sheets(_draft: V2Draft) -> SheetDelivery | bool:
        raise AmbiguousRemoteEffect("unexpected V2 Sheets transition")

    with V2Workflow(args.db) as workflow:
        adapter = TelegramApprovalAdapter(
            os.environ["TELEGRAM_BOT_TOKEN"],
            cast(Any, SimpleNamespace(chat_id=int(os.environ["NEWSBOT_APPROVER_CHAT_ID"]))),
        )
        notifier = TelegramV2Notifier(adapter)
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=cast(CandidateNotificationPort, refuse_candidate),
            generate_draft=V2CodexGenerator(),
            notify_draft=cast(DraftNotificationPort, notifier.draft),
            deliver_sheets=cast(SheetsDeliveryPort, refuse_sheets),
        )
        result = live.run(args.candidate_id)
        print(json.dumps({"id": result.id, "state": result.state}))
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

    live = commands.add_parser(
        "collect-live", help="collect configured Telegram handles using private V2 environment credentials"
    )
    live.add_argument("--lookback-hours", type=int, choices=range(1, 169), default=24)
    live.add_argument("--limit", type=int, choices=range(1, 501), default=100)
    live.set_defaults(handler=collect_live)

    codex = commands.add_parser("generate-codex", help="generate and notify one approved V2 candidate")
    codex.add_argument("candidate_id")
    codex.set_defaults(handler=generate_codex)

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

"""Operational commands for the independent Newsbot V2 workflow.

The commands intentionally accept explicit database paths. V2 never opens or
migrates the legacy Newsbot database.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from contextlib import suppress
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from .approval.telegram import TelegramApprovalAdapter, TelegramDeadline
from .collectors.base import SourceObservation, UrlCandidate
from .collectors.telethon import TelethonCollector
from .v2_live import (
    CandidateNotificationPort,
    DraftNotificationPort,
    TelegramV2Notifier,
    V2LiveWorkflow,
    deliver_v2_google_sheets,
    recover_v2_google_sheets_delivery,
    v2_draft_handoff_values,
)
from .v2_runtime import V2Runtime
from .v2_workflow import V2Draft, V2State, V2Workflow, V2WorkflowError


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
        codex = []
        for candidate in candidates:
            request = workflow.get_codex_request(candidate.id)
            if request is None:
                continue
            codex.append(
                {
                    "candidate_id": candidate.id,
                    "request_digest": request.digest,
                    "status": request.status,
                    "output_digest": request.output_digest,
                    "attempts": [
                        {
                            "number": attempt.number,
                            "status": attempt.status,
                            "error_code": attempt.error_code,
                        }
                        for attempt in workflow.list_codex_attempts(candidate.id)
                    ],
                }
            )
        print(
            json.dumps(
                {
                    "candidates": [asdict(candidate) for candidate in candidates],
                    "codex": codex,
                },
                ensure_ascii=False,
            )
        )
    return 0


def _settle_google_sheets_bootstrap_failure(workflow: V2Workflow, draft: V2Draft, error: Exception) -> V2Draft:
    """Terminally record local/configuration failures before a dispatch adapter exists."""
    receipt = workflow.remote_effect(draft.id, "sheets_delivery")
    if receipt is None or receipt["status"] == "failed":
        workflow.record_remote_attempt(draft.id, "sheets_delivery")
    workflow.settle_remote_effect(
        draft.id,
        "sheets_delivery",
        "failed",
        detail=json.dumps(
            {
                "error": type(error).__name__,
                "failure": "terminal_pre_dispatch_failure",
                "phase": "bootstrap_failed",
            },
            sort_keys=True,
        ),
    )
    return cast(
        V2Draft,
        workflow.mark_manual_review(draft.id, f"sheets_delivery bootstrap failed: {type(error).__name__}"),
    )


def _deliver_google_sheets_draft(workflow: V2Workflow, draft: V2Draft, deadline: float) -> V2Draft:
    """Deliver one approved V2 draft through the existing idempotent Sheets adapter."""
    import time
    from zoneinfo import ZoneInfo

    from .secrets import read_service_account_info
    from .sheets.google import GoogleSheetsAdapter

    if draft.state == V2State.SHEET_DELIVERED:
        return draft
    if draft.state != V2State.DRAFT_APPROVED:
        raise V2WorkflowError("V2 Google Sheets delivery requires draft_approved")
    receipt = workflow.remote_effect(draft.id, "sheets_delivery")
    if receipt is not None and receipt["status"] != "failed":
        return recover_v2_google_sheets_delivery(workflow, draft)
    if receipt is not None and receipt["status"] == "failed":
        try:
            detail = json.loads(str(receipt["detail"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            detail = None
        attempts = receipt.get("attempts")
        retryable = (
            isinstance(detail, dict)
            and detail.get("failure") == "clear_pre_dispatch_network"
            and isinstance(attempts, int)
            and not isinstance(attempts, bool)
            and attempts < 2
        )
        if not retryable:
            return cast(
                V2Draft,
                workflow.mark_manual_review(draft.id, "sheets_delivery retry is not authorized"),
            )
    if deadline <= 0:
        raise ValueError("--deadline must be positive")
    try:
        approved_at = datetime.fromisoformat(workflow.draft_updated_at(draft.id))
        if approved_at.tzinfo is None:
            approved_at = approved_at.replace(tzinfo=UTC)
        approved_date = approved_at.astimezone(ZoneInfo("Asia/Seoul")).date().isoformat()
        values = v2_draft_handoff_values(draft, approved_date)
        credentials = read_service_account_info(os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"])
        adapter = GoogleSheetsAdapter.from_credentials(
            credential_info=credentials,
            spreadsheet_id=os.environ["GOOGLE_SHEETS_SPREADSHEET_ID"],
            deadline_monotonic=time.monotonic() + deadline,
        )
    except Exception as exc:
        return _settle_google_sheets_bootstrap_failure(workflow, draft, exc)
    return deliver_v2_google_sheets(workflow, draft, adapter, values, lease_seconds=deadline + 30)


def deliver_google_sheets(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        result = _deliver_google_sheets_draft(workflow, workflow.get_draft(args.draft_id), args.deadline)
        print(json.dumps({"id": result.id, "state": result.state}))
    return 0


def deliver_google_sheets_next(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        draft = workflow.next_draft_approved_sheets_delivery()
        if draft is None:
            print(json.dumps({"status": "no_work"}))
            return 0
        result = _deliver_google_sheets_draft(workflow, draft, args.deadline)
        print(json.dumps({"id": result.id, "state": result.state}))
    return 0


def seed_telegram_cursor(args: argparse.Namespace) -> int:
    with V2Workflow(args.db) as workflow:
        next_offset = workflow.handoff_telegram_cursor(args.next_offset)
    print(json.dumps({"next_offset": next_offset}))
    return 0


def _telegram_callback_status(
    workflow: V2Workflow, update: dict[str, Any], *, chat_id: int, user_ids: set[int]
) -> str | None:
    callback = update.get("callback_query")
    if not isinstance(callback, dict):
        return None
    message, user = callback.get("message"), callback.get("from")
    token = callback.get("data")
    if not isinstance(message, dict) or not isinstance(user, dict) or not isinstance(token, str):
        return None
    chat = message.get("chat")
    remote_chat_id, remote_user_id = (chat.get("id") if isinstance(chat, dict) else None), user.get("id")
    if (
        isinstance(remote_chat_id, bool)
        or not isinstance(remote_chat_id, int)
        or remote_chat_id != chat_id
        or isinstance(remote_user_id, bool)
        or not isinstance(remote_user_id, int)
        or remote_user_id not in user_ids
    ):
        return None
    from .approval.base import hash_callback_token

    try:
        token_hash = hash_callback_token(token)
    except ValueError:
        return None
    settled = workflow.settle_callback_any(token_hash, datetime.now(UTC).isoformat())
    return None if settled is None else f"v2_{settled[1]}_approved"


def telegram_tick(args: argparse.Namespace) -> int:
    """Run the sole V2 Telegram send-and-poll tick against the V2 cursor."""
    chat_id = int(os.environ["NEWSBOT_APPROVER_CHAT_ID"])
    user_ids = {int(value) for value in os.environ["NEWSBOT_APPROVER_USER_IDS"].split(",") if value.strip()}
    if not user_ids:
        raise ValueError("NEWSBOT_APPROVER_USER_IDS must contain at least one user ID")
    if args.deadline <= 0 or args.timeout < 0:
        raise ValueError("invalid Telegram tick bounds")
    adapter = TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], type("Audience", (), {"chat_id": chat_id})())
    deadline = TelegramDeadline.after(args.deadline)
    handled = 0
    with V2Workflow(args.db) as workflow:
        offset = workflow.telegram_next_offset()
        if offset is None:
            raise V2WorkflowError("V2 Telegram cursor handoff is required")
        notifier = TelegramV2Notifier(adapter, deadline_seconds=args.deadline)
        workflow.reconcile_expired_approval_capabilities(datetime.now(UTC).isoformat())
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=cast(CandidateNotificationPort, notifier.candidate),
            generate_draft=lambda _candidate: "",
            notify_draft=cast(DraftNotificationPort, notifier.draft),
            deliver_sheets=lambda _draft: False,
        )
        draft = workflow.next_draft_pending_notification()
        candidate = None if draft is not None else workflow.next_candidate_pending_notification()
        if draft is not None:
            live.run(draft.candidate_id)
        elif candidate is not None:
            live.run(candidate.id)
        payload = {"timeout": str(args.timeout), "limit": "100"}
        payload["offset"] = str(offset)
        response = adapter._request("getUpdates", payload, deadline=deadline)
        updates = response.get("result")
        if not isinstance(updates, list):
            raise RuntimeError("Telegram Bot API getUpdates returned an invalid result")
        for update in updates:
            if not isinstance(update, dict):
                continue
            update_id = update.get("update_id")
            if isinstance(update_id, bool) or not isinstance(update_id, int) or update_id < 0:
                continue
            status = _telegram_callback_status(workflow, update, chat_id=chat_id, user_ids=user_ids)
            callback = update.get("callback_query")
            callback_id = callback.get("id") if isinstance(callback, dict) else None
            if status is not None:
                handled += 1
                if isinstance(callback_id, str):
                    with suppress(Exception):
                        adapter._request(
                            "answerCallbackQuery",
                            {"callback_query_id": callback_id, "text": status},
                            deadline=deadline,
                        )
            workflow.advance_telegram_cursor(update_id + 1)
    print(json.dumps({"handled": handled}))
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

    fixture = commands.add_parser("collect-fixture")
    fixture.add_argument("--fixture", type=Path, required=True)
    fixture.set_defaults(handler=collect_fixture)

    view = commands.add_parser("status")
    view.set_defaults(handler=status)

    sheets = commands.add_parser("deliver-google-sheets", help="deliver one second-approved V2 draft")
    sheets.add_argument("draft_id")
    sheets.add_argument("--deadline", type=float, default=120.0)
    sheets.set_defaults(handler=deliver_google_sheets)

    sheets_next = commands.add_parser("deliver-google-sheets-next", help="deliver one approved V2 draft")
    sheets_next.add_argument("--deadline", type=float, default=120.0)
    sheets_next.set_defaults(handler=deliver_google_sheets_next)

    cursor = commands.add_parser("seed-telegram-cursor", help="merge the stopped owner's Telegram cursor")
    cursor.add_argument("--next-offset", type=int, required=True)
    cursor.set_defaults(handler=seed_telegram_cursor)

    telegram = commands.add_parser("telegram-tick", help="send one V2 approval and poll the V2 Telegram stream")
    telegram.add_argument("--deadline", type=float, default=30.0)
    telegram.add_argument("--timeout", type=int, choices=range(0, 51), default=10)
    telegram.set_defaults(handler=telegram_tick)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())

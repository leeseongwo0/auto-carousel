"""Operational commands for the independent Newsbot V2 workflow.

The commands intentionally accept explicit database paths. V2 never opens or
migrates the legacy Newsbot database.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import tempfile
from contextlib import suppress
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from .approval.telegram import TelegramApprovalAdapter, TelegramDeadline
from .collectors.base import SourceObservation, UrlCandidate
from .collectors.telethon import TelethonCollector
from .v2_article import (
    ArticleResult,
    ArticleSnapshot,
    SafeArticleTransport,
    UrlReference,
    body_identity,
    material_character_count,
    select_article_urls,
)
from .v2_live import (
    CandidateNotificationPort,
    DraftNotificationPort,
    TelegramV2Notifier,
    V2LiveWorkflow,
    deliver_v2_google_sheets,
    recover_v2_google_sheets_delivery,
    v2_draft_handoff_values,
)
from .v2_observability import LoggingObservabilitySink
from .v2_policy import (
    SourceDisposition,
    V2Outcome,
    V2PolicyInput,
    V2PolicyResult,
    evaluate_v2_content,
)
from .v2_workflow import V2Candidate, V2Draft, V2State, V2Workflow, V2WorkflowError


def _fixture_datetime(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _url_source(value: object) -> Literal["preview", "entity", "bare"]:
    source = str(value)
    if source not in {"preview", "entity", "bare"}:
        raise ValueError(f"invalid URL source: {source}")
    return cast(Literal["preview", "entity", "bare"], source)


def _observation(value: dict[str, Any]) -> SourceObservation:
    published = _fixture_datetime(value["published_at"])
    urls = tuple(
        UrlCandidate(
            str(item["url"]) if isinstance(item, dict) else str(item),
            source=_url_source(item.get("source", "bare") if isinstance(item, dict) else "bare"),
            occurrence=int(item.get("occurrence", index)) if isinstance(item, dict) else index,
            title=str(item["title"]) if isinstance(item, dict) and item.get("title") is not None else None,
            description=(
                str(item["description"]) if isinstance(item, dict) and item.get("description") is not None else None
            ),
        )
        for index, item in enumerate(value.get("urls", ()))
    )
    return SourceObservation(
        channel_id=str(value["channel_id"]),
        channel_handle=str(value.get("channel_handle", value["channel_id"])),
        external_post_id=str(value["external_post_id"]),
        published_at=published,
        observed_at=_fixture_datetime(value["observed_at"]) if value.get("observed_at") else None,
        edited_at=_fixture_datetime(value["edited_at"]) if value.get("edited_at") else None,
        preview_title=str(value["preview_title"]) if value.get("preview_title") is not None else None,
        preview_description=(
            str(value["preview_description"]) if value.get("preview_description") is not None else None
        ),
        text=str(value.get("text", "")),
        sponsored=bool(value.get("sponsored", False)),
        urls=urls,
    )


def _source_failure_policy(
    observation: SourceObservation,
    *,
    disposition: SourceDisposition,
) -> V2PolicyResult:
    """Evaluate source failure through the same total policy lattice."""
    return evaluate_v2_content(
        V2PolicyInput(
            telegram_text=observation.text,
            telegram_date=observation.published_at,
            display_url=observation.urls[0].url if observation.urls else None,
            preview_title=observation.preview_title,
            preview_description=observation.preview_description,
            sponsored=observation.sponsored,
            source_disposition=disposition,
        )
    )


def _finalize_leased_observation(
    workflow: V2Workflow,
    observation: SourceObservation,
    transport: Any,
    lease: Any,
) -> V2Candidate | None:
    selected = select_article_urls(
        (UrlReference(item.url, item.source, item.occurrence) for item in observation.urls),
        limit=8,
    )
    if not selected:
        return workflow.finalize_enrichment(
            lease,
            ArticleSnapshot(ArticleResult.PERMANENT_FAILURE, ""),
            _source_failure_policy(
                observation,
                disposition=SourceDisposition.NO_ELIGIBLE_URL,
            ),
        )

    if observation.published_at < datetime.now(UTC) - timedelta(hours=24):
        persisted = asdict(
            ArticleSnapshot(
                ArticleResult.PERMANENT_FAILURE,
                selected[0].requested_url,
            )
        )
        persisted["canonical_url"] = selected[0].canonical_url
        return workflow.finalize_enrichment(
            lease,
            persisted,
            V2PolicyResult(V2Outcome.NON_NEWS, "freshness_gate", "freshness"),
        )

    if not workflow.mark_enrichment_dispatched(lease):
        return None

    snapshot = ArticleSnapshot(
        ArticleResult.PERMANENT_FAILURE,
        selected[0].requested_url,
    )
    selected_url = selected[0]
    all_unsafe = True
    for selected_url in selected[:3]:
        snapshot = transport.fetch(
            selected_url.requested_url,
            telegram_date=observation.published_at,
        )
        if snapshot.result is ArticleResult.SUCCESS:
            break
        if snapshot.result is ArticleResult.TRANSIENT_FAILURE:
            if lease.attempt_number == 1:
                workflow.settle_enrichment(lease, snapshot, transient=True)
                return None
            persisted = asdict(snapshot)
            persisted["canonical_url"] = selected_url.canonical_url
            return workflow.finalize_enrichment(
                lease,
                persisted,
                _source_failure_policy(
                    observation,
                    disposition=SourceDisposition.SOURCE_UNAVAILABLE,
                ),
            )
        all_unsafe &= snapshot.result is ArticleResult.UNSAFE_URL
    else:
        persisted = asdict(snapshot)
        persisted["canonical_url"] = selected_url.canonical_url
        policy = _source_failure_policy(
            observation,
            disposition=(SourceDisposition.UNSAFE_SOURCE_URL if all_unsafe else SourceDisposition.SOURCE_UNAVAILABLE),
        )
        return workflow.finalize_enrichment(lease, persisted, policy)

    policy = evaluate_v2_content(
        V2PolicyInput(
            telegram_text=observation.text,
            telegram_date=observation.published_at,
            display_url=snapshot.canonical_url,
            preview_title=observation.preview_title,
            preview_description=observation.preview_description,
            article_title=snapshot.title,
            article_body=snapshot.body,
            source_date=snapshot.source_date,
            source_date_conflict=snapshot.source_date_conflict,
            sponsored=observation.sponsored,
            source_disposition=SourceDisposition.SUCCESS,
        )
    )
    return workflow.finalize_enrichment(lease, snapshot, policy)


def _finalize_observation(
    workflow: V2Workflow,
    observation: SourceObservation,
    transport: Any,
    *,
    owner: str,
) -> V2Candidate | None:
    """Record an immutable intent, then use the durable due-work boundary."""
    revision = workflow.record_revision(observation)
    if workflow.observation_has_claim(revision.identity):
        return None
    lease = workflow.claim_enrichment(
        f"{owner}:{secrets.token_hex(16)}",
        revision.id,
    )
    if lease is None:
        return None
    return _finalize_leased_observation(
        workflow,
        observation,
        transport,
        lease,
    )


def _drain_enrichment_queue(
    workflow: V2Workflow,
    transport: Any,
    *,
    owner: str,
    limit: int,
) -> list[str]:
    """Drain one bounded DB-owned due-work page, including prior-run retries."""
    candidate_ids: list[str] = []
    for _ in range(limit):
        lease = workflow.claim_enrichment(f"{owner}:{secrets.token_hex(16)}")
        if lease is None:
            break
        observation = _observation(workflow.get_revision_observation(lease.revision_id))
        candidate = _finalize_leased_observation(
            workflow,
            observation,
            transport,
            lease,
        )
        if candidate is not None and candidate.id not in candidate_ids:
            candidate_ids.append(candidate.id)
    return candidate_ids


class _FixtureArticleTransport:
    """Deterministic in-process article port; it follows the live orchestration exactly."""

    def __init__(self, values: dict[str, dict[str, Any]]) -> None:
        self._values = values

    def fetch(
        self,
        requested_url: str,
        *,
        telegram_date: datetime,
    ) -> ArticleSnapshot:
        item = self._values[requested_url]
        result = ArticleResult(
            str(
                item.get(
                    "article_result",
                    ArticleResult.SUCCESS,
                )
            )
        )
        body = str(
            item.get(
                "article_body",
                item.get("text", ""),
            )
        )
        raw_title = item.get("article_title")
        title = None if raw_title is None else str(raw_title)
        return ArticleSnapshot(
            result=result,
            requested_url=requested_url,
            final_url=requested_url,
            canonical_url=requested_url,
            title=title,
            body=body,
            body_hash=body_identity(
                body,
                title=title,
            ),
            material_count=material_character_count(
                body,
                title=title,
            ),
            source_date=None,
            source_date_evidence=None,
            source_date_conflict=False,
            provenance={},
        )


def collect_fixture(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.fixture).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("fixture must contain a JSON array")
    candidates: list[str] = []
    fixture_values: dict[str, dict[str, Any]] = {}
    observations: list[SourceObservation] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ValueError("each fixture item must be an object")
        observation = _observation(item)
        observations.append(observation)
        fixture_values.update({url.url: item for url in observation.urls})
    transport = _FixtureArticleTransport(fixture_values)
    with V2Workflow(args.db, mode="create") as workflow:
        for observation in observations:
            candidate = _finalize_observation(workflow, observation, transport, owner="fixture-collect")
            if candidate is not None and candidate.id not in candidates:
                candidates.append(candidate.id)
    print(json.dumps({"candidates": candidates, "count": len(candidates)}, ensure_ascii=False))
    return 0


def _live_article_transport() -> SafeArticleTransport:
    blocked_hosts = tuple(
        value.strip()
        for value in os.environ.get(
            "NEWSBOT_V2_BLOCKED_ARTICLE_HOSTS",
            "",
        ).split(",")
        if value.strip()
    )
    return SafeArticleTransport(
        blocked_hosts=blocked_hosts,
        observability=LoggingObservabilitySink(),
    )


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

    async def collect(
        workflow: V2Workflow,
        intake_budget: int,
    ) -> None:
        try:
            initial_lower_bound = datetime.now(UTC) - timedelta(hours=args.lookback_hours)
            remaining = intake_budget
            for index, handle in enumerate(handles):
                if remaining <= 0:
                    break
                channels_left = len(handles) - index
                allocation = max(1, remaining // channels_left)
                high_water, edit_watermark = workflow.channel_cursor(handle)
                upper = await collector.latest_message_id(handle)
                due_before = workflow.enrichment_backlog_count(cap=args.limit + 1)
                new_limit = allocation if upper is None or upper <= high_water else max(1, allocation // 2)
                if upper is not None and upper > high_water and new_limit > 0:
                    new_page = list(
                        await collector.collect_ascending(
                            handle,
                            after_message_id=high_water,
                            upper_message_id=upper,
                            limit=new_limit,
                            lower_bound=(initial_lower_bound if high_water == 0 else None),
                        )
                    )
                    workflow.record_new_message_page(
                        handle,
                        new_page,
                        upper_message_id=upper,
                        page_limit=new_limit,
                    )
                due_after_new = workflow.enrichment_backlog_count(cap=args.limit + 1)
                added_new = max(0, due_after_new - due_before)
                handle_remaining = max(0, allocation - added_new)
                remaining = max(0, args.limit - due_after_new)
                edit_limit = min(handle_remaining, remaining)
                if edit_limit <= 0:
                    continue
                before_message_id, scan_started_raw = workflow.edit_scan_state(handle)
                scan_started = (
                    datetime.now(UTC) if scan_started_raw is None else datetime.fromisoformat(scan_started_raw)
                )
                edit_page = await collector.collect_edit_sweep(
                    handle,
                    after=edit_watermark,
                    before_message_id=before_message_id,
                    lower_bound=scan_started - timedelta(hours=24),
                    limit=edit_limit,
                )
                workflow.record_edit_sweep_page(
                    handle,
                    list(edit_page.observations),
                    next_before_message_id=(edit_page.next_before_message_id),
                    scan_started_at=scan_started.isoformat(),
                    complete=edit_page.complete,
                )
                due_after_edit = workflow.enrichment_backlog_count(cap=args.limit + 1)
                remaining = max(0, args.limit - due_after_edit)
        finally:
            await collector.close()

    with V2Workflow(args.db, mode="runtime") as workflow:
        workflow.reconcile_expired_enrichment_leases()
        due = workflow.enrichment_backlog_count(cap=args.limit + 1)
        intake_budget = max(0, args.limit - due)
        asyncio.run(collect(workflow, intake_budget))
        candidates = _drain_enrichment_queue(
            workflow,
            _live_article_transport(),
            owner="live-collect",
            limit=args.limit,
        )
    print(
        json.dumps(
            {"candidates": candidates, "count": len(candidates)},
            ensure_ascii=False,
        )
    )
    return 0


def _backup_sqlite(source_path: Path, destination_path: Path) -> None:
    source_uri = f"file:{source_path.resolve().as_posix()}?mode=ro"
    with (
        sqlite3.connect(
            source_uri,
            uri=True,
            timeout=5,
        ) as source,
        sqlite3.connect(destination_path) as destination,
    ):
        source.backup(destination)
        if destination.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise V2WorkflowError("SQLite backup failed integrity check")


def _sqlite_snapshot_hash(path: Path, directory: Path, name: str) -> str:
    snapshot = directory / name
    _backup_sqlite(path, snapshot)
    return hashlib.sha256(snapshot.read_bytes()).hexdigest()


def _same_filesystem(left: Path, right: Path) -> bool:
    return left.stat().st_dev == right.stat().st_dev


def _migration_preflight(
    source_path: Path,
    backup_path: Path,
) -> dict[str, object]:
    if not source_path.is_file():
        raise V2WorkflowError("migration source database does not exist")
    source = source_path.resolve()
    backup = backup_path.resolve()
    if source == backup:
        raise V2WorkflowError("migration backup must differ from source")
    if backup.exists():
        raise V2WorkflowError("migration backup path already exists")
    if not backup.parent.is_dir():
        raise V2WorkflowError("migration backup directory does not exist")
    wal_path = Path(str(source) + "-wal")
    source_bytes = source.stat().st_size
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    combined_bytes = source_bytes + wal_bytes
    shared_filesystem = _same_filesystem(source.parent, backup.parent)
    source_required_free_bytes = (4 if shared_filesystem else 3) * combined_bytes
    backup_required_free_bytes = 0 if shared_filesystem else combined_bytes
    source_free_bytes = shutil.disk_usage(source.parent).free
    backup_free_bytes = source_free_bytes if shared_filesystem else shutil.disk_usage(backup.parent).free
    if source_free_bytes < source_required_free_bytes:
        raise V2WorkflowError("migration preflight source filesystem lacks database-plus-WAL working headroom")
    if backup_free_bytes < backup_required_free_bytes:
        raise V2WorkflowError("migration preflight backup filesystem lacks snapshot capacity")

    source_uri = f"file:{source.as_posix()}?mode=ro"
    try:
        with sqlite3.connect(
            source_uri,
            uri=True,
            timeout=5,
        ) as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if integrity is None or str(integrity[0]).lower() != "ok":
                raise V2WorkflowError("migration preflight integrity check failed")
            if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise V2WorkflowError("migration preflight foreign key check failed")

        with sqlite3.connect(
            source,
            timeout=5,
        ) as connection:
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint is None or int(checkpoint[0]) != 0:
                raise V2WorkflowError("migration preflight WAL checkpoint is busy")
    except sqlite3.DatabaseError as error:
        raise V2WorkflowError("migration preflight SQLite validation failed") from error

    reserved = False
    try:
        descriptor = os.open(
            backup,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600,
        )
        os.close(descriptor)
        reserved = True
        _backup_sqlite(source, backup)
    except FileExistsError as error:
        raise V2WorkflowError("migration backup path already exists") from error
    except BaseException:
        if reserved and backup.exists():
            backup.unlink()
        raise
    return {
        "source_bytes": source_bytes,
        "wal_bytes": wal_bytes,
        "shared_filesystem": shared_filesystem,
        "source_required_free_bytes": source_required_free_bytes,
        "source_free_bytes": source_free_bytes,
        "backup_required_free_bytes": backup_required_free_bytes,
        "backup_free_bytes": backup_free_bytes,
        "backup": str(backup),
        "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest(),
    }


def validate_selection(args: argparse.Namespace) -> int:
    """Replay a reviewed fixture three times on a disposable V2 database copy."""
    if not args.no_send:
        raise ValueError("validate-selection requires --no-send")
    payload = json.loads(args.fixture.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError("fixture must contain a JSON object array")
    observations = [_observation(cast(dict[str, Any], item)) for item in payload]
    fixture_values = {
        url.url: cast(dict[str, Any], item)
        for item, observation in zip(payload, observations, strict=True)
        for url in observation.urls
    }

    cycles: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="newsbot-v2-validate-") as directory_name:
        directory = Path(directory_name)
        before = _sqlite_snapshot_hash(
            args.db,
            directory,
            "production-before.sqlite",
        )
        copied_db = directory / "newsbot-v2.sqlite"
        _backup_sqlite(args.db, copied_db)
        transport = _FixtureArticleTransport(fixture_values)
        for cycle in range(1, 4):
            with V2Workflow(copied_db, mode="runtime") as workflow:
                counts_before = workflow.validation_counts()
                for observation in observations:
                    _finalize_observation(
                        workflow,
                        observation,
                        transport,
                        owner=f"no-send-cycle-{cycle}",
                    )
                counts_after = workflow.validation_counts()
                invariants = workflow.verify_invariants()
            deltas = {key: counts_after[key] - counts_before[key] for key in counts_before}
            cycles.append(
                {
                    "cycle": cycle,
                    "invariants": invariants,
                    "deltas": deltas,
                    "external_effects": (deltas["remote_effects"] + deltas["callbacks"]),
                    "held_release": (deltas["held_candidates"] < 0),
                }
            )
        after = _sqlite_snapshot_hash(
            args.db,
            directory,
            "production-after.sqlite",
        )

    copy_stable_after_first = all(all(delta == 0 for delta in cycle["deltas"].values()) for cycle in cycles[1:])
    production_stable = before == after
    passed = (
        production_stable
        and copy_stable_after_first
        and all(
            cycle["external_effects"] == 0
            and cycle["held_release"] is False
            and all(value == 0 for value in cycle["invariants"].values())
            for cycle in cycles
        )
    )
    print(
        json.dumps(
            {
                "no_send": True,
                "passed": passed,
                "cycles": cycles,
                "before_db_hash": before,
                "after_db_hash": after,
                "stable": production_stable,
                "copy_stable_after_first": (copy_stable_after_first),
            },
            sort_keys=True,
        )
    )
    return 0 if passed else 1


def status(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="runtime") as workflow:
        items, next_cursor = workflow.status_page(
            getattr(args, "limit", 50),
            getattr(args, "cursor", None),
            getattr(args, "state", None),
        )
        baseline_raw = os.environ.get("NEWSBOT_V2_SEVEN_DAY_STORAGE_BASELINE_BYTES")
        baseline = None if baseline_raw is None else int(baseline_raw)
        aggregate = workflow.status_aggregate(seven_day_storage_baseline_bytes=baseline)
        print(
            json.dumps(
                {
                    "aggregate": aggregate,
                    "items": [asdict(item) for item in items],
                    "next_cursor": next_cursor,
                },
                ensure_ascii=False,
            )
        )
    return 0


def create_db(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="create") as workflow:
        print(json.dumps({"mode": "create", "schema": workflow.SCHEMA_VERSION}))
    return 0


def runtime_db(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="runtime") as workflow:
        print(json.dumps({"mode": "runtime", "schema": workflow.SCHEMA_VERSION}))
    return 0


def migrate_db(args: argparse.Namespace) -> int:
    preflight = _migration_preflight(
        args.db,
        args.backup,
    )
    with V2Workflow(
        args.db,
        mode="migrate",
        migration_deadline_seconds=args.timeout_seconds,
    ) as workflow:
        print(
            json.dumps(
                {
                    "mode": "migrate",
                    "schema": workflow.SCHEMA_VERSION,
                    "preflight": preflight,
                }
            )
        )
    return 0


def verify_db(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="verify") as workflow:
        print(json.dumps({"mode": "verify", "invariants": workflow.verify_invariants()}))
    return 0


def compact_db(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="runtime") as workflow:
        print(json.dumps(workflow.compact(batch_size=args.batch_size, dry_run=args.dry_run)))
    return 0


def hold_backlog(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="runtime") as workflow:
        print(json.dumps(workflow.hold_notification_eligible_candidates(), ensure_ascii=False))
    return 0


def release_backlog(args: argparse.Namespace) -> int:
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    required_item_keys = {
        "id",
        "revision_digest",
        "snapshot_digest",
        "story_id",
        "story_keys_digest",
    }
    if (
        not isinstance(manifest, dict)
        or not isinstance(manifest.get("ids"), list)
        or not isinstance(manifest.get("items"), list)
        or not isinstance(manifest.get("digest"), str)
    ):
        raise ValueError("manifest must contain ids, evidence items, and digest")
    ids = manifest["ids"]
    items = manifest["items"]
    if (
        any(not isinstance(candidate_id, str) for candidate_id in ids)
        or ids != sorted(set(ids))
        or len(items) != len(ids)
        or any(
            not isinstance(item, dict)
            or set(item) != required_item_keys
            or any(not isinstance(value, str) for value in item.values())
            for item in items
        )
        or [item["id"] for item in items] != ids
        or V2Workflow.release_manifest_digest(
            cast(
                list[dict[str, str]],
                items,
            )
        )
        != manifest["digest"]
    ):
        raise ValueError("manifest ids, evidence items, and digest must be canonical and bound")
    with V2Workflow(args.db, mode="runtime") as workflow:
        print(json.dumps(workflow.release_held_candidates(ids, manifest["digest"]), ensure_ascii=False))
    return 0


def _settle_google_sheets_bootstrap_failure(
    workflow: V2Workflow,
    draft: V2Draft,
    error: Exception,
) -> V2Draft:
    """Terminally record a pre-dispatch bootstrap failure."""
    claim_detail = json.dumps(
        {
            "operation": "sheets_delivery",
            "phase": "bootstrap",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    if not workflow.claim_remote_effect(
        draft.id,
        "sheets_delivery",
        claim_detail,
    ):
        receipt = workflow.remote_effect(
            draft.id,
            "sheets_delivery",
        )
        if receipt is None or receipt["status"] not in {
            "ambiguous",
            "failed",
        }:
            raise V2WorkflowError("Sheets bootstrap failure could not claim or reconcile durable effect")
        return cast(
            V2Draft,
            workflow.mark_manual_review(
                draft.id,
                f"sheets_delivery bootstrap failed: {type(error).__name__}",
            ),
        )
    settled = workflow.settle_remote_effect_claim(
        draft.id,
        "sheets_delivery",
        claim_detail,
        "failed",
        detail=json.dumps(
            {
                "error": type(error).__name__,
                "failure": ("terminal_pre_dispatch_failure"),
                "phase": "bootstrap_failed",
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    if not settled:
        raise V2WorkflowError("Sheets bootstrap failure settlement lost its durable claim")
    return cast(
        V2Draft,
        workflow.mark_manual_review(
            draft.id,
            f"sheets_delivery bootstrap failed: {type(error).__name__}",
        ),
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
    with V2Workflow(args.db, mode="runtime") as workflow:
        result = _deliver_google_sheets_draft(workflow, workflow.get_draft(args.draft_id), args.deadline)
        print(json.dumps({"id": result.id, "state": result.state}))
    return 0


def deliver_google_sheets_next(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="runtime") as workflow:
        draft = workflow.next_draft_approved_sheets_delivery()
        if draft is None:
            print(json.dumps({"status": "no_work"}))
            return 0
        result = _deliver_google_sheets_draft(workflow, draft, args.deadline)
        print(json.dumps({"id": result.id, "state": result.state}))
    return 0


def seed_telegram_cursor(args: argparse.Namespace) -> int:
    with V2Workflow(args.db, mode="runtime") as workflow:
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
    with V2Workflow(args.db, mode="runtime") as workflow:
        offset = workflow.telegram_next_offset()
        if offset is None:
            raise V2WorkflowError("V2 Telegram cursor handoff is required")
        notifier = TelegramV2Notifier(
            adapter,
            candidate_evidence=workflow.candidate_evidence,
            deadline_seconds=args.deadline,
        )
        workflow.reconcile_expired_approval_capabilities(datetime.now(UTC).isoformat())
        live = V2LiveWorkflow(
            workflow,
            notify_candidate=cast(CandidateNotificationPort, notifier.candidate),
            notify_draft=cast(DraftNotificationPort, notifier.draft),
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

    live = commands.add_parser(
        "collect-live", help="collect configured Telegram handles using private V2 environment credentials"
    )
    live.add_argument("--lookback-hours", type=int, choices=range(1, 169), default=24)
    live.add_argument("--limit", type=int, choices=range(1, 501), default=100)
    live.set_defaults(handler=collect_live)

    fixture = commands.add_parser("collect-fixture")
    fixture.add_argument("--fixture", type=Path, required=True)
    fixture.set_defaults(handler=collect_fixture)
    validate = commands.add_parser("validate-selection", help="run V2 cutover validation without external sends")
    validate.add_argument("--no-send", action="store_true", required=True)
    validate.add_argument("--fixture", type=Path, required=True)
    validate.set_defaults(handler=validate_selection)

    view = commands.add_parser("v2-status", help="read one bounded keyset page of V2 status")
    view.add_argument("--limit", type=int, choices=range(1, 201), default=50)
    view.add_argument("--cursor")
    view.add_argument("--state", choices=[state.value for state in V2State])
    view.set_defaults(handler=status)

    create = commands.add_parser("create-db", help="create a new isolated V2 database")
    create.set_defaults(handler=create_db)
    runtime = commands.add_parser("runtime-db", help="verify that an existing V2 database opens for runtime")
    runtime.set_defaults(handler=runtime_db)
    migrate = commands.add_parser("migrate-db", help="atomically migrate an isolated V2 database")
    migrate.add_argument(
        "--backup",
        type=Path,
        required=True,
    )
    migrate.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
    )
    migrate.set_defaults(handler=migrate_db)
    verify = commands.add_parser("verify-db", help="read-only V2 schema and invariant verification")
    verify.set_defaults(handler=verify_db)
    compact = commands.add_parser("compact", help="compact one bounded V2 retention batch")
    compact.add_argument("--batch-size", type=int, choices=range(1, 501), default=500)
    compact.add_argument("--dry-run", action="store_true")
    compact.set_defaults(handler=compact_db)
    hold = commands.add_parser(
        "hold-backlog", help="hold notification-eligible candidates and print a release manifest"
    )
    hold.set_defaults(handler=hold_backlog)
    release = commands.add_parser("release-backlog", help="release only IDs in a reviewed held-backlog manifest")
    release.add_argument("--manifest", type=Path, required=True)
    release.set_defaults(handler=release_backlog)

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

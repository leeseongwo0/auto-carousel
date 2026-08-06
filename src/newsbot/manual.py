"""Private, local-only command handlers for the manual workflow."""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from .ai.fake import FakeGenerationProvider
from .ai.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider
from .collectors.base import Engagement, SourceObservation, UrlCandidate
from .collectors.telethon import TelethonCollector
from .config import BehaviorProfile, Capability, ChannelConfig, load_behavior_profile, validate_capabilities
from .exports import canonical_json_bytes
from .manual_storage import ManualStorage, ManualStorageError, default_manual_state_path
from .pipeline import NewsPipeline
from .runtime import FixtureClock, SystemClock
from .storage import DurableCollection, Storage, persist_observation

_MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
_MAX_RECORDS = 10_000
_MAX_STRING = 20_000


class _ProfileAdapter:
    """Supply the legacy ranking shape without expanding the profile API."""

    def __init__(self, profile: BehaviorProfile) -> None:
        self.digest = profile.digest
        self.policy = profile.policy
        if profile.news_policy is None:
            raise ValueError("manual behavior profile requires news policy")
        self.news_policy = profile.news_policy
        self.channels = tuple(
            ChannelConfig(
                source.id,
                source.name,
                source.telegram_handle or source.id,
                True,
                source.priority,
                source.source_quality,
                source.classification,
                source.official_domains,
                source.original_domains,
            )
            for source in profile.sources
        )
        self.channels_by_id = {channel.id: channel for channel in self.channels}


def _state_path(args: Any) -> Path:
    value = getattr(args, "state", None)
    return default_manual_state_path() if value is None else Path(value)


def _output(args: Any, descendant: str) -> ManualStorage:
    value = getattr(args, "output_dir", None)
    return ManualStorage.open_directory(_state_path(args) / descendant if value is None else value)


def _profile(args: Any) -> BehaviorProfile:
    return load_behavior_profile(args.profile)


def _open(args: Any, *, bind: bool) -> tuple[ManualStorage, Storage, BehaviorProfile]:
    profile = _profile(args)
    state = ManualStorage.open_directory(_state_path(args))
    try:
        reservation = state.reserve_database(args.database)
        storage = Storage.open(
            reservation.path,
            phase_guard=lambda phase: state.sqlite_phase(phase, args.database),
        )
        try:
            if bind:
                storage.bind_manual_profile(profile.schema, profile.digest)
            else:
                storage.require_manual_profile(profile.schema, profile.digest)
            return state, storage, profile
        except BaseException:
            storage.close()
            raise
    except BaseException:
        state.close()
        raise


def _close(state: ManualStorage, storage: Storage, args: Any) -> None:
    try:
        with state.private_umask():
            state.before_sqlite_phase(args.database)
            storage.close()
            state.after_sqlite_phase(args.database)
    finally:
        state.close()


class _NoNotificationApprovalService:
    """Suppress remote approval authority while preserving ranking behavior."""

    def create_digest(self, run_id: int, *, actor_id: int) -> None:
        del run_id, actor_id
        return None


def _redacted(**counts: object) -> int:
    print(json.dumps(counts, sort_keys=True))
    return 0


def _manual_markdown_bytes(payload: dict[str, Any]) -> bytes:
    cover = payload["cover"]
    lines = [f"# {cover['title']}", "", str(cover["subtitle"])]
    for index, body in enumerate(payload["bodies"], start=2):
        lines.extend(("", f"## {index}. {body['subtitle']}", "", str(body["body"])))
    caption = payload["caption"]
    lines.extend(("", "## Caption"))
    for key in ("hook", "context", "details", "implications", "questions"):
        lines.extend(("", str(caption[key])))
    hashtags = " ".join(str(value) for value in caption["hashtags"])
    if hashtags:
        lines.extend(("", hashtags))
    return ("\n".join(lines) + "\n").encode("utf-8")


def manual_init(args: Any) -> int:
    state, storage, profile = _open(args, bind=True)
    try:
        return _redacted(status="initialized", sources=len(profile.sources))
    finally:
        _close(state, storage, args)


def _import_observations(path: Path, profile: BehaviorProfile) -> tuple[SourceObservation, ...]:
    try:
        raw = ManualStorage.read_private_input(path, max_bytes=_MAX_DOCUMENT_BYTES)
    except ManualStorageError as error:
        raise ValueError("manual import document cannot be read") from error
    if len(raw) > _MAX_DOCUMENT_BYTES:
        raise ValueError("manual import document exceeds bounds")
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("manual import document is invalid") from error
    if not isinstance(document, dict) or document.get("schema") != "newsbot.manual.import.v1":
        raise ValueError("manual import document schema is invalid")
    records = document.get("records")
    if not isinstance(records, list) or len(records) > _MAX_RECORDS:
        raise ValueError("manual import record bounds are invalid")
    sources = {source.id: source for source in profile.sources}
    observations: list[SourceObservation] = []
    identities: set[tuple[str, str]] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) - {
            "source_id",
            "post_id",
            "published_at",
            "text",
            "urls",
            "views",
            "reactions",
            "forwards",
        }:
            raise ValueError("manual import record is invalid")
        source_id = record.get("source_id")
        post_id = record.get("post_id")
        published_at = record.get("published_at")
        text = record.get("text", "")
        if (
            source_id not in sources
            or not isinstance(post_id, str)
            or not isinstance(published_at, str)
            or not isinstance(text, str)
        ):
            raise ValueError("manual import source mapping is invalid")
        if (
            not post_id
            or len(post_id) > 128
            or len(text) > _MAX_STRING
            or any(len(value) > _MAX_STRING for value in (source_id, post_id, published_at))
        ):
            raise ValueError("manual import string bounds are invalid")
        try:
            when = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("manual import timestamp is invalid") from error
        if when.tzinfo is None:
            raise ValueError("manual import timestamp is invalid")
        urls = record.get("urls", [])
        if (
            not isinstance(urls, list)
            or len(urls) > 32
            or any(not isinstance(url, str) or len(url) > 2048 for url in urls)
            or any(urlsplit(url).scheme.lower() not in {"http", "https"} or not urlsplit(url).netloc for url in urls)
        ):
            raise ValueError("manual import URL bounds are invalid")
        identity = (source_id, post_id)
        if identity in identities:
            raise ValueError("manual import has duplicate source post")
        identities.add(identity)
        metrics = []
        for name in ("views", "reactions", "forwards"):
            value = record.get(name)
            if value is not None and (not isinstance(value, int) or value < 0 or value > 2**31 - 1):
                raise ValueError("manual import engagement is invalid")
            metrics.append(value)
        source = sources[source_id]
        observations.append(
            SourceObservation(
                source_id,
                source.telegram_handle or source_id,
                post_id,
                when.astimezone(UTC),
                text=text,
                urls=tuple(UrlCandidate(url) for url in urls),
                engagement=Engagement(*metrics),
            )
        )
    return tuple(observations)


def _run(storage: Storage, profile: BehaviorProfile, observations: tuple[SourceObservation, ...], key: str) -> int:
    pipeline = NewsPipeline(
        storage,
        _ProfileAdapter(profile),
        lambda: (_ for _ in ()).throw(RuntimeError("provider unavailable")),
        SystemClock(),
    )
    result = asyncio.run(
        pipeline.run(
            observations,
            run_key=key,
            approval_service=cast(Any, _NoNotificationApprovalService()),
            actor_id=0,
        )
    )
    return result.run_id


def manual_import(args: Any) -> int:
    profile = _profile(args)
    observations = _import_observations(args.input, profile)  # fully validate before opening mutable state
    state, storage, _ = _open(args, bind=False)
    try:
        with storage.transaction() as connection:
            for observation in observations:
                persist_observation(connection, observation, datetime.now(UTC))
        return _redacted(status="imported", records=len(observations))
    finally:
        _close(state, storage, args)


def manual_rank(args: Any) -> int:
    state, storage, profile = _open(args, bind=False)
    try:
        observations = storage.latest_observations()
        run_id = _run(
            storage,
            profile,
            observations,
            "manual-rank-"
            + profile.digest
            + "-"
            + sha256(
                b"".join(f"{item.channel_id}:{item.external_post_id}".encode() for item in observations)
            ).hexdigest(),
        )
        return _redacted(status="ranked", observations=len(observations), run_id=run_id)
    finally:
        _close(state, storage, args)


def _require_receipt(expected: str, actual: str, *, code: str) -> None:
    if len(expected) != 64 or expected != actual:
        raise ValueError(code)


def manual_candidates(args: Any) -> int:
    output = _output(args, "previews")
    state, storage, _ = _open(args, bind=False)
    try:
        payload, receipt = storage.manual_candidate_preview(args.run_id)
        raw = canonical_json_bytes(payload)
        filename = f"candidates-{args.run_id}-{receipt}.json"
        output.materialize(
            filename,
            raw,
            sha256=sha256(raw).hexdigest(),
        )
        return _redacted(
            status="listed",
            run_id=args.run_id,
            candidates=len(cast(list[object], payload["candidates"])),
            artifact_filename=filename,
            receipt=receipt,
        )
    finally:
        output.close()
        _close(state, storage, args)


def manual_candidate_decision(args: Any) -> int:
    state, storage, _ = _open(args, bind=False)
    try:
        decided_at = datetime.now(UTC).isoformat()
        result = storage.apply_manual_candidate_decision(
            args.run_id,
            args.candidate_id,
            args.decision,
            decided_at,
            args.expected_receipt,
        )
        return _redacted(
            status="selected" if result.decision == "select" else "rejected",
            candidate_id=result.candidate_id,
            generation_job_id=result.generation_job_id,
        )
    finally:
        _close(state, storage, args)


def _openai_provider() -> OpenAICompatibleProvider:
    return OpenAICompatibleProvider(
        OpenAICompatibleConfig(
            base_url=os.environ["OPENAI_BASE_URL"],
            api_key=os.environ["OPENAI_API_KEY"],
            model=os.environ["OPENAI_MODEL"],
            timeout_seconds=float(os.environ["OPENAI_TIMEOUT_SECONDS"]),
        )
    )


def manual_generate(args: Any) -> int:
    output = _output(args, "drafts")
    state, storage, profile = _open(args, bind=False)
    try:
        if not 1 <= args.page_count <= 8:
            raise ValueError("manual page count is invalid")
        provider: Any
        if args.provider == "fake":
            provider = FakeGenerationProvider
        else:
            validate_capabilities(Capability.GENERATE_OPENAI)
            provider = _openai_provider
        pipeline = NewsPipeline(
            storage, _ProfileAdapter(profile), provider, FixtureClock() if args.provider == "fake" else SystemClock()
        )
        result = asyncio.run(pipeline.generate_selected(args.candidate_id, page_count=args.page_count))
        row = storage.fetch_one(
            "SELECT content_json FROM generations WHERE id=? AND status='current'",
            (result.generation_id,),
        )
        if row is None:
            raise RuntimeError("manual generation commit is unavailable")
        raw = canonical_json_bytes(json.loads(str(row["content_json"])))
        output.materialize(f"draft-{result.generation_id}.json", raw, sha256=sha256(raw).hexdigest())
        return _redacted(
            status="generated", candidate_id=args.candidate_id, generation_id=result.generation_id, reused=result.reused
        )
    finally:
        output.close()
        _close(state, storage, args)


def manual_draft(args: Any) -> int:
    output = _output(args, "drafts")
    state, storage, _ = _open(args, bind=False)
    try:
        row = storage.fetch_one(
            "SELECT content_json FROM generations WHERE id=? AND status='current'", (args.generation_id,)
        )
        if row is None:
            raise ValueError("manual draft is not found")
        raw = canonical_json_bytes(json.loads(str(row["content_json"])))
        output.materialize(f"draft-{args.generation_id}.json", raw, sha256=sha256(raw).hexdigest())
        return _redacted(status="materialized", generation_id=args.generation_id)
    finally:
        output.close()
        _close(state, storage, args)


def manual_review(args: Any) -> int:
    state, storage, _ = _open(args, bind=False)
    try:
        decision = args.decision.replace("-", "_")
        row = storage.fetch_one(
            "SELECT content_json FROM generations WHERE id=? AND status='current'",
            (args.generation_id,),
        )
        if row is None:
            raise ValueError("manual review has no exact current generation")
        payload = json.loads(str(row["content_json"]))
        canonical_draft = canonical_json_bytes(payload)
        _require_receipt(
            args.expected_draft_digest,
            sha256(canonical_draft).hexdigest(),
            code="manual review draft receipt is stale",
        )
        canonical_json: bytes | None = canonical_draft
        prior = storage.fetch_one(
            "SELECT decision,decided_at FROM manual_local_decisions WHERE generation_id=?",
            (args.generation_id,),
        )
        decided_at = (
            str(prior["decided_at"])
            if prior is not None and str(prior["decision"]) == decision
            else datetime.now(UTC).isoformat()
        )
        canonical_markdown: bytes | None = None
        if decision == "approve_local":
            canonical_markdown = _manual_markdown_bytes(payload)
        else:
            canonical_json = None
        result = storage.apply_manual_review_decision(
            args.candidate_id,
            args.generation_id,
            decision,
            decided_at,
            canonical_json,
            canonical_markdown,
        )
        return _redacted(
            status=result.status,
            candidate_id=result.candidate_id,
            generation_id=result.generation_id,
            generation_job_id=result.generation_job_id,
            exports=len(result.exports),
        )
    finally:
        _close(state, storage, args)


def manual_export(args: Any) -> int:
    output = _output(args, "exports")
    state, storage, _ = _open(args, bind=False)
    try:
        rows = storage.fetch_all(
            "SELECT id,export_format,canonical_bytes,canonical_sha256,state FROM manual_local_export_outbox "
            "WHERE state IN ('ready','materialized') ORDER BY id"
        )
        for row in rows:
            suffix = "json" if row["export_format"] == "json" else "md"
            output.materialize(
                f"export-{int(row['id'])}.{suffix}", bytes(row["canonical_bytes"]), sha256=str(row["canonical_sha256"])
            )
            if str(row["state"]) == "ready":
                storage.mark_manual_local_export_materialized(int(row["id"]), datetime.now(UTC).isoformat())
        return _redacted(status="exported", exports=len(rows))
    finally:
        output.close()
        _close(state, storage, args)


def _count(storage: Storage, query: str, parameters: tuple[object, ...] = ()) -> int:
    row = storage.fetch_one(query, parameters)
    if row is None:
        raise RuntimeError("aggregate query returned no row")
    return int(row["n"])


def manual_status(args: Any) -> int:
    state, storage, _ = _open(args, bind=False)
    try:
        return _redacted(
            status="ready",
            observations=_count(storage, "SELECT COUNT(*) AS n FROM source_posts"),
            exports=_count(storage, "SELECT COUNT(*) AS n FROM manual_local_export_outbox"),
        )
    finally:
        _close(state, storage, args)


def manual_inspect(args: Any) -> int:
    state, storage, _ = _open(args, bind=False)
    try:
        if args.run_id is None and args.candidate_id is None and args.generation_id is None:
            raise ValueError("manual inspect requires an explicit ID")
        payload: dict[str, object] = {"status": "inspected"}
        if args.run_id is not None:
            payload["run_candidates"] = _count(
                storage,
                "SELECT COUNT(*) AS n FROM candidate_evaluations WHERE run_id=?",
                (args.run_id,),
            )
        if args.candidate_id is not None:
            row = storage.fetch_one("SELECT status,rank FROM candidates WHERE id=?", (args.candidate_id,))
            if row is None:
                raise ValueError("manual candidate is not found")
            payload.update(
                candidate_id=args.candidate_id,
                candidate_status=str(row["status"]),
                rank=int(row["rank"]) if row["rank"] is not None else None,
            )
        if args.generation_id is not None:
            row = storage.fetch_one("SELECT status FROM generations WHERE id=?", (args.generation_id,))
            if row is None:
                raise ValueError("manual generation is not found")
            payload.update(generation_id=args.generation_id, generation_status=str(row["status"]))
        return _redacted(**payload)
    finally:
        _close(state, storage, args)


def manual_collect_telethon(args: Any) -> int:
    if (
        not 1 <= args.lookback_hours <= 168
        or not 1 <= args.page_limit <= 100
        or not 1 <= args.max_pages <= 10
        or not 1 <= args.deadline_seconds <= 900
    ):
        raise ValueError("manual Telethon bounds are invalid")
    profile = _profile(args)
    channels = tuple(
        ChannelConfig(
            source.id,
            source.name,
            source.telegram_handle,
            True,
            source.priority,
            source.source_quality,
            source.classification,
            source.official_domains,
            source.original_domains,
        )
        for source in profile.sources
        if source.telegram_handle is not None
    )
    if not channels:
        raise ValueError("manual Telethon collection requires explicit public handles")
    validate_capabilities(Capability.LIVE_COLLECTION)
    state, storage, _ = _open(args, bind=False)
    collector: TelethonCollector | None = None
    loop = asyncio.new_event_loop()
    try:
        deadline_at = time.monotonic() + args.deadline_seconds
        collector = TelethonCollector(
            int(os.environ["TELEGRAM_API_ID"]),
            os.environ["TELEGRAM_API_HASH"],
            os.environ["TELEGRAM_SESSION_PATH"],
            deadline_at=deadline_at,
        )

        class _SynchronousTelethon:
            def latest_message_id(self, channel: object) -> int | None:
                latest = loop.run_until_complete(cast(Any, collector).latest_message_id(channel))
                return None if latest is None else int(latest)

            def collect(self, channel: object, **kwargs: Any) -> tuple[SourceObservation, ...]:
                return tuple(loop.run_until_complete(cast(Any, collector).collect(channel, **kwargs)))

        durable = DurableCollection(storage)
        now = datetime.now(UTC)
        persisted = 0
        for channel in channels:
            for _ in range(args.max_pages):
                if time.monotonic() >= deadline_at:
                    raise TimeoutError("manual Telethon collection deadline exhausted")
                result = durable.collect_channel(
                    _SynchronousTelethon(),
                    channel,
                    now=now,
                    page_size=args.page_limit,
                    initial_lookback=timedelta(hours=args.lookback_hours),
                    max_overlap_pages=1,
                    max_remote_pages=1,
                )
                persisted += result.persisted
                if result.cursor_promoted:
                    break
        return _redacted(status="collected", observations=persisted)
    finally:
        if collector is not None:
            loop.run_until_complete(collector.close())
        loop.close()
        _close(state, storage, args)

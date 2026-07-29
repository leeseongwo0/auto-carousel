"""Argparse entry point for local-first newsbot operations."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

from .ai.base import GenerationProvider
from .approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from .candidates import CandidateApprovalService, CandidateDigest
from .collectors.base import SourceObservation
from .collectors.fixture import FixtureCollector
from .config import AppConfig, Capability, ConfigError, load_config, validate_capabilities
from .exports import materialize_outbox, verify_ready_outbox
from .observability import inspect, status
from .pipeline import NewsPipeline, _draft_payload
from .runtime import FixtureClock, SystemClock
from .secrets import SessionStore, ensure_private_directory
from .storage import DurableCollection, Storage

CommandHandler = Callable[[argparse.Namespace], int]


def _path_from_environment(option: str, fallback: str) -> Path:
    return Path(os.environ.get(option, fallback))


def _config(args: argparse.Namespace) -> AppConfig:
    overrides: dict[str, Path] = {}
    if args.db is not None:
        overrides["database_path"] = args.db
    if getattr(args, "output", None) is not None:
        overrides["output_dir"] = args.output
    return load_config(args.config, cli_overrides=overrides)


def _fixture_provider_factory() -> GenerationProvider:
    """Import the fixture-only provider only after a scripted [제작] lease."""
    from .ai.fake import FakeGenerationProvider

    return FakeGenerationProvider()


def _adaptive_page_count(storage: Storage, candidate_id: int) -> int:
    from .copywriting import adaptive_page_count

    rows = storage.fetch_all(
        "SELECT version.body FROM candidate_sources sources "
        "JOIN source_post_versions version ON version.id=sources.source_post_version_id "
        "WHERE sources.candidate_id=? ORDER BY version.id",
        (candidate_id,),
    )
    return adaptive_page_count(str(row["body"]) for row in rows)


def _provider_factory(provider_name: str) -> Callable[[], GenerationProvider]:
    if provider_name == "fake":
        return _fixture_provider_factory
    if provider_name == "openai_compatible":

        def create_openai_provider() -> GenerationProvider:
            validate_capabilities(Capability.GENERATE_OPENAI)
            from .ai.openai_compatible import OpenAICompatibleConfig, OpenAICompatibleProvider

            return OpenAICompatibleProvider(
                OpenAICompatibleConfig(
                    base_url=os.environ["OPENAI_BASE_URL"],
                    api_key=os.environ["OPENAI_API_KEY"],
                    model=os.environ["OPENAI_MODEL"],
                    timeout_seconds=float(os.environ["OPENAI_TIMEOUT_SECONDS"]),
                )
            )

        return create_openai_provider
    raise ValueError(f"unsupported provider: {provider_name}")


def _database(args: argparse.Namespace) -> Path:
    return args.db if args.db is not None else _path_from_environment("NEWSBOT_DATABASE", "data/newsbot.sqlite")


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def init_db(args: argparse.Namespace) -> int:
    with Storage.open(_database(args)):
        pass
    _print({"database": str(_database(args)), "status": "initialized"})
    return 0


def run_fixture(args: argparse.Namespace) -> int:
    config = _config(args)
    clock = FixtureClock()
    fixture_path = args.fixture
    fixture_sha256 = sha256(Path(fixture_path).read_bytes()).hexdigest()
    run_key = (
        "fixture-input-"
        + sha256(
            json.dumps(
                {
                    "schema_version": "newsbot-fixture-input-v1",
                    "config_digest": config.digest,
                    "fixture_sha256": fixture_sha256,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
    )
    with Storage.open(config.database_path) as storage:
        terminal_run = storage.fetch_one(
            "SELECT id FROM runs WHERE run_key=? AND mode='fixture' AND status='ready'",
            (run_key,),
        )
        if terminal_run is not None:
            terminal = _ready_fixture_result(storage, int(terminal_run["id"]), config.output_dir)
            if terminal is not None:
                _print(terminal)
                return 0
        durable = DurableCollection(storage)
        for channel in config.enabled_channels:
            collector = FixtureCollector(fixture_path)
            collection = durable.collect_channel(collector, channel, now=clock.now())
            while not collection.interval_complete:
                collection = durable.collect_channel(collector, channel, now=clock.now())
        observations = storage.latest_observations()
        run = storage.fetch_one("SELECT id FROM runs WHERE run_key=?", (run_key,))
        if run is not None:
            ready = _ready_fixture_result(storage, int(run["id"]), config.output_dir)
            if ready is not None:
                _print(ready)
                return 0
        pipeline = NewsPipeline(storage, config, config.output_dir, _fixture_provider_factory, clock)
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        stage = asyncio.run(pipeline.run(observations, run_key=run_key, approval_service=service, actor_id=1))
        result: dict[str, Any] = {
            "candidate_count": len(stage.digest.candidates),
            "digest_id": stage.digest.id,
            "run_id": stage.run_id,
            "status": "pending_selection",
        }
        ready = _ready_fixture_result(storage, stage.run_id, config.output_dir)
        if ready is not None:
            result.update(ready)
            _print(result)
            return 0
        if args.scripted_approve:
            if not stage.digest.candidates:
                raise ValueError("fixture has no eligible candidate")
            candidate_id = int(stage.digest.candidates[0]["candidate_id"])
            adapter = ScriptedApprovalAdapter(service)
            make = next(button for button in stage.digest.buttons[candidate_id] if button.label == "[제작]")
            if adapter.apply(ScriptedAction(make.token, 1, 1)).status != "queued":
                raise RuntimeError("scripted selection was not queued")
            generated = asyncio.run(
                pipeline.generate_selected(
                    candidate_id, page_count=args.page_count or _adaptive_page_count(storage, candidate_id)
                )
            )
            _print(
                {
                    "candidate_id": candidate_id,
                    "draft": _draft_payload(generated.draft),
                    "generation_id": generated.generation_id,
                    "status": "pending_review",
                }
            )
            review = next(
                button
                for button in service.review_buttons(
                    candidate_id,
                    generated.generation_id,
                    actor_id=1,
                    source_version_ids=generated.source_version_ids,
                )
                if button.action.value == "approve_handoff"
            )
            if adapter.apply(ScriptedAction(review.token, 1, 1)).status != "approved":
                raise RuntimeError("scripted review was not approved")
            exported = pipeline.materialize_approved_export(generated.generation_id)
            result.update(
                {
                    "candidate_id": exported.candidate_id,
                    "export_id": exported.export.export_id,
                    "generation_id": exported.generation_id,
                    "json_path": str(exported.export.json_path),
                    "markdown_path": str(exported.export.markdown_path),
                    "reused": generated.reused,
                    "status": "ready",
                }
            )
    _print(result)
    return 0


def _ready_fixture_result(storage: Storage, run_id: int, output_dir: Path) -> dict[str, Any] | None:
    row = storage.fetch_one(
        "SELECT c.id AS candidate_id, g.id AS generation_id, json_outbox.export_id AS json_export_id, "
        "markdown_outbox.export_id AS markdown_export_id "
        "FROM runs r JOIN candidate_evaluations ce ON ce.run_id=r.id "
        "JOIN candidates c ON c.evaluation_id=ce.id "
        "JOIN selections s ON s.candidate_id=c.id "
        "JOIN generation_jobs j ON j.selection_id=s.id "
        "JOIN generations g ON g.generation_job_id=j.id "
        "JOIN export_outbox json_outbox ON json_outbox.digest_id=s.digest_id "
        "AND json_outbox.generation_id=g.id AND json_outbox.export_kind='json' AND json_outbox.status='ready' "
        "JOIN export_outbox markdown_outbox ON markdown_outbox.digest_id=s.digest_id "
        "AND markdown_outbox.generation_id=g.id AND markdown_outbox.export_kind='markdown' "
        "AND markdown_outbox.status='ready' "
        "WHERE r.id=? AND r.status='ready' AND g.status='current'",
        (run_id,),
    )
    if row is None:
        return None
    generation_id = int(row["generation_id"])
    pair = verify_ready_outbox(storage, output_dir, generation_id)
    if pair is None:
        return None
    if row["json_export_id"] != pair.export_id or row["markdown_export_id"] != pair.export_id:
        raise RuntimeError("ready fixture run has invalid export records")
    candidate_count = storage.fetch_one(
        "SELECT COUNT(*) AS count FROM candidates c "
        "JOIN candidate_evaluations ce ON ce.id=c.evaluation_id "
        "WHERE ce.run_id=? AND c.status!='rejected'",
        (run_id,),
    )
    digest = storage.fetch_one(
        "SELECT id FROM digests WHERE run_id=? ORDER BY id DESC LIMIT 1",
        (run_id,),
    )
    if candidate_count is None or digest is None:
        raise RuntimeError("ready fixture run is missing candidate or digest state")
    return {
        "candidate_count": int(candidate_count["count"]),
        "candidate_id": int(row["candidate_id"]),
        "digest_id": int(digest["id"]),
        "export_id": pair.export_id,
        "generation_id": generation_id,
        "json_path": str(pair.json_path),
        "markdown_path": str(pair.markdown_path),
        "reused": True,
        "run_id": run_id,
        "status": "ready",
    }


def show_status(args: argparse.Namespace) -> int:
    with Storage.open(_database(args)) as storage:
        _print(status(storage))
    return 0


def show_inspect(args: argparse.Namespace) -> int:
    with Storage.open(_database(args)) as storage:
        _print(inspect(storage, args.run_id))
    return 0


def repair_exports(args: argparse.Namespace) -> int:
    """Materialize each complete SQLite-authoritative export outbox pair."""
    output = args.output if args.output is not None else _path_from_environment("NEWSBOT_OUTPUT_DIR", "output")
    repaired = 0
    corrupt = 0
    with Storage.open(_database(args)) as storage:
        rows = storage.fetch_all(
            "SELECT generation_id, export_kind FROM export_outbox "
            "WHERE status IN ('pending', 'materializing', 'ready') ORDER BY generation_id, export_kind"
        )
        grouped: dict[int, set[str]] = {}
        for row in rows:
            grouped.setdefault(int(row["generation_id"]), set()).add(str(row["export_kind"]))
        for generation_id, kinds in grouped.items():
            if kinds != {"json", "markdown"}:
                with storage.transaction() as connection:
                    connection.execute(
                        "UPDATE export_outbox SET status='corrupt' WHERE generation_id=?",
                        (generation_id,),
                    )
                corrupt += 1
                continue
            try:
                materialize_outbox(storage, output, generation_id)
            except (FileExistsError, OSError, RuntimeError, ValueError):
                corrupt += 1
            else:
                repaired += 1
    _print({"corrupt": corrupt, "repaired": repaired})
    return 0


def auth_telethon(args: argparse.Namespace) -> int:
    """Authorize an owner-only local Telethon session."""
    validate_capabilities(Capability.AUTH_TELETHON)
    session_value = os.environ.get("TELEGRAM_SESSION_PATH")
    if not session_value:
        raise ConfigError("missing required environment variables: TELEGRAM_SESSION_PATH")
    session_path = Path(session_value)
    ensure_private_directory(session_path.parent)
    loop = asyncio.new_event_loop()
    collector, close = _live_collector(loop, session_path)
    try:
        cast(Any, collector).authenticate()
        SessionStore(session_path).validate()
    finally:
        try:
            close()
        finally:
            loop.close()
    _print({"session_path": str(session_path), "status": "authorized"})
    return 0


class _ExactRangeCollector:
    """Constrain durable reconciliation to an inclusive message-ID interval."""

    def __init__(self, collector: object, from_id: int, to_id: int) -> None:
        self._collector = collector
        self._from_id = from_id
        self._to_id = to_id

    def latest_message_id(self, channel: object) -> int:
        return self._to_id

    def collect(self, channel: object, **kwargs: Any) -> Sequence[SourceObservation]:
        requested_min = kwargs.pop("min_message_id", None)
        requested_max = kwargs.pop("max_message_id", None)
        kwargs["min_message_id"] = max(self._from_id - 1, requested_min or 0)
        kwargs["max_message_id"] = min(self._to_id, requested_max if requested_max is not None else self._to_id)
        return cast(Sequence[SourceObservation], cast(Any, self._collector).collect(channel, **kwargs))


def _reconcile_range(args: argparse.Namespace, *, required: bool = False) -> tuple[int, int] | None:
    has_range = args.from_id is not None or args.to_id is not None
    has_lookback = args.lookback_hours is not None
    if has_range and has_lookback:
        raise ValueError("message-ID bounds and --lookback-hours are mutually exclusive")
    if has_range:
        if args.from_id is None or args.to_id is None:
            raise ValueError("--from-id and --to-id must be provided together")
        if args.from_id < 1 or args.to_id < 1:
            raise ValueError("--from-id and --to-id must be positive")
        if args.from_id > args.to_id:
            raise ValueError("--from-id must not exceed --to-id")
        return args.from_id, args.to_id
    if has_lookback:
        if args.lookback_hours <= 0:
            raise ValueError("--lookback-hours must be positive")
        return None
    if required:
        raise ValueError("provide either --from-id with --to-id or --lookback-hours")
    return None


def rank(args: argparse.Namespace) -> int:
    config = _config(args)
    now = datetime.now(UTC)
    with Storage.open(config.database_path) as storage:
        observations = storage.latest_observations()
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=lambda: now)
        pipeline = DurableLivePipeline(storage, config, config.output_dir, _live_provider_forbidden, SystemClock())
        stage = asyncio.run(
            pipeline.run(
                observations, run_key=_live_run_key(observations, config), approval_service=service, actor_id=1
            )
        )
        digest_path = _write_pending_selection_digest(config.output_dir, stage.run_id, stage.digest)
    _print(
        {
            "candidate_count": len(stage.digest.candidates),
            "digest_id": stage.digest.id,
            "digest_path": str(digest_path),
            "mode": "rank",
            "run_id": stage.run_id,
            "status": "pending_selection",
        }
    )
    return 0


def reconcile_fixture(args: argparse.Namespace) -> int:
    """Perform bounded fixture recovery without advancing the normal cursor."""
    range_ids = _reconcile_range(args, required=True)
    config = _config(args)
    channel = next((item for item in config.enabled_channels if item.id == args.channel), None)
    if channel is None:
        raise ValueError("--channel must identify an enabled channel")
    clock = FixtureClock()
    now = clock.now()
    with Storage.open(config.database_path) as storage:
        collector: object = FixtureCollector(args.fixture)
        if range_ids is not None:
            collector = _ExactRangeCollector(collector, *range_ids)
            lower_bound = datetime.min.replace(tzinfo=UTC)
        else:
            lower_bound = now - timedelta(hours=args.lookback_hours)
        persisted = DurableCollection(storage).reconcile_channel(
            collector,
            channel,
            lower_bound=lower_bound,
            upper_bound=now,
            page_size=args.page_size,
            max_pages=args.max_pages,
            observed_at=now,
        )
        observations = storage.latest_observations()
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=clock.now)
        pipeline = DurableLivePipeline(storage, config, config.output_dir, _live_provider_forbidden, clock)
        stage = asyncio.run(
            pipeline.run(
                observations,
                run_key=_live_run_key(observations, config),
                approval_service=service,
                actor_id=1,
            )
        )
        digest_path = _write_pending_selection_digest(config.output_dir, stage.run_id, stage.digest)
    _print(
        {
            "channel": channel.id,
            "candidate_count": len(stage.digest.candidates),
            "digest_id": stage.digest.id,
            "digest_path": str(digest_path),
            "mode": "reconcile",
            "persisted": persisted,
            "run_id": stage.run_id,
            "status": "pending_selection",
        }
    )
    return 0


def _live_collector(loop: asyncio.AbstractEventLoop, session_path: Path) -> tuple[object, Callable[[], None]]:
    from .collectors.telethon import TelethonCollector

    collector = TelethonCollector(
        int(os.environ["TELEGRAM_API_ID"]),
        os.environ["TELEGRAM_API_HASH"],
        str(session_path),
    )

    class SynchronousTelethonScan:
        def latest_message_id(self, channel: object) -> int | None:
            return loop.run_until_complete(collector.latest_message_id(channel))

        def collect(
            self,
            channel: object,
            *,
            lower_bound: datetime | None = None,
            upper_bound: datetime | None = None,
            after: tuple[str, str] | None = None,
            min_message_id: int | None = None,
            max_message_id: int | None = None,
            limit: int | None = None,
        ) -> Sequence[SourceObservation]:
            return loop.run_until_complete(
                collector.collect(
                    channel,
                    lower_bound=lower_bound,
                    upper_bound=upper_bound,
                    after=after,
                    min_message_id=min_message_id,
                    max_message_id=max_message_id,
                    limit=limit,
                )
            )

        def authenticate(self) -> None:
            loop.run_until_complete(collector.authenticate())

    return SynchronousTelethonScan(), lambda: loop.run_until_complete(collector.close())


def _live_run_key(observations: Sequence[SourceObservation], config: AppConfig) -> str:
    payload = [
        {
            "channel_id": item.channel_id,
            "external_post_id": item.external_post_id,
            "published_at": item.published_at.isoformat(),
            "edited_at": item.edited_at.isoformat() if item.edited_at else None,
            "text": item.text,
            "kind": item.kind,
            "sponsored": item.sponsored,
            "urls": [(url.url, url.source, url.title, url.description) for url in item.urls],
            "media": [(media.kind, media.caption, media.identity, media.is_service) for media in item.media],
            "engagement": (item.engagement.views, item.engagement.reactions, item.engagement.forwards),
            "conflicts": item.conflicts,
        }
        for item in observations
    ]
    digest = sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return f"live-{config.digest}-{digest}"


def _write_pending_selection_digest(output_dir: Path, run_id: int, digest: CandidateDigest) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"pending-selection-{run_id}.json"
    candidates = digest.candidates
    path.write_text(
        json.dumps({"run_id": run_id, "candidates": candidates}, ensure_ascii=False, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    return path


def _live_provider_forbidden() -> GenerationProvider:
    raise RuntimeError("live collection must not construct a generation provider")


class DurableLivePipeline(NewsPipeline):
    """Rank directly from durable collector snapshots without copying revisions."""

    def _persist_sources(self, observations: Sequence[SourceObservation], now: datetime) -> dict[tuple[str, str], int]:
        source_ids: dict[tuple[str, str], int] = {}
        for observation in observations:
            row = self.storage.fetch_one(
                "SELECT observation.id FROM source_posts post "
                "JOIN source_post_observations observation ON observation.id=(SELECT MAX(current.id) "
                "FROM source_post_observations current WHERE current.source_post_id=post.id) "
                "WHERE post.channel_id=? AND post.external_post_id=?",
                (observation.channel_id, observation.external_post_id),
            )
            if row is None:
                raise RuntimeError("live ranking observation was not durably persisted")
            source_ids[(observation.channel_id, observation.external_post_id)] = int(row["id"])
        return source_ids


def _collect_live(args: argparse.Namespace, *, reconcile: bool) -> int:
    range_ids = _reconcile_range(args, required=True) if reconcile else None
    if args.page_size < 1 or args.max_pages < 1:
        raise ValueError("--page-size and --max-pages must be positive")
    validate_capabilities(Capability.LIVE_RECONCILE if reconcile else Capability.LIVE_COLLECTION)
    config = _config(args)
    channels = tuple(config.enabled_channels)
    if reconcile:
        channels = tuple(channel for channel in channels if channel.id == args.channel)
        if not channels:
            raise ValueError("--channel must identify an enabled channel")
    session_path = SessionStore(os.environ["TELEGRAM_SESSION_PATH"]).validate()
    loop = asyncio.new_event_loop()
    collector, close = _live_collector(loop, session_path)
    try:
        with Storage.open(config.database_path) as storage:
            durable = DurableCollection(storage)
            counts: dict[str, int] = {}
            channel_errors: dict[str, str] = {}
            for channel in channels:
                capture_time = datetime.now(UTC)
                try:
                    if reconcile:
                        bounded_collector = (
                            _ExactRangeCollector(collector, *range_ids) if range_ids is not None else collector
                        )
                        counts[channel.id] = durable.reconcile_channel(
                            bounded_collector,
                            channel,
                            lower_bound=datetime.min.replace(tzinfo=UTC)
                            if range_ids is not None
                            else capture_time - timedelta(hours=args.lookback_hours),
                            upper_bound=capture_time,
                            page_size=args.page_size,
                            max_pages=args.max_pages,
                            observed_at=capture_time,
                        )
                    else:
                        counts[channel.id] = durable.collect_channel(
                            collector,
                            channel,
                            now=capture_time,
                            page_size=args.page_size,
                            initial_lookback=timedelta(hours=args.lookback_hours),
                            max_overlap_pages=args.max_pages,
                        ).persisted
                except Exception as error:
                    channel_errors[channel.id] = f"{type(error).__name__}: {error}"
                    if reconcile:
                        raise
            now = datetime.now(UTC)
            observations = storage.latest_observations()
            service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=lambda: now)
            pipeline = DurableLivePipeline(storage, config, config.output_dir, _live_provider_forbidden, SystemClock())
            stage = loop.run_until_complete(
                pipeline.run(
                    observations,
                    run_key=_live_run_key(observations, config),
                    approval_service=service,
                    actor_id=1,
                )
            )
            digest_path = _write_pending_selection_digest(config.output_dir, stage.run_id, stage.digest)
    finally:
        try:
            close()
        finally:
            loop.close()
    _print(
        {
            "channels": counts,
            "channel_errors": channel_errors,
            "digest_id": stage.digest.id,
            "digest_path": str(digest_path),
            "pending_selection_digest_path": str(digest_path),
            "mode": "reconcile" if reconcile else "collect",
            "run_id": stage.run_id,
            "status": "pending_selection",
        }
    )
    return 0


def collect_live(args: argparse.Namespace) -> int:
    return _collect_live(args, reconcile=False)


def reconcile_live(args: argparse.Namespace) -> int:
    return _collect_live(args, reconcile=True)


def generate_pending(args: argparse.Namespace) -> int:
    if args.provider == "fake" and not args.fixture_only:
        raise ValueError("the fake provider is fixture-only; pass --fixture-only explicitly")
    config = _config(args)
    with Storage.open(config.database_path) as storage:
        clock = FixtureClock() if args.provider == "fake" else SystemClock()
        pipeline = NewsPipeline(storage, config, config.output_dir, _provider_factory(args.provider), clock)
        generated = asyncio.run(
            pipeline.generate_selected(
                args.candidate_id, page_count=args.page_count or _adaptive_page_count(storage, args.candidate_id)
            )
        )
    _print(
        {
            "candidate_id": generated.candidate_id,
            "generation_id": generated.generation_id,
            "page_count": generated.draft.page_count,
            "reused": generated.reused,
            "status": "pending_review",
        }
    )
    return 0


def _approval_service(storage: Storage) -> CandidateApprovalService:
    from .candidates import CandidateApprovalService

    chat_id = int(os.environ["NEWSBOT_APPROVER_CHAT_ID"])
    user_ids = {int(value.strip()) for value in os.environ["NEWSBOT_APPROVER_USER_IDS"].split(",") if value.strip()}
    if not user_ids:
        raise ConfigError("NEWSBOT_APPROVER_USER_IDS must contain at least one integer user id")
    clock = SystemClock()
    return CandidateApprovalService(storage, chat_id=chat_id, authorized_user_ids=user_ids, now=clock.now)


def notify_candidates(args: argparse.Namespace) -> int:
    validate_capabilities(Capability.NOTIFY_CANDIDATES)
    from .approval.telegram import TelegramApprovalAdapter

    with Storage.open(_database(args)) as storage:
        service = _approval_service(storage)
        digest = service.create_digest(args.run_id, actor_id=args.actor_id)
        TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service).send_candidate_digest(digest)
    _print({"candidate_count": len(digest.candidates), "digest_id": digest.id, "status": "sent"})
    return 0


def notify_review(args: argparse.Namespace) -> int:
    validate_capabilities(Capability.NOTIFY_CANDIDATES)
    from .approval.telegram import TelegramApprovalAdapter

    with Storage.open(_database(args)) as storage:
        row = storage.fetch_one(
            "SELECT g.content_json FROM generations g JOIN generation_jobs j ON j.id=g.generation_job_id "
            "JOIN selections s ON s.id=j.selection_id JOIN candidates c ON c.id=s.candidate_id "
            "WHERE g.id=? AND s.candidate_id=? AND c.status='pending_review' AND g.status='current'",
            (args.generation_id, args.candidate_id),
        )
        if row is None:
            raise ValueError("generation is not the current review draft for this candidate")
        sources = storage.fetch_all(
            "SELECT source_post_version_id FROM generation_sources WHERE generation_id=? ORDER BY source_post_version_id",
            (args.generation_id,),
        )
        source_version_ids = tuple(int(source["source_post_version_id"]) for source in sources)
        if not source_version_ids:
            raise ValueError("generation has no source revision binding")
        TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], _approval_service(storage)).send_review_draft(
            candidate_id=args.candidate_id,
            generation_id=args.generation_id,
            source_version_ids=source_version_ids,
            draft_text=json.dumps(json.loads(str(row["content_json"])), ensure_ascii=False, sort_keys=True),
            actor_id=args.actor_id,
        )
    _print({"candidate_id": args.candidate_id, "generation_id": args.generation_id, "status": "sent"})
    return 0


def _notify_resumed_approval(
    adapter: Any, storage: Storage, service: CandidateApprovalService, candidate_id: int, actor_id: int
) -> None:
    row = storage.fetch_one(
        "SELECT c.status, ce.run_id FROM candidates c JOIN candidate_evaluations ce ON ce.id=c.evaluation_id WHERE c.id=?",
        (candidate_id,),
    )
    if row is None:
        return
    if row["status"] == "pending_selection":
        digest = service.create_digest(int(row["run_id"]), actor_id=actor_id)
        candidate = next((item for item in digest.candidates if int(item["candidate_id"]) == candidate_id), None)
        if candidate is not None:
            adapter.send_candidate_digest(
                CandidateDigest(digest.id, digest.run_id, digest.revision, (candidate,), digest.buttons)
            )
        return
    if row["status"] != "pending_review":
        return
    generation = storage.fetch_one(
        "SELECT g.id, g.content_json FROM generations g JOIN generation_jobs j ON j.id=g.generation_job_id "
        "JOIN selections s ON s.id=j.selection_id WHERE s.candidate_id=? AND g.status='current' ORDER BY g.id DESC LIMIT 1",
        (candidate_id,),
    )
    if generation is None:
        return
    sources = storage.fetch_all(
        "SELECT source_post_version_id FROM generation_sources WHERE generation_id=? ORDER BY source_post_version_id",
        (int(generation["id"]),),
    )
    source_version_ids = tuple(int(source["source_post_version_id"]) for source in sources)
    if source_version_ids:
        adapter.send_review_draft(
            candidate_id=candidate_id,
            generation_id=int(generation["id"]),
            source_version_ids=source_version_ids,
            draft_text=json.dumps(json.loads(str(generation["content_json"])), ensure_ascii=False, sort_keys=True),
            actor_id=actor_id,
        )


def poll_approvals(args: argparse.Namespace) -> int:
    config: AppConfig | None = None
    capabilities: list[Capability] = [Capability.APPROVE_POLL]
    if args.process_generation:
        if args.provider is None:
            raise ValueError("--provider is required with --process-generation")
        if args.provider == "fake" and not args.fixture_only:
            raise ValueError("the fake provider is fixture-only; pass --fixture-only explicitly")
        config = _config(args)
        capabilities.append(Capability.GENERATE_FAKE if args.provider == "fake" else Capability.GENERATE_OPENAI)
    validate_capabilities(capabilities)
    from .approval.telegram import TelegramApprovalAdapter

    with Storage.open(_database(args)) as storage:
        service = _approval_service(storage)
        adapter = TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service)
        payload: dict[str, Any] = {"timeout": str(args.timeout)}
        if args.offset is not None:
            payload["offset"] = str(args.offset)
        response = adapter._request("getUpdates", payload)
        updates = response.get("result", [])
        if not isinstance(updates, list):
            raise RuntimeError("Telegram Bot API returned an invalid getUpdates result")
        statuses = [
            result
            for update in updates
            if isinstance(update, dict)
            if (result := adapter.handle_update(update)) is not None
        ]
        resumed = service.resume_due(SystemClock().now())
        for candidate_id in resumed:
            _notify_resumed_approval(adapter, storage, service, candidate_id, min(service.authorized_user_ids))
        generated: list[int] = []
        if args.process_generation:
            assert args.provider is not None
            assert config is not None
            candidate_rows = storage.fetch_all(
                "SELECT DISTINCT s.candidate_id FROM generation_jobs j JOIN selections s ON s.id=j.selection_id "
                "WHERE j.status IN ('queued', 'failed_recoverable') ORDER BY s.candidate_id"
            )
            clock = FixtureClock() if args.provider == "fake" else SystemClock()
            pipeline = NewsPipeline(storage, config, config.output_dir, _provider_factory(args.provider), clock)
            for row in candidate_rows:
                candidate_id = int(row["candidate_id"])
                generated.append(
                    asyncio.run(
                        pipeline.generate_selected(
                            candidate_id, page_count=args.page_count or _adaptive_page_count(storage, candidate_id)
                        )
                    ).generation_id
                )
    _print({"handled": len(statuses), "statuses": statuses, "generated": generated})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="newsbot", description="Local-first Telegram news digest workflow")
    commands = parser.add_subparsers(dest="command", required=True)

    init = commands.add_parser("init-db", help="create or migrate a local SQLite database")
    init.add_argument("--db", type=Path)
    init.set_defaults(handler=init_db)

    fixture = commands.add_parser("run-fixture", help="run the credential-free fixture pipeline")
    fixture.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    fixture.add_argument("--fixture", type=Path, required=True)
    fixture.add_argument("--db", type=Path)
    fixture.add_argument("--page-count", type=int, choices=range(1, 9))
    fixture.add_argument("--output", type=Path)
    fixture.add_argument(
        "--scripted-approve", action="store_true", help="apply scripted selection and review callbacks"
    )
    fixture.set_defaults(handler=run_fixture)

    status_command = commands.add_parser("status", help="show redacted local workflow counters")
    status_command.add_argument("--db", type=Path)
    status_command.set_defaults(handler=show_status)

    inspect_command = commands.add_parser("inspect", help="show a redacted run summary")
    inspect_command.add_argument("--db", type=Path)
    inspect_command.add_argument("--run-id", type=int, required=True)
    inspect_command.set_defaults(handler=show_inspect)

    repair = commands.add_parser("repair-exports", help="repair missing verified Markdown exports")
    repair.add_argument("--db", type=Path)
    repair.add_argument("--output", type=Path)
    repair.set_defaults(handler=repair_exports)
    auth = commands.add_parser("auth-telethon", help="interactively authorize an owner-only local Telethon session")
    auth.set_defaults(handler=auth_telethon)

    rank_command = commands.add_parser("rank", help="rank durable local observations without a provider")
    rank_command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    rank_command.add_argument("--db", type=Path)
    rank_command.add_argument("--output", type=Path)
    rank_command.set_defaults(handler=rank)

    fixture_reconcile = commands.add_parser("reconcile", help="perform bounded fixture recovery into local ranking")
    fixture_reconcile.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    fixture_reconcile.add_argument("--fixture", type=Path, required=True)
    fixture_reconcile.add_argument("--channel", required=True)
    fixture_reconcile.add_argument("--db", type=Path)
    fixture_reconcile.add_argument("--output", type=Path)
    fixture_range_mode = fixture_reconcile.add_mutually_exclusive_group()
    fixture_range_mode.add_argument("--lookback-hours", type=int)
    fixture_range_mode.add_argument("--from-id", type=int)
    fixture_reconcile.add_argument("--to-id", type=int)
    fixture_reconcile.add_argument("--page-size", type=int, default=100)
    fixture_reconcile.add_argument("--max-pages", type=int, default=10)
    fixture_reconcile.set_defaults(handler=reconcile_fixture)

    live = commands.add_parser("collect-live", help="collect a durable page from each configured Telethon channel")
    live.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    live.add_argument("--db", type=Path)
    live.add_argument("--output", type=Path)
    live.add_argument("--lookback-hours", type=int, default=24)
    live.add_argument("--page-size", type=int, default=100)
    live.add_argument("--max-pages", type=int, default=10)
    live.set_defaults(handler=collect_live)

    reconcile = commands.add_parser("reconcile-live", help="perform bounded durable Telethon reconciliation")
    reconcile.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    reconcile.add_argument("--db", type=Path)
    reconcile.add_argument("--output", type=Path)
    reconcile.add_argument("--channel", required=True)
    range_mode = reconcile.add_mutually_exclusive_group()
    range_mode.add_argument("--lookback-hours", type=int)
    range_mode.add_argument("--from-id", type=int)
    reconcile.add_argument("--to-id", type=int)
    reconcile.add_argument("--page-size", type=int, default=100)
    reconcile.add_argument("--max-pages", type=int, default=10)
    reconcile.set_defaults(handler=reconcile_live)

    generate = commands.add_parser("generate-pending", help="lease and generate one selected queued candidate")
    generate.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    generate.add_argument("--db", type=Path)
    generate.add_argument("--output", type=Path)
    generate.add_argument("--candidate-id", type=int, required=True)
    generate.add_argument("--provider", choices=("fake", "openai_compatible"), required=True)
    generate.add_argument("--fixture-only", action="store_true")
    generate.add_argument("--page-count", type=int, choices=range(1, 9))
    generate.set_defaults(handler=generate_pending)

    notify = commands.add_parser("notify-candidates", help="send candidate digest through the Telegram Bot API")
    notify.add_argument("--db", type=Path)
    notify.add_argument("--run-id", type=int, required=True)
    notify.add_argument("--actor-id", type=int, required=True)
    notify.set_defaults(handler=notify_candidates)

    review = commands.add_parser("notify-review", help="send the exact current draft and bound review callbacks")
    review.add_argument("--db", type=Path)
    review.add_argument("--candidate-id", type=int, required=True)
    review.add_argument("--generation-id", type=int, required=True)
    review.add_argument("--actor-id", type=int, required=True)
    review.set_defaults(handler=notify_review)

    poll = commands.add_parser("poll-approvals", help="poll Telegram callback updates once")
    poll.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    poll.add_argument("--db", type=Path)
    poll.add_argument("--output", type=Path)
    poll.add_argument("--offset", type=int)
    poll.add_argument("--timeout", type=int, choices=range(0, 51), default=0)
    poll.add_argument("--process-generation", action="store_true")
    poll.add_argument("--provider", choices=("fake", "openai_compatible"))
    poll.add_argument("--fixture-only", action="store_true")
    poll.add_argument("--page-count", type=int, choices=range(1, 9))
    poll.set_defaults(handler=poll_approvals)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        handler = cast(CommandHandler, args.handler)
        return handler(args)
    except (ConfigError, LookupError, OSError, RuntimeError, ValueError) as error:
        parser.error(str(error))
    return 2

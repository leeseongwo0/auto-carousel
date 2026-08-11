"""Argparse entry point for local-first newsbot operations."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import stat
import sys
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

from . import manual
from .ai.base import GenerationProvider
from .approval.base import hash_callback_token
from .approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from .candidates import CandidateApprovalService, CandidateDigest
from .collectors.base import SourceObservation
from .collectors.fixture import FixtureCollector
from .config import AppConfig, Capability, ConfigError, load_config, validate_capabilities
from .observability import inspect, status
from .pipeline import NewsPipeline, _draft_payload
from .runtime import FixtureClock, SystemClock
from .secrets import SecretFileError, SessionStore, ensure_private_directory, read_service_account_info
from .sheets.base import GoogleSheetsDeadlineExceeded, SheetsAdapter
from .storage import DurableCollection, Storage

CommandHandler = Callable[[argparse.Namespace], int]


def _path_from_environment(option: str, fallback: str) -> Path:
    return Path(os.environ.get(option, fallback))


def _config(args: argparse.Namespace) -> AppConfig:
    overrides: dict[str, Path] = {}
    if args.db is not None:
        overrides["database_path"] = args.db
    return load_config(getattr(args, "config", Path("config/channels.toml")), cli_overrides=overrides)


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


def _assert_legacy_database_authority(path: Path) -> None:
    """Read existing authority without creating or migrating the database."""
    if not path.exists():
        return
    connection = sqlite3.connect(path.resolve(strict=True).as_uri() + "?mode=ro", uri=True)
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='manual_profile_bindings'"
        ).fetchone()
        if (
            table is not None
            and connection.execute("SELECT 1 FROM manual_profile_bindings WHERE id=1").fetchone() is not None
        ):
            raise RuntimeError("manual profile authority conflicts with automation")
    finally:
        connection.close()


def _attested_runtime_release_digest(
    stable: Path = Path("/usr/local/bin/newsbot"),
    *,
    executing_prefix: Path | None = None,
) -> str:
    try:
        stable_metadata = stable.lstat()
        if not stat.S_ISLNK(stable_metadata.st_mode):
            raise RuntimeError("stable newsbot entrypoint is not an attested release link")
        entrypoint = stable.resolve(strict=True)
        release_root = entrypoint.parents[2]
        if entrypoint != release_root / "venv/bin/newsbot":
            raise RuntimeError("stable newsbot entrypoint has an invalid release layout")
        runtime_prefix = Path(sys.prefix) if executing_prefix is None else executing_prefix
        if runtime_prefix.resolve(strict=True) != release_root / "venv":
            raise RuntimeError("executing runtime does not match the stable release")
        manifest = release_root / "runtime-manifest.json"
        metadata = manifest.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
            raise RuntimeError("runtime manifest identity is invalid")
        payload = manifest.read_bytes()
        value = json.loads(payload)
        if not isinstance(value, dict) or value.get("version") != "newsbot-runtime-release-manifest-v1":
            raise RuntimeError("runtime manifest schema is invalid")
        if value.get("source_commit") != release_root.name:
            raise RuntimeError("runtime manifest release identity drifted")
    except (IndexError, OSError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError("runtime release attestation failed") from error
    return sha256(payload).hexdigest()


def _require_runtime_release_digest(expected: str) -> None:
    if len(expected) != 64 or any(character not in "0123456789abcdef" for character in expected):
        raise RuntimeError("release digest must be lowercase SHA-256")
    if _attested_runtime_release_digest() != expected:
        raise RuntimeError("runtime release digest does not match the stable entrypoint")


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str))


def init_db(args: argparse.Namespace) -> int:
    with Storage.open(_database(args)):
        pass
    _print({"database": str(_database(args)), "status": "initialized"})
    return 0


def run_fixture(args: argparse.Namespace) -> int:
    from .automation import automation_lock

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
    with automation_lock("collect"), Storage.open(config.database_path) as storage:
        if storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None:
            raise RuntimeError("fixture command is disabled after automation cutover")
        from .handoffs import SheetHandoffService
        from .sheets.schema import WORKPLACE_ORACLE_FINGERPRINT

        fixture_target_id = SheetHandoffService(storage).ensure_binding(
            binding_key="workplace",
            spreadsheet_id="fixture://workplace",
            sheet_id=0,
            now=clock.now().isoformat(),
            oracle_fingerprint=WORKPLACE_ORACLE_FINGERPRINT,
        )
        durable = DurableCollection(storage)
        for channel in config.enabled_channels:
            collector = FixtureCollector(fixture_path)
            collection = durable.collect_channel(collector, channel, now=clock.now())
            while not collection.interval_complete:
                collection = durable.collect_channel(collector, channel, now=clock.now())
        observations = storage.latest_observations()
        run = storage.fetch_one("SELECT id FROM runs WHERE run_key=?", (run_key,))
        if args.scripted_approve:
            existing = storage.fetch_one(
                "SELECT h.id AS handoff_id, h.generation_id, c.id AS candidate_id, d.id AS digest_id "
                "FROM sheet_handoffs h JOIN generations g ON g.id=h.generation_id "
                "JOIN generation_jobs j ON j.id=g.generation_job_id JOIN selections s ON s.id=j.selection_id "
                "JOIN candidates c ON c.id=s.candidate_id JOIN candidate_evaluations e ON e.id=c.evaluation_id "
                "JOIN digests d ON d.run_id=e.run_id WHERE e.run_id=? ORDER BY h.id DESC LIMIT 1",
                (int(run["id"]),) if run is not None else (-1,),
            )
            if existing is not None:
                assert run is not None
                candidate_count = storage.fetch_one(
                    "SELECT COUNT(*) AS count FROM candidates c "
                    "JOIN candidate_evaluations e ON e.id=c.evaluation_id "
                    "WHERE e.run_id=? AND c.rank IS NOT NULL",
                    (int(run["id"]),),
                )
                assert candidate_count is not None
                _print(
                    {
                        "candidate_count": int(candidate_count["count"]),
                        "candidate_id": int(existing["candidate_id"]),
                        "digest_id": int(existing["digest_id"]),
                        "generation_id": int(existing["generation_id"]),
                        "handoff_id": int(existing["handoff_id"]),
                        "reused": True,
                        "run_id": int(run["id"]),
                        "status": "approved",
                    }
                )
                return 0
        pipeline = NewsPipeline(storage, config, _fixture_provider_factory, clock)
        service = CandidateApprovalService(
            storage,
            chat_id=1,
            authorized_user_ids={1},
            now=clock.now,
            sheet_target_binding_id=fixture_target_id,
        )
        stage = asyncio.run(pipeline.run(observations, run_key=run_key, approval_service=service, actor_id=1))
        digest = stage.selection_digest
        result: dict[str, Any] = {
            "candidate_count": len(digest.candidates) if digest is not None else 0,
            "digest_id": digest.id if digest is not None else None,
            "routed_counts": {outcome.value: count for outcome, count in stage.routed_counts.items()},
            "run_id": stage.run_id,
            "status": "pending_selection" if digest is not None else "no_immediate_candidates",
        }
        if args.scripted_approve:
            if digest is None or not digest.candidates:
                raise ValueError("fixture has no immediate candidate")
            candidate_id = int(digest.candidates[0]["candidate_id"])
            adapter = ScriptedApprovalAdapter(service)
            make = next(button for button in digest.buttons[candidate_id] if button.label == "[제작]")
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
            handoff = storage.fetch_one(
                "SELECT id FROM sheet_handoffs WHERE generation_id=? ORDER BY id DESC LIMIT 1",
                (generated.generation_id,),
            )
            if handoff is None:
                raise RuntimeError("scripted approval did not create a Sheets handoff")
            result.update(
                {
                    "candidate_id": candidate_id,
                    "generation_id": generated.generation_id,
                    "handoff_id": int(handoff["id"]),
                    "reused": generated.reused,
                    "status": "approved",
                }
            )
    _print(result)
    return 0


def show_status(args: argparse.Namespace) -> int:
    with Storage.open(_database(args)) as storage:
        _print(status(storage))
    return 0


def show_inspect(args: argparse.Namespace) -> int:
    with Storage.open(_database(args)) as storage:
        _print(inspect(storage, args.run_id))
    return 0


def auth_telethon(args: argparse.Namespace) -> int:
    """Authorize an owner-only local Telethon session."""
    validate_capabilities(Capability.AUTH_TELETHON)
    from .automation import automation_lock

    with automation_lock("collect"):
        session_value = os.environ.get("TELEGRAM_SESSION_PATH")
        if not session_value:
            raise ConfigError("missing required environment variables: TELEGRAM_SESSION_PATH")
        session_path = Path(session_value)
        previous_umask = os.umask(0o077)
        try:
            ensure_private_directory(session_path.parent)
            loop = asyncio.new_event_loop()
            try:
                collector, close = _live_collector(loop, session_path)
                try:
                    cast(Any, collector).authenticate()
                    SessionStore(session_path).validate()
                finally:
                    close()
            finally:
                loop.close()
        finally:
            os.umask(previous_umask)
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
    from .automation import automation_lock

    config = _config(args)
    now = datetime.now(UTC)
    with automation_lock("collect"), Storage.open(config.database_path) as storage:
        if storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None:
            raise RuntimeError("legacy command is disabled after automation cutover")
        observations = storage.latest_observations()
        service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=lambda: now)
        pipeline = DurableLivePipeline(storage, config, _live_provider_forbidden, SystemClock())
        stage = asyncio.run(
            pipeline.run(
                observations, run_key=_live_run_key(observations, config), approval_service=service, actor_id=1
            )
        )
        _print(
            {
                "candidate_count": len(stage.selection_digest.candidates) if stage.selection_digest is not None else 0,
                "digest_id": stage.selection_digest.id if stage.selection_digest is not None else None,
                "mode": "rank",
                "routed_counts": {outcome.value: count for outcome, count in stage.routed_counts.items()},
                "run_id": stage.run_id,
                "status": "pending_selection" if stage.selection_digest is not None else "no_immediate_candidates",
            }
        )
    return 0


def reconcile_fixture(args: argparse.Namespace) -> int:
    """Perform bounded fixture recovery without advancing the normal cursor."""
    from .automation import automation_lock

    range_ids = _reconcile_range(args, required=True)
    config = _config(args)
    channel = next((item for item in config.enabled_channels if item.id == args.channel), None)
    if channel is None:
        raise ValueError("--channel must identify an enabled channel")
    clock = FixtureClock()
    now = clock.now()
    with automation_lock("collect"), Storage.open(config.database_path) as storage:
        if storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None:
            raise RuntimeError("legacy command is disabled after automation cutover")
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
        pipeline = DurableLivePipeline(storage, config, _live_provider_forbidden, clock)
        stage = asyncio.run(
            pipeline.run(
                observations,
                run_key=_live_run_key(observations, config),
                approval_service=service,
                actor_id=1,
            )
        )
        _print(
            {
                "channel": channel.id,
                "candidate_count": len(stage.selection_digest.candidates) if stage.selection_digest is not None else 0,
                "digest_id": stage.selection_digest.id if stage.selection_digest is not None else None,
                "mode": "reconcile",
                "persisted": persisted,
                "routed_counts": {outcome.value: count for outcome, count in stage.routed_counts.items()},
                "run_id": stage.run_id,
                "status": "pending_selection" if stage.selection_digest is not None else "no_immediate_candidates",
            }
        )
    return 0


def _live_collector(
    loop: asyncio.AbstractEventLoop,
    session_path: Path,
    *,
    deadline_at: float | None = None,
) -> tuple[object, Callable[[], None]]:
    from .collectors.telethon import TelethonCollector

    collector = TelethonCollector(
        int(os.environ["TELEGRAM_API_ID"]),
        os.environ["TELEGRAM_API_HASH"],
        str(session_path),
        deadline_at=deadline_at,
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


def _collect_live(
    args: argparse.Namespace,
    *,
    reconcile: bool,
    config: AppConfig | None = None,
) -> int:
    range_ids = _reconcile_range(args, required=True) if reconcile else None
    if args.page_size < 1 or args.max_pages < 1:
        raise ValueError("--page-size and --max-pages must be positive")
    deadline_at = time.monotonic() + float(getattr(args, "deadline", 24 * 60 * 60))
    validate_capabilities(Capability.LIVE_RECONCILE if reconcile else Capability.LIVE_COLLECTION)
    config = _config(args) if config is None else config
    channels = tuple(config.enabled_channels)
    if reconcile:
        channels = tuple(channel for channel in channels if channel.id == args.channel)
        if not channels:
            raise ValueError("--channel must identify an enabled channel")
    session_path = SessionStore(os.environ["TELEGRAM_SESSION_PATH"]).validate()
    loop = asyncio.new_event_loop()
    collector, close = _live_collector(loop, session_path, deadline_at=deadline_at)
    try:
        with Storage.open(config.database_path) as storage:
            durable = DurableCollection(storage)
            counts: dict[str, int] = {}
            channel_errors: dict[str, str] = {}
            for channel in channels:
                if time.monotonic() >= deadline_at:
                    raise TimeoutError("collection application deadline exhausted")
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
                    if time.monotonic() >= deadline_at:
                        raise TimeoutError("collection application deadline exhausted")
                except Exception as error:
                    channel_errors[channel.id] = f"{type(error).__name__}: {error}"
                    if reconcile:
                        raise
            if channel_errors and bool(getattr(args, "fail_on_channel_error", False)):
                failed = ", ".join(sorted(channel_errors))
                raise RuntimeError(f"automated collection failed for channels: {failed}")
            if time.monotonic() >= deadline_at:
                raise TimeoutError("collection application deadline exhausted")
            now = datetime.now(UTC)
            configured_channels = getattr(config, "channels", None)
            if configured_channels is None:
                configured_channel_ids = {
                    str(channel_id)
                    for channel_id, channel in getattr(config, "channels_by_id", {}).items()
                    if bool(getattr(channel, "enabled", True))
                }
            else:
                configured_channel_ids = {
                    str(channel.id) for channel in configured_channels if bool(getattr(channel, "enabled", True))
                }
            observations = tuple(
                observation
                for observation in storage.latest_observations()
                if observation.channel_id in configured_channel_ids
            )
            service = CandidateApprovalService(storage, chat_id=1, authorized_user_ids={1}, now=lambda: now)
            pipeline = DurableLivePipeline(storage, config, _live_provider_forbidden, SystemClock())
            stage = loop.run_until_complete(
                pipeline.run(
                    observations,
                    run_key=_live_run_key(observations, config),
                    approval_service=service,
                    actor_id=1,
                )
            )
    finally:
        try:
            close()
        finally:
            loop.close()
    _print(
        {
            "channels": counts,
            "channel_errors": channel_errors,
            "candidate_count": len(stage.selection_digest.candidates) if stage.selection_digest is not None else 0,
            "digest_id": stage.selection_digest.id if stage.selection_digest is not None else None,
            "mode": "reconcile" if reconcile else "collect",
            "routed_counts": {outcome.value: count for outcome, count in stage.routed_counts.items()},
            "run_id": stage.run_id,
            "status": "pending_selection" if stage.selection_digest is not None else "no_immediate_candidates",
        }
    )
    return 0


def _reject_legacy_when_automation_active(args: argparse.Namespace, *, database: Path | None = None) -> None:
    path = database or _database(args)
    if not path.exists():
        return
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='automation_cutovers'"
        ).fetchone()
        if (
            exists is not None
            and connection.execute("SELECT 1 FROM automation_cutovers WHERE id=1").fetchone() is not None
        ):
            raise RuntimeError("legacy command is disabled after automation cutover")
    finally:
        connection.close()


def _legacy_live_preflight(args: argparse.Namespace, *, reconcile: bool) -> AppConfig:
    config = _config(args)
    _reject_legacy_when_automation_active(args, database=config.database_path)
    validate_capabilities(Capability.LIVE_RECONCILE if reconcile else Capability.LIVE_COLLECTION)
    SessionStore(os.environ["TELEGRAM_SESSION_PATH"]).validate()
    return config


def collect_live(args: argparse.Namespace) -> int:
    from .automation import automation_lock

    with automation_lock("collect"):
        config = _legacy_live_preflight(args, reconcile=False)
        return _collect_live(args, reconcile=False, config=config)


def reconcile_live(args: argparse.Namespace) -> int:
    from .automation import automation_lock

    with automation_lock("collect"):
        config = _legacy_live_preflight(args, reconcile=True)
        return _collect_live(args, reconcile=True, config=config)


def generate_pending(args: argparse.Namespace) -> int:
    config = _config(args)
    _reject_legacy_when_automation_active(args, database=config.database_path)
    if args.provider == "fake" and not args.fixture_only:
        raise ValueError("the fake provider is fixture-only; pass --fixture-only explicitly")
    with Storage.open(config.database_path) as storage:
        clock = FixtureClock() if args.provider == "fake" else SystemClock()
        pipeline = NewsPipeline(storage, config, _provider_factory(args.provider), clock)
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
    spreadsheet_id = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    target_binding_id: int | None = None
    cutover_target = storage.fetch_one(
        "SELECT target.id,target.target_ref_sha256 FROM automation_cutovers cutover "
        "JOIN sheet_target_bindings target ON target.id=cutover.target_binding_id WHERE cutover.id=1"
    )
    if cutover_target is not None:
        if not spreadsheet_id or sha256(spreadsheet_id.encode()).hexdigest() != str(
            cutover_target["target_ref_sha256"]
        ):
            raise ConfigError("Google Sheets target does not match the active automation cutover")
        target_binding_id = int(cutover_target["id"])
    elif spreadsheet_id:
        target = storage.fetch_one(
            "SELECT id FROM sheet_target_bindings WHERE target_ref_sha256=?",
            (sha256(spreadsheet_id.encode()).hexdigest(),),
        )
        if target is None:
            raise ConfigError("Google Sheets target must be bootstrapped before approval")
        target_binding_id = int(target["id"])
    clock = SystemClock()
    return CandidateApprovalService(
        storage,
        chat_id=chat_id,
        authorized_user_ids=user_ids,
        now=clock.now,
        sheet_target_binding_id=target_binding_id,
    )


def _attest_codex_activation() -> str:
    credential = Path("/run/newsbot-codex-activation-v1")
    manifest = Path("/usr/local/lib/newsbot-codex-manifest-v1.json")
    try:
        credential_status = credential.lstat()
        manifest_status = manifest.lstat()
        raw = credential.read_bytes()
        value = json.loads(raw)
        cgroup = Path("/proc/self/cgroup").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigError("Codex service attestation unavailable") from exc
    if (
        not stat.S_ISREG(credential_status.st_mode)
        or credential_status.st_uid != 0
        or credential_status.st_nlink != 1
        or stat.S_IMODE(credential_status.st_mode) != 0o440
        or not stat.S_ISREG(manifest_status.st_mode)
        or manifest_status.st_uid != 0
        or manifest_status.st_nlink != 1
        or manifest_status.st_mode & 0o022
        or not isinstance(value, dict)
        or set(value) != {"activation", "manifest_sha256", "unit", "version"}
        or value["version"] != 1
        or value["unit"] not in {"newsbot-generate-codex.service", "newsbot-generate-codex-canary.service"}
        or not isinstance(value["activation"], str)
        or len(value["activation"]) != 32
        or any(character not in "0123456789abcdef" for character in value["activation"])
        or value["manifest_sha256"] != sha256(manifest.read_bytes()).hexdigest()
        or f"/{value['unit']}" not in cgroup
    ):
        raise ConfigError("Codex service attestation failed")
    return str(value["unit"])


def generate_codex_once(args: argparse.Namespace) -> int:
    """Systemd-only exact Codex activation; no user-controlled provider settings."""
    unit = _attest_codex_activation()
    if unit != "newsbot-generate-codex-canary.service":
        raise ConfigError("legacy Codex generation is canary-only")
    expected_db = Path("/var/lib/newsbot-canary/newsbot.db")
    if args.config != Path("/etc/newsbot/config.toml") or _database(args) != expected_db:
        raise ConfigError("Codex service paths are fixed")
    validate_capabilities(Capability.GENERATE_CODEX)
    config = _config(args)
    with Storage.open(config.database_path) as storage:
        pipeline = NewsPipeline(storage, config, _fixture_provider_factory, SystemClock())
        job_id = pipeline.select_codex_job_id()
        if job_id is None:
            _print({"status": "no_job"})
            return 0
        result = asyncio.run(pipeline.generate_codex_job_exact(job_id))
    _print({"status": "no_op" if result is None else "pending_review"})
    return 0


def generate_codex_v2_once(args: argparse.Namespace) -> int:
    """Systemd-only V2 Codex worker with durable request and attempt receipts."""
    unit = _attest_codex_activation()
    database = _database(args)
    if unit != "newsbot-generate-codex.service" or database != Path("/var/lib/newsbot-v2/newsbot-v2.sqlite"):
        raise ConfigError("V2 Codex service path and unit are fixed")
    validate_capabilities(Capability.GENERATE_CODEX)

    from .v2_codex import V2CodexWorker
    from .v2_workflow import V2Workflow

    with V2Workflow(database, mode="runtime") as workflow:
        interrupted = workflow.reconcile_interrupted_codex_requests()
        if interrupted:
            _print({"manual_review": interrupted, "status": "interrupted"})
            return 0
        draft = asyncio.run(V2CodexWorker(workflow).generate_next())
    _print({"status": "no_job" if draft is None else "pending_review"})
    return 0


def v2_status(args: argparse.Namespace) -> int:
    from .v2_cli import status

    return status(args)


def v2_collect_live(args: argparse.Namespace) -> int:
    from .v2_cli import collect_live

    return collect_live(args)


def v2_telegram_tick(args: argparse.Namespace) -> int:
    from .v2_cli import telegram_tick

    return telegram_tick(args)


def v2_deliver_google_sheets_next(args: argparse.Namespace) -> int:
    from .v2_cli import deliver_google_sheets_next

    return deliver_google_sheets_next(args)


def v2_seed_telegram_cursor(args: argparse.Namespace) -> int:
    from .v2_cli import seed_telegram_cursor

    return seed_telegram_cursor(args)


def _positive_actor(actor_id: int) -> int:
    if actor_id <= 0:
        raise ValueError("actor-id must be positive")
    return actor_id


def _control_operation(args: argparse.Namespace, action: str) -> int:
    actor_id = _positive_actor(args.actor_id)
    now = datetime.now(UTC).isoformat()
    payload: dict[str, object]
    with Storage.open(_database(args)) as storage, storage.transaction() as connection:
        control = connection.execute(
            "SELECT paused_at, pause_reason_code, control_version "
            "FROM generation_provider_controls WHERE provider_name='codex_cli'"
        ).fetchone()
        if control is None:
            raise ValueError("control conflict")
        latest = connection.execute(
            "SELECT id, operation_id, action, reason_code, actor_id, previous_control_version, "
            "resulting_control_version, resulting_paused "
            "FROM generation_provider_control_events WHERE provider_name='codex_cli' "
            "ORDER BY resulting_control_version DESC LIMIT 1"
        ).fetchone()
        is_replay = (
            latest is not None
            and str(latest["action"]) == action
            and latest["actor_id"] == actor_id
            and str(latest["reason_code"]) == args.reason_code
            and int(latest["previous_control_version"]) == args.expected_control_version
            and (
                (action == "pause" and control["paused_at"] is not None)
                or (action == "resume" and control["paused_at"] is None)
            )
            and int(latest["resulting_control_version"]) == int(control["control_version"])
        )
        if is_replay:
            operation_id = str(latest["operation_id"])
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM generation_job_retry_events "
                    "WHERE action='release' AND reason_code='provider_resumed' AND operation_id=?",
                    (operation_id,),
                ).fetchone()[0]
            )
            payload = {
                "affected_job_count": count,
                "changed": False,
                "event_id": int(latest["id"]),
                "operation_id": operation_id,
                "provider": "codex_cli",
                "resulting_control_version": int(latest["resulting_control_version"]),
                "status": "paused" if action == "pause" else "active",
            }
        else:
            if int(control["control_version"]) != args.expected_control_version:
                raise ValueError("control conflict")
            operation_id = "cxo_" + token_hex(16)
            if action == "pause":
                if control["paused_at"] is not None:
                    raise ValueError("control conflict")
                version = int(control["control_version"]) + 1
                connection.execute(
                    "UPDATE generation_provider_controls SET paused_at=?, pause_reason_code=?, resumed_at=NULL, "
                    "control_version=?, updated_at=? WHERE provider_name='codex_cli'",
                    (now, args.reason_code, version, now),
                )
                cursor = connection.execute(
                    "INSERT INTO generation_provider_control_events("
                    "operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, "
                    "previous_control_version, resulting_control_version, control_version"
                    ") VALUES (?, 'codex_cli', 'pause', ?, 'operator', ?, 1, ?, ?, ?)",
                    (operation_id, args.reason_code, actor_id, version - 1, version, version),
                )
                count = 0
                status_value = "paused"
            else:
                compatibility = {
                    "codex_auth_unavailable": "auth_restored",
                    "codex_runner_config": "config_repaired",
                    "codex_supervisor": "config_repaired",
                    "codex_unknown_exit": "config_repaired",
                    "codex_outer_timeout": "config_repaired",
                    "codex_runner_attestation": "attestation_passed",
                    "operator_security_hold": "security_reviewed",
                    "maintenance": "maintenance_complete",
                }
                pause_reason = str(control["pause_reason_code"])
                if control["paused_at"] is None or compatibility.get(pause_reason) != args.reason_code:
                    raise ValueError("control conflict")
                version = int(control["control_version"]) + 1
                connection.execute(
                    "UPDATE generation_provider_controls SET paused_at=NULL, pause_reason_code=NULL, resumed_at=?, "
                    "control_version=?, updated_at=? WHERE provider_name='codex_cli'",
                    (now, version, now),
                )
                cursor = connection.execute(
                    "INSERT INTO generation_provider_control_events("
                    "operation_id, provider_name, action, reason_code, actor_kind, actor_id, resulting_paused, "
                    "previous_control_version, resulting_control_version, control_version"
                    ") VALUES (?, 'codex_cli', 'resume', ?, 'operator', ?, 0, ?, ?, ?)",
                    (operation_id, args.reason_code, actor_id, version - 1, version, version),
                )
                rows = connection.execute(
                    "SELECT j.id, r.consecutive_failures, r.retry_version FROM generation_jobs j "
                    "JOIN generation_job_provider_bindings b "
                    "ON b.generation_job_id=j.id AND b.provider_name='codex_cli' "
                    "JOIN generation_job_retry_state r ON r.generation_job_id=j.id "
                    "WHERE j.status='failed_recoverable' AND j.retry_at IS NULL "
                    "AND r.blocked_by_control_version=? AND r.blocked_by_safe_code=? "
                    "ORDER BY j.id",
                    (version - 1, pause_reason),
                ).fetchall()
                for row in rows:
                    connection.execute(
                        "UPDATE generation_job_retry_state SET blocked_by_control_version=NULL, "
                        "blocked_by_safe_code=NULL, retry_version=retry_version+1, updated_at=? "
                        "WHERE generation_job_id=?",
                        (now, int(row["id"])),
                    )
                    connection.execute("UPDATE generation_jobs SET retry_at=? WHERE id=?", (now, int(row["id"])))
                    connection.execute(
                        "INSERT INTO generation_job_retry_events("
                        "generation_job_id, operation_id, action, reason_code, actor_kind, actor_id, "
                        "resulting_held, resulting_consecutive_failures, previous_retry_version, "
                        "resulting_retry_version, control_version"
                        ") VALUES (?, ?, 'release', 'provider_resumed', 'operator', ?, 0, ?, ?, ?, ?)",
                        (
                            int(row["id"]),
                            operation_id,
                            actor_id,
                            int(row["consecutive_failures"]),
                            int(row["retry_version"]),
                            int(row["retry_version"]) + 1,
                            version,
                        ),
                    )
                count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM generation_job_retry_events "
                        "WHERE action='release' AND reason_code='provider_resumed' AND operation_id=?",
                        (operation_id,),
                    ).fetchone()[0]
                )
                status_value = "active"
            event_id = cursor.lastrowid
            assert event_id is not None
            payload = {
                "affected_job_count": count,
                "changed": True,
                "event_id": int(event_id),
                "operation_id": operation_id,
                "provider": "codex_cli",
                "resulting_control_version": version,
                "status": status_value,
            }
    _print(payload)
    return 0


def codex_provider_pause(args: argparse.Namespace) -> int:
    return _control_operation(args, "pause")


def codex_provider_resume(args: argparse.Namespace) -> int:
    return _control_operation(args, "resume")


def _retry_mutation(args: argparse.Namespace, action: str) -> int:
    actor_id = _positive_actor(args.actor_id)
    now = datetime.now(UTC).isoformat()
    payload: dict[str, object]
    with Storage.open(_database(args)) as storage, storage.transaction() as connection:
        row = connection.execute(
            "SELECT r.consecutive_failures, r.retry_version, r.held_at, j.status "
            "FROM generation_job_retry_state r "
            "JOIN generation_jobs j ON j.id=r.generation_job_id "
            "JOIN generation_job_provider_bindings b "
            "ON b.generation_job_id=j.id AND b.provider_name='codex_cli' "
            "WHERE r.generation_job_id=? AND j.status IN ('queued','failed_recoverable','running')",
            (args.generation_job_id,),
        ).fetchone()
        if row is None:
            raise ValueError("retry conflict")
        already_applied = (action == "hold" and row["held_at"] is not None) or (
            action == "release" and row["held_at"] is None
        )
        if already_applied:
            latest = connection.execute(
                "SELECT id, action, reason_code, actor_id FROM generation_job_retry_events "
                "WHERE generation_job_id=? ORDER BY resulting_retry_version DESC LIMIT 1",
                (args.generation_job_id,),
            ).fetchone()
            if (
                latest is None
                or latest["action"] != action
                or latest["reason_code"] != args.reason_code
                or latest["actor_id"] != actor_id
            ):
                raise ValueError("retry conflict")
            payload = {
                "changed": False,
                "event_id": int(latest["id"]),
                "generation_job_id": args.generation_job_id,
                "status": "held" if action == "hold" else "released",
            }
        else:
            previous_version = int(row["retry_version"])
            version = previous_version + 1
            if action == "hold":
                failures = int(row["consecutive_failures"])
                connection.execute(
                    "UPDATE generation_job_retry_state SET held_at=?, hold_reason_code=?, retry_version=?, "
                    "updated_at=? WHERE generation_job_id=?",
                    (now, args.reason_code, version, now, args.generation_job_id),
                )
            else:
                failures = 0
                connection.execute(
                    "UPDATE generation_job_retry_state SET held_at=NULL, hold_reason_code=NULL, "
                    "consecutive_failures=0, retry_version=?, updated_at=? WHERE generation_job_id=?",
                    (version, now, args.generation_job_id),
                )
                control = connection.execute(
                    "SELECT paused_at FROM generation_provider_controls WHERE provider_name='codex_cli'"
                ).fetchone()
                if row["status"] == "failed_recoverable" and control is not None and control["paused_at"] is None:
                    connection.execute(
                        "UPDATE generation_jobs SET retry_at=? WHERE id=?",
                        (now, args.generation_job_id),
                    )
            cursor = connection.execute(
                "INSERT INTO generation_job_retry_events("
                "generation_job_id, action, reason_code, actor_kind, actor_id, resulting_held, "
                "resulting_consecutive_failures, previous_retry_version, resulting_retry_version"
                ") VALUES (?, ?, ?, 'operator', ?, ?, ?, ?, ?)",
                (
                    args.generation_job_id,
                    action,
                    args.reason_code,
                    actor_id,
                    1 if action == "hold" else 0,
                    failures,
                    previous_version,
                    version,
                ),
            )
            event_id = cursor.lastrowid
            assert event_id is not None
            payload = {
                "changed": True,
                "event_id": int(event_id),
                "generation_job_id": args.generation_job_id,
                "status": "held" if action == "hold" else "released",
            }
    _print(payload)
    return 0


def codex_job_hold(args: argparse.Namespace) -> int:
    return _retry_mutation(args, "hold")


def codex_job_release(args: argparse.Namespace) -> int:
    return _retry_mutation(args, "release")


def notify_candidates(args: argparse.Namespace) -> int:
    validate_capabilities(Capability.NOTIFY_CANDIDATES)
    from .approval.telegram import TelegramApprovalAdapter
    from .automation import automation_lock

    with automation_lock("telegram"), Storage.open(_database(args)) as storage:
        if storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None:
            raise RuntimeError("activated notification must use the fenced dispatcher")
        service = _approval_service(storage)
        digest = service.create_digest(args.run_id, actor_id=args.actor_id)
        TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service).send_candidate_digest(digest)
    _print({"candidate_count": len(digest.candidates), "digest_id": digest.id, "status": "sent"})
    return 0


def notify_review(args: argparse.Namespace) -> int:
    validate_capabilities(Capability.NOTIFY_CANDIDATES)
    from .approval.telegram import TelegramApprovalAdapter
    from .automation import automation_lock

    with automation_lock("telegram"), Storage.open(_database(args)) as storage:
        if storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None:
            raise RuntimeError("activated notification must use the fenced dispatcher")
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


def _approval_poll_offset(storage: Storage, explicit_offset: int | None) -> int | None:
    if explicit_offset is not None:
        return explicit_offset
    cursor = storage.fetch_one("SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'")
    return None if cursor is None else int(cursor["next_offset"])


def _advance_approval_poll_offset(storage: Storage, update_id: int) -> None:
    with storage.transaction() as connection:
        connection.execute(
            "INSERT INTO telegram_update_cursors(stream,next_offset) VALUES ('approval',?) "
            "ON CONFLICT(stream) DO UPDATE SET "
            "next_offset=MAX(telegram_update_cursors.next_offset,excluded.next_offset), "
            "updated_at=CURRENT_TIMESTAMP",
            (update_id + 1,),
        )


def _poll_approvals_unlocked(args: argparse.Namespace) -> int:
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
    _reject_legacy_when_automation_active(
        args,
        database=config.database_path if config is not None else None,
    )
    from .approval.telegram import TelegramApprovalAdapter

    with Storage.open(_database(args)) as storage:
        service = _approval_service(storage)
        adapter = TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service)
        payload: dict[str, Any] = {"timeout": str(args.timeout)}
        poll_offset = _approval_poll_offset(storage, args.offset)
        if poll_offset is not None:
            payload["offset"] = str(poll_offset)
        response = adapter._request("getUpdates", payload)
        updates = response.get("result", [])
        if not isinstance(updates, list):
            raise RuntimeError("Telegram Bot API returned an invalid getUpdates result")
        statuses: list[str] = []
        for update in updates:
            if not isinstance(update, dict):
                continue
            update_id = update.get("update_id")
            if not isinstance(update_id, int) or isinstance(update_id, bool) or update_id < 0:
                continue
            result = adapter.handle_update(update)
            if result is not None:
                statuses.append(result)
            _advance_approval_poll_offset(storage, update_id)
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
            pipeline = NewsPipeline(storage, config, _provider_factory(args.provider), clock)
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


def poll_approvals(args: argparse.Namespace) -> int:
    from .automation import automation_lock

    with automation_lock("telegram"):
        return _poll_approvals_unlocked(args)


def _require_sheets_worker_deadline(args: argparse.Namespace) -> None:
    deadline = getattr(args, "_sheets_deadline_monotonic", None)
    if deadline is None:
        return
    if time.monotonic() >= deadline:
        raise GoogleSheetsDeadlineExceeded("Sheets worker deadline exceeded")


def _live_sheets(args: argparse.Namespace) -> tuple[AppConfig, SheetsAdapter, str]:
    validate_capabilities(Capability.LIVE_SHEETS)
    config = _config(args)
    _require_sheets_worker_deadline(args)
    if config.google_service_account_file is None or config.google_sheets_spreadsheet_id is None:
        raise ConfigError("Google Sheets capability is incomplete")
    try:
        credential_info = read_service_account_info(config.google_service_account_file)
    except (OSError, SecretFileError) as error:
        raise ConfigError("Google Sheets credential file is invalid") from error
    _require_sheets_worker_deadline(args)
    try:
        from .sheets.google import GoogleSheetsAdapter

        adapter = GoogleSheetsAdapter.from_credentials(
            credential_info=credential_info,
            spreadsheet_id=config.google_sheets_spreadsheet_id,
            deadline_monotonic=getattr(args, "_sheets_deadline_monotonic", None),
        )
    except GoogleSheetsDeadlineExceeded:
        raise
    except (ImportError, RuntimeError, ValueError) as error:
        raise RuntimeError("Google Sheets capability is unavailable") from error
    return config, adapter, credential_info["client_email"]


def _request_sha256(body: object) -> str:
    return sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _telegram_markup_identity(
    storage: Storage, markup: dict[str, object] | None
) -> tuple[object | None, tuple[str, ...]]:
    if markup is None:
        return None, ()
    keyboard = markup.get("inline_keyboard")
    if not isinstance(keyboard, list):
        raise RuntimeError("Telegram markup is invalid")
    semantic_rows: list[list[dict[str, str]]] = []
    token_hashes: list[str] = []
    for row in keyboard:
        if not isinstance(row, list):
            raise RuntimeError("Telegram markup is invalid")
        semantic_row: list[dict[str, str]] = []
        for button in row:
            if not isinstance(button, dict):
                raise RuntimeError("Telegram markup is invalid")
            label = button.get("text")
            token = button.get("callback_data")
            if not isinstance(label, str) or not isinstance(token, str):
                raise RuntimeError("Telegram markup is invalid")
            token_hash = hash_callback_token(token)
            binding = storage.fetch_one(
                "SELECT action,payload_json FROM callback_tokens WHERE token=?",
                (token_hash,),
            )
            if binding is None:
                raise RuntimeError("Telegram callback binding drift")
            payload = json.loads(str(binding["payload_json"]))
            semantic_row.append(
                {
                    "text": label,
                    "action": str(binding["action"]),
                    "payload_sha256": _request_sha256(payload),
                }
            )
            token_hashes.append(token_hash)
        semantic_rows.append(semantic_row)
    return {"inline_keyboard": semantic_rows}, tuple(token_hashes)


def _live_target_binding(service: object, config: AppConfig, *, now: str, oracle_fingerprint: str) -> int:
    from .handoffs import SheetHandoffService

    assert isinstance(service, SheetHandoffService)
    assert config.google_sheets_spreadsheet_id is not None
    return service.ensure_binding(
        binding_key="workplace",
        spreadsheet_id=config.google_sheets_spreadsheet_id,
        sheet_id=0,
        now=now,
        oracle_fingerprint=oracle_fingerprint,
    )


def sheets_validate(args: argparse.Namespace) -> int:
    _, adapter, _ = _live_sheets(args)
    from .sheets.schema import delivery_metadata_value

    probe = adapter.probe(metadata_value=delivery_metadata_value("exp_" + "0" * 32, "0" * 64))
    if probe.safe_code is not None:
        raise RuntimeError("Google Sheets workplace validation failed")
    _print({"status": "valid"})
    return 0


def sheets_bootstrap(args: argparse.Namespace) -> int:
    from .automation import automation_lock

    config = _config(args)
    with automation_lock("sheets"), Storage.open(config.database_path) as storage:
        if storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None:
            raise RuntimeError("Sheets bootstrap is disabled after automation cutover")
        return _sheets_bootstrap_unlocked(args)


def _sheets_bootstrap_unlocked(args: argparse.Namespace) -> int:
    config, adapter, email = _live_sheets(args)
    from .handoffs import SheetHandoffService
    from .sheets.base import DeliveryOutcome, MetadataState, SafeCode
    from .sheets.schema import (
        WORKPLACE_ORACLE_FINGERPRINT,
        schema_metadata_value,
    )

    now = datetime.now(UTC).isoformat()
    expires_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    controls_fingerprint = _request_sha256(
        {
            "schema": WORKPLACE_ORACLE_FINGERPRINT,
            "controls": schema_metadata_value(),
        }
    )
    with Storage.open(config.database_path) as storage:
        service = SheetHandoffService(storage)
        target_id = _live_target_binding(service, config, now=now, oracle_fingerprint=WORKPLACE_ORACLE_FINGERPRINT)
        bootstrap_status = service.ensure_bootstrap(
            target_binding_id=target_id,
            marker_value=schema_metadata_value(),
            controls_fingerprint=controls_fingerprint,
        )
        if bootstrap_status == "ready":
            _print({"status": "ready", "reused": True})
            return 0
        lease = service.acquire_initial(
            None,
            operation_kind="bootstrap",
            target_binding_id=target_id,
            now=now,
            expires_at=expires_at,
        )
        if lease is None:
            _print({"status": "not_acquired"})
            return 0
        prepared = adapter.prepare_bootstrap(service_account_email=email)
        request = prepared.body
        request_sha = prepared.request_sha256
        has_requests = bool(request.get("requests"))
        preflight: Literal["exact", "absent", "conflict"] = "absent" if has_requests else "exact"
        if not service.record_preflight(lease, outcome=preflight, now=now):
            raise RuntimeError("sheet bootstrap lease was lost after preflight")
        if not has_requests:
            if not service.finish(lease, outcome="reused", now=now):
                raise RuntimeError("sheet bootstrap lease was lost before reuse settlement")
            _print({"status": "ready", "reused": True})
            return 0
        attestation = adapter.dispatch_credential_attestation()
        adapter.arm_prepared_dispatch()
        dispatch_at = datetime.now(UTC).isoformat()
        if not service.mark_possibly_sent(
            lease,
            request_sha256=request_sha,
            oracle_fingerprint=WORKPLACE_ORACLE_FINGERPRINT,
            controls_fingerprint=controls_fingerprint,
            credential_refreshed_at=attestation.refreshed_at,
            credential_expires_at=attestation.expires_at,
            credential_scope_ok=attestation.scope_ok,
            now=dispatch_at,
        ):
            raise RuntimeError("sheet bootstrap lease was lost before dispatch")
        result = adapter.dispatch_prepared_bootstrap(prepared)
        settled_at = datetime.now(UTC).isoformat()
        if result.metadata is not None:
            probe_outcome = cast(
                Literal["exact", "absent", "duplicate", "conflict", "unavailable"],
                {
                    MetadataState.EXACT: "exact",
                    MetadataState.ABSENT: "absent",
                    MetadataState.DUPLICATE: "duplicate",
                    MetadataState.CONFLICT: "conflict",
                }[result.metadata],
            )
            if not service.record_probe(lease, outcome=probe_outcome, now=settled_at):
                raise RuntimeError("sheet bootstrap probe evidence was lost")
        if result.outcome is DeliveryOutcome.APPLIED:
            if not service.finish(lease, outcome="applied", now=settled_at):
                raise RuntimeError("sheet bootstrap lease was lost before settlement")
            _print({"status": "ready", "reused": False})
        elif result.outcome is DeliveryOutcome.AMBIGUOUS:
            if not service.release_possibly_sent(lease, now=settled_at):
                raise RuntimeError("sheet bootstrap lease was lost before ambiguity release")
            _print({"status": "ambiguous"})
        elif result.metadata is not None:
            if not service.finish(lease, outcome="schema_conflict", now=settled_at):
                raise RuntimeError("sheet bootstrap lease was lost before conflict settlement")
            _print(
                {
                    "safe_code": result.safe_code.value if result.safe_code else None,
                    "status": "blocked",
                }
            )
        else:
            retryable = result.outcome is DeliveryOutcome.NOT_APPLIED
            if not service.settle_trusted_rejection(
                lease,
                retryable=retryable,
                safe_code=(result.safe_code.value if result.safe_code is not None else SafeCode.AMBIGUOUS.value),
                now=settled_at,
                retry_at=(
                    (
                        datetime.fromisoformat(settled_at) + timedelta(seconds=result.retry_after_seconds or 1)
                    ).isoformat()
                    if retryable
                    else None
                ),
            ):
                raise RuntimeError("sheet bootstrap rejection evidence was lost")
            _print(
                {
                    "safe_code": result.safe_code.value if result.safe_code else None,
                    "status": "retryable" if retryable else "blocked",
                }
            )
    return 0


def sheets_status(args: argparse.Namespace) -> int:
    config = _config(args)
    with Storage.open(config.database_path) as storage:
        row = storage.fetch_one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN finished_at IS NULL THEN 1 ELSE 0 END) AS open "
            "FROM sheet_remote_operations"
        )
        handoffs = storage.fetch_one(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN status='ambiguous' THEN 1 ELSE 0 END) AS ambiguous, "
            "SUM(CASE WHEN status='delivered' THEN 1 ELSE 0 END) AS delivered "
            "FROM sheet_handoffs"
        )
        assert row is not None and handoffs is not None
    _print(
        {
            "ambiguous_handoffs": int(handoffs["ambiguous"] or 0),
            "delivered_handoffs": int(handoffs["delivered"] or 0),
            "handoffs": int(handoffs["total"]),
            "open_operations": int(row["open"] or 0),
            "operations": int(row["total"]),
            "status": "ok",
        }
    )
    return 0


def _handoff_projection(storage: Storage, handoff_id: int) -> tuple[str, str, tuple[str, ...]]:
    from .sheets.schema import project_handoff

    row = storage.fetch_one(
        "SELECT canonical_bytes, canonical_sha256, category, approved_at FROM sheet_handoffs WHERE id=?",
        (handoff_id,),
    )
    if row is None:
        raise LookupError("unknown sheet handoff")
    try:
        payload = json.loads(bytes(row["canonical_bytes"]).decode("utf-8"))
        pages = payload["pages"]
        cover = pages[0]
        projected_pages = [(cover["title"], cover["subtitle"])] + [
            (page["subtitle"], page["body"]) for page in pages[1:]
        ]
        values = project_handoff(
            approved_date=_seoul_date(str(row["approved_at"])),
            page_count=len(pages),
            category=str(row["category"]),
            caption=payload["caption"]["text"],
            pages=projected_pages,
        )
        export_id = payload["export_id"]
    except (KeyError, TypeError, UnicodeDecodeError, ValueError) as error:
        raise ValueError("invalid immutable sheet handoff") from error
    if not isinstance(export_id, str) or payload.get("category") != row["category"]:
        raise ValueError("invalid immutable sheet handoff")
    return export_id, str(row["canonical_sha256"]), values


def _seoul_date(value: str) -> str:
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(ZoneInfo("Asia/Seoul")).date().isoformat()


def sheets_deliver(args: argparse.Namespace) -> int:
    from .automation import automation_lock

    if bool(getattr(args, "_automation_sheets_worker", False)):
        return _sheets_deliver_unlocked(args)
    with automation_lock("sheets"):
        return _sheets_deliver_unlocked(args)


def _sheets_deliver_unlocked(args: argparse.Namespace) -> int:
    _require_sheets_worker_deadline(args)
    if not bool(getattr(args, "_automation_sheets_worker", False)):
        _reject_legacy_when_automation_active(args)
    config, adapter, _ = _live_sheets(args)
    _require_sheets_worker_deadline(args)
    from .handoffs import OperationOutcome, SheetHandoffService
    from .sheets.base import DeliveryOutcome, MetadataState, SafeCode
    from .sheets.schema import (
        WORKPLACE_ORACLE_FINGERPRINT,
        schema_metadata_value,
    )

    now = datetime.now(UTC).isoformat()
    expires_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    controls_fingerprint = _request_sha256(
        {
            "schema": WORKPLACE_ORACLE_FINGERPRINT,
            "controls": schema_metadata_value(),
        }
    )
    with Storage.open(config.database_path) as storage:
        service = SheetHandoffService(storage)
        target_id = _live_target_binding(service, config, now=now, oracle_fingerprint=WORKPLACE_ORACLE_FINGERPRINT)
        export_id, payload_sha256, values = _handoff_projection(storage, args.handoff_id)
        lease = service.acquire_initial(
            args.handoff_id,
            operation_kind="delivery",
            target_binding_id=target_id,
            now=now,
            expires_at=expires_at,
        )
        if lease is None:
            _print({"status": "not_acquired"})
            return 0
        _require_sheets_worker_deadline(args)
        prepared = adapter.prepare_delivery(export_id=export_id, canonical_sha256=payload_sha256, values=values)
        request_sha = prepared.request_sha256
        preflight: Literal["exact", "absent", "conflict"] = (
            "exact"
            if prepared.metadata is MetadataState.EXACT
            else ("conflict" if prepared.metadata in {MetadataState.DUPLICATE, MetadataState.CONFLICT} else "absent")
        )
        if not service.record_preflight(lease, outcome=preflight, now=now):
            raise RuntimeError("sheet mutation lease was lost after preflight")
        if prepared.metadata is MetadataState.EXACT:
            if not service.finish(lease, outcome="reused", now=now):
                raise RuntimeError("sheet preflight lease was lost")
            _print({"status": "delivered", "reused": True})
            return 0
        if prepared.metadata in {MetadataState.DUPLICATE, MetadataState.CONFLICT}:
            outcome: OperationOutcome = (
                "duplicate_metadata" if prepared.metadata is MetadataState.DUPLICATE else "conflicting_metadata"
            )
            if not service.finish(lease, outcome=outcome, now=now):
                raise RuntimeError("sheet preflight lease was lost")
            _print({"safe_code": SafeCode.METADATA_CONFLICT.value, "status": "blocked"})
            return 0
        _require_sheets_worker_deadline(args)
        attestation = adapter.dispatch_credential_attestation()
        adapter.arm_prepared_dispatch()
        _require_sheets_worker_deadline(args)
        dispatch_at = datetime.now(UTC).isoformat()
        if not service.mark_possibly_sent(
            lease,
            request_sha256=request_sha,
            oracle_fingerprint=WORKPLACE_ORACLE_FINGERPRINT,
            controls_fingerprint=controls_fingerprint,
            credential_refreshed_at=attestation.refreshed_at,
            credential_expires_at=attestation.expires_at,
            credential_scope_ok=attestation.scope_ok,
            now=dispatch_at,
        ):
            raise RuntimeError("sheet mutation lease was lost before dispatch")
        result = adapter.dispatch_prepared(prepared)
        settled_at = datetime.now(UTC).isoformat()
        if result.metadata is not None:
            probe_outcome = cast(
                Literal["exact", "absent", "duplicate", "conflict", "unavailable"],
                {
                    MetadataState.EXACT: "exact",
                    MetadataState.ABSENT: "absent",
                    MetadataState.DUPLICATE: "duplicate",
                    MetadataState.CONFLICT: "conflict",
                }[result.metadata],
            )
            if not service.record_probe(lease, outcome=probe_outcome, now=settled_at):
                raise RuntimeError("sheet mutation probe evidence was lost")
        if result.outcome is DeliveryOutcome.APPLIED:
            if not service.finish(lease, outcome="applied", now=settled_at):
                raise RuntimeError("sheet mutation lease was lost before settlement")
            _print({"status": "delivered", "reused": False})
        elif result.outcome is DeliveryOutcome.AMBIGUOUS:
            if not service.release_possibly_sent(lease, now=settled_at):
                raise RuntimeError("sheet mutation lease was lost before ambiguity release")
            _print({"status": "ambiguous"})
        elif result.metadata is not None:
            outcome = "duplicate_metadata" if result.metadata is MetadataState.DUPLICATE else "conflicting_metadata"
            if not service.finish(lease, outcome=outcome, now=settled_at):
                raise RuntimeError("sheet mutation lease was lost before conflict settlement")
            _print(
                {
                    "safe_code": result.safe_code.value if result.safe_code else None,
                    "status": "blocked",
                }
            )
        else:
            retryable = result.outcome is DeliveryOutcome.NOT_APPLIED
            if not service.settle_trusted_rejection(
                lease,
                retryable=retryable,
                safe_code=(result.safe_code.value if result.safe_code is not None else SafeCode.AMBIGUOUS.value),
                now=settled_at,
                retry_at=(
                    (
                        datetime.fromisoformat(settled_at) + timedelta(seconds=result.retry_after_seconds or 1)
                    ).isoformat()
                    if retryable
                    else None
                ),
            ):
                raise RuntimeError("sheet mutation rejection evidence was lost")
            _print(
                {
                    "safe_code": result.safe_code.value if result.safe_code else None,
                    "status": "retryable" if retryable else "blocked",
                }
            )
    return 0


def sheets_retry_blocked(args: argparse.Namespace) -> int:
    config = _config(args)
    from .handoffs import SheetHandoffService

    now = datetime.now(UTC).isoformat()
    with Storage.open(config.database_path) as storage:
        corrected = SheetHandoffService(storage).retry_blocked(
            args.operation_id,
            now=now,
        )
    _print({"status": "retryable" if corrected else "not_corrected"})
    return 0


def sheets_reconcile(args: argparse.Namespace) -> int:
    config, adapter, email = _live_sheets(args)
    from .handoffs import OperationOutcome, SheetHandoffService
    from .sheets.base import MetadataState, SafeCode
    from .sheets.schema import delivery_metadata_value

    now = datetime.now(UTC).isoformat()
    expires_at = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
    with Storage.open(config.database_path) as storage:
        operation = storage.fetch_one(
            "SELECT o.id AS operation_id,o.operation_kind,"
            "o.last_fence_version AS expected_fence,"
            "h.canonical_bytes,h.canonical_sha256 "
            "FROM sheet_remote_operations o "
            "LEFT JOIN sheet_handoffs h ON h.id=o.handoff_id "
            "WHERE o.id=? AND o.operation_kind IN ('delivery','bootstrap')",
            (args.operation_id,),
        )
        if operation is None:
            raise LookupError("unknown sheet operation")
        export_id: str | None = None
        if operation["operation_kind"] == "delivery":
            try:
                export_id = json.loads(bytes(operation["canonical_bytes"]).decode("utf-8"))["export_id"]
            except (
                KeyError,
                TypeError,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as error:
                raise ValueError("invalid immutable sheet handoff") from error
            if not isinstance(export_id, str):
                raise ValueError("invalid immutable sheet handoff")
        service = SheetHandoffService(storage)
        lease = service.acquire_probe(
            args.operation_id,
            expected_fence=int(operation["expected_fence"]),
            now=now,
            expires_at=expires_at,
        )
        if lease is None:
            _print({"status": "not_acquired"})
            return 0
        if operation["operation_kind"] == "bootstrap":
            probe = adapter.probe_bootstrap(service_account_email=email)
        else:
            assert export_id is not None
            probe = adapter.probe(metadata_value=delivery_metadata_value(export_id, str(operation["canonical_sha256"])))
        settled_at = datetime.now(UTC).isoformat()
        if probe.safe_code is SafeCode.AMBIGUOUS:
            probe_outcome: Literal["exact", "absent", "duplicate", "conflict", "unavailable"] = "unavailable"
        elif probe.metadata is MetadataState.EXACT:
            probe_outcome = "exact"
        elif probe.metadata is MetadataState.DUPLICATE:
            probe_outcome = "duplicate"
        elif probe.metadata is MetadataState.CONFLICT:
            probe_outcome = "conflict"
        else:
            probe_outcome = "absent"
        if not service.record_probe(lease, outcome=probe_outcome, now=settled_at):
            raise RuntimeError("sheet probe lease was lost before observation")
        if probe_outcome == "exact":
            if not service.finish(lease, outcome="applied", now=settled_at):
                raise RuntimeError("sheet probe lease was lost before settlement")
            status = "ready" if operation["operation_kind"] == "bootstrap" else "delivered"
        elif probe_outcome in {"duplicate", "conflict"}:
            outcome: OperationOutcome
            if operation["operation_kind"] == "bootstrap":
                outcome = "schema_conflict"
            else:
                outcome = "duplicate_metadata" if probe_outcome == "duplicate" else "conflicting_metadata"
            if not service.finish(lease, outcome=outcome, now=settled_at):
                raise RuntimeError("sheet probe lease was lost before settlement")
            status = "blocked"
        else:
            unresolved: Literal["absent", "unavailable"] = "absent" if probe_outcome == "absent" else "unavailable"
            if not service.release_probe_unresolved(lease, outcome=unresolved, now=settled_at):
                raise RuntimeError("sheet probe lease was lost before release")
            status = "ambiguous"
    _print(
        {
            "safe_code": probe.safe_code.value if probe.safe_code is not None else None,
            "status": status,
        }
    )
    return 0


def automation_status(args: argparse.Namespace) -> int:
    """Emit aggregate-only automation health."""
    with Storage.open(_database(args)) as storage:
        from .observability import automation_status as aggregate

        _print(aggregate(storage))
    return 0


def automation_quiescence_check(args: argparse.Namespace) -> int:
    """Return a bounded redacted cutover quiescence assertion."""
    with Storage.open(_database(args)) as storage:
        from .automation import AutomationAuthority

        _print({"quiescent": AutomationAuthority(storage).quiescent()})
    return 0


def automation_notification_inspect(args: argparse.Namespace) -> int:
    """Inspect one notification without disclosing its identity or payload."""
    with Storage.open(_database(args)) as storage:
        row = storage.fetch_one(
            "SELECT outbox.state,"
            "EXISTS(SELECT 1 FROM automation_cutovers cutover WHERE cutover.id=outbox.cutover_id "
            "AND cutover.audience_binding_id=outbox.audience_binding_id) AS binding_match "
            "FROM telegram_notification_outbox outbox WHERE outbox.id=?",
            (args.intent_id,),
        )
        if row is None:
            raise LookupError("notification intent does not exist")
        events = storage.fetch_one(
            "SELECT COUNT(*) AS count FROM telegram_chunk_attempts attempt "
            "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
            "WHERE chunk.notification_id=?",
            (args.intent_id,),
        )
        assert events is not None
        _print(
            {
                "binding_match": bool(row["binding_match"]),
                "chunk_attempt_count": int(events["count"]),
                "manual_required": str(row["state"]) in {"ambiguous", "partial_manual_required"},
                "terminal": str(row["state"]) in {"sent", "canceled", "resolved_delivered", "resolved_abandoned"},
            }
        )
    return 0


def _callback_actor_id(authorized_user_ids: set[int] | frozenset[int] | None = None) -> int:
    raw_actor = os.environ.get("NEWSBOT_CALLBACK_ACTOR_ID", "").strip()
    try:
        actor_id = int(raw_actor)
    except ValueError as error:
        raise ConfigError("NEWSBOT_CALLBACK_ACTOR_ID must be a positive integer") from error
    if actor_id < 1:
        raise ConfigError("NEWSBOT_CALLBACK_ACTOR_ID must be a positive integer")
    if authorized_user_ids is not None and actor_id not in authorized_user_ids:
        raise ConfigError("NEWSBOT_CALLBACK_ACTOR_ID must be an authorized approver")
    return actor_id


def _runtime_audience(
    authority: object,
    adapter: object,
    *,
    require_active: bool = True,
    deadline: object | None = None,
) -> int:
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["NEWSBOT_APPROVER_CHAT_ID"].strip()
    actor = str(_callback_actor_id())
    users = tuple(value.strip() for value in os.environ["NEWSBOT_APPROVER_USER_IDS"].split(",") if value.strip())
    if not chat_id or not actor or not users:
        raise ConfigError("automation audience binding is incomplete")
    response = cast(Any, adapter)._request("getMe", {}, deadline=deadline)
    result = response.get("result")
    bot_id = result.get("id") if isinstance(result, dict) else None
    if not isinstance(bot_id, int) or isinstance(bot_id, bool) or bot_id < 1:
        raise RuntimeError("Telegram Bot API returned an invalid getMe result")
    from .automation import AutomationAuthority

    token_hmac, audience_hmac = AutomationAuthority.audience_hmac(token, chat_id, users, actor)
    bot_digest = sha256(str(bot_id).encode()).hexdigest()
    if require_active and not cast(Any, authority).validate_active_audience(
        bot_id_digest=bot_digest, token_hmac=token_hmac, audience_hmac=audience_hmac
    ):
        raise RuntimeError("automation audience binding drift")
    return int(
        cast(Any, authority).record_audience_binding(
            bot_id_digest=bot_digest,
            token_hmac=token_hmac,
            audience_hmac=audience_hmac,
            version=1,
        )
    )


def _require_production_cutover_baseline(storage: Storage) -> None:
    baseline = storage.fetch_one(
        "SELECT candidate.id AS candidate_id,generation.id AS generation_id,"
        "generation.status AS generation_status,"
        "1+json_array_length(json_extract(generation.content_json,'$.bodies')) AS page_count,"
        "handoff.id AS handoff_id,handoff.status AS handoff_status "
        "FROM candidates candidate "
        "JOIN selections selection ON selection.candidate_id=candidate.id "
        "JOIN generation_jobs job ON job.selection_id=selection.id "
        "JOIN generations generation ON generation.generation_job_id=job.id "
        "JOIN sheet_handoffs handoff ON handoff.generation_id=generation.id "
        "WHERE candidate.id=12 AND generation.id=1 AND handoff.id=1"
    )
    cursor = storage.fetch_one("SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'")
    sheets_history = storage.fetch_one("SELECT COUNT(*) AS count FROM sheet_remote_operations")
    if (
        baseline is None
        or int(baseline["candidate_id"]) != 12
        or int(baseline["generation_id"]) != 1
        or str(baseline["generation_status"]) != "current"
        or int(baseline["page_count"]) != 4
        or int(baseline["handoff_id"]) != 1
        or str(baseline["handoff_status"]) != "delivered"
        or cursor is None
        or int(cursor["next_offset"]) <= 0
        or sheets_history is None
        or int(sheets_history["count"]) <= 0
    ):
        raise RuntimeError("production cutover baseline does not match")


def automation_cutover_preview(args: argparse.Namespace) -> int:
    from .approval.telegram import TelegramApprovalAdapter
    from .automation import AutomationAuthority, CutoverProposal, Frontier, cutover_locks
    from .config import validate_automation_bindings

    validate_capabilities((Capability.LIVE_COLLECTION, Capability.NOTIFY_CANDIDATES, Capability.LIVE_SHEETS))
    config = _config(args)
    validate_automation_bindings(config)
    now = datetime.now(UTC)
    with cutover_locks():
        _require_runtime_release_digest(args.release_digest)
        loop = asyncio.new_event_loop()
        try:
            session = SessionStore(os.environ["TELEGRAM_SESSION_PATH"]).validate()
            collector, close = _live_collector(loop, session)
            try:
                with Storage.open(config.database_path) as storage:
                    _require_production_cutover_baseline(storage)
                    authority = AutomationAuthority(storage)
                    if not authority.quiescent():
                        raise RuntimeError("automation is not quiescent")

                    def persist_preview() -> None:
                        service = _approval_service(storage)
                        adapter = TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service)
                        response = adapter._request("getMe", {})
                        result = response.get("result")
                        bot_id = result.get("id") if isinstance(result, dict) else None
                        if not isinstance(bot_id, int) or isinstance(bot_id, bool) or bot_id < 1:
                            raise RuntimeError("Telegram Bot API returned an invalid getMe result")
                        token_hmac, audience_hmac = AutomationAuthority.audience_hmac(
                            os.environ["TELEGRAM_BOT_TOKEN"],
                            os.environ["NEWSBOT_APPROVER_CHAT_ID"].strip(),
                            tuple(v.strip() for v in os.environ["NEWSBOT_APPROVER_USER_IDS"].split(",") if v.strip()),
                            os.environ["NEWSBOT_CALLBACK_ACTOR_ID"].strip(),
                        )
                        bot_id_digest = sha256(str(bot_id).encode()).hexdigest()
                        audience_id = authority.record_audience_binding(
                            bot_id_digest=bot_id_digest,
                            token_hmac=token_hmac,
                            audience_hmac=audience_hmac,
                            version=authority.next_audience_version(bot_id_digest),
                        )
                        frontiers = tuple(
                            Frontier(
                                sha256(channel.id.encode()).hexdigest(),
                                int(cast(Any, collector).latest_message_id(channel) or 0),
                                now,
                            )
                            for channel in config.enabled_channels
                        )
                        spreadsheet_id = config.google_sheets_spreadsheet_id
                        if not spreadsheet_id:
                            raise RuntimeError("Sheets target is not configured")
                        target = storage.fetch_one(
                            "SELECT target.id,target.target_ref_sha256 FROM sheet_target_bindings target "
                            "JOIN sheet_bootstraps bootstrap ON bootstrap.target_binding_id=target.id "
                            "WHERE bootstrap.status='ready' AND target.target_ref_sha256=?",
                            (sha256(spreadsheet_id.encode()).hexdigest(),),
                        )
                        if target is None:
                            raise RuntimeError("configured Sheets target is not ready")
                        tables = ("candidates", "generation_jobs", "generations", "decision_events", "sheet_handoffs")
                        maxima_values: list[int] = []
                        for table in tables:
                            maximum = storage.fetch_one(f"SELECT COALESCE(MAX(id),0) AS value FROM {table}")
                            if maximum is None:
                                raise RuntimeError("automation baseline query failed")
                            maxima_values.append(int(maximum["value"]))
                        maxima = tuple(maxima_values)
                        cursor = storage.fetch_one(
                            "SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'"
                        )
                        offset = 0 if cursor is None else int(cursor["next_offset"])
                        receipt = authority.persist_proposal(
                            CutoverProposal(
                                args.proposal_id,
                                config.digest,
                                sha256(str(offset).encode()).hexdigest(),
                                sha256(b"quiescent").hexdigest(),
                                int(target["id"]),
                                str(target["target_ref_sha256"]),
                                args.release_digest,
                                sha256(str(audience_id).encode()).hexdigest(),
                                cast(tuple[int, int, int, int, int], maxima),
                                offset,
                                frontiers,
                            ),
                            now=now,
                        )
                        _print({"proposal_sha256": receipt, "status": "previewed"})

                    persist_preview()
            finally:
                close()
        finally:
            loop.close()
    return 0


def automation_cutover_apply(args: argparse.Namespace) -> int:
    from .approval.telegram import TelegramApprovalAdapter
    from .automation import AutomationAuthority, cutover_locks
    from .config import validate_automation_bindings

    validate_capabilities((Capability.NOTIFY_CANDIDATES, Capability.LIVE_SHEETS))
    config = _config(args)
    validate_automation_bindings(config)

    def apply_locked(storage: Storage) -> None:
        authority = AutomationAuthority(storage)
        service = _approval_service(storage)
        audience_id = _runtime_audience(
            authority, TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service), require_active=False
        )
        expected_frontiers = {sha256(channel.id.encode()).hexdigest() for channel in config.enabled_channels}

        def validate_snapshot() -> bool:
            proposal = storage.fetch_one(
                "SELECT config_digest,cursor_digest,intervals_digest,ready_target_fingerprint "
                "FROM automation_cutover_proposals WHERE id=?",
                (args.proposal_id,),
            )
            frontiers = storage.fetch_all(
                "SELECT channel_key_digest FROM automation_proposal_frontiers WHERE proposal_id=?",
                (args.proposal_id,),
            )
            cursor = storage.fetch_one("SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'")
            offset = 0 if cursor is None else int(cursor["next_offset"])
            return bool(
                authority.quiescent()
                and proposal is not None
                and config.google_sheets_spreadsheet_id is not None
                and str(proposal["ready_target_fingerprint"])
                == sha256(config.google_sheets_spreadsheet_id.encode()).hexdigest()
                and str(proposal["config_digest"]) == config.digest
                and str(proposal["cursor_digest"]) == sha256(str(offset).encode()).hexdigest()
                and str(proposal["intervals_digest"]) == sha256(b"quiescent").hexdigest()
                and {str(row["channel_key_digest"]) for row in frontiers} == expected_frontiers
            )

        result = authority.apply_proposal(
            args.proposal_id,
            args.proposal_sha256,
            audience_binding_id=audience_id,
            release_digest=args.release_digest,
            config=config,
            now=datetime.now(UTC),
            validate=validate_snapshot,
        )
        _print({"changed": bool(result["changed"]), "status": str(result["status"])})

    with cutover_locks():
        _require_runtime_release_digest(args.release_digest)
        with Storage.open(_database(args)) as storage:
            apply_locked(storage)
    return 0


def automation_release_activate(args: argparse.Namespace) -> int:
    from .automation import AutomationAuthority, cutover_locks

    def activate_locked(storage: Storage) -> None:
        authority = AutomationAuthority(storage)
        config = _config(args)
        result = authority.activate_release(
            args.release_digest,
            config=config,
            now=datetime.now(UTC),
            validate=authority.quiescent,
        )
        _print(
            {
                "activation_id": cast(int, result["activation_id"]),
                "changed": bool(result["changed"]),
                "status": str(result["status"]),
            }
        )

    with cutover_locks():
        _require_runtime_release_digest(args.release_digest)
        with Storage.open(_database(args)) as storage:
            activate_locked(storage)
    return 0


def automation_collect_once(args: argparse.Namespace) -> int:
    from .automation import AutomationAuthority, automation_lock

    validate_capabilities(Capability.LIVE_COLLECTION)
    with automation_lock("collect"), Storage.open(_database(args)) as storage:
        authority = AutomationAuthority(storage)
        config = _config(args)
        configured_frontiers = tuple(sha256(channel.id.encode()).hexdigest() for channel in config.enabled_channels)
        active_cutover = storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1")
        active_frontiers = tuple(frontier.channel_key_digest for frontier in authority.active_frontiers())
        if active_cutover is None:
            raise RuntimeError("automated collection requires an active cutover")
        if (
            len(configured_frontiers) != 6
            or len(set(configured_frontiers)) != 6
            or tuple(sorted(configured_frontiers)) != active_frontiers
        ):
            raise RuntimeError("automated collection configuration drifted from the active cutover")
        with storage.transaction() as connection:
            if authority.validate_active_config_binding(connection, config) == 0:
                raise RuntimeError("automated collection requires a release/config binding")
        lease = authority.acquire_lease(
            "collect",
            now=datetime.now(UTC),
            lease_seconds=int(getattr(args, "lease_seconds", 225)),
        )
        outcome = "failed"
        try:
            result = _collect_live(
                argparse.Namespace(**vars(args), fail_on_channel_error=True),
                reconcile=False,
                config=config,
            )
            outcome = "done"
            return result
        finally:
            authority.release_lease(lease, now=datetime.now(UTC), outcome=outcome)


def _notification_payload(
    storage: Storage,
    service: CandidateApprovalService,
    notification_id: int,
    *,
    actor_id: int,
) -> tuple[str, dict[str, object] | None]:
    from .approval.telegram import format_review_draft

    row = storage.fetch_one(
        "SELECT notification_kind,candidate_id,generation_id,defer_authority_id,ambiguous_window_id,subject_digest "
        "FROM telegram_notification_outbox WHERE id=?",
        (notification_id,),
    )
    if row is None:
        raise RuntimeError("notification disappeared")
    if row["notification_kind"] == "noon_digest":
        window = storage.fetch_one(
            "SELECT window.id,window.config_binding_id FROM ambiguous_digest_windows window "
            "JOIN telegram_notification_outbox outbox ON outbox.ambiguous_window_id=window.id "
            "WHERE outbox.id=? AND window.state='queued'",
            (notification_id,),
        )
        active_binding = storage.fetch_one(
            "SELECT binding.id FROM automation_release_activations activation "
            "JOIN automation_release_config_bindings binding ON binding.activation_id=activation.id "
            "WHERE activation.cutover_id=1 ORDER BY activation.id DESC LIMIT 1"
        )
        if (
            window is None
            or active_binding is None
            or int(window["config_binding_id"]) != int(active_binding["id"])
            or str(row["subject_digest"]) != sha256(f"noon:{int(window['id'])}".encode()).hexdigest()
        ):
            raise RuntimeError("noon notification binding drift")
        items = storage.fetch_all(
            "SELECT normalized_title FROM ambiguous_digest_items WHERE window_id=? ORDER BY ordering_timestamp,id",
            (int(window["id"]),),
        )
        if not items:
            raise RuntimeError("noon notification has no frozen titles")
        return "\n".join(str(item["normalized_title"]) for item in items), None
    if row["notification_kind"] == "candidate":
        candidate = storage.fetch_one(
            "SELECT candidate.id,candidate_evaluations.run_id FROM candidates candidate "
            "JOIN candidate_evaluations ON candidate_evaluations.id=candidate.evaluation_id WHERE candidate.id=?",
            (int(row["candidate_id"]),),
        )
        if candidate is None:
            raise RuntimeError("candidate notification binding drift")
        digest = service.create_digest(int(candidate["run_id"]), actor_id=actor_id)
        item = next(
            (value for value in digest.candidates if int(value["candidate_id"]) == int(row["candidate_id"])), None
        )
        if item is None:
            raise RuntimeError("candidate notification is no longer eligible")
        buttons = digest.buttons[int(row["candidate_id"])]
        return (
            f"제목: {item['title']}\n출처: {item['source_url']}",
            {"inline_keyboard": [[{"text": button.label, "callback_data": button.token}] for button in buttons]},
        )
    if row["notification_kind"] == "review":
        generation = storage.fetch_one(
            "SELECT generation.id,generation.content_json,selection.candidate_id FROM generations generation "
            "JOIN generation_jobs job ON job.id=generation.generation_job_id "
            "JOIN selections selection ON selection.id=job.selection_id "
            "WHERE generation.id=? AND generation.status='current'",
            (int(row["generation_id"]),),
        )
        if generation is None:
            raise RuntimeError("review notification binding drift")
        sources = storage.fetch_all(
            "SELECT source_post_version_id FROM generation_sources WHERE generation_id=? ORDER BY source_post_version_id",
            (int(generation["id"]),),
        )
        source_version_ids = tuple(int(source["source_post_version_id"]) for source in sources)
        if not source_version_ids:
            raise RuntimeError("review notification has no source binding")
        buttons = service.review_buttons(
            int(generation["candidate_id"]),
            int(generation["id"]),
            actor_id=actor_id,
            source_version_ids=source_version_ids,
        )
        return (
            format_review_draft(str(generation["content_json"])),
            {"inline_keyboard": [[{"text": button.label, "callback_data": button.token}] for button in buttons]},
        )
    authority = storage.fetch_one(
        "SELECT defer.candidate_id,defer.stage FROM automation_defer_authority defer "
        "WHERE defer.id=? AND defer.cutover_id=1",
        (int(row["defer_authority_id"]),),
    )
    if authority is None:
        raise RuntimeError("resume notification binding drift")
    candidate_id = int(authority["candidate_id"])
    if str(authority["stage"]) == "selection":
        candidate = storage.fetch_one(
            "SELECT candidate_evaluations.run_id FROM candidates "
            "JOIN candidate_evaluations ON candidate_evaluations.id=candidates.evaluation_id "
            "WHERE candidates.id=? AND candidates.status='pending_selection'",
            (candidate_id,),
        )
        if candidate is None:
            raise RuntimeError("resumed selection is no longer eligible")
        digest = service.create_digest(int(candidate["run_id"]), actor_id=actor_id)
        item = next(
            (value for value in digest.candidates if int(value["candidate_id"]) == candidate_id),
            None,
        )
        if item is None:
            raise RuntimeError("resumed selection is no longer eligible")
        buttons = digest.buttons[candidate_id]
        return (
            f"제목: {item['title']}\n출처: {item['source_url']}",
            {"inline_keyboard": [[{"text": button.label, "callback_data": button.token}] for button in buttons]},
        )
    generation = storage.fetch_one(
        "SELECT generation.id,generation.content_json FROM generations generation "
        "JOIN generation_jobs job ON job.id=generation.generation_job_id "
        "JOIN selections selection ON selection.id=job.selection_id "
        "WHERE selection.candidate_id=? AND generation.status='current' "
        "ORDER BY generation.id DESC LIMIT 1",
        (candidate_id,),
    )
    if generation is None:
        raise RuntimeError("resumed review is no longer eligible")
    sources = storage.fetch_all(
        "SELECT source_post_version_id FROM generation_sources WHERE generation_id=? ORDER BY source_post_version_id",
        (int(generation["id"]),),
    )
    source_version_ids = tuple(int(source["source_post_version_id"]) for source in sources)
    buttons = service.review_buttons(
        candidate_id,
        int(generation["id"]),
        actor_id=actor_id,
        source_version_ids=source_version_ids,
    )
    return (
        format_review_draft(str(generation["content_json"])),
        {"inline_keyboard": [[{"text": button.label, "callback_data": button.token}] for button in buttons]},
    )


def telegram_tick(args: argparse.Namespace) -> int:
    from .approval.telegram import TelegramApprovalAdapter, TelegramDeadline, split_telegram_text, split_telegram_titles
    from .automation import AutomationAuthority, automation_lock

    validate_capabilities(Capability.APPROVE_POLL)
    with automation_lock("telegram"), Storage.open(_database(args)) as storage:
        authority = AutomationAuthority(storage)
        service = _approval_service(storage)
        adapter = TelegramApprovalAdapter(os.environ["TELEGRAM_BOT_TOKEN"], service)
        tick_deadline = TelegramDeadline.after(args.deadline)
        config = _config(args)
        with storage.transaction(immediate=False) as connection:
            authority.validate_active_config_binding(connection, config)
        audience_binding_id = _runtime_audience(authority, adapter, deadline=tick_deadline)
        admission = authority.acquire_lease(
            "telegram_dispatch",
            now=datetime.now(UTC),
            lease_seconds=int(getattr(args, "lease_seconds", 90)),
        )
        try:
            authority.seal_noon_window(config, now=lambda: datetime.now(UTC))
        finally:
            authority.release_lease(admission, now=datetime.now(UTC), outcome="done")
        poll = authority.acquire_lease(
            "approval_poll",
            now=datetime.now(UTC),
            lease_seconds=int(getattr(args, "lease_seconds", 90)),
        )
        poll_outcome = "failed"
        handled = 0
        try:
            response = adapter._request(
                "getUpdates",
                {
                    "timeout": str(args.timeout),
                    "limit": str(args.limit),
                    "offset": str(_approval_poll_offset(storage, None) or 0),
                },
                deadline=tick_deadline,
            )
            updates = response.get("result")
            if not isinstance(updates, list):
                raise RuntimeError("Telegram Bot API returned an invalid getUpdates result")
            for update in updates:
                if not isinstance(update, dict) or not isinstance(update.get("update_id"), int):
                    continue
                callback = update.get("callback_query")
                token = callback.get("data") if isinstance(callback, dict) else None
                admitted = (
                    isinstance(token, str)
                    and storage.fetch_one(
                        "SELECT 1 FROM callback_tokens token JOIN telegram_notification_outbox outbox "
                        "ON outbox.id=token.notification_id WHERE token.token=? "
                        "AND outbox.cutover_id=1 AND outbox.audience_binding_id=? "
                        "AND outbox.state IN ('sent','ambiguous','resolved_delivered')",
                        (hash_callback_token(token), audience_binding_id),
                    )
                    is not None
                )
                if admitted:
                    adapter.handle_update(update, automation_lease=poll, deadline=tick_deadline)
                    handled += 1
                _advance_approval_poll_offset(storage, int(update["update_id"]))
            poll_outcome = "done"
        finally:
            authority.release_lease(poll, now=datetime.now(UTC), outcome=poll_outcome)
        dispatch = authority.acquire_lease(
            "telegram_dispatch",
            now=datetime.now(UTC),
            lease_seconds=int(getattr(args, "lease_seconds", 90)),
        )
        dispatch_outcome = "failed"
        try:
            authority.resume_due_and_enqueue(dispatch, now=datetime.now(UTC))
            claim = authority.claim_next_notification(dispatch, now=datetime.now(UTC))
            if claim is None:
                _print({"handled": handled, "status": "no_work"})
                dispatch_outcome = "done"
                return 0
            if claim.state == "sending" and authority.recover_possibly_sent(
                claim.notification_id, dispatch, now=datetime.now(UTC)
            ):
                _print({"handled": handled, "status": "ambiguous_recovered"})
                dispatch_outcome = "done"
                return 0
            text, markup = _notification_payload(
                storage,
                service,
                claim.notification_id,
                actor_id=_callback_actor_id(service.authorized_user_ids),
            )
            notification = storage.fetch_one(
                "SELECT notification_kind FROM telegram_notification_outbox WHERE id=?",
                (claim.notification_id,),
            )
            if notification is None:
                raise RuntimeError("notification disappeared")
            chunks = (
                split_telegram_titles(tuple(text.split("\n")))
                if str(notification["notification_kind"]) == "noon_digest"
                else split_telegram_text(text)
            )
            markup_identity, _callback_token_hashes = _telegram_markup_identity(storage, markup)
            metadata = tuple(
                (
                    len(chunk.encode("utf-16-le", "surrogatepass")) // 2,
                    _request_sha256(
                        {
                            "text": chunk,
                            "markup": markup_identity if index == len(chunks) - 1 else None,
                        }
                    ),
                    index == len(chunks) - 1 and markup is not None,
                )
                for index, chunk in enumerate(chunks)
            )
            authority.create_notification_chunks(claim.notification_id, metadata)
            next_chunk = authority.next_chunk(claim.notification_id, dispatch, now=datetime.now(UTC))
            if next_chunk is None:
                _print({"handled": handled, "status": "no_work"})
                dispatch_outcome = "done"
                return 0
            chunk_id, chunk_index, template_digest, has_buttons = next_chunk
            audience = storage.fetch_one(
                "SELECT outbox.audience_binding_id FROM telegram_notification_outbox outbox "
                "WHERE outbox.id=? AND outbox.cutover_id=1 AND outbox.audience_binding_id=?",
                (claim.notification_id, audience_binding_id),
            )
            if audience is None:
                raise RuntimeError("notification audience binding drift")
            prepared_payload = adapter.prepare_message_payload(
                chunks[chunk_index],
                markup=markup if has_buttons else None,
            )
            attempt = authority.prepare_chunk_attempt(
                claim.notification_id,
                chunk_id,
                _request_sha256(prepared_payload),
                dispatch,
                now=datetime.now(UTC),
            )
            if has_buttons:
                callback_linked = bool(_callback_token_hashes)
                for token_hash in _callback_token_hashes:
                    token_row = storage.fetch_one(
                        "SELECT id FROM callback_tokens WHERE token=?",
                        (token_hash,),
                    )
                    if token_row is None or not authority.link_callback(
                        int(token_row["id"]),
                        claim.notification_id,
                        attempt,
                        dispatch,
                        now=datetime.now(UTC),
                    ):
                        callback_linked = False
                        break
                if not callback_linked:
                    authority.settle_attempt(
                        attempt,
                        "abandoned_pre_marker",
                        dispatch,
                        now=datetime.now(UTC),
                    )
                    _print({"handled": handled, "status": "callback_link_failed"})
                    dispatch_outcome = "done"
                    return 0
            if tick_deadline.remaining() <= 0:
                authority.settle_attempt(attempt, "abandoned_pre_marker", dispatch, now=datetime.now(UTC))
                _print({"handled": handled, "status": "deadline_exhausted"})
                dispatch_outcome = "done"
                return 0
            authority.mark_possibly_sent(attempt, dispatch, now=datetime.now(UTC))
            result = adapter.send_prepared_message_once(
                prepared_payload,
                deadline=tick_deadline,
            )
            if result.accepted:
                authority.settle_attempt(
                    attempt, "accepted", dispatch, now=datetime.now(UTC), accepted_message_id=result.message_id
                )
                with suppress(TimeoutError):
                    adapter.pace_after_send(tick_deadline)
            elif result.safe_code == "rate_limited":
                authority.settle_attempt(
                    attempt,
                    "trusted_rejected",
                    dispatch,
                    now=datetime.now(UTC),
                    retryable=True,
                )
            elif result.safe_code == "transport_rejected":
                authority.settle_attempt(attempt, "trusted_rejected", dispatch, now=datetime.now(UTC))
            else:
                authority.settle_attempt(attempt, "ambiguous", dispatch, now=datetime.now(UTC))
            _print({"handled": handled, "status": "dispatched"})
            dispatch_outcome = "done"
            return 0
        finally:
            authority.release_lease(dispatch, now=datetime.now(UTC), outcome=dispatch_outcome)


def sheets_deliver_pending_once(args: argparse.Namespace) -> int:
    from .automation import AutomationAuthority, automation_lock
    from .handoffs import SheetHandoffService

    deadline_seconds = int(getattr(args, "deadline", 90))
    if deadline_seconds < 0:
        raise ValueError("--deadline-seconds must not be negative")
    worker_args = argparse.Namespace(
        **vars(args),
        _sheets_deadline_monotonic=time.monotonic() + deadline_seconds,
        _automation_sheets_worker=True,
    )
    with automation_lock("sheets"), Storage.open(_database(worker_args)) as storage:
        authority = AutomationAuthority(storage)
        config = _config(worker_args)
        with storage.transaction(immediate=False) as connection:
            authority.validate_active_config_binding(connection, config)
        lease = authority.acquire_lease(
            "sheets_delivery",
            now=datetime.now(UTC),
            lease_seconds=int(getattr(worker_args, "lease_seconds", 135)),
        )
        outcome = "failed"
        try:
            _require_sheets_worker_deadline(worker_args)
            target = storage.fetch_one("SELECT target_binding_id FROM automation_cutovers WHERE id=1")
            if target is None:
                _print({"status": "no_work"})
                outcome = "done"
                return 0
            SheetHandoffService(storage).recover_expired_pre_marker(
                int(target["target_binding_id"]), datetime.now(UTC).isoformat()
            )
            _require_sheets_worker_deadline(worker_args)
            handoffs = authority.post_baseline_handoff_ids(1)
            if not handoffs:
                _print({"status": "no_work"})
                outcome = "done"
                return 0
            result = _sheets_deliver_unlocked(argparse.Namespace(**vars(worker_args), handoff_id=handoffs[0]))
            outcome = "done"
            return result
        except GoogleSheetsDeadlineExceeded:
            _print({"status": "deadline_exhausted"})
            return 0
        finally:
            authority.release_lease(lease, now=datetime.now(UTC), outcome=outcome)


def automation_notification_resolve(args: argparse.Namespace) -> int:
    """Resolve only through the authority's supported immutable transition API."""
    with Storage.open(_database(args)) as storage:
        from .automation import AutomationAuthority

        state = storage.fetch_one("SELECT state FROM telegram_notification_outbox WHERE id=?", (args.intent_id,))
        if state is None:
            raise LookupError("notification intent does not exist")
        expected = str(state["state"])
        if args.expected_status != "manual_required" or expected not in {"ambiguous", "partial_manual_required"}:
            raise RuntimeError("notification is not in the expected manual-required state")
        if args.actor_id < 1:
            raise ValueError("--actor-id must be positive")
        resolution = "resolved_delivered" if args.resolution == "delivered" else "resolved_abandoned"
        expected_reason = "transport_verified" if resolution == "resolved_delivered" else "operator_abandoned"
        if args.reason_code != expected_reason:
            raise ValueError("resolution and reason code do not match")
        changed = AutomationAuthority(storage).resolve_notification(
            args.intent_id,
            cast(Literal["ambiguous", "partial_manual_required"], expected),
            cast(Literal["resolved_delivered", "resolved_abandoned"], resolution),
            actor_id=args.actor_id,
            reason_code=cast(Literal["transport_verified", "operator_abandoned"], args.reason_code),
            now=datetime.now(UTC),
        )
        _print({"changed": changed, "resolved": changed})
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

    auth = commands.add_parser("auth-telethon", help="interactively authorize an owner-only local Telethon session")
    auth.set_defaults(handler=auth_telethon)

    rank_command = commands.add_parser("rank", help="rank durable local observations without a provider")
    rank_command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    rank_command.add_argument("--db", type=Path)
    rank_command.set_defaults(handler=rank)

    fixture_reconcile = commands.add_parser("reconcile", help="perform bounded fixture recovery into local ranking")
    fixture_reconcile.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    fixture_reconcile.add_argument("--fixture", type=Path, required=True)
    fixture_reconcile.add_argument("--channel", required=True)
    fixture_reconcile.add_argument("--db", type=Path)
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
    live.add_argument("--lookback-hours", type=int, default=24)
    live.add_argument("--page-size", type=int, default=100)
    live.add_argument("--max-pages", type=int, default=10)
    live.set_defaults(handler=collect_live)

    reconcile = commands.add_parser("reconcile-live", help="perform bounded durable Telethon reconciliation")
    reconcile.add_argument("--config", type=Path, default=Path("config/channels.toml"))
    reconcile.add_argument("--db", type=Path)
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
    generate.add_argument("--candidate-id", type=int, required=True)
    generate.add_argument("--provider", choices=("fake", "openai_compatible"), required=True)
    generate.add_argument("--fixture-only", action="store_true")
    generate.add_argument("--page-count", type=int, choices=range(1, 9))
    generate.set_defaults(handler=generate_pending)
    codex_once = commands.add_parser("generate-codex-once", help="run one attested exact Codex generation job")
    codex_once.add_argument("--config", type=Path, default=Path("/etc/newsbot/config.toml"))
    codex_once.add_argument("--db", type=Path, default=Path("/var/lib/newsbot-canary/newsbot.db"))
    codex_once.set_defaults(handler=generate_codex_once)
    codex_v2_once = commands.add_parser(
        "generate-codex-v2-once",
        help="run one attested V2 Codex generation job",
    )
    codex_v2_once.add_argument(
        "--db",
        type=Path,
        default=Path("/var/lib/newsbot-v2/newsbot-v2.sqlite"),
    )
    codex_v2_once.set_defaults(handler=generate_codex_v2_once)
    v2_collect = commands.add_parser("v2-collect-live", help="collect one bounded V2 Telegram cycle")
    v2_collect.add_argument("--db", type=Path, required=True)
    v2_collect.add_argument("--lookback-hours", type=int, choices=range(1, 169), default=24)
    v2_collect.add_argument("--limit", type=int, choices=range(1, 501), default=100)
    v2_collect.set_defaults(handler=v2_collect_live)

    v2_telegram = commands.add_parser("v2-telegram-tick", help="run one V2 approval send-and-poll cycle")
    v2_telegram.add_argument("--db", type=Path, required=True)
    v2_telegram.add_argument("--deadline", type=float, default=30.0)
    v2_telegram.add_argument("--timeout", type=int, choices=range(0, 51), default=10)
    v2_telegram.set_defaults(handler=v2_telegram_tick)

    v2_sheets = commands.add_parser("v2-deliver-google-sheets-next", help="deliver one approved V2 draft")
    v2_sheets.add_argument("--db", type=Path, required=True)
    v2_sheets.add_argument("--deadline", type=float, default=120.0)
    v2_sheets.set_defaults(handler=v2_deliver_google_sheets_next)

    v2_cursor = commands.add_parser("v2-seed-telegram-cursor", help="merge the stopped owner's V2 cursor")
    v2_cursor.add_argument("--db", type=Path, required=True)
    v2_cursor.add_argument("--next-offset", type=int, required=True)
    v2_cursor.set_defaults(handler=v2_seed_telegram_cursor)
    v2_view = commands.add_parser("v2-status", help="show redacted V2 workflow status")
    v2_view.add_argument("--db", type=Path, required=True)
    v2_view.set_defaults(handler=v2_status)
    provider_pause = commands.add_parser("codex-provider-pause")
    provider_pause.add_argument("--db", type=Path)
    provider_pause.add_argument("--actor-id", type=int, required=True)
    provider_pause.add_argument("--expected-control-version", type=int, required=True)
    provider_pause.add_argument(
        "--reason-code", dest="reason_code", choices=("operator_security_hold", "maintenance"), required=True
    )
    provider_pause.set_defaults(handler=codex_provider_pause)
    provider_resume = commands.add_parser("codex-provider-resume")
    provider_resume.add_argument("--db", type=Path)
    provider_resume.add_argument("--actor-id", type=int, required=True)
    provider_resume.add_argument("--expected-control-version", type=int, required=True)
    provider_resume.add_argument(
        "--reason-code",
        dest="reason_code",
        choices=(
            "auth_restored",
            "config_repaired",
            "attestation_passed",
            "security_reviewed",
            "maintenance_complete",
        ),
        required=True,
    )
    provider_resume.set_defaults(handler=codex_provider_resume)
    for name, handler, reasons in (
        ("codex-job-hold", codex_job_hold, ("operator_review", "poison_output")),
        (
            "codex-job-release",
            codex_job_release,
            ("operator_reviewed", "source_packet_reduced", "transient_cleared"),
        ),
    ):
        retry = commands.add_parser(name)
        retry.add_argument("--db", type=Path)
        retry.add_argument("--actor-id", type=int, required=True)
        retry.add_argument("--generation-job-id", type=int, required=True)
        retry.add_argument("--reason-code", dest="reason_code", choices=reasons, required=True)
        retry.set_defaults(handler=handler)

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
    poll.add_argument("--offset", type=int)
    poll.add_argument("--timeout", type=int, choices=range(0, 51), default=0)
    poll.add_argument("--process-generation", action="store_true")
    poll.add_argument("--provider", choices=("fake", "openai_compatible"))
    poll.add_argument("--fixture-only", action="store_true")
    poll.add_argument("--page-count", type=int, choices=range(1, 9))
    poll.set_defaults(handler=poll_approvals)
    for name, handler, help_text in (
        ("sheets-validate", sheets_validate, "validate the fixed workplace Sheets schema"),
        ("sheets-bootstrap", sheets_bootstrap, "validate and bind the fixed workplace Sheets target"),
        ("sheets-status", sheets_status, "show redacted workplace delivery operation counts"),
        ("sheets-deliver", sheets_deliver, "deliver one immutable approved handoff"),
        ("sheets-reconcile", sheets_reconcile, "probe one possibly-sent Sheets operation"),
        (
            "sheets-retry-blocked",
            sheets_retry_blocked,
            "audit a corrected not-applied Sheets blocker",
        ),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
        command.add_argument("--db", type=Path)
        if name == "sheets-deliver":
            command.add_argument("--handoff-id", type=int, required=True)
        elif name in {"sheets-reconcile", "sheets-retry-blocked"}:
            command.add_argument("--operation-id", type=int, required=True)
        command.set_defaults(handler=handler)
    for name, handler, help_text in (
        ("automation-status", automation_status, "show redacted automation aggregates"),
        ("automation-quiescence-check", automation_quiescence_check, "check bounded automation quiescence"),
        (
            "automation-notification-inspect",
            automation_notification_inspect,
            "inspect one notification without content",
        ),
        (
            "automation-notification-resolve",
            automation_notification_resolve,
            "settle an eligible ambiguous notification without resend",
        ),
        (
            "automation-cutover-preview",
            automation_cutover_preview,
            "capture a bounded immutable cutover proposal",
        ),
        (
            "automation-cutover-apply",
            automation_cutover_apply,
            "apply an exact immutable cutover proposal",
        ),
        (
            "automation-release-activate",
            automation_release_activate,
            "append a quiescent compatible runtime activation",
        ),
        (
            "automation-collect-once",
            automation_collect_once,
            "run one fenced automatic collection",
        ),
        ("telegram-tick", telegram_tick, "run one fenced Telegram worker tick"),
        (
            "sheets-deliver-pending-once",
            sheets_deliver_pending_once,
            "deliver one fenced post-baseline Sheets handoff",
        ),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--db", type=Path)
        if name == "automation-notification-inspect":
            command.add_argument("--intent-id", type=int, required=True)
        elif name == "automation-notification-resolve":
            command.add_argument("--intent-id", type=int, required=True)
            command.add_argument("--expected-status", choices=("manual_required",), required=True)
            command.add_argument("--resolution", choices=("delivered", "abandoned"), required=True)
            command.add_argument("--actor-id", type=int, required=True)
            command.add_argument(
                "--reason-code",
                choices=("transport_verified", "operator_abandoned"),
                required=True,
            )
        elif name == "automation-cutover-preview":
            command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
            command.add_argument("--proposal-id", required=True)
            command.add_argument("--release-digest", required=True)
        elif name == "automation-cutover-apply":
            command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
            command.add_argument("--proposal-id", required=True)
            command.add_argument("--proposal-sha256", required=True)
            command.add_argument("--release-digest", required=True)
        elif name == "automation-release-activate":
            command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
            command.add_argument("--release-digest", required=True)
        elif name == "automation-collect-once":
            command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
            command.add_argument("--page-size", type=int, default=100)
            command.add_argument("--max-pages", type=int, default=10)
            command.add_argument("--lookback-hours", type=int, default=24)
            command.add_argument("--deadline-seconds", dest="deadline", type=int, default=180)
            command.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=225)
        elif name == "telegram-tick":
            command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
            command.add_argument("--poll-timeout", dest="timeout", type=int, choices=range(0, 51), default=10)
            command.add_argument("--max-updates", dest="limit", type=int, choices=range(1, 101), default=50)
            command.add_argument("--max-notifications", type=int, choices=(1,), default=1)
            command.add_argument("--deadline-seconds", dest="deadline", type=int, default=60)
            command.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=90)
        elif name == "sheets-deliver-pending-once":
            command.add_argument("--config", type=Path, default=Path("config/channels.toml"))
            command.add_argument("--max-handoffs", type=int, choices=(1,), default=1)
            command.add_argument("--deadline-seconds", dest="deadline", type=int, default=90)
            command.add_argument("--lease-seconds", dest="lease_seconds", type=int, default=135)
            command.add_argument("--sheet-lease-seconds", type=int, choices=(300,), default=300)
        command.set_defaults(handler=handler)

    def manual_command(name: str, handler: CommandHandler, help_text: str) -> argparse.ArgumentParser:
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--profile", type=Path, required=True)
        command.add_argument("--state")
        command.add_argument("--database", default="newsbot.sqlite3")
        command.set_defaults(handler=handler)
        return command

    manual_command("manual-init", manual.manual_init, "initialize private local manual state")
    imported = manual_command("manual-import", manual.manual_import, "import bounded local source observations")
    imported.add_argument("--input", type=Path, required=True)
    collected = manual_command(
        "manual-collect-telethon", manual.manual_collect_telethon, "collect a bounded public Telethon page"
    )
    collected.add_argument("--lookback-hours", type=int, required=True)
    collected.add_argument("--page-limit", type=int, required=True)
    collected.add_argument("--max-pages", type=int, default=1)
    collected.add_argument("--deadline-seconds", type=int, default=180)
    manual_command("manual-rank", manual.manual_rank, "rank existing manual observations")
    candidates = manual_command("manual-candidates", manual.manual_candidates, "list deterministic manual candidates")
    candidates.add_argument("--run-id", type=int, required=True)
    candidates.add_argument("--output-dir")
    decision = manual_command(
        "manual-candidate-decision", manual.manual_candidate_decision, "apply a local candidate selection decision"
    )
    decision.add_argument("--run-id", type=int, required=True)
    decision.add_argument("--candidate-id", type=int, required=True)
    decision.add_argument("--decision", choices=("select", "reject"), required=True)
    decision.add_argument("--expected-receipt", required=True)
    generated = manual_command("manual-generate", manual.manual_generate, "generate one selected local candidate")
    generated.add_argument("--candidate-id", type=int, required=True)
    generated.add_argument("--provider", choices=("fake", "openai_compatible"), required=True)
    generated.add_argument("--page-count", type=int, default=1)
    generated.add_argument("--output-dir")
    draft = manual_command("manual-draft", manual.manual_draft, "materialize an exact local draft")
    draft.add_argument("--generation-id", type=int, required=True)
    draft.add_argument("--output-dir")
    review = manual_command("manual-review", manual.manual_review, "record a local review decision")
    review.add_argument("--candidate-id", type=int, required=True)
    review.add_argument("--generation-id", type=int, required=True)
    review.add_argument("--decision", choices=("approve-local", "regenerate", "reject"), required=True)
    review.add_argument("--expected-draft-digest", required=True)
    exported = manual_command("manual-export", manual.manual_export, "materialize approved local exports")
    exported.add_argument("--output-dir")
    manual_command("manual-status", manual.manual_status, "show aggregate manual state")
    inspected = manual_command("manual-inspect", manual.manual_inspect, "show bounded redacted manual detail")
    inspected.add_argument("--run-id", type=int)
    inspected.add_argument("--candidate-id", type=int)
    inspected.add_argument("--generation-id", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        # Legacy entry points must refuse a manually bound database before any
        # automation lock, provider, Telegram, or Sheets authority is acquired.
        if not args.command.startswith("manual-") and args.command != "init-db" and hasattr(args, "db"):
            _assert_legacy_database_authority(_database(args))
        handler = cast(CommandHandler, args.handler)
        return handler(args)
    except (ConfigError, LookupError, OSError, RuntimeError, ValueError) as error:
        parser.error(str(error))
    return 2

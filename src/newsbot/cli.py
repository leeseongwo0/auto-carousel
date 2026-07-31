"""Argparse entry point for local-first newsbot operations."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import stat
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

from .ai.base import GenerationProvider
from .approval.scripted import ScriptedAction, ScriptedApprovalAdapter
from .candidates import CandidateApprovalService, CandidateDigest
from .collectors.base import SourceObservation
from .collectors.fixture import FixtureCollector
from .config import AppConfig, Capability, ConfigError, load_config, validate_capabilities
from .observability import inspect, status
from .pipeline import NewsPipeline, _draft_payload
from .runtime import FixtureClock, SystemClock
from .secrets import SecretFileError, SessionStore, ensure_private_directory, read_service_account_info
from .sheets.base import SheetsAdapter
from .storage import DurableCollection, Storage

CommandHandler = Callable[[argparse.Namespace], int]


def _path_from_environment(option: str, fallback: str) -> Path:
    return Path(os.environ.get(option, fallback))


def _config(args: argparse.Namespace) -> AppConfig:
    overrides: dict[str, Path] = {}
    if args.db is not None:
        overrides["database_path"] = args.db
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
        result: dict[str, Any] = {
            "candidate_count": len(stage.digest.candidates),
            "digest_id": stage.digest.id,
            "run_id": stage.run_id,
            "status": "pending_selection",
        }
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
        pipeline = DurableLivePipeline(storage, config, _live_provider_forbidden, SystemClock())
        stage = asyncio.run(
            pipeline.run(
                observations, run_key=_live_run_key(observations, config), approval_service=service, actor_id=1
            )
        )
    _print(
        {
            "candidate_count": len(stage.digest.candidates),
            "digest_id": stage.digest.id,
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
            "candidate_count": len(stage.digest.candidates),
            "digest_id": stage.digest.id,
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
            "digest_id": stage.digest.id,
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
    if spreadsheet_id:
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
    expected_db = {
        "newsbot-generate-codex.service": Path("/var/lib/newsbot/newsbot.db"),
        "newsbot-generate-codex-canary.service": Path("/var/lib/newsbot-canary/newsbot.db"),
    }[unit]
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


def _live_sheets(args: argparse.Namespace) -> tuple[AppConfig, SheetsAdapter, str]:
    validate_capabilities(Capability.LIVE_SHEETS)
    config = _config(args)
    if config.google_service_account_file is None or config.google_sheets_spreadsheet_id is None:
        raise ConfigError("Google Sheets capability is incomplete")
    try:
        credential_info = read_service_account_info(config.google_service_account_file)
    except (OSError, SecretFileError) as error:
        raise ConfigError("Google Sheets credential file is invalid") from error
    try:
        from .sheets.google import GoogleSheetsAdapter

        adapter = GoogleSheetsAdapter.from_credentials(
            credential_info=credential_info,
            spreadsheet_id=config.google_sheets_spreadsheet_id,
        )
    except (ImportError, RuntimeError, ValueError) as error:
        raise RuntimeError("Google Sheets capability is unavailable") from error
    return config, adapter, credential_info["client_email"]


def _request_sha256(body: object) -> str:
    return sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


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
    config, adapter, _ = _live_sheets(args)
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
    codex_once.add_argument("--db", type=Path, default=Path("/var/lib/newsbot/newsbot.db"))
    codex_once.set_defaults(handler=generate_codex_once)
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

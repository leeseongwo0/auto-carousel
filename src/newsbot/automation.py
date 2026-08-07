"""Durable authority primitives for the systemd automation workers.

This module contains no provider calls.  Callers prepare/settle durable state on
these APIs and perform remote work only after the prepare transaction commits.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from hmac import compare_digest
from hmac import new as hmac_new
from pathlib import Path
from secrets import token_urlsafe
from typing import Any, Literal, cast
from zoneinfo import ZoneInfo

from .storage import Storage

Stream = Literal["collect", "approval_poll", "telegram_dispatch", "sheets_delivery"]
NotificationState = Literal[
    "pending",
    "claimed",
    "sending",
    "sent",
    "canceled",
    "ambiguous",
    "partial_manual_required",
    "resolved_delivered",
    "resolved_abandoned",
]

_LOCK_DIRECTORY = Path("/var/lib/newsbot/locks")


class AutomationBusyError(RuntimeError):
    """Raised when another worker owns a nonblocking process lock or lease."""


class AutomationDriftError(RuntimeError):
    """Raised when immutable preview state no longer matches application state."""


@dataclass(frozen=True, slots=True)
class StreamLease:
    stream: Stream
    owner_token: str
    owner_hash: str
    fence: int
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class Frontier:
    channel_key_digest: str
    upper_message_id: int
    captured_at: datetime


@dataclass(frozen=True, slots=True)
class CutoverProposal:
    proposal_id: str
    config_digest: str
    cursor_digest: str
    intervals_digest: str
    target_id: int
    target_fingerprint: str
    release_digest: str
    audience_digest: str
    maxima: tuple[int, int, int, int, int]
    approval_offset: int
    frontiers: tuple[Frontier, ...]


@dataclass(frozen=True, slots=True)
class AutomationTopology:
    """Redacted current-runtime authority result for one database connection."""

    active_cutover_present: bool
    active_frontier_count: int
    config_binding_match: bool
    configured_channel_count: int
    configured_channel_unique: bool
    frontier_coverage: bool
    frontier_shape_supported: bool
    status: Literal["ok", "drift"]


@dataclass(frozen=True, slots=True)
class NotificationClaim:
    notification_id: int
    state: NotificationState
    lease: StreamLease


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _digest(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("authority digests must be lowercase SHA-256 hex")
    return value


def _owner_hash(token: str) -> str:
    return sha256(token.encode("utf-8")).hexdigest()


@contextmanager
def automation_lock(
    kind: Literal["collect", "telegram", "sheets"],
    *,
    directory: Path | None = None,
) -> Iterator[None]:
    """Acquire one canonical nonblocking advisory lock without exposing its path."""
    production = directory is None
    directory = _LOCK_DIRECTORY if production else directory
    assert directory is not None
    lock_path = directory / f"{kind}.lock"
    if production:
        try:
            identity = lock_path.lstat()
        except OSError as exc:
            raise AutomationBusyError(f"{kind} lock identity failed") from exc
        if (
            not stat.S_ISREG(identity.st_mode)
            or stat.S_ISLNK(identity.st_mode)
            or identity.st_uid != os.geteuid()
            or identity.st_gid != os.getegid()
            or stat.S_IMODE(identity.st_mode) != 0o600
            or identity.st_nlink != 1
        ):
            raise AutomationBusyError(f"{kind} lock identity failed")
        fd = os.open(lock_path, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
    else:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AutomationBusyError(f"{kind} worker is busy") from exc
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


@contextmanager
def cutover_locks(*, directory: Path | None = None) -> Iterator[None]:
    """Acquire canonical collect, Telegram, and Sheets locks in that order."""
    with (
        automation_lock("collect", directory=directory),
        automation_lock("telegram", directory=directory),
        automation_lock("sheets", directory=directory),
    ):
        yield


class AutomationAuthority:
    """SQLite-backed fencing, cutover and safe notification authority."""

    @staticmethod
    def _canonical_policy(config: object) -> tuple[str, str, str]:
        policy = getattr(config, "news_policy", None)
        if policy is None:
            raise AutomationDriftError("runtime news policy is unavailable")
        fields = getattr(policy, "__dataclass_fields__", {})
        canonical = json.dumps(
            {name: getattr(policy, name) for name in sorted(fields)},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return str(getattr(config, "digest", "")), str(getattr(policy, "version", "")), canonical

    @classmethod
    def topology_status(cls, connection: sqlite3.Connection, config: object) -> AutomationTopology:
        """Evaluate current topology on this connection without exposing membership."""
        config_any: Any = config
        try:
            channels = tuple(config_any.enabled_channels)
            configured_digests = tuple(sha256(str(channel.id).encode()).hexdigest() for channel in channels)
        except (AttributeError, TypeError):
            channels = ()
            configured_digests = ()
        configured_unique = len(configured_digests) == 5 and len(set(configured_digests)) == 5
        cutover = connection.execute("SELECT proposal_id FROM automation_cutovers WHERE id=1").fetchone()
        frontier_digests: set[str] = set()
        if cutover is not None:
            frontier_digests = {
                str(row["channel_key_digest"])
                for row in connection.execute(
                    "SELECT channel_key_digest FROM automation_proposal_frontiers WHERE proposal_id=?",
                    (str(cutover["proposal_id"]),),
                )
            }
        frontier_count = len(frontier_digests)
        frontier_shape_supported = frontier_count in {5, 6}
        frontier_coverage = configured_unique and set(configured_digests).issubset(frontier_digests)
        binding_match = False
        try:
            config_digest, policy_version, canonical = cls._canonical_policy(config)
        except AutomationDriftError:
            pass
        else:
            binding = connection.execute(
                "SELECT binding.config_digest,binding.news_policy_version,binding.canonical_policy_json "
                "FROM (SELECT id FROM automation_release_activations "
                "WHERE cutover_id=1 ORDER BY id DESC LIMIT 1) activation "
                "LEFT JOIN automation_release_config_bindings binding ON binding.activation_id=activation.id"
            ).fetchone()
            binding_match = (
                binding is not None
                and binding["config_digest"] is not None
                and (
                    compare_digest(str(binding["config_digest"]), config_digest)
                    and compare_digest(str(binding["news_policy_version"]), policy_version)
                    and str(binding["canonical_policy_json"]) == canonical
                )
            )
        active = cutover is not None
        return AutomationTopology(
            active_cutover_present=active,
            active_frontier_count=frontier_count,
            config_binding_match=binding_match,
            configured_channel_count=len(channels),
            configured_channel_unique=configured_unique,
            frontier_coverage=frontier_coverage,
            frontier_shape_supported=frontier_shape_supported,
            status=(
                "ok"
                if active and configured_unique and frontier_shape_supported and frontier_coverage and binding_match
                else "drift"
            ),
        )

    @classmethod
    def validate_active_topology(
        cls, connection: sqlite3.Connection, config: object, *, require_binding: bool
    ) -> AutomationTopology:
        """Require current five-channel authority, optionally including its latest binding."""
        topology = cls.topology_status(connection, config)
        if (
            not topology.active_cutover_present
            or topology.configured_channel_count != 5
            or not topology.configured_channel_unique
            or not topology.frontier_shape_supported
            or not topology.frontier_coverage
            or (require_binding and not topology.config_binding_match)
        ):
            raise AutomationDriftError("runtime automation topology drifted")
        return topology

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    @staticmethod
    def enqueue_candidate_notification(
        connection: sqlite3.Connection, *, candidate_id: int, source_set_key: str, subject_digest: str
    ) -> bool:
        """Insert the one semantic candidate intent for active post-frontier work."""
        row = connection.execute("SELECT audience_binding_id FROM automation_cutovers WHERE id=1").fetchone()
        if row is None:
            return False
        cursor = connection.execute(
            "INSERT OR IGNORE INTO telegram_notification_outbox("
            "audience_binding_id,cutover_id,notification_kind,candidate_id,source_set_key,subject_digest,state"
            ") VALUES(?,1,'candidate',?,?,?,'pending')",
            (int(row["audience_binding_id"]), candidate_id, source_set_key, subject_digest),
        )
        return cursor.rowcount == 1

    @classmethod
    def validate_active_config_binding(cls, connection: sqlite3.Connection, config: object) -> int:
        """Require the latest immutable release/config pair before worker mutation."""
        cls.validate_active_topology(connection, config, require_binding=True)
        config_digest, policy_version, canonical = cls._canonical_policy(config)
        row = connection.execute(
            "SELECT binding.id FROM (SELECT id FROM automation_release_activations "
            "WHERE cutover_id=1 ORDER BY id DESC LIMIT 1) activation "
            "LEFT JOIN automation_release_config_bindings binding ON binding.activation_id=activation.id "
            "WHERE binding.config_digest=? AND binding.news_policy_version=? AND binding.canonical_policy_json=?",
            (config_digest, policy_version, canonical),
        ).fetchone()
        if row is None or row["id"] is None:
            raise AutomationDriftError("runtime release/config binding drifted")
        return int(row["id"])

    @staticmethod
    def enqueue_noon_digest_notification(
        connection: sqlite3.Connection,
        *,
        window_id: int,
        subject_digest: str,
        committed_at: datetime,
    ) -> bool:
        """Persist a noon intent with its post-write-lock admission instant."""
        row = connection.execute("SELECT audience_binding_id FROM automation_cutovers WHERE id=1").fetchone()
        if row is None:
            return False
        cursor = connection.execute(
            "INSERT OR IGNORE INTO telegram_notification_outbox("
            "audience_binding_id,cutover_id,notification_kind,ambiguous_window_id,subject_digest,state,created_at"
            ") VALUES(?,1,'noon_digest',?,?,'pending',?)",
            (int(row["audience_binding_id"]), window_id, subject_digest, _timestamp(committed_at)),
        )
        return cursor.rowcount == 1

    def seal_noon_window(self, config: object, *, now: datetime | Callable[[], datetime]) -> None:
        """Linearize Seoul noon admission after acquiring SQLite's write lock."""
        with self.storage.transaction() as connection:
            sampled_now = now() if callable(now) else now
            if connection.execute("SELECT 1 FROM automation_cutovers WHERE id=1").fetchone() is None:
                return
            binding_id = self.validate_active_config_binding(connection, config)
            if binding_id == 0:
                return
            local = sampled_now.astimezone(ZoneInfo("Asia/Seoul"))
            today = local.date()
            connection.execute(
                "UPDATE ambiguous_digest_windows SET state='skipped' "
                "WHERE scheduled_local_date<? AND state='collecting'",
                (today.isoformat(),),
            )
            row = connection.execute(
                "SELECT id,state,config_binding_id FROM ambiguous_digest_windows WHERE scheduled_local_date=?",
                (today.isoformat(),),
            ).fetchone()
            in_window = local.hour == 12
            if row is None:
                state = "empty" if in_window else ("skipped" if local.hour >= 13 else None)
                if state is not None:
                    opens = datetime.combine(today, datetime.min.time(), ZoneInfo("Asia/Seoul")).replace(hour=12)
                    connection.execute(
                        "INSERT INTO ambiguous_digest_windows(scheduled_local_date,config_binding_id,opens_at,closes_at,state,created_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (
                            today.isoformat(),
                            binding_id,
                            _timestamp(opens),
                            _timestamp(opens + timedelta(hours=1)),
                            state,
                            _timestamp(sampled_now),
                        ),
                    )
                return
            if str(row["state"]) != "collecting":
                return
            if int(row["config_binding_id"]) != binding_id:
                raise AutomationDriftError("noon window binding drifted")
            if not in_window:
                if local.hour >= 13:
                    connection.execute(
                        "UPDATE ambiguous_digest_windows SET state='skipped' WHERE id=?", (int(row["id"]),)
                    )
                return
            items = connection.execute(
                "SELECT id FROM ambiguous_digest_items WHERE window_id=? ORDER BY ordering_timestamp,id",
                (int(row["id"]),),
            ).fetchall()
            if not items:
                connection.execute("UPDATE ambiguous_digest_windows SET state='empty' WHERE id=?", (int(row["id"]),))
                return
            subject = sha256(f"noon:{int(row['id'])}".encode()).hexdigest()
            self.enqueue_noon_digest_notification(
                connection,
                window_id=int(row["id"]),
                subject_digest=subject,
                committed_at=sampled_now,
            )
            connection.execute("UPDATE ambiguous_digest_windows SET state='queued' WHERE id=?", (int(row["id"]),))

    @staticmethod
    def enqueue_review_notification(
        connection: sqlite3.Connection, *, generation_id: int, generation_job_id: int, subject_digest: str
    ) -> bool:
        """Insert a review intent only for the exact post-cutover job authority."""
        row = connection.execute(
            "SELECT cutover.audience_binding_id FROM automation_cutovers cutover "
            "JOIN automation_generation_authority authority ON authority.cutover_id=cutover.id "
            "WHERE cutover.id=1 AND authority.generation_job_id=? "
            "AND ? > cutover.baseline_generation_job_id",
            (generation_job_id, generation_job_id),
        ).fetchone()
        if row is None:
            return False
        cursor = connection.execute(
            "INSERT OR IGNORE INTO telegram_notification_outbox("
            "audience_binding_id,cutover_id,notification_kind,generation_id,subject_digest,state"
            ") VALUES(?,1,'review',?,?,'pending')",
            (int(row["audience_binding_id"]), generation_id, subject_digest),
        )
        return cursor.rowcount == 1

    def resume_due_and_enqueue(self, lease: StreamLease, *, now: datetime) -> tuple[int, ...]:
        """The only automated post-cutover deferred-to-review/selection transition."""
        due_at = _timestamp(now)
        resumed: list[int] = []
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            rows = tuple(
                connection.execute(
                    "SELECT authority.id,authority.candidate_id,authority.stage,authority.due_at,"
                    "cutover.audience_binding_id FROM automation_defer_authority authority "
                    "JOIN automation_cutovers cutover ON cutover.id=authority.cutover_id "
                    "JOIN candidates candidate ON candidate.id=authority.candidate_id "
                    "WHERE authority.cutover_id=1 AND candidate.status='deferred' "
                    "AND candidate.deferred_stage=authority.stage AND candidate.deferred_until=authority.due_at "
                    "AND aware_epoch_us(authority.due_at)<=aware_epoch_us(?) ORDER BY authority.id",
                    (due_at,),
                )
            )
            for row in rows:
                candidate_id = int(row["candidate_id"])
                stage = str(row["stage"])
                self.storage.authorize_defer_transition(candidate_id, None, None, lease.owner_token, lease.fence)
                status = "pending_selection" if stage == "selection" else "pending_review"
                updated = connection.execute(
                    "UPDATE candidates SET status=?,deferred_stage=NULL,deferred_until=NULL "
                    "WHERE id=? AND status='deferred' AND deferred_stage=? AND deferred_until=?",
                    (status, candidate_id, stage, row["due_at"]),
                )
                if updated.rowcount != 1:
                    continue
                connection.execute(
                    "UPDATE callback_tokens SET revoked_at=? WHERE revoked_at IS NULL "
                    "AND CAST(json_extract(payload_json,'$.candidate_id') AS INTEGER)=?",
                    (due_at, candidate_id),
                )
                connection.execute(
                    "INSERT OR IGNORE INTO telegram_notification_outbox("
                    "audience_binding_id,cutover_id,notification_kind,defer_authority_id,stage,subject_digest,state"
                    ") VALUES(?,1,'resume',?,?,?,'pending')",
                    (
                        int(row["audience_binding_id"]),
                        int(row["id"]),
                        stage,
                        sha256(f"resume:{row['id']}:{candidate_id}:{stage}".encode()).hexdigest(),
                    ),
                )
                resumed.append(candidate_id)
        return tuple(resumed)

    def acquire_lease(
        self, stream: Stream, *, now: datetime, lease_seconds: int, owner_token: str | None = None
    ) -> StreamLease:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        token = owner_token or token_urlsafe(32)
        owner_hash = _owner_hash(token)
        expiry = now.astimezone(UTC) + timedelta(seconds=lease_seconds)
        with self.storage.transaction() as connection:
            row = connection.execute(
                "SELECT owner_hash,fence,expires_at FROM automation_stream_leases WHERE stream=?", (stream,)
            ).fetchone()
            if row is not None and datetime.fromisoformat(str(row["expires_at"])).astimezone(UTC) > now.astimezone(UTC):
                raise AutomationBusyError(f"{stream} stream is leased")
            if row is not None:
                if stream == "telegram_dispatch":
                    self._recover_expired_prepared_attempts(
                        connection,
                        owner_hash=str(row["owner_hash"]),
                        fence=int(row["fence"]),
                        now=now,
                    )
                connection.execute(
                    "UPDATE automation_stream_runs SET finished_at=?,outcome='abandoned' "
                    "WHERE stream=? AND fence=? AND finished_at IS NULL",
                    (_timestamp(now), stream, int(row["fence"])),
                )
            fence = int(
                connection.execute(
                    "SELECT COALESCE(MAX(fence),0)+1 FROM automation_stream_runs WHERE stream=?", (stream,)
                ).fetchone()[0]
            )
            connection.execute(
                "INSERT INTO automation_stream_leases(stream,owner_hash,fence,expires_at,acquired_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(stream) DO UPDATE SET owner_hash=excluded.owner_hash,fence=excluded.fence,expires_at=excluded.expires_at,acquired_at=excluded.acquired_at",
                (stream, owner_hash, fence, _timestamp(expiry), _timestamp(now)),
            )
            connection.execute(
                "INSERT INTO automation_stream_runs(stream,owner_hash,fence,started_at) VALUES(?,?,?,?)",
                (stream, owner_hash, fence, _timestamp(now)),
            )
        return StreamLease(stream, token, owner_hash, fence, expiry)

    def release_lease(self, lease: StreamLease, *, now: datetime, outcome: str = "done") -> bool:
        with self.storage.transaction() as connection:
            if lease.stream == "telegram_dispatch" and outcome != "done":
                self._recover_expired_prepared_attempts(
                    connection,
                    owner_hash=lease.owner_hash,
                    fence=lease.fence,
                    now=now,
                )
            cursor = connection.execute(
                "DELETE FROM automation_stream_leases WHERE stream=? AND owner_hash=? AND fence=? AND aware_epoch_us(expires_at)>aware_epoch_us(?)",
                (lease.stream, lease.owner_hash, lease.fence, _timestamp(now)),
            )
            connection.execute(
                "UPDATE automation_stream_runs SET finished_at=?,outcome=? WHERE stream=? AND owner_hash=? AND fence=? AND finished_at IS NULL",
                (_timestamp(now), outcome, lease.stream, lease.owner_hash, lease.fence),
            )
            return cursor.rowcount == 1

    def record_audience_binding(self, *, bot_id_digest: str, token_hmac: str, audience_hmac: str, version: int) -> int:
        """Persist opaque audience proofs without retaining any audience identifier."""
        if version < 1:
            raise ValueError("audience version must be positive")
        _digest(bot_id_digest)
        _digest(token_hmac)
        _digest(audience_hmac)
        with self.storage.transaction() as connection:
            rows = connection.execute(
                "SELECT id,token_hmac,audience_hmac,version FROM telegram_audience_bindings "
                "WHERE bot_id_digest=? ORDER BY version",
                (bot_id_digest,),
            ).fetchall()
            for row in rows:
                if compare_digest(str(row["token_hmac"]), token_hmac) and compare_digest(
                    str(row["audience_hmac"]), audience_hmac
                ):
                    return int(row["id"])
            expected_version = 1 if not rows else int(rows[-1]["version"]) + 1
            if version != expected_version:
                raise AutomationDriftError("Telegram audience binding version drifted")
            cursor = connection.execute(
                "INSERT INTO telegram_audience_bindings(bot_id_digest,token_hmac,audience_hmac,version) VALUES(?,?,?,?)",
                (bot_id_digest, token_hmac, audience_hmac, version),
            )
            if cursor.lastrowid is None:
                raise AutomationDriftError("Telegram audience binding failed")
            return int(cursor.lastrowid)

    def next_audience_version(self, bot_id_digest: str) -> int:
        _digest(bot_id_digest)
        row = self.storage.fetch_one(
            "SELECT COALESCE(MAX(version),0)+1 AS version FROM telegram_audience_bindings WHERE bot_id_digest=?",
            (bot_id_digest,),
        )
        if row is None:
            raise AutomationDriftError("Telegram audience version lookup failed")
        return int(row["version"])

    def safe_cutover(self) -> dict[str, bool]:
        """Return only cutover presence, not proposal, target, or audience identity."""
        return {"active": self.storage.fetch_one("SELECT 1 FROM automation_cutovers WHERE id=1") is not None}

    def claim_next_notification(self, lease: StreamLease, *, now: datetime) -> NotificationClaim | None:
        """Claim the oldest pending notification under the current Telegram fence."""
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            row = connection.execute(
                "SELECT id,state FROM telegram_notification_outbox WHERE state IN ('pending','claimed','sending') ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            notification_id = int(row["id"])
            if str(row["state"]) in {"claimed", "sending"}:
                return NotificationClaim(notification_id, cast(NotificationState, str(row["state"])), lease)
            cursor = connection.execute(
                "UPDATE telegram_notification_outbox SET state='claimed',claimed_at=? WHERE id=? AND state='pending'",
                (_timestamp(now), notification_id),
            )
            if cursor.rowcount != 1:
                return None
            connection.execute(
                "INSERT INTO telegram_notification_events(notification_id,event_kind,created_at) VALUES(?,'claimed',?)",
                (notification_id, _timestamp(now)),
            )
            return NotificationClaim(notification_id, "claimed", lease)

    def recover_possibly_sent(self, notification_id: int, lease: StreamLease, *, now: datetime) -> bool:
        """Fence takeover from resending a request whose remote effect is unknown."""
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            attempt = connection.execute(
                "SELECT attempt.id FROM telegram_chunk_attempts attempt "
                "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
                "WHERE chunk.notification_id=? AND attempt.state='possibly_sent' ORDER BY attempt.id LIMIT 1",
                (notification_id,),
            ).fetchone()
            if attempt is None:
                return False
            attempt_id = int(attempt["id"])
            cursor = connection.execute(
                "UPDATE telegram_chunk_attempts SET state='ambiguous',settled_at=? "
                "WHERE id=? AND state='possibly_sent'",
                (_timestamp(now), attempt_id),
            )
            if cursor.rowcount != 1:
                return False
            connection.execute(
                "INSERT INTO telegram_chunk_attempt_events(chunk_attempt_id,event_kind,created_at) "
                "VALUES(?,'ambiguous',?)",
                (attempt_id, _timestamp(now)),
            )
            connection.execute(
                "UPDATE telegram_notification_outbox SET state='ambiguous',terminal_at=? "
                "WHERE id=? AND state='sending'",
                (_timestamp(now), notification_id),
            )
            connection.execute(
                "INSERT INTO telegram_notification_events(notification_id,chunk_attempt_id,event_kind) "
                "VALUES(?,?,'ambiguous')",
                (notification_id, attempt_id),
            )
            return True

    @staticmethod
    def _recover_expired_prepared_attempts(
        connection: sqlite3.Connection, *, owner_hash: str, fence: int, now: datetime
    ) -> None:
        """Abandon requests that were prepared but never crossed the send marker."""
        rows = tuple(
            connection.execute(
                "SELECT attempt.id,chunk.notification_id FROM telegram_chunk_attempts attempt "
                "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
                "JOIN telegram_notification_outbox notification ON notification.id=chunk.notification_id "
                "WHERE attempt.owner_hash=? AND attempt.fence=? AND attempt.state='prepared' "
                "AND notification.state='sending'",
                (owner_hash, fence),
            )
        )
        if not rows:
            return
        settled_at = _timestamp(now)
        attempt_ids = tuple(int(row["id"]) for row in rows)
        notification_ids = tuple({int(row["notification_id"]) for row in rows})
        placeholders = ",".join("?" for _ in attempt_ids)
        connection.execute(
            "UPDATE telegram_chunk_attempts SET state='abandoned_pre_marker',settled_at=? "
            f"WHERE id IN ({placeholders}) AND owner_hash=? AND fence=? AND state='prepared'",
            (settled_at, *attempt_ids, owner_hash, fence),
        )
        connection.executemany(
            "INSERT INTO telegram_chunk_attempt_events(chunk_attempt_id,event_kind,created_at) "
            "VALUES(?,'abandoned_pre_marker',?)",
            ((attempt_id, settled_at) for attempt_id in attempt_ids),
        )
        connection.execute(
            f"UPDATE callback_tokens SET revoked_at=? WHERE chunk_attempt_id IN ({placeholders}) "
            "AND consumed_at IS NULL AND revoked_at IS NULL",
            (settled_at, *attempt_ids),
        )
        for notification_id in notification_ids:
            accepted = connection.execute(
                "SELECT 1 FROM telegram_chunk_attempts attempt "
                "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
                "WHERE chunk.notification_id=? AND attempt.state='accepted' LIMIT 1",
                (notification_id,),
            ).fetchone()
            if accepted is None:
                connection.execute(
                    "UPDATE telegram_notification_outbox "
                    "SET state='pending',claimed_at=NULL,terminal_at=NULL "
                    "WHERE id=? AND state='sending'",
                    (notification_id,),
                )
                continue
            attempt_id = next(int(row["id"]) for row in rows if int(row["notification_id"]) == notification_id)
            connection.execute(
                "UPDATE telegram_notification_outbox "
                "SET state='partial_manual_required',terminal_at=? "
                "WHERE id=? AND state='sending'",
                (settled_at, notification_id),
            )
            connection.execute(
                "INSERT INTO telegram_notification_events("
                "notification_id,chunk_attempt_id,event_kind,created_at"
                ") VALUES(?,?,'partial_manual_required',?)",
                (notification_id, attempt_id, settled_at),
            )

    def _current_lease(self, connection: sqlite3.Connection, lease: StreamLease, now: datetime) -> None:
        row = connection.execute(
            "SELECT 1 FROM automation_stream_leases WHERE stream=? AND owner_hash=? AND fence=? AND aware_epoch_us(expires_at)>aware_epoch_us(?)",
            (lease.stream, lease.owner_hash, lease.fence, _timestamp(now)),
        ).fetchone()
        if row is None:
            raise AutomationBusyError("stream lease is no longer current")

    @staticmethod
    def _is_quiescent(connection: sqlite3.Connection) -> bool:
        checks = (
            "SELECT COUNT(*) FROM collection_intervals",
            "SELECT COUNT(*) FROM candidates WHERE status NOT IN ('rejected','approved','superseded')",
            "SELECT COUNT(*) FROM digests WHERE status NOT IN ('approved','superseded')",
            "SELECT COUNT(*) FROM generation_jobs WHERE status NOT IN ('succeeded','superseded')",
            "SELECT COUNT(*) FROM sheet_handoffs WHERE status NOT IN ('delivered','corrupt','manual_required')",
            "SELECT COUNT(*) FROM sheet_remote_operations WHERE finished_at IS NULL",
            "SELECT COUNT(*) FROM sheet_operation_leases WHERE status='active'",
            "SELECT COUNT(*) FROM automation_stream_leases",
            "SELECT COUNT(*) FROM automation_stream_runs WHERE finished_at IS NULL",
            "SELECT COUNT(*) FROM telegram_notification_outbox",
        )
        return all(int(connection.execute(query).fetchone()[0]) == 0 for query in checks)

    @staticmethod
    def _is_runtime_quiescent(connection: sqlite3.Connection) -> bool:
        checks = (
            "SELECT COUNT(*) FROM collection_intervals",
            "SELECT COUNT(*) FROM sheet_remote_operations WHERE finished_at IS NULL",
            "SELECT COUNT(*) FROM sheet_operation_leases WHERE status='active'",
            "SELECT COUNT(*) FROM automation_stream_leases",
            "SELECT COUNT(*) FROM automation_stream_runs WHERE finished_at IS NULL",
            "SELECT COUNT(*) FROM telegram_notification_outbox "
            "WHERE state IN ('pending','claimed','sending','ambiguous','partial_manual_required')",
            "SELECT COUNT(*) FROM ambiguous_digest_windows WHERE state='collecting'",
        )
        return all(int(connection.execute(query).fetchone()[0]) == 0 for query in checks)

    @staticmethod
    def _proposal_state_matches(connection: sqlite3.Connection, proposal: Mapping[str, Any]) -> bool:
        tables = ("candidates", "generation_jobs", "generations", "decision_events", "sheet_handoffs")
        maxima = tuple(
            int(connection.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}").fetchone()[0]) for table in tables
        )
        expected_maxima = tuple(
            int(proposal[column])
            for column in (
                "candidate_max_id",
                "generation_job_max_id",
                "generation_max_id",
                "decision_event_max_id",
                "handoff_max_id",
            )
        )
        cursor = connection.execute(
            "SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'"
        ).fetchone()
        offset = 0 if cursor is None else int(cursor["next_offset"])
        target = connection.execute(
            "SELECT target.target_ref_sha256 FROM sheet_target_bindings target "
            "JOIN sheet_bootstraps bootstrap ON bootstrap.target_binding_id=target.id "
            "WHERE target.id=? AND bootstrap.status='ready'",
            (int(proposal["ready_target_id"]),),
        ).fetchone()
        audience = connection.execute(
            "SELECT id FROM telegram_audience_bindings WHERE id=?",
            (int(proposal["audience_binding_id"]) if "audience_binding_id" in proposal else -1,),
        ).fetchone()
        return (
            maxima == expected_maxima
            and offset == int(proposal["callback_offset"])
            and AutomationAuthority._is_quiescent(connection)
            and int(proposal["nonterminal_job_count"]) == 0
            and int(proposal["outbox_count"]) == 0
            and target is not None
            and str(target["target_ref_sha256"]) == str(proposal["ready_target_fingerprint"])
            and (
                audience is not None
                and sha256(str(int(audience["id"])).encode()).hexdigest() == str(proposal["audience_binding_digest"])
            )
        )

    def persist_proposal(self, proposal: CutoverProposal, *, now: datetime) -> str:
        if len(proposal.frontiers) != 5 or len({item.channel_key_digest for item in proposal.frontiers}) != 5:
            raise ValueError("a cutover proposal requires exactly five distinct frontiers")
        if any(item.upper_message_id < 0 for item in proposal.frontiers):
            raise ValueError("frontier IDs must be nonnegative")
        values = [
            _digest(value)
            for value in (
                proposal.config_digest,
                proposal.cursor_digest,
                proposal.intervals_digest,
                proposal.target_fingerprint,
                proposal.release_digest,
                proposal.audience_digest,
            )
        ]
        frontier_digest = sha256(
            "|".join(
                f"{item.channel_key_digest}:{item.upper_message_id}:{_timestamp(item.captured_at)}"
                for item in sorted(proposal.frontiers, key=lambda item: item.channel_key_digest)
            ).encode()
        ).hexdigest()
        receipt = sha256(
            "|".join(
                [
                    proposal.proposal_id,
                    *values,
                    frontier_digest,
                    *(str(value) for value in proposal.maxima),
                    str(proposal.approval_offset),
                ]
            ).encode()
        ).hexdigest()
        with self.storage.transaction() as connection:
            target = connection.execute(
                "SELECT target.target_ref_sha256 FROM sheet_target_bindings target "
                "JOIN sheet_bootstraps bootstrap ON bootstrap.target_binding_id=target.id "
                "WHERE target.id=? AND bootstrap.status='ready'",
                (proposal.target_id,),
            ).fetchone()
            audience = connection.execute("SELECT id FROM telegram_audience_bindings ORDER BY id").fetchall()
            current_maxima = tuple(
                int(connection.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}").fetchone()[0])
                for table in ("candidates", "generation_jobs", "generations", "decision_events", "sheet_handoffs")
            )
            cursor = connection.execute(
                "SELECT next_offset FROM telegram_update_cursors WHERE stream='approval'"
            ).fetchone()
            current_offset = 0 if cursor is None else int(cursor["next_offset"])
            if (
                current_maxima != proposal.maxima
                or current_offset != proposal.approval_offset
                or not self._is_quiescent(connection)
                or target is None
                or str(target["target_ref_sha256"]) != proposal.target_fingerprint
                or not any(
                    sha256(str(int(row["id"])).encode()).hexdigest() == proposal.audience_digest for row in audience
                )
            ):
                raise AutomationDriftError("cutover preview state is not quiescent")
            connection.execute(
                "INSERT INTO automation_cutover_proposals(id,created_at,expires_at,config_digest,frontiers_digest,cursor_digest,intervals_digest,candidate_max_id,generation_job_max_id,generation_max_id,decision_event_max_id,handoff_max_id,callback_offset,nonterminal_job_count,outbox_count,ready_target_id,ready_target_fingerprint,application_release_digest,audience_binding_digest,proposal_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    proposal.proposal_id,
                    _timestamp(now),
                    _timestamp(now + timedelta(seconds=600)),
                    proposal.config_digest,
                    frontier_digest,
                    proposal.cursor_digest,
                    proposal.intervals_digest,
                    *proposal.maxima,
                    proposal.approval_offset,
                    0,
                    0,
                    proposal.target_id,
                    proposal.target_fingerprint,
                    proposal.release_digest,
                    proposal.audience_digest,
                    receipt,
                ),
            )
            connection.executemany(
                "INSERT INTO automation_proposal_frontiers(proposal_id,channel_key_digest,upper_message_id,captured_at) VALUES(?,?,?,?)",
                [
                    (proposal.proposal_id, item.channel_key_digest, item.upper_message_id, _timestamp(item.captured_at))
                    for item in proposal.frontiers
                ],
            )
        return receipt

    def apply_proposal(
        self,
        proposal_id: str,
        expected_sha256: str,
        *,
        audience_binding_id: int,
        release_digest: str,
        config: object | None = None,
        now: datetime,
        validate: Callable[[], bool],
    ) -> dict[str, object]:
        _digest(expected_sha256)
        _digest(release_digest)
        with self.storage.transaction() as connection:
            active = connection.execute(
                "SELECT cutover.proposal_id,cutover.audience_binding_id,cutover.release_digest,"
                "proposal.proposal_sha256 FROM automation_cutovers cutover "
                "JOIN automation_cutover_proposals proposal ON proposal.id=cutover.proposal_id WHERE cutover.id=1"
            ).fetchone()
            if active is not None:
                frontier_count = int(
                    connection.execute(
                        "SELECT COUNT(DISTINCT channel_key_digest) FROM automation_proposal_frontiers WHERE proposal_id=?",
                        (str(active["proposal_id"]),),
                    ).fetchone()[0]
                )
                if (
                    str(active["proposal_id"]) == proposal_id
                    and str(active["proposal_sha256"]) == expected_sha256
                    and str(active["release_digest"]) == release_digest
                    and int(active["audience_binding_id"]) == audience_binding_id
                    and frontier_count in {5, 6}
                ):
                    return {"changed": False, "status": "active"}
                raise AutomationDriftError("active cutover replay identity drifted")
            proposal = connection.execute(
                "SELECT * FROM automation_cutover_proposals WHERE id=? AND proposal_sha256=?",
                (proposal_id, expected_sha256),
            ).fetchone()
            if (
                proposal is None
                or str(proposal["application_release_digest"]) != release_digest
                or datetime.fromisoformat(str(proposal["expires_at"])).astimezone(UTC) <= now.astimezone(UTC)
            ):
                raise AutomationDriftError("proposal is missing, expired, or for another release")
            proposal_state = dict(proposal)
            proposal_state["audience_binding_id"] = audience_binding_id
            frontier_count = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT channel_key_digest) FROM automation_proposal_frontiers WHERE proposal_id=?",
                    (proposal_id,),
                ).fetchone()[0]
            )
            if frontier_count != 5 or not self._proposal_state_matches(connection, proposal_state) or not validate():
                raise AutomationDriftError("cutover state drifted")
            connection.execute(
                "INSERT INTO automation_cutovers(id,proposal_id,audience_binding_id,target_binding_id,release_digest,activated_at,baseline_candidate_id,baseline_generation_job_id,baseline_generation_id,baseline_decision_event_id,baseline_handoff_id,approval_offset) VALUES(1,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    proposal_id,
                    audience_binding_id,
                    proposal["ready_target_id"],
                    release_digest,
                    _timestamp(now),
                    proposal["candidate_max_id"],
                    proposal["generation_job_max_id"],
                    proposal["generation_max_id"],
                    proposal["decision_event_max_id"],
                    proposal["handoff_max_id"],
                    proposal["callback_offset"],
                ),
            )
            activation = connection.execute(
                "INSERT INTO automation_release_activations(cutover_id,prior_activation_id,release_digest,activated_at) "
                "VALUES(1,NULL,?,?)",
                (release_digest, _timestamp(now)),
            )
            if config is not None:
                if activation.lastrowid is None:
                    raise AutomationDriftError("initial runtime activation failed")
                config_any: Any = config
                policy: Any = getattr(config_any, "news_policy", None)
                fields = getattr(policy, "__dataclass_fields__", {}) if policy is not None else {}
                canonical = json.dumps(
                    {name: getattr(policy, name) for name in sorted(fields)},
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.execute(
                    "INSERT INTO automation_release_config_bindings("
                    "activation_id,config_digest,news_policy_version,canonical_policy_json,created_at"
                    ") VALUES(?,?,?,?,?)",
                    (
                        int(activation.lastrowid),
                        str(config_any.digest),
                        str(policy.version),
                        canonical,
                        _timestamp(now),
                    ),
                )
        return {"changed": True, "status": "active"}

    def activate_release(
        self,
        release_digest: str,
        *,
        config: object,
        now: datetime,
        validate: Callable[[], bool],
    ) -> dict[str, object]:
        """Append the latest immutable release/config pair under quiescence."""
        _digest(release_digest)
        config_any: Any = config
        config_digest, policy_version, canonical = self._canonical_policy(config_any)
        with self.storage.transaction() as connection:
            topology = self.validate_active_topology(connection, config_any, require_binding=False)
            latest = connection.execute(
                "SELECT activation.id,activation.release_digest,binding.config_digest FROM automation_release_activations activation "
                "LEFT JOIN automation_release_config_bindings binding ON binding.activation_id=activation.id "
                "WHERE activation.cutover_id=1 ORDER BY activation.id DESC LIMIT 1"
            ).fetchone()
            if latest is None:
                raise AutomationDriftError("initial runtime activation is missing")
            if (
                compare_digest(str(latest["release_digest"]), release_digest)
                and latest["config_digest"] is not None
                and compare_digest(str(latest["config_digest"]), config_digest)
            ):
                if not topology.config_binding_match:
                    raise AutomationDriftError("runtime release/config binding drifted")
                return {"activation_id": int(latest["id"]), "changed": False, "status": "active"}
            if not self._is_runtime_quiescent(connection) or not validate():
                raise AutomationDriftError("runtime activation state drifted")
            cursor = connection.execute(
                "INSERT INTO automation_release_activations(cutover_id,prior_activation_id,release_digest,activated_at) "
                "VALUES(1,?,?,?)",
                (int(latest["id"]), release_digest, _timestamp(now)),
            )
            if cursor.lastrowid is None:
                raise AutomationDriftError("runtime activation failed")
            activation_id = int(cursor.lastrowid)
            connection.execute(
                "INSERT INTO automation_release_config_bindings("
                "activation_id,config_digest,news_policy_version,canonical_policy_json,created_at"
                ") VALUES(?,?,?,?,?)",
                (activation_id, config_digest, policy_version, canonical, _timestamp(now)),
            )
            return {"activation_id": activation_id, "changed": True, "status": "active"}

    def discover_notification(
        self, notification_id: int, lease: StreamLease, *, now: datetime
    ) -> NotificationClaim | None:
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            cursor = connection.execute(
                "UPDATE telegram_notification_outbox SET state='claimed',claimed_at=? WHERE id=? AND state='pending'",
                (_timestamp(now), notification_id),
            )
            if cursor.rowcount != 1:
                return None
            return NotificationClaim(notification_id, "claimed", lease)

    def prepare_chunk_attempt(
        self, notification_id: int, chunk_id: int, request_sha256: str, lease: StreamLease, *, now: datetime
    ) -> int:
        _digest(request_sha256)
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            row = connection.execute(
                "SELECT state FROM telegram_notification_outbox WHERE id=?", (notification_id,)
            ).fetchone()
            if row is None or str(row["state"]) not in {"claimed", "sending"}:
                raise AutomationDriftError("notification is not dispatchable")
            ordinal = int(
                connection.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 FROM telegram_chunk_attempts WHERE chunk_id=?", (chunk_id,)
                ).fetchone()[0]
            )
            connection.execute("UPDATE telegram_notification_outbox SET state='sending' WHERE id=?", (notification_id,))
            cursor = connection.execute(
                "INSERT INTO telegram_chunk_attempts(chunk_id,ordinal,owner_hash,fence,request_sha256,state,prepared_at) VALUES(?,?,?,?,?,'prepared',?)",
                (chunk_id, ordinal, lease.owner_hash, lease.fence, request_sha256, _timestamp(now)),
            )
            if cursor.lastrowid is None:
                raise AutomationDriftError("attempt identity was not created")
            attempt_id = int(cursor.lastrowid)
            connection.execute(
                "INSERT INTO telegram_notification_events(notification_id,chunk_attempt_id,event_kind,created_at) VALUES(?,?,'prepared',?)",
                (notification_id, attempt_id, _timestamp(now)),
            )
            return attempt_id

    def mark_possibly_sent(self, attempt_id: int, lease: StreamLease, *, now: datetime) -> None:
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            cursor = connection.execute(
                "UPDATE telegram_chunk_attempts SET state='possibly_sent',marked_at=? WHERE id=? AND state='prepared' AND owner_hash=? AND fence=?",
                (_timestamp(now), attempt_id, lease.owner_hash, lease.fence),
            )
            if cursor.rowcount != 1:
                raise AutomationDriftError("attempt cannot be marked")

    def settle_attempt(
        self,
        attempt_id: int,
        outcome: Literal["accepted", "trusted_rejected", "ambiguous", "abandoned_pre_marker"],
        lease: StreamLease,
        *,
        now: datetime,
        accepted_message_id: int | None = None,
        retryable: bool = False,
    ) -> None:
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            cursor = connection.execute(
                "UPDATE telegram_chunk_attempts SET state=?,accepted_message_id=?,settled_at=? WHERE id=? AND owner_hash=? AND fence=? AND state IN ('prepared','possibly_sent')",
                (
                    outcome,
                    accepted_message_id if outcome == "accepted" else None,
                    _timestamp(now),
                    attempt_id,
                    lease.owner_hash,
                    lease.fence,
                ),
            )
            if cursor.rowcount != 1:
                raise AutomationDriftError("attempt cannot be settled")
            notification = connection.execute(
                "SELECT chunk.notification_id,outbox.notification_kind FROM telegram_chunk_attempts attempt "
                "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
                "JOIN telegram_notification_outbox outbox ON outbox.id=chunk.notification_id WHERE attempt.id=?",
                (attempt_id,),
            ).fetchone()
            assert notification is not None
            notification_id = int(notification["notification_id"])
            if outcome == "abandoned_pre_marker":
                connection.execute(
                    "INSERT INTO telegram_chunk_attempt_events(chunk_attempt_id,event_kind,created_at) VALUES(?,?,?)",
                    (attempt_id, outcome, _timestamp(now)),
                )
                connection.execute(
                    "UPDATE callback_tokens SET revoked_at=? WHERE chunk_attempt_id=? "
                    "AND consumed_at IS NULL AND revoked_at IS NULL",
                    (_timestamp(now), attempt_id),
                )
                accepted = connection.execute(
                    "SELECT 1 FROM telegram_chunk_attempts attempt "
                    "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
                    "WHERE chunk.notification_id=? AND attempt.state='accepted' LIMIT 1",
                    (notification_id,),
                ).fetchone()
                state = "pending" if accepted is None else "partial_manual_required"
                connection.execute(
                    "UPDATE telegram_notification_outbox "
                    "SET state=?,claimed_at=NULL,terminal_at=? "
                    "WHERE id=? AND state='sending'",
                    (
                        state,
                        None if state == "pending" else _timestamp(now),
                        notification_id,
                    ),
                )
                if state == "partial_manual_required":
                    connection.execute(
                        "INSERT INTO telegram_notification_events("
                        "notification_id,chunk_attempt_id,event_kind,created_at"
                        ") VALUES(?,?,'partial_manual_required',?)",
                        (notification_id, attempt_id, _timestamp(now)),
                    )
                return
            connection.execute(
                "INSERT INTO telegram_notification_events(notification_id,chunk_attempt_id,event_kind) VALUES(?,?,?)",
                (notification_id, attempt_id, outcome),
            )
            if outcome in {"trusted_rejected", "ambiguous"}:
                accepted = connection.execute(
                    "SELECT 1 FROM telegram_chunk_attempts attempt JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id "
                    "WHERE chunk.notification_id=? AND attempt.state='accepted' LIMIT 1",
                    (notification_id,),
                ).fetchone()
                if (
                    outcome == "trusted_rejected"
                    and retryable
                    and (accepted is None or str(notification["notification_kind"]) == "noon_digest")
                ):
                    state = "pending"
                else:
                    state = (
                        "partial_manual_required"
                        if accepted is not None
                        else ("ambiguous" if outcome == "ambiguous" else "canceled")
                    )
                connection.execute(
                    "UPDATE telegram_notification_outbox SET state=?,claimed_at=NULL,terminal_at=? WHERE id=? AND state='sending'",
                    (state, None if state == "pending" else _timestamp(now), notification_id),
                )
                if outcome == "trusted_rejected":
                    connection.execute(
                        "UPDATE callback_tokens SET revoked_at=? WHERE chunk_attempt_id=? AND consumed_at IS NULL",
                        (_timestamp(now), attempt_id),
                    )
                if state != "pending":
                    connection.execute(
                        "INSERT INTO telegram_notification_events("
                        "notification_id,chunk_attempt_id,event_kind"
                        ") VALUES(?,?,?)",
                        (notification_id, attempt_id, state),
                    )
            if outcome == "accepted":
                notification = connection.execute(
                    "SELECT chunk.notification_id FROM telegram_chunk_attempts attempt "
                    "JOIN telegram_notification_chunks chunk ON chunk.id=attempt.chunk_id WHERE attempt.id=?",
                    (attempt_id,),
                ).fetchone()
                assert notification is not None
                unresolved = connection.execute(
                    "SELECT 1 FROM telegram_notification_chunks chunk WHERE chunk.notification_id=? "
                    "AND NOT EXISTS(SELECT 1 FROM telegram_chunk_attempts attempt "
                    "WHERE attempt.chunk_id=chunk.id AND attempt.state='accepted') LIMIT 1",
                    (notification["notification_id"],),
                ).fetchone()
                if unresolved is None:
                    connection.execute(
                        "UPDATE telegram_notification_outbox SET state='sent',terminal_at=? "
                        "WHERE id=? AND state='sending'",
                        (_timestamp(now), notification["notification_id"]),
                    )

    def resolve_notification(
        self,
        notification_id: int,
        expected_state: Literal["ambiguous", "partial_manual_required"],
        resolution: Literal["resolved_delivered", "resolved_abandoned"],
        *,
        actor_id: int,
        reason_code: Literal["transport_verified", "operator_abandoned"],
        now: datetime,
    ) -> bool:
        with self.storage.transaction() as connection:
            cursor = connection.execute(
                "UPDATE telegram_notification_outbox SET state=?,terminal_at=? WHERE id=? AND state=?",
                (resolution, _timestamp(now), notification_id, expected_state),
            )
            if cursor.rowcount:
                connection.execute(
                    "INSERT INTO telegram_notification_resolutions("
                    "notification_id,expected_status,prior_state,resolution,actor_id,reason_code,created_at"
                    ") VALUES(?,'manual_required',?,?,?,?,?)",
                    (notification_id, expected_state, resolution, actor_id, reason_code, _timestamp(now)),
                )
            if cursor.rowcount and resolution == "resolved_abandoned":
                connection.execute(
                    "UPDATE callback_tokens SET revoked_at=? WHERE notification_id=? AND consumed_at IS NULL",
                    (_timestamp(now), notification_id),
                )
            if cursor.rowcount:
                connection.execute(
                    "INSERT INTO telegram_notification_events(notification_id,event_kind,created_at) VALUES(?,?,?)",
                    (notification_id, resolution, _timestamp(now)),
                )
            return cursor.rowcount == 1

    def safe_status(self) -> dict[str, int | bool]:
        """Return counts only; this intentionally omits identifiers and content."""

        def count(query: str) -> int:
            row = self.storage.fetch_one(query)
            if row is None:
                raise AutomationDriftError("automation status query failed")
            return int(row["count"])

        return {
            "cutover_active": self.storage.fetch_one("SELECT 1 AS present FROM automation_cutovers WHERE id=1")
            is not None,
            "open_leases": count("SELECT COUNT(*) AS count FROM automation_stream_leases"),
            "open_runs": count("SELECT COUNT(*) AS count FROM automation_stream_runs WHERE finished_at IS NULL"),
            "pending_notifications": count(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox "
                "WHERE state IN ('pending','claimed','sending')"
            ),
            "ambiguous_notifications": count(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox WHERE state='ambiguous'"
            ),
            "partial_notifications": count(
                "SELECT COUNT(*) AS count FROM telegram_notification_outbox WHERE state='partial_manual_required'"
            ),
        }

    def quiescent(self) -> bool:
        status = self.safe_status()
        return status["open_leases"] == 0 and status["open_runs"] == 0 and status["pending_notifications"] == 0

    @staticmethod
    def audience_hmac(
        bot_token: str, chat_id: str, authorized_user_ids: tuple[str, ...], callback_actor_id: str
    ) -> tuple[str, str]:
        """Compute opaque, domain-separated runtime audience proofs."""
        if callback_actor_id not in authorized_user_ids or len(set(authorized_user_ids)) != len(authorized_user_ids):
            raise ValueError("callback actor must be one distinct authorized actor")
        token = hmac_new(bot_token.encode(), b"newsbot/automation/audience-token/v1", "sha256").hexdigest()
        canonical = "\n".join((chat_id, *sorted(authorized_user_ids), callback_actor_id)).encode()
        audience = hmac_new(bot_token.encode(), b"newsbot/automation/audience/v1\0" + canonical, "sha256").hexdigest()
        return token, audience

    def validate_active_audience(self, *, bot_id_digest: str, token_hmac: str, audience_hmac: str) -> bool:
        """Compare runtime proofs with the immutable activated audience binding."""
        _digest(bot_id_digest)
        _digest(token_hmac)
        _digest(audience_hmac)
        row = self.storage.fetch_one(
            "SELECT binding.bot_id_digest,binding.token_hmac,binding.audience_hmac FROM automation_cutovers cutover "
            "JOIN telegram_audience_bindings binding ON binding.id=cutover.audience_binding_id WHERE cutover.id=1"
        )
        return row is not None and all(
            compare_digest(str(row[key]), value)
            for key, value in (
                ("bot_id_digest", bot_id_digest),
                ("token_hmac", token_hmac),
                ("audience_hmac", audience_hmac),
            )
        )

    def active_cutover_snapshot(self) -> dict[str, int | str] | None:
        """Return release and baseline values only; never target or audience identity."""
        row = self.storage.fetch_one(
            "SELECT release_digest,baseline_candidate_id,baseline_generation_job_id,baseline_generation_id,"
            "baseline_decision_event_id,baseline_handoff_id,approval_offset FROM automation_cutovers WHERE id=1"
        )
        return None if row is None else {key: row[key] for key in row}

    def active_frontiers(self) -> tuple[Frontier, ...]:
        rows = self.storage.fetch_all(
            "SELECT frontier.channel_key_digest,frontier.upper_message_id,frontier.captured_at FROM automation_cutovers cutover "
            "JOIN automation_proposal_frontiers frontier ON frontier.proposal_id=cutover.proposal_id WHERE cutover.id=1 ORDER BY frontier.channel_key_digest"
        )
        return tuple(
            Frontier(
                str(row["channel_key_digest"]),
                int(row["upper_message_id"]),
                datetime.fromisoformat(str(row["captured_at"])).astimezone(UTC),
            )
            for row in rows
        )

    def next_chunk(
        self, notification_id: int, lease: StreamLease, *, now: datetime
    ) -> tuple[int, int, str, bool] | None:
        """Select the first immutable chunk that has no accepted attempt."""
        with self.storage.transaction(immediate=False) as connection:
            self._current_lease(connection, lease, now)
            row = connection.execute(
                "SELECT chunk.id,chunk.chunk_index,chunk.template_digest,chunk.has_buttons FROM telegram_notification_chunks chunk "
                "WHERE chunk.notification_id=? AND NOT EXISTS(SELECT 1 FROM telegram_chunk_attempts attempt WHERE attempt.chunk_id=chunk.id AND attempt.state='accepted') ORDER BY chunk.chunk_index LIMIT 1",
                (notification_id,),
            ).fetchone()
            return (
                None
                if row is None
                else (int(row["id"]), int(row["chunk_index"]), str(row["template_digest"]), bool(row["has_buttons"]))
            )

    def link_callback(
        self, callback_token_id: int, notification_id: int, attempt_id: int, lease: StreamLease, *, now: datetime
    ) -> bool:
        """Link an existing callback token without reading or returning its raw value."""
        with self.storage.transaction() as connection:
            self._current_lease(connection, lease, now)
            cursor = connection.execute(
                "UPDATE callback_tokens SET notification_id=?,chunk_attempt_id=? WHERE id=? AND notification_id IS NULL AND chunk_attempt_id IS NULL",
                (notification_id, attempt_id, callback_token_id),
            )
            if cursor.rowcount:
                connection.execute(
                    "INSERT INTO telegram_notification_events(notification_id,chunk_attempt_id,event_kind) VALUES(?,?,'callback_linked')",
                    (notification_id, attempt_id),
                )
            return cursor.rowcount == 1

    def generation_authority(self, generation_job_id: int) -> bool:
        return (
            self.storage.fetch_one(
                "SELECT 1 FROM automation_generation_authority authority JOIN automation_cutovers cutover ON cutover.id=authority.cutover_id "
                "WHERE authority.generation_job_id=? AND authority.cutover_id=1 AND ? > cutover.baseline_generation_job_id",
                (generation_job_id, generation_job_id),
            )
            is not None
        )

    def create_notification_chunks(self, notification_id: int, chunks: tuple[tuple[int, str, bool], ...]) -> None:
        """Persist deterministic chunk metadata once, before any request is prepared."""
        if not chunks or any(length < 1 or length > 4096 or not _digest(digest) for length, digest, _ in chunks):
            raise ValueError("chunks require UTF-16 lengths and digest-only templates")
        if any(buttons for _, _, buttons in chunks[:-1]):
            raise ValueError("buttons belong only to the final chunk")
        with self.storage.transaction() as connection:
            parent = connection.execute(
                "SELECT state FROM telegram_notification_outbox WHERE id=?", (notification_id,)
            ).fetchone()
            if parent is None or str(parent["state"]) not in {"pending", "claimed", "sending"}:
                raise AutomationDriftError("chunks require a dispatchable notification")
            existing = int(
                connection.execute(
                    "SELECT COUNT(*) FROM telegram_notification_chunks WHERE notification_id=?", (notification_id,)
                ).fetchone()[0]
            )
            if existing:
                rows = tuple(
                    connection.execute(
                        "SELECT utf16_length,template_digest,has_buttons FROM telegram_notification_chunks "
                        "WHERE notification_id=? ORDER BY chunk_index",
                        (notification_id,),
                    )
                )
                persisted = tuple(
                    (int(row["utf16_length"]), str(row["template_digest"]), bool(row["has_buttons"])) for row in rows
                )
                if persisted != chunks:
                    raise AutomationDriftError("notification chunk identity drifted")
                return
            connection.executemany(
                "INSERT INTO telegram_notification_chunks(notification_id,chunk_index,utf16_length,template_digest,has_buttons) VALUES(?,?,?,?,?)",
                [
                    (notification_id, index, length, digest, int(buttons))
                    for index, (length, digest, buttons) in enumerate(chunks)
                ],
            )

    def post_baseline_handoff_ids(self, limit: int, *, now: datetime | None = None) -> tuple[int, ...]:
        """Select only post-cutover handoffs; delivery keeps its own effect authority."""
        if limit < 1:
            raise ValueError("limit must be positive")
        rows = self.storage.fetch_all(
            "SELECT handoff.id FROM automation_cutovers cutover JOIN sheet_handoffs handoff "
            "ON handoff.id>cutover.baseline_handoff_id AND handoff.target_binding_id=cutover.target_binding_id "
            "WHERE cutover.id=1 AND (handoff.status='pending' OR "
            "(handoff.status='retryable' AND aware_epoch_us(handoff.retry_at)<=aware_epoch_us(?))) "
            "ORDER BY handoff.id LIMIT ?",
            (_timestamp(now or datetime.now(UTC)), limit),
        )
        return tuple(int(row["id"]) for row in rows)

    def defer_authority(self, candidate_id: int, stage: Literal["selection", "review"], due_at: datetime) -> bool:
        return (
            self.storage.fetch_one(
                "SELECT 1 FROM automation_defer_authority WHERE cutover_id=1 AND candidate_id=? AND stage=? AND due_at=?",
                (candidate_id, stage, _timestamp(due_at)),
            )
            is not None
        )

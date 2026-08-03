"""SQLite persistence primitives for the local-first news bot."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import RLock
from typing import Any, cast
from zoneinfo import ZoneInfo

from .collectors.base import Engagement, Media, MessageKind, SourceObservation, UrlCandidate

_OUTBOX_V007_COLUMNS = ("id", "audience_binding_id", "cutover_id", "notification_kind", "candidate_id", "generation_id", "defer_authority_id", "source_set_key", "stage", "subject_digest", "state", "created_at", "claimed_at", "terminal_at")
_OUTBOX_TRIGGER_NAMES = ("telegram_outbox_identity_immutable", "telegram_outbox_no_delete", "telegram_outbox_transitions", "telegram_outbox_terminal_immutable")
_OUTBOX_INDEX_NAMES = ("telegram_notification_candidate_unique", "telegram_notification_review_unique", "telegram_notification_resume_unique")
_OUTBOX_DEPENDENTS = (
    ("callback_tokens", "notification_id"),
    ("automation_defer_authority", "notification_id"),
    ("telegram_notification_chunks", "notification_id"),
    ("telegram_notification_events", "notification_id"),
    ("telegram_notification_resolutions", "notification_id"),
)
_OUTBOX_V008_DDL = """CREATE TABLE telegram_notification_outbox_v008 (
id INTEGER PRIMARY KEY, audience_binding_id INTEGER NOT NULL REFERENCES telegram_audience_bindings(id) ON DELETE RESTRICT, cutover_id INTEGER NOT NULL REFERENCES automation_cutovers(id) ON DELETE RESTRICT,
notification_kind TEXT NOT NULL CHECK(notification_kind IN ('candidate','review','resume','noon_digest')), candidate_id INTEGER REFERENCES candidates(id) ON DELETE RESTRICT, generation_id INTEGER REFERENCES generations(id) ON DELETE RESTRICT, defer_authority_id INTEGER REFERENCES automation_defer_authority(id) ON DELETE RESTRICT, source_set_key TEXT, stage TEXT CHECK(stage IN ('selection','review')), ambiguous_window_id INTEGER REFERENCES ambiguous_digest_windows(id) ON DELETE RESTRICT, subject_digest TEXT NOT NULL CHECK(length(subject_digest)=64), state TEXT NOT NULL CHECK(state IN ('pending','claimed','sending','sent','canceled','ambiguous','partial_manual_required','resolved_delivered','resolved_abandoned')), created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, claimed_at TEXT, terminal_at TEXT,
CHECK((notification_kind='candidate' AND candidate_id IS NOT NULL AND source_set_key IS NOT NULL AND generation_id IS NULL AND defer_authority_id IS NULL AND ambiguous_window_id IS NULL AND stage IS NULL) OR (notification_kind='review' AND generation_id IS NOT NULL AND candidate_id IS NULL AND defer_authority_id IS NULL AND ambiguous_window_id IS NULL AND source_set_key IS NULL AND stage IS NULL) OR (notification_kind='resume' AND defer_authority_id IS NOT NULL AND stage IS NOT NULL AND candidate_id IS NULL AND generation_id IS NULL AND ambiguous_window_id IS NULL AND source_set_key IS NULL) OR (notification_kind='noon_digest' AND ambiguous_window_id IS NOT NULL AND candidate_id IS NULL AND generation_id IS NULL AND defer_authority_id IS NULL AND source_set_key IS NULL AND stage IS NULL)))"""
_OUTBOX_V008_OBJECTS = """CREATE UNIQUE INDEX telegram_notification_candidate_unique ON telegram_notification_outbox(audience_binding_id,source_set_key) WHERE notification_kind='candidate';
CREATE UNIQUE INDEX telegram_notification_review_unique ON telegram_notification_outbox(audience_binding_id,generation_id) WHERE notification_kind='review';
CREATE UNIQUE INDEX telegram_notification_resume_unique ON telegram_notification_outbox(audience_binding_id,defer_authority_id,stage) WHERE notification_kind='resume';
CREATE UNIQUE INDEX telegram_notification_noon_unique ON telegram_notification_outbox(audience_binding_id,ambiguous_window_id) WHERE notification_kind='noon_digest';
CREATE TRIGGER telegram_outbox_identity_immutable BEFORE UPDATE OF id,audience_binding_id,cutover_id,notification_kind,candidate_id,generation_id,defer_authority_id,source_set_key,stage,ambiguous_window_id,subject_digest,created_at ON telegram_notification_outbox WHEN NEW.id IS NOT OLD.id OR NEW.audience_binding_id IS NOT OLD.audience_binding_id OR NEW.cutover_id IS NOT OLD.cutover_id OR NEW.notification_kind IS NOT OLD.notification_kind OR NEW.candidate_id IS NOT OLD.candidate_id OR NEW.generation_id IS NOT OLD.generation_id OR NEW.defer_authority_id IS NOT OLD.defer_authority_id OR NEW.source_set_key IS NOT OLD.source_set_key OR NEW.stage IS NOT OLD.stage OR NEW.ambiguous_window_id IS NOT OLD.ambiguous_window_id OR NEW.subject_digest IS NOT OLD.subject_digest OR NEW.created_at IS NOT OLD.created_at BEGIN SELECT RAISE(ABORT,'notification identity is immutable'); END;
CREATE TRIGGER telegram_outbox_no_delete BEFORE DELETE ON telegram_notification_outbox BEGIN SELECT RAISE(ABORT,'notifications cannot be deleted'); END;
CREATE TRIGGER telegram_outbox_transitions BEFORE UPDATE OF state ON telegram_notification_outbox WHEN NOT ((OLD.state IN ('pending','claimed','sending') AND NEW.state IN ('pending','claimed','sending','sent','canceled','ambiguous','partial_manual_required')) OR (OLD.state IN ('sent','ambiguous','partial_manual_required') AND NEW.state IN ('resolved_delivered','resolved_abandoned'))) BEGIN SELECT RAISE(ABORT,'invalid notification transition'); END;
CREATE TRIGGER telegram_outbox_terminal_immutable BEFORE UPDATE ON telegram_notification_outbox WHEN OLD.state IN ('sent','canceled','resolved_delivered','resolved_abandoned') BEGIN SELECT RAISE(ABORT,'terminal notification immutable'); END;"""

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def aware_epoch_us(value: object) -> int | None:
    """Return a strict aware ISO timestamp as UTC microseconds, or ``None``."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        delta = parsed.astimezone(UTC) - _EPOCH
    except (OverflowError, ValueError):
        return None
    return ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds


class Storage:
    """A small, explicit wrapper around one SQLite connection.

    Callers should group related writes with :meth:`transaction`; read helpers
    return ``sqlite3.Row`` objects so column access remains named and direct.
    """

    def __init__(self, database_path: str | Path) -> None:
        path = str(database_path)
        self._is_memory = path == ":memory:"
        if path != ":memory:":
            Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = RLock()
        self._lease_authority_hash: str | None = None
        self._lease_authority_fence: int | None = None
        self._defer_authority: tuple[int, str | None, str | None] | None = None
        self._configure()

    @classmethod
    def open(cls, database_path: str | Path) -> Storage:
        """Open a database and apply all bundled migrations."""
        storage = cls(database_path)
        storage.migrate()
        return storage

    def _configure(self) -> None:
        with self._lock:
            self._connection.create_function(
                "automation_defer_authorized",
                3,
                lambda candidate_id, stage, due_at: int(self._defer_authority == (int(candidate_id), stage, due_at)),
            )
            self._connection.create_function(
                "sha256_hex",
                1,
                lambda value: sha256(bytes(value)).hexdigest(),
                deterministic=True,
            )
            self._connection.create_function(
                "lease_owner_hash",
                0,
                lambda: self._lease_authority_hash,
            )
            self._connection.create_function("lease_fence", 0, lambda: self._lease_authority_fence)
            self._connection.create_function(
                "aware_epoch_us",
                1,
                aware_epoch_us,
                deterministic=True,
            )
            journal_mode = str(self._connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
            self._connection.execute("PRAGMA synchronous = FULL")
            synchronous = int(self._connection.execute("PRAGMA synchronous").fetchone()[0])
            self._connection.execute("PRAGMA foreign_keys = ON")
            foreign_keys = int(self._connection.execute("PRAGMA foreign_keys").fetchone()[0])
            self._connection.execute("PRAGMA busy_timeout = 5000")
            busy_timeout = int(self._connection.execute("PRAGMA busy_timeout").fetchone()[0])
            expected_journal_mode = "memory" if self._is_memory else "wal"
            if (journal_mode, synchronous, foreign_keys, busy_timeout) != (
                expected_journal_mode,
                2,
                1,
                5000,
            ):
                raise RuntimeError("SQLite authority pragmas were not applied")

    def migrate(self) -> None:
        """Apply each numbered SQL migration once, in filename order."""
        migration_dir = Path(__file__).with_name("migrations")
        migrations = sorted(migration_dir.glob("[0-9][0-9][0-9]_*.sql"))
        if not migrations:
            raise RuntimeError(f"No migrations found in {migration_dir}")

        with self._lock:
            if self._connection.in_transaction:
                raise RuntimeError("Cannot migrate inside an active transaction")
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version TEXT PRIMARY KEY, "
                "applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
            )
            applied = {row["version"] for row in self._connection.execute("SELECT version FROM schema_migrations")}
            for migration in migrations:
                if migration.name in applied:
                    continue
                script = migration.read_text(encoding="utf-8")
                if migration.name == "004_sheets_authority_upgrade.sql":
                    self._assert_sheets_authority_upgrade_supported(migration_dir / "003_sheets_handoff.sql")
                    handoff_columns = {
                        str(row["name"]) for row in self._connection.execute("PRAGMA table_info(sheet_handoffs)")
                    }
                    target_expression = (
                        "h.target_binding_id" if "target_binding_id" in handoff_columns else "b.target_binding_id"
                    )
                    script = script.replace("__HANDOFF_TARGET_EXPR__", target_expression)
                if migration.name == "008_hourly_news_eligibility.sql":
                    self._migrate_hourly_news_eligibility(migration, script)
                    continue
                version = migration.name.replace("'", "''")
                foreign_keys_disabled = migration.name in {
                    "002_canonical_authority.sql",
                    "004_sheets_authority_upgrade.sql",
                }
                if migration.name == "002_canonical_authority.sql":
                    self._prepare_canonical_authority_upgrade()
                if foreign_keys_disabled:
                    self._connection.execute("PRAGMA foreign_keys = OFF")
                try:
                    if migration.name in {"004_sheets_authority_upgrade.sql", "007_systemd_automation.sql"}:
                        self._connection.executescript(f"BEGIN IMMEDIATE;\n{script}\n")
                        violations = self._connection.execute("PRAGMA foreign_key_check").fetchall()
                        if violations:
                            raise RuntimeError(f"{migration.name} introduced foreign key violations")
                        self._connection.execute(
                            "INSERT INTO schema_migrations (version) VALUES (?)",
                            (migration.name,),
                        )
                        self._connection.commit()
                    else:
                        self._connection.executescript(
                            "BEGIN IMMEDIATE;\n"
                            f"{script}\n"
                            "INSERT INTO schema_migrations (version) "
                            f"VALUES ('{version}');\n"
                            "COMMIT;"
                        )
                except BaseException:
                    self._connection.rollback()
                    raise
                finally:
                    if migration.name == "004_sheets_authority_upgrade.sql":
                        self._connection.execute("PRAGMA legacy_alter_table = OFF")
                    if foreign_keys_disabled:
                        self._connection.execute("PRAGMA foreign_keys = ON")
                    if migration.name == "002_canonical_authority.sql":
                        violations = self._connection.execute("PRAGMA foreign_key_check").fetchall()
                        if violations:
                            raise RuntimeError(f"{migration.name} introduced foreign key violations")

    def _prepare_canonical_authority_upgrade(self) -> None:
        """Add columns SQLite cannot add conditionally from a SQL migration."""
        callback_columns = {str(row["name"]) for row in self._connection.execute("PRAGMA table_info(callback_tokens)")}
        if "revoked_at" not in callback_columns:
            self._connection.execute("ALTER TABLE callback_tokens ADD COLUMN revoked_at TEXT")

    def _assert_sheets_authority_upgrade_supported(self, migration_path: Path) -> None:
        """Fail closed when an applied 003 is not the supported authority shape."""
        authority_tables = (
            "sheet_bootstraps",
            "sheet_handoff_bindings",
            "sheet_operation_events",
            "sheet_operation_leases",
            "sheet_operation_probes",
            "sheet_operation_settlements",
            "sheet_remote_operations",
            "sheet_target_bindings",
        )
        placeholders = ",".join("?" for _ in authority_tables)
        query = (
            "SELECT type,name,tbl_name,sql FROM sqlite_master "
            "WHERE type IN ('table','trigger','index') AND tbl_name IN ("
            f"{placeholders}) AND sql IS NOT NULL ORDER BY type,name"
        )
        installed = [tuple(row) for row in self._connection.execute(query, authority_tables)]

        expected_connection = sqlite3.connect(":memory:")
        try:
            expected_connection.create_function(
                "sha256_hex",
                1,
                lambda value: sha256(bytes(value)).hexdigest(),
                deterministic=True,
            )
            expected_connection.create_function("lease_owner_hash", 0, lambda: None)
            expected_connection.create_function("aware_epoch_us", 1, aware_epoch_us, deterministic=True)
            expected_connection.executescript(
                "CREATE TABLE generations(id INTEGER PRIMARY KEY);"
                "CREATE TABLE decision_events(id INTEGER PRIMARY KEY);" + migration_path.read_text(encoding="utf-8")
            )
            expected = [tuple(row) for row in expected_connection.execute(query, authority_tables)]
        finally:
            expected_connection.close()
        if installed != expected:
            raise RuntimeError("unsupported applied 003 Sheets authority schema; automatic upgrade refused")

    def _migrate_hourly_news_eligibility(self, migration: Path, script: str) -> None:
        if self._connection.in_transaction or not int(self._connection.execute("PRAGMA foreign_keys").fetchone()[0]):
            raise RuntimeError("Migration 008 requires an idle connection with foreign keys ON")
        oracle = self._build_hourly_news_oracle(migration.parent, script)
        if self._outbox_schema_attestation(self._connection) != oracle["v007"]:
            raise RuntimeError("Unsupported migration-007 outbox schema; automatic upgrade refused")
        foreign_keys = int(self._connection.execute("PRAGMA foreign_keys").fetchone()[0])
        legacy = int(self._connection.execute("PRAGMA legacy_alter_table").fetchone()[0])
        try:
            self._connection.execute("PRAGMA foreign_keys=OFF")
            if int(self._connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 0:
                raise RuntimeError("Migration 008 could not disable foreign keys")
            self._connection.execute("PRAGMA legacy_alter_table=ON")
            if int(self._connection.execute("PRAGMA legacy_alter_table").fetchone()[0]) != 1:
                raise RuntimeError("Migration 008 could not enable legacy alter table")
            self._connection.execute("BEGIN IMMEDIATE")
            if self._outbox_schema_attestation(self._connection) != oracle["v007"]:
                raise RuntimeError("Unsupported migration-007 outbox schema; automatic upgrade refused")
            parity = self._outbox_history_parity(self._connection)
            columns = ",".join(_OUTBOX_V007_COLUMNS)
            self._execute_sql_script(script)
            self._connection.execute(_OUTBOX_V008_DDL)
            self._connection.execute(
                f"INSERT INTO telegram_notification_outbox_v008({columns},ambiguous_window_id) "
                f"SELECT {columns},NULL FROM telegram_notification_outbox"
            )
            if self._table_digest(self._connection, "telegram_notification_outbox_v008", _OUTBOX_V007_COLUMNS) != parity["telegram_notification_outbox"]:
                raise RuntimeError("Migration 008 outbox copy parity failed")
            for name in _OUTBOX_TRIGGER_NAMES + _OUTBOX_INDEX_NAMES:
                kind = "TRIGGER" if name in _OUTBOX_TRIGGER_NAMES else "INDEX"
                self._connection.execute(f"DROP {kind} {name}")
            self._connection.execute("DROP TABLE telegram_notification_outbox")
            self._connection.execute("ALTER TABLE telegram_notification_outbox_v008 RENAME TO telegram_notification_outbox")
            self._execute_sql_script(_OUTBOX_V008_OBJECTS)
            self._assert_outbox_history_parity(parity, "pre-commit")
            self._assert_inbound_outbox_rows()
            self._assert_hourly_news_v008_schema(oracle["v008"])
            self._assert_no_foreign_key_violations()
            self._connection.execute("INSERT INTO schema_migrations(version) VALUES(?)", (migration.name,))
            self._assert_no_foreign_key_violations()
            self._connection.commit()
            self._assert_outbox_history_parity(parity, "post-commit")
            self._assert_inbound_outbox_rows()
            self._assert_hourly_news_v008_schema(oracle["v008"])
            self._assert_no_foreign_key_violations()
        except BaseException:
            self._connection.rollback()
            raise
        finally:
            self._connection.execute(f"PRAGMA legacy_alter_table={legacy}")
            self._connection.execute(f"PRAGMA foreign_keys={foreign_keys}")
            if int(self._connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
                raise RuntimeError("Migration 008 did not restore foreign keys")

    def _build_hourly_news_oracle(self, migration_dir: Path, script: str) -> dict[str, object]:
        """Build supported 007 and exact 008 shapes from the bundled migrations."""
        oracle = sqlite3.connect(":memory:")
        oracle.row_factory = sqlite3.Row
        oracle.create_function("automation_defer_authorized", 3, lambda *_: 0)
        oracle.create_function("sha256_hex", 1, lambda value: sha256(bytes(value)).hexdigest(), deterministic=True)
        oracle.create_function("lease_owner_hash", 0, lambda: None)
        oracle.create_function("lease_fence", 0, lambda: None)
        oracle.create_function("aware_epoch_us", 1, aware_epoch_us, deterministic=True)
        try:
            for path in sorted(migration_dir.glob("00[1-7]_*.sql")):
                sql = path.read_text(encoding="utf-8")
                if path.name in {"002_canonical_authority.sql", "004_sheets_authority_upgrade.sql"}:
                    oracle.execute("PRAGMA foreign_keys=OFF")
                if path.name == "004_sheets_authority_upgrade.sql":
                    sql = sql.replace("__HANDOFF_TARGET_EXPR__", "b.target_binding_id")
                oracle.executescript(sql)
                oracle.execute("INSERT INTO schema_migrations(version) VALUES(?)", (path.name,))
                oracle.commit()
                if path.name in {"002_canonical_authority.sql", "004_sheets_authority_upgrade.sql"}:
                    oracle.execute("PRAGMA foreign_keys=ON")
            v007 = self._outbox_schema_attestation(oracle)
            oracle.execute("PRAGMA foreign_keys=OFF")
            oracle.execute("PRAGMA legacy_alter_table=ON")
            oracle.execute("BEGIN IMMEDIATE")
            self._execute_sql_script_on(oracle, script)
            oracle.execute(_OUTBOX_V008_DDL)
            for name in _OUTBOX_TRIGGER_NAMES + _OUTBOX_INDEX_NAMES:
                oracle.execute(f"DROP {'TRIGGER' if name in _OUTBOX_TRIGGER_NAMES else 'INDEX'} {name}")
            oracle.execute("DROP TABLE telegram_notification_outbox")
            oracle.execute("ALTER TABLE telegram_notification_outbox_v008 RENAME TO telegram_notification_outbox")
            self._execute_sql_script_on(oracle, _OUTBOX_V008_OBJECTS)
            return {"v007": v007, "v008": self._schema_inventory(oracle)}
        finally:
            oracle.close()

    def _outbox_schema_attestation(self, connection: sqlite3.Connection) -> tuple[object, ...]:
        columns = tuple(tuple(row) for row in connection.execute("PRAGMA table_info(telegram_notification_outbox)"))
        foreign_keys = tuple(tuple(row) for row in connection.execute("PRAGMA foreign_key_list(telegram_notification_outbox)"))
        indexes = tuple(tuple(row) for row in connection.execute("PRAGMA index_list(telegram_notification_outbox)"))
        objects = self._schema_inventory(connection)
        inbound = tuple(
            (table, column)
            for table, column in _OUTBOX_DEPENDENTS
            if any(str(row["table"]) == "telegram_notification_outbox" and str(row["from"]) == column
                   for row in connection.execute(f"PRAGMA foreign_key_list({table})"))
        )
        return columns, foreign_keys, indexes, objects, inbound

    def _schema_inventory(self, connection: sqlite3.Connection) -> tuple[tuple[object, ...], ...]:
        hourly_tables = (
            "telegram_notification_outbox",
            "automation_release_config_bindings",
            "news_policy_evaluations",
            "ambiguous_digest_windows",
            "ambiguous_digest_items",
        )
        marks = ",".join("?" for _ in hourly_tables)
        return tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations' "
                f"AND (tbl_name IN ({marks}) OR instr(lower(sql),'telegram_notification_outbox')>0) "
                "ORDER BY type,name",
                hourly_tables,
            )
        )

    def _outbox_history_parity(self, connection: sqlite3.Connection) -> dict[str, tuple[int, str]]:
        return {
            "telegram_notification_outbox": self._table_digest(connection, "telegram_notification_outbox", _OUTBOX_V007_COLUMNS),
            **{table: self._table_digest(connection, table) for table, _ in _OUTBOX_DEPENDENTS},
        }

    def _table_digest(self, connection: sqlite3.Connection, table: str, columns: Sequence[str] | None = None) -> tuple[int, str]:
        selected = tuple(columns or tuple(str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")))
        rows = connection.execute(f"SELECT {','.join(selected)} FROM {table} ORDER BY id").fetchall()
        encoded = json.dumps([list(row) for row in rows], ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        return len(rows), sha256(encoded).hexdigest()

    def _assert_outbox_history_parity(self, expected: dict[str, tuple[int, str]], stage: str) -> None:
        actual = self._outbox_history_parity(self._connection)
        if actual != expected:
            raise RuntimeError(f"Migration 008 {stage} history parity failed")

    def _assert_inbound_outbox_rows(self) -> None:
        for table, column in _OUTBOX_DEPENDENTS:
            unresolved = self._connection.execute(
                f"SELECT COUNT(*) FROM {table} dependent LEFT JOIN telegram_notification_outbox outbox "
                f"ON outbox.id=dependent.{column} WHERE dependent.{column} IS NOT NULL AND outbox.id IS NULL"
            ).fetchone()[0]
            if unresolved:
                raise RuntimeError(f"Migration 008 orphaned {table}.{column}")

    def _assert_hourly_news_v008_schema(self, expected: object) -> None:
        if self._schema_inventory(self._connection) != expected:
            raise RuntimeError("Migration 008 schema parity failed")

    def _assert_no_foreign_key_violations(self) -> None:
        if self._connection.execute("PRAGMA foreign_key_check").fetchall():
            raise RuntimeError("Migration 008 introduced foreign key violations")

    @staticmethod
    def _execute_sql_script_on(connection: sqlite3.Connection, script: str) -> None:
        statement = ""
        for line in script.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                connection.execute(statement)
                statement = ""

    def _execute_sql_script(self, script: str) -> None:
        statement = ""
        for line in script.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                self._connection.execute(statement)
                statement = ""

    def create_release_config_binding(self, activation_id: int, config_digest: str, news_policy_version: str,
                                      canonical_policy_json: str, *, created_at: datetime) -> int:
        if len(config_digest) != 64:
            raise ValueError("config_digest must be a SHA-256 digest")
        with self.transaction() as connection:
            return cast(int, connection.execute(
                "INSERT INTO automation_release_config_bindings(activation_id,config_digest,news_policy_version,canonical_policy_json,created_at) VALUES(?,?,?,?,?)",
                (activation_id, config_digest, news_policy_version, canonical_policy_json, _timestamp(created_at)),
            ).lastrowid)

    def record_news_policy_evaluation(self, candidate_evaluation_id: int, config_binding_id: int, outcome: str,
                                      reason: str, rationale_json: str, *, created_at: datetime) -> int:
        with self.transaction() as connection:
            return self._record_news_policy_evaluation(
                connection,
                candidate_evaluation_id,
                config_binding_id,
                outcome,
                reason,
                rationale_json,
                created_at=created_at,
            )

    @staticmethod
    def _record_news_policy_evaluation(
        connection: sqlite3.Connection,
        candidate_evaluation_id: int,
        config_binding_id: int,
        outcome: str,
        reason: str,
        rationale_json: str,
        *,
        created_at: datetime,
    ) -> int:
        return cast(int, connection.execute(
            "INSERT INTO news_policy_evaluations(candidate_evaluation_id,config_binding_id,outcome,reason,rationale_json,created_at) VALUES(?,?,?,?,?,?)",
            (candidate_evaluation_id, config_binding_id, outcome, reason, rationale_json, _timestamp(created_at)),
        ).lastrowid)

    def create_ambiguous_digest_window(self, scheduled_local_date: date, config_binding_id: int, *,
                                       created_at: datetime) -> int:
        opens_at = datetime.combine(scheduled_local_date, datetime.min.time(), ZoneInfo("Asia/Seoul")).replace(hour=12)
        with self.transaction() as connection:
            return cast(int, connection.execute(
                "INSERT INTO ambiguous_digest_windows(scheduled_local_date,config_binding_id,opens_at,closes_at,state,created_at) VALUES(?,?,?,?,?,?)",
                (scheduled_local_date.isoformat(), config_binding_id, _timestamp(opens_at), _timestamp(opens_at + timedelta(hours=1)), "collecting", _timestamp(created_at)),
            ).lastrowid)

    def add_ambiguous_digest_item(self, window_id: int, news_policy_evaluation_id: int, source_post_version_id: int,
                                  normalized_title: str, ordering_timestamp: datetime, story_key: str, content_key: str,
                                  *, created_at: datetime) -> int:
        with self.transaction() as connection:
            return cast(int, connection.execute(
                "INSERT INTO ambiguous_digest_items(window_id,news_policy_evaluation_id,source_post_version_id,normalized_title,ordering_timestamp,story_key,content_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (window_id, news_policy_evaluation_id, source_post_version_id, normalized_title, _timestamp(ordering_timestamp), story_key, content_key, _timestamp(created_at)),
            ).lastrowid)
    def _authorize_lease(self, owner_token: str, fence: int | None = None) -> None:
        """Authorize one transaction to write events for the named lease owner."""
        self._lease_authority_hash = sha256(owner_token.encode()).hexdigest()
        self._lease_authority_fence = fence

    def authorize_defer_transition(
        self, candidate_id: int, stage: str | None, due_at: str | None, owner_token: str, fence: int
    ) -> None:
        """Authorize one exact deferred transition within the active transaction."""
        if not self._connection.in_transaction:
            raise RuntimeError("defer authorization requires storage.transaction()")
        self._authorize_lease(owner_token, fence)
        self._defer_authority = (candidate_id, stage, due_at)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        """Yield the connection inside an all-or-nothing transaction."""
        begin = "BEGIN IMMEDIATE" if immediate else "BEGIN"
        with self._lock:
            if self._connection.in_transaction:
                raise RuntimeError("Nested transactions are not supported")
            self._connection.execute(begin)
            try:
                yield self._connection
                self._connection.commit()
            except BaseException:
                self._connection.rollback()
                raise
            finally:
                self._lease_authority_hash = None
                self._lease_authority_fence = None
                self._defer_authority = None

    def execute(self, sql: str, parameters: Sequence[Any] = ()) -> sqlite3.Cursor:
        """Execute one statement. Writes require an explicit transaction."""
        with self._lock:
            if not self._connection.in_transaction and not sql.lstrip().upper().startswith("SELECT"):
                raise RuntimeError("Writes require storage.transaction()")
            return self._connection.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Sequence[Sequence[Any]]) -> sqlite3.Cursor:
        """Execute repeated writes inside an existing transaction."""
        with self._lock:
            if not self._connection.in_transaction:
                raise RuntimeError("Writes require storage.transaction()")
            return self._connection.executemany(sql, parameters)

    def fetch_one(self, sql: str, parameters: Sequence[Any] = ()) -> sqlite3.Row | None:
        """Return one row or ``None``."""
        with self._lock:
            row = self._connection.execute(sql, parameters).fetchone()
        if row is None:
            return None
        if not isinstance(row, sqlite3.Row):
            raise TypeError("SQLite connection must use sqlite3.Row row_factory")
        return row

    def fetch_all(self, sql: str, parameters: Sequence[Any] = ()) -> list[sqlite3.Row]:
        """Return all result rows."""
        with self._lock:
            return list(self._connection.execute(sql, parameters))

    def latest_observations(self) -> tuple[SourceObservation, ...]:
        """Rehydrate the newest immutable version of every durable source post."""
        rows = self.fetch_all(
            "SELECT post.channel_id, post.external_post_id, post.published_at AS post_published_at, "
            "version.body, version.kind, version.sponsored, version.urls_json, version.media_json, "
            "observation.channel_handle, observation.published_at AS observation_published_at, "
            "observation.edited_at, observation.engagement_json, observation.observed_at AS observation_observed_at, "
            "version.conflicts_json FROM source_posts post JOIN source_post_observations observation "
            "ON observation.id=(SELECT MAX(current.id) FROM source_post_observations current "
            "WHERE current.source_post_id=post.id) JOIN source_post_versions version "
            "ON version.id=observation.source_post_version_id "
            "ORDER BY post.channel_id, CAST(post.external_post_id AS INTEGER), post.external_post_id"
        )
        observations: list[SourceObservation] = []
        for row in rows:
            urls = json.loads(str(row["urls_json"]))
            media = json.loads(str(row["media_json"]))
            engagement = json.loads(str(row["engagement_json"])) if row["engagement_json"] else {}
            published_at = row["observation_published_at"] or row["post_published_at"]
            if not isinstance(published_at, str):
                raise RuntimeError("source observation has no published timestamp")
            kind = str(row["kind"])
            if kind not in {"message", "service", "deleted", "unsupported"}:
                raise RuntimeError("source version has an invalid message kind")
            observations.append(
                SourceObservation(
                    channel_id=str(row["channel_id"]),
                    channel_handle=str(row["channel_handle"]),
                    external_post_id=str(row["external_post_id"]),
                    published_at=_parse_timestamp(published_at),
                    text=str(row["body"]),
                    edited_at=_parse_timestamp(str(row["edited_at"])) if row["edited_at"] else None,
                    observed_at=_parse_timestamp(str(row["observation_observed_at"]))
                    if row["observation_observed_at"]
                    else None,
                    kind=cast(MessageKind, kind),
                    sponsored=bool(row["sponsored"]),
                    urls=tuple(UrlCandidate(**item) for item in urls),
                    media=tuple(Media(**item) for item in media),
                    engagement=Engagement(
                        views=engagement.get("views"),
                        reactions=engagement.get("reactions"),
                        forwards=engagement.get("forwards"),
                    ),
                    conflicts=tuple(str(item) for item in json.loads(str(row["conflicts_json"]))),
                )
            )
        return tuple(observations)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> Storage:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


@dataclass(frozen=True, slots=True)
class CollectionResult:
    """The durable state reached by one bounded collection invocation."""

    persisted: int
    interval_complete: bool
    cursor_promoted: bool


class DurableCollection:
    """Persist cap-safe source collection without advancing a cursor prematurely."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def collect_channel(
        self,
        collector: Any,
        channel: object,
        *,
        now: datetime,
        page_size: int = 100,
        initial_lookback: timedelta = timedelta(hours=24),
        overlap: timedelta = timedelta(hours=72),
        overlap_message_ids: int = 100,
        max_overlap_pages: int = 10,
        crash_after_page: bool = False,
    ) -> CollectionResult:
        """Collect one fixed message-ID interval and promote it only when complete."""
        if page_size < 1 or max_overlap_pages < 1 or overlap_message_ids < 1:
            raise ValueError("page_size, max_overlap_pages, and overlap_message_ids must be positive")
        now = _utc(now)
        channel_id = str(getattr(channel, "id", channel))
        interval = self.storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (channel_id,))
        if interval is None:
            cursor = self.storage.fetch_one(
                "SELECT published_at, external_post_id FROM collection_cursors WHERE channel_id=?",
                (channel_id,),
            )
            floor = _parse_timestamp(cursor["published_at"]) if cursor is not None else now - initial_lookback
            base_message_id = _message_id(cursor["external_post_id"]) if cursor is not None else 0
            fixed_upper = collector.latest_message_id(channel)
            upper_message_id = base_message_id if fixed_upper is None else _message_id(fixed_upper)
            if upper_message_id < base_message_id:
                raise RuntimeError("collector newest message ID predates the collection frontier")
            with self.storage.transaction() as connection:
                connection.execute(
                    "INSERT INTO collection_intervals("
                    "channel_id, floor_at, upper_bound_at, base_message_id, upper_message_id, next_message_id, floor_applies"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        channel_id,
                        _timestamp(floor),
                        _timestamp(now),
                        base_message_id,
                        upper_message_id,
                        base_message_id,
                        int(cursor is None),
                    ),
                )
            interval = self.storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (channel_id,))
            assert interval is not None

        floor = _parse_timestamp(interval["floor_at"])
        upper = _parse_timestamp(interval["upper_bound_at"])
        base_message_id = int(interval["base_message_id"])
        upper_message_id = int(interval["upper_message_id"])
        next_message_id = int(interval["next_message_id"])
        if bool(interval["page_complete"]):
            page: tuple[Any, ...] = ()
        else:
            page_floor = floor if bool(interval["floor_applies"]) else None
            page, has_more = self._page(collector, channel, page_floor, next_message_id, upper_message_id, page_size)
            self._persist_page(page, now)
            with self.storage.transaction() as connection:
                if has_more:
                    if not page:
                        raise RuntimeError("collector reported another page without a continuation ID")
                    connection.execute(
                        "UPDATE collection_intervals SET next_message_id=?, updated_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                        (_message_id(page[-1].external_post_id), channel_id),
                    )
                else:
                    connection.execute(
                        "UPDATE collection_intervals SET page_complete=1, updated_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                        (channel_id,),
                    )

        if crash_after_page:
            raise RuntimeError("deterministic collection crash after committed page")

        interval = self.storage.fetch_one("SELECT * FROM collection_intervals WHERE channel_id=?", (channel_id,))
        assert interval is not None
        if not bool(interval["page_complete"]):
            return CollectionResult(len(page), False, False)

        overlap_floor = max(floor, upper - overlap) if bool(interval["floor_applies"]) else upper - overlap
        overlap_after = interval["overlap_next_message_id"]
        overlap_persisted, overlap_complete = self._reconcile(
            collector,
            channel,
            overlap_floor,
            max(0, base_message_id - overlap_message_ids) if overlap_after is None else int(overlap_after),
            base_message_id,
            page_size,
            max_overlap_pages,
            observed_at=now,
            interval_channel_id=channel_id,
        )
        if overlap_complete:
            with self.storage.transaction() as connection:
                connection.execute(
                    "INSERT INTO collection_cursors(channel_id, published_at, external_post_id) VALUES (?, ?, ?) "
                    "ON CONFLICT(channel_id) DO UPDATE SET published_at=excluded.published_at, "
                    "external_post_id=excluded.external_post_id, updated_at=CURRENT_TIMESTAMP",
                    (channel_id, _timestamp(upper), str(upper_message_id)),
                )
                connection.execute("DELETE FROM collection_intervals WHERE channel_id=?", (channel_id,))
            return CollectionResult(len(page) + overlap_persisted, True, True)
        return CollectionResult(len(page) + overlap_persisted, False, False)

    def reconcile_channel(
        self,
        collector: Any,
        channel: object,
        *,
        lower_bound: datetime,
        upper_bound: datetime,
        page_size: int = 100,
        max_pages: int = 10,
        observed_at: datetime | None = None,
    ) -> int:
        """Perform a bounded deep reconciliation without changing normal cursors."""
        if page_size < 1 or max_pages < 1:
            raise ValueError("page_size and max_pages must be positive")
        lower_bound = _utc(lower_bound)
        upper_bound = _utc(upper_bound)
        observed_at = _utc(observed_at) if observed_at is not None else datetime.now(UTC)
        if lower_bound > upper_bound:
            raise ValueError("reconciliation lower_bound must not exceed upper_bound")
        upper_message_id = collector.latest_message_id(channel)
        if upper_message_id is None:
            return 0
        persisted, _ = self._reconcile(
            collector,
            channel,
            lower_bound,
            0,
            _message_id(upper_message_id),
            page_size,
            max_pages,
            observed_at=observed_at,
            upper_bound=upper_bound,
        )
        return persisted

    def _reconcile(
        self,
        collector: Any,
        channel: object,
        floor: datetime,
        after_message_id: int,
        upper_message_id: int,
        page_size: int,
        max_pages: int,
        *,
        interval_channel_id: str | None = None,
        upper_bound: datetime | None = None,
        observed_at: datetime | None = None,
    ) -> tuple[int, bool]:
        observed_at = _utc(observed_at) if observed_at is not None else datetime.now(UTC)
        persisted = 0
        for _ in range(max_pages):
            page, has_more = self._page(
                collector,
                channel,
                floor,
                after_message_id,
                upper_message_id,
                page_size,
                upper_bound=upper_bound,
            )
            self._persist_page(page, observed_at)
            persisted += len(page)
            if not has_more:
                return persisted, True
            if not page:
                raise RuntimeError("collector reported another page without a continuation ID")
            after_message_id = _message_id(page[-1].external_post_id)
            if interval_channel_id is not None:
                with self.storage.transaction() as connection:
                    connection.execute(
                        "UPDATE collection_intervals SET overlap_next_message_id=?, "
                        "updated_at=CURRENT_TIMESTAMP WHERE channel_id=?",
                        (after_message_id, interval_channel_id),
                    )
        return persisted, False

    def _page(
        self,
        collector: Any,
        channel: object,
        floor: datetime | None,
        after_message_id: int,
        upper_message_id: int,
        page_size: int,
        *,
        upper_bound: datetime | None = None,
    ) -> tuple[tuple[Any, ...], bool]:
        request: dict[str, Any] = {
            "lower_bound": floor,
            "min_message_id": after_message_id,
            "max_message_id": upper_message_id,
            "limit": page_size + 1,
        }
        if upper_bound is not None:
            request["upper_bound"] = upper_bound
        rows = collector.collect(channel, **request)
        ordered = tuple(
            sorted(
                (
                    row
                    for row in rows
                    if (floor is None or floor <= _utc(row.published_at))
                    and (upper_bound is None or _utc(row.published_at) <= upper_bound)
                    and after_message_id < _message_id(row.external_post_id) <= upper_message_id
                ),
                key=lambda row: _message_id(row.external_post_id),
            )
        )
        return ordered[:page_size], len(ordered) > page_size

    def _persist_page(self, observations: Sequence[Any], observed_at: datetime) -> None:
        with self.storage.transaction() as connection:
            for observation in observations:
                persist_observation(connection, observation, observed_at)


def persist_observation(
    connection: sqlite3.Connection,
    observation: Any,
    observed_at: datetime,
    *,
    return_observation_id: bool = False,
) -> int:
    published_at = _timestamp(_utc(observation.published_at))
    handle = observation.channel_handle.lstrip("@")
    source_url = f"https://t.me/{handle}/{observation.external_post_id}" if handle else None
    connection.execute(
        "INSERT OR IGNORE INTO source_posts(channel_id, external_post_id, published_at, source_url) VALUES (?, ?, ?, ?)",
        (observation.channel_id, observation.external_post_id, published_at, source_url),
    )
    post = connection.execute(
        "SELECT id FROM source_posts WHERE channel_id=? AND external_post_id=?",
        (observation.channel_id, observation.external_post_id),
    ).fetchone()
    assert post is not None
    material_payload = {
        "text": observation.text,
        "kind": observation.kind,
        "sponsored": observation.sponsored,
        "urls": [
            {"url": url.url, "source": url.source, "title": url.title, "description": url.description}
            for url in observation.urls
        ],
        "media": [
            {"kind": media.kind, "caption": media.caption, "identity": media.identity, "is_service": media.is_service}
            for media in observation.media
        ],
        "conflicts": list(observation.conflicts),
    }
    engagement_payload = {
        "views": observation.engagement.views,
        "reactions": observation.engagement.reactions,
        "forwards": observation.engagement.forwards,
    }
    version_key = _payload_hash(material_payload)
    connection.execute(
        "INSERT OR IGNORE INTO source_post_versions("
        "source_post_id, version_key, body, media_json, kind, sponsored, urls_json, conflicts_json"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            post["id"],
            version_key,
            observation.text,
            json.dumps(material_payload["media"], ensure_ascii=False, separators=(",", ":")),
            observation.kind,
            int(observation.sponsored),
            json.dumps(material_payload["urls"], ensure_ascii=False, separators=(",", ":")),
            json.dumps(material_payload["conflicts"], ensure_ascii=False, separators=(",", ":")),
        ),
    )
    version = connection.execute(
        "SELECT id FROM source_post_versions WHERE source_post_id=? AND version_key=?",
        (post["id"], version_key),
    ).fetchone()
    assert version is not None
    snapshot_payload = {
        "version_key": version_key,
        "channel_handle": observation.channel_handle,
        "published_at": published_at,
        "edited_at": _timestamp(_utc(observation.edited_at)) if observation.edited_at else None,
        "observed_at": _timestamp(_utc(observation.observed_at or observed_at)),
        "engagement": engagement_payload,
    }
    observation_key = _payload_hash(snapshot_payload)
    connection.execute(
        "INSERT OR IGNORE INTO source_post_observations("
        "source_post_id, source_post_version_id, observation_key, observed_at, channel_handle, published_at, "
        "edited_at, engagement_json"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            post["id"],
            version["id"],
            observation_key,
            snapshot_payload["observed_at"],
            observation.channel_handle,
            published_at,
            snapshot_payload["edited_at"],
            json.dumps(engagement_payload, ensure_ascii=False, separators=(",", ":")),
        ),
    )
    persisted = connection.execute(
        "SELECT id FROM source_post_observations WHERE source_post_id=? AND observation_key=?",
        (post["id"], observation_key),
    ).fetchone()
    assert persisted is not None
    return int(persisted["id"] if return_observation_id else version["id"])


def has_newer_material_source(connection: sqlite3.Connection, source_version_ids: Sequence[int]) -> bool:
    """Return whether bindings differ from each post's latest observation."""
    if not source_version_ids:
        return True
    marks = ",".join("?" for _ in source_version_ids)
    return (
        connection.execute(
            "SELECT 1 FROM source_post_versions bound "
            f"WHERE bound.id IN ({marks}) AND EXISTS ("
            "SELECT 1 FROM source_post_observations latest "
            "WHERE latest.source_post_id=bound.source_post_id "
            "AND latest.id=(SELECT MAX(current.id) FROM source_post_observations current "
            "WHERE current.source_post_id=bound.source_post_id) "
            "AND latest.source_post_version_id != bound.id) LIMIT 1",
            source_version_ids,
        ).fetchone()
        is not None
    )


def _payload_hash(payload: object) -> str:
    return sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("collection timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat()


def _parse_timestamp(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value))


def _message_id(value: object) -> int:
    if not isinstance(value, (int, str, bytes, bytearray)):
        raise ValueError("collector message IDs must be integers")
    try:
        message_id = int(value)
    except ValueError as exc:
        raise ValueError("collector message IDs must be integers") from exc
    if message_id < 0:
        raise ValueError("collector message IDs must be nonnegative")
    return message_id

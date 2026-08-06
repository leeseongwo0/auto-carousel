from __future__ import annotations

import errno
import hashlib
import os
import shutil
import sqlite3
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from newsbot.manual_storage import Identity, ManualStorage, ManualStorageError
from newsbot.storage import Storage


@pytest.fixture
def private_root() -> Path:
    root = Path(tempfile.mkdtemp(prefix="newsbot-manual-storage-integration-", dir=Path.home()))
    os.chmod(root, 0o700)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_sqlite_phase_hooks_reject_deterministic_identity_substitution(private_root: Path) -> None:
    with ManualStorage.open_directory(private_root / "manual") as storage:
        storage.reserve_database()
        storage.before_sqlite_phase()
        original = storage._identities
        storage._identities = (*original[:-1], Identity(0, 0, os.geteuid(), 0o700))
        with pytest.raises(ManualStorageError) as raised:
            storage.after_sqlite_phase()
    assert raised.value.code == "state_path_changed"


def test_sidecar_validation_rejects_wrong_mode_before_sqlite_phase(private_root: Path) -> None:
    with ManualStorage.open_directory(private_root / "manual") as storage:
        storage.reserve_database()
        wal = storage.path / "newsbot.sqlite3-wal"
        wal.write_bytes(b"wal")
        os.chmod(wal, 0o644)
        with pytest.raises(ManualStorageError) as raised:
            storage.before_sqlite_phase()
    assert raised.value.code == "unsafe_database_sidecar"


def test_materialization_rechecks_pinned_chain_at_publication(
    private_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"content that must not be published after a chain mismatch"
    digest = hashlib.sha256(payload).hexdigest()
    with ManualStorage.open_directory(private_root / "manual") as storage:
        original_attest = storage.attest
        calls = 0

        def substituted_attest() -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ManualStorageError("state_path_changed")
            original_attest()

        monkeypatch.setattr(storage, "attest", substituted_attest)
        with pytest.raises(ManualStorageError) as raised:
            storage.materialize("preview.txt", payload, sha256=digest)
        assert raised.value.code == "state_path_changed"
        assert not (storage.path / "preview.txt").exists()
        assert not list(storage.path.glob(".newsbot-*.tmp"))


def test_existing_output_hardlink_is_refused_without_repair(private_root: Path) -> None:
    payload = b"published bytes"
    digest = hashlib.sha256(payload).hexdigest()
    with ManualStorage.open_directory(private_root / "manual") as storage:
        output = storage.path / "preview.txt"
        output.write_bytes(payload)
        os.chmod(output, 0o600)
        os.link(output, storage.path / "preview-copy")
        with pytest.raises(ManualStorageError) as raised:
            storage.materialize("preview.txt", payload, sha256=digest)
        assert raised.value.code == "local_output_conflict"
        assert output.read_bytes() == payload
        assert output.stat().st_nlink == 2
        assert stat.S_IMODE(output.stat().st_mode) == 0o600


def test_materialization_race_cleanup_failure_is_redacted(private_root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"sensitive race payload must not appear in errors"
    digest = hashlib.sha256(payload).hexdigest()
    with ManualStorage.open_directory(private_root / "manual") as storage:
        original_link = os.link
        original_unlink = os.unlink

        def losing_link(source: str, destination: str, **kwargs: object) -> None:
            original_link(source, destination, **kwargs)
            raise FileExistsError(errno.EEXIST, "destination raced")

        def cleanup_failure(name: str, **kwargs: object) -> None:
            if name.startswith(".newsbot-"):
                raise PermissionError(errno.EPERM, "cleanup denied")
            original_unlink(name, **kwargs)

        monkeypatch.setattr(os, "link", losing_link)
        monkeypatch.setattr(os, "unlink", cleanup_failure)
        with pytest.raises(ManualStorageError) as raised:
            storage.materialize("preview.txt", payload, sha256=digest)

        error = raised.value
        assert error.code == "local_output_conflict"
        assert str(error) == "local_output_conflict"
        assert str(storage.path) not in str(error)
        assert payload.decode() not in str(error)
        output = storage.path / "preview.txt"
        assert output.read_bytes() == payload
        assert stat.S_IMODE(output.stat().st_mode) == 0o600
        temporary = next(storage.path.glob(".newsbot-*.tmp"))
        assert temporary.read_bytes() == payload
        assert stat.S_IMODE(temporary.stat().st_mode) == 0o600


def _sentinel(path: Path, payload: bytes = b"attacker sentinel") -> tuple[bytes, os.stat_result]:
    path.write_bytes(payload)
    os.chmod(path, 0o600)
    return payload, path.stat()


def _assert_redacted_substitution(
    error: ManualStorageError, path: Path, payload: bytes, original: os.stat_result
) -> None:
    assert error.code == "state_path_changed"
    assert str(error) == "state_path_changed"
    assert str(path) not in str(error)
    assert payload.decode() not in str(error)
    assert path.read_bytes() == payload
    current = path.stat()
    assert (current.st_dev, current.st_ino, stat.S_IMODE(current.st_mode), current.st_size) == (
        original.st_dev,
        original.st_ino,
        stat.S_IMODE(original.st_mode),
        original.st_size,
    )


@pytest.mark.parametrize(
    "phase",
    (
        "connect",
        "udf",
        "pre_wal",
        "post_wal",
        "migration_setup",
        "migration_body",
        "migration_commit",
        "migration_restore",
        "migration_008_preparation",
        "migration_008_body",
        "migration_008_commit",
        "migration_008_post_validation",
        "migration_008_restore",
    ),
)
def test_named_storage_phases_reject_database_substitution_before_sqlite_touch(private_root: Path, phase: str) -> None:
    with ManualStorage.open_directory(private_root / "manual") as state:
        reservation = state.reserve_database()
        database = reservation.path
        attacker = private_root / "attacker.sqlite3"
        replaced: tuple[bytes, os.stat_result] | None = None

        @contextmanager
        def guard(actual_phase: str) -> Iterator[None]:
            nonlocal replaced
            if actual_phase == phase:
                payload, _ = _sentinel(attacker)
                os.replace(attacker, database)
                replaced = (payload, database.stat())
            with state.sqlite_phase(actual_phase, database.name):
                yield

        with pytest.raises(ManualStorageError) as raised:
            Storage.open(database, phase_guard=guard)

        assert replaced is not None
        _assert_redacted_substitution(raised.value, database, *replaced)


@pytest.mark.parametrize("phase", ("transaction_begin", "transaction_commit", "transaction_rollback", "close"))
def test_named_transaction_and_close_phases_reject_database_substitution(private_root: Path, phase: str) -> None:
    with ManualStorage.open_directory(private_root / "manual") as state:
        reservation = state.reserve_database()
        database = reservation.path
        attacker = private_root / "attacker.sqlite3"
        replaced: tuple[bytes, os.stat_result] | None = None

        @contextmanager
        def guard(actual_phase: str) -> Iterator[None]:
            nonlocal replaced
            if actual_phase == phase:
                payload, _ = _sentinel(attacker)
                os.replace(attacker, database)
                replaced = (payload, database.stat())
            with state.sqlite_phase(actual_phase, database.name):
                yield

        storage = Storage.open(database, phase_guard=guard)
        with pytest.raises(ManualStorageError) as raised:
            if phase == "transaction_begin":
                with storage.transaction():
                    pass
            elif phase == "transaction_commit":
                with storage.transaction() as connection:
                    connection.execute("CREATE TEMP TABLE hostile_phase_probe(value INTEGER)")
            elif phase == "transaction_rollback":
                with storage.transaction():
                    raise RuntimeError("force rollback")
            else:
                storage.close()

        assert replaced is not None
        _assert_redacted_substitution(raised.value, database, *replaced)
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            storage.fetch_one("SELECT 1")


def test_migration_rollback_rejects_database_substitution_before_rollback(
    private_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with ManualStorage.open_directory(private_root / "manual") as state:
        reservation = state.reserve_database()
        database = reservation.path
        attacker = private_root / "attacker.sqlite3"
        replaced: tuple[bytes, os.stat_result] | None = None

        @contextmanager
        def guard(phase: str) -> Iterator[None]:
            nonlocal replaced
            if phase == "migration_rollback":
                payload, _ = _sentinel(attacker)
                os.replace(attacker, database)
                replaced = (payload, database.stat())
            with state.sqlite_phase(phase, database.name):
                yield

        def fail_migration(storage: Storage, _script: str) -> None:
            storage._connection.execute("CREATE TABLE rollback_probe(value INTEGER)")
            raise RuntimeError("forced migration failure")

        monkeypatch.setattr(Storage, "_execute_sql_script", fail_migration)
        with pytest.raises(ManualStorageError) as raised:
            Storage.open(database, phase_guard=guard)

        assert replaced is not None
        _assert_redacted_substitution(raised.value, database, *replaced)


def test_named_phase_rejects_pinned_ancestor_substitution_before_reopen(private_root: Path) -> None:
    manual = private_root / "manual"
    with ManualStorage.open_directory(manual) as state:
        reservation = state.reserve_database()

        @contextmanager
        def safe_guard(phase: str) -> Iterator[None]:
            with state.sqlite_phase(phase, reservation.path.name):
                yield

        storage = Storage.open(reservation.path, phase_guard=safe_guard)
        storage.close()

        attacker_directory = private_root / "attacker-manual"
        moved_directory = private_root / "moved-manual"
        os.rename(manual, moved_directory)
        attacker_directory.mkdir(mode=0o700)
        attacker_database = attacker_directory / reservation.path.name
        payload, original = _sentinel(attacker_database)
        os.rename(attacker_directory, manual)
        live_attacker_database = manual / reservation.path.name

        @contextmanager
        def guard(phase: str) -> Iterator[None]:
            with state.sqlite_phase(phase, reservation.path.name):
                yield

        with pytest.raises(ManualStorageError) as raised:
            Storage(reservation.path, phase_guard=guard)

        _assert_redacted_substitution(raised.value, live_attacker_database, payload, original)


def test_named_phase_rejects_registered_wal_substitution_before_sqlite_touch(private_root: Path) -> None:
    with ManualStorage.open_directory(private_root / "manual") as state:
        reservation = state.reserve_database()
        database = reservation.path
        wal = database.with_name(f"{database.name}-wal")
        attacker = private_root / "attacker-wal"
        replaced: tuple[bytes, os.stat_result] | None = None

        @contextmanager
        def guard(phase: str) -> Iterator[None]:
            nonlocal replaced
            if phase == "pre_wal":
                _sentinel(wal, b"trusted wal")
                state.validate_database(database.name)
                payload, _ = _sentinel(attacker)
                os.replace(attacker, wal)
                replaced = (payload, wal.stat())
            with state.sqlite_phase(phase, database.name):
                yield

        with pytest.raises(ManualStorageError) as raised:
            Storage(database, phase_guard=guard)

        assert replaced is not None
        _assert_redacted_substitution(raised.value, wal, *replaced)

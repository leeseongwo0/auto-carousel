from __future__ import annotations

import hashlib
import os
import shutil
import stat
import tempfile
from pathlib import Path

import pytest

from newsbot.manual_storage import ManualStorage, ManualStorageError, default_manual_state_path, validate_manual_path


@pytest.fixture
def private_root() -> Path:
    root = Path(tempfile.mkdtemp(prefix="newsbot-manual-storage-", dir=Path.home()))
    os.chmod(root, 0o700)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_default_path_uses_absolute_xdg_or_home_fallback(private_root: Path) -> None:
    assert (
        default_manual_state_path(environ={"XDG_STATE_HOME": str(private_root)}) == private_root / "newsbot" / "manual"
    )
    assert default_manual_state_path(environ={"XDG_STATE_HOME": "relative"}, home=private_root) == (
        private_root / ".local" / "state" / "newsbot" / "manual"
    )


@pytest.mark.parametrize("value", ["relative", ":memory:", "/safe/../unsafe", "/safe/db.sqlite3-wal"])
def test_path_validation_rejects_ambiguous_database_forms(value: str) -> None:
    with pytest.raises(ManualStorageError) as raised:
        validate_manual_path(value, database=True)
    assert raised.value.code in {"unsafe_state_anchor", "unsafe_database_file"}
    assert value not in str(raised.value)


def test_creates_private_chain_and_reserves_private_database(private_root: Path) -> None:
    target = private_root / "state" / "manual"
    with ManualStorage.open_directory(target) as storage:
        reservation = storage.reserve_database()
        assert reservation.created
        assert reservation.path == target / "newsbot.sqlite3"
        assert stat.S_IMODE(target.stat().st_mode) == 0o700
        assert stat.S_IMODE(reservation.path.stat().st_mode) == 0o600
        assert storage.reserve_database().created is False


def test_reservation_rejects_path_replacement_before_descriptor_closes(
    private_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = private_root / "manual"
    with ManualStorage.open_directory(target) as storage:
        original_fsync = ManualStorage._fsync
        calls = 0
        sentinel = b"attacker-sentinel"

        def replace_after_file_fsync(descriptor: int) -> None:
            nonlocal calls
            original_fsync(descriptor)
            calls += 1
            if calls == 1:
                attacker = private_root / "attacker.sqlite3"
                attacker.write_bytes(sentinel)
                os.chmod(attacker, 0o600)
                os.replace(attacker, target / "newsbot.sqlite3")

        monkeypatch.setattr(ManualStorage, "_fsync", staticmethod(replace_after_file_fsync))
        with pytest.raises(ManualStorageError) as raised:
            storage.reserve_database()

    assert raised.value.code == "state_path_changed"
    assert (target / "newsbot.sqlite3").read_bytes() == sentinel


def test_rejects_writable_ancestor_before_creating_descendant(private_root: Path) -> None:
    shared = private_root / "shared"
    shared.mkdir(mode=0o755)
    os.chmod(shared, 0o775)
    with pytest.raises(ManualStorageError) as raised:
        ManualStorage.open_directory(shared / "manual")
    assert raised.value.code == "unsafe_state_ancestor"
    assert not (shared / "manual").exists()
    assert str(shared) not in str(raised.value)


def test_rejects_symlink_and_non_private_final_directory(private_root: Path) -> None:
    destination = private_root / "destination"
    destination.mkdir(mode=0o700)
    link = private_root / "link"
    link.symlink_to(destination, target_is_directory=True)
    with pytest.raises(ManualStorageError) as raised:
        ManualStorage.open_directory(link / "manual")
    assert raised.value.code == "unsafe_state_ancestor"

    os.chmod(destination, 0o755)
    with pytest.raises(ManualStorageError) as raised:
        ManualStorage.open_directory(destination, create=False)
    assert raised.value.code == "unsafe_state_parent"


def test_database_rejects_hardlink_and_restores_umask(private_root: Path) -> None:
    previous_umask = os.umask(0o022)
    try:
        with ManualStorage.open_directory(private_root / "manual") as storage:
            database = storage.reserve_database().path
            observed = os.umask(0o077)
            os.umask(observed)
            assert observed == 0o022
            os.link(database, storage.path / "another-link")
            with pytest.raises(ManualStorageError) as raised:
                storage.reserve_database()
            assert raised.value.code == "unsafe_database_file"
    finally:
        os.umask(previous_umask)


def test_materializer_exact_reuse_and_collision_refusal(private_root: Path) -> None:
    payload = b"private preview\n"
    digest = hashlib.sha256(payload).hexdigest()
    with ManualStorage.open_directory(private_root / "manual") as storage:
        output = storage.materialize("preview.txt", payload, sha256=digest)
        assert output.read_bytes() == payload
        assert stat.S_IMODE(output.stat().st_mode) == 0o600
        assert storage.materialize("preview.txt", payload, sha256=digest) == output
        with pytest.raises(ManualStorageError) as raised:
            storage.materialize("preview.txt", b"other", sha256=hashlib.sha256(b"other").hexdigest())
        assert raised.value.code == "local_output_conflict"


def test_materializer_rejects_unsafe_existing_types(private_root: Path) -> None:
    with ManualStorage.open_directory(private_root / "manual") as storage:
        (storage.path / "preview.txt").symlink_to("target")
        with pytest.raises(ManualStorageError) as raised:
            storage.materialize("preview.txt", b"x", sha256=hashlib.sha256(b"x").hexdigest())
    assert raised.value.code == "local_output_conflict"


def test_private_input_reader_rejects_symlink_and_bounds(private_root: Path) -> None:
    input_dir = private_root / "input"
    input_dir.mkdir(mode=0o700)
    document = input_dir / "document.json"
    document.write_bytes(b"{}")
    document.chmod(0o600)
    assert ManualStorage.read_private_input(document, max_bytes=2) == b"{}"
    document.unlink()
    document.symlink_to(document.name)
    with pytest.raises(ManualStorageError) as raised:
        ManualStorage.read_private_input(document, max_bytes=2)
    assert raised.value.code == "unsafe_import_input"

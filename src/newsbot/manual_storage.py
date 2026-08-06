"""Fail-closed POSIX storage primitives for the manual/local workflow.

This module deliberately has no dependency on the production Storage class.  It is
an authority boundary: callers must open a trusted directory here before handing
an ordinary pathname to SQLite or writing manual output.
"""

from __future__ import annotations

import errno
import hashlib
import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_PRIVATE_DIR_MODE: Final = 0o700
_PRIVATE_FILE_MODE: Final = 0o600
_DEFAULT_MAX_BYTES: Final = 16 * 1024 * 1024
_REQUIRED_OPEN_FLAGS: Final = ("O_DIRECTORY", "O_NOFOLLOW", "O_CLOEXEC")


class ManualStorageError(RuntimeError):
    """A public, redacted error raised by the manual filesystem boundary."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _unsupported() -> None:
    if os.name != "posix" or any(not hasattr(os, flag) for flag in _REQUIRED_OPEN_FLAGS):
        raise ManualStorageError("manual_storage_unsupported")


class _PrivateUmask:
    _previous: int

    def __enter__(self) -> None:
        self._previous = os.umask(0o077)

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        os.umask(self._previous)


def _private_umask() -> _PrivateUmask:
    return _PrivateUmask()


def default_manual_state_path(*, environ: dict[str, str] | None = None, home: Path | None = None) -> Path:
    """Return the candidate default state path without trusting or creating it."""
    environment = os.environ if environ is None else environ
    xdg = environment.get("XDG_STATE_HOME")
    if xdg:
        candidate = Path(xdg).expanduser()
        if candidate.is_absolute():
            return candidate / "newsbot" / "manual"
    selected_home = Path.home() if home is None else home
    return selected_home / ".local" / "state" / "newsbot" / "manual"


def validate_manual_path(value: str | Path, *, database: bool = False) -> Path:
    """Perform lexical validation; filesystem trust is established by open_directory."""
    raw = os.fspath(value)
    if not raw or "\x00" in raw or not os.path.isabs(os.path.expanduser(raw)):
        raise ManualStorageError("unsafe_state_anchor")
    path = Path(os.path.expanduser(raw))
    parts = path.parts
    if any(part in (".", "..") for part in parts) or any("\x00" in part for part in parts):
        raise ManualStorageError("unsafe_state_anchor")
    if database:
        name = path.name
        if name in ("", ".", "..") or name.endswith("-wal") or name.endswith("-shm") or not name.endswith(".sqlite3"):
            raise ManualStorageError("unsafe_database_file")
    return path


@dataclass(frozen=True)
class Identity:
    device: int
    inode: int
    uid: int
    mode: int

    @classmethod
    def from_stat(cls, result: os.stat_result) -> Identity:
        return cls(result.st_dev, result.st_ino, result.st_uid, stat.S_IMODE(result.st_mode))


@dataclass(frozen=True)
class DatabaseReservation:
    path: Path
    created: bool


def _directory_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _file_flags() -> int:
    return os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC


class ManualStorage:
    """Pinned descriptor-relative gateway for one trusted private directory."""

    def __init__(self, path: Path, descriptor: int, identities: tuple[Identity, ...]) -> None:
        self.path = path
        self._fd = descriptor
        self._identities = identities
        self._file_identities: dict[str, Identity] = {}
        self._uid = os.geteuid()
        self._closed = False

    @classmethod
    def open_directory(cls, value: str | Path, *, create: bool = True) -> ManualStorage:
        """Open/create a final private directory after validating every ancestor."""
        _unsupported()
        path = validate_manual_path(value)
        uid = os.geteuid()
        try:
            fd = os.open("/", _directory_flags())
        except OSError as exc:
            raise cls._error_for_oserror(exc, "unsafe_state_anchor") from None
        identities: list[Identity] = []
        try:
            root_stat = os.fstat(fd)
            cls._check_directory(root_stat, uid, final=False, code="unsafe_state_anchor")
            identities.append(Identity.from_stat(root_stat))
            for component in path.parts[1:]:
                try:
                    child = os.open(component, _directory_flags(), dir_fd=fd)
                except FileNotFoundError:
                    if not create:
                        raise ManualStorageError("unsafe_state_parent") from None
                    with _private_umask(), suppress(FileExistsError):
                        os.mkdir(component, _PRIVATE_DIR_MODE, dir_fd=fd)
                    cls._fsync(fd)
                    try:
                        child = os.open(component, _directory_flags(), dir_fd=fd)
                    except OSError as exc:
                        raise cls._error_for_oserror(exc, "unsafe_state_ancestor") from None
                except OSError as exc:
                    raise cls._error_for_oserror(exc, "unsafe_state_ancestor") from None
                try:
                    child_stat = os.fstat(child)
                    cls._check_directory(child_stat, uid, final=False, code="unsafe_state_ancestor")
                    named_stat = os.stat(component, dir_fd=fd, follow_symlinks=False)
                    if Identity.from_stat(named_stat) != Identity.from_stat(child_stat):
                        raise ManualStorageError("state_path_changed")
                    identities.append(Identity.from_stat(child_stat))
                except BaseException:
                    os.close(child)
                    raise
                os.close(fd)
                fd = child
            cls._check_directory(os.fstat(fd), uid, final=True, code="unsafe_state_parent")
            storage = cls(path, fd, tuple(identities))
            storage.attest()
            return storage
        except BaseException:
            os.close(fd)
            raise

    @classmethod
    def read_private_input(cls, value: str | Path, *, max_bytes: int) -> bytes:
        """Read one bounded owner-only input file through a pinned parent descriptor."""
        path = validate_manual_path(value)
        try:
            with cls.open_directory(path.parent, create=False) as parent:
                parent.attest()
                data = parent._read_checked(path.name, "unsafe_import_input", max_bytes=max_bytes)
                parent.attest()
                return data
        except ManualStorageError:
            raise
        except OSError:
            raise ManualStorageError("unsafe_import_input") from None

    @staticmethod
    def _check_directory(result: os.stat_result, uid: int, *, final: bool, code: str) -> None:
        mode = stat.S_IMODE(result.st_mode)
        if not stat.S_ISDIR(result.st_mode) or result.st_uid not in (0, uid) or mode & 0o022:
            raise ManualStorageError(code)
        if final and (result.st_uid != uid or mode != _PRIVATE_DIR_MODE):
            raise ManualStorageError("unsafe_state_parent")

    @staticmethod
    def _check_private_file(result: os.stat_result, code: str) -> None:
        if (
            not stat.S_ISREG(result.st_mode)
            or result.st_uid != os.geteuid()
            or stat.S_IMODE(result.st_mode) != _PRIVATE_FILE_MODE
            or result.st_nlink != 1
        ):
            raise ManualStorageError(code)

    @staticmethod
    def _fsync(descriptor: int) -> None:
        try:
            os.fsync(descriptor)
        except OSError as exc:
            if exc.errno in (errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP):
                raise ManualStorageError("manual_storage_unsupported") from None
            raise ManualStorageError("state_path_changed") from None

    @staticmethod
    def _error_for_oserror(exc: OSError, code: str) -> ManualStorageError:
        if exc.errno in (errno.ELOOP, errno.ENOTDIR, errno.EACCES, errno.EPERM, errno.ENOENT):
            return ManualStorageError(code)
        if exc.errno in (errno.ENOTSUP, errno.EOPNOTSUPP):
            return ManualStorageError("manual_storage_unsupported")
        return ManualStorageError(code)

    def close(self) -> None:
        if not self._closed:
            os.close(self._fd)
            self._closed = True

    def __enter__(self) -> ManualStorage:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise ManualStorageError("state_path_changed")

    @contextmanager
    def private_umask(self) -> Iterator[None]:
        """Keep SQLite-created files owner-only during a verified lifecycle phase."""
        self._require_open()
        self.attest()
        with _private_umask():
            try:
                yield
            finally:
                self.attest()

    def attest(self) -> None:
        """Re-walk the ordinary pathname without symlinks and compare pinned identities."""
        self._require_open()
        fd = os.open("/", _directory_flags())
        try:
            actual: list[Identity] = [Identity.from_stat(os.fstat(fd))]
            for component in self.path.parts[1:]:
                next_fd = os.open(component, _directory_flags(), dir_fd=fd)
                os.close(fd)
                fd = next_fd
                actual.append(Identity.from_stat(os.fstat(fd)))
            if tuple(actual) != self._identities or Identity.from_stat(os.fstat(self._fd)) != self._identities[-1]:
                raise ManualStorageError("state_path_changed")
            self._check_directory(os.fstat(fd), self._uid, final=True, code="unsafe_state_parent")
        except ManualStorageError:
            raise
        except OSError:
            raise ManualStorageError("state_path_changed") from None
        finally:
            os.close(fd)

    def _plain_name(self, name: str, *, code: str) -> None:
        if not name or name in (".", "..") or "/" in name or "\\" in name or any(ord(char) < 32 for char in name):
            raise ManualStorageError(code)

    def _inspect_file(self, name: str, *, code: str) -> os.stat_result:
        self._plain_name(name, code=code)
        try:
            named = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            self._check_private_file(named, code)
            descriptor = os.open(name, _file_flags(), dir_fd=self._fd)
        except FileNotFoundError:
            raise
        except OSError as exc:
            raise self._error_for_oserror(exc, code) from None
        try:
            opened = os.fstat(descriptor)
            self._check_private_file(opened, code)
            if Identity.from_stat(named) != Identity.from_stat(opened):
                raise ManualStorageError("state_path_changed")
            return opened
        finally:
            os.close(descriptor)

    def reserve_database(self, filename: str = "newsbot.sqlite3") -> DatabaseReservation:
        """Exclusively reserve a private SQLite database, never repairing an existing object."""
        self._require_open()
        self._database_name(filename)
        self.attest()
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
        try:
            with _private_umask():
                descriptor = os.open(filename, flags, _PRIVATE_FILE_MODE, dir_fd=self._fd)
        except FileExistsError:
            self.validate_database(filename)
            return DatabaseReservation(self.path / filename, False)
        except OSError as exc:
            raise self._error_for_oserror(exc, "unsafe_database_file") from None
        try:
            created = Identity.from_stat(os.fstat(descriptor))
            self._check_private_file(os.fstat(descriptor), "unsafe_database_file")
            self._fsync(descriptor)
            self._fsync(self._fd)
            self.attest()
            named = Identity.from_stat(self._inspect_file(filename, code="unsafe_database_file"))
            if named != created:
                raise ManualStorageError("state_path_changed")
            self._file_identities = {filename: named}
            return DatabaseReservation(self.path / filename, True)
        finally:
            os.close(descriptor)

    def validate_database(self, filename: str = "newsbot.sqlite3") -> None:
        """Attest the main database and every currently present SQLite sidecar."""
        self._require_open()
        self._database_name(filename)
        self.attest()
        main = self._inspect_file(filename, code="unsafe_database_file")
        current = {filename: Identity.from_stat(main)}
        for sidecar in (f"{filename}-wal", f"{filename}-shm"):
            try:
                current[sidecar] = Identity.from_stat(self._inspect_file(sidecar, code="unsafe_database_sidecar"))
            except FileNotFoundError:
                continue
        prior = self._file_identities.get(filename)
        if prior is not None and prior != current[filename]:
            raise ManualStorageError("state_path_changed")
        for sidecar, identity in current.items():
            prior = self._file_identities.get(sidecar)
            if prior is not None and prior != identity:
                raise ManualStorageError("state_path_changed")
        self._file_identities = current
        self.attest()

    def _database_name(self, filename: str) -> None:
        self._plain_name(filename, code="unsafe_database_file")
        if not filename.endswith(".sqlite3") or filename.endswith("-wal") or filename.endswith("-shm"):
            raise ManualStorageError("unsafe_database_file")

    def before_sqlite_phase(self, filename: str = "newsbot.sqlite3") -> None:
        self.validate_database(filename)

    def after_sqlite_phase(self, filename: str = "newsbot.sqlite3") -> None:
        self.validate_database(filename)

    @contextmanager
    def sqlite_phase(self, phase: str, filename: str = "newsbot.sqlite3") -> Iterator[None]:
        """Attest the controlling chain and SQLite files around one named database phase."""
        if not phase:
            raise ManualStorageError("state_path_changed")
        with self.private_umask():
            self.before_sqlite_phase(filename)
            try:
                yield
            finally:
                self.after_sqlite_phase(filename)

    def materialize(
        self,
        filename: str,
        content: bytes,
        *,
        sha256: str,
        allowed_suffixes: tuple[str, ...] = (".json", ".md", ".txt"),
        max_bytes: int = _DEFAULT_MAX_BYTES,
    ) -> Path:
        """Durably publish bytes using an owner-only temp and atomic no-clobber link."""
        self._require_open()
        self._plain_name(filename, code="local_output_conflict")
        if len(filename) > 128 or not any(filename.endswith(suffix) for suffix in allowed_suffixes):
            raise ManualStorageError("local_output_conflict")
        if len(content) > max_bytes or len(sha256) != 64 or hashlib.sha256(content).hexdigest() != sha256:
            raise ManualStorageError("local_output_conflict")
        self.attest()
        try:
            existing = self._inspect_file(filename, code="local_output_conflict")
        except FileNotFoundError:
            existing = None
        if existing is not None:
            if self._read_checked(filename, "local_output_conflict") == content:
                self.attest()
                return self.path / filename
            raise ManualStorageError("local_output_conflict")

        temp_name = self._new_temp_name()
        descriptor = -1
        temp_identity: Identity | None = None
        try:
            try:
                with _private_umask():
                    descriptor = os.open(
                        temp_name,
                        os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                        _PRIVATE_FILE_MODE,
                        dir_fd=self._fd,
                    )
            except OSError as exc:
                raise self._error_for_oserror(exc, "local_output_conflict") from None
            temp_stat = os.fstat(descriptor)
            self._check_private_file(temp_stat, "local_output_conflict")
            temp_identity = Identity.from_stat(temp_stat)
            self._write_all(descriptor, content)
            self._fsync(descriptor)
            reread = self._read_descriptor(descriptor)
            if reread != content or hashlib.sha256(reread).hexdigest() != sha256:
                raise ManualStorageError("local_output_conflict")
            self.attest()
            try:
                os.link(temp_name, filename, src_dir_fd=self._fd, dst_dir_fd=self._fd, follow_symlinks=False)
            except FileExistsError:
                if self._read_checked(filename, "local_output_conflict") != content:
                    raise ManualStorageError("local_output_conflict") from None
                self._cleanup_temp(temp_name, temp_identity, required=True)
                temp_name = ""
                self.attest()
                return self.path / filename
            except (AttributeError, NotImplementedError):
                raise ManualStorageError("manual_storage_unsupported") from None
            except OSError as exc:
                raise self._error_for_oserror(exc, "local_output_conflict") from None
            self._cleanup_temp(temp_name, temp_identity, required=True)
            temp_name = ""
            self.attest()
            if self._read_checked(filename, "local_output_conflict") != content:
                raise ManualStorageError("state_path_changed")
            self._fsync(self._fd)
            return self.path / filename
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temp_name:
                with suppress(ManualStorageError):
                    self._cleanup_temp(temp_name, temp_identity, required=False)

    def _new_temp_name(self) -> str:
        for _ in range(64):
            name = f".newsbot-{secrets.token_hex(16)}.tmp"
            try:
                os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            except FileNotFoundError:
                return name
            except OSError as exc:
                raise self._error_for_oserror(exc, "local_output_conflict") from None
        raise ManualStorageError("local_output_conflict")

    def _cleanup_temp(self, name: str, identity: Identity | None, *, required: bool) -> None:
        try:
            current = os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            if identity is None or not stat.S_ISREG(current.st_mode) or Identity.from_stat(current) != identity:
                raise ManualStorageError("state_path_changed")
            os.unlink(name, dir_fd=self._fd)
            self._fsync(self._fd)
            try:
                os.stat(name, dir_fd=self._fd, follow_symlinks=False)
            except FileNotFoundError:
                self.attest()
                return
            raise ManualStorageError("state_path_changed")
        except FileNotFoundError:
            if required:
                raise ManualStorageError("local_output_conflict") from None
        except ManualStorageError:
            if required:
                raise
        except OSError as exc:
            if required:
                raise self._error_for_oserror(exc, "local_output_conflict") from None

    def _read_checked(self, name: str, code: str, *, max_bytes: int | None = None) -> bytes:
        self._inspect_file(name, code=code)
        try:
            descriptor = os.open(name, _file_flags(), dir_fd=self._fd)
        except OSError as exc:
            raise self._error_for_oserror(exc, code) from None
        try:
            data = self._read_descriptor(descriptor, max_bytes=max_bytes)
            self._check_private_file(os.fstat(descriptor), code)
            return data
        except OSError as exc:
            raise self._error_for_oserror(exc, code) from None
        finally:
            os.close(descriptor)

    @staticmethod
    def _write_all(descriptor: int, content: bytes) -> None:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ManualStorageError("local_output_conflict")
            view = view[written:]

    @staticmethod
    def _read_descriptor(descriptor: int, *, max_bytes: int | None = None) -> bytes:
        os.lseek(descriptor, 0, os.SEEK_SET)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if max_bytes is not None and total > max_bytes:
                raise ManualStorageError("unsafe_import_input")
            chunks.append(chunk)

"""No-follow, owner-only local storage for secrets and Telethon session files."""

from __future__ import annotations

import json
import os
import secrets
import stat
from contextlib import suppress
from pathlib import Path


class SecretFileError(RuntimeError):
    pass


def _uid() -> int:
    return os.getuid() if hasattr(os, "getuid") else -1


def _check_owner(mode: int, owner: int, path: Path, *, directory: bool = False) -> None:
    expected = stat.S_IFDIR if directory else stat.S_IFREG
    if stat.S_IFMT(mode) != expected:
        raise SecretFileError(f"unsafe {'directory' if directory else 'file'} type: {path}")
    if owner != _uid():
        raise SecretFileError(f"unsafe owner for {path}")
    if mode & 0o077:
        raise SecretFileError(f"unsafe permissions for {path}")


def _lstat(path: Path, *, directory: bool = False) -> os.stat_result:
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise
    _check_owner(info.st_mode, info.st_uid, path, directory=directory)
    return info


def ensure_private_directory(path: str | Path) -> Path:
    """Create or validate an owner-only directory without following symlinks."""
    directory = Path(path)
    old_umask = os.umask(0o077)
    try:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    finally:
        os.umask(old_umask)
    _lstat(directory, directory=True)
    return directory


def _secure_parent(path: Path) -> None:
    ensure_private_directory(path.parent)


def _open_readonly(path: Path) -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise SecretFileError(f"cannot safely open {path}") from error
    try:
        info = os.fstat(descriptor)
        _check_owner(info.st_mode, info.st_uid, path)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def validate_private_session_file(path: str | Path) -> Path:
    """Validate an existing Telethon session without following links."""
    session_path = Path(path)
    parent = session_path.parent
    parent_info = _lstat(parent, directory=True)
    if stat.S_IMODE(parent_info.st_mode) != 0o700:
        raise SecretFileError(f"unsafe permissions for {parent}")
    session_info = _lstat(session_path)
    if stat.S_IMODE(session_info.st_mode) != 0o600:
        raise SecretFileError(f"unsafe permissions for {session_path}")
    descriptor = _open_readonly(session_path)
    os.close(descriptor)
    return session_path


def read_private_bytes(path: str | Path) -> bytes:
    secret_path = Path(path)
    _lstat(secret_path)
    descriptor = _open_readonly(secret_path)
    try:
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 64 * 1024):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def read_private_text(path: str | Path, *, encoding: str = "utf-8") -> str:
    return read_private_bytes(path).decode(encoding)


def read_service_account_info(path: str | Path) -> dict[str, str]:
    """Read and validate service-account JSON once through a hardened descriptor."""
    credential_path = Path(path)
    parent_info = _lstat(credential_path.parent, directory=True)
    if stat.S_IMODE(parent_info.st_mode) != 0o700:
        raise SecretFileError("unsafe service-account directory")
    credential_info = _lstat(credential_path)
    if stat.S_IMODE(credential_info.st_mode) != 0o600:
        raise SecretFileError("unsafe service-account file")
    try:
        raw = read_private_bytes(credential_path)
        if len(raw) > 64 * 1024:
            raise ValueError
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise SecretFileError("invalid service-account credentials") from error
    required = {
        "type",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "client_x509_cert_url",
        "universe_domain",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise SecretFileError("invalid service-account credentials")
    if value["type"] != "service_account":
        raise SecretFileError("invalid service-account credentials")
    if any(not isinstance(value[key], str) or not value[key] for key in required - {"type"}):
        raise SecretFileError("invalid service-account credentials")
    if not value["private_key"].startswith("-----BEGIN PRIVATE KEY-----\n") or not value["private_key"].endswith(
        "-----END PRIVATE KEY-----\n"
    ):
        raise SecretFileError("invalid service-account credentials")
    if not value["client_email"].endswith(".gserviceaccount.com"):
        raise SecretFileError("invalid service-account credentials")
    if value["token_uri"] != "https://oauth2.googleapis.com/token":
        raise SecretFileError("invalid service-account credentials")
    return {str(key): str(item) for key, item in value.items()}


def validate_service_account_file(path: str | Path) -> Path:
    """Validate a private Google service-account JSON file."""
    credential_path = Path(path)
    read_service_account_info(credential_path)
    return credential_path


def write_private_bytes(path: str | Path, data: bytes) -> None:
    """Atomically replace an owner-only regular file in an owner-only directory."""
    secret_path = Path(path)
    if not secret_path.name:
        raise SecretFileError("secret path must name a file")
    _secure_parent(secret_path)
    with suppress(FileNotFoundError):
        _lstat(secret_path)
    temp = secret_path.with_name(f".{secret_path.name}.{secrets.token_hex(16)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temp, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        _check_owner(info.st_mode, info.st_uid, temp)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(temp)
        raise
    finally:
        os.close(descriptor)
    try:
        os.replace(temp, secret_path)
        os.chmod(secret_path, 0o600)
        directory_fd = os.open(secret_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(temp)
        raise
    _lstat(secret_path)


def write_private_text(path: str | Path, value: str, *, encoding: str = "utf-8") -> None:
    write_private_bytes(path, value.encode(encoding))


class SessionStore:
    """Secure local byte storage for an adapter session; never logs its contents."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def validate(self) -> Path:
        return validate_private_session_file(self.path)

    def exists(self) -> bool:
        try:
            _lstat(self.path)
        except FileNotFoundError:
            return False
        return True

    def read(self) -> bytes:
        return read_private_bytes(self.path)

    def write(self, session: bytes) -> None:
        write_private_bytes(self.path, session)

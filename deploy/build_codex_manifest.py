#!/usr/bin/env python3
"""Build the fixed host-specific Codex release manifest after staged installation."""

from __future__ import annotations

import hashlib
import json
import os
import pwd
import stat
import sys
from pathlib import Path

OUTPUT = Path("/usr/local/lib/newsbot-codex-manifest-v1.json")
MODEL = "gpt-5-codex"
CODEX_VERSION = "0.138.0"
ARTIFACTS = {
    "/usr/local/libexec/codex-v0.138.0": 0o755,
    "/usr/local/libexec/newsbot-codex-runner-v1": 0o755,
    "/usr/local/sbin/newsbot-codex-containment-v1": 0o755,
    "/usr/local/share/newsbot/copy_draft.schema.json": 0o644,
    "/etc/sudoers.d/newsbot-codex": 0o440,
    "/etc/systemd/system/newsbot-generate-codex.service": 0o644,
    "/etc/systemd/system/newsbot-generate-codex-canary.service": 0o644,
    "/etc/systemd/system/newsbot-generate-codex.timer": 0o644,
    "/etc/tmpfiles.d/newsbot-codex.conf": 0o644,
}


def _artifact(path_text: str, expected_mode: int) -> dict[str, object]:
    path = Path(path_text)
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != expected_mode
        or metadata.st_nlink != 1
    ):
        raise RuntimeError("release artifact attestation failed")
    return {
        "group": 0,
        "mode": format(expected_mode, "04o"),
        "owner": 0,
        "path": path_text,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def main() -> int:
    if os.geteuid() != 0 or len(sys.argv) != 1:
        return 1
    try:
        newsbot = pwd.getpwnam("newsbot")
        codex = pwd.getpwnam("newsbot-codex")
        artifacts = [_artifact(path, mode) for path, mode in sorted(ARTIFACTS.items())]
        value = {
            "artifacts": artifacts,
            "codex_version": CODEX_VERSION,
            "model": MODEL,
            "newsbot_codex_gid": codex.pw_gid,
            "newsbot_codex_uid": codex.pw_uid,
            "newsbot_gid": newsbot.pw_gid,
            "newsbot_uid": newsbot.pw_uid,
            "version": "newsbot-codex-release-manifest-v1",
        }
        data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii") + b"\n"
        parent_fd = os.open(OUTPUT.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        temporary = OUTPUT.name + ".new"
        try:
            fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o644,
                dir_fd=parent_fd,
            )
            try:
                if os.write(fd, data) != len(data):
                    raise RuntimeError("manifest write failed")
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temporary, OUTPUT.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    except (KeyError, OSError, RuntimeError):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

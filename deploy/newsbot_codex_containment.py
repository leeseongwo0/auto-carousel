#!/usr/bin/env python3
"""Fail-closed durable containment state machine for the fixed Codex units."""

import fcntl
import grp
import hashlib
import json
import os
import pwd
import secrets
import stat
import subprocess
import sys
import time

ROOT = "/var/lib/newsbot-containment"
STATE = "codex-state-v1"
AUDIT = "audit"
RECOVERY_LOCK = "codex-recovery.lock"
CREDENTIAL = "/run/newsbot-codex-activation-v1"
LOCK = "/run/lock/newsbot-codex/generation.lock"
MANIFEST = "/usr/local/lib/newsbot-codex-manifest-v1.json"
UNITS = ("newsbot-generate-codex.service", "newsbot-generate-codex-canary.service")
CGROUPS = (
    "/sys/fs/cgroup/system.slice/newsbot-generate-codex.service",
    "/sys/fs/cgroup/system.slice/newsbot-generate-codex-canary.service",
)
MANIFEST_ARTIFACTS = frozenset(
    {
        "/usr/local/libexec/codex-v0.146.0",
        "/usr/local/libexec/newsbot-codex-runner-v1",
        "/usr/local/sbin/newsbot-codex-containment-v1",
        "/usr/local/share/newsbot/copy_draft.schema.json",
        "/etc/codex/requirements.toml",
        "/etc/sudoers.d/newsbot-codex",
        "/etc/systemd/system/newsbot-generate-codex.service",
        "/etc/systemd/system/newsbot-generate-codex-canary.service",
        "/etc/systemd/system/newsbot-generate-codex.timer",
        "/etc/tmpfiles.d/newsbot-codex.conf",
    }
)


class Blocked(Exception):
    pass


def die():
    raise Blocked()


def root_dir(path, mode):
    parents_safe(path)
    value = os.stat(path, follow_symlinks=False)
    if (
        not stat.S_ISDIR(value.st_mode)
        or value.st_uid != 0
        or value.st_gid != 0
        or stat.S_IMODE(value.st_mode) != mode
        or value.st_nlink < 2
    ):
        die()
    return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)


def parents_safe(path):
    current = "/"
    for component in path.strip("/").split("/"):
        current = os.path.join(current, component)
        value = os.stat(current, follow_symlinks=False)
        if not stat.S_ISDIR(value.st_mode) or value.st_uid != 0 or value.st_mode & 0o022:
            die()


def regular_at(dirfd, name, mode=0o600):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=dirfd)
    value = os.fstat(fd)
    if (
        not stat.S_ISREG(value.st_mode)
        or value.st_uid != 0
        or value.st_gid != 0
        or stat.S_IMODE(value.st_mode) != mode
        or value.st_nlink != 1
    ):
        os.close(fd)
        die()
    return fd, value


def recovery_lock(rootfd):
    fd = os.open(
        RECOVERY_LOCK,
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
        dir_fd=rootfd,
    )
    value = os.fstat(fd)
    if (
        not stat.S_ISREG(value.st_mode)
        or value.st_uid != 0
        or value.st_gid != 0
        or stat.S_IMODE(value.st_mode) != 0o600
        or value.st_nlink != 1
    ):
        os.close(fd)
        die()
    fcntl.flock(fd, fcntl.LOCK_EX)
    os.fsync(rootfd)
    return fd


def current_unit():
    try:
        with open("/proc/self/cgroup", encoding="ascii") as source:
            paths = tuple(line.rstrip().split(":", 2)[-1] for line in source)
    except OSError:
        die()
    matches = tuple(unit for unit in UNITS if any(path.endswith("/" + unit) for path in paths))
    if len(matches) > 1:
        die()
    return matches[0] if matches else None


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii") + b"\n"


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def manifest_digest():
    parents_safe(MANIFEST)
    fd = os.open(MANIFEST, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o644
            or metadata.st_nlink != 1
        ):
            die()
        manifest = load_json_fd(fd)
    finally:
        os.close(fd)
    required = {
        "artifacts",
        "codex_version",
        "model",
        "newsbot_codex_gid",
        "newsbot_codex_uid",
        "newsbot_gid",
        "newsbot_uid",
        "version",
    }
    if (
        set(manifest) != required
        or manifest["version"] != "newsbot-codex-release-manifest-v1"
        or manifest["codex_version"] != "0.146.0"
        or manifest["model"] != "gpt-5.6-terra"
        or not isinstance(manifest["artifacts"], list)
    ):
        die()
    try:
        newsbot = pwd.getpwnam("newsbot")
        codex = pwd.getpwnam("newsbot-codex")
    except KeyError:
        die()
    if (
        manifest["newsbot_uid"] != newsbot.pw_uid
        or manifest["newsbot_gid"] != newsbot.pw_gid
        or manifest["newsbot_codex_uid"] != codex.pw_uid
        or manifest["newsbot_codex_gid"] != codex.pw_gid
    ):
        die()
    seen = set()
    for artifact in manifest["artifacts"]:
        if not isinstance(artifact, dict) or set(artifact) != {
            "group",
            "mode",
            "owner",
            "path",
            "sha256",
        }:
            die()
        path = artifact["path"]
        if path not in MANIFEST_ARTIFACTS or path in seen:
            die()
        seen.add(path)
        if (
            artifact["owner"] != 0
            or artifact["group"] != 0
            or not isinstance(artifact["mode"], str)
            or len(artifact["mode"]) != 4
            or any(character not in "01234567" for character in artifact["mode"])
            or not isinstance(artifact["sha256"], str)
            or len(artifact["sha256"]) != 64
            or any(character not in "0123456789abcdef" for character in artifact["sha256"])
        ):
            die()
        parents_safe(path)
        artifact_fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            current = os.fstat(artifact_fd)
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_uid != 0
                or current.st_gid != 0
                or stat.S_IMODE(current.st_mode) != int(artifact["mode"], 8)
                or current.st_nlink != 1
            ):
                die()
            hasher = hashlib.sha256()
            total = 0
            while chunk := os.read(artifact_fd, 1024 * 1024):
                total += len(chunk)
                if total > 512 * 1024 * 1024:
                    die()
                hasher.update(chunk)
            if hasher.hexdigest() != artifact["sha256"]:
                die()
        finally:
            os.close(artifact_fd)
    if seen != MANIFEST_ARTIFACTS:
        die()
    return digest(manifest)


def state_fd(rootfd):
    try:
        return regular_at(rootfd, STATE)
    except OSError:
        die()


def load_json_fd(fd):
    data = bytearray()
    while len(data) <= 65536:
        chunk = os.read(fd, 65537 - len(data))
        if not chunk:
            break
        data.extend(chunk)
    if not data or len(data) > 65536:
        die()
    try:
        value = json.loads(
            data,
            object_pairs_hook=lambda pairs: (
                dict(pairs) if len({x[0] for x in pairs}) == len(pairs) else (_ for _ in ()).throw(ValueError())
            ),
        )
    except (ValueError, json.JSONDecodeError):
        die()
    if canonical(value) != bytes(data):
        die()
    return value


def load_state(rootfd):
    fd, inode = state_fd(rootfd)
    try:
        return load_json_fd(fd), inode
    finally:
        os.close(fd)


def publish_credential(dirty):
    runfd = root_dir("/run", 0o755)
    name = ".newsbot-codex-activation-v1." + secrets.token_hex(16)
    fd = -1
    try:
        try:
            group = grp.getgrnam("newsbot").gr_gid
        except KeyError:
            die()
        try:
            os.stat(os.path.basename(CREDENTIAL), dir_fd=runfd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            die()
        fd = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=runfd,
        )
        os.fchown(fd, 0, group)
        os.fchmod(fd, 0o440)
        value = {
            "activation": dirty["activation"],
            "manifest_sha256": dirty["manifest_sha256"],
            "unit": dirty["unit"],
            "version": 1,
        }
        data = canonical(value)
        if os.write(fd, data) != len(data):
            die()
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(name, os.path.basename(CREDENTIAL), src_dir_fd=runfd, dst_dir_fd=runfd)
        os.fsync(runfd)
        check = os.open(
            os.path.basename(CREDENTIAL),
            os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=runfd,
        )
        try:
            metadata = os.fstat(check)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_gid != group
                or stat.S_IMODE(metadata.st_mode) != 0o440
                or metadata.st_nlink != 1
                or load_json_fd(check) != value
            ):
                die()
        finally:
            os.close(check)
    finally:
        if fd >= 0:
            os.close(fd)
        os.close(runfd)


def clear_credential(dirty, required):
    runfd = root_dir("/run", 0o755)
    name = os.path.basename(CREDENTIAL)
    try:
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=runfd)
        except FileNotFoundError:
            if required:
                die()
            return
        try:
            group = grp.getgrnam("newsbot").gr_gid
        except KeyError:
            die()
        try:
            metadata = os.fstat(fd)
            value = load_json_fd(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_gid != group
                or stat.S_IMODE(metadata.st_mode) != 0o440
                or metadata.st_nlink != 1
            ):
                die()
            if dirty is not None and value != {
                "activation": dirty["activation"],
                "manifest_sha256": dirty["manifest_sha256"],
                "unit": dirty["unit"],
                "version": 1,
            }:
                die()
            current = os.stat(name, dir_fd=runfd, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
                die()
        finally:
            os.close(fd)
        os.unlink(name, dir_fd=runfd)
        os.fsync(runfd)
        try:
            os.stat(name, dir_fd=runfd, follow_symlinks=False)
        except FileNotFoundError:
            return
        die()
    finally:
        os.close(runfd)


def receipt(rootfd, name):
    if (
        not isinstance(name, str)
        or not name.startswith("clean-")
        or not name.endswith(".json")
        or "/" in name
        or len(name) > 96
    ):
        die()
    auditfd = root_dir(os.path.join(ROOT, AUDIT), 0o700)
    try:
        fd, _ = regular_at(auditfd, name)
        try:
            return load_json_fd(fd)
        finally:
            os.close(fd)
    finally:
        os.close(auditfd)


def valid_clean(rootfd, state, manifest):
    required = {
        "version",
        "state",
        "activation",
        "unit",
        "previous_state_sha256",
        "manifest_sha256",
        "receipt",
        "receipt_sha256",
    }
    if (
        set(state) != required
        or state["version"] != 1
        or state["state"] != "clean"
        or not isinstance(state["activation"], str)
        or len(state["activation"]) != 32
        or state["unit"] not in (*UNITS, "reset")
        or state["manifest_sha256"] != manifest
    ):
        die()
    proof = receipt(rootfd, state["receipt"])
    if hashlib.sha256(canonical(proof)).hexdigest() != state["receipt_sha256"]:
        die()
    if (
        proof.get("version") != 1
        or proof.get("kind") not in {"clean", "reset"}
        or proof.get("activation") != state["activation"]
        or proof.get("unit") != state["unit"]
        or proof.get("manifest_sha256") != manifest
        or proof.get("state_sha256") != state["previous_state_sha256"]
    ):
        die()


def atomic_state(rootfd, value):
    name = ".codex-state-v1." + secrets.token_hex(16)
    fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=rootfd)
    try:
        data = canonical(value)
        if os.write(fd, data) != len(data):
            die()
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(name, STATE, src_dir_fd=rootfd, dst_dir_fd=rootfd)
    os.fsync(rootfd)
    fd, _ = state_fd(rootfd)
    try:
        if load_json_fd(fd) != value:
            die()
    finally:
        os.close(fd)


def fixed_lock_free():
    parents_safe(LOCK)
    fd = os.open(LOCK, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        value = os.fstat(fd)
        try:
            group = pwd.getpwnam("newsbot-codex").pw_gid
        except KeyError:
            die()
        if (
            not stat.S_ISREG(value.st_mode)
            or value.st_uid != 0
            or value.st_gid != group
            or stat.S_IMODE(value.st_mode) != 0o640
            or value.st_nlink != 1
        ):
            die()
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            die()
    finally:
        os.close(fd)


def cgroup_empty(path, own_pid=None):
    if not os.path.exists(path):
        return
    try:
        found = set()
        for base, _dirs, files in os.walk(path):
            if "cgroup.procs" in files:
                with open(os.path.join(base, "cgroup.procs"), encoding="ascii") as source:
                    found.update(int(line) for line in source if line.strip())
    except (OSError, ValueError):
        die()
    if own_pid is not None:
        found.discard(own_pid)
    if found:
        die()


def inactive_and_empty(active_unit=None):
    if active_unit is not None and active_unit not in UNITS:
        die()
    for unit, cgroup in zip(UNITS, CGROUPS, strict=False):
        if unit == active_unit:
            cgroup_empty(cgroup, os.getpid())
            continue
        result = subprocess.run(
            ("/usr/bin/systemctl", "is-active", "--quiet", unit),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            die()
        cgroup_empty(cgroup)
    fixed_lock_free()


def write_receipt(rootfd, dirty, kind, manifest):
    auditfd = root_dir(os.path.join(ROOT, AUDIT), 0o700)
    name = "clean-" + secrets.token_hex(16) + ".json"
    value = {
        "activation": dirty["activation"],
        "kind": kind,
        "manifest_sha256": manifest,
        "state_sha256": digest(dirty),
        "time_ns": time.time_ns(),
        "unit": dirty["unit"],
        "version": 1,
    }
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=auditfd)
        try:
            data = canonical(value)
            if os.write(fd, data) != len(data):
                die()
            os.fsync(fd)
        finally:
            os.close(fd)
        os.fsync(auditfd)
    finally:
        os.close(auditfd)
    return name, value


def start():
    rootfd = root_dir(ROOT, 0o700)
    lockfd = recovery_lock(rootfd)
    try:
        unit = current_unit()
        if unit not in UNITS:
            die()
        manifest = manifest_digest()
        current, _ = load_state(rootfd)
        valid_clean(rootfd, current, manifest)
        inactive_and_empty(unit)
        dirty = {
            "activation": secrets.token_hex(16),
            "manifest_sha256": manifest,
            "previous_activation": current["activation"],
            "previous_state_sha256": digest(current),
            "state": "dirty",
            "unit": unit,
            "version": 1,
        }
        atomic_state(rootfd, dirty)
        publish_credential(dirty)
    finally:
        os.close(lockfd)
        os.close(rootfd)


def clean(kind):
    rootfd = root_dir(ROOT, 0o700)
    lockfd = recovery_lock(rootfd)
    try:
        manifest = manifest_digest()
        unit = current_unit()
        if kind == "clean":
            if unit not in UNITS:
                die()
            dirty, _ = load_state(rootfd)
            if (
                set(dirty)
                != {
                    "activation",
                    "manifest_sha256",
                    "previous_activation",
                    "previous_state_sha256",
                    "state",
                    "unit",
                    "version",
                }
                or dirty["version"] != 1
                or dirty["state"] != "dirty"
                or dirty["unit"] != unit
                or dirty["manifest_sha256"] != manifest
            ):
                die()
            inactive_and_empty(unit)
        else:
            if unit is not None:
                die()
            inactive_and_empty()
            try:
                dirty, _ = load_state(rootfd)
            except (Blocked, OSError):
                dirty = {
                    "activation": secrets.token_hex(16),
                    "manifest_sha256": manifest,
                    "previous_activation": "0" * 32,
                    "previous_state_sha256": "0" * 64,
                    "state": "dirty",
                    "unit": "reset",
                    "version": 1,
                }
            else:
                dirty = {
                    "activation": secrets.token_hex(16),
                    "manifest_sha256": manifest,
                    "previous_activation": str(dirty.get("activation", "0" * 32)),
                    "previous_state_sha256": digest(dirty),
                    "state": "dirty",
                    "unit": "reset",
                    "version": 1,
                }
            atomic_state(rootfd, dirty)
            clear_credential(None, False)
        name, proof = write_receipt(rootfd, dirty, kind, manifest)
        clear_credential(dirty, kind == "clean")
        clean_state = {
            "activation": dirty["activation"],
            "manifest_sha256": manifest,
            "previous_state_sha256": digest(dirty),
            "receipt": name,
            "receipt_sha256": hashlib.sha256(canonical(proof)).hexdigest(),
            "state": "clean",
            "unit": dirty["unit"],
            "version": 1,
        }
        atomic_state(rootfd, clean_state)
    finally:
        os.close(lockfd)
        os.close(rootfd)


def inspect():
    blocked = True
    complete = False
    try:
        rootfd = root_dir(ROOT, 0o700)
        try:
            manifest = manifest_digest()
            value, _ = load_state(rootfd)
            valid_clean(rootfd, value, manifest)
            inactive_and_empty()
            blocked = False
            complete = True
        finally:
            os.close(rootfd)
    except Blocked:
        pass
    print(
        json.dumps(
            {
                "blocked": blocked,
                "latch_attested": complete,
                "nonempty_service_count": 0 if complete else len(UNITS),
                "proof_complete": complete,
                "service_count": len(UNITS),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if complete else 1


def main():
    if os.geteuid() != 0 or len(sys.argv) != 2 or sys.argv[1] not in {"start", "stop", "inspect", "reset"}:
        return 1
    try:
        if sys.argv[1] == "start":
            start()
        elif sys.argv[1] == "stop":
            clean("clean")
        elif sys.argv[1] == "reset":
            clean("reset")
        else:
            return inspect()
        return 0
    except (Blocked, OSError):
        return 1


if __name__ == "__main__":
    sys.exit(main())

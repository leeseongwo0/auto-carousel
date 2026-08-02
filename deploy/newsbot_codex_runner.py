#!/usr/bin/env python3
"""Root-installed, argument-free Codex runner v1."""

import asyncio
import contextlib
import ctypes
import fcntl
import json
import os
import pwd
import selectors
import signal
import stat
import sys
import time

CONTRACT = "codex-runner-contract-v1"
RUNNER_USER = "newsbot-codex"
RUNNER_HOME = "/var/lib/newsbot-codex"
RUNNER_CWD = "/var/empty/newsbot-codex"
LOCK_DIR = "/run/lock/newsbot-codex"
LOCK_NAME = "generation.lock"
PINNED_CODEX = "/usr/local/libexec/codex-v0.146.0"
PINNED_MODEL = "gpt-5.6-terra"
PINNED_SCHEMA = "/usr/local/share/newsbot/copy_draft.schema.json"
INPUT_CAP = 524288
AUTH_CAP = 32768
STDOUT_CAP = 262144
STDERR_CAP = 131072
BUDGET = 210.0


class RunnerError(Exception):
    def __init__(self, code):
        self.code = code


def remaining(deadline):
    value = deadline - time.monotonic()
    if value <= 0:
        raise RunnerError(22)
    return value


def attest_dir(path, mode, group=0):
    value = os.stat(path, follow_symlinks=False)
    if (
        not stat.S_ISDIR(value.st_mode)
        or value.st_uid != 0
        or value.st_gid != group
        or stat.S_IMODE(value.st_mode) != mode
    ):
        raise RunnerError(21)


def acquire_lock():
    try:
        group = pwd.getpwnam(RUNNER_USER).pw_gid
    except KeyError:
        raise RunnerError(21) from None
    attest_dir(LOCK_DIR, 0o750, group)
    fd = os.open(os.path.join(LOCK_DIR, LOCK_NAME), os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    value = os.fstat(fd)
    if (
        not stat.S_ISREG(value.st_mode)
        or value.st_uid != 0
        or value.st_gid != group
        or stat.S_IMODE(value.st_mode) != 0o640
        or value.st_nlink != 1
    ):
        os.close(fd)
        raise RunnerError(21)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise RunnerError(25) from None
    return fd


def no_duplicate_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError()
        value[key] = item
    return value


def read_input(deadline):
    selector = selectors.DefaultSelector()
    selector.register(sys.stdin.buffer, selectors.EVENT_READ)
    data = bytearray()
    try:
        while True:
            events = selector.select(remaining(deadline))
            if not events:
                raise RunnerError(22)
            chunk = os.read(sys.stdin.fileno(), INPUT_CAP + 1 - len(data))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > INPUT_CAP:
                raise RunnerError(23)
    finally:
        selector.close()
    raw = bytes(data)
    if raw.startswith(b"\xef\xbb\xbf"):
        raise RunnerError(23)
    try:
        decoded = raw.decode("utf-8")
        value = json.loads(decoded, object_pairs_hook=no_duplicate_object)
        if not isinstance(value, dict):
            raise ValueError()
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise RunnerError(23) from None
    if set(value) != {"contract", "system_instruction", "user_payload"} or value["contract"] != CONTRACT:
        raise RunnerError(23)
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if raw != canonical:
        raise RunnerError(23)
    return raw


async def drain(stream, cap, retain, deadline):
    saved = bytearray()
    size = 0
    while True:
        try:
            part = await asyncio.wait_for(stream.read(65536), remaining(deadline))
        except TimeoutError:
            raise RunnerError(22) from None
        if not part:
            return bytes(saved)
        size += len(part)
        if size > cap:
            raise RunnerError(24)
        if retain:
            saved.extend(part)


async def stop_and_reap(proc, drains):
    if proc.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGTERM)
        try:
            await asyncio.wait_for(proc.wait(), 5)
        except TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
    try:
        await asyncio.wait_for(proc.wait(), 5)
    except TimeoutError:
        raise RunnerError(27) from None
    try:
        await asyncio.wait_for(asyncio.gather(*drains, return_exceptions=True), 5)
    except TimeoutError:
        raise RunnerError(27) from None


async def execute(argv, stdin, stdout_cap, stderr_cap, deadline, auth=False):
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=RUNNER_CWD,
            env=os.environ.copy(),
            start_new_session=True,
        )
    except (OSError, ValueError):
        raise RunnerError(21) from None
    out_task = asyncio.create_task(drain(proc.stdout, stdout_cap, not auth, deadline))
    err_task = asyncio.create_task(drain(proc.stderr, stderr_cap, False, deadline))
    try:
        if stdin:
            proc.stdin.write(stdin)
            await asyncio.wait_for(proc.stdin.drain(), remaining(deadline))
        proc.stdin.close()
        await asyncio.wait_for(proc.wait(), remaining(deadline))
        stdout, _stderr = await asyncio.gather(out_task, err_task)
    except (RunnerError, TimeoutError, BrokenPipeError):
        await stop_and_reap(proc, (out_task, err_task))
        code = 22 if time.monotonic() >= deadline else 24
        raise RunnerError(code) from None
    if proc.returncode != 0:
        raise RunnerError(20 if auth else 26)
    return stdout


def prepare_identity():
    try:
        account = pwd.getpwnam(RUNNER_USER)
    except KeyError:
        raise RunnerError(21) from None
    if os.geteuid() != account.pw_uid or os.getegid() != account.pw_gid or set(os.getgroups()) - {account.pw_gid}:
        raise RunnerError(21)
    os.umask(0o077)
    with contextlib.suppress(AttributeError, OSError):
        ctypes.CDLL(None).prctl(4, 0, 0, 0, 0)
    attest_dir(RUNNER_CWD, 0o755)
    os.chdir(RUNNER_CWD)
    os.environ.clear()
    os.environ.update(
        {
            "HOME": RUNNER_HOME,
            "CODEX_HOME": RUNNER_HOME + "/.codex",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/bin:/bin",
            "SSL_CERT_FILE": "/etc/ssl/certs/ca-certificates.crt",
        }
    )


def main():
    if len(sys.argv) != 1:
        return 23
    deadline = time.monotonic() + BUDGET
    lock_fd = None
    try:
        prepare_identity()
        payload = read_input(deadline)
        lock_fd = acquire_lock()
        asyncio.run(execute([PINNED_CODEX, "login", "status"], b"", AUTH_CAP, AUTH_CAP, deadline, True))
        output = asyncio.run(
            execute(
                [
                    PINNED_CODEX,
                    "exec",
                    "--ephemeral",
                    "--ignore-user-config",
                    "--ignore-rules",
                    "--color",
                    "never",
                    "--skip-git-repo-check",
                    "--model",
                    PINNED_MODEL,
                    "--output-schema",
                    PINNED_SCHEMA,
                    "-",
                ],
                payload,
                STDOUT_CAP,
                STDERR_CAP,
                deadline,
            )
        )
        try:
            text = output.decode("utf-8")
            parsed, end = json.JSONDecoder().raw_decode(text)
            if not isinstance(parsed, dict) or any(character not in " \t\r\n\f\v" for character in text[end:]):
                raise ValueError()
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return 27
        sys.stdout.buffer.write(output)
        return 0
    except RunnerError as exc:
        return exc.code
    except Exception:
        return 27
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


if __name__ == "__main__":
    sys.exit(main())

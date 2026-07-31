"""Fixed, privilege-separated Codex CLI generation provider."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import stat
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from newsbot.ai.base import GenerationRequest, ProviderError
from newsbot.ai.structured_copy import SYSTEM_INSTRUCTION, validate_draft_mapping
from newsbot.copywriting import CopyDraft

_CONTRACT = "codex-runner-contract-v1"
_INPUT_LIMIT = 524_288
_STDOUT_LIMIT = 262_144
_STDERR_LIMIT = 131_072
_TIMEOUT_SECONDS = 235
_REAP_SECONDS = 10
_ARGV = (
    "/usr/bin/sudo",
    "-n",
    "-H",
    "-u",
    "newsbot-codex",
    "--",
    "/usr/local/libexec/newsbot-codex-runner-v1",
)
_CWD = "/var/empty/newsbot-provider"
_ENV = {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}


class _CodexError(ProviderError):
    def __init__(self) -> None:
        super().__init__(f"{type(self).__name__}: generation failed")


class CodexAuthUnavailableError(_CodexError):
    """The runner could not verify its device-authenticated Codex session."""


class CodexRunnerConfigError(_CodexError):
    """The runner rejected its fixed configuration or release artifacts."""


class CodexTimeoutError(_CodexError):
    """The runner's internal deadline elapsed."""


class CodexInputLimitError(_CodexError):
    """The canonical runner input exceeded its fixed cap."""


class CodexOutputLimitError(_CodexError):
    """The runner output exceeded a fixed transport cap."""


class CodexBusyError(_CodexError):
    """The account-wide runner lock was busy."""


class CodexNonzeroError(_CodexError):
    """Codex failed after a successful runner preflight."""


class CodexSupervisorError(_CodexError):
    """The target-UID runner could not supervise its child process."""


class CodexUnknownExitError(_CodexError):
    """The privileged runner exited with an unrecognized status."""


class CodexOuterTimeoutError(_CodexError):
    """The provider-side deadline elapsed before the runner completed."""


class CodexInvalidDraftError(_CodexError):
    """The successful runner output was not a valid CopyDraft."""


class CodexRunnerAttestationError(_CodexError):
    """A fixed production launch artifact failed ownership or file-type checks."""


_EXIT_ERRORS: dict[int, type[_CodexError]] = {
    20: CodexAuthUnavailableError,
    21: CodexRunnerConfigError,
    22: CodexTimeoutError,
    23: CodexInputLimitError,
    24: CodexOutputLimitError,
    25: CodexBusyError,
    26: CodexNonzeroError,
    27: CodexSupervisorError,
}


@dataclass(frozen=True, slots=True)
class _LaunchResult:
    returncode: int
    stdout: bytes


_Launcher = Callable[[bytes], Awaitable[_LaunchResult]]
_Attester = Callable[[], None]


class _CodexCliCore:
    """Internal dependency-injectable encoder, launcher, and decoder seam."""

    def __init__(self, launcher: _Launcher | None = None, attester: _Attester | None = None) -> None:
        self._launcher = launcher or _launch_production
        self._attester = attester or _attest_production

    async def generate(self, request: GenerationRequest) -> CopyDraft:
        payload = _encode_request(request)
        if len(payload) > _INPUT_LIMIT:
            raise CodexInputLimitError()
        self._attester()
        result = await self._launcher(payload)
        if result.returncode != 0:
            raise _EXIT_ERRORS.get(result.returncode, CodexUnknownExitError)()
        return _decode_draft(result.stdout, request)


class CodexCliProvider:
    """Production provider with immutable runner, environment, and transport limits."""

    def __init__(self) -> None:
        self._core = _CodexCliCore()

    async def generate(self, request: GenerationRequest) -> CopyDraft:
        return await self._core.generate(request)


def _encode_request(request: GenerationRequest) -> bytes:
    payload = {
        "contract": _CONTRACT,
        "system_instruction": SYSTEM_INSTRUCTION,
        "user_payload": json.loads(_serialize_evidence_for_runner(request)),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _serialize_evidence_for_runner(request: GenerationRequest) -> str:
    # Kept local to avoid exposing the injectable seam through the public provider.
    from newsbot.ai.structured_copy import serialize_evidence

    return serialize_evidence(request)


def _decode_draft(output: bytes, request: GenerationRequest) -> CopyDraft:
    try:
        text = output.decode("utf-8", errors="strict")
        if not text.startswith("{"):
            raise ValueError("runner output must be one JSON object")
        value = json.loads(text, object_pairs_hook=_no_duplicate_object)
        if not isinstance(value, dict):
            raise ValueError("runner output must be an object")
        return validate_draft_mapping(value, request)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise CodexInvalidDraftError() from exc


def _no_duplicate_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


async def _launch_production(payload: bytes) -> _LaunchResult:
    try:
        process = await asyncio.create_subprocess_exec(
            *_ARGV,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=_CWD,
            env=_ENV,
            start_new_session=True,
        )
    except OSError as exc:
        raise CodexUnknownExitError() from exc
    try:
        async with asyncio.timeout(_TIMEOUT_SECONDS):
            stdout_task = asyncio.create_task(_read_limited(process.stdout, _STDOUT_LIMIT, retain=True))
            stderr_task = asyncio.create_task(_read_limited(process.stderr, _STDERR_LIMIT, retain=False))
            try:
                assert process.stdin is not None
                process.stdin.write(payload)
                await process.stdin.drain()
            finally:
                if process.stdin is not None:
                    process.stdin.close()
            done, _ = await asyncio.wait((stdout_task, stderr_task), return_when=asyncio.FIRST_COMPLETED)
            if any(task.result()[1] for task in done):
                await _terminate_process_group(process)
                await asyncio.gather(stdout_task, stderr_task)
                raise CodexOutputLimitError()
            (stdout, stdout_overflow), (_, stderr_overflow) = await asyncio.gather(stdout_task, stderr_task)
            if stdout_overflow or stderr_overflow or stdout is None:
                await _terminate_process_group(process)
                raise CodexOutputLimitError()
            await process.wait()
            return _LaunchResult(process.returncode if process.returncode is not None else -1, stdout)
    except TimeoutError as exc:
        await _terminate_process_group(process)
        raise CodexOuterTimeoutError() from exc
    except asyncio.CancelledError:
        await _terminate_process_group(process)
        raise
    except BaseException:
        await _terminate_process_group(process)
        raise


async def _read_limited(reader: asyncio.StreamReader | None, limit: int, *, retain: bool) -> tuple[bytes | None, bool]:
    if reader is None:
        return None, True
    chunks: list[bytes] = []
    remaining = limit
    overflowed = False
    while chunk := await reader.read(65_536):
        if len(chunk) > remaining:
            overflowed = True
        elif retain:
            chunks.append(chunk)
        remaining = max(0, remaining - len(chunk))
        if overflowed:
            return None, True
    return (None if overflowed else b"".join(chunks), overflowed)


async def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    _signal_process_group(process.pid, signal.SIGTERM)
    try:
        await asyncio.wait_for(process.wait(), _REAP_SECONDS)
        return
    except TimeoutError:
        _signal_process_group(process.pid, signal.SIGKILL)
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(process.wait(), _REAP_SECONDS)


def _signal_process_group(pid: int | None, sig: signal.Signals) -> None:
    if pid is None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(pid, sig)


def _attest_production() -> None:
    try:
        _attest_regular_file(Path(_ARGV[0]))
        _attest_regular_file(Path(_ARGV[-1]))
        _attest_empty_directory(Path(_CWD))
    except OSError as exc:
        raise CodexRunnerAttestationError() from exc


def _attest_regular_file(path: Path) -> None:
    status = path.lstat()
    if not stat.S_ISREG(status.st_mode) or stat.S_ISLNK(status.st_mode) or status.st_uid != 0 or status.st_nlink != 1:
        raise CodexRunnerAttestationError()
    if status.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise CodexRunnerAttestationError()


def _attest_empty_directory(path: Path) -> None:
    status = path.lstat()
    if (
        not stat.S_ISDIR(status.st_mode)
        or stat.S_ISLNK(status.st_mode)
        or status.st_uid != 0
        or stat.S_IMODE(status.st_mode) != 0o755
    ):
        raise CodexRunnerAttestationError()
    if any(path.iterdir()):
        raise CodexRunnerAttestationError()


__all__ = [
    "CodexAuthUnavailableError",
    "CodexBusyError",
    "CodexCliProvider",
    "CodexInputLimitError",
    "CodexInvalidDraftError",
    "CodexNonzeroError",
    "CodexOuterTimeoutError",
    "CodexOutputLimitError",
    "CodexRunnerAttestationError",
    "CodexRunnerConfigError",
    "CodexSupervisorError",
    "CodexTimeoutError",
    "CodexUnknownExitError",
]

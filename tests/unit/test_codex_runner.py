from __future__ import annotations

import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot.ai.structured_copy import RESPONSE_SCHEMA, SYSTEM_INSTRUCTION


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location(
        "test_codex_runner_module", Path(__file__).parents[2] / "deploy/newsbot_codex_runner.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _envelope(runner: object) -> bytes:
    value = {"contract": runner.CONTRACT, "system_instruction": "fixed", "user_payload": {"facts": []}}
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _read_with(runner: object, monkeypatch: pytest.MonkeyPatch, chunks: list[bytes]) -> bytes:
    class Selector:
        def register(self, *args: object) -> None:
            pass

        def select(self, timeout: float) -> list[object]:
            return [object()] if chunks else [object()]

        def close(self) -> None:
            pass

    monkeypatch.setattr(runner.selectors, "DefaultSelector", Selector)
    monkeypatch.setattr(runner.sys, "stdin", SimpleNamespace(buffer=SimpleNamespace(), fileno=lambda: 42))
    monkeypatch.setattr(runner.os, "read", lambda fd, count: chunks.pop(0) if chunks else b"")
    return runner.read_input(float("inf"))


def test_read_input_requires_exact_no_newline_canonical_bytes(runner: object, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _envelope(runner)
    assert not payload.endswith(b"\n")
    assert _read_with(runner, monkeypatch, [payload]) == payload


@pytest.mark.parametrize(
    "payload",
    [
        b'\xef\xbb\xbf{"contract":"codex-runner-contract-v1","system_instruction":"x","user_payload":{}}',
        b'{"contract":"codex-runner-contract-v1","contract":"codex-runner-contract-v1","system_instruction":"x","user_payload":{}}',
        b'{"contract":"codex-runner-contract-v1","system_instruction":"x","user_payload":{},"extra":true}',
        b'{"contract":"codex-runner-contract-v1","system_instruction":"x","user_payload":{}}\n',
    ],
)
def test_read_input_rejects_bom_duplicates_extra_fields_and_noncanonical_bytes(
    runner: object, monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    with pytest.raises(runner.RunnerError) as raised:
        _read_with(runner, monkeypatch, [payload])
    assert raised.value.code == 23


def test_read_input_rejects_byte_over_cap(runner: object, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(runner.RunnerError) as raised:
        _read_with(runner, monkeypatch, [b"x" * (runner.INPUT_CAP + 1)])
    assert raised.value.code == 23


def test_runner_constants_pin_environment_and_codex_argv(runner: object) -> None:
    assert (runner.INPUT_CAP, runner.STDOUT_CAP, runner.STDERR_CAP) == (524288, 262144, 131072)
    assert (runner.PINNED_CODEX, runner.PINNED_MODEL, runner.PINNED_SCHEMA) == (
        "/usr/local/libexec/codex-v0.146.0",
        "gpt-5.6-terra",
        "/usr/local/share/newsbot/copy_draft.schema.json",
    )
    assert runner.RUNNER_CWD == "/var/empty/newsbot-codex"

def test_bundled_output_schema_matches_runtime_contract() -> None:
    schema = json.loads(
        (Path(__file__).parents[2] / "src/newsbot/ai/resources/copy_draft.schema.json").read_text(encoding="utf-8")
    )

    assert schema == RESPONSE_SCHEMA
    assert schema["properties"]["draft"] == {"type": "boolean", "const": True}
    assert schema["properties"]["source_reported"] == {"type": "boolean", "const": True}
    assert "page_count_mode is flexible" in SYSTEM_INSTRUCTION
    assert "concise Korean card-news copy" in SYSTEM_INSTRUCTION
    assert "exact pair from the same supplied evidence" in SYSTEM_INSTRUCTION



def test_attest_dir_accepts_fixed_runner_group_only(runner: object, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runner.os,
        "stat",
        lambda path, **_: SimpleNamespace(
            st_mode=runner.stat.S_IFDIR | 0o750,
            st_uid=0,
            st_gid=997,
        ),
    )

    runner.attest_dir(runner.LOCK_DIR, 0o750, 997)

    with pytest.raises(runner.RunnerError) as raised:
        runner.attest_dir(runner.LOCK_DIR, 0o750)
    assert raised.value.code == 21

def test_execute_maps_auth_and_generation_failure_codes_without_running_codex(
    runner: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Process:
        stdin = SimpleNamespace(write=lambda value: None, drain=None, close=lambda: None)
        stdout = None
        stderr = None
        returncode = 1
        pid = 1

        async def wait(self) -> None:
            return None

    async def spawn(*args: object, **kwargs: object) -> Process:
        return Process()

    async def drain(stream: object, cap: int, retain: bool, deadline: float) -> bytes:
        return b""

    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(runner, "drain", drain)
    for auth, expected in ((True, 20), (False, 26)):
        with pytest.raises(runner.RunnerError) as raised:
            asyncio.run(runner.execute([runner.PINNED_CODEX], b"", 1, 1, float("inf"), auth=auth))
        assert raised.value.code == expected


def test_stop_and_reap_kills_only_after_bounded_wait(runner: object, monkeypatch: pytest.MonkeyPatch) -> None:
    signals: list[object] = []

    class Process:
        returncode = None
        pid = 9

        async def wait(self) -> None:
            return None

    monkeypatch.setattr(runner.os, "killpg", lambda pid, signal: signals.append(signal))
    asyncio.run(runner.stop_and_reap(Process(), ()))
    assert signals == [runner.signal.SIGTERM]

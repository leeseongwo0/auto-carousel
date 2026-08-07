from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from newsbot.ai import codex_cli
from newsbot.ai.base import FactClaim, GenerationRequest


def _request(*, page_count: int = 1, flexible_page_count: bool = False) -> GenerationRequest:
    return GenerationRequest(
        candidate_id=1,
        source_version_ids=(1,),
        page_count=page_count,
        facts=(
            FactClaim(
                id="claim_a",
                source_version_id=1,
                source_identity="source",
                material_identity="material",
                observation_identity="observation",
                captured_at="2026-01-01T00:00:00Z",
                source_url=None,
                evidence="evidence",
                evidence_spans=((0, 8),),
                conflicts=(),
                uncertainty=(),
            ),
        ),
        flexible_page_count=flexible_page_count,
    )


def _draft() -> dict[str, object]:
    return {
        "draft": True,
        "source_reported": True,
        "category": "AI",
        "cover": {
            "title": "제목",
            "subtitle": "",
            "factual_units": [{"text": "evidence", "references": [{"claim_id": "claim_a", "source_version_id": 1}]}],
        },
        "bodies": [],
        "caption": {
            "hook": "요약",
            "context": "맥락",
            "details": "세부",
            "implications": "영향",
            "questions": "질문",
            "hashtags": ["#뉴스"],
        },
    }


def test_request_envelope_is_canonical_and_launch_surface_is_fixed() -> None:
    payload = codex_cli._encode_request(_request())
    assert (
        payload == json.dumps(json.loads(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    )
    assert json.loads(payload) == {
        "contract": "codex-runner-contract-v1",
        "system_instruction": codex_cli.SYSTEM_INSTRUCTION,
        "user_payload": json.loads(codex_cli._serialize_evidence_for_runner(_request())),
    }
    assert codex_cli._ARGV == (
        "/usr/bin/sudo",
        "-n",
        "-H",
        "-u",
        "newsbot-codex",
        "--",
        "/usr/local/libexec/newsbot-codex-runner-v1",
    )
    assert codex_cli._ENV == {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    assert codex_cli._CWD == "/var/empty/newsbot-provider"


def test_flexible_request_exposes_range_policy_and_accepts_model_selected_page_count() -> None:
    request = _request(page_count=8, flexible_page_count=True)
    payload = json.loads(codex_cli._serialize_evidence_for_runner(request))

    assert payload["page_count"] == 8
    assert payload["page_count_mode"] == "flexible"
    assert codex_cli._decode_draft(json.dumps(_draft()).encode(), request).page_count == 1

    with pytest.raises(codex_cli.CodexInvalidDraftError):
        codex_cli._decode_draft(json.dumps(_draft()).encode(), _request(page_count=8))


def test_core_accepts_exact_input_cap_and_rejects_one_byte_over(monkeypatch: pytest.MonkeyPatch) -> None:
    launched: list[bytes] = []

    async def launcher(payload: bytes) -> codex_cli._LaunchResult:
        launched.append(payload)
        return codex_cli._LaunchResult(0, json.dumps(_draft()).encode())

    core = codex_cli._CodexCliCore(launcher, lambda: None)
    monkeypatch.setattr(codex_cli, "_encode_request", lambda request: b"x" * codex_cli._INPUT_LIMIT)
    assert asyncio.run(core.generate(_request())).category == "AI"
    assert len(launched[0]) == 524288
    monkeypatch.setattr(codex_cli, "_encode_request", lambda request: b"x" * (codex_cli._INPUT_LIMIT + 1))
    with pytest.raises(codex_cli.CodexInputLimitError):
        asyncio.run(core.generate(_request()))


@pytest.mark.parametrize(
    ("code", "error"),
    [
        (20, codex_cli.CodexAuthUnavailableError),
        (21, codex_cli.CodexRunnerConfigError),
        (22, codex_cli.CodexTimeoutError),
        (23, codex_cli.CodexInputLimitError),
        (24, codex_cli.CodexOutputLimitError),
        (25, codex_cli.CodexBusyError),
        (26, codex_cli.CodexNonzeroError),
        (27, codex_cli.CodexSupervisorError),
        (99, codex_cli.CodexUnknownExitError),
    ],
)
def test_runner_exit_codes_are_stable_and_safe(code: int, error: type[Exception]) -> None:
    async def launcher(payload: bytes) -> codex_cli._LaunchResult:
        return codex_cli._LaunchResult(code, b"secret stdout")

    with pytest.raises(error) as raised:
        asyncio.run(codex_cli._CodexCliCore(launcher, lambda: None).generate(_request()))
    assert "secret stdout" not in str(raised.value)
    assert "stderr" not in str(raised.value)


@pytest.mark.parametrize(
    "output",
    [
        b"\xff",
        b"[]",
        b"{} trailing",
        b'{"draft":true,"draft":false}',
        json.dumps({"extra": True}).encode(),
        json.dumps(_draft()).encode(),
    ],
)
def test_decoder_rejects_noncanonical_or_schema_invalid_drafts_without_leaking_output(output: bytes) -> None:
    if output == json.dumps(_draft()).encode():
        assert codex_cli._decode_draft(output, _request()).category == "AI"
        return
    with pytest.raises(codex_cli.CodexInvalidDraftError) as raised:
        codex_cli._decode_draft(output, _request())
    assert str(raised.value) == "CodexInvalidDraftError: generation failed"


def test_timeout_cleanup_signals_process_group_then_reaps(monkeypatch: pytest.MonkeyPatch) -> None:
    signals: list[object] = []

    class Process:
        pid = 123
        returncode = None

        async def wait(self) -> None:
            return None

    async def timed_wait(awaitable: object, timeout: float) -> None:
        await awaitable  # type: ignore[misc]

    monkeypatch.setattr(codex_cli, "_signal_process_group", lambda pid, signal: signals.append(signal))
    monkeypatch.setattr(asyncio, "wait_for", timed_wait)
    asyncio.run(codex_cli._terminate_process_group(Process()))  # type: ignore[arg-type]
    assert signals == [codex_cli.signal.SIGTERM]
def test_production_codex_topology_drift_precedes_job_selection(monkeypatch, tmp_path: Path) -> None:
    from newsbot import cli
    from newsbot.automation import AutomationDriftError
    from newsbot.storage import Storage

    storage_open = Storage.open

    @contextmanager
    def open_storage(_path: Path):
        with storage_open(tmp_path / "codex.sqlite") as storage:
            yield storage

    class Pipeline:
        def select_codex_job_id(self, *, production_config: object | None = None) -> None:
            assert production_config is not None
            raise AutomationDriftError("drift")

    monkeypatch.setattr(cli, "_attest_codex_activation", lambda: "newsbot-generate-codex.service")
    monkeypatch.setattr(cli, "_database", lambda _args: Path("/var/lib/newsbot/newsbot.db"))
    monkeypatch.setattr(cli, "_config", lambda _args: SimpleNamespace(database_path=tmp_path / "codex.sqlite"))
    monkeypatch.setattr(cli, "validate_capabilities", lambda _capability: None)
    monkeypatch.setattr(cli.Storage, "open", open_storage)
    monkeypatch.setattr(cli, "NewsPipeline", lambda *_args, **_kwargs: Pipeline())

    with pytest.raises(AutomationDriftError):
        cli.generate_codex_once(
            SimpleNamespace(config=Path("/etc/newsbot/config.toml"), db=Path("/var/lib/newsbot/newsbot.db"))
        )


def test_matching_production_codex_topology_allows_no_job_flow(monkeypatch, tmp_path: Path, capsys) -> None:
    from newsbot import cli
    from newsbot.storage import Storage

    storage_open = Storage.open

    @contextmanager
    def open_storage(_path: Path):
        with storage_open(tmp_path / "codex.sqlite") as storage:
            yield storage

    validated: list[object | None] = []

    class Pipeline:
        def select_codex_job_id(self, *, production_config: object | None = None) -> None:
            validated.append(production_config)
            return None

    config = SimpleNamespace(database_path=tmp_path / "codex.sqlite")
    monkeypatch.setattr(cli, "_attest_codex_activation", lambda: "newsbot-generate-codex.service")
    monkeypatch.setattr(cli, "_database", lambda _args: Path("/var/lib/newsbot/newsbot.db"))
    monkeypatch.setattr(cli, "_config", lambda _args: config)
    monkeypatch.setattr(cli, "validate_capabilities", lambda _capability: None)
    monkeypatch.setattr(cli.Storage, "open", open_storage)
    monkeypatch.setattr(cli, "NewsPipeline", lambda *_args, **_kwargs: Pipeline())

    assert (
        cli.generate_codex_once(
            SimpleNamespace(config=Path("/etc/newsbot/config.toml"), db=Path("/var/lib/newsbot/newsbot.db"))
        )
        == 0
    )
    assert validated == [config]
    assert capsys.readouterr().out == '{"status": "no_job"}\n'

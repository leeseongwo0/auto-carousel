from __future__ import annotations

import importlib.util
import json
import os
import stat
import subprocess
import sys
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

SPEC = importlib.util.spec_from_file_location(
    "runtime_release", Path(__file__).parents[2] / "deploy/build_newsbot_release.py"
)
assert SPEC and SPEC.loader
runtime_release = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runtime_release
SPEC.loader.exec_module(runtime_release)


def test_cli_runtime_release_digest_is_bound_to_stable_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from newsbot import cli

    commit = "a" * 40
    release = tmp_path / commit
    entrypoint = release / "venv/bin/newsbot"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.write_text("#!/bin/sh\n", encoding="utf-8")
    manifest = release / "runtime-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": "newsbot-runtime-release-manifest-v1",
                "source_commit": commit,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    stable = tmp_path / "newsbot"
    stable.symlink_to(entrypoint)
    expected = sha256(manifest.read_bytes()).hexdigest()

    assert cli._attested_runtime_release_digest(stable, executing_prefix=release / "venv") == expected
    old_prefix = tmp_path / ("b" * 40) / "venv"
    old_prefix.mkdir(parents=True)
    with pytest.raises(RuntimeError, match="executing runtime"):
        cli._attested_runtime_release_digest(stable, executing_prefix=old_prefix)
    monkeypatch.setattr(cli, "_attested_runtime_release_digest", lambda: expected)
    cli._require_runtime_release_digest(expected)
    with pytest.raises(RuntimeError, match="does not match"):
        cli._require_runtime_release_digest("b" * 64)


def test_release_activation_rejects_digest_before_opening_database(monkeypatch: pytest.MonkeyPatch) -> None:
    from newsbot import automation, cli

    @contextmanager
    def no_lock():
        yield

    monkeypatch.setattr(automation, "cutover_locks", no_lock)
    monkeypatch.setattr(cli, "_attested_runtime_release_digest", lambda: "a" * 64)
    monkeypatch.setattr(
        cli.Storage,
        "open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("database opened")),
    )

    with pytest.raises(RuntimeError, match="does not match"):
        cli.automation_release_activate(SimpleNamespace(db=Path("unused.sqlite"), release_digest="b" * 64))


def test_attest_uv_accepts_only_expected_identity(tmp_path: Path) -> None:
    tool = tmp_path / "uv"
    tool.write_text("ignored")
    metadata = SimpleNamespace(st_mode=stat.S_IFREG | 0o755, st_uid=0, st_gid=0, st_nlink=1)

    result = runtime_release.attest_uv(
        tool,
        "a" * 64,
        lambda *_args, **_kwargs: SimpleNamespace(stdout="uv 0.8.8\n"),
        lstat=lambda _path: metadata,
        sha256=lambda _path: "a" * 64,
    )

    assert result["version"] == "0.8.8"
    for changed in ("st_uid", "st_gid", "st_nlink"):
        bad = SimpleNamespace(**vars(metadata))
        setattr(bad, changed, 1 if changed != "st_nlink" else 2)
        with pytest.raises(runtime_release.ReleaseError):
            runtime_release.attest_uv(
                tool,
                "a" * 64,
                lambda *_args, **_kwargs: SimpleNamespace(stdout="uv 0.8.8"),
                lstat=lambda _path, value=bad: value,
                sha256=lambda _path: "a" * 64,
            )


def test_commands_are_frozen_hashed_and_production_only(tmp_path: Path) -> None:
    paths = runtime_release.ReleasePaths.for_sha("a" * 40, tmp_path / "releases")
    builder = runtime_release.ReleaseBuilder(uv_sha256="a" * 64, uv=Path("/tmp/uv"), python=Path("/tmp/python"))

    commands = builder.commands(paths)

    _, export, _, sync, install = commands
    extra_values = [export[index + 1] for index, value in enumerate(export) if value == "--extra"]
    assert extra_values == ["telegram", "sheets"]
    assert export.count("--extra") == 2
    assert "--frozen" in export and "--no-dev" in export and "--no-emit-project" in export
    assert "--require-hashes" in sync
    assert "--no-deps" in install
    with pytest.raises(runtime_release.ReleaseError):
        builder.commands(paths, extras=("telegram", "unexpected"))


def test_switch_refuses_without_quiescence(tmp_path: Path) -> None:
    paths = runtime_release.ReleasePaths.for_sha("b" * 40, tmp_path / "releases")
    with pytest.raises(runtime_release.ReleaseError, match="quiescence"):
        runtime_release.atomic_switch(paths, tmp_path / "manifest.json", None, stable=tmp_path / "newsbot")


def test_held_locks_refuse_missing_or_wrong_identity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock_paths = tuple(tmp_path / f"{kind}.lock" for kind in ("collect", "telegram", "sheets"))
    monkeypatch.setattr(runtime_release, "LOCK_PATHS", lock_paths)
    monkeypatch.setattr(runtime_release, "_lock_owner", lambda: (os.getuid(), os.getgid()))
    with pytest.raises(runtime_release.ReleaseError, match="lock identity"), runtime_release.held_quiescence_locks():
        pass

    for path in lock_paths:
        path.write_bytes(b"")
        path.chmod(0o600)
    lock_paths[1].chmod(0o644)
    with pytest.raises(runtime_release.ReleaseError, match="lock identity"), runtime_release.held_quiescence_locks():
        pass


def test_switch_holds_locks_and_restores_verified_entrypoint_on_postcheck_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = runtime_release.ReleasePaths.for_sha("c" * 40, tmp_path / "releases")
    target = paths.venv / "bin/newsbot"
    target.parent.mkdir(parents=True)
    target.write_text("#!/bin/sh\n")
    target.chmod(0o755)
    previous = tmp_path / "previous-newsbot"
    previous.write_text("#!/bin/sh\n")
    previous.chmod(0o755)
    stable = tmp_path / "newsbot"
    stable.symlink_to(previous)

    held = False

    @contextmanager
    def locks():
        nonlocal held
        held = True
        try:
            yield
        finally:
            held = False

    checks: list[bool] = []

    def quiescent() -> bool:
        checks.append(held)
        return len(checks) < 3

    monkeypatch.setattr(runtime_release, "held_quiescence_locks", locks)
    monkeypatch.setattr(runtime_release, "candidate_canary", lambda *_args: None)
    monkeypatch.setattr(runtime_release, "missing_extra_refusals", lambda *_args: None)
    monkeypatch.setattr(runtime_release, "missing_extra_candidates", lambda _paths: {})

    with pytest.raises(runtime_release.ReleaseError, match="quiescence"):
        runtime_release.atomic_switch(paths, tmp_path / "manifest.json", quiescent, stable=stable)

    assert checks == [True, True, True]
    assert stable.resolve() == previous.resolve()


def test_switch_refuses_nonexecutable_candidate_before_first_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = runtime_release.ReleasePaths.for_sha("e" * 40, tmp_path / "releases")
    target = paths.venv / "bin/newsbot"
    target.parent.mkdir(parents=True)
    target.write_text("not executable")

    @contextmanager
    def locks():
        yield

    monkeypatch.setattr(runtime_release, "held_quiescence_locks", locks)
    monkeypatch.setattr(runtime_release, "candidate_canary", lambda *_args: None)
    monkeypatch.setattr(runtime_release, "missing_extra_refusals", lambda *_args: None)
    monkeypatch.setattr(runtime_release, "missing_extra_candidates", lambda _paths: {})

    with pytest.raises(runtime_release.ReleaseError, match="candidate entrypoint"):
        runtime_release.atomic_switch(paths, tmp_path / "manifest.json", lambda: True, stable=tmp_path / "newsbot")


def test_live_quiescence_requires_all_services_application_and_containment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = runtime_release.ReleasePaths.for_sha("c" * 40, tmp_path / "releases")
    monkeypatch.setattr(runtime_release, "_database_quiescent", lambda: True)
    monkeypatch.setattr(runtime_release, "_locks_quiescent", lambda: True)

    def runner(argv, **_kwargs):
        if argv[:2] == ["systemctl", "is-enabled"]:
            return SimpleNamespace(stdout="disabled\n")
        if argv[:2] == ["systemctl", "is-active"]:
            return SimpleNamespace(stdout="inactive\n")
        if argv[-1:] == ["inspect"]:
            return SimpleNamespace(stdout='{"blocked":false,"latch_attested":true,"proof_complete":true}\n')
        return SimpleNamespace(stdout="unused\n")

    assert runtime_release.live_quiescence(paths, runner)

    def active_runner(argv, **kwargs):
        result = runner(argv, **kwargs)
        if argv[:2] == ["systemctl", "is-active"] and argv[-1] == "newsbot-telegram.service":
            return SimpleNamespace(stdout="active\n")
        return result

    assert not runtime_release.live_quiescence(paths, active_runner)


def test_systemd_unit_digest_drift_is_refused(tmp_path: Path) -> None:
    source = tmp_path / "src"
    units = source / "deploy/systemd"
    units.mkdir(parents=True)
    for name in runtime_release.SYSTEMD_UNITS:
        (units / name).write_text(name)
    expected = runtime_release.systemd_unit_digests(source)
    runtime_release.attest_systemd_units(source, expected)
    (units / runtime_release.SYSTEMD_UNITS[0]).write_text("drift")
    with pytest.raises(runtime_release.ReleaseError, match="systemd unit"):
        runtime_release.attest_systemd_units(source, expected)


def test_missing_extra_candidates_are_refused_for_their_capabilities(tmp_path: Path) -> None:
    base = runtime_release.ReleasePaths.for_sha("d" * 40, tmp_path / "releases")
    base.source.mkdir(parents=True)
    (base.source / "uv.lock").write_text("frozen")
    candidates = runtime_release.missing_extra_candidates(base)
    for candidate in candidates.values():
        (candidate.venv / "bin").mkdir(parents=True)
        python = candidate.venv / "bin/python"
        python.write_text("#!/bin/false\n")
        python.chmod(0o755)
        candidate.dist.mkdir()
        (candidate.dist / "production-requirements.txt").write_text("package==1 --hash=sha256:abc\n")
        (candidate.dist / "telegram_news_bot-0.1.0-py3-none-any.whl").write_bytes(b"wheel")

    called: list[str] = []

    def missing_only(argv, **_kwargs):
        module = argv[-1]
        called.append(module)
        if (argv[0].find("missing-telegram") >= 0 and module == "telethon") or (
            argv[0].find("missing-sheets") >= 0 and module == "googleapiclient.discovery"
        ):
            raise subprocess.CalledProcessError(1, argv)
        return SimpleNamespace()

    runtime_release.missing_extra_refusals(candidates, missing_only)
    assert called == [
        "googleapiclient.discovery",
        "telethon",
        "telethon",
        "googleapiclient.discovery",
    ]

    with pytest.raises(runtime_release.ReleaseError, match="missing capability"):
        runtime_release.missing_extra_refusals(candidates, lambda *_args, **_kwargs: SimpleNamespace())


def test_missing_extra_refusal_rejects_an_unbuilt_candidate(tmp_path: Path) -> None:
    base = runtime_release.ReleasePaths.for_sha("1" * 40, tmp_path / "releases")
    base.source.mkdir(parents=True)
    (base.source / "uv.lock").write_text("frozen")
    candidates = runtime_release.missing_extra_candidates(base)

    with pytest.raises(runtime_release.ReleaseError, match="candidate attestation"):
        runtime_release.missing_extra_refusals(candidates)


def test_disposable_missing_extra_builds_create_candidate_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = runtime_release.ReleasePaths.for_sha("f" * 40, tmp_path / "releases")
    base.root.mkdir(parents=True)
    built: list[tuple[str, tuple[str, ...]]] = []
    builder = runtime_release.ReleaseBuilder(
        uv_sha256="a" * 64,
        releases=tmp_path / "releases",
    )

    def record(candidate: runtime_release.ReleasePaths, extras: tuple[str, ...]) -> None:
        assert candidate.root.is_dir()
        assert candidate.root.stat().st_mode & 0o777 == 0o755
        built.append((candidate.root.name, extras))

    monkeypatch.setattr(builder, "_build_candidate", record)

    candidates = builder.disposable_missing_extra_candidates(base)

    assert set(candidates) == {"telegram", "sheets"}
    assert built == [
        ("missing-telegram", ("sheets",)),
        ("missing-sheets", ("telegram",)),
    ]


def test_live_quiescence_refuses_enabled_timer_and_sheets_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = runtime_release.ReleasePaths.for_sha("e" * 40, tmp_path / "releases")
    monkeypatch.setattr(runtime_release, "_database_quiescent", lambda: True)
    monkeypatch.setattr(runtime_release, "_locks_quiescent", lambda: True)

    def runner(argv, **_kwargs):
        if argv[:2] == ["systemctl", "is-enabled"]:
            return SimpleNamespace(stdout="disabled\n")
        if argv[:2] == ["systemctl", "is-active"]:
            return SimpleNamespace(stdout="inactive\n")
        return SimpleNamespace(stdout='{"blocked":false,"latch_attested":true,"proof_complete":true}\n')

    def enabled_timer(argv, **kwargs):
        result = runner(argv, **kwargs)
        if argv == ["systemctl", "is-enabled", "newsbot-sheets.timer"]:
            return SimpleNamespace(stdout="enabled\n")
        return result

    assert not runtime_release.live_quiescence(paths, enabled_timer)
    monkeypatch.setattr(runtime_release, "_locks_quiescent", lambda: False)
    assert Path("/var/lib/newsbot/locks/sheets.lock") in runtime_release.LOCK_PATHS
    assert not runtime_release.live_quiescence(paths, runner)


def test_deployment_docs_require_one_quiescence_proof_across_build_and_switch() -> None:
    repository = Path(__file__).parents[2]
    operations = (repository / "docs/operations.md").read_text()
    guide = (repository / "vps-deployment-guide.html").read_text()

    cases = (
        (operations, "build <COMMIT_SHA>", "switch <COMMIT_SHA>"),
        (guide, "build $RELEASE_GIT_SHA", "switch $RELEASE_GIT_SHA"),
    )
    for document, build_marker, switch_marker in cases:
        proof = document.index('printf "quiescent\\n"')
        build = document.index(build_marker)
        switch = document.index(switch_marker)
        migration = document.index("init-db", switch)
        preview = document.index("automation-cutover-preview", migration)
        apply = document.index("automation-cutover-apply", preview)
        assert proof < build < switch < migration < preview < apply
        assert "--quiescence-proof" in document[build:switch]
        assert "--quiescence-proof" in document[switch:migration]

    assert "collect → Telegram → Sheets" in operations
    assert "collect → Telegram → Sheets" in guide
    architecture = (repository / "docs/architecture.md").read_text()
    requirements = (repository / "docs/requirements.md").read_text()
    assert architecture.index("switch/re-attest") < architecture.index("init-db")
    assert requirements.index("switch/re-attestation") < requirements.index("migration/FK check")

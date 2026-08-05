#!/usr/bin/env python3
"""Build and attest an immutable, dependency-complete Newsbot runtime release."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import pwd
import re
import sqlite3
import stat
import subprocess
import tomllib
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

UV = Path("/usr/local/libexec/uv-v0.8.8")
PYTHON = Path("/usr/bin/python3.12")
RELEASES = Path("/opt/newsbot/releases")
STABLE_ENTRYPOINT = Path("/usr/local/bin/newsbot")
UV_VERSION = "uv 0.8.8"
EXTRAS = ("sheets", "telegram")
SYSTEMD_UNITS = (
    "newsbot-collect.service",
    "newsbot-collect.timer",
    "newsbot-generate-codex-canary.service",
    "newsbot-generate-codex.service",
    "newsbot-generate-codex.timer",
    "newsbot-sheets.service",
    "newsbot-sheets.timer",
    "newsbot-telegram.service",
    "newsbot-telegram.timer",
)
TIMER_UNITS = (
    "newsbot-collect.timer",
    "newsbot-telegram.timer",
    "newsbot-sheets.timer",
    "newsbot-generate-codex.timer",
)
SERVICE_UNITS = (
    "newsbot-collect.service",
    "newsbot-telegram.service",
    "newsbot-sheets.service",
    "newsbot-generate-codex.service",
)
LOCK_PATHS = (
    Path("/var/lib/newsbot/locks/collect.lock"),
    Path("/var/lib/newsbot/locks/telegram.lock"),
    Path("/var/lib/newsbot/locks/sheets.lock"),
)
EXPECTED_CLOSURE = frozenset(
    {
        "certifi==2026.7.22",
        "cffi==2.1.0",
        "charset-normalizer==3.4.9",
        "cryptography==50.0.0",
        "google-api-core==2.33.0",
        "google-api-python-client==2.198.0",
        "google-auth==2.56.2",
        "google-auth-httplib2==0.4.0",
        "googleapis-common-protos==1.75.0",
        "httplib2==0.32.0",
        "idna==3.18",
        "proto-plus==1.28.2",
        "protobuf==7.35.1",
        "pyaes==1.6.1",
        "pyasn1==0.6.4",
        "pyasn1-modules==0.4.2",
        "pycparser==3.0",
        "pyparsing==3.3.2",
        "requests==2.34.2",
        "rsa==4.9.1",
        "telegram-news-bot==0.1.0",
        "telethon==1.44.0",
        "uritemplate==4.2.0",
        "urllib3==2.7.0",
    }
)
SHA = re.compile(r"^[0-9a-f]{40}$")


class ReleaseError(RuntimeError):
    """A redacted release gate failure."""


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _regular_root_owned(path: Path, mode: int | None = None) -> os.stat_result:
    value = path.lstat()
    if (
        not stat.S_ISREG(value.st_mode)
        or stat.S_ISLNK(value.st_mode)
        or value.st_uid != 0
        or value.st_gid != 0
        or value.st_nlink != 1
        or (mode is not None and stat.S_IMODE(value.st_mode) != mode)
    ):
        raise ReleaseError("artifact identity failed")
    return value


def _inside(path: Path, root: Path) -> Path:
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ReleaseError("release path failed") from exc
    # Refuse symlinks in any existing path component, including a symlinked release root.
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        if current.is_symlink():
            raise ReleaseError("release path failed")
    return path


def attest_uv(
    path: Path = UV,
    expected_sha256: str = "",
    runner: Callable[..., object] = subprocess.run,
    *,
    lstat: Callable[[Path], os.stat_result] | None = None,
    sha256: Callable[[Path], str] = digest,
) -> dict[str, str]:
    """Validate a root-owned pinned uv artifact before release filesystem mutation."""
    if not path.is_absolute() or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ReleaseError("uv identity failed")
    metadata = (lstat or (lambda candidate: candidate.lstat()))(path)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
        or metadata.st_nlink != 1
        or sha256(path) != expected_sha256
    ):
        raise ReleaseError("uv identity failed")
    try:
        result = runner([str(path), "--version"], check=True, capture_output=True, text=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError("uv identity failed") from exc
    if getattr(result, "stdout", "").strip() != UV_VERSION:
        raise ReleaseError("uv identity failed")
    return {"path": str(path), "sha256": expected_sha256, "version": "0.8.8"}


def _run(runner: Callable[..., object], argv: Sequence[str]) -> None:
    try:
        runner(list(argv), check=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError("release command failed") from exc


@dataclass(frozen=True)
class ReleasePaths:
    sha: str
    root: Path
    source: Path
    venv: Path
    dist: Path

    @classmethod
    def for_sha(cls, sha: str, releases: Path = RELEASES) -> ReleasePaths:
        if not SHA.fullmatch(sha):
            raise ReleaseError("release identity failed")
        root = releases / sha
        _inside(root, releases)
        return cls(sha, root, root / "src", root / "venv", root / "dist")


class ReleaseBuilder:
    """Filesystem/runner-injectable builder; command arguments are intentionally literal."""

    def __init__(
        self,
        *,
        uv_sha256: str,
        runner: Callable[..., object] = subprocess.run,
        uv: Path = UV,
        python: Path = PYTHON,
        releases: Path = RELEASES,
    ) -> None:
        self.uv_sha256, self.runner, self.uv, self.python, self.releases = uv_sha256, runner, uv, python, releases

    def commands(self, paths: ReleasePaths, extras: Sequence[str] = EXTRAS) -> list[list[str]]:
        selected = tuple(sorted(extras))
        if selected not in (EXTRAS, ("sheets",), ("telegram",)):
            raise ReleaseError("production capabilities failed")
        export_extras = ("telegram", "sheets") if selected == EXTRAS else selected
        wheel = paths.dist / "telegram_news_bot-0.1.0-py3-none-any.whl"
        requirements = paths.dist / "production-requirements.txt"
        export = [
            str(self.uv),
            "export",
            "--project",
            str(paths.source),
            "--frozen",
            "--format",
            "requirements-txt",
        ]
        for extra in export_extras:
            export.extend(("--extra", extra))
        export.extend(("--no-dev", "--no-emit-project", "--output-file", str(requirements)))
        return [
            [str(self.uv), "build", "--project", str(paths.source), "--wheel", "--out-dir", str(paths.dist)],
            export,
            [str(self.uv), "venv", "--python", str(self.python), str(paths.venv)],
            [
                str(self.uv),
                "pip",
                "sync",
                "--python",
                str(paths.venv / "bin/python"),
                "--require-hashes",
                str(requirements),
            ],
            [
                str(self.uv),
                "pip",
                "install",
                "--python",
                str(paths.venv / "bin/python"),
                "--no-deps",
                "--reinstall",
                str(wheel),
            ],
        ]

    def _build_candidate(self, paths: ReleasePaths, extras: Sequence[str]) -> None:
        if (
            not (paths.source / "uv.lock").is_file()
            or not (paths.source / "pyproject.toml").is_file()
            or paths.venv.exists()
        ):
            raise ReleaseError("release layout failed")
        paths.dist.mkdir(mode=0o755, exist_ok=False)
        os.chmod(paths.dist, 0o755)
        for argv in self.commands(paths, extras):
            _run(self.runner, argv)
        wheel = paths.dist / "telegram_news_bot-0.1.0-py3-none-any.whl"
        requirements = paths.dist / "production-requirements.txt"
        if not wheel.is_file() or not requirements.is_file() or "--hash=" not in requirements.read_text():
            raise ReleaseError("release output failed")
        self.attest_noneditable(paths)

    def build(self, sha: str) -> ReleasePaths:
        paths = ReleasePaths.for_sha(sha, self.releases)
        if self.uv != UV or self.python != PYTHON:
            raise ReleaseError("release tool identity failed")
        attest_uv(self.uv, self.uv_sha256, self.runner)
        self._build_candidate(paths, EXTRAS)
        return paths

    def disposable_missing_extra_candidates(self, paths: ReleasePaths) -> dict[str, ReleasePaths]:
        candidates = missing_extra_candidates(paths)
        for missing, selected in (("telegram", ("sheets",)), ("sheets", ("telegram",))):
            candidate = candidates[missing]
            candidate.root.mkdir(mode=0o755, exist_ok=False)
            os.chmod(candidate.root, 0o755)
            self._build_candidate(candidate, selected)
        return candidates

    def attest_noneditable(self, paths: ReleasePaths) -> None:
        site = _site_packages(paths.venv)
        for path in site.rglob("*.pth"):
            if paths.source.as_posix() in path.read_text(errors="ignore"):
                raise ReleaseError("editable provenance failed")
        direct = next(site.glob("telegram_news_bot-*.dist-info/direct_url.json"), None)
        if direct and json.loads(direct.read_text()).get("dir_info", {}).get("editable"):
            raise ReleaseError("editable provenance failed")


def _site_packages(venv: Path) -> Path:
    matches = sorted((venv / "lib").glob("python*/site-packages"))
    if len(matches) != 1:
        raise ReleaseError("runtime layout failed")
    return matches[0]


def _attest_missing_extra_candidate(paths: ReleasePaths, missing: str) -> None:
    if paths.root.name != f"missing-{missing}" or not (paths.source / "uv.lock").is_file():
        raise ReleaseError("candidate attestation failed")
    python = paths.venv / "bin/python"
    requirements = paths.dist / "production-requirements.txt"
    wheel = paths.dist / "telegram_news_bot-0.1.0-py3-none-any.whl"
    if (
        not python.is_file()
        or not os.access(python, os.X_OK)
        or not requirements.is_file()
        or "--hash=" not in requirements.read_text()
        or not wheel.is_file()
        or wheel.is_symlink()
    ):
        raise ReleaseError("candidate attestation failed")


def installed_closure(venv: Path) -> list[dict[str, str]]:
    site = _site_packages(venv)
    entries: list[dict[str, str]] = []
    for info in sorted(site.glob("*.dist-info")):
        metadata, record = info / "METADATA", info / "RECORD"
        if not metadata.is_file() or not record.is_file():
            raise ReleaseError("distribution metadata failed")
        fields = dict(line.split(": ", 1) for line in metadata.read_text().splitlines() if ": " in line)
        name, version = fields.get("Name"), fields.get("Version")
        if not name or not version:
            raise ReleaseError("distribution metadata failed")
        entries.append(
            {
                "name": name.lower().replace("_", "-"),
                "version": version,
                "record_sha256": digest(record),
                "location": str(info.relative_to(site)),
            }
        )
    entries.sort(key=lambda item: item["name"])
    actual = frozenset(f"{x['name']}=={x['version']}" for x in entries)
    if actual != EXPECTED_CLOSURE:
        raise ReleaseError("distribution closure failed")
    return entries


def package_resources(venv: Path) -> list[dict[str, str]]:
    site, package = _site_packages(venv), _site_packages(venv) / "newsbot"
    wanted = sorted((package / "migrations").glob("*.sql")) + [package / "ai/resources/copy_draft.schema.json"]
    if (
        not wanted
        or any(not item.is_file() for item in wanted)
        or not any(item.name == "007_systemd_automation.sql" for item in wanted)
    ):
        raise ReleaseError("package resource failed")
    return [{"path": str(item.relative_to(site)), "sha256": digest(item)} for item in wanted]


def systemd_unit_digests(source: Path) -> list[dict[str, str]]:
    units = source / "deploy/systemd"
    entries: list[dict[str, str]] = []
    for name in SYSTEMD_UNITS:
        unit = units / name
        if not unit.is_file() or unit.is_symlink():
            raise ReleaseError("systemd unit identity failed")
        entries.append({"path": f"deploy/systemd/{name}", "sha256": digest(unit)})
    return entries


def attest_systemd_units(source: Path, expected: object) -> None:
    if expected != systemd_unit_digests(source):
        raise ReleaseError("systemd unit identity failed")


def missing_extra_candidates(paths: ReleasePaths) -> dict[str, ReleasePaths]:
    return {
        missing: ReleasePaths(
            paths.sha,
            paths.root / f"missing-{missing}",
            paths.source,
            paths.root / f"missing-{missing}/venv",
            paths.root / f"missing-{missing}/dist",
        )
        for missing in ("telegram", "sheets")
    }


def missing_extra_refusals(candidates: dict[str, ReleasePaths], runner: Callable[..., object] = subprocess.run) -> None:
    required = {
        "telegram": ("telethon", "googleapiclient.discovery"),
        "sheets": ("googleapiclient.discovery", "telethon"),
    }
    if set(candidates) != set(required):
        raise ReleaseError("production capabilities failed")
    for missing, (missing_module, present_module) in required.items():
        candidate = candidates[missing]
        _attest_missing_extra_candidate(candidate, missing)
        capability_canary(candidate, present_module, runner)
        try:
            capability_canary(candidate, missing_module, runner)
        except ReleaseError:
            continue
        raise ReleaseError("missing capability accepted")


def capability_canary(paths: ReleasePaths, module: str, runner: Callable[..., object] = subprocess.run) -> None:
    script = "import importlib,sys; loaded=importlib.import_module(sys.argv[1]); assert str(loaded.__file__).startswith(sys.prefix + '/')"
    try:
        runner(
            [str(paths.venv / "bin/python"), "-I", "-c", script, module],
            check=True,
            env={"PATH": "/usr/bin:/bin"},
            capture_output=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError("candidate capability failed") from exc


def python_identity(python: Path, runner: Callable[..., object] = subprocess.run) -> dict[str, str]:
    code = "import json,platform,sys;print(json.dumps({'implementation':platform.python_implementation(),'version':platform.python_version(),'abi':sys.implementation.cache_tag}))"
    try:
        result = runner([str(python), "-I", "-c", code], check=True, capture_output=True, text=True)
        identity = json.loads(getattr(result, "stdout", ""))
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise ReleaseError("python identity failed") from exc
    if identity.get("implementation") != "CPython" or not identity.get("version", "").startswith("3.12."):
        raise ReleaseError("python identity failed")
    identity["path"] = str(python)
    identity["sha256"] = digest(python)
    return identity


def manifest(
    paths: ReleasePaths, uv_identity: dict[str, str], runner: Callable[..., object] = subprocess.run
) -> dict[str, object]:
    wheel = paths.dist / "telegram_news_bot-0.1.0-py3-none-any.whl"
    entrypoint = paths.venv / "bin/newsbot"
    if not wheel.is_file() or not entrypoint.is_file():
        raise ReleaseError("release output failed")
    python = paths.venv / "bin/python"
    lock = tomllib.loads((paths.source / "uv.lock").read_text())
    distributions = installed_closure(paths.venv)
    application = next((item for item in distributions if item["name"] == "telegram-news-bot"), None)
    if application is None:
        raise ReleaseError("distribution closure failed")
    result = {
        "version": "newsbot-runtime-release-manifest-v1",
        "uv": uv_identity,
        "python": python_identity(python, runner),
        "source_commit": paths.sha,
        "pyproject_sha256": digest(paths.source / "pyproject.toml"),
        "uv_lock_sha256": digest(paths.source / "uv.lock"),
        "lock_format": "uv",
        "lock_revision": lock.get("version"),
        "wheel": {
            "filename": wheel.name,
            "version": "0.1.0",
            "sha256": digest(wheel),
            "installed_record_sha256": application["record_sha256"],
        },
        "selected_extras": list(EXTRAS),
        "selected_dependency_groups": [],
        "requirements_sha256": digest(paths.dist / "production-requirements.txt"),
        "distributions": distributions,
        "resources": package_resources(paths.venv),
        "systemd_units": systemd_unit_digests(paths.source),
        "entrypoint": {"realpath": str(entrypoint.resolve()), "sha256": digest(entrypoint)},
    }
    if not isinstance(result["lock_revision"], int):
        raise ReleaseError("lock identity failed")
    return result


def write_manifest(paths: ReleasePaths, value: dict[str, object]) -> Path:
    output = paths.root / "runtime-manifest.json"
    data = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode() + b"\n"
    temporary = output.with_name(output.name + ".new")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
        os.chmod(temporary, 0o644)
    finally:
        os.close(fd)
    os.replace(temporary, output)
    _regular_root_owned(output, 0o644)
    return output


def candidate_canary(paths: ReleasePaths, manifest_path: Path, runner: Callable[..., object] = subprocess.run) -> None:
    # The candidate interpreter itself validates imports, metadata closure and packaged resource hashes.
    try:
        value = json.loads(manifest_path.read_text())
        if value.get("selected_extras") != list(EXTRAS) or value.get("selected_dependency_groups") != []:
            raise ReleaseError("candidate canary failed")
        attest_systemd_units(paths.source, value.get("systemd_units"))
    except (OSError, ValueError, ReleaseError) as exc:
        raise ReleaseError("candidate canary failed") from exc
    script = r"""import hashlib,importlib,importlib.metadata,json,pathlib,sys
m=json.load(open(sys.argv[1])); root=sys.prefix+'/'; site=root+'lib/python%s.%s/site-packages/'%(sys.version_info[:2]); mods=['newsbot','newsbot.cli','newsbot.collectors.telethon','newsbot.sheets.google','telethon','googleapiclient.discovery','google.auth','google_auth_httplib2']
assert all(str(importlib.import_module(x).__file__).startswith(root) for x in mods)
actual=sorted((d.metadata['Name'].lower().replace('_','-'),d.version) for d in importlib.metadata.distributions())
expected=sorted((d['name'],d['version']) for d in m['distributions']); assert actual==expected
for d in m['distributions']:
 p=site+d['location']+'/RECORD'; assert hashlib.sha256(open(p,'rb').read()).hexdigest()==d['record_sha256']
for r in m['resources']:
 p=site+r['path']; assert hashlib.sha256(open(p,'rb').read()).hexdigest()==r['sha256']
entry=m['entrypoint']; assert entry['realpath'].startswith(root) and hashlib.sha256(open(entry['realpath'],'rb').read()).hexdigest()==entry['sha256']
assert not any('editable' in p.read_text(errors='ignore').lower() or '/src/' in p.read_text(errors='ignore') for p in pathlib.Path(site).rglob('*.pth'))
assert not any(json.loads(p.read_text()).get('dir_info',{}).get('editable') for p in pathlib.Path(site).glob('telegram_news_bot-*.dist-info/direct_url.json'))"""
    try:
        runner(
            [str(paths.venv / "bin/python"), "-I", "-c", script, str(manifest_path)],
            check=True,
            env={"PATH": "/usr/bin:/bin"},
            capture_output=True,
        )
        runner(
            [str(paths.venv / "bin/python"), "-I", "-m", "newsbot", "--help"],
            check=True,
            env={"PATH": "/usr/bin:/bin"},
            capture_output=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReleaseError("candidate canary failed") from exc


def _database_quiescent(database: Path = Path("/var/lib/newsbot/newsbot.db")) -> bool:
    if not database.is_file():
        return False
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
        tables = {str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        def empty(table: str, where: str = "1") -> bool:
            return (
                table not in tables
                or int(connection.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]) == 0
            )

        return all(
            (
                empty("collection_intervals"),
                empty("automation_stream_leases"),
                empty("automation_stream_runs", "finished_at IS NULL"),
                empty("telegram_chunk_attempts", "state IN ('prepared','possibly_sent')"),
                empty("sheet_remote_operations", "status IN ('acquired','possibly_sent')"),
                empty("sheet_operation_leases", "status = 'active'"),
            )
        )
    except (OSError, sqlite3.Error):
        return False
    finally:
        if connection is not None:
            connection.close()


def _lock_owner() -> tuple[int, int]:
    try:
        account = pwd.getpwnam("newsbot")
    except KeyError as exc:
        raise ReleaseError("lock identity failed") from exc
    return account.pw_uid, account.pw_gid


def _attest_lock(path: Path, expected_owner: tuple[int, int]) -> os.stat_result:
    try:
        value = path.lstat()
    except OSError as exc:
        raise ReleaseError("lock identity failed") from exc
    if (
        not stat.S_ISREG(value.st_mode)
        or stat.S_ISLNK(value.st_mode)
        or value.st_uid != expected_owner[0]
        or value.st_gid != expected_owner[1]
        or value.st_nlink != 1
        or stat.S_IMODE(value.st_mode) != 0o600
    ):
        raise ReleaseError("lock identity failed")
    return value


@contextmanager
def held_quiescence_locks() -> Iterator[None]:
    """Hold canonical worker locks, refusing missing or substituted lock files."""
    expected_owner = _lock_owner()
    handles: list[tuple[Path, object, os.stat_result]] = []
    try:
        for path in LOCK_PATHS:
            expected = _attest_lock(path, expected_owner)
            handle = path.open("rb")
            opened = os.fstat(handle.fileno())
            if (opened.st_dev, opened.st_ino) != (expected.st_dev, expected.st_ino):
                handle.close()
                raise ReleaseError("lock identity failed")
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            handles.append((path, handle, expected))
        yield
        for path, _handle, expected in handles:
            current = _attest_lock(path, expected_owner)
            if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
                raise ReleaseError("lock identity failed")
    except (OSError, BlockingIOError) as exc:
        raise ReleaseError("lock quiescence failed") from exc
    finally:
        for _path, handle, _expected in reversed(handles):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


def _locks_quiescent() -> bool:
    try:
        with held_quiescence_locks():
            return True
    except ReleaseError:
        return False


def live_quiescence(
    paths: ReleasePaths,
    runner: Callable[..., object] = subprocess.run,
    *,
    locks_held: bool = False,
) -> bool:
    try:
        for timer in TIMER_UNITS:
            result = runner(
                ["systemctl", "is-enabled", timer],
                check=False,
                capture_output=True,
                text=True,
            )
            if getattr(result, "stdout", "").strip() != "disabled":
                return False
        for service in SERVICE_UNITS:
            result = runner(
                ["systemctl", "is-active", service],
                check=False,
                capture_output=True,
                text=True,
            )
            if getattr(result, "stdout", "").strip() != "inactive":
                return False
        application_quiescent = _database_quiescent() and (locks_held or _locks_quiescent())
        containment = runner(
            ["/usr/local/sbin/newsbot-codex-containment-v1", "inspect"],
            check=True,
            capture_output=True,
            text=True,
        )
        containment_status = json.loads(getattr(containment, "stdout", ""))
    except (OSError, ValueError, subprocess.SubprocessError):
        return False
    return bool(
        application_quiescent
        and containment_status.get("blocked") is False
        and containment_status.get("latch_attested") is True
        and containment_status.get("proof_complete") is True
    )


def _executable(path: Path) -> bool:
    try:
        return path.is_file() and bool(stat.S_IMODE(path.stat().st_mode) & 0o111)
    except OSError:
        return False


def _verified_stable_target(stable: Path) -> Path | None:
    if not os.path.lexists(stable):
        return None
    try:
        if not stable.is_symlink():
            raise ReleaseError("stable entrypoint identity failed")
        target = stable.resolve(strict=True)
    except OSError as exc:
        raise ReleaseError("stable entrypoint identity failed") from exc
    if not _executable(target):
        raise ReleaseError("stable entrypoint identity failed")
    return target


def _attest_stable_target(stable: Path, target: Path) -> None:
    try:
        if stable.resolve(strict=True) != target.resolve(strict=True) or digest(stable.resolve()) != digest(target):
            raise ReleaseError("stable entrypoint attestation failed")
    except OSError as exc:
        raise ReleaseError("stable entrypoint attestation failed") from exc


def atomic_switch(
    paths: ReleasePaths,
    manifest_path: Path,
    quiescent: Callable[[], bool] | None,
    stable: Path = STABLE_ENTRYPOINT,
    runner: Callable[..., object] = subprocess.run,
) -> None:
    if quiescent is None:
        raise ReleaseError("quiescence failed")
    with held_quiescence_locks():
        if not quiescent():
            raise ReleaseError("quiescence failed")
        candidate_canary(paths, manifest_path, runner)
        missing_extra_refusals(missing_extra_candidates(paths), runner)
        if not quiescent():
            raise ReleaseError("quiescence failed")
        target = paths.venv / "bin/newsbot"
        if not _executable(target):
            raise ReleaseError("candidate entrypoint failed")
        previous = _verified_stable_target(stable)
        temporary = stable.with_name(stable.name + ".new")
        swapped = False
        try:
            temporary.symlink_to(target)
            os.replace(temporary, stable)
            swapped = True
            _attest_stable_target(stable, target)
            candidate_canary(paths, manifest_path, runner)
            if not quiescent():
                raise ReleaseError("quiescence failed")
        except Exception:
            if swapped:
                if previous is None:
                    stable.unlink(missing_ok=True)
                else:
                    temporary.symlink_to(previous)
                    os.replace(temporary, stable)
                    _attest_stable_target(stable, previous)
            raise
        finally:
            if temporary.is_symlink():
                temporary.unlink()


def _proof(path: Path) -> bool:
    try:
        return _regular_root_owned(path, 0o600) is not None and path.read_bytes() == b"quiescent\n"
    except (OSError, ReleaseError):
        return False


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--uv-sha256", required=True)
    parser.add_argument("command", choices=("build", "switch"))
    parser.add_argument("sha")
    parser.add_argument("--quiescence-proof")
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        return 1
    try:
        builder = ReleaseBuilder(uv_sha256=args.uv_sha256)
        paths = ReleasePaths.for_sha(args.sha)
        if args.command == "build":
            if not args.quiescence_proof or not (_proof(Path(args.quiescence_proof)) and live_quiescence(paths)):
                raise ReleaseError("quiescence failed")
            paths = builder.build(args.sha)
            value = manifest(paths, attest_uv(builder.uv, builder.uv_sha256), builder.runner)
            output = write_manifest(paths, value)
            candidate_canary(paths, output)
            missing_extra_refusals(builder.disposable_missing_extra_candidates(paths), builder.runner)
            if not (_proof(Path(args.quiescence_proof)) and live_quiescence(paths)):
                raise ReleaseError("quiescence failed")
        else:
            if not args.quiescence_proof:
                raise ReleaseError("quiescence failed")
            atomic_switch(
                paths,
                paths.root / "runtime-manifest.json",
                lambda: _proof(Path(args.quiescence_proof)) and live_quiescence(paths, locks_held=True),
            )
    except (OSError, ValueError, ReleaseError):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

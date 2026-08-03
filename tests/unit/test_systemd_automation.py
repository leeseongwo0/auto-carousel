from __future__ import annotations

import hashlib
import shlex
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SYSTEMD = ROOT / "deploy" / "systemd"
CODEX_UNIT_HASHES = {
    "newsbot-generate-codex.service": "7deea7ee96fe97b8b5ce85e09f36a5003743622e15e780b6d4d58c59c0d8b231",
    "newsbot-generate-codex-canary.service": "64e00828d97f72c2bdbf9efe23c7631005ae0af853fe97162c4c7aa10c363f61",
    "newsbot-generate-codex.timer": "612070bf7ea70047c67424e554a5a90997cd7e091f8e3f587ed549835a5a74f3",
}

SERVICES = {
    "newsbot-sheets.service": {
        "exec_start": "/usr/local/bin/newsbot sheets-deliver-pending-once --config /etc/newsbot/config.toml --db /var/lib/newsbot/newsbot.db --max-handoffs 1 --deadline-seconds 90 --lease-seconds 135 --sheet-lease-seconds 300",
        "deadline": 90,
        "timeout": 120,
        "lease": 135,
        "sheet_lease": 300,
        "memory": "768M",
    },
    "newsbot-telegram.service": {
        "exec_start": "/usr/local/bin/newsbot telegram-tick --config /etc/newsbot/config.toml --db /var/lib/newsbot/newsbot.db --poll-timeout 10 --max-updates 50 --max-notifications 1 --deadline-seconds 60 --lease-seconds 90",
        "deadline": 60,
        "timeout": 75,
        "lease": 90,
        "memory": "512M",
    },
    "newsbot-collect.service": {
        "exec_start": "/usr/local/bin/newsbot automation-collect-once --config /etc/newsbot/config.toml --db /var/lib/newsbot/newsbot.db --lookback-hours 24 --page-size 50 --max-pages 2 --deadline-seconds 180 --lease-seconds 225",
        "deadline": 180,
        "timeout": 210,
        "lease": 225,
        "memory": "512M",
    },
}

TIMERS = {
    "newsbot-sheets.timer": {
        "Description": "Schedule Newsbot Sheets delivery",
        "OnBootSec": "90s",
        "OnUnitInactiveSec": "1min",
        "RandomizedDelaySec": "10s",
        "AccuracySec": "5s",
        "Unit": "newsbot-sheets.service",
    },
    "newsbot-telegram.timer": {
        "Description": "Schedule Newsbot Telegram processing",
        "OnBootSec": "30s",
        "OnUnitInactiveSec": "20s",
        "RandomizedDelaySec": "0",
        "AccuracySec": "1s",
        "Unit": "newsbot-telegram.service",
    },
    "newsbot-collect.timer": {
        "Description": "Schedule Newsbot collection",
        "OnBootSec": "2min",
        "OnUnitInactiveSec": "1h",
        "RandomizedDelaySec": "30s",
        "AccuracySec": "5s",
        "Unit": "newsbot-collect.service",
    },
}


def _parse_unit(path: Path) -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    section: dict[str, str] | None = None
    for number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = sections.setdefault(line[1:-1], {})
            continue
        assert section is not None, f"{path}:{number}: directive outside a section"
        assert "=" in line, f"{path}:{number}: malformed directive"
        key, value = line.split("=", maxsplit=1)
        assert key not in section, f"{path}:{number}: duplicate {key} directive"
        section[key] = value
    return sections


@pytest.fixture(autouse=True)
def codex_units_remain_byte_for_byte_unchanged() -> None:
    assert {
        name: hashlib.sha256((SYSTEMD / name).read_bytes()).hexdigest() for name in CODEX_UNIT_HASHES
    } == CODEX_UNIT_HASHES


@pytest.mark.parametrize(("name", "contract"), SERVICES.items())
def test_automation_services_have_exact_isolated_oneshot_contract(name: str, contract: dict[str, object]) -> None:
    path = SYSTEMD / name
    unit = _parse_unit(path)
    assert set(unit) == {"Unit", "Service"}
    assert unit["Unit"] == {"After": "network-online.target", "Wants": "network-online.target"}
    assert unit["Service"] == {
        "Type": "oneshot",
        "User": "newsbot",
        "Group": "newsbot",
        "WorkingDirectory": "/var/lib/newsbot",
        "EnvironmentFile": "/etc/newsbot/newsbot.env",
        "UMask": "0077",
        "NoNewPrivileges": "true",
        "CapabilityBoundingSet": "",
        "ExecStart": contract["exec_start"],
        "TimeoutStartSec": str(contract["timeout"]),
        "TimeoutStopSec": "10",
        "KillMode": "control-group",
        "SendSIGKILL": "yes",
        "TasksMax": "64",
        "MemoryMax": contract["memory"],
        "MemorySwapMax": "0",
        "CPUQuota": "100%",
    }
    assert int(contract["deadline"]) < int(contract["timeout"]) < int(contract["lease"])
    if "sheet_lease" in contract:
        assert int(contract["lease"]) < int(contract["sheet_lease"])

    contents = path.read_text(encoding="utf-8")
    from newsbot.cli import build_parser

    argv = shlex.split(str(contract["exec_start"]))[1:]
    parsed = build_parser().parse_args(argv)
    assert callable(parsed.handler)
    forbidden = (
        "PrivateTmp",
        "InaccessiblePaths",
        "BindPaths",
        "BindReadOnlyPaths",
        "TemporaryFileSystem",
        "MountFlags",
        "MountImage",
        "RootImage",
        "Session",
        "session-copy",
        "copy_draft",
        "newsbot-codex-runner",
        "ExecStartPre",
        "ExecStartPost",
        "ExecStop",
        "ExecStopPost",
        "ExecReload",
        "codex",
        "provider",
        "containment",
        "systemctl",
        "/bin/sh",
        "/bin/bash",
        "sudo",
        "SupplementaryGroups",
        "DynamicUser",
    )
    assert not [term for term in forbidden if term.casefold() in contents.casefold()]


@pytest.mark.parametrize(("name", "contract"), TIMERS.items())
def test_automation_timers_have_exact_cadence_and_enablement_contract(name: str, contract: dict[str, str]) -> None:
    unit = _parse_unit(SYSTEMD / name)
    assert unit == {
        "Unit": {"Description": contract["Description"]},
        "Timer": {
            "OnBootSec": contract["OnBootSec"],
            "OnUnitInactiveSec": contract["OnUnitInactiveSec"],
            "RandomizedDelaySec": contract["RandomizedDelaySec"],
            "AccuracySec": contract["AccuracySec"],
            "Unit": contract["Unit"],
            "Persistent": "false",
        },
        "Install": {"WantedBy": "timers.target"},
    }

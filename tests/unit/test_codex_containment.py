from __future__ import annotations

import hashlib
import importlib.util
import tomllib
from pathlib import Path

import pytest


@pytest.fixture
def containment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    spec = importlib.util.spec_from_file_location(
        "test_codex_containment_module", Path(__file__).parents[2] / "deploy/newsbot_codex_containment.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ROOT", str(tmp_path / "containment"))
    monkeypatch.setattr(module, "MANIFEST", str(tmp_path / "manifest.json"))
    return module


def test_manifest_attests_managed_codex_requirements(containment: object) -> None:
    spec = importlib.util.spec_from_file_location(
        "test_codex_manifest_module", Path(__file__).parents[2] / "deploy/build_codex_manifest.py"
    )
    assert spec and spec.loader
    manifest = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manifest)

    assert set(manifest.ARTIFACTS) == set(containment.MANIFEST_ARTIFACTS)
    assert manifest.ARTIFACTS["/etc/codex/requirements.toml"] == 0o644


def test_bundled_codex_policy_is_minimal_read_only_and_denies_device_auth() -> None:
    policy = tomllib.loads((Path(__file__).parents[2] / "deploy/codex-requirements.toml").read_text(encoding="utf-8"))

    assert policy["default_permissions"] == "newsbot_generate"
    assert policy["allowed_approval_policies"] == ["never"]
    assert policy["allowed_web_search_modes"] == []
    assert policy["allow_remote_control"] is False
    assert policy["allowed_permission_profiles"] == {"newsbot_generate": True}
    assert policy["permissions"]["newsbot_generate"]["extends"] == ":read-only"
    assert policy["permissions"]["newsbot_generate"]["filesystem"] == {"~/.codex": "deny"}


def _dirty(module: object, *, unit: str | None = None, manifest: str = "m" * 64) -> dict[str, object]:
    return {
        "activation": "a" * 32,
        "manifest_sha256": manifest,
        "previous_activation": "b" * 32,
        "previous_state_sha256": "c" * 64,
        "state": "dirty",
        "unit": unit or module.UNITS[0],
        "version": 1,
    }


def _clean(module: object, dirty: dict[str, object], proof: dict[str, object]) -> dict[str, object]:
    return {
        "activation": dirty["activation"],
        "manifest_sha256": dirty["manifest_sha256"],
        "previous_state_sha256": module.digest(dirty),
        "receipt": "clean-proof.json",
        "receipt_sha256": hashlib.sha256(module.canonical(proof)).hexdigest(),
        "state": "clean",
        "unit": dirty["unit"],
        "version": 1,
    }


def _proof(module: object, dirty: dict[str, object]) -> dict[str, object]:
    return {
        "activation": dirty["activation"],
        "kind": "clean",
        "manifest_sha256": dirty["manifest_sha256"],
        "state_sha256": module.digest(dirty),
        "time_ns": 1,
        "unit": dirty["unit"],
        "version": 1,
    }


def test_absent_or_malformed_state_never_counts_as_clean(containment: object, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(containment, "receipt", lambda rootfd, name: {})
    with pytest.raises(containment.Blocked):
        containment.valid_clean(1, {}, "m" * 64)
    monkeypatch.setattr(containment, "load_state", lambda rootfd: (_ for _ in ()).throw(containment.Blocked()))
    monkeypatch.setattr(containment, "root_dir", lambda path, mode: 1)
    monkeypatch.setattr(containment, "recovery_lock", lambda rootfd: 2)
    monkeypatch.setattr(containment, "manifest_digest", lambda: "m" * 64)
    monkeypatch.setattr(containment, "inactive_and_empty", lambda *args: None)
    monkeypatch.setattr(containment.os, "close", lambda fd: None)
    with pytest.raises(containment.Blocked):
        containment.start()


@pytest.mark.parametrize("corruption", ["receipt_sha256", "previous_state_sha256", "manifest_sha256", "unit"])
def test_clean_state_requires_matching_attested_receipt(
    containment: object, monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    dirty = _dirty(containment)
    proof = _proof(containment, dirty)
    state = _clean(containment, dirty, proof)
    if corruption == "receipt_sha256" or corruption == "previous_state_sha256":
        state[corruption] = "0" * 64
    elif corruption == "manifest_sha256":
        state[corruption] = "x" * 64
    else:
        state[corruption] = containment.UNITS[1]
    monkeypatch.setattr(containment, "receipt", lambda rootfd, name: proof)
    with pytest.raises(containment.Blocked):
        containment.valid_clean(1, state, "m" * 64)


def test_start_writes_dirty_before_publishing_credential(containment: object, monkeypatch: pytest.MonkeyPatch) -> None:
    dirty = _dirty(containment)
    proof = _proof(containment, dirty)
    state = _clean(containment, dirty, proof)
    written: list[dict[str, object]] = []
    monkeypatch.setattr(containment, "root_dir", lambda path, mode: 10)
    monkeypatch.setattr(containment, "recovery_lock", lambda rootfd: 11)
    monkeypatch.setattr(containment, "current_unit", lambda: containment.UNITS[0])
    monkeypatch.setattr(containment, "manifest_digest", lambda: "m" * 64)
    monkeypatch.setattr(containment, "load_state", lambda rootfd: (state, object()))
    monkeypatch.setattr(containment, "receipt", lambda rootfd, name: proof)
    monkeypatch.setattr(containment, "inactive_and_empty", lambda active=None: None)
    monkeypatch.setattr(containment, "atomic_state", lambda rootfd, value: written.append(value))
    monkeypatch.setattr(
        containment, "publish_credential", lambda value: (_ for _ in ()).throw(RuntimeError("credential failed"))
    )
    monkeypatch.setattr(containment.os, "close", lambda fd: None)
    with pytest.raises(RuntimeError, match="credential failed"):
        containment.start()
    assert written and written[0]["state"] == "dirty"


def test_stop_and_reset_clean_only_after_proof_and_credential_clear(
    containment: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    dirty = _dirty(containment)
    writes: list[dict[str, object]] = []
    events: list[str] = []
    monkeypatch.setattr(containment, "root_dir", lambda path, mode: 10)
    monkeypatch.setattr(containment, "recovery_lock", lambda rootfd: 11)
    monkeypatch.setattr(containment, "manifest_digest", lambda: "m" * 64)
    monkeypatch.setattr(containment, "current_unit", lambda: containment.UNITS[0])
    monkeypatch.setattr(containment, "load_state", lambda rootfd: (dirty, object()))
    monkeypatch.setattr(containment, "inactive_and_empty", lambda active=None: events.append("proof"))
    monkeypatch.setattr(
        containment,
        "write_receipt",
        lambda rootfd, value, kind, manifest: ("clean-proof.json", _proof(containment, value)),
    )
    monkeypatch.setattr(containment, "clear_credential", lambda value, required: events.append("credential"))
    monkeypatch.setattr(containment, "atomic_state", lambda rootfd, value: writes.append(value))
    monkeypatch.setattr(containment.os, "close", lambda fd: None)
    containment.clean("clean")
    assert events == ["proof", "credential"] and writes[-1]["state"] == "clean"
    monkeypatch.setattr(containment, "current_unit", lambda: None)
    containment.clean("reset")
    assert writes[-1]["state"] == "clean" and writes[-1]["unit"] == "reset"


def test_proof_or_receipt_failure_leaves_last_durable_state_dirty(
    containment: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    dirty = _dirty(containment)
    writes: list[dict[str, object]] = []
    monkeypatch.setattr(containment, "root_dir", lambda path, mode: 10)
    monkeypatch.setattr(containment, "recovery_lock", lambda rootfd: 11)
    monkeypatch.setattr(containment, "manifest_digest", lambda: "m" * 64)
    monkeypatch.setattr(containment, "current_unit", lambda: containment.UNITS[0])
    monkeypatch.setattr(containment, "load_state", lambda rootfd: (dirty, object()))
    monkeypatch.setattr(containment, "inactive_and_empty", lambda active=None: None)
    monkeypatch.setattr(containment, "atomic_state", lambda rootfd, value: writes.append(value))
    monkeypatch.setattr(
        containment, "write_receipt", lambda *args: (_ for _ in ()).throw(RuntimeError("receipt failed"))
    )
    monkeypatch.setattr(containment.os, "close", lambda fd: None)
    with pytest.raises(RuntimeError, match="receipt failed"):
        containment.clean("clean")
    assert writes == []
    monkeypatch.setattr(
        containment,
        "write_receipt",
        lambda rootfd, value, kind, manifest: ("clean-proof.json", _proof(containment, value)),
    )
    monkeypatch.setattr(containment, "clear_credential", lambda value, required: None)
    monkeypatch.setattr(
        containment,
        "atomic_state",
        lambda rootfd, value: (
            (_ for _ in ()).throw(RuntimeError("storage failed")) if value["state"] == "clean" else writes.append(value)
        ),
    )
    with pytest.raises(RuntimeError, match="storage failed"):
        containment.clean("clean")
    assert writes == []


def test_recovery_lock_takes_exclusive_flock_before_state_transition(
    containment: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(containment, "parents_safe", lambda path: None)
    monkeypatch.setattr(containment.os, "open", lambda *args, **kwargs: 3)
    monkeypatch.setattr(
        containment.os,
        "fstat",
        lambda fd: type("S", (), {"st_mode": 0o100600, "st_uid": 0, "st_gid": 0, "st_nlink": 1})(),
    )
    monkeypatch.setattr(containment.fcntl, "flock", lambda fd, flags: calls.append((fd, flags)))
    monkeypatch.setattr(containment.os, "fsync", lambda fd: None)
    assert containment.recovery_lock(1) == 3
    assert calls == [(3, containment.fcntl.LOCK_EX)]

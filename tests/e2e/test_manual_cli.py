"""Installed-style CLI-only manual workflow outside the checkout."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

_POLICY = """
[policy]
version = "candidate_policy_v1"
locale = "ko-KR"
initial_lookback_hours = 24
max_candidate_age_hours = 72
future_tolerance_hours = 2
min_semantic_chars = 80
min_material_sentence_chars = 40
freshness_horizon_hours = 48
novelty_window_days = 7
min_topic_relevance = 0.20
min_total_score = 0.55
disclosure_markers = ["[광고]", "sponsored"]
referral_markers = ["추천인", "referral"]

[policy.weights]
source_quality = 0.15
freshness = 0.15
engagement = 0.10
topic_relevance = 0.25
novelty = 0.15
official_evidence = 0.15
certainty = 0.05

[policy.topic_positive_phrases]
ai = 0.60
"인공지능" = 0.60

[policy.topic_exclusion_phrases]

[policy.engagement_weights]
views = 0.60
reactions = 0.25
forwards = 0.15

[policy.engagement_saturation]
views = 100000
reactions = 5000
forwards = 1000

[policy.certainty_markers]
rumor = 0.30
alleged = 0.30
"루머" = 0.30
"설" = 0.30
anonymous = 0.20
unattributed = 0.20
"익명" = 0.20

[policy.certainty_penalties]
conflicts = 0.50
missing_url = 0.20

[news_policy]
version = "news_policy_v1"
timezone = "Asia/Seoul"
noon_hour = 12
noon_minute = 0
activation_minutes = 60
material_semantic_chars = 80
material_sentence_chars = 40
analysis_semantic_chars = 160
analysis_sentence_chars = 40
analysis_min_sentences = 2
event_markers_ko = ["출시", "공개", "발표"]
event_markers_en = ["launched", "released", "announced"]
analysis_markers_ko = ["분석", "리뷰"]
analysis_markers_en = ["analysis", "review"]
evidence_markers_ko = ["에 따르면", "공식 문서"]
evidence_markers_en = ["according to", "official documentation"]
promotion_markers_ko = ["광고", "협찬"]
promotion_markers_en = ["ad", "sponsored"]
tutorial_markers_ko = ["튜토리얼", "가이드"]
tutorial_markers_en = ["tutorial", "guide"]
reaction_markers_ko = ["내 생각", "반응"]
reaction_markers_en = ["my take", "reaction"]
"""


def _run(
    executable: Path,
    root: Path,
    *arguments: str,
    extra_env: dict[str, str] | None = None,
) -> dict[str, object]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    if extra_env is not None:
        environment.update(extra_env)
    completed = subprocess.run(
        [str(executable), *arguments],
        cwd=root,
        check=False,
        capture_output=True,
        env=environment,
        preexec_fn=lambda: os.umask(0),
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""
    return json.loads(completed.stdout)


def _candidate_artifact_from_command(output_dir: Path, command: dict[str, object], run_id: int) -> tuple[Path, str]:
    receipt = str(command["receipt"])
    filename = str(command["artifact_filename"])
    assert filename == f"candidates-{run_id}-{receipt}.json"
    assert len(receipt) == 64
    assert all(character in "0123456789abcdef" for character in receipt)
    artifact_path = output_dir / filename
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert artifact["receipt"] == receipt
    return artifact_path, receipt


@pytest.mark.skipif("NEWSBOT_WHEEL" not in os.environ, reason="requires an exact built wheel")
def test_manual_cli_installed_wheel_two_source_private_workflow() -> None:
    wheel = Path(os.environ["NEWSBOT_WHEEL"]).resolve()
    names = zipfile.ZipFile(wheel).namelist()
    assert all(name.startswith(("newsbot/", "telegram_news_bot-0.1.0.dist-info/")) for name in names)
    assert not any(part in name for name in names for part in (".github/", "tests/", "config/", ".pyc"))
    assert b"private-source-content-must-not-be-disclosed" not in wheel.read_bytes()

    root = Path(tempfile.mkdtemp(prefix=".newsbot-manual-cli-", dir=Path.home()))
    root.chmod(0o700)
    try:
        environment = os.environ.copy()
        environment.pop("PYTHONPATH", None)
        venv = root / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, cwd=root, env=environment)
        subprocess.run(
            [str(venv / "bin" / "pip"), "install", "--no-deps", "--no-index", str(wheel)],
            check=True,
            cwd=root,
            env=environment,
        )
        executable = venv / "bin" / "newsbot"
        profile = root / "profile.toml"
        profile.write_text(
            """schema = "newsbot.behavior.v1"
operation = "manual_local"

[[sources]]
id = "synthetic-source-a"
name = "Synthetic Source A"
enabled = true
priority = 100
source_quality = 1.0
classification = "official"
official_domains = ["example.com"]
original_domains = []

[[sources]]
id = "synthetic-source-b"
name = "Synthetic Source B"
enabled = true
priority = 90
source_quality = 1.0
classification = "official"
official_domains = ["example.com"]
original_domains = []

"""
            + _POLICY
        )
        profile.chmod(0o600)
        default_xdg = root / "xdg-state"
        default_xdg.mkdir(mode=0o700)
        assert _run(
            executable,
            root,
            "manual-init",
            "--profile",
            str(profile),
            extra_env={"XDG_STATE_HOME": str(default_xdg)},
        ) == {"sources": 2, "status": "initialized"}
        default_state = default_xdg / "newsbot" / "manual"
        assert stat.S_IMODE(default_state.stat().st_mode) == 0o700
        assert stat.S_IMODE((default_state / "newsbot.sqlite3").stat().st_mode) == 0o600
        state = root / "state"
        base = ("--profile", str(profile), "--state", str(state))
        assert _run(executable, root, "manual-init", *base) == {"sources": 2, "status": "initialized"}

        document = root / "observations.json"
        document.write_text(
            json.dumps(
                {
                    "schema": "newsbot.manual.import.v1",
                    "records": [
                        {
                            "source_id": source_id,
                            "post_id": f"synthetic-{index}",
                            "published_at": datetime.now(UTC).isoformat(),
                            "text": (
                                f"합성 사례 {index}: 인공지능 연구팀이 공개 평가 도구를 발표했습니다. "
                                "이 도구는 재현 가능한 로컬 실행과 검증 가능한 결과 내보내기를 지원하며, "
                                "개인정보나 외부 서비스 권한 없이 개발자가 안전하게 시험할 수 있도록 설계됐습니다."
                            ),
                            "urls": [f"https://example.com/synthetic-{index}"],
                            "views": 1000,
                            "reactions": 10,
                            "forwards": 1,
                        }
                        for index, source_id in enumerate(("synthetic-source-a", "synthetic-source-b"), start=1)
                    ],
                }
            )
        )
        document.chmod(0o600)
        assert _run(executable, root, "manual-import", *base, "--input", str(document))["status"] == "imported"
        with sqlite3.connect(state / "newsbot.sqlite3") as connection:
            for table in ("candidate_evaluations", "generations", "manual_local_export_outbox"):
                assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (0,)

        ranked = _run(executable, root, "manual-rank", *base)
        run_id = int(ranked["run_id"])
        explicit_previews = root / "explicit-previews"
        explicit_previews.mkdir(mode=0o700)
        stale = explicit_previews / f"candidates-{run_id}-{'0' * 64}.json"
        stale.write_text('{"receipt":"stale"}', encoding="utf-8")
        os.utime(stale, (2_000_000_000, 2_000_000_000))
        listed = _run(
            executable,
            root,
            "manual-candidates",
            *base,
            "--run-id",
            str(run_id),
            "--output-dir",
            str(explicit_previews),
        )
        assert listed["status"] == "listed"
        assert str(state) not in json.dumps(listed)
        assert str(explicit_previews) not in json.dumps(listed)
        preview, receipt = _candidate_artifact_from_command(explicit_previews, listed, run_id)
        artifact = json.loads(preview.read_text())
        assert len(artifact["candidates"]) == 2
        rejected_id = int(artifact["candidates"][0]["id"])
        assert artifact["candidates"][0]["sources"]
        assert stale.stat().st_mtime_ns > preview.stat().st_mtime_ns
        rejected = _run(
            executable,
            root,
            "manual-candidate-decision",
            *base,
            "--run-id",
            str(run_id),
            "--candidate-id",
            str(rejected_id),
            "--decision",
            "reject",
            "--expected-receipt",
            receipt,
        )
        assert rejected["status"] == "rejected"

        refreshed_command = _run(
            executable,
            root,
            "manual-candidates",
            *base,
            "--run-id",
            str(run_id),
            "--output-dir",
            str(explicit_previews),
        )
        assert refreshed_command["status"] == "listed"
        refreshed_path, refreshed_receipt = _candidate_artifact_from_command(
            explicit_previews, refreshed_command, run_id
        )
        assert refreshed_path != preview
        assert stale.stat().st_mtime_ns > refreshed_path.stat().st_mtime_ns
        refreshed = json.loads(refreshed_path.read_text())
        assert len(refreshed["candidates"]) == 1
        candidate_id = int(refreshed["candidates"][0]["id"])
        selected = _run(
            executable,
            root,
            "manual-candidate-decision",
            *base,
            "--run-id",
            str(run_id),
            "--candidate-id",
            str(candidate_id),
            "--decision",
            "select",
            "--expected-receipt",
            refreshed_receipt,
        )
        assert selected["status"] == "selected"
        generated = _run(
            executable, root, "manual-generate", *base, "--candidate-id", str(candidate_id), "--provider", "fake"
        )
        generation_id = int(generated["generation_id"])
        draft = state / "drafts" / f"draft-{generation_id}.json"
        explicit_drafts = root / "explicit-drafts"
        _run(
            executable,
            root,
            "manual-draft",
            *base,
            "--generation-id",
            str(generation_id),
            "--output-dir",
            str(explicit_drafts),
        )
        digest = hashlib.sha256(draft.read_bytes()).hexdigest()
        reviewed = _run(
            executable,
            root,
            "manual-review",
            *base,
            "--candidate-id",
            str(candidate_id),
            "--generation-id",
            str(generation_id),
            "--decision",
            "approve-local",
            "--expected-draft-digest",
            digest,
        )
        assert reviewed["status"] == "approved"
        assert _run(executable, root, "manual-export", *base) == {"exports": 2, "status": "exported"}
        default_exports = sorted((state / "exports").glob("export-*"))
        default_digests = [hashlib.sha256(path.read_bytes()).hexdigest() for path in default_exports]
        explicit_exports = root / "explicit-exports"
        assert _run(executable, root, "manual-export", *base, "--output-dir", str(explicit_exports)) == {
            "exports": 2,
            "status": "exported",
        }
        assert _run(executable, root, "manual-export", *base) == {"exports": 2, "status": "exported"}
        assert default_digests == [hashlib.sha256(path.read_bytes()).hexdigest() for path in default_exports]
        assert _run(executable, root, "manual-status", *base)["status"] == "ready"
        assert sorted(path.suffix for path in default_exports) == [".json", ".md"]
        for path in (
            state,
            state / "newsbot.sqlite3",
            preview,
            draft,
            explicit_previews,
            explicit_drafts,
            explicit_exports,
        ):
            assert stat.S_IMODE(path.stat().st_mode) in {0o700, 0o600}
    finally:
        shutil.rmtree(root)

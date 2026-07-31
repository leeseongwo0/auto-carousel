# 로컬 운영 가이드

## 기본 원칙

SQLite가 원고, 승인, Sheets handoff와 원격 작업 이력의 권위다. fixture 명령은 Google 패키지·자격증명·네트워크 없이 동작하며 일반 결과 파일을 만들지 않는다. 비밀값, 스프레드시트 ID, 원고 본문, Google 오류 본문은 로그·SQLite safe code·영수증에 기록하지 않는다.

## 환경과 설치

```bash
uv sync --group dev
uv sync --group dev --extra sheets  # 실계정 Sheets 명령 전용
```

| 명령 | 필요한 환경 변수 |
|---|---|
| `init-db`, `run-fixture`, `reconcile`, `rank`, `status`, `inspect` | 없음 |
| `auth-telethon`, `collect-live`, `reconcile-live` | `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_PATH` |
| `notify-candidates`, `notify-review`, `poll-approvals` | `TELEGRAM_BOT_TOKEN`, `NEWSBOT_APPROVER_CHAT_ID`, `NEWSBOT_APPROVER_USER_IDS` |
| `generate-pending --provider openai_compatible` | `OPENAI_BASE_URL`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_TIMEOUT_SECONDS` |
| `sheets-validate`, `sheets-bootstrap`, `sheets-deliver`, `sheets-reconcile` | `GOOGLE_SHEETS_SPREADSHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_FILE` |

`GOOGLE_SERVICE_ACCOUNT_FILE`은 소유자 전용 `0700` 디렉터리 안의 소유자 전용 `0600` 일반 JSON 파일이어야 한다. 심볼릭 링크, 그룹/기타 권한, 잘못된 service-account JSON은 Google import나 네트워크 전에 거부된다. 권한 범위는 Sheets 전용이다.
## Codex provider 제어와 안전한 출력

`codex_cli`는 credential-free application provider다. ChatGPT device login은 `newsbot-codex` 계정에서만 수행하며 Newsbot 환경 파일에는 Codex token/API key를 넣지 않는다. production 호출은 `sudo systemctl start newsbot-generate-codex.service` 하나뿐이다. `generate-pending`, Codex binary, runner를 직접 실행하거나 multi-job 옵션을 주지 않는다.

```bash
newsbot codex-provider-pause --db /var/lib/newsbot/newsbot.db \
  --actor-id 42 --expected-control-version 1 --reason-code operator_security_hold
newsbot codex-provider-resume --db /var/lib/newsbot/newsbot.db \
  --actor-id 42 --expected-control-version 2 --reason-code security_reviewed
newsbot codex-job-hold --db /var/lib/newsbot/newsbot.db \
  --generation-job-id 17 --actor-id 42 --reason-code operator_review
newsbot codex-job-release --db /var/lib/newsbot/newsbot.db \
  --generation-job-id 17 --actor-id 42 --reason-code operator_reviewed
```

resume reason은 pause reason과 호환되어야 한다: `codex_auth_unavailable→auth_restored`, config/supervisor/unknown-exit/outer-timeout→`config_repaired`, attestation→`attestation_passed`, `operator_security_hold→security_reviewed`, `maintenance→maintenance_complete`다. pause/resume/hold/release는 immutable audit event만 append한다. resume stdout은 job ID, safe code, path, prompt, secret 없이 `changed`, `event_id`, `operation_id`, `affected_job_count`, `provider`, `resulting_control_version`, `status`만 낸다. replay는 같은 immutable operation/count/version을 `changed:false`로 반환한다.

`affected_job_count`는 mutable/stored counter가 아니다. 그 resume `operation_id`를 FK로 참조하는 immutable `provider_resumed` release rows의 `COUNT(*)`만 operator가 확인하는 값이다. FK, unique job link, control/retry-version 또는 count 불일치는 fail closed하며 DB를 편집해 되돌리지 않는다.


## Fixture와 승인

```bash
uv run newsbot init-db --db var/e2e/newsbot.db
uv run newsbot run-fixture --config config/channels.toml --fixture tests/fixtures/channel_messages.json --db var/e2e/newsbot.db
uv run newsbot run-fixture --config config/channels.toml --fixture tests/fixtures/channel_messages.json --db var/e2e/newsbot.db --scripted-approve
uv run newsbot status --db var/e2e/newsbot.db
uv run newsbot inspect --db var/e2e/newsbot.db --run-id 1
```

`--scripted-approve`는 선택·생성·최종 승인을 수행하고 SQLite에 불변 Sheets handoff를 만든다. 파일 export나 자동 Sheets 전송은 하지 않는다. 생성 category는 원고 내용에서 `AI` 또는 `Blockchain`으로 결정되며 사람이 별도 선택하지 않는다.

## Sheets 준비와 전달

대상은 사전 공유된 고정 스프레드시트의 정확한 `workplace` 탭(sheetId 0)이다. `Demo`와 다른 탭은 읽기/쓰기 판단에 사용하지 않는다.

```bash
uv run newsbot sheets-validate --config config/channels.toml --db var/live/newsbot.db
uv run newsbot sheets-bootstrap --config config/channels.toml --db var/live/newsbot.db
uv run newsbot sheets-status --config config/channels.toml --db var/live/newsbot.db
uv run newsbot sheets-deliver --config config/channels.toml --db var/live/newsbot.db --handoff-id 1
uv run newsbot sheets-reconcile --config config/channels.toml --db var/live/newsbot.db --operation-id 1
```

1. `sheets-validate`는 A1:V3 헤더·병합·타깃을 읽기 전용으로 검증한다.
2. `sheets-bootstrap`은 동일 binding mutex 아래 schema metadata, D/E validation, A:D protection을 원자적으로 설치하거나 정확히 재사용한다. 헤더, row 4 값, 다른 탭은 수정하지 않는다.
   보호 요청의 명시적 편집자는 서비스 계정 하나다. Google 응답에는 제거할 수 없는 스프레드시트 소유자가 함께 정규화될 수 있으므로 검증은 서비스 계정 포함, 그룹 없음, 도메인 편집 비활성화를 요구하며 소유자 권한을 보안 경계로 간주하지 않는다.
3. `sheets-deliver`는 최종 승인 handoff만 읽어 A:V 22개 문자열을 새 행에 추가한다. A는 빈 문자열, E는 `X`다.
4. Instagram 업로드 후 사람은 E를 `O`로 바꾼다. 봇은 기존 행을 수정·복구하지 않는다.
5. 정확한 document metadata가 있으면 zero-write 재사용한다. 중복/충돌 metadata는 차단한다.

## 불확실 전송과 복구

Bootstrap과 delivery는 `workplace` binding 하나의 SQLite mutex/fence를 공유한다. 원격 mutation 전에 요청 SHA와 `possibly_sent` 상태를 커밋한다. 그 뒤 timeout, reset, 5xx, redirect, malformed response, 프로세스 종료, 음성 probe는 영구적으로 자동 재전송을 금지한다.

- 정확한 metadata probe: delivered/ready로 확정
- duplicate/conflict: blocked
- absent/unavailable: 여전히 ambiguous, 재전송 금지
- 완전히 수신·파싱된 원자적 4xx/허용된 rate-limit rejection만 settled-not-applied
- stale token/fence: 상태 변경·event·전송 모두 0건

`sheet_operation_leases`, events, probes와 terminal operation은 삭제하지 않는다. 복구는 probe 또는 명시적 운영자 판단만 사용한다.

## Codex VPS A/B cutover와 forward-only rollback

1. production timer를 중지하고 `newsbot-generate-codex.service`가 inactive이며 cgroup residue가 없음을 증명한 뒤 SQLite backup을 만든다. `newsbot`과 login-shell 없는 `newsbot-codex`를 분리하고 `/var/lib/newsbot`, `/var/lib/newsbot-codex/.codex`, `/var/empty/newsbot-{provider,codex}`를 owner/mode대로 만든다.
2. 새 release를 A/B slot에 pin한다. application/lockfile, Codex binary/model, runner, schema, `/etc/codex/requirements.toml`, sudoers, units/timer와 dependency version/checksum/owner/mode를 release manifest로 attest한다. root-owned regular non-symlink artifact 또는 effective permission profile 증거가 틀리거나 불명확/확장되면 중단한다.
3. `newsbot-codex`로 pinned Codex의 `login --device-auth`를 수행하고 auth를 출력·복사하지 않는다. `login status`와 owner-only `CODEX_HOME`만 확인한다. exact sudoers no-argument runner 하나 외의 sudo, wildcard, inherited environment는 허용하지 않는다.
4. 새 DB에 schema migration을 적용하고 installed schema/manifest를 대조한다. `PRAGMA foreign_key_check`와 migration version 불일치, secret/sentinel log leak은 BLOCK이다. containment genesis receipt를 durable하게 남기고 `/var/lib/newsbot-containment/codex-state-v1`을 receipt를 참조하는 `clean`으로 초기화한다. state/receipt 부재 또는 `dirty`는 activation 금지다.
5. `systemctl daemon-reload` 후 provider-free Telegram/Sheets canary(기존 `sheets-validate`, `sheets-bootstrap` 절 포함)를 실행한다. 그 다음 effective profile과 `/proc`/FD/socket/descendant/web/MCP/plugin/remote-control deny canary, runner attestation, cgroup-empty canary를 통과시킨다.
6. production timer를 멈춘 상태로 byte-identical canary unit 한 번, 이어 live `newsbot-generate-codex.service` 한 번만 실행한다. 각각 clean receipt와 empty cgroup을 확인한 뒤에만 `newsbot-generate-codex.timer`를 enable한다.
7. rollback은 timer와 runner activation을 먼저 끄고 previous verified A/B pin으로만 되돌린다. attempts, generations, pause/resume/hold/release events, Sheets audit, receipt 또는 DB rows를 delete/update/restore하지 않는다. migration은 forward-only이며 교정 release와 새 immutable audit로 진행한다.

production rollout은 unproven effective permission profile, cgroup residue, bad/missing state 또는 receipt, migration mismatch, secret/sentinel leak, unknown/widened policy 중 하나라도 있으면 BLOCK이다. 기존 Sheets 행·metadata·controls와 Telegram/Sheets 운영 절차는 rollback으로 삭제·복원하지 않는다.

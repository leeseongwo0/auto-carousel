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

## Cutover와 rollback

1. 구 materializer/poller를 중지하고 SQLite를 백업한다.
2. 새 binary로 migration 003을 적용하고 `PRAGMA foreign_key_check` 및 legacy counter를 확인한다.
3. `sheets-validate`, `sheets-bootstrap`, disposable spreadsheet append-placement canary를 통과시킨다.
4. 새 승인/전달 worker만 활성화한다.

Rollback은 원격 행·metadata·controls 또는 새 SQLite audit를 삭제하지 않는다. 효과를 중지하고 새 binary/DB를 고쳐 앞으로 진행한다. 구 binary/DB 복원이나 legacy outbox backfill은 금지한다.

Figma 편집과 Instagram 게시는 수동이다. commit, push, 배포, VPS 스케줄링은 이 도구가 자동 수행하지 않는다.

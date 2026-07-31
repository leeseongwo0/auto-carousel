# 뉴스 캐러셀 워크플로

Telegram의 여섯 소스 채널에서 후보를 수집·정렬하고, 사람이 선택한 주제로 1~8페이지 원고를 생성한 뒤 최종 승인된 원고만 Google Sheets `workplace` 탭에 전달하는 로컬 우선 도구입니다. SQLite가 원고·승인·전달 상태의 권위입니다.

## 편집 및 전달 흐름

1. 수집과 결정론적 후보 평가를 수행합니다. 이 단계에서는 생성 제공자를 호출하지 않습니다.
2. 권한 있는 사람이 정확한 후보/리비전에서 `[제작]`을 선택합니다.
3. 선택된 작업만 생성합니다. 제공자는 원고 내용에 따라 `AI` 또는 `Blockchain`을 함께 반환합니다.
4. 사람이 정확한 초안 리비전을 검토하고 최종 `[시트 전달]`을 승인합니다.
5. 승인은 SQLite에 불변 handoff만 원자적으로 기록합니다. 별도 `sheets-deliver` 작업이 고정 스프레드시트의 `workplace`(sheetId 0)에 새 행을 추가합니다.
6. 새 행의 E(`업로드여부`)는 `X`입니다. Instagram 업로드 후 사람이 `O`로 바꿉니다. 봇은 전달된 행을 수정하지 않습니다.

선택과 생성 성공은 승인이 아닙니다. Figma/Instagram 자동화, Drive 범위, 기존 자료 backfill, 다른 탭(`Demo` 포함) 수정은 범위 밖입니다.

## 설치

```bash
uv sync --group dev
uv sync --group dev --extra sheets  # Google Sheets 실계정 명령을 사용할 때만
```

기본 fixture 경로는 Google 패키지, 자격증명, 네트워크가 필요 없고 일반 결과 파일을 만들지 않습니다.

## 주요 명령

```bash
uv run newsbot init-db --db var/e2e/newsbot.db
uv run newsbot run-fixture --config config/channels.toml --fixture tests/fixtures/channel_messages.json --db var/e2e/newsbot.db --scripted-approve
uv run newsbot status --db var/e2e/newsbot.db
uv run newsbot inspect --db var/e2e/newsbot.db --run-id 1

uv run newsbot sheets-validate --config config/channels.toml --db var/e2e/newsbot.db
uv run newsbot sheets-bootstrap --config config/channels.toml --db var/e2e/newsbot.db
uv run newsbot sheets-deliver --config config/channels.toml --db var/e2e/newsbot.db --handoff-id 1
uv run newsbot sheets-status --config config/channels.toml --db var/e2e/newsbot.db
uv run newsbot sheets-reconcile --config config/channels.toml --db var/e2e/newsbot.db --operation-id 1
```

실계정 명령은 `.env.example`의 `GOOGLE_SHEETS_SPREADSHEET_ID`와 `GOOGLE_SERVICE_ACCOUNT_FILE`을 요구합니다. 서비스 계정 파일은 소유자 전용 `0700` 디렉터리의 `0600` 일반 파일이어야 하며 심볼릭 링크는 거부됩니다.

한 번 원격 요청 가능성이 기록되면 시간 경과나 음성 조회 결과로 자동 재전송하지 않습니다. 정확한 문서 메타데이터만 전달 완료를 증명하며, 불확실 상태는 조회/운영자 조치만 허용합니다.

## 고정 소스 채널

`testingcatalog`, `ai_masters_community`, `aipost`, `coinnesskr`, `exilist_official`, `dolbikong` — 정확히 여섯 채널만 구성합니다.

## 문서

- [요구사항](docs/requirements.md)
- [아키텍처](docs/architecture.md)
- [운영](docs/operations.md)
- [ADR-0001](docs/adr/0001-local-first-sqlite-and-adapters.md)
- [ADR-0002](docs/adr/0002-google-sheets-workplace-delivery.md)